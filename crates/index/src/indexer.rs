// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};
use tokio::sync::mpsc::{Receiver, Sender};
use yellowstone_grpc_proto::{
    geyser::{SlotStatus, SubscribeUpdate, subscribe_update::UpdateOneof},
    prelude::UnixTimestamp,
};
use cloudbreak_core::{
    EnvironmentInfo, IndexConfig, Result as CloudbreakResult, TryLoadConfig,
    modules::account_owner_map::AccountOwnerMap,
};

use crate::modules::snapshot::SnapshotProcessingState;
use crate::modules::{
    finalize_slot::{FinalizeSlotMessage, PendingGapFillReplays, UpdatedAccountsDuringStartup},
    self_healing::SelfHealingState,
};
use crate::modules::{
    hash_checker::{self, HashCheckerState},
    panic_handler,
};
use crate::{metrics, modules};

async fn force_finalize_lost_slots(
    indexer_state: &IndexerState,
    finalize_slot_handler_tx: &Sender<FinalizeSlotMessage>,
    db: &DatabaseConnection,
) {
    let stale: Vec<(u64, AccountsReceivedPerBlock)> = {
        let mut map = indexer_state
            .updated_accounts_map
            .lock()
            .expect("Failed to lock updated_accounts_map");
        std::mem::take(&mut *map).into_iter().collect()
    };

    if stale.is_empty() {
        return;
    }

    tracing::info!(
        "Force-finalizing {} stale slots after grpc reconnect",
        stale.len()
    );

    for (slot, updated_accounts) in stale {
        finalize_slot_handler_tx
            .send(FinalizeSlotMessage {
                slot,
                db: db.clone(),
                updated_accounts,
                updated_accounts_during_startup: indexer_state
                    .updated_accounts_during_startup
                    .clone(),
                pending_gap_fill_replays: indexer_state.pending_gap_fill_replays.clone(),
            })
            .await
            .expect("Failed to send finalize slot message");
    }
}

#[derive(Clone)]
pub struct IndexerState {
    /// Used to track the size of the GRPCbuffer channel and record the metrics
    pub buffer_channel_rx_len: Arc<Mutex<usize>>,
    /// Used to track the snapshot processing state and only process the snapshot once, and mark the service as healthy once finished
    pub snapshot_processing_state: Arc<Mutex<SnapshotProcessingState>>,
    /// Used to track the self healing state and check for slot gaps
    pub self_healing_state: SelfHealingState,
    /// Contains the accounts that were updated in each slot for later cleanup.
    /// Usually this is going to contain the amount of slots between confirmed and finalized commitments.=
    pub updated_accounts_map: Arc<Mutex<HashMap<u64, AccountsReceivedPerBlock>>>,
    /// Contains the accounts that were updated during startup for later cleanup.
    /// The goal is to only cleanup `snapshot_accounts` table once the snapshot is processed and db indexes are created.
    pub updated_accounts_during_startup: UpdatedAccountsDuringStartup,
    /// Used to track the size of the finalize slot buffer and record the metrics (this is used in snapshot
    /// for the cluster operation, to avoid overloading the DB)
    pub finalize_slot_buffer_size: Arc<Mutex<usize>>,
    /// Used to buffer closure cleanups received during gap filling so they can be replayed after
    /// the gap fill completes, catching any zombie rows inserted by the gap fill.
    pub pending_gap_fill_replays: PendingGapFillReplays,
    /// Used to track the accounts owner
    pub accounts_owner_map: AccountOwnerMap,
}

pub async fn run(config: &str) -> CloudbreakResult<()> {
    panic_handler::start();

    let config = IndexConfig::try_load(config)?;

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    metrics::setup_metrics(&config)?;

    let mut connect_options = ConnectOptions::from(config.database.clone());
    let server_side_timeout = config.database.server_side_timeout.to_string();
    connect_options.map_sqlx_postgres_opts(move |pg_opts| {
        pg_opts.options([("statement_timeout", &server_side_timeout)])
    });

    let db = Database::connect(connect_options)
        .await
        .expect("Failed to connect to database");

    EnvironmentInfo::upsert_filters(&db, &config.programs)
        .await
        .expect("Failed to upsert indexer filter");

    // Buffer layer that allows to keep track of falling behind ocurrences (and help recovering from them)
    let (buffer_channel_tx, buffer_channel_rx) =
        tokio::sync::mpsc::channel(config.grpc.sources_channel_size);

    let snapshot_processing_state = Arc::new(Mutex::new(SnapshotProcessingState::NotStarted));
    let finalize_slot_buffer_size = Arc::new(Mutex::new(0));

    let pending_gap_fill_replays = PendingGapFillReplays::default();

    let query_timeout = Duration::from_secs(config.database.save_block_queries_timeout);
    let accounts_owner_map = if config.accounts_owner_map_enabled {
        AccountOwnerMap::new(db.clone(), query_timeout)
    } else {
        AccountOwnerMap::default()
    };

    let indexer_state = IndexerState {
        buffer_channel_rx_len: Arc::new(Mutex::new(buffer_channel_rx.len())),
        snapshot_processing_state: snapshot_processing_state.clone(),
        self_healing_state: SelfHealingState::new(&config, pending_gap_fill_replays.clone()),
        updated_accounts_map: Arc::new(Mutex::new(HashMap::new())),
        updated_accounts_during_startup: UpdatedAccountsDuringStartup::new(
            snapshot_processing_state.clone(),
        ),
        finalize_slot_buffer_size: finalize_slot_buffer_size.clone(),
        pending_gap_fill_replays,
        accounts_owner_map,
    };

    let grpc_cancel = Arc::new(AtomicBool::new(false));

    let hash_checker_state = match (&config.hash_checker, &config.snapshot) {
        (Some(hc_cfg), Some(snap_cfg)) => Some(HashCheckerState::new(
            hc_cfg.clone(),
            snap_cfg.clone(),
            grpc_cancel.clone(),
        )),
        (Some(_), None) => {
            panic!("hash-checker config requires the snapshot section for sidecar endpoint")
        }
        _ => None,
    };

    if let Some(hc) = &hash_checker_state {
        hc.spawn_orchestrator();
    }

    let grpc_handle = modules::grpc::subscribe_grpc_with_reconnection(
        config.clone(),
        buffer_channel_tx,
        indexer_state.buffer_channel_rx_len.clone(),
        indexer_state.self_healing_state.last_slot_received.clone(),
        grpc_cancel.clone(),
        indexer_state.self_healing_state.grpc_reconnected.clone(),
        db.clone(),
    );

    let finalize_slot_handler_tx = modules::finalize_slot::start_finalize_slot_handler(
        &config,
        indexer_state.finalize_slot_buffer_size.clone(),
    );

    let self_healing_fill_gaps_handle = indexer_state
        .self_healing_state
        .clone()
        .fill_gaps(
            db.clone(),
            config.clone(),
            indexer_state.clone(),
            finalize_slot_handler_tx.clone(),
        )
        .await;

    let _epoch_stakes_handle =
        modules::epoch_stakes::spawn_epoch_stakes_recomputer(db.clone(), config.clone());

    metrics::GAPS_LIST
        .set(indexer_state.self_healing_state.gaps_list.clone())
        .expect("Failed to set gaps list");
    metrics::GAPS_TO_CONFIRM
        .set(indexer_state.self_healing_state.gaps_to_confirm.clone())
        .expect("Failed to set gaps to confirm");
    metrics::UPDATED_ACCOUNTS_MAP
        .set(indexer_state.updated_accounts_map.clone())
        .expect("Failed to set updated accounts map");

    tokio::select! {
        _ = main_loop(
            buffer_channel_rx,
            indexer_state.clone(),
            finalize_slot_handler_tx.clone(),
            db.clone(),
            config.clone(),
            hash_checker_state.clone(),
        ) => {
            tracing::warn!("Main loop finished");

            if let Some(hc) = hash_checker_state {
                match hash_checker::finalize_and_compare(
                    hc,
                    db,
                    config,
                    indexer_state,
                    finalize_slot_handler_tx,
                ).await {
                    Ok(true) => std::process::exit(0),
                    Ok(false) => std::process::exit(1),
                    Err(e) => {
                        tracing::error!("hash-checker finalize failed: {:?}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
        result = grpc_handle => {
            match result {
                Ok(_) => tracing::warn!("GRPC handle finished"),
                Err(e) => tracing::error!("GRPC handle panicked: {:?}", e.into_panic()),
            }
        }
        result = self_healing_fill_gaps_handle => {
            result??;
        }
    }

    Ok(())
}

async fn main_loop(
    mut buffer_channel_rx: Receiver<SubscribeUpdate>,
    indexer_state: IndexerState,
    finalize_slot_handler_tx: Sender<FinalizeSlotMessage>,
    db: DatabaseConnection,
    config: IndexConfig,
    hash_checker_state: Option<HashCheckerState>,
) {
    while let Some(update) = buffer_channel_rx.recv().await {
        {
            let current_buffer_channel_rx_len = buffer_channel_rx.len();
            *indexer_state
                .buffer_channel_rx_len
                .lock()
                .expect("Failed to lock buffer_channel_rx_len") = current_buffer_channel_rx_len;

            metrics::record_grpc_buffer_channel_size(current_buffer_channel_rx_len);
        }

        if let Some(hc) = &hash_checker_state {
            hash_checker::note_update(hc, &update);
            if hc.is_buffering() {
                hc.push(update);
                if hc.should_break() {
                    return;
                }
                continue;
            }
        }

        process_update(
            update,
            &indexer_state,
            &finalize_slot_handler_tx,
            &db,
            &config,
        )
        .await;

        if let Some(hc) = &hash_checker_state
            && hc.should_break()
        {
            return;
        }
    }
}

pub async fn process_update(
    update: SubscribeUpdate,
    indexer_state: &IndexerState,
    finalize_slot_handler_tx: &Sender<FinalizeSlotMessage>,
    db: &DatabaseConnection,
    config: &IndexConfig,
) {
    match update.update_oneof {
        Some(UpdateOneof::Block(block)) => {
            let reconnect_gap = indexer_state
                .self_healing_state
                .check_slot_gap(block.slot)
                .await;

            if reconnect_gap {
                force_finalize_lost_slots(indexer_state, finalize_slot_handler_tx, db).await;
            }

            modules::save_block::save_block(block, db, config.clone(), indexer_state.clone()).await;
        }
        Some(UpdateOneof::Slot(slot_update)) => {
            let slot = slot_update.slot;
            let commitment = SlotStatus::try_from(slot_update.status).expect("Invalid slot status");

            match commitment {
                SlotStatus::SlotProcessed | SlotStatus::SlotConfirmed => (),
                SlotStatus::SlotFinalized => {
                    let map_len = indexer_state
                        .updated_accounts_map
                        .lock()
                        .expect("Failed to lock updated_accounts_map")
                        .len();

                    let updated_accounts = indexer_state
                        .updated_accounts_map
                        .lock()
                        .expect("Failed to lock updated_accounts_map")
                        .remove(&slot)
                        .unwrap_or_else(|| {
                            // We make the limit on 31, because on normal startup operation, we can
                            // expect that the slot is not in the hashmap yet (Normal length of the hashmap is 31 slots
                            // which is the amount of slots between confirmed and finalized commitments)
                            if map_len >= 31 {
                                tracing::error!(
                                    "Updated accounts not found for slot {} - MAP LEN: {}",
                                    slot,
                                    map_len
                                );
                            } else {
                                tracing::debug!(
                                    "Slots hashmap filling up - slot {} - MAP LEN: {}",
                                    slot,
                                    map_len
                                );
                            }

                            AccountsReceivedPerBlock::default()
                        });

                    let updated_accounts_during_startup =
                        indexer_state.updated_accounts_during_startup.clone();

                    finalize_slot_handler_tx
                        .send(FinalizeSlotMessage {
                            slot,
                            db: db.clone(),
                            updated_accounts,
                            updated_accounts_during_startup,
                            pending_gap_fill_replays: indexer_state
                                .pending_gap_fill_replays
                                .clone(),
                        })
                        .await
                        .expect("Failed to send finalize slot message");
                }
                _ => tracing::error!("Unexpected slot status: {:?}", commitment),
            }
        }
        _ => {}
    }
}

/// Tracks the accounts that were received in the block/slot for later cleanup
#[derive(Default, Debug)]
pub struct AccountsReceivedPerBlock {
    pub block_time: Option<UnixTimestamp>,
    pub accounts: Vec<Vec<u8>>,
    pub closed_accounts: Vec<Vec<u8>>,
}
