// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::{IndexConfig, SnapshotConfig};
use cloudbreak_snapshot::sidecar::SnapshotType;
use sea_orm::DatabaseConnection;
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use yellowstone_grpc_proto::{geyser::SubscribeUpdateBlock, prelude::UnixTimestamp};

use crate::{
    db_queries,
    indexer::{AccountsReceivedPerBlock, IndexerState},
    metrics,
    modules::{
        finalize_slot::{FinalizeSlotMessage, PendingGapFillReplays, SlotAccounts},
        snapshot::SnapshotProcessingState,
    },
};

// Yellowstone confirmed block updates can arrive out of order under load. Keep
// newly observed slot gaps pending long enough for late block updates to land
// before we promote them to confirmed gaps and mark service health unhealthy.
const GAP_CONFIRMATION_REORDER_TOLERANCE_SLOTS: u64 = 1024;

#[derive(Clone, Default, Debug)]
pub struct SlotGap {
    pub start_slot: u64,
    pub end_slot: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MissingSlot {
    pub slot: u64,
    pub is_confirmed: bool,
}

/// Used to track the self healing state and check for slot gaps
///
/// If a slot gap is detected, will add the slot to the gaps list and mark the service as unhealthy.
///
/// It will periodically try to fill the gaps list by processing the slots that are in the gaps list from
/// a new snapshot.
#[derive(Clone, Default)]
pub struct SelfHealingState {
    pub last_slot_received: Arc<Mutex<u64>>,
    pub gaps_to_confirm: Arc<Mutex<Vec<SlotGap>>>,
    pub gaps_list: Arc<Mutex<Vec<MissingSlot>>>,
    pub rpc_url: String,
    pub pending_gap_fill_replays: PendingGapFillReplays,
    pub grpc_reconnected: Arc<AtomicBool>,
}

impl SelfHealingState {
    pub fn new(config: &IndexConfig, pending_gap_fill_replays: PendingGapFillReplays) -> Self {
        let rpc_url = config.grpc.rpc_url();

        Self {
            last_slot_received: Arc::new(Mutex::new(0)),
            gaps_to_confirm: Arc::new(Mutex::new(Vec::new())),
            gaps_list: Arc::new(Mutex::new(Vec::new())),
            rpc_url,
            pending_gap_fill_replays,
            grpc_reconnected: Arc::new(AtomicBool::new(false)),
        }
    }

    fn get_confirmed_gaps_list(&self) -> Vec<u64> {
        self.gaps_list
            .lock()
            .expect("Failed to lock gaps_list")
            .clone()
            .into_iter()
            .filter(|s| s.is_confirmed)
            .map(|s| s.slot)
            .collect::<Vec<_>>()
    }

    fn get_gaps_to_confirm(&self) -> Vec<SlotGap> {
        self.gaps_to_confirm
            .lock()
            .expect("Failed to lock gaps_to_confirm")
            .clone()
    }

    fn remove_slot_from_gaps_list(&self, slot: u64) {
        let mut gaps_list = self.gaps_list.lock().expect("Failed to lock gaps_list");
        gaps_list.retain(|s| {
            let keep_slot = s.slot != slot;
            if !keep_slot && !s.is_confirmed {
                tracing::warn!("Slot {} is not confirmed, removing from gaps list", slot);
            }

            keep_slot
        });
    }

    /// Checks if the slot is the next one after the last received slot.
    /// If not, it will add the slot to the gaps list and mark the service as unhealthy.
    /// Returns true if the gap coincides with a grpc reconnect.
    pub async fn check_slot_gap(&self, slot: u64) -> bool {
        let last_slot_received = *self
            .last_slot_received
            .lock()
            .expect("Failed to lock last_slot_received");

        if slot < last_slot_received {
            tracing::warn!(
                "Out of order slot received: {} - previous slot received: {}",
                slot,
                last_slot_received
            );

            // If we had added the slot to the gaps list, remove it
            self.remove_slot_from_gaps_list(slot);

            return false;
        }

        let mut reconnect_gap = false;
        if last_slot_received != 0 && slot - last_slot_received > 1 {
            reconnect_gap = self.grpc_reconnected.swap(false, Ordering::SeqCst);

            // We add slots to both lists, to easily cover the out of order slots scenario.
            self.gaps_to_confirm
                .lock()
                .expect("Failed to lock gaps_to_confirm")
                .push(SlotGap {
                    start_slot: last_slot_received,
                    end_slot: slot,
                });

            let slots_lists = ((last_slot_received + 1)..slot)
                .map(|gap_slot| MissingSlot {
                    slot: gap_slot,
                    is_confirmed: false,
                })
                .collect::<Vec<_>>();

            self.gaps_list
                .lock()
                .expect("Failed to lock gaps_list")
                .extend(slots_lists);

            *self
                .pending_gap_fill_replays
                .gap_fill_active
                .lock()
                .expect("Failed to lock gap_fill_active") = true;
        }

        *self
            .last_slot_received
            .lock()
            .expect("Failed to lock last_slot_received") = slot;
        reconnect_gap
    }

    /// It will confirm which missing slots belongs to valid blocks and remove from the gaps list the ones that are not.
    /// If the response from the RPC is malformed, it will simply skip to retry in the next iteration.
    /// Will mark the service as unhealthy if the gaps list is not empty.
    async fn confirm_gaps(&self, db: DatabaseConnection) {
        let gaps_list = self.gaps_list.clone();
        let gaps_to_confirm_values = self.get_gaps_to_confirm();
        let rpc_url = self.rpc_url.clone();
        let gaps_to_confirm = self.gaps_to_confirm.clone();
        let last_slot_received = *self
            .last_slot_received
            .lock()
            .expect("Failed to lock last_slot_received");

        tokio::spawn(async move {
            let _guard = metrics::TokioTaskCounterGuard::new("self_healing");

            let client = solana_client::nonblocking::rpc_client::RpcClient::new(rpc_url);
            let mut valid_blocks_on_the_gaps = Vec::new();

            for gap in gaps_to_confirm_values {
                if last_slot_received.saturating_sub(gap.end_slot)
                    < GAP_CONFIRMATION_REORDER_TOLERANCE_SLOTS
                {
                    tracing::debug!(
                        target: "self_healing",
                        "Delaying slot gap confirmation for reorder tolerance: start_slot: {} - end_slot: {} - last_slot_received: {}",
                        gap.start_slot,
                        gap.end_slot,
                        last_slot_received
                    );
                    continue;
                }

                let gap_len = (gap.end_slot - gap.start_slot + 1) as usize;
                let pending_gap_slots = gaps_list
                    .lock()
                    .expect("Failed to lock gaps_list")
                    .iter()
                    .map(|slot| slot.slot)
                    .collect::<HashSet<_>>();

                let result = client.get_blocks_with_limit(gap.start_slot, gap_len).await;

                match result {
                    Ok(blocks) => {
                        if !blocks.contains(&gap.start_slot) || !blocks.contains(&gap.end_slot) {
                            // It should always contain the last slot received and the current slot
                            tracing::debug!(
                                target: "self_healing",
                                "Malformed RPC blocks response for slot gap: start_slot: {} - end_slot: {}. Response: {:?} - skipping",
                                gap.start_slot,
                                gap.end_slot,
                                blocks
                            );

                            continue;
                        }

                        for gap_slot in (gap.start_slot + 1)..gap.end_slot {
                            if !pending_gap_slots.contains(&gap_slot) {
                                continue;
                            }

                            // Ensure that we only count slots that belong to valid blocks
                            if blocks.contains(&gap_slot) {
                                valid_blocks_on_the_gaps.push(gap_slot);
                            } else {
                                // We remove unconfirmed slots that we know are not valid, to avoid the list growing unnecessarily
                                gaps_list
                                    .lock()
                                    .expect("Failed to lock gaps_list")
                                    .retain(|s| s.slot != gap_slot || s.is_confirmed);
                            }
                        }

                        // Remove gap from gaps to confirm
                        gaps_to_confirm
                            .lock()
                            .expect("Failed to lock gaps_to_confirm")
                            .retain(|g| {
                                g.start_slot != gap.start_slot || g.end_slot != gap.end_slot
                            });
                    }
                    Err(e) => {
                        tracing::error!("Failed to get blocks: {:?}", e);
                    }
                }
            }

            // If we have valid blocks on the gap, mark them as confimed in the gaps list and mark the service as unhealthy
            if !valid_blocks_on_the_gaps.is_empty() {
                {
                    tracing::error!(
                        target: "self_healing",
                        "Adding confirmedslots to the gaps list: {:?}",
                        valid_blocks_on_the_gaps
                    );

                    let mut gaps_list = gaps_list.lock().expect("Failed to lock gaps_list");
                    for slot in gaps_list.iter_mut() {
                        if valid_blocks_on_the_gaps.contains(&slot.slot) {
                            slot.is_confirmed = true;
                        }
                    }
                }

                db_queries::update_service_health(&db, false).await;
            }
        });
    }

    /// Starts a separate task that will periodically check for gaps in proccesed slots and fill them out of incremental snapshots.
    ///
    /// Processes only one snapshot at a time.
    ///
    /// Downloads an incremental snapshot that covers the newest slot in the gaps list and processes only the slots that are in the gaps list.
    ///
    /// After that cleans up/finalizes the slots that were repaired and removes the slots from the gaps list.
    ///
    /// Will run iteratively based on the configured interval for gap filling.
    ///
    /// Will only try to repair CONFIRMED slots.
    pub async fn fill_gaps(
        self,
        db: DatabaseConnection,
        config: IndexConfig,
        indexer_state: IndexerState,
        finalize_slot_handler_tx: Sender<FinalizeSlotMessage>,
    ) -> JoinHandle<Result<(), anyhow::Error>> {
        tokio::spawn(async move {
            let _guard = metrics::TokioTaskCounterGuard::new("self_healing_fill_gaps");

            loop {
                // TODO: Make the gap filling interval configurable
                tokio::time::sleep(Duration::from_secs(30)).await;

                let is_startup_finished = {
                    *indexer_state
                        .snapshot_processing_state
                        .lock()
                        .expect("Failed to lock snapshot_processing_state")
                        == SnapshotProcessingState::FinishedAndCleanedUp
                };

                if is_startup_finished {
                    self.confirm_gaps(db.clone()).await;
                } else {
                    tracing::debug!(
                        target: "self_healing",
                        "Skipping slot gap confirmation while snapshot startup is still in progress"
                    );
                }

                let mut confirmed_gaps_list = self.get_confirmed_gaps_list();
                if confirmed_gaps_list.is_empty() {
                    let gaps_list_empty = self
                        .gaps_list
                        .lock()
                        .expect("Failed to lock gaps_list")
                        .is_empty();
                    if gaps_list_empty {
                        *indexer_state
                            .pending_gap_fill_replays
                            .gap_fill_active
                            .lock()
                            .expect("Failed to lock gap_fill_active") = false;
                    }
                    if is_startup_finished {
                        db_queries::update_service_health(&db, true).await;
                    }

                    continue;
                }

                // Dowloading more than one snapshot at a time causes some issues with the current implementation.
                if !is_startup_finished {
                    continue;
                }

                let start_time = tokio::time::Instant::now();
                tracing::info!("Starting to fill gaps: {:?}", confirmed_gaps_list,);

                confirmed_gaps_list.sort();
                let newest_slot_in_gaps_list =
                    *confirmed_gaps_list.last().expect("No slots in gaps list");

                let snapshot_config = config.snapshot.as_ref().unwrap();
                let snapshot_config = SnapshotConfig {
                    accounts_file_concurency: snapshot_config.accounts_file_concurency,
                    database: config.database.clone(),
                    tracker_endpoint: snapshot_config.tracker_endpoint.clone(),
                    metrics: config.metrics.clone(),
                    programs: config.programs.clone(),
                    pg_indexes: snapshot_config.pg_indexes.clone(),
                };

                let (handle, mut rx) = match download_and_process_snapshot_for_gap_filling(
                    Some(newest_slot_in_gaps_list),
                    snapshot_config,
                    confirmed_gaps_list.clone(),
                )
                .await
                {
                    Ok((handle, rx)) => (handle, rx),
                    Err(e) => {
                        tracing::warn!(
                            "Snapshot is not available for gap filling yet, waiting for next iteration (error: {:?})",
                            e
                        );
                        continue;
                    }
                };

                let mut repaired_slots = Vec::new();
                while let Some(update_block) = rx.recv().await {
                    repaired_slots.push(update_block.slot);

                    crate::modules::save_block::save_block(
                        update_block,
                        &db,
                        config.clone(),
                        indexer_state.clone(),
                    )
                    .await;
                }

                handle.await??;

                let repaired_slot_set = repaired_slots.iter().copied().collect::<HashSet<_>>();

                // Finalize the slots that produced relevant account updates and remove them from
                // the gaps list.
                for slot in repaired_slots {
                    let updated_accounts = indexer_state
                        .updated_accounts_map
                        .lock()
                        .expect("Failed to lock updated_accounts_map")
                        .remove(&slot)
                        .unwrap_or_default();

                    finalize_slot_handler_tx
                        .send(FinalizeSlotMessage {
                            slot,
                            db: db.clone(),
                            updated_accounts,
                            updated_accounts_during_startup: indexer_state
                                .updated_accounts_during_startup
                                .clone(),
                            pending_gap_fill_replays: indexer_state
                                .pending_gap_fill_replays
                                .clone(),
                        })
                        .await?;

                    self.remove_slot_from_gaps_list(slot);
                }

                // Some confirmed slots can be real blocks but contain no account updates relevant
                // to the configured program filter. In that case the snapshot processor has
                // successfully proven there is nothing for Cloudbreak to persist, so the gap must
                // still be cleared instead of being retried forever.
                for slot in confirmed_gaps_list
                    .iter()
                    .copied()
                    .filter(|slot| !repaired_slot_set.contains(slot))
                {
                    tracing::info!(
                        "Clearing gap slot with no relevant account updates after snapshot fill: {}",
                        slot
                    );
                    self.remove_slot_from_gaps_list(slot);
                }

                let elapsed = start_time.elapsed().as_secs_f64();
                tracing::info!(
                    "Finished filling gaps: {:?} - in {} seconds",
                    confirmed_gaps_list,
                    elapsed
                );

                let gaps_list_empty = self
                    .gaps_list
                    .lock()
                    .expect("Failed to lock gaps_list")
                    .is_empty();
                if gaps_list_empty {
                    *indexer_state
                        .pending_gap_fill_replays
                        .gap_fill_active
                        .lock()
                        .expect("Failed to lock gap_fill_active") = false;
                }

                if self.get_confirmed_gaps_list().is_empty() {
                    db_queries::update_service_health(&db, true).await;
                }

                let buffered = std::mem::take(
                    &mut *indexer_state
                        .pending_gap_fill_replays
                        .closures_buffer
                        .lock()
                        .expect("Failed to lock closures_buffer"),
                );

                if !buffered.is_empty() {
                    tracing::info!(
                        "Replaying {} buffered closure cleanups after gap fill",
                        buffered.len()
                    );

                    let now_ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("system time before UNIX_EPOCH")
                        .as_secs() as i64;

                    for SlotAccounts { accounts, slot } in buffered {
                        finalize_slot_handler_tx
                            .send(FinalizeSlotMessage {
                                slot,
                                db: db.clone(),
                                updated_accounts: AccountsReceivedPerBlock {
                                    block_time: Some(UnixTimestamp { timestamp: now_ts }),
                                    accounts: Vec::new(),
                                    closed_accounts: accounts,
                                },
                                updated_accounts_during_startup: indexer_state
                                    .updated_accounts_during_startup
                                    .clone(),
                                pending_gap_fill_replays: indexer_state
                                    .pending_gap_fill_replays
                                    .clone(),
                            })
                            .await?;
                    }
                }
            }
        })
    }
}

async fn download_and_process_snapshot_for_gap_filling(
    received_slot: Option<u64>,
    config: SnapshotConfig,
    gaps_list: Vec<u64>,
) -> Result<
    (
        JoinHandle<Result<(), anyhow::Error>>,
        Receiver<SubscribeUpdateBlock>,
    ),
    anyhow::Error,
> {
    let snapshot_pair_future = cloudbreak_snapshot::sidecar::get_snapshot_data(
        &config.tracker_endpoint.endpoint,
        received_slot,
        true,
        true,
    );

    let snapshot_pair =
        tokio::time::timeout(Duration::from_secs(60), snapshot_pair_future).await??;

    let (tx, rx) = tokio::sync::mpsc::channel::<SubscribeUpdateBlock>(100);

    // We passed the force_returned_incremental flag to true, so we know that the snapshot pair contains an incremental snapshot
    let incremental_snapshot_data = snapshot_pair
        .incremental_snapshot
        .ok_or_else(|| anyhow::anyhow!("No incremental snapshot available"))?;

    let handle = tokio::spawn(async move {
        cloudbreak_snapshot::sidecar::download_snapshot_file(
            &snapshot_pair.downloading_endpoint,
            incremental_snapshot_data.clone(),
            SnapshotType::Incremental,
        )
        .await?;

        cloudbreak_snapshot::process_downloaded_snapshot_with_gap_filling(
            incremental_snapshot_data.slot,
            incremental_snapshot_data.file_name,
            config,
            gaps_list,
            tx,
        )
        .await?;

        Ok(())
    });

    Ok((handle, rx))
}
