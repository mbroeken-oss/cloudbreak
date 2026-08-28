// SPDX-License-Identifier: AGPL-3.0-only
//! Rooted materialization of current Stake-program accounts for Cloudbreak API.
//!
//! A complete generation is built from the finalized raw account tables before
//! `stake_projection_status` points readers at it. The previous generation is
//! retained for one refresh so an API request that already read the old status
//! cannot observe an empty page during a generation switch.

use cloudbreak_core::{IndexConfig, STAKE_PROGRAM_ID};
use futures::TryStreamExt;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, StreamTrait};
use solana_epoch_schedule::EpochSchedule;
use solana_pubkey::Pubkey;
use solana_runtime::non_circulating_supply::{non_circulating_accounts, withdraw_authority};
use solana_stake_interface::state::StakeStateV2;
use std::{
    collections::HashSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{task::JoinHandle, time::sleep};
use yellowstone_grpc_proto::geyser::CommitmentLevel;

const REFRESH_INTERVAL: Duration = Duration::from_secs(60);

pub fn spawn_stake_projection_rebuilder(
    db: DatabaseConnection,
    config: IndexConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if !config.programs.is_program_selected(&STAKE_PROGRAM_ID) {
            tracing::info!("Stake projection disabled because Stake program is not indexed");
            return;
        }

        loop {
            if let Err(error) = refresh_if_needed(&db).await {
                tracing::warn!(%error, "stake projection refresh failed");
            }
            sleep(REFRESH_INTERVAL).await;
        }
    })
}

async fn refresh_if_needed(db: &DatabaseConnection) -> anyhow::Result<()> {
    if !service_healthy(db).await {
        return Ok(());
    }
    let Some(context_slot) = finalized_slot(db).await else {
        return Ok(());
    };
    if active_context_slot(db).await? >= Some(context_slot) {
        return Ok(());
    }

    let generation = next_generation(db).await?;
    let started = std::time::Instant::now();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        r#"
        INSERT INTO stake_accounts_current (generation, pubkey, slot, lamports, data)
        SELECT $1, pubkey, slot, lamports, data
        FROM (
            SELECT DISTINCT ON (pubkey) pubkey, slot, lamports, data
            FROM (
                SELECT pubkey, slot, lamports, data
                FROM accounts
                WHERE owner = $2 AND slot <= $3
                UNION ALL
                SELECT pubkey, slot, lamports, data
                FROM snapshot_accounts
                WHERE owner = $2 AND slot <= $3
            ) AS versions
            ORDER BY pubkey ASC, slot DESC
        ) AS current_stake_accounts
        WHERE lamports > 0
        "#,
        [
            generation.into(),
            STAKE_PROGRAM_ID.to_bytes().to_vec().into(),
            (context_slot as i64).into(),
        ],
    ))
    .await?;

    // Audit the complete new generation before publishing it. If the audit fails,
    // readers continue using the prior finalized generation.
    compute_non_circulating_audit(db, generation, context_slot).await?;

    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        r#"
        INSERT INTO stake_projection_status (id, generation, context_slot, refreshed_at_ms)
        VALUES (1, $1, $2, $3)
        ON CONFLICT (id) DO UPDATE SET
            generation = EXCLUDED.generation,
            context_slot = EXCLUDED.context_slot,
            refreshed_at_ms = EXCLUDED.refreshed_at_ms
        "#,
        [
            generation.into(),
            (context_slot as i64).into(),
            (now_ms() as i64).into(),
        ],
    ))
    .await?;

    // Retain the current and immediately preceding generation to make the API
    // status-read then page-read sequence safe across an atomic status switch.
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "DELETE FROM stake_accounts_current WHERE generation < $1",
        [(generation - 1).into()],
    ))
    .await?;

    tracing::info!(
        generation,
        context_slot,
        elapsed_ms = started.elapsed().as_millis(),
        "rebuilt rooted stake-account projection"
    );
    Ok(())
}

async fn service_healthy(db: &DatabaseConnection) -> bool {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT healthy FROM service_health WHERE id = 1".to_string(),
    ))
    .await
    .ok()
    .flatten()
    .and_then(|row| row.try_get::<bool>("", "healthy").ok())
    .unwrap_or(false)
}

async fn finalized_slot(db: &DatabaseConnection) -> Option<u64> {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT slot FROM slots WHERE commitment = $1",
        [(CommitmentLevel::Finalized as i32).into()],
    ))
    .await
    .ok()
    .flatten()
    .and_then(|row| row.try_get::<i64>("", "slot").ok())
    .and_then(|slot| slot.try_into().ok())
}

async fn active_context_slot(db: &DatabaseConnection) -> anyhow::Result<Option<u64>> {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT context_slot FROM stake_projection_status WHERE id = 1".to_string(),
        ))
        .await?;
    row.map(|row| {
        let slot: i64 = row.try_get("", "context_slot")?;
        slot.try_into()
            .map_err(|_| sea_orm::DbErr::Custom("negative stake projection slot".to_string()))
    })
    .transpose()
    .map_err(Into::into)
}

async fn next_generation(db: &DatabaseConnection) -> anyhow::Result<i64> {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COALESCE(MAX(generation), 0) + 1 AS generation FROM stake_projection_status"
                .to_string(),
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("stake projection status query returned no row"))?;
    Ok(row.try_get("", "generation")?)
}

async fn compute_non_circulating_audit(
    db: &DatabaseConnection,
    generation: i64,
    context_slot: u64,
) -> anyhow::Result<()> {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT block_time FROM slots WHERE commitment = $1 AND slot = $2",
            [
                (CommitmentLevel::Finalized as i32).into(),
                (context_slot as i64).into(),
            ],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing finalized block time"))?;
    let block_time: i64 = row.try_get("", "block_time")?;
    // Match Agave's Clock epoch, including the historical warmup schedule.
    let epoch = EpochSchedule::default().get_epoch(context_slot);
    let static_accounts: HashSet<Pubkey> = non_circulating_accounts().into_iter().collect();
    let withdrawers: HashSet<Pubkey> = withdraw_authority().into_iter().collect();

    let mut lamports = 0u64;
    let mut account_count = 0u64;
    let mut stream = db
        .stream(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT pubkey, lamports, data FROM stake_accounts_current WHERE generation = $1",
            [generation.into()],
        ))
        .await?;
    while let Some(row) = stream.try_next().await? {
        let pubkey_bytes: Vec<u8> = row.try_get("", "pubkey")?;
        let pubkey = Pubkey::try_from(pubkey_bytes.as_slice())?;
        if static_accounts.contains(&pubkey) {
            continue;
        }
        let data: Vec<u8> = row.try_get("", "data")?;
        let Ok(state) = bincode::deserialize::<StakeStateV2>(&data) else {
            continue;
        };
        let Some(meta) = state.meta() else { continue };
        let locked = meta.lockup.unix_timestamp > block_time
            || meta.lockup.epoch > epoch
            || withdrawers.contains(&meta.authorized.withdrawer);
        if locked {
            let value: i64 = row.try_get("", "lamports")?;
            lamports = lamports
                .checked_add(value.try_into()?)
                .ok_or_else(|| anyhow::anyhow!("non-circulating sum overflow"))?;
            account_count += 1;
        }
    }
    drop(stream);

    let static_values = static_accounts
        .iter()
        .map(|key| format!("('\\\\x{}'::bytea)", hex::encode(key.as_ref())))
        .collect::<Vec<_>>()
        .join(",");
    let static_rows = db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            format!(
                "WITH requested(pubkey) AS (VALUES {static_values}), latest AS (\
                 SELECT DISTINCT ON (pubkey) pubkey, lamports FROM (\
                 SELECT pubkey, slot, lamports FROM accounts WHERE slot <= {context_slot} \
                 UNION ALL SELECT pubkey, slot, lamports FROM snapshot_accounts WHERE slot <= {context_slot}\
                 ) versions WHERE pubkey IN (SELECT pubkey FROM requested) ORDER BY pubkey, slot DESC) \
                 SELECT lamports FROM latest WHERE lamports > 0"
            ),
        ))
        .await?;
    for row in static_rows {
        let value: i64 = row.try_get("", "lamports")?;
        lamports = lamports
            .checked_add(value.try_into()?)
            .ok_or_else(|| anyhow::anyhow!("non-circulating sum overflow"))?;
        account_count += 1;
    }

    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO stake_supply_audits (context_slot, generation, block_time, epoch, non_circulating_lamports, account_count, computed_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (context_slot) DO UPDATE SET generation = EXCLUDED.generation, block_time = EXCLUDED.block_time, epoch = EXCLUDED.epoch, non_circulating_lamports = EXCLUDED.non_circulating_lamports, account_count = EXCLUDED.account_count, computed_at_ms = EXCLUDED.computed_at_ms",
        [
            (context_slot as i64).into(), generation.into(), block_time.into(), (epoch as i64).into(),
            (lamports as i64).into(), (account_count as i64).into(), (now_ms() as i64).into(),
        ],
    )).await?;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
