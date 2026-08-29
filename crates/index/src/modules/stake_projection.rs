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
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_clock::Clock;
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

        let rpc_client = RpcClient::new(config.grpc.rpc_url());
        loop {
            if let Err(error) = refresh_if_needed(&db, &rpc_client).await {
                tracing::warn!(%error, "stake projection refresh failed");
            }
            sleep(REFRESH_INTERVAL).await;
        }
    })
}

async fn refresh_if_needed(db: &DatabaseConnection, rpc_client: &RpcClient) -> anyhow::Result<()> {
    if !service_healthy(db).await {
        return Ok(());
    }
    let Some(context_slot) = finalized_context_slot(db).await else {
        return Ok(());
    };
    if active_context_slot(db).await? >= Some(context_slot) {
        return Ok(());
    }
    // `slots.block_time` is not populated by the current Geyser feed. Using its
    // zero placeholder makes every timestamp-locked stake account look locked
    // since 1970. Fetch the rooted slot's real timestamp from the canonical RPC
    // before building or publishing the generation.
    let unix_timestamp = rpc_client
        .get_block_time(context_slot)
        .await
        .map_err(|error| anyhow::anyhow!("getBlockTime({context_slot}) failed: {error}"))?;
    let clock = clock_at(context_slot, unix_timestamp);

    let generation = next_generation(db).await?;
    let started = std::time::Instant::now();
    // A failed off-path build has no status row, so remove its incomplete
    // generation before retrying the same generation number.
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "DELETE FROM stake_accounts_current WHERE generation >= $1",
        [generation.into()],
    ))
    .await?;

    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        r#"
        INSERT INTO stake_accounts_current (generation, pubkey, slot, lamports, data)
        SELECT $1, pubkey, slot, lamports, data
        FROM (
            SELECT DISTINCT ON (pubkey) pubkey, slot, write_version, lamports, data
            FROM (
                SELECT pubkey, slot, write_version, lamports, data
                FROM accounts
                WHERE owner = $2 AND slot <= $3
                UNION ALL
                SELECT pubkey, slot, write_version, lamports, data
                FROM snapshot_accounts
                WHERE owner = $2 AND slot <= $3
            ) AS versions
            ORDER BY pubkey ASC, slot DESC, write_version DESC
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
    compute_non_circulating_audit(db, generation, &clock).await?;

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

async fn finalized_context_slot(db: &DatabaseConnection) -> Option<u64> {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT slot FROM slots WHERE commitment = $1",
            [(CommitmentLevel::Finalized as i32).into()],
        ))
        .await
        .ok()
        .flatten()?;
    let slot: i64 = row.try_get("", "slot").ok()?;
    slot.try_into().ok()
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
    clock: &Clock,
) -> anyhow::Result<()> {
    let context_slot = clock.slot;
    let static_accounts: HashSet<Pubkey> = non_circulating_accounts().into_iter().collect();
    let withdrawers: HashSet<Pubkey> = withdraw_authority().into_iter().collect();

    let mut lamports = 0u64;
    // Agave always returns every configured static non-circulating key in the
    // account list, even when its current balance is zero or the account is
    // absent. Only the lamport sum depends on the current account value.
    let mut account_count = static_accounts.len() as u64;
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
        let locked = meta.lockup.is_in_force(clock, None)
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
                 SELECT pubkey, slot, write_version, lamports FROM accounts WHERE slot <= {context_slot} \
                 UNION ALL SELECT pubkey, slot, write_version, lamports FROM snapshot_accounts WHERE slot <= {context_slot}\
                 ) versions WHERE pubkey IN (SELECT pubkey FROM requested) \
                 ORDER BY pubkey, slot DESC, write_version DESC) \
                 SELECT lamports FROM latest"
            ),
        ))
        .await?;
    for row in static_rows {
        let value: i64 = row.try_get("", "lamports")?;
        lamports = lamports
            .checked_add(value.try_into()?)
            .ok_or_else(|| anyhow::anyhow!("non-circulating sum overflow"))?;
    }

    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO stake_supply_audits (context_slot, generation, block_time, epoch, non_circulating_lamports, account_count, computed_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (context_slot) DO UPDATE SET generation = EXCLUDED.generation, block_time = EXCLUDED.block_time, epoch = EXCLUDED.epoch, non_circulating_lamports = EXCLUDED.non_circulating_lamports, account_count = EXCLUDED.account_count, computed_at_ms = EXCLUDED.computed_at_ms",
        [
            (context_slot as i64).into(), generation.into(), clock.unix_timestamp.into(), (clock.epoch as i64).into(),
            (lamports as i64).into(), (account_count as i64).into(), (now_ms() as i64).into(),
        ],
    )).await?;
    Ok(())
}

fn clock_at(slot: u64, unix_timestamp: i64) -> Clock {
    Clock {
        slot,
        epoch: EpochSchedule::default().get_epoch(slot),
        unix_timestamp,
        ..Clock::default()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Database;
    use std::str::FromStr;

    #[test]
    fn clock_uses_warmup_aware_epoch_and_supplied_timestamp() {
        let clock = clock_at(435_665_776, 1_788_040_186);

        assert_eq!(clock.slot, 435_665_776);
        assert_eq!(clock.epoch, EpochSchedule::default().get_epoch(clock.slot));
        assert_eq!(clock.unix_timestamp, 1_788_040_186);
    }

    #[tokio::test]
    #[ignore = "requires CLOUDBREAK_POSTGRES_DSN and a canonical local RPC"]
    async fn live_cached_supply_matches_raw_accounts_at_same_slot() {
        let database_url =
            std::env::var("CLOUDBREAK_POSTGRES_DSN").expect("CLOUDBREAK_POSTGRES_DSN must be set");
        let rpc_url = std::env::var("CLOUDBREAK_RPC_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());
        let db = Database::connect(database_url).await.unwrap();
        let snapshot = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT context_slot, payload FROM supply_snapshots WHERE commitment = 2"
                    .to_string(),
            ))
            .await
            .unwrap()
            .expect("canonical finalized supply snapshot");
        let context_slot: i64 = snapshot.try_get("", "context_slot").unwrap();
        let context_slot: u64 = context_slot.try_into().unwrap();
        let payload: String = snapshot.try_get("", "payload").unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
        let expected_lamports = payload["value"]["nonCirculating"].as_u64().unwrap();
        let expected_accounts = payload["value"]["nonCirculatingAccounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| Pubkey::from_str(value.as_str().unwrap()).unwrap())
            .collect::<HashSet<_>>();

        let rpc_client = RpcClient::new(rpc_url);
        let unix_timestamp = rpc_client.get_block_time(context_slot).await.unwrap();
        let clock = clock_at(context_slot, unix_timestamp);
        let (actual_lamports, actual_accounts) =
            calculate_raw_non_circulating(&db, &clock).await.unwrap();

        assert_eq!(actual_accounts, expected_accounts);
        assert_eq!(actual_lamports, expected_lamports);
    }

    async fn calculate_raw_non_circulating(
        db: &DatabaseConnection,
        clock: &Clock,
    ) -> anyhow::Result<(u64, HashSet<Pubkey>)> {
        let static_accounts: HashSet<Pubkey> = non_circulating_accounts().into_iter().collect();
        let withdrawers: HashSet<Pubkey> = withdraw_authority().into_iter().collect();
        let mut accounts = static_accounts.clone();
        let mut lamports = 0u64;
        let mut stream = db
            .stream(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                r#"
                SELECT pubkey, lamports, data
                FROM (
                    SELECT DISTINCT ON (pubkey)
                        pubkey, slot, write_version, lamports, data
                    FROM (
                        SELECT pubkey, slot, write_version, lamports, data
                        FROM accounts
                        WHERE owner = $1 AND slot <= $2
                        UNION ALL
                        SELECT pubkey, slot, write_version, lamports, data
                        FROM snapshot_accounts
                        WHERE owner = $1 AND slot <= $2
                    ) AS versions
                    ORDER BY pubkey, slot DESC, write_version DESC
                ) AS current_stake_accounts
                WHERE lamports > 0
                "#,
                [
                    STAKE_PROGRAM_ID.to_bytes().to_vec().into(),
                    (clock.slot as i64).into(),
                ],
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
            if meta.lockup.is_in_force(clock, None)
                || withdrawers.contains(&meta.authorized.withdrawer)
            {
                let value: i64 = row.try_get("", "lamports")?;
                lamports = lamports.checked_add(value.try_into()?).unwrap();
                accounts.insert(pubkey);
            }
        }
        drop(stream);

        let static_values = static_accounts
            .iter()
            .map(|key| format!("('\\x{}'::bytea)", hex::encode(key.as_ref())))
            .collect::<Vec<_>>()
            .join(",");
        let static_rows = db
            .query_all(Statement::from_string(
                DatabaseBackend::Postgres,
                format!(
                    "WITH requested(pubkey) AS (VALUES {static_values}), latest AS (\
                     SELECT DISTINCT ON (pubkey) pubkey, lamports FROM (\
                     SELECT pubkey, slot, write_version, lamports FROM accounts WHERE slot <= {} \
                     UNION ALL SELECT pubkey, slot, write_version, lamports FROM snapshot_accounts WHERE slot <= {}\
                     ) versions WHERE pubkey IN (SELECT pubkey FROM requested) \
                     ORDER BY pubkey, slot DESC, write_version DESC) \
                     SELECT lamports FROM latest",
                    clock.slot, clock.slot
                ),
            ))
            .await?;
        for row in static_rows {
            let value: i64 = row.try_get("", "lamports")?;
            lamports = lamports.checked_add(value.try_into()?).unwrap();
        }

        Ok((lamports, accounts))
    }
}
