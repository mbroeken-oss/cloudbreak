// SPDX-License-Identifier: AGPL-3.0-only
//! Persisted, single-flight cache for canonical finalized `getSupply` results.

use anyhow::{Context, Result};
use cloudbreak_core::SupplyCacheConfig;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use serde_json::{Value, json};
use std::{
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::sleep;

#[derive(Clone, Debug)]
pub struct SupplySnapshot {
    pub result: Value,
    pub context_slot: u64,
    pub sampled_at_ms: u64,
}

pub struct SupplyCache {
    latest: RwLock<Option<SupplySnapshot>>,
    max_staleness: Duration,
}

pub type SharedSupplyCache = Arc<SupplyCache>;

impl SupplyCache {
    pub fn latest_fresh(&self) -> Option<SupplySnapshot> {
        let snapshot = self.latest.read().ok()?.clone()?;
        let now = now_ms();
        if now.saturating_sub(snapshot.sampled_at_ms) > self.max_staleness.as_millis() as u64 {
            return None;
        }
        Some(snapshot)
    }

    fn replace(&self, snapshot: SupplySnapshot) {
        if let Ok(mut latest) = self.latest.write() {
            let should_replace = latest
                .as_ref()
                .map(|current| snapshot.context_slot >= current.context_slot)
                .unwrap_or(true);
            if should_replace {
                *latest = Some(snapshot);
            }
        }
    }
}

pub async fn start(
    database: DatabaseConnection,
    config: SupplyCacheConfig,
) -> Result<SharedSupplyCache> {
    let cache = Arc::new(SupplyCache {
        latest: RwLock::new(load_latest(&database).await?),
        max_staleness: Duration::from_millis(config.max_staleness_ms),
    });
    let worker_cache = Arc::clone(&cache);
    tokio::spawn(async move { refresh_loop(database, config, worker_cache).await });
    Ok(cache)
}

async fn refresh_loop(
    database: DatabaseConnection,
    config: SupplyCacheConfig,
    cache: SharedSupplyCache,
) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(%error, "failed to build supply-cache HTTP client");
            return;
        }
    };

    loop {
        match fetch_snapshot(&client, &config.source_url).await {
            Ok((snapshot, latency_ms)) => {
                if let Err(error) = persist(&database, &snapshot, latency_ms).await {
                    tracing::error!(%error, "failed to persist supply-cache snapshot");
                } else {
                    cache.replace(snapshot);
                }
            }
            Err(error) => tracing::warn!(%error, "supply-cache refresh failed"),
        }
        sleep(Duration::from_millis(config.refresh_interval_ms)).await;
    }
}

async fn fetch_snapshot(
    client: &reqwest::Client,
    source_url: &str,
) -> Result<(SupplySnapshot, u64)> {
    let started = std::time::Instant::now();
    let response = client
        .post(source_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSupply",
            "params": [{
                "commitment": "finalized",
                "excludeNonCirculatingAccountsList": false
            }]
        }))
        .send()
        .await
        .context("request canonical getSupply")?
        .error_for_status()
        .context("canonical getSupply HTTP status")?;
    let body: Value = response
        .json()
        .await
        .context("decode canonical getSupply JSON")?;
    let result = body
        .get("result")
        .cloned()
        .context("canonical getSupply returned no result")?;
    let context_slot = validate_result(&result)?;
    Ok((
        SupplySnapshot {
            result,
            context_slot,
            sampled_at_ms: now_ms(),
        },
        started.elapsed().as_millis() as u64,
    ))
}

fn validate_result(result: &Value) -> Result<u64> {
    let context_slot = result
        .get("context")
        .and_then(|context| context.get("slot"))
        .and_then(Value::as_u64)
        .context("canonical getSupply result has no numeric context.slot")?;
    let value = result
        .get("value")
        .context("canonical getSupply result has no value")?;
    for field in ["total", "circulating", "nonCirculating"] {
        value
            .get(field)
            .and_then(Value::as_u64)
            .with_context(|| format!("canonical getSupply result has no numeric value.{field}"))?;
    }
    let accounts = value
        .get("nonCirculatingAccounts")
        .and_then(Value::as_array)
        .context("canonical getSupply result has no nonCirculatingAccounts array")?;
    if !accounts.iter().all(Value::is_string) {
        anyhow::bail!("canonical getSupply has a non-string nonCirculatingAccounts entry");
    }
    Ok(context_slot)
}

async fn load_latest(database: &DatabaseConnection) -> Result<Option<SupplySnapshot>> {
    let row = database
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT context_slot, payload, sampled_at_ms FROM supply_snapshots WHERE commitment = 2",
        ))
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let context_slot: i64 = row.try_get("", "context_slot")?;
    let payload: String = row.try_get("", "payload")?;
    let sampled_at_ms: i64 = row.try_get("", "sampled_at_ms")?;
    let result: Value =
        serde_json::from_str(&payload).context("decode persisted supply snapshot")?;
    validate_result(&result)?;
    Ok(Some(SupplySnapshot {
        result,
        context_slot: context_slot
            .try_into()
            .context("negative supply context slot")?,
        sampled_at_ms: sampled_at_ms
            .try_into()
            .context("negative supply sample time")?,
    }))
}

async fn persist(
    database: &DatabaseConnection,
    snapshot: &SupplySnapshot,
    source_latency_ms: u64,
) -> Result<()> {
    let payload = serde_json::to_string(&snapshot.result)?;
    database
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            r#"
            INSERT INTO supply_snapshots
                (commitment, context_slot, payload, sampled_at_ms, source_latency_ms)
            VALUES (2, $1, $2, $3, $4)
            ON CONFLICT (commitment) DO UPDATE SET
                context_slot = EXCLUDED.context_slot,
                payload = EXCLUDED.payload,
                sampled_at_ms = EXCLUDED.sampled_at_ms,
                source_latency_ms = EXCLUDED.source_latency_ms
            WHERE supply_snapshots.context_slot <= EXCLUDED.context_slot
            "#,
            [
                (snapshot.context_slot as i64).into(),
                payload.into(),
                (snapshot.sampled_at_ms as i64).into(),
                (source_latency_ms as i64).into(),
            ],
        ))
        .await?;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::validate_result;
    use serde_json::json;

    #[test]
    fn accepts_canonical_supply_shape() {
        assert_eq!(
            validate_result(&json!({
                "context": {"slot": 42},
                "value": {
                    "total": 10,
                    "circulating": 8,
                    "nonCirculating": 2,
                    "nonCirculatingAccounts": ["abc"]
                }
            }))
            .unwrap(),
            42
        );
    }

    #[test]
    fn rejects_malformed_supply_shape() {
        assert!(validate_result(&json!({"context": {"slot": 42}, "value": {}})).is_err());
    }
}
