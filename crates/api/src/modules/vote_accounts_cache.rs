use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use solana_pubkey::Pubkey;
use tokio::task::JoinHandle;

/// Per-epoch stake snapshot extracted from the indexer's `epoch_stakes` table.
#[derive(Debug, Clone)]
pub struct StakesSnapshot {
    pub epoch: u64,
    pub voters: HashMap<Pubkey, VoterEntry>,
}

#[derive(Debug, Clone)]
pub struct VoterEntry {
    pub node_pubkey: Pubkey,
    pub activated_stake: u64,
    pub in_epoch_set: bool,
}

impl StakesSnapshot {
    pub fn empty() -> Self {
        Self {
            epoch: 0,
            voters: HashMap::new(),
        }
    }
}

pub type SharedStakesSnapshot = Arc<RwLock<Arc<StakesSnapshot>>>;

const STAKES_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Load the latest epoch's stake rows from the `epoch_stakes` table.
/// Returns `None` if the table is empty (the indexer hasn't processed a
/// snapshot with stake data yet).
pub async fn load_latest_stakes(
    db: &DatabaseConnection,
) -> Result<Option<StakesSnapshot>, anyhow::Error> {
    let max_epoch_row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT MAX(epoch) AS epoch FROM epoch_stakes".to_string(),
        ))
        .await?;

    let Some(row) = max_epoch_row else {
        return Ok(None);
    };
    let epoch: Option<i64> = row.try_get("", "epoch").ok();
    let Some(epoch) = epoch else {
        return Ok(None);
    };

    let rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT vote_pubkey, node_pubkey, activated_stake, in_epoch_set \
             FROM epoch_stakes WHERE epoch = $1",
            [epoch.into()],
        ))
        .await?;

    let mut voters = HashMap::with_capacity(rows.len());
    for row in rows {
        let vote_pubkey: Vec<u8> = row.try_get("", "vote_pubkey")?;
        let node_pubkey: Vec<u8> = row.try_get("", "node_pubkey")?;
        let activated_stake: i64 = row.try_get("", "activated_stake")?;
        let in_epoch_set: bool = row.try_get("", "in_epoch_set")?;

        let vote_pubkey = Pubkey::try_from(vote_pubkey.as_slice())
            .map_err(|_| anyhow::anyhow!("invalid vote_pubkey length in epoch_stakes"))?;
        let node_pubkey = Pubkey::try_from(node_pubkey.as_slice())
            .map_err(|_| anyhow::anyhow!("invalid node_pubkey length in epoch_stakes"))?;

        voters.insert(
            vote_pubkey,
            VoterEntry {
                node_pubkey,
                activated_stake: activated_stake as u64,
                in_epoch_set,
            },
        );
    }

    Ok(Some(StakesSnapshot {
        epoch: epoch as u64,
        voters,
    }))
}

/// Spawn a background task that polls `epoch_stakes` on a fixed interval and
/// writes a new `StakesSnapshot` into the shared cell when the latest epoch in
/// the DB is newer than the cached one, OR when the current epoch's stake total
/// has changed.
pub fn spawn_poll_task(db: DatabaseConnection, cache: SharedStakesSnapshot) -> JoinHandle<()> {
    fn total_stake(snapshot: &StakesSnapshot) -> u128 {
        snapshot
            .voters
            .values()
            .map(|v| v.activated_stake as u128)
            .sum()
    }

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(STAKES_POLL_INTERVAL).await;

            match load_latest_stakes(&db).await {
                Ok(Some(snapshot)) => {
                    let (current_epoch, current_total) = {
                        let current = cache.read().unwrap();
                        (current.epoch, total_stake(&current))
                    };
                    let new_total = total_stake(&snapshot);
                    if snapshot.epoch > current_epoch {
                        tracing::info!(
                            target: "vote_accounts_cache",
                            "refreshing stakes cache: epoch {} -> {} ({} voters, total {})",
                            current_epoch,
                            snapshot.epoch,
                            snapshot.voters.len(),
                            new_total
                        );
                        *cache.write().unwrap() = Arc::new(snapshot);
                    } else if snapshot.epoch == current_epoch && new_total != current_total {
                        tracing::info!(
                            target: "vote_accounts_cache",
                            "refreshing in-epoch stakes for epoch {}: total {} -> {} ({} voters)",
                            snapshot.epoch,
                            current_total,
                            new_total,
                            snapshot.voters.len()
                        );
                        *cache.write().unwrap() = Arc::new(snapshot);
                    }
                }
                Ok(None) => {
                    tracing::debug!(
                        target: "vote_accounts_cache",
                        "epoch_stakes table empty; will retry"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        target: "vote_accounts_cache",
                        "failed to load latest stakes: {:?}",
                        e
                    );
                }
            }
        }
    })
}
