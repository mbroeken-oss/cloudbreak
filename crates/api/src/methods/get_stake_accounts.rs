// SPDX-License-Identifier: AGPL-3.0-only
//! Bounded rooted discovery of materialized Stake-program accounts.

use base64::{Engine, engine::general_purpose::STANDARD};
use cloudbreak_core::STAKE_PROGRAM_ID;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use serde::{Deserialize, Serialize};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use solana_stake_interface::state::StakeStateV2;

use crate::{error::RpcError, http::CloudbreakRpcState, methods::resolve_commitment};

const DEFAULT_LIMIT: u16 = 100;
const MAX_LIMIT: u16 = 1_000;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetStakeAccountsConfig {
    pub commitment: Option<CommitmentLevel>,
    pub min_context_slot: Option<u64>,
    pub limit: Option<u16>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetStakeAccountsValue {
    pub accounts: Vec<StakeAccountSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StakeAccountSummary {
    pub pubkey: String,
    pub lamports: u64,
    /// `uninitialized`, `initialized`, `delegated`, `rewardsPool`, or `unknown`.
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staker_authority: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdraw_authority: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lockup: Option<StakeLockup>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegation: Option<StakeDelegation>,
    /// Base64-encoded stake account data. Consumers needing program-account
    /// wire compatibility should use getProgramAccounts(Stake111...).
    pub data: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StakeLockup {
    pub unix_timestamp: i64,
    pub epoch: u64,
    pub custodian: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StakeDelegation {
    pub vote_pubkey: String,
    pub stake: u64,
    pub activation_epoch: u64,
    pub deactivation_epoch: u64,
}

pub async fn get_stake_accounts(
    state: &CloudbreakRpcState,
    config: Option<GetStakeAccountsConfig>,
) -> Result<RpcResponse<GetStakeAccountsValue>, RpcError> {
    if !state.indexer_filter.is_program_selected(&STAKE_PROGRAM_ID) {
        return Err(RpcError::KeyExcludedFromSecondaryIndex {
            key: STAKE_PROGRAM_ID.to_string(),
        });
    }

    let config = config.unwrap_or_default();
    let commitment = config
        .commitment
        .map(|value| resolve_commitment(value, state.processed_commitment))
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);
    if commitment != CommitmentLevel::Finalized {
        return Err(RpcError::InvalidParamsWithMessage(
            "getStakeAccounts is currently available only with finalized commitment".to_string(),
        ));
    }

    let projection = state
        .database
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT generation, context_slot FROM stake_projection_status WHERE id = 1".to_string(),
        ))
        .await
        .map_err(|error| {
            tracing::error!(%error, "getStakeAccounts projection status query failed");
            RpcError::InternalError
        })?
        .ok_or_else(|| state.node_unhealthy())?;
    let active_generation: i64 = projection
        .try_get("", "generation")
        .map_err(|_| RpcError::InternalError)?;
    let active_context_slot: i64 = projection
        .try_get("", "context_slot")
        .map_err(|_| RpcError::InternalError)?;
    let active_context_slot: u64 = active_context_slot
        .try_into()
        .map_err(|_| RpcError::InternalError)?;
    let cursor = decode_cursor(config.cursor.as_deref())?;
    let (generation, context_slot, cursor_pubkey) = match cursor {
        Some(cursor) => {
            if cursor.generation > active_generation || cursor.generation < active_generation - 1 {
                return Err(expired_cursor_error());
            }
            (
                cursor.generation,
                cursor.context_slot,
                cursor.pubkey.to_bytes().to_vec(),
            )
        }
        None => (active_generation, active_context_slot, Vec::new()),
    };

    if let Some(min_context_slot) = config.min_context_slot
        && context_slot < min_context_slot
    {
        return Err(RpcError::RpcSlotBehindMinContextSlot {
            rpc_slot: context_slot,
        });
    }

    let limit = config.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 || limit > MAX_LIMIT {
        return Err(RpcError::InvalidParamsWithMessage(format!(
            "limit must be between 1 and {MAX_LIMIT}"
        )));
    }
    let generation_exists = state
        .database
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT 1 FROM stake_accounts_current WHERE generation = $1 LIMIT 1",
            [generation.into()],
        ))
        .await
        .map_err(|error| {
            tracing::error!(%error, "getStakeAccounts projection generation query failed");
            RpcError::InternalError
        })?
        .is_some();
    if !generation_exists {
        return Err(expired_cursor_error());
    }

    let rows = state
        .database
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            r#"
            SELECT pubkey, lamports, data
            FROM stake_accounts_current
            WHERE generation = $1 AND pubkey > $2
            ORDER BY pubkey ASC
            LIMIT $3
            "#,
            [
                generation.into(),
                cursor_pubkey.into(),
                (limit as i64 + 1).into(),
            ],
        ))
        .await
        .map_err(|error| {
            tracing::error!(%error, "getStakeAccounts projection page query failed");
            RpcError::InternalError
        })?;

    let has_more = rows.len() > limit as usize;
    let mut accounts = Vec::with_capacity(limit as usize);
    let mut last_pubkey = None;
    for row in rows.into_iter().take(limit as usize) {
        let pubkey_bytes: Vec<u8> = row
            .try_get("", "pubkey")
            .map_err(|_| RpcError::InternalError)?;
        let lamports: i64 = row
            .try_get("", "lamports")
            .map_err(|_| RpcError::InternalError)?;
        let data: Vec<u8> = row
            .try_get("", "data")
            .map_err(|_| RpcError::InternalError)?;
        let pubkey =
            Pubkey::try_from(pubkey_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;
        last_pubkey = Some(pubkey);
        accounts.push(summarize_account(
            pubkey,
            lamports.try_into().map_err(|_| RpcError::InternalError)?,
            data,
        ));
    }

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: context_slot,
            api_version: None,
        },
        value: GetStakeAccountsValue {
            accounts,
            next_cursor: has_more.then(|| {
                encode_cursor(StakeCursor {
                    generation,
                    context_slot,
                    pubkey: last_pubkey.expect("page has entries"),
                })
            }),
        },
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StakeCursor {
    generation: i64,
    context_slot: u64,
    pubkey: Pubkey,
}

fn decode_cursor(cursor: Option<&str>) -> Result<Option<StakeCursor>, RpcError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let mut parts = cursor.split(':');
    let (Some("v1"), Some(generation), Some(context_slot), Some(pubkey), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(RpcError::InvalidParamsWithMessage(
            "invalid cursor".to_string(),
        ));
    };
    let generation = generation
        .parse::<i64>()
        .ok()
        .filter(|generation| *generation > 0)
        .ok_or_else(|| RpcError::InvalidParamsWithMessage("invalid cursor".to_string()))?;
    let context_slot = context_slot
        .parse::<u64>()
        .map_err(|_| RpcError::InvalidParamsWithMessage("invalid cursor".to_string()))?;
    let bytes = bs58::decode(pubkey)
        .into_vec()
        .map_err(|_| RpcError::InvalidParamsWithMessage("invalid cursor".to_string()))?;
    let pubkey = Pubkey::try_from(bytes.as_slice())
        .map_err(|_| RpcError::InvalidParamsWithMessage("invalid cursor".to_string()))?;
    Ok(Some(StakeCursor {
        generation,
        context_slot,
        pubkey,
    }))
}

fn encode_cursor(cursor: StakeCursor) -> String {
    format!(
        "v1:{}:{}:{}",
        cursor.generation,
        cursor.context_slot,
        bs58::encode(cursor.pubkey.as_ref()).into_string()
    )
}

fn expired_cursor_error() -> RpcError {
    RpcError::InvalidParamsWithMessage(
        "cursor has expired; restart pagination without a cursor".to_string(),
    )
}

fn summarize_account(pubkey: Pubkey, lamports: u64, data: Vec<u8>) -> StakeAccountSummary {
    let mut summary = StakeAccountSummary {
        pubkey: pubkey.to_string(),
        lamports,
        state: "unknown".to_string(),
        staker_authority: None,
        withdraw_authority: None,
        lockup: None,
        delegation: None,
        data: STANDARD.encode(&data),
    };

    let Ok(stake_state) = bincode::deserialize::<StakeStateV2>(&data) else {
        return summary;
    };
    match stake_state {
        StakeStateV2::Uninitialized => summary.state = "uninitialized".to_string(),
        StakeStateV2::RewardsPool => summary.state = "rewardsPool".to_string(),
        StakeStateV2::Initialized(meta) => {
            summary.state = "initialized".to_string();
            populate_meta(&mut summary, meta);
        }
        StakeStateV2::Stake(meta, stake, _) => {
            summary.state = "delegated".to_string();
            populate_meta(&mut summary, meta);
            summary.delegation = Some(StakeDelegation {
                vote_pubkey: stake.delegation.voter_pubkey.to_string(),
                stake: stake.delegation.stake,
                activation_epoch: stake.delegation.activation_epoch,
                deactivation_epoch: stake.delegation.deactivation_epoch,
            });
        }
    }
    summary
}

fn populate_meta(summary: &mut StakeAccountSummary, meta: solana_stake_interface::state::Meta) {
    summary.staker_authority = Some(meta.authorized.staker.to_string());
    summary.withdraw_authority = Some(meta.authorized.withdrawer.to_string());
    summary.lockup = Some(StakeLockup {
        unix_timestamp: meta.lockup.unix_timestamp,
        epoch: meta.lockup.epoch,
        custodian: meta.lockup.custodian.to_string(),
    });
}

#[cfg(test)]
mod tests {
    use super::{StakeCursor, decode_cursor, encode_cursor, summarize_account};
    use solana_pubkey::Pubkey;
    use solana_stake_interface::state::{Meta, StakeStateV2};

    #[test]
    fn cursor_round_trip() {
        let cursor = StakeCursor {
            generation: 7,
            context_slot: 42,
            pubkey: Pubkey::new_unique(),
        };
        assert_eq!(
            decode_cursor(Some(&encode_cursor(cursor))).unwrap(),
            Some(cursor)
        );
    }

    #[test]
    fn rejects_legacy_or_malformed_cursor() {
        assert!(decode_cursor(Some(&Pubkey::new_unique().to_string())).is_err());
        assert!(decode_cursor(Some("v1:0:42:not-a-pubkey")).is_err());
    }

    #[test]
    fn summarizes_initialized_stake_metadata() {
        let account = Pubkey::new_unique();
        let staker = Pubkey::new_unique();
        let withdrawer = Pubkey::new_unique();
        let custodian = Pubkey::new_unique();
        let mut meta = Meta::auto(&staker);
        meta.authorized.withdrawer = withdrawer;
        meta.lockup.unix_timestamp = 123;
        meta.lockup.epoch = 456;
        meta.lockup.custodian = custodian;

        let summary = summarize_account(
            account,
            789,
            bincode::serialize(&StakeStateV2::Initialized(meta)).unwrap(),
        );

        assert_eq!(summary.pubkey, account.to_string());
        assert_eq!(summary.lamports, 789);
        assert_eq!(summary.state, "initialized");
        let staker = staker.to_string();
        let withdrawer = withdrawer.to_string();
        assert_eq!(summary.staker_authority.as_deref(), Some(staker.as_str()));
        assert_eq!(
            summary.withdraw_authority.as_deref(),
            Some(withdrawer.as_str())
        );
        let lockup = summary.lockup.unwrap();
        assert_eq!(lockup.unix_timestamp, 123);
        assert_eq!(lockup.epoch, 456);
        assert_eq!(lockup.custodian, custodian.to_string());
        assert!(summary.delegation.is_none());
    }
}
