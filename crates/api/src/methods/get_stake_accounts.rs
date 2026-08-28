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
    let generation: i64 = projection
        .try_get("", "generation")
        .map_err(|_| RpcError::InternalError)?;
    let context_slot: i64 = projection
        .try_get("", "context_slot")
        .map_err(|_| RpcError::InternalError)?;
    let context_slot: u64 = context_slot
        .try_into()
        .map_err(|_| RpcError::InternalError)?;

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
    let cursor = decode_cursor(config.cursor.as_deref())?;
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
            [generation.into(), cursor.into(), (limit as i64 + 1).into()],
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
            next_cursor: has_more.then(|| encode_cursor(last_pubkey.expect("page has entries"))),
        },
    })
}

fn decode_cursor(cursor: Option<&str>) -> Result<Vec<u8>, RpcError> {
    let Some(cursor) = cursor else {
        return Ok(Vec::new());
    };
    let bytes = bs58::decode(cursor)
        .into_vec()
        .map_err(|_| RpcError::InvalidParamsWithMessage("invalid cursor".to_string()))?;
    if bytes.len() != 32 {
        return Err(RpcError::InvalidParamsWithMessage(
            "invalid cursor".to_string(),
        ));
    }
    Ok(bytes)
}

fn encode_cursor(pubkey: Pubkey) -> String {
    bs58::encode(pubkey.as_ref()).into_string()
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
    use super::{decode_cursor, encode_cursor};
    use solana_pubkey::Pubkey;

    #[test]
    fn cursor_round_trip() {
        let key = Pubkey::new_unique();
        assert_eq!(
            decode_cursor(Some(&encode_cursor(key))).unwrap(),
            key.to_bytes()
        );
    }
}
