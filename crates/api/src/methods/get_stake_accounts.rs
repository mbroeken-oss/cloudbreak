// SPDX-License-Identifier: AGPL-3.0-only
//! Bounded rooted discovery of Stake-program accounts.

use base64::{Engine, engine::general_purpose::STANDARD};
use cloudbreak_core::STAKE_PROGRAM_ID;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use serde::{Deserialize, Serialize};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};

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
    /// Base64-encoded stake account data. Consumers needing program-account
    /// wire compatibility should use getProgramAccounts(Stake111...).
    pub data: String,
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

    let (context_slot, _) = state
        .latest_slot_and_block_time(CommitmentLevel::Finalized)
        .await?;
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
            WITH latest AS (
                SELECT DISTINCT ON (pubkey) pubkey, lamports, data
                FROM (
                    SELECT pubkey, slot, lamports, data
                    FROM accounts
                    WHERE owner = $1 AND slot <= $2 AND pubkey > $3
                    UNION ALL
                    SELECT pubkey, slot, lamports, data
                    FROM snapshot_accounts
                    WHERE owner = $1 AND slot <= $2 AND pubkey > $3
                ) AS versions
                ORDER BY pubkey ASC, slot DESC
            )
            SELECT pubkey, lamports, data
            FROM latest
            WHERE lamports > 0
            ORDER BY pubkey ASC
            LIMIT $4
            "#,
            [
                STAKE_PROGRAM_ID.to_bytes().to_vec().into(),
                (context_slot as i64).into(),
                cursor.into(),
                (limit as i64 + 1).into(),
            ],
        ))
        .await
        .map_err(|error| {
            tracing::error!(%error, "getStakeAccounts query failed");
            RpcError::InternalError
        })?;

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
        accounts.push(StakeAccountSummary {
            pubkey: pubkey.to_string(),
            lamports: lamports.try_into().map_err(|_| RpcError::InternalError)?,
            data: STANDARD.encode(data),
        });
    }
    let has_more = accounts.len() == limit as usize
        && state
            .database
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                r#"
                WITH latest AS (
                    SELECT DISTINCT ON (pubkey) pubkey, lamports
                    FROM (
                        SELECT pubkey, slot, lamports FROM accounts
                        WHERE owner = $1 AND slot <= $2 AND pubkey > $3
                        UNION ALL
                        SELECT pubkey, slot, lamports FROM snapshot_accounts
                        WHERE owner = $1 AND slot <= $2 AND pubkey > $3
                    ) AS versions
                    ORDER BY pubkey ASC, slot DESC
                )
                SELECT pubkey FROM latest WHERE lamports > 0 ORDER BY pubkey ASC LIMIT 1
                "#,
                [
                    STAKE_PROGRAM_ID.to_bytes().to_vec().into(),
                    (context_slot as i64).into(),
                    last_pubkey
                        .map(|pubkey| pubkey.to_bytes().to_vec())
                        .unwrap_or_default()
                        .into(),
                ],
            ))
            .await
            .map_err(|_| RpcError::InternalError)?
            .is_some();

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
