// SPDX-License-Identifier: AGPL-3.0-only
//! Constant-time serving of a persisted canonical finalized `getSupply` cache.

use crate::{error::RpcError, http::CloudbreakRpcState, methods::resolve_commitment};
use serde::Deserialize;
use serde_json::Value;
use solana_commitment_config::CommitmentLevel;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetSupplyConfig {
    pub commitment: Option<CommitmentLevel>,
    pub min_context_slot: Option<u64>,
    pub exclude_non_circulating_accounts_list: Option<bool>,
}

pub async fn get_supply(
    state: &CloudbreakRpcState,
    config: Option<GetSupplyConfig>,
) -> Result<Value, RpcError> {
    let config = config.unwrap_or_default();
    let commitment = config
        .commitment
        .map(|value| resolve_commitment(value, state.processed_commitment))
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);
    if commitment != CommitmentLevel::Finalized {
        return Err(RpcError::InvalidParamsWithMessage(
            "getSupply is currently available only with finalized commitment".to_string(),
        ));
    }

    let snapshot = state
        .supply_cache
        .as_ref()
        .and_then(|cache| cache.latest_fresh())
        .ok_or(RpcError::SupplyCacheUnavailable)?;

    if let Some(min_context_slot) = config.min_context_slot
        && snapshot.context_slot < min_context_slot
    {
        return Err(RpcError::RpcSlotBehindMinContextSlot {
            rpc_slot: snapshot.context_slot,
        });
    }

    let mut result = snapshot.result;
    if config
        .exclude_non_circulating_accounts_list
        .unwrap_or(false)
        && let Some(accounts) = result
            .get_mut("value")
            .and_then(|value| value.get_mut("nonCirculatingAccounts"))
    {
        *accounts = Value::Array(Vec::new());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::GetSupplyConfig;

    #[test]
    fn parses_standard_supply_flags() {
        let config: GetSupplyConfig = serde_json::from_value(serde_json::json!({
            "commitment": "finalized",
            "minContextSlot": 7,
            "excludeNonCirculatingAccountsList": true
        }))
        .unwrap();
        assert_eq!(config.min_context_slot, Some(7));
        assert_eq!(config.exclude_non_circulating_accounts_list, Some(true));
    }
}
