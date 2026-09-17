# Upstream PR #48 Supply Tracker Review

Reviewed upstream commit: `3025a76`
Downstream supply implementation: `acdff81`

## Decision

Do not port PR #48, wholly or selectively, in the largest-accounts integration.

Cloudbreak already serves `getSupply` in constant time from a persisted finalized response sampled from a canonical local RPC. PR #48 replaces that single authority with an indexer-owned state machine, clears its persisted rows during indexer startup, and rebuilds from snapshot/live account data. Running both would create dual ownership; replacing the downstream cache would regress immediate restart availability, wall-clock staleness enforcement, and `minContextSlot` behavior.

## Coupled components rejected

- `crates/core/src/modules/supply/*`: native hot-account cache and supply state machine.
- `crates/core/src/modules/non_circulating/{persist,read,tracker}.rs`: changes GLA classification state, bootstrap ordering, expiry semantics, and memory use rather than providing an isolated supply fix.
- `crates/index/src/modules/save_block.rs`, `self_healing.rs`, and `db_queries.rs`: DB success propagation and capitalization plumbing exist to feed the new state machine.
- `crates/index/src/operational_endpoints/{supply,non_circulating}.rs`: controls only the rejected native trackers.
- `m20260717_000000_create_supply_tables`: persistence schema for the rejected authority.
- Upstream API `get_supply.rs` and `supply_cache.rs`: incompatible with the downstream persisted canonical response cache and its finalized-only contract.

## Preserved downstream contract

- One `getSupply` state authority: `crates/api/src/modules/supply_cache.rs`.
- Persisted last-known finalized response across API restarts.
- Configured request timeout, refresh interval, and wall-clock maximum staleness.
- Canonical response shape validation before persistence.
- Finalized-only serving, exclusion-list handling, and `minContextSlot` rejection.

The non-circulating tracker rewrite may be reconsidered as a dedicated feature with its own memory budget, bootstrap/restart proof, and GLA semantic comparison. It is not an independent patch suitable for this port.
