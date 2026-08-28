# Cloudbreak stake accounts and cached SOL supply — design

**Date:** 2026-08-28

## Decision

Cloudbreak will add two separate capabilities:

1. **`getStakeAccounts`** — a Cloudbreak extension backed by a durable, rooted projection of Stake-program accounts.
2. **`getSupply`** — a standard Solana-shaped, persisted cache of a canonical finalized `getSupply` response. Client reads must never trigger the expensive calculation.

The initial `getSupply` implementation deliberately **does not derive chain-wide total supply from Cloudbreak's account tables**. Cloudbreak indexes selected program accounts, while exact capitalization requires full account state. A correct native total would need a second, global all-account lamport index or a new canonical capitalization feed. Both are materially larger projects.

Instead, the cache stores an exact canonical response at a known finalized slot. This is the smallest correct way to remove repeated heavy public RPC calls now.

## Goals

- Make public `getSupply` requests O(1) database/cache reads.
- Preserve Solana response shape, `minContextSlot`, and `excludeNonCirculatingAccountsList` behavior.
- Keep the cache rooted and explicit about its context slot.
- Add paginated stake-account discovery without forcing unbounded `getProgramAccounts(Stake...)` responses.
- Reuse Cloudbreak's selected-program indexer model; require Stake-program coverage explicitly.
- Leave Sentinel's live Cloudbreak routing disabled until API and parity verification pass.

## Non-goals

- No public routing change in Sentinel in this slice.
- No attempt to calculate total SOL capitalization from Cloudbreak's selected-program database.
- No synchronous scan of all stake accounts from a request handler.
- No unbounded custom stake-account response.
- No processing commitment support for the rooted cache in the first slice.

## Constraints discovered in the current codebase

- Cloudbreak persists only accounts selected by `AccountSelectorConfig`; the indexer filters both snapshot and live updates before inserting account data.
- The API already has a finalized/confirmed slot synchronizer and a `getVoteAccounts` background-cache pattern.
- Agave's `getSupply` computes non-circulating supply on each request. Its ProgramId account index narrows candidates to Stake accounts, but it still deserializes and evaluates every candidate.
- Cloudbreak already depends on `reqwest`, so the refresh worker needs no new HTTP client dependency.

## Architecture

### A. Stake account projection

Add the Stake program to the Cloudbreak indexer's selected programs. A snapshot rebuild is required whenever the existing database does not contain Stake-program accounts; changing the filter alone cannot backfill past account state.

Create `stake_accounts_current`, one row per latest rooted Stake account:

- `pubkey` (primary key)
- `slot`
- `lamports`
- `state` (`uninitialized`, `initialized`, `delegated`, or `unknown`)
- `withdraw_authority`
- `lockup_unix_timestamp`
- `lockup_epoch`
- `lockup_custodian`
- delegation fields when present: `vote_pubkey`, `delegated_stake`, `activation_epoch`, `deactivation_epoch`
- `updated_at`

The projection is populated from both snapshot ingestion and finalized live slot updates. It is updated only after the corresponding account version becomes rooted, and a zero-lamport/closed Stake account removes the row. Invalid or unsupported account data must be stored as `unknown` rather than crashing the indexer; a metric records decode failures.

Indexes:

- primary key `pubkey`
- `(slot DESC, pubkey)` for rooted pagination
- `(state, pubkey)`
- `(vote_pubkey, pubkey)` where non-null
- `(withdraw_authority, pubkey)` where non-null
- lockup filter index only after query measurements show it is needed

The raw account tables remain the source for encoded account data. The projection is an API/search structure and future input to a non-circulating-supply materialization.

### Implemented projection slice

The first implementation materializes a **generationed rooted copy** of the latest Stake-account rows (`pubkey`, `slot`, `lamports`, `data`) rather than parsing every field into columns. The indexer builds a complete new generation from accounts at or below the finalized slot, atomically points `stake_projection_status` at it, and retains the preceding generation for in-flight pagination safety. `getStakeAccounts` reads that projection with keyset pagination. Decoded state/authority/lockup/delegation columns and the incremental non-circulating aggregate remain the next slice.

### B. `getStakeAccounts` extension

`getStakeAccounts` is a Cloudbreak extension; Solana-compatible callers should continue to use `getProgramAccounts` for Stake-program semantics.

Request config:

```json
{
  "commitment": "finalized",
  "minContextSlot": 123,
  "limit": 100,
  "cursor": "opaque-last-pubkey",
  "state": "delegated",
  "votePubkey": "...",
  "withdrawAuthority": "...",
  "lockupActive": true
}
```

Response:

```json
{
  "context": {"slot": 123},
  "value": {
    "accounts": [
      {
        "pubkey": "...",
        "lamports": 1,
        "state": "delegated",
        "withdrawAuthority": "...",
        "lockup": {"unixTimestamp": "0", "epoch": "0", "custodian": "..."},
        "delegation": {"votePubkey": "...", "stake": "1", "activationEpoch": "1", "deactivationEpoch": "18446744073709551615"}
      }
    ],
    "nextCursor": "..."
  }
}
```

Rules:

- Finalized only in the first slice; processed remains subject to the existing configured policy and confirmed is rejected rather than returning an ambiguous rooted projection.
- Default limit 100; cap 1,000.
- Cursor is a base64url-encoded versioned keyset cursor containing the last `pubkey` and the projection context slot. It is opaque to clients.
- `minContextSlot` returns the existing slot-behind error when the rooted projection is older than the request.
- The API returns `NODE_UNHEALTHY` until the Stake projection has completed its initial snapshot/rooted bootstrap.

### C. Canonical `getSupply` cache

Create `supply_snapshots`:

- `commitment` (initially `finalized` only)
- `context_slot` (unique with commitment)
- `total`, `circulating`, `non_circulating`
- `non_circulating_accounts` JSONB
- `sampled_at`
- `source_latency_ms`
- `source_name`
- `response_hash`

Add optional `[supply-cache]` API configuration, disabled by default:

- `source-url`: local/private canonical RPC endpoint
- `refresh-interval-ms`: default 60,000
- `request-timeout-ms`
- `max-staleness-ms`
- `initial-load-timeout-ms`

On startup, the worker loads the newest valid persisted finalized sample. When enabled, a single background task:

1. Requests canonical `getSupply` with finalized commitment and `excludeNonCirculatingAccountsList=false`.
2. Validates JSON-RPC success, response shape, non-negative fields, account strings, and context slot.
3. Refuses to replace a cache entry with an older context slot.
4. Persists the validated sample transactionally, updates the in-memory latest snapshot, and records metrics.
5. Uses bounded exponential retry after errors; only one refresh is ever in flight.

The request handler only reads the in-memory/persisted latest valid sample. It never contacts the source RPC.

`getSupply` behavior:

- Accept standard `RpcSupplyConfig` fields used by the cache: `commitment`, `minContextSlot`, `excludeNonCirculatingAccountsList`.
- Serve only finalized in the first slice. Confirmed/processed return a clear invalid-params error.
- If the snapshot is absent, stale beyond configured max age, unhealthy, or below `minContextSlot`, return an explicit RPC error instead of a guessed value.
- When `excludeNonCirculatingAccountsList=true`, remove the account list from the stored response at serialization time; do not make a second source request.

### D. Future native supply materialization

The Stake projection enables a future incremental non-circulating aggregate, including recomputation at lockup expiry boundaries. It does not by itself provide total capitalization.

A native Cloudbreak `getSupply` implementation requires one of:

1. a versioned internal Sentinel feed that publishes `(root_slot, capitalization)`; or
2. a global Cloudbreak balance index that tracks every live account's lamports through snapshots and updates.

The first is strongly preferred. It avoids duplicating a large all-account database solely to maintain one aggregate.

### Implemented rooted audit

Cloudbreak now computes and persists a rooted `stake_supply_audits` record each time it publishes a Stake projection generation. It uses Agave's current non-circulating static-account and withdrawal-authority lists, evaluates lockups against the projection's finalized block time and epoch, and records a local non-circulating lamport total plus account count. This is an audit/proof input only: public `getSupply` continues to serve the exact canonical cache, rather than combining independently sampled totals and components from different slots.

## Data-flow and lifecycle

1. Operator adds the Stake program to Cloudbreak's indexed programs and completes a snapshot rebuild.
2. Snapshot ingestion projects rooted stake records; live finalized slot processing maintains them.
3. The API exposes bounded, rooted stake discovery after bootstrap.
4. The supply worker periodically obtains exactly one canonical finalized response and persists it.
5. Public `getSupply` reads are local and constant-time.
6. Sentinel can later shadow this `getSupply` response, comparing only equal-or-newer finalized context slots. Live route enablement remains a separate approval gate.

## Failure handling

- Source unavailable: retain last known sample only until `max-staleness-ms`, then fail closed with an observable error.
- Source returns malformed/older response: reject it and retain the latest valid snapshot.
- Database unavailable: return existing in-memory sample only while it remains fresh; refresh remains failed and metrics expose it.
- Stake data absent from the indexer filter: methods report unsupported/unhealthy rather than partial results.
- Cursor invalid or context mismatch: invalid parameters.
- Snapshot/live projection decode failure: count and log it; do not corrupt aggregate state or stop indexing.

## Metrics and operational visibility

Add:

- `cloudbreak_supply_cache_refresh_total{outcome}`
- `cloudbreak_supply_cache_age_seconds`
- `cloudbreak_supply_cache_context_slot`
- `cloudbreak_supply_cache_source_latency_seconds`
- `cloudbreak_stake_projection_rows`
- `cloudbreak_stake_projection_decode_failures_total`
- `cloudbreak_get_stake_accounts_requests_total{outcome}`

Expose cache freshness and context slot through the existing health/operational surface, without putting source URLs or credentials in output.

## Tests and verification

Unit tests:

- stake state parsing for initialized, delegated, locked, closed, malformed, and unknown payloads
- pagination ordering/cursor validation/filter combinations
- supply response parsing, stale/older source rejection, excluded account-list serialization, and min-context behavior
- config validation and default-disabled behavior

Integration tests:

- snapshot + finalized live-update projection lifecycle
- API fixture returns a page of stake accounts at a known rooted slot
- cache worker accepts a mock canonical RPC response and serves it without a second mock request
- cache worker failure never replaces a newer sample
- standard `getSupply` response shape matches canonical fixture exactly at the cached slot

Operational gate:

- migration applies cleanly to an empty database and an upgraded database
- existing API methods remain green
- Cloudbreak remains healthy with `[supply-cache]` omitted
- service configuration and Sentinel routing are not changed by this code slice

## Rollout and rollback

1. Deploy code with both features disabled by default.
2. Apply migration.
3. Add Stake program to a test configuration and rebuild the Cloudbreak index.
4. Enable and validate `getStakeAccounts` in shadow/test traffic.
5. Configure the private canonical supply source, validate cache freshness/parity, then enable public `getSupply` on Cloudbreak.
6. Only after evidence, consider Sentinel shadow and route configuration.

Rollback is configuration-first: disable `[supply-cache]` and the stake endpoint feature flag; leave the projection and cache tables intact for forensics. No live Sentinel route changes are included here.
