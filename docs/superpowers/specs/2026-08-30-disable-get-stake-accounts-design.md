# Remove `getStakeAccounts`

## Goal

Completely remove the experimental `getStakeAccounts` RPC and its full Stake projection rebuild until a new incremental design is implemented from scratch.

## Scope

- Stop spawning Cloudbreak's periodic Stake projection rebuilder.
- Remove `getStakeAccounts` from Cloudbreak's JSON-RPC dispatch surface.
- Remove `getStakeAccounts` from Sentinel's Cloudbreak-supported methods and public Agave RPC extension.
- Remove the benchmark case and method-specific comparison normalization.
- Drop only the derived Stake projection tables and indexes through a forward migration while retaining migration history.
- Delete the endpoint and worker implementation source.

## Runtime behavior

After deployment, neither Cloudbreak nor Sentinel advertises or serves `getStakeAccounts`; JSON-RPC returns method-not-found. No task rebuilds Stake projections, and only the derived projection tables are removed.

## Safety

- Do not raise Sentinel's Cloudbreak lag threshold.
- Do not alter raw account history or the canonical supply snapshot cache.
- Do not alter any other RPC method, route, or supply-audit schema.
- Keep `getSupply` on its independent canonical persisted cache.
- Verify Cloudbreak's finalizer queue and indexed-slot lag begin draining after the worker stops.

## Verification

- Run focused Cloudbreak and Sentinel format/check/test gates.
- Build release binaries.
- Deploy Cloudbreak API/indexer, Sentinel, and the benchmark site.
- Confirm `getStakeAccounts` returns method-not-found through both Cloudbreak and Sentinel.
- Confirm the benchmark definition no longer contains `getStakeAccounts`.
- Confirm no new Stake projection rebuild log entries appear after restart.
