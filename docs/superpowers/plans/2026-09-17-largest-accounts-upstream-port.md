# Largest Accounts Upstream Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port upstream Cloudbreak PRs #37 and #43 so `getLargestAccounts` is available and `getTokenLargestAccounts` uses persisted top-account records, then review PR #48 against the existing canonical local-RPC supply cache without replacing a working subsystem blindly.

**Architecture:** Merge upstream through commit `2ae3cd1`, preserving downstream health, snapshot, migration, and API state additions while adopting upstream's split `largest-accounts` and `token-largest-accounts` trackers. Keep downstream `getSupply` as the serving authority unless a file-by-file review of `3025a76` proves a compatible, independently valuable subset; document that decision in the resulting commit history and verification evidence.

**Tech Stack:** Rust, Tokio, SeaORM/sqlx, PostgreSQL, Solana JSON-RPC types, Cargo.

**Spec:** User request in Telegram message `44300`; upstream commits `2dfa7dc`, `2ae3cd1`, and review target `3025a76`.

## Global Constraints

- Work only in the canonical `/opt/cloudbreak` checkout on branch `main`.
- Preserve all downstream commits after shared base `22564b9`.
- Preserve the existing canonical local-RPC `getSupply` cache and its migration unless the #48 review finds a demonstrably compatible improvement.
- Preserve downstream health, snapshot ingestion, query-tracker, and stake-audit behavior when resolving conflicts.
- Run fresh formatting, compilation, and tests before committing or pushing.

---

### Task 1: Port upstream largest-account trackers and API methods

**Files:**
- Create: `crates/api/src/methods/get_largest_accounts.rs`
- Modify: `crates/api/src/methods/get_token_largest_accounts.rs`
- Create: `crates/core/src/modules/largest_accounts/{mod,persist,prune,read,tracker}.rs`
- Create: `crates/core/src/modules/non_circulating/{lists,mod}.rs`
- Create: `crates/migration/src/m20260808_000000_largest_accounts_record.rs`
- Modify: API/indexer wiring, configuration, metrics, snapshots, integration tests, README, and example configuration files carried by `2dfa7dc..2ae3cd1`.

**Interfaces:**
- Consumes: finalized account updates, tracked mint configuration, database connection, and API latest-slot resolution.
- Produces: `get_largest_accounts(state, Option<RpcLargestAccountsConfig>)`, `fetch_largest_record(state, mint, latest_slot)`, and record-backed `get_token_largest_accounts`.

- [x] **Step 1: Merge upstream through #43 without committing**

Run:
```bash
git merge --no-ff --no-commit 2ae3cd1
```
Expected: conflicts only in the six pre-mapped additive integration files.

- [x] **Step 2: Resolve API state conflicts additively**

Keep downstream `supply_cache`, vote-account cache, and query-tracker fields while adding upstream largest-account method gates and wiring in:
```rust
pub largest_accounts: MethodSection,
pub token_largest_accounts: MethodSection,
```
Expected: both existing downstream RPC methods and new upstream methods compile from the same `CloudbreakRpcState`.

- [x] **Step 3: Resolve config and migration conflicts additively**

Keep `SupplyCacheConfig` and every downstream migration, and add upstream:
```rust
pub largest_accounts: Option<LargestAccountsConfig>,
pub token_largest_accounts: Option<TokenLargestAccountsConfig>,
```
Expected: old downstream config continues to deserialize and new tracker sections are independently optional.

- [x] **Step 4: Resolve index finalization and snapshot conflicts**

Retain downstream rooted stake-audit and snapshot-range fixes while invoking upstream tracker finalize/prune/bootstrap hooks at the same lifecycle boundaries.
Expected: no conflict markers and no downstream hook removed.

- [x] **Step 5: Add focused API tests**

Retain upstream integration comparisons and add or preserve unit coverage for method gating, standard Solana error codes, and record-backed account ordering where the existing test seams permit.

- [x] **Step 6: Verify the port**

Run:
```bash
cargo fmt --check
cargo check --workspace --all-targets
cargo test --workspace
```
Expected: all commands exit 0.

### Task 2: Review PR #48 against the downstream supply cache

**Files:**
- Compare: `crates/api/src/methods/get_supply.rs`
- Compare: `crates/api/src/modules/supply_cache.rs`
- Compare: `crates/core/src/modules/{supply,non_circulating,largest_accounts}/`
- Compare: `crates/index/src/{indexer.rs,modules/save_block.rs,modules/self_healing.rs}`
- Compare: `crates/migration/src/lib.rs`

**Interfaces:**
- Consumes: upstream `3025a76`, downstream `acdff81`, and the completed #37+#43 port.
- Produces: an explicit adopt/reject decision for each #48 subsystem, with any compatible improvement implemented and tested.

- [x] **Step 1: Classify #48 by responsibility**

Review the diff as four units: native supply tracker/cache, non-circulating tracker split, API serving behavior, and indexer lifecycle integration.

- [x] **Step 2: Preserve externally sourced canonical supply semantics**

Reject changes that would replace downstream's persisted canonical local-RPC cache with a second state authority unless correctness, restart behavior, and staleness guarantees are at least equivalent.

- [x] **Step 3: Adopt only independent compatible fixes**

For each candidate, require a focused regression test showing value without dual ownership of `getSupply` state. If no such subset exists, record the reviewed rejection in the merge commit message rather than adding dead architecture.

- [x] **Step 4: Verify supply behavior**

Run:
```bash
cargo test -p cloudbreak-api get_supply
cargo test -p cloudbreak-api supply_cache
```
Expected: standard config parsing, malformed canonical response rejection, staleness behavior, and finalized-only serving tests pass.

### Task 3: Delivery and live proof

**Files:**
- Modify only if required: deployed Cloudbreak configuration for the new optional tracker sections.

**Interfaces:**
- Consumes: verified source tree and existing service deployment workflow.
- Produces: pushed commit and live JSON-RPC evidence when deployment is safe in the current service state.

- [ ] **Step 1: Inspect the final diff and requirements**

Run:
```bash
git diff --check
git status --short
git diff --stat HEAD
```
Expected: no whitespace errors, no conflict markers, and only intended files changed.

- [ ] **Step 2: Commit and push**

Run:
```bash
git commit
GIT_SSH_COMMAND="ssh -i ~/.ssh/github" git push mbroeken main
```
Expected: local `HEAD` equals `mbroeken/main`.

- [ ] **Step 3: Prove runtime behavior if configuration and service topology permit**

Probe `getLargestAccounts`, `getTokenLargestAccounts`, and `getSupply` with bounded timeouts. Expected: the new methods no longer return `-32601`, GTLA completes from persisted records once tracker data exists, and downstream `getSupply` remains healthy.
