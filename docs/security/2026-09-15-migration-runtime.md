# Migration CLI: replace discontinued async-std

The migration executable now initializes the existing Tokio runtime directly.
SeaORM already selects `runtime-tokio-rustls`; previously async-std wrapped this
through its Tokio compatibility feature. No migration SQL, API/indexer code,
protocol data, or production database was changed.

Cargo removed async-std 1.13.2 and 16 dependencies no longer needed. No new
package/version or retained checksum changes were introduced. RUSTSEC-2025-0052
(discontinued async-std) leaves the lockfile; this is a maintenance-debt removal,
not a claim of fixing an exploitable production API flaw.

Verification on 2026-09-15:
- `cargo check -p cloudbreak-migration`
- `cargo fmt --all --check`
- `cargo test --locked -p cloudbreak-migration`
- `cargo test --locked --workspace --lib`
- `cargo build --release --locked -p cloudbreak-migration`
- Release CLI `--help`
- Release CLI status, first migration up, status, down, status against a new
  disposable PostgreSQL database. All five commands succeeded. Final migration
  count zero and slots table absent. Disposable database removed afterward.
- Fresh OSV query: 12 package/version findings (7 vulnerability/unsoundness,
  5 maintenance-only), down from 13. Remaining findings are not suppressed.

No service restart is required for this CLI-only change. Use the new release
migration executable on the next migration invocation. Do not run migration up
against production merely to activate this code change.
