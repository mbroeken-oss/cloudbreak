# Compatible security dependency remediation — 2026-09-13

Scope: the user approved the previously reviewed Cloudbreak networking/TLS patch batch. No Sentinel source, consensus rules, database, ledger, service configuration or running service was changed.

## Updated lockfile

| Package | Before | After |
|---|---|---|
| h2 | 0.4.13 | 0.4.16 |
| rustls-webpki | 0.103.9 | 0.103.15 |
| aws-lc-rs | 1.15.4 | 1.18.1 |
| aws-lc-sys | 0.37.0 | 0.45.0 |
| openssl | 0.10.75 | 0.10.81 |
| openssl-sys | 0.9.111 | 0.9.117 |
| crossbeam-epoch | 0.9.18 | 0.9.20 |
| event-listener | 5.4.1 | 5.4.2 |
| keccak | 0.1.5 | 0.1.6 |
| rand | 0.8.5 | 0.8.6 |

AWS-LC was resolved through its compatible Rust parent; no forced sys override or manifest changes. Cargo also reselected existing windows-sys and heck dependency edges within their published version ranges. No other package versions changed. Updated dependencies removed now-unused edges and added AWS-LC's pkg-config build dependency.

## Verification

- `cargo test --locked --workspace --lib -j 4`: exit 0; 29 passed, zero failed/ignored.
- `cargo metadata --locked --format-version 1`: exit 0.
- `git diff --check`: exit 0.
- Fresh OSV querybatch: all 939 registry package/version pairs scanned; findings reduced from 22 to 14. None of the ten updated versions has a finding in this scan. This is not an exploitability assessment or proof of vulnerability-free software.
- `cargo build --locked --release --workspace --bins -j 4`: exit 0, optimized build completed.
- CLI smoke checks: `--help` exits 0 for cloudbreak, cloudbreak-dbtools, cloudbreak-migration and integration_tests. SHA-256 hashes recorded locally.
- Updated executables are installed at their existing release paths for the next authorized service start. No live Cloudbreak Rust deployment was attempted because its services were already disabled/inactive.
- Sentinel: same PID 2003, zero restarts, RPC getHealth=ok; dashboard HTTP 200.

Full local evidence and previous executable rollback copies: `/root/.openclaw/backups/cloudbreak-security-20260913/`.

## Remaining findings

Exact package/version/advisory IDs: `2026-09-13-osv-remaining.json`. These include unmaintained libraries, memory-safety notices and vulnerabilities requiring separate review.

- Legacy curve25519/ed25519/rand 0.7: Solana/Agave graph, not part of this compatibility-preserving update.
- memmap2 0.5, bitmaps, rsa: require affected-call/feature assessment or parent migration. Do not force format/crypto changes.
- rkyv 0.7: `cargo tree --locked -i rkyv` reports nothing for current target/default feature graph. A lockfile finding is not evidence it is used by the built service.
- OpenTelemetry SDK 0.31: fixed by 0.32.1, a coordinated pre-1.0 stack migration. Advisory concerns W3C Baggage propagation; no Baggage/propagator setup found in Cloudbreak source. This is not a complete reachability proof.
- async-std, bincode, derivative, paste, proc-macro-error2, libsecp256k1: maintenance notices remain.

Sources: https://api.osv.dev/v1/querybatch and https://api.osv.dev/v1/vulns/{advisory-id}; retrieved September 13, 2026.

## Runtime boundary

Cloudbreak Rust API and indexer services were inactive/disabled before remediation. Their configured executable is `/opt/cloudbreak/target/release/cloudbreak`. Do not start them just to claim a live deployment. The active `cloudbreak-snapshot-tracker` is a separate Python script and does not use these Cargo dependencies. Sentinel remains untouched. Full live database/indexer integration is not verified by unit tests or CLI smoke tests.
