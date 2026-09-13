# Cloudbreak security dependencies implementation plan

**Goal:** Apply reviewed compatible security fixes without changing consensus dependencies.
**Architecture:** Targeted Cargo.lock resolution only; existing manifests and default production features remain unchanged.
**Tech Stack:** Rust/Cargo, AWS-LC, OpenSSL, rustls, h2.
**Spec:** User-approved Cloudbreak networking/TLS remediation, September 13, 2026.

## Constraints
Do not change Sentinel or start disabled Cloudbreak services. Preserve runtime data. No forced dependency overrides. Deploy only after successful build and tests.

## Steps
- [x] Update h2 to 0.4.16, rustls-webpki to 0.103.15, openssl to 0.10.81, crossbeam-epoch to 0.9.20, event-listener to 5.4.2, keccak to 0.1.6, rand 0.8 to 0.8.6; update aws-lc-rs through compatible parent constraints.
- [x] Inspect all lockfile collateral changes and repeat OSV scan.
- [x] Run cargo test --locked --workspace --lib with bounded build parallelism and cargo build --locked --release --workspace --bins.
- [x] Commit/push verified lockfile and evidence; retain previous binaries before release build.
- [x] Update running services only if a matching local Cloudbreak deployment exists; otherwise document deployment boundary. Never claim running patch without executable verification.
