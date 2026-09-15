# Query tracker on the shared Sentinel host

This deployment uses **127.0.0.1:4004** (4001 is API metrics; 4002 is indexer
metrics). Use the same database as the running API/indexer. `index_patterns`
must already exist: this installation deliberately performs **no migrations**.

## Configure and activate

Run as root, after confirming those ports and paths:

```sh
python3 deploy/configure-query-tracker.py --api-config /etc/cloudbreak/cloudbreak.api.toml --tracker-config /etc/cloudbreak/cloudbreak.query-tracker.toml
install -m 644 deploy/cloudbreak-query-tracker.service /etc/systemd/system/
mkdir -p /etc/systemd/system/cloudbreak-api.service.d
install -m 644 deploy/cloudbreak-api-query-tracker.conf /etc/systemd/system/cloudbreak-api.service.d/query-tracker.conf
systemd-analyze verify /etc/systemd/system/cloudbreak-query-tracker.service
systemctl daemon-reload
systemctl enable --now cloudbreak-query-tracker
systemctl restart cloudbreak-api
```

The configuration helper copies the API URL **locally**, adds database session
limits, and backs up existing files. Keep its generated config and backups out
of Git: they may contain credentials. The template has no credentials. Existing
custom database session options require a manual merge; the helper refuses to
silently discard them. Neither Sentinel nor the indexer needs a restart.

## Deliberately conservative limits

- API batching: 5 seconds, 1,000 buffered identities, 100 observations/batch,
  2-second request timeout. Recorder retries preserve demand on temporary failure.
- Tracker: 3 database connections, 512 MiB process cap, low CPU/IO weights.
  PostgreSQL resource use is separate from the tracker process, so the connection
  has maintenance_work_mem=64 MiB and no parallel maintenance workers.
- Creation requires ten observations, checks every five minutes, and pauses if
  the indexer's finalize queue is nonzero **or cannot be read**.
- Index cap **8 counts all indexes on the snapshot parent table**, including its
  seven baseline indexes. This currently allows only **one additional pair**;
  it is not a limit of eight new dynamic indexes. Partition child indexes are
  not independently counted. Review the baseline before reusing this setting.
- DDL lock acquisition timeout 500ms; statement timeout 5s. Existing upstream
  CREATE INDEX is non-concurrent on partitioned parent tables. The queue check
  is a pre-DDL gate, not a cancellation mechanism during DDL. Each table DDL
  can therefore briefly block writes up to its budget. A large index may fail
  within the budget and retry later; do not increase limits on this shared host
  merely to make a demo pass. Arrange a separately measured maintenance plan.
- Token programs already have specialized indexes and are excluded from generic
  creation. Other filtered-program patterns can be recorded automatically.
- Eviction, health writes, EXPLAIN sampling and discrepancy scans are disabled.
  No index drops. No production schema migration or baseline reconciliation.

## Verify without confusing liveness and indexing

`curl http://127.0.0.1:4004/health` is **liveness only**. Check service enablement,
tracker metrics, durable `index_patterns` demand, API/indexer health and slot lag.
Send a bounded real getProgramAccounts request with useful filters for a small
program; inspect its persisted demand after the next batch flush. Token-program
queries alone do not prove generic recording.

A pattern being `candidate` proves recording, not that an index was created.
Inspect `status`, `create_attempts`, `last_create_error`, actual paired index
validity and EXPLAIN separately. Compare complete results at compatible slots
before making performance/correctness claims. A disposable database can prove the
creation/backpressure lifecycle without forcing a large production-table index.

## Rollback

Stop/disable `cloudbreak-query-tracker`, restore the saved API config, remove
only the API tracker drop-in added by this procedure, reload systemd, and restart
only the API. Keep recorded patterns and any indexes for review; do not drop data
or indexes as an automatic rollback step. Secret-bearing backups stay local.
