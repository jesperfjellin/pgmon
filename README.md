# pgmon — PostgreSQL DBA Health Platform (Project Summary)

> **Elevator pitch:**
> A read-only, containerized **Postgres agent** that polls core health metrics, exposes a **Prometheus `/metrics`** endpoint, ships a minimal **web UI**, and (optionally) sends **webhook alerts**. Built to keep *your* weather/IoT Postgres fast and safe, but architected so scale bottlenecks are compute, not design.

---

## 1) Goals & Non-Goals

**Goals (v1):**

* **Zero-write** monitoring (uses `pg_monitor` only).
* Lightweight **poller loops** (15s / 60s / 10m / hourly) with strict time budgets.
* **Top queries**, **locks**, **autovacuum health**, **WAL/checkpoints**, **replication lag**, **wraparound safety**, **partition sanity**.
* **Prometheus exporter** + tiny **web UI**. (Alert notifiers such as Slack/Email are planned for a later release once core surfaces stabilize.)
* Clear **SQL transparency**: every UI panel links to its SQL.

**Non-Goals (v1):**

* No cross-cluster federation, no long-term TSDB inside the agent (use Prometheus for history).
* No auto-tuning DDL/DML; **no writes** to user DBs.
* No query text as high-cardinality labels (use `queryid`).

---

## 2) Reference Stack (default)

* **Language:** Rust (Tokio async).
* **HTTP:** `axum` (REST + `/metrics` + `/healthz` + static UI).
* **DB:** `sqlx` (Postgres TLS, read-only).
* **Metrics:** `prometheus` crate (counters/gauges/histograms).
* **Config:** `serde_yaml` + `clap` for flags.
* **Logging:** `tracing` + JSON logs.
* **Container:** Distroless scratch or Alpine; **Docker healthcheck** hits `/healthz`.
* **UI:** React + Vite bundle served as static assets via `axum`.

> *Alternative:* Go (chi + pgx + promhttp). Keep shapes identical.

---

## 3) Data Sources (Postgres ≥ 12; callouts for ≥14)

* **Sessions/locks:** `pg_stat_activity`, `pg_locks`
* **Statements:** `pg_stat_statements` (req. `shared_preload_libraries` + `CREATE EXTENSION`)
* **Autovacuum progress:** `pg_stat_progress_vacuum`
* **Database stats:** `pg_stat_database` (includes `temp_files/temp_bytes` in PG13+)
* **Background writer:** `pg_stat_bgwriter`
* **WAL (PG14+):** `pg_stat_wal`
* **Replication:** `pg_stat_replication`, `pg_last_wal_receive_lsn()`, `pg_last_wal_replay_lsn()`, `pg_last_xact_replay_timestamp()`
* **Tables/indexes:** `pg_stat_user_tables`, `pg_stat_user_indexes`, `pg_class`, `pg_namespace`
* **Partitions:** `pg_inherits`
* **Wraparound:** `age(datfrozenxid)` (DB), `age(relfrozenxid)` (table)
* **(Optional) Bloat deep-dive:** `pgstattuple`, `pageinspect`

---

## 4) Poller Loops & Time Budgets

**15 s loop (<1 s budget):**

* Connections vs `max_connections`
* Long / idle-in-transaction sessions (max age)
* Blocking chains (blocked > blocker, max wait)
* Autovacuum workers by phase (`pg_stat_progress_vacuum`)
* Temp spill pulse (current `temp_files/temp_bytes` deltas via `pg_stat_database`)

**60 s loop (<3 s):**

* TPS/QPS & latency: deltas from `pg_stat_database` + `pg_stat_statements` (`pg_stat_monitor` adds p95 when present)
* Top statements by `total_time`, `calls`, `shared_blks_read` (via `queryid`)
* WAL/checkpoints: `pg_stat_bgwriter`, `pg_stat_wal` (PG14+), requested ratio + mean interval
* Replication lag (time/bytes)
* Vacuum/Analyze freshness (table-level) + autovac starvation heuristics
* WAL/temp surge detection (bytes/sec thresholds)

**5–10 min loop (<10 s):**

* Relation sizes (Top-N tables/indexes)
* Dead vs live tuples; `%dead` per table
* Index usage; track 0-scan indexes + unused index footprint
* Partition horizon sanity (future coverage & gaps)

**Hourly loop (<60 s):**

* Lightweight bloat estimate (size vs tuples; sample `pgstattuple` on Top-N)
* Wraparound safety (`age(datfrozenxid)`, worst `relfrozenxid`) with warn/crit alerts
* Bloat sampling via `pgstattuple_approx` (top relations)
* Stale stats (time since last analyze)

---

## 5) Prometheus Metric Schema (low cardinality)

All metrics include `{cluster,db}` unless noted.

* **Connections:**
  `pg_connections`, `pg_max_connections`
* **Transactions:**
  `pg_tps` (gauge via delta), `pg_qps`
* **Latency (overall):**
  `pg_query_latency_seconds_mean`, `pg_query_latency_seconds_p95`, `pg_query_latency_seconds_p99`
  *(`p95/p99` require `pg_stat_monitor` histogram data; metrics fall back to `None` otherwise.)*
* **Statements (per top query):**
  `pg_stmt_calls_total{queryid}`, `pg_stmt_time_seconds_total{queryid}`,
  `pg_stmt_shared_blks_read_total{queryid}`
  *(Keep Top-N to control cardinality; never label raw SQL.)*
* **Locks:**
  `pg_blocked_sessions_total`, `pg_longest_blocked_seconds`
* **Autovacuum:**
  `pg_autovacuum_jobs{phase}`, `pg_table_last_autovacuum_seconds{relation}`, `pg_table_last_autoanalyze_seconds{relation}`, `pg_dead_tuple_backlog_alert{relation,severity}` (1=warn/2=crit)
* **Tuples:**
  `pg_table_dead_tuples{relation}`, `pg_table_pct_dead{relation}`
* **Stats freshness:**
  `pg_table_stats_stale_seconds{relation}`
* **Storage:**
  `pg_relation_size_bytes{relation,relkind}`, `pg_relation_table_bytes{relation,relkind}`, `pg_relation_index_bytes{relation,relkind}`, `pg_relation_toast_bytes{relation,relkind}`, `pg_relation_bloat_estimated_bytes{relation,relkind}`
* **Bloat samples:**
  `pg_relation_bloat_sample_free_bytes{relation}`, `pg_relation_bloat_sample_free_percent{relation}` *(requires `pgstattuple` or `pgstattuple_approx`)*
* **Index usage:**
  `pg_index_scans_total{index}`, `pg_unused_index_bytes{index}`
* **WAL / checkpoints:**
  `pg_wal_bytes_written_total`, `pg_checkpoints_timed_total`, `pg_checkpoints_requested_total`,
  `pg_checkpoints_requested_ratio`, `pg_checkpoints_mean_interval_seconds`
* **Temp:**
  `pg_temp_bytes_total{db}`, `pg_temp_files_total{db}`, `pg_temp_bytes_written_per_second`
* **Replication:**
  `pg_replication_lag_seconds{replica}`, `pg_replication_lag_bytes{replica}`
* **Partitions:**
  `pg_partition_missing_future{parent}`, `pg_partition_future_gap_seconds{parent}`
* **Wraparound:**
  `pg_wraparound_database_tx_age{database}` (seconds), `pg_wraparound_relation_tx_age{relation}`
* **Agent health:**
  `pgmon_scrape_duration_seconds{loop}`, `pgmon_last_scrape_success{loop}` (0/1), `pgmon_errors_total{loop}`
* **Alerts:**
  `alerts_total{cluster,kind,severity}`

**Cardinality rules:**

* Cap Top-N for per-relation and per-queryid metrics (e.g., N=50).
* No dynamic/unbounded labels (no raw SQL, no usernames, no PIDs).
* Relation/index labels are **sanitized name** not OID.

---

## 6) Alert Catalog (internal defaults)

Each alert has: **expr**, **for**, **severity**, **message**, **dedupe key**.

* **Connection saturation**: `connections/max_connections > 0.8` for 2m → warn/crit
  *“Connections at {{pct}}% of max ({{curr}}/{{max}})”*
* **Long transactions**: oldest xact age > 5m (warn) / > 30m (crit); flag idle-in-txn
  *“Txn {{pid}} age {{age}} (state={{state}})”*
* **Blocking locks**: any blocked > 30s (warn) / > 120s (crit)
  *“{{blocked_pid}} blocked {{dur}} by {{blocker_pid}}”*
* **Dead tuples backlog**: `%dead > 20% AND n_dead_tup > 100k` sustained 10m
  *“{{rel}}: {{pct_dead}}% (~{{dead}} rows)”*
* **Stale stats**: changed in last 1h AND last (auto)analyze > 12h
  *“{{rel}}: stats stale {{age}}”*
* **Autovacuum starvation**: high churn, rising dead tuples, no (auto)vacuum in 6h
  *“Autovac starvation {{relation}} ({{hours}}h since run)”*
* **Checkpoint thrash**: requested/(timed+requested) > 0.2 OR mean interval < 5m
  *“Checkpoint thrash … requested ratio {{ratio}} / interval {{interval}}”*
* **WAL surge**: wal bytes rate > baseline + 3σ over 15m
* **Temp surge**: temp bytes rate > baseline + 3σ over 5m
  *Implementation: pgmon surfaces rates via `pg_wal_bytes_written_per_second` / `pg_temp_bytes_written_per_second`; configure thresholds (`wal_surge_*`, `temp_surge_*`) or alert from Prometheus.*
* **Replication lag**: > 30s (warn) / > 300s (crit) or bytes threshold
* **Wraparound danger**: `age(datfrozenxid) > 1.5e9` (warn) / `> 1.8e9` (crit)
  *“Wraparound database {{db}} age {{tx_age}}”*
* **Partition horizon risk**: newest partition upper bound < `now + partition_horizon` (warn)
  *“{{parent}} gap {{gap_hours}}h (next due {{next_partition}})”*
* **Unused indexes**: surface via `pg_unused_index_bytes` – recommend Prometheus alerting (e.g. warn at ≥100 MiB, crit at ≥500 MiB)

> **Future notifier work:** Webhook/email delivery will come after the core telemetry set is stable; today alerts surface in the UI and Prometheus metrics only.

> **Prometheus hint:**
> ```yaml
> - alert: PgmonUnusedIndexWarn
>   expr: pgmon_pg_unused_index_bytes > 100 * 1024 * 1024
>   for: 6h
>   labels:
>     severity: warn
>   annotations:
>     summary: "Unused index {{ $labels.relation }} -> {{ $labels.index }} ({{ $value | humanize1024 }})"
> - alert: PgmonUnusedIndexCrit
>   expr: pgmon_pg_unused_index_bytes > 500 * 1024 * 1024
>   for: 6h
>   labels:
>     severity: crit
>   annotations:
>     summary: "Unused index critical {{ $labels.relation }} -> {{ $labels.index }} ({{ $value | humanize1024 }})"
> ```

---

## 7) HTTP API & UI

**Endpoints**

* `GET /healthz` — liveness/ready: DB ok, loops on schedule.
* `GET /metrics` — Prometheus text format.
* `GET /api/v1/overview` — connections, TPS/QPS, latency, WAL MB/s, checkpoints, open alerts, and `blocking_events` (top blocker↔blocked chains including wait duration and optional query snippets when `security.redact_sql_text=false`).
* `GET /api/v1/autovacuum` — table list: `%dead`, `dead/live`, last (auto)vacuum/analyze, hints.
* `GET /api/v1/top-queries` — top by total_time/calls/I/O (ids + normalized sample).
* `GET /api/v1/storage` — Top-N tables/indexes by bytes; growth snapshot.
* `GET /api/v1/bloat` — sampled `pgstattuple_approx` output (table bytes, free bytes/%) for top relations (requires `pgstattuple` extension).
* `GET /api/v1/unused-indexes` — indexes with `idx_scan = 0` and size ≥ 100 MiB (relation/index name, bytes).
* `GET /api/v1/stale-stats` — tables whose statistics are older than alert thresholds.
* `GET /api/v1/partitions` — inventory by parent; oldest/newest; latest upper bound; cadence seconds; suggested next range; gap seconds; retention/future holes.
* `GET /api/v1/replication` — per-replica lag.
* `GET /api/v1/wraparound` — worst database/table wraparound ages.
* `GET /` — minimal UI (Overview, Autovacuum, Top Queries, Stale Stats, Storage/Idx, Partitions, Replication) served from the React/Vite bundle (`frontend/dist`). Each panel shows **SQL used** (collapsible) for transparency.

---

## 8) Config & Runtime

**YAML (checked-in, non-secret)**

```yaml
# excerpt from `config.pgmon.sample.yaml`
# Logical label for the monitored Postgres environment (think "prod-eu-west").
cluster: "example-cluster"
# Connection secrets are injected via the `PGMON_DSN` environment variable.
sample_intervals:
  hot_path: "15s"
  workload: "60s"
  storage: "10m"
  hourly: "1h"
limits:
  top_relations: 50
  top_indexes: 50
  top_queries: 50
  partition_horizon_days: 7
alerts:
  connections_warn: 0.80
  long_txn_warn_s: 300
  long_txn_crit_s: 1800
  repl_warn_s: 30
  repl_crit_s: 300
  stats_stale_warn_hours: 12
  stats_stale_crit_hours: 24
  wraparound_warn_tx_age: 1500000000
  wraparound_crit_tx_age: 1800000000
  wal_surge_warn_bytes_per_sec: 104857600      # 100 MiB/s
  wal_surge_crit_bytes_per_sec: 209715200      # 200 MiB/s
  temp_surge_warn_bytes_per_sec: 52428800      # 50 MiB/s
  temp_surge_crit_bytes_per_sec: 104857600     # 100 MiB/s
  autovac_starvation_warn_hours: 6
  autovac_starvation_crit_hours: 12
  checkpoint_ratio_warn: 0.2
  checkpoint_ratio_crit: 0.5
  checkpoint_interval_warn_seconds: 300
  checkpoint_interval_crit_seconds: 180
notifiers:
  slack_webhook: ""   # optional, not a priority
  email_smtp: ""      # optional, not a priority
http:
  bind: "0.0.0.0:8181"
  static_dir: "/opt/pgmon/ui"
security:
  read_only_enforce: true
  redact_sql_text: true
ui:
  hide_postgres_defaults: true
timeouts:
  statement_timeout_ms: 3000
  lock_timeout_ms: 1000
```

**`.env` (secrets + runtime overrides)**

```bash
# Required: read-only Postgres DSN with TLS.
PGMON_DSN=postgres://pgmon_agent:REPLACE_WITH_PASSWORD@db.example.com:5432/app_db?sslmode=require

# Optional: override the logical cluster label without editing YAML.
# PGMON_CLUSTER=prod-eu-west

# Logging verbosity.
RUST_LOG=info
```

**Env overrides:** `PGMON_DSN` (required), `PGMON_CLUSTER`, `PGMON_CONFIG` (custom path), plus tracing/log variables.

The repo ships a starter config at `config.pgmon.sample.yaml`. `docker compose up pgmon` mounts it into the container as `/config/pgmon.yaml`; copy and edit this file (or set `PGMON_CONFIG`) to point the agent at your own Postgres cluster.

- `ui.hide_postgres_defaults` hides the built-in `postgres`/`template*` databases from wraparound panels, keeping the focus on user databases.

---

## 9) Safety, Perf & Ops

* **DB role:** `CREATE ROLE monitor LOGIN; GRANT pg_monitor TO monitor;`
  Ensure no extra grants; agent **refuses** to start if can write DDL/DML.
* **Preload:** `shared_preload_libraries='pg_stat_statements'`; `CREATE EXTENSION pg_stat_statements;`
* **Useful settings:** `track_io_timing=on`, `pg_stat_statements.track=all`.
* **Query safety:** set `statement_timeout`, `lock_timeout`; use `SET LOCAL` per session.
* **Overhead target:** <1% DB CPU; loops within budget or log **degraded mode**.
* **Agent self-metrics:** loop durations, last success, error counters; `/healthz` reflects status.
* **Cardinality controls:** cap Top-N; sanitize names; no dynamic labels.
* **TLS:** require `sslmode=require` when DSN is remote/untrusted networks.
* **PII:** do not export raw SQL or user names in metrics; show sample SQL only in UI.

---

## 10) Minimal SQL (snippets the agent runs)

**Connections**

```sql
SELECT (SELECT COUNT(*) FROM pg_stat_activity) AS connections,
       current_setting('max_connections')::int AS max_connections;
```

**Long / idle-in-txn**

```sql
SELECT pid, usename, state, xact_start, now() - xact_start AS age
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY xact_start ASC
LIMIT 50;
```

**Blockers vs blocked**

```sql
WITH blocking AS (
    SELECT
        bl.pid AS blocked_pid,
        kl.pid AS blocker_pid,
        ka.usename AS blocked_usename,
        ka.xact_start AS blocked_xact_start,
        CASE WHEN ka.query_start IS NOT NULL
             THEN EXTRACT(EPOCH FROM now() - ka.query_start)
             ELSE NULL
        END AS blocked_wait_seconds,
        LEFT(ka.query, 256) AS blocked_query,
        aa.usename AS blocker_usename,
        aa.state AS blocker_state,
        (aa.wait_event IS NOT NULL) AS blocker_waiting,
        LEFT(aa.query, 256) AS blocker_query
    FROM pg_locks bl
    JOIN pg_stat_activity ka ON ka.pid = bl.pid
    JOIN pg_locks kl ON bl.locktype = kl.locktype
        AND bl.database IS NOT DISTINCT FROM kl.database
        AND bl.relation IS NOT DISTINCT FROM kl.relation
        AND bl.page IS NOT DISTINCT FROM kl.page
        AND bl.tuple IS NOT DISTINCT FROM kl.tuple
        AND bl.virtualxid IS NOT DISTINCT FROM kl.virtualxid
        AND bl.transactionid IS NOT DISTINCT FROM kl.transactionid
        AND bl.classid IS NOT DISTINCT FROM kl.classid
        AND bl.objid IS NOT DISTINCT FROM kl.objid
        AND bl.objsubid IS NOT DISTINCT FROM kl.objsubid
        AND bl.pid <> kl.pid
    JOIN pg_stat_activity aa ON aa.pid = kl.pid
    WHERE NOT bl.granted
)
SELECT DISTINCT ON (blocked_pid, blocker_pid)
    blocked_pid,
    blocker_pid,
    blocked_wait_seconds,
    blocked_query,
    blocker_query
FROM blocking
ORDER BY blocked_pid, blocker_pid, blocked_wait_seconds DESC NULLS LAST
LIMIT 50;
```

**Top statements**

```sql
SELECT queryid, calls, total_time, mean_time, rows,
       shared_blks_hit, shared_blks_read
FROM pg_stat_statements
ORDER BY total_time DESC
LIMIT 50;
```

**Vacuum/analyze freshness**

```sql
SELECT relid::regclass AS relation, n_live_tup, n_dead_tup,
       last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC
LIMIT 100;
```

**WAL/checkpoints**

```sql
SELECT checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_backend_fsync
FROM pg_stat_bgwriter;
-- PG14+
SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_wal;
```

**Temp spill**

```sql
SELECT datname, temp_files, temp_bytes FROM pg_stat_database;
```

**Relation sizes (Top-N)**

```sql
SELECT c.oid::regclass AS relation,
       pg_total_relation_size(c.oid) AS total_bytes,
       pg_relation_size(c.oid) AS table_bytes,
       pg_indexes_size(c.oid) AS index_bytes,
       GREATEST(pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid), 0) AS toast_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog','information_schema')
  AND c.relkind IN ('r','m')
ORDER BY total_bytes DESC
LIMIT 100;
```

**Unused indexes (snapshot)**

```sql
SELECT s.relname AS table_name, i.relname AS index_name,
       si.idx_scan, pg_relation_size(i.oid) AS bytes
FROM pg_stat_user_indexes si
JOIN pg_class i ON si.indexrelid = i.oid
JOIN pg_class s ON si.relid = s.oid
WHERE si.idx_scan = 0
ORDER BY bytes DESC
LIMIT 100;
```

**Wraparound**

```sql
SELECT datname, age(datfrozenxid) AS tx_age
FROM pg_database
ORDER BY tx_age DESC;
```

**Partition inventory**

```sql
SELECT inhparent::regclass AS parent, inhrelid::regclass AS child FROM pg_inherits;
```

---

## 11) Packaging

**Dockerfile (sketch):**

```dockerfile
FROM node:20-alpine AS web
WORKDIR /web
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ .
RUN npm run build

FROM rust:1.81-alpine AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN apk add --no-cache musl-dev openssl-dev pkgconfig && \
    cargo fetch
COPY src ./src
RUN cargo build --release

FROM gcr.io/distroless/cc
COPY --from=build /app/target/release/pgmon /usr/local/bin/pgmon
COPY --from=web /web/dist /opt/pgmon/ui
EXPOSE 8181
HEALTHCHECK --interval=30s --timeout=3s CMD ["/usr/local/bin/pgmon","--healthcheck"]
ENTRYPOINT ["/usr/local/bin/pgmon","--config","/config/pgmon.yaml"]
```

The release image bakes the React bundle (`frontend/dist`) under `/opt/pgmon/ui`; `axum` should mount that directory for `GET /`.

### Backend crate layout (current)

- `src/config.rs` loads YAML + env overrides into `AppConfig`.
- `src/db.rs` provisions a read-only `sqlx::PgPool` and enforces session guardrails.
- `src/metrics.rs` registers the `pgmon_*` self-metrics (duration, success, error counters).
- `src/state.rs` tracks snapshot data + loop health behind async `RwLock`s.
- `src/poller/mod.rs` wires the 15s/60s/10m/1h loops (15s hot path, 60s workload, 10m storage, 1h wraparound/replication) with budgets + metrics.
- `src/poller/storage.rs` surfaces relation sizes + dead tuple ratios, while `src/poller/hourly.rs` now refreshes replication lag, partition inventory, and wraparound safety snapshots.
- `src/http/mod.rs` exposes `/healthz`, `/metrics`, JSON APIs, and serves `frontend/dist/` via `ServeDir`.
- `src/main.rs` handles CLI (`--config`), tracing, pool bootstrap, poller spawn, and graceful shutdown.

Run the dev server with:

```bash
cargo run -- --config pgmon.yaml
```

### Implemented backend surface (March 2025)

- **Startup:** read-only pool validation (`SET default_transaction_read_only`) and cluster label bootstrapped from config.
- **Hot path loop (15s):**
  - Active vs max connections, blocked session count, longest transaction + blocked wait.
  - Blocking chain snapshot (top blocker↔blocked pairs with wait duration; queries omitted when `redact_sql_text=true`).
  - Autovacuum progress aggregation and temp spill deltas.
  - Feeds `/api/v1/overview` snapshot and Prometheus gauges (`pg_connections`, `pg_max_connections`, `pg_blocked_sessions_total`, `pg_longest_*`, `pg_autovacuum_jobs{phase}`, `pg_temp_*`).
- **Workload loop (60s):**
  - Aggregates cluster TPS/QPS, mean latency (pg_stat_statements), WAL bytes, checkpoints.
  - Top queries (queryid keyed) and autovacuum freshness table.
  - Publishes workload gauges (`pg_tps`, `pg_qps`, `pg_query_latency_seconds_mean`, `pg_query_latency_seconds_p95`, `pg_query_latency_seconds_p99`, `pg_wal_*`, `pg_checkpoints_*`) and autovacuum tables + metrics (`pg_table_dead_tuples`, `pg_table_pct_dead`, `pg_table_last_auto*`).
  - Exposed through `/api/v1/overview`, `/api/v1/top-queries`, and Prometheus (`pg_stmt_*`).
- **Storage loop (10m):**
  - Top relations by size with heap/index/TOAST split, dead tuple %, and index scan counters.
  - Unused index spotlight (`idx_scan = 0`, size ≥100 MiB) emitted via `/api/v1/unused-indexes` + `pg_unused_index_bytes`.
  - Exposed through `/api/v1/storage` and Prometheus (`pg_relation_size_bytes`, `pg_relation_table_bytes`, `pg_relation_index_bytes`, `pg_relation_toast_bytes`, `pg_index_scans_total`, `pg_unused_index_bytes`).
- **Hourly loop (1h):**
  - Replication lag (seconds/bytes) per replica, wraparound ages for databases + relations, partition inventory snapshot.
  - Stale-stat detection (time since analyze).
  - Metrics exported via `pg_replication_lag_seconds/bytes`, `pg_wraparound_database_tx_age`, `pg_wraparound_relation_tx_age`, `pg_table_stats_stale_seconds`.
- **Partition inventory:** aggregated per-parent summary including child count, oldest/newest partition bounds (RFC3339 where available), latest upper bound, latest partition name, derived `future_gap_seconds`, cadence estimation, and recommended next partition range. Served via `/api/v1/partitions`, exported via `pg_partition_missing_future` / `pg_partition_future_gap_seconds`, and emits warn-level alerts plus `alerts_total{partition_gap}` when gaps persist.
- **REST API:** `/api/v1/overview`, `/autovacuum`, `/top-queries`, `/storage`, `/stale-stats`, `/partitions`, `/replication`, `/wraparound` serve current snapshots. Static UI served from `http.static_dir`.
- **Prometheus exporter:** `/metrics` renders all self-metrics plus collected gauges/counters; loop health histograms/counters track scrape duration, success, and errors (`pgmon_*`).
- **Autovacuum backlog alerts:** dead tuples exceeding README thresholds emit overview warn/crit entries and maintain `pg_dead_tuple_backlog_alert{cluster,relation,severity}` (1=warn/2=crit).
- **Graceful shutdown:** listens for Ctrl+C/SIGTERM, aborts pollers, drains Axum server.
- **Frontend:** Vite/React single-page UI polls backend every 30s, displaying overview KPIs, blocking chain detail, warn/crit alert lists, autovacuum freshness, top queries, replication lag, storage top-N (heap/index/TOAST split), unused index spotlight, partition gap alerts + horizon table, and wraparound safety summaries. Each panel includes a collapsible SQL snippet for transparency.
- **Alert metrics:** Warn/crit events increment `alerts_total{cluster,kind,severity}`, enabling Prometheus alertmanager integrations while keeping severity history visible.

The remaining work items from the spec (alert engines, notifier plumbing, frontend charts, partition gap detection, advanced bloat analysis, etc.) still need implementation.

**Kubernetes hints:** add `readOnlyRootFilesystem: true`, CPU/memory requests, liveness/readiness probes → `/healthz`.

---

## 12) Testing Strategy

* **Unit:** SQL mappers → structs; cardinality guards; time-bucket deltas.
* **Integration:** testcontainers: spin PG 12/14/16; seed workload; verify metrics/alerts.
* **Synthetic canary:** `SELECT 1` and tiny `EXPLAIN (ANALYZE, BUFFERS)` on a known table; record latency gauge.
* **Perf:** ensure each loop remains within budget under moderate concurrency.
* **Chaos:** long txn, blocking locks, forced temp spills, WAL bursts, paused autovac, replica lag simulation.

---

## 13) Postgres Catalog Cheat Sheet

A quick reference for the upstream views/functions the pollers rely on. See the official PostgreSQL docs for full schemas; below are the columns we consume today.

### `pg_stat_monitor` (extension ≥ 2.2.0)

* **Latency histogram:** `resp_calls[]` (text array where each element is the bucket frequency) and the set-returning function `histogram(bucket int, queryid text)` returning `(range text, freq int, bar text)`.
* **Histogram GUCs:** `pg_stat_monitor.pgsm_histogram_min`, `pg_stat_monitor.pgsm_histogram_max`, `pg_stat_monitor.pgsm_histogram_buckets` (fetched via `pg_settings` to derive bucket widths).
* **Other fields in use:** `bucket`, `bucket_start_time`, `queryid`, `calls`, `total_exec_time`, `mean_exec_time`, `shared_blks_read`, `wal_bytes`.
* **Percentiles:** Calculated client-side from histogram buckets (we no longer expect `resp_percentile_*` columns).

### `pg_stat_statements`

* `queryid`, `calls`, `total_exec_time`, `mean_exec_time`, `shared_blks_read` power the top-query table and mean latency baseline.

### `pg_stat_progress_vacuum`

* `relid`, `phase`, `heap_blks_scanned`, `heap_blks_total` feed the autovacuum progress snapshot.

### `pg_stat_database`

* `xact_commit`, `xact_rollback`, `temp_files`, `temp_bytes` drive TPS/QPS and temp spill deltas.

### `pg_stat_wal` (PG14+)

* `wal_bytes`, `wal_records`, `wal_fpi` for WAL throughput metrics.

### `pg_stat_checkpointer`

* `num_timed`, `num_requested`, `last_restartpoint` for checkpoint ratios and intervals.

### `pg_stat_activity` & `pg_locks`

* `pid`, `datname`, `usename`, `state`, `query_start`, `wait_event_type`, `blocked_by` (derived) for session/lock diagnostics.

### `pg_stat_user_tables` & `pg_stat_user_indexes`

* Table/index sizes (`pg_relation_size`, `pg_total_relation_size`), `n_live_tup`, `n_dead_tup`, `last_autovacuum`, `last_autoanalyze`.

### `pg_stat_replication`

* `application_name`, `state`, `write_lag`, `flush_lag`, `replay_lag`, `write_lsn`, `replay_lsn` for replica status.

### `pg_inherits`

* `inhparent`, `inhrelid` to enumerate partition children; combined with `pg_class.relname` and `pg_range` (if available) for bounds.

### Wraparound views

* Database risk: `age(datfrozenxid)` via `pg_database`.
* Relation risk: `age(relfrozenxid)` via `pg_class` joined with `pg_stat_user_tables`.

Keep this section updated when pollers adopt new columns or extensions so contributors know which upstream changes might break us.

---

## 13) MVP Milestones (build order)

1. **Startup guardrails** (`pg_stat_statements` check) + `/healthz`
2. **15s loop** (connections, long/idle xacts, locks, autovac, temp pulse)
3. **60s loop** (statements, WAL/bgwriter, replication, freshness)
4. **5–10m loop** (sizes, dead/live, index usage, partitions)
5. **Hourly** (wraparound, stale stats)
6. **Alerts** (internal engine + Slack/Email) - not prioritized
7. **Minimal UI** (Overview, Autovacuum, Top queries, Storage/Idx, Partitions, Replication)
8. **Prometheus parity** (all surfaced metrics exported 1:1)

---

## 14) v2+ Roadmap (stretch)

* **Autovacuum tuner** (per-table thresholds/scale factors from observed churn)
* **Index advisor (safe)** via `hypopg` on top queries
* **Accurate bloat** sampling via `pgstattuple` / `pageinspect`
* **Plan regression detector** (hash plan + latency deltas)
* **Partitioning advisor** (period, future schedule, template DDL)
* **Vacuum outcome tracer** (vacuum runs ↔ dead-tuple/size deltas)
* **PgBouncer scrape**, **backup/PITR sanity** (`pg_stat_archiver`)
* **Config drift** (snapshot/diff `pg_settings`)

---

## 15) “How to work” — for the Codex Agent

* **Defaults matter:** If unspecified, use the **reference Rust stack** and this spec’s shapes.
* **Be explicit:** Return **full, runnable** code (no ellipses), including Cargo.toml and minimal UI assets when relevant.
* **Safety first:** Always assume **read-only** DB role; set `statement_timeout`/`lock_timeout` per session.
* **Performance:** Respect loop budgets; pre-prepare statements; avoid N+1 queries; cap Top-N.
* **Cardinality:** Never put raw SQL/usernames/PIDs as labels; use `queryid` and sanitized names.
* **Transparency:** Include the exact SQL behind each UI panel (collapsible).
* **Observability:** Instrument agent with the **self-metrics** in §5; implement `/healthz`.
* **Configurability:** All thresholds/intervals/Top-N configurable via YAML/env.
* **Deliverables:** For each feature, provide (a) SQL, (b) collector code, (c) Prom metric, (d) UI slice, (e) alert rule stub, (f) tests.

---

## 16) Glossary (quick)

* **%dead** — n_dead_tup / (n_dead_tup + n_live_tup) × 100.
* **Wraparound** — exhaustion of 32-bit TXIDs; fatal if not prevented via vacuum freeze.
* **Checkpoint thrash** — frequent forced checkpoints due to WAL growth hitting `max_wal_size`.
* **Idle in txn** — session left in a transaction without commit/rollback; blocks vacuum.

---
