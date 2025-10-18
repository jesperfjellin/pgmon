# pgmon — PostgreSQL DBA Health Platform (Project Summary)

> **Elevator pitch:**
> A read-only, containerized **Postgres agent** that polls core health metrics, exposes a **Prometheus `/metrics`** endpoint, ships a minimal **web UI**, and (optionally) sends **webhook alerts**. Built to keep *your* weather/IoT Postgres fast and safe, but architected so scale bottlenecks are compute, not design.

---

## 1) Goals & Non-Goals

**Goals (v1):**

* **Zero-write** monitoring (uses `pg_monitor` only).
* Lightweight **poller loops** (15s / 60s / 10m / hourly) with strict time budgets.
* **Top queries**, **locks**, **autovacuum health**, **WAL/checkpoints**, **replication lag**, **bloat signals**, **wraparound safety**, **partition sanity**.
* **Prometheus exporter** + tiny **web UI** + optional **Slack/Email** notifiers.
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
* Autovacuum activity (`pg_stat_progress_vacuum`)
* Temp spill pulse (deltas of `temp_files/temp_bytes`) — optional here

**60 s loop (<3 s):**

* TPS/QPS & latency: deltas from `pg_stat_database` + `pg_stat_statements`
* Top statements by `total_time`, `calls`, `shared_blks_read` (via `queryid`)
* WAL/checkpoints: `pg_stat_bgwriter`, `pg_stat_wal` (PG14+)
* Replication lag (time/bytes)
* Vacuum/Analyze freshness (table-level)

**5–10 min loop (<10 s):**

* Relation sizes (Top-N tables/indexes)
* Dead vs live tuples; `%dead` per table
* Index usage; track 0-scan indexes
* Partition/retention sanity (IoT focus)

**Hourly loop (<60 s):**

* Lightweight bloat estimate (size vs tuples; sample `pgstattuple` on Top-N)
* Wraparound safety (`age(datfrozenxid)`, worst `relfrozenxid`)
* Stale stats (time since last analyze)

---

## 5) Prometheus Metric Schema (low cardinality)

All metrics include `{cluster,db}` unless noted.

* **Connections:**
  `pg_connections`, `pg_max_connections`
* **Transactions:**
  `pg_tps` (gauge via delta), `pg_qps`
* **Latency (overall):**
  `pg_query_latency_seconds_mean`, `pg_query_latency_seconds_p95`
* **Statements (per top query):**
  `pg_stmt_calls_total{queryid}`, `pg_stmt_time_seconds_total{queryid}`,
  `pg_stmt_shared_blks_read_total{queryid}`
  *(Keep Top-N to control cardinality; never label raw SQL.)*
* **Locks:**
  `pg_blocked_sessions_total`, `pg_longest_blocked_seconds`
* **Autovacuum:**
  `pg_autovacuum_jobs{phase}`, `pg_table_last_autovacuum_seconds{relation}`, `pg_table_last_autoanalyze_seconds{relation}`
* **Tuples/bloat signals:**
  `pg_table_dead_tuples{relation}`, `pg_table_pct_dead{relation}`
* **Storage:**
  `pg_relation_size_bytes{relation,relkind}`, `pg_index_size_bytes{index}`
* **Index usage:**
  `pg_index_scans_total{index}`
* **WAL / checkpoints:**
  `pg_wal_bytes_written_total`, `pg_checkpoints_timed_total`, `pg_checkpoints_requested_total`
* **Temp:**
  `pg_temp_bytes_total{db}`
* **Replication:**
  `pg_replication_lag_seconds{replica}`, `pg_replication_lag_bytes{replica}`
* **Wraparound:**
  `pg_wraparound_tx_age`
* **Agent health:**
  `pgmon_scrape_duration_seconds{loop}`, `pgmon_last_scrape_success{loop}` (0/1), `pgmon_errors_total{loop}`

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
* **Checkpoint thrash**: requested/(timed+requested) > 0.2 OR mean interval < 5m
* **WAL surge**: wal bytes rate > baseline + 3σ over 15m
* **Temp surge**: temp bytes rate > baseline + 3σ over 5m
* **Replication lag**: > 30s (warn) / > 300s (crit) or bytes threshold
* **Wraparound danger**: `age(datfrozenxid) > 1.5e9` (warn) / `> 1.8e9` (crit)
* **Unused indexes**: `idx_scan = 0` for 7d **and** size > 100MB

> **Notifier:** Slack webhook / Email; compact payload + deep-link to UI panel.

---

## 7) HTTP API & UI

**Endpoints**

* `GET /healthz` — liveness/ready: DB ok, loops on schedule.
* `GET /metrics` — Prometheus text format.
* `GET /api/v1/overview` — connections, TPS/QPS, latency, WAL MB/s, checkpoints, open alerts.
* `GET /api/v1/autovacuum` — table list: `%dead`, `dead/live`, last (auto)vacuum/analyze, hints.
* `GET /api/v1/top-queries` — top by total_time/calls/I/O (ids + normalized sample).
* `GET /api/v1/storage` — Top-N tables/indexes by bytes; growth snapshot.
* `GET /api/v1/partitions` — inventory by parent; oldest/newest; retention/future holes.
* `GET /api/v1/replication` — per-replica lag.
* `GET /` — minimal UI (Overview, Autovacuum, Top Queries, Storage/Idx, Partitions, Replication) served from the React/Vite bundle (`frontend/dist`).
  Each panel shows **SQL used** (collapsible) for transparency.

---

## 8) Config & Runtime

**YAML (example)**

```yaml
cluster: "hydro-prod"
dsn: "postgres://monitor:***@pg-host:5432/postgres?sslmode=require"
sample_intervals:
  hot_path: "15s"
  workload: "60s"
  storage: "10m"
  hourly: "1h"
limits:
  top_relations: 50
  top_indexes: 50
  top_queries: 50
alerts:
  connections_warn: 0.80
  long_txn_warn_s: 300
  long_txn_crit_s: 1800
  repl_warn_s: 30
  repl_crit_s: 300
notifiers:
  slack_webhook: ""   # optional
  email_smtp: ""      # optional
http:
  bind: "0.0.0.0:8181"
security:
  read_only_enforce: true
  redact_sql_text: true
timeouts:
  statement_timeout_ms: 3000
  lock_timeout_ms: 1000
```

**Env overrides:** `PGMON_DSN`, `PGMON_CLUSTER`, `PGMON_ALERTS_*`, etc.

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
SELECT bl.pid AS blocked_pid, ka.query AS blocked_query,
       kl.pid AS blocker_pid, aa.query AS blocker_query,
       now() - ka.query_start AS blocked_for
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
WHERE NOT bl.granted;
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
       pg_total_relation_size(c.oid) - pg_relation_size(c.oid) AS index_toast_bytes
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

**Kubernetes hints:** add `readOnlyRootFilesystem: true`, CPU/memory requests, liveness/readiness probes → `/healthz`.

---

## 12) Testing Strategy

* **Unit:** SQL mappers → structs; cardinality guards; time-bucket deltas.
* **Integration:** testcontainers: spin PG 12/14/16; seed workload; verify metrics/alerts.
* **Synthetic canary:** `SELECT 1` and tiny `EXPLAIN (ANALYZE, BUFFERS)` on a known table; record latency gauge.
* **Perf:** ensure each loop remains within budget under moderate concurrency.
* **Chaos:** long txn, blocking locks, forced temp spills, WAL bursts, paused autovac, replica lag simulation.

---

## 13) MVP Milestones (build order)

1. **Startup guardrails** (`pg_stat_statements` check) + `/healthz`
2. **15s loop** (connections, long/idle xacts, locks, autovac, temp pulse)
3. **60s loop** (statements, WAL/bgwriter, replication, freshness)
4. **5–10m loop** (sizes, dead/live, index usage, partitions)
5. **Hourly** (wraparound, stale stats, lite bloat)
6. **Alerts** (internal engine + Slack/Email)
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
