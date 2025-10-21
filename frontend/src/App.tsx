import { useEffect, useMemo, useState } from "react";
import "./App.css";
import {
  api,
  AutovacuumEntry,
  BlockingEvent,
  BloatSample,
  OverviewSnapshot,
  PartitionSlice,
  ReplicaLag,
  StaleStatEntry,
  StorageEntry,
  TopQueryEntry,
  UnusedIndexEntry,
  WraparoundSnapshot,
  createPoller,
} from "./api";

const numberFormatter = new Intl.NumberFormat();

const CONNECTION_WARN = 0.8;
const CONNECTION_CRIT = 0.95;
const LONG_TXN_WARN = 300;
const LONG_TXN_CRIT = 1800;
const BLOCKED_WARN = 30;
const BLOCKED_CRIT = 120;

function formatBytes(value: number) {
  if (!Number.isFinite(value)) {
    return "–";
  }
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let v = value;
  let unitIndex = 0;
  while (v >= 1024 && unitIndex < units.length - 1) {
    v /= 1024;
    unitIndex += 1;
  }
  return `${v.toFixed(v >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatSeconds(value?: number | null) {
  if (value === undefined || value === null || value < 0) {
    return "–";
  }
  if (value >= 86_400) {
    return `${(value / 86_400).toFixed(1)}d`;
  }
  if (value < 60) {
    return `${value.toFixed(0)}s`;
  }
  if (value < 3600) {
    return `${(value / 60).toFixed(1)}m`;
  }
  return `${(value / 3600).toFixed(1)}h`;
}

function formatRelativeTimestamp(value?: number | null) {
  if (value === undefined || value === null) {
    return "never";
  }
  if (value <= 0) {
    return "never";
  }
  const date = new Date(value * 1000);
  if (Number.isNaN(date.getTime()) || date.getUTCFullYear() < 2000) {
    return "never";
  }
  if (Number.isNaN(date.getTime())) {
    return "never";
  }
  const diffMs = date.getTime() - Date.now();
  const diffMinutes = Math.round(diffMs / (60 * 1000));
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (Math.abs(diffMinutes) < 60) {
    return formatter.format(diffMinutes, "minute");
  }
  const diffHours = Math.round(diffMinutes / 60);
  if (Math.abs(diffHours) < 48) {
    return formatter.format(diffHours, "hour");
  }
  const diffDays = Math.round(diffHours / 24);
  return formatter.format(diffDays, "day");
}

function formatRange(
  start?: number | null,
  end?: number | null,
): string {
  const startText =
    start === undefined || start === null ? null : formatRelativeTimestamp(start);
  const endText =
    end === undefined || end === null ? null : formatRelativeTimestamp(end);
  if (startText && endText) {
    return `${startText} → ${endText}`;
  }
  if (startText) {
    return startText;
  }
  if (endText) {
    return endText;
  }
  return "–";
}

function warnClass(condition: boolean, crit = false) {
  if (crit && condition) {
    return "status status--crit";
  }
  if (condition) {
    return "status status--warn";
  }
  return "status status--ok";
}

function formatHours(value?: number | null) {
  if (value === undefined || value === null || value < 0) {
    return "–";
  }
  if (value >= 48) {
    return `${(value / 24).toFixed(1)} d`;
  }
  return `${value.toFixed(1)} h`;
}

function formatQuerySnippet(query?: string | null) {
  if (!query) {
    return "–";
  }
  const normalized = query.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "–";
  }
  return normalized.length > 120
    ? `${normalized.slice(0, 117)}…`
    : normalized;
}

const SQL_SNIPPETS = {
  overview: `
SELECT (SELECT COUNT(*) FROM pg_stat_activity) AS connections,
       current_setting('max_connections')::bigint AS max_connections;

SELECT COUNT(*) AS blocked_sessions,
       MAX(EXTRACT(EPOCH FROM now() - act.query_start)) AS longest_blocked_seconds
FROM pg_locks l
JOIN pg_stat_activity act ON act.pid = l.pid
WHERE NOT l.granted;

SELECT MAX(EXTRACT(EPOCH FROM now() - xact_start)) AS longest_transaction_seconds
FROM pg_stat_activity
WHERE xact_start IS NOT NULL;
`,
  blocking: `
WITH blocking AS (
    SELECT
        bl.pid AS blocked_pid,
        kl.pid AS blocker_pid,
        ka.usename AS blocked_usename,
        ka.xact_start AS blocked_xact_start,
        CASE
            WHEN ka.query_start IS NOT NULL THEN EXTRACT(EPOCH FROM now() - ka.query_start)
            ELSE NULL
        END AS blocked_wait_seconds,
        ka.query AS blocked_query,
        aa.usename AS blocker_usename,
        aa.state AS blocker_state,
        (aa.wait_event IS NOT NULL) AS blocker_waiting,
        aa.query AS blocker_query
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
    blocked_wait_seconds
FROM blocking
ORDER BY blocked_pid, blocker_pid, blocked_wait_seconds DESC NULLS LAST
LIMIT $1;
`,
  autovacuum: `
SELECT
    relid::regclass::text AS relation,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC
LIMIT $1;
`,
  topQueries: `
SELECT
    queryid,
    calls,
    total_exec_time,
    mean_exec_time,
    shared_blks_read
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT $1;
`,
  storage: `
SELECT
    c.oid::regclass::text AS relation,
    c.relkind::text AS relkind,
    pg_total_relation_size(c.oid) AS total_bytes,
    pg_relation_size(c.oid) AS table_bytes,
    pg_indexes_size(c.oid) AS index_bytes,
    GREATEST(pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid), 0) AS toast_bytes,
    NULLIF(s.n_live_tup + s.n_dead_tup, 0)::double precision AS tuple_denominator,
    s.n_dead_tup AS dead_tuples,
    s.last_autovacuum,
    c.reltuples::double precision AS reltuples
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND c.relkind IN ('r','m')
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT $1;
`,
  unusedIndexes: `
SELECT
    rel.oid::regclass::text AS relation,
    idx.oid::regclass::text AS index,
    pg_relation_size(idx.oid) AS bytes
FROM pg_class idx
JOIN pg_index i ON i.indexrelid = idx.oid
JOIN pg_class rel ON rel.oid = i.indrelid
JOIN pg_namespace n ON n.oid = rel.relnamespace
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = idx.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND idx.relkind = 'i'
  AND COALESCE(s.idx_scan, 0) = 0
  AND pg_relation_size(idx.oid) >= 100 * 1024 * 1024
ORDER BY bytes DESC
LIMIT $1;
`,
  bloat: `
WITH top_relations AS (
    SELECT c.oid::regclass AS rel,
           c.oid,
           n.nspname,
           c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND c.relkind = 'r'
    ORDER BY pg_total_relation_size(c.oid) DESC
    LIMIT $1
)
SELECT
    (nspname || '.' || relname) AS relation,
    stats.table_len AS table_bytes,
    stats.free_space AS free_bytes,
    stats.free_percent
FROM top_relations
JOIN LATERAL pgstattuple_approx(top_relations.rel) stats ON TRUE;
`,
  staleStats: `
SELECT
    relid::regclass::text AS relation,
    n_live_tup,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables;
`,
  replication: `
SELECT
    application_name,
    GREATEST(
        COALESCE(EXTRACT(EPOCH FROM write_lag), 0),
        COALESCE(EXTRACT(EPOCH FROM flush_lag), 0),
        COALESCE(EXTRACT(EPOCH FROM replay_lag), 0)
    ) AS lag_seconds,
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint AS lag_bytes
FROM pg_stat_replication;
`,
  partitions: `
SELECT
    parent.relname AS parent_name,
    parent.oid::regclass::text AS parent,
    child.oid::regclass::text AS child,
    pg_get_expr(child.relpartbound, child.oid) AS bounds
FROM pg_inherits
JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
JOIN pg_class child ON child.oid = pg_inherits.inhrelid
JOIN pg_namespace pn ON pn.oid = parent.relnamespace
JOIN pg_namespace cn ON cn.oid = child.relnamespace
WHERE pn.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY parent, child;
`,
  wraparound: `
SELECT datname, age(datfrozenxid) AS tx_age
FROM pg_database
ORDER BY tx_age DESC
LIMIT 10;

SELECT c.oid::regclass::text AS relation, age(c.relfrozenxid) AS tx_age
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND c.relkind IN ('r','m')
ORDER BY age(c.relfrozenxid) DESC
LIMIT $1;
`,
} as const;

function SqlSnippet({ sql }: { sql: string }) {
  const normalized = sql.trim();
  return (
    <details className="sql-snippet">
      <summary>SQL</summary>
      <pre>
        <code>{normalized}</code>
      </pre>
    </details>
  );
}

function usePollingData<T>(path: string, initial: T, intervalMs?: number) {
  const [data, setData] = useState<T>(initial);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const handleData = (next: T) => {
      setData(next);
      setError(null);
    };
    const stop = createPoller<T>(
      path,
      handleData,
      (err) => setError(err.message),
      intervalMs,
    );
    return stop;
  }, [path, intervalMs]);

  return { data, error };
}

function AlertsPanel({
  overview,
}: {
  overview: OverviewSnapshot | null;
}) {
  const warnAlerts = overview?.open_alerts ?? [];
  const critAlerts = overview?.open_crit_alerts ?? [];

  if (!warnAlerts.length && !critAlerts.length) {
    return (
      <div className="panel">
        <h2>Alerts</h2>
        <p className="muted">No active alerts detected.</p>
      </div>
    );
  }

  return (
    <div className="panel">
      <h2>Alerts</h2>
      {critAlerts.length > 0 && (
        <div className="alert-list">
          <h3>Critical</h3>
          <ul>
            {critAlerts.map((alert) => (
              <li key={`crit-${alert}`} className="alert alert--crit">
                {alert}
              </li>
            ))}
          </ul>
        </div>
      )}
      {warnAlerts.length > 0 && (
        <div className="alert-list">
          <h3>Warning</h3>
          <ul>
            {warnAlerts.map((alert) => (
              <li key={`warn-${alert}`} className="alert alert--warn">
                {alert}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function OverviewPanel({
  overview,
}: {
  overview: OverviewSnapshot | null;
}) {
  if (!overview) {
    return (
      <div className="panel">
        <h2>Cluster Overview</h2>
        <p className="muted">Loading...</p>
      </div>
    );
  }

  const connectionRatio =
    overview.max_connections > 0
      ? overview.connections / overview.max_connections
      : 0;
  const longestTx = overview.longest_transaction_seconds ?? 0;
  const longestBlocked = overview.longest_blocked_seconds ?? 0;
  const blockedWarn = overview.blocked_sessions > 0 || longestBlocked >= BLOCKED_WARN;
  const blockedCrit = longestBlocked >= BLOCKED_CRIT;
  const blockingEvents = overview.blocking_events ?? [];
  const topBlocking = useMemo<BlockingEvent[]>(
    () => blockingEvents.slice(0, 8),
    [blockingEvents],
  );

  return (
    <div className="panel">
      <h2>Cluster Overview</h2>
      <div className="metrics-grid">
        <MetricCard
          label="Connections"
          value={`${overview.connections}/${overview.max_connections}`}
          status={
            connectionRatio >= CONNECTION_CRIT
              ? "crit"
              : connectionRatio >= CONNECTION_WARN
              ? "warn"
              : undefined
          }
        />
        <MetricCard
          label="Blocked Sessions"
          value={overview.blocked_sessions}
          status={blockedCrit ? "crit" : blockedWarn ? "warn" : undefined}
        />
        <MetricCard
          label="Longest Transaction"
          value={formatSeconds(overview.longest_transaction_seconds)}
          status={
            longestTx >= LONG_TXN_CRIT
              ? "crit"
              : longestTx >= LONG_TXN_WARN
              ? "warn"
              : undefined
          }
        />
        <MetricCard
          label="TPS"
          value={overview.tps ? overview.tps.toFixed(1) : "–"}
        />
        <MetricCard
          label="QPS"
          value={overview.qps ? overview.qps.toFixed(1) : "–"}
        />
        <MetricCard
          label="Mean Latency"
          value={
            overview.mean_latency_ms
              ? `${overview.mean_latency_ms.toFixed(1)} ms`
              : "–"
          }
        />
        <MetricCard
          label="P95 Latency"
          value={
            overview.latency_p95_ms
              ? `${overview.latency_p95_ms.toFixed(1)} ms`
              : "–"
          }
        />
        <MetricCard
          label="P99 Latency"
          value={
            overview.latency_p99_ms
              ? `${overview.latency_p99_ms.toFixed(1)} ms`
              : "–"
          }
        />
      </div>
      <p className="muted">
        Updated {formatRelativeTimestamp(overview.generated_at)}
      </p>
      <section>
        <h3>Blocking Chains</h3>
        {topBlocking.length === 0 ? (
          <p className="muted">No blocking chains detected.</p>
        ) : (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Blocked</th>
                  <th className="numeric">Wait</th>
                  <th>Blocker</th>
                  <th>Status</th>
                  <th>Queries</th>
                </tr>
              </thead>
              <tbody>
                {topBlocking.map((event) => (
                  <tr key={`${event.blocked_pid}-${event.blocker_pid}`}>
                    <td>
                      <div>
                        PID {event.blocked_pid}
                        {event.blocked_usename && ` · ${event.blocked_usename}`}
                      </div>
                      <div className="muted">
                        Txn {event.blocked_transaction_start
                          ? formatRelativeTimestamp(event.blocked_transaction_start)
                          : "unknown"}
                      </div>
                    </td>
                    <td className="numeric">
                      {formatSeconds(event.blocked_wait_seconds)}
                    </td>
                    <td>
                      <div>
                        PID {event.blocker_pid}
                        {event.blocker_usename && ` · ${event.blocker_usename}`}
                      </div>
                    </td>
                    <td>
                      {event.blocker_state ?? "unknown"}
                      {event.blocker_waiting ? " (waiting)" : ""}
                    </td>
                    <td>
                      <div className="muted">
                        Blocked: {formatQuerySnippet(event.blocked_query)}
                      </div>
                      <div className="muted">
                        Blocker: {formatQuerySnippet(event.blocker_query)}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
      <SqlSnippet sql={SQL_SNIPPETS.overview} />
      <SqlSnippet sql={SQL_SNIPPETS.blocking} />
    </div>
  );
}

function MetricCard({
  label,
  value,
  status,
}: {
  label: string;
  value: number | string;
  status?: "warn" | "crit";
}) {
  const badgeClass =
    status === "crit"
      ? "status status--crit"
      : status === "warn"
      ? "status status--warn"
      : undefined;
  return (
    <div className="metric-card">
      <div className="metric-label">{label}</div>
      <div className="metric-value">
        {value}
        {badgeClass && (
          <span className={`metric-badge ${badgeClass}`}>
            {status === "crit" ? "crit" : "warn"}
          </span>
        )}
      </div>
    </div>
  );
}

function TopQueriesPanel({ queries }: { queries: TopQueryEntry[] }) {
  const topQueries = useMemo(() => queries.slice(0, 10), [queries]);

  return (
    <div className="panel wide">
      <h2>Top Queries</h2>
      {topQueries.length === 0 ? (
        <p className="muted">No query activity recorded yet.</p>
      ) : (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Query ID</th>
                <th className="numeric">Calls</th>
                <th className="numeric">Total Time (s)</th>
                <th className="numeric">Mean Time (ms)</th>
                <th className="numeric">Shared Blks Read</th>
              </tr>
            </thead>
            <tbody>
              {topQueries.map((row) => (
                <tr key={row.queryid}>
                  <td>{row.queryid}</td>
                  <td className="numeric">{numberFormatter.format(row.calls)}</td>
                  <td className="numeric">
                    {row.total_time_seconds.toFixed(2)}
                  </td>
                  <td className="numeric">{row.mean_time_ms.toFixed(2)}</td>
                  <td className="numeric">
                    {numberFormatter.format(row.shared_blks_read)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <SqlSnippet sql={SQL_SNIPPETS.topQueries} />
    </div>
  );
}

function StaleStatsPanel({ rows }: { rows: StaleStatEntry[] }) {
  const topRows = useMemo(() => rows.slice(0, 12), [rows]);

  return (
    <div className="panel wide">
      <h2>Stale Statistics</h2>
      {topRows.length === 0 ? (
        <p className="muted">No tables exceed the stale-stat thresholds.</p>
      ) : (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Relation</th>
                <th className="numeric">Hours Since Analyze</th>
                <th>Last Analyze</th>
                <th>Last Autoanalyze</th>
                <th className="numeric">Live Tuples</th>
              </tr>
            </thead>
            <tbody>
              {topRows.map((row) => (
                <tr key={row.relation}>
                  <td>{row.relation}</td>
                  <td className="numeric">{formatHours(row.hours_since_analyze)}</td>
                  <td>{formatRelativeTimestamp(row.last_analyze ?? undefined)}</td>
                  <td>{formatRelativeTimestamp(row.last_autoanalyze ?? undefined)}</td>
                  <td className="numeric">{numberFormatter.format(row.n_live_tup)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <SqlSnippet sql={SQL_SNIPPETS.staleStats} />
    </div>
  );
}

function AutovacuumPanel({ tables }: { tables: AutovacuumEntry[] }) {
  const topTables = useMemo(() => tables.slice(0, 8), [tables]);

  return (
    <div className="panel wide">
      <h2>Autovacuum Health</h2>
      {topTables.length === 0 ? (
        <p className="muted">No user tables observed yet.</p>
      ) : (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Relation</th>
                <th className="numeric">Dead Tuples</th>
                <th className="numeric">% Dead</th>
                <th>Last Autovacuum</th>
                <th>Last Autoanalyze</th>
              </tr>
            </thead>
            <tbody>
              {topTables.map((row) => {
                const total = row.n_live_tup + row.n_dead_tup;
                const pctDead =
                  total > 0 ? (row.n_dead_tup / total) * 100 : undefined;
                return (
                  <tr key={row.relation}>
                    <td>{row.relation}</td>
                    <td className="numeric">
                      {numberFormatter.format(row.n_dead_tup)}
                    </td>
                    <td className="numeric">
                      {pctDead !== undefined
                        ? `${pctDead.toFixed(1)}%`
                        : "—"}
                    </td>
                    <td>{formatRelativeTimestamp(row.last_autovacuum ?? undefined)}</td>
                    <td>{formatRelativeTimestamp(row.last_autoanalyze ?? undefined)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      <SqlSnippet sql={SQL_SNIPPETS.autovacuum} />
    </div>
  );
}

function StoragePanel({ rows }: { rows: StorageEntry[] }) {
  const topRows = useMemo(() => rows.slice(0, 8), [rows]);
  return (
    <div className="panel wide">
      <h2>Largest Relations</h2>
      {topRows.length === 0 ? (
        <p className="muted">No relation statistics available yet.</p>
      ) : (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Relation</th>
                <th className="numeric">Total Size</th>
                <th className="numeric">Heap</th>
                <th className="numeric">Indexes</th>
                <th className="numeric">TOAST</th>
                <th className="numeric">% Dead</th>
                <th className="numeric">Est. Bloat</th>
              </tr>
            </thead>
            <tbody>
              {topRows.map((row) => (
                <tr key={row.relation}>
                  <td>{row.relation}</td>
                  <td className="numeric">{formatBytes(row.total_bytes)}</td>
                  <td className="numeric">{formatBytes(row.table_bytes)}</td>
                  <td className="numeric">{formatBytes(row.index_bytes)}</td>
                  <td className="numeric">{formatBytes(row.toast_bytes)}</td>
                  <td className="numeric">
                    {row.dead_tuple_ratio !== undefined
                      ? `${row.dead_tuple_ratio.toFixed(1)}%`
                      : "—"}
                  </td>
                  <td className="numeric">
                    {row.estimated_bloat_bytes !== undefined && row.estimated_bloat_bytes !== null
                      ? formatBytes(row.estimated_bloat_bytes)
                      : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <SqlSnippet sql={SQL_SNIPPETS.storage} />
    </div>
  );
}

function BloatPanel({ samples }: { samples: BloatSample[] }) {
  if (samples.length === 0) {
    return (
      <div className="panel">
        <h2>Bloat Samples</h2>
        <p className="muted">
          No bloat data available. Ensure <code>pgstattuple</code> extension is installed and
          hourly loop is running. Set <code>bloat.sampling_mode: "exact"</code> for detailed tuple
          statistics.
        </p>
        <SqlSnippet sql={SQL_SNIPPETS.bloat} />
      </div>
    );
  }

  const topSamples = useMemo(() => samples.slice(0, 20), [samples]);

  // Check if any sample has advanced fields (exact mode)
  const hasAdvancedFields = topSamples.some(s => s.dead_tuple_count != null);

  return (
    <div className="panel wide">
      <h2>Bloat Samples {hasAdvancedFields && <span className="muted">(exact mode)</span>}</h2>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Relation</th>
              <th className="numeric">Table Bytes</th>
              <th className="numeric">Free Bytes</th>
              <th className="numeric">Free %</th>
              {hasAdvancedFields && (
                <>
                  <th className="numeric">Dead Tuples</th>
                  <th className="numeric">Dead %</th>
                  <th className="numeric">Live Tuples</th>
                  <th className="numeric">Tuple Density %</th>
                </>
              )}
            </tr>
          </thead>
          <tbody>
            {topSamples.map((sample) => (
              <tr key={sample.relation}>
                <td>{sample.relation}</td>
                <td className="numeric">{formatBytes(sample.table_bytes)}</td>
                <td className="numeric">{formatBytes(sample.free_bytes)}</td>
                <td className="numeric">{sample.free_percent.toFixed(1)}%</td>
                {hasAdvancedFields && (
                  <>
                    <td className="numeric">
                      {sample.dead_tuple_count != null
                        ? sample.dead_tuple_count.toLocaleString()
                        : "—"}
                    </td>
                    <td className="numeric">
                      {sample.dead_tuple_percent != null
                        ? sample.dead_tuple_percent.toFixed(1) + "%"
                        : "—"}
                    </td>
                    <td className="numeric">
                      {sample.live_tuple_count != null
                        ? sample.live_tuple_count.toLocaleString()
                        : "—"}
                    </td>
                    <td className="numeric">
                      {sample.tuple_density != null
                        ? sample.tuple_density.toFixed(1) + "%"
                        : "—"}
                    </td>
                  </>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <SqlSnippet sql={SQL_SNIPPETS.bloat} />
    </div>
  );
}

function UnusedIndexPanel({ indexes }: { indexes: UnusedIndexEntry[] }) {
  if (indexes.length === 0) {
    return (
      <div className="panel">
        <h2>Unused Indexes</h2>
        <p className="muted">No large unused indexes detected.</p>
        <SqlSnippet sql={SQL_SNIPPETS.unusedIndexes} />
      </div>
    );
  }

  const topIndexes = useMemo(() => indexes.slice(0, 20), [indexes]);

  return (
    <div className="panel wide">
      <h2>Unused Indexes</h2>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Relation</th>
              <th>Index</th>
              <th className="numeric">Bytes</th>
            </tr>
          </thead>
          <tbody>
            {topIndexes.map((idx) => (
              <tr key={`${idx.relation}-${idx.index}`}>
                <td>{idx.relation}</td>
                <td>{idx.index}</td>
                <td className="numeric">{formatBytes(idx.bytes)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <SqlSnippet sql={SQL_SNIPPETS.unusedIndexes} />
    </div>
  );
}

function ReplicationPanel({ replicas }: { replicas: ReplicaLag[] }) {
  return (
    <div className="panel">
      <h2>Replication</h2>
      {replicas.length === 0 ? (
        <p className="muted">No replication sessions detected.</p>
      ) : (
        <ul className="list">
          {replicas.map((replica) => (
            <li key={replica.replica}>
              <span className="list__title">{replica.replica}</span>
              <span className="list__value">
                {formatSeconds(replica.lag_seconds)}
              </span>
            </li>
          ))}
        </ul>
      )}
      <SqlSnippet sql={SQL_SNIPPETS.replication} />
    </div>
  );
}

function PartitionPanel({ slices }: { slices: PartitionSlice[] }) {
  if (slices.length === 0) {
    return (
      <div className="panel">
        <h2>Partitions</h2>
        <p className="muted">No partitioned parents discovered.</p>
      </div>
    );
  }

  const sortedSlices = useMemo(() => {
    return [...slices].sort((a, b) => {
      if (a.missing_future_partition === b.missing_future_partition) {
        return a.parent.localeCompare(b.parent);
      }
      return a.missing_future_partition ? -1 : 1;
    });
  }, [slices]);
  const atRisk = useMemo(
    () => sortedSlices.filter((slice) => slice.missing_future_partition),
    [sortedSlices],
  );
  return (
    <div className="panel wide">
      <h2>Partition Horizon</h2>
      <p className="muted">
        Cadence and suggested ranges are derived from recent partitions; review before applying
        DDL in production.
      </p>
      {atRisk.length > 0 ? (
        <div className="alert-list">
          <h3>Missing Future Partitions</h3>
          <ul>
            {atRisk.map((slice) => (
              <li key={`gap-${slice.parent}`} className="alert alert--warn">
                {slice.parent}: gap {formatSeconds(slice.future_gap_seconds)} ·
                next due {formatRelativeTimestamp(slice.next_expected_partition)}
                {slice.advisory_note ? (
                  <>
                    {" "}
                    <span className="muted">— {slice.advisory_note}</span>
                  </>
                ) : null}
              </li>
            ))}
          </ul>
        </div>
      ) : (
        <p className="muted">
          All parents extend beyond the configured partition horizon.
        </p>
      )}
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Parent</th>
              <th className="numeric">Children</th>
              <th>Oldest Start</th>
              <th>Latest Partition</th>
              <th>Upper Bound</th>
              <th>Cadence</th>
              <th>Suggested Range</th>
              <th>Gap</th>
              <th>Status</th>
              <th>Guidance</th>
            </tr>
          </thead>
          <tbody>
            {sortedSlices.map((slice) => (
              <tr key={slice.parent}>
                <td>{slice.parent}</td>
                <td className="numeric">{slice.child_count}</td>
                <td>{formatRelativeTimestamp(slice.oldest_partition)}</td>
                <td>
                  {slice.latest_partition_name
                    ? `${slice.latest_partition_name} · ${formatRelativeTimestamp(
                        slice.newest_partition,
                      )}`
                    : formatRelativeTimestamp(slice.newest_partition)}
                </td>
                <td>
                  {formatRelativeTimestamp(slice.latest_partition_upper)}
                </td>
                <td>{formatSeconds(slice.cadence_seconds)}</td>
                <td>
                  {formatRange(
                    slice.suggested_next_start,
                    slice.suggested_next_end,
                  )}
                </td>
                <td>{formatSeconds(slice.future_gap_seconds)}</td>
                <td>
                  <span className={warnClass(slice.missing_future_partition)}>
                    {slice.missing_future_partition ? "At Risk" : "Healthy"}
                  </span>
                </td>
                <td className="muted">
                  {slice.advisory_note ?? "–"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <SqlSnippet sql={SQL_SNIPPETS.partitions} />
    </div>
  );
}

function WraparoundPanel({ snapshot }: { snapshot: WraparoundSnapshot }) {
  const topDatabases = snapshot.databases.slice(0, 5);
  const topRelations = snapshot.relations.slice(0, 5);
  return (
    <div className="panel">
      <h2>Wraparound Safety</h2>
      <div className="wrap-grid">
        <div>
          <h3>Databases</h3>
          <ul className="list">
            {topDatabases.map((row) => (
              <li key={row.database}>
                <span className="list__title">{row.database}</span>
                <span className="list__value">
                  {numberFormatter.format(row.tx_age)}
                </span>
              </li>
            ))}
            {topDatabases.length === 0 && <p className="muted">n/a</p>}
          </ul>
        </div>
        <div>
          <h3>Relations</h3>
          <ul className="list">
            {topRelations.map((row) => (
              <li key={row.relation}>
                <span className="list__title">{row.relation}</span>
                <span className="list__value">
                  {numberFormatter.format(row.tx_age)}
                </span>
              </li>
            ))}
            {topRelations.length === 0 && <p className="muted">n/a</p>}
          </ul>
        </div>
      </div>
      <SqlSnippet sql={SQL_SNIPPETS.wraparound} />
    </div>
  );
}

function App() {
  const {
    data: overview,
    error: overviewError,
  } = usePollingData<OverviewSnapshot | null>(api.overview, null, 15_000);
  const { data: autovacuum } = usePollingData<AutovacuumEntry[]>(
    api.autovacuum,
    [],
    60_000,
  );
  const { data: topQueries } = usePollingData<TopQueryEntry[]>(
    api.topQueries,
    [],
    60_000,
  );
  const { data: staleStats } = usePollingData<StaleStatEntry[]>(
    api.staleStats,
    [],
    3600_000,
  );
  const { data: replication } = usePollingData<ReplicaLag[]>(
    api.replication,
    [],
    30_000,
  );
  const { data: storage } = usePollingData<StorageEntry[]>(
    api.storage,
    [],
    300_000,
  );
  const { data: bloatSamples } = usePollingData<BloatSample[]>(
    api.bloat,
    [],
    3_600_000,
  );
  const { data: partitions } = usePollingData<PartitionSlice[]>(
    api.partitions,
    [],
    300_000,
  );
  const { data: unusedIndexes } = usePollingData<UnusedIndexEntry[]>(
    api.unusedIndexes,
    [],
    300_000,
  );
  const { data: wraparound } = usePollingData<WraparoundSnapshot>(
    api.wraparound,
    { databases: [], relations: [] },
    300_000,
  );

  return (
    <div className="app">
      <header className="app__header">
        <h1>PGMon</h1>
        <p>
          Monitoring cluster{" "}
          <strong>{overview?.cluster ?? "…"}</strong>{" "}
          {overview?.generated_at && (
            <span>· refreshed {formatRelativeTimestamp(overview.generated_at)}</span>
          )}
        </p>
        {overviewError && (
          <div className="error-banner">
            Failed to load overview: {overviewError}
          </div>
        )}
      </header>
      <main className="app__main">
        <OverviewPanel overview={overview} />
        <AlertsPanel overview={overview} />
        <ReplicationPanel replicas={replication} />
        <WraparoundPanel snapshot={wraparound} />
        <AutovacuumPanel tables={autovacuum} />
        <TopQueriesPanel queries={topQueries} />
        <StaleStatsPanel rows={staleStats} />
        <StoragePanel rows={storage} />
        <BloatPanel samples={bloatSamples} />
        <UnusedIndexPanel indexes={unusedIndexes} />
        <PartitionPanel slices={partitions} />
      </main>
      <footer className="app__footer">
        Prometheus metrics available at <code>/metrics</code> · UI refreshes every
        30s.
      </footer>
    </div>
  );
}

export default App;
