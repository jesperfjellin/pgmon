import { useEffect, useMemo, useState } from "react";
import "./App.css";
import {
  api,
  AutovacuumEntry,
  OverviewSnapshot,
  PartitionSlice,
  ReplicaLag,
  StorageEntry,
  TopQueryEntry,
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

function formatSeconds(value?: number) {
  if (value === undefined || value === null || value < 0) {
    return "–";
  }
  if (value < 60) {
    return `${value.toFixed(0)}s`;
  }
  if (value < 3600) {
    return `${(value / 60).toFixed(1)}m`;
  }
  return `${(value / 3600).toFixed(1)}h`;
}

function formatRelativeTimestamp(value?: number) {
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

function warnClass(condition: boolean, crit = false) {
  if (crit && condition) {
    return "status status--crit";
  }
  if (condition) {
    return "status status--warn";
  }
  return "status status--ok";
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
      </div>
      <p className="muted">
        Updated {formatRelativeTimestamp(overview.generated_at)}
      </p>
      <SqlSnippet sql={SQL_SNIPPETS.overview} />
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
                    <td>{formatRelativeTimestamp(row.last_autovacuum)}</td>
                    <td>{formatRelativeTimestamp(row.last_autoanalyze)}</td>
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
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <SqlSnippet sql={SQL_SNIPPETS.storage} />
      <SqlSnippet sql={SQL_SNIPPETS.partitions} />
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
  return (
    <div className="panel wide">
      <h2>Partition Horizon</h2>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Parent</th>
              <th className="numeric">Children</th>
              <th>Oldest</th>
              <th>Newest</th>
              <th>Next Expected</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {slices.map((slice) => (
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
                <td>{formatRelativeTimestamp(slice.next_expected_partition)}</td>
                <td>
                  <span
                    className={warnClass(
                      slice.missing_future_partition,
                      slice.missing_future_partition,
                    )}
                  >
                    {slice.missing_future_partition ? "At Risk" : "Healthy"}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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
  const { data: partitions } = usePollingData<PartitionSlice[]>(
    api.partitions,
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
        <StoragePanel rows={storage} />
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
