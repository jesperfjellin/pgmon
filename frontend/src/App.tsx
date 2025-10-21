import { useEffect, useMemo, useState } from "react";
import {
  Database,
  Activity,
  Gauge,
  Lock,
  Zap,
  BarChart2,
  Server,
  Layers,
  AlertTriangle,
  Settings,
  Clock4,
} from "lucide-react";
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
import { Badge, Card, CardHeader, CardBody, MetricCard, Section, SqlSnippet } from "./components/ui";

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

function classNames(...xs: (string | false | null | undefined)[]) {
  return xs.filter(Boolean).join(" ");
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

// ---------- Tab Definitions ----------
const tabs = [
  { key: "overview", label: "Overview", icon: <Gauge className="h-4 w-4" /> },
  { key: "workload", label: "Workload", icon: <Activity className="h-4 w-4" /> },
  { key: "autovac", label: "Autovacuum", icon: <Zap className="h-4 w-4" /> },
  { key: "storage", label: "Storage", icon: <Layers className="h-4 w-4" /> },
  { key: "bloat", label: "Bloat Deep", icon: <BarChart2 className="h-4 w-4" /> },
  { key: "stale-stats", label: "Stale Stats", icon: <Clock4 className="h-4 w-4" /> },
  { key: "partitions", label: "Partitions", icon: <BarChart2 className="h-4 w-4" /> },
  { key: "replication", label: "Replication", icon: <Server className="h-4 w-4" /> },
  { key: "alerts", label: "Alerts", icon: <AlertTriangle className="h-4 w-4" /> },
  { key: "wraparound", label: "Wraparound", icon: <Lock className="h-4 w-4" /> },
] as const;

// ---------- Panel Components ----------
function OverviewTab({ overview }: { overview: OverviewSnapshot | null }) {
  if (!overview) {
    return (
      <div className="space-y-4">
        <p className="text-sm text-slate-500">Loading...</p>
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
    <div className="space-y-6">
      {/* KPI cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-3">
        <MetricCard
          title="Connections"
          value={`${overview.connections}/${overview.max_connections}`}
          icon={<Database className="h-5 w-5" />}
          tone="violet"
          status={
            connectionRatio >= CONNECTION_CRIT
              ? "crit"
              : connectionRatio >= CONNECTION_WARN
              ? "warn"
              : undefined
          }
        />
        <MetricCard
          title="TPS"
          value={overview.tps ? overview.tps.toFixed(1) : "–"}
          icon={<Activity className="h-5 w-5" />}
          tone="green"
        />
        <MetricCard
          title="QPS"
          value={overview.qps ? overview.qps.toFixed(1) : "–"}
          icon={<Activity className="h-5 w-5" />}
          tone="blue"
        />
        <MetricCard
          title="Mean Latency"
          value={overview.mean_latency_ms ? overview.mean_latency_ms.toFixed(1) : "–"}
          unit="ms"
          icon={<Gauge className="h-5 w-5" />}
          tone="amber"
        />
        <MetricCard
          title="p95 Latency"
          value={overview.latency_p95_ms ? overview.latency_p95_ms.toFixed(1) : "–"}
          unit="ms"
          icon={<Gauge className="h-5 w-5" />}
          tone="rose"
        />
        <MetricCard
          title="Blocked Sessions"
          value={overview.blocked_sessions}
          icon={<Lock className="h-5 w-5" />}
          tone="slate"
          status={blockedCrit ? "crit" : blockedWarn ? "warn" : undefined}
        />
      </div>

      {/* Alerts + Blocking Chains */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2">
          <CardHeader
            title="Active Alerts"
            icon={<AlertTriangle className="h-4 w-4 text-amber-600" />}
            actions={
              <Badge tone={overview.open_alerts.length + overview.open_crit_alerts.length === 0 ? "green" : "yellow"}>
                {overview.open_alerts.length + overview.open_crit_alerts.length === 0
                  ? "Healthy"
                  : `${overview.open_alerts.length + overview.open_crit_alerts.length} alerts`}
              </Badge>
            }
          />
          <CardBody>
            {overview.open_crit_alerts.length === 0 && overview.open_alerts.length === 0 ? (
              <div className="text-sm text-slate-500">No active alerts.</div>
            ) : (
              <ul className="divide-y divide-slate-100">
                {overview.open_crit_alerts.map((alert, i) => (
                  <li key={`crit-${i}`} className="py-2 flex items-center gap-3">
                    <Badge tone="red">crit</Badge>
                    <div className="text-sm text-slate-800">{alert}</div>
                  </li>
                ))}
                {overview.open_alerts.map((alert, i) => (
                  <li key={`warn-${i}`} className="py-2 flex items-center gap-3">
                    <Badge tone="yellow">warn</Badge>
                    <div className="text-sm text-slate-800">{alert}</div>
                  </li>
                ))}
              </ul>
            )}
          </CardBody>
        </Card>
        <Card>
          <CardHeader title="Blocking Chains" icon={<Lock className="h-4 w-4 text-slate-500" />} />
          <CardBody>
            {topBlocking.length === 0 ? (
              <div className="text-sm text-slate-500">No blocking chains detected.</div>
            ) : (
              <div className="space-y-2">
                {topBlocking.slice(0, 5).map((event) => (
                  <div key={`${event.blocked_pid}-${event.blocker_pid}`} className="text-xs">
                    <div className="font-medium text-slate-900">
                      PID {event.blocked_pid} ← {event.blocker_pid}
                    </div>
                    <div className="text-slate-500">
                      Wait: {formatSeconds(event.blocked_wait_seconds)}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardBody>
        </Card>
      </div>

      <SqlSnippet sql={SQL_SNIPPETS.overview} />
    </div>
  );
}

function WorkloadTab({ queries }: { queries: TopQueryEntry[] }) {
  const topQueries = useMemo(() => queries.slice(0, 10), [queries]);

  return (
    <div className="space-y-4">
      <Section
        title="Top Queries"
        subtitle="By total execution time"
        icon={<Activity className="h-5 w-5 text-slate-500" />}
      />

      <Card>
        <CardBody>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 border-b border-slate-100">
                  <th className="py-2 pr-4">Query ID</th>
                  <th className="py-2 pr-4">Calls</th>
                  <th className="py-2 pr-4">Total Time (s)</th>
                  <th className="py-2 pr-4">Mean (ms)</th>
                  <th className="py-2 pr-4">Shared Blks Read</th>
                </tr>
              </thead>
              <tbody>
                {topQueries.map((q) => {
                  return (
                    <tr key={q.queryid} className="border-b border-slate-50 hover:bg-slate-50/60">
                      <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{q.queryid}</td>
                      <td className="py-2 pr-4">{numberFormatter.format(q.calls)}</td>
                      <td className="py-2 pr-4">{q.total_time_seconds.toFixed(2)}</td>
                      <td className="py-2 pr-4">{q.mean_time_ms.toFixed(2)}</td>
                      <td className="py-2 pr-4">{numberFormatter.format(q.shared_blks_read)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </CardBody>
      </Card>

      <SqlSnippet sql={SQL_SNIPPETS.topQueries} />
    </div>
  );
}

function AutovacTab({ tables }: { tables: AutovacuumEntry[] }) {
  const topTables = useMemo(() => tables.slice(0, 8), [tables]);

  return (
    <div className="space-y-4">
      <Section
        title="Autovacuum Health"
        subtitle="Dead tuples, freshness, and recent activity"
        icon={<Zap className="h-5 w-5 text-slate-500" />}
      />
      <Card>
        <CardBody>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 border-b border-slate-100">
                  <th className="py-2 pr-4">Relation</th>
                  <th className="py-2 pr-4">Dead Tuples</th>
                  <th className="py-2 pr-4">% Dead</th>
                  <th className="py-2 pr-4">Last Vacuum</th>
                  <th className="py-2 pr-4">Last Analyze</th>
                </tr>
              </thead>
              <tbody>
                {topTables.map((row) => {
                  const total = row.n_live_tup + row.n_dead_tup;
                  const pctDead = total > 0 ? (row.n_dead_tup / total) * 100 : undefined;
                  return (
                    <tr key={row.relation} className="border-b border-slate-50 hover:bg-slate-50/60">
                      <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{row.relation}</td>
                      <td className="py-2 pr-4">{numberFormatter.format(row.n_dead_tup)}</td>
                      <td className="py-2 pr-4">{pctDead !== undefined ? `${pctDead.toFixed(1)}%` : "—"}</td>
                      <td className="py-2 pr-4">{formatRelativeTimestamp(row.last_autovacuum ?? undefined)}</td>
                      <td className="py-2 pr-4">{formatRelativeTimestamp(row.last_autoanalyze ?? undefined)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </CardBody>
      </Card>
      <SqlSnippet sql={SQL_SNIPPETS.autovacuum} />
    </div>
  );
}

function StorageTab({ rows }: { rows: StorageEntry[] }) {
  const topRows = useMemo(() => rows.slice(0, 8), [rows]);
  return (
    <div className="space-y-4">
      <Section
        title="Largest Relations"
        subtitle="Heap + index + TOAST split"
        icon={<Layers className="h-5 w-5 text-slate-500" />}
      />

      <Card>
        <CardBody>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 border-b border-slate-100">
                  <th className="py-2 pr-4">Relation</th>
                  <th className="py-2 pr-4">Total Size</th>
                  <th className="py-2 pr-4">Heap</th>
                  <th className="py-2 pr-4">Indexes</th>
                  <th className="py-2 pr-4">TOAST</th>
                  <th className="py-2 pr-4">% Dead</th>
                  <th className="py-2 pr-4">Est. Bloat</th>
                </tr>
              </thead>
              <tbody>
                {topRows.map((row) => (
                  <tr key={row.relation} className="border-b border-slate-50 hover:bg-slate-50/60">
                    <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{row.relation}</td>
                    <td className="py-2 pr-4">{formatBytes(row.total_bytes)}</td>
                    <td className="py-2 pr-4">{formatBytes(row.table_bytes)}</td>
                    <td className="py-2 pr-4">{formatBytes(row.index_bytes)}</td>
                    <td className="py-2 pr-4">{formatBytes(row.toast_bytes)}</td>
                    <td className="py-2 pr-4">
                      {row.dead_tuple_ratio !== undefined ? `${row.dead_tuple_ratio.toFixed(1)}%` : "—"}
                    </td>
                    <td className="py-2 pr-4">
                      {row.estimated_bloat_bytes !== undefined && row.estimated_bloat_bytes !== null
                        ? formatBytes(row.estimated_bloat_bytes)
                        : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardBody>
      </Card>

      <SqlSnippet sql={SQL_SNIPPETS.storage} />
    </div>
  );
}

function BloatTab({ samples }: { samples: BloatSample[] }) {
  if (samples.length === 0) {
    return (
      <div className="space-y-4">
        <Section title="Bloat Samples" icon={<BarChart2 className="h-5 w-5 text-slate-500" />} />
        <Card>
          <CardBody>
            <p className="text-sm text-slate-500">
              No bloat data available. Ensure <code className="bg-slate-100 px-1 rounded">pgstattuple</code> extension is installed.
            </p>
          </CardBody>
        </Card>
        <SqlSnippet sql={SQL_SNIPPETS.bloat} />
      </div>
    );
  }

  const topSamples = useMemo(() => samples.slice(0, 20), [samples]);
  const hasAdvancedFields = topSamples.some((s) => s.dead_tuple_count != null);

  return (
    <div className="space-y-4">
      <Section
        title="Bloat Deep"
        subtitle={hasAdvancedFields ? "Exact mode" : "Approx mode"}
        icon={<BarChart2 className="h-5 w-5 text-slate-500" />}
      />
      <Card>
        <CardBody>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 border-b border-slate-100">
                  <th className="py-2 pr-4">Relation</th>
                  <th className="py-2 pr-4">Table Bytes</th>
                  <th className="py-2 pr-4">Free Bytes</th>
                  <th className="py-2 pr-4">Free %</th>
                  {hasAdvancedFields && (
                    <>
                      <th className="py-2 pr-4">Dead Tuples</th>
                      <th className="py-2 pr-4">Dead %</th>
                      <th className="py-2 pr-4">Live Tuples</th>
                      <th className="py-2 pr-4">Tuple Density %</th>
                    </>
                  )}
                </tr>
              </thead>
              <tbody>
                {topSamples.map((sample) => (
                  <tr key={sample.relation} className="border-b border-slate-50 hover:bg-slate-50/60">
                    <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{sample.relation}</td>
                    <td className="py-2 pr-4">{formatBytes(sample.table_bytes)}</td>
                    <td className="py-2 pr-4">{formatBytes(sample.free_bytes)}</td>
                    <td className="py-2 pr-4">{sample.free_percent.toFixed(1)}%</td>
                    {hasAdvancedFields && (
                      <>
                        <td className="py-2 pr-4">
                          {sample.dead_tuple_count != null ? sample.dead_tuple_count.toLocaleString() : "—"}
                        </td>
                        <td className="py-2 pr-4">
                          {sample.dead_tuple_percent != null ? sample.dead_tuple_percent.toFixed(1) + "%" : "—"}
                        </td>
                        <td className="py-2 pr-4">
                          {sample.live_tuple_count != null ? sample.live_tuple_count.toLocaleString() : "—"}
                        </td>
                        <td className="py-2 pr-4">
                          {sample.tuple_density != null ? sample.tuple_density.toFixed(1) + "%" : "—"}
                        </td>
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardBody>
      </Card>
      <SqlSnippet sql={SQL_SNIPPETS.bloat} />
    </div>
  );
}

function StaleStatsTab({ rows }: { rows: StaleStatEntry[] }) {
  const topRows = useMemo(() => rows.slice(0, 12), [rows]);

  return (
    <div className="space-y-4">
      <Section
        title="Stale Statistics"
        subtitle="Tables needing analyze"
        icon={<Clock4 className="h-5 w-5 text-slate-500" />}
      />
      <Card>
        <CardBody>
          {topRows.length === 0 ? (
            <div className="text-sm text-slate-500">No tables exceed the stale-stat thresholds.</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="text-left text-slate-500 border-b border-slate-100">
                    <th className="py-2 pr-4">Relation</th>
                    <th className="py-2 pr-4">Hours Since Analyze</th>
                    <th className="py-2 pr-4">Last Analyze</th>
                    <th className="py-2 pr-4">Last Autoanalyze</th>
                    <th className="py-2 pr-4">Live Tuples</th>
                  </tr>
                </thead>
                <tbody>
                  {topRows.map((row) => (
                    <tr key={row.relation} className="border-b border-slate-50 hover:bg-slate-50/60">
                      <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{row.relation}</td>
                      <td className="py-2 pr-4">{formatHours(row.hours_since_analyze)}</td>
                      <td className="py-2 pr-4">{formatRelativeTimestamp(row.last_analyze ?? undefined)}</td>
                      <td className="py-2 pr-4">{formatRelativeTimestamp(row.last_autoanalyze ?? undefined)}</td>
                      <td className="py-2 pr-4">{numberFormatter.format(row.n_live_tup)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardBody>
      </Card>
      <SqlSnippet sql={SQL_SNIPPETS.staleStats} />
    </div>
  );
}

function PartitionsTab({ slices }: { slices: PartitionSlice[] }) {
  if (slices.length === 0) {
    return (
      <div className="space-y-4">
        <Section title="Partitions" icon={<BarChart2 className="h-5 w-5 text-slate-500" />} />
        <Card>
          <CardBody>
            <p className="text-sm text-slate-500">No partitioned parents discovered.</p>
          </CardBody>
        </Card>
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

  return (
    <div className="space-y-4">
      <Section
        title="Partition Horizon"
        subtitle="Future coverage & gaps"
        icon={<BarChart2 className="h-5 w-5 text-slate-500" />}
      />
      <Card>
        <CardBody>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 border-b border-slate-100">
                  <th className="py-2 pr-4">Parent</th>
                  <th className="py-2 pr-4">Children</th>
                  <th className="py-2 pr-4">Latest Upper Bound</th>
                  <th className="py-2 pr-4">Cadence</th>
                  <th className="py-2 pr-4">Suggested Range</th>
                  <th className="py-2 pr-4">Gap</th>
                  <th className="py-2 pr-4">Status</th>
                </tr>
              </thead>
              <tbody>
                {sortedSlices.map((slice) => (
                  <tr key={slice.parent} className="border-b border-slate-50 hover:bg-slate-50/60">
                    <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{slice.parent}</td>
                    <td className="py-2 pr-4">{numberFormatter.format(slice.child_count)}</td>
                    <td className="py-2 pr-4">{formatRelativeTimestamp(slice.latest_partition_upper)}</td>
                    <td className="py-2 pr-4">{formatSeconds(slice.cadence_seconds)}</td>
                    <td className="py-2 pr-4">{formatRange(slice.suggested_next_start, slice.suggested_next_end)}</td>
                    <td className="py-2 pr-4">{formatSeconds(slice.future_gap_seconds)}</td>
                    <td className="py-2 pr-4">
                      {slice.missing_future_partition ? <Badge tone="yellow">Gap</Badge> : <Badge tone="green">Healthy</Badge>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardBody>
      </Card>
      <SqlSnippet sql={SQL_SNIPPETS.partitions} />
    </div>
  );
}

function ReplicationTab({ replicas }: { replicas: ReplicaLag[] }) {
  return (
    <div className="space-y-4">
      <Section
        title="Replication"
        subtitle="Replica lag (time/bytes)"
        icon={<Server className="h-5 w-5 text-slate-500" />}
      />
      <Card>
        <CardBody>
          {replicas.length === 0 ? (
            <div className="text-sm text-slate-500">No replication sessions detected.</div>
          ) : (
            <ul className="divide-y divide-slate-100">
              {replicas.map((replica) => (
                <li key={replica.replica} className="py-2 flex items-center justify-between">
                  <span className="text-sm text-slate-900">{replica.replica}</span>
                  <span className="text-sm text-slate-600">{formatSeconds(replica.lag_seconds)}</span>
                </li>
              ))}
            </ul>
          )}
        </CardBody>
      </Card>
      <SqlSnippet sql={SQL_SNIPPETS.replication} />
    </div>
  );
}

function AlertsTab({ overview }: { overview: OverviewSnapshot | null }) {
  const warnAlerts = overview?.open_alerts ?? [];
  const critAlerts = overview?.open_crit_alerts ?? [];

  return (
    <div className="space-y-4">
      <Section
        title="Alerts"
        subtitle="Current and recent alerts"
        icon={<AlertTriangle className="h-5 w-5 text-amber-600" />}
      />
      <Card>
        <CardBody>
          {critAlerts.length === 0 && warnAlerts.length === 0 ? (
            <div className="text-sm text-slate-500">No active alerts.</div>
          ) : (
            <ul className="divide-y divide-slate-100">
              {critAlerts.map((alert, i) => (
                <li key={`crit-${i}`} className="py-2 flex items-center gap-3">
                  <Badge tone="red">crit</Badge>
                  <div className="text-sm text-slate-800">{alert}</div>
                </li>
              ))}
              {warnAlerts.map((alert, i) => (
                <li key={`warn-${i}`} className="py-2 flex items-center gap-3">
                  <Badge tone="yellow">warn</Badge>
                  <div className="text-sm text-slate-800">{alert}</div>
                </li>
              ))}
            </ul>
          )}
        </CardBody>
      </Card>
    </div>
  );
}

function WraparoundTab({ snapshot }: { snapshot: WraparoundSnapshot }) {
  const topDatabases = snapshot.databases.slice(0, 5);
  const topRelations = snapshot.relations.slice(0, 5);
  return (
    <div className="space-y-4">
      <Section
        title="Wraparound Safety"
        subtitle="Transaction age monitoring"
        icon={<Lock className="h-5 w-5 text-slate-500" />}
      />
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card>
          <CardHeader title="Databases" />
          <CardBody>
            {topDatabases.length === 0 ? (
              <div className="text-sm text-slate-500">n/a</div>
            ) : (
              <ul className="divide-y divide-slate-100">
                {topDatabases.map((row) => (
                  <li key={row.database} className="py-2 flex items-center justify-between">
                    <span className="text-sm text-slate-900">{row.database}</span>
                    <span className="text-sm text-slate-600">{numberFormatter.format(row.tx_age)}</span>
                  </li>
                ))}
              </ul>
            )}
          </CardBody>
        </Card>
        <Card>
          <CardHeader title="Relations" />
          <CardBody>
            {topRelations.length === 0 ? (
              <div className="text-sm text-slate-500">n/a</div>
            ) : (
              <ul className="divide-y divide-slate-100">
                {topRelations.map((row) => (
                  <li key={row.relation} className="py-2 flex items-center justify-between">
                    <span className="text-sm text-slate-900 font-mono text-xs">{row.relation}</span>
                    <span className="text-sm text-slate-600">{numberFormatter.format(row.tx_age)}</span>
                  </li>
                ))}
              </ul>
            )}
          </CardBody>
        </Card>
      </div>
      <SqlSnippet sql={SQL_SNIPPETS.wraparound} />
    </div>
  );
}

function App() {
  const [active, setActive] = useState("overview");

  const { data: overview, error: overviewError } = usePollingData<OverviewSnapshot | null>(api.overview, null, 15_000);
  const { data: autovacuum } = usePollingData<AutovacuumEntry[]>(api.autovacuum, [], 60_000);
  const { data: topQueries } = usePollingData<TopQueryEntry[]>(api.topQueries, [], 60_000);
  const { data: staleStats } = usePollingData<StaleStatEntry[]>(api.staleStats, [], 3600_000);
  const { data: replication } = usePollingData<ReplicaLag[]>(api.replication, [], 30_000);
  const { data: storage } = usePollingData<StorageEntry[]>(api.storage, [], 300_000);
  const { data: bloatSamples } = usePollingData<BloatSample[]>(api.bloat, [], 3_600_000);
  const { data: partitions } = usePollingData<PartitionSlice[]>(api.partitions, [], 300_000);
  const { data: unusedIndexes } = usePollingData<UnusedIndexEntry[]>(api.unusedIndexes, [], 300_000);
  const { data: wraparound } = usePollingData<WraparoundSnapshot>(api.wraparound, { databases: [], relations: [] }, 300_000);

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-50 to-white text-slate-900">
      {/* Top Nav */}
      <div className="border-b border-slate-200 bg-white/70 backdrop-blur sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="h-9 w-9 rounded-xl bg-sky-500 text-white grid place-items-center shadow-sm">
              <Database className="h-5 w-5" />
            </div>
            <div>
              <div className="text-sm text-slate-500">Monitoring cluster</div>
              <div className="font-semibold">{overview?.cluster ?? "…"}</div>
            </div>
            {overview?.generated_at && (
              <Badge tone="green">refreshed {formatRelativeTimestamp(overview.generated_at)}</Badge>
            )}
          </div>
          {overviewError && (
            <div className="text-sm text-rose-600">Error: {overviewError}</div>
          )}
        </div>
      </div>

      {/* Body */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6 grid grid-cols-1 lg:grid-cols-[230px_1fr] gap-6">
        {/* Sidebar */}
        <aside className="lg:sticky lg:top-16 self-start">
          <Card>
            <CardBody>
              <nav className="flex flex-col gap-1">
                {tabs.map((t) => (
                  <button
                    key={t.key}
                    onClick={() => setActive(t.key)}
                    className={classNames(
                      "w-full flex items-center gap-2 px-3 py-2 rounded-xl text-sm",
                      active === t.key ? "bg-sky-50 text-sky-700 border border-sky-100" : "hover:bg-slate-50 text-slate-700"
                    )}
                  >
                    {t.icon}
                    <span>{t.label}</span>
                  </button>
                ))}
              </nav>
            </CardBody>
          </Card>
          <div className="mt-4 text-xs text-slate-500 px-2">
            UI refreshes every 30s · Prom metrics at <code className="font-mono">/metrics</code>
          </div>
        </aside>

        {/* Main Content */}
        <main>
          {active === "overview" && <OverviewTab overview={overview} />}
          {active === "workload" && <WorkloadTab queries={topQueries} />}
          {active === "autovac" && <AutovacTab tables={autovacuum} />}
          {active === "storage" && <StorageTab rows={storage} />}
          {active === "bloat" && <BloatTab samples={bloatSamples} />}
          {active === "stale-stats" && <StaleStatsTab rows={staleStats} />}
          {active === "partitions" && <PartitionsTab slices={partitions} />}
          {active === "replication" && <ReplicationTab replicas={replication} />}
          {active === "alerts" && <AlertsTab overview={overview} />}
          {active === "wraparound" && <WraparoundTab snapshot={wraparound} />}
        </main>
      </div>
    </div>
  );
}

export default App;
