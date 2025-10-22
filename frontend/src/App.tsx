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
  AlertEvent,
  OverviewSnapshot,
  PartitionSlice,
  Recommendation,
  RecommendationsResponse,
  ReplicaLag,
  StaleStatEntry,
  StorageEntry,
  TopQueryEntry,
  UnusedIndexEntry,
  WraparoundSnapshot,
  createPoller,
} from "./api";
import { Badge, Card, CardHeader, CardBody, MetricCard, Section, SqlSnippet, formatPercentMaybe } from "./components/ui";
import { useRef } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from 'recharts';

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
  { key: "autovac", label: "Autovac", icon: <Zap className="h-4 w-4" /> },
  { key: "storage", label: "Storage", icon: <Layers className="h-4 w-4" /> },
  { key: "bloat", label: "Bloat", icon: <BarChart2 className="h-4 w-4" /> },
  { key: "recommendations", label: "Recommendations", icon: <Zap className="h-4 w-4" /> },
  { key: "history", label: "History", icon: <Activity className="h-4 w-4" /> },
  { key: "stale-stats", label: "Stale Stats", icon: <Clock4 className="h-4 w-4" /> },
  { key: "indexes", label: "Indexes", icon: <Layers className="h-4 w-4" /> },
  { key: "partitions", label: "Partitions", icon: <BarChart2 className="h-4 w-4" /> },
  { key: "replication", label: "Replication", icon: <Server className="h-4 w-4" /> },
  { key: "alerts", label: "Alerts", icon: <AlertTriangle className="h-4 w-4" /> },
  { key: "wraparound", label: "Wraparound", icon: <Lock className="h-4 w-4" /> },
] as const;

// ---------- Panel Components ----------
function OverviewTab({ overview }: { overview: OverviewSnapshot | null }) {
  // History-derived KPI series
  const [loaded, setLoaded] = useState(false);
  const [selectedMetric, setSelectedMetric] = useState<string | null>("tps"); // Start with TPS selected
  const [series, setSeries] = useState<{
    connections: { ts: number; value: number }[];
    tps: { ts: number; value: number }[];
    qps: { ts: number; value: number }[];
    mean_latency_ms: { ts: number; value: number }[];
    latency_p95_ms: { ts: number; value: number }[];
    latency_p99_ms: { ts: number; value: number }[];
    blocked_sessions: { ts: number; value: number }[];
  }>({
    connections: [],
    tps: [],
    qps: [],
    mean_latency_ms: [],
    latency_p95_ms: [],
    latency_p99_ms: [],
    blocked_sessions: [],
  });
  // Track last timestamp to enable incremental polling
  const lastTsRef = useRef<number | null>(null);

  // Initial fetch + polling of combined overview history
  useEffect(() => {
    let stopped = false;
    const shape = (pts: any[]) => pts.map(p => ({ ts: p.ts * 1000, value: p.value }));
    const fullLoad = async () => {
      try {
        const resp = await fetch(`${api.overviewHistory}?window=1h&max_points=300`);
        const json = await resp.json();
        if (stopped) return;
        setSeries({
          connections: shape(json.connections || []),
          tps: shape(json.tps || []),
          qps: shape(json.qps || []),
            mean_latency_ms: shape(json.mean_latency_ms || []),
            latency_p95_ms: shape(json.latency_p95_ms || []),
            latency_p99_ms: shape(json.latency_p99_ms || []),
            blocked_sessions: shape(json.blocked_sessions || []),
        });
        // Establish lastTs from maximum across all returned series
        const allTs = [
          ...((json.connections || []).map((p:any)=>p.ts)),
          ...((json.tps || []).map((p:any)=>p.ts)),
          ...((json.qps || []).map((p:any)=>p.ts)),
          ...((json.mean_latency_ms || []).map((p:any)=>p.ts)),
          ...((json.latency_p95_ms || []).map((p:any)=>p.ts)),
          ...((json.latency_p99_ms || []).map((p:any)=>p.ts)),
          ...((json.blocked_sessions || []).map((p:any)=>p.ts)),
        ];
        lastTsRef.current = allTs.length ? Math.max(...allTs) * 1000 : null; // store ms
        setLoaded(true);
      } catch (e) {
        console.error('overview history fetch failed', e);
      }
    };
    const incremental = async () => {
      if (lastTsRef.current == null) {
        return fullLoad();
      }
      try {
        const sinceSeconds = Math.floor(lastTsRef.current / 1000);
        const resp = await fetch(`${api.overviewHistory}?window=1h&max_points=300&since=${sinceSeconds}`);
        const json = await resp.json();
        if (stopped) return;
        // Partial response may return empty arrays; merge only new points
        const merge = (curr: {ts:number;value:number}[], incoming: any[]) => {
          if (!Array.isArray(incoming) || incoming.length === 0) return curr;
          const mapped = shape(incoming);
          // Drop any duplicates (ts equality) in case of race
          const existingSet = new Set(curr.map(p => p.ts));
          const appended = mapped.filter(p => !existingSet.has(p.ts));
          return [...curr, ...appended];
        };
        setSeries(prev => ({
          connections: merge(prev.connections, json.connections || []),
          tps: merge(prev.tps, json.tps || []),
          qps: merge(prev.qps, json.qps || []),
          mean_latency_ms: merge(prev.mean_latency_ms, json.mean_latency_ms || []),
          latency_p95_ms: merge(prev.latency_p95_ms, json.latency_p95_ms || []),
          latency_p99_ms: merge(prev.latency_p99_ms, json.latency_p99_ms || []),
          blocked_sessions: merge(prev.blocked_sessions, json.blocked_sessions || []),
        }));
        const newTs = [
          ...((json.connections || []).map((p:any)=>p.ts)),
          ...((json.tps || []).map((p:any)=>p.ts)),
          ...((json.qps || []).map((p:any)=>p.ts)),
          ...((json.mean_latency_ms || []).map((p:any)=>p.ts)),
          ...((json.latency_p95_ms || []).map((p:any)=>p.ts)),
          ...((json.latency_p99_ms || []).map((p:any)=>p.ts)),
          ...((json.blocked_sessions || []).map((p:any)=>p.ts)),
        ];
        if (newTs.length) {
          const newest = Math.max(...newTs) * 1000;
          if (newest > (lastTsRef.current ?? 0)) {
            lastTsRef.current = newest;
          }
        }
      } catch (e) {
        console.error('overview incremental history fetch failed', e);
      }
    };
    fullLoad();
    const id = setInterval(incremental, 15_000);
    return () => { stopped = true; clearInterval(id); };
  }, []);

  // Derive current values from last history point.
  const last = <T extends { ts: number; value: number }[]>(xs: T) => (xs.length ? xs[xs.length - 1].value : undefined);
  const currentConnections = last(series.connections);
  const currentTps = last(series.tps);
  const currentQps = last(series.qps);
  const currentMeanLatency = last(series.mean_latency_ms);
  const currentP95Latency = last(series.latency_p95_ms);
  const currentP99Latency = last(series.latency_p99_ms);
  const currentBlocked = last(series.blocked_sessions);

  // Need overview for max_connections for ratio calculation.
  if (!overview || !loaded) {
    return <div className="text-sm text-slate-500">Loading history…</div>;
  }

  const connectionRatio = overview.max_connections > 0 && currentConnections !== undefined
    ? currentConnections / overview.max_connections
    : 0;
  const longestBlocked = overview.longest_blocked_seconds ?? 0;
  const blockedWarn = (currentBlocked ?? 0) > 0 || longestBlocked >= BLOCKED_WARN;
  const blockedCrit = longestBlocked >= BLOCKED_CRIT;

  // Metric configuration
  const metrics = [
    { key: 'connections', title: 'Connections', value: currentConnections !== undefined ? `${Math.round(currentConnections)}/${overview.max_connections}` : '–', tone: 'violet' as const, unit: undefined, status: connectionRatio >= CONNECTION_CRIT ? 'crit' as const : connectionRatio >= CONNECTION_WARN ? 'warn' as const : undefined },
    { key: 'tps', title: 'TPS', value: currentTps !== undefined ? currentTps.toFixed(1) : '–', tone: 'green' as const, unit: undefined, status: undefined },
    { key: 'qps', title: 'QPS', value: currentQps !== undefined ? currentQps.toFixed(1) : '–', tone: 'blue' as const, unit: undefined, status: undefined },
    { key: 'mean_latency_ms', title: 'Mean Latency', value: currentMeanLatency !== undefined ? currentMeanLatency.toFixed(1) : '–', tone: 'amber' as const, unit: 'ms', status: undefined },
    { key: 'latency_p95_ms', title: 'p95 Latency', value: currentP95Latency !== undefined ? currentP95Latency.toFixed(1) : '–', tone: 'rose' as const, unit: 'ms', status: undefined },
    { key: 'latency_p99_ms', title: 'p99 Latency', value: currentP99Latency !== undefined ? currentP99Latency.toFixed(1) : '–', tone: 'red' as const, unit: 'ms', status: undefined },
    { key: 'blocked_sessions', title: 'Blocked Sessions', value: currentBlocked !== undefined ? Math.round(currentBlocked) : '–', tone: 'slate' as const, unit: undefined, status: blockedCrit ? 'crit' as const : blockedWarn ? 'warn' as const : undefined },
  ];

  // Get selected metric's data
  const selectedMetricData = selectedMetric ? series[selectedMetric as keyof typeof series] || [] : [];
  const selectedMetricConfig = metrics.find(m => m.key === selectedMetric);

  return (
    <div className="space-y-6">
      {/* KPI cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-7 gap-3">
        {metrics.map(metric => (
          <MetricCard
            key={metric.key}
            title={metric.title}
            value={metric.value}
            unit={metric.unit}
            tone={metric.tone}
            status={metric.status}
            series={series[metric.key as keyof typeof series].map(p => ({ value: p.value }))}
            onClick={() => setSelectedMetric(metric.key)}
            isActive={selectedMetric === metric.key}
          />
        ))}
      </div>

      {/* Full-size chart for selected metric */}
      {selectedMetric && selectedMetricData.length > 0 && (
        <Card>
          <CardHeader
            title={`${selectedMetricConfig?.title || selectedMetric} History`}
            icon={<Activity className="h-4 w-4 text-slate-500" />}
            actions={
              <button
                onClick={() => setSelectedMetric(null)}
                className="text-xs text-slate-500 hover:text-slate-700"
              >
                Close
              </button>
            }
          />
          <CardBody>
            <div className="h-80 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={selectedMetricData} margin={{ left: 8, right: 16, top: 8, bottom: 8 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis
                    dataKey="ts"
                    type="number"
                    domain={['auto','auto']}
                    tickFormatter={(ts) => new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    stroke="#64748b"
                  />
                  <YAxis stroke="#64748b" />
                  <Tooltip
                    labelFormatter={(ts) => new Date(ts as number).toLocaleString()}
                    contentStyle={{ backgroundColor: 'white', border: '1px solid #e2e8f0', borderRadius: '8px' }}
                  />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="value"
                    stroke={selectedMetricConfig?.tone === 'blue' ? '#0ea5e9' : selectedMetricConfig?.tone === 'green' ? '#10b981' : selectedMetricConfig?.tone === 'amber' ? '#f59e0b' : selectedMetricConfig?.tone === 'rose' ? '#ef4444' : selectedMetricConfig?.tone === 'violet' ? '#8b5cf6' : selectedMetricConfig?.tone === 'red' ? '#ef4444' : '#64748b'}
                    strokeWidth={2}
                    dot={false}
                    name={selectedMetricConfig?.title || selectedMetric}
                    animationDuration={300}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardBody>
        </Card>
      )}

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
                  <th className="py-2 pr-4">Cache Hit %</th>
                </tr>
              </thead>
              <tbody>
                {topQueries.map((q) => {
                  const cacheHitPercent = (q.cache_hit_ratio * 100);
                  let barColor = "bg-slate-300";
                  if (q.cache_hit_ratio >= 0.99) {
                    barColor = "bg-green-500";
                  } else if (q.cache_hit_ratio >= 0.95) {
                    barColor = "bg-amber-500";
                  } else {
                    barColor = "bg-red-500";
                  }

                  return (
                    <tr key={q.queryid} className="border-b border-slate-50 hover:bg-slate-50/60">
                      <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{q.queryid}</td>
                      <td className="py-2 pr-4">{numberFormatter.format(q.calls)}</td>
                      <td className="py-2 pr-4">{q.total_time_seconds.toFixed(2)}</td>
                      <td className="py-2 pr-4">{q.mean_time_ms.toFixed(2)}</td>
                      <td className="py-2 pr-4">
                        <div className="flex items-center gap-2 min-w-[120px]">
                          <div className="flex-1 bg-slate-100 rounded h-4 overflow-hidden">
                            <div
                              className={`h-full ${barColor}`}
                              style={{ width: `${cacheHitPercent}%` }}
                            />
                          </div>
                          <span className="text-xs font-mono w-12 text-right">
                            {cacheHitPercent.toFixed(1)}%
                          </span>
                        </div>
                      </td>
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
                  <th className="py-2 pr-4">Cache Hit %</th>
                  <th className="py-2 pr-4">% Dead</th>
                  <th className="py-2 pr-4">Est. Bloat</th>
                </tr>
              </thead>
              <tbody>
                {topRows.map((row) => {
                  const cacheHitRatio = row.cache_hit_ratio;
                  let cacheColorClass = "text-slate-400";
                  if (cacheHitRatio !== null && cacheHitRatio !== undefined) {
                    if (cacheHitRatio >= 0.99) {
                      cacheColorClass = "text-green-600 font-semibold";
                    } else if (cacheHitRatio >= 0.95) {
                      cacheColorClass = "text-amber-600 font-semibold";
                    } else {
                      cacheColorClass = "text-red-600 font-semibold";
                    }
                  }

                  return (
                    <tr key={row.relation} className="border-b border-slate-50 hover:bg-slate-50/60">
                      <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{row.relation}</td>
                      <td className="py-2 pr-4">{formatBytes(row.total_bytes)}</td>
                      <td className="py-2 pr-4">{formatBytes(row.table_bytes)}</td>
                      <td className="py-2 pr-4">{formatBytes(row.index_bytes)}</td>
                      <td className="py-2 pr-4">{formatBytes(row.toast_bytes)}</td>
                      <td className={`py-2 pr-4 ${cacheColorClass}`}>
                        {cacheHitRatio !== null && cacheHitRatio !== undefined
                          ? `${(cacheHitRatio * 100).toFixed(1)}%`
                          : "—"}
                      </td>
                      <td className="py-2 pr-4">
                        {formatPercentMaybe(row.dead_tuple_ratio)}
                      </td>
                      <td className="py-2 pr-4">
                        {row.estimated_bloat_bytes !== undefined && row.estimated_bloat_bytes !== null
                          ? formatBytes(row.estimated_bloat_bytes)
                          : "—"}
                      </td>
                    </tr>
                  );
                })}
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
  // Move hooks before early return to comply with Rules of Hooks
  const topSamples = useMemo(() => samples.slice(0, 20), [samples]);
  const hasAdvancedFields = useMemo(
    () => topSamples.some((s) => s.dead_tuple_count != null),
    [topSamples]
  );

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
  // Move hooks before early return to comply with Rules of Hooks
  const sortedSlices = useMemo(() => {
    return [...slices].sort((a, b) => {
      if (a.missing_future_partition === b.missing_future_partition) {
        return a.parent.localeCompare(b.parent);
      }
      return a.missing_future_partition ? -1 : 1;
    });
  }, [slices]);

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
  // Alert timeline polling
  const [alertEvents, setAlertEvents] = useState<AlertEvent[]>([]);
  useEffect(() => {
    return createPoller<AlertEvent[]>(api.alertsHistory + '?limit=200', setAlertEvents, console.error, 30000);
  }, []);

  // Derive current active alerts from timeline (those without cleared_at)
  const activeWarnAlerts = alertEvents.filter(e => e.severity === 'warn' && e.cleared_at == null).map(e => e.message);
  const activeCritAlerts = alertEvents.filter(e => e.severity === 'crit' && e.cleared_at == null).map(e => e.message);
  const warnAlerts = activeWarnAlerts;
  const critAlerts = activeCritAlerts;

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

function RecommendationsTab({ recommendations }: { recommendations: Recommendation[] }) {
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);

  const handleCopy = (sql: string, index: number) => {
    navigator.clipboard.writeText(sql);
    setCopiedIndex(index);
    setTimeout(() => setCopiedIndex(null), 2000);
  };

  const getSeverityColor = (severity: string) => {
    if (severity === "crit") return "red";
    if (severity === "warn") return "yellow";
    return "blue";
  };

  const getKindLabel = (kind: string) => {
    if (kind === "vacuum_analyze") return "VACUUM ANALYZE";
    if (kind === "vacuum_full") return "VACUUM FULL";
    if (kind === "analyze") return "ANALYZE";
    if (kind === "reindex") return "REINDEX";
    return kind;
  };

  return (
    <div className="space-y-4">
      <Section
        title="Recommendations"
        subtitle={recommendations.length === 0 ? "All healthy!" : `${recommendations.length} maintenance suggestions`}
        icon={<Zap className="h-5 w-5 text-amber-500" />}
      />

      {recommendations.length === 0 ? (
        <Card>
          <CardBody>
            <div className="text-center py-8">
              <div className="text-4xl mb-2">✨</div>
              <div className="text-lg font-semibold text-slate-700">No recommendations</div>
              <div className="text-sm text-slate-500 mt-1">All tables are healthy!</div>
            </div>
          </CardBody>
        </Card>
      ) : (
        <div className="space-y-3">
          {recommendations.map((rec, index) => (
            <Card key={`${rec.relation}-${rec.kind}-${index}`}>
              <CardBody>
                <div className="space-y-3">
                  {/* Header */}
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex items-center gap-2">
                      <Badge tone={getSeverityColor(rec.severity)}>{rec.severity}</Badge>
                      <Badge tone="slate">{getKindLabel(rec.kind)}</Badge>
                      <span className="font-mono text-sm text-slate-700">{rec.relation}</span>
                    </div>
                  </div>

                  {/* Rationale */}
                  <div className="text-sm text-slate-600 leading-relaxed">
                    {rec.rationale}
                  </div>

                  {/* Impact details */}
                  <div className="flex items-center gap-4 text-xs text-slate-500">
                    {rec.impact.estimated_duration_seconds && (
                      <span>~{rec.impact.estimated_duration_seconds}s duration</span>
                    )}
                    {rec.impact.locks_table && (
                      <span className="text-amber-600 font-semibold">⚠️ Locks table</span>
                    )}
                    {rec.impact.reclaim_bytes && (
                      <span>Reclaim: {formatBytes(rec.impact.reclaim_bytes)}</span>
                    )}
                  </div>

                  {/* SQL Command */}
                  <div className="relative">
                    <pre className="bg-slate-800 text-slate-100 p-3 rounded text-sm font-mono overflow-x-auto">
                      {rec.sql_command}
                    </pre>
                    <button
                      onClick={() => handleCopy(rec.sql_command, index)}
                      className="absolute top-2 right-2 px-2 py-1 text-xs bg-slate-700 hover:bg-slate-600 text-slate-100 rounded transition-colors"
                    >
                      {copiedIndex === index ? "Copied!" : "Copy SQL"}
                    </button>
                  </div>
                </div>
              </CardBody>
            </Card>
          ))}
        </div>
      )}
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

function HistoryCharts() {
  const [points, setPoints] = useState<{ ts: number; tps: number; qps: number }[]>([]);

  useEffect(() => {
    // Poll both endpoints sequentially and merge by timestamp (assumes identical ts ordering)
    const poll = async () => {
      try {
        const tpsResp = await fetch(api.metricHistory('tps') + '?window=6h&max_points=300');
        const qpsResp = await fetch(api.metricHistory('qps') + '?window=6h&max_points=300');
        const tpsData = await tpsResp.json();
        const qpsData = await qpsResp.json();
        const tpsPoints: { ts: number; value: number }[] = (tpsData.points || []).map((p: any) => ({ ts: p.ts * 1000, value: p.value }));
        const qpsPoints: { ts: number; value: number }[] = (qpsData.points || []).map((p: any) => ({ ts: p.ts * 1000, value: p.value }));
        // Merge by index (simplest) – could be by ts if lengths differ
        const merged: { ts: number; tps: number; qps: number }[] = tpsPoints.map((p, i) => ({ ts: p.ts, tps: p.value, qps: qpsPoints[i]?.value ?? NaN }));
        setPoints(merged);
      } catch (e) {
        console.error('history poll error', e);
      }
    };
    poll();
    const id = setInterval(poll, 60_000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="space-y-4">
      <Section title="History (6h)" subtitle="TPS & QPS" icon={<Activity className="h-5 w-5 text-slate-500" />} />
      <Card>
        <CardBody>
          <div className="h-64 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={points} margin={{ left: 8, right: 16, top: 8, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="ts" type="number" domain={['auto','auto']} tickFormatter={(ts) => new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} />
                <YAxis />
                <Tooltip labelFormatter={(ts) => new Date(ts as number).toLocaleTimeString()} />
                <Legend />
                <Line type="monotone" dataKey="tps" stroke="#0ea5e9" strokeWidth={2} dot={false} name="TPS" />
                <Line type="monotone" dataKey="qps" stroke="#6366f1" strokeWidth={2} dot={false} name="QPS" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </CardBody>
      </Card>
      <SqlSnippet sql={`GET ${api.metricHistory('tps')}?window=6h&max_points=300`} />
    </div>
  );
}

export default App;

// Stable default arrays to prevent infinite re-renders
const EMPTY_BLOCKING_EVENTS: BlockingEvent[] = [];
const EMPTY_ALERTS: string[] = [];

// Stable default object to prevent infinite re-renders
const EMPTY_OVERVIEW: OverviewSnapshot = {
  cluster: '',
  connections: 0,
  max_connections: 0,
  blocked_sessions: 0,
  blocking_events: EMPTY_BLOCKING_EVENTS,
  open_alerts: EMPTY_ALERTS,
  open_crit_alerts: EMPTY_ALERTS,
};

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
  const { data: recommendationsData } = usePollingData<RecommendationsResponse>(api.recommendations, { recommendations: [] }, 60_000);

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
        <aside className="lg:sticky lg:top-16 self-start space-y-3">
          <Card>
            <CardBody className="py-3">
              <nav className="flex flex-col gap-1 text-sm">
                {tabs.map(t => (
                  <button
                    key={t.key}
                    onClick={() => setActive(t.key)}
                    className={classNames(
                      'w-full group flex items-center gap-2 px-3 py-2 rounded-xl text-sm transition-colors',
                      active === t.key ? 'bg-sky-50 text-sky-700 border border-sky-100 shadow-sm' : 'hover:bg-slate-50 text-slate-700'
                    )}
                  >
                    {t.icon}
                    <span>{t.label}</span>
                  </button>
                ))}
              </nav>
            </CardBody>
          </Card>
          <div className="text-xs text-slate-500 px-1">UI refresh ~15s · Prom metrics at <code className="font-mono">/metrics</code></div>
        </aside>
        <main className="space-y-6">
          {active === 'overview' && <OverviewTab overview={overview ?? EMPTY_OVERVIEW} />}
          {active === 'workload' && <WorkloadTab queries={topQueries} />}
          {active === 'autovac' && <AutovacTab tables={autovacuum} />}
          {active === 'storage' && <StorageTab rows={storage} />}
          {active === 'bloat' && <BloatTab samples={bloatSamples} />}
          {active === 'recommendations' && <RecommendationsTab recommendations={recommendationsData.recommendations} />}
          {active === 'history' && <HistoryCharts />}
          {active === 'stale-stats' && <StaleStatsTab rows={staleStats} />}
          {active === 'replication' && <ReplicationTab replicas={replication} />}
          {active === 'partitions' && <PartitionsTab slices={partitions} />}
          {active === 'alerts' && <AlertsTab overview={overview} />}
          {active === 'wraparound' && <WraparoundTab snapshot={wraparound} />}
          {active === 'indexes' && (
            <div className="space-y-4">
              <Section title="Unused Indexes" icon={<Layers className="h-5 w-5 text-slate-500" />} />
              <Card><CardBody>
                {unusedIndexes.length === 0 ? <div className="text-sm text-slate-500">No unused indexes detected.</div> : (
                  <table className="min-w-full text-sm"><thead><tr className="text-left text-slate-500 border-b border-slate-100"><th className="py-2 pr-4">Relation</th><th className="py-2 pr-4">Index</th><th className="py-2 pr-4">Bytes</th></tr></thead><tbody>{unusedIndexes.map(ix => (
                    <tr key={ix.relation+ix.index} className="border-b border-slate-50 hover:bg-slate-50/60"><td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{ix.relation}</td><td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{ix.index}</td><td className="py-2 pr-4">{formatBytes(ix.bytes)}</td></tr>
                  ))}</tbody></table>
                )}
              </CardBody></Card>
              <SqlSnippet sql={SQL_SNIPPETS.unusedIndexes} />
            </div>
          )}
          {/* legacy alias fallback */}
          {active === 'stale' && <StaleStatsTab rows={staleStats} />}
        </main>
      </div>
    </div>
  );
}
