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
  TrendingUp,
  X,
} from "lucide-react";
import {
  api,
  AutovacuumEntry,
  BlockingEvent,
  BloatSample,
  AlertEvent,
  Forecast,
  ForecastsResponse,
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
import { DataTable, ColumnDef } from "./components/DataTable";
import { useRef } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend, ComposedChart, ReferenceArea, ReferenceLine } from 'recharts';

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

function formatRelativeTimestamp(value?: string | null) {
  if (!value) {
    return "never";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime()) || date.getUTCFullYear() < 2000) {
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

// Helper to extract numeric value from backend's { source, parsedValue } format
function extractNumericValue(value?: number | { source: string; parsedValue: number } | null): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return value;
  return value.parsedValue;
}

function formatRange(
  start?: number | string | null,
  end?: number | string | null,
): string {
  // Convert epoch numbers to ISO strings for formatRelativeTimestamp
  const toISOString = (val?: number | string | null): string | null => {
    if (!val) return null;
    if (typeof val === 'string') return val;
    return new Date(val * 1000).toISOString();
  };

  const startText = formatRelativeTimestamp(toISOString(start));
  const endText = formatRelativeTimestamp(toISOString(end));

  if (startText !== "never" && endText !== "never") {
    return `${startText} → ${endText}`;
  }
  if (startText !== "never") {
    return startText;
  }
  if (endText !== "never") {
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
// Chart color hierarchy: use these colors in order for multi-line charts
const CHART_COLORS = [
  '#f97316',  // Line 1: Orange (orange-500)
  '#ef4444',  // Line 2: Red (red-500)
  '#f59e0b',  // Line 3: Amber (amber-400)
] as const;

const tabs = [
  { key: "overview", label: "Overview", icon: <Gauge className="h-4 w-4" /> },
  { key: "workload", label: "Workload", icon: <Activity className="h-4 w-4" /> },
  { key: "storage", label: "Storage & Bloat", icon: <Layers className="h-4 w-4" /> },
  { key: "recommendations", label: "Recommendations", icon: <Zap className="h-4 w-4" /> },
  { key: "history", label: "History", icon: <Activity className="h-4 w-4" /> },
  { key: "partitions", label: "Partitions", icon: <BarChart2 className="h-4 w-4" /> },
  { key: "replication", label: "Replication", icon: <Server className="h-4 w-4" /> },
  { key: "alerts", label: "Alerts", icon: <AlertTriangle className="h-4 w-4" /> },
  { key: "wraparound", label: "Wraparound", icon: <Lock className="h-4 w-4" /> },
] as const;

// ---------- Panel Components ----------
function WraparoundRiskGraph({
  xidData,
  mxidData,
}: {
  xidData: { ts: number; value: number }[];
  mxidData: { ts: number; value: number }[];
}) {
  // Combine both series into single data array for chart
  const chartData = useMemo(() => {
    const xidMap = new Map(xidData.map(p => [p.ts, p.value]));
    const mxidMap = new Map(mxidData.map(p => [p.ts, p.value]));
    const allTs = new Set([...xidData.map(p => p.ts), ...mxidData.map(p => p.ts)]);

    return Array.from(allTs)
      .sort((a, b) => a - b)
      .map(ts => ({
        ts,
        xidPct: xidMap.get(ts) ?? null,
        mxidPct: mxidMap.get(ts) ?? null,
      }));
  }, [xidData, mxidData]);

  const hasData = chartData.length > 0;

  return (
    <Card>
      <CardHeader
        title="Wraparound Risk"
        icon={<Lock className="h-4 w-4 text-slate-500" />}
      />
      <CardBody>
        {hasData ? (
          <div className="h-80 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={chartData} margin={{ left: 8, right: 16, top: 8, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis
                  dataKey="ts"
                  type="number"
                  domain={['dataMin', 'dataMax']}
                  tickFormatter={(ts) => new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  stroke="#64748b"
                />
                <YAxis domain={[0, 100]} ticks={[0, 50, 70, 85, 100]} tickFormatter={(v) => `${v}%`} stroke="#64748b" />

                {/* Reference bands */}
                <ReferenceArea y1={70} y2={85} fill="#f59e0b" fillOpacity={0.08} />
                <ReferenceArea y1={85} y2={100} fill="#ef4444" fillOpacity={0.08} />
                <ReferenceLine y={70} strokeDasharray="4 4" stroke="#f59e0b" strokeOpacity={0.5} />
                <ReferenceLine y={85} strokeDasharray="4 4" stroke="#ef4444" strokeOpacity={0.5} />

                {/* Series */}
                <Line
                  type="stepAfter"
                  dataKey="mxidPct"
                  name="MXID %"
                  stroke={CHART_COLORS[0]}
                  strokeWidth={2}
                  dot={false}
                  connectNulls
                />
                <Line
                  type="stepAfter"
                  dataKey="xidPct"
                  name="XID %"
                  stroke={CHART_COLORS[1]}
                  strokeWidth={2}
                  dot={false}
                  connectNulls
                />

                <Tooltip
                  formatter={(val: any, name: string) => [`${typeof val === 'number' ? val.toFixed(1) : '–'}%`, name]}
                  labelFormatter={(ts) => new Date(ts as number).toLocaleString()}
                  contentStyle={{ backgroundColor: 'white', border: '1px solid #e2e8f0', borderRadius: '8px' }}
                />
                <Legend verticalAlign="top" align="right" />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="h-80 flex items-center justify-center text-slate-400">
            No wraparound data available
          </div>
        )}
      </CardBody>
    </Card>
  );
}

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
    wraparound_xid_pct: { ts: number; value: number }[];
    wraparound_mxid_pct: { ts: number; value: number }[];
  }>({
    connections: [],
    tps: [],
    qps: [],
    mean_latency_ms: [],
    latency_p95_ms: [],
    latency_p99_ms: [],
    blocked_sessions: [],
    wraparound_xid_pct: [],
    wraparound_mxid_pct: [],
  });
  // Track last timestamp to enable incremental polling
  const lastTsRef = useRef<number | null>(null);

  // Initial fetch + polling of combined overview history
  useEffect(() => {
    let stopped = false;
    const shape = (pts: any[]) => pts.map(p => ({ ts: p.ts * 1000, value: p.value }));
    const fullLoad = async () => {
      try {
        const resp = await fetch(`${api.overviewHistory}?window=all&max_points=1500`);
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
            wraparound_xid_pct: shape(json.wraparound_xid_pct || []),
            wraparound_mxid_pct: shape(json.wraparound_mxid_pct || []),
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
          ...((json.wraparound_xid_pct || []).map((p:any)=>p.ts)),
          ...((json.wraparound_mxid_pct || []).map((p:any)=>p.ts)),
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
        const resp = await fetch(`${api.overviewHistory}?window=all&max_points=1500&since=${sinceSeconds}`);
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
          wraparound_xid_pct: merge(prev.wraparound_xid_pct, json.wraparound_xid_pct || []),
          wraparound_mxid_pct: merge(prev.wraparound_mxid_pct, json.wraparound_mxid_pct || []),
        }));
        const newTs = [
          ...((json.connections || []).map((p:any)=>p.ts)),
          ...((json.tps || []).map((p:any)=>p.ts)),
          ...((json.qps || []).map((p:any)=>p.ts)),
          ...((json.mean_latency_ms || []).map((p:any)=>p.ts)),
          ...((json.latency_p95_ms || []).map((p:any)=>p.ts)),
          ...((json.latency_p99_ms || []).map((p:any)=>p.ts)),
          ...((json.blocked_sessions || []).map((p:any)=>p.ts)),
          ...((json.wraparound_xid_pct || []).map((p:any)=>p.ts)),
          ...((json.wraparound_mxid_pct || []).map((p:any)=>p.ts)),
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

  // Calculate 24h average and change for each metric
  const calc24hStats = (data: { ts: number; value: number }[], currentValue?: number) => {
    if (data.length === 0 || currentValue === undefined) {
      return { average24h: undefined, change: undefined, changePercent: undefined };
    }

    // Calculate average from all available data (which is already filtered to show full history)
    // Since we're showing "all" data now, this will be the average of whatever we have
    // For metrics collected every minute, this is ~25 hours; for hourly metrics, this is ~62 days
    const sum = data.reduce((acc, p) => acc + p.value, 0);
    const average24h = sum / data.length;

    const change = currentValue - average24h;
    const changePercent = average24h !== 0 ? (change / average24h) * 100 : 0;

    return { average24h, change, changePercent };
  };

  const tpsStats = calc24hStats(series.tps, currentTps);
  const qpsStats = calc24hStats(series.qps, currentQps);
  const meanLatencyStats = calc24hStats(series.mean_latency_ms, currentMeanLatency);
  const p95LatencyStats = calc24hStats(series.latency_p95_ms, currentP95Latency);
  const p99LatencyStats = calc24hStats(series.latency_p99_ms, currentP99Latency);
  const connectionsStats = calc24hStats(series.connections, currentConnections);
  const blockedStats = calc24hStats(series.blocked_sessions, currentBlocked);

  // Metric configuration
  const metrics = [
    { key: 'connections', title: 'Connections', value: currentConnections !== undefined ? `${Math.round(currentConnections)}/${overview.max_connections}` : '–', tone: 'violet' as const, unit: undefined, status: connectionRatio >= CONNECTION_CRIT ? 'crit' as const : connectionRatio >= CONNECTION_WARN ? 'warn' as const : undefined, ...connectionsStats },
    { key: 'tps', title: 'TPS', value: currentTps !== undefined ? currentTps.toFixed(1) : '–', tone: 'green' as const, unit: undefined, status: undefined, ...tpsStats },
    { key: 'qps', title: 'QPS', value: currentQps !== undefined ? currentQps.toFixed(1) : '–', tone: 'blue' as const, unit: undefined, status: undefined, ...qpsStats },
    { key: 'mean_latency_ms', title: 'Mean Latency', value: currentMeanLatency !== undefined ? currentMeanLatency.toFixed(1) : '–', tone: 'amber' as const, unit: 'ms', status: undefined, ...meanLatencyStats },
    { key: 'latency_p95_ms', title: 'p95 Latency', value: currentP95Latency !== undefined ? currentP95Latency.toFixed(1) : '–', tone: 'rose' as const, unit: 'ms', status: undefined, ...p95LatencyStats },
    { key: 'latency_p99_ms', title: 'p99 Latency', value: currentP99Latency !== undefined ? currentP99Latency.toFixed(1) : '–', tone: 'red' as const, unit: 'ms', status: undefined, ...p99LatencyStats },
    { key: 'blocked_sessions', title: 'Blocked Sessions', value: currentBlocked !== undefined ? Math.round(currentBlocked) : '–', tone: 'slate' as const, unit: undefined, status: blockedCrit ? 'crit' as const : blockedWarn ? 'warn' as const : undefined, ...blockedStats },
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
            series={series[metric.key as keyof typeof series].slice(-10).map(p => ({ value: p.value }))}
            onClick={() => setSelectedMetric(metric.key)}
            isActive={selectedMetric === metric.key}
            change={metric.change}
            changePercent={metric.changePercent}
            average24h={metric.average24h}
          />
        ))}
      </div>

      {/* Two-graph layout: Selected metric on left, Wraparound Risk on right */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Left: Selected metric chart */}
        {selectedMetric && selectedMetricData.length > 0 && (
          <Card>
            <CardHeader
              title={`${selectedMetricConfig?.title || selectedMetric} History`}
              icon={<Activity className="h-4 w-4 text-slate-500" />}
            />
            <CardBody>
              <div className="h-80 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={selectedMetricData} margin={{ left: 8, right: 16, top: 8, bottom: 8 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                    <XAxis
                      dataKey="ts"
                      type="number"
                      domain={['dataMin', 'dataMax']}
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
                      stroke={CHART_COLORS[0]}
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

        {/* Right: Wraparound Risk chart */}
        <WraparoundRiskGraph
          xidData={series.wraparound_xid_pct}
          mxidData={series.wraparound_mxid_pct}
        />
      </div>

      <SqlSnippet sql={SQL_SNIPPETS.overview} />
    </div>
  );
}

function WorkloadTab({ queries }: { queries: TopQueryEntry[] }) {
  const [selectedQuery, setSelectedQuery] = useState<TopQueryEntry | null>(null);

  const columns: ColumnDef<TopQueryEntry>[] = [
    {
      key: 'table_names',
      label: 'Table',
      width: 'w-48',
      sortable: true,
      sortValue: (row) => row.table_names || '',
      render: (row) => (
        <span className="text-slate-600 truncate block">
          {row.table_names || <span className="text-slate-400">–</span>}
        </span>
      ),
    },
    {
      key: 'queryid',
      label: 'Query ID',
      width: 'w-28',
      render: (row) => {
        const queryIdStr = String(row.queryid);
        const truncated = queryIdStr.length > 10 ? queryIdStr.slice(0, 10) + '...' : queryIdStr;

        return (
          <span className="font-mono text-[12px]">
            {row.query_text ? (
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  setSelectedQuery(row);
                }}
                className="text-blue-600 hover:text-blue-700 hover:underline cursor-pointer bg-transparent border-0 p-0 font-mono text-[12px]"
              >
                {truncated}
              </button>
            ) : (
              <span className="text-slate-700">{truncated}</span>
            )}
          </span>
        );
      },
    },
    {
      key: 'calls',
      label: 'Calls',
      width: 'w-24',
      sortable: true,
      align: 'right',
      sortValue: (row) => row.calls,
      render: (row) => numberFormatter.format(row.calls),
    },
    {
      key: 'total_time_seconds',
      label: 'Total Time (s)',
      width: 'w-28',
      sortable: true,
      align: 'right',
      sortValue: (row) => row.total_time_seconds,
      render: (row) => row.total_time_seconds.toFixed(2),
    },
    {
      key: 'mean_time_ms',
      label: 'Mean (ms)',
      width: 'w-24',
      sortable: true,
      align: 'right',
      sortValue: (row) => row.mean_time_ms,
      render: (row) => row.mean_time_ms.toFixed(2),
    },
    {
      key: 'cache_hit_ratio',
      label: 'Query Cache Hit',
      width: 'w-32',
      sortable: true,
      align: 'right',
      sortValue: (row) => row.cache_hit_ratio,
      render: (row) => {
        const getColor = (ratio: number) => {
          if (ratio >= 0.99) return "text-green-600 font-semibold";
          if (ratio >= 0.95) return "text-amber-600 font-semibold";
          return "text-red-600 font-semibold";
        };
        return (
          <span className={getColor(row.cache_hit_ratio)}>
            {(row.cache_hit_ratio * 100).toFixed(1)}%
          </span>
        );
      },
    },
  ];

  return (
    <div className="space-y-4">
      <Section
        title="Top Queries"
        subtitle="Sorted by mean latency (click headers to sort)"
        icon={<Activity className="h-5 w-5 text-slate-500" />}
      />

      <Card>
        <CardBody>
          <DataTable
            data={queries}
            columns={columns}
            defaultSortKey="mean_time_ms"
            defaultSortDir="desc"
            maxRows={10}
            rowKey={(row) => row.queryid}
          />
        </CardBody>
      </Card>

      {/* Query Detail Modal */}
      {selectedQuery && selectedQuery.query_text && (
        <div
          className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4"
          onClick={() => setSelectedQuery(null)}
        >
          <div
            className="bg-white rounded-lg shadow-xl max-w-4xl w-full max-h-[80vh] overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-4 border-b border-slate-200">
              <h3 className="text-lg font-semibold text-slate-900">
                Query Details
                <span className="ml-2 text-sm font-mono text-slate-500">
                  ID: {selectedQuery.queryid}
                </span>
              </h3>
              <button
                onClick={() => setSelectedQuery(null)}
                className="text-slate-400 hover:text-slate-600 transition-colors"
              >
                <svg className="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <div className="p-4 overflow-y-auto max-h-[calc(80vh-8rem)]">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4 text-sm">
                <div>
                  <div className="text-slate-500">Calls</div>
                  <div className="font-semibold">{numberFormatter.format(selectedQuery.calls)}</div>
                </div>
                <div>
                  <div className="text-slate-500">Total Time</div>
                  <div className="font-semibold">{selectedQuery.total_time_seconds.toFixed(2)}s</div>
                </div>
                <div>
                  <div className="text-slate-500">Mean Time</div>
                  <div className="font-semibold">{selectedQuery.mean_time_ms.toFixed(2)}ms</div>
                </div>
                <div>
                  <div className="text-slate-500">Cache Hit</div>
                  <div className="font-semibold">{(selectedQuery.cache_hit_ratio * 100).toFixed(1)}%</div>
                </div>
              </div>
              <div className="bg-slate-50 p-4 rounded border border-slate-200">
                <div className="text-xs font-semibold text-slate-500 uppercase mb-2">SQL Query</div>
                <pre className="text-sm font-mono text-slate-800 whitespace-pre-wrap break-words">
                  {selectedQuery.query_text}
                </pre>
              </div>
            </div>
          </div>
        </div>
      )}

      <SqlSnippet sql={SQL_SNIPPETS.topQueries} />
    </div>
  );
}

// Combined Storage & Bloat Tab (consolidates Storage, Bloat, Autovac, Stale Stats, and Unused Indexes)
function StorageAndBloatTab({
  storage,
  bloatSamples,
  unusedIndexes,
  staleStats,
}: {
  storage: StorageEntry[];
  bloatSamples: BloatSample[];
  unusedIndexes: UnusedIndexEntry[];
  staleStats: StaleStatEntry[];
}) {
  const [showUnusedIndexes, setShowUnusedIndexes] = useState(false);

  // Merge data from different sources by relation name
  const combinedData = useMemo(() => {
    const bloatMap = new Map(bloatSamples.map(b => [b.relation, b]));

    return storage.map(storageEntry => {
      const bloat = bloatMap.get(storageEntry.relation);

      return {
        ...storageEntry,
        bloat_free_percent: bloat?.free_percent,
      };
    });
  }, [storage, bloatSamples]);

  const getCacheHitColor = (ratio?: number | null) => {
    if (ratio === null || ratio === undefined) return "text-slate-400";
    if (ratio >= 0.99) return "text-green-600 font-semibold";
    if (ratio >= 0.95) return "text-amber-600 font-semibold";
    return "text-red-600 font-semibold";
  };

  const hasAdvancedBloatFields = useMemo(
    () => bloatSamples.some((s) => s.dead_tuple_count != null),
    [bloatSamples]
  );

  type CombinedStorageEntry = StorageEntry & {
    bloat_free_percent?: number;
  };

  const columns: ColumnDef<CombinedStorageEntry>[] = [
    {
      key: 'relation',
      label: 'Relation',
      width: 'w-64',
      sortable: true,
      sortValue: (row) => row.relation,
      render: (row) => (
        <span className="font-mono text-[12px] text-slate-700 truncate block">
          {row.relation}
        </span>
      ),
    },
    {
      key: 'total_bytes',
      label: 'Total Size',
      width: 'w-28',
      sortable: true,
      align: 'right',
      sortValue: (row) => row.total_bytes,
      render: (row) => formatBytes(row.total_bytes),
    },
    {
      key: 'dead_tuple_ratio',
      label: 'Dead Tuples',
      width: 'w-28',
      sortable: true,
      align: 'right',
      sortValue: (row) => extractNumericValue(row.dead_tuple_ratio) ?? 0,
      render: (row) => {
        const ratio = extractNumericValue(row.dead_tuple_ratio);
        return ratio !== null ? `${(ratio * 100).toFixed(1)}%` : "—";
      },
    },
    {
      key: 'bloat_free_percent',
      label: 'Bloat',
      width: 'w-24',
      sortable: true,
      align: 'right',
      sortValue: (row) => row.bloat_free_percent ?? 0,
      render: (row) =>
        row.bloat_free_percent !== undefined && row.bloat_free_percent !== null
          ? `${row.bloat_free_percent.toFixed(1)}%`
          : "—",
    },
    {
      key: 'cache_hit_ratio',
      label: 'Table Cache Hit',
      width: 'w-32',
      sortable: true,
      align: 'right',
      sortValue: (row) => row.cache_hit_ratio ?? 0,
      render: (row) => (
        <span className={getCacheHitColor(row.cache_hit_ratio)}>
          {row.cache_hit_ratio !== null && row.cache_hit_ratio !== undefined
            ? `${(row.cache_hit_ratio * 100).toFixed(1)}%`
            : "—"}
        </span>
      ),
    },
    {
      key: 'last_autovacuum',
      label: 'Last Vacuum',
      width: 'w-32',
      sortable: true,
      sortValue: (row) => row.last_autovacuum ? new Date(row.last_autovacuum).getTime() : 0,
      render: (row) => formatRelativeTimestamp(row.last_autovacuum),
    },
    {
      key: 'last_analyzed',
      label: 'Last Analyzed',
      width: 'w-32',
      sortable: true,
      align: 'right',
      sortValue: (row) => {
        const analyze = row.last_analyze ? new Date(row.last_analyze).getTime() : 0;
        const autoanalyze = row.last_autoanalyze ? new Date(row.last_autoanalyze).getTime() : 0;
        return Math.max(analyze, autoanalyze);
      },
      render: (row) => {
        const analyze = row.last_analyze;
        const autoanalyze = row.last_autoanalyze;
        
        let latest: string | null = null;
        if (analyze && autoanalyze) {
          latest = new Date(analyze) > new Date(autoanalyze) ? analyze : autoanalyze;
        } else if (analyze) {
          latest = analyze;
        } else if (autoanalyze) {
          latest = autoanalyze;
        }
        
        return formatRelativeTimestamp(latest);
      },
    },
  ];

  return (
    <div className="space-y-4">
      <Section
        title="Storage & Bloat"
        subtitle={hasAdvancedBloatFields ? "Combined table health (Exact bloat mode)" : "Combined table health (Approx bloat mode)"}
        icon={<Layers className="h-5 w-5 text-slate-500" />}
      />

      <Card>
        <CardBody>
          <DataTable
            data={combinedData}
            columns={columns}
            defaultSortKey="total_bytes"
            defaultSortDir="desc"
            maxRows={15}
            rowKey={(row) => row.relation}
          />
        </CardBody>
      </Card>

      {/* Unused Indexes Section */}
      {unusedIndexes.length > 0 && (
        <Card>
          <CardBody>
            <button
              onClick={() => setShowUnusedIndexes(!showUnusedIndexes)}
              className="w-full flex items-center justify-between py-2 text-left hover:bg-slate-50/60 rounded transition-colors"
            >
              <div className="flex items-center gap-2">
                <Layers className="h-4 w-4 text-slate-500" />
                <span className="font-semibold text-slate-700">Unused Indexes ({unusedIndexes.length})</span>
              </div>
              <span className="text-slate-400">{showUnusedIndexes ? "▼" : "►"}</span>
            </button>

            {showUnusedIndexes && (
              <div className="mt-4 overflow-x-auto">
                <table className="min-w-full text-sm">
                  <thead>
                    <tr className="text-left text-slate-500 border-b border-slate-100">
                      <th className="py-2 pr-4">Relation</th>
                      <th className="py-2 pr-4">Index</th>
                      <th className="py-2 pr-4">Size</th>
                    </tr>
                  </thead>
                  <tbody>
                    {unusedIndexes.map((ix) => (
                      <tr key={ix.relation + ix.index} className="border-b border-slate-50 hover:bg-slate-50/60">
                        <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{ix.relation}</td>
                        <td className="py-2 pr-4 font-mono text-[12px] text-slate-700">{ix.index}</td>
                        <td className="py-2 pr-4">{formatBytes(ix.bytes)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardBody>
        </Card>
      )}

      <SqlSnippet sql={SQL_SNIPPETS.storage} />
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
                    <td className="py-2 pr-4">{formatRelativeTimestamp(slice.latest_partition_upper ? new Date(slice.latest_partition_upper * 1000).toISOString() : null)}</td>
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

// Combined Recommendations Tab (includes both Recommendations and Forecasts)
function RecommendationsTab({
  recommendations,
  forecasts
}: {
  recommendations: Recommendation[];
  forecasts: Forecast[];
}) {
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);
  const [dismissed, setDismissed] = useState<Set<string>>(new Set());

  // Clear dismissed recommendations when new data arrives from backend
  // This ensures dismissed items reappear if they're still valid after the next hourly refresh
  useEffect(() => {
    setDismissed(new Set());
  }, [recommendations]);

  const getRecommendationKey = (rec: Recommendation) => `${rec.relation}-${rec.kind}`;

  const handleDismiss = (rec: Recommendation) => {
    const key = getRecommendationKey(rec);
    setDismissed(prev => new Set(prev).add(key));
  };

  const handleCopy = (sql: string, index: number) => {
    navigator.clipboard.writeText(sql);
    setCopiedIndex(index);
    setTimeout(() => setCopiedIndex(null), 2000);
  };

  const getSeverityColor = (severity: string) => {
    if (severity === "crit" || severity === "urgent") return "red";
    if (severity === "warn") return "yellow";
    return "blue";
  };

  const getRecKindLabel = (kind: string) => {
    if (kind === "vacuum_analyze") return "VACUUM ANALYZE";
    if (kind === "vacuum_full") return "VACUUM FULL";
    if (kind === "analyze") return "ANALYZE";
    if (kind === "reindex") return "REINDEX";
    if (kind === "autovacuum_tuning") return "AUTOVACUUM TUNING";
    return kind;
  };

  const getForecastKindLabel = (kind: string) => {
    if (kind === "wraparound_database") return "WRAPAROUND (DB)";
    if (kind === "wraparound_relation") return "WRAPAROUND (TABLE)";
    if (kind === "table_growth") return "TABLE GROWTH";
    if (kind === "connection_saturation") return "CONNECTIONS";
    return kind;
  };

  const formatDate = (epoch: number | null | undefined) => {
    if (!epoch) return "N/A";
    return new Date(epoch * 1000).toLocaleDateString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  };

  // Filter out dismissed recommendations
  const visibleRecommendations = recommendations.filter(
    rec => !dismissed.has(getRecommendationKey(rec))
  );

  return (
    <div className="space-y-6">
      {/* Immediate Actions Section */}
      <div className="space-y-4">
        <Section
          title="Immediate Actions"
          subtitle={visibleRecommendations.length === 0 ? "All healthy!" : `${visibleRecommendations.length} maintenance suggestions`}
          icon={<Zap className="h-5 w-5 text-amber-500" />}
        />

        {visibleRecommendations.length === 0 ? (
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
            {visibleRecommendations.map((rec, index) => (
              <Card key={`${rec.relation}-${rec.kind}-${index}`}>
                <CardBody>
                  <div className="space-y-3">
                    {/* Header */}
                    <div className="flex items-start justify-between gap-4">
                      <div className="flex items-center gap-2">
                        <Badge tone={getSeverityColor(rec.severity)}>{rec.severity}</Badge>
                        <Badge tone="slate">{getRecKindLabel(rec.kind)}</Badge>
                        <span className="font-mono text-sm text-slate-700">{rec.relation}</span>
                      </div>
                      <button
                        onClick={() => handleDismiss(rec)}
                        className="flex-shrink-0 p-1 hover:bg-slate-100 rounded transition-colors group"
                        title="Dismiss (will reappear on next refresh if still valid)"
                      >
                        <X className="h-4 w-4 text-slate-400 group-hover:text-slate-600" />
                      </button>
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

      {/* Forecasting Section */}
      <div className="space-y-4">
        <Section
          title="Capacity Forecasts"
          subtitle={forecasts.length === 0 ? "All trending healthy!" : `${forecasts.length} capacity warnings`}
          icon={<TrendingUp className="h-5 w-5 text-blue-500" />}
        />

        {forecasts.length === 0 ? (
          <Card>
            <CardBody>
              <div className="text-center py-8">
                <div className="text-4xl mb-2">✨</div>
                <div className="text-lg font-semibold text-slate-700">No capacity issues forecast</div>
                <div className="text-sm text-slate-500 mt-1">All resources are trending healthy!</div>
              </div>
            </CardBody>
          </Card>
        ) : (
          <div className="space-y-3">
            {forecasts.map((forecast, index) => (
              <Card key={`${forecast.resource}-${forecast.kind}-${index}`}>
                <CardBody>
                  <div className="space-y-3">
                    {/* Header */}
                    <div className="flex items-start justify-between gap-4">
                      <div className="flex items-center gap-2">
                        <Badge tone={getSeverityColor(forecast.severity)}>{forecast.severity.toUpperCase()}</Badge>
                        <Badge tone="slate">{getForecastKindLabel(forecast.kind)}</Badge>
                        <span className="font-mono text-sm text-slate-700">{forecast.resource}</span>
                      </div>
                    </div>

                    {/* Message */}
                    <div className="text-sm text-slate-600 leading-relaxed">
                      {forecast.message}
                    </div>

                    {/* Forecast details */}
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-xs">
                      <div>
                        <div className="text-slate-500">Current</div>
                        <div className="font-semibold text-slate-900">{forecast.current_value.toFixed(0)}</div>
                      </div>
                      <div>
                        <div className="text-slate-500">Threshold</div>
                        <div className="font-semibold text-slate-900">{forecast.threshold.toFixed(0)}</div>
                      </div>
                      {forecast.growth_rate_per_day > 0 && (
                        <div>
                          <div className="text-slate-500">Growth/Day</div>
                          <div className="font-semibold text-amber-600">+{forecast.growth_rate_per_day.toFixed(1)}</div>
                        </div>
                      )}
                      {forecast.days_until_threshold !== null && forecast.days_until_threshold !== undefined && (
                        <div>
                          <div className="text-slate-500">Days Until</div>
                          <div className="font-semibold text-red-600">{forecast.days_until_threshold.toFixed(0)} days</div>
                        </div>
                      )}
                    </div>

                    {/* Predicted date */}
                    {forecast.predicted_date && (
                      <div className="bg-amber-50 border border-amber-200 rounded p-3 text-sm">
                        <span className="font-semibold text-amber-900">Predicted breach: </span>
                        <span className="text-amber-700">{formatDate(forecast.predicted_date)}</span>
                      </div>
                    )}
                  </div>
                </CardBody>
              </Card>
            ))}
          </div>
        )}
      </div>
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
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="ts" type="number" domain={['dataMin', 'dataMax']} tickFormatter={(ts) => new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} stroke="#64748b" />
                <YAxis stroke="#64748b" />
                <Tooltip labelFormatter={(ts) => new Date(ts as number).toLocaleTimeString()} />
                <Legend />
                <Line type="monotone" dataKey="tps" stroke={CHART_COLORS[0]} strokeWidth={2} dot={false} name="TPS" />
                <Line type="monotone" dataKey="qps" stroke={CHART_COLORS[1]} strokeWidth={2} dot={false} name="QPS" />
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
  const { data: wraparound } = usePollingData<WraparoundSnapshot>(api.wraparound, { databases: [], relations: [], xid_limit: 0, mxid_limit: 0, xid_pct: 0, mxid_pct: 0 }, 300_000);
  const { data: recommendationsData } = usePollingData<RecommendationsResponse>(api.recommendations, { recommendations: [] }, 60_000);
  const { data: forecastsData } = usePollingData<ForecastsResponse>(api.forecasts, { forecasts: [] }, 60_000);

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      {/* Top Nav */}
      <div className="border-b border-slate-200 bg-white sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-2 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <img src="/android-chrome-192x192.png" alt="PGMon" className="h-18 w-18 rounded-xl" />
            <div>
              <div className="text-base text-slate-500">Monitoring cluster</div>
              <div className="font-semibold text-slate-900 text-lg">{overview?.cluster ?? "…"}</div>
            </div>
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
                      'w-full group flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors border-0',
                      active === t.key ? 'bg-[#fff1f2] text-slate-900' : 'bg-transparent hover:bg-slate-50/50 text-slate-600'
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
          {active === 'storage' && <StorageAndBloatTab storage={storage} bloatSamples={bloatSamples} unusedIndexes={unusedIndexes} staleStats={staleStats} />}
          {active === 'recommendations' && <RecommendationsTab recommendations={recommendationsData.recommendations} forecasts={forecastsData.forecasts} />}
          {active === 'history' && <HistoryCharts />}
          {active === 'replication' && <ReplicationTab replicas={replication} />}
          {active === 'partitions' && <PartitionsTab slices={partitions} />}
          {active === 'alerts' && <AlertsTab overview={overview} />}
          {active === 'wraparound' && <WraparoundTab snapshot={wraparound} />}
        </main>
      </div>
    </div>
  );
}
