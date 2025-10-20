export interface OverviewSnapshot {
  cluster: string;
  generated_at?: number;
  connections: number;
  max_connections: number;
  blocked_sessions: number;
  blocking_events: BlockingEvent[];
  longest_transaction_seconds?: number;
  longest_blocked_seconds?: number;
  tps?: number;
  qps?: number;
  mean_latency_ms?: number;
  latency_p95_ms?: number;
  wal_bytes_per_second?: number;
  checkpoints_timed?: number;
  checkpoints_requested?: number;
  checkpoint_requested_ratio?: number;
  checkpoint_mean_interval_seconds?: number;
  open_alerts: string[];
  open_crit_alerts: string[];
}

export interface AutovacuumEntry {
  relation: string;
  n_live_tup: number;
  n_dead_tup: number;
  last_vacuum?: number | null;
  last_autovacuum?: number | null;
  last_analyze?: number | null;
  last_autoanalyze?: number | null;
}

export interface StorageEntry {
  relation: string;
  relkind: string;
  total_bytes: number;
  table_bytes: number;
  index_bytes: number;
  toast_bytes: number;
  dead_tuple_ratio?: number;
  last_autovacuum?: number;
  reltuples?: number;
  dead_tuples?: number;
  estimated_bloat_bytes?: number | null;
}

export interface UnusedIndexEntry {
  relation: string;
  index: string;
  bytes: number;
}

export interface BloatSample {
  relation: string;
  table_bytes: number;
  free_bytes: number;
  free_percent: number;
}

export interface TopQueryEntry {
  queryid: number;
  calls: number;
  total_time_seconds: number;
  mean_time_ms: number;
  shared_blks_read: number;
}

export interface StaleStatEntry {
  relation: string;
  last_analyze?: number | null;
  last_autoanalyze?: number | null;
  hours_since_analyze?: number | null;
  n_live_tup: number;
}

export interface ReplicaLag {
  replica: string;
  lag_seconds?: number;
  lag_bytes?: number;
}

export interface PartitionSlice {
  parent: string;
  child_count: number;
  oldest_partition?: number | null;
  newest_partition?: number | null;
  latest_partition_upper?: number | null;
  latest_partition_name?: string;
  next_expected_partition?: number | null;
  missing_future_partition: boolean;
  future_gap_seconds?: number | null;
}

export interface WraparoundSnapshot {
  databases: WraparoundDatabase[];
  relations: WraparoundRelation[];
}

export interface WraparoundDatabase {
  database: string;
  tx_age: number;
}

export interface WraparoundRelation {
  relation: string;
  tx_age: number;
}

const DEFAULT_REFRESH_MS = 30_000;

async function fetchJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, { signal });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }
  return (await response.json()) as T;
}

export function createPoller<T>(
  path: string,
  setter: (value: T) => void,
  onError: (err: Error) => void,
  intervalMs: number = DEFAULT_REFRESH_MS,
) {
  let timer: number | undefined;
  let abortController: AbortController | undefined;
  let stopped = false;

  const tick = async () => {
    abortController?.abort();
    abortController = new AbortController();
    try {
      const data = await fetchJson<T>(path, abortController.signal);
      if (!stopped) {
        setter(data);
      }
    } catch (err) {
      if (!stopped && err instanceof Error && err.name !== "AbortError") {
        onError(err);
      }
    } finally {
      if (!stopped) {
        timer = window.setTimeout(tick, intervalMs);
      }
    }
  };

  tick();

  return () => {
    stopped = true;
    if (timer !== undefined) {
      window.clearTimeout(timer);
    }
    abortController?.abort();
  };
}

export const api = {
  overview: "/api/v1/overview",
  autovacuum: "/api/v1/autovacuum",
  topQueries: "/api/v1/top-queries",
  unusedIndexes: "/api/v1/unused-indexes",
  bloat: "/api/v1/bloat",
  staleStats: "/api/v1/stale-stats",
  replication: "/api/v1/replication",
  storage: "/api/v1/storage",
  partitions: "/api/v1/partitions",
  wraparound: "/api/v1/wraparound",
};
export interface BlockingEvent {
  blocked_pid: number;
  blocked_usename?: string | null;
  blocked_transaction_start?: number | null;
  blocked_wait_seconds?: number | null;
  blocked_query?: string | null;
  blocker_pid: number;
  blocker_usename?: string | null;
  blocker_state?: string | null;
  blocker_waiting: boolean;
  blocker_query?: string | null;
}
