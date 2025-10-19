export interface OverviewSnapshot {
  cluster: string;
  generated_at?: number;
  connections: number;
  max_connections: number;
  blocked_sessions: number;
  longest_transaction_seconds?: number;
  longest_blocked_seconds?: number;
  tps?: number;
  qps?: number;
  mean_latency_ms?: number;
  wal_bytes_per_second?: number;
  checkpoints_timed?: number;
  checkpoints_requested?: number;
  open_alerts: string[];
  open_crit_alerts: string[];
}

export interface AutovacuumEntry {
  relation: string;
  n_live_tup: number;
  n_dead_tup: number;
  last_vacuum?: number;
  last_autovacuum?: number;
  last_analyze?: number;
  last_autoanalyze?: number;
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
}

export interface ReplicaLag {
  replica: string;
  lag_seconds?: number;
  lag_bytes?: number;
}

export interface PartitionSlice {
  parent: string;
  child_count: number;
  oldest_partition?: number;
  newest_partition?: number;
  latest_partition_name?: string;
  next_expected_partition?: number;
  missing_future_partition: boolean;
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
  replication: "/api/v1/replication",
  storage: "/api/v1/storage",
  partitions: "/api/v1/partitions",
  wraparound: "/api/v1/wraparound",
};
