use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::RwLock;

/// Aggregated snapshots that back the REST API.
#[derive(Debug, Clone, Serialize)]
pub struct AppSnapshots {
    pub overview: OverviewSnapshot,
    pub autovacuum: Vec<AutovacuumEntry>,
    pub top_queries: Vec<TopQueryEntry>,
    pub storage: Vec<StorageEntry>,
    pub stale_stats: Vec<StaleStatEntry>,
    pub unused_indexes: Vec<UnusedIndexEntry>,
    pub partitions: Vec<PartitionSlice>,
    pub replication: Vec<ReplicaLag>,
    pub wraparound: WraparoundSnapshot,
}

impl Default for AppSnapshots {
    fn default() -> Self {
        Self {
            overview: OverviewSnapshot::default(),
            autovacuum: Vec::new(),
            top_queries: Vec::new(),
            storage: Vec::new(),
            stale_stats: Vec::new(),
            unused_indexes: Vec::new(),
            partitions: Vec::new(),
            replication: Vec::new(),
            wraparound: WraparoundSnapshot::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OverviewSnapshot {
    pub cluster: String,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub generated_at: Option<DateTime<Utc>>,
    pub connections: i64,
    pub max_connections: i64,
    pub blocked_sessions: i64,
    pub blocking_events: Vec<BlockingEvent>,
    pub longest_transaction_seconds: Option<f64>,
    pub longest_blocked_seconds: Option<f64>,
    pub tps: Option<f64>,
    pub qps: Option<f64>,
    pub mean_latency_ms: Option<f64>,
    pub latency_p95_ms: Option<f64>,
    pub wal_bytes_per_second: Option<f64>,
    pub checkpoints_timed: Option<i64>,
    pub checkpoints_requested: Option<i64>,
    pub checkpoint_requested_ratio: Option<f64>,
    pub checkpoint_mean_interval_seconds: Option<f64>,
    pub temp_bytes_per_second: Option<f64>,
    pub open_alerts: Vec<String>,
    pub open_crit_alerts: Vec<String>,
}

impl Default for OverviewSnapshot {
    fn default() -> Self {
        Self {
            cluster: "unknown".into(),
            generated_at: None,
            connections: 0,
            max_connections: 0,
            blocked_sessions: 0,
            blocking_events: Vec::new(),
            longest_transaction_seconds: None,
            longest_blocked_seconds: None,
            tps: None,
            qps: None,
            mean_latency_ms: None,
            latency_p95_ms: None,
            wal_bytes_per_second: None,
            checkpoints_timed: None,
            checkpoints_requested: None,
            checkpoint_requested_ratio: None,
            checkpoint_mean_interval_seconds: None,
            temp_bytes_per_second: None,
            open_alerts: Vec::new(),
            open_crit_alerts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockingEvent {
    pub blocked_pid: i32,
    pub blocked_usename: Option<String>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub blocked_transaction_start: Option<DateTime<Utc>>,
    pub blocked_wait_seconds: Option<f64>,
    pub blocked_query: Option<String>,
    pub blocker_pid: i32,
    pub blocker_usename: Option<String>,
    pub blocker_state: Option<String>,
    pub blocker_waiting: bool,
    pub blocker_query: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutovacuumEntry {
    pub relation: String,
    pub n_live_tup: i64,
    pub n_dead_tup: i64,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub last_vacuum: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub last_autovacuum: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub last_analyze: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub last_autoanalyze: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopQueryEntry {
    pub queryid: i64,
    pub calls: i64,
    pub total_time_seconds: f64,
    pub mean_time_ms: f64,
    pub shared_blks_read: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageEntry {
    pub relation: String,
    pub relkind: String,
    pub total_bytes: i64,
    pub table_bytes: i64,
    pub index_bytes: i64,
    pub toast_bytes: i64,
    pub dead_tuple_ratio: Option<f64>,
    pub last_autovacuum: Option<DateTime<Utc>>,
    pub reltuples: Option<f64>,
    pub dead_tuples: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnusedIndexEntry {
    pub relation: String,
    pub index: String,
    pub bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StaleStatEntry {
    pub relation: String,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub last_analyze: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub last_autoanalyze: Option<DateTime<Utc>>,
    pub hours_since_analyze: Option<f64>,
    pub n_live_tup: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionSlice {
    pub parent: String,
    pub child_count: usize,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub oldest_partition: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub newest_partition: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub latest_partition_upper: Option<DateTime<Utc>>,
    pub latest_partition_name: Option<String>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub next_expected_partition: Option<DateTime<Utc>>,
    pub missing_future_partition: bool,
    pub future_gap_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicaLag {
    pub replica: String,
    pub lag_seconds: Option<f64>,
    pub lag_bytes: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct WraparoundSnapshot {
    pub databases: Vec<WraparoundDatabase>,
    pub relations: Vec<WraparoundRelation>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WraparoundDatabase {
    pub database: String,
    pub tx_age: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WraparoundRelation {
    pub relation: String,
    pub tx_age: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoopHealth {
    pub name: String,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub last_success_at: Option<DateTime<Utc>>,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
}

impl LoopHealth {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            last_success_at: None,
            consecutive_failures: 0,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkloadSample {
    pub collected_at: DateTime<Utc>,
    pub total_xacts: f64,
    pub total_calls: Option<f64>,
    pub total_time_ms: Option<f64>,
    pub wal_bytes: Option<i64>,
    pub temp_bytes: Option<i64>,
    pub checkpoints_timed: Option<i64>,
    pub checkpoints_requested: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct WorkloadSummary {
    pub tps: Option<f64>,
    pub qps: Option<f64>,
    pub mean_latency_ms: Option<f64>,
    pub latency_p95_ms: Option<f64>,
    pub wal_bytes_total: Option<i64>,
    pub wal_bytes_per_second: Option<f64>,
    pub temp_bytes_total: Option<i64>,
    pub temp_bytes_per_second: Option<f64>,
    pub checkpoints_timed_total: Option<i64>,
    pub checkpoints_requested_total: Option<i64>,
    pub checkpoint_requested_ratio: Option<f64>,
    pub checkpoint_mean_interval_seconds: Option<f64>,
}

impl WorkloadSummary {
    fn from_samples(previous: &WorkloadSample, current: &WorkloadSample) -> Option<Self> {
        let elapsed = current
            .collected_at
            .signed_duration_since(previous.collected_at)
            .num_milliseconds() as f64
            / 1000.0;

        if elapsed <= 0.0 {
            return None;
        }

        let tps = compute_rate(current.total_xacts - previous.total_xacts, elapsed);

        let qps = match (current.total_calls, previous.total_calls) {
            (Some(curr), Some(prev)) => compute_rate(curr - prev, elapsed),
            _ => None,
        };

        let mean_latency_ms = match (
            current.total_calls,
            previous.total_calls,
            current.total_time_ms,
            previous.total_time_ms,
        ) {
            (Some(curr_calls), Some(prev_calls), Some(curr_time), Some(prev_time)) => {
                let call_delta = curr_calls - prev_calls;
                let time_delta = curr_time - prev_time;
                if call_delta > 0.0 && time_delta >= 0.0 {
                    Some(time_delta / call_delta)
                } else {
                    None
                }
            }
            _ => None,
        };

        let wal_bytes_per_second = match (current.wal_bytes, previous.wal_bytes) {
            (Some(curr), Some(prev)) => compute_rate((curr - prev) as f64, elapsed),
            _ => None,
        };

        let checkpoints_timed_delta = match (current.checkpoints_timed, previous.checkpoints_timed)
        {
            (Some(curr), Some(prev)) => Some((curr - prev).max(0)),
            _ => None,
        };

        let checkpoints_requested_delta = match (
            current.checkpoints_requested,
            previous.checkpoints_requested,
        ) {
            (Some(curr), Some(prev)) => Some((curr - prev).max(0)),
            _ => None,
        };

        let total_delta = match (checkpoints_timed_delta, checkpoints_requested_delta) {
            (Some(t), Some(r)) => Some(t + r),
            (Some(t), None) => Some(t),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };

        let checkpoint_requested_ratio = match (checkpoints_requested_delta, total_delta) {
            (Some(req), Some(total)) if total > 0 => Some(req as f64 / total as f64),
            _ => None,
        };

        let checkpoint_mean_interval_seconds = match total_delta {
            Some(total) if total > 0 => Some(elapsed / total as f64),
            _ => None,
        };

        let temp_bytes_per_second = match (current.temp_bytes, previous.temp_bytes) {
            (Some(curr), Some(prev)) => compute_rate((curr - prev) as f64, elapsed),
            _ => None,
        };

        Some(Self {
            tps,
            qps,
            mean_latency_ms,
            latency_p95_ms: None,
            wal_bytes_total: current.wal_bytes,
            wal_bytes_per_second,
            temp_bytes_total: current.temp_bytes,
            temp_bytes_per_second,
            checkpoints_timed_total: current.checkpoints_timed,
            checkpoints_requested_total: current.checkpoints_requested,
            checkpoint_requested_ratio,
            checkpoint_mean_interval_seconds,
        })
    }
}

fn compute_rate(delta: f64, elapsed_seconds: f64) -> Option<f64> {
    if delta > 0.0 && elapsed_seconds > 0.0 {
        Some(delta / elapsed_seconds)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};

    #[test]
    fn workload_summary_computes_rates() {
        let previous = WorkloadSample {
            collected_at: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            total_xacts: 1_000.0,
            total_calls: Some(2_000.0),
            total_time_ms: Some(40_000.0),
            wal_bytes: Some(1_000),
            temp_bytes: Some(10_000),
            checkpoints_timed: Some(10),
            checkpoints_requested: Some(2),
        };

        let current = WorkloadSample {
            collected_at: previous.collected_at + Duration::seconds(60),
            total_xacts: 1_360.0,
            total_calls: Some(2_300.0),
            total_time_ms: Some(43_000.0),
            wal_bytes: Some(1_600),
            temp_bytes: Some(16_000),
            checkpoints_timed: Some(11),
            checkpoints_requested: Some(3),
        };

        let summary = WorkloadSummary::from_samples(&previous, &current).expect("summary");

        assert!((summary.tps.expect("tps") - 6.0).abs() < 1e-6);
        assert!((summary.qps.expect("qps") - 5.0).abs() < 1e-6);
        assert!((summary.mean_latency_ms.expect("lat") - 10.0).abs() < 1e-6);
        assert_eq!(summary.wal_bytes_total, Some(1_600));
        assert!((summary.wal_bytes_per_second.expect("wal rate") - 10.0).abs() < 1e-6);
        assert_eq!(summary.temp_bytes_total, Some(16_000));
        assert!((summary.temp_bytes_per_second.expect("temp rate") - 100.0).abs() < 1e-6);
        assert_eq!(summary.checkpoints_timed_total, Some(11));
        assert_eq!(summary.checkpoints_requested_total, Some(3));
        assert_eq!(
            summary.checkpoint_requested_ratio,
            Some(0.5),
            "requested ratio mismatch"
        );
        assert!(
            summary
                .checkpoint_mean_interval_seconds
                .map(|v| (v - 30.0).abs() < 1e-6)
                .unwrap_or(false),
            "mean interval mismatch"
        );
    }

    #[test]
    fn workload_summary_handles_zero_elapsed() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let sample = WorkloadSample {
            collected_at: timestamp,
            total_xacts: 100.0,
            total_calls: Some(200.0),
            total_time_ms: Some(1_000.0),
            wal_bytes: Some(1_000),
            temp_bytes: Some(2_000),
            checkpoints_timed: Some(5),
            checkpoints_requested: Some(1),
        };

        assert!(WorkloadSummary::from_samples(&sample, &sample).is_none());
    }
}

#[derive(Default)]
struct SharedStateInner {
    snapshots: RwLock<AppSnapshots>,
    loop_health: RwLock<HashMap<String, LoopHealth>>,
    workload_sample: RwLock<Option<WorkloadSample>>,
}

/// Shared state container for the HTTP layer and poller loops.
#[derive(Clone, Default)]
pub struct SharedState {
    inner: Arc<SharedStateInner>,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SharedStateInner::default()),
        }
    }

    pub async fn get_snapshots(&self) -> AppSnapshots {
        self.inner.snapshots.read().await.clone()
    }

    pub async fn update_overview(&self, overview: OverviewSnapshot) {
        let mut guard = self.inner.snapshots.write().await;
        guard.overview = overview;
    }

    pub async fn update_overview_with<F>(&self, apply: F)
    where
        F: FnOnce(&mut OverviewSnapshot),
    {
        let mut guard = self.inner.snapshots.write().await;
        apply(&mut guard.overview);
    }

    pub async fn update_autovacuum(&self, rows: Vec<AutovacuumEntry>) {
        let mut guard = self.inner.snapshots.write().await;
        guard.autovacuum = rows;
    }

    pub async fn update_top_queries(&self, rows: Vec<TopQueryEntry>) {
        let mut guard = self.inner.snapshots.write().await;
        guard.top_queries = rows;
    }

    pub async fn update_storage(&self, rows: Vec<StorageEntry>) {
        let mut guard = self.inner.snapshots.write().await;
        guard.storage = rows;
    }

    pub async fn update_unused_indexes(&self, rows: Vec<UnusedIndexEntry>) {
        let mut guard = self.inner.snapshots.write().await;
        guard.unused_indexes = rows;
    }

    pub async fn update_stale_stats(&self, rows: Vec<StaleStatEntry>) {
        let mut guard = self.inner.snapshots.write().await;
        guard.stale_stats = rows;
    }

    pub async fn update_partitions(&self, rows: Vec<PartitionSlice>) {
        let mut guard = self.inner.snapshots.write().await;
        guard.partitions = rows;
    }

    pub async fn update_replication(&self, rows: Vec<ReplicaLag>) {
        let mut guard = self.inner.snapshots.write().await;
        guard.replication = rows;
    }

    pub async fn update_wraparound(&self, snapshot: WraparoundSnapshot) {
        let mut guard = self.inner.snapshots.write().await;
        guard.wraparound = snapshot;
    }

    pub async fn record_workload_sample(&self, sample: WorkloadSample) -> Option<WorkloadSummary> {
        let mut guard = self.inner.workload_sample.write().await;
        let summary = guard
            .as_ref()
            .and_then(|previous| WorkloadSummary::from_samples(previous, &sample));
        *guard = Some(sample);
        summary
    }

    pub async fn record_loop_success(&self, loop_name: &str) {
        let mut guard = self.inner.loop_health.write().await;
        let entry = guard
            .entry(loop_name.to_string())
            .or_insert_with(|| LoopHealth::new(loop_name));
        entry.last_success_at = Some(Utc::now());
        entry.consecutive_failures = 0;
        entry.last_error = None;
    }

    pub async fn record_loop_failure(&self, loop_name: &str, error: String) {
        let mut guard = self.inner.loop_health.write().await;
        let entry = guard
            .entry(loop_name.to_string())
            .or_insert_with(|| LoopHealth::new(loop_name));
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        entry.last_error = Some(error);
    }

    pub async fn loop_health(&self) -> Vec<LoopHealth> {
        self.inner
            .loop_health
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn is_ready(&self, loop_names: &[&str], max_staleness: Duration) -> bool {
        let health = self.inner.loop_health.read().await;
        let now = Utc::now();
        let staleness = chrono::Duration::from_std(max_staleness)
            .unwrap_or_else(|_| chrono::Duration::seconds(300));

        loop_names.iter().all(|name| {
            if let Some(entry) = health.get(*name) {
                if entry.consecutive_failures > 0 {
                    return false;
                }
                if let Some(last) = entry.last_success_at {
                    return now.signed_duration_since(last) <= staleness;
                }
                false
            } else {
                false
            }
        })
    }
}
