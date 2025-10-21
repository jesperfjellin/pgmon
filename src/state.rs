use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
    pub bloat_samples: Vec<BloatSample>,
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
            bloat_samples: Vec::new(),
            partitions: Vec::new(),
            replication: Vec::new(),
            wraparound: WraparoundSnapshot::default(),
        }
    }
}

// =============================
// Historical Retention (Phase 1)
// =============================
// High-resolution metric samples (ring buffers) retained for ~24h and future
// aggregation windows (7d, 30d, 365d) planned per README Historical Retention & Aggregation.
// This initial commit introduces in-memory data structures only; poller wiring
// and HTTP endpoints will follow in subsequent steps.

/// Point-in-time numeric observation for a specific metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimePoint {
    #[serde(with = "chrono::serde::ts_seconds")]
    pub ts: DateTime<Utc>,
    pub value: f64,
}

/// Daily aggregate summary (intraday collapse) for medium horizons.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyMetricSummary {
    /// UTC date (YYYY-MM-DD) string for simplicity (avoid chrono::Date serialization complexity here).
    pub date: String,
    pub mean: f64,
    pub max: f64,
    pub min: f64,
}

/// Weekly aggregate (Monday-based ISO week number) for long horizons.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeeklyMetricSummary {
    /// ISO week identifier "YYYY-Www".
    pub week: String,
    pub mean: f64,
    pub max: f64,
    pub min: f64,
}

/// Historical alert lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub id: u64,
    pub kind: String,
    pub message: String,
    pub severity: String, // warn | crit
    #[serde(with = "chrono::serde::ts_seconds")]
    pub started_at: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub cleared_at: Option<DateTime<Utc>>,
}

/// Ring-buffer configuration and storage for high-resolution series.
#[derive(Debug, Clone, Serialize)]
pub struct MetricHistory {
    pub max_points: usize,
    pub tps: Vec<TimePoint>,
    pub qps: Vec<TimePoint>,
    pub mean_latency_ms: Vec<TimePoint>,
    pub latency_p95_ms: Vec<TimePoint>,
    pub latency_p99_ms: Vec<TimePoint>,
    pub wal_bytes_per_second: Vec<TimePoint>,
    pub temp_bytes_per_second: Vec<TimePoint>,
    pub blocked_sessions: Vec<TimePoint>,
    pub connections: Vec<TimePoint>,
    // Future: add more series as needed (e.g., checkpoints, replication lag, dead tuple ratios)
    // Daily & weekly rollups (populated by future background task):
    pub daily: Vec<DailyMetricSummary>,
    pub weekly: Vec<WeeklyMetricSummary>,
}

impl MetricHistory {
    pub fn new(max_points: usize) -> Self {
        Self {
            max_points,
            tps: Vec::with_capacity(max_points),
            qps: Vec::with_capacity(max_points),
            mean_latency_ms: Vec::with_capacity(max_points),
            latency_p95_ms: Vec::with_capacity(max_points),
            latency_p99_ms: Vec::with_capacity(max_points),
            wal_bytes_per_second: Vec::with_capacity(max_points),
            temp_bytes_per_second: Vec::with_capacity(max_points),
            blocked_sessions: Vec::with_capacity(max_points),
            connections: Vec::with_capacity(max_points),
            daily: Vec::new(),
            weekly: Vec::new(),
        }
    }

    #[allow(dead_code)]
    fn push(series: &mut Vec<TimePoint>, point: TimePoint, max: usize) {
        series.push(point);
        if series.len() > max {
            let overflow = series.len() - max;
            series.drain(0..overflow);
        }
    }

    #[allow(dead_code)]
    pub fn record_tps(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(&mut self.tps, TimePoint { ts, value }, self.max_points);
    }
    #[allow(dead_code)]
    pub fn record_qps(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(&mut self.qps, TimePoint { ts, value }, self.max_points);
    }
    #[allow(dead_code)]
    pub fn record_mean_latency(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(
            &mut self.mean_latency_ms,
            TimePoint { ts, value },
            self.max_points,
        );
    }
    #[allow(dead_code)]
    pub fn record_latency_p95(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(
            &mut self.latency_p95_ms,
            TimePoint { ts, value },
            self.max_points,
        );
    }
    #[allow(dead_code)]
    pub fn record_latency_p99(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(
            &mut self.latency_p99_ms,
            TimePoint { ts, value },
            self.max_points,
        );
    }
    #[allow(dead_code)]
    pub fn record_wal_rate(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(
            &mut self.wal_bytes_per_second,
            TimePoint { ts, value },
            self.max_points,
        );
    }
    #[allow(dead_code)]
    pub fn record_temp_rate(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(
            &mut self.temp_bytes_per_second,
            TimePoint { ts, value },
            self.max_points,
        );
    }
    #[allow(dead_code)]
    pub fn record_blocked_sessions(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(
            &mut self.blocked_sessions,
            TimePoint { ts, value },
            self.max_points,
        );
    }
    #[allow(dead_code)]
    pub fn record_connections(&mut self, ts: DateTime<Utc>, value: f64) {
        Self::push(
            &mut self.connections,
            TimePoint { ts, value },
            self.max_points,
        );
    }

    /// Fetch a series by logical name; used by history API for dynamic selection.
    #[allow(dead_code)]
    pub fn series(&self, name: &str) -> Option<&[TimePoint]> {
        match name {
            "tps" => Some(&self.tps),
            "qps" => Some(&self.qps),
            "mean_latency_ms" => Some(&self.mean_latency_ms),
            "latency_p95_ms" => Some(&self.latency_p95_ms),
            "latency_p99_ms" => Some(&self.latency_p99_ms),
            "wal_bytes_per_second" => Some(&self.wal_bytes_per_second),
            "temp_bytes_per_second" => Some(&self.temp_bytes_per_second),
            "blocked_sessions" => Some(&self.blocked_sessions),
            "connections" => Some(&self.connections),
            _ => None,
        }
    }

    // TODO(jesperfjellin): Implement nightly aggregation job collapsing high-res points into DailyMetricSummary/WeeklyMetricSummary.
    // Strategy outline:
    //   1. At UTC rollover, scan each series' points for previous day, compute mean/max/min, push to daily.
    //   2. At start of ISO week (Mon 00:00 UTC), collapse last 7 daily entries into weekly summary.
    //   3. Enforce retention: keep 7d daily for 30d window (prune >30d), keep 52 weekly entries (~1y).
    //   4. Persist summaries + alert_events to JSON files under PGMON_DATA_DIR for durability across restarts.
    //   5. Provide /api/v1/history/{metric}?window=7d&rollup=daily and 30d/365d variants.
    // NOTE: Implementation deferred until baseline high-res history proves stable under load.
}

impl Default for MetricHistory {
    fn default() -> Self {
        // Provide a conservative default size if constructed indirectly (e.g., tests).
        MetricHistory::new(1000)
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
    pub open_alerts: Vec<String>,
    pub open_crit_alerts: Vec<String>,
    #[serde(skip)]
    pub(crate) _internal_alert_hash: u64,
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
            open_alerts: Vec::new(),
            open_crit_alerts: Vec::new(),
            _internal_alert_hash: 0,
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
    pub estimated_bloat_bytes: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BloatSample {
    pub relation: String,
    pub table_bytes: i64,
    pub free_bytes: i64,
    pub free_percent: f64,
    // Advanced fields from pgstattuple (exact) or pageinspect
    pub dead_tuple_count: Option<i64>,
    pub dead_tuple_percent: Option<f64>,
    pub live_tuple_count: Option<i64>,
    pub live_tuple_percent: Option<f64>,
    pub tuple_density: Option<f64>,
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
    pub cadence_seconds: Option<i64>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub suggested_next_start: Option<DateTime<Utc>>,
    #[serde(with = "chrono::serde::ts_seconds_option")]
    pub suggested_next_end: Option<DateTime<Utc>>,
    pub missing_future_partition: bool,
    pub future_gap_seconds: Option<i64>,
    pub advisory_note: Option<String>,
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
    pub latency_p99_ms: Option<f64>,
    pub wal_bytes_total: Option<i64>,
    pub wal_bytes_per_second: Option<f64>,
    #[allow(dead_code)]
    pub temp_bytes_total: Option<i64>,
    pub temp_bytes_per_second: Option<f64>,
    pub checkpoints_timed_total: Option<i64>,
    pub checkpoints_requested_total: Option<i64>,
    pub checkpoint_requested_ratio: Option<f64>,
    pub checkpoint_mean_interval_seconds: Option<f64>,
}

impl WorkloadSummary {
    /// Construct a `WorkloadSummary` given two cumulative `WorkloadSample`s.
    /// Returns `None` if the elapsed time between samples is zero or negative.
    pub fn from_samples(previous: &WorkloadSample, current: &WorkloadSample) -> Option<Self> {
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
            latency_p99_ms: None,
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

#[derive(Default)]
struct SharedStateInner {
    snapshots: RwLock<AppSnapshots>,
    loop_health: RwLock<HashMap<String, LoopHealth>>,
    workload_sample: RwLock<Option<WorkloadSample>>,
    // High-res metric retention (24h window target). Default 24h at 60s workload cadence ~1440 points; choose 1500.
    #[allow(dead_code)]
    metric_history: RwLock<MetricHistory>,
    // Alert history (recent events). Capacity is modest (e.g., 200) to keep memory bounded.
    #[allow(dead_code)]
    alert_events: RwLock<Vec<AlertEvent>>,
    #[allow(dead_code)]
    previous_alerts: RwLock<(Vec<String>, Vec<String>)>,
}

/// Shared state container for the HTTP layer and poller loops.
#[derive(Clone, Default)]
pub struct SharedState {
    inner: Arc<SharedStateInner>,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SharedStateInner {
                snapshots: RwLock::new(AppSnapshots::default()),
                loop_health: RwLock::new(HashMap::new()),
                workload_sample: RwLock::new(None),
                metric_history: RwLock::new(MetricHistory::new(1_500)),
                alert_events: RwLock::new(Vec::new()),
                previous_alerts: RwLock::new((Vec::new(), Vec::new())),
            }),
        }
    }

    pub async fn get_snapshots(&self) -> AppSnapshots {
        self.inner.snapshots.read().await.clone()
    }

    #[allow(dead_code)]
    pub async fn update_overview(&self, overview: OverviewSnapshot) {
        let mut guard = self.inner.snapshots.write().await;
        let prev_warn = guard.overview.open_alerts.clone();
        let prev_crit = guard.overview.open_crit_alerts.clone();
        guard.overview = overview;
        let current = guard.overview.clone();
        drop(guard);
        diff_alerts_and_record(self, &prev_warn, &prev_crit, &current).await;
    }

    pub async fn update_overview_with<F>(&self, apply: F)
    where
        F: FnOnce(&mut OverviewSnapshot),
    {
        let mut guard = self.inner.snapshots.write().await;
        let prev_warn = guard.overview.open_alerts.clone();
        let prev_crit = guard.overview.open_crit_alerts.clone();
        apply(&mut guard.overview);
        let current = guard.overview.clone();
        drop(guard);
        diff_alerts_and_record(self, &prev_warn, &prev_crit, &current).await;
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

    pub async fn update_bloat_samples(&self, rows: Vec<BloatSample>) {
        let mut guard = self.inner.snapshots.write().await;
        guard.bloat_samples = rows;
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

    /// Record workload-derived history points (called after overview update).
    #[allow(dead_code)]
    pub async fn record_history_points(&self, overview: &OverviewSnapshot) {
        let mut guard = self.inner.metric_history.write().await;
        if let Some(ts) = overview.generated_at {
            // Only record series derivable from snapshot numeric counts that remain.
            guard.record_blocked_sessions(ts, overview.blocked_sessions as f64);
            guard.record_connections(ts, overview.connections as f64);
            // Debug logging for early visibility into history recording cadence.
            tracing::debug!(
                ts = ts.timestamp(),
                connections = overview.connections,
                blocked_sessions = overview.blocked_sessions,
                "recorded overview history points"
            );
        }
    }

    /// Fetch high-resolution history for a metric name; caller may downsample.
    #[allow(dead_code)]
    pub async fn get_history_series(&self, metric: &str) -> Option<Vec<TimePoint>> {
        let guard = self.inner.metric_history.read().await;
        guard.series(metric).map(|s| s.to_vec())
    }

    /// Append an alert lifecycle event; enforce bounded capacity.
    #[allow(dead_code)]
    pub async fn push_alert_event(&self, event: AlertEvent) {
        const MAX_EVENTS: usize = 200; // retention cap
        let mut guard = self.inner.alert_events.write().await;
        guard.push(event);
        if guard.len() > MAX_EVENTS {
            let overflow = guard.len() - MAX_EVENTS;
            guard.drain(0..overflow);
        }
    }

    /// List recent alert events (chronological order, oldest first).
    #[allow(dead_code)]
    pub async fn list_alert_events(&self) -> Vec<AlertEvent> {
        self.inner.alert_events.read().await.clone()
    }

    pub async fn snapshot_metric_history(&self) -> MetricHistory {
        self.inner.metric_history.read().await.clone()
    }

    pub async fn replace_metric_history<F: FnOnce(&mut MetricHistory)>(&self, f: F) {
        let mut mh = self.inner.metric_history.write().await;
        f(&mut mh);
    }

    pub async fn replace_alert_events(&self, events: Vec<AlertEvent>) {
        let mut ae = self.inner.alert_events.write().await;
        *ae = events;
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

    #[allow(dead_code)]
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

/// Compute diff between previous and current alert sets and generate AlertEvent start/clear events.
async fn diff_alerts_and_record(
    state: &SharedState,
    prev_warn: &[String],
    prev_crit: &[String],
    overview: &OverviewSnapshot,
) {
    use std::collections::HashSet;
    let prev_warn_set: HashSet<&String> = prev_warn.iter().collect();
    let prev_crit_set: HashSet<&String> = prev_crit.iter().collect();
    let curr_warn_set: HashSet<&String> = overview.open_alerts.iter().collect();
    let curr_crit_set: HashSet<&String> = overview.open_crit_alerts.iter().collect();

    let started_warn: Vec<&String> = curr_warn_set.difference(&prev_warn_set).cloned().collect();
    let cleared_warn: Vec<&String> = prev_warn_set.difference(&curr_warn_set).cloned().collect();
    let started_crit: Vec<&String> = curr_crit_set.difference(&prev_crit_set).cloned().collect();
    let cleared_crit: Vec<&String> = prev_crit_set.difference(&curr_crit_set).cloned().collect();

    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static EVENT_ID: AtomicU64 = AtomicU64::new(50_000);
    let now = Utc::now();

    for msg in started_warn.iter().chain(started_crit.iter()) {
        state
            .push_alert_event(AlertEvent {
                id: EVENT_ID.fetch_add(1, AtomicOrdering::Relaxed),
                kind: classify_kind(msg),
                message: (*msg).clone(),
                severity: if overview.open_crit_alerts.contains(*msg) {
                    "crit".into()
                } else {
                    "warn".into()
                },
                started_at: now,
                cleared_at: None,
            })
            .await;
    }

    for msg in cleared_warn.iter().chain(cleared_crit.iter()) {
        state
            .push_alert_event(AlertEvent {
                id: EVENT_ID.fetch_add(1, AtomicOrdering::Relaxed),
                kind: classify_kind(msg),
                message: (*msg).clone(),
                severity: if prev_crit_set.contains(*msg) {
                    "crit".into()
                } else {
                    "warn".into()
                },
                started_at: now,
                cleared_at: Some(now),
            })
            .await;
    }
}

fn classify_kind(message: &str) -> String {
    let lower = message.to_ascii_lowercase();
    for (needle, kind) in [
        ("connections", "connections"),
        ("longest transaction", "long_txn"),
        ("longest blocked", "blocked"),
        ("wal surge", "wal_surge"),
        ("temp surge", "temp_surge"),
        ("checkpoint thrash", "checkpoint"),
        ("dead tuple backlog", "dead_tuples"),
        ("autovac starvation", "autovac_starvation"),
        ("replication lag", "replication_lag"),
        ("wraparound", "wraparound"),
        ("partition horizon", "partition_gap"),
        ("stats stale", "stale_stats"),
    ] {
        if lower.contains(needle) {
            return kind.to_string();
        }
    }
    "other".into()
}
