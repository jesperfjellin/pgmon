use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

const AUTOVACUUM_PHASES: &[&str] = &[
    "initializing",
    "scanning heap",
    "vacuuming heap",
    "vacuuming indexes",
    "vacuuming TOAST table",
    "cleaning up indexes",
    "cleaning up TOAST table",
    "truncating heap",
    "performing final cleanup",
];

/// Metrics registry for the agent scraped by Prometheus.
#[derive(Clone)]
pub struct AppMetrics {
    registry: Arc<Registry>,
    loops: LoopMetrics,
    overview: OverviewMetrics,
    workload: WorkloadMetrics,
    wraparound: WraparoundMetrics,
    replication: ReplicationMetrics,
    storage: StorageMetrics,
    unused_indexes: UnusedIndexMetrics,
    autovac: AutovacMetrics,
    hot_path: HotPathMetrics,
    index_usage: IndexMetrics,
    stale_stats: StaleStatsMetrics,
    statements: StatementMetrics,
    partition: PartitionMetrics,
    bloat_samples: BloatSampleMetrics,
    alert_counters: AlertCounters,
}

impl AppMetrics {
    pub fn new() -> Result<Self> {
        let registry = Arc::new(Registry::new_custom(Some("pgmon".into()), None)?);

        let loops = LoopMetrics::register(&registry)?;
        let overview = OverviewMetrics::register(&registry)?;
        let workload = WorkloadMetrics::register(&registry)?;
        let wraparound = WraparoundMetrics::register(&registry)?;
        let replication = ReplicationMetrics::register(&registry)?;
        let storage = StorageMetrics::register(&registry)?;
        let unused_indexes = UnusedIndexMetrics::register(&registry)?;
        let autovac = AutovacMetrics::register(&registry)?;
        let hot_path = HotPathMetrics::register(&registry)?;
        let index_usage = IndexMetrics::register(&registry)?;
        let stale_stats = StaleStatsMetrics::register(&registry)?;
        let statements = StatementMetrics::register(&registry)?;
        let partition = PartitionMetrics::register(&registry)?;
        let bloat_samples = BloatSampleMetrics::register(&registry)?;
        let alert_counters = AlertCounters::register(&registry)?;

        Ok(Self {
            registry,
            loops,
            overview,
            workload,
            wraparound,
            replication,
            storage,
            unused_indexes,
            autovac,
            hot_path,
            index_usage,
            stale_stats,
            statements,
            partition,
            bloat_samples,
            alert_counters,
        })
    }

    /// Observe the execution duration for a loop.
    pub fn observe_duration(&self, loop_name: &str, duration: Duration) {
        let seconds = duration.as_secs_f64();
        self.loops
            .scrape_duration
            .with_label_values(&[loop_name])
            .observe(seconds);
    }

    /// Record a success flag for a loop iteration (1=success, 0=failed).
    pub fn record_success(&self, loop_name: &str, success: bool) {
        self.loops
            .last_success
            .with_label_values(&[loop_name])
            .set(if success { 1 } else { 0 });
    }

    /// Increment the error counter for a loop.
    pub fn inc_error(&self, loop_name: &str) {
        self.loops
            .errors_total
            .with_label_values(&[loop_name])
            .inc();
    }

    /// Record connection and lock headline metrics.
    pub fn set_overview_metrics(
        &self,
        cluster: &str,
        connections: i64,
        max_connections: i64,
        blocked_sessions: i64,
        longest_tx_seconds: Option<f64>,
        longest_blocked_seconds: Option<f64>,
    ) {
        let cluster_label = &[cluster];
        self.overview
            .connections
            .with_label_values(cluster_label)
            .set(connections);
        self.overview
            .max_connections
            .with_label_values(cluster_label)
            .set(max_connections);
        self.overview
            .blocked_sessions
            .with_label_values(cluster_label)
            .set(blocked_sessions);

        set_optional_gauge(
            &self.overview.longest_tx_seconds,
            cluster,
            longest_tx_seconds,
        );
        set_optional_gauge(
            &self.overview.longest_blocked_seconds,
            cluster,
            longest_blocked_seconds,
        );
    }

    pub fn set_workload_metrics(
        &self,
        cluster: &str,
        tps: Option<f64>,
        qps: Option<f64>,
        mean_latency_ms: Option<f64>,
        wal_bytes_total: Option<i64>,
        wal_bytes_per_second: Option<f64>,
        checkpoints_timed_total: Option<i64>,
        checkpoints_requested_total: Option<i64>,
        checkpoint_requested_ratio: Option<f64>,
        checkpoint_mean_interval_seconds: Option<f64>,
        temp_bytes_per_second: Option<f64>,
        latency_p95_ms: Option<f64>,
        latency_p99_ms: Option<f64>,
    ) {
        set_optional_gauge(&self.workload.tps, cluster, tps);
        set_optional_gauge(&self.workload.qps, cluster, qps);
        let mean_latency_seconds = mean_latency_ms.map(|ms| ms / 1_000.0);
        set_optional_gauge(
            &self.workload.mean_latency_seconds,
            cluster,
            mean_latency_seconds,
        );
        let latency_p95_seconds = latency_p95_ms.map(|ms| ms / 1_000.0);
        set_optional_gauge(
            &self.workload.latency_seconds_p95,
            cluster,
            latency_p95_seconds,
        );
        let latency_p99_seconds = latency_p99_ms.map(|ms| ms / 1_000.0);
        set_optional_gauge(
            &self.workload.latency_seconds_p99,
            cluster,
            latency_p99_seconds,
        );
        set_optional_int_gauge(&self.workload.wal_bytes_total, cluster, wal_bytes_total);
        set_optional_gauge(
            &self.workload.wal_bytes_per_second,
            cluster,
            wal_bytes_per_second,
        );
        set_optional_int_gauge(
            &self.workload.checkpoints_timed_total,
            cluster,
            checkpoints_timed_total,
        );
        set_optional_int_gauge(
            &self.workload.checkpoints_requested_total,
            cluster,
            checkpoints_requested_total,
        );
        set_optional_gauge(
            &self.workload.checkpoint_requested_ratio,
            cluster,
            checkpoint_requested_ratio,
        );
        set_optional_gauge(
            &self.workload.checkpoint_mean_interval_seconds,
            cluster,
            checkpoint_mean_interval_seconds,
        );
        set_optional_gauge(
            &self.workload.temp_bytes_per_second,
            cluster,
            temp_bytes_per_second,
        );
    }

    pub fn set_storage_metrics(&self, cluster: &str, entries: &[crate::state::StorageEntry]) {
        self.storage.relation_size_bytes.reset();
        self.storage.relation_table_bytes.reset();
        self.storage.relation_index_bytes.reset();
        self.storage.relation_toast_bytes.reset();
        self.storage.relation_bloat_estimated_bytes.reset();

        for entry in entries {
            let relation = sanitize_label(&entry.relation);
            let relkind = entry.relkind.as_str();
            self.storage
                .relation_size_bytes
                .with_label_values(&[cluster, relation.as_str(), relkind])
                .set(entry.total_bytes);

            self.storage
                .relation_table_bytes
                .with_label_values(&[cluster, relation.as_str(), relkind])
                .set(entry.table_bytes);

            self.storage
                .relation_index_bytes
                .with_label_values(&[cluster, relation.as_str(), relkind])
                .set(entry.index_bytes);

            self.storage
                .relation_toast_bytes
                .with_label_values(&[cluster, relation.as_str(), relkind])
                .set(entry.toast_bytes);

            if let Some(bloat) = entry.estimated_bloat_bytes {
                self.storage
                    .relation_bloat_estimated_bytes
                    .with_label_values(&[cluster, relation.as_str(), relkind])
                    .set(bloat.max(0));
            }
        }
    }

    pub fn set_unused_index_metrics(
        &self,
        cluster: &str,
        entries: &[crate::state::UnusedIndexEntry],
    ) {
        self.unused_indexes.bytes.reset();

        for entry in entries {
            let relation = sanitize_label(&entry.relation);
            let index = sanitize_label(&entry.index);
            self.unused_indexes
                .bytes
                .with_label_values(&[cluster, relation.as_str(), index.as_str()])
                .set(entry.bytes);
        }
    }

    pub fn set_statement_metrics(&self, cluster: &str, entries: &[crate::state::TopQueryEntry]) {
        self.statements.calls.reset();
        self.statements.total_time_seconds.reset();
        self.statements.shared_reads.reset();

        for entry in entries {
            let queryid = entry.queryid.to_string();
            let labels = [cluster, queryid.as_str()];
            self.statements
                .calls
                .with_label_values(&labels)
                .set(entry.calls);
            self.statements
                .total_time_seconds
                .with_label_values(&labels)
                .set(entry.total_time_seconds);
            self.statements
                .shared_reads
                .with_label_values(&labels)
                .set(entry.shared_blks_read);
        }
    }

    pub fn set_stale_stats_metrics(&self, cluster: &str, entries: &[crate::state::StaleStatEntry]) {
        self.stale_stats.last_analyze_age.reset();

        for entry in entries {
            if let Some(hours) = entry.hours_since_analyze {
                let relation = sanitize_label(&entry.relation);
                self.stale_stats
                    .last_analyze_age
                    .with_label_values(&[cluster, relation.as_str()])
                    .set(hours * 3600.0);
            }
        }
    }

    pub fn set_partition_metrics(&self, cluster: &str, entries: &[crate::state::PartitionSlice]) {
        self.partition.missing_future.reset();
        self.partition.future_gap_seconds.reset();

        for entry in entries {
            let parent = sanitize_label(&entry.parent);
            let labels = [cluster, parent.as_str()];
            let gap_seconds = entry.future_gap_seconds.unwrap_or(0) as f64;

            self.partition
                .missing_future
                .with_label_values(&labels)
                .set(if entry.missing_future_partition { 1 } else { 0 });
            self.partition
                .future_gap_seconds
                .with_label_values(&labels)
                .set(gap_seconds);
        }
    }

    pub fn set_bloat_sample_metrics(&self, cluster: &str, entries: &[crate::state::BloatSample]) {
        self.bloat_samples.free_bytes.reset();
        self.bloat_samples.free_percent.reset();

        for entry in entries {
            let relation = sanitize_label(&entry.relation);
            self.bloat_samples
                .free_bytes
                .with_label_values(&[cluster, relation.as_str()])
                .set(entry.free_bytes);
            self.bloat_samples
                .free_percent
                .with_label_values(&[cluster, relation.as_str()])
                .set(entry.free_percent);
        }
    }

    pub fn set_autovacuum_metrics(&self, cluster: &str, entries: &[crate::state::AutovacuumEntry]) {
        self.autovac.dead_tuples.reset();
        self.autovac.dead_ratio.reset();
        self.autovac.last_autovacuum.reset();
        self.autovac.last_autoanalyze.reset();
        self.autovac.alerts_dead_backlog.reset();

        let now = Utc::now();

        for entry in entries {
            let relation = sanitize_label(&entry.relation);
            self.autovac
                .dead_tuples
                .with_label_values(&[cluster, relation.as_str()])
                .set(entry.n_dead_tup);

            let total = entry.n_live_tup + entry.n_dead_tup;
            if total > 0 {
                let ratio = (entry.n_dead_tup as f64 / total as f64) * 100.0;
                self.autovac
                    .dead_ratio
                    .with_label_values(&[cluster, relation.as_str()])
                    .set(ratio);

                if let Some(thresholds) = dead_tuple_alert(ratio, entry.n_dead_tup) {
                    self.autovac
                        .alerts_dead_backlog
                        .with_label_values(&[cluster, relation.as_str(), thresholds.as_str()])
                        .set(thresholds.gauge_value());
                }
            }

            if let Some(last) = entry.last_autovacuum {
                let age_seconds = (now - last).num_seconds();
                if age_seconds >= 0 {
                    self.autovac
                        .last_autovacuum
                        .with_label_values(&[cluster, relation.as_str()])
                        .set(age_seconds as f64);
                }
            }

            if let Some(last) = entry.last_autoanalyze {
                let age_seconds = (now - last).num_seconds();
                if age_seconds >= 0 {
                    self.autovac
                        .last_autoanalyze
                        .with_label_values(&[cluster, relation.as_str()])
                        .set(age_seconds as f64);
                }
            }
        }
    }

    pub fn set_autovacuum_jobs(&self, cluster: &str, jobs: &[(String, i64)]) {
        let mut counts: HashMap<&str, i64> = HashMap::new();
        for (phase, count) in jobs {
            counts.insert(phase.as_str(), *count);
        }

        for phase in AUTOVACUUM_PHASES {
            let value = *counts.get(phase).unwrap_or(&0);
            let label = sanitize_label(phase);
            self.hot_path
                .autovac_jobs
                .with_label_values(&[cluster, label.as_str()])
                .set(value);
        }

        for (&phase, &value) in counts.iter() {
            if !AUTOVACUUM_PHASES.iter().any(|known| *known == phase) {
                let label = sanitize_label(phase);
                self.hot_path
                    .autovac_jobs
                    .with_label_values(&[cluster, label.as_str()])
                    .set(value);
            }
        }
    }

    pub fn set_temp_metrics(&self, cluster: &str, stats: &[(String, i64, i64)]) {
        self.hot_path.temp_files.reset();
        self.hot_path.temp_bytes.reset();

        for (db, temp_files, temp_bytes) in stats {
            let db_label = sanitize_label(db);
            self.hot_path
                .temp_files
                .with_label_values(&[cluster, db_label.as_str()])
                .set(*temp_files);
            self.hot_path
                .temp_bytes
                .with_label_values(&[cluster, db_label.as_str()])
                .set(*temp_bytes);
        }
    }

    pub fn set_index_usage_metrics(&self, cluster: &str, stats: &[(String, String, i64)]) {
        self.index_usage.scans_total.reset();

        for (relation, index, scans) in stats {
            let relation_label = sanitize_label(relation);
            let index_label = sanitize_label(index);
            self.index_usage
                .scans_total
                .with_label_values(&[cluster, relation_label.as_str(), index_label.as_str()])
                .set(*scans);
        }
    }

    pub fn inc_alert(&self, cluster: &str, kind: AlertKind, severity: AlertSeverity) {
        let severity_label = severity.as_str();
        let kind_label = kind.as_str();
        self.alert_counters
            .alerts_total
            .with_label_values(&[cluster, kind_label, severity_label])
            .inc();
    }

    pub fn set_wraparound_metrics(
        &self,
        cluster: &str,
        databases: &[crate::state::WraparoundDatabase],
        relations: &[crate::state::WraparoundRelation],
    ) {
        self.wraparound.database_tx_age.reset();
        self.wraparound.relation_tx_age.reset();

        for db in databases {
            let database = sanitize_label(&db.database);
            self.wraparound
                .database_tx_age
                .with_label_values(&[cluster, database.as_str()])
                .set(db.tx_age as f64);
        }

        for rel in relations {
            let relation = sanitize_label(&rel.relation);
            self.wraparound
                .relation_tx_age
                .with_label_values(&[cluster, relation.as_str()])
                .set(rel.tx_age as f64);
        }
    }

    pub fn set_replication_metrics(&self, cluster: &str, replicas: &[crate::state::ReplicaLag]) {
        self.replication.lag_seconds.reset();
        self.replication.lag_bytes.reset();

        for replica in replicas {
            let replica_label = sanitize_label(&replica.replica);
            if let Some(seconds) = replica.lag_seconds {
                self.replication
                    .lag_seconds
                    .with_label_values(&[cluster, replica_label.as_str()])
                    .set(seconds);
            }
            if let Some(bytes) = replica.lag_bytes {
                self.replication
                    .lag_bytes
                    .with_label_values(&[cluster, replica_label.as_str()])
                    .set(bytes);
            }
        }
    }

    /// Encode metrics into Prometheus exposition format.
    pub fn encode(&self) -> Result<String> {
        let families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}

#[derive(Clone)]
struct LoopMetrics {
    scrape_duration: HistogramVec,
    last_success: IntGaugeVec,
    errors_total: IntCounterVec,
}

impl LoopMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let scrape_duration = HistogramVec::new(
            HistogramOpts::new("scrape_duration_seconds", "Loop execution duration"),
            &["loop"],
        )?;
        registry.register(Box::new(scrape_duration.clone()))?;

        let last_success = IntGaugeVec::new(
            Opts::new(
                "last_scrape_success",
                "Loop success flag (1=success, 0=failure)",
            ),
            &["loop"],
        )?;
        registry.register(Box::new(last_success.clone()))?;

        let errors_total =
            IntCounterVec::new(Opts::new("errors_total", "Total loop errors"), &["loop"])?;
        registry.register(Box::new(errors_total.clone()))?;

        Ok(Self {
            scrape_duration,
            last_success,
            errors_total,
        })
    }
}

#[derive(Clone)]
struct OverviewMetrics {
    connections: IntGaugeVec,
    max_connections: IntGaugeVec,
    blocked_sessions: IntGaugeVec,
    longest_tx_seconds: GaugeVec,
    longest_blocked_seconds: GaugeVec,
}

impl OverviewMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let connections = IntGaugeVec::new(
            Opts::new("pg_connections", "Active connections observed"),
            &["cluster"],
        )?;
        registry.register(Box::new(connections.clone()))?;

        let max_connections = IntGaugeVec::new(
            Opts::new("pg_max_connections", "Postgres max_connections setting"),
            &["cluster"],
        )?;
        registry.register(Box::new(max_connections.clone()))?;

        let blocked_sessions = IntGaugeVec::new(
            Opts::new("pg_blocked_sessions_total", "Currently blocked backends"),
            &["cluster"],
        )?;
        registry.register(Box::new(blocked_sessions.clone()))?;

        let longest_tx_seconds = GaugeVec::new(
            Opts::new(
                "pg_longest_transaction_seconds",
                "Longest active transaction duration",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(longest_tx_seconds.clone()))?;

        let longest_blocked_seconds = GaugeVec::new(
            Opts::new(
                "pg_longest_blocked_seconds",
                "Longest blocking duration among waiting sessions",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(longest_blocked_seconds.clone()))?;

        Ok(Self {
            connections,
            max_connections,
            blocked_sessions,
            longest_tx_seconds,
            longest_blocked_seconds,
        })
    }
}

#[derive(Clone)]
struct HotPathMetrics {
    autovac_jobs: IntGaugeVec,
    temp_files: IntGaugeVec,
    temp_bytes: IntGaugeVec,
}

impl HotPathMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let autovac_jobs = IntGaugeVec::new(
            Opts::new(
                "pg_autovacuum_jobs",
                "Number of autovacuum workers grouped by phase",
            ),
            &["cluster", "phase"],
        )?;
        registry.register(Box::new(autovac_jobs.clone()))?;

        let temp_files = IntGaugeVec::new(
            Opts::new(
                "pg_temp_files_total",
                "Total temp files created per database (pg_stat_database)",
            ),
            &["cluster", "db"],
        )?;
        registry.register(Box::new(temp_files.clone()))?;

        let temp_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_temp_bytes_total",
                "Total temp bytes written per database (pg_stat_database)",
            ),
            &["cluster", "db"],
        )?;
        registry.register(Box::new(temp_bytes.clone()))?;

        Ok(Self {
            autovac_jobs,
            temp_files,
            temp_bytes,
        })
    }
}

#[derive(Clone)]
struct WorkloadMetrics {
    tps: GaugeVec,
    qps: GaugeVec,
    mean_latency_seconds: GaugeVec,
    latency_seconds_p95: GaugeVec,
    latency_seconds_p99: GaugeVec,
    wal_bytes_total: IntGaugeVec,
    wal_bytes_per_second: GaugeVec,
    checkpoints_timed_total: IntGaugeVec,
    checkpoints_requested_total: IntGaugeVec,
    checkpoint_requested_ratio: GaugeVec,
    checkpoint_mean_interval_seconds: GaugeVec,
    temp_bytes_per_second: GaugeVec,
}

impl WorkloadMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let tps = GaugeVec::new(Opts::new("pg_tps", "Transactions per second"), &["cluster"])?;
        registry.register(Box::new(tps.clone()))?;

        let qps = GaugeVec::new(Opts::new("pg_qps", "Queries per second"), &["cluster"])?;
        registry.register(Box::new(qps.clone()))?;

        let mean_latency_seconds = GaugeVec::new(
            Opts::new(
                "pg_query_latency_seconds_mean",
                "Mean query latency derived from pg_stat_statements",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(mean_latency_seconds.clone()))?;

        let latency_seconds_p95 = GaugeVec::new(
            Opts::new(
                "pg_query_latency_seconds_p95",
                "95th percentile query latency derived from pg_stat_monitor when available",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(latency_seconds_p95.clone()))?;

        let latency_seconds_p99 = GaugeVec::new(
            Opts::new(
                "pg_query_latency_seconds_p99",
                "99th percentile query latency derived from pg_stat_monitor when available",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(latency_seconds_p99.clone()))?;

        let wal_bytes_total = IntGaugeVec::new(
            Opts::new("pg_wal_bytes_written_total", "Total WAL bytes written"),
            &["cluster"],
        )?;
        registry.register(Box::new(wal_bytes_total.clone()))?;

        let wal_bytes_per_second = GaugeVec::new(
            Opts::new(
                "pg_wal_bytes_written_per_second",
                "WAL bytes per second derived from pg_stat_wal",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(wal_bytes_per_second.clone()))?;

        let checkpoints_timed_total = IntGaugeVec::new(
            Opts::new("pg_checkpoints_timed_total", "Total timed checkpoints"),
            &["cluster"],
        )?;
        registry.register(Box::new(checkpoints_timed_total.clone()))?;

        let checkpoints_requested_total = IntGaugeVec::new(
            Opts::new(
                "pg_checkpoints_requested_total",
                "Total requested checkpoints",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(checkpoints_requested_total.clone()))?;

        let checkpoint_requested_ratio = GaugeVec::new(
            Opts::new(
                "pg_checkpoints_requested_ratio",
                "Ratio of requested checkpoints over total in the last sample",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(checkpoint_requested_ratio.clone()))?;

        let checkpoint_mean_interval_seconds = GaugeVec::new(
            Opts::new(
                "pg_checkpoints_mean_interval_seconds",
                "Mean interval between checkpoints over the last sample",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(checkpoint_mean_interval_seconds.clone()))?;

        let temp_bytes_per_second = GaugeVec::new(
            Opts::new(
                "pg_temp_bytes_written_per_second",
                "Temp bytes per second derived from pg_stat_database",
            ),
            &["cluster"],
        )?;
        registry.register(Box::new(temp_bytes_per_second.clone()))?;

        Ok(Self {
            tps,
            qps,
            mean_latency_seconds,
            latency_seconds_p95,
            latency_seconds_p99,
            wal_bytes_total,
            wal_bytes_per_second,
            checkpoints_timed_total,
            checkpoints_requested_total,
            checkpoint_requested_ratio,
            checkpoint_mean_interval_seconds,
            temp_bytes_per_second,
        })
    }
}

fn set_optional_gauge(vec: &GaugeVec, cluster: &str, value: Option<f64>) {
    let gauge = vec.with_label_values(&[cluster]);
    gauge.set(value.unwrap_or(0.0));
}

fn set_optional_int_gauge(vec: &IntGaugeVec, cluster: &str, value: Option<i64>) {
    vec.with_label_values(&[cluster]).set(value.unwrap_or(0));
}

#[derive(Clone)]
struct WraparoundMetrics {
    database_tx_age: GaugeVec,
    relation_tx_age: GaugeVec,
}

impl WraparoundMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let database_tx_age = GaugeVec::new(
            Opts::new(
                "pg_wraparound_database_tx_age",
                "Transaction ID age per database",
            ),
            &["cluster", "database"],
        )?;
        registry.register(Box::new(database_tx_age.clone()))?;

        let relation_tx_age = GaugeVec::new(
            Opts::new(
                "pg_wraparound_relation_tx_age",
                "Transaction ID age per relation",
            ),
            &["cluster", "relation"],
        )?;
        registry.register(Box::new(relation_tx_age.clone()))?;

        Ok(Self {
            database_tx_age,
            relation_tx_age,
        })
    }
}

#[derive(Clone)]
struct ReplicationMetrics {
    lag_seconds: GaugeVec,
    lag_bytes: IntGaugeVec,
}

impl ReplicationMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let lag_seconds = GaugeVec::new(
            Opts::new(
                "pg_replication_lag_seconds",
                "Replication lag in seconds per replica",
            ),
            &["cluster", "replica"],
        )?;
        registry.register(Box::new(lag_seconds.clone()))?;

        let lag_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_replication_lag_bytes",
                "Replication lag in bytes per replica",
            ),
            &["cluster", "replica"],
        )?;
        registry.register(Box::new(lag_bytes.clone()))?;

        Ok(Self {
            lag_seconds,
            lag_bytes,
        })
    }
}

#[derive(Clone)]
struct StorageMetrics {
    relation_size_bytes: IntGaugeVec,
    relation_table_bytes: IntGaugeVec,
    relation_index_bytes: IntGaugeVec,
    relation_toast_bytes: IntGaugeVec,
    relation_bloat_estimated_bytes: IntGaugeVec,
}

impl StorageMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let relation_size_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_relation_size_bytes",
                "Total relation size including indexes and toast",
            ),
            &["cluster", "relation", "relkind"],
        )?;
        registry.register(Box::new(relation_size_bytes.clone()))?;

        let relation_table_bytes = IntGaugeVec::new(
            Opts::new("pg_relation_table_bytes", "Heap bytes for relation"),
            &["cluster", "relation", "relkind"],
        )?;
        registry.register(Box::new(relation_table_bytes.clone()))?;

        let relation_index_bytes = IntGaugeVec::new(
            Opts::new("pg_relation_index_bytes", "Index bytes for relation"),
            &["cluster", "relation", "relkind"],
        )?;
        registry.register(Box::new(relation_index_bytes.clone()))?;

        let relation_toast_bytes = IntGaugeVec::new(
            Opts::new("pg_relation_toast_bytes", "TOAST bytes for relation"),
            &["cluster", "relation", "relkind"],
        )?;
        registry.register(Box::new(relation_toast_bytes.clone()))?;

        let relation_bloat_estimated_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_relation_bloat_estimated_bytes",
                "Estimated table bloat bytes derived from dead tuple density",
            ),
            &["cluster", "relation", "relkind"],
        )?;
        registry.register(Box::new(relation_bloat_estimated_bytes.clone()))?;

        Ok(Self {
            relation_size_bytes,
            relation_table_bytes,
            relation_index_bytes,
            relation_toast_bytes,
            relation_bloat_estimated_bytes,
        })
    }
}

#[derive(Clone)]
struct IndexMetrics {
    scans_total: IntGaugeVec,
}

impl IndexMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let scans_total = IntGaugeVec::new(
            Opts::new(
                "pg_index_scans_total",
                "Total index scans observed (pg_stat_user_indexes)",
            ),
            &["cluster", "relation", "index"],
        )?;
        registry.register(Box::new(scans_total.clone()))?;

        Ok(Self { scans_total })
    }
}

#[derive(Clone)]
struct StaleStatsMetrics {
    last_analyze_age: GaugeVec,
}

impl StaleStatsMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let last_analyze_age = GaugeVec::new(
            Opts::new(
                "pg_table_stats_stale_seconds",
                "Seconds since statistics were last refreshed for a table",
            ),
            &["cluster", "relation"],
        )?;
        registry.register(Box::new(last_analyze_age.clone()))?;

        Ok(Self { last_analyze_age })
    }
}

#[derive(Clone)]
struct UnusedIndexMetrics {
    bytes: IntGaugeVec,
}

impl UnusedIndexMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let bytes = IntGaugeVec::new(
            Opts::new(
                "pg_unused_index_bytes",
                "Footprint of indexes with zero scans since statistics reset",
            ),
            &["cluster", "relation", "index"],
        )?;
        registry.register(Box::new(bytes.clone()))?;

        Ok(Self { bytes })
    }
}

#[derive(Clone)]
struct StatementMetrics {
    calls: IntGaugeVec,
    total_time_seconds: GaugeVec,
    shared_reads: IntGaugeVec,
}

impl StatementMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let calls = IntGaugeVec::new(
            Opts::new(
                "pg_stmt_calls_total",
                "Total calls recorded for the queryid (Top-N subset)",
            ),
            &["cluster", "queryid"],
        )?;
        registry.register(Box::new(calls.clone()))?;

        let total_time_seconds = GaugeVec::new(
            Opts::new(
                "pg_stmt_time_seconds_total",
                "Total execution time in seconds recorded for the queryid (Top-N subset)",
            ),
            &["cluster", "queryid"],
        )?;
        registry.register(Box::new(total_time_seconds.clone()))?;

        let shared_reads = IntGaugeVec::new(
            Opts::new(
                "pg_stmt_shared_blks_read_total",
                "Total shared blocks read for the queryid (Top-N subset)",
            ),
            &["cluster", "queryid"],
        )?;
        registry.register(Box::new(shared_reads.clone()))?;

        Ok(Self {
            calls,
            total_time_seconds,
            shared_reads,
        })
    }
}

#[derive(Clone)]
struct PartitionMetrics {
    missing_future: IntGaugeVec,
    future_gap_seconds: GaugeVec,
}

impl PartitionMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let missing_future = IntGaugeVec::new(
            Opts::new(
                "pg_partition_missing_future",
                "Flag indicating whether a partitioned table lacks coverage beyond the configured horizon",
            ),
            &["cluster", "parent"],
        )?;
        registry.register(Box::new(missing_future.clone()))?;

        let future_gap_seconds = GaugeVec::new(
            Opts::new(
                "pg_partition_future_gap_seconds",
                "Seconds between the current horizon cut-off and the latest available partition upper bound",
            ),
            &["cluster", "parent"],
        )?;
        registry.register(Box::new(future_gap_seconds.clone()))?;

        Ok(Self {
            missing_future,
            future_gap_seconds,
        })
    }
}

#[derive(Clone)]
struct BloatSampleMetrics {
    free_bytes: IntGaugeVec,
    free_percent: GaugeVec,
}

impl BloatSampleMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let free_bytes = IntGaugeVec::new(
            Opts::new(
                "pg_relation_bloat_sample_free_bytes",
                "Free space bytes reported by pgstattuple_approx",
            ),
            &["cluster", "relation"],
        )?;
        registry.register(Box::new(free_bytes.clone()))?;

        let free_percent = GaugeVec::new(
            Opts::new(
                "pg_relation_bloat_sample_free_percent",
                "Free space percentage reported by pgstattuple_approx",
            ),
            &["cluster", "relation"],
        )?;
        registry.register(Box::new(free_percent.clone()))?;

        Ok(Self {
            free_bytes,
            free_percent,
        })
    }
}

#[derive(Clone)]
struct AutovacMetrics {
    dead_tuples: IntGaugeVec,
    dead_ratio: GaugeVec,
    last_autovacuum: GaugeVec,
    last_autoanalyze: GaugeVec,
    alerts_dead_backlog: IntGaugeVec,
}

impl AutovacMetrics {
    fn register(registry: &Registry) -> Result<Self> {
        let dead_tuples = IntGaugeVec::new(
            Opts::new("pg_table_dead_tuples", "Dead tuples per table"),
            &["cluster", "relation"],
        )?;
        registry.register(Box::new(dead_tuples.clone()))?;

        let dead_ratio = GaugeVec::new(
            Opts::new(
                "pg_table_pct_dead",
                "Dead tuple percentage per table derived from pg_stat_user_tables",
            ),
            &["cluster", "relation"],
        )?;
        registry.register(Box::new(dead_ratio.clone()))?;

        let last_autovacuum = GaugeVec::new(
            Opts::new(
                "pg_table_last_autovacuum_seconds",
                "Seconds since last autovacuum",
            ),
            &["cluster", "relation"],
        )?;
        registry.register(Box::new(last_autovacuum.clone()))?;

        let last_autoanalyze = GaugeVec::new(
            Opts::new(
                "pg_table_last_autoanalyze_seconds",
                "Seconds since last autoanalyze",
            ),
            &["cluster", "relation"],
        )?;
        registry.register(Box::new(last_autoanalyze.clone()))?;

        let alerts_dead_backlog = IntGaugeVec::new(
            Opts::new(
                "pg_dead_tuple_backlog_alert",
                "Dead tuple backlog alert state (1=warn,2=crit,0=ok)",
            ),
            &["cluster", "relation", "severity"],
        )?;
        registry.register(Box::new(alerts_dead_backlog.clone()))?;

        Ok(Self {
            dead_tuples,
            dead_ratio,
            last_autovacuum,
            last_autoanalyze,
            alerts_dead_backlog,
        })
    }
}

fn sanitize_label(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) fn dead_tuple_alert(ratio: f64, dead_tuples: i64) -> Option<AlertSeverity> {
    if dead_tuples >= 100_000 && ratio >= 20.0 {
        if ratio >= 40.0 || dead_tuples >= 500_000 {
            Some(AlertSeverity::Crit)
        } else {
            Some(AlertSeverity::Warn)
        }
    } else {
        None
    }
}

#[derive(Clone)]
struct AlertCounters {
    alerts_total: IntCounterVec,
}

impl AlertCounters {
    fn register(registry: &Registry) -> Result<Self> {
        let alerts_total = IntCounterVec::new(
            Opts::new(
                "alerts_total",
                "Total emitted alerts grouped by kind and severity",
            ),
            &["cluster", "kind", "severity"],
        )?;
        registry.register(Box::new(alerts_total.clone()))?;
        Ok(Self { alerts_total })
    }
}

#[derive(Copy, Clone)]
pub enum AlertSeverity {
    Warn,
    Crit,
}

impl AlertSeverity {
    fn as_str(self) -> &'static str {
        match self {
            AlertSeverity::Warn => "warn",
            AlertSeverity::Crit => "crit",
        }
    }

    fn gauge_value(self) -> i64 {
        match self {
            AlertSeverity::Warn => 1,
            AlertSeverity::Crit => 2,
        }
    }
}

#[derive(Copy, Clone)]
pub enum AlertKind {
    Connections,
    LongTransaction,
    BlockedSession,
    ReplicationLag,
    DeadTuples,
    PartitionGap,
    Wraparound,
    WalSurge,
    TempSurge,
    AutovacuumStarvation,
    CheckpointThrash,
    StaleStats,
}

impl AlertKind {
    fn as_str(self) -> &'static str {
        match self {
            AlertKind::Connections => "connections",
            AlertKind::LongTransaction => "long_transaction",
            AlertKind::BlockedSession => "blocked_session",
            AlertKind::ReplicationLag => "replication_lag",
            AlertKind::DeadTuples => "dead_tuples",
            AlertKind::PartitionGap => "partition_gap",
            AlertKind::Wraparound => "wraparound",
            AlertKind::WalSurge => "wal_surge",
            AlertKind::TempSurge => "temp_surge",
            AlertKind::AutovacuumStarvation => "autovacuum_starvation",
            AlertKind::CheckpointThrash => "checkpoint_thrash",
            AlertKind::StaleStats => "stale_stats",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        AutovacuumEntry, BloatSample, PartitionSlice, StaleStatEntry, StorageEntry, TopQueryEntry,
        UnusedIndexEntry,
    };
    use chrono::Utc;

    #[test]
    fn storage_metrics_sanitize_labels_and_capture_sizes() {
        let metrics = AppMetrics::new().expect("metrics");
        let entry = StorageEntry {
            relation: "foo bar/baz".into(),
            relkind: "r".into(),
            total_bytes: 1_048_576,
            table_bytes: 524_288,
            index_bytes: 262_144,
            toast_bytes: 262_144,
            dead_tuple_ratio: Some(12.5),
            last_autovacuum: None,
            reltuples: Some(100.0),
            dead_tuples: Some(12_000),
            estimated_bloat_bytes: Some(100_000),
        };

        metrics.set_storage_metrics("cluster", &[entry]);
        let output = metrics.encode().expect("encode");
        let size_line = output.lines().find(|line| {
            line.starts_with("pgmon_pg_relation_size_bytes")
                && line.contains("relation=\"foo_bar_baz\"")
                && (line.contains("1.048576e+06") || line.trim_end().ends_with("1048576"))
        });
        assert!(size_line.is_some(), "relation size missing: {output}");
    }

    #[test]
    fn bloat_sample_metrics_capture_free_space() {
        let metrics = AppMetrics::new().expect("metrics");
        let sample = BloatSample {
            relation: "public.foo".into(),
            table_bytes: 1_000,
            free_bytes: 200,
            free_percent: 20.0,
        };

        metrics.set_bloat_sample_metrics("cluster", &[sample]);
        let output = metrics.encode().expect("encode");
        assert!(output.contains(
            "pgmon_pg_relation_bloat_sample_free_bytes{cluster=\"cluster\",relation=\"public_foo\"} 200"
        ));
        assert!(output.contains(
            "pgmon_pg_relation_bloat_sample_free_percent{cluster=\"cluster\",relation=\"public_foo\"} 20"
        ));
    }

    #[test]
    fn stale_stats_metrics_record_entries() {
        let metrics = AppMetrics::new().expect("metrics");
        let entry = StaleStatEntry {
            relation: "public.foo".into(),
            last_analyze: None,
            last_autoanalyze: None,
            hours_since_analyze: Some(36.0),
            n_live_tup: 1_000,
        };

        metrics.set_stale_stats_metrics("cluster", &[entry]);
        let output = metrics.encode().expect("encode");
        assert!(output.contains("pgmon_pg_table_stats_stale_seconds"));
    }

    #[test]
    fn statement_metrics_export_top_queries() {
        let metrics = AppMetrics::new().expect("metrics");
        let entry = TopQueryEntry {
            queryid: 123,
            calls: 42,
            total_time_seconds: 12.5,
            mean_time_ms: 297.0,
            shared_blks_read: 900,
        };

        metrics.set_statement_metrics("cluster", &[entry]);
        let output = metrics.encode().expect("encode");
        assert!(
            output.contains("pgmon_pg_stmt_calls_total{cluster=\"cluster\",queryid=\"123\"} 42"),
            "statement calls missing: {output}"
        );
        assert!(
            output.contains(
                "pgmon_pg_stmt_time_seconds_total{cluster=\"cluster\",queryid=\"123\"} 12.5"
            ),
            "statement time missing: {output}"
        );
        assert!(
            output.contains(
                "pgmon_pg_stmt_shared_blks_read_total{cluster=\"cluster\",queryid=\"123\"} 900"
            ),
            "statement shared reads missing: {output}"
        );
    }

    #[test]
    fn partition_metrics_emit_gap_flags() {
        let metrics = AppMetrics::new().expect("metrics");
        let now = Utc::now();
        let slice = PartitionSlice {
            parent: "public.orders".into(),
            child_count: 4,
            oldest_partition: None,
            newest_partition: Some(now - chrono::Duration::days(1)),
            latest_partition_upper: Some(now - chrono::Duration::hours(2)),
            latest_partition_name: Some("orders_202403".into()),
            next_expected_partition: Some(now + chrono::Duration::days(5)),
            missing_future_partition: true,
            future_gap_seconds: Some(7_200),
        };

        metrics.set_partition_metrics("cluster", &[slice]);
        let output = metrics.encode().expect("encode");
        assert!(
            output.contains(
                "pgmon_pg_partition_missing_future{cluster=\"cluster\",parent=\"public_orders\"} 1"
            ),
            "missing future flag absent: {output}"
        );
        assert!(
            output.contains(
                "pgmon_pg_partition_future_gap_seconds{cluster=\"cluster\",parent=\"public_orders\"} 7200"
            ),
            "gap seconds absent: {output}"
        );
    }

    #[test]
    fn unused_index_metrics_capture_bytes() {
        let metrics = AppMetrics::new().expect("metrics");
        let entry = UnusedIndexEntry {
            relation: "public.accounts".into(),
            index: "accounts_idx".into(),
            bytes: 200_000_000,
        };

        metrics.set_unused_index_metrics("cluster", &[entry]);
        let output = metrics.encode().expect("encode");
        assert!(
            output.contains("pgmon_pg_unused_index_bytes"),
            "metric absent: {output}"
        );
        assert!(
            output.contains("cluster=\"cluster\"")
                && output.contains("relation=\"public_accounts\"")
                && output.contains("index=\"accounts_idx\""),
            "labels missing: {output}"
        );
        assert!(
            output.contains(" 2e+08") || output.contains(" 200000000"),
            "value missing: {output}"
        );
    }

    #[test]
    fn autovacuum_metrics_emit_percentages() {
        let metrics = AppMetrics::new().expect("metrics");
        let entry = AutovacuumEntry {
            relation: "public.foo".into(),
            n_live_tup: 500_000,
            n_dead_tup: 200_000,
            last_vacuum: None,
            last_autovacuum: Some(Utc::now() - chrono::Duration::minutes(30)),
            last_analyze: None,
            last_autoanalyze: Some(Utc::now() - chrono::Duration::minutes(60)),
        };

        metrics.set_autovacuum_metrics("cluster", &[entry]);
        let output = metrics.encode().expect("encode");
        let pct_line = output
            .lines()
            .find(|line| {
                line.starts_with("pgmon_pg_table_pct_dead")
                    && line.contains("relation=\"public_foo\"")
            })
            .expect("pct dead line");
        let pct_value: f64 = pct_line
            .split_whitespace()
            .last()
            .expect("pct value")
            .parse()
            .expect("pct parse");
        assert!((pct_value - 28.57142857142857).abs() < 1e-6);

        let backlog_line = output.lines().find(|line| {
            line.starts_with("pgmon_pg_dead_tuple_backlog_alert")
                && line.contains("relation=\"public_foo\"")
                && line.contains("severity=\"warn\"")
                && line.trim_end().ends_with(" 1")
        });
        assert!(backlog_line.is_some(), "backlog gauge missing: {output}");
    }

    #[test]
    fn alert_counter_records_severities() {
        let metrics = AppMetrics::new().expect("metrics");
        metrics.inc_alert("cluster", AlertKind::Connections, AlertSeverity::Warn);
        metrics.inc_alert("cluster", AlertKind::Connections, AlertSeverity::Warn);
        metrics.inc_alert("cluster", AlertKind::Connections, AlertSeverity::Crit);

        let output = metrics.encode().expect("encode");
        let warn_line = output.lines().find(|line| {
            line.starts_with("pgmon_alerts_total")
                && line.contains("kind=\"connections\"")
                && line.contains("severity=\"warn\"")
                && line.trim_end().ends_with(" 2")
        });
        let crit_line = output.lines().find(|line| {
            line.starts_with("pgmon_alerts_total")
                && line.contains("kind=\"connections\"")
                && line.contains("severity=\"crit\"")
                && line.trim_end().ends_with(" 1")
        });
        assert!(warn_line.is_some(), "alerts warn missing: {output}");
        assert!(crit_line.is_some(), "alerts crit missing: {output}");
    }

    #[test]
    fn dead_tuple_alert_thresholds() {
        assert!(matches!(
            dead_tuple_alert(25.0, 150_000),
            Some(AlertSeverity::Warn)
        ));
        assert!(matches!(
            dead_tuple_alert(45.0, 150_000),
            Some(AlertSeverity::Crit)
        ));
        assert!(dead_tuple_alert(10.0, 150_000).is_none());
        assert!(dead_tuple_alert(25.0, 50_000).is_none());
    }
}
