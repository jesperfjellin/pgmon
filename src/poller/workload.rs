use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::Row;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{instrument, warn};

use crate::app::AppContext;
use crate::metrics::{AlertKind, AlertSeverity, dead_tuple_alert};
use crate::poller::util::{is_missing_pg_stat_statements, is_missing_relation};
use crate::state::{AutovacuumEntry, TopQueryEntry, WorkloadSample, WorkloadSummary};

static PG_STAT_MONITOR_MISSING: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
struct LatencyPercentiles {
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
}

struct HistogramConfig {
    min: f64,
    max: f64,
    buckets: usize,
}

#[instrument(skip_all)]
pub async fn run(ctx: &AppContext) -> Result<()> {
    update_workload_overview(ctx).await?;
    update_top_queries(ctx).await?;
    update_autovacuum(ctx).await?;
    Ok(())
}

async fn update_workload_overview(ctx: &AppContext) -> Result<()> {
    let total_xacts_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(xact_commit + xact_rollback), 0)::bigint AS total_xacts
        FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1')
        "#,
    )
    .fetch_one(&ctx.pool)
    .await?;

    let total_xacts: i64 = total_xacts_row.try_get("total_xacts")?;

    let stmt_totals = sqlx::query(
        r#"
        SELECT SUM(calls)::double precision AS calls, SUM(total_exec_time) AS total_time
        FROM pg_stat_statements
        "#,
    )
    .fetch_one(&ctx.pool)
    .await;

    let (total_calls, total_time_ms) = match stmt_totals {
        Ok(row) => {
            let calls: Option<f64> = row.try_get("calls")?;
            let total_time: Option<f64> = row.try_get("total_time")?;
            (calls, total_time)
        }
        Err(err) => {
            if is_missing_pg_stat_statements(&err) {
                warn!("pg_stat_statements unavailable; workload latency metrics disabled");
                (None, None)
            } else {
                return Err(err.into());
            }
        }
    };

    let wal_bytes = match sqlx::query("SELECT wal_bytes::bigint AS wal_bytes FROM pg_stat_wal")
        .fetch_one(&ctx.pool)
        .await
    {
        Ok(row) => Some(row.try_get::<i64, _>("wal_bytes")?),
        Err(err) => {
            if let sqlx::Error::Database(db_err) = &err {
                if is_missing_relation(db_err.as_ref()) {
                    warn!("pg_stat_wal unavailable; WAL metrics disabled");
                    None
                } else {
                    return Err(err.into());
                }
            } else {
                return Err(err.into());
            }
        }
    };

    let (checkpoints_timed, checkpoints_requested) = fetch_checkpoint_counters(ctx).await?;

    let temp_bytes_row = sqlx::query(
        r#"
        SELECT COALESCE(SUM(temp_bytes), 0)::bigint AS temp_bytes
        FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1')
        "#,
    )
    .fetch_one(&ctx.pool)
    .await?;
    let temp_bytes = Some(temp_bytes_row.try_get::<i64, _>("temp_bytes")?);

    let collected_at = Utc::now();
    let sample = WorkloadSample {
        collected_at,
        total_xacts: total_xacts as f64,
        total_calls,
        total_time_ms,
        wal_bytes,
        temp_bytes,
        checkpoints_timed: Some(checkpoints_timed),
        checkpoints_requested: Some(checkpoints_requested),
    };

    if let Some(mut summary) = ctx.state.record_workload_sample(sample).await {
        tracing::debug!(
            tps = summary.tps,
            qps = summary.qps,
            mean_latency_ms = summary.mean_latency_ms,
            "workload summary delta computed"
        );
        let latency = fetch_latency_percentiles(ctx).await;
        summary.latency_p95_ms = latency.p95_ms;
        summary.latency_p99_ms = latency.p99_ms;

        // KPI numeric fields have been removed from OverviewSnapshot; history is recorded directly via state.record_workload_sample and metrics exported separately.

        // Capture post-update overview snapshot into history buffers.
        // Record connection & blocked history via hot_path loop only; workload loop no longer pushes KPI snapshot values.

        // Record workload-derived KPI series directly into MetricHistory so the
        // /history/overview endpoint exposes them. Previously removed when
        // snapshot numeric fields were pruned; without these pushes the frontend
        // sees empty series (Connections show because hot_path still records).
        if summary.tps.is_some()
            || summary.qps.is_some()
            || summary.mean_latency_ms.is_some()
            || summary.latency_p95_ms.is_some()
            || summary.latency_p99_ms.is_some()
        {
            let ts = collected_at;
            ctx.state
                .replace_metric_history(|mh| {
                    if let Some(v) = summary.tps {
                        mh.record_tps(ts, v);
                    }
                    if let Some(v) = summary.qps {
                        mh.record_qps(ts, v);
                    }
                    if let Some(v) = summary.mean_latency_ms {
                        mh.record_mean_latency(ts, v);
                    }
                    if let Some(v) = summary.latency_p95_ms {
                        mh.record_latency_p95(ts, v);
                    }
                    if let Some(v) = summary.latency_p99_ms {
                        mh.record_latency_p99(ts, v);
                    }
                    tracing::debug!(
                        ts = ts.timestamp(),
                        tps_points = mh.tps.len(),
                        qps_points = mh.qps.len(),
                        mean_latency_points = mh.mean_latency_ms.len(),
                        p95_points = mh.latency_p95_ms.len(),
                        p99_points = mh.latency_p99_ms.len(),
                        "recorded workload history points"
                    );
                })
                .await;

            // Ensure persistence sees newly created workload points promptly. We rely on periodic
            // flush loop, but if it drifts, this guarantees durability within a few ms.
            tokio::spawn({
                let state_clone = ctx.state.clone();
                async move {
                    if let Some(cfg) = crate::persistence::PersistenceConfig::from_env() {
                        if let Err(err) = crate::persistence::flush_once(&cfg, &state_clone).await {
                            tracing::warn!(error=?err, "on-demand workload flush failed");
                        } else {
                            tracing::debug!("on-demand workload flush complete");
                        }
                    }
                }
            });
        }

        ctx.metrics
            .set_workload_metrics(ctx.cluster_name(), &summary);

        emit_wal_temp_alerts(ctx, &summary).await;
        emit_checkpoint_alerts(ctx, &summary).await;
    }

    Ok(())
}

async fn update_top_queries(ctx: &AppContext) -> Result<()> {
    let limit = ctx.config.limits.top_queries as i64;
    let redact_sql = ctx.config.security.redact_sql_text;

    // Conditionally include query text based on redact_sql_text setting
    let sql = if redact_sql {
        r#"
        SELECT
            queryid,
            calls,
            total_exec_time AS total_time,
            mean_exec_time AS mean_time,
            shared_blks_read,
            shared_blks_hit,
            NULL::text AS query
        FROM pg_stat_statements
        ORDER BY total_time DESC
        LIMIT $1
        "#
    } else {
        r#"
        SELECT
            queryid,
            calls,
            total_exec_time AS total_time,
            mean_exec_time AS mean_time,
            shared_blks_read,
            shared_blks_hit,
            query
        FROM pg_stat_statements
        ORDER BY total_time DESC
        LIMIT $1
        "#
    };

    let rows = sqlx::query(sql).bind(limit).fetch_all(&ctx.pool).await;

    match rows {
        Ok(rows) => {
            let mut entries = Vec::with_capacity(rows.len());
            for row in rows {
                let queryid: i64 = row.try_get("queryid")?;
                let calls: i64 = row.try_get("calls")?;
                let total_time_ms: f64 = row.try_get("total_time")?;
                let mean_time_ms: f64 = row.try_get("mean_time")?;
                let shared_blks_read: i64 = row.try_get("shared_blks_read")?;
                let shared_blks_hit: i64 = row.try_get("shared_blks_hit")?;
                let query_text: Option<String> = row.try_get("query")?;

                // Calculate cache hit ratio
                let total_blks = shared_blks_read + shared_blks_hit;
                let cache_hit_ratio = if total_blks > 0 {
                    shared_blks_hit as f64 / total_blks as f64
                } else {
                    1.0 // No I/O = perfect cache hit
                };

                // Extract table names from query text
                let table_names = query_text.as_ref().and_then(|q| extract_table_names(q));

                entries.push(TopQueryEntry {
                    queryid,
                    calls,
                    total_time_seconds: total_time_ms / 1_000.0,
                    mean_time_ms,
                    shared_blks_read,
                    shared_blks_hit,
                    cache_hit_ratio,
                    query_text,
                    table_names,
                });
            }
            ctx.metrics
                .set_statement_metrics(ctx.cluster_name(), &entries);
            ctx.state.update_top_queries(entries).await;
        }
        Err(err) => {
            if let sqlx::Error::Database(db_err) = &err
                && is_missing_pg_stat_statements(&err)
            {
                warn!(
                    error = %db_err,
                    "pg_stat_statements unavailable; top query export disabled"
                );
                ctx.state.update_top_queries(Vec::new()).await;
                ctx.metrics.set_statement_metrics(ctx.cluster_name(), &[]);
                return Ok(());
            }
            return Err(err.into());
        }
    }

    Ok(())
}

async fn fetch_checkpoint_counters(ctx: &AppContext) -> Result<(i64, i64)> {
    let row = sqlx::query(
        r#"
        SELECT num_timed AS checkpoints_timed,
               num_requested AS checkpoints_requested
        FROM pg_stat_checkpointer
        "#,
    )
    .fetch_one(&ctx.pool)
    .await?;

    Ok((
        row.try_get::<i64, _>("checkpoints_timed")?,
        row.try_get::<i64, _>("checkpoints_requested")?,
    ))
}

async fn emit_wal_temp_alerts(ctx: &AppContext, summary: &WorkloadSummary) {
    let thresholds = &ctx.config.alerts;
    let wal_warn = thresholds.wal_surge_warn_bytes_per_sec as f64;
    let wal_crit = thresholds.wal_surge_crit_bytes_per_sec as f64;
    let temp_warn = thresholds.temp_surge_warn_bytes_per_sec as f64;
    let temp_crit = thresholds.temp_surge_crit_bytes_per_sec as f64;

    let mut warn = Vec::new();
    let mut crit = Vec::new();

    if let Some(rate) = summary.wal_bytes_per_second {
        if rate >= wal_crit {
            let message = format!("WAL surge critical: {}", format_rate(rate));
            crit.push(message.clone());
            ctx.metrics
                .inc_alert(ctx.cluster_name(), AlertKind::WalSurge, AlertSeverity::Crit);
        } else if rate >= wal_warn {
            let message = format!("WAL surge: {}", format_rate(rate));
            warn.push(message.clone());
            ctx.metrics
                .inc_alert(ctx.cluster_name(), AlertKind::WalSurge, AlertSeverity::Warn);
        }
    }

    if let Some(rate) = summary.temp_bytes_per_second {
        if rate >= temp_crit {
            let message = format!("Temp surge critical: {}", format_rate(rate));
            crit.push(message.clone());
            ctx.metrics.inc_alert(
                ctx.cluster_name(),
                AlertKind::TempSurge,
                AlertSeverity::Crit,
            );
        } else if rate >= temp_warn {
            let message = format!("Temp surge: {}", format_rate(rate));
            warn.push(message.clone());
            ctx.metrics.inc_alert(
                ctx.cluster_name(),
                AlertKind::TempSurge,
                AlertSeverity::Warn,
            );
        }
    }

    ctx.state
        .update_overview_with(|overview| {
            overview
                .open_alerts
                .retain(|alert| !alert.starts_with("WAL surge"));
            overview
                .open_alerts
                .retain(|alert| !alert.starts_with("Temp surge"));
            overview
                .open_crit_alerts
                .retain(|alert| !alert.starts_with("WAL surge"));
            overview
                .open_crit_alerts
                .retain(|alert| !alert.starts_with("Temp surge"));
            overview.open_alerts.extend(warn.iter().cloned());
            overview.open_crit_alerts.extend(crit.iter().cloned());
        })
        .await;
}

fn format_rate(bytes_per_second: f64) -> String {
    const UNITS: &[&str] = &["B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s"];
    let mut value = bytes_per_second;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if value >= 10.0 {
        format!("{:.0} {}", value, UNITS[unit])
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

async fn emit_checkpoint_alerts(ctx: &AppContext, summary: &WorkloadSummary) {
    let thresholds = &ctx.config.alerts;
    let ratio_warn = thresholds.checkpoint_ratio_warn;
    let ratio_crit = thresholds.checkpoint_ratio_crit;
    let interval_warn = thresholds.checkpoint_interval_warn_seconds as f64;
    let interval_crit = thresholds.checkpoint_interval_crit_seconds as f64;

    let mut warn = Vec::new();
    let mut crit = Vec::new();

    if let Some(ratio) = summary.checkpoint_requested_ratio {
        if ratio >= ratio_crit {
            let message = format!(
                "Checkpoint thrash critical: requested ratio {:.0}%",
                ratio * 100.0
            );
            crit.push(message);
        } else if ratio >= ratio_warn {
            let message = format!("Checkpoint thrash: requested ratio {:.0}%", ratio * 100.0);
            warn.push(message);
        }
    }

    if let Some(interval) = summary.checkpoint_mean_interval_seconds {
        if interval <= interval_crit {
            let message = format!(
                "Checkpoint thrash critical: mean interval {}",
                format_seconds(interval)
            );
            crit.push(message);
        } else if interval <= interval_warn {
            let message = format!(
                "Checkpoint thrash: mean interval {}",
                format_seconds(interval)
            );
            warn.push(message);
        }
    }

    if warn.is_empty() && crit.is_empty() {
        ctx.state
            .update_overview_with(|overview| {
                overview
                    .open_alerts
                    .retain(|alert| !alert.starts_with("Checkpoint thrash"));
                overview
                    .open_crit_alerts
                    .retain(|alert| !alert.starts_with("Checkpoint thrash"));
            })
            .await;
        return;
    }

    for _ in &warn {
        ctx.metrics.inc_alert(
            ctx.cluster_name(),
            AlertKind::CheckpointThrash,
            AlertSeverity::Warn,
        );
    }
    for _ in &crit {
        ctx.metrics.inc_alert(
            ctx.cluster_name(),
            AlertKind::CheckpointThrash,
            AlertSeverity::Crit,
        );
    }

    ctx.state
        .update_overview_with(|overview| {
            overview
                .open_alerts
                .retain(|alert| !alert.starts_with("Checkpoint thrash"));
            overview
                .open_crit_alerts
                .retain(|alert| !alert.starts_with("Checkpoint thrash"));
            overview.open_alerts.extend(warn.iter().cloned());
            overview.open_crit_alerts.extend(crit.iter().cloned());
        })
        .await;
}

fn format_seconds(sec: f64) -> String {
    if sec >= 3600.0 {
        format!("{:.1}h", sec / 3600.0)
    } else if sec >= 60.0 {
        format!("{:.1}m", sec / 60.0)
    } else {
        format!("{sec:.0}s")
    }
}

async fn fetch_latency_percentiles(ctx: &AppContext) -> LatencyPercentiles {
    match compute_latency_percentiles(ctx).await {
        Ok(Some(percentiles)) => {
            PG_STAT_MONITOR_MISSING.store(false, Ordering::Relaxed);
            percentiles
        }
        Ok(None) => {
            if !PG_STAT_MONITOR_MISSING.swap(true, Ordering::Relaxed) {
                warn!("pg_stat_monitor histogram unavailable; latency p95/p99 disabled");
            }
            LatencyPercentiles::default()
        }
        Err(err) => {
            warn!(
                error = ?err,
                "failed to derive pg_stat_monitor latency percentiles"
            );
            LatencyPercentiles::default()
        }
    }
}

async fn compute_latency_percentiles(ctx: &AppContext) -> Result<Option<LatencyPercentiles>> {
    let Some(config) = load_histogram_config(ctx).await? else {
        return Ok(None);
    };

    let (counts, total) = match aggregate_histogram_counts(ctx, config.buckets).await {
        Ok(result) => result,
        Err(sqlx::Error::Database(db_err)) => {
            if is_missing_relation(db_err.as_ref()) {
                return Ok(None);
            }
            return Err(db_err.into());
        }
        Err(err) => return Err(err.into()),
    };
    if total <= 0.0 {
        return Ok(None);
    }

    let percentiles = derive_percentiles(&config, &counts, total);
    if percentiles.p95_ms.is_some() || percentiles.p99_ms.is_some() {
        Ok(Some(percentiles))
    } else {
        Ok(None)
    }
}

async fn load_histogram_config(ctx: &AppContext) -> Result<Option<HistogramConfig>> {
    let rows = sqlx::query(
        r#"
        SELECT name, setting
        FROM pg_settings
        WHERE name IN (
            'pg_stat_monitor.pgsm_histogram_min',
            'pg_stat_monitor.pgsm_histogram_max',
            'pg_stat_monitor.pgsm_histogram_buckets'
        )
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;

    let mut min = None;
    let mut max = None;
    let mut buckets = None;

    for row in rows {
        let name: String = row.try_get("name")?;
        let setting: String = row.try_get("setting")?;
        match name.as_str() {
            "pg_stat_monitor.pgsm_histogram_min" => {
                if let Ok(value) = setting.parse::<f64>() {
                    min = Some(value);
                }
            }
            "pg_stat_monitor.pgsm_histogram_max" => {
                if let Ok(value) = setting.parse::<f64>() {
                    max = Some(value);
                }
            }
            "pg_stat_monitor.pgsm_histogram_buckets" => {
                if let Ok(value) = setting.parse::<usize>() {
                    buckets = Some(value);
                }
            }
            _ => {}
        }
    }

    match (min, max, buckets) {
        (Some(min), Some(max), Some(buckets)) if buckets > 0 && max > min => {
            Ok(Some(HistogramConfig { min, max, buckets }))
        }
        _ => Ok(None),
    }
}

async fn aggregate_histogram_counts(
    ctx: &AppContext,
    initial_buckets: usize,
) -> sqlx::Result<(Vec<f64>, f64)> {
    let mut counts = vec![0.0; initial_buckets];
    let mut total = 0.0;

    let rows = sqlx::query(
        r#"
        SELECT resp_calls
        FROM pg_stat_monitor
        WHERE resp_calls IS NOT NULL
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;

    for row in rows {
        let resp_calls: Option<Vec<Option<String>>> = row.try_get("resp_calls")?;
        let Some(resp_calls) = resp_calls else {
            continue;
        };

        for (idx, value) in resp_calls.into_iter().enumerate() {
            let Some(value) = value else {
                continue;
            };
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(count) = trimmed.parse::<f64>() {
                if idx >= counts.len() {
                    counts.resize(idx + 1, 0.0);
                }
                counts[idx] += count;
                total += count;
            }
        }
    }

    Ok((counts, total))
}

fn derive_percentiles(config: &HistogramConfig, counts: &[f64], total: f64) -> LatencyPercentiles {
    if total <= 0.0 {
        return LatencyPercentiles::default();
    }

    let bucket_width = (config.max - config.min) / config.buckets as f64;
    if !bucket_width.is_finite() || bucket_width <= 0.0 {
        return LatencyPercentiles::default();
    }

    let mut cumulative = 0.0;
    let mut p95_ms = None;
    let mut p99_ms = None;

    for (idx, count) in counts.iter().enumerate() {
        let count = *count;
        if count <= 0.0 {
            continue;
        }

        cumulative += count;
        let share = cumulative / total;
        let upper_bound = if idx < config.buckets {
            config.min + bucket_width * (idx as f64 + 1.0)
        } else {
            config.max
        };
        // histogram bounds are recorded in microseconds; normalise to milliseconds
        let upper_bound_ms = upper_bound / 1_000.0;

        if p95_ms.is_none() && share >= 0.95 {
            p95_ms = Some(upper_bound_ms);
        }
        if p99_ms.is_none() && share >= 0.99 {
            p99_ms = Some(upper_bound_ms);
            break;
        }
    }

    LatencyPercentiles { p95_ms, p99_ms }
}

async fn update_autovacuum(ctx: &AppContext) -> Result<()> {
    let limit = ctx.config.limits.top_relations as i64;
    let rows = sqlx::query(
        r#"
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
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(&ctx.pool)
    .await?;

    let mut entries = Vec::with_capacity(rows.len());
    let mut warn_alerts = Vec::new();
    let mut crit_alerts = Vec::new();
    let mut starvation_warn = Vec::new();
    let mut starvation_crit = Vec::new();
    for row in rows {
        let relation: String = row.try_get("relation")?;
        let n_live_tup: i64 = row.try_get("n_live_tup")?;
        let n_dead_tup: i64 = row.try_get("n_dead_tup")?;
        let last_vacuum: Option<DateTime<Utc>> = row.try_get("last_vacuum")?;
        let last_autovacuum: Option<DateTime<Utc>> = row.try_get("last_autovacuum")?;
        let last_analyze: Option<DateTime<Utc>> = row.try_get("last_analyze")?;
        let last_autoanalyze: Option<DateTime<Utc>> = row.try_get("last_autoanalyze")?;

        entries.push(AutovacuumEntry {
            relation: relation.clone(),
            n_live_tup,
            n_dead_tup,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
        });

        let total = n_live_tup + n_dead_tup;
        if total > 0 {
            let ratio = (n_dead_tup as f64 / total as f64) * 100.0;
            if let Some(severity) = dead_tuple_alert(ratio, n_dead_tup) {
                ctx.metrics
                    .inc_alert(ctx.cluster_name(), AlertKind::DeadTuples, severity);

                let message =
                    format!("Dead tuple backlog {relation} ({ratio:.1}% ~{n_dead_tup} dead)");

                match severity {
                    AlertSeverity::Warn => warn_alerts.push(message),
                    AlertSeverity::Crit => crit_alerts.push(message),
                }
            }
        }

        let last_maintenance = last_autovacuum.or(last_vacuum);
        let hours_since_maintenance =
            last_maintenance.map(|ts| (Utc::now() - ts).num_seconds().max(0) as f64 / 3600.0);

        let starvation_base = ratio_from_counts(n_dead_tup, n_live_tup);
        let thresholds = &ctx.config.alerts;
        if let Some(hours) = hours_since_maintenance {
            if hours >= thresholds.autovac_starvation_crit_hours as f64 && starvation_base > 0.05 {
                let message = format!(
                    "Autovac starvation critical {relation} ({hours_floor:.0}h since run)",
                    hours_floor = hours.floor()
                );
                starvation_crit.push(message);
                ctx.metrics.inc_alert(
                    ctx.cluster_name(),
                    AlertKind::AutovacuumStarvation,
                    AlertSeverity::Crit,
                );
            } else if hours >= thresholds.autovac_starvation_warn_hours as f64
                && starvation_base > 0.02
            {
                let message = format!(
                    "Autovac starvation {relation} ({hours_floor:.0}h since run)",
                    hours_floor = hours.floor()
                );
                starvation_warn.push(message);
                ctx.metrics.inc_alert(
                    ctx.cluster_name(),
                    AlertKind::AutovacuumStarvation,
                    AlertSeverity::Warn,
                );
            }
        } else if thresholds.autovac_starvation_crit_hours > 0 {
            let message = format!("Autovac starvation critical {relation} (never vacuumed)");
            starvation_crit.push(message);
            ctx.metrics.inc_alert(
                ctx.cluster_name(),
                AlertKind::AutovacuumStarvation,
                AlertSeverity::Crit,
            );
        }
    }

    ctx.metrics
        .set_autovacuum_metrics(ctx.cluster_name(), &entries);
    ctx.state.update_autovacuum(entries).await;

    ctx.state
        .update_overview_with(|overview| {
            overview
                .open_alerts
                .retain(|alert| !alert.starts_with("Dead tuple backlog"));
            overview
                .open_crit_alerts
                .retain(|alert| !alert.starts_with("Dead tuple backlog"));
            overview
                .open_alerts
                .retain(|alert| !alert.starts_with("Autovac starvation"));
            overview
                .open_crit_alerts
                .retain(|alert| !alert.starts_with("Autovac starvation"));
            overview.open_alerts.extend(warn_alerts.clone());
            overview.open_crit_alerts.extend(crit_alerts.clone());
            overview.open_alerts.extend(starvation_warn.clone());
            overview.open_crit_alerts.extend(starvation_crit.clone());
        })
        .await;
    Ok(())
}

fn ratio_from_counts(dead: i64, live: i64) -> f64 {
    let total = dead + live;
    if total <= 0 {
        0.0
    } else {
        dead as f64 / total as f64
    }
}

/// Extract table names from SQL query text.
/// Looks for tables in INSERT, UPDATE, DELETE, FROM, and JOIN clauses.
fn extract_table_names(query: &str) -> Option<String> {
    use regex::Regex;

    // Match table names after DML keywords (INSERT INTO, UPDATE, DELETE FROM)
    // and query keywords (FROM, JOIN)
    // Handles schema.table and basic table names
    // For UPDATE: must NOT be followed by SET (to avoid matching "DO UPDATE SET" in ON CONFLICT)
    let re = Regex::new(
        r"(?i)\b(?:INSERT\s+INTO|DELETE\s+FROM|FROM|JOIN)\s+(?:ONLY\s+)?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)"
    ).ok()?;

    // Separate pattern for UPDATE to ensure it's followed by a table name, not SET
    let update_re = Regex::new(
        r"(?i)\bUPDATE\s+(?:ONLY\s+)?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s+SET",
    )
    .ok()?;

    let mut tables = Vec::new();

    // Extract from INSERT, DELETE, FROM, JOIN
    for cap in re.captures_iter(query) {
        if let Some(table) = cap.get(1) {
            let table_str = table.as_str().trim();
            if !table_str.is_empty() && !tables.contains(&table_str) {
                tables.push(table_str);
            }
        }
    }

    // Extract from UPDATE ... SET
    for cap in update_re.captures_iter(query) {
        if let Some(table) = cap.get(1) {
            let table_str = table.as_str().trim();
            if !table_str.is_empty() && !tables.contains(&table_str) {
                tables.push(table_str);
            }
        }
    }

    if tables.is_empty() {
        return None;
    }

    // Format: show up to 3 tables, then "..."
    if tables.len() <= 3 {
        Some(tables.join(", "))
    } else {
        Some(format!(
            "{}, {}... (+{})",
            tables[0],
            tables[1],
            tables.len() - 2
        ))
    }
}
