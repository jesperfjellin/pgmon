use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::Row;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{instrument, warn};

use crate::app::AppContext;
use crate::metrics::{AlertKind, AlertSeverity, dead_tuple_alert};
use crate::poller::util::{is_missing_column, is_missing_pg_stat_statements, is_missing_relation};
use crate::state::{AutovacuumEntry, TopQueryEntry, WorkloadSample, WorkloadSummary};

static PG_STAT_MONITOR_MISSING: AtomicBool = AtomicBool::new(false);

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

    let (checkpoints_timed, checkpoints_requested) = fetch_checkpoint_counters(&ctx).await?;

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

    let sample = WorkloadSample {
        collected_at: Utc::now(),
        total_xacts: total_xacts as f64,
        total_calls,
        total_time_ms,
        wal_bytes,
        temp_bytes,
        checkpoints_timed: Some(checkpoints_timed),
        checkpoints_requested: Some(checkpoints_requested),
    };

    if let Some(mut summary) = ctx.state.record_workload_sample(sample).await {
        let latency_p95_ms = fetch_latency_p95(ctx).await;
        summary.latency_p95_ms = latency_p95_ms;

        ctx.state
            .update_overview_with(|overview| {
                overview.tps = summary.tps;
                overview.qps = summary.qps;
                overview.mean_latency_ms = summary.mean_latency_ms;
                overview.latency_p95_ms = latency_p95_ms;
                overview.wal_bytes_per_second = summary.wal_bytes_per_second;
                overview.checkpoints_timed = summary.checkpoints_timed_total;
                overview.checkpoints_requested = summary.checkpoints_requested_total;
                overview.checkpoint_requested_ratio = summary.checkpoint_requested_ratio;
                overview.checkpoint_mean_interval_seconds =
                    summary.checkpoint_mean_interval_seconds;
                overview.temp_bytes_per_second = summary.temp_bytes_per_second;
            })
            .await;

        ctx.metrics.set_workload_metrics(
            ctx.cluster_name(),
            summary.tps,
            summary.qps,
            summary.mean_latency_ms,
            summary.wal_bytes_total,
            summary.wal_bytes_per_second,
            summary.checkpoints_timed_total,
            summary.checkpoints_requested_total,
            summary.checkpoint_requested_ratio,
            summary.checkpoint_mean_interval_seconds,
            summary.temp_bytes_per_second,
            latency_p95_ms,
        );

        emit_wal_temp_alerts(ctx, &summary).await;
        emit_checkpoint_alerts(ctx, &summary).await;
    }

    Ok(())
}

async fn update_top_queries(ctx: &AppContext) -> Result<()> {
    let limit = ctx.config.limits.top_queries as i64;
    let rows = sqlx::query(
        r#"
        SELECT
            queryid,
            calls,
            total_exec_time AS total_time,
            mean_exec_time AS mean_time,
            shared_blks_read
        FROM pg_stat_statements
        ORDER BY total_time DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(&ctx.pool)
    .await;

    match rows {
        Ok(rows) => {
            let mut entries = Vec::with_capacity(rows.len());
            for row in rows {
                let queryid: i64 = row.try_get("queryid")?;
                let calls: i64 = row.try_get("calls")?;
                let total_time_ms: f64 = row.try_get("total_time")?;
                let mean_time_ms: f64 = row.try_get("mean_time")?;
                let shared_blks_read: i64 = row.try_get("shared_blks_read")?;

                entries.push(TopQueryEntry {
                    queryid,
                    calls,
                    total_time_seconds: total_time_ms / 1_000.0,
                    mean_time_ms,
                    shared_blks_read,
                });
            }
            ctx.metrics
                .set_statement_metrics(ctx.cluster_name(), &entries);
            ctx.state.update_top_queries(entries).await;
        }
        Err(err) => {
            if let sqlx::Error::Database(db_err) = &err {
                if is_missing_pg_stat_statements(&err) {
                    warn!(
                        error = %db_err,
                        "pg_stat_statements unavailable; top query export disabled"
                    );
                    ctx.state.update_top_queries(Vec::new()).await;
                    ctx.metrics.set_statement_metrics(ctx.cluster_name(), &[]);
                    return Ok(());
                }
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
        format!("{:.0}s", sec)
    }
}

async fn fetch_latency_p95(ctx: &AppContext) -> Option<f64> {
    match sqlx::query(
        r#"
        SELECT MAX(resp_percentile_95) AS p95
        FROM pg_stat_monitor
        "#,
    )
    .fetch_one(&ctx.pool)
    .await
    {
        Ok(row) => match row.try_get::<Option<f64>, _>("p95") {
            Ok(value) => value,
            Err(err) => {
                warn!(error = ?err, "failed to read resp_percentile_95 from pg_stat_monitor");
                None
            }
        },
        Err(sqlx::Error::Database(db_err)) => {
            let error = db_err.as_ref();
            if is_missing_relation(error) || is_missing_column(error) {
                if !PG_STAT_MONITOR_MISSING.swap(true, Ordering::Relaxed) {
                    warn!("pg_stat_monitor unavailable; latency p95 reporting disabled");
                }
                None
            } else {
                warn!(error = %error, "pg_stat_monitor query failed");
                None
            }
        }
        Err(err) => {
            warn!(error = ?err, "failed to query pg_stat_monitor for latency p95");
            None
        }
    }
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

                let message = format!(
                    "Dead tuple backlog {} ({:.1}% ~{} dead)",
                    relation, ratio, n_dead_tup
                );

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
                    "Autovac starvation critical {} ({}h since run)",
                    relation,
                    hours.floor()
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
                    "Autovac starvation {} ({}h since run)",
                    relation,
                    hours.floor()
                );
                starvation_warn.push(message);
                ctx.metrics.inc_alert(
                    ctx.cluster_name(),
                    AlertKind::AutovacuumStarvation,
                    AlertSeverity::Warn,
                );
            }
        } else if thresholds.autovac_starvation_crit_hours > 0 {
            let message = format!("Autovac starvation critical {} (never vacuumed)", relation);
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
