use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::Row;
use tracing::{instrument, warn};

use crate::app::AppContext;
use crate::metrics::{AlertKind, AlertSeverity, dead_tuple_alert};
use crate::poller::util::{is_missing_pg_stat_statements, is_missing_relation};
use crate::state::{AutovacuumEntry, TopQueryEntry, WorkloadSample};

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

    let sample = WorkloadSample {
        collected_at: Utc::now(),
        total_xacts: total_xacts as f64,
        total_calls,
        total_time_ms,
        wal_bytes,
        checkpoints_timed: Some(checkpoints_timed),
        checkpoints_requested: Some(checkpoints_requested),
    };

    if let Some(summary) = ctx.state.record_workload_sample(sample).await {
        ctx.state
            .update_overview_with(|overview| {
                overview.tps = summary.tps;
                overview.qps = summary.qps;
                overview.mean_latency_ms = summary.mean_latency_ms;
                overview.wal_bytes_per_second = summary.wal_bytes_per_second;
                overview.checkpoints_timed = summary.checkpoints_timed_total;
                overview.checkpoints_requested = summary.checkpoints_requested_total;
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
        );
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
            overview.open_alerts.extend(warn_alerts.clone());
            overview.open_crit_alerts.extend(crit_alerts.clone());
        })
        .await;
    Ok(())
}
