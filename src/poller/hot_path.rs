use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::Row;
use tracing::instrument;

use crate::app::AppContext;
use std::cmp::Ordering;

use crate::metrics::{AlertKind, AlertSeverity};
use crate::poller::util::to_optional_positive;
use crate::state::BlockingEvent;

const BLOCKING_EVENTS_SQL: &str = r#"
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
    blocked_usename,
    blocked_xact_start,
    blocked_wait_seconds,
    LEFT(blocked_query, 256) AS blocked_query,
    blocker_pid,
    blocker_usename,
    blocker_state,
    blocker_waiting,
    LEFT(blocker_query, 256) AS blocker_query
FROM blocking
ORDER BY blocked_pid, blocker_pid, blocked_wait_seconds DESC NULLS LAST
LIMIT $1
"#;

#[instrument(skip_all)]
pub async fn run(ctx: &AppContext) -> Result<()> {
    let connections_row = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*)::bigint FROM pg_stat_activity) AS connections,
            current_setting('max_connections')::bigint AS max_connections
        "#,
    )
    .fetch_one(&ctx.pool)
    .await?;

    let connections: i64 = connections_row.try_get("connections")?;
    let max_connections: i64 = connections_row.try_get("max_connections")?;

    let blocked_row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::bigint AS blocked_sessions,
            MAX(EXTRACT(EPOCH FROM now() - act.query_start)::double precision) AS longest_blocked_seconds
        FROM pg_locks l
        JOIN pg_stat_activity act ON act.pid = l.pid
        WHERE NOT l.granted
        "#,
    )
    .fetch_one(&ctx.pool)
    .await?;

    let blocked_sessions: i64 = blocked_row.try_get("blocked_sessions")?;
    let longest_blocked_seconds: Option<f64> = blocked_row.try_get("longest_blocked_seconds")?;

    let longest_tx_row = sqlx::query(
        r#"
        SELECT MAX(EXTRACT(EPOCH FROM now() - xact_start)::double precision) AS longest_tx
        FROM pg_stat_activity
        WHERE xact_start IS NOT NULL
        "#,
    )
    .fetch_one(&ctx.pool)
    .await?;

    let longest_transaction_seconds: Option<f64> = longest_tx_row.try_get("longest_tx")?;

    let longest_tx = to_optional_positive(longest_transaction_seconds);
    let longest_blocked = to_optional_positive(longest_blocked_seconds);

    let blocking_rows = sqlx::query(BLOCKING_EVENTS_SQL)
        .bind(50_i64)
        .fetch_all(&ctx.pool)
        .await?;

    let redact_sql = ctx.config.security.redact_sql_text;
    let mut blocking_events = Vec::with_capacity(blocking_rows.len());
    for row in blocking_rows {
        let blocked_pid: i32 = row.try_get("blocked_pid")?;
        let blocked_usename: Option<String> = row.try_get("blocked_usename")?;
        let blocked_xact_start: Option<DateTime<Utc>> = row.try_get("blocked_xact_start")?;
        let blocked_wait_seconds: Option<f64> = row.try_get("blocked_wait_seconds")?;
        let blocked_query_raw: Option<String> = row.try_get("blocked_query")?;
        let blocker_pid: i32 = row.try_get("blocker_pid")?;
        let blocker_usename: Option<String> = row.try_get("blocker_usename")?;
        let blocker_state: Option<String> = row.try_get("blocker_state")?;
        let blocker_waiting: bool = row.try_get("blocker_waiting")?;
        let blocker_query_raw: Option<String> = row.try_get("blocker_query")?;

        blocking_events.push(BlockingEvent {
            blocked_pid,
            blocked_usename,
            blocked_transaction_start: blocked_xact_start,
            blocked_wait_seconds,
            blocked_query: normalize_query(blocked_query_raw, redact_sql),
            blocker_pid,
            blocker_usename,
            blocker_state,
            blocker_waiting,
            blocker_query: normalize_query(blocker_query_raw, redact_sql),
        });
    }

    blocking_events.sort_by(
        |a, b| match (b.blocked_wait_seconds, a.blocked_wait_seconds) {
            (Some(bw), Some(aw)) => bw.partial_cmp(&aw).unwrap_or(Ordering::Equal),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        },
    );

    let autovac_rows = sqlx::query(
        r#"
        SELECT COALESCE(phase, 'unknown') AS phase, COUNT(*)::bigint AS jobs
        FROM pg_stat_progress_vacuum
        GROUP BY phase
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;

    let mut autovac_jobs = Vec::with_capacity(autovac_rows.len());
    for row in autovac_rows {
        let phase: String = row.try_get("phase")?;
        let jobs: i64 = row.try_get("jobs")?;
        autovac_jobs.push((phase, jobs));
    }
    ctx.metrics
        .set_autovacuum_jobs(ctx.cluster_name(), &autovac_jobs);

    let temp_rows = sqlx::query(
        r#"
        SELECT datname, temp_files::bigint AS temp_files, temp_bytes::bigint AS temp_bytes
        FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1')
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;

    let mut temp_stats = Vec::with_capacity(temp_rows.len());
    for row in temp_rows {
        let db: String = row.try_get("datname")?;
        let temp_files: i64 = row.try_get("temp_files")?;
        let temp_bytes: i64 = row.try_get("temp_bytes")?;
        temp_stats.push((db, temp_files, temp_bytes));
    }
    ctx.metrics
        .set_temp_metrics(ctx.cluster_name(), &temp_stats);

    let mut open_alerts = Vec::new();
    let mut open_crit_alerts = Vec::new();
    let alerts_cfg = &ctx.config.alerts;

    if max_connections > 0 {
        let ratio = connections as f64 / max_connections as f64;
        if ratio >= alerts_cfg.connections_warn {
            open_alerts.push(format!(
                "Connections at {:.0}% ({}/{})",
                ratio * 100.0,
                connections,
                max_connections
            ));
            ctx.metrics.inc_alert(
                ctx.cluster_name(),
                AlertKind::Connections,
                AlertSeverity::Warn,
            );
        }
        if let Some(crit) = alerts_cfg.connections_crit {
            if ratio >= crit {
                open_crit_alerts.push(format!(
                    "Connections critical {:.0}% ({}/{})",
                    ratio * 100.0,
                    connections,
                    max_connections
                ));
                ctx.metrics.inc_alert(
                    ctx.cluster_name(),
                    AlertKind::Connections,
                    AlertSeverity::Crit,
                );
            }
        }
    }

    if let Some(tx) = longest_tx {
        if tx >= alerts_cfg.long_txn_warn_s as f64 {
            open_alerts.push(format!("Longest transaction {:.0}s", tx));
            ctx.metrics.inc_alert(
                ctx.cluster_name(),
                AlertKind::LongTransaction,
                AlertSeverity::Warn,
            );
        }
        if tx >= alerts_cfg.long_txn_crit_s as f64 {
            open_crit_alerts.push(format!("Longest transaction critical {:.0}s", tx));
            ctx.metrics.inc_alert(
                ctx.cluster_name(),
                AlertKind::LongTransaction,
                AlertSeverity::Crit,
            );
        }
    }

    if let Some(blocked) = longest_blocked {
        if blocked >= alerts_cfg.long_txn_warn_s as f64 {
            open_alerts.push(format!("Longest blocked session {:.0}s", blocked));
            ctx.metrics.inc_alert(
                ctx.cluster_name(),
                AlertKind::BlockedSession,
                AlertSeverity::Warn,
            );
        }
        if blocked >= alerts_cfg.long_txn_crit_s as f64 {
            open_crit_alerts.push(format!("Longest blocked session critical {:.0}s", blocked));
            ctx.metrics.inc_alert(
                ctx.cluster_name(),
                AlertKind::BlockedSession,
                AlertSeverity::Crit,
            );
        }
    }

    let blocking_for_state = blocking_events.clone();
    ctx.state
        .update_overview_with(|overview| {
            let replication_alerts: Vec<String> = overview
                .open_alerts
                .iter()
                .filter(|alert| alert.starts_with("Replication lag"))
                .cloned()
                .collect();
            let replication_crit_alerts: Vec<String> = overview
                .open_crit_alerts
                .iter()
                .filter(|alert| alert.starts_with("Replication lag"))
                .cloned()
                .collect();

            overview.cluster = ctx.cluster_name().to_string();
            overview.generated_at = Some(Utc::now());
            overview.connections = connections;
            overview.max_connections = max_connections;
            overview.blocked_sessions = blocked_sessions;
            overview.blocking_events = blocking_for_state.clone();
            overview.longest_transaction_seconds = longest_tx;
            overview.longest_blocked_seconds = longest_blocked;
            overview.open_alerts = open_alerts.clone();
            overview.open_alerts.extend(replication_alerts);
            overview.open_crit_alerts = open_crit_alerts.clone();
            overview.open_crit_alerts.extend(replication_crit_alerts);
        })
        .await;

    // After overview changes, record high-resolution history points.
    let latest = ctx.state.get_snapshots().await.overview; // clone snapshot
    ctx.state.record_history_points(&latest).await;

    ctx.metrics.set_overview_metrics(
        ctx.cluster_name(),
        connections,
        max_connections,
        blocked_sessions,
        longest_tx,
        longest_blocked,
    );

    Ok(())
}

fn normalize_query(value: Option<String>, redact: bool) -> Option<String> {
    if redact {
        None
    } else {
        value
            .map(|q| q.trim().to_string())
            .filter(|q| !q.is_empty())
    }
}
