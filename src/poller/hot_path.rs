use anyhow::Result;
use chrono::Utc;
use sqlx::Row;
use tracing::instrument;

use crate::app::AppContext;
use crate::metrics::{AlertKind, AlertSeverity};
use crate::poller::util::to_optional_positive;

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
            overview.longest_transaction_seconds = longest_tx;
            overview.longest_blocked_seconds = longest_blocked;
            overview.open_alerts = open_alerts.clone();
            overview.open_alerts.extend(replication_alerts);
            overview.open_crit_alerts = open_crit_alerts.clone();
            overview.open_crit_alerts.extend(replication_crit_alerts);
        })
        .await;

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
