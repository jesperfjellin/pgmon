use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use sqlx::Row;
use tracing::instrument;

use crate::app::AppContext;
use crate::metrics::{AlertKind, AlertSeverity};
use crate::state::{
    PartitionSlice, ReplicaLag, WraparoundDatabase, WraparoundRelation, WraparoundSnapshot,
};

const PARTITION_SQL: &str = r#"
SELECT
    parent.relname AS parent_name,
    parent.oid::regclass::text AS parent,
    child.oid::regclass::text AS child,
    pg_get_expr(child.relpartbound, child.oid) AS bounds
FROM pg_inherits
JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
JOIN pg_class child ON child.oid = pg_inherits.inhrelid
JOIN pg_namespace pn ON pn.oid = parent.relnamespace
JOIN pg_namespace cn ON cn.oid = child.relnamespace
WHERE pn.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY parent, child
"#;

const STALE_STATS_SQL: &str = r#"
SELECT
    relid::regclass::text AS relation,
    n_live_tup,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
"#;

#[instrument(skip_all)]
pub async fn run(ctx: &AppContext) -> Result<()> {
    update_partitions(ctx).await?;
    update_replication(ctx).await?;
    update_wraparound(ctx).await?;
    update_stale_stats(ctx).await?;
    Ok(())
}

async fn update_partitions(ctx: &AppContext) -> Result<()> {
    let rows = sqlx::query(PARTITION_SQL).fetch_all(&ctx.pool).await?;

    #[derive(Default)]
    struct PartitionAccumulator {
        child_count: usize,
        oldest_start: Option<DateTime<Utc>>,
        newest_start: Option<DateTime<Utc>>,
        latest_upper: Option<DateTime<Utc>>,
        latest_partition_name: Option<String>,
    }

    let mut acc: HashMap<String, PartitionAccumulator> = HashMap::new();

    for row in rows {
        let parent: String = row.try_get("parent")?;
        let child: String = row.try_get("child")?;
        let bounds: Option<String> = row.try_get("bounds")?;

        let accumulator = acc.entry(parent.clone()).or_default();
        accumulator.child_count += 1;

        if let Some(bounds) = bounds {
            let (lower, upper) = parse_partition_bounds(&bounds);

            if let Some(lower) = lower {
                match accumulator.oldest_start {
                    Some(existing) if lower >= existing => {}
                    _ => accumulator.oldest_start = Some(lower),
                }

                match accumulator.newest_start {
                    Some(existing) if lower <= existing => {}
                    _ => accumulator.newest_start = Some(lower),
                }
            }

            if let Some(upper) = upper {
                match accumulator.latest_upper {
                    Some(existing) if upper <= existing => {}
                    _ => {
                        accumulator.latest_upper = Some(upper);
                        accumulator.latest_partition_name = Some(child.clone());
                    }
                }
            }
        }
    }

    let now = Utc::now();
    let horizon = chrono::Duration::days(ctx.config.limits.partition_horizon_days as i64);
    let horizon_cutoff = now + horizon;

    let mut summaries = Vec::with_capacity(acc.len());
    for (parent, data) in acc {
        let PartitionAccumulator {
            child_count,
            oldest_start,
            newest_start,
            latest_upper,
            latest_partition_name,
        } = data;

        let (missing_future, future_gap_seconds, next_expected, latest_upper_bound) =
            if let Some(upper) = latest_upper {
                let missing = upper < horizon_cutoff;
                let gap = if missing {
                    Some((horizon_cutoff - upper).num_seconds().max(0))
                } else {
                    None
                };
                let next_candidate = upper + horizon;
                let next = if next_candidate > now {
                    Some(next_candidate)
                } else {
                    None
                };
                (missing, gap, next, Some(upper))
            } else {
                (false, None, None, None)
            };

        summaries.push(PartitionSlice {
            parent,
            child_count,
            oldest_partition: oldest_start,
            newest_partition: newest_start,
            latest_partition_upper: latest_upper_bound,
            latest_partition_name,
            next_expected_partition: next_expected,
            missing_future_partition: missing_future,
            future_gap_seconds,
        });
    }

    let at_risk: Vec<&PartitionSlice> = summaries
        .iter()
        .filter(|slice| slice.missing_future_partition)
        .collect();

    ctx.metrics
        .set_partition_metrics(ctx.cluster_name(), &summaries);

    for _slice in &at_risk {
        ctx.metrics.inc_alert(
            ctx.cluster_name(),
            AlertKind::PartitionGap,
            AlertSeverity::Warn,
        );
    }

    let alerts: Vec<String> = at_risk
        .iter()
        .map(|slice| partition_alert_message(slice))
        .collect();

    let alerts_for_overview = alerts.clone();
    ctx.state
        .update_overview_with(move |overview| {
            overview
                .open_alerts
                .retain(|alert| !alert.starts_with("Partition horizon"));
            overview
                .open_crit_alerts
                .retain(|alert| !alert.starts_with("Partition horizon"));

            if !alerts_for_overview.is_empty() {
                overview
                    .open_alerts
                    .extend(alerts_for_overview.iter().cloned());
            }
        })
        .await;

    ctx.state.update_partitions(summaries).await;
    Ok(())
}

async fn update_replication(ctx: &AppContext) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT
            application_name,
            GREATEST(
                COALESCE(EXTRACT(EPOCH FROM write_lag), 0),
                COALESCE(EXTRACT(EPOCH FROM flush_lag), 0),
                COALESCE(EXTRACT(EPOCH FROM replay_lag), 0)
            ) AS lag_seconds,
            pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint AS lag_bytes
        FROM pg_stat_replication
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;

    let mut replicas = Vec::with_capacity(rows.len());
    for row in rows {
        let replica: String = row.try_get("application_name")?;
        let lag_seconds: Option<f64> = row.try_get("lag_seconds")?;
        let lag_bytes: Option<i64> = row.try_get("lag_bytes")?;

        replicas.push(ReplicaLag {
            replica,
            lag_seconds,
            lag_bytes,
        });
    }

    ctx.metrics
        .set_replication_metrics(ctx.cluster_name(), &replicas);
    let replicas_clone = replicas.clone();
    ctx.state.update_replication(replicas).await;

    let alerts_cfg = &ctx.config.alerts;
    ctx.state
        .update_overview_with(|overview| {
            overview
                .open_alerts
                .retain(|alert| !alert.starts_with("Replication lag"));
            overview
                .open_crit_alerts
                .retain(|alert| !alert.starts_with("Replication lag"));

            for replica in &replicas_clone {
                if let Some(lag) = replica.lag_seconds {
                    if lag >= alerts_cfg.repl_warn_s as f64 {
                        overview
                            .open_alerts
                            .push(format!("Replication lag {}: {:.0}s", replica.replica, lag));
                        ctx.metrics.inc_alert(
                            ctx.cluster_name(),
                            AlertKind::ReplicationLag,
                            AlertSeverity::Warn,
                        );
                    }
                    if lag >= alerts_cfg.repl_crit_s as f64 {
                        overview.open_crit_alerts.push(format!(
                            "Replication lag critical {}: {:.0}s",
                            replica.replica, lag
                        ));
                        ctx.metrics.inc_alert(
                            ctx.cluster_name(),
                            AlertKind::ReplicationLag,
                            AlertSeverity::Crit,
                        );
                    }
                }
            }
        })
        .await;
    Ok(())
}

async fn update_wraparound(ctx: &AppContext) -> Result<()> {
    let db_rows = sqlx::query(
        r#"
        SELECT datname, age(datfrozenxid)::bigint AS tx_age
        FROM pg_database
        ORDER BY tx_age DESC
        LIMIT 10
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;

    let mut databases = Vec::with_capacity(db_rows.len());
    for row in db_rows {
        let database: String = row.try_get("datname")?;
        let tx_age: i64 = row.try_get::<i64, _>("tx_age")?;
        databases.push(WraparoundDatabase { database, tx_age });
    }

    let rel_rows = sqlx::query(
        r#"
        SELECT c.oid::regclass::text AS relation, age(c.relfrozenxid)::bigint AS tx_age
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND c.relkind IN ('r','m')
        ORDER BY age(c.relfrozenxid) DESC
        LIMIT $1
        "#,
    )
    .bind(50_i64)
    .fetch_all(&ctx.pool)
    .await?;

    let mut relations = Vec::with_capacity(rel_rows.len());
    for row in rel_rows {
        let relation: String = row.try_get("relation")?;
        let tx_age: i64 = row.try_get::<i64, _>("tx_age")?;
        relations.push(WraparoundRelation { relation, tx_age });
    }

    let snapshot = WraparoundSnapshot {
        databases,
        relations,
    };

    let filtered_snapshot = if ctx.config.ui.hide_postgres_defaults {
        filter_wraparound_snapshot(snapshot)
    } else {
        snapshot
    };

    ctx.metrics.set_wraparound_metrics(
        ctx.cluster_name(),
        &filtered_snapshot.databases,
        &filtered_snapshot.relations,
    );
    emit_wraparound_alerts(ctx, &filtered_snapshot).await;
    ctx.state.update_wraparound(filtered_snapshot).await;
    Ok(())
}

async fn emit_wraparound_alerts(ctx: &AppContext, snapshot: &WraparoundSnapshot) {
    let thresholds = &ctx.config.alerts;
    let warn_age = thresholds.wraparound_warn_tx_age as i64;
    let crit_age = thresholds.wraparound_crit_tx_age as i64;

    let mut warn = Vec::new();
    let mut crit = Vec::new();

    for db in &snapshot.databases {
        classify_wraparound(
            ctx,
            &mut warn,
            &mut crit,
            "database",
            &db.database,
            db.tx_age,
            warn_age,
            crit_age,
        );
    }

    for rel in &snapshot.relations {
        classify_wraparound(
            ctx,
            &mut warn,
            &mut crit,
            "relation",
            &rel.relation,
            rel.tx_age,
            warn_age,
            crit_age,
        );
    }

    ctx.state
        .update_overview_with(|overview| {
            overview
                .open_alerts
                .retain(|alert| !alert.starts_with("Wraparound"));
            overview
                .open_crit_alerts
                .retain(|alert| !alert.starts_with("Wraparound"));
            overview.open_alerts.extend(warn.iter().cloned());
            overview.open_crit_alerts.extend(crit.iter().cloned());
        })
        .await;
}

fn classify_wraparound(
    ctx: &AppContext,
    warn: &mut Vec<String>,
    crit: &mut Vec<String>,
    kind: &str,
    name: &str,
    tx_age: i64,
    warn_age: i64,
    crit_age: i64,
) {
    if tx_age >= crit_age {
        let message = format!(
            "Wraparound {kind} critical {name} age {}",
            format_tx_age(tx_age)
        );
        crit.push(message.clone());
        ctx.metrics.inc_alert(
            ctx.cluster_name(),
            AlertKind::Wraparound,
            AlertSeverity::Crit,
        );
    } else if tx_age >= warn_age {
        let message = format!("Wraparound {kind} {name} age {}", format_tx_age(tx_age));
        warn.push(message.clone());
        ctx.metrics.inc_alert(
            ctx.cluster_name(),
            AlertKind::Wraparound,
            AlertSeverity::Warn,
        );
    }
}

fn format_tx_age(age: i64) -> String {
    if age >= 1_000_000_000 {
        format!("{:.2}e9", age as f64 / 1_000_000_000.0)
    } else if age >= 1_000_000 {
        format!("{:.2}e6", age as f64 / 1_000_000.0)
    } else {
        age.to_string()
    }
}

async fn update_stale_stats(ctx: &AppContext) -> Result<()> {
    let rows = sqlx::query(STALE_STATS_SQL).fetch_all(&ctx.pool).await?;

    let now = Utc::now();
    let warn_hours = ctx.config.alerts.stats_stale_warn_hours as f64;
    let crit_hours = ctx.config.alerts.stats_stale_crit_hours as f64;
    let warn_seconds = warn_hours * 3600.0;
    let crit_seconds = crit_hours * 3600.0;

    let mut entries = Vec::new();
    let mut alerts_warn = Vec::new();
    let mut alerts_crit = Vec::new();

    for row in rows {
        let relation: String = row.try_get("relation")?;
        let n_live_tup: i64 = row.try_get("n_live_tup")?;
        let last_analyze: Option<DateTime<Utc>> = row.try_get("last_analyze")?;
        let last_autoanalyze: Option<DateTime<Utc>> = row.try_get("last_autoanalyze")?;

        let latest = match (last_analyze, last_autoanalyze) {
            (Some(a), Some(b)) => Some(if a > b { a } else { b }),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let age_seconds = latest.map(|ts| (now - ts).num_seconds().max(0) as f64);
        let is_stale = match age_seconds {
            Some(secs) => secs >= warn_seconds,
            None => true,
        };

        if !is_stale {
            continue;
        }

        let (severity, message) = match age_seconds {
            Some(secs) if secs >= crit_seconds => (
                AlertSeverity::Crit,
                format!("Stats stale {relation}: {:.1}h", secs / 3600.0),
            ),
            Some(secs) => (
                AlertSeverity::Warn,
                format!("Stats stale {relation}: {:.1}h", secs / 3600.0),
            ),
            None => (
                AlertSeverity::Crit,
                format!("Stats stale {relation}: never analyzed"),
            ),
        };

        match severity {
            AlertSeverity::Crit => alerts_crit.push(message.clone()),
            AlertSeverity::Warn => alerts_warn.push(message.clone()),
        }
        ctx.metrics
            .inc_alert(ctx.cluster_name(), AlertKind::StaleStats, severity);

        let hours_since = age_seconds.map(|secs| secs / 3600.0);
        entries.push(crate::state::StaleStatEntry {
            relation,
            last_analyze,
            last_autoanalyze,
            hours_since_analyze: hours_since,
            n_live_tup,
        });
    }

    entries.sort_by(|a, b| {
        let a_age = a.hours_since_analyze.map(|h| h as i64).unwrap_or(i64::MAX);
        let b_age = b.hours_since_analyze.map(|h| h as i64).unwrap_or(i64::MAX);
        b_age.cmp(&a_age)
    });

    let limit = ctx.config.limits.top_relations as usize;
    let limited = if entries.len() > limit {
        entries[..limit].to_vec()
    } else {
        entries.clone()
    };

    ctx.metrics
        .set_stale_stats_metrics(ctx.cluster_name(), &limited);
    ctx.state.update_stale_stats(limited.clone()).await;

    if !alerts_warn.is_empty() || !alerts_crit.is_empty() {
        ctx.state
            .update_overview_with(|overview| {
                overview
                    .open_alerts
                    .retain(|alert| !alert.starts_with("Stats stale"));
                overview
                    .open_crit_alerts
                    .retain(|alert| !alert.starts_with("Stats stale"));
                overview.open_alerts.extend(alerts_warn.clone());
                overview.open_crit_alerts.extend(alerts_crit.clone());
            })
            .await;
    } else {
        ctx.state
            .update_overview_with(|overview| {
                overview
                    .open_alerts
                    .retain(|alert| !alert.starts_with("Stats stale"));
                overview
                    .open_crit_alerts
                    .retain(|alert| !alert.starts_with("Stats stale"));
            })
            .await;
    }

    Ok(())
}

fn filter_wraparound_snapshot(snapshot: WraparoundSnapshot) -> WraparoundSnapshot {
    const SYSTEM_DATABASES: &[&str] = &["postgres", "template0", "template1"]; // Always skip system defaults in the UI when requested.

    let databases = snapshot
        .databases
        .into_iter()
        .filter(|db| !SYSTEM_DATABASES.contains(&db.database.as_str()))
        .collect();

    WraparoundSnapshot {
        databases,
        relations: snapshot.relations,
    }
}

fn partition_alert_message(slice: &PartitionSlice) -> String {
    let next_due = slice
        .next_expected_partition
        .as_ref()
        .map(|ts| ts.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "unknown".into());

    if let Some(gap_seconds) = slice.future_gap_seconds {
        let gap_hours = (gap_seconds as f64 / 3600.0).max(0.0);
        format!(
            "Partition horizon risk {}: coverage lags {:.1}h (next due {next_due})",
            slice.parent, gap_hours
        )
    } else {
        format!(
            "Partition horizon risk {}: next due {next_due}",
            slice.parent
        )
    }
}

fn parse_partition_bounds(bound: &str) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    if bound.contains("FOR VALUES FROM") {
        let lower_raw = extract_between(bound, "FROM", "TO");
        let upper_raw = extract_after(bound, "TO");

        let lower = lower_raw.as_deref().and_then(parse_datetime_string);
        let upper = upper_raw.as_deref().and_then(parse_datetime_string);

        (lower, upper)
    } else if bound.contains("FOR VALUES IN") {
        let value_raw = extract_after(bound, "IN");
        let value = value_raw.as_deref().and_then(parse_datetime_string);
        (value, value)
    } else {
        (None, None)
    }
}

fn extract_between(input: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start = input.find(start_marker)?;
    let after_start = input[start + start_marker.len()..].trim_start();
    let open_paren = after_start.find('(')?;
    let after_paren = &after_start[open_paren + 1..];
    let end = after_paren.find(end_marker)?;
    let segment = after_paren[..end].trim();
    Some(clean_bound_segment(segment))
}

fn extract_after(input: &str, marker: &str) -> Option<String> {
    let start = input.find(marker)?;
    let after = input[start + marker.len()..].trim_start();
    let open_paren = after.find('(')?;
    let after_paren = &after[open_paren + 1..];
    let close_paren = after_paren.find(')')?;
    let segment = after_paren[..close_paren].trim();
    Some(clean_bound_segment(segment))
}

fn clean_bound_segment(segment: &str) -> String {
    let first = segment.split(',').next().unwrap_or(segment);
    first
        .trim_matches(|c| c == '\'' || c == '(' || c == ')')
        .to_string()
}

fn parse_datetime_string(value: &str) -> Option<DateTime<Utc>> {
    if value.eq_ignore_ascii_case("maxvalue") || value.eq_ignore_ascii_case("minvalue") {
        return None;
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.with_timezone(&Utc));
    }

    if let Ok(date_time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Some(Utc.from_utc_datetime(&date_time));
    }

    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        if let Some(dt) = date.and_hms_opt(0, 0, 0) {
            return Some(Utc.from_utc_datetime(&dt));
        }
    }

    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y%m%d") {
        if let Some(dt) = date.and_hms_opt(0, 0, 0) {
            return Some(Utc.from_utc_datetime(&dt));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_range_bounds() {
        let input = "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')";
        let (lower, upper) = parse_partition_bounds(input);
        let expected_lower = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let expected_upper = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();

        assert_eq!(lower, Some(expected_lower));
        assert_eq!(upper, Some(expected_upper));
    }

    #[test]
    fn parses_list_bounds() {
        let input = "FOR VALUES IN ('2024-03-15')";
        let (lower, upper) = parse_partition_bounds(input);
        let expected = Utc.with_ymd_and_hms(2024, 3, 15, 0, 0, 0).unwrap();
        assert_eq!(lower, Some(expected));
        assert_eq!(upper, Some(expected));
    }

    #[test]
    fn ignores_unparsable_values() {
        let input = "FOR VALUES FROM (MINVALUE) TO (MAXVALUE)";
        let (lower, upper) = parse_partition_bounds(input);
        assert!(lower.is_none());
        assert!(upper.is_none());
    }
}
