use chrono::Utc;
use serde::Serialize;

use crate::state::{AutovacuumEntry, BloatSample, StaleStatEntry, StorageEntry};

/// Type of maintenance recommendation
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationKind {
    VacuumAnalyze,
    VacuumFull,
    Analyze,
    Reindex,
}

/// Severity level for recommendations
#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warn,
    Crit,
}

/// Impact details for a recommendation
#[derive(Debug, Clone, Serialize)]
pub struct Impact {
    pub estimated_duration_seconds: Option<u32>,
    pub locks_table: bool,
    pub reclaim_bytes: Option<i64>,
}

/// A maintenance recommendation with SQL command and rationale
#[derive(Debug, Clone, Serialize)]
pub struct Recommendation {
    pub kind: RecommendationKind,
    pub relation: String,
    pub severity: Severity,
    pub sql_command: String,
    pub rationale: String,
    pub impact: Impact,
}

/// Generate maintenance recommendations based on current system state
pub fn generate_recommendations(
    storage: &[StorageEntry],
    bloat: &[BloatSample],
    autovac: &[AutovacuumEntry],
    stale_stats: &[StaleStatEntry],
) -> Vec<Recommendation> {
    let mut recommendations = Vec::new();

    // Generate VACUUM ANALYZE recommendations
    recommendations.extend(generate_vacuum_analyze_recommendations(storage, autovac));

    // Generate VACUUM FULL recommendations
    recommendations.extend(generate_vacuum_full_recommendations(bloat));

    // Generate ANALYZE recommendations
    recommendations.extend(generate_analyze_recommendations(stale_stats));

    // Sort by severity (Crit first, then Warn, then Info)
    recommendations.sort_by(|a, b| b.severity.cmp(&a.severity));

    recommendations
}

/// Recommend VACUUM ANALYZE for tables with high dead tuple ratios
fn generate_vacuum_analyze_recommendations(
    storage: &[StorageEntry],
    autovac: &[AutovacuumEntry],
) -> Vec<Recommendation> {
    let mut recommendations = Vec::new();

    for entry in storage {
        // Skip if no dead tuple data
        let dead_tuples = match entry.dead_tuples {
            Some(dt) if dt > 0 => dt,
            _ => continue,
        };

        let dead_ratio = match entry.dead_tuple_ratio {
            Some(dr) if dr > 0.0 => dr,
            _ => continue,
        };

        // Find matching autovacuum entry to check last vacuum time
        let last_vacuum_age_hours = autovac
            .iter()
            .find(|av| av.relation == entry.relation)
            .and_then(|av| {
                av.last_autovacuum
                    .or(av.last_vacuum)
                    .map(|dt| {
                        let age = Utc::now().signed_duration_since(dt);
                        age.num_hours() as f64 + (age.num_minutes() % 60) as f64 / 60.0
                    })
            });

        // Rule 1: Dead tuple ratio >= 40% OR dead tuples >= 500k = CRITICAL
        let is_critical = dead_ratio >= 40.0 || dead_tuples >= 500_000;

        // Rule 2: Dead tuple ratio >= 20% AND dead tuples >= 100k = WARNING
        let is_warning = dead_ratio >= 20.0 && dead_tuples >= 100_000;

        if !is_critical && !is_warning {
            continue;
        }

        let severity = if is_critical {
            Severity::Crit
        } else {
            Severity::Warn
        };

        // Estimate reclaim bytes (dead tuples * average row size)
        let reclaim_bytes = if entry.reltuples.unwrap_or(0.0) > 0.0 && entry.table_bytes > 0 {
            let avg_row_bytes = entry.table_bytes as f64 / entry.reltuples.unwrap_or(1.0);
            Some((avg_row_bytes * dead_tuples as f64) as i64)
        } else {
            entry.estimated_bloat_bytes
        };

        // Build rationale
        let mut rationale_parts = vec![format!(
            "Table has {:.1}% dead tuples ({} rows)",
            dead_ratio,
            format_number(dead_tuples)
        )];

        if let Some(hours) = last_vacuum_age_hours {
            if hours >= 1.0 {
                rationale_parts.push(format!("and hasn't been vacuumed in {:.1} hours", hours));
            }
        }

        if let Some(reclaim) = reclaim_bytes {
            rationale_parts.push(format!(
                "Running VACUUM ANALYZE will reclaim ~{} and update statistics",
                format_bytes(reclaim)
            ));
        } else {
            rationale_parts
                .push("Running VACUUM ANALYZE will reclaim space and update statistics".into());
        }

        let rationale = rationale_parts.join(". ") + ".";

        // Estimate duration based on table size (rough heuristic: ~100MB/sec)
        let estimated_duration = if entry.table_bytes > 0 {
            Some((entry.table_bytes / 100_000_000).max(5) as u32)
        } else {
            None
        };

        recommendations.push(Recommendation {
            kind: RecommendationKind::VacuumAnalyze,
            relation: entry.relation.clone(),
            severity,
            sql_command: format!("VACUUM ANALYZE {};", entry.relation),
            rationale,
            impact: Impact {
                estimated_duration_seconds: estimated_duration,
                locks_table: false,
                reclaim_bytes,
            },
        });
    }

    recommendations
}

/// Recommend VACUUM FULL for tables with high free space but low dead tuples
fn generate_vacuum_full_recommendations(bloat: &[BloatSample]) -> Vec<Recommendation> {
    let mut recommendations = Vec::new();

    for sample in bloat {
        // Only recommend VACUUM FULL when:
        // 1. Free space > 40%
        // 2. Dead tuple % is low (< 5%) OR not available (approx mode)
        //    This indicates the space is truly wasted, not just needs regular VACUUM
        let free_percent = sample.free_percent;
        let dead_percent = sample.dead_tuple_percent.unwrap_or(0.0);

        let needs_vacuum_full = free_percent > 40.0 && dead_percent < 5.0;

        if !needs_vacuum_full {
            continue;
        }

        let severity = if free_percent > 60.0 {
            Severity::Crit
        } else if free_percent > 50.0 {
            Severity::Warn
        } else {
            Severity::Info
        };

        let reclaim_bytes = sample.free_bytes;

        let rationale = if sample.dead_tuple_percent.is_some() {
            format!(
                "Table has {:.1}% free space but only {:.1}% dead tuples. Regular VACUUM won't help - bloat needs VACUUM FULL to reclaim {}. ⚠️ Requires exclusive lock.",
                free_percent,
                dead_percent,
                format_bytes(reclaim_bytes)
            )
        } else {
            format!(
                "Table has {:.1}% free space. VACUUM FULL will reclaim {}. ⚠️ Requires exclusive lock.",
                free_percent,
                format_bytes(reclaim_bytes)
            )
        };

        // VACUUM FULL duration is harder to estimate, but assume ~50MB/sec (slower than regular VACUUM)
        let estimated_duration = if sample.table_bytes > 0 {
            Some((sample.table_bytes / 50_000_000).max(10) as u32)
        } else {
            None
        };

        recommendations.push(Recommendation {
            kind: RecommendationKind::VacuumFull,
            relation: sample.relation.clone(),
            severity,
            sql_command: format!(
                "-- ⚠️ LOCKS TABLE - run during maintenance window\nVACUUM FULL {};",
                sample.relation
            ),
            rationale,
            impact: Impact {
                estimated_duration_seconds: estimated_duration,
                locks_table: true,
                reclaim_bytes: Some(reclaim_bytes),
            },
        });
    }

    recommendations
}

/// Recommend ANALYZE for tables with stale statistics
fn generate_analyze_recommendations(stale_stats: &[StaleStatEntry]) -> Vec<Recommendation> {
    let mut recommendations = Vec::new();

    for stat in stale_stats {
        let hours = match stat.hours_since_analyze {
            Some(h) if h > 0.0 => h,
            _ => continue,
        };

        // Rule 1: > 24 hours = CRITICAL
        // Rule 2: > 12 hours = WARNING
        // Rule 3: > 6 hours with large table (> 100M rows) = INFO
        let severity = if hours >= 24.0 {
            Severity::Crit
        } else if hours >= 12.0 {
            Severity::Warn
        } else if hours >= 6.0 && stat.n_live_tup > 100_000_000 {
            Severity::Info
        } else {
            continue;
        };

        let last_analyze_str = if let Some(dt) = stat.last_analyze.or(stat.last_autoanalyze) {
            dt.format("%Y-%m-%d %H:%M UTC").to_string()
        } else {
            "never".to_string()
        };

        let rationale = format!(
            "Table statistics are {:.1} hours old (last analyzed: {}). Running ANALYZE will help the query planner choose better indexes and execution plans.",
            hours,
            last_analyze_str
        );

        // ANALYZE is typically fast, estimate based on table size (~1GB/sec scan)
        let estimated_duration = Some(5u32); // Usually < 5 seconds for most tables

        recommendations.push(Recommendation {
            kind: RecommendationKind::Analyze,
            relation: stat.relation.clone(),
            severity,
            sql_command: format!("ANALYZE {};", stat.relation),
            rationale,
            impact: Impact {
                estimated_duration_seconds: estimated_duration,
                locks_table: false,
                reclaim_bytes: None,
            },
        });
    }

    recommendations
}

// Helper formatters
fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1024 * KB;
    const GB: i64 = 1024 * MB;
    const TB: i64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
