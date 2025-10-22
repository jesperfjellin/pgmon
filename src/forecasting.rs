use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::state::{
    DailyMetricSummary, OverviewSnapshot, StorageEntry, WraparoundDatabase, WraparoundRelation,
};

/// Forecast severity based on time until threshold breach
#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum ForecastSeverity {
    Info,   // >90 days
    Warn,   // 30-90 days
    Crit,   // <30 days
    Urgent, // <7 days
}

/// A capacity forecast with predicted exhaustion date
#[derive(Debug, Clone, Serialize)]
pub struct Forecast {
    pub kind: ForecastKind,
    pub resource: String,
    pub current_value: f64,
    pub threshold: f64,
    pub growth_rate_per_day: f64,
    pub days_until_threshold: Option<f64>,
    pub predicted_date: Option<DateTime<Utc>>,
    pub severity: ForecastSeverity,
    pub message: String,
}

/// Type of capacity forecast
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForecastKind {
    WraparoundDatabase,
    WraparoundRelation,
    TableGrowth,
    ConnectionSaturation,
}

/// Response containing all forecasts
#[derive(Debug, Clone, Serialize)]
pub struct ForecastsResponse {
    pub forecasts: Vec<Forecast>,
}

/// Generate all capacity forecasts
pub fn generate_forecasts(
    overview: &OverviewSnapshot,
    storage: &[StorageEntry],
    wraparound_databases: &[WraparoundDatabase],
    wraparound_relations: &[WraparoundRelation],
    connections_history: &[DailyMetricSummary],
) -> Vec<Forecast> {
    let mut forecasts = Vec::new();

    // Wraparound forecasting
    forecasts.extend(forecast_wraparound_databases(wraparound_databases));
    forecasts.extend(forecast_wraparound_relations(wraparound_relations));

    // Table growth forecasting (simplified - needs historical storage data for real trends)
    forecasts.extend(forecast_table_growth(storage));

    // Connection saturation forecasting
    forecasts.extend(forecast_connection_saturation(
        overview,
        connections_history,
    ));

    // Sort by severity (Urgent first, then Crit, Warn, Info)
    forecasts.sort_by(|a, b| b.severity.cmp(&a.severity));

    forecasts
}

/// Forecast wraparound risk for databases
fn forecast_wraparound_databases(databases: &[WraparoundDatabase]) -> Vec<Forecast> {
    let mut forecasts = Vec::new();

    // PostgreSQL autovacuum_freeze_max_age default is 200M transactions
    const FREEZE_MAX_AGE: i64 = 200_000_000;
    const CRITICAL_THRESHOLD: i64 = (FREEZE_MAX_AGE as f64 * 0.8) as i64; // 80% = 160M

    for db in databases {
        let current_age = db.tx_age;
        let remaining = CRITICAL_THRESHOLD - current_age;

        if remaining <= 0 {
            // Already at or past critical threshold
            forecasts.push(Forecast {
                kind: ForecastKind::WraparoundDatabase,
                resource: db.database.clone(),
                current_value: current_age as f64,
                threshold: CRITICAL_THRESHOLD as f64,
                growth_rate_per_day: 0.0, // Unknown without historical data
                days_until_threshold: Some(0.0),
                predicted_date: Some(Utc::now()),
                severity: ForecastSeverity::Urgent,
                message: format!(
                    "Database '{}' has reached critical wraparound age ({} transactions, {}% of limit). Immediate VACUUM required!",
                    db.database,
                    format_number(current_age),
                    (current_age as f64 / FREEZE_MAX_AGE as f64 * 100.0) as i64
                ),
            });
        } else if current_age > (FREEZE_MAX_AGE as f64 * 0.5) as i64 {
            // Above 50% of limit, start warning
            let pct = (current_age as f64 / CRITICAL_THRESHOLD as f64 * 100.0) as i64;
            let severity = if pct >= 90 {
                ForecastSeverity::Crit
            } else if pct >= 70 {
                ForecastSeverity::Warn
            } else {
                ForecastSeverity::Info
            };

            forecasts.push(Forecast {
                kind: ForecastKind::WraparoundDatabase,
                resource: db.database.clone(),
                current_value: current_age as f64,
                threshold: CRITICAL_THRESHOLD as f64,
                growth_rate_per_day: 0.0,
                days_until_threshold: None,
                predicted_date: None,
                severity,
                message: format!(
                    "Database '{}' at {}% of wraparound threshold ({} / {} transactions). Monitor closely.",
                    db.database,
                    pct,
                    format_number(current_age),
                    format_number(CRITICAL_THRESHOLD)
                ),
            });
        }
    }

    forecasts
}

/// Forecast wraparound risk for relations
fn forecast_wraparound_relations(relations: &[WraparoundRelation]) -> Vec<Forecast> {
    let mut forecasts = Vec::new();

    const FREEZE_MAX_AGE: i64 = 200_000_000;
    const CRITICAL_THRESHOLD: i64 = (FREEZE_MAX_AGE as f64 * 0.8) as i64;

    for rel in relations {
        let current_age = rel.tx_age;

        if current_age > (CRITICAL_THRESHOLD as f64 * 0.9) as i64 {
            let remaining = CRITICAL_THRESHOLD - current_age;
            let severity = if remaining <= 0 {
                ForecastSeverity::Urgent
            } else if current_age > (CRITICAL_THRESHOLD as f64 * 0.95) as i64 {
                ForecastSeverity::Crit
            } else {
                ForecastSeverity::Warn
            };

            forecasts.push(Forecast {
                kind: ForecastKind::WraparoundRelation,
                resource: rel.relation.clone(),
                current_value: current_age as f64,
                threshold: CRITICAL_THRESHOLD as f64,
                growth_rate_per_day: 0.0,
                days_until_threshold: None,
                predicted_date: None,
                severity,
                message: format!(
                    "Table '{}' at {}% of wraparound threshold ({} / {} transactions). VACUUM FREEZE recommended.",
                    rel.relation,
                    (current_age as f64 / CRITICAL_THRESHOLD as f64 * 100.0) as i64,
                    format_number(current_age),
                    format_number(CRITICAL_THRESHOLD)
                ),
            });
        }
    }

    forecasts
}

/// Forecast table growth (simplified - would need historical storage snapshots for accurate trends)
fn forecast_table_growth(storage: &[StorageEntry]) -> Vec<Forecast> {
    let mut forecasts = Vec::new();

    // For now, just flag very large tables as "approaching capacity"
    // Real implementation would track daily storage snapshots and use linear regression
    const LARGE_TABLE_THRESHOLD: i64 = 100 * 1024 * 1024 * 1024; // 100 GB

    for entry in storage {
        if entry.total_bytes > LARGE_TABLE_THRESHOLD {
            let gb = entry.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            forecasts.push(Forecast {
                kind: ForecastKind::TableGrowth,
                resource: entry.relation.clone(),
                current_value: entry.total_bytes as f64,
                threshold: LARGE_TABLE_THRESHOLD as f64,
                growth_rate_per_day: 0.0, // Would need historical data
                days_until_threshold: None,
                predicted_date: None,
                severity: ForecastSeverity::Info,
                message: format!(
                    "Table '{}' is {:.1} GB. Consider partitioning or archival strategy for continued growth.",
                    entry.relation, gb
                ),
            });
        }
    }

    forecasts
}

/// Forecast connection saturation using linear regression on historical connection counts
fn forecast_connection_saturation(
    overview: &OverviewSnapshot,
    connections_history: &[DailyMetricSummary],
) -> Vec<Forecast> {
    let mut forecasts = Vec::new();

    let max_connections = overview.max_connections;
    let current_connections = overview.connections;

    // Need at least 7 days of data for meaningful forecast
    if connections_history.len() < 7 {
        return forecasts;
    }

    // Extract mean connection counts from last 30 days (or available data)
    let recent_history: Vec<f64> = connections_history
        .iter()
        .rev()
        .take(30)
        .map(|day| day.mean)
        .collect();

    if recent_history.is_empty() {
        return forecasts;
    }

    // Simple linear regression: y = mx + b
    let (slope, _intercept) = linear_regression(&recent_history);

    // If connections are growing (positive slope)
    if slope > 0.1 {
        // Predict when we'll hit 90% of max_connections
        let threshold = (max_connections as f64 * 0.9).floor();
        let remaining = threshold - current_connections as f64;
        let days_until = remaining / slope;

        if days_until > 0.0 && days_until < 180.0 {
            let predicted_date = Utc::now() + Duration::days(days_until as i64);

            let severity = if days_until < 7.0 {
                ForecastSeverity::Urgent
            } else if days_until < 30.0 {
                ForecastSeverity::Crit
            } else if days_until < 90.0 {
                ForecastSeverity::Warn
            } else {
                ForecastSeverity::Info
            };

            forecasts.push(Forecast {
                kind: ForecastKind::ConnectionSaturation,
                resource: "database".to_string(),
                current_value: current_connections as f64,
                threshold,
                growth_rate_per_day: slope,
                days_until_threshold: Some(days_until),
                predicted_date: Some(predicted_date),
                severity,
                message: format!(
                    "Connections growing at {:.1}/day. Will reach 90% capacity ({} / {}) in {} days on {}.",
                    slope,
                    threshold as i64,
                    max_connections,
                    days_until as i64,
                    predicted_date.format("%Y-%m-%d")
                ),
            });
        }
    }

    forecasts
}

/// Simple linear regression: returns (slope, intercept)
/// Given a time series [y0, y1, y2, ...] at equal intervals
fn linear_regression(values: &[f64]) -> (f64, f64) {
    let n = values.len() as f64;
    if n < 2.0 {
        return (0.0, 0.0);
    }

    // X values are simply 0, 1, 2, 3, ... (day indices)
    let sum_x: f64 = (0..values.len()).map(|i| i as f64).sum();
    let sum_y: f64 = values.iter().sum();
    let sum_xy: f64 = values
        .iter()
        .enumerate()
        .map(|(i, &y)| i as f64 * y)
        .sum();
    let sum_x2: f64 = (0..values.len()).map(|i| (i as f64).powi(2)).sum();

    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));
    let intercept = (sum_y - slope * sum_x) / n;

    (slope, intercept)
}

// Helper formatter
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
