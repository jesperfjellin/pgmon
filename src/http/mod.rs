use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, get_service};
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;
use tracing::warn;

use crate::app::AppContext;
use crate::poller::{HOT_PATH_LOOP, HOURLY_LOOP, STORAGE_LOOP, WORKLOAD_LOOP};

const LOOP_NAMES: &[&str] = &[HOT_PATH_LOOP, WORKLOAD_LOOP, STORAGE_LOOP, HOURLY_LOOP];

pub fn create_router(ctx: AppContext) -> Router {
    let static_dir = ctx.config.http.static_dir.clone();

    let asset_service = get_service(ServeDir::new(static_dir));

    let api = Router::new()
        .route("/overview", get(get_overview))
        .route("/autovacuum", get(get_autovacuum))
        .route("/top-queries", get(get_top_queries))
        .route("/storage", get(get_storage))
        .route("/bloat", get(get_bloat))
        .route("/unused-indexes", get(get_unused_indexes))
        .route("/stale-stats", get(get_stale_stats))
        .route("/partitions", get(get_partitions))
        .route("/replication", get(get_replication))
        .route("/wraparound", get(get_wraparound))
        .route("/recommendations", get(get_recommendations))
        .route("/forecasts", get(get_forecasts))
        .route("/alerts/history", get(get_alerts_history))
        .route("/history/:metric", get(get_history));
    // Combined KPI history endpoint.
    let api = api.route("/history/overview", get(get_overview_history));

    Router::new()
        .route("/healthz", get(get_healthz))
        .route("/metrics", get(get_metrics))
        .nest("/api/v1", api)
        .fallback_service(asset_service)
        .layer(TraceLayer::new_for_http())
        .with_state(ctx)
}

async fn get_healthz(State(ctx): State<AppContext>) -> StatusCode {
    let is_ready = ctx
        .state
        .is_ready(LOOP_NAMES, Duration::from_secs(180))
        .await;

    if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn get_metrics(State(ctx): State<AppContext>) -> Response {
    match ctx.metrics.encode() {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        )
            .into_response(),
        Err(err) => {
            warn!(error = ?err, "failed to encode metrics");
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
    }
}

async fn get_overview(State(ctx): State<AppContext>) -> Json<crate::state::OverviewSnapshot> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.overview)
}

async fn get_autovacuum(State(ctx): State<AppContext>) -> Json<Vec<crate::state::AutovacuumEntry>> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.autovacuum)
}

async fn get_top_queries(State(ctx): State<AppContext>) -> Json<Vec<crate::state::TopQueryEntry>> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.top_queries)
}

async fn get_storage(State(ctx): State<AppContext>) -> Json<Vec<crate::state::StorageEntry>> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.storage)
}

async fn get_bloat(State(ctx): State<AppContext>) -> Json<Vec<crate::state::BloatSample>> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.bloat_samples)
}

async fn get_unused_indexes(
    State(ctx): State<AppContext>,
) -> Json<Vec<crate::state::UnusedIndexEntry>> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.unused_indexes)
}

async fn get_stale_stats(State(ctx): State<AppContext>) -> Json<Vec<crate::state::StaleStatEntry>> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.stale_stats)
}

async fn get_partitions(State(ctx): State<AppContext>) -> Json<Vec<crate::state::PartitionSlice>> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.partitions)
}

async fn get_replication(State(ctx): State<AppContext>) -> Json<Vec<crate::state::ReplicaLag>> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.replication)
}

async fn get_wraparound(State(ctx): State<AppContext>) -> Json<crate::state::WraparoundSnapshot> {
    let snapshots = ctx.state.get_snapshots().await;
    Json(snapshots.wraparound)
}

#[derive(serde::Serialize)]
struct RecommendationsResponse {
    recommendations: Vec<crate::recommendations::Recommendation>,
}

async fn get_recommendations(State(ctx): State<AppContext>) -> Json<RecommendationsResponse> {
    let snapshots = ctx.state.get_snapshots().await;
    let recommendations = crate::recommendations::generate_recommendations(
        &snapshots.storage,
        &snapshots.bloat_samples,
        &snapshots.autovacuum,
        &snapshots.stale_stats,
        ctx.config.bloat.min_reclaim_bytes,
    );
    Json(RecommendationsResponse { recommendations })
}

async fn get_forecasts(State(ctx): State<AppContext>) -> Json<crate::forecasting::ForecastsResponse> {
    let snapshots = ctx.state.get_snapshots().await;
    let history = ctx.state.snapshot_metric_history().await;

    let forecasts = crate::forecasting::generate_forecasts(
        &snapshots.overview,
        &snapshots.storage,
        &snapshots.wraparound.databases,
        &snapshots.wraparound.relations,
        &history.daily, // Use daily aggregates for connection history
    );
    Json(crate::forecasting::ForecastsResponse { forecasts })
}

async fn get_alerts_history(
    State(ctx): State<AppContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<Vec<crate::state::AlertEvent>> {
    let limit: usize = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let mut events = ctx.state.list_alert_events().await;
    if events.len() > limit {
        let start = events.len() - limit;
        events = events[start..].to_vec();
    }
    Json(events)
}

#[derive(serde::Serialize)]
struct HistoryResponse {
    metric: String,
    points: Vec<crate::state::TimePoint>,
    downsampled: bool,
}

#[derive(serde::Serialize)]
struct OverviewHistoryResponse {
    // Each series is already filtered + maybe downsampled.
    tps: Vec<crate::state::TimePoint>,
    qps: Vec<crate::state::TimePoint>,
    mean_latency_ms: Vec<crate::state::TimePoint>,
    latency_p95_ms: Vec<crate::state::TimePoint>,
    latency_p99_ms: Vec<crate::state::TimePoint>,
    connections: Vec<crate::state::TimePoint>,
    blocked_sessions: Vec<crate::state::TimePoint>,
    wraparound_xid_pct: Vec<crate::state::TimePoint>,
    wraparound_mxid_pct: Vec<crate::state::TimePoint>,
    window: String,
    downsampled: bool,
    partial: bool, // true when filtered by `since` producing only incremental new points
}

/// Returns high-resolution time series for a metric.
/// Query params:
///   ?window=24h | 6h | 1h | all (default 24h)
///   ?max_points=1000 (downsample target)
async fn get_history(
    State(ctx): State<AppContext>,
    axum::extract::Path(metric): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<HistoryResponse>, StatusCode> {
    let window = params.get("window").map(String::as_str).unwrap_or("24h");
    let max_points: usize = params
        .get("max_points")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    let all_points = match ctx.state.get_history_series(&metric).await {
        Some(series) => series,
        None => return Err(StatusCode::NOT_FOUND),
    };

    // Window filter
    let cutoff = cutoff_for_window(window).map_err(|_| StatusCode::BAD_REQUEST)?;
    let filtered: Vec<_> = match cutoff {
        Some(cutoff_time) => all_points.into_iter().filter(|p| p.ts >= cutoff_time).collect(),
        None => all_points, // No time filtering - return all points
    };

    let (points, downsampled) = maybe_downsample(filtered, max_points);
    Ok(Json(HistoryResponse {
        metric,
        points,
        downsampled,
    }))
}

/// Returns multiple KPI series in one call to reduce frontend round trips.
/// Query params mirror single metric history:
///   ?window=24h|6h|1h|all (default 1h for overview density)
///   ?max_points=300 (downsample target shared across all series)
async fn get_overview_history(
    State(ctx): State<AppContext>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<OverviewHistoryResponse>, StatusCode> {
    let window = params.get("window").map(String::as_str).unwrap_or("1h");
    let max_points: usize = params
        .get("max_points")
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    // Optional incremental cutoff: only return points with ts > since
    let since_cutoff: Option<chrono::DateTime<chrono::Utc>> = params
        .get("since")
        .and_then(|v| v.parse::<i64>().ok())
        .and_then(|secs| chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0));

    let cutoff = cutoff_for_window(window).map_err(|_| StatusCode::BAD_REQUEST)?;
    let history = ctx.state.snapshot_metric_history().await; // single lock clone.

    // Build filtered + optional incremental slice.

    let mut downsample_any = false;
    let ds = |pts: Vec<crate::state::TimePoint>, max: usize, flag: &mut bool| {
        let (sampled, down) = crate::http::maybe_downsample(pts, max);
        if down {
            *flag = true;
        }
        sampled
    };

    let effective_filter = |points: &[crate::state::TimePoint]| {
        let base: Vec<_> = match cutoff {
            Some(cutoff_time) => points.iter().filter(|p| p.ts >= cutoff_time).cloned().collect(),
            None => points.to_vec(), // No time filtering - return all points
        };
        if let Some(since) = since_cutoff {
            base.into_iter().filter(|p| p.ts > since).collect()
        } else {
            base
        }
    };

    let tps = ds(
        effective_filter(&history.tps),
        max_points,
        &mut downsample_any,
    );
    let qps = ds(
        effective_filter(&history.qps),
        max_points,
        &mut downsample_any,
    );
    let mean_latency_ms = ds(
        effective_filter(&history.mean_latency_ms),
        max_points,
        &mut downsample_any,
    );
    let latency_p95_ms = ds(
        effective_filter(&history.latency_p95_ms),
        max_points,
        &mut downsample_any,
    );
    let latency_p99_ms = ds(
        effective_filter(&history.latency_p99_ms),
        max_points,
        &mut downsample_any,
    );
    let connections = ds(
        effective_filter(&history.connections),
        max_points,
        &mut downsample_any,
    );
    let blocked_sessions = ds(
        effective_filter(&history.blocked_sessions),
        max_points,
        &mut downsample_any,
    );
    let wraparound_xid_pct = ds(
        effective_filter(&history.wraparound_xid_pct),
        max_points,
        &mut downsample_any,
    );
    let wraparound_mxid_pct = ds(
        effective_filter(&history.wraparound_mxid_pct),
        max_points,
        &mut downsample_any,
    );

    Ok(Json(OverviewHistoryResponse {
        tps,
        qps,
        mean_latency_ms,
        latency_p95_ms,
        latency_p99_ms,
        connections,
        blocked_sessions,
        wraparound_xid_pct,
        wraparound_mxid_pct,
        window: window.to_string(),
        downsampled: downsample_any,
        partial: since_cutoff.is_some(),
    }))
}

fn cutoff_for_window(window: &str) -> Result<Option<chrono::DateTime<chrono::Utc>>, ()> {
    let now = chrono::Utc::now();
    match window {
        "1h" => Ok(Some(now - chrono::Duration::hours(1))),
        "6h" => Ok(Some(now - chrono::Duration::hours(6))),
        "24h" => Ok(Some(now - chrono::Duration::hours(24))),
        "all" => Ok(None), // No time cutoff - return all available data
        _ => Err(()), // Invalid window
    }
}

pub fn maybe_downsample(
    points: Vec<crate::state::TimePoint>,
    max_points: usize,
) -> (Vec<crate::state::TimePoint>, bool) {
    if points.len() <= max_points || max_points == 0 {
        return (points, false);
    }
    let step = (points.len() as f64 / max_points as f64).ceil() as usize;
    if step <= 1 {
        return (points, false);
    }
    let mut sampled = Vec::with_capacity(max_points);
    for (idx, p) in points.into_iter().enumerate() {
        if idx % step == 0 {
            sampled.push(p);
        }
    }
    (sampled, true)
}

// Inline downsampling test moved to integration test under tests/history_downsampling.rs
