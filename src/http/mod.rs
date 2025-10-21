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
        .route("/alerts/history", get(get_alerts_history))
        .route("/history/:metric", get(get_history));

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

/// Returns high-resolution time series for a metric.
/// Query params:
///   ?window=24h | 1h | 6h (default 24h)
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
    let cutoff = cutoff_for_window(window).ok_or(StatusCode::BAD_REQUEST)?;
    let filtered: Vec<_> = all_points.into_iter().filter(|p| p.ts >= cutoff).collect();

    let (points, downsampled) = maybe_downsample(filtered, max_points);
    Ok(Json(HistoryResponse {
        metric,
        points,
        downsampled,
    }))
}

fn cutoff_for_window(window: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let now = chrono::Utc::now();
    match window {
        "1h" => Some(now - chrono::Duration::hours(1)),
        "6h" => Some(now - chrono::Duration::hours(6)),
        "24h" => Some(now - chrono::Duration::hours(24)),
        _ => None,
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
