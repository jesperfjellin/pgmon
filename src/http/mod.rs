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
        .route("/partitions", get(get_partitions))
        .route("/replication", get(get_replication))
        .route("/wraparound", get(get_wraparound));

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
