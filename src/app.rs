use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::DbPool;
use crate::metrics::AppMetrics;
use crate::state::SharedState;

/// Shared application context passed to HTTP handlers and pollers.
#[derive(Clone)]
pub struct AppContext {
    pub config: Arc<AppConfig>,
    pub pool: DbPool,
    pub metrics: AppMetrics,
    pub state: SharedState,
}

impl AppContext {
    pub fn new(config: AppConfig, pool: DbPool, metrics: AppMetrics, state: SharedState) -> Self {
        Self {
            config: Arc::new(config),
            pool,
            metrics,
            state,
        }
    }

    pub fn cluster_name(&self) -> &str {
        &self.config.cluster
    }
}
