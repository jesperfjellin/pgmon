use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::state::{AlertEvent, MetricHistory, SharedState, TimePoint};

#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    metric_history: PersistedMetricHistory,
    alert_events: Vec<AlertEvent>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedMetricHistory {
    tps: Vec<TimePoint>,
    qps: Vec<TimePoint>,
    mean_latency_ms: Vec<TimePoint>,
    latency_p95_ms: Vec<TimePoint>,
    latency_p99_ms: Vec<TimePoint>,
    wal_bytes_per_second: Vec<TimePoint>,
    temp_bytes_per_second: Vec<TimePoint>,
    blocked_sessions: Vec<TimePoint>,
    connections: Vec<TimePoint>,
}

impl From<&MetricHistory> for PersistedMetricHistory {
    fn from(m: &MetricHistory) -> Self {
        Self {
            tps: m.tps.clone(),
            qps: m.qps.clone(),
            mean_latency_ms: m.mean_latency_ms.clone(),
            latency_p95_ms: m.latency_p95_ms.clone(),
            latency_p99_ms: m.latency_p99_ms.clone(),
            wal_bytes_per_second: m.wal_bytes_per_second.clone(),
            temp_bytes_per_second: m.temp_bytes_per_second.clone(),
            blocked_sessions: m.blocked_sessions.clone(),
            connections: m.connections.clone(),
        }
    }
}

pub struct PersistenceConfig {
    pub data_dir: PathBuf,
    pub flush_interval: Duration,
}

impl PersistenceConfig {
    pub fn from_env() -> Option<Self> {
        let dir = std::env::var("PGMON_DATA_DIR").ok()?;
        let interval = std::env::var("PGMON_FLUSH_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60);
        Some(Self {
            data_dir: PathBuf::from(dir),
            flush_interval: Duration::from_secs(interval),
        })
    }
}

pub async fn load_if_exists(cfg: &PersistenceConfig, state: &SharedState) {
    let path = state_file_path(&cfg.data_dir);
    match tokio::fs::read(&path).await {
        Ok(bytes) => match serde_json::from_slice::<PersistedState>(&bytes) {
            Ok(persisted) => {
                restore_into_state(state, persisted).await;
                info!(file=?path, "loaded persisted state");
            }
            Err(err) => {
                warn!(error=?err, file=?path, "failed to parse persisted state JSON");
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            info!(file=?path, "no persisted state found (first run?)");
        }
        Err(err) => {
            warn!(error=?err, file=?path, "failed reading persisted state");
        }
    }
}

async fn restore_into_state(state: &SharedState, persisted: PersistedState) {
    // Replace metric history contents.
    state
        .replace_metric_history(|mh| {
            mh.tps = persisted.metric_history.tps;
            mh.qps = persisted.metric_history.qps;
            mh.mean_latency_ms = persisted.metric_history.mean_latency_ms;
            mh.latency_p95_ms = persisted.metric_history.latency_p95_ms;
            mh.latency_p99_ms = persisted.metric_history.latency_p99_ms;
            mh.wal_bytes_per_second = persisted.metric_history.wal_bytes_per_second;
            mh.temp_bytes_per_second = persisted.metric_history.temp_bytes_per_second;
            mh.blocked_sessions = persisted.metric_history.blocked_sessions;
            mh.connections = persisted.metric_history.connections;
        })
        .await;

    state.replace_alert_events(persisted.alert_events).await;
}

pub async fn spawn_flush_loop(
    cfg: PersistenceConfig,
    state: SharedState,
) -> tokio::task::JoinHandle<()> {
    info!(dir=?cfg.data_dir, interval=?cfg.flush_interval, "starting persistence flush loop");
    tokio::spawn(async move {
        loop {
            if let Err(err) = flush_once(&cfg, &state).await {
                error!(error=?err, "flush failed");
            }
            sleep(cfg.flush_interval).await;
        }
    })
}

async fn flush_once(cfg: &PersistenceConfig, state: &SharedState) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&cfg.data_dir).await?;
    let path = state_file_path(&cfg.data_dir);
    let metric_history = state.snapshot_metric_history().await;
    let alert_events = state.list_alert_events().await;
    let persisted = PersistedState {
        metric_history: (&metric_history).into(),
        alert_events,
    };
    let json = serde_json::to_vec_pretty(&persisted)?;
    // Atomic write: write to tmp then rename.
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, &json).await?;
    tokio::fs::rename(&tmp, &path).await?;
    info!(file=?path, size=json.len(), "flushed persisted state");
    Ok(())
}

fn state_file_path(dir: &Path) -> PathBuf {
    dir.join("state.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::TimePoint;
    use chrono::Utc;
    use tokio::runtime::Runtime;

    #[test]
    fn flush_and_load_round_trip() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let state = SharedState::new();
            // Seed some history points.
            // Directly mutate metric history for test seeding.
            state
                .replace_metric_history(|mh| {
                    mh.tps.push(TimePoint {
                        ts: Utc::now(),
                        value: 5.0,
                    });
                    mh.qps.push(TimePoint {
                        ts: Utc::now(),
                        value: 7.0,
                    });
                })
                .await;
            let dir_path = std::env::temp_dir().join("pgmon_persist_test");
            let cfg = PersistenceConfig {
                data_dir: dir_path,
                flush_interval: Duration::from_secs(1),
            };
            flush_once(&cfg, &state).await.expect("flush ok");
            // Create a new state instance and load.
            let new_state = SharedState::new();
            load_if_exists(&cfg, &new_state).await;
            let mh = new_state.snapshot_metric_history().await;
            assert_eq!(mh.tps.len(), 1);
            assert_eq!(mh.qps.len(), 1);
        });
    }
}
