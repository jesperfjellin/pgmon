use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tracing::{info, warn};

const DEFAULT_CONFIG_PATH: &str = "/config/pgmon.yaml";

/// Top-level configuration for the pgmon agent.
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub cluster: String,
    #[serde(default)]
    pub dsn: String,
    #[serde(default)]
    pub sample_intervals: SampleIntervals,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub alerts: AlertThresholds,
    #[serde(default)]
    pub notifiers: Notifiers,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub ui: UiConfig,
    #[serde(default)]
    pub timeouts: StatementTimeouts,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            cluster: "local".into(),
            dsn: String::new(),
            sample_intervals: SampleIntervals::default(),
            limits: Limits::default(),
            alerts: AlertThresholds::default(),
            notifiers: Notifiers::default(),
            http: HttpConfig::default(),
            security: SecurityConfig::default(),
            ui: UiConfig::default(),
            timeouts: StatementTimeouts::default(),
        }
    }
}

/// Loop schedule configuration (with friendly duration parsing).
#[derive(Debug, Clone, Deserialize)]
pub struct SampleIntervals {
    /// "Hot path" loop (sessions, locks, autovacuum progress).
    #[serde(
        default = "SampleIntervals::default_hot_path",
        with = "humantime_serde"
    )]
    pub hot_path: Duration,
    /// Workload loop (queries, TPS, WAL).
    #[serde(
        default = "SampleIntervals::default_workload",
        with = "humantime_serde"
    )]
    pub workload: Duration,
    /// Storage loop (relation sizes, vacuum state).
    #[serde(default = "SampleIntervals::default_storage", with = "humantime_serde")]
    pub storage: Duration,
    /// Hourly loop (bloat, wraparound, stale stats).
    #[serde(default = "SampleIntervals::default_hourly", with = "humantime_serde")]
    pub hourly: Duration,
}

impl SampleIntervals {
    const fn default_hot_path() -> Duration {
        Duration::from_secs(15)
    }

    const fn default_workload() -> Duration {
        Duration::from_secs(60)
    }

    const fn default_storage() -> Duration {
        Duration::from_secs(600)
    }

    const fn default_hourly() -> Duration {
        Duration::from_secs(3600)
    }
}

impl Default for SampleIntervals {
    fn default() -> Self {
        Self {
            hot_path: Self::default_hot_path(),
            workload: Self::default_workload(),
            storage: Self::default_storage(),
            hourly: Self::default_hourly(),
        }
    }
}

/// Limits that keep metrics cardinality bounded.
#[derive(Debug, Clone, Deserialize)]
pub struct Limits {
    #[serde(default = "Limits::default_top_relations")]
    pub top_relations: u32,
    #[serde(default = "Limits::default_top_indexes")]
    pub top_indexes: u32,
    #[serde(default = "Limits::default_top_queries")]
    pub top_queries: u32,
    #[serde(default = "Limits::default_partition_horizon_days")]
    pub partition_horizon_days: u32,
}

impl Limits {
    const fn default_top_relations() -> u32 {
        50
    }

    const fn default_top_indexes() -> u32 {
        50
    }

    const fn default_top_queries() -> u32 {
        50
    }

    const fn default_partition_horizon_days() -> u32 {
        7
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            top_relations: Self::default_top_relations(),
            top_indexes: Self::default_top_indexes(),
            top_queries: Self::default_top_queries(),
            partition_horizon_days: Self::default_partition_horizon_days(),
        }
    }
}

/// Alerting thresholds in seconds / ratios, matching README guidance.
#[derive(Debug, Clone, Deserialize)]
pub struct AlertThresholds {
    #[serde(default = "AlertThresholds::default_connections_warn")]
    pub connections_warn: f64,
    #[serde(default = "AlertThresholds::default_connections_crit")]
    pub connections_crit: Option<f64>,
    #[serde(default = "AlertThresholds::default_long_txn_warn_s")]
    pub long_txn_warn_s: u64,
    #[serde(default = "AlertThresholds::default_long_txn_crit_s")]
    pub long_txn_crit_s: u64,
    #[serde(default = "AlertThresholds::default_repl_warn_s")]
    pub repl_warn_s: u64,
    #[serde(default = "AlertThresholds::default_repl_crit_s")]
    pub repl_crit_s: u64,
}

impl AlertThresholds {
    const fn default_connections_warn() -> f64 {
        0.8
    }

    const fn default_connections_crit() -> Option<f64> {
        Some(0.95)
    }

    const fn default_long_txn_warn_s() -> u64 {
        300
    }

    const fn default_long_txn_crit_s() -> u64 {
        1_800
    }

    const fn default_repl_warn_s() -> u64 {
        30
    }

    const fn default_repl_crit_s() -> u64 {
        300
    }
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            connections_warn: Self::default_connections_warn(),
            connections_crit: Self::default_connections_crit(),
            long_txn_warn_s: Self::default_long_txn_warn_s(),
            long_txn_crit_s: Self::default_long_txn_crit_s(),
            repl_warn_s: Self::default_repl_warn_s(),
            repl_crit_s: Self::default_repl_crit_s(),
        }
    }
}

/// Optional notifier configuration (Slack, email, etc).
#[derive(Debug, Clone, Deserialize)]
pub struct Notifiers {
    #[serde(default)]
    pub slack_webhook: Option<String>,
    #[serde(default)]
    pub email_smtp: Option<String>,
}

impl Default for Notifiers {
    fn default() -> Self {
        Self {
            slack_webhook: None,
            email_smtp: None,
        }
    }
}

/// HTTP listener configuration (bind address).
#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "HttpConfig::default_bind")]
    pub bind: String,
    #[serde(default = "HttpConfig::default_static_dir")]
    pub static_dir: String,
}

impl HttpConfig {
    fn default_bind() -> String {
        "0.0.0.0:8181".to_string()
    }

    fn default_static_dir() -> String {
        "frontend/dist".to_string()
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: Self::default_bind(),
            static_dir: Self::default_static_dir(),
        }
    }
}

fn default_true() -> bool {
    true
}

/// Security guardrails enforced by the agent.
#[derive(Debug, Clone, Deserialize)]
pub struct SecurityConfig {
    #[serde(default = "default_true")]
    pub read_only_enforce: bool,
    #[serde(default = "default_true")]
    pub redact_sql_text: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            read_only_enforce: true,
            redact_sql_text: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UiConfig {
    #[serde(default)]
    pub hide_postgres_defaults: bool,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            hide_postgres_defaults: false,
        }
    }
}

/// Postgres session timeouts.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementTimeouts {
    #[serde(default = "StatementTimeouts::default_statement_timeout_ms")]
    pub statement_timeout_ms: u64,
    #[serde(default = "StatementTimeouts::default_lock_timeout_ms")]
    pub lock_timeout_ms: u64,
}

impl StatementTimeouts {
    const fn default_statement_timeout_ms() -> u64 {
        3_000
    }

    const fn default_lock_timeout_ms() -> u64 {
        1_000
    }
}

impl Default for StatementTimeouts {
    fn default() -> Self {
        Self {
            statement_timeout_ms: Self::default_statement_timeout_ms(),
            lock_timeout_ms: Self::default_lock_timeout_ms(),
        }
    }
}

/// Load configuration from YAML disk file, falling back to defaults + env overrides.
pub fn load_config(path: Option<&Path>) -> Result<AppConfig> {
    let target_path = if let Some(path) = path {
        path.to_path_buf()
    } else if let Ok(env_path) = env::var("PGMON_CONFIG") {
        PathBuf::from(env_path)
    } else {
        PathBuf::from(DEFAULT_CONFIG_PATH)
    };

    let mut config = match try_parse_file(&target_path)? {
        Some(cfg) => {
            info!(path = %target_path.display(), "loaded configuration");
            cfg
        }
        None => {
            warn!(path = %target_path.display(), "config file not found; using built-in defaults");
            AppConfig::default()
        }
    };

    enforce_yaml_policy(&config)?;
    apply_env_overrides(&mut config)?;
    ensure_required_secrets(&config)?;
    Ok(config)
}

fn try_parse_file(path: &Path) -> Result<Option<AppConfig>> {
    match fs::read_to_string(path) {
        Ok(raw) => {
            let cfg = serde_yaml::from_str(&raw)
                .with_context(|| format!("failed to parse YAML config at {}", path.display()))?;
            Ok(Some(cfg))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => {
            Err(err).with_context(|| format!("failed to read config file at {}", path.display()))
        }
    }
}

fn enforce_yaml_policy(config: &AppConfig) -> Result<()> {
    if !config.dsn.trim().is_empty() {
        bail!(
            "Remove `dsn` from pgmon YAML config; set the Postgres connection string via the PGMON_DSN environment variable (see .env.sample)."
        );
    }
    Ok(())
}

fn apply_env_overrides(config: &mut AppConfig) -> Result<()> {
    if let Ok(cluster) = env::var("PGMON_CLUSTER") {
        if !cluster.is_empty() {
            config.cluster = cluster;
        }
    }

    match env::var("PGMON_DSN") {
        Ok(dsn) => {
            if dsn.trim().is_empty() {
                bail!(
                    "Environment variable PGMON_DSN is set but empty; populate it in your .env file."
                );
            }
            config.dsn = dsn;
        }
        Err(env::VarError::NotPresent) => {}
        Err(err) => return Err(err.into()),
    };

    Ok(())
}

fn ensure_required_secrets(config: &AppConfig) -> Result<()> {
    if config.dsn.trim().is_empty() {
        bail!(
            "Missing Postgres DSN. Set the PGMON_DSN environment variable (see .env.sample). Secrets must not be stored in YAML."
        );
    }
    Ok(())
}
