mod app;
mod config;
mod db;
mod http;
mod metrics;
mod persistence;
mod poller;
mod recommendations;
mod state;

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use dotenvy::Error as DotenvError;
use tokio::task::JoinHandle;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::app::AppContext;
use crate::persistence::{PersistenceConfig, flush_once, load_if_exists, spawn_flush_loop};

#[derive(Debug, Parser)]
#[command(author, version, about = "pgmon — PostgreSQL DBA Health Platform")]
struct Cli {
    /// Path to YAML configuration file. Defaults to env PGMON_CONFIG or built-in defaults.
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    init_tracing();

    let cli = Cli::parse();

    let config = config::load_config(cli.config.as_deref())?;
    let bind_addr: SocketAddr = config
        .http
        .bind
        .parse()
        .context("invalid http.bind address")?;

    let metrics = metrics::AppMetrics::new()?;
    let state = state::SharedState::new();
    let pool = db::create_pool(&config).await?;

    let ctx = AppContext::new(config, pool, metrics, state);

    // Persistence: load existing state then spawn flush loop if configured.
    if let Some(persist_cfg) = PersistenceConfig::from_env() {
        load_if_exists(&persist_cfg, &ctx.state).await;
        // Perform an immediate flush so state.json appears quickly and verify permissions.
        match flush_once(&persist_cfg, &ctx.state).await {
            Ok(_) => {
                tracing::info!(dir=?persist_cfg.data_dir, "initial persistence flush complete")
            }
            Err(err) => {
                tracing::error!(error=?err, dir=?persist_cfg.data_dir, "initial persistence flush failed")
            }
        }
        // Fire-and-forget background flush loop.
        let _flush_handle = spawn_flush_loop(persist_cfg, ctx.state.clone());
        // Schedule an early second flush (~20s) to capture the first 1-2 hot_path + workload samples
        // instead of waiting a full flush interval (default 60s). This improves observability right
        // after startup and helps verify that history recording works.
        let state_for_early = ctx.state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(20)).await;
            if let Some(cfg) = PersistenceConfig::from_env() {
                // re-read env in case unchanged
                if let Err(err) = flush_once(&cfg, &state_for_early).await {
                    tracing::warn!(error=?err, "early second persistence flush failed");
                } else {
                    tracing::info!("early second persistence flush complete");
                }
            }
        });
    } else {
        tracing::warn!("persistence disabled: PGMON_DATA_DIR not set");
    }

    ctx.state
        .update_overview_with(|overview| {
            overview.cluster = ctx.cluster_name().to_string();
        })
        .await;

    let poller_handles = poller::spawn_all(ctx.clone());
    let router = http::create_router(ctx.clone());

    info!("pgmon listening on {}", bind_addr);

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .context("failed to bind HTTP listener")?;

    if let Err(err) = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!(error = ?err, "server terminated with error");
    }

    shutdown_pollers(poller_handles).await;

    // Final persistence flush on shutdown so the latest workload & hot_path samples
    // are durable even if they arrived just before termination.
    if let Some(cfg) = PersistenceConfig::from_env() {
        if let Err(err) = flush_once(&cfg, &ctx.state).await {
            tracing::warn!(error=?err, "final persistence flush failed");
        } else {
            tracing::info!("final persistence flush complete");
        }
    }

    Ok(())
}

fn load_env() {
    if let Err(err) = dotenvy::dotenv() {
        match err {
            DotenvError::Io(io_err) if io_err.kind() == ErrorKind::NotFound => {}
            other => eprintln!("warning: failed to load .env file: {other}"),
        }
    }
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("pgmon=info,axum::rejection=trace"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            sigterm.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received");
}

async fn shutdown_pollers(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        handle.abort();
    }
}
