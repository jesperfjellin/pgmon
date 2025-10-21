use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::app::AppContext;

mod hot_path;
mod hourly;
mod storage;
mod util;
mod workload;

pub const HOT_PATH_LOOP: &str = "hot_path";
pub const WORKLOAD_LOOP: &str = "workload";
pub const STORAGE_LOOP: &str = "storage";
pub const HOURLY_LOOP: &str = "hourly";

type LoopFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type LoopFn = fn(AppContext) -> LoopFuture;

/// Spawn all poller loops and return their join handles.
pub fn spawn_all(ctx: AppContext) -> Vec<JoinHandle<()>> {
    let intervals = ctx.config.sample_intervals.clone();

    vec![
        spawn_loop(
            ctx.clone(),
            HOT_PATH_LOOP,
            intervals.hot_path,
            Duration::from_millis(900),
            poll_hot_path,
        ),
        spawn_loop(
            ctx.clone(),
            WORKLOAD_LOOP,
            intervals.workload,
            Duration::from_secs(3),
            poll_workload,
        ),
        spawn_loop(
            ctx.clone(),
            STORAGE_LOOP,
            intervals.storage,
            Duration::from_secs(10),
            poll_storage,
        ),
        spawn_loop(
            ctx,
            HOURLY_LOOP,
            intervals.hourly,
            Duration::from_secs(60),
            poll_hourly,
        ),
    ]
}

fn spawn_loop(
    ctx: AppContext,
    loop_name: &'static str,
    interval: Duration,
    budget: Duration,
    poll_fn: LoopFn,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            loop_name,
            interval = ?interval,
            budget = ?budget,
            "starting poller loop"
        );

        // tokio::time::interval() completes the first tick immediately,
        // ensuring all loops execute on startup before waiting for the interval
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            if let Err(err) = poll_once(ctx.clone(), loop_name, budget, poll_fn).await {
                error!(loop_name, error = ?err, "poller loop iteration failed");
            }
        }
    })
}

async fn poll_once(
    ctx: AppContext,
    loop_name: &'static str,
    budget: Duration,
    poll_fn: LoopFn,
) -> Result<()> {
    let start = Instant::now();
    match poll_fn(ctx.clone()).await {
        Ok(_) => {
            let elapsed = start.elapsed();
            ctx.metrics.observe_duration(loop_name, elapsed);
            if elapsed > budget {
                warn!(
                    loop_name,
                    elapsed = ?elapsed,
                    budget = ?budget,
                    "loop exceeded budget"
                );
            } else {
                info!(
                    loop_name,
                    elapsed = ?elapsed,
                    "loop completed successfully"
                );
            }
            ctx.metrics.record_success(loop_name, true);
            ctx.state.record_loop_success(loop_name).await;
            Ok(())
        }
        Err(err) => {
            ctx.metrics.record_success(loop_name, false);
            ctx.metrics.inc_error(loop_name);
            ctx.state
                .record_loop_failure(loop_name, err.to_string())
                .await;
            Err(err)
        }
    }
}

fn poll_hot_path(ctx: AppContext) -> LoopFuture {
    Box::pin(async move { hot_path::run(&ctx).await })
}

fn poll_workload(ctx: AppContext) -> LoopFuture {
    Box::pin(async move { workload::run(&ctx).await })
}

fn poll_storage(ctx: AppContext) -> LoopFuture {
    Box::pin(async move { storage::run(&ctx).await })
}

fn poll_hourly(ctx: AppContext) -> LoopFuture {
    Box::pin(async move { hourly::run(&ctx).await })
}
