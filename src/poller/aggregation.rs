use anyhow::Result;
use chrono::{Datelike, Utc};
use tracing::{info, instrument};

use crate::app::AppContext;

/// Background job that runs metric aggregation.
///
/// This poller runs frequently (every 15 minutes) to catch the UTC midnight rollover
/// and aggregate high-resolution metrics into daily and weekly summaries:
///
/// - **Daily aggregation**: Runs after midnight UTC to process yesterday's data
/// - **Weekly aggregation**: Runs on Mondays after midnight UTC to process last week's data
///
/// Both aggregation methods are idempotent, so running them multiple times is safe.
/// The actual logic will skip creating duplicates if summaries already exist.
#[instrument(skip_all)]
pub async fn run(ctx: &AppContext) -> Result<()> {
    let now = Utc::now();

    // Always attempt daily aggregation (idempotent - will skip if already done)
    let daily_count = ctx
        .state
        .replace_metric_history(|mh| mh.aggregate_to_daily(now))
        .await;

    if daily_count > 0 {
        info!(
            date = now.format("%Y-%m-%d").to_string(),
            summaries_created = daily_count,
            "completed daily metric aggregation"
        );

        // Trigger an immediate persistence flush after creating new daily summaries
        if let Some(persist_cfg) = &crate::persistence::PersistenceConfig::from_env() {
            if let Err(err) = crate::persistence::flush_once(persist_cfg, &ctx.state).await {
                tracing::error!(error=?err, "failed to flush after daily aggregation");
            }
        }
    }

    // Check if it's Monday to run weekly aggregation
    let is_monday = now.weekday() == chrono::Weekday::Mon;
    if is_monday {
        let weekly_count = ctx
            .state
            .replace_metric_history(|mh| mh.aggregate_to_weekly(now))
            .await;

        if weekly_count > 0 {
            info!(
                week = now.format("%Y-W%V").to_string(),
                summaries_created = weekly_count,
                "completed weekly metric aggregation"
            );

            // Trigger an immediate persistence flush after creating new weekly summaries
            if let Some(persist_cfg) = &crate::persistence::PersistenceConfig::from_env() {
                if let Err(err) = crate::persistence::flush_once(persist_cfg, &ctx.state).await {
                    tracing::error!(error=?err, "failed to flush after weekly aggregation");
                }
            }
        }
    }

    Ok(())
}
