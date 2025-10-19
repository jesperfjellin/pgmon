use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tracing::{error, info};

use crate::config::AppConfig;

pub type DbPool = PgPool;

/// Build a connection pool configured for read-only monitoring workloads.
pub async fn create_pool(config: &AppConfig) -> Result<DbPool> {
    let connect_options = PgConnectOptions::from_str(&config.dsn)
        .context("invalid Postgres DSN supplied")?
        .application_name("pgmon")
        .options([
            (
                "statement_timeout",
                config.timeouts.statement_timeout_ms.to_string(),
            ),
            ("lock_timeout", config.timeouts.lock_timeout_ms.to_string()),
        ]);

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
                    .execute(&mut *conn)
                    .await
                    .map_err(|err| {
                        error!(error = ?err, "failed to enforce read-only session");
                        err
                    })?;
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(&mut *conn)
                    .await
                    .map_err(|err| {
                        error!(error = ?err, "failed to enforce read-only default");
                        err
                    })?;
                sqlx::query("SET application_name = 'pgmon'")
                    .execute(&mut *conn)
                    .await
                    .map_err(|err| {
                        error!(error = ?err, "failed to set application_name");
                        err
                    })?;
                Ok(())
            })
        })
        .connect_with(connect_options)
        .await
        .context("failed to connect to postgres")?;

    if config.security.read_only_enforce {
        verify_read_only(&pool).await?;
    }

    info!("connected to cluster {}", config.cluster);
    Ok(pool)
}

/// Sanity-check the monitor role to ensure it cannot perform writes.
async fn verify_read_only(pool: &DbPool) -> Result<()> {
    let (default_read_only,): (bool,) =
        sqlx::query_as("SELECT current_setting('default_transaction_read_only') = 'on'")
            .fetch_one(pool)
            .await
            .context("pgmon requires default_transaction_read_only to be ON")?;

    if !default_read_only {
        anyhow::bail!("monitor role is not read-only; refused to start");
    }

    Ok(())
}
