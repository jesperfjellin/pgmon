use anyhow::Result;
use sqlx::Row;
use tracing::instrument;

use crate::app::AppContext;
use crate::state::StorageEntry;

const DEEP_SAMPLE_SQL: &str = r#"
SELECT
    c.oid::regclass::text AS relation,
    c.relkind::text AS relkind,
    pg_total_relation_size(c.oid) AS total_bytes,
    pg_relation_size(c.oid) AS table_bytes,
    pg_indexes_size(c.oid) AS index_bytes,
    GREATEST(pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid), 0) AS toast_bytes,
    NULLIF(s.n_live_tup + s.n_dead_tup, 0)::double precision AS tuple_denominator,
    s.n_dead_tup AS dead_tuples,
    s.last_autovacuum,
    c.reltuples::double precision AS reltuples
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND c.relkind IN ('r','m')
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT $1
"#;

#[instrument(skip_all)]
pub async fn run(ctx: &AppContext) -> Result<()> {
    let limit = ctx.config.limits.top_relations as i64;
    let rows = sqlx::query(DEEP_SAMPLE_SQL)
        .bind(limit)
        .fetch_all(&ctx.pool)
        .await?;

    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        let relation: String = row.try_get("relation")?;
        let relkind: String = row.try_get("relkind")?;
        let total_bytes: i64 = row.try_get("total_bytes")?;
        let table_bytes: i64 = row.try_get("table_bytes")?;
        let index_bytes: i64 = row.try_get("index_bytes")?;
        let toast_bytes: i64 = row.try_get("toast_bytes")?;
        let dead_tuples: Option<i64> = row.try_get("dead_tuples")?;
        let tuple_denominator: Option<f64> = row.try_get("tuple_denominator")?;
        let last_autovacuum: Option<chrono::DateTime<chrono::Utc>> =
            row.try_get("last_autovacuum")?;
        let reltuples: Option<f64> = row.try_get("reltuples")?;

        let dead_ratio = match (dead_tuples, tuple_denominator) {
            (Some(dead), Some(total)) if total > 0.0 => Some(dead as f64 / total * 100.0),
            _ => None,
        };

        let dead_tuples_count = dead_tuples;

        entries.push(StorageEntry {
            relation,
            relkind,
            total_bytes,
            table_bytes,
            index_bytes,
            toast_bytes,
            dead_tuple_ratio: dead_ratio,
            last_autovacuum,
            reltuples,
            dead_tuples: dead_tuples_count,
        });
    }

    ctx.metrics
        .set_storage_metrics(ctx.cluster_name(), &entries);
    ctx.state.update_storage(entries).await;
    Ok(())
}
