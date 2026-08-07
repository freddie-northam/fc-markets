use crate::domain::Observation;
use crate::ids::{AssetId, MarketId, PollOutcome, RunId, RunStatus};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};
use uuid::Uuid;

/// One lock identifier for the whole ingest path. The serve loop and the CLI
/// share one entry point, so without this a slow run and a manual run spend the
/// request budget twice and race on poll state.
const INGEST_LOCK: i64 = 0x0FC_1_A5_5E7;

pub async fn connect(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(url)
        .await?)
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("../../migrations").run(pool).await?;
    Ok(())
}

/// Returns None when another run already holds the lock. The caller logs and
/// stops rather than waiting, because the next scheduled tick is the retry.
pub async fn try_lock(pool: &PgPool) -> Result<Option<sqlx::pool::PoolConnection<Postgres>>> {
    let mut conn = pool.acquire().await?;
    let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(INGEST_LOCK)
        .fetch_one(&mut *conn)
        .await?;
    Ok(if got { Some(conn) } else { None })
}

pub async fn open_run(pool: &PgPool, source: &str, parser_version: &str) -> Result<RunId> {
    let id = RunId::new();
    sqlx::query(
        "INSERT INTO ingest_runs (id, source, parser_version, heartbeat_at)
         VALUES ($1, $2, $3, now())",
    )
    .bind(id.0)
    .bind(source)
    .bind(parser_version)
    .execute(pool)
    .await?;
    Ok(id)
}

pub async fn close_run(
    pool: &PgPool,
    id: RunId,
    status: RunStatus,
    seen: i32,
    written: i32,
    rejected: i32,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "UPDATE ingest_runs
            SET finished_at = now(), status = $2, records_seen = $3,
                records_written = $4, records_rejected = $5, error = $6
          WHERE id = $1",
    )
    .bind(id.0)
    .bind(status.as_str())
    .bind(seen)
    .bind(written)
    .bind(rejected)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

/// A killed process leaves a run marked running for ever, which would make the
/// open-run count meaningless. The next start clears them.
pub async fn reap_abandoned_runs(pool: &PgPool) -> Result<u64> {
    let r = sqlx::query(
        "UPDATE ingest_runs
            SET status = 'abandoned', finished_at = now()
          WHERE status = 'running'
            AND heartbeat_at < now() - INTERVAL '1 hour'",
    )
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

pub struct DueAsset {
    pub asset_id: AssetId,
    pub market_id: MarketId,
    pub external_id: String,
    pub name: String,
}

pub async fn due_assets(
    pool: &PgPool,
    source: &str,
    limit: i64,
) -> Result<Vec<DueAsset>> {
    let rows: Vec<(Uuid, Uuid, String, String)> = sqlx::query_as(
        "SELECT s.asset_id, s.market_id, si.external_id, a.name
           FROM asset_poll_state s
           JOIN assets a ON a.id = s.asset_id
           JOIN asset_source_ids si
             ON si.asset_id = s.asset_id AND si.source = $1
          WHERE s.last_polled_at IS NULL
             OR s.last_polled_at + make_interval(secs => s.poll_interval_seconds) < now()
          ORDER BY s.last_polled_at NULLS FIRST
          LIMIT $2",
    )
    .bind(source)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(a, m, external_id, name)| DueAsset {
            asset_id: AssetId(a),
            market_id: MarketId(m),
            external_id,
            name,
        })
        .collect())
}

/// The observation insert, the poll rows and the poll state update are one
/// transaction. A kill between them would otherwise leave the coverage record
/// claiming we read a price that was never stored.
pub async fn write_batch(
    pool: &PgPool,
    observations: &[Observation],
    polls: &[(AssetId, MarketId, DateTime<Utc>, Option<DateTime<Utc>>, PollOutcome)],
    run_id: RunId,
) -> Result<u64> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;
    let mut written = 0u64;

    for o in observations {
        let r = sqlx::query(
            "INSERT INTO market_observations
               (asset_id, market_id, source, observed_at, price,
                min_price, max_price, source_ref, ingest_run_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
             ON CONFLICT DO NOTHING",
        )
        .bind(o.asset_id.0)
        .bind(o.market_id.0)
        .bind(o.source)
        .bind(o.observed_at)
        .bind(o.price)
        .bind(o.min_price)
        .bind(o.max_price)
        .bind(o.source_ref.as_deref())
        .bind(o.ingest_run_id.0)
        .execute(&mut *tx)
        .await?;
        written += r.rows_affected();
    }

    for (asset_id, market_id, polled_at, source_observed_at, outcome) in polls {
        sqlx::query(
            "INSERT INTO ingest_polls
               (asset_id, market_id, polled_at, source_observed_at, outcome, run_id)
             VALUES ($1,$2,$3,$4,$5,$6)
             ON CONFLICT DO NOTHING",
        )
        .bind(asset_id.0)
        .bind(market_id.0)
        .bind(polled_at)
        .bind(*source_observed_at)
        .bind(outcome.as_str())
        .bind(run_id.0)
        .execute(&mut *tx)
        .await?;

        // A read that produced a price clears the failure count. A rejection
        // increments it, which is what the valuation freshness gate reads.
        let failed = matches!(outcome, PollOutcome::Rejected | PollOutcome::NoPrice);
        sqlx::query(
            "UPDATE asset_poll_state
                SET last_polled_at = $3,
                    consecutive_failures = CASE WHEN $4 THEN consecutive_failures + 1 ELSE 0 END
              WHERE asset_id = $1 AND market_id = $2",
        )
        .bind(asset_id.0)
        .bind(market_id.0)
        .bind(polled_at)
        .bind(failed)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(written)
}

/// Health reads the poll table, never the observation table. The observation
/// table records changes only, so a quiet market writes nothing and would look
/// exactly like an outage.
pub async fn newest_poll_age_seconds(pool: &PgPool) -> Result<Option<f64>> {
    Ok(
        sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM (now() - max(polled_at))) FROM ingest_polls")
            .fetch_one(pool)
            .await?,
    )
}

pub async fn largest_poll_interval_seconds(pool: &PgPool) -> Result<i32> {
    Ok(
        sqlx::query_scalar("SELECT COALESCE(max(poll_interval_seconds), 14400) FROM asset_poll_state")
            .fetch_one(pool)
            .await?,
    )
}

/// Replay is the one deliberate exception to the append-only rule. Without it a
/// parser bug would be permanent, because ON CONFLICT DO NOTHING keeps whatever
/// was written first.
pub async fn delete_run_observations(pool: &PgPool, run_id: RunId) -> Result<u64> {
    let r = sqlx::query("DELETE FROM market_observations WHERE ingest_run_id = $1")
        .bind(run_id.0)
        .execute(pool)
        .await?;
    Ok(r.rows_affected())
}
