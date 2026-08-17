use crate::domain::{AssetAttributes, Observation, Poll, PollResult};
use crate::ids::{AssetId, GameId, MarketId, Platform, PlayerId, PollOutcome, RunId, RunStatus};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};
use std::collections::HashSet;
use uuid::Uuid;

/// One lock identifier for the whole ingest path. The serve loop and the CLI
/// share one entry point, so without this a slow run and a manual run spend the
/// request budget twice and race on poll state.
const INGEST_LOCK: i64 = 0x0000_FC1A_55E7;

/// The interval assumed when no asset has one yet. It is the slowest tier, so a
/// health check falling back to it waits the longest before crying outage. One
/// definition, because the scheduler and the health check must agree on it. It
/// must stay equal to the slowest band of `poll_interval_seconds`.
pub const DEFAULT_POLL_INTERVAL_SECONDS: i32 = 1_209_600;

/// How long a run may go without a heartbeat before the next start declares it
/// dead. A run that dies leaves its status at `running` for ever, which makes the
/// open-run count meaningless.
const ABANDON_AFTER: &str = "1 hour";

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

/// Holds the ingest advisory lock for the length of a run.
///
/// It owns a dedicated connection rather than a pooled one. `pg_advisory_lock`
/// is scoped to a SESSION, and returning a pooled connection does not end its
/// session: the lock would survive the run and every later run in the same
/// process would find itself locked out for ever. Ending the connection ends the
/// session, so the lock goes even if the run panics.
pub struct IngestLock {
    conn: sqlx::PgConnection,
}

impl IngestLock {
    /// Releases the lock promptly. Dropping the guard also releases it, because
    /// the session dies with the connection, but that path is the panic
    /// backstop rather than the normal one.
    pub async fn release(self) {
        use sqlx::Connection;
        let _ = self.conn.close().await;
    }
}

/// Returns None when another run already holds the lock. The caller logs and
/// stops rather than waiting, because the next scheduled tick is the retry.
pub async fn try_lock(database_url: &str) -> Result<Option<IngestLock>> {
    use sqlx::Connection;

    let mut conn = sqlx::PgConnection::connect(database_url).await?;
    let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(INGEST_LOCK)
        .fetch_one(&mut conn)
        .await?;

    if !got {
        let _ = conn.close().await;
        return Ok(None);
    }
    Ok(Some(IngestLock { conn }))
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

pub struct Game {
    pub id: GameId,
    pub released_at: DateTime<Utc>,
}

/// Rule 2 needs `released_at` as its lower bound, so every run loads the game
/// before it validates anything.
pub async fn game_by_code(pool: &PgPool, code: &str) -> Result<Game> {
    let (id, released_at): (Uuid, DateTime<Utc>) =
        sqlx::query_as("SELECT id, released_at FROM games WHERE code = $1")
            .bind(code)
            .fetch_optional(pool)
            .await?
            .with_context(|| format!("no game with code {code}; run the migrations"))?;
    Ok(Game {
        id: GameId(id),
        released_at,
    })
}

pub async fn markets_for_game(pool: &PgPool, game: GameId) -> Result<Vec<(MarketId, Platform)>> {
    let rows: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT id, platform FROM markets WHERE game_id = $1 ORDER BY platform")
            .bind(game.0)
            .fetch_all(pool)
            .await?;
    rows.into_iter()
        .map(|(id, raw)| Ok((MarketId(id), platform(&raw)?)))
        .collect()
}

/// The `CHECK` constraint on `markets.platform` holds the closed list, so an
/// unknown value here means the constraint was dropped. Failing loudly beats
/// dropping the market and quietly halving our coverage.
fn platform(raw: &str) -> Result<Platform> {
    Platform::parse(raw).with_context(|| format!("markets holds an unknown platform: {raw}"))
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

/// Returns the run's own start alongside its identifier. Rule 2 bounds
/// timestamps against this value rather than the wall clock, so replaying one
/// archive twice gives the same table.
pub async fn open_run(
    pool: &PgPool,
    source: &str,
    parser_version: &str,
) -> Result<(RunId, DateTime<Utc>)> {
    let id = RunId::new();
    let started_at: DateTime<Utc> = sqlx::query_scalar(
        "INSERT INTO ingest_runs (id, source, parser_version, heartbeat_at)
         VALUES ($1, $2, $3, now())
         RETURNING started_at",
    )
    .bind(id.0)
    .bind(source)
    .bind(parser_version)
    .fetch_one(pool)
    .await?;
    Ok((id, started_at))
}

/// A long run must show that it is alive, or the reaper below would declare it
/// abandoned while it is still working.
pub async fn touch_heartbeat(pool: &PgPool, id: RunId) -> Result<()> {
    sqlx::query("UPDATE ingest_runs SET heartbeat_at = now() WHERE id = $1")
        .bind(id.0)
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RunCounts {
    pub seen: i32,
    pub written: i32,
    pub rejected: i32,
}

pub async fn close_run(
    pool: &PgPool,
    id: RunId,
    status: RunStatus,
    counts: RunCounts,
    error: Option<&str>,
    metadata: serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "UPDATE ingest_runs
            SET finished_at = now(), status = $2, records_seen = $3,
                records_written = $4, records_rejected = $5, error = $6,
                metadata = metadata || $7::jsonb
          WHERE id = $1",
    )
    .bind(id.0)
    .bind(status.as_str())
    .bind(counts.seen)
    .bind(counts.written)
    .bind(counts.rejected)
    .bind(error)
    .bind(metadata)
    .execute(pool)
    .await?;
    Ok(())
}

/// Records progress on the run row as it happens, not when the run closes.
///
/// Three subsystems read this metadata: replay reads `price_keys`, the daily
/// budget sums `requests`, and the cadence gates read the step flags. Writing it
/// only in `close_run` meant a run killed part way through, which is exactly the
/// case the reaper exists for, was reaped with an empty metadata object. Its
/// archived payloads then sat in the bucket unreplayable, its spend vanished so
/// the next run overspent the day, and its finished discovery was bought again.
///
/// One extra statement per batch and per step, against a per-run row.
pub async fn record_request(pool: &PgPool, run_id: RunId) -> Result<()> {
    sqlx::query(
        "UPDATE ingest_runs
            SET metadata = jsonb_set(metadata, '{requests}',
                  to_jsonb(COALESCE((metadata->>'requests')::bigint, 0) + 1))
          WHERE id = $1",
    )
    .bind(run_id.0)
    .execute(pool)
    .await?;
    Ok(())
}

/// Appends an archived payload's key the moment the object is stored, so the
/// archive and the record of it cannot disagree.
pub async fn record_price_key(pool: &PgPool, run_id: RunId, key: &str) -> Result<()> {
    sqlx::query(
        "UPDATE ingest_runs
            SET metadata = jsonb_set(metadata, '{price_keys}',
                  COALESCE(metadata->'price_keys', '[]'::jsonb) || to_jsonb($2::text))
          WHERE id = $1",
    )
    .bind(run_id.0)
    .bind(key)
    .execute(pool)
    .await?;
    Ok(())
}

/// Marks a slow step finished. The next run reads this to decide its cadence.
pub async fn record_step(pool: &PgPool, run_id: RunId, step: &str) -> Result<()> {
    sqlx::query(
        "UPDATE ingest_runs
            SET metadata = jsonb_set(metadata, ARRAY[$2], 'true'::jsonb)
          WHERE id = $1",
    )
    .bind(run_id.0)
    .bind(step)
    .execute(pool)
    .await?;
    Ok(())
}

/// A killed process leaves a run marked running for ever, which would make the
/// open-run count meaningless. The next start clears them.
pub async fn reap_abandoned_runs(pool: &PgPool) -> Result<u64> {
    // Bound, not interpolated. The value is a constant so nothing could be
    // injected through it, but this was the one query in the server built by
    // string formatting, and that is the shape somebody copies next time with a
    // value that did come from outside.
    let r = sqlx::query(
        "UPDATE ingest_runs
            SET status = 'abandoned', finished_at = now()
          WHERE status = 'running'
            AND heartbeat_at < now() - $1::interval",
    )
    .bind(ABANDON_AFTER)
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

/// The budget is daily, so it must count what every run has already spent today,
/// not what this run has spent. A per-run count would let twelve runs spend
/// twelve budgets.
pub async fn requests_spent_today(pool: &PgPool, source: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(sum((metadata->>'requests')::bigint), 0)::bigint
           FROM ingest_runs
          -- The day is a UTC day, stated explicitly. date_trunc on a bare now()
          -- truncates in the session TimeZone, so the budget window would move
          -- with the server's configuration rather than staying put.
          WHERE source = $1
            AND started_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'",
    )
    .bind(source)
    .fetch_one(pool)
    .await?)
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

/// The row shape both target queries select, before it becomes a `PollTarget`.
type TargetRow = (Uuid, Uuid, String, String, String);

fn to_targets(rows: Vec<TargetRow>) -> Result<Vec<PollTarget>> {
    rows.into_iter()
        .map(|(asset_id, market_id, external_id, raw, name)| {
            Ok(PollTarget {
                asset_id: AssetId(asset_id),
                market_id: MarketId(market_id),
                external_id,
                platform: platform(&raw)?,
                name,
            })
        })
        .collect()
}

/// One asset, one market, and the identifier the source knows it by.
pub struct PollTarget {
    pub asset_id: AssetId,
    pub market_id: MarketId,
    pub external_id: String,
    pub platform: Platform,
    pub name: String,
}

/// How far a failing asset's interval may stretch.
///
/// Section 4.5 has no retry policy: the next scheduled poll is the retry, and
/// `consecutive_failures` supplies the backoff. Without a cap a card that failed
/// for a week would stop being tried at all; without the backoff a permanently
/// broken card would spend full rate quota for ever and crowd out healthy ones.
const MAX_FAILURE_BACKOFF: i32 = 8;

/// The identifiers that are due, not the (asset, market) pairs.
///
/// A request returns every platform at once, so the budget is spent per
/// identifier. Selecting pairs here would count one request twice.
pub async fn due_external_ids(
    pool: &PgPool,
    source: &str,
    game: GameId,
    limit: i64,
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        // The LIMIT is applied OUTSIDE the DISTINCT ON. Inside it, the
        // last_polled_at term only breaks ties within one identifier, because
        // DISTINCT ON forces external_id to lead the sort. Capping there took the
        // alphabetically lowest identifiers, so on a day where discovery and the
        // metadata step had eaten the budget, a card unread for four hours lost
        // its slot to one that came due a second ago.
        "SELECT external_id FROM (
             SELECT DISTINCT ON (si.external_id) si.external_id, s.last_polled_at
               FROM asset_poll_state s
               JOIN assets a ON a.id = s.asset_id
               JOIN asset_source_ids si
                 ON si.asset_id = s.asset_id AND si.source = $1 AND si.game_id = a.game_id
              WHERE a.game_id = $2
                -- consecutive_failures stretches the interval. A card we cannot
                -- read is retried more slowly each time rather than spending full
                -- rate quota that a healthy card could use.
                AND (s.last_polled_at IS NULL
                     OR s.last_polled_at
                        + make_interval(secs => s.poll_interval_seconds
                                                * LEAST(s.consecutive_failures + 1, $4))
                        <= now())
              ORDER BY si.external_id, s.last_polled_at NULLS FIRST
         ) due
         ORDER BY due.last_polled_at NULLS FIRST
         LIMIT $3",
    )
    .bind(source)
    .bind(game.0)
    .bind(limit)
    .bind(MAX_FAILURE_BACKOFF)
    .fetch_all(pool)
    .await?)
}

/// Every (asset, market) pair behind the requested identifiers, due or not.
///
/// One response carries every platform. Recording only the pairs that were due
/// would throw away a price we have already paid for and would leave the
/// coverage record claiming we did not look.
pub async fn poll_targets_for(
    pool: &PgPool,
    source: &str,
    game: GameId,
    external_ids: &[String],
) -> Result<Vec<PollTarget>> {
    let rows: Vec<TargetRow> = sqlx::query_as(
        "SELECT s.asset_id, s.market_id, si.external_id, m.platform, a.name
           FROM asset_poll_state s
           JOIN assets  a ON a.id = s.asset_id
           JOIN markets m ON m.id = s.market_id
           JOIN asset_source_ids si
             ON si.asset_id = s.asset_id AND si.source = $1 AND si.game_id = a.game_id
          WHERE a.game_id = $2 AND si.external_id = ANY($3)",
    )
    .bind(source)
    .bind(game.0)
    .bind(external_ids)
    .fetch_all(pool)
    .await?;

    to_targets(rows)
}

// ---------------------------------------------------------------------------
// The write path
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, Copy)]
pub struct WriteCounts {
    pub written: u64,
    pub unchanged: u64,
}

/// The observation insert, the poll rows and the poll state update are one
/// transaction. A kill between them would otherwise leave the coverage record
/// claiming we read a price that was never stored.
///
/// `written` against `unchanged` is decided here rather than by the caller,
/// because only the idempotency index knows whether the price actually moved.
pub async fn write_polls(
    pool: &PgPool,
    polls: &[Poll],
    polled_at: DateTime<Utc>,
    run_id: RunId,
) -> Result<WriteCounts> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;
    let mut counts = WriteCounts::default();

    for poll in polls {
        let outcome = match &poll.result {
            PollResult::Failed(outcome) => *outcome,
            PollResult::Priced(observation) => {
                if insert_observation(&mut tx, observation, run_id).await? {
                    counts.written += 1;
                    PollOutcome::Written
                } else {
                    // The insert conflicted, so this exact price at this exact
                    // time is already recorded. The price did not move. That is
                    // coverage evidence, not a write.
                    counts.unchanged += 1;
                    PollOutcome::Unchanged
                }
            }
        };

        sqlx::query(
            "INSERT INTO ingest_polls
               (asset_id, market_id, polled_at, source_observed_at, outcome, run_id)
             VALUES ($1,$2,$3,$4,$5,$6)
             ON CONFLICT DO NOTHING",
        )
        .bind(poll.asset_id.0)
        .bind(poll.market_id.0)
        .bind(polled_at)
        .bind(poll.source_observed_at)
        .bind(outcome.as_str())
        .bind(run_id.0)
        .execute(&mut *tx)
        .await?;

        // A read that produced a price clears the failure count, whether or not
        // the price moved. Only a rejection increments it, and that count is what
        // the valuation freshness gate reads.
        let failed = matches!(outcome, PollOutcome::Rejected | PollOutcome::NoPrice);
        sqlx::query(
            "UPDATE asset_poll_state
                SET last_polled_at = $3,
                    consecutive_failures =
                        CASE WHEN $4 THEN consecutive_failures + 1 ELSE 0 END
              WHERE asset_id = $1 AND market_id = $2",
        )
        .bind(poll.asset_id.0)
        .bind(poll.market_id.0)
        .bind(polled_at)
        .bind(failed)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(counts)
}

/// True when the row was stored, false when the idempotency index rejected it.
async fn insert_observation(
    tx: &mut Transaction<'_, Postgres>,
    o: &Observation,
    run_id: RunId,
) -> Result<bool> {
    let stored: Option<i32> = sqlx::query_scalar(
        "INSERT INTO market_observations
           (asset_id, market_id, source, observed_at, price,
            min_price, max_price, source_ref, ingest_run_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT DO NOTHING
         RETURNING 1",
    )
    .bind(o.asset_id.0)
    .bind(o.market_id.0)
    .bind(o.source)
    .bind(o.observed_at)
    .bind(o.price)
    .bind(o.min_price)
    .bind(o.max_price)
    .bind(o.source_ref.as_deref())
    .bind(run_id.0)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(stored.is_some())
}

// ---------------------------------------------------------------------------
// Discovery and metadata
// ---------------------------------------------------------------------------

pub async fn known_external_ids(
    pool: &PgPool,
    source: &str,
    game: GameId,
) -> Result<HashSet<String>> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT external_id FROM asset_source_ids WHERE source = $1 AND game_id = $2",
    )
    .bind(source)
    .bind(game.0)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Creates the asset and its provider mapping.
///
/// The player row is a placeholder. Discovery knows a name and nothing else, and
/// the real footballer is only identifiable once the metadata step supplies
/// `ea_base_id`. `link_player` below repairs the link at that point.
pub async fn insert_discovered_asset(
    pool: &PgPool,
    game: GameId,
    source: &str,
    external_id: &str,
    name: &str,
) -> Result<AssetId> {
    let mut tx = pool.begin().await?;

    let player = PlayerId::new();
    sqlx::query("INSERT INTO players (id, name) VALUES ($1, $2)")
        .bind(player.0)
        .bind(name)
        .execute(&mut *tx)
        .await?;

    let asset = AssetId::new();
    sqlx::query("INSERT INTO assets (id, game_id, player_id, name) VALUES ($1,$2,$3,$4)")
        .bind(asset.0)
        .bind(game.0)
        .bind(player.0)
        .bind(name)
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "INSERT INTO asset_source_ids (asset_id, game_id, source, external_id)
         VALUES ($1,$2,$3,$4)",
    )
    .bind(asset.0)
    .bind(game.0)
    .bind(source)
    .bind(external_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(asset)
}

/// How long ago a slow step last ran, taken from the run records.
///
/// A separate table would hold one row per step and would need its own migration
/// for every new step. The run already records what it did.
pub async fn seconds_since_step(pool: &PgPool, source: &str, step: &str) -> Result<Option<f64>> {
    Ok(sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (now() - max(started_at)))::float8
           FROM ingest_runs
          WHERE source = $1
            AND status <> 'failed'
            AND (metadata->>$2)::boolean IS TRUE",
    )
    .bind(source)
    .bind(step)
    .fetch_one(pool)
    .await?)
}

pub struct CoveredAsset {
    pub id: AssetId,
    pub version: String,
    pub rating: Option<i16>,
}

/// The assets worth spending the request budget on, most valuable first.
///
/// A promotional card outranks a base card whatever its rating, for the reason in
/// section 4.2: pack supply accumulates for a base card and does not for a
/// promotional one, so the two trade orders of magnitude apart.
///
/// `id` breaks the final tie so that two runs over unchanged data pick the same
/// set. Without it the covered set would churn and assets would silently enter
/// and leave the rotation.
pub async fn most_valuable_assets(
    pool: &PgPool,
    game: GameId,
    limit: i64,
) -> Result<Vec<CoveredAsset>> {
    let rows: Vec<(Uuid, String, Option<i16>)> = sqlx::query_as(
        "SELECT id, version, rating
           FROM assets
          WHERE game_id = $1
          -- The base version is bound from BASE_VERSION, so this ranking and
          -- the tier expression in domain.rs cannot come to disagree about which
          -- version is the plain one. They stay two expressions on purpose: one
          -- ranks six hundred rows, which belongs in SQL, and one assigns an
          -- interval, which is unit tested in Rust. Only the value they share
          -- needs a single definition.
          ORDER BY (version <> $3) DESC, rating DESC NULLS LAST, id
          LIMIT $2",
    )
    .bind(game.0)
    .bind(limit)
    .bind(crate::domain::BASE_VERSION)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, version, rating)| CoveredAsset {
            id: AssetId(id),
            version,
            rating,
        })
        .collect())
}

/// Stops polling the assets that fell out of the covered set.
///
/// Only the schedule goes. The observations and the coverage record stay, because
/// the ledger is the asset, and the quota frees immediately.
pub async fn drop_poll_state_outside(pool: &PgPool, game: GameId, covered: &[Uuid]) -> Result<u64> {
    let r = sqlx::query(
        "DELETE FROM asset_poll_state s
          USING assets a
          WHERE a.id = s.asset_id
            AND a.game_id = $1
            AND NOT (s.asset_id = ANY($2))",
    )
    .bind(game.0)
    .bind(covered)
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

/// Section 4.7: discovery is the only thing that creates poll state rows.
///
/// One statement for the whole covered set. Coverage is reasserted on every run,
/// so a query per asset and market would be twelve hundred round trips every
/// fifteen minutes to restate rows that almost never change.
pub async fn ensure_poll_state(pool: &PgPool, rows: &[(AssetId, MarketId, i32)]) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let assets: Vec<Uuid> = rows.iter().map(|(a, _, _)| a.0).collect();
    let markets: Vec<Uuid> = rows.iter().map(|(_, m, _)| m.0).collect();
    let intervals: Vec<i32> = rows.iter().map(|(_, _, i)| *i).collect();

    let result = sqlx::query(
        "INSERT INTO asset_poll_state (asset_id, market_id, poll_interval_seconds)
         SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::int[])
         ON CONFLICT (asset_id, market_id) DO NOTHING",
    )
    .bind(&assets)
    .bind(&markets)
    .bind(&intervals)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Assets that have never had attributes, and assets whose attributes are older
/// than the metadata cadence.
///
/// Without this the valuation columns stay null and the terminal has nothing to
/// compute, whatever the price history holds.
pub async fn assets_needing_metadata(
    pool: &PgPool,
    source: &str,
    game: GameId,
    cadence_seconds: i64,
    limit: i64,
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT si.external_id
           FROM assets a
           JOIN asset_source_ids si
             ON si.asset_id = a.id AND si.source = $1 AND si.game_id = a.game_id
          WHERE a.game_id = $2
            AND (a.last_seen_at IS NULL
                 OR a.last_seen_at < now() - make_interval(secs => $3))
          ORDER BY a.last_seen_at NULLS FIRST
          LIMIT $4",
    )
    .bind(source)
    .bind(game.0)
    .bind(cadence_seconds as f64)
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

/// Returns the asset AND the name we currently hold for it.
///
/// The name comes back because rule 1 has to be applied here too: the caller
/// compares it against what the provider now claims before letting the provider
/// rewrite it.
pub async fn asset_by_external_id(
    pool: &PgPool,
    source: &str,
    game: GameId,
    external_id: &str,
) -> Result<Option<(AssetId, String)>> {
    let row: Option<(Uuid, String)> = sqlx::query_as(
        "SELECT si.asset_id, a.name
           FROM asset_source_ids si
           JOIN assets a ON a.id = si.asset_id
          WHERE si.source = $1 AND si.game_id = $2 AND si.external_id = $3",
    )
    .bind(source)
    .bind(game.0)
    .bind(external_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, name)| (AssetId(id), name)))
}

/// Writes the canonical attributes and re-tiers the asset.
///
/// The poll interval is recomputed here because the tier expression reads
/// `version` and `rating`, and both arrive with the metadata. Discovery creates
/// the row; this keeps its interval honest afterwards.
pub async fn update_asset_attributes(
    pool: &PgPool,
    asset: AssetId,
    attrs: &AssetAttributes,
    interval_seconds: i32,
) -> Result<()> {
    let mut tx = pool.begin().await?;

    sqlx::query(
        "UPDATE assets
            SET name = $2, rating = $3, rarity = $4, version = $5, position = $6,
                league_id = $7, nation_id = $8, club_id = $9,
                skill_moves = $10, weak_foot = $11, pace = $12, shooting = $13,
                passing = $14, dribbling = $15, defending = $16, physicality = $17,
                playstyle_count = $18, last_seen_at = now()
          WHERE id = $1",
    )
    .bind(asset.0)
    .bind(&attrs.name)
    .bind(attrs.rating)
    .bind(attrs.rarity.as_str())
    .bind(&attrs.version)
    .bind(attrs.position.as_deref())
    .bind(attrs.league_id)
    .bind(attrs.nation_id)
    .bind(attrs.club_id)
    .bind(attrs.skill_moves)
    .bind(attrs.weak_foot)
    .bind(attrs.pace)
    .bind(attrs.shooting)
    .bind(attrs.passing)
    .bind(attrs.dribbling)
    .bind(attrs.defending)
    .bind(attrs.physicality)
    .bind(attrs.playstyle_count)
    .execute(&mut *tx)
    .await?;

    sqlx::query("UPDATE asset_poll_state SET poll_interval_seconds = $2 WHERE asset_id = $1")
        .bind(asset.0)
        .bind(interval_seconds)
        .execute(&mut *tx)
        .await?;

    if let Some(ea_base_id) = attrs.ea_base_id {
        link_player(&mut tx, asset, &attrs.name, ea_base_id).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Points the asset at the real footballer.
///
/// Discovery gives every asset its own placeholder player, so a base card and a
/// promotional card of one footballer start apart. `ea_base_id` is the first
/// thing that can join them, and it only arrives with the metadata.
async fn link_player(
    tx: &mut Transaction<'_, Postgres>,
    asset: AssetId,
    name: &str,
    ea_base_id: i64,
) -> Result<()> {
    let player: Uuid = sqlx::query_scalar(
        "INSERT INTO players (id, name, ea_base_id) VALUES ($1,$2,$3)
         ON CONFLICT (ea_base_id) DO UPDATE SET name = EXCLUDED.name
         RETURNING id",
    )
    .bind(PlayerId::new().0)
    .bind(name)
    .bind(ea_base_id)
    .fetch_one(&mut **tx)
    .await?;

    let previous: Uuid = sqlx::query_scalar("SELECT player_id FROM assets WHERE id = $1")
        .bind(asset.0)
        .fetch_one(&mut **tx)
        .await?;

    sqlx::query("UPDATE assets SET player_id = $2 WHERE id = $1")
        .bind(asset.0)
        .bind(player)
        .execute(&mut **tx)
        .await?;

    // The placeholder is rubbish once nothing points at it, and leaving it would
    // make the player table a count of cards rather than of footballers.
    if previous != player {
        sqlx::query(
            "DELETE FROM players p
              WHERE p.id = $1 AND p.ea_base_id IS NULL
                AND NOT EXISTS (SELECT 1 FROM assets a WHERE a.player_id = p.id)",
        )
        .bind(previous)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

/// How many bytes this database holds on its volume.
///
/// It counts the database and the write ahead log, which are the two things
/// that grow without bound. It cannot see anything else on the volume, so the
/// figure is a floor and the caller takes it from a stated capacity.
///
/// This exists because a platform that gives each service its own volume stops
/// the server from measuring the database's disk directly.
pub async fn database_bytes(pool: &PgPool) -> Result<i64> {
    let database: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await?;
    // pg_ls_waldir needs pg_monitor. A role without it still gets a useful
    // number from the database size alone, so this must not fail the check.
    let wal: i64 = sqlx::query_scalar("SELECT coalesce(sum(size), 0)::bigint FROM pg_ls_waldir()")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    Ok(database.saturating_add(wal))
}

/// Health reads the poll table, never the observation table. The observation
/// table records changes only, so a quiet market writes nothing and would look
/// exactly like an outage.
pub async fn newest_poll_age_seconds(pool: &PgPool) -> Result<Option<f64>> {
    Ok(sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (now() - max(polled_at)))::float8 FROM ingest_polls",
    )
    .fetch_one(pool)
    .await?)
}

pub async fn largest_poll_interval_seconds(pool: &PgPool) -> Result<i32> {
    Ok(
        sqlx::query_scalar("SELECT COALESCE(max(poll_interval_seconds), $1) FROM asset_poll_state")
            .bind(DEFAULT_POLL_INTERVAL_SECONDS)
            .fetch_one(pool)
            .await?,
    )
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

/// Swaps one run's observations for a freshly parsed set, in one transaction.
///
/// Replay is the one deliberate exception to the append-only rule. Without it a
/// parser bug would be permanent, because ON CONFLICT DO NOTHING keeps whatever
/// was written first.
///
/// The delete and the rewrite are one unit on purpose. A delete that committed
/// before a failed rewrite would destroy exactly the rows the replay set out to
/// correct, and the caller would be left with a gap it could only fill by
/// getting the archive read to work.
pub async fn replace_run_observations(
    pool: &PgPool,
    original: RunId,
    replacement: RunId,
    observations: &[Observation],
) -> Result<(u64, u64)> {
    let mut tx = pool.begin().await?;

    let deleted = sqlx::query("DELETE FROM market_observations WHERE ingest_run_id = $1")
        .bind(original.0)
        .execute(&mut *tx)
        .await?
        .rows_affected();

    let mut written = 0;
    for o in observations {
        if insert_observation(&mut tx, o, replacement).await? {
            written += 1;
        }
    }

    // A replay that resolves nothing must not be allowed to erase what it was
    // meant to correct. This is the only path in the system that deletes ledger
    // rows, and the two ways to reach it are ordinary: a parser change that
    // rejects the whole payload, and an asset mapping that no longer resolves
    // the identifiers the run recorded. Both would commit an empty rewrite and
    // report success.
    //
    // Returning before the commit rolls the delete back. An operator who really
    // does mean to drop a run's observations can do it deliberately; they should
    // not get there by replaying.
    if deleted > 0 && written == 0 {
        anyhow::bail!(
            "replay resolved nothing from run {original}, so it would delete {deleted} \
             observations and write none; refusing to erase a history while correcting it"
        );
    }

    tx.commit().await?;
    Ok((deleted, written))
}

/// Identity resolution on its own, with no reference to the schedule.
///
/// Section 4.4: one indexed lookup of `(game_id, source, external_id)` returns the
/// asset, and there is no fuzzy match. Replay uses this rather than
/// `poll_targets_for`, because an asset that has left the covered set still has a
/// history and that history must still be correctable.
pub async fn resolve_targets(
    pool: &PgPool,
    source: &str,
    game: GameId,
    external_ids: &[String],
) -> Result<Vec<PollTarget>> {
    let rows: Vec<TargetRow> = sqlx::query_as(
        "SELECT si.asset_id, m.id, si.external_id, m.platform, a.name
           FROM asset_source_ids si
           JOIN assets  a ON a.id = si.asset_id
           JOIN markets m ON m.game_id = si.game_id
          WHERE si.source = $1 AND si.game_id = $2 AND si.external_id = ANY($3)",
    )
    .bind(source)
    .bind(game.0)
    .bind(external_ids)
    .fetch_all(pool)
    .await?;

    to_targets(rows)
}

/// What replay needs about the run it re-parses: when it started, and which
/// payloads it archived.
///
/// One row, one query. Rule 2 bounds timestamps against the ORIGINAL start, so
/// these two facts are only ever wanted together.
pub struct ReplaySource {
    pub started_at: DateTime<Utc>,
    /// Which provider wrote the run. Replay must re-parse with the same one:
    /// handing run A's payloads to source B's parser rewrites A's history under
    /// a parser that never saw that shape, silently.
    pub source: String,
    pub keys: Vec<String>,
}

pub async fn run_replay_source(pool: &PgPool, run_id: RunId) -> Result<Option<ReplaySource>> {
    let row: Option<(DateTime<Utc>, String, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT started_at, source, metadata->'price_keys' FROM ingest_runs WHERE id = $1",
    )
    .bind(run_id.0)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(started_at, source, keys)| ReplaySource {
        started_at,
        source,
        keys: keys
            .and_then(|v| serde_json::from_value::<Vec<String>>(v).ok())
            .unwrap_or_default(),
    }))
}
