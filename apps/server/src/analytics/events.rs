//! When the game did something.
//!
//! A price series says a cohort moved. It cannot say why. This records the
//! releases and promotions that move it, so a study can ask what happened in the
//! days around one.
//!
//! It is the only part of the ledger that cannot be backfilled. Prices we can
//! accumulate from today onwards; a promotion that ran last Thursday leaves no
//! trace in a price series saying so. Every day this does not exist is a day of
//! event studies that can never be run.
//!
//! Append only, like everything else here. A time we learn more precisely is a
//! NEW row superseding the old one, so a study run last week reproduces exactly.

use crate::ids::GameId;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

/// One thing the game did.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MarketEvent {
    pub id: Uuid,
    pub kind: String,
    pub name: String,
    /// When it happened in the game, never our clock.
    pub occurred_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
    pub note: Option<String>,
}

/// What to record.
#[derive(Debug, Clone, Deserialize)]
pub struct NewEvent {
    pub kind: String,
    pub name: String,
    pub occurred_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub note: Option<String>,
    /// The row this corrects, when a time is learned more precisely.
    pub supersedes: Option<Uuid>,
}

/// Records an event.
///
/// `kind` is folded to the canonical shape the database enforces, for the reason
/// `version` is: a study groups on it, and "TOTW" arriving beside "totw" would
/// split one series into two without raising anything.
///
/// A `supersedes` id arrives from the caller, so it is checked against this game
/// before it is written. Without that a row in one title could supersede an
/// event in another, and since a superseded event is hidden from every study,
/// the hiding would be permanent and silent.
pub async fn record(pool: &PgPool, game: GameId, event: &NewEvent) -> Result<MarketEvent> {
    let kind = event
        .kind
        .trim()
        .to_lowercase()
        .replace(char::is_whitespace, "_");

    // The supersedes id comes from the caller, so it is checked against this
    // game before it is written. Without this a row in one title could supersede
    // an event in another, and because a superseded event is hidden from every
    // study, that hiding would be permanent and silent.
    if let Some(superseded) = event.supersedes {
        let same_game: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM market_events WHERE id = $1 AND game_id = $2)",
        )
        .bind(superseded)
        .bind(game.0)
        .fetch_one(pool)
        .await?;
        if !same_game {
            anyhow::bail!("an event can only supersede one from the same game");
        }
    }
    Ok(sqlx::query_as::<_, MarketEvent>(
        "INSERT INTO market_events
           (id, game_id, kind, name, occurred_at, ended_at, supersedes, note)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         RETURNING id, kind, name, occurred_at, ended_at, recorded_at, note",
    )
    .bind(Uuid::now_v7())
    .bind(game.0)
    .bind(kind)
    .bind(event.name.trim())
    .bind(event.occurred_at)
    .bind(event.ended_at)
    .bind(event.supersedes)
    .bind(event.note.as_deref())
    .fetch_one(pool)
    .await?)
}

/// Every event a study would see AS OF a stated time.
///
/// Bounded by `recorded_at`, not by `occurred_at`, for the same reason the price
/// derivations are bounded by `ingested_at`: the question is what we KNEW then.
/// An event recorded yesterday about last Tuesday was not available to a study
/// run on Monday, and counting it is hindsight.
///
/// Superseded rows are excluded, so a corrected time replaces rather than
/// duplicates, while the row it replaced stays readable. The superseding row
/// must belong to the same game: the write path refuses anything else, and this
/// refuses to honour it even if a row got in another way. One door each side.
pub async fn known_at(
    pool: &PgPool,
    game: GameId,
    as_of: DateTime<Utc>,
) -> Result<Vec<MarketEvent>> {
    Ok(sqlx::query_as::<_, MarketEvent>(
        "SELECT e.id, e.kind, e.name, e.occurred_at, e.ended_at, e.recorded_at, e.note
           FROM market_events e
          WHERE e.game_id = $1
            AND e.recorded_at <= $2
            AND NOT EXISTS (
                SELECT 1 FROM market_events s
                 WHERE s.supersedes = e.id
                   AND s.game_id = e.game_id
                   AND s.recorded_at <= $2
            )
          ORDER BY e.occurred_at DESC, e.id",
    )
    .bind(game.0)
    .bind(as_of)
    .fetch_all(pool)
    .await?)
}
