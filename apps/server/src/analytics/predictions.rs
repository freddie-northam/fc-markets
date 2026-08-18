//! What the system claimed, and whether it was right.
//!
//! Collecting prices proves nothing on its own. In six weeks there will be a
//! price history and still no evidence the signal works, unless the claims were
//! written down BEFORE their outcomes were knowable. Written down afterwards,
//! choosing the winners is indistinguishable from finding them.
//!
//! Three things make a score believable and all three are enforced here rather
//! than intended. The claim is immutable. The horizon is fixed when the claim is
//! made, so it cannot become a free parameter chosen once the answer is known.
//! And the outcome is DERIVED when asked rather than stored, so it cannot be
//! written down wrong or quietly corrected.
//!
//! The benchmark is the claim's own cohort, not zero. A pick that rose 5% in a
//! cohort that rose 8% lost, and a score against zero would call it a win.

use super::{cohort, trust_window, valuation};
use crate::ids::{GameId, MarketId};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::PgPool;
use sqlx::postgres::types::PgInterval;
use uuid::Uuid;

/// The claim this module makes: the card is cheap against comparable cards.
pub const UNDERVALUED: &str = "undervalued";

/// One recorded claim.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Prediction {
    pub id: Uuid,
    pub asset_id: Uuid,
    pub market_id: Uuid,
    pub claim: String,
    pub made_at: DateTime<Utc>,
    pub price: i64,
    pub value_ratio: Option<f64>,
    pub cohort_band: String,
    pub cohort_version: String,
}

/// How a matured claim turned out.
#[derive(Debug, Clone, Serialize)]
pub struct Score {
    pub prediction: Prediction,
    pub price_then: i64,
    pub price_now: i64,
    /// The card's own move over the horizon.
    pub asset_return: f64,
    /// What its cohort did over the same window. The benchmark, because beating
    /// zero is not the same as beating the market you were picking from.
    pub cohort_return: Option<f64>,
    /// Asset move less cohort move. The number that says whether the claim was
    /// worth making.
    pub excess_return: Option<f64>,
}

fn interval(d: Duration) -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: d.num_microseconds().unwrap_or(0),
    }
}

/// Records the cheapest cards in each cohort as claims.
///
/// Reads the class median AS OF `made_at`, so the ranking behind these picks can
/// be recomputed exactly. Nothing here consults the clock.
///
/// Re-running for the same instant records nothing new: the claim is keyed on
/// (market, asset, claim, made_at), so a retry after a failure cannot double
/// count and a second run cannot quietly improve yesterday's picks.
pub async fn record_picks(
    pool: &PgPool,
    game: GameId,
    market: MarketId,
    made_at: DateTime<Utc>,
    horizon: Duration,
    top_n: i64,
) -> Result<u64> {
    // A generous row cap: the ranking is filtered below, and asking for exactly
    // top_n would let a floored or ratio-less card consume a slot.
    let ranked = valuation::class_median(pool, market, top_n * 20, made_at).await?;

    let picks: Vec<&valuation::ClassValue> = ranked
        .iter()
        // A floored card is not cheap, it is floored. Ranking it as a pick would
        // fill the list with cards that cannot fall further.
        .filter(|v| !v.at_floor)
        // A null ratio means the class was too thin to say anything. Treating
        // that as cheap is the thin-class trap the valuation already refuses.
        .filter(|v| v.value_ratio.is_some())
        .take(top_n as usize)
        .collect();
    if picks.is_empty() {
        return Ok(0);
    }

    let mut written = 0u64;
    for pick in picks {
        written += sqlx::query(
            "INSERT INTO predictions
               (id, game_id, market_id, asset_id, claim, made_at, horizon,
                price, class_median, value_ratio, cohort_band, cohort_version)
             SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    rating_band(a.rating), a.version
               FROM assets a WHERE a.id = $4
             ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(game.0)
        .bind(market.0)
        .bind(pick.asset_id)
        .bind(UNDERVALUED)
        .bind(made_at)
        .bind(interval(horizon))
        .bind(pick.price)
        .bind(pick.median_price)
        .bind(pick.value_ratio)
        .execute(pool)
        .await?
        .rows_affected();
    }
    Ok(written)
}

/// Scores every claim whose horizon has passed, as of a stated time.
///
/// The outcome is computed here rather than read, so a claim cannot be marked
/// twice or marked differently on two days.
pub async fn score_matured(
    pool: &PgPool,
    game: GameId,
    as_of: DateTime<Utc>,
) -> Result<Vec<Score>> {
    let due: Vec<Prediction> = sqlx::query_as(
        "SELECT id, asset_id, market_id, claim, made_at, price, value_ratio,
                cohort_band, cohort_version
           FROM predictions
          WHERE game_id = $1 AND made_at + horizon <= $2
          ORDER BY made_at, id",
    )
    .bind(game.0)
    .bind(as_of)
    .fetch_all(pool)
    .await?;

    let mut scored = Vec::with_capacity(due.len());
    for prediction in due {
        // The price we can see now for the card we picked.
        let price_now: Option<i64> = sqlx::query_scalar(
            "SELECT price FROM trusted_latest_prices($1, $2)
              WHERE asset_id = $3 AND market_id = $4",
        )
        .bind(as_of)
        .bind(trust_window())
        .bind(prediction.asset_id)
        .bind(prediction.market_id)
        .fetch_optional(pool)
        .await?;
        // A card we can no longer read cannot be scored. Dropping it is honest;
        // scoring it as flat would count a blind spot as a result.
        let Some(price_now) = price_now else { continue };

        let cohort_return = cohort_move(
            pool,
            MarketId(prediction.market_id),
            &prediction.cohort_version,
            &prediction.cohort_band,
            prediction.made_at,
            as_of,
        )
        .await?;

        let asset_return = (price_now - prediction.price) as f64 / prediction.price as f64;
        scored.push(Score {
            price_then: prediction.price,
            price_now,
            asset_return,
            cohort_return,
            excess_return: cohort_return.map(|c| asset_return - c),
            prediction,
        });
    }
    Ok(scored)
}

/// What one cohort's median did between two instants.
async fn cohort_move(
    pool: &PgPool,
    market: MarketId,
    version: &str,
    band: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Option<f64>> {
    let at = |when| async move {
        let rows = cohort::snapshot(pool, market, when).await?;
        Ok::<_, anyhow::Error>(
            rows.into_iter()
                .find(|r| r.version == version && r.rating_band == band)
                .and_then(|r| r.median_price),
        )
    };
    let (before, after) = (at(from).await?, at(to).await?);
    Ok(match (before, after) {
        (Some(b), Some(a)) if b > 0.0 => Some((a - b) / b),
        _ => None,
    })
}
