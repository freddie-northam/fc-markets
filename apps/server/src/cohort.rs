//! Cohorts: the market as bands rather than as cards.
//!
//! A trader does not ask what one card is worth. They ask whether 85+ rare golds
//! rise before content drops. The unit of analysis is therefore a band, and this
//! module aggregates the ledger into those bands.
//!
//! A cohort is `(version, rating band, market)`. Both columns are already stored
//! and both are already load bearing elsewhere: `version` decides the valuation
//! class, and the rating band is defined once in SQL by `rating_band`.
//!
//! Which prices count is NOT decided here. That is `trusted_latest_prices`,
//! shared with the class median, because two definitions of trust would drift
//! apart in silence and this is exactly the aggregate where a stale price does
//! the most damage: an unchanged illiquid price reads as a calm market rather
//! than an absent one.

use crate::ids::MarketId;
use anyhow::Result;
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

/// One band's current state.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct CohortSnapshot {
    pub market_id: Uuid,
    pub version: String,
    pub rating_band: String,
    /// Cards we hold a trusted price for. This is the population the median
    /// describes, and it is smaller than the band's true size whenever coverage
    /// is incomplete, which is why it is reported rather than implied.
    pub members: i64,
    /// Members resting on EA's minimum listing price. They are excluded from the
    /// median and counted here instead: a band that is mostly floored has no
    /// price signal, and a median over the remainder would hide that.
    pub floored: i64,
    pub median_price: Option<f64>,
    pub total_price: Option<i64>,
    /// The oldest trusted price in the band, in hours. A band whose freshest
    /// member is days old is not a quiet market, it is an unread one.
    pub stalest_hours: Option<f64>,
}

/// Current state of every cohort in one market.
///
/// Floored cards are counted but kept out of the median, for the reason the
/// class median gives: a card resting on the floor is not cheap, and including
/// it drags down every comparison made against it.
pub const COHORT_SNAPSHOT_SQL: &str = r#"
SELECT t.market_id,
       a.version,
       rating_band(a.rating) AS rating_band,
       count(*) AS members,
       count(*) FILTER (WHERE t.at_floor) AS floored,
       percentile_cont(0.5) WITHIN GROUP (
           ORDER BY CASE WHEN NOT t.at_floor THEN t.price END
       ) AS median_price,
       -- Cast, because sum() over bigint returns NUMERIC and sqlx will not
       -- decode that into i64. The same trap as EXTRACT(EPOCH ...) below.
       (sum(t.price) FILTER (WHERE NOT t.at_floor))::bigint AS total_price,
       max(EXTRACT(EPOCH FROM (now() - t.observed_at))::float8) / 3600.0 AS stalest_hours
FROM trusted_latest_prices t
JOIN assets a ON a.id = t.asset_id
WHERE t.market_id = $1
GROUP BY t.market_id, a.version, rating_band(a.rating)
-- Deterministic, so the same ledger renders the same table twice. Ordered by
-- size because a cohort of three says nothing and belongs at the bottom.
ORDER BY members DESC, a.version, rating_band(a.rating)
"#;

pub async fn snapshot(pool: &PgPool, market: MarketId) -> Result<Vec<CohortSnapshot>> {
    Ok(sqlx::query_as::<_, CohortSnapshot>(COHORT_SNAPSHOT_SQL)
        .bind(market.0)
        .fetch_all(pool)
        .await?)
}
