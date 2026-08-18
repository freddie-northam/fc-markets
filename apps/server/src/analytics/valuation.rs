use crate::ids::MarketId;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ClassValue {
    pub asset_id: Uuid,
    pub name: String,
    pub price: i64,
    pub median_price: Option<f64>,
    pub class_size: Option<i64>,
    /// Null when the class holds too few cards to say anything. Null is the
    /// honest answer: a class of one returns the card's own price as the median
    /// and a ratio of exactly 1.0, which reads as signal and is not.
    pub value_ratio: Option<f64>,
    pub at_floor: bool,
}

/// The class median. Every clause below answers a specific way this can lie.
/// The class median. Every clause below answers a specific way this can lie.
///
/// Which prices are trustworthy is NOT decided here. That lives in the
/// `trusted_latest_prices` view, because the cohort index needs the same answer
/// and two definitions of trust would drift apart in silence.
pub const CLASS_MEDIAN_SQL: &str = r#"
WITH latest AS (
    SELECT asset_id, price, observed_at, min_price, at_floor
    FROM trusted_latest_prices($3)
    WHERE market_id = $1
),
buckets AS (
    SELECT a.rarity, a.version, a.rating,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY l.price) AS median_price,
           count(*) AS n
    FROM latest l
    JOIN assets a ON a.id = l.asset_id
    -- A floored card is not cheap. Leaving it in drags the median down for every
    -- card measured against it.
    WHERE NOT l.at_floor
    -- version carries the promotional variant and is the single most important
    -- column here. Without it a promo card and a base card of the same rating
    -- share a class, they trade orders of magnitude apart, and the ratio stops
    -- measuring cheapness and starts measuring which promo a card belongs to.
    GROUP BY a.rarity, a.version, a.rating
)
SELECT a.id AS asset_id, a.name, l.price,
       b.median_price, b.n AS class_size,
       CASE WHEN b.n >= 5 THEN l.price::float8 / b.median_price END AS value_ratio,
       l.at_floor
FROM latest l
JOIN assets a ON a.id = l.asset_id
-- LEFT JOIN, not INNER. An inner join with a count threshold deletes the asset
-- instead of its ratio, and the thin classes are exactly the Icons and top promo
-- cards that carry the volume.
LEFT JOIN buckets b
       ON b.rarity = a.rarity AND b.version = a.version AND b.rating = a.rating
-- name and id break the tie. Every thin class carries a null ratio, so those
-- rows all tie with each other, and an ORDER BY that does not settle them lets
-- the planner return them in any order it likes: the table reshuffles between
-- page loads, and a row cap keeps an arbitrary subset.
ORDER BY at_floor, value_ratio NULLS LAST, a.name, a.id
LIMIT $2
"#;

/// The class median as of a stated time.
///
/// `as_of` is a parameter and not `now()` so the same question always returns
/// the same answer. A ranking that shifts under a prediction cannot be used to
/// score it.
pub async fn class_median(
    pool: &PgPool,
    market: MarketId,
    limit: i64,
    as_of: DateTime<Utc>,
) -> Result<Vec<ClassValue>> {
    Ok(sqlx::query_as::<_, ClassValue>(CLASS_MEDIAN_SQL)
        .bind(market.0)
        .bind(limit)
        .bind(as_of)
        .fetch_all(pool)
        .await?)
}
