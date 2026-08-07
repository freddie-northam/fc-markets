use crate::ids::MarketId;
use anyhow::Result;
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
pub const CLASS_MEDIAN_SQL: &str = r#"
WITH live AS (
    -- Freshness comes from the poll state, not from how old the price is. An
    -- old observed_at is ambiguous: the price held, or we have failed to read
    -- the card for weeks. Prices deflate, so a stale read is biased high and
    -- would manufacture false buy signals.
    SELECT asset_id
    FROM asset_poll_state
    WHERE market_id = $1
      AND consecutive_failures = 0
      AND last_polled_at > now() - INTERVAL '3 days'
),
latest AS (
    -- No time window on the observations. The table records changes only, so a
    -- card that has held one price for months has no recent row, and a window
    -- would drop the illiquid majority and bias every median upward.
    SELECT v.asset_id, o.price, o.observed_at, o.min_price
    FROM live v
    CROSS JOIN LATERAL (
        SELECT price, observed_at, min_price
        FROM market_observations
        WHERE market_id = $1 AND asset_id = v.asset_id
        -- source breaks the tie. Without it two providers at one instant make
        -- the same query return different answers on different days.
        ORDER BY observed_at DESC, source
        LIMIT 1
    ) o
),
buckets AS (
    SELECT a.rarity, a.version, a.rating,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY l.price) AS median_price,
           count(*) AS n
    FROM latest l
    JOIN assets a ON a.id = l.asset_id
    -- A card resting on EA's minimum listing price is not cheap, it is floored.
    -- Leaving it in drags the median down for every card measured against it.
    WHERE l.min_price IS NULL OR l.price > l.min_price
    -- version carries the promotional variant and is the single most important
    -- column here. Without it a promo card and a base card of the same rating
    -- share a class, they trade orders of magnitude apart, and the ratio stops
    -- measuring cheapness and starts measuring which promo a card belongs to.
    GROUP BY a.rarity, a.version, a.rating
)
SELECT a.id AS asset_id, a.name, l.price,
       b.median_price, b.n AS class_size,
       CASE WHEN b.n >= 5 THEN l.price::float8 / b.median_price END AS value_ratio,
       (l.min_price IS NOT NULL AND l.price <= l.min_price) AS at_floor
FROM latest l
JOIN assets a ON a.id = l.asset_id
-- LEFT JOIN, not INNER. An inner join with a count threshold deletes the asset
-- instead of its ratio, and the thin classes are exactly the Icons and top promo
-- cards that carry the volume.
LEFT JOIN buckets b
       ON b.rarity = a.rarity AND b.version = a.version AND b.rating = a.rating
ORDER BY at_floor, value_ratio NULLS LAST
LIMIT $2
"#;

pub async fn class_median(pool: &PgPool, market: MarketId, limit: i64) -> Result<Vec<ClassValue>> {
    Ok(sqlx::query_as::<_, ClassValue>(CLASS_MEDIAN_SQL)
        .bind(market.0)
        .bind(limit)
        .fetch_all(pool)
        .await?)
}
