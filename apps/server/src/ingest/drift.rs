//! Section 4.9. Ingestion degrades quietly over long runs.
//!
//! A source that returns nulls, frozen values or a changed schema looks exactly
//! like a healthy source from the outside. Every check here turns one of those
//! silent failures into a `degraded` run, and section 4.9 makes a degraded run
//! withhold the heartbeat, so the same alarm that reports a dead host also
//! reports a dead feed.
//!
//! The thresholds are guesses made with no source available. Check them against
//! real data before they are fixed.

use crate::ids::RunId;
use anyhow::Result;
use sqlx::PgPool;

/// Above this share of price-bearing reads coming back empty, the feed is not
/// serving prices even though it is answering.
const MAX_NO_PRICE_SHARE: f64 = 0.5;

/// A source that reports one price for everything is frozen or is serving a
/// placeholder. Below this many distinct prices across a run of real size, treat
/// it as frozen.
const MIN_DISTINCT_PRICES: i64 = 3;

/// Under this many observations a run is too small for the distinct-price and
/// distribution checks to mean anything.
const MIN_SAMPLE: i64 = 20;

/// When not one asset anywhere in the feed has moved for this long, the provider
/// has stopped publishing. Across hundreds of covered assets, at least one price
/// changing inside a day is a very safe assumption; none changing is not a quiet
/// market, it is a dead one.
const FROZEN_AFTER_HOURS: f64 = 24.0;

/// A median asset moving by more than this factor between two runs is a unit or
/// schema change, not a market. Prices do move, so the bound is deliberately
/// loose.
const MAX_MEDIAN_MOVE: f64 = 3.0;

/// Every check that fired. An empty result means the run looks healthy.
pub async fn checks(
    pool: &PgPool,
    run_id: RunId,
    seen: i32,
    rejected_no_price: i32,
) -> Result<Vec<String>> {
    let mut failed = Vec::new();

    if seen > 0 {
        let share = f64::from(rejected_no_price) / f64::from(seen);
        if share > MAX_NO_PRICE_SHARE {
            failed.push(format!(
                "{:.0}% of records carried no price, limit {:.0}%",
                share * 100.0,
                MAX_NO_PRICE_SHARE * 100.0
            ));
        }
    }

    let (observations, distinct): (i64, i64) = sqlx::query_as(
        "SELECT count(*)::bigint, count(DISTINCT price)::bigint
           FROM market_observations WHERE ingest_run_id = $1",
    )
    .bind(run_id.0)
    .fetch_one(pool)
    .await?;

    if observations >= MIN_SAMPLE && distinct < MIN_DISTINCT_PRICES {
        failed.push(format!(
            "only {distinct} distinct prices across {observations} observations, the source looks frozen"
        ));
    }

    if let Some(reason) = frozen_feed(pool).await? {
        failed.push(reason);
    }

    if let Some(reason) = distribution_shift(pool, run_id, observations).await? {
        failed.push(reason);
    }

    Ok(failed)
}

/// Whether the feed as a whole has stopped advancing its source timestamps.
///
/// This is measured across the whole feed and across time, NOT as a per-run share
/// of assets that did not advance. That per-run form is a trap, and section 4.9
/// names it: a card that holds its price is behaving correctly, so a quiet run
/// legitimately advances nothing. A check built that way reads 100% stalled on a
/// perfectly healthy market, withholds the heartbeat, and sits red until somebody
/// stops reading it.
///
/// A dead provider looks different from a quiet market only over time and only in
/// aggregate: across every covered asset, nothing at all has moved for a day.
async fn frozen_feed(pool: &PgPool) -> Result<Option<String>> {
    let newest: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (now() - max(source_observed_at)))::float8 / 3600.0
           FROM ingest_polls
          WHERE source_observed_at IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;

    let Some(hours) = newest else {
        // Nothing has ever carried a source timestamp. A fresh install is not a
        // frozen feed.
        return Ok(None);
    };
    if hours > FROZEN_AFTER_HOURS {
        return Ok(Some(format!(
            "no asset anywhere in the feed has moved for {hours:.0} hours, limit {FROZEN_AFTER_HOURS:.0}"
        )));
    }
    Ok(None)
}

/// Compares this run's prices against each asset's previous price.
///
/// It deliberately does not compare this run's median against the last run's
/// median. Tiered polling means consecutive runs cover different assets, so those
/// two medians describe different populations and their ratio would be noise. The
/// median per-asset move is the same comparison done in a way that survives a
/// changing sample.
async fn distribution_shift(
    pool: &PgPool,
    run_id: RunId,
    observations: i64,
) -> Result<Option<String>> {
    if observations < MIN_SAMPLE {
        return Ok(None);
    }

    let median_move: Option<f64> = sqlx::query_scalar(
        "WITH current AS (
             SELECT asset_id, market_id, observed_at, price
               FROM market_observations
              WHERE ingest_run_id = $1
         ),
         moves AS (
             SELECT c.price::float8 / p.price::float8 AS ratio
               FROM current c
               CROSS JOIN LATERAL (
                   SELECT price
                     FROM market_observations o
                    WHERE o.asset_id = c.asset_id
                      AND o.market_id = c.market_id
                      AND o.observed_at < c.observed_at
                    ORDER BY o.observed_at DESC, o.source
                    LIMIT 1
               ) p
         )
         SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio) FROM moves",
    )
    .bind(run_id.0)
    .fetch_one(pool)
    .await?;

    let Some(ratio) = median_move else {
        // Nothing to compare against yet. A first run is not a drift.
        return Ok(None);
    };
    if !(1.0 / MAX_MEDIAN_MOVE..=MAX_MEDIAN_MOVE).contains(&ratio) {
        return Ok(Some(format!(
            "the median asset moved by a factor of {ratio:.2}, limit {MAX_MEDIAN_MOVE:.0}"
        )));
    }
    Ok(None)
}
