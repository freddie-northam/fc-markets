//! The prediction log. Every test names a way a self-scoring system flatters
//! itself.

mod common;

use chrono::{Duration, Utc};
use common::*;
use fc_market::analytics::predictions::{self, UNDERVALUED};
use fc_market::ids::MarketId;

const HORIZON: Duration = Duration::days(7);

/// A card priced at a stated instant.
///
/// The instant is a parameter rather than something patched afterwards: an
/// observation cannot be moved between hypertable chunks by UPDATE, so a test
/// that backdates after inserting fails on a chunk constraint.
async fn seed_card(
    db: &TestDb,
    market: MarketId,
    name: &str,
    rating: i16,
    price: i64,
    at: chrono::DateTime<Utc>,
) -> anyhow::Result<fc_market::ids::AssetId> {
    let game = db.game().await?;
    let asset = seed_asset(&db.pool, game.id, name, rating, "icon").await?;
    seed_poll_state(&db.pool, asset, market, 0, Some(at)).await?;
    seed_observation(&db.pool, asset, market, "test", at, price, Some(200)).await?;
    // We learn a price when we poll for it, so a past question can see it.
    sqlx::query(
        "UPDATE market_observations SET ingested_at = $2
          WHERE asset_id = $1 AND observed_at = $2",
    )
    .bind(asset.0)
    .bind(at)
    .execute(&db.pool)
    .await?;
    Ok(asset)
}

/// Five cards of ONE class, so the median describes something.
///
/// The class key is (rarity, version, rating), and the valuation refuses a ratio
/// for a class thinner than five. Five different ratings would be five classes
/// of one and every ratio would be null, which is the thin-class rule working
/// rather than a bug.
async fn seed_class(
    db: &TestDb,
    market: MarketId,
    cheap: i64,
    at: chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    for (i, price) in [cheap, 900_000, 1_000_000, 1_100_000, 1_200_000]
        .into_iter()
        .enumerate()
    {
        seed_card(db, market, &format!("Card {i}"), 91, price, at).await?;
    }
    Ok(())
}

/// Reads a card again at a later instant.
///
/// A claim is only scoreable if the card is still readable at the horizon, which
/// is the trust window doing its job: a price nobody has re-read is not evidence.
async fn repoll(
    db: &TestDb,
    market: MarketId,
    name: &str,
    price: i64,
    at: chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    let asset: uuid::Uuid = sqlx::query_scalar("SELECT id FROM assets WHERE name = $1")
        .bind(name)
        .fetch_one(&db.pool)
        .await?;
    let asset = fc_market::ids::AssetId(asset);
    sqlx::query(
        "UPDATE asset_poll_state SET last_polled_at = $3 WHERE asset_id = $1 AND market_id = $2",
    )
    .bind(asset.0)
    .bind(market.0)
    .bind(at)
    .execute(&db.pool)
    .await?;
    seed_poll(&db.pool, asset, market, at, "written").await?;
    seed_observation(&db.pool, asset, market, "test", at, price, Some(200)).await?;
    sqlx::query(
        "UPDATE market_observations SET ingested_at = $2
          WHERE asset_id = $1 AND observed_at = $2",
    )
    .bind(asset.0)
    .bind(at)
    .execute(&db.pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn the_cheapest_card_in_a_class_is_recorded_as_a_claim() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    seed_class(&db, market, 300_000, Utc::now() - Duration::hours(2))
        .await
        .unwrap();

    let written = predictions::record_picks(&db.pool, game.id, market, Utc::now(), HORIZON, 2)
        .await
        .unwrap();
    assert!(written > 0, "a claim must be recorded");

    let cheapest: i64 = db
        .count("SELECT price FROM predictions ORDER BY value_ratio LIMIT 1")
        .await;
    assert_eq!(cheapest, 300_000, "the claim must be the cheapest card");
    db.cleanup().await;
}

/// A claim must never be improved after the fact. Re-running for the same
/// instant is a retry, not a second opinion, and double counting would inflate
/// any score computed over these rows.
#[tokio::test]
async fn recording_twice_for_one_instant_records_one_claim() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    seed_class(&db, market, 300_000, Utc::now() - Duration::hours(2))
        .await
        .unwrap();
    let made_at = Utc::now();

    predictions::record_picks(&db.pool, game.id, market, made_at, HORIZON, 2)
        .await
        .unwrap();
    let after_first = db.count("SELECT count(*) FROM predictions").await;
    predictions::record_picks(&db.pool, game.id, market, made_at, HORIZON, 2)
        .await
        .unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM predictions").await,
        after_first,
        "a retry must not double count"
    );
    db.cleanup().await;
}

/// A claim is not scoreable until its horizon has passed. Scoring early lets the
/// horizon be chosen once the answer is known, which is the ordinary way a
/// backtest lies.
#[tokio::test]
async fn a_claim_is_not_scored_before_its_horizon() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    let made_at = Utc::now() - Duration::days(8);
    seed_class(&db, market, 300_000, made_at).await.unwrap();
    predictions::record_picks(&db.pool, game.id, market, made_at, HORIZON, 2)
        .await
        .unwrap();
    // Read again recently, so the card is still evidence at the horizon.
    for i in 0..5 {
        repoll(
            &db,
            market,
            &format!("Card {i}"),
            400_000,
            Utc::now() - Duration::hours(2),
        )
        .await
        .unwrap();
    }

    let early = predictions::score_matured(&db.pool, game.id, made_at + Duration::days(3))
        .await
        .unwrap();
    assert!(early.is_empty(), "three days into a seven day horizon");

    let due = predictions::score_matured(&db.pool, game.id, Utc::now())
        .await
        .unwrap();
    assert!(!due.is_empty(), "and scoreable once the horizon passes");
    db.cleanup().await;
}

/// The benchmark is the cohort, not zero. A pick that rose while its whole
/// cohort rose further did not work, and scoring against zero would call it a
/// win.
#[tokio::test]
async fn a_pick_that_rose_less_than_its_cohort_scores_negative() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    let made_at = Utc::now() - Duration::days(8);
    seed_class(&db, market, 300_000, made_at).await.unwrap();
    predictions::record_picks(&db.pool, game.id, market, made_at, HORIZON, 1)
        .await
        .unwrap();

    // The cohort roughly doubles. The pick rises a tenth.
    let now = Utc::now() - Duration::hours(2);
    repoll(&db, market, "Card 0", 330_000, now).await.unwrap();
    for (i, price) in [1_800_000_i64, 2_000_000, 2_200_000, 2_400_000]
        .into_iter()
        .enumerate()
    {
        repoll(&db, market, &format!("Card {}", i + 1), price, now)
            .await
            .unwrap();
    }

    let scored = predictions::score_matured(&db.pool, game.id, Utc::now())
        .await
        .unwrap();
    let s = scored.first().expect("a matured claim");
    assert!(
        s.asset_return > 0.0,
        "the pick did rise: {}",
        s.asset_return
    );
    assert!(
        s.excess_return.is_some_and(|e| e < 0.0),
        "but it lost to its cohort, so the score must be negative: asset {:?} cohort {:?}",
        s.asset_return,
        s.cohort_return
    );
    db.cleanup().await;
}

/// A card resting on the floor cannot fall further and is not cheap. Ranking it
/// as a pick fills the list with cards that have nowhere to go.
#[tokio::test]
async fn a_floored_card_is_never_claimed_as_cheap() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    seed_class(&db, market, 900_000, Utc::now() - Duration::hours(2))
        .await
        .unwrap();
    // The cheapest card of all, and on the floor.
    seed_card(
        &db,
        market,
        "Floored",
        91,
        200,
        Utc::now() - Duration::hours(2),
    )
    .await
    .unwrap();

    predictions::record_picks(&db.pool, game.id, market, Utc::now(), HORIZON, 3)
        .await
        .unwrap();
    let floored: i64 = db
        .count("SELECT count(*) FROM predictions p JOIN assets a ON a.id = p.asset_id WHERE a.name = 'Floored'")
        .await;
    assert_eq!(floored, 0, "a floored card must not be claimed as cheap");
    db.cleanup().await;
}

/// A card we can no longer read cannot be scored. Counting it as flat would turn
/// a blind spot into a result.
#[tokio::test]
async fn a_claim_we_can_no_longer_read_is_dropped_rather_than_scored_flat() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    seed_class(&db, market, 300_000, Utc::now() - Duration::hours(2))
        .await
        .unwrap();
    predictions::record_picks(&db.pool, game.id, market, Utc::now(), HORIZON, 2)
        .await
        .unwrap();

    // Nothing is polled again, so nothing is readable at the horizon.
    let scored = predictions::score_matured(&db.pool, game.id, Utc::now() + Duration::days(30))
        .await
        .unwrap();
    assert!(
        scored.is_empty(),
        "an unreadable card must be dropped, not scored: {scored:?}"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn the_claim_records_the_evidence_that_produced_it() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    seed_class(&db, market, 300_000, Utc::now() - Duration::hours(2))
        .await
        .unwrap();
    predictions::record_picks(&db.pool, game.id, market, Utc::now(), HORIZON, 1)
        .await
        .unwrap();

    let row: (String, Option<f64>, Option<f64>, String) = sqlx::query_as(
        "SELECT claim, class_median, value_ratio, cohort_band FROM predictions LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(row.0, UNDERVALUED);
    assert!(row.1.is_some(), "the class median it was measured against");
    assert!(row.2.is_some(), "and the ratio that made it a pick");
    assert_eq!(row.3, "90+", "and the cohort it will be scored against");
    db.cleanup().await;
}
