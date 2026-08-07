//! Section 5.1. Each test here answers one specific way the class median can lie.

mod common;

use chrono::{Duration, Utc};
use common::*;
use fc_market::ids::{MarketId, Platform};
use fc_market::valuation::{self, ClassValue};

const PS: Platform = Platform::Playstation;

/// A live asset: polled recently, with no failures, holding one price.
async fn live_card(
    db: &TestDb,
    market: MarketId,
    name: &str,
    rating: i16,
    version: &str,
    price: i64,
    min_price: Option<i64>,
) -> fc_market::ids::AssetId {
    let game = db.game().await.unwrap();
    let asset = seed_asset(&db.pool, game.id, name, rating, version)
        .await
        .unwrap();
    seed_poll_state(&db.pool, asset, market, 0, Some(Utc::now()))
        .await
        .unwrap();
    seed_observation(
        &db.pool,
        asset,
        market,
        "test",
        Utc::now() - Duration::hours(2),
        price,
        min_price,
    )
    .await
    .unwrap();
    asset
}

async fn value(db: &TestDb, market: MarketId) -> Vec<ClassValue> {
    valuation::class_median(&db.pool, market, 5_000)
        .await
        .unwrap()
}

fn row<'a>(rows: &'a [ClassValue], name: &str) -> &'a ClassValue {
    rows.iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("{name} is missing from the valuation"))
}

/// A class of six, one of which is deliberately cheap. The ratio must be the
/// same number every time the query runs over the same rows.
#[tokio::test]
async fn the_class_median_ratio_is_stable_and_reproducible_on_fixed_input() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();

    for (name, price) in [
        ("Cheap", 9_000),
        ("A", 15_000),
        ("B", 17_000),
        ("C", 18_000),
        ("D", 19_000),
        ("E", 21_000),
    ] {
        live_card(&db, market, name, 84, "base", price, Some(200)).await;
    }

    let first = value(&db, market).await;
    let second = value(&db, market).await;

    let cheap = row(&first, "Cheap");
    assert_eq!(cheap.class_size, Some(6));
    assert_eq!(cheap.median_price, Some(17_500.0));
    assert_eq!(cheap.value_ratio, Some(9_000.0 / 17_500.0));
    assert_eq!(
        first.iter().map(|r| r.name.clone()).collect::<Vec<_>>(),
        second.iter().map(|r| r.name.clone()).collect::<Vec<_>>(),
        "the same rows must give the same order"
    );
    assert_eq!(first[0].name, "Cheap", "the cheapest for its class leads");
    db.cleanup().await;
}

/// Without the `source` tiebreak PostgreSQL returns an arbitrary row once two
/// providers hold the same instant, and the query answers differently on
/// different days.
#[tokio::test]
async fn two_sources_at_one_timestamp_give_a_deterministic_result() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();

    let asset = seed_asset(&db.pool, game.id, "Contested", 84, "base")
        .await
        .unwrap();
    seed_poll_state(&db.pool, asset, market, 0, Some(Utc::now()))
        .await
        .unwrap();

    let at = Utc::now() - Duration::hours(1);
    // Inserted in the order that would make an unordered query pick the wrong one.
    seed_observation(&db.pool, asset, market, "zulu", at, 50_000, Some(200))
        .await
        .unwrap();
    seed_observation(&db.pool, asset, market, "alpha", at, 10_000, Some(200))
        .await
        .unwrap();

    for _ in 0..5 {
        let rows = value(&db, market).await;
        assert_eq!(
            row(&rows, "Contested").price,
            10_000,
            "the source tiebreak must settle it the same way every time"
        );
    }
    db.cleanup().await;
}

/// The single most important line in the query. An 89 rated Team of the Season
/// card and an 89 rated base rare share a rating and a rarity, and late in a
/// cycle they trade orders of magnitude apart.
#[tokio::test]
async fn a_promotional_card_and_a_base_card_of_one_rating_fall_in_separate_classes() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();

    for (name, price) in [("B1", 9_000), ("B2", 10_000), ("B3", 11_000)] {
        live_card(&db, market, name, 89, "base", price, Some(200)).await;
    }
    for (name, price) in [("P1", 900_000), ("P2", 1_000_000), ("P3", 1_100_000)] {
        live_card(&db, market, name, 89, "tots", price, Some(200)).await;
    }

    let rows = value(&db, market).await;
    assert_eq!(row(&rows, "B2").median_price, Some(10_000.0));
    assert_eq!(row(&rows, "P2").median_price, Some(1_000_000.0));
    assert_ne!(
        row(&rows, "B2").median_price,
        row(&rows, "P2").median_price,
        "pooling these two would describe neither"
    );
    db.cleanup().await;
}

/// A class of one returns the card's own price as the median and a ratio of
/// exactly 1.0, which reads as signal and is not. Null is the honest answer, and
/// the asset must still appear.
#[tokio::test]
async fn an_asset_in_a_thin_class_appears_with_a_null_ratio_never_one() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();

    live_card(&db, market, "Alone", 94, "icon", 1_400_000, Some(200)).await;

    let rows = value(&db, market).await;
    let alone = row(&rows, "Alone");
    assert_eq!(alone.class_size, Some(1));
    assert_eq!(
        alone.value_ratio, None,
        "a thin class must not manufacture a ratio of 1.0"
    );
    assert!(
        alone.median_price.is_some(),
        "the reader still gets the numbers behind the null"
    );
    db.cleanup().await;
}

/// The table records changes only, so a card that has held one price for months
/// has no recent row. A time window would drop the illiquid majority and bias
/// every remaining median upward.
#[tokio::test]
async fn an_asset_whose_last_price_is_months_old_still_appears() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();

    let asset = seed_asset(&db.pool, game.id, "Illiquid", 84, "base")
        .await
        .unwrap();
    // Polled minutes ago, priced four months ago. That is a healthy, quiet card.
    seed_poll_state(&db.pool, asset, market, 0, Some(Utc::now()))
        .await
        .unwrap();
    seed_observation(
        &db.pool,
        asset,
        market,
        "test",
        Utc::now() - Duration::days(120),
        12_000,
        Some(200),
    )
    .await
    .unwrap();

    let rows = value(&db, market).await;
    assert_eq!(row(&rows, "Illiquid").price, 12_000);
    db.cleanup().await;
}

/// Freshness comes from the poll state, not from the observation age. A dead
/// feed keeps voting otherwise, and because prices deflate across a cycle its
/// stale value is biased high and manufactures false buy signals.
#[tokio::test]
async fn an_asset_with_failing_polls_is_excluded_and_its_price_leaves_the_median() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();

    for (name, price) in [("A", 10_000), ("B", 12_000), ("C", 14_000)] {
        live_card(&db, market, name, 84, "base", price, Some(200)).await;
    }

    // A card we have failed to read. Its last known price is far above the class.
    let dead = seed_asset(&db.pool, game.id, "Dead feed", 84, "base")
        .await
        .unwrap();
    seed_poll_state(&db.pool, dead, market, 4, Some(Utc::now()))
        .await
        .unwrap();
    seed_observation(
        &db.pool,
        dead,
        market,
        "test",
        Utc::now() - Duration::hours(3),
        900_000,
        Some(200),
    )
    .await
    .unwrap();

    let rows = value(&db, market).await;
    assert!(
        !rows.iter().any(|r| r.name == "Dead feed"),
        "an asset we cannot read must not appear"
    );
    assert_eq!(
        row(&rows, "B").median_price,
        Some(12_000.0),
        "its stale price must leave the median too"
    );
    db.cleanup().await;
}

/// An asset that has not been polled for days is equally excluded. The gate is
/// the poll state, and staleness there is exactly the ambiguity section 4.3
/// exists to resolve.
#[tokio::test]
async fn an_asset_that_has_not_been_polled_for_days_is_excluded() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();

    let asset = seed_asset(&db.pool, game.id, "Forgotten", 84, "base")
        .await
        .unwrap();
    seed_poll_state(
        &db.pool,
        asset,
        market,
        0,
        Some(Utc::now() - Duration::days(5)),
    )
    .await
    .unwrap();
    seed_observation(
        &db.pool,
        asset,
        market,
        "test",
        Utc::now() - Duration::days(5),
        12_000,
        Some(200),
    )
    .await
    .unwrap();

    assert!(value(&db, market).await.is_empty());
    db.cleanup().await;
}

/// EA sets a minimum listing price. A card resting on it holds the lowest ratio
/// in its class by construction, so an ascending sort would fill the head of the
/// product's main table with cards that are not cheap, only floored.
#[tokio::test]
async fn a_card_at_its_minimum_price_does_not_lead_the_cheap_list() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();

    for (name, price) in [
        ("Cheap", 9_000),
        ("A", 15_000),
        ("B", 17_000),
        ("C", 18_000),
        ("D", 19_000),
        ("E", 21_000),
    ] {
        live_card(&db, market, name, 84, "base", price, Some(200)).await;
    }
    live_card(&db, market, "Floored", 84, "base", 200, Some(200)).await;

    let rows = value(&db, market).await;
    let floored = row(&rows, "Floored");

    assert!(floored.at_floor);
    assert_eq!(
        rows[0].name, "Cheap",
        "a floored card must not lead the table"
    );
    assert_eq!(
        rows.last().unwrap().name,
        "Floored",
        "it sorts last instead"
    );
    // It is also kept out of the fit, or it would drag the median down for every
    // card measured against it.
    assert_eq!(row(&rows, "B").median_price, Some(17_500.0));
    assert_eq!(row(&rows, "B").class_size, Some(6));
    db.cleanup().await;
}

/// One asset trades independently on each platform, so a valuation is always
/// asked for one market.
#[tokio::test]
async fn a_valuation_covers_one_market_only() {
    let db = TestDb::new().await.unwrap();
    let playstation = db.market(Platform::Playstation).await.unwrap();
    let pc = db.market(Platform::Pc).await.unwrap();

    live_card(
        &db,
        playstation,
        "Only on PlayStation",
        84,
        "base",
        9_000,
        Some(200),
    )
    .await;

    assert_eq!(value(&db, playstation).await.len(), 1);
    assert!(value(&db, pc).await.is_empty());
    db.cleanup().await;
}
