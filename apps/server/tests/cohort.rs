//! Cohorts: the market as bands. Every test names a way a band aggregate lies.

mod common;

use chrono::{Duration, Utc};
use common::*;
use fc_market::cohort;
use fc_market::ids::MarketId;

/// One cohort member. Defaults so a test states only what it is testing.
struct Member<'a> {
    name: &'a str,
    rating: i16,
    version: &'a str,
    price: i64,
    min_price: i64,
    polled_hours_ago: i64,
}

impl Default for Member<'_> {
    fn default() -> Self {
        Self {
            name: "Card",
            rating: 84,
            version: "base",
            price: 10_000,
            min_price: 200,
            polled_hours_ago: 1,
        }
    }
}

/// Seeds a member end to end, so the shared view's own rules apply rather than
/// being bypassed by a hand built row.
async fn seed_member(db: &TestDb, market: MarketId, m: Member<'_>) -> anyhow::Result<()> {
    let game = db.game().await?;
    let asset = seed_asset(&db.pool, game.id, m.name, m.rating, m.version).await?;
    let at = Utc::now() - Duration::hours(m.polled_hours_ago);
    seed_poll_state(&db.pool, asset, market, 0, Some(at)).await?;
    seed_observation(
        &db.pool,
        asset,
        market,
        "test",
        at,
        m.price,
        Some(m.min_price),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn cards_group_into_bands_by_rating_and_version() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    for (name, rating, price) in [("A", 91, 800_000), ("B", 93, 1_200_000), ("C", 90, 400_000)] {
        seed_member(
            &db,
            market,
            Member {
                name,
                rating,
                version: "icon",
                price,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    // A different band, and a different version, so neither may merge with it.
    seed_member(
        &db,
        market,
        Member {
            name: "D",
            rating: 87,
            version: "icon",
            price: 90_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    seed_member(
        &db,
        market,
        Member {
            name: "E",
            rating: 91,
            price: 30_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let rows = cohort::snapshot(&db.pool, market).await.unwrap();
    let icon_top = rows
        .iter()
        .find(|r| r.version == "icon" && r.rating_band == "90+")
        .expect("the 90+ icon band");
    assert_eq!(icon_top.members, 3);
    assert_eq!(
        icon_top.median_price,
        Some(800_000.0),
        "the median of 400k, 800k and 1.2m"
    );
    assert!(
        rows.iter()
            .any(|r| r.version == "base" && r.rating_band == "90+"),
        "a base card must not join the icon band at the same rating"
    );
    assert!(
        rows.iter()
            .any(|r| r.version == "icon" && r.rating_band == "86-89"),
        "87 belongs to its own band"
    );
    db.cleanup().await;
}

/// The finding that prompted this module. A card resting on EA's minimum is not
/// a cheap card, and a band mostly on the floor has no price signal at all.
/// Averaging it in reports a low number as though it were a market.
#[tokio::test]
async fn floored_cards_are_counted_but_kept_out_of_the_median() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    seed_member(
        &db,
        market,
        Member {
            name: "Traded",
            rating: 76,
            price: 5_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    seed_member(
        &db,
        market,
        Member {
            name: "Traded2",
            rating: 77,
            price: 7_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // At the floor: price equals the minimum.
    for (i, name) in ["Floor1", "Floor2", "Floor3"].iter().enumerate() {
        seed_member(
            &db,
            market,
            Member {
                name,
                rating: 75 + i as i16,
                price: 200,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    let band = cohort::snapshot(&db.pool, market)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.rating_band == "75-79")
        .expect("the 75-79 band");
    assert_eq!(band.members, 5, "every card is a member");
    assert_eq!(band.floored, 3, "and the floored ones are visible");
    assert_eq!(
        band.median_price,
        Some(6_000.0),
        "the median must describe the two traded cards, not the floor"
    );
    db.cleanup().await;
}

/// An unchanged illiquid price reads as a calm market rather than an absent one.
/// The band must therefore state how stale its worst member is.
#[tokio::test]
async fn a_band_reports_how_stale_its_worst_member_is() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    seed_member(
        &db,
        market,
        Member {
            name: "Fresh",
            rating: 91,
            version: "icon",
            price: 500_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    seed_member(
        &db,
        market,
        Member {
            name: "Stale",
            rating: 92,
            version: "icon",
            price: 900_000,
            polled_hours_ago: 40,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let band = cohort::snapshot(&db.pool, market)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.rating_band == "90+")
        .expect("the 90+ band");
    let hours = band.stalest_hours.expect("a staleness figure");
    assert!(
        (39.0..41.0).contains(&hours),
        "the worst member is 40 hours old, got {hours:.1}"
    );
    db.cleanup().await;
}

/// Freshness comes from the poll state, not the observation age. A card we have
/// failed to read for a week must leave the cohort entirely, because its last
/// price is biased high and would lift the whole band.
#[tokio::test]
async fn a_card_we_have_stopped_reading_leaves_the_cohort() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    seed_member(
        &db,
        market,
        Member {
            name: "Read",
            rating: 91,
            version: "icon",
            price: 500_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    seed_member(
        &db,
        market,
        Member {
            name: "Abandoned",
            rating: 92,
            version: "icon",
            price: 9_000_000,
            polled_hours_ago: 24 * 7,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let band = cohort::snapshot(&db.pool, market)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.rating_band == "90+")
        .expect("the 90+ band");
    assert_eq!(band.members, 1, "the abandoned card must not be a member");
    assert_eq!(band.median_price, Some(500_000.0));
    db.cleanup().await;
}

/// A card trades independently on each platform, so a cohort that mixed markets
/// would be describing no market at all.
#[tokio::test]
async fn a_cohort_covers_one_market_only() {
    let db = TestDb::new().await.unwrap();
    let playstation = db.market(PS).await.unwrap();
    let pc = db.market(PC).await.unwrap();
    seed_member(
        &db,
        playstation,
        Member {
            name: "OnPS",
            rating: 91,
            version: "icon",
            price: 500_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    seed_member(
        &db,
        pc,
        Member {
            name: "OnPC",
            rating: 91,
            version: "icon",
            price: 300_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let ps_band = cohort::snapshot(&db.pool, playstation)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.rating_band == "90+")
        .unwrap();
    assert_eq!(ps_band.members, 1);
    assert_eq!(ps_band.median_price, Some(500_000.0));
    db.cleanup().await;
}

/// An asset discovered before its attributes has no rating. It must be visible
/// as unrated rather than silently joining the lowest band, which would make
/// that band's median a mixture of cheap cards and unknowns.
#[tokio::test]
async fn an_unrated_asset_forms_its_own_band() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    let asset = seed_asset(&db.pool, game.id, "Unknown", 0, "base")
        .await
        .unwrap();
    sqlx::query("UPDATE assets SET rating = NULL WHERE id = $1")
        .bind(asset.0)
        .execute(&db.pool)
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
        Utc::now() - Duration::hours(1),
        1_000,
        Some(200),
    )
    .await
    .unwrap();

    let rows = cohort::snapshot(&db.pool, market).await.unwrap();
    assert!(
        rows.iter().any(|r| r.rating_band == "unrated"),
        "an unrated card must be visible, not folded into under-75: {rows:?}"
    );
    db.cleanup().await;
}
