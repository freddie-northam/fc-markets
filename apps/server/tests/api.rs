//! Sections 6 and 4.9: the read path, the health signal and the heartbeat.

mod common;

use chrono::{Duration, Utc};
use common::*;
use fc_market::config::Config;
use fc_market::heartbeat;
use fc_market::ids::{Platform, RunStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Serves the real router on a loopback port. Testing the handlers through HTTP
/// is what makes the status codes part of the assertion.
struct Server {
    base: String,
    handle: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(db: &TestDb, config: Config) -> Self {
        let app = fc_market::api::router(db.pool.clone(), config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { base, handle }
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        reqwest::get(format!("{}{path}", self.base)).await.unwrap()
    }

    fn stop(self) {
        self.handle.abort();
    }
}

#[tokio::test]
async fn observations_return_in_chronological_order_and_respect_the_row_cap() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    let asset = seed_asset(&db.pool, game.id, "Marco Verratti", 84, "base")
        .await
        .unwrap();
    seed_poll_state(&db.pool, asset, market, 0, Some(Utc::now()))
        .await
        .unwrap();

    // Inserted newest first, so an unordered query would return them backwards.
    for hours in [1_i64, 5, 9, 13, 17] {
        seed_observation(
            &db.pool,
            asset,
            market,
            "test",
            Utc::now() - Duration::hours(hours),
            10_000 + hours,
            Some(200),
        )
        .await
        .unwrap();
    }

    let mut config = db.config();
    // Without a bound, one request for an old tier A asset serialises hundreds of
    // thousands of rows on the host that also runs ingestion.
    config.api_row_cap = 3;
    let server = Server::start(&db, config).await;

    let rows: Vec<serde_json::Value> = server
        .get(&format!("/assets/{}/prices?market={}", asset.0, market.0))
        .await
        .json()
        .await
        .unwrap();

    assert_eq!(rows.len(), 3, "the server side row cap must apply");
    let times: Vec<&str> = rows
        .iter()
        .map(|r| r["observed_at"].as_str().unwrap())
        .collect();
    let mut sorted = times.clone();
    sorted.sort();
    assert_eq!(times, sorted, "observations must come back oldest first");

    // The cap must take from the NEWEST end. Seeded at 1, 5, 9, 13 and 17 hours
    // ago with price 10_000 + hours, so the three newest are 10_001, 10_005 and
    // 10_009. Capping an ascending scan would have returned the three OLDEST
    // instead, and the caller's "latest price" would be the cap boundary. The
    // count and the ordering hold either way, which is why this assertion is the
    // one that catches it.
    let mut prices: Vec<i64> = rows.iter().map(|r| r["price"].as_i64().unwrap()).collect();
    prices.sort();
    assert_eq!(
        prices,
        vec![10_001, 10_005, 10_009],
        "the cap must drop the oldest rows, never the newest"
    );
    server.stop();
    db.cleanup().await;
}

/// A card trades independently on each platform, so a series that mixes two
/// markets is not a price history.
#[tokio::test]
async fn a_price_series_can_be_confined_to_one_market() {
    let db = TestDb::new().await.unwrap();
    let playstation = db.market(Platform::Playstation).await.unwrap();
    let pc = db.market(Platform::Pc).await.unwrap();
    let game = db.game().await.unwrap();
    let asset = seed_asset(&db.pool, game.id, "Marco Verratti", 84, "base")
        .await
        .unwrap();

    let at = Utc::now() - Duration::hours(2);
    seed_observation(&db.pool, asset, playstation, "test", at, 9_000, Some(200))
        .await
        .unwrap();
    seed_observation(&db.pool, asset, pc, "test", at, 8_700, Some(200))
        .await
        .unwrap();

    let server = Server::start(&db, db.config()).await;

    let both: Vec<serde_json::Value> = server
        .get(&format!("/assets/{}/prices", asset.0))
        .await
        .json()
        .await
        .unwrap();
    let one: Vec<serde_json::Value> = server
        .get(&format!(
            "/assets/{}/prices?market={}",
            asset.0, playstation.0
        ))
        .await
        .json()
        .await
        .unwrap();

    assert_eq!(both.len(), 2);
    assert!(
        both.iter().all(|r| r["market_id"].is_string()),
        "an unfiltered series must say which market each point belongs to"
    );
    assert_eq!(one.len(), 1);
    assert_eq!(one[0]["price"], 9_000);
    server.stop();
    db.cleanup().await;
}

#[tokio::test]
async fn an_unknown_asset_is_a_404_and_a_known_one_is_a_200() {
    let db = TestDb::new().await.unwrap();
    let game = db.game().await.unwrap();
    let asset = seed_asset(&db.pool, game.id, "Marco Verratti", 84, "base")
        .await
        .unwrap();
    let server = Server::start(&db, db.config()).await;

    assert_eq!(
        server.get(&format!("/assets/{}", asset.0)).await.status(),
        200
    );
    assert_eq!(
        server
            .get("/assets/00000000-0000-4000-8000-000000000000")
            .await
            .status(),
        404
    );
    server.stop();
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_stopped_ledger_makes_health_return_503() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    let asset = seed_asset(&db.pool, game.id, "Marco Verratti", 84, "base")
        .await
        .unwrap();
    seed_poll_state(&db.pool, asset, market, 0, Some(Utc::now()))
        .await
        .unwrap();
    // The newest evidence that we looked is older than the largest poll interval.
    seed_poll(
        &db.pool,
        asset,
        market,
        Utc::now() - Duration::hours(6),
        "written",
    )
    .await
    .unwrap();

    let server = Server::start(&db, db.config()).await;
    let response = server.get("/health").await;
    assert_eq!(response.status(), 503);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert!(!body["reasons"].as_array().unwrap().is_empty());
    server.stop();
    db.cleanup().await;
}

/// `written` means the price moved. A card that holds its price produces
/// `unchanged` for months, which is correct behaviour, and a check that treated
/// it as a fault would sit red on a healthy system until somebody stopped
/// reading it.
#[tokio::test]
async fn a_market_where_no_price_moves_keeps_health_at_200() {
    let db = TestDb::new().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let game = db.game().await.unwrap();
    let asset = seed_asset(&db.pool, game.id, "Marco Verratti", 84, "base")
        .await
        .unwrap();
    seed_poll_state(&db.pool, asset, market, 0, Some(Utc::now()))
        .await
        .unwrap();

    // Months of polling, and not one price change. Nothing is wrong.
    for minutes in [1_i64, 10, 20, 30] {
        seed_poll(
            &db.pool,
            asset,
            market,
            Utc::now() - Duration::minutes(minutes),
            "unchanged",
        )
        .await
        .unwrap();
    }
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        0,
        "the observation table is deliberately empty here"
    );

    let server = Server::start(&db, db.config()).await;
    let response = server.get("/health").await;
    assert_eq!(
        response.status(),
        200,
        "reading the observation table instead would call this an outage"
    );
    server.stop();
    db.cleanup().await;
}

#[tokio::test]
async fn a_fresh_install_with_no_polls_is_not_a_fault() {
    let db = TestDb::new().await.unwrap();
    let server = Server::start(&db, db.config()).await;
    assert_eq!(server.get("/health").await.status(), 200);
    server.stop();
    db.cleanup().await;
}

/// Section 4.9 on a platform that gives each service its own volume. The server
/// cannot see the database's disk there, so the size comes from the database
/// and is taken from the volume's stated capacity. A migrated database is
/// several megabytes, so a capacity equal to the floor leaves less than the
/// floor free.
#[tokio::test]
async fn a_full_database_volume_makes_health_return_503_without_reading_the_disk() {
    let db = TestDb::new().await.unwrap();
    let config = Config {
        // Deliberately unreachable. Reaching it would prove the check still
        // measured the filesystem, which is the defect this guards.
        disk_check_path: "/nonexistent/not/a/path".into(),
        database_volume_bytes: Some(8 * 1024 * 1024),
        min_free_disk_bytes: 8 * 1024 * 1024,
        ..db.config()
    };
    let server = Server::start(&db, config).await;
    let response = server.get("/health").await;
    assert_eq!(response.status(), 503, "a volume with no room is a fault");
    assert!(
        response.text().await.unwrap().contains("free disk"),
        "the reason must name the disk, or an operator cannot act on it"
    );
    server.stop();
    db.cleanup().await;
}

/// The same deployment with room left must stay healthy, or the check would be
/// satisfied by any capacity at all and assert nothing.
#[tokio::test]
async fn a_database_volume_with_room_keeps_health_at_200() {
    let db = TestDb::new().await.unwrap();
    let config = Config {
        disk_check_path: "/nonexistent/not/a/path".into(),
        database_volume_bytes: Some(64 * 1024 * 1024 * 1024),
        min_free_disk_bytes: 1024 * 1024 * 1024,
        ..db.config()
    };
    let server = Server::start(&db, config).await;
    assert_eq!(server.get("/health").await.status(), 200);
    server.stop();
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

/// Counts the calls a dead man's switch would receive.
async fn switch() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let app = axum::Router::new().route(
        "/beat",
        axum::routing::get(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                "ok"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/beat", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (url, hits, handle)
}

#[tokio::test]
async fn a_healthy_run_sends_one_heartbeat() {
    let (url, hits, handle) = switch().await;
    heartbeat::send(Some(&url), RunStatus::Ok).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    handle.abort();
}

/// A degraded run sends nothing, so the drift checks raise their alarm through
/// the same channel as a dead host: silence.
#[tokio::test]
async fn a_degraded_or_failed_run_sends_no_heartbeat() {
    let (url, hits, handle) = switch().await;
    heartbeat::send(Some(&url), RunStatus::Degraded).await;
    heartbeat::send(Some(&url), RunStatus::Failed).await;
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    handle.abort();
}

/// Unset in tests and local development, so neither needs an external service.
#[tokio::test]
async fn an_unset_heartbeat_url_is_skipped_rather_than_failing() {
    heartbeat::send(None, RunStatus::Ok).await;
}

/// A failed or slow heartbeat writes a warning and never changes the run status.
/// The ledger is correct whether or not a third party service answered.
#[tokio::test]
async fn an_unreachable_switch_does_not_panic() {
    heartbeat::send(Some("http://127.0.0.1:1/beat"), RunStatus::Ok).await;
}
