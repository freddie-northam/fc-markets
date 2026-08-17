//! Section 9: the ingestion guarantees, against real PostgreSQL and TimescaleDB.

mod common;

use chrono::{Duration, Utc};
use common::*;
use fc_market::archive::Archive;
use fc_market::config::Config;
use fc_market::db;
use fc_market::domain::{Observation, Poll, PollResult};
use fc_market::ids::{AssetId, PollOutcome, RunStatus};
use fc_market::ingest::{self, RunOutcome, Runner};

/// One poll cycle over the given source.
async fn run_once(
    db: &TestDb,
    source: &TestSource,
    archive: &dyn Archive,
) -> anyhow::Result<RunOutcome> {
    let config = db.config();
    let runner = Runner {
        pool: &db.pool,
        archive,
        source,
        config: &config,
    };
    ingest::run(&runner).await
}

/// Why a run degraded, as it recorded on itself. Asserting the status alone is
/// too weak: several unrelated checks all produce `Degraded`, so a test can keep
/// passing after the behaviour it names is removed.
/// A runner whose slow steps are out of cadence, so only the price path runs.
///
/// Discovery and the metadata step emit their own degrade messages using the
/// same words as the price path ("rate limited", "budget", "archived"). With
/// them in play, a reason assertion passes even when the price path's own
/// handling has been deleted, which is exactly how these tests used to lie.
fn price_path_only<'a>(
    db: &'a TestDb,
    source: &'a TestSource,
    archive: &'a dyn Archive,
) -> Runner<'a> {
    Runner {
        pool: &db.pool,
        archive,
        source,
        config: Box::leak(Box::new(Config {
            discovery_cadence_seconds: 86_400,
            metadata_cadence_seconds: 604_800,
            ..db.config()
        })),
    }
}

/// Asserts the newest run recorded exactly this reason.
async fn assert_reason(db: &TestDb, expected: &str) {
    let reasons = degraded_reasons(db).await;
    assert!(
        reasons.iter().any(|r| r == expected),
        "expected the run to record {expected:?}, got {reasons:?}"
    );
}

async fn degraded_reasons(db: &TestDb) -> Vec<String> {
    let raw: serde_json::Value = sqlx::query_scalar(
        "SELECT metadata->'degraded_reasons' FROM ingest_runs ORDER BY started_at DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("the newest run");
    serde_json::from_value(raw).unwrap_or_default()
}

fn status(outcome: &RunOutcome) -> RunStatus {
    match outcome {
        RunOutcome::Completed { status, .. } => *status,
        RunOutcome::Skipped => panic!("the run was skipped"),
    }
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_external_identifier_always_maps_to_one_internal_asset() {
    let db = TestDb::new().await.unwrap();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2026-01-05T09:00:00Z",
    )]);
    let archive = MemoryArchive::default();

    run_once(&db, &source, &archive).await.unwrap();
    make_everything_due(&db.pool).await.unwrap();
    // A second run must resolve the same identifier onto the same asset rather
    // than discovering it again.
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(db.count("SELECT count(*) FROM assets").await, 1);
    assert_eq!(db.count("SELECT count(*) FROM asset_source_ids").await, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn two_sources_map_to_one_internal_asset() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let first = TestSource::named(
        "alpha",
        vec![Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z")],
    );
    run_once(&db, &first, &archive).await.unwrap();

    let asset: AssetId =
        db::asset_by_external_id(&db.pool, "alpha", db.game().await.unwrap().id, "101")
            .await
            .unwrap()
            .unwrap()
            .0;

    // The second provider knows the same card by a different identifier. Mapping
    // it onto the existing asset is what keeps one price history per card.
    sqlx::query(
        "INSERT INTO asset_source_ids (asset_id, game_id, source, external_id)
         VALUES ($1, $2, 'beta', '999')",
    )
    .bind(asset.0)
    .bind(db.game().await.unwrap().id.0)
    .execute(&db.pool)
    .await
    .unwrap();

    let second = TestSource::named(
        "beta",
        vec![Card::new("999", "Marco Verratti").quote(PS, 9_100, "2026-01-05T10:00:00Z")],
    );
    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &second, &archive).await.unwrap();

    assert_eq!(db.count("SELECT count(*) FROM assets").await, 1);
    assert_eq!(
        db.count("SELECT count(DISTINCT source) FROM market_observations")
            .await,
        2,
        "both providers must write into one asset's history"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn the_same_external_identifier_in_two_games_maps_to_two_assets() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2026-01-05T09:00:00Z",
    )]);
    run_once(&db, &source, &archive).await.unwrap();

    // The next title reuses identifier 101 for a different card. Without game_id
    // in the key it would resolve onto this season's asset and contaminate its
    // history for ever.
    let (next_game, _) = seed_second_game(&db.pool).await.unwrap();
    let mut config = db.config();
    config.game_code = "FC27".into();
    let runner = runner_for(&db, &source, &archive, &config);
    ingest::run(&runner).await.unwrap();

    assert_eq!(db.count("SELECT count(*) FROM assets").await, 2);
    assert_eq!(
        db.count(&format!(
            "SELECT count(*) FROM assets WHERE game_id = '{}'",
            next_game.0
        ))
        .await,
        1
    );
    db.cleanup().await;
}

#[tokio::test]
async fn a_payload_whose_name_disagrees_with_the_resolved_asset_is_rejected() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2026-01-05T09:00:00Z",
    )]);
    run_once(&db, &source, &archive).await.unwrap();

    // The provider re-points identifier 101 at a different footballer. Rule 1
    // catches it; without the check every later price would attach to the wrong
    // card and nothing would raise an error.
    source.set_cards(vec![
        Card::new("101", "Marco Verratti")
            .payload_name("Erling Haaland")
            .quote(PS, 250_000, "2026-01-05T12:00:00Z"),
    ]);
    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        1,
        "the mismatched payload must not reach the ledger"
    );
    assert_eq!(
        db.count("SELECT count(*) FROM ingest_polls WHERE outcome = 'rejected'")
            .await,
        1
    );
    db.cleanup().await;
}

/// Ones to Watch, Path to Glory and Road to the Knockouts cards raise their
/// rating in season by design, and EA refreshes ratings mid season. A rating
/// check would reject exactly the high value cards on every poll.
#[tokio::test]
async fn a_payload_whose_rating_disagrees_is_accepted() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti")
            .rating(84)
            .recent(9_000, 6),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    source.set_cards(vec![
        Card::new("101", "Marco Verratti")
            .rating(88)
            .recent(40_000, 3),
    ]);
    make_everything_due(&db.pool).await.unwrap();
    let outcome = run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Ok);
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        4,
        "an upgraded card must keep writing prices on both platforms"
    );
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Tiers and coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn discovery_creates_one_poll_state_row_for_each_asset_and_market_pair() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "One").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
        Card::new("102", "Two").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
        Card::new("103", "Three").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    // Three assets, two markets.
    assert_eq!(db.count("SELECT count(*) FROM asset_poll_state").await, 6);
    db.cleanup().await;
}

#[tokio::test]
async fn the_tier_expression_follows_the_rating_and_the_version() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Promo").version("tots").rating(84),
        Card::new("102", "Elite").rating(91),
        Card::new("103", "Mid").rating(85),
        Card::new("104", "Filler").rating(70),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    let intervals: Vec<(String, i32)> = sqlx::query_as(
        "SELECT DISTINCT a.name, s.poll_interval_seconds
           FROM asset_poll_state s JOIN assets a ON a.id = s.asset_id
          ORDER BY a.name",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();

    let by_name: std::collections::HashMap<_, _> = intervals.into_iter().collect();
    assert_eq!(
        by_name["Promo"], 86_400,
        "a promotional card never falls to a slow band"
    );
    assert_eq!(by_name["Elite"], 14_400);
    assert_eq!(by_name["Mid"], 86_400);
    assert_eq!(by_name["Filler"], 1_209_600);
    assert_eq!(
        db.count(
            "SELECT count(*) FROM (
               SELECT asset_id FROM asset_poll_state
                GROUP BY asset_id HAVING count(DISTINCT poll_interval_seconds) > 1
             ) x"
        )
        .await,
        0,
        "the spec row is exactly one tier per asset, across every market"
    );
    db.cleanup().await;
}

/// The quota decides coverage, not the code. An asset outside it keeps its
/// history and its coverage record, and only stops being polled.
#[tokio::test]
async fn coverage_follows_the_configured_budget() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Best").rating(95),
        Card::new("102", "Good").rating(90),
        Card::new("103", "Spare").rating(60),
    ]);

    let mut config = db.config();
    config.asset_coverage = 2;
    let runner = runner_for(&db, &source, &archive, &config);
    ingest::run(&runner).await.unwrap();

    assert_eq!(db.count("SELECT count(*) FROM assets").await, 3);
    assert_eq!(
        db.count("SELECT count(*) FROM asset_poll_state").await,
        4,
        "two covered assets across two markets"
    );
    assert_eq!(
        db.count(
            "SELECT count(*) FROM asset_poll_state s JOIN assets a ON a.id = s.asset_id
              WHERE a.name = 'Spare'"
        )
        .await,
        0
    );
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Idempotency and restatement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_payload_ingested_twice_does_not_double_the_observation_count() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti")
            .quote(PS, 9_000, "2026-01-05T09:00:00Z")
            .quote(PC, 8_700, "2026-01-05T09:00:00Z"),
    ]);

    run_once(&db, &source, &archive).await.unwrap();
    let after_first = db.count("SELECT count(*) FROM market_observations").await;
    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(after_first, 2);
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        2,
        "the idempotency index must absorb the identical replay"
    );
    db.cleanup().await;
}

/// The whole ledger rests on this index surviving compression. TimescaleDB can
/// only enforce a unique constraint against a compressed chunk when every column
/// of the index also appears in `compress_segmentby` or `compress_orderby`.
#[tokio::test]
async fn the_unique_constraint_still_rejects_a_duplicate_after_chunk_compression() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2026-01-05T09:00:00Z",
    )]);
    run_once(&db, &source, &archive).await.unwrap();

    let compressed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (
            SELECT compress_chunk(c) FROM show_chunks('market_observations') c
         ) s",
    )
    .fetch_one(&db.pool)
    .await
    .expect("compressing the chunk must succeed");
    assert!(compressed > 0, "there must be a chunk to compress");

    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        1,
        "a duplicate must still conflict once the chunk is compressed"
    );
    assert_eq!(
        db.count("SELECT count(*) FROM ingest_polls WHERE outcome = 'unchanged'")
            .await,
        1
    );
    db.cleanup().await;
}

/// `price` is in the idempotency key on purpose. Without it an upstream
/// correction at an already recorded timestamp is discarded in silence.
#[tokio::test]
async fn a_different_price_at_the_same_timestamp_lands_as_a_second_row() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2026-01-05T09:00:00Z",
    )]);
    run_once(&db, &source, &archive).await.unwrap();

    source.set_cards(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_400,
        "2026-01-05T09:00:00Z",
    )]);
    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        2,
        "a restatement is a second truthful row, not a discarded one"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn an_old_observed_at_and_a_current_ingested_at_stay_separate() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2025-10-01T09:00:00Z",
    )]);
    run_once(&db, &source, &archive).await.unwrap();

    let (observed, ingested): (chrono::DateTime<Utc>, chrono::DateTime<Utc>) =
        sqlx::query_as("SELECT observed_at, ingested_at FROM market_observations")
            .fetch_one(&db.pool)
            .await
            .unwrap();

    assert_eq!(observed, ts("2025-10-01T09:00:00Z"));
    assert!(
        ingested > observed + Duration::days(30),
        "our clock must never overwrite the provider's"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn one_asset_holds_separate_observations_and_poll_state_for_each_market() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti")
            .quote(PS, 9_000, "2026-01-05T09:00:00Z")
            .quote(PC, 8_700, "2026-01-05T09:00:00Z"),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    let prices: Vec<i64> =
        sqlx::query_scalar("SELECT price FROM market_observations ORDER BY price")
            .fetch_all(&db.pool)
            .await
            .unwrap();

    assert_eq!(prices, vec![8_700, 9_000]);
    assert_eq!(db.count("SELECT count(*) FROM asset_poll_state").await, 2);
    assert_eq!(
        db.count("SELECT count(DISTINCT market_id) FROM market_observations")
            .await,
        2
    );
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_far_future_or_pre_release_observed_at_is_rejected() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Before Release").at_raw(PS, 9_000, ts("2024-01-01T09:00:00Z")),
        Card::new("102", "Far Future").at_raw(PS, 9_000, Utc::now() + Duration::days(3_650)),
        Card::new("103", "Sound").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        1,
        "only the sound timestamp may reach the hypertable"
    );
    // One bad timestamp far in the future would make TimescaleDB create a chunk
    // at that date, and it would then sit between us and every range query.
    assert_eq!(
        db.count("SELECT count(*) FROM show_chunks('market_observations')")
            .await,
        1
    );
    db.cleanup().await;
}

#[tokio::test]
async fn a_missing_source_timestamp_is_rejected_and_counted() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Undated")
            .undated(PS, 9_000)
            .undated(PC, 9_000),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        0,
        "we never substitute our own clock for a missing source timestamp"
    );
    assert_eq!(
        db.count(
            "SELECT records_rejected::bigint FROM ingest_runs ORDER BY started_at DESC LIMIT 1"
        )
        .await,
        2
    );
    assert_eq!(
        db.count("SELECT count(*) FROM ingest_polls WHERE outcome = 'rejected'")
            .await,
        2
    );
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Coverage evidence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unchanged_price_writes_a_poll_row_with_outcome_unchanged() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2026-01-05T09:00:00Z",
    )]);
    run_once(&db, &source, &archive).await.unwrap();
    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM ingest_polls WHERE outcome = 'written'")
            .await,
        1
    );
    assert_eq!(
        db.count("SELECT count(*) FROM ingest_polls WHERE outcome = 'unchanged'")
            .await,
        1,
        "without this row a stable price is indistinguishable from an outage"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn a_rejected_record_writes_a_poll_row_with_its_own_outcome() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Undated")
            .undated(PS, 9_000)
            .undated(PC, 9_000),
        Card::new("102", "Priceless")
            .no_price(PS, "2026-01-05T09:00:00Z")
            .no_price(PC, "2026-01-05T09:00:00Z"),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM ingest_polls WHERE outcome = 'rejected'")
            .await,
        2
    );
    assert_eq!(
        db.count("SELECT count(*) FROM ingest_polls WHERE outcome = 'no_price'")
            .await,
        2,
        "an asset failing validation on every run must not look like a stable price"
    );
    // The failure count is what the valuation freshness gate reads.
    assert_eq!(
        db.count("SELECT count(*) FROM asset_poll_state WHERE consecutive_failures > 0")
            .await,
        4
    );
    db.cleanup().await;
}

#[tokio::test]
async fn one_bad_record_does_not_fail_the_run() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Undated")
            .undated(PS, 9_000)
            .undated(PC, 9_000),
        Card::new("102", "Sound").recent(12_000, 4),
        Card::new("103", "Also sound").recent(13_000, 4),
    ]);
    let outcome = run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Ok);
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        4,
        "the sound records must still be written"
    );
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Concurrency, atomicity and the archive precondition
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_second_run_stops_when_the_advisory_lock_is_held() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_000, 5)]);

    // Hold the lock, exactly as a slow run would.
    let held = db::try_lock(&db.url)
        .await
        .unwrap()
        .expect("the lock must be free");

    let outcome = run_once(&db, &source, &archive).await.unwrap();
    assert!(
        matches!(outcome, RunOutcome::Skipped),
        "without this the manual run and the scheduled run spend the budget twice"
    );
    assert_eq!(db.count("SELECT count(*) FROM ingest_runs").await, 0);

    // Releasing must hand the lock straight back. A pooled connection would not:
    // returning it to the pool leaves its session, and its lock, alive.
    held.release().await;
    let outcome = run_once(&db, &source, &archive).await.unwrap();
    assert_eq!(status(&outcome), RunStatus::Ok);
    db.cleanup().await;
}

/// The observation, the poll row and the poll state update are one transaction.
/// A failure part way through must leave none of them.
#[tokio::test]
async fn a_failure_between_the_writes_leaves_no_poll_row_without_its_observation() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2026-01-05T09:00:00Z",
    )]);
    run_once(&db, &source, &archive).await.unwrap();

    let game = db.game().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let asset = db::asset_by_external_id(&db.pool, "test", game.id, "101")
        .await
        .unwrap()
        .unwrap()
        .0;
    let run = db::open_run(&db.pool, "test", "test/1").await.unwrap().0;

    let before = db.count("SELECT count(*) FROM market_observations").await;
    let polls = vec![
        Poll {
            asset_id: asset,
            market_id: market,
            source_observed_at: Some(ts("2026-02-01T09:00:00Z")),
            result: PollResult::Priced(Observation {
                asset_id: asset,
                market_id: market,
                source: "test",
                observed_at: ts("2026-02-01T09:00:00Z"),
                price: 11_000,
                min_price: None,
                max_price: None,
                source_ref: Some("101".into()),
                ingest_run_id: run,
            }),
        },
        // An asset that does not exist. The foreign key aborts the transaction
        // after the first observation has already been inserted.
        Poll {
            asset_id: AssetId::new(),
            market_id: market,
            source_observed_at: None,
            result: PollResult::Failed(PollOutcome::NoPrice),
        },
    ];

    let result = db::write_polls(&db.pool, &polls, Utc::now(), run).await;
    assert!(result.is_err(), "the batch must fail");
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        before,
        "the first observation must roll back with the rest of the batch"
    );
    assert_eq!(
        db.count(&format!(
            "SELECT count(*) FROM ingest_polls WHERE run_id = '{}'",
            run.0
        ))
        .await,
        0,
        "no poll row may survive a batch whose observation rolled back"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn a_failed_archive_write_stops_the_batch_and_marks_the_run_degraded() {
    let db = TestDb::new().await.unwrap();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").quote(
        PS,
        9_000,
        "2026-01-05T09:00:00Z",
    )]);

    // Discover first with a working archive, so the failure under test is the
    // price archive write and nothing else.
    let working = MemoryArchive::default();
    run_once(&db, &source, &working).await.unwrap();
    make_everything_due(&db.pool).await.unwrap();
    let before = db.count("SELECT count(*) FROM market_observations").await;

    let outcome = ingest::run(&price_path_only(&db, &source, &RefusingArchive))
        .await
        .unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    assert!(
        degraded_reasons(&db)
            .await
            .iter()
            .any(|r| r.starts_with("batch 0 was not archived")),
        "the price path must name its own archive failure: {:?}",
        degraded_reasons(&db).await
    );
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        before,
        "a payload that was never stored must never be parsed"
    );
    db.cleanup().await;
}

/// An HTTP 429 stops the run and marks it degraded. It is not a per-record
/// rejection, and retrying inside the same run would spend a quota the provider
/// has already said is gone.
#[tokio::test]
async fn a_rate_limited_provider_stops_the_run_and_marks_it_degraded() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_000, 3)]);
    run_once(&db, &source, &archive).await.unwrap();

    make_everything_due(&db.pool).await.unwrap();
    source.set_rate_limited(true);
    let outcome = ingest::run(&price_path_only(&db, &source, &archive))
        .await
        .unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    // The exact reason. Asserting the status alone kept this test green with the
    // rate-limit handling deleted, because several unrelated checks also degrade
    // a run, and the slow steps emit their own "rate limited" wording.
    assert_reason(&db, "the provider rate limited this run").await;
    db.cleanup().await;
}

/// The budget is daily. A per-run budget would let twelve runs spend twelve
/// budgets.
#[tokio::test]
async fn the_daily_request_budget_stops_a_run() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "One").recent(9_000, 3),
        Card::new("102", "Two").recent(9_000, 3),
    ])
    .batch_size(1);
    run_once(&db, &source, &archive).await.unwrap();

    let spent = db::requests_spent_today(&db.pool, "test").await.unwrap();
    assert!(spent > 0, "a run must record what it spent");

    make_everything_due(&db.pool).await.unwrap();
    let mut runner = price_path_only(&db, &source, &archive);
    let config = Box::leak(Box::new(Config {
        daily_request_budget: spent as u32,
        ..runner.config.clone()
    }));
    runner.config = config;
    let outcome = ingest::run(&runner).await.unwrap();

    assert_eq!(
        status(&outcome),
        RunStatus::Degraded,
        "the day's budget is already spent, so this run must stop and say so"
    );
    assert_reason(
        &db,
        "the daily request budget was already spent, so no price was read",
    )
    .await;
    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replaying_one_archive_twice_gives_the_same_table() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti")
            .quote(PS, 9_000, "2026-01-05T09:00:00Z")
            .quote(PC, 8_700, "2026-01-05T09:00:00Z"),
        Card::new("102", "Bruno Fernandes").quote(PS, 15_000, "2026-01-05T09:00:00Z"),
    ]);
    let outcome = run_once(&db, &source, &archive).await.unwrap();
    let RunOutcome::Completed { run_id, .. } = outcome else {
        panic!("the run must complete")
    };

    async fn snapshot(db: &TestDb) -> Vec<(uuid::Uuid, uuid::Uuid, chrono::DateTime<Utc>, i64)> {
        sqlx::query_as(
            "SELECT asset_id, market_id, observed_at, price
               FROM market_observations
              ORDER BY asset_id, market_id, observed_at, price",
        )
        .fetch_all(&db.pool)
        .await
        .unwrap()
    }

    let original = snapshot(&db).await;
    assert_eq!(original.len(), 3);

    let config = db.config();
    let runner = runner_for(&db, &source, &archive, &config);
    ingest::replay(&runner, run_id).await.unwrap();
    let once = snapshot(&db).await;
    ingest::replay(&runner, run_id).await.unwrap();
    let twice = snapshot(&db).await;

    assert_eq!(original, once, "a replay must reproduce the same rows");
    assert_eq!(once, twice, "a second replay must change nothing");
    assert_eq!(
        db.count("SELECT count(*) FROM ingest_polls").await,
        4,
        "a replay is not evidence that we looked at the market"
    );
    db.cleanup().await;
}

/// Rule 2 bounds a timestamp against the run's own start. A wall clock bound
/// would accept on replay what it rejected at ingest time, so the same archive
/// would produce two different tables.
#[tokio::test]
async fn replay_keeps_the_original_runs_verdict_on_a_timestamp() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    // One card priced a week ago, one priced two days ago. The original run
    // accepted both: its own start was later than either.
    let source = TestSource::new(vec![
        Card::new("101", "Long settled").recent(9_000, 168),
        Card::new("102", "Recently moved").recent(12_000, 48),
    ]);
    let outcome = run_once(&db, &source, &archive).await.unwrap();
    let RunOutcome::Completed { run_id, .. } = outcome else {
        panic!("the run must complete")
    };
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        4
    );

    // Move the recorded start back between the two prices. The wall clock has
    // not moved, so the two bounds now disagree: the recorded start rejects the
    // two day old price and keeps the week old one, while a wall clock bound
    // would keep both.
    age_runs(&db.pool, Duration::days(4)).await.unwrap();

    let config = db.config();
    let runner = runner_for(&db, &source, &archive, &config);
    ingest::replay(&runner, run_id).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        2,
        "replay must bound against the run's recorded start; a wall clock bound \
         would have kept all four and made one archive give two tables"
    );
    assert_eq!(
        db.count(
            "SELECT count(*) FROM market_observations o JOIN assets a ON a.id = o.asset_id
              WHERE a.name = 'Long settled'"
        )
        .await,
        2,
        "and it is the price inside the bound that survived"
    );
    db.cleanup().await;
}

/// A spent budget must not look like a healthy, quiet run.
///
/// With nothing left to spend the due query returns no rows, which reads exactly
/// like nothing being due. If the run closed `ok` it would send a heartbeat, and
/// the dead man's switch would stay green while the ledger had stopped.
#[tokio::test]
async fn a_run_that_cannot_afford_a_single_request_does_not_close_ok() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_000, 4)]);
    run_once(&db, &source, &archive).await.unwrap();

    let mut config = db.config();
    config.daily_request_budget = 0;
    // The slow steps are not due, so only the price path is left to notice.
    config.discovery_cadence_seconds = 86_400;
    config.metadata_cadence_seconds = 604_800;
    make_everything_due(&db.pool).await.unwrap();

    let runner = runner_for(&db, &source, &archive, &config);
    let outcome = ingest::run(&runner).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    db.cleanup().await;
}

/// The delete and the rewrite are one transaction. A delete that committed
/// before a failed rewrite would destroy exactly the rows the replay set out to
/// correct.
#[tokio::test]
async fn a_replay_that_cannot_read_its_archive_leaves_the_ledger_untouched() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").recent(9_000, 4),
        Card::new("102", "Bruno Fernandes").recent(15_000, 4),
    ]);
    let outcome = run_once(&db, &source, &archive).await.unwrap();
    let RunOutcome::Completed { run_id, .. } = outcome else {
        panic!("the run must complete")
    };

    let before = db.count("SELECT count(*) FROM market_observations").await;
    assert_eq!(before, 4);

    // The object store has gone away between the run and the replay.
    let config = db.config();
    let runner = runner_for(&db, &source, &RefusingArchive, &config);
    assert!(ingest::replay(&runner, run_id).await.is_err());

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        before,
        "a failed replay must not leave a hole where the history was"
    );
    db.cleanup().await;
}

/// Section 4.5 has no retry policy: the next scheduled poll is the retry, and
/// `consecutive_failures` supplies the backoff. Without it a permanently broken
/// card spends full rate quota for ever and crowds out the healthy ones.
#[tokio::test]
async fn a_failing_asset_backs_off_while_a_healthy_one_stays_on_its_interval() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Healthy").recent(9_000, 3),
        Card::new("102", "Broken")
            .undated(PS, 9_000)
            .undated(PC, 9_000),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    // Both were polled just now. Wind the clock back by one interval: the healthy
    // card is due again, the failing one is not, because its interval doubled.
    sqlx::query(
        "UPDATE asset_poll_state
            SET last_polled_at = now() - make_interval(secs => poll_interval_seconds + 1)",
    )
    .execute(&db.pool)
    .await
    .unwrap();

    let game = db.game().await.unwrap();
    let due = db::due_external_ids(&db.pool, "test", game.id, 100)
        .await
        .unwrap();

    assert!(due.contains(&"101".to_string()), "the healthy card is due");
    assert!(
        !due.contains(&"102".to_string()),
        "the failing card must wait longer than its base interval"
    );

    // Far enough back and even a failing card is retried. A backoff that never
    // ends is an asset silently dropped from coverage.
    sqlx::query(
        "UPDATE asset_poll_state
            SET last_polled_at = now() - make_interval(secs => poll_interval_seconds * 20)",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let due = db::due_external_ids(&db.pool, "test", game.id, 100)
        .await
        .unwrap();
    assert!(
        due.contains(&"102".to_string()),
        "the retry must come round"
    );

    db.cleanup().await;
}

/// Compression is applied to chunks older than 30 days, and the ledger is kept
/// for ever, so almost every row will live in a compressed chunk. A restatement
/// or a replay that could not touch one would mean the archive stops being able
/// to correct the ledger after a month.
#[tokio::test]
async fn a_compressed_chunk_still_accepts_a_restatement_and_a_replay() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_000, 6)]);
    let outcome = run_once(&db, &source, &archive).await.unwrap();
    let RunOutcome::Completed { run_id, .. } = outcome else {
        panic!("the run must complete")
    };

    let compressed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT compress_chunk(c) FROM show_chunks('market_observations') c) s",
    )
    .fetch_one(&db.pool)
    .await
    .expect("the chunk must compress");
    assert!(compressed > 0);

    // A genuine restatement: the same instant, a corrected price.
    source.set_cards(vec![Card::new("101", "Marco Verratti").recent(9_000, 6)]);
    let corrected = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_400, 6)]);
    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &corrected, &archive).await.unwrap();

    assert!(
        db.count("SELECT count(*) FROM market_observations").await > 2,
        "a corrected price must still land in a compressed chunk"
    );

    // And replay must still be able to delete out of one.
    let config = db.config();
    let runner = runner_for(&db, &source, &archive, &config);
    let report = ingest::replay(&runner, run_id).await.unwrap();
    assert!(
        report.deleted > 0,
        "replay must be able to remove rows from a compressed chunk"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// Drift (section 4.9)
// ---------------------------------------------------------------------------
//
// These four checks are the only alarm the product has. A degraded run withholds
// the heartbeat, so if one of them silently stops working, a dead feed looks
// exactly like a healthy one and the dead man's switch stays green.

/// Cards priced `hours` ago, one per entry of `prices`.
fn priced(prices: &[i64], hours: i64) -> Vec<Card> {
    prices
        .iter()
        .enumerate()
        .map(|(i, p)| Card::new(&format!("{}", 100 + i), &format!("Card {i}")).recent(*p, hours))
        .collect()
}

#[tokio::test]
async fn a_feed_that_mostly_returns_no_price_degrades_the_run() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();

    // Three cards priced, four returning nothing: 8 of 14 reads carry no price,
    // past the half that the check allows.
    let mut cards = priced(&[10_000, 11_000, 12_000], 2);
    for i in 0..4 {
        cards.push(
            Card::new(&format!("{}", 900 + i), &format!("Empty {i}"))
                .no_price(PS, "2026-01-05T09:00:00Z"),
        );
    }
    let outcome = run_once(&db, &TestSource::new(cards), &archive)
        .await
        .unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    assert!(
        degraded_reasons(&db)
            .await
            .iter()
            .any(|r| r.contains("carried no price")),
        "{:?}",
        degraded_reasons(&db).await
    );
    db.cleanup().await;
}

/// A quiet market is NOT this. Section 4.9 warns that a held price is correct
/// behaviour, so the check has to measure the whole feed over a day, not one run.
#[tokio::test]
async fn a_feed_where_nothing_has_moved_for_a_day_degrades_the_run() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(priced(&[10_000, 11_000, 12_000], 30));

    let outcome = run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    assert!(
        degraded_reasons(&db)
            .await
            .iter()
            .any(|r| r.contains("has moved for")),
        "{:?}",
        degraded_reasons(&db).await
    );
    db.cleanup().await;
}

#[tokio::test]
async fn a_quiet_market_inside_the_window_stays_healthy() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(priced(&[10_000, 11_000, 12_000], 6));

    run_once(&db, &source, &archive).await.unwrap();
    make_everything_due(&db.pool).await.unwrap();
    // The identical payload again: every price held, nothing advanced.
    let outcome = run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        status(&outcome),
        RunStatus::Ok,
        "a market where nothing moved is healthy, not frozen: {:?}",
        degraded_reasons(&db).await
    );
    db.cleanup().await;
}

#[tokio::test]
async fn a_source_serving_one_price_for_everything_degrades_the_run() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    // Ten cards, all at one price: twenty observations, two distinct values.
    let source = TestSource::new(priced(&[7_000; 10], 2));

    let outcome = run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    assert!(
        degraded_reasons(&db)
            .await
            .iter()
            .any(|r| r.contains("looks frozen")),
        "{:?}",
        degraded_reasons(&db).await
    );
    db.cleanup().await;
}

/// A provider that changes units, or starts serving a different field, moves
/// every asset at once. A real market does not.
#[tokio::test]
async fn a_whole_feed_repricing_at_once_degrades_the_run() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let base: Vec<i64> = (0..10).map(|i| 10_000 + i * 1_000).collect();
    let source = TestSource::new(priced(&base, 6));
    run_once(&db, &source, &archive).await.unwrap();

    // Every price multiplied by five, which no market does in one poll.
    let moved: Vec<i64> = base.iter().map(|p| p * 5).collect();
    source.set_cards(priced(&moved, 2));
    make_everything_due(&db.pool).await.unwrap();
    let outcome = run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    assert!(
        degraded_reasons(&db)
            .await
            .iter()
            .any(|r| r.contains("median asset moved by a factor")),
        "{:?}",
        degraded_reasons(&db).await
    );
    db.cleanup().await;
}

/// A run that read something and rejected all of it is not healthy, even though
/// no step returned an error.
#[tokio::test]
async fn a_run_that_rejects_everything_it_read_degrades() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "Undated")
            .undated(PS, 9_000)
            .undated(PC, 9_000),
        Card::new("102", "Also undated")
            .undated(PS, 8_000)
            .undated(PC, 8_000),
    ]);

    let outcome = run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        0
    );
    db.cleanup().await;
}

/// Replay reads `price_keys` off the run row. Written only at close, a run
/// killed part way through, which is exactly what the reaper exists for, left
/// its archived payloads sitting in the bucket with nothing pointing at them.
#[tokio::test]
async fn a_run_that_never_closes_still_records_what_it_archived() {
    let db = TestDb::new().await.unwrap();
    let (run, _) = db::open_run(&db.pool, "test", "test/1").await.unwrap();

    db::record_request(&db.pool, run).await.unwrap();
    db::record_price_key(&db.pool, run, "raw/test/2026/01/05/a-0.json.zst")
        .await
        .unwrap();
    db::record_step(&db.pool, run, "discovery_ran")
        .await
        .unwrap();

    // The run is killed here. close_run never happens; the reaper finds it.
    assert_eq!(db::reap_abandoned_runs(&db.pool).await.unwrap(), 0);
    sqlx::query("UPDATE ingest_runs SET heartbeat_at = now() - INTERVAL '2 hours'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(db::reap_abandoned_runs(&db.pool).await.unwrap(), 1);

    let recorded = db::run_replay_source(&db.pool, run).await.unwrap().unwrap();
    assert_eq!(
        recorded.keys,
        vec!["raw/test/2026/01/05/a-0.json.zst"],
        "the archive it wrote must still be reachable"
    );
    assert_eq!(
        db::requests_spent_today(&db.pool, "test").await.unwrap(),
        1,
        "its spend must still count against the day, or the next run overspends"
    );
    assert!(
        db::seconds_since_step(&db.pool, "test", "discovery_ran")
            .await
            .unwrap()
            .is_some(),
        "its finished discovery must not be bought again"
    );
    db.cleanup().await;
}

/// Handing one provider's archived payloads to another's parser would rewrite
/// that run's history under a parser that never saw the shape, and it would look
/// like a successful replay.
#[tokio::test]
async fn replay_refuses_a_run_written_by_a_different_source() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let alpha = TestSource::named(
        "alpha",
        vec![Card::new("101", "Marco Verratti").recent(9_000, 3)],
    );
    let outcome = run_once(&db, &alpha, &archive).await.unwrap();
    let RunOutcome::Completed { run_id, .. } = outcome else {
        panic!("the run must complete")
    };

    let beta = TestSource::named(
        "beta",
        vec![Card::new("101", "Marco Verratti").recent(9_000, 3)],
    );
    let config = db.config();
    let runner = runner_for(&db, &beta, &archive, &config);

    let error = ingest::replay(&runner, run_id)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("alpha") && error.contains("beta"),
        "the refusal must name both sources: {error}"
    );
    db.cleanup().await;
}

/// The budget is spent against the persisted count, so a request recorded twice
/// shrinks the day by two. A cold run does exactly three: the asset list, one
/// metadata batch, one price batch.
#[tokio::test]
async fn every_request_is_counted_exactly_once() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_000, 3)]);

    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db::requests_spent_today(&db.pool, "test").await.unwrap(),
        3,
        "the asset list, the metadata batch and the price batch, once each"
    );
    db.cleanup().await;
}

/// The step flag suppresses the metadata step for a whole cadence, seven days by
/// default. A run that stopped early must not set it, or every asset it did not
/// reach keeps a null rating and version: tiered slowest, and pooled into one
/// meaningless valuation class until the cadence comes round again.
#[tokio::test]
async fn a_metadata_step_that_stopped_early_does_not_mark_itself_done() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let cards: Vec<Card> = (0..6)
        .map(|i| Card::new(&format!("{}", 100 + i), &format!("Card {i}")).recent(9_000, 3))
        .collect();
    // One asset per request, so the budget runs out part way through.
    let source = TestSource::new(cards).batch_size(1);

    let config = Box::leak(Box::new(Config {
        // The asset list, then two metadata batches, then nothing.
        daily_request_budget: 3,
        ..db.config()
    }));
    let runner = runner_for(&db, &source, &archive, config);
    ingest::run(&runner).await.unwrap();

    assert!(
        db::seconds_since_step(&db.pool, "test", "metadata_ran")
            .await
            .unwrap()
            .is_none(),
        "a step that ran out of budget must leave itself due"
    );
    // Discovery did finish, so it may claim its flag.
    assert!(
        db::seconds_since_step(&db.pool, "test", "discovery_ran")
            .await
            .unwrap()
            .is_some(),
        "a step that did finish should not be bought again"
    );
    db.cleanup().await;
}

/// The budget can be smaller than the due set on a day where the slow steps ate
/// it. The identifiers that go first must be the ones waiting longest, not the
/// ones earliest in the alphabet.
#[tokio::test]
async fn the_most_overdue_assets_are_polled_first_when_the_budget_is_short() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    // Alphabetically ascending identifiers, so an alphabetical cap would take
    // "aaa" first, which is the one polled most recently. Both are rated into
    // the 4 hour band, so both are genuinely due and the order is what decides.
    let source = TestSource::new(vec![
        Card::new("aaa", "Freshly polled")
            .rating(91)
            .recent(9_000, 3),
        Card::new("bbb", "Waiting longest")
            .rating(91)
            .recent(9_000, 3),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    let game = db.game().await.unwrap();
    sqlx::query(
        "UPDATE asset_poll_state s SET last_polled_at = CASE
             WHEN si.external_id = 'aaa' THEN now() - INTERVAL '5 hours'
             ELSE now() - INTERVAL '9 hours' END
           FROM asset_source_ids si
          WHERE si.asset_id = s.asset_id",
    )
    .execute(&db.pool)
    .await
    .unwrap();

    let due = db::due_external_ids(&db.pool, "test", game.id, 1)
        .await
        .unwrap();

    assert_eq!(
        due,
        vec!["bbb".to_string()],
        "the single affordable slot must go to the asset waiting longest"
    );
    db.cleanup().await;
}

/// A payload that carries one pair twice with different numbers is telling us
/// two things about one card, which is the symptom rule 1 exists for. One quote
/// has to win so the valuation's "latest" stays settled, but the contradiction
/// must not vanish with it.
#[tokio::test]
async fn a_payload_that_contradicts_itself_is_counted_not_silently_reduced() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();

    // The same card and platform twice, at two different prices.
    let mut card = Card::new("101", "Marco Verratti").recent(9_000, 3);
    let contradiction = card.quotes[0].clone();
    card.quotes.push(TestQuote {
        price: Some(50_000),
        ..contradiction
    });
    let source = TestSource::new(vec![card]);

    run_once(&db, &source, &archive).await.unwrap();

    let seen: i64 = db
        .count("SELECT records_seen::bigint FROM ingest_runs ORDER BY started_at DESC LIMIT 1")
        .await;
    let rejected: i64 = db
        .count("SELECT records_rejected::bigint FROM ingest_runs ORDER BY started_at DESC LIMIT 1")
        .await;

    // Two markets asked about, plus the overwritten quote.
    assert_eq!(seen, 3, "the contradicted quote must still be counted");
    assert_eq!(rejected, 1, "and counted as rejected, not as a price");
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        2,
        "one price per pair reaches the ledger, so latest stays settled"
    );
    db.cleanup().await;
}

/// Replay is the only thing in the system that deletes from the ledger, and its
/// whole purpose is to CORRECT it. A replay that resolves nothing must not be
/// allowed to erase what it was meant to fix.
#[tokio::test]
async fn a_replay_that_would_write_nothing_refuses_to_delete_everything() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_000, 3)]);
    let outcome = run_once(&db, &source, &archive).await.unwrap();
    let RunOutcome::Completed { run_id, .. } = outcome else {
        panic!("the run must complete")
    };
    let before = db.count("SELECT count(*) FROM market_observations").await;
    assert_eq!(before, 2);

    // The parser now resolves nothing from the same archived payload. A changed
    // provider shape or a re-seeded asset mapping both land here.
    source.set_cards(vec![]);
    let config = db.config();
    let runner = runner_for(&db, &source, &archive, &config);

    let result = ingest::replay(&runner, run_id).await;

    assert!(
        result.is_err(),
        "replay must refuse to replace a history with nothing"
    );
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        before,
        "and the ledger must be exactly as it was"
    );
    db.cleanup().await;
}

/// The provider lists an unreleased card with no name and fills the name in on
/// release. FUT-DB does this for thousands of assets, so the transition is
/// routine rather than exceptional.
///
/// Read as a re-point it strands the asset for ever: attributes are never
/// applied, so it keeps a null rating, the slowest tier and the base valuation
/// class, and every run degrades with a re-point warning that would otherwise be
/// how a real re-point is noticed.
#[tokio::test]
async fn a_card_that_gains_its_first_name_adopts_it_rather_than_being_rejected() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    // No name and no rating, which is exactly how the provider lists a card it
    // has not released.
    let source = TestSource::new(vec![Card::new("101", "").rating(0).recent(9_000, 6)]);
    run_once(&db, &source, &archive).await.unwrap();

    let held: String = sqlx::query_scalar("SELECT name FROM assets LIMIT 1")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(held, "", "the provider gave no name, so we hold none");

    // EA releases the card and the provider names it.
    source.set_cards(vec![
        Card::new("101", "Kylian Mbappe")
            .rating(91)
            .recent(250_000, 3),
    ]);
    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &source, &archive).await.unwrap();

    let (name, rating): (String, Option<i16>) =
        sqlx::query_as("SELECT name, rating FROM assets LIMIT 1")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(name, "Kylian Mbappe", "the first real name must be adopted");
    assert_eq!(
        rating,
        Some(91),
        "and its attributes with it, or the card is stuck in the slowest tier"
    );

    let reasons = degraded_reasons(&db).await;
    assert!(
        !reasons.iter().any(|r| r.contains("different card")),
        "naming a card is not a re-point, so it must not degrade the run: {reasons:?}"
    );
    db.cleanup().await;
}

/// Rule 1 rejects a price payload whose name disagrees with the resolved asset,
/// because that is a provider re-pointing an identifier at a different card. But
/// the metadata step writes that same name, from the same provider, with no
/// check. A re-pointed identifier only had to survive until the next refresh to
/// be adopted, and then every later price for the new card would match the
/// renamed asset and land on the old card's history.
#[tokio::test]
async fn the_metadata_step_cannot_be_used_to_defeat_the_identity_check() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_000, 6)]);
    run_once(&db, &source, &archive).await.unwrap();

    let before = db.count("SELECT count(*) FROM market_observations").await;
    assert_eq!(before, 2);

    // The provider now says identifier 101 is a different footballer, in the
    // metadata AND in the prices, so the two agree with each other.
    source.set_cards(vec![Card::new("101", "Erling Haaland").recent(250_000, 3)]);
    make_everything_due(&db.pool).await.unwrap();
    run_once(&db, &source, &archive).await.unwrap();

    let name: String = sqlx::query_scalar("SELECT name FROM assets LIMIT 1")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        name, "Marco Verratti",
        "the identity anchor must not be overwritten by the provider that is \
         contradicting it"
    );
    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        before,
        "and the other card's prices must not land on this card's history"
    );
    db.cleanup().await;
}

/// Every column of a fact table's unique index must also appear in its
/// compression settings.
///
/// TimescaleDB cannot enforce uniqueness against a compressed chunk on a column
/// it did not segment or order by. Get this wrong and the ledger keeps its only
/// idempotency guarantee for thirty days and then quietly loses it, so a replay
/// or a retry starts duplicating rows and nothing says so. It warns once, at
/// migration time, into a log nobody reads twice.
///
/// This asserts the relationship rather than the current column list, so adding
/// a column to either index without adding it to the compression settings fails
/// here instead of in production a month later.
#[tokio::test]
async fn compression_covers_every_column_of_the_idempotency_indexes() {
    let db = TestDb::new().await.unwrap();

    let uncovered: Vec<(String, String, String)> = sqlx::query_as(
        "WITH idx AS (
             SELECT i.indrelid::regclass::text AS tbl,
                    i.indexrelid::regclass::text AS idx,
                    a.attname
               FROM pg_index i
               JOIN pg_attribute a
                 ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
              WHERE i.indisunique
                AND i.indrelid::regclass::text IN ('market_observations', 'ingest_polls')
         ),
         comp AS (
             SELECT hypertable_name AS tbl, attname
               FROM timescaledb_information.compression_settings
         )
         SELECT idx.tbl, idx.idx, idx.attname
           FROM idx
           LEFT JOIN comp ON comp.tbl = idx.tbl AND comp.attname = idx.attname
          WHERE comp.attname IS NULL
          ORDER BY 1, 3",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();

    assert!(
        uncovered.is_empty(),
        "these unique-index columns are not in compress_segmentby or \
         compress_orderby, so uniqueness stops being enforceable once the chunk \
         compresses: {uncovered:?}"
    );

    // A guard that finds nothing because it looked at nothing is not a guard.
    let checked: i64 = db
        .count(
            "SELECT count(*) FROM pg_index i
              WHERE i.indisunique
                AND i.indrelid::regclass::text IN ('market_observations', 'ingest_polls')",
        )
        .await;
    assert_eq!(
        checked, 2,
        "both fact tables must have a unique index to check"
    );
    db.cleanup().await;
}
