//! Section 9: the ingestion guarantees, against real PostgreSQL and TimescaleDB.

mod common;

use chrono::{Duration, Utc};
use common::*;
use fc_market::archive::Archive;
use fc_market::db;
use fc_market::domain::{Observation, Poll, PollResult};
use fc_market::ids::{AssetId, Platform, PollOutcome, RunStatus};
use fc_market::ingest::{self, RunOutcome, Runner};

const PS: Platform = Platform::Playstation;
const PC: Platform = Platform::Pc;

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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
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

    let asset: AssetId = db::asset_by_external_id(
        &db.pool,
        "alpha",
        db.game().await.unwrap().id,
        "101",
    )
    .await
    .unwrap()
    .unwrap();

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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    // The next title reuses identifier 101 for a different card. Without game_id
    // in the key it would resolve onto this season's asset and contaminate its
    // history for ever.
    let (next_game, _) = seed_second_game(&db.pool).await.unwrap();
    let mut config = db.config();
    config.game_code = "FC27".into();
    let runner = Runner {
        pool: &db.pool,
        archive: &archive,
        source: &source,
        config: &config,
    };
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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
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
        Card::new("101", "Marco Verratti").rating(84).recent(9_000, 6),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    source.set_cards(vec![
        Card::new("101", "Marco Verratti").rating(88).recent(40_000, 3),
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
    assert_eq!(by_name["Promo"], 900, "a promotional card is always tier A");
    assert_eq!(by_name["Elite"], 900);
    assert_eq!(by_name["Mid"], 3_600);
    assert_eq!(by_name["Filler"], 14_400);
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
    let runner = Runner {
        pool: &db.pool,
        archive: &archive,
        source: &source,
        config: &config,
    };
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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    source.set_cards(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_400, "2026-01-05T09:00:00Z"),
    ]);
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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2025-10-01T09:00:00Z"),
    ]);
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
        Card::new("101", "Undated").undated(PS, 9_000).undated(PC, 9_000),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        0,
        "we never substitute our own clock for a missing source timestamp"
    );
    assert_eq!(
        db.count("SELECT records_rejected::bigint FROM ingest_runs ORDER BY started_at DESC LIMIT 1")
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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
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
        Card::new("101", "Undated").undated(PS, 9_000).undated(PC, 9_000),
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
        Card::new("101", "Undated").undated(PS, 9_000).undated(PC, 9_000),
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
    let held = db::try_lock(&db.url).await.unwrap().expect("the lock must be free");

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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    let game = db.game().await.unwrap();
    let market = db.market(PS).await.unwrap();
    let asset = db::asset_by_external_id(&db.pool, "test", game.id, "101")
        .await
        .unwrap()
        .unwrap();
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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);

    // Discover first with a working archive, so the failure under test is the
    // price archive write and nothing else.
    let working = MemoryArchive::default();
    run_once(&db, &source, &working).await.unwrap();
    make_everything_due(&db.pool).await.unwrap();
    let before = db.count("SELECT count(*) FROM market_observations").await;

    let outcome = run_once(&db, &source, &RefusingArchive).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
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
    let source = TestSource::new(vec![
        Card::new("101", "Marco Verratti").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ]);
    run_once(&db, &source, &archive).await.unwrap();

    make_everything_due(&db.pool).await.unwrap();
    source.set_rate_limited(true);
    let outcome = run_once(&db, &source, &archive).await.unwrap();

    assert_eq!(status(&outcome), RunStatus::Degraded);
    db.cleanup().await;
}

/// The budget is daily. A per-run budget would let twelve runs spend twelve
/// budgets.
#[tokio::test]
async fn the_daily_request_budget_stops_a_run() {
    let db = TestDb::new().await.unwrap();
    let archive = MemoryArchive::default();
    let source = TestSource::new(vec![
        Card::new("101", "One").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
        Card::new("102", "Two").quote(PS, 9_000, "2026-01-05T09:00:00Z"),
    ])
    .batch_size(1);
    run_once(&db, &source, &archive).await.unwrap();

    let spent = db::requests_spent_today(&db.pool, "test").await.unwrap();
    assert!(spent > 0, "a run must record what it spent");

    let mut config = db.config();
    config.daily_request_budget = spent as u32;
    make_everything_due(&db.pool).await.unwrap();
    let runner = Runner {
        pool: &db.pool,
        archive: &archive,
        source: &source,
        config: &config,
    };
    let outcome = ingest::run(&runner).await.unwrap();

    assert_eq!(
        status(&outcome),
        RunStatus::Degraded,
        "the day's budget is already spent, so this run must stop and say so"
    );
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
    let runner = Runner {
        pool: &db.pool,
        archive: &archive,
        source: &source,
        config: &config,
    };
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
    // Priced two days ago, so the original run accepted it: its own start was
    // later than that.
    let source = TestSource::new(vec![Card::new("101", "Marco Verratti").recent(9_000, 48)]);
    let outcome = run_once(&db, &source, &archive).await.unwrap();
    let RunOutcome::Completed { run_id, .. } = outcome else {
        panic!("the run must complete")
    };
    assert_eq!(db.count("SELECT count(*) FROM market_observations").await, 2);

    // Move the recorded start back behind the quote. The wall clock has not
    // moved, so the two bounds now disagree, and only one of them can be the one
    // replay uses.
    age_runs(&db.pool, Duration::days(4)).await.unwrap();

    let config = db.config();
    let runner = Runner {
        pool: &db.pool,
        archive: &archive,
        source: &source,
        config: &config,
    };
    ingest::replay(&runner, run_id).await.unwrap();

    assert_eq!(
        db.count("SELECT count(*) FROM market_observations").await,
        0,
        "replay must bound against the run's recorded start; a wall clock bound \
         would have kept these rows and made one archive give two tables"
    );
    db.cleanup().await;
}
