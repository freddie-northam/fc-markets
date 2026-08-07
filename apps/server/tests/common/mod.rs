//! Shared harness for the database tests.
//!
//! Every test gets its own database with the real migrations applied, because
//! the guarantees under test are database guarantees. A mock would assert that
//! our mock behaves like our mock: the idempotency index, the hypertable
//! partitioning and the compression settings are exactly the parts that cannot
//! be checked any other way.

#![allow(dead_code)]

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use fc_market::archive::{Archive, Envelope};
use fc_market::config::Config;
use fc_market::db;
use fc_market::domain::{AssetAttributes, Rarity};
use fc_market::ids::{GameId, MarketId, Platform};

const PS_: Platform = Platform::Playstation;
const PC_: Platform = Platform::Pc;
use fc_market::source::{FetchError, FetchResult, Listing, ParsedQuote, Source};
use sqlx::{Executor, PgPool};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use uuid::Uuid;

/// The game seeded by migration 0002.
pub const GAME_CODE: &str = "FC26";
pub const RELEASED_AT: &str = "2025-09-26T00:00:00Z";

pub fn ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

/// A whole second, `n` hours before now.
///
/// Tests that assert a run closes `ok` must use this rather than a fixed date.
/// The drift checks treat a feed whose newest source timestamp is a day old as
/// frozen, so a hardcoded date would pass today and start failing tomorrow.
pub fn hours_ago(n: i64) -> DateTime<Utc> {
    let seconds = (Utc::now() - Duration::hours(n)).timestamp();
    DateTime::from_timestamp(seconds, 0).expect("a valid instant")
}

/// A throwaway database, migrated and dropped with the test.
pub struct TestDb {
    pub pool: PgPool,
    pub url: String,
    name: String,
    admin_url: String,
}

impl TestDb {
    pub async fn new() -> Result<Self> {
        let _ = dotenvy::dotenv();
        let base = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://fcmarket:fcmarket@localhost:5434/fcmarket".into());
        let admin_url = replace_database(&base, "postgres");
        let name = format!("fcm_test_{}", Uuid::now_v7().simple());

        let admin = sqlx::PgPool::connect(&admin_url).await?;
        admin
            .execute(format!(r#"CREATE DATABASE "{name}""#).as_str())
            .await?;
        admin.close().await;

        let url = replace_database(&base, &name);
        let pool = db::connect(&url).await?;
        db::migrate(&pool).await?;

        Ok(Self {
            pool,
            url,
            name,
            admin_url,
        })
    }

    /// Explicit rather than a Drop impl, because dropping a database is async
    /// and a Drop that blocks inside the test runtime deadlocks.
    pub async fn cleanup(self) {
        let name = self.name.clone();
        let admin_url = self.admin_url.clone();
        self.pool.close().await;
        if let Ok(admin) = sqlx::PgPool::connect(&admin_url).await {
            let _ = admin
                .execute(format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#).as_str())
                .await;
            admin.close().await;
        }
    }

    pub async fn game(&self) -> Result<db::Game> {
        db::game_by_code(&self.pool, GAME_CODE).await
    }

    pub async fn markets(&self) -> Result<Vec<(MarketId, Platform)>> {
        let game = self.game().await?;
        db::markets_for_game(&self.pool, game.id).await
    }

    pub async fn market(&self, platform: Platform) -> Result<MarketId> {
        Ok(self
            .markets()
            .await?
            .into_iter()
            .find(|(_, p)| *p == platform)
            .expect("the migration seeds both platforms")
            .0)
    }

    /// Config pointed at this database. The advisory lock opens its own
    /// connection, so the URL has to be real.
    pub fn config(&self) -> Config {
        Config {
            database_url: self.url.clone(),
            ..test_config()
        }
    }

    pub async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .expect("count query")
    }
}

fn replace_database(url: &str, name: &str) -> String {
    let cut = url.rfind('/').expect("a connection URL has a path");
    format!("{}/{name}", &url[..cut])
}

/// Config with every knob at a value that suits a test.
pub fn test_config() -> Config {
    Config {
        database_url: String::new(),
        object_store_endpoint: String::new(),
        object_store_bucket: String::new(),
        object_store_access_key: String::new(),
        object_store_secret_key: String::new(),
        source_api_key: None,
        daily_request_budget: 1_000,
        http_timeout: std::time::Duration::from_secs(5),
        min_free_disk_bytes: 0,
        heartbeat_url: None,
        api_row_cap: 5_000,
        bind_address: "127.0.0.1:0".into(),
        game_code: GAME_CODE.into(),
        asset_coverage: 600,
        // Zero so that every run performs both slow steps. A test that had to
        // wait a day for discovery would test nothing.
        discovery_cadence_seconds: 0,
        metadata_cadence_seconds: 0,
        fixture_dir: String::new(),
        ingest_interval_seconds: 900,
        pg_dump_path: "pg_dump".into(),
        backup_keep: 30,
    }
}

// ---------------------------------------------------------------------------
// Archive doubles
// ---------------------------------------------------------------------------

/// Keeps envelopes in memory under the same keys the real archive uses.
#[derive(Default)]
pub struct MemoryArchive {
    objects: Mutex<HashMap<String, Envelope>>,
}

impl MemoryArchive {
    pub fn len(&self) -> usize {
        self.objects.lock().unwrap().len()
    }

    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<_> = self.objects.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }
}

#[async_trait]
impl Archive for MemoryArchive {
    async fn put(&self, key: &str, envelope: &Envelope) -> Result<()> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), envelope.clone());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Envelope> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no object at {key}"))
    }
}

/// Refuses every write. Section 4.10 makes the archive a precondition, so this
/// must stop the batch rather than let it write observations.
pub struct RefusingArchive;

#[async_trait]
impl Archive for RefusingArchive {
    async fn put(&self, _key: &str, _envelope: &Envelope) -> Result<()> {
        anyhow::bail!("the object store refused the write")
    }

    async fn get(&self, _key: &str) -> Result<Envelope> {
        anyhow::bail!("the object store refused the read")
    }
}

// ---------------------------------------------------------------------------
// Source double
// ---------------------------------------------------------------------------

/// One card the test source will serve.
#[derive(Clone)]
pub struct Card {
    pub external_id: String,
    pub name: String,
    pub rating: Option<i16>,
    pub version: String,
    pub rarity: Rarity,
    pub ea_base_id: Option<i64>,
    /// Per platform: price, min price, source timestamp.
    pub quotes: Vec<(Platform, Option<i64>, Option<i64>, Option<DateTime<Utc>>)>,
    /// The name the PRICE payload carries, when it differs from `name`. Rule 1
    /// compares this against the resolved asset.
    pub payload_name: Option<String>,
}

impl Card {
    pub fn new(external_id: &str, name: &str) -> Self {
        Self {
            external_id: external_id.into(),
            name: name.into(),
            rating: Some(84),
            version: "base".into(),
            rarity: Rarity::Rare,
            ea_base_id: None,
            quotes: Vec::new(),
            payload_name: None,
        }
    }

    pub fn rating(mut self, rating: i16) -> Self {
        self.rating = Some(rating);
        self
    }

    pub fn version(mut self, version: &str) -> Self {
        self.version = version.into();
        self
    }

    pub fn ea_base_id(mut self, id: i64) -> Self {
        self.ea_base_id = Some(id);
        self
    }

    pub fn payload_name(mut self, name: &str) -> Self {
        self.payload_name = Some(name.into());
        self
    }

    pub fn quote(mut self, platform: Platform, price: i64, at: &str) -> Self {
        self.quotes.push((platform, Some(price), Some(200), Some(ts(at))));
        self
    }

    /// A quote priced `hours` ago, on both platforms.
    ///
    /// Both, because coverage creates poll state for every market: quoting one
    /// platform leaves the other recording `no_price`, which is correct but
    /// makes a count assertion read as though something failed.
    pub fn recent(mut self, price: i64, hours: i64) -> Self {
        let at = hours_ago(hours);
        self.quotes.push((PS_, Some(price), Some(200), Some(at)));
        self.quotes.push((PC_, Some(price * 97 / 100), Some(200), Some(at)));
        self
    }

    pub fn quote_with_floor(mut self, platform: Platform, price: i64, floor: i64, at: &str) -> Self {
        self.quotes
            .push((platform, Some(price), Some(floor), Some(ts(at))));
        self
    }

    /// A quote the provider returned with no price at all.
    pub fn no_price(mut self, platform: Platform, at: &str) -> Self {
        self.quotes.push((platform, None, Some(200), Some(ts(at))));
        self
    }

    /// A quote with a price but no usable timestamp. Rule 3 rejects it.
    pub fn undated(mut self, platform: Platform, price: i64) -> Self {
        self.quotes.push((platform, Some(price), Some(200), None));
        self
    }

    /// A quote whose timestamp sits outside the game's lifetime. Rule 2 rejects it.
    pub fn at_raw(mut self, platform: Platform, price: i64, at: DateTime<Utc>) -> Self {
        self.quotes.push((platform, Some(price), Some(200), Some(at)));
        self
    }
}

/// A provider under the test's control. It serves exactly what it was given and
/// records what it was asked for.
pub struct TestSource {
    name: &'static str,
    parser_version: &'static str,
    cards: Mutex<Vec<Card>>,
    rate_limited: AtomicBool,
    pub batch_size: usize,
    pub price_requests: Mutex<Vec<Vec<String>>>,
}

impl TestSource {
    pub fn new(cards: Vec<Card>) -> Self {
        Self {
            name: "test",
            parser_version: "test/1",
            cards: Mutex::new(cards),
            rate_limited: AtomicBool::new(false),
            batch_size: 50,
            price_requests: Mutex::new(Vec::new()),
        }
    }

    pub fn named(name: &'static str, cards: Vec<Card>) -> Self {
        let mut source = Self::new(cards);
        source.name = name;
        source
    }

    /// Flippable mid-test, so a run can discover assets normally and only then
    /// meet a provider that has started refusing.
    pub fn set_rate_limited(&self, on: bool) {
        self.rate_limited.store(on, Ordering::SeqCst);
    }

    fn is_rate_limited(&self) -> bool {
        self.rate_limited.load(Ordering::SeqCst)
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Replaces the served cards, which is how a test simulates the next poll.
    pub fn set_cards(&self, cards: Vec<Card>) {
        *self.cards.lock().unwrap() = cards;
    }

    fn selected(&self, wanted: &[String]) -> Vec<Card> {
        self.cards
            .lock()
            .unwrap()
            .iter()
            .filter(|c| wanted.iter().any(|w| *w == c.external_id))
            .cloned()
            .collect()
    }

    fn envelope(&self, url: &str, requested: Vec<String>) -> Envelope {
        Envelope::new(url, requested, 200, serde_json::json!({}), Utc::now())
    }
}

#[async_trait]
impl Source for TestSource {
    fn name(&self) -> &'static str {
        self.name
    }

    fn parser_version(&self) -> &'static str {
        self.parser_version
    }

    fn price_batch_size(&self) -> usize {
        self.batch_size
    }

    fn metadata_batch_size(&self) -> usize {
        self.batch_size
    }

    async fn fetch_prices(&self, external_ids: &[String]) -> FetchResult {
        self.price_requests
            .lock()
            .unwrap()
            .push(external_ids.to_vec());
        if self.is_rate_limited() {
            return Err(FetchError::RateLimited);
        }
        let selected = self.selected(external_ids);
        let mut envelope = self.envelope("test://prices", external_ids.to_vec());
        envelope.body = serde_json::to_value(
            selected
                .iter()
                .map(|c| c.external_id.clone())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        Ok(envelope)
    }

    fn parse_prices(&self, envelope: &Envelope) -> Result<Vec<ParsedQuote>> {
        let ids: Vec<String> = serde_json::from_value(envelope.body.clone()).unwrap_or_default();
        let cards = self.selected(&ids);
        let mut out = Vec::new();
        for card in cards {
            for (platform, price, min_price, observed_at) in &card.quotes {
                out.push(ParsedQuote {
                    external_id: card.external_id.clone(),
                    platform: *platform,
                    price: *price,
                    min_price: *min_price,
                    max_price: None,
                    observed_at: *observed_at,
                    name: card.payload_name.clone().or_else(|| Some(card.name.clone())),
                });
            }
        }
        Ok(out)
    }

    async fn fetch_asset_list(&self) -> FetchResult {
        if self.is_rate_limited() {
            return Err(FetchError::RateLimited);
        }
        Ok(self.envelope("test://assets", Vec::new()))
    }

    fn parse_asset_list(&self, _envelope: &Envelope) -> Result<Vec<Listing>> {
        Ok(self
            .cards
            .lock()
            .unwrap()
            .iter()
            .map(|c| Listing {
                external_id: c.external_id.clone(),
                name: c.name.clone(),
            })
            .collect())
    }

    async fn fetch_metadata(&self, external_ids: &[String]) -> FetchResult {
        if self.is_rate_limited() {
            return Err(FetchError::RateLimited);
        }
        let mut envelope = self.envelope("test://metadata", external_ids.to_vec());
        envelope.body = serde_json::to_value(external_ids).unwrap();
        Ok(envelope)
    }

    fn parse_metadata(&self, envelope: &Envelope) -> Result<Vec<AssetAttributes>> {
        let ids: Vec<String> = serde_json::from_value(envelope.body.clone()).unwrap_or_default();
        Ok(self
            .selected(&ids)
            .into_iter()
            .map(|c| AssetAttributes {
                name: c.name,
                external_id: c.external_id,
                ea_base_id: c.ea_base_id,
                rating: c.rating,
                rarity: c.rarity,
                version: c.version,
                position: Some("CM".into()),
                league_id: Some(13),
                nation_id: Some(18),
                club_id: Some(9),
                skill_moves: Some(3),
                weak_foot: Some(3),
                pace: Some(80),
                shooting: Some(80),
                passing: Some(80),
                dribbling: Some(80),
                defending: Some(50),
                physicality: Some(70),
                playstyle_count: Some(2),
            })
            .collect())
    }
}

/// A second game, so that a provider identifier reused across titles can be
/// tested. Section 4.4: without `game_id` in the key, next season's card
/// resolves onto this season's asset and contaminates its history for ever.
pub async fn seed_second_game(pool: &PgPool) -> Result<(GameId, MarketId)> {
    let game = GameId::new();
    let market = MarketId::new();
    sqlx::query("INSERT INTO games (id, code, name, released_at) VALUES ($1,'FC27','EA SPORTS FC 27',$2)")
        .bind(game.0)
        .bind(ts("2026-09-25T00:00:00Z"))
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO markets (id, game_id, platform) VALUES ($1,$2,'playstation')")
        .bind(market.0)
        .bind(game.0)
        .execute(pool)
        .await?;
    Ok((game, market))
}

/// Ages every poll so the next run treats each asset as due.
pub async fn make_everything_due(pool: &PgPool) -> Result<()> {
    sqlx::query("UPDATE asset_poll_state SET last_polled_at = NULL")
        .execute(pool)
        .await?;
    Ok(())
}

/// Moves a run's start backwards, so a cadence gate that reads the run records
/// behaves as though time passed.
pub async fn age_runs(pool: &PgPool, by: Duration) -> Result<()> {
    sqlx::query("UPDATE ingest_runs SET started_at = started_at - $1::interval")
        .bind(by)
        .execute(pool)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Direct seeding
// ---------------------------------------------------------------------------
//
// The valuation query is what is under test in those files, so the rows go in
// directly. Driving the whole pipeline to produce one stale price would test the
// pipeline again and leave the interesting case hard to reach.

use fc_market::ids::AssetId;

pub async fn seed_asset(
    pool: &PgPool,
    game: GameId,
    name: &str,
    rating: i16,
    version: &str,
) -> Result<AssetId> {
    let player = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO players (id, name) VALUES ($1, $2)")
        .bind(player)
        .bind(name)
        .execute(pool)
        .await?;

    let asset = AssetId::new();
    sqlx::query(
        "INSERT INTO assets (id, game_id, player_id, name, rating, rarity, version)
         VALUES ($1,$2,$3,$4,$5,'rare',$6)",
    )
    .bind(asset.0)
    .bind(game.0)
    .bind(player)
    .bind(name)
    .bind(rating)
    .bind(version)
    .execute(pool)
    .await?;
    Ok(asset)
}

/// `failures` above zero is what removes an asset from the valuation: a feed we
/// keep failing to read votes with a stale, upward biased price otherwise.
pub async fn seed_poll_state(
    pool: &PgPool,
    asset: AssetId,
    market: MarketId,
    failures: i32,
    last_polled_at: Option<DateTime<Utc>>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO asset_poll_state
           (asset_id, market_id, poll_interval_seconds, last_polled_at, consecutive_failures)
         VALUES ($1,$2,900,$3,$4)",
    )
    .bind(asset.0)
    .bind(market.0)
    .bind(last_polled_at)
    .bind(failures)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn seed_observation(
    pool: &PgPool,
    asset: AssetId,
    market: MarketId,
    source: &str,
    observed_at: DateTime<Utc>,
    price: i64,
    min_price: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO market_observations
           (asset_id, market_id, source, observed_at, price, min_price)
         VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(asset.0)
    .bind(market.0)
    .bind(source)
    .bind(observed_at)
    .bind(price)
    .bind(min_price)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn seed_poll(
    pool: &PgPool,
    asset: AssetId,
    market: MarketId,
    polled_at: DateTime<Utc>,
    outcome: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO ingest_polls (asset_id, market_id, polled_at, outcome)
         VALUES ($1,$2,$3,$4)",
    )
    .bind(asset.0)
    .bind(market.0)
    .bind(polled_at)
    .bind(outcome)
    .execute(pool)
    .await?;
    Ok(())
}
