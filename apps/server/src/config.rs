use anyhow::{Context, Result};
use std::time::Duration;

/// Read once in main and passed down. No configuration crate and no file format,
/// because environment variables already carry everything the server needs.
#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub object_store_endpoint: String,
    pub object_store_bucket: String,
    pub object_store_access_key: String,
    pub object_store_secret_key: String,
    /// Read by a provider module. No provider is available yet, so nothing
    /// consumes these two today. They stay because section 4.5 requires the
    /// source client to set an explicit request timeout, and a source added
    /// without one would retry silently and spend the quota twice.
    pub source_api_key: Option<String>,
    pub daily_request_budget: u32,
    pub http_timeout: Duration,
    pub min_free_disk_bytes: u64,
    /// Which filesystem the disk check measures.
    ///
    /// Section 4.9 means the DATA volume: compression needs room for a
    /// compressed copy before it drops the original, and a full volume stops
    /// PostgreSQL. Once the server runs in its own container, its working
    /// directory is a different filesystem from the database's, so the default
    /// of "." measures the wrong thing and must be pointed at the data volume.
    pub disk_check_path: String,
    /// Optional so that tests and local development need no external service.
    pub heartbeat_url: Option<String>,
    pub api_row_cap: i64,
    pub bind_address: String,
    /// Which game the run works on. One row of `games`, resolved by code.
    pub game_code: String,
    /// How many assets the request budget can afford to follow. Section 4.7 sets
    /// this from what the source allows, not from what the code can manage.
    pub asset_coverage: i64,
    pub discovery_cadence_seconds: i64,
    pub metadata_cadence_seconds: i64,
    /// Where the fixture source reads its payloads from. A real provider replaces
    /// the source module and ignores this.
    pub fixture_dir: String,
    /// How often the `serve` interval task starts a run.
    pub ingest_interval_seconds: u64,
    /// `pg_dump` must match the server's major version. The container image ships
    /// a matching one; a host binary often does not.
    pub pg_dump_path: String,
    pub backup_keep: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        // Ignore a missing .env: production supplies real environment variables.
        let _ = dotenvy::dotenv();

        Ok(Self {
            database_url: req("DATABASE_URL")?,
            object_store_endpoint: opt("OBJECT_STORE_ENDPOINT", "http://localhost:9002"),
            object_store_bucket: opt("OBJECT_STORE_BUCKET", "fc-market-raw"),
            object_store_access_key: opt("OBJECT_STORE_ACCESS_KEY", "fcmarket"),
            object_store_secret_key: opt("OBJECT_STORE_SECRET_KEY", "fcmarketsecret"),
            source_api_key: std::env::var("SOURCE_API_KEY").ok(),
            daily_request_budget: num("DAILY_REQUEST_BUDGET", 18_000)?,
            http_timeout: Duration::from_secs(num("HTTP_TIMEOUT_SECONDS", 30)?),
            min_free_disk_bytes: num("MIN_FREE_DISK_BYTES", 5 * 1024 * 1024 * 1024)?,
            disk_check_path: opt("DISK_CHECK_PATH", "."),
            heartbeat_url: std::env::var("HEARTBEAT_URL")
                .ok()
                .filter(|s| !s.is_empty()),
            api_row_cap: num("API_ROW_CAP", 5_000)?,
            bind_address: opt("BIND_ADDRESS", "0.0.0.0:8090"),
            game_code: opt("GAME_CODE", "FC26"),
            asset_coverage: num("ASSET_COVERAGE", 600)?,
            discovery_cadence_seconds: num("DISCOVERY_CADENCE_SECONDS", 86_400)?,
            metadata_cadence_seconds: num("METADATA_CADENCE_SECONDS", 604_800)?,
            fixture_dir: opt("FIXTURE_DIR", "fixtures/fixture"),
            ingest_interval_seconds: num("INGEST_INTERVAL_SECONDS", 900)?,
            pg_dump_path: opt("PG_DUMP_PATH", "pg_dump"),
            backup_keep: num("BACKUP_KEEP", 30)?,
        })
    }
}

fn req(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("{key} is not set"))
}

fn opt(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_string())
}

fn num<T: std::str::FromStr>(key: &str, fallback: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Err(_) => Ok(fallback),
        Ok(raw) => raw
            .parse()
            .map_err(|e| anyhow::anyhow!("{key} is not a number: {e}")),
    }
}
