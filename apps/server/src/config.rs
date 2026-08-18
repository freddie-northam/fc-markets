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
    /// The store's own region, and whether it serves paths or virtual hosted
    /// buckets. MinIO wants path style and ignores the region; a hosted store
    /// usually wants the opposite.
    pub object_store_region: String,
    pub object_store_path_style: bool,
    /// Which provider the run reads. `fixture` serves archived payloads from
    /// disk; `futdb` reads api.fut-db.com.
    pub source_name: String,
    pub source_base_url: String,
    /// Section 4.5 requires the source client to set an explicit request
    /// timeout. A source without one retries silently and spends the quota twice.
    pub source_api_key: Option<String>,
    pub daily_request_budget: u32,
    pub http_timeout: Duration,
    /// How stale the newest poll may get before /health calls it an outage.
    ///
    /// Stated rather than derived. The daily request budget, not the polling
    /// bands, is what bounds how often anything is polled: the budget runs out
    /// mid-day and nothing is polled until the quota window reopens. So the
    /// honest assertion is "at least one poll a day", with room for the window
    /// to drift.
    pub max_poll_age_seconds: i32,
    pub min_free_disk_bytes: u64,
    /// The database volume's capacity, in bytes.
    ///
    /// Set this where the server cannot see the database's volume. A platform
    /// that gives each service its own volume, as Railway does, stops `statvfs`
    /// from reaching that disk, so the size is asked of the database instead and
    /// taken from this figure. Leave it unset where the volume is shared, as
    /// docker-compose mounts it, and the filesystem answers directly.
    pub database_volume_bytes: Option<u64>,
    /// Which filesystem the disk check measures. Ignored when
    /// `database_volume_bytes` is set.
    ///
    /// Section 4.9 means the DATA volume: compression needs room for a
    /// compressed copy before it drops the original, and a full volume stops
    /// PostgreSQL. Once the server runs in its own container, its working
    /// directory is a different filesystem from the database's, so the default
    /// of "." measures the wrong thing and must be pointed at the data volume.
    pub disk_check_path: String,
    /// Optional so that tests and local development need no external service.
    ///
    /// The two are complementary, not alternatives. The heartbeat's silence is
    /// the alarm and needs a service watching for it; the webhook's message is
    /// the alarm and needs a human reading it.
    pub heartbeat_url: Option<String>,
    pub alert_webhook_url: Option<String>,
    pub api_row_cap: i64,
    pub bind_address: String,
    /// Which game the run works on. One row of `games`, resolved by code.
    pub game_code: String,
    /// How many assets the request budget can afford to follow. Section 4.7 sets
    /// this from what the source allows, not from what the code can manage. The
    /// tier bands hold the whole measured catalogue of 27,121 inside 18,000
    /// requests a day, so the default covers it with room for growth.
    pub asset_coverage: i64,
    pub discovery_cadence_seconds: i64,
    pub metadata_cadence_seconds: i64,
    /// How often the reference lists are re-walked. Monthly by default: they are
    /// static between titles, so a daily walk would spend 75 requests a day to
    /// learn nothing.
    pub reference_cadence_seconds: i64,
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
            // Required, not defaulted. A default would let a production
            // deployment that missed one variable authenticate with the
            // development credentials printed in .env.example, and succeed
            // quietly if the store happened to accept them.
            object_store_endpoint: req("OBJECT_STORE_ENDPOINT")?,
            object_store_bucket: req("OBJECT_STORE_BUCKET")?,
            object_store_access_key: req("OBJECT_STORE_ACCESS_KEY")?,
            object_store_secret_key: req("OBJECT_STORE_SECRET_KEY")?,
            object_store_region: opt("OBJECT_STORE_REGION", "us-east-1"),
            object_store_path_style: opt("OBJECT_STORE_PATH_STYLE", "true") != "false",
            source_name: opt("SOURCE", "fixture"),
            source_base_url: opt("SOURCE_BASE_URL", crate::ingest::futdb::DEFAULT_BASE_URL),
            source_api_key: std::env::var("SOURCE_API_KEY")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            daily_request_budget: num("DAILY_REQUEST_BUDGET", 18_000)?,
            http_timeout: Duration::from_secs(num("HTTP_TIMEOUT_SECONDS", 30)?),
            max_poll_age_seconds: num("MAX_POLL_AGE_SECONDS", 108_000)?,
            min_free_disk_bytes: num("MIN_FREE_DISK_BYTES", 5 * 1024 * 1024 * 1024)?,
            database_volume_bytes: opt_num("DATABASE_VOLUME_BYTES")?,
            disk_check_path: opt("DISK_CHECK_PATH", "."),
            heartbeat_url: optional("HEARTBEAT_URL"),
            alert_webhook_url: optional("ALERT_WEBHOOK_URL"),
            api_row_cap: num("API_ROW_CAP", 5_000)?,
            bind_address: opt("BIND_ADDRESS", "0.0.0.0:8090"),
            game_code: opt("GAME_CODE", "FC26"),
            asset_coverage: num("ASSET_COVERAGE", 40_000)?,
            discovery_cadence_seconds: num("DISCOVERY_CADENCE_SECONDS", 86_400)?,
            metadata_cadence_seconds: num("METADATA_CADENCE_SECONDS", 604_800)?,
            reference_cadence_seconds: num("REFERENCE_CADENCE_SECONDS", 2_592_000)?,
            fixture_dir: opt("FIXTURE_DIR", "fixtures/fixture"),
            ingest_interval_seconds: num("INGEST_INTERVAL_SECONDS", 900)?,
            pg_dump_path: opt("PG_DUMP_PATH", "pg_dump"),
            backup_keep: num("BACKUP_KEEP", 30)?,
        })
    }
}

/// An optional string. Blank reads as unset, so a deployment can clear one by
/// emptying it rather than by deleting the variable.
fn optional(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn req(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("{key} is not set"))
}

fn opt(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_string())
}

/// An optional number. An empty value reads as unset, so a deployment can clear
/// one by blanking it rather than by deleting the variable.
fn opt_num<T: std::str::FromStr>(key: &str) -> Result<Option<T>>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Err(_) => Ok(None),
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => raw
            .trim()
            .parse()
            .map(Some)
            .map_err(|e| anyhow::anyhow!("{key} is not a number: {e}")),
    }
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
