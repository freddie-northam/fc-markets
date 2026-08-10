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
    pub source_api_key: Option<String>,
    pub daily_request_budget: u32,
    pub http_timeout: Duration,
    pub min_free_disk_bytes: u64,
    /// Optional so that tests and local development need no external service.
    pub heartbeat_url: Option<String>,
    pub api_row_cap: i64,
    pub bind_address: String,
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
            heartbeat_url: std::env::var("HEARTBEAT_URL").ok().filter(|s| !s.is_empty()),
            api_row_cap: num("API_ROW_CAP", 5_000)?,
            bind_address: opt("BIND_ADDRESS", "0.0.0.0:8080"),
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
