use crate::config::Config;
use crate::db;
use crate::ids::MarketId;
use crate::valuation;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub config: Config,
}

pub fn router(pool: PgPool, config: Config) -> Router {
    let state = AppState { pool, config };
    Router::new()
        .route("/health", get(health))
        .route("/markets", get(list_markets))
        .route("/assets", get(list_assets))
        .route("/assets/{id}", get(get_asset))
        .route("/assets/{id}/prices", get(asset_prices))
        .route("/markets/{id}/valuations", get(valuations))
        .with_state(state)
}

#[derive(Serialize)]
struct Health {
    ok: bool,
    reasons: Vec<String>,
}

/// Returns 503 only when the ledger has stopped.
///
/// It reads the poll table, never the observation table. The observation table
/// records changes only, so a market where nothing moves writes nothing and
/// would look identical to an outage.
///
/// A missing price CHANGE is never a failure condition here. A card that holds
/// its price is normal, and a check that treats it as a fault sits red on a
/// healthy system until somebody stops reading it.
async fn health(State(s): State<AppState>) -> (StatusCode, Json<Health>) {
    let mut reasons = Vec::new();

    match db::newest_poll_age_seconds(&s.pool).await {
        Err(e) => {
            // The detail goes to the operator, not into an unauthenticated body.
            // ApiError already draws that line; this path was crossing it.
            tracing::error!(error = %e, "cannot read the poll history");
            reasons.push("the poll history cannot be read".to_string());
        }
        Ok(None) => {} // Nothing polled yet. A fresh install is not a fault.
        Ok(Some(age)) => {
            let limit = db::largest_poll_interval_seconds(&s.pool)
                .await
                .unwrap_or(db::DEFAULT_POLL_INTERVAL_SECONDS);
            if age > f64::from(limit) {
                reasons.push(format!("no poll for {age:.0}s, limit {limit}s"));
            }
        }
    }

    if let Some(free) = free_disk_bytes(&s.config.disk_check_path)
        && free < s.config.min_free_disk_bytes
    {
        // Compression needs room for a compressed copy before it drops the
        // original, and a full volume stops PostgreSQL outright.
        reasons.push(format!(
            "free disk {free} below {}",
            s.config.min_free_disk_bytes
        ));
    }

    let ok = reasons.is_empty();
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(Health { ok, reasons }))
}

#[derive(Serialize, sqlx::FromRow)]
struct Market {
    id: Uuid,
    platform: String,
    game_code: String,
}

/// The frontend resolves a market here rather than holding a seeded identifier.
/// A hardcoded UUID in the web application would break silently the first time a
/// second game joined the rotation.
async fn list_markets(State(s): State<AppState>) -> Result<Json<Vec<Market>>, ApiError> {
    let rows = sqlx::query_as::<_, Market>(
        "SELECT m.id, m.platform, g.code AS game_code
           FROM markets m
           JOIN games g ON g.id = m.game_id
          ORDER BY g.code, m.platform",
    )
    .fetch_all(&s.pool)
    .await?;
    Ok(Json(rows))
}

#[derive(Serialize, sqlx::FromRow)]
struct Asset {
    id: Uuid,
    name: String,
    rating: Option<i16>,
    rarity: String,
    version: String,
    position: Option<String>,
}

/// Capped like every other list. Discovery records an `assets` row for the
/// provider's whole catalogue, not just the covered few hundred, so without a
/// bound this is the one endpoint that would serialise tens of thousands of rows
/// on the host that also runs ingestion.
async fn list_assets(State(s): State<AppState>) -> Result<Json<Vec<Asset>>, ApiError> {
    let rows = sqlx::query_as::<_, Asset>(
        "SELECT id, name, rating, rarity, version, position
           FROM assets
          -- id settles two cards that share a name and a rating, so the cap
          -- always keeps the same ones.
          ORDER BY rating DESC NULLS LAST, name, id
          LIMIT $1",
    )
    .bind(s.config.api_row_cap)
    .fetch_all(&s.pool)
    .await?;
    Ok(Json(rows))
}

async fn get_asset(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Asset>, ApiError> {
    sqlx::query_as::<_, Asset>(
        "SELECT id, name, rating, rarity, version, position FROM assets WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&s.pool)
    .await?
    .map(Json)
    .ok_or(ApiError::NotFound)
}

#[derive(Deserialize)]
struct Range {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    /// Optional. A card trades independently on each platform, so a series that
    /// mixes two markets is not a price history.
    market: Option<Uuid>,
}

#[derive(Serialize, sqlx::FromRow)]
struct PricePoint {
    observed_at: DateTime<Utc>,
    price: i64,
    source: String,
    /// Always returned. Without it a caller that omits the `market` filter
    /// receives both platforms interleaved and cannot separate them, and a chart
    /// drawn from that zigzags between two markets.
    market_id: Uuid,
}

/// Bounded on purpose. Without a default range and a row cap, one request for an
/// old, frequently polled asset serialises hundreds of thousands of rows on the
/// same host that runs ingestion.
async fn asset_prices(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
    Query(range): Query<Range>,
) -> Result<Json<Vec<PricePoint>>, ApiError> {
    let to = range.to.unwrap_or_else(Utc::now);
    let from = range.from.unwrap_or(to - chrono::Duration::days(30));

    let rows = sqlx::query_as::<_, PricePoint>(
        // The cap takes from the NEWEST end and the result is then re-sorted
        // ascending. Capping an ascending scan keeps the oldest rows, so a range
        // wider than the cap silently dropped the most recent prices, and the
        // caller's "latest" became the price at the cap boundary.
        //
        // market_id and source break the tie. Without them two markets, or two
        // sources at one instant, come back in an arbitrary order and the same
        // request answers differently on different days.
        "SELECT * FROM (
            SELECT observed_at, price, source, market_id
              FROM market_observations
             WHERE asset_id = $1
               AND observed_at >= $2 AND observed_at <= $3
               AND ($5::uuid IS NULL OR market_id = $5)
             ORDER BY observed_at DESC, market_id DESC, source DESC
             LIMIT $4
         ) recent
         ORDER BY observed_at, market_id, source",
    )
    .bind(id)
    .bind(from)
    .bind(to)
    .bind(s.config.api_row_cap)
    .bind(range.market)
    .fetch_all(&s.pool)
    .await?;
    Ok(Json(rows))
}

async fn valuations(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<valuation::ClassValue>>, ApiError> {
    let rows = valuation::class_median(&s.pool, MarketId(id), s.config.api_row_cap).await?;
    Ok(Json(rows))
}

pub enum ApiError {
    NotFound,
    Internal(String),
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        Self::Internal(e.to_string())
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e.to_string())
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (code, message) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
            // The caller gets a readable sentence, the operator gets the detail.
            Self::Internal(detail) => {
                tracing::error!(%detail, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the server could not complete this request".to_string(),
                )
            }
        };
        (code, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

/// Free bytes on the volume holding `path`, or None when the call fails.
///
/// The layout comes from `libc`, never from a struct declared here. `fsblkcnt_t`
/// is 32 bits on macOS and 64 on Linux, so a hand written binding that assumes
/// one width reads the wrong fields on the other platform and returns nonsense.
/// It would do so silently, and this is the check that keeps compression from
/// filling the volume and stopping PostgreSQL.
fn free_disk_bytes(path: &str) -> Option<u64> {
    use std::ffi::CString;

    let c = CString::new(path).ok()?;
    let mut buf = std::mem::MaybeUninit::<libc::statvfs>::uninit();

    // SAFETY: `c` is a valid nul terminated path, and statvfs either fills `buf`
    // completely or returns non-zero, which is the only case we read it in.
    let failed = unsafe { libc::statvfs(c.as_ptr(), buf.as_mut_ptr()) } != 0;
    if failed {
        return None;
    }
    let buf = unsafe { buf.assume_init() };
    // `as` rather than `from`: these widths are platform dependent, so only a
    // cast compiles everywhere, and widening never truncates. Whichever of the
    // two is already u64 on the host makes its own cast redundant there, which
    // is what the allow covers.
    #[allow(clippy::unnecessary_cast)]
    Some((buf.f_bavail as u64).saturating_mul(buf.f_frsize as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A plausibility check rather than an exact one: the number must be a real
    /// figure for the volume, not the garbage a mismatched struct layout gives.
    #[test]
    fn free_disk_space_is_a_believable_figure() {
        let free = free_disk_bytes(".").expect("statvfs must answer for the working directory");
        assert!(free > 1024 * 1024, "an implausibly small figure: {free}");
        assert!(
            free < 1 << 50,
            "an implausibly large figure, which is what a wrong layout gives: {free}"
        );
    }

    #[test]
    fn a_path_that_does_not_exist_returns_none() {
        assert_eq!(free_disk_bytes("/no/such/path/here"), None);
    }
}
