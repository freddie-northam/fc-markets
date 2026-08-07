pub mod fixture;

use crate::archive::{Archive, Envelope, Kind, object_key};
use crate::db;
use crate::domain::{Observation, Rejection, timestamp_in_range};
use crate::ids::{AssetId, MarketId, PollOutcome, RunId, RunStatus};
use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{info, warn};

/// How many raw fragments a single run will log.
///
/// A provider schema change rejects every record at once. Logging all of them
/// would fill the disk and stop the database, which turns a recoverable parsing
/// fault into an outage. The archive holds the full payload regardless.
const MAX_LOGGED_FRAGMENTS: usize = 10;

#[derive(Debug, Default)]
pub struct RunReport {
    pub seen: i32,
    pub written: i32,
    pub rejected: i32,
    pub rejections: HashMap<&'static str, i32>,
}

impl RunReport {
    fn reject(&mut self, why: Rejection) {
        self.rejected += 1;
        *self.rejections.entry(why.as_str()).or_default() += 1;
    }

    /// A run that read nothing but rejected everything is not healthy, even
    /// though no step returned an error.
    pub fn status(&self) -> RunStatus {
        if self.seen > 0 && self.written == 0 && self.rejected == self.seen {
            RunStatus::Degraded
        } else {
            RunStatus::Ok
        }
    }
}

pub struct Context<'a> {
    pub pool: &'a PgPool,
    pub archive: &'a dyn Archive,
    pub run_id: RunId,
    /// The run's own start. Rule 2 bounds timestamps against this rather than the
    /// wall clock, so replaying one archive twice gives the same table.
    pub run_started_at: DateTime<Utc>,
    pub game_released_at: DateTime<Utc>,
}

/// Turns one archived payload into canonical rows.
///
/// The order matters: archive, then parse. A payload that was never stored
/// cannot be re-parsed later, so writing observations from it would quietly
/// void the replay guarantee.
pub async fn ingest_payload(
    ctx: &Context<'_>,
    envelope: &Envelope,
    batch: usize,
    resolved: &HashMap<(String, String), (AssetId, MarketId, String)>,
) -> Result<RunReport> {
    let key = object_key(
        fixture::SOURCE,
        Kind::Prices,
        &ctx.run_id.to_string(),
        batch,
        ctx.run_started_at,
    );
    // Fatal on purpose. See the note above.
    ctx.archive.put(&key, envelope)?;

    let quotes = fixture::parse_prices(&envelope.body)?;
    let mut report = RunReport::default();
    let mut observations = Vec::new();
    let mut polls = Vec::new();
    let polled_at = Utc::now();
    let mut logged = 0usize;

    for q in quotes {
        report.seen += 1;

        let Some((asset_id, market_id, _name)) =
            resolved.get(&(q.external_id.clone(), q.platform.to_string())).copied_tuple()
        else {
            report.reject(Rejection::UnknownAsset);
            continue;
        };

        let outcome = match validate(&q, ctx) {
            Err(why) => {
                report.reject(why);
                if logged < MAX_LOGGED_FRAGMENTS {
                    logged += 1;
                    warn!(external_id = %q.external_id, reason = why.as_str(), "record rejected");
                }
                match why {
                    Rejection::NoPrice => PollOutcome::NoPrice,
                    _ => PollOutcome::Rejected,
                }
            }
            Ok((price, observed_at)) => {
                observations.push(Observation {
                    asset_id,
                    market_id,
                    source: fixture::SOURCE,
                    observed_at,
                    price,
                    min_price: q.min_price,
                    max_price: q.max_price,
                    source_ref: Some(q.external_id.clone()),
                    ingest_run_id: ctx.run_id,
                });
                PollOutcome::Written
            }
        };

        polls.push((asset_id, market_id, polled_at, q.observed_at, outcome));
    }

    let written = db::write_batch(ctx.pool, &observations, &polls, ctx.run_id).await?;
    report.written = written as i32;

    // A quote that carried a price but conflicted on insert means the price did
    // not move. That is coverage evidence, not a write.
    info!(
        seen = report.seen,
        written = report.written,
        rejected = report.rejected,
        "batch ingested"
    );
    Ok(report)
}

/// Applies rules 2 and 3 to one quote.
fn validate(
    q: &fixture::ParsedQuote,
    ctx: &Context<'_>,
) -> Result<(i64, DateTime<Utc>), Rejection> {
    let Some(price) = q.price.filter(|p| *p > 0) else {
        return Err(Rejection::NoPrice);
    };
    // Rule 3. Never substitute our own clock for a missing source timestamp.
    let Some(observed_at) = q.observed_at else {
        return Err(Rejection::MissingTimestamp);
    };
    // Rule 2. Keep a bad timestamp out of the hypertable entirely.
    if !timestamp_in_range(observed_at, ctx.game_released_at, ctx.run_started_at) {
        return Err(Rejection::TimestampOutOfRange);
    }
    Ok((price, observed_at))
}

/// Small helper so the resolution map reads cleanly at the call site.
trait CopiedTuple {
    fn copied_tuple(self) -> Option<(AssetId, MarketId, String)>;
}

impl CopiedTuple for Option<&(AssetId, MarketId, String)> {
    fn copied_tuple(self) -> Option<(AssetId, MarketId, String)> {
        self.map(|(a, m, n)| (*a, *m, n.clone()))
    }
}
