//! Run orchestration.
//!
//! One entry point serves both the `ingest` command and the `serve` interval
//! task. The advisory lock in section 4.5 is what keeps them apart: without it a
//! slow scheduled run and a manual run spend the request budget twice and race
//! on poll state.

pub mod discovery;
pub mod drift;
pub mod fixture;

use crate::archive::{Archive, Envelope, Kind, object_key};
use crate::config::Config;
use crate::db::{self, PollTarget};
use crate::domain::{Observation, Poll, PollResult, Rejection, names_match, timestamp_in_range};
use crate::ids::{Platform, PollOutcome, RunId, RunStatus};
use crate::source::{FetchError, Source};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

/// How many raw fragments a single run will log.
///
/// A provider schema change rejects every record at once. Logging all of them
/// would fill the disk and stop the database, which turns a recoverable parsing
/// fault into an outage. The archive holds the full payload regardless.
const MAX_LOGGED_FRAGMENTS: usize = 10;

#[derive(Debug)]
pub enum RunOutcome {
    /// Another run holds the lock. The next scheduled tick is the retry.
    Skipped,
    Completed {
        run_id: RunId,
        status: RunStatus,
        counts: db::RunCounts,
    },
}

/// Everything one run accumulates. Kept in one place so that the close path
/// cannot forget a field.
#[derive(Debug, Default)]
pub(crate) struct Tally {
    seen: i32,
    written: i32,
    unchanged: i32,
    rejected: i32,
    no_price: i32,
    requests: u32,
    /// Recorded so replay can re-read exactly what this run parsed.
    price_keys: Vec<String>,
    /// Every reason a batch or a check degraded the run.
    degraded: Vec<String>,
    /// The slow steps this run performed. Recorded on the run so the next run can
    /// tell how long ago each last ran, without a table of its own.
    steps: Vec<&'static str>,
    logged_fragments: usize,
}

impl Tally {
    pub(crate) fn degrade(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        warn!(reason = %reason, "run degraded");
        self.degraded.push(reason);
    }

    fn counts(&self) -> db::RunCounts {
        db::RunCounts {
            seen: self.seen,
            written: self.written,
            rejected: self.rejected,
        }
    }

    fn status(&self) -> RunStatus {
        if !self.degraded.is_empty() {
            return RunStatus::Degraded;
        }
        // A run that read something and rejected all of it is not healthy, even
        // though no step returned an error.
        if self.seen > 0 && self.rejected == self.seen {
            return RunStatus::Degraded;
        }
        RunStatus::Ok
    }
}

/// The request budget for one run.
///
/// It starts at whatever the day has left, not at the whole daily figure. A
/// per-run budget would let twelve runs spend twelve budgets.
pub(crate) struct Budget {
    pub(crate) remaining: u32,
}

impl Budget {
    pub(crate) fn take(&mut self) -> bool {
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }
}

pub struct Runner<'a> {
    pub pool: &'a PgPool,
    pub archive: &'a dyn Archive,
    pub source: &'a dyn Source,
    pub config: &'a Config,
}

/// One ingestion pass.
///
/// The run record always closes. A panic inside a batch would leave it open, so
/// section 4.9 also has the next start reap runs whose heartbeat stopped.
pub async fn run(runner: &Runner<'_>) -> Result<RunOutcome> {
    let reaped = db::reap_abandoned_runs(runner.pool).await?;
    if reaped > 0 {
        warn!(reaped, "cleared runs left open by a killed process");
    }

    // Stop rather than wait. The next scheduled tick is the retry.
    let Some(lock) = db::try_lock(&runner.config.database_url).await? else {
        warn!("another ingest run holds the lock, stopping");
        return Ok(RunOutcome::Skipped);
    };

    // The lock is released on every path, including the error one. A run that
    // kept it would lock out every later run in this process.
    let outcome = run_locked(runner).await;
    lock.release().await;
    outcome
}

async fn run_locked(runner: &Runner<'_>) -> Result<RunOutcome> {
    let source_name = runner.source.name();
    let game = db::game_by_code(runner.pool, &runner.config.game_code).await?;
    let (run_id, started_at) =
        db::open_run(runner.pool, source_name, runner.source.parser_version()).await?;
    info!(%run_id, source = source_name, "run opened");

    let spent_today = db::requests_spent_today(runner.pool, source_name).await?;
    let mut budget = Budget {
        remaining: runner
            .config
            .daily_request_budget
            .saturating_sub(spent_today.max(0) as u32),
    };

    let mut tally = Tally::default();
    let result = execute(runner, &game, run_id, started_at, &mut budget, &mut tally).await;

    let error = match result {
        Ok(()) => None,
        Err(e) => {
            warn!(error = %e, "run failed");
            Some(e.to_string())
        }
    };

    // Drift checks run even on a failed run, because a partial run still tells us
    // whether the feed is serving rubbish.
    match drift::checks(runner.pool, run_id, tally.seen, tally.no_price).await {
        Ok(reasons) => reasons.into_iter().for_each(|r| tally.degrade(r)),
        Err(e) => tally.degrade(format!("drift checks did not run: {e}")),
    }

    let status = if error.is_some() {
        RunStatus::Failed
    } else {
        tally.status()
    };

    let mut metadata = serde_json::json!({
        "requests": tally.requests,
        "price_keys": tally.price_keys,
        "unchanged": tally.unchanged,
        "no_price": tally.no_price,
        "degraded_reasons": tally.degraded,
    });
    for step in &tally.steps {
        metadata[*step] = serde_json::Value::Bool(true);
    }

    db::close_run(
        runner.pool,
        run_id,
        status,
        tally.counts(),
        error.as_deref(),
        metadata,
    )
    .await?;

    info!(
        %run_id, ?status,
        seen = tally.seen, written = tally.written,
        unchanged = tally.unchanged, rejected = tally.rejected,
        requests = tally.requests,
        "run closed"
    );

    Ok(RunOutcome::Completed {
        run_id,
        status,
        counts: tally.counts(),
    })
}

/// The body of a run, separated so that `run` always closes the record.
async fn execute(
    runner: &Runner<'_>,
    game: &db::Game,
    run_id: RunId,
    started_at: DateTime<Utc>,
    budget: &mut Budget,
    tally: &mut Tally,
) -> Result<()> {
    // Discovery and metadata share the price budget, so they run first: an asset
    // that is not discovered is never polled, and one with no attributes has no
    // valuation input whatever its price history holds.
    discovery::maybe_discover(runner, game, run_id, budget, tally).await?;
    discovery::maybe_refresh_metadata(runner, game, run_id, budget, tally).await?;
    // Coverage runs after the metadata step, so the tier expression reads the
    // ratings this run just learned rather than last week's.
    discovery::apply_coverage(runner, game).await?;

    ingest_prices(runner, game, run_id, started_at, budget, tally).await
}

async fn ingest_prices(
    runner: &Runner<'_>,
    game: &db::Game,
    run_id: RunId,
    started_at: DateTime<Utc>,
    budget: &mut Budget,
    tally: &mut Tally,
) -> Result<()> {
    // Checked before the query, not after it. With nothing left to spend the
    // query returns no rows, which is indistinguishable from nothing being due,
    // and the run would close ok and send a heartbeat while the ledger had
    // stopped. Silence is the alarm, so the run must not look healthy here.
    if budget.remaining == 0 {
        tally.degrade("the daily request budget was already spent, so no price was read");
        return Ok(());
    }

    let batch_size = runner.source.price_batch_size().max(1);
    // Ask for no more identifiers than the day can still pay for.
    let affordable = (budget.remaining as i64).saturating_mul(batch_size as i64);
    let due = db::due_external_ids(runner.pool, runner.source.name(), game.id, affordable).await?;

    if due.is_empty() {
        info!("no assets are due");
        return Ok(());
    }
    info!(assets = due.len(), "assets due for a price read");

    for (batch, chunk) in due.chunks(batch_size).enumerate() {
        if !budget.take() {
            tally.degrade("the daily request budget stopped this run");
            break;
        }
        db::touch_heartbeat(runner.pool, run_id).await?;
        tally.requests += 1;

        let envelope = match runner.source.fetch_prices(chunk).await {
            Ok(envelope) => envelope,
            Err(FetchError::RateLimited) => {
                // Section 4.5: this stops the run. It is not a per-record
                // rejection, and retrying inside the same run would spend a quota
                // the provider has already said is gone.
                tally.degrade("the provider rate limited this run");
                break;
            }
            Err(FetchError::Other(e)) => {
                tally.degrade(format!("batch {batch} could not be fetched: {e}"));
                continue;
            }
        };

        // The archive write is a precondition. A payload that was never stored
        // cannot be re-parsed, so writing observations from it would quietly void
        // the replay guarantee.
        let key = object_key(
            runner.source.name(),
            Kind::Prices,
            &run_id.to_string(),
            batch,
            started_at,
        );
        if let Err(e) = runner.archive.put(&key, &envelope).await {
            tally.degrade(format!("batch {batch} was not archived, so it was not parsed: {e}"));
            continue;
        }
        tally.price_keys.push(key);

        if let Err(e) = ingest_batch(runner, game, run_id, started_at, chunk, &envelope, tally).await
        {
            tally.degrade(format!("batch {batch} could not be stored: {e}"));
        }
    }
    Ok(())
}

/// Turns one archived payload into canonical rows.
///
/// It iterates over what we ASKED for, not over what came back. An identifier the
/// provider silently dropped must still produce a poll row, or the coverage
/// record would claim we never looked.
async fn ingest_batch(
    runner: &Runner<'_>,
    game: &db::Game,
    run_id: RunId,
    started_at: DateTime<Utc>,
    requested: &[String],
    envelope: &Envelope,
    tally: &mut Tally,
) -> Result<()> {
    let quotes = runner.source.parse_prices(envelope)?;
    let targets =
        db::poll_targets_for(runner.pool, runner.source.name(), game.id, requested).await?;

    let mut by_key: HashMap<(&str, Platform), _> = HashMap::with_capacity(quotes.len());
    for quote in &quotes {
        by_key.insert((quote.external_id.as_str(), quote.platform), quote);
    }

    let polled_at = Utc::now();
    let mut polls = Vec::with_capacity(targets.len());

    for target in &targets {
        tally.seen += 1;
        let quote = by_key.get(&(target.external_id.as_str(), target.platform));

        let (result, source_observed_at) = match quote {
            // We asked and the provider returned nothing for this pair. That is
            // a failure to read, not a stable price.
            None => (PollResult::Failed(PollOutcome::NoPrice), None),
            Some(quote) => {
                let observed = quote.observed_at;
                match validate(game, target, quote, started_at) {
                    Ok((price, observed_at)) => (
                        PollResult::Priced(Observation {
                            asset_id: target.asset_id,
                            market_id: target.market_id,
                            source: runner.source.name(),
                            observed_at,
                            price,
                            min_price: quote.min_price,
                            max_price: quote.max_price,
                            source_ref: Some(quote.external_id.clone()),
                            ingest_run_id: run_id,
                        }),
                        observed,
                    ),
                    Err(why) => {
                        log_rejection(tally, &target.external_id, why);
                        let outcome = match why {
                            Rejection::NoPrice => PollOutcome::NoPrice,
                            _ => PollOutcome::Rejected,
                        };
                        (PollResult::Failed(outcome), observed)
                    }
                }
            }
        };

        if let PollResult::Failed(outcome) = result {
            tally.rejected += 1;
            if outcome == PollOutcome::NoPrice {
                tally.no_price += 1;
            }
        }

        polls.push(Poll {
            asset_id: target.asset_id,
            market_id: target.market_id,
            source_observed_at,
            result,
        });
    }

    // A quote for something we never asked about has no poll row to live in,
    // because we hold no (asset, market) pair for it. It must still be counted:
    // silently dropping it would hide a provider answering with cards we have no
    // mapping for, which is the first symptom of a renumbered identifier space.
    let asked: HashSet<_> = targets
        .iter()
        .map(|t| (t.external_id.as_str(), t.platform))
        .collect();
    for quote in &quotes {
        if !asked.contains(&(quote.external_id.as_str(), quote.platform)) {
            tally.seen += 1;
            tally.rejected += 1;
            log_rejection(tally, &quote.external_id, Rejection::UnknownAsset);
        }
    }

    let counts = db::write_polls(runner.pool, &polls, polled_at, run_id).await?;
    tally.written += counts.written as i32;
    tally.unchanged += counts.unchanged as i32;

    info!(
        asked = targets.len(),
        written = counts.written,
        unchanged = counts.unchanged,
        "batch stored"
    );
    Ok(())
}

#[derive(Debug, Default)]
pub struct ReplayReport {
    pub deleted: u64,
    pub rewritten: u64,
}

/// Re-parses one run's archived payloads under the current parser.
///
/// This is the one deliberate exception to the append-only rule, and it is a
/// manual command that never runs automatically. Without it `ON CONFLICT DO
/// NOTHING` makes the first parse permanent and a better parser could never
/// correct a wrong row.
///
/// Two things make a replay reproduce the original verdicts rather than today's.
/// It validates against the ORIGINAL run's `started_at`, so rule 2 accepts and
/// rejects exactly what it did the first time. It writes no `ingest_polls` row,
/// because a replay is not evidence that we looked at the market.
pub async fn replay(runner: &Runner<'_>, original: RunId) -> Result<ReplayReport> {
    let Some(lock) = db::try_lock(&runner.config.database_url).await? else {
        anyhow::bail!("an ingest run holds the lock; replay would race it");
    };

    let report = replay_locked(runner, original).await;
    lock.release().await;
    report
}

async fn replay_locked(runner: &Runner<'_>, original: RunId) -> Result<ReplayReport> {
    let started_at = db::run_started_at(runner.pool, original)
        .await?
        .with_context(|| format!("no run {original}"))?;
    let keys = db::run_archive_keys(runner.pool, original).await?;
    if keys.is_empty() {
        anyhow::bail!("run {original} archived no price payloads, so there is nothing to replay");
    }

    let game = db::game_by_code(runner.pool, &runner.config.game_code).await?;
    let (replacement, _) =
        db::open_run(runner.pool, runner.source.name(), runner.source.parser_version()).await?;

    // Everything is read and parsed BEFORE anything is deleted. A failed archive
    // read must leave the ledger exactly as it was.
    let mut replacements = Vec::new();
    let mut tally = Tally::default();
    for key in &keys {
        let envelope = runner.archive.get(key).await?;
        let quotes = runner.source.parse_prices(&envelope)?;

        // Identity resolves through asset_source_ids, never through the schedule.
        // An asset that has since left the covered set still has a history, and
        // that history must still be correctable.
        let targets = db::resolve_targets(
            runner.pool,
            runner.source.name(),
            game.id,
            &envelope.requested_ids,
        )
        .await?;

        let mut by_key: HashMap<(&str, Platform), _> = HashMap::with_capacity(quotes.len());
        for quote in &quotes {
            by_key.insert((quote.external_id.as_str(), quote.platform), quote);
        }

        let mut observations = Vec::new();
        for target in &targets {
            let Some(quote) = by_key.get(&(target.external_id.as_str(), target.platform))
            else {
                continue;
            };
            tally.seen += 1;
            match validate(&game, target, quote, started_at) {
                Err(why) => {
                    tally.rejected += 1;
                    log_rejection(&mut tally, &target.external_id, why);
                }
                Ok((price, observed_at)) => observations.push(Observation {
                    asset_id: target.asset_id,
                    market_id: target.market_id,
                    source: runner.source.name(),
                    observed_at,
                    price,
                    min_price: quote.min_price,
                    max_price: quote.max_price,
                    source_ref: Some(quote.external_id.clone()),
                    ingest_run_id: replacement,
                }),
            }
        }
        replacements.extend(observations);
    }

    let (deleted, rewritten) =
        db::replace_run_observations(runner.pool, original, replacement, &replacements).await?;
    let report = ReplayReport { deleted, rewritten };

    tally.written = report.rewritten as i32;
    db::close_run(
        runner.pool,
        replacement,
        RunStatus::Ok,
        tally.counts(),
        None,
        serde_json::json!({
            "replay_of": original.to_string(),
            "price_keys": keys,
            "requests": 0,
        }),
    )
    .await?;

    Ok(report)
}

/// Applies rules 1, 2 and 3 to one quote.
fn validate(
    game: &db::Game,
    target: &PollTarget,
    quote: &crate::source::ParsedQuote,
    started_at: DateTime<Utc>,
) -> Result<(i64, DateTime<Utc>), Rejection> {
    // Rule 1. A provider that re-points an existing identifier at a different
    // card would otherwise attach one card's prices to another card's history,
    // and nothing would raise an error. We never compare the rating: cards raise
    // their rating in season by design.
    if let Some(name) = &quote.name
        && !names_match(name, &target.name)
    {
        return Err(Rejection::IdentityMismatch);
    }

    let Some(price) = quote.price.filter(|p| *p > 0) else {
        return Err(Rejection::NoPrice);
    };
    // Rule 3. Never substitute our own clock for a missing source timestamp.
    let Some(observed_at) = quote.observed_at else {
        return Err(Rejection::MissingTimestamp);
    };
    // Rule 2. Keep a bad timestamp out of the hypertable entirely.
    if !timestamp_in_range(observed_at, game.released_at, started_at) {
        return Err(Rejection::TimestampOutOfRange);
    }
    Ok((price, observed_at))
}

/// Rule 4. One bad record never fails a run, and the log sample stays bounded.
fn log_rejection(tally: &mut Tally, external_id: &str, why: Rejection) {
    if tally.logged_fragments < MAX_LOGGED_FRAGMENTS {
        tally.logged_fragments += 1;
        warn!(external_id, reason = why.as_str(), "record rejected");
    }
}
