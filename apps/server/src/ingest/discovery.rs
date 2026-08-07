//! Asset discovery, card attributes and coverage.
//!
//! Section 4.5 puts all three on a slow cadence and on the same request budget as
//! prices. Without discovery no asset is ever polled. Without the metadata step
//! every valuation column stays null, and the terminal has a price history it
//! cannot value.
//!
//! The three steps run in this order inside one run, so a brand new install
//! converges in a single pass: find the assets, learn what they are, then decide
//! which of them the budget can afford to follow.

use super::{Budget, Runner, Tally};
use crate::archive::{Kind, object_key};
use crate::db;
use crate::domain::poll_interval_seconds;
use crate::ids::RunId;
use crate::source::FetchError;
use anyhow::Result;
use tracing::info;

/// Recorded in `ingest_runs.metadata` so the next run can tell how long ago each
/// slow step last ran, without a table of its own.
const DISCOVERY_STEP: &str = "discovery_ran";
const METADATA_STEP: &str = "metadata_ran";

/// Enumerates the source's full asset list and records identifiers we have not
/// seen.
///
/// It deliberately keeps no highest-identifier watermark. A provider can
/// renumber or backfill its records, and a watermark would make every backfilled
/// card invisible for ever.
pub(crate) async fn maybe_discover(
    runner: &Runner<'_>,
    game: &db::Game,
    run_id: RunId,
    budget: &mut Budget,
    tally: &mut Tally,
) -> Result<()> {
    let source = runner.source.name();
    if !is_due(runner, source, DISCOVERY_STEP, runner.config.discovery_cadence_seconds).await? {
        return Ok(());
    }
    if !budget.take() {
        tally.degrade("the daily request budget stopped asset discovery");
        return Ok(());
    }
    tally.requests += 1;

    let envelope = match runner.source.fetch_asset_list().await {
        Ok(envelope) => envelope,
        Err(FetchError::RateLimited) => {
            tally.degrade("the provider rate limited asset discovery");
            return Ok(());
        }
        Err(FetchError::Other(e)) => {
            tally.degrade(format!("the asset list could not be fetched: {e}"));
            return Ok(());
        }
    };

    // Archive before parse, exactly as for prices. The asset list is what decides
    // which cards exist at all, so a change in it must stay auditable.
    let key = object_key(source, Kind::AssetList, &run_id.to_string(), 0, envelope.fetched_at);
    if let Err(e) = runner.archive.put(&key, &envelope).await {
        tally.degrade(format!("the asset list was not archived, so it was not parsed: {e}"));
        return Ok(());
    }

    let listings = runner.source.parse_asset_list(&envelope)?;
    let known = db::known_external_ids(runner.pool, source, game.id).await?;

    let mut added = 0;
    for listing in &listings {
        if known.contains(&listing.external_id) {
            continue;
        }
        db::insert_discovered_asset(
            runner.pool,
            game.id,
            source,
            &listing.external_id,
            &listing.name,
        )
        .await?;
        added += 1;
    }

    tally.steps.push(DISCOVERY_STEP);
    info!(listed = listings.len(), added, "asset discovery finished");
    Ok(())
}

/// Fetches card attributes for assets that are new, and for assets whose
/// attributes are older than the metadata cadence.
///
/// This step is not optional. Every valuation input in section 5 reads these
/// columns, and without it they stay null whatever the price history holds.
pub(crate) async fn maybe_refresh_metadata(
    runner: &Runner<'_>,
    game: &db::Game,
    run_id: RunId,
    budget: &mut Budget,
    tally: &mut Tally,
) -> Result<()> {
    let source = runner.source.name();
    if !is_due(runner, source, METADATA_STEP, runner.config.metadata_cadence_seconds).await? {
        return Ok(());
    }

    let batch_size = runner.source.metadata_batch_size().max(1);
    let affordable = (budget.remaining as i64).saturating_mul(batch_size as i64);
    let stale = db::assets_needing_metadata(
        runner.pool,
        source,
        game.id,
        runner.config.metadata_cadence_seconds,
        affordable,
    )
    .await?;

    if stale.is_empty() {
        tally.steps.push(METADATA_STEP);
        return Ok(());
    }

    let mut updated = 0;
    for (batch, chunk) in stale.chunks(batch_size).enumerate() {
        if !budget.take() {
            tally.degrade("the daily request budget stopped the metadata step");
            break;
        }
        tally.requests += 1;

        let envelope = match runner.source.fetch_metadata(chunk).await {
            Ok(envelope) => envelope,
            Err(FetchError::RateLimited) => {
                tally.degrade("the provider rate limited the metadata step");
                break;
            }
            Err(FetchError::Other(e)) => {
                tally.degrade(format!("metadata batch {batch} could not be fetched: {e}"));
                continue;
            }
        };

        // The archive wraps every provider response, including this one. Card
        // attributes feed every valuation input, so without them in the archive
        // they exist in one place only.
        let key = object_key(
            source,
            Kind::Metadata,
            &run_id.to_string(),
            batch,
            envelope.fetched_at,
        );
        if let Err(e) = runner.archive.put(&key, &envelope).await {
            tally.degrade(format!(
                "metadata batch {batch} was not archived, so it was not parsed: {e}"
            ));
            continue;
        }

        for attrs in runner.source.parse_metadata(&envelope)? {
            let Some(asset) =
                db::asset_by_external_id(runner.pool, source, game.id, &attrs.external_id).await?
            else {
                continue;
            };
            let interval = poll_interval_seconds(&attrs.version, attrs.rating);
            db::update_asset_attributes(runner.pool, asset, &attrs, interval).await?;
            updated += 1;
        }
    }

    tally.steps.push(METADATA_STEP);
    info!(updated, "card attributes refreshed");
    Ok(())
}

/// Decides which assets the budget can afford to follow, and creates exactly one
/// poll state row for each covered asset and market.
///
/// Nothing else creates rows in that table. The coverage figure is a quota
/// decision, not a code decision: section 4.7 sets it from what the source
/// allows, and the tier expression then spends it.
pub async fn apply_coverage(runner: &Runner<'_>, game: &db::Game) -> Result<()> {
    let covered = db::most_valuable_assets(runner.pool, game.id, runner.config.asset_coverage).await?;
    let markets = db::markets_for_game(runner.pool, game.id).await?;

    for asset in &covered {
        // One definition of the tier expression, in Rust, where the unit tests
        // cover it. A second copy in SQL would drift from this one in silence.
        let interval = poll_interval_seconds(&asset.version, asset.rating);
        for (market, _platform) in &markets {
            db::ensure_poll_state(runner.pool, asset.id, *market, interval).await?;
        }
    }

    // An asset that has dropped out of the covered set stops being polled. Its
    // history and its coverage record stay, and the quota frees immediately.
    let ids: Vec<_> = covered.iter().map(|a| a.id.0).collect();
    let dropped = db::drop_poll_state_outside(runner.pool, game.id, &ids).await?;

    info!(covered = covered.len(), dropped, "coverage applied");
    Ok(())
}

/// True when a slow step has not run inside its cadence.
async fn is_due(runner: &Runner<'_>, source: &str, step: &str, cadence_seconds: i64) -> Result<bool> {
    let since = db::seconds_since_step(runner.pool, source, step).await?;
    Ok(match since {
        None => true,
        Some(seconds) => seconds >= cadence_seconds as f64,
    })
}
