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

use super::{Budget, Fetched, Run, Runner, Tally, fetch_and_archive};
use crate::archive::Kind;
use crate::db;
use crate::domain::poll_interval_seconds;
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
    run: Run,
    budget: &mut Budget,
    tally: &mut Tally,
) -> Result<()> {
    let source = runner.source.name();
    if !is_due(
        runner,
        source,
        DISCOVERY_STEP,
        runner.config.discovery_cadence_seconds,
    )
    .await?
    {
        return Ok(());
    }
    if !budget.take() {
        tally.degrade("the daily request budget stopped asset discovery");
        return Ok(());
    }
    tally.requests += 1;
    db::record_request(runner.pool, run.id).await?;
    db::touch_heartbeat(runner.pool, run.id).await?;

    // The asset list decides which cards exist at all, so a change in it must
    // stay auditable. Same archive-before-parse rule as prices.
    let Fetched::Ready(envelope, _) = fetch_and_archive(
        runner,
        tally,
        run,
        Kind::AssetList,
        0,
        runner.source.fetch_asset_list(),
    )
    .await
    else {
        return Ok(());
    };

    // A parse failure here degrades the run rather than ending it. The asset
    // list changing shape must not also stop us reading prices for the assets we
    // already know: that would turn a discovery problem into a gap in the ledger.
    let listings = match runner.source.parse_asset_list(&envelope) {
        Ok(listings) => listings,
        Err(e) => {
            tally.degrade(format!("the asset list could not be parsed: {e}"));
            return Ok(());
        }
    };
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
    db::record_step(runner.pool, run.id, DISCOVERY_STEP).await?;
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
    run: Run,
    budget: &mut Budget,
    tally: &mut Tally,
) -> Result<()> {
    let source = runner.source.name();
    if !is_due(
        runner,
        source,
        METADATA_STEP,
        runner.config.metadata_cadence_seconds,
    )
    .await?
    {
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
        // Nothing needs attributes, so the step is genuinely done.
        tally.steps.push(METADATA_STEP);
        db::record_step(runner.pool, run.id, METADATA_STEP).await?;
        return Ok(());
    }

    let mut updated = 0;
    // Only a step that got all the way through may mark itself done. The flag
    // suppresses the step for a whole cadence, so a run that stopped early on
    // budget or a rate limit would leave every remaining asset without
    // attributes for a week: null rating and version, every one tiered slowest,
    // and one meaningless valuation class holding all of them.
    let mut complete = true;
    for (batch, chunk) in stale.chunks(batch_size).enumerate() {
        if !budget.take() {
            tally.degrade("the daily request budget stopped the metadata step");
            complete = false;
            break;
        }
        tally.requests += 1;
        db::record_request(runner.pool, run.id).await?;
        // The reaper only sees the heartbeat. A metadata refresh that walks the
        // catalogue can outlast ABANDON_AFTER before the price loop ever runs.
        db::touch_heartbeat(runner.pool, run.id).await?;

        // The archive wraps every provider response, including this one. Card
        // attributes feed every valuation input, so without them in the archive
        // they exist in one place only.
        let envelope = match fetch_and_archive(
            runner,
            tally,
            run,
            Kind::Metadata,
            batch,
            runner.source.fetch_metadata(chunk),
        )
        .await
        {
            Fetched::Ready(envelope, _) => envelope,
            Fetched::Skip => {
                complete = false;
                continue;
            }
            Fetched::Stop => {
                complete = false;
                break;
            }
        };

        // Same reasoning as the asset list: a metadata schema change must not
        // stop the price path, which is the part that cannot be caught up later.
        let parsed = match runner.source.parse_metadata(&envelope) {
            Ok(parsed) => parsed,
            Err(e) => {
                tally.degrade(format!("metadata batch {batch} could not be parsed: {e}"));
                complete = false;
                continue;
            }
        };

        for attrs in parsed {
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

    // Completion is measured against the work, not against the loop. The stale
    // list was already capped by what the budget could afford, so a run that
    // processed its whole capped list has still only done a fraction, and
    // marking the step done there would suppress it for the rest of the cadence.
    // Anything still missing attributes means the step is not finished.
    let outstanding = db::assets_needing_metadata(
        runner.pool,
        source,
        game.id,
        runner.config.metadata_cadence_seconds,
        1,
    )
    .await?;
    let finished = complete && outstanding.is_empty();

    if finished {
        tally.steps.push(METADATA_STEP);
        db::record_step(runner.pool, run.id, METADATA_STEP).await?;
    }
    info!(updated, finished, "card attributes refreshed");
    Ok(())
}

/// Decides which assets the budget can afford to follow, and creates exactly one
/// poll state row for each covered asset and market.
///
/// Nothing else creates rows in that table. The coverage figure is a quota
/// decision, not a code decision: section 4.7 sets it from what the source
/// allows, and the tier expression then spends it.
pub async fn apply_coverage(runner: &Runner<'_>, game: &db::Game) -> Result<()> {
    let covered =
        db::most_valuable_assets(runner.pool, game.id, runner.config.asset_coverage).await?;
    let markets = db::markets_for_game(runner.pool, game.id).await?;

    let mut rows = Vec::with_capacity(covered.len() * markets.len());
    for asset in &covered {
        // One definition of the tier expression, in Rust, where the unit tests
        // cover it. A second copy in SQL would drift from this one in silence.
        let interval = poll_interval_seconds(&asset.version, asset.rating);
        rows.extend(
            markets
                .iter()
                .map(|(market, _)| (asset.id, *market, interval)),
        );
    }
    let added = db::ensure_poll_state(runner.pool, &rows).await?;

    // An asset that has dropped out of the covered set stops being polled. Its
    // history and its coverage record stay, and the quota frees immediately.
    let ids: Vec<_> = covered.iter().map(|a| a.id.0).collect();
    let dropped = db::drop_poll_state_outside(runner.pool, game.id, &ids).await?;

    info!(covered = covered.len(), added, dropped, "coverage applied");
    Ok(())
}

/// True when a slow step has not run inside its cadence.
async fn is_due(
    runner: &Runner<'_>,
    source: &str,
    step: &str,
    cadence_seconds: i64,
) -> Result<bool> {
    let since = db::seconds_since_step(runner.pool, source, step).await?;
    Ok(match since {
        None => true,
        Some(seconds) => seconds >= cadence_seconds as f64,
    })
}
