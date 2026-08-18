//! The step that learns what the identifiers on an asset mean.
//!
//! An asset stores `league_id`, `nation_id` and `club_id` as bare integers. This
//! walks the provider's reference lists so a card can say Premier League rather
//! than league 13, and so a cohort can be cut by league or nation.
//!
//! It is the slowest step in the pipeline on purpose. The lists are static
//! between titles: 61 leagues, 161 nations and 1,235 clubs, about 75 pages in
//! total. Refreshing them daily would spend that every day to learn nothing.

use super::{Budget, Fetched, Run, Runner, Tally, fetch_and_archive};
use crate::archive::Kind;
use crate::db;
use anyhow::Result;
use tracing::info;

/// Recorded on the run so the next one can tell how long ago this last ran.
const REFERENCE_STEP: &str = "reference_ran";

/// A hard stop per list, for the reason the asset walk has one: the budget
/// bounds this only at thousands of requests, and a provider that always claims
/// one more page would spend the day before the budget noticed.
const MAX_PAGES_PER_LIST: u32 = 200;

pub(crate) async fn maybe_refresh(
    runner: &Runner<'_>,
    game: &db::Game,
    run: Run,
    budget: &mut Budget,
    tally: &mut Tally,
) -> Result<()> {
    let kinds = runner.source.reference_kinds();
    if kinds.is_empty() {
        return Ok(());
    }
    let source = runner.source.name();
    if !super::discovery::is_due(
        runner,
        source,
        REFERENCE_STEP,
        runner.config.reference_cadence_seconds,
    )
    .await?
    {
        return Ok(());
    }

    let mut written = 0usize;
    // Only a walk that finished every list may mark the step done. The flag
    // suppresses it for a whole cadence, which is a month, so a half walk would
    // leave the catalogue half named until September.
    let mut complete = true;

    'kinds: for kind in kinds {
        let mut page = 1u32;
        let mut pages = 0u32;
        loop {
            if pages >= MAX_PAGES_PER_LIST {
                tally.degrade(format!(
                    "the {} list did not end within {MAX_PAGES_PER_LIST} pages",
                    kind.as_str()
                ));
                complete = false;
                break;
            }
            if !budget.take() {
                tally.degrade("the daily request budget stopped the reference step");
                complete = false;
                break 'kinds;
            }
            tally.requests += 1;
            db::record_request(runner.pool, run.id).await?;
            db::touch_heartbeat(runner.pool, run.id).await?;

            let Fetched::Ready(envelope, _) = fetch_and_archive(
                runner,
                tally,
                run,
                Kind::AssetList,
                page as usize,
                runner.source.fetch_reference(*kind, page),
            )
            .await
            else {
                complete = false;
                break;
            };
            pages += 1;

            match runner.source.parse_reference(*kind, &envelope) {
                Ok(rows) => {
                    written += db::upsert_reference(runner.pool, source, game.id, *kind, &rows)
                        .await? as usize;
                }
                Err(e) => {
                    tally.degrade(format!(
                        "the {} list could not be parsed: {e}",
                        kind.as_str()
                    ));
                    complete = false;
                    break;
                }
            }

            match runner.source.next_page(&envelope) {
                // Strictly increasing, or not at all. A provider answering with
                // the page we just read would spin until the budget died.
                Some(next) if next > page => page = next,
                Some(_) => {
                    tally.degrade(format!("the {} list did not advance", kind.as_str()));
                    complete = false;
                    break;
                }
                None => break,
            }
        }
    }

    if complete {
        tally.steps.push(REFERENCE_STEP);
        db::record_step(runner.pool, run.id, REFERENCE_STEP).await?;
    }
    info!(written, complete, "reference lists refreshed");
    Ok(())
}
