//! The step that writes down what the system currently thinks.
//!
//! Automatic and daily on purpose. An experiment that depends on somebody
//! remembering to run it is not an experiment, and the claims are worthless
//! unless they exist BEFORE the outcome does.
//!
//! It runs last in a run, after prices, so it reasons about what this run just
//! learned rather than about yesterday.

use super::{Run, Runner, Tally};
use crate::analytics::predictions;
use crate::db;
use anyhow::Result;
use chrono::Duration;
use tracing::info;

const PREDICTION_STEP: &str = "predictions_ran";

pub(crate) async fn maybe_record(
    runner: &Runner<'_>,
    game: &db::Game,
    run: Run,
    tally: &mut Tally,
) -> Result<()> {
    let source = runner.source.name();
    if !super::discovery::is_due(
        runner,
        source,
        PREDICTION_STEP,
        runner.config.prediction_cadence_seconds,
    )
    .await?
    {
        return Ok(());
    }

    // The run's own instant, not the wall clock at the end of it. Every claim
    // from one run therefore shares one as_of, and the ranking behind them
    // recomputes exactly.
    let made_at = run.started_at;
    let horizon = Duration::seconds(runner.config.prediction_horizon_seconds);

    let mut written = 0u64;
    for (market, _) in db::markets_for_game(runner.pool, game.id).await? {
        written += predictions::record_picks(
            runner.pool,
            game.id,
            market,
            made_at,
            horizon,
            runner.config.predictions_per_market,
        )
        .await?;
    }

    // Recorded whatever the count. A day on which nothing was cheap enough to
    // claim is a real observation, and leaving the step unmarked would retry it
    // every fifteen minutes for the rest of the day.
    tally.steps.push(PREDICTION_STEP);
    db::record_step(runner.pool, run.id, PREDICTION_STEP).await?;
    info!(written, "predictions recorded");
    Ok(())
}
