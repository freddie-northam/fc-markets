//! Push alerts to a Discord webhook.
//!
//! This is the inverse of `heartbeat`. The heartbeat makes silence the alarm and
//! needs an external service to notice that silence. This posts to a channel, so
//! a human notices instead: one line a day when a run succeeds, and a message
//! the moment something goes wrong that is not routine.
//!
//! It cannot report its own death. Nothing self-hosted can. It reports the
//! failure this project is actually most exposed to, which is a healthy process
//! collecting nothing, and it leaves the dead-process case to the platform's own
//! restart on a failing health check.

use super::outbound::{Call, fire};
use crate::ids::RunStatus;
use crate::ingest::RunSummary;

/// Reasons that are normal operation rather than faults.
///
/// The budget running out is the expected end of a day's work, and it degrades
/// roughly ninety runs a day. Posting those would bury the messages that matter,
/// which is the failure mode of every alert channel anybody has ever ignored.
const ROUTINE: [&str; 1] = ["request budget"];

fn is_routine(reason: &str) -> bool {
    ROUTINE.iter().any(|marker| reason.contains(marker))
}

/// What, if anything, this run is worth saying.
///
/// Returned as a value rather than posted directly so the decision is testable
/// without a network.
pub fn message(
    status: RunStatus,
    summary: &RunSummary,
    poll_age_seconds: Option<f64>,
    max_poll_age_seconds: i32,
) -> Option<String> {
    // Checked first and unconditionally. A run can close Ok while the ledger has
    // been stalled for a day, because "ok" describes this run and not the feed.
    if let Some(age) = poll_age_seconds
        && age > f64::from(max_poll_age_seconds)
    {
        return Some(format!(
            "**Nothing collected for {:.0} hours** (limit {:.0}h). Ingestion is probably stopped.",
            age / 3600.0,
            f64::from(max_poll_age_seconds) / 3600.0
        ));
    }

    if status == RunStatus::Ok {
        return Some(format!(
            "Run ok: **{}** written, {} unchanged, {} rejected, {} requests.",
            summary.written, summary.unchanged, summary.rejected, summary.requests
        ));
    }

    let notable: Vec<&String> = summary.reasons.iter().filter(|r| !is_routine(r)).collect();
    if notable.is_empty() {
        return None;
    }
    Some(format!(
        "**Run {}.** {}",
        status.as_str(),
        notable
            .iter()
            .map(|r| format!("\n- {r}"))
            .collect::<String>()
    ))
}

/// Posts the message, if there is one.
///
/// Discord rejects a payload it does not recognise with 400 and no message ever
/// appears, so the body shape here is not incidental: `content` is the field it
/// requires.
pub async fn send(
    url: Option<&str>,
    status: RunStatus,
    summary: &RunSummary,
    poll_age_seconds: Option<f64>,
    max_poll_age_seconds: i32,
) {
    let Some(url) = url else {
        return;
    };
    let Some(content) = message(status, summary, poll_age_seconds, max_poll_age_seconds) else {
        return;
    };

    let body = serde_json::json!({ "username": "FC Market Ingest", "content": content });
    fire("alert", Call::PostJson(url, body)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(reasons: &[&str]) -> RunSummary {
        RunSummary {
            written: 27_478,
            unchanged: 0,
            rejected: 4_846,
            requests: 17_519,
            reasons: reasons.iter().map(|r| r.to_string()).collect(),
        }
    }

    const MAX_AGE: i32 = 108_000;

    #[test]
    fn a_successful_run_reports_what_it_collected() {
        let m = message(RunStatus::Ok, &summary(&[]), Some(60.0), MAX_AGE).unwrap();
        assert!(m.contains("27478"), "the figure an operator checks: {m}");
        assert!(m.contains("Run ok"));
    }

    /// Roughly ninety runs a day degrade this way once the budget is spent.
    /// Posting them would bury everything worth reading.
    #[test]
    fn a_run_degraded_only_by_the_spent_budget_says_nothing() {
        let s = summary(&["the daily request budget was already spent, so no price was read"]);
        assert_eq!(message(RunStatus::Degraded, &s, Some(60.0), MAX_AGE), None);
    }

    #[test]
    fn a_run_degraded_by_anything_else_is_reported() {
        let s = summary(&["the provider rate limited this run"]);
        let m = message(RunStatus::Degraded, &s, Some(60.0), MAX_AGE).unwrap();
        assert!(m.contains("rate limited"), "{m}");
    }

    /// A real fault alongside a routine one must not be silenced by it.
    #[test]
    fn a_real_reason_survives_being_mixed_with_a_routine_one() {
        let s = summary(&[
            "the daily request budget was already spent, so no price was read",
            "12 identifiers now name a different card",
        ]);
        let m = message(RunStatus::Degraded, &s, Some(60.0), MAX_AGE).unwrap();
        assert!(m.contains("different card"), "{m}");
        assert!(
            !m.contains("request budget"),
            "the routine reason is noise even here: {m}"
        );
    }

    /// "Ok" describes the run, not the feed. A run can do everything asked of it
    /// while the ledger has been stalled for a day, so staleness outranks it.
    #[test]
    fn a_stalled_ledger_is_reported_even_when_the_run_closed_ok() {
        let m = message(RunStatus::Ok, &summary(&[]), Some(200_000.0), MAX_AGE).unwrap();
        assert!(m.contains("Nothing collected"), "{m}");
        assert!(!m.contains("Run ok"), "staleness must win: {m}");
    }

    /// A fresh install has polled nothing and is not a fault.
    #[test]
    fn no_poll_history_is_not_reported_as_stale() {
        let m = message(RunStatus::Ok, &summary(&[]), None, MAX_AGE).unwrap();
        assert!(m.contains("Run ok"), "{m}");
    }
}
