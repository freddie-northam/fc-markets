//! Section 4.9. The only signal that points outward.
//!
//! A pull check on a failing host cannot report that the host is gone. This sends
//! one request to an external dead man's switch when, and only when, a run closes
//! healthy. Silence is therefore the alarm, and it covers a dead host, a dead
//! process and a dead feed with one mechanism.

use crate::ids::RunStatus;
use std::time::Duration;
use tracing::warn;

/// The switch is a courtesy call, not part of the ledger. A slow one must not
/// hold a run open.
const TIMEOUT: Duration = Duration::from_secs(10);

/// Sends the heartbeat if the run earned one.
///
/// A degraded or failed run deliberately sends nothing, so the drift checks raise
/// their alarm through the same channel as a dead host. A failed or slow
/// heartbeat writes a warning and never changes the run status: the ledger is
/// correct whether or not a third party service answered.
pub async fn send(url: Option<&str>, status: RunStatus) {
    let Some(url) = url else {
        // Unset on purpose in tests and local development, so neither needs an
        // external service.
        return;
    };
    if status != RunStatus::Ok {
        warn!(?status, "run did not close ok, so no heartbeat was sent");
        return;
    }

    let client = match reqwest::Client::builder().timeout(TIMEOUT).build() {
        Ok(client) => client,
        Err(e) => {
            warn!(error = %e, "could not build the heartbeat client");
            return;
        }
    };

    match client.get(url).send().await {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => warn!(status = %response.status(), "the heartbeat was refused"),
        Err(e) => warn!(error = %e, "the heartbeat did not arrive"),
    }
}
