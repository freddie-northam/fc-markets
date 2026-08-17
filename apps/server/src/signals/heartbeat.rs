//! Section 4.9. The signal whose silence is the alarm.
//!
//! A pull check on a failing host cannot report that the host is gone. This
//! sends one request to an external dead man's switch when, and only when, a run
//! closes healthy, so one mechanism covers a dead host, a dead process and a
//! dead feed.

use super::outbound::{Call, fire};
use crate::ids::RunStatus;
use tracing::warn;

/// Sends the heartbeat if the run earned one.
///
/// A degraded or failed run deliberately sends nothing, so the drift checks
/// raise their alarm through the same channel as a dead host.
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
    fire("heartbeat", Call::Get(url)).await;
}
