//! One way to call a third party, used by every outbound signal.
//!
//! Every signal in this module tree shares the same contract: it is a courtesy
//! call, it must not hold a run open, and it must never change the run's
//! outcome. The ledger is correct whether or not anyone answered. Writing that
//! contract once means a new signal cannot get the timeout, the error handling
//! or the swallowing wrong.

use std::time::Duration;
use tracing::warn;

/// A slow third party must not hold a run open.
const TIMEOUT: Duration = Duration::from_secs(10);

/// What to send. The shape is the caller's business; the failure handling is not.
pub(crate) enum Call<'a> {
    /// A dead man's switch is pinged, not posted to.
    Get(&'a str),
    /// A chat webhook needs a body, and rejects one it does not recognise.
    PostJson(&'a str, serde_json::Value),
}

/// Sends one request and turns every failure into a warning.
///
/// `label` names the signal in the log, so a refused heartbeat and a refused
/// alert are distinguishable without a stack trace.
pub(crate) async fn fire(label: &str, call: Call<'_>) {
    let client = match reqwest::Client::builder().timeout(TIMEOUT).build() {
        Ok(client) => client,
        Err(e) => {
            warn!(signal = label, error = %e, "could not build the client");
            return;
        }
    };

    let request = match call {
        Call::Get(url) => client.get(url),
        Call::PostJson(url, body) => client.post(url).json(&body),
    };

    match request.send().await {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => warn!(signal = label, status = %response.status(), "refused"),
        Err(e) => warn!(signal = label, error = %e, "did not arrive"),
    }
}
