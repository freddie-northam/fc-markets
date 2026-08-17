//! Section 4.8. The nightly database dump.
//!
//! The archive alone cannot rebuild the ledger. Identity lives in the database:
//! `assets`, `players` and `asset_source_ids` are what turn an archived payload
//! back into rows, and replay resolves through them. A bucket full of raw
//! payloads and no database is a bucket of unattributable JSON.

use crate::archive::S3Archive;
use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Timelike, Utc};
use tracing::{info, warn};

const PREFIX: &str = "backup/pg";

/// Runs `pg_dump -Fc` and stores the result beside the archive.
///
/// The custom format is what `pg_restore` needs, and a restore must call
/// `timescaledb_pre_restore()` before the load and `timescaledb_post_restore()`
/// after it. That is written on the object rather than left to memory.
pub async fn run(
    archive: &S3Archive,
    database_url: &str,
    pg_dump_path: &str,
    keep: usize,
    at: DateTime<Utc>,
) -> Result<String> {
    let (connection, password) = split_password(database_url);
    let mut command = tokio::process::Command::new(pg_dump_path);
    command
        .arg("--format=custom")
        .arg("--no-owner")
        .arg("--no-acl")
        .arg(&connection);
    if let Some(password) = password {
        command.env("PGPASSWORD", password);
    }

    let output = command.output().await.with_context(|| {
        format!("could not run {pg_dump_path}; it must match the server's major version")
    })?;

    if !output.status.success() {
        anyhow::bail!(
            "pg_dump failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let key = format!(
        "{PREFIX}/{:04}{:02}{:02}T{:02}{:02}{:02}Z.dump",
        at.year(),
        at.month(),
        at.day(),
        at.hour(),
        at.minute(),
        at.second()
    );
    let bytes = output.stdout.len();
    archive.put_bytes(&key, output.stdout).await?;
    info!(key, bytes, "database dump stored");

    prune(archive, keep).await?;
    Ok(key)
}

/// Splits the password out of a connection URL.
///
/// An argument is visible to every user on the host through the process list, so
/// the password must not travel that way. libpq reads it from `PGPASSWORD`, and
/// the argument then carries everything else.
///
/// A URL we cannot parse is passed through untouched: failing the backup over a
/// parse we do not understand would be worse than the exposure.
fn split_password(database_url: &str) -> (String, Option<String>) {
    let Ok(mut url) = url::Url::parse(database_url) else {
        // libpq also accepts "host=... password=...", which parses as nothing
        // here and would then travel on the argv this function exists to keep
        // it off. Say so rather than silently reverting to the exposure.
        if database_url.contains("password=") {
            tracing::warn!(
                "DATABASE_URL is in libpq keyword form, so the password cannot be moved out \
                 of the pg_dump arguments; use a postgres:// URL to keep it off the process list"
            );
        }
        return (database_url.to_string(), None);
    };
    let Some(password) = url.password().map(str::to_string) else {
        return (database_url.to_string(), None);
    };
    if url.set_password(None).is_err() {
        return (database_url.to_string(), None);
    }
    (url.to_string(), Some(password))
}

/// Keeps the newest `keep` dumps.
///
/// The keys sort by time because they start with a zero padded UTC timestamp, so
/// a lexical sort is a chronological one.
async fn prune(archive: &S3Archive, keep: usize) -> Result<()> {
    let stale = stale_keys(archive.list(PREFIX).await?, keep);
    let removed = stale.len();

    for key in stale {
        if let Err(e) = archive.delete(&key).await {
            // A dump that will not delete is a tidiness problem, never a reason
            // to fail the backup that just succeeded.
            warn!(key, error = %e, "could not remove an old dump");
        }
    }
    info!(removed, "old dumps removed");
    Ok(())
}

/// The dumps to drop, oldest first.
///
/// Separated from the deleting so the choice can be tested. The I/O is a thin
/// call; the risk lives here, in picking which files go, and an inverted sort
/// would delete the newest dumps and leave the useless ones.
///
/// Keys sort chronologically because they start with a zero padded UTC
/// timestamp, so a lexical sort is a chronological one.
fn stale_keys(mut keys: Vec<String>, keep: usize) -> Vec<String> {
    // At least one dump always survives. `keep` comes from configuration, and
    // zero would otherwise delete the dump this run just took, so the command
    // would report success having destroyed its own output.
    let keep = keep.max(1);
    if keys.len() <= keep {
        return Vec::new();
    }
    keys.sort();
    keys.truncate(keys.len() - keep);
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dumps(names: &[&str]) -> Vec<String> {
        names
            .iter()
            .map(|n| format!("backup/pg/{n}.dump"))
            .collect()
    }

    #[test]
    fn the_oldest_dumps_are_the_ones_dropped() {
        // Deliberately out of order: the input is a bucket listing, not a sorted
        // one, and the sort is what makes lexical order chronological.
        let keys = dumps(&["20260103T000000Z", "20260101T000000Z", "20260102T000000Z"]);

        assert_eq!(
            stale_keys(keys, 2),
            dumps(&["20260101T000000Z"]),
            "the newest must survive and the oldest must go"
        );
    }

    #[test]
    fn nothing_is_dropped_while_the_history_is_short() {
        let keys = dumps(&["20260101T000000Z", "20260102T000000Z"]);
        assert!(stale_keys(keys.clone(), 2).is_empty());
        assert!(stale_keys(keys, 30).is_empty());
    }

    /// A misconfigured keep of zero would otherwise delete the dump the run just
    /// took, and the command would report success having destroyed its output.
    #[test]
    fn one_dump_always_survives_however_it_is_configured() {
        let keys = dumps(&["20260101T000000Z", "20260102T000000Z"]);
        assert_eq!(
            stale_keys(keys, 0),
            dumps(&["20260101T000000Z"]),
            "keep zero must still leave the newest dump standing"
        );
    }

    #[test]
    fn the_password_leaves_the_connection_argument() {
        let (connection, password) =
            split_password("postgres://fcmarket:secret@localhost:5434/fcmarket");
        assert!(
            !connection.contains("secret"),
            "the process list must not carry the password: {connection}"
        );
        assert!(connection.contains("localhost:5434"));
        assert!(connection.ends_with("/fcmarket"));
        assert_eq!(password.as_deref(), Some("secret"));
    }

    #[test]
    fn a_url_with_no_password_is_unchanged() {
        let (connection, password) = split_password("postgres://fcmarket@localhost/fcmarket");
        assert_eq!(connection, "postgres://fcmarket@localhost/fcmarket");
        assert_eq!(password, None);
    }

    /// Better to keep the exposure than to fail every backup over a form we did
    /// not anticipate.
    #[test]
    fn an_unparseable_url_passes_through() {
        let (connection, password) = split_password("not a url");
        assert_eq!(connection, "not a url");
        assert_eq!(password, None);
    }
}
