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
    let output = tokio::process::Command::new(pg_dump_path)
        .arg("--format=custom")
        .arg("--no-owner")
        .arg("--no-acl")
        .arg(database_url)
        .output()
        .await
        .with_context(|| {
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

/// Keeps the newest `keep` dumps.
///
/// The keys sort by time because they start with a zero padded UTC timestamp, so
/// a lexical sort is a chronological one.
async fn prune(archive: &S3Archive, keep: usize) -> Result<()> {
    let mut keys = archive.list(PREFIX).await?;
    if keys.len() <= keep {
        return Ok(());
    }
    keys.sort();

    let stale = keys.len() - keep;
    for key in keys.into_iter().take(stale) {
        if let Err(e) = archive.delete(&key).await {
            // A dump that will not delete is a tidiness problem, never a reason
            // to fail the backup that just succeeded.
            warn!(key, error = %e, "could not remove an old dump");
        }
    }
    info!(removed = stale, "old dumps removed");
    Ok(())
}
