use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// What we store, rather than the bare response body.
///
/// `requested_ids` is the reason this wrapper exists. When a provider response
/// carries no player identifier we must ask for one player per call, and in that
/// case the body alone cannot be attributed to an asset on replay. The request
/// supplies the missing link.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub fetched_at: DateTime<Utc>,
    pub url: String,
    pub requested_ids: Vec<String>,
    pub http_status: u16,
    pub sha256: String,
    pub body: serde_json::Value,
}

impl Envelope {
    pub fn new(
        url: impl Into<String>,
        requested_ids: Vec<String>,
        http_status: u16,
        body: serde_json::Value,
        fetched_at: DateTime<Utc>,
    ) -> Self {
        let digest = Sha256::digest(body.to_string().as_bytes());
        Self {
            fetched_at,
            url: url.into(),
            requested_ids,
            http_status,
            sha256: format!("{digest:x}"),
            body,
        }
    }
}

/// Where an archived object lives. Date partitioned so a human can find the
/// payload behind a given day without listing the whole bucket.
pub fn object_key(source: &str, kind: Kind, run_id: &str, batch: usize, at: DateTime<Utc>) -> String {
    let prefix = match kind {
        Kind::Prices => format!("raw/{source}"),
        Kind::Metadata => format!("raw/{source}/metadata"),
    };
    format!(
        "{prefix}/{:04}/{:02}/{:02}/{run_id}-{batch}.json.zst",
        at.year(),
        at.month(),
        at.day()
    )
}

#[derive(Debug, Clone, Copy)]
pub enum Kind {
    Prices,
    Metadata,
}

/// Writes the archive before anything parses the payload, so an improved parser
/// can rebuild the canonical tables later.
///
/// A failed write is fatal to the batch. The alternative would be observations
/// whose stated source of truth does not exist, which quietly voids replay.
pub trait Archive {
    fn put(&self, key: &str, envelope: &Envelope) -> Result<()>;
    fn get(&self, key: &str) -> Result<Envelope>;
}

/// Filesystem backed archive. The S3 client speaks the same two operations, so
/// swapping it in adds a struct and changes no caller.
pub struct FileArchive {
    root: std::path::PathBuf,
}

impl FileArchive {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl Archive for FileArchive {
    fn put(&self, key: &str, envelope: &Envelope) -> Result<()> {
        let path = self.root.join(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        let json = serde_json::to_vec(envelope)?;
        let compressed = zstd::encode_all(&json[..], 3)?;
        std::fs::write(&path, compressed)
            .with_context(|| format!("cannot write {}", path.display()))?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Envelope> {
        let path = self.root.join(key);
        let compressed = std::fs::read(&path)
            .with_context(|| format!("cannot read {}", path.display()))?;
        let json = zstd::decode_all(&compressed[..])?;
        Ok(serde_json::from_slice(&json)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-07T09:05:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn a_written_envelope_reads_back_identically() {
        let dir = std::env::temp_dir().join(format!("fcm-archive-{}", uuid::Uuid::now_v7()));
        let archive = FileArchive::new(&dir);
        let env = Envelope::new(
            "https://example.test/prices",
            vec!["101".into(), "102".into()],
            200,
            serde_json::json!({"prices": [{"id": 101, "price": 12000}]}),
            at(),
        );

        let key = object_key("fixture", Kind::Prices, "run-1", 0, at());
        archive.put(&key, &env).unwrap();
        let back = archive.get(&key).unwrap();

        assert_eq!(back.body, env.body);
        assert_eq!(back.requested_ids, vec!["101", "102"]);
        assert_eq!(back.sha256, env.sha256);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn prices_and_metadata_do_not_share_a_prefix() {
        let p = object_key("fixture", Kind::Prices, "r", 0, at());
        let m = object_key("fixture", Kind::Metadata, "r", 0, at());
        assert_eq!(p, "raw/fixture/2026/08/07/r-0.json.zst");
        assert_eq!(m, "raw/fixture/metadata/2026/08/07/r-0.json.zst");
    }

    #[test]
    fn the_checksum_follows_the_body() {
        let a = Envelope::new("u", vec![], 200, serde_json::json!({"a": 1}), at());
        let b = Envelope::new("u", vec![], 200, serde_json::json!({"a": 2}), at());
        assert_ne!(a.sha256, b.sha256);
    }
}
