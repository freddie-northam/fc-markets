use anyhow::{Context, Result};
use async_trait::async_trait;
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
        // serde_json orders object keys, so one body always gives one digest.
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
pub fn object_key(
    source: &str,
    kind: Kind,
    run_id: &str,
    batch: usize,
    at: DateTime<Utc>,
) -> String {
    let prefix = match kind {
        Kind::Prices => format!("raw/{source}"),
        Kind::Metadata => format!("raw/{source}/metadata"),
        Kind::AssetList => format!("raw/{source}/assets"),
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
    AssetList,
}

/// Writes the archive before anything parses the payload, so an improved parser
/// can rebuild the canonical tables later.
///
/// A failed write is fatal to the batch. The alternative would be observations
/// whose stated source of truth does not exist, which quietly voids replay.
#[async_trait]
pub trait Archive: Send + Sync {
    async fn put(&self, key: &str, envelope: &Envelope) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Envelope>;
}

fn encode(envelope: &Envelope) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(envelope)?;
    Ok(zstd::encode_all(&json[..], 3)?)
}

fn decode(bytes: &[u8]) -> Result<Envelope> {
    let json = zstd::decode_all(bytes)?;
    Ok(serde_json::from_slice(&json)?)
}

/// S3 compatible object storage. MinIO in development and the same code path in
/// production, which is why there is no storage abstraction beyond this.
pub struct S3Archive {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3Archive {
    /// `path_style` and `region` differ by store and cannot be guessed. MinIO
    /// serves paths and ignores the region. Other S3 compatible stores serve
    /// virtual hosted buckets and state their own region, and sending the wrong
    /// one fails at the first request rather than at configuration time.
    pub async fn connect(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
        path_style: bool,
    ) -> Result<Self> {
        let credentials = aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "fc-market-config",
        );
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region.to_string()))
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .force_path_style(path_style)
            .build();

        let archive = Self {
            client: aws_sdk_s3::Client::from_conf(config),
            bucket: bucket.to_string(),
        };
        archive.ensure_bucket().await?;
        Ok(archive)
    }

    /// The archive is a precondition for ingestion, so a missing bucket must fail
    /// at start rather than at the first batch write.
    async fn ensure_bucket(&self) -> Result<()> {
        if self
            .client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }
        self.client
            .create_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .with_context(|| format!("cannot create bucket {}", self.bucket))?;
        Ok(())
    }

    /// Used by the nightly dump, which stores a binary file rather than an
    /// envelope.
    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.into())
            .send()
            .await
            .with_context(|| format!("cannot write {key}"))?;
        Ok(())
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();

        while let Some(page) = pages.next().await {
            let page = page.with_context(|| format!("cannot list {prefix}"))?;
            keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|o| o.key().map(String::from)),
            );
        }
        Ok(keys)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("cannot delete {key}"))?;
        Ok(())
    }
}

#[async_trait]
impl Archive for S3Archive {
    async fn put(&self, key: &str, envelope: &Envelope) -> Result<()> {
        self.put_bytes(key, encode(envelope)?).await
    }

    async fn get(&self, key: &str) -> Result<Envelope> {
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("cannot read {key}"))?;
        let bytes = object.body.collect().await?.into_bytes();
        decode(&bytes)
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

    /// The stored bytes must come back as the same envelope. `requested_ids` is
    /// the field that matters most: when a response carries no player identifier
    /// it is the only thing that can attribute the body on replay.
    #[test]
    fn an_encoded_envelope_decodes_back_identically() {
        let env = Envelope::new(
            "https://example.test/prices",
            vec!["101".into(), "102".into()],
            200,
            serde_json::json!({"prices": [{"id": 101, "price": 12000}]}),
            at(),
        );

        let back = decode(&encode(&env).unwrap()).unwrap();

        assert_eq!(back.body, env.body);
        assert_eq!(back.requested_ids, vec!["101", "102"]);
        assert_eq!(back.sha256, env.sha256);
    }

    #[test]
    fn each_kind_of_payload_gets_its_own_prefix() {
        let p = object_key("fixture", Kind::Prices, "r", 0, at());
        let m = object_key("fixture", Kind::Metadata, "r", 0, at());
        let a = object_key("fixture", Kind::AssetList, "r", 0, at());
        assert_eq!(p, "raw/fixture/2026/08/07/r-0.json.zst");
        assert_eq!(m, "raw/fixture/metadata/2026/08/07/r-0.json.zst");
        assert_eq!(a, "raw/fixture/assets/2026/08/07/r-0.json.zst");
    }

    #[test]
    fn the_checksum_follows_the_body() {
        let a = Envelope::new("u", vec![], 200, serde_json::json!({"a": 1}), at());
        let b = Envelope::new("u", vec![], 200, serde_json::json!({"a": 2}), at());
        assert_ne!(a.sha256, b.sha256);
    }
}
