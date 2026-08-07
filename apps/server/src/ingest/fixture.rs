//! A source that serves archived payloads from disk.
//!
//! No provider is available yet, so this module stands in as the first real
//! source. Every link in the pipeline is genuine except the network call, which
//! keeps the parser tests deterministic and makes replay testable today. A real
//! provider replaces this one file and changes no caller.
//!
//! Every provider type below is private. A provider shape must not escape its
//! module, because the moment one does, the rest of the program starts depending
//! on a provider we intend to be able to replace.

use crate::archive::Envelope;
use crate::domain::{AssetAttributes, Rarity};
use crate::ids::Platform;
use crate::source::{FetchError, FetchResult, Listing, ParsedQuote, Source};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::path::{Path, PathBuf};

pub const SOURCE: &str = "fixture";
pub const PARSER_VERSION: &str = "fixture/1";

/// A price for one card on both platforms, as the provider states it.
#[derive(Debug, Deserialize)]
struct PriceRecord {
    /// Present on purpose. A response that cannot be keyed to a player must be
    /// requested one player at a time instead, because a positional mapping
    /// attaches every later price to the wrong asset and nothing raises an error.
    id: String,
    /// Rule 1 compares this against the resolved asset when it is present.
    name: Option<String>,
    playstation: Option<Quote>,
    pc: Option<Quote>,
}

#[derive(Debug, Deserialize)]
struct Quote {
    price: Option<i64>,
    #[serde(rename = "minPrice")]
    min_price: Option<i64>,
    #[serde(rename = "maxPrice")]
    max_price: Option<i64>,
    /// The provider's own change timestamp. This becomes observed_at.
    #[serde(rename = "priceUpdate")]
    price_update: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct ListingRecord {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct MetadataRecord {
    id: String,
    name: String,
    #[serde(rename = "eaBaseId")]
    ea_base_id: Option<i64>,
    rating: Option<i16>,
    rarity: Option<String>,
    version: Option<String>,
    position: Option<String>,
    league: Option<i32>,
    nation: Option<i32>,
    club: Option<i32>,
    #[serde(rename = "skillMoves")]
    skill_moves: Option<i16>,
    #[serde(rename = "weakFoot")]
    weak_foot: Option<i16>,
    pace: Option<i16>,
    shooting: Option<i16>,
    passing: Option<i16>,
    dribbling: Option<i16>,
    defending: Option<i16>,
    physicality: Option<i16>,
    #[serde(rename = "playStyles")]
    play_styles: Option<Vec<String>>,
}

/// Serves prices, the asset list and card attributes from a fixture directory.
pub struct FixtureSource {
    root: PathBuf,
}

impl FixtureSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    async fn read_json(&self, file: &str) -> Result<serde_json::Value> {
        let path: PathBuf = Path::new(&self.root).join(file);
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("cannot read fixture {}", path.display()))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn url(&self, file: &str) -> String {
        format!("file://{}/{}", self.root.display(), file)
    }
}

/// Keeps only the records the run asked for. A real provider returns exactly
/// what was requested, and an identifier it no longer knows simply does not come
/// back. That absence is coverage evidence, so the orchestration records it.
fn select_requested(body: serde_json::Value, wanted: &[String]) -> serde_json::Value {
    let serde_json::Value::Array(records) = body else {
        return body;
    };
    serde_json::Value::Array(
        records
            .into_iter()
            .filter(|r| {
                r.get("id")
                    .and_then(|v| v.as_str())
                    .is_some_and(|id| wanted.iter().any(|w| w == id))
            })
            .collect(),
    )
}

#[async_trait]
impl Source for FixtureSource {
    fn name(&self) -> &'static str {
        SOURCE
    }

    fn parser_version(&self) -> &'static str {
        PARSER_VERSION
    }

    /// Safe above 1 only because every record in this provider's price payload
    /// carries its own `id`. See the Rule 1 note on the trait.
    fn price_batch_size(&self) -> usize {
        50
    }

    fn metadata_batch_size(&self) -> usize {
        50
    }

    async fn fetch_prices(&self, external_ids: &[String]) -> FetchResult {
        let body = self
            .read_json("prices.json")
            .await
            .map_err(FetchError::Other)?;
        Ok(Envelope::new(
            self.url("prices.json"),
            external_ids.to_vec(),
            200,
            select_requested(body, external_ids),
            Utc::now(),
        ))
    }

    fn parse_prices(&self, envelope: &Envelope) -> Result<Vec<ParsedQuote>> {
        // Borrowed, not cloned: from_value would deep-copy the whole payload,
        // and this runs on every batch of every run and again on replay.
        let records: Vec<PriceRecord> = Vec::deserialize(&envelope.body)?;
        let mut out = Vec::with_capacity(records.len() * 2);

        for r in records {
            for (platform, quote) in [(Platform::Playstation, r.playstation), (Platform::Pc, r.pc)]
            {
                let Some(q) = quote else { continue };
                out.push(ParsedQuote {
                    external_id: r.id.clone(),
                    platform,
                    price: q.price,
                    min_price: q.min_price,
                    max_price: q.max_price,
                    observed_at: q.price_update,
                    name: r.name.clone(),
                });
            }
        }
        Ok(out)
    }

    async fn fetch_asset_list(&self) -> FetchResult {
        let body = self
            .read_json("assets.json")
            .await
            .map_err(FetchError::Other)?;
        Ok(Envelope::new(
            self.url("assets.json"),
            Vec::new(),
            200,
            body,
            Utc::now(),
        ))
    }

    fn parse_asset_list(&self, envelope: &Envelope) -> Result<Vec<Listing>> {
        // Borrowed, not cloned: from_value would deep-copy the whole payload,
        // and this runs on every batch of every run and again on replay.
        let records: Vec<ListingRecord> = Vec::deserialize(&envelope.body)?;
        Ok(records
            .into_iter()
            .map(|r| Listing {
                external_id: r.id,
                name: r.name,
            })
            .collect())
    }

    async fn fetch_metadata(&self, external_ids: &[String]) -> FetchResult {
        let body = self
            .read_json("metadata.json")
            .await
            .map_err(FetchError::Other)?;
        Ok(Envelope::new(
            self.url("metadata.json"),
            external_ids.to_vec(),
            200,
            select_requested(body, external_ids),
            Utc::now(),
        ))
    }

    /// Maps provider codes onto the canonical domains we own. Doing this here, at
    /// the edge, is what stops a provider change from silently moving assets
    /// between valuation classes.
    fn parse_metadata(&self, envelope: &Envelope) -> Result<Vec<AssetAttributes>> {
        // Borrowed, not cloned: from_value would deep-copy the whole payload,
        // and this runs on every batch of every run and again on replay.
        let records: Vec<MetadataRecord> = Vec::deserialize(&envelope.body)?;

        Ok(records
            .into_iter()
            .map(|r| AssetAttributes {
                name: r.name,
                external_id: r.id,
                ea_base_id: r.ea_base_id,
                rating: r.rating,
                rarity: match r.rarity.as_deref() {
                    Some("common") => Rarity::Common,
                    _ => Rarity::Rare,
                },
                version: r.version.unwrap_or_else(|| "base".to_string()),
                position: r.position,
                league_id: r.league,
                nation_id: r.nation,
                club_id: r.club,
                skill_moves: r.skill_moves,
                weak_foot: r.weak_foot,
                pace: r.pace,
                shooting: r.shooting,
                passing: r.passing,
                dribbling: r.dribbling,
                defending: r.defending,
                physicality: r.physicality,
                playstyle_count: r.play_styles.map(|p| p.len() as i16),
            })
            .collect())
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

    fn envelope(body: serde_json::Value) -> Envelope {
        Envelope::new("file://test", vec![], 200, body, at())
    }

    fn source() -> FixtureSource {
        FixtureSource::new("fixtures/fixture")
    }

    fn prices_json() -> serde_json::Value {
        serde_json::json!([
            {
                "id": "505120", "name": "Zinedine Zidane",
                "playstation": {
                    "price": 1420000, "minPrice": 900000, "maxPrice": 2000000,
                    "priceUpdate": "2026-08-07T09:00:00Z"
                },
                "pc": {
                    "price": 1380000, "minPrice": 900000, "maxPrice": 2000000,
                    "priceUpdate": "2026-08-07T09:00:00Z"
                }
            },
            { "id": "231747", "playstation": { "price": null, "priceUpdate": null }, "pc": null }
        ])
    }

    #[test]
    fn one_record_yields_one_quote_for_each_platform_present() {
        let quotes = source().parse_prices(&envelope(prices_json())).unwrap();
        assert_eq!(quotes.len(), 3);
        assert_eq!(quotes[0].external_id, "505120");
        assert_eq!(quotes[0].platform, Platform::Playstation);
        assert_eq!(quotes[1].platform, Platform::Pc);
        // The second record has no pc quote at all, so it contributes one entry.
        assert_eq!(quotes[2].external_id, "231747");
    }

    /// The parser reshapes and does not judge. A missing price and a missing
    /// timestamp both survive parsing so the caller can count them as rejections.
    #[test]
    fn a_missing_price_survives_parsing_for_the_caller_to_reject() {
        let quotes = source().parse_prices(&envelope(prices_json())).unwrap();
        let empty = quotes.iter().find(|q| q.external_id == "231747").unwrap();
        assert_eq!(empty.price, None);
        assert_eq!(empty.observed_at, None);
    }

    #[test]
    fn quotes_keep_the_provider_timestamp_not_our_clock() {
        let quotes = source().parse_prices(&envelope(prices_json())).unwrap();
        assert_eq!(
            quotes[0].observed_at.unwrap().to_rfc3339(),
            "2026-08-07T09:00:00+00:00"
        );
    }

    /// Rule 1 needs the name to reach the caller, or the identity check has
    /// nothing to compare against.
    #[test]
    fn the_payload_name_reaches_the_caller() {
        let quotes = source().parse_prices(&envelope(prices_json())).unwrap();
        assert_eq!(quotes[0].name.as_deref(), Some("Zinedine Zidane"));
    }

    #[test]
    fn metadata_maps_onto_the_domains_we_own() {
        let body = serde_json::json!([{
            "id": "505120", "name": "Test Player", "eaBaseId": 231747,
            "rating": 97, "rarity": "rare", "version": "tots", "position": "CM",
            "playStyles": ["Incisive Pass", "Press Proven"]
        }]);
        let attrs = source().parse_metadata(&envelope(body)).unwrap();
        assert_eq!(attrs[0].version, "tots");
        assert_eq!(attrs[0].rarity, Rarity::Rare);
        assert_eq!(attrs[0].playstyle_count, Some(2));
    }

    /// An absent version must not silently become a promotional one, because the
    /// class median groups on it.
    #[test]
    fn a_missing_version_defaults_to_base() {
        let body = serde_json::json!([{ "id": "1", "name": "X" }]);
        let attrs = source().parse_metadata(&envelope(body)).unwrap();
        assert_eq!(attrs[0].version, "base");
    }

    /// An identifier the provider no longer knows simply does not come back. The
    /// orchestration must still see that we asked, so the filter drops the record
    /// rather than inventing one.
    #[test]
    fn a_response_carries_only_the_records_that_were_requested() {
        let selected = select_requested(prices_json(), &["231747".to_string()]);
        let records = selected.as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["id"], "231747");
    }
}
