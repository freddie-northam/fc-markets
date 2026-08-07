//! A source that reads archived envelopes from disk.
//!
//! No provider is available yet, so this module stands in as the first real
//! source. Every link in the pipeline is genuine except the network call, which
//! keeps the parser tests deterministic and makes replay testable today.
//!
//! Every type below is private. A provider shape must not escape its module,
//! because the moment one does, the rest of the program starts depending on a
//! provider we intend to be able to replace.

use crate::domain::{AssetAttributes, Rarity};
use chrono::{DateTime, Utc};
use serde::Deserialize;

pub const SOURCE: &str = "fixture";
pub const PARSER_VERSION: &str = "fixture/1";

/// A price for one card on both platforms, as the provider states it.
#[derive(Debug, Deserialize)]
struct PriceRecord {
    /// Present here on purpose. A response that cannot be keyed to a player must
    /// be requested one player at a time instead, because a positional mapping
    /// attaches every later price to the wrong asset and nothing raises an error.
    id: String,
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

/// One quote after conversion. The caller resolves identity and applies the
/// validation rules; this module only reshapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedQuote {
    pub external_id: String,
    pub platform: &'static str,
    pub price: Option<i64>,
    pub min_price: Option<i64>,
    pub max_price: Option<i64>,
    pub observed_at: Option<DateTime<Utc>>,
}

/// Reshapes a price payload. It never rejects a record: rejection is the
/// caller's job, because only the caller knows the run's own start time and the
/// game's release date.
pub fn parse_prices(body: &serde_json::Value) -> Result<Vec<ParsedQuote>, serde_json::Error> {
    let records: Vec<PriceRecord> = serde_json::from_value(body.clone())?;
    let mut out = Vec::with_capacity(records.len() * 2);

    for r in records {
        for (platform, quote) in [("playstation", r.playstation), ("pc", r.pc)] {
            let Some(q) = quote else { continue };
            out.push(ParsedQuote {
                external_id: r.id.clone(),
                platform,
                price: q.price,
                min_price: q.min_price,
                max_price: q.max_price,
                observed_at: q.price_update,
            });
        }
    }
    Ok(out)
}

/// Maps provider codes onto the canonical domains we own. Doing this here, at
/// the edge, is what stops a provider change from silently moving assets between
/// valuation classes.
pub fn parse_metadata(
    body: &serde_json::Value,
) -> Result<Vec<AssetAttributes>, serde_json::Error> {
    let records: Vec<MetadataRecord> = serde_json::from_value(body.clone())?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn prices_json() -> serde_json::Value {
        serde_json::json!([
            {
                "id": "505120",
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
        let quotes = parse_prices(&prices_json()).unwrap();
        assert_eq!(quotes.len(), 3);
        assert_eq!(quotes[0].external_id, "505120");
        assert_eq!(quotes[0].platform, "playstation");
        assert_eq!(quotes[1].platform, "pc");
        // The second record has no pc quote at all, so it contributes one entry.
        assert_eq!(quotes[2].external_id, "231747");
    }

    /// The parser reshapes and does not judge. A missing price and a missing
    /// timestamp both survive parsing so the caller can count them as rejections.
    #[test]
    fn a_missing_price_survives_parsing_for_the_caller_to_reject() {
        let quotes = parse_prices(&prices_json()).unwrap();
        let empty = quotes.iter().find(|q| q.external_id == "231747").unwrap();
        assert_eq!(empty.price, None);
        assert_eq!(empty.observed_at, None);
    }

    #[test]
    fn quotes_keep_the_provider_timestamp_not_our_clock() {
        let quotes = parse_prices(&prices_json()).unwrap();
        assert_eq!(
            quotes[0].observed_at.unwrap().to_rfc3339(),
            "2026-08-07T09:00:00+00:00"
        );
    }

    #[test]
    fn metadata_maps_onto_the_domains_we_own() {
        let body = serde_json::json!([{
            "id": "505120", "name": "Test Player", "eaBaseId": 231747,
            "rating": 97, "rarity": "rare", "version": "tots", "position": "CM",
            "playStyles": ["Incisive Pass", "Press Proven"]
        }]);
        let attrs = parse_metadata(&body).unwrap();
        assert_eq!(attrs[0].version, "tots");
        assert_eq!(attrs[0].rarity, Rarity::Rare);
        assert_eq!(attrs[0].playstyle_count, Some(2));
    }

    /// An absent version must not silently become a promotional one, because the
    /// class median groups on it.
    #[test]
    fn a_missing_version_defaults_to_base() {
        let body = serde_json::json!([{ "id": "1", "name": "X" }]);
        let attrs = parse_metadata(&body).unwrap();
        assert_eq!(attrs[0].version, "base");
    }
}
