//! The FUT-DB provider (`api.fut-db.com`). Provider types stay private.
//!
//! Three measured facts shape this module. The documentation contradicts two.
//! 1. The bulk price endpoint charges N+1 requests for N cards and returns no
//!    identifier. It is worse on both counts, so `price_batch_size` is 1.
//! 2. `/api/players` returns whole records, 20 to a page. Attributes come from
//!    the list walk. The metadata step only refreshes a stale card.
//! 3. `version` is the game year, always "26". The card version comes from
//!    `rarity`, an integer into a 175-row vocabulary.

use crate::archive::Envelope;
use crate::domain::{AssetAttributes, Rarity, canonical_version};
use crate::ids::Platform;
use crate::source::{FetchError, FetchResult, Listing, ParsedQuote, Source};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

pub const SOURCE: &str = "futdb";
pub const PARSER_VERSION: &str = "futdb/1";
pub const DEFAULT_BASE_URL: &str = "https://api.fut-db.com";

/// The rarity names that carry no promotional version.
const BASE_RARITIES: [&str; 2] = ["common", "rare"];

#[derive(Debug, Deserialize)]
struct PricesResponse {
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
    #[serde(rename = "priceUpdate", default, deserialize_with = "naive_utc")]
    price_update: Option<DateTime<Utc>>,
}

/// The value the provider sends for a card it has never priced. It is .NET's
/// `DateTime.MinValue`, so it means "never" and not a date. Parsed as a date it
/// becomes year 1 AD, which rule 2 would then reject as out of range: the right
/// outcome for the wrong reason, and only by luck, because rule 3 checks the
/// price first and reports the accurate rejection.
const NEVER: &str = "0001-01-01T00:00:00";

/// The provider sends no zone, as `2026-08-17T12:40:34.58`. Local time would
/// move every observation by the host offset. An absent, unreadable or sentinel
/// value becomes None, which rule 3 then rejects.
fn naive_utc<'de, D>(d: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(raw) = Option::<String>::deserialize(d)? else {
        return Ok(None);
    };
    if raw.starts_with(NEVER) {
        return Ok(None);
    }
    if let Ok(zoned) = DateTime::parse_from_rfc3339(&raw) {
        return Ok(Some(zoned.with_timezone(&Utc)));
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(&raw, format) {
            return Ok(Some(Utc.from_utc_datetime(&naive)));
        }
    }
    Ok(None)
}

#[derive(Debug, Deserialize)]
struct Paged<T> {
    pagination: Option<Pagination>,
    items: Option<Vec<T>>,
}

#[derive(Debug, Deserialize)]
struct Pagination {
    #[serde(rename = "pageCurrent")]
    page_current: Option<u32>,
    #[serde(rename = "pageTotal")]
    page_total: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct PlayerResponse {
    player: Option<PlayerModel>,
}

#[derive(Debug, Deserialize)]
struct RarityModel {
    id: i32,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PlayerModel {
    id: i64,
    name: Option<String>,
    #[serde(rename = "commonName")]
    common_name: Option<String>,
    #[serde(rename = "resourceBaseId")]
    resource_base_id: Option<i64>,
    rating: Option<i16>,
    rarity: Option<i32>,
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
    play_styles: Option<Vec<serde_json::Value>>,
    #[serde(rename = "playStylesPlus")]
    play_styles_plus: Option<Vec<serde_json::Value>>,
}

pub struct FutdbSource {
    base_url: String,
    client: reqwest::Client,
    api_key: String,
    rarities: HashMap<i32, String>,
}

impl FutdbSource {
    /// Loads the rarity vocabulary at construction. A failure here stops the
    /// process. Without the vocabulary every card looks like a base card.
    pub async fn connect(base_url: &str, api_key: &str, timeout: Duration) -> Result<Self> {
        let mut source = Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            // Section 4.5 requires an explicit timeout. A hung provider otherwise
            // holds the advisory lock until the reaper takes the run.
            client: reqwest::Client::builder().timeout(timeout).build()?,
            api_key: api_key.to_string(),
            rarities: HashMap::new(),
        };
        source.rarities = source.load_rarities().await?;
        Ok(source)
    }

    /// One GET. The envelope keeps the status, so replay sees what we saw.
    async fn get(&self, path: &str, requested_ids: Vec<String>) -> FetchResult {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .client
            .get(&url)
            .header("X-AUTH-TOKEN", &self.api_key)
            .send()
            .await
            .map_err(|e| FetchError::Other(anyhow!("{url} could not be reached: {e}")))?;
        let status = response.status().as_u16();
        // 429 is the documented refusal. 403 means the premium window closed,
        // which stops a run the same way.
        if status == 429 || status == 403 {
            return Err(FetchError::RateLimited);
        }
        let fetched_at = Utc::now();
        let body = response
            .json()
            .await
            .map_err(|e| FetchError::Other(anyhow!("{url} did not return JSON: {e}")))?;
        Ok(Envelope::new(url, requested_ids, status, body, fetched_at))
    }

    /// Reads all 9 pages. The first page alone leaves 155 rarities unnamed.
    async fn load_rarities(&self) -> Result<HashMap<i32, String>> {
        let mut out = HashMap::new();
        let mut page = 1u32;
        loop {
            let envelope = self
                .get(&format!("/api/rarities?page={page}"), Vec::new())
                .await
                .map_err(|e| anyhow!("the rarity vocabulary could not be loaded: {e}"))?;
            let parsed: Paged<RarityModel> = serde_json::from_value(envelope.body)
                .context("the rarity vocabulary had an unexpected shape")?;
            for rarity in parsed.items.unwrap_or_default() {
                if let Some(name) = rarity.name {
                    out.insert(rarity.id, name);
                }
            }
            let total = parsed.pagination.and_then(|p| p.page_total).unwrap_or(1);
            if page >= total {
                break;
            }
            page += 1;
        }
        if out.is_empty() {
            return Err(anyhow!("the rarity vocabulary came back empty"));
        }
        Ok(out)
    }

    /// An unknown rarity becomes base. That understates a new promotional card
    /// instead of mixing it into a promotional class.
    fn version_for(&self, rarity: Option<i32>) -> String {
        match rarity.and_then(|id| self.rarities.get(&id)) {
            Some(name) if !BASE_RARITIES.contains(&name.trim().to_lowercase().as_str()) => {
                canonical_version(Some(name.clone()))
            }
            _ => canonical_version(None),
        }
    }

    fn attributes_from(&self, player: PlayerModel) -> AssetAttributes {
        let version = self.version_for(player.rarity);
        let base = version == canonical_version(None);
        AssetAttributes {
            // The game shows commonName when it exists. Rule 1 compares this
            // against the held name, so every path must pick it the same way.
            name: player
                .common_name
                .filter(|n| !n.trim().is_empty())
                .or(player.name)
                .unwrap_or_default(),
            external_id: player.id.to_string(),
            ea_base_id: player.resource_base_id,
            rating: player.rating,
            rarity: if base { Rarity::Common } else { Rarity::Rare },
            version,
            position: player.position,
            league_id: player.league,
            nation_id: player.nation,
            club_id: player.club,
            skill_moves: player.skill_moves,
            weak_foot: player.weak_foot,
            pace: player.pace,
            shooting: player.shooting,
            passing: player.passing,
            dribbling: player.dribbling,
            defending: player.defending,
            physicality: player.physicality,
            playstyle_count: Some(
                (player.play_styles.map_or(0, |p| p.len())
                    + player.play_styles_plus.map_or(0, |p| p.len())) as i16,
            ),
        }
    }

    fn players(&self, envelope: &Envelope) -> Result<Vec<PlayerModel>> {
        let parsed: Paged<PlayerModel> = serde_json::from_value(envelope.body.clone())?;
        Ok(parsed.items.unwrap_or_default())
    }
}

#[async_trait]
impl Source for FutdbSource {
    fn name(&self) -> &'static str {
        SOURCE
    }

    fn parser_version(&self) -> &'static str {
        PARSER_VERSION
    }

    /// Fact 1. The bulk endpoint costs more and cannot be keyed, so this is 1.
    fn price_batch_size(&self) -> usize {
        1
    }

    fn metadata_batch_size(&self) -> usize {
        1
    }

    async fn fetch_prices(&self, external_ids: &[String]) -> FetchResult {
        let [id] = external_ids else {
            return Err(FetchError::Other(anyhow!(
                "this provider prices one card at a time, but {} were requested",
                external_ids.len()
            )));
        };
        self.get(&format!("/api/players/{id}/price"), external_ids.to_vec())
            .await
    }

    /// The response carries no identifier, so the quote takes the requested one.
    /// That holds only because `fetch_prices` allows exactly one.
    fn parse_prices(&self, envelope: &Envelope) -> Result<Vec<ParsedQuote>> {
        let [external_id] = envelope.requested_ids.as_slice() else {
            return Err(anyhow!(
                "a price envelope must carry exactly one requested identifier, found {}",
                envelope.requested_ids.len()
            ));
        };
        let parsed: PricesResponse = serde_json::from_value(envelope.body.clone())?;
        Ok([
            (Platform::Playstation, parsed.playstation),
            (Platform::Pc, parsed.pc),
        ]
        .into_iter()
        .filter_map(|(platform, quote)| {
            let q = quote?;
            Some(ParsedQuote {
                external_id: external_id.clone(),
                platform,
                price: q.price,
                min_price: q.min_price,
                max_price: q.max_price,
                observed_at: q.price_update,
                // The price response states no name, so rule 1 cannot fire here.
                // It still fires where this provider does state one.
                name: None,
            })
        })
        .collect())
    }

    async fn fetch_asset_list(&self, page: u32) -> FetchResult {
        self.get(&format!("/api/players?page={page}"), Vec::new())
            .await
    }

    fn parse_asset_list(&self, envelope: &Envelope) -> Result<Vec<Listing>> {
        Ok(self
            .players(envelope)?
            .into_iter()
            .map(|p| {
                let attrs = self.attributes_from(p);
                Listing {
                    external_id: attrs.external_id,
                    name: attrs.name,
                }
            })
            .collect())
    }

    fn next_asset_list_page(&self, envelope: &Envelope) -> Option<u32> {
        let parsed: Paged<PlayerModel> = serde_json::from_value(envelope.body.clone()).ok()?;
        let p = parsed.pagination?;
        let (current, total) = (p.page_current?, p.page_total?);
        (current < total).then_some(current + 1)
    }

    /// Fact 2. The metadata endpoint would cost one request per card, which is
    /// 27,121 on a first run against a budget of 18,000 a day.
    fn parse_asset_list_attributes(&self, envelope: &Envelope) -> Result<Vec<AssetAttributes>> {
        Ok(self
            .players(envelope)?
            .into_iter()
            .map(|p| self.attributes_from(p))
            .collect())
    }

    async fn fetch_metadata(&self, external_ids: &[String]) -> FetchResult {
        let [id] = external_ids else {
            return Err(FetchError::Other(anyhow!(
                "this provider reads attributes one card at a time, but {} were requested",
                external_ids.len()
            )));
        };
        self.get(&format!("/api/players/{id}"), external_ids.to_vec())
            .await
    }

    fn parse_metadata(&self, envelope: &Envelope) -> Result<Vec<AssetAttributes>> {
        let parsed: PlayerResponse = serde_json::from_value(envelope.body.clone())?;
        Ok(parsed
            .player
            .map(|p| vec![self.attributes_from(p)])
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-17T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn source() -> FutdbSource {
        FutdbSource {
            base_url: DEFAULT_BASE_URL.to_string(),
            client: reqwest::Client::new(),
            api_key: "test".to_string(),
            rarities: HashMap::from([
                (0, "Common".to_string()),
                (1, "Rare".to_string()),
                (11, "TEAM OF THE SEASON".to_string()),
                (12, "Icon".to_string()),
            ]),
        }
    }

    fn price_envelope(ids: Vec<String>, body: serde_json::Value) -> Envelope {
        Envelope::new("https://test/price", ids, 200, body, at())
    }

    /// Copied from a live response on 2026-08-17.
    fn price_body() -> serde_json::Value {
        serde_json::json!({
            "playstation": { "id": 115916, "price": 350, "minPrice": 300,
                "maxPrice": 10000, "prp": 0, "priceUpdate": "2026-08-17T12:40:34.58" },
            "pc": { "id": 115917, "price": 400, "minPrice": 300,
                "maxPrice": 10000, "prp": 1, "priceUpdate": "2026-08-17T12:40:34.58" }
        })
    }

    #[test]
    fn one_response_yields_one_quote_for_each_platform() {
        let quotes = source()
            .parse_prices(&price_envelope(vec!["26353".into()], price_body()))
            .unwrap();
        assert_eq!(quotes.len(), 2);
        assert_eq!(quotes[0].platform, Platform::Playstation);
        assert_eq!(quotes[0].price, Some(350));
        assert_eq!(quotes[1].platform, Platform::Pc);
        assert_eq!(quotes[1].price, Some(400));
    }

    /// The response has no identifier, so both quotes take the requested one.
    /// This is why price_batch_size is 1.
    #[test]
    fn both_quotes_are_attributed_to_the_requested_identifier() {
        let quotes = source()
            .parse_prices(&price_envelope(vec!["26353".into()], price_body()))
            .unwrap();
        assert!(quotes.iter().all(|q| q.external_id == "26353"));
    }

    /// A guess here is the defect that rule 1 exists to stop.
    #[test]
    fn an_envelope_that_does_not_carry_exactly_one_identifier_is_refused() {
        let s = source();
        for ids in [vec!["1".into(), "2".into()], Vec::new()] {
            assert!(s.parse_prices(&price_envelope(ids, price_body())).is_err());
        }
    }

    #[test]
    fn an_unzoned_timestamp_is_read_as_utc() {
        let quotes = source()
            .parse_prices(&price_envelope(vec!["26353".into()], price_body()))
            .unwrap();
        assert_eq!(
            quotes[0].observed_at.unwrap().to_rfc3339(),
            "2026-08-17T12:40:34.580+00:00"
        );
    }

    #[test]
    fn a_zoned_timestamp_is_still_accepted() {
        let body = serde_json::json!({
            "playstation": { "price": 1, "priceUpdate": "2026-08-17T12:40:34Z" }, "pc": null });
        let quotes = source()
            .parse_prices(&price_envelope(vec!["1".into()], body))
            .unwrap();
        assert_eq!(
            quotes[0].observed_at.unwrap().to_rfc3339(),
            "2026-08-17T12:40:34+00:00"
        );
    }

    /// The parser reshapes and does not judge. Rule 3 rejects it later.
    #[test]
    fn an_unreadable_timestamp_survives_parsing_as_absent() {
        let body = serde_json::json!({
            "playstation": { "price": 1, "priceUpdate": "not a timestamp" }, "pc": null });
        let quotes = source()
            .parse_prices(&price_envelope(vec!["1".into()], body))
            .unwrap();
        assert_eq!(quotes[0].observed_at, None);
        assert_eq!(quotes[0].price, Some(1));
    }

    /// The exact shape a never-priced card returns, copied from player 55682 on
    /// 2026-08-17. The sentinel must not become a date: parsed, it is year 1 AD.
    #[test]
    fn the_never_priced_sentinel_is_read_as_no_timestamp() {
        let body = serde_json::json!({
            "playstation": { "id": 1, "price": null, "minPrice": null, "maxPrice": null,
                "prp": null, "priceUpdate": "0001-01-01T00:00:00" },
            "pc": null
        });
        let quotes = source()
            .parse_prices(&price_envelope(vec!["55682".into()], body))
            .unwrap();
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].price, None);
        assert_eq!(
            quotes[0].observed_at, None,
            "the sentinel means never, so it must not arrive as a year 1 date"
        );
    }

    #[test]
    fn a_null_platform_contributes_no_quote_and_does_not_fail() {
        let body = serde_json::json!({ "playstation": null, "pc": null });
        assert!(
            source()
                .parse_prices(&price_envelope(vec!["1".into()], body))
                .unwrap()
                .is_empty()
        );
    }

    fn list_envelope(body: serde_json::Value) -> Envelope {
        Envelope::new("https://test/players", Vec::new(), 200, body, at())
    }

    fn list_body(page: u32, total: u32, rarity: i32) -> serde_json::Value {
        serde_json::json!({
            "pagination": { "countCurrent": 1, "countTotal": 27121,
                "pageCurrent": page, "pageTotal": total, "itemsPerPage": 20 },
            "items": [{
                "id": 26353, "name": "Christy Ucheibe", "commonName": null,
                "resourceBaseId": 231747, "rating": 72, "rarity": rarity,
                "position": "CM", "league": 1, "nation": 2, "club": 3,
                "skillMoves": 3, "weakFoot": 3, "pace": 70, "shooting": 60,
                "passing": 65, "dribbling": 68, "defending": 72,
                "physicality": 75, "version": "26",
                "playStyles": ["Anticipate"], "playStylesPlus": ["Bruiser"] }]
        })
    }

    /// Fact 3. A miss here puts the whole catalogue in one tier. The rarity name
    /// keeps its words and only its shape is folded, so the version is
    /// `team_of_the_season` and never the provider's `version` of "26".
    #[test]
    fn the_card_version_comes_from_rarity_not_from_the_version_field() {
        let attrs = source()
            .parse_asset_list_attributes(&list_envelope(list_body(1, 5, 11)))
            .unwrap();
        assert_eq!(attrs[0].version, "team_of_the_season");
    }

    #[test]
    fn common_and_rare_both_fold_to_the_base_version() {
        for rarity in [0, 1] {
            let attrs = source()
                .parse_asset_list_attributes(&list_envelope(list_body(1, 5, rarity)))
                .unwrap();
            assert_eq!(attrs[0].version, canonical_version(None));
            assert_eq!(attrs[0].rarity, Rarity::Common);
        }
    }

    #[test]
    fn an_unknown_rarity_falls_back_to_base() {
        let attrs = source()
            .parse_asset_list_attributes(&list_envelope(list_body(1, 5, 9999)))
            .unwrap();
        assert_eq!(attrs[0].version, canonical_version(None));
    }

    #[test]
    fn playstyles_count_both_lists() {
        let attrs = source()
            .parse_asset_list_attributes(&list_envelope(list_body(1, 5, 12)))
            .unwrap();
        assert_eq!(attrs[0].playstyle_count, Some(2));
    }

    #[test]
    fn the_list_walk_advances_until_the_provider_says_it_is_done() {
        let s = source();
        assert_eq!(
            s.next_asset_list_page(&list_envelope(list_body(1, 5, 0))),
            Some(2)
        );
        assert_eq!(
            s.next_asset_list_page(&list_envelope(list_body(4, 5, 0))),
            Some(5)
        );
        assert_eq!(
            s.next_asset_list_page(&list_envelope(list_body(5, 5, 0))),
            None
        );
    }

    /// A page without pagination must end the walk. Some(1) would loop until
    /// the budget died.
    #[test]
    fn a_response_without_pagination_ends_the_walk() {
        let body = serde_json::json!({ "items": [] });
        assert_eq!(source().next_asset_list_page(&list_envelope(body)), None);
    }

    /// Rule 1 compares the name one path writes against the name the other holds.
    #[test]
    fn the_listing_and_the_attributes_agree_on_identity() {
        let envelope = list_envelope(list_body(1, 1, 12));
        let listings = source().parse_asset_list(&envelope).unwrap();
        let attrs = source().parse_asset_list_attributes(&envelope).unwrap();
        assert_eq!(listings[0].external_id, attrs[0].external_id);
        assert_eq!(listings[0].name, attrs[0].name);
    }

    #[test]
    fn the_common_name_wins_but_an_empty_one_does_not() {
        for (given, want) in [("Ucheibe", "Ucheibe"), ("  ", "Christy Ucheibe")] {
            let mut body = list_body(1, 1, 0);
            body["items"][0]["commonName"] = serde_json::json!(given);
            let attrs = source()
                .parse_asset_list_attributes(&list_envelope(body))
                .unwrap();
            assert_eq!(attrs[0].name, want);
        }
    }

    #[test]
    fn metadata_reads_the_single_player_envelope() {
        let body = serde_json::json!({
            "player": { "id": 26353, "name": "Christy Ucheibe", "rating": 72, "rarity": 12 } });
        let envelope = Envelope::new("https://test/p", vec!["26353".into()], 200, body, at());
        let attrs = source().parse_metadata(&envelope).unwrap();
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].external_id, "26353");
        assert_eq!(attrs[0].version, "icon");
    }
}
