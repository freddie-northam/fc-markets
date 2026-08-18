//! The contract every provider module implements.
//!
//! The orchestration in `ingest` never sees a provider struct. It fetches an
//! envelope, archives it, then hands it back to the same module to parse. That
//! order is what makes replay possible, and the split is what stops a provider
//! shape from leaking into the rest of the program.

use crate::archive::Envelope;
use crate::domain::AssetAttributes;
use crate::ids::Platform;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// One quote after conversion. The caller resolves identity and applies the
/// validation rules; a source module only reshapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedQuote {
    pub external_id: String,
    pub platform: Platform,
    pub price: Option<i64>,
    pub min_price: Option<i64>,
    pub max_price: Option<i64>,
    pub observed_at: Option<DateTime<Utc>>,
    /// Rule 1. When the payload carries a name we compare it against the
    /// resolved asset, which catches a provider that re-points an existing
    /// identifier at a different card. We never compare the rating: cards raise
    /// their rating in season by design.
    pub name: Option<String>,
}

/// A reference list the provider publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceKind {
    League,
    Nation,
    Club,
}

impl ReferenceKind {
    /// The stored value. Matches the CHECK on `reference_entities.kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::League => "league",
            Self::Nation => "nation",
            Self::Club => "club",
        }
    }
}

/// One row of a reference list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceEntity {
    pub external_id: i32,
    pub name: String,
    /// What it belongs to: a club's league, a league's nation. None for a
    /// nation, and None when the provider omits it.
    pub parent_id: Option<i32>,
}

/// One entry of the provider's asset list. Discovery needs nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    pub external_id: String,
    pub name: String,
}

/// A provider refused the run rather than one record. Section 4.5 makes this
/// stop the run and mark it degraded, because retrying inside the same run
/// spends a quota the provider has already told us is exhausted.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("the provider rate limited this run")]
    RateLimited,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type FetchResult = Result<Envelope, FetchError>;

#[async_trait]
pub trait Source: Send + Sync {
    /// Recorded on every observation, so a row always states who supplied it.
    fn name(&self) -> &'static str;

    /// Recorded on every run. Replay cannot be reasoned about without knowing
    /// which parser wrote a row.
    fn parser_version(&self) -> &'static str;

    /// Rule 1. This MUST return 1 unless the price response carries a player
    /// identifier for every quote. A response that can omit, reorder or
    /// de-duplicate players attaches every later price to the wrong asset when
    /// it is mapped by position, and nothing raises an error.
    fn price_batch_size(&self) -> usize;

    fn metadata_batch_size(&self) -> usize;

    async fn fetch_prices(&self, external_ids: &[String]) -> FetchResult;

    /// Takes the whole envelope, not the body. A source whose response carries
    /// no identifier attributes its single quote from `requested_ids`, which is
    /// the reason the envelope stores them.
    fn parse_prices(&self, envelope: &Envelope) -> anyhow::Result<Vec<ParsedQuote>>;

    /// Fetches ONE page of the asset list. Pages start at 1. The caller charges
    /// the budget and archives each response, so a module that walked its own
    /// pages would spend a quota the budget counted once, and would collapse
    /// many responses into one object that replay cannot reproduce.
    async fn fetch_asset_list(&self, page: u32) -> FetchResult;

    fn parse_asset_list(&self, envelope: &Envelope) -> anyhow::Result<Vec<Listing>>;

    /// Whether the provider can be asked for only what it has added.
    ///
    /// A provider without such an endpoint keeps the default, and the caller
    /// falls back to walking the whole catalogue.
    fn supports_incremental_discovery(&self) -> bool {
        false
    }

    /// One page of the assets the provider added AFTER this identifier.
    ///
    /// The response has the same shape as the asset list, so `parse_asset_list`,
    /// `parse_asset_list_attributes` and `next_page` all serve it unchanged.
    async fn fetch_assets_after(&self, _after: &str, _page: u32) -> FetchResult {
        Err(FetchError::Other(anyhow::anyhow!(
            "this provider cannot be asked for new assets alone"
        )))
    }

    /// The next page, read from the response just received. `None` ends the
    /// walk, so a provider that answers whole needs no implementation.
    ///
    /// Shared by every paginated walk, because how a provider paginates is a
    /// property of the provider and not of the list being walked.
    fn next_page(&self, _envelope: &Envelope) -> Option<u32> {
        None
    }

    /// Fetches one page of a reference list. A provider that publishes none
    /// keeps the default and the step does nothing.
    ///
    /// Reference lists name what the identifiers on an asset mean. Without them
    /// a card knows it belongs to league 13 and cannot say Premier League.
    async fn fetch_reference(&self, _kind: ReferenceKind, _page: u32) -> FetchResult {
        Err(FetchError::Other(anyhow::anyhow!(
            "this provider publishes no reference lists"
        )))
    }

    fn parse_reference(
        &self,
        _kind: ReferenceKind,
        _envelope: &Envelope,
    ) -> anyhow::Result<Vec<ReferenceEntity>> {
        Ok(Vec::new())
    }

    /// Which reference lists this provider publishes. Empty by default, so the
    /// step is skipped rather than special cased.
    fn reference_kinds(&self) -> &'static [ReferenceKind] {
        &[]
    }

    /// Attributes the asset list already carries. A provider that answers the
    /// list with whole records owes nothing more for them, because the caller
    /// has already fetched, paid for and archived the page. The caller applies
    /// the same identity check here as it applies to the metadata step, so this
    /// is not a way around rule 1.
    fn parse_asset_list_attributes(
        &self,
        _envelope: &Envelope,
    ) -> anyhow::Result<Vec<AssetAttributes>> {
        Ok(Vec::new())
    }

    async fn fetch_metadata(&self, external_ids: &[String]) -> FetchResult;

    fn parse_metadata(&self, envelope: &Envelope) -> anyhow::Result<Vec<AssetAttributes>>;
}
