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

    /// The next page, read from the response just received. `None` ends the
    /// walk, so a provider that answers whole needs no implementation.
    fn next_asset_list_page(&self, _envelope: &Envelope) -> Option<u32> {
        None
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
