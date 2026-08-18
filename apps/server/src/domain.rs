use crate::ids::{AssetId, MarketId, PollOutcome, RunId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

/// One canonical price observation. Every source converts into this shape, and
/// nothing downstream can tell which provider supplied it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub asset_id: AssetId,
    pub market_id: MarketId,
    pub source: &'static str,
    /// When the market showed this price, taken from the provider. Never our
    /// clock: a substituted clock turns an unknown time into a false one.
    pub observed_at: DateTime<Utc>,
    pub price: i64,
    pub min_price: Option<i64>,
    pub max_price: Option<i64>,
    pub source_ref: Option<String>,
    pub ingest_run_id: RunId,
}

/// What one asked-about asset produced. The database decides `written` against
/// `unchanged`, because only the idempotency index knows whether the price moved,
/// so a passing record carries its observation rather than a verdict.
#[derive(Debug, Clone)]
pub enum PollResult {
    Priced(Observation),
    Failed(PollOutcome),
}

/// One asset we asked about, and what came back. Section 4.3: without this
/// record the ledger cannot separate a stable price from an outage.
#[derive(Debug, Clone)]
pub struct Poll {
    pub asset_id: AssetId,
    pub market_id: MarketId,
    /// Null exactly when the provider returned no timestamp, which is the case
    /// that proves we looked and found nothing.
    pub source_observed_at: Option<DateTime<Utc>>,
    pub result: PollResult,
}

/// Card attributes. Every valuation input in the class median query reads these,
/// so a source that supplies none leaves the terminal with nothing to compute.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssetAttributes {
    pub name: String,
    pub external_id: String,
    pub ea_base_id: Option<i64>,
    pub rating: Option<i16>,
    pub rarity: Rarity,
    pub version: String,
    pub position: Option<String>,
    pub league_id: Option<i32>,
    pub nation_id: Option<i32>,
    pub club_id: Option<i32>,
    pub skill_moves: Option<i16>,
    pub weak_foot: Option<i16>,
    pub pace: Option<i16>,
    pub shooting: Option<i16>,
    pub passing: Option<i16>,
    pub dribbling: Option<i16>,
    pub defending: Option<i16>,
    pub physicality: Option<i16>,
    pub playstyle_count: Option<i16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Rarity {
    Common,
    #[default]
    Rare,
}

impl Rarity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Common => "common",
            Self::Rare => "rare",
        }
    }
}

/// The version every card has until a promotion gives it another.
pub const BASE_VERSION: &str = "base";

/// Folds a provider's version string into the canonical form.
///
/// Section 4.2 makes version a closed domain we own. The promotional list is
/// open ended, so this constrains the shape rather than the values, and the
/// database enforces the same shape.
///
/// It matters because the comparison is exact in three places: the polling tier,
/// the coverage ranking and the valuation class key. A provider sending "Base"
/// or " TOTS " instead of "base" and "tots" would silently re-tier and re-class
/// every card it touched.
pub fn canonical_version(raw: Option<String>) -> String {
    let folded = raw
        .unwrap_or_default()
        .trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("_");

    if folded.is_empty() {
        BASE_VERSION.to_string()
    } else {
        folded
    }
}

/// The fastest interval worth asking for.
///
/// Measured on 2026-08-17: the provider refreshes prices in one nightly window
/// of roughly 05:00 to 07:00 UTC. Eleven of the most valuable Icons in the game
/// all carried stamps inside a fifteen minute span and none had moved in the
/// eleven hours after. `priceUpdate` marks when the price CHANGED, not when the
/// provider last looked, so an illiquid card can sit unchanged for days.
///
/// Polling faster than this therefore buys nothing. It re-reads a number that
/// cannot have moved, and spends a quota that could have covered another card.
pub const UPSTREAM_REFRESH_SECONDS: i32 = 86_400;

/// The slowest band. Stated once, because two other things must agree with it.
pub const SLOWEST_POLL_INTERVAL_SECONDS: i32 = 3 * UPSTREAM_REFRESH_SECONDS;

/// How old a price may be before a derivation stops trusting it.
///
/// It MUST exceed the slowest polling band, and the margin is the point. An
/// asset becomes due at the end of its band, and the budget decides when a due
/// asset is actually read, so an overdue asset is normal. If the trust window
/// equalled the band, every one of the 19,798 assets in the slow tier would drop
/// out of the class median and the cohorts while waiting for a poll that was
/// only just late. Overdue is not the same as unknown.
///
/// Twice the slowest band gives a full extra cycle of slack.
pub const TRUSTED_PRICE_MAX_AGE_SECONDS: i32 = 2 * SLOWEST_POLL_INTERVAL_SECONDS;

// Checked when the crate compiles rather than when a test runs, because this is
// a fact about two constants and a build that violates it should not exist. The
// margin must be a full cycle: an asset falls due at the end of its band and the
// budget decides when a due asset is actually read, so overdue is normal.
const _: () = assert!(
    TRUSTED_PRICE_MAX_AGE_SECONDS - SLOWEST_POLL_INTERVAL_SECONDS
        >= SLOWEST_POLL_INTERVAL_SECONDS,
    "the trust window must exceed the slowest polling band by a full cycle"
);

/// Assigns a polling interval from columns we already hold. A tier is a
/// computable predicate, not an editorial judgement, so that discovery can
/// create poll state without a human deciding anything.
///
/// With a daily upstream there is only one question left, and it is coverage
/// rather than frequency: which cards can the budget afford EACH DAY. Against a
/// measured catalogue of 27,121 and a budget of 18,000, where one request prices
/// both platforms:
///
/// | band            | assets | interval | requests/day |
/// |-----------------|--------|----------|--------------|
/// | 75+ or promo    |  7,278 | 24h      |        7,278 |
/// | below 75        | 19,798 | 72h      |        6,599 |
/// | discovery walk  |      - | daily    |        1,357 |
///
/// That is about 15,200, leaving headroom for retries and failure backoff. The
/// tail is the compromise: those cards mostly sit at their floor and rarely
/// trade, so they lose resolution before anything else does.
///
/// An asset with no rating yet falls to the slower band, so a catalogue that
/// arrives before its attributes cannot drain the budget.
pub fn poll_interval_seconds(version: &str, rating: Option<i16>) -> i32 {
    // A promotion marks the traded end of the market, so it earns a daily read
    // whatever its rating. Every promotional card measured was rated 80 or
    // above, so today this changes nothing; it is here so that a low rated
    // promotion does not quietly land in the tail.
    if version != BASE_VERSION || rating.unwrap_or(0) >= 75 {
        UPSTREAM_REFRESH_SECONDS
    } else {
        SLOWEST_POLL_INTERVAL_SECONDS
    }
}

/// Rejection reasons, kept as data so a run can count them and a human can see
/// which rule fired without reading the log line by line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    /// Rule 3. The provider gave no usable timestamp.
    MissingTimestamp,
    /// Rule 2. Outside the game's lifetime, or implausibly far ahead.
    TimestampOutOfRange,
    /// Rule 1. The payload describes a different card than the identifier maps to.
    IdentityMismatch,
    /// The provider returned no price, or a nonsense one.
    NoPrice,
    UnknownAsset,
}

impl Rejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MissingTimestamp => "missing_timestamp",
            Self::TimestampOutOfRange => "timestamp_out_of_range",
            Self::IdentityMismatch => "identity_mismatch",
            Self::NoPrice => "no_price",
            Self::UnknownAsset => "unknown_asset",
        }
    }
}

/// Rule 1. Compares a payload name against the resolved asset.
///
/// This catches a provider that re-points an existing identifier at a different
/// card, which would otherwise attach one card's prices to another card's history
/// with nothing raising an error.
///
/// The comparison folds case, accents, punctuation and repeated spaces. A
/// provider writing "Mbappe" where we hold "Mbappé" is the same footballer, and
/// rejecting on that would reject every accented name on every poll.
///
/// We deliberately do NOT compare the rating. Ones to Watch, Path to Glory and
/// Road to the Knockouts cards raise their rating in season by design, and EA
/// also refreshes ratings mid season, so a rating check would reject exactly the
/// high value cards until the metadata step caught up.
pub fn names_match(payload: &str, resolved: &str) -> bool {
    fold_name(payload) == fold_name(resolved)
}

/// Rule 1, with the one exception the rule needs to stay useful: an asset we
/// hold under no name has no identity to defend.
///
/// A provider lists an unreleased card with an empty name and fills the name in
/// on release. Reading that as a re-point rejects the first real name the card
/// ever has, and keeps rejecting it, because discovery retries every cadence.
/// The asset then never gets attributes, sits in the slowest tier and the base
/// valuation class for ever, and every run degrades with a re-point warning. The
/// warning is how a REAL re-point would be noticed, so the false ones do not
/// merely add noise, they hide the fault the rule exists to catch.
///
/// The exception is one directional. An empty payload name against a name we
/// hold is still a mismatch: that erases identity rather than establishing it.
pub fn name_is_consistent(payload: &str, resolved: &str) -> bool {
    if fold_name(resolved).is_empty() {
        return true;
    }
    names_match(payload, resolved)
}

fn fold_name(s: &str) -> String {
    let flattened: String = s
        .nfd()
        .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
        // An apostrophe is elided rather than turned into a separator, because
        // providers disagree on it within one word: "O'Neill" and "ONeill" are
        // the same footballer. A hyphen or comma does separate, because
        // "Jean-Pierre" and "Jean Pierre" are also the same footballer.
        .filter(|c| !matches!(c, '\'' | '\u{2019}' | '\u{02BC}'))
        .flat_map(char::to_lowercase)
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect();
    flattened.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Rule 2. A single far future timestamp makes TimescaleDB create a chunk at
/// that date, and that chunk then sits between us and every range query for the
/// life of the database.
///
/// The upper bound is the run's own start, not the wall clock. A wall clock
/// bound would accept on replay what it rejected at ingest time, so replaying
/// one archive twice would give two different tables.
pub fn timestamp_in_range(
    observed_at: DateTime<Utc>,
    game_released_at: DateTime<Utc>,
    run_started_at: DateTime<Utc>,
) -> bool {
    observed_at >= game_released_at && observed_at <= run_started_at + chrono::Duration::minutes(5)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn a_version_folds_to_one_canonical_form() {
        assert_eq!(canonical_version(Some("  TOTS ".into())), "tots");
        assert_eq!(
            canonical_version(Some("Winter Wildcards".into())),
            "winter_wildcards"
        );
        assert_eq!(canonical_version(Some("Base".into())), BASE_VERSION);
        assert_eq!(canonical_version(None), BASE_VERSION);
        assert_eq!(canonical_version(Some("   ".into())), BASE_VERSION);
    }

    /// The whole point: a provider changing its casing must not re-tier a card.
    #[test]
    fn casing_from_a_provider_cannot_change_the_tier() {
        let folded = canonical_version(Some("BASE".into()));
        assert_eq!(poll_interval_seconds(&folded, Some(70)), 259_200);
    }

    /// Every promotional card measured was rated 80 or above, so this changes
    /// nothing today. It is asserted so that a low rated promotion cannot
    /// quietly land in the tail when one appears.
    #[test]
    fn a_promotion_earns_a_daily_read_whatever_its_rating() {
        assert_eq!(poll_interval_seconds("tots", Some(70)), 86_400);
        assert_eq!(poll_interval_seconds("icon", Some(50)), 86_400);
    }

    #[test]
    fn base_cards_split_at_seventy_five() {
        assert_eq!(poll_interval_seconds("base", Some(91)), 86_400);
        assert_eq!(poll_interval_seconds("base", Some(75)), 86_400);
        assert_eq!(poll_interval_seconds("base", Some(74)), 259_200);
        assert_eq!(poll_interval_seconds("base", Some(50)), 259_200);
    }

    /// Nothing polls faster than the provider refreshes. A shorter interval
    /// re-reads a number that cannot have moved and spends a request that could
    /// have covered another card.
    #[test]
    fn no_asset_polls_faster_than_the_provider_refreshes() {
        for rating in 0..=99i16 {
            for version in ["base", "tots", "icon", "team_of_the_season"] {
                assert!(
                    poll_interval_seconds(version, Some(rating)) >= UPSTREAM_REFRESH_SECONDS,
                    "rating {rating} version {version} outruns the feed"
                );
            }
        }
    }

    /// An asset discovered before its attributes must not take the daily band,
    /// or a fresh catalogue would drain the budget on cards we cannot yet value.
    #[test]
    fn an_asset_without_a_rating_takes_the_slowest_band() {
        assert_eq!(poll_interval_seconds("base", None), 259_200);
    }

    #[test]
    fn every_asset_lands_in_exactly_one_tier() {
        for rating in 0..=99i16 {
            for version in ["base", "tots", "icon"] {
                let secs = poll_interval_seconds(version, Some(rating));
                assert!(matches!(secs, 86_400 | 259_200), "rating {rating}");
            }
        }
    }

    #[test]
    fn every_band_is_inside_the_trust_window() {
        for rating in 0..=99i16 {
            for version in ["base", "tots", "icon"] {
                assert!(
                    poll_interval_seconds(version, Some(rating)) < TRUSTED_PRICE_MAX_AGE_SECONDS,
                    "rating {rating} is polled less often than it is trusted"
                );
            }
        }
    }

    /// The bands must fit the budget, discovery included. A change that
    /// overspends fails here rather than in production a day later.
    #[test]
    fn the_measured_catalogue_fits_the_daily_budget() {
        // Measured on 2026-08-17 across the full 27,121 asset catalogue:
        // (count, rating, version).
        let catalogue = [
            (1_193, 91, "icon"),
            (3_146, 87, "tots"),
            (705, 84, "tots"),
            (217, 81, "tots"),
            (1_193, 81, "base"),
            (868, 77, "base"),
            (19_798, 65, "base"),
        ];
        let prices: f64 = catalogue
            .iter()
            .map(|(count, rating, version)| {
                let interval = poll_interval_seconds(version, Some(*rating)) as f64;
                *count as f64 * 86_400.0 / interval
            })
            .sum();
        // The asset list walk spends the same budget: 27,121 assets at 20 to a
        // page, once a day. Leaving it out is how a tier change looks affordable
        // and then starves discovery.
        let discovery = (27_121.0f64 / 20.0).ceil();
        let daily = prices + discovery;
        assert!(
            daily <= 18_000.0,
            "prices {prices:.0} plus discovery {discovery:.0} is {daily:.0} a day"
        );
    }

    #[test]
    fn rejects_a_timestamp_from_before_the_game_existed() {
        let released = ts("2025-09-26T00:00:00Z");
        let run = ts("2026-08-07T12:00:00Z");
        assert!(!timestamp_in_range(
            ts("2024-01-01T00:00:00Z"),
            released,
            run
        ));
    }

    #[test]
    fn rejects_a_timestamp_far_in_the_future() {
        let released = ts("2025-09-26T00:00:00Z");
        let run = ts("2026-08-07T12:00:00Z");
        assert!(!timestamp_in_range(
            ts("2087-01-01T00:00:00Z"),
            released,
            run
        ));
    }

    #[test]
    fn accepts_a_small_clock_skew_ahead_of_the_run() {
        let released = ts("2025-09-26T00:00:00Z");
        let run = ts("2026-08-07T12:00:00Z");
        assert!(timestamp_in_range(
            ts("2026-08-07T12:03:00Z"),
            released,
            run
        ));
        assert!(!timestamp_in_range(
            ts("2026-08-07T12:07:00Z"),
            released,
            run
        ));
    }

    #[test]
    fn a_name_that_differs_only_by_accent_or_case_still_matches() {
        assert!(names_match("Kylian Mbappé", "Kylian Mbappe"));
        assert!(names_match("kylian  mbappe", "Kylian Mbappe"));
        assert!(names_match("O'Neill, Martin", "ONeill Martin"));
        assert!(names_match("O\u{2019}Neill", "O'Neill"));
        assert!(names_match("Jean-Pierre Papin", "Jean Pierre Papin"));
    }

    /// The check exists to catch a provider that re-points an identifier at a
    /// different card, so a genuinely different footballer must fail it.
    #[test]
    fn a_different_footballer_fails_the_identity_check() {
        assert!(!names_match("Kylian Mbappe", "Erling Haaland"));
        assert!(!names_match("Lionel Messi", "Lionel Scaloni"));
    }

    /// The provider lists an unreleased card with no name and fills it in on
    /// release. That transition establishes identity rather than changing it, so
    /// it must be adopted. Rejecting it strands the asset without attributes for
    /// ever, because discovery retries the same comparison every cadence.
    #[test]
    fn a_first_real_name_is_adopted_over_a_name_we_never_had() {
        assert!(name_is_consistent("Kylian Mbappe", ""));
        assert!(name_is_consistent("Kylian Mbappe", "   "));
    }

    /// One directional. Blanking a name we hold erases identity, so it stays a
    /// mismatch, and so does a genuine re-point.
    #[test]
    fn the_empty_name_exception_does_not_work_in_reverse() {
        assert!(!name_is_consistent("", "Kylian Mbappe"));
        assert!(!name_is_consistent("   ", "Kylian Mbappe"));
        assert!(!name_is_consistent("Erling Haaland", "Kylian Mbappe"));
    }

    /// The exception must not weaken the rule where a name exists on both sides,
    /// which is every case the rule was written for.
    #[test]
    fn the_exception_leaves_the_ordinary_comparison_alone() {
        assert!(name_is_consistent("Kylian Mbappé", "Kylian Mbappe"));
        assert!(!name_is_consistent("Lionel Messi", "Lionel Scaloni"));
    }

    /// The bound must follow the run, not the wall clock, or the same archive
    /// would produce a different table depending on when it is replayed.
    #[test]
    fn the_upper_bound_follows_the_run_not_the_wall_clock() {
        let released = ts("2025-09-26T00:00:00Z");
        let observed = ts("2026-08-07T12:30:00Z");
        let original_run = ts("2026-08-07T12:00:00Z");
        let later_replay = ts("2027-01-01T00:00:00Z");

        assert!(!timestamp_in_range(observed, released, original_run));
        // Replaying with the ORIGINAL run start keeps the original verdict.
        assert!(!timestamp_in_range(observed, released, original_run));
        // Using the replay's own clock would wrongly accept it.
        assert!(timestamp_in_range(observed, released, later_replay));
    }
}
