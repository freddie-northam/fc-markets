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

/// Assigns a polling interval from columns we already hold. A tier is a
/// computable predicate, not an editorial judgement, so that discovery can
/// create poll state without a human deciding anything.
///
/// The bands come from a measured catalogue of 27,121 assets against a budget
/// of 18,000 requests a day, where one request prices both platforms. They spend
/// about 16,400 a day and leave the rest for discovery and retries. A faster top
/// band does not fit: 1,193 assets at 4 hours already take 7,158 of it.
///
/// An asset with no rating yet falls to the slowest band, so a catalogue that
/// arrives before its attributes cannot drain the budget.
pub fn poll_interval_seconds(version: &str, rating: Option<i16>) -> i32 {
    let by_rating = match rating.unwrap_or(0) {
        r if r >= 90 => 14_400,
        r if r >= 86 => 43_200,
        r if r >= 83 => 86_400,
        r if r >= 80 => 172_800,
        r if r >= 75 => 604_800,
        _ => 1_209_600,
    };
    // A promotional card is the traded end of the market, so it never falls to
    // the slow bands whatever its rating.
    if version == BASE_VERSION {
        by_rating
    } else {
        by_rating.min(86_400)
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
        assert_eq!(poll_interval_seconds(&folded, Some(70)), 1_209_600);
    }

    #[test]
    fn promotional_cards_never_fall_to_the_slow_bands() {
        assert_eq!(poll_interval_seconds("tots", Some(70)), 86_400);
        assert_eq!(poll_interval_seconds("icon", Some(81)), 86_400);
        // A promotion does not slow a card that its rating already polls faster.
        assert_eq!(poll_interval_seconds("tots", Some(91)), 14_400);
    }

    #[test]
    fn base_cards_tier_by_rating() {
        assert_eq!(poll_interval_seconds("base", Some(91)), 14_400);
        assert_eq!(poll_interval_seconds("base", Some(87)), 43_200);
        assert_eq!(poll_interval_seconds("base", Some(85)), 86_400);
        assert_eq!(poll_interval_seconds("base", Some(81)), 172_800);
        assert_eq!(poll_interval_seconds("base", Some(77)), 604_800);
        assert_eq!(poll_interval_seconds("base", Some(72)), 1_209_600);
    }

    /// An asset discovered before its attributes must not take a fast band, or a
    /// fresh catalogue would drain the budget on cards we cannot yet value.
    #[test]
    fn an_asset_without_a_rating_takes_the_slowest_band() {
        assert_eq!(poll_interval_seconds("base", None), 1_209_600);
    }

    #[test]
    fn every_asset_lands_in_exactly_one_tier() {
        for rating in 0..=99i16 {
            for version in ["base", "tots", "icon"] {
                let secs = poll_interval_seconds(version, Some(rating));
                assert!(
                    matches!(
                        secs,
                        14_400 | 43_200 | 86_400 | 172_800 | 604_800 | 1_209_600
                    ),
                    "rating {rating}"
                );
            }
        }
    }

    /// The bands must fit the budget. A change that spends more than a day's
    /// requests fails here rather than in production a day later.
    #[test]
    fn the_measured_catalogue_fits_the_daily_budget() {
        // Measured on 2026-08-17 from a 500-player sample of 27,121 assets:
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
        let daily: f64 = catalogue
            .iter()
            .map(|(count, rating, version)| {
                let interval = poll_interval_seconds(version, Some(*rating)) as f64;
                *count as f64 * 86_400.0 / interval
            })
            .sum();
        assert!(
            daily <= 18_000.0,
            "the tiers ask for {daily:.0} requests a day"
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
