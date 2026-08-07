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

/// Assigns a polling interval from columns we already hold. A tier is a
/// computable predicate, not an editorial judgement, so that discovery can
/// create poll state without a human deciding anything.
///
/// A promotional card or a high rating polls often because its price moves
/// often. Low rated base cards move slowly and would waste the request budget.
pub fn poll_interval_seconds(version: &str, rating: Option<i16>) -> i32 {
    let rating = rating.unwrap_or(0);
    if version != "base" || rating >= 88 {
        900
    } else if rating >= 83 {
        3_600
    } else {
        14_400
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
    observed_at >= game_released_at
        && observed_at <= run_started_at + chrono::Duration::minutes(5)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn promotional_cards_poll_fastest_whatever_their_rating() {
        assert_eq!(poll_interval_seconds("tots", Some(84)), 900);
        assert_eq!(poll_interval_seconds("icon", Some(70)), 900);
    }

    #[test]
    fn base_cards_tier_by_rating() {
        assert_eq!(poll_interval_seconds("base", Some(91)), 900);
        assert_eq!(poll_interval_seconds("base", Some(85)), 3_600);
        assert_eq!(poll_interval_seconds("base", Some(72)), 14_400);
        assert_eq!(poll_interval_seconds("base", None), 14_400);
    }

    #[test]
    fn every_asset_lands_in_exactly_one_tier() {
        for rating in 0..=99i16 {
            for version in ["base", "tots", "icon"] {
                let secs = poll_interval_seconds(version, Some(rating));
                assert!(matches!(secs, 900 | 3_600 | 14_400), "rating {rating}");
            }
        }
    }

    #[test]
    fn rejects_a_timestamp_from_before_the_game_existed() {
        let released = ts("2025-09-26T00:00:00Z");
        let run = ts("2026-08-07T12:00:00Z");
        assert!(!timestamp_in_range(ts("2024-01-01T00:00:00Z"), released, run));
    }

    #[test]
    fn rejects_a_timestamp_far_in_the_future() {
        let released = ts("2025-09-26T00:00:00Z");
        let run = ts("2026-08-07T12:00:00Z");
        assert!(!timestamp_in_range(ts("2087-01-01T00:00:00Z"), released, run));
    }

    #[test]
    fn accepts_a_small_clock_skew_ahead_of_the_run() {
        let released = ts("2025-09-26T00:00:00Z");
        let run = ts("2026-08-07T12:00:00Z");
        assert!(timestamp_in_range(ts("2026-08-07T12:03:00Z"), released, run));
        assert!(!timestamp_in_range(ts("2026-08-07T12:07:00Z"), released, run));
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
