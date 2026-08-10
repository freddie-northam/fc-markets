use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Typed identifiers. These exist to stop an asset identifier reaching a
/// parameter that wants a market identifier, which the compiler cannot catch
/// when every identifier is a bare Uuid.
macro_rules! typed_id {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
            sqlx::Type,
        )]
        #[sqlx(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            /// Version 7 so that identifiers sort by creation time. This keeps
            /// index writes local instead of scattering them.
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl From<Uuid> for $name {
            fn from(id: Uuid) -> Self {
                Self(id)
            }
        }
    };
}

typed_id!(GameId);
typed_id!(PlayerId);
typed_id!(AssetId);
typed_id!(MarketId);
typed_id!(RunId);

/// The platform an asset trades on. A closed list, because free text lets a
/// provider rename fork an asset's price history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Playstation,
    Pc,
}

impl Platform {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Playstation => "playstation",
            Self::Pc => "pc",
        }
    }

    /// Returns None for anything outside the list. A platform we cannot name is
    /// a platform we cannot key a market on, and guessing would fork a price
    /// history under a second name.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "playstation" => Some(Self::Playstation),
            "pc" => Some(Self::Pc),
            _ => None,
        }
    }
}

/// What happened when we asked about one asset. The observation table records
/// changes only, so without this a card we keep failing to read looks exactly
/// like a card whose price is stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollOutcome {
    Written,
    Unchanged,
    Rejected,
    NoPrice,
}

impl PollOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Written => "written",
            Self::Unchanged => "unchanged",
            Self::Rejected => "rejected",
            Self::NoPrice => "no_price",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Ok,
    Degraded,
    Failed,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
            Self::Failed => "failed",
        }
    }
}
