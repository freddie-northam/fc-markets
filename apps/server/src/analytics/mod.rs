//! Everything the ledger derives, as opposed to everything it records.
//!
//! One rule holds this layer together, and it is worth more than the folder:
//!
//! **Nothing here may call `now()`. Every derivation takes `as_of`.**
//!
//! That makes each calculation a pure function of the ledger and a time, which
//! buys three things. It is testable without freezing a clock. It composes
//! without ordering hazards. And it is SCOREABLE: we can ask what the system
//! said last Tuesday and get the same answer for ever, which is the only honest
//! way to mark a prediction we made.
//!
//! The rule needs enforcing rather than intending. It is easy to break by
//! reaching for `now()` in one SQL fragment, and the result still looks correct
//! because it answers confidently. `trusted_latest_prices(as_of)` is the shared
//! foundation and reads only append-only tables for the same reason: the poll
//! state it used to read is overwritten on every poll, so it could describe the
//! present and never the past.

pub mod cohort;
pub mod valuation;
