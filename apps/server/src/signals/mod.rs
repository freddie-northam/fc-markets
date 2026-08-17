//! Everything this server says to the outside world.
//!
//! Two signals, deliberately opposite, because they fail in opposite ways.
//!
//! - [`heartbeat`] makes SILENCE the alarm. It pings only when a run closes
//!   healthy, so one mechanism covers a dead host, a dead process and a dead
//!   feed. It needs a third party to notice the silence.
//! - [`notify`] makes a MESSAGE the alarm. It posts to a chat channel, so a
//!   human notices instead of a service. It cannot report its own death.
//!
//! Neither is part of the ledger. Both share [`outbound`], which holds the one
//! rule they must not get wrong: a third party's failure never changes a run.

pub mod heartbeat;
pub mod notify;
mod outbound;
