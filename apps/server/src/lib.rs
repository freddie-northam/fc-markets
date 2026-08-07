//! FC Market: a canonical, replayable ledger of Ultimate Team market prices.
//!
//! The crate is a library with a thin binary on top so that the database tests
//! can drive the same code the `ingest` command drives. A test that exercises a
//! copy of the pipeline proves nothing about the pipeline.

pub mod api;
pub mod archive;
pub mod backup;
pub mod config;
pub mod db;
pub mod domain;
pub mod heartbeat;
pub mod ids;
pub mod ingest;
pub mod source;
pub mod valuation;
