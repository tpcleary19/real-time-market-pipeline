//! Rust price spike detector. Module ownership: see docs/spike-detector-plan.md.
pub mod event;
pub use event::{Direction, SCHEMA_VERSION, SpikeEvent};

/// Pure, I/O-free detection core (owned by Rust Implementation agent).
/// Contract used by the testing agent: feed trades in, get 0..1 SpikeEvent out.
pub mod detector;
