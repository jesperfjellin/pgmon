// Internal modules required when compiled as a library for tests.
pub mod app;
pub mod config;
pub mod db;
pub mod http;
pub mod metrics;
pub mod persistence;
pub mod poller;
pub mod recommendations;
pub mod state;
// Re-export commonly used types for tests
pub use state::{OverviewSnapshot, SharedState, WorkloadSample, WorkloadSummary};
