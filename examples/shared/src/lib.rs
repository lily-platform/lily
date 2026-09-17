//! Application services reused by HTTP, WebSocket and Consumer.
//! Each host resolves a fresh scoped service; no connection crosses a scope.
mod error;
mod jobs;
mod notes;
mod repository;
mod telemetry;
mod worker;

pub use error::{DemoError, validate_job, validate_text};
pub use jobs::{JobOperations, JobService};
pub use notes::NoteService;
pub use repository::JobRepository;
pub use telemetry::tracing_config;
pub use worker::SummaryWorker;
