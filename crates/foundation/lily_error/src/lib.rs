//! Shared, typed error contracts used across Lily framework crates.
//!
//! Application code normally receives these errors from the facade that owns
//! the operation: `lily_http_api::HttpApiError`,
//! `lily_injection::InjectionError`, `lily_config::ConfigError`,
//! `lily_mongodb::MongoDbError`, or the consumer facade. A direct `lily_error`
//! dependency is primarily required by framework extension code—or a Lily
//! queue handler—whose trait signature explicitly names one of these shared
//! contracts.
//!
//! Diagnostic strings carried by errors are process-internal. HTTP responses
//! must be produced through [`application::http_api::HttpApiError::public_body`]
//! or `to_public_json`, which deliberately redact those diagnostics.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

/// Errors shared by application-facing Lily components.
pub mod application;
/// Configuration loading and validation errors owned by `lily_config`.
pub mod config;
/// Dependency-injection and lifecycle errors owned by `lily_injection`.
pub mod injection;
/// Immutable localization catalog and startup loading errors.
pub mod localization;

pub use localization::{LocalizationCatalog, LocalizationError, LocalizationManager};
