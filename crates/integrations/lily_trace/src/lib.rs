#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! OpenTelemetry tracing, log-event, and metric export for Lily applications.
//!
//! `lily_trace` is an emission and runtime-ownership crate. It installs one
//! process-wide `tracing` subscriber, propagates W3C Trace Context, exports
//! telemetry, and reports whether shutdown drained successfully. Storage,
//! querying, dashboards, and trace-tree analysis belong to the selected
//! OpenTelemetry backend rather than this crate.
//!
//! Lily HTTP, WebSocket, and consumer builders normally own this lifecycle for
//! an application. Use [`TracingRuntimeOwner`] directly only in a custom
//! composition root.
//!
//! ## Quick Start
//!
//! ```no_run
//! use lily_trace::prelude::*;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Loading is strict: missing, malformed, unknown, or unsupported
//!     // configuration is returned as an error.
//!     let config = TraceConfig::try_load()?;
//!     let owner = match TracingRuntimeOwner::install(&config)? {
//!         TraceInstallOutcome::Disabled => None,
//!         TraceInstallOutcome::Owned(owner) => Some(owner),
//!     };
//!
//!     traced_operation().await;
//!
//!     if let Some(owner) = owner {
//!         let report = owner.shutdown(Duration::from_secs(10)).await;
//!         if !report.is_success() {
//!             return Err(format!("telemetry shutdown was incomplete: {report:?}").into());
//!         }
//!     }
//!     Ok(())
//! }
//!
//! #[lily_trace]
//! async fn traced_operation() {}
//! ```
//!
//! Use [`tracing`] spans and events for application instrumentation. Function
//! arguments are **not** recorded by [`lily_trace`](macro@lily_trace) unless explicitly named in
//! `fields(...)`; never record secrets, credentials, request bodies, or other
//! unbounded user input. [`spawn`] and its variants preserve the currently
//! entered span across a new Tokio task.
//!
//! [`TraceConfig::environment`] is an OpenTelemetry resource attribute. The
//! `env = ...` option of [`lily_trace`](macro@lily_trace) instead reads Lily's process-level
//! `LILY_ENV` classification.

#![doc = include_str!("../METHOD_TRACING.md")]

/// Span-preserving Tokio task helpers.
pub mod api;
/// Task-local component identity used by multi-component runtimes.
pub mod component;
/// W3C Trace Context propagation primitives.
pub mod core;
extern crate self as lily_trace;

mod instrumentation;
/// Shutdown-coordinator adapter for application composition roots.
pub mod lifecycle;
/// Canonical low-cardinality span names and telemetry safety checks.
pub mod observability;
/// Convenient imports for application instrumentation.
pub mod prelude;
/// Application-owned result classification for traced methods.
pub mod result;
/// Tracing configuration, runtime ownership, and shutdown evidence.
pub mod runtime;

pub use api::{spawn, spawn_blocking, spawn_local};
pub use component::{
    ComponentIdentity, current_component, record_component, record_component_identity,
    record_current_component, scope_component,
};
pub use core::{
    TraceError, W3CTraceContext, context_for_span, current_context, extract_context,
    inject_context, inject_current_context, install_w3c_propagator, set_parent,
};
pub use lifecycle::{TracingShutdownEvidence, TracingShutdownHandle};
pub use result::{TraceFailure, TraceResultError, record_result};
pub use runtime::{
    ExportConfig, ExportTaskShutdownStatus, FileExportConfig, FileExportInitError,
    FileExportMetricSnapshot, FileExportShutdownStatus, FileRotation, FilterRule,
    LogExportMetricSnapshot, LogExportReport, OtlpExportConfig, SamplingConfig, SamplingStrategy,
    SpanExportMetricSnapshot, SpanExportReport, SpanExportTaskShutdownStatus, TraceCellConfig,
    TraceConfig, TraceConfigLoadError, TraceInstallError, TraceInstallOutcome, TraceShutdownReport,
    TracingMode, TracingRuntimeOwner, TracingRuntimeStatus, tracing_runtime_status,
};

pub use lily_trace_macros::lily_trace;

/// The `tracing` crate version used by Lily.
///
/// Generated code and applications may use this re-export to avoid a second
/// direct dependency when they only need Lily-compatible spans and events.
pub use tracing;

/// Implementation support for generated code, including facade re-exports.
#[doc(hidden)]
pub mod __private {
    pub use crate::core::owned_context::TraceContextSnapshot;
    pub use crate::instrumentation::{LifecycleEvent, OperationGuard};
}

/// Generated-macro ABI for matching an explicit environment allow-list.
#[doc(hidden)]
pub fn environment_matches(allowed: &[&str]) -> bool {
    let current = lily_core::RuntimeEnvironment::current();
    allowed
        .iter()
        .filter_map(|value| lily_core::RuntimeEnvironment::parse(value).ok())
        .any(|environment| environment == current)
}
