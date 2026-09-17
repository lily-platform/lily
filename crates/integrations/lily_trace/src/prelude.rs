//! Common instrumentation imports for Lily applications.
//!
//! ```ignore
//! use lily_trace::prelude::*;
//! ```

// Core types
pub use crate::component::{
    current_component, record_component, record_component_identity, record_current_component,
    scope_component, ComponentIdentity,
};
pub use crate::core::{
    context_for_span, current_context, extract_context, inject_context, inject_current_context,
    install_w3c_propagator, set_parent, TraceError, W3CTraceContext,
};

// Runtime
pub use crate::runtime::{
    tracing_runtime_status, ExportConfig, ExportTaskShutdownStatus, FileExportConfig,
    FileExportInitError, FileExportMetricSnapshot, FileExportShutdownStatus, FileRotation,
    FilterRule, LogExportMetricSnapshot, LogExportReport, OtlpExportConfig, SamplingConfig,
    SamplingStrategy, SpanExportMetricSnapshot, SpanExportReport, SpanExportTaskShutdownStatus,
    TraceCellConfig, TraceConfig, TraceConfigLoadError, TraceInstallError, TraceInstallOutcome,
    TraceShutdownReport, TracingMode, TracingRuntimeOwner, TracingRuntimeStatus,
};

// API
pub use crate::api::{spawn, spawn_blocking, spawn_local};
pub use crate::observability;
pub use crate::result::{record_result, TraceFailure, TraceResultError};

// Re-export macro
pub use lily_trace_macros::lily_trace;

// Re-export tracing macros and traits
pub use tracing::Instrument;
pub use tracing::{debug, error, info, instrument, span, trace, warn};
