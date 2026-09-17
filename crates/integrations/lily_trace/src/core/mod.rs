//! W3C Trace Context extraction, injection, and explicit context values.

pub(crate) mod owned_context;
mod propagation;

pub use propagation::{
    context_for_span, current_context, extract_context, inject_context, inject_current_context,
    install_w3c_propagator, set_parent, TraceError, W3CTraceContext,
};
