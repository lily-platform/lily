mod extension;
pub(crate) mod registration_plan;
mod resolution_trace;
pub(crate) mod scope_context;
pub(crate) mod scope_manager;
mod scope_trace;

pub use extension::*;
pub(crate) use scope_context::{
    ScopeContext, ScopeResolutionDrainHandoff, ScopeResolutionGuard, ScopedInstanceKind,
};
pub(crate) use scope_manager::{ScopeCleanupObservation, ScopeCleanupTicket, ScopeManager};
