//! Resolution diagnostics never format service instances, contexts, or error messages.

use std::future::Future;

use lily_error::injection::InjectionError;
use lily_injection_registry::ServiceLifetime;
use lily_trace::{
    __private::{LifecycleEvent, OperationGuard},
    TraceFailure, TraceResultError,
};
use tracing::{Instrument, Span};

pub(super) struct ResolutionMetadata {
    pub requested: &'static str,
    pub implementation: Option<&'static str>,
    pub lifetime: Option<ServiceLifetime>,
}

impl ResolutionMetadata {
    fn lifetime_name(&self) -> Option<&'static str> {
        self.lifetime.map(|lifetime| match lifetime {
            ServiceLifetime::Singleton => "singleton",
            ServiceLifetime::Scoped => "scoped",
            ServiceLifetime::Transient => "transient",
        })
    }

    fn failure(&self, code: &'static str) {
        // A filtered DEBUG span must not hide failures or detach them from the
        // caller's request span. This event is emitted in either filtering path.
        tracing::event!(
            name: "di.service.resolve.failed",
            tracing::Level::ERROR,
            lily.operation = "di.service.resolve",
            lily.outcome = "error",
            lily.error_code = code,
            di.requested_type = self.requested,
            di.implementation_type = self.implementation,
            di.lifetime = self.lifetime_name(),
            "DI service resolution failed"
        );
    }
}

pub(super) async fn observe<T>(
    metadata: ResolutionMetadata,
    operation: impl Future<Output = Result<T, InjectionError>>,
) -> Result<T, InjectionError> {
    let span = tracing::debug_span!(
        "di.service.resolve",
        di.requested_type = metadata.requested,
        di.implementation_type = metadata.implementation,
        di.lifetime = metadata.lifetime_name(),
        lily.instrumentation = "method",
        lily.lifecycle = tracing::field::Empty,
        lily.duration_ms = tracing::field::Empty,
        lily.outcome = tracing::field::Empty,
        lily.error_code = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    if span.is_disabled() {
        let result = operation.await;
        if let Err(error) = &result {
            metadata.failure(error_code(error));
        }
        return result;
    }

    let operation_span = span.clone();
    async move {
        let guard = OperationGuard::start(operation_span, emit);
        let result = operation.await;
        let classification = result.as_ref().map(|_| ()).map_err(|error| {
            let code = error_code(error);
            metadata.failure(code);
            ResolutionFailure(code)
        });
        guard.finish_result(&classification);
        result
    }
    .instrument(span)
    .await
}

fn emit(span: &Span, event: LifecycleEvent) {
    if event.phase == "started" {
        tracing::event!(
            name: "lily.method.started", parent: span,
            tracing::Level::DEBUG,
            lily.operation = "di.service.resolve",
            lily.lifecycle = "started",
            "method started"
        );
    } else {
        tracing::event!(
            name: "lily.method.finished", parent: span,
            tracing::Level::DEBUG,
            lily.operation = "di.service.resolve",
            lily.lifecycle = event.phase,
            lily.duration_ms = event.duration_ms,
            lily.outcome = event.outcome,
            lily.error_code = event.error_code,
            "method finished"
        );
    }
}

struct ResolutionFailure(&'static str);
impl TraceResultError for ResolutionFailure {
    fn trace_failure(&self) -> TraceFailure {
        TraceFailure::Error { code: self.0 }
    }
}

fn error_code(error: &InjectionError) -> &'static str {
    // Match variants, never their user-controlled diagnostic payloads. Keeping
    // this exhaustive makes new error variants require an explicit safe code.
    match error {
        InjectionError::InitError(_) => "di.init_failed",
        InjectionError::DisposeError(_) => "di.dispose_failed",
        InjectionError::DisposalPanicked { .. } => "di.disposal_panicked",
        InjectionError::NewError(_) => "di.construction_failed",
        InjectionError::General(_) => "di.operation_failed",
        InjectionError::MessageBroker(_) => "di.message_broker_failed",
        InjectionError::ServiceNotFound(_) => "di.service_not_found",
        InjectionError::ServiceResolutionFailed(_) => "di.resolution_failed",
        InjectionError::DependencyResolutionFailed { .. } => "di.dependency_resolution_failed",
        InjectionError::MissingDependency { .. } => "di.missing_dependency",
        InjectionError::CircularDependency { .. } => "di.circular_dependency",
        InjectionError::LifetimeMismatch { .. } => "di.lifetime_mismatch",
        InjectionError::AmbiguousRegistration { .. } => "di.ambiguous_registration",
        InjectionError::DuplicateInterfaceBinding { .. } => "di.duplicate_interface_binding",
        InjectionError::InvalidRegistrationPlan(_) => "di.invalid_registration_plan",
        InjectionError::RuntimeUnavailable { .. } => "di.runtime_unavailable",
        InjectionError::ContainerClosing => "di.container_closing",
        InjectionError::ContainerClosed => "di.container_closed",
        InjectionError::ScopeAlreadyActive { .. } => "di.scope_already_active",
        InjectionError::ScopeClosed { .. } => "di.scope_closed",
        InjectionError::ScopeCleanupTimedOut { .. } => "di.scope_cleanup_timed_out",
        InjectionError::ScopeRequired { .. } => "di.scope_required",
        InjectionError::ServiceInitializationFailed { .. } => "di.service_initialization_failed",
        InjectionError::InitializationCleanupFailed { .. } => "di.initialization_cleanup_failed",
        InjectionError::StartupRollbackFailed { .. } => "di.startup_rollback_failed",
        InjectionError::ShutdownTimedOut { .. } => "di.shutdown_timed_out",
        InjectionError::ShutdownFailed { .. } => "di.shutdown_failed",
    }
}
