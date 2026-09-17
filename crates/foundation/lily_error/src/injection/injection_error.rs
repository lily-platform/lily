use std::any::TypeId;

use crate::application::MessageBrokerError;

/// Terminal result of one owned component or shutdown phase.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ShutdownOutcomeStatus {
    /// The component completed graceful shutdown.
    Completed,
    /// The component returned a shutdown error.
    Failed,
    /// The component panicked while shutting down.
    Panicked,
    /// The component exceeded its shutdown deadline.
    TimedOut,
    /// The component was cancelled before completion.
    Cancelled,
}

/// Structured shutdown evidence retained on both successful reports and
/// aggregate shutdown failures.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ShutdownOutcome {
    /// Stable component or shutdown-phase name.
    pub component: String,
    /// Terminal component status.
    pub status: ShutdownOutcomeStatus,
    /// Optional bounded diagnostic detail.
    pub detail: Option<String>,
}

impl ShutdownOutcome {
    /// Creates evidence for a component that completed successfully.
    pub fn completed(component: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            status: ShutdownOutcomeStatus::Completed,
            detail: None,
        }
    }

    /// Creates evidence with a terminal status and bounded detail.
    pub fn with_detail(
        component: impl Into<String>,
        status: ShutdownOutcomeStatus,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            component: component.into(),
            status,
            detail: Some(detail.into()),
        }
    }
}

/// Resource counters captured when an aggregate shutdown failure is returned.
/// This makes a failed close as observable as a successful shutdown report.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct ShutdownRemainingWork {
    /// Request or application scopes still active at shutdown return.
    pub active_scopes: usize,
    /// Cleanup tasks still pending.
    pub cleanup_tasks: usize,
    /// Dependency resolutions still in progress.
    pub active_resolutions: usize,
    /// Root lifecycle entries still owned by the container.
    pub root_lifecycle_entries: usize,
}

/// Dependency registration, resolution, scope, and lifecycle failures.
#[derive(Debug, PartialEq, Clone)]
pub enum InjectionError {
    /// A service's initialization hook failed.
    InitError(String),
    /// A service's disposal hook failed.
    DisposeError(String),
    /// A service panicked during disposal.
    DisposalPanicked {
        /// Service that panicked.
        service: String,
        /// Bounded panic diagnostic.
        message: String,
    },
    /// Constructing a service instance failed.
    NewError(String),
    /// A DI operation failed without a narrower category.
    General(String),
    /// A message-broker operation failed while constructing or initializing a service.
    MessageBroker(MessageBrokerError),
    /// No registration exists for the requested service.
    ServiceNotFound(String),
    /// A registered service could not be resolved.
    ServiceResolutionFailed(String),
    /// Resolving a service's dependency failed.
    DependencyResolutionFailed {
        /// Service being constructed.
        service: String,
        /// Dependency that could not be resolved.
        dependency: String,
        /// Typed underlying resolution failure.
        source: Box<InjectionError>,
    },
    /// A required dependency type is not registered.
    MissingDependency {
        /// Service declaring the dependency.
        service: String,
        /// Missing dependency's Rust type identifier.
        dependency_type_id: TypeId,
    },
    /// The registration graph contains a dependency cycle.
    CircularDependency {
        /// Ordered service path closing the cycle.
        cycle: Vec<String>,
    },
    /// A longer-lived service attempts to capture a shorter-lived dependency.
    LifetimeMismatch {
        /// Service declaring the invalid dependency.
        service: String,
        /// Declaring service lifetime.
        service_lifetime: String,
        /// Captured dependency.
        dependency: String,
        /// Captured dependency lifetime.
        dependency_lifetime: String,
    },
    /// More than one concrete registration matches an unqualified type lookup.
    AmbiguousRegistration {
        /// Ambiguous Rust type identifier.
        type_id: TypeId,
        /// Matching service names.
        services: Vec<String>,
    },
    /// More than one implementation claims the same unique interface binding.
    DuplicateInterfaceBinding {
        /// Interface Rust type identifier.
        interface_type_id: TypeId,
        /// Human-readable interface type name.
        interface: String,
        /// Conflicting implementation names.
        implementations: Vec<String>,
    },
    /// Generated or programmatic registrations form an invalid plan.
    InvalidRegistrationPlan(String),
    /// An async operation was requested without a Tokio runtime.
    RuntimeUnavailable {
        /// Operation requiring the runtime.
        operation: String,
    },
    /// The container has begun rejecting new work during shutdown.
    ContainerClosing,
    /// The container has completed shutdown.
    ContainerClosed,
    /// The requested scope identifier is already active.
    ScopeAlreadyActive {
        /// Duplicate scope identifier.
        scope_id: String,
    },
    /// The requested scope is already closed.
    ScopeClosed {
        /// Closed scope identifier.
        scope_id: String,
    },
    /// A scope's asynchronous disposal exceeded its framework deadline and
    /// was forcefully stopped before its owner could continue.
    ScopeCleanupTimedOut {
        /// Scope whose cleanup task was stopped.
        scope_id: String,
    },
    /// Resolving the service requires an active scope.
    ScopeRequired {
        /// Scoped service being resolved.
        service: String,
    },
    /// A specific service failed during eager initialization.
    ServiceInitializationFailed {
        /// Service that failed to initialize.
        service: String,
        /// Typed initialization failure.
        source: Box<InjectionError>,
    },
    /// Failed initialization was followed by failed cleanup.
    InitializationCleanupFailed {
        /// Service being initialized.
        service: String,
        /// Original initialization failure.
        initialization: Box<InjectionError>,
        /// Cleanup failure observed during rollback.
        cleanup: Box<InjectionError>,
    },
    /// Application startup failed and bounded rollback also reported failures.
    ///
    /// `startup` remains the primary typed cause. The secondary fields retain
    /// per-component outcomes and remaining-work counters without flattening
    /// lifecycle evidence into display text.
    StartupRollbackFailed {
        /// Original startup failure.
        startup: Box<InjectionError>,
        /// Bounded rollback diagnostics.
        rollback_errors: Vec<String>,
        /// Typed per-component rollback evidence.
        rollback_outcomes: Vec<ShutdownOutcome>,
        /// Terminal resource counters observed after rollback.
        rollback_remaining: Option<ShutdownRemainingWork>,
    },
    /// Container shutdown exceeded its configured deadline.
    ShutdownTimedOut {
        /// Configured shutdown deadline in milliseconds.
        timeout_ms: u64,
        /// Scopes still active when the deadline elapsed.
        active_scopes: usize,
    },
    /// One or more shutdown phases failed.
    ShutdownFailed {
        /// Bounded aggregate diagnostics.
        errors: Vec<String>,
        /// Per-component shutdown evidence.
        outcomes: Vec<ShutdownOutcome>,
        /// Resource counters retained when work remains.
        remaining: Option<ShutdownRemainingWork>,
    },
}

impl std::fmt::Display for InjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InjectionError::InitError(msg) => write!(f, "InitError: {msg}"),
            InjectionError::DisposeError(msg) => write!(f, "DisposeError: {msg}"),
            InjectionError::DisposalPanicked { service, message } => {
                write!(
                    f,
                    "DisposalPanicked: service '{service}' panicked: {message}"
                )
            }
            InjectionError::NewError(msg) => write!(f, "NewError: {msg}"),
            InjectionError::General(msg) => write!(f, "General: {msg}"),
            InjectionError::MessageBroker(error) => error.fmt(f),
            InjectionError::ServiceNotFound(msg) => write!(f, "ServiceNotFound: {msg}"),
            InjectionError::ServiceResolutionFailed(msg) => {
                write!(f, "ServiceResolutionFailed: {msg}")
            }
            InjectionError::DependencyResolutionFailed {
                service,
                dependency,
                source,
            } => write!(
                f,
                "DependencyResolutionFailed: service '{service}' could not resolve '{dependency}': {source}"
            ),
            InjectionError::MissingDependency {
                service,
                dependency_type_id,
            } => write!(
                f,
                "MissingDependency: service '{service}' depends on unregistered type {dependency_type_id:?}"
            ),
            InjectionError::CircularDependency { cycle } => {
                write!(f, "CircularDependency: {}", cycle.join(" -> "))
            }
            InjectionError::LifetimeMismatch {
                service,
                service_lifetime,
                dependency,
                dependency_lifetime,
            } => write!(
                f,
                "LifetimeMismatch: {service} ({service_lifetime}) cannot capture {dependency} ({dependency_lifetime})"
            ),
            InjectionError::AmbiguousRegistration { type_id, services } => write!(
                f,
                "AmbiguousRegistration: type {type_id:?} is provided by [{}]",
                services.join(", ")
            ),
            InjectionError::DuplicateInterfaceBinding {
                interface_type_id,
                interface,
                implementations,
            } => write!(
                f,
                "DuplicateInterfaceBinding: interface '{interface}' ({interface_type_id:?}) is provided by [{}]",
                implementations.join(", ")
            ),
            InjectionError::InvalidRegistrationPlan(message) => {
                write!(f, "InvalidRegistrationPlan: {message}")
            }
            InjectionError::RuntimeUnavailable { operation } => write!(
                f,
                "RuntimeUnavailable: '{operation}' requires an active Tokio runtime"
            ),
            InjectionError::ContainerClosing => {
                write!(
                    f,
                    "ContainerClosing: the application is no longer accepting new work"
                )
            }
            InjectionError::ContainerClosed => {
                write!(
                    f,
                    "ContainerClosed: the application service provider is closed"
                )
            }
            InjectionError::ScopeAlreadyActive { scope_id } => write!(
                f,
                "ScopeAlreadyActive: application scope '{scope_id}' is already active"
            ),
            InjectionError::ScopeClosed { scope_id } => write!(
                f,
                "ScopeClosed: application scope '{scope_id}' is closing or already closed"
            ),
            InjectionError::ScopeCleanupTimedOut { scope_id } => write!(
                f,
                "ScopeCleanupTimedOut: application scope '{scope_id}' did not finish disposal before its deadline"
            ),
            InjectionError::ScopeRequired { service } => write!(
                f,
                "ScopeRequired: scoped service '{service}' must be resolved inside an application scope"
            ),
            InjectionError::ServiceInitializationFailed { service, source } => write!(
                f,
                "ServiceInitializationFailed: service '{service}' failed to start: {source}"
            ),
            InjectionError::InitializationCleanupFailed {
                service,
                initialization,
                cleanup,
            } => write!(
                f,
                "InitializationCleanupFailed: service '{service}' failed to start ({initialization}) and cleanup of its partial state also failed ({cleanup})"
            ),
            InjectionError::StartupRollbackFailed {
                startup,
                rollback_errors,
                ..
            } => write!(
                f,
                "StartupRollbackFailed: startup failed ({startup}); rollback also failed: {}",
                rollback_errors.join("; ")
            ),
            InjectionError::ShutdownTimedOut {
                timeout_ms,
                active_scopes,
            } => write!(
                f,
                "ShutdownTimedOut: {active_scopes} active scope(s) remained after {timeout_ms}ms"
            ),
            InjectionError::ShutdownFailed { errors, .. } => {
                write!(f, "ShutdownFailed: {}", errors.join("; "))
            }
        }
    }
}

impl std::error::Error for InjectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            InjectionError::DependencyResolutionFailed { source, .. }
            | InjectionError::ServiceInitializationFailed { source, .. } => Some(source.as_ref()),
            InjectionError::InitializationCleanupFailed { initialization, .. } => {
                Some(initialization.as_ref())
            }
            InjectionError::StartupRollbackFailed { startup, .. } => Some(startup.as_ref()),
            InjectionError::MessageBroker(error) => Some(error),
            _ => None,
        }
    }
}

impl InjectionError {
    /// Returns the primary typed message-broker cause retained through DI
    /// startup wrappers.
    ///
    /// Cleanup and rollback failures remain available on the original error;
    /// this accessor deliberately follows only the primary startup branch.
    pub fn message_broker_error(&self) -> Option<&MessageBrokerError> {
        match self {
            Self::MessageBroker(error) => Some(error),
            Self::DependencyResolutionFailed { source, .. }
            | Self::ServiceInitializationFailed { source, .. } => source.message_broker_error(),
            Self::InitializationCleanupFailed { initialization, .. } => {
                initialization.message_broker_error()
            }
            Self::StartupRollbackFailed { startup, .. } => startup.message_broker_error(),
            _ => None,
        }
    }
}

// From implementations for error conversion
impl From<std::io::Error> for InjectionError {
    fn from(error: std::io::Error) -> Self {
        InjectionError::General(format!("IO Error: {error}"))
    }
}

impl From<MessageBrokerError> for InjectionError {
    fn from(error: MessageBrokerError) -> Self {
        Self::MessageBroker(error)
    }
}

impl From<crate::config::ConfigError> for InjectionError {
    fn from(error: crate::config::ConfigError) -> Self {
        match error {
            crate::config::ConfigError::KeyNotFound(key) => {
                InjectionError::ServiceResolutionFailed(format!(
                    "Configuration key not found: {key}"
                ))
            }
            crate::config::ConfigError::TypeCastError {
                key,
                expected_type,
                error,
            } => InjectionError::ServiceResolutionFailed(format!(
                "Configuration type cast error for key '{key}': expected {expected_type}, error: {error}"
            )),
            crate::config::ConfigError::IoError(msg) => {
                InjectionError::InitError(format!("Configuration IO error: {msg}"))
            }
            crate::config::ConfigError::ParseError(msg) => {
                InjectionError::InitError(format!("Configuration parse error: {msg}"))
            }
            crate::config::ConfigError::SerializationError(msg) => {
                InjectionError::InitError(format!("Configuration serialization error: {msg}"))
            }
            crate::config::ConfigError::ValidationError(msg) => {
                InjectionError::InitError(format!("Configuration validation error: {msg}"))
            }
            crate::config::ConfigError::SecretResolveError { key, error } => {
                InjectionError::InitError(format!(
                    "Failed to resolve configuration secret '{key}': {error}"
                ))
            }
        }
    }
}
