//! Struct-based WebSocket controllers and their app-local runtime registry.
//!
//! A controller is constructed once for each [`crate::WsApp`] build that
//! materializes one of its operations. Static registration contains only type
//! identities, function pointers and immutable metadata; live controller and
//! dependency values remain owned by the application.

mod context;
mod registry;

pub use context::{ClientProxy, WebSocketClients, WebSocketContext, WebSocketRooms};
#[cfg(test)]
pub(crate) use registry::materialize_websocket_controllers;
#[doc(hidden)]
pub use registry::{
    BoundWebSocketOperation, ErasedWebSocketController, PENDING_WEBSOCKET_OPERATION_REGISTRATIONS,
    PendingWebSocketOperation, PendingWebSocketOperationRegistrationFn,
    WEBSOCKET_CONTROLLER_REGISTRATIONS, WebSocketActionFuture, WebSocketActionHandler,
    WebSocketActionRegistration, WebSocketAsyncApiRegistration, WebSocketAsyncApiStatus,
    WebSocketConnectionMiddlewareRegistration, WebSocketControllerDefinition,
    WebSocketControllerRegistration, WebSocketControllerRegistrationFn,
    WebSocketFrameCodecRegistration, WebSocketGuardRegistration,
    WebSocketHandshakeMiddlewareRegistration, WebSocketIdentityMiddlewareRegistration,
    WebSocketLifecycleAction, WebSocketLifecycleFuture, WebSocketLifecycleHandler,
    WebSocketMessageAction, WebSocketMessageMiddlewareRegistration, WebSocketOperationKind,
    WebSocketOperationMetadata, WebSocketPayloadCodecRegistration, downcast_websocket_controller,
    get_pending_websocket_operations, get_websocket_controller_registrations,
};
pub(crate) use registry::{
    MaterializedWebSocketAction, WebSocketActionTable, WebSocketLifecycleHandlers,
    materialize_websocket_controllers_with_pipeline,
};

use std::sync::Arc;

use async_trait::async_trait;
use lily_injection::Extensions;

/// Application-scoped WebSocket controller construction contract.
///
/// Lily invokes [`Self::new`] once per application that materializes at least
/// one operation belonging to the controller. Retained DI dependencies must be
/// singleton services. Resolve scoped and transient services through the active
/// invocation's extraction or extensions instead of retaining them here.
/// Directly constructed resources and raw spawned tasks are application-owned;
/// register app-lifetime async resources as DI-managed services for Lily cleanup.
#[async_trait]
pub trait WebSocketControllerTrait: Send + Sync + 'static {
    /// Construct this application's controller instance from Lily DI services.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError>
    where
        Self: Sized;
}

/// Secret-safe controller initialization failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketControllerInitError {
    /// A required application setting was not supplied.
    MissingConfiguration,
    /// A supplied application setting failed validation.
    InvalidConfiguration,
    /// A required controller dependency could not be initialized.
    Dependency,
    /// Initialization failed for another secret-safe reason.
    Internal,
}

impl WebSocketControllerInitError {
    /// Redact an arbitrary dependency failure to its stable public category.
    pub fn dependency<E>(_source: E) -> Self {
        Self::Dependency
    }

    /// Redact an arbitrary configuration failure to its stable public category.
    pub fn invalid_configuration<E>(_source: E) -> Self {
        Self::InvalidConfiguration
    }

    /// Redact an arbitrary implementation failure to its stable public category.
    pub fn internal<E>(_source: E) -> Self {
        Self::Internal
    }
}

impl std::fmt::Display for WebSocketControllerInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MissingConfiguration => "required WebSocket controller configuration is missing",
            Self::InvalidConfiguration => "WebSocket controller configuration is invalid",
            Self::Dependency => "WebSocket controller dependency initialization failed",
            Self::Internal => "WebSocket controller initialization failed internally",
        })
    }
}

impl std::error::Error for WebSocketControllerInitError {}

/// Build-time mismatch between a generated operation and its controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct WebSocketControllerBindingError {
    expected_controller: &'static str,
}

impl WebSocketControllerBindingError {
    pub(crate) const fn type_mismatch(expected_controller: &'static str) -> Self {
        Self {
            expected_controller,
        }
    }

    /// Fully qualified controller type expected by the generated adapter.
    pub const fn expected_controller(&self) -> &'static str {
        self.expected_controller
    }
}

impl std::fmt::Display for WebSocketControllerBindingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "WebSocket operation expected controller instance type '{}'",
            self.expected_controller
        )
    }
}

impl std::error::Error for WebSocketControllerBindingError {}

/// Failure while converting static controller metadata into an app-local table.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WebSocketControllerMaterializationError {
    /// One static registration function panicked while producing metadata.
    #[error("WebSocket {registry} registration at index {index} panicked")]
    RegistrationPanicked {
        /// Stable registry category (`controller` or `operation`).
        registry: &'static str,
        /// Position in the immutable link-time slice.
        index: usize,
    },
    /// The same Rust controller type was registered more than once.
    #[error("WebSocket controller '{controller}' is registered more than once")]
    DuplicateRegistration {
        /// Fully qualified Rust controller type.
        controller: &'static str,
    },
    /// An operation refers to a controller without a matching registration.
    #[error(
        "WebSocket controller '{controller}' required by operation '{operation}' is not registered"
    )]
    MissingRegistration {
        /// Fully qualified Rust controller type.
        controller: &'static str,
        /// Stable generated operation identity.
        operation: &'static str,
    },
    /// Type identity and type name in static metadata disagree.
    #[error(
        "operation '{operation}' names controller '{operation_controller}' but its TypeId belongs to '{registered_controller}'"
    )]
    MetadataMismatch {
        /// Controller name associated with the registered TypeId.
        registered_controller: &'static str,
        /// Controller name carried by operation metadata.
        operation_controller: &'static str,
        /// Stable generated operation identity.
        operation: &'static str,
    },
    /// A controller namespace is not a canonical exact route token.
    #[error("WebSocket controller '{controller}' has invalid namespace '{namespace}'")]
    InvalidNamespace {
        /// Fully qualified Rust controller type.
        controller: &'static str,
        /// Rejected namespace metadata.
        namespace: String,
    },
    /// Two controllers claim the same exact namespace.
    #[error(
        "WebSocket namespace '{namespace}' is registered by both '{first_controller}' and '{duplicate_controller}'"
    )]
    DuplicateNamespace {
        /// Exact duplicated namespace.
        namespace: String,
        /// First controller that claimed the namespace.
        first_controller: &'static str,
        /// Later controller that claimed the namespace.
        duplicate_controller: &'static str,
    },
    /// A message event is not a canonical local route token.
    #[error("WebSocket operation '{operation}' has invalid event '{event}'")]
    InvalidEvent {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Rejected local event metadata.
        event: String,
    },
    /// Combined `namespace:event` exceeds the canonical wire route bound.
    #[error("WebSocket route '{namespace}:{event}' exceeds {maximum_bytes} bytes")]
    RouteTooLong {
        /// Exact controller namespace.
        namespace: String,
        /// Local action event.
        event: String,
        /// Maximum accepted UTF-8 byte length.
        maximum_bytes: usize,
    },
    /// A controller/action timeout exceeds the canonical execution bound.
    #[error(
        "WebSocket operation '{operation}' timeout must be between 1 and {maximum_seconds} seconds (got {seconds})"
    )]
    InvalidTimeout {
        /// Controller or generated operation identity.
        operation: &'static str,
        /// Rejected timeout in whole seconds.
        seconds: u64,
        /// Canonical maximum execution timeout.
        maximum_seconds: u64,
    },
    /// Operation kind and event metadata form an invalid pair.
    #[error("WebSocket operation '{operation}' has invalid {kind} metadata")]
    InvalidOperationMetadata {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Stable operation kind name.
        kind: &'static str,
    },
    /// The same `(namespace, event)` message operation was registered twice.
    #[error("duplicate WebSocket message operation '{namespace}:{event}'")]
    DuplicateOperation {
        /// Exact controller namespace.
        namespace: String,
        /// Local action event.
        event: String,
    },
    /// A controller registered the same lifecycle hook more than once.
    #[error("WebSocket controller namespace '{namespace}' has more than one {kind} hook")]
    DuplicateLifecycle {
        /// Exact controller namespace.
        namespace: String,
        /// `connected` or `disconnected`.
        kind: &'static str,
    },
    /// The same component type occurs twice in one effective plan.
    #[error(
        "WebSocket operation '{operation}' contains duplicate {component_kind} type '{component}'"
    )]
    DuplicateComponent {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Middleware or guard category.
        component_kind: &'static str,
        /// Fully qualified duplicate component type.
        component: &'static str,
    },
    /// A middleware constructor failed during application build.
    #[error(
        "WebSocket middleware '{middleware}' initialization failed for operation '{operation}': {source}"
    )]
    MiddlewareInitialization {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Fully qualified middleware type.
        middleware: &'static str,
        /// Secret-safe middleware initialization category.
        source: crate::middleware::WsMiddlewareInitError,
    },
    /// A middleware constructor panicked during application build.
    #[error(
        "WebSocket middleware '{middleware}' initialization panicked for operation '{operation}'"
    )]
    MiddlewareInitializationPanicked {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Fully qualified middleware type.
        middleware: &'static str,
    },
    /// One effective middleware plan failed static validation.
    #[error("WebSocket operation '{operation}' has an invalid middleware plan: {source}")]
    InvalidMiddlewarePlan {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Secret-safe bounded plan validation failure.
        source: crate::middleware::MiddlewareConfigError,
    },
    /// Middleware descriptor or validation code panicked during app build.
    #[error("WebSocket operation '{operation}' middleware plan panicked during validation")]
    MiddlewarePlanPanicked {
        /// Stable generated operation identity.
        operation: &'static str,
    },
    /// A guard constructor failed during application build.
    #[error(
        "WebSocket guard '{guard}' initialization failed for operation '{operation}': {source}"
    )]
    GuardInitialization {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Fully qualified guard type.
        guard: &'static str,
        /// Existing secret-safe guard initialization category.
        source: crate::guard::GuardInitializationError,
    },
    /// A guard constructor panicked during application build.
    #[error("WebSocket guard '{guard}' initialization panicked for operation '{operation}'")]
    GuardInitializationPanicked {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Fully qualified guard type.
        guard: &'static str,
    },
    /// A frame or payload codec constructor failed during application build.
    #[error("WebSocket {codec_kind} codec '{codec}' initialization failed: {source}")]
    CodecInitialization {
        /// `frame` or `payload`.
        codec_kind: &'static str,
        /// Fully qualified codec type.
        codec: &'static str,
        /// Secret-safe codec initialization category.
        source: crate::codec::WebSocketCodecInitError,
    },
    /// A codec constructor panicked during application build.
    #[error("WebSocket {codec_kind} codec '{codec}' initialization panicked")]
    CodecInitializationPanicked {
        /// `frame` or `payload`.
        codec_kind: &'static str,
        /// Fully qualified codec type.
        codec: &'static str,
    },
    /// Static codec metadata could not bind its concrete app-owned instance.
    #[error("WebSocket {codec_kind} codec '{codec}' binding failed")]
    CodecBinding {
        /// `frame` or `payload`.
        codec_kind: &'static str,
        /// Fully qualified codec type.
        codec: &'static str,
    },
    /// The application-scoped controller constructor failed.
    #[error("WebSocket controller '{controller}' initialization failed: {source}")]
    Initialization {
        /// Fully qualified Rust controller type.
        controller: &'static str,
        /// Secret-safe constructor failure category.
        source: WebSocketControllerInitError,
    },
    /// A generated operation could not bind to the controller instance.
    #[error("WebSocket controller '{controller}' operation '{operation}' binding failed")]
    Binding {
        /// Fully qualified Rust controller type.
        controller: &'static str,
        /// Stable generated operation identity.
        operation: &'static str,
    },
    /// A generated operation binder panicked during application build.
    #[error("WebSocket controller '{controller}' operation '{operation}' binding panicked")]
    BindingPanicked {
        /// Fully qualified Rust controller type.
        controller: &'static str,
        /// Stable generated operation identity.
        operation: &'static str,
    },
    /// Generated binder returned a handler for a different operation kind.
    #[error(
        "WebSocket operation '{operation}' declared kind '{declared}' but bound kind '{bound}'"
    )]
    BoundKindMismatch {
        /// Stable generated operation identity.
        operation: &'static str,
        /// Kind in static pending metadata.
        declared: &'static str,
        /// Kind returned by the generated binder.
        bound: &'static str,
    },
}

/// Bounded lifecycle failure used by connected/disconnected adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WebSocketLifecycleError {
    /// Application rejected lifecycle admission with a stable public code.
    #[error("WebSocket lifecycle rejected with code '{code}'")]
    Rejected {
        /// Stable, non-secret application error code.
        code: &'static str,
    },
    /// Lifecycle execution failed internally.
    #[error("WebSocket lifecycle failed internally")]
    Internal,
}

impl WebSocketLifecycleError {
    /// Construct a stable lifecycle rejection without retaining its source.
    pub const fn rejected(code: &'static str) -> Self {
        Self::Rejected { code }
    }

    /// Redact an arbitrary implementation failure.
    pub fn internal<E>(_source: E) -> Self {
        Self::Internal
    }
}
