#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![doc(hidden)]

//! Link-time metadata ABI between `lily_queue` handler macros and
//! `lily_consumer`.
//!
//! Application code must not register or enumerate these records directly.
//! Import `queue` and `queue_service` from `lily_queue`; the consumer
//! composition root owns discovery. Items remain Rust-public only because
//! procedural macro output is compiled in a downstream crate.

use std::any::{Any, TypeId};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use lily_error::application::{QueueHandlerError, QueueHandlerFailureClass};
use lily_injection::{Extensions, InjectionError};

/// `linkme` path used by generated handler registration code.
#[doc(hidden)]
pub use linkme;

/// Type-erased handler function for one validated queue delivery.
///
/// The invocation is erased to keep this low-level registry independent from
/// the `lily_queue` facade. Generated adapters downcast it to Lily's private
/// delivery invocation before running typed extractors. A mismatch is a typed,
/// retryable framework failure rather than a panic.
pub type HandlerFunction =
    for<'a> fn(
        Arc<dyn std::any::Any + Send + Sync>,
        &'a mut (dyn std::any::Any + Send),
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueHandlerError>> + Send + 'a>>;

/// Database-neutral delivery guarantee selected by one generated handler.
///
/// The registry records intent only. A concrete consumer runtime must bind a
/// transactional handler to exactly one supported storage backend before it
/// admits broker deliveries.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum DeliveryGuarantee {
    /// Ordinary RabbitMQ at-least-once processing and settlement.
    #[default]
    AtLeastOnce,
    /// Inbox deduplication and outbox enqueue participate in the same database
    /// transaction as application writes made through the typed transaction
    /// context.
    TransactionalInbox,
}

/// One application-owned, type-erased middleware or guard instance.
///
/// The concrete value is erased only across Lily's private registry/runtime
/// boundary. Generated registrations retain its exact [`TypeId`], and the
/// runtime adapter validates every downcast without exposing the value to
/// application code.
pub type ErasedQueuePipelineComponent = Arc<dyn Any + Send + Sync>;

/// Future returned while constructing one application-owned pipeline component.
pub type QueuePipelineInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<ErasedQueuePipelineComponent, QueuePipelineComponentInitError>>
            + Send
            + 'static,
    >,
>;

/// Borrowed future returned by one delivery middleware or guard hook.
pub type QueuePipelineHookFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), QueueHandlerError>> + Send + 'a>>;

/// Source-free failure raised while constructing a queue middleware or guard.
///
/// Constructors may log a trusted internal source at their own boundary, but
/// this cross-crate value deliberately retains only a stable category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum QueuePipelineComponentInitError {
    /// A required application-lifetime dependency is unavailable.
    MissingDependency,
    /// Construction attempted to resolve a delivery-scoped dependency.
    ScopeRequired,
    /// Immutable component policy or configuration is invalid.
    InvalidPolicy,
    /// Construction failed for another internal reason.
    Internal,
}

impl QueuePipelineComponentInitError {
    /// Stable, low-cardinality diagnostic code.
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::MissingDependency => "QUEUE_PIPELINE_DEPENDENCY_MISSING",
            Self::ScopeRequired => "QUEUE_PIPELINE_SCOPE_REQUIRED",
            Self::InvalidPolicy => "QUEUE_PIPELINE_POLICY_INVALID",
            Self::Internal => "QUEUE_PIPELINE_INITIALIZATION_FAILED",
        }
    }

    fn from_injection_error(error: &InjectionError) -> Self {
        match error {
            InjectionError::ScopeRequired { .. } => Self::ScopeRequired,
            InjectionError::ServiceNotFound(_) | InjectionError::MissingDependency { .. } => {
                Self::MissingDependency
            }
            InjectionError::DependencyResolutionFailed { source, .. } => {
                Self::from_injection_error(source)
            }
            _ => Self::Internal,
        }
    }
}

impl From<InjectionError> for QueuePipelineComponentInitError {
    fn from(error: InjectionError) -> Self {
        Self::from_injection_error(&error)
    }
}

impl std::fmt::Display for QueuePipelineComponentInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.diagnostic_code())
    }
}

impl std::error::Error for QueuePipelineComponentInitError {}

/// Framework-owned terminal state supplied to reverse middleware unwind.
///
/// The value is intentionally bounded and source-free. Application errors,
/// message payloads and broker details never cross this hook boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum QueueDeliveryOutcome {
    /// Extraction and the application handler completed successfully.
    Succeeded,
    /// A typed handler, extractor, guard or middleware failure was produced.
    Failed {
        /// Explicit retry/permanent settlement class.
        class: QueueHandlerFailureClass,
        /// Stable bounded failure code.
        code: &'static str,
    },
    /// A framework containment boundary caught a panic.
    Panicked,
    /// The delivery execution budget expired.
    TimedOut,
    /// Cooperative lifecycle cancellation stopped delivery execution.
    Cancelled,
}

impl QueueDeliveryOutcome {
    /// Whether the delivery pipeline completed successfully.
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Succeeded)
    }

    /// Explicit retry/permanent class when this is a typed failure.
    #[must_use]
    pub const fn failure_class(self) -> Option<QueueHandlerFailureClass> {
        match self {
            Self::Failed { class, .. } => Some(class),
            Self::Succeeded | Self::Panicked | Self::TimedOut | Self::Cancelled => None,
        }
    }

    /// Stable failure code when this is a typed failure.
    #[must_use]
    pub const fn failure_code(self) -> Option<&'static str> {
        match self {
            Self::Failed { code, .. } => Some(code),
            Self::Succeeded | Self::Panicked | Self::TimedOut | Self::Cancelled => None,
        }
    }
}

/// Type-erased constructor for one queue middleware or guard.
pub type QueuePipelineInitializer = fn(Arc<Extensions>) -> QueuePipelineInitializationFuture;

/// Type-erased middleware enter hook.
pub type QueueMiddlewareBeforeHook = for<'a> fn(
    &'a (dyn Any + Send + Sync),
    &'a mut (dyn Any + Send),
) -> QueuePipelineHookFuture<'a>;

/// Type-erased middleware reverse-unwind hook.
pub type QueueMiddlewareAfterHook = for<'a> fn(
    &'a (dyn Any + Send + Sync),
    &'a mut (dyn Any + Send),
    QueueDeliveryOutcome,
) -> QueuePipelineHookFuture<'a>;

/// Type-erased abnormal middleware cleanup with adapter-owned invocation metadata.
pub type QueueMiddlewareTerminationHook = for<'a> fn(
    &'a (dyn Any + Send + Sync),
    &'a mut (dyn Any + Send),
    &'a (dyn Any + Send + Sync),
) -> QueuePipelineHookFuture<'a>;

/// Type-erased delivery guard hook.
pub type QueueGuardHook = for<'a> fn(
    &'a (dyn Any + Send + Sync),
    &'a mut (dyn Any + Send),
) -> QueuePipelineHookFuture<'a>;

/// Build-time registration for one concrete queue middleware type.
///
/// This is a hidden macro/runtime ABI. Applications select middleware through
/// `ConsumerBuilder` or `#[middleware(...)]` rather than constructing this
/// record directly.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct QueueMiddlewareRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: QueuePipelineInitializer,
    before: QueueMiddlewareBeforeHook,
    after: QueueMiddlewareAfterHook,
    termination: Option<QueueMiddlewareTerminationHook>,
}

impl QueueMiddlewareRegistration {
    /// Constructs a generated middleware registration.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        type_id: TypeId,
        type_name: &'static str,
        initialize: QueuePipelineInitializer,
        before: QueueMiddlewareBeforeHook,
        after: QueueMiddlewareAfterHook,
    ) -> Self {
        Self {
            type_id,
            type_name,
            initialize,
            before,
            after,
            termination: None,
        }
    }

    /// Attach the typed adapter's abnormal cleanup callback.
    #[doc(hidden)]
    #[must_use]
    pub const fn with_termination(mut self, hook: QueueMiddlewareTerminationHook) -> Self {
        self.termination = Some(hook);
        self
    }

    /// Run abnormal cleanup without exposing a mutable settlement result.
    #[doc(hidden)]
    pub fn terminate<'a>(
        self,
        component: &'a (dyn Any + Send + Sync),
        invocation: &'a mut (dyn Any + Send),
        termination: &'a (dyn Any + Send + Sync),
    ) -> QueuePipelineHookFuture<'a> {
        match self.termination {
            Some(hook) => hook(component, invocation, termination),
            None => Box::pin(async { Ok(()) }),
        }
    }

    /// Concrete middleware identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    /// Fully-qualified concrete middleware type name.
    #[doc(hidden)]
    #[must_use]
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }

    /// Constructs the application-owned erased middleware instance.
    #[doc(hidden)]
    pub fn initialize(self, extensions: Arc<Extensions>) -> QueuePipelineInitializationFuture {
        (self.initialize)(extensions)
    }

    /// Enters this middleware for one borrowed delivery invocation.
    #[doc(hidden)]
    pub fn before<'a>(
        self,
        component: &'a (dyn Any + Send + Sync),
        invocation: &'a mut (dyn Any + Send),
    ) -> QueuePipelineHookFuture<'a> {
        (self.before)(component, invocation)
    }

    /// Unwinds this middleware for one borrowed delivery invocation.
    #[doc(hidden)]
    pub fn after<'a>(
        self,
        component: &'a (dyn Any + Send + Sync),
        invocation: &'a mut (dyn Any + Send),
        outcome: QueueDeliveryOutcome,
    ) -> QueuePipelineHookFuture<'a> {
        (self.after)(component, invocation, outcome)
    }
}

impl std::fmt::Debug for QueueMiddlewareRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueueMiddlewareRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

/// Build-time registration for one concrete queue guard type.
///
/// This is a hidden macro/runtime ABI. Applications select guards through
/// `ConsumerBuilder` or `#[guard(...)]` rather than constructing this record
/// directly.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct QueueGuardRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: QueuePipelineInitializer,
    can_activate: QueueGuardHook,
}

impl QueueGuardRegistration {
    /// Constructs a generated guard registration.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        type_id: TypeId,
        type_name: &'static str,
        initialize: QueuePipelineInitializer,
        can_activate: QueueGuardHook,
    ) -> Self {
        Self {
            type_id,
            type_name,
            initialize,
            can_activate,
        }
    }

    /// Concrete guard identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    /// Fully-qualified concrete guard type name.
    #[doc(hidden)]
    #[must_use]
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }

    /// Constructs the application-owned erased guard instance.
    #[doc(hidden)]
    pub fn initialize(self, extensions: Arc<Extensions>) -> QueuePipelineInitializationFuture {
        (self.initialize)(extensions)
    }

    /// Evaluates this guard for one borrowed delivery invocation.
    #[doc(hidden)]
    pub fn can_activate<'a>(
        self,
        component: &'a (dyn Any + Send + Sync),
        invocation: &'a mut (dyn Any + Send),
    ) -> QueuePipelineHookFuture<'a> {
        (self.can_activate)(component, invocation)
    }
}

impl std::fmt::Debug for QueueGuardRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueueGuardRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

/// Payload authority selected by a generated typed handler plan.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueuePayloadKind {
    /// The handler consumes delivery parts only.
    None,
    /// A JSON payload is deserialized into an application type.
    Json,
    /// A strict UTF-8 text payload is consumed.
    Text,
    /// Bounded binary bytes are consumed.
    Binary,
    /// The bounded read-only raw delivery view is consumed.
    Raw,
    /// An application-defined terminal payload extractor is used.
    Custom,
}

/// Static input contract inferred from one generated handler signature.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueueHandlerInputContract {
    /// Kind of the sole terminal payload authority, or [`QueuePayloadKind::None`].
    pub payload_kind: QueuePayloadKind,
    /// Rust type name of the terminal extractor when one exists.
    pub payload_type_name: Option<&'static str>,
}

/// Effective documentation intent baked into one canonical handler record.
///
/// This type exists only when the queue AsyncAPI feature is enabled. It is
/// carried by [`QueueHandlerMetadata`]; there is deliberately no second
/// documentation registry.
#[cfg(feature = "asyncapi")]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[doc(hidden)]
pub enum QueueAsyncApiStatus {
    /// Neither the queue service nor this handler selected a documentation policy.
    #[default]
    Unspecified,
    /// The accepted handler is included in the Consumer AsyncAPI document.
    Documented,
    /// The accepted handler is intentionally excluded from the document.
    Skipped,
}

/// Payload schema authority retained by one documented handler registration.
///
/// Built-in typed extractors generate these values from their exact input
/// type. Raw, custom and body-unobserved handlers must explicitly select a
/// generated schema or an opaque contract in `#[asyncapi(...)]`.
#[cfg(feature = "asyncapi")]
#[derive(Clone, Debug)]
#[doc(hidden)]
pub enum QueueAsyncApiPayload {
    /// Draft 7 inbound schema inferred from Lily's built-in JSON extractor.
    Generated {
        /// Monomorphized schema factory.
        schema: lily_asyncapi::__private::SchemaFactory,
        /// Canonical MIME type describing the payload bytes.
        content_type: &'static str,
    },
    /// Explicit application-authored schema authority for a raw, custom or
    /// body-unobserved handler.
    ExplicitGenerated {
        /// Monomorphized schema factory.
        schema: lily_asyncapi::__private::SchemaFactory,
        /// Canonical MIME type describing the payload bytes.
        content_type: &'static str,
    },
    /// Strict UTF-8 string payload.
    Text {
        /// Canonical MIME type describing the payload bytes.
        content_type: &'static str,
    },
    /// Opaque binary byte payload.
    Binary {
        /// Canonical MIME type describing the payload bytes.
        content_type: &'static str,
    },
    /// Application-authored opaque wire contract.
    Opaque {
        /// Canonical MIME type describing the payload bytes.
        content_type: &'static str,
    },
    /// No schema/content decision was available for this handler.
    Unspecified,
}

#[cfg(feature = "asyncapi")]
impl QueueAsyncApiPayload {
    /// Creates an inferred JSON input contract.
    #[must_use]
    pub const fn generated(
        schema: lily_asyncapi::__private::SchemaFactory,
        content_type: &'static str,
    ) -> Self {
        Self::Generated {
            schema,
            content_type,
        }
    }

    /// Creates an explicit schema authority supplied by handler metadata.
    #[must_use]
    pub const fn explicit_generated(
        schema: lily_asyncapi::__private::SchemaFactory,
        content_type: &'static str,
    ) -> Self {
        Self::ExplicitGenerated {
            schema,
            content_type,
        }
    }

    /// Creates the canonical strict UTF-8 text contract.
    #[must_use]
    pub const fn text() -> Self {
        Self::Text {
            content_type: "text/plain; charset=utf-8",
        }
    }

    /// Creates the canonical binary contract.
    #[must_use]
    pub const fn binary() -> Self {
        Self::Binary {
            content_type: "application/octet-stream",
        }
    }

    /// Creates an explicit opaque payload contract.
    #[must_use]
    pub const fn opaque(content_type: &'static str) -> Self {
        Self::Opaque { content_type }
    }
}

/// Effective AsyncAPI metadata attached to one canonical handler record.
#[cfg(feature = "asyncapi")]
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct QueueAsyncApiRegistration {
    /// Effective documented/skipped/unspecified status.
    pub status: QueueAsyncApiStatus,
    /// Operation summary after service/handler inheritance.
    pub summary: Option<&'static str>,
    /// Operation description after service/handler inheritance.
    pub description: Option<&'static str>,
    /// Optional explicit AsyncAPI operations-map key.
    pub operation_id: Option<&'static str>,
    /// Deterministic parent-then-child tag union.
    pub tags: Vec<&'static str>,
    /// Effective security alternatives; a child list replaces its parent list.
    pub security: Vec<&'static str>,
    /// Deprecation state projected onto this exact inbound Message Object.
    pub deprecated: bool,
    /// Static JSON examples; parsing and bounded canonicalization happen at build time.
    pub examples: Vec<&'static str>,
    /// Exact payload schema/content authority.
    pub payload: QueueAsyncApiPayload,
}

#[cfg(feature = "asyncapi")]
impl QueueAsyncApiRegistration {
    /// Creates an unspecified registration with no documentation allocation.
    #[must_use]
    pub fn unspecified() -> Self {
        Self {
            status: QueueAsyncApiStatus::Unspecified,
            summary: None,
            description: None,
            operation_id: None,
            tags: Vec::new(),
            security: Vec::new(),
            deprecated: false,
            examples: Vec::new(),
            payload: QueueAsyncApiPayload::Unspecified,
        }
    }

    /// Creates an intentionally skipped registration.
    #[must_use]
    pub fn skipped() -> Self {
        Self {
            status: QueueAsyncApiStatus::Skipped,
            ..Self::unspecified()
        }
    }
}

impl QueueHandlerInputContract {
    /// Constructs an input contract for registry metadata.
    #[must_use]
    pub const fn new(
        payload_kind: QueuePayloadKind,
        payload_type_name: Option<&'static str>,
    ) -> Self {
        Self {
            payload_kind,
            payload_type_name,
        }
    }
}

/// Exact immutable key selecting one handler from a physical queue's local
/// dispatch table.
///
/// Every linked [`QueueHandlerMetadata`] contributes one supported contract.
/// The consumer rejects two metadata records with the same key before broker
/// admission instead of allowing RabbitMQ competing-consumer selection to
/// choose a schema-incompatible handler.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueueHandlerDispatchKey {
    queue_name: &'static str,
    schema_version: u16,
    content_kind: &'static str,
}

impl QueueHandlerDispatchKey {
    /// Constructs the key for generated/runtime registry integration.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        queue_name: &'static str,
        schema_version: u16,
        content_kind: &'static str,
    ) -> Self {
        Self {
            queue_name,
            schema_version,
            content_kind,
        }
    }

    /// Physical queue identity participating in dispatch.
    #[must_use]
    pub const fn queue_name(self) -> &'static str {
        self.queue_name
    }

    /// Exact positive message schema version accepted by the handler.
    #[must_use]
    pub const fn schema_version(self) -> u16 {
        self.schema_version
    }

    /// Exact canonical content-kind token accepted by the handler.
    #[must_use]
    pub const fn content_kind(self) -> &'static str {
        self.content_kind
    }
}

/// Queue handler metadata for compile-time registration
///
/// Contains all information needed to register and invoke a queue handler method.
/// This metadata is collected at compile-time via proc-macros and stored in a
/// distributed slice for automatic discovery.
#[derive(Clone)]
pub struct QueueHandlerMetadata {
    /// Unique type identifier for the service that owns this handler
    pub service_type_id: TypeId,

    /// Human-readable service type name for debugging
    pub service_type_name: &'static str,

    /// Component kind declared by a specialized worker derive.
    /// Generic queue handlers leave this as `None`.
    pub component_kind: Option<&'static str>,

    /// Queue name to listen on (e.g., "user.created")
    pub queue_name: &'static str,

    /// Method name of the handler function
    pub method_name: &'static str,

    /// Stable fully-qualified handler identity used by diagnostics and schemas.
    pub handler_name: &'static str,

    /// Lily queue envelope schema expected by this handler
    pub schema_version: u16,

    /// Exact Lily content-kind token accepted by this handler.
    pub content_kind: &'static str,

    /// Database-neutral processing guarantee requested by this handler.
    pub delivery_guarantee: DeliveryGuarantee,

    /// Typed parts/payload contract inferred from the handler signature.
    pub input_contract: QueueHandlerInputContract,

    /// Effective feature-gated AsyncAPI metadata for this exact handler.
    #[cfg(feature = "asyncapi")]
    pub asyncapi: QueueAsyncApiRegistration,

    /// Ordered middleware inherited from the owning `#[queue_service]` impl.
    pub service_middlewares: Vec<QueueMiddlewareRegistration>,

    /// Ordered guards inherited from the owning `#[queue_service]` impl.
    pub service_guards: Vec<QueueGuardRegistration>,

    /// Ordered middleware declared on this handler method.
    pub handler_middlewares: Vec<QueueMiddlewareRegistration>,

    /// Ordered guards declared on this handler method.
    pub handler_guards: Vec<QueueGuardRegistration>,

    /// Type-erased handler function
    pub handler_fn: HandlerFunction,
}

impl QueueHandlerMetadata {
    /// Returns the exact immutable local-dispatch contract represented by this
    /// metadata record.
    #[must_use]
    pub const fn dispatch_key(&self) -> QueueHandlerDispatchKey {
        QueueHandlerDispatchKey::new(self.queue_name, self.schema_version, self.content_kind)
    }
}

/// Distributed slice for metadata getter functions (compile-time collection)
///
/// This allows queue_service macros to automatically register handlers at compile time.
/// Each handler gets a getter function that returns its metadata, and all getters
/// are collected into this distributed slice via linkme.
#[linkme::distributed_slice]
pub static QUEUE_HANDLER_GETTERS: [fn() -> &'static QueueHandlerMetadata] = [..];

/// Collect metadata from every generated getter in this linked binary.
#[doc(hidden)]
pub fn get_all_queue_handlers() -> Vec<&'static QueueHandlerMetadata> {
    QUEUE_HANDLER_GETTERS
        .iter()
        .map(|getter| getter())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_function_signature() {
        // Verify HandlerFunction type is correctly defined
        fn _test_handler<'a>(
            _service: Arc<dyn std::any::Any + Send + Sync>,
            _invocation: &'a mut (dyn std::any::Any + Send),
        ) -> Pin<Box<dyn Future<Output = Result<(), QueueHandlerError>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        let _handler: HandlerFunction = _test_handler;
    }

    #[test]
    fn input_contract_preserves_payload_authority_metadata() {
        let contract = QueueHandlerInputContract::new(
            QueuePayloadKind::Json,
            Some("lily_queue::Json<OrderCreated>"),
        );

        assert_eq!(contract.payload_kind, QueuePayloadKind::Json);
        assert_eq!(
            contract.payload_type_name,
            Some("lily_queue::Json<OrderCreated>")
        );
    }

    #[test]
    fn dispatch_key_preserves_the_exact_supported_contract() {
        let key = QueueHandlerDispatchKey::new("orders.created", 65535, "application.protobuf");

        assert_eq!(key.queue_name(), "orders.created");
        assert_eq!(key.schema_version(), u16::MAX);
        assert_eq!(key.content_kind(), "application.protobuf");
        assert_ne!(
            key,
            QueueHandlerDispatchKey::new("orders.created", 65534, "application.protobuf")
        );
        assert_ne!(
            key,
            QueueHandlerDispatchKey::new("orders.created", 65535, "json")
        );
    }

    #[test]
    fn dispatch_key_forms_an_exact_supported_contract_set() {
        let mut supported = std::collections::HashSet::new();

        assert!(supported.insert(QueueHandlerDispatchKey::new("orders.created", 1, "json")));
        assert!(supported.insert(QueueHandlerDispatchKey::new("orders.created", 2, "json")));
        assert!(supported.insert(QueueHandlerDispatchKey::new("orders.created", 1, "binary")));
        assert!(!supported.insert(QueueHandlerDispatchKey::new("orders.created", 1, "json")));
    }

    #[test]
    fn delivery_outcome_preserves_only_the_typed_failure_contract() {
        let outcome = QueueDeliveryOutcome::Failed {
            class: QueueHandlerFailureClass::Retryable,
            code: "DEPENDENCY_UNAVAILABLE",
        };

        assert_eq!(
            outcome.failure_class(),
            Some(QueueHandlerFailureClass::Retryable)
        );
        assert_eq!(outcome.failure_code(), Some("DEPENDENCY_UNAVAILABLE"));
        assert!(!outcome.is_success());
        assert!(QueueDeliveryOutcome::Succeeded.is_success());
    }

    #[test]
    fn pipeline_initialization_errors_are_source_free_and_stable() {
        assert_eq!(
            QueuePipelineComponentInitError::ScopeRequired.diagnostic_code(),
            "QUEUE_PIPELINE_SCOPE_REQUIRED"
        );
        assert_eq!(
            QueuePipelineComponentInitError::Internal.to_string(),
            "QUEUE_PIPELINE_INITIALIZATION_FAILED"
        );
    }

    #[test]
    fn injection_failures_map_to_bounded_pipeline_initialization_categories() {
        let missing = InjectionError::MissingDependency {
            service: "sensitive::AuditMiddleware".to_owned(),
            dependency_type_id: TypeId::of::<String>(),
        };
        assert_eq!(
            QueuePipelineComponentInitError::from(missing),
            QueuePipelineComponentInitError::MissingDependency
        );

        let scope = InjectionError::ScopeRequired {
            service: "sensitive::RequestState".to_owned(),
        };
        assert_eq!(
            QueuePipelineComponentInitError::from(scope),
            QueuePipelineComponentInitError::ScopeRequired
        );

        let nested_missing = InjectionError::DependencyResolutionFailed {
            service: "sensitive::AuditMiddleware".to_owned(),
            dependency: "sensitive::AuditStore".to_owned(),
            source: Box::new(InjectionError::ServiceNotFound(
                "sensitive registration detail".to_owned(),
            )),
        };
        let mapped = QueuePipelineComponentInitError::from(nested_missing);
        assert_eq!(mapped, QueuePipelineComponentInitError::MissingDependency);
        assert_eq!(mapped.to_string(), "QUEUE_PIPELINE_DEPENDENCY_MISSING");

        let unrelated = InjectionError::InitError("sensitive source".to_owned());
        assert_eq!(
            QueuePipelineComponentInitError::from(unrelated),
            QueuePipelineComponentInitError::Internal
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn asyncapi_registration_defaults_are_explicit_and_allocation_free() {
        let unspecified = QueueAsyncApiRegistration::unspecified();
        assert_eq!(unspecified.status, QueueAsyncApiStatus::Unspecified);
        assert!(unspecified.summary.is_none());
        assert!(unspecified.tags.is_empty());
        assert!(matches!(
            unspecified.payload,
            QueueAsyncApiPayload::Unspecified
        ));

        let skipped = QueueAsyncApiRegistration::skipped();
        assert_eq!(skipped.status, QueueAsyncApiStatus::Skipped);
        assert!(matches!(skipped.payload, QueueAsyncApiPayload::Unspecified));
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn built_in_payload_content_types_are_canonical() {
        assert!(matches!(
            QueueAsyncApiPayload::text(),
            QueueAsyncApiPayload::Text {
                content_type: "text/plain; charset=utf-8"
            }
        ));
        assert!(matches!(
            QueueAsyncApiPayload::binary(),
            QueueAsyncApiPayload::Binary {
                content_type: "application/octet-stream"
            }
        ));
        assert!(matches!(
            QueueAsyncApiPayload::opaque("application/x-order"),
            QueueAsyncApiPayload::Opaque {
                content_type: "application/x-order"
            }
        ));
        assert!(matches!(
            QueueAsyncApiPayload::explicit_generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<String>(),
                "application/x-order+json",
            ),
            QueueAsyncApiPayload::ExplicitGenerated {
                content_type: "application/x-order+json",
                ..
            }
        ));
    }
}
