use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use lily_injection::Extensions;
use linkme::distributed_slice;

#[cfg(test)]
use crate::codec::LilyEnvelopeCodec;
use crate::codec::{
    WebSocketCodecFactory, WebSocketCodecInitError, WebSocketFrameCodec, WebSocketPayloadCodec,
};
use crate::guard::{GuardInitializationError, WsGuard};
use crate::middleware::{
    CompiledWsConnectionChain, CompiledWsHandshakeChain, CompiledWsIdentityMiddleware,
    CompiledWsMessageChain, WebSocketHandshakeMiddleware, WebSocketIdentityMiddleware,
    WsConnectionMiddleware, WsMessageMiddleware, WsMiddlewareInitError, WsMiddlewareObserver,
};
#[cfg(test)]
use crate::middleware::{MiddlewareDescriptor, WsMiddlewareObservationOutcome};
use crate::request::{MAX_CANONICAL_EVENT_BYTES, MAX_CANONICAL_NAMESPACE_BYTES};

use super::{
    WebSocketControllerBindingError, WebSocketControllerInitError,
    WebSocketControllerMaterializationError, WebSocketControllerTrait, WebSocketLifecycleError,
};
use crate::extractor::{WebSocketLifecycleInvocation, WebSocketMessageInvocation};
use crate::outcome::{PendingWebSocketActionOutcome, WebSocketActionError};

/// App-local erased controller value used only while binding operations.
#[doc(hidden)]
pub type ErasedWebSocketController = Arc<dyn Any + Send + Sync>;

/// Typed WebSocket message action future.
#[doc(hidden)]
pub type WebSocketActionFuture = Pin<
    Box<
        dyn Future<Output = Result<PendingWebSocketActionOutcome, WebSocketActionError>>
            + Send
            + 'static,
    >,
>;

/// Typed lifecycle action future used by connected/disconnected adapters.
#[doc(hidden)]
pub type WebSocketLifecycleFuture =
    Pin<Box<dyn Future<Output = Result<(), WebSocketLifecycleError>> + Send + 'static>>;

/// Object-safe message adapter generated for one controller method.
#[doc(hidden)]
pub trait WebSocketMessageAction: Send + Sync {
    /// Invoke the bound typed controller method without a runtime downcast.
    fn call(&self, invocation: WebSocketMessageInvocation) -> WebSocketActionFuture;
}

/// Object-safe lifecycle adapter generated for one controller method.
#[doc(hidden)]
pub trait WebSocketLifecycleAction: Send + Sync {
    /// Invoke the bound connected/disconnected controller method.
    fn call(&self, invocation: WebSocketLifecycleInvocation) -> WebSocketLifecycleFuture;
}

/// Ready message handler retaining exactly one app-owned controller instance.
#[doc(hidden)]
pub type WebSocketActionHandler = Arc<dyn WebSocketMessageAction>;

/// Ready lifecycle handler retaining exactly one app-owned controller instance.
#[doc(hidden)]
pub type WebSocketLifecycleHandler = Arc<dyn WebSocketLifecycleAction>;

/// Kind of a statically registered controller operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[doc(hidden)]
pub enum WebSocketOperationKind {
    /// A routed `namespace:event` message action.
    Message,
    /// The controller's optional connection-open hook.
    Connected,
    /// The controller's optional connection-close hook.
    Disconnected,
}

impl WebSocketOperationKind {
    /// Stable lowercase metadata name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
        }
    }
}

/// Controller-bound handler returned by a generated build-time binder.
#[derive(Clone)]
#[doc(hidden)]
pub enum BoundWebSocketOperation {
    /// Ready message handler.
    Message(WebSocketActionHandler),
    /// Ready connection-open handler.
    Connected(WebSocketLifecycleHandler),
    /// Ready connection-close handler.
    Disconnected(WebSocketLifecycleHandler),
}

impl BoundWebSocketOperation {
    fn kind(&self) -> WebSocketOperationKind {
        match self {
            Self::Message(_) => WebSocketOperationKind::Message,
            Self::Connected(_) => WebSocketOperationKind::Connected,
            Self::Disconnected(_) => WebSocketOperationKind::Disconnected,
        }
    }
}

type HandshakeMiddlewareInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<Arc<dyn WebSocketHandshakeMiddleware>, WsMiddlewareInitError>>
            + Send
            + 'static,
    >,
>;

/// Fallible app-build registration for one async handshake middleware.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct WebSocketHandshakeMiddlewareRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> HandshakeMiddlewareInitializationFuture,
}

impl WebSocketHandshakeMiddlewareRegistration {
    /// Registers one DI-aware pre-upgrade middleware type.
    pub fn of<M>() -> Self
    where
        M: WebSocketHandshakeMiddleware,
    {
        Self {
            type_id: TypeId::of::<M>(),
            type_name: std::any::type_name::<M>(),
            initialize: initialize_handshake_middleware::<M>,
        }
    }

    /// Registered concrete type identity.
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    /// Fully qualified concrete type name.
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }

    async fn instantiate(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<Arc<dyn WebSocketHandshakeMiddleware>, WsMiddlewareInitError> {
        (self.initialize)(extensions).await
    }
}

impl std::fmt::Debug for WebSocketHandshakeMiddlewareRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketHandshakeMiddlewareRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

fn initialize_handshake_middleware<M>(
    extensions: Arc<Extensions>,
) -> HandshakeMiddlewareInitializationFuture
where
    M: WebSocketHandshakeMiddleware,
{
    Box::pin(async move {
        M::new(extensions)
            .await
            .map(|middleware| Arc::new(middleware) as Arc<dyn WebSocketHandshakeMiddleware>)
    })
}

type ConnectionMiddlewareInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<Arc<dyn WsConnectionMiddleware>, WsMiddlewareInitError>>
            + Send
            + 'static,
    >,
>;

/// Fallible app-build registration for one connection lifecycle middleware.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct WebSocketConnectionMiddlewareRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> ConnectionMiddlewareInitializationFuture,
}

impl WebSocketConnectionMiddlewareRegistration {
    /// Registers one DI-aware post-upgrade middleware type.
    pub fn of<M>() -> Self
    where
        M: WsConnectionMiddleware,
    {
        Self {
            type_id: TypeId::of::<M>(),
            type_name: std::any::type_name::<M>(),
            initialize: initialize_connection_middleware::<M>,
        }
    }

    /// Registered concrete middleware identity.
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    /// Fully qualified concrete middleware type name.
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }

    async fn instantiate(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<Arc<dyn WsConnectionMiddleware>, WsMiddlewareInitError> {
        (self.initialize)(extensions).await
    }
}

impl std::fmt::Debug for WebSocketConnectionMiddlewareRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketConnectionMiddlewareRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

fn initialize_connection_middleware<M>(
    extensions: Arc<Extensions>,
) -> ConnectionMiddlewareInitializationFuture
where
    M: WsConnectionMiddleware,
{
    Box::pin(async move {
        M::new(extensions)
            .await
            .map(|middleware| Arc::new(middleware) as Arc<dyn WsConnectionMiddleware>)
    })
}

type IdentityMiddlewareInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<Arc<dyn WebSocketIdentityMiddleware>, WsMiddlewareInitError>>
            + Send
            + 'static,
    >,
>;

/// Fallible app-build registration for the optional application identity slot.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct WebSocketIdentityMiddlewareRegistration {
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> IdentityMiddlewareInitializationFuture,
}

impl WebSocketIdentityMiddlewareRegistration {
    /// Registers one application-owned async identity middleware type.
    pub fn of<M>() -> Self
    where
        M: WebSocketIdentityMiddleware,
    {
        Self {
            type_name: std::any::type_name::<M>(),
            initialize: initialize_identity_middleware::<M>,
        }
    }

    async fn instantiate(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<Arc<dyn WebSocketIdentityMiddleware>, WsMiddlewareInitError> {
        (self.initialize)(extensions).await
    }
}

impl std::fmt::Debug for WebSocketIdentityMiddlewareRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketIdentityMiddlewareRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

fn initialize_identity_middleware<M>(
    extensions: Arc<Extensions>,
) -> IdentityMiddlewareInitializationFuture
where
    M: WebSocketIdentityMiddleware,
{
    Box::pin(async move {
        M::new(extensions)
            .await
            .map(|middleware| Arc::new(middleware) as Arc<dyn WebSocketIdentityMiddleware>)
    })
}

type MessageMiddlewareInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<Arc<dyn WsMessageMiddleware>, WsMiddlewareInitError>>
            + Send
            + 'static,
    >,
>;

/// Fallible app-build registration for one typed message middleware.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct WebSocketMessageMiddlewareRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> MessageMiddlewareInitializationFuture,
}

impl WebSocketMessageMiddlewareRegistration {
    /// Register one current-generation message middleware type.
    pub fn of<M>() -> Self
    where
        M: WsMessageMiddleware,
    {
        Self {
            type_id: TypeId::of::<M>(),
            type_name: std::any::type_name::<M>(),
            initialize: initialize_message_middleware::<M>,
        }
    }

    /// Registered concrete middleware identity.
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    /// Fully qualified concrete middleware type name.
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }

    async fn instantiate(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<Arc<dyn WsMessageMiddleware>, WsMiddlewareInitError> {
        (self.initialize)(extensions).await
    }
}

impl std::fmt::Debug for WebSocketMessageMiddlewareRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketMessageMiddlewareRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

fn initialize_message_middleware<M>(
    extensions: Arc<Extensions>,
) -> MessageMiddlewareInitializationFuture
where
    M: WsMessageMiddleware,
{
    Box::pin(async move {
        M::new(extensions)
            .await
            .map(|middleware| Arc::new(middleware) as Arc<dyn WsMessageMiddleware>)
    })
}

type ErasedWebSocketCodec = Arc<dyn Any + Send + Sync>;
type CodecInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<ErasedWebSocketCodec, WebSocketCodecInitError>> + Send + 'static,
    >,
>;

/// Build-time registration for a controller/app frame codec.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct WebSocketFrameCodecRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> CodecInitializationFuture,
    bind: fn(ErasedWebSocketCodec) -> Option<Arc<dyn WebSocketFrameCodec>>,
}

impl WebSocketFrameCodecRegistration {
    /// Describe a DI-aware concrete frame codec without constructing it.
    pub fn of<C>() -> Self
    where
        C: WebSocketFrameCodec,
    {
        Self {
            type_id: TypeId::of::<C>(),
            type_name: std::any::type_name::<C>(),
            initialize: initialize_codec::<C>,
            bind: bind_frame_codec::<C>,
        }
    }

    /// Registered concrete codec identity.
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    /// Fully qualified concrete codec type.
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }
}

impl std::fmt::Debug for WebSocketFrameCodecRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketFrameCodecRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

/// Build-time registration for an action/controller/app payload codec.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct WebSocketPayloadCodecRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> CodecInitializationFuture,
    bind: fn(ErasedWebSocketCodec) -> Option<Arc<dyn WebSocketPayloadCodec>>,
}

impl WebSocketPayloadCodecRegistration {
    /// Describe a DI-aware concrete payload codec without constructing it.
    pub fn of<C>() -> Self
    where
        C: WebSocketPayloadCodec,
    {
        Self {
            type_id: TypeId::of::<C>(),
            type_name: std::any::type_name::<C>(),
            initialize: initialize_codec::<C>,
            bind: bind_payload_codec::<C>,
        }
    }

    /// Registered concrete codec identity.
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    /// Fully qualified concrete codec type.
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }
}

impl std::fmt::Debug for WebSocketPayloadCodecRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketPayloadCodecRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

fn initialize_codec<C>(extensions: Arc<Extensions>) -> CodecInitializationFuture
where
    C: WebSocketCodecFactory,
{
    Box::pin(async move {
        C::new(extensions)
            .await
            .map(|codec| Arc::new(codec) as ErasedWebSocketCodec)
    })
}

fn bind_frame_codec<C>(codec: ErasedWebSocketCodec) -> Option<Arc<dyn WebSocketFrameCodec>>
where
    C: WebSocketFrameCodec,
{
    codec
        .downcast::<C>()
        .ok()
        .map(|codec| codec as Arc<dyn WebSocketFrameCodec>)
}

fn bind_payload_codec<C>(codec: ErasedWebSocketCodec) -> Option<Arc<dyn WebSocketPayloadCodec>>
where
    C: WebSocketPayloadCodec,
{
    codec
        .downcast::<C>()
        .ok()
        .map(|codec| codec as Arc<dyn WebSocketPayloadCodec>)
}

type GuardInitializationFuture = Pin<
    Box<dyn Future<Output = Result<Arc<dyn WsGuard>, GuardInitializationError>> + Send + 'static>,
>;

/// Fallible app-build guard registration emitted by controller/action macros.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct WebSocketGuardRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> GuardInitializationFuture,
}

impl WebSocketGuardRegistration {
    /// Register a concrete current-generation WebSocket guard.
    pub fn of<G>() -> Self
    where
        G: WsGuard + 'static,
    {
        Self {
            type_id: TypeId::of::<G>(),
            type_name: std::any::type_name::<G>(),
            initialize: initialize_guard::<G>,
        }
    }

    /// Registered concrete guard identity.
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    /// Fully qualified concrete guard type name.
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }

    async fn instantiate(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<Arc<dyn WsGuard>, GuardInitializationError> {
        (self.initialize)(extensions).await
    }
}

impl std::fmt::Debug for WebSocketGuardRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketGuardRegistration")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

fn initialize_guard<G>(extensions: Arc<Extensions>) -> GuardInitializationFuture
where
    G: WsGuard + 'static,
{
    Box::pin(async move {
        G::new(extensions)
            .await
            .map(|guard| Arc::new(guard) as Arc<dyn WsGuard>)
    })
}

/// Documentation intent carried by runtime operation metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[doc(hidden)]
pub enum WebSocketAsyncApiStatus {
    /// No explicit documentation decision was made.
    #[default]
    Unspecified,
    /// Metadata should be projected when AsyncAPI is enabled.
    Documented,
    /// The operation is deliberately excluded from AsyncAPI.
    Skipped,
}

/// Typed, side-effect-free AsyncAPI metadata retained with a registration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct WebSocketAsyncApiRegistration {
    status: WebSocketAsyncApiStatus,
    summary: Option<&'static str>,
    description: Option<&'static str>,
    operation_id: Option<&'static str>,
    tags: Vec<&'static str>,
    security: Vec<&'static str>,
    deprecated: bool,
}

impl WebSocketAsyncApiRegistration {
    /// No explicit documentation intent.
    pub fn unspecified() -> Self {
        Self::new(WebSocketAsyncApiStatus::Unspecified)
    }

    /// Explicitly include the controller/operation in AsyncAPI.
    pub fn documented() -> Self {
        Self::new(WebSocketAsyncApiStatus::Documented)
    }

    /// Explicitly exclude the controller/operation from AsyncAPI.
    pub fn skipped() -> Self {
        Self::new(WebSocketAsyncApiStatus::Skipped)
    }

    fn new(status: WebSocketAsyncApiStatus) -> Self {
        Self {
            status,
            summary: None,
            description: None,
            operation_id: None,
            tags: Vec::new(),
            security: Vec::new(),
            deprecated: false,
        }
    }

    /// Add one deterministic tag. Duplicate tags are ignored.
    pub fn with_tag(mut self, tag: &'static str) -> Self {
        if !self.tags.contains(&tag) {
            self.tags.push(tag);
        }
        self
    }

    /// Set a human-readable operation summary.
    pub const fn with_summary(mut self, summary: &'static str) -> Self {
        self.summary = Some(summary);
        self
    }

    /// Set a human-readable operation description.
    pub const fn with_description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }

    /// Set an explicit stable operation identifier.
    pub const fn with_operation_id(mut self, operation_id: &'static str) -> Self {
        self.operation_id = Some(operation_id);
        self
    }

    /// Mark the operation deprecated or active.
    pub const fn with_deprecated(mut self, deprecated: bool) -> Self {
        self.deprecated = deprecated;
        self
    }

    /// Add one named security requirement. Duplicate names are ignored.
    pub fn with_security(mut self, security: &'static str) -> Self {
        if !self.security.contains(&security) {
            self.security.push(security);
        }
        self
    }

    /// Explicit documentation status.
    pub const fn status(&self) -> WebSocketAsyncApiStatus {
        self.status
    }

    /// Summary supplied by this metadata level.
    pub const fn summary(&self) -> Option<&'static str> {
        self.summary
    }

    /// Description supplied by this metadata level.
    pub const fn description(&self) -> Option<&'static str> {
        self.description
    }

    /// Explicit operation ID supplied by this metadata level.
    pub const fn operation_id(&self) -> Option<&'static str> {
        self.operation_id
    }

    /// Deterministic, duplicate-free tag list.
    pub fn tags(&self) -> &[&'static str] {
        &self.tags
    }

    /// Deterministic, duplicate-free security requirement names.
    pub fn security(&self) -> &[&'static str] {
        &self.security
    }

    /// Whether this operation is marked deprecated.
    pub const fn deprecated(&self) -> bool {
        self.deprecated
    }
}

impl Default for WebSocketAsyncApiRegistration {
    fn default() -> Self {
        Self::unspecified()
    }
}

/// Controller-level metadata generated by `#[derive(WebSocketController)]`.
#[doc(hidden)]
pub trait WebSocketControllerDefinition: WebSocketControllerTrait {
    /// Exact controller namespace.
    fn namespace() -> &'static str;

    /// Controller-specific async pre-upgrade middleware types.
    fn handshake_middleware_registrations() -> Vec<WebSocketHandshakeMiddlewareRegistration>;

    /// Controller-specific post-upgrade connection middleware types.
    fn connection_middleware_registrations() -> Vec<WebSocketConnectionMiddlewareRegistration>;

    /// Controller-level message middleware types.
    fn message_middleware_registrations() -> Vec<WebSocketMessageMiddlewareRegistration>;

    /// Controller-level message guards.
    fn guard_registrations() -> Vec<WebSocketGuardRegistration>;

    /// Optional controller frame codec selected before route lookup.
    fn frame_codec_registration() -> Option<WebSocketFrameCodecRegistration> {
        None
    }

    /// Optional controller payload codec inherited by message actions.
    fn payload_codec_registration() -> Option<WebSocketPayloadCodecRegistration> {
        None
    }

    /// Default timeout for each complete message pipeline in this controller.
    /// Connection lifecycle hooks use their own operation/server cap.
    fn timeout() -> Option<Duration>;

    /// Controller documentation defaults.
    fn asyncapi_registration() -> WebSocketAsyncApiRegistration;
}

#[derive(Debug, Clone)]
struct WebSocketControllerMetadata {
    namespace: &'static str,
    handshake_middleware: Vec<WebSocketHandshakeMiddlewareRegistration>,
    connection_middleware: Vec<WebSocketConnectionMiddlewareRegistration>,
    message_middleware: Vec<WebSocketMessageMiddlewareRegistration>,
    guards: Vec<WebSocketGuardRegistration>,
    frame_codec: Option<WebSocketFrameCodecRegistration>,
    payload_codec: Option<WebSocketPayloadCodecRegistration>,
    timeout: Option<Duration>,
    asyncapi: WebSocketAsyncApiRegistration,
}

type ControllerInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<ErasedWebSocketController, WebSocketControllerInitError>>
            + Send
            + 'static,
    >,
>;

/// Static controller constructor and definition metadata.
#[derive(Clone)]
#[doc(hidden)]
pub struct WebSocketControllerRegistration {
    type_id: TypeId,
    type_name: &'static str,
    metadata: WebSocketControllerMetadata,
    initialize: fn(Arc<Extensions>) -> ControllerInitializationFuture,
}

impl WebSocketControllerRegistration {
    /// Create registration metadata for a derived controller type.
    pub fn of<C>() -> Self
    where
        C: WebSocketControllerDefinition,
    {
        Self {
            type_id: TypeId::of::<C>(),
            type_name: std::any::type_name::<C>(),
            metadata: WebSocketControllerMetadata {
                namespace: C::namespace(),
                handshake_middleware: C::handshake_middleware_registrations(),
                connection_middleware: C::connection_middleware_registrations(),
                message_middleware: C::message_middleware_registrations(),
                guards: C::guard_registrations(),
                frame_codec: C::frame_codec_registration(),
                payload_codec: C::payload_codec_registration(),
                timeout: C::timeout(),
                asyncapi: C::asyncapi_registration(),
            },
            initialize: initialize_controller::<C>,
        }
    }

    /// Concrete controller identity.
    pub const fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// Fully qualified concrete controller type.
    pub const fn type_name(&self) -> &'static str {
        self.type_name
    }

    /// Exact controller namespace.
    pub const fn namespace(&self) -> &'static str {
        self.metadata.namespace
    }
}

impl std::fmt::Debug for WebSocketControllerRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketControllerRegistration")
            .field("type_name", &self.type_name)
            .field("namespace", &self.metadata.namespace)
            .finish_non_exhaustive()
    }
}

fn initialize_controller<C>(extensions: Arc<Extensions>) -> ControllerInitializationFuture
where
    C: WebSocketControllerDefinition,
{
    Box::pin(async move {
        match AssertUnwindSafe(C::new(extensions)).catch_unwind().await {
            Ok(Ok(controller)) => Ok(Arc::new(controller) as ErasedWebSocketController),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(WebSocketControllerInitError::Internal),
        }
    })
}

/// Generated operation-specific app-build binder.
#[doc(hidden)]
pub type WebSocketActionBinder =
    fn(
        ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError>;

/// Static operation/controller binding metadata.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct WebSocketActionRegistration {
    controller_type_id: TypeId,
    controller_type_name: &'static str,
    bind: WebSocketActionBinder,
}

impl WebSocketActionRegistration {
    /// Bind one generated operation adapter to controller type `C`.
    pub fn of<C>(bind: WebSocketActionBinder) -> Self
    where
        C: WebSocketControllerTrait,
    {
        Self {
            controller_type_id: TypeId::of::<C>(),
            controller_type_name: std::any::type_name::<C>(),
            bind,
        }
    }

    /// Controller TypeId required by this operation.
    pub const fn controller_type_id(self) -> TypeId {
        self.controller_type_id
    }

    /// Fully qualified controller type required by this operation.
    pub const fn controller_type_name(self) -> &'static str {
        self.controller_type_name
    }

    fn bind(
        self,
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        (self.bind)(controller)
    }
}

/// Build-time downcast used exactly once by a generated operation binder.
#[doc(hidden)]
pub fn downcast_websocket_controller<C>(
    controller: ErasedWebSocketController,
) -> Result<Arc<C>, WebSocketControllerBindingError>
where
    C: WebSocketControllerTrait,
{
    controller
        .downcast::<C>()
        .map_err(|_| WebSocketControllerBindingError::type_mismatch(std::any::type_name::<C>()))
}

/// Action-level metadata generated by `#[websocket_controller]`.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct WebSocketOperationMetadata {
    message_middleware: Vec<WebSocketMessageMiddlewareRegistration>,
    guards: Vec<WebSocketGuardRegistration>,
    payload_codec: Option<WebSocketPayloadCodecRegistration>,
    timeout: Option<Duration>,
    asyncapi: WebSocketAsyncApiRegistration,
}

impl WebSocketOperationMetadata {
    /// Construct complete static metadata for one operation.
    pub fn new(
        message_middleware: Vec<WebSocketMessageMiddlewareRegistration>,
        guards: Vec<WebSocketGuardRegistration>,
        payload_codec: Option<WebSocketPayloadCodecRegistration>,
        timeout: Option<Duration>,
        asyncapi: WebSocketAsyncApiRegistration,
    ) -> Self {
        Self {
            message_middleware,
            guards,
            payload_codec,
            timeout,
            asyncapi,
        }
    }

    /// Empty metadata used by operations without optional attributes.
    pub fn empty() -> Self {
        Self::new(Vec::new(), Vec::new(), None, None, Default::default())
    }
}

impl Default for WebSocketOperationMetadata {
    fn default() -> Self {
        Self::empty()
    }
}

/// Static operation definition awaiting one app's controller instance.
#[derive(Clone)]
#[doc(hidden)]
pub struct PendingWebSocketOperation {
    kind: WebSocketOperationKind,
    event: Option<&'static str>,
    handler_name: &'static str,
    action_registration: WebSocketActionRegistration,
    metadata: WebSocketOperationMetadata,
}

impl PendingWebSocketOperation {
    /// Construct one pending message or lifecycle operation.
    pub fn new(
        kind: WebSocketOperationKind,
        event: Option<&'static str>,
        handler_name: &'static str,
        action_registration: WebSocketActionRegistration,
        metadata: WebSocketOperationMetadata,
    ) -> Self {
        Self {
            kind,
            event,
            handler_name,
            action_registration,
            metadata,
        }
    }

    /// Declared operation kind.
    pub const fn kind(&self) -> WebSocketOperationKind {
        self.kind
    }

    /// Local event for message operations.
    pub const fn event(&self) -> Option<&'static str> {
        self.event
    }

    /// Stable generated `module::Controller::method` identity.
    pub const fn handler_name(&self) -> &'static str {
        self.handler_name
    }
}

/// Function emitted once for every derived WebSocket controller.
#[doc(hidden)]
pub type WebSocketControllerRegistrationFn = fn() -> WebSocketControllerRegistration;

/// Immutable link-time controller metadata. It never stores live app values.
#[distributed_slice]
#[doc(hidden)]
pub static WEBSOCKET_CONTROLLER_REGISTRATIONS: [WebSocketControllerRegistrationFn] = [..];

/// Function emitted once for every controller operation.
#[doc(hidden)]
pub type PendingWebSocketOperationRegistrationFn = fn() -> PendingWebSocketOperation;

/// Immutable link-time operation metadata. It never stores live app values.
#[distributed_slice]
#[doc(hidden)]
pub static PENDING_WEBSOCKET_OPERATION_REGISTRATIONS: [PendingWebSocketOperationRegistrationFn] =
    [..];

/// Snapshot static controller definitions without mutating process state.
#[doc(hidden)]
pub fn get_websocket_controller_registrations()
-> Result<Vec<WebSocketControllerRegistration>, WebSocketControllerMaterializationError> {
    snapshot_controller_registrations(&WEBSOCKET_CONTROLLER_REGISTRATIONS)
}

fn snapshot_controller_registrations(
    registrations: &[WebSocketControllerRegistrationFn],
) -> Result<Vec<WebSocketControllerRegistration>, WebSocketControllerMaterializationError> {
    registrations
        .iter()
        .enumerate()
        .map(|(index, registration)| {
            catch_unwind(AssertUnwindSafe(registration)).map_err(|_| {
                WebSocketControllerMaterializationError::RegistrationPanicked {
                    registry: "controller",
                    index,
                }
            })
        })
        .collect()
}

/// Snapshot static operation definitions without mutating process state.
#[doc(hidden)]
pub fn get_pending_websocket_operations()
-> Result<Vec<PendingWebSocketOperation>, WebSocketControllerMaterializationError> {
    snapshot_pending_operations(&PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)
}

fn snapshot_pending_operations(
    registrations: &[PendingWebSocketOperationRegistrationFn],
) -> Result<Vec<PendingWebSocketOperation>, WebSocketControllerMaterializationError> {
    registrations
        .iter()
        .enumerate()
        .map(|(index, registration)| {
            catch_unwind(AssertUnwindSafe(registration)).map_err(|_| {
                WebSocketControllerMaterializationError::RegistrationPanicked {
                    registry: "operation",
                    index,
                }
            })
        })
        .collect()
}

/// One ready message action in the immutable app-local table.
pub(crate) struct MaterializedWebSocketAction {
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    handler_name: &'static str,
    handler: WebSocketActionHandler,
    message_middleware: Arc<CompiledWsMessageChain>,
    guards: Arc<[Arc<dyn WsGuard>]>,
    timeout: Option<Duration>,
    payload_codec: Arc<dyn WebSocketPayloadCodec>,
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    controller_asyncapi: WebSocketAsyncApiRegistration,
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    operation_asyncapi: WebSocketAsyncApiRegistration,
}

impl MaterializedWebSocketAction {
    /// Effective global -> controller -> action message-middleware plan.
    pub(crate) fn middleware_chain(&self) -> &CompiledWsMessageChain {
        &self.message_middleware
    }

    /// Effective global -> controller -> action guard plan.
    pub(crate) fn guards(&self) -> &[Arc<dyn WsGuard>] {
        &self.guards
    }

    /// Effective whole-message timeout (`action > controller`; app fallback excluded).
    pub(crate) const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Effective action -> controller -> app payload codec.
    pub(crate) fn payload_codec(&self) -> Arc<dyn WebSocketPayloadCodec> {
        Arc::clone(&self.payload_codec)
    }

    /// Controller documentation defaults retained for later projection.
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    pub(crate) const fn controller_asyncapi(&self) -> &WebSocketAsyncApiRegistration {
        &self.controller_asyncapi
    }

    /// Action-specific documentation metadata retained for later projection.
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    pub(crate) const fn operation_asyncapi(&self) -> &WebSocketAsyncApiRegistration {
        &self.operation_asyncapi
    }

    /// Invoke the immutable typed action adapter.
    pub(crate) async fn invoke(
        &self,
        invocation: WebSocketMessageInvocation,
    ) -> Result<PendingWebSocketActionOutcome, WebSocketActionError> {
        self.handler.call(invocation).await
    }
}

impl std::fmt::Debug for MaterializedWebSocketAction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MaterializedWebSocketAction")
            .field("handler_name", &self.handler_name)
            .field(
                "has_message_middleware",
                &!self.message_middleware.is_empty(),
            )
            .field("guards", &self.guards.len())
            .field("timeout", &self.timeout)
            .field("payload_codec", &"<app-owned>")
            .finish_non_exhaustive()
    }
}

/// One ready lifecycle hook retaining its controller and effective timeout.
#[derive(Clone)]
pub(crate) struct MaterializedWebSocketLifecycle {
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    handler_name: &'static str,
    handler: WebSocketLifecycleHandler,
    timeout: Option<Duration>,
}

impl MaterializedWebSocketLifecycle {
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    pub(crate) const fn handler_name(&self) -> &'static str {
        self.handler_name
    }

    pub(crate) fn handler(&self) -> WebSocketLifecycleHandler {
        Arc::clone(&self.handler)
    }

    pub(crate) const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}

/// Connected/disconnected hooks for one exact controller namespace.
#[derive(Clone, Default)]
pub(crate) struct WebSocketLifecycleHandlers {
    connected: Option<MaterializedWebSocketLifecycle>,
    disconnected: Option<MaterializedWebSocketLifecycle>,
}

impl WebSocketLifecycleHandlers {
    pub(crate) fn connected(&self) -> Option<WebSocketLifecycleHandler> {
        self.connected
            .as_ref()
            .map(MaterializedWebSocketLifecycle::handler)
    }

    pub(crate) fn disconnected(&self) -> Option<WebSocketLifecycleHandler> {
        self.disconnected
            .as_ref()
            .map(MaterializedWebSocketLifecycle::handler)
    }

    pub(crate) fn connected_operation(&self) -> Option<&MaterializedWebSocketLifecycle> {
        self.connected.as_ref()
    }

    pub(crate) fn disconnected_operation(&self) -> Option<&MaterializedWebSocketLifecycle> {
        self.disconnected.as_ref()
    }
}

struct MaterializedWebSocketController {
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    controller_type_name: &'static str,
    actions: HashMap<String, Arc<MaterializedWebSocketAction>>,
    lifecycle: WebSocketLifecycleHandlers,
    handshake_middleware: Arc<CompiledWsHandshakeChain>,
    connection_middleware: Arc<CompiledWsConnectionChain>,
    frame_codec: Arc<dyn WebSocketFrameCodec>,
    payload_codec: Arc<dyn WebSocketPayloadCodec>,
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    asyncapi: WebSocketAsyncApiRegistration,
}

/// Immutable, application-owned controller and operation table.
pub(crate) struct WebSocketActionTable {
    controllers: BTreeMap<String, MaterializedWebSocketController>,
    identity_middleware: Option<Arc<CompiledWsIdentityMiddleware>>,
    action_count: usize,
}

impl std::fmt::Debug for WebSocketActionTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketActionTable")
            .field("namespaces", &self.controllers.keys().collect::<Vec<_>>())
            .field("action_count", &self.action_count)
            .finish_non_exhaustive()
    }
}

impl WebSocketActionTable {
    /// Exact namespace admission; `/` is not a wildcard.
    pub(crate) fn contains_namespace(&self, namespace: &str) -> bool {
        self.controllers.contains_key(namespace)
    }

    /// Deterministically ordered exact controller namespaces.
    pub(crate) fn namespaces(&self) -> impl Iterator<Item = &str> {
        self.controllers.keys().map(String::as_str)
    }

    /// O(1) expected lookup within one exact namespace.
    pub(crate) fn find_action(
        &self,
        namespace: &str,
        event: &str,
    ) -> Option<Arc<MaterializedWebSocketAction>> {
        self.controllers
            .get(namespace)
            .and_then(|controller| controller.actions.get(event))
            .map(Arc::clone)
    }

    /// Lifecycle hooks for exactly one namespace, never a wildcard fan-out.
    pub(crate) fn lifecycle_for_namespace(
        &self,
        namespace: &str,
    ) -> Option<WebSocketLifecycleHandlers> {
        self.controllers
            .get(namespace)
            .map(|controller| controller.lifecycle.clone())
    }

    /// Effective global -> controller pre-upgrade middleware chain.
    pub(crate) fn handshake_middleware(
        &self,
        namespace: &str,
    ) -> Option<Arc<CompiledWsHandshakeChain>> {
        self.controllers
            .get(namespace)
            .map(|controller| Arc::clone(&controller.handshake_middleware))
    }

    /// Effective global -> controller post-upgrade connection chain.
    pub(crate) fn connection_middleware(
        &self,
        namespace: &str,
    ) -> Option<Arc<CompiledWsConnectionChain>> {
        self.controllers
            .get(namespace)
            .map(|controller| Arc::clone(&controller.connection_middleware))
    }

    /// Optional single application-owned identity middleware.
    pub(crate) fn identity_middleware(&self) -> Option<Arc<CompiledWsIdentityMiddleware>> {
        self.identity_middleware.as_ref().map(Arc::clone)
    }

    /// Effective controller -> app frame codec for pre-route decode.
    pub(crate) fn frame_codec(&self, namespace: &str) -> Option<Arc<dyn WebSocketFrameCodec>> {
        self.controllers
            .get(namespace)
            .map(|controller| Arc::clone(&controller.frame_codec))
    }

    /// Effective controller -> app payload codec for pre-action terminals.
    pub(crate) fn payload_codec(&self, namespace: &str) -> Option<Arc<dyn WebSocketPayloadCodec>> {
        self.controllers
            .get(namespace)
            .map(|controller| Arc::clone(&controller.payload_codec))
    }

    /// Number of materialized message actions.
    pub(crate) const fn action_count(&self) -> usize {
        self.action_count
    }

    /// Controller identity and AsyncAPI defaults retained for projection.
    #[allow(dead_code)] // Consumed by the CAP-ASYNC-01 projection.
    pub(crate) fn controller_metadata(
        &self,
        namespace: &str,
    ) -> Option<(&'static str, &WebSocketAsyncApiRegistration)> {
        self.controllers
            .get(namespace)
            .map(|controller| (controller.controller_type_name, &controller.asyncapi))
    }
}

struct PendingValidatedOperation {
    pending: PendingWebSocketOperation,
    registration: WebSocketControllerRegistration,
}

#[cfg(test)]
struct RegistryNoopWsMiddlewareObserver;

#[cfg(test)]
impl WsMiddlewareObserver for RegistryNoopWsMiddlewareObserver {
    fn observe(
        &self,
        _descriptor: MiddlewareDescriptor,
        _outcome: WsMiddlewareObservationOutcome,
        _elapsed: Duration,
    ) {
    }
}

/// Materialize static metadata for exactly one `WsApp` build.
///
/// Controller and guard instance maps are local to this call. Ready handlers
/// retain the controller `Arc`; invocation performs no global lookup, registry
/// lock or controller downcast.
#[cfg(test)]
pub(crate) async fn materialize_websocket_controllers(
    registrations: Vec<WebSocketControllerRegistration>,
    pending_operations: Vec<PendingWebSocketOperation>,
    extensions: Arc<Extensions>,
) -> Result<WebSocketActionTable, WebSocketControllerMaterializationError> {
    materialize_websocket_controllers_with_codecs(
        registrations,
        pending_operations,
        extensions,
        WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
        WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
    )
    .await
}

/// Materialize controllers using explicit app-default codec authorities.
#[cfg(test)]
pub(crate) async fn materialize_websocket_controllers_with_codecs(
    registrations: Vec<WebSocketControllerRegistration>,
    pending_operations: Vec<PendingWebSocketOperation>,
    extensions: Arc<Extensions>,
    app_frame_codec: WebSocketFrameCodecRegistration,
    app_payload_codec: WebSocketPayloadCodecRegistration,
) -> Result<WebSocketActionTable, WebSocketControllerMaterializationError> {
    materialize_websocket_controllers_with_pipeline(
        registrations,
        pending_operations,
        extensions,
        app_frame_codec,
        app_payload_codec,
        Vec::new(),
        Vec::new(),
        None,
        Vec::new(),
        Vec::new(),
        Arc::new(RegistryNoopWsMiddlewareObserver),
    )
    .await
}

/// Materialize controllers and compile immutable app/controller/action plans.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn materialize_websocket_controllers_with_pipeline(
    mut registrations: Vec<WebSocketControllerRegistration>,
    mut pending_operations: Vec<PendingWebSocketOperation>,
    extensions: Arc<Extensions>,
    app_frame_codec: WebSocketFrameCodecRegistration,
    app_payload_codec: WebSocketPayloadCodecRegistration,
    app_handshake_middlewares: Vec<WebSocketHandshakeMiddlewareRegistration>,
    app_connection_middlewares: Vec<WebSocketConnectionMiddlewareRegistration>,
    app_identity_middleware: Option<WebSocketIdentityMiddlewareRegistration>,
    app_message_middlewares: Vec<WebSocketMessageMiddlewareRegistration>,
    app_guards: Vec<WebSocketGuardRegistration>,
    middleware_observer: Arc<dyn WsMiddlewareObserver>,
) -> Result<WebSocketActionTable, WebSocketControllerMaterializationError> {
    lily_middleware::validate_middleware_count(app_handshake_middlewares.len()).map_err(
        |source| WebSocketControllerMaterializationError::InvalidMiddlewarePlan {
            operation: "application handshake",
            source,
        },
    )?;
    lily_middleware::validate_middleware_count(app_connection_middlewares.len()).map_err(
        |source| WebSocketControllerMaterializationError::InvalidMiddlewarePlan {
            operation: "application connection",
            source,
        },
    )?;
    validate_unique_types(
        "application",
        "global handshake middleware",
        app_handshake_middlewares
            .iter()
            .map(|middleware| (middleware.type_id, middleware.type_name)),
    )?;
    validate_unique_types(
        "application",
        "global connection middleware",
        app_connection_middlewares
            .iter()
            .map(|middleware| (middleware.type_id, middleware.type_name)),
    )?;
    lily_middleware::validate_middleware_count(app_message_middlewares.len()).map_err(
        |source| WebSocketControllerMaterializationError::InvalidMiddlewarePlan {
            operation: "application",
            source,
        },
    )?;
    validate_unique_types(
        "application",
        "global message middleware",
        app_message_middlewares
            .iter()
            .map(|middleware| (middleware.type_id, middleware.type_name)),
    )?;
    validate_unique_types(
        "application",
        "global guard",
        app_guards
            .iter()
            .map(|guard| (guard.type_id, guard.type_name)),
    )?;

    registrations.sort_by(|left, right| left.type_name.cmp(right.type_name));
    pending_operations.sort_by(|left, right| {
        left.action_registration
            .controller_type_name
            .cmp(right.action_registration.controller_type_name)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.event.cmp(&right.event))
            .then_with(|| left.handler_name.cmp(right.handler_name))
    });

    let mut registrations_by_type = HashMap::with_capacity(registrations.len());
    let mut namespace_owners = HashMap::<&'static str, &'static str>::new();
    for registration in &registrations {
        if registrations_by_type
            .insert(registration.type_id, registration.clone())
            .is_some()
        {
            return Err(
                WebSocketControllerMaterializationError::DuplicateRegistration {
                    controller: registration.type_name,
                },
            );
        }
        validate_controller_metadata(registration)?;
        let effective_handshake_count =
            app_handshake_middlewares.len() + registration.metadata.handshake_middleware.len();
        lily_middleware::validate_middleware_count(effective_handshake_count).map_err(
            |source| WebSocketControllerMaterializationError::InvalidMiddlewarePlan {
                operation: registration.type_name,
                source,
            },
        )?;
        validate_unique_types(
            registration.type_name,
            "effective handshake middleware",
            app_handshake_middlewares
                .iter()
                .chain(&registration.metadata.handshake_middleware)
                .map(|middleware| (middleware.type_id, middleware.type_name)),
        )?;
        let effective_connection_count =
            app_connection_middlewares.len() + registration.metadata.connection_middleware.len();
        lily_middleware::validate_middleware_count(effective_connection_count).map_err(
            |source| WebSocketControllerMaterializationError::InvalidMiddlewarePlan {
                operation: registration.type_name,
                source,
            },
        )?;
        validate_unique_types(
            registration.type_name,
            "effective connection middleware",
            app_connection_middlewares
                .iter()
                .chain(&registration.metadata.connection_middleware)
                .map(|middleware| (middleware.type_id, middleware.type_name)),
        )?;
        if registration
            .metadata
            .frame_codec
            .is_some_and(|codec| codec.type_id == app_frame_codec.type_id)
        {
            return Err(
                WebSocketControllerMaterializationError::DuplicateComponent {
                    operation: registration.type_name,
                    component_kind: "effective frame codec",
                    component: app_frame_codec.type_name,
                },
            );
        }
        if registration
            .metadata
            .payload_codec
            .is_some_and(|codec| codec.type_id == app_payload_codec.type_id)
        {
            return Err(
                WebSocketControllerMaterializationError::DuplicateComponent {
                    operation: registration.type_name,
                    component_kind: "effective payload codec",
                    component: app_payload_codec.type_name,
                },
            );
        }
        if let Some(first_controller) =
            namespace_owners.insert(registration.metadata.namespace, registration.type_name)
        {
            return Err(
                WebSocketControllerMaterializationError::DuplicateNamespace {
                    namespace: registration.metadata.namespace.to_owned(),
                    first_controller,
                    duplicate_controller: registration.type_name,
                },
            );
        }
    }

    let mut required_types = HashSet::with_capacity(pending_operations.len());
    let mut validated = Vec::with_capacity(pending_operations.len());
    let mut message_routes = HashSet::<(&'static str, &'static str)>::new();
    let mut lifecycle_routes = HashSet::<(&'static str, WebSocketOperationKind)>::new();

    for pending in pending_operations {
        let action = pending.action_registration;
        let Some(registration) = registrations_by_type.get(&action.controller_type_id) else {
            return Err(
                WebSocketControllerMaterializationError::MissingRegistration {
                    controller: action.controller_type_name,
                    operation: pending.handler_name,
                },
            );
        };
        if registration.type_name != action.controller_type_name {
            return Err(WebSocketControllerMaterializationError::MetadataMismatch {
                registered_controller: registration.type_name,
                operation_controller: action.controller_type_name,
                operation: pending.handler_name,
            });
        }

        validate_pending_operation(registration, &pending)?;
        if pending.kind == WebSocketOperationKind::Message {
            let effective_middleware_count = app_message_middlewares.len()
                + registration.metadata.message_middleware.len()
                + pending.metadata.message_middleware.len();
            lily_middleware::validate_middleware_count(effective_middleware_count).map_err(
                |source| WebSocketControllerMaterializationError::InvalidMiddlewarePlan {
                    operation: pending.handler_name,
                    source,
                },
            )?;
            validate_unique_types(
                pending.handler_name,
                "effective message middleware",
                app_message_middlewares
                    .iter()
                    .chain(&registration.metadata.message_middleware)
                    .chain(&pending.metadata.message_middleware)
                    .map(|middleware| (middleware.type_id, middleware.type_name)),
            )?;
            validate_unique_types(
                pending.handler_name,
                "effective guard",
                app_guards
                    .iter()
                    .chain(&registration.metadata.guards)
                    .chain(&pending.metadata.guards)
                    .map(|guard| (guard.type_id, guard.type_name)),
            )?;
        }
        if let Some(action_codec) = pending.metadata.payload_codec {
            let inherited = registration
                .metadata
                .payload_codec
                .unwrap_or(app_payload_codec);
            if action_codec.type_id == inherited.type_id {
                return Err(
                    WebSocketControllerMaterializationError::DuplicateComponent {
                        operation: pending.handler_name,
                        component_kind: "effective payload codec",
                        component: action_codec.type_name,
                    },
                );
            }
        }
        match pending.kind {
            WebSocketOperationKind::Message => {
                let event = pending
                    .event
                    .expect("message event was validated before duplicate detection");
                if !message_routes.insert((registration.metadata.namespace, event)) {
                    return Err(
                        WebSocketControllerMaterializationError::DuplicateOperation {
                            namespace: registration.metadata.namespace.to_owned(),
                            event: event.to_owned(),
                        },
                    );
                }
            }
            kind @ (WebSocketOperationKind::Connected | WebSocketOperationKind::Disconnected) => {
                if !lifecycle_routes.insert((registration.metadata.namespace, kind)) {
                    return Err(
                        WebSocketControllerMaterializationError::DuplicateLifecycle {
                            namespace: registration.metadata.namespace.to_owned(),
                            kind: kind.as_str(),
                        },
                    );
                }
            }
        }

        required_types.insert(action.controller_type_id);
        validated.push(PendingValidatedOperation {
            pending,
            registration: registration.clone(),
        });
    }

    let mut instances =
        HashMap::<TypeId, ErasedWebSocketController>::with_capacity(required_types.len());
    for registration in registrations
        .iter()
        .filter(|registration| required_types.contains(&registration.type_id))
    {
        let instance =
            match run_root_initialization((registration.initialize)(Arc::clone(&extensions))).await
            {
                Ok(Ok(instance)) => instance,
                Ok(Err(source)) => {
                    return Err(WebSocketControllerMaterializationError::Initialization {
                        controller: registration.type_name,
                        source,
                    });
                }
                Err(()) => {
                    return Err(WebSocketControllerMaterializationError::Initialization {
                        controller: registration.type_name,
                        source: WebSocketControllerInitError::Internal,
                    });
                }
            };
        instances.insert(registration.type_id, instance);
    }

    let mut middleware_instances = HashMap::<TypeId, Arc<dyn WsMessageMiddleware>>::new();
    let mut handshake_middleware_instances =
        HashMap::<TypeId, Arc<dyn WebSocketHandshakeMiddleware>>::new();
    let mut connection_middleware_instances =
        HashMap::<TypeId, Arc<dyn WsConnectionMiddleware>>::new();
    let mut guard_instances = HashMap::<TypeId, Arc<dyn WsGuard>>::new();
    let mut codec_instances = HashMap::<TypeId, ErasedWebSocketCodec>::new();
    let identity_middleware = match app_identity_middleware {
        Some(registration) => {
            let middleware =
                match run_root_initialization(registration.instantiate(Arc::clone(&extensions)))
                    .await
                {
                    Ok(Ok(middleware)) => middleware,
                    Ok(Err(source)) => {
                        return Err(
                            WebSocketControllerMaterializationError::MiddlewareInitialization {
                                operation: "application identity",
                                middleware: registration.type_name,
                                source,
                            },
                        );
                    }
                    Err(()) => {
                        return Err(
                        WebSocketControllerMaterializationError::MiddlewareInitializationPanicked {
                            operation: "application identity",
                            middleware: registration.type_name,
                        },
                    );
                    }
                };
            let compiled = catch_unwind(AssertUnwindSafe(|| {
                CompiledWsIdentityMiddleware::compile_observed(
                    middleware,
                    Arc::clone(&middleware_observer),
                )
            }));
            match compiled {
                Ok(Ok(compiled)) => Some(Arc::new(compiled)),
                Ok(Err(source)) => {
                    return Err(
                        WebSocketControllerMaterializationError::InvalidMiddlewarePlan {
                            operation: "application identity",
                            source,
                        },
                    );
                }
                Err(_) => {
                    return Err(
                        WebSocketControllerMaterializationError::MiddlewarePlanPanicked {
                            operation: "application identity",
                        },
                    );
                }
            }
        }
        None => None,
    };
    let mut controllers = BTreeMap::<String, MaterializedWebSocketController>::new();
    for registration in registrations
        .into_iter()
        .filter(|registration| required_types.contains(&registration.type_id))
    {
        let frame_registration = registration.metadata.frame_codec.unwrap_or(app_frame_codec);
        let frame_codec = materialize_frame_codec(
            frame_registration,
            &mut codec_instances,
            Arc::clone(&extensions),
        )
        .await?;
        let payload_registration = registration
            .metadata
            .payload_codec
            .unwrap_or(app_payload_codec);
        let payload_codec = materialize_payload_codec(
            payload_registration,
            &mut codec_instances,
            Arc::clone(&extensions),
        )
        .await?;
        let handshake_middleware = materialize_effective_handshake_middleware(
            registration.type_name,
            &app_handshake_middlewares,
            &registration.metadata.handshake_middleware,
            &mut handshake_middleware_instances,
            Arc::clone(&extensions),
            Arc::clone(&middleware_observer),
        )
        .await?;
        let connection_middleware = materialize_effective_connection_middleware(
            registration.type_name,
            &app_connection_middlewares,
            &registration.metadata.connection_middleware,
            &mut connection_middleware_instances,
            Arc::clone(&extensions),
            Arc::clone(&middleware_observer),
        )
        .await?;
        controllers.insert(
            registration.metadata.namespace.to_owned(),
            MaterializedWebSocketController {
                controller_type_name: registration.type_name,
                actions: HashMap::new(),
                lifecycle: WebSocketLifecycleHandlers::default(),
                handshake_middleware,
                connection_middleware,
                frame_codec,
                payload_codec,
                asyncapi: registration.metadata.asyncapi,
            },
        );
    }

    let mut action_count = 0usize;
    for validated in validated {
        let PendingValidatedOperation {
            pending,
            registration,
        } = validated;
        let controller = Arc::clone(
            instances
                .get(&pending.action_registration.controller_type_id)
                .expect("controller metadata was validated before initialization"),
        );
        let bound = match catch_unwind(AssertUnwindSafe(|| {
            pending.action_registration.bind(controller)
        })) {
            Ok(Ok(bound)) => bound,
            Ok(Err(_source)) => {
                return Err(WebSocketControllerMaterializationError::Binding {
                    controller: registration.type_name,
                    operation: pending.handler_name,
                });
            }
            Err(_) => {
                return Err(WebSocketControllerMaterializationError::BindingPanicked {
                    controller: registration.type_name,
                    operation: pending.handler_name,
                });
            }
        };
        if bound.kind() != pending.kind {
            return Err(WebSocketControllerMaterializationError::BoundKindMismatch {
                operation: pending.handler_name,
                declared: pending.kind.as_str(),
                bound: bound.kind().as_str(),
            });
        }

        let namespace = registration.metadata.namespace;
        let entry = controllers
            .get_mut(namespace)
            .expect("validated controller namespace has an app-local entry");
        let timeout = match pending.kind {
            WebSocketOperationKind::Message => {
                pending.metadata.timeout.or(registration.metadata.timeout)
            }
            _ => pending.metadata.timeout,
        };

        match bound {
            BoundWebSocketOperation::Message(handler) => {
                let message_middleware = materialize_effective_message_middleware(
                    pending.handler_name,
                    &app_message_middlewares,
                    &registration.metadata.message_middleware,
                    &pending.metadata.message_middleware,
                    &mut middleware_instances,
                    Arc::clone(&extensions),
                    Arc::clone(&middleware_observer),
                )
                .await?;
                let guards = materialize_effective_guards(
                    pending.handler_name,
                    &app_guards,
                    &registration.metadata.guards,
                    &pending.metadata.guards,
                    &mut guard_instances,
                    Arc::clone(&extensions),
                )
                .await?;
                let payload_codec =
                    if let Some(payload_registration) = pending.metadata.payload_codec {
                        materialize_payload_codec(
                            payload_registration,
                            &mut codec_instances,
                            Arc::clone(&extensions),
                        )
                        .await?
                    } else {
                        Arc::clone(&entry.payload_codec)
                    };
                let event = pending
                    .event
                    .expect("message event was validated before binding");
                let previous = entry.actions.insert(
                    event.to_owned(),
                    Arc::new(MaterializedWebSocketAction {
                        handler_name: pending.handler_name,
                        handler,
                        message_middleware,
                        guards,
                        timeout,
                        payload_codec,
                        controller_asyncapi: registration.metadata.asyncapi.clone(),
                        operation_asyncapi: pending.metadata.asyncapi,
                    }),
                );
                debug_assert!(previous.is_none());
                action_count += 1;
            }
            BoundWebSocketOperation::Connected(handler) => {
                entry.lifecycle.connected = Some(MaterializedWebSocketLifecycle {
                    handler_name: pending.handler_name,
                    handler,
                    timeout,
                });
            }
            BoundWebSocketOperation::Disconnected(handler) => {
                entry.lifecycle.disconnected = Some(MaterializedWebSocketLifecycle {
                    handler_name: pending.handler_name,
                    handler,
                    timeout,
                });
            }
        }
    }

    Ok(WebSocketActionTable {
        controllers,
        identity_middleware,
        action_count,
    })
}

fn validate_controller_metadata(
    registration: &WebSocketControllerRegistration,
) -> Result<(), WebSocketControllerMaterializationError> {
    let metadata = &registration.metadata;
    if !crate::request::is_canonical_route_token(metadata.namespace, MAX_CANONICAL_NAMESPACE_BYTES)
    {
        return Err(WebSocketControllerMaterializationError::InvalidNamespace {
            controller: registration.type_name,
            namespace: metadata.namespace.to_owned(),
        });
    }
    validate_operation_timeout(registration.type_name, metadata.timeout)?;

    validate_unique_types(
        registration.type_name,
        "controller guard",
        metadata
            .guards
            .iter()
            .map(|guard| (guard.type_id, guard.type_name)),
    )?;
    validate_unique_types(
        registration.type_name,
        "handshake middleware",
        metadata
            .handshake_middleware
            .iter()
            .map(|middleware| (middleware.type_id, middleware.type_name)),
    )?;
    validate_unique_types(
        registration.type_name,
        "connection middleware",
        metadata
            .connection_middleware
            .iter()
            .map(|middleware| (middleware.type_id, middleware.type_name)),
    )?;
    validate_unique_types(
        registration.type_name,
        "controller message middleware",
        metadata
            .message_middleware
            .iter()
            .map(|middleware| (middleware.type_id, middleware.type_name)),
    )?;

    Ok(())
}

fn validate_pending_operation(
    registration: &WebSocketControllerRegistration,
    pending: &PendingWebSocketOperation,
) -> Result<(), WebSocketControllerMaterializationError> {
    validate_operation_timeout(pending.handler_name, pending.metadata.timeout)?;
    match (pending.kind, pending.event) {
        (WebSocketOperationKind::Message, Some(event)) => {
            if !crate::request::is_canonical_route_token(event, MAX_CANONICAL_EVENT_BYTES) {
                return Err(WebSocketControllerMaterializationError::InvalidEvent {
                    operation: pending.handler_name,
                    event: event.to_owned(),
                });
            }
            let route_bytes = registration.metadata.namespace.len() + 1 + event.len();
            if route_bytes > MAX_CANONICAL_EVENT_BYTES {
                return Err(WebSocketControllerMaterializationError::RouteTooLong {
                    namespace: registration.metadata.namespace.to_owned(),
                    event: event.to_owned(),
                    maximum_bytes: MAX_CANONICAL_EVENT_BYTES,
                });
            }
            validate_unique_types(
                pending.handler_name,
                "action message middleware",
                pending
                    .metadata
                    .message_middleware
                    .iter()
                    .map(|middleware| (middleware.type_id, middleware.type_name)),
            )?;
            validate_unique_types(
                pending.handler_name,
                "action guard",
                pending
                    .metadata
                    .guards
                    .iter()
                    .map(|guard| (guard.type_id, guard.type_name)),
            )?;
        }
        (WebSocketOperationKind::Connected | WebSocketOperationKind::Disconnected, None) => {
            if !pending.metadata.message_middleware.is_empty()
                || !pending.metadata.guards.is_empty()
            {
                return Err(
                    WebSocketControllerMaterializationError::InvalidOperationMetadata {
                        operation: pending.handler_name,
                        kind: pending.kind.as_str(),
                    },
                );
            }
        }
        _ => {
            return Err(
                WebSocketControllerMaterializationError::InvalidOperationMetadata {
                    operation: pending.handler_name,
                    kind: pending.kind.as_str(),
                },
            );
        }
    }
    Ok(())
}

fn validate_operation_timeout(
    operation: &'static str,
    timeout: Option<Duration>,
) -> Result<(), WebSocketControllerMaterializationError> {
    let Some(timeout) = timeout else {
        return Ok(());
    };
    let maximum_seconds = crate::server::MAX_EXECUTION_TIMEOUT_SECS;
    if timeout.is_zero() || timeout > Duration::from_secs(maximum_seconds) {
        return Err(WebSocketControllerMaterializationError::InvalidTimeout {
            operation,
            seconds: timeout.as_secs(),
            maximum_seconds,
        });
    }
    Ok(())
}

fn validate_unique_types(
    operation: &'static str,
    component_kind: &'static str,
    components: impl IntoIterator<Item = (TypeId, &'static str)>,
) -> Result<(), WebSocketControllerMaterializationError> {
    let mut seen = HashSet::new();
    for (type_id, type_name) in components {
        if !seen.insert(type_id) {
            return Err(
                WebSocketControllerMaterializationError::DuplicateComponent {
                    operation,
                    component_kind,
                    component: type_name,
                },
            );
        }
    }
    Ok(())
}

async fn materialize_erased_codec(
    type_id: TypeId,
    type_name: &'static str,
    codec_kind: &'static str,
    initialize: fn(Arc<Extensions>) -> CodecInitializationFuture,
    cache: &mut HashMap<TypeId, ErasedWebSocketCodec>,
    extensions: Arc<Extensions>,
) -> Result<ErasedWebSocketCodec, WebSocketControllerMaterializationError> {
    if let Some(codec) = cache.get(&type_id) {
        return Ok(Arc::clone(codec));
    }
    let codec = match run_root_initialization(initialize(extensions)).await {
        Ok(Ok(codec)) => codec,
        Ok(Err(source)) => {
            return Err(
                WebSocketControllerMaterializationError::CodecInitialization {
                    codec_kind,
                    codec: type_name,
                    source,
                },
            );
        }
        Err(()) => {
            return Err(
                WebSocketControllerMaterializationError::CodecInitializationPanicked {
                    codec_kind,
                    codec: type_name,
                },
            );
        }
    };
    cache.insert(type_id, Arc::clone(&codec));
    Ok(codec)
}

async fn materialize_frame_codec(
    registration: WebSocketFrameCodecRegistration,
    cache: &mut HashMap<TypeId, ErasedWebSocketCodec>,
    extensions: Arc<Extensions>,
) -> Result<Arc<dyn WebSocketFrameCodec>, WebSocketControllerMaterializationError> {
    let codec = materialize_erased_codec(
        registration.type_id,
        registration.type_name,
        "frame",
        registration.initialize,
        cache,
        extensions,
    )
    .await?;
    (registration.bind)(codec).ok_or(WebSocketControllerMaterializationError::CodecBinding {
        codec_kind: "frame",
        codec: registration.type_name,
    })
}

async fn materialize_payload_codec(
    registration: WebSocketPayloadCodecRegistration,
    cache: &mut HashMap<TypeId, ErasedWebSocketCodec>,
    extensions: Arc<Extensions>,
) -> Result<Arc<dyn WebSocketPayloadCodec>, WebSocketControllerMaterializationError> {
    let codec = materialize_erased_codec(
        registration.type_id,
        registration.type_name,
        "payload",
        registration.initialize,
        cache,
        extensions,
    )
    .await?;
    (registration.bind)(codec).ok_or(WebSocketControllerMaterializationError::CodecBinding {
        codec_kind: "payload",
        codec: registration.type_name,
    })
}

struct AbortInitializationTaskOnDrop(tokio::task::AbortHandle);

impl Drop for AbortInitializationTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Poll an app-owned component constructor without inheriting a caller's
/// ambient message/request scope.
///
/// `Extensions::get_service(None)` intentionally consults the current
/// `ProcessContext`. App build may itself be called from application code that
/// happens to run inside a scope, but middleware and guards outlive that scope.
/// A fresh Tokio task provides the required root-only construction boundary.
/// The abort guard prevents a cancelled app build from detaching user
/// constructor work.
async fn run_root_initialization<F, T, E>(future: F) -> Result<Result<T, E>, ()>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    let task = tokio::spawn(future);
    let _abort_on_drop = AbortInitializationTaskOnDrop(task.abort_handle());
    task.await.map_err(|_| ())
}

async fn materialize_effective_handshake_middleware(
    operation: &'static str,
    app_middlewares: &[WebSocketHandshakeMiddlewareRegistration],
    controller_middlewares: &[WebSocketHandshakeMiddlewareRegistration],
    cache: &mut HashMap<TypeId, Arc<dyn WebSocketHandshakeMiddleware>>,
    extensions: Arc<Extensions>,
    observer: Arc<dyn WsMiddlewareObserver>,
) -> Result<Arc<CompiledWsHandshakeChain>, WebSocketControllerMaterializationError> {
    let mut middlewares = Vec::with_capacity(app_middlewares.len() + controller_middlewares.len());
    for registration in app_middlewares.iter().chain(controller_middlewares) {
        if let Some(middleware) = cache.get(&registration.type_id) {
            middlewares.push(Arc::clone(middleware));
            continue;
        }
        let middleware = match run_root_initialization(
            registration.instantiate(Arc::clone(&extensions)),
        )
        .await
        {
            Ok(Ok(middleware)) => middleware,
            Ok(Err(source)) => {
                return Err(
                    WebSocketControllerMaterializationError::MiddlewareInitialization {
                        operation,
                        middleware: registration.type_name,
                        source,
                    },
                );
            }
            Err(()) => {
                return Err(
                    WebSocketControllerMaterializationError::MiddlewareInitializationPanicked {
                        operation,
                        middleware: registration.type_name,
                    },
                );
            }
        };
        cache.insert(registration.type_id, Arc::clone(&middleware));
        middlewares.push(middleware);
    }

    match catch_unwind(AssertUnwindSafe(|| {
        CompiledWsHandshakeChain::compile_observed(middlewares, observer)
    })) {
        Ok(Ok(chain)) => Ok(Arc::new(chain)),
        Ok(Err(source)) => Err(
            WebSocketControllerMaterializationError::InvalidMiddlewarePlan { operation, source },
        ),
        Err(_) => {
            Err(WebSocketControllerMaterializationError::MiddlewarePlanPanicked { operation })
        }
    }
}

async fn materialize_effective_connection_middleware(
    operation: &'static str,
    app_middlewares: &[WebSocketConnectionMiddlewareRegistration],
    controller_middlewares: &[WebSocketConnectionMiddlewareRegistration],
    cache: &mut HashMap<TypeId, Arc<dyn WsConnectionMiddleware>>,
    extensions: Arc<Extensions>,
    observer: Arc<dyn WsMiddlewareObserver>,
) -> Result<Arc<CompiledWsConnectionChain>, WebSocketControllerMaterializationError> {
    let mut middlewares = Vec::with_capacity(app_middlewares.len() + controller_middlewares.len());
    for registration in app_middlewares.iter().chain(controller_middlewares) {
        if let Some(middleware) = cache.get(&registration.type_id) {
            middlewares.push(Arc::clone(middleware));
            continue;
        }
        let middleware = match run_root_initialization(
            registration.instantiate(Arc::clone(&extensions)),
        )
        .await
        {
            Ok(Ok(middleware)) => middleware,
            Ok(Err(source)) => {
                return Err(
                    WebSocketControllerMaterializationError::MiddlewareInitialization {
                        operation,
                        middleware: registration.type_name,
                        source,
                    },
                );
            }
            Err(()) => {
                return Err(
                    WebSocketControllerMaterializationError::MiddlewareInitializationPanicked {
                        operation,
                        middleware: registration.type_name,
                    },
                );
            }
        };
        cache.insert(registration.type_id, Arc::clone(&middleware));
        middlewares.push(middleware);
    }

    match catch_unwind(AssertUnwindSafe(|| {
        CompiledWsConnectionChain::compile_observed(middlewares, observer)
    })) {
        Ok(Ok(chain)) => Ok(Arc::new(chain)),
        Ok(Err(source)) => Err(
            WebSocketControllerMaterializationError::InvalidMiddlewarePlan { operation, source },
        ),
        Err(_) => {
            Err(WebSocketControllerMaterializationError::MiddlewarePlanPanicked { operation })
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn materialize_effective_message_middleware(
    operation: &'static str,
    app_middlewares: &[WebSocketMessageMiddlewareRegistration],
    controller_middlewares: &[WebSocketMessageMiddlewareRegistration],
    action_middlewares: &[WebSocketMessageMiddlewareRegistration],
    cache: &mut HashMap<TypeId, Arc<dyn WsMessageMiddleware>>,
    extensions: Arc<Extensions>,
    observer: Arc<dyn WsMiddlewareObserver>,
) -> Result<Arc<CompiledWsMessageChain>, WebSocketControllerMaterializationError> {
    let registrations = app_middlewares
        .iter()
        .chain(controller_middlewares)
        .chain(action_middlewares);
    let mut middlewares = Vec::with_capacity(
        app_middlewares.len() + controller_middlewares.len() + action_middlewares.len(),
    );

    for registration in registrations {
        if let Some(middleware) = cache.get(&registration.type_id) {
            middlewares.push(Arc::clone(middleware));
            continue;
        }

        let middleware = match run_root_initialization(
            registration.instantiate(Arc::clone(&extensions)),
        )
        .await
        {
            Ok(Ok(middleware)) => middleware,
            Ok(Err(source)) => {
                return Err(
                    WebSocketControllerMaterializationError::MiddlewareInitialization {
                        operation,
                        middleware: registration.type_name,
                        source,
                    },
                );
            }
            Err(()) => {
                return Err(
                    WebSocketControllerMaterializationError::MiddlewareInitializationPanicked {
                        operation,
                        middleware: registration.type_name,
                    },
                );
            }
        };
        cache.insert(registration.type_id, Arc::clone(&middleware));
        middlewares.push(middleware);
    }

    match catch_unwind(AssertUnwindSafe(|| {
        CompiledWsMessageChain::compile_observed(middlewares, observer)
    })) {
        Ok(Ok(chain)) => Ok(Arc::new(chain)),
        Ok(Err(source)) => Err(
            WebSocketControllerMaterializationError::InvalidMiddlewarePlan { operation, source },
        ),
        Err(_) => {
            Err(WebSocketControllerMaterializationError::MiddlewarePlanPanicked { operation })
        }
    }
}

async fn materialize_effective_guards(
    operation: &'static str,
    app_guards: &[WebSocketGuardRegistration],
    controller_guards: &[WebSocketGuardRegistration],
    action_guards: &[WebSocketGuardRegistration],
    cache: &mut HashMap<TypeId, Arc<dyn WsGuard>>,
    extensions: Arc<Extensions>,
) -> Result<Arc<[Arc<dyn WsGuard>]>, WebSocketControllerMaterializationError> {
    let registrations = app_guards
        .iter()
        .chain(controller_guards)
        .chain(action_guards);
    let mut guards =
        Vec::with_capacity(app_guards.len() + controller_guards.len() + action_guards.len());
    for registration in registrations {
        if let Some(guard) = cache.get(&registration.type_id) {
            guards.push(Arc::clone(guard));
            continue;
        }

        let guard = match run_root_initialization(registration.instantiate(Arc::clone(&extensions)))
            .await
        {
            Ok(Ok(guard)) => guard,
            Ok(Err(source)) => {
                return Err(
                    WebSocketControllerMaterializationError::GuardInitialization {
                        operation,
                        guard: registration.type_name,
                        source,
                    },
                );
            }
            Err(()) => {
                return Err(
                    WebSocketControllerMaterializationError::GuardInitializationPanicked {
                        operation,
                        guard: registration.type_name,
                    },
                );
            }
        };
        cache.insert(registration.type_id, Arc::clone(&guard));
        guards.push(guard);
    }
    Ok(guards.into())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, Weak};

    use super::*;
    use async_trait::async_trait;
    use lily_injection::{ApplicationContainer, ProcessContext};

    static SINGLETON_CONTROLLER_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static SINGLETON_GUARD_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static HANDSHAKE_MIDDLEWARE_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static CONNECTION_MIDDLEWARE_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static ROLLBACK_HANDSHAKE_MIDDLEWARE_DROPS: AtomicUsize = AtomicUsize::new(0);
    static ROLLBACK_IDENTITY_MIDDLEWARE_DROPS: AtomicUsize = AtomicUsize::new(0);
    static SHARED_MESSAGE_MIDDLEWARE_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static PIPELINE_ORDER: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    static LIFECYCLE_PIPELINE_ORDER: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    static PIPELINE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static UNUSED_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static ROLLBACK_DROPS: AtomicUsize = AtomicUsize::new(0);
    static APP_FRAME_CODEC_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static APP_PAYLOAD_CODEC_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static CONTROLLER_FRAME_CODEC_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static CONTROLLER_PAYLOAD_CODEC_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static ACTION_PAYLOAD_CODEC_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static SHARED_CODEC_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static CODEC_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static BOUND_CONTROLLERS: Mutex<Vec<Weak<SingletonController>>> = Mutex::new(Vec::new());

    struct TestController;
    struct TestGuard;
    struct SingletonController;
    struct SingletonGuard;
    struct Middleware;
    struct GlobalHandshakeMiddleware;
    struct ControllerHandshakeMiddleware;
    struct GlobalConnectionMiddleware;
    struct ControllerConnectionMiddleware;
    struct FailingHandshakeMiddleware;
    struct PanickingHandshakeMiddleware;
    struct FailingConnectionMiddleware;
    struct PanickingConnectionMiddleware;
    struct FailingIdentityMiddleware;
    struct PanickingIdentityMiddleware;
    struct RollbackHandshakeMiddleware;
    struct RollbackIdentityMiddleware;
    struct GlobalMiddleware;
    struct ControllerMiddleware;
    struct ActionMiddleware;
    struct SharedMessageMiddleware;
    struct FailingMessageMiddleware;
    struct PanickingMessageMiddleware;
    struct GlobalGuard;
    struct ActionGuard;
    struct FailingGuard;
    struct PanickingGuard;
    struct InvalidNamespaceController;
    struct DuplicateNamespaceFirst;
    struct DuplicateNamespaceSecond;
    struct InitFailureController;
    struct InitPanicController;
    struct BinderPanicController;
    struct KindMismatchController;
    struct UnusedController;
    struct RequiredController;
    struct RollbackDropController;
    struct RollbackFailingController;
    struct AppCodecController;
    struct OverrideCodecController;
    struct SharedCodecController;
    struct CodecInitFailureController;
    struct CodecInitPanicController;
    struct DuplicateAppFrameController;
    struct DuplicateAppPayloadController;

    struct AppFrameCodec;
    struct AppPayloadCodec;
    struct ControllerFrameCodec;
    struct ControllerPayloadCodec;
    struct ActionPayloadCodec;
    struct SharedCodec;
    struct FailingFrameCodec;
    struct PanickingPayloadCodec;

    macro_rules! frame_codec_fixture {
        ($codec:ty, $initializations:ident, $marker:literal) => {
            #[async_trait]
            impl WebSocketCodecFactory for $codec {
                async fn new(
                    _extensions: Arc<Extensions>,
                ) -> Result<Self, WebSocketCodecInitError> {
                    $initializations.fetch_add(1, Ordering::SeqCst);
                    Ok(Self)
                }
            }

            impl WebSocketFrameCodec for $codec {
                fn decode_frame(
                    &self,
                    _frame: crate::codec::RawEnvelope,
                ) -> Result<crate::codec::DecodedWebSocketMessage, crate::codec::WebSocketCodecError>
                {
                    Err(crate::codec::WebSocketCodecError::unsupported_frame())
                }

                fn encode_frame(
                    &self,
                    _message: crate::codec::EncodedWebSocketMessage,
                ) -> Result<tokio_tungstenite::tungstenite::Message, crate::codec::WebSocketCodecError>
                {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(
                        $marker.to_owned(),
                    ))
                }
            }
        };
    }

    macro_rules! payload_codec_fixture {
        ($codec:ty, $initializations:ident, $marker:literal) => {
            #[async_trait]
            impl WebSocketCodecFactory for $codec {
                async fn new(
                    _extensions: Arc<Extensions>,
                ) -> Result<Self, WebSocketCodecInitError> {
                    $initializations.fetch_add(1, Ordering::SeqCst);
                    Ok(Self)
                }
            }

            impl WebSocketPayloadCodec for $codec {
                fn decode_payload(
                    &self,
                    _payload: crate::codec::EncodedWebSocketPayload,
                ) -> Result<crate::codec::DecodedWebSocketPayload, crate::codec::WebSocketCodecError>
                {
                    Ok(crate::codec::DecodedWebSocketPayload::Raw(
                        $marker.as_bytes().to_vec(),
                    ))
                }

                fn encode_payload(
                    &self,
                    _payload: crate::codec::DecodedWebSocketPayload,
                ) -> Result<crate::codec::EncodedWebSocketPayload, crate::codec::WebSocketCodecError>
                {
                    crate::codec::EncodedWebSocketPayload::try_new(
                        crate::codec::WebSocketContentKind::Raw,
                        $marker,
                        "identity",
                        crate::codec::EncodedWebSocketPayloadData::Bytes(Vec::new()),
                    )
                }
            }
        };
    }

    frame_codec_fixture!(AppFrameCodec, APP_FRAME_CODEC_INITIALIZATIONS, "app-frame");
    frame_codec_fixture!(
        ControllerFrameCodec,
        CONTROLLER_FRAME_CODEC_INITIALIZATIONS,
        "controller-frame"
    );
    payload_codec_fixture!(
        AppPayloadCodec,
        APP_PAYLOAD_CODEC_INITIALIZATIONS,
        "app-payload"
    );
    payload_codec_fixture!(
        ControllerPayloadCodec,
        CONTROLLER_PAYLOAD_CODEC_INITIALIZATIONS,
        "controller-payload"
    );
    payload_codec_fixture!(
        ActionPayloadCodec,
        ACTION_PAYLOAD_CODEC_INITIALIZATIONS,
        "action-payload"
    );

    #[async_trait]
    impl WebSocketCodecFactory for SharedCodec {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketCodecInitError> {
            SHARED_CODEC_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(Self)
        }
    }

    impl WebSocketFrameCodec for SharedCodec {
        fn decode_frame(
            &self,
            _frame: crate::codec::RawEnvelope,
        ) -> Result<crate::codec::DecodedWebSocketMessage, crate::codec::WebSocketCodecError>
        {
            Err(crate::codec::WebSocketCodecError::unsupported_frame())
        }

        fn encode_frame(
            &self,
            _message: crate::codec::EncodedWebSocketMessage,
        ) -> Result<tokio_tungstenite::tungstenite::Message, crate::codec::WebSocketCodecError>
        {
            Ok(tokio_tungstenite::tungstenite::Message::Text(
                "shared-frame".to_owned(),
            ))
        }
    }

    impl WebSocketPayloadCodec for SharedCodec {
        fn decode_payload(
            &self,
            _payload: crate::codec::EncodedWebSocketPayload,
        ) -> Result<crate::codec::DecodedWebSocketPayload, crate::codec::WebSocketCodecError>
        {
            Ok(crate::codec::DecodedWebSocketPayload::Raw(Vec::new()))
        }

        fn encode_payload(
            &self,
            _payload: crate::codec::DecodedWebSocketPayload,
        ) -> Result<crate::codec::EncodedWebSocketPayload, crate::codec::WebSocketCodecError>
        {
            crate::codec::EncodedWebSocketPayload::try_new(
                crate::codec::WebSocketContentKind::Raw,
                "shared-payload",
                "identity",
                crate::codec::EncodedWebSocketPayloadData::Bytes(Vec::new()),
            )
        }
    }

    #[async_trait]
    impl WebSocketCodecFactory for FailingFrameCodec {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketCodecInitError> {
            Err(WebSocketCodecInitError::dependency(
                "postgres://administrator:do-not-publish@database.internal/production",
            ))
        }
    }

    impl WebSocketFrameCodec for FailingFrameCodec {
        fn decode_frame(
            &self,
            _frame: crate::codec::RawEnvelope,
        ) -> Result<crate::codec::DecodedWebSocketMessage, crate::codec::WebSocketCodecError>
        {
            unreachable!("a failed codec must never reach runtime dispatch")
        }

        fn encode_frame(
            &self,
            _message: crate::codec::EncodedWebSocketMessage,
        ) -> Result<tokio_tungstenite::tungstenite::Message, crate::codec::WebSocketCodecError>
        {
            unreachable!("a failed codec must never reach runtime dispatch")
        }
    }

    #[async_trait]
    impl WebSocketCodecFactory for PanickingPayloadCodec {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketCodecInitError> {
            panic!("codec initialization fixture panic")
        }
    }

    impl WebSocketPayloadCodec for PanickingPayloadCodec {
        fn decode_payload(
            &self,
            _payload: crate::codec::EncodedWebSocketPayload,
        ) -> Result<crate::codec::DecodedWebSocketPayload, crate::codec::WebSocketCodecError>
        {
            unreachable!("a panicked codec must never reach runtime dispatch")
        }

        fn encode_payload(
            &self,
            _payload: crate::codec::DecodedWebSocketPayload,
        ) -> Result<crate::codec::EncodedWebSocketPayload, crate::codec::WebSocketCodecError>
        {
            unreachable!("a panicked codec must never reach runtime dispatch")
        }
    }

    impl Drop for RollbackDropController {
        fn drop(&mut self) {
            ROLLBACK_DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    macro_rules! empty_definition {
        ($controller:ty, $namespace:literal) => {
            impl WebSocketControllerDefinition for $controller {
                fn namespace() -> &'static str {
                    $namespace
                }

                fn handshake_middleware_registrations()
                -> Vec<WebSocketHandshakeMiddlewareRegistration> {
                    Vec::new()
                }

                fn connection_middleware_registrations()
                -> Vec<WebSocketConnectionMiddlewareRegistration> {
                    Vec::new()
                }

                fn message_middleware_registrations() -> Vec<WebSocketMessageMiddlewareRegistration>
                {
                    Vec::new()
                }

                fn guard_registrations() -> Vec<WebSocketGuardRegistration> {
                    Vec::new()
                }

                fn timeout() -> Option<Duration> {
                    None
                }

                fn asyncapi_registration() -> WebSocketAsyncApiRegistration {
                    WebSocketAsyncApiRegistration::unspecified()
                }
            }
        };
    }

    macro_rules! successful_controller {
        ($controller:ty, $namespace:literal) => {
            #[async_trait]
            impl WebSocketControllerTrait for $controller {
                async fn new(
                    _extensions: Arc<Extensions>,
                ) -> Result<Self, WebSocketControllerInitError> {
                    Ok(Self)
                }
            }

            empty_definition!($controller, $namespace);
        };
    }

    successful_controller!(InvalidNamespaceController, "/");
    successful_controller!(DuplicateNamespaceFirst, "duplicate");
    successful_controller!(DuplicateNamespaceSecond, "duplicate");
    successful_controller!(BinderPanicController, "binder-panic");
    successful_controller!(KindMismatchController, "kind-mismatch");
    successful_controller!(RequiredController, "required");
    successful_controller!(RollbackDropController, "rollback-drop");

    macro_rules! codec_controller_definition {
        ($controller:ty, $namespace:literal, $frame_codec:expr, $payload_codec:expr) => {
            #[async_trait]
            impl WebSocketControllerTrait for $controller {
                async fn new(
                    _extensions: Arc<Extensions>,
                ) -> Result<Self, WebSocketControllerInitError> {
                    Ok(Self)
                }
            }

            impl WebSocketControllerDefinition for $controller {
                fn namespace() -> &'static str {
                    $namespace
                }

                fn handshake_middleware_registrations()
                -> Vec<WebSocketHandshakeMiddlewareRegistration> {
                    Vec::new()
                }

                fn connection_middleware_registrations()
                -> Vec<WebSocketConnectionMiddlewareRegistration> {
                    Vec::new()
                }

                fn message_middleware_registrations() -> Vec<WebSocketMessageMiddlewareRegistration>
                {
                    Vec::new()
                }

                fn guard_registrations() -> Vec<WebSocketGuardRegistration> {
                    Vec::new()
                }

                fn frame_codec_registration() -> Option<WebSocketFrameCodecRegistration> {
                    $frame_codec
                }

                fn payload_codec_registration() -> Option<WebSocketPayloadCodecRegistration> {
                    $payload_codec
                }

                fn timeout() -> Option<Duration> {
                    None
                }

                fn asyncapi_registration() -> WebSocketAsyncApiRegistration {
                    WebSocketAsyncApiRegistration::unspecified()
                }
            }
        };
    }

    codec_controller_definition!(AppCodecController, "app-codec", None, None);
    codec_controller_definition!(
        OverrideCodecController,
        "override-codec",
        Some(WebSocketFrameCodecRegistration::of::<ControllerFrameCodec>()),
        Some(WebSocketPayloadCodecRegistration::of::<
            ControllerPayloadCodec,
        >())
    );
    codec_controller_definition!(
        SharedCodecController,
        "shared-codec",
        Some(WebSocketFrameCodecRegistration::of::<SharedCodec>()),
        Some(WebSocketPayloadCodecRegistration::of::<SharedCodec>())
    );
    codec_controller_definition!(
        CodecInitFailureController,
        "codec-init-failure",
        Some(WebSocketFrameCodecRegistration::of::<FailingFrameCodec>()),
        None
    );
    codec_controller_definition!(
        CodecInitPanicController,
        "codec-init-panic",
        None,
        Some(WebSocketPayloadCodecRegistration::of::<PanickingPayloadCodec>())
    );
    codec_controller_definition!(
        DuplicateAppFrameController,
        "duplicate-app-frame",
        Some(WebSocketFrameCodecRegistration::of::<AppFrameCodec>()),
        None
    );
    codec_controller_definition!(
        DuplicateAppPayloadController,
        "duplicate-app-payload",
        None,
        Some(WebSocketPayloadCodecRegistration::of::<AppPayloadCodec>())
    );

    #[async_trait]
    impl WebSocketControllerTrait for InitFailureController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            Err(WebSocketControllerInitError::dependency(
                "postgres://administrator:do-not-publish@database.internal/production",
            ))
        }
    }
    empty_definition!(InitFailureController, "init-failure");

    #[async_trait]
    impl WebSocketControllerTrait for InitPanicController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            panic!("controller initialization fixture panic")
        }
    }
    empty_definition!(InitPanicController, "init-panic");

    #[async_trait]
    impl WebSocketControllerTrait for UnusedController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            UNUSED_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(Self)
        }
    }
    empty_definition!(UnusedController, "unused");

    #[async_trait]
    impl WebSocketControllerTrait for RollbackFailingController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            Err(WebSocketControllerInitError::Internal)
        }
    }
    empty_definition!(RollbackFailingController, "rollback-failing");

    #[async_trait]
    impl WebSocketControllerTrait for TestController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            Ok(Self)
        }
    }

    impl WebSocketControllerDefinition for TestController {
        fn namespace() -> &'static str {
            "chat"
        }

        fn handshake_middleware_registrations() -> Vec<WebSocketHandshakeMiddlewareRegistration> {
            Vec::new()
        }

        fn connection_middleware_registrations() -> Vec<WebSocketConnectionMiddlewareRegistration> {
            Vec::new()
        }

        fn message_middleware_registrations() -> Vec<WebSocketMessageMiddlewareRegistration> {
            Vec::new()
        }

        fn guard_registrations() -> Vec<WebSocketGuardRegistration> {
            vec![WebSocketGuardRegistration::of::<TestGuard>()]
        }

        fn timeout() -> Option<Duration> {
            Some(Duration::from_secs(3))
        }

        fn asyncapi_registration() -> WebSocketAsyncApiRegistration {
            WebSocketAsyncApiRegistration::documented().with_tag("chat")
        }
    }

    #[async_trait]
    impl WsGuard for TestGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            _exchange: &mut crate::middleware::WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::WebSocketGuardRejection> {
            Ok(())
        }
    }

    #[async_trait]
    impl WebSocketControllerTrait for SingletonController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            SINGLETON_CONTROLLER_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(Self)
        }
    }

    impl WebSocketControllerDefinition for SingletonController {
        fn namespace() -> &'static str {
            "singleton"
        }

        fn handshake_middleware_registrations() -> Vec<WebSocketHandshakeMiddlewareRegistration> {
            Vec::new()
        }

        fn connection_middleware_registrations() -> Vec<WebSocketConnectionMiddlewareRegistration> {
            Vec::new()
        }

        fn message_middleware_registrations() -> Vec<WebSocketMessageMiddlewareRegistration> {
            Vec::new()
        }

        fn guard_registrations() -> Vec<WebSocketGuardRegistration> {
            vec![WebSocketGuardRegistration::of::<SingletonGuard>()]
        }

        fn timeout() -> Option<Duration> {
            Some(Duration::from_secs(3))
        }

        fn asyncapi_registration() -> WebSocketAsyncApiRegistration {
            WebSocketAsyncApiRegistration::documented().with_tag("singleton")
        }
    }

    #[async_trait]
    impl WsGuard for SingletonGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            SINGLETON_GUARD_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(Self)
        }

        async fn can_activate(
            &self,
            _exchange: &mut crate::middleware::WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::WebSocketGuardRejection> {
            Ok(())
        }
    }

    #[async_trait]
    impl WsMessageMiddleware for Middleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("test.middleware", crate::middleware::MiddlewareKind::Custom)
        }
    }

    macro_rules! ordered_handshake_middleware {
        ($middleware:ty, $name:literal) => {
            #[async_trait]
            impl WebSocketHandshakeMiddleware for $middleware {
                async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
                    HANDSHAKE_MIDDLEWARE_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
                    Ok(Self)
                }

                fn descriptor(&self) -> MiddlewareDescriptor {
                    MiddlewareDescriptor::new($name, crate::middleware::MiddlewareKind::Custom)
                }

                fn validate(&self) -> Result<(), crate::middleware::MiddlewareConfigError> {
                    LIFECYCLE_PIPELINE_ORDER
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push($name);
                    Ok(())
                }

                async fn handle(
                    &self,
                    _exchange: &mut crate::middleware::WsHandshakeExchange,
                    _cancellation: crate::ExecutionCancellation,
                ) -> Result<(), crate::middleware::WsHandshakeRejection> {
                    Ok(())
                }
            }
        };
    }

    macro_rules! ordered_connection_middleware {
        ($middleware:ty, $name:literal) => {
            #[async_trait]
            impl WsConnectionMiddleware for $middleware {
                async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
                    CONNECTION_MIDDLEWARE_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
                    Ok(Self)
                }

                fn descriptor(&self) -> MiddlewareDescriptor {
                    MiddlewareDescriptor::new($name, crate::middleware::MiddlewareKind::Custom)
                }

                fn validate(&self) -> Result<(), crate::middleware::MiddlewareConfigError> {
                    LIFECYCLE_PIPELINE_ORDER
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push($name);
                    Ok(())
                }
            }
        };
    }

    ordered_handshake_middleware!(GlobalHandshakeMiddleware, "global_handshake");
    ordered_handshake_middleware!(ControllerHandshakeMiddleware, "controller_handshake");
    ordered_connection_middleware!(GlobalConnectionMiddleware, "global_connection");
    ordered_connection_middleware!(ControllerConnectionMiddleware, "controller_connection");

    #[async_trait]
    impl WebSocketHandshakeMiddleware for FailingHandshakeMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::MissingDependency)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("failed handshake middleware is never compiled")
        }

        async fn handle(
            &self,
            _exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsHandshakeRejection> {
            unreachable!("failed handshake middleware is never executed")
        }
    }

    #[async_trait]
    impl WebSocketHandshakeMiddleware for PanickingHandshakeMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            panic!("handshake constructor fixture secret must be redacted")
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("panicked handshake middleware is never compiled")
        }

        async fn handle(
            &self,
            _exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsHandshakeRejection> {
            unreachable!("panicked handshake middleware is never executed")
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for FailingConnectionMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::InvalidConfiguration)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("failed connection middleware is never compiled")
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for PanickingConnectionMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            panic!("connection constructor fixture secret must be redacted")
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("panicked connection middleware is never compiled")
        }
    }

    #[async_trait]
    impl WebSocketIdentityMiddleware for FailingIdentityMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::ScopeRequired)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("failed identity middleware is never compiled")
        }

        async fn identify(
            &self,
            _exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<crate::middleware::WebSocketIdentity, crate::middleware::WsHandshakeRejection>
        {
            unreachable!("failed identity middleware is never executed")
        }
    }

    #[async_trait]
    impl WebSocketIdentityMiddleware for PanickingIdentityMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            panic!("identity constructor fixture secret must be redacted")
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("panicked identity middleware is never compiled")
        }

        async fn identify(
            &self,
            _exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<crate::middleware::WebSocketIdentity, crate::middleware::WsHandshakeRejection>
        {
            unreachable!("panicked identity middleware is never executed")
        }
    }

    #[async_trait]
    impl WebSocketHandshakeMiddleware for RollbackHandshakeMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "rollback_handshake",
                crate::middleware::MiddlewareKind::WebSocketHandshake,
            )
        }

        async fn handle(
            &self,
            _exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsHandshakeRejection> {
            Ok(())
        }
    }

    impl Drop for RollbackHandshakeMiddleware {
        fn drop(&mut self) {
            ROLLBACK_HANDSHAKE_MIDDLEWARE_DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl WebSocketIdentityMiddleware for RollbackIdentityMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "rollback_identity",
                crate::middleware::MiddlewareKind::WebSocketHandshake,
            )
        }

        async fn identify(
            &self,
            _exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<crate::middleware::WebSocketIdentity, crate::middleware::WsHandshakeRejection>
        {
            Ok(crate::middleware::WebSocketIdentity::anonymous())
        }
    }

    impl Drop for RollbackIdentityMiddleware {
        fn drop(&mut self) {
            ROLLBACK_IDENTITY_MIDDLEWARE_DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    macro_rules! ordered_message_middleware {
        ($middleware:ty, $name:literal) => {
            #[async_trait]
            impl WsMessageMiddleware for $middleware {
                async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
                    Ok(Self)
                }

                fn descriptor(&self) -> MiddlewareDescriptor {
                    MiddlewareDescriptor::new($name, crate::middleware::MiddlewareKind::Custom)
                }

                fn validate(&self) -> Result<(), crate::middleware::MiddlewareConfigError> {
                    PIPELINE_ORDER
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push($name);
                    Ok(())
                }
            }
        };
    }

    ordered_message_middleware!(GlobalMiddleware, "global");
    ordered_message_middleware!(ControllerMiddleware, "controller");
    ordered_message_middleware!(ActionMiddleware, "action");

    #[async_trait]
    impl WsMessageMiddleware for SharedMessageMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            SHARED_MESSAGE_MIDDLEWARE_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("shared", crate::middleware::MiddlewareKind::Custom)
        }
    }

    #[async_trait]
    impl WsMessageMiddleware for FailingMessageMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::InvalidConfiguration)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("failed middleware is never compiled")
        }
    }

    #[async_trait]
    impl WsMessageMiddleware for PanickingMessageMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            panic!("middleware constructor fixture secret must be redacted")
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("panicked middleware is never compiled")
        }
    }

    macro_rules! guard_fixture {
        ($guard:ty) => {
            #[async_trait]
            impl WsGuard for $guard {
                async fn new(
                    _extensions: Arc<Extensions>,
                ) -> Result<Self, GuardInitializationError> {
                    Ok(Self)
                }

                async fn can_activate(
                    &self,
                    _exchange: &mut crate::middleware::WsMessageExchange,
                    _cancellation: crate::ExecutionCancellation,
                ) -> Result<(), crate::guard::WebSocketGuardRejection> {
                    Ok(())
                }
            }
        };
    }

    guard_fixture!(GlobalGuard);
    guard_fixture!(ActionGuard);

    #[async_trait]
    impl WsGuard for FailingGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            Err(GuardInitializationError::MissingDependency)
        }

        async fn can_activate(
            &self,
            _exchange: &mut crate::middleware::WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::WebSocketGuardRejection> {
            unreachable!("failed guard is never executed")
        }
    }

    #[async_trait]
    impl WsGuard for PanickingGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            panic!("guard constructor fixture secret must be redacted")
        }

        async fn can_activate(
            &self,
            _exchange: &mut crate::middleware::WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::WebSocketGuardRejection> {
            unreachable!("panicked guard is never executed")
        }
    }

    struct NoopMessageAction {
        _controller: Arc<TestController>,
    }

    impl WebSocketMessageAction for NoopMessageAction {
        fn call(&self, _invocation: WebSocketMessageInvocation) -> WebSocketActionFuture {
            Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) })
        }
    }

    struct GenericMessageAction<C> {
        _controller: Arc<C>,
    }

    impl<C> WebSocketMessageAction for GenericMessageAction<C>
    where
        C: Send + Sync + 'static,
    {
        fn call(&self, _invocation: WebSocketMessageInvocation) -> WebSocketActionFuture {
            Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) })
        }
    }

    struct GenericLifecycleAction<C> {
        _controller: Arc<C>,
    }

    impl<C> WebSocketLifecycleAction for GenericLifecycleAction<C>
    where
        C: Send + Sync + 'static,
    {
        fn call(&self, _invocation: WebSocketLifecycleInvocation) -> WebSocketLifecycleFuture {
            Box::pin(async { Ok(()) })
        }
    }

    fn bind_test_message(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        let controller = downcast_websocket_controller::<TestController>(controller)?;
        Ok(BoundWebSocketOperation::Message(Arc::new(
            NoopMessageAction {
                _controller: controller,
            },
        )))
    }

    fn bind_singleton_message(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        let controller = downcast_websocket_controller::<SingletonController>(controller)?;
        BOUND_CONTROLLERS
            .lock()
            .expect("binding fixture lock is available")
            .push(Arc::downgrade(&controller));
        Ok(BoundWebSocketOperation::Message(Arc::new(
            GenericMessageAction {
                _controller: controller,
            },
        )))
    }

    fn bind_singleton_connected(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        let controller = downcast_websocket_controller::<SingletonController>(controller)?;
        BOUND_CONTROLLERS
            .lock()
            .expect("binding fixture lock is available")
            .push(Arc::downgrade(&controller));
        Ok(BoundWebSocketOperation::Connected(Arc::new(
            GenericLifecycleAction {
                _controller: controller,
            },
        )))
    }

    fn bind_generic_message<C>(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError>
    where
        C: WebSocketControllerTrait,
    {
        let controller = downcast_websocket_controller::<C>(controller)?;
        Ok(BoundWebSocketOperation::Message(Arc::new(
            GenericMessageAction {
                _controller: controller,
            },
        )))
    }

    fn bind_generic_connected<C>(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError>
    where
        C: WebSocketControllerTrait,
    {
        let controller = downcast_websocket_controller::<C>(controller)?;
        Ok(BoundWebSocketOperation::Connected(Arc::new(
            GenericLifecycleAction {
                _controller: controller,
            },
        )))
    }

    fn bind_panics(
        _controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        panic!("generated binder fixture panic")
    }

    fn generic_message<C>(
        event: &'static str,
        handler_name: &'static str,
    ) -> PendingWebSocketOperation
    where
        C: WebSocketControllerTrait,
    {
        PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some(event),
            handler_name,
            WebSocketActionRegistration::of::<C>(bind_generic_message::<C>),
            WebSocketOperationMetadata::empty(),
        )
    }

    fn generic_connected<C>(handler_name: &'static str) -> PendingWebSocketOperation
    where
        C: WebSocketControllerTrait,
    {
        PendingWebSocketOperation::new(
            WebSocketOperationKind::Connected,
            None,
            handler_name,
            WebSocketActionRegistration::of::<C>(bind_generic_connected::<C>),
            WebSocketOperationMetadata::empty(),
        )
    }

    fn panicking_registration() -> WebSocketControllerRegistration {
        panic!("link-time controller registration fixture panic")
    }

    fn panicking_operation_registration() -> PendingWebSocketOperation {
        panic!("link-time operation registration fixture panic")
    }

    fn message(
        event: &'static str,
        handler_name: &'static str,
        timeout: Option<Duration>,
    ) -> PendingWebSocketOperation {
        PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some(event),
            handler_name,
            WebSocketActionRegistration::of::<TestController>(bind_test_message),
            WebSocketOperationMetadata::new(
                Vec::new(),
                Vec::new(),
                None,
                timeout,
                WebSocketAsyncApiRegistration::documented(),
            ),
        )
    }

    fn singleton_message(
        event: &'static str,
        handler_name: &'static str,
        timeout: Option<Duration>,
    ) -> PendingWebSocketOperation {
        PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some(event),
            handler_name,
            WebSocketActionRegistration::of::<SingletonController>(bind_singleton_message),
            WebSocketOperationMetadata::new(
                Vec::new(),
                Vec::new(),
                None,
                timeout,
                WebSocketAsyncApiRegistration::documented(),
            ),
        )
    }

    fn message_with_components(
        event: &'static str,
        handler_name: &'static str,
        middleware: Vec<WebSocketMessageMiddlewareRegistration>,
        guards: Vec<WebSocketGuardRegistration>,
    ) -> PendingWebSocketOperation {
        PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some(event),
            handler_name,
            WebSocketActionRegistration::of::<TestController>(bind_test_message),
            WebSocketOperationMetadata::new(
                middleware,
                guards,
                None,
                None,
                WebSocketAsyncApiRegistration::unspecified(),
            ),
        )
    }

    fn codec_message<C>(
        event: &'static str,
        handler_name: &'static str,
        payload_codec: Option<WebSocketPayloadCodecRegistration>,
    ) -> PendingWebSocketOperation
    where
        C: WebSocketControllerTrait,
    {
        PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some(event),
            handler_name,
            WebSocketActionRegistration::of::<C>(bind_generic_message::<C>),
            WebSocketOperationMetadata::new(
                Vec::new(),
                Vec::new(),
                payload_codec,
                None,
                WebSocketAsyncApiRegistration::unspecified(),
            ),
        )
    }

    async fn materialize_test_lifecycle_pipeline(
        registration: WebSocketControllerRegistration,
        extensions: Arc<Extensions>,
        app_handshake_middlewares: Vec<WebSocketHandshakeMiddlewareRegistration>,
        app_connection_middlewares: Vec<WebSocketConnectionMiddlewareRegistration>,
        app_identity_middleware: Option<WebSocketIdentityMiddlewareRegistration>,
    ) -> Result<WebSocketActionTable, WebSocketControllerMaterializationError> {
        materialize_websocket_controllers_with_pipeline(
            vec![registration],
            vec![message(
                "transport-contract",
                "fixture::transport_contract",
                None,
            )],
            extensions,
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            app_handshake_middlewares,
            app_connection_middlewares,
            app_identity_middleware,
            Vec::new(),
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
    }

    fn frame_codec_marker(codec: Arc<dyn WebSocketFrameCodec>) -> String {
        let payload = crate::codec::EncodedWebSocketPayload::try_new(
            crate::codec::WebSocketContentKind::Raw,
            "fixture",
            "identity",
            crate::codec::EncodedWebSocketPayloadData::Bytes(Vec::new()),
        )
        .expect("fixture payload is valid");
        match codec
            .encode_frame(crate::codec::EncodedWebSocketMessage {
                kind: crate::codec::WebSocketOutboundMessageKind::Event,
                namespace: "fixture".to_owned(),
                event: "run".to_owned(),
                payload,
                message_id: None,
                ack_id: None,
                headers: crate::codec::WebSocketMessageHeaders::default(),
                frame_kind: crate::codec::WebSocketFrameKind::Text,
            })
            .expect("selected frame codec encodes the fixture")
        {
            tokio_tungstenite::tungstenite::Message::Text(marker) => marker,
            other => panic!("expected marker text frame, got {other:?}"),
        }
    }

    fn payload_codec_marker(codec: Arc<dyn WebSocketPayloadCodec>) -> String {
        codec
            .encode_payload(crate::codec::DecodedWebSocketPayload::Raw(Vec::new()))
            .expect("selected payload codec encodes the fixture")
            .content_type()
            .to_owned()
    }

    #[tokio::test]
    async fn one_controller_instance_and_one_guard_instance_back_all_actions() {
        SINGLETON_CONTROLLER_INITIALIZATIONS.store(0, Ordering::SeqCst);
        SINGLETON_GUARD_INITIALIZATIONS.store(0, Ordering::SeqCst);
        BOUND_CONTROLLERS
            .lock()
            .expect("binding fixture lock is available")
            .clear();
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");

        let table = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<SingletonController>()],
            vec![
                singleton_message("send", "fixture::SingletonController::send", None),
                singleton_message(
                    "typing",
                    "fixture::SingletonController::typing",
                    Some(Duration::from_secs(1)),
                ),
                PendingWebSocketOperation::new(
                    WebSocketOperationKind::Connected,
                    None,
                    "fixture::SingletonController::connected",
                    WebSocketActionRegistration::of::<SingletonController>(
                        bind_singleton_connected,
                    ),
                    WebSocketOperationMetadata::empty(),
                ),
            ],
            app.services(),
        )
        .await
        .expect("controller operations materialize");

        assert_eq!(
            SINGLETON_CONTROLLER_INITIALIZATIONS.load(Ordering::SeqCst),
            1
        );
        assert_eq!(SINGLETON_GUARD_INITIALIZATIONS.load(Ordering::SeqCst), 1);
        assert_eq!(table.action_count(), 2);
        assert!(table.contains_namespace("singleton"));
        assert!(!table.contains_namespace("/"));
        assert_eq!(
            table
                .find_action("singleton", "send")
                .and_then(|action| action.timeout()),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            table
                .find_action("singleton", "typing")
                .and_then(|action| action.timeout()),
            Some(Duration::from_secs(1))
        );
        let send = table.find_action("singleton", "send").unwrap();
        assert_eq!(
            table
                .lifecycle_for_namespace("singleton")
                .unwrap()
                .connected_operation()
                .unwrap()
                .timeout(),
            None,
            "controller message timeout must not become a connection hook cap"
        );
        let typing = table.find_action("singleton", "typing").unwrap();
        assert_eq!(send.guards().len(), 1);
        assert!(Arc::ptr_eq(&send.guards()[0], &typing.guards()[0]));

        {
            let pointers = BOUND_CONTROLLERS
                .lock()
                .expect("binding fixture lock is available");
            assert_eq!(pointers.len(), 3);
            let first = pointers[0].upgrade().expect("controller remains retained");
            assert!(
                pointers
                    .iter()
                    .all(|candidate| Weak::ptr_eq(candidate, &Arc::downgrade(&first)))
            );
        }

        drop(table);
        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn controller_handshake_and_connection_metadata_materialize_active_plans() {
        let _lock = PIPELINE_TEST_LOCK.lock().await;
        HANDSHAKE_MIDDLEWARE_INITIALIZATIONS.store(0, Ordering::SeqCst);
        CONNECTION_MIDDLEWARE_INITIALIZATIONS.store(0, Ordering::SeqCst);
        LIFECYCLE_PIPELINE_ORDER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();

        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let mut registration = WebSocketControllerRegistration::of::<TestController>();
        registration.metadata.handshake_middleware =
            vec![WebSocketHandshakeMiddlewareRegistration::of::<
                ControllerHandshakeMiddleware,
            >()];
        registration.metadata.connection_middleware =
            vec![WebSocketConnectionMiddlewareRegistration::of::<
                ControllerConnectionMiddleware,
            >()];

        let table = materialize_websocket_controllers_with_pipeline(
            vec![registration],
            vec![message(
                "active",
                "fixture::active_lifecycle_pipeline",
                None,
            )],
            app.services(),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            vec![WebSocketHandshakeMiddlewareRegistration::of::<
                GlobalHandshakeMiddleware,
            >()],
            vec![WebSocketConnectionMiddlewareRegistration::of::<
                GlobalConnectionMiddleware,
            >()],
            None,
            Vec::new(),
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect("controller lifecycle middleware metadata materializes");

        assert!(
            !table
                .handshake_middleware("chat")
                .expect("chat handshake plan exists")
                .is_empty()
        );
        assert!(
            !table
                .connection_middleware("chat")
                .expect("chat connection plan exists")
                .is_empty()
        );
        assert!(table.identity_middleware().is_none());
        assert_eq!(
            HANDSHAKE_MIDDLEWARE_INITIALIZATIONS.load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            CONNECTION_MIDDLEWARE_INITIALIZATIONS.load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            LIFECYCLE_PIPELINE_ORDER
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            [
                "global_handshake",
                "controller_handshake",
                "global_connection",
                "controller_connection",
            ]
        );

        drop(table);
        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn duplicate_effective_handshake_and_connection_plans_fail_before_construction() {
        let _lock = PIPELINE_TEST_LOCK.lock().await;
        HANDSHAKE_MIDDLEWARE_INITIALIZATIONS.store(0, Ordering::SeqCst);
        CONNECTION_MIDDLEWARE_INITIALIZATIONS.store(0, Ordering::SeqCst);
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let extensions = app.services();

        let duplicate_handshake =
            WebSocketHandshakeMiddlewareRegistration::of::<GlobalHandshakeMiddleware>();
        let mut handshake_controller = WebSocketControllerRegistration::of::<TestController>();
        handshake_controller.metadata.handshake_middleware = vec![duplicate_handshake];
        let handshake_error = materialize_test_lifecycle_pipeline(
            handshake_controller,
            Arc::clone(&extensions),
            vec![duplicate_handshake],
            Vec::new(),
            None,
        )
        .await
        .expect_err("duplicate effective handshake plan fails closed");
        assert_eq!(
            handshake_error,
            WebSocketControllerMaterializationError::DuplicateComponent {
                operation: std::any::type_name::<TestController>(),
                component_kind: "effective handshake middleware",
                component: std::any::type_name::<GlobalHandshakeMiddleware>(),
            }
        );

        let duplicate_connection =
            WebSocketConnectionMiddlewareRegistration::of::<GlobalConnectionMiddleware>();
        let mut connection_controller = WebSocketControllerRegistration::of::<TestController>();
        connection_controller.metadata.connection_middleware = vec![duplicate_connection];
        let connection_error = materialize_test_lifecycle_pipeline(
            connection_controller,
            extensions,
            Vec::new(),
            vec![duplicate_connection],
            None,
        )
        .await
        .expect_err("duplicate effective connection plan fails closed");
        assert_eq!(
            connection_error,
            WebSocketControllerMaterializationError::DuplicateComponent {
                operation: std::any::type_name::<TestController>(),
                component_kind: "effective connection middleware",
                component: std::any::type_name::<GlobalConnectionMiddleware>(),
            }
        );
        assert_eq!(
            HANDSHAKE_MIDDLEWARE_INITIALIZATIONS.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            CONNECTION_MIDDLEWARE_INITIALIZATIONS.load(Ordering::SeqCst),
            0
        );

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn lifecycle_constructor_failures_and_panics_use_typed_middleware_diagnostics() {
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let extensions = app.services();
        let controller = || WebSocketControllerRegistration::of::<TestController>();

        let handshake_failure = materialize_test_lifecycle_pipeline(
            controller(),
            Arc::clone(&extensions),
            vec![WebSocketHandshakeMiddlewareRegistration::of::<
                FailingHandshakeMiddleware,
            >()],
            Vec::new(),
            None,
        )
        .await
        .expect_err("handshake constructor failure aborts materialization");
        assert_eq!(
            handshake_failure,
            WebSocketControllerMaterializationError::MiddlewareInitialization {
                operation: std::any::type_name::<TestController>(),
                middleware: std::any::type_name::<FailingHandshakeMiddleware>(),
                source: WsMiddlewareInitError::MissingDependency,
            }
        );

        let handshake_panic = materialize_test_lifecycle_pipeline(
            controller(),
            Arc::clone(&extensions),
            vec![WebSocketHandshakeMiddlewareRegistration::of::<
                PanickingHandshakeMiddleware,
            >()],
            Vec::new(),
            None,
        )
        .await
        .expect_err("handshake constructor panic aborts materialization");
        assert_eq!(
            handshake_panic,
            WebSocketControllerMaterializationError::MiddlewareInitializationPanicked {
                operation: std::any::type_name::<TestController>(),
                middleware: std::any::type_name::<PanickingHandshakeMiddleware>(),
            }
        );
        assert!(!handshake_panic.to_string().contains("fixture secret"));

        let connection_failure = materialize_test_lifecycle_pipeline(
            controller(),
            Arc::clone(&extensions),
            Vec::new(),
            vec![WebSocketConnectionMiddlewareRegistration::of::<
                FailingConnectionMiddleware,
            >()],
            None,
        )
        .await
        .expect_err("connection constructor failure aborts materialization");
        assert_eq!(
            connection_failure,
            WebSocketControllerMaterializationError::MiddlewareInitialization {
                operation: std::any::type_name::<TestController>(),
                middleware: std::any::type_name::<FailingConnectionMiddleware>(),
                source: WsMiddlewareInitError::InvalidConfiguration,
            }
        );

        let connection_panic = materialize_test_lifecycle_pipeline(
            controller(),
            Arc::clone(&extensions),
            Vec::new(),
            vec![WebSocketConnectionMiddlewareRegistration::of::<
                PanickingConnectionMiddleware,
            >()],
            None,
        )
        .await
        .expect_err("connection constructor panic aborts materialization");
        assert_eq!(
            connection_panic,
            WebSocketControllerMaterializationError::MiddlewareInitializationPanicked {
                operation: std::any::type_name::<TestController>(),
                middleware: std::any::type_name::<PanickingConnectionMiddleware>(),
            }
        );
        assert!(!connection_panic.to_string().contains("fixture secret"));

        let identity_failure = materialize_test_lifecycle_pipeline(
            controller(),
            Arc::clone(&extensions),
            Vec::new(),
            Vec::new(),
            Some(WebSocketIdentityMiddlewareRegistration::of::<
                FailingIdentityMiddleware,
            >()),
        )
        .await
        .expect_err("identity constructor failure aborts materialization");
        assert_eq!(
            identity_failure,
            WebSocketControllerMaterializationError::MiddlewareInitialization {
                operation: "application identity",
                middleware: std::any::type_name::<FailingIdentityMiddleware>(),
                source: WsMiddlewareInitError::ScopeRequired,
            }
        );

        let identity_panic = materialize_test_lifecycle_pipeline(
            controller(),
            extensions,
            Vec::new(),
            Vec::new(),
            Some(WebSocketIdentityMiddlewareRegistration::of::<
                PanickingIdentityMiddleware,
            >()),
        )
        .await
        .expect_err("identity constructor panic aborts materialization");
        assert_eq!(
            identity_panic,
            WebSocketControllerMaterializationError::MiddlewareInitializationPanicked {
                operation: "application identity",
                middleware: std::any::type_name::<PanickingIdentityMiddleware>(),
            }
        );
        assert!(!identity_panic.to_string().contains("fixture secret"));

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn later_connection_failure_drops_initialized_identity_and_handshake_middleware() {
        ROLLBACK_HANDSHAKE_MIDDLEWARE_DROPS.store(0, Ordering::SeqCst);
        ROLLBACK_IDENTITY_MIDDLEWARE_DROPS.store(0, Ordering::SeqCst);
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");

        let error = materialize_test_lifecycle_pipeline(
            WebSocketControllerRegistration::of::<TestController>(),
            app.services(),
            vec![WebSocketHandshakeMiddlewareRegistration::of::<
                RollbackHandshakeMiddleware,
            >()],
            vec![WebSocketConnectionMiddlewareRegistration::of::<
                FailingConnectionMiddleware,
            >()],
            Some(WebSocketIdentityMiddlewareRegistration::of::<
                RollbackIdentityMiddleware,
            >()),
        )
        .await
        .expect_err("connection failure rolls back earlier middleware instances");
        assert_eq!(
            error,
            WebSocketControllerMaterializationError::MiddlewareInitialization {
                operation: std::any::type_name::<TestController>(),
                middleware: std::any::type_name::<FailingConnectionMiddleware>(),
                source: WsMiddlewareInitError::InvalidConfiguration,
            }
        );
        assert_eq!(
            ROLLBACK_HANDSHAKE_MIDDLEWARE_DROPS.load(Ordering::SeqCst),
            1
        );
        assert_eq!(ROLLBACK_IDENTITY_MIDDLEWARE_DROPS.load(Ordering::SeqCst), 1);

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn effective_pipeline_order_is_global_then_controller_then_action() {
        let _lock = PIPELINE_TEST_LOCK.lock().await;
        PIPELINE_ORDER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();

        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let mut registration = WebSocketControllerRegistration::of::<TestController>();
        registration.metadata.message_middleware =
            vec![WebSocketMessageMiddlewareRegistration::of::<
                ControllerMiddleware,
            >()];
        let operation = message_with_components(
            "ordered",
            "fixture::ordered",
            vec![WebSocketMessageMiddlewareRegistration::of::<ActionMiddleware>()],
            vec![WebSocketGuardRegistration::of::<ActionGuard>()],
        );

        let table = materialize_websocket_controllers_with_pipeline(
            vec![registration],
            vec![operation],
            app.services(),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            vec![WebSocketMessageMiddlewareRegistration::of::<GlobalMiddleware>()],
            vec![WebSocketGuardRegistration::of::<GlobalGuard>()],
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect("effective pipeline materializes");

        let action = table
            .find_action("chat", "ordered")
            .expect("ordered action exists");
        assert!(!action.middleware_chain().is_empty());
        assert_eq!(
            PIPELINE_ORDER
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_slice(),
            ["global", "controller", "action"]
        );
        let guard_names = action
            .guards()
            .iter()
            .map(|guard| guard.name())
            .collect::<Vec<_>>();
        assert!(guard_names[0].ends_with("GlobalGuard"));
        assert!(guard_names[1].ends_with("TestGuard"));
        assert!(guard_names[2].ends_with("ActionGuard"));

        drop(table);
        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn same_middleware_type_across_disjoint_routes_initializes_once() {
        let _lock = PIPELINE_TEST_LOCK.lock().await;
        SHARED_MESSAGE_MIDDLEWARE_INITIALIZATIONS.store(0, Ordering::SeqCst);
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let shared = WebSocketMessageMiddlewareRegistration::of::<SharedMessageMiddleware>();
        let table = materialize_websocket_controllers_with_pipeline(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![
                message_with_components("first", "fixture::first", vec![shared], Vec::new()),
                message_with_components("second", "fixture::second", vec![shared], Vec::new()),
            ],
            app.services(),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect("disjoint route plans materialize");

        assert_eq!(
            SHARED_MESSAGE_MIDDLEWARE_INITIALIZATIONS.load(Ordering::SeqCst),
            1
        );
        assert!(table.find_action("chat", "first").is_some());
        assert!(table.find_action("chat", "second").is_some());
        drop(table);
        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn oversized_effective_plan_fails_before_any_middleware_constructor() {
        SHARED_MESSAGE_MIDDLEWARE_INITIALIZATIONS.store(0, Ordering::SeqCst);
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let middleware = WebSocketMessageMiddlewareRegistration::of::<SharedMessageMiddleware>();
        let result = materialize_websocket_controllers_with_pipeline(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![message("send", "fixture::oversized", None)],
            app.services(),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            vec![middleware; lily_middleware::MAX_HTTP_MIDDLEWARES + 1],
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await;

        assert!(matches!(
            result,
            Err(
                WebSocketControllerMaterializationError::InvalidMiddlewarePlan {
                    source: crate::middleware::MiddlewareConfigError::TooManyMiddlewares { .. },
                    ..
                }
            )
        ));
        assert_eq!(
            SHARED_MESSAGE_MIDDLEWARE_INITIALIZATIONS.load(Ordering::SeqCst),
            0
        );
        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn every_app_owned_constructor_runs_outside_the_callers_ambient_scope() {
        let observations = ProcessContext::scope(ProcessContext::new(), async {
            assert!(ProcessContext::current().is_some());
            let mut observations = Vec::new();
            for _component in ["controller", "codec", "middleware", "guard"] {
                let observed = run_root_initialization(async {
                    Ok::<bool, ()>(ProcessContext::current().is_some())
                })
                .await
                .expect("root constructor task joins")
                .expect("fixture constructor succeeds");
                observations.push(observed);
            }
            observations
        })
        .await;

        assert_eq!(observations, [false, false, false, false]);
    }

    #[tokio::test]
    async fn controller_and_action_timeouts_are_bounded_before_construction() {
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let mut controller = WebSocketControllerRegistration::of::<TestController>();
        controller.metadata.timeout = Some(Duration::from_secs(
            crate::server::MAX_EXECUTION_TIMEOUT_SECS + 1,
        ));
        let controller_error = materialize_websocket_controllers(
            vec![controller],
            vec![message("send", "fixture::controller_timeout", None)],
            Arc::clone(&app.services()),
        )
        .await;
        assert!(matches!(
            controller_error,
            Err(WebSocketControllerMaterializationError::InvalidTimeout { .. })
        ));

        let mut action = message("send", "fixture::action_timeout", None);
        action.metadata.timeout = Some(Duration::from_secs(
            crate::server::MAX_EXECUTION_TIMEOUT_SECS + 1,
        ));
        let action_error = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![action],
            app.services(),
        )
        .await;
        assert!(matches!(
            action_error,
            Err(WebSocketControllerMaterializationError::InvalidTimeout { .. })
        ));
        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn duplicate_effective_middleware_and_guard_plans_fail_closed() {
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let mut middleware_registration = WebSocketControllerRegistration::of::<TestController>();
        middleware_registration.metadata.message_middleware =
            vec![WebSocketMessageMiddlewareRegistration::of::<GlobalMiddleware>()];
        let middleware_error = materialize_websocket_controllers_with_pipeline(
            vec![middleware_registration],
            vec![message("send", "fixture::middleware_duplicate", None)],
            Arc::clone(&app.services()),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            vec![WebSocketMessageMiddlewareRegistration::of::<GlobalMiddleware>()],
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect_err("duplicate effective middleware fails");
        assert!(matches!(
            middleware_error,
            WebSocketControllerMaterializationError::DuplicateComponent {
                component_kind: "effective message middleware",
                ..
            }
        ));

        let guard_error = materialize_websocket_controllers_with_pipeline(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![message("send", "fixture::guard_duplicate", None)],
            app.services(),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            vec![WebSocketGuardRegistration::of::<TestGuard>()],
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect_err("duplicate effective guard fails");
        assert!(matches!(
            guard_error,
            WebSocketControllerMaterializationError::DuplicateComponent {
                component_kind: "effective guard",
                ..
            }
        ));

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn pipeline_initialization_failures_and_panics_are_typed_and_secret_safe() {
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let error = materialize_websocket_controllers_with_pipeline(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![message_with_components(
                "failure",
                "fixture::middleware_failure",
                vec![WebSocketMessageMiddlewareRegistration::of::<
                    FailingMessageMiddleware,
                >()],
                Vec::new(),
            )],
            Arc::clone(&app.services()),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect_err("middleware initialization failure aborts materialization");
        assert!(matches!(
            error,
            WebSocketControllerMaterializationError::MiddlewareInitialization {
                source: WsMiddlewareInitError::InvalidConfiguration,
                ..
            }
        ));

        let panic_error = materialize_websocket_controllers_with_pipeline(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![message_with_components(
                "panic",
                "fixture::middleware_panic",
                vec![WebSocketMessageMiddlewareRegistration::of::<
                    PanickingMessageMiddleware,
                >()],
                Vec::new(),
            )],
            app.services(),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect_err("middleware initialization panic aborts materialization");
        assert!(matches!(
            panic_error,
            WebSocketControllerMaterializationError::MiddlewareInitializationPanicked { .. }
        ));
        assert!(!panic_error.to_string().contains("fixture secret"));

        let guard_error = materialize_websocket_controllers_with_pipeline(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![message_with_components(
                "guard-failure",
                "fixture::guard_failure",
                Vec::new(),
                vec![WebSocketGuardRegistration::of::<FailingGuard>()],
            )],
            Arc::clone(&app.services()),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect_err("guard initialization failure aborts materialization");
        assert!(matches!(
            guard_error,
            WebSocketControllerMaterializationError::GuardInitialization {
                source: GuardInitializationError::MissingDependency,
                ..
            }
        ));

        let guard_panic = materialize_websocket_controllers_with_pipeline(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![message_with_components(
                "guard-panic",
                "fixture::guard_panic",
                Vec::new(),
                vec![WebSocketGuardRegistration::of::<PanickingGuard>()],
            )],
            app.services(),
            WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
            Arc::new(RegistryNoopWsMiddlewareObserver),
        )
        .await
        .expect_err("guard initialization panic aborts materialization");
        assert!(matches!(
            guard_panic,
            WebSocketControllerMaterializationError::GuardInitializationPanicked { .. }
        ));
        assert!(!guard_panic.to_string().contains("fixture secret"));

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn duplicate_missing_and_invalid_metadata_fail_closed() {
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let extensions = app.services();

        let duplicate = materialize_websocket_controllers(
            vec![
                WebSocketControllerRegistration::of::<TestController>(),
                WebSocketControllerRegistration::of::<TestController>(),
            ],
            Vec::new(),
            Arc::clone(&extensions),
        )
        .await
        .expect_err("duplicate controller metadata fails");
        assert!(matches!(
            duplicate,
            WebSocketControllerMaterializationError::DuplicateRegistration { .. }
        ));

        let missing = materialize_websocket_controllers(
            Vec::new(),
            vec![message("send", "fixture::missing", None)],
            Arc::clone(&extensions),
        )
        .await
        .expect_err("missing controller metadata fails");
        assert!(matches!(
            missing,
            WebSocketControllerMaterializationError::MissingRegistration { .. }
        ));

        let duplicate_action = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![
                message("send", "fixture::first", None),
                message("send", "fixture::second", None),
            ],
            Arc::clone(&extensions),
        )
        .await
        .expect_err("duplicate event fails");
        assert!(matches!(
            duplicate_action,
            WebSocketControllerMaterializationError::DuplicateOperation { .. }
        ));

        let middleware_operation = PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some("send"),
            "fixture::middleware",
            WebSocketActionRegistration::of::<TestController>(bind_test_message),
            WebSocketOperationMetadata::new(
                vec![WebSocketMessageMiddlewareRegistration::of::<Middleware>()],
                Vec::new(),
                None,
                None,
                Default::default(),
            ),
        );
        let table = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![middleware_operation],
            extensions,
        )
        .await
        .expect("typed message middleware metadata is materialized");
        assert!(
            !table
                .find_action("chat", "send")
                .expect("message action exists")
                .middleware_chain()
                .is_empty()
        );

        app.close().await.expect("application container closes");
    }

    #[test]
    fn registration_function_panics_become_typed_snapshot_errors() {
        let registrations: [WebSocketControllerRegistrationFn; 1] = [panicking_registration];
        let error = snapshot_controller_registrations(&registrations)
            .expect_err("controller registration panic must not escape the snapshot boundary");
        assert_eq!(
            error,
            WebSocketControllerMaterializationError::RegistrationPanicked {
                registry: "controller",
                index: 0,
            }
        );

        let operations: [PendingWebSocketOperationRegistrationFn; 1] =
            [panicking_operation_registration];
        let error = snapshot_pending_operations(&operations)
            .err()
            .expect("operation registration panic must not escape the snapshot boundary");
        assert_eq!(
            error,
            WebSocketControllerMaterializationError::RegistrationPanicked {
                registry: "operation",
                index: 0,
            }
        );
    }

    #[tokio::test]
    async fn invalid_duplicate_namespace_and_duplicate_lifecycle_fail_closed() {
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let extensions = app.services();

        let invalid_namespace = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<
                InvalidNamespaceController,
            >()],
            Vec::new(),
            Arc::clone(&extensions),
        )
        .await
        .expect_err("root wildcard namespace must be rejected");
        assert_eq!(
            invalid_namespace,
            WebSocketControllerMaterializationError::InvalidNamespace {
                controller: std::any::type_name::<InvalidNamespaceController>(),
                namespace: "/".to_owned(),
            }
        );

        let duplicate_namespace = materialize_websocket_controllers(
            vec![
                WebSocketControllerRegistration::of::<DuplicateNamespaceFirst>(),
                WebSocketControllerRegistration::of::<DuplicateNamespaceSecond>(),
            ],
            Vec::new(),
            Arc::clone(&extensions),
        )
        .await
        .expect_err("duplicate exact namespace must be rejected");
        assert!(matches!(
            duplicate_namespace,
            WebSocketControllerMaterializationError::DuplicateNamespace {
                namespace,
                first_controller: _,
                duplicate_controller: _,
            } if namespace == "duplicate"
        ));

        let oversized_event = Box::leak(
            "a".repeat(MAX_CANONICAL_EVENT_BYTES - "chat".len())
                .into_boxed_str(),
        );
        let oversized_route = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![generic_message::<TestController>(
                oversized_event,
                "fixture::TestController::oversized_route",
            )],
            Arc::clone(&extensions),
        )
        .await
        .expect_err("combined namespace:event wire route must remain bounded");
        assert_eq!(
            oversized_route,
            WebSocketControllerMaterializationError::RouteTooLong {
                namespace: "chat".to_owned(),
                event: oversized_event.to_owned(),
                maximum_bytes: MAX_CANONICAL_EVENT_BYTES,
            }
        );

        let duplicate_lifecycle = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![
                generic_connected::<TestController>("fixture::TestController::first_connected"),
                generic_connected::<TestController>("fixture::TestController::second_connected"),
            ],
            Arc::clone(&extensions),
        )
        .await
        .expect_err("duplicate connected hook must be rejected");
        assert_eq!(
            duplicate_lifecycle,
            WebSocketControllerMaterializationError::DuplicateLifecycle {
                namespace: "chat".to_owned(),
                kind: "connected",
            }
        );

        let duplicate_guard_operation = PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some("duplicate-guard"),
            "fixture::TestController::duplicate_guard",
            WebSocketActionRegistration::of::<TestController>(bind_test_message),
            WebSocketOperationMetadata::new(
                Vec::new(),
                vec![WebSocketGuardRegistration::of::<TestGuard>()],
                None,
                None,
                Default::default(),
            ),
        );
        let duplicate_guard = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<TestController>()],
            vec![duplicate_guard_operation],
            extensions,
        )
        .await
        .expect_err("same guard at controller and action scopes must be rejected");
        assert!(matches!(
            duplicate_guard,
            WebSocketControllerMaterializationError::DuplicateComponent {
                component_kind: "effective guard",
                component: _,
                ..
            }
        ));

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn controller_initialization_and_generated_binding_failures_are_bounded() {
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let extensions = app.services();

        let initialization = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<InitFailureController>()],
            vec![generic_message::<InitFailureController>(
                "run",
                "fixture::InitFailureController::run",
            )],
            Arc::clone(&extensions),
        )
        .await
        .expect_err("controller initialization error must stop the build");
        assert_eq!(
            initialization,
            WebSocketControllerMaterializationError::Initialization {
                controller: std::any::type_name::<InitFailureController>(),
                source: WebSocketControllerInitError::Dependency,
            }
        );
        let public_message = initialization.to_string();
        assert!(!public_message.contains("administrator"));
        assert!(!public_message.contains("do-not-publish"));
        assert!(!public_message.contains("database.internal"));

        let initialization_panic = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<InitPanicController>()],
            vec![generic_message::<InitPanicController>(
                "run",
                "fixture::InitPanicController::run",
            )],
            Arc::clone(&extensions),
        )
        .await
        .expect_err("controller initialization panic must become an internal category");
        assert_eq!(
            initialization_panic,
            WebSocketControllerMaterializationError::Initialization {
                controller: std::any::type_name::<InitPanicController>(),
                source: WebSocketControllerInitError::Internal,
            }
        );

        let binder_panic = PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some("run"),
            "fixture::BinderPanicController::run",
            WebSocketActionRegistration::of::<BinderPanicController>(bind_panics),
            WebSocketOperationMetadata::empty(),
        );
        let binder_panic = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<BinderPanicController>()],
            vec![binder_panic],
            Arc::clone(&extensions),
        )
        .await
        .expect_err("generated binder panic must not escape app build");
        assert_eq!(
            binder_panic,
            WebSocketControllerMaterializationError::BindingPanicked {
                controller: std::any::type_name::<BinderPanicController>(),
                operation: "fixture::BinderPanicController::run",
            }
        );

        let kind_mismatch = PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some("run"),
            "fixture::KindMismatchController::run",
            WebSocketActionRegistration::of::<KindMismatchController>(
                bind_generic_connected::<KindMismatchController>,
            ),
            WebSocketOperationMetadata::empty(),
        );
        let kind_mismatch = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<KindMismatchController>()],
            vec![kind_mismatch],
            extensions,
        )
        .await
        .expect_err("binder result kind must match static operation metadata");
        assert_eq!(
            kind_mismatch,
            WebSocketControllerMaterializationError::BoundKindMismatch {
                operation: "fixture::KindMismatchController::run",
                declared: "message",
                bound: "connected",
            }
        );

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn operationless_controllers_are_absent_and_partial_build_instances_drop() {
        UNUSED_INITIALIZATIONS.store(0, Ordering::SeqCst);
        ROLLBACK_DROPS.store(0, Ordering::SeqCst);
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let extensions = app.services();

        let table = materialize_websocket_controllers(
            vec![
                WebSocketControllerRegistration::of::<UnusedController>(),
                WebSocketControllerRegistration::of::<RequiredController>(),
            ],
            vec![generic_message::<RequiredController>(
                "required",
                "fixture::RequiredController::required",
            )],
            Arc::clone(&extensions),
        )
        .await
        .expect("the required controller materializes");
        assert_eq!(UNUSED_INITIALIZATIONS.load(Ordering::SeqCst), 0);
        assert!(!table.contains_namespace("unused"));
        assert!(table.contains_namespace("required"));
        drop(table);

        let rollback = materialize_websocket_controllers(
            vec![
                WebSocketControllerRegistration::of::<RollbackFailingController>(),
                WebSocketControllerRegistration::of::<RollbackDropController>(),
            ],
            vec![
                generic_message::<RollbackDropController>(
                    "run",
                    "fixture::RollbackDropController::run",
                ),
                generic_message::<RollbackFailingController>(
                    "run",
                    "fixture::RollbackFailingController::run",
                ),
            ],
            extensions,
        )
        .await
        .expect_err("later initialization failure must roll back earlier controller values");
        assert!(matches!(
            rollback,
            WebSocketControllerMaterializationError::Initialization {
                controller,
                source: WebSocketControllerInitError::Internal,
            } if controller == std::any::type_name::<RollbackFailingController>()
        ));
        assert_eq!(ROLLBACK_DROPS.load(Ordering::SeqCst), 1);

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn codec_precedence_selects_controller_frame_and_action_controller_app_payloads() {
        let _codec_test_guard = CODEC_TEST_LOCK.lock().await;
        APP_FRAME_CODEC_INITIALIZATIONS.store(0, Ordering::SeqCst);
        APP_PAYLOAD_CODEC_INITIALIZATIONS.store(0, Ordering::SeqCst);
        CONTROLLER_FRAME_CODEC_INITIALIZATIONS.store(0, Ordering::SeqCst);
        CONTROLLER_PAYLOAD_CODEC_INITIALIZATIONS.store(0, Ordering::SeqCst);
        ACTION_PAYLOAD_CODEC_INITIALIZATIONS.store(0, Ordering::SeqCst);

        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let table = materialize_websocket_controllers_with_codecs(
            vec![
                WebSocketControllerRegistration::of::<AppCodecController>(),
                WebSocketControllerRegistration::of::<OverrideCodecController>(),
            ],
            vec![
                codec_message::<AppCodecController>(
                    "run",
                    "fixture::AppCodecController::run",
                    None,
                ),
                codec_message::<OverrideCodecController>(
                    "inherited",
                    "fixture::OverrideCodecController::inherited",
                    None,
                ),
                codec_message::<OverrideCodecController>(
                    "overridden",
                    "fixture::OverrideCodecController::overridden",
                    Some(WebSocketPayloadCodecRegistration::of::<ActionPayloadCodec>()),
                ),
            ],
            app.services(),
            WebSocketFrameCodecRegistration::of::<AppFrameCodec>(),
            WebSocketPayloadCodecRegistration::of::<AppPayloadCodec>(),
        )
        .await
        .expect("codec precedence fixture materializes");

        assert_eq!(
            frame_codec_marker(
                table
                    .frame_codec("app-codec")
                    .expect("app-default controller exists"),
            ),
            "app-frame"
        );
        assert_eq!(
            frame_codec_marker(
                table
                    .frame_codec("override-codec")
                    .expect("controller-override exists"),
            ),
            "controller-frame"
        );
        assert_eq!(
            payload_codec_marker(
                table
                    .find_action("app-codec", "run")
                    .expect("app payload action exists")
                    .payload_codec(),
            ),
            "app-payload"
        );
        assert_eq!(
            payload_codec_marker(
                table
                    .find_action("override-codec", "inherited")
                    .expect("controller payload action exists")
                    .payload_codec(),
            ),
            "controller-payload"
        );
        assert_eq!(
            payload_codec_marker(
                table
                    .find_action("override-codec", "overridden")
                    .expect("action payload override exists")
                    .payload_codec(),
            ),
            "action-payload"
        );

        assert_eq!(APP_FRAME_CODEC_INITIALIZATIONS.load(Ordering::SeqCst), 1);
        assert_eq!(APP_PAYLOAD_CODEC_INITIALIZATIONS.load(Ordering::SeqCst), 1);
        assert_eq!(
            CONTROLLER_FRAME_CODEC_INITIALIZATIONS.load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            CONTROLLER_PAYLOAD_CODEC_INITIALIZATIONS.load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            ACTION_PAYLOAD_CODEC_INITIALIZATIONS.load(Ordering::SeqCst),
            1
        );

        drop(table);
        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn one_concrete_codec_is_initialized_once_and_shared_across_frame_and_payload_traits() {
        let _codec_test_guard = CODEC_TEST_LOCK.lock().await;
        SHARED_CODEC_INITIALIZATIONS.store(0, Ordering::SeqCst);
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let table = materialize_websocket_controllers_with_codecs(
            vec![WebSocketControllerRegistration::of::<SharedCodecController>()],
            vec![codec_message::<SharedCodecController>(
                "run",
                "fixture::SharedCodecController::run",
                None,
            )],
            app.services(),
            WebSocketFrameCodecRegistration::of::<AppFrameCodec>(),
            WebSocketPayloadCodecRegistration::of::<AppPayloadCodec>(),
        )
        .await
        .expect("shared codec fixture materializes");

        let frame_codec = table
            .frame_codec("shared-codec")
            .expect("shared frame codec exists");
        let payload_codec = table
            .find_action("shared-codec", "run")
            .expect("shared payload action exists")
            .payload_codec();
        assert_eq!(frame_codec_marker(Arc::clone(&frame_codec)), "shared-frame");
        assert_eq!(
            payload_codec_marker(Arc::clone(&payload_codec)),
            "shared-payload"
        );
        assert_eq!(SHARED_CODEC_INITIALIZATIONS.load(Ordering::SeqCst), 1);
        assert_eq!(
            Arc::as_ptr(&frame_codec) as *const (),
            Arc::as_ptr(&payload_codec) as *const (),
            "both trait objects must retain the same cached codec allocation"
        );

        drop(table);
        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn codec_initialization_errors_and_panics_become_secret_safe_typed_build_failures() {
        let _codec_test_guard = CODEC_TEST_LOCK.lock().await;
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let extensions = app.services();

        let initialization = materialize_websocket_controllers_with_codecs(
            vec![WebSocketControllerRegistration::of::<
                CodecInitFailureController,
            >()],
            vec![codec_message::<CodecInitFailureController>(
                "run",
                "fixture::CodecInitFailureController::run",
                None,
            )],
            Arc::clone(&extensions),
            WebSocketFrameCodecRegistration::of::<AppFrameCodec>(),
            WebSocketPayloadCodecRegistration::of::<AppPayloadCodec>(),
        )
        .await
        .expect_err("codec initialization error must stop app materialization");
        assert_eq!(
            initialization,
            WebSocketControllerMaterializationError::CodecInitialization {
                codec_kind: "frame",
                codec: std::any::type_name::<FailingFrameCodec>(),
                source: WebSocketCodecInitError::Dependency,
            }
        );
        let public_message = initialization.to_string();
        assert!(!public_message.contains("administrator"));
        assert!(!public_message.contains("do-not-publish"));
        assert!(!public_message.contains("database.internal"));
        assert!(!public_message.contains("production"));

        let initialization_panic = materialize_websocket_controllers_with_codecs(
            vec![WebSocketControllerRegistration::of::<
                CodecInitPanicController,
            >()],
            vec![codec_message::<CodecInitPanicController>(
                "run",
                "fixture::CodecInitPanicController::run",
                None,
            )],
            extensions,
            WebSocketFrameCodecRegistration::of::<AppFrameCodec>(),
            WebSocketPayloadCodecRegistration::of::<AppPayloadCodec>(),
        )
        .await
        .expect_err("codec initialization panic must not escape app materialization");
        assert_eq!(
            initialization_panic,
            WebSocketControllerMaterializationError::CodecInitializationPanicked {
                codec_kind: "payload",
                codec: std::any::type_name::<PanickingPayloadCodec>(),
            }
        );

        app.close().await.expect("application container closes");
    }

    #[tokio::test]
    async fn duplicate_effective_codec_types_fail_closed_at_every_precedence_boundary() {
        let _codec_test_guard = CODEC_TEST_LOCK.lock().await;
        let app = ApplicationContainer::build()
            .await
            .expect("application container builds");
        let extensions = app.services();

        let duplicate_frame = materialize_websocket_controllers_with_codecs(
            vec![WebSocketControllerRegistration::of::<
                DuplicateAppFrameController,
            >()],
            vec![codec_message::<DuplicateAppFrameController>(
                "run",
                "fixture::DuplicateAppFrameController::run",
                None,
            )],
            Arc::clone(&extensions),
            WebSocketFrameCodecRegistration::of::<AppFrameCodec>(),
            WebSocketPayloadCodecRegistration::of::<AppPayloadCodec>(),
        )
        .await
        .expect_err("controller/app frame duplication must fail closed");
        assert_eq!(
            duplicate_frame,
            WebSocketControllerMaterializationError::DuplicateComponent {
                operation: std::any::type_name::<DuplicateAppFrameController>(),
                component_kind: "effective frame codec",
                component: std::any::type_name::<AppFrameCodec>(),
            }
        );

        let duplicate_payload = materialize_websocket_controllers_with_codecs(
            vec![WebSocketControllerRegistration::of::<
                DuplicateAppPayloadController,
            >()],
            vec![codec_message::<DuplicateAppPayloadController>(
                "run",
                "fixture::DuplicateAppPayloadController::run",
                None,
            )],
            Arc::clone(&extensions),
            WebSocketFrameCodecRegistration::of::<AppFrameCodec>(),
            WebSocketPayloadCodecRegistration::of::<AppPayloadCodec>(),
        )
        .await
        .expect_err("controller/app payload duplication must fail closed");
        assert_eq!(
            duplicate_payload,
            WebSocketControllerMaterializationError::DuplicateComponent {
                operation: std::any::type_name::<DuplicateAppPayloadController>(),
                component_kind: "effective payload codec",
                component: std::any::type_name::<AppPayloadCodec>(),
            }
        );

        let duplicate_action = materialize_websocket_controllers_with_codecs(
            vec![WebSocketControllerRegistration::of::<OverrideCodecController>()],
            vec![codec_message::<OverrideCodecController>(
                "run",
                "fixture::OverrideCodecController::duplicate_payload",
                Some(WebSocketPayloadCodecRegistration::of::<
                    ControllerPayloadCodec,
                >()),
            )],
            extensions,
            WebSocketFrameCodecRegistration::of::<AppFrameCodec>(),
            WebSocketPayloadCodecRegistration::of::<AppPayloadCodec>(),
        )
        .await
        .expect_err("action/inherited payload duplication must fail closed");
        assert_eq!(
            duplicate_action,
            WebSocketControllerMaterializationError::DuplicateComponent {
                operation: "fixture::OverrideCodecController::duplicate_payload",
                component_kind: "effective payload codec",
                component: std::any::type_name::<ControllerPayloadCodec>(),
            }
        );

        app.close().await.expect("application container closes");
    }
}
