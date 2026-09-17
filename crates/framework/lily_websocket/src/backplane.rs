//! Transport-neutral distributed dispatch contracts for WebSocket scale-out.
//!
//! Application code selects one backplane type through
//! [`crate::WsAppBuilder::backplane`]. Lily creates that type once from the
//! application [`Extensions`], owns its lifecycle, and keeps the node-local
//! [`crate::ConnectionManager`] as the only connection and room authority.
//! Backplane implementations transport opaque [`WebSocketBackplaneFrame`]
//! values; they do not parse or recreate Lily's private routing protocol.
//!
//! Lily's current private frame generation is `v4`, which requires explicit namespace
//! authority for every target and rejects legacy global targets.
//! Nodes that share one transport must
//! switch protocol generations together; adapters should isolate incompatible
//! generations at their channel or topic boundary instead of attempting a
//! partial cross-version decode.

mod outbound;

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{FutureExt, future::BoxFuture, future::Shared};
use lily_injection::{Extensions, ProcessContext};
use lily_monitoring::{HealthRegistry, HealthStatus};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use uuid::Uuid;

use crate::connection::{
    BroadcastMessage, BroadcastReport, BroadcastTarget, ConnectionError, ConnectionManager,
    MAX_BROADCAST_CONNECTION_TARGETS as MAX_BACKPLANE_CONNECTION_TARGETS,
    MAX_BROADCAST_EXCLUSIONS as MAX_BACKPLANE_EXCLUSIONS,
    MAX_BROADCAST_ROOMS as MAX_BACKPLANE_ROOMS, PrincipalId, validate_broadcast_namespace,
};
use crate::request::{WsMessageBody, WsWireFormat, is_canonical_room};

const BACKPLANE_PROTOCOL_VERSION: u16 = 4;
// Includes the worst-case JSON escaping expansion for every bounded room,
// namespace, UUID and trace field in the framework-owned envelope.
const BACKPLANE_FRAME_OVERHEAD_BYTES: usize = 256 * 1024;
const MAX_TRACEPARENT_BYTES: usize = 128;
const DEDUPE_CAPACITY: usize = 4096;
const DEDUPE_TTL: Duration = Duration::from_secs(60);

pub(crate) const WS_BACKPLANE_PUBLISHER_HEALTH_CHECK: &str = "backplane.publisher";
pub(crate) const WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK: &str = "backplane.subscriber";

const DISPATCHER_ACCEPTING: u8 = 0;
const DISPATCHER_DRAINING: u8 = 1;
const DISPATCHER_CLOSING: u8 = 2;
const DISPATCHER_CLOSED: u8 = 3;
const SUBSCRIPTION_NOT_STARTED: u8 = 0;
const SUBSCRIPTION_STARTING: u8 = 1;
const SUBSCRIPTION_READY: u8 = 2;
const SUBSCRIPTION_RECONNECTING: u8 = 3;
const SUBSCRIPTION_UNAVAILABLE: u8 = 4;
const SUBSCRIPTION_STOPPED: u8 = 5;

/// Whether a configured backplane participates in application readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackplaneRequirement {
    /// Initialization failure rejects app build and runtime loss lowers readiness.
    Required,
    /// Initialization failure permits local-only operation with degraded health.
    Optional,
}

/// Stable category for one backplane transport failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketBackplaneErrorKind {
    /// The implementation's bounded publish admission path is full.
    Saturated,
    /// The selected transport is not currently available.
    Unavailable,
    /// The transport could not publish an already validated frame.
    Publish,
    /// The inbound subscription could not continue receiving frames.
    Receive,
    /// The backplane could not complete ordered shutdown.
    Shutdown,
    /// Another provider-specific transport operation failed.
    Transport,
    /// A Lily-owned bounded provider operation exceeded its deadline.
    TimedOut,
    /// Provider code panicked at a framework-owned boundary.
    Panicked,
    /// Shutdown interrupted a provider operation after local delivery.
    Interrupted,
}

impl WebSocketBackplaneErrorKind {
    /// Stable low-cardinality identifier suitable for telemetry.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Saturated => "saturated",
            Self::Unavailable => "unavailable",
            Self::Publish => "publish_failed",
            Self::Receive => "receive_failed",
            Self::Shutdown => "shutdown_failed",
            Self::Transport => "transport_failed",
            Self::TimedOut => "timed_out",
            Self::Panicked => "panicked",
            Self::Interrupted => "interrupted",
        }
    }
}

/// Error returned by a running backplane implementation.
///
/// Provider diagnostics are deliberately excluded from `Debug`, `Display`
/// and framework tracing. Adapters must log their own bounded, redacted
/// diagnostics and must never include credentials or message payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("WebSocket backplane failure: {kind:?}")]
pub struct WebSocketBackplaneError {
    kind: WebSocketBackplaneErrorKind,
}

impl WebSocketBackplaneError {
    /// Creates one stable, low-cardinality provider error.
    pub const fn new(kind: WebSocketBackplaneErrorKind) -> Self {
        Self { kind }
    }

    /// Maps a provider source without retaining or exposing its diagnostic.
    pub fn from_source(kind: WebSocketBackplaneErrorKind, _source: impl fmt::Display) -> Self {
        Self::new(kind)
    }

    /// Stable category of this failure.
    pub const fn kind(&self) -> WebSocketBackplaneErrorKind {
        self.kind
    }
}

/// Stable category for backplane construction failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketBackplaneInitErrorKind {
    /// A dependency could not be resolved from application DI.
    Dependency,
    /// Application/provider configuration was invalid.
    Configuration,
    /// The provider transport could not be initialized.
    Transport,
    /// Lily's bounded construction deadline expired.
    TimedOut,
    /// Provider construction panicked.
    Panicked,
}

/// Error returned while constructing the selected backplane from application DI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("WebSocket backplane initialization failed: {kind:?}")]
pub struct WebSocketBackplaneInitError {
    kind: WebSocketBackplaneInitErrorKind,
}

impl WebSocketBackplaneInitError {
    /// Creates a bounded initialization error category.
    pub const fn new(kind: WebSocketBackplaneInitErrorKind) -> Self {
        Self { kind }
    }

    /// Maps a DI resolution error into the initialization boundary.
    pub fn dependency(_error: impl fmt::Display) -> Self {
        Self::new(WebSocketBackplaneInitErrorKind::Dependency)
    }

    /// Stable category of this initialization failure.
    pub const fn kind(&self) -> WebSocketBackplaneInitErrorKind {
        self.kind
    }
}

/// Opaque, validated Lily frame supplied to a backplane transport.
///
/// The constructor remains framework-private. A custom adapter forwards
/// [`Self::as_bytes`] without interpreting the routing envelope.
#[derive(Clone)]
pub struct WebSocketBackplaneFrame {
    bytes: Arc<[u8]>,
}

impl fmt::Debug for WebSocketBackplaneFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketBackplaneFrame")
            .field("len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl WebSocketBackplaneFrame {
    /// Encoded frame bytes to publish without modification.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Encoded frame length in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether this frame contains no bytes. Lily never constructs an empty frame.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Runtime event returned by a backplane's single inbound subscription loop.
#[non_exhaustive]
pub enum WebSocketBackplaneEvent {
    /// One opaque frame received from another application node.
    Frame(Vec<u8>),
    /// The provider has an active subscription and can receive distributed work.
    SubscriptionReady,
    /// The provider temporarily lost its subscription and is reconnecting.
    SubscriptionReconnecting,
    /// The provider is currently unavailable but may continue retrying internally.
    SubscriptionUnavailable,
    /// The outbound publisher is available after a provider-observed transition.
    PublisherReady,
    /// The outbound publisher is reconnecting after a provider-observed transition.
    PublisherReconnecting,
    /// The outbound publisher is unavailable after a provider-observed transition.
    PublisherUnavailable,
}

impl fmt::Debug for WebSocketBackplaneEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame(bytes) => formatter
                .debug_struct("Frame")
                .field("len", &bytes.len())
                .finish(),
            Self::SubscriptionReady => formatter.write_str("SubscriptionReady"),
            Self::SubscriptionReconnecting => formatter.write_str("SubscriptionReconnecting"),
            Self::SubscriptionUnavailable => formatter.write_str("SubscriptionUnavailable"),
            Self::PublisherReady => formatter.write_str("PublisherReady"),
            Self::PublisherReconnecting => formatter.write_str("PublisherReconnecting"),
            Self::PublisherUnavailable => formatter.write_str("PublisherUnavailable"),
        }
    }
}

/// Lily-owned inbound allocation admission passed to each receive operation.
///
/// A provider must enforce this limit before copying broker payload bytes into
/// a [`WebSocketBackplaneEvent::Frame`]. Lily validates the returned frame a
/// second time, but that later check cannot undo an oversized allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebSocketBackplaneInboundAdmission {
    maximum_frame_bytes: usize,
}

impl WebSocketBackplaneInboundAdmission {
    /// Maximum encoded Lily frame size accepted by this app instance.
    pub const fn maximum_frame_bytes(self) -> usize {
        self.maximum_frame_bytes
    }
}

/// Provider acknowledgement that a frame was accepted by its publish boundary.
///
/// This is not a remote client delivery acknowledgement and deliberately
/// contains no global subscriber or connection count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebSocketBackplanePublishReceipt {
    _private: (),
}

impl WebSocketBackplanePublishReceipt {
    /// Records transport acceptance without claiming downstream delivery.
    pub const fn accepted() -> Self {
        Self { _private: () }
    }
}

/// Transport-neutral WebSocket backplane lifecycle.
///
/// `receive` is invoked sequentially by one Lily-owned task while `publish`
/// may be invoked concurrently. Implementations
/// should keep transient reconnect loops inside that method and emit
/// [`WebSocketBackplaneEvent::SubscriptionReconnecting`] or
/// [`WebSocketBackplaneEvent::SubscriptionUnavailable`] when state changes. The future
/// is also the provider's runtime event stream: an adapter that can observe
/// publisher connectivity independently of a publish attempt must emit the
/// corresponding `Publisher*` transition. Implementations may multiplex those
/// transitions with inbound subscription events without running another Lily
/// receive loop. The future
/// must be cancellation-safe because application shutdown may abort the
/// receiver before calling [`Self::close`]. The adapter channel is a private
/// framework transport boundary: it must use environment/application-specific
/// isolation, broker ACLs and authenticated encrypted transport. Lily envelope
/// validation is not authorization or message authentication.
///
/// A minimal custom adapter keeps Lily frames opaque:
///
/// ```
/// use std::sync::Arc;
/// use lily_websocket::{
///     Extensions, WebSocketBackplane, WebSocketBackplaneError,
///     WebSocketBackplaneEvent, WebSocketBackplaneFrame,
///     WebSocketBackplaneInboundAdmission, WebSocketBackplaneInitError,
///     WebSocketBackplanePublishReceipt, async_trait,
/// };
///
/// struct ApplicationBackplane;
///
/// #[async_trait]
/// impl WebSocketBackplane for ApplicationBackplane {
///     async fn new(
///         _extensions: Arc<Extensions>,
///     ) -> Result<Self, WebSocketBackplaneInitError> {
///         Ok(Self)
///     }
///
///     async fn publish(
///         &self,
///         frame: WebSocketBackplaneFrame,
///     ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
///         let _opaque_bytes = frame.as_bytes();
///         Ok(WebSocketBackplanePublishReceipt::accepted())
///     }
///
///     async fn receive(
///         &self,
///         admission: WebSocketBackplaneInboundAdmission,
///     ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
///         let _maximum = admission.maximum_frame_bytes();
///         std::future::pending().await
///     }
/// }
/// ```
#[async_trait]
pub trait WebSocketBackplane: Send + Sync + 'static {
    /// Constructs the selected implementation once from the application DI root.
    ///
    /// Success means the outbound publisher is usable. Lily may drop this
    /// future at its startup deadline, so construction must be cancellation-safe
    /// and must not detach work that can later access application DI.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError>
    where
        Self: Sized;

    /// Publishes one opaque Lily frame through the provider's bounded path.
    ///
    /// Implementations own their transport queue and must reject full
    /// admission with [`WebSocketBackplaneErrorKind::Saturated`] rather than
    /// growing an unbounded buffer. A disconnected publisher should return
    /// [`WebSocketBackplaneErrorKind::Unavailable`]. Lily may call this method
    /// concurrently and may drop it on timeout or forced shutdown. Transport
    /// acceptance can therefore be ambiguous after cancellation; adapters must
    /// not perform blind internal retries that can duplicate dispatch.
    async fn publish(
        &self,
        frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError>;

    /// Waits for the next inbound frame or provider availability transition.
    ///
    /// The first frame is accepted only after
    /// [`WebSocketBackplaneEvent::SubscriptionReady`]. A recoverable outage
    /// must be reported with subscription state events while reconnecting
    /// inside the adapter. When outbound connectivity can be observed without
    /// a publish attempt, the same stream must report the corresponding
    /// `Publisher*` transition. Returning `Err` is terminal: Lily marks the
    /// subscriber unhealthy and does not invoke `receive` again. The adapter's
    /// broker prefetch, subscription backlog and internal channel must remain
    /// bounded; it must not hide overload in an unbounded inbound queue.
    async fn receive(
        &self,
        admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError>;

    /// Stops subscription admission and closes provider resources.
    ///
    /// Lily invokes this method at most once from a framework-owned task. A
    /// graceful or forced shutdown waiter may stop waiting without cancelling
    /// the provider close operation; later waiters observe the same terminal
    /// result. Implementations must still be cancellation-safe because runtime
    /// termination or Lily's authoritative hard-timeout reconciliation can
    /// abort the owning task.
    async fn close(&self) -> Result<(), WebSocketBackplaneError> {
        Ok(())
    }
}

type BackplaneFactoryFuture = Pin<
    Box<
        dyn Future<Output = Result<Arc<dyn WebSocketBackplane>, WebSocketBackplaneInitError>>
            + Send,
    >,
>;
type BackplaneFactory = fn(Arc<Extensions>) -> BackplaneFactoryFuture;

#[derive(Clone, Copy)]
pub(crate) struct WebSocketBackplaneRegistration {
    type_name: &'static str,
    requirement: BackplaneRequirement,
    factory: BackplaneFactory,
}

impl WebSocketBackplaneRegistration {
    pub(crate) fn of<B>(requirement: BackplaneRequirement) -> Self
    where
        B: WebSocketBackplane,
    {
        Self {
            type_name: std::any::type_name::<B>(),
            requirement,
            factory: |extensions| {
                Box::pin(async move {
                    AssertUnwindSafe(B::new(extensions))
                        .catch_unwind()
                        .await
                        .map_err(|_| {
                            WebSocketBackplaneInitError::new(
                                WebSocketBackplaneInitErrorKind::Panicked,
                            )
                        })?
                        .map(|backplane| Arc::new(backplane) as Arc<dyn WebSocketBackplane>)
                })
            },
        }
    }

    pub(crate) const fn requirement(self) -> BackplaneRequirement {
        self.requirement
    }

    pub(crate) const fn type_name(self) -> &'static str {
        self.type_name
    }

    pub(crate) async fn materialize(
        self,
        extensions: Arc<Extensions>,
        startup_timeout: Duration,
    ) -> Result<Arc<dyn WebSocketBackplane>, WebSocketBackplaneInitError> {
        tokio::time::timeout(startup_timeout, (self.factory)(extensions))
            .await
            .map_err(|_| {
                WebSocketBackplaneInitError::new(WebSocketBackplaneInitErrorKind::TimedOut)
            })?
    }
}

#[derive(Clone)]
enum BackplaneSlot {
    Disabled,
    ConfiguredUnavailable {
        requirement: BackplaneRequirement,
    },
    Active {
        requirement: BackplaneRequirement,
        backplane: Arc<dyn WebSocketBackplane>,
    },
}

impl BackplaneSlot {
    const fn requirement(&self) -> Option<BackplaneRequirement> {
        match self {
            Self::Disabled => None,
            Self::ConfiguredUnavailable { requirement } | Self::Active { requirement, .. } => {
                Some(*requirement)
            }
        }
    }
}

/// Distributed outcome for one dispatch attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketBackplaneDispatchReceipt {
    /// The validated explicit selection contains no recipients, so no local
    /// delivery or provider publication was needed, regardless of configuration.
    NoTargets,
    /// No backplane was configured, so only local delivery was attempted.
    Disabled,
    /// The configured transport accepted the frame without guaranteeing delivery.
    Accepted(WebSocketBackplanePublishReceipt),
}

/// Separate local and distributed accounting for one outbound dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebSocketDispatchReceipt {
    local: BroadcastReport,
    backplane: WebSocketBackplaneDispatchReceipt,
}

impl WebSocketDispatchReceipt {
    /// Node-local terminal delivery accounting.
    pub const fn local(&self) -> &BroadcastReport {
        &self.local
    }

    /// Whether publication was unnecessary, disabled, or accepted by the transport.
    pub const fn backplane(&self) -> WebSocketBackplaneDispatchReceipt {
        self.backplane
    }

    /// Whether all known local queues accepted the message and any possible
    /// remote-only explicit target was accepted by the backplane transport.
    pub fn is_fully_accepted(&self) -> bool {
        match self.backplane {
            WebSocketBackplaneDispatchReceipt::NoTargets
            | WebSocketBackplaneDispatchReceipt::Disabled => self.local.is_fully_delivered(),
            WebSocketBackplaneDispatchReceipt::Accepted(_) => {
                self.local.backpressured == 0 && self.local.closed == 0
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BackplaneTarget {
    Namespace {
        namespace: String,
    },
    Room {
        namespace: String,
        room: String,
    },
    Rooms {
        namespace: String,
        rooms: Vec<String>,
    },
    NamespaceConnections {
        namespace: String,
        connection_ids: Vec<Uuid>,
    },
    Principal {
        namespace: String,
        principal_id: PrincipalId,
    },
}

impl From<&BroadcastTarget> for BackplaneTarget {
    fn from(target: &BroadcastTarget) -> Self {
        match target {
            BroadcastTarget::Namespace(namespace) => Self::Namespace {
                namespace: namespace.clone(),
            },
            BroadcastTarget::Room { namespace, room } => Self::Room {
                namespace: namespace.clone(),
                room: room.clone(),
            },
            BroadcastTarget::Rooms { namespace, rooms } => Self::Rooms {
                namespace: namespace.clone(),
                rooms: rooms.clone(),
            },
            BroadcastTarget::NamespaceConnections {
                namespace,
                connection_ids,
            } => Self::NamespaceConnections {
                namespace: namespace.clone(),
                connection_ids: connection_ids.clone(),
            },
            BroadcastTarget::Principal {
                namespace,
                principal_id,
            } => Self::Principal {
                namespace: namespace.clone(),
                principal_id: principal_id.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BackplaneWireFormat {
    Text,
    Binary,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackplaneEnvelope {
    protocol_version: u16,
    message_id: Uuid,
    origin_node_id: Uuid,
    target: BackplaneTarget,
    exclusions: Vec<Uuid>,
    message: WsMessageBody,
    wire_format: BackplaneWireFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    traceparent: Option<String>,
}

#[derive(Serialize)]
struct OutboundBackplaneEnvelope<'a> {
    protocol_version: u16,
    message_id: Uuid,
    origin_node_id: Uuid,
    target: &'a BackplaneTarget,
    exclusions: &'a [Uuid],
    message: &'a WsMessageBody,
    wire_format: BackplaneWireFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    traceparent: Option<&'a str>,
}

/// Validation failure in Lily's private backplane protocol.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WebSocketBackplaneProtocolError {
    /// Encoded input exceeds the effective app message bound plus fixed metadata overhead.
    #[error("backplane frame exceeds its configured byte limit")]
    FrameTooLarge,
    /// Bytes are not a strict Lily backplane envelope.
    #[error("backplane frame is not a valid envelope")]
    InvalidEnvelope,
    /// The embedded application message exceeds this node's configured limit.
    #[error("embedded WebSocket message exceeds its configured byte limit")]
    MessageTooLarge,
    /// The envelope protocol version is unsupported.
    #[error("unsupported backplane protocol version: {actual}")]
    UnsupportedVersion {
        /// Unsupported version read from the envelope.
        actual: u16,
    },
    /// A target is malformed or exceeds a fixed collection/string bound.
    #[error("backplane target is invalid or exceeds its bound")]
    InvalidTarget,
    /// The exclusion list exceeds its fixed cardinality bound.
    #[error("backplane exclusion list exceeds its bound")]
    TooManyExclusions,
    /// The embedded WebSocket message violates its existing wire contract.
    #[error("embedded WebSocket message is invalid")]
    InvalidMessage,
    /// The optional W3C correlation value is malformed or too large.
    #[error("backplane trace context is invalid")]
    InvalidTraceContext,
}

/// A valid envelope can fail locally without violating the wire protocol.
enum WebSocketBackplaneIngressError {
    Protocol(WebSocketBackplaneProtocolError),
    LocalDispatch(ConnectionError),
}

impl fmt::Debug for WebSocketBackplaneIngressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => f.debug_tuple("Protocol").field(error).finish(),
            Self::LocalDispatch(error) => f
                .debug_struct("LocalDispatch")
                .field("reason", &local_dispatch_failure_reason(error))
                .finish(),
        }
    }
}

// Keep the typed ConnectionError internally; diagnostics expose only a fixed
// category, never connection identifiers or arbitrary nested error text.
fn local_dispatch_failure_reason(error: &ConnectionError) -> &'static str {
    match error {
        ConnectionError::InvalidOperation(
            crate::connection::ConnectionOperationError::IdentityLifecycleUnavailable,
        ) => "identity_lifecycle_unavailable",
        _ => "other",
    }
}

struct DedupeWindow {
    order: VecDeque<(Instant, Uuid)>,
    ids: HashSet<Uuid>,
}

impl DedupeWindow {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            ids: HashSet::new(),
        }
    }

    fn seen_or_insert(&mut self, message_id: Uuid, now: Instant) -> bool {
        while self
            .order
            .front()
            .is_some_and(|(seen_at, _)| now.saturating_duration_since(*seen_at) >= DEDUPE_TTL)
        {
            if let Some((_, expired)) = self.order.pop_front() {
                self.ids.remove(&expired);
            }
        }
        if self.ids.contains(&message_id) {
            return true;
        }
        while self.order.len() >= DEDUPE_CAPACITY {
            if let Some((_, evicted)) = self.order.pop_front() {
                self.ids.remove(&evicted);
            }
        }
        self.order.push_back((now, message_id));
        self.ids.insert(message_id);
        false
    }
}

type BackplaneCloseReceipt = Shared<BoxFuture<'static, Result<(), WebSocketBackplaneError>>>;

#[derive(Clone)]
struct BackplaneCloseOwner {
    receipt: BackplaneCloseReceipt,
    abort: tokio::task::AbortHandle,
}

struct WebSocketDispatcherInner {
    connection_manager: Arc<ConnectionManager>,
    node_id: Uuid,
    slot: BackplaneSlot,
    max_frame_size: usize,
    dedupe: StdMutex<DedupeWindow>,
    health: Option<HealthRegistry>,
    publish_timeout: Duration,
    dispatch_state: AtomicU8,
    in_flight: AtomicUsize,
    drain_notify: tokio::sync::Notify,
    force_dispatch: tokio_util::sync::CancellationToken,
    shutdown_budget: crate::shutdown::ShutdownBudget,
    subscription_state: AtomicU8,
    subscription_notify: tokio::sync::Notify,
    ingress_running: AtomicUsize,
    ingress_notify: tokio::sync::Notify,
    ingress_cancel: tokio_util::sync::CancellationToken,
    close_owner: OnceLock<BackplaneCloseOwner>,
    ingress_joins: crate::tasks::TaskRegistry,
    close_driver_joins: crate::tasks::TaskRegistry,
}

struct DispatchLease {
    inner: Arc<WebSocketDispatcherInner>,
}

impl Drop for DispatchLease {
    fn drop(&mut self) {
        if self.inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.drain_notify.notify_waiters();
        }
    }
}

struct IngressRunGuard {
    inner: Arc<WebSocketDispatcherInner>,
}

impl Drop for IngressRunGuard {
    fn drop(&mut self) {
        if self.inner.ingress_running.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.ingress_notify.notify_waiters();
        }
    }
}

pub(crate) struct WebSocketBackplaneIngressTask {
    task: Option<crate::tasks::TaskReceipt<()>>,
    dispatcher: Option<WebSocketDispatcher>,
}

impl WebSocketBackplaneIngressTask {
    pub(crate) async fn stop(mut self) -> Result<(), Arc<tokio::task::JoinError>> {
        let dispatcher = self.dispatcher.take();
        let result = match self.task.take() {
            None => Ok(()),
            Some(task) => {
                task.abort();
                match task.await {
                    Err(error) if error.is_cancelled() => Ok(()),
                    result => result.map(|_| ()),
                }
            }
        };
        if let Some(dispatcher) = dispatcher {
            dispatcher.set_subscription_state(
                SUBSCRIPTION_STOPPED,
                HealthStatus::Degraded,
                "resources_closed",
            );
        }
        result
    }
}

impl Drop for WebSocketBackplaneIngressTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // The dispatcher retains the actual join. Dropping a stop waiter is
        // an abort request, not evidence that the receive future has stopped.
        self.dispatcher.take();
    }
}

/// App-scoped client dispatcher preserving local delivery while optionally
/// publishing the same command to a distributed backplane.
#[derive(Clone)]
pub struct WebSocketDispatcher(Arc<WebSocketDispatcherInner>);

impl WebSocketDispatcher {
    pub(crate) fn local(connection_manager: Arc<ConnectionManager>) -> Self {
        Self::from_slot(
            connection_manager,
            BackplaneSlot::Disabled,
            Duration::from_secs(5),
            None,
        )
    }

    pub(crate) fn active(
        connection_manager: Arc<ConnectionManager>,
        requirement: BackplaneRequirement,
        backplane: Arc<dyn WebSocketBackplane>,
        publish_timeout: Duration,
        health: HealthRegistry,
    ) -> Self {
        Self::from_slot(
            connection_manager,
            BackplaneSlot::Active {
                requirement,
                backplane,
            },
            publish_timeout,
            Some(health),
        )
    }

    pub(crate) fn configured_unavailable(
        connection_manager: Arc<ConnectionManager>,
        requirement: BackplaneRequirement,
        publish_timeout: Duration,
        health: HealthRegistry,
    ) -> Self {
        Self::from_slot(
            connection_manager,
            BackplaneSlot::ConfiguredUnavailable { requirement },
            publish_timeout,
            Some(health),
        )
    }

    fn from_slot(
        connection_manager: Arc<ConnectionManager>,
        slot: BackplaneSlot,
        publish_timeout: Duration,
        health: Option<HealthRegistry>,
    ) -> Self {
        let max_frame_size = connection_manager
            .max_outbound_message_size()
            .saturating_add(BACKPLANE_FRAME_OVERHEAD_BYTES);
        let node_id = if matches!(slot, BackplaneSlot::Disabled) {
            Uuid::nil()
        } else {
            Uuid::new_v4()
        };
        Self(Arc::new(WebSocketDispatcherInner {
            connection_manager,
            node_id,
            slot,
            max_frame_size,
            dedupe: StdMutex::new(DedupeWindow::new()),
            health,
            publish_timeout,
            dispatch_state: AtomicU8::new(DISPATCHER_ACCEPTING),
            in_flight: AtomicUsize::new(0),
            drain_notify: tokio::sync::Notify::new(),
            force_dispatch: tokio_util::sync::CancellationToken::new(),
            shutdown_budget: Default::default(),
            subscription_state: AtomicU8::new(SUBSCRIPTION_NOT_STARTED),
            subscription_notify: tokio::sync::Notify::new(),
            ingress_running: AtomicUsize::new(0),
            ingress_notify: tokio::sync::Notify::new(),
            ingress_cancel: tokio_util::sync::CancellationToken::new(),
            close_owner: OnceLock::new(),
            ingress_joins: Default::default(),
            close_driver_joins: Default::default(),
        }))
    }

    /// Node-local connection manager used for membership and terminal delivery.
    pub fn connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.0.connection_manager
    }

    /// Requirement selected for this app, or `None` in local-only mode.
    pub fn backplane_requirement(&self) -> Option<BackplaneRequirement> {
        self.0.slot.requirement()
    }

    /// Dispatches a canonical message locally and, when configured, through
    /// the backplane. A backplane failure retains the completed local report
    /// in the returned [`ConnectionError`].
    ///
    /// Local delivery precedes provider publish. Dropping or cancelling this
    /// future can therefore leave local delivery completed while remote
    /// transport acceptance is unknown. A caller must not blindly retry a
    /// cancelled dispatch unless its application message is idempotent.
    /// Interrupted local admission returns [`ConnectionError::DispatchInterrupted`]
    /// without fabricating a complete per-target report.
    ///
    /// During drain, new external calls are rejected. Lily's connection/message
    /// callbacks receive private, bounded continuation authority only for their
    /// invocation. Cleanup authority is separate from execution cancellation.
    /// Neither authority keeps a target alive or guarantees peer delivery.
    pub async fn dispatch(
        &self,
        broadcast: BroadcastMessage,
    ) -> Result<WebSocketDispatchReceipt, ConnectionError> {
        let authority = outbound::current_authority(self);
        let _lease = self.acquire_dispatch(authority.as_ref())?;
        let stopped = self.dispatch_stopped(authority.as_ref());
        tokio::pin!(stopped);
        // Expired invocation/root authority must not begin local side effects.
        tokio::select! {
            biased;
            _ = &mut stopped => return Err(crate::connection::ConnectionOperationError::DispatcherNotAccepting.into()),
            _ = std::future::ready(()) => {}
        }
        let prepared = self.0.connection_manager.prepare_broadcast(broadcast)?;
        // Only explicit selections can prove there are no remote recipients.
        // An empty local namespace/room/principal query must still be published.
        if prepared.has_no_targets() {
            return Ok(WebSocketDispatchReceipt {
                local: BroadcastReport::default(),
                backplane: WebSocketBackplaneDispatchReceipt::NoTargets,
            });
        }
        let frame = match &self.0.slot {
            BackplaneSlot::Active { .. } => {
                let broadcast = prepared.command();
                let target = BackplaneTarget::from(&broadcast.target);
                Some(self.encode(broadcast, &target)?)
            }
            _ => None,
        };
        // Qualification pauses the actual future after validation/encoding and
        // before any local admission. This seam is absent from production builds.
        #[cfg(test)]
        tests::preflight::pause_if_armed().await;
        let local = tokio::select! {
            biased;
            _ = &mut stopped => return Err(ConnectionError::DispatchInterrupted),
            local = self.0.connection_manager.broadcast_prepared(prepared) => local?,
        };
        match &self.0.slot {
            BackplaneSlot::Disabled => Ok(WebSocketDispatchReceipt {
                local,
                backplane: WebSocketBackplaneDispatchReceipt::Disabled,
            }),
            BackplaneSlot::ConfiguredUnavailable { .. } => {
                self.0
                    .connection_manager
                    .metrics()
                    .backplane_publish_unavailable();
                Err(ConnectionError::BackplaneUnavailable { local })
            }
            BackplaneSlot::Active { backplane, .. } => {
                let frame =
                    frame.expect("active backplane frame was preflighted before local delivery");
                // Include synchronous provider invocation in both the deadline
                // and panic boundary; no provider call starts after expiry.
                let publish =
                    AssertUnwindSafe(async { backplane.publish(frame).await }).catch_unwind();
                let result = tokio::select! {
                    biased;
                    reason = &mut stopped => {
                        Err(WebSocketBackplaneError::new(reason))
                    }
                    result = tokio::time::timeout(self.0.publish_timeout, publish) => {
                        match result {
                            Err(_) => Err(WebSocketBackplaneError::new(
                                WebSocketBackplaneErrorKind::TimedOut,
                            )),
                            Ok(Err(_)) => Err(WebSocketBackplaneError::new(
                                WebSocketBackplaneErrorKind::Panicked,
                            )),
                            Ok(Ok(result)) => result,
                        }
                    }
                };
                match result {
                    Ok(receipt) => {
                        self.update_publisher_health(HealthStatus::Healthy, "available");
                        self.0
                            .connection_manager
                            .metrics()
                            .backplane_publish_accepted();
                        Ok(WebSocketDispatchReceipt {
                            local,
                            backplane: WebSocketBackplaneDispatchReceipt::Accepted(receipt),
                        })
                    }
                    Err(source) => {
                        self.record_publish_failure(source.kind());
                        Err(ConnectionError::BackplanePublish { local, source })
                    }
                }
            }
        }
    }

    fn acquire_dispatch(
        &self,
        authority: Option<&outbound::DispatchAuthority>,
    ) -> Result<DispatchLease, ConnectionError> {
        let accepts = || match self.0.dispatch_state.load(Ordering::Acquire) {
            DISPATCHER_ACCEPTING => true,
            DISPATCHER_DRAINING => authority.is_some(),
            _ => false,
        };
        if !accepts() {
            return Err(crate::connection::ConnectionOperationError::DispatcherNotAccepting.into());
        }
        self.0.in_flight.fetch_add(1, Ordering::AcqRel);
        if !accepts() {
            if self.0.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.0.drain_notify.notify_waiters();
            }
            return Err(crate::connection::ConnectionOperationError::DispatcherNotAccepting.into());
        }
        Ok(DispatchLease {
            inner: Arc::clone(&self.0),
        })
    }

    fn encode(
        &self,
        broadcast: &BroadcastMessage,
        target: &BackplaneTarget,
    ) -> Result<WebSocketBackplaneFrame, ConnectionError> {
        let traceparent = lily_trace::W3CTraceContext::from_current_span()
            .map(|context| context.to_traceparent());
        let envelope = OutboundBackplaneEnvelope {
            protocol_version: BACKPLANE_PROTOCOL_VERSION,
            message_id: Uuid::new_v4(),
            origin_node_id: self.0.node_id,
            target,
            exclusions: &broadcast.exclude,
            message: &broadcast.message,
            wire_format: match broadcast.wire_format {
                WsWireFormat::Text => BackplaneWireFormat::Text,
                WsWireFormat::Binary => BackplaneWireFormat::Binary,
            },
            traceparent: traceparent.as_deref(),
        };
        let bytes = serde_json::to_vec(&envelope).map_err(ConnectionError::Serialization)?;
        if bytes.len() > self.0.max_frame_size {
            return Err(
                crate::connection::ConnectionOperationError::OutboundMessageTooLarge.into(),
            );
        }
        Ok(WebSocketBackplaneFrame {
            bytes: bytes.into(),
        })
    }

    fn validate_room(&self, room: &str) -> Result<(), WebSocketBackplaneProtocolError> {
        if !is_canonical_room(room, self.0.connection_manager.max_room_name_length()) {
            Err(WebSocketBackplaneProtocolError::InvalidTarget)
        } else {
            Ok(())
        }
    }

    fn decode(&self, bytes: &[u8]) -> Result<BackplaneEnvelope, WebSocketBackplaneProtocolError> {
        if bytes.is_empty() {
            return Err(WebSocketBackplaneProtocolError::InvalidEnvelope);
        }
        if bytes.len() > self.0.max_frame_size {
            return Err(WebSocketBackplaneProtocolError::FrameTooLarge);
        }
        let envelope: BackplaneEnvelope = serde_json::from_slice(bytes)
            .map_err(|_| WebSocketBackplaneProtocolError::InvalidEnvelope)?;
        if envelope.protocol_version != BACKPLANE_PROTOCOL_VERSION {
            return Err(WebSocketBackplaneProtocolError::UnsupportedVersion {
                actual: envelope.protocol_version,
            });
        }
        if envelope.message_id.is_nil() || envelope.origin_node_id.is_nil() {
            return Err(WebSocketBackplaneProtocolError::InvalidEnvelope);
        }
        envelope
            .message
            .validate_wire()
            .map_err(|_| WebSocketBackplaneProtocolError::InvalidMessage)?;
        let message = match envelope.wire_format {
            BackplaneWireFormat::Text => envelope.message.to_message(),
            BackplaneWireFormat::Binary => envelope.message.to_binary_message(),
        }
        .map_err(|_| WebSocketBackplaneProtocolError::InvalidMessage)?;
        self.0
            .connection_manager
            .validate_outbound_application_frame(&message)
            .map_err(|error| match error {
                ConnectionError::InvalidOperation(
                    crate::connection::ConnectionOperationError::OutboundMessageTooLarge,
                ) => WebSocketBackplaneProtocolError::MessageTooLarge,
                _ => WebSocketBackplaneProtocolError::InvalidMessage,
            })?;
        self.validate_decoded_target(&envelope.target)?;
        if envelope.exclusions.len() > MAX_BACKPLANE_EXCLUSIONS
            || contains_duplicate_ids(&envelope.exclusions)
        {
            return Err(WebSocketBackplaneProtocolError::TooManyExclusions);
        }
        if let Some(traceparent) = envelope.traceparent.as_deref()
            && (traceparent.len() > MAX_TRACEPARENT_BYTES
                || lily_trace::W3CTraceContext::from_traceparent(traceparent).is_err())
        {
            return Err(WebSocketBackplaneProtocolError::InvalidTraceContext);
        }
        Ok(envelope)
    }

    fn validate_decoded_target(
        &self,
        target: &BackplaneTarget,
    ) -> Result<(), WebSocketBackplaneProtocolError> {
        match target {
            BackplaneTarget::Namespace { namespace } => validate_namespace(namespace),
            BackplaneTarget::Room { namespace, room } => {
                validate_namespace(namespace)?;
                self.validate_room(room)
            }
            BackplaneTarget::Rooms { namespace, rooms } => {
                validate_namespace(namespace)?;
                if rooms.is_empty()
                    || rooms.len() > MAX_BACKPLANE_ROOMS
                    || !strictly_sorted_unique(rooms)
                {
                    return Err(WebSocketBackplaneProtocolError::InvalidTarget);
                }
                for room in rooms {
                    self.validate_room(room)?;
                }
                Ok(())
            }
            BackplaneTarget::NamespaceConnections {
                namespace,
                connection_ids,
            } => {
                validate_namespace(namespace)?;
                if connection_ids.is_empty()
                    || connection_ids.len() > MAX_BACKPLANE_CONNECTION_TARGETS
                    || !strictly_sorted_unique(connection_ids)
                {
                    Err(WebSocketBackplaneProtocolError::InvalidTarget)
                } else {
                    Ok(())
                }
            }
            BackplaneTarget::Principal {
                namespace,
                principal_id,
            } => {
                validate_namespace(namespace)?;
                PrincipalId::try_new(principal_id.as_str())
                    .map(|_| ())
                    .map_err(|_| WebSocketBackplaneProtocolError::InvalidTarget)
            }
        }
    }

    async fn ingest(&self, bytes: Vec<u8>) -> Result<(), WebSocketBackplaneIngressError> {
        let envelope = self
            .decode(&bytes)
            .map_err(WebSocketBackplaneIngressError::Protocol)?;
        if envelope.origin_node_id == self.0.node_id {
            self.0
                .connection_manager
                .metrics()
                .backplane_origin_loop_suppressed();
            return Ok(());
        }
        let duplicate = self
            .0
            .dedupe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .seen_or_insert(envelope.message_id, Instant::now());
        if duplicate {
            self.0
                .connection_manager
                .metrics()
                .backplane_duplicate_suppressed();
            return Ok(());
        }

        let span = tracing::info_span!(
            "websocket.backplane.dispatch",
            otel.kind = "consumer",
            lily.backplane.message_id = %envelope.message_id,
            lily.backplane.origin_node_id = %envelope.origin_node_id,
            lily.outcome = tracing::field::Empty,
            lily.error_category = tracing::field::Empty,
        );
        if let Some(parent) = envelope
            .traceparent
            .as_deref()
            .and_then(|value| lily_trace::W3CTraceContext::from_traceparent(value).ok())
        {
            let _ = parent.attach_to_span(&span);
        }

        let target = match envelope.target {
            BackplaneTarget::Namespace { namespace } => BroadcastTarget::Namespace(namespace),
            BackplaneTarget::Room { namespace, room } => BroadcastTarget::Room { namespace, room },
            BackplaneTarget::Rooms { namespace, rooms } => {
                BroadcastTarget::Rooms { namespace, rooms }
            }
            BackplaneTarget::NamespaceConnections {
                namespace,
                connection_ids,
            } => BroadcastTarget::NamespaceConnections {
                namespace,
                connection_ids,
            },
            BackplaneTarget::Principal {
                namespace,
                principal_id,
            } => BroadcastTarget::Principal {
                namespace,
                principal_id,
            },
        };
        let report = self
            .0
            .connection_manager
            .broadcast(BroadcastMessage {
                target,
                message: envelope.message,
                wire_format: match envelope.wire_format {
                    BackplaneWireFormat::Text => WsWireFormat::Text,
                    BackplaneWireFormat::Binary => WsWireFormat::Binary,
                },
                exclude: envelope.exclusions,
            })
            .instrument(span.clone())
            .await
            .map_err(|error| {
                span.record("lily.outcome", "local_dispatch_failed");
                span.record("lily.error_category", local_dispatch_failure_reason(&error));
                WebSocketBackplaneIngressError::LocalDispatch(error)
            })?;
        span.record(
            "lily.outcome",
            if report.is_fully_delivered() {
                "delivered"
            } else {
                "partial"
            },
        );
        Ok(())
    }

    fn update_health(&self, check: &'static str, status: HealthStatus, reason: &'static str) {
        if let Some(health) = &self.0.health
            && let Err(error) = health.update(check, status, reason)
        {
            tracing::error!(%error, "WebSocket backplane health publication failed");
        }
    }

    fn update_publisher_health(&self, status: HealthStatus, reason: &'static str) {
        self.update_health(WS_BACKPLANE_PUBLISHER_HEALTH_CHECK, status, reason);
    }

    fn update_subscriber_health(&self, status: HealthStatus, reason: &'static str) {
        self.update_health(WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK, status, reason);
    }

    fn record_publish_failure(&self, kind: WebSocketBackplaneErrorKind) {
        let metrics = self.0.connection_manager.metrics();
        let status = match kind {
            WebSocketBackplaneErrorKind::Saturated => {
                metrics.backplane_publish_saturated();
                HealthStatus::Degraded
            }
            WebSocketBackplaneErrorKind::Unavailable => {
                metrics.backplane_publish_unavailable();
                HealthStatus::Unhealthy
            }
            WebSocketBackplaneErrorKind::Publish
            | WebSocketBackplaneErrorKind::Receive
            | WebSocketBackplaneErrorKind::Shutdown
            | WebSocketBackplaneErrorKind::Transport
            | WebSocketBackplaneErrorKind::TimedOut
            | WebSocketBackplaneErrorKind::Panicked
            | WebSocketBackplaneErrorKind::Interrupted => {
                metrics.backplane_publish_failed();
                HealthStatus::Unhealthy
            }
        };
        self.update_publisher_health(status, kind.as_str());
    }

    fn set_subscription_state(&self, state: u8, health: HealthStatus, reason: &'static str) {
        self.0.subscription_state.store(state, Ordering::Release);
        self.update_subscriber_health(health, reason);
        self.0.subscription_notify.notify_waiters();
    }

    pub(crate) fn spawn_ingress(&self) -> Option<WebSocketBackplaneIngressTask> {
        let BackplaneSlot::Active { backplane, .. } = &self.0.slot else {
            return None;
        };
        if self.0.ingress_cancel.is_cancelled()
            || self.0.subscription_state.load(Ordering::Acquire) == SUBSCRIPTION_STOPPED
            || self
                .0
                .ingress_running
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return None;
        }
        let ingress_guard = IngressRunGuard {
            inner: Arc::clone(&self.0),
        };
        if self.0.ingress_cancel.is_cancelled() {
            return None;
        }
        self.set_subscription_state(SUBSCRIPTION_STARTING, HealthStatus::Unhealthy, "starting");
        let dispatcher = self.clone();
        let backplane = Arc::clone(backplane);
        let admission = WebSocketBackplaneInboundAdmission {
            maximum_frame_bytes: self.0.max_frame_size,
        };
        let task_dispatcher = dispatcher.clone();
        let ingress_cancel = self.0.ingress_cancel.clone();
        let task = tokio::spawn(async move {
            let _ingress_guard = ingress_guard;
            loop {
                let receive = AssertUnwindSafe(backplane.receive(admission)).catch_unwind();
                let event = tokio::select! {
                    biased;
                    _ = ingress_cancel.cancelled() => {
                        task_dispatcher.set_subscription_state(
                            SUBSCRIPTION_STOPPED,
                            HealthStatus::Degraded,
                            "resources_closed",
                        );
                        break;
                    }
                    result = receive => match result {
                        Ok(event) => event,
                        Err(_) => {
                            task_dispatcher.set_subscription_state(
                                SUBSCRIPTION_STOPPED,
                                HealthStatus::Unhealthy,
                                "receive_panicked",
                            );
                            tracing::error!("WebSocket backplane receive implementation panicked");
                            break;
                        }
                    }
                };
                match event {
                    Ok(WebSocketBackplaneEvent::Frame(bytes)) => {
                        if task_dispatcher.0.subscription_state.load(Ordering::Acquire)
                            != SUBSCRIPTION_READY
                        {
                            task_dispatcher
                                .0
                                .connection_manager
                                .metrics()
                                .backplane_invalid_frame();
                            tracing::warn!(
                                "Rejected WebSocket backplane frame before subscription readiness"
                            );
                            continue;
                        }
                        match task_dispatcher.ingest(bytes).await {
                            Ok(()) => {}
                            Err(WebSocketBackplaneIngressError::Protocol(error)) => {
                                task_dispatcher
                                    .0
                                    .connection_manager
                                    .metrics()
                                    .backplane_invalid_frame();
                                tracing::warn!(
                                    error = %error,
                                    "Rejected invalid WebSocket backplane frame"
                                );
                            }
                            Err(WebSocketBackplaneIngressError::LocalDispatch(error)) => {
                                task_dispatcher
                                    .0
                                    .connection_manager
                                    .metrics()
                                    .backplane_local_dispatch_failed();
                                tracing::warn!(
                                    lily.error_category = local_dispatch_failure_reason(&error),
                                    "WebSocket backplane local dispatch failed"
                                );
                            }
                        }
                    }
                    Ok(WebSocketBackplaneEvent::SubscriptionReady) => {
                        task_dispatcher.set_subscription_state(
                            SUBSCRIPTION_READY,
                            HealthStatus::Healthy,
                            "connected",
                        );
                    }
                    Ok(WebSocketBackplaneEvent::SubscriptionReconnecting) => {
                        task_dispatcher.set_subscription_state(
                            SUBSCRIPTION_RECONNECTING,
                            HealthStatus::Degraded,
                            "reconnecting",
                        );
                    }
                    Ok(WebSocketBackplaneEvent::SubscriptionUnavailable) => {
                        task_dispatcher.set_subscription_state(
                            SUBSCRIPTION_UNAVAILABLE,
                            HealthStatus::Unhealthy,
                            "unavailable",
                        );
                    }
                    Ok(WebSocketBackplaneEvent::PublisherReady) => {
                        task_dispatcher.update_publisher_health(HealthStatus::Healthy, "available");
                    }
                    Ok(WebSocketBackplaneEvent::PublisherReconnecting) => {
                        task_dispatcher
                            .update_publisher_health(HealthStatus::Degraded, "reconnecting");
                    }
                    Ok(WebSocketBackplaneEvent::PublisherUnavailable) => {
                        task_dispatcher
                            .update_publisher_health(HealthStatus::Unhealthy, "unavailable");
                    }
                    Err(error) => {
                        task_dispatcher.set_subscription_state(
                            SUBSCRIPTION_STOPPED,
                            HealthStatus::Unhealthy,
                            error.kind().as_str(),
                        );
                        tracing::error!(
                            lily.error_kind = error.kind().as_str(),
                            "WebSocket backplane receive loop stopped"
                        );
                        break;
                    }
                }
            }
        });
        Some(WebSocketBackplaneIngressTask {
            task: Some(self.0.ingress_joins.track(task)),
            dispatcher: Some(dispatcher),
        })
    }

    pub(crate) async fn wait_for_subscription_ready(&self) -> Result<(), WebSocketBackplaneError> {
        if !self.has_active_backplane() {
            return Ok(());
        }
        loop {
            let notified = self.0.subscription_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.0.subscription_state.load(Ordering::Acquire) {
                SUBSCRIPTION_READY => return Ok(()),
                SUBSCRIPTION_STOPPED => {
                    return Err(WebSocketBackplaneError::new(
                        WebSocketBackplaneErrorKind::Receive,
                    ));
                }
                _ => notified.as_mut().await,
            }
        }
    }

    pub(crate) fn has_active_backplane(&self) -> bool {
        matches!(self.0.slot, BackplaneSlot::Active { .. })
    }

    pub(crate) fn begin_drain(&self) {
        let _ = self.0.dispatch_state.compare_exchange(
            DISPATCHER_ACCEPTING,
            DISPATCHER_DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if self.0.in_flight.load(Ordering::Acquire) == 0 {
            self.0.drain_notify.notify_waiters();
        }
    }

    pub(crate) fn force_drain(&self) {
        self.begin_drain();
        self.0.force_dispatch.cancel();
    }

    pub(crate) async fn wait_drained(&self) {
        loop {
            let notified = self.0.drain_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.as_mut().await;
        }
    }

    async fn wait_ingress_stopped(&self) {
        loop {
            let notified = self.0.ingress_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.ingress_running.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.as_mut().await;
        }
    }

    pub(crate) async fn close_backplane(&self) -> Result<(), WebSocketBackplaneError> {
        let result = self.close_owner().receipt.await;
        // This join also proves the receipt driver itself no longer runs.
        self.0
            .shutdown_budget
            .reconcile(self.0.close_driver_joins.wait())
            .await
            .map_err(|_| WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::TimedOut))?;
        result
    }

    /// Cancels the owned provider-close task at an authoritative outer hard
    /// deadline and waits for its shared terminal receipt. Ordinary graceful
    /// and force waiters must use `close_backplane` so dropping a waiter alone
    /// never cancels the provider operation.
    pub(crate) async fn abort_and_join_backplane_close(
        &self,
    ) -> Result<(), WebSocketBackplaneError> {
        let Some(owner) = self.0.close_owner.get().cloned() else {
            return Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::TimedOut,
            ));
        };
        owner.abort.abort();
        self.0
            .shutdown_budget
            .reconcile(async {
                let result = owner.receipt.await;
                self.0.close_driver_joins.wait().await;
                result
            })
            .await
            .unwrap_or_else(|_| {
                Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::TimedOut,
                ))
            })
    }

    pub(crate) fn stop_ingress(&self) {
        self.0.ingress_cancel.cancel();
        self.0.ingress_joins.abort_all();
    }

    pub(crate) async fn reconcile_ingress(&self) -> bool {
        let _ = self
            .0
            .shutdown_budget
            .reconcile(self.0.ingress_joins.wait())
            .await;
        self.0.ingress_joins.snapshot().outstanding == 0
    }

    pub(crate) fn ingress_panicked(&self) -> bool {
        self.0.ingress_joins.snapshot().panicked > 0
    }

    pub(crate) fn dependency_users_terminal(&self) -> bool {
        self.0.in_flight.load(Ordering::Acquire) == 0
            && self.0.ingress_joins.snapshot().outstanding == 0
    }

    pub(crate) fn close_terminal(&self) -> bool {
        self.0
            .close_owner
            .get()
            .is_some_and(|owner| owner.receipt.clone().now_or_never().is_some())
            && self.0.close_driver_joins.snapshot().outstanding == 0
    }

    pub(crate) fn shutdown_accounting(
        &self,
    ) -> (
        crate::reporting::DependencyState,
        crate::tasks::TaskSnapshot,
        crate::tasks::TaskSnapshot,
        usize,
    ) {
        use crate::reporting::DependencyState as State;
        let state = match self.0.close_owner.get() {
            None => State::NotStarted,
            Some(owner) => match owner.receipt.clone().now_or_never() {
                None => State::Unconfirmed,
                Some(Ok(())) => State::Completed,
                Some(Err(error)) => match error.kind() {
                    WebSocketBackplaneErrorKind::TimedOut => State::TimedOut,
                    WebSocketBackplaneErrorKind::Interrupted => State::Cancelled,
                    WebSocketBackplaneErrorKind::Panicked => State::Panicked,
                    _ => State::Failed,
                },
            },
        };
        (
            state,
            self.0.ingress_joins.snapshot(),
            self.0.close_driver_joins.snapshot(),
            self.0.in_flight.load(Ordering::Acquire),
        )
    }

    fn close_owner(&self) -> BackplaneCloseOwner {
        if let Some(owner) = self.0.close_owner.get() {
            return owner.clone();
        }

        let runtime = tokio::runtime::Handle::current();
        self.0
            .dispatch_state
            .fetch_max(DISPATCHER_CLOSING, Ordering::AcqRel);
        let owner_dispatcher = self.clone();
        let observed_dispatcher = Arc::downgrade(&self.0);
        let process_context = ProcessContext::current();
        let close_span = tracing::Span::current();
        self.0
            .close_owner
            .get_or_init(move || {
                let close = async move {
                    let budget = owner_dispatcher.0.shutdown_budget.clone();
                    budget
                        .cleanup(None, owner_dispatcher.perform_backplane_close())
                        .await
                        .unwrap_or_else(|_| {
                            Err(WebSocketBackplaneError::new(
                                WebSocketBackplaneErrorKind::TimedOut,
                            ))
                        })
                };
                let owner = runtime.spawn(
                    async move {
                        if let Some(context) = process_context {
                            ProcessContext::scope(context, close).await
                        } else {
                            close.await
                        }
                    }
                    .instrument(close_span),
                );
                let abort = owner.abort_handle();
                let receipt = async move {
                    match owner.await {
                        Ok(result) => result,
                        Err(error) => {
                            let kind = if error.is_panic() {
                                WebSocketBackplaneErrorKind::Panicked
                            } else {
                                WebSocketBackplaneErrorKind::Interrupted
                            };
                            mark_close_owner_failure(&observed_dispatcher);
                            Err(WebSocketBackplaneError::new(kind))
                        }
                    }
                }
                .boxed()
                .shared();

                // A retained receipt makes the result replayable; this driver
                // also joins the owner when every external waiter is dropped.
                let driver = receipt.clone();
                self.0.close_driver_joins.track(runtime.spawn(async move {
                    let _ = driver.await;
                }));
                BackplaneCloseOwner { receipt, abort }
            })
            .clone()
    }

    async fn perform_backplane_close(&self) -> Result<(), WebSocketBackplaneError> {
        // A temporary zero dispatch count during drain does not seal future
        // callback continuations. Dependency close explicitly seals admission
        // first, then observes every already-acquired dispatch lease.
        self.0
            .dispatch_state
            .fetch_max(DISPATCHER_CLOSING, Ordering::AcqRel);
        self.wait_drained().await;
        self.0.ingress_cancel.cancel();
        self.wait_ingress_stopped().await;
        self.0.ingress_joins.wait().await;
        let result = if let BackplaneSlot::Active { backplane, .. } = &self.0.slot {
            self.update_publisher_health(HealthStatus::Degraded, "closing");
            self.update_subscriber_health(HealthStatus::Degraded, "closing");
            match AssertUnwindSafe(backplane.close()).catch_unwind().await {
                Err(_) => Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Panicked,
                )),
                Ok(result) => result,
            }
        } else {
            Ok(())
        };
        match result {
            Ok(()) => {
                self.0
                    .dispatch_state
                    .store(DISPATCHER_CLOSED, Ordering::Release);
                if self.0.slot.requirement().is_some() {
                    self.update_publisher_health(HealthStatus::Degraded, "resources_closed");
                    self.update_subscriber_health(HealthStatus::Degraded, "resources_closed");
                }
                Ok(())
            }
            Err(error) => {
                self.update_publisher_health(HealthStatus::Unhealthy, "shutdown_failed");
                self.update_subscriber_health(HealthStatus::Unhealthy, "shutdown_failed");
                Err(error)
            }
        }
    }
}

fn mark_close_owner_failure(dispatcher: &Weak<WebSocketDispatcherInner>) {
    let Some(inner) = dispatcher.upgrade() else {
        return;
    };
    let dispatcher = WebSocketDispatcher(inner);
    dispatcher.update_publisher_health(HealthStatus::Unhealthy, "shutdown_failed");
    dispatcher.update_subscriber_health(HealthStatus::Unhealthy, "shutdown_failed");
}

fn validate_namespace(namespace: &str) -> Result<(), WebSocketBackplaneProtocolError> {
    validate_broadcast_namespace(namespace)
        .map_err(|_| WebSocketBackplaneProtocolError::InvalidTarget)
}

fn contains_duplicate_ids(ids: &[Uuid]) -> bool {
    let mut unique = HashSet::with_capacity(ids.len());
    ids.iter().any(|id| !unique.insert(*id))
}

fn strictly_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod contract_tests;
#[cfg(test)]
mod protocol_tests;
#[cfg(test)]
mod provider_fault_tests;
#[cfg(test)]
mod state_machine_tests;

#[cfg(test)]
mod tests {
    mod outbound_shutdown;
    pub(super) mod preflight;
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{Mutex, mpsc};
    use tokio::time::timeout;

    use lily_monitoring::{HealthCheckKind, HealthCriticality};
    use lily_shutdown::ShutdownState;
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;

    struct RecordingBackplane {
        published: Mutex<Vec<WebSocketBackplaneFrame>>,
        events: Mutex<mpsc::Receiver<WebSocketBackplaneEvent>>,
        closed: AtomicBool,
        received_maximum: AtomicUsize,
    }

    impl RecordingBackplane {
        fn channel() -> (Arc<Self>, mpsc::Sender<WebSocketBackplaneEvent>) {
            let (sender, receiver) = mpsc::channel(8);
            (
                Arc::new(Self {
                    published: Mutex::new(Vec::new()),
                    events: Mutex::new(receiver),
                    closed: AtomicBool::new(false),
                    received_maximum: AtomicUsize::new(0),
                }),
                sender,
            )
        }

        async fn last_frame(&self) -> WebSocketBackplaneFrame {
            self.published
                .lock()
                .await
                .last()
                .expect("published frame")
                .clone()
        }
    }

    #[async_trait]
    impl WebSocketBackplane for RecordingBackplane {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
            let (_, receiver) = mpsc::channel(1);
            Ok(Self {
                published: Mutex::new(Vec::new()),
                events: Mutex::new(receiver),
                closed: AtomicBool::new(false),
                received_maximum: AtomicUsize::new(0),
            })
        }

        async fn publish(
            &self,
            frame: WebSocketBackplaneFrame,
        ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
            self.published.lock().await.push(frame);
            Ok(WebSocketBackplanePublishReceipt::accepted())
        }

        async fn receive(
            &self,
            admission: WebSocketBackplaneInboundAdmission,
        ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
            self.received_maximum
                .store(admission.maximum_frame_bytes(), Ordering::Release);
            self.events
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Receive))
        }

        async fn close(&self) -> Result<(), WebSocketBackplaneError> {
            self.closed.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn test_health(requirement: BackplaneRequirement) -> HealthRegistry {
        let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
        let criticality = match requirement {
            BackplaneRequirement::Required => HealthCriticality::Critical,
            BackplaneRequirement::Optional => HealthCriticality::NonCritical,
        };
        for check in [
            WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
            WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        ] {
            health
                .register(check, HealthCheckKind::Dependency, criticality)
                .unwrap();
            health
                .update(check, HealthStatus::Healthy, "connected")
                .unwrap();
        }
        health
    }

    fn message(target: BroadcastTarget) -> BroadcastMessage {
        BroadcastMessage {
            target,
            message: WsMessageBody::try_new("orders:created", json!({ "id": "42" })).unwrap(),
            wire_format: WsWireFormat::Text,
            exclude: Vec::new(),
        }
    }

    fn exact_size_message(size: usize, wire_format: WsWireFormat) -> BroadcastMessage {
        let mut message = WsMessageBody::try_new("orders:created", json!("")).unwrap();
        message.timestamp = 123;
        let empty_size = match wire_format {
            WsWireFormat::Text => message.to_message(),
            WsWireFormat::Binary => message.to_binary_message(),
        }
        .unwrap()
        .len();
        assert!(size >= empty_size);
        message.data = json!("x".repeat(size - empty_size));
        let encoded_size = match wire_format {
            WsWireFormat::Text => message.to_message(),
            WsWireFormat::Binary => message.to_binary_message(),
        }
        .unwrap()
        .len();
        assert_eq!(encoded_size, size);
        BroadcastMessage {
            target: BroadcastTarget::Namespace("orders".to_owned()),
            message,
            wire_format,
            exclude: Vec::new(),
        }
    }

    fn outbound_limited_manager(limit: usize) -> Arc<ConnectionManager> {
        Arc::new(
            ConnectionManager::with_registered_namespaces_and_identity_and_outbound_limit(
                8,
                128,
                ["orders".to_owned()],
                false,
                limit,
            ),
        )
    }

    fn active_dispatcher(
        manager: Arc<ConnectionManager>,
        backplane: Arc<RecordingBackplane>,
    ) -> WebSocketDispatcher {
        WebSocketDispatcher::active(
            manager,
            BackplaneRequirement::Required,
            backplane,
            Duration::from_secs(1),
            test_health(BackplaneRequirement::Required),
        )
    }

    #[tokio::test]
    async fn local_mode_preserves_the_existing_delivery_contract() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(2);
        manager
            .add_connection(connection_id, sender, None, Some("orders".to_owned()))
            .await
            .unwrap();
        let dispatcher = WebSocketDispatcher::local(manager);

        let receipt = dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await
            .unwrap();

        assert_eq!(receipt.local().sent, 1);
        assert_eq!(
            receipt.backplane(),
            WebSocketBackplaneDispatchReceipt::Disabled
        );
        assert!(receipt.is_fully_accepted());
        assert!(matches!(receiver.recv().await, Some(Message::Text(_))));
    }

    #[tokio::test]
    async fn empty_selections_are_validated_noops_in_every_dispatcher_profile() {
        let manager = outbound_limited_manager(1024);
        let id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .add_connection(id, sender, None, Some("orders".to_owned()))
            .await
            .unwrap();
        let (backplane, _) = RecordingBackplane::channel();
        let profiles = [
            WebSocketDispatcher::local(Arc::clone(&manager)),
            WebSocketDispatcher::configured_unavailable(
                Arc::clone(&manager),
                BackplaneRequirement::Optional,
                Duration::from_secs(1),
                test_health(BackplaneRequirement::Optional),
            ),
            active_dispatcher(Arc::clone(&manager), Arc::clone(&backplane)),
        ];
        for dispatcher in &profiles {
            for wire_format in [WsWireFormat::Text, WsWireFormat::Binary] {
                for target in [
                    BroadcastTarget::NamespaceConnections {
                        namespace: "orders".to_owned(),
                        connection_ids: Vec::new(),
                    },
                    BroadcastTarget::NamespaceConnections {
                        namespace: "orders".to_owned(),
                        connection_ids: Vec::new(),
                    },
                    BroadcastTarget::Rooms {
                        namespace: "orders".to_owned(),
                        rooms: Vec::new(),
                    },
                    BroadcastTarget::NamespaceConnections {
                        namespace: "orders".to_owned(),
                        connection_ids: vec![id, id],
                    },
                    BroadcastTarget::NamespaceConnections {
                        namespace: "orders".to_owned(),
                        connection_ids: vec![id],
                    },
                ] {
                    let mut command = message(target);
                    command.wire_format = wire_format;
                    command.exclude = vec![id, id];
                    let receipt = dispatcher.dispatch(command.clone()).await.unwrap();
                    assert_eq!(*receipt.local(), BroadcastReport::default());
                    assert_eq!(
                        receipt.backplane(),
                        WebSocketBackplaneDispatchReceipt::NoTargets
                    );
                    assert!(receipt.is_fully_accepted());

                    // No recipients does not exempt malformed or oversized inputs.
                    let mut oversized = exact_size_message(1025, wire_format);
                    oversized.target = command.target.clone();
                    oversized.exclude = command.exclude.clone();
                    assert!(matches!(
                        dispatcher.dispatch(oversized).await,
                        Err(ConnectionError::InvalidOperation(
                            crate::ConnectionOperationError::OutboundMessageTooLarge
                        ))
                    ));
                    command.exclude = vec![id; MAX_BACKPLANE_EXCLUSIONS + 1];
                    assert!(matches!(
                        dispatcher.dispatch(command).await,
                        Err(ConnectionError::InvalidOperation(
                            crate::ConnectionOperationError::BackplaneExclusionLimitExceeded
                        ))
                    ));
                }
            }
            for invalid in [
                BroadcastTarget::NamespaceConnections {
                    namespace: "orders/escape".to_owned(),
                    connection_ids: Vec::new(),
                },
                BroadcastTarget::Rooms {
                    namespace: "orders/escape".to_owned(),
                    rooms: Vec::new(),
                },
                BroadcastTarget::Rooms {
                    namespace: "orders".to_owned(),
                    rooms: vec![String::new()],
                },
            ] {
                assert!(matches!(
                    dispatcher.dispatch(message(invalid)).await,
                    Err(ConnectionError::InvalidOperation(
                        crate::ConnectionOperationError::InvalidBackplaneTarget
                    ))
                ));
            }
        }
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(backplane.published.lock().await.is_empty());
        assert_eq!(manager.metrics_snapshot().backplane_publish_unavailable, 0);

        // A zero local result is not proof that remote targets do not exist.
        for target in [
            BroadcastTarget::Namespace("billing".to_owned()),
            BroadcastTarget::Room {
                namespace: "orders".to_owned(),
                room: "remote-room".to_owned(),
            },
            BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: vec![Uuid::new_v4()],
            },
        ] {
            let receipt = profiles[2].dispatch(message(target)).await.unwrap();
            assert_eq!(receipt.local().sent, 0);
            assert!(matches!(
                receipt.backplane(),
                WebSocketBackplaneDispatchReceipt::Accepted(_)
            ));
        }
        assert_eq!(backplane.published.lock().await.len(), 3);
    }

    #[tokio::test]
    async fn dispatcher_profiles_share_the_exact_outbound_message_limit() {
        let mut template = WsMessageBody::try_new("orders:created", json!("")).unwrap();
        template.timestamp = 123;
        let canonical_empty_size = template.to_message().unwrap().len();
        let limit = canonical_empty_size + 8;

        for wire_format in [WsWireFormat::Text, WsWireFormat::Binary] {
            for size in [limit - 1, limit, limit + 1] {
                let local_manager = outbound_limited_manager(limit);
                let local = WebSocketDispatcher::local(Arc::clone(&local_manager));
                let local_result = local.dispatch(exact_size_message(size, wire_format)).await;

                let unavailable_manager = outbound_limited_manager(limit);
                let unavailable = WebSocketDispatcher::configured_unavailable(
                    Arc::clone(&unavailable_manager),
                    BackplaneRequirement::Optional,
                    Duration::from_secs(1),
                    test_health(BackplaneRequirement::Optional),
                );
                let unavailable_result = unavailable
                    .dispatch(exact_size_message(size, wire_format))
                    .await;

                let active_manager = outbound_limited_manager(limit);
                let (backplane, _) = RecordingBackplane::channel();
                let active = active_dispatcher(active_manager, Arc::clone(&backplane));
                let active_result = active.dispatch(exact_size_message(size, wire_format)).await;

                if size <= limit {
                    let local_receipt = local_result.unwrap();
                    assert_eq!(local_receipt.local(), &BroadcastReport::default());
                    assert_eq!(
                        local_receipt.backplane(),
                        WebSocketBackplaneDispatchReceipt::Disabled
                    );
                    assert!(matches!(
                        unavailable_result,
                        Err(ConnectionError::BackplaneUnavailable { local })
                            if local == BroadcastReport::default()
                    ));
                    assert!(active_result.is_ok());
                    assert_eq!(backplane.published.lock().await.len(), 1);
                    assert_eq!(
                        unavailable_manager
                            .metrics_snapshot()
                            .backplane_publish_unavailable,
                        1
                    );
                } else {
                    for error in [
                        local_result.unwrap_err(),
                        unavailable_result.unwrap_err(),
                        active_result.unwrap_err(),
                    ] {
                        assert!(matches!(
                            error,
                            ConnectionError::InvalidOperation(
                                crate::connection::ConnectionOperationError::OutboundMessageTooLarge
                            )
                        ));
                    }
                    assert!(backplane.published.lock().await.is_empty());
                    assert_eq!(
                        unavailable_manager
                            .metrics_snapshot()
                            .backplane_publish_unavailable,
                        0
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn backplane_ingress_enforces_outbound_limit_before_local_delivery() {
        let mut template = WsMessageBody::try_new("orders:created", json!("")).unwrap();
        template.timestamp = 123;
        let limit = template.to_message().unwrap().len() + 8;

        for (wire_format, backplane_wire_format) in [
            (WsWireFormat::Text, BackplaneWireFormat::Text),
            (WsWireFormat::Binary, BackplaneWireFormat::Binary),
        ] {
            let manager = outbound_limited_manager(limit);
            let connection_id = Uuid::new_v4();
            let (sender, mut receiver) = mpsc::channel(2);
            manager
                .add_connection(connection_id, sender, None, Some("orders".to_owned()))
                .await
                .unwrap();
            let (backplane, _) = RecordingBackplane::channel();
            let dispatcher = active_dispatcher(manager, backplane);

            for size in [limit - 1, limit, limit + 1] {
                let fixture = exact_size_message(size, wire_format);
                let encoded = serde_json::to_vec(&BackplaneEnvelope {
                    protocol_version: BACKPLANE_PROTOCOL_VERSION,
                    message_id: Uuid::new_v4(),
                    origin_node_id: Uuid::new_v4(),
                    target: BackplaneTarget::Namespace {
                        namespace: "orders".to_owned(),
                    },
                    exclusions: Vec::new(),
                    message: fixture.message,
                    wire_format: backplane_wire_format,
                    traceparent: None,
                })
                .unwrap();
                assert!(
                    encoded.len() < dispatcher.0.max_frame_size,
                    "outer provider-frame limit must not mask the embedded message boundary"
                );

                if size <= limit {
                    dispatcher.ingest(encoded).await.unwrap();
                    let delivered = timeout(Duration::from_secs(1), receiver.recv())
                        .await
                        .expect("accepted ingress must reach local delivery within the deadline")
                        .expect("local application data channel remains open");
                    assert_eq!(delivered.len(), size);
                    assert_eq!(
                        matches!(delivered, Message::Binary(_)),
                        wire_format == WsWireFormat::Binary
                    );
                } else {
                    assert!(matches!(
                        dispatcher.ingest(encoded).await,
                        Err(WebSocketBackplaneIngressError::Protocol(
                            WebSocketBackplaneProtocolError::MessageTooLarge
                        ))
                    ));
                    assert!(matches!(
                        receiver.try_recv(),
                        Err(mpsc::error::TryRecvError::Empty)
                    ));
                }
            }
        }
    }

    #[test]
    fn opaque_frame_debug_exposes_only_structure() {
        let secret = b"secret-backplane-payload".to_vec();
        let event = WebSocketBackplaneEvent::Frame(secret.clone());
        let debug = format!("{event:?}");

        assert!(debug.contains(&format!("len: {}", secret.len())));
        assert!(!debug.contains("secret-backplane-payload"));
    }

    #[tokio::test]
    async fn remote_only_connection_is_transport_accepted_without_fake_delivery_count() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let (backplane, _) = RecordingBackplane::channel();
        let dispatcher = active_dispatcher(manager, Arc::clone(&backplane));

        let receipt = dispatcher
            .dispatch(message(BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: vec![Uuid::new_v4()],
            }))
            .await
            .unwrap();

        assert_eq!(receipt.local().missing, 1);
        assert!(matches!(
            receipt.backplane(),
            WebSocketBackplaneDispatchReceipt::Accepted(_)
        ));
        assert!(receipt.is_fully_accepted());
        assert_eq!(backplane.published.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn origin_loop_and_duplicate_frames_are_suppressed() {
        let source_manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let source_id = Uuid::new_v4();
        let (source_tx, mut source_rx) = mpsc::channel(4);
        source_manager
            .add_connection(source_id, source_tx, None, Some("orders".to_owned()))
            .await
            .unwrap();
        let (source_backplane, _) = RecordingBackplane::channel();
        let source = active_dispatcher(source_manager, Arc::clone(&source_backplane));

        source
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await
            .unwrap();
        assert!(matches!(source_rx.recv().await, Some(Message::Text(_))));
        let frame = source_backplane.last_frame().await;
        source.ingest(frame.as_bytes().to_vec()).await.unwrap();
        assert!(
            timeout(Duration::from_millis(25), source_rx.recv())
                .await
                .is_err()
        );

        let destination_manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let destination_id = Uuid::new_v4();
        let (destination_tx, mut destination_rx) = mpsc::channel(4);
        destination_manager
            .add_connection(
                destination_id,
                destination_tx,
                None,
                Some("orders".to_owned()),
            )
            .await
            .unwrap();
        let (destination_backplane, _) = RecordingBackplane::channel();
        let destination = active_dispatcher(destination_manager, destination_backplane);

        destination.ingest(frame.as_bytes().to_vec()).await.unwrap();
        destination.ingest(frame.as_bytes().to_vec()).await.unwrap();
        assert!(matches!(
            destination_rx.recv().await,
            Some(Message::Text(_))
        ));
        assert!(
            timeout(Duration::from_millis(25), destination_rx.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn target_cardinality_is_rejected_before_local_or_remote_delivery() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("orders".to_owned()))
            .await
            .unwrap();
        let (backplane, _) = RecordingBackplane::channel();
        let dispatcher = active_dispatcher(manager, Arc::clone(&backplane));
        let mut targets = (0..MAX_BACKPLANE_CONNECTION_TARGETS)
            .map(|_| Uuid::new_v4())
            .collect::<Vec<_>>();
        targets.push(connection_id);

        let error = dispatcher
            .dispatch(message(BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: targets,
            }))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ConnectionError::InvalidOperation(
                crate::connection::ConnectionOperationError::BackplaneTargetLimitExceeded
            )
        ));
        assert!(receiver.try_recv().is_err());
        assert!(backplane.published.lock().await.is_empty());
    }

    #[tokio::test]
    async fn noncanonical_room_is_rejected_before_local_or_remote_delivery() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("orders".to_owned()))
            .await
            .unwrap();
        manager
            .join_room("orders", connection_id, "priority")
            .await
            .unwrap();
        let (backplane, _) = RecordingBackplane::channel();
        let dispatcher = active_dispatcher(manager, Arc::clone(&backplane));

        for invalid in ["priority/escape", "priority\nnext", "öncelik"] {
            let error = dispatcher
                .dispatch(message(BroadcastTarget::Rooms {
                    namespace: "orders".to_owned(),
                    rooms: vec!["priority".to_owned(), invalid.to_owned()],
                }))
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                ConnectionError::InvalidOperation(
                    crate::connection::ConnectionOperationError::InvalidBackplaneTarget
                )
            ));
        }

        assert!(receiver.try_recv().is_err());
        assert!(backplane.published.lock().await.is_empty());
    }

    #[tokio::test]
    async fn forged_noncanonical_room_is_rejected_before_local_delivery() {
        let source_manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let (source_backplane, _) = RecordingBackplane::channel();
        let source = active_dispatcher(source_manager, Arc::clone(&source_backplane));
        source
            .dispatch(message(BroadcastTarget::Rooms {
                namespace: "orders".to_owned(),
                rooms: vec!["priority".to_owned()],
            }))
            .await
            .unwrap();
        let frame = source_backplane.last_frame().await;
        let mut value: serde_json::Value = serde_json::from_slice(frame.as_bytes()).unwrap();
        value["target"]["rooms"] = json!(["priority", "priority/escape"]);

        let destination_manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        destination_manager
            .add_connection(connection_id, sender, None, Some("orders".to_owned()))
            .await
            .unwrap();
        destination_manager
            .join_room("orders", connection_id, "priority")
            .await
            .unwrap();
        let (destination_backplane, _) = RecordingBackplane::channel();
        let destination = active_dispatcher(destination_manager, destination_backplane);

        let error = destination
            .ingest(serde_json::to_vec(&value).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WebSocketBackplaneIngressError::Protocol(
                WebSocketBackplaneProtocolError::InvalidTarget
            )
        ));
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn invalid_version_is_rejected_before_local_dispatch() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let (backplane, _) = RecordingBackplane::channel();
        let dispatcher = active_dispatcher(manager, Arc::clone(&backplane));
        dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await
            .unwrap();
        let frame = backplane.last_frame().await;
        let mut value: serde_json::Value = serde_json::from_slice(frame.as_bytes()).unwrap();
        for unsupported in [1, 2, BACKPLANE_PROTOCOL_VERSION + 1] {
            value["protocol_version"] = json!(unsupported);
            let error = dispatcher
                .ingest(serde_json::to_vec(&value).unwrap())
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                WebSocketBackplaneIngressError::Protocol(WebSocketBackplaneProtocolError::UnsupportedVersion {
                    actual
                }) if actual == unsupported
            ));
        }
    }

    #[tokio::test]
    async fn lifecycle_waiters_replay_and_observe_terminal_publications() {
        let manager = Arc::new(ConnectionManager::new());
        let (backplane, _) = RecordingBackplane::channel();
        let ready_before = active_dispatcher(Arc::clone(&manager), Arc::clone(&backplane));
        ready_before.set_subscription_state(SUBSCRIPTION_READY, HealthStatus::Healthy, "connected");
        timeout(
            Duration::from_secs(1),
            ready_before.wait_for_subscription_ready(),
        )
        .await
        .expect("subscription readiness published before wait must be replayed")
        .expect("ready subscription must succeed");

        let ready_after = active_dispatcher(Arc::clone(&manager), Arc::clone(&backplane));
        let ready_waiter = ready_after.clone();
        let ready = tokio::spawn(async move { ready_waiter.wait_for_subscription_ready().await });
        tokio::task::yield_now().await;
        ready_after.set_subscription_state(SUBSCRIPTION_READY, HealthStatus::Healthy, "connected");
        timeout(Duration::from_secs(1), ready)
            .await
            .expect("registered subscription waiter must be notified")
            .expect("subscription waiter task must not panic")
            .expect("ready subscription must succeed");

        let drained_before = active_dispatcher(Arc::clone(&manager), Arc::clone(&backplane));
        timeout(Duration::from_secs(1), drained_before.wait_drained())
            .await
            .expect("zero in-flight work must be replayed as drained");

        let drained_after = active_dispatcher(Arc::clone(&manager), Arc::clone(&backplane));
        drained_after.0.in_flight.store(1, Ordering::Release);
        let lease = DispatchLease {
            inner: Arc::clone(&drained_after.0),
        };
        let drain_waiter = drained_after.clone();
        let drained = tokio::spawn(async move { drain_waiter.wait_drained().await });
        tokio::task::yield_now().await;
        drop(lease);
        timeout(Duration::from_secs(1), drained)
            .await
            .expect("registered drain waiter must be notified")
            .expect("drain waiter task must not panic");

        let ingress_before = active_dispatcher(Arc::clone(&manager), Arc::clone(&backplane));
        timeout(
            Duration::from_secs(1),
            ingress_before.wait_ingress_stopped(),
        )
        .await
        .expect("stopped ingress published before wait must be replayed");

        let ingress_after = active_dispatcher(manager, backplane);
        ingress_after.0.ingress_running.store(1, Ordering::Release);
        let ingress_guard = IngressRunGuard {
            inner: Arc::clone(&ingress_after.0),
        };
        let ingress_waiter = ingress_after.clone();
        let ingress = tokio::spawn(async move { ingress_waiter.wait_ingress_stopped().await });
        tokio::task::yield_now().await;
        drop(ingress_guard);
        timeout(Duration::from_secs(1), ingress)
            .await
            .expect("registered ingress waiter must be notified")
            .expect("ingress waiter task must not panic");
    }

    #[tokio::test]
    async fn runtime_availability_events_drive_required_readiness() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let (backplane, events) = RecordingBackplane::channel();
        let health = test_health(BackplaneRequirement::Required);
        let dispatcher = WebSocketDispatcher::active(
            manager,
            BackplaneRequirement::Required,
            backplane,
            Duration::from_secs(1),
            health.clone(),
        );
        let ingress = dispatcher.spawn_ingress().unwrap();

        events
            .send(WebSocketBackplaneEvent::SubscriptionUnavailable)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = health.snapshot().unwrap();
                let check = snapshot
                    .checks
                    .iter()
                    .find(|check| check.name == WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK)
                    .unwrap();
                if check.status == HealthStatus::Unhealthy {
                    assert!(!snapshot.ready);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        events
            .send(WebSocketBackplaneEvent::SubscriptionReady)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if health.snapshot().unwrap().ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        events
            .send(WebSocketBackplaneEvent::PublisherReconnecting)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = health.snapshot().unwrap();
                let check = snapshot
                    .checks
                    .iter()
                    .find(|check| check.name == WS_BACKPLANE_PUBLISHER_HEALTH_CHECK)
                    .unwrap();
                if check.status == HealthStatus::Degraded {
                    assert!(!snapshot.ready);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        events
            .send(WebSocketBackplaneEvent::PublisherUnavailable)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = health.snapshot().unwrap();
                let check = snapshot
                    .checks
                    .iter()
                    .find(|check| check.name == WS_BACKPLANE_PUBLISHER_HEALTH_CHECK)
                    .unwrap();
                if check.status == HealthStatus::Unhealthy {
                    assert!(!snapshot.ready);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        events
            .send(WebSocketBackplaneEvent::PublisherReady)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if health.snapshot().unwrap().ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        ingress.stop().await.unwrap();
        dispatcher.close_backplane().await.unwrap();
    }

    static BUILDER_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static BUILDER_CLOSES: AtomicUsize = AtomicUsize::new(0);

    struct BuilderBackplane;

    #[async_trait]
    impl WebSocketBackplane for BuilderBackplane {
        async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
            extensions
                .get_service::<lily_config::ConfigService>(None)
                .await
                .map_err(WebSocketBackplaneInitError::dependency)?;
            BUILDER_INITIALIZATIONS.fetch_add(1, Ordering::AcqRel);
            Ok(Self)
        }

        async fn publish(
            &self,
            _frame: WebSocketBackplaneFrame,
        ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
            Ok(WebSocketBackplanePublishReceipt::accepted())
        }

        async fn receive(
            &self,
            _admission: WebSocketBackplaneInboundAdmission,
        ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
            std::future::pending().await
        }

        async fn close(&self) -> Result<(), WebSocketBackplaneError> {
            BUILDER_CLOSES.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    struct FailingBackplane;

    #[async_trait]
    impl WebSocketBackplane for FailingBackplane {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
            Err(WebSocketBackplaneInitError::new(
                WebSocketBackplaneInitErrorKind::Transport,
            ))
        }

        async fn publish(
            &self,
            _frame: WebSocketBackplaneFrame,
        ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
            unreachable!()
        }

        async fn receive(
            &self,
            _admission: WebSocketBackplaneInboundAdmission,
        ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
            unreachable!()
        }
    }

    struct SaturatedBackplane;

    #[async_trait]
    impl WebSocketBackplane for SaturatedBackplane {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
            Ok(Self)
        }

        async fn publish(
            &self,
            _frame: WebSocketBackplaneFrame,
        ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
            Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Saturated,
            ))
        }

        async fn receive(
            &self,
            _admission: WebSocketBackplaneInboundAdmission,
        ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
            std::future::pending().await
        }
    }

    struct PanickingBackplane;

    #[async_trait]
    impl WebSocketBackplane for PanickingBackplane {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
            panic!("intentional constructor panic")
        }

        async fn publish(
            &self,
            _frame: WebSocketBackplaneFrame,
        ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
            unreachable!()
        }

        async fn receive(
            &self,
            _admission: WebSocketBackplaneInboundAdmission,
        ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
            unreachable!()
        }
    }

    struct GatedBackplane {
        publish_started: tokio::sync::Notify,
        publish_release: tokio::sync::Notify,
        close_started: tokio::sync::Notify,
        close_release: tokio::sync::Notify,
        close_calls: AtomicUsize,
        close_completed: AtomicBool,
        close_future_dropped: AtomicBool,
    }

    struct CloseFutureDropProbe<'a>(&'a AtomicBool);

    impl Drop for CloseFutureDropProbe<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl GatedBackplane {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                publish_started: tokio::sync::Notify::new(),
                publish_release: tokio::sync::Notify::new(),
                close_started: tokio::sync::Notify::new(),
                close_release: tokio::sync::Notify::new(),
                close_calls: AtomicUsize::new(0),
                close_completed: AtomicBool::new(false),
                close_future_dropped: AtomicBool::new(false),
            })
        }
    }

    #[async_trait]
    impl WebSocketBackplane for GatedBackplane {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
            unreachable!()
        }

        async fn publish(
            &self,
            _frame: WebSocketBackplaneFrame,
        ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
            self.publish_started.notify_one();
            self.publish_release.notified().await;
            Ok(WebSocketBackplanePublishReceipt::accepted())
        }

        async fn receive(
            &self,
            _admission: WebSocketBackplaneInboundAdmission,
        ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
            std::future::pending().await
        }

        async fn close(&self) -> Result<(), WebSocketBackplaneError> {
            let _drop_probe = CloseFutureDropProbe(&self.close_future_dropped);
            self.close_calls.fetch_add(1, Ordering::AcqRel);
            self.close_started.notify_one();
            self.close_release.notified().await;
            self.close_completed.store(true, Ordering::Release);
            Ok(())
        }
    }

    struct IngressCloseOrderBackplane {
        receive_started: tokio::sync::Notify,
        receive_dropped: AtomicBool,
        close_raced_receive: AtomicBool,
    }

    impl IngressCloseOrderBackplane {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                receive_started: tokio::sync::Notify::new(),
                receive_dropped: AtomicBool::new(false),
                close_raced_receive: AtomicBool::new(false),
            })
        }
    }

    #[async_trait]
    impl WebSocketBackplane for IngressCloseOrderBackplane {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
            unreachable!()
        }

        async fn publish(
            &self,
            _frame: WebSocketBackplaneFrame,
        ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
            Ok(WebSocketBackplanePublishReceipt::accepted())
        }

        async fn receive(
            &self,
            _admission: WebSocketBackplaneInboundAdmission,
        ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
            struct DropEvidence<'a>(&'a AtomicBool);

            impl Drop for DropEvidence<'_> {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Release);
                }
            }

            let _evidence = DropEvidence(&self.receive_dropped);
            self.receive_started.notify_one();
            std::future::pending().await
        }

        async fn close(&self) -> Result<(), WebSocketBackplaneError> {
            if !self.receive_dropped.load(Ordering::Acquire) {
                self.close_raced_receive.store(true, Ordering::Release);
            }
            Ok(())
        }
    }

    static PENDING_CONSTRUCTOR_DROPPED: AtomicBool = AtomicBool::new(false);

    struct PendingConstructorBackplane;

    #[async_trait]
    impl WebSocketBackplane for PendingConstructorBackplane {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
            struct DropEvidence;
            impl Drop for DropEvidence {
                fn drop(&mut self) {
                    PENDING_CONSTRUCTOR_DROPPED.store(true, Ordering::Release);
                }
            }
            let _evidence = DropEvidence;
            std::future::pending().await
        }

        async fn publish(
            &self,
            _frame: WebSocketBackplaneFrame,
        ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
            unreachable!()
        }

        async fn receive(
            &self,
            _admission: WebSocketBackplaneInboundAdmission,
        ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn builder_materializes_only_the_selected_type_once_and_closes_it() {
        let initializations_before = BUILDER_INITIALIZATIONS.load(Ordering::Acquire);
        let closes_before = BUILDER_CLOSES.load(Ordering::Acquire);
        let app = crate::WsAppBuilder::new("127.0.0.1:0")
            .backplane::<BuilderBackplane>(BackplaneRequirement::Required)
            .build()
            .await
            .unwrap();

        assert_eq!(
            BUILDER_INITIALIZATIONS.load(Ordering::Acquire),
            initializations_before + 1
        );
        app.close().await.unwrap();
        assert_eq!(BUILDER_CLOSES.load(Ordering::Acquire), closes_before + 1);
    }

    #[tokio::test]
    async fn required_failure_rejects_build_while_optional_failure_is_degraded() {
        let required = crate::WsAppBuilder::new("127.0.0.1:0")
            .backplane::<FailingBackplane>(BackplaneRequirement::Required)
            .build()
            .await;
        assert!(required.is_err());

        let optional = crate::WsAppBuilder::new("127.0.0.1:0")
            .backplane::<FailingBackplane>(BackplaneRequirement::Optional)
            .build()
            .await
            .unwrap();
        let snapshot = optional.health_snapshot().unwrap();
        let backplane = snapshot
            .checks
            .iter()
            .find(|check| check.name == WS_BACKPLANE_PUBLISHER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(backplane.criticality, HealthCriticality::NonCritical);
        assert_eq!(backplane.status, HealthStatus::Degraded);
        assert_eq!(backplane.reason_code, "initialization_failed");
        optional.close().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_builder_registration_is_rejected_before_initialization() {
        let result = crate::WsAppBuilder::new("127.0.0.1:0")
            .backplane::<BuilderBackplane>(BackplaneRequirement::Required)
            .backplane::<FailingBackplane>(BackplaneRequirement::Optional)
            .build()
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn bounded_publish_saturation_is_typed_and_degrades_health() {
        let health = test_health(BackplaneRequirement::Required);
        let dispatcher = WebSocketDispatcher::active(
            Arc::new(ConnectionManager::with_registered_namespaces(
                8,
                128,
                ["orders".to_owned()],
            )),
            BackplaneRequirement::Required,
            Arc::new(SaturatedBackplane),
            Duration::from_secs(1),
            health.clone(),
        );

        let error = dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await
            .unwrap_err();
        let ConnectionError::BackplanePublish { local, source } = error else {
            panic!("expected a typed backplane publish failure");
        };
        assert_eq!(local.missing, 0);
        assert_eq!(source.kind(), WebSocketBackplaneErrorKind::Saturated);

        let snapshot = health.snapshot().unwrap();
        let check = snapshot
            .checks
            .iter()
            .find(|check| check.name == WS_BACKPLANE_PUBLISHER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(check.status, HealthStatus::Degraded);
        assert_eq!(check.reason_code, "saturated");
    }

    #[tokio::test]
    async fn constructor_panic_is_a_typed_build_error() {
        let result = crate::WsAppBuilder::new("127.0.0.1:0")
            .backplane::<PanickingBackplane>(BackplaneRequirement::Required)
            .build()
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn constructor_timeout_drops_provider_future_and_is_typed() {
        use lily_injection::ApplicationContainer;

        PENDING_CONSTRUCTOR_DROPPED.store(false, Ordering::Release);
        let container = ApplicationContainer::build().await.unwrap();
        let result = WebSocketBackplaneRegistration::of::<PendingConstructorBackplane>(
            BackplaneRequirement::Required,
        )
        .materialize(container.services(), Duration::from_millis(20))
        .await;
        let error = match result {
            Ok(_) => panic!("pending constructor must time out"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), WebSocketBackplaneInitErrorKind::TimedOut);
        assert!(PENDING_CONSTRUCTOR_DROPPED.load(Ordering::Acquire));
        container.close().await.unwrap();
    }

    #[test]
    fn provider_diagnostics_are_not_exposed_by_public_errors() {
        let secret = "redis://user:password@private-host/secret-payload";
        let runtime =
            WebSocketBackplaneError::from_source(WebSocketBackplaneErrorKind::Transport, secret);
        let initialization = WebSocketBackplaneInitError::dependency(secret);

        for rendered in [
            format!("{runtime}"),
            format!("{runtime:?}"),
            format!("{initialization}"),
            format!("{initialization:?}"),
        ] {
            assert!(!rendered.contains("password"));
            assert!(!rendered.contains("private-host"));
            assert!(!rendered.contains("secret-payload"));
        }
    }

    #[tokio::test]
    async fn ingress_receives_effective_allocation_admission() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let (backplane, _events) = RecordingBackplane::channel();
        let dispatcher = active_dispatcher(manager, Arc::clone(&backplane));
        let ingress = dispatcher.spawn_ingress().unwrap();

        timeout(Duration::from_secs(1), async {
            while backplane.received_maximum.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            backplane.received_maximum.load(Ordering::Acquire),
            1024 * 1024 + BACKPLANE_FRAME_OVERHEAD_BYTES
        );
        ingress.stop().await.unwrap();
    }

    #[tokio::test]
    async fn close_waits_for_an_owner_dropped_ingress_receive_future() {
        let backplane = IngressCloseOrderBackplane::new();
        let dispatcher = WebSocketDispatcher::active(
            Arc::new(ConnectionManager::new()),
            BackplaneRequirement::Required,
            Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
            Duration::from_secs(1),
            test_health(BackplaneRequirement::Required),
        );
        let ingress = dispatcher.spawn_ingress().unwrap();
        timeout(Duration::from_secs(1), backplane.receive_started.notified())
            .await
            .unwrap();
        drop(ingress);

        timeout(Duration::from_secs(1), dispatcher.close_backplane())
            .await
            .unwrap()
            .unwrap();

        assert!(backplane.receive_dropped.load(Ordering::Acquire));
        assert!(!backplane.close_raced_receive.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn drain_rejects_new_dispatch_before_local_delivery_and_waits_for_inflight() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(4);
        manager
            .add_connection(connection_id, sender, None, Some("orders".to_owned()))
            .await
            .unwrap();
        let backplane = GatedBackplane::new();
        let dispatcher = WebSocketDispatcher::active(
            manager,
            BackplaneRequirement::Required,
            Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
            Duration::from_secs(1),
            test_health(BackplaneRequirement::Required),
        );
        let first_dispatcher = dispatcher.clone();
        let first = tokio::spawn(async move {
            first_dispatcher
                .dispatch(message(BroadcastTarget::NamespaceConnections {
                    namespace: "orders".to_owned(),
                    connection_ids: vec![connection_id],
                }))
                .await
        });
        backplane.publish_started.notified().await;
        assert!(matches!(receiver.recv().await, Some(Message::Text(_))));

        dispatcher.begin_drain();
        let second = dispatcher
            .dispatch(message(BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: vec![connection_id],
            }))
            .await
            .unwrap_err();
        assert!(matches!(
            second,
            ConnectionError::InvalidOperation(
                crate::connection::ConnectionOperationError::DispatcherNotAccepting
            )
        ));
        assert!(receiver.try_recv().is_err());
        assert!(
            timeout(Duration::from_millis(20), dispatcher.wait_drained())
                .await
                .is_err()
        );

        backplane.publish_release.notify_one();
        first.await.unwrap().unwrap();
        dispatcher.wait_drained().await;
    }

    #[tokio::test]
    async fn publish_timeout_is_typed_and_releases_dispatch_lease() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let backplane = GatedBackplane::new();
        let dispatcher = WebSocketDispatcher::active(
            manager,
            BackplaneRequirement::Required,
            Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
            Duration::from_millis(20),
            test_health(BackplaneRequirement::Required),
        );

        let error = dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await
            .unwrap_err();
        let ConnectionError::BackplanePublish { source, .. } = error else {
            panic!("expected a typed publish error");
        };
        assert_eq!(source.kind(), WebSocketBackplaneErrorKind::TimedOut);
        dispatcher.begin_drain();
        timeout(Duration::from_millis(20), dispatcher.wait_drained())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn aborted_waiter_does_not_cancel_owned_close_and_success_is_replayed() {
        let backplane = GatedBackplane::new();
        let dispatcher = WebSocketDispatcher::active(
            Arc::new(ConnectionManager::new()),
            BackplaneRequirement::Required,
            Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
            Duration::from_secs(1),
            test_health(BackplaneRequirement::Required),
        );
        let closing_dispatcher = dispatcher.clone();
        let closing = tokio::spawn(async move { closing_dispatcher.close_backplane().await });
        timeout(Duration::from_secs(1), backplane.close_started.notified())
            .await
            .unwrap();
        closing.abort();
        assert!(
            timeout(Duration::from_secs(1), closing)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(!backplane.close_future_dropped.load(Ordering::Acquire));

        // The owner progresses without another caller polling the receipt.
        backplane.close_release.notify_one();
        timeout(Duration::from_secs(1), async {
            while !backplane.close_future_dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(backplane.close_completed.load(Ordering::Acquire));
        assert!(backplane.close_future_dropped.load(Ordering::Acquire));

        timeout(Duration::from_secs(1), dispatcher.close_backplane())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(backplane.close_calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn authoritative_hard_timeout_abort_joins_and_replays_interrupted() {
        let backplane = GatedBackplane::new();
        let health = test_health(BackplaneRequirement::Required);
        let dispatcher = WebSocketDispatcher::active(
            Arc::new(ConnectionManager::new()),
            BackplaneRequirement::Required,
            Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
            Duration::from_secs(1),
            health.clone(),
        );
        let waiting_dispatcher = dispatcher.clone();
        let waiter = tokio::spawn(async move { waiting_dispatcher.close_backplane().await });
        timeout(Duration::from_secs(1), backplane.close_started.notified())
            .await
            .unwrap();

        let reconciled = timeout(
            Duration::from_secs(1),
            dispatcher.abort_and_join_backplane_close(),
        )
        .await
        .expect("hard-timeout owner reconciliation did not join")
        .unwrap_err();
        assert_eq!(reconciled.kind(), WebSocketBackplaneErrorKind::Interrupted);
        assert!(backplane.close_future_dropped.load(Ordering::Acquire));

        let waiter_error = timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            waiter_error.kind(),
            WebSocketBackplaneErrorKind::Interrupted
        );
        assert_eq!(
            dispatcher.close_backplane().await.unwrap_err().kind(),
            WebSocketBackplaneErrorKind::Interrupted
        );
        assert_eq!(backplane.close_calls.load(Ordering::Acquire), 1);
        let snapshot = health.snapshot().unwrap();
        for name in [
            WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
            WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        ] {
            let check = snapshot
                .checks
                .iter()
                .find(|check| check.name == name)
                .expect("registered backplane health check");
            assert_eq!(check.status, HealthStatus::Unhealthy);
            assert_eq!(check.reason_code, "shutdown_failed");
        }
    }

    #[tokio::test]
    async fn concurrent_close_calls_provider_once_and_replay_success() {
        let backplane = GatedBackplane::new();
        let dispatcher = WebSocketDispatcher::active(
            Arc::new(ConnectionManager::new()),
            BackplaneRequirement::Required,
            Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
            Duration::from_secs(1),
            test_health(BackplaneRequirement::Required),
        );
        let first_dispatcher = dispatcher.clone();
        let second_dispatcher = dispatcher.clone();
        let first = tokio::spawn(async move { first_dispatcher.close_backplane().await });
        timeout(Duration::from_secs(1), backplane.close_started.notified())
            .await
            .unwrap();
        let second = tokio::spawn(async move { second_dispatcher.close_backplane().await });
        backplane.close_release.notify_one();

        timeout(Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(1), dispatcher.close_backplane())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(backplane.close_calls.load(Ordering::Acquire), 1);
        let rejected = dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await
            .unwrap_err();
        assert!(matches!(
            rejected,
            ConnectionError::InvalidOperation(
                crate::connection::ConnectionOperationError::DispatcherNotAccepting
            )
        ));
    }
}
