use futures_util::{StreamExt, stream::FuturesUnordered};
use lily_web_core::{Principal, RequestConnectionInfo};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram, UpDownCounter},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock, mpsc, watch};
use tokio::time::{Instant as TokioInstant, timeout_at};
use tokio_tungstenite::tungstenite::{Message, protocol::CloseFrame};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod broadcast;
pub(crate) use broadcast::{
    MAX_BROADCAST_CONNECTION_TARGETS, MAX_BROADCAST_EXCLUSIONS, MAX_BROADCAST_ROOMS,
    PreparedBroadcast, validate_broadcast_namespace,
};

use crate::groups::{GroupError, GroupManager};
use crate::middleware::WsConnectionCloseCategory;
use crate::request::{ConnectionState, WsMessageBody, WsWireFormat};
use crate::server::{
    DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE_BYTES, DEFAULT_OUTBOUND_ADMISSION_TIMEOUT_MILLIS,
    WsTransportSecurity,
};

/// Maximum application metadata entries retained by one live connection.
pub const MAX_CONNECTION_METADATA_ENTRIES: usize = 32;
/// Maximum UTF-8 byte length of one connection metadata key.
pub const MAX_CONNECTION_METADATA_KEY_BYTES: usize = 128;
/// Maximum UTF-8 byte length of one connection metadata value.
pub const MAX_CONNECTION_METADATA_VALUE_BYTES: usize = 1024;
/// Maximum UTF-8 byte length of an application-owned WebSocket principal ID.
pub const MAX_PRINCIPAL_ID_BYTES: usize = 256;

/// Exact, bounded application-owned identity used only for online targeting.
///
/// Lily never normalizes, verifies, logs, or interprets this value. The
/// application identity middleware derives it from a verified [`Principal`].
/// Equality is byte-exact and case-sensitive. `Debug` deliberately redacts the
/// value because it commonly contains personally identifying information.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrincipalId(Arc<str>);

impl PrincipalId {
    /// Validates and owns one exact principal identifier.
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, PrincipalIdError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(PrincipalIdError::Empty);
        }
        if value.len() > MAX_PRINCIPAL_ID_BYTES {
            return Err(PrincipalIdError::TooLong);
        }
        if value.trim() != value {
            return Err(PrincipalIdError::SurroundingWhitespace);
        }
        if value.chars().any(char::is_control) {
            return Err(PrincipalIdError::ControlCharacter);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Returns the exact application-owned identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PrincipalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PrincipalId([REDACTED])")
    }
}

impl FromStr for PrincipalId {
    type Err = PrincipalIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_new(value)
    }
}

impl TryFrom<String> for PrincipalId {
    type Error = PrincipalIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl TryFrom<&str> for PrincipalId {
    type Error = PrincipalIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl Serialize for PrincipalId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PrincipalId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(de::Error::custom)
    }
}

/// Validation failure for an application-owned principal identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PrincipalIdError {
    /// The identifier is empty.
    #[error("WebSocket principal ID cannot be empty")]
    Empty,
    /// The identifier exceeds [`MAX_PRINCIPAL_ID_BYTES`].
    #[error("WebSocket principal ID exceeds its byte limit")]
    TooLong,
    /// Leading or trailing whitespace would make the identifier ambiguous.
    #[error("WebSocket principal ID cannot contain surrounding whitespace")]
    SurroundingWhitespace,
    /// Control characters are not valid identity data.
    #[error("WebSocket principal ID cannot contain control characters")]
    ControlCharacter,
}

/// Immutable application-verified identity accepted for one WebSocket
/// connection generation.
///
/// The framework validates only the bounded targeting identifier derived from
/// [`Principal::subject`]. Token, session, credential, role, and claim
/// verification remain application responsibilities.
#[derive(Clone, PartialEq)]
pub struct AuthenticatedWebSocketIdentity {
    principal: Principal,
    principal_id: PrincipalId,
    expires_at: Option<TokioInstant>,
}

impl AuthenticatedWebSocketIdentity {
    /// Creates a bounded targeting identity from an application-verified
    /// principal.
    pub fn try_new(principal: Principal) -> Result<Self, PrincipalIdError> {
        let principal_id = PrincipalId::try_new(principal.subject())?;
        Ok(Self {
            principal,
            principal_id,
            expires_at: None,
        })
    }

    /// Schedules a hard policy close at the supplied monotonic deadline.
    ///
    /// Passing a deadline that has already elapsed is valid; Lily will reject
    /// publication or close the connection immediately. The application owns
    /// refresh/re-authentication before this deadline.
    #[must_use]
    pub fn expires_at(mut self, deadline: TokioInstant) -> Self {
        self.expires_at = Some(deadline);
        self
    }

    /// Application-owned principal and claims for this identity generation.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Exact bounded online-targeting identifier.
    #[must_use]
    pub const fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    /// Optional hard expiry deadline.
    #[must_use]
    pub const fn expiry(&self) -> Option<TokioInstant> {
        self.expires_at
    }
}

impl fmt::Debug for AuthenticatedWebSocketIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedWebSocketIdentity")
            .field("principal", &"[REDACTED]")
            .field("has_expiry", &self.expires_at.is_some())
            .finish()
    }
}

/// Opaque generation number for one connection's current identity snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WebSocketIdentityRevision(u64);

impl WebSocketIdentityRevision {
    const INITIAL: Self = Self(0);

    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// Immutable point-in-time view of the connection's current identity.
#[derive(Clone, PartialEq)]
pub struct WebSocketIdentitySnapshot {
    revision: WebSocketIdentityRevision,
    authenticated: Option<AuthenticatedWebSocketIdentity>,
}

impl WebSocketIdentitySnapshot {
    pub(crate) fn anonymous() -> Self {
        Self {
            revision: WebSocketIdentityRevision::INITIAL,
            authenticated: None,
        }
    }

    pub(crate) fn authenticated(identity: AuthenticatedWebSocketIdentity) -> Self {
        Self {
            revision: WebSocketIdentityRevision::INITIAL,
            authenticated: Some(identity),
        }
    }

    /// Generation used for compare-and-replace re-authentication.
    #[must_use]
    pub const fn revision(&self) -> WebSocketIdentityRevision {
        self.revision
    }

    /// Current application-verified identity, when the connection is not
    /// anonymous.
    #[must_use]
    pub const fn authenticated_identity(&self) -> Option<&AuthenticatedWebSocketIdentity> {
        self.authenticated.as_ref()
    }

    /// Current application-owned principal.
    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        self.authenticated
            .as_ref()
            .map(|identity| &identity.principal)
    }

    /// Current online-targeting identifier.
    #[must_use]
    pub fn principal_id(&self) -> Option<&PrincipalId> {
        self.authenticated
            .as_ref()
            .map(|identity| &identity.principal_id)
    }

    /// Current hard expiry deadline.
    #[must_use]
    pub fn expiry(&self) -> Option<TokioInstant> {
        self.authenticated
            .as_ref()
            .and_then(|identity| identity.expires_at)
    }

    fn with_replacement(&self, replacement: AuthenticatedWebSocketIdentity) -> Option<Self> {
        Some(Self {
            revision: self.revision.next()?,
            authenticated: Some(replacement),
        })
    }
}

impl fmt::Debug for WebSocketIdentitySnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketIdentitySnapshot")
            .field("revision", &self.revision)
            .field("authenticated", &self.authenticated.is_some())
            .field("has_expiry", &self.expiry().is_some())
            .finish()
    }
}

/// Result of a revision-checked application re-authentication attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebSocketIdentityUpdateOutcome {
    /// The supplied identity became current at this new revision.
    Updated(WebSocketIdentityRevision),
    /// A newer identity already replaced the caller's observed revision.
    Stale(WebSocketIdentityRevision),
    /// Connection closing already began; its identity can no longer change.
    AlreadyClosing,
}

pub(crate) enum IdentityExpiryClaim {
    Superseded,
    Closing(CloseRequestOutcome),
}

#[derive(Clone)]
pub(crate) struct ConnectionIdentityHandle {
    updates: watch::Sender<WebSocketIdentitySnapshot>,
}

impl fmt::Debug for ConnectionIdentityHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionIdentityHandle")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl ConnectionIdentityHandle {
    pub(crate) fn new(snapshot: WebSocketIdentitySnapshot) -> Self {
        let (updates, _) = watch::channel(snapshot);
        Self { updates }
    }

    pub(crate) fn snapshot(&self) -> WebSocketIdentitySnapshot {
        self.updates.borrow().clone()
    }

    fn publish(&self, snapshot: WebSocketIdentitySnapshot) {
        self.updates.send_replace(snapshot);
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<WebSocketIdentitySnapshot> {
        self.updates.subscribe()
    }
}

/// Coalescing protocol-control path owned separately from application data.
///
/// Close is first-writer-wins and protocol traffic retains only its newest
/// frame. Both slots remain bounded regardless of application queue depth.
#[derive(Debug, Clone)]
pub(crate) struct ConnectionControlSender {
    close: watch::Sender<Option<ConnectionCloseRequest>>,
    protocol: watch::Sender<Option<Message>>,
    close_requested: Arc<AtomicBool>,
}

#[derive(Debug)]
pub(crate) struct ConnectionControlReceiver {
    close: watch::Receiver<Option<ConnectionCloseRequest>>,
    protocol: watch::Receiver<Option<Message>>,
}

/// One provenance-preserving terminal request for the transport writer.
///
/// The wire frame and lifecycle category are published through the same
/// first-writer slot so a losing close cannot replace either half.
#[derive(Debug, Clone)]
pub(crate) struct ConnectionCloseRequest {
    message: Message,
    category: WsConnectionCloseCategory,
}

impl ConnectionCloseRequest {
    #[cfg(test)]
    pub(crate) fn message(&self) -> &Message {
        &self.message
    }

    #[cfg(test)]
    pub(crate) const fn category(&self) -> WsConnectionCloseCategory {
        self.category
    }

    pub(crate) fn into_parts(self) -> (Message, WsConnectionCloseCategory) {
        (self.message, self.category)
    }
}

#[derive(Debug)]
pub(crate) enum ConnectionControlFrame {
    Close(ConnectionCloseRequest),
    Protocol(Message),
}

/// Result of requesting the single bounded close frame for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseRequestOutcome {
    /// This request won the first-writer close slot and queued its frame.
    Requested,
    /// Another shutdown, timeout, or application request already owns close.
    AlreadyClosing,
}

pub(crate) fn connection_control_channel() -> (ConnectionControlSender, ConnectionControlReceiver) {
    let (close, close_rx) = watch::channel(None);
    let (protocol, protocol_rx) = watch::channel(None);
    (
        ConnectionControlSender {
            close,
            protocol,
            close_requested: Arc::new(AtomicBool::new(false)),
        },
        ConnectionControlReceiver {
            close: close_rx,
            protocol: protocol_rx,
        },
    )
}

impl ConnectionControlSender {
    pub(crate) fn request_close(
        &self,
        message: Message,
        category: WsConnectionCloseCategory,
    ) -> Result<CloseRequestOutcome, ()> {
        debug_assert!(matches!(message, Message::Close(_)));
        if self
            .close_requested
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(CloseRequestOutcome::AlreadyClosing);
        }
        self.close
            .send(Some(ConnectionCloseRequest { message, category }))
            .map_err(|_| ())?;
        Ok(CloseRequestOutcome::Requested)
    }

    pub(crate) fn send_protocol(&self, message: Message) -> Result<(), ()> {
        debug_assert!(matches!(message, Message::Ping(_) | Message::Pong(_)));
        self.protocol.send(Some(message)).map_err(|_| ())
    }
}

impl ConnectionControlReceiver {
    pub(crate) async fn next(&mut self) -> Result<ConnectionControlFrame, ()> {
        tokio::select! {
            biased;
            result = self.close.changed() => {
                result.map_err(|_| ())?;
                self.close
                    .borrow_and_update()
                    .clone()
                    .map(ConnectionControlFrame::Close)
                    .ok_or(())
            }
            result = self.protocol.changed() => {
                result.map_err(|_| ())?;
                self.protocol
                    .borrow_and_update()
                    .clone()
                    .map(ConnectionControlFrame::Protocol)
                    .ok_or(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundAdmissionWaitError {
    TimedOut,
    Closed,
}

/// Exact connection-local application-byte authority.
///
/// Reservations include frames waiting for a message-count slot, frames in
/// the bounded channel, and the one frame currently owned by the transport
/// writer. Closing the budget wakes every pending admission without retaining
/// any replay state for the disconnected connection.
#[derive(Debug)]
pub(crate) struct OutboundByteBudget {
    limit: usize,
    used: AtomicUsize,
    released: Notify,
    closed: CancellationToken,
}

impl OutboundByteBudget {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
            released: Notify::new(),
            closed: CancellationToken::new(),
        })
    }

    fn close(&self) {
        self.closed.cancel();
        self.released.notify_waiters();
    }

    fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }

    fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<OutboundByteReservation> {
        if self.is_closed() {
            return None;
        }
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes)?;
            if next > self.limit {
                return None;
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let reservation = OutboundByteReservation {
                        budget: Arc::clone(self),
                        bytes,
                    };
                    if self.is_closed() {
                        drop(reservation);
                        return None;
                    }
                    return Some(reservation);
                }
                Err(observed) => current = observed,
            }
        }
    }

    async fn reserve_until(
        self: &Arc<Self>,
        bytes: usize,
        deadline: TokioInstant,
    ) -> Result<OutboundByteReservation, OutboundAdmissionWaitError> {
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            // Register before checking capacity so a concurrent reservation
            // release cannot be lost between the CAS and the await.
            released.as_mut().enable();
            if let Some(reservation) = self.try_reserve(bytes) {
                return Ok(reservation);
            }
            if self.is_closed() {
                return Err(OutboundAdmissionWaitError::Closed);
            }
            tokio::select! {
                biased;
                _ = self.closed.cancelled() => {
                    return Err(OutboundAdmissionWaitError::Closed);
                }
                result = timeout_at(deadline, &mut released) => {
                    if result.is_err() {
                        return Err(OutboundAdmissionWaitError::TimedOut);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn used_bytes(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub(crate) struct OutboundByteReservation {
    budget: Arc<OutboundByteBudget>,
    bytes: usize,
}

impl Drop for OutboundByteReservation {
    fn drop(&mut self) {
        let previous = self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes);
        self.budget.released.notify_waiters();
    }
}

/// One immutable application message retained either by its direct producer
/// or as a shared broadcast payload. Broadcast admission clones only the Arc;
/// each transport writer materializes an owned Tungstenite message when it is
/// actually ready to write.
#[derive(Debug)]
enum OutboundApplicationMessage {
    Owned(Message),
    Shared(Arc<Message>),
}

impl OutboundApplicationMessage {
    fn len(&self) -> usize {
        self.as_message().len()
    }

    fn as_message(&self) -> &Message {
        match self {
            Self::Owned(message) => message,
            Self::Shared(message) => message.as_ref(),
        }
    }

    fn into_message(self) -> Message {
        match self {
            Self::Owned(message) => message,
            Self::Shared(message) => Arc::unwrap_or_clone(message),
        }
    }
}

/// One application frame whose byte reservation lives through transport
/// write and flush completion. Dropping a queued or in-flight frame releases
/// its reservation exactly once.
#[derive(Debug)]
pub(crate) struct QueuedApplicationFrame {
    message: OutboundApplicationMessage,
    reservation: OutboundByteReservation,
}

impl QueuedApplicationFrame {
    pub(crate) fn into_parts(self) -> (Message, OutboundByteReservation) {
        (self.message.into_message(), self.reservation)
    }

    #[cfg(test)]
    pub(crate) fn for_test(message: Message) -> Self {
        Self::for_test_with_budget(message).0
    }

    #[cfg(test)]
    pub(crate) fn for_test_with_budget(message: Message) -> (Self, Arc<OutboundByteBudget>) {
        let budget = OutboundByteBudget::new(message.len());
        let reservation = budget
            .try_reserve(message.len())
            .expect("test frame budget exactly fits");
        (
            Self {
                message: OutboundApplicationMessage::Owned(message),
                reservation,
            },
            budget,
        )
    }

    #[cfg(test)]
    pub(crate) fn for_test_shared_with_budget(
        message: Arc<Message>,
    ) -> (Self, Arc<OutboundByteBudget>) {
        let budget = OutboundByteBudget::new(message.len());
        let reservation = budget
            .try_reserve(message.len())
            .expect("test shared frame budget exactly fits");
        (
            Self {
                message: OutboundApplicationMessage::Shared(message),
                reservation,
            },
            budget,
        )
    }

    #[cfg(test)]
    pub(crate) fn message(&self) -> &Message {
        self.message.as_message()
    }

    #[cfg(test)]
    fn shares_message_with(&self, expected: &Arc<Message>) -> bool {
        matches!(
            &self.message,
            OutboundApplicationMessage::Shared(message) if Arc::ptr_eq(message, expected)
        )
    }

    #[cfg(test)]
    fn shares_message_with_frame(&self, other: &Self) -> bool {
        matches!(
            (&self.message, &other.message),
            (
                OutboundApplicationMessage::Shared(left),
                OutboundApplicationMessage::Shared(right),
            ) if Arc::ptr_eq(left, right)
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ConnectionDataSender {
    Managed(mpsc::Sender<QueuedApplicationFrame>),
    #[cfg(any(test, feature = "fuzzing"))]
    Harness(mpsc::Sender<Message>),
}

impl From<mpsc::Sender<QueuedApplicationFrame>> for ConnectionDataSender {
    fn from(sender: mpsc::Sender<QueuedApplicationFrame>) -> Self {
        Self::Managed(sender)
    }
}

#[cfg(any(test, feature = "fuzzing"))]
impl From<mpsc::Sender<Message>> for ConnectionDataSender {
    fn from(sender: mpsc::Sender<Message>) -> Self {
        Self::Harness(sender)
    }
}

enum ConnectionDataPermit {
    Managed(mpsc::OwnedPermit<QueuedApplicationFrame>),
    #[cfg(any(test, feature = "fuzzing"))]
    Harness(mpsc::OwnedPermit<Message>),
}

impl ConnectionDataPermit {
    fn send(self, message: OutboundApplicationMessage, reservation: OutboundByteReservation) {
        match self {
            Self::Managed(permit) => {
                permit.send(QueuedApplicationFrame {
                    message,
                    reservation,
                });
            }
            #[cfg(any(test, feature = "fuzzing"))]
            Self::Harness(permit) => {
                permit.send(message.into_message());
                drop(reservation);
            }
        }
    }
}

impl ConnectionDataSender {
    fn max_capacity(&self) -> usize {
        match self {
            Self::Managed(sender) => sender.max_capacity(),
            #[cfg(any(test, feature = "fuzzing"))]
            Self::Harness(sender) => sender.max_capacity(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Managed(sender) => sender.capacity(),
            #[cfg(any(test, feature = "fuzzing"))]
            Self::Harness(sender) => sender.capacity(),
        }
    }

    async fn reserve_until(
        &self,
        closed: &CancellationToken,
        deadline: TokioInstant,
    ) -> Result<ConnectionDataPermit, OutboundAdmissionWaitError> {
        match self {
            Self::Managed(sender) => {
                let reserve = sender.clone().reserve_owned();
                tokio::select! {
                    biased;
                    _ = closed.cancelled() => Err(OutboundAdmissionWaitError::Closed),
                    result = timeout_at(deadline, reserve) => match result {
                        Err(_) => Err(OutboundAdmissionWaitError::TimedOut),
                        Ok(Err(_)) => Err(OutboundAdmissionWaitError::Closed),
                        Ok(Ok(permit)) => Ok(ConnectionDataPermit::Managed(permit)),
                    }
                }
            }
            #[cfg(any(test, feature = "fuzzing"))]
            Self::Harness(sender) => {
                let reserve = sender.clone().reserve_owned();
                tokio::select! {
                    biased;
                    _ = closed.cancelled() => Err(OutboundAdmissionWaitError::Closed),
                    result = timeout_at(deadline, reserve) => match result {
                        Err(_) => Err(OutboundAdmissionWaitError::TimedOut),
                        Ok(Err(_)) => Err(OutboundAdmissionWaitError::Closed),
                        Ok(Ok(permit)) => Ok(ConnectionDataPermit::Harness(permit)),
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
/// Application-owned node-local registry for active WebSocket connections.
///
/// Most handlers should use [`crate::WebSocketContext`] instead. This lower-level
/// handle is exposed by [`crate::WsApp::connection_manager`] for operational
/// inspection and deliberately local broadcasts. Use
/// [`crate::WsApp::dispatcher`] when an out-of-controller send must participate
/// in a configured distributed backplane.
///
/// Registry removal belongs to the connection lifecycle. Applications use
/// [`crate::WebSocketContext::close`] to request closure; they cannot detach a
/// live transport from its cleanup owner:
///
/// ```compile_fail,E0624
/// use lily_websocket::ConnectionManager;
/// use uuid::Uuid;
/// async fn detach(manager: &ConnectionManager, id: Uuid) {
///     manager.remove_connection(id).await.unwrap();
/// }
/// ```
pub struct ConnectionManager {
    /// Active connections and their optional identity index share one lock so
    /// admission, re-authentication, close, and targeting are linearizable.
    registry: Arc<RwLock<ConnectionRegistry>>,
    /// Group manager for namespaces and rooms
    group_manager: Arc<GroupManager>,
    identity_index_enabled: bool,
    max_rooms_per_connection: usize,
    max_room_name_length: usize,
    max_outbound_message_size: usize,
    outbound_queue_max_bytes: usize,
    outbound_admission_timeout: Duration,
    metrics: Arc<WebSocketServerMetrics>,
    #[cfg(test)]
    join_room_registry_read_barrier: Arc<std::sync::Mutex<Option<JoinRoomTestBarrier>>>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct JoinRoomTestBarrier {
    connection_id: Uuid,
    registry_read: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrincipalTargetKey {
    namespace: String,
    principal_id: PrincipalId,
}

#[derive(Debug)]
struct ConnectionRegistry {
    connections: HashMap<Uuid, Connection>,
    principal_index: Option<HashMap<PrincipalTargetKey, HashSet<Uuid>>>,
}

impl ConnectionRegistry {
    fn new(identity_index_enabled: bool) -> Self {
        Self {
            connections: HashMap::new(),
            principal_index: identity_index_enabled.then(HashMap::new),
        }
    }

    fn remove_principal_membership(&mut self, connection: &Connection) {
        self.remove_principal_membership_parts(
            &connection.namespace,
            connection.id,
            connection.identity.as_ref(),
        );
    }

    fn remove_principal_membership_parts(
        &mut self,
        namespace: &str,
        connection_id: Uuid,
        identity: Option<&ConnectionIdentityHandle>,
    ) {
        let Some(identity) = identity else {
            return;
        };
        let snapshot = identity.snapshot();
        let Some(principal_id) = snapshot.principal_id() else {
            return;
        };
        let key = PrincipalTargetKey {
            namespace: namespace.to_owned(),
            principal_id: principal_id.clone(),
        };
        let Some(index) = self.principal_index.as_mut() else {
            return;
        };
        if let Some(connection_ids) = index.get_mut(&key) {
            connection_ids.remove(&connection_id);
            if connection_ids.is_empty() {
                index.remove(&key);
            }
        }
    }

    fn add_principal_membership(&mut self, connection: &Connection) {
        self.add_principal_membership_parts(
            &connection.namespace,
            connection.id,
            connection.identity.as_ref(),
        );
    }

    fn add_principal_membership_parts(
        &mut self,
        namespace: &str,
        connection_id: Uuid,
        identity: Option<&ConnectionIdentityHandle>,
    ) {
        let Some(identity) = identity else {
            return;
        };
        let snapshot = identity.snapshot();
        let Some(principal_id) = snapshot.principal_id() else {
            return;
        };
        let Some(index) = self.principal_index.as_mut() else {
            return;
        };
        index
            .entry(PrincipalTargetKey {
                namespace: namespace.to_owned(),
                principal_id: principal_id.clone(),
            })
            .or_default()
            .insert(connection_id);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Point-in-time bounded counters for the WebSocket server runtime.
pub struct WebSocketServerMetricSnapshot {
    /// Connections admitted after all admission policies completed.
    pub connection_admitted: u64,
    /// Connections rejected by capacity or admission policy.
    pub connection_rejected: u64,
    /// Successful HTTP Upgrade handshakes.
    pub handshakes_succeeded: u64,
    /// Failed HTTP Upgrade handshakes.
    pub handshakes_failed: u64,
    /// Accepted inbound application messages.
    pub inbound_messages: u64,
    /// Application messages accepted by outbound queues.
    pub outbound_messages: u64,
    /// Outbound sends whose bounded admission deadline elapsed.
    pub backpressured: u64,
    /// Outbound sends rejected because a connection queue was closed.
    pub channel_closed: u64,
    /// Action handlers that completed successfully.
    pub handler_succeeded: u64,
    /// Action handlers that failed or panicked.
    pub handler_failed: u64,
    /// Connections closed through a normal bounded lifecycle.
    pub graceful_closes: u64,
    /// Connections requiring forced cleanup.
    pub forced_closes: u64,
    /// Connection tasks terminated by cancellation.
    pub cancellations: u64,
    /// Bounded stages that reached their deadline.
    pub timeouts: u64,
    /// Outbound frames accepted by the configured backplane transport.
    ///
    /// This counter does not claim delivery to a remote node or client.
    pub backplane_publish_accepted: u64,
    /// Outbound frames rejected by a full bounded backplane publisher.
    pub backplane_publish_saturated: u64,
    /// Distributed dispatches rejected because the backplane was unavailable.
    pub backplane_publish_unavailable: u64,
    /// Other outbound backplane publish failures or panics.
    pub backplane_publish_failed: u64,
    /// Inbound backplane frames rejected by Lily protocol validation.
    pub backplane_invalid_frames: u64,
    /// Valid inbound backplane frames whose node-local broadcast returned an error.
    /// Per-recipient partial results remain in the outbound delivery counters.
    pub backplane_local_dispatch_failed: u64,
    /// Inbound backplane frames suppressed as already processed.
    pub backplane_duplicates_suppressed: u64,
    /// Inbound backplane frames suppressed because this node originated them.
    pub backplane_origin_loops_suppressed: u64,
}

#[derive(Debug)]
pub(crate) struct WebSocketServerMetrics {
    handshakes: Counter<u64>,
    connection_admission: Counter<u64>,
    connection_admission_wait: Histogram<f64>,
    handshake_duration: Histogram<f64>,
    active_connections: UpDownCounter<i64>,
    connection_duration: Histogram<f64>,
    messages: Counter<u64>,
    handler_duration: Histogram<f64>,
    queue_depth: Histogram<u64>,
    queue_wait: Histogram<f64>,
    backpressure: Counter<u64>,
    close_outcomes: Counter<u64>,
    timeouts_metric: Counter<u64>,
    backplane_outcomes: Counter<u64>,
    handshakes_succeeded: AtomicU64,
    handshakes_failed: AtomicU64,
    connection_admitted: AtomicU64,
    connection_rejected: AtomicU64,
    inbound_messages: AtomicU64,
    outbound_messages: AtomicU64,
    backpressured: AtomicU64,
    channel_closed: AtomicU64,
    handler_succeeded: AtomicU64,
    handler_failed: AtomicU64,
    graceful_closes: AtomicU64,
    forced_closes: AtomicU64,
    cancellations: AtomicU64,
    timeouts: AtomicU64,
    backplane_publish_accepted: AtomicU64,
    backplane_publish_saturated: AtomicU64,
    backplane_publish_unavailable: AtomicU64,
    backplane_publish_failed: AtomicU64,
    backplane_invalid_frames: AtomicU64,
    backplane_local_dispatch_failed: AtomicU64,
    backplane_duplicates_suppressed: AtomicU64,
    backplane_origin_loops_suppressed: AtomicU64,
}

impl WebSocketServerMetrics {
    fn new() -> Arc<Self> {
        let meter = global::meter("lily_websocket");
        Arc::new(Self {
            handshakes: meter.u64_counter("websocket.server.handshakes").build(),
            connection_admission: meter
                .u64_counter("websocket.server.connection.admission")
                .build(),
            connection_admission_wait: meter
                .f64_histogram("websocket.server.connection.admission.wait.duration")
                .with_unit("s")
                .build(),
            handshake_duration: meter
                .f64_histogram("websocket.server.handshake.duration")
                .with_unit("s")
                .build(),
            active_connections: meter
                .i64_up_down_counter("websocket.server.connections.active")
                .build(),
            connection_duration: meter
                .f64_histogram("websocket.server.connection.duration")
                .with_unit("s")
                .build(),
            messages: meter.u64_counter("websocket.server.messages").build(),
            handler_duration: meter
                .f64_histogram("websocket.server.handler.duration")
                .with_unit("s")
                .build(),
            queue_depth: meter
                .u64_histogram("websocket.server.outbound.queue.depth")
                .build(),
            queue_wait: meter
                .f64_histogram("websocket.server.outbound.queue.wait.duration")
                .with_unit("s")
                .build(),
            backpressure: meter.u64_counter("websocket.server.backpressure").build(),
            close_outcomes: meter.u64_counter("websocket.server.close.outcomes").build(),
            timeouts_metric: meter.u64_counter("websocket.server.timeouts").build(),
            backplane_outcomes: meter
                .u64_counter("websocket.server.backplane.outcomes")
                .with_description(
                    "Bounded WebSocket backplane publish, inbound protocol and local dispatch outcomes",
                )
                .build(),
            handshakes_succeeded: AtomicU64::new(0),
            handshakes_failed: AtomicU64::new(0),
            connection_admitted: AtomicU64::new(0),
            connection_rejected: AtomicU64::new(0),
            inbound_messages: AtomicU64::new(0),
            outbound_messages: AtomicU64::new(0),
            backpressured: AtomicU64::new(0),
            channel_closed: AtomicU64::new(0),
            handler_succeeded: AtomicU64::new(0),
            handler_failed: AtomicU64::new(0),
            graceful_closes: AtomicU64::new(0),
            forced_closes: AtomicU64::new(0),
            cancellations: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            backplane_publish_accepted: AtomicU64::new(0),
            backplane_publish_saturated: AtomicU64::new(0),
            backplane_publish_unavailable: AtomicU64::new(0),
            backplane_publish_failed: AtomicU64::new(0),
            backplane_invalid_frames: AtomicU64::new(0),
            backplane_local_dispatch_failed: AtomicU64::new(0),
            backplane_duplicates_suppressed: AtomicU64::new(0),
            backplane_origin_loops_suppressed: AtomicU64::new(0),
        })
    }

    pub(crate) fn connection_admission(
        &self,
        outcome: &'static str,
        duration: std::time::Duration,
    ) {
        let attributes = [KeyValue::new("lily.outcome", outcome)];
        self.connection_admission.add(1, &attributes);
        self.connection_admission_wait
            .record(duration.as_secs_f64(), &attributes);
        if outcome == "accepted" {
            self.connection_admitted.fetch_add(1, Ordering::Relaxed);
        } else {
            self.connection_rejected.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn handshake(&self, outcome: &'static str, duration: std::time::Duration) {
        let attributes = [KeyValue::new("lily.outcome", outcome)];
        self.handshakes.add(1, &attributes);
        self.handshake_duration
            .record(duration.as_secs_f64(), &attributes);
        if outcome == "success" {
            self.handshakes_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.handshakes_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn inbound_message(&self) {
        self.inbound_messages.fetch_add(1, Ordering::Relaxed);
        self.messages
            .add(1, &[KeyValue::new("lily.direction", "inbound")]);
    }

    pub(crate) fn handler(&self, outcome: &'static str, duration: std::time::Duration) {
        self.handler_duration.record(
            duration.as_secs_f64(),
            &[KeyValue::new("lily.outcome", outcome)],
        );
        if outcome == "success" {
            self.handler_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.handler_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn close(&self, outcome: &'static str) {
        self.close_outcomes
            .add(1, &[KeyValue::new("lily.close_category", outcome)]);
        match outcome {
            "forced" => {
                self.forced_closes.fetch_add(1, Ordering::Relaxed);
            }
            "cancelled" => {
                self.cancellations.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.graceful_closes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn timeout(&self, category: &'static str) {
        self.timeouts_metric
            .add(1, &[KeyValue::new("lily.timeout_category", category)]);
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    fn backplane_outcome(
        &self,
        direction: &'static str,
        outcome: &'static str,
        snapshot_counter: &AtomicU64,
    ) {
        self.backplane_outcomes.add(
            1,
            &[
                KeyValue::new("lily.direction", direction),
                KeyValue::new("lily.outcome", outcome),
            ],
        );
        snapshot_counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn backplane_publish_accepted(&self) {
        self.backplane_outcome("outbound", "accepted", &self.backplane_publish_accepted);
    }

    pub(crate) fn backplane_publish_saturated(&self) {
        self.backplane_outcome("outbound", "saturated", &self.backplane_publish_saturated);
    }

    pub(crate) fn backplane_publish_unavailable(&self) {
        self.backplane_outcome(
            "outbound",
            "unavailable",
            &self.backplane_publish_unavailable,
        );
    }

    pub(crate) fn backplane_publish_failed(&self) {
        self.backplane_outcome("outbound", "failed", &self.backplane_publish_failed);
    }

    pub(crate) fn backplane_invalid_frame(&self) {
        self.backplane_outcome("inbound", "invalid", &self.backplane_invalid_frames);
    }

    pub(crate) fn backplane_local_dispatch_failed(&self) {
        self.backplane_outcome(
            "inbound",
            "local_dispatch_failed",
            &self.backplane_local_dispatch_failed,
        );
    }

    pub(crate) fn backplane_duplicate_suppressed(&self) {
        self.backplane_outcome(
            "inbound",
            "deduplicated",
            &self.backplane_duplicates_suppressed,
        );
    }

    pub(crate) fn backplane_origin_loop_suppressed(&self) {
        self.backplane_outcome(
            "inbound",
            "origin_loop_suppressed",
            &self.backplane_origin_loops_suppressed,
        );
    }

    fn outbound_admission(&self, result: &'static str, wait: std::time::Duration, depth: usize) {
        self.queue_wait
            .record(wait.as_secs_f64(), &[KeyValue::new("lily.outcome", result)]);
        self.queue_depth
            .record(u64::try_from(depth).unwrap_or(u64::MAX), &[]);
        match result {
            "accepted" => {
                self.outbound_messages.fetch_add(1, Ordering::Relaxed);
                self.messages
                    .add(1, &[KeyValue::new("lily.direction", "outbound")]);
            }
            "timeout" => {
                self.backpressured.fetch_add(1, Ordering::Relaxed);
                self.backpressure
                    .add(1, &[KeyValue::new("lily.outcome", "timeout")]);
            }
            "closed" => {
                self.channel_closed.fetch_add(1, Ordering::Relaxed);
                self.backpressure
                    .add(1, &[KeyValue::new("lily.outcome", "closed")]);
            }
            _ => {}
        }
    }

    fn broadcast_outcomes(&self, report: &BroadcastReport) {
        self.outbound_messages.fetch_add(
            u64::try_from(report.sent).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.backpressured.fetch_add(
            u64::try_from(report.backpressured).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.channel_closed.fetch_add(
            u64::try_from(report.closed).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        for (result, count) in [
            ("accepted", report.sent),
            ("timeout", report.backpressured),
            ("closed", report.closed),
            ("missing", report.missing),
        ] {
            if count > 0 {
                self.messages.add(
                    u64::try_from(count).unwrap_or(u64::MAX),
                    &[
                        KeyValue::new("lily.direction", "outbound"),
                        KeyValue::new("lily.outcome", result),
                    ],
                );
            }
        }
    }

    fn snapshot(&self) -> WebSocketServerMetricSnapshot {
        WebSocketServerMetricSnapshot {
            connection_admitted: self.connection_admitted.load(Ordering::Acquire),
            connection_rejected: self.connection_rejected.load(Ordering::Acquire),
            handshakes_succeeded: self.handshakes_succeeded.load(Ordering::Acquire),
            handshakes_failed: self.handshakes_failed.load(Ordering::Acquire),
            inbound_messages: self.inbound_messages.load(Ordering::Acquire),
            outbound_messages: self.outbound_messages.load(Ordering::Acquire),
            backpressured: self.backpressured.load(Ordering::Acquire),
            channel_closed: self.channel_closed.load(Ordering::Acquire),
            handler_succeeded: self.handler_succeeded.load(Ordering::Acquire),
            handler_failed: self.handler_failed.load(Ordering::Acquire),
            graceful_closes: self.graceful_closes.load(Ordering::Acquire),
            forced_closes: self.forced_closes.load(Ordering::Acquire),
            cancellations: self.cancellations.load(Ordering::Acquire),
            timeouts: self.timeouts.load(Ordering::Acquire),
            backplane_publish_accepted: self.backplane_publish_accepted.load(Ordering::Acquire),
            backplane_publish_saturated: self.backplane_publish_saturated.load(Ordering::Acquire),
            backplane_publish_unavailable: self
                .backplane_publish_unavailable
                .load(Ordering::Acquire),
            backplane_publish_failed: self.backplane_publish_failed.load(Ordering::Acquire),
            backplane_invalid_frames: self.backplane_invalid_frames.load(Ordering::Acquire),
            backplane_local_dispatch_failed: self
                .backplane_local_dispatch_failed
                .load(Ordering::Acquire),
            backplane_duplicates_suppressed: self
                .backplane_duplicates_suppressed
                .load(Ordering::Acquire),
            backplane_origin_loops_suppressed: self
                .backplane_origin_loops_suppressed
                .load(Ordering::Acquire),
        }
    }
}

/// RAII registration for one connection. If a connection future is cancelled
/// or panics, dropping this lease schedules idempotent connection/group/room
/// cleanup on the current Tokio runtime.
#[cfg(test)]
pub(crate) struct ConnectionLease {
    manager: ConnectionManager,
    connection_id: Uuid,
    released: bool,
}

#[cfg(test)]
impl ConnectionLease {
    pub(crate) fn new(manager: ConnectionManager, connection_id: Uuid) -> Self {
        Self {
            manager,
            connection_id,
            released: false,
        }
    }

    pub(crate) async fn release(&mut self) -> Result<(), ConnectionError> {
        if !self.released {
            self.released = true;
            match self.manager.remove_connection(self.connection_id).await {
                Ok(()) | Err(ConnectionError::ConnectionNotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let manager = self.manager.clone();
        let connection_id = self.connection_id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                match manager.remove_connection(connection_id).await {
                    Ok(()) | Err(ConnectionError::ConnectionNotFound { .. }) => {}
                    Err(error) => {
                        tracing::warn!(%connection_id, %error, "WebSocket RAII cleanup failed");
                    }
                }
            });
        }
    }
}

/// Individual WebSocket connection
pub(crate) struct Connection {
    /// Connection ID
    pub id: Uuid,
    /// Bounded application-data sender.
    data_sender: ConnectionDataSender,
    /// Exact application bytes charged from admission through write/flush.
    outbound_budget: Arc<OutboundByteBudget>,
    /// Coalescing protocol-control sender.
    control_sender: ConnectionControlSender,
    /// Connection metadata
    metadata: HashMap<String, String>,
    /// Current namespace
    pub namespace: String,
    /// Current versioned application identity when the app configured the
    /// identity boundary. `None` means the capability is disabled, not merely
    /// that this connection is anonymous.
    identity: Option<ConnectionIdentityHandle>,
    /// Connection state
    pub state: ConnectionState,
    /// Typed socket-peer and effective-client identity.
    pub connection_info: RequestConnectionInfo,
    /// Server-owned WS/WSS transport classification.
    pub transport_security: WsTransportSecurity,
    /// Connection creation time
    pub created_at: std::time::SystemTime,
    /// Last transport liveness observed, including Ping/Pong frames.
    pub last_liveness_at: std::time::SystemTime,
    /// Last application Text/Binary message received. Application-idle
    /// enforcement uses only this timestamp.
    pub last_application_activity_at: std::time::SystemTime,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut metadata_keys = self.metadata.keys().map(String::as_str).collect::<Vec<_>>();
        metadata_keys.sort_unstable();
        formatter
            .debug_struct("Connection")
            .field("id", &self.id)
            .field("namespace", &self.namespace)
            .field("state", &self.state)
            .field("connection_info", &self.connection_info)
            .field("transport_security", &self.transport_security)
            .field("created_at", &self.created_at)
            .field("last_liveness_at", &self.last_liveness_at)
            .field(
                "last_application_activity_at",
                &self.last_application_activity_at,
            )
            .field("metadata_keys", &metadata_keys)
            .finish_non_exhaustive()
    }
}

impl Connection {
    fn identity_expired(&self) -> bool {
        self.identity
            .as_ref()
            .map(ConnectionIdentityHandle::snapshot)
            .and_then(|snapshot| snapshot.expiry())
            .is_some_and(|deadline| deadline <= TokioInstant::now())
    }
}

#[derive(Debug, Clone)]
struct ConnectionAdmissionTarget {
    connection_id: Uuid,
    sender: ConnectionDataSender,
    budget: Arc<OutboundByteBudget>,
}

#[derive(Debug, Clone)]
enum ConnectionTargetFilter {
    Namespace(String),
    Principal(PrincipalTargetKey),
}

impl ConnectionTargetFilter {
    fn matches(&self, connection: &Connection) -> bool {
        match self {
            Self::Namespace(namespace) => connection.namespace == *namespace,
            Self::Principal(expected) => {
                connection.namespace == expected.namespace
                    && connection
                        .identity
                        .as_ref()
                        .map(ConnectionIdentityHandle::snapshot)
                        .and_then(|snapshot| snapshot.principal_id().cloned())
                        .as_ref()
                        == Some(&expected.principal_id)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundAdmissionFailure {
    TimedOut,
    Closed,
    Missing,
    IdentityExpired(WebSocketIdentityRevision),
    TargetChanged,
}

/// Message for broadcasting to multiple connections
#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    /// Target type for the broadcast
    pub target: BroadcastTarget,
    /// Message to broadcast
    pub message: WsMessageBody,
    /// Text and binary share one canonical versioned JSON envelope.
    pub wire_format: WsWireFormat,
    /// Exclude specific connections from broadcast
    pub exclude: Vec<Uuid>,
}

/// Broadcast target rooted in one exact controller namespace.
#[derive(Debug, Clone)]
pub enum BroadcastTarget {
    /// Broadcast to every connection in one namespace.
    Namespace(String),
    /// Broadcast to specific room in namespace
    Room {
        /// Controller namespace containing the room.
        namespace: String,
        /// Room name.
        room: String,
    },
    /// Union of room membership, with each connection targeted once.
    Rooms {
        /// Controller namespace containing the rooms.
        namespace: String,
        /// Room names whose membership is unioned.
        rooms: Vec<String>,
    },
    /// Broadcast to every online connection currently bound to one
    /// application-owned principal in one exact controller namespace.
    Principal {
        /// Controller namespace that owns the outbound event contract.
        namespace: String,
        /// Exact application-owned principal identifier.
        principal_id: PrincipalId,
    },
    /// Broadcast to explicit connection IDs inside one exact namespace.
    /// IDs outside this namespace are accounted as missing targets.
    NamespaceConnections {
        /// Controller namespace that owns this selection.
        namespace: String,
        /// Explicit connection IDs, deduplicated before local admission.
        connection_ids: Vec<Uuid>,
    },
}

impl BroadcastTarget {
    /// Exact namespace that owns this selection, including an empty selection.
    pub fn namespace(&self) -> &str {
        match self {
            Self::Namespace(namespace)
            | Self::Room { namespace, .. }
            | Self::Rooms { namespace, .. }
            | Self::Principal { namespace, .. }
            | Self::NamespaceConnections { namespace, .. } => namespace,
        }
    }
}

/// Per-target terminal accounting for a broadcast attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BroadcastReport {
    /// Unique connection targets selected for this attempt.
    pub targeted: usize,
    /// Targets whose bounded outbound queue accepted the message.
    pub sent: usize,
    /// Targets whose bounded outbound admission deadline elapsed.
    pub backpressured: usize,
    /// Targets whose outbound queue was closed.
    pub closed: usize,
    /// Targets absent from the selected scope, including explicit connection
    /// IDs registered in a different namespace.
    pub missing: usize,
}

/// Result of atomically claiming stale connections for idle-timeout cleanup.
///
/// A failed close-frame enqueue is deliberately reported to the application
/// owner instead of removing manager state here. Registry-owned connections
/// must run their reverse lifecycle cleanup before manager bookkeeping becomes
/// invisible.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct InactiveConnectionCleanupClaim {
    claimed_ids: Vec<Uuid>,
    close_not_queued_ids: Vec<Uuid>,
}

impl InactiveConnectionCleanupClaim {
    pub(crate) fn claimed_ids(&self) -> &[Uuid] {
        &self.claimed_ids
    }

    pub(crate) fn close_not_queued_ids(&self) -> &[Uuid] {
        &self.close_not_queued_ids
    }
}

impl BroadcastReport {
    /// Whether every selected target accepted the message.
    pub fn is_fully_delivered(&self) -> bool {
        self.targeted == self.sent
    }

    /// Whether every target is represented by exactly one terminal outcome.
    pub fn is_balanced(&self) -> bool {
        self.targeted == self.sent + self.backpressured + self.closed + self.missing
    }
}

impl ConnectionManager {
    /// Create new connection manager
    pub fn new() -> Self {
        Self::with_room_limits(128, 128)
    }

    /// Creates a standalone manager with bounded room membership and room-name
    /// limits. A [`crate::WsApp`] normally constructs this automatically.
    pub fn with_room_limits(max_rooms_per_connection: usize, max_room_name_length: usize) -> Self {
        Self::with_registered_namespaces(
            max_rooms_per_connection,
            max_room_name_length,
            std::iter::empty(),
        )
    }

    pub(crate) fn with_registered_namespaces(
        max_rooms_per_connection: usize,
        max_room_name_length: usize,
        namespaces: impl IntoIterator<Item = String>,
    ) -> Self {
        Self::with_registered_namespaces_and_identity(
            max_rooms_per_connection,
            max_room_name_length,
            namespaces,
            false,
        )
    }

    pub(crate) fn with_registered_namespaces_and_identity(
        max_rooms_per_connection: usize,
        max_room_name_length: usize,
        namespaces: impl IntoIterator<Item = String>,
        identity_index_enabled: bool,
    ) -> Self {
        Self::with_registered_namespaces_and_identity_and_outbound_limit(
            max_rooms_per_connection,
            max_room_name_length,
            namespaces,
            identity_index_enabled,
            DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE_BYTES,
        )
    }

    pub(crate) fn with_registered_namespaces_and_identity_and_outbound_limit(
        max_rooms_per_connection: usize,
        max_room_name_length: usize,
        namespaces: impl IntoIterator<Item = String>,
        identity_index_enabled: bool,
        max_outbound_message_size: usize,
    ) -> Self {
        Self::with_registered_namespaces_and_identity_and_outbound_policy(
            max_rooms_per_connection,
            max_room_name_length,
            namespaces,
            identity_index_enabled,
            max_outbound_message_size,
            max_outbound_message_size.max(DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE_BYTES),
            Duration::from_millis(DEFAULT_OUTBOUND_ADMISSION_TIMEOUT_MILLIS),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root supplies independent bounded outbound policy authorities"
    )]
    pub(crate) fn with_registered_namespaces_and_identity_and_outbound_policy(
        max_rooms_per_connection: usize,
        max_room_name_length: usize,
        namespaces: impl IntoIterator<Item = String>,
        identity_index_enabled: bool,
        max_outbound_message_size: usize,
        outbound_queue_max_bytes: usize,
        outbound_admission_timeout: Duration,
    ) -> Self {
        let registry = Arc::new(RwLock::new(ConnectionRegistry::new(identity_index_enabled)));
        let group_manager = Arc::new(GroupManager::with_namespaces(namespaces));

        Self {
            registry,
            group_manager,
            identity_index_enabled,
            max_rooms_per_connection,
            max_room_name_length,
            max_outbound_message_size,
            outbound_queue_max_bytes,
            outbound_admission_timeout,
            metrics: WebSocketServerMetrics::new(),
            #[cfg(test)]
            join_room_registry_read_barrier: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub(crate) const fn max_outbound_message_size(&self) -> usize {
        self.max_outbound_message_size
    }

    pub(crate) const fn max_room_name_length(&self) -> usize {
        self.max_room_name_length
    }

    /// Captures the current bounded server counters.
    pub fn metrics_snapshot(&self) -> WebSocketServerMetricSnapshot {
        self.metrics.snapshot()
    }

    pub(crate) fn metrics(&self) -> &Arc<WebSocketServerMetrics> {
        &self.metrics
    }

    /// Add new connection
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) async fn add_connection_with_control(
        &self,
        connection_id: Uuid,
        data_sender: mpsc::Sender<Message>,
        control_sender: ConnectionControlSender,
        connection_info: RequestConnectionInfo,
        transport_security: WsTransportSecurity,
        namespace: Option<String>,
    ) -> Result<(), ConnectionError> {
        self.add_connection_with_control_and_identity(
            connection_id,
            data_sender,
            control_sender,
            connection_info,
            transport_security,
            namespace,
            None,
        )
        .await
        .map(|_| ())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the internal atomic admission boundary receives each independently owned connection authority explicitly"
    )]
    pub(crate) async fn add_connection_with_control_and_identity<S>(
        &self,
        connection_id: Uuid,
        data_sender: S,
        control_sender: ConnectionControlSender,
        connection_info: RequestConnectionInfo,
        transport_security: WsTransportSecurity,
        namespace: Option<String>,
        identity: Option<ConnectionIdentityHandle>,
    ) -> Result<std::time::SystemTime, ConnectionError>
    where
        S: Into<ConnectionDataSender>,
    {
        let namespace = namespace.ok_or(ConnectionOperationError::MissingNamespace)?;
        if identity.is_some() != self.identity_index_enabled {
            return Err(ConnectionOperationError::IdentityLifecycleUnavailable.into());
        }
        if identity
            .as_ref()
            .map(ConnectionIdentityHandle::snapshot)
            .and_then(|snapshot| snapshot.expiry())
            .is_some_and(|deadline| deadline <= TokioInstant::now())
        {
            return Err(ConnectionOperationError::IdentityExpired.into());
        }

        let now = std::time::SystemTime::now();
        let connection = Connection {
            id: connection_id,
            data_sender: data_sender.into(),
            outbound_budget: OutboundByteBudget::new(self.outbound_queue_max_bytes),
            control_sender,
            metadata: HashMap::new(),
            namespace: namespace.clone(),
            identity,
            state: ConnectionState::Connected,
            connection_info,
            transport_security,
            created_at: now,
            last_liveness_at: now,
            last_application_activity_at: now,
        };

        // A connection UUID identifies one live transport. Keep the manager
        // write lock through namespace admission so concurrent duplicate and
        // remove operations cannot observe or replace a half-admitted entry.
        let mut registry = self.registry.write().await;
        if registry.connections.contains_key(&connection_id) {
            return Err(ConnectionOperationError::DuplicateConnection { connection_id }.into());
        }
        self.group_manager
            .add_connection_to_namespace(&namespace, connection_id)
            .await
            .map_err(ConnectionError::Group)?;
        registry.add_principal_membership(&connection);
        registry.connections.insert(connection_id, connection);

        self.metrics.active_connections.add(1, &[]);
        drop(registry);

        Ok(now)
    }

    #[cfg(test)]
    pub(crate) async fn add_connection(
        &self,
        connection_id: Uuid,
        data_sender: mpsc::Sender<Message>,
        client_ip: Option<String>,
        namespace: Option<String>,
    ) -> Result<(), ConnectionError> {
        let (control_sender, mut control_receiver) = connection_control_channel();
        let control_bridge = data_sender.clone();
        let connection_info = client_ip
            .and_then(|client_ip| client_ip.parse().ok())
            .map(RequestConnectionInfo::direct)
            .unwrap_or_default();
        let identity = self
            .identity_index_enabled
            .then(|| ConnectionIdentityHandle::new(WebSocketIdentitySnapshot::anonymous()));
        let result = self
            .add_connection_with_control_and_identity(
                connection_id,
                data_sender,
                control_sender,
                connection_info,
                WsTransportSecurity::Plaintext,
                namespace,
                identity,
            )
            .await
            .map(|_| ());
        if result.is_ok() {
            tokio::spawn(async move {
                while let Ok(frame) = control_receiver.next().await {
                    let (message, terminal) = match frame {
                        ConnectionControlFrame::Close(request) => {
                            let (message, _) = request.into_parts();
                            (message, true)
                        }
                        ConnectionControlFrame::Protocol(message) => (message, false),
                    };
                    if control_bridge.send(message).await.is_err() || terminal {
                        break;
                    }
                }
            });
        }
        result
    }

    #[cfg(test)]
    pub(crate) async fn add_authenticated_connection(
        &self,
        connection_id: Uuid,
        data_sender: mpsc::Sender<Message>,
        namespace: Option<String>,
        identity: AuthenticatedWebSocketIdentity,
    ) -> Result<(), ConnectionError> {
        let (control_sender, mut control_receiver) = connection_control_channel();
        let control_bridge = data_sender.clone();
        let result = self
            .add_connection_with_control_and_identity(
                connection_id,
                data_sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                namespace,
                Some(ConnectionIdentityHandle::new(
                    WebSocketIdentitySnapshot::authenticated(identity),
                )),
            )
            .await
            .map(|_| ());
        if result.is_ok() {
            tokio::spawn(async move {
                while let Ok(frame) = control_receiver.next().await {
                    let (message, terminal) = match frame {
                        ConnectionControlFrame::Close(request) => {
                            let (message, _) = request.into_parts();
                            (message, true)
                        }
                        ConnectionControlFrame::Protocol(message) => (message, false),
                    };
                    if control_bridge.send(message).await.is_err() || terminal {
                        break;
                    }
                }
            });
        }
        result
    }

    /// Lifecycle-owned registry/index cleanup after connection termination.
    /// Application code requests closure through `WebSocketContext::close`.
    pub(crate) async fn remove_connection(
        &self,
        connection_id: Uuid,
    ) -> Result<(), ConnectionError> {
        let connection = {
            let mut registry = self.registry.write().await;
            let connection = registry.connections.remove(&connection_id);
            if let Some(connection) = connection.as_ref() {
                registry.remove_principal_membership(connection);
            }
            connection
        };

        if let Some(connection) = connection {
            connection.outbound_budget.close();
            self.metrics.active_connections.add(-1, &[]);
            self.metrics.connection_duration.record(
                connection
                    .created_at
                    .elapsed()
                    .unwrap_or_default()
                    .as_secs_f64(),
                &[],
            );
            // Remove from namespace and all rooms
            self.group_manager
                .remove_connection_from_namespace(&connection.namespace, connection_id)
                .await
                .map_err(ConnectionError::Group)?;
        }

        Ok(())
    }

    /// Send to one connection in the exact namespace on this node.
    /// A connection outside this scope is reported as not found.
    pub async fn send_to_connection(
        &self,
        namespace: &str,
        connection_id: Uuid,
        message: WsMessageBody,
    ) -> Result<(), ConnectionError> {
        let frame = self.encode_outbound_application_frame(&message, WsWireFormat::Text)?;
        self.send_application_frame(namespace, connection_id, frame)
            .await
    }

    /// Send the canonical Lily v2 envelope in a WebSocket binary frame.
    /// A connection outside the exact namespace is reported as not found.
    pub async fn send_binary_to_connection(
        &self,
        namespace: &str,
        connection_id: Uuid,
        message: WsMessageBody,
    ) -> Result<(), ConnectionError> {
        let frame = self.encode_outbound_application_frame(&message, WsWireFormat::Binary)?;
        self.send_application_frame(namespace, connection_id, frame)
            .await
    }

    pub(crate) fn encode_outbound_application_frame(
        &self,
        message: &WsMessageBody,
        wire_format: WsWireFormat,
    ) -> Result<Message, ConnectionError> {
        let frame = match wire_format {
            WsWireFormat::Text => message.to_message(),
            WsWireFormat::Binary => message.to_binary_message(),
        }
        .map_err(ConnectionError::Message)?;
        self.validate_outbound_application_frame(&frame)?;
        Ok(frame)
    }

    pub(crate) fn validate_outbound_application_frame(
        &self,
        frame: &Message,
    ) -> Result<(), ConnectionError> {
        if !matches!(frame, Message::Text(_) | Message::Binary(_)) {
            return Err(ConnectionOperationError::InvalidApplicationFrame.into());
        }
        if frame.len() > self.max_outbound_message_size {
            return Err(ConnectionOperationError::OutboundMessageTooLarge.into());
        }
        Ok(())
    }

    /// Enqueue one already encoded Text or Binary application frame.
    ///
    /// Typed action dispatch calls this only after middleware unwind and DI
    /// message-scope cleanup have both succeeded. The bounded queue therefore
    /// remains the single terminal application-frame sink.
    pub(crate) async fn send_application_frame(
        &self,
        namespace: &str,
        connection_id: Uuid,
        frame: Message,
    ) -> Result<(), ConnectionError> {
        self.validate_outbound_application_frame(&frame)?;
        validate_broadcast_namespace(namespace)?;
        let target_filter = ConnectionTargetFilter::Namespace(namespace.to_owned());
        let deadline = TokioInstant::now() + self.outbound_admission_timeout;
        let started = Instant::now();
        let mut frame = frame;
        loop {
            let (target, expired_revision) = {
                let registry = self.registry.read().await;
                let connection = registry
                    .connections
                    .get(&connection_id)
                    .filter(|connection| target_filter.matches(connection))
                    .ok_or(ConnectionError::ConnectionNotFound { connection_id })?;
                if connection.state != ConnectionState::Connected {
                    return Err(ConnectionError::ConnectionClosed { connection_id });
                }

                let expired_revision = connection.identity.as_ref().and_then(|identity| {
                    let snapshot = identity.snapshot();
                    snapshot
                        .expiry()
                        .is_some_and(|deadline| deadline <= TokioInstant::now())
                        .then_some(snapshot.revision())
                });
                (
                    ConnectionAdmissionTarget {
                        connection_id,
                        sender: connection.data_sender.clone(),
                        budget: Arc::clone(&connection.outbound_budget),
                    },
                    expired_revision,
                )
            };

            if let Some(revision) = expired_revision {
                match self
                    .claim_identity_expiry_for_generation(
                        connection_id,
                        revision,
                        Some(&target.budget),
                    )
                    .await?
                {
                    IdentityExpiryClaim::Superseded => continue,
                    IdentityExpiryClaim::Closing(_) => {
                        return Err(ConnectionOperationError::IdentityExpired.into());
                    }
                }
            }
            let depth = || {
                target
                    .sender
                    .max_capacity()
                    .saturating_sub(target.sender.capacity())
            };
            match self
                .admit_application_frame(&target, frame, deadline, &target_filter)
                .await
            {
                Ok(()) => {
                    self.metrics
                        .outbound_admission("accepted", started.elapsed(), depth());
                    return Ok(());
                }
                Err((OutboundAdmissionFailure::TimedOut, _returned)) => {
                    let closed_current = self.close_slow_consumer(&target).await;
                    return if closed_current {
                        self.metrics
                            .outbound_admission("timeout", started.elapsed(), depth());
                        Err(ConnectionError::Backpressure { connection_id })
                    } else {
                        self.metrics
                            .outbound_admission("closed", started.elapsed(), depth());
                        Err(ConnectionError::ConnectionClosed { connection_id })
                    };
                }
                Err((OutboundAdmissionFailure::IdentityExpired(revision), returned)) => {
                    frame = returned;
                    match self
                        .claim_identity_expiry_for_generation(
                            connection_id,
                            revision,
                            Some(&target.budget),
                        )
                        .await?
                    {
                        IdentityExpiryClaim::Superseded => continue,
                        IdentityExpiryClaim::Closing(_) => {
                            self.metrics
                                .outbound_admission("closed", started.elapsed(), depth());
                            return Err(ConnectionOperationError::IdentityExpired.into());
                        }
                    }
                }
                Err((
                    OutboundAdmissionFailure::Missing | OutboundAdmissionFailure::TargetChanged,
                    _,
                )) => {
                    return Err(ConnectionError::ConnectionNotFound { connection_id });
                }
                Err((_, _)) => {
                    self.metrics
                        .outbound_admission("closed", started.elapsed(), depth());
                    return Err(ConnectionError::ConnectionClosed { connection_id });
                }
            }
        }
    }

    async fn admit_application_frame(
        &self,
        target: &ConnectionAdmissionTarget,
        frame: Message,
        deadline: TokioInstant,
        target_filter: &ConnectionTargetFilter,
    ) -> Result<(), (OutboundAdmissionFailure, Message)> {
        self.admit_outbound_application_frame(
            target,
            OutboundApplicationMessage::Owned(frame),
            deadline,
            target_filter,
        )
        .await
        .map_err(|(failure, frame)| (failure, frame.into_message()))
    }

    async fn admit_shared_application_frame(
        &self,
        target: &ConnectionAdmissionTarget,
        frame: Arc<Message>,
        deadline: TokioInstant,
        target_filter: &ConnectionTargetFilter,
    ) -> Result<(), OutboundAdmissionFailure> {
        self.admit_outbound_application_frame(
            target,
            OutboundApplicationMessage::Shared(frame),
            deadline,
            target_filter,
        )
        .await
        .map_err(|(failure, _frame)| failure)
    }

    async fn admit_outbound_application_frame(
        &self,
        target: &ConnectionAdmissionTarget,
        frame: OutboundApplicationMessage,
        deadline: TokioInstant,
        target_filter: &ConnectionTargetFilter,
    ) -> Result<(), (OutboundAdmissionFailure, OutboundApplicationMessage)> {
        let reservation = match target.budget.reserve_until(frame.len(), deadline).await {
            Ok(reservation) => reservation,
            Err(OutboundAdmissionWaitError::TimedOut) => {
                return Err((OutboundAdmissionFailure::TimedOut, frame));
            }
            Err(OutboundAdmissionWaitError::Closed) => {
                return Err((OutboundAdmissionFailure::Closed, frame));
            }
        };
        let permit = match target
            .sender
            .reserve_until(&target.budget.closed, deadline)
            .await
        {
            Ok(permit) => permit,
            Err(OutboundAdmissionWaitError::TimedOut) => {
                return Err((OutboundAdmissionFailure::TimedOut, frame));
            }
            Err(OutboundAdmissionWaitError::Closed) => {
                return Err((OutboundAdmissionFailure::Closed, frame));
            }
        };

        // Capacity waits happen without the registry lock. Revalidate the
        // exact transport generation under a read lock before the queue permit
        // is committed so remove/re-add of the same UUID cannot receive stale
        // work, and every Closing transition remains linearizable.
        let registry = self.registry.read().await;
        let Some(connection) = registry.connections.get(&target.connection_id) else {
            return Err((OutboundAdmissionFailure::Missing, frame));
        };
        if !Arc::ptr_eq(&connection.outbound_budget, &target.budget)
            || connection.state != ConnectionState::Connected
        {
            return Err((OutboundAdmissionFailure::Closed, frame));
        }
        if !target_filter.matches(connection) {
            return Err((OutboundAdmissionFailure::TargetChanged, frame));
        }
        if let Some(identity) = connection.identity.as_ref() {
            let snapshot = identity.snapshot();
            if snapshot
                .expiry()
                .is_some_and(|expiry| expiry <= TokioInstant::now())
            {
                return Err((
                    OutboundAdmissionFailure::IdentityExpired(snapshot.revision()),
                    frame,
                ));
            }
        }
        permit.send(frame, reservation);
        Ok(())
    }

    /// Generation-checked slow-consumer transition. An admission belonging to
    /// a removed transport can never close a replacement that reused its UUID.
    async fn close_slow_consumer(&self, target: &ConnectionAdmissionTarget) -> bool {
        let mut registry = self.registry.write().await;
        let Some(connection) = registry.connections.get(&target.connection_id) else {
            return false;
        };
        if !Arc::ptr_eq(&connection.outbound_budget, &target.budget)
            || connection.state != ConnectionState::Connected
        {
            return false;
        }
        let namespace = connection.namespace.clone();
        let identity = connection.identity.clone();
        let control = connection.control_sender.clone();
        let budget = Arc::clone(&connection.outbound_budget);
        registry.remove_principal_membership_parts(
            &namespace,
            target.connection_id,
            identity.as_ref(),
        );
        registry
            .connections
            .get_mut(&target.connection_id)
            .expect("generation-checked connection remains present under write lock")
            .state = ConnectionState::Closing;
        budget.close();
        let _ = control.request_close(
            Message::Close(Some(crate::request::WsCloseReason::SlowConsumer.frame())),
            WsConnectionCloseCategory::SlowConsumer,
        );
        true
    }

    pub(crate) async fn request_close_frame(
        &self,
        connection_id: Uuid,
        frame: Option<CloseFrame<'static>>,
        category: WsConnectionCloseCategory,
    ) -> Result<CloseRequestOutcome, ConnectionError> {
        let mut registry = self.registry.write().await;
        let (namespace, identity, state, sender, budget) = {
            let connection = registry
                .connections
                .get(&connection_id)
                .ok_or(ConnectionError::ConnectionNotFound { connection_id })?;
            (
                connection.namespace.clone(),
                connection.identity.clone(),
                connection.state.clone(),
                connection.control_sender.clone(),
                Arc::clone(&connection.outbound_budget),
            )
        };

        let identity_expired = identity
            .as_ref()
            .map(ConnectionIdentityHandle::snapshot)
            .and_then(|snapshot| snapshot.expiry())
            .is_some_and(|deadline| deadline <= TokioInstant::now());
        if state == ConnectionState::Connected {
            registry.remove_principal_membership_parts(
                &namespace,
                connection_id,
                identity.as_ref(),
            );
            registry
                .connections
                .get_mut(&connection_id)
                .expect("connection remains present while manager write lock is held")
                .state = ConnectionState::Closing;
        }

        let (message, category) = if identity_expired {
            (
                Message::Close(Some(crate::request::WsCloseReason::IdentityExpired.frame())),
                WsConnectionCloseCategory::IdentityExpired,
            )
        } else {
            (Message::Close(frame), category)
        };
        budget.close();
        sender
            .request_close(message, category)
            .map_err(|()| ConnectionError::ConnectionClosed { connection_id })
    }

    /// Send a non-terminal protocol frame (Ping or Pong) to one client.
    ///
    /// Close producers must use [`Self::request_close_frame`] so the wire frame
    /// and its lifecycle provenance enter the first-writer slot atomically.
    pub(crate) async fn send_raw_to_connection(
        &self,
        connection_id: Uuid,
        message: Message,
    ) -> Result<(), ConnectionError> {
        match message {
            message @ (Message::Ping(_) | Message::Pong(_)) => {
                let registry = self.registry.read().await;
                let connection = registry
                    .connections
                    .get(&connection_id)
                    .ok_or(ConnectionError::ConnectionNotFound { connection_id })?;
                connection
                    .control_sender
                    .send_protocol(message)
                    .map_err(|()| ConnectionError::ConnectionClosed { connection_id })
            }
            Message::Close(_) | Message::Text(_) | Message::Binary(_) | Message::Frame(_) => {
                Err(ConnectionOperationError::InvalidProtocolFrame.into())
            }
        }
    }

    /// Broadcast locally after shared message, target and raw-list validation.
    /// This uses the same input contract as the dispatcher and controller facade.
    /// Backplane publication is deliberately excluded from this operation.
    pub async fn broadcast(
        &self,
        broadcast: BroadcastMessage,
    ) -> Result<BroadcastReport, ConnectionError> {
        self.broadcast_prepared(self.prepare_broadcast(broadcast)?)
            .await
    }

    pub(crate) async fn broadcast_prepared(
        &self,
        prepared: PreparedBroadcast,
    ) -> Result<BroadcastReport, ConnectionError> {
        let report = self
            .handle_broadcast(prepared.command, prepared.frame)
            .await?;
        self.metrics.broadcast_outcomes(&report);
        Ok(report)
    }

    /// Join a connection to a room in the exact namespace.
    pub async fn join_room(
        &self,
        namespace: &str,
        connection_id: Uuid,
        room_name: &str,
    ) -> Result<(), ConnectionError> {
        validate_broadcast_namespace(namespace)?;
        // Keep registry authority through the group mutation. Removal needs the
        // registry write lock, so it either happens before this lookup (and the
        // join is rejected) or after the room insert (and removes that insert).
        let registry = self.registry.read().await;
        registry
            .connections
            .get(&connection_id)
            .filter(|connection| connection.namespace == namespace)
            .ok_or(ConnectionError::ConnectionNotFound { connection_id })?;

        #[cfg(test)]
        let test_barrier = self
            .join_room_registry_read_barrier
            .lock()
            .unwrap()
            .as_ref()
            .filter(|barrier| barrier.connection_id == connection_id)
            .cloned();
        #[cfg(test)]
        if let Some(test_barrier) = test_barrier {
            test_barrier.registry_read.wait().await;
            test_barrier.resume.wait().await;
        }

        let result = self
            .group_manager
            .join_room_bounded(
                namespace,
                room_name,
                connection_id,
                self.max_rooms_per_connection,
                self.max_room_name_length,
            )
            .await
            .map_err(ConnectionError::Group);
        drop(registry);

        result
    }

    /// Leave a room for a connection in the exact namespace.
    pub async fn leave_room(
        &self,
        namespace: &str,
        connection_id: Uuid,
        room_name: &str,
    ) -> Result<(), ConnectionError> {
        validate_broadcast_namespace(namespace)?;
        let registry = self.registry.read().await;
        registry
            .connections
            .get(&connection_id)
            .filter(|connection| connection.namespace == namespace)
            .ok_or(ConnectionError::ConnectionNotFound { connection_id })?;
        let result = self
            .group_manager
            .leave_room(namespace, room_name, connection_id)
            .await
            .map_err(ConnectionError::Group);
        drop(registry);
        result
    }

    /// Get connection info
    pub async fn get_connection(&self, connection_id: Uuid) -> Option<ConnectionInfo> {
        let registry = self.registry.read().await;
        registry
            .connections
            .get(&connection_id)
            .map(|conn| ConnectionInfo {
                id: conn.id,
                namespace: conn.namespace.clone(),
                state: conn.state.clone(),
                connection_info: conn.connection_info,
                transport_security: conn.transport_security,
                created_at: conn.created_at,
                last_liveness_at: conn.last_liveness_at,
                last_application_activity_at: conn.last_application_activity_at,
                metadata: conn.metadata.clone(),
            })
    }

    /// Snapshot the unique connection IDs in this exact namespace on this node.
    /// Order is unspecified and does not represent connection or delivery order.
    /// Callers requiring deterministic presentation can sort the returned IDs.
    pub async fn get_namespace_connections(&self, namespace: &str) -> Vec<Uuid> {
        self.group_manager
            .get_namespace_connections(namespace)
            .await
    }

    /// Snapshot the unique connection IDs in this exact namespace/room on this node.
    /// Order is unspecified; compare membership as a set. A later query may
    /// observe different membership after connections join, leave, or disconnect.
    pub async fn get_room_connections(&self, namespace: &str, room: &str) -> Vec<Uuid> {
        self.group_manager
            .get_room_connections(namespace, room)
            .await
    }

    /// Look up a room's existence and unique member IDs in one node-local snapshot.
    ///
    /// `None` means the exact namespace/room entry is absent. `Some` preserves
    /// an existing entry even if its member list is empty. Normal leave and
    /// lifecycle cleanup delete the room when its last member is removed.
    /// Member order is unspecified; concurrent joins can recreate a deleted room
    /// after this query. This is an exact-key lookup, not target validation.
    pub async fn get_room_snapshot(
        &self,
        namespace: &str,
        room: &str,
    ) -> Option<WebSocketRoomSnapshot> {
        self.group_manager
            .find_room_connections(namespace, room)
            .await
            .map(|connection_ids| WebSocketRoomSnapshot {
                namespace: namespace.to_owned(),
                room: room.to_owned(),
                connection_ids,
            })
    }

    /// Record transport liveness. Heartbeats deliberately do not extend the
    /// application-idle deadline.
    pub(crate) async fn record_liveness(&self, connection_id: Uuid) -> Result<(), ConnectionError> {
        let mut registry = self.registry.write().await;
        if let Some(connection) = registry.connections.get_mut(&connection_id) {
            connection.last_liveness_at = std::time::SystemTime::now();
            Ok(())
        } else {
            Err(ConnectionError::ConnectionNotFound { connection_id })
        }
    }

    /// Record an application Text/Binary message. It is also proof of
    /// transport liveness, so both clocks move together.
    pub(crate) async fn record_application_activity(
        &self,
        connection_id: Uuid,
    ) -> Result<(), ConnectionError> {
        let mut registry = self.registry.write().await;
        if let Some(connection) = registry.connections.get_mut(&connection_id) {
            let now = std::time::SystemTime::now();
            connection.last_liveness_at = now;
            connection.last_application_activity_at = now;
            Ok(())
        } else {
            Err(ConnectionError::ConnectionNotFound { connection_id })
        }
    }

    /// Set metadata for a connection in the exact namespace.
    pub async fn set_connection_metadata(
        &self,
        namespace: &str,
        connection_id: Uuid,
        key: String,
        value: String,
    ) -> Result<(), ConnectionError> {
        validate_broadcast_namespace(namespace)?;
        validate_connection_metadata(&key, &value)?;
        let mut registry = self.registry.write().await;
        if let Some(connection) = registry
            .connections
            .get_mut(&connection_id)
            .filter(|connection| connection.namespace == namespace)
        {
            if !connection.metadata.contains_key(&key)
                && connection.metadata.len() >= MAX_CONNECTION_METADATA_ENTRIES
            {
                return Err(ConnectionMetadataError::TooManyEntries.into());
            }
            connection.metadata.insert(key, value);
            Ok(())
        } else {
            Err(ConnectionError::ConnectionNotFound { connection_id })
        }
    }

    /// Count published connections across every namespace on this node.
    /// Closing connections remain counted until they are removed from the registry.
    pub async fn connection_count(&self) -> usize {
        let registry = self.registry.read().await;
        registry.connections.len()
    }

    /// Use the same publication boundary as the application total, without
    /// depending on namespace/room index cleanup or allocating connection IDs.
    pub(crate) async fn namespace_connection_count(&self, namespace: &str) -> usize {
        let registry = self.registry.read().await;
        registry
            .connections
            .values()
            .filter(|connection| connection.namespace == namespace)
            .count()
    }

    pub(crate) async fn reauthenticate(
        &self,
        connection_id: Uuid,
        expected_revision: WebSocketIdentityRevision,
        replacement: AuthenticatedWebSocketIdentity,
    ) -> Result<WebSocketIdentityUpdateOutcome, ConnectionError> {
        let mut registry = self.registry.write().await;
        let (namespace, identity, state) = {
            let connection = registry
                .connections
                .get(&connection_id)
                .ok_or(ConnectionError::ConnectionNotFound { connection_id })?;
            (
                connection.namespace.clone(),
                connection.identity.clone(),
                connection.state.clone(),
            )
        };
        let identity = identity.ok_or(ConnectionOperationError::IdentityLifecycleUnavailable)?;
        if state != ConnectionState::Connected {
            return Ok(WebSocketIdentityUpdateOutcome::AlreadyClosing);
        }
        let current = identity.snapshot();
        if current.revision() != expected_revision {
            return Ok(WebSocketIdentityUpdateOutcome::Stale(current.revision()));
        }
        if current
            .expiry()
            .is_some_and(|deadline| deadline <= TokioInstant::now())
        {
            return Err(ConnectionOperationError::IdentityExpired.into());
        }
        if replacement
            .expiry()
            .is_some_and(|deadline| deadline <= TokioInstant::now())
        {
            return Err(ConnectionOperationError::IdentityExpired.into());
        }
        let replacement = current
            .with_replacement(replacement)
            .ok_or(ConnectionOperationError::IdentityRevisionExhausted)?;

        registry.remove_principal_membership_parts(&namespace, connection_id, Some(&identity));
        identity.publish(replacement.clone());
        registry.add_principal_membership_parts(&namespace, connection_id, Some(&identity));

        Ok(WebSocketIdentityUpdateOutcome::Updated(
            replacement.revision(),
        ))
    }

    pub(crate) async fn claim_identity_expiry(
        &self,
        connection_id: Uuid,
        revision: WebSocketIdentityRevision,
    ) -> Result<IdentityExpiryClaim, ConnectionError> {
        self.claim_identity_expiry_for_generation(connection_id, revision, None)
            .await
    }

    async fn claim_identity_expiry_for_generation(
        &self,
        connection_id: Uuid,
        revision: WebSocketIdentityRevision,
        expected_budget: Option<&Arc<OutboundByteBudget>>,
    ) -> Result<IdentityExpiryClaim, ConnectionError> {
        let mut registry = self.registry.write().await;
        let (namespace, identity, state, sender, budget) = {
            let Some(connection) = registry.connections.get(&connection_id) else {
                return Err(if expected_budget.is_some() {
                    ConnectionError::ConnectionClosed { connection_id }
                } else {
                    ConnectionError::ConnectionNotFound { connection_id }
                });
            };
            if expected_budget
                .is_some_and(|expected| !Arc::ptr_eq(&connection.outbound_budget, expected))
            {
                return Err(ConnectionError::ConnectionClosed { connection_id });
            }
            (
                connection.namespace.clone(),
                connection.identity.clone(),
                connection.state.clone(),
                connection.control_sender.clone(),
                Arc::clone(&connection.outbound_budget),
            )
        };
        let identity = identity.ok_or(ConnectionOperationError::IdentityLifecycleUnavailable)?;
        let current = identity.snapshot();
        let still_expired = current.revision() == revision
            && current
                .expiry()
                .is_some_and(|deadline| deadline <= TokioInstant::now());
        if !still_expired {
            return Ok(IdentityExpiryClaim::Superseded);
        }

        registry.remove_principal_membership_parts(&namespace, connection_id, Some(&identity));
        if state == ConnectionState::Connected {
            registry
                .connections
                .get_mut(&connection_id)
                .expect("connection remains present under manager write lock")
                .state = ConnectionState::Closing;
        }
        budget.close();
        let outcome = sender
            .request_close(
                Message::Close(Some(crate::request::WsCloseReason::IdentityExpired.frame())),
                WsConnectionCloseCategory::IdentityExpired,
            )
            .map_err(|()| ConnectionError::ConnectionClosed { connection_id })?;
        Ok(IdentityExpiryClaim::Closing(outcome))
    }

    pub(crate) async fn expired_identity_revision(
        &self,
        connection_id: Uuid,
    ) -> Result<Option<WebSocketIdentityRevision>, ConnectionError> {
        let registry = self.registry.read().await;
        let connection = registry
            .connections
            .get(&connection_id)
            .ok_or(ConnectionError::ConnectionNotFound { connection_id })?;
        Ok(connection.identity.as_ref().and_then(|identity| {
            let snapshot = identity.snapshot();
            snapshot
                .expiry()
                .is_some_and(|deadline| deadline <= TokioInstant::now())
                .then_some(snapshot.revision())
        }))
    }

    /// Atomically stop one connection from accepting application messages.
    ///
    /// Returns `true` only for the transition into `Closing`; repeated calls
    /// are successful no-ops so competing shutdown paths remain idempotent.
    pub(crate) async fn mark_closing(&self, connection_id: Uuid) -> Result<bool, ConnectionError> {
        let mut registry = self.registry.write().await;
        let (namespace, identity, state, budget) = {
            let connection = registry
                .connections
                .get(&connection_id)
                .ok_or(ConnectionError::ConnectionNotFound { connection_id })?;
            (
                connection.namespace.clone(),
                connection.identity.clone(),
                connection.state.clone(),
                Arc::clone(&connection.outbound_budget),
            )
        };
        if matches!(state, ConnectionState::Closing | ConnectionState::Closed) {
            return Ok(false);
        }

        registry.remove_principal_membership_parts(&namespace, connection_id, identity.as_ref());
        registry
            .connections
            .get_mut(&connection_id)
            .expect("connection remains present while manager write lock is held")
            .state = ConnectionState::Closing;
        budget.close();
        Ok(true)
    }

    /// Remove any bookkeeping left by connection tasks that had to be aborted
    /// at the application shutdown deadline.
    pub(crate) async fn remove_all_connections(&self) -> usize {
        let connection_ids = {
            let registry = self.registry.read().await;
            registry.connections.keys().copied().collect::<Vec<_>>()
        };
        let count = connection_ids.len();
        for connection_id in connection_ids {
            if let Err(error) = self.remove_connection(connection_id).await {
                tracing::warn!(%connection_id, %error, "WebSocket shutdown cleanup failed");
            }
        }
        count
    }

    /// Atomically claim stale connections without making manager state
    /// invisible when close-frame delivery fails.
    pub(crate) async fn cleanup_inactive_connections(
        &self,
        timeout: std::time::Duration,
    ) -> InactiveConnectionCleanupClaim {
        let now = std::time::SystemTime::now();
        let stale_connections = {
            let mut registry = self.registry.write().await;
            let stale_ids = registry
                .connections
                .iter()
                .filter_map(|(connection_id, connection)| {
                    let is_stale = connection.state == ConnectionState::Connected
                        && now
                            .duration_since(connection.last_application_activity_at)
                            .is_ok_and(|elapsed| elapsed > timeout);
                    if !is_stale {
                        return None;
                    }
                    Some(*connection_id)
                })
                .collect::<Vec<_>>();
            stale_ids
                .into_iter()
                .map(|connection_id| {
                    let (namespace, identity, sender, budget) = {
                        let connection = registry
                            .connections
                            .get(&connection_id)
                            .expect("stale connection remains present under manager write lock");
                        (
                            connection.namespace.clone(),
                            connection.identity.clone(),
                            connection.control_sender.clone(),
                            Arc::clone(&connection.outbound_budget),
                        )
                    };
                    registry.remove_principal_membership_parts(
                        &namespace,
                        connection_id,
                        identity.as_ref(),
                    );
                    registry
                        .connections
                        .get_mut(&connection_id)
                        .expect("stale connection remains present under manager write lock")
                        .state = ConnectionState::Closing;
                    budget.close();
                    (connection_id, sender)
                })
                .collect::<Vec<_>>()
        };

        let mut claim = InactiveConnectionCleanupClaim {
            claimed_ids: Vec::with_capacity(stale_connections.len()),
            close_not_queued_ids: Vec::new(),
        };
        for (connection_id, sender) in stale_connections {
            claim.claimed_ids.push(connection_id);
            if sender
                .request_close(
                    Message::Close(Some(crate::request::WsCloseReason::IdleTimeout.frame())),
                    WsConnectionCloseCategory::IdleTimeout,
                )
                .is_err()
            {
                claim.close_not_queued_ids.push(connection_id);
            }
        }

        claim
    }

    /// Handle broadcast message
    async fn handle_broadcast(
        &self,
        broadcast: BroadcastMessage,
        ws_message: Message,
    ) -> Result<BroadcastReport, ConnectionError> {
        let BroadcastMessage {
            target,
            mut exclude,
            ..
        } = broadcast;
        let target_filter = match &target {
            BroadcastTarget::Principal {
                namespace,
                principal_id,
            } => ConnectionTargetFilter::Principal(PrincipalTargetKey {
                namespace: namespace.clone(),
                principal_id: principal_id.clone(),
            }),
            _ => ConnectionTargetFilter::Namespace(target.namespace().to_owned()),
        };
        let mut target_connections = match target {
            BroadcastTarget::Namespace(namespace) => {
                self.group_manager
                    .get_namespace_connections(&namespace)
                    .await
            }
            BroadcastTarget::Room { namespace, room } => {
                self.group_manager
                    .get_room_connections(&namespace, &room)
                    .await
            }
            BroadcastTarget::Rooms { namespace, rooms } => {
                let mut unique = HashSet::new();
                for room in &rooms {
                    unique.extend(
                        self.group_manager
                            .get_room_connections(&namespace, room)
                            .await,
                    );
                }
                unique.into_iter().collect()
            }
            BroadcastTarget::Principal {
                namespace,
                principal_id,
            } => {
                let key = PrincipalTargetKey {
                    namespace,
                    principal_id,
                };
                let registry = self.registry.read().await;
                let connection_ids = registry
                    .principal_index
                    .as_ref()
                    .ok_or(ConnectionOperationError::IdentityLifecycleUnavailable)?
                    .get(&key)
                    .map(|connection_ids| connection_ids.iter().copied().collect())
                    .unwrap_or_default();
                connection_ids
            }
            BroadcastTarget::NamespaceConnections { connection_ids, .. } => connection_ids,
        };
        target_connections.sort_unstable();
        target_connections.dedup();
        exclude.sort_unstable();
        exclude.dedup();

        let mut report = BroadcastReport {
            targeted: target_connections
                .iter()
                .filter(|connection_id| exclude.binary_search(connection_id).is_err())
                .count(),
            ..Default::default()
        };
        let mut targets = Vec::new();
        {
            let registry = self.registry.read().await;
            for connection_id in target_connections {
                if exclude.binary_search(&connection_id).is_ok() {
                    continue;
                }
                let Some(connection) = registry.connections.get(&connection_id) else {
                    report.missing += 1;
                    continue;
                };
                if !target_filter.matches(connection) {
                    report.missing += 1;
                    continue;
                }
                if connection.state != ConnectionState::Connected || connection.identity_expired() {
                    report.closed += 1;
                    continue;
                }
                targets.push(ConnectionAdmissionTarget {
                    connection_id,
                    sender: connection.data_sender.clone(),
                    budget: Arc::clone(&connection.outbound_budget),
                });
            }
        }

        let deadline = TokioInstant::now() + self.outbound_admission_timeout;
        let ws_message = Arc::new(ws_message);
        let mut admissions = FuturesUnordered::new();
        for target in targets {
            let manager = self.clone();
            let frame = Arc::clone(&ws_message);
            let expected = target_filter.clone();
            admissions.push(async move {
                let result = manager
                    .admit_shared_application_frame(&target, frame, deadline, &expected)
                    .await;
                (manager, target, result)
            });
        }
        // The per-target futures and admitted queues now own every required
        // reference. Do not keep an extra broadcast-scope reference alive:
        // a single-recipient writer can then move the message out of its Arc,
        // while multi-recipient writers clone only when ownership requires it.
        drop(ws_message);
        while let Some((manager, target, result)) = admissions.next().await {
            match result {
                Ok(()) => report.sent += 1,
                Err(OutboundAdmissionFailure::TimedOut) => {
                    if manager.close_slow_consumer(&target).await {
                        report.backpressured += 1;
                    } else {
                        report.closed += 1;
                    }
                }
                Err(OutboundAdmissionFailure::Missing)
                | Err(OutboundAdmissionFailure::TargetChanged) => report.missing += 1,
                Err(OutboundAdmissionFailure::Closed) => report.closed += 1,
                Err(OutboundAdmissionFailure::IdentityExpired(revision)) => {
                    let _ = manager
                        .claim_identity_expiry_for_generation(
                            target.connection_id,
                            revision,
                            Some(&target.budget),
                        )
                        .await;
                    report.closed += 1;
                }
            }
        }
        debug_assert!(report.is_balanced());
        Ok(report)
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// An existing room and its unique, unordered members on this node.
///
/// Existence and membership are captured under the same room-index read lock.
/// The snapshot does not promise that any member remains connected afterward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSocketRoomSnapshot {
    /// Namespace containing the room.
    pub namespace: String,
    /// Exact room name.
    pub room: String,
    /// Unique member IDs in unspecified order.
    pub connection_ids: Vec<Uuid>,
}

/// Connection information for external queries
#[derive(Clone)]
pub struct ConnectionInfo {
    /// Stable connection identifier.
    pub id: Uuid,
    /// Controller namespace selected during the handshake.
    pub namespace: String,
    /// Current lifecycle state.
    pub state: ConnectionState,
    /// Typed socket-peer and effective-client identity.
    pub connection_info: RequestConnectionInfo,
    /// Server-owned WS/WSS transport classification.
    pub transport_security: WsTransportSecurity,
    /// Time at which the manager admitted the connection.
    pub created_at: std::time::SystemTime,
    /// Latest transport liveness observation, including Ping/Pong.
    pub last_liveness_at: std::time::SystemTime,
    /// Latest inbound Text or Binary application frame.
    pub last_application_activity_at: std::time::SystemTime,
    /// Application metadata attached to the connection.
    pub metadata: HashMap<String, String>,
}

impl ConnectionInfo {
    /// Effective client address after trusted-proxy processing.
    pub fn client_ip(&self) -> Option<std::net::IpAddr> {
        self.connection_info.client_ip()
    }

    /// Socket peer before trusted-proxy processing.
    pub fn peer_ip(&self) -> Option<std::net::IpAddr> {
        self.connection_info.peer_ip()
    }

    /// Whether an explicitly trusted proxy supplied the effective client IP.
    pub fn via_trusted_proxy(&self) -> bool {
        self.connection_info.via_trusted_proxy()
    }

    /// Whether this connection uses Lily-terminated TLS.
    pub const fn is_secure(&self) -> bool {
        self.transport_security.is_secure()
    }
}

impl std::fmt::Debug for ConnectionInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut metadata_keys = self.metadata.keys().map(String::as_str).collect::<Vec<_>>();
        metadata_keys.sort_unstable();
        formatter
            .debug_struct("ConnectionInfo")
            .field("id", &self.id)
            .field("namespace", &self.namespace)
            .field("state", &self.state)
            .field("connection_info", &self.connection_info)
            .field("transport_security", &self.transport_security)
            .field("created_at", &self.created_at)
            .field("last_liveness_at", &self.last_liveness_at)
            .field(
                "last_application_activity_at",
                &self.last_application_activity_at,
            )
            .field("metadata_keys", &metadata_keys)
            .finish()
    }
}

/// Admission failure for application metadata retained by one connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectionMetadataError {
    /// A key is empty, contains a control byte, or exceeds its byte bound.
    #[error("WebSocket connection metadata key is invalid")]
    InvalidKey,
    /// A value contains NUL or exceeds its byte bound.
    #[error("WebSocket connection metadata value is invalid")]
    InvalidValue,
    /// The connection already contains the maximum number of distinct keys.
    #[error("WebSocket connection metadata entry limit was reached")]
    TooManyEntries,
}

/// Invalid operation requested from the connection manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectionOperationError {
    /// Admission did not provide one exact registered namespace.
    #[error("an exact registered controller namespace is required")]
    MissingNamespace,
    /// A live transport already owns the requested connection identifier.
    #[error("connection already exists: {connection_id}")]
    DuplicateConnection {
        /// Duplicate live connection identifier.
        connection_id: Uuid,
    },
    /// Application delivery accepts only Text and Binary frames.
    #[error("application frame must be Text or Binary")]
    InvalidApplicationFrame,
    /// The raw protocol path accepts only Close, Ping and Pong frames.
    #[error("protocol frame must be Close, Ping or Pong")]
    InvalidProtocolFrame,
    /// The app-scoped dispatcher is draining or has completed terminal shutdown.
    #[error("WebSocket dispatcher is not accepting new work")]
    DispatcherNotAccepting,
    /// A broadcast namespace or room target violated its canonical format contract.
    /// The historical variant name also applies to node-local broadcasts.
    #[error("WebSocket backplane target is invalid")]
    InvalidBackplaneTarget,
    /// A broadcast target list exceeded its fixed raw-input cardinality bound.
    #[error("WebSocket backplane target exceeds its cardinality limit")]
    BackplaneTargetLimitExceeded,
    /// A broadcast exclusion list exceeded its fixed raw-input cardinality bound.
    #[error("WebSocket backplane exclusions exceed their cardinality limit")]
    BackplaneExclusionLimitExceeded,
    /// Principal lifecycle APIs were used without the application identity
    /// middleware that owns the corresponding bounded index.
    #[error("WebSocket identity lifecycle is not enabled for this application")]
    IdentityLifecycleUnavailable,
    /// The supplied identity deadline had already elapsed.
    #[error("WebSocket identity has already expired")]
    IdentityExpired,
    /// The opaque compare-and-replace identity revision cannot advance.
    #[error("WebSocket identity revision space is exhausted")]
    IdentityRevisionExhausted,
    /// The serialized outbound message exceeded the effective outbound message limit.
    #[error("WebSocket outbound message exceeds the configured size limit")]
    OutboundMessageTooLarge,
}

fn validate_connection_metadata(key: &str, value: &str) -> Result<(), ConnectionMetadataError> {
    if key.is_empty()
        || key.len() > MAX_CONNECTION_METADATA_KEY_BYTES
        || key.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(ConnectionMetadataError::InvalidKey);
    }
    if value.len() > MAX_CONNECTION_METADATA_VALUE_BYTES || value.bytes().any(|byte| byte == 0) {
        return Err(ConnectionMetadataError::InvalidValue);
    }
    Ok(())
}

/// Connection management errors
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// The requested connection is not registered.
    #[error("Connection not found: {connection_id}")]
    ConnectionNotFound {
        /// Missing connection identifier.
        connection_id: Uuid,
    },
    /// The connection's outbound queue is closed.
    #[error("Connection closed: {connection_id}")]
    ConnectionClosed {
        /// Closed connection identifier.
        connection_id: Uuid,
    },
    /// The bounded outbound admission deadline elapsed.
    #[error("Connection outbound admission timed out: {connection_id}")]
    Backpressure {
        /// Backpressured connection identifier.
        connection_id: Uuid,
    },
    /// Connection metadata failed its fixed admission contract.
    #[error(transparent)]
    Metadata(#[from] ConnectionMetadataError),
    /// Public-room membership operation failed.
    #[error(transparent)]
    Group(#[from] GroupError),
    /// Envelope construction or transport conversion failed.
    #[error(transparent)]
    Message(#[from] crate::request::WsBodyError),
    /// A shared broadcast command channel is closed.
    #[error("Broadcast channel closed")]
    BroadcastChannelClosed,
    /// The requested operation violates connection state or policy.
    #[error(transparent)]
    InvalidOperation(#[from] ConnectionOperationError),
    /// Shutdown or invocation expiry interrupted node-local queue admission.
    /// Some targets may already have accepted the message; their final report
    /// is unavailable. Backplane publish has not started. Do not blindly retry.
    #[error("WebSocket local dispatch was interrupted; partial local acceptance is possible")]
    DispatchInterrupted,
    /// A broadcast payload could not be serialized.
    #[error("WebSocket payload serialization failed")]
    Serialization(#[source] serde_json::Error),
    /// At least one selected target did not accept a broadcast.
    #[error("Broadcast was only partially delivered: {report:?}")]
    PartialBroadcast {
        /// Per-target terminal accounting.
        report: BroadcastReport,
    },
    /// A configured optional backplane was unavailable after local delivery.
    #[error("WebSocket backplane is unavailable after local delivery: {local:?}")]
    BackplaneUnavailable {
        /// Node-local terminal accounting retained for the caller.
        local: BroadcastReport,
    },
    /// The configured backplane rejected the publish after local delivery.
    #[error("WebSocket backplane publish failed after local delivery: {local:?}")]
    BackplanePublish {
        /// Node-local terminal accounting retained for the caller.
        local: BroadcastReport,
        /// Provider failure returned by the selected backplane.
        #[source]
        source: crate::WebSocketBackplaneError,
    },
}

#[cfg(test)]
#[path = "connection/local_delivery_qualification_tests.rs"]
mod local_delivery_qualification_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_connection_metric_uses_the_same_series_for_increment_and_decrement() {
        let source = include_str!("connection.rs");
        assert!(source.contains("active_connections.add(1, &[]);"));
        assert!(source.contains("active_connections.add(-1, &[]);"));
    }

    #[test]
    fn backplane_metric_snapshot_preserves_every_bounded_outcome() {
        let manager = test_manager();
        let metrics = manager.metrics();

        metrics.backplane_publish_accepted();
        metrics.backplane_publish_saturated();
        metrics.backplane_publish_unavailable();
        metrics.backplane_publish_failed();
        metrics.backplane_invalid_frame();
        metrics.backplane_local_dispatch_failed();
        metrics.backplane_duplicate_suppressed();
        metrics.backplane_origin_loop_suppressed();

        let snapshot = manager.metrics_snapshot();
        assert_eq!(snapshot.backplane_publish_accepted, 1);
        assert_eq!(snapshot.backplane_publish_saturated, 1);
        assert_eq!(snapshot.backplane_publish_unavailable, 1);
        assert_eq!(snapshot.backplane_publish_failed, 1);
        assert_eq!(snapshot.backplane_invalid_frames, 1);
        assert_eq!(snapshot.backplane_local_dispatch_failed, 1);
        assert_eq!(snapshot.backplane_duplicates_suppressed, 1);
        assert_eq!(snapshot.backplane_origin_loops_suppressed, 1);
    }

    use serde_json::json;
    use tokio::time::{Duration, timeout};

    fn application_message() -> WsMessageBody {
        WsMessageBody::try_new("orders:created", json!({ "id": "42" })).unwrap()
    }

    fn test_manager() -> ConnectionManager {
        ConnectionManager::with_registered_namespaces(128, 128, ["test".into()])
    }

    fn outbound_limited_manager(max_outbound_message_size: usize) -> ConnectionManager {
        ConnectionManager::with_registered_namespaces_and_identity_and_outbound_limit(
            128,
            128,
            ["test".into()],
            false,
            max_outbound_message_size,
        )
    }

    fn outbound_policy_manager(
        max_outbound_message_size: usize,
        outbound_queue_max_bytes: usize,
        outbound_admission_timeout: Duration,
    ) -> ConnectionManager {
        ConnectionManager::with_registered_namespaces_and_identity_and_outbound_policy(
            128,
            128,
            ["test".into()],
            false,
            max_outbound_message_size,
            outbound_queue_max_bytes,
            outbound_admission_timeout,
        )
    }

    async fn add_managed_connection(
        manager: &ConnectionManager,
        connection_id: Uuid,
        queue_capacity: usize,
    ) -> (
        mpsc::Receiver<QueuedApplicationFrame>,
        ConnectionControlReceiver,
        Arc<OutboundByteBudget>,
    ) {
        let (data_sender, data_receiver) = mpsc::channel(queue_capacity);
        let (control_sender, control_receiver) = connection_control_channel();
        let created_at = manager
            .add_connection_with_control_and_identity(
                connection_id,
                data_sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("test".into()),
                None,
            )
            .await
            .unwrap();
        let registry = manager.registry.read().await;
        let connection = registry
            .connections
            .get(&connection_id)
            .expect("managed test connection must be registered");
        assert_eq!(connection.created_at, created_at);
        let budget = Arc::clone(&connection.outbound_budget);
        (data_receiver, control_receiver, budget)
    }

    fn assert_outbound_too_large(error: ConnectionError) {
        assert!(matches!(
            error,
            ConnectionError::InvalidOperation(ConnectionOperationError::OutboundMessageTooLarge)
        ));
    }

    fn identity_manager() -> ConnectionManager {
        ConnectionManager::with_registered_namespaces_and_identity(
            128,
            128,
            ["orders".into(), "billing".into()],
            true,
        )
    }

    fn authenticated_identity(subject: &str) -> AuthenticatedWebSocketIdentity {
        AuthenticatedWebSocketIdentity::try_new(Principal::new(
            subject,
            Vec::<String>::new(),
            Vec::<String>::new(),
            serde_json::Map::new(),
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn application_frame_sink_enforces_text_and_binary_exact_boundaries() {
        const LIMIT: usize = 8;

        let manager = outbound_limited_manager(LIMIT);
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(8);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();

        for binary in [false, true] {
            for size in [LIMIT - 1, LIMIT] {
                let frame = if binary {
                    Message::Binary(vec![b'x'; size])
                } else {
                    Message::Text("x".repeat(size))
                };
                manager
                    .send_application_frame("test", connection_id, frame)
                    .await
                    .unwrap();
                let admitted = timeout(Duration::from_secs(1), receiver.recv())
                    .await
                    .expect("exact application frame must be admitted within the test deadline")
                    .expect("application data channel remains open");
                assert_eq!(admitted.len(), size);
                assert_eq!(matches!(admitted, Message::Binary(_)), binary);
            }

            let oversized = if binary {
                Message::Binary(vec![b'x'; LIMIT + 1])
            } else {
                Message::Text("x".repeat(LIMIT + 1))
            };
            assert_outbound_too_large(
                manager
                    .send_application_frame("test", connection_id, oversized)
                    .await
                    .unwrap_err(),
            );
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }

        manager
            .send_application_frame("test", connection_id, Message::Text("éééé".into()))
            .await
            .expect("four two-byte UTF-8 scalars fit the eight-byte limit");
        let utf8 = timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("UTF-8 boundary frame must be admitted within the test deadline")
            .expect("application data channel remains open");
        assert_eq!(utf8.len(), LIMIT);
        assert_outbound_too_large(
            manager
                .send_application_frame("test", connection_id, Message::Text("ééééx".into()))
                .await
                .unwrap_err(),
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        assert_eq!(manager.metrics_snapshot().outbound_messages, 5);
    }

    #[tokio::test]
    async fn managed_outbound_budget_waits_at_n_plus_one_and_releases_on_frame_drop() {
        const BYTE_BUDGET: usize = 8;

        let manager = outbound_policy_manager(BYTE_BUDGET, BYTE_BUDGET, Duration::from_secs(1));
        let connection_id = Uuid::new_v4();
        let (mut receiver, _control_receiver, budget) =
            add_managed_connection(&manager, connection_id, 2).await;

        manager
            .send_application_frame(
                "test",
                connection_id,
                Message::Text("x".repeat(BYTE_BUDGET)),
            )
            .await
            .expect("a frame exactly equal to the byte budget must be admitted");
        let in_flight = receiver
            .recv()
            .await
            .expect("the exact-budget frame must reach the managed writer queue");
        assert_eq!(in_flight.message().len(), BYTE_BUDGET);
        assert_eq!(budget.used_bytes(), BYTE_BUDGET);

        let waiting_manager = manager.clone();
        let waiting_send = tokio::spawn(async move {
            waiting_manager
                .send_application_frame("test", connection_id, Message::Text("y".into()))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !waiting_send.is_finished(),
            "an aggregate N+1 bytes must wait while the N-byte frame is in flight"
        );

        drop(in_flight);
        timeout(Duration::from_secs(1), waiting_send)
            .await
            .expect("dropping the in-flight frame must wake byte admission")
            .expect("the waiting send task must not panic")
            .expect("the waiting byte must be admitted before its deadline");
        let admitted = receiver
            .recv()
            .await
            .expect("the woken admission must enqueue its managed frame");
        assert_eq!(admitted.message(), &Message::Text("y".into()));
        assert_eq!(budget.used_bytes(), 1);

        drop(admitted);
        assert_eq!(
            budget.used_bytes(),
            0,
            "dropping the final queued frame must release its reservation exactly once"
        );
    }

    #[tokio::test]
    async fn count_full_admission_waits_then_succeeds_when_the_queue_drains() {
        const BYTE_BUDGET: usize = 8;

        let manager = outbound_policy_manager(BYTE_BUDGET, BYTE_BUDGET, Duration::from_secs(1));
        let connection_id = Uuid::new_v4();
        let (mut receiver, _control_receiver, budget) =
            add_managed_connection(&manager, connection_id, 1).await;

        manager
            .send_application_frame("test", connection_id, Message::Text("a".into()))
            .await
            .unwrap();
        let waiting_manager = manager.clone();
        let waiting_send = tokio::spawn(async move {
            waiting_manager
                .send_application_frame("test", connection_id, Message::Text("b".into()))
                .await
        });
        timeout(Duration::from_secs(1), async {
            while budget.used_bytes() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second frame must reserve bytes before waiting for count capacity");
        assert!(!waiting_send.is_finished());
        assert_eq!(
            budget.used_bytes(),
            2,
            "a frame waiting only for a count slot remains byte-accounted"
        );

        let first = receiver.recv().await.unwrap();
        timeout(Duration::from_secs(1), waiting_send)
            .await
            .expect("queue drain must wake count admission")
            .unwrap()
            .expect("the deadline must not reject a recovered queue");
        let second = receiver.recv().await.unwrap();
        assert_eq!(first.message(), &Message::Text("a".into()));
        assert_eq!(second.message(), &Message::Text("b".into()));
        assert_eq!(budget.used_bytes(), 2);

        drop((first, second));
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Connected
        );
    }

    #[tokio::test]
    async fn broadcast_admission_retains_one_shared_payload_across_target_queues() {
        const BYTES: usize = 8;

        let manager = outbound_policy_manager(BYTES, BYTES, Duration::from_secs(1));
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        let (mut first_receiver, _first_control, first_budget) =
            add_managed_connection(&manager, first_id, 1).await;
        let (mut second_receiver, _second_control, second_budget) =
            add_managed_connection(&manager, second_id, 1).await;
        let (first_target, second_target) = {
            let registry = manager.registry.read().await;
            let target = |connection_id| {
                let connection = registry.connections.get(&connection_id).unwrap();
                ConnectionAdmissionTarget {
                    connection_id,
                    sender: connection.data_sender.clone(),
                    budget: Arc::clone(&connection.outbound_budget),
                }
            };
            (target(first_id), target(second_id))
        };
        let shared = Arc::new(Message::Binary(vec![7; BYTES]));
        let deadline = TokioInstant::now() + Duration::from_secs(1);

        let filter = ConnectionTargetFilter::Namespace("test".to_owned());
        let (first, second) = tokio::join!(
            manager.admit_shared_application_frame(
                &first_target,
                Arc::clone(&shared),
                deadline,
                &filter
            ),
            manager.admit_shared_application_frame(
                &second_target,
                Arc::clone(&shared),
                deadline,
                &filter
            ),
        );
        first.unwrap();
        second.unwrap();
        let first_frame = first_receiver.recv().await.unwrap();
        let second_frame = second_receiver.recv().await.unwrap();
        assert!(first_frame.shares_message_with(&shared));
        assert!(second_frame.shares_message_with(&shared));
        assert_eq!(first_budget.used_bytes(), BYTES);
        assert_eq!(second_budget.used_bytes(), BYTES);

        drop((first_frame, second_frame));
        assert_eq!(first_budget.used_bytes(), 0);
        assert_eq!(second_budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn public_broadcast_queues_one_shared_payload_generation_for_all_targets() {
        let message = application_message();
        let expected_frame = message.to_message().unwrap();
        let frame_size = expected_frame.len();
        let manager = outbound_policy_manager(frame_size, frame_size, Duration::from_secs(1));
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        let (mut first_receiver, _first_control, first_budget) =
            add_managed_connection(&manager, first_id, 1).await;
        let (mut second_receiver, _second_control, second_budget) =
            add_managed_connection(&manager, second_id, 1).await;

        let report = manager
            .broadcast(BroadcastMessage {
                target: BroadcastTarget::NamespaceConnections {
                    namespace: "test".to_owned(),
                    connection_ids: vec![first_id, second_id],
                },
                message,
                wire_format: WsWireFormat::Text,
                exclude: Vec::new(),
            })
            .await
            .expect("the public broadcast path must admit both managed targets");
        assert_eq!(
            report,
            BroadcastReport {
                targeted: 2,
                sent: 2,
                ..Default::default()
            }
        );

        let first_frame = first_receiver
            .recv()
            .await
            .expect("the first managed target must retain the broadcast frame");
        let second_frame = second_receiver
            .recv()
            .await
            .expect("the second managed target must retain the broadcast frame");
        assert_eq!(first_frame.message(), &expected_frame);
        assert_eq!(second_frame.message(), &expected_frame);
        assert!(
            first_frame.shares_message_with_frame(&second_frame),
            "public fan-out must queue one shared payload allocation, not one deep clone per target"
        );
        assert_eq!(first_budget.used_bytes(), frame_size);
        assert_eq!(second_budget.used_bytes(), frame_size);

        drop((first_frame, second_frame));
        assert_eq!(first_budget.used_bytes(), 0);
        assert_eq!(second_budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn removal_cancels_pending_admission_without_locking_or_reaching_a_replacement() {
        const BYTE_BUDGET: usize = 8;

        let manager = outbound_policy_manager(BYTE_BUDGET, BYTE_BUDGET, Duration::from_secs(1));
        let connection_id = Uuid::new_v4();
        let (old_receiver, _old_control, old_budget) =
            add_managed_connection(&manager, connection_id, 1).await;
        manager
            .send_application_frame("test", connection_id, Message::Text("xxxx".into()))
            .await
            .unwrap();

        let pending_manager = manager.clone();
        let pending = tokio::spawn(async move {
            pending_manager
                .send_application_frame("test", connection_id, Message::Text("y".into()))
                .await
        });
        timeout(Duration::from_secs(1), async {
            while old_budget.used_bytes() != 5 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending admission must own its byte reservation before removal");
        assert!(!pending.is_finished());

        timeout(
            Duration::from_millis(100),
            manager.remove_connection(connection_id),
        )
        .await
        .expect("pending admission must not retain the registry lock")
        .unwrap();
        assert!(matches!(
            timeout(Duration::from_millis(100), pending)
                .await
                .expect("removal must cancel the pending admission")
                .unwrap(),
            Err(ConnectionError::ConnectionClosed { connection_id: closed })
                if closed == connection_id
        ));

        let (mut replacement, replacement_control, replacement_budget) =
            add_managed_connection(&manager, connection_id, 1).await;
        assert!(replacement_control.close.borrow().is_none());
        assert!(!replacement_budget.is_closed());
        assert!(matches!(
            replacement.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        manager
            .send_application_frame("test", connection_id, Message::Text("z".into()))
            .await
            .expect("a fresh send must target only the replacement generation");
        let fresh = replacement.recv().await.unwrap();
        assert_eq!(fresh.message(), &Message::Text("z".into()));
        drop(fresh);
        drop(old_receiver);
        assert_eq!(old_budget.used_bytes(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn outbound_admission_timeout_closes_exactly_the_slow_connection() {
        const BYTE_BUDGET: usize = 4;
        const ADMISSION_TIMEOUT: Duration = Duration::from_millis(50);

        let manager = outbound_policy_manager(BYTE_BUDGET, BYTE_BUDGET, ADMISSION_TIMEOUT);
        let connection_id = Uuid::new_v4();
        let (receiver, mut control_receiver, budget) =
            add_managed_connection(&manager, connection_id, 1).await;
        manager
            .send_application_frame("test", connection_id, Message::Text("xxxx".into()))
            .await
            .expect("the first frame must consume the exact byte budget");

        let waiting_manager = manager.clone();
        let waiting_send = tokio::spawn(async move {
            waiting_manager
                .send_application_frame("test", connection_id, Message::Text("y".into()))
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(ADMISSION_TIMEOUT - Duration::from_millis(1)).await;
        assert!(
            !waiting_send.is_finished(),
            "admission must remain pending before the configured deadline"
        );
        tokio::time::advance(Duration::from_millis(1)).await;

        assert!(matches!(
            waiting_send.await.expect("the waiting send must not panic"),
            Err(ConnectionError::Backpressure {
                connection_id: timed_out
            }) if timed_out == connection_id
        ));
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Closing
        );
        assert!(budget.is_closed());
        assert!(matches!(
            control_receiver.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::SlowConsumer,
            }))
                if frame.code == crate::request::WsCloseReason::SlowConsumer.code()
                    && frame.reason == crate::request::WsCloseReason::SlowConsumer.reason()
        ));
        assert_eq!(
            manager
                .request_close_frame(
                    connection_id,
                    Some(crate::request::WsCloseReason::Application.frame()),
                    WsConnectionCloseCategory::Application,
                )
                .await
                .unwrap(),
            CloseRequestOutcome::AlreadyClosing
        );
        assert!(matches!(
            control_receiver.close.borrow().as_ref(),
            Some(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::SlowConsumer,
            })
                if frame.code == crate::request::WsCloseReason::SlowConsumer.code()
                    && frame.reason == crate::request::WsCloseReason::SlowConsumer.reason()
        ));

        drop(receiver);
        assert_eq!(
            budget.used_bytes(),
            0,
            "dropping a closed writer queue must release every queued byte"
        );
    }

    #[tokio::test]
    async fn outbound_application_limit_does_not_block_protocol_control_frames() {
        let manager = outbound_limited_manager(1);
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(4);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();

        assert_outbound_too_large(
            manager
                .send_application_frame("test", connection_id, Message::Text("xx".into()))
                .await
                .unwrap_err(),
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        for control in [Message::Ping(vec![1, 2, 3]), Message::Pong(vec![4, 5, 6])] {
            let expected = control.clone();
            manager
                .send_raw_to_connection(connection_id, control)
                .await
                .expect("application byte limit must not govern protocol control frames");
            assert_eq!(
                timeout(Duration::from_secs(1), receiver.recv())
                    .await
                    .expect("control frame must be forwarded within the test deadline"),
                Some(expected)
            );
        }

        let close_frame = crate::request::WsCloseReason::InternalFailure.frame();
        let untyped_close = Message::Close(Some(close_frame.clone()));
        assert!(matches!(
            manager
                .send_raw_to_connection(connection_id, untyped_close)
                .await,
            Err(ConnectionError::InvalidOperation(
                ConnectionOperationError::InvalidProtocolFrame
            ))
        ));
        manager
            .request_close_frame(
                connection_id,
                Some(close_frame.clone()),
                WsConnectionCloseCategory::InternalError,
            )
            .await
            .expect("typed close control must bypass the application byte limit");
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("close frame must be forwarded within the test deadline"),
            Some(Message::Close(Some(close_frame)))
        );
    }

    #[tokio::test]
    async fn canonical_direct_send_and_broadcast_cannot_bypass_outbound_limit() {
        let message = application_message();
        let encoded_size = message.to_message().unwrap().len();
        assert_eq!(encoded_size, message.to_binary_message().unwrap().len());

        let accepted = outbound_limited_manager(encoded_size);
        let accepted_id = Uuid::new_v4();
        let (accepted_sender, mut accepted_receiver) = mpsc::channel(2);
        accepted
            .add_connection(accepted_id, accepted_sender, None, Some("test".into()))
            .await
            .unwrap();
        accepted
            .send_to_connection("test", accepted_id, message.clone())
            .await
            .unwrap();
        accepted
            .send_binary_to_connection("test", accepted_id, message.clone())
            .await
            .unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(1), accepted_receiver.recv())
                .await
                .expect("text direct send must complete within the test deadline"),
            Some(Message::Text(_))
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), accepted_receiver.recv())
                .await
                .expect("binary direct send must complete within the test deadline"),
            Some(Message::Binary(_))
        ));

        let rejected = outbound_limited_manager(encoded_size - 1);
        let rejected_id = Uuid::new_v4();
        let (rejected_sender, mut rejected_receiver) = mpsc::channel(2);
        rejected
            .add_connection(rejected_id, rejected_sender, None, Some("test".into()))
            .await
            .unwrap();
        assert_outbound_too_large(
            rejected
                .send_to_connection("test", rejected_id, message.clone())
                .await
                .unwrap_err(),
        );
        assert_outbound_too_large(
            rejected
                .send_binary_to_connection("test", rejected_id, message.clone())
                .await
                .unwrap_err(),
        );
        assert_outbound_too_large(
            rejected
                .broadcast(BroadcastMessage {
                    target: BroadcastTarget::Namespace("test".to_owned()),
                    message,
                    wire_format: WsWireFormat::Text,
                    exclude: Vec::new(),
                })
                .await
                .unwrap_err(),
        );
        assert!(matches!(
            rejected_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        let no_targets = outbound_limited_manager(encoded_size - 1);
        assert_outbound_too_large(
            no_targets
                .broadcast(BroadcastMessage {
                    target: BroadcastTarget::Namespace("test".to_owned()),
                    message: application_message(),
                    wire_format: WsWireFormat::Binary,
                    exclude: Vec::new(),
                })
                .await
                .unwrap_err(),
        );
    }

    fn principal_broadcast(namespace: &str, subject: &str) -> BroadcastMessage {
        BroadcastMessage {
            target: BroadcastTarget::Principal {
                namespace: namespace.to_owned(),
                principal_id: PrincipalId::try_new(subject).unwrap(),
            },
            message: application_message(),
            wire_format: WsWireFormat::Text,
            exclude: Vec::new(),
        }
    }

    async fn add_identity_client(
        manager: &ConnectionManager,
        namespace: &str,
        subject: &str,
    ) -> (Uuid, mpsc::Receiver<Message>) {
        let connection_id = Uuid::new_v4();
        let (sender, receiver) = mpsc::channel(4);
        manager
            .add_authenticated_connection(
                connection_id,
                sender,
                Some(namespace.to_owned()),
                authenticated_identity(subject),
            )
            .await
            .unwrap();
        (connection_id, receiver)
    }

    #[test]
    fn principal_id_is_exact_bounded_validated_and_debug_redacted() {
        let exact = "x".repeat(MAX_PRINCIPAL_ID_BYTES);
        assert_eq!(PrincipalId::try_new(&exact).unwrap().as_str(), exact);
        assert!(matches!(
            PrincipalId::try_new(""),
            Err(PrincipalIdError::Empty)
        ));
        assert!(matches!(
            PrincipalId::try_new("x".repeat(MAX_PRINCIPAL_ID_BYTES + 1)),
            Err(PrincipalIdError::TooLong)
        ));
        for ambiguous in [" account-42", "account-42 "] {
            assert!(matches!(
                PrincipalId::try_new(ambiguous),
                Err(PrincipalIdError::SurroundingWhitespace)
            ));
        }
        assert!(matches!(
            PrincipalId::try_new("account\n42"),
            Err(PrincipalIdError::ControlCharacter)
        ));

        let secret = "sensitive-account-42";
        let principal_id = PrincipalId::try_new(secret).unwrap();
        assert!(!format!("{principal_id:?}").contains(secret));
        assert_eq!(
            serde_json::to_string(&principal_id).unwrap(),
            format!("\"{secret}\"")
        );
    }

    #[tokio::test]
    async fn identity_disabled_manager_allocates_no_index_and_rejects_principal_authority() {
        let manager = test_manager();
        assert!(manager.registry.read().await.principal_index.is_none());

        let (sender, _receiver) = mpsc::channel(1);
        let error = manager
            .add_authenticated_connection(
                Uuid::new_v4(),
                sender,
                Some("test".into()),
                authenticated_identity("account-42"),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ConnectionError::InvalidOperation(
                ConnectionOperationError::IdentityLifecycleUnavailable
            )
        ));

        let error = manager
            .broadcast(principal_broadcast("test", "account-42"))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ConnectionError::InvalidOperation(
                ConnectionOperationError::IdentityLifecycleUnavailable
            )
        ));
    }

    #[tokio::test]
    async fn principal_fanout_is_multi_device_and_namespace_isolated() {
        let manager = identity_manager();
        let (_first_id, mut first) = add_identity_client(&manager, "orders", "account-42").await;
        let (_second_id, mut second) = add_identity_client(&manager, "orders", "account-42").await;
        let (_other_id, mut other) = add_identity_client(&manager, "orders", "account-73").await;
        let (_other_namespace_id, mut other_namespace) =
            add_identity_client(&manager, "billing", "account-42").await;

        let report = manager
            .broadcast(principal_broadcast("orders", "account-42"))
            .await
            .unwrap();
        assert_eq!(
            report,
            BroadcastReport {
                targeted: 2,
                sent: 2,
                ..Default::default()
            }
        );
        assert!(matches!(first.try_recv(), Ok(Message::Text(_))));
        assert!(matches!(second.try_recv(), Ok(Message::Text(_))));
        assert!(matches!(
            other.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            other_namespace.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn closing_and_removal_clear_principal_membership_before_terminal_cleanup() {
        let manager = identity_manager();
        let (connection_id, mut receiver) =
            add_identity_client(&manager, "orders", "account-42").await;

        assert!(manager.mark_closing(connection_id).await.unwrap());
        let report = manager
            .broadcast(principal_broadcast("orders", "account-42"))
            .await
            .unwrap();
        assert_eq!(report, BroadcastReport::default());
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        manager.remove_connection(connection_id).await.unwrap();
        let registry = manager.registry.read().await;
        assert!(
            registry
                .principal_index
                .as_ref()
                .is_some_and(HashMap::is_empty)
        );
    }

    #[tokio::test]
    async fn reauthentication_moves_membership_and_rejects_a_stale_verifier() {
        let manager = identity_manager();
        let (connection_id, mut receiver) =
            add_identity_client(&manager, "orders", "account-a").await;
        let observed = manager
            .registry
            .read()
            .await
            .connections
            .get(&connection_id)
            .and_then(|connection| connection.identity.as_ref())
            .map(ConnectionIdentityHandle::snapshot)
            .unwrap();

        let outcome = manager
            .reauthenticate(
                connection_id,
                observed.revision(),
                authenticated_identity("account-b"),
            )
            .await
            .unwrap();
        let WebSocketIdentityUpdateOutcome::Updated(current_revision) = outcome else {
            panic!("the observed identity revision must win");
        };
        assert_eq!(
            manager
                .reauthenticate(
                    connection_id,
                    observed.revision(),
                    authenticated_identity("account-c"),
                )
                .await
                .unwrap(),
            WebSocketIdentityUpdateOutcome::Stale(current_revision)
        );

        assert_eq!(
            manager
                .broadcast(principal_broadcast("orders", "account-a"))
                .await
                .unwrap(),
            BroadcastReport::default()
        );
        assert_eq!(
            manager
                .broadcast(principal_broadcast("orders", "account-c"))
                .await
                .unwrap(),
            BroadcastReport::default()
        );
        assert_eq!(
            manager
                .broadcast(principal_broadcast("orders", "account-b"))
                .await
                .unwrap(),
            BroadcastReport {
                targeted: 1,
                sent: 1,
                ..Default::default()
            }
        );
        assert!(matches!(receiver.try_recv(), Ok(Message::Text(_))));
    }

    #[tokio::test]
    async fn concurrent_reauthentication_has_one_revision_winner() {
        let manager = Arc::new(identity_manager());
        let (connection_id, _receiver) =
            add_identity_client(manager.as_ref(), "orders", "account-a").await;
        let observed = manager
            .registry
            .read()
            .await
            .connections
            .get(&connection_id)
            .and_then(|connection| connection.identity.as_ref())
            .map(ConnectionIdentityHandle::snapshot)
            .unwrap();

        let first_manager = Arc::clone(&manager);
        let second_manager = Arc::clone(&manager);
        let (first, second) = tokio::join!(
            first_manager.reauthenticate(
                connection_id,
                observed.revision(),
                authenticated_identity("account-b"),
            ),
            second_manager.reauthenticate(
                connection_id,
                observed.revision(),
                authenticated_identity("account-c"),
            ),
        );
        let outcomes = [first.unwrap(), second.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, WebSocketIdentityUpdateOutcome::Updated(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, WebSocketIdentityUpdateOutcome::Stale(_)))
                .count(),
            1
        );

        let current = manager
            .registry
            .read()
            .await
            .connections
            .get(&connection_id)
            .and_then(|connection| connection.identity.as_ref())
            .map(ConnectionIdentityHandle::snapshot)
            .unwrap();
        assert!(matches!(
            current.principal_id().map(PrincipalId::as_str),
            Some("account-b" | "account-c")
        ));
        assert_eq!(
            manager
                .broadcast(principal_broadcast("orders", "account-a"))
                .await
                .unwrap(),
            BroadcastReport::default()
        );
    }

    #[tokio::test]
    async fn an_old_expiry_revision_cannot_close_a_refreshed_identity() {
        let manager = identity_manager();
        let (connection_id, _receiver) =
            add_identity_client(&manager, "orders", "account-42").await;
        let old = manager
            .registry
            .read()
            .await
            .connections
            .get(&connection_id)
            .and_then(|connection| connection.identity.as_ref())
            .map(ConnectionIdentityHandle::snapshot)
            .unwrap();
        let replacement = authenticated_identity("account-42")
            .expires_at(TokioInstant::now() + Duration::from_secs(60));
        assert!(matches!(
            manager
                .reauthenticate(connection_id, old.revision(), replacement)
                .await
                .unwrap(),
            WebSocketIdentityUpdateOutcome::Updated(_)
        ));

        assert!(matches!(
            manager
                .claim_identity_expiry(connection_id, old.revision())
                .await
                .unwrap(),
            IdentityExpiryClaim::Superseded
        ));
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Connected
        );
    }

    #[tokio::test]
    async fn expired_identity_claim_removes_target_and_blocks_reauthentication() {
        let manager = identity_manager();
        let connection_id = Uuid::new_v4();
        let (data_sender, mut data_receiver) = mpsc::channel::<Message>(2);
        let (control_sender, mut control_receiver) = connection_control_channel();
        let identity = authenticated_identity("account-42")
            .expires_at(TokioInstant::now() + Duration::from_millis(10));
        let handle =
            ConnectionIdentityHandle::new(WebSocketIdentitySnapshot::authenticated(identity));
        let revision = handle.snapshot().revision();
        manager
            .add_connection_with_control_and_identity(
                connection_id,
                data_sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("orders".into()),
                Some(handle),
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(
            manager
                .send_to_connection("orders", connection_id, application_message())
                .await,
            Err(ConnectionError::InvalidOperation(
                ConnectionOperationError::IdentityExpired
            ))
        ));
        assert_eq!(
            manager
                .broadcast(principal_broadcast("orders", "account-42"))
                .await
                .unwrap(),
            BroadcastReport::default()
        );
        assert_eq!(
            manager
                .reauthenticate(
                    connection_id,
                    revision,
                    authenticated_identity("account-73"),
                )
                .await
                .unwrap(),
            WebSocketIdentityUpdateOutcome::AlreadyClosing
        );
        assert!(matches!(
            control_receiver.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::IdentityExpired,
            }))
                if frame.code == crate::request::WsCloseReason::IdentityExpired.code()
                    && frame.reason == crate::request::WsCloseReason::IdentityExpired.reason()
        ));
        assert!(matches!(
            data_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn reauthentication_cannot_revive_an_expired_current_revision() {
        let manager = identity_manager();
        let connection_id = Uuid::new_v4();
        let (data_sender, _data_receiver) = mpsc::channel::<Message>(1);
        let (control_sender, mut control_receiver) = connection_control_channel();
        let identity = authenticated_identity("account-old")
            .expires_at(TokioInstant::now() + Duration::from_millis(10));
        let handle =
            ConnectionIdentityHandle::new(WebSocketIdentitySnapshot::authenticated(identity));
        let revision = handle.snapshot().revision();
        manager
            .add_connection_with_control_and_identity(
                connection_id,
                data_sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("orders".into()),
                Some(handle),
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(
            manager
                .reauthenticate(
                    connection_id,
                    revision,
                    authenticated_identity("account-new"),
                )
                .await,
            Err(ConnectionError::InvalidOperation(
                ConnectionOperationError::IdentityExpired
            ))
        ));
        assert_eq!(
            manager
                .registry
                .read()
                .await
                .connections
                .get(&connection_id)
                .and_then(|connection| connection.identity.as_ref())
                .map(ConnectionIdentityHandle::snapshot)
                .and_then(|snapshot| snapshot.principal_id().cloned())
                .as_ref()
                .map(PrincipalId::as_str),
            Some("account-old")
        );

        assert!(matches!(
            manager
                .claim_identity_expiry(connection_id, revision)
                .await
                .unwrap(),
            IdentityExpiryClaim::Closing(CloseRequestOutcome::Requested)
        ));
        assert!(matches!(
            control_receiver.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::IdentityExpired,
            }))
                if frame.reason == crate::request::WsCloseReason::IdentityExpired.reason()
        ));
    }

    #[tokio::test]
    async fn close_admission_uses_identity_expiry_frame_and_category_when_deadline_elapsed() {
        let manager = identity_manager();
        let connection_id = Uuid::new_v4();
        let (data_sender, _data_receiver) = mpsc::channel::<Message>(1);
        let (control_sender, mut control_receiver) = connection_control_channel();
        let identity = authenticated_identity("account-42")
            .expires_at(TokioInstant::now() + Duration::from_millis(10));
        manager
            .add_connection_with_control_and_identity(
                connection_id,
                data_sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("orders".into()),
                Some(ConnectionIdentityHandle::new(
                    WebSocketIdentitySnapshot::authenticated(identity),
                )),
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            manager
                .request_close_frame(
                    connection_id,
                    Some(crate::request::WsCloseReason::Application.frame()),
                    WsConnectionCloseCategory::Application,
                )
                .await
                .unwrap(),
            CloseRequestOutcome::Requested
        );
        assert!(matches!(
            control_receiver.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::IdentityExpired,
            }))
                if frame.code == crate::request::WsCloseReason::IdentityExpired.code()
                    && frame.reason == crate::request::WsCloseReason::IdentityExpired.reason()
        ));
    }

    #[tokio::test]
    async fn manager_preserves_typed_client_identity_and_transport_security() {
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let (data_sender, _data_receiver) = mpsc::channel(1);
        let (control_sender, _control_receiver) = connection_control_channel();
        let peer_ip = "10.2.3.4".parse().unwrap();
        let client_ip = "198.51.100.20".parse().unwrap();
        let connection_info =
            RequestConnectionInfo::from_trusted_transport(Some(peer_ip), Some(client_ip), true);

        manager
            .add_connection_with_control(
                connection_id,
                data_sender,
                control_sender,
                connection_info,
                WsTransportSecurity::Tls,
                Some("test".to_owned()),
            )
            .await
            .unwrap();

        let info = manager.get_connection(connection_id).await.unwrap();
        assert_eq!(info.peer_ip(), Some(peer_ip));
        assert_eq!(info.client_ip(), Some(client_ip));
        assert!(info.via_trusted_proxy());
        assert!(info.is_secure());
    }

    #[tokio::test]
    async fn namespace_broadcast_is_isolated_and_uses_data_field() {
        let manager = ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into(), "users".into()],
        );
        let orders_id = Uuid::new_v4();
        let users_id = Uuid::new_v4();
        let (orders_tx, mut orders_rx) = mpsc::channel(4);
        let (users_tx, mut users_rx) = mpsc::channel(4);

        manager
            .add_connection(orders_id, orders_tx, None, Some("orders".to_string()))
            .await
            .unwrap();
        manager
            .add_connection(users_id, users_tx, None, Some("users".to_string()))
            .await
            .unwrap();

        manager
            .broadcast(BroadcastMessage {
                target: BroadcastTarget::Namespace("orders".to_string()),
                message: WsMessageBody::try_new("orders:created", json!({ "id": "42" })).unwrap(),
                wire_format: WsWireFormat::Text,
                exclude: Vec::new(),
            })
            .await
            .unwrap();

        let message = timeout(Duration::from_secs(1), orders_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let Message::Text(text) = message else {
            panic!("expected a text frame");
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["event"], "orders:created");
        assert_eq!(value["data"]["id"], "42");
        assert!(value.get("payload").is_none());
        assert!(
            timeout(Duration::from_millis(25), users_rx.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn broadcast_report_reconciles_every_terminal_target() {
        let manager = outbound_policy_manager(
            DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE_BYTES,
            DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE_BYTES,
            Duration::from_millis(50),
        );
        let sent_id = Uuid::new_v4();
        let backpressured_id = Uuid::new_v4();
        let closed_id = Uuid::new_v4();
        let missing_id = Uuid::new_v4();

        let (sent_tx, mut sent_rx) = mpsc::channel(1);
        let (backpressured_tx, _backpressured_rx) = mpsc::channel(1);
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        manager
            .add_connection(sent_id, sent_tx, None, Some("test".into()))
            .await
            .unwrap();
        manager
            .add_connection(
                backpressured_id,
                backpressured_tx.clone(),
                None,
                Some("test".into()),
            )
            .await
            .unwrap();
        manager
            .add_connection(closed_id, closed_tx, None, Some("test".into()))
            .await
            .unwrap();
        backpressured_tx
            .try_send(Message::Ping(Vec::new()))
            .unwrap();

        let report = manager
            .broadcast(BroadcastMessage {
                target: BroadcastTarget::NamespaceConnections {
                    namespace: "test".to_owned(),
                    connection_ids: vec![sent_id, backpressured_id, closed_id, missing_id],
                },
                message: WsMessageBody::try_new("orders:created", json!({ "id": "42" })).unwrap(),
                wire_format: WsWireFormat::Text,
                exclude: Vec::new(),
            })
            .await
            .unwrap();

        assert_eq!(
            report,
            BroadcastReport {
                targeted: 4,
                sent: 1,
                backpressured: 1,
                closed: 1,
                missing: 1,
            }
        );
        let metrics = manager.metrics_snapshot();
        assert_eq!(metrics.outbound_messages, 1);
        assert_eq!(metrics.backpressured, 1);
        assert_eq!(metrics.channel_closed, 1);
        assert!(report.is_balanced());
        assert!(sent_rx.recv().await.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_broadcast_admits_healthy_peers_before_slow_peer_timeout() {
        const ADMISSION_TIMEOUT: Duration = Duration::from_millis(100);

        let message = application_message();
        let frame_size = message.to_message().unwrap().len();
        let manager = outbound_policy_manager(frame_size, frame_size, ADMISSION_TIMEOUT);
        let a_id = Uuid::new_v4();
        let b_id = Uuid::new_v4();
        let c_id = Uuid::new_v4();
        let (mut a_receiver, _a_control, _a_budget) =
            add_managed_connection(&manager, a_id, 1).await;
        let (mut b_receiver, mut b_control, b_budget) =
            add_managed_connection(&manager, b_id, 1).await;
        let (mut c_receiver, _c_control, _c_budget) =
            add_managed_connection(&manager, c_id, 1).await;

        manager
            .send_to_connection("test", b_id, message.clone())
            .await
            .expect("B's first frame must saturate its exact byte budget");
        assert_eq!(b_budget.used_bytes(), frame_size);

        let broadcast_manager = manager.clone();
        let broadcast = tokio::spawn(async move {
            broadcast_manager
                .broadcast(BroadcastMessage {
                    target: BroadcastTarget::NamespaceConnections {
                        namespace: "test".to_owned(),
                        connection_ids: vec![a_id, b_id, c_id],
                    },
                    message,
                    wire_format: WsWireFormat::Text,
                    exclude: Vec::new(),
                })
                .await
        });

        let mut a_frame = None;
        let mut c_frame = None;
        for _ in 0..16 {
            if a_frame.is_none() {
                a_frame = a_receiver.try_recv().ok();
            }
            if c_frame.is_none() {
                c_frame = c_receiver.try_recv().ok();
            }
            if a_frame.is_some() && c_frame.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let a_frame = a_frame.expect("A must be admitted without waiting for saturated B");
        let c_frame = c_frame.expect("C must be admitted without waiting for saturated B");
        assert_eq!(a_frame.message().len(), frame_size);
        assert_eq!(c_frame.message().len(), frame_size);
        assert!(
            !broadcast.is_finished(),
            "the aggregate report must still be waiting for B's shared deadline"
        );

        tokio::time::advance(ADMISSION_TIMEOUT).await;
        let report = broadcast
            .await
            .expect("broadcast task must not panic")
            .expect("per-target timeout is represented in the broadcast report");
        assert_eq!(
            report,
            BroadcastReport {
                targeted: 3,
                sent: 2,
                backpressured: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            manager.get_connection(a_id).await.unwrap().state,
            ConnectionState::Connected
        );
        assert_eq!(
            manager.get_connection(b_id).await.unwrap().state,
            ConnectionState::Closing
        );
        assert_eq!(
            manager.get_connection(c_id).await.unwrap().state,
            ConnectionState::Connected
        );
        assert!(matches!(
            b_control.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::SlowConsumer,
            }))
                if frame.code == crate::request::WsCloseReason::SlowConsumer.code()
                    && frame.reason == crate::request::WsCloseReason::SlowConsumer.reason()
        ));

        let original_b_frame = b_receiver
            .try_recv()
            .expect("B must retain only the frame that originally consumed its budget");
        assert!(matches!(
            b_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop((a_frame, c_frame, original_b_frame));
    }

    #[tokio::test(start_paused = true)]
    async fn stale_timeout_cannot_close_a_readded_connection_with_the_same_uuid() {
        const BYTE_BUDGET: usize = 4;
        const ADMISSION_TIMEOUT: Duration = Duration::from_millis(50);

        let manager = outbound_policy_manager(BYTE_BUDGET, BYTE_BUDGET, ADMISSION_TIMEOUT);
        let connection_id = Uuid::new_v4();
        let (old_receiver, _old_control, old_budget) =
            add_managed_connection(&manager, connection_id, 1).await;
        manager
            .send_application_frame("test", connection_id, Message::Text("xxxx".into()))
            .await
            .expect("the old generation must begin with a saturated budget");
        let old_target = {
            let registry = manager.registry.read().await;
            let connection = registry.connections.get(&connection_id).unwrap();
            ConnectionAdmissionTarget {
                connection_id,
                sender: connection.data_sender.clone(),
                budget: Arc::clone(&connection.outbound_budget),
            }
        };

        let deadline = TokioInstant::now() + ADMISSION_TIMEOUT;
        let admission_manager = manager.clone();
        let (timed_out_sender, timed_out_receiver) = tokio::sync::oneshot::channel();
        let (continue_sender, continue_receiver) = tokio::sync::oneshot::channel();
        let stale_admission = tokio::spawn(async move {
            let (failure, _frame) = admission_manager
                .admit_application_frame(
                    &old_target,
                    Message::Text("y".into()),
                    deadline,
                    &ConnectionTargetFilter::Namespace("test".to_owned()),
                )
                .await
                .expect_err("the old generation has no remaining byte capacity");
            timed_out_sender
                .send(failure)
                .expect("the test must observe the admission result");
            continue_receiver
                .await
                .expect("the test must release the stale close attempt");
            (
                failure,
                admission_manager.close_slow_consumer(&old_target).await,
            )
        });

        tokio::task::yield_now().await;
        tokio::time::advance(ADMISSION_TIMEOUT).await;
        assert_eq!(
            timed_out_receiver.await.unwrap(),
            OutboundAdmissionFailure::TimedOut
        );

        manager.remove_connection(connection_id).await.unwrap();
        let (mut replacement_receiver, replacement_control, replacement_budget) =
            add_managed_connection(&manager, connection_id, 1).await;
        continue_sender.send(()).unwrap();
        let (failure, closed_replacement) = stale_admission.await.unwrap();
        assert_eq!(failure, OutboundAdmissionFailure::TimedOut);
        assert!(
            !closed_replacement,
            "an old timeout must fail its generation check against the replacement"
        );
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Connected
        );
        assert!(!replacement_budget.is_closed());
        assert!(replacement_control.close.borrow().is_none());
        assert!(matches!(
            replacement_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        drop(old_receiver);
        assert_eq!(old_budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn broadcast_deduplicates_explicit_targets_and_exclusions() {
        let manager = test_manager();
        let sent_id = Uuid::new_v4();
        let excluded_id = Uuid::new_v4();
        let missing_id = Uuid::new_v4();
        let (sent_tx, mut sent_rx) = mpsc::channel(2);
        let (excluded_tx, mut excluded_rx) = mpsc::channel(2);
        manager
            .add_connection(sent_id, sent_tx, None, Some("test".into()))
            .await
            .unwrap();
        manager
            .add_connection(excluded_id, excluded_tx, None, Some("test".into()))
            .await
            .unwrap();

        let report = manager
            .broadcast(BroadcastMessage {
                target: BroadcastTarget::NamespaceConnections {
                    namespace: "test".to_owned(),
                    connection_ids: vec![
                        missing_id,
                        sent_id,
                        excluded_id,
                        sent_id,
                        missing_id,
                        excluded_id,
                    ],
                },
                message: application_message(),
                wire_format: WsWireFormat::Text,
                exclude: vec![excluded_id, excluded_id],
            })
            .await
            .unwrap();

        assert_eq!(
            report,
            BroadcastReport {
                targeted: 2,
                sent: 1,
                missing: 1,
                ..Default::default()
            }
        );
        assert!(sent_rx.recv().await.is_some());
        assert!(
            timeout(Duration::from_millis(25), sent_rx.recv())
                .await
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(25), excluded_rx.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn removing_a_connection_cleans_rooms_and_keeps_registered_namespace() {
        let manager = ConnectionManager::with_registered_namespaces(128, 128, ["orders".into()]);
        let connection_id = Uuid::new_v4();
        let (sender, _receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        manager
            .join_room("orders", connection_id, "priority")
            .await
            .unwrap();

        manager.remove_connection(connection_id).await.unwrap();

        let namespace = manager
            .group_manager
            .get_namespace_stats("orders")
            .await
            .expect("registered namespace remains available");
        assert_eq!(namespace.total_connections, 0);
        assert_eq!(namespace.total_rooms, 0);
        assert!(
            manager
                .group_manager
                .get_room_info("orders", "priority")
                .await
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_join_cannot_reinsert_room_membership_after_removal_returns() {
        let manager = ConnectionManager::with_registered_namespaces(128, 128, ["orders".into()]);
        let connection_id = Uuid::new_v4();
        let room = "priority";
        let (sender, _receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();

        let registry_read = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        *manager.join_room_registry_read_barrier.lock().unwrap() = Some(JoinRoomTestBarrier {
            connection_id,
            registry_read: Arc::clone(&registry_read),
            resume: Arc::clone(&resume),
        });

        let join_manager = manager.clone();
        let mut join_task =
            tokio::spawn(
                async move { join_manager.join_room("orders", connection_id, room).await },
            );
        timeout(Duration::from_secs(1), registry_read.wait())
            .await
            .expect("join_room did not reach the post-registry-read barrier");

        let registry_authority_held = manager.registry.try_write().is_err();
        let remove_manager = manager.clone();
        let mut remove_task =
            tokio::spawn(async move { remove_manager.remove_connection(connection_id).await });

        resume.wait().await;
        timeout(Duration::from_secs(1), &mut join_task)
            .await
            .expect("join task did not finish after the test barrier opened")
            .expect("join task panicked")
            .expect("join that began before removal should linearize successfully");
        timeout(Duration::from_secs(1), &mut remove_task)
            .await
            .expect("removal did not finish after join_room released its authority")
            .expect("remove task panicked")
            .expect("connection removal failed");
        *manager.join_room_registry_read_barrier.lock().unwrap() = None;

        assert!(
            registry_authority_held,
            "join_room released registry authority before its group mutation"
        );
        assert!(manager.get_connection(connection_id).await.is_none());
        assert!(
            !manager
                .get_namespace_connections("orders")
                .await
                .contains(&connection_id)
        );
        let room_connections = manager.get_room_connections("orders", room).await;
        assert!(
            !room_connections.contains(&connection_id),
            "RSK-001: join_room reinserted a removed connection into room `{room}`; room_connections={room_connections:?}"
        );
    }

    #[tokio::test]
    async fn room_membership_enforces_the_canonical_configured_token_bound() {
        let manager = ConnectionManager::with_registered_namespaces(8, 8, ["orders".into()]);
        let connection_id = Uuid::new_v4();
        let (sender, _receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();

        manager
            .join_room("orders", connection_id, "room-1._")
            .await
            .unwrap();
        for invalid in [
            "",
            "room name",
            "room:name",
            "room\nname",
            "öncelik",
            "room-1._x",
        ] {
            let error = manager
                .join_room("orders", connection_id, invalid)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                ConnectionError::Group(GroupError::InvalidRoomName { maximum: 8 })
            ));
        }
    }

    #[tokio::test]
    async fn connection_admission_rejects_unregistered_namespaces() {
        let manager = ConnectionManager::with_registered_namespaces(128, 128, ["orders".into()]);
        let (sender, _receiver) = mpsc::channel(1);
        let error = manager
            .add_connection(Uuid::new_v4(), sender, None, Some("missing".into()))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ConnectionError::Group(GroupError::NamespaceNotFound { namespace })
                if namespace == "missing"
        ));
        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn connection_admission_requires_an_exact_namespace() {
        let manager = test_manager();
        let (sender, _receiver) = mpsc::channel(1);
        let error = manager
            .add_connection(Uuid::new_v4(), sender, None, None)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ConnectionError::InvalidOperation(ConnectionOperationError::MissingNamespace)
        ));
        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn connection_metadata_is_bounded_replaceable_and_debug_redacted() {
        const SECRET: &str = "LILY_CONNECTION_METADATA_SECRET_9AE1";
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let (sender, _receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".to_string()))
            .await
            .unwrap();

        for index in 0..MAX_CONNECTION_METADATA_ENTRIES {
            manager
                .set_connection_metadata(
                    "test",
                    connection_id,
                    format!("metadata-{index}"),
                    if index == 0 {
                        SECRET.into()
                    } else {
                        "ok".into()
                    },
                )
                .await
                .unwrap();
        }
        manager
            .set_connection_metadata(
                "test",
                connection_id,
                "metadata-1".into(),
                "replacement".into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            manager
                .set_connection_metadata("test", connection_id, "overflow".into(), "no".into())
                .await,
            Err(ConnectionError::Metadata(
                ConnectionMetadataError::TooManyEntries
            ))
        ));
        assert!(matches!(
            manager
                .set_connection_metadata(
                    "test",
                    connection_id,
                    "oversized".into(),
                    "x".repeat(MAX_CONNECTION_METADATA_VALUE_BYTES + 1),
                )
                .await,
            Err(ConnectionError::Metadata(
                ConnectionMetadataError::InvalidValue
            ))
        ));

        let info = manager.get_connection(connection_id).await.unwrap();
        let debug = format!("{info:?}");
        assert!(!debug.contains(SECRET));
        assert!(!debug.contains("replacement"));
        assert!(debug.contains("metadata-0"));

        for debug in [format!("{manager:?}"), format!("{manager:#?}")] {
            assert!(!debug.contains(SECRET));
            assert!(!debug.contains("replacement"));
            assert!(debug.contains("metadata-0"));
        }
    }

    #[tokio::test]
    async fn application_close_keeps_application_category_when_reason_matches_identity_expiry() {
        let manager = Arc::new(test_manager());
        let connection_id = Uuid::new_v4();
        let (sender, _receiver) = mpsc::channel(1);
        let (control_sender, mut control_receiver) = connection_control_channel();
        manager
            .add_connection_with_control(
                connection_id,
                sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("test".into()),
            )
            .await
            .unwrap();
        let context = crate::controller::WebSocketContext::new(
            connection_id,
            Arc::clone(&manager),
            "test".into(),
        );

        assert_eq!(
            context
                .close(
                    crate::CloseConnection::policy(
                        crate::request::WsCloseReason::IdentityExpired.reason(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            CloseRequestOutcome::Requested
        );
        assert!(matches!(
            control_receiver.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::Application,
            })) if frame.code == crate::request::WsCloseReason::IdentityExpired.code()
                && frame.reason == crate::request::WsCloseReason::IdentityExpired.reason()
        ));
    }

    #[tokio::test]
    async fn close_control_first_writer_keeps_the_winning_frame_and_category_together() {
        let (sender, mut receiver) = connection_control_channel();
        let winning_message =
            Message::Close(Some(crate::request::WsCloseReason::Application.frame()));
        assert_eq!(
            sender
                .request_close(
                    winning_message.clone(),
                    WsConnectionCloseCategory::Application,
                )
                .unwrap(),
            CloseRequestOutcome::Requested
        );
        assert_eq!(
            sender
                .request_close(
                    Message::Close(Some(crate::request::WsCloseReason::SlowConsumer.frame())),
                    WsConnectionCloseCategory::SlowConsumer,
                )
                .unwrap(),
            CloseRequestOutcome::AlreadyClosing
        );

        let ConnectionControlFrame::Close(request) = receiver.next().await.unwrap() else {
            panic!("the first published control frame must be terminal");
        };
        assert_eq!(request.message(), &winning_message);
        assert_eq!(request.category(), WsConnectionCloseCategory::Application);
        let (message, category) = request.into_parts();
        assert_eq!(message, winning_message);
        assert_eq!(category, WsConnectionCloseCategory::Application);
        assert!(
            timeout(Duration::from_millis(20), receiver.next())
                .await
                .is_err(),
            "the losing request must not publish either a frame or category"
        );
    }

    #[tokio::test]
    async fn shutdown_close_marks_connection_closing_and_cleanup_removes_bookkeeping() {
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let (sender, _receiver) = mpsc::channel(1);
        let (control_sender, mut control_receiver) = connection_control_channel();
        manager
            .add_connection_with_control(
                connection_id,
                sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("test".into()),
            )
            .await
            .unwrap();

        manager
            .request_close_frame(
                connection_id,
                Some(crate::request::WsCloseReason::ServerShutdown.frame()),
                WsConnectionCloseCategory::ServerShutdown,
            )
            .await
            .unwrap();
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Closing
        );
        assert!(matches!(
            control_receiver.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::ServerShutdown,
            }))
                if frame.code == crate::request::WsCloseReason::ServerShutdown.code()
                    && frame.reason == crate::request::WsCloseReason::ServerShutdown.reason()
        ));

        assert_eq!(manager.remove_all_connections().await, 1);
        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn token_expiry_and_action_timeout_close_are_exactly_first_writer_wins() {
        let manager = Arc::new(test_manager());
        let connection_id = Uuid::new_v4();
        let (sender, _receiver) = mpsc::channel(1);
        let (control_sender, mut control_receiver) = connection_control_channel();
        manager
            .add_connection_with_control(
                connection_id,
                sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("test".into()),
            )
            .await
            .unwrap();
        let context = crate::controller::WebSocketContext::new(
            connection_id,
            Arc::clone(&manager),
            "test".into(),
        );
        let token_expired = crate::CloseConnection::policy("token_expired").unwrap();
        let action_timed_out = crate::CloseConnection::try_new(1011, "action_timed_out").unwrap();

        let (expiry, timeout_close) = tokio::join!(
            context.close(token_expired),
            context.close(action_timed_out),
        );
        let expiry = expiry.unwrap();
        let timeout_close = timeout_close.unwrap();
        assert!(matches!(
            (expiry, timeout_close),
            (
                CloseRequestOutcome::Requested,
                CloseRequestOutcome::AlreadyClosing
            ) | (
                CloseRequestOutcome::AlreadyClosing,
                CloseRequestOutcome::Requested
            )
        ));

        let expected_reason = if expiry == CloseRequestOutcome::Requested {
            "token_expired"
        } else {
            "action_timed_out"
        };
        assert!(matches!(
            control_receiver.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::Application,
            }))
                if frame.reason == expected_reason
        ));
        assert!(
            timeout(Duration::from_millis(20), control_receiver.next())
                .await
                .is_err(),
            "the bounded control slot must not publish a second close"
        );
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Closing
        );
        manager.remove_connection(connection_id).await.unwrap();
    }

    #[tokio::test]
    async fn connection_lease_release_is_idempotent_after_owner_side_reconciliation() {
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let (sender, _receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        let mut lease = ConnectionLease::new(manager.clone(), connection_id);

        manager.remove_connection(connection_id).await.unwrap();

        lease.release().await.unwrap();
        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn closing_connection_rejects_application_sends_and_broadcasts() {
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(4);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();

        assert!(manager.mark_closing(connection_id).await.unwrap());
        assert!(!manager.mark_closing(connection_id).await.unwrap());

        assert!(matches!(
            manager
                .send_to_connection("test", connection_id, application_message())
                .await,
            Err(ConnectionError::ConnectionClosed {
                connection_id: closed_id
            }) if closed_id == connection_id
        ));
        assert!(matches!(
            manager
                .send_binary_to_connection("test", connection_id, application_message())
                .await,
            Err(ConnectionError::ConnectionClosed {
                connection_id: closed_id
            }) if closed_id == connection_id
        ));

        let report = manager
            .broadcast(BroadcastMessage {
                target: BroadcastTarget::NamespaceConnections {
                    namespace: "test".to_owned(),
                    connection_ids: vec![connection_id],
                },
                message: application_message(),
                wire_format: WsWireFormat::Text,
                exclude: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(
            report,
            BroadcastReport {
                targeted: 1,
                closed: 1,
                ..Default::default()
            }
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn idle_cleanup_marks_closing_and_queues_canonical_close_once() {
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        manager
            .registry
            .write()
            .await
            .connections
            .get_mut(&connection_id)
            .unwrap()
            .last_application_activity_at = std::time::SystemTime::UNIX_EPOCH;

        let claim = manager
            .cleanup_inactive_connections(Duration::from_secs(1))
            .await;
        assert_eq!(claim.claimed_ids(), &[connection_id]);
        assert!(claim.close_not_queued_ids().is_empty());
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Closing
        );
        assert!(matches!(
            receiver.recv().await,
            Some(Message::Close(Some(frame)))
                if frame.code == crate::request::WsCloseReason::IdleTimeout.code()
                    && frame.reason == crate::request::WsCloseReason::IdleTimeout.reason()
        ));
        assert_eq!(
            manager
                .cleanup_inactive_connections(Duration::from_secs(1))
                .await,
            InactiveConnectionCleanupClaim::default()
        );
    }

    #[tokio::test]
    async fn heartbeat_liveness_does_not_extend_application_idle_deadline() {
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        manager
            .registry
            .write()
            .await
            .connections
            .get_mut(&connection_id)
            .unwrap()
            .last_application_activity_at = std::time::SystemTime::UNIX_EPOCH;

        manager.record_liveness(connection_id).await.unwrap();
        let info = manager.get_connection(connection_id).await.unwrap();
        assert!(info.last_liveness_at > info.last_application_activity_at);

        let claim = manager
            .cleanup_inactive_connections(Duration::from_secs(1))
            .await;
        assert_eq!(claim.claimed_ids(), &[connection_id]);
        assert!(matches!(
            receiver.recv().await,
            Some(Message::Close(Some(_)))
        ));
    }

    #[tokio::test]
    async fn idle_cleanup_control_close_bypasses_full_data_queue() {
        let manager = ConnectionManager::with_registered_namespaces(128, 128, ["orders".into()]);
        let connection_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        sender.try_send(Message::Ping(Vec::new())).unwrap();
        let (control_sender, mut control_receiver) = connection_control_channel();
        manager
            .add_connection_with_control(
                connection_id,
                sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("orders".to_string()),
            )
            .await
            .unwrap();
        manager
            .registry
            .write()
            .await
            .connections
            .get_mut(&connection_id)
            .unwrap()
            .last_application_activity_at = std::time::SystemTime::UNIX_EPOCH;

        let claim = manager
            .cleanup_inactive_connections(Duration::from_secs(1))
            .await;
        assert_eq!(claim.claimed_ids(), &[connection_id]);
        assert!(claim.close_not_queued_ids().is_empty());
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Closing
        );
        assert!(
            manager
                .get_namespace_connections("orders")
                .await
                .contains(&connection_id)
        );
        assert!(matches!(receiver.recv().await, Some(Message::Ping(_))));
        assert!(matches!(
            control_receiver.next().await,
            Ok(ConnectionControlFrame::Close(ConnectionCloseRequest {
                message: Message::Close(Some(frame)),
                category: WsConnectionCloseCategory::IdleTimeout,
            }))
                if frame.code == crate::request::WsCloseReason::IdleTimeout.code()
                    && frame.reason == crate::request::WsCloseReason::IdleTimeout.reason()
        ));
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn idle_cleanup_reports_closed_queue_without_removing_manager_entry() {
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let (control_sender, control_receiver) = connection_control_channel();
        drop(control_receiver);
        manager
            .add_connection_with_control(
                connection_id,
                sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("test".into()),
            )
            .await
            .unwrap();
        manager
            .registry
            .write()
            .await
            .connections
            .get_mut(&connection_id)
            .unwrap()
            .last_application_activity_at = std::time::SystemTime::UNIX_EPOCH;

        let claim = manager
            .cleanup_inactive_connections(Duration::from_secs(1))
            .await;
        assert_eq!(claim.claimed_ids(), &[connection_id]);
        assert_eq!(claim.close_not_queued_ids(), &[connection_id]);
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Closing
        );
    }

    #[tokio::test]
    async fn duplicate_connection_id_is_rejected_without_replacing_the_live_connection() {
        let manager = ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into(), "users".into()],
        );
        let connection_id = Uuid::new_v4();
        let (original_sender, mut original_receiver) = mpsc::channel(1);
        let (duplicate_sender, mut duplicate_receiver) = mpsc::channel(1);
        manager
            .add_connection(
                connection_id,
                original_sender,
                None,
                Some("orders".to_string()),
            )
            .await
            .unwrap();

        assert!(matches!(
            manager
                .add_connection(
                    connection_id,
                    duplicate_sender,
                    None,
                    Some("users".to_string()),
                )
                .await,
            Err(ConnectionError::InvalidOperation(
                ConnectionOperationError::DuplicateConnection {
                    connection_id: duplicate_id,
                },
            )) if duplicate_id == connection_id
        ));
        assert_eq!(manager.connection_count().await, 1);
        assert_eq!(
            manager
                .get_connection(connection_id)
                .await
                .unwrap()
                .namespace,
            "orders"
        );
        assert_eq!(
            manager.get_namespace_connections("orders").await,
            vec![connection_id]
        );
        assert!(manager.get_namespace_connections("users").await.is_empty());

        manager
            .send_to_connection("orders", connection_id, application_message())
            .await
            .unwrap();
        assert!(original_receiver.recv().await.is_some());
        assert!(matches!(
            duplicate_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}
