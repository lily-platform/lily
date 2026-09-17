use std::future::pending;
use std::net::SocketAddr;
use std::sync::Arc;

use lily_web_core::RequestExtensions;
use serde::Serialize;
use uuid::Uuid;

use crate::connection::{
    AuthenticatedWebSocketIdentity, BroadcastMessage, BroadcastTarget, CloseRequestOutcome,
    ConnectionIdentityHandle, ConnectionManager, PrincipalId, WebSocketIdentityRevision,
    WebSocketIdentitySnapshot, WebSocketIdentityUpdateOutcome,
};
use crate::request::{WsMessageBody, WsWireFormat};
use crate::{WebSocketDispatchReceipt, WebSocketDispatcher};

/// Connection-scoped capabilities exposed to a WebSocket controller action.
///
/// The context belongs to one accepted connection and one exact controller
/// namespace. It provides explicit client targeting, room membership and the
/// immutable data accepted at the HTTP upgrade boundary.
#[derive(Clone)]
pub struct WebSocketContext {
    shutdown_budget: crate::shutdown::ShutdownBudget,
    connection_id: Uuid,
    connection_manager: Arc<ConnectionManager>,
    dispatcher: Arc<WebSocketDispatcher>,
    namespace: String,
    handshake: Option<Arc<crate::server::WsHandshakeContext>>,
    identity: Option<ConnectionIdentityHandle>,
    message_identity_snapshot: Option<Option<WebSocketIdentitySnapshot>>,
    connection_locals: Arc<RequestExtensions>,
}

impl WebSocketContext {
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn new(
        connection_id: Uuid,
        connection_manager: Arc<ConnectionManager>,
        namespace: String,
    ) -> Self {
        let dispatcher = Arc::new(WebSocketDispatcher::local(Arc::clone(&connection_manager)));
        Self::new_with_dispatcher(connection_id, connection_manager, dispatcher, namespace)
    }

    pub(crate) fn new_with_dispatcher(
        connection_id: Uuid,
        connection_manager: Arc<ConnectionManager>,
        dispatcher: Arc<WebSocketDispatcher>,
        namespace: String,
    ) -> Self {
        Self {
            shutdown_budget: Default::default(),
            connection_id,
            connection_manager,
            dispatcher,
            namespace,
            handshake: None,
            identity: None,
            message_identity_snapshot: None,
            connection_locals: Arc::new(RequestExtensions::new()),
        }
    }

    pub(crate) fn with_shutdown_budget(mut self, budget: crate::shutdown::ShutdownBudget) -> Self {
        self.shutdown_budget = budget;
        self
    }

    pub(crate) fn shutdown_budget(&self) -> &crate::shutdown::ShutdownBudget {
        &self.shutdown_budget
    }

    pub(crate) fn dispatcher(&self) -> &Arc<WebSocketDispatcher> {
        &self.dispatcher
    }

    /// Attach the immutable typed state accepted for this connection.
    pub(crate) fn with_connection_locals(
        mut self,
        connection_locals: Arc<RequestExtensions>,
    ) -> Self {
        self.connection_locals = connection_locals;
        self
    }

    /// Attach immutable information accepted during the HTTP upgrade.
    pub(crate) fn with_handshake_context(
        mut self,
        handshake: Arc<crate::server::WsHandshakeContext>,
    ) -> Self {
        debug_assert_eq!(self.namespace, handshake.namespace());
        self.handshake = Some(handshake);
        self
    }

    pub(crate) fn with_identity_snapshot(
        mut self,
        identity: Option<WebSocketIdentitySnapshot>,
    ) -> Self {
        self.identity = identity.map(ConnectionIdentityHandle::new);
        self
    }

    pub(crate) fn identity_handle(&self) -> Option<ConnectionIdentityHandle> {
        self.identity.clone()
    }

    pub(crate) const fn has_identity_lifecycle(&self) -> bool {
        self.identity.is_some()
    }

    pub(crate) fn with_message_identity_snapshot(
        mut self,
        identity: Option<WebSocketIdentitySnapshot>,
    ) -> Self {
        self.message_identity_snapshot = Some(identity);
        self
    }

    /// Trusted connection identity and transport data accepted at upgrade.
    pub fn handshake(&self) -> Option<&crate::server::WsHandshakeContext> {
        self.handshake.as_deref()
    }

    /// Headers captured from the trusted HTTP upgrade boundary.
    pub fn handshake_headers(&self) -> Option<&crate::request::WsHeaders> {
        self.handshake()
            .map(crate::server::WsHandshakeContext::headers)
    }

    /// Current application-owned principal, if this connection is
    /// authenticated.
    ///
    /// The returned value is an inexpensive owned clone of the immutable
    /// principal snapshot. [`Self::handshake`] retains the original identity
    /// accepted at Upgrade for audit purposes.
    pub fn principal(&self) -> Option<lily_web_core::Principal> {
        if let Some(identity) = self.message_identity_snapshot.as_ref() {
            return identity
                .as_ref()
                .and_then(WebSocketIdentitySnapshot::principal)
                .cloned();
        }
        match self.identity_snapshot() {
            Some(snapshot) => snapshot.principal().cloned(),
            None => self
                .handshake()
                .and_then(crate::server::WsHandshakeContext::principal)
                .cloned(),
        }
    }

    /// Captures the current versioned identity for compare-and-replace
    /// re-authentication. `None` means no identity middleware was configured;
    /// `Some` with no principal means the configured identity accepted an
    /// anonymous connection. Within one inbound message this returns the
    /// immutable snapshot captured before middleware, guards, extraction, and
    /// the action began; concurrent re-authentication becomes visible only to
    /// the next message.
    #[must_use]
    pub fn identity_snapshot(&self) -> Option<WebSocketIdentitySnapshot> {
        if let Some(identity) = self.message_identity_snapshot.as_ref() {
            return identity.clone();
        }
        self.identity
            .as_ref()
            .map(ConnectionIdentityHandle::snapshot)
    }

    /// Atomically publishes an application-verified replacement identity.
    ///
    /// Callers must pass the revision they observed before performing async
    /// verification. A delayed verifier therefore cannot overwrite a newer
    /// refresh result. Lily validates bounds and owns index/timer updates, but
    /// never verifies credentials or authorization policy.
    pub async fn reauthenticate(
        &self,
        expected_revision: WebSocketIdentityRevision,
        replacement: AuthenticatedWebSocketIdentity,
    ) -> Result<WebSocketIdentityUpdateOutcome, crate::connection::ConnectionError> {
        self.connection_manager
            .reauthenticate(self.connection_id, expected_revision, replacement)
            .await
    }

    pub(crate) async fn wait_for_identity_expiry(&self) -> WebSocketIdentityRevision {
        let Some(identity) = self.identity.as_ref() else {
            return pending().await;
        };
        let mut updates = identity.subscribe();
        loop {
            let observed = updates.borrow().clone();
            let Some(deadline) = observed.expiry() else {
                if updates.changed().await.is_err() {
                    return pending().await;
                }
                continue;
            };

            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    let current = updates.borrow().clone();
                    if current.revision() == observed.revision()
                        && current
                            .expiry()
                            .is_some_and(|expiry| expiry <= tokio::time::Instant::now())
                    {
                        return observed.revision();
                    }
                }
                changed = updates.changed() => {
                    if changed.is_err() {
                        return pending().await;
                    }
                }
            }
        }
    }

    /// Negotiated WebSocket subprotocol, if one was selected.
    pub fn subprotocol(&self) -> Option<&str> {
        self.handshake()
            .and_then(crate::server::WsHandshakeContext::subprotocol)
    }

    /// Peer socket address captured by the listener.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.handshake()
            .map(crate::server::WsHandshakeContext::peer_addr)
    }

    /// Typed socket-peer and effective-client identity accepted at Upgrade.
    pub fn transport_connection_info(&self) -> Option<lily_web_core::RequestConnectionInfo> {
        self.handshake()
            .map(crate::server::WsHandshakeContext::connection_info)
    }

    /// Effective client address after trusted-proxy processing.
    pub fn client_ip(&self) -> Option<std::net::IpAddr> {
        self.handshake().and_then(|handshake| handshake.client_ip())
    }

    /// Server-owned WS/WSS transport classification, when attached.
    pub fn transport_security(&self) -> Option<crate::server::WsTransportSecurity> {
        self.handshake()
            .map(crate::server::WsHandshakeContext::transport_security)
    }

    /// Whether this connection uses Lily-terminated TLS.
    pub fn is_secure(&self) -> bool {
        self.handshake()
            .is_some_and(crate::server::WsHandshakeContext::is_secure)
    }

    /// Stable identifier of the current connection.
    pub const fn connection_id(&self) -> Uuid {
        self.connection_id
    }

    #[cfg(test)]
    pub(crate) fn connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.connection_manager
    }

    /// Exact controller namespace selected during the handshake.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Connection-lifetime typed state published at the trusted admission boundary.
    ///
    /// The map is immutable after publication. Applications that need mutable
    /// state should store an `Arc` containing their own synchronization type.
    #[must_use]
    pub fn connection_locals(&self) -> &RequestExtensions {
        &self.connection_locals
    }

    /// Build an explicit client-targeting facade.
    pub fn clients(&self) -> WebSocketClients {
        WebSocketClients {
            connection_id: self.connection_id,
            connection_manager: Arc::clone(&self.connection_manager),
            dispatcher: Arc::clone(&self.dispatcher),
            namespace: self.namespace.clone(),
        }
    }

    /// Build a room-membership facade for this connection.
    pub fn rooms(&self) -> WebSocketRooms {
        WebSocketRooms {
            connection_id: self.connection_id,
            connection_manager: Arc::clone(&self.connection_manager),
            namespace: self.namespace.clone(),
        }
    }

    /// Read the current connection manager snapshot, if still connected.
    pub async fn connection_info(&self) -> Option<crate::connection::ConnectionInfo> {
        self.connection_manager
            .get_connection(self.connection_id)
            .await
    }

    /// Requests one validated application close for this connection.
    ///
    /// The control slot is bounded and first-writer-wins. This makes the
    /// primitive safe for an application policy task racing Lily's hard identity
    /// deadline, shutdown, or an action timeout: exactly one close frame is
    /// retained and the outcome reports whether this call selected it. Lily
    /// schedules the optional deadline supplied through
    /// [`AuthenticatedWebSocketIdentity`]; all other token/session policy remains
    /// application-owned.
    pub async fn close(
        &self,
        close: crate::CloseConnection,
    ) -> Result<CloseRequestOutcome, crate::connection::ConnectionError> {
        self.connection_manager
            .request_close_frame(
                self.connection_id,
                Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: close.code().into(),
                    reason: close.reason().to_owned().into(),
                }),
                crate::middleware::WsConnectionCloseCategory::Application,
            )
            .await
    }

    pub(crate) async fn request_close(
        &self,
        reason: crate::request::WsCloseReason,
        category: crate::middleware::WsConnectionCloseCategory,
    ) -> Result<(), crate::connection::ConnectionError> {
        self.connection_manager
            .request_close_frame(self.connection_id, Some(reason.frame()), category)
            .await
            .map(|_| ())
    }
}

/// Explicit client and room targeting rooted in one controller namespace.
pub struct WebSocketClients {
    connection_id: Uuid,
    connection_manager: Arc<ConnectionManager>,
    dispatcher: Arc<WebSocketDispatcher>,
    namespace: String,
}

impl WebSocketClients {
    /// Target the current connection inside this namespace.
    pub fn caller(&self) -> ClientProxy {
        self.client(self.connection_id)
    }

    /// Target every connection in this namespace except the caller.
    pub fn others(&self) -> ClientProxy {
        ClientProxy {
            target: BroadcastTarget::Namespace(self.namespace.clone()),
            dispatcher: Arc::clone(&self.dispatcher),
            exclude: vec![self.connection_id],
        }
    }

    /// Target every connection in this namespace, including the caller.
    pub fn all(&self) -> ClientProxy {
        ClientProxy {
            target: BroadcastTarget::Namespace(self.namespace.clone()),
            dispatcher: Arc::clone(&self.dispatcher),
            exclude: Vec::new(),
        }
    }

    /// Target one explicit connection inside this namespace.
    pub fn client(&self, connection_id: Uuid) -> ClientProxy {
        self.clients(vec![connection_id])
    }

    /// Target an explicit set of connections inside this namespace.
    /// IDs registered in other namespaces are reported as missing targets.
    /// An empty selection is a successful no-op after message validation.
    pub fn clients(&self, connection_ids: Vec<Uuid>) -> ClientProxy {
        ClientProxy {
            target: BroadcastTarget::NamespaceConnections {
                namespace: self.namespace.clone(),
                connection_ids,
            },
            dispatcher: Arc::clone(&self.dispatcher),
            exclude: Vec::new(),
        }
    }

    /// Target every online device currently bound to one principal inside the
    /// current controller namespace.
    ///
    /// The application remains responsible for deciding whether the caller may
    /// address this principal. Lily performs no role, tenant, or relationship
    /// authorization inference.
    pub fn principal(&self, principal_id: PrincipalId) -> ClientProxy {
        ClientProxy {
            target: BroadcastTarget::Principal {
                namespace: self.namespace.clone(),
                principal_id,
            },
            dispatcher: Arc::clone(&self.dispatcher),
            exclude: Vec::new(),
        }
    }

    /// Target one room inside the current controller namespace.
    pub fn room(&self, room_name: &str) -> ClientProxy {
        ClientProxy {
            target: BroadcastTarget::Room {
                namespace: self.namespace.clone(),
                room: room_name.to_owned(),
            },
            dispatcher: Arc::clone(&self.dispatcher),
            exclude: Vec::new(),
        }
    }

    /// Target the deterministic union of multiple rooms in this namespace.
    /// An empty list is a successful no-op after message validation.
    pub fn rooms(&self, room_names: Vec<String>) -> ClientProxy {
        let target = if room_names.is_empty() {
            BroadcastTarget::NamespaceConnections {
                namespace: self.namespace.clone(),
                connection_ids: Vec::new(),
            }
        } else {
            BroadcastTarget::Rooms {
                namespace: self.namespace.clone(),
                rooms: room_names,
            }
        };

        ClientProxy {
            target,
            dispatcher: Arc::clone(&self.dispatcher),
            exclude: Vec::new(),
        }
    }

    /// Current number of published connections in this exact namespace on this node.
    ///
    /// Includes the caller while registered and closing connections until removal.
    /// A backplane does not extend this snapshot to other nodes. For the local
    /// application total across namespaces, use [`crate::WsApp::active_connection_count`].
    pub async fn count(&self) -> usize {
        self.connection_manager
            .namespace_connection_count(&self.namespace)
            .await
    }
}

/// Room-membership operations for the current or an explicit connection.
pub struct WebSocketRooms {
    connection_id: Uuid,
    connection_manager: Arc<ConnectionManager>,
    namespace: String,
}

impl WebSocketRooms {
    /// Snapshot an existing room in this controller namespace on this node.
    /// `None` means the room entry is absent; member order is unspecified.
    pub async fn snapshot(&self, room_name: &str) -> Option<crate::WebSocketRoomSnapshot> {
        self.connection_manager
            .get_room_snapshot(&self.namespace, room_name)
            .await
    }

    /// Add the current connection to a room.
    pub async fn join(&self, room_name: &str) -> Result<(), crate::connection::ConnectionError> {
        self.connection_manager
            .join_room(&self.namespace, self.connection_id, room_name)
            .await
    }

    /// Remove the current connection from a room.
    pub async fn leave(&self, room_name: &str) -> Result<(), crate::connection::ConnectionError> {
        self.connection_manager
            .leave_room(&self.namespace, self.connection_id, room_name)
            .await
    }

    /// Add an explicit connection in this namespace to a room.
    pub async fn join_connection(
        &self,
        connection_id: Uuid,
        room_name: &str,
    ) -> Result<(), crate::connection::ConnectionError> {
        self.connection_manager
            .join_room(&self.namespace, connection_id, room_name)
            .await
    }

    /// Remove an explicit connection in this namespace from a room.
    pub async fn leave_connection(
        &self,
        connection_id: Uuid,
        room_name: &str,
    ) -> Result<(), crate::connection::ConnectionError> {
        self.connection_manager
            .leave_room(&self.namespace, connection_id, room_name)
            .await
    }
}

/// Fluent explicit target for controller-initiated outbound events.
///
/// During drain, await sends within a live Lily callback. Keeping this proxy
/// does not retain its callback's outbound authority; raw spawned tasks do not
/// inherit it. Success reports queue/backplane acceptance, not peer delivery.
pub struct ClientProxy {
    target: BroadcastTarget,
    dispatcher: Arc<WebSocketDispatcher>,
    exclude: Vec<Uuid>,
}

impl ClientProxy {
    fn require_full_acceptance(
        receipt: WebSocketDispatchReceipt,
    ) -> Result<(), crate::connection::ConnectionError> {
        if receipt.is_fully_accepted() {
            Ok(())
        } else {
            Err(crate::connection::ConnectionError::PartialBroadcast {
                report: *receipt.local(),
            })
        }
    }

    /// Exclude explicit connection IDs from the selected target.
    pub fn except(mut self, connection_ids: Vec<Uuid>) -> Self {
        self.exclude.extend(connection_ids);
        self
    }

    /// Send a serializable value in the canonical text envelope.
    pub async fn send<T: Serialize>(
        &self,
        event: &str,
        data: T,
    ) -> Result<(), crate::connection::ConnectionError> {
        let receipt = self
            .send_with_format(event, data, WsWireFormat::Text)
            .await?;
        Self::require_full_acceptance(receipt)
    }

    /// Sends a serializable value and returns separate local/backplane accounting.
    pub async fn send_with_receipt<T: Serialize>(
        &self,
        event: &str,
        data: T,
    ) -> Result<WebSocketDispatchReceipt, crate::connection::ConnectionError> {
        self.send_with_format(event, data, WsWireFormat::Text).await
    }

    /// Send the canonical envelope in a binary WebSocket frame.
    ///
    /// This changes only the transport frame. Typed binary application data
    /// is represented independently by [`crate::BinaryPayload`] and the
    /// selected payload codec.
    pub async fn send_binary<T: Serialize>(
        &self,
        event: &str,
        data: T,
    ) -> Result<(), crate::connection::ConnectionError> {
        let receipt = self
            .send_with_format(event, data, WsWireFormat::Binary)
            .await?;
        Self::require_full_acceptance(receipt)
    }

    /// Sends a binary WebSocket frame and returns local/backplane accounting.
    pub async fn send_binary_with_receipt<T: Serialize>(
        &self,
        event: &str,
        data: T,
    ) -> Result<WebSocketDispatchReceipt, crate::connection::ConnectionError> {
        self.send_with_format(event, data, WsWireFormat::Binary)
            .await
    }

    async fn send_with_format<T: Serialize>(
        &self,
        event: &str,
        data: T,
        wire_format: WsWireFormat,
    ) -> Result<WebSocketDispatchReceipt, crate::connection::ConnectionError> {
        let message = WsMessageBody::try_new(event, data)
            .map_err(crate::connection::ConnectionError::Serialization)?;

        self.dispatcher
            .dispatch(BroadcastMessage {
                target: self.target.clone(),
                message,
                wire_format,
                exclude: self.exclude.clone(),
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{ConnectionControlReceiver, connection_control_channel};
    use crate::server::WsTransportSecurity;
    use lily_web_core::{Principal, RequestConnectionInfo};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn client_count_tracks_only_its_namespace_until_registry_removal() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            64,
            [
                "orders".to_owned(),
                "orders-admin".to_owned(),
                "empty".to_owned(),
            ],
        ));
        let caller = Uuid::new_v4();
        let peer = Uuid::new_v4();
        let foreign = Uuid::new_v4();
        let context = WebSocketContext::new(caller, Arc::clone(&manager), "orders".to_owned());
        let clients = context.clients();
        let foreign_clients =
            WebSocketContext::new(foreign, Arc::clone(&manager), "orders-admin".to_owned())
                .clients();
        let empty_clients =
            WebSocketContext::new(Uuid::new_v4(), Arc::clone(&manager), "empty".to_owned())
                .clients();
        assert_eq!(clients.count().await, 0);
        assert_eq!(foreign_clients.count().await, 0);
        assert_eq!(manager.connection_count().await, 0);

        let mut receivers = Vec::new();
        for (id, namespace, expected_orders, expected_foreign) in [
            (caller, "orders", 1, 0),
            (peer, "orders", 2, 0),
            (foreign, "orders-admin", 2, 1),
        ] {
            let (sender, receiver) = mpsc::channel(1);
            manager
                .add_connection(id, sender, None, Some(namespace.to_owned()))
                .await
                .unwrap();
            receivers.push(receiver);
            assert_eq!(clients.count().await, expected_orders);
            assert_eq!(foreign_clients.count().await, expected_foreign);
            assert_eq!(empty_clients.count().await, 0);
            assert_eq!(
                manager.connection_count().await,
                expected_orders + expected_foreign
            );
        }

        // Closing transports remain published until lifecycle cleanup removes
        // them, just as they do in the application-wide registry count.
        context
            .request_close(
                crate::request::WsCloseReason::Application,
                crate::middleware::WsConnectionCloseCategory::Application,
            )
            .await
            .unwrap();
        assert_eq!(
            manager.get_connection(caller).await.unwrap().state,
            crate::ConnectionState::Closing
        );
        assert_eq!(clients.count().await, 2);
        assert_eq!(manager.connection_count().await, 3);

        for (id, expected_orders, expected_foreign) in
            [(peer, 1, 1), (foreign, 1, 0), (caller, 0, 0)]
        {
            manager.remove_connection(id).await.unwrap();
            assert_eq!(clients.count().await, expected_orders);
            assert_eq!(foreign_clients.count().await, expected_foreign);
            assert_eq!(empty_clients.count().await, 0);
            assert_eq!(
                manager.connection_count().await,
                expected_orders + expected_foreign
            );
        }
    }

    #[tokio::test]
    async fn explicit_client_proxies_enforce_namespace_and_preserve_target_accounting() {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            64,
            ["orders".to_owned(), "billing".to_owned()],
        ));
        let caller = Uuid::new_v4();
        let peer = Uuid::new_v4();
        let foreign = Uuid::new_v4();
        let missing = Uuid::new_v4();
        let mut receivers = Vec::new();
        for (id, namespace) in [(caller, "orders"), (peer, "orders"), (foreign, "billing")] {
            let (sender, receiver) = mpsc::channel(8);
            manager
                .add_connection(id, sender, None, Some(namespace.to_owned()))
                .await
                .unwrap();
            receivers.push(receiver);
        }
        let context = WebSocketContext::new(caller, Arc::clone(&manager), "orders".to_owned());
        let clients = context.clients();

        for wire_format in [WsWireFormat::Text, WsWireFormat::Binary] {
            for (proxy, recipient, targeted, missing_count) in [
                (clients.caller(), Some(0), 1, 0),
                (clients.client(peer), Some(1), 1, 0),
                (
                    clients
                        .clients(vec![caller, peer, foreign, missing, peer])
                        .except(vec![caller, caller]),
                    Some(1),
                    3,
                    2,
                ),
                (clients.client(foreign), None, 1, 1),
            ] {
                let receipt = proxy
                    .send_with_format("orders:updated", (), wire_format)
                    .await
                    .unwrap();
                assert_eq!(
                    *receipt.local(),
                    crate::BroadcastReport {
                        targeted,
                        sent: usize::from(recipient.is_some()),
                        missing: missing_count,
                        ..Default::default()
                    }
                );
                assert!(receipt.local().is_balanced());
                for (index, receiver) in receivers.iter_mut().enumerate() {
                    if recipient == Some(index) {
                        let frame = receiver
                            .try_recv()
                            .expect("selected in-namespace peer receives one frame");
                        assert_eq!(
                            matches!(frame, Message::Binary(_)),
                            wire_format == WsWireFormat::Binary
                        );
                    }
                    assert!(matches!(
                        receiver.try_recv(),
                        Err(mpsc::error::TryRecvError::Empty)
                    ));
                }
            }
        }

        assert!(matches!(
            clients.client(foreign).send("orders:updated", ()).await,
            Err(crate::ConnectionError::PartialBroadcast { report }) if report.missing == 1 && report.sent == 0
        ));
        for proxy in [clients.clients(Vec::new()), clients.rooms(Vec::new())] {
            let receipt = proxy.send_with_receipt("orders:updated", ()).await.unwrap();
            assert_eq!(*receipt.local(), crate::BroadcastReport::default());
            assert_eq!(
                receipt.backplane(),
                crate::WebSocketBackplaneDispatchReceipt::NoTargets
            );
            assert!(receipt.is_fully_accepted());
            proxy.send("orders:updated", ()).await.unwrap();
        }

        // A retained caller proxy must keep its original namespace even if an
        // operational caller removes and reuses the UUID for another transport.
        let retained_caller = clients.caller();
        manager.remove_connection(caller).await.unwrap();
        let (replacement_sender, mut replacement_receiver) = mpsc::channel(1);
        manager
            .add_connection(caller, replacement_sender, None, Some("billing".to_owned()))
            .await
            .unwrap();
        let receipt = retained_caller
            .send_with_receipt("orders:updated", ())
            .await
            .unwrap();
        assert_eq!(receipt.local().sent, 0);
        assert_eq!(receipt.local().missing, 1);
        assert!(matches!(
            replacement_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    fn authenticated_identity(
        subject: &str,
        expiry: Option<tokio::time::Instant>,
    ) -> AuthenticatedWebSocketIdentity {
        let identity = AuthenticatedWebSocketIdentity::try_new(Principal::new(
            subject,
            [],
            [],
            serde_json::Map::new(),
        ))
        .expect("bounded test identity");
        match expiry {
            Some(deadline) => identity.expires_at(deadline),
            None => identity,
        }
    }

    async fn registered_identity_context(
        identity: AuthenticatedWebSocketIdentity,
    ) -> (
        Arc<ConnectionManager>,
        Arc<WebSocketContext>,
        mpsc::Receiver<Message>,
        ConnectionControlReceiver,
    ) {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces_and_identity(
            8,
            64,
            ["chat".to_owned()],
            true,
        ));
        let connection_id = Uuid::new_v4();
        let context = Arc::new(
            WebSocketContext::new(connection_id, Arc::clone(&manager), "chat".to_owned())
                .with_identity_snapshot(Some(WebSocketIdentitySnapshot::authenticated(identity))),
        );
        let (data_tx, data_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = connection_control_channel();
        manager
            .add_connection_with_control_and_identity(
                connection_id,
                data_tx,
                control_tx,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("chat".to_owned()),
                context.identity_handle(),
            )
            .await
            .expect("register identity-enabled connection");
        (manager, context, data_rx, control_rx)
    }

    #[tokio::test]
    async fn message_snapshot_stays_stable_and_the_next_message_observes_reauthentication() {
        let (manager, context, _data_rx, _control_rx) =
            registered_identity_context(authenticated_identity("subject-old", None)).await;
        assert!(context.has_identity_lifecycle());
        let original = context.identity_snapshot().expect("identity snapshot");
        let current_message = context
            .as_ref()
            .clone()
            .with_message_identity_snapshot(Some(original.clone()));

        let outcome = context
            .reauthenticate(
                original.revision(),
                authenticated_identity("subject-new", None),
            )
            .await
            .expect("publish verified replacement");
        let WebSocketIdentityUpdateOutcome::Updated(updated_revision) = outcome else {
            panic!("the first compare-and-replace update must succeed");
        };

        assert_eq!(
            current_message
                .principal()
                .map(|principal| principal.subject().to_owned()),
            Some("subject-old".to_owned())
        );
        assert_eq!(
            current_message
                .identity_snapshot()
                .expect("frozen message identity")
                .revision(),
            original.revision()
        );
        assert_eq!(
            context
                .principal()
                .map(|principal| principal.subject().to_owned()),
            Some("subject-new".to_owned())
        );
        let next_message = context
            .as_ref()
            .clone()
            .with_message_identity_snapshot(context.identity_snapshot());
        assert_eq!(
            next_message
                .principal()
                .map(|principal| principal.subject().to_owned()),
            Some("subject-new".to_owned())
        );
        assert_eq!(
            next_message
                .identity_snapshot()
                .expect("next message identity")
                .revision(),
            updated_revision
        );

        assert_eq!(
            context
                .reauthenticate(
                    original.revision(),
                    authenticated_identity("stale-subject", None),
                )
                .await
                .expect("stale replacement is a typed outcome"),
            WebSocketIdentityUpdateOutcome::Stale(updated_revision)
        );
        manager
            .remove_connection(context.connection_id())
            .await
            .expect("remove fixture connection");
    }

    #[tokio::test]
    async fn expiry_wait_supersedes_old_revisions_and_returns_the_newest_deadline() {
        let first_deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let (manager, context, _data_rx, _control_rx) = registered_identity_context(
            authenticated_identity("subject-initial", Some(first_deadline)),
        )
        .await;
        let first_revision = context.identity_snapshot().unwrap().revision();
        let mut expiry_wait = Box::pin(context.wait_for_identity_expiry());

        // Poll the waiter once so it is subscribed to the initial generation.
        assert!(
            tokio::time::timeout(Duration::from_millis(1), expiry_wait.as_mut())
                .await
                .is_err()
        );
        let long_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let WebSocketIdentityUpdateOutcome::Updated(long_revision) = context
            .reauthenticate(
                first_revision,
                authenticated_identity("subject-refreshed", Some(long_deadline)),
            )
            .await
            .expect("supersede the first deadline")
        else {
            panic!("refresh must publish a new identity revision");
        };

        // The original 200ms deadline passes while the replacement remains
        // valid. A revision-aware waiter must not report the stale deadline.
        assert!(
            tokio::time::timeout(Duration::from_millis(250), expiry_wait.as_mut())
                .await
                .is_err()
        );
        let final_deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let WebSocketIdentityUpdateOutcome::Updated(final_revision) = context
            .reauthenticate(
                long_revision,
                authenticated_identity("subject-final", Some(final_deadline)),
            )
            .await
            .expect("publish the final deadline")
        else {
            panic!("second refresh must publish a new identity revision");
        };
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), expiry_wait.as_mut())
                .await
                .expect("final identity deadline"),
            final_revision
        );

        manager
            .remove_connection(context.connection_id())
            .await
            .expect("remove fixture connection");
    }

    #[test]
    fn identity_capability_is_absent_without_the_identity_middleware_slot() {
        let context = WebSocketContext::new(
            Uuid::new_v4(),
            Arc::new(ConnectionManager::new()),
            "chat".to_owned(),
        );

        assert!(!context.has_identity_lifecycle());
    }
}
