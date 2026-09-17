//! Deterministic supplements to the public local-delivery-rooms suite.
//! Validation tests enforce the approved shared DEC-004 broadcast contract.

use super::*;
use crate::{ClientProxy, WebSocketContext, WebSocketDispatchReceipt, WebSocketDispatcher};
use futures_util::poll;
use lily_web_core::Principal;
use serde_json::json;
use std::time::Duration;

struct Fixture {
    manager: Arc<ConnectionManager>,
    context: WebSocketContext,
    id: Uuid,
    receiver: mpsc::Receiver<QueuedApplicationFrame>,
    control: ConnectionControlReceiver,
    identity: ConnectionIdentityHandle,
    budget: Arc<OutboundByteBudget>,
}

fn identity(subject: &str) -> AuthenticatedWebSocketIdentity {
    AuthenticatedWebSocketIdentity::try_new(Principal::new(
        subject,
        Vec::<String>::new(),
        Vec::<String>::new(),
        serde_json::Map::new(),
    ))
    .unwrap()
}

impl Fixture {
    async fn new(max_message: usize, expiry: Option<TokioInstant>) -> Self {
        let manager = Arc::new(
            ConnectionManager::with_registered_namespaces_and_identity_and_outbound_policy(
                8,
                64,
                ["orders".to_owned(), "billing".to_owned()],
                true,
                max_message,
                16384,
                Duration::from_secs(60),
            ),
        );
        let id = Uuid::new_v4();
        let mut authenticated = identity("account-old");
        if let Some(expiry) = expiry {
            authenticated = authenticated.expires_at(expiry);
        }
        let identity =
            ConnectionIdentityHandle::new(WebSocketIdentitySnapshot::authenticated(authenticated));
        let (sender, receiver) = mpsc::channel::<QueuedApplicationFrame>(1);
        let (control_sender, control) = connection_control_channel();
        manager
            .add_connection_with_control_and_identity(
                id,
                sender,
                control_sender,
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("orders".to_owned()),
                Some(identity.clone()),
            )
            .await
            .unwrap();
        let budget = Arc::clone(&manager.registry.read().await.connections[&id].outbound_budget);
        let context = WebSocketContext::new(id, Arc::clone(&manager), "orders".to_owned());
        Self {
            manager,
            context,
            id,
            receiver,
            control,
            identity,
            budget,
        }
    }

    async fn fill_queue(&self) {
        self.manager
            .send_application_frame(
                "orders",
                self.id,
                Message::Text("queued-before-selection".into()),
            )
            .await
            .unwrap();
    }

    fn release_queue(&mut self) {
        let queued = self
            .receiver
            .try_recv()
            .expect("the pre-selection frame fills the queue");
        assert_eq!(
            queued.message(),
            &Message::Text("queued-before-selection".into())
        );
        drop(queued);
    }

    async fn assert_connected(&self) {
        assert_eq!(
            self.manager.get_connection(self.id).await.unwrap().state,
            ConnectionState::Connected
        );
        assert!(!self.budget.is_closed());
        assert!(self.control.close.borrow().is_none());
    }
}

fn command(target: BroadcastTarget, wire_format: WsWireFormat) -> BroadcastMessage {
    BroadcastMessage {
        target,
        wire_format,
        exclude: vec![],
        message: WsMessageBody::try_new("orders:probe", json!({"value": 1})).unwrap(),
    }
}

async fn send(
    proxy: &ClientProxy,
    wire: WsWireFormat,
) -> Result<WebSocketDispatchReceipt, ConnectionError> {
    match wire {
        WsWireFormat::Text => {
            proxy
                .send_with_receipt("orders:probe", json!({"value": 1}))
                .await
        }
        WsWireFormat::Binary => {
            proxy
                .send_binary_with_receipt("orders:probe", json!({"value": 1}))
                .await
        }
    }
}

fn assert_invalid_target<T>(result: Result<T, ConnectionError>) {
    assert!(matches!(
        result,
        Err(ConnectionError::InvalidOperation(
            ConnectionOperationError::InvalidBackplaneTarget
        ))
    ));
}

#[tokio::test]
async fn dec_004_manager_dispatcher_and_facade_share_validation() {
    let mut fixture = Fixture::new(4096, None).await;
    fixture
        .manager
        .join_room("orders", fixture.id, "valid.room")
        .await
        .unwrap();
    let dispatcher = WebSocketDispatcher::local(Arc::clone(&fixture.manager));
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        for room in ["".to_owned(), "bad room".to_owned(), "x".repeat(65)] {
            let input = command(
                BroadcastTarget::Room {
                    namespace: "orders".into(),
                    room: room.clone(),
                },
                wire,
            );
            assert_invalid_target(fixture.manager.broadcast(input.clone()).await);
            assert_invalid_target(dispatcher.dispatch(input).await);
            assert_invalid_target(send(&fixture.context.clients().room(&room), wire).await);
        }
        // A malformed namespace is representable in a low-level target, but
        // cannot originate from an app-created, canonical controller context.
        let input = command(BroadcastTarget::Namespace("bad namespace".into()), wire);
        assert_invalid_target(fixture.manager.broadcast(input.clone()).await);
        assert_invalid_target(dispatcher.dispatch(input).await);
        assert_eq!(fixture.context.namespace(), "orders");
        assert!(matches!(
            fixture.receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        // Valid but absent scopes and explicit empty selections are not errors.
        for target in [
            BroadcastTarget::Namespace("unregistered".into()),
            BroadcastTarget::Room {
                namespace: "orders".into(),
                room: "absent.room".into(),
            },
            BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: vec![],
            },
            BroadcastTarget::Rooms {
                namespace: "orders".into(),
                rooms: vec![],
            },
        ] {
            let input = command(target, wire);
            assert_eq!(
                fixture.manager.broadcast(input.clone()).await.unwrap(),
                BroadcastReport::default()
            );
            assert_eq!(
                *dispatcher.dispatch(input).await.unwrap().local(),
                BroadcastReport::default()
            );
        }
        for proxy in [
            fixture.context.clients().room("absent.room"),
            fixture.context.clients().clients(vec![]),
            fixture.context.clients().rooms(vec![]),
        ] {
            assert_eq!(
                *send(&proxy, wire).await.unwrap().local(),
                BroadcastReport::default()
            );
        }

        // Positive controls use the same managed transport and a real room.
        let target = BroadcastTarget::Room {
            namespace: "orders".into(),
            room: "valid.room".into(),
        };
        assert_eq!(
            fixture
                .manager
                .broadcast(command(target.clone(), wire))
                .await
                .unwrap()
                .sent,
            1
        );
        drop(fixture.receiver.try_recv().unwrap());
        assert_eq!(
            dispatcher
                .dispatch(command(target, wire))
                .await
                .unwrap()
                .local()
                .sent,
            1
        );
        drop(fixture.receiver.try_recv().unwrap());
        assert_eq!(
            send(&fixture.context.clients().room("valid.room"), wire)
                .await
                .unwrap()
                .local()
                .sent,
            1
        );
        drop(fixture.receiver.try_recv().unwrap());
    }

    // All public surfaces enforce the same encoded message limit.
    let tiny = Fixture::new(1, None).await;
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        let input = command(
            BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: vec![tiny.id],
            },
            wire,
        );
        let manager_error = tiny.manager.broadcast(input.clone()).await.unwrap_err();
        let dispatcher_error = WebSocketDispatcher::local(Arc::clone(&tiny.manager))
            .dispatch(input)
            .await
            .unwrap_err();
        let facade_error = send(&tiny.context.clients().caller(), wire)
            .await
            .unwrap_err();
        for error in [manager_error, dispatcher_error, facade_error] {
            assert!(matches!(
                error,
                ConnectionError::InvalidOperation(
                    ConnectionOperationError::OutboundMessageTooLarge
                )
            ));
        }
        assert_eq!(tiny.budget.used_bytes(), 0);
    }
}

#[tokio::test]
async fn dec_004_raw_list_limits_precede_deduplication_on_every_surface() {
    let mut fixture = Fixture::new(4096, None).await;
    fixture
        .manager
        .join_room("orders", fixture.id, "valid.room")
        .await
        .unwrap();
    let dispatcher = WebSocketDispatcher::local(Arc::clone(&fixture.manager));
    let excluded = Uuid::new_v4();
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        for size in [255, 256, 257] {
            for kind in ["namespace_connections", "rooms", "exclusions"] {
                let (mut input, proxy) = match kind {
                    "rooms" => (
                        command(
                            BroadcastTarget::Rooms {
                                namespace: "orders".into(),
                                rooms: vec!["valid.room".into(); size],
                            },
                            wire,
                        ),
                        fixture
                            .context
                            .clients()
                            .rooms(vec!["valid.room".into(); size]),
                    ),
                    "namespace_connections" => (
                        command(
                            BroadcastTarget::NamespaceConnections {
                                namespace: "orders".into(),
                                connection_ids: vec![fixture.id; size],
                            },
                            wire,
                        ),
                        fixture.context.clients().clients(vec![fixture.id; size]),
                    ),
                    "exclusions" => (
                        command(
                            BroadcastTarget::NamespaceConnections {
                                namespace: "orders".to_owned(),
                                connection_ids: vec![fixture.id],
                            },
                            wire,
                        ),
                        fixture
                            .context
                            .clients()
                            .caller()
                            .except(vec![excluded; size]),
                    ),
                    _ => unreachable!(),
                };
                if kind == "exclusions" {
                    input.exclude = vec![excluded; size];
                }
                for surface in ["manager", "dispatcher", "facade"] {
                    let result = match surface {
                        "manager" => fixture.manager.broadcast(input.clone()).await,
                        "dispatcher" => dispatcher
                            .dispatch(input.clone())
                            .await
                            .map(|receipt| *receipt.local()),
                        "facade" => send(&proxy, wire).await.map(|receipt| *receipt.local()),
                        _ => unreachable!(),
                    };
                    if size > 256 {
                        let expected = if kind == "exclusions" {
                            ConnectionOperationError::BackplaneExclusionLimitExceeded
                        } else {
                            ConnectionOperationError::BackplaneTargetLimitExceeded
                        };
                        assert!(
                            matches!(result, Err(ConnectionError::InvalidOperation(actual)) if actual == expected),
                            "{surface}/{kind}/{size}"
                        );
                    } else {
                        assert_eq!(
                            result.unwrap(),
                            BroadcastReport {
                                targeted: 1,
                                sent: 1,
                                ..Default::default()
                            },
                            "{surface}/{kind}/{size}"
                        );
                        let frame = fixture.receiver.try_recv().unwrap();
                        assert_eq!(
                            matches!(frame.message(), Message::Binary(_)),
                            wire == WsWireFormat::Binary
                        );
                        drop(frame);
                    }
                    assert_eq!(fixture.budget.used_bytes(), 0);
                    assert!(matches!(
                        fixture.receiver.try_recv(),
                        Err(mpsc::error::TryRecvError::Empty)
                    ));
                }
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn principal_recheck_after_selection_rejects_reauthenticated_target() {
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        let mut fixture = Fixture::new(4096, None).await;
        fixture.fill_queue().await;
        let before = fixture.budget.used_bytes();
        let proxy = fixture
            .context
            .clients()
            .principal(PrincipalId::try_new("account-old").unwrap());
        let pending = send(&proxy, wire);
        tokio::pin!(pending);
        assert!(poll!(&mut pending).is_pending());
        // The broadcast has selected this exact transport and reserved bytes;
        // its only blocked step is the full managed channel's queue permit.
        assert!(fixture.budget.used_bytes() > before);
        let observed = fixture.identity.snapshot().revision();
        assert!(matches!(
            fixture
                .manager
                .reauthenticate(fixture.id, observed, identity("account-new"))
                .await
                .unwrap(),
            WebSocketIdentityUpdateOutcome::Updated(_)
        ));
        fixture.release_queue();
        let receipt = pending.await.unwrap();
        assert_eq!(
            *receipt.local(),
            BroadcastReport {
                targeted: 1,
                missing: 1,
                ..Default::default()
            }
        );
        assert!(!receipt.is_fully_accepted());
        assert!(matches!(
            fixture.receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(fixture.budget.used_bytes(), 0);
        fixture.assert_connected().await;

        let new_proxy = fixture
            .context
            .clients()
            .principal(PrincipalId::try_new("account-new").unwrap());
        assert_eq!(send(&new_proxy, wire).await.unwrap().local().sent, 1);
        drop(fixture.receiver.try_recv().unwrap());
    }
}

#[tokio::test(start_paused = true)]
async fn expiry_recheck_after_selection_suppresses_frame_at_exact_deadline() {
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        let expiry = TokioInstant::now() + Duration::from_secs(5);
        let mut fixture = Fixture::new(4096, Some(expiry)).await;
        fixture.fill_queue().await;
        let before = fixture.budget.used_bytes();
        let proxy = fixture
            .context
            .clients()
            .principal(PrincipalId::try_new("account-old").unwrap());
        let pending = send(&proxy, wire);
        tokio::pin!(pending);
        assert!(poll!(&mut pending).is_pending());
        assert!(fixture.budget.used_bytes() > before);
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(TokioInstant::now(), expiry);
        // No lifecycle expiry task is running: admission itself must recheck.
        fixture.assert_connected().await;
        fixture.release_queue();
        let receipt = pending.await.unwrap();
        assert_eq!(
            *receipt.local(),
            BroadcastReport {
                targeted: 1,
                closed: 1,
                ..Default::default()
            }
        );
        assert!(!receipt.is_fully_accepted());
        assert!(matches!(
            fixture.receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(fixture.budget.used_bytes(), 0);
        assert_eq!(
            fixture
                .manager
                .get_connection(fixture.id)
                .await
                .unwrap()
                .state,
            ConnectionState::Closing
        );
        assert_eq!(
            fixture.control.close.borrow().as_ref().unwrap().category,
            WsConnectionCloseCategory::IdentityExpired
        );
        let registry = fixture.manager.registry.read().await;
        assert!(
            !registry
                .principal_index
                .as_ref()
                .unwrap()
                .values()
                .any(|ids| ids.contains(&fixture.id))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn expiry_recheck_uses_renewed_identity_instead_of_selected_revision() {
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        let start = TokioInstant::now();
        let mut fixture = Fixture::new(4096, Some(start + Duration::from_secs(5))).await;
        fixture.fill_queue().await;
        let before = fixture.budget.used_bytes();
        let proxy = fixture
            .context
            .clients()
            .principal(PrincipalId::try_new("account-old").unwrap());
        let pending = send(&proxy, wire);
        tokio::pin!(pending);
        assert!(poll!(&mut pending).is_pending());
        assert!(fixture.budget.used_bytes() > before);
        let revision = fixture.identity.snapshot().revision();
        assert!(matches!(
            fixture
                .manager
                .reauthenticate(
                    fixture.id,
                    revision,
                    identity("account-old").expires_at(start + Duration::from_secs(30)),
                )
                .await
                .unwrap(),
            WebSocketIdentityUpdateOutcome::Updated(_)
        ));
        tokio::time::advance(Duration::from_secs(5)).await;
        fixture.release_queue();
        let receipt = pending.await.unwrap();
        assert_eq!(
            *receipt.local(),
            BroadcastReport {
                targeted: 1,
                sent: 1,
                ..Default::default()
            }
        );
        assert!(receipt.is_fully_accepted());
        let frame = fixture.receiver.try_recv().unwrap();
        assert_eq!(
            matches!(frame.message(), Message::Binary(_)),
            wire == WsWireFormat::Binary
        );
        drop(frame);
        assert_eq!(fixture.budget.used_bytes(), 0);
        fixture.assert_connected().await;
    }
}

#[tokio::test(start_paused = true)]
async fn connected_registry_with_closed_data_receiver_has_typed_terminal_results() {
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        let mut fixture = Fixture::new(4096, None).await;
        fixture.receiver.close();
        fixture.assert_connected().await;
        let message = command(
            BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: vec![fixture.id],
            },
            wire,
        )
        .message;
        let error = match wire {
            WsWireFormat::Text => {
                fixture
                    .manager
                    .send_to_connection("orders", fixture.id, message)
                    .await
            }
            WsWireFormat::Binary => {
                fixture
                    .manager
                    .send_binary_to_connection("orders", fixture.id, message)
                    .await
            }
        }
        .unwrap_err();
        assert!(
            matches!(error, ConnectionError::ConnectionClosed { connection_id } if connection_id == fixture.id)
        );
        let expected = BroadcastReport {
            targeted: 1,
            closed: 1,
            ..Default::default()
        };
        assert_eq!(
            fixture
                .manager
                .broadcast(command(
                    BroadcastTarget::NamespaceConnections {
                        namespace: "orders".to_owned(),
                        connection_ids: vec![fixture.id]
                    },
                    wire
                ))
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            *send(&fixture.context.clients().caller(), wire)
                .await
                .unwrap()
                .local(),
            expected
        );
        assert_eq!(fixture.budget.used_bytes(), 0);
        // A channel failure reports closure without taking registry-removal
        // ownership away from the lifecycle.
        fixture.assert_connected().await;
        fixture.manager.remove_connection(fixture.id).await.unwrap();
        assert!(fixture.manager.get_connection(fixture.id).await.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn data_receiver_close_releases_pending_admission_without_timeout() {
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        let mut fixture = Fixture::new(4096, None).await;
        fixture.fill_queue().await;
        let before = fixture.budget.used_bytes();
        let proxy = fixture.context.clients().caller();
        let pending = send(&proxy, wire);
        tokio::pin!(pending);
        assert!(poll!(&mut pending).is_pending());
        assert!(fixture.budget.used_bytes() > before);
        let closed_at = TokioInstant::now();
        fixture.receiver.close();
        let receipt = pending.await.unwrap();
        assert_eq!(
            TokioInstant::now(),
            closed_at,
            "channel closure must wake the waiter without the admission deadline"
        );
        assert_eq!(
            *receipt.local(),
            BroadcastReport {
                targeted: 1,
                closed: 1,
                ..Default::default()
            }
        );
        fixture.release_queue();
        assert_eq!(fixture.budget.used_bytes(), 0);
        fixture.assert_connected().await;
    }
}

#[tokio::test]
async fn room_snapshot_observes_last_leave_and_lifecycle_entry_deletion() {
    let fixture = Fixture::new(4096, None).await;
    assert!(fixture.context.rooms().snapshot("shared").await.is_none());
    fixture
        .manager
        .join_room("orders", fixture.id, "shared")
        .await
        .unwrap();
    let snapshot = fixture.context.rooms().snapshot("shared").await.unwrap();
    assert_eq!(snapshot.namespace, "orders");
    assert_eq!(snapshot.room, "shared");
    assert_eq!(snapshot.connection_ids, vec![fixture.id]);
    let peer = Uuid::new_v4();
    let (sender, _receiver) = mpsc::channel::<QueuedApplicationFrame>(1);
    let (control, _control_receiver) = connection_control_channel();
    fixture
        .manager
        .add_connection_with_control_and_identity(
            peer,
            sender,
            control,
            RequestConnectionInfo::default(),
            WsTransportSecurity::Plaintext,
            Some("orders".to_owned()),
            Some(ConnectionIdentityHandle::new(
                WebSocketIdentitySnapshot::authenticated(identity("peer")),
            )),
        )
        .await
        .unwrap();
    fixture
        .manager
        .join_room("orders", peer, "shared")
        .await
        .unwrap();
    assert!(
        fixture
            .manager
            .get_room_snapshot("billing", "shared")
            .await
            .is_none()
    );
    fixture
        .manager
        .leave_room("orders", fixture.id, "shared")
        .await
        .unwrap();
    assert_eq!(
        fixture
            .context
            .rooms()
            .snapshot("shared")
            .await
            .unwrap()
            .connection_ids,
        vec![peer]
    );
    fixture
        .manager
        .leave_room("orders", peer, "shared")
        .await
        .unwrap();
    assert!(fixture.context.rooms().snapshot("shared").await.is_none());
    // Earlier snapshots retain their captured membership.
    assert_eq!(snapshot.connection_ids, vec![fixture.id]);
    fixture
        .manager
        .join_room("orders", fixture.id, "shared")
        .await
        .unwrap();
    fixture.manager.remove_connection(fixture.id).await.unwrap();
    assert!(
        fixture
            .manager
            .get_room_snapshot("orders", "shared")
            .await
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn namespace_scope_rejects_direct_send_room_and_metadata_mutations() {
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        let mut fixture = Fixture::new(4096, None).await;
        let manager = Arc::clone(&fixture.manager);
        manager
            .join_room("orders", fixture.id, "retained")
            .await
            .unwrap();
        manager
            .set_connection_metadata("orders", fixture.id, "marker".into(), "original".into())
            .await
            .unwrap();
        let foreign = WebSocketContext::new(Uuid::new_v4(), Arc::clone(&manager), "billing".into());
        let message = command(BroadcastTarget::Namespace("orders".into()), wire).message;
        let error = match wire {
            WsWireFormat::Text => {
                manager
                    .send_to_connection("billing", fixture.id, message)
                    .await
            }
            WsWireFormat::Binary => {
                manager
                    .send_binary_to_connection("billing", fixture.id, message)
                    .await
            }
        }
        .unwrap_err();
        assert!(
            matches!(error, ConnectionError::ConnectionNotFound { connection_id } if connection_id == fixture.id)
        );
        for result in [
            foreign
                .rooms()
                .join_connection(fixture.id, "forbidden")
                .await,
            foreign
                .rooms()
                .leave_connection(fixture.id, "retained")
                .await,
            manager.join_room("billing", fixture.id, "forbidden").await,
            manager.leave_room("billing", fixture.id, "retained").await,
            manager
                .set_connection_metadata("billing", fixture.id, "marker".into(), "changed".into())
                .await,
        ] {
            assert!(
                matches!(result, Err(ConnectionError::ConnectionNotFound { connection_id }) if connection_id == fixture.id)
            );
        }
        assert_eq!(
            manager.get_room_connections("orders", "retained").await,
            vec![fixture.id]
        );
        assert!(
            manager
                .get_room_snapshot("orders", "forbidden")
                .await
                .is_none()
        );
        assert!(
            manager
                .get_room_snapshot("billing", "forbidden")
                .await
                .is_none()
        );
        assert_eq!(
            manager.get_connection(fixture.id).await.unwrap().metadata["marker"],
            "original"
        );
        assert!(matches!(
            fixture.receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(fixture.budget.used_bytes(), 0);
        fixture.assert_connected().await;
    }
}

#[tokio::test(start_paused = true)]
async fn every_broadcast_target_checks_namespace_before_selection_and_after_capacity_wait() {
    for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
        for kind in ["namespace", "room", "rooms", "connections", "principal"] {
            for after_wait in [false, true] {
                let mut fixture = Fixture::new(4096, None).await;
                let manager = Arc::clone(&fixture.manager);
                manager
                    .join_room("orders", fixture.id, "selected")
                    .await
                    .unwrap();
                let target = match kind {
                    "namespace" => BroadcastTarget::Namespace("orders".into()),
                    "room" => BroadcastTarget::Room {
                        namespace: "orders".into(),
                        room: "selected".into(),
                    },
                    "rooms" => BroadcastTarget::Rooms {
                        namespace: "orders".into(),
                        rooms: vec!["selected".into()],
                    },
                    "connections" => BroadcastTarget::NamespaceConnections {
                        namespace: "orders".into(),
                        connection_ids: vec![fixture.id],
                    },
                    "principal" => BroadcastTarget::Principal {
                        namespace: "orders".into(),
                        principal_id: PrincipalId::try_new("account-old").unwrap(),
                    },
                    _ => unreachable!(),
                };
                if after_wait {
                    fixture.fill_queue().await;
                }
                let pending = manager.broadcast(command(target, wire));
                tokio::pin!(pending);
                if after_wait {
                    let before = fixture.budget.used_bytes();
                    assert!(poll!(&mut pending).is_pending());
                    assert!(fixture.budget.used_bytes() > before);
                }
                // Deliberately retain the old membership index/snapshot while
                // changing registry authority. Production cannot move a live
                // connection between namespaces; this internal fault proves
                // that neither a stale index nor a pending admission can bypass
                // the final registry fence, independently of transport generation.
                manager
                    .registry
                    .write()
                    .await
                    .connections
                    .get_mut(&fixture.id)
                    .unwrap()
                    .namespace = "billing".into();
                if after_wait {
                    fixture.release_queue();
                }
                let report = pending.await.unwrap();
                assert_eq!(
                    report,
                    BroadcastReport {
                        targeted: 1,
                        missing: 1,
                        ..Default::default()
                    },
                    "{kind}, after_wait={after_wait}"
                );
                assert_eq!(fixture.budget.used_bytes(), 0);
                assert!(matches!(
                    fixture.receiver.try_recv(),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                ));
                fixture.assert_connected().await;
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn wrong_namespace_send_cannot_claim_another_scopes_expired_identity() {
    let fixture = Fixture::new(4096, Some(TokioInstant::now() + Duration::from_secs(1))).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    let message = command(
        BroadcastTarget::Namespace("billing".into()),
        WsWireFormat::Text,
    )
    .message;
    assert!(matches!(
        fixture
            .manager
            .send_to_connection("billing", fixture.id, message)
            .await,
        Err(ConnectionError::ConnectionNotFound { .. })
    ));
    fixture.assert_connected().await;
    assert_eq!(fixture.budget.used_bytes(), 0);
}
