//! Private backplane envelope protocol tests.

use super::*;

use std::future::pending;

use lily_shutdown::ShutdownState;
use serde_json::{Value, json};

struct NoopBackplane;

#[async_trait]
impl WebSocketBackplane for NoopBackplane {
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
        pending().await
    }
}

fn dispatcher(max_outbound_message_size: usize) -> WebSocketDispatcher {
    WebSocketDispatcher::active(
        Arc::new(
            ConnectionManager::with_registered_namespaces_and_identity_and_outbound_limit(
                128,
                128,
                std::iter::empty(),
                false,
                max_outbound_message_size,
            ),
        ),
        BackplaneRequirement::Required,
        Arc::new(NoopBackplane),
        Duration::from_secs(1),
        HealthRegistry::new(Arc::new(ShutdownState::new())),
    )
}

fn fixed_message() -> WsMessageBody {
    let mut message = WsMessageBody::try_new("orders:created", json!({ "id": "42" })).unwrap();
    message.timestamp = 123;
    message
}

fn fixed_envelope(target: BackplaneTarget, wire_format: BackplaneWireFormat) -> BackplaneEnvelope {
    BackplaneEnvelope {
        protocol_version: BACKPLANE_PROTOCOL_VERSION,
        message_id: Uuid::from_u128(1),
        origin_node_id: Uuid::from_u128(2),
        target,
        exclusions: Vec::new(),
        message: fixed_message(),
        wire_format,
        traceparent: None,
    }
}

fn targets() -> Vec<BackplaneTarget> {
    vec![
        BackplaneTarget::Namespace {
            namespace: "orders".to_owned(),
        },
        BackplaneTarget::Room {
            namespace: "orders".to_owned(),
            room: "priority".to_owned(),
        },
        BackplaneTarget::Rooms {
            namespace: "orders".to_owned(),
            rooms: vec!["priority".to_owned(), "standard".to_owned()],
        },
        BackplaneTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: vec![Uuid::from_u128(10), Uuid::from_u128(11)],
        },
        BackplaneTarget::Principal {
            namespace: "orders".to_owned(),
            principal_id: PrincipalId::try_new("account-42").unwrap(),
        },
    ]
}

fn valid_value() -> Value {
    serde_json::to_value(fixed_envelope(
        BackplaneTarget::Namespace {
            namespace: "orders".to_owned(),
        },
        BackplaneWireFormat::Text,
    ))
    .unwrap()
}

fn assert_protocol_error(
    dispatcher: &WebSocketDispatcher,
    value: Value,
    expected: impl FnOnce(WebSocketBackplaneProtocolError) -> bool,
) {
    let error = dispatcher
        .decode(&serde_json::to_vec(&value).unwrap())
        .unwrap_err();
    assert!(expected(error));
}

#[test]
fn every_target_and_wire_format_has_a_stable_round_trip() {
    let dispatcher = dispatcher(16 * 1024);
    for wire_format in [BackplaneWireFormat::Text, BackplaneWireFormat::Binary] {
        for target in targets() {
            let envelope = fixed_envelope(target, wire_format);
            let expected = serde_json::to_value(&envelope).unwrap();
            let encoded = serde_json::to_vec(&envelope).unwrap();
            let decoded = dispatcher.decode(&encoded).unwrap();

            assert_eq!(serde_json::to_value(&decoded).unwrap(), expected);
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), encoded);
        }
    }
}

#[test]
fn representative_json_wire_vector_is_stable() {
    let encoded = serde_json::to_string(&fixed_envelope(
        BackplaneTarget::Namespace {
            namespace: "orders".to_owned(),
        },
        BackplaneWireFormat::Text,
    ))
    .unwrap();
    assert_eq!(
        encoded,
        r#"{"protocol_version":4,"message_id":"00000000-0000-0000-0000-000000000001","origin_node_id":"00000000-0000-0000-0000-000000000002","target":{"kind":"namespace","namespace":"orders"},"exclusions":[],"message":{"protocol_version":2,"msg_type":"event","event":"orders:created","content_kind":"json","content_type":"application/json","encoding":"identity","data":{"id":"42"},"timestamp":123},"wire_format":"text"}"#
    );
}

#[test]
fn representative_principal_target_wire_vector_is_stable_and_redacted_in_debug() {
    let principal_id = PrincipalId::try_new("sensitive-account-42").unwrap();
    let envelope = fixed_envelope(
        BackplaneTarget::Principal {
            namespace: "orders".to_owned(),
            principal_id,
        },
        BackplaneWireFormat::Text,
    );
    let encoded = serde_json::to_string(&envelope).unwrap();
    assert_eq!(
        encoded,
        r#"{"protocol_version":4,"message_id":"00000000-0000-0000-0000-000000000001","origin_node_id":"00000000-0000-0000-0000-000000000002","target":{"kind":"principal","namespace":"orders","principal_id":"sensitive-account-42"},"exclusions":[],"message":{"protocol_version":2,"msg_type":"event","event":"orders:created","content_kind":"json","content_type":"application/json","encoding":"identity","data":{"id":"42"},"timestamp":123},"wire_format":"text"}"#
    );
    assert!(!format!("{envelope:?}").contains("sensitive-account-42"));
}

#[test]
fn empty_malformed_unknown_missing_and_nil_authority_are_fail_closed() {
    let dispatcher = dispatcher(16 * 1024);
    assert!(matches!(
        dispatcher.decode(&[]),
        Err(WebSocketBackplaneProtocolError::InvalidEnvelope)
    ));
    assert!(matches!(
        dispatcher.decode(b"{"),
        Err(WebSocketBackplaneProtocolError::InvalidEnvelope)
    ));

    let mut unknown = valid_value();
    unknown["unexpected"] = json!(true);
    assert_protocol_error(&dispatcher, unknown, |error| {
        matches!(error, WebSocketBackplaneProtocolError::InvalidEnvelope)
    });

    let mut missing = valid_value();
    missing.as_object_mut().unwrap().remove("message_id");
    assert_protocol_error(&dispatcher, missing, |error| {
        matches!(error, WebSocketBackplaneProtocolError::InvalidEnvelope)
    });

    for field in ["message_id", "origin_node_id"] {
        let mut nil = valid_value();
        nil[field] = json!(Uuid::nil());
        assert_protocol_error(&dispatcher, nil, |error| {
            matches!(error, WebSocketBackplaneProtocolError::InvalidEnvelope)
        });
    }

    let mut wire_format = valid_value();
    wire_format["wire_format"] = json!("frames");
    assert_protocol_error(&dispatcher, wire_format, |error| {
        matches!(error, WebSocketBackplaneProtocolError::InvalidEnvelope)
    });
}

#[test]
fn frame_limit_accepts_exactly_maximum_and_rejects_maximum_plus_one() {
    let dispatcher = dispatcher(4 * 1024);
    let encoded = serde_json::to_vec(&fixed_envelope(
        BackplaneTarget::Namespace {
            namespace: "orders".to_owned(),
        },
        BackplaneWireFormat::Text,
    ))
    .unwrap();
    assert!(encoded.len() < dispatcher.0.max_frame_size);

    let mut exact = encoded;
    exact.resize(dispatcher.0.max_frame_size, b' ');
    dispatcher.decode(&exact).unwrap();
    exact.push(b' ');
    assert!(matches!(
        dispatcher.decode(&exact),
        Err(WebSocketBackplaneProtocolError::FrameTooLarge)
    ));
}

#[test]
fn embedded_message_uses_outbound_authority_for_text_and_binary_wire_formats() {
    for wire_format in [BackplaneWireFormat::Text, BackplaneWireFormat::Binary] {
        let envelope = fixed_envelope(
            BackplaneTarget::Namespace {
                namespace: "orders".to_owned(),
            },
            wire_format,
        );
        let wire_len = match wire_format {
            BackplaneWireFormat::Text => envelope.message.to_message().unwrap().len(),
            BackplaneWireFormat::Binary => envelope.message.to_binary_message().unwrap().len(),
        };
        let encoded = serde_json::to_vec(&envelope).unwrap();

        dispatcher(wire_len).decode(&encoded).unwrap();
        assert!(matches!(
            dispatcher(wire_len - 1).decode(&encoded),
            Err(WebSocketBackplaneProtocolError::MessageTooLarge)
        ));
    }
}

#[test]
fn invalid_embedded_message_is_rejected_before_dispatch() {
    let dispatcher = dispatcher(16 * 1024);
    for (field, value) in [
        ("protocol_version", json!(u16::MAX)),
        ("event", json!("not-a-route")),
        ("timestamp", json!(-1)),
    ] {
        let mut invalid = valid_value();
        invalid["message"][field] = value;
        assert_protocol_error(&dispatcher, invalid, |error| {
            matches!(error, WebSocketBackplaneProtocolError::InvalidMessage)
        });
    }
}

#[test]
fn collection_targets_reject_empty_unsorted_duplicate_and_oversized_values() {
    let dispatcher = dispatcher(64 * 1024);

    let invalid_targets = [
        json!({ "kind": "rooms", "namespace": "orders", "rooms": [] }),
        json!({ "kind": "rooms", "namespace": "orders", "rooms": ["standard", "priority"] }),
        json!({ "kind": "rooms", "namespace": "orders", "rooms": ["priority", "priority"] }),
        json!({ "kind": "namespace_connections", "namespace": "orders", "connection_ids": [] }),
        json!({ "kind": "namespace_connections", "namespace": "orders", "connection_ids": [Uuid::from_u128(2), Uuid::from_u128(1)] }),
        json!({ "kind": "namespace_connections", "namespace": "orders", "connection_ids": [Uuid::from_u128(1), Uuid::from_u128(1)] }),
        json!({ "kind": "rooms", "namespace": "orders", "rooms": (0..=MAX_BACKPLANE_ROOMS).map(|index| format!("room-{index:03}")).collect::<Vec<_>>() }),
        json!({ "kind": "namespace_connections", "namespace": "orders", "connection_ids": (1..=MAX_BACKPLANE_CONNECTION_TARGETS+1).map(|index| Uuid::from_u128(index as u128)).collect::<Vec<_>>() }),
    ];
    for target in invalid_targets {
        let mut invalid = valid_value();
        invalid["target"] = target;
        assert_protocol_error(&dispatcher, invalid, |error| {
            matches!(error, WebSocketBackplaneProtocolError::InvalidTarget)
        });
    }
}

#[test]
fn scalar_targets_reject_noncanonical_namespace_and_room() {
    let dispatcher = dispatcher(16 * 1024);
    for target in [
        json!({ "kind": "namespace", "namespace": "orders/escape" }),
        json!({ "kind": "namespace_connections", "namespace": "orders/escape", "connection_ids": [Uuid::from_u128(1)] }),
        json!({ "kind": "room", "namespace": "orders", "room": "priority*" }),
        json!({ "kind": "room", "namespace": "orders/escape", "room": "priority" }),
    ] {
        let mut invalid = valid_value();
        invalid["target"] = target;
        assert_protocol_error(&dispatcher, invalid, |error| {
            matches!(error, WebSocketBackplaneProtocolError::InvalidTarget)
        });
    }
}

#[test]
fn namespace_connections_require_scope_and_validate_outbound_cardinality() {
    let dispatcher = dispatcher(64 * 1024);
    let mut missing_namespace = valid_value();
    missing_namespace["target"] = json!({
        "kind": "namespace_connections", "connection_ids": [Uuid::from_u128(1)]
    });
    assert_protocol_error(&dispatcher, missing_namespace, |error| {
        matches!(error, WebSocketBackplaneProtocolError::InvalidEnvelope)
    });
    let ids = vec![Uuid::from_u128(2), Uuid::from_u128(1), Uuid::from_u128(2)];
    let mut command = BroadcastMessage {
        target: BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: ids,
        },
        message: fixed_message(),
        wire_format: WsWireFormat::Text,
        exclude: Vec::new(),
    };
    let prepared = dispatcher
        .connection_manager()
        .prepare_broadcast(command.clone())
        .unwrap();
    let target = BackplaneTarget::from(&prepared.command().target);
    assert_eq!(
        serde_json::to_value(target).unwrap(),
        json!({
            "kind": "namespace_connections", "namespace": "orders",
            "connection_ids": [Uuid::from_u128(1), Uuid::from_u128(2)]
        })
    );
    for (namespace, ids, exceeds_limit) in [
        ("orders/escape", vec![Uuid::from_u128(1)], false),
        ("orders/escape", Vec::new(), false),
        (
            "orders",
            vec![Uuid::from_u128(1); MAX_BACKPLANE_CONNECTION_TARGETS + 1],
            true,
        ),
    ] {
        command.target = BroadcastTarget::NamespaceConnections {
            namespace: namespace.to_owned(),
            connection_ids: ids,
        };
        let error = dispatcher
            .connection_manager()
            .prepare_broadcast(command.clone())
            .unwrap_err();
        if exceeds_limit {
            assert!(matches!(
                error,
                ConnectionError::InvalidOperation(
                    crate::ConnectionOperationError::BackplaneTargetLimitExceeded
                )
            ));
        } else {
            assert!(matches!(
                error,
                ConnectionError::InvalidOperation(
                    crate::ConnectionOperationError::InvalidBackplaneTarget
                )
            ));
        }
    }
}

#[test]
fn principal_target_rejects_invalid_identity_and_namespace() {
    let dispatcher = dispatcher(16 * 1024);
    let mut exact_maximum = valid_value();
    exact_maximum["target"] = json!({
        "kind": "principal",
        "namespace": "orders",
        "principal_id": "x".repeat(crate::connection::MAX_PRINCIPAL_ID_BYTES)
    });
    dispatcher
        .decode(&serde_json::to_vec(&exact_maximum).unwrap())
        .unwrap();

    for target in [
        json!({ "kind": "principal", "namespace": "orders", "principal_id": "" }),
        json!({ "kind": "principal", "namespace": "orders", "principal_id": " account-42" }),
        json!({
            "kind": "principal",
            "namespace": "orders",
            "principal_id": "account\n42"
        }),
        json!({
            "kind": "principal",
            "namespace": "orders",
            "principal_id": "x".repeat(crate::connection::MAX_PRINCIPAL_ID_BYTES + 1)
        }),
        json!({ "kind": "principal", "namespace": "orders" }),
        json!({
            "kind": "principal",
            "namespace": "orders",
            "principal_id": "account-42",
            "unexpected": true
        }),
    ] {
        let mut invalid = valid_value();
        invalid["target"] = target;
        assert_protocol_error(&dispatcher, invalid, |error| {
            matches!(error, WebSocketBackplaneProtocolError::InvalidEnvelope)
        });
    }

    let mut invalid_namespace = valid_value();
    invalid_namespace["target"] = json!({
        "kind": "principal",
        "namespace": "orders/escape",
        "principal_id": "account-42"
    });
    assert_protocol_error(&dispatcher, invalid_namespace, |error| {
        matches!(error, WebSocketBackplaneProtocolError::InvalidTarget)
    });
}

#[test]
fn exclusion_cardinality_and_uniqueness_are_strict() {
    let dispatcher = dispatcher(128 * 1024);
    let duplicate = Uuid::from_u128(7);
    let mut duplicate_exclusions = valid_value();
    duplicate_exclusions["exclusions"] = json!([duplicate, duplicate]);
    assert_protocol_error(&dispatcher, duplicate_exclusions, |error| {
        matches!(error, WebSocketBackplaneProtocolError::TooManyExclusions)
    });

    let mut too_many = valid_value();
    too_many["exclusions"] = json!(
        (1..=MAX_BACKPLANE_EXCLUSIONS + 1)
            .map(|index| Uuid::from_u128(index as u128))
            .collect::<Vec<_>>()
    );
    assert_protocol_error(&dispatcher, too_many, |error| {
        matches!(error, WebSocketBackplaneProtocolError::TooManyExclusions)
    });
}

#[test]
fn traceparent_is_validated_and_bounded() {
    let dispatcher = dispatcher(16 * 1024);
    let mut valid = valid_value();
    valid["traceparent"] = json!("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    dispatcher
        .decode(&serde_json::to_vec(&valid).unwrap())
        .unwrap();

    for traceparent in ["invalid".to_owned(), "a".repeat(MAX_TRACEPARENT_BYTES + 1)] {
        let mut invalid = valid_value();
        invalid["traceparent"] = json!(traceparent);
        assert_protocol_error(&dispatcher, invalid, |error| {
            matches!(error, WebSocketBackplaneProtocolError::InvalidTraceContext)
        });
    }
}

#[test]
fn outbound_message_and_collection_boundaries_are_exact() {
    let exact_message = BroadcastMessage {
        target: BroadcastTarget::Namespace("orders".to_owned()),
        message: fixed_message(),
        wire_format: WsWireFormat::Text,
        exclude: Vec::new(),
    };
    let message_len = exact_message.message.to_message().unwrap().len();
    dispatcher(message_len)
        .connection_manager()
        .prepare_broadcast(exact_message.clone())
        .unwrap();

    let oversized_message = exact_message.clone();
    let error = dispatcher(message_len - 1)
        .connection_manager()
        .prepare_broadcast(oversized_message.clone())
        .unwrap_err();
    assert!(matches!(
        error,
        ConnectionError::InvalidOperation(
            crate::connection::ConnectionOperationError::OutboundMessageTooLarge
        )
    ));

    let empty_connections = BroadcastMessage {
        target: BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: Vec::new(),
        },
        ..exact_message.clone()
    };
    let empty = dispatcher(16 * 1024)
        .connection_manager()
        .prepare_broadcast(empty_connections.clone())
        .unwrap();
    assert!(matches!(
        BackplaneTarget::from(&empty.command().target),
        BackplaneTarget::NamespaceConnections { connection_ids, .. } if connection_ids.is_empty()
    ));

    let principal = BroadcastMessage {
        target: BroadcastTarget::Principal {
            namespace: "orders".to_owned(),
            principal_id: PrincipalId::try_new("account-42").unwrap(),
        },
        ..exact_message.clone()
    };
    let canonical = dispatcher(16 * 1024)
        .connection_manager()
        .prepare_broadcast(principal.clone())
        .unwrap();
    assert!(matches!(
        BackplaneTarget::from(&canonical.command().target),
        BackplaneTarget::Principal {
            namespace,
            principal_id,
        } if namespace == "orders" && principal_id.as_str() == "account-42"
    ));

    let invalid_principal_namespace = BroadcastMessage {
        target: BroadcastTarget::Principal {
            namespace: "orders/escape".to_owned(),
            principal_id: PrincipalId::try_new("account-42").unwrap(),
        },
        ..exact_message
    };
    let error = dispatcher(16 * 1024)
        .connection_manager()
        .prepare_broadcast(invalid_principal_namespace.clone())
        .unwrap_err();
    assert!(matches!(
        error,
        ConnectionError::InvalidOperation(
            crate::connection::ConnectionOperationError::InvalidBackplaneTarget
        )
    ));
}

#[test]
fn every_private_target_requires_namespace_and_rejects_unknown_fields() {
    let dispatcher = dispatcher(16 * 1024);
    for target in targets() {
        let valid =
            serde_json::to_value(fixed_envelope(target, BackplaneWireFormat::Text)).unwrap();
        for mutation in ["missing", "null", "extra"] {
            let mut invalid = valid.clone();
            let target = invalid["target"].as_object_mut().unwrap();
            match mutation {
                "missing" => {
                    target.remove("namespace");
                }
                "null" => {
                    target.insert("namespace".into(), Value::Null);
                }
                "extra" => {
                    target.insert("unexpected".into(), json!(true));
                }
                _ => unreachable!(),
            }
            assert_protocol_error(&dispatcher, invalid, |error| {
                matches!(error, WebSocketBackplaneProtocolError::InvalidEnvelope)
            });
        }
        let mut invalid = valid;
        invalid["target"]["namespace"] = json!("");
        assert_protocol_error(&dispatcher, invalid, |error| {
            matches!(error, WebSocketBackplaneProtocolError::InvalidTarget)
        });
    }
}

#[test]
fn legacy_global_targets_are_rejected_even_when_they_include_a_namespace() {
    let dispatcher = dispatcher(16 * 1024);
    for target in [
        json!({"kind":"all"}),
        json!({"kind":"all","unexpected":true}),
        json!({"kind":"all","namespace":"orders"}),
        json!({"kind":"connections","connection_ids":[Uuid::from_u128(1)]}),
        json!({"kind":"connections","namespace":"orders","connection_ids":[Uuid::from_u128(1)]}),
    ] {
        let mut invalid = valid_value();
        invalid["target"] = target;
        assert_protocol_error(&dispatcher, invalid, |error| {
            matches!(error, WebSocketBackplaneProtocolError::InvalidEnvelope)
        });
    }
}
