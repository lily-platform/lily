//! Opt-in AsyncAPI snapshot produced from the accepted Consumer execution plan.

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use lily_asyncapi::__private::TransactionalInboxBackend;
use lily_asyncapi::__private::{
    Action, AmqpQueueBinding, ChannelBinding, ChannelDescriptor, DeliveryGuaranteeExtension,
    Documentation, LilyExtension, MessageDescriptor, OperationDescriptor, PayloadSchema,
    PreparedAsyncApi, RabbitMqDeadLetterDescriptor, RabbitMqExchangeKind,
    RabbitMqMainTopologyDescriptor, RabbitMqQueueType, RabbitMqRetryAttemptDescriptor,
    RabbitMqRetryBucketDescriptor, RabbitMqTopologyExtension, RabbitMqTopologyOwnership,
    SettlementExtension, TransportContribution, TransportKind, amqp_server_names, prepare_document,
};
use lily_asyncapi::{AsyncApiBuildError, AsyncApiConfig, AsyncApiService};
use lily_config::{
    QueueDefinition, RabbitMqExchangeKind as ConfigExchangeKind, RabbitMqQueueTopologyPlan,
    RabbitMqQueueType as ConfigQueueType, RabbitMqTopologyConfig,
    RabbitMqTopologyOwnership as ConfigOwnership, RabbitMqTopologyPlan,
};
use lily_queue::__private::{
    DeliveryGuarantee, QueueAsyncApiPayload, QueueAsyncApiStatus, QueueHandlerMetadata,
    QueuePayloadKind,
};

use crate::plan::ConsumerAsyncApiPlanBinding;

/// Opaque marker separating a Consumer document from other AsyncAPI snapshots
/// stored in the same dependency-injection container.
#[derive(Debug)]
pub struct ConsumerDocument;

/// Immutable AsyncAPI snapshot for the accepted Consumer process plan.
pub type ConsumerAsyncApiService = AsyncApiService<ConsumerDocument>;

pub(crate) type PreparedConsumerAsyncApi = PreparedAsyncApi<ConsumerDocument>;

pub(crate) fn prepare_consumer_asyncapi(
    config: &AsyncApiConfig,
    virtual_host: &str,
    bindings: &[ConsumerAsyncApiPlanBinding],
) -> Result<PreparedConsumerAsyncApi, AsyncApiBuildError> {
    validate_virtual_host(virtual_host)?;
    let servers = amqp_server_names(config)?;
    let topology = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
        queues: bindings
            .iter()
            .map(|binding| binding.definition.clone())
            .collect(),
    })
    .map_err(|error| AsyncApiBuildError::Validation {
        field: "consumer.topology".to_owned(),
        detail: error.to_string(),
    })?;

    let mut contribution = TransportContribution::new(TransportKind::Amqp);
    for binding in bindings {
        let queue = topology.queue(&binding.definition.name).ok_or_else(|| {
            AsyncApiBuildError::Reference {
                owner: "consumer.execution_plan".to_owned(),
                target: binding.definition.name.clone(),
                detail: "accepted RabbitMQ topology omitted an execution-plan queue".to_owned(),
            }
        })?;
        contribution = project_queue(
            contribution,
            &servers,
            virtual_host,
            queue,
            &binding.handlers,
        )?;
    }

    prepare_document(config, vec![contribution])
}

fn project_queue(
    mut contribution: TransportContribution,
    servers: &[String],
    virtual_host: &str,
    topology: &RabbitMqQueueTopologyPlan,
    handlers: &[&'static QueueHandlerMetadata],
) -> Result<TransportContribution, AsyncApiBuildError> {
    let channel_identity = channel_identity(topology.main_queue_name());
    let mut documented_messages = Vec::new();
    let mut has_documented = false;
    let mut has_unspecified = false;

    for handler in handlers {
        let message_identity = message_identity(handler);
        match handler.asyncapi.status {
            QueueAsyncApiStatus::Documented => {
                has_documented = true;
                documented_messages.push(message_identity.clone());
                let message = documented_message(handler, message_identity.clone())?;
                let operation = documented_operation(
                    topology.definition(),
                    handler,
                    &channel_identity,
                    &message_identity,
                )?;
                contribution = contribution
                    .message(Documentation::Documented(message))
                    .operation(Documentation::Documented(operation));
            }
            QueueAsyncApiStatus::Skipped => {
                contribution = contribution
                    .message(Documentation::Skipped {
                        identity: message_identity,
                    })
                    .operation(Documentation::Skipped {
                        identity: operation_identity(handler),
                    });
            }
            QueueAsyncApiStatus::Unspecified => {
                has_unspecified = true;
                contribution = contribution
                    .message(Documentation::Unspecified {
                        identity: message_identity,
                    })
                    .operation(Documentation::Unspecified {
                        identity: operation_identity(handler),
                    });
            }
        }
    }

    let channel = if has_unspecified {
        Documentation::Unspecified {
            identity: channel_identity,
        }
    } else if !has_documented {
        Documentation::Skipped {
            identity: channel_identity,
        }
    } else {
        let definition = topology.definition();
        let mut channel = ChannelDescriptor::new(&channel_identity, topology.main_queue_name())
            .binding(ChannelBinding::AmqpQueue(AmqpQueueBinding::new(
                topology.main_queue_name(),
                definition.durable,
                definition.exclusive,
                definition.auto_delete,
                virtual_host,
            )))
            .extension(LilyExtension::RabbitMqTopology(topology_extension(
                topology,
            )));
        for server in servers {
            channel = channel.server(server);
        }
        for message in documented_messages {
            channel = channel.message(message);
        }
        Documentation::Documented(channel)
    };

    Ok(contribution.channel(channel))
}

fn documented_message(
    handler: &QueueHandlerMetadata,
    identity: String,
) -> Result<MessageDescriptor, AsyncApiBuildError> {
    let (content_type, payload) = payload_schema(handler)?;
    let registration = &handler.asyncapi;
    let mut message = MessageDescriptor::new(identity, content_type, payload)
        .rabbitmq_headers(handler.schema_version, handler.content_kind)
        .deprecated(registration.deprecated);
    for example in &registration.examples {
        message = message.example_json(example)?;
    }
    Ok(message)
}

fn documented_operation(
    definition: &QueueDefinition,
    handler: &QueueHandlerMetadata,
    channel_identity: &str,
    message_identity: &str,
) -> Result<OperationDescriptor, AsyncApiBuildError> {
    let registration = &handler.asyncapi;
    let mut operation = OperationDescriptor::new(
        operation_identity(handler),
        Action::Receive,
        channel_identity,
    )
    .message(message_identity)
    .extension(LilyExtension::Settlement(SettlementExtension::manual_ack()))
    .extension(LilyExtension::DeliveryGuarantee(delivery_guarantee(
        definition,
        handler.delivery_guarantee,
    )?));
    if let Some(id) = registration.operation_id {
        operation = operation.id(id);
    }
    if let Some(summary) = registration.summary {
        operation = operation.summary(summary);
    }
    if let Some(description) = registration.description {
        operation = operation.description(description);
    }
    for tag in &registration.tags {
        operation = operation.tag(*tag);
    }
    for security in &registration.security {
        operation = operation.security(*security);
    }
    Ok(operation)
}

fn payload_schema(
    handler: &QueueHandlerMetadata,
) -> Result<(&'static str, PayloadSchema), AsyncApiBuildError> {
    let invalid = |detail: &'static str| AsyncApiBuildError::Validation {
        field: format!("handler.{}.asyncapi.payload", handler.handler_name),
        detail: detail.to_owned(),
    };
    match (
        &handler.asyncapi.payload,
        handler.input_contract.payload_kind,
    ) {
        (
            QueueAsyncApiPayload::Generated {
                schema,
                content_type,
            },
            QueuePayloadKind::Json,
        ) if *content_type == "application/json" => {
            Ok((content_type, PayloadSchema::Generated(*schema)))
        }
        (
            QueueAsyncApiPayload::ExplicitGenerated {
                schema,
                content_type,
            },
            QueuePayloadKind::Raw | QueuePayloadKind::Custom | QueuePayloadKind::None,
        ) => Ok((content_type, PayloadSchema::Generated(*schema))),
        (QueueAsyncApiPayload::Text { content_type }, QueuePayloadKind::Text)
            if *content_type == "text/plain; charset=utf-8" =>
        {
            Ok((content_type, PayloadSchema::String))
        }
        (QueueAsyncApiPayload::Binary { content_type }, QueuePayloadKind::Binary)
            if *content_type == "application/octet-stream" =>
        {
            Ok((content_type, PayloadSchema::Binary))
        }
        (
            QueueAsyncApiPayload::Opaque { content_type },
            QueuePayloadKind::Raw | QueuePayloadKind::Custom | QueuePayloadKind::None,
        ) => Ok((content_type, PayloadSchema::Opaque)),
        (QueueAsyncApiPayload::Unspecified, _) => Err(invalid(
            "a documented handler requires an exact generated or explicit opaque payload contract",
        )),
        _ => Err(invalid(
            "documentation payload kind diverges from the accepted typed extractor contract",
        )),
    }
}

fn topology_extension(topology: &RabbitMqQueueTopologyPlan) -> RabbitMqTopologyExtension {
    let main = RabbitMqMainTopologyDescriptor::new(
        topology.exchange_name(),
        match topology.exchange_kind() {
            ConfigExchangeKind::Direct => RabbitMqExchangeKind::Direct,
        },
        topology.main_queue_name(),
        topology.routing_key(),
    );
    let dead_letter = RabbitMqDeadLetterDescriptor::new(
        topology.dead_letter_exchange_name(),
        topology.dead_letter_queue_name(),
        topology.dead_letter_routing_key(),
    );
    let ownership = match topology.topology_ownership() {
        ConfigOwnership::FrameworkManaged => RabbitMqTopologyOwnership::FrameworkManaged,
        ConfigOwnership::External => RabbitMqTopologyOwnership::External,
    };
    let queue_type = match topology.system_queue_type() {
        ConfigQueueType::Classic => RabbitMqQueueType::Classic,
        ConfigQueueType::Quorum => RabbitMqQueueType::Quorum,
    };
    let mut extension = if topology.retry_buckets().is_empty() {
        RabbitMqTopologyExtension::without_retry(ownership, queue_type, main, dead_letter)
    } else {
        RabbitMqTopologyExtension::new(
            ownership,
            queue_type,
            main,
            topology.retry_exchange_name(),
            dead_letter,
        )
    };
    for bucket in topology.retry_buckets() {
        extension = extension.retry_bucket(RabbitMqRetryBucketDescriptor::new(
            bucket.queue_name(),
            bucket.routing_key(),
            bucket.delay_millis(),
        ));
    }
    for attempt in 1..=topology.definition().retry_attempts {
        if let Some(delays) = topology.retry_bucket_delays(attempt) {
            extension = extension.retry_attempt(RabbitMqRetryAttemptDescriptor::new(
                attempt,
                delays.to_vec(),
            ));
        }
    }
    extension
}

fn delivery_guarantee(
    definition: &QueueDefinition,
    guarantee: DeliveryGuarantee,
) -> Result<DeliveryGuaranteeExtension, AsyncApiBuildError> {
    match guarantee {
        DeliveryGuarantee::AtLeastOnce => Ok(DeliveryGuaranteeExtension::AtLeastOnce),
        DeliveryGuarantee::TransactionalInbox => {
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            {
                let binding = definition.transactional_inbox.as_ref().ok_or_else(|| {
                    AsyncApiBuildError::Validation {
                        field: "consumer.transactional_inbox".to_owned(),
                        detail: "accepted transactional handler lost its storage binding"
                            .to_owned(),
                    }
                })?;
                let backend = match binding.backend {
                    #[cfg(any(
                        feature = "transactional-inbox-postgresql",
                        feature = "transactional-inbox-postgresql-factory"
                    ))]
                    lily_config::TransactionalInboxBackend::PostgreSql => {
                        TransactionalInboxBackend::Postgresql
                    }
                    #[cfg(any(
                        feature = "transactional-inbox-mongodb",
                        feature = "transactional-inbox-mongodb-factory"
                    ))]
                    lily_config::TransactionalInboxBackend::MongoDb => {
                        TransactionalInboxBackend::Mongodb
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
                        return Err(AsyncApiBuildError::Validation {
                            field: "consumer.transactional_inbox.backend".to_owned(),
                            detail: "the selected storage adapter is not enabled".to_owned(),
                        });
                    }
                };
                Ok(DeliveryGuaranteeExtension::TransactionalInbox(backend))
            }
            #[cfg(not(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            )))]
            {
                let _ = definition;
                Err(AsyncApiBuildError::Validation {
                    field: "consumer.transactional_inbox".to_owned(),
                    detail: "transactional handler selected while no storage adapter is enabled"
                        .to_owned(),
                })
            }
        }
    }
}

fn validate_virtual_host(virtual_host: &str) -> Result<(), AsyncApiBuildError> {
    if virtual_host.len() > 255 || virtual_host.chars().any(char::is_control) {
        return Err(AsyncApiBuildError::Validation {
            field: "consumer.rabbitmq.virtual_host".to_owned(),
            detail: "must contain at most 255 control-free bytes".to_owned(),
        });
    }
    Ok(())
}

fn channel_identity(queue: &str) -> String {
    format!("queue[{}]:{queue}", queue.len())
}

fn message_identity(handler: &QueueHandlerMetadata) -> String {
    format!(
        "delivery[q={}:{};v={};c={}:{}]",
        handler.queue_name.len(),
        handler.queue_name,
        handler.schema_version,
        handler.content_kind.len(),
        handler.content_kind,
    )
}

fn operation_identity(handler: &QueueHandlerMetadata) -> String {
    format!("receive:{}", message_identity(handler))
}

#[cfg(test)]
mod tests {
    use std::{any::TypeId, sync::Arc};

    use lily_asyncapi::{AsyncApiServer, AsyncApiServerProtocol};
    use lily_config::QueueRetentionConfig;
    use lily_queue::__private::{
        QueueAsyncApiRegistration, QueueGuardRegistration, QueueHandlerInputContract,
        QueueMiddlewareRegistration,
    };
    use serde::Deserialize;
    use serde_json::Value;

    use super::*;

    #[derive(Deserialize, lily_asyncapi::schemars::JsonSchema)]
    #[schemars(crate = "lily_asyncapi::schemars")]
    #[allow(dead_code)]
    struct OrderCreated {
        order_id: String,
    }

    struct TestHandler;

    fn handler_fn<'a>(
        _service: Arc<dyn std::any::Any + Send + Sync>,
        _invocation: &'a mut (dyn std::any::Any + Send),
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), lily_queue::QueueHandlerError>> + Send + 'a,
        >,
    > {
        Box::pin(async { Ok(()) })
    }

    fn definition() -> QueueDefinition {
        QueueDefinition {
            name: "orders.created".to_owned(),
            exchange_name: "orders".to_owned(),
            routing_key: "orders.created".to_owned(),
            queue_type: ConfigQueueType::Quorum,
            retry_attempts: 2,
            retry_backoff_millis: Some(250),
            max_retry_backoff_millis: Some(2_000),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 10_000,
                main_max_bytes: 64 * 1024 * 1024,
                retry_bucket_max_messages: 1_000,
                retry_bucket_max_bytes: 16 * 1024 * 1024,
                dead_letter_max_messages: 1_000,
                dead_letter_max_bytes: 16 * 1024 * 1024,
            }),
            ..QueueDefinition::default()
        }
    }

    fn registration(payload: QueueAsyncApiPayload) -> QueueAsyncApiRegistration {
        QueueAsyncApiRegistration {
            status: QueueAsyncApiStatus::Documented,
            summary: Some("Consume one exact order event"),
            description: None,
            operation_id: None,
            tags: Vec::new(),
            security: Vec::new(),
            deprecated: false,
            examples: Vec::new(),
            payload,
        }
    }

    fn metadata(
        schema_version: u16,
        content_kind: &'static str,
        payload_kind: QueuePayloadKind,
        payload: QueueAsyncApiPayload,
    ) -> &'static QueueHandlerMetadata {
        metadata_with_registration(
            schema_version,
            content_kind,
            payload_kind,
            registration(payload),
        )
    }

    fn metadata_with_registration(
        schema_version: u16,
        content_kind: &'static str,
        payload_kind: QueuePayloadKind,
        asyncapi: QueueAsyncApiRegistration,
    ) -> &'static QueueHandlerMetadata {
        Box::leak(Box::new(QueueHandlerMetadata {
            service_type_id: TypeId::of::<TestHandler>(),
            service_type_name: "TestHandler",
            component_kind: None,
            queue_name: "orders.created",
            method_name: "consume",
            // Deliberately identical across exact dispatch keys: document
            // operation identity must include the accepted dispatch contract.
            handler_name: "tests::TestHandler::consume",
            schema_version,
            content_kind,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            input_contract: QueueHandlerInputContract::new(payload_kind, Some("test payload")),
            asyncapi,
            service_middlewares: Vec::<QueueMiddlewareRegistration>::new(),
            service_guards: Vec::<QueueGuardRegistration>::new(),
            handler_middlewares: Vec::<QueueMiddlewareRegistration>::new(),
            handler_guards: Vec::<QueueGuardRegistration>::new(),
            handler_fn,
        }))
    }

    fn config() -> AsyncApiConfig {
        let mut config = AsyncApiConfig::new("Orders", "1.0.0").expect("config");
        config
            .add_server(
                AsyncApiServer::new(
                    "rabbit",
                    "events.example.com:5671",
                    AsyncApiServerProtocol::Amqps,
                )
                .expect("server"),
            )
            .expect("server registration");
        config
    }

    fn build_with(handlers: &[&'static QueueHandlerMetadata]) -> (Value, Vec<u8>) {
        let definition = definition();
        let topology = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .expect("topology");
        let contribution = project_queue(
            TransportContribution::new(TransportKind::Amqp),
            &["rabbit".to_owned()],
            "/tenant-a",
            topology.queue("orders.created").expect("queue"),
            handlers,
        )
        .expect("projection");
        let (document, bytes) =
            lily_asyncapi::__private::build_document(&config(), vec![contribution])
                .expect("document");
        (
            serde_json::to_value(document).expect("document JSON"),
            bytes.to_vec(),
        )
    }

    #[test]
    fn projection_is_exact_order_stable_and_makes_no_upcaster_claim() {
        let json = metadata(
            1,
            "json",
            QueuePayloadKind::Json,
            QueueAsyncApiPayload::generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<OrderCreated>(),
                "application/json",
            ),
        );
        let binary = metadata(
            2,
            "binary",
            QueuePayloadKind::Binary,
            QueueAsyncApiPayload::binary(),
        );
        let (document, forward) = build_with(&[json, binary]);
        let (_, reverse) = build_with(&[binary, json]);
        assert_eq!(forward, reverse);
        if let Ok(path) = std::env::var("LILY_CAP_ASYNC02B_DOCUMENT_PATH") {
            let path = std::path::Path::new(&path);
            assert!(
                path.is_absolute(),
                "qualification evidence path must be absolute"
            );
            std::fs::write(path, &forward).expect("write qualification document evidence");
        }

        let channels = document["channels"].as_object().expect("channels");
        let channel = channels.values().next().expect("channel");
        assert_eq!(channel["bindings"]["amqp"]["bindingVersion"], "0.3.0");
        assert_eq!(channel["bindings"]["amqp"]["queue"]["vhost"], "/tenant-a");
        assert_eq!(channel["x-lily-rabbitmq-topology"]["queue_type"], "quorum");
        assert_eq!(
            channel["x-lily-rabbitmq-topology"]["retry_attempts"]
                .as_array()
                .expect("attempts")
                .len(),
            2
        );

        let messages = document["components"]["messages"]
            .as_object()
            .expect("messages");
        assert_eq!(messages.len(), 2);
        let binary_message = messages
            .values()
            .find(|message| message["contentType"] == "application/octet-stream")
            .expect("binary message");
        assert_eq!(binary_message["payload"]["format"], "binary");
        assert!(binary_message["payload"].get("contentEncoding").is_none());
        let accepted = messages
            .values()
            .map(|message| {
                (
                    message["headers"]["properties"]["x-lily-schema-version"]["const"]
                        .as_str()
                        .expect("version"),
                    message["headers"]["properties"]["x-lily-content-kind"]["const"]
                        .as_str()
                        .expect("content"),
                )
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(accepted, [("1", "json"), ("2", "binary")].into());

        let operations = document["operations"].as_object().expect("operations");
        assert_eq!(operations.len(), 2);
        assert!(operations.values().all(|operation| {
            operation["x-lily-settlement"]["manual_ack"] == true
                && operation["x-lily-settlement"]["retryable"]["when_retry_budget_available"]
                    == "retry_handoff_then_ack"
                && operation["x-lily-settlement"]["retryable"]["when_retry_budget_exhausted"]
                    == "dead_letter_handoff_then_ack"
                && operation["x-lily-delivery-guarantee"]["delivery_guarantee"] == "at_least_once"
        }));
        let serialized = String::from_utf8(forward).expect("canonical JSON");
        assert!(!serialized.contains("upcaster"));
        assert!(!serialized.contains("global_exactly_once\":true"));
    }

    #[test]
    fn accepted_documentation_status_set_matches_the_receive_operation_set() {
        let documented = metadata(
            1,
            "json",
            QueuePayloadKind::Json,
            QueueAsyncApiPayload::generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<OrderCreated>(),
                "application/json",
            ),
        );
        let skipped = metadata_with_registration(
            2,
            "binary",
            QueuePayloadKind::Binary,
            QueueAsyncApiRegistration::skipped(),
        );
        let definition = definition();
        let topology = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .expect("topology");
        let contribution = project_queue(
            TransportContribution::new(TransportKind::Amqp),
            &["rabbit".to_owned()],
            "/",
            topology.queue("orders.created").expect("queue"),
            &[documented, skipped],
        )
        .expect("projection");
        let (document, bytes) =
            lily_asyncapi::__private::build_document(&config(), vec![contribution])
                .expect("document");
        let document = serde_json::to_value(document).expect("document JSON");
        assert_eq!(
            document["operations"]
                .as_object()
                .expect("operations")
                .len(),
            1
        );
        assert_eq!(
            document["components"]["messages"]
                .as_object()
                .expect("messages")
                .len(),
            1
        );
        let serialized = String::from_utf8(bytes.to_vec()).expect("canonical JSON");
        assert!(!serialized.contains(&message_identity(skipped)));
        assert!(!serialized.contains(&operation_identity(skipped)));

        let unspecified = metadata_with_registration(
            3,
            "raw",
            QueuePayloadKind::Raw,
            QueueAsyncApiRegistration::unspecified(),
        );
        let contribution = project_queue(
            TransportContribution::new(TransportKind::Amqp),
            &["rabbit".to_owned()],
            "/",
            topology.queue("orders.created").expect("queue"),
            &[documented, unspecified],
        )
        .expect("projection records the unspecified authority");
        assert!(
            lily_asyncapi::__private::build_document(&config(), vec![contribution]).is_err(),
            "one accepted unspecified handler must fail the whole Consumer document"
        );
    }

    #[test]
    fn virtual_host_bound_is_exact() {
        assert!(validate_virtual_host("").is_ok());
        assert!(validate_virtual_host(&"v".repeat(255)).is_ok());
        assert!(validate_virtual_host(&"v".repeat(256)).is_err());
        assert!(validate_virtual_host("bad\nvirtual-host").is_err());
    }

    #[test]
    fn projection_preserves_the_runtime_empty_virtual_host() {
        let handler = metadata(
            1,
            "json",
            QueuePayloadKind::Json,
            QueueAsyncApiPayload::generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<OrderCreated>(),
                "application/json",
            ),
        );
        let topology = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition()],
        })
        .expect("topology");
        let contribution = project_queue(
            TransportContribution::new(TransportKind::Amqp),
            &["rabbit".to_owned()],
            "",
            topology.queue("orders.created").expect("queue"),
            &[handler],
        )
        .expect("projection");
        let (document, _) = lily_asyncapi::__private::build_document(&config(), vec![contribution])
            .expect("document");
        let document = serde_json::to_value(document).expect("document JSON");
        let channel = document["channels"]
            .as_object()
            .and_then(|channels| channels.values().next())
            .expect("channel");
        assert_eq!(channel["bindings"]["amqp"]["queue"]["vhost"], "");
    }

    #[test]
    fn external_classic_profile_is_projected_from_the_accepted_topology() {
        let handler = metadata(
            1,
            "json",
            QueuePayloadKind::Json,
            QueueAsyncApiPayload::generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<OrderCreated>(),
                "application/json",
            ),
        );
        let mut definition = definition();
        definition.queue_type = ConfigQueueType::Classic;
        definition.topology_ownership = ConfigOwnership::External;
        definition.retry_attempts = 0;
        definition.retry_backoff_millis = None;
        definition.max_retry_backoff_millis = None;
        definition.dead_letter_exchange = Some("external.orders.dlx".to_owned());
        definition.dead_letter_routing_key = Some("external.orders.failed".to_owned());
        let topology = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .expect("external classic topology");
        let contribution = project_queue(
            TransportContribution::new(TransportKind::Amqp),
            &["rabbit".to_owned()],
            "/",
            topology.queue("orders.created").expect("queue"),
            &[handler],
        )
        .expect("projection");
        let (document, _) = lily_asyncapi::__private::build_document(&config(), vec![contribution])
            .expect("document");
        let document = serde_json::to_value(document).expect("document JSON");
        let channel = document["channels"]
            .as_object()
            .and_then(|channels| channels.values().next())
            .expect("channel");
        let profile = &channel["x-lily-rabbitmq-topology"];
        assert_eq!(profile["ownership"], "external");
        assert_eq!(profile["queue_type"], "classic");
        assert!(profile.get("retry_exchange").is_none());
        assert!(profile.get("retry_buckets").is_none());
        assert!(profile.get("retry_attempts").is_none());
        assert_eq!(profile["dead_letter"]["exchange"], "external.orders.dlx");
        assert_eq!(
            profile["dead_letter"]["routing_key"],
            "external.orders.failed"
        );
    }

    #[test]
    fn inferred_and_explicit_payload_authorities_cannot_cross_contracts() {
        let explicit_json = metadata(
            3,
            "json",
            QueuePayloadKind::Json,
            QueueAsyncApiPayload::explicit_generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<OrderCreated>(),
                "application/xml",
            ),
        );
        assert!(payload_schema(explicit_json).is_err());

        let inferred_raw = metadata(
            4,
            "raw",
            QueuePayloadKind::Raw,
            QueueAsyncApiPayload::generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<OrderCreated>(),
                "application/json",
            ),
        );
        assert!(payload_schema(inferred_raw).is_err());
    }

    #[test]
    fn text_binary_raw_custom_and_body_unobserved_contracts_are_exact() {
        let text = metadata(
            5,
            "text",
            QueuePayloadKind::Text,
            QueueAsyncApiPayload::text(),
        );
        let (content_type, payload) = payload_schema(text).expect("text contract");
        assert_eq!(content_type, "text/plain; charset=utf-8");
        assert!(matches!(payload, PayloadSchema::String));

        let binary = metadata(
            6,
            "binary",
            QueuePayloadKind::Binary,
            QueueAsyncApiPayload::binary(),
        );
        let (content_type, payload) = payload_schema(binary).expect("binary contract");
        assert_eq!(content_type, "application/octet-stream");
        assert!(matches!(payload, PayloadSchema::Binary));

        for (version, kind) in [
            (7, QueuePayloadKind::Raw),
            (8, QueuePayloadKind::Custom),
            (9, QueuePayloadKind::None),
        ] {
            let opaque = metadata(
                version,
                "vendor",
                kind,
                QueueAsyncApiPayload::opaque("application/vnd.lily.event"),
            );
            let (content_type, payload) = payload_schema(opaque).expect("opaque contract");
            assert_eq!(content_type, "application/vnd.lily.event");
            assert!(matches!(payload, PayloadSchema::Opaque));
        }

        let raw_with_schema = metadata(
            10,
            "vendor",
            QueuePayloadKind::Raw,
            QueueAsyncApiPayload::explicit_generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<OrderCreated>(),
                "application/vnd.lily.order+json",
            ),
        );
        assert!(matches!(
            payload_schema(raw_with_schema),
            Ok((
                "application/vnd.lily.order+json",
                PayloadSchema::Generated(_)
            ))
        ));
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    #[test]
    fn postgresql_transactional_contract_documents_only_the_local_boundary() {
        let mut definition = definition();
        let policy = lily_config::TransactionalInboxConfig::default();
        definition.transactional_inbox = Some(policy.clone());
        assert_eq!(
            delivery_guarantee(&definition, DeliveryGuarantee::TransactionalInbox)
                .expect("PostgreSQL guarantee"),
            DeliveryGuaranteeExtension::TransactionalInbox(TransactionalInboxBackend::Postgresql)
        );
        assert_mixed_transactional_projection(policy, "postgresql");
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    #[test]
    fn mongodb_transactional_contract_documents_only_the_local_boundary() {
        let mut policy = lily_config::TransactionalInboxConfig::default();
        policy.backend = lily_config::TransactionalInboxBackend::MongoDb;
        let mut definition = definition();
        definition.transactional_inbox = Some(policy.clone());
        assert_eq!(
            delivery_guarantee(&definition, DeliveryGuarantee::TransactionalInbox)
                .expect("MongoDB guarantee"),
            DeliveryGuaranteeExtension::TransactionalInbox(TransactionalInboxBackend::Mongodb)
        );
        assert_mixed_transactional_projection(policy, "mongodb");
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    fn assert_mixed_transactional_projection(
        policy: lily_config::TransactionalInboxConfig,
        expected_backend: &str,
    ) {
        let at_least_once = metadata(
            1,
            "json",
            QueuePayloadKind::Json,
            QueueAsyncApiPayload::generated(
                lily_asyncapi::__private::SchemaFactory::inbound::<OrderCreated>(),
                "application/json",
            ),
        );
        let mut transactional = (*metadata(
            2,
            "binary",
            QueuePayloadKind::Binary,
            QueueAsyncApiPayload::binary(),
        ))
        .clone();
        transactional.delivery_guarantee = DeliveryGuarantee::TransactionalInbox;
        let transactional = Box::leak(Box::new(transactional));

        let mut definition = definition();
        definition.transactional_inbox = Some(policy);
        let topology = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .expect("topology");
        let contribution = project_queue(
            TransportContribution::new(TransportKind::Amqp),
            &["rabbit".to_owned()],
            "/",
            topology.queue("orders.created").expect("queue"),
            &[at_least_once, transactional],
        )
        .expect("mixed guarantee projection");
        let (document, _) = lily_asyncapi::__private::build_document(&config(), vec![contribution])
            .expect("mixed guarantee document");
        let document = serde_json::to_value(document).expect("document JSON");
        let operations = document["operations"].as_object().expect("operations");
        assert_eq!(operations.len(), 2);

        let at_least_once = operations
            .values()
            .map(|operation| &operation["x-lily-delivery-guarantee"])
            .find(|guarantee| guarantee["delivery_guarantee"] == "at_least_once")
            .expect("at-least-once operation");
        assert!(at_least_once.get("backend").is_none());
        assert_eq!(at_least_once["local_transaction"], false);
        assert_eq!(at_least_once["broker_database_2pc"], false);
        assert_eq!(at_least_once["global_exactly_once"], false);

        let transactional = operations
            .values()
            .map(|operation| &operation["x-lily-delivery-guarantee"])
            .find(|guarantee| guarantee["delivery_guarantee"] == "transactional_inbox")
            .expect("transactional operation");
        assert_eq!(transactional["backend"], expected_backend);
        assert_eq!(transactional["local_transaction"], true);
        assert_eq!(transactional["broker_database_2pc"], false);
        assert_eq!(transactional["global_exactly_once"], false);
    }
}
