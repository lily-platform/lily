#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use lily_config::{
    QueueDefinition, QueueRetentionConfig, RabbitMqExchangeKind, RabbitMqQueueType,
    RabbitMqTopologyOwnership,
};
use lily_consumer::fuzzing::{
    build_consumer_plan, validate_consumer_config, ConsumerHandlerInput, ConsumerTraceCellInput,
};
use serde::Deserialize;

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize, Arbitrary)]
#[serde(default)]
struct PlanInput {
    definitions: Vec<QueueInput>,
    handlers: Vec<HandlerInput>,
    trace_cells: Vec<TraceCellInput>,
}

impl Default for PlanInput {
    fn default() -> Self {
        Self {
            definitions: Vec::new(),
            handlers: Vec::new(),
            trace_cells: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Arbitrary)]
#[serde(default)]
struct QueueInput {
    name: String,
    exchange_name: String,
    routing_key: String,
    exchange_kind: ExchangeKindInput,
    queue_type: QueueTypeInput,
    topology_ownership: TopologyOwnershipInput,
    concurrency: u32,
    prefetch_count: u16,
    delivery_buffer_capacity: Option<usize>,
    retry_attempts: u32,
    retry_backoff_millis: Option<u64>,
    max_retry_backoff_millis: Option<u64>,
    retry_jitter_ratio: f64,
    delivery_execution_timeout_millis: Option<u64>,
    settlement_timeout_millis: Option<u64>,
    durable: bool,
    max_message_size_bytes: usize,
    retention: Option<QueueRetentionInput>,
    dead_letter_exchange: Option<String>,
    dead_letter_routing_key: Option<String>,
    message_ttl_ms: Option<u32>,
    exclusive: bool,
    auto_delete: bool,
    single_active_consumer: bool,
    max_priority: Option<u8>,
}

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(rename_all = "snake_case")]
enum ExchangeKindInput {
    #[default]
    Direct,
}

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(rename_all = "snake_case")]
enum QueueTypeInput {
    #[default]
    Classic,
    Quorum,
}

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(rename_all = "snake_case")]
enum TopologyOwnershipInput {
    #[default]
    FrameworkManaged,
    External,
}

#[derive(Debug, Deserialize, Arbitrary)]
struct QueueRetentionInput {
    main_max_messages: u64,
    main_max_bytes: u64,
    retry_bucket_max_messages: u64,
    retry_bucket_max_bytes: u64,
    dead_letter_max_messages: u64,
    dead_letter_max_bytes: u64,
}

impl From<QueueRetentionInput> for QueueRetentionConfig {
    fn from(input: QueueRetentionInput) -> Self {
        Self {
            main_max_messages: input.main_max_messages,
            main_max_bytes: input.main_max_bytes,
            retry_bucket_max_messages: input.retry_bucket_max_messages,
            retry_bucket_max_bytes: input.retry_bucket_max_bytes,
            dead_letter_max_messages: input.dead_letter_max_messages,
            dead_letter_max_bytes: input.dead_letter_max_bytes,
        }
    }
}

impl Default for QueueInput {
    fn default() -> Self {
        let definition = QueueDefinition::default();
        Self {
            name: definition.name,
            exchange_name: definition.exchange_name,
            routing_key: definition.routing_key,
            exchange_kind: ExchangeKindInput::Direct,
            queue_type: match definition.queue_type {
                RabbitMqQueueType::Classic => QueueTypeInput::Classic,
                RabbitMqQueueType::Quorum => QueueTypeInput::Quorum,
            },
            topology_ownership: match definition.topology_ownership {
                RabbitMqTopologyOwnership::FrameworkManaged => {
                    TopologyOwnershipInput::FrameworkManaged
                }
                RabbitMqTopologyOwnership::External => TopologyOwnershipInput::External,
            },
            concurrency: definition.concurrency,
            prefetch_count: definition.prefetch_count,
            delivery_buffer_capacity: definition.delivery_buffer_capacity,
            retry_attempts: definition.retry_attempts,
            retry_backoff_millis: definition.retry_backoff_millis,
            max_retry_backoff_millis: definition.max_retry_backoff_millis,
            retry_jitter_ratio: definition.retry_jitter_ratio,
            delivery_execution_timeout_millis: definition.delivery_execution_timeout_millis,
            settlement_timeout_millis: definition.settlement_timeout_millis,
            durable: definition.durable,
            max_message_size_bytes: definition.max_message_size_bytes,
            retention: definition.retention.map(|retention| QueueRetentionInput {
                main_max_messages: retention.main_max_messages,
                main_max_bytes: retention.main_max_bytes,
                retry_bucket_max_messages: retention.retry_bucket_max_messages,
                retry_bucket_max_bytes: retention.retry_bucket_max_bytes,
                dead_letter_max_messages: retention.dead_letter_max_messages,
                dead_letter_max_bytes: retention.dead_letter_max_bytes,
            }),
            dead_letter_exchange: definition.dead_letter_exchange,
            dead_letter_routing_key: definition.dead_letter_routing_key,
            message_ttl_ms: definition.message_ttl_ms,
            exclusive: definition.exclusive,
            auto_delete: definition.auto_delete,
            single_active_consumer: definition.single_active_consumer,
            max_priority: definition.max_priority,
        }
    }
}

impl From<QueueInput> for QueueDefinition {
    fn from(input: QueueInput) -> Self {
        Self {
            name: input.name,
            exchange_name: input.exchange_name,
            routing_key: input.routing_key,
            exchange_kind: match input.exchange_kind {
                ExchangeKindInput::Direct => RabbitMqExchangeKind::Direct,
            },
            queue_type: match input.queue_type {
                QueueTypeInput::Classic => RabbitMqQueueType::Classic,
                QueueTypeInput::Quorum => RabbitMqQueueType::Quorum,
            },
            topology_ownership: match input.topology_ownership {
                TopologyOwnershipInput::FrameworkManaged => {
                    RabbitMqTopologyOwnership::FrameworkManaged
                }
                TopologyOwnershipInput::External => RabbitMqTopologyOwnership::External,
            },
            concurrency: input.concurrency,
            prefetch_count: input.prefetch_count,
            delivery_buffer_capacity: input.delivery_buffer_capacity,
            retry_attempts: input.retry_attempts,
            retry_backoff_millis: input.retry_backoff_millis,
            max_retry_backoff_millis: input.max_retry_backoff_millis,
            retry_jitter_ratio: input.retry_jitter_ratio,
            delivery_execution_timeout_millis: input.delivery_execution_timeout_millis,
            settlement_timeout_millis: input.settlement_timeout_millis,
            durable: input.durable,
            max_message_size_bytes: input.max_message_size_bytes,
            retention: input.retention.map(Into::into),
            dead_letter_exchange: input.dead_letter_exchange,
            dead_letter_routing_key: input.dead_letter_routing_key,
            message_ttl_ms: input.message_ttl_ms,
            exclusive: input.exclusive,
            auto_delete: input.auto_delete,
            single_active_consumer: input.single_active_consumer,
            max_priority: input.max_priority,
        }
    }
}

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
struct HandlerInput {
    queue_name: String,
    service_type_name: String,
    component_kind: Option<String>,
    method_name: String,
    handler_name: String,
    schema_version: u16,
    content_kind: String,
    payload_kind: u8,
    payload_type_name: Option<String>,
}

impl From<HandlerInput> for ConsumerHandlerInput {
    fn from(input: HandlerInput) -> Self {
        Self {
            queue_name: input.queue_name,
            service_type_name: input.service_type_name,
            component_kind: input.component_kind,
            method_name: input.method_name,
            handler_name: input.handler_name,
            schema_version: input.schema_version,
            content_kind: input.content_kind,
            payload_kind: input.payload_kind,
            payload_type_name: input.payload_type_name,
        }
    }
}

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
struct TraceCellInput {
    id: String,
    worker_type: String,
    worker_name: String,
    kind: String,
}

impl From<TraceCellInput> for ConsumerTraceCellInput {
    fn from(input: TraceCellInput) -> Self {
        Self {
            id: input.id,
            worker_type: input.worker_type,
            worker_name: input.worker_name,
            kind: input.kind,
        }
    }
}

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let Some((&selector, input)) = bytes.split_first() else {
        return;
    };
    let input = if selector == b'j' {
        serde_json::from_slice::<PlanInput>(input).ok()
    } else {
        PlanInput::arbitrary(&mut Unstructured::new(input)).ok()
    };
    let Some(input) = input else {
        return;
    };

    let definitions = input
        .definitions
        .into_iter()
        .map(QueueDefinition::from)
        .collect::<Vec<_>>();
    let handlers = input
        .handlers
        .into_iter()
        .map(ConsumerHandlerInput::from)
        .collect::<Vec<_>>();
    let trace_cells = input
        .trace_cells
        .into_iter()
        .map(ConsumerTraceCellInput::from)
        .collect::<Vec<_>>();

    if let Ok(plan) = build_consumer_plan(&definitions, &handlers, trace_cells) {
        assert_eq!(plan.bindings.len(), definitions.len());
        assert!(validate_consumer_config(&definitions).is_ok());
        for (queue_index, binding) in plan.bindings.iter().enumerate() {
            assert_eq!(binding.queue_index, queue_index);
            assert!(!binding.handlers.is_empty());
            for handler in &binding.handlers {
                assert!(handler.handler_index < handlers.len());
            }
        }
    }
});
