use std::{collections::HashMap, sync::Arc, time::Duration};

use lily_config::{
    QueueDefinition, RabbitMqExchangeKind, RabbitMqQueueType, RabbitMqTopologyConfig,
    RabbitMqTopologyOwnership, RabbitMqTopologyPlan,
};
use lily_error::application::{MessageBrokerError, message_broker::RabbitMQError};

use crate::delivery_context::{
    MAX_QUEUE_TRANSPORT_IDENTITY_BYTES, queue_transport_identity_is_valid,
};
use crate::providers::rabbitmq::topology::QueueTopology;

pub(crate) const MAX_QUEUE_CONCURRENCY: u32 = 1_024;
pub(crate) const MAX_QUEUE_PREFETCH: u16 = 4_096;
pub(crate) const MAX_QUEUE_MESSAGE_SIZE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_QUEUE_RETRY_ATTEMPTS: u32 = 100;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueRetentionSetting {
    pub(crate) main_max_messages: u64,
    pub(crate) main_max_bytes: u64,
    pub(crate) retry_bucket_max_messages: u64,
    pub(crate) retry_bucket_max_bytes: u64,
    pub(crate) dead_letter_max_messages: u64,
    pub(crate) dead_letter_max_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueRuntimeSetting {
    pub(crate) exchange_name: String,
    pub(crate) routing_key: String,
    pub(crate) exchange_kind: RabbitMqExchangeKind,
    pub(crate) queue_type: RabbitMqQueueType,
    pub(crate) topology_ownership: RabbitMqTopologyOwnership,
    pub(crate) concurrency: usize,
    pub(crate) prefetch_count: u16,
    pub(crate) delivery_buffer_capacity: usize,
    pub(crate) max_message_size_bytes: usize,
    pub(crate) retention: QueueRetentionSetting,
    pub(crate) retry_attempts: u32,
    pub(crate) retry_backoff: Duration,
    pub(crate) max_retry_backoff: Duration,
    pub(crate) delivery_execution_timeout: Duration,
    pub(crate) settlement_timeout: Duration,
    pub(crate) durable: bool,
    pub(crate) dead_letter_exchange: Option<String>,
    pub(crate) dead_letter_routing_key: Option<String>,
    pub(crate) message_ttl_ms: Option<u32>,
    pub(crate) exclusive: bool,
    pub(crate) auto_delete: bool,
    pub(crate) single_active_consumer: bool,
    pub(crate) max_priority: Option<u8>,
}

impl QueueRuntimeSetting {
    pub(crate) fn from_definition(
        definition: &QueueDefinition,
    ) -> Result<Self, MessageBrokerError> {
        if !queue_transport_identity_is_valid(&definition.name)
            || !queue_transport_identity_is_valid(&definition.exchange_name)
            || !queue_transport_identity_is_valid(&definition.routing_key)
        {
            return Err(configuration(format!(
                "queue {:?} name, exchange_name and routing_key must contain 1..={} trimmed, control-free bytes",
                definition.name, MAX_QUEUE_TRANSPORT_IDENTITY_BYTES,
            )));
        }
        if !(1..=16).contains(&definition.max_priority.unwrap_or(1)) {
            return Err(configuration(format!(
                "queue {:?} max_priority must be between 1 and 16",
                definition.name,
            )));
        }
        if definition.queue_type == RabbitMqQueueType::Quorum {
            if !definition.durable {
                return Err(configuration(format!(
                    "quorum queue {:?} must be durable",
                    definition.name,
                )));
            }
            if definition.exclusive {
                return Err(configuration(format!(
                    "quorum queue {:?} cannot be exclusive",
                    definition.name,
                )));
            }
            if definition.auto_delete {
                return Err(configuration(format!(
                    "quorum queue {:?} cannot be auto-delete",
                    definition.name,
                )));
            }
            if definition.max_priority.is_some() {
                return Err(configuration(format!(
                    "quorum queue {:?} cannot declare max_priority",
                    definition.name,
                )));
            }
        }
        if !(1..=MAX_QUEUE_CONCURRENCY).contains(&definition.concurrency) {
            return Err(configuration(format!(
                "queue {:?} concurrency must be between 1 and {}",
                definition.name, MAX_QUEUE_CONCURRENCY
            )));
        }
        if !(1..=MAX_QUEUE_PREFETCH).contains(&definition.prefetch_count) {
            return Err(configuration(format!(
                "queue {:?} prefetch_count must be between 1 and {}",
                definition.name, MAX_QUEUE_PREFETCH
            )));
        }
        let delivery_buffer_capacity = definition
            .delivery_buffer_capacity
            .unwrap_or_else(|| usize::from(definition.prefetch_count));
        if delivery_buffer_capacity == 0
            || delivery_buffer_capacity > usize::from(definition.prefetch_count)
        {
            return Err(configuration(format!(
                "queue {:?} delivery_buffer_capacity must be between 1 and prefetch_count ({})",
                definition.name, definition.prefetch_count
            )));
        }
        if !(1..=MAX_QUEUE_MESSAGE_SIZE_BYTES).contains(&definition.max_message_size_bytes) {
            return Err(configuration(format!(
                "queue {:?} max_message_size_bytes must be between 1 and {}",
                definition.name, MAX_QUEUE_MESSAGE_SIZE_BYTES
            )));
        }
        let retention = definition.retention.as_ref().ok_or_else(|| {
            configuration(format!(
                "queue {:?} must define explicit main/retry-bucket/dead-letter retention bounds",
                definition.name
            ))
        })?;
        let retention_fields = [
            ("main_max_messages", retention.main_max_messages),
            ("main_max_bytes", retention.main_max_bytes),
            (
                "retry_bucket_max_messages",
                retention.retry_bucket_max_messages,
            ),
            ("retry_bucket_max_bytes", retention.retry_bucket_max_bytes),
            (
                "dead_letter_max_messages",
                retention.dead_letter_max_messages,
            ),
            ("dead_letter_max_bytes", retention.dead_letter_max_bytes),
        ];
        for (field, value) in retention_fields {
            if value == 0 || value > i64::MAX as u64 {
                return Err(configuration(format!(
                    "queue {:?} retention.{field} must be between 1 and {}",
                    definition.name,
                    i64::MAX
                )));
            }
        }
        if definition.retry_attempts > MAX_QUEUE_RETRY_ATTEMPTS {
            return Err(configuration(format!(
                "queue {:?} retry_attempts must be at most {MAX_QUEUE_RETRY_ATTEMPTS}",
                definition.name,
            )));
        }
        if !definition.retry_jitter_ratio.is_finite()
            || !(0.0..=0.5).contains(&definition.retry_jitter_ratio)
        {
            return Err(configuration(format!(
                "queue {:?} retry_jitter_ratio must be finite and between 0.0 and 0.5",
                definition.name,
            )));
        }
        for (field, value) in [
            (
                "dead_letter_exchange",
                definition.dead_letter_exchange.as_deref(),
            ),
            (
                "dead_letter_routing_key",
                definition.dead_letter_routing_key.as_deref(),
            ),
        ] {
            if value.is_some_and(|value| !queue_transport_identity_is_valid(value)) {
                return Err(configuration(format!(
                    "queue {:?} {field} must contain 1..={} trimmed, control-free bytes",
                    definition.name, MAX_QUEUE_TRANSPORT_IDENTITY_BYTES,
                )));
            }
        }
        if definition.message_ttl_ms == Some(0) {
            return Err(configuration(format!(
                "queue {:?} message_ttl_ms must be greater than zero",
                definition.name
            )));
        }
        let retry_backoff_millis = definition.retry_backoff_millis.unwrap_or(1_000);
        let max_retry_backoff_millis = definition.max_retry_backoff_millis.unwrap_or(60_000);
        let delivery_execution_timeout_millis = definition
            .delivery_execution_timeout_millis
            .unwrap_or(30_000);
        let settlement_timeout_millis = definition.settlement_timeout_millis.unwrap_or(5_000);
        if !(100..=300_000).contains(&retry_backoff_millis)
            || !(retry_backoff_millis..=900_000).contains(&max_retry_backoff_millis)
        {
            return Err(configuration(format!(
                "queue {:?} retry backoff must satisfy 100 <= retry_backoff_millis <= max_retry_backoff_millis <= 900000",
                definition.name
            )));
        }
        if !(100..=900_000).contains(&delivery_execution_timeout_millis) {
            return Err(configuration(format!(
                "queue {:?} delivery_execution_timeout_millis must be between 100 and 900000",
                definition.name
            )));
        }
        if !(1..=60_000).contains(&settlement_timeout_millis) {
            return Err(configuration(format!(
                "queue {:?} settlement_timeout_millis must be between 1 and 60000",
                definition.name
            )));
        }

        Ok(Self {
            exchange_name: definition.exchange_name.clone(),
            routing_key: definition.routing_key.clone(),
            exchange_kind: definition.exchange_kind,
            queue_type: definition.queue_type,
            topology_ownership: definition.topology_ownership,
            concurrency: usize::try_from(definition.concurrency).map_err(|_| {
                configuration(format!(
                    "queue {:?} concurrency exceeds platform range",
                    definition.name
                ))
            })?,
            prefetch_count: definition.prefetch_count,
            delivery_buffer_capacity,
            max_message_size_bytes: definition.max_message_size_bytes,
            retention: QueueRetentionSetting {
                main_max_messages: retention.main_max_messages,
                main_max_bytes: retention.main_max_bytes,
                retry_bucket_max_messages: retention.retry_bucket_max_messages,
                retry_bucket_max_bytes: retention.retry_bucket_max_bytes,
                dead_letter_max_messages: retention.dead_letter_max_messages,
                dead_letter_max_bytes: retention.dead_letter_max_bytes,
            },
            retry_attempts: definition.retry_attempts,
            retry_backoff: Duration::from_millis(retry_backoff_millis),
            max_retry_backoff: Duration::from_millis(max_retry_backoff_millis),
            delivery_execution_timeout: Duration::from_millis(delivery_execution_timeout_millis),
            settlement_timeout: Duration::from_millis(settlement_timeout_millis),
            durable: definition.durable,
            dead_letter_exchange: definition.dead_letter_exchange.clone(),
            dead_letter_routing_key: definition.dead_letter_routing_key.clone(),
            message_ttl_ms: definition.message_ttl_ms,
            exclusive: definition.exclusive,
            auto_delete: definition.auto_delete,
            single_active_consumer: definition.single_active_consumer,
            max_priority: definition.max_priority,
        })
    }
}

#[derive(Clone)]
pub(crate) struct MessageBrokerSetting {
    pub(crate) confirm_timeout: Duration,
    queues: Arc<HashMap<String, QueueRuntimePlan>>,
    topology_plan: Arc<RabbitMqTopologyPlan>,
}

#[derive(Clone)]
struct QueueRuntimePlan {
    setting: QueueRuntimeSetting,
    topology: QueueTopology,
}

impl MessageBrokerSetting {
    pub(crate) fn from_definitions(
        confirm_timeout: Duration,
        definitions: &[QueueDefinition],
    ) -> Result<Self, MessageBrokerError> {
        let topology_plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: definitions.to_vec(),
        })
        .map_err(|error| configuration(error.to_string()))?;
        Self::from_topology_plan(confirm_timeout, &topology_plan)
    }

    pub(crate) fn from_topology_plan(
        confirm_timeout: Duration,
        topology_plan: &RabbitMqTopologyPlan,
    ) -> Result<Self, MessageBrokerError> {
        let mut queues = HashMap::with_capacity(topology_plan.len());
        for accepted in topology_plan.queues() {
            let definition = accepted.definition();
            let setting = QueueRuntimeSetting::from_definition(definition)?;
            let topology = QueueTopology::from_plan(accepted);
            if queues
                .insert(
                    definition.name.clone(),
                    QueueRuntimePlan { setting, topology },
                )
                .is_some()
            {
                return Err(configuration(format!(
                    "duplicate queue configuration {:?}",
                    definition.name
                )));
            }
        }
        Ok(Self {
            confirm_timeout,
            queues: Arc::new(queues),
            topology_plan: Arc::new(topology_plan.clone()),
        })
    }

    pub(crate) fn queue(&self, name: &str) -> Result<&QueueRuntimeSetting, MessageBrokerError> {
        self.queue_plan(name).map(|plan| &plan.setting)
    }

    pub(crate) fn topology(&self, name: &str) -> Result<&QueueTopology, MessageBrokerError> {
        self.queue_plan(name).map(|plan| &plan.topology)
    }

    pub(crate) fn topology_plan(&self) -> &RabbitMqTopologyPlan {
        &self.topology_plan
    }

    fn queue_plan(&self, name: &str) -> Result<&QueueRuntimePlan, MessageBrokerError> {
        self.queues.get(name).ok_or_else(|| {
            configuration(format!(
                "queue {name:?} has handler metadata but no [[rabbitmq.topology.queues]] configuration"
            ))
        })
    }
}

fn configuration(message: impl Into<String>) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_config::{QueueRetentionConfig, RabbitMqQueueType, RabbitMqTopologyOwnership};

    fn queue(name: &str) -> QueueDefinition {
        QueueDefinition {
            name: name.into(),
            exchange_name: "events".into(),
            routing_key: name.into(),
            retry_backoff_millis: Some(250),
            max_retry_backoff_millis: Some(5_000),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100_000,
                main_max_bytes: 1024 * 1024 * 1024,
                retry_bucket_max_messages: 10_000,
                retry_bucket_max_bytes: 256 * 1024 * 1024,
                dead_letter_max_messages: 10_000,
                dead_letter_max_bytes: 256 * 1024 * 1024,
            }),
            ..QueueDefinition::default()
        }
    }

    #[test]
    fn validates_queue_policy_and_rejects_duplicates() {
        let setting =
            MessageBrokerSetting::from_definitions(Duration::from_secs(2), &[queue("orders")])
                .unwrap();
        let queue_setting = setting.queue("orders").unwrap();
        assert_eq!(queue_setting.retry_attempts, 0);
        assert_eq!(queue_setting.concurrency, 1);
        assert_eq!(queue_setting.prefetch_count, 10);
        assert_eq!(queue_setting.delivery_buffer_capacity, 10);
        assert_eq!(queue_setting.max_message_size_bytes, 1024 * 1024);
        assert_eq!(queue_setting.retention.main_max_messages, 100_000);
        assert_eq!(
            queue_setting.delivery_execution_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(queue_setting.settlement_timeout, Duration::from_secs(5));
        assert_eq!(queue_setting.exchange_name, "events");
        assert_eq!(queue_setting.routing_key, "orders");
        assert_eq!(setting.topology("orders").unwrap().main_exchange, "events");
        assert!(
            MessageBrokerSetting::from_definitions(
                Duration::from_secs(2),
                &[queue("orders"), queue("orders")],
            )
            .is_err()
        );

        let mut unsafe_queue = queue("unsafe");
        unsafe_queue.delivery_buffer_capacity = Some(11);
        assert!(
            MessageBrokerSetting::from_definitions(Duration::from_secs(2), &[unsafe_queue],)
                .is_err()
        );

        let mut oversized_policy = queue("oversized-policy");
        oversized_policy.max_message_size_bytes = MAX_QUEUE_MESSAGE_SIZE_BYTES + 1;
        assert!(QueueRuntimeSetting::from_definition(&oversized_policy).is_err());

        let mut missing_retention = queue("missing-retention");
        missing_retention.retention = None;
        assert!(QueueRuntimeSetting::from_definition(&missing_retention).is_err());

        for accepted in [1, 60_000] {
            let mut definition = queue("settlement-boundary");
            definition.settlement_timeout_millis = Some(accepted);
            assert!(QueueRuntimeSetting::from_definition(&definition).is_ok());
        }
        for rejected in [0, 60_001] {
            let mut definition = queue("settlement-out-of-range");
            definition.settlement_timeout_millis = Some(rejected);
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());
        }

        for accepted in [0.0, 0.5] {
            let mut definition = queue("jitter-boundary");
            definition.retry_jitter_ratio = accepted;
            assert!(QueueRuntimeSetting::from_definition(&definition).is_ok());
        }
        for rejected in [-0.1, 0.500_001, f64::NAN, f64::INFINITY] {
            let mut definition = queue("jitter-out-of-range");
            definition.retry_jitter_ratio = rejected;
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());
        }
    }

    #[test]
    fn preserves_independent_concurrency_prefetch_and_retry_authorities() {
        let mut definition = queue("orders");
        definition.concurrency = 4;
        definition.prefetch_count = 20;
        definition.delivery_buffer_capacity = Some(12);
        definition.retry_attempts = 3;

        let setting = QueueRuntimeSetting::from_definition(&definition).unwrap();
        assert_eq!(setting.concurrency, 4);
        assert_eq!(setting.prefetch_count, 20);
        assert_eq!(setting.delivery_buffer_capacity, 12);
        assert_eq!(setting.retry_attempts, 3);
    }

    #[test]
    fn transport_identities_share_exact_trim_control_and_byte_bounds() {
        let mut exact = queue(&"q".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES));
        exact.exchange_name = "e".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES);
        exact.routing_key = "r".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES);
        exact.dead_letter_exchange = Some("d".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES));
        exact.dead_letter_routing_key = Some("r".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES));
        assert!(QueueRuntimeSetting::from_definition(&exact).is_ok());

        for invalid in [
            " queue".to_string(),
            "queue ".to_string(),
            "queue\rforged".to_string(),
            "q".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES + 1),
        ] {
            let mut definition = queue("queue");
            definition.name = invalid.clone();
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());

            let mut definition = queue("queue");
            definition.exchange_name = invalid.clone();
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());

            let mut definition = queue("queue");
            definition.routing_key = invalid;
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());
        }

        for invalid in [" exchange", "routing\nforged"] {
            let mut definition = queue("queue");
            definition.dead_letter_exchange = Some(invalid.to_string());
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());

            let mut definition = queue("queue");
            definition.dead_letter_routing_key = Some(invalid.to_string());
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());
        }
    }

    #[test]
    fn rejects_invalid_quorum_and_priority_combinations_before_broker_io() {
        for mutate in [
            |definition: &mut QueueDefinition| definition.durable = false,
            |definition: &mut QueueDefinition| definition.exclusive = true,
            |definition: &mut QueueDefinition| definition.auto_delete = true,
            |definition: &mut QueueDefinition| definition.max_priority = Some(10),
        ] {
            let mut definition = queue("quorum");
            definition.queue_type = RabbitMqQueueType::Quorum;
            mutate(&mut definition);
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());
        }

        for priority in [Some(1), Some(16), None] {
            let mut definition = queue("classic");
            definition.max_priority = priority;
            assert!(QueueRuntimeSetting::from_definition(&definition).is_ok());
        }
        for priority in [Some(0), Some(17)] {
            let mut definition = queue("classic");
            definition.max_priority = priority;
            assert!(QueueRuntimeSetting::from_definition(&definition).is_err());
        }

        let mut external = queue("external");
        external.topology_ownership = RabbitMqTopologyOwnership::External;
        let setting = QueueRuntimeSetting::from_definition(&external).unwrap();
        assert_eq!(
            setting.topology_ownership,
            RabbitMqTopologyOwnership::External
        );
    }
}
