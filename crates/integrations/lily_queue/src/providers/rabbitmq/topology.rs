use std::time::Duration;

use lily_config::{
    RabbitMqExchangeKind, RabbitMqQueueTopologyPlan, RabbitMqQueueType, RabbitMqTopologyOwnership,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetryBucket {
    pub(crate) delay: Duration,
    pub(crate) queue: String,
    pub(crate) routing_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueTopology {
    pub(crate) main_exchange: String,
    pub(crate) main_exchange_kind: RabbitMqExchangeKind,
    pub(crate) main_queue: String,
    pub(crate) routing_key: String,
    pub(crate) queue_type: RabbitMqQueueType,
    pub(crate) ownership: RabbitMqTopologyOwnership,
    pub(crate) single_active_consumer: bool,
    pub(crate) max_priority: Option<u8>,
    pub(crate) retry_exchange: String,
    pub(crate) retry_buckets: Vec<RetryBucket>,
    pub(crate) retry_bucket_delays_by_attempt: Vec<Vec<Duration>>,
    pub(crate) dead_letter_exchange: String,
    pub(crate) dead_letter_queue: String,
    pub(crate) dead_letter_routing_key: String,
}

impl QueueTopology {
    pub(crate) fn from_plan(plan: &RabbitMqQueueTopologyPlan) -> Self {
        let retry_buckets = plan
            .retry_buckets()
            .iter()
            .map(|bucket| RetryBucket {
                delay: Duration::from_millis(bucket.delay_millis()),
                queue: bucket.queue_name().to_string(),
                routing_key: bucket.routing_key().to_string(),
            })
            .collect();
        let definition = plan.definition();
        let retry_bucket_delays_by_attempt = (1..=definition.retry_attempts)
            .map(|retry_attempt| {
                plan.retry_bucket_delays(retry_attempt)
                    .unwrap_or_default()
                    .iter()
                    .copied()
                    .map(Duration::from_millis)
                    .collect()
            })
            .collect();
        Self {
            main_exchange: plan.exchange_name().to_string(),
            main_exchange_kind: plan.exchange_kind(),
            main_queue: plan.main_queue_name().to_string(),
            routing_key: plan.routing_key().to_string(),
            queue_type: plan.system_queue_type(),
            ownership: plan.topology_ownership(),
            single_active_consumer: definition.single_active_consumer,
            max_priority: definition.max_priority,
            retry_exchange: plan.retry_exchange_name().to_string(),
            retry_buckets,
            retry_bucket_delays_by_attempt,
            dead_letter_exchange: plan.dead_letter_exchange_name().to_string(),
            dead_letter_queue: plan.dead_letter_queue_name().to_string(),
            dead_letter_routing_key: plan.dead_letter_routing_key().to_string(),
        }
    }

    pub(crate) fn retry_bucket(&self, event_id: &str, retry_attempt: u32) -> Option<&RetryBucket> {
        let route_index = usize::try_from(retry_attempt.checked_sub(1)?).ok()?;
        let candidates = self.retry_bucket_delays_by_attempt.get(route_index)?;
        if candidates.is_empty() {
            return None;
        }
        let candidate_index = stable_retry_bucket_hash(event_id, retry_attempt)
            % u64::try_from(candidates.len()).ok()?;
        let delay = candidates[usize::try_from(candidate_index).ok()?];
        self.retry_buckets
            .iter()
            .find(|bucket| bucket.delay == delay)
    }
}

fn stable_retry_bucket_hash(event_id: &str, retry_attempt: u32) -> u64 {
    // FNV-1a is deliberately fixed here rather than using DefaultHasher,
    // whose algorithm is not a stable cross-release contract. This is routing
    // spread, not a security boundary.
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    event_id
        .as_bytes()
        .iter()
        .copied()
        .chain(retry_attempt.to_be_bytes())
        .fold(FNV_OFFSET_BASIS, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_config::{
        QueueDefinition, QueueRetentionConfig, RabbitMqQueueType, RabbitMqTopologyConfig,
        RabbitMqTopologyOwnership, RabbitMqTopologyPlan,
    };

    fn topology(definition: QueueDefinition) -> QueueTopology {
        let queue_name = definition.name.clone();
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .unwrap();
        QueueTopology::from_plan(plan.queue(&queue_name).unwrap())
    }

    fn definition() -> QueueDefinition {
        QueueDefinition {
            name: "order.created".into(),
            exchange_name: "orders".into(),
            routing_key: "order.created".into(),
            retry_attempts: 3,
            retry_backoff_millis: Some(1_000),
            max_retry_backoff_millis: Some(5_000),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100,
                main_max_bytes: 1024 * 1024,
                retry_bucket_max_messages: 100,
                retry_bucket_max_bytes: 1024 * 1024,
                dead_letter_max_messages: 100,
                dead_letter_max_bytes: 1024 * 1024,
            }),
            ..QueueDefinition::default()
        }
    }

    #[test]
    fn topology_names_are_versioned_and_retry_delay_is_bounded() {
        let topology = topology(definition());
        assert_eq!(topology.retry_exchange, "orders.retry.v2");
        assert_eq!(topology.dead_letter_queue, "order.created.dlq.v2");
        assert_eq!(topology.retry_buckets.len(), 3);
        assert_eq!(
            topology.retry_buckets[0].queue,
            "order.created.retry.v2.1000ms"
        );
    }

    #[test]
    fn accepted_plan_has_no_retry_buckets_when_retry_is_disabled() {
        let mut definition = definition();
        definition.retry_attempts = 0;
        assert!(topology(definition).retry_buckets.is_empty());
    }

    #[test]
    fn zero_jitter_selects_the_exact_nominal_bucket() {
        let topology = topology(definition());
        let event_id = "11111111-1111-4111-8111-111111111111";

        assert_eq!(
            topology
                .retry_bucket(event_id, 1)
                .map(|bucket| bucket.delay),
            Some(Duration::from_millis(1_000))
        );
        assert_eq!(
            topology
                .retry_bucket(event_id, 2)
                .map(|bucket| bucket.delay),
            Some(Duration::from_millis(2_000))
        );
        assert_eq!(
            topology
                .retry_bucket(event_id, 3)
                .map(|bucket| bucket.delay),
            Some(Duration::from_millis(4_000))
        );
        assert!(topology.retry_bucket(event_id, 0).is_none());
        assert!(topology.retry_bucket(event_id, 4).is_none());
    }

    #[test]
    fn jitter_bucket_selection_is_stable_and_bounded_by_the_compiled_plan() {
        let mut definition = definition();
        definition.retry_jitter_ratio = 0.2;
        let topology = topology(definition);
        let event_id = "11111111-1111-4111-8111-111111111111";

        let selected = topology
            .retry_bucket(event_id, 2)
            .expect("compiled retry route must select one bucket");
        assert_eq!(selected.delay, Duration::from_millis(2_000));
        for _ in 0..32 {
            assert_eq!(
                topology
                    .retry_bucket(event_id, 2)
                    .expect("same identity and attempt must remain routable")
                    .delay,
                selected.delay
            );
        }
        assert!(
            [
                Duration::from_millis(1_600),
                Duration::from_millis(2_000),
                Duration::from_millis(2_400),
            ]
            .contains(&selected.delay)
        );
        assert_eq!(
            stable_retry_bucket_hash(event_id, 2),
            0x97ef_60b7_b59d_4439,
            "the cross-node retry routing hash is a frozen protocol detail"
        );
        let distributed = [
            "11111111-1111-4111-8111-000000000002",
            "11111111-1111-4111-8111-000000000001",
            "11111111-1111-4111-8111-000000000022",
        ]
        .map(|event_id| topology.retry_bucket(event_id, 2).unwrap().delay);
        assert_eq!(
            distributed,
            [
                Duration::from_millis(1_600),
                Duration::from_millis(2_000),
                Duration::from_millis(2_400),
            ]
        );
    }

    #[test]
    fn topology_freezes_typed_config_authority() {
        let mut definition = definition();
        definition.exchange_name = "domain.events".into();
        definition.routing_key = "orders.created.v2".into();
        definition.queue_type = RabbitMqQueueType::Quorum;
        definition.topology_ownership = RabbitMqTopologyOwnership::External;
        definition.single_active_consumer = true;
        definition.dead_letter_exchange = Some("domain.dead-letters".into());
        definition.dead_letter_routing_key = Some("orders.created.dead".into());

        let topology = topology(definition);

        assert_eq!(topology.main_exchange, "domain.events");
        assert_eq!(topology.routing_key, "orders.created.v2");
        assert_eq!(topology.queue_type, RabbitMqQueueType::Quorum);
        assert_eq!(topology.ownership, RabbitMqTopologyOwnership::External);
        assert!(topology.single_active_consumer);
    }
}
