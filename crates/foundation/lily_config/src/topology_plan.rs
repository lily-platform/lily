use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

use crate::secret::parse_config_reference;
use crate::{
    QueueDefinition, RabbitMqExchangeKind, RabbitMqQueueType, RabbitMqTopologyConfig,
    RabbitMqTopologyOwnership,
};

const TOPOLOGY_VERSION: &str = "v2";
const MAX_IDENTITY_BYTES: usize = 200;
const MAX_QUEUE_CONCURRENCY: u32 = 1_024;
const MAX_QUEUE_PREFETCH: u16 = 4_096;
const MAX_QUEUE_MESSAGE_SIZE_BYTES: usize = 16 * 1024 * 1024;
const MAX_QUEUE_RETRY_ATTEMPTS: u32 = 100;

/// Maximum number of distinct physical retry-delay queues for one logical queue.
pub const MAX_RETRY_BUCKETS_PER_QUEUE: usize = 32;

/// Maximum number of distinct physical retry-delay queues in one topology plan.
pub const MAX_RETRY_BUCKETS_PER_TOPOLOGY: usize = 4_096;

/// Immutable, broker-free projection of the accepted RabbitMQ topology.
///
/// The plan contains no connection URI or credential material. Consumers,
/// publishers, explicit topology bootstrap and future AsyncAPI projection can
/// therefore share it without creating a second topology authority. Protected
/// config references are not accepted as topology identities.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct RabbitMqTopologyPlan {
    queues: Vec<RabbitMqQueueTopologyPlan>,
}

impl RabbitMqTopologyPlan {
    /// Validates and compiles the canonical RabbitMQ topology configuration.
    pub fn compile(config: &RabbitMqTopologyConfig) -> Result<Self, RabbitMqTopologyPlanError> {
        let mut logical_queues = HashSet::with_capacity(config.queues.len());
        let mut physical_queues = HashSet::new();
        let mut exchanges = HashMap::new();
        let mut queues = Vec::with_capacity(config.queues.len());
        let mut retry_bucket_count = 0usize;

        for definition in &config.queues {
            if !logical_queues.insert(definition.name.as_str()) {
                return Err(RabbitMqTopologyPlanError::DuplicateQueueName {
                    name: definition.name.clone(),
                });
            }

            validate_definition(definition)?;
            let queue = RabbitMqQueueTopologyPlan::compile(definition)?;
            retry_bucket_count = retry_bucket_count
                .checked_add(queue.retry_buckets().len())
                .ok_or(
                    RabbitMqTopologyPlanError::TopologyRetryBucketLimitExceeded {
                        count: usize::MAX,
                        maximum: MAX_RETRY_BUCKETS_PER_TOPOLOGY,
                    },
                )?;
            if retry_bucket_count > MAX_RETRY_BUCKETS_PER_TOPOLOGY {
                return Err(
                    RabbitMqTopologyPlanError::TopologyRetryBucketLimitExceeded {
                        count: retry_bucket_count,
                        maximum: MAX_RETRY_BUCKETS_PER_TOPOLOGY,
                    },
                );
            }

            register_exchange(
                &mut exchanges,
                queue.exchange_name(),
                queue.exchange_kind(),
                queue.topology_ownership(),
            )?;
            register_exchange(
                &mut exchanges,
                queue.dead_letter_exchange_name(),
                RabbitMqExchangeKind::Direct,
                queue.topology_ownership(),
            )?;
            if !queue.retry_buckets().is_empty() {
                register_exchange(
                    &mut exchanges,
                    queue.retry_exchange_name(),
                    RabbitMqExchangeKind::Direct,
                    queue.topology_ownership(),
                )?;
            }

            register_physical_queue(&mut physical_queues, queue.main_queue_name())?;
            for bucket in queue.retry_buckets() {
                register_physical_queue(&mut physical_queues, bucket.queue_name())?;
            }
            register_physical_queue(&mut physical_queues, queue.dead_letter_queue_name())?;
            queues.push(queue);
        }

        Ok(Self { queues })
    }

    /// Returns every accepted physical queue plan in configuration order.
    pub fn queues(&self) -> &[RabbitMqQueueTopologyPlan] {
        &self.queues
    }

    /// Returns the accepted plan for one configured main queue.
    pub fn queue(&self, name: &str) -> Option<&RabbitMqQueueTopologyPlan> {
        self.queues
            .iter()
            .find(|queue| queue.main_queue_name() == name)
    }

    /// Returns the number of configured main queues.
    pub fn len(&self) -> usize {
        self.queues.len()
    }

    /// Returns `true` when no RabbitMQ destinations are configured.
    pub fn is_empty(&self) -> bool {
        self.queues.is_empty()
    }
}

impl TryFrom<&RabbitMqTopologyConfig> for RabbitMqTopologyPlan {
    type Error = RabbitMqTopologyPlanError;

    fn try_from(config: &RabbitMqTopologyConfig) -> Result<Self, Self::Error> {
        Self::compile(config)
    }
}

/// Immutable topology projection for one configured main queue.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct RabbitMqQueueTopologyPlan {
    definition: QueueDefinition,
    exchange_name: String,
    exchange_kind: RabbitMqExchangeKind,
    main_queue_name: String,
    routing_key: String,
    retry_exchange_name: String,
    retry_buckets: Vec<RabbitMqRetryBucketPlan>,
    retry_bucket_delays_by_attempt: Vec<Vec<u64>>,
    dead_letter_exchange_name: String,
    dead_letter_queue_name: String,
    dead_letter_routing_key: String,
    system_queue_type: RabbitMqQueueType,
    topology_ownership: RabbitMqTopologyOwnership,
}

impl RabbitMqQueueTopologyPlan {
    fn compile(definition: &QueueDefinition) -> Result<Self, RabbitMqTopologyPlanError> {
        let retry_exchange_name = format!("{}.retry.{TOPOLOGY_VERSION}", definition.exchange_name);
        validate_derived_identity(definition, "retry_exchange_name", &retry_exchange_name)?;

        let retry_bucket_delays_by_attempt = retry_bucket_delays_by_attempt(definition);
        let retry_delays = retry_bucket_delays(&retry_bucket_delays_by_attempt);
        if retry_delays.len() > MAX_RETRY_BUCKETS_PER_QUEUE {
            return Err(RabbitMqTopologyPlanError::RetryBucketLimitExceeded {
                queue: definition.name.clone(),
                count: retry_delays.len(),
                maximum: MAX_RETRY_BUCKETS_PER_QUEUE,
            });
        }

        let mut retry_buckets = Vec::with_capacity(retry_delays.len());
        for delay_millis in retry_delays {
            let queue_name = format!(
                "{}.retry.{TOPOLOGY_VERSION}.{delay_millis}ms",
                definition.name
            );
            let routing_key = format!("{}.retry.{delay_millis}ms", definition.name);
            validate_derived_identity(definition, "retry_bucket.queue_name", &queue_name)?;
            validate_derived_identity(definition, "retry_bucket.routing_key", &routing_key)?;
            retry_buckets.push(RabbitMqRetryBucketPlan {
                queue_name,
                routing_key,
                delay_millis,
                queue_type: definition.queue_type,
            });
        }

        let dead_letter_exchange_name = definition
            .dead_letter_exchange
            .clone()
            .unwrap_or_else(|| format!("{}.dlx.{TOPOLOGY_VERSION}", definition.exchange_name));
        let dead_letter_queue_name = format!("{}.dlq.{TOPOLOGY_VERSION}", definition.name);
        let dead_letter_routing_key = definition
            .dead_letter_routing_key
            .clone()
            .unwrap_or_else(|| definition.name.clone());
        validate_derived_identity(
            definition,
            "dead_letter_exchange_name",
            &dead_letter_exchange_name,
        )?;
        validate_derived_identity(
            definition,
            "dead_letter_queue_name",
            &dead_letter_queue_name,
        )?;
        validate_derived_identity(
            definition,
            "dead_letter_routing_key",
            &dead_letter_routing_key,
        )?;

        Ok(Self {
            definition: definition.clone(),
            exchange_name: definition.exchange_name.clone(),
            exchange_kind: definition.exchange_kind,
            main_queue_name: definition.name.clone(),
            routing_key: definition.routing_key.clone(),
            retry_exchange_name,
            retry_buckets,
            retry_bucket_delays_by_attempt,
            dead_letter_exchange_name,
            dead_letter_queue_name,
            dead_letter_routing_key,
            system_queue_type: definition.queue_type,
            topology_ownership: definition.topology_ownership,
        })
    }

    /// Returns the exact accepted source definition.
    pub const fn definition(&self) -> &QueueDefinition {
        &self.definition
    }

    /// Returns the main exchange name.
    pub fn exchange_name(&self) -> &str {
        &self.exchange_name
    }

    /// Returns the main exchange kind.
    pub const fn exchange_kind(&self) -> RabbitMqExchangeKind {
        self.exchange_kind
    }

    /// Returns the main physical queue name.
    pub fn main_queue_name(&self) -> &str {
        &self.main_queue_name
    }

    /// Returns the exact main binding routing key.
    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }

    /// Returns the versioned retry exchange name.
    pub fn retry_exchange_name(&self) -> &str {
        &self.retry_exchange_name
    }

    /// Returns the deduplicated retry-delay bucket plans.
    pub fn retry_buckets(&self) -> &[RabbitMqRetryBucketPlan] {
        &self.retry_buckets
    }

    /// Returns the ordered, deduplicated retry bucket delays eligible for one
    /// retry attempt.
    ///
    /// With jitter disabled this slice contains exactly the nominal delay.
    /// With jitter enabled it contains at most the bounded lower, nominal and
    /// upper delays. Attempt zero or an attempt beyond the configured retry
    /// policy returns `None`.
    pub fn retry_bucket_delays(&self, retry_attempt: u32) -> Option<&[u64]> {
        let index = usize::try_from(retry_attempt.checked_sub(1)?).ok()?;
        self.retry_bucket_delays_by_attempt
            .get(index)
            .map(Vec::as_slice)
    }

    /// Returns the effective dead-letter exchange name.
    pub fn dead_letter_exchange_name(&self) -> &str {
        &self.dead_letter_exchange_name
    }

    /// Returns the versioned dead-letter queue name.
    pub fn dead_letter_queue_name(&self) -> &str {
        &self.dead_letter_queue_name
    }

    /// Returns the effective dead-letter binding routing key.
    pub fn dead_letter_routing_key(&self) -> &str {
        &self.dead_letter_routing_key
    }

    /// Returns the queue type inherited by retry and dead-letter queues.
    pub const fn system_queue_type(&self) -> RabbitMqQueueType {
        self.system_queue_type
    }

    /// Returns the authority responsible for this destination.
    pub const fn topology_ownership(&self) -> RabbitMqTopologyOwnership {
        self.topology_ownership
    }
}

/// Immutable topology projection for one physical retry-delay queue.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RabbitMqRetryBucketPlan {
    queue_name: String,
    routing_key: String,
    delay_millis: u64,
    queue_type: RabbitMqQueueType,
}

impl RabbitMqRetryBucketPlan {
    /// Returns the versioned retry bucket queue name.
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Returns the retry exchange binding routing key.
    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }

    /// Returns the exact retry delay in milliseconds.
    pub const fn delay_millis(&self) -> u64 {
        self.delay_millis
    }

    /// Returns the queue type inherited from the main queue.
    pub const fn queue_type(&self) -> RabbitMqQueueType {
        self.queue_type
    }
}

/// Typed failure produced before any RabbitMQ operation is attempted.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RabbitMqTopologyPlanError {
    /// Two configured logical queues use the same physical main queue name.
    DuplicateQueueName {
        /// Colliding logical queue name.
        name: String,
    },
    /// A configured RabbitMQ identity is empty, oversized, padded, contains
    /// controls or is a protected config reference.
    InvalidIdentity {
        /// Queue definition containing the invalid identity.
        queue: String,
        /// Invalid configuration field.
        field: &'static str,
    },
    /// A derived v2 RabbitMQ identity exceeds the bounded identity contract.
    InvalidDerivedIdentity {
        /// Queue definition producing the invalid derived identity.
        queue: String,
        /// Derived topology resource.
        resource: &'static str,
    },
    /// Explicit bounded retention is absent.
    MissingRetention {
        /// Queue definition missing retention.
        queue: String,
    },
    /// One explicit retention value is outside RabbitMQ's accepted integer range.
    InvalidRetention {
        /// Queue definition containing the invalid retention value.
        queue: String,
        /// Invalid retention field.
        field: &'static str,
    },
    /// A queue runtime or typed topology policy is inconsistent.
    InvalidQueuePolicy {
        /// Queue definition containing the invalid policy.
        queue: String,
        /// Invalid typed policy field.
        field: &'static str,
    },
    /// One logical queue expands to more bounded retry-delay queues than Lily
    /// permits.
    RetryBucketLimitExceeded {
        /// Logical queue whose retry policy exceeded the bound.
        queue: String,
        /// Number of distinct retry buckets produced by the policy.
        count: usize,
        /// Maximum accepted distinct retry buckets per logical queue.
        maximum: usize,
    },
    /// The aggregate topology expands to more retry-delay queues than Lily
    /// permits.
    TopologyRetryBucketLimitExceeded {
        /// Number of distinct retry buckets produced by the topology.
        count: usize,
        /// Maximum accepted aggregate retry bucket count.
        maximum: usize,
    },
    /// External ownership omitted a destination that Lily cannot create.
    ExternalDestinationRequired {
        /// Externally owned queue definition.
        queue: String,
        /// Missing explicit destination field.
        field: &'static str,
    },
    /// Two logical plans resolve to the same physical queue name.
    PhysicalQueueNameCollision {
        /// Colliding physical queue name.
        name: String,
    },
    /// One physical exchange is assigned conflicting kind or ownership metadata.
    ExchangeContractConflict {
        /// Conflicting physical exchange name.
        name: String,
    },
}

impl fmt::Display for RabbitMqTopologyPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateQueueName { name } => {
                write!(formatter, "duplicate RabbitMQ queue definition {name:?}")
            }
            Self::InvalidIdentity { queue, field } => write!(
                formatter,
                "RabbitMQ queue {queue:?} field {field} must contain 1..={MAX_IDENTITY_BYTES} trimmed, control-free bytes and cannot be a protected config reference"
            ),
            Self::InvalidDerivedIdentity { queue, resource } => write!(
                formatter,
                "RabbitMQ queue {queue:?} derived {resource} exceeds the bounded identity contract"
            ),
            Self::MissingRetention { queue } => write!(
                formatter,
                "RabbitMQ queue {queue:?} must define explicit retention bounds"
            ),
            Self::InvalidRetention { queue, field } => write!(
                formatter,
                "RabbitMQ queue {queue:?} retention.{field} must be between 1 and {}",
                i64::MAX
            ),
            Self::InvalidQueuePolicy { queue, field } => write!(
                formatter,
                "RabbitMQ queue {queue:?} has invalid {field} policy"
            ),
            Self::RetryBucketLimitExceeded {
                queue,
                count,
                maximum,
            } => write!(
                formatter,
                "RabbitMQ queue {queue:?} produces {count} retry buckets; the maximum is {maximum}"
            ),
            Self::TopologyRetryBucketLimitExceeded { count, maximum } => write!(
                formatter,
                "RabbitMQ topology produces {count} retry buckets; the maximum is {maximum}"
            ),
            Self::ExternalDestinationRequired { queue, field } => write!(
                formatter,
                "externally owned RabbitMQ queue {queue:?} must explicitly define {field}"
            ),
            Self::PhysicalQueueNameCollision { name } => write!(
                formatter,
                "RabbitMQ physical queue name {name:?} is produced by more than one topology destination"
            ),
            Self::ExchangeContractConflict { name } => write!(
                formatter,
                "RabbitMQ exchange {name:?} has conflicting kind or ownership metadata"
            ),
        }
    }
}

impl std::error::Error for RabbitMqTopologyPlanError {}

fn validate_definition(definition: &QueueDefinition) -> Result<(), RabbitMqTopologyPlanError> {
    for (field, value) in [
        ("name", Some(definition.name.as_str())),
        ("exchange_name", Some(definition.exchange_name.as_str())),
        ("routing_key", Some(definition.routing_key.as_str())),
        (
            "dead_letter_exchange",
            definition.dead_letter_exchange.as_deref(),
        ),
        (
            "dead_letter_routing_key",
            definition.dead_letter_routing_key.as_deref(),
        ),
    ] {
        if value.is_some_and(|value| !identity_is_valid(value)) {
            return Err(RabbitMqTopologyPlanError::InvalidIdentity {
                queue: definition.name.clone(),
                field,
            });
        }
    }

    // Lily's publisher bootstrap, consumer bootstrap and delivery channels
    // use separate pooled connections. RabbitMQ exclusive queues are bound to
    // their declaring connection, so no Lily-owned lifecycle can safely hand
    // one from topology bootstrap to runtime consumption or publication.
    if definition.exclusive {
        return invalid_policy(definition, "exclusive");
    }

    if definition.topology_ownership == RabbitMqTopologyOwnership::External {
        for (field, present) in [
            (
                "dead_letter_exchange",
                definition.dead_letter_exchange.is_some(),
            ),
            (
                "dead_letter_routing_key",
                definition.dead_letter_routing_key.is_some(),
            ),
        ] {
            if !present {
                return Err(RabbitMqTopologyPlanError::ExternalDestinationRequired {
                    queue: definition.name.clone(),
                    field,
                });
            }
        }
    }

    if !(1..=MAX_QUEUE_CONCURRENCY).contains(&definition.concurrency) {
        return invalid_policy(definition, "concurrency");
    }
    if !(1..=MAX_QUEUE_PREFETCH).contains(&definition.prefetch_count) {
        return invalid_policy(definition, "prefetch_count");
    }
    let delivery_buffer_capacity = definition
        .delivery_buffer_capacity
        .unwrap_or_else(|| usize::from(definition.prefetch_count));
    if delivery_buffer_capacity == 0
        || delivery_buffer_capacity > usize::from(definition.prefetch_count)
    {
        return invalid_policy(definition, "delivery_buffer_capacity");
    }
    if !(1..=MAX_QUEUE_MESSAGE_SIZE_BYTES).contains(&definition.max_message_size_bytes) {
        return invalid_policy(definition, "max_message_size_bytes");
    }
    if definition.retry_attempts > MAX_QUEUE_RETRY_ATTEMPTS {
        return invalid_policy(definition, "retry_attempts");
    }
    if !definition.retry_jitter_ratio.is_finite()
        || !(0.0..=0.5).contains(&definition.retry_jitter_ratio)
    {
        return invalid_policy(definition, "retry_jitter_ratio");
    }

    let retry_backoff_millis = definition.retry_backoff_millis.unwrap_or(1_000);
    let max_retry_backoff_millis = definition.max_retry_backoff_millis.unwrap_or(60_000);
    if !(100..=300_000).contains(&retry_backoff_millis)
        || !(retry_backoff_millis..=900_000).contains(&max_retry_backoff_millis)
    {
        return invalid_policy(definition, "retry_backoff");
    }
    if !(100..=900_000).contains(
        &definition
            .delivery_execution_timeout_millis
            .unwrap_or(30_000),
    ) {
        return invalid_policy(definition, "delivery_execution_timeout_millis");
    }
    if !(1..=60_000).contains(&definition.settlement_timeout_millis.unwrap_or(5_000)) {
        return invalid_policy(definition, "settlement_timeout_millis");
    }
    if definition.message_ttl_ms == Some(0) {
        return invalid_policy(definition, "message_ttl_ms");
    }

    let retention = definition.retention.as_ref().ok_or_else(|| {
        RabbitMqTopologyPlanError::MissingRetention {
            queue: definition.name.clone(),
        }
    })?;
    for (field, value) in [
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
    ] {
        if value == 0 || value > i64::MAX as u64 {
            return Err(RabbitMqTopologyPlanError::InvalidRetention {
                queue: definition.name.clone(),
                field,
            });
        }
    }

    match definition.queue_type {
        RabbitMqQueueType::Classic => {
            if definition
                .max_priority
                .is_some_and(|priority| !(1..=16).contains(&priority))
            {
                return invalid_policy(definition, "max_priority");
            }
        }
        RabbitMqQueueType::Quorum => {
            if !definition.durable {
                return invalid_policy(definition, "durable");
            }
            if definition.auto_delete {
                return invalid_policy(definition, "auto_delete");
            }
            if definition.max_priority.is_some() {
                return invalid_policy(definition, "max_priority");
            }
        }
    }

    Ok(())
}

fn invalid_policy<T>(
    definition: &QueueDefinition,
    field: &'static str,
) -> Result<T, RabbitMqTopologyPlanError> {
    Err(RabbitMqTopologyPlanError::InvalidQueuePolicy {
        queue: definition.name.clone(),
        field,
    })
}

fn retry_bucket_delays_by_attempt(definition: &QueueDefinition) -> Vec<Vec<u64>> {
    let base_millis = definition.retry_backoff_millis.unwrap_or(1_000);
    let cap_millis = definition.max_retry_backoff_millis.unwrap_or(60_000);
    let mut delays = Vec::with_capacity(definition.retry_attempts as usize);
    for retry_count in 1..=definition.retry_attempts {
        let exponent = retry_count.saturating_sub(1).min(31);
        let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let nominal = base_millis.saturating_mul(multiplier).min(cap_millis);
        let spread = ((nominal as f64) * definition.retry_jitter_ratio).round() as u64;
        let mut candidates = Vec::with_capacity(3);
        for candidate in [
            nominal.saturating_sub(spread),
            nominal,
            nominal.saturating_add(spread),
        ] {
            let candidate = candidate.clamp(base_millis, cap_millis);
            if !candidates.contains(&candidate) {
                candidates.push(candidate);
            }
        }
        delays.push(candidates);
    }
    delays
}

fn retry_bucket_delays(routes: &[Vec<u64>]) -> Vec<u64> {
    routes
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn validate_derived_identity(
    definition: &QueueDefinition,
    resource: &'static str,
    value: &str,
) -> Result<(), RabbitMqTopologyPlanError> {
    if identity_is_valid(value) {
        Ok(())
    } else {
        Err(RabbitMqTopologyPlanError::InvalidDerivedIdentity {
            queue: definition.name.clone(),
            resource,
        })
    }
}

fn identity_is_valid(value: &str) -> bool {
    (1..=MAX_IDENTITY_BYTES).contains(&value.len())
        && value.trim() == value
        && !value.chars().any(char::is_control)
        && parse_config_reference(value).is_none()
}

fn register_physical_queue(
    queues: &mut HashSet<String>,
    name: &str,
) -> Result<(), RabbitMqTopologyPlanError> {
    if queues.insert(name.to_string()) {
        Ok(())
    } else {
        Err(RabbitMqTopologyPlanError::PhysicalQueueNameCollision {
            name: name.to_string(),
        })
    }
}

fn register_exchange(
    exchanges: &mut HashMap<String, (RabbitMqExchangeKind, RabbitMqTopologyOwnership)>,
    name: &str,
    kind: RabbitMqExchangeKind,
    ownership: RabbitMqTopologyOwnership,
) -> Result<(), RabbitMqTopologyPlanError> {
    if let Some((accepted_kind, accepted_ownership)) =
        exchanges.insert(name.to_string(), (kind, ownership))
        && (accepted_kind != kind || accepted_ownership != ownership)
    {
        return Err(RabbitMqTopologyPlanError::ExchangeContractConflict {
            name: name.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueueRetentionConfig;

    fn definition(name: &str) -> QueueDefinition {
        QueueDefinition {
            name: name.into(),
            exchange_name: "orders".into(),
            routing_key: name.into(),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100,
                main_max_bytes: 1024 * 1024,
                retry_bucket_max_messages: 10,
                retry_bucket_max_bytes: 256 * 1024,
                dead_letter_max_messages: 10,
                dead_letter_max_bytes: 256 * 1024,
            }),
            ..QueueDefinition::default()
        }
    }

    #[test]
    fn compiles_exact_v2_names_and_inherited_system_queue_types() {
        let mut definition = definition("orders.created");
        definition.queue_type = RabbitMqQueueType::Quorum;
        definition.retry_attempts = 3;
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition.clone()],
        })
        .unwrap();

        let queue = plan.queue("orders.created").unwrap();
        assert_eq!(queue.definition(), &definition);
        assert_eq!(queue.exchange_name(), "orders");
        assert_eq!(queue.routing_key(), "orders.created");
        assert_eq!(queue.retry_exchange_name(), "orders.retry.v2");
        assert_eq!(queue.dead_letter_exchange_name(), "orders.dlx.v2");
        assert_eq!(queue.dead_letter_queue_name(), "orders.created.dlq.v2");
        assert_eq!(queue.dead_letter_routing_key(), "orders.created");
        assert_eq!(queue.system_queue_type(), RabbitMqQueueType::Quorum);
        assert_eq!(
            queue
                .retry_buckets()
                .iter()
                .map(RabbitMqRetryBucketPlan::delay_millis)
                .collect::<Vec<_>>(),
            vec![1_000, 2_000, 4_000]
        );
        assert!(
            queue
                .retry_buckets()
                .iter()
                .all(|bucket| bucket.queue_type() == RabbitMqQueueType::Quorum)
        );
    }

    #[test]
    fn retry_delays_saturate_cap_and_are_deduplicated() {
        let mut definition = definition("orders.created");
        definition.retry_attempts = 100;
        definition.max_retry_backoff_millis = Some(5_000);
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .unwrap();

        assert_eq!(
            plan.queues()[0]
                .retry_buckets()
                .iter()
                .map(RabbitMqRetryBucketPlan::delay_millis)
                .collect::<Vec<_>>(),
            vec![1_000, 2_000, 4_000, 5_000]
        );
    }

    #[test]
    fn jitter_predeclares_only_bounded_queue_level_ttl_candidates() {
        let mut definition = definition("orders.created");
        definition.retry_attempts = 4;
        definition.retry_backoff_millis = Some(1_000);
        definition.max_retry_backoff_millis = Some(5_000);
        definition.retry_jitter_ratio = 0.2;
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .unwrap();
        let queue = plan.queue("orders.created").unwrap();

        assert_eq!(queue.retry_bucket_delays(0), None);
        assert_eq!(
            queue.retry_bucket_delays(1),
            Some([1_000, 1_200].as_slice())
        );
        assert_eq!(
            queue.retry_bucket_delays(2),
            Some([1_600, 2_000, 2_400].as_slice())
        );
        assert_eq!(
            queue.retry_bucket_delays(3),
            Some([3_200, 4_000, 4_800].as_slice())
        );
        assert_eq!(
            queue.retry_bucket_delays(4),
            Some([4_000, 5_000].as_slice())
        );
        assert_eq!(queue.retry_bucket_delays(5), None);
        assert_eq!(
            queue
                .retry_buckets()
                .iter()
                .map(RabbitMqRetryBucketPlan::delay_millis)
                .collect::<Vec<_>>(),
            vec![
                1_000, 1_200, 1_600, 2_000, 2_400, 3_200, 4_000, 4_800, 5_000
            ]
        );
    }

    #[test]
    fn jitter_ratio_is_finite_bounded_and_defaults_to_disabled() {
        let default = definition("orders.default");
        assert_eq!(default.retry_jitter_ratio, 0.0);

        for ratio in [-0.1, 0.500_001, f64::NAN, f64::INFINITY] {
            let mut invalid = definition("orders.invalid");
            invalid.retry_jitter_ratio = ratio;
            assert!(matches!(
                RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                    queues: vec![invalid]
                }),
                Err(RabbitMqTopologyPlanError::InvalidQueuePolicy {
                    field: "retry_jitter_ratio",
                    ..
                })
            ));
        }
    }

    #[test]
    fn per_queue_retry_bucket_limit_accepts_exact_and_rejects_plus_one() {
        let mut exact = definition("orders.exact");
        exact.retry_attempts = MAX_QUEUE_RETRY_ATTEMPTS;
        exact.retry_backoff_millis = Some(100);
        exact.max_retry_backoff_millis = Some(900_000);
        exact.retry_jitter_ratio = 0.000_1;
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![exact],
        })
        .unwrap();
        assert_eq!(
            plan.queues()[0].retry_buckets().len(),
            MAX_RETRY_BUCKETS_PER_QUEUE
        );

        let mut overflow = definition("orders.overflow");
        overflow.retry_attempts = MAX_QUEUE_RETRY_ATTEMPTS;
        overflow.retry_backoff_millis = Some(100);
        overflow.max_retry_backoff_millis = Some(900_000);
        overflow.retry_jitter_ratio = 0.000_2;
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![overflow]
            }),
            Err(RabbitMqTopologyPlanError::RetryBucketLimitExceeded {
                count: 34,
                maximum: MAX_RETRY_BUCKETS_PER_QUEUE,
                ..
            })
        ));
    }

    #[test]
    fn aggregate_retry_bucket_limit_accepts_exact_and_rejects_plus_one() {
        let mut queues = Vec::new();
        for index in 0..273 {
            let mut queue = definition(&format!("queue-{index:03}"));
            queue.retry_attempts = MAX_QUEUE_RETRY_ATTEMPTS;
            queue.retry_backoff_millis = Some(100);
            queue.max_retry_backoff_millis = Some(900_000);
            queues.push(queue);
        }
        let mut tail = definition("queue-tail");
        tail.retry_attempts = 1;
        tail.retry_backoff_millis = Some(100);
        tail.max_retry_backoff_millis = Some(900_000);
        queues.push(tail);

        let exact = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: queues.clone(),
        })
        .unwrap();
        assert_eq!(
            exact
                .queues()
                .iter()
                .map(|queue| queue.retry_buckets().len())
                .sum::<usize>(),
            MAX_RETRY_BUCKETS_PER_TOPOLOGY
        );

        let mut overflow = definition("queue-overflow");
        overflow.retry_attempts = 1;
        overflow.retry_backoff_millis = Some(100);
        overflow.max_retry_backoff_millis = Some(900_000);
        queues.push(overflow);
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig { queues }),
            Err(
                RabbitMqTopologyPlanError::TopologyRetryBucketLimitExceeded {
                    count: 4_097,
                    maximum: MAX_RETRY_BUCKETS_PER_TOPOLOGY,
                }
            )
        ));
    }

    #[test]
    fn external_destinations_are_explicit_and_never_defaulted() {
        let mut external = definition("orders.created");
        external.topology_ownership = RabbitMqTopologyOwnership::External;
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![external.clone()]
            }),
            Err(RabbitMqTopologyPlanError::ExternalDestinationRequired {
                field: "dead_letter_exchange",
                ..
            })
        ));

        external.dead_letter_exchange = Some("external.dlx".into());
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![external.clone()]
            }),
            Err(RabbitMqTopologyPlanError::ExternalDestinationRequired {
                field: "dead_letter_routing_key",
                ..
            })
        ));

        external.dead_letter_routing_key = Some("orders.failed".into());
        external.exclusive = true;
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![external.clone()]
            }),
            Err(RabbitMqTopologyPlanError::InvalidQueuePolicy {
                field: "exclusive",
                ..
            })
        ));

        external.exclusive = false;
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![external],
        })
        .unwrap();
        assert_eq!(plan.queues()[0].dead_letter_exchange_name(), "external.dlx");
        assert_eq!(plan.queues()[0].dead_letter_routing_key(), "orders.failed");
    }

    #[test]
    fn protected_config_references_never_enter_the_serializable_plan() {
        let fields = [
            "name",
            "exchange_name",
            "routing_key",
            "dead_letter_exchange",
            "dead_letter_routing_key",
        ];
        for reference in [
            "${secret:rabbitmq.topology.identity}",
            "${file:/run/secrets/rabbitmq-topology-identity}",
        ] {
            for (field_index, expected_field) in fields.iter().enumerate() {
                let mut protected = definition("orders.created");
                match field_index {
                    0 => protected.name = reference.into(),
                    1 => protected.exchange_name = reference.into(),
                    2 => protected.routing_key = reference.into(),
                    3 => protected.dead_letter_exchange = Some(reference.into()),
                    4 => protected.dead_letter_routing_key = Some(reference.into()),
                    _ => unreachable!(),
                }

                assert!(matches!(
                    RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                        queues: vec![protected]
                    }),
                    Err(RabbitMqTopologyPlanError::InvalidIdentity { field, .. })
                        if field == *expected_field
                ));
            }
        }
    }

    #[test]
    fn derived_identity_overflow_and_global_physical_collision_are_rejected() {
        let oversized = definition(&"q".repeat(MAX_IDENTITY_BYTES));
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![oversized]
            }),
            Err(RabbitMqTopologyPlanError::InvalidDerivedIdentity {
                resource: "dead_letter_queue_name",
                ..
            })
        ));

        let first = definition("orders");
        let mut collision = definition("orders.dlq.v2");
        collision.routing_key = "collision".into();
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![first, collision]
            }),
            Err(RabbitMqTopologyPlanError::PhysicalQueueNameCollision { ref name })
                if name == "orders.dlq.v2"
        ));
    }

    #[test]
    fn queue_policy_and_exchange_contract_conflicts_are_rejected() {
        let mut exclusive_classic = definition("exclusive.events");
        exclusive_classic.exclusive = true;
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![exclusive_classic]
            }),
            Err(RabbitMqTopologyPlanError::InvalidQueuePolicy {
                field: "exclusive",
                ..
            })
        ));

        let mut quorum = definition("orders.created");
        quorum.queue_type = RabbitMqQueueType::Quorum;
        quorum.max_priority = Some(1);
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![quorum]
            }),
            Err(RabbitMqTopologyPlanError::InvalidQueuePolicy {
                field: "max_priority",
                ..
            })
        ));

        let managed = definition("orders.created");
        let mut external = definition("orders.updated");
        external.topology_ownership = RabbitMqTopologyOwnership::External;
        external.dead_letter_exchange = Some("orders.external.dlx".into());
        external.dead_letter_routing_key = Some("orders.updated.failed".into());
        assert!(matches!(
            RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
                queues: vec![managed, external]
            }),
            Err(RabbitMqTopologyPlanError::ExchangeContractConflict { ref name })
                if name == "orders"
        ));
    }
}
