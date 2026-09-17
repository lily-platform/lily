//! Bounded, supervised transactional outbox relay.

use std::{
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::{
    FutureExt,
    future::{BoxFuture, join_all},
};
use lapin::{
    BasicProperties,
    options::BasicPublishOptions,
    types::{AMQPValue, FieldTable, ShortString},
};
use lily_config::TransactionalInboxConfig;
use lily_error::application::{MessageBrokerError, message_broker::RabbitMQError};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    channel_manager_trait::ChannelManager,
    providers::rabbitmq::{
        channel_manager::RabbitMQChannelManager, metadata::validate_amqp_metadata,
    },
    transactional::TransactionalInboxSnapshot,
};

use crate::{
    owned_tasks::{TaskReceipt, TransactionTasks},
    shutdown_budget::{QueueShutdownBudget, QueueShutdownDeadlines},
};

const RELAY_STARTING: u8 = 0;
const RELAY_READY: u8 = 1;
const RELAY_DEGRADED: u8 = 2;
const RELAY_DRAINING: u8 = 3;
const RELAY_STOPPED: u8 = 4;
const RELAY_FAILED: u8 = 5;
const DEGRADATION_STORAGE: u8 = 1 << 0;
const DEGRADATION_PUBLISHER: u8 = 1 << 1;
const OUTBOX_RECORD_TOO_LARGE: &str = "QUEUE_OUTBOX_RELAY_RECORD_TOO_LARGE";

#[cfg(feature = "test-support")]
mod post_confirm_test_support {
    use super::*;
    use std::sync::{Mutex as StdMutex, OnceLock, Weak};

    static ACTIVE_PROBE: OnceLock<StdMutex<Option<Weak<OutboxPostConfirmProbe>>>> = OnceLock::new();

    /// Exact post-confirm/pre-delivered-mark pause used by crash qualification.
    ///
    /// The probe is compiled only by the non-default `test-support` feature and
    /// therefore cannot add a production pause or public application contract.
    #[doc(hidden)]
    pub struct OutboxPostConfirmProbe {
        event_id: Uuid,
        pause_claimed: AtomicBool,
        entered: AtomicBool,
        entered_notify: Notify,
        release: CancellationToken,
    }

    impl OutboxPostConfirmProbe {
        /// Wait until RabbitMQ has confirmed the selected event while its
        /// durable outbox record is still marked undelivered.
        pub async fn wait_until_paused(&self) {
            loop {
                let entered = self.entered_notify.notified();
                tokio::pin!(entered);
                entered.as_mut().enable();
                if self.entered.load(Ordering::Acquire) {
                    return;
                }
                entered.as_mut().await;
            }
        }

        /// Release a paused qualification without killing the process.
        pub fn release(&self) {
            self.release.cancel();
        }
    }

    /// Installs one process-local weak probe for an exact stable outbox event.
    #[must_use]
    pub fn install_outbox_post_confirm_probe(event_id: Uuid) -> Arc<OutboxPostConfirmProbe> {
        let probe = Arc::new(OutboxPostConfirmProbe {
            event_id,
            pause_claimed: AtomicBool::new(false),
            entered: AtomicBool::new(false),
            entered_notify: Notify::new(),
            release: CancellationToken::new(),
        });
        let slot = ACTIVE_PROBE.get_or_init(|| StdMutex::new(None));
        let mut slot = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            slot.as_ref().and_then(Weak::upgrade).is_none(),
            "only one live outbox post-confirm probe may be installed"
        );
        *slot = Some(Arc::downgrade(&probe));
        probe
    }

    pub(super) async fn pause(event_id: Uuid, force: &CancellationToken) {
        let probe = ACTIVE_PROBE
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade);
        let Some(probe) = probe else {
            return;
        };
        let should_pause = event_id == probe.event_id
            && probe
                .pause_claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        if !should_pause {
            return;
        }
        probe.entered.store(true, Ordering::Release);
        probe.entered_notify.notify_waiters();
        tokio::select! {
            biased;
            () = force.cancelled() => {}
            () = probe.release.cancelled() => {}
        }
    }
}

#[cfg(feature = "test-support")]
pub use post_confirm_test_support::{OutboxPostConfirmProbe, install_outbox_post_confirm_probe};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboxRelayState {
    Starting,
    Ready,
    Degraded,
    Draining,
    Stopped,
    Failed,
}

impl OutboxRelayState {
    fn from_raw(value: u8) -> Self {
        match value {
            RELAY_READY => Self::Ready,
            RELAY_DEGRADED => Self::Degraded,
            RELAY_DRAINING => Self::Draining,
            RELAY_STOPPED => Self::Stopped,
            RELAY_FAILED => Self::Failed,
            _ => Self::Starting,
        }
    }
}

/// Sampling-independent state of the framework-owned transactional outbox relay.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TransactionalOutboxRelaySnapshot {
    /// Number of exact transactional storage authorities registered with the runtime.
    pub registered_relays: u64,
    /// Number of relay workers currently ready to claim durable work.
    pub ready_relays: u64,
    /// Number of outbox records currently owned by a worker.
    pub in_flight: u64,
    /// Records confirmed by RabbitMQ and then marked delivered in durable storage.
    pub delivered: u64,
    /// RabbitMQ publish attempts which failed and were retried or released.
    pub publish_failures: u64,
    /// Durable rows blocked after exhausting the configured attempt budget.
    pub exhausted: u64,
    /// Confirmed publishes whose delivered mark could not be proven.
    ///
    /// Such records are intentionally left for lease expiry and may be
    /// published again with the same event ID.
    pub uncertain_after_publish: u64,
    /// Most recent stable, secret-safe relay failure code.
    pub last_failure_code: Option<&'static str>,
    /// Aggregate relay lifecycle state.
    pub state: TransactionalOutboxRelayState,
}

impl Default for TransactionalOutboxRelaySnapshot {
    fn default() -> Self {
        Self {
            registered_relays: 0,
            ready_relays: 0,
            in_flight: 0,
            delivered: 0,
            publish_failures: 0,
            exhausted: 0,
            uncertain_after_publish: 0,
            last_failure_code: None,
            state: TransactionalOutboxRelayState::Starting,
        }
    }
}

/// Aggregate lifecycle state of the transactional outbox relay.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionalOutboxRelayState {
    /// The provider has not started relay workers yet.
    Starting,
    /// Every registered relay is healthy and accepting durable work.
    Ready,
    /// At least one operation failed; bounded retry remains active.
    Degraded,
    /// New claims have stopped while owned work drains.
    Draining,
    /// Every relay task terminated and was joined.
    Stopped,
    /// A relay task panicked or otherwise lost lifecycle ownership.
    Failed,
}

impl From<OutboxRelayState> for TransactionalOutboxRelayState {
    fn from(value: OutboxRelayState) -> Self {
        match value {
            OutboxRelayState::Starting => Self::Starting,
            OutboxRelayState::Ready => Self::Ready,
            OutboxRelayState::Degraded => Self::Degraded,
            OutboxRelayState::Draining => Self::Draining,
            OutboxRelayState::Stopped => Self::Stopped,
            OutboxRelayState::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelayRecord {
    pub(crate) record_id: Uuid,
    pub(crate) claim_token: Uuid,
    pub(crate) event_id: Uuid,
    pub(crate) exchange: Arc<str>,
    pub(crate) routing_key: Arc<str>,
    pub(crate) schema_version: u16,
    pub(crate) content_kind: Arc<str>,
    pub(crate) content_type: Arc<str>,
    pub(crate) traceparent: Option<Arc<str>>,
    pub(crate) publish_attempts: u32,
    pub(crate) body: Arc<[u8]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RelayFailure {
    code: &'static str,
}

impl RelayFailure {
    pub(crate) const fn new(code: &'static str) -> Self {
        Self { code }
    }

    pub(crate) const fn code(self) -> &'static str {
        self.code
    }
}

#[async_trait]
pub(crate) trait OutboxRelayStore: Send + Sync {
    async fn ensure_schema_ready(&self) -> Result<(), RelayFailure>;

    async fn claim_batch(&self, owner: Uuid) -> Result<Vec<RelayRecord>, RelayFailure>;

    async fn begin_publish(&self, record: &RelayRecord) -> Result<Option<u32>, RelayFailure>;

    async fn mark_delivered(&self, record: &RelayRecord) -> Result<(), RelayFailure>;

    async fn record_failure(
        &self,
        record: &RelayRecord,
        retry_after: Duration,
        code: &'static str,
    ) -> Result<(), RelayFailure>;

    async fn cleanup(&self) -> Result<(), RelayFailure>;

    async fn exhausted_count(&self) -> Result<u64, RelayFailure>;
}

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
struct PostgresRelayStore {
    runtime: Arc<crate::transactional_postgresql::PostgresTransactionalRuntime>,
}

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
#[async_trait]
impl OutboxRelayStore for PostgresRelayStore {
    async fn ensure_schema_ready(&self) -> Result<(), RelayFailure> {
        self.runtime
            .ensure_schema_ready()
            .await
            .map_err(postgres_relay_failure)
    }

    async fn claim_batch(&self, owner: Uuid) -> Result<Vec<RelayRecord>, RelayFailure> {
        self.runtime
            .claim_outbox_batch(owner)
            .await
            .map_err(postgres_relay_failure)?
            .into_iter()
            .map(|message| {
                Ok(RelayRecord {
                    record_id: message.record_id(),
                    claim_token: message.claim_token(),
                    event_id: message.event_id(),
                    exchange: Arc::from(message.exchange()),
                    routing_key: Arc::from(message.routing_key()),
                    schema_version: message.schema_version(),
                    content_kind: Arc::from(message.content_kind()),
                    content_type: Arc::from(message.content_type()),
                    traceparent: message.traceparent().map(Arc::from),
                    publish_attempts: u32::try_from(message.publish_attempts()).map_err(|_| {
                        RelayFailure::new("QUEUE_OUTBOX_RELAY_ATTEMPT_CONTRACT_VIOLATION")
                    })?,
                    body: Arc::from(message.body()),
                })
            })
            .collect()
    }

    async fn begin_publish(&self, record: &RelayRecord) -> Result<Option<u32>, RelayFailure> {
        let attempt = match self
            .runtime
            .begin_outbox_publish(record.record_id, record.claim_token)
            .await
        {
            Ok(attempt) => attempt,
            Err(crate::transactional_postgresql::PostgresReliabilityError::OutboxClaimLost) => {
                return Ok(None);
            }
            Err(error) => return Err(postgres_relay_failure(error)),
        };
        u32::try_from(attempt)
            .map(Some)
            .map_err(|_| RelayFailure::new("QUEUE_OUTBOX_RELAY_ATTEMPT_CONTRACT_VIOLATION"))
    }

    async fn mark_delivered(&self, record: &RelayRecord) -> Result<(), RelayFailure> {
        self.runtime
            .mark_outbox_delivered(record.record_id, record.claim_token)
            .await
            .map_err(postgres_relay_failure)
    }

    async fn record_failure(
        &self,
        record: &RelayRecord,
        retry_after: Duration,
        code: &'static str,
    ) -> Result<(), RelayFailure> {
        self.runtime
            .record_outbox_failure(record.record_id, record.claim_token, retry_after, code)
            .await
            .map_err(postgres_relay_failure)
    }

    async fn cleanup(&self) -> Result<(), RelayFailure> {
        self.runtime
            .cleanup()
            .await
            .map(|_| ())
            .map_err(postgres_relay_failure)
    }

    async fn exhausted_count(&self) -> Result<u64, RelayFailure> {
        self.runtime
            .exhausted_outbox_count()
            .await
            .map_err(postgres_relay_failure)
    }
}

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
fn postgres_relay_failure(
    error: crate::transactional_postgresql::PostgresReliabilityError,
) -> RelayFailure {
    RelayFailure::new(error.code())
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
struct MongoRelayStore {
    runtime: Arc<crate::transactional_mongodb::MongoTransactionalRuntime>,
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
#[async_trait]
impl OutboxRelayStore for MongoRelayStore {
    async fn ensure_schema_ready(&self) -> Result<(), RelayFailure> {
        self.runtime
            .ensure_schema_ready()
            .await
            .map_err(mongo_relay_failure)
    }

    async fn claim_batch(&self, owner: Uuid) -> Result<Vec<RelayRecord>, RelayFailure> {
        self.runtime
            .claim_outbox_batch(owner)
            .await
            .map_err(mongo_relay_failure)?
            .into_iter()
            .map(|message| {
                Ok(RelayRecord {
                    record_id: message.record_id(),
                    claim_token: message.claim_token(),
                    event_id: message.event_id(),
                    exchange: Arc::from(message.exchange()),
                    routing_key: Arc::from(message.routing_key()),
                    schema_version: message.schema_version(),
                    content_kind: Arc::from(message.content_kind()),
                    content_type: Arc::from(message.content_type()),
                    traceparent: message.traceparent().map(Arc::from),
                    publish_attempts: u32::try_from(message.publish_attempts()).map_err(|_| {
                        RelayFailure::new("QUEUE_OUTBOX_RELAY_ATTEMPT_CONTRACT_VIOLATION")
                    })?,
                    body: Arc::from(message.body()),
                })
            })
            .collect()
    }

    async fn begin_publish(&self, record: &RelayRecord) -> Result<Option<u32>, RelayFailure> {
        let attempt = match self
            .runtime
            .begin_outbox_publish(record.record_id, record.claim_token)
            .await
        {
            Ok(attempt) => attempt,
            Err(crate::transactional_mongodb::MongoReliabilityError::OutboxClaimLost) => {
                return Ok(None);
            }
            Err(error) => return Err(mongo_relay_failure(error)),
        };
        u32::try_from(attempt)
            .map(Some)
            .map_err(|_| RelayFailure::new("QUEUE_OUTBOX_RELAY_ATTEMPT_CONTRACT_VIOLATION"))
    }

    async fn mark_delivered(&self, record: &RelayRecord) -> Result<(), RelayFailure> {
        self.runtime
            .mark_outbox_delivered(record.record_id, record.claim_token)
            .await
            .map_err(mongo_relay_failure)
    }

    async fn record_failure(
        &self,
        record: &RelayRecord,
        retry_after: Duration,
        code: &'static str,
    ) -> Result<(), RelayFailure> {
        self.runtime
            .record_outbox_failure(record.record_id, record.claim_token, retry_after, code)
            .await
            .map_err(mongo_relay_failure)
    }

    async fn cleanup(&self) -> Result<(), RelayFailure> {
        self.runtime
            .cleanup()
            .await
            .map(|_| ())
            .map_err(mongo_relay_failure)
    }

    async fn exhausted_count(&self) -> Result<u64, RelayFailure> {
        self.runtime
            .exhausted_outbox_count()
            .await
            .map_err(mongo_relay_failure)
    }
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
fn mongo_relay_failure(error: crate::transactional_mongodb::MongoReliabilityError) -> RelayFailure {
    RelayFailure::new(error.code())
}

/// Backend-neutral ownership contract retained by the relay supervisor.
///
/// Storage adapters keep their transaction implementation private while the
/// shared RabbitMQ lifecycle still stops admission, drains finalization and
/// proves that no transaction authority remains active before DI disposal.
#[async_trait]
pub(crate) trait TransactionalRuntimeLifecycle: Send + Sync {
    fn stop_admission(&self);

    fn active_transactions(&self) -> usize;
    fn set_shutdown_deadlines(&self, _deadlines: QueueShutdownDeadlines) {}
    fn request_force(&self) {}
    fn reconciled(&self) -> bool;

    fn transactional_inbox_snapshot(&self) -> TransactionalInboxSnapshot {
        TransactionalInboxSnapshot {
            active_transactions: u64::try_from(self.active_transactions()).unwrap_or(u64::MAX),
            ..TransactionalInboxSnapshot::default()
        }
    }

    async fn drain(&self, timeout: Duration) -> Result<(), RelayFailure>;

    async fn force_drain(&self, timeout: Duration) -> Result<(), RelayFailure>;
}

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
#[async_trait]
impl TransactionalRuntimeLifecycle
    for crate::transactional_postgresql::PostgresTransactionalRuntime
{
    fn stop_admission(&self) {
        self.stop_transaction_admission();
    }

    fn set_shutdown_deadlines(&self, deadlines: QueueShutdownDeadlines) {
        self.set_shutdown_deadlines(deadlines);
    }
    fn request_force(&self) {
        self.request_force();
    }
    fn reconciled(&self) -> bool {
        self.transactions_reconciled()
    }

    fn active_transactions(&self) -> usize {
        self.active_transactions()
    }

    async fn drain(&self, timeout: Duration) -> Result<(), RelayFailure> {
        self.drain_transactions(timeout)
            .await
            .map_err(postgres_relay_failure)
    }

    async fn force_drain(&self, timeout: Duration) -> Result<(), RelayFailure> {
        self.request_force();
        self.drain_transactions(timeout)
            .await
            .map_err(postgres_relay_failure)
    }
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
#[async_trait]
impl TransactionalRuntimeLifecycle for crate::transactional_mongodb::MongoTransactionalRuntime {
    fn stop_admission(&self) {
        self.stop_transaction_admission();
    }

    fn set_shutdown_deadlines(&self, deadlines: QueueShutdownDeadlines) {
        self.set_shutdown_deadlines(deadlines);
    }
    fn request_force(&self) {
        self.request_force();
    }
    fn reconciled(&self) -> bool {
        self.transactions_reconciled()
    }

    fn active_transactions(&self) -> usize {
        self.active_transactions()
    }

    fn transactional_inbox_snapshot(&self) -> TransactionalInboxSnapshot {
        self.transactional_inbox_snapshot()
    }

    async fn drain(&self, timeout: Duration) -> Result<(), RelayFailure> {
        self.drain_transactions(timeout)
            .await
            .map_err(mongo_relay_failure)
    }

    async fn force_drain(&self, timeout: Duration) -> Result<(), RelayFailure> {
        self.force_drain_transactions(timeout)
            .await
            .map_err(mongo_relay_failure)
    }
}

#[derive(Clone)]
struct TrackedTransactionalRuntime {
    key: Arc<str>,
    runtime: Arc<dyn TransactionalRuntimeLifecycle>,
    timeout: Duration,
}

#[async_trait]
pub(crate) trait OutboxRelayPublisher: Send + Sync {
    async fn publish(
        &self,
        record: &RelayRecord,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<(), MessageBrokerError>;
}

pub(crate) struct RabbitMqOutboxPublisher {
    channel_manager: Arc<RabbitMQChannelManager>,
}

impl RabbitMqOutboxPublisher {
    pub(crate) fn new(channel_manager: Arc<RabbitMQChannelManager>) -> Arc<Self> {
        Arc::new(Self { channel_manager })
    }
}

#[async_trait]
impl OutboxRelayPublisher for RabbitMqOutboxPublisher {
    async fn publish(
        &self,
        record: &RelayRecord,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<(), MessageBrokerError> {
        let properties = materialize_publish_properties(record)?;
        let channel = self
            .channel_manager
            .get_channel("lily.transactional_outbox", cancellation.clone(), true)
            .await?;
        let confirmation = channel
            .basic_publish(
                ShortString::from(record.exchange.as_ref()),
                ShortString::from(record.routing_key.as_ref()),
                BasicPublishOptions {
                    mandatory: true,
                    immediate: false,
                },
                record.body.as_ref(),
                properties,
            )
            .await
            .map_err(|error| {
                MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(error.to_string()))
            })?;
        lily_queue_client::await_publisher_confirm(confirmation, timeout, &cancellation)
            .await
            .map(|_| ())
    }
}

fn materialize_publish_properties(
    record: &RelayRecord,
) -> Result<BasicProperties, MessageBrokerError> {
    if record.event_id.is_nil() || record.schema_version == 0 {
        return Err(invalid_message("invalid persisted outbox identity"));
    }
    let content_kind = record.content_kind.as_ref();
    let canonical_content_type: Arc<str> = match content_kind {
        "json" => Arc::from("application/json"),
        "text" => Arc::from("text/plain; charset=utf-8"),
        "binary" => Arc::from("application/octet-stream"),
        custom => {
            let custom = lily_queue_client::CustomPublishContent::try_new(
                custom,
                record.content_type.as_ref(),
            )
            .map_err(|_| invalid_message("invalid persisted outbox content contract"))?;
            Arc::from(custom.content_type())
        }
    };
    if record.content_type.as_ref() != canonical_content_type.as_ref() {
        return Err(invalid_message("persisted outbox content type mismatch"));
    }

    let mut headers = FieldTable::default();
    headers.insert(
        "x-lily-event-id".into(),
        AMQPValue::LongString(record.event_id.to_string().into()),
    );
    headers.insert(
        "x-lily-schema-version".into(),
        AMQPValue::LongString(record.schema_version.to_string().into()),
    );
    headers.insert(
        "x-lily-content-kind".into(),
        AMQPValue::LongString(record.content_kind.as_ref().into()),
    );
    if let Some(traceparent) = record.traceparent.as_deref() {
        let canonical = lily_trace::W3CTraceContext::from_traceparent(traceparent)
            .map_err(|_| invalid_message("invalid persisted outbox traceparent"))?
            .to_traceparent();
        if canonical != traceparent {
            return Err(invalid_message(
                "non-canonical persisted outbox traceparent",
            ));
        }
        headers.insert(
            "traceparent".into(),
            AMQPValue::LongString(canonical.into()),
        );
    }

    let properties = BasicProperties::default()
        .with_content_type(record.content_type.as_ref().into())
        .with_message_id(record.event_id.to_string().into())
        .with_delivery_mode(2)
        .with_headers(headers);
    validate_amqp_metadata(&properties).map_err(invalid_message)?;
    Ok(properties)
}

fn invalid_message(reason: impl Into<String>) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(reason.into()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OutboxRelayPolicy {
    batch_size: usize,
    max_in_flight_bytes: usize,
    poll_interval: Duration,
    publish_timeout: Duration,
    max_publish_attempts: u32,
    initial_backoff: Duration,
    maximum_backoff: Duration,
    cleanup_interval: Duration,
    shutdown_drain_timeout: Duration,
}

impl From<&TransactionalInboxConfig> for OutboxRelayPolicy {
    fn from(config: &TransactionalInboxConfig) -> Self {
        Self {
            batch_size: config.relay_batch_size,
            max_in_flight_bytes: config.relay_max_in_flight_bytes,
            poll_interval: Duration::from_millis(config.relay_poll_interval_millis),
            publish_timeout: Duration::from_millis(config.relay_publish_timeout_millis),
            max_publish_attempts: config.relay_max_publish_attempts,
            initial_backoff: Duration::from_millis(config.relay_retry_initial_backoff_millis),
            maximum_backoff: Duration::from_millis(config.relay_retry_max_backoff_millis),
            cleanup_interval: Duration::from_secs(config.cleanup_interval_secs),
            shutdown_drain_timeout: Duration::from_millis(config.shutdown_drain_timeout_millis),
        }
    }
}

#[derive(Default)]
struct RelayLedger {
    registered: AtomicU64,
    ready: AtomicU64,
    in_flight: AtomicU64,
    delivered: AtomicU64,
    publish_failures: AtomicU64,
    exhausted: AtomicU64,
    uncertain_after_publish: AtomicU64,
    degraded_workers: AtomicU64,
    state: AtomicU8,
    last_failure_code: Mutex<Option<&'static str>>,
}

impl RelayLedger {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(RELAY_STARTING),
            ..Self::default()
        }
    }

    fn record_failure(&self, code: &'static str) {
        *self
            .last_failure_code
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(code);
        self.state.store(RELAY_DEGRADED, Ordering::Release);
    }

    fn record_worker_ready(&self) {
        if self.degraded_workers.load(Ordering::Acquire) != 0
            || self.exhausted.load(Ordering::Acquire) != 0
        {
            return;
        }
        let _ = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                matches!(state, RELAY_STARTING | RELAY_READY).then_some(RELAY_READY)
            });
    }

    fn record_recovered(&self) {
        if self.degraded_workers.load(Ordering::Acquire) != 0
            || self.exhausted.load(Ordering::Acquire) != 0
        {
            return;
        }
        let _ = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                matches!(state, RELAY_STARTING | RELAY_READY | RELAY_DEGRADED)
                    .then_some(RELAY_READY)
            });
    }

    fn snapshot(&self) -> TransactionalOutboxRelaySnapshot {
        TransactionalOutboxRelaySnapshot {
            registered_relays: self.registered.load(Ordering::Acquire),
            ready_relays: self.ready.load(Ordering::Acquire),
            in_flight: self.in_flight.load(Ordering::Acquire),
            delivered: self.delivered.load(Ordering::Acquire),
            publish_failures: self.publish_failures.load(Ordering::Acquire),
            exhausted: self.exhausted.load(Ordering::Acquire),
            uncertain_after_publish: self.uncertain_after_publish.load(Ordering::Acquire),
            last_failure_code: *self
                .last_failure_code
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            state: OutboxRelayState::from_raw(self.state.load(Ordering::Acquire)).into(),
        }
    }
}

struct RelayWorker {
    store: Arc<dyn OutboxRelayStore>,
    publisher: Arc<dyn OutboxRelayPublisher>,
    policy: OutboxRelayPolicy,
    config: TransactionalInboxConfig,
    admission: CancellationToken,
    force: CancellationToken,
    terminal: Notify,
    terminal_state: AtomicU8,
    handle: tokio::sync::Mutex<Option<TaskReceipt>>,
    execution: Arc<TransactionTasks>,
    ledger: Arc<RelayLedger>,
    failure_signal: Arc<Notify>,
    terminal_failure: Arc<Mutex<Option<&'static str>>>,
    degradation: AtomicU8,
}

impl RelayWorker {
    fn new(
        store: Arc<dyn OutboxRelayStore>,
        publisher: Arc<dyn OutboxRelayPublisher>,
        policy: OutboxRelayPolicy,
        config: TransactionalInboxConfig,
        ledger: Arc<RelayLedger>,
        failure_signal: Arc<Notify>,
        terminal_failure: Arc<Mutex<Option<&'static str>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            publisher,
            policy,
            config,
            admission: CancellationToken::new(),
            force: CancellationToken::new(),
            terminal: Notify::new(),
            terminal_state: AtomicU8::new(RELAY_STARTING),
            handle: tokio::sync::Mutex::new(None),
            execution: Arc::default(),
            ledger,
            failure_signal,
            terminal_failure,
            degradation: AtomicU8::new(0),
        })
    }

    async fn start(self: &Arc<Self>) -> Result<(), MessageBrokerError> {
        let mut handle = self.handle.lock().await;
        if handle.is_some() {
            return Ok(());
        }
        if self.admission.is_cancelled() {
            self.terminal_state.store(RELAY_STOPPED, Ordering::Release);
            return Ok(());
        }
        let worker = Arc::clone(self);
        let (ready, ready_receiver) = oneshot::channel();
        *handle = Some(self.execution.tasks.spawn(async move {
            let outcome = worker
                .execution
                .run(
                    None,
                    AssertUnwindSafe(worker.clone().run(ready)).catch_unwind(),
                )
                .await
                .unwrap_or(Ok(Err("QUEUE_OUTBOX_RELAY_INTERRUPTED")));
            let failure = match outcome {
                Ok(Ok(())) => None,
                Ok(Err(code)) => Some(code),
                Err(_) => Some("QUEUE_OUTBOX_RELAY_PANICKED"),
            };
            let state = if let Some(code) = failure {
                worker.record_terminal_failure(code);
                *worker
                    .terminal_failure
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(code);
                worker.failure_signal.notify_waiters();
                RELAY_FAILED
            } else {
                RELAY_STOPPED
            };
            worker.terminal_state.store(state, Ordering::Release);
            worker.terminal.notify_waiters();
        }));
        drop(handle);
        ready_receiver.await.map_err(|_| {
            MessageBrokerError::RabbitMQError(RabbitMQError::QueueSettlement(
                "QUEUE_OUTBOX_RELAY_START_FAILED",
            ))
        })
    }

    async fn run(self: Arc<Self>, ready: oneshot::Sender<()>) -> Result<(), &'static str> {
        self.terminal_state.store(RELAY_READY, Ordering::Release);
        let _ready = RelayReadyGuard::new(Arc::clone(&self.ledger));
        // Starting another database-cell worker must not hide an existing
        // worker's degradation or overwrite a concurrent drain state.
        self.ledger.record_worker_ready();
        let _ = ready.send(());
        let owner = Uuid::new_v4();
        let mut cleanup_at = tokio::time::Instant::now() + self.policy.cleanup_interval;

        loop {
            if self.admission.is_cancelled() || self.force.is_cancelled() {
                break;
            }

            if tokio::time::Instant::now() >= cleanup_at {
                if let Err(error) = self.store.cleanup().await {
                    self.degrade_storage(error.code());
                }
                cleanup_at = tokio::time::Instant::now() + self.policy.cleanup_interval;
            }

            let records = match self.store.claim_batch(owner).await {
                Ok(records) => records,
                Err(error) => {
                    self.degrade_storage(error.code());
                    if error.code() == OUTBOX_RECORD_TOO_LARGE {
                        return Err(OUTBOX_RECORD_TOO_LARGE);
                    }
                    if wait_or_cancel(self.policy.initial_backoff, &self.admission, &self.force)
                        .await
                    {
                        break;
                    }
                    continue;
                }
            };

            if records.is_empty() {
                match self.store.exhausted_count().await {
                    Ok(exhausted) => {
                        self.ledger.exhausted.store(exhausted, Ordering::Release);
                        if exhausted == 0 {
                            // claim_batch + exhausted_count is the idle storage
                            // health probe. It proves storage recovery only;
                            // no broker operation occurred here.
                            self.recover_storage();
                        } else {
                            self.degrade_storage("QUEUE_OUTBOX_RELAY_ATTEMPTS_EXHAUSTED");
                            return Err("QUEUE_OUTBOX_RELAY_ATTEMPTS_EXHAUSTED");
                        }
                    }
                    Err(error) => self.degrade_storage(error.code()),
                }
                if wait_or_cancel(self.policy.poll_interval, &self.admission, &self.force).await {
                    break;
                }
                continue;
            }

            if records.len() > self.policy.batch_size
                || aggregate_body_bytes(&records) > self.policy.max_in_flight_bytes
            {
                self.degrade_storage("QUEUE_OUTBOX_RELAY_STORAGE_BOUND_VIOLATION");
                return Err("QUEUE_OUTBOX_RELAY_STORAGE_BOUND_VIOLATION");
            }

            for mut record in records {
                if self.force.is_cancelled() {
                    break;
                }
                let _in_flight = RelayInFlightGuard::new(Arc::clone(&self.ledger));
                record.publish_attempts = match self.store.begin_publish(&record).await {
                    Ok(Some(attempt)) => attempt,
                    // A batch tail may have expired and been reclaimed by a
                    // different process while this worker published earlier
                    // rows. The changed claim token proves another owner now
                    // has authority; skipping the stale local copy is healthy.
                    Ok(None) => continue,
                    Err(error) => {
                        self.degrade_storage(error.code());
                        return Err(error.code());
                    }
                };
                self.publish_record(&record).await?;
                // Graceful admission stop never abandons the tail of a batch
                // already leased from durable storage. Only force cancellation may
                // interrupt owned work; otherwise every claimed row reaches a
                // confirmed mark or an explicit durable failure release.
                if self.force.is_cancelled() {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn publish_record(&self, record: &RelayRecord) -> Result<(), &'static str> {
        if record.publish_attempts == 0
            || record.publish_attempts > self.policy.max_publish_attempts
        {
            self.degrade_storage("QUEUE_OUTBOX_RELAY_ATTEMPT_CONTRACT_VIOLATION");
            return Err("QUEUE_OUTBOX_RELAY_ATTEMPT_CONTRACT_VIOLATION");
        }

        let publish =
            self.publisher
                .publish(record, self.policy.publish_timeout, self.force.clone());
        let outcome = tokio::time::timeout(self.policy.publish_timeout, publish).await;
        match outcome {
            Ok(Ok(())) => {
                let mut marking = ConfirmedPublishGuard {
                    ledger: Arc::clone(&self.ledger),
                    marked: false,
                };
                #[cfg(feature = "test-support")]
                post_confirm_test_support::pause(record.event_id, &self.force).await;
                match self.store.mark_delivered(record).await {
                    Ok(()) => {
                        marking.marked = true;
                        self.ledger.delivered.fetch_add(1, Ordering::AcqRel);
                        self.recover_publisher();
                        self.recover_storage();
                    }
                    Err(error) => {
                        // The broker outcome is known but durable delivered marking is
                        // not. Do not release or rewrite this claim: lease expiry
                        // deliberately permits a duplicate publish with the exact
                        // same stable event identity.
                        self.degrade_storage(error.code());
                        self.recover_publisher();
                        if record.publish_attempts == self.policy.max_publish_attempts {
                            self.ledger.exhausted.fetch_add(1, Ordering::AcqRel);
                            return Err("QUEUE_OUTBOX_RELAY_ATTEMPTS_EXHAUSTED");
                        }
                    }
                }
            }
            failure => {
                self.ledger.publish_failures.fetch_add(1, Ordering::AcqRel);
                let code = match failure {
                    Ok(Err(error)) => error.error_code(),
                    Err(_) => "QUEUE_OUTBOX_RELAY_PUBLISH_TIMEOUT",
                    Ok(Ok(())) => unreachable!(),
                };
                self.degrade_publisher(code);
                let exponent = record.publish_attempts.saturating_sub(1).min(31);
                let retry_after = self
                    .policy
                    .initial_backoff
                    .saturating_mul(1u32 << exponent)
                    .min(self.policy.maximum_backoff);
                if let Err(error) = self.store.record_failure(record, retry_after, code).await {
                    self.degrade_storage(error.code());
                } else {
                    self.recover_storage();
                }
                if record.publish_attempts == self.policy.max_publish_attempts {
                    self.ledger.exhausted.fetch_add(1, Ordering::AcqRel);
                    self.degrade_publisher("QUEUE_OUTBOX_RELAY_ATTEMPTS_EXHAUSTED");
                    return Err("QUEUE_OUTBOX_RELAY_ATTEMPTS_EXHAUSTED");
                }
            }
        }
        Ok(())
    }

    fn degrade_storage(&self, code: &'static str) {
        self.degrade(DEGRADATION_STORAGE, code);
    }

    fn degrade_publisher(&self, code: &'static str) {
        self.degrade(DEGRADATION_PUBLISHER, code);
    }

    fn degrade(&self, cause: u8, code: &'static str) {
        self.ledger.record_failure(code);
        if self.degradation.fetch_or(cause, Ordering::AcqRel) == 0 {
            self.ledger.degraded_workers.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn record_terminal_failure(&self, code: &'static str) {
        if self.degradation.load(Ordering::Acquire) == 0 {
            self.degrade_storage(code);
        } else {
            self.ledger.record_failure(code);
        }
    }

    fn recover_storage(&self) {
        self.recover(DEGRADATION_STORAGE);
    }

    fn recover_publisher(&self) {
        self.recover(DEGRADATION_PUBLISHER);
    }

    fn recover(&self, cause: u8) {
        let previous = self.degradation.fetch_and(!cause, Ordering::AcqRel);
        if previous & cause != 0 && previous & !cause == 0 {
            let previous = self.ledger.degraded_workers.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0, "outbox relay degraded-worker underflow");
            if previous == 1 && self.ledger.exhausted.load(Ordering::Acquire) == 0 {
                self.ledger.record_recovered();
            }
        }
    }

    async fn reap(&self) -> Result<(), MessageBrokerError> {
        let receipt = self.handle.lock().await.clone();
        if let Some(receipt) = receipt {
            receipt
                .join()
                .await
                .map_err(|_| relay_failure("QUEUE_OUTBOX_RELAY_JOIN_FAILED"))?;
        }
        if self.terminal_state.load(Ordering::Acquire) == RELAY_FAILED {
            Err(relay_failure(
                self.terminal_failure
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .unwrap_or("QUEUE_OUTBOX_RELAY_FAILED"),
            ))
        } else {
            Ok(())
        }
    }

    fn request_force(&self) {
        self.admission.cancel();
        self.force.cancel();
        self.execution.request_force();
    }

    fn reconciled(&self) -> bool {
        self.handle
            .try_lock()
            .is_ok_and(|handle| match handle.as_ref() {
                Some(receipt) => receipt.terminal(),
                None => self.admission.is_cancelled(),
            })
    }
}

fn aggregate_body_bytes(records: &[RelayRecord]) -> usize {
    records.iter().fold(0usize, |total, record| {
        total.saturating_add(record.body.len())
    })
}

struct ConfirmedPublishGuard {
    ledger: Arc<RelayLedger>,
    marked: bool,
}
impl Drop for ConfirmedPublishGuard {
    fn drop(&mut self) {
        if !self.marked {
            self.ledger
                .uncertain_after_publish
                .fetch_add(1, Ordering::AcqRel);
        }
    }
}

struct RelayInFlightGuard {
    ledger: Arc<RelayLedger>,
}

struct RelayReadyGuard {
    ledger: Arc<RelayLedger>,
}

impl RelayReadyGuard {
    fn new(ledger: Arc<RelayLedger>) -> Self {
        ledger.ready.fetch_add(1, Ordering::AcqRel);
        Self { ledger }
    }
}

impl Drop for RelayReadyGuard {
    fn drop(&mut self) {
        self.ledger.ready.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RelayInFlightGuard {
    fn new(ledger: Arc<RelayLedger>) -> Self {
        ledger.in_flight.fetch_add(1, Ordering::AcqRel);
        Self { ledger }
    }
}

impl Drop for RelayInFlightGuard {
    fn drop(&mut self) {
        self.ledger.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn wait_or_cancel(
    duration: Duration,
    admission: &CancellationToken,
    force: &CancellationToken,
) -> bool {
    tokio::select! {
        biased;
        _ = force.cancelled() => true,
        _ = admission.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

pub(crate) struct OutboxRelaySupervisor {
    publisher: Arc<dyn OutboxRelayPublisher>,
    workers: Mutex<HashMap<Arc<str>, Arc<RelayWorker>>>,
    started: AtomicBool,
    admission_stopped: AtomicBool,
    ledger: Arc<RelayLedger>,
    failure_signal: Arc<Notify>,
    terminal_failure: Arc<Mutex<Option<&'static str>>>,
    transactional_runtimes: Mutex<Vec<TrackedTransactionalRuntime>>,
    budget: QueueShutdownBudget,
}

impl OutboxRelaySupervisor {
    pub(crate) fn new(publisher: Arc<dyn OutboxRelayPublisher>) -> Arc<Self> {
        Arc::new(Self {
            publisher,
            workers: Mutex::new(HashMap::new()),
            started: AtomicBool::new(false),
            admission_stopped: AtomicBool::new(false),
            ledger: Arc::new(RelayLedger::new()),
            failure_signal: Arc::new(Notify::new()),
            terminal_failure: Arc::new(Mutex::new(None)),
            transactional_runtimes: Mutex::new(Vec::new()),
            budget: QueueShutdownBudget::default(),
        })
    }

    pub(crate) async fn register(
        &self,
        key: Arc<str>,
        store: Arc<dyn OutboxRelayStore>,
        config: &TransactionalInboxConfig,
    ) -> Result<(), MessageBrokerError> {
        if self.admission_stopped.load(Ordering::Acquire) {
            return Err(relay_failure("QUEUE_OUTBOX_RELAY_ADMISSION_CLOSED"));
        }
        store
            .ensure_schema_ready()
            .await
            .map_err(|error| relay_failure(error.code()))?;
        let policy = OutboxRelayPolicy::from(config);
        let (worker, must_start) = {
            let mut workers = self
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // This check shares the workers lock with insertion. Once shutdown
            // seals admission, stop_admission() cannot take a snapshot that
            // misses a concurrently inserted worker.
            if self.admission_stopped.load(Ordering::Acquire) {
                return Err(relay_failure("QUEUE_OUTBOX_RELAY_ADMISSION_CLOSED"));
            }
            if let Some(existing) = workers.get(&key) {
                if existing.config != *config {
                    return Err(relay_failure("QUEUE_OUTBOX_RELAY_POLICY_CONFLICT"));
                }
                return Ok(());
            }
            let worker = RelayWorker::new(
                store,
                Arc::clone(&self.publisher),
                policy,
                config.clone(),
                Arc::clone(&self.ledger),
                Arc::clone(&self.failure_signal),
                Arc::clone(&self.terminal_failure),
            );
            if let Some(root) = self.budget.deadlines() {
                worker.execution.budget.install(root);
            }
            workers.insert(key, Arc::clone(&worker));
            self.ledger.registered.fetch_add(1, Ordering::AcqRel);
            (worker, self.started.load(Ordering::Acquire))
        };
        if must_start {
            worker.start().await?;
        }
        Ok(())
    }

    pub(crate) async fn start(&self) -> Result<(), MessageBrokerError> {
        if self.admission_stopped.load(Ordering::Acquire) {
            return Err(relay_failure("QUEUE_OUTBOX_RELAY_ADMISSION_CLOSED"));
        }
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let workers = self.workers();
        for worker in workers {
            worker.start().await?;
        }
        if self.ledger.registered.load(Ordering::Acquire) == 0 {
            self.ledger.state.store(RELAY_READY, Ordering::Release);
        }
        Ok(())
    }

    pub(crate) fn set_shutdown_deadlines(&self, deadlines: QueueShutdownDeadlines) {
        // Registration inherits this publication while holding the same locks.
        self.budget.install(deadlines);
        for worker in self.workers() {
            worker.execution.budget.install(deadlines);
        }
        for tracked in self.transactional_runtimes() {
            tracked.runtime.set_shutdown_deadlines(deadlines);
        }
    }

    pub(crate) fn stop_admission(&self) {
        if self.admission_stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        let workers = self.workers();
        let runtimes = self.transactional_runtimes();
        if self.budget.deadlines().is_none() {
            let timeout = workers
                .iter()
                .map(|w| w.policy.shutdown_drain_timeout)
                .chain(runtimes.iter().map(|r| r.timeout))
                .max()
                .unwrap_or(Duration::ZERO);
            self.set_shutdown_deadlines(QueueShutdownDeadlines::starting_at(
                tokio::time::Instant::now(),
                timeout,
            ));
        }
        self.ledger.state.store(RELAY_DRAINING, Ordering::Release);
        for worker in workers {
            worker.admission.cancel();
        }
        for tracked in runtimes {
            tracked.runtime.stop_admission();
        }
    }

    pub(crate) fn request_force(&self) {
        self.stop_admission();
        for worker in self.workers() {
            worker.request_force();
        }
        for tracked in self.transactional_runtimes() {
            tracked.runtime.request_force();
        }
    }

    async fn drain_before(&self, force: bool) -> Result<(), MessageBrokerError> {
        if force {
            self.request_force();
        } else {
            self.stop_admission();
        }
        let root = self
            .budget
            .deadlines()
            .expect("admission stop installs one attempt");
        let deadline = if force { root.hard() } else { root.graceful() };
        let mut operations: Vec<BoxFuture<'static, Result<(), MessageBrokerError>>> = Vec::new();
        for worker in self.workers() {
            operations.push(async move { worker.reap().await }.boxed());
        }
        for tracked in self.transactional_runtimes() {
            operations.push(
                async move {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if force {
                        tracked.runtime.force_drain(remaining).await
                    } else {
                        tracked.runtime.drain(remaining).await
                    }
                    .map_err(|e| relay_failure(e.code()))
                }
                .boxed(),
            );
        }
        let result = if operations.is_empty() {
            Ok(())
        } else {
            self.budget
                .run_until(deadline, join_all(operations))
                .await
                .map_err(|_| {
                    MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
                        "transactional outbox drain".into(),
                    ))
                })?
                .into_iter()
                .find_map(Result::err)
                .map_or(Ok(()), Err)
        };
        self.ledger.state.store(
            if result.is_ok() {
                RELAY_STOPPED
            } else {
                RELAY_FAILED
            },
            Ordering::Release,
        );
        result
    }

    pub(crate) async fn drain(&self) -> Result<(), MessageBrokerError> {
        self.drain_before(false).await
    }
    pub(crate) async fn force_drain(&self) -> Result<(), MessageBrokerError> {
        self.drain_before(true).await
    }

    pub(crate) fn reconciled(&self) -> bool {
        self.workers().iter().all(|worker| worker.reconciled())
            && self
                .transactional_runtimes()
                .iter()
                .all(|tracked| tracked.runtime.reconciled())
    }

    pub(crate) fn snapshot(&self) -> TransactionalOutboxRelaySnapshot {
        self.ledger.snapshot()
    }

    pub(crate) fn transactional_inbox_snapshot(&self) -> TransactionalInboxSnapshot {
        let mut snapshot = TransactionalInboxSnapshot::default();
        for tracked in self.transactional_runtimes() {
            snapshot.saturating_add_assign(tracked.runtime.transactional_inbox_snapshot());
        }
        snapshot
    }

    pub(crate) fn is_ready(&self) -> bool {
        let snapshot = self.snapshot();
        snapshot.exhausted == 0
            && snapshot.state == TransactionalOutboxRelayState::Ready
            && snapshot.registered_relays == snapshot.ready_relays
    }

    pub(crate) async fn wait_for_terminal_failure(&self) -> MessageBrokerError {
        loop {
            let notified = self.failure_signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(code) = *self
                .terminal_failure
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
            {
                return MessageBrokerError::RabbitMQError(RabbitMQError::QueueSettlement(code));
            }
            notified.as_mut().await;
        }
    }

    fn workers(&self) -> Vec<Arc<RelayWorker>> {
        self.workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect()
    }

    fn transactional_runtimes(&self) -> Vec<TrackedTransactionalRuntime> {
        self.transactional_runtimes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn track_transactional_runtime(
        &self,
        key: Arc<str>,
        runtime: Arc<dyn TransactionalRuntimeLifecycle>,
        timeout: Duration,
    ) {
        let mut runtimes = self
            .transactional_runtimes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !runtimes.iter().any(|existing| existing.key == key) {
            if let Some(root) = self.budget.deadlines() {
                runtime.set_shutdown_deadlines(root);
            }
            if self.admission_stopped.load(Ordering::Acquire) {
                runtime.stop_admission();
            }
            runtimes.push(TrackedTransactionalRuntime {
                key,
                runtime,
                timeout,
            });
        }
    }
}

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
pub(crate) async fn register_postgres_runtime(
    supervisor: &OutboxRelaySupervisor,
    runtime: Arc<crate::transactional_postgresql::PostgresTransactionalRuntime>,
) -> Result<(), MessageBrokerError> {
    let config = runtime.config().clone();
    let relay_key = postgres_runtime_key(runtime.database_cell());
    let lifecycle_key = transactional_runtime_lifecycle_key(
        "postgresql",
        runtime.database_cell(),
        runtime.queue_identity(),
    );
    let lifecycle: Arc<dyn TransactionalRuntimeLifecycle> = runtime.clone();
    supervisor.track_transactional_runtime(
        lifecycle_key,
        lifecycle,
        Duration::from_millis(config.shutdown_drain_timeout_millis),
    );
    supervisor
        .register(relay_key, Arc::new(PostgresRelayStore { runtime }), &config)
        .await
}

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
fn postgres_runtime_key(database_cell: Option<&str>) -> Arc<str> {
    Arc::from(match database_cell {
        Some(cell) => format!("postgresql:{cell}"),
        None => "postgresql:<single>".to_owned(),
    })
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
pub(crate) async fn register_mongo_runtime(
    supervisor: &OutboxRelaySupervisor,
    runtime: Arc<crate::transactional_mongodb::MongoTransactionalRuntime>,
) -> Result<(), MessageBrokerError> {
    let config = runtime.config().clone();
    let relay_key = mongo_runtime_key(runtime.database_cell());
    let lifecycle_key = transactional_runtime_lifecycle_key(
        "mongodb",
        runtime.database_cell(),
        runtime.queue_identity(),
    );
    let lifecycle: Arc<dyn TransactionalRuntimeLifecycle> = runtime.clone();
    supervisor.track_transactional_runtime(
        lifecycle_key,
        lifecycle,
        Duration::from_millis(config.shutdown_drain_timeout_millis),
    );
    supervisor
        .register(relay_key, Arc::new(MongoRelayStore { runtime }), &config)
        .await
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
fn mongo_runtime_key(database_cell: Option<&str>) -> Arc<str> {
    Arc::from(match database_cell {
        Some(cell) => format!("mongodb:{cell}"),
        None => "mongodb:<single>".to_owned(),
    })
}

/// Relay workers are shared per backend/database authority, but transaction
/// ownership is per physical queue runtime. Keeping the queue identity in this
/// key prevents worker de-duplication from also (incorrectly) de-duplicating
/// the independent transaction trackers which must all drain before DI
/// disposal.
fn transactional_runtime_lifecycle_key(
    backend: &str,
    database_cell: Option<&str>,
    queue_identity: &str,
) -> Arc<str> {
    let database_cell = database_cell.unwrap_or("<single>");
    Arc::from(format!(
        "{backend}:cell:{}:{database_cell}:queue:{}:{queue_identity}",
        database_cell.len(),
        queue_identity.len(),
    ))
}

fn relay_failure(code: &'static str) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::QueueSettlement(code))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashSet, VecDeque},
        sync::atomic::AtomicUsize,
    };

    use super::*;

    struct FakeStore {
        claimed: Mutex<VecDeque<Vec<RelayRecord>>>,
        claim_failures: Mutex<VecDeque<RelayFailure>>,
        claim_calls: AtomicUsize,
        delivered: Mutex<Vec<Uuid>>,
        failed: Mutex<Vec<Uuid>>,
        mark_failures: AtomicU64,
        mark_pending: AtomicBool,
        mark_entered: CancellationToken,
        mark_release: CancellationToken,
        exhausted: AtomicU64,
        ready: AtomicBool,
        begun: Mutex<Vec<Uuid>>,
        lost_before_begin: Mutex<HashSet<Uuid>>,
    }

    impl FakeStore {
        fn with_batches(batches: Vec<Vec<RelayRecord>>) -> Arc<Self> {
            Arc::new(Self {
                claimed: Mutex::new(batches.into()),
                claim_failures: Mutex::new(VecDeque::new()),
                claim_calls: AtomicUsize::new(0),
                delivered: Mutex::new(Vec::new()),
                failed: Mutex::new(Vec::new()),
                mark_failures: AtomicU64::new(0),
                mark_pending: AtomicBool::new(false),
                mark_entered: CancellationToken::new(),
                mark_release: CancellationToken::new(),
                exhausted: AtomicU64::new(0),
                ready: AtomicBool::new(true),
                begun: Mutex::new(Vec::new()),
                lost_before_begin: Mutex::new(HashSet::new()),
            })
        }
    }

    #[async_trait]
    impl OutboxRelayStore for FakeStore {
        async fn ensure_schema_ready(&self) -> Result<(), RelayFailure> {
            self.ready
                .load(Ordering::Acquire)
                .then_some(())
                .ok_or(RelayFailure::new("SCHEMA_NOT_READY"))
        }

        async fn claim_batch(&self, _owner: Uuid) -> Result<Vec<RelayRecord>, RelayFailure> {
            self.claim_calls.fetch_add(1, Ordering::AcqRel);
            if let Some(error) = self
                .claim_failures
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
            {
                return Err(error);
            }
            Ok(self
                .claimed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or_default())
        }

        async fn begin_publish(&self, record: &RelayRecord) -> Result<Option<u32>, RelayFailure> {
            if self
                .lost_before_begin
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&record.event_id)
            {
                return Ok(None);
            }
            self.begun
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(record.event_id);
            record
                .publish_attempts
                .checked_add(1)
                .map(Some)
                .ok_or(RelayFailure::new(
                    "QUEUE_OUTBOX_RELAY_ATTEMPT_CONTRACT_VIOLATION",
                ))
        }

        async fn mark_delivered(&self, record: &RelayRecord) -> Result<(), RelayFailure> {
            if self.mark_pending.load(Ordering::Acquire) {
                self.mark_entered.cancel();
                self.mark_release.cancelled().await;
            }
            if self.mark_failures.load(Ordering::Acquire) > 0 {
                self.mark_failures.fetch_sub(1, Ordering::AcqRel);
                return Err(RelayFailure::new("MARK_FAILED"));
            }
            self.delivered
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(record.event_id);
            Ok(())
        }

        async fn record_failure(
            &self,
            record: &RelayRecord,
            _retry_after: Duration,
            _code: &'static str,
        ) -> Result<(), RelayFailure> {
            self.failed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(record.event_id);
            if record.publish_attempts >= 5 {
                self.exhausted.store(1, Ordering::Release);
            }
            Ok(())
        }

        async fn cleanup(&self) -> Result<(), RelayFailure> {
            Ok(())
        }

        async fn exhausted_count(&self) -> Result<u64, RelayFailure> {
            Ok(self.exhausted.load(Ordering::Acquire))
        }
    }

    struct FakePublisher {
        outcomes: Mutex<VecDeque<Result<(), MessageBrokerError>>>,
        events: Mutex<Vec<Uuid>>,
        pending: AtomicBool,
        block_first: AtomicBool,
        first_entered: Notify,
        first_release: Notify,
        calls: AtomicUsize,
    }

    impl FakePublisher {
        fn new(outcomes: Vec<Result<(), MessageBrokerError>>) -> Arc<Self> {
            Arc::new(Self {
                outcomes: Mutex::new(outcomes.into()),
                events: Mutex::new(Vec::new()),
                pending: AtomicBool::new(false),
                block_first: AtomicBool::new(false),
                first_entered: Notify::new(),
                first_release: Notify::new(),
                calls: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl OutboxRelayPublisher for FakePublisher {
        async fn publish(
            &self,
            record: &RelayRecord,
            _timeout: Duration,
            cancellation: CancellationToken,
        ) -> Result<(), MessageBrokerError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(record.event_id);
            if self.block_first.swap(false, Ordering::AcqRel) {
                self.first_entered.notify_one();
                self.first_release.notified().await;
            }
            if self.pending.load(Ordering::Acquire) {
                cancellation.cancelled().await;
                return Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled));
            }
            self.outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or(Ok(()))
        }
    }

    struct FakeTransactionalRuntime {
        active: AtomicUsize,
        admission_stops: AtomicUsize,
        drains: AtomicUsize,
        force_drains: AtomicUsize,
    }

    impl FakeTransactionalRuntime {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                active: AtomicUsize::new(1),
                admission_stops: AtomicUsize::new(0),
                drains: AtomicUsize::new(0),
                force_drains: AtomicUsize::new(0),
            })
        }
    }

    struct BlockingForceRuntime {
        active: AtomicUsize,
        admission_stops: AtomicUsize,
        force_drains: AtomicUsize,
        block_force: bool,
    }

    impl BlockingForceRuntime {
        fn new(block_force: bool) -> Arc<Self> {
            Arc::new(Self {
                active: AtomicUsize::new(1),
                admission_stops: AtomicUsize::new(0),
                force_drains: AtomicUsize::new(0),
                block_force,
            })
        }
    }

    #[async_trait]
    impl TransactionalRuntimeLifecycle for BlockingForceRuntime {
        fn reconciled(&self) -> bool {
            self.active_transactions() == 0
        }
        fn stop_admission(&self) {
            self.admission_stops.fetch_add(1, Ordering::AcqRel);
        }

        fn active_transactions(&self) -> usize {
            self.active.load(Ordering::Acquire)
        }

        async fn drain(&self, _timeout: Duration) -> Result<(), RelayFailure> {
            Ok(())
        }

        async fn force_drain(&self, _timeout: Duration) -> Result<(), RelayFailure> {
            self.force_drains.fetch_add(1, Ordering::AcqRel);
            if self.block_force {
                std::future::pending::<()>().await;
            }
            self.active.store(0, Ordering::Release);
            Ok(())
        }
    }

    #[async_trait]
    impl TransactionalRuntimeLifecycle for FakeTransactionalRuntime {
        fn reconciled(&self) -> bool {
            self.active_transactions() == 0
        }
        fn stop_admission(&self) {
            self.admission_stops.fetch_add(1, Ordering::AcqRel);
        }

        fn active_transactions(&self) -> usize {
            self.active.load(Ordering::Acquire)
        }

        async fn drain(&self, _timeout: Duration) -> Result<(), RelayFailure> {
            self.drains.fetch_add(1, Ordering::AcqRel);
            self.active.store(0, Ordering::Release);
            Ok(())
        }

        async fn force_drain(&self, _timeout: Duration) -> Result<(), RelayFailure> {
            self.force_drains.fetch_add(1, Ordering::AcqRel);
            self.active.store(0, Ordering::Release);
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn confirmed_publish_interrupted_during_mark_is_uncertain_and_joined() {
        let store = FakeStore::with_batches(vec![vec![record(Uuid::new_v4(), 8)]]);
        store.mark_pending.store(true, Ordering::Release);
        let publisher = FakePublisher::new(vec![]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("mark-pending"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        store.mark_entered.cancelled().await;
        let started = tokio::time::Instant::now();
        supervisor.set_shutdown_deadlines(QueueShutdownDeadlines::before(
            started,
            started + Duration::from_millis(100),
        ));
        let error = supervisor.force_drain().await.unwrap_err();
        assert_eq!(error.error_code(), "QUEUE_OUTBOX_RELAY_INTERRUPTED");
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_millis(75)
        );
        assert!(supervisor.reconciled());
        assert_eq!(supervisor.snapshot().uncertain_after_publish, 1);
        assert_eq!(supervisor.snapshot().delivered, 0);
        assert!(store.delivered.lock().unwrap().is_empty());
        assert!(
            store.failed.lock().unwrap().is_empty(),
            "confirmed claim must not be rewritten as publish failure"
        );
        assert_eq!(publisher.calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn cancelling_relay_drain_keeps_the_worker_join_and_durable_mark_owner() {
        let store = FakeStore::with_batches(vec![vec![record(Uuid::new_v4(), 8)]]);
        store.mark_pending.store(true, Ordering::Release);
        let supervisor = OutboxRelaySupervisor::new(FakePublisher::new(vec![]));
        let now = tokio::time::Instant::now();
        supervisor.set_shutdown_deadlines(QueueShutdownDeadlines::before(
            now + Duration::from_secs(5),
            now + Duration::from_secs(6),
        ));
        supervisor
            .register(Arc::from("cancel-waiter"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        store.mark_entered.cancelled().await;
        supervisor.stop_admission();
        let child = supervisor.clone();
        let drain = tokio::spawn(async move { child.drain().await });
        tokio::task::yield_now().await;
        drain.abort();
        assert!(drain.await.unwrap_err().is_cancelled());
        assert!(!supervisor.reconciled());
        assert!(supervisor.workers()[0].handle.lock().await.is_some());
        store.mark_release.cancel();
        supervisor.drain().await.unwrap();
        assert!(supervisor.reconciled());
        assert_eq!(supervisor.snapshot().delivered, 1);
        assert_eq!(supervisor.snapshot().uncertain_after_publish, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_force_wait_cannot_restart_the_original_root() {
        let supervisor = OutboxRelaySupervisor::new(FakePublisher::new(Vec::new()));
        let stuck = BlockingForceRuntime::new(true);
        supervisor.track_transactional_runtime(
            Arc::from("stuck"),
            stuck.clone(),
            Duration::from_secs(30),
        );
        let started = tokio::time::Instant::now();
        supervisor.set_shutdown_deadlines(QueueShutdownDeadlines::before(
            started,
            started + Duration::from_millis(20),
        ));
        assert!(supervisor.force_drain().await.is_err());
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_millis(20)
        );
        assert_eq!(stuck.force_drains.load(Ordering::Acquire), 1);
        assert!(supervisor.force_drain().await.is_err());
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_millis(20)
        );
        assert_eq!(
            stuck.force_drains.load(Ordering::Acquire),
            1,
            "expired root may signal, but cannot start another I/O future"
        );
        assert!(!supervisor.reconciled());
    }

    #[tokio::test]
    async fn supervisor_uses_backend_neutral_transaction_lifecycle_for_each_drain_mode() {
        let runtime = FakeTransactionalRuntime::new();
        let supervisor = OutboxRelaySupervisor::new(FakePublisher::new(Vec::new()));
        let erased: Arc<dyn TransactionalRuntimeLifecycle> = runtime.clone();
        supervisor.track_transactional_runtime(
            Arc::from("test:graceful"),
            erased,
            Duration::from_millis(10),
        );
        supervisor.drain().await.unwrap();
        assert_eq!(runtime.admission_stops.load(Ordering::Acquire), 1);
        assert_eq!(runtime.drains.load(Ordering::Acquire), 1);
        assert_eq!(runtime.force_drains.load(Ordering::Acquire), 0);
        assert!(supervisor.reconciled());

        let runtime = FakeTransactionalRuntime::new();
        let supervisor = OutboxRelaySupervisor::new(FakePublisher::new(Vec::new()));
        let erased: Arc<dyn TransactionalRuntimeLifecycle> = runtime.clone();
        supervisor.track_transactional_runtime(
            Arc::from("test:forced"),
            erased,
            Duration::from_millis(10),
        );
        supervisor.force_drain().await.unwrap();
        assert_eq!(runtime.admission_stops.load(Ordering::Acquire), 1);
        assert_eq!(runtime.drains.load(Ordering::Acquire), 0);
        assert_eq!(runtime.force_drains.load(Ordering::Acquire), 1);
        assert!(supervisor.reconciled());
    }

    #[tokio::test]
    async fn force_drain_starts_every_runtime_before_a_stuck_runtime_exhausts_the_deadline() {
        let stuck = BlockingForceRuntime::new(true);
        let healthy = BlockingForceRuntime::new(false);
        let supervisor = OutboxRelaySupervisor::new(FakePublisher::new(Vec::new()));
        let stuck_erased: Arc<dyn TransactionalRuntimeLifecycle> = stuck.clone();
        supervisor.track_transactional_runtime(
            Arc::from("test:stuck"),
            stuck_erased,
            Duration::from_millis(25),
        );
        let healthy_erased: Arc<dyn TransactionalRuntimeLifecycle> = healthy.clone();
        supervisor.track_transactional_runtime(
            Arc::from("test:healthy"),
            healthy_erased,
            Duration::from_millis(25),
        );

        let error = supervisor
            .force_drain()
            .await
            .expect_err("the shared deadline must report the stuck runtime");

        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(_))
        ));
        assert_eq!(stuck.admission_stops.load(Ordering::Acquire), 1);
        assert_eq!(healthy.admission_stops.load(Ordering::Acquire), 1);
        assert_eq!(stuck.force_drains.load(Ordering::Acquire), 1);
        assert_eq!(healthy.force_drains.load(Ordering::Acquire), 1);
        assert_eq!(healthy.active.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn shared_relay_database_tracks_every_physical_queue_runtime_for_shutdown() {
        for backend in ["postgresql", "mongodb"] {
            let first = FakeTransactionalRuntime::new();
            let second = FakeTransactionalRuntime::new();
            let supervisor = OutboxRelaySupervisor::new(FakePublisher::new(Vec::new()));

            let first_erased: Arc<dyn TransactionalRuntimeLifecycle> = first.clone();
            supervisor.track_transactional_runtime(
                transactional_runtime_lifecycle_key(backend, Some("primary"), "orders.created"),
                first_erased,
                Duration::from_millis(10),
            );
            let second_erased: Arc<dyn TransactionalRuntimeLifecycle> = second.clone();
            supervisor.track_transactional_runtime(
                transactional_runtime_lifecycle_key(backend, Some("primary"), "orders.cancelled"),
                second_erased,
                Duration::from_millis(10),
            );

            assert_eq!(
                supervisor
                    .transactional_inbox_snapshot()
                    .active_transactions,
                2
            );

            supervisor.drain().await.unwrap();

            assert_eq!(first.admission_stops.load(Ordering::Acquire), 1);
            assert_eq!(second.admission_stops.load(Ordering::Acquire), 1);
            assert_eq!(first.drains.load(Ordering::Acquire), 1);
            assert_eq!(second.drains.load(Ordering::Acquire), 1);
            assert!(supervisor.reconciled());
        }
    }

    #[test]
    fn lifecycle_keys_include_backend_cell_and_physical_queue() {
        assert_eq!(
            transactional_runtime_lifecycle_key("postgresql", None, "orders.created").as_ref(),
            "postgresql:cell:8:<single>:queue:14:orders.created"
        );
        assert_ne!(
            transactional_runtime_lifecycle_key("mongodb", Some("primary"), "orders.created"),
            transactional_runtime_lifecycle_key("mongodb", Some("primary"), "orders.cancelled")
        );
        assert_ne!(
            transactional_runtime_lifecycle_key("postgresql", Some("primary"), "orders.created"),
            transactional_runtime_lifecycle_key("mongodb", Some("primary"), "orders.created")
        );
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    #[test]
    fn postgres_runtime_keys_are_backend_namespaced() {
        assert_eq!(postgres_runtime_key(None).as_ref(), "postgresql:<single>");
        assert_eq!(
            postgres_runtime_key(Some("primary")).as_ref(),
            "postgresql:primary"
        );
    }

    fn record(event_id: Uuid, bytes: usize) -> RelayRecord {
        RelayRecord {
            record_id: Uuid::new_v4(),
            claim_token: Uuid::new_v4(),
            event_id,
            exchange: Arc::from("events"),
            routing_key: Arc::from("orders.created"),
            schema_version: 1,
            content_kind: Arc::from("json"),
            content_type: Arc::from("application/json"),
            traceparent: None,
            publish_attempts: 0,
            body: Arc::from(vec![b'x'; bytes]),
        }
    }

    fn config() -> TransactionalInboxConfig {
        TransactionalInboxConfig {
            relay_poll_interval_millis: 1,
            relay_publish_timeout_millis: 100,
            relay_retry_initial_backoff_millis: 1,
            relay_retry_max_backoff_millis: 2,
            cleanup_interval_secs: 60,
            shutdown_drain_timeout_millis: 100,
            ..TransactionalInboxConfig::default()
        }
    }

    async fn wait_for_calls(publisher: &FakePublisher, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while publisher.calls.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publisher calls");
    }

    async fn wait_for_claims(store: &FakeStore, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.claim_calls.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("storage claims");
    }

    #[tokio::test]
    async fn successful_empty_probe_recovers_storage_degradation() {
        let store = FakeStore::with_batches(vec![]);
        store
            .claim_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_back(RelayFailure::new("STORAGE_TEMPORARILY_UNAVAILABLE"));
        let publisher = FakePublisher::new(vec![]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();

        wait_for_claims(&store, 2).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while supervisor.snapshot().state != TransactionalOutboxRelayState::Ready {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("storage recovery");
        assert!(supervisor.is_ready());
        assert_eq!(publisher.calls.load(Ordering::Acquire), 0);

        supervisor.stop_admission();
        supervisor.drain().await.unwrap();
    }

    #[tokio::test]
    async fn empty_storage_probe_does_not_recover_publisher_degradation() {
        let store = FakeStore::with_batches(vec![vec![record(Uuid::new_v4(), 8)]]);
        let publisher = FakePublisher::new(vec![Err(MessageBrokerError::RabbitMQError(
            RabbitMQError::PublisherNack,
        ))]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();

        wait_for_calls(&publisher, 1).await;
        wait_for_claims(&store, 2).await;
        assert_eq!(
            supervisor.snapshot().state,
            TransactionalOutboxRelayState::Degraded
        );
        assert!(!supervisor.is_ready());

        supervisor.stop_admission();
        supervisor.drain().await.unwrap();
    }

    #[tokio::test]
    async fn starting_another_cell_does_not_hide_existing_worker_degradation() {
        let degraded_store = FakeStore::with_batches(vec![vec![record(Uuid::new_v4(), 8)]]);
        let publisher = FakePublisher::new(vec![Err(MessageBrokerError::RabbitMQError(
            RabbitMQError::PublisherNack,
        ))]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(
                Arc::from("degraded-cell"),
                degraded_store.clone(),
                &config(),
            )
            .await
            .unwrap();
        supervisor.start().await.unwrap();

        wait_for_calls(&publisher, 1).await;
        wait_for_claims(&degraded_store, 2).await;
        assert_eq!(
            supervisor.snapshot().state,
            TransactionalOutboxRelayState::Degraded
        );

        let healthy_store = FakeStore::with_batches(vec![]);
        supervisor
            .register(Arc::from("healthy-cell"), healthy_store.clone(), &config())
            .await
            .unwrap();
        wait_for_claims(&healthy_store, 1).await;

        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.registered_relays, 2);
        assert_eq!(snapshot.ready_relays, 2);
        assert_eq!(snapshot.state, TransactionalOutboxRelayState::Degraded);
        assert!(!supervisor.is_ready());

        supervisor.stop_admission();
        supervisor.drain().await.unwrap();
    }

    #[tokio::test]
    async fn oversized_persisted_record_is_a_typed_terminal_storage_failure() {
        let store = FakeStore::with_batches(vec![]);
        store
            .claim_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_back(RelayFailure::new(OUTBOX_RECORD_TOO_LARGE));
        let publisher = FakePublisher::new(vec![]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store, &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();

        assert_eq!(
            supervisor.wait_for_terminal_failure().await.error_code(),
            OUTBOX_RECORD_TOO_LARGE
        );
        supervisor.stop_admission();
        assert_eq!(
            supervisor.drain().await.unwrap_err().error_code(),
            OUTBOX_RECORD_TOO_LARGE
        );
        assert_eq!(publisher.calls.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn confirmed_publish_is_marked_once_and_drained_before_close() {
        let event_id = Uuid::new_v4();
        let store = FakeStore::with_batches(vec![vec![record(event_id, 8)]]);
        let publisher = FakePublisher::new(vec![Ok(())]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        wait_for_calls(&publisher, 1).await;
        supervisor.stop_admission();
        supervisor.drain().await.unwrap();

        assert_eq!(
            store
                .delivered
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &[event_id]
        );
        assert!(supervisor.reconciled());
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.delivered, 1);
        assert_eq!(snapshot.in_flight, 0);
        assert_eq!(snapshot.state, TransactionalOutboxRelayState::Stopped);
    }

    #[tokio::test]
    async fn publish_then_mark_failure_preserves_same_event_id_for_redelivery() {
        let event_id = Uuid::new_v4();
        let first = record(event_id, 8);
        let second = RelayRecord {
            claim_token: Uuid::new_v4(),
            publish_attempts: 1,
            ..first.clone()
        };
        let store = FakeStore::with_batches(vec![vec![first], vec![second]]);
        store.mark_failures.store(1, Ordering::Release);
        let publisher = FakePublisher::new(vec![Ok(()), Ok(())]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        wait_for_calls(&publisher, 2).await;
        supervisor.stop_admission();
        supervisor.drain().await.unwrap();

        let events = publisher
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(events.as_slice(), &[event_id, event_id]);
        assert_eq!(supervisor.snapshot().uncertain_after_publish, 1);
    }

    #[tokio::test]
    async fn publish_failure_retries_within_bound_then_releases_claim() {
        let event_id = Uuid::new_v4();
        let records = (0..5)
            .map(|publish_attempts| {
                vec![RelayRecord {
                    publish_attempts,
                    claim_token: Uuid::new_v4(),
                    ..record(event_id, 8)
                }]
            })
            .collect();
        let store = FakeStore::with_batches(records);
        let failure = || {
            Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::PublisherNack,
            ))
        };
        let publisher =
            FakePublisher::new(vec![failure(), failure(), failure(), failure(), failure()]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        wait_for_calls(&publisher, 5).await;
        let terminal = supervisor.wait_for_terminal_failure().await;
        assert_eq!(
            terminal.error_code(),
            "QUEUE_OUTBOX_RELAY_ATTEMPTS_EXHAUSTED"
        );
        supervisor.stop_admission();
        assert_eq!(
            supervisor.drain().await.unwrap_err().error_code(),
            "QUEUE_OUTBOX_RELAY_ATTEMPTS_EXHAUSTED"
        );

        assert_eq!(publisher.calls.load(Ordering::Acquire), 5);
        let failures = store
            .failed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(failures.as_slice(), &[event_id; 5]);
        assert_eq!(supervisor.snapshot().exhausted, 1);
    }

    #[tokio::test]
    async fn force_drain_aborts_and_joins_pending_publish() {
        let store = FakeStore::with_batches(vec![vec![record(Uuid::new_v4(), 8)]]);
        let publisher = FakePublisher::new(vec![]);
        publisher.pending.store(true, Ordering::Release);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store, &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        wait_for_calls(&publisher, 1).await;
        supervisor.stop_admission();
        supervisor.force_drain().await.unwrap();

        assert!(supervisor.reconciled());
        assert_eq!(supervisor.snapshot().in_flight, 0);
    }

    #[tokio::test]
    async fn force_drain_does_not_consume_publish_attempts_for_unvisited_batch_tail() {
        let events = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let store = FakeStore::with_batches(vec![vec![
            record(events[0], 8),
            record(events[1], 8),
            record(events[2], 8),
        ]]);
        let publisher = FakePublisher::new(vec![]);
        publisher.pending.store(true, Ordering::Release);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        wait_for_calls(&publisher, 1).await;

        supervisor.stop_admission();
        supervisor.force_drain().await.unwrap();

        assert_eq!(
            store
                .begun
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &[events[0]],
            "only the exact record entering broker I/O may consume an attempt"
        );
        assert_eq!(publisher.calls.load(Ordering::Acquire), 1);
        assert_eq!(supervisor.snapshot().in_flight, 0);
    }

    #[tokio::test]
    async fn reclaimed_batch_tail_is_skipped_without_stopping_the_relay() {
        let reclaimed = Uuid::new_v4();
        let owned = Uuid::new_v4();
        let store = FakeStore::with_batches(vec![vec![record(reclaimed, 8), record(owned, 8)]]);
        store
            .lost_before_begin
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(reclaimed);
        let publisher = FakePublisher::new(vec![Ok(())]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        wait_for_calls(&publisher, 1).await;

        supervisor.stop_admission();
        supervisor.drain().await.unwrap();

        assert_eq!(
            publisher
                .events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &[owned]
        );
        assert_eq!(
            store
                .delivered
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &[owned]
        );
        assert_eq!(
            supervisor.snapshot().state,
            TransactionalOutboxRelayState::Stopped
        );
    }

    #[tokio::test]
    async fn graceful_stop_finishes_every_record_in_an_already_claimed_batch() {
        let events = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let store = FakeStore::with_batches(vec![vec![
            record(events[0], 8),
            record(events[1], 8),
            record(events[2], 8),
        ]]);
        let publisher = FakePublisher::new(vec![Ok(()), Ok(()), Ok(())]);
        publisher.block_first.store(true, Ordering::Release);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store.clone(), &config())
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        publisher.first_entered.notified().await;

        supervisor.stop_admission();
        publisher.first_release.notify_one();
        supervisor.drain().await.unwrap();

        assert_eq!(publisher.calls.load(Ordering::Acquire), 3);
        assert_eq!(
            store
                .delivered
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &events
        );
        assert_eq!(supervisor.snapshot().in_flight, 0);
    }

    #[tokio::test]
    async fn one_database_cell_has_one_policy_and_one_worker_authority() {
        let first_store = FakeStore::with_batches(vec![]);
        let second_store = FakeStore::with_batches(vec![]);
        let publisher = FakePublisher::new(vec![]);
        let supervisor = OutboxRelaySupervisor::new(publisher);
        supervisor
            .register(Arc::from("orders"), first_store, &config())
            .await
            .unwrap();
        supervisor
            .register(Arc::from("orders"), second_store.clone(), &config())
            .await
            .unwrap();
        assert_eq!(supervisor.snapshot().registered_relays, 1);

        let mut conflicting = config();
        conflicting.relay_batch_size += 1;
        assert_eq!(
            supervisor
                .register(Arc::from("orders"), second_store, &conflicting)
                .await
                .unwrap_err()
                .error_code(),
            "QUEUE_OUTBOX_RELAY_POLICY_CONFLICT"
        );
    }

    #[tokio::test]
    async fn storage_cannot_return_more_than_the_configured_batch_or_byte_bound() {
        let mut policy = config();
        policy.relay_batch_size = 1;
        policy.relay_max_in_flight_bytes = 8;
        let store = FakeStore::with_batches(vec![vec![
            record(Uuid::new_v4(), 8),
            record(Uuid::new_v4(), 1),
        ]]);
        let publisher = FakePublisher::new(vec![]);
        let supervisor = OutboxRelaySupervisor::new(publisher.clone());
        supervisor
            .register(Arc::from("default"), store, &policy)
            .await
            .unwrap();
        supervisor.start().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while supervisor.snapshot().last_failure_code
                != Some("QUEUE_OUTBOX_RELAY_STORAGE_BOUND_VIOLATION")
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        supervisor.stop_admission();
        assert_eq!(
            supervisor.drain().await.unwrap_err().error_code(),
            "QUEUE_OUTBOX_RELAY_STORAGE_BOUND_VIOLATION"
        );

        assert_eq!(publisher.calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn persisted_trace_context_and_canonical_envelope_are_republished() {
        let event_id = Uuid::new_v4();
        let mut record = record(event_id, 8);
        record.schema_version = 3;
        record.traceparent = Some(Arc::from(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));

        let properties = materialize_publish_properties(&record).unwrap();
        let headers = properties.headers().as_ref().unwrap();
        let long_string = |name: &str| match headers.inner().get(name) {
            Some(AMQPValue::LongString(value)) => std::str::from_utf8(value.as_bytes()).unwrap(),
            value => panic!("missing long-string {name}: {value:?}"),
        };
        assert_eq!(long_string("x-lily-event-id"), event_id.to_string());
        assert_eq!(long_string("x-lily-schema-version"), "3");
        assert_eq!(long_string("x-lily-content-kind"), "json");
        assert_eq!(
            long_string("traceparent"),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
        assert_eq!(properties.delivery_mode().as_ref().copied(), Some(2));
        assert_eq!(
            properties.message_id().as_ref().map(ToString::to_string),
            Some(event_id.to_string())
        );
    }

    #[test]
    fn invalid_or_noncanonical_trace_context_fails_before_broker_io() {
        let mut invalid = record(Uuid::new_v4(), 8);
        invalid.traceparent = Some(Arc::from("not-a-traceparent"));
        assert_eq!(
            materialize_publish_properties(&invalid)
                .unwrap_err()
                .error_code(),
            "BROKER_INVALID_MESSAGE"
        );
    }
}
