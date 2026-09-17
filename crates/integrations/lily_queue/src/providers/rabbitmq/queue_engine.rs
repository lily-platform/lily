use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{FutureExt, StreamExt};
use lapin::{
    Channel, ExchangeKind,
    message::Delivery,
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicNackOptions,
        BasicQosOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    protocol::{AMQPErrorKind, AMQPSoftError},
    types::{AMQPValue, FieldTable, ShortString},
};
use lily_config::{
    RabbitMqExchangeKind, RabbitMqQueueType, RabbitMqTopologyOwnership, RabbitMqTopologyPlan,
};
use lily_error::application::{
    MessageBrokerError, QueueHandlerError, QueueHandlerFailureClass,
    message_broker::{
        RabbitMQError, RabbitMqConsumerTaskFailure, RabbitMqConsumerTaskFailureKind,
        RabbitMqConsumerTaskRole, RabbitMqTopologyError, RabbitMqTopologyErrorKind,
        RabbitMqTopologyOperation, RabbitMqTopologyResourceKind,
    },
};
use lily_queue_client::{RabbitMqTopologyBootstrapReport, execute_rabbitmq_topology_plan};
use opentelemetry::propagation::Extractor;
use std::{
    collections::BTreeMap,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::{error, warn};

use crate::{
    DeliveryCancellationReason,
    cancellation::DeliveryCancellationSource,
    channel_manager_trait::ChannelManager,
    delivery_context::{
        ContentKind, DeliveryContext, DeliveryHeaderValue, DeliveryHeaders, DeliveryInput,
        DeliveryProperties, EventId, Redelivered, RetryCount, SchemaVersion,
    },
    queue_engine_trait::{QueueDeliveryHandler, QueueEngine, QueueExecutionError},
    retry_engine_trait::{FailureClass, RetryEngine, RetryHandoff},
    setting::{MessageBrokerSetting, QueueRuntimeSetting},
    settlement::{
        ExecutionOutcome, HandoffPlan, HandoffReceipt, SettlementAuthority, SettlementFailure,
        SettlementFailureKind, SettlementObserver, SettlementObserverFailure, SettlementOperation,
        SettlementPort, SettlementReport, SettlementTerminal, materialize_broker_dead_letter,
        materialize_delivery,
    },
    shutdown_budget::{QueueShutdownBudget, QueueShutdownDeadlines},
    telemetry::{
        ConsumerReadinessGuard, DeliveryAttemptGuard, DeliveryTerminalLedger,
        DeliveryTerminalSnapshot,
    },
};

#[cfg(test)]
use super::metadata::{
    MAX_AMQP_HEADER_ENTRIES, MAX_AMQP_HEADER_KEY_BYTES, MAX_AMQP_METADATA_BYTES,
    MAX_AMQP_NESTED_ELEMENTS, MAX_AMQP_NESTING_DEPTH, MAX_AMQP_VALUE_BYTES, validate_amqp_metadata,
};
use super::metadata::{canonical_retry_count, validate_handoff_metadata_headroom};
use super::topology::QueueTopology;

use std::future::Future;
use tokio::task::{Id, JoinHandle, JoinSet};

#[cfg(feature = "test-support")]
mod registration_handoff_test_support {
    use super::*;
    use std::sync::{Mutex as StdMutex, OnceLock, Weak};

    static ACTIVE_PROBE: OnceLock<StdMutex<Option<Weak<RabbitMqRegistrationHandoffProbe>>>> =
        OnceLock::new();

    /// Broker-accepted registration evidence used only by live qualification.
    #[doc(hidden)]
    pub struct RabbitMqRegistrationHandoffProbe {
        pause_queue: String,
        pause_claimed: AtomicBool,
        accepted: StdMutex<Vec<(String, Channel)>>,
        accepted_notify: tokio::sync::Notify,
        release: CancellationToken,
    }

    impl RabbitMqRegistrationHandoffProbe {
        /// Wait until the broker accepted at least `expected` consumers for a queue.
        pub async fn wait_for_accepted(&self, queue: &str, expected: usize) {
            loop {
                let accepted = self.accepted_notify.notified();
                tokio::pin!(accepted);
                accepted.as_mut().enable();
                if self.accepted_count(queue) >= expected {
                    return;
                }
                accepted.as_mut().await;
            }
        }

        /// Number of Basic.Consume acknowledgements observed for one queue.
        #[must_use]
        pub fn accepted_count(&self, queue: &str) -> usize {
            self.accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|(accepted_queue, _)| accepted_queue == queue)
                .count()
        }

        /// Total dedicated channels retained as test evidence.
        #[must_use]
        pub fn channel_count(&self) -> usize {
            self.accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        }

        /// Evidence channels which still report a connected state.
        #[must_use]
        pub fn connected_channel_count(&self) -> usize {
            self.accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|(_, channel)| channel.status().connected())
                .count()
        }

        /// Release a paused accepted registration when a test needs recovery.
        pub fn release(&self) {
            self.release.cancel();
        }

        fn record(&self, queue: &str, channel: &Channel) {
            self.accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((queue.to_owned(), channel.clone()));
            self.accepted_notify.notify_waiters();
        }
    }

    /// Install one process-local weak probe. It cannot affect production builds.
    #[must_use]
    pub fn install_rabbitmq_registration_handoff_probe(
        pause_queue: impl Into<String>,
    ) -> Arc<RabbitMqRegistrationHandoffProbe> {
        let probe = Arc::new(RabbitMqRegistrationHandoffProbe {
            pause_queue: pause_queue.into(),
            pause_claimed: AtomicBool::new(false),
            accepted: StdMutex::new(Vec::new()),
            accepted_notify: tokio::sync::Notify::new(),
            release: CancellationToken::new(),
        });
        let slot = ACTIVE_PROBE.get_or_init(|| StdMutex::new(None));
        let mut slot = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            slot.as_ref().and_then(Weak::upgrade).is_none(),
            "only one live RabbitMQ registration handoff probe may be installed"
        );
        *slot = Some(Arc::downgrade(&probe));
        probe
    }

    pub(super) async fn accepted(
        queue: &str,
        channel: &Channel,
        cancellation: &CancellationToken,
    ) -> Result<(), MessageBrokerError> {
        let probe = ACTIVE_PROBE
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade);
        let Some(probe) = probe else {
            return Ok(());
        };
        probe.record(queue, channel);
        let should_pause = queue == probe.pause_queue
            && probe
                .pause_claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        if !should_pause {
            return Ok(());
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled))
            }
            () = probe.release.cancelled() => Ok(()),
        }
    }
}

#[cfg(feature = "test-support")]
pub use registration_handoff_test_support::{
    RabbitMqRegistrationHandoffProbe, install_rabbitmq_registration_handoff_probe,
};

#[cfg(feature = "test-support")]
mod post_handler_settlement_test_support {
    use super::*;
    use std::sync::{Mutex as StdMutex, OnceLock, Weak};
    use uuid::Uuid;

    static ACTIVE_PROBE: OnceLock<StdMutex<Option<Weak<RabbitMqPostHandlerSettlementProbe>>>> =
        OnceLock::new();

    /// Exact post-handler/pre-settlement pause used by crash qualification.
    ///
    /// The probe is available only with the non-default `test-support` feature.
    /// It lets a qualification process prove the commit-before-ACK ambiguity
    /// window without adding timing sleeps to production behavior.
    #[doc(hidden)]
    pub struct RabbitMqPostHandlerSettlementProbe {
        event_id: Uuid,
        pause_claimed: AtomicBool,
        entered: AtomicBool,
        entered_notify: tokio::sync::Notify,
        release: CancellationToken,
    }

    impl RabbitMqPostHandlerSettlementProbe {
        /// Wait until the selected handler has returned successfully and no
        /// broker settlement has started yet.
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

        /// Release the paused delivery in qualifications which do not kill the
        /// owning process.
        pub fn release(&self) {
            self.release.cancel();
        }
    }

    /// Installs one process-local weak probe for an exact event identity.
    #[must_use]
    pub fn install_rabbitmq_post_handler_settlement_probe(
        event_id: Uuid,
    ) -> Arc<RabbitMqPostHandlerSettlementProbe> {
        let probe = Arc::new(RabbitMqPostHandlerSettlementProbe {
            event_id,
            pause_claimed: AtomicBool::new(false),
            entered: AtomicBool::new(false),
            entered_notify: tokio::sync::Notify::new(),
            release: CancellationToken::new(),
        });
        let slot = ACTIVE_PROBE.get_or_init(|| StdMutex::new(None));
        let mut slot = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            slot.as_ref().and_then(Weak::upgrade).is_none(),
            "only one live RabbitMQ post-handler settlement probe may be installed"
        );
        *slot = Some(Arc::downgrade(&probe));
        probe
    }

    pub(super) async fn pause(
        event_id: Uuid,
        cancellation: &CancellationToken,
    ) -> Result<(), MessageBrokerError> {
        let probe = ACTIVE_PROBE
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade);
        let Some(probe) = probe else {
            return Ok(());
        };
        let should_pause = event_id == probe.event_id
            && probe
                .pause_claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        if !should_pause {
            return Ok(());
        }
        probe.entered.store(true, Ordering::Release);
        probe.entered_notify.notify_waiters();
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled))
            }
            () = probe.release.cancelled() => Ok(()),
        }
    }
}

#[cfg(feature = "test-support")]
pub use post_handler_settlement_test_support::{
    RabbitMqPostHandlerSettlementProbe, install_rabbitmq_post_handler_settlement_probe,
};

struct SupervisedQueuePair {
    queue: String,
    handle: JoinHandle<Result<(), RabbitMqConsumerTaskFailure>>,
}

/// One retained physical channel generation, independent of receiver/task Drop.
/// A new generation cannot replace a channel whose cleanup is unproven.
#[derive(Default)]
struct ConsumerChannelOwnership {
    channel: std::sync::Mutex<Option<ConsumerChannelResources>>,
    pair: std::sync::Mutex<Option<Arc<QueuePairEvidence>>>,
}

struct ConsumerChannelResources {
    channel: Channel,
    // Retain Lapin's cancel-on-drop guard alongside its physical channel.
    // Receiver abort or an expired close waiter must not initiate unowned I/O.
    consumer: Option<lapin::Consumer>,
}

#[derive(Default)]
struct QueuePairEvidence {
    reconciled: AtomicBool,
    runtime_failure: Option<QueueRuntimeFailureAuthority>,
}

struct QueueRuntimeFailureAuthority {
    admission: CancellationToken,
    execution: DeliveryCancellationSource,
    force: CancellationToken,
}

impl QueueRuntimeFailureAuthority {
    fn notify(&self) {
        self.admission.cancel();
        self.execution
            .cancel(DeliveryCancellationReason::RuntimeFailure);
        self.force.cancel();
    }
}

impl ConsumerChannelOwnership {
    fn adopt(&self, channel: &Channel) {
        let mut owned = self.channel.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            owned.is_none(),
            "consumer channel generation replaced before cleanup proof"
        );
        *owned = Some(ConsumerChannelResources {
            channel: channel.clone(),
            consumer: None,
        });
    }

    fn retain_consumer(&self, consumer: &lapin::Consumer) {
        self.channel
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
            .expect("channel adopted before Basic.Consume")
            .consumer = Some(consumer.clone());
    }

    fn release_if_terminal(&self) -> bool {
        let mut owned = self.channel.lock().unwrap_or_else(|e| e.into_inner());
        if owned
            .as_ref()
            .is_none_or(|owned| dedicated_consumer_channel_cleanup_proven(owned.channel.status()))
        {
            *owned = None;
            true
        } else {
            false
        }
    }

    fn track_pair(&self, evidence: Arc<QueuePairEvidence>) {
        assert!(
            self.children_reconciled(),
            "previous generation has unjoined children"
        );
        *self.pair.lock().unwrap_or_else(|e| e.into_inner()) = Some(evidence);
    }

    fn children_reconciled(&self) -> bool {
        self.pair
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_none_or(|evidence| evidence.reconciled.load(Ordering::Acquire))
    }
}

/// Basic.Cancel stops the subscription while accepted work retains its channel.
/// Closing the channel is a parent action, after the real dispatcher joins.
async fn finish_consumer_generation<P, C, L>(
    pair: P,
    cancel: C,
    close: L,
    evidence: &QueuePairEvidence,
) -> Result<(), RabbitMqConsumerTaskFailure>
where
    P: Future<Output = Result<(), RabbitMqConsumerTaskFailure>>,
    C: Future<Output = bool>,
    L: Future<Output = Result<(), RabbitMqConsumerTaskFailure>>,
{
    let (children, cancel_confirmed) = tokio::join!(pair, cancel);
    if !evidence.reconciled.load(Ordering::Acquire) {
        // Dispatcher Drop cannot prove termination of its nested JoinSet.
        // The engine retains the channel and withholds parent disposal.
        return Err(children.expect_err("unreconciled supervisor cannot report success"));
    }
    if !cancel_confirmed {
        warn!(
            error_code = "CONSUMER_CANCEL_UNCONFIRMED",
            "Subscription cancellation was not confirmed; channel close must prove transport termination"
        );
    }
    let transport = close.await;
    if let Err(failure) = &transport {
        warn!(
            error_code = failure.operation_error_code,
            "Dedicated consumer channel cleanup failed after child drain"
        );
    }
    children.and(transport)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueueRegistrationState {
    Registering,
    Active,
    CleanupUnproven,
}

fn reserve_queue_registration(
    states: &DashMap<(String, String), QueueRegistrationState>,
    key: &(String, String),
) -> Result<(), MessageBrokerError> {
    if states.contains_key(key) {
        return Err(MessageBrokerError::RabbitMQError(
            RabbitMQError::Configuration(format!(
                "duplicate consumer registration for exchange {:?} and queue {:?}",
                key.0, key.1,
            )),
        ));
    }
    states.insert(key.clone(), QueueRegistrationState::Registering);
    Ok(())
}

fn finish_registration_rollback(
    states: &DashMap<(String, String), QueueRegistrationState>,
    key: &(String, String),
    cleanup_unproven: bool,
) {
    if cleanup_unproven {
        states.insert(key.clone(), QueueRegistrationState::CleanupUnproven);
    } else {
        states.remove(key);
    }
}

fn finish_registration_cleanup_result(
    states: &DashMap<(String, String), QueueRegistrationState>,
    key: &(String, String),
    cleanup: &Result<(), MessageBrokerError>,
) {
    finish_registration_rollback(states, key, cleanup.is_err());
}

fn adopt_supervised_queue_task<F>(tasks: &mut Vec<SupervisedQueuePair>, queue: String, task: F)
where
    F: Future<Output = Result<(), RabbitMqConsumerTaskFailure>> + Send + 'static,
{
    // The task cannot touch broker or readiness state until its JoinHandle is
    // already retained by the engine. There is deliberately no await between
    // ledger insertion and releasing this barrier.
    let (adopted_tx, adopted_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        if adopted_rx.await.is_err() {
            return Ok(());
        }
        task.await
    });
    tasks.push(SupervisedQueuePair { queue, handle });
    let _ = adopted_tx.send(());
}

async fn observe_registration_attempt<F, T, E>(
    startup: &mut tokio::sync::oneshot::Sender<Result<(), MessageBrokerError>>,
    cancellation: &CancellationToken,
    attempt: F,
) -> (Result<T, E>, bool)
where
    F: Future<Output = Result<T, E>>,
{
    tokio::pin!(attempt);
    tokio::select! {
        biased;
        result = &mut attempt => (result, false),
        _ = startup.closed() => {
            // Caller cancellation withdraws only its startup observation.
            // The engine-owned task retains the attempt until the attempt's
            // own bounded rollback has completed.
            cancellation.cancel();
            (attempt.await, true)
        }
    }
}

fn publish_registration_readiness(
    startup: tokio::sync::oneshot::Sender<Result<(), MessageBrokerError>>,
    ledger: Arc<DeliveryTerminalLedger>,
) -> Result<ConsumerReadinessGuard, ()> {
    // Publish readiness before waking the startup observer. `oneshot::send`
    // may schedule a receiver immediately on another multi-thread runtime
    // worker, so send-before-readiness would violate `Ok(()) => ready`.
    let mut readiness = ConsumerReadinessGuard::opened(ledger);
    if startup.send(Ok(())).is_err() {
        readiness.rollback_uncommitted();
        return Err(());
    }
    Ok(readiness)
}

struct BackgroundDrainOutcome {
    result: Result<(), MessageBrokerError>,
    reconciled: bool,
}

#[derive(Debug)]
enum ChildTaskExit {
    Completed {
        role: RabbitMqConsumerTaskRole,
        shutdown_requested_at_exit: bool,
    },
    OperationFailed {
        role: RabbitMqConsumerTaskRole,
        error_code: &'static str,
        shutdown_requested_at_exit: bool,
    },
    Panicked(RabbitMqConsumerTaskRole),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportAdmissionFailure {
    PayloadTooLarge,
    MetadataBoundsExceeded,
    InvalidEnvelope,
}

#[cfg(feature = "fuzzing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FuzzTransportAdmissionFailure {
    PayloadTooLarge,
    MetadataBoundsExceeded,
    InvalidEnvelope,
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_transport_admission_failure(
    body_len: usize,
    max_message_size_bytes: usize,
    properties: &lapin::BasicProperties,
) -> Option<FuzzTransportAdmissionFailure> {
    transport_admission_failure(body_len, max_message_size_bytes, properties).map(|failure| {
        match failure {
            TransportAdmissionFailure::PayloadTooLarge => {
                FuzzTransportAdmissionFailure::PayloadTooLarge
            }
            TransportAdmissionFailure::MetadataBoundsExceeded => {
                FuzzTransportAdmissionFailure::MetadataBoundsExceeded
            }
            TransportAdmissionFailure::InvalidEnvelope => {
                FuzzTransportAdmissionFailure::InvalidEnvelope
            }
        }
    })
}

fn transport_admission_failure(
    body_len: usize,
    max_message_size_bytes: usize,
    properties: &lapin::BasicProperties,
) -> Option<TransportAdmissionFailure> {
    if delivery_exceeds_limit(body_len, max_message_size_bytes) {
        return Some(TransportAdmissionFailure::PayloadTooLarge);
    }
    if validate_handoff_metadata_headroom(properties).is_err() {
        return Some(TransportAdmissionFailure::MetadataBoundsExceeded);
    }
    if validate_delivery_envelope(properties).is_err() {
        return Some(TransportAdmissionFailure::InvalidEnvelope);
    }
    None
}

fn oversized_delivery_nack_options() -> BasicNackOptions {
    BasicNackOptions {
        multiple: false,
        requeue: false,
    }
}

fn requeue_delivery_nack_options() -> BasicNackOptions {
    BasicNackOptions {
        multiple: false,
        requeue: true,
    }
}

fn failure_class(error: &QueueHandlerError) -> FailureClass {
    match error.class() {
        QueueHandlerFailureClass::Retryable => FailureClass::Retryable,
        QueueHandlerFailureClass::Permanent => FailureClass::Permanent,
    }
}

fn failure_code(error: &QueueHandlerError) -> &'static str {
    error.code()
}

fn handler_failure_outcome(error: &QueueHandlerError) -> &'static str {
    if failure_code(error) == "QUEUE_HANDLER_PANICKED" {
        "panic"
    } else {
        "error"
    }
}

struct RabbitMqSettlementPort<'a> {
    worker: &'a str,
    queue: &'a str,
    delivery: &'a Delivery,
    body: &'a [u8],
    cancellation: &'a CancellationToken,
    retry_engine: &'a Arc<dyn RetryEngine<Delivery>>,
}

fn lapin_settlement_error(operation: &'static str, error: lapin::Error) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(format!("{operation} failed: {error}")))
}

#[async_trait]
impl SettlementPort for RabbitMqSettlementPort<'_> {
    async fn handoff(&mut self, plan: HandoffPlan) -> Result<HandoffReceipt, MessageBrokerError> {
        let materialized_retry_count =
            canonical_retry_count(self.delivery.properties.headers().as_ref())
                .map_err(invalid_message)?;
        if materialized_retry_count != plan.current_retry_count() {
            return Err(invalid_message(
                "settlement handoff retry count changed after policy materialization",
            ));
        }

        self.retry_engine
            .retry(
                self.worker,
                self.queue,
                self.body,
                self.delivery,
                plan,
                self.cancellation.clone(),
            )
            .instrument(tracing::info_span!("messaging.consume.handoff"))
            .await
            .map(|receipt| match receipt {
                RetryHandoff::RetryConfirmed => HandoffReceipt::RetryConfirmed,
                RetryHandoff::DeadLetterConfirmed => HandoffReceipt::DeadLetterConfirmed,
            })
    }

    async fn ack(&mut self) -> Result<bool, MessageBrokerError> {
        self.delivery
            .ack(BasicAckOptions::default())
            .instrument(tracing::info_span!("messaging.consume.ack"))
            .await
            .map_err(|error| lapin_settlement_error("consumer ACK settlement", error))
    }

    async fn nack_requeue(&mut self) -> Result<bool, MessageBrokerError> {
        self.delivery
            .nack(requeue_delivery_nack_options())
            .instrument(tracing::info_span!("messaging.consume.nack"))
            .await
            .map_err(|error| lapin_settlement_error("consumer requeue NACK settlement", error))
    }

    async fn nack_dead_letter(&mut self) -> Result<bool, MessageBrokerError> {
        self.delivery
            .nack(oversized_delivery_nack_options())
            .instrument(tracing::info_span!("messaging.consume.nack"))
            .await
            .map_err(|error| {
                lapin_settlement_error("consumer broker dead-letter NACK settlement", error)
            })
    }
}

struct DeliverySettlementObserver<'a>(&'a mut DeliveryAttemptGuard);

impl SettlementObserver for DeliverySettlementObserver<'_> {
    fn handoff_confirmed(
        &mut self,
        receipt: HandoffReceipt,
    ) -> Result<(), SettlementObserverFailure> {
        self.0
            .record_confirmed_handoff(receipt == HandoffReceipt::RetryConfirmed);
        tracing::Span::current().record(
            "lily.handoff_outcome",
            match receipt {
                HandoffReceipt::RetryConfirmed => "retry_confirmed",
                HandoffReceipt::DeadLetterConfirmed => "dlq_confirmed",
            },
        );
        Ok(())
    }

    fn terminal(&mut self, terminal: SettlementTerminal) -> Result<(), SettlementObserverFailure> {
        match terminal {
            SettlementTerminal::AckedSuccess => {
                self.0.finish_handler_success_ack();
                tracing::Span::current().record("lily.ack_outcome", "handler_success");
            }
            SettlementTerminal::AckedRetry | SettlementTerminal::AckedDeadLetter => {
                self.0.finish_confirmed_handoff_ack();
                tracing::Span::current().record("lily.ack_outcome", "confirmed_handoff");
            }
            SettlementTerminal::NackRequeue => {
                self.0.finish_nack_requeue();
                tracing::Span::current().record("lily.ack_outcome", "nack_requeue");
            }
            SettlementTerminal::NackDeadLetter => {
                self.0.finish_broker_dead_letter();
                tracing::Span::current().record("lily.ack_outcome", "broker_dead_lettered");
            }
            SettlementTerminal::Unresolved => {
                tracing::Span::current().record("lily.ack_outcome", "unresolved");
                // The attempt guard's Drop is the single authority that records
                // unresolved delivery evidence after this observer returns.
            }
        }
        Ok(())
    }
}

fn settlement_operation_name(operation: Option<SettlementOperation>) -> &'static str {
    match operation {
        Some(SettlementOperation::Handoff) => "handoff",
        Some(SettlementOperation::Ack) => "ack",
        Some(SettlementOperation::NackRequeue) => "requeue nack",
        Some(SettlementOperation::NackDeadLetter) => "dead-letter nack",
        None => "materialization",
    }
}

fn settlement_failure_error(failure: SettlementFailure) -> MessageBrokerError {
    let stable_code = failure.stable_code();
    if let Some(source) = failure.into_source() {
        return source;
    }

    MessageBrokerError::RabbitMQError(RabbitMQError::QueueSettlement(stable_code))
}

fn finish_settlement_report(report: SettlementReport) -> Result<(), MessageBrokerError> {
    if let Some(secondary) = report.secondary_failure() {
        warn!(
            error_code = secondary.stable_code(),
            operation = settlement_operation_name(secondary.operation()),
            "Secondary queue settlement operation failed"
        );
    }
    if report.observer_failure().is_some() {
        warn!(
            error_code = "QUEUE_SETTLEMENT_OBSERVER_FAILED",
            "Queue settlement observation failed without changing the delivery outcome"
        );
    }
    report
        .into_failure()
        .map_or(Ok(()), |failure| Err(settlement_failure_error(failure)))
}

#[cfg(test)]
mod failure_policy_tests {
    use super::*;
    use lapin::protocol::AMQPError;

    fn setting() -> QueueRuntimeSetting {
        QueueRuntimeSetting {
            exchange_name: "events".into(),
            routing_key: "events.created".into(),
            exchange_kind: RabbitMqExchangeKind::Direct,
            queue_type: RabbitMqQueueType::Classic,
            topology_ownership: RabbitMqTopologyOwnership::FrameworkManaged,
            concurrency: 4,
            prefetch_count: 20,
            delivery_buffer_capacity: 12,
            max_message_size_bytes: 1024,
            retention: crate::setting::QueueRetentionSetting {
                main_max_messages: 100,
                main_max_bytes: 1024 * 1024,
                retry_bucket_max_messages: 100,
                retry_bucket_max_bytes: 1024 * 1024,
                dead_letter_max_messages: 100,
                dead_letter_max_bytes: 1024 * 1024,
            },
            retry_attempts: 0,
            retry_backoff: Duration::from_millis(100),
            max_retry_backoff: Duration::from_secs(1),
            delivery_execution_timeout: Duration::from_secs(2),
            settlement_timeout: Duration::from_secs(5),
            durable: true,
            dead_letter_exchange: None,
            dead_letter_routing_key: None,
            message_ttl_ms: None,
            exclusive: false,
            auto_delete: false,
            single_active_consumer: false,
            max_priority: None,
        }
    }

    fn topology(queue_type: RabbitMqQueueType) -> QueueTopology {
        QueueTopology {
            main_exchange: "events".into(),
            main_exchange_kind: RabbitMqExchangeKind::Direct,
            main_queue: "events.created".into(),
            routing_key: "events.created".into(),
            queue_type,
            ownership: RabbitMqTopologyOwnership::FrameworkManaged,
            single_active_consumer: false,
            max_priority: None,
            retry_exchange: "events.retry.v2".into(),
            retry_buckets: Vec::new(),
            retry_bucket_delays_by_attempt: Vec::new(),
            dead_letter_exchange: "events.dlx.v2".into(),
            dead_letter_queue: "events.created.dlq.v2".into(),
            dead_letter_routing_key: "events.created".into(),
        }
    }

    #[test]
    fn validation_failures_are_permanent_and_runtime_failures_retry() {
        assert_eq!(
            FailureClass::Permanent,
            failure_class(&QueueHandlerError::permanent("INVALID"))
        );
        assert_eq!(
            FailureClass::Retryable,
            failure_class(&QueueHandlerError::retryable("DEPENDENCY"))
        );
        assert_eq!(
            "PAYLOAD_TOO_LARGE",
            failure_code(&QueueHandlerError::permanent("PAYLOAD_TOO_LARGE"))
        );
        assert_eq!(
            "PRIVATE-PAYLOAD-SENTINEL",
            failure_code(&QueueHandlerError::permanent("PRIVATE-PAYLOAD-SENTINEL"))
        );
        assert_eq!(
            "panic",
            handler_failure_outcome(&QueueHandlerError::retryable("QUEUE_HANDLER_PANICKED"))
        );
        assert_eq!(
            "error",
            handler_failure_outcome(&QueueHandlerError::retryable(
                "QUEUE_DELIVERY_EXECUTION_TIMED_OUT"
            ))
        );
    }

    #[test]
    fn concurrency_prefetch_and_payload_admission_are_independent() {
        let setting = setting();
        let dispatch = DispatchPolicy::from_setting(&setting, Duration::from_secs(3));

        assert_eq!(dispatch.handler_concurrency, 4);
        assert_eq!(rabbitmq_prefetch(&setting), 20);
        assert_eq!(setting.delivery_buffer_capacity, 12);
        assert!(!delivery_exceeds_limit(
            1024,
            setting.max_message_size_bytes
        ));
        assert!(delivery_exceeds_limit(1025, setting.max_message_size_bytes));
    }

    #[test]
    fn source_less_settlement_failures_preserve_their_stable_runtime_codes() {
        for kind in [
            SettlementFailureKind::AdapterError,
            SettlementFailureKind::Declined,
            SettlementFailureKind::Panicked,
            SettlementFailureKind::TimedOut,
            SettlementFailureKind::Cancelled,
            SettlementFailureKind::ReceiptMismatch,
            SettlementFailureKind::AuthorityConsumed,
        ] {
            let expected = kind.stable_code();
            let error = settlement_failure_error(SettlementFailure::classified_for_test(
                kind,
                Some(SettlementOperation::Ack),
            ));

            assert_eq!(error.error_code(), expected);
            assert!(matches!(
                error,
                MessageBrokerError::RabbitMQError(RabbitMQError::QueueSettlement(code))
                    if code == expected
            ));
        }
    }

    #[test]
    fn topology_ownership_selects_exactly_one_non_destructive_execution_mode() {
        assert_eq!(
            topology_execution_mode(RabbitMqTopologyOwnership::FrameworkManaged),
            TopologyExecutionMode::ActiveDeclare
        );
        assert_eq!(
            topology_execution_mode(RabbitMqTopologyOwnership::External),
            TopologyExecutionMode::PassiveVerify
        );
    }

    #[test]
    fn managed_queue_arguments_are_typed_and_system_queue_type_is_inherited() {
        let mut classic = topology(RabbitMqQueueType::Classic);
        classic.single_active_consumer = true;
        classic.max_priority = Some(16);
        let mut main_arguments = FieldTable::default();
        insert_main_queue_profile_arguments(&mut main_arguments, &classic);
        assert!(matches!(
            main_arguments.inner().get("x-queue-type"),
            Some(AMQPValue::LongString(value)) if value.as_bytes() == b"classic"
        ));
        assert!(matches!(
            main_arguments.inner().get("x-single-active-consumer"),
            Some(AMQPValue::Boolean(true))
        ));
        assert!(matches!(
            main_arguments.inner().get("x-max-priority"),
            Some(AMQPValue::LongInt(16))
        ));

        let quorum = topology(RabbitMqQueueType::Quorum);
        let mut system_arguments = FieldTable::default();
        insert_queue_type_argument(&mut system_arguments, quorum.queue_type);
        assert!(matches!(
            system_arguments.inner().get("x-queue-type"),
            Some(AMQPValue::LongString(value)) if value.as_bytes() == b"quorum"
        ));
        assert!(!system_arguments.inner().contains_key("x-max-priority"));
        assert!(
            !system_arguments
                .inner()
                .contains_key("x-single-active-consumer")
        );

        let system_options = managed_system_queue_declare_options();
        assert!(system_options.durable);
        assert!(!system_options.exclusive);
        assert!(!system_options.auto_delete);
        assert!(!system_options.passive);
    }

    #[test]
    fn topology_protocol_replies_map_to_secret_safe_typed_errors() {
        for (soft_error, operation, expected_kind) in [
            (
                AMQPSoftError::ACCESSREFUSED,
                RabbitMqTopologyOperation::Declare,
                RabbitMqTopologyErrorKind::PermissionDenied,
            ),
            (
                AMQPSoftError::NOTFOUND,
                RabbitMqTopologyOperation::Verify,
                RabbitMqTopologyErrorKind::PassiveResourceNotFound,
            ),
            (
                AMQPSoftError::PRECONDITIONFAILED,
                RabbitMqTopologyOperation::Declare,
                RabbitMqTopologyErrorKind::DeclarationMismatch,
            ),
        ] {
            let provider_error = lapin::ErrorKind::ProtocolError(AMQPError::new(
                AMQPErrorKind::Soft(soft_error),
                "RAW_BROKER_REPLY_MUST_NOT_ESCAPE".into(),
            ))
            .into();
            let mapped = topology_error(
                provider_error,
                operation,
                RabbitMqTopologyResourceKind::Queue,
                "orders.created",
            );

            assert!(matches!(
                &mapped,
                MessageBrokerError::RabbitMQError(RabbitMQError::Topology(
                    RabbitMqTopologyError {
                        kind,
                        operation: mapped_operation,
                        resource_kind: RabbitMqTopologyResourceKind::Queue,
                        resource_name,
                    }
                )) if *kind == expected_kind
                    && *mapped_operation == operation
                    && resource_name == "orders.created"
            ));
            assert!(
                !mapped
                    .to_string()
                    .contains("RAW_BROKER_REPLY_MUST_NOT_ESCAPE")
            );
        }
    }
}

struct AmqpHeaderExtractor<'a>(&'a FieldTable);

struct BufferedDelivery {
    delivery: Delivery,
    body: Vec<u8>,
    enqueued_at: std::time::Instant,
    attempt_guard: DeliveryAttemptGuard,
    settlement_authority: SettlementAuthority,
}

#[derive(Clone, Copy)]
struct DispatchPolicy {
    handler_concurrency: usize,
    retry_attempts: u32,
    delivery_execution_timeout: Duration,
    handoff_timeout: Duration,
    settlement_timeout: Duration,
}

impl DispatchPolicy {
    fn from_setting(setting: &QueueRuntimeSetting, handoff_timeout: Duration) -> Self {
        Self {
            handler_concurrency: setting.concurrency,
            retry_attempts: setting.retry_attempts,
            delivery_execution_timeout: setting.delivery_execution_timeout,
            handoff_timeout,
            settlement_timeout: setting.settlement_timeout,
        }
    }
}

fn delivery_exceeds_limit(body_len: usize, max_message_size_bytes: usize) -> bool {
    body_len > max_message_size_bytes
}

fn rabbitmq_prefetch(setting: &QueueRuntimeSetting) -> u16 {
    setting.prefetch_count
}

async fn drain_background_tasks(
    background_tasks: &Mutex<Vec<SupervisedQueuePair>>,
) -> BackgroundDrainOutcome {
    // Keep handles in the engine while awaiting them. If this future is
    // cancelled, the mutex guard returns the same handles to the next
    // lifecycle attempt instead of detaching them.
    let mut tasks = background_tasks.lock().await;
    let mut first_failure = None;
    let mut reconciled = true;
    while !tasks.is_empty() {
        let join_result = (&mut tasks[0].handle).await;
        let task = tasks.swap_remove(0);
        match join_result {
            Ok(Ok(())) => {}
            Ok(Err(failure)) => {
                if matches!(
                    failure.role,
                    RabbitMqConsumerTaskRole::Dispatcher | RabbitMqConsumerTaskRole::Supervisor
                ) && matches!(
                    failure.kind,
                    RabbitMqConsumerTaskFailureKind::Panicked
                        | RabbitMqConsumerTaskFailureKind::Cancelled
                ) {
                    reconciled = false;
                }
                first_failure.get_or_insert(failure);
            }
            Err(error) => {
                reconciled = false;
                first_failure.get_or_insert(RabbitMqConsumerTaskFailure {
                    queue: task.queue,
                    role: RabbitMqConsumerTaskRole::Supervisor,
                    kind: if error.is_panic() {
                        RabbitMqConsumerTaskFailureKind::Panicked
                    } else {
                        RabbitMqConsumerTaskFailureKind::Cancelled
                    },
                    operation_error_code: None,
                });
            }
        }
    }
    let result = match first_failure {
        Some(failure) => Err(MessageBrokerError::RabbitMQError(
            RabbitMQError::ConsumerTaskFailed(failure),
        )),
        None => Ok(()),
    };
    BackgroundDrainOutcome { result, reconciled }
}

fn child_role_for_id(id: Id, receiver_id: Id, dispatcher_id: Id) -> RabbitMqConsumerTaskRole {
    if id == receiver_id {
        RabbitMqConsumerTaskRole::Receiver
    } else {
        debug_assert_eq!(id, dispatcher_id);
        RabbitMqConsumerTaskRole::Dispatcher
    }
}

fn child_failure(queue: &str, exit: ChildTaskExit) -> Option<RabbitMqConsumerTaskFailure> {
    match exit {
        ChildTaskExit::Completed {
            shutdown_requested_at_exit: true,
            ..
        } => None,
        ChildTaskExit::OperationFailed {
            error_code,
            shutdown_requested_at_exit: true,
            ..
        } if error_code == "BROKER_CANCELLED"
            || error_code == SettlementFailureKind::Cancelled.stable_code() =>
        {
            None
        }
        ChildTaskExit::Completed { role, .. } => Some(RabbitMqConsumerTaskFailure {
            queue: queue.to_owned(),
            role,
            kind: RabbitMqConsumerTaskFailureKind::UnexpectedExit,
            operation_error_code: None,
        }),
        ChildTaskExit::OperationFailed {
            role, error_code, ..
        } => Some(RabbitMqConsumerTaskFailure {
            queue: queue.to_owned(),
            role,
            kind: RabbitMqConsumerTaskFailureKind::OperationFailed,
            operation_error_code: Some(error_code),
        }),
        ChildTaskExit::Panicked(role) => Some(RabbitMqConsumerTaskFailure {
            queue: queue.to_owned(),
            role,
            kind: RabbitMqConsumerTaskFailureKind::Panicked,
            operation_error_code: None,
        }),
    }
}

async fn supervise_queue_pair<R, D>(
    queue: String,
    admission: CancellationToken,
    force: CancellationToken,
    execution: DeliveryCancellationSource,
    terminal_ledger: Arc<DeliveryTerminalLedger>,
    receiver: R,
    dispatcher: D,
    evidence: Arc<QueuePairEvidence>,
) -> Result<(), RabbitMqConsumerTaskFailure>
where
    R: Future<Output = Result<(), &'static str>> + Send + 'static,
    D: Future<Output = Result<(), &'static str>> + Send + 'static,
{
    let mut tasks = JoinSet::new();
    let receiver_admission = admission.clone();
    let receiver_handle = tasks.spawn(async move {
        let result = AssertUnwindSafe(receiver).catch_unwind().await;
        let shutdown_requested_at_exit = receiver_admission.is_cancelled();
        match result {
            Ok(Ok(())) => ChildTaskExit::Completed {
                role: RabbitMqConsumerTaskRole::Receiver,
                shutdown_requested_at_exit,
            },
            Ok(Err(error_code)) => ChildTaskExit::OperationFailed {
                role: RabbitMqConsumerTaskRole::Receiver,
                error_code,
                shutdown_requested_at_exit,
            },
            Err(_) => ChildTaskExit::Panicked(RabbitMqConsumerTaskRole::Receiver),
        }
    });
    let dispatcher_admission = admission.clone();
    let dispatcher_handle = tasks.spawn(async move {
        let result = AssertUnwindSafe(dispatcher).catch_unwind().await;
        let shutdown_requested_at_exit = dispatcher_admission.is_cancelled();
        match result {
            Ok(Ok(())) => ChildTaskExit::Completed {
                role: RabbitMqConsumerTaskRole::Dispatcher,
                shutdown_requested_at_exit,
            },
            Ok(Err(error_code)) => ChildTaskExit::OperationFailed {
                role: RabbitMqConsumerTaskRole::Dispatcher,
                error_code,
                shutdown_requested_at_exit,
            },
            Err(_) => ChildTaskExit::Panicked(RabbitMqConsumerTaskRole::Dispatcher),
        }
    });
    let receiver_id = receiver_handle.id();
    let dispatcher_id = dispatcher_handle.id();
    let mut failure = None;
    let mut admission_observed = false;
    let mut force_observed = false;
    let mut dispatcher_returned = false;

    while !tasks.is_empty() {
        let join_result = if force_observed {
            tasks
                .join_next_with_id()
                .await
                .expect("non-empty supervised task set must yield")
        } else {
            tokio::select! {
                biased;
                _ = force.cancelled() => {
                    force_observed = true;
                    admission.cancel();
                    execution.cancel(DeliveryCancellationReason::RuntimeCancellation);
                    // The dispatcher observes `force` itself, aborts and joins
                    // every nested handler/settlement task. Only the receiver
                    // is aborted here so the supervisor never drops the
                    // dispatcher's JoinSet before that reconciliation ends.
                    receiver_handle.abort();
                    continue;
                }
                _ = admission.cancelled(), if !admission_observed => {
                    admission_observed = true;
                    if terminal_ledger.is_failed() {
                        execution.cancel(DeliveryCancellationReason::RuntimeFailure);
                        force.cancel();
                    }
                    continue;
                }
                result = tasks.join_next_with_id() => {
                    result.expect("non-empty supervised task set must yield")
                }
            }
        };
        let shutdown_requested = admission.is_cancelled();
        let candidate = match join_result {
            Ok((id, exit)) => {
                if id == dispatcher_id && !matches!(&exit, ChildTaskExit::Panicked(_)) {
                    dispatcher_returned = true;
                }
                child_failure(&queue, exit)
            }
            Err(error) if error.is_cancelled() && force_observed && error.id() == receiver_id => {
                None
            }
            Err(error)
                if error.is_cancelled()
                    && shutdown_requested
                    && terminal_ledger.is_failed()
                    && error.id() == receiver_id =>
            {
                None
            }
            Err(error) => Some(RabbitMqConsumerTaskFailure {
                queue: queue.clone(),
                role: child_role_for_id(error.id(), receiver_id, dispatcher_id),
                kind: if error.is_panic() {
                    RabbitMqConsumerTaskFailureKind::Panicked
                } else {
                    RabbitMqConsumerTaskFailureKind::Cancelled
                },
                operation_error_code: None,
            }),
        };
        if failure.is_none()
            && let Some(candidate) = candidate
        {
            terminal_ledger.record_operational_failure(
                candidate
                    .operation_error_code
                    .unwrap_or("BROKER_CONSUMER_TASK_FAILED"),
            );
            terminal_ledger.failed();
            admission.cancel();
            execution.cancel(DeliveryCancellationReason::RuntimeFailure);
            force.cancel();
            if let Some(runtime) = &evidence.runtime_failure {
                runtime.notify();
            }
            failure = Some(candidate);
        }
    }

    evidence
        .reconciled
        .store(dispatcher_returned, Ordering::Release);
    match failure {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
}

impl Extractor for AmqpHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        match self.0.inner().get(key)? {
            AMQPValue::LongString(value) => std::str::from_utf8(value.as_bytes()).ok(),
            AMQPValue::ShortString(value) => Some(value.as_str()),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        self.0.inner().keys().map(|key| key.as_str()).collect()
    }
}

/// RabbitMQ consumer engine assembled internally by [`crate::QueueService`].
pub(crate) struct RabbitMQQueueEngine {
    options: MessageBrokerSetting,
    channel_manager: Arc<dyn ChannelManager<Channel>>,
    retry_engine: Arc<dyn RetryEngine<Delivery>>,

    buffers: Arc<DashMap<(String, String), tokio::sync::mpsc::Sender<BufferedDelivery>>>,
    registration_gate: Arc<Mutex<()>>,
    registration_states: Arc<DashMap<(String, String), QueueRegistrationState>>,
    registration_sealed: Arc<AtomicBool>,

    background_tasks: Arc<Mutex<Vec<SupervisedQueuePair>>>,
    channel_owners: Arc<DashMap<(String, String), Arc<ConsumerChannelOwnership>>>,
    drain_gate: Arc<Mutex<()>>,
    drain_terminal: Arc<Mutex<Option<Result<(), MessageBrokerError>>>>,
    drain_reconciled: AtomicBool,

    runtime_tokens: Arc<Mutex<Option<QueueRuntimeTokens>>>,
    execution: DeliveryCancellationSource,
    cleanup: CancellationToken,
    shutdown_budget: QueueShutdownBudget,
    terminal_ledger: Arc<DeliveryTerminalLedger>,
}

#[derive(Clone)]
struct QueueRuntimeTokens {
    admission: CancellationToken,
    settlement: CancellationToken,
    force: CancellationToken,
}

impl RabbitMQQueueEngine {
    async fn release_buffered_deliveries(rx: &mut tokio::sync::mpsc::Receiver<BufferedDelivery>) {
        rx.close();
        while let Some(mut accepted) = rx.recv().await {
            accepted.attempt_guard.finish_buffered_pending_redelivery();
        }
    }

    fn reconcile_in_flight_result(
        join_result: Result<Result<(), &'static str>, tokio::task::JoinError>,
    ) -> Result<(), &'static str> {
        match join_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error_code)) => Err(error_code),
            Err(error) => {
                error!(
                    lily.error_code = "BROKER_CONSUMER_TASK_FAILED",
                    task_outcome = if error.is_panic() {
                        "panicked"
                    } else {
                        "cancelled"
                    },
                    "In-flight task join error"
                );
                Err("BROKER_CONSUMER_TASK_FAILED")
            }
        }
    }

    fn retain_first_in_flight_failure(
        first_failure: &mut Option<&'static str>,
        join_result: Result<Result<(), &'static str>, tokio::task::JoinError>,
        aborted: bool,
    ) {
        if aborted && matches!(&join_result, Err(error) if error.is_cancelled()) {
            return;
        }
        if let Err(error_code) = Self::reconcile_in_flight_result(join_result) {
            first_failure.get_or_insert(error_code);
        }
    }

    async fn finish_in_flight_tasks(
        in_flight_tasks: &mut JoinSet<Result<(), &'static str>>,
        mut stopping: bool,
        mut first_failure: Option<&'static str>,
        force: Option<&CancellationToken>,
        execution: Option<&DeliveryCancellationSource>,
        root: Option<&QueueShutdownBudget>,
        settlement: Option<&CancellationToken>,
    ) -> Result<(), &'static str> {
        let budget = root.cloned().unwrap_or_default();
        let mut aborted = false;
        if stopping {
            if let Some(execution) = execution {
                execution.cancel(if first_failure.is_some() {
                    DeliveryCancellationReason::RuntimeFailure
                } else {
                    DeliveryCancellationReason::RuntimeCancellation
                });
            }
        }
        while !in_flight_tasks.is_empty() {
            let revision = budget.revision();
            let deadline = if stopping && !aborted {
                Some(
                    execution
                        .and_then(DeliveryCancellationSource::requested_at)
                        .map_or_else(tokio::time::Instant::now, |at| {
                            budget.forced_delivery_deadline(at)
                        }),
                )
            } else {
                None
            };
            if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                if let Some(settlement) = settlement {
                    settlement.cancel();
                }
                in_flight_tasks.abort_all();
                aborted = true;
                continue;
            }
            let join_result = tokio::select! {
                biased;
                result = in_flight_tasks.join_next() => result,
                _ = async { match force { Some(force) => force.cancelled().await, None => std::future::pending().await } }, if !stopping => {
                    stopping = true;
                    if let Some(execution) = execution { execution.cancel(DeliveryCancellationReason::RuntimeCancellation); }
                    continue;
                },
                _ = tokio::time::sleep_until(deadline.unwrap_or_else(tokio::time::Instant::now)), if deadline.is_some() => continue,
                _ = budget.changed_since(revision) => continue,
            };
            let Some(join_result) = join_result else {
                break;
            };
            Self::retain_first_in_flight_failure(&mut first_failure, join_result, aborted);
            if first_failure.is_some() && !stopping {
                stopping = true;
                if let Some(execution) = execution {
                    execution.cancel(DeliveryCancellationReason::RuntimeFailure);
                }
            }
        }
        first_failure.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    async fn abort_and_join_in_flight_tasks(
        in_flight_tasks: &mut JoinSet<Result<(), &'static str>>,
    ) -> Result<(), &'static str> {
        Self::finish_in_flight_tasks(in_flight_tasks, true, None, None, None, None, None).await
    }

    pub(crate) fn new(
        options: MessageBrokerSetting,
        channel_manager: Arc<dyn ChannelManager<Channel>>,
        retry_engine: Arc<dyn RetryEngine<Delivery>>,
    ) -> Self {
        Self {
            options,
            channel_manager,
            retry_engine,
            buffers: Arc::new(DashMap::new()),
            registration_gate: Arc::new(Mutex::new(())),
            registration_states: Arc::new(DashMap::new()),
            registration_sealed: Arc::new(AtomicBool::new(false)),
            background_tasks: Arc::new(Mutex::new(Vec::new())),
            channel_owners: Arc::new(DashMap::new()),
            drain_gate: Arc::new(Mutex::new(())),
            drain_terminal: Arc::new(Mutex::new(None)),
            drain_reconciled: AtomicBool::new(false),
            runtime_tokens: Arc::new(Mutex::new(None)),
            execution: DeliveryCancellationSource::new(),
            cleanup: CancellationToken::new(),
            shutdown_budget: QueueShutdownBudget::default(),
            terminal_ledger: Arc::new(DeliveryTerminalLedger::default()),
        }
    }

    /// Sampling-independent delivery settlement snapshot for qualification.
    pub(crate) fn delivery_terminal_snapshot(&self) -> DeliveryTerminalSnapshot {
        self.terminal_ledger.snapshot()
    }

    async fn drain_to_terminal(&self) -> Result<(), MessageBrokerError> {
        // Serialize all graceful observers without taking task ownership out
        // of the engine. The guard and handles survive caller cancellation;
        // completed callers replay the same immutable terminal result.
        let _gate = self.drain_gate.lock().await;
        let mut terminal = self.drain_terminal.lock().await;
        if let Some(result) = terminal.clone() {
            return result;
        }
        let outcome = drain_background_tasks(&self.background_tasks).await;
        let reconciled = outcome.reconciled
            && self
                .channel_owners
                .iter()
                .all(|entry| entry.value().children_reconciled());
        self.drain_reconciled.store(reconciled, Ordering::Release);
        if !reconciled {
            self.terminal_ledger.failed();
        }
        let result = outcome.result;
        *terminal = Some(result.clone());
        result
    }
    #[allow(clippy::too_many_arguments)]
    async fn receive_generation(
        mut consumer: lapin::Consumer,
        receiver_worker: String,
        receiver_queue: String,
        receiver_setting: QueueRuntimeSetting,
        tx_clone: tokio::sync::mpsc::Sender<BufferedDelivery>,
        admission_clone: CancellationToken,
        settlement_clone: CancellationToken,
        receiver_retry_engine: Arc<dyn RetryEngine<Delivery>>,
        receiver_terminal_ledger: Arc<DeliveryTerminalLedger>,
        receiver_handoff_timeout: Duration,
        shutdown_budget: QueueShutdownBudget,
    ) -> Result<bool, &'static str> {
        let lost = loop {
            tokio::select! {
                biased;
                _ = admission_clone.cancelled() => break false,

                delivery_result = consumer.next() => {
                    match delivery_result {
                        Some(Ok(mut delivery)) => {
                            if admission_clone.is_cancelled() { break false; }
                            if delivery.redelivered {
                                receiver_terminal_ledger.record_redelivery();
                            }
                            match transport_admission_failure(
                                delivery.data.len(),
                                receiver_setting.max_message_size_bytes,
                                &delivery.properties,
                            ) {
                                Some(TransportAdmissionFailure::PayloadTooLarge) => {
                                    receiver_terminal_ledger
                                        .record_handler_outcome("payload_too_large");
                                    receiver_terminal_ledger.record_poison();
                                    let mut attempt_guard = DeliveryAttemptGuard::begin(
                                        Arc::clone(&receiver_terminal_ledger),
                                        None,
                                        1,
                                    );
                                    let settlement_authority = SettlementAuthority::new();
                                    if let Err(error) = RabbitMQQueueEngine::materialize_broker_dead_letter(
                                        &delivery,
                                        &settlement_clone,
                                        &settlement_authority,
                                        &mut attempt_guard,
                                        receiver_setting.settlement_timeout,
                                     &shutdown_budget,)
                                    .await
                                    {
                                        let error_code = error.error_code();
                                        warn!(
                                            error_code,
                                            "Oversize RabbitMQ delivery broker dead-letter settlement failed"
                                        );
                                        return Err(error_code);
                                    }
                                    continue;
                                }
                                Some(TransportAdmissionFailure::MetadataBoundsExceeded) => {
                                    receiver_terminal_ledger.record_handler_outcome(
                                        "amqp_metadata_bounds_exceeded",
                                    );
                                    receiver_terminal_ledger.record_poison();
                                    let mut attempt_guard = DeliveryAttemptGuard::begin(
                                        Arc::clone(&receiver_terminal_ledger),
                                        canonical_event_id(&delivery.properties),
                                        canonical_delivery_attempt(&delivery.properties),
                                    );
                                    // Rebuild only the validated, bounded Lily identity.
                                    // Never clone or retain the oversized remote table.
                                    delivery.properties =
                                        sanitized_handoff_properties(&delivery.properties);
                                    let settlement_authority = SettlementAuthority::new();
                                    if let Err(error) = RabbitMQQueueEngine::materialize_outcome(
                                        &receiver_worker,
                                        &receiver_queue,
                                        &delivery,
                                        &delivery.data,
                                        &settlement_clone,
                                        &receiver_retry_engine,
                                        &settlement_authority,
                                        &mut attempt_guard,
                                        ExecutionOutcome::Failure {
                                            class: FailureClass::Permanent,
                                            code: "amqp_metadata_bounds_exceeded",
                                        },
                                        receiver_setting.retry_attempts,
                                        receiver_handoff_timeout,
                                        receiver_setting.settlement_timeout,
                                     &shutdown_budget,)
                                    .await
                                    {
                                        let error_code = error.error_code();
                                        warn!(
                                            error_code,
                                            "Invalid RabbitMQ metadata settlement failed"
                                        );
                                        return Err(error_code);
                                    }
                                    continue;
                                }
                                Some(TransportAdmissionFailure::InvalidEnvelope) => {
                                    receiver_terminal_ledger
                                        .record_handler_outcome("invalid_envelope");
                                    receiver_terminal_ledger.record_poison();
                                    let mut attempt_guard = DeliveryAttemptGuard::begin(
                                        Arc::clone(&receiver_terminal_ledger),
                                        canonical_event_id(&delivery.properties),
                                        canonical_delivery_attempt(&delivery.properties),
                                    );
                                    // Malformed framework-owned retry metadata must not be
                                    // replayed into the permanent handoff. Preserve only a
                                    // canonical bounded envelope and retry count.
                                    delivery.properties =
                                        sanitized_handoff_properties(&delivery.properties);
                                    let settlement_authority = SettlementAuthority::new();
                                    if let Err(error) = RabbitMQQueueEngine::materialize_outcome(
                                        &receiver_worker,
                                        &receiver_queue,
                                        &delivery,
                                        &delivery.data,
                                        &settlement_clone,
                                        &receiver_retry_engine,
                                        &settlement_authority,
                                        &mut attempt_guard,
                                        ExecutionOutcome::Failure {
                                            class: FailureClass::Permanent,
                                            code: "invalid_transport_envelope",
                                        },
                                        receiver_setting.retry_attempts,
                                        receiver_handoff_timeout,
                                        receiver_setting.settlement_timeout,
                                     &shutdown_budget,)
                                    .await
                                    {
                                        let error_code = error.error_code();
                                        warn!(
                                            error_code,
                                            "Invalid RabbitMQ envelope settlement failed"
                                        );
                                        return Err(error_code);
                                    }
                                    continue;
                                }
                                None => {}
                            }
                            let keep_receiving = RabbitMQQueueEngine::materialize_and_buffer_delivery(
                                &receiver_worker,
                                &receiver_queue,
                                delivery,
                                &tx_clone,
                                &settlement_clone,
                                &receiver_retry_engine,
                                &receiver_terminal_ledger,
                                receiver_setting.retry_attempts,
                                receiver_handoff_timeout,
                                receiver_setting.settlement_timeout,
                             &shutdown_budget,)
                            .await
                            .map_err(|error| {
                                let error_code = error.error_code();
                                warn!(
                                    error_code,
                                    "Invalid RabbitMQ transport identity settlement failed"
                                );
                                error_code
                            })?;
                            if !keep_receiving {
                                break false;
                            }
                        }
                        Some(Err(_)) => {
                            receiver_terminal_ledger
                                .record_operational_failure("BROKER_TRANSPORT");
                            warn!(
                                error_code = "BROKER_CONSUMER_STREAM_FAILED",
                                "RabbitMQ consumer stream failed; recovery is required"
                            );
                            break true;
                        }
                        None => {
                            receiver_terminal_ledger
                                .record_operational_failure("BROKER_TRANSPORT");
                            break true;
                        }
                    }
                }
            }
        };
        Ok(lost)
    }
}

#[async_trait]
impl QueueEngine for RabbitMQQueueEngine {
    fn set_shutdown_deadlines(&self, deadlines: QueueShutdownDeadlines) {
        self.shutdown_budget.install(deadlines);
    }

    fn cancel_execution(&self, reason: DeliveryCancellationReason) {
        self.execution.cancel(reason);
    }

    fn finish_cleanup(&self) {
        self.cleanup.cancel();
        // Called only after the parent connection has proven closed.
        self.channel_owners.clear();
    }

    #[lily_trace::prelude::instrument(name = "rabbitmq.queue.start", skip(self, ct))]
    /// Retain the runtime cancellation token before registering queues.
    async fn start(&self, ct: CancellationToken) -> Result<(), MessageBrokerError> {
        self.registration_sealed.store(false, Ordering::Release);
        let mut runtime_tokens = self.runtime_tokens.lock().await;
        let report = match execute_topology_plan_bounded(
            &self.channel_manager,
            self.options.topology_plan(),
            &ct,
            self.options.confirm_timeout,
        )
        .await
        {
            Ok(report) => report,
            Err(error) => {
                self.terminal_ledger
                    .record_operational_failure(error.error_code());
                return Err(error);
            }
        };
        if report.external_bindings_unverified() > 0 {
            warn!(
                topology_ownership = "external",
                binding_verified = false,
                binding_count = report.external_bindings_unverified(),
                "RabbitMQ passive verification proved resource existence only; bindings remain operator-owned and unverified"
            );
        }
        self.terminal_ledger
            .configure_expected_consumers(self.options.topology_plan().len());
        *runtime_tokens = Some(QueueRuntimeTokens {
            admission: ct.child_token(),
            settlement: CancellationToken::new(),
            force: ct.child_token(),
        });
        Ok(())
    }

    async fn stop_admission(&self) -> Result<(), MessageBrokerError> {
        self.terminal_ledger.begin_draining();
        // Seal first so a waiter which acquires the registration gate after
        // this point cannot publish another broker consumer or supervisor.
        self.registration_sealed.store(true, Ordering::Release);
        let runtime_tokens = {
            let runtime_tokens = self.runtime_tokens.lock().await;
            runtime_tokens.clone()
        };

        if let Some(runtime_tokens) = runtime_tokens {
            runtime_tokens.admission.cancel();
        }

        // Registration owns this gate until its bounded open/rollback has
        // reached a terminal state. Crossing the gate is therefore the
        // shutdown quiescence proof: no later Basic.Consume can be admitted.
        let registration_barrier = self.registration_gate.lock().await;

        // Tüm producer sender'larını kapat
        self.buffers.clear();
        drop(registration_barrier);

        Ok(())
    }

    async fn drain(&self) -> Result<(), MessageBrokerError> {
        // The application-wide shutdown coordinator is the sole deadline
        // authority. If it cancels this future, task handles remain retained
        // for `force_drain` and its bounded force reserve.
        self.stop_admission().await?;
        let result = self.drain_to_terminal().await;
        if let Some(runtime_tokens) = self.runtime_tokens.lock().await.take() {
            runtime_tokens.admission.cancel();
            runtime_tokens.settlement.cancel();
            runtime_tokens.force.cancel();
        }
        self.terminal_ledger.stopped();
        result
    }

    async fn force_drain(&self) -> Result<(), MessageBrokerError> {
        // A lifecycle adapter normally publishes the precise cause first.
        // Direct provider-local force callers still notify before aborting.
        let reason = if self
            .shutdown_budget
            .deadlines()
            .is_some_and(|deadlines| tokio::time::Instant::now() >= deadlines.graceful())
        {
            DeliveryCancellationReason::ShutdownDeadline
        } else {
            DeliveryCancellationReason::ForcedShutdown
        };
        self.execution.cancel(reason);
        self.registration_sealed.store(true, Ordering::Release);
        let runtime_tokens = self.runtime_tokens.lock().await.take();
        if let Some(runtime_tokens) = &runtime_tokens {
            runtime_tokens.admission.cancel();
            runtime_tokens.force.cancel();
        }
        let registration_barrier = self.registration_gate.lock().await;
        self.buffers.clear();
        drop(registration_barrier);

        // Force is propagated through supervisor -> dispatcher -> nested
        // handler tasks. Joining the retained supervisors therefore proves
        // that every framework-owned child task has also been joined.
        let _gate = self.drain_gate.lock().await;
        let mut terminal = self.drain_terminal.lock().await;
        let result = if let Some(cached) = terminal.clone() {
            // A prior graceful observer joined every task. Its primary error
            // remains replayable from `drain`; forced cleanup itself has no
            // remaining resource to reconcile.
            if self.drain_reconciled.load(Ordering::Acquire) {
                Ok(())
            } else {
                cached
            }
        } else {
            let outcome = drain_background_tasks(&self.background_tasks).await;
            let reconciled = outcome.reconciled
                && self
                    .channel_owners
                    .iter()
                    .all(|entry| entry.value().children_reconciled());
            self.drain_reconciled.store(reconciled, Ordering::Release);
            if !reconciled {
                self.terminal_ledger.failed();
            }
            let result = outcome.result;
            *terminal = Some(result.clone());
            result
        };
        self.terminal_ledger.stopped();
        if let Some(runtime_tokens) = runtime_tokens {
            runtime_tokens.settlement.cancel();
        }
        result
    }

    /// Background task'ların tamamlanmasını bekle
    async fn wait_for_completion(&self) -> Result<(), MessageBrokerError> {
        self.drain_to_terminal().await
    }

    fn drain_reconciled(&self) -> bool {
        self.drain_reconciled.load(Ordering::Acquire)
    }

    #[lily_trace::prelude::instrument(
        name = "rabbitmq.consumer.create",
        skip(self, handler),
        fields(worker = %worker, queue = %queue)
    )]
    /// Register one bounded, supervised RabbitMQ consumer pair.
    async fn create_consumer(
        &self,
        worker: &str,
        queue: &str,
        handler: QueueDeliveryHandler,
    ) -> Result<(), MessageBrokerError> {
        let runtime_tokens = {
            let runtime_tokens = self.runtime_tokens.lock().await;
            runtime_tokens
                .clone()
                .ok_or(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                    "Consumer engine not started".to_string(),
                )))?
        };
        let admission = runtime_tokens.admission;
        let settlement = runtime_tokens.settlement;
        let force = runtime_tokens.force;
        let execution = self.execution.clone();

        if self.registration_sealed.load(Ordering::Acquire) || admission.is_cancelled() {
            return Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled));
        }

        let queue_setting = self.options.queue(queue)?.clone();
        let topology = self.options.topology(queue)?.clone();
        if worker != topology.main_exchange {
            return Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::Configuration(format!(
                    "consumer exchange {worker:?} does not match accepted topology exchange {:?} for queue {queue:?}",
                    topology.main_exchange,
                )),
            ));
        }

        // Bounded channel: backpressure sağlar
        let channel_capacity = queue_setting.delivery_buffer_capacity;
        let (tx, rx) = tokio::sync::mpsc::channel::<BufferedDelivery>(channel_capacity);

        let key = (worker.to_owned(), queue.to_owned());
        // Acquire the engine ledger before spawning. Cancellation while this
        // await is pending owns no broker resource and creates no detached
        // task. Once spawned, the adoption barrier below prevents the task
        // from running until its JoinHandle is retained in this vector.
        let mut background_tasks = self.background_tasks.lock().await;
        if self.registration_sealed.load(Ordering::Acquire) || admission.is_cancelled() {
            return Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled));
        }

        let registration_gate = Arc::clone(&self.registration_gate);
        let registration_states = Arc::clone(&self.registration_states);
        let registration_sealed = Arc::clone(&self.registration_sealed);
        let task_channel_owners = self.channel_owners.clone();
        let task_shutdown_budget = self.shutdown_budget.clone();
        let task_channel_manager = Arc::clone(&self.channel_manager);
        let task_buffers = Arc::clone(&self.buffers);
        let task_terminal_ledger = Arc::clone(&self.terminal_ledger);
        let task_retry_engine = Arc::clone(&self.retry_engine);
        let task_worker = worker.to_owned();
        let task_queue = queue.to_owned();
        let task_key = key.clone();
        let confirm_timeout = self.options.confirm_timeout;
        let (startup_tx, startup_rx) =
            tokio::sync::oneshot::channel::<Result<(), MessageBrokerError>>();
        let failure_admission = admission.clone();
        let failure_execution = execution.clone();
        let failure_force = force.clone();
        let failure_ledger = task_terminal_ledger.clone();
        let failure_queue = task_queue.clone();

        let registration_task = async move {
            let mut startup_tx = startup_tx;
            let registration = tokio::select! {
                biased;
                _ = startup_tx.closed() => return Ok(()),
                _ = admission.cancelled() => {
                    let _ = startup_tx.send(Err(MessageBrokerError::RabbitMQError(
                        RabbitMQError::Cancelled,
                    )));
                    return Ok(());
                }
                registration = registration_gate.lock() => registration,
            };

            if registration_sealed.load(Ordering::Acquire) || admission.is_cancelled() {
                let _ = startup_tx.send(Err(MessageBrokerError::RabbitMQError(
                    RabbitMQError::Cancelled,
                )));
                return Ok(());
            }
            if let Err(error) = reserve_queue_registration(&registration_states, &task_key) {
                let _ = startup_tx.send(Err(error));
                return Ok(());
            }

            let channel_ownership = Arc::new(ConsumerChannelOwnership::default());
            task_channel_owners.insert(task_key.clone(), channel_ownership.clone());
            let registration_cancellation = admission.child_token();
            let (open_result, observer_dropped) = observe_registration_attempt(
                &mut startup_tx,
                &registration_cancellation,
                open_consumer_bounded(
                    &task_channel_manager,
                    &task_worker,
                    &task_queue,
                    &topology,
                    &queue_setting,
                    &registration_cancellation,
                    queue_setting.max_retry_backoff,
                    "consumer startup readiness",
                    &task_terminal_ledger,
                    &task_shutdown_budget,
                    &channel_ownership,
                ),
            )
            .await;

            let (initial_channel, initial_consumer, initial_tag) = match open_result {
                Ok(opened) => opened,
                Err(failure) => {
                    task_terminal_ledger.record_operational_failure(failure.error_code());
                    finish_registration_rollback(
                        &registration_states,
                        &task_key,
                        failure.cleanup_unproven,
                    );
                    drop(registration);
                    if !observer_dropped {
                        let _ = startup_tx.send(Err(failure.into_primary()));
                    }
                    return Ok(());
                }
            };

            if observer_dropped || startup_tx.is_closed() || admission.is_cancelled() {
                let cleanup = cancel_and_close_consumer_bounded(
                    &initial_channel,
                    &initial_tag,
                    queue_setting.settlement_timeout,
                    &task_shutdown_budget,
                )
                .await;
                if let Err(error) = &cleanup {
                    task_terminal_ledger.record_operational_failure(error.error_code());
                }
                channel_ownership.release_if_terminal();
                finish_registration_cleanup_result(&registration_states, &task_key, &cleanup);
                drop(registration);
                return Ok(());
            }

            task_buffers.insert(task_key.clone(), tx.clone());
            registration_states.insert(task_key.clone(), QueueRegistrationState::Active);

            let readiness =
                match publish_registration_readiness(startup_tx, Arc::clone(&task_terminal_ledger))
                {
                    Ok(readiness) => readiness,
                    Err(()) => {
                        task_buffers.remove_if(&task_key, |_, sender| sender.same_channel(&tx));
                        let cleanup = cancel_and_close_consumer_bounded(
                            &initial_channel,
                            &initial_tag,
                            queue_setting.settlement_timeout,
                            &task_shutdown_budget,
                        )
                        .await;
                        if let Err(error) = &cleanup {
                            task_terminal_ledger.record_operational_failure(error.error_code());
                        }
                        channel_ownership.release_if_terminal();
                        finish_registration_cleanup_result(
                            &registration_states,
                            &task_key,
                            &cleanup,
                        );
                        drop(registration);
                        return Ok(());
                    }
                };

            // Shutdown's registration-gate barrier may pass only after the
            // committed buffer, state and readiness are all visible. Failure
            // above retains the same guard through cancel/close rollback.
            drop(registration);

            let readiness = Arc::new(std::sync::Mutex::new(readiness));
            let mut current = Some((initial_channel, initial_consumer, initial_tag, tx, rx));
            let mut recovery_backoff = queue_setting.retry_backoff;
            loop {
                let (channel, consumer, tag, tx, rx) = match current.take() {
                    Some(current) => current,
                    None => {
                        let registration = tokio::select! {
                            biased;
                            _ = admission.cancelled() => break,
                            guard = registration_gate.lock() => guard,
                        };
                        if registration_sealed.load(Ordering::Acquire) || admission.is_cancelled() {
                            break;
                        }
                        task_terminal_ledger.record_consumer_recovery_attempt();
                        let opened = open_consumer_bounded(
                            &task_channel_manager,
                            &task_worker,
                            &task_queue,
                            &topology,
                            &queue_setting,
                            &admission,
                            queue_setting.max_retry_backoff,
                            "consumer runtime recovery",
                            &task_terminal_ledger,
                            &task_shutdown_budget,
                            &channel_ownership,
                        )
                        .await;
                        match opened {
                            Ok((channel, consumer, tag)) => {
                                if registration_sealed.load(Ordering::Acquire)
                                    || admission.is_cancelled()
                                {
                                    let cleanup = cancel_and_close_consumer_bounded(
                                        &channel,
                                        &tag,
                                        queue_setting.settlement_timeout,
                                        &task_shutdown_budget,
                                    )
                                    .await;
                                    channel_ownership.release_if_terminal();
                                    finish_registration_cleanup_result(
                                        &registration_states,
                                        &task_key,
                                        &cleanup,
                                    );
                                    if let Err(error) = cleanup {
                                        return Err(RabbitMqConsumerTaskFailure {
                                            queue: task_queue.clone(),
                                            role: RabbitMqConsumerTaskRole::Receiver,
                                            kind: RabbitMqConsumerTaskFailureKind::OperationFailed,
                                            operation_error_code: Some(error.error_code()),
                                        });
                                    }
                                    break;
                                }
                                let (tx, rx) = tokio::sync::mpsc::channel(channel_capacity);
                                task_buffers.insert(task_key.clone(), tx.clone());
                                readiness.lock().unwrap_or_else(|e| e.into_inner()).ready();
                                recovery_backoff = queue_setting.retry_backoff;
                                task_terminal_ledger.record_consumer_recovery_outcome("success");
                                drop(registration);
                                (channel, consumer, tag, tx, rx)
                            }
                            Err(failure) => {
                                task_terminal_ledger.record_consumer_recovery_outcome("failure");
                                task_terminal_ledger
                                    .record_operational_failure(failure.error_code());
                                drop(registration);
                                if failure.cleanup_unproven {
                                    registration_states.insert(
                                        task_key.clone(),
                                        QueueRegistrationState::CleanupUnproven,
                                    );
                                    return Err(RabbitMqConsumerTaskFailure {
                                        queue: task_queue.clone(),
                                        role: RabbitMqConsumerTaskRole::Receiver,
                                        kind: RabbitMqConsumerTaskFailureKind::OperationFailed,
                                        operation_error_code: Some(failure.error_code()),
                                    });
                                }
                                tokio::select! {
                                    _ = admission.cancelled() => break,
                                    _ = tokio::time::sleep(recovery_backoff) => {},
                                }
                                recovery_backoff = recovery_backoff
                                    .saturating_mul(2)
                                    .min(queue_setting.max_retry_backoff);
                                continue;
                            }
                        }
                    }
                };
                let generation_admission = admission.child_token();
                let generation_force = force.child_token();
                let generation_settlement = settlement.child_token();
                let generation_execution = execution.child();
                let recover = Arc::new(AtomicBool::new(false));
                let receiver_recover = recover.clone();
                let receiver_admission = generation_admission.clone();
                let receiver_force = generation_force.clone();
                let receiver_settlement = generation_settlement.clone();
                let receiver_execution = generation_execution.clone();
                let receiver_readiness = readiness.clone();
                let receiver_ledger = task_terminal_ledger.clone();
                let receiving = Self::receive_generation(
                    consumer,
                    task_worker.clone(),
                    task_queue.clone(),
                    queue_setting.clone(),
                    tx.clone(),
                    receiver_admission.clone(),
                    receiver_settlement.clone(),
                    task_retry_engine.clone(),
                    task_terminal_ledger.clone(),
                    confirm_timeout,
                    task_shutdown_budget.clone(),
                );
                let receiver = async move {
                    if receiving.await? {
                        receiver_readiness
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .recovering();
                        receiver_ledger.record_consumer_connection_loss();
                        receiver_recover.store(true, Ordering::Release);
                        // Transport loss ends only this physical generation. Its
                        // old deliveries must be terminal before a new one opens.
                        receiver_admission.cancel();
                        receiver_execution.cancel(DeliveryCancellationReason::RuntimeCancellation);
                        receiver_settlement.cancel();
                        receiver_force.cancel();
                    }
                    Ok(())
                };
                let dispatcher = Self::dispatch_owned(
                    task_worker.clone(),
                    task_queue.clone(),
                    handler.clone(),
                    rx,
                    generation_admission.clone(),
                    generation_force.clone(),
                    generation_settlement.clone(),
                    DispatchPolicy::from_setting(&queue_setting, confirm_timeout),
                    task_retry_engine.clone(),
                    task_terminal_ledger.clone(),
                    generation_execution.clone(),
                    task_shutdown_budget.clone(),
                );
                let evidence = Arc::new(QueuePairEvidence {
                    reconciled: AtomicBool::new(false),
                    runtime_failure: Some(QueueRuntimeFailureAuthority {
                        admission: admission.clone(),
                        execution: execution.clone(),
                        force: force.clone(),
                    }),
                });
                channel_ownership.track_pair(evidence.clone());
                let pair = supervise_queue_pair(
                    task_queue.clone(),
                    generation_admission.clone(),
                    generation_force,
                    generation_execution,
                    task_terminal_ledger.clone(),
                    receiver,
                    dispatcher,
                    evidence.clone(),
                );
                let result = finish_consumer_generation(
                    pair,
                    async {
                        generation_admission.cancelled().await;
                        cancel_consumer_bounded(
                            &channel,
                            &tag,
                            queue_setting.settlement_timeout,
                            &task_shutdown_budget,
                        )
                        .await
                    },
                    async {
                        close_consumer_channel_bounded(
                            &channel,
                            queue_setting.settlement_timeout,
                            &task_shutdown_budget,
                        )
                        .await
                        .map_err(|error| RabbitMqConsumerTaskFailure {
                            queue: task_queue.clone(),
                            role: RabbitMqConsumerTaskRole::Receiver,
                            kind: RabbitMqConsumerTaskFailureKind::OperationFailed,
                            operation_error_code: Some(error.error_code()),
                        })
                    },
                    &evidence,
                )
                .await;
                generation_settlement.cancel();
                task_buffers.remove_if(&task_key, |_, sender| sender.same_channel(&tx));
                let transport_terminal = channel_ownership.release_if_terminal();
                if !transport_terminal {
                    registration_states
                        .insert(task_key.clone(), QueueRegistrationState::CleanupUnproven);
                }
                result?;
                if admission.is_cancelled() || !recover.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    _ = admission.cancelled() => break,
                    _ = tokio::time::sleep(queue_setting.retry_backoff) => {},
                }
            }
            Ok(())
        };

        let supervised_registration = async move {
            let result = AssertUnwindSafe(registration_task)
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(RabbitMqConsumerTaskFailure {
                        queue: failure_queue,
                        role: RabbitMqConsumerTaskRole::Supervisor,
                        kind: RabbitMqConsumerTaskFailureKind::Panicked,
                        operation_error_code: None,
                    })
                });
            if let Err(failure) = &result {
                // Generation-local recovery signals must not hide a fatal
                // registration/dispatcher failure from sibling subscriptions.
                failure_ledger.record_operational_failure(
                    failure
                        .operation_error_code
                        .unwrap_or("BROKER_CONSUMER_TASK_FAILED"),
                );
                failure_ledger.failed();
                failure_admission.cancel();
                failure_execution.cancel(DeliveryCancellationReason::RuntimeFailure);
                failure_force.cancel();
            }
            result
        };
        adopt_supervised_queue_task(
            &mut background_tasks,
            queue.to_owned(),
            supervised_registration,
        );
        drop(background_tasks);

        startup_rx.await.unwrap_or_else(|_| {
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "consumer registration owner terminated before readiness".into(),
            )))
        })
    }

    fn delivery_terminal_snapshot(&self) -> DeliveryTerminalSnapshot {
        RabbitMQQueueEngine::delivery_terminal_snapshot(self)
    }

    fn delivery_terminal_observations(&self) -> crate::DeliveryTerminalObservationsSnapshot {
        self.terminal_ledger.observations_snapshot()
    }
}

impl RabbitMQQueueEngine {
    #[allow(
        clippy::too_many_arguments,
        reason = "the private coordinator keeps delivery identity, policy, lifecycle and evidence explicit"
    )]
    async fn materialize_outcome(
        worker: &str,
        queue: &str,
        delivery: &Delivery,
        body: &[u8],
        cancellation: &CancellationToken,
        retry_engine: &Arc<dyn RetryEngine<Delivery>>,
        authority: &SettlementAuthority,
        attempt_guard: &mut DeliveryAttemptGuard,
        outcome: ExecutionOutcome,
        retry_attempts: u32,
        handoff_timeout: Duration,
        settlement_timeout: Duration,
        shutdown_budget: &QueueShutdownBudget,
    ) -> Result<(), MessageBrokerError> {
        let outcome = if cancellation.is_cancelled() {
            ExecutionOutcome::FrameworkCancelled
        } else {
            outcome
        };
        let current_retry_count = match outcome {
            ExecutionOutcome::FrameworkCancelled | ExecutionOutcome::RequeueDeferred { .. } => 0,
            _ => canonical_retry_count(delivery.properties.headers().as_ref())
                .map_err(invalid_message)?,
        };
        let mut port = RabbitMqSettlementPort {
            worker,
            queue,
            delivery,
            body,
            cancellation,
            retry_engine,
        };
        // From this point forward a cancelled future may have reached broker
        // I/O. Drop must therefore retain `Unresolved`; only a force abort
        // before settlement starts proves pending broker redelivery.
        attempt_guard.mark_settlement_started();
        let mut observer = DeliverySettlementObserver(attempt_guard);
        let report = materialize_delivery(
            &mut port,
            authority,
            &mut observer,
            outcome,
            current_retry_count,
            retry_attempts,
            cancellation,
            handoff_timeout,
            settlement_timeout,
            &shutdown_budget,
        )
        .await;
        finish_settlement_report(report)
    }

    async fn materialize_broker_dead_letter(
        delivery: &Delivery,
        cancellation: &CancellationToken,
        authority: &SettlementAuthority,
        attempt_guard: &mut DeliveryAttemptGuard,
        settlement_timeout: Duration,
        shutdown_budget: &QueueShutdownBudget,
    ) -> Result<(), MessageBrokerError> {
        struct BrokerDeadLetterPort<'a>(&'a Delivery);

        #[async_trait]
        impl SettlementPort for BrokerDeadLetterPort<'_> {
            async fn handoff(
                &mut self,
                _plan: HandoffPlan,
            ) -> Result<HandoffReceipt, MessageBrokerError> {
                Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                    "broker dead-letter adapter does not publish handoffs".into(),
                )))
            }

            async fn ack(&mut self) -> Result<bool, MessageBrokerError> {
                Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                    "broker dead-letter adapter does not ACK".into(),
                )))
            }

            async fn nack_requeue(&mut self) -> Result<bool, MessageBrokerError> {
                Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                    "broker dead-letter adapter does not requeue".into(),
                )))
            }

            async fn nack_dead_letter(&mut self) -> Result<bool, MessageBrokerError> {
                self.0
                    .nack(oversized_delivery_nack_options())
                    .instrument(tracing::info_span!("messaging.consume.nack"))
                    .await
                    .map_err(|error| {
                        lapin_settlement_error("consumer broker dead-letter NACK settlement", error)
                    })
            }
        }

        let mut port = BrokerDeadLetterPort(delivery);
        attempt_guard.mark_settlement_started();
        let mut observer = DeliverySettlementObserver(attempt_guard);
        let report = materialize_broker_dead_letter(
            &mut port,
            authority,
            &mut observer,
            cancellation,
            settlement_timeout,
            &shutdown_budget,
        )
        .await;
        finish_settlement_report(report)
    }

    async fn wait_for_dispatch_capacity(
        in_flight_tasks: &mut JoinSet<Result<(), &'static str>>,
        admission: &CancellationToken,
        force: &CancellationToken,
        handler_concurrency: usize,
        terminal_ledger: &DeliveryTerminalLedger,
    ) -> Result<bool, &'static str> {
        if in_flight_tasks.len() < handler_concurrency {
            return Ok(true);
        }

        let wait_started = std::time::Instant::now();
        while in_flight_tasks.len() >= handler_concurrency {
            tokio::select! {
                biased;
                _ = force.cancelled() => return Ok(false),
                _ = admission.cancelled() => return Ok(false),
                join_result = in_flight_tasks.join_next() => {
                    let Some(join_result) = join_result else {
                        return Err("BROKER_CONSUMER_TASK_FAILED");
                    };
                    Self::reconcile_in_flight_result(join_result)?;
                }
            }
        }
        terminal_ledger.record_semaphore_wait(wait_started.elapsed());
        Ok(true)
    }

    /// Yeni model:
    /// - Mesaj gelir gelmez işleme alınır
    /// - Aynı anda en fazla configured concurrency kadar mesaj işlenir
    #[allow(
        clippy::too_many_arguments,
        reason = "dispatcher receives distinct admission and settlement lifecycle authorities"
    )]
    #[cfg(test)]
    fn dispatch_loop(
        &self,
        worker: &str,
        queue: &str,
        handler: QueueDeliveryHandler,
        rx: tokio::sync::mpsc::Receiver<BufferedDelivery>,
        admission: CancellationToken,
        force: CancellationToken,
        settlement: CancellationToken,
        policy: DispatchPolicy,
    ) -> impl Future<Output = Result<(), &'static str>> + Send + 'static {
        Self::dispatch_owned(
            worker.to_owned(),
            queue.to_owned(),
            handler,
            rx,
            admission,
            force,
            settlement,
            policy,
            self.retry_engine.clone(),
            self.terminal_ledger.clone(),
            self.execution.clone(),
            self.shutdown_budget.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_owned(
        worker: String,
        queue: String,
        handler: QueueDeliveryHandler,
        mut rx: tokio::sync::mpsc::Receiver<BufferedDelivery>,
        admission: CancellationToken,
        force: CancellationToken,
        settlement: CancellationToken,
        policy: DispatchPolicy,
        retry_engine: Arc<dyn RetryEngine<Delivery>>,
        terminal_ledger: Arc<DeliveryTerminalLedger>,
        execution: DeliveryCancellationSource,
        shutdown_budget: QueueShutdownBudget,
    ) -> impl Future<Output = Result<(), &'static str>> + Send + 'static {
        async move {
            let mut in_flight_tasks = JoinSet::new();
            let mut first_failure = None;

            loop {
                // Reap every result which is already ready before admitting a
                // new delivery. A sustained ready receiver cannot hide a
                // completed settlement failure or retain completed JoinSet
                // records indefinitely.
                while let Some(join_result) = in_flight_tasks.try_join_next() {
                    Self::retain_first_in_flight_failure(&mut first_failure, join_result, false);
                }
                if first_failure.is_some() {
                    break;
                }

                // Do not remove a delivery from the bounded receiver while
                // every execution slot is occupied. Capacity is reopened by
                // observing the completed task itself, not merely by a
                // semaphore wake-up. Therefore a terminal task failure is
                // reconciled before another handler can start.
                let capacity = Self::wait_for_dispatch_capacity(
                    &mut in_flight_tasks,
                    &admission,
                    &force,
                    policy.handler_concurrency,
                    &terminal_ledger,
                )
                .await;
                match capacity {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(error_code) => {
                        first_failure.get_or_insert(error_code);
                        break;
                    }
                }

                tokio::select! {
                    biased;
                    _ = force.cancelled() => {
                        break;
                    }
                    _ = admission.cancelled() => {
                        break;
                    }

                    Some(join_result) = in_flight_tasks.join_next(), if !in_flight_tasks.is_empty() => {
                        Self::retain_first_in_flight_failure(
                            &mut first_failure,
                            join_result,
                            false,
                        );
                        if first_failure.is_some() {
                            break;
                        }
                    }

                    msg = rx.recv() => {
                        match msg {
                            Some(mut accepted) => {
                                if admission.is_cancelled() || force.is_cancelled() {
                                    accepted.attempt_guard.finish_buffered_pending_redelivery();
                                    break;
                                }
                                let BufferedDelivery {
                                    delivery,
                                    body,
                                    enqueued_at,
                                    mut attempt_guard,
                                    settlement_authority,
                                } = accepted;
                                let queue_wait = enqueued_at.elapsed();
                                terminal_ledger.record_queue_wait(queue_wait);
                                terminal_ledger.record_queue_depth(rx.len());

                                let worker = worker.clone();
                                let queue = queue.clone();
                                let handler = handler.clone();
                                let execution = execution.clone();
                                let shutdown_budget = shutdown_budget.clone();
                                let settlement = settlement.clone();
                                let retry_engine = retry_engine.clone();
                                let terminal_ledger = Arc::clone(&terminal_ledger);
                                attempt_guard.arm_force_redelivery(force.clone());

                                in_flight_tasks.spawn(async move {
                                    let result = Self::handle_message_static(
                                        &worker,
                                        &queue,
                                        delivery,
                                        &handler,
                                        body,
                                        &execution,
                                        &settlement,
                                        &retry_engine,
                                        &terminal_ledger,
                                        attempt_guard,
                                        settlement_authority,
                                        queue_wait,
                                        policy.retry_attempts,
                                        policy.delivery_execution_timeout,
                                        policy.handoff_timeout,
                                        policy.settlement_timeout,
                                        &shutdown_budget,
                                    )
                                    .await;
                                    if let Err(error) = &result {
                                        error!(
                                            "Message processing failed for worker={}, queue={}: {}",
                                            worker, queue, error
                                        );
                                    }
                                    result.map_err(|error| error.error_code())
                                });
                            }
                            None => {
                                break;
                            }
                        }
                    }
                }
            }

            // Buffered deliveries never started execution and are released
            // for broker redelivery. A force signal or any observed terminal
            // error notifies started work before its bounded stop window; every JoinSet entry
            // is still consumed before the dispatcher reports its first
            // failure. Graceful admission shutdown continues to await started
            // handlers and their settlement work.
            Self::release_buffered_deliveries(&mut rx).await;
            let abort_remaining = force.is_cancelled() || first_failure.is_some();
            Self::finish_in_flight_tasks(
                &mut in_flight_tasks,
                abort_remaining,
                first_failure,
                Some(&force),
                Some(&execution),
                Some(&shutdown_budget),
                Some(&settlement),
            )
            .await
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    async fn materialize_delivery_context_or_settle(
        worker: &str,
        queue: &str,
        delivery: &Delivery,
        settlement: &CancellationToken,
        retry_engine: &Arc<dyn RetryEngine<Delivery>>,
        terminal_ledger: &Arc<DeliveryTerminalLedger>,
        retry_attempts: u32,
        handoff_timeout: Duration,
        settlement_timeout: Duration,
        shutdown_budget: &QueueShutdownBudget,
    ) -> Result<Option<(DeliveryAttemptGuard, SettlementAuthority)>, MessageBrokerError> {
        match delivery_context(delivery, queue) {
            Ok(context) => Ok(Some((
                DeliveryAttemptGuard::begin(
                    Arc::clone(terminal_ledger),
                    Some(context.event_id().into_inner().to_string()),
                    context.retry_count().into_inner().saturating_add(1),
                ),
                SettlementAuthority::new(),
            ))),
            Err(_) => {
                terminal_ledger.record_handler_outcome("invalid_transport_identity");
                terminal_ledger.record_poison();
                let mut attempt_guard = DeliveryAttemptGuard::begin(
                    Arc::clone(terminal_ledger),
                    canonical_event_id(&delivery.properties),
                    canonical_delivery_attempt(&delivery.properties),
                );
                let settlement_authority = SettlementAuthority::new();
                Self::materialize_outcome(
                    worker,
                    queue,
                    delivery,
                    &delivery.data,
                    settlement,
                    retry_engine,
                    &settlement_authority,
                    &mut attempt_guard,
                    ExecutionOutcome::Failure {
                        class: FailureClass::Permanent,
                        code: "invalid_transport_identity",
                    },
                    retry_attempts,
                    handoff_timeout,
                    settlement_timeout,
                    &shutdown_budget,
                )
                .await?;
                Ok(None)
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "receiver admission keeps broker identity, delivery, buffer, settlement authority and terminal evidence explicit"
    )]
    async fn materialize_and_buffer_delivery(
        worker: &str,
        queue: &str,
        delivery: Delivery,
        tx: &tokio::sync::mpsc::Sender<BufferedDelivery>,
        settlement: &CancellationToken,
        retry_engine: &Arc<dyn RetryEngine<Delivery>>,
        terminal_ledger: &Arc<DeliveryTerminalLedger>,
        retry_attempts: u32,
        handoff_timeout: Duration,
        settlement_timeout: Duration,
        shutdown_budget: &QueueShutdownBudget,
    ) -> Result<bool, MessageBrokerError> {
        let Some((attempt_guard, settlement_authority)) =
            Self::materialize_delivery_context_or_settle(
                worker,
                queue,
                &delivery,
                settlement,
                retry_engine,
                terminal_ledger,
                retry_attempts,
                handoff_timeout,
                settlement_timeout,
                &shutdown_budget,
            )
            .await?
        else {
            // The poison delivery already reached one proven terminal
            // settlement. Keep this receiver alive for the next delivery.
            return Ok(true);
        };

        let accepted = BufferedDelivery {
            body: delivery.data.clone(),
            delivery,
            enqueued_at: std::time::Instant::now(),
            attempt_guard,
            settlement_authority,
        };
        let admission_started = std::time::Instant::now();
        if let Err(error) = tx.send(accepted).await {
            let mut accepted = error.0;
            accepted.attempt_guard.finish_buffered_pending_redelivery();
            return Ok(false);
        }
        terminal_ledger.record_admission_wait(admission_started.elapsed());
        terminal_ledger.record_queue_depth(tx.max_capacity().saturating_sub(tx.capacity()));
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_message_static(
        worker: &str,
        queue: &str,
        delivery: Delivery,
        handler: &QueueDeliveryHandler,
        body: Vec<u8>,
        execution: &DeliveryCancellationSource,
        settlement: &CancellationToken,
        retry_engine: &Arc<dyn RetryEngine<Delivery>>,
        terminal_ledger: &Arc<DeliveryTerminalLedger>,
        mut attempt_guard: DeliveryAttemptGuard,
        settlement_authority: SettlementAuthority,
        queue_wait: Duration,
        retry_attempts: u32,
        delivery_execution_timeout: Duration,
        handoff_timeout: Duration,
        settlement_timeout: Duration,
        shutdown_budget: &QueueShutdownBudget,
    ) -> Result<(), MessageBrokerError> {
        let remote_parent = delivery
            .properties
            .headers()
            .as_ref()
            .map(|headers| lily_trace::extract_context(&AmqpHeaderExtractor(headers)))
            .unwrap_or_default();
        let context = delivery_context(&delivery, queue)?;
        let span = tracing::info_span!(
            "messaging.consume",
            otel.kind = "consumer",
            messaging.system = "rabbitmq",
            messaging.destination.name = %queue,
            messaging.operation.type = "process",
            messaging.message.body.size = body.len(),
            messaging.rabbitmq.message.redelivered = delivery.redelivered,
            lily.event_id = %context.event_id().into_inner(),
            lily.delivery_attempt = u64::from(context.retry_count().into_inner()).saturating_add(1),
            lily.schema_version = u64::from(context.schema_version().into_inner()),
            lily.handler = %worker,
            lily.queue_wait_ms = queue_wait.as_secs_f64() * 1_000.0,
            lily.outcome = tracing::field::Empty,
            lily.handoff_outcome = tracing::field::Empty,
            lily.ack_outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        lily_trace::set_parent(&span, remote_parent);

        let result = Self::handle_message_inner(
            worker,
            queue,
            delivery,
            handler,
            body,
            execution,
            settlement,
            retry_engine,
            terminal_ledger,
            &settlement_authority,
            &mut attempt_guard,
            retry_attempts,
            delivery_execution_timeout,
            handoff_timeout,
            settlement_timeout,
            &shutdown_budget,
        )
        .instrument(span.clone())
        .await;

        match &result {
            Ok(()) => {
                // Completion of this task can also mean an unresolved,
                // force-interrupted delivery. ACK evidence is a separate field.
                span.record("lily.outcome", "completed");
            }
            Err(error) => {
                span.record("lily.outcome", "error");
                span.record("lily.error_code", error.error_code());
                span.record("otel.status_code", "ERROR");
            }
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_message_inner(
        worker: &str,
        queue: &str,
        delivery: Delivery,
        handler: &QueueDeliveryHandler,
        body: Vec<u8>,
        execution: &DeliveryCancellationSource,
        settlement: &CancellationToken,
        retry_engine: &Arc<dyn RetryEngine<Delivery>>,
        terminal_ledger: &Arc<DeliveryTerminalLedger>,
        settlement_authority: &SettlementAuthority,
        attempt_guard: &mut DeliveryAttemptGuard,
        retry_attempts: u32,
        delivery_execution_timeout: Duration,
        handoff_timeout: Duration,
        settlement_timeout: Duration,
        shutdown_budget: &QueueShutdownBudget,
    ) -> Result<(), MessageBrokerError> {
        let decode_span = tracing::info_span!(
            "messaging.consume.decode",
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let envelope_result =
            decode_span.in_scope(|| validate_delivery_envelope(&delivery.properties));
        if let Err(error) = envelope_result {
            decode_span.record("lily.outcome", "invalid");
            decode_span.record("lily.error_code", error.error_code());
            decode_span.record("otel.status_code", "ERROR");
            terminal_ledger.record_handler_outcome("invalid_envelope");
            terminal_ledger.record_poison();
            Self::materialize_outcome(
                worker,
                queue,
                &delivery,
                &body,
                settlement,
                retry_engine,
                settlement_authority,
                attempt_guard,
                ExecutionOutcome::Failure {
                    class: FailureClass::Permanent,
                    code: "invalid_transport_envelope",
                },
                retry_attempts,
                handoff_timeout,
                settlement_timeout,
                &shutdown_budget,
            )
            .await?;
            return Ok(());
        }
        decode_span.record("lily.outcome", "success");

        let deadline = tokio::time::Instant::now() + delivery_execution_timeout;
        let input = delivery_input(&delivery, queue, body.clone(), execution.child(), deadline)?;
        let handler_started = std::time::Instant::now();
        let handler_span = tracing::info_span!(
            "messaging.consume.handler",
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        // QueueService owns the aggregate deadline and explicit DI-scope
        // cleanup. The engine must not drop that future on a second timeout:
        // doing so could settle the delivery before scoped dependencies close.
        let handler_result = AssertUnwindSafe(handler(input))
            .catch_unwind()
            .instrument(handler_span.clone())
            .await;
        let handler_duration = handler_started.elapsed();
        match handler_result {
            Ok(Ok(_)) => {
                terminal_ledger.record_handler_duration(handler_duration, "success");
                handler_span.record("lily.outcome", "success");
                attempt_guard.record_handler_success();
                #[cfg(feature = "test-support")]
                post_handler_settlement_test_support::pause(
                    delivery_context(&delivery, queue)?.event_id().into_inner(),
                    settlement,
                )
                .await?;
                Self::materialize_outcome(
                    worker,
                    queue,
                    &delivery,
                    &body,
                    settlement,
                    retry_engine,
                    settlement_authority,
                    attempt_guard,
                    ExecutionOutcome::Success,
                    retry_attempts,
                    handoff_timeout,
                    settlement_timeout,
                    &shutdown_budget,
                )
                .await?;
                tracing::Span::current().record("lily.ack_outcome", "handler_success");
            }
            Ok(Err(QueueExecutionError::Handler(error))) => {
                let failure_code = failure_code(&error);
                let failure_outcome = handler_failure_outcome(&error);
                terminal_ledger.record_handler_duration(handler_duration, failure_outcome);
                handler_span.record("lily.outcome", failure_outcome);
                handler_span.record("lily.error_code", failure_code);
                handler_span.record("otel.status_code", "ERROR");
                let failure_class = failure_class(&error);
                error!(error_code = failure_code, "Queue handler failed");
                terminal_ledger.handler_failure(failure_outcome);
                Self::materialize_outcome(
                    worker,
                    queue,
                    &delivery,
                    &body,
                    settlement,
                    retry_engine,
                    settlement_authority,
                    attempt_guard,
                    ExecutionOutcome::Failure {
                        class: failure_class,
                        code: failure_code,
                    },
                    retry_attempts,
                    handoff_timeout,
                    settlement_timeout,
                    &shutdown_budget,
                )
                .await?;
            }
            Ok(Err(QueueExecutionError::RequeueDeferred { code })) => {
                terminal_ledger.record_handler_duration(handler_duration, "requeue_deferred");
                handler_span.record("lily.outcome", "requeue_deferred");
                handler_span.record("lily.error_code", code);
                Self::materialize_outcome(
                    worker,
                    queue,
                    &delivery,
                    &body,
                    settlement,
                    retry_engine,
                    settlement_authority,
                    attempt_guard,
                    ExecutionOutcome::RequeueDeferred { code },
                    retry_attempts,
                    handoff_timeout,
                    settlement_timeout,
                    &shutdown_budget,
                )
                .await?;
                tracing::Span::current().record("lily.ack_outcome", "nack_requeue");
            }
            Ok(Err(QueueExecutionError::FrameworkCancelled { code })) => {
                terminal_ledger.record_handler_duration(handler_duration, "cancelled");
                handler_span.record("lily.outcome", "cancelled");
                handler_span.record("lily.error_code", code);
                // A framework-owned cancellation is not an application
                // failure and must not enter retry/DLQ policy. No broker
                // settlement has started. Controlled force therefore uses
                // the same pending-redelivery evidence as an execution Drop;
                // absent that proof the outcome remains unresolved.
                if let Some(outcome) = attempt_guard.finish_interrupted() {
                    tracing::Span::current().record("lily.ack_outcome", outcome.as_str());
                }
                return Ok(());
            }
            Err(_) => {
                terminal_ledger.record_handler_duration(handler_duration, "panic");
                handler_span.record("lily.outcome", "panic");
                handler_span.record("lily.error_code", "HANDLER_PANIC");
                handler_span.record("otel.status_code", "ERROR");
                error!("Handler panicked; applying retry/DLQ policy");
                terminal_ledger.handler_failure("panic");
                Self::materialize_outcome(
                    worker,
                    queue,
                    &delivery,
                    &body,
                    settlement,
                    retry_engine,
                    settlement_authority,
                    attempt_guard,
                    ExecutionOutcome::Failure {
                        class: FailureClass::Retryable,
                        code: "QUEUE_HANDLER_PANICKED",
                    },
                    retry_attempts,
                    handoff_timeout,
                    settlement_timeout,
                    &shutdown_budget,
                )
                .await?;
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
struct CanonicalEnvelope<'a> {
    event_id: &'a str,
    schema_version: &'a str,
    content_kind: &'a str,
}

fn bounded_header_text<'a>(
    headers: &'a FieldTable,
    key: &str,
    max_bytes: usize,
) -> Option<&'a str> {
    match headers.inner().get(key)? {
        AMQPValue::LongString(value) if value.len() <= max_bytes => {
            std::str::from_utf8(value.as_bytes()).ok()
        }
        AMQPValue::ShortString(value) if value.as_str().len() <= max_bytes => Some(value.as_str()),
        _ => None,
    }
}

fn canonical_envelope(
    properties: &lapin::BasicProperties,
) -> Result<CanonicalEnvelope<'_>, MessageBrokerError> {
    let headers = properties.headers().as_ref();
    let event_id = headers
        .and_then(|headers| bounded_header_text(headers, "x-lily-event-id", 36))
        .ok_or_else(|| invalid_message("x-lily-event-id is missing"))?;
    let schema_version = headers
        .and_then(|headers| bounded_header_text(headers, "x-lily-schema-version", 5))
        .ok_or_else(|| invalid_message("x-lily-schema-version is missing"))?;
    let content_kind = headers
        .and_then(|headers| {
            bounded_header_text(
                headers,
                "x-lily-content-kind",
                crate::MAX_QUEUE_CONTENT_KIND_BYTES,
            )
        })
        .ok_or_else(|| invalid_message("x-lily-content-kind is missing"))?;

    if event_id.len() != 36 {
        return Err(invalid_message("x-lily-event-id is not a canonical UUID"));
    }
    let parsed_event_id = uuid::Uuid::parse_str(event_id)
        .map_err(|_| invalid_message("x-lily-event-id is not a canonical UUID"))?;
    if parsed_event_id.is_nil() || parsed_event_id.hyphenated().to_string() != event_id {
        return Err(invalid_message("x-lily-event-id is not a canonical UUID"));
    }
    let parsed_schema_version = schema_version
        .parse::<u16>()
        .ok()
        .and_then(|value| SchemaVersion::try_new(value).ok())
        .ok_or_else(|| invalid_message("x-lily-schema-version must be a positive u16"))?;
    if parsed_schema_version.into_inner().to_string() != schema_version {
        return Err(invalid_message(
            "x-lily-schema-version is not canonical decimal",
        ));
    }
    if !valid_content_kind_token(content_kind) {
        return Err(invalid_message("x-lily-content-kind is not canonical"));
    }

    Ok(CanonicalEnvelope {
        event_id,
        schema_version,
        content_kind,
    })
}

fn valid_content_kind_token(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=crate::MAX_QUEUE_CONTENT_KIND_BYTES).contains(&bytes.len())
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().skip(1).all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'+' | b'-')
        })
}

fn canonical_event_id(properties: &lapin::BasicProperties) -> Option<String> {
    canonical_envelope(properties)
        .ok()
        .map(|envelope| envelope.event_id.to_owned())
}

fn canonical_delivery_attempt(properties: &lapin::BasicProperties) -> u32 {
    canonical_retry_count(properties.headers().as_ref())
        .unwrap_or(0)
        .saturating_add(1)
}

fn sanitized_handoff_properties(properties: &lapin::BasicProperties) -> lapin::BasicProperties {
    let mut headers = FieldTable::default();
    if let Ok(envelope) = canonical_envelope(properties) {
        headers.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString(envelope.event_id.into()),
        );
        headers.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString(envelope.schema_version.into()),
        );
        headers.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString(envelope.content_kind.into()),
        );
        if let Ok(retry_count) = canonical_retry_count(properties.headers().as_ref())
            && properties
                .headers()
                .as_ref()
                .is_some_and(|headers| headers.inner().contains_key("x-retry-count"))
        {
            headers.insert(
                "x-retry-count".into(),
                AMQPValue::LongInt(
                    i32::try_from(retry_count)
                        .expect("canonical retry count is bounded by configuration"),
                ),
            );
        }
    }
    lapin::BasicProperties::default().with_headers(headers)
}

fn validate_delivery_envelope(
    properties: &lapin::BasicProperties,
) -> Result<(), MessageBrokerError> {
    canonical_envelope(properties)?;
    canonical_retry_count(properties.headers().as_ref()).map_err(invalid_message)?;
    Ok(())
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_validate_delivery_envelope(
    properties: &lapin::BasicProperties,
) -> Result<(), MessageBrokerError> {
    validate_delivery_envelope(properties)
}

fn delivery_context(
    delivery: &Delivery,
    queue: &str,
) -> Result<DeliveryContext, MessageBrokerError> {
    for (field, value) in [
        ("queue", queue),
        ("exchange", delivery.exchange.as_str()),
        ("routing_key", delivery.routing_key.as_str()),
    ] {
        if !crate::delivery_context::queue_transport_identity_is_valid(value) {
            return Err(invalid_message(format!(
                "{field} identity is outside the bounded transport contract"
            )));
        }
    }

    let headers = delivery.properties.headers().as_ref();
    let event_id = headers
        .and_then(|headers| header_text(headers, "x-lily-event-id"))
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .ok_or_else(|| invalid_message("validated event id could not be materialized"))?;
    let schema_version = headers
        .and_then(|headers| header_text(headers, "x-lily-schema-version"))
        .and_then(|value| value.parse::<u16>().ok())
        .and_then(|value| SchemaVersion::try_new(value).ok())
        .ok_or_else(|| invalid_message("validated schema version could not be materialized"))?;
    let content_kind = headers
        .and_then(|headers| header_text(headers, "x-lily-content-kind"))
        .and_then(|value| ContentKind::try_new(value).ok())
        .ok_or_else(|| invalid_message("validated content kind could not be materialized"))?;

    Ok(DeliveryContext {
        event_id: EventId(event_id),
        schema_version,
        content_kind,
        retry_count: RetryCount(
            canonical_retry_count(headers).map_err(|reason| invalid_message(reason.to_string()))?,
        ),
        redelivered: Redelivered(delivery.redelivered),
        queue: Arc::from(queue),
        exchange: Arc::from(delivery.exchange.as_str()),
        routing_key: Arc::from(delivery.routing_key.as_str()),
    })
}

fn delivery_header_value(value: &AMQPValue) -> DeliveryHeaderValue {
    match value {
        AMQPValue::Boolean(value) => DeliveryHeaderValue::Boolean(*value),
        AMQPValue::ShortShortInt(value) => DeliveryHeaderValue::Signed(i64::from(*value)),
        AMQPValue::ShortShortUInt(value) => DeliveryHeaderValue::Unsigned(u64::from(*value)),
        AMQPValue::ShortInt(value) => DeliveryHeaderValue::Signed(i64::from(*value)),
        AMQPValue::ShortUInt(value) => DeliveryHeaderValue::Unsigned(u64::from(*value)),
        AMQPValue::LongInt(value) => DeliveryHeaderValue::Signed(i64::from(*value)),
        AMQPValue::LongUInt(value) => DeliveryHeaderValue::Unsigned(u64::from(*value)),
        AMQPValue::LongLongInt(value) => DeliveryHeaderValue::Signed(*value),
        AMQPValue::Float(value) => DeliveryHeaderValue::Float(f64::from(*value)),
        AMQPValue::Double(value) => DeliveryHeaderValue::Float(*value),
        AMQPValue::DecimalValue(value) => DeliveryHeaderValue::Decimal {
            scale: value.scale,
            value: value.value,
        },
        AMQPValue::ShortString(value) => DeliveryHeaderValue::Text(Arc::from(value.as_str())),
        AMQPValue::LongString(value) => match std::str::from_utf8(value.as_bytes()) {
            Ok(value) => DeliveryHeaderValue::Text(Arc::from(value)),
            Err(_) => DeliveryHeaderValue::Binary(Bytes::copy_from_slice(value.as_bytes())),
        },
        AMQPValue::FieldArray(values) => DeliveryHeaderValue::Array(
            values
                .as_slice()
                .iter()
                .map(delivery_header_value)
                .collect::<Vec<_>>()
                .into(),
        ),
        AMQPValue::Timestamp(value) => DeliveryHeaderValue::Timestamp(*value),
        AMQPValue::FieldTable(table) => DeliveryHeaderValue::Table(Arc::new(
            table
                .inner()
                .iter()
                .map(|(key, value)| (key.as_str().to_owned(), delivery_header_value(value)))
                .collect(),
        )),
        AMQPValue::ByteArray(value) => {
            DeliveryHeaderValue::Binary(Bytes::copy_from_slice(value.as_slice()))
        }
        AMQPValue::Void => DeliveryHeaderValue::Void,
    }
}

fn delivery_headers(properties: &lapin::BasicProperties) -> DeliveryHeaders {
    let entries = properties
        .headers()
        .as_ref()
        .map(|headers| {
            headers
                .inner()
                .iter()
                .map(|(key, value)| (key.as_str().to_owned(), delivery_header_value(value)))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    DeliveryHeaders::from_entries(entries)
}

fn delivery_properties(properties: &lapin::BasicProperties) -> DeliveryProperties {
    let text = |value: Option<&ShortString>| value.map(|value| Arc::from(value.as_str()));
    DeliveryProperties {
        content_type: text(properties.content_type().as_ref()),
        content_encoding: text(properties.content_encoding().as_ref()),
        correlation_id: text(properties.correlation_id().as_ref()),
        message_id: text(properties.message_id().as_ref()),
        message_type: text(properties.kind().as_ref()),
        reply_to: text(properties.reply_to().as_ref()),
        app_id: text(properties.app_id().as_ref()),
        user_id: text(properties.user_id().as_ref()),
        expiration: text(properties.expiration().as_ref()),
        delivery_mode: properties.delivery_mode().as_ref().copied(),
        priority: properties.priority().as_ref().copied(),
        timestamp: properties.timestamp().as_ref().copied(),
    }
}

fn delivery_input(
    delivery: &Delivery,
    queue: &str,
    body: Vec<u8>,
    cancellation: DeliveryCancellationSource,
    deadline: tokio::time::Instant,
) -> Result<DeliveryInput, MessageBrokerError> {
    Ok(DeliveryInput {
        body: Bytes::from(body),
        context: delivery_context(delivery, queue)?,
        headers: delivery_headers(&delivery.properties),
        properties: delivery_properties(&delivery.properties),
        cancellation,
        deadline,
    })
}

fn header_text<'a>(headers: &'a FieldTable, key: &str) -> Option<&'a str> {
    match headers.inner().get(key)? {
        AMQPValue::LongString(value) => std::str::from_utf8(value.as_bytes()).ok(),
        AMQPValue::ShortString(value) => Some(value.as_str()),
        _ => None,
    }
}

fn invalid_message(reason: impl Into<String>) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(reason.into()))
}

async fn ensure_queue_topology(
    channel: &Channel,
    topology: &QueueTopology,
    setting: &QueueRuntimeSetting,
    terminal_ledger: &DeliveryTerminalLedger,
) -> Result<(), MessageBrokerError> {
    match topology_execution_mode(topology.ownership) {
        TopologyExecutionMode::ActiveDeclare => {
            declare_managed_queue_topology(channel, topology, setting, terminal_ledger).await
        }
        TopologyExecutionMode::PassiveVerify => {
            verify_external_queue_topology(channel, topology, terminal_ledger).await
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopologyExecutionMode {
    ActiveDeclare,
    PassiveVerify,
}

fn topology_execution_mode(ownership: RabbitMqTopologyOwnership) -> TopologyExecutionMode {
    match ownership {
        RabbitMqTopologyOwnership::FrameworkManaged => TopologyExecutionMode::ActiveDeclare,
        RabbitMqTopologyOwnership::External => TopologyExecutionMode::PassiveVerify,
    }
}

fn exchange_kind(kind: RabbitMqExchangeKind) -> ExchangeKind {
    match kind {
        RabbitMqExchangeKind::Direct => ExchangeKind::Direct,
    }
}

fn required_exchanges(topology: &QueueTopology) -> Vec<(&str, ExchangeKind)> {
    let mut exchanges = vec![(
        topology.main_exchange.as_str(),
        exchange_kind(topology.main_exchange_kind),
    )];
    if !topology.retry_buckets.is_empty()
        && !exchanges
            .iter()
            .any(|(name, _)| *name == topology.retry_exchange)
    {
        exchanges.push((topology.retry_exchange.as_str(), ExchangeKind::Direct));
    }
    if !exchanges
        .iter()
        .any(|(name, _)| *name == topology.dead_letter_exchange)
    {
        exchanges.push((topology.dead_letter_exchange.as_str(), ExchangeKind::Direct));
    }
    exchanges
}

async fn declare_managed_queue_topology(
    channel: &Channel,
    topology: &QueueTopology,
    setting: &QueueRuntimeSetting,
    terminal_ledger: &DeliveryTerminalLedger,
) -> Result<(), MessageBrokerError> {
    let exchange_options = ExchangeDeclareOptions {
        durable: true,
        auto_delete: false,
        internal: false,
        nowait: false,
        passive: false,
    };
    for (exchange, kind) in required_exchanges(topology) {
        channel
            .exchange_declare(
                ShortString::from(exchange),
                kind,
                exchange_options,
                FieldTable::default(),
            )
            .await
            .map_err(|error| {
                topology_error(
                    error,
                    RabbitMqTopologyOperation::Declare,
                    RabbitMqTopologyResourceKind::Exchange,
                    exchange,
                )
            })?;
    }

    let mut main_arguments = FieldTable::default();
    insert_main_queue_profile_arguments(&mut main_arguments, topology);
    insert_retention_arguments(
        &mut main_arguments,
        setting.retention.main_max_messages,
        setting.retention.main_max_bytes,
    );
    main_arguments.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString(topology.dead_letter_exchange.clone().into()),
    );
    main_arguments.insert(
        "x-dead-letter-routing-key".into(),
        AMQPValue::LongString(topology.dead_letter_routing_key.clone().into()),
    );
    if let Some(message_ttl_ms) = setting.message_ttl_ms {
        main_arguments.insert(
            "x-message-ttl".into(),
            AMQPValue::LongLongInt(i64::from(message_ttl_ms)),
        );
    }
    let main_queue = channel
        .queue_declare(
            ShortString::from(topology.main_queue.clone()),
            QueueDeclareOptions {
                durable: setting.durable,
                auto_delete: setting.auto_delete,
                exclusive: setting.exclusive,
                nowait: false,
                passive: false,
            },
            main_arguments,
        )
        .await
        .map_err(|error| {
            topology_error(
                error,
                RabbitMqTopologyOperation::Declare,
                RabbitMqTopologyResourceKind::Queue,
                &topology.main_queue,
            )
        })?;
    terminal_ledger.record_broker_state(
        &topology.main_queue,
        main_queue.message_count(),
        main_queue.consumer_count(),
    );
    channel
        .queue_bind(
            ShortString::from(topology.main_queue.clone()),
            ShortString::from(topology.main_exchange.clone()),
            ShortString::from(topology.routing_key.clone()),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|error| {
            topology_error(
                error,
                RabbitMqTopologyOperation::Bind,
                RabbitMqTopologyResourceKind::Binding,
                &topology.main_queue,
            )
        })?;

    for bucket in &topology.retry_buckets {
        let mut retry_arguments = FieldTable::default();
        insert_queue_type_argument(&mut retry_arguments, topology.queue_type);
        retry_arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(topology.main_exchange.clone().into()),
        );
        retry_arguments.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString(topology.routing_key.clone().into()),
        );
        retry_arguments.insert(
            "x-message-ttl".into(),
            AMQPValue::LongLongInt(i64::try_from(bucket.delay.as_millis()).map_err(|_| {
                topology_configuration("retry bucket delay exceeds RabbitMQ range")
            })?),
        );
        insert_retention_arguments(
            &mut retry_arguments,
            setting.retention.retry_bucket_max_messages,
            setting.retention.retry_bucket_max_bytes,
        );
        channel
            .queue_declare(
                ShortString::from(bucket.queue.clone()),
                managed_system_queue_declare_options(),
                retry_arguments,
            )
            .await
            .map_err(|error| {
                topology_error(
                    error,
                    RabbitMqTopologyOperation::Declare,
                    RabbitMqTopologyResourceKind::Queue,
                    &bucket.queue,
                )
            })?;
        channel
            .queue_bind(
                ShortString::from(bucket.queue.clone()),
                ShortString::from(topology.retry_exchange.clone()),
                ShortString::from(bucket.routing_key.clone()),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|error| {
                topology_error(
                    error,
                    RabbitMqTopologyOperation::Bind,
                    RabbitMqTopologyResourceKind::Binding,
                    &bucket.queue,
                )
            })?;
    }

    let mut dead_letter_arguments = FieldTable::default();
    insert_queue_type_argument(&mut dead_letter_arguments, topology.queue_type);
    insert_retention_arguments(
        &mut dead_letter_arguments,
        setting.retention.dead_letter_max_messages,
        setting.retention.dead_letter_max_bytes,
    );
    channel
        .queue_declare(
            ShortString::from(topology.dead_letter_queue.clone()),
            managed_system_queue_declare_options(),
            dead_letter_arguments,
        )
        .await
        .map_err(|error| {
            topology_error(
                error,
                RabbitMqTopologyOperation::Declare,
                RabbitMqTopologyResourceKind::Queue,
                &topology.dead_letter_queue,
            )
        })?;
    channel
        .queue_bind(
            ShortString::from(topology.dead_letter_queue.clone()),
            ShortString::from(topology.dead_letter_exchange.clone()),
            ShortString::from(topology.dead_letter_routing_key.clone()),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|error| {
            topology_error(
                error,
                RabbitMqTopologyOperation::Bind,
                RabbitMqTopologyResourceKind::Binding,
                &topology.dead_letter_queue,
            )
        })?;
    Ok(())
}

async fn verify_external_queue_topology(
    channel: &Channel,
    topology: &QueueTopology,
    terminal_ledger: &DeliveryTerminalLedger,
) -> Result<(), MessageBrokerError> {
    let exchange_options = ExchangeDeclareOptions {
        passive: true,
        ..ExchangeDeclareOptions::default()
    };
    for (exchange, kind) in required_exchanges(topology) {
        channel
            .exchange_declare(
                ShortString::from(exchange),
                kind,
                exchange_options,
                FieldTable::default(),
            )
            .await
            .map_err(|error| {
                topology_error(
                    error,
                    RabbitMqTopologyOperation::Verify,
                    RabbitMqTopologyResourceKind::Exchange,
                    exchange,
                )
            })?;
    }

    let passive_queue_options = QueueDeclareOptions {
        passive: true,
        ..QueueDeclareOptions::default()
    };
    let main_queue = channel
        .queue_declare(
            ShortString::from(topology.main_queue.clone()),
            passive_queue_options,
            FieldTable::default(),
        )
        .await
        .map_err(|error| {
            topology_error(
                error,
                RabbitMqTopologyOperation::Verify,
                RabbitMqTopologyResourceKind::Queue,
                &topology.main_queue,
            )
        })?;
    terminal_ledger.record_broker_state(
        &topology.main_queue,
        main_queue.message_count(),
        main_queue.consumer_count(),
    );

    for bucket in &topology.retry_buckets {
        channel
            .queue_declare(
                ShortString::from(bucket.queue.clone()),
                passive_queue_options,
                FieldTable::default(),
            )
            .await
            .map_err(|error| {
                topology_error(
                    error,
                    RabbitMqTopologyOperation::Verify,
                    RabbitMqTopologyResourceKind::Queue,
                    &bucket.queue,
                )
            })?;
    }
    channel
        .queue_declare(
            ShortString::from(topology.dead_letter_queue.clone()),
            passive_queue_options,
            FieldTable::default(),
        )
        .await
        .map_err(|error| {
            topology_error(
                error,
                RabbitMqTopologyOperation::Verify,
                RabbitMqTopologyResourceKind::Queue,
                &topology.dead_letter_queue,
            )
        })?;

    warn!(
        topology_ownership = "external",
        exchange = %topology.main_exchange,
        queue = %topology.main_queue,
        routing_key = %topology.routing_key,
        binding_verified = false,
        "RabbitMQ passive verification proved resource existence only; binding remains operator-owned and unverified"
    );
    Ok(())
}

fn insert_queue_type_argument(arguments: &mut FieldTable, queue_type: RabbitMqQueueType) {
    let value = match queue_type {
        RabbitMqQueueType::Classic => "classic",
        RabbitMqQueueType::Quorum => "quorum",
    };
    arguments.insert("x-queue-type".into(), AMQPValue::LongString(value.into()));
}

fn managed_system_queue_declare_options() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        auto_delete: false,
        exclusive: false,
        nowait: false,
        passive: false,
    }
}

fn insert_main_queue_profile_arguments(arguments: &mut FieldTable, topology: &QueueTopology) {
    insert_queue_type_argument(arguments, topology.queue_type);
    if topology.single_active_consumer {
        arguments.insert("x-single-active-consumer".into(), AMQPValue::Boolean(true));
    }
    if let Some(max_priority) = topology.max_priority {
        debug_assert_eq!(topology.queue_type, RabbitMqQueueType::Classic);
        arguments.insert(
            "x-max-priority".into(),
            AMQPValue::LongInt(i32::from(max_priority)),
        );
    }
}

fn insert_retention_arguments(arguments: &mut FieldTable, max_messages: u64, max_bytes: u64) {
    arguments.insert(
        "x-max-length".into(),
        AMQPValue::LongLongInt(i64::try_from(max_messages).expect("validated retention count")),
    );
    arguments.insert(
        "x-max-length-bytes".into(),
        AMQPValue::LongLongInt(i64::try_from(max_bytes).expect("validated retention bytes")),
    );
    arguments.insert(
        "x-overflow".into(),
        AMQPValue::LongString("reject-publish".into()),
    );
}

fn topology_configuration(message: impl Into<String>) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(message.into()))
}

fn consumer_registration_panicked(queue: &str) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::ConsumerTaskFailed(
        RabbitMqConsumerTaskFailure {
            queue: queue.to_owned(),
            role: RabbitMqConsumerTaskRole::Supervisor,
            kind: RabbitMqConsumerTaskFailureKind::Panicked,
            operation_error_code: None,
        },
    ))
}

#[derive(Debug)]
struct ConsumerOpenAttemptFailure {
    primary: MessageBrokerError,
    cleanup_unproven: bool,
}

impl ConsumerOpenAttemptFailure {
    fn operation(primary: MessageBrokerError) -> Self {
        Self {
            primary,
            cleanup_unproven: false,
        }
    }

    fn error_code(&self) -> &'static str {
        self.primary.error_code()
    }

    fn into_primary(self) -> MessageBrokerError {
        self.primary
    }
}

async fn cancel_consumer_bounded(
    channel: &Channel,
    consumer_tag: &str,
    timeout: Duration,
    shutdown_budget: &QueueShutdownBudget,
) -> bool {
    let now = tokio::time::Instant::now();
    // Preserve some of the root for channel close even if Cancel never replies.
    let deadline = shutdown_budget.deadlines().map_or(now + timeout, |root| {
        (now + timeout).min(
            root.hard()
                - (root.hard().saturating_duration_since(now) / 4)
                    .min(crate::shutdown_budget::CONSUMER_CHANNEL_CLOSE_RESERVE),
        )
    });
    matches!(
        shutdown_budget
            .run_until(
                deadline,
                AssertUnwindSafe(channel.basic_cancel(
                    ShortString::from(consumer_tag.to_owned()),
                    BasicCancelOptions::default()
                ))
                .catch_unwind()
            )
            .await,
        Ok(Ok(Ok(_)))
    )
}

async fn cancel_and_close_consumer_bounded(
    channel: &Channel,
    consumer_tag: &str,
    timeout: Duration,
    shutdown_budget: &QueueShutdownBudget,
) -> Result<(), MessageBrokerError> {
    let deadline = tokio::time::Instant::now() + timeout;
    cancel_then_close_consumer(
        || cancel_consumer_bounded(channel, consumer_tag, timeout / 2, shutdown_budget),
        || close_consumer_channel_before(channel, deadline, shutdown_budget),
    )
    .await
}

async fn cancel_then_close_consumer<C, CF, L, LF>(
    cancel: C,
    close: L,
) -> Result<(), MessageBrokerError>
where
    C: FnOnce() -> CF,
    CF: Future<Output = bool>,
    L: FnOnce() -> LF,
    LF: Future<Output = Result<(), MessageBrokerError>>,
{
    if !cancel().await {
        warn!(
            error_code = "CONSUMER_CANCEL_FAILED",
            "RabbitMQ consumer cancel was not acknowledged; dedicated channel close remains the cleanup authority"
        );
    }

    // A proven channel close removes every consumer on that channel. It is
    // therefore sufficient even when Basic.Cancel itself failed or timed out.
    close().await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DedicatedConsumerChannelStatus {
    initializing: bool,
    closing: bool,
    connected: bool,
    reconnecting: bool,
}

impl DedicatedConsumerChannelStatus {
    fn capture(status: &lapin::ChannelStatus) -> Self {
        Self {
            initializing: status.initializing(),
            closing: status.closing(),
            connected: status.connected(),
            reconnecting: status.reconnecting(),
        }
    }

    fn proves_terminal_cleanup(self) -> bool {
        !(self.initializing || self.closing || self.connected || self.reconnecting)
    }
}

fn dedicated_consumer_channel_cleanup_proven(status: &lapin::ChannelStatus) -> bool {
    DedicatedConsumerChannelStatus::capture(status).proves_terminal_cleanup()
}

async fn close_consumer_channel_bounded(
    channel: &Channel,
    timeout: Duration,
    shutdown_budget: &QueueShutdownBudget,
) -> Result<(), MessageBrokerError> {
    close_consumer_channel_before(
        channel,
        tokio::time::Instant::now() + timeout,
        shutdown_budget,
    )
    .await
}

async fn close_consumer_channel_before(
    channel: &Channel,
    deadline: tokio::time::Instant,
    shutdown_budget: &QueueShutdownBudget,
) -> Result<(), MessageBrokerError> {
    if dedicated_consumer_channel_cleanup_proven(channel.status()) {
        return Ok(());
    }

    let close_result = shutdown_budget
        .run_until(
            deadline,
            AssertUnwindSafe(channel.close(200, ShortString::from("consumer lifecycle cleanup")))
                .catch_unwind(),
        )
        .await;

    // `ChannelStatus::connected()` alone is not a terminal-cleanup proof:
    // Initial, Closing and Reconnecting are all disconnected while the
    // channel may still become live or retain an accepted consumer. Only the
    // predicate combination which represents Lapin's Closed/Error states can
    // release the physical-queue registration tombstone.
    if dedicated_consumer_channel_cleanup_proven(channel.status()) {
        return Ok(());
    }

    match close_result {
        Ok(_) => {
            warn!(
                error_code = "CONSUMER_CHANNEL_CLOSE_FAILED",
                "RabbitMQ dedicated consumer channel cleanup was not proven"
            );
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(
                "dedicated consumer channel cleanup was not acknowledged".into(),
            )))
        }
        Err(_) => {
            warn!(
                error_code = "CONSUMER_CHANNEL_CLOSE_TIMEOUT",
                "RabbitMQ dedicated consumer channel cleanup timed out"
            );
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
                "dedicated consumer channel cleanup".into(),
            )))
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the startup/recovery boundary keeps broker identity, topology, policy, cancellation and typed timeout evidence explicit"
)]
async fn open_consumer_bounded(
    channel_manager: &Arc<dyn ChannelManager<Channel>>,
    worker: &str,
    queue: &str,
    topology: &QueueTopology,
    setting: &QueueRuntimeSetting,
    cancellation: &CancellationToken,
    timeout: Duration,
    timeout_category: &'static str,
    terminal_ledger: &DeliveryTerminalLedger,
    shutdown_budget: &QueueShutdownBudget,
    ownership: &ConsumerChannelOwnership,
) -> Result<(Channel, lapin::Consumer, String), ConsumerOpenAttemptFailure> {
    let deadline = tokio::time::Instant::now() + timeout;
    let channel = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            return Err(ConsumerOpenAttemptFailure::operation(
                MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled),
            ));
        }
        result = shutdown_budget.run_until(
            deadline,
            AssertUnwindSafe(
                channel_manager.get_dedicated_channel(worker, cancellation.clone(), false),
            )
            .catch_unwind(),
        ) => {
            match result {
                Ok(Ok(Ok(channel))) => channel,
                Ok(Ok(Err(error))) => {
                    return Err(ConsumerOpenAttemptFailure::operation(error));
                }
                Ok(Err(_)) => {
                    return Err(ConsumerOpenAttemptFailure::operation(
                        consumer_registration_panicked(queue),
                    ));
                }
                Err(_) => {
                    return Err(ConsumerOpenAttemptFailure::operation(
                        MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
                            timeout_category.into(),
                        )),
                    ));
                }
            }
        }
    };

    ownership.adopt(&channel);

    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled))
        }
        result = shutdown_budget.run_until(
            deadline,
            AssertUnwindSafe(open_consumer_on_channel(
                    &channel,
                    worker,
                    queue,
                    topology,
                    setting,
                    terminal_ledger,
                ))
                .catch_unwind(),
        ) => {
            match result {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(consumer_registration_panicked(queue)),
                Err(_) => Err(MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
                    timeout_category.into(),
                ))),
            }
        }
    };

    if let Ok((consumer, _)) = &result {
        ownership.retain_consumer(consumer);
    }

    #[cfg(feature = "test-support")]
    let result = match result {
        Ok(opened) => registration_handoff_test_support::accepted(queue, &channel, cancellation)
            .await
            .map(|()| opened),
        Err(error) => Err(error),
    };

    match result {
        Ok((consumer, tag)) => Ok((channel, consumer, tag)),
        Err(primary) => {
            let cleanup_unproven = close_consumer_channel_bounded(
                &channel,
                setting.settlement_timeout,
                &shutdown_budget,
            )
            .await
            .is_err();
            ownership.release_if_terminal();
            Err(ConsumerOpenAttemptFailure {
                primary,
                cleanup_unproven,
            })
        }
    }
}

async fn execute_topology_plan_bounded(
    channel_manager: &Arc<dyn ChannelManager<Channel>>,
    plan: &RabbitMqTopologyPlan,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> Result<RabbitMqTopologyBootstrapReport, MessageBrokerError> {
    if plan.is_empty() {
        return Ok(RabbitMqTopologyBootstrapReport::default());
    }

    tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled))
        }
        result = tokio::time::timeout(timeout, async {
            let channel = channel_manager
                .get_channel("lily.consumer.topology-bootstrap", cancellation.clone(), false)
                .await?;
            execute_rabbitmq_topology_plan(&channel, plan).await
        }) => {
            result.map_err(|_| {
                MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
                    "consumer topology bootstrap".into(),
                ))
            })?
        }
    }
}

async fn open_consumer_on_channel(
    channel: &Channel,
    worker: &str,
    queue: &str,
    topology: &QueueTopology,
    setting: &QueueRuntimeSetting,
    terminal_ledger: &DeliveryTerminalLedger,
) -> Result<(lapin::Consumer, String), MessageBrokerError> {
    ensure_queue_topology(channel, topology, setting, terminal_ledger).await?;
    channel
        .basic_qos(
            rabbitmq_prefetch(setting),
            BasicQosOptions { global: false },
        )
        .await
        .map_err(|error| {
            MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(format!(
                "BasicQos failed: {error}"
            )))
        })?;
    let tag = format!("{worker}-{}", uuid::Uuid::new_v4());
    let consumer = channel
        .basic_consume(
            ShortString::from(queue),
            ShortString::from(tag.clone()),
            BasicConsumeOptions {
                no_ack: false,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|error| {
            MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(format!(
                "BasicConsume failed: {error}"
            )))
        })?;
    Ok((consumer, tag))
}

fn topology_error(
    error: lapin::Error,
    operation: RabbitMqTopologyOperation,
    resource_kind: RabbitMqTopologyResourceKind,
    resource_name: &str,
) -> MessageBrokerError {
    let kind = match error.kind() {
        lapin::ErrorKind::ProtocolError(protocol) => match protocol.kind() {
            AMQPErrorKind::Soft(AMQPSoftError::ACCESSREFUSED) => {
                Some(RabbitMqTopologyErrorKind::PermissionDenied)
            }
            AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND)
                if operation == RabbitMqTopologyOperation::Verify =>
            {
                Some(RabbitMqTopologyErrorKind::PassiveResourceNotFound)
            }
            AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED) => {
                Some(RabbitMqTopologyErrorKind::DeclarationMismatch)
            }
            _ => None,
        },
        _ => None,
    };

    if let Some(kind) = kind {
        MessageBrokerError::RabbitMQError(RabbitMQError::Topology(RabbitMqTopologyError {
            kind,
            operation,
            resource_kind,
            resource_name: resource_name.to_owned(),
        }))
    } else {
        MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(format!(
            "RabbitMQ topology {operation} failed for {resource_kind} {resource_name:?}"
        )))
    }
}

#[cfg(test)]
mod propagation_tests {
    use super::*;
    use async_trait::async_trait;
    use lily_config::{QueueDefinition, QueueRetentionConfig};
    use opentelemetry::trace::TraceContextExt;
    use std::sync::{
        Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct DropSignal(Arc<AtomicBool>);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedHandoff {
        worker: String,
        queue: String,
        failure_class: FailureClass,
        failure_code: &'static str,
    }

    type HandoffCalls = Arc<StdMutex<Vec<RecordedHandoff>>>;

    struct RecordingRetryEngine {
        calls: HandoffCalls,
        fail: bool,
    }

    struct UnusedChannelManager;

    #[derive(Default)]
    struct FailingBootstrapChannelManager {
        calls: AtomicUsize,
    }

    #[derive(Default)]
    struct FailingDedicatedChannelManager {
        shared_calls: AtomicUsize,
        dedicated_calls: AtomicUsize,
    }

    #[derive(Default)]
    struct PanickingDedicatedChannelManager {
        dedicated_calls: AtomicUsize,
    }

    #[async_trait]
    impl ChannelManager<Channel> for UnusedChannelManager {
        async fn get_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            unreachable!("drain-only tests must not acquire a RabbitMQ channel")
        }

        async fn get_dedicated_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            unreachable!("drain-only tests must not acquire a RabbitMQ channel")
        }
    }

    #[async_trait]
    impl ChannelManager<Channel> for FailingBootstrapChannelManager {
        async fn get_channel(
            &self,
            _exchange_name: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "injected topology bootstrap failure".into(),
            )))
        }

        async fn get_dedicated_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            unreachable!("topology bootstrap failure must precede consumer channel acquisition")
        }
    }

    #[async_trait]
    impl ChannelManager<Channel> for FailingDedicatedChannelManager {
        async fn get_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            self.shared_calls.fetch_add(1, Ordering::SeqCst);
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "shared channel must not serve Basic.Consume".into(),
            )))
        }

        async fn get_dedicated_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            self.dedicated_calls.fetch_add(1, Ordering::SeqCst);
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "injected dedicated channel acquisition failure".into(),
            )))
        }
    }

    #[async_trait]
    impl ChannelManager<Channel> for PanickingDedicatedChannelManager {
        async fn get_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            unreachable!("consumer registration must use a dedicated channel")
        }

        async fn get_dedicated_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            self.dedicated_calls.fetch_add(1, Ordering::SeqCst);
            panic!("injected dedicated channel acquisition panic")
        }
    }

    #[async_trait]
    impl RetryEngine<Delivery> for RecordingRetryEngine {
        async fn retry(
            &self,
            worker: &str,
            queue: &str,
            _body: &[u8],
            _delivery: &Delivery,
            plan: HandoffPlan,
            _ct: CancellationToken,
        ) -> Result<RetryHandoff, MessageBrokerError> {
            self.calls
                .lock()
                .expect("recording retry engine lock")
                .push(RecordedHandoff {
                    worker: worker.to_owned(),
                    queue: queue.to_owned(),
                    failure_class: plan.failure_class(),
                    failure_code: plan.failure_code(),
                });
            if self.fail {
                Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                    "injected handoff failure".into(),
                )))
            } else {
                Ok(RetryHandoff::DeadLetterConfirmed)
            }
        }
    }

    fn recording_retry_engine(fail: bool) -> (Arc<dyn RetryEngine<Delivery>>, HandoffCalls) {
        let calls = Arc::new(StdMutex::new(Vec::new()));
        (
            Arc::new(RecordingRetryEngine {
                calls: Arc::clone(&calls),
                fail,
            }),
            calls,
        )
    }

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn extracts_w3c_context_from_amqp_headers() {
        lily_trace::install_w3c_propagator();
        let mut headers = FieldTable::default();
        headers.insert(
            "traceparent".into(),
            AMQPValue::LongString("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        );
        headers.insert(
            "tracestate".into(),
            AMQPValue::LongString("vendor=opaque".into()),
        );

        let context = lily_trace::extract_context(&AmqpHeaderExtractor(&headers));
        let span = context.span();
        let span_context = span.span_context();
        assert!(span_context.is_remote());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
        assert_eq!(span_context.trace_state().header(), "vendor=opaque");
    }

    #[test]
    fn consume_spans_and_metrics_never_declare_payload_credentials_or_id_labels() {
        let source = include_str!("queue_engine.rs")
            .split("\n#[cfg(test)]\nmod propagation_tests")
            .next()
            .expect("production source");
        for forbidden in [
            "otel.status_message =",
            "messaging.message.payload =",
            "authorization =",
            "password =",
            "\"lily.event_id\" =>",
            "\"messaging.message.id\" =>",
        ] {
            assert!(
                !source.contains(forbidden),
                "forbidden telemetry declaration: {forbidden}"
            );
        }
        assert!(source.contains("\"messaging.consume\""));
        assert!(source.contains("\"messaging.consume.handler\""));
        assert!(source.contains("\"messaging.consume.handoff\""));
    }

    #[test]
    fn framework_owned_queue_arguments_are_bounded_and_reject_new_publishes() {
        let mut arguments = FieldTable::default();
        insert_retention_arguments(&mut arguments, 100, 4096);

        assert_eq!(
            arguments.inner().get("x-max-length"),
            Some(&AMQPValue::LongLongInt(100))
        );
        assert_eq!(
            arguments.inner().get("x-max-length-bytes"),
            Some(&AMQPValue::LongLongInt(4096))
        );
        assert_eq!(
            arguments.inner().get("x-overflow"),
            Some(&AMQPValue::LongString("reject-publish".into()))
        );
    }

    #[test]
    fn envelope_validation_is_fail_closed_and_accepts_canonical_non_json_contracts() {
        assert!(validate_delivery_envelope(&lapin::BasicProperties::default()).is_err());

        let mut canonical = FieldTable::default();
        canonical.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString(uuid::Uuid::new_v4().hyphenated().to_string().into()),
        );
        canonical.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("1".into()),
        );
        canonical.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString("json".into()),
        );
        assert!(
            validate_delivery_envelope(&lapin::BasicProperties::default().with_headers(canonical))
                .is_ok()
        );

        let mut partial = FieldTable::default();
        partial.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("1".into()),
        );
        assert!(
            validate_delivery_envelope(&lapin::BasicProperties::default().with_headers(partial))
                .is_err()
        );

        let mut binary = FieldTable::default();
        binary.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString(uuid::Uuid::new_v4().to_string().into()),
        );
        binary.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("2".into()),
        );
        binary.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString("binary".into()),
        );
        assert!(
            validate_delivery_envelope(&lapin::BasicProperties::default().with_headers(binary))
                .is_ok()
        );

        let mut invalid_kind = FieldTable::default();
        invalid_kind.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString(uuid::Uuid::new_v4().to_string().into()),
        );
        invalid_kind.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("1".into()),
        );
        invalid_kind.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString("Invalid Kind".into()),
        );
        assert!(
            validate_delivery_envelope(
                &lapin::BasicProperties::default().with_headers(invalid_kind)
            )
            .is_err()
        );

        for (event_id, schema_version) in [
            (uuid::Uuid::nil().hyphenated().to_string(), "1"),
            (uuid::Uuid::new_v4().hyphenated().to_string(), "01"),
            (uuid::Uuid::new_v4().hyphenated().to_string(), "00001"),
        ] {
            let mut non_canonical = FieldTable::default();
            non_canonical.insert(
                "x-lily-event-id".into(),
                AMQPValue::LongString(event_id.into()),
            );
            non_canonical.insert(
                "x-lily-schema-version".into(),
                AMQPValue::LongString(schema_version.into()),
            );
            non_canonical.insert(
                "x-lily-content-kind".into(),
                AMQPValue::LongString("json".into()),
            );
            assert!(
                validate_delivery_envelope(
                    &lapin::BasicProperties::default().with_headers(non_canonical)
                )
                .is_err()
            );
        }
    }

    #[test]
    fn body_limit_precedes_metadata_and_envelope_admission() {
        let invalid_properties = lapin::BasicProperties::default();
        assert_eq!(
            transport_admission_failure(2, 1, &invalid_properties),
            Some(TransportAdmissionFailure::PayloadTooLarge)
        );
        assert_eq!(
            transport_admission_failure(1, 1, &invalid_properties),
            Some(TransportAdmissionFailure::InvalidEnvelope)
        );
    }

    #[test]
    fn oversized_body_uses_one_non_requeueing_broker_dead_letter_settlement() {
        let options = oversized_delivery_nack_options();
        assert!(!options.multiple);
        assert!(!options.requeue);
    }

    #[test]
    fn deferred_or_failed_handoff_uses_one_requeueing_nack_settlement() {
        let options = requeue_delivery_nack_options();
        assert!(!options.multiple);
        assert!(options.requeue);
    }

    #[test]
    fn oversized_metadata_handoff_retains_only_bounded_canonical_identity() {
        const EVENT_ID: &str = "11111111-1111-4111-8111-111111111111";
        let mut headers = FieldTable::default();
        headers.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString(EVENT_ID.into()),
        );
        headers.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("1".into()),
        );
        headers.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString("json".into()),
        );
        headers.insert("x-retry-count".into(), AMQPValue::LongInt(2));
        for index in 0..=MAX_AMQP_HEADER_ENTRIES {
            headers.insert(
                format!("untrusted-{index}").into(),
                AMQPValue::LongString("PRIVATE-METADATA-SENTINEL".into()),
            );
        }
        let properties = properties_with_headers(headers);
        assert_eq!(
            transport_admission_failure(1, 1, &properties),
            Some(TransportAdmissionFailure::MetadataBoundsExceeded)
        );

        let sanitized = sanitized_handoff_properties(&properties);
        assert!(validate_amqp_metadata(&sanitized).is_ok());
        let envelope = canonical_envelope(&sanitized).unwrap();
        assert_eq!(envelope.event_id, EVENT_ID);
        assert_eq!(envelope.schema_version, "1");
        assert_eq!(envelope.content_kind, "json");
        let sanitized_headers = sanitized.headers().as_ref().unwrap();
        assert_eq!(sanitized_headers.inner().len(), 4);
        assert_eq!(
            sanitized_headers.inner().get("x-retry-count"),
            Some(&AMQPValue::LongInt(2))
        );
        assert!(
            sanitized_headers
                .inner()
                .keys()
                .all(|key| !key.as_str().starts_with("untrusted-"))
        );

        let mut oversized_identity = FieldTable::default();
        oversized_identity.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString("f".repeat(MAX_AMQP_VALUE_BYTES + 1).into()),
        );
        oversized_identity.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("1".into()),
        );
        oversized_identity.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString("json".into()),
        );
        let oversized_identity = properties_with_headers(oversized_identity);
        assert_eq!(
            transport_admission_failure(1, 1, &oversized_identity),
            Some(TransportAdmissionFailure::MetadataBoundsExceeded)
        );
        assert!(canonical_event_id(&oversized_identity).is_none());
        assert!(
            sanitized_handoff_properties(&oversized_identity)
                .headers()
                .as_ref()
                .is_some_and(|headers| headers.inner().is_empty())
        );
    }

    pub(super) fn canonical_delivery(exchange: &str, routing_key: &str) -> Delivery {
        let mut headers = FieldTable::default();
        headers.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString(uuid::Uuid::new_v4().hyphenated().to_string().into()),
        );
        headers.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("1".into()),
        );
        headers.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString("json".into()),
        );
        let mut delivery =
            Delivery::mock(1, exchange.into(), routing_key.into(), false, Vec::new());
        delivery.properties = lapin::BasicProperties::default().with_headers(headers);
        delivery
    }

    #[test]
    fn delivery_context_rejects_unbounded_or_control_bearing_transport_identities() {
        let exact = "q".repeat(crate::delivery_context::MAX_QUEUE_TRANSPORT_IDENTITY_BYTES);
        let delivery = canonical_delivery(&exact, &exact);
        assert!(delivery_context(&delivery, &exact).is_ok());

        let oversized = "q".repeat(crate::delivery_context::MAX_QUEUE_TRANSPORT_IDENTITY_BYTES + 1);
        for (queue, exchange, routing_key) in [
            (oversized.as_str(), "exchange", "routing"),
            ("queue", oversized.as_str(), "routing"),
            ("queue", "exchange", oversized.as_str()),
            ("queue\nforged", "exchange", "routing"),
            ("queue", "exchange\rforged", "routing"),
            ("queue", "exchange", "routing\tforged"),
        ] {
            let delivery = canonical_delivery(exchange, routing_key);
            assert!(delivery_context(&delivery, queue).is_err());
        }
    }

    #[test]
    fn retry_count_policy_is_identical_for_admission_and_context_materialization() {
        for retry_count in [0, crate::setting::MAX_QUEUE_RETRY_ATTEMPTS] {
            let mut delivery = canonical_delivery("exchange", "route");
            let mut headers = delivery.properties.headers().as_ref().unwrap().clone();
            headers.insert(
                "x-retry-count".into(),
                AMQPValue::LongInt(i32::try_from(retry_count).unwrap()),
            );
            delivery.properties = delivery.properties.clone().with_headers(headers);

            assert!(validate_delivery_envelope(&delivery.properties).is_ok());
            assert_eq!(
                delivery_context(&delivery, "queue")
                    .unwrap()
                    .retry_count()
                    .into_inner(),
                retry_count
            );
            assert_eq!(
                canonical_delivery_attempt(&delivery.properties),
                retry_count + 1
            );
        }

        for invalid in [
            AMQPValue::LongInt(-1),
            AMQPValue::LongInt(
                i32::try_from(crate::setting::MAX_QUEUE_RETRY_ATTEMPTS + 1).unwrap(),
            ),
            AMQPValue::LongString("1".into()),
        ] {
            let mut delivery = canonical_delivery("exchange", "route");
            let mut headers = delivery.properties.headers().as_ref().unwrap().clone();
            headers.insert("x-retry-count".into(), invalid);
            delivery.properties = delivery.properties.clone().with_headers(headers);

            assert!(validate_delivery_envelope(&delivery.properties).is_err());
            assert!(delivery_context(&delivery, "queue").is_err());
            assert_eq!(canonical_delivery_attempt(&delivery.properties), 1);
        }
    }

    #[tokio::test]
    async fn invalid_transport_identity_is_dead_lettered_once_and_next_delivery_is_accepted() {
        let oversized = "x".repeat(crate::delivery_context::MAX_QUEUE_TRANSPORT_IDENTITY_BYTES + 1);
        let invalid = canonical_delivery(&oversized, "route");
        let invalid_acker = invalid.acker.clone();
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let (retry_engine, calls) = recording_retry_engine(false);
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);

        let keep_receiving = RabbitMQQueueEngine::materialize_and_buffer_delivery(
            "worker",
            "queue",
            invalid,
            &tx,
            &CancellationToken::new(),
            &retry_engine,
            &ledger,
            0,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &QueueShutdownBudget::default(),
        )
        .await
        .expect("confirmed invalid-identity handoff");

        assert!(keep_receiving);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(!invalid_acker.usable());
        assert_eq!(
            calls.lock().expect("recorded handoff calls").as_slice(),
            &[RecordedHandoff {
                worker: "worker".into(),
                queue: "queue".into(),
                failure_class: FailureClass::Permanent,
                failure_code: "invalid_transport_identity",
            }]
        );
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.acked_confirmed_handoff, 1);
        assert_eq!(snapshot.dead_letter_confirmed, 1);
        assert_eq!(snapshot.unacked_or_in_flight(), 0);
        assert!(snapshot.is_reconciled());

        let healthy = canonical_delivery("exchange", "route");
        let keep_receiving = RabbitMQQueueEngine::materialize_and_buffer_delivery(
            "worker",
            "queue",
            healthy,
            &tx,
            &CancellationToken::new(),
            &retry_engine,
            &ledger,
            0,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &QueueShutdownBudget::default(),
        )
        .await
        .expect("healthy delivery admission");
        assert!(keep_receiving);
        let mut accepted = rx
            .try_recv()
            .expect("healthy delivery must reach the dispatch buffer");
        accepted.attempt_guard.finish_buffered_pending_redelivery();

        assert_eq!(calls.lock().expect("recorded handoff calls").len(), 1);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 2);
        assert_eq!(snapshot.buffered_pending_redelivery, 1);
        assert_eq!(snapshot.unacked_or_in_flight(), 0);
        assert!(snapshot.is_reconciled());
    }

    #[tokio::test]
    async fn invalid_transport_identity_handoff_failure_nacks_once_and_is_typed() {
        let invalid = canonical_delivery("exchange\nforged", "route");
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let (retry_engine, calls) = recording_retry_engine(true);

        let error = match RabbitMQQueueEngine::materialize_delivery_context_or_settle(
            "worker",
            "queue",
            &invalid,
            &CancellationToken::new(),
            &retry_engine,
            &ledger,
            0,
            Duration::from_secs(1),
            Duration::from_secs(1),
            &QueueShutdownBudget::default(),
        )
        .await
        {
            Ok(_) => panic!("failed handoff must return its typed error"),
            Err(error) => error,
        };

        assert_eq!(error.error_code(), "BROKER_GENERAL");
        assert_eq!(calls.lock().expect("recorded handoff calls").len(), 1);
        assert!(!invalid.acker.usable());
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.nacked_or_requeued, 1);
        assert_eq!(snapshot.acked_confirmed_handoff, 0);
        assert_eq!(snapshot.unacked_or_in_flight(), 0);
        assert!(snapshot.is_reconciled());
    }

    fn nested_array(depth: usize) -> AMQPValue {
        let mut value = AMQPValue::Void;
        for _ in 0..depth {
            value = AMQPValue::FieldArray(vec![value].into());
        }
        value
    }

    fn properties_with_headers(headers: FieldTable) -> lapin::BasicProperties {
        lapin::BasicProperties::default().with_headers(headers)
    }

    #[test]
    fn amqp_metadata_admission_enforces_header_count_key_and_value_bounds() {
        let mut exact_count = FieldTable::default();
        for index in 0..MAX_AMQP_HEADER_ENTRIES {
            exact_count.insert(format!("h{index}").into(), AMQPValue::Void);
        }
        assert!(validate_amqp_metadata(&properties_with_headers(exact_count.clone())).is_ok());
        exact_count.insert("overflow".into(), AMQPValue::Void);
        assert!(validate_amqp_metadata(&properties_with_headers(exact_count)).is_err());

        let mut exact_key = FieldTable::default();
        exact_key.insert(
            "k".repeat(MAX_AMQP_HEADER_KEY_BYTES).into(),
            AMQPValue::Void,
        );
        assert!(validate_amqp_metadata(&properties_with_headers(exact_key)).is_ok());
        let mut oversized_key = FieldTable::default();
        oversized_key.insert(
            "k".repeat(MAX_AMQP_HEADER_KEY_BYTES + 1).into(),
            AMQPValue::Void,
        );
        assert!(validate_amqp_metadata(&properties_with_headers(oversized_key)).is_err());

        let mut exact_value = FieldTable::default();
        exact_value.insert(
            "v".into(),
            AMQPValue::LongString(vec![b'x'; MAX_AMQP_VALUE_BYTES].into()),
        );
        assert!(validate_amqp_metadata(&properties_with_headers(exact_value)).is_ok());
        let mut oversized_value = FieldTable::default();
        oversized_value.insert(
            "v".into(),
            AMQPValue::LongString(vec![b'x'; MAX_AMQP_VALUE_BYTES + 1].into()),
        );
        assert!(validate_amqp_metadata(&properties_with_headers(oversized_value)).is_err());
    }

    #[test]
    fn amqp_metadata_admission_enforces_aggregate_depth_and_element_bounds() {
        let mut exact_aggregate = FieldTable::default();
        for key in ["a", "b", "c", "d"] {
            exact_aggregate.insert(
                key.into(),
                AMQPValue::LongString(vec![b'x'; MAX_AMQP_VALUE_BYTES - 1].into()),
            );
        }
        assert_eq!(4 * MAX_AMQP_VALUE_BYTES, MAX_AMQP_METADATA_BYTES);
        assert!(validate_amqp_metadata(&properties_with_headers(exact_aggregate)).is_ok());

        let mut oversized_aggregate = FieldTable::default();
        for key in ["a", "b", "c"] {
            oversized_aggregate.insert(
                key.into(),
                AMQPValue::LongString(vec![b'x'; MAX_AMQP_VALUE_BYTES - 1].into()),
            );
        }
        oversized_aggregate.insert(
            "d".into(),
            AMQPValue::LongString(vec![b'x'; MAX_AMQP_VALUE_BYTES].into()),
        );
        assert!(validate_amqp_metadata(&properties_with_headers(oversized_aggregate)).is_err());

        let mut exact_depth = FieldTable::default();
        exact_depth.insert("nested".into(), nested_array(MAX_AMQP_NESTING_DEPTH));
        assert!(validate_amqp_metadata(&properties_with_headers(exact_depth)).is_ok());
        let mut oversized_depth = FieldTable::default();
        oversized_depth.insert("nested".into(), nested_array(MAX_AMQP_NESTING_DEPTH + 1));
        assert!(validate_amqp_metadata(&properties_with_headers(oversized_depth)).is_err());

        let mut exact_elements = FieldTable::default();
        exact_elements.insert(
            "items".into(),
            AMQPValue::FieldArray(vec![AMQPValue::Void; MAX_AMQP_NESTED_ELEMENTS - 1].into()),
        );
        assert!(validate_amqp_metadata(&properties_with_headers(exact_elements)).is_ok());
        let mut oversized_elements = FieldTable::default();
        oversized_elements.insert(
            "items".into(),
            AMQPValue::FieldArray(vec![AMQPValue::Void; MAX_AMQP_NESTED_ELEMENTS].into()),
        );
        assert!(validate_amqp_metadata(&properties_with_headers(oversized_elements)).is_err());
    }

    #[tokio::test]
    async fn already_consumed_acker_is_not_misclassified_as_successful_settlement() {
        let delivery = canonical_delivery("exchange", "route");
        assert!(
            delivery
                .ack(BasicAckOptions::default())
                .await
                .expect("first mock settlement")
        );

        assert!(
            !delivery
                .nack(BasicNackOptions::default())
                .await
                .expect("consumed mock acker reports a declined second settlement")
        );
    }

    pub(super) async fn started_lifecycle_engine() -> RabbitMQQueueEngine {
        let setting = MessageBrokerSetting::from_definitions(Duration::from_secs(1), &[])
            .expect("empty test topology");
        let (retry_engine, _) = recording_retry_engine(false);
        let engine =
            RabbitMQQueueEngine::new(setting, Arc::new(UnusedChannelManager), retry_engine);
        engine
            .start(CancellationToken::new())
            .await
            .expect("start engine");
        engine
    }

    #[tokio::test]
    async fn graceful_admission_closure_preserves_accepted_execution_and_settlement() {
        let engine = started_lifecycle_engine().await;
        let tokens = engine
            .runtime_tokens
            .lock()
            .await
            .clone()
            .expect("started tokens");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let observed = Arc::new(StdMutex::new(None));
        let handler: QueueDeliveryHandler = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let observed = Arc::clone(&observed);
            Arc::new(move |input| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    *observed.lock().expect("observed token") = Some(input.cancellation.clone());
                    entered.notify_one();
                    release.notified().await;
                    assert!(
                        !input.cancellation.is_cancelled(),
                        "admission is not execution cancellation"
                    );
                    Ok(())
                })
            })
        };
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        for _ in 0..2 {
            RabbitMQQueueEngine::materialize_and_buffer_delivery(
                "worker",
                "queue",
                canonical_delivery("exchange", "route"),
                &tx,
                &tokens.settlement,
                &engine.retry_engine,
                &engine.terminal_ledger,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                &QueueShutdownBudget::default(),
            )
            .await
            .expect("buffer delivery");
        }
        let dispatcher = tokio::spawn(engine.dispatch_loop(
            "worker",
            "queue",
            handler,
            rx,
            tokens.admission.clone(),
            tokens.force.clone(),
            tokens.settlement.clone(),
            DispatchPolicy {
                handler_concurrency: 1,
                retry_attempts: 0,
                delivery_execution_timeout: Duration::from_secs(10),
                handoff_timeout: Duration::from_secs(1),
                settlement_timeout: Duration::from_secs(1),
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("first delivery started");
        engine.stop_admission().await.expect("close admission");
        assert!(tokens.admission.is_cancelled());
        assert!(!tokens.settlement.is_cancelled());
        assert!(!engine.cleanup.is_cancelled());
        assert_eq!(
            observed
                .lock()
                .expect("observed token")
                .as_ref()
                .unwrap()
                .reason(),
            None
        );
        assert!(
            !dispatcher.is_finished(),
            "graceful drain must await the accepted handler"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), dispatcher)
            .await
            .expect("dispatcher drain is bounded")
            .expect("join dispatcher")
            .expect("handler and mock settlement complete");
        let snapshot = engine.terminal_ledger.snapshot();
        assert_eq!(snapshot.handler_success, 1);
        assert_eq!(snapshot.acked_handler_success, 1);
        assert_eq!(snapshot.buffered_pending_redelivery, 1);
        assert_eq!(snapshot.unacked_or_in_flight(), 0);
        assert!(snapshot.is_reconciled());
    }

    #[tokio::test]
    async fn execution_cancellation_is_independent_of_cleanup_and_settlement() {
        let engine = started_lifecycle_engine().await;
        let tokens = engine
            .runtime_tokens
            .lock()
            .await
            .clone()
            .expect("started tokens");
        let first_delivery = engine.execution.child();
        let second_delivery = engine.execution.child();
        first_delivery.cancel(DeliveryCancellationReason::DeliveryTimeout);
        assert!(!second_delivery.is_cancelled());
        assert!(!engine.execution.is_cancelled());
        engine.cancel_execution(DeliveryCancellationReason::ForcedShutdown);
        assert!(first_delivery.is_cancelled());
        assert!(second_delivery.is_cancelled());
        assert_eq!(
            first_delivery.reason(),
            Some(DeliveryCancellationReason::DeliveryTimeout)
        );
        assert_eq!(
            second_delivery.reason(),
            Some(DeliveryCancellationReason::ForcedShutdown)
        );
        assert!(!tokens.settlement.is_cancelled());
        assert!(!engine.cleanup.is_cancelled());
        // Each cleanup invocation gets a sibling child, never the root authority.
        let first_cleanup = engine.cleanup.child_token();
        let second_cleanup = engine.cleanup.child_token();
        first_cleanup.cancel();
        assert!(!second_cleanup.is_cancelled());
        assert!(!engine.cleanup.is_cancelled());
        engine.finish_cleanup();
        assert!(second_cleanup.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn force_notifies_execution_before_abort_and_joins_the_terminated_task() {
        struct RecordCancellationOnDrop {
            source: DeliveryCancellationSource,
            observed: Arc<StdMutex<Option<DeliveryCancellationReason>>>,
        }
        impl Drop for RecordCancellationOnDrop {
            fn drop(&mut self) {
                assert!(
                    self.source.is_cancelled(),
                    "abort must follow cancellation notification"
                );
                *self.observed.lock().expect("drop evidence") = self.source.reason();
            }
        }
        let execution = DeliveryCancellationSource::new();
        let observed = Arc::new(StdMutex::new(None));
        let witness = RecordCancellationOnDrop {
            source: execution.child(),
            observed: Arc::clone(&observed),
        };
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let _witness = witness;
            started_tx.send(()).expect("notify started");
            std::future::pending::<Result<(), &'static str>>().await
        });
        started_rx
            .await
            .expect("execution must be polled before force");
        let force = CancellationToken::new();
        execution.cancel(DeliveryCancellationReason::ForcedShutdown);
        force.cancel();
        let notified_at = tokio::time::Instant::now();
        RabbitMQQueueEngine::finish_in_flight_tasks(
            &mut tasks,
            false,
            None,
            Some(&force),
            Some(&execution),
            None,
            None,
        )
        .await
        .expect("abort and join");
        assert_eq!(
            tokio::time::Instant::now() - notified_at,
            Duration::from_secs(1)
        );
        assert!(tasks.is_empty(), "abort request alone is insufficient");
        assert_eq!(
            *observed.lock().expect("drop evidence"),
            Some(DeliveryCancellationReason::ForcedShutdown)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn force_drain_polls_cooperative_delivery_to_completion_before_final_abort() {
        let execution = DeliveryCancellationSource::new();
        let child = execution.child();
        let completed = Arc::new(AtomicBool::new(false));
        let task_completed = completed.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            child.cancelled().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            task_completed.store(true, Ordering::Release);
            Ok(())
        });
        let now = tokio::time::Instant::now();
        let force = CancellationToken::new();
        execution.cancel(DeliveryCancellationReason::ForcedShutdown);
        force.cancel();
        let budget = QueueShutdownBudget::default();
        budget.install(QueueShutdownDeadlines::before(
            now,
            now + Duration::from_secs(2),
        ));
        RabbitMQQueueEngine::finish_in_flight_tasks(
            &mut tasks,
            false,
            None,
            Some(&force),
            Some(&execution),
            Some(&budget),
            None,
        )
        .await
        .unwrap();
        assert!(
            completed.load(Ordering::Acquire),
            "force must not abort accepted work before cooperation"
        );
        assert!(tasks.is_empty(), "actual completion must also be joined");
        assert_eq!(tokio::time::Instant::now() - now, Duration::from_millis(50));
        assert_eq!(
            execution.reason(),
            Some(DeliveryCancellationReason::ForcedShutdown)
        );
    }

    #[tokio::test]
    async fn terminal_task_failure_is_reconciled_before_dispatch_capacity_reopens() {
        let mut in_flight_tasks = JoinSet::new();
        in_flight_tasks.spawn(async { Err("BROKER_ACK") });
        let admission = CancellationToken::new();
        let force = CancellationToken::new();
        let terminal_ledger = DeliveryTerminalLedger::default();

        let result = RabbitMQQueueEngine::wait_for_dispatch_capacity(
            &mut in_flight_tasks,
            &admission,
            &force,
            1,
            &terminal_ledger,
        )
        .await;

        assert_eq!(result, Err("BROKER_ACK"));
        assert!(in_flight_tasks.is_empty());
    }

    #[tokio::test]
    async fn runtime_failure_aborts_and_joins_pending_sibling_before_returning() {
        let dropped = Arc::new(AtomicBool::new(false));
        let sibling_dropped = Arc::clone(&dropped);
        let (failure_completed_tx, failure_completed_rx) = tokio::sync::oneshot::channel();
        let (sibling_started_tx, sibling_started_rx) = tokio::sync::oneshot::channel();
        let mut in_flight_tasks = JoinSet::new();

        in_flight_tasks.spawn(async move {
            let _ = failure_completed_tx.send(());
            Err("BROKER_ACK")
        });
        in_flight_tasks.spawn(async move {
            let _drop_signal = DropSignal(sibling_dropped);
            let _ = sibling_started_tx.send(());
            std::future::pending::<Result<(), &'static str>>().await
        });

        failure_completed_rx
            .await
            .expect("failing task must complete");
        sibling_started_rx.await.expect("sibling task must start");

        let result = RabbitMQQueueEngine::finish_in_flight_tasks(
            &mut in_flight_tasks,
            false,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

        assert_eq!(result, Err("BROKER_ACK"));
        assert!(in_flight_tasks.is_empty());
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn force_interrupts_dispatch_capacity_wait_without_admitting_more_work() {
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = Arc::clone(&dropped);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut in_flight_tasks = JoinSet::new();
        in_flight_tasks.spawn(async move {
            let _drop_signal = DropSignal(task_dropped);
            let _ = started_tx.send(());
            std::future::pending::<Result<(), &'static str>>().await
        });
        started_rx.await.expect("capacity-owning task must start");
        let admission = CancellationToken::new();
        let force = CancellationToken::new();
        force.cancel();

        let capacity = RabbitMQQueueEngine::wait_for_dispatch_capacity(
            &mut in_flight_tasks,
            &admission,
            &force,
            1,
            &DeliveryTerminalLedger::default(),
        )
        .await
        .expect("force is a planned lifecycle outcome");

        assert!(!capacity);
        RabbitMQQueueEngine::abort_and_join_in_flight_tasks(&mut in_flight_tasks)
            .await
            .expect("force must join the capacity owner");
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn forced_in_flight_cleanup_aborts_and_joins_the_owned_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = Arc::clone(&dropped);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut in_flight_tasks = JoinSet::new();
        in_flight_tasks.spawn(async move {
            let _drop_signal = DropSignal(task_dropped);
            let _ = started_tx.send(());
            std::future::pending::<Result<(), &'static str>>().await
        });
        started_rx.await.expect("in-flight task must start");

        RabbitMQQueueEngine::abort_and_join_in_flight_tasks(&mut in_flight_tasks)
            .await
            .expect("forced in-flight cleanup");

        assert!(in_flight_tasks.is_empty());
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn forced_cleanup_retains_completed_failure_and_joins_pending_sibling() {
        let dropped = Arc::new(AtomicBool::new(false));
        let sibling_dropped = Arc::clone(&dropped);
        let (failure_completed_tx, failure_completed_rx) = tokio::sync::oneshot::channel();
        let (sibling_started_tx, sibling_started_rx) = tokio::sync::oneshot::channel();
        let mut in_flight_tasks = JoinSet::new();

        in_flight_tasks.spawn(async move {
            let _ = failure_completed_tx.send(());
            Err("BROKER_NACK")
        });
        in_flight_tasks.spawn(async move {
            let _drop_signal = DropSignal(sibling_dropped);
            let _ = sibling_started_tx.send(());
            std::future::pending::<Result<(), &'static str>>().await
        });

        failure_completed_rx
            .await
            .expect("failing task must complete");
        sibling_started_rx.await.expect("sibling task must start");

        let result =
            RabbitMQQueueEngine::abort_and_join_in_flight_tasks(&mut in_flight_tasks).await;

        assert_eq!(result, Err("BROKER_NACK"));
        assert!(in_flight_tasks.is_empty());
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn force_signal_joins_dispatcher_cleanup_before_supervisor_completes() {
        let admission = CancellationToken::new();
        let force = CancellationToken::new();
        let receiver_dropped = Arc::new(AtomicBool::new(false));
        let receiver_drop_witness = Arc::clone(&receiver_dropped);
        let dispatcher_completed = Arc::new(AtomicBool::new(false));
        let dispatcher_completion_witness = Arc::clone(&dispatcher_completed);
        let dispatcher_force = force.clone();
        let (receiver_started_tx, receiver_started_rx) = tokio::sync::oneshot::channel();
        let (dispatcher_started_tx, dispatcher_started_rx) = tokio::sync::oneshot::channel();

        let supervisor = tokio::spawn(supervise_queue_pair(
            "orders".into(),
            admission,
            force.clone(),
            DeliveryCancellationSource::new(),
            Arc::new(DeliveryTerminalLedger::default()),
            async move {
                let _drop_signal = DropSignal(receiver_drop_witness);
                let _ = receiver_started_tx.send(());
                std::future::pending::<Result<(), &'static str>>().await
            },
            async move {
                let _ = dispatcher_started_tx.send(());
                dispatcher_force.cancelled().await;
                dispatcher_completion_witness.store(true, Ordering::SeqCst);
                Ok(())
            },
            Arc::new(QueuePairEvidence::default()),
        ));
        receiver_started_rx.await.expect("receiver must start");
        dispatcher_started_rx.await.expect("dispatcher must start");

        force.cancel();
        tokio::time::timeout(Duration::from_secs(1), supervisor)
            .await
            .expect("forced supervisor must be bounded")
            .expect("supervisor task join")
            .expect("planned force must not become a runtime failure");

        assert!(receiver_dropped.load(Ordering::SeqCst));
        assert!(dispatcher_completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_drain_retains_handles_for_forced_abort_and_join() {
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = Arc::clone(&dropped);
        let background = tokio::spawn(async move {
            let _drop_signal = DropSignal(task_dropped);
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        });
        let tasks = Arc::new(Mutex::new(vec![SupervisedQueuePair {
            queue: "orders".into(),
            handle: background,
        }]));
        let drain_tasks = Arc::clone(&tasks);
        let shutdown =
            tokio::spawn(async move { drain_background_tasks(&drain_tasks).await.result });

        tokio::task::yield_now().await;
        shutdown.abort();
        assert!(matches!(shutdown.await, Err(error) if error.is_cancelled()));
        assert_eq!(tasks.lock().await.len(), 1);
        let retained = tasks
            .lock()
            .await
            .pop()
            .expect("cancelled drain must retain the supervisor handle");
        retained.handle.abort();
        assert!(matches!(retained.handle.await, Err(error) if error.is_cancelled()));
        assert!(tasks.lock().await.is_empty());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("aborted queue task did not drop");
    }

    #[tokio::test]
    async fn registration_task_is_ledger_owned_before_it_can_start() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut tasks = Vec::new();

        adopt_supervised_queue_task(&mut tasks, "orders.created".into(), async move {
            let _ = started_tx.send(());
            Ok(())
        });

        assert_eq!(tasks.len(), 1, "the engine must own the JoinHandle first");
        started_rx
            .await
            .expect("the adopted task must be released after ledger insertion");
        let outcome = drain_background_tasks(&Mutex::new(tasks)).await;
        assert!(outcome.result.is_ok());
        assert!(outcome.reconciled);
    }

    #[test]
    fn startup_result_and_readiness_commit_in_one_synchronous_boundary() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        ledger.configure_expected_consumers(1);
        let (startup_tx, mut startup_rx) = tokio::sync::oneshot::channel();

        let readiness = publish_registration_readiness(startup_tx, Arc::clone(&ledger))
            .expect("open startup observer");

        assert!(ledger.snapshot().is_ready());
        assert_eq!(startup_rx.try_recv(), Ok(Ok(())));
        drop(readiness);
        assert_eq!(ledger.snapshot().ready_consumers, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_observer_on_another_worker_never_precedes_readiness() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        ledger.configure_expected_consumers(1);
        let observer_ledger = Arc::clone(&ledger);
        let (startup_tx, startup_rx) =
            tokio::sync::oneshot::channel::<Result<(), MessageBrokerError>>();
        let observer = tokio::spawn(async move {
            startup_rx
                .await
                .expect("startup sender")
                .expect("startup success");
            assert!(
                observer_ledger.snapshot().is_ready(),
                "a different runtime worker must not observe Ok before readiness"
            );
        });
        tokio::task::yield_now().await;

        let readiness = publish_registration_readiness(startup_tx, Arc::clone(&ledger))
            .expect("open startup observer");
        observer.await.expect("startup observer join");
        drop(readiness);
    }

    #[test]
    fn closed_startup_observer_rolls_back_provisional_readiness() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        ledger.configure_expected_consumers(1);
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        drop(startup_rx);

        assert!(publish_registration_readiness(startup_tx, Arc::clone(&ledger)).is_err());
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.registered_consumers, 0);
        assert_eq!(snapshot.ready_consumers, 0);
        assert!(!snapshot.is_ready());
    }

    #[tokio::test]
    async fn nth_registration_failure_retains_prior_committed_owner_for_shutdown() {
        let admission = CancellationToken::new();
        let first_admission = admission.clone();
        let first_dropped = Arc::new(AtomicBool::new(false));
        let first_drop_witness = Arc::clone(&first_dropped);
        let (second_startup_tx, second_startup_rx) =
            tokio::sync::oneshot::channel::<Result<(), MessageBrokerError>>();
        let mut tasks = Vec::new();

        adopt_supervised_queue_task(&mut tasks, "orders.created".into(), async move {
            let _drop_signal = DropSignal(first_drop_witness);
            first_admission.cancelled().await;
            Ok(())
        });
        adopt_supervised_queue_task(&mut tasks, "orders.cancelled".into(), async move {
            let _ = second_startup_tx.send(Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::General("injected Nth registration failure".into()),
            )));
            // Pre-commit registration failure is reported to its startup
            // observer; the engine-owned task itself is still cleanly reaped.
            Ok(())
        });

        let error = second_startup_rx
            .await
            .expect("the Nth registration observer must receive a terminal result")
            .expect_err("the Nth registration must fail");
        assert_eq!(error.error_code(), "BROKER_GENERAL");
        assert_eq!(
            tasks.len(),
            2,
            "the prior committed registration must remain in the engine ledger"
        );

        admission.cancel();
        let outcome = drain_background_tasks(&Mutex::new(tasks)).await;
        assert!(outcome.result.is_ok());
        assert!(outcome.reconciled);
        assert!(first_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dropped_registration_observer_cancels_but_does_not_detach_the_owned_attempt() {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        let mut tasks = Vec::new();

        adopt_supervised_queue_task(&mut tasks, "orders.created".into(), async move {
            let mut startup_tx = startup_tx;
            let attempt = async move {
                task_cancellation.cancelled().await;
                let _ = cleanup_tx.send(());
                Ok::<(), ()>(())
            };
            let (result, observer_dropped) =
                observe_registration_attempt(&mut startup_tx, &cancellation, attempt).await;
            assert!(observer_dropped);
            assert!(result.is_ok());
            Ok(())
        });

        drop(startup_rx);
        cleanup_rx
            .await
            .expect("engine-owned attempt must finish cancellation cleanup");
        let outcome = drain_background_tasks(&Mutex::new(tasks)).await;
        assert!(outcome.result.is_ok());
        assert!(outcome.reconciled);
    }

    #[test]
    fn unproven_registration_cleanup_poison_blocks_the_same_physical_queue() {
        let states = DashMap::new();
        let key = ("orders".to_owned(), "orders.created".to_owned());

        reserve_queue_registration(&states, &key).expect("first reservation");
        let close_failure = Err(MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
            "injected dedicated channel close".into(),
        )));
        finish_registration_cleanup_result(&states, &key, &close_failure);

        assert_eq!(
            states.get(&key).map(|state| *state),
            Some(QueueRegistrationState::CleanupUnproven)
        );
        let error = reserve_queue_registration(&states, &key)
            .expect_err("cleanup-unproven tombstone must reject a second consumer");
        assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
        assert_eq!(states.len(), 1);
    }

    #[test]
    fn proven_registration_cleanup_releases_the_physical_queue_key() {
        let states = DashMap::new();
        let key = ("orders".to_owned(), "orders.created".to_owned());

        reserve_queue_registration(&states, &key).expect("first reservation");
        finish_registration_rollback(&states, &key, false);
        reserve_queue_registration(&states, &key)
            .expect("proven cleanup permits one later registration");

        assert_eq!(
            states.get(&key).map(|state| *state),
            Some(QueueRegistrationState::Registering)
        );
    }

    #[test]
    fn only_closed_or_error_equivalent_channel_status_proves_cleanup() {
        let cases = [
            (
                "initial",
                DedicatedConsumerChannelStatus {
                    initializing: true,
                    closing: false,
                    connected: false,
                    reconnecting: false,
                },
                false,
            ),
            (
                "reconnecting",
                DedicatedConsumerChannelStatus {
                    initializing: true,
                    closing: true,
                    connected: false,
                    reconnecting: true,
                },
                false,
            ),
            (
                "connected",
                DedicatedConsumerChannelStatus {
                    initializing: false,
                    closing: false,
                    connected: true,
                    reconnecting: false,
                },
                false,
            ),
            (
                "closing",
                DedicatedConsumerChannelStatus {
                    initializing: false,
                    closing: true,
                    connected: false,
                    reconnecting: false,
                },
                false,
            ),
            (
                "closed-or-error",
                DedicatedConsumerChannelStatus {
                    initializing: false,
                    closing: false,
                    connected: false,
                    reconnecting: false,
                },
                true,
            ),
        ];

        for (name, status, expected) in cases {
            assert_eq!(
                status.proves_terminal_cleanup(),
                expected,
                "unexpected cleanup classification for {name}"
            );
        }
    }

    #[test]
    fn unproven_disconnected_channel_status_preserves_registration_tombstone() {
        let states = DashMap::new();
        let key = ("orders".to_owned(), "orders.created".to_owned());
        let closing = DedicatedConsumerChannelStatus {
            initializing: false,
            closing: true,
            connected: false,
            reconnecting: false,
        };

        reserve_queue_registration(&states, &key).expect("first reservation");
        finish_registration_rollback(&states, &key, !closing.proves_terminal_cleanup());

        assert_eq!(
            states.get(&key).map(|state| *state),
            Some(QueueRegistrationState::CleanupUnproven)
        );
        reserve_queue_registration(&states, &key)
            .expect_err("an unproven disconnected channel must keep the tombstone");
    }

    #[tokio::test]
    async fn accepted_registration_cleanup_orders_cancel_before_channel_close() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let cancel_events = Arc::clone(&events);
        let close_events = Arc::clone(&events);

        cancel_then_close_consumer(
            move || async move {
                cancel_events.lock().expect("event ledger").push("cancel");
                true
            },
            move || async move {
                close_events.lock().expect("event ledger").push("close");
                Ok(())
            },
        )
        .await
        .expect("accepted registration cleanup");

        assert_eq!(*events.lock().expect("event ledger"), ["cancel", "close"]);
    }

    #[tokio::test]
    async fn proven_channel_close_reconciles_an_unacknowledged_consumer_cancel() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let cancel_events = Arc::clone(&events);
        let close_events = Arc::clone(&events);

        cancel_then_close_consumer(
            move || async move {
                cancel_events.lock().expect("event ledger").push("cancel");
                false
            },
            move || async move {
                close_events.lock().expect("event ledger").push("close");
                Ok(())
            },
        )
        .await
        .expect("channel close is the terminal cleanup proof");

        assert_eq!(*events.lock().expect("event ledger"), ["cancel", "close"]);
    }

    #[tokio::test]
    async fn topology_bootstrap_failure_precedes_runtime_token_and_consumer_admission() {
        let definition = QueueDefinition {
            name: "orders.created".into(),
            exchange_name: "orders".into(),
            routing_key: "orders.created".into(),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100,
                main_max_bytes: 1024 * 1024,
                retry_bucket_max_messages: 100,
                retry_bucket_max_bytes: 1024 * 1024,
                dead_letter_max_messages: 100,
                dead_letter_max_bytes: 1024 * 1024,
            }),
            ..QueueDefinition::default()
        };
        let setting = MessageBrokerSetting::from_definitions(
            Duration::from_secs(1),
            std::slice::from_ref(&definition),
        )
        .expect("accepted topology setting");
        let manager = Arc::new(FailingBootstrapChannelManager::default());
        let channel_manager: Arc<dyn ChannelManager<Channel>> = manager.clone();
        let (retry_engine, _) = recording_retry_engine(false);
        let engine = RabbitMQQueueEngine::new(setting, channel_manager, retry_engine);

        let error = engine
            .start(CancellationToken::new())
            .await
            .expect_err("topology bootstrap must fail before consumer admission");

        assert_eq!(error.error_code(), "BROKER_GENERAL");
        assert_eq!(manager.calls.load(Ordering::SeqCst), 1);
        assert!(engine.runtime_tokens.lock().await.is_none());
        assert!(engine.buffers.is_empty());
        assert!(engine.background_tasks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn basic_consume_admission_uses_an_uncached_dedicated_channel() {
        let definition = QueueDefinition {
            name: "orders.created".into(),
            exchange_name: "orders".into(),
            routing_key: "orders.created".into(),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100,
                main_max_bytes: 1024 * 1024,
                retry_bucket_max_messages: 100,
                retry_bucket_max_bytes: 1024 * 1024,
                dead_letter_max_messages: 100,
                dead_letter_max_bytes: 1024 * 1024,
            }),
            ..QueueDefinition::default()
        };
        let setting = MessageBrokerSetting::from_definitions(
            Duration::from_secs(1),
            std::slice::from_ref(&definition),
        )
        .expect("accepted topology setting");
        let manager = Arc::new(FailingDedicatedChannelManager::default());
        let channel_manager: Arc<dyn ChannelManager<Channel>> = manager.clone();
        let (retry_engine, _) = recording_retry_engine(false);
        let engine = RabbitMQQueueEngine::new(setting, channel_manager, retry_engine);
        let settlement = CancellationToken::new();
        *engine.runtime_tokens.lock().await = Some(QueueRuntimeTokens {
            admission: settlement.child_token(),
            settlement,
            force: CancellationToken::new(),
        });
        engine.terminal_ledger.configure_expected_consumers(1);
        let handler: QueueDeliveryHandler = Arc::new(|_input| Box::pin(async { Ok(()) }));

        let error = engine
            .create_consumer("orders", "orders.created", handler)
            .await
            .expect_err("dedicated channel acquisition is intentionally failed");

        assert_eq!(error.error_code(), "BROKER_GENERAL");
        assert_eq!(manager.shared_calls.load(Ordering::SeqCst), 0);
        assert_eq!(manager.dedicated_calls.load(Ordering::SeqCst), 1);
        assert!(engine.buffers.is_empty());
        assert_eq!(
            engine.background_tasks.lock().await.len(),
            1,
            "even a failed registration remains engine-owned until drain joins it"
        );
        assert!(engine.registration_states.is_empty());
        let snapshot = engine.delivery_terminal_snapshot();
        assert_eq!(snapshot.registered_consumers, 0);
        assert_eq!(
            snapshot.last_operational_failure_code,
            Some("BROKER_GENERAL")
        );
        engine
            .drain()
            .await
            .expect("failed registration owner is still cleanly joinable");
        assert!(engine.background_tasks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn registration_channel_acquisition_panic_is_typed_and_reaped() {
        let definition = QueueDefinition {
            name: "orders.created".into(),
            exchange_name: "orders".into(),
            routing_key: "orders.created".into(),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100,
                main_max_bytes: 1024 * 1024,
                retry_bucket_max_messages: 100,
                retry_bucket_max_bytes: 1024 * 1024,
                dead_letter_max_messages: 100,
                dead_letter_max_bytes: 1024 * 1024,
            }),
            ..QueueDefinition::default()
        };
        let setting = MessageBrokerSetting::from_definitions(
            Duration::from_secs(1),
            std::slice::from_ref(&definition),
        )
        .expect("accepted topology setting");
        let manager = Arc::new(PanickingDedicatedChannelManager::default());
        let channel_manager: Arc<dyn ChannelManager<Channel>> = manager.clone();
        let (retry_engine, _) = recording_retry_engine(false);
        let engine = RabbitMQQueueEngine::new(setting, channel_manager, retry_engine);
        let settlement = CancellationToken::new();
        *engine.runtime_tokens.lock().await = Some(QueueRuntimeTokens {
            admission: settlement.child_token(),
            settlement,
            force: CancellationToken::new(),
        });
        engine.terminal_ledger.configure_expected_consumers(1);
        let handler: QueueDeliveryHandler = Arc::new(|_input| Box::pin(async { Ok(()) }));

        let error = engine
            .create_consumer("orders", "orders.created", handler)
            .await
            .expect_err("registration panic must be contained");

        assert_eq!(error.error_code(), "BROKER_CONSUMER_TASK_FAILED");
        assert_eq!(manager.dedicated_calls.load(Ordering::SeqCst), 1);
        assert!(engine.registration_states.is_empty());
        assert!(engine.buffers.is_empty());
        assert_eq!(engine.background_tasks.lock().await.len(), 1);
        assert_eq!(engine.delivery_terminal_snapshot().registered_consumers, 0);

        engine
            .drain()
            .await
            .expect("contained registration panic leaves a joinable owner");
        assert!(engine.background_tasks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn shutdown_seal_prevents_a_waiting_registration_from_reaching_transport() {
        let definition = QueueDefinition {
            name: "orders.created".into(),
            exchange_name: "orders".into(),
            routing_key: "orders.created".into(),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100,
                main_max_bytes: 1024 * 1024,
                retry_bucket_max_messages: 100,
                retry_bucket_max_bytes: 1024 * 1024,
                dead_letter_max_messages: 100,
                dead_letter_max_bytes: 1024 * 1024,
            }),
            ..QueueDefinition::default()
        };
        let setting = MessageBrokerSetting::from_definitions(
            Duration::from_secs(1),
            std::slice::from_ref(&definition),
        )
        .expect("accepted topology setting");
        let manager = Arc::new(FailingDedicatedChannelManager::default());
        let channel_manager: Arc<dyn ChannelManager<Channel>> = manager.clone();
        let (retry_engine, _) = recording_retry_engine(false);
        let engine = Arc::new(RabbitMQQueueEngine::new(
            setting,
            channel_manager,
            retry_engine,
        ));
        let settlement = CancellationToken::new();
        *engine.runtime_tokens.lock().await = Some(QueueRuntimeTokens {
            admission: settlement.child_token(),
            settlement,
            force: CancellationToken::new(),
        });
        engine.terminal_ledger.configure_expected_consumers(1);

        let registration_gate = engine.registration_gate.lock().await;
        let registering_engine = Arc::clone(&engine);
        let registration = tokio::spawn(async move {
            let handler: QueueDeliveryHandler = Arc::new(|_input| Box::pin(async { Ok(()) }));
            registering_engine
                .create_consumer("orders", "orders.created", handler)
                .await
        });
        loop {
            if engine.background_tasks.lock().await.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }

        let stopping_engine = Arc::clone(&engine);
        let stop = tokio::spawn(async move { stopping_engine.stop_admission().await });
        while !engine.registration_sealed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        drop(registration_gate);

        stop.await
            .expect("stop task join")
            .expect("registration barrier must reconcile");
        let error = registration
            .await
            .expect("registration observer join")
            .expect_err("sealed admission must reject the waiting registration");
        assert_eq!(error.error_code(), "BROKER_CANCELLED");
        assert_eq!(manager.shared_calls.load(Ordering::SeqCst), 0);
        assert_eq!(manager.dedicated_calls.load(Ordering::SeqCst), 0);
        assert!(engine.registration_states.is_empty());
        assert!(engine.buffers.is_empty());

        engine
            .drain()
            .await
            .expect("the adopted registration task must remain joinable");
        assert!(engine.background_tasks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn cancelled_engine_drain_is_reconciled_by_the_same_force_owned_handles() {
        let setting = MessageBrokerSetting::from_definitions(Duration::from_secs(1), &[])
            .expect("empty test setting");
        let (retry_engine, _) = recording_retry_engine(false);
        let engine = Arc::new(RabbitMQQueueEngine::new(
            setting,
            Arc::new(UnusedChannelManager),
            retry_engine,
        ));
        engine
            .start(CancellationToken::new())
            .await
            .expect("start drain-only engine");
        let tokens = engine
            .runtime_tokens
            .lock()
            .await
            .clone()
            .expect("runtime tokens");
        let receiver_dropped = Arc::new(AtomicBool::new(false));
        let receiver_drop_witness = Arc::clone(&receiver_dropped);
        let dispatcher_force = tokens.force.clone();
        let (receiver_started_tx, receiver_started_rx) = tokio::sync::oneshot::channel();
        let supervisor = tokio::spawn(supervise_queue_pair(
            "orders".into(),
            tokens.admission,
            tokens.force,
            engine.execution.clone(),
            Arc::clone(&engine.terminal_ledger),
            async move {
                let _drop_signal = DropSignal(receiver_drop_witness);
                let _ = receiver_started_tx.send(());
                std::future::pending::<Result<(), &'static str>>().await
            },
            async move {
                dispatcher_force.cancelled().await;
                Ok(())
            },
            Arc::new(QueuePairEvidence::default()),
        ));
        engine
            .background_tasks
            .lock()
            .await
            .push(SupervisedQueuePair {
                queue: "orders".into(),
                handle: supervisor,
            });
        receiver_started_rx.await.expect("receiver must start");

        let drain_engine = Arc::clone(&engine);
        let drain = tokio::spawn(async move { drain_engine.drain_to_terminal().await });
        tokio::task::yield_now().await;
        drain.abort();
        assert!(matches!(drain.await, Err(error) if error.is_cancelled()));
        assert_eq!(engine.background_tasks.lock().await.len(), 1);

        engine
            .force_drain()
            .await
            .expect("force must consume the retained supervisor");
        assert!(engine.background_tasks.lock().await.is_empty());
        assert!(engine.drain_reconciled());
        assert!(receiver_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn concurrent_drain_observers_replay_one_terminal_task_failure() {
        let setting = MessageBrokerSetting::from_definitions(Duration::from_secs(1), &[])
            .expect("empty test setting");
        let (retry_engine, _) = recording_retry_engine(false);
        let engine = Arc::new(RabbitMQQueueEngine::new(
            setting,
            Arc::new(UnusedChannelManager),
            retry_engine,
        ));
        engine
            .start(CancellationToken::new())
            .await
            .expect("start drain-only engine");
        let runtime_tokens = engine
            .runtime_tokens
            .lock()
            .await
            .clone()
            .expect("runtime tokens");
        let failure = RabbitMqConsumerTaskFailure {
            queue: "orders".into(),
            role: RabbitMqConsumerTaskRole::Dispatcher,
            kind: RabbitMqConsumerTaskFailureKind::OperationFailed,
            operation_error_code: Some("BROKER_ACK"),
        };
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let task_release = Arc::clone(&release);
        engine
            .background_tasks
            .lock()
            .await
            .push(SupervisedQueuePair {
                queue: "orders".into(),
                handle: tokio::spawn(async move {
                    let _ = started_tx.send(());
                    task_release.notified().await;
                    Err(failure)
                }),
            });

        let first_engine = Arc::clone(&engine);
        let first = tokio::spawn(async move { first_engine.drain_to_terminal().await });
        let second_engine = Arc::clone(&engine);
        let second = tokio::spawn(async move { second_engine.drain_to_terminal().await });
        started_rx.await.expect("supervised task must start");
        release.notify_one();

        let first = first.await.expect("first drain join");
        let second = second.await.expect("second drain join");
        assert_eq!(first, second);
        assert_eq!(
            first
                .expect_err("task failure must be replayed")
                .error_code(),
            "BROKER_CONSUMER_TASK_FAILED"
        );
        assert!(engine.background_tasks.lock().await.is_empty());
        assert!(engine.force_drain().await.is_ok());
        assert!(runtime_tokens.admission.is_cancelled());
        assert!(runtime_tokens.force.is_cancelled());
        assert!(runtime_tokens.settlement.is_cancelled());
    }

    #[tokio::test]
    async fn supervisor_join_panic_never_opens_connection_or_di_disposal_gate() {
        let setting = MessageBrokerSetting::from_definitions(Duration::from_secs(1), &[])
            .expect("empty test setting");
        let (retry_engine, _) = recording_retry_engine(false);
        let engine =
            RabbitMQQueueEngine::new(setting, Arc::new(UnusedChannelManager), retry_engine);
        engine
            .background_tasks
            .lock()
            .await
            .push(SupervisedQueuePair {
                queue: "orders".into(),
                handle: tokio::spawn(async move {
                    panic!("injected top-level supervisor panic");
                }),
            });

        let error = engine
            .drain_to_terminal()
            .await
            .expect_err("supervisor panic must fail drain");
        assert_eq!(error.error_code(), "BROKER_CONSUMER_TASK_FAILED");
        assert!(!engine.drain_reconciled());
        assert_eq!(
            engine.delivery_terminal_snapshot().runtime_state,
            crate::ConsumerRuntimeState::Failed
        );
        assert!(engine.force_drain().await.is_err());
        assert!(!engine.drain_reconciled());
    }

    #[tokio::test]
    async fn unexpected_receiver_exit_fails_the_pair_and_cancels_the_runtime() {
        let cancellation = CancellationToken::new();
        let force = CancellationToken::new();
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let dispatcher_cancellation = cancellation.clone();

        let failure = supervise_queue_pair(
            "orders".into(),
            cancellation.clone(),
            force,
            DeliveryCancellationSource::new(),
            Arc::clone(&ledger),
            async { Ok(()) },
            async move {
                dispatcher_cancellation.cancelled().await;
                Ok(())
            },
            Arc::new(QueuePairEvidence::default()),
        )
        .await
        .expect_err("an unexpected receiver exit must fail the queue pair");

        assert_eq!(failure.queue, "orders");
        assert_eq!(failure.role, RabbitMqConsumerTaskRole::Receiver);
        assert_eq!(
            failure.kind,
            RabbitMqConsumerTaskFailureKind::UnexpectedExit
        );
        assert!(cancellation.is_cancelled());
        assert_eq!(
            ledger.snapshot().runtime_state,
            crate::ConsumerRuntimeState::Failed
        );
        assert_eq!(
            ledger.snapshot().last_operational_failure_code,
            Some("BROKER_CONSUMER_TASK_FAILED")
        );
    }

    #[test]
    fn shutdown_after_child_exit_cannot_reclassify_the_latched_cause() {
        let unexpected = child_failure(
            "orders",
            ChildTaskExit::Completed {
                role: RabbitMqConsumerTaskRole::Receiver,
                shutdown_requested_at_exit: false,
            },
        )
        .expect("an exit which preceded shutdown must remain a failure");
        assert_eq!(
            unexpected.kind,
            RabbitMqConsumerTaskFailureKind::UnexpectedExit
        );

        let pre_shutdown_cancellation_code = child_failure(
            "orders",
            ChildTaskExit::OperationFailed {
                role: RabbitMqConsumerTaskRole::Dispatcher,
                error_code: "BROKER_CANCELLED",
                shutdown_requested_at_exit: false,
            },
        )
        .expect("a cancellation-coded exit which preceded shutdown is still a failure");
        assert_eq!(
            pre_shutdown_cancellation_code.kind,
            RabbitMqConsumerTaskFailureKind::OperationFailed
        );

        assert!(
            child_failure(
                "orders",
                ChildTaskExit::Completed {
                    role: RabbitMqConsumerTaskRole::Receiver,
                    shutdown_requested_at_exit: true,
                },
            )
            .is_none(),
            "a child which observed shutdown before exiting is planned"
        );
    }

    #[tokio::test]
    async fn runtime_failure_forces_and_joins_its_sibling_without_waiting_for_handler_timeout() {
        let cancellation = CancellationToken::new();
        let force = CancellationToken::new();
        let dispatcher_force = force.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let sibling_dropped = Arc::clone(&dropped);

        let failure = tokio::time::timeout(
            Duration::from_secs(1),
            supervise_queue_pair(
                "orders".into(),
                cancellation,
                force,
                DeliveryCancellationSource::new(),
                Arc::new(DeliveryTerminalLedger::default()),
                async { Err("BROKER_ACK") },
                async move {
                    let _drop_signal = DropSignal(sibling_dropped);
                    dispatcher_force.cancelled().await;
                    Ok(())
                },
                Arc::new(QueuePairEvidence::default()),
            ),
        )
        .await
        .expect("runtime supervision did not fail fast")
        .expect_err("an operation failure must fail the queue pair");

        assert_eq!(failure.role, RabbitMqConsumerTaskRole::Receiver);
        assert_eq!(
            failure.kind,
            RabbitMqConsumerTaskFailureKind::OperationFailed
        );
        assert_eq!(failure.operation_error_code, Some("BROKER_ACK"));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn planned_cancellation_allows_both_queue_tasks_to_exit_cleanly() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let force = CancellationToken::new();
        let receiver_cancellation = cancellation.clone();
        let dispatcher_cancellation = cancellation.clone();

        supervise_queue_pair(
            "orders".into(),
            cancellation,
            force,
            DeliveryCancellationSource::new(),
            Arc::new(DeliveryTerminalLedger::default()),
            async move {
                receiver_cancellation.cancelled().await;
                Err("BROKER_CANCELLED")
            },
            async move {
                dispatcher_cancellation.cancelled().await;
                Err(SettlementFailureKind::Cancelled.stable_code())
            },
            Arc::new(QueuePairEvidence::default()),
        )
        .await
        .expect("planned cancellation is not a runtime task failure");
    }

    #[tokio::test]
    async fn settlement_operation_failure_is_typed_and_fails_the_runtime() {
        let cancellation = CancellationToken::new();
        let force = CancellationToken::new();
        let receiver_cancellation = cancellation.clone();

        let failure = supervise_queue_pair(
            "orders".into(),
            cancellation.clone(),
            force,
            DeliveryCancellationSource::new(),
            Arc::new(DeliveryTerminalLedger::default()),
            async move {
                receiver_cancellation.cancelled().await;
                Ok(())
            },
            async { Err("BROKER_NACK") },
            Arc::new(QueuePairEvidence::default()),
        )
        .await
        .expect_err("a failed settlement must fail the queue runtime");

        assert_eq!(failure.role, RabbitMqConsumerTaskRole::Dispatcher);
        assert_eq!(
            failure.kind,
            RabbitMqConsumerTaskFailureKind::OperationFailed
        );
        assert_eq!(failure.operation_error_code, Some("BROKER_NACK"));
        assert!(cancellation.is_cancelled());
    }
}

#[cfg(test)]
#[path = "consumer_drain_tests.rs"]
mod consumer_drain_tests;
