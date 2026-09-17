use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;

use crate::{
    queue_engine_trait::QueueEngine,
    queue_service::{DeliveryScopeTracker, RegisteredQueueHandler},
    queue_trait::Queue,
};

use lily_error::application::message_broker::RabbitMQError;

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use crate::outbox_relay::{
    OutboxRelaySupervisor, RabbitMqOutboxPublisher, TransactionalOutboxRelaySnapshot,
};
#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use crate::transactional_mongodb::MongoTransactionalRuntime;
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
use crate::transactional_postgresql::PostgresTransactionalRuntime;

pub(crate) mod channel_manager;
pub(crate) mod connection_manager;
pub(crate) mod metadata;
pub(crate) mod queue_engine;
pub(crate) mod retry_engine;
pub(crate) mod topology;

#[cfg(feature = "fuzzing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FuzzAmqpMetadataFootprint {
    pub(crate) header_entries: usize,
    pub(crate) aggregate_bytes: usize,
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_amqp_metadata_footprint(
    properties: &lapin::BasicProperties,
) -> Result<FuzzAmqpMetadataFootprint, &'static str> {
    metadata::amqp_metadata_footprint(properties).map(|footprint| FuzzAmqpMetadataFootprint {
        header_entries: footprint.header_entries,
        aggregate_bytes: footprint.aggregate_bytes,
    })
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_validate_amqp_metadata(
    properties: &lapin::BasicProperties,
) -> Result<(), &'static str> {
    metadata::validate_amqp_metadata(properties)
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_projected_handoff_properties(
    properties: &lapin::BasicProperties,
) -> lapin::BasicProperties {
    metadata::projected_handoff_properties(properties)
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_validate_handoff_metadata_headroom(
    properties: &lapin::BasicProperties,
) -> Result<(), &'static str> {
    metadata::validate_handoff_metadata_headroom(properties)
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_canonical_retry_count(
    headers: Option<&lapin::types::FieldTable>,
) -> Result<u32, &'static str> {
    metadata::canonical_retry_count(headers)
}

#[cfg(feature = "fuzzing")]
pub(crate) use queue_engine::{
    FuzzTransportAdmissionFailure, fuzz_transport_admission_failure,
    fuzz_validate_delivery_envelope,
};

#[cfg(test)]
mod live_rabbitmq_delivery;

#[async_trait]
trait RabbitMqConsumerConnectionLifecycle: Send + Sync {
    async fn start(&self, cancellation: CancellationToken) -> Result<(), MessageBrokerError>;

    async fn close(&self) -> Result<(), MessageBrokerError>;
}

#[async_trait]
impl RabbitMqConsumerConnectionLifecycle for connection_manager::RabbitMQConnectionManager {
    async fn start(&self, cancellation: CancellationToken) -> Result<(), MessageBrokerError> {
        connection_manager::RabbitMQConnectionManager::start(self, cancellation).await
    }

    async fn close(&self) -> Result<(), MessageBrokerError> {
        connection_manager::RabbitMQConnectionManager::close(self).await
    }
}

/// Internal RabbitMQ provider which joins the queue engine and connection
/// lifecycle.
pub(crate) struct RabbitMQConsumer {
    engine: Arc<dyn QueueEngine>,
    connection_manager: Arc<dyn RabbitMqConsumerConnectionLifecycle>,
    delivery_scope_trackers: Mutex<Vec<Arc<DeliveryScopeTracker>>>,
    delivery_shutdown_budget: crate::shutdown_budget::QueueShutdownBudget,
    closed: AtomicBool,
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    outbox_relay: Option<Arc<OutboxRelaySupervisor>>,
}

fn ensure_drain_reconciled(reconciled: bool) -> Result<(), MessageBrokerError> {
    if reconciled {
        Ok(())
    } else {
        Err(MessageBrokerError::RabbitMQError(
            lily_error::application::message_broker::RabbitMQError::General(
                "queue task drain was not reconciled before connection disposal".into(),
            ),
        ))
    }
}

impl RabbitMQConsumer {
    #[cfg(any(
        test,
        not(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))
    ))]
    pub(crate) fn new(
        engine: Arc<dyn QueueEngine>,
        connection_manager: Arc<connection_manager::RabbitMQConnectionManager>,
    ) -> Self {
        Self::from_connection_lifecycle(engine, connection_manager)
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub(crate) fn new_with_outbox_relay(
        engine: Arc<dyn QueueEngine>,
        connection_manager: Arc<connection_manager::RabbitMQConnectionManager>,
        channel_manager: Arc<channel_manager::RabbitMQChannelManager>,
    ) -> Self {
        let mut consumer = Self::from_connection_lifecycle(engine, connection_manager);
        consumer.outbox_relay = Some(OutboxRelaySupervisor::new(RabbitMqOutboxPublisher::new(
            channel_manager,
        )));
        consumer
    }

    fn from_connection_lifecycle(
        engine: Arc<dyn QueueEngine>,
        connection_manager: Arc<dyn RabbitMqConsumerConnectionLifecycle>,
    ) -> Self {
        Self {
            engine,
            connection_manager,
            delivery_scope_trackers: Mutex::new(Vec::new()),
            delivery_shutdown_budget: crate::shutdown_budget::QueueShutdownBudget::default(),
            closed: AtomicBool::new(false),
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            outbox_relay: None,
        }
    }

    fn delivery_scope_trackers(&self) -> Vec<Arc<DeliveryScopeTracker>> {
        self.delivery_scope_trackers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn delivery_scopes_reconciled(&self) -> bool {
        self.delivery_scope_trackers()
            .iter()
            .all(|tracker| tracker.reconciled())
    }
}

#[async_trait]
impl Queue for RabbitMQConsumer {
    fn set_shutdown_deadlines(&self, deadlines: crate::shutdown_budget::QueueShutdownDeadlines) {
        self.engine.set_shutdown_deadlines(deadlines);
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        if let Some(relay) = &self.outbox_relay {
            relay.set_shutdown_deadlines(deadlines);
        }
        let trackers = self
            .delivery_scope_trackers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.delivery_shutdown_budget.install(deadlines);
        for tracker in trackers.iter() {
            tracker.set_shutdown_deadlines(deadlines);
        }
    }

    fn cancel_execution(&self, reason: crate::DeliveryCancellationReason) {
        self.engine.cancel_execution(reason);
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        if let Some(relay) = &self.outbox_relay {
            relay.request_force();
        }
    }

    async fn create_queue(
        &self,
        exchange_name: &str,
        queue: &str,
        handler: RegisteredQueueHandler,
    ) -> Result<(), MessageBrokerError> {
        let RegisteredQueueHandler {
            callback,
            scope_tracker,
        } = handler;
        // Adopt the handler's delivery-scope owner before registration can
        // yield to broker I/O. If the caller drops this future while the
        // engine is still registering the consumer, provider shutdown must
        // nevertheless retain and reconcile every scope the engine-owned
        // callback may create.
        {
            let mut trackers = self
                .delivery_scope_trackers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(deadlines) = self.delivery_shutdown_budget.deadlines() {
                scope_tracker.set_shutdown_deadlines(deadlines);
            }
            trackers.push(scope_tracker);
        }
        self.engine
            .create_consumer(exchange_name, queue, callback)
            .await?;
        Ok(())
    }

    async fn start_async(&self, ct: CancellationToken) -> Result<(), MessageBrokerError> {
        self.closed.store(false, Ordering::Release);
        self.connection_manager.start(ct.clone()).await?;
        if let Err(error) = self.engine.start(ct).await {
            match self.connection_manager.close().await {
                Ok(()) => {
                    // No runtime tokens or delivery tasks were committed when
                    // engine readiness failed. A proven connection close is a
                    // terminal provider state, so later DI cleanup is a no-op
                    // rather than a second stop authority.
                    self.closed.store(true, Ordering::Release);
                }
                Err(cleanup_error) => {
                    lily_trace::tracing::warn!(
                        %cleanup_error,
                        "RabbitMQ consumer readiness rollback failed"
                    );
                }
            }
            return Err(error);
        }
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        if let Some(relay) = self.outbox_relay.as_ref() {
            relay.start().await?;
        }
        Ok(())
    }

    async fn stop_async(&self) -> Result<(), MessageBrokerError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let admission = self.stop_admission_async().await;
        let drain = self.drain_async().await;
        let close = self.close_async().await;
        admission.and(drain).and(close)
    }

    async fn stop_admission_async(&self) -> Result<(), MessageBrokerError> {
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        if let Some(relay) = self.outbox_relay.as_ref() {
            relay.stop_admission();
        }
        self.engine.stop_admission().await
    }

    async fn drain_async(&self) -> Result<(), MessageBrokerError> {
        let engine_result = self.engine.drain().await;
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let relay = match self.outbox_relay.as_ref() {
            Some(relay) => relay.drain().await,
            None => Ok(()),
        };
        #[cfg(not(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        )))]
        let relay: Result<(), MessageBrokerError> = Ok(());
        // Transactions may transfer DI cleanup only when their owners stop.
        reconcile_delivery_scope_drains(engine_result.and(relay), self.delivery_scope_trackers())
            .await
    }

    async fn force_drain_async(&self) -> Result<(), MessageBrokerError> {
        let engine = self.engine.force_drain();
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let relay = async {
            match self.outbox_relay.as_ref() {
                Some(relay) => relay.force_drain().await,
                None => Ok(()),
            }
        };
        #[cfg(not(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        )))]
        let relay = async { Ok::<(), MessageBrokerError>(()) };
        // Force cancellation must reach the broker engine and every
        // transactional runtime during the first poll. Reconciliation remains
        // ordered inside each owner, while a stuck engine or delivery scope can
        // no longer prevent the relay from broadcasting its force signal.
        let (engine, relay) = tokio::join!(engine, relay);
        reconcile_delivery_scope_drains(engine.and(relay), self.delivery_scope_trackers()).await
    }

    async fn close_async(&self) -> Result<(), MessageBrokerError> {
        ensure_drain_reconciled(
            self.engine.drain_reconciled() && self.delivery_scopes_reconciled() && {
                #[cfg(any(
                    feature = "transactional-inbox-postgresql",
                    feature = "transactional-inbox-postgresql-factory",
                    feature = "transactional-inbox-mongodb",
                    feature = "transactional-inbox-mongodb-factory"
                ))]
                {
                    self.outbox_relay
                        .as_ref()
                        .is_none_or(|relay| relay.reconciled())
                }
                #[cfg(not(any(
                    feature = "transactional-inbox-postgresql",
                    feature = "transactional-inbox-postgresql-factory",
                    feature = "transactional-inbox-mongodb",
                    feature = "transactional-inbox-mongodb-factory"
                )))]
                {
                    true
                }
            },
        )?;
        let result = match self.delivery_shutdown_budget.deadlines() {
            Some(root) => self
                .delivery_shutdown_budget
                .run_until(root.hard(), self.connection_manager.close())
                .await
                .map_err(|_| {
                    MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
                        "consumer connection close".into(),
                    ))
                })?,
            None => self.connection_manager.close().await,
        };
        if result.is_ok() {
            self.engine.finish_cleanup();
            self.closed.store(true, Ordering::Release);
        }
        result
    }

    async fn wait_for_shutdown(&self) -> Result<(), MessageBrokerError> {
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        if let Some(relay) = self.outbox_relay.as_ref() {
            return tokio::select! {
                result = self.engine.wait_for_completion() => result,
                error = relay.wait_for_terminal_failure() => Err(error),
            };
        }
        self.engine.wait_for_completion().await
    }

    fn close_reconciled(&self) -> bool {
        self.closed.load(Ordering::Acquire) && self.drain_reconciled()
    }

    fn drain_reconciled(&self) -> bool {
        self.engine.drain_reconciled() && self.delivery_scopes_reconciled() && {
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            {
                self.outbox_relay
                    .as_ref()
                    .is_none_or(|relay| relay.reconciled())
            }
            #[cfg(not(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            )))]
            {
                true
            }
        }
    }

    fn delivery_terminal_snapshot(&self) -> crate::DeliveryTerminalSnapshot {
        self.engine.delivery_terminal_snapshot()
    }

    fn delivery_terminal_observations(&self) -> crate::DeliveryTerminalObservationsSnapshot {
        self.engine.delivery_terminal_observations()
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    async fn register_transactional_outbox(
        &self,
        runtime: Arc<PostgresTransactionalRuntime>,
    ) -> Result<(), MessageBrokerError> {
        let relay = self.outbox_relay.as_ref().ok_or_else(|| {
            MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(
                "transactional outbox relay is not installed".into(),
            ))
        })?;
        crate::outbox_relay::register_postgres_runtime(relay, runtime).await
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    async fn register_mongodb_transactional_outbox(
        &self,
        runtime: Arc<MongoTransactionalRuntime>,
    ) -> Result<(), MessageBrokerError> {
        let relay = self.outbox_relay.as_ref().ok_or_else(|| {
            MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(
                "transactional outbox relay is not installed".into(),
            ))
        })?;
        crate::outbox_relay::register_mongo_runtime(relay, runtime).await
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    fn transactional_outbox_snapshot(&self) -> TransactionalOutboxRelaySnapshot {
        self.outbox_relay
            .as_ref()
            .map_or_else(TransactionalOutboxRelaySnapshot::default, |relay| {
                relay.snapshot()
            })
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    fn transactional_inbox_snapshot(&self) -> crate::TransactionalInboxSnapshot {
        self.outbox_relay
            .as_ref()
            .map_or_else(crate::TransactionalInboxSnapshot::default, |relay| {
                relay.transactional_inbox_snapshot()
            })
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    fn transactional_outbox_ready(&self) -> bool {
        self.outbox_relay
            .as_ref()
            .is_none_or(|relay| relay.is_ready())
    }
}

async fn reconcile_delivery_scope_drains(
    engine_result: Result<(), MessageBrokerError>,
    trackers: Vec<Arc<DeliveryScopeTracker>>,
) -> Result<(), MessageBrokerError> {
    let mut first_error = engine_result.err();
    for tracker in trackers {
        if let Err(error) = tracker.drain().await {
            if first_error.is_none() {
                first_error = Some(error);
            } else {
                lily_trace::tracing::warn!(
                    lily.error_code = error.error_code(),
                    "additional queue delivery-scope reconciliation failure"
                );
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    use std::time::Duration;

    use async_trait::async_trait;
    use lily_error::application::{MessageBrokerError, message_broker::RabbitMQError};
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    use super::{
        DeliveryScopeTracker, Queue, QueueEngine, RabbitMQConsumer,
        RabbitMqConsumerConnectionLifecycle, ensure_drain_reconciled,
        reconcile_delivery_scope_drains,
    };
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    use crate::outbox_relay::{
        OutboxRelayPublisher, OutboxRelaySupervisor, RelayFailure, RelayRecord,
        TransactionalRuntimeLifecycle,
    };
    use crate::{
        DeliveryTerminalObservationsSnapshot, DeliveryTerminalSnapshot,
        queue_engine_trait::QueueDeliveryHandler, queue_service::RegisteredQueueHandler,
    };

    struct PendingRegistrationEngine {
        create_entered: Notify,
        create_release: Notify,
    }

    impl PendingRegistrationEngine {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                create_entered: Notify::new(),
                create_release: Notify::new(),
            })
        }
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    struct PendingForceEngine {
        force_calls: AtomicUsize,
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    #[async_trait]
    impl QueueEngine for PendingForceEngine {
        async fn start(&self, _ct: CancellationToken) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn stop_admission(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn drain(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn force_drain(&self) -> Result<(), MessageBrokerError> {
            self.force_calls.fetch_add(1, Ordering::AcqRel);
            std::future::pending::<Result<(), MessageBrokerError>>().await
        }

        async fn create_consumer(
            &self,
            _exchange_name: &str,
            _queue: &str,
            _handler: QueueDeliveryHandler,
        ) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn wait_for_completion(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        fn drain_reconciled(&self) -> bool {
            false
        }

        fn delivery_terminal_snapshot(&self) -> DeliveryTerminalSnapshot {
            DeliveryTerminalSnapshot::default()
        }

        fn delivery_terminal_observations(&self) -> DeliveryTerminalObservationsSnapshot {
            DeliveryTerminalObservationsSnapshot::default()
        }
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    struct ForceObservedRuntime {
        active: AtomicUsize,
        force_calls: AtomicUsize,
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    #[async_trait]
    impl TransactionalRuntimeLifecycle for ForceObservedRuntime {
        fn reconciled(&self) -> bool {
            self.active_transactions() == 0
        }
        fn stop_admission(&self) {}

        fn active_transactions(&self) -> usize {
            self.active.load(Ordering::Acquire)
        }

        async fn drain(&self, _timeout: Duration) -> Result<(), RelayFailure> {
            Ok(())
        }

        async fn force_drain(&self, _timeout: Duration) -> Result<(), RelayFailure> {
            self.force_calls.fetch_add(1, Ordering::AcqRel);
            self.active.store(0, Ordering::Release);
            Ok(())
        }
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    struct NoopOutboxPublisher;

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    #[async_trait]
    impl OutboxRelayPublisher for NoopOutboxPublisher {
        async fn publish(
            &self,
            _record: &RelayRecord,
            _timeout: Duration,
            _cancellation: CancellationToken,
        ) -> Result<(), MessageBrokerError> {
            Ok(())
        }
    }

    #[async_trait]
    impl QueueEngine for PendingRegistrationEngine {
        async fn start(&self, _ct: CancellationToken) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn stop_admission(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn drain(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn force_drain(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn create_consumer(
            &self,
            _exchange_name: &str,
            _queue: &str,
            _handler: QueueDeliveryHandler,
        ) -> Result<(), MessageBrokerError> {
            self.create_entered.notify_one();
            self.create_release.notified().await;
            Ok(())
        }

        async fn wait_for_completion(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        fn drain_reconciled(&self) -> bool {
            true
        }

        fn delivery_terminal_snapshot(&self) -> DeliveryTerminalSnapshot {
            DeliveryTerminalSnapshot::default()
        }

        fn delivery_terminal_observations(&self) -> DeliveryTerminalObservationsSnapshot {
            DeliveryTerminalObservationsSnapshot::default()
        }
    }

    struct StartFailingEngine {
        stop_admission_calls: AtomicUsize,
        drain_calls: AtomicUsize,
        reconciled: AtomicBool,
    }

    impl StartFailingEngine {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                stop_admission_calls: AtomicUsize::new(0),
                drain_calls: AtomicUsize::new(0),
                reconciled: AtomicBool::new(false),
            })
        }
    }

    #[async_trait]
    impl QueueEngine for StartFailingEngine {
        async fn start(&self, _ct: CancellationToken) -> Result<(), MessageBrokerError> {
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "engine-start-sentinel".into(),
            )))
        }

        async fn stop_admission(&self) -> Result<(), MessageBrokerError> {
            self.stop_admission_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn drain(&self) -> Result<(), MessageBrokerError> {
            self.drain_calls.fetch_add(1, Ordering::SeqCst);
            self.reconciled.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn force_drain(&self) -> Result<(), MessageBrokerError> {
            self.reconciled.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn create_consumer(
            &self,
            _exchange_name: &str,
            _queue: &str,
            _handler: QueueDeliveryHandler,
        ) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn wait_for_completion(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        fn drain_reconciled(&self) -> bool {
            self.reconciled.load(Ordering::SeqCst)
        }

        fn delivery_terminal_snapshot(&self) -> DeliveryTerminalSnapshot {
            DeliveryTerminalSnapshot::default()
        }

        fn delivery_terminal_observations(&self) -> DeliveryTerminalObservationsSnapshot {
            DeliveryTerminalObservationsSnapshot::default()
        }
    }

    #[derive(Clone, Copy)]
    enum CloseBehavior {
        Succeed,
        FailThenSucceed,
    }

    struct RecordingConnectionLifecycle {
        close_behavior: CloseBehavior,
        start_calls: AtomicUsize,
        close_calls: AtomicUsize,
    }

    impl RecordingConnectionLifecycle {
        fn new(close_behavior: CloseBehavior) -> Arc<Self> {
            Arc::new(Self {
                close_behavior,
                start_calls: AtomicUsize::new(0),
                close_calls: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl RabbitMqConsumerConnectionLifecycle for RecordingConnectionLifecycle {
        async fn start(&self, _cancellation: CancellationToken) -> Result<(), MessageBrokerError> {
            self.start_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&self) -> Result<(), MessageBrokerError> {
            let call = self.close_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.close_behavior, CloseBehavior::FailThenSucceed) && call == 0 {
                Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                    "connection-close-sentinel".into(),
                )))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn queue_registration_adopts_scope_tracker_before_cancellable_engine_await() {
        let engine = PendingRegistrationEngine::new();
        let connection = RecordingConnectionLifecycle::new(CloseBehavior::Succeed);
        let consumer = Arc::new(RabbitMQConsumer::from_connection_lifecycle(
            engine.clone(),
            connection,
        ));
        let scope_tracker = Arc::new(DeliveryScopeTracker::default());
        let handler_tracker = Arc::clone(&scope_tracker);
        let callback: QueueDeliveryHandler = Arc::new(|_input| Box::pin(async { Ok(()) }));

        let registering_consumer = Arc::clone(&consumer);
        let registration = tokio::spawn(async move {
            registering_consumer
                .create_queue(
                    "events",
                    "orders.created",
                    RegisteredQueueHandler {
                        callback,
                        scope_tracker: handler_tracker,
                    },
                )
                .await
        });

        engine.create_entered.notified().await;
        assert!(
            !registration.is_finished(),
            "the deterministic engine seam must hold registration at its cancellable await"
        );
        let adopted = consumer.delivery_scope_trackers();
        assert_eq!(adopted.len(), 1);
        assert!(Arc::ptr_eq(&adopted[0], &scope_tracker));

        registration.abort();
        let join_error = registration
            .await
            .expect_err("the registration caller must be cancelled at the pending engine await");
        assert!(join_error.is_cancelled());

        let retained = consumer.delivery_scope_trackers();
        assert_eq!(retained.len(), 1);
        assert!(
            Arc::ptr_eq(&retained[0], &scope_tracker),
            "caller cancellation must not remove the provider-owned scope tracker"
        );
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    #[tokio::test]
    async fn provider_force_starts_transaction_runtime_even_when_engine_never_reconciles() {
        let engine = Arc::new(PendingForceEngine {
            force_calls: AtomicUsize::new(0),
        });
        let connection = RecordingConnectionLifecycle::new(CloseBehavior::Succeed);
        let relay = OutboxRelaySupervisor::new(Arc::new(NoopOutboxPublisher));
        let runtime = Arc::new(ForceObservedRuntime {
            active: AtomicUsize::new(1),
            force_calls: AtomicUsize::new(0),
        });
        let erased: Arc<dyn TransactionalRuntimeLifecycle> = runtime.clone();
        relay.track_transactional_runtime(
            Arc::from("test:provider-force"),
            erased,
            Duration::from_secs(1),
        );
        let mut consumer = RabbitMQConsumer::from_connection_lifecycle(engine.clone(), connection);
        consumer.outbox_relay = Some(relay);
        let consumer = Arc::new(consumer);

        let forcing_consumer = Arc::clone(&consumer);
        let forcing = tokio::spawn(async move { forcing_consumer.force_drain_async().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if runtime.force_calls.load(Ordering::Acquire) == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("relay force cancellation must not wait behind the stuck engine");

        assert_eq!(engine.force_calls.load(Ordering::Acquire), 1);
        assert_eq!(runtime.force_calls.load(Ordering::Acquire), 1);
        assert_eq!(runtime.active.load(Ordering::Acquire), 0);
        assert!(
            !forcing.is_finished(),
            "the engine seam must remain pending after relay force was broadcast"
        );
        forcing.abort();
        let join = forcing
            .await
            .expect_err("qualification force task is aborted");
        assert!(join.is_cancelled());
    }

    #[test]
    fn connection_disposal_requires_proven_delivery_task_join() {
        ensure_drain_reconciled(true).expect("joined delivery tasks permit connection disposal");
        let error = ensure_drain_reconciled(false)
            .expect_err("unreconciled delivery tasks must withhold connection disposal");
        assert!(error.to_string().contains("before connection disposal"));
    }

    #[tokio::test]
    async fn scope_drain_attempts_every_tracker_and_preserves_the_first_failure() {
        let failed = Arc::new(DeliveryScopeTracker::default());
        failed.begin();
        failed.mark_failed();
        failed.finish();

        let later = Arc::new(DeliveryScopeTracker::default());
        later.begin();
        let drain_later = Arc::clone(&later);
        let drain = tokio::spawn(async move {
            reconcile_delivery_scope_drains(Ok(()), vec![failed, drain_later]).await
        });

        tokio::task::yield_now().await;
        assert!(
            !drain.is_finished(),
            "a failed first tracker must not skip a later active tracker"
        );
        later.finish();

        let error = drain
            .await
            .expect("scope drain task must join")
            .expect_err("the first tracker failure must be reported");
        assert!(error.to_string().contains("delivery scope cleanup failed"));
        assert!(later.reconciled());
    }

    #[tokio::test]
    async fn engine_failure_does_not_skip_scope_reconciliation() {
        let tracker = Arc::new(DeliveryScopeTracker::default());
        tracker.begin();
        let drain_tracker = Arc::clone(&tracker);
        let drain = tokio::spawn(async move {
            reconcile_delivery_scope_drains(
                Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                    "engine-drain-sentinel".into(),
                ))),
                vec![drain_tracker],
            )
            .await
        });

        tokio::task::yield_now().await;
        assert!(
            !drain.is_finished(),
            "engine failure must not bypass an active delivery scope"
        );
        tracker.finish();

        let error = drain
            .await
            .expect("combined drain task must join")
            .expect_err("engine error must remain the primary failure");
        assert!(error.to_string().contains("engine-drain-sentinel"));
        assert!(tracker.reconciled());
    }

    #[tokio::test]
    async fn engine_start_failure_with_proven_close_is_terminal() {
        let engine = StartFailingEngine::new();
        let connection = RecordingConnectionLifecycle::new(CloseBehavior::Succeed);
        let consumer =
            RabbitMQConsumer::from_connection_lifecycle(engine.clone(), connection.clone());

        let error = consumer
            .start_async(CancellationToken::new())
            .await
            .expect_err("engine readiness failure must remain primary");
        assert!(error.to_string().contains("engine-start-sentinel"));
        assert_eq!(connection.start_calls.load(Ordering::SeqCst), 1);
        assert_eq!(connection.close_calls.load(Ordering::SeqCst), 1);

        consumer
            .stop_async()
            .await
            .expect("a provider with proven startup rollback is already terminal");
        assert_eq!(connection.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.stop_admission_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.drain_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn failed_readiness_close_remains_owned_for_disposal_retry() {
        let engine = StartFailingEngine::new();
        let connection = RecordingConnectionLifecycle::new(CloseBehavior::FailThenSucceed);
        let consumer =
            RabbitMQConsumer::from_connection_lifecycle(engine.clone(), connection.clone());

        let error = consumer
            .start_async(CancellationToken::new())
            .await
            .expect_err("engine readiness failure must remain primary");
        assert!(error.to_string().contains("engine-start-sentinel"));
        assert!(!error.to_string().contains("connection-close-sentinel"));
        assert_eq!(connection.close_calls.load(Ordering::SeqCst), 1);

        consumer
            .stop_async()
            .await
            .expect("DI disposal must retry the retained connection owner");
        consumer
            .stop_async()
            .await
            .expect("terminal disposal replay must be a no-op");

        assert_eq!(engine.stop_admission_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.drain_calls.load(Ordering::SeqCst), 1);
        assert_eq!(connection.close_calls.load(Ordering::SeqCst), 2);
    }
}
