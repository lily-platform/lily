use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use tokio_util::sync::CancellationToken;

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use std::sync::Arc;

use crate::{
    DeliveryTerminalObservationsSnapshot, DeliveryTerminalSnapshot,
    queue_service::RegisteredQueueHandler,
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
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use crate::{TransactionalInboxSnapshot, outbox_relay::TransactionalOutboxRelaySnapshot};

/// Internal lifecycle and delivery contract implemented by the RabbitMQ
/// runtime.
#[async_trait]
pub(crate) trait Queue: Send + Sync {
    /// Adopt the composition root's deadlines without renewing its budget.
    fn set_shutdown_deadlines(&self, _deadlines: crate::shutdown_budget::QueueShutdownDeadlines) {}

    /// Execution notification is separate from admission, cleanup and settlement.
    fn cancel_execution(&self, _reason: crate::DeliveryCancellationReason) {}

    /// Register a queue and begin message delivery.
    ///
    /// `exchange_name` is the RabbitMQ exchange namespace; `queue` is the
    /// logical queue and `handler` receives one owned body at a time.
    async fn create_queue(
        &self,
        exchange_name: &str,
        queue: &str,
        handler: RegisteredQueueHandler,
    ) -> Result<(), MessageBrokerError>;

    /// Start broker resources and retain the runtime cancellation token.
    async fn start_async(&self, ct: CancellationToken) -> Result<(), MessageBrokerError>;

    /// Execute the complete provider-local shutdown sequence.
    async fn stop_async(&self) -> Result<(), MessageBrokerError>;

    /// Stops broker delivery admission without closing resources needed by
    /// in-flight handler settlement.
    async fn stop_admission_async(&self) -> Result<(), MessageBrokerError>;

    /// Awaits every receiver, handler, retry/confirm and ACK/NACK task.
    async fn drain_async(&self) -> Result<(), MessageBrokerError>;

    /// Bounds cooperative termination, then stops and joins remaining owners.
    async fn force_drain_async(&self) -> Result<(), MessageBrokerError>;

    /// Closes channels and connections after delivery drain.
    async fn close_async(&self) -> Result<(), MessageBrokerError>;

    /// Wait for all supervised runtime tasks to terminate.
    async fn wait_for_shutdown(&self) -> Result<(), MessageBrokerError>;

    /// Whether task/scope drain and broker connection close are both confirmed.
    fn close_reconciled(&self) -> bool {
        false
    }

    /// Whether all framework-owned task joins and scope receipts are terminal.
    fn drain_reconciled(&self) -> bool;

    /// Sampling-independent terminal settlement accounting.
    fn delivery_terminal_snapshot(&self) -> DeliveryTerminalSnapshot;

    /// Bounded, sampling-independent event-level settlement observations.
    fn delivery_terminal_observations(&self) -> DeliveryTerminalObservationsSnapshot;

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    async fn register_transactional_outbox(
        &self,
        _runtime: Arc<PostgresTransactionalRuntime>,
    ) -> Result<(), MessageBrokerError> {
        Err(MessageBrokerError::RabbitMQError(
            lily_error::application::message_broker::RabbitMQError::Configuration(
                "queue provider does not support PostgreSQL transactional outbox relay".into(),
            ),
        ))
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    async fn register_mongodb_transactional_outbox(
        &self,
        _runtime: Arc<MongoTransactionalRuntime>,
    ) -> Result<(), MessageBrokerError> {
        Err(MessageBrokerError::RabbitMQError(
            lily_error::application::message_broker::RabbitMQError::Configuration(
                "queue provider does not support MongoDB transactional outbox relay".into(),
            ),
        ))
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    fn transactional_outbox_snapshot(&self) -> TransactionalOutboxRelaySnapshot {
        TransactionalOutboxRelaySnapshot::default()
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    fn transactional_inbox_snapshot(&self) -> TransactionalInboxSnapshot {
        TransactionalInboxSnapshot::default()
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    fn transactional_outbox_ready(&self) -> bool {
        true
    }
}
