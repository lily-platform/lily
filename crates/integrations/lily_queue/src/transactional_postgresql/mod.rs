mod error;
mod migration;
mod storage;

use std::sync::Arc;

use lily_config::{QueueDefinition, TransactionalInboxBackend};
use lily_error::application::message_broker::RabbitMQError;
use lily_error::application::{MessageBrokerError, QueueHandlerError};
use lily_injection::ApplicationContainer;

pub use error::PostgresReliabilityError;
pub use migration::{PostgresInboxOutboxMigrationReport, PostgresInboxOutboxMigrator};
pub use storage::{ClaimedOutboxMessage, PostgresTransaction, PostgresTransactionalRuntime};

impl From<PostgresReliabilityError> for QueueHandlerError {
    fn from(error: PostgresReliabilityError) -> Self {
        let code = error.code();
        if matches!(error, PostgresReliabilityError::InvalidContract { .. }) {
            QueueHandlerError::permanent_with_source(code, error)
        } else {
            QueueHandlerError::retryable_with_source(code, error)
        }
    }
}

/// Resolves and validates the PostgreSQL binding for one physical queue.
///
/// This cross-crate framework ABI performs only dependency resolution and a
/// read-only schema version check. It never executes DDL.
pub async fn prepare_postgresql_transactional_runtime(
    container: Arc<ApplicationContainer>,
    definition: &QueueDefinition,
) -> Result<Arc<PostgresTransactionalRuntime>, MessageBrokerError> {
    let config = definition.transactional_inbox.clone().ok_or_else(|| {
        MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(
            "QUEUE_POSTGRES_BINDING_REQUIRED".to_owned(),
        ))
    })?;
    if config.backend != TransactionalInboxBackend::PostgreSql {
        return Err(MessageBrokerError::RabbitMQError(
            RabbitMQError::Configuration("QUEUE_POSTGRES_BACKEND_MISMATCH".to_owned()),
        ));
    }
    let mut runtime = PostgresTransactionalRuntime::resolve(container, config)
        .await
        .map_err(as_broker_configuration)?;
    runtime.bind_queue_identity(&definition.name);
    Ok(Arc::new(runtime))
}

fn as_broker_configuration(error: PostgresReliabilityError) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(error.code().to_owned()))
}
