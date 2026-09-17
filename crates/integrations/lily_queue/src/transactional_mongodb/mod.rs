//! MongoDB adapter for Lily's transactional inbox/outbox contract.
//!
//! This module is available only through the explicit MongoDB transactional
//! feature family. It shares the application's existing MongoDB service and
//! never creates a client, database, collection or index during runtime
//! startup.

mod error;
mod migration;
mod storage;

use std::sync::Arc;

use lily_config::{QueueDefinition, TransactionalInboxBackend};
use lily_error::application::message_broker::RabbitMQError;
use lily_error::application::{MessageBrokerError, QueueHandlerError};
use lily_injection::ApplicationContainer;

pub use error::MongoReliabilityError;
pub use migration::{MongoInboxOutboxMigrationReport, MongoInboxOutboxMigrator};
pub(crate) use storage::MongoDeliveryExecution;
pub use storage::{ClaimedMongoOutboxMessage, MongoTransaction, MongoTransactionalRuntime};

impl From<MongoReliabilityError> for QueueHandlerError {
    fn from(error: MongoReliabilityError) -> Self {
        let code = error.code();
        if matches!(
            error,
            MongoReliabilityError::InvalidContract { .. }
                | MongoReliabilityError::SchemaMissing
                | MongoReliabilityError::SchemaOutdated { .. }
                | MongoReliabilityError::SchemaTooNew { .. }
                | MongoReliabilityError::SchemaDrift
                | MongoReliabilityError::TransactionTopologyUnsupported
        ) {
            QueueHandlerError::permanent_with_source(code, error)
        } else {
            QueueHandlerError::retryable_with_source(code, error)
        }
    }
}

/// Resolves and validates the MongoDB binding for one physical queue.
///
/// This cross-crate framework ABI performs only DI resolution, deployment
/// capability checks and read-only migration verification. Runtime startup
/// never creates a MongoDB collection or index.
pub async fn prepare_mongodb_transactional_runtime(
    container: Arc<ApplicationContainer>,
    definition: &QueueDefinition,
) -> Result<Arc<MongoTransactionalRuntime>, MessageBrokerError> {
    let config = definition.transactional_inbox.clone().ok_or_else(|| {
        MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(
            "QUEUE_MONGODB_BINDING_REQUIRED".to_owned(),
        ))
    })?;
    if config.backend != TransactionalInboxBackend::MongoDb {
        return Err(MessageBrokerError::RabbitMQError(
            RabbitMQError::Configuration("QUEUE_MONGODB_BACKEND_MISMATCH".to_owned()),
        ));
    }
    let mut runtime = MongoTransactionalRuntime::resolve(container, config)
        .await
        .map_err(as_broker_configuration)?;
    runtime.bind_queue_identity(&definition.name);
    Ok(Arc::new(runtime))
}

fn as_broker_configuration(error: MongoReliabilityError) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(error.code().to_owned()))
}
