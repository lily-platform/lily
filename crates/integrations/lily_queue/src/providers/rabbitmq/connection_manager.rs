use crate::connection_manager_trait::ConnectionManager;
use async_trait::async_trait;
use lapin::Connection;
use lily_error::application::MessageBrokerError;
use lily_queue_client::{ConnectionManager as ClientConnectionManager, RabbitMqOptions};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) struct RabbitMQConnectionManager {
    inner: lily_queue_client::RabbitMQConnectionManager,
}
impl RabbitMQConnectionManager {
    pub(crate) fn new(options: RabbitMqOptions) -> Self {
        Self {
            inner: lily_queue_client::RabbitMQConnectionManager::new(options),
        }
    }

    pub(crate) async fn start(&self, ct: CancellationToken) -> Result<(), MessageBrokerError> {
        self.inner.start(ct).await
    }

    pub(crate) async fn close(&self) -> Result<(), MessageBrokerError> {
        self.inner.close().await
    }
}

#[async_trait]
impl ConnectionManager<Connection> for RabbitMQConnectionManager {
    #[lily_trace::prelude::instrument(name = "rabbitmq.connection.get", skip(self, ct))]
    async fn get_connection(
        &self,
        ct: CancellationToken,
    ) -> Result<Arc<Connection>, MessageBrokerError> {
        ClientConnectionManager::get_connection(&self.inner, ct).await
    }
}
