use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(crate) trait ConnectionManager<R>: Send + Sync {
    async fn get_connection(&self, ct: CancellationToken) -> Result<Arc<R>, MessageBrokerError>;
}
