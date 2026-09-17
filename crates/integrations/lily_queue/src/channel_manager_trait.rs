use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(crate) trait ChannelManager<R>: Send + Sync {
    /// Return a shared, cached channel whose lifetime is manager-owned.
    async fn get_channel(
        &self,
        worker: &str,
        cancel_token: CancellationToken,
        confirm_channel: bool,
    ) -> Result<R, MessageBrokerError>;

    /// Open an uncached channel whose complete lifecycle belongs to one
    /// consumer attempt.
    async fn get_dedicated_channel(
        &self,
        worker: &str,
        cancel_token: CancellationToken,
        confirm_channel: bool,
    ) -> Result<R, MessageBrokerError>;
}
