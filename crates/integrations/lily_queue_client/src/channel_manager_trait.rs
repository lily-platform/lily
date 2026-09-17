// =============================================================================
// ChannelManager Trait - Channel/Session Management
// =============================================================================

use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use tokio_util::sync::CancellationToken;

/// Internal RabbitMQ channel-pool interface.
///
/// Responsibilities:
/// - Channel creation and reuse
/// - Channel pooling for performance
/// - Channel-level configuration
///
/// Topology declaration and verification deliberately do not belong to
/// channel acquisition. A publisher channel can target any exchange selected
/// by an individual publish operation.
#[async_trait]
pub(crate) trait ChannelManager<R>: Send + Sync {
    /// Gets or creates a destination-independent RabbitMQ channel.
    ///
    /// # Arguments
    /// * `cancel_token` - Cancellation token for graceful shutdown
    /// * `confirm_channel` - Whether to enable publisher confirms
    ///
    /// # Returns
    /// RabbitMQ channel for message operations
    async fn get_channel(
        &self,
        cancel_token: CancellationToken,
        confirm_channel: bool,
    ) -> Result<R, MessageBrokerError>;
}
