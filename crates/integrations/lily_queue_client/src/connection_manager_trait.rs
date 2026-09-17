// =============================================================================
// ConnectionManager Trait - Connection Lifecycle Management
// =============================================================================

use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Hidden RabbitMQ connection-pool interface shared by transport components.
///
/// Responsibilities:
/// - Connection establishment and reuse
/// - Connection health monitoring
/// - Automatic reconnection on failure
/// - Thread-safe connection sharing
#[async_trait]
pub trait ConnectionManager<R>: Send + Sync {
    /// Gets a healthy pooled RabbitMQ connection, recovering it when needed.
    ///
    /// # Arguments
    /// * `ct` - Cancellation token for graceful shutdown
    ///
    /// # Returns
    /// Arc-wrapped RabbitMQ connection for thread-safe sharing
    async fn get_connection(&self, ct: CancellationToken) -> Result<Arc<R>, MessageBrokerError>;

    /// Checks whether at least one pooled RabbitMQ connection is healthy.
    ///
    /// # Returns
    /// true if connection is active and healthy
    fn is_connected(&self) -> bool;
}
