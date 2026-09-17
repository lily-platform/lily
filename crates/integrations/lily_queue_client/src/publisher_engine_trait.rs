// =============================================================================
// PublisherEngine Trait - Message Publishing Engine
// =============================================================================

use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use tokio_util::sync::CancellationToken;

use crate::PublishOutcome;

/// Internal RabbitMQ publishing engine with mandatory confirmation semantics.
///
/// Responsibilities:
/// - Message publishing to RabbitMQ exchanges
/// - Publisher confirms for reliability
/// - Routing key management
/// - Message properties handling
#[async_trait]
pub(crate) trait PublisherEngine<P>: Send + Sync {
    /// Publishes bytes to the specified RabbitMQ exchange.
    ///
    /// # Arguments
    /// * `exchange` - RabbitMQ exchange name
    /// * `routing_key` - Routing key for message delivery
    /// * `body` - Message payload
    /// * `properties` - AMQP message properties
    /// * `ct` - Cancellation token for graceful shutdown
    ///
    /// # Returns
    /// Ok(()) if message was successfully published and confirmed
    async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        body: Vec<u8>,
        properties: P,
        ct: CancellationToken,
    ) -> Result<PublishOutcome, MessageBrokerError>;
}
