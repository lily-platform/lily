// =============================================================================
// QueueClient Trait - High-Level Client Interface
// =============================================================================

use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use tokio_util::sync::CancellationToken;

use crate::{PublishEnvelope, PublishTerminalSnapshot};

/// Hidden RabbitMQ transport interface used by the public publishing service.
///
/// This is cross-crate qualification ABI, not an extension point for adding
/// broker providers. Lily V1 supports RabbitMQ only.
#[async_trait]
pub trait QueueClient: Send + Sync {
    /// Start the client and initialize connections
    ///
    /// # Arguments
    /// * `ct` - Cancellation token for graceful shutdown
    async fn start(&self, ct: CancellationToken) -> Result<(), MessageBrokerError>;

    /// Stop the client and cleanup resources
    async fn stop(&self) -> Result<(), MessageBrokerError>;

    /// Publish JSON bytes through a RabbitMQ exchange.
    ///
    /// # Arguments
    /// * `exchange` - RabbitMQ exchange name
    /// * `routing_key` - Routing key for message delivery
    /// * `message` - Serialized JSON bytes
    async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        message: Vec<u8>,
    ) -> Result<(), MessageBrokerError>;

    /// Publish raw bytes through a RabbitMQ exchange.
    ///
    /// # Arguments
    /// * `exchange` - RabbitMQ exchange name
    /// * `routing_key` - Routing key for message delivery
    /// * `body` - Raw message bytes
    async fn publish_raw(
        &self,
        exchange: &str,
        routing_key: &str,
        body: Vec<u8>,
    ) -> Result<(), MessageBrokerError>;

    /// Publish with a caller-owned stable event ID. Transactional outbox
    /// relays must use this method so a retry does not create a new identity.
    async fn publish_enveloped(
        &self,
        exchange: &str,
        routing_key: &str,
        body: Vec<u8>,
        envelope: PublishEnvelope,
    ) -> Result<(), MessageBrokerError>;

    /// Check if client is connected and healthy
    fn is_connected(&self) -> bool;

    /// Sampling-independent terminal accounting for admitted publish attempts.
    ///
    /// Implementations must not include destinations, event IDs, credentials
    /// or payloads in this snapshot.
    fn publish_terminal_snapshot(&self) -> PublishTerminalSnapshot;
}
