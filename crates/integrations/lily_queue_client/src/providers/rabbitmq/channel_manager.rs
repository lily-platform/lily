// =============================================================================
// RabbitMQ Channel Manager - Channel Pooling with DashMap
// =============================================================================

use async_trait::async_trait;
use dashmap::DashMap;
use lapin::{options::ConfirmSelectOptions, Channel, Connection};
use lily_error::application::{message_broker::RabbitMQError, MessageBrokerError};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::{telemetry::RabbitMqClientMetrics, ChannelManager, ConnectionManager};

/// RabbitMQ channel manager with concurrent channel pooling
///
/// Performance optimizations:
/// - DashMap for lock-free concurrent access
/// - Separate pools for regular and confirm channels
/// - Channel reuse across operations
///
/// Opening a channel never declares or verifies broker topology. Topology is
/// an explicit concern and publishing to an exchange does not grant this
/// component permission to create it.
pub(crate) struct RabbitMQChannelManager {
    connection_manager: Arc<dyn ConnectionManager<Connection>>,

    /// Regular channels (no publisher confirms)
    channels: DashMap<(), Channel>,

    /// Confirm channels (with publisher confirms enabled)
    confirm_channels: DashMap<(), Channel>,
    metrics: RabbitMqClientMetrics,
}

impl RabbitMQChannelManager {
    pub(crate) fn new(connection_manager: Arc<dyn ConnectionManager<Connection>>) -> Self {
        Self {
            connection_manager,
            channels: DashMap::new(),
            confirm_channels: DashMap::new(),
            metrics: RabbitMqClientMetrics::default(),
        }
    }

    /// Drops cached channel handles without touching the owned connections.
    ///
    /// A stopped client may be started again with a fresh connection pool. In
    /// that case retaining channels from the previous pool would make the next
    /// publish fail on a closed channel instead of rebuilding it.
    pub(crate) fn clear(&self) {
        self.channels.clear();
        self.confirm_channels.clear();
        self.metrics.active_channels(false, 0);
        self.metrics.active_channels(true, 0);
    }
}

#[async_trait]
impl ChannelManager<Channel> for RabbitMQChannelManager {
    async fn get_channel(
        &self,
        ct: CancellationToken,
        confirm_channel: bool,
    ) -> Result<Channel, MessageBrokerError> {
        // Select appropriate channel pool
        let pool = if confirm_channel {
            &self.confirm_channels
        } else {
            &self.channels
        };

        // Fast path: only reuse a channel that still belongs to a live
        // connection. Lapin channel handles remain cloneable after the broker
        // closes them, so presence in the cache is not a readiness signal.
        if let Some(channel) = pool.get(&()).filter(|channel| channel.status().connected()) {
            return Ok(channel.clone());
        }
        pool.remove(&());
        self.metrics.channel_recovery(confirm_channel);

        // Slow path: create new channel
        let connection = self.connection_manager.get_connection(ct).await?;

        let channel = connection.create_channel().await.map_err(|e| {
            MessageBrokerError::RabbitMQError(RabbitMQError::General(format!(
                "Failed to create channel: {}",
                e
            )))
        })?;

        // Enable publisher confirms if requested
        if confirm_channel {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| {
                    MessageBrokerError::RabbitMQError(RabbitMQError::General(format!(
                        "Failed to enable publisher confirms: {}",
                        e
                    )))
                })?;
        }

        // Store in pool for reuse
        pool.insert((), channel.clone());
        self.metrics.active_channels(confirm_channel, pool.len());

        Ok(channel)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn channel_acquisition_has_no_topology_operation() {
        let production_source = include_str!("channel_manager.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production channel manager source");

        assert!(!production_source.contains("exchange_declare"));
        assert!(!production_source.contains("queue_declare"));
        assert!(!production_source.contains("queue_bind"));
    }
}
