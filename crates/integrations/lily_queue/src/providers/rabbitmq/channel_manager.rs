use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use lapin::{Channel, Connection, options::ConfirmSelectOptions};
use lily_error::application::{MessageBrokerError, message_broker::RabbitMQError};
use tokio_util::sync::CancellationToken;

use crate::{channel_manager_trait::ChannelManager, connection_manager_trait::ConnectionManager};
pub(crate) struct RabbitMQChannelManager {
    connection_manager: Arc<dyn ConnectionManager<Connection>>,

    channels: DashMap<String, Channel>,
    confirm_channels: DashMap<String, Channel>,
}

impl RabbitMQChannelManager {
    pub(crate) fn new(connection_manager: Arc<dyn ConnectionManager<Connection>>) -> Self {
        Self {
            connection_manager,
            channels: DashMap::new(),
            confirm_channels: DashMap::new(),
        }
    }

    async fn create_channel(
        &self,
        cancellation: CancellationToken,
        confirm_channel: bool,
    ) -> Result<Channel, MessageBrokerError> {
        let connection = self.connection_manager.get_connection(cancellation).await?;

        let channel = connection.create_channel().await.map_err(|error| {
            MessageBrokerError::RabbitMQError(RabbitMQError::General(error.to_string()))
        })?;

        if confirm_channel {
            channel
                .confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|error| {
                    MessageBrokerError::RabbitMQError(RabbitMQError::General(error.to_string()))
                })?;
        }

        Ok(channel)
    }
}

#[async_trait]
impl ChannelManager<Channel> for RabbitMQChannelManager {
    #[lily_trace::prelude::instrument(
        name = "rabbitmq.channel.get",
        skip(self, cancellation),
        fields(worker = %worker, confirm = confirm_channel)
    )]
    async fn get_channel(
        &self,
        worker: &str,
        cancellation: CancellationToken,
        confirm_channel: bool,
    ) -> Result<Channel, MessageBrokerError> {
        lily_trace::prelude::debug!("Getting channel for worker: {}", worker);

        let dict = if confirm_channel {
            &self.confirm_channels
        } else {
            &self.channels
        };

        if let Some(ch) = dict
            .get(worker)
            .filter(|channel| channel.status().connected())
        {
            return Ok(ch.clone());
        }
        dict.remove(worker);

        let channel = self.create_channel(cancellation, confirm_channel).await?;

        dict.insert(worker.to_string(), channel.clone());

        Ok(channel)
    }

    #[lily_trace::prelude::instrument(
        name = "rabbitmq.channel.get_dedicated",
        skip(self, cancellation),
        fields(worker = %worker, confirm = confirm_channel)
    )]
    async fn get_dedicated_channel(
        &self,
        worker: &str,
        cancellation: CancellationToken,
        confirm_channel: bool,
    ) -> Result<Channel, MessageBrokerError> {
        lily_trace::prelude::debug!("Opening dedicated channel for consumer worker: {}", worker);
        self.create_channel(cancellation, confirm_channel).await
    }
}
