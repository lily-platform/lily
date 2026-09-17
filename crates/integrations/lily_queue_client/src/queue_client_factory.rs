//! Named RabbitMQ publisher composition.
//!
//! The `factory` feature manages multiple RabbitMQ connections by cell name.

use async_trait::async_trait;
use lily_config::{ConfigService, LilyConfig, QueueClientCellConfig};
use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::{QueueClient, QueueClientService, RabbitMQClient, RabbitMqOptions};

/// Injectable singleton registry of named RabbitMQ publishers.
///
/// Available only with `default-features = false, features = ["factory"]`.
/// All configured cells are validated before broker I/O and become visible
/// atomically after every client starts successfully.
#[cfg(feature = "factory")]
#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct QueueClientFactory {
    cells: RwLock<HashMap<String, Arc<QueueClientService>>>,
    #[inject]
    config_service: Arc<ConfigService>,
}

#[cfg(feature = "factory")]
impl QueueClientFactory {
    /// Returns the initialized publisher registered under `name`.
    ///
    /// `None` means no configured factory cell has that exact name.
    pub fn get(&self, name: &str) -> Option<Arc<QueueClientService>> {
        self.cells.read().ok()?.get(name).cloned()
    }

    /// Creates and starts a RabbitMQ client from validated configuration.
    async fn create_rabbitmq_client(
        &self,
        options: RabbitMqOptions,
    ) -> Result<RabbitMQClient, InjectionError> {
        let client = RabbitMQClient::new(options);
        let ct = CancellationToken::new();

        client.start(ct.clone()).await.map_err(|e| {
            InjectionError::InitError(format!("Failed to start RabbitMQ client: {}", e))
        })?;
        Ok(client)
    }
}

#[cfg(feature = "factory")]
fn configured_factory_cells(
    config: &LilyConfig,
) -> Result<&[QueueClientCellConfig], InjectionError> {
    let queue_client = config.queue_client.as_ref();
    let mode = queue_client
        .and_then(|queue_client| queue_client.mode.as_deref())
        .unwrap_or("single");

    if mode != "factory" {
        return Err(InjectionError::InitError(
            "QueueClientFactory requires queue_client.mode = \"factory\" in configuration"
                .to_string(),
        ));
    }

    let cells = queue_client
        .and_then(|queue_client| queue_client.cells.as_deref())
        .filter(|cells| !cells.is_empty())
        .ok_or_else(|| {
            InjectionError::InitError(
                "No queue client cells configured. Please add [[queue_client.cells]] sections to lily.toml"
                    .to_string(),
            )
        })?;

    Ok(cells)
}

#[cfg(feature = "factory")]
#[async_trait]
impl ServiceTrait for QueueClientFactory {
    #[lily_trace::lily_trace(
        name = "queue_client_factory.initialize",
        skip(self),
        env = "development"
    )]
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        lily_trace::prelude::info!("Initializing QueueClientFactory");

        // Read one immutable typed snapshot so mode and every cell are from
        // the same configuration version throughout initialization.
        let config = self.config_service.get_lily_config().await;
        let configured_cells = configured_factory_cells(&config)?;
        let plans = configured_cells
            .iter()
            .map(|cell| {
                RabbitMqOptions::from_cell(cell)
                    .map(|options| (cell.name.clone(), options))
                    .map_err(|error| InjectionError::InitError(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut cells = HashMap::new();

        for (name, options) in plans {
            lily_trace::prelude::info!("Configuring queue client cell: {}", name);

            let client = self.create_rabbitmq_client(options).await?;

            // Create QueueClientService
            let queue_client_service = QueueClientService::new(Arc::new(client));

            cells.insert(name.clone(), Arc::new(queue_client_service));

            lily_trace::prelude::info!("✅ RabbitMQ queue client cell '{}' configured", name);
        }

        let cell_count = cells.len();
        self.cells = RwLock::new(cells);
        lily_trace::prelude::info!("QueueClientFactory initialized with {} cells", cell_count);
        Ok(())
    }

    #[lily_trace::lily_trace(
        name = "queue_client_factory.dispose",
        skip(self),
        env = "development"
    )]
    async fn dispose(&self) -> Result<(), InjectionError> {
        lily_trace::prelude::info!("Disposing QueueClientFactory");

        // Remove ownership before awaiting cleanup. A second disposal is then a
        // no-op, while all clients are still given a shutdown attempt.
        let cells = {
            let mut cells = self.cells.write().map_err(|_| {
                InjectionError::General("QueueClientFactory cell lock poisoned".into())
            })?;
            std::mem::take(&mut *cells)
        };
        let mut errors = Vec::new();
        for (name, service) in cells {
            lily_trace::prelude::info!("Stopping queue client cell: {}", name);
            match service.provider() {
                Ok(provider) => {
                    if let Err(error) = provider.stop().await {
                        errors.push(format!("{}: {}", name, error));
                    }
                }
                Err(error) => errors.push(format!("{}: {}", name, error)),
            }
        }

        if !errors.is_empty() {
            return Err(InjectionError::General(format!(
                "Failed to stop queue client cells: {}",
                errors.join("; ")
            )));
        }
        lily_trace::prelude::info!("QueueClientFactory disposed");
        Ok(())
    }
}

#[cfg(all(test, feature = "factory"))]
mod tests {
    use super::configured_factory_cells;
    use lily_config::{LilyConfig, QueueClientCellConfig, QueueClientConfig};

    fn config(mode: &str, cells: Option<Vec<QueueClientCellConfig>>) -> LilyConfig {
        LilyConfig {
            queue_client: Some(QueueClientConfig {
                mode: Some(mode.to_string()),
                cells,
                ..QueueClientConfig::default()
            }),
            ..LilyConfig::default()
        }
    }

    #[test]
    fn consumes_typed_queue_client_cells_without_flat_key_scanning() {
        let config = config(
            "factory",
            Some(vec![QueueClientCellConfig {
                name: "commands".to_string(),
                connection_string: None,
                pool_size: Some(8),
                connection_timeout_secs: Some(10),
                confirm_timeout_secs: Some(10),
                heartbeat_secs: Some(30),
                max_reconnect_attempts: Some(5),
                reconnect_backoff_millis: Some(250),
                use_tls: Some(true),
                tls: Default::default(),
                persistence_enabled: Some(true),
                username: Some("publisher".to_string()),
                password: Some("secret".to_string()),
                hostname: Some("rabbit.internal".to_string()),
                port: Some(5671),
                vhost: Some("/lily".to_string()),
            }]),
        );

        let cells = configured_factory_cells(&config).unwrap();

        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].name, "commands");
        assert_eq!(cells[0].pool_size, Some(8));
    }

    #[test]
    fn rejects_empty_factory_cells_before_broker_io() {
        let error = configured_factory_cells(&config("factory", Some(Vec::new())))
            .expect_err("an empty typed cell list must fail");

        assert!(error
            .to_string()
            .contains("No queue client cells configured"));
    }

    #[test]
    fn rejects_non_factory_mode_before_broker_io() {
        let error = configured_factory_cells(&config("single", None))
            .expect_err("the factory must reject single mode");

        assert!(error
            .to_string()
            .contains("queue_client.mode = \"factory\""));
    }
}
