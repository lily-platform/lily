//! WebSocket Client Factory - Multi-Connection Management
//!
//! This module provides factory pattern for managing multiple WebSocket client connections.
//! Only available with "factory" feature flag.

use async_trait::async_trait;
use lily_config::{ConfigService, LilyConfig, WebSocketClientCellConfig};
use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::{BearerTokenFile, TokioWsClient, WebSocketClientService, WsClient};

#[derive(Debug, Clone, PartialEq)]
struct FactoryCellConfig {
    name: String,
    url: String,
    namespace: Option<String>,
    reconnection_enabled: bool,
    max_reconnection_attempts: usize,
    reconnection_delay_secs: u64,
    max_reconnection_delay_secs: u64,
    backoff_multiplier: f64,
    jitter_ratio: f64,
    ping_interval_secs: u64,
    pong_timeout_secs: u64,
    max_message_size: usize,
    max_frame_size: usize,
    origin: Option<String>,
    subprotocols: Vec<String>,
    require_subprotocol: bool,
    additional_ca_bundle: Option<std::path::PathBuf>,
    authorization_bearer_file: Option<std::path::PathBuf>,
    outbound_queue_capacity: usize,
    callback_queue_capacity: usize,
    callback_concurrency: usize,
    callback_timeout_secs: u64,
    connect_timeout_secs: u64,
    send_timeout_secs: u64,
    shutdown_timeout_secs: u64,
    close_timeout_secs: u64,
    idle_timeout_secs: u64,
}

impl From<&WebSocketClientCellConfig> for FactoryCellConfig {
    fn from(cell: &WebSocketClientCellConfig) -> Self {
        Self {
            name: cell.name.clone(),
            url: cell.url.clone(),
            namespace: cell.namespace.clone(),
            reconnection_enabled: cell.reconnection_enabled.unwrap_or(true),
            max_reconnection_attempts: cell.max_reconnection_attempts.unwrap_or(5),
            reconnection_delay_secs: cell.reconnection_delay_secs.unwrap_or(1),
            max_reconnection_delay_secs: cell.max_reconnection_delay_secs.unwrap_or(30),
            backoff_multiplier: cell.backoff_multiplier.unwrap_or(2.0),
            jitter_ratio: cell.jitter_ratio.unwrap_or(0.2),
            ping_interval_secs: cell.ping_interval_secs.unwrap_or(30),
            pong_timeout_secs: cell.pong_timeout_secs.unwrap_or(10),
            max_message_size: cell.max_message_size.unwrap_or(1024 * 1024),
            max_frame_size: cell.max_frame_size.unwrap_or(256 * 1024),
            origin: cell.origin.clone(),
            subprotocols: cell.subprotocols.clone(),
            require_subprotocol: cell.require_subprotocol.unwrap_or(false),
            additional_ca_bundle: cell.additional_ca_bundle.clone(),
            authorization_bearer_file: cell.authorization_bearer_file.clone(),
            outbound_queue_capacity: cell.outbound_queue_capacity.unwrap_or(256),
            callback_queue_capacity: cell.callback_queue_capacity.unwrap_or(256),
            callback_concurrency: cell.callback_concurrency.unwrap_or(16),
            callback_timeout_secs: cell.callback_timeout_secs.unwrap_or(30),
            connect_timeout_secs: cell.connect_timeout_secs.unwrap_or(10),
            send_timeout_secs: cell.send_timeout_secs.unwrap_or(10),
            shutdown_timeout_secs: cell.shutdown_timeout_secs.unwrap_or(10),
            close_timeout_secs: cell.close_timeout_secs.unwrap_or(5),
            idle_timeout_secs: cell.idle_timeout_secs.unwrap_or(90),
        }
    }
}

/// DI-managed collection of configured, named WebSocket client cells.
///
/// Resolve this singleton from Lily's application container when the crate is
/// built with only the `factory` feature. Initialization validates unique cell
/// names and opens every socket atomically; a later failure closes cells that
/// were already opened.
#[cfg(feature = "factory")]
#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct WebSocketClientFactory {
    cells: RwLock<HashMap<String, Arc<WebSocketClientService>>>,
    #[inject]
    config_service: Arc<ConfigService>,
}

#[cfg(feature = "factory")]
impl WebSocketClientFactory {
    /// Returns the connected client service registered under `name`.
    ///
    /// `None` means no configured cell has that exact name.
    pub fn get(&self, name: &str) -> Option<Arc<WebSocketClientService>> {
        self.cells.read().ok()?.get(name).cloned()
    }

    fn resolve_factory_cells(
        config: &LilyConfig,
    ) -> Result<Vec<FactoryCellConfig>, InjectionError> {
        let websocket_client = config.websocket_client.as_ref();
        let mode = websocket_client
            .and_then(|config| config.mode.as_deref())
            .unwrap_or("single");

        if mode != "factory" {
            return Err(InjectionError::InitError(
                "WebSocketClientFactory requires websocket_client.mode = \"factory\" in configuration"
                    .to_string(),
            ));
        }

        let cells = websocket_client
            .and_then(|config| config.cells.as_ref())
            .filter(|cells| !cells.is_empty())
            .ok_or_else(|| {
                InjectionError::InitError(
                    "No WebSocket client cells configured. Please add [[websocket_client.cells]] sections to lily.toml".to_string(),
                )
            })?;

        if cells.len() > 256 {
            return Err(InjectionError::InitError(
                "WebSocketClientFactory supports at most 256 configured cells".into(),
            ));
        }
        let mut names = HashSet::with_capacity(cells.len());
        for cell in cells {
            if cell.name.trim().is_empty() {
                return Err(InjectionError::InitError(
                    "WebSocket client cell names cannot be empty".into(),
                ));
            }
            if !names.insert(cell.name.as_str()) {
                return Err(InjectionError::InitError(format!(
                    "Duplicate WebSocket client cell name `{}`",
                    cell.name
                )));
            }
        }

        Ok(cells.iter().map(FactoryCellConfig::from).collect())
    }

    /// Create a WebSocket client from configuration
    async fn create_websocket_client(
        &self,
        cell: &FactoryCellConfig,
    ) -> Result<TokioWsClient, InjectionError> {
        let mut headers = HashMap::new();
        if let Some(origin) = &cell.origin {
            headers.insert("origin".into(), origin.clone());
        }
        let config = crate::WebSocketClientConfig {
            url: cell.url.clone(),
            namespace: cell.namespace.clone(),
            reconnection: crate::ReconnectionConfig {
                enabled: cell.reconnection_enabled,
                max_retries: cell.max_reconnection_attempts,
                initial_delay_secs: cell.reconnection_delay_secs,
                max_delay_secs: cell.max_reconnection_delay_secs,
                backoff_multiplier: cell.backoff_multiplier,
                jitter_ratio: cell.jitter_ratio,
            },
            ping_interval_secs: cell.ping_interval_secs,
            pong_timeout_secs: cell.pong_timeout_secs,
            max_message_size: cell.max_message_size,
            max_frame_size: cell.max_frame_size,
            headers,
            subprotocols: cell.subprotocols.clone(),
            require_subprotocol: cell.require_subprotocol,
            additional_ca_bundle: cell.additional_ca_bundle.clone(),
            outbound_queue_capacity: cell.outbound_queue_capacity,
            callback_queue_capacity: cell.callback_queue_capacity,
            callback_concurrency: cell.callback_concurrency,
            callback_timeout_secs: cell.callback_timeout_secs,
            connect_timeout_secs: cell.connect_timeout_secs,
            send_timeout_secs: cell.send_timeout_secs,
            shutdown_timeout_secs: cell.shutdown_timeout_secs,
            close_timeout_secs: cell.close_timeout_secs,
            idle_timeout_secs: cell.idle_timeout_secs,
        };

        let client = TokioWsClient::with_config(config).map_err(|e| {
            InjectionError::InitError(format!("Failed to create WebSocket client: {}", e))
        })?;
        if let Some(path) = &cell.authorization_bearer_file {
            let provider = BearerTokenFile::new(path.clone())
                .map_err(|error| InjectionError::InitError(error.to_string()))?;
            client.set_auth_header_provider(Arc::new(provider)).await;
        }

        let ct = CancellationToken::new();

        client.connect(ct.clone()).await.map_err(|e| {
            InjectionError::InitError(format!("Failed to connect WebSocket client: {}", e))
        })?;

        Ok(client)
    }

    async fn disconnect_cells(cells: HashMap<String, Arc<WebSocketClientService>>) -> Vec<String> {
        let mut tasks = tokio::task::JoinSet::new();
        for (name, service) in cells {
            tasks.spawn(async move {
                let result = match service.client() {
                    Ok(client) => client.disconnect().await,
                    Err(error) => Err(error),
                };
                (name, result)
            });
        }

        let mut errors = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((name, Err(error))) => errors.push(format!("{name}: {error}")),
                Ok((_, Ok(()))) => {}
                Err(error) => errors.push(format!("disconnect task failed: {error}")),
            }
        }
        errors
    }
}

#[cfg(feature = "factory")]
#[async_trait]
impl ServiceTrait for WebSocketClientFactory {
    #[lily_trace::lily_trace(
        name = "websocket_client_factory.initialize",
        skip(self),
        env = "development"
    )]
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        lily_trace::prelude::info!("Initializing WebSocketClientFactory");

        // Consume one immutable typed snapshot. Factory cells are standard
        // TOML array-of-tables; indexed flat-key probing can otherwise stop at
        // the first absent key and silently discard later cells.
        let lily_config = self.config_service.get_lily_config().await;
        let configured_cells = Self::resolve_factory_cells(&lily_config)?;
        let mut cells = HashMap::new();

        for cell in configured_cells {
            lily_trace::prelude::info!("Configuring WebSocket client cell: {}", cell.name);

            // Create WebSocket client. If a later cell fails, already-owned
            // sockets are closed before initialization returns the error.
            let client = match self.create_websocket_client(&cell).await {
                Ok(client) => client,
                Err(error) => {
                    let cleanup_errors = Self::disconnect_cells(cells).await;
                    if cleanup_errors.is_empty() {
                        return Err(error);
                    }
                    return Err(InjectionError::InitError(format!(
                        "{error}; rollback failures: {}",
                        cleanup_errors.join("; ")
                    )));
                }
            };

            // Create WebSocketClientService
            let ws_client_service = WebSocketClientService::new(Arc::new(client));

            cells.insert(cell.name.clone(), Arc::new(ws_client_service));

            lily_trace::prelude::info!("WebSocket client cell '{}' configured", cell.name);
        }

        let cell_count = cells.len();
        self.cells = RwLock::new(cells);
        lily_trace::prelude::info!(
            "WebSocketClientFactory initialized with {} cells",
            cell_count
        );
        Ok(())
    }

    #[lily_trace::lily_trace(
        name = "websocket_client_factory.dispose",
        skip(self),
        env = "development"
    )]
    async fn dispose(&self) -> Result<(), InjectionError> {
        lily_trace::prelude::info!("Disposing WebSocketClientFactory");

        let cells = {
            let mut cells = self.cells.write().map_err(|_| {
                InjectionError::General("WebSocketClientFactory cell lock poisoned".into())
            })?;
            std::mem::take(&mut *cells)
        };
        let errors = Self::disconnect_cells(cells).await;

        if !errors.is_empty() {
            return Err(InjectionError::General(format!(
                "Failed to disconnect WebSocket client cells: {}",
                errors.join("; ")
            )));
        }
        lily_trace::prelude::info!("WebSocketClientFactory disposed");
        Ok(())
    }
}

#[cfg(all(test, feature = "factory"))]
mod tests {
    use super::{FactoryCellConfig, WebSocketClientFactory};
    use lily_config::{
        LilyConfig, WebSocketClientCellConfig, WebSocketClientConfig as LilyWebSocketClientConfig,
    };

    fn cell(name: &str, url: &str) -> WebSocketClientCellConfig {
        WebSocketClientCellConfig {
            name: name.to_string(),
            url: url.to_string(),
            namespace: None,
            reconnection_enabled: None,
            max_reconnection_attempts: None,
            reconnection_delay_secs: None,
            max_reconnection_delay_secs: None,
            backoff_multiplier: None,
            jitter_ratio: None,
            ping_interval_secs: None,
            pong_timeout_secs: None,
            max_message_size: None,
            max_frame_size: None,
            origin: None,
            subprotocols: Vec::new(),
            require_subprotocol: None,
            additional_ca_bundle: None,
            authorization_bearer_file: None,
            outbound_queue_capacity: None,
            callback_queue_capacity: None,
            callback_concurrency: None,
            callback_timeout_secs: None,
            connect_timeout_secs: None,
            send_timeout_secs: None,
            shutdown_timeout_secs: None,
            close_timeout_secs: None,
            idle_timeout_secs: None,
        }
    }

    fn config(mode: &str, cells: Option<Vec<WebSocketClientCellConfig>>) -> LilyConfig {
        LilyConfig {
            websocket_client: Some(LilyWebSocketClientConfig {
                mode: Some(mode.to_string()),
                cells,
                ..LilyWebSocketClientConfig::default()
            }),
            ..LilyConfig::default()
        }
    }

    fn error_message(error: lily_error::injection::InjectionError) -> String {
        error.to_string()
    }

    #[test]
    fn resolves_typed_factory_cells_with_defaults_and_overrides() {
        let first = cell("primary", "wss://example.test/socket");
        let mut second = cell("backup", "ws://127.0.0.1:9000/socket");
        second.namespace = Some("events".to_string());
        second.reconnection_enabled = Some(false);
        second.max_reconnection_attempts = Some(9);
        second.reconnection_delay_secs = Some(2);
        second.max_reconnection_delay_secs = Some(45);
        second.backoff_multiplier = Some(3.0);
        second.jitter_ratio = Some(0.1);
        second.ping_interval_secs = Some(20);
        second.pong_timeout_secs = Some(7);
        second.max_message_size = Some(2048);
        second.max_frame_size = Some(1024);

        let resolved = WebSocketClientFactory::resolve_factory_cells(&config(
            "factory",
            Some(vec![first, second]),
        ))
        .unwrap();

        assert_eq!(
            resolved[0],
            FactoryCellConfig {
                name: "primary".to_string(),
                url: "wss://example.test/socket".to_string(),
                namespace: None,
                reconnection_enabled: true,
                max_reconnection_attempts: 5,
                reconnection_delay_secs: 1,
                max_reconnection_delay_secs: 30,
                backoff_multiplier: 2.0,
                jitter_ratio: 0.2,
                ping_interval_secs: 30,
                pong_timeout_secs: 10,
                max_message_size: 1024 * 1024,
                max_frame_size: 256 * 1024,
                origin: None,
                subprotocols: Vec::new(),
                require_subprotocol: false,
                additional_ca_bundle: None,
                authorization_bearer_file: None,
                outbound_queue_capacity: 256,
                callback_queue_capacity: 256,
                callback_concurrency: 16,
                callback_timeout_secs: 30,
                connect_timeout_secs: 10,
                send_timeout_secs: 10,
                shutdown_timeout_secs: 10,
                close_timeout_secs: 5,
                idle_timeout_secs: 90,
            }
        );
        assert_eq!(resolved[1].namespace.as_deref(), Some("events"));
        assert!(!resolved[1].reconnection_enabled);
        assert_eq!(resolved[1].max_reconnection_attempts, 9);
        assert_eq!(resolved[1].reconnection_delay_secs, 2);
        assert_eq!(resolved[1].max_reconnection_delay_secs, 45);
        assert_eq!(resolved[1].backoff_multiplier, 3.0);
        assert_eq!(resolved[1].jitter_ratio, 0.1);
        assert_eq!(resolved[1].ping_interval_secs, 20);
        assert_eq!(resolved[1].pong_timeout_secs, 7);
        assert_eq!(resolved[1].max_message_size, 2048);
        assert_eq!(resolved[1].max_frame_size, 1024);
    }

    #[test]
    fn rejects_empty_typed_factory_cells_without_opening_a_socket() {
        for cells in [None, Some(Vec::new())] {
            let error = WebSocketClientFactory::resolve_factory_cells(&config("factory", cells))
                .unwrap_err();
            assert!(error_message(error).contains("No WebSocket client cells configured"));
        }
    }

    #[test]
    fn rejects_wrong_mode_without_opening_a_socket() {
        let error = WebSocketClientFactory::resolve_factory_cells(&config(
            "single",
            Some(vec![cell("primary", "wss://example.test/socket")]),
        ))
        .unwrap_err();

        assert!(error_message(error)
            .contains("WebSocketClientFactory requires websocket_client.mode = \"factory\""));
    }

    #[test]
    fn rejects_duplicate_cell_owners_before_opening_a_socket() {
        let error = WebSocketClientFactory::resolve_factory_cells(&config(
            "factory",
            Some(vec![
                cell("primary", "wss://one.example.test/socket"),
                cell("primary", "wss://two.example.test/socket"),
            ]),
        ))
        .unwrap_err();

        assert!(error_message(error).contains("Duplicate WebSocket client cell name `primary`"));
    }
}
