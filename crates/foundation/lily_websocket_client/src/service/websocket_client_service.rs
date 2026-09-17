// =============================================================================
// WebSocketClientService - Single Mode with DI Support
// =============================================================================

use serde::Serialize;
use std::{sync::Arc, time::Duration};

#[cfg(feature = "single")]
use crate::BearerTokenFile;
use crate::{TokioWsClient, WebSocketError, WebSocketReply, WsClient};

// Single mode - with DI support
#[cfg(feature = "single")]
use async_trait::async_trait;
#[cfg(feature = "single")]
use lily_config::{ConfigService, WebSocketClientConfig};
#[cfg(feature = "single")]
use lily_error::injection::InjectionError;
#[cfg(feature = "single")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "single")]
use lily_injection::ServiceTrait;
#[cfg(feature = "single")]
use tokio_util::sync::CancellationToken;

/// DI-managed facade for the configured single WebSocket client.
///
/// Resolve this singleton from Lily's application container; constructing its
/// generated `Default` value directly leaves it uninitialized. Initialization
/// opens the configured socket before the service becomes resolvable.
#[cfg(feature = "single")]
#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct WebSocketClientService {
    client: Option<Arc<TokioWsClient>>,

    #[inject]
    config_service: Arc<ConfigService>,
}

/// One connected client cell owned by [`crate::WebSocketClientFactory`].
///
/// Obtain instances through [`crate::WebSocketClientFactory::get`].
#[cfg(all(feature = "factory", not(feature = "single")))]
pub struct WebSocketClientService {
    client: Option<Arc<TokioWsClient>>,
}

#[cfg(all(feature = "factory", not(feature = "single")))]
impl WebSocketClientService {
    /// Create a new WebSocketClientService (factory mode)
    pub(crate) fn new(client: Arc<TokioWsClient>) -> Self {
        Self {
            client: Some(client),
        }
    }
}

impl WebSocketClientService {
    /// Returns the underlying transport after successful initialization.
    pub fn client(&self) -> Result<Arc<TokioWsClient>, WebSocketError> {
        self.client.clone().ok_or(WebSocketError::NotInitialized)
    }

    /// Serializes and flushes one canonical Lily event through the configured client.
    #[lily_trace::lily_trace(name = "websocket_client.send", skip(self, data), env = "development")]
    pub async fn send<T: Serialize + Send>(
        &self,
        event: &str,
        data: T,
    ) -> Result<(), WebSocketError> {
        lily_trace::prelude::debug!("Sending WebSocket event");

        self.client()?.send(event, data).await.map(|_| {
            lily_trace::prelude::debug!("WebSocket event sent successfully");
        })
    }

    /// Sends one request and waits for its bounded correlated terminal reply.
    pub async fn request<T: Serialize + Send>(
        &self,
        event: &str,
        data: T,
        acknowledgement_timeout: Duration,
    ) -> Result<WebSocketReply, WebSocketError> {
        self.client()?
            .request(event, data, acknowledgement_timeout)
            .await
    }

    /// Serializes and flushes one canonical Lily UTF-8 text event.
    pub async fn send_text(&self, event: &str, text: String) -> Result<(), WebSocketError> {
        self.client()?.send_text_event(event, text).await
    }

    /// Serializes and flushes one canonical Lily binary event.
    pub async fn send_binary(&self, event: &str, data: Vec<u8>) -> Result<(), WebSocketError> {
        self.client()?.send_binary_event(event, data).await
    }

    /// Registers a synchronous listener on the configured client.
    ///
    /// Use [`Self::client`] and [`WsClient::on_async`] when the callback waits
    /// on I/O.
    pub fn on<F>(&self, event: &str, callback: F) -> Result<(), WebSocketError>
    where
        F: Fn(String, Vec<u8>) + Send + Sync + 'static,
    {
        lily_trace::prelude::debug!("Registering WebSocket event listener");
        self.client()?.on(event, callback);
        Ok(())
    }

    /// Returns whether this service currently owns an established socket.
    pub fn is_connected(&self) -> Result<bool, WebSocketError> {
        Ok(self.client()?.is_connected())
    }
}

// ============================================================================
// Dependency Injection Support (Single mode only)
// ============================================================================

#[cfg(feature = "single")]
#[async_trait]
impl ServiceTrait for WebSocketClientService {
    /// Initialize the WebSocket client service
    #[lily_trace::lily_trace(name = "websocket_client.initialize", skip(self), env = "development")]
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        lily_trace::prelude::info!("Initializing WebSocket client service");

        let lily_config = self.config_service.get_lily_config().await;

        let ws_client_config: WebSocketClientConfig =
            lily_config.websocket_client.ok_or_else(|| {
                lily_trace::prelude::error!(
                    "WebSocket client configuration not found in lily.toml"
                );
                InjectionError::General(
                    "WebSocket client configuration not found in lily.toml".to_string(),
                )
            })?;
        if ws_client_config.mode.as_deref().unwrap_or("single") != "single" {
            return Err(InjectionError::InitError(
                "WebSocketClientService requires websocket_client.mode = \"single\" in configuration"
                    .into(),
            ));
        }

        let url = ws_client_config.url.ok_or_else(|| {
            lily_trace::prelude::error!("WebSocket client URL not configured");
            InjectionError::General("WebSocket client URL not configured".to_string())
        })?;

        // Create WebSocket client configuration
        let mut headers = std::collections::HashMap::new();
        if let Some(origin) = ws_client_config.origin {
            headers.insert("origin".into(), origin);
        }
        let config = crate::WebSocketClientConfig {
            url: url.clone(),
            namespace: ws_client_config.namespace,
            reconnection: crate::ReconnectionConfig {
                enabled: ws_client_config.reconnection_enabled.unwrap_or(true),
                max_retries: ws_client_config.max_reconnection_attempts.unwrap_or(5),
                initial_delay_secs: ws_client_config.reconnection_delay_secs.unwrap_or(1),
                max_delay_secs: ws_client_config.max_reconnection_delay_secs.unwrap_or(30),
                backoff_multiplier: ws_client_config.backoff_multiplier.unwrap_or(2.0),
                jitter_ratio: ws_client_config.jitter_ratio.unwrap_or(0.2),
            },
            ping_interval_secs: ws_client_config.ping_interval_secs.unwrap_or(30),
            pong_timeout_secs: ws_client_config.pong_timeout_secs.unwrap_or(10),
            max_message_size: ws_client_config.max_message_size.unwrap_or(1024 * 1024),
            max_frame_size: ws_client_config.max_frame_size.unwrap_or(256 * 1024),
            headers,
            subprotocols: ws_client_config.subprotocols,
            require_subprotocol: ws_client_config.require_subprotocol.unwrap_or(false),
            additional_ca_bundle: ws_client_config.additional_ca_bundle,
            outbound_queue_capacity: ws_client_config.outbound_queue_capacity.unwrap_or(256),
            callback_queue_capacity: ws_client_config.callback_queue_capacity.unwrap_or(256),
            callback_concurrency: ws_client_config.callback_concurrency.unwrap_or(16),
            callback_timeout_secs: ws_client_config.callback_timeout_secs.unwrap_or(30),
            connect_timeout_secs: ws_client_config.connect_timeout_secs.unwrap_or(10),
            send_timeout_secs: ws_client_config.send_timeout_secs.unwrap_or(10),
            shutdown_timeout_secs: ws_client_config.shutdown_timeout_secs.unwrap_or(10),
            close_timeout_secs: ws_client_config.close_timeout_secs.unwrap_or(5),
            idle_timeout_secs: ws_client_config.idle_timeout_secs.unwrap_or(90),
        };
        lily_trace::prelude::debug!("Configuring WebSocket client: {:?}", config);

        // Create and connect client
        let client = TokioWsClient::with_config(config).map_err(|e| {
            lily_trace::prelude::error!("Failed to create WebSocket client: {}", e);
            InjectionError::General(e.to_string())
        })?;
        if let Some(path) = ws_client_config.authorization_bearer_file {
            let provider = BearerTokenFile::new(path)
                .map_err(|error| InjectionError::InitError(error.to_string()))?;
            client.set_auth_header_provider(Arc::new(provider)).await;
        }

        let ct = CancellationToken::new();
        lily_trace::prelude::info!("Connecting configured WebSocket client");

        client.connect(ct.clone()).await.map_err(|e| {
            lily_trace::prelude::error!("Failed to connect to WebSocket server: {}", e);
            InjectionError::General(e.to_string())
        })?;

        lily_trace::prelude::info!("Connected to WebSocket server successfully");

        self.client = Some(Arc::new(client));
        lily_trace::prelude::info!("WebSocket client service initialized successfully");

        Ok(())
    }

    /// Dispose of the WebSocket client service
    async fn dispose(&self) -> Result<(), InjectionError> {
        lily_trace::prelude::info!("Disposing WebSocket client service");

        if let Some(client) = &self.client {
            client
                .disconnect()
                .await
                .map_err(|e| InjectionError::General(e.to_string()))?;
        }

        lily_trace::prelude::info!("WebSocket client service disposed");
        Ok(())
    }
}
