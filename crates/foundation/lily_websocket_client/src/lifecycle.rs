//! Framework shutdown adapter for the canonical WebSocket client.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lily_shutdown::{FrameworkShutdownComponent, FrameworkShutdownPhase, ShutdownError};

use crate::{TokioWsClient, WsClient};

/// Application-owned client stop handle. `disconnect` performs graceful close,
/// bounds the wait, aborts a straggling supervisor, and awaits its JoinHandle.
pub struct WebSocketClientShutdownHandle {
    client: Arc<TokioWsClient>,
    timeout: Duration,
}

impl WebSocketClientShutdownHandle {
    /// Wraps a client for registration with Lily's framework shutdown
    /// coordinator.
    ///
    /// `timeout` is the coordinator budget for this component. The client's
    /// own close and shutdown budgets remain controlled by
    /// [`crate::WebSocketClientConfig`].
    pub fn new(client: Arc<TokioWsClient>, timeout: Duration) -> Self {
        Self { client, timeout }
    }
}

#[async_trait]
impl FrameworkShutdownComponent for WebSocketClientShutdownHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        self.client
            .disconnect()
            .await
            .map_err(|error| ShutdownError::Component(error.to_string()))
    }

    fn name(&self) -> &str {
        "websocket-client"
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        FrameworkShutdownPhase::DisposeDependencies
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn force_shutdown(&mut self) -> Option<lily_shutdown::FrameworkForceShutdownFuture<'_>> {
        self.client.request_shutdown();
        Some(Box::pin(async move { self.shutdown().await }))
    }
}

#[cfg(test)]
mod tests {
    use lily_shutdown::{FrameworkShutdownCoordinator, ShutdownSignal, ShutdownState};

    use super::*;
    use crate::WebSocketClientConfig;

    #[tokio::test]
    async fn disconnected_client_is_an_idempotent_framework_resource() {
        let client = Arc::new(
            TokioWsClient::with_config(WebSocketClientConfig {
                url: "ws://127.0.0.1:1/socket".to_string(),
                namespace: Some("test".into()),
                ..WebSocketClientConfig::default()
            })
            .unwrap(),
        );
        let state = Arc::new(ShutdownState::new());
        let mut coordinator = FrameworkShutdownCoordinator::new(state, Duration::from_secs(1));
        coordinator.register(WebSocketClientShutdownHandle::new(
            client,
            Duration::from_secs(1),
        ));

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert!(report.is_graceful());
        assert!(report.is_terminal_complete());
        assert!(report.reconciles());
    }
}
