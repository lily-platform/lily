use std::{fmt, future::Future, pin::Pin, sync::Arc};

use async_trait::async_trait;
use lily_error::application::{MessageBrokerError, QueueHandlerError};
use tokio_util::sync::CancellationToken;

use crate::{
    DeliveryTerminalObservationsSnapshot, DeliveryTerminalSnapshot, delivery_context::DeliveryInput,
};

/// Framework-private execution result crossing the queue-service/provider boundary.
///
/// User handlers can only produce [`QueueHandlerError`], which is always wrapped in
/// [`Self::Handler`]. Provider control flow must never be selected by matching a
/// user-controlled error code.
pub(crate) enum QueueExecutionError {
    Handler(QueueHandlerError),
    #[allow(
        dead_code,
        reason = "constructed only when a transactional MongoDB adapter feature is enabled"
    )]
    RequeueDeferred {
        code: &'static str,
    },
    #[allow(
        dead_code,
        reason = "constructed only when a transactional MongoDB adapter feature is enabled"
    )]
    FrameworkCancelled {
        code: &'static str,
    },
}

impl QueueExecutionError {
    #[allow(
        dead_code,
        reason = "called only when a transactional MongoDB adapter feature is enabled"
    )]
    pub(crate) const fn requeue_deferred(code: &'static str) -> Self {
        Self::RequeueDeferred { code }
    }

    pub(crate) const fn framework_cancelled(code: &'static str) -> Self {
        Self::FrameworkCancelled { code }
    }

    #[cfg(test)]
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::Handler(error) => error.code(),
            Self::RequeueDeferred { code } | Self::FrameworkCancelled { code } => code,
        }
    }
}

impl fmt::Debug for QueueExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Handler(error) => formatter
                .debug_struct("QueueExecutionError::Handler")
                .field("code", &error.code())
                .finish(),
            Self::RequeueDeferred { code } => formatter
                .debug_struct("QueueExecutionError::RequeueDeferred")
                .field("code", code)
                .finish(),
            Self::FrameworkCancelled { code } => formatter
                .debug_struct("QueueExecutionError::FrameworkCancelled")
                .field("code", code)
                .finish(),
        }
    }
}

impl From<QueueHandlerError> for QueueExecutionError {
    fn from(error: QueueHandlerError) -> Self {
        Self::Handler(error)
    }
}

pub(crate) type QueueDeliveryHandler = Arc<
    dyn Fn(DeliveryInput) -> Pin<Box<dyn Future<Output = Result<(), QueueExecutionError>> + Send>>
        + Send
        + Sync,
>;

#[async_trait]
pub(crate) trait QueueEngine: Send + Sync {
    /// Publish the composition root's absolute budget before shutdown starts.
    fn set_shutdown_deadlines(&self, _deadlines: crate::shutdown_budget::QueueShutdownDeadlines) {}

    /// Notify accepted work before any forced task termination is requested.
    fn cancel_execution(&self, _reason: crate::DeliveryCancellationReason) {}

    /// End cleanup authority only after the provider proved its child scopes terminal.
    fn finish_cleanup(&self) {}

    /// Start the engine and retain its shutdown token.
    async fn start(&self, ct: CancellationToken) -> Result<(), MessageBrokerError>;

    /// Reject new broker deliveries while settlement resources stay alive.
    async fn stop_admission(&self) -> Result<(), MessageBrokerError>;

    /// Await every owned receiver, dispatcher and handler task.
    async fn drain(&self) -> Result<(), MessageBrokerError>;

    /// Abort and join every retained runtime task after graceful drain was
    /// cancelled, failed or exceeded the process-level deadline.
    async fn force_drain(&self) -> Result<(), MessageBrokerError>;

    /// Register one queue and its owned-body async handler.
    async fn create_consumer(
        &self,
        exchange_name: &str,
        queue: &str,
        handler: QueueDeliveryHandler,
    ) -> Result<(), MessageBrokerError>;

    /// Wait for every supervised queue task to terminate.
    async fn wait_for_completion(&self) -> Result<(), MessageBrokerError>;

    /// Whether every retained receiver/dispatcher/handler task reached a
    /// joined terminal state. Broker and DI disposal must not overtake this
    /// proof, including after a force future times out.
    fn drain_reconciled(&self) -> bool;

    /// Sampling-independent settlement accounting for all deliveries admitted
    /// by this engine.
    fn delivery_terminal_snapshot(&self) -> DeliveryTerminalSnapshot;

    /// Bounded, sampling-independent event-level settlement observations.
    fn delivery_terminal_observations(&self) -> DeliveryTerminalObservationsSnapshot;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_error_code_cannot_select_framework_contention_control_flow() {
        for code in [
            "QUEUE_MONGODB_INBOX_CONTENTION_DEFERRED",
            "QUEUE_MONGODB_COMMIT_OUTCOME_DEFERRED",
            "QUEUE_MONGODB_TRANSACTION_CANCELLED",
        ] {
            let error = QueueExecutionError::from(QueueHandlerError::retryable(code));
            assert!(matches!(error, QueueExecutionError::Handler(_)));
        }
    }
}
