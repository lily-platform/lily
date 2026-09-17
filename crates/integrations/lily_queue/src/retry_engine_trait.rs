use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use tokio_util::sync::CancellationToken;

use crate::settlement::HandoffPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryHandoff {
    RetryConfirmed,
    DeadLetterConfirmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureClass {
    Retryable,
    Permanent,
}

#[async_trait]
pub(crate) trait RetryEngine<M>: Send + Sync {
    #[allow(
        clippy::too_many_arguments,
        reason = "retry handoff requires destination identity, immutable payload/metadata, typed failure classification and cancellation"
    )]
    async fn retry(
        &self,
        worker: &str,
        queue: &str,
        body: &[u8],
        metadata: &M,
        plan: HandoffPlan,
        ct: CancellationToken,
    ) -> Result<RetryHandoff, MessageBrokerError>;
}
