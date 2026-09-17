// =============================================================================
// RabbitMQ Publisher Engine - High-Performance Message Publishing
// =============================================================================

use async_trait::async_trait;
use lapin::{options::BasicPublishOptions, BasicProperties, Channel};
use lily_error::application::{message_broker::RabbitMQError, MessageBrokerError};
use lily_trace::tracing::{self, Instrument};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::{
    await_publisher_confirm, ChannelManager, PublishOutcome, PublishTerminalLedger, PublisherEngine,
};

fn publish_outcome_label(outcome: &Result<PublishOutcome, MessageBrokerError>) -> &'static str {
    match outcome {
        Ok(_) => "ack",
        Err(MessageBrokerError::RabbitMQError(RabbitMQError::Unroutable(_))) => "return",
        Err(MessageBrokerError::RabbitMQError(RabbitMQError::PublisherNack)) => "nack",
        Err(MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(_))) => "timeout",
        Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled)) => "cancelled",
        Err(_) => "error",
    }
}

/// RabbitMQ publisher engine with publisher confirms
///
/// Performance optimizations:
/// - Publisher confirms for reliability
/// - Channel reuse via ChannelManager
/// - Async/await throughout
/// - Zero-copy message publishing where possible
pub(crate) struct RabbitMQPublisherEngine {
    channel_manager: Arc<dyn ChannelManager<Channel>>,
    confirm_timeout: std::time::Duration,
    terminal_ledger: Arc<PublishTerminalLedger>,
}

struct PublishAttemptGuard {
    ledger: Arc<PublishTerminalLedger>,
    finished: bool,
}

impl PublishAttemptGuard {
    fn begin(ledger: Arc<PublishTerminalLedger>) -> Self {
        ledger.begin();
        Self {
            ledger,
            finished: false,
        }
    }

    fn finish(mut self, outcome: &Result<PublishOutcome, MessageBrokerError>) {
        match outcome {
            Ok(PublishOutcome::Confirmed) => self.ledger.finish_ack(),
            Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::PublisherNack | RabbitMQError::Unroutable(_),
            )) => self.ledger.finish_nack_or_return(),
            Err(_) => self.ledger.finish_timeout_or_unresolved(),
        }
        self.finished = true;
    }
}

impl Drop for PublishAttemptGuard {
    fn drop(&mut self) {
        if !self.finished {
            // A dropped publish future is cancellation from the caller's
            // perspective. It must not disappear from terminal accounting.
            self.ledger.finish_timeout_or_unresolved();
        }
    }
}

impl RabbitMQPublisherEngine {
    pub(crate) fn with_terminal_ledger(
        channel_manager: Arc<dyn ChannelManager<Channel>>,
        confirm_timeout: std::time::Duration,
        terminal_ledger: Arc<PublishTerminalLedger>,
    ) -> Self {
        Self {
            channel_manager,
            confirm_timeout,
            terminal_ledger,
        }
    }
}

#[async_trait]
impl PublisherEngine<BasicProperties> for RabbitMQPublisherEngine {
    async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        body: Vec<u8>,
        properties: BasicProperties,
        ct: CancellationToken,
    ) -> Result<PublishOutcome, MessageBrokerError> {
        let started = std::time::Instant::now();
        let terminal_guard = PublishAttemptGuard::begin(Arc::clone(&self.terminal_ledger));
        let outcome = async {
            // Channel acquisition and basic_publish failures are part of the
            // same terminal attempt ledger as broker ACK/NACK/Return.
            let acquisition_started = std::time::Instant::now();
            let channel = self
                .channel_manager
                .get_channel(ct.clone(), true)
                .instrument(tracing::info_span!("messaging.publish.acquire"))
                .await?;
            self.terminal_ledger
                .record_acquisition_wait(acquisition_started.elapsed());
            let confirm = channel
                .basic_publish(
                    exchange.into(),
                    routing_key.into(),
                    BasicPublishOptions {
                        mandatory: true,
                        immediate: false,
                    },
                    &body,
                    properties,
                )
                .instrument(tracing::info_span!("messaging.publish.broker"))
                .await
                .map_err(|error| {
                    MessageBrokerError::RabbitMQError(RabbitMQError::General(format!(
                        "Failed to publish message: {error}"
                    )))
                })?;
            let confirm_started = std::time::Instant::now();
            let result = await_publisher_confirm(confirm, self.confirm_timeout, &ct)
                .instrument(tracing::info_span!("messaging.publish.confirm"))
                .await;
            self.terminal_ledger
                .record_confirm_wait(confirm_started.elapsed());
            result
        }
        .await;
        let result = publish_outcome_label(&outcome);
        self.terminal_ledger
            .record_publish_duration(started.elapsed(), result);
        terminal_guard.finish(&outcome);
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_terminal_labels_are_mutually_exclusive() {
        assert_eq!(publish_outcome_label(&Ok(PublishOutcome::Confirmed)), "ack");
        assert_eq!(
            publish_outcome_label(&Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::PublisherNack
            ))),
            "nack"
        );
        assert_eq!(
            publish_outcome_label(&Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::Unroutable("missing route".into())
            ))),
            "return"
        );
    }

    #[test]
    fn dropped_attempt_is_reconciled_as_unresolved() {
        let ledger = Arc::new(PublishTerminalLedger::default());
        drop(PublishAttemptGuard::begin(Arc::clone(&ledger)));
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.attempts, 1);
        assert_eq!(snapshot.timeout_or_unresolved, 1);
        assert!(snapshot.is_reconciled());
    }
}
