use std::time::Duration;

use lapin::{Confirmation, PublisherConfirm};
use lily_error::application::{message_broker::RabbitMQError, MessageBrokerError};
use tokio_util::sync::CancellationToken;

/// Successful terminal outcome of one mandatory RabbitMQ publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// RabbitMQ acknowledged the publish without returning it as unroutable.
    Confirmed,
}

/// Waits for and classifies a Lapin publisher confirmation under a timeout and
/// cancellation token.
///
/// This is hidden transport ABI shared with `lily_queue`; applications should
/// publish through `QueueClientService` when a composition feature is enabled.
pub async fn await_publisher_confirm(
    confirm: PublisherConfirm,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<PublishOutcome, MessageBrokerError> {
    if timeout.is_zero() {
        return Err(broker_error(RabbitMQError::Configuration(
            "publisher confirm timeout must be greater than zero".into(),
        )));
    }
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(broker_error(RabbitMQError::Cancelled)),
        _ = tokio::time::sleep(timeout) => Err(broker_error(RabbitMQError::Timeout("publisher confirm".into()))),
        result = confirm => {
            let confirmation = result.map_err(|error| broker_error(RabbitMQError::Lapin(error.to_string())))?;
            classify_confirmation(confirmation)
        }
    }
}

fn classify_confirmation(confirmation: Confirmation) -> Result<PublishOutcome, MessageBrokerError> {
    match confirmation {
        Confirmation::Ack(None) => Ok(PublishOutcome::Confirmed),
        Confirmation::Ack(Some(returned)) | Confirmation::Nack(Some(returned)) => {
            Err(broker_error(RabbitMQError::Unroutable(format!(
                "code={:?}, text={}",
                returned.reply_code, returned.reply_text
            ))))
        }
        Confirmation::Nack(None) => Err(broker_error(RabbitMQError::PublisherNack)),
        Confirmation::NotRequested => {
            Err(broker_error(RabbitMQError::PublisherConfirmNotRequested))
        }
    }
}

fn broker_error(error: RabbitMQError) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_ack_without_return_is_success() {
        assert_eq!(
            classify_confirmation(Confirmation::Ack(None)).unwrap(),
            PublishOutcome::Confirmed
        );
        assert!(matches!(
            classify_confirmation(Confirmation::Nack(None)),
            Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::PublisherNack
            ))
        ));
        assert!(matches!(
            classify_confirmation(Confirmation::NotRequested),
            Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::PublisherConfirmNotRequested
            ))
        ));
    }
}
