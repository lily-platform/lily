//! RabbitMQ-only broker error contract shared by Lily queue crates.

mod queue_handler_error;
mod rabbitmq_error;
pub use queue_handler_error::*;
pub use rabbitmq_error::*;

/// Top-level error returned by Lily's RabbitMQ broker implementation.
#[derive(Clone, Debug, PartialEq)]
pub enum MessageBrokerError {
    /// A RabbitMQ transport, configuration, publish, or consumer failure.
    RabbitMQError(RabbitMQError),
}

impl std::fmt::Display for MessageBrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageBrokerError::RabbitMQError(msg) => {
                write!(f, "MessageBrokerError: {msg}")
            }
        }
    }
}

impl std::error::Error for MessageBrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RabbitMQError(error) => Some(error),
        }
    }
}

// From implementations for error conversion
impl From<std::io::Error> for MessageBrokerError {
    fn from(error: std::io::Error) -> Self {
        MessageBrokerError::RabbitMQError(RabbitMQError::Io(error.to_string()))
    }
}

impl MessageBrokerError {
    /// Stable, secret-safe category for trace attributes and bounded metrics.
    pub const fn error_code(&self) -> &'static str {
        match self {
            MessageBrokerError::RabbitMQError(error) => error.error_code(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MessageBrokerError, RabbitMQError, RabbitMqConsumerTaskFailure,
        RabbitMqConsumerTaskFailureKind, RabbitMqConsumerTaskRole,
    };
    #[test]
    fn supervised_consumer_failure_has_stable_typed_evidence() {
        let error = MessageBrokerError::RabbitMQError(RabbitMQError::ConsumerTaskFailed(
            RabbitMqConsumerTaskFailure {
                queue: "orders.created".into(),
                role: RabbitMqConsumerTaskRole::Dispatcher,
                kind: RabbitMqConsumerTaskFailureKind::OperationFailed,
                operation_error_code: Some("BROKER_NACK"),
            },
        ));

        assert_eq!(error.error_code(), "BROKER_CONSUMER_TASK_FAILED");
        assert!(error.to_string().contains("orders.created"));
        assert!(error.to_string().contains("dispatcher"));
        assert!(error.to_string().contains("BROKER_NACK"));
    }

    #[test]
    fn io_errors_preserve_the_broker_io_category() {
        let error = MessageBrokerError::from(std::io::Error::other("socket closed"));

        assert_eq!(error.error_code(), "BROKER_IO");
        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Io(_))
        ));
    }
}
