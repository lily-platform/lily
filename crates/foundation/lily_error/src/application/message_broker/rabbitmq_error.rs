/// Secret-safe failure category for RabbitMQ TLS material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RabbitMqTlsErrorKind {
    /// The configured additional CA bundle cannot be accessed.
    AdditionalCaUnavailable,
    /// The additional CA path is not a regular file.
    AdditionalCaNotRegularFile,
    /// The additional CA bundle is empty.
    AdditionalCaEmpty,
    /// The additional CA bundle exceeds its byte limit.
    AdditionalCaTooLarge,
    /// The additional CA bundle is not valid certificate PEM.
    AdditionalCaInvalid,
    /// The configured client certificate cannot be accessed.
    ClientCertificateUnavailable,
    /// The client certificate path is not a regular file.
    ClientCertificateNotRegularFile,
    /// The client certificate chain is empty.
    ClientCertificateEmpty,
    /// The client certificate chain exceeds its byte limit.
    ClientCertificateTooLarge,
    /// The client certificate chain is not valid certificate PEM.
    ClientCertificateInvalid,
    /// The configured client private key cannot be accessed.
    ClientPrivateKeyUnavailable,
    /// The client private-key path is not a regular file.
    ClientPrivateKeyNotRegularFile,
    /// The client private-key file is empty.
    ClientPrivateKeyEmpty,
    /// The client private-key file exceeds its byte limit.
    ClientPrivateKeyTooLarge,
    /// The client private key is unsupported or invalid.
    ClientPrivateKeyInvalid,
    /// The TLS stack rejected the assembled client identity.
    ClientIdentityRejected,
    /// The crypto provider cannot support the required TLS versions.
    UnsupportedProtocolVersions,
}

impl std::fmt::Display for RabbitMqTlsErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::AdditionalCaUnavailable => "additional CA bundle is unavailable",
            Self::AdditionalCaNotRegularFile => "additional CA bundle is not a regular file",
            Self::AdditionalCaEmpty => "additional CA bundle is empty",
            Self::AdditionalCaTooLarge => "additional CA bundle exceeds the size limit",
            Self::AdditionalCaInvalid => "additional CA bundle is not valid certificate PEM",
            Self::ClientCertificateUnavailable => "client certificate chain is unavailable",
            Self::ClientCertificateNotRegularFile => {
                "client certificate chain is not a regular file"
            }
            Self::ClientCertificateEmpty => "client certificate chain is empty",
            Self::ClientCertificateTooLarge => "client certificate chain exceeds the size limit",
            Self::ClientCertificateInvalid => {
                "client certificate chain is not valid certificate PEM"
            }
            Self::ClientPrivateKeyUnavailable => "client private key is unavailable",
            Self::ClientPrivateKeyNotRegularFile => "client private key is not a regular file",
            Self::ClientPrivateKeyEmpty => "client private key is empty",
            Self::ClientPrivateKeyTooLarge => "client private key exceeds the size limit",
            Self::ClientPrivateKeyInvalid => {
                "client private key must contain exactly one supported unencrypted PEM key"
            }
            Self::ClientIdentityRejected => {
                "client certificate and private key were rejected or do not match"
            }
            Self::UnsupportedProtocolVersions => {
                "TLS crypto provider does not support the default protocol versions"
            }
        })
    }
}

/// Supervised task role within a RabbitMQ consumer runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RabbitMqConsumerTaskRole {
    /// Receives deliveries from the RabbitMQ transport.
    Receiver,
    /// Dispatches buffered deliveries to handler workers.
    Dispatcher,
    /// Observes and coordinates the consumer tasks.
    Supervisor,
}

impl std::fmt::Display for RabbitMqConsumerTaskRole {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Receiver => "receiver",
            Self::Dispatcher => "dispatcher",
            Self::Supervisor => "supervisor",
        })
    }
}

/// Stable classification for a supervised consumer-task failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RabbitMqConsumerTaskFailureKind {
    /// The task returned although its lifecycle required it to remain active.
    UnexpectedExit,
    /// The task unwound with a panic.
    Panicked,
    /// The task was cancelled outside the expected shutdown path.
    Cancelled,
    /// A bounded runtime operation returned an error.
    OperationFailed,
}

impl std::fmt::Display for RabbitMqConsumerTaskFailureKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::UnexpectedExit => "unexpected exit",
            Self::Panicked => "panic",
            Self::Cancelled => "unexpected cancellation",
            Self::OperationFailed => "runtime operation failure",
        })
    }
}

/// RabbitMQ topology resource involved in a startup operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RabbitMqTopologyResourceKind {
    /// An AMQP exchange.
    Exchange,
    /// An AMQP queue.
    Queue,
    /// A queue-to-exchange binding.
    Binding,
    /// The accepted immutable topology plan as a whole.
    Plan,
}

impl std::fmt::Display for RabbitMqTopologyResourceKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Exchange => "exchange",
            Self::Queue => "queue",
            Self::Binding => "binding",
            Self::Plan => "topology plan",
        })
    }
}

/// Operation that produced a RabbitMQ topology failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RabbitMqTopologyOperation {
    /// Actively asserting a framework-managed resource.
    Declare,
    /// Passively verifying an externally managed resource.
    Verify,
    /// Binding a framework-managed queue to an exchange.
    Bind,
    /// Publishing before the required managed plan is ready.
    Publish,
}

impl std::fmt::Display for RabbitMqTopologyOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Declare => "declare",
            Self::Verify => "verify",
            Self::Bind => "bind",
            Self::Publish => "publish",
        })
    }
}

/// Stable RabbitMQ topology failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RabbitMqTopologyErrorKind {
    /// A resource exists with properties different from the accepted plan.
    DeclarationMismatch,
    /// RabbitMQ rejected the operation because the principal lacks permission.
    PermissionDenied,
    /// An externally managed resource was absent during passive verification.
    PassiveResourceNotFound,
    /// A managed destination was used before explicit topology bootstrap.
    NotReady,
}

impl RabbitMqTopologyErrorKind {
    /// Stable, secret-safe category for telemetry and operational policy.
    pub const fn error_code(self) -> &'static str {
        match self {
            Self::DeclarationMismatch => "BROKER_TOPOLOGY_MISMATCH",
            Self::PermissionDenied => "BROKER_TOPOLOGY_PERMISSION",
            Self::PassiveResourceNotFound => "BROKER_TOPOLOGY_NOT_FOUND",
            Self::NotReady => "BROKER_TOPOLOGY_NOT_READY",
        }
    }
}

impl std::fmt::Display for RabbitMqTopologyErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::DeclarationMismatch => "declaration mismatch",
            Self::PermissionDenied => "permission denied",
            Self::PassiveResourceNotFound => "passive resource not found",
            Self::NotReady => "topology not ready",
        })
    }
}

/// Secret-safe evidence for a RabbitMQ topology startup failure.
///
/// Resource identities originate from Lily's bounded, control-free topology
/// configuration. Broker reply text is deliberately not retained here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RabbitMqTopologyError {
    /// Stable failure category.
    pub kind: RabbitMqTopologyErrorKind,
    /// Operation that failed.
    pub operation: RabbitMqTopologyOperation,
    /// Resource category involved in the operation.
    pub resource_kind: RabbitMqTopologyResourceKind,
    /// Bounded configured resource identity.
    pub resource_name: String,
}

impl std::fmt::Display for RabbitMqTopologyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "RabbitMQ topology {} failed for {} {:?}: {}",
            self.operation, self.resource_kind, self.resource_name, self.kind
        )
    }
}

/// Structured evidence retained when a supervised consumer task terminates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RabbitMqConsumerTaskFailure {
    /// Queue whose consumer task failed.
    pub queue: String,
    /// Runtime role performed by the failed task.
    pub role: RabbitMqConsumerTaskRole,
    /// Terminal failure category.
    pub kind: RabbitMqConsumerTaskFailureKind,
    /// Optional stable code from the failing broker operation.
    pub operation_error_code: Option<&'static str>,
}

impl std::fmt::Display for RabbitMqConsumerTaskFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "RabbitMQ queue {:?} {} task failed: {}",
            self.queue, self.role, self.kind
        )?;
        if let Some(error_code) = self.operation_error_code {
            write!(formatter, " ({error_code})")?;
        }
        Ok(())
    }
}

/// RabbitMQ transport, configuration, publishing, and consuming failures.
#[derive(Clone, Debug, PartialEq)]
pub enum RabbitMQError {
    /// A broker operation failed without a narrower typed category.
    General(String),
    /// A broker I/O operation failed.
    Io(String),
    /// The underlying AMQP transport returned an error.
    Lapin(String),
    /// Loading or applying RabbitMQ TLS material failed.
    Tls(RabbitMqTlsErrorKind),
    /// RabbitMQ configuration is invalid.
    Configuration(String),
    /// RabbitMQ topology declaration or passive verification failed.
    Topology(RabbitMqTopologyError),
    /// An operation was attempted before broker initialization.
    NotInitialized,
    /// The operation was cancelled by shutdown or its owner.
    Cancelled,
    /// The named broker operation exceeded its deadline.
    Timeout(String),
    /// RabbitMQ negatively acknowledged a published message.
    PublisherNack,
    /// Publish confirmation was required but not requested on the channel.
    PublisherConfirmNotRequested,
    /// A mandatory message could not be routed.
    Unroutable(String),
    /// The delivered or published message violates its contract.
    InvalidMessage(String),
    /// Queue settlement failed with the supplied stable, secret-safe code.
    ///
    /// Provider errors and broker response details must remain in internal
    /// diagnostics rather than being embedded in this public error value.
    QueueSettlement(&'static str),
    /// A supervised consumer task terminated unexpectedly.
    ConsumerTaskFailed(RabbitMqConsumerTaskFailure),
}

impl std::fmt::Display for RabbitMQError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RabbitMQError::General(msg) => write!(f, "RabbitMQ error: {msg}"),
            RabbitMQError::Io(msg) => write!(f, "Io: {msg}"),
            RabbitMQError::Lapin(msg) => write!(f, "Lapin: {msg}"),
            RabbitMQError::Tls(msg) => write!(f, "Tls: {msg}"),
            RabbitMQError::Configuration(msg) => write!(f, "Configuration: {msg}"),
            RabbitMQError::Topology(error) => error.fmt(f),
            RabbitMQError::NotInitialized => write!(f, "RabbitMQ service is not initialized"),
            RabbitMQError::Cancelled => write!(f, "RabbitMQ operation was cancelled"),
            RabbitMQError::Timeout(operation) => {
                write!(f, "RabbitMQ {operation} timed out")
            }
            RabbitMQError::PublisherNack => {
                write!(f, "RabbitMQ negatively acknowledged the publish")
            }
            RabbitMQError::PublisherConfirmNotRequested => {
                write!(f, "RabbitMQ publisher confirms were not enabled")
            }
            RabbitMQError::Unroutable(reason) => {
                write!(f, "RabbitMQ returned the mandatory publish: {reason}")
            }
            RabbitMQError::InvalidMessage(reason) => {
                write!(f, "RabbitMQ message contract is invalid: {reason}")
            }
            RabbitMQError::QueueSettlement(_) => {
                write!(f, "RabbitMQ queue settlement failed")
            }
            RabbitMQError::ConsumerTaskFailed(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for RabbitMQError {}

// From implementations for error conversion
impl From<std::io::Error> for RabbitMQError {
    fn from(error: std::io::Error) -> Self {
        RabbitMQError::Io(error.to_string())
    }
}

impl RabbitMQError {
    /// Stable, secret-safe category for trace attributes and bounded metrics.
    pub const fn error_code(&self) -> &'static str {
        match self {
            Self::General(_) => "BROKER_GENERAL",
            Self::Io(_) => "BROKER_IO",
            Self::Lapin(_) => "BROKER_TRANSPORT",
            Self::Tls(_) => "BROKER_TLS",
            Self::Configuration(_) => "BROKER_CONFIGURATION",
            Self::Topology(error) => error.kind.error_code(),
            Self::NotInitialized => "BROKER_NOT_INITIALIZED",
            Self::Cancelled => "BROKER_CANCELLED",
            Self::Timeout(_) => "BROKER_TIMEOUT",
            Self::PublisherNack => "BROKER_NACK",
            Self::PublisherConfirmNotRequested => "BROKER_CONFIRM_NOT_REQUESTED",
            Self::Unroutable(_) => "BROKER_UNROUTABLE",
            Self::InvalidMessage(_) => "BROKER_INVALID_MESSAGE",
            Self::QueueSettlement(error_code) => error_code,
            Self::ConsumerTaskFailed(_) => "BROKER_CONSUMER_TASK_FAILED",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RabbitMQError, RabbitMqTopologyError, RabbitMqTopologyErrorKind, RabbitMqTopologyOperation,
        RabbitMqTopologyResourceKind,
    };
    use crate::application::MessageBrokerError;

    #[test]
    fn queue_settlement_error_returns_its_stable_code() {
        let error = RabbitMQError::QueueSettlement("QUEUE_SETTLEMENT_PANICKED");

        assert_eq!(error.error_code(), "QUEUE_SETTLEMENT_PANICKED");

        let error = MessageBrokerError::RabbitMQError(error);
        assert_eq!(error.error_code(), "QUEUE_SETTLEMENT_PANICKED");
    }

    #[test]
    fn topology_errors_keep_structured_secret_safe_evidence() {
        let error = RabbitMQError::Topology(RabbitMqTopologyError {
            kind: RabbitMqTopologyErrorKind::PassiveResourceNotFound,
            operation: RabbitMqTopologyOperation::Verify,
            resource_kind: RabbitMqTopologyResourceKind::Queue,
            resource_name: "orders.created".into(),
        });

        assert_eq!(error.error_code(), "BROKER_TOPOLOGY_NOT_FOUND");
        assert!(error.to_string().contains("orders.created"));
        assert!(!error.to_string().contains("reply"));
    }

    #[test]
    fn queue_settlement_display_is_generic_and_secret_safe() {
        const PROVIDER_DETAIL_SENTINEL: &str = "RAW_PROVIDER_DETAIL_MUST_NOT_BE_FORMATTED";
        let error = RabbitMQError::QueueSettlement(PROVIDER_DETAIL_SENTINEL);

        assert_eq!(error.to_string(), "RabbitMQ queue settlement failed");
        assert!(!error.to_string().contains(PROVIDER_DETAIL_SENTINEL));
    }
}
