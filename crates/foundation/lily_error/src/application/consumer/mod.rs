use std::sync::Arc;

/// Cloneable ownership of a non-cloneable operational error source.
///
/// Its `Debug` and `Display` representations are deliberately redacted.
/// Call [`std::error::Error::source`] on the surrounding [`ConsumerError`] for
/// programmatic inspection of the original typed error.
#[derive(Clone)]
pub struct SharedConsumerErrorSource(Arc<dyn std::error::Error + Send + Sync>);

impl SharedConsumerErrorSource {
    fn new(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(error))
    }

    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self.0.as_ref()
    }
}

impl std::fmt::Debug for SharedConsumerErrorSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SharedConsumerErrorSource(<redacted>)")
    }
}

impl std::fmt::Display for SharedConsumerErrorSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("consumer operational error source is redacted")
    }
}

impl std::error::Error for SharedConsumerErrorSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.as_error())
    }
}

/// Bounded configuration failure categories detected before broker admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerConfigurationFailure {
    /// A caller-owned container and a builder-owned secret resolver were both supplied.
    BootstrapOwnershipConflict,
    /// Pipeline construction timeout was zero or exceeded the public bound.
    PipelineInitializationTimeoutInvalid,
    /// The effective configuration has no RabbitMQ consumer section.
    RabbitMqConsumerMissing,
    /// More than one AsyncAPI configuration was supplied to one Consumer builder.
    DuplicateAsyncApiConfiguration,
    /// A trace or component descriptor violated a bounded plan constraint.
    TraceDescriptorInvalid(ConsumerPlanFailureKind),
    /// Queue/handler selection violated the immutable execution-plan contract.
    ExecutionPlanInvalid(ConsumerPlanFailureKind),
}

/// Consumer AsyncAPI composition phase which failed before runtime commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerAsyncApiFailureStage {
    /// The accepted execution plan could not be projected into a valid document.
    DocumentBuild,
    /// The immutable document service could not be attached to the application provider.
    Attachment,
}

/// Stable public categories corresponding to bounded Consumer plan validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerPlanFailureKind {
    /// Configured physical queue count exceeded the bound.
    TooManyQueues,
    /// Linked handler count exceeded the bound.
    TooManyHandlers,
    /// Component trace-cell count exceeded the bound.
    TooManyTraceCells,
    /// A bounded descriptor contained an invalid value.
    DescriptorInvalid,
    /// Aggregate retained execution-plan bytes exceeded the bound.
    PlanBytesExceeded,
    /// Queue configuration failed canonical provider validation.
    QueueConfigurationInvalid,
    /// Queue concurrency exceeded the canonical bound.
    QueueConcurrencyExceeded,
    /// Queue prefetch exceeded the canonical bound.
    QueuePrefetchExceeded,
    /// A configured queue had no matching linked handler.
    HandlerMissing,
    /// More than one handler claimed the same version/content key.
    HandlerDuplicate,
    /// A transactional handler was linked while no matching transactional
    /// inbox storage adapter was compiled into the Consumer.
    TransactionalInboxFeatureDisabled,
    /// A transactional handler had no queue-level storage binding.
    TransactionalInboxBindingMissing,
    /// A queue-level storage binding had no transactional handler.
    TransactionalInboxBindingUnused,
    /// Component tracing configuration was invalid.
    TraceConfigurationInvalid,
    /// Required component trace identity was absent.
    TraceCellMissing,
    /// Component trace identity used an incompatible kind.
    TraceCellKindMismatch,
}

impl ConsumerPlanFailureKind {
    /// Stable low-cardinality diagnostic code.
    pub const fn error_code(self) -> &'static str {
        match self {
            Self::TooManyQueues => "CONSUMER_QUEUE_LIMIT",
            Self::TooManyHandlers => "CONSUMER_HANDLER_LIMIT",
            Self::TooManyTraceCells => "CONSUMER_TRACE_CELL_LIMIT",
            Self::DescriptorInvalid => "CONSUMER_DESCRIPTOR_INVALID",
            Self::PlanBytesExceeded => "CONSUMER_PLAN_BYTES_EXCEEDED",
            Self::QueueConfigurationInvalid => "CONSUMER_QUEUE_CONFIG_INVALID",
            Self::QueueConcurrencyExceeded => "CONSUMER_QUEUE_CONCURRENCY_EXCEEDED",
            Self::QueuePrefetchExceeded => "CONSUMER_QUEUE_PREFETCH_EXCEEDED",
            Self::HandlerMissing => "CONSUMER_HANDLER_MISSING",
            Self::HandlerDuplicate => "CONSUMER_HANDLER_DUPLICATE",
            Self::TransactionalInboxFeatureDisabled => {
                "CONSUMER_TRANSACTIONAL_INBOX_FEATURE_DISABLED"
            }
            Self::TransactionalInboxBindingMissing => {
                "CONSUMER_TRANSACTIONAL_INBOX_BINDING_MISSING"
            }
            Self::TransactionalInboxBindingUnused => "CONSUMER_TRANSACTIONAL_INBOX_BINDING_UNUSED",
            Self::TraceConfigurationInvalid => "CONSUMER_TRACE_CONFIG_INVALID",
            Self::TraceCellMissing => "CONSUMER_TRACE_CELL_MISSING",
            Self::TraceCellKindMismatch => "CONSUMER_TRACE_CELL_KIND_MISMATCH",
        }
    }
}

/// Tracing lifecycle phase which failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerTracingFailureStage {
    /// Strict tracing configuration loading or validation.
    Configuration,
    /// Process-global tracing runtime installation.
    Initialization,
    /// Bounded tracing exporter/provider shutdown.
    Shutdown,
}

/// Operating-system or managed signal observation phase which failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerSignalFailureStage {
    /// Platform signal handler installation.
    Installation,
    /// Waiting for the first platform signal.
    Receive,
    /// Stopping the platform signal monitor.
    Cleanup,
    /// Receiving a managed shutdown notification.
    ManagedReceive,
}

/// Managed Tokio task terminal failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerManagedTaskFailureKind {
    /// The runtime task panicked. Its panic payload is never exposed.
    Panicked,
    /// The runtime task was cancelled before producing a terminal result.
    Cancelled,
}

/// Typed aggregate shutdown failure class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerShutdownFailureKind {
    /// A component returned an operational failure or panicked.
    PrimaryFailure,
    /// At least one component could not prove terminal cleanup.
    Incomplete,
}

/// Bounded, payload-free aggregate shutdown evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerShutdownFailureEvidence {
    /// Aggregate failure class.
    pub kind: ConsumerShutdownFailureKind,
    /// Graceful component failures.
    pub failed: usize,
    /// Graceful component panics.
    pub panicked: usize,
    /// Graceful component timeouts.
    pub timed_out: usize,
    /// Components interrupted by force escalation.
    pub cancelled_by_force: usize,
    /// Force cleanups which failed.
    pub forced_cleanup_failed: usize,
    /// Force cleanups which panicked.
    pub forced_cleanup_panicked: usize,
    /// Force cleanups which timed out.
    pub forced_cleanup_timed_out: usize,
    /// Components requiring force cleanup without a force handle.
    pub forced_cleanup_unavailable: usize,
}

/// Primary-plus-secondary ordering for a multi-phase Consumer failure.
#[derive(Clone)]
pub struct ConsumerLifecycleFailures {
    primary: Box<ConsumerError>,
    secondary: Box<[ConsumerError]>,
}

impl ConsumerLifecycleFailures {
    /// Construct an ordered aggregate. The first failure is authoritative.
    pub fn new(primary: ConsumerError, secondary: Vec<ConsumerError>) -> Self {
        Self {
            primary: Box::new(primary),
            secondary: secondary.into_boxed_slice(),
        }
    }

    /// Authoritative runtime/startup failure.
    pub fn primary(&self) -> &ConsumerError {
        &self.primary
    }

    /// Cleanup and lifecycle failures observed after the primary failure.
    pub fn secondary_failures(&self) -> &[ConsumerError] {
        &self.secondary
    }
}

impl std::fmt::Debug for ConsumerLifecycleFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConsumerLifecycleFailures")
            .field("primary_code", &self.primary.error_code())
            .field("secondary_count", &self.secondary.len())
            .finish()
    }
}

/// Consumer bootstrap, runtime-supervision and shutdown failures.
#[derive(Clone)]
pub enum ConsumerError {
    /// Bounded configuration validation failed before admission.
    Configuration(ConsumerConfigurationFailure),
    /// Application dependency construction or resolution failed.
    DependencyInitialization {
        /// Typed DI source retained without formatting it into telemetry.
        source: crate::injection::InjectionError,
    },
    /// Application dependency disposal failed.
    DependencyDisposal {
        /// Typed DI source retained without formatting it into telemetry.
        source: crate::injection::InjectionError,
    },
    /// Tracing configuration, installation or shutdown failed.
    Tracing {
        /// Failing tracing phase.
        stage: ConsumerTracingFailureStage,
        /// Original non-cloneable error, when that phase returned one.
        source: Option<SharedConsumerErrorSource>,
    },
    /// AsyncAPI document projection or final service publication failed.
    AsyncApi {
        /// Failing AsyncAPI composition phase.
        stage: ConsumerAsyncApiFailureStage,
        /// Original typed error retained behind a cloneable, redacted owner.
        source: SharedConsumerErrorSource,
    },
    /// Queue pipeline compilation failed with a typed broker cause.
    PipelineInitialization {
        /// Typed compilation failure.
        source: crate::application::MessageBrokerError,
    },
    /// Broker topology or queue registration failed during startup.
    TopologyBootstrap {
        /// Typed broker failure.
        source: crate::application::MessageBrokerError,
        /// Optional DI rollback evidence when cleanup also failed.
        lifecycle: Option<Box<crate::injection::InjectionError>>,
    },
    /// Queue receiver/runtime construction failed.
    ReceiverStartup {
        /// Typed broker failure.
        source: crate::application::MessageBrokerError,
    },
    /// A supervised receiver or dispatcher terminated unexpectedly.
    RuntimeSupervision {
        /// Typed broker failure.
        source: crate::application::MessageBrokerError,
    },
    /// The managed runtime task did not return normally.
    ManagedRuntimeTask {
        /// Panic or cancellation category.
        kind: ConsumerManagedTaskFailureKind,
        /// Original Tokio join error; panic payload stays redacted.
        source: SharedConsumerErrorSource,
    },
    /// Runtime ended before managed readiness was published.
    ManagedStartupIncomplete,
    /// Managed task ownership disappeared without a replayable terminal result.
    ManagedRuntimeStateUnavailable,
    /// Platform or managed signal lifecycle failed.
    SignalHandling {
        /// Signal lifecycle stage.
        stage: ConsumerSignalFailureStage,
        /// Original signal/receive error.
        source: SharedConsumerErrorSource,
    },
    /// Initiating the durable shutdown transition failed.
    ShutdownInitiation {
        /// Original shutdown-state error.
        source: SharedConsumerErrorSource,
    },
    /// Framework shutdown completed with failed or unreconciled components.
    Shutdown {
        /// Bounded aggregate component evidence.
        evidence: ConsumerShutdownFailureEvidence,
    },
    /// A CRUD service operation invoked by the consumer failed.
    BaseService(crate::application::base_service::BaseServiceError),
    /// Ordered primary failure plus later cleanup failures.
    LifecycleFailures(ConsumerLifecycleFailures),
}

impl std::fmt::Debug for ConsumerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConsumerError")
            .field("error_code", &self.error_code())
            .finish()
    }
}

impl std::fmt::Display for ConsumerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "consumer operation failed [{}]",
            self.error_code()
        )
    }
}

impl std::error::Error for ConsumerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DependencyInitialization { source } | Self::DependencyDisposal { source } => {
                Some(source)
            }
            Self::Tracing {
                source: Some(source),
                ..
            }
            | Self::ManagedRuntimeTask { source, .. }
            | Self::SignalHandling { source, .. }
            | Self::ShutdownInitiation { source } => Some(source.as_error()),
            Self::AsyncApi { source, .. } => Some(source.as_error()),
            Self::PipelineInitialization { source }
            | Self::TopologyBootstrap { source, .. }
            | Self::ReceiverStartup { source }
            | Self::RuntimeSupervision { source } => Some(source),
            Self::BaseService(error) => Some(error),
            Self::LifecycleFailures(errors) => Some(errors.primary()),
            Self::Configuration(_)
            | Self::Tracing { source: None, .. }
            | Self::ManagedStartupIncomplete
            | Self::ManagedRuntimeStateUnavailable
            | Self::Shutdown { .. } => None,
        }
    }
}

impl ConsumerError {
    /// Stable, secret-safe, low-cardinality operational code.
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Configuration(failure) => match failure {
                ConsumerConfigurationFailure::BootstrapOwnershipConflict => {
                    "CONSUMER_BOOTSTRAP_OWNERSHIP_CONFLICT"
                }
                ConsumerConfigurationFailure::PipelineInitializationTimeoutInvalid => {
                    "CONSUMER_PIPELINE_TIMEOUT_INVALID"
                }
                ConsumerConfigurationFailure::RabbitMqConsumerMissing => {
                    "CONSUMER_RABBITMQ_CONFIG_MISSING"
                }
                ConsumerConfigurationFailure::DuplicateAsyncApiConfiguration => {
                    "CONSUMER_ASYNCAPI_CONFIGURATION_DUPLICATE"
                }
                ConsumerConfigurationFailure::TraceDescriptorInvalid(kind)
                | ConsumerConfigurationFailure::ExecutionPlanInvalid(kind) => kind.error_code(),
            },
            Self::DependencyInitialization { .. } => "CONSUMER_DEPENDENCY_INITIALIZATION",
            Self::DependencyDisposal { .. } => "CONSUMER_DEPENDENCY_DISPOSAL",
            Self::Tracing { stage, .. } => match stage {
                ConsumerTracingFailureStage::Configuration => "CONSUMER_TRACING_CONFIGURATION",
                ConsumerTracingFailureStage::Initialization => "CONSUMER_TRACING_INITIALIZATION",
                ConsumerTracingFailureStage::Shutdown => "CONSUMER_TRACING_SHUTDOWN",
            },
            Self::AsyncApi { stage, .. } => match stage {
                ConsumerAsyncApiFailureStage::DocumentBuild => "CONSUMER_ASYNCAPI_DOCUMENT_BUILD",
                ConsumerAsyncApiFailureStage::Attachment => "CONSUMER_ASYNCAPI_ATTACHMENT",
            },
            Self::PipelineInitialization { source }
            | Self::TopologyBootstrap { source, .. }
            | Self::ReceiverStartup { source }
            | Self::RuntimeSupervision { source } => source.error_code(),
            Self::ManagedRuntimeTask { kind, .. } => match kind {
                ConsumerManagedTaskFailureKind::Panicked => "CONSUMER_RUNTIME_TASK_PANICKED",
                ConsumerManagedTaskFailureKind::Cancelled => "CONSUMER_RUNTIME_TASK_CANCELLED",
            },
            Self::ManagedStartupIncomplete => "CONSUMER_MANAGED_STARTUP_INCOMPLETE",
            Self::ManagedRuntimeStateUnavailable => "CONSUMER_MANAGED_STATE_UNAVAILABLE",
            Self::SignalHandling { stage, .. } => match stage {
                ConsumerSignalFailureStage::Installation => "CONSUMER_SIGNAL_INSTALLATION",
                ConsumerSignalFailureStage::Receive => "CONSUMER_SIGNAL_RECEIVE",
                ConsumerSignalFailureStage::Cleanup => "CONSUMER_SIGNAL_CLEANUP",
                ConsumerSignalFailureStage::ManagedReceive => "CONSUMER_MANAGED_SHUTDOWN_RECEIVE",
            },
            Self::ShutdownInitiation { .. } => "CONSUMER_SHUTDOWN_INITIATION",
            Self::Shutdown { evidence } => match evidence.kind {
                ConsumerShutdownFailureKind::PrimaryFailure => "CONSUMER_SHUTDOWN_PRIMARY_FAILURE",
                ConsumerShutdownFailureKind::Incomplete => "CONSUMER_SHUTDOWN_INCOMPLETE",
            },
            Self::BaseService(_) => "CONSUMER_BASE_SERVICE",
            Self::LifecycleFailures(errors) => errors.primary().error_code(),
        }
    }

    /// Retain a typed DI initialization failure, promoting a nested broker
    /// source to the topology/bootstrap category.
    pub fn dependency_initialization(error: crate::injection::InjectionError) -> Self {
        if let Some(source) = error.message_broker_error().cloned() {
            let lifecycle = has_secondary_startup_failure(&error).then(|| Box::new(error));
            Self::TopologyBootstrap { source, lifecycle }
        } else {
            Self::DependencyInitialization { source: error }
        }
    }

    /// Retain one typed DI disposal failure.
    pub fn dependency_disposal(error: crate::injection::InjectionError) -> Self {
        Self::DependencyDisposal { source: error }
    }

    /// Retain a non-cloneable tracing source behind a cloneable typed phase.
    pub fn tracing(
        stage: ConsumerTracingFailureStage,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Tracing {
            stage,
            source: Some(SharedConsumerErrorSource::new(error)),
        }
    }

    /// Retain a typed AsyncAPI composition failure without exposing document details in logs.
    pub fn asyncapi(
        stage: ConsumerAsyncApiFailureStage,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::AsyncApi {
            stage,
            source: SharedConsumerErrorSource::new(error),
        }
    }

    /// Record tracing shutdown failure when only a terminal report exists.
    pub const fn tracing_shutdown_incomplete() -> Self {
        Self::Tracing {
            stage: ConsumerTracingFailureStage::Shutdown,
            source: None,
        }
    }

    /// Convert a Tokio task join failure without exposing its panic payload.
    pub fn managed_task(error: tokio::task::JoinError) -> Self {
        let kind = if error.is_panic() {
            ConsumerManagedTaskFailureKind::Panicked
        } else {
            ConsumerManagedTaskFailureKind::Cancelled
        };
        Self::ManagedRuntimeTask {
            kind,
            source: SharedConsumerErrorSource::new(error),
        }
    }

    /// Retain one platform or managed signal error.
    pub fn signal(
        stage: ConsumerSignalFailureStage,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::SignalHandling {
            stage,
            source: SharedConsumerErrorSource::new(error),
        }
    }

    /// Retain one shutdown-initiation error.
    pub fn shutdown_initiation(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::ShutdownInitiation {
            source: SharedConsumerErrorSource::new(error),
        }
    }

    /// Build a non-empty ordered lifecycle aggregate.
    pub fn aggregate(primary: ConsumerError, secondary: Vec<ConsumerError>) -> Self {
        if secondary.is_empty() {
            primary
        } else {
            Self::LifecycleFailures(ConsumerLifecycleFailures::new(primary, secondary))
        }
    }

    /// Returns the first typed message-broker failure retained by this error.
    pub fn message_broker_error(&self) -> Option<&crate::application::MessageBrokerError> {
        match self {
            Self::PipelineInitialization { source }
            | Self::TopologyBootstrap { source, .. }
            | Self::ReceiverStartup { source }
            | Self::RuntimeSupervision { source } => Some(source),
            Self::LifecycleFailures(errors) => {
                errors.primary().message_broker_error().or_else(|| {
                    errors
                        .secondary_failures()
                        .iter()
                        .find_map(Self::message_broker_error)
                })
            }
            _ => None,
        }
    }
}

impl From<crate::application::base_service::BaseServiceError> for ConsumerError {
    fn from(error: crate::application::base_service::BaseServiceError) -> Self {
        Self::BaseService(error)
    }
}

fn has_secondary_startup_failure(error: &crate::injection::InjectionError) -> bool {
    match error {
        crate::injection::InjectionError::InitializationCleanupFailed { .. }
        | crate::injection::InjectionError::StartupRollbackFailed { .. } => true,
        crate::injection::InjectionError::DependencyResolutionFailed { source, .. }
        | crate::injection::InjectionError::ServiceInitializationFailed { source, .. } => {
            has_secondary_startup_failure(source)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        application::message_broker::{
            RabbitMQError, RabbitMqTopologyError, RabbitMqTopologyErrorKind,
            RabbitMqTopologyOperation, RabbitMqTopologyResourceKind,
        },
        injection::InjectionError,
    };

    use super::{ConsumerAsyncApiFailureStage, ConsumerConfigurationFailure, ConsumerError};

    fn topology_failure() -> crate::application::MessageBrokerError {
        crate::application::MessageBrokerError::RabbitMQError(RabbitMQError::Topology(
            RabbitMqTopologyError {
                kind: RabbitMqTopologyErrorKind::PassiveResourceNotFound,
                operation: RabbitMqTopologyOperation::Verify,
                resource_kind: RabbitMqTopologyResourceKind::Queue,
                resource_name: "orders.external".into(),
            },
        ))
    }

    #[test]
    fn nested_di_startup_preserves_direct_typed_broker_error() {
        let error = InjectionError::ServiceInitializationFailed {
            service: "lily_queue::QueueService".into(),
            source: Box::new(InjectionError::MessageBroker(topology_failure())),
        };

        let consumer = ConsumerError::dependency_initialization(error);
        let ConsumerError::TopologyBootstrap {
            source,
            lifecycle: None,
        } = consumer
        else {
            panic!("broker startup failure must remain directly matchable");
        };
        assert_eq!(source.error_code(), "BROKER_TOPOLOGY_NOT_FOUND");
    }

    #[test]
    fn startup_rollback_preserves_typed_primary_and_secondary_evidence() {
        let error = InjectionError::StartupRollbackFailed {
            startup: Box::new(InjectionError::ServiceInitializationFailed {
                service: "lily_queue::QueueService".into(),
                source: Box::new(InjectionError::InitializationCleanupFailed {
                    service: "lily_queue::QueueService".into(),
                    initialization: Box::new(InjectionError::MessageBroker(topology_failure())),
                    cleanup: Box::new(InjectionError::DisposeError("close failed".into())),
                }),
            }),
            rollback_errors: vec!["config cleanup failed".into()],
            rollback_outcomes: Vec::new(),
            rollback_remaining: None,
        };

        let consumer = ConsumerError::dependency_initialization(error);
        let ConsumerError::TopologyBootstrap {
            source,
            lifecycle: Some(lifecycle),
        } = consumer
        else {
            panic!("secondary startup failures must retain a typed composite");
        };
        assert_eq!(source.error_code(), "BROKER_TOPOLOGY_NOT_FOUND");
        assert!(matches!(
            lifecycle.as_ref(),
            InjectionError::StartupRollbackFailed {
                rollback_errors,
                ..
            } if rollback_errors == &["config cleanup failed"]
        ));
    }

    #[test]
    fn debug_and_display_do_not_expose_underlying_source_text() {
        let error = ConsumerError::shutdown_initiation(std::io::Error::other(
            "redis://user:secret@example.invalid",
        ));
        assert!(!error.to_string().contains("secret"));
        assert!(!format!("{error:?}").contains("secret"));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn asyncapi_failures_have_stable_redacted_stage_codes() {
        let document = ConsumerError::asyncapi(
            ConsumerAsyncApiFailureStage::DocumentBuild,
            std::io::Error::other("amqp://guest:secret@example.invalid/private"),
        );
        let attachment = ConsumerError::asyncapi(
            ConsumerAsyncApiFailureStage::Attachment,
            std::io::Error::other("provider contains private application state"),
        );
        let duplicate = ConsumerError::Configuration(
            ConsumerConfigurationFailure::DuplicateAsyncApiConfiguration,
        );

        assert_eq!(document.error_code(), "CONSUMER_ASYNCAPI_DOCUMENT_BUILD");
        assert_eq!(attachment.error_code(), "CONSUMER_ASYNCAPI_ATTACHMENT");
        assert_eq!(
            duplicate.error_code(),
            "CONSUMER_ASYNCAPI_CONFIGURATION_DUPLICATE"
        );
        for error in [&document, &attachment] {
            assert!(!error.to_string().contains("secret"));
            assert!(!format!("{error:?}").contains("secret"));
            assert!(std::error::Error::source(error).is_some());
        }
    }
}
