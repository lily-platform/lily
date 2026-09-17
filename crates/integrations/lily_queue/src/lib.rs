#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! RabbitMQ consumer runtime and typed handler contracts for Lily applications.
//!
//! Application code normally uses four parts of this crate:
//!
//! - [`queue_service`] on an injectable service implementation;
//! - [`queue`] on each typed handler;
//! - typed delivery extractors such as [`Json`], [`DeliveryContext`] and
//!   [`Service`];
//! - optional [`QueueMiddleware`] and [`QueueGuard`] types selected globally
//!   through `lily_consumer::ConsumerBuilder`, or at service/handler level
//!   through [`middleware`] and [`guard`].
//!
//! Queue policy is not declared in an attribute. It has one authority:
//! `[[rabbitmq.topology.queues]]` in Lily configuration. `lily_consumer` loads
//! that configuration, discovers generated handler metadata, creates a fresh
//! DI scope for every delivery and owns graceful queue shutdown.
//! The one exception is handler processing intent: omitting
//! `delivery_guarantee` selects [`DeliveryGuarantee::AtLeastOnce`], while
//! `delivery_guarantee = "transactional_inbox"` records
//! [`DeliveryGuarantee::TransactionalInbox`]. The latter is storage-neutral
//! metadata and must be bound to a configured backend by the consumer before
//! broker admission.
//!
//! One configured physical queue may have several handlers when every
//! `(schema version, content kind)` pair is unique. Lily registers one RabbitMQ
//! consumer pipeline for that queue and performs exact selection through an
//! immutable local dispatch table before creating the delivery scope. Event
//! ID, positive schema-version and content-kind headers are mandatory; Lily
//! has no headerless V1/JSON fallback. Because RabbitMQ does not select
//! competing processes by those headers, every replica sharing a queue must
//! support every contract publishers can emit. Incompatible version-specific
//! binaries require separate physical queues or routing bindings.
//!
//! Publishing is intentionally a separate concern provided by
//! `lily_queue_client`. Optional PostgreSQL and MongoDB transactional
//! inbox/outbox profiles are transaction-bound: only mutations made through
//! the selected typed `PostgresTransaction` or `MongoTransaction` participate
//! in the effectively-once database boundary. Lily does not claim
//! broker/database 2PC or global exactly-once delivery.
//! The PostgreSQL and MongoDB queue features compile their adapter surfaces
//! without eagerly registering a database service; the matching
//! `lily_consumer` single/factory feature is the canonical composition
//! authority. A standalone QueueService owner must explicitly enable and own
//! the matching database crate's DI mode. MongoDB transient transaction labels
//! replay the whole delivery pipeline in a fresh DI scope, so
//! transaction-external side effects are outside the guarantee.
//!
//! Every effective middleware runs first in global, queue-service, handler
//! order. Guards then run in their own global, queue-service, handler order.
//! Declaration order is preserved within each category and level; interleaved
//! middleware/guard declarations do not create a cross-category order. A
//! concrete type is initialized once per Consumer build and shared by unrelated
//! handler plans. Repeating the same concrete type inside one effective plan is
//! a startup error, not an implicit deduplication.
//! Constructors are polled directly by the Consumer composition task under one
//! aggregate deadline. Error, panic, timeout or outer cancellation drops the
//! pending constructor before already initialized components are released once
//! in reverse constructor order; Lily leaves no framework-owned constructor
//! child task behind. Tasks explicitly spawned by application constructors are
//! application-owned.
//!
//! Queue-provider startup is owned before its first asynchronous operation.
//! If DI build fails, panics, or is cancelled while RabbitMQ is connecting,
//! Lily first drops the exact startup future and then stops the same retained
//! provider through the DI rollback ledger. A successful provider is stopped
//! by normal Consumer shutdown; application code does not own this boundary.
//! Every physical RabbitMQ registration is likewise adopted by the engine
//! before dedicated-channel acquisition or `Basic.Consume` starts. Caller
//! cancellation withdraws only the startup observer: the retained task still
//! performs bounded cancel/channel-close reconciliation, and cleanup which
//! cannot be proven prevents a second registration of the same physical queue
//! for that runtime.

#[cfg(all(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
compile_error!(
    "features `transactional-inbox-postgresql` and `transactional-inbox-postgresql-factory` are mutually exclusive"
);

#[cfg(all(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
compile_error!(
    "features `transactional-inbox-mongodb` and `transactional-inbox-mongodb-factory` are mutually exclusive"
);

mod cancellation;
mod channel_manager_trait;
mod connection_manager_trait;
mod delivery_context;
mod delivery_execution;
mod delivery_lifecycle;
mod delivery_termination;
mod extractor;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
mod lifecycle;
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
mod outbox_relay;
#[cfg(any(
    test,
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
mod owned_tasks;
mod pipeline;
mod providers;
mod queue_engine_trait;
mod queue_service;
mod queue_trait;
mod retry_engine_trait;
#[allow(missing_docs)]
mod setting;
mod settlement;
mod shutdown_budget;
mod telemetry;
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
mod transactional;
#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
mod transactional_mongodb;
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
mod transactional_postgresql;

pub use async_trait::async_trait;
pub use cancellation::DeliveryCancellationReason;
pub(crate) use delivery_context::DeliveryInput;
pub use delivery_context::{
    ContentKind, DeliveryCancellation, DeliveryContext, DeliveryDeadline, DeliveryHeaderValue,
    DeliveryHeaders, DeliveryProperties, EventId, MAX_QUEUE_CONTENT_KIND_BYTES, Redelivered,
    RetryCount, SchemaVersion,
};
pub use delivery_termination::{
    DeliveryCleanupCancellation, DeliveryNormalExit, DeliveryTerminationReason,
    QueueDeliveryTerminationContext,
};
pub use extractor::{
    BinaryPayload, DeliveryInvocation, DeliveryPayloadInput, FromDelivery, FromDeliveryParts, Json,
    Local, MAX_DELIVERY_LOCAL_ENTRIES, OptionalFromDeliveryParts, RawDelivery, Service,
    TextPayload,
};
/// Exact schema implementation used by Lily's optional AsyncAPI queue adapter.
#[cfg(feature = "asyncapi")]
pub use lily_asyncapi::schemars;
pub use lily_error::application::{QueueHandlerError, QueueHandlerFailureClass};
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
pub use lily_queue_client::{CustomPublishContent, PublishContentKind};
pub use lily_queue_derive::{asyncapi, guard, middleware, queue, queue_service};
pub use lily_queue_registry::{
    DeliveryGuarantee, QueueDeliveryOutcome, QueuePayloadKind, QueuePipelineComponentInitError,
};
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
pub use outbox_relay::{TransactionalOutboxRelaySnapshot, TransactionalOutboxRelayState};
pub use pipeline::{
    MAX_QUEUE_GUARDS, MAX_QUEUE_MIDDLEWARES, QueueDeliveryExchange, QueueGuard, QueueMiddleware,
};
pub use queue_service::QueueService;
pub use telemetry::{
    ConsumerRuntimeState, DeliveryTerminalObservation, DeliveryTerminalObservationsSnapshot,
    DeliveryTerminalOutcome, DeliveryTerminalSnapshot,
};
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
pub use transactional::{
    CleanupReport, TransactionalExecution, TransactionalInboxSnapshot,
    TransactionalOutboxContractError, TransactionalOutboxMessage,
};
#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
pub use transactional_mongodb::{
    MongoInboxOutboxMigrationReport, MongoInboxOutboxMigrator, MongoReliabilityError,
    MongoTransaction,
};
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
pub use transactional_postgresql::{
    PostgresInboxOutboxMigrationReport, PostgresInboxOutboxMigrator, PostgresReliabilityError,
    PostgresTransaction,
};

/// Generated-code and framework-integration ABI.
///
/// This module is public only because procedural macro output and
/// `lily_consumer` compile in different crates. It is not an application API,
/// and is excluded from normal documentation. The facade, derive and registry
/// crates are released in lockstep and exact-pinned while this ABI is private.
#[doc(hidden)]
pub mod __private {
    pub use crate::shutdown_budget::QueueShutdownDeadlines;
    use std::{sync::Arc, time::Duration};

    use lily_config::{QueueDefinition, RabbitMqConsumerConfig};
    use lily_error::application::MessageBrokerError;
    use lily_shutdown::FrameworkShutdownCoordinator;

    use crate::{QueueService, queue_trait::Queue, setting::MessageBrokerSetting};

    #[cfg(feature = "asyncapi")]
    pub use crate::extractor::delivery_asyncapi_payload;
    pub use crate::extractor::{
        DeliveryInvocation, delivery_input_contract, extract_delivery_arguments,
    };
    #[cfg(all(
        feature = "test-support",
        any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        )
    ))]
    pub use crate::outbox_relay::{OutboxPostConfirmProbe, install_outbox_post_confirm_probe};
    #[cfg(feature = "test-support")]
    pub use crate::providers::rabbitmq::queue_engine::{
        RabbitMqPostHandlerSettlementProbe, RabbitMqRegistrationHandoffProbe,
        install_rabbitmq_post_handler_settlement_probe,
        install_rabbitmq_registration_handoff_probe,
    };
    #[cfg(feature = "test-support")]
    pub use crate::queue_service::{
        QueueServiceTestLifecycleCall, QueueServiceTestLifecycleSnapshot, QueueServiceTestProbe,
    };
    pub use crate::{
        pipeline::{
            QueueHandlerCompilationInput, queue_guard_registration, queue_middleware_registration,
        },
        queue_service::{CompiledQueueDispatch, CompiledQueueHandler, compile_queue_handlers},
    };
    #[cfg(feature = "asyncapi")]
    pub use lily_asyncapi::__private::SchemaFactory;
    pub use lily_error::application::{
        MessageBrokerError as GeneratedMessageBrokerError, QueueHandlerError,
        message_broker::RabbitMQError as GeneratedRabbitMQError,
    };
    pub use lily_queue_registry::{
        DeliveryGuarantee, HandlerFunction, QUEUE_HANDLER_GETTERS, QueueGuardRegistration,
        QueueHandlerInputContract, QueueHandlerMetadata, QueueMiddlewareRegistration,
        QueuePayloadKind, get_all_queue_handlers, linkme,
    };
    #[cfg(feature = "asyncapi")]
    pub use lily_queue_registry::{
        QueueAsyncApiPayload, QueueAsyncApiRegistration, QueueAsyncApiStatus,
    };
    pub use serde_json;

    /// Resolve the exact, validated RabbitMQ virtual host used by the consumer
    /// transport without exposing the credential-bearing connection URI.
    ///
    /// This is private facade ABI for sibling Lily composition crates. Public
    /// applications should continue to configure the virtual host through
    /// [`RabbitMqConsumerConfig`].
    pub fn rabbitmq_consumer_virtual_host(
        config: &RabbitMqConsumerConfig,
    ) -> Result<String, MessageBrokerError> {
        lily_queue_client::RabbitMqOptions::from_consumer(config)?.virtual_host()
    }

    /// Exact PostgreSQL query crates used by feature-gated framework
    /// qualification targets. This remains hidden framework ABI; application
    /// handlers canonically import these re-exports from `lily_postgresql`.
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    pub mod postgresql_reexports {
        pub use lily_postgresql::{diesel, diesel_async};
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub use crate::transactional::PreparedTransactionalRuntime;
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub use crate::transactional::{CleanupReport, TransactionalExecution};
    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub use crate::transactional_mongodb::{
        ClaimedMongoOutboxMessage, MongoTransactionalRuntime, prepare_mongodb_transactional_runtime,
    };
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    pub use crate::transactional_postgresql::{
        ClaimedOutboxMessage, PostgresTransactionalRuntime,
        prepare_postgresql_transactional_runtime,
    };

    /// Maximum accepted per-queue handler concurrency.
    pub const MAX_QUEUE_CONCURRENCY: u32 = crate::setting::MAX_QUEUE_CONCURRENCY;

    /// Maximum accepted RabbitMQ prefetch count.
    pub const MAX_QUEUE_PREFETCH: u16 = crate::setting::MAX_QUEUE_PREFETCH;

    /// Opaque access to one initialized queue runtime.
    #[derive(Clone)]
    pub struct QueueRuntimeHandle(Arc<dyn Queue>);

    /// Cloneable typed error evidence retained by queue shutdown adapters.
    #[derive(Clone)]
    pub struct QueueLifecycleEvidence(crate::lifecycle::QueueLifecycleEvidence);

    impl QueueLifecycleEvidence {
        /// First typed provider failure observed in canonical shutdown order.
        pub fn primary_error(&self) -> Option<MessageBrokerError> {
            self.0.primary_error()
        }

        /// Distinct typed provider failures in canonical shutdown order.
        pub fn failures(&self) -> Vec<MessageBrokerError> {
            self.0.failures()
        }
    }

    impl QueueRuntimeHandle {
        /// Wait until the queue runtime terminates.
        pub async fn wait_for_shutdown(&self) -> Result<(), MessageBrokerError> {
            self.0.wait_for_shutdown().await
        }

        /// Whether the runtime proved that every delivery task was joined.
        pub fn drain_reconciled(&self) -> bool {
            self.0.drain_reconciled()
        }

        /// Whether both runtime drain and broker connection disposal completed.
        pub fn close_reconciled(&self) -> bool {
            self.0.close_reconciled()
        }

        /// Read the aggregate, sampling-independent delivery and readiness
        /// state retained by this runtime.
        ///
        /// This intentionally excludes payloads, event identifiers and broker
        /// connection details. It exists for application adapters such as
        /// `lily_consumer` to expose one canonical operational snapshot without
        /// making the internal queue provider part of their public API.
        pub fn delivery_terminal_snapshot(&self) -> crate::DeliveryTerminalSnapshot {
            self.0.delivery_terminal_snapshot()
        }

        /// Read transactional outbox relay health without exposing provider internals.
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        pub fn transactional_outbox_snapshot(&self) -> crate::TransactionalOutboxRelaySnapshot {
            self.0.transactional_outbox_snapshot()
        }

        /// Read sampling-independent transactional inbox execution evidence.
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        pub fn transactional_inbox_snapshot(&self) -> crate::TransactionalInboxSnapshot {
            self.0.transactional_inbox_snapshot()
        }

        /// Whether every configured transactional outbox relay is ready.
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        pub fn transactional_outbox_ready(&self) -> bool {
            self.0.transactional_outbox_ready()
        }
    }

    /// Obtain the opaque runtime owned by an initialized queue service.
    pub fn queue_runtime(service: &QueueService) -> Result<QueueRuntimeHandle, MessageBrokerError> {
        service.provider().map(QueueRuntimeHandle)
    }

    /// Validate queue definitions through the same setting conversion used at
    /// runtime without exposing mutable runtime settings.
    pub fn validate_queue_definitions(
        definitions: &[QueueDefinition],
    ) -> Result<(), MessageBrokerError> {
        MessageBrokerSetting::from_definitions(Duration::from_secs(1), definitions).map(|_| ())
    }

    /// Construct a transport-free queue service seed for framework contract
    /// tests. This API is absent unless the non-default `test-support` feature
    /// is explicitly enabled.
    #[cfg(feature = "test-support")]
    pub fn queue_service_test_seed(
        config_service: Arc<lily_config::ConfigService>,
    ) -> (QueueService, QueueServiceTestProbe) {
        QueueService::test_support_seed(config_service)
    }

    /// Register one previously compiled immutable version/content dispatcher.
    pub async fn register_compiled_dispatch(
        queue_service: &QueueService,
        exchange_name: &str,
        dispatch: CompiledQueueDispatch,
    ) -> Result<(), MessageBrokerError> {
        queue_service
            .register_compiled_dispatch(exchange_name, dispatch)
            .await
    }

    /// Attach one prepared PostgreSQL transaction runtime and its relay before listener admission.
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    pub async fn register_postgresql_transactional_runtime(
        queue_service: &QueueService,
        runtime: Arc<PostgresTransactionalRuntime>,
    ) -> Result<(), MessageBrokerError> {
        queue_service.register_transactional_outbox(runtime).await
    }

    /// Attach one prepared MongoDB transaction runtime and its relay before listener admission.
    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub async fn register_mongodb_transactional_runtime(
        queue_service: &QueueService,
        runtime: Arc<MongoTransactionalRuntime>,
    ) -> Result<(), MessageBrokerError> {
        queue_service
            .register_mongodb_transactional_outbox(runtime)
            .await
    }

    /// Attach queue admission, drain and connection disposal to framework
    /// shutdown ordering.
    pub fn register_queue_lifecycle(
        coordinator: &mut FrameworkShutdownCoordinator,
        runtime: QueueRuntimeHandle,
        timeout: Duration,
    ) -> QueueLifecycleEvidence {
        QueueLifecycleEvidence(crate::lifecycle::register_queue_lifecycle(
            coordinator,
            runtime.0,
            timeout,
        ))
    }
}
