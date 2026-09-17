#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Composition root for a Lily RabbitMQ consumer process.
//!
//! Define DI services through `lily_consumer::{Injectable, ServiceTrait}`.
//! The root also exposes the DI error, container and scope APIs. No additional
//! derive, registry or `linkme` dependency is needed for service registration.
//!
//! Define injectable handlers with `lily_queue::{queue_service, queue}`, add
//! one matching `[[rabbitmq.topology.queues]]` entry per active physical queue,
//! and start the process with [`Consumer::run`] or [`Consumer::builder`]. A
//! configured queue must have at least one linked handler. Several handlers may
//! share it when each `(schema version, content kind)` key is unique; Lily uses
//! one RabbitMQ consumer pipeline and an immutable process-local dispatch
//! table. Exact duplicates fail startup before broker admission.
//!
//! Every accepted delivery must carry canonical event ID, positive schema
//! version and content-kind headers. There is no headerless V1/JSON fallback.
//! RabbitMQ does not select competing consumer processes by these headers, so
//! every replica on one physical queue must support every contract publishers
//! may emit. Incompatible version-specific binaries require separate queues or
//! routing bindings.
//!
//! Transactional inbox/outbox processing is explicitly opt-in. Select exactly
//! one single/factory mode for each compiled backend: PostgreSQL through
//! `transactional-inbox-postgresql*`, or MongoDB through
//! `transactional-inbox-mongodb*`. Mark the handler with
//! `delivery_guarantee = "transactional_inbox"` and bind its physical queue to
//! one exact `transactional_inbox.backend`. Lily resolves the already
//! initialized database authority from this Consumer's application container
//! and verifies the explicit framework migration before RabbitMQ admission. It
//! never opens a second client/pool, silently falls back to at-least-once, or
//! runs DDL during ordinary Consumer startup. MongoDB bindings additionally
//! require a transaction-capable replica set or sharded deployment; a
//! `TransientTransactionError` replays the complete handler pipeline in a
//! fresh delivery DI scope. A configured transactional binding with no matching
//! transactional handler is rejected as stale configuration. With every
//! transactional feature disabled, both storage adapters are absent from the
//! Consumer dependency graph and selected transactional metadata fails startup
//! before broker I/O.
//!
//! [`ConsumerBuilder`] exposes explicit ownership choices for DI, secrets,
//! tracing and shutdown. Tracing is disabled by default. A container supplied
//! with [`ConsumerBuilder::container`] remains caller-owned; a container built
//! by the consumer is closed by the consumer.
//!
//! Application-defined queue middleware and guards are opt-in. Register global
//! types with [`ConsumerBuilder::middleware`] and [`ConsumerBuilder::guard`];
//! use `#[middleware(Type)]` and `#[guard(Type)]` beneath `#[queue_service]`
//! for service/handler plans. Lily constructs each concrete type once during
//! startup, while scoped/transient dependencies must be resolved from the
//! per-delivery exchange rather than retained by that singleton component.
//! Constructor futures are polled in the composition task under one aggregate
//! timeout. Cancelling that task drops the pending constructor before the
//! completed middleware/guard prefix is released in reverse order; no
//! framework-owned constructor child task is detached.
//!
//! AsyncAPI generation is also opt-in through the `asyncapi` feature and
//! `ConsumerBuilder::asyncapi`. Queue-service and handler `#[asyncapi(...)]`
//! metadata is folded into the same canonical handler registration used by
//! runtime dispatch; Lily never scans a second documentation registry. The
//! immutable `ConsumerAsyncApiService` is attached to DI only after the
//! accepted execution plan, physical queue registration and final startup
//! cancellation fence all succeed. JSON, text and binary extractors infer
//! their payload contract. Raw, custom and body-unobserved handlers must state
//! an explicit `schema + content_type` or `opaque + content_type` contract, or
//! be excluded with `#[asyncapi(skip)]`. Lily creates no documentation route,
//! file, UI, topology mutation or publisher operation.
//!
//! Consumer startup has one private cleanup authority. A cancellation token is
//! observed during DI construction, configuration resolution, pipeline
//! compilation and queue registration. The pending activity becomes terminal
//! before queue admission/drain/close, framework-owned DI disposal and
//! framework-owned tracing shutdown run in that order. A caller-provided
//! container and external tracing runtime are never disposed by Lily, but the
//! Consumer still closes the queue runtime it acquired from that container.
//! Explicit startup errors await rollback and retain typed secondary evidence;
//! caller drop/abort transfers the same resources to one bounded task on the
//! Tokio runtime which polled startup. The runtime must remain alive for that
//! asynchronous Drop fallback to finish. A pending DI build retains its own
//! rollback receipt under the Consumer attempt's bounded root; completed
//! composition uses the same application-owned shutdown budgeting principles.
//!
//! A successful build commit transfers its resources synchronously, with no
//! intervening `.await`, into one private runtime owner. That owner is the sole
//! post-readiness cleanup authority and moves the queue runtime, shutdown
//! state, optional signal monitor, lifecycle budget, captured Tokio handle,
//! and only framework-owned DI and tracing owners into one supervised task
//! before runtime waiting starts. This is an internal ownership boundary, not
//! an application cleanup API or a delivery-path extension.
//!
//! Dropping or aborting an outer `run()` or `run_with_cancellation()` waiter
//! requests durable `Manual` shutdown without aborting the supervisor, which
//! retains every cleanup resource while it completes canonical bounded
//! shutdown. Its asynchronous `Drop` fallback is guaranteed only while the
//! captured Tokio runtime remains alive. Provider termination, signals, caller
//! cancellation, managed shutdown or readiness-waiter loss, and outer-waiter
//! drop all converge on the same owner and coordinator rather than creating a
//! second cleanup authority.
//! Caller-owned DI and external tracing are never moved into that cleanup
//! authority or shut down by Lily; the Consumer-owned queue runtime is still
//! closed even when it came from a caller-provided container.
//!
//! The managed readiness waiter uses an armed cancellation token and requests
//! cancellation immediately if dropped before returning a [`ManagedConsumer`].
//! Provider completion remains first in biased runtime selection, preserving a
//! simultaneous provider terminal result. Concurrent managed `wait` and
//! `shutdown` observers retain the supervisor and replay one immutable
//! terminal result even when an individual observer future is dropped.
//!
//! [`ManagedConsumer::snapshot`] is the canonical immutable operational
//! surface. It separates process liveness from delivery readiness and reports
//! lifecycle, broker/topology/admission, bounded delivery/settlement counters
//! and shutdown reconciliation. Broker recovery remains live while readiness
//! is false; readiness can return only after the required queues recover.
//! Snapshots implement `serde::Serialize` and contain no payload, event ID,
//! routing key, broker endpoint or credential. This crate intentionally creates
//! no HTTP health endpoint: the application decides whether and where to expose
//! the snapshot.
//! [`ManagedConsumer::shutdown_report`] additionally exposes the original
//! application shutdown actions and dependency termination evidence after the
//! actual runtime join, even on failure. A returned runtime result, successful
//! action, and confirmed resource termination are separate facts. Reports retain
//! bounded application evidence rather than per-delivery history.
//!
//! [`ConsumerError`] has no string catch-all. Operational failures retain typed
//! sources while their `Debug`/`Display` output remains secret-safe. If a
//! primary startup/runtime failure is followed by shutdown or cleanup failures,
//! [`ConsumerLifecycleFailures`] preserves the authoritative primary cause and
//! the ordered secondary evidence.
//!
//! ```no_run
//! use lily_consumer::{Consumer, ConsumerError, Injectable, ServiceTrait};
//! use lily_queue::{Json, QueueHandlerError, queue, queue_service};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct OrderCreated {
//!     order_id: String,
//! }
//!
//! #[derive(Default, Injectable)]
//! #[service(lifetime = "Singleton")]
//! struct OrderWorker;
//!
//! impl ServiceTrait for OrderWorker {}
//!
//! #[queue_service]
//! impl OrderWorker {
//!     #[queue("orders.created", version = 1, content = "json")]
//!     async fn created(
//!         &self,
//!         Json(message): Json<OrderCreated>,
//!     ) -> Result<(), QueueHandlerError> {
//!         println!("{}", message.order_id);
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), ConsumerError> {
//!     Consumer::run().await
//! }
//! ```

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

#[cfg(feature = "asyncapi")]
mod asyncapi;
mod consumer;
mod operational;
mod plan;
mod shutdown_report;

#[cfg(test)]
fn test_application_container_builder() -> lily_injection::ApplicationContainerBuilder {
    let builder = lily_injection::ApplicationContainer::builder();
    #[cfg(feature = "transactional-inbox-postgresql")]
    let builder =
        builder.seed_singleton(lily_postgresql::PgDatabaseService::test_container_fixture());
    #[cfg(feature = "transactional-inbox-postgresql-factory")]
    let builder = builder.seed_singleton(lily_postgresql::PgFactory::test_container_fixture());
    #[cfg(feature = "transactional-inbox-mongodb")]
    let builder = builder.seed_singleton(lily_mongodb::DatabaseService::test_container_fixture());
    #[cfg(feature = "transactional-inbox-mongodb-factory")]
    let builder = builder.seed_singleton(lily_mongodb::MongoFactory::test_container_fixture());
    builder
}

#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;

#[cfg(feature = "asyncapi")]
pub use asyncapi::{ConsumerAsyncApiService, ConsumerDocument};
pub use consumer::{Consumer, ConsumerBuilder, ManagedConsumer};
#[cfg(feature = "asyncapi")]
pub use lily_asyncapi::{
    schemars, AsyncApiApiKeyLocation, AsyncApiBuildError, AsyncApiConfig,
    AsyncApiHttpApiKeyLocation, AsyncApiOAuthFlow, AsyncApiOAuthFlows, AsyncApiSecurityScheme,
    AsyncApiServer, AsyncApiServerProtocol, AsyncApiServiceError, AsyncApiSnapshot, AsyncApiTag,
};
pub use lily_error::application::consumer::{
    ConsumerAsyncApiFailureStage, ConsumerConfigurationFailure, ConsumerError,
    ConsumerLifecycleFailures, ConsumerManagedTaskFailureKind, ConsumerPlanFailureKind,
    ConsumerShutdownFailureEvidence, ConsumerShutdownFailureKind, ConsumerSignalFailureStage,
    ConsumerTracingFailureStage,
};
pub use lily_injection::async_trait;
pub use lily_injection::{
    ApplicationContainer, ApplicationContainerBuilder, ApplicationScope, ApplicationScopeFactory,
    ContainerShutdownReport, Extensions, Injectable, InjectionError, ProcessContext,
    ServiceLifetime, ServiceScope, ServiceTrait, ShutdownOutcome, ShutdownOutcomeStatus,
    ShutdownRemainingWork, BUILD_ROLLBACK_TIMEOUT_ENV, DEFAULT_SHUTDOWN_TIMEOUT,
    MAX_BUILD_ROLLBACK_TIMEOUT_SECS,
};
pub use operational::{
    ConsumerAdmissionState, ConsumerBrokerState, ConsumerLifecycleState,
    ConsumerOperationalSnapshot, ConsumerShutdownSnapshot, ConsumerTopologyState,
};
pub use shutdown_report::{
    ConsumerDependencyReport, ConsumerResourceReport, ConsumerShutdownActionOutcome,
    ConsumerShutdownActionReport, ConsumerShutdownCompletion, ConsumerShutdownReport,
};
pub use tokio_util::sync::CancellationToken;

/// Expansion support for the re-exported `Injectable` derive.
#[doc(hidden)]
pub mod __private {
    pub use lily_injection;
}
