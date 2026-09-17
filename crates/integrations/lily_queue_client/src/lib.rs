//! RabbitMQ-only publishing for Lily applications.
//!
//! The default `single` feature registers one injectable
//! `QueueClientService`. Resolve or inject that service and call its `publish`,
//! `publish_raw`, or
//! version-aware typed JSON/text/binary/custom methods. Stable outbox identity
//! is supplied with [`PublishMetadata`]. The mutually exclusive
//! `factory` feature registers a `QueueClientFactory` for multiple named
//! RabbitMQ connections; “factory” selects composition style, not broker type.
//! With both composition features disabled, the crate exposes only the
//! low-level transport contracts used by other Lily adapters and does not
//! register an application-facing publisher service.
//!
//! A successful publish means RabbitMQ returned an ACK and did not return the
//! mandatory message as unroutable. NACK, Return, timeout, cancellation, and
//! transport failure are errors. Delivery remains at-least-once; transactional
//! outbox relays must reuse both event ID and schema version with
//! [`PublishMetadata`]. Built-in typed methods bind payload encoding,
//! `x-lily-content-kind`, and AMQP `content_type` so callers cannot create a
//! contradictory envelope.
//!
//! Publisher startup never creates broker topology. Publisher-only processes
//! may explicitly apply the canonical [`lily_config::RabbitMqTopologyConfig`]
//! with [`RabbitMqTopologyBootstrap`]; externally owned resources are only
//! checked passively and their bindings remain operator-unverified.
//!
//! Connection-pool startup retains active, partially opened, and retired
//! connection generations inside the manager across every async boundary.
//! Cancelling startup or close therefore cannot discard the handles required
//! for a later proven asynchronous close. Only observed terminal `Closed` or
//! `Error` state releases a handle; `Closing`, `Reconnecting`, and every other
//! non-terminal state remain manager-owned. This is an internal lifecycle
//! guarantee shared by publisher and consumer adapters, not a public pool API.
//!
//! # Default single-client usage
//!
//! ```no_run
//! # #[cfg(feature = "single")]
//! # mod single_client_example {
//! use std::sync::Arc;
//! use lily_queue_client::QueueClientService;
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! struct UserCreated {
//!     user_id: String,
//! }
//!
//! # async fn publish(
//! #     publisher: Arc<QueueClientService>,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! let event = UserCreated { user_id: "42".into() };
//! publisher.publish("events", "user.created", &event).await?;
//! # Ok(())
//! # }
//! # }
//! ```
//!
//! Configuration and complete single/factory examples are in the crate
//! README. Applications should not construct the hidden RabbitMQ transport
//! engine types directly.
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

#[cfg(all(feature = "single", feature = "factory"))]
compile_error!(
    "lily_queue_client features `single` and `factory` are mutually exclusive; disable default features before enabling `factory`"
);

mod channel_manager_trait;
mod client_trait;
mod connection_manager_trait;
mod message_envelope;
mod providers;
mod publish_outcome;
mod publisher_engine_trait;
mod rabbitmq_options;
mod telemetry;
mod topology_bootstrap;

#[cfg(any(
    all(feature = "single", not(feature = "factory")),
    all(feature = "factory", not(feature = "single"))
))]
mod queue_client_service;

// Factory module - only available with factory feature
#[cfg(all(feature = "factory", not(feature = "single")))]
mod queue_client_factory;

// Cross-crate implementation ABI shared with `lily_queue` and qualification
// fixtures. These are intentionally absent from the end-user rustdoc surface.
#[doc(hidden)]
pub use client_trait::QueueClient;
#[doc(hidden)]
pub use connection_manager_trait::ConnectionManager;
pub use message_envelope::{
    CustomPublishContent, PublishContractError, PublishMetadata, PublishSchemaVersion,
    MAX_PUBLISH_CONTENT_KIND_BYTES, MAX_PUBLISH_CONTENT_TYPE_BYTES,
};
#[doc(hidden)]
pub use message_envelope::{PublishContentKind, PublishEnvelope};
#[doc(hidden)]
pub use providers::rabbitmq::{RabbitMQClient, RabbitMQConnectionManager};
#[doc(hidden)]
pub use publish_outcome::{await_publisher_confirm, PublishOutcome};
#[doc(hidden)]
pub use rabbitmq_options::RabbitMqOptions;

pub(crate) use channel_manager_trait::ChannelManager;
pub(crate) use publisher_engine_trait::PublisherEngine;
pub(crate) use telemetry::PublishTerminalLedger;
pub use telemetry::PublishTerminalSnapshot;
pub use topology_bootstrap::{RabbitMqTopologyBootstrap, RabbitMqTopologyBootstrapReport};

#[doc(hidden)]
pub use topology_bootstrap::execute_rabbitmq_topology_plan;

#[cfg(any(
    all(feature = "single", not(feature = "factory")),
    all(feature = "factory", not(feature = "single"))
))]
pub use queue_client_service::QueueClientService;

#[cfg(all(feature = "factory", not(feature = "single")))]
pub use queue_client_factory::QueueClientFactory;
