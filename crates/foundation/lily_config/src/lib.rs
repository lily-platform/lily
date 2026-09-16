#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Strict, immutable configuration for Lily applications.
//!
//! For service registration and scope APIs, add `lily_injection` directly or
//! use the root re-exports of `lily_http_api`, `lily_websocket` or `lily_consumer`.
//!
//! [`ConfigService`] is the canonical entry point. It reads one standard TOML
//! document, applies environment overrides, resolves protected references and
//! atomically publishes one immutable [`ConfigSnapshot`]. Production startup
//! fails when the file is absent or any configured value is invalid.
//!
//! # Startup contract
//!
//! Configuration is assembled in this order:
//!
//! 1. parse the configured TOML file with the standard `toml` crate;
//! 2. apply canonical `LILY__SECTION__FIELD` environment overrides;
//! 3. resolve exact `${secret:key}` and `${file:/absolute/path}` values;
//! 4. deserialize and validate the typed [`LilyConfig`] schema;
//! 5. publish version 1 for the lifetime of the process.
//!
//! Runtime reload is deliberately unsupported. Replace the application
//! instance to publish a new file, environment value or secret generation.
//!
//! # Standalone use
//!
//! ```no_run
//! use lily_config::{ConfigOptions, ConfigService};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ConfigService::new(
//!     ConfigOptions::production("/etc/lily/lily.toml")
//!         .require_key("server.host")
//!         .require_key("server.port"),
//! );
//! config.load().await?;
//!
//! let host: String = config.get("server.host").await?;
//! let port: u16 = config.get("server.port").await?;
//! let debug: bool = config.get_or_default("custom.debug", false).await?;
//! # let _ = (host, port, debug);
//! # Ok(())
//! # }
//! ```
//!
//! Lily application builders normally own this lifecycle through dependency
//! injection. Application services inject `Arc<ConfigService>`; callers do
//! not construct a second service or reload it per request.
//!
//! ```no_run
//! use std::sync::Arc;
//! use lily_config::ConfigService;
//! use lily_injection::{Injectable, ServiceTrait};
//!
//! #[derive(Injectable)]
//! #[service(lifetime = "Singleton")]
//! struct FeatureService {
//!     #[inject]
//!     config: Arc<ConfigService>,
//! }
//!
//! impl ServiceTrait for FeatureService {}
//! # fn main() {}
//! ```
//!
//! # TOML schema
//!
//! Unknown typed fields are rejected. `[database]` belongs to Lily's MongoDB
//! adapter; PostgreSQL uses its separate `[postgresql]` section.
//! The queue-level `[rabbitmq.topology.queues.transactional_inbox]` section is
//! accepted only when an opt-in `transactional-inbox-postgresql` or
//! `transactional-inbox-mongodb` Cargo feature is compiled. Without either
//! feature it remains an unknown field and startup fails rather than silently
//! dropping the guarantee. The MongoDB backend reuses `[database]`; its
//! optional nested `mongodb` table contains retry bounds, never credentials.
//! Every transactional queue binding must state `backend = "postgresql"` or
//! `backend = "mongodb"` explicitly. Lily does not infer storage authority
//! from whichever Cargo features happen to be compiled into the binary.
//!
//! ```toml
//! [server]
//! host = "127.0.0.1"
//! port = 8080
//!
//! [postgresql]
//! mode = "single"
//! connection_string = "${secret:postgresql.primary_url}"
//!
//! [custom]
//! debug = false
//! ```
//!
//! A single underscore remains part of a field name, while a double
//! underscore separates nesting:
//!
//! ```text
//! LILY__SERVER__PORT=9090
//! LILY__POSTGRESQL__POOL__MAX_SIZE=32
//! ```
//!
//! `LILY_CONFIG_PATH` and `LILY_CONFIG_MODE` are the bootstrap-only pair used
//! by [`ConfigService::default`] and DI registration. They must be supplied
//! together. `LILY_CONFIG_MODE` accepts `development`, `test` or `production`.
//!
//! # Secrets
//!
//! Lily intentionally provides no vendor-specific secret client. Applications
//! implement [`SecretResolver`] and seed the resulting [`ConfigService`] at
//! the composition root before the container initializes services:
//!
//! ```no_run
//! use async_trait::async_trait;
//! use lily_config::{ConfigError, ConfigOptions, ConfigService, SecretResolver};
//! use lily_injection::ApplicationContainer;
//!
//! struct ApplicationSecrets;
//!
//! #[async_trait]
//! impl SecretResolver for ApplicationSecrets {
//!     async fn resolve(&self, key: &str) -> Result<String, ConfigError> {
//!         # let _ = key;
//!         # unimplemented!("read from the application's secret provider")
//!     }
//!
//!     fn provider_name(&self) -> &'static str {
//!         "application-secrets"
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ConfigService::with_options_and_secret_resolver(
//!     ConfigOptions::production("/etc/lily/lily.toml"),
//!     ApplicationSecrets,
//! );
//! let container = ApplicationContainer::builder()
//!     .seed_singleton(config)
//!     .build()
//!     .await?;
//! # container.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Never log [`LilyConfig`], [`ConfigSnapshot::values`] or values returned by
//! [`ConfigService::get`]. Use [`ConfigService::redacted_effective_config`] for
//! operational diagnostics.

mod config;
mod config_service;
mod secret;
mod topology_plan;
mod value;

#[cfg(feature = "transactional-inbox-mongodb")]
pub use config::MongoTransactionalInboxConfig;
pub use config::{
    CacheCellConfig, CacheConfig, ClickhouseCellConfig, ClickhouseConfig, DatabaseCellConfig,
    DatabaseConfig, HttpClientConfig, HttpClientFactoryConfig, HttpClientProtocol, LifecycleConfig,
    LilyConfig, PgCellConfig, PgConfig, PgMode, PgPoolConfig, PgTlsConfig, PgTlsMode,
    QueueClientCellConfig, QueueClientConfig, QueueDefinition, QueueRetentionConfig,
    RabbitMqConfig, RabbitMqConsumerConfig, RabbitMqExchangeKind, RabbitMqQueueType,
    RabbitMqTlsConfig, RabbitMqTopologyConfig, RabbitMqTopologyOwnership,
    RedisWebSocketBackplaneConfig, ServerConfig, WebSocketClientCellConfig, WebSocketClientConfig,
    WebSocketConfig,
};
#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
pub use config::{TransactionalInboxBackend, TransactionalInboxConfig};
pub use config_service::{
    ConfigMode, ConfigOptions, ConfigService, ConfigSnapshot, EffectiveConfigMetadata,
    RedactedEffectiveConfig,
};
pub use lily_error::config::ConfigError;
pub use secret::{ResolvedSecret, SecretBinding, SecretResolver};
pub use topology_plan::{
    MAX_RETRY_BUCKETS_PER_QUEUE, MAX_RETRY_BUCKETS_PER_TOPOLOGY, RabbitMqQueueTopologyPlan,
    RabbitMqRetryBucketPlan, RabbitMqTopologyPlan, RabbitMqTopologyPlanError,
};
pub use value::FromTomlValue;
