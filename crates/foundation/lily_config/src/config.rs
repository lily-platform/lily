// =============================================================================
// LilyConfig - Configuration Schema for lily.toml
// =============================================================================

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

const REDACTED_DEBUG_VALUE: &str = "<redacted>";

fn redacted_option<T>(value: &Option<T>) -> Option<&'static str> {
    value.as_ref().map(|_| REDACTED_DEBUG_VALUE)
}

struct RedactedMapValues<'a>(&'a HashMap<String, String>);

impl fmt::Debug for RedactedMapValues<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_map()
            .entries(self.0.keys().map(|key| (key, REDACTED_DEBUG_VALUE)))
            .finish()
    }
}

macro_rules! impl_redacted_debug {
    (
        $type:ident {
            visible: [$($visible:ident),* $(,)?],
            sensitive_options: [$($sensitive_option:ident),* $(,)?],
            sensitive_values: [$($sensitive_value:ident),* $(,)?] $(,)?
        }
    ) => {
        impl fmt::Debug for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($type));
                $(debug.field(stringify!($visible), &self.$visible);)*
                $(debug.field(
                    stringify!($sensitive_option),
                    &redacted_option(&self.$sensitive_option),
                );)*
                $(debug.field(stringify!($sensitive_value), &REDACTED_DEBUG_VALUE);)*
                debug.finish()
            }
        }
    };
}

/// Main configuration structure for lily.toml
/// This provides a clear schema that users can follow
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LilyConfig {
    /// Server configuration
    pub server: ServerConfig,
    /// Application lifecycle and graceful-shutdown configuration
    pub lifecycle: LifecycleConfig,
    /// Database configuration (optional)
    pub database: Option<DatabaseConfig>,

    /// Diesel-first PostgreSQL configuration (optional).
    /// This is intentionally separate from `database`, which remains the
    /// canonical MongoDB configuration surface.
    pub postgresql: Option<PgConfig>,
    /// ClickHouse configuration (optional)
    pub clickhouse: Option<ClickhouseConfig>,
    /// Cache configuration (optional)
    pub cache: Option<CacheConfig>,
    /// RabbitMQ consumer connection and shared topology configuration.
    pub rabbitmq: RabbitMqConfig,
    /// Queue client configuration (optional)
    pub queue_client: Option<QueueClientConfig>,
    /// WebSocket configuration (optional)
    pub websocket: Option<WebSocketConfig>,
    /// WebSocket client configuration (optional)
    pub websocket_client: Option<WebSocketClientConfig>,

    /// HTTP Client Factory configuration (optional)
    pub http_client_factory: Option<HttpClientFactoryConfig>,
    /// Custom user-defined sections
    pub custom: HashMap<String, String>,
}

impl fmt::Debug for LilyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LilyConfig")
            .field("server", &self.server)
            .field("lifecycle", &self.lifecycle)
            .field("database", &self.database)
            .field("postgresql", &self.postgresql)
            .field("clickhouse", &self.clickhouse)
            .field("cache", &self.cache)
            .field("rabbitmq", &self.rabbitmq)
            .field("queue_client", &self.queue_client)
            .field("websocket", &self.websocket)
            .field("websocket_client", &self.websocket_client)
            .field("http_client_factory", &self.http_client_factory)
            .field("custom", &RedactedMapValues(&self.custom))
            .finish()
    }
}

/// Application lifecycle and graceful-shutdown configuration.
///
/// This section is always available, even when `[lifecycle]` is omitted from
/// `lily.toml`, so composition roots can apply one deterministic shutdown
/// deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LifecycleConfig {
    /// Maximum time allowed for graceful shutdown, in seconds (default: 30).
    /// Must not exceed [`Self::MAX_SHUTDOWN_TIMEOUT_SECS`].
    pub shutdown_timeout_secs: u64,
}

impl LifecycleConfig {
    /// Inclusive upper bound for the application graceful-shutdown deadline.
    ///
    /// This keeps deadline arithmetic within the cross-platform range
    /// supported by `std::time::Instant`.
    pub const MAX_SHUTDOWN_TIMEOUT_SECS: u64 = 100 * 365 * 24 * 60 * 60;
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout_secs: 30,
        }
    }
}

/// Server configuration section
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Server host (default: "127.0.0.1")
    pub host: String,
    /// Server port (default: 8080)
    pub port: u16,
    /// Maximum concurrent connections (optional, default: 10000)
    pub max_connections: Option<usize>,
    /// Maximum bytes retained for one multipart part (optional, default: 8 MiB).
    pub max_multipart_part_bytes: Option<usize>,
    /// Maximum multipart parts retained per request (optional, default: 128).
    pub max_multipart_parts: Option<usize>,
    /// Maximum retained header metadata per multipart part (optional, default: 16 KiB).
    pub max_multipart_metadata_bytes: Option<usize>,
    /// Connection idle timeout in seconds (optional, default: 120)
    pub connection_idle_timeout_secs: Option<u64>,
    /// Enable TLS/HTTPS (optional, default: false)
    pub tls_enabled: Option<bool>,
    /// Path to TLS certificate file
    pub tls_cert_path: Option<PathBuf>,
    /// Path to TLS private key file
    pub tls_key_path: Option<PathBuf>,
    /// Request timeout in seconds (optional, default: 60)
    pub request_timeout_secs: Option<u64>,
}

impl_redacted_debug!(ServerConfig {
    visible: [
        host,
        port,
        max_connections,
        max_multipart_part_bytes,
        max_multipart_parts,
        max_multipart_metadata_bytes,
        connection_idle_timeout_secs,
        tls_enabled,
        tls_cert_path,
        request_timeout_secs,
    ],
    sensitive_options: [tls_key_path],
    sensitive_values: [],
});

/// Database configuration section
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Configuration mode: "single" or "factory" (default: "single")
    pub mode: Option<String>,
    /// Adapter discriminator. The `[database]` section accepts `"mongodb"` or
    /// `"none"`; PostgreSQL uses [`LilyConfig::postgresql`].
    pub database_type: Option<String>,
    /// Database connection string - used in single mode
    pub connection_string: Option<String>,
    /// Connection pool size (default: 10) - used in single mode
    pub pool_size: Option<usize>,
    /// Connection timeout in seconds (default: 30) - used in single mode
    pub connection_timeout_secs: Option<u64>,
    /// Query timeout in seconds (default: 30) - used in single mode
    pub query_timeout_secs: Option<u64>,
    /// Enable connection pooling (default: true) - used in single mode
    pub pooling_enabled: Option<bool>,
    /// Database name - used in single mode
    pub database_name: Option<String>,
    /// MongoDB host (default: "localhost") - used in single mode
    pub host: Option<String>,
    /// MongoDB port (default: 27017) - used in single mode
    pub port: Option<u16>,
    /// Username for authentication - used in single mode
    pub username: Option<String>,
    /// Password for authentication - used in single mode
    pub password: Option<String>,
    /// Authentication database - used in single mode
    pub auth_database: Option<String>,
    /// Use TLS/SSL connection - used in single mode
    pub use_tls: Option<bool>,
    /// Application name for connection - used in single mode
    pub app_name: Option<String>,
    /// Database cells for factory mode
    pub cells: Option<Vec<DatabaseCellConfig>>,
}

impl_redacted_debug!(DatabaseConfig {
    visible: [
        mode,
        database_type,
        pool_size,
        connection_timeout_secs,
        query_timeout_secs,
        pooling_enabled,
        database_name,
        host,
        port,
        username,
        auth_database,
        use_tls,
        app_name,
        cells,
    ],
    sensitive_options: [connection_string, password],
    sensitive_values: [],
});

/// Database cell configuration for factory mode
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseCellConfig {
    /// Cell name (identifier)
    pub name: String,
    /// Adapter discriminator. MongoDB factory cells use `"mongodb"`.
    pub database_type: String,
    /// Database connection string
    pub connection_string: Option<String>,
    /// Connection pool size (default: 10)
    pub pool_size: Option<usize>,
    /// Connection timeout in seconds (default: 30)
    pub connection_timeout_secs: Option<u64>,
    /// Query timeout in seconds (default: 30)
    pub query_timeout_secs: Option<u64>,
    /// Enable connection pooling (default: true)
    pub pooling_enabled: Option<bool>,
    /// Database name
    pub database_name: String,
    /// MongoDB host (default: "localhost")
    pub host: Option<String>,
    /// MongoDB port (default: 27017)
    pub port: Option<u16>,
    /// Username for authentication
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// Authentication database
    pub auth_database: Option<String>,
    /// Use TLS/SSL connection
    pub use_tls: Option<bool>,
    /// Application name for connection
    pub app_name: Option<String>,
}

impl_redacted_debug!(DatabaseCellConfig {
    visible: [
        name,
        database_type,
        pool_size,
        connection_timeout_secs,
        query_timeout_secs,
        pooling_enabled,
        database_name,
        host,
        port,
        username,
        auth_database,
        use_tls,
        app_name,
    ],
    sensitive_options: [connection_string, password],
    sensitive_values: [],
});

/// Diesel-first PostgreSQL configuration.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PgConfig {
    /// Selects one connection or a set of named connection cells.
    pub mode: PgMode,
    /// Single-mode connection string. Secret/file placeholders are resolved by
    /// `ConfigService` before this typed configuration is published.
    pub connection_string: Option<String>,
    /// Default connection-pool policy used in single mode.
    pub pool: PgPoolConfig,
    /// Default transport-security policy used in single mode.
    pub tls: PgTlsConfig,
    /// Named database plans used only in factory mode.
    pub cells: Vec<PgCellConfig>,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            mode: PgMode::Single,
            connection_string: None,
            pool: PgPoolConfig::default(),
            tls: PgTlsConfig::default(),
            cells: Vec::new(),
        }
    }
}

impl std::fmt::Debug for PgConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgConfig")
            .field("mode", &self.mode)
            .field(
                "connection_string",
                &self.connection_string.as_ref().map(|_| "<redacted>"),
            )
            .field("pool", &self.pool)
            .field("tls", &self.tls)
            .field("cells", &self.cells)
            .finish()
    }
}

/// PostgreSQL connection topology.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PgMode {
    /// Build one default PostgreSQL service.
    #[default]
    Single,
    /// Build the explicitly named entries in [`PgConfig::cells`].
    Factory,
}

/// One immutable named PostgreSQL database plan in factory mode.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PgCellConfig {
    /// Unique name used to resolve this database cell.
    pub name: String,
    /// PostgreSQL connection string for this cell.
    pub connection_string: String,
    /// Pool policy for this cell.
    pub pool: PgPoolConfig,
    /// TLS policy for this cell.
    pub tls: PgTlsConfig,
}

impl std::fmt::Debug for PgCellConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgCellConfig")
            .field("name", &self.name)
            .field("connection_string", &"<redacted>")
            .field("pool", &self.pool)
            .field("tls", &self.tls)
            .finish()
    }
}

/// Bounded PostgreSQL connection-pool policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PgPoolConfig {
    /// Maximum number of connections owned by the pool.
    pub max_size: usize,
    /// Maximum time allowed to establish one new connection, in seconds.
    pub connect_timeout_secs: u64,
    /// Maximum time allowed to acquire a pooled connection, in seconds.
    pub acquire_timeout_secs: u64,
    /// Maximum time allowed to validate a recycled connection, in seconds.
    pub recycle_timeout_secs: u64,
    /// Maximum time for transaction cleanup after execution cancellation, in seconds.
    /// This covers the query cancellation attempt and Diesel rollback together;
    /// it does not limit ordinary queries or an already-started commit.
    pub transaction_cleanup_timeout_secs: u64,
    /// Maximum time allowed to drain the pool during shutdown, in seconds.
    pub shutdown_timeout_secs: u64,
}

impl Default for PgPoolConfig {
    fn default() -> Self {
        Self {
            max_size: 16,
            connect_timeout_secs: 10,
            acquire_timeout_secs: 5,
            recycle_timeout_secs: 5,
            transaction_cleanup_timeout_secs: 5,
            shutdown_timeout_secs: 10,
        }
    }
}

/// PostgreSQL transport-security policy.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PgTlsConfig {
    /// Certificate and hostname verification mode.
    pub mode: PgTlsMode,
    /// Optional absolute PEM bundle path extending platform trust roots.
    pub additional_ca_bundle: Option<PathBuf>,
}

impl Default for PgTlsConfig {
    fn default() -> Self {
        Self {
            mode: PgTlsMode::VerifyFull,
            additional_ca_bundle: None,
        }
    }
}

impl std::fmt::Debug for PgTlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgTlsConfig")
            .field("mode", &self.mode)
            .field(
                "additional_ca_bundle",
                &self.additional_ca_bundle.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// PostgreSQL TLS verification mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PgTlsMode {
    /// Certificate chain and PostgreSQL hostname are both verified.
    #[default]
    VerifyFull,
    /// Explicit plaintext transport for development or trusted private
    /// networks. Production-mode ConfigService rejects this value.
    Disable,
}

/// Cache configuration section
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Configuration mode: "single" or "factory" (default: "single")
    pub mode: Option<String>,
    /// Cache provider ("redis" or "none") - used in single mode
    pub provider: Option<String>,
    /// Redis connection string (when provider is "redis") - used in single mode
    pub redis_url: Option<String>,
    /// Require a TLS Redis URL (`rediss://`). Plain private-network Redis must
    /// be selected explicitly with `false`.
    pub use_tls: Option<bool>,
    /// Optional absolute PEM bundle path for a private Redis trust anchor.
    pub additional_ca_bundle: Option<PathBuf>,
    /// Prefix applied to every application key.
    pub key_namespace: Option<String>,
    /// Default TTL in seconds (default: 3600) - used in single mode
    pub default_ttl_secs: Option<u64>,
    /// Maximum Redis connections owned by the application.
    pub pool_size: Option<usize>,
    /// Pool connection/create timeout, distinct from entry TTL.
    pub connection_timeout_secs: Option<u64>,
    /// Per-operation deadline, distinct from entry TTL.
    pub operation_timeout_secs: Option<u64>,
    /// Maximum SCAN count hint accepted for one page.
    pub scan_page_size: Option<usize>,
    /// Hard maximum number of keys accepted from one SCAN response.
    pub max_scan_results: Option<usize>,
    /// Cache cells for factory mode
    pub cells: Option<Vec<CacheCellConfig>>,
}

impl_redacted_debug!(CacheConfig {
    visible: [
        mode,
        provider,
        use_tls,
        key_namespace,
        default_ttl_secs,
        pool_size,
        connection_timeout_secs,
        operation_timeout_secs,
        scan_page_size,
        max_scan_results,
        cells,
    ],
    sensitive_options: [redis_url, additional_ca_bundle],
    sensitive_values: [],
});

/// Cache cell configuration for factory mode
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheCellConfig {
    /// Cell name (identifier)
    pub name: String,
    /// Cache provider. Lily V1 accepts only "redis".
    pub provider: String,
    /// Redis connection string (when provider is "redis")
    pub redis_url: Option<String>,
    /// Requires a `rediss://` URL when true.
    pub use_tls: Option<bool>,
    /// Optional absolute PEM bundle path for a private Redis trust anchor.
    pub additional_ca_bundle: Option<PathBuf>,
    /// Prefix applied to every application key in this cell.
    pub key_namespace: Option<String>,
    /// Default TTL in seconds (default: 3600)
    pub default_ttl_secs: Option<u64>,
    /// Maximum Redis connections owned by this cell.
    pub pool_size: Option<usize>,
    /// Pool connection/create timeout, in seconds.
    pub connection_timeout_secs: Option<u64>,
    /// Deadline for one Redis operation, in seconds.
    pub operation_timeout_secs: Option<u64>,
    /// Maximum SCAN count hint accepted for one page.
    pub scan_page_size: Option<usize>,
    /// Hard maximum keys accepted from one SCAN response.
    pub max_scan_results: Option<usize>,
}

impl_redacted_debug!(CacheCellConfig {
    visible: [
        name,
        provider,
        use_tls,
        key_namespace,
        default_ttl_secs,
        pool_size,
        connection_timeout_secs,
        operation_timeout_secs,
        scan_page_size,
        max_scan_results,
    ],
    sensitive_options: [redis_url, additional_ca_bundle],
    sensitive_values: [],
});

/// ClickHouse configuration section
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickhouseConfig {
    /// Configuration mode: "single" or "factory" (default: "single")
    pub mode: Option<String>,
    /// ClickHouse host (default: "localhost") - used in single mode
    pub host: Option<String>,
    /// ClickHouse port (default: 8123) - used in single mode
    pub port: Option<u16>,
    /// Database name (default: "default") - used in single mode
    pub database: Option<String>,
    /// Username for authentication (default: "default") - used in single mode
    pub username: Option<String>,
    /// Password for authentication - used in single mode
    pub password: Option<String>,
    /// Connection pool size (default: 10) - used in single mode
    pub pool_size: Option<usize>,
    /// Connection timeout in seconds (default: 30) - used in single mode
    pub connection_timeout_secs: Option<u64>,
    /// Query/operation timeout in seconds (default: 30)
    pub query_timeout_secs: Option<u64>,
    /// Use certificate-verifying HTTPS transport
    pub use_tls: Option<bool>,
    /// Enable LZ4 request/response compression
    pub compression_enabled: Option<bool>,
    /// ClickHouse cells for factory mode
    pub cells: Option<Vec<ClickhouseCellConfig>>,
}

impl_redacted_debug!(ClickhouseConfig {
    visible: [
        mode,
        host,
        port,
        database,
        username,
        pool_size,
        connection_timeout_secs,
        query_timeout_secs,
        use_tls,
        compression_enabled,
        cells,
    ],
    sensitive_options: [password],
    sensitive_values: [],
});

/// ClickHouse cell configuration for factory mode
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseCellConfig {
    /// Cell name (identifier)
    pub name: String,
    /// ClickHouse host (default: "localhost")
    pub host: String,
    /// ClickHouse port (default: 8123)
    pub port: Option<u16>,
    /// Database name (default: "default")
    pub database: String,
    /// Username for authentication (default: "default")
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// Connection pool size (default: 10)
    pub pool_size: Option<usize>,
    /// Connection timeout in seconds (default: 30)
    pub connection_timeout_secs: Option<u64>,
    /// Query/operation timeout in seconds (default: 30)
    pub query_timeout_secs: Option<u64>,
    /// Use certificate-verifying HTTPS transport
    pub use_tls: Option<bool>,
    /// Enable LZ4 request/response compression
    pub compression_enabled: Option<bool>,
}

impl_redacted_debug!(ClickhouseCellConfig {
    visible: [
        name,
        host,
        port,
        database,
        username,
        pool_size,
        connection_timeout_secs,
        query_timeout_secs,
        use_tls,
        compression_enabled,
    ],
    sensitive_options: [password],
    sensitive_values: [],
});

/// Canonical RabbitMQ consumer profile and shared topology authority.
///
/// `consumer` owns only the consumer connection/lifecycle profile. `topology`
/// is credential-free and is shared by consumers, explicit publisher-only
/// bootstrap and metadata projections such as AsyncAPI. Ordinary publisher
/// connection pools remain under [`LilyConfig::queue_client`].
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RabbitMqConfig {
    /// RabbitMQ consumer connection profile. `None` keeps consumer support off.
    pub consumer: Option<RabbitMqConsumerConfig>,
    /// Canonical exchange, queue and binding topology.
    pub topology: RabbitMqTopologyConfig,
}

/// RabbitMQ consumer connection configuration.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RabbitMqConsumerConfig {
    /// Broker connection string
    pub connection_string: Option<String>,
    /// Connection pool size (default: 5)
    pub pool_size: usize,
    /// Maximum time allowed to establish a RabbitMQ connection, in seconds.
    pub connection_timeout_secs: Option<u64>,
    /// Maximum wait for a publish confirmation, in seconds.
    pub confirm_timeout_secs: Option<u64>,
    /// Requested AMQP heartbeat interval, in seconds.
    pub heartbeat_secs: Option<u16>,
    /// Maximum reconnect attempts after a connection failure.
    pub max_reconnect_attempts: Option<u32>,
    /// Initial delay between reconnect attempts, in milliseconds.
    pub reconnect_backoff_millis: Option<u64>,
    /// Requires TLS transport when true.
    pub use_tls: Option<bool>,
    /// Optional private trust anchor and client identity for AMQPS/mTLS.
    pub tls: RabbitMqTlsConfig,
    /// Enable message persistence (default: true)
    pub persistence_enabled: bool,
    /// RabbitMQ username; required when connection_string is absent
    pub username: Option<String>,
    /// RabbitMQ password; required when connection_string is absent
    pub password: Option<String>,
    /// RabbitMQ hostname; required when connection_string is absent
    pub hostname: Option<String>,
    /// RabbitMQ port (default: 5672)
    pub port: Option<u16>,
    /// RabbitMQ virtual host (default: "/")
    pub vhost: Option<String>,
}

impl_redacted_debug!(RabbitMqConsumerConfig {
    visible: [
        pool_size,
        connection_timeout_secs,
        confirm_timeout_secs,
        heartbeat_secs,
        max_reconnect_attempts,
        reconnect_backoff_millis,
        use_tls,
        tls,
        persistence_enabled,
        username,
        hostname,
        port,
        vhost,
    ],
    sensitive_options: [connection_string, password],
    sensitive_values: [],
});

/// Canonical RabbitMQ topology shared by consumers, publishers and explicit
/// publisher-only topology bootstrap.
///
/// Topology identities are public transport metadata and must be configured as
/// direct values. Protected `${secret:...}` and `${file:...}` references are
/// rejected before resolution so this topology remains credential-free.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RabbitMqTopologyConfig {
    /// Physical queues and their exact exchange bindings.
    pub queues: Vec<QueueDefinition>,
}

/// RabbitMQ exchange kind supported by Lily's topology contract.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqExchangeKind {
    /// Direct exchange with an exact routing-key binding.
    #[default]
    Direct,
}

/// RabbitMQ physical queue implementation.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqQueueType {
    /// RabbitMQ classic queue.
    #[default]
    Classic,
    /// Replicated RabbitMQ quorum queue.
    Quorum,
}

/// Authority responsible for provisioning one RabbitMQ topology destination.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqTopologyOwnership {
    /// Lily declares the accepted immutable topology during explicit bootstrap.
    #[default]
    FrameworkManaged,
    /// An operator provisions topology; Lily performs only non-destructive checks.
    External,
}

/// Explicit RabbitMQ retention bounds for one logical queue.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRetentionConfig {
    /// Maximum ready messages retained by the main queue.
    pub main_max_messages: u64,
    /// Maximum message-body bytes retained by the main queue.
    pub main_max_bytes: u64,
    /// Maximum ready messages retained by each physical retry-delay bucket.
    pub retry_bucket_max_messages: u64,
    /// Maximum message-body bytes retained by each physical retry-delay bucket.
    pub retry_bucket_max_bytes: u64,
    /// Maximum ready messages retained by the dead-letter queue.
    pub dead_letter_max_messages: u64,
    /// Maximum message-body bytes retained by the dead-letter queue.
    pub dead_letter_max_bytes: u64,
}

/// Storage backend used by a transactional inbox/outbox queue binding.
#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransactionalInboxBackend {
    /// PostgreSQL transaction authority supplied by `lily_postgresql`.
    #[cfg(feature = "transactional-inbox-postgresql")]
    #[serde(rename = "postgresql")]
    PostgreSql,
    /// MongoDB transaction authority supplied by `lily_mongodb`.
    #[cfg(feature = "transactional-inbox-mongodb")]
    #[serde(rename = "mongodb")]
    MongoDb,
}

/// MongoDB transaction retry policy for one transactional queue binding.
///
/// MongoDB may return labels which require retrying the complete transaction
/// body or only reconciling the commit result. These bounds keep both paths
/// deterministic. Omitting the nested `mongodb` table uses these defaults.
#[cfg(feature = "transactional-inbox-mongodb")]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MongoTransactionalInboxConfig {
    /// Maximum complete transaction-body executions, including the first.
    pub max_transaction_attempts: u32,
    /// Initial complete-transaction retry backoff (default: 10 milliseconds).
    pub retry_initial_backoff_millis: u64,
    /// Maximum complete-transaction retry backoff (default: 1 second).
    pub retry_max_backoff_millis: u64,
    /// Aggregate deadline for commit-result reconciliation (default: 10 seconds).
    pub commit_retry_timeout_millis: u64,
}

#[cfg(feature = "transactional-inbox-mongodb")]
impl MongoTransactionalInboxConfig {
    /// Maximum complete transaction-body executions.
    pub const MAX_TRANSACTION_ATTEMPTS: u32 = 100;
    /// Maximum initial complete-transaction retry backoff (60 seconds).
    pub const MAX_RETRY_INITIAL_BACKOFF_MILLIS: u64 = 60_000;
    /// Maximum complete-transaction retry backoff (15 minutes).
    pub const MAX_RETRY_BACKOFF_MILLIS: u64 = 900_000;
    /// Maximum commit-result reconciliation deadline (60 seconds).
    pub const MAX_COMMIT_RETRY_TIMEOUT_MILLIS: u64 = 60_000;

    fn validate_contract(&self) -> Result<(), &'static str> {
        if !(1..=Self::MAX_TRANSACTION_ATTEMPTS).contains(&self.max_transaction_attempts) {
            return Err("mongodb.max_transaction_attempts");
        }
        if !(1..=Self::MAX_RETRY_INITIAL_BACKOFF_MILLIS)
            .contains(&self.retry_initial_backoff_millis)
        {
            return Err("mongodb.retry_initial_backoff_millis");
        }
        if !(1..=Self::MAX_RETRY_BACKOFF_MILLIS).contains(&self.retry_max_backoff_millis) {
            return Err("mongodb.retry_max_backoff_millis");
        }
        if !(1..=Self::MAX_COMMIT_RETRY_TIMEOUT_MILLIS).contains(&self.commit_retry_timeout_millis)
        {
            return Err("mongodb.commit_retry_timeout_millis");
        }
        if self.retry_max_backoff_millis < self.retry_initial_backoff_millis {
            return Err("mongodb.retry_max_backoff_before_initial_backoff");
        }

        Ok(())
    }
}

#[cfg(feature = "transactional-inbox-mongodb")]
impl Default for MongoTransactionalInboxConfig {
    fn default() -> Self {
        Self {
            max_transaction_attempts: 3,
            retry_initial_backoff_millis: 10,
            retry_max_backoff_millis: 1_000,
            commit_retry_timeout_millis: 10_000,
        }
    }
}

/// Bounded inbox/outbox policy for one logical queue.
///
/// This value only selects framework lifecycle policy and an optional exact
/// storage cell. It never contains credentials and does not provision or
/// migrate application storage implicitly.
///
/// Relational validation is fail-closed: the outbox claim lease must cover the
/// publish-confirm timeout, maximum retry backoff must not precede its initial
/// backoff, and cleanup cadence cannot exceed either retention horizon.
#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionalInboxConfig {
    /// Transactional storage backend compiled into this application.
    ///
    /// This field is deliberately required in serialized configuration. Lily
    /// never infers a storage authority from the set or order of compiled
    /// Cargo features.
    pub backend: TransactionalInboxBackend,
    /// Exact storage factory cell.
    ///
    /// This must be `None` in single mode and must be `Some(exact_name)` in
    /// factory mode; Lily never guesses a factory cell.
    #[serde(default)]
    pub database_cell: Option<String>,
    /// Inbox admission timeout and stale processing-row lease (default: 30 seconds).
    ///
    /// The transaction-scoped advisory admission lock itself lives until the
    /// owning transaction commits or rolls back; this is not a hard maximum
    /// duration for application transaction work.
    #[serde(default = "default_transactional_inbox_lock_timeout_millis")]
    pub inbox_lock_timeout_millis: u64,
    /// MongoDB-specific bounded transaction retry policy.
    ///
    /// This is forbidden for a PostgreSQL binding. For a MongoDB binding,
    /// omission is equivalent to [`MongoTransactionalInboxConfig::default`].
    #[cfg(feature = "transactional-inbox-mongodb")]
    #[serde(default)]
    pub mongodb: Option<MongoTransactionalInboxConfig>,
    /// Lease held by one outbox relay claim (default: 30 seconds).
    #[serde(default = "default_transactional_outbox_claim_lease_millis")]
    pub outbox_claim_lease_millis: u64,
    /// Maximum rows claimed by one relay iteration (default: 100).
    #[serde(default = "default_transactional_relay_batch_size")]
    pub relay_batch_size: usize,
    /// Maximum payload bytes held by one relay batch and accepted per enqueue
    /// under this policy (default: 8 MiB).
    ///
    /// Every individual outbox message also has a framework absolute ceiling
    /// of 16 MiB, even when this configured bound is larger.
    #[serde(default = "default_transactional_relay_max_in_flight_bytes")]
    pub relay_max_in_flight_bytes: usize,
    /// Delay between idle relay polls (default: 250 milliseconds).
    #[serde(default = "default_transactional_relay_poll_interval_millis")]
    pub relay_poll_interval_millis: u64,
    /// Maximum wait for one broker publish confirmation (default: 10 seconds).
    #[serde(default = "default_transactional_relay_publish_timeout_millis")]
    pub relay_publish_timeout_millis: u64,
    /// Maximum persisted publish attempts per durable row, including the first (default: 5).
    #[serde(default = "default_transactional_relay_max_publish_attempts")]
    pub relay_max_publish_attempts: u32,
    /// Initial relay failure backoff (default: 250 milliseconds).
    #[serde(default = "default_transactional_relay_retry_initial_backoff_millis")]
    pub relay_retry_initial_backoff_millis: u64,
    /// Maximum relay failure backoff (default: 30 seconds).
    #[serde(default = "default_transactional_relay_retry_max_backoff_millis")]
    pub relay_retry_max_backoff_millis: u64,
    /// Retention for completed inbox records (default: 7 days).
    #[serde(default = "default_transactional_inbox_retention_secs")]
    pub inbox_retention_secs: u64,
    /// Retention for published outbox records (default: 7 days).
    #[serde(default = "default_transactional_outbox_retention_secs")]
    pub outbox_retention_secs: u64,
    /// Interval between bounded cleanup passes (default: 1 hour).
    #[serde(default = "default_transactional_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,
    /// Per-runtime budget for relay and transaction reconciliation
    /// during shutdown (default: 30 seconds).
    ///
    /// The process-level shutdown coordinator remains the aggregate deadline
    /// authority; this field does not extend that outer deadline.
    #[serde(default = "default_transactional_shutdown_drain_timeout_millis")]
    pub shutdown_drain_timeout_millis: u64,
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_inbox_lock_timeout_millis() -> u64 {
    30_000
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_outbox_claim_lease_millis() -> u64 {
    30_000
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_relay_batch_size() -> usize {
    100
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_relay_max_in_flight_bytes() -> usize {
    8 * 1024 * 1024
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_relay_poll_interval_millis() -> u64 {
    250
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_relay_publish_timeout_millis() -> u64 {
    10_000
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_relay_max_publish_attempts() -> u32 {
    5
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_relay_retry_initial_backoff_millis() -> u64 {
    250
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_relay_retry_max_backoff_millis() -> u64 {
    30_000
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_inbox_retention_secs() -> u64 {
    7 * 24 * 60 * 60
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_outbox_retention_secs() -> u64 {
    7 * 24 * 60 * 60
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_cleanup_interval_secs() -> u64 {
    60 * 60
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
const fn default_transactional_shutdown_drain_timeout_millis() -> u64 {
    30_000
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
impl TransactionalInboxConfig {
    /// Maximum UTF-8 byte length of an explicit storage cell name.
    pub const MAX_DATABASE_CELL_BYTES: usize = 128;
    /// Minimum MongoDB processing lease, allowing bounded heartbeat renewal.
    #[cfg(feature = "transactional-inbox-mongodb")]
    pub const MIN_MONGODB_INBOX_LOCK_TIMEOUT_MILLIS: u64 = 1_000;
    /// Maximum inbox lock timeout (60 seconds).
    pub const MAX_INBOX_LOCK_TIMEOUT_MILLIS: u64 = 60_000;
    /// Maximum outbox claim lease (15 minutes).
    pub const MAX_OUTBOX_CLAIM_LEASE_MILLIS: u64 = 900_000;
    /// Maximum rows in one relay batch.
    pub const MAX_RELAY_BATCH_SIZE: usize = 1_000;
    /// Maximum aggregate relay payload (256 MiB).
    pub const MAX_RELAY_IN_FLIGHT_BYTES: usize = 256 * 1024 * 1024;
    /// Maximum idle poll interval (60 seconds).
    pub const MAX_RELAY_POLL_INTERVAL_MILLIS: u64 = 60_000;
    /// Maximum publish-confirmation timeout (60 seconds).
    pub const MAX_RELAY_PUBLISH_TIMEOUT_MILLIS: u64 = 60_000;
    /// Maximum persisted relay attempts per durable row.
    pub const MAX_RELAY_PUBLISH_ATTEMPTS: u32 = 100;
    /// Maximum initial retry backoff (60 seconds).
    pub const MAX_RELAY_INITIAL_BACKOFF_MILLIS: u64 = 60_000;
    /// Maximum retry backoff (15 minutes).
    pub const MAX_RELAY_BACKOFF_MILLIS: u64 = 900_000;
    /// Maximum inbox/outbox retention (365 days).
    pub const MAX_RETENTION_SECS: u64 = 365 * 24 * 60 * 60;
    /// Maximum cleanup interval (24 hours).
    pub const MAX_CLEANUP_INTERVAL_SECS: u64 = 24 * 60 * 60;
    /// Maximum relay shutdown drain (5 minutes).
    pub const MAX_SHUTDOWN_DRAIN_TIMEOUT_MILLIS: u64 = 300_000;

    /// Validates the canonical cross-crate runtime contract.
    ///
    /// Configuration loading and feature-gated queue runtimes call this same
    /// authority so programmatically constructed values cannot bypass TOML
    /// bounds or relational rules. The returned token is stable, contains no
    /// user data and identifies the violated field or relationship.
    #[doc(hidden)]
    pub fn validate_contract(&self) -> Result<(), &'static str> {
        if let Some(cell) = self.database_cell.as_deref()
            && (cell.is_empty()
                || cell.len() > Self::MAX_DATABASE_CELL_BYTES
                || cell.trim() != cell
                || cell.chars().any(char::is_control))
        {
            return Err("database_cell");
        }

        macro_rules! bounded {
            ($field:ident, $minimum:expr, $maximum:expr) => {
                if !($minimum..=$maximum).contains(&self.$field) {
                    return Err(stringify!($field));
                }
            };
        }

        bounded!(
            inbox_lock_timeout_millis,
            1,
            Self::MAX_INBOX_LOCK_TIMEOUT_MILLIS
        );
        bounded!(
            outbox_claim_lease_millis,
            1,
            Self::MAX_OUTBOX_CLAIM_LEASE_MILLIS
        );
        bounded!(relay_batch_size, 1, Self::MAX_RELAY_BATCH_SIZE);
        bounded!(
            relay_max_in_flight_bytes,
            1,
            Self::MAX_RELAY_IN_FLIGHT_BYTES
        );
        bounded!(
            relay_poll_interval_millis,
            1,
            Self::MAX_RELAY_POLL_INTERVAL_MILLIS
        );
        bounded!(
            relay_publish_timeout_millis,
            1,
            Self::MAX_RELAY_PUBLISH_TIMEOUT_MILLIS
        );
        bounded!(
            relay_max_publish_attempts,
            1,
            Self::MAX_RELAY_PUBLISH_ATTEMPTS
        );
        bounded!(
            relay_retry_initial_backoff_millis,
            1,
            Self::MAX_RELAY_INITIAL_BACKOFF_MILLIS
        );
        bounded!(
            relay_retry_max_backoff_millis,
            1,
            Self::MAX_RELAY_BACKOFF_MILLIS
        );
        bounded!(inbox_retention_secs, 1, Self::MAX_RETENTION_SECS);
        bounded!(outbox_retention_secs, 1, Self::MAX_RETENTION_SECS);
        bounded!(cleanup_interval_secs, 1, Self::MAX_CLEANUP_INTERVAL_SECS);
        bounded!(
            shutdown_drain_timeout_millis,
            1,
            Self::MAX_SHUTDOWN_DRAIN_TIMEOUT_MILLIS
        );

        if self.outbox_claim_lease_millis < self.relay_publish_timeout_millis {
            return Err("outbox_claim_lease_before_publish_timeout");
        }
        if self.relay_retry_max_backoff_millis < self.relay_retry_initial_backoff_millis {
            return Err("retry_max_backoff_before_initial_backoff");
        }
        if self.cleanup_interval_secs > self.inbox_retention_secs.min(self.outbox_retention_secs) {
            return Err("cleanup_interval_exceeds_retention");
        }

        match self.backend {
            #[cfg(feature = "transactional-inbox-postgresql")]
            TransactionalInboxBackend::PostgreSql =>
            {
                #[cfg(feature = "transactional-inbox-mongodb")]
                if self.mongodb.is_some() {
                    return Err("mongodb_policy_for_postgresql_backend");
                }
            }
            #[cfg(feature = "transactional-inbox-mongodb")]
            TransactionalInboxBackend::MongoDb => {
                if self.inbox_lock_timeout_millis < Self::MIN_MONGODB_INBOX_LOCK_TIMEOUT_MILLIS {
                    return Err("mongodb.inbox_lock_timeout_millis");
                }
                self.mongodb
                    .clone()
                    .unwrap_or_default()
                    .validate_contract()?;
            }
        }

        Ok(())
    }
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
impl Default for TransactionalInboxConfig {
    fn default() -> Self {
        Self {
            backend: {
                #[cfg(feature = "transactional-inbox-postgresql")]
                {
                    TransactionalInboxBackend::PostgreSql
                }
                #[cfg(all(
                    not(feature = "transactional-inbox-postgresql"),
                    feature = "transactional-inbox-mongodb"
                ))]
                {
                    TransactionalInboxBackend::MongoDb
                }
            },
            database_cell: None,
            inbox_lock_timeout_millis: default_transactional_inbox_lock_timeout_millis(),
            #[cfg(feature = "transactional-inbox-mongodb")]
            mongodb: None,
            outbox_claim_lease_millis: default_transactional_outbox_claim_lease_millis(),
            relay_batch_size: default_transactional_relay_batch_size(),
            relay_max_in_flight_bytes: default_transactional_relay_max_in_flight_bytes(),
            relay_poll_interval_millis: default_transactional_relay_poll_interval_millis(),
            relay_publish_timeout_millis: default_transactional_relay_publish_timeout_millis(),
            relay_max_publish_attempts: default_transactional_relay_max_publish_attempts(),
            relay_retry_initial_backoff_millis:
                default_transactional_relay_retry_initial_backoff_millis(),
            relay_retry_max_backoff_millis: default_transactional_relay_retry_max_backoff_millis(),
            inbox_retention_secs: default_transactional_inbox_retention_secs(),
            outbox_retention_secs: default_transactional_outbox_retention_secs(),
            cleanup_interval_secs: default_transactional_cleanup_interval_secs(),
            shutdown_drain_timeout_millis: default_transactional_shutdown_drain_timeout_millis(),
        }
    }
}

/// Queue definition for consumer configuration
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueDefinition {
    /// Queue name (e.g., "user.created")
    pub name: String,
    /// RabbitMQ exchange bound to this queue.
    pub exchange_name: String,
    /// Exact routing key used by the queue binding.
    pub routing_key: String,
    /// Typed RabbitMQ exchange kind.
    pub exchange_kind: RabbitMqExchangeKind,
    /// Physical RabbitMQ queue implementation.
    pub queue_type: RabbitMqQueueType,
    /// Authority responsible for provisioning this topology destination.
    pub topology_ownership: RabbitMqTopologyOwnership,
    /// Number of concurrent consumers (default: 1)
    pub concurrency: u32,
    /// Prefetch count - max unacked messages (default: 10)
    pub prefetch_count: u16,
    /// Bounded in-process delivery buffer. When omitted, prefetch_count is used.
    pub delivery_buffer_capacity: Option<usize>,
    /// Retry attempts on failure (default: 0 - no retry)
    pub retry_attempts: u32,
    /// Initial retry delay in milliseconds (default: 1000)
    pub retry_backoff_millis: Option<u64>,
    /// Maximum exponential retry delay in milliseconds (default: 60000)
    pub max_retry_backoff_millis: Option<u64>,
    /// Bounded retry-delay jitter ratio (default: `0.0`, disabled).
    ///
    /// Values must be finite and within `0.0..=0.5`. A non-zero ratio does
    /// not create arbitrary per-message TTLs: Lily predeclares at most the
    /// lower, nominal and upper queue-level TTL bucket for each exponential
    /// retry delay, then chooses one deterministically from the event ID and
    /// retry attempt. The routing hash is FNV-1a 64 over the canonical event
    /// ID bytes followed by the retry attempt as four-byte big-endian data.
    pub retry_jitter_ratio: f64,
    /// Aggregate budget for one delivery execution, including extraction,
    /// handler execution and delivery-scope cleanup (default: 30000). The
    /// queue adapter reserves a bounded tail for mandatory scope cleanup and
    /// exposes the earlier application-work cutoff as `DeliveryDeadline`.
    pub delivery_execution_timeout_millis: Option<u64>,
    /// Maximum wait for one original-delivery ACK or NACK operation
    /// (default: 5000).
    pub settlement_timeout_millis: Option<u64>,
    /// Queue durability - survive broker restart (default: true)
    pub durable: bool,
    /// Maximum accepted delivery body size before any clone or decode (default: 1 MiB)
    pub max_message_size_bytes: usize,
    /// Explicit bounds for every framework-owned physical queue.
    pub retention: Option<QueueRetentionConfig>,
    /// Dead letter exchange name (optional)
    pub dead_letter_exchange: Option<String>,
    /// Dead letter routing key (optional)
    pub dead_letter_routing_key: Option<String>,
    /// Message TTL in milliseconds (optional)
    pub message_ttl_ms: Option<u32>,
    /// RabbitMQ exclusive flag. Lily's pooled lifecycle requires `false` and
    /// rejects `true` during canonical topology compilation.
    pub exclusive: bool,
    /// Auto-delete queue when no consumers (default: false)
    pub auto_delete: bool,
    /// Elect only one active consumer while retaining standby consumers.
    pub single_active_consumer: bool,
    /// Maximum classic-queue message priority (`1..=16`).
    pub max_priority: Option<u8>,
    /// Optional transactional inbox/outbox storage binding.
    ///
    /// It is required when any selected handler for this physical queue uses
    /// `delivery_guarantee = "transactional_inbox"`, and forbidden when none
    /// does. Missing or stale bindings fail Consumer startup before listener
    /// admission.
    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-postgresql"
    ))]
    pub transactional_inbox: Option<TransactionalInboxConfig>,
}

/// Queue client configuration section
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueClientConfig {
    /// Configuration mode: "single" or "factory" (default: "single")
    pub mode: Option<String>,
    /// Provider connection string - used in single mode
    pub connection_string: Option<String>,
    /// Connection pool size (default: 5) - used in single mode
    pub pool_size: Option<usize>,
    /// Maximum RabbitMQ connection time, in seconds.
    pub connection_timeout_secs: Option<u64>,
    /// Maximum publish-confirmation wait, in seconds.
    pub confirm_timeout_secs: Option<u64>,
    /// Requested AMQP heartbeat interval, in seconds.
    pub heartbeat_secs: Option<u16>,
    /// Maximum reconnect attempts after a connection failure.
    pub max_reconnect_attempts: Option<u32>,
    /// Initial reconnect delay, in milliseconds.
    pub reconnect_backoff_millis: Option<u64>,
    /// Requires TLS transport when true.
    pub use_tls: Option<bool>,
    /// Optional private trust anchor and client identity for AMQPS/mTLS.
    pub tls: RabbitMqTlsConfig,
    /// Enable message persistence (default: true) - used in single mode
    pub persistence_enabled: Option<bool>,
    /// RabbitMQ username; required when connection_string is absent
    pub username: Option<String>,
    /// RabbitMQ password; required when connection_string is absent
    pub password: Option<String>,
    /// RabbitMQ hostname; required when connection_string is absent
    pub hostname: Option<String>,
    /// RabbitMQ port (default: 5672) - used in single mode
    pub port: Option<u16>,
    /// RabbitMQ virtual host (default: "/") - used in single mode
    pub vhost: Option<String>,
    /// Queue client cells for factory mode
    pub cells: Option<Vec<QueueClientCellConfig>>,
}

impl_redacted_debug!(QueueClientConfig {
    visible: [
        mode,
        pool_size,
        connection_timeout_secs,
        confirm_timeout_secs,
        heartbeat_secs,
        max_reconnect_attempts,
        reconnect_backoff_millis,
        use_tls,
        tls,
        persistence_enabled,
        username,
        hostname,
        port,
        vhost,
        cells,
    ],
    sensitive_options: [connection_string, password],
    sensitive_values: [],
});

/// Queue client cell configuration for factory mode
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueClientCellConfig {
    /// Cell name (identifier)
    pub name: String,
    /// Provider connection string
    pub connection_string: Option<String>,
    /// Connection pool size (default: 5)
    pub pool_size: Option<usize>,
    /// Maximum RabbitMQ connection time, in seconds.
    pub connection_timeout_secs: Option<u64>,
    /// Maximum publish-confirmation wait, in seconds.
    pub confirm_timeout_secs: Option<u64>,
    /// Requested AMQP heartbeat interval, in seconds.
    pub heartbeat_secs: Option<u16>,
    /// Maximum reconnect attempts after a connection failure.
    pub max_reconnect_attempts: Option<u32>,
    /// Initial reconnect delay, in milliseconds.
    pub reconnect_backoff_millis: Option<u64>,
    /// Requires TLS transport when true.
    pub use_tls: Option<bool>,
    /// Optional private trust anchor and client identity for AMQPS/mTLS.
    #[serde(default)]
    pub tls: RabbitMqTlsConfig,
    /// Enable message persistence (default: true)
    pub persistence_enabled: Option<bool>,
    /// RabbitMQ username; required when connection_string is absent
    pub username: Option<String>,
    /// RabbitMQ password; required when connection_string is absent
    pub password: Option<String>,
    /// RabbitMQ hostname; required when connection_string is absent
    pub hostname: Option<String>,
    /// RabbitMQ port (default: 5672)
    pub port: Option<u16>,
    /// RabbitMQ virtual host (default: "/")
    pub vhost: Option<String>,
}

impl_redacted_debug!(QueueClientCellConfig {
    visible: [
        name,
        pool_size,
        connection_timeout_secs,
        confirm_timeout_secs,
        heartbeat_secs,
        max_reconnect_attempts,
        reconnect_backoff_millis,
        use_tls,
        tls,
        persistence_enabled,
        username,
        hostname,
        port,
        vhost,
    ],
    sensitive_options: [connection_string, password],
    sensitive_values: [],
});

/// RabbitMQ client-side TLS material.
///
/// `additional_ca_bundle` extends the platform trust store. Supplying both
/// client identity paths enables mutual TLS; either path on its own is
/// rejected before any broker connection is attempted.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RabbitMqTlsConfig {
    /// Optional absolute path to a PEM bundle containing private trust roots.
    pub additional_ca_bundle: Option<PathBuf>,
    /// Optional absolute path to the PEM client certificate chain.
    pub client_certificate_chain: Option<PathBuf>,
    /// Optional absolute path to the unencrypted PEM client private key.
    pub client_private_key: Option<PathBuf>,
}

impl fmt::Debug for RabbitMqTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqTlsConfig")
            .field(
                "has_additional_ca_bundle",
                &self.additional_ca_bundle.is_some(),
            )
            .field(
                "has_client_certificate_chain",
                &self.client_certificate_chain.is_some(),
            )
            .field("has_client_private_key", &self.client_private_key.is_some())
            .finish()
    }
}

/// WebSocket configuration section
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSocketConfig {
    /// Enable WebSocket server (default: false)
    pub enabled: bool,
    /// WebSocket listener host (default: "127.0.0.1")
    pub host: String,
    /// WebSocket server port (default: 8081)
    pub port: u16,
    /// Exact HTTP Upgrade endpoint path (default: "/ws")
    pub endpoint_path: String,
    /// Maximum concurrent WebSocket connections (default: 1000)
    pub max_connections: usize,
    /// WebSocket ping interval in seconds (default: 30)
    pub ping_interval_secs: u64,
    /// WebSocket idle connection timeout in seconds (default: 60)
    pub idle_timeout_secs: u64,
    /// Maximum reassembled inbound message size in bytes (default: 1MiB).
    pub max_message_size_bytes: usize,
    /// Maximum frame size in bytes (default: 256KiB)
    pub max_frame_size_bytes: usize,
    /// Maximum canonical encoded outbound Text or Binary application message
    /// size in bytes (default: 1MiB).
    ///
    /// This excludes WebSocket framing, transport buffering, and an optional
    /// distributed backplane envelope.
    pub max_outbound_message_size_bytes: usize,
    /// HTTP Upgrade handshake timeout in seconds (default: 10)
    pub handshake_timeout_secs: u64,
    /// Handshake/identity/admit/opened invocation cap and aggregate closed-chain
    /// cap (default: 10 seconds).
    pub connection_middleware_timeout_secs: u64,
    /// One deadline for the complete message pipeline, including normal reverse
    /// middleware, guards, extraction, action and response preparation (default: 30 seconds).
    pub message_timeout_secs: u64,
    /// Aggregate cap for one message termination cleanup chain (default: 10 seconds).
    /// This is independent of the normal message execution deadline.
    pub message_cleanup_timeout_secs: u64,
    /// Default connected/disconnected controller hook cap (default: 30 seconds).
    pub connection_lifecycle_timeout_secs: u64,
    /// Supported WebSocket subprotocols in server preference order.
    pub supported_protocols: Vec<String>,
    /// Exact browser origins allowed during HTTP Upgrade.
    pub allowed_origins: Vec<String>,
    /// Explicitly accept every browser Origin.
    pub allow_any_origin: bool,
    /// Explicitly accept clients which omit Origin.
    pub allow_missing_origin: bool,
    /// Require successful subprotocol negotiation.
    pub require_subprotocol: bool,
    /// Maximum complete application messages retained behind the one active
    /// per-connection action (default: 16).
    pub inbound_queue_capacity: usize,
    /// Maximum aggregate payload bytes retained in that inbound queue
    /// (default: 1MiB).
    pub inbound_queue_max_bytes: usize,
    /// Bounded outbound queue capacity per connection (default: 256)
    pub outbound_queue_capacity: usize,
    /// Maximum aggregate canonical application-message bytes admitted to one
    /// connection's outbound data path until each write completes
    /// (default: 1MiB).
    ///
    /// The dequeued in-flight application frame remains charged. Protocol-
    /// control frames and transport-owned buffers are independent from this
    /// application queue budget.
    pub outbound_queue_max_bytes: usize,
    /// Maximum wait for one outbound application message to acquire both the
    /// connection-local message-count and byte admission capacity
    /// (default: 5000 milliseconds; accepted runtime range: 100..=300000).
    ///
    /// Expiry classifies the peer as a slow consumer and initiates WebSocket
    /// Close `1013` with reason `lily.v2.slow_consumer`. This is independent
    /// from the socket write/flush deadline and does not provide delivery,
    /// reconnect replay, event-log, sequence, or acknowledgement guarantees.
    /// Protocol-control frames do not consume this application admission
    /// budget.
    pub outbound_admission_timeout_millis: u64,
    /// Maximum time for one socket write or flush in milliseconds (default: 5000).
    /// WebSocket graceful shutdown also uses this as the aggregate cap for
    /// draining already-admitted terminal frames, sending `1001 Going Away`,
    /// and receiving the peer Close acknowledgement.
    pub write_timeout_millis: u64,
    /// Tungstenite write batching threshold in bytes (default: 128KiB)
    pub write_buffer_size_bytes: usize,
    /// Hard Tungstenite write-buffer ceiling in bytes (default: 2MiB).
    /// It must fit `max_outbound_message_size_bytes` plus the corresponding
    /// unmasked server-frame header.
    pub max_write_buffer_size_bytes: usize,
    /// Maximum rooms one connection may join (default: 128)
    pub max_rooms_per_connection: usize,
    /// Maximum UTF-8 byte length of one room name (default: 128)
    pub max_room_name_length: usize,
    /// Socket CIDR networks allowed to supply `X-Forwarded-For`.
    /// Empty by default, so forwarding metadata is never trusted implicitly.
    pub trusted_proxy_cidrs: Vec<String>,
    /// Maximum comma-separated `X-Forwarded-For` hops accepted from a trusted proxy.
    pub max_forwarded_hops: usize,
    /// Maximum Pong wait after Ping in seconds (default: 10)
    pub pong_timeout_secs: u64,
    /// Optional Redis-backed distributed online fan-out configuration.
    ///
    /// This section is consumed only when an application explicitly selects
    /// `RedisWebSocketBackplane` on its WebSocket builder. Configuring it does
    /// not enable a backplane by itself.
    pub backplane: Option<RedisWebSocketBackplaneConfig>,
}

/// Redis Pub/Sub transport configuration for Lily's optional WebSocket backplane.
///
/// `redis_url` may contain an exact `${secret:key}` or `${file:/absolute/path}`
/// reference; [`crate::ConfigService`] resolves it before publishing this
/// typed value. `custom_ca_bundle` is instead a direct absolute canonical
/// filesystem path; it is not a secret placeholder. The URL, credentials and
/// CA path are redacted from `Debug`.
///
/// One adapter instance consumes one stable Redis URL. Native Redis Cluster
/// and Sentinel discovery/routing are not part of this configuration model.
/// Redis infrastructure, ACLs, TLS endpoints and high availability remain
/// application/deployment responsibilities.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisWebSocketBackplaneConfig {
    /// Redis or Redis-over-TLS connection URL, including the selected database.
    pub redis_url: String,
    /// Whether verified Redis TLS is mandatory for this endpoint.
    pub use_tls: bool,
    /// Optional absolute canonical PEM trust bundle for a private Redis CA.
    ///
    /// When set, this bundle replaces the adapter's built-in public WebPKI
    /// roots for the Redis connection; it is not merged with them.
    #[serde(default)]
    pub custom_ca_bundle: Option<PathBuf>,
    /// Stable application isolation token used in the private Pub/Sub channel.
    ///
    /// Must contain 1..=64 ASCII alphanumeric, `-` or `_` characters. A period
    /// is reserved as the channel-segment delimiter and is rejected.
    pub application_namespace: String,
    /// Stable deployment/environment isolation token used in the channel.
    ///
    /// Must contain 1..=64 ASCII alphanumeric, `-` or `_` characters. A period
    /// is reserved as the channel-segment delimiter and is rejected.
    pub environment_namespace: String,
    /// Additional application-owned channel token (default: `events`).
    ///
    /// Must contain 1..=64 ASCII alphanumeric, `-` or `_` characters. A period
    /// is reserved as the channel-segment delimiter and is rejected.
    #[serde(default = "default_websocket_backplane_channel_namespace")]
    pub channel_namespace: String,
    /// Maximum publish requests retained in the bounded adapter queue
    /// (1..=1024, default: 64).
    ///
    /// One additional request may be executing as the current Redis command.
    #[serde(default = "default_websocket_backplane_publish_capacity")]
    pub publish_capacity: usize,
    /// Capacity of each adapter-owned Redis ingress stage (1..=4096, default:
    /// 256).
    ///
    /// The Redis callback-to-subscriber stage and subscriber-to-Lily frame
    /// stage are independently bounded at this value.
    #[serde(default = "default_websocket_backplane_ingress_capacity")]
    pub ingress_capacity: usize,
    /// Deadline for establishing one Redis connection (100..=120000 ms,
    /// default: 5000 ms).
    #[serde(default = "default_websocket_backplane_connection_timeout_millis")]
    pub connection_timeout_millis: u64,
    /// Deadline for one Redis command, including publish and subscribe
    /// (10..=300000 ms, default: 2000 ms).
    #[serde(default = "default_websocket_backplane_operation_timeout_millis")]
    pub operation_timeout_millis: u64,
    /// Initial reconnect delay after a publisher or subscriber outage
    /// (10..=60000 ms, default: 100 ms).
    #[serde(default = "default_websocket_backplane_reconnect_initial_millis")]
    pub reconnect_initial_delay_millis: u64,
    /// Maximum reconnect delay after exponential backoff. It must be at least
    /// `reconnect_initial_delay_millis` and at most 300000 ms (default: 5000
    /// ms).
    #[serde(default = "default_websocket_backplane_reconnect_max_millis")]
    pub reconnect_max_delay_millis: u64,
    /// Random downward jitter applied to reconnect delays (default: `0.2`).
    ///
    /// The value must be finite and within 0.0..=1.0. `0.0` disables jitter;
    /// `1.0` permits the whole computed delay window.
    /// Jitter prevents all application nodes from reconnecting to Redis in the
    /// same instant after a shared outage.
    #[serde(default = "default_websocket_backplane_reconnect_jitter_ratio")]
    pub reconnect_jitter_ratio: f64,
}

impl fmt::Debug for RedisWebSocketBackplaneConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisWebSocketBackplaneConfig")
            .field("redis_url", &REDACTED_DEBUG_VALUE)
            .field("use_tls", &self.use_tls)
            .field("custom_ca_bundle", &redacted_option(&self.custom_ca_bundle))
            .field("application_namespace", &self.application_namespace)
            .field("environment_namespace", &self.environment_namespace)
            .field("channel_namespace", &self.channel_namespace)
            .field("publish_capacity", &self.publish_capacity)
            .field("ingress_capacity", &self.ingress_capacity)
            .field("connection_timeout_millis", &self.connection_timeout_millis)
            .field("operation_timeout_millis", &self.operation_timeout_millis)
            .field(
                "reconnect_initial_delay_millis",
                &self.reconnect_initial_delay_millis,
            )
            .field(
                "reconnect_max_delay_millis",
                &self.reconnect_max_delay_millis,
            )
            .field("reconnect_jitter_ratio", &self.reconnect_jitter_ratio)
            .finish()
    }
}

fn default_websocket_backplane_channel_namespace() -> String {
    "events".to_owned()
}

const fn default_websocket_backplane_publish_capacity() -> usize {
    64
}

const fn default_websocket_backplane_ingress_capacity() -> usize {
    256
}

const fn default_websocket_backplane_connection_timeout_millis() -> u64 {
    5_000
}

const fn default_websocket_backplane_operation_timeout_millis() -> u64 {
    2_000
}

const fn default_websocket_backplane_reconnect_initial_millis() -> u64 {
    100
}

const fn default_websocket_backplane_reconnect_max_millis() -> u64 {
    5_000
}

const fn default_websocket_backplane_reconnect_jitter_ratio() -> f64 {
    0.2
}

/// WebSocket client configuration section
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSocketClientConfig {
    /// Configuration mode: "single" or "factory" (default: "single")
    pub mode: Option<String>,
    /// WebSocket server URL - used in single mode
    pub url: Option<String>,
    /// Namespace for Socket.IO-like functionality - used in single mode
    pub namespace: Option<String>,
    /// Enable automatic reconnection (default: true) - used in single mode
    pub reconnection_enabled: Option<bool>,
    /// Maximum reconnection attempts (default: 5) - used in single mode
    pub max_reconnection_attempts: Option<usize>,
    /// Initial reconnection delay in seconds (default: 1) - used in single mode
    pub reconnection_delay_secs: Option<u64>,
    /// Maximum reconnection delay in seconds (default: 30) - used in single mode
    pub max_reconnection_delay_secs: Option<u64>,
    /// Exponential reconnection multiplier (default: 2.0)
    pub backoff_multiplier: Option<f64>,
    /// Reconnection jitter ratio (default: 0.2)
    pub jitter_ratio: Option<f64>,
    /// Ping interval in seconds (default: 30) - used in single mode
    pub ping_interval_secs: Option<u64>,
    /// Pong timeout in seconds (default: 10) - used in single mode
    pub pong_timeout_secs: Option<u64>,
    /// Maximum message size in bytes (default: 1MB) - used in single mode
    pub max_message_size: Option<usize>,
    /// Maximum frame size in bytes (default: 256KiB)
    pub max_frame_size: Option<usize>,
    /// Exact browser Origin sent during the HTTP Upgrade.
    pub origin: Option<String>,
    /// Ordered WebSocket subprotocol preferences.
    pub subprotocols: Vec<String>,
    /// Fail the handshake when the server selects no supported subprotocol.
    pub require_subprotocol: Option<bool>,
    /// Additional public CA bundle used in addition to public WebPKI roots.
    pub additional_ca_bundle: Option<PathBuf>,
    /// File containing only the bearer token. It is re-read before every
    /// initial or reconnect handshake.
    pub authorization_bearer_file: Option<PathBuf>,
    /// Bounded application-to-socket queue capacity.
    pub outbound_queue_capacity: Option<usize>,
    /// Bounded callback dispatch queue capacity.
    pub callback_queue_capacity: Option<usize>,
    /// Maximum concurrently executing callbacks.
    pub callback_concurrency: Option<usize>,
    /// Callback timeout in seconds.
    pub callback_timeout_secs: Option<u64>,
    /// Connect timeout in seconds.
    pub connect_timeout_secs: Option<u64>,
    /// Send acknowledgement timeout in seconds.
    pub send_timeout_secs: Option<u64>,
    /// Graceful client shutdown timeout in seconds.
    pub shutdown_timeout_secs: Option<u64>,
    /// Closing handshake timeout in seconds.
    pub close_timeout_secs: Option<u64>,
    /// Idle connection timeout in seconds.
    pub idle_timeout_secs: Option<u64>,
    /// WebSocket client cells for factory mode
    pub cells: Option<Vec<WebSocketClientCellConfig>>,
}

impl_redacted_debug!(WebSocketClientConfig {
    visible: [
        mode,
        namespace,
        reconnection_enabled,
        max_reconnection_attempts,
        reconnection_delay_secs,
        max_reconnection_delay_secs,
        backoff_multiplier,
        jitter_ratio,
        ping_interval_secs,
        pong_timeout_secs,
        max_message_size,
        max_frame_size,
        origin,
        subprotocols,
        require_subprotocol,
        outbound_queue_capacity,
        callback_queue_capacity,
        callback_concurrency,
        callback_timeout_secs,
        connect_timeout_secs,
        send_timeout_secs,
        shutdown_timeout_secs,
        close_timeout_secs,
        idle_timeout_secs,
        cells,
    ],
    sensitive_options: [url, additional_ca_bundle, authorization_bearer_file],
    sensitive_values: [],
});

/// WebSocket client cell configuration for factory mode
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketClientCellConfig {
    /// Cell name (identifier)
    pub name: String,
    /// WebSocket server URL
    pub url: String,
    /// Namespace for Socket.IO-like functionality
    pub namespace: Option<String>,
    /// Enable automatic reconnection (default: true)
    pub reconnection_enabled: Option<bool>,
    /// Maximum reconnection attempts (default: 5)
    pub max_reconnection_attempts: Option<usize>,
    /// Initial reconnection delay in seconds (default: 1)
    pub reconnection_delay_secs: Option<u64>,
    /// Maximum reconnection delay in seconds (default: 30)
    pub max_reconnection_delay_secs: Option<u64>,
    /// Exponential reconnection multiplier (default: 2.0)
    pub backoff_multiplier: Option<f64>,
    /// Reconnection jitter ratio (default: 0.2)
    pub jitter_ratio: Option<f64>,
    /// Ping interval in seconds (default: 30)
    pub ping_interval_secs: Option<u64>,
    /// Pong timeout in seconds (default: 10)
    pub pong_timeout_secs: Option<u64>,
    /// Maximum message size in bytes (default: 1MB)
    pub max_message_size: Option<usize>,
    /// Maximum frame size in bytes (default: 256KiB)
    pub max_frame_size: Option<usize>,
    /// Exact browser Origin sent during the HTTP Upgrade.
    pub origin: Option<String>,
    /// Ordered WebSocket subprotocol preferences.
    #[serde(default)]
    pub subprotocols: Vec<String>,
    /// Fail the handshake when the server selects no supported subprotocol.
    pub require_subprotocol: Option<bool>,
    /// Additional public CA bundle used in addition to public WebPKI roots.
    pub additional_ca_bundle: Option<PathBuf>,
    /// File containing only the bearer token. It is re-read on reconnect.
    pub authorization_bearer_file: Option<PathBuf>,
    /// Bounded application-to-socket queue capacity.
    pub outbound_queue_capacity: Option<usize>,
    /// Bounded callback dispatch queue capacity.
    pub callback_queue_capacity: Option<usize>,
    /// Maximum concurrently executing callbacks.
    pub callback_concurrency: Option<usize>,
    /// Callback timeout in seconds.
    pub callback_timeout_secs: Option<u64>,
    /// Connect timeout in seconds.
    pub connect_timeout_secs: Option<u64>,
    /// Send acknowledgement timeout in seconds.
    pub send_timeout_secs: Option<u64>,
    /// Graceful client shutdown timeout in seconds.
    pub shutdown_timeout_secs: Option<u64>,
    /// Closing handshake timeout in seconds.
    pub close_timeout_secs: Option<u64>,
    /// Idle connection timeout in seconds.
    pub idle_timeout_secs: Option<u64>,
}

impl_redacted_debug!(WebSocketClientCellConfig {
    visible: [
        name,
        namespace,
        reconnection_enabled,
        max_reconnection_attempts,
        reconnection_delay_secs,
        max_reconnection_delay_secs,
        backoff_multiplier,
        jitter_ratio,
        ping_interval_secs,
        pong_timeout_secs,
        max_message_size,
        max_frame_size,
        origin,
        subprotocols,
        require_subprotocol,
        outbound_queue_capacity,
        callback_queue_capacity,
        callback_concurrency,
        callback_timeout_secs,
        connect_timeout_secs,
        send_timeout_secs,
        shutdown_timeout_secs,
        close_timeout_secs,
        idle_timeout_secs,
    ],
    sensitive_options: [additional_ca_bundle, authorization_bearer_file],
    sensitive_values: [url],
});

/// HTTP Client Factory configuration section
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpClientFactoryConfig {
    /// Connection timeout in seconds (default: 10)
    pub connect_timeout_secs: u64,
    /// Request timeout in seconds (default: 30)
    pub request_timeout_secs: u64,
    /// Maximum number of redirects to follow (default: 5)
    pub max_redirects: u32,
    /// User agent string (default: "lily-http-client/1.0")
    pub user_agent: String,
    /// Default wire protocol policy for named clients.
    pub protocol: HttpClientProtocol,
    /// Maximum requests admitted by one named client.
    pub max_in_flight_requests: usize,
    /// Maximum requests admitted for one origin.
    pub max_in_flight_requests_per_origin: usize,
    /// Maximum buffered request body size.
    pub max_request_body_bytes: usize,
    /// Maximum buffered response body size.
    pub max_response_body_bytes: usize,
    /// Maximum request or response header field count.
    pub max_header_count: usize,
    /// Maximum aggregate request or response header bytes.
    pub max_header_bytes: usize,
    /// Hyper idle-pool retention time in seconds.
    pub pool_idle_timeout_secs: u64,
    /// Maximum idle connections retained per origin and protocol pool.
    pub pool_max_idle_per_host: usize,
    /// Maximum distinct origins whose pools may be retained.
    pub max_retained_origins: usize,
    /// Initial HTTP/2 stream flow-control window.
    pub http2_initial_stream_window_bytes: u32,
    /// Initial HTTP/2 connection flow-control window.
    pub http2_initial_connection_window_bytes: u32,
    /// Maximum HTTP/2 frame size.
    pub http2_max_frame_bytes: u32,
    /// HTTP/2 keep-alive interval in seconds.
    pub http2_keep_alive_interval_secs: u64,
    /// HTTP/2 keep-alive acknowledgement timeout in seconds.
    pub http2_keep_alive_timeout_secs: u64,
    /// Retry only requests proven not to have started on a stale pooled connection.
    pub retry_unstarted_requests: bool,
    /// Named HTTP clients configuration
    pub clients: HashMap<String, HttpClientConfig>,
}

impl_redacted_debug!(HttpClientFactoryConfig {
    visible: [
        connect_timeout_secs,
        request_timeout_secs,
        max_redirects,
        protocol,
        max_in_flight_requests,
        max_in_flight_requests_per_origin,
        max_request_body_bytes,
        max_response_body_bytes,
        max_header_count,
        max_header_bytes,
        pool_idle_timeout_secs,
        pool_max_idle_per_host,
        max_retained_origins,
        http2_initial_stream_window_bytes,
        http2_initial_connection_window_bytes,
        http2_max_frame_bytes,
        http2_keep_alive_interval_secs,
        http2_keep_alive_timeout_secs,
        retry_unstarted_requests,
        clients,
    ],
    sensitive_options: [],
    sensitive_values: [user_agent],
});

/// Individual HTTP Client configuration
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpClientConfig {
    /// Base address for this client (for example, `<https://api.example.com>`).
    pub base_address: String,
    /// Connection timeout in seconds (optional, inherits from factory default)
    pub connect_timeout_secs: Option<u64>,
    /// Request timeout in seconds (optional, inherits from factory default)
    pub request_timeout_secs: Option<u64>,
    /// Maximum number of redirects to follow (optional, inherits from factory default)
    pub max_redirects: Option<u32>,
    /// User agent string (optional, inherits from factory default)
    pub user_agent: Option<String>,
    /// Optional protocol override.
    pub protocol: Option<HttpClientProtocol>,
    /// Optional total request-admission override.
    pub max_in_flight_requests: Option<usize>,
    /// Optional per-origin request-admission override.
    pub max_in_flight_requests_per_origin: Option<usize>,
    /// Optional buffered request-body limit override.
    pub max_request_body_bytes: Option<usize>,
    /// Optional buffered response-body limit override.
    pub max_response_body_bytes: Option<usize>,
    /// Optional request/response header-count override.
    pub max_header_count: Option<usize>,
    /// Optional aggregate request/response header-byte override.
    pub max_header_bytes: Option<usize>,
    /// Optional Hyper idle-pool timeout override, in seconds.
    pub pool_idle_timeout_secs: Option<u64>,
    /// Optional per-origin/protocol idle-connection override.
    pub pool_max_idle_per_host: Option<usize>,
    /// Optional retained-origin registry capacity override.
    pub max_retained_origins: Option<usize>,
    /// Optional initial HTTP/2 stream-window override.
    pub http2_initial_stream_window_bytes: Option<u32>,
    /// Optional initial HTTP/2 connection-window override.
    pub http2_initial_connection_window_bytes: Option<u32>,
    /// Optional maximum HTTP/2 frame-size override.
    pub http2_max_frame_bytes: Option<u32>,
    /// Optional HTTP/2 keep-alive interval override, in seconds.
    pub http2_keep_alive_interval_secs: Option<u64>,
    /// Optional HTTP/2 keep-alive acknowledgement timeout, in seconds.
    pub http2_keep_alive_timeout_secs: Option<u64>,
    /// Optional safe stale-pool retry policy override.
    pub retry_unstarted_requests: Option<bool>,
    /// Optional absolute private trust-root PEM bundle path.
    pub additional_ca_bundle: Option<PathBuf>,
    /// Default headers for this client (e.g., {"Content-Type": "application/json"}).
    /// `User-Agent` must be configured through `user_agent` instead.
    pub headers: Option<HashMap<String, String>>,
}

impl_redacted_debug!(HttpClientConfig {
    visible: [
        connect_timeout_secs,
        request_timeout_secs,
        max_redirects,
        protocol,
        max_in_flight_requests,
        max_in_flight_requests_per_origin,
        max_request_body_bytes,
        max_response_body_bytes,
        max_header_count,
        max_header_bytes,
        pool_idle_timeout_secs,
        pool_max_idle_per_host,
        max_retained_origins,
        http2_initial_stream_window_bytes,
        http2_initial_connection_window_bytes,
        http2_max_frame_bytes,
        http2_keep_alive_interval_secs,
        http2_keep_alive_timeout_secs,
        retry_unstarted_requests,
    ],
    sensitive_options: [user_agent, headers, additional_ca_bundle],
    sensitive_values: [base_address],
});

/// HTTP wire protocol policy shared by factory defaults and named overrides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HttpClientProtocol {
    /// Negotiate HTTP/1.1 or HTTP/2 according to endpoint capabilities.
    #[default]
    Auto,
    /// Require the HTTP/1.1 client path.
    Http1,
    /// Require the HTTP/2 client path.
    Http2,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            max_connections: None,
            max_multipart_part_bytes: None,
            max_multipart_parts: None,
            max_multipart_metadata_bytes: None,
            connection_idle_timeout_secs: None,
            tls_enabled: None,
            tls_cert_path: None,
            tls_key_path: None,
            request_timeout_secs: None,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            mode: Some("single".to_string()),
            database_type: Some("none".to_string()),
            connection_string: Some("".to_string()),
            pool_size: Some(10),
            connection_timeout_secs: Some(30),
            query_timeout_secs: Some(30),
            pooling_enabled: Some(true),
            database_name: Some("default".to_string()),
            host: Some("localhost".to_string()),
            port: Some(27017),
            username: None,
            password: None,
            auth_database: None,
            use_tls: Some(false),
            app_name: Some("lily_app".to_string()),
            cells: None,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            mode: Some("single".to_string()),
            provider: Some("redis".to_string()),
            redis_url: Some("redis://127.0.0.1:6379/0".to_string()),
            use_tls: Some(false),
            additional_ca_bundle: None,
            key_namespace: Some("lily".to_string()),
            default_ttl_secs: Some(3600),
            pool_size: Some(16),
            connection_timeout_secs: Some(5),
            operation_timeout_secs: Some(2),
            scan_page_size: Some(100),
            max_scan_results: Some(1_000),
            cells: None,
        }
    }
}

impl Default for ClickhouseConfig {
    fn default() -> Self {
        Self {
            mode: Some("single".to_string()),
            host: Some("localhost".to_string()),
            port: Some(8123),
            database: Some("default".to_string()),
            username: Some("default".to_string()),
            password: None,
            pool_size: Some(10),
            connection_timeout_secs: Some(30),
            query_timeout_secs: Some(30),
            use_tls: Some(false),
            compression_enabled: Some(true),
            cells: None,
        }
    }
}

impl Default for RabbitMqConsumerConfig {
    fn default() -> Self {
        Self {
            connection_string: None,
            pool_size: 5,
            connection_timeout_secs: Some(10),
            confirm_timeout_secs: Some(10),
            heartbeat_secs: Some(30),
            max_reconnect_attempts: Some(5),
            reconnect_backoff_millis: Some(250),
            use_tls: None,
            tls: RabbitMqTlsConfig::default(),
            persistence_enabled: true,
            username: None,
            password: None,
            hostname: None,
            port: None,
            vhost: None,
        }
    }
}

impl Default for QueueClientConfig {
    fn default() -> Self {
        Self {
            mode: Some("single".to_string()),
            connection_string: None,
            pool_size: Some(5),
            connection_timeout_secs: Some(10),
            confirm_timeout_secs: Some(10),
            heartbeat_secs: Some(30),
            max_reconnect_attempts: Some(5),
            reconnect_backoff_millis: Some(250),
            use_tls: None,
            tls: RabbitMqTlsConfig::default(),
            persistence_enabled: Some(true),
            username: None,
            password: None,
            hostname: None,
            port: None,
            vhost: None,
            cells: None,
        }
    }
}

impl Default for QueueDefinition {
    fn default() -> Self {
        Self {
            name: String::new(),
            exchange_name: String::new(),
            routing_key: String::new(),
            exchange_kind: RabbitMqExchangeKind::Direct,
            queue_type: RabbitMqQueueType::Classic,
            topology_ownership: RabbitMqTopologyOwnership::FrameworkManaged,
            concurrency: 1,
            prefetch_count: 10,
            delivery_buffer_capacity: None,
            retry_attempts: 0,
            retry_backoff_millis: Some(1_000),
            max_retry_backoff_millis: Some(60_000),
            retry_jitter_ratio: 0.0,
            delivery_execution_timeout_millis: Some(30_000),
            settlement_timeout_millis: Some(5_000),
            durable: true,
            max_message_size_bytes: 1024 * 1024,
            retention: None,
            dead_letter_exchange: None,
            dead_letter_routing_key: None,
            message_ttl_ms: None,
            exclusive: false,
            auto_delete: false,
            single_active_consumer: false,
            max_priority: None,
            #[cfg(any(
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-postgresql"
            ))]
            transactional_inbox: None,
        }
    }
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "127.0.0.1".to_string(),
            port: 8081,
            endpoint_path: "/ws".to_string(),
            max_connections: 1000,
            ping_interval_secs: 30,
            idle_timeout_secs: 60,
            max_message_size_bytes: 1024 * 1024, // 1MB
            max_frame_size_bytes: 256 * 1024,
            max_outbound_message_size_bytes: 1024 * 1024,
            handshake_timeout_secs: 10,
            connection_middleware_timeout_secs: 10,
            message_timeout_secs: 30,
            message_cleanup_timeout_secs: 10,
            connection_lifecycle_timeout_secs: 30,
            supported_protocols: vec!["lily.v2".to_string()],
            allowed_origins: Vec::new(),
            allow_any_origin: false,
            allow_missing_origin: false,
            require_subprotocol: false,
            inbound_queue_capacity: 16,
            inbound_queue_max_bytes: 1024 * 1024,
            outbound_queue_capacity: 256,
            outbound_queue_max_bytes: 1024 * 1024,
            outbound_admission_timeout_millis: 5_000,
            write_timeout_millis: 5_000,
            write_buffer_size_bytes: 128 * 1024,
            max_write_buffer_size_bytes: 2 * 1024 * 1024,
            max_rooms_per_connection: 128,
            max_room_name_length: 128,
            trusted_proxy_cidrs: Vec::new(),
            max_forwarded_hops: 16,
            pong_timeout_secs: 10,
            backplane: None,
        }
    }
}

impl Default for WebSocketClientConfig {
    fn default() -> Self {
        Self {
            mode: Some("single".to_string()),
            url: None,
            namespace: None,
            reconnection_enabled: Some(true),
            max_reconnection_attempts: Some(5),
            reconnection_delay_secs: Some(1),
            max_reconnection_delay_secs: Some(30),
            backoff_multiplier: Some(2.0),
            jitter_ratio: Some(0.2),
            ping_interval_secs: Some(30),
            pong_timeout_secs: Some(10),
            max_message_size: Some(1024 * 1024), // 1MB
            max_frame_size: Some(256 * 1024),
            origin: None,
            subprotocols: Vec::new(),
            require_subprotocol: Some(false),
            additional_ca_bundle: None,
            authorization_bearer_file: None,
            outbound_queue_capacity: Some(256),
            callback_queue_capacity: Some(256),
            callback_concurrency: Some(16),
            callback_timeout_secs: Some(30),
            connect_timeout_secs: Some(10),
            send_timeout_secs: Some(10),
            shutdown_timeout_secs: Some(10),
            close_timeout_secs: Some(5),
            idle_timeout_secs: Some(90),
            cells: None,
        }
    }
}

impl Default for HttpClientFactoryConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            max_redirects: 5,
            user_agent: "lily-http-client/1.0".to_string(),
            protocol: HttpClientProtocol::Auto,
            max_in_flight_requests: 1024,
            max_in_flight_requests_per_origin: 256,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_response_body_bytes: 16 * 1024 * 1024,
            max_header_count: 128,
            max_header_bytes: 64 * 1024,
            pool_idle_timeout_secs: 90,
            pool_max_idle_per_host: 32,
            max_retained_origins: 64,
            http2_initial_stream_window_bytes: 1024 * 1024,
            http2_initial_connection_window_bytes: 2 * 1024 * 1024,
            http2_max_frame_bytes: 16 * 1024,
            http2_keep_alive_interval_secs: 30,
            http2_keep_alive_timeout_secs: 10,
            retry_unstarted_requests: true,
            clients: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lily_config_default() {
        let config = LilyConfig::default();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.lifecycle.shutdown_timeout_secs, 30);
        assert!(config.database.is_none());
    }

    #[test]
    fn lifecycle_config_is_serde_compatible_and_defaults_missing_values() {
        let configured: LifecycleConfig = toml::from_str("shutdown_timeout_secs = 45").unwrap();
        assert_eq!(configured.shutdown_timeout_secs, 45);

        let defaulted: LifecycleConfig = toml::from_str("").unwrap();
        assert_eq!(defaulted, LifecycleConfig::default());

        let serialized = toml::to_string(&configured).unwrap();
        assert!(serialized.contains("shutdown_timeout_secs = 45"));
    }

    #[test]
    fn queue_defaults_are_bounded_and_removed_pseudo_features_are_rejected() {
        let queue = QueueDefinition::default();
        assert_eq!(queue.concurrency, 1);
        assert_eq!(queue.prefetch_count, 10);
        assert_eq!(queue.delivery_buffer_capacity, None);
        assert_eq!(queue.retry_attempts, 0);
        assert_eq!(queue.max_message_size_bytes, 1024 * 1024);
        assert_eq!(queue.delivery_execution_timeout_millis, Some(30_000));
        assert_eq!(queue.settlement_timeout_millis, Some(5_000));
        assert_eq!(queue.exchange_kind, RabbitMqExchangeKind::Direct);
        assert_eq!(queue.queue_type, RabbitMqQueueType::Classic);
        assert_eq!(
            queue.topology_ownership,
            RabbitMqTopologyOwnership::FrameworkManaged
        );
        assert!(!queue.single_active_consumer);
        assert_eq!(queue.max_priority, None);

        for source in [
            "[rabbitmq.consumer]\nbroker_type = 'rabbitmq'",
            "[rabbitmq.consumer]\nmessage_timeout_secs = 30",
            "[queue_client]\nprovider_type = 'rabbitmq'",
            "[queue_client]\nmessage_timeout_secs = 30",
            "[[rabbitmq.topology.queues]]\nbatch_enabled = false",
            "[[rabbitmq.topology.queues]]\nauto_ack = false",
            "[[rabbitmq.topology.queues]]\nhandler_timeout_millis = 30000",
            "[[rabbitmq.topology.queues]]\nservice_name = 'legacy'",
            "[rabbitmq.consumer]\ndefault_exchange = 'legacy'",
            "[queue_client]\ndefault_exchange = 'legacy'",
        ] {
            assert!(toml::from_str::<LilyConfig>(source).is_err(), "{source}");
        }
    }

    #[test]
    fn rabbitmq_topology_enums_use_snake_case_serde_values() {
        let config: LilyConfig = toml::from_str(
            r#"
[[rabbitmq.topology.queues]]
name = "orders"
exchange_name = "events"
routing_key = "orders.created"
exchange_kind = "direct"
queue_type = "quorum"
topology_ownership = "framework_managed"
single_active_consumer = true
"#,
        )
        .unwrap();
        let queue = &config.rabbitmq.topology.queues[0];
        assert_eq!(queue.exchange_kind, RabbitMqExchangeKind::Direct);
        assert_eq!(queue.queue_type, RabbitMqQueueType::Quorum);
        assert_eq!(
            queue.topology_ownership,
            RabbitMqTopologyOwnership::FrameworkManaged
        );
        assert!(queue.single_active_consumer);

        let serialized = toml::to_string(&config).unwrap();
        assert!(serialized.contains("queue_type = \"quorum\""));
        assert!(serialized.contains("topology_ownership = \"framework_managed\""));
    }

    #[test]
    fn rabbitmq_consumer_and_topology_have_one_nested_canonical_serde_surface() {
        let source = r#"
[rabbitmq.consumer]
connection_string = "amqps://consumer:secret@rabbit.internal/%2f"
pool_size = 7
use_tls = true

[[rabbitmq.topology.queues]]
name = "orders.created"
exchange_name = "orders"
routing_key = "orders.created"
retry_jitter_ratio = 0.2
retention = { main_max_messages = 100, main_max_bytes = 1048576, retry_bucket_max_messages = 10, retry_bucket_max_bytes = 262144, dead_letter_max_messages = 10, dead_letter_max_bytes = 262144 }
"#;
        let config: LilyConfig = toml::from_str(source).unwrap();
        let consumer = config.rabbitmq.consumer.as_ref().unwrap();
        assert_eq!(consumer.pool_size, 7);
        assert_eq!(config.rabbitmq.topology.queues.len(), 1);
        assert_eq!(config.rabbitmq.topology.queues[0].name, "orders.created");
        assert_eq!(config.rabbitmq.topology.queues[0].retry_jitter_ratio, 0.2);

        let serialized = toml::to_string(&config).unwrap();
        assert!(serialized.contains("[rabbitmq.consumer]"));
        assert!(serialized.contains("[[rabbitmq.topology.queues]]"));
        assert!(!serialized.contains("message_broker"));
        assert!(!serialized.contains("rabbitmq_topology"));

        let round_trip: LilyConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(round_trip.rabbitmq.consumer.unwrap().pool_size, 7);
        assert_eq!(
            round_trip.rabbitmq.topology.queues[0].name,
            "orders.created"
        );
        assert_eq!(
            round_trip.rabbitmq.topology.queues[0].retry_jitter_ratio,
            0.2
        );
    }

    #[test]
    fn queue_client_cannot_define_a_second_topology_authority() {
        let source = r#"
[queue_client]
connection_string = "amqps://publisher:secret@rabbit.internal/%2f"

[queue_client.topology]
queues = []
"#;

        assert!(toml::from_str::<LilyConfig>(source).is_err());
    }

    #[test]
    fn http_client_factory_rejects_removed_tls_verification_switches() {
        let factory_error = toml::from_str::<LilyConfig>(
            r#"
[http_client_factory]
verify_ssl = true
"#,
        )
        .unwrap_err();
        assert!(factory_error.to_string().contains("verify_ssl"));

        let named_client_error = toml::from_str::<LilyConfig>(
            r#"
[http_client_factory.clients.inventory]
base_address = "https://inventory.example.test"
verify_ssl = true
"#,
        )
        .unwrap_err();
        assert!(named_client_error.to_string().contains("verify_ssl"));
    }

    #[test]
    fn http_client_factory_deserializes_every_supported_field() {
        let config = toml::from_str::<LilyConfig>(
            r#"
[http_client_factory]
connect_timeout_secs = 7
request_timeout_secs = 19
max_redirects = 3
user_agent = "lily-config-contract/1.0"
protocol = "http2"
max_in_flight_requests = 80
max_in_flight_requests_per_origin = 10
max_request_body_bytes = 1111
max_response_body_bytes = 2222
max_header_count = 40
max_header_bytes = 4000
pool_idle_timeout_secs = 44
pool_max_idle_per_host = 7
max_retained_origins = 8
http2_initial_stream_window_bytes = 65536
http2_initial_connection_window_bytes = 131072
http2_max_frame_bytes = 32768
http2_keep_alive_interval_secs = 21
http2_keep_alive_timeout_secs = 6
retry_unstarted_requests = false

[http_client_factory.clients.inventory]
base_address = "https://inventory.example.test/v1"
connect_timeout_secs = 11
request_timeout_secs = 23
max_redirects = 1
user_agent = "inventory-client/1.0"
protocol = "http1"
max_in_flight_requests = 90
max_in_flight_requests_per_origin = 9
max_request_body_bytes = 3333
max_response_body_bytes = 4444
max_header_count = 50
max_header_bytes = 5000
pool_idle_timeout_secs = 55
pool_max_idle_per_host = 6
max_retained_origins = 7
http2_initial_stream_window_bytes = 98304
http2_initial_connection_window_bytes = 196608
http2_max_frame_bytes = 65536
http2_keep_alive_interval_secs = 22
http2_keep_alive_timeout_secs = 7
retry_unstarted_requests = true
additional_ca_bundle = "/run/secrets/inventory-ca.pem"
headers = { Accept = "application/json", X-Tenant = "catalog" }
"#,
        )
        .unwrap();

        let factory = config.http_client_factory.unwrap();
        assert_eq!(factory.connect_timeout_secs, 7);
        assert_eq!(factory.request_timeout_secs, 19);
        assert_eq!(factory.max_redirects, 3);
        assert_eq!(factory.user_agent, "lily-config-contract/1.0");
        assert_eq!(factory.protocol, HttpClientProtocol::Http2);
        assert_eq!(factory.max_in_flight_requests, 80);
        assert_eq!(factory.max_in_flight_requests_per_origin, 10);
        assert_eq!(factory.max_request_body_bytes, 1111);
        assert_eq!(factory.max_response_body_bytes, 2222);
        assert_eq!(factory.max_header_count, 40);
        assert_eq!(factory.max_header_bytes, 4000);
        assert_eq!(factory.pool_idle_timeout_secs, 44);
        assert_eq!(factory.pool_max_idle_per_host, 7);
        assert_eq!(factory.max_retained_origins, 8);
        assert_eq!(factory.http2_initial_stream_window_bytes, 65536);
        assert_eq!(factory.http2_initial_connection_window_bytes, 131072);
        assert_eq!(factory.http2_max_frame_bytes, 32768);
        assert_eq!(factory.http2_keep_alive_interval_secs, 21);
        assert_eq!(factory.http2_keep_alive_timeout_secs, 6);
        assert!(!factory.retry_unstarted_requests);

        let inventory = &factory.clients["inventory"];
        assert_eq!(inventory.base_address, "https://inventory.example.test/v1");
        assert_eq!(inventory.connect_timeout_secs, Some(11));
        assert_eq!(inventory.request_timeout_secs, Some(23));
        assert_eq!(inventory.max_redirects, Some(1));
        assert_eq!(inventory.protocol, Some(HttpClientProtocol::Http1));
        assert_eq!(inventory.max_in_flight_requests, Some(90));
        assert_eq!(inventory.max_in_flight_requests_per_origin, Some(9));
        assert_eq!(inventory.max_request_body_bytes, Some(3333));
        assert_eq!(inventory.max_response_body_bytes, Some(4444));
        assert_eq!(inventory.max_header_count, Some(50));
        assert_eq!(inventory.max_header_bytes, Some(5000));
        assert_eq!(inventory.pool_idle_timeout_secs, Some(55));
        assert_eq!(inventory.pool_max_idle_per_host, Some(6));
        assert_eq!(inventory.max_retained_origins, Some(7));
        assert_eq!(inventory.http2_initial_stream_window_bytes, Some(98304));
        assert_eq!(
            inventory.http2_initial_connection_window_bytes,
            Some(196608)
        );
        assert_eq!(inventory.http2_max_frame_bytes, Some(65536));
        assert_eq!(inventory.http2_keep_alive_interval_secs, Some(22));
        assert_eq!(inventory.http2_keep_alive_timeout_secs, Some(7));
        assert_eq!(inventory.retry_unstarted_requests, Some(true));
        assert_eq!(
            inventory.additional_ca_bundle.as_deref(),
            Some(std::path::Path::new("/run/secrets/inventory-ca.pem"))
        );
        assert_eq!(
            inventory.user_agent.as_deref(),
            Some("inventory-client/1.0")
        );
        let headers = inventory.headers.as_ref().unwrap();
        assert_eq!(headers["Accept"], "application/json");
        assert_eq!(headers["X-Tenant"], "catalog");
    }

    #[test]
    fn test_lily_config_structure() {
        let config = LilyConfig::default();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
        assert!(config.database.is_none());
    }

    #[test]
    fn test_lily_config_builder_pattern() {
        // Test that we can build configs programmatically
        let mut config = LilyConfig::default();
        config.server.host = "0.0.0.0".to_string();
        config.server.port = 9090;
        config.server.max_connections = Some(5000);

        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 9090);
        assert_eq!(config.server.max_connections, Some(5000));
    }

    #[test]
    fn server_schema_accepts_canonical_http_and_multipart_fields_and_rejects_removed_ones() {
        let parsed: LilyConfig = toml::from_str(
            r#"
[server]
connection_idle_timeout_secs = 45
max_multipart_part_bytes = 4096
max_multipart_parts = 17
max_multipart_metadata_bytes = 2048
"#,
        )
        .expect("canonical HTTP idle timeout must deserialize");
        assert_eq!(parsed.server.connection_idle_timeout_secs, Some(45));
        assert_eq!(parsed.server.max_multipart_part_bytes, Some(4096));
        assert_eq!(parsed.server.max_multipart_parts, Some(17));
        assert_eq!(parsed.server.max_multipart_metadata_bytes, Some(2048));

        for removed in ["connection_timeout_secs = 45", "compression_enabled = true"] {
            let source = format!("[server]\n{removed}\n");
            assert!(
                toml::from_str::<LilyConfig>(&source).is_err(),
                "removed HTTP server field must be rejected: {removed}"
            );
        }
    }

    #[test]
    fn websocket_server_schema_exposes_the_complete_canonical_profile() {
        let defaults = WebSocketConfig::default();
        assert_eq!(defaults.message_timeout_secs, 30);
        assert_eq!(defaults.message_cleanup_timeout_secs, 10);
        assert_eq!(defaults.connection_middleware_timeout_secs, 10);
        assert_eq!(defaults.connection_lifecycle_timeout_secs, 30);
        assert_eq!(defaults.supported_protocols, ["lily.v2"]);
        assert_eq!(defaults.inbound_queue_capacity, 16);
        assert_eq!(defaults.inbound_queue_max_bytes, 1024 * 1024);
        assert_eq!(defaults.max_outbound_message_size_bytes, 1024 * 1024);
        assert_eq!(defaults.outbound_queue_max_bytes, 1024 * 1024);
        assert_eq!(defaults.outbound_admission_timeout_millis, 5_000);
        let omitted: LilyConfig = toml::from_str("[websocket]\nenabled = true\n")
            .expect("pre-outbound-policy WebSocket profiles must retain typed defaults");
        let omitted = omitted.websocket.expect("websocket section");
        assert_eq!(
            omitted.max_outbound_message_size_bytes,
            defaults.max_outbound_message_size_bytes
        );
        assert_eq!(
            omitted.outbound_queue_max_bytes,
            defaults.outbound_queue_max_bytes
        );
        assert_eq!(
            omitted.outbound_admission_timeout_millis,
            defaults.outbound_admission_timeout_millis
        );
        let parsed: LilyConfig = toml::from_str(
            r#"
[websocket]
enabled = true
host = "0.0.0.0"
port = 9091
endpoint_path = "/socket"
max_connections = 77
ping_interval_secs = 31
idle_timeout_secs = 61
max_message_size_bytes = 524288
max_frame_size_bytes = 131072
max_outbound_message_size_bytes = 393216
handshake_timeout_secs = 11
connection_middleware_timeout_secs = 12
message_timeout_secs = 9
message_cleanup_timeout_secs = 13
connection_lifecycle_timeout_secs = 27
supported_protocols = ["lily.v2"]
allowed_origins = ["https://app.example.test"]
allow_any_origin = false
allow_missing_origin = false
require_subprotocol = true
inbound_queue_capacity = 12
inbound_queue_max_bytes = 786432
outbound_queue_capacity = 99
outbound_queue_max_bytes = 1048576
outbound_admission_timeout_millis = 1250
write_timeout_millis = 7500
write_buffer_size_bytes = 65536
max_write_buffer_size_bytes = 1048576
max_rooms_per_connection = 44
max_room_name_length = 66
trusted_proxy_cidrs = ["10.0.0.0/8", "2001:db8::/32"]
max_forwarded_hops = 7
pong_timeout_secs = 13
"#,
        )
        .expect("canonical WebSocket server profile must deserialize");
        let websocket = parsed.websocket.expect("websocket section");
        assert_eq!(websocket.host, "0.0.0.0");
        assert_eq!(websocket.endpoint_path, "/socket");
        assert_eq!(websocket.idle_timeout_secs, 61);
        assert_eq!(websocket.max_frame_size_bytes, 131072);
        assert_eq!(websocket.max_outbound_message_size_bytes, 393216);
        assert!(websocket.require_subprotocol);
        assert_eq!(websocket.inbound_queue_capacity, 12);
        assert_eq!(websocket.inbound_queue_max_bytes, 786432);
        assert_eq!(websocket.outbound_queue_capacity, 99);
        assert_eq!(websocket.outbound_queue_max_bytes, 1048576);
        assert_eq!(websocket.outbound_admission_timeout_millis, 1250);
        assert_eq!(websocket.write_timeout_millis, 7500);
        assert_eq!(websocket.message_timeout_secs, 9);
        assert_eq!(websocket.message_cleanup_timeout_secs, 13);
        assert_eq!(websocket.connection_middleware_timeout_secs, 12);
        assert_eq!(websocket.connection_lifecycle_timeout_secs, 27);
        assert_eq!(websocket.write_buffer_size_bytes, 65536);
        assert_eq!(websocket.max_write_buffer_size_bytes, 1048576);
        assert_eq!(
            websocket.trusted_proxy_cidrs,
            ["10.0.0.0/8", "2001:db8::/32"]
        );
        assert_eq!(websocket.max_forwarded_hops, 7);

        for removed in [
            "connection_timeout_secs = 45",
            "enable_cors = false",
            "middleware_timeout_secs = 10",
            "guard_timeout_secs = 10",
            "action_timeout_secs = 30",
        ] {
            let source = format!("[websocket]\n{removed}\n");
            assert!(
                toml::from_str::<LilyConfig>(&source).is_err(),
                "removed WebSocket field must be rejected: {removed}"
            );
        }
    }

    #[test]
    fn websocket_client_schema_carries_reconnect_and_frame_parity_fields() {
        let parsed: LilyConfig = toml::from_str(
            r#"
[websocket_client]
mode = "factory"

[[websocket_client.cells]]
name = "events"
url = "wss://events.example.test/socket"
backoff_multiplier = 3.0
jitter_ratio = 0.1
max_message_size = 1048576
max_frame_size = 131072
"#,
        )
        .expect("WebSocket client parity fields must deserialize");
        let cell = &parsed
            .websocket_client
            .expect("websocket client section")
            .cells
            .expect("factory cells")[0];
        assert_eq!(cell.backoff_multiplier, Some(3.0));
        assert_eq!(cell.jitter_ratio, Some(0.1));
        assert_eq!(cell.max_frame_size, Some(131072));
    }

    #[test]
    fn postgresql_config_is_separate_from_mongodb_database_config() {
        let parsed: LilyConfig = toml::from_str(
            r#"
[database]
database_type = "mongodb"
connection_string = "mongodb://mongo.internal/app"

[postgresql]
mode = "single"
connection_string = "postgresql://postgres.internal/app"
"#,
        )
        .unwrap();

        assert_eq!(
            parsed
                .database
                .as_ref()
                .and_then(|database| database.database_type.as_deref()),
            Some("mongodb")
        );
        assert_eq!(
            parsed.postgresql.as_ref().map(|postgresql| postgresql.mode),
            Some(PgMode::Single)
        );
    }

    #[test]
    fn postgresql_debug_redacts_single_and_factory_connection_strings() {
        let mut config = PgConfig {
            connection_string: Some("postgresql://alice:secret@db/app".into()),
            tls: PgTlsConfig {
                additional_ca_bundle: Some("/run/secrets/postgres-ca.pem".into()),
                ..PgTlsConfig::default()
            },
            ..PgConfig::default()
        };
        config.cells.push(PgCellConfig {
            name: "orders".into(),
            connection_string: "postgresql://bob:other-secret@db/orders".into(),
            ..PgCellConfig::default()
        });

        let debug = format!("{config:?}");
        assert!(!debug.contains("alice"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("bob"));
        assert!(!debug.contains("/run/secrets/postgres-ca.pem"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn debug_redacts_every_secret_bearing_config_surface() {
        let config: LilyConfig = toml::from_str(
            r#"
[server]
tls_key_path = "/run/secrets/tls-private-key-sentinel"

[database]
connection_string = "mongodb://db-user:db-uri-secret-sentinel@mongo/app"
password = "db-password-sentinel"

[[database.cells]]
name = "orders"
database_type = "mongodb"
connection_string = "mongodb://cell-user:db-cell-uri-secret-sentinel@mongo/orders"
database_name = "orders"
password = "db-cell-password-sentinel"

[cache]
redis_url = "rediss://cache-user:cache-uri-secret-sentinel@redis/0"
additional_ca_bundle = "/run/secrets/cache-ca-sentinel"

[[cache.cells]]
name = "sessions"
provider = "redis"
redis_url = "rediss://cell-user:cache-cell-uri-secret-sentinel@redis/1"
additional_ca_bundle = "/run/secrets/cache-cell-ca-sentinel"

[clickhouse]
password = "clickhouse-password-sentinel"

[[clickhouse.cells]]
name = "analytics"
host = "clickhouse.internal"
database = "analytics"
password = "clickhouse-cell-password-sentinel"

[rabbitmq.consumer]
connection_string = "amqps://publisher:broker-uri-secret-sentinel@rabbit/%2f"
password = "broker-password-sentinel"

[queue_client]
connection_string = "amqps://client:queue-uri-secret-sentinel@rabbit/%2f"
password = "queue-password-sentinel"

[[queue_client.cells]]
name = "events"
connection_string = "amqps://cell:queue-cell-uri-secret-sentinel@rabbit/%2f"
password = "queue-cell-password-sentinel"

[websocket_client]
url = "wss://socket-url-secret-sentinel@example.test/events?token=hidden"
additional_ca_bundle = "/run/secrets/socket-ca-sentinel"
authorization_bearer_file = "/run/secrets/socket-bearer-sentinel"

[[websocket_client.cells]]
name = "updates"
url = "wss://socket-cell-url-secret-sentinel@example.test/events"
additional_ca_bundle = "/run/secrets/socket-cell-ca-sentinel"
authorization_bearer_file = "/run/secrets/socket-cell-bearer-sentinel"

[http_client_factory]
user_agent = "http-factory-user-agent-sentinel"

[http_client_factory.clients.inventory]
base_address = "https://http-url-secret-sentinel@example.test/api?token=hidden"
user_agent = "http-client-user-agent-sentinel"
additional_ca_bundle = "/run/secrets/http-client-ca-sentinel"
headers = { Authorization = "Bearer http-authorization-sentinel", X-Api-Key = "http-api-key-sentinel" }

[custom]
private_value = "custom-value-secret-sentinel"
"#,
        )
        .unwrap();

        let direct_debug = [
            format!("{:?}", config.server),
            format!("{:?}", config.database.as_ref().unwrap()),
            format!("{:?}", config.cache.as_ref().unwrap()),
            format!("{:?}", config.clickhouse.as_ref().unwrap()),
            format!("{:?}", config.rabbitmq.consumer.as_ref().unwrap()),
            format!("{:?}", config.queue_client.as_ref().unwrap()),
            format!("{:?}", config.websocket_client.as_ref().unwrap()),
            format!(
                "{:?}",
                &config.http_client_factory.as_ref().unwrap().clients["inventory"]
            ),
            format!("{:?}", config.http_client_factory.as_ref().unwrap()),
        ]
        .join("\n");
        let top_level_debug = format!("{config:#?}");

        let sensitive_literals = [
            "tls-private-key-sentinel",
            "db-uri-secret-sentinel",
            "db-password-sentinel",
            "db-cell-uri-secret-sentinel",
            "db-cell-password-sentinel",
            "cache-uri-secret-sentinel",
            "cache-ca-sentinel",
            "cache-cell-uri-secret-sentinel",
            "cache-cell-ca-sentinel",
            "clickhouse-password-sentinel",
            "clickhouse-cell-password-sentinel",
            "broker-uri-secret-sentinel",
            "broker-password-sentinel",
            "queue-uri-secret-sentinel",
            "queue-password-sentinel",
            "queue-cell-uri-secret-sentinel",
            "queue-cell-password-sentinel",
            "socket-url-secret-sentinel",
            "socket-ca-sentinel",
            "socket-bearer-sentinel",
            "socket-cell-url-secret-sentinel",
            "socket-cell-ca-sentinel",
            "socket-cell-bearer-sentinel",
            "http-url-secret-sentinel",
            "http-factory-user-agent-sentinel",
            "http-client-user-agent-sentinel",
            "http-client-ca-sentinel",
            "http-authorization-sentinel",
            "http-api-key-sentinel",
            "custom-value-secret-sentinel",
        ];

        for sensitive in sensitive_literals {
            assert!(
                !direct_debug.contains(sensitive),
                "direct Debug leaked {sensitive}: {direct_debug}"
            );
            assert!(
                !top_level_debug.contains(sensitive),
                "LilyConfig Debug leaked {sensitive}: {top_level_debug}"
            );
        }
        assert!(direct_debug.contains(REDACTED_DEBUG_VALUE));
        assert!(top_level_debug.contains(REDACTED_DEBUG_VALUE));
        assert!(top_level_debug.contains("private_value"));
    }
}
