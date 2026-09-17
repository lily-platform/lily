use std::{
    fmt,
    path::{Component, Path},
    time::Duration,
};

use deadpool::managed::{PoolConfig, Timeouts};
use lily_config::{CacheCellConfig, CacheConfig};
use redis::{ConnectionAddr, IntoConnectionInfo};

use crate::CacheError;
use crate::pool::{RedisPool, RedisPoolManager};

const MAX_TTL_SECS: u64 = 31_536_000;
const MAX_SCAN_RESULTS: usize = 10_000;

#[derive(Clone)]
/// Validated, secret-redacting connection and operation plan for one Redis cell.
///
/// Build a plan from Lily's typed configuration. Validation performs no
/// network I/O; [`crate::CacheService::connect`] consumes the plan.
pub struct RedisCachePlan {
    redis_url: String,
    use_tls: bool,
    database: i64,
    has_auth: bool,
    key_namespace: String,
    default_ttl: Duration,
    pool_size: usize,
    connection_timeout: Duration,
    operation_timeout: Duration,
    scan_page_size: usize,
    max_scan_results: usize,
    additional_ca_pem: Option<Vec<u8>>,
}

impl RedisCachePlan {
    /// Builds the default single-service plan from `[cache]` configuration.
    pub fn from_single(config: &CacheConfig) -> Result<Self, CacheError> {
        if config.mode.as_deref().unwrap_or("single") != "single" {
            return Err(CacheError::InvalidConfiguration(
                "CacheService requires cache.mode = \"single\"".into(),
            ));
        }
        let provider = config.provider.as_deref().unwrap_or("redis");
        if provider != "redis" {
            return Err(CacheError::InvalidConfiguration(format!(
                "unsupported cache provider {provider:?}; Lily V1 supports only redis"
            )));
        }
        let plan = Self::build(
            config.redis_url.as_deref(),
            config.use_tls,
            config.key_namespace.as_deref(),
            config.default_ttl_secs,
            config.pool_size,
            config.connection_timeout_secs,
            config.operation_timeout_secs,
            config.scan_page_size,
            config.max_scan_results,
        )?;
        plan.with_additional_ca_bundle(config.additional_ca_bundle.as_deref())
    }

    /// Builds a plan for one named factory cell.
    pub fn from_cell(cell: &CacheCellConfig) -> Result<Self, CacheError> {
        if cell.provider != "redis" {
            return Err(CacheError::InvalidConfiguration(format!(
                "cache cell {:?} uses unsupported provider {:?}; Lily V1 supports only redis",
                cell.name, cell.provider
            )));
        }
        let plan = Self::build(
            cell.redis_url.as_deref(),
            cell.use_tls,
            cell.key_namespace.as_deref(),
            cell.default_ttl_secs,
            cell.pool_size,
            cell.connection_timeout_secs,
            cell.operation_timeout_secs,
            cell.scan_page_size,
            cell.max_scan_results,
        )?;
        plan.with_additional_ca_bundle(cell.additional_ca_bundle.as_deref())
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        redis_url: Option<&str>,
        use_tls: Option<bool>,
        key_namespace: Option<&str>,
        default_ttl_secs: Option<u64>,
        pool_size: Option<usize>,
        connection_timeout_secs: Option<u64>,
        operation_timeout_secs: Option<u64>,
        scan_page_size: Option<usize>,
        max_scan_results: Option<usize>,
    ) -> Result<Self, CacheError> {
        let redis_url = redis_url
            .ok_or_else(|| CacheError::InvalidConfiguration("redis_url is required".into()))?;
        let use_tls = use_tls.ok_or_else(|| {
            CacheError::InvalidConfiguration(
                "use_tls must be explicit so transport cannot silently downgrade".into(),
            )
        })?;
        let connection = redis_url
            .into_connection_info()
            .map_err(|_| CacheError::InvalidConfiguration("redis_url is invalid".into()))?;
        match &connection.addr {
            ConnectionAddr::Tcp(_, _) if use_tls => {
                return Err(CacheError::InvalidConfiguration(
                    "use_tls=true requires a rediss:// URL".into(),
                ));
            }
            ConnectionAddr::TcpTls { insecure: true, .. } => {
                return Err(CacheError::InvalidConfiguration(
                    "insecure Redis TLS verification is unsupported".into(),
                ));
            }
            ConnectionAddr::TcpTls { .. } if !use_tls => {
                return Err(CacheError::InvalidConfiguration(
                    "rediss:// requires use_tls=true".into(),
                ));
            }
            ConnectionAddr::Unix(_) => {
                return Err(CacheError::InvalidConfiguration(
                    "Unix socket Redis URLs are outside the Lily V1 support profile".into(),
                ));
            }
            _ => {}
        }
        if connection.redis.db < 0 {
            return Err(CacheError::InvalidConfiguration(
                "Redis database number cannot be negative".into(),
            ));
        }

        let key_namespace = key_namespace.unwrap_or("lily");
        if key_namespace.is_empty()
            || key_namespace.len() > 64
            || !key_namespace
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "-_: .".contains(character))
            || key_namespace.contains(' ')
        {
            return Err(CacheError::InvalidConfiguration(
                "key_namespace must contain 1..=64 ASCII alphanumeric, '-', '_', ':' or '.' characters"
                    .into(),
            ));
        }

        let default_ttl_secs = bounded_u64(
            "default_ttl_secs",
            default_ttl_secs.unwrap_or(3_600),
            1,
            MAX_TTL_SECS,
        )?;
        let pool_size = bounded_usize("pool_size", pool_size.unwrap_or(16), 1, 128)?;
        let connection_timeout_secs = bounded_u64(
            "connection_timeout_secs",
            connection_timeout_secs.unwrap_or(5),
            1,
            120,
        )?;
        let operation_timeout_secs = bounded_u64(
            "operation_timeout_secs",
            operation_timeout_secs.unwrap_or(2),
            1,
            300,
        )?;
        let scan_page_size = bounded_usize(
            "scan_page_size",
            scan_page_size.unwrap_or(100),
            1,
            crate::operation::MAX_SCAN_PAGE_SIZE,
        )?;
        let max_scan_results = bounded_usize(
            "max_scan_results",
            max_scan_results.unwrap_or(1_000),
            scan_page_size,
            MAX_SCAN_RESULTS,
        )?;

        Ok(Self {
            redis_url: redis_url.to_owned(),
            use_tls,
            database: connection.redis.db,
            has_auth: connection.redis.username.is_some() || connection.redis.password.is_some(),
            key_namespace: key_namespace.to_owned(),
            default_ttl: Duration::from_secs(default_ttl_secs),
            pool_size,
            connection_timeout: Duration::from_secs(connection_timeout_secs),
            operation_timeout: Duration::from_secs(operation_timeout_secs),
            scan_page_size,
            max_scan_results,
            additional_ca_pem: None,
        })
    }

    /// Adds PEM encoded trust anchors for a verified `rediss://` endpoint.
    /// Public/system roots remain the default when this is not configured.
    pub fn additional_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Result<Self, CacheError> {
        if !self.use_tls {
            return Err(CacheError::InvalidConfiguration(
                "additional Redis CA requires use_tls=true and a rediss:// URL".into(),
            ));
        }
        let pem = pem.into();
        if pem.is_empty() || pem.len() > 1024 * 1024 {
            return Err(CacheError::InvalidConfiguration(
                "additional Redis CA bundle is outside the supported size bound".into(),
            ));
        }
        let mut certificates = 0_usize;
        use rustls_pki_types::pem::{PemObject as _, SectionKind};

        for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(&pem) {
            match item.map_err(|_| {
                CacheError::InvalidConfiguration(
                    "additional Redis CA bundle could not be parsed".into(),
                )
            })? {
                (SectionKind::Certificate, _) => certificates += 1,
                _ => {
                    return Err(CacheError::InvalidConfiguration(
                        "additional Redis CA bundle contains non-certificate material".into(),
                    ));
                }
            }
            if certificates > 32 {
                return Err(CacheError::InvalidConfiguration(
                    "additional Redis CA bundle contains too many certificates".into(),
                ));
            }
        }
        if certificates == 0 {
            return Err(CacheError::InvalidConfiguration(
                "additional Redis CA bundle contains no certificates".into(),
            ));
        }
        self.additional_ca_pem = Some(pem);
        Ok(self)
    }

    fn with_additional_ca_bundle(self, path: Option<&Path>) -> Result<Self, CacheError> {
        let Some(path) = path else {
            return Ok(self);
        };
        if !path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(CacheError::InvalidConfiguration(
                "additional Redis CA bundle path must be absolute and normalized".into(),
            ));
        }
        let metadata = std::fs::symlink_metadata(path).map_err(|_| {
            CacheError::InvalidConfiguration(
                "additional Redis CA bundle path could not be inspected".into(),
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(CacheError::InvalidConfiguration(
                "additional Redis CA bundle must be a regular non-symlink file".into(),
            ));
        }
        if metadata.len() == 0 || metadata.len() > 1024 * 1024 {
            return Err(CacheError::InvalidConfiguration(
                "additional Redis CA bundle is outside the supported size bound".into(),
            ));
        }
        let canonical = std::fs::canonicalize(path).map_err(|_| {
            CacheError::InvalidConfiguration(
                "additional Redis CA bundle path could not be canonicalized".into(),
            )
        })?;
        if canonical != path {
            return Err(CacheError::InvalidConfiguration(
                "additional Redis CA bundle path must be canonical".into(),
            ));
        }
        let pem = std::fs::read(path).map_err(|_| {
            CacheError::InvalidConfiguration("additional Redis CA bundle could not be read".into())
        })?;
        self.additional_ca_pem(pem)
    }

    pub(crate) fn create_pool(&self) -> Result<RedisPool, CacheError> {
        let client = if let Some(root_cert) = &self.additional_ca_pem {
            redis::Client::build_with_tls(
                self.redis_url.clone(),
                redis::TlsCertificates {
                    client_tls: None,
                    root_cert: Some(root_cert.clone()),
                },
            )
            .map_err(|_| {
                CacheError::InvalidConfiguration(
                    "Redis TLS client could not load the additional CA bundle".into(),
                )
            })?
        } else {
            redis::Client::open(self.redis_url.clone()).map_err(|_| {
                CacheError::InvalidConfiguration(
                    "Redis client could not load the validated connection plan".into(),
                )
            })?
        };
        RedisPool::builder(RedisPoolManager::new(client))
            .config(PoolConfig {
                max_size: self.pool_size,
                timeouts: Timeouts {
                    wait: Some(self.connection_timeout),
                    create: Some(self.connection_timeout),
                    recycle: Some(self.connection_timeout),
                },
                ..PoolConfig::new(self.pool_size)
            })
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .map_err(|_| {
                CacheError::InvalidConfiguration(
                    "Redis pool could not be created from the validated plan".into(),
                )
            })
    }

    pub(crate) fn qualified_key(&self, key: &str) -> Result<String, CacheError> {
        if key.is_empty() || key.len() > 256 || key.contains('\0') {
            return Err(CacheError::InvalidKey(
                "key must contain 1..=256 non-NUL bytes".into(),
            ));
        }
        Ok(format!("{}:{key}", self.key_namespace))
    }

    pub(crate) fn qualified_pattern(&self, pattern: &str) -> Result<String, CacheError> {
        if pattern.is_empty() || pattern.len() > 256 || pattern.contains('\0') {
            return Err(CacheError::InvalidScan(
                "pattern must contain 1..=256 non-NUL bytes".into(),
            ));
        }
        Ok(format!("{}:{pattern}", self.key_namespace))
    }

    pub(crate) fn strip_namespace(&self, key: String) -> Result<String, CacheError> {
        key.strip_prefix(&format!("{}:", self.key_namespace))
            .map(ToOwned::to_owned)
            .ok_or_else(|| CacheError::Backend("Redis returned a key outside the namespace".into()))
    }

    /// Returns the configured default entry TTL.
    pub const fn default_ttl(&self) -> Duration {
        self.default_ttl
    }

    /// Returns the maximum number of concurrent pooled Redis connections.
    pub const fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Returns the deadline applied by default cache operations.
    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Returns the connection acquisition and establishment timeout.
    pub const fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    /// Returns the configured maximum `SCAN` page size.
    pub const fn scan_page_size(&self) -> usize {
        self.scan_page_size
    }

    /// Returns the maximum number of keys accepted from one `SCAN` response.
    pub const fn max_scan_results(&self) -> usize {
        self.max_scan_results
    }
}

impl fmt::Debug for RedisCachePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisCachePlan")
            .field("redis_url", &"<redacted>")
            .field("use_tls", &self.use_tls)
            .field("database", &self.database)
            .field("has_auth", &self.has_auth)
            .field("key_namespace", &self.key_namespace)
            .field("default_ttl", &self.default_ttl)
            .field("pool_size", &self.pool_size)
            .field("connection_timeout", &self.connection_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("scan_page_size", &self.scan_page_size)
            .field("max_scan_results", &self.max_scan_results)
            .field(
                "additional_ca_pem",
                &self.additional_ca_pem.as_ref().map(|_| "<configured>"),
            )
            .finish()
    }
}

fn bounded_u64(name: &str, value: u64, min: u64, max: u64) -> Result<u64, CacheError> {
    if !(min..=max).contains(&value) {
        return Err(CacheError::InvalidConfiguration(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
}

fn bounded_usize(name: &str, value: usize, min: usize, max: usize) -> Result<usize, CacheError> {
    if !(min..=max).contains(&value) {
        return Err(CacheError::InvalidConfiguration(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &str, use_tls: bool) -> CacheConfig {
        CacheConfig {
            redis_url: Some(url.into()),
            use_tls: Some(use_tls),
            ..CacheConfig::default()
        }
    }

    #[test]
    fn url_auth_database_tls_and_redaction_are_validated() {
        let plan = RedisCachePlan::from_single(&config(
            "rediss://alice:secret@cache.example:6380/7",
            true,
        ))
        .unwrap();
        let debug = format!("{plan:?}");
        assert!(debug.contains("database: 7"));
        assert!(debug.contains("has_auth: true"));
        assert!(!debug.contains("alice"));
        assert!(!debug.contains("secret"));
        assert!(
            RedisCachePlan::from_single(&config("rediss://cache.example:6380/0/#insecure", true))
                .is_err()
        );
    }

    #[test]
    fn transport_mismatch_and_unbounded_values_fail_before_io() {
        assert!(RedisCachePlan::from_single(&config("redis://localhost:6379/0", true)).is_err());
        assert!(RedisCachePlan::from_single(&config("rediss://localhost:6380/0", false)).is_err());
        let mut invalid = config("redis://localhost:6379/0", false);
        invalid.pool_size = Some(0);
        assert!(RedisCachePlan::from_single(&invalid).is_err());
    }

    #[test]
    fn key_namespace_is_applied_and_stripped() {
        let plan = RedisCachePlan::from_single(&config("redis://localhost:6379/0", false)).unwrap();
        assert_eq!(plan.qualified_key("session:1").unwrap(), "lily:session:1");
        assert_eq!(
            plan.strip_namespace("lily:session:1".into()).unwrap(),
            "session:1"
        );
    }
}
