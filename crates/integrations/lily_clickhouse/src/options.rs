use std::fmt;
use std::time::Duration;

use clickhouse::{Client, Compression};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::TokioExecutor,
};
use lily_config::{ClickhouseCellConfig, ClickhouseConfig};

use crate::ClickhouseError;

const MAX_POOL_SIZE: usize = 128;
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_QUERY_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone)]
/// Validated and secret-redacting connection plan for one ClickHouse client.
///
/// Plan construction performs no network I/O. [`crate::DatabaseService::connect`]
/// consumes the plan and publishes a client only after readiness succeeds.
pub struct ClickhouseClientPlan {
    host: String,
    port: u16,
    database: String,
    username: Option<String>,
    password: Option<String>,
    pool_size: usize,
    connect_timeout: Duration,
    query_timeout: Duration,
    use_tls: bool,
    compression_enabled: bool,
}

impl ClickhouseClientPlan {
    /// Builds the default single-service plan from `[clickhouse]` configuration.
    pub fn from_config(config: &ClickhouseConfig) -> Result<Self, ClickhouseError> {
        if config.mode.as_deref().unwrap_or("single") != "single" {
            return Err(invalid(
                "DatabaseService requires clickhouse.mode = \"single\"",
            ));
        }
        Self::build(
            config.host.as_deref().unwrap_or("localhost"),
            config.port,
            config.database.as_deref().unwrap_or("default"),
            config.username.as_deref(),
            config.password.as_deref(),
            config.pool_size,
            config.connection_timeout_secs,
            config.query_timeout_secs,
            config.use_tls,
            config.compression_enabled,
        )
    }

    /// Builds a plan for one named factory cell.
    pub fn from_cell(config: &ClickhouseCellConfig) -> Result<Self, ClickhouseError> {
        Self::build(
            &config.host,
            config.port,
            &config.database,
            config.username.as_deref(),
            config.password.as_deref(),
            config.pool_size,
            config.connection_timeout_secs,
            config.query_timeout_secs,
            config.use_tls,
            config.compression_enabled,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        host: &str,
        port: Option<u16>,
        database: &str,
        username: Option<&str>,
        password: Option<&str>,
        pool_size: Option<usize>,
        connect_timeout_secs: Option<u64>,
        query_timeout_secs: Option<u64>,
        use_tls: Option<bool>,
        compression_enabled: Option<bool>,
    ) -> Result<Self, ClickhouseError> {
        let host = host.trim();
        if host.is_empty()
            || host.contains('/')
            || host.contains('@')
            || host.chars().any(char::is_control)
        {
            return Err(invalid("host is invalid"));
        }
        validate_identifier(database, "database")?;
        let pool_size = pool_size.unwrap_or(10);
        if pool_size == 0 || pool_size > MAX_POOL_SIZE {
            return Err(invalid("pool_size must be between 1 and 128"));
        }
        let connect_timeout = Duration::from_secs(connect_timeout_secs.unwrap_or(30));
        if connect_timeout.is_zero() || connect_timeout > MAX_CONNECT_TIMEOUT {
            return Err(invalid("connection_timeout_secs must be between 1 and 120"));
        }
        let query_timeout = Duration::from_secs(query_timeout_secs.unwrap_or(30));
        if query_timeout.is_zero() || query_timeout > MAX_QUERY_TIMEOUT {
            return Err(invalid("query_timeout_secs must be between 1 and 300"));
        }
        if username.is_some() != password.is_some() {
            return Err(invalid("username and password must be configured together"));
        }
        Ok(Self {
            host: host.to_owned(),
            port: port.unwrap_or(if use_tls == Some(true) { 8443 } else { 8123 }),
            database: database.to_owned(),
            username: username.map(str::to_owned),
            password: password.map(str::to_owned),
            pool_size,
            connect_timeout,
            query_timeout,
            use_tls: use_tls.unwrap_or(false),
            compression_enabled: compression_enabled.unwrap_or(true),
        })
    }

    pub(crate) fn client(&self) -> Result<Client, ClickhouseError> {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(self.connect_timeout));
        let client = if self.use_tls {
            connector.enforce_http(false);
            let connector = HttpsConnectorBuilder::new()
                .with_provider_and_webpki_roots(rustls::crypto::ring::default_provider())
                .map_err(|_| invalid("TLS protocol configuration is unavailable"))?
                .https_or_http()
                .enable_http1()
                .wrap_connector(connector);
            let client = HyperClient::builder(TokioExecutor::new())
                .pool_max_idle_per_host(self.pool_size)
                .build(connector);
            Client::with_http_client(client)
        } else {
            connector.enforce_http(true);
            let client = HyperClient::builder(TokioExecutor::new())
                .pool_max_idle_per_host(self.pool_size)
                .build(connector);
            Client::with_http_client(client)
        };
        let host = if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let mut client = client
            .with_url(format!(
                "{}://{host}:{}",
                if self.use_tls { "https" } else { "http" },
                self.port
            ))
            .with_database(&self.database)
            .with_compression(if self.compression_enabled {
                Compression::Lz4
            } else {
                Compression::None
            })
            .with_option(
                "max_execution_time",
                self.query_timeout.as_secs().to_string(),
            );
        if let Some(username) = &self.username {
            client = client.with_user(username);
        }
        if let Some(password) = &self.password {
            client = client.with_password(password);
        }
        Ok(client)
    }

    /// Returns the validated database identifier.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Returns the deadline applied to each query operation.
    pub const fn query_timeout(&self) -> Duration {
        self.query_timeout
    }

    /// Returns the maximum concurrent operations admitted by the service.
    pub const fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Reports whether HTTPS transport is configured.
    pub const fn uses_tls(&self) -> bool {
        self.use_tls
    }
}

impl fmt::Debug for ClickhouseClientPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickhouseClientPlan")
            .field("endpoint", &"<redacted>")
            .field("database", &self.database)
            .field("credentials", &"<redacted>")
            .field("pool_size", &self.pool_size)
            .field("connect_timeout", &self.connect_timeout)
            .field("query_timeout", &self.query_timeout)
            .field("use_tls", &self.use_tls)
            .field("compression_enabled", &self.compression_enabled)
            .finish()
    }
}

pub(crate) fn validate_identifier(value: &str, kind: &str) -> Result<(), ClickhouseError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || value.as_bytes()[0].is_ascii_digit()
    {
        return Err(ClickhouseError::InvalidIdentifier(format!(
            "{kind} must be an ASCII identifier starting with a letter or underscore"
        )));
    }
    Ok(())
}

pub(crate) fn quote_identifier(value: &str, kind: &str) -> Result<String, ClickhouseError> {
    validate_identifier(value, kind)?;
    Ok(format!("`{value}`"))
}

pub(crate) fn quote_column_identifier(value: &str) -> Result<String, ClickhouseError> {
    if value.is_empty()
        || value.len() > 128
        || value.split('.').any(|segment| {
            segment.is_empty()
                || segment.as_bytes()[0].is_ascii_digit()
                || !segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
    {
        return Err(ClickhouseError::InvalidIdentifier(
            "column must be a safe ASCII identifier".into(),
        ));
    }
    Ok(format!("`{value}`"))
}

fn invalid(message: &str) -> ClickhouseError {
    ClickhouseError::InvalidConfiguration(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_is_bounded_tls_explicit_and_redacted() {
        let config = ClickhouseConfig {
            mode: Some("single".into()),
            host: Some("analytics.internal".into()),
            port: Some(8443),
            database: Some("analytics".into()),
            username: Some("reader".into()),
            password: Some("secret".into()),
            pool_size: Some(12),
            connection_timeout_secs: Some(4),
            query_timeout_secs: Some(8),
            use_tls: Some(true),
            compression_enabled: Some(true),
            cells: None,
        };
        let plan = ClickhouseClientPlan::from_config(&config).unwrap();
        assert!(plan.uses_tls());
        assert_eq!(plan.query_timeout(), Duration::from_secs(8));
        assert!(!format!("{plan:?}").contains("secret"));
    }

    #[test]
    fn invalid_identifier_and_bounds_are_rejected() {
        assert!(validate_identifier("events; DROP", "table").is_err());
        let mut config = ClickhouseConfig {
            pool_size: Some(0),
            ..ClickhouseConfig::default()
        };
        assert!(ClickhouseClientPlan::from_config(&config).is_err());
        config.pool_size = Some(1);
        config.query_timeout_secs = Some(301);
        assert!(ClickhouseClientPlan::from_config(&config).is_err());
    }
}
