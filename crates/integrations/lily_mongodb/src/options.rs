use std::fmt;
use std::time::Duration;

#[cfg(feature = "factory")]
use lily_config::DatabaseCellConfig;
use lily_config::DatabaseConfig;
use lily_error::application::mongodb::MongoDbError;
use mongodb::options::{ClientOptions, Credential, ServerAddress, Tls, TlsOptions};
use mongodb::{Client, bson::doc};

const DEFAULT_POOL_SIZE: usize = 10;
const MAX_POOL_SIZE: usize = 128;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone)]
enum ConnectionSource {
    Uri(String),
    Manual {
        host: String,
        port: u16,
        username: Option<String>,
        password: Option<String>,
        auth_database: Option<String>,
    },
}

/// Validated effective MongoDB connection configuration.
///
/// Its `Debug` representation never exposes the URI, username, password or
/// authentication database. Normal Lily applications do not retain this type:
/// `DatabaseService` creates the plan from `lily_config` during startup. It is
/// public so a standalone composition root can validate a `DatabaseConfig` and
/// pass the result to `DatabaseService::connect`.
#[derive(Clone)]
pub struct MongoClientPlan {
    source: ConnectionSource,
    database_name: String,
    app_name: Option<String>,
    pool_size: u32,
    connect_timeout: Duration,
    operation_timeout: Duration,
    use_tls: Option<bool>,
}

impl MongoClientPlan {
    /// Validates the single-database portion of a Lily configuration snapshot.
    ///
    /// This rejects non-MongoDB or factory-mode configurations, unsupported
    /// pool settings, incomplete credentials, invalid names and out-of-range
    /// timeouts before any network operation begins.
    pub fn from_database(config: &DatabaseConfig) -> Result<Self, MongoDbError> {
        if config.mode.as_deref().unwrap_or("single") != "single" {
            return Err(invalid(
                "DatabaseService requires database.mode = \"single\"",
            ));
        }
        if config.database_type.as_deref() != Some("mongodb") {
            return Err(invalid(
                "DatabaseService requires database_type = \"mongodb\"",
            ));
        }
        Self::build(
            config.connection_string.as_deref(),
            config.host.as_deref(),
            config.port,
            config.username.as_deref(),
            config.password.as_deref(),
            config.auth_database.as_deref(),
            config.database_name.as_deref(),
            config.app_name.as_deref(),
            config.pool_size,
            config.pooling_enabled,
            config.connection_timeout_secs,
            config.query_timeout_secs,
            config.use_tls,
        )
    }

    #[cfg(feature = "factory")]
    pub(crate) fn from_cell(config: &DatabaseCellConfig) -> Result<Self, MongoDbError> {
        if config.database_type != "mongodb" {
            return Err(invalid("MongoDB cell requires database_type = \"mongodb\""));
        }
        Self::build(
            config.connection_string.as_deref(),
            config.host.as_deref(),
            config.port,
            config.username.as_deref(),
            config.password.as_deref(),
            config.auth_database.as_deref(),
            Some(config.database_name.as_str()),
            config.app_name.as_deref(),
            config.pool_size,
            config.pooling_enabled,
            config.connection_timeout_secs,
            config.query_timeout_secs,
            config.use_tls,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        connection_string: Option<&str>,
        host: Option<&str>,
        port: Option<u16>,
        username: Option<&str>,
        password: Option<&str>,
        auth_database: Option<&str>,
        database_name: Option<&str>,
        app_name: Option<&str>,
        pool_size: Option<usize>,
        pooling_enabled: Option<bool>,
        connect_timeout_secs: Option<u64>,
        operation_timeout_secs: Option<u64>,
        use_tls: Option<bool>,
    ) -> Result<Self, MongoDbError> {
        if pooling_enabled == Some(false) {
            return Err(invalid(
                "pooling_enabled=false is unsupported by the MongoDB driver; remove the field or set it to true",
            ));
        }
        let pool_size = pool_size.unwrap_or(DEFAULT_POOL_SIZE);
        if !(1..=MAX_POOL_SIZE).contains(&pool_size) {
            return Err(invalid("pool_size must be between 1 and 128"));
        }
        let connect_timeout =
            Duration::from_secs(connect_timeout_secs.unwrap_or(DEFAULT_CONNECT_TIMEOUT.as_secs()));
        let operation_timeout = Duration::from_secs(
            operation_timeout_secs.unwrap_or(DEFAULT_OPERATION_TIMEOUT.as_secs()),
        );
        if connect_timeout.is_zero() || connect_timeout > MAX_CONNECT_TIMEOUT {
            return Err(invalid("connection_timeout_secs must be between 1 and 120"));
        }
        if operation_timeout.is_zero() || operation_timeout > MAX_OPERATION_TIMEOUT {
            return Err(invalid("query_timeout_secs must be between 1 and 300"));
        }

        let database_name = database_name
            .map(str::trim)
            .filter(|name| valid_database_name(name))
            .ok_or_else(|| invalid("database_name is missing or invalid"))?
            .to_owned();
        let app_name = app_name
            .map(str::trim)
            .filter(|name| !name.is_empty() && name.len() <= 128)
            .map(str::to_owned);

        let source = match connection_string
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(uri) => ConnectionSource::Uri(uri.to_owned()),
            None => {
                if username.is_some() != password.is_some() {
                    return Err(invalid("username and password must be configured together"));
                }
                if auth_database.is_some() && username.is_none() {
                    return Err(invalid("auth_database requires username and password"));
                }
                let host = host.unwrap_or("localhost").trim();
                if host.is_empty() || host.chars().any(char::is_control) {
                    return Err(invalid("MongoDB host is invalid"));
                }
                ConnectionSource::Manual {
                    host: host.to_owned(),
                    port: port.unwrap_or(27017),
                    username: username.map(str::to_owned),
                    password: password.map(str::to_owned),
                    auth_database: auth_database.map(str::to_owned),
                }
            }
        };

        Ok(Self {
            source,
            database_name,
            app_name,
            pool_size: u32::try_from(pool_size).expect("validated pool size fits u32"),
            connect_timeout,
            operation_timeout,
            use_tls,
        })
    }

    pub(crate) fn database_name(&self) -> &str {
        &self.database_name
    }

    pub(crate) const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    async fn client_options(&self) -> Result<ClientOptions, MongoDbError> {
        let mut options = match &self.source {
            ConnectionSource::Uri(uri) => ClientOptions::parse(uri)
                .await
                .map_err(|_| invalid("MongoDB connection string could not be parsed"))?,
            ConnectionSource::Manual {
                host,
                port,
                username,
                password,
                auth_database,
            } => {
                let credential = username.as_ref().map(|username| {
                    let mut credential = Credential::builder()
                        .username(username.clone())
                        .password(password.clone())
                        .build();
                    credential.source.clone_from(auth_database);
                    credential
                });
                let host = if host.contains(':') && !host.starts_with('[') {
                    format!("[{host}]")
                } else {
                    host.clone()
                };
                let address = format!("{host}:{port}");
                let server = ServerAddress::parse(address)
                    .map_err(|_| invalid("MongoDB host or port could not be parsed"))?;
                let mut options = ClientOptions::default();
                options.hosts = vec![server];
                options.credential = credential;
                options
            }
        };

        options.app_name.clone_from(&self.app_name);
        options.max_pool_size = Some(self.pool_size);
        options.connect_timeout = Some(self.connect_timeout);
        options.server_selection_timeout = Some(self.connect_timeout);
        options.retry_reads = Some(true);
        options.retry_writes = Some(true);
        if let Some(use_tls) = self.use_tls {
            options.tls = Some(if use_tls {
                Tls::Enabled(TlsOptions::default())
            } else {
                Tls::Disabled
            });
        }
        Ok(options)
    }

    pub(crate) async fn connect_and_ping(&self) -> Result<Client, MongoDbError> {
        let client = Client::with_options(self.client_options().await?)
            .map_err(|_| invalid("MongoDB client options are invalid"))?;
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(MongoDbError::from)?;
        Ok(client)
    }
}

impl fmt::Debug for MongoClientPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoClientPlan")
            .field("connection", &"<redacted>")
            .field("database_name", &self.database_name)
            .field("app_name", &self.app_name)
            .field("pool_size", &self.pool_size)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("use_tls", &self.use_tls)
            .finish()
    }
}

fn valid_database_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.chars().any(|character| {
            matches!(
                character,
                '/' | '\\' | '.' | ' ' | '"' | '$' | '*' | '<' | '>' | ':' | '|' | '?'
            ) || character.is_control()
        })
}

fn invalid(message: &str) -> MongoDbError {
    MongoDbError::InvalidConfiguration(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_config() -> DatabaseConfig {
        DatabaseConfig {
            mode: Some("single".into()),
            database_type: Some("mongodb".into()),
            connection_string: None,
            pool_size: Some(17),
            connection_timeout_secs: Some(3),
            query_timeout_secs: Some(4),
            pooling_enabled: Some(true),
            database_name: Some("orders".into()),
            host: Some("localhost".into()),
            port: Some(27017),
            username: Some("reader".into()),
            password: Some("secret".into()),
            auth_database: Some("admin".into()),
            use_tls: Some(true),
            app_name: Some("orders-api".into()),
            cells: None,
        }
    }

    #[tokio::test]
    async fn manual_credentials_and_runtime_options_are_applied_without_uri_interpolation() {
        let plan = MongoClientPlan::from_database(&database_config()).unwrap();
        assert!(!format!("{plan:?}").contains("secret"));
        let options = plan.client_options().await.unwrap();
        assert_eq!(options.max_pool_size, Some(17));
        assert_eq!(options.connect_timeout, Some(Duration::from_secs(3)));
        assert_eq!(
            options.server_selection_timeout,
            Some(Duration::from_secs(3))
        );
        assert_eq!(options.retry_reads, Some(true));
        assert_eq!(options.retry_writes, Some(true));
        assert_eq!(
            options.hosts,
            vec![ServerAddress::Tcp {
                host: "localhost".to_string(),
                port: Some(27017),
            }]
        );
        assert!(matches!(options.tls, Some(Tls::Enabled(_))));
        let credential = options.credential.expect("typed credential");
        assert_eq!(credential.username.as_deref(), Some("reader"));
        assert_eq!(credential.source.as_deref(), Some("admin"));
    }

    #[test]
    fn unsupported_or_unbounded_config_is_rejected() {
        let mut config = database_config();
        config.pooling_enabled = Some(false);
        assert!(MongoClientPlan::from_database(&config).is_err());
        config.pooling_enabled = Some(true);
        config.pool_size = Some(MAX_POOL_SIZE + 1);
        assert!(MongoClientPlan::from_database(&config).is_err());
        config.pool_size = Some(1);
        config.query_timeout_secs = Some(0);
        assert!(MongoClientPlan::from_database(&config).is_err());
        config.query_timeout_secs = Some(1);
        config.password = None;
        assert!(MongoClientPlan::from_database(&config).is_err());
    }
}
