//! Application-owned ClickHouse connection and bounded operation surface.

use std::sync::{Arc, RwLock};
use std::time::Duration;

#[cfg(feature = "single")]
use async_trait::async_trait;
use clickhouse::{Client, Row};
#[cfg(feature = "single")]
use lily_config::ConfigService;
#[cfg(feature = "single")]
use lily_error::injection::InjectionError;
#[cfg(feature = "single")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "single")]
use lily_injection::ServiceTrait;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::operation::MAX_CLICKHOUSE_WRITE_BATCH_SIZE;
use crate::options::{quote_column_identifier, quote_identifier};
use crate::{
    ClickhouseClientPlan, ClickhouseError, ClickhouseOperationContext, ClickhousePageRequest,
    ClickhouseSelectPlan,
};

#[derive(Default)]
struct DatabaseState {
    client: RwLock<Option<Client>>,
    database: RwLock<Option<String>>,
    query_timeout: RwLock<Option<Duration>>,
    permits: RwLock<Option<Arc<Semaphore>>>,
}

/// A ClickHouse client handle whose usable state is owned by an application lifecycle.
///
/// `Default` is deliberately uninitialized so dependency construction cannot
/// accidentally contact localhost. DI initialization or [`Self::connect`]
/// publishes a client only after a readiness query succeeds. Direct query
/// methods are first-class application API; a generated table is optional.
#[cfg_attr(feature = "single", derive(Injectable))]
#[cfg_attr(feature = "single", service(lifetime = "Singleton"))]
#[derive(Clone, Default)]
pub struct DatabaseService {
    #[cfg(feature = "single")]
    #[inject]
    config_service: Arc<ConfigService>,
    state: Arc<DatabaseState>,
}

impl DatabaseService {
    /// Connects a standalone database service from a validated client plan.
    ///
    /// The caller owns the returned service and must eventually call
    /// [`Self::close`] or register a [`crate::ClickhouseShutdownHandle`].
    #[lily_trace::lily_trace(name = "clickhouse.database.connect")]
    pub async fn connect(plan: ClickhouseClientPlan) -> Result<Self, ClickhouseError> {
        let service = Self::default();
        service.install_ready(plan).await?;
        Ok(service)
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.readiness")]
    async fn install_ready(&self, plan: ClickhouseClientPlan) -> Result<(), ClickhouseError> {
        let client = plan.client()?;
        let operation =
            ClickhouseOperationContext::new(CancellationToken::new(), plan.query_timeout())?;
        operation
            .execute(async {
                client
                    .query("SELECT 1")
                    .execute()
                    .await
                    .map_err(|error| ClickhouseError::ConnectionError(error.to_string()))
            })
            .await?;

        *self
            .state
            .client
            .write()
            .map_err(|_| lock_error("client"))? = Some(client);
        *self
            .state
            .database
            .write()
            .map_err(|_| lock_error("database"))? = Some(plan.database().to_owned());
        *self
            .state
            .query_timeout
            .write()
            .map_err(|_| lock_error("timeout"))? = Some(plan.query_timeout());
        *self
            .state
            .permits
            .write()
            .map_err(|_| lock_error("permits"))? = Some(Arc::new(Semaphore::new(plan.pool_size())));
        Ok(())
    }

    fn client(&self) -> Result<Client, ClickhouseError> {
        self.state
            .client
            .read()
            .map_err(|_| lock_error("client"))?
            .clone()
            .ok_or(ClickhouseError::NotInitialized)
    }

    /// Returns the configured database name, or `NotInitialized` before readiness.
    pub fn database_name(&self) -> Result<String, ClickhouseError> {
        self.state
            .database
            .read()
            .map_err(|_| lock_error("database"))?
            .clone()
            .ok_or(ClickhouseError::NotInitialized)
    }

    /// Creates an operation context carrying caller cancellation and the configured timeout.
    pub fn operation_context(
        &self,
        cancellation: CancellationToken,
    ) -> Result<ClickhouseOperationContext, ClickhouseError> {
        let timeout = self
            .state
            .query_timeout
            .read()
            .map_err(|_| lock_error("timeout"))?
            .ok_or(ClickhouseError::NotInitialized)?;
        ClickhouseOperationContext::new(cancellation, timeout)
    }

    /// Creates a deadline-bounded context for callers that do not yet carry a
    /// request cancellation token. Request-facing code should prefer
    /// `operation_context` and propagate its own token.
    pub fn bounded_operation(&self) -> Result<ClickhouseOperationContext, ClickhouseError> {
        self.operation_context(CancellationToken::new())
    }
    #[lily_trace::lily_trace(name = "clickhouse.database.insert")]
    /// Inserts one row into a validated table identifier.
    pub async fn insert_one<T>(
        &self,
        table: &str,
        entity: &T,
        operation: &ClickhouseOperationContext,
    ) -> Result<(), ClickhouseError>
    where
        T: Row + Serialize,
    {
        self.insert_many(table, std::slice::from_ref(entity), operation)
            .await
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.insert.many")]
    /// Inserts a bounded batch of rows into a validated table identifier.
    pub async fn insert_many<T>(
        &self,
        table: &str,
        entities: &[T],
        operation: &ClickhouseOperationContext,
    ) -> Result<(), ClickhouseError>
    where
        T: Row + Serialize,
    {
        if entities.len() > MAX_CLICKHOUSE_WRITE_BATCH_SIZE {
            return Err(ClickhouseError::InvalidPage(format!(
                "write batch cannot exceed {MAX_CLICKHOUSE_WRITE_BATCH_SIZE} rows"
            )));
        }
        if entities.is_empty() {
            return Ok(());
        }

        let (client, _permit) = self.client_for_operation(operation).await?;
        let table = quote_identifier(table, "table")?;
        let timeout = self.query_timeout()?;
        operation
            .execute(async move {
                let mut insert = client
                    .insert(&table)
                    .map_err(|error| ClickhouseError::QueryError(error.to_string()))?
                    .with_timeouts(Some(timeout), Some(timeout));
                for entity in entities {
                    insert
                        .write(entity)
                        .await
                        .map_err(|error| ClickhouseError::QueryError(error.to_string()))?;
                }
                insert
                    .end()
                    .await
                    .map_err(|error| ClickhouseError::QueryError(error.to_string()))
            })
            .await
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.find")]
    /// Selects one bounded page by equality on a generated allowlisted column.
    pub async fn find_equal<T, V>(
        &self,
        table: &str,
        allowed_columns: &[&str],
        column: &str,
        value: V,
        page: ClickhousePageRequest,
        operation: &ClickhouseOperationContext,
    ) -> Result<Vec<T>, ClickhouseError>
    where
        T: Row + DeserializeOwned,
        V: Serialize,
    {
        let table = quote_identifier(table, "table")?;
        validate_allowed_column(allowed_columns, column)?;
        let column = quote_column_identifier(column)?;
        let (client, _permit) = self.client_for_operation(operation).await?;
        let query = format!("SELECT ?fields FROM {table} WHERE {column} = ? LIMIT ? OFFSET ?");
        operation
            .execute(async move {
                client
                    .query(&query)
                    .bind(value)
                    .bind(page.limit())
                    .bind(page.offset())
                    .fetch_all::<T>()
                    .await
                    .map_err(ClickhouseError::from)
            })
            .await
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.select")]
    /// Executes a validated, bound and bounded select plan.
    pub async fn select<T>(
        &self,
        table: &str,
        allowed_columns: &[&str],
        plan: &ClickhouseSelectPlan,
        operation: &ClickhouseOperationContext,
    ) -> Result<Vec<T>, ClickhouseError>
    where
        T: Row + DeserializeOwned,
    {
        if plan.has_empty_set() {
            return Ok(Vec::new());
        }
        let statement = plan.statement(table, allowed_columns)?;
        let (client, _permit) = self.client_for_operation(operation).await?;
        operation
            .execute(async {
                let mut query = client.query(&statement);
                for predicate in &plan.predicates {
                    if let Some(map_key) = &predicate.map_key {
                        query = query.bind(map_key);
                    }
                    query = match &predicate.value {
                        crate::ClickhouseValue::String(value) => query.bind(value),
                        crate::ClickhouseValue::I64(value) => query.bind(*value),
                        crate::ClickhouseValue::U64(value) => query.bind(*value),
                        crate::ClickhouseValue::U8(value) => query.bind(*value),
                        crate::ClickhouseValue::Strings(value) => query.bind(value),
                    };
                }
                query
                    .bind(plan.page.limit())
                    .bind(plan.page.offset())
                    .fetch_all::<T>()
                    .await
                    .map_err(ClickhouseError::from)
            })
            .await
    }

    /// Selects at most one row by equality on a generated allowlisted column.
    pub async fn find_one_equal<T, V>(
        &self,
        table: &str,
        allowed_columns: &[&str],
        column: &str,
        value: V,
        operation: &ClickhouseOperationContext,
    ) -> Result<Option<T>, ClickhouseError>
    where
        T: Row + DeserializeOwned,
        V: Serialize,
    {
        let page = ClickhousePageRequest::new(1, 0)?;
        Ok(self
            .find_equal(table, allowed_columns, column, value, page, operation)
            .await?
            .into_iter()
            .next())
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.count")]
    /// Counts all rows in a validated table identifier.
    pub async fn count(
        &self,
        table: &str,
        operation: &ClickhouseOperationContext,
    ) -> Result<u64, ClickhouseError> {
        let table = quote_identifier(table, "table")?;
        let (client, _permit) = self.client_for_operation(operation).await?;
        operation
            .execute(async move {
                client
                    .query(&format!("SELECT count() FROM {table}"))
                    .fetch_one::<u64>()
                    .await
                    .map_err(ClickhouseError::from)
            })
            .await
    }

    /// Issues an asynchronous ClickHouse mutation. Completion means the server
    /// accepted the mutation command, not that all parts have already merged.
    #[lily_trace::lily_trace(name = "clickhouse.database.delete")]
    pub async fn delete_equal<V>(
        &self,
        table: &str,
        allowed_columns: &[&str],
        column: &str,
        value: V,
        operation: &ClickhouseOperationContext,
    ) -> Result<(), ClickhouseError>
    where
        V: Serialize,
    {
        let table = quote_identifier(table, "table")?;
        validate_allowed_column(allowed_columns, column)?;
        let column = quote_column_identifier(column)?;
        let (client, _permit) = self.client_for_operation(operation).await?;
        let query = format!("ALTER TABLE {table} DELETE WHERE {column} = ?");
        operation
            .execute(async move {
                client
                    .query(&query)
                    .bind(value)
                    .execute()
                    .await
                    .map_err(ClickhouseError::from)
            })
            .await
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.migration.execute")]
    pub(crate) async fn execute_migration(
        &self,
        statement: &str,
        operation: &ClickhouseOperationContext,
    ) -> Result<(), ClickhouseError> {
        let (client, _permit) = self.client_for_operation(operation).await?;
        operation
            .execute(async move {
                client
                    .query(statement)
                    .execute()
                    .await
                    .map_err(ClickhouseError::from)
            })
            .await
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.migration.fetch")]
    pub(crate) async fn fetch_migration_rows<T>(
        &self,
        statement: &str,
        operation: &ClickhouseOperationContext,
    ) -> Result<Vec<T>, ClickhouseError>
    where
        T: Row + DeserializeOwned,
    {
        let (client, _permit) = self.client_for_operation(operation).await?;
        operation
            .execute(async move {
                client
                    .query(statement)
                    .fetch_all::<T>()
                    .await
                    .map_err(ClickhouseError::from)
            })
            .await
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.migration.record")]
    pub(crate) async fn record_migration(
        &self,
        version: i64,
        name: &str,
        checksum: &str,
        operation: &ClickhouseOperationContext,
    ) -> Result<(), ClickhouseError> {
        let (client, _permit) = self.client_for_operation(operation).await?;
        operation
            .execute(async move {
                client
                    .query(
                        "INSERT INTO `_lily_schema_migrations` \
                         (version, name, checksum, applied_at) \
                         VALUES (?, ?, ?, now64(3))",
                    )
                    .bind(version)
                    .bind(name)
                    .bind(checksum)
                    .execute()
                    .await
                    .map_err(ClickhouseError::from)
            })
            .await
    }

    fn query_timeout(&self) -> Result<Duration, ClickhouseError> {
        self.state
            .query_timeout
            .read()
            .map_err(|_| lock_error("timeout"))?
            .ok_or(ClickhouseError::NotInitialized)
    }

    #[lily_trace::lily_trace(name = "clickhouse.database.acquire")]
    async fn client_for_operation(
        &self,
        operation: &ClickhouseOperationContext,
    ) -> Result<(Client, OwnedSemaphorePermit), ClickhouseError> {
        let client = self.client()?;
        let permits = self
            .state
            .permits
            .read()
            .map_err(|_| lock_error("permits"))?
            .clone()
            .ok_or(ClickhouseError::NotInitialized)?;
        let permit = operation
            .execute(async {
                permits
                    .acquire_owned()
                    .await
                    .map_err(|_| ClickhouseError::NotInitialized)
            })
            .await?;
        Ok((client, permit))
    }

    pub(crate) fn clear(&self) -> Result<(), ClickhouseError> {
        *self
            .state
            .client
            .write()
            .map_err(|_| lock_error("client"))? = None;
        *self
            .state
            .database
            .write()
            .map_err(|_| lock_error("database"))? = None;
        *self
            .state
            .query_timeout
            .write()
            .map_err(|_| lock_error("timeout"))? = None;
        if let Some(permits) = self
            .state
            .permits
            .write()
            .map_err(|_| lock_error("permits"))?
            .take()
        {
            permits.close();
        }
        Ok(())
    }

    /// Idempotently closes operation admission and releases client handles.
    ///
    /// Lily DI calls this automatically. Call it directly for a service created
    /// with [`Self::connect`] or when intentionally closing a client early.
    #[lily_trace::lily_trace(name = "clickhouse.database.close")]
    pub fn close(&self) -> Result<(), ClickhouseError> {
        self.clear()
    }
}

fn validate_allowed_column(allowed: &[&str], column: &str) -> Result<(), ClickhouseError> {
    quote_column_identifier(column)?;
    if !allowed.contains(&column) {
        return Err(ClickhouseError::InvalidIdentifier(
            "column is not part of the generated schema allowlist".into(),
        ));
    }
    Ok(())
}

fn lock_error(name: &str) -> ClickhouseError {
    ClickhouseError::InternalError(format!("ClickHouse {name} state lock is poisoned"))
}

#[cfg(feature = "single")]
#[async_trait]
impl ServiceTrait for DatabaseService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        let config = self.config_service.get_lily_config().await;
        let config = config.clickhouse.as_ref().ok_or_else(|| {
            InjectionError::InitError("ClickHouse configuration is missing".into())
        })?;
        let plan = ClickhouseClientPlan::from_config(config)
            .map_err(|error| InjectionError::InitError(error.to_string()))?;
        self.install_ready(plan).await.map_err(|error| {
            tracing::error!(error = %error, "ClickHouse startup readiness probe failed");
            InjectionError::InitError("ClickHouse startup readiness probe failed".into())
        })?;
        tracing::info!(database = %self.database_name().unwrap_or_default(), "ClickHouse initialized");
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.close()
            .map_err(|error| InjectionError::General(error.to_string()))?;
        tracing::info!("ClickHouse disposed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_service_is_explicitly_uninitialized() {
        let service = DatabaseService::default();
        assert_eq!(
            service.database_name(),
            Err(ClickhouseError::NotInitialized)
        );
        assert!(matches!(
            service.operation_context(CancellationToken::new()),
            Err(ClickhouseError::NotInitialized)
        ));
    }

    #[test]
    fn generated_identifier_allowlist_is_fail_closed() {
        assert!(validate_allowed_column(&["tenant_id"], "tenant_id").is_ok());
        assert!(validate_allowed_column(&["tenant_id"], "other").is_err());
        assert!(validate_allowed_column(&["tenant_id"], "id; DROP").is_err());
    }
}
