use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

#[cfg(feature = "single")]
use async_trait::async_trait;
#[cfg(any(feature = "single", feature = "factory"))]
use deadpool::Runtime;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncConnection, AsyncPgConnection};
#[cfg(feature = "single")]
use lily_config::ConfigService;
#[cfg(feature = "single")]
use lily_error::injection::InjectionError;
#[cfg(feature = "single")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "single")]
use lily_injection::ServiceTrait;
#[cfg(any(feature = "single", feature = "factory"))]
use tokio::sync::Mutex;
use tokio::sync::Notify;

use crate::PgTransaction;
#[cfg(any(feature = "single", feature = "factory"))]
use crate::plan::PgConnectionPlan;
#[cfg(any(feature = "single", feature = "factory"))]
use crate::tls::connection_manager;
use crate::transaction::{
    CancelTransactionOnDrop, drive_transaction, observe_transaction_finalization,
};
use crate::{ExecutionCancellation, PgError, PgResult};

pub(crate) mod lease;
pub use lease::PgConnectionLease;

/// Boxed future returned by a scoped PostgreSQL connection callback.
///
/// Callers normally create this value with `Box::pin(async move { ... })`; an
/// explicit type annotation is rarely needed. The lifetime prevents the
/// borrowed connection from escaping its [`PgDatabaseService`] operation.
/// The error defaults to [`PgError`]; [`crate::PgDbContext::with_connection`]
/// also accepts application error types.
pub type PgConnectionFuture<'connection, T, E = PgError> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'connection>>;

type PgPool = Pool<AsyncPgConnection>;

/// Point-in-time operational state of a PostgreSQL connection pool.
///
/// Values are observations rather than reservations and may change
/// immediately after [`PgDatabaseService::status`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgPoolStatus {
    /// Configured maximum number of pooled connections.
    pub max_size: usize,
    /// Number of connection slots currently managed by the pool.
    pub size: usize,
    /// Number of immediately available idle connections.
    pub available: usize,
    /// Number of callers currently waiting for a connection.
    pub waiting: usize,
    /// Number of scoped Lily operations that have entered the database
    /// boundary and have not yet completed or been dropped.
    pub in_flight: usize,
    /// Whether the pool rejects new acquisitions.
    pub closed: bool,
}

#[derive(Default)]
struct OperationTracker {
    accepting: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
}

impl OperationTracker {
    #[cfg(any(feature = "single", feature = "factory"))]
    fn open(&self) {
        self.accepting.store(true, Ordering::Release);
    }

    fn begin(self: &Arc<Self>) -> PgResult<OperationGuard> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(PgError::PoolClosed);
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        if !self.accepting.load(Ordering::Acquire) {
            self.finish();
            return Err(PgError::PoolClosed);
        }
        Ok(OperationGuard {
            tracker: Arc::clone(self),
        })
    }

    #[cfg(any(feature = "single", feature = "factory"))]
    fn close_admission(&self) {
        self.accepting.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn finish(&self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.changed.notify_waiters();
        }
    }

    #[cfg(any(feature = "single", feature = "factory"))]
    async fn wait_until_idle(&self, timeout: Duration) -> PgResult<()> {
        let wait = async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.active.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.as_mut().await;
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| PgError::ShutdownTimeout {
                remaining: self.active.load(Ordering::Acquire),
            })
    }
}

struct OperationGuard {
    tracker: Arc<OperationTracker>,
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.tracker.finish();
    }
}

struct DatabaseRuntime {
    pool: PgPool,
    cancellation_tls: Option<rustls::ClientConfig>,
    cancel_request_timeout: Duration,
    transaction_cleanup_timeout: Duration,
    #[cfg(any(feature = "single", feature = "factory"))]
    shutdown_timeout: Duration,
    operations: Arc<OperationTracker>,
    #[cfg(any(feature = "single", feature = "factory"))]
    close_gate: Mutex<()>,
}

impl DatabaseRuntime {
    async fn cancel_query(&self, token: tokio_postgres::CancelToken) {
        let request = async {
            match &self.cancellation_tls {
                Some(tls) => {
                    token
                        .cancel_query(tokio_postgres_rustls::MakeRustlsConnect::new(tls.clone()))
                        .await
                }
                None => token.cancel_query(tokio_postgres::NoTls).await,
            }
        };
        // A failed cancel request does not prevent Diesel from trying rollback.
        // Reserve at least half of the cleanup budget for that attempt.
        if !matches!(
            tokio::time::timeout(self.cancel_request_timeout, request).await,
            Ok(Ok(()))
        ) {
            tracing::debug!("PostgreSQL query cancellation request failed; awaiting rollback");
        }
    }
}

#[derive(Default)]
struct DatabaseState {
    runtime: OnceLock<Arc<DatabaseRuntime>>,
}

/// Application-owned asynchronous Diesel PostgreSQL service.
///
/// The raw pool is intentionally private. All access is scoped through
/// [`Self::with_connection`], [`Self::transaction`] or an owned
/// [`Self::acquire_connection`] lease, keeping shutdown accounting attached to
/// the connection's actual lifetime. With feature `single`, Lily's DI
/// container owns this singleton. With feature `factory`, `PgFactory` owns the
/// named service instances and callers obtain them through the factory.
///
/// `Default` only exists for DI construction. A manually default-constructed
/// value is uninitialized and returns [`PgError::NotInitialized`].
#[cfg_attr(feature = "single", derive(Injectable))]
#[cfg_attr(feature = "single", service(lifetime = "Singleton"))]
#[derive(Default)]
pub struct PgDatabaseService {
    #[cfg(feature = "single")]
    #[inject]
    config_service: Arc<ConfigService>,
    state: Arc<DatabaseState>,
    #[cfg(all(feature = "test-support", feature = "single"))]
    test_container_fixture: bool,
}

impl PgDatabaseService {
    /// Builds an inert singleton seed for transport-free framework tests.
    ///
    /// This fixture opens no pool and performs no network I/O. PostgreSQL
    /// operations remain fail-closed as uninitialized; only DI initialization
    /// is bypassed so unrelated composition and lifecycle tests can preserve
    /// the production registration graph.
    #[cfg(all(feature = "test-support", feature = "single"))]
    #[doc(hidden)]
    #[must_use]
    pub fn test_container_fixture() -> Self {
        Self {
            test_container_fixture: true,
            ..Self::default()
        }
    }

    #[cfg(feature = "factory")]
    pub(crate) async fn connect(plan: PgConnectionPlan) -> PgResult<Self> {
        let service = Self::default();
        service.install_ready(plan).await?;
        Ok(service)
    }

    #[cfg(any(feature = "single", feature = "factory"))]
    async fn install_ready(&self, plan: PgConnectionPlan) -> PgResult<()> {
        if self.state.runtime.get().is_some() {
            return Err(PgError::AlreadyInitialized);
        }
        let (manager, cancellation_tls) = connection_manager(&plan).await?;
        let pool = PgPool::builder(manager)
            .max_size(plan.max_size())
            .wait_timeout(Some(plan.acquire_timeout()))
            .create_timeout(Some(plan.connect_timeout()))
            .recycle_timeout(Some(plan.recycle_timeout()))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|_| PgError::PoolBuild)?;

        let readiness = async {
            use diesel_async::RunQueryDsl as _;

            let mut connection = pool.get().await.map_err(PgError::from)?;
            let value = tokio::time::timeout(
                plan.connect_timeout(),
                diesel::select(diesel::dsl::sql::<diesel::sql_types::Integer>("1"))
                    .get_result::<i32>(&mut connection),
            )
            .await
            .map_err(|_| PgError::ReadinessFailed)?
            .map_err(|_| PgError::ReadinessFailed)?;
            if value != 1 {
                return Err(PgError::ReadinessFailed);
            }
            Ok(())
        }
        .await;
        if let Err(error) = readiness {
            pool.close();
            return Err(error);
        }

        let operations = Arc::new(OperationTracker::default());
        operations.open();
        let runtime = Arc::new(DatabaseRuntime {
            pool,
            cancellation_tls,
            cancel_request_timeout: plan
                .connect_timeout()
                .min(plan.transaction_cleanup_timeout() / 2),
            transaction_cleanup_timeout: plan.transaction_cleanup_timeout(),
            shutdown_timeout: plan.shutdown_timeout(),
            operations,
            close_gate: Mutex::new(()),
        });
        self.state.runtime.set(runtime).map_err(|runtime| {
            runtime.pool.close();
            PgError::AlreadyInitialized
        })?;
        Ok(())
    }

    fn runtime(&self) -> PgResult<&Arc<DatabaseRuntime>> {
        self.state.runtime.get().ok_or(PgError::NotInitialized)
    }

    pub(crate) fn cleanup_timeout(&self) -> PgResult<Duration> {
        Ok(self.runtime()?.transaction_cleanup_timeout)
    }

    /// Acquires exclusive connection ownership without exposing the pool.
    ///
    /// The lease retains shutdown accounting until it is dropped. Cancellation
    /// interrupts pool waiting and returns [`PgError::OperationCancelled`].
    /// The optional signal applies to acquisition; supply the desired signal
    /// separately when executing work through the lease.
    pub async fn acquire_connection(
        &self,
        cancellation: Option<ExecutionCancellation>,
    ) -> PgResult<PgConnectionLease> {
        if lease::is_cancelled(&cancellation) {
            return Err(PgError::OperationCancelled);
        }
        let runtime = Arc::clone(self.runtime()?);
        let operation = runtime.operations.begin()?;
        let connection = tokio::select! {
            biased;
            _ = lease::cancelled(&cancellation) => return Err(PgError::OperationCancelled),
            connection = runtime.pool.get() => connection.map_err(PgError::from)?,
        };
        if lease::is_cancelled(&cancellation) {
            return Err(PgError::OperationCancelled);
        }
        Ok(PgConnectionLease::new(connection, runtime, operation))
    }

    /// Returns a point-in-time pool and scoped-operation snapshot.
    ///
    /// An uninitialized service returns [`PgError::NotInitialized`].
    pub fn status(&self) -> PgResult<PgPoolStatus> {
        let runtime = self.runtime()?;
        let status = runtime.pool.status();
        Ok(PgPoolStatus {
            max_size: status.max_size,
            size: status.size,
            available: status.available,
            waiting: status.waiting,
            in_flight: runtime.operations.active.load(Ordering::Acquire),
            closed: runtime.pool.is_closed(),
        })
    }

    #[lily_trace::lily_trace(name = "postgresql.database.with_connection")]
    /// Runs one caller-supplied Diesel operation on a pooled connection.
    ///
    /// The callback must return [`PgConnectionFuture`], normally through
    /// `Box::pin(async move { ... })`. The connection cannot escape the
    /// callback. Cancellation covers pool acquisition and callback execution;
    /// the same optional view is forwarded to the callback. Interrupted
    /// connections are discarded. Already completed statements outside a
    /// transaction are not rolled back by cancellation.
    pub async fn with_connection<T, Operation>(
        &self,
        operation: Operation,
        cancellation: Option<ExecutionCancellation>,
    ) -> PgResult<T>
    where
        T: Send,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
                Option<ExecutionCancellation>,
            ) -> PgConnectionFuture<'connection, T>
            + Send,
    {
        self.acquire_connection(cancellation.clone())
            .await?
            .with_connection(operation, cancellation)
            .await
    }

    #[lily_trace::lily_trace(name = "postgresql.database.transaction")]
    /// Runs one caller-supplied Diesel operation inside a transaction, with
    /// optional execution cancellation forwarded to the callback.
    ///
    /// Returning `Err` rolls the transaction back according to
    /// `diesel_async::AsyncConnection::transaction`; returning `Ok` commits it.
    /// `None` disables cancellation. With `Some`, cancellation stops connection
    /// acquisition or the callback and prevents commit if observed before the
    /// callback completes. Diesel still owns BEGIN, COMMIT and ROLLBACK.
    ///
    /// Cancellation cleanup has its own `transaction_cleanup_timeout_secs`
    /// budget, independent of the cancelled signal. A running SQL query receives
    /// a best-effort driver cancellation request using the pool's TLS policy.
    /// Cancelled connections are discarded to isolate delayed cancel requests
    /// from subsequent pool users. Cleanup failures take precedence over
    /// [`PgError::TransactionCancelled`].
    ///
    /// Once callback completion hands control to COMMIT or ordinary error
    /// rollback, finalization is awaited without token interruption. Cancellation
    /// during BEGIN waits for it and rolls back, or discards the connection when
    /// the cleanup budget expires. The borrowed connection cannot escape the
    /// callback. Keep awaiting this method after signalling cancellation:
    /// externally dropping its future does not guarantee completed rollback.
    pub async fn transaction<T, Operation>(
        &self,
        cancellation: Option<ExecutionCancellation>,
        operation: Operation,
    ) -> PgResult<T>
    where
        T: Send,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
                Option<ExecutionCancellation>,
            ) -> PgConnectionFuture<'connection, T>
            + Send,
    {
        if cancellation
            .as_ref()
            .is_some_and(ExecutionCancellation::is_cancelled)
        {
            return Err(PgError::TransactionCancelled);
        }
        let runtime = Arc::clone(self.runtime()?);
        let _operation_guard = runtime.operations.begin()?;
        let mut connection = match &cancellation {
            Some(token) => tokio::select! {
                biased;
                _ = token.cancelled() => return Err(PgError::TransactionCancelled),
                connection = runtime.pool.get() => connection,
            },
            None => runtime.pool.get().await,
        }
        .map_err(PgError::from)?;

        let Some(cancellation) = cancellation else {
            return connection
                .transaction(async move |connection| operation(connection, None).await)
                .await;
        };
        if cancellation.is_cancelled() {
            return Err(PgError::TransactionCancelled);
        }

        // These flags coordinate the callback with its enclosing cleanup wait;
        // both futures run in this task and require neither spawning nor channels.
        let finalizing = AtomicBool::new(false);
        let discard_connection = AtomicBool::new(false);
        let result = {
            let transaction = connection.transaction(async |connection| {
                if cancellation.is_cancelled() {
                    discard_connection.store(true, Ordering::Relaxed);
                    return Err(PgError::TransactionCancelled);
                }
                let cancel_token = connection.cancel_token();
                let result = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Err(PgError::TransactionCancelled),
                    result = operation(connection, Some(cancellation.clone())) => result,
                };
                // Also catch cancellation raised by a callback that returns Ok
                // in the same poll. No cancelled operation may choose commit.
                if cancellation.is_cancelled() {
                    discard_connection.store(true, Ordering::Relaxed);
                    runtime.cancel_query(cancel_token).await;
                    return Err(PgError::TransactionCancelled);
                }
                finalizing.store(true, Ordering::Relaxed);
                result
            });
            tokio::pin!(transaction);
            tokio::select! {
                biased;
                result = &mut transaction => result,
                _ = cancellation.cancelled() => {
                    if finalizing.load(Ordering::Relaxed) {
                        transaction.await
                    } else {
                        tokio::time::timeout(runtime.transaction_cleanup_timeout, transaction)
                            .await
                            .unwrap_or(Err(PgError::TransactionCleanupTimeout))
                    }
                }
            }
        };

        // Diesel preserves the callback error if its transaction manager broke.
        // Do not turn that case into an apparently completed cancellation cleanup.
        let result = if matches!(result, Err(PgError::TransactionCancelled))
            && diesel_async::pooled_connection::PoolableConnection::is_broken(&mut *connection)
        {
            Err(PgError::Query {
                kind: crate::PgQueryErrorKind::Transaction,
            })
        } else {
            result
        };
        if discard_connection.load(Ordering::Relaxed)
            || matches!(result, Err(PgError::TransactionCleanupTimeout))
        {
            drop(deadpool::managed::Object::take(connection));
        }
        result
    }

    /// Runs framework-owned work with a cloneable transaction handle.
    ///
    /// This is infrastructure ABI for Lily integrations such as a
    /// transactional inbox/outbox adapter. Application code should normally
    /// use [`Self::transaction`]. The spawned owner keeps the pooled connection
    /// and operation-accounting guard alive until commit or rollback completes,
    /// even when the awaiting caller is cancelled.
    #[doc(hidden)]
    pub async fn transaction_with_handle<T, Operation, OperationFuture>(
        &self,
        operation: Operation,
    ) -> PgResult<T>
    where
        T: Send + 'static,
        Operation: FnOnce(PgTransaction) -> OperationFuture + Send + 'static,
        OperationFuture: Future<Output = PgResult<T>> + Send + 'static,
    {
        self.transaction_with_handle_observed(operation, || {})
            .await
    }

    /// Runs framework-owned transaction work and invokes `finalized` only
    /// after the Diesel owner has completed commit or rollback.
    ///
    /// This hidden integration ABI lets a caller retain lifecycle evidence
    /// across actual database finalization. If the awaiting caller is dropped,
    /// the spawned owner first observes cancellation and rolls back; only then
    /// is the observer invoked. Observer panics are contained and cannot
    /// replace the transaction result.
    #[doc(hidden)]
    pub async fn transaction_with_handle_observed<T, Operation, OperationFuture, Finalized>(
        &self,
        operation: Operation,
        finalized: Finalized,
    ) -> PgResult<T>
    where
        T: Send + 'static,
        Operation: FnOnce(PgTransaction) -> OperationFuture + Send + 'static,
        OperationFuture: Future<Output = PgResult<T>> + Send + 'static,
        Finalized: FnOnce() + Send + 'static,
    {
        let runtime = Arc::clone(self.runtime()?);
        let operation_guard = runtime.operations.begin()?;
        let (cancel_sender, cancel_receiver) = tokio::sync::oneshot::channel();
        let mut cancellation = CancelTransactionOnDrop::new(cancel_sender);

        let owner = tokio::spawn(observe_transaction_finalization(
            async move {
                let _operation_guard = operation_guard;
                let mut cancel_receiver = cancel_receiver;
                let connection = tokio::select! {
                    biased;
                    _ = &mut cancel_receiver => {
                        return Err(PgError::TransactionOperationDetached);
                    }
                    connection = runtime.pool.get() => connection.map_err(PgError::from)?,
                };
                let mut connection = connection;
                connection
                    .transaction(async move |connection| {
                        let (transaction, commands) = PgTransaction::channel();
                        let operation = operation(transaction);
                        drive_transaction(connection, commands, operation, cancel_receiver).await
                    })
                    .await
            },
            finalized,
        ));

        let result = owner.await.map_err(|_| PgError::TransactionFinalization)?;
        cancellation.disarm();
        result
    }

    /// Runs an integration-owned transaction without spawning another owner.
    ///
    /// The integration must retain and join the task polling this future.
    /// Cancellation requests rollback; dropping the future discards the pooled
    /// connection, so an unfinished transaction can never return to the pool.
    #[doc(hidden)]
    pub async fn transaction_with_handle_owned<T, Operation, OperationFuture>(
        &self,
        operation: Operation,
        mut cancellation: tokio::sync::oneshot::Receiver<()>,
    ) -> PgResult<T>
    where
        T: Send + 'static,
        Operation: FnOnce(PgTransaction) -> OperationFuture + Send + 'static,
        OperationFuture: Future<Output = PgResult<T>> + Send + 'static,
    {
        let runtime = Arc::clone(self.runtime()?);
        let guard = runtime.operations.begin()?;
        let connection = tokio::select! {
            biased;
            _ = &mut cancellation => return Err(PgError::TransactionOperationDetached),
            connection = runtime.pool.get() => connection.map_err(PgError::from)?,
        };
        let mut lease = PgConnectionLease::new(connection, runtime, guard);
        lease
            .run(move |connection| {
                Box::pin(async move {
                    connection
                        .transaction(async move |connection| {
                            let (transaction, commands) = PgTransaction::channel();
                            drive_transaction(
                                connection,
                                commands,
                                operation(transaction),
                                cancellation,
                            )
                            .await
                        })
                        .await
                })
            })
            .await?
    }

    /// Runs pending Diesel migrations explicitly and returns applied versions.
    ///
    /// DI initialization never invokes this method. Run it from a dedicated
    /// deployment or migration binary on a multi-thread Tokio runtime. Migration
    /// errors are deliberately reduced to the secret-safe
    /// [`PgError::Migration`] category.
    pub async fn run_pending_migrations<Source>(&self, source: Source) -> PgResult<Vec<String>>
    where
        Source: diesel::migration::MigrationSource<diesel::pg::Pg> + Send + 'static,
    {
        use diesel_async::AsyncMigrationHarness;
        use diesel_migrations::MigrationHarness as _;

        if tokio::runtime::Handle::current().runtime_flavor()
            == tokio::runtime::RuntimeFlavor::CurrentThread
        {
            return Err(PgError::Migration {
                code: "multi_thread_runtime_required",
            });
        }

        let runtime = Arc::clone(self.runtime()?);
        let _operation_guard = runtime.operations.begin()?;
        let connection = runtime.pool.get().await.map_err(PgError::from)?;
        let mut harness = AsyncMigrationHarness::new(connection);
        harness
            .run_pending_migrations(source)
            .map(|versions| {
                versions
                    .into_iter()
                    .map(|version| version.to_string())
                    .collect()
            })
            .map_err(|_| PgError::Migration {
                code: "run_pending_failed",
            })
    }

    /// Stops new acquisitions and drains scoped operations for the owning
    /// container or factory.
    #[cfg(any(feature = "single", feature = "factory"))]
    pub(crate) async fn close(&self) -> PgResult<()> {
        let Some(runtime) = self.state.runtime.get().cloned() else {
            return Ok(());
        };
        let _close_guard = runtime.close_gate.lock().await;
        runtime.operations.close_admission();
        runtime.pool.close();
        runtime
            .operations
            .wait_until_idle(runtime.shutdown_timeout)
            .await
    }
}

#[cfg(feature = "single")]
#[async_trait]
impl ServiceTrait for PgDatabaseService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        #[cfg(feature = "test-support")]
        if self.test_container_fixture {
            return Ok(());
        }
        let lily = self.config_service.get_lily_config().await;
        let config = lily.postgresql.as_ref().ok_or_else(|| {
            InjectionError::InitError("PostgreSQL configuration is missing".into())
        })?;
        let plan = PgConnectionPlan::from_single(config)
            .map_err(|error| InjectionError::InitError(error.to_string()))?;
        self.install_ready(plan).await.map_err(|error| {
            tracing::error!(
                error_code = error_code(&error),
                "PostgreSQL startup readiness failed"
            );
            InjectionError::InitError("PostgreSQL startup readiness failed".into())
        })?;
        tracing::info!("PostgreSQL database service initialized");
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.close().await.map_err(|error| {
            tracing::error!(
                error_code = error_code(&error),
                "PostgreSQL database service shutdown failed"
            );
            InjectionError::DisposeError("PostgreSQL shutdown failed".into())
        })?;
        tracing::info!("PostgreSQL database service disposed");
        Ok(())
    }
}

#[cfg(feature = "single")]
fn error_code(error: &PgError) -> &'static str {
    match error {
        PgError::FeatureModeMismatch { .. } => "feature_mode_mismatch",
        PgError::InvalidConfiguration { .. } => "invalid_configuration",
        PgError::NotInitialized => "not_initialized",
        PgError::RepositoryDatabaseNotSet => "repository_database_not_set",
        PgError::UnknownCell { .. } => "unknown_cell",
        PgError::AlreadyInitialized => "already_initialized",
        PgError::PoolClosed => "pool_closed",
        PgError::OperationCancelled => "operation_cancelled",
        PgError::ConnectionInterrupted => "connection_interrupted",
        PgError::OperationPanicked => "operation_panicked",
        PgError::ContextClosed => "context_closed",
        PgError::ContextBusy => "context_busy",
        PgError::ContextTransactionActive => "context_transaction_active",
        PgError::TransactionRollbackOnly => "transaction_rollback_only",
        PgError::ContextCleanupTimeout => "context_cleanup_timeout",
        PgError::PoolTimeout { .. } => "pool_timeout",
        PgError::TransactionCancelled => "transaction_cancelled",
        PgError::TransactionCleanupTimeout => "transaction_cleanup_timeout",
        PgError::ConnectionFailed => "connection_failed",
        PgError::ReadinessFailed => "readiness_failed",
        PgError::Query { .. } => "query_failed",
        PgError::Tls { .. } => "tls_failed",
        PgError::PoolBuild => "pool_build_failed",
        PgError::ShutdownTimeout { .. } => "shutdown_timeout",
        PgError::FactoryStartup { .. } => "factory_startup_failed",
        PgError::FactoryStartupRollback { .. } => "factory_startup_rollback_failed",
        PgError::FactoryShutdown { .. } => "factory_shutdown_failed",
        PgError::Migration { .. } => "migration_failed",
        PgError::TransactionCommandSaturated => "transaction_command_saturated",
        PgError::TransactionClosed => "transaction_closed",
        PgError::TransactionOperationDetached => "transaction_operation_detached",
        PgError::TransactionFinalization => "transaction_finalization_failed",
    }
}

#[cfg(test)]
mod repository_tests;

#[cfg(test)]
mod cancellation_tests;

#[cfg(all(test, any(feature = "single", feature = "factory")))]
mod context_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_service_does_not_contact_or_publish_a_database() {
        let service = PgDatabaseService::default();
        assert_eq!(service.status(), Err(PgError::NotInitialized));
    }

    #[tokio::test]
    async fn operation_tracker_rejects_new_work_and_drains_existing_work() {
        let tracker = Arc::new(OperationTracker::default());
        tracker.open();
        let operation = tracker.begin().unwrap();
        tracker.close_admission();
        assert!(matches!(tracker.begin(), Err(PgError::PoolClosed)));

        let waiter = {
            let tracker = Arc::clone(&tracker);
            tokio::spawn(async move { tracker.wait_until_idle(Duration::from_millis(100)).await })
        };
        drop(operation);
        assert_eq!(waiter.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn operation_tracker_reports_bounded_shutdown_timeout() {
        let tracker = Arc::new(OperationTracker::default());
        tracker.open();
        let _operation = tracker.begin().unwrap();
        tracker.close_admission();
        assert_eq!(
            tracker.wait_until_idle(Duration::from_millis(1)).await,
            Err(PgError::ShutdownTimeout { remaining: 1 })
        );
    }
}
