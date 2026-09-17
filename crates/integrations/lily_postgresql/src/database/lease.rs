use std::sync::Arc;

use diesel_async::pooled_connection::PoolableConnection;
use diesel_async::{AsyncConnection, AsyncPgConnection, TransactionManager};
use futures_util::FutureExt;

use super::{DatabaseRuntime, OperationGuard};
use crate::{ExecutionCancellation, PgConnectionFuture, PgError, PgResult};

type Connection = diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>;
type Manager = <AsyncPgConnection as AsyncConnection>::TransactionManager;

/// Exclusive ownership of one pooled connection and its shutdown reservation.
///
/// Acquired through [`crate::PgDatabaseService::acquire_connection`]. A healthy
/// connection returns to the private pool on drop. Interrupted operations and
/// unfinished or broken transactions remove the connection from the pool.
/// The wrapper exposes borrowed connection access, keeping the raw pool private.
pub struct PgConnectionLease {
    connection: Option<Connection>,
    runtime: Arc<DatabaseRuntime>,
    _operation: OperationGuard,
    discard_on_drop: bool,
    operation_interrupted: bool,
}

impl PgConnectionLease {
    pub(super) fn new(
        connection: Connection,
        runtime: Arc<DatabaseRuntime>,
        operation: OperationGuard,
    ) -> Self {
        Self {
            connection: Some(connection),
            runtime,
            _operation: operation,
            discard_on_drop: false,
            operation_interrupted: false,
        }
    }

    /// Executes a callback with optional cancellation on this connection.
    ///
    /// Cancellation stops the callback, attempts bounded driver cancellation,
    /// and prevents reuse of this lease. Dropping the future also prevents reuse.
    /// This is connection access, not an implicit transaction: completed SQL
    /// outside a transaction may already have committed when cancellation wins.
    pub async fn with_connection<T, Operation>(
        &mut self,
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
        if is_cancelled(&cancellation) {
            return Err(PgError::OperationCancelled);
        }
        let driver = self.cancellation();
        let result = {
            let work = std::panic::AssertUnwindSafe(
                self.run(|connection| operation(connection, cancellation.clone())),
            )
            .catch_unwind();
            tokio::select! {
                biased;
                _ = cancelled(&cancellation) => Err(PgError::OperationCancelled),
                result = work => result
                    .unwrap_or(Err(PgError::OperationPanicked))
                    .and_then(std::convert::identity),
            }
        };
        if is_cancelled(&cancellation) {
            self.discard_on_drop = true;
            self.operation_interrupted = true;
            driver.cancel_query().await;
            return Err(PgError::OperationCancelled);
        }
        result
    }

    /// Permanently removes this connection from the pool instead of recycling it.
    pub fn discard(mut self) {
        self.discard_on_drop = true;
    }

    // Separate lease failures from the callback result so callers can release
    // their locks and finish cleanup before invoking application conversions.
    pub(crate) async fn run<T, E, Operation>(
        &mut self,
        operation: Operation,
    ) -> PgResult<Result<T, E>>
    where
        T: Send,
        E: Send,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
            ) -> PgConnectionFuture<'connection, T, E>
            + Send,
    {
        if self.operation_interrupted {
            return Err(PgError::ConnectionInterrupted);
        }
        // A dropped future or synchronous callback panic leaves this set.
        self.operation_interrupted = true;
        let result = operation(self.connection()).await;
        self.operation_interrupted = false;
        Ok(result)
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.operation_interrupted
    }

    pub(crate) fn cancellation(&self) -> PgQueryCancellation {
        PgQueryCancellation {
            token: self
                .connection
                .as_ref()
                .expect("lease owns connection")
                .cancel_token(),
            runtime: Arc::clone(&self.runtime),
        }
    }

    fn connection(&mut self) -> &mut AsyncPgConnection {
        self.connection.as_mut().expect("lease owns connection")
    }

    pub(crate) async fn begin(&mut self) -> PgResult<()> {
        self.discard_on_drop = true;
        Manager::begin_transaction(self.connection())
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn finalize(&mut self, commit: bool) -> PgResult<()> {
        let depth =
            Manager::transaction_manager_status_mut(self.connection()).transaction_depth()?;
        if depth.map(|depth| depth.get()) != Some(1) || (commit && self.operation_interrupted) {
            return Err(PgError::TransactionFinalization);
        }
        if commit {
            Manager::commit_transaction(self.connection()).await?;
        } else {
            Manager::rollback_transaction(self.connection()).await?;
        }
        if self.connection().is_broken() {
            return Err(PgError::TransactionFinalization);
        }
        self.operation_interrupted = false;
        self.discard_on_drop = false;
        Ok(())
    }
}

impl Drop for PgConnectionLease {
    fn drop(&mut self) {
        if let Some(mut connection) = self.connection.take()
            && (self.discard_on_drop
                || self.operation_interrupted
                || std::thread::panicking()
                || connection.is_broken())
        {
            drop(deadpool::managed::Object::take(connection));
        }
    }
}

pub(crate) struct PgQueryCancellation {
    token: tokio_postgres::CancelToken,
    runtime: Arc<DatabaseRuntime>,
}

impl PgQueryCancellation {
    pub(crate) async fn cancel_query(&self) {
        self.runtime.cancel_query(self.token.clone()).await;
    }
}

pub(crate) fn is_cancelled(cancellation: &Option<ExecutionCancellation>) -> bool {
    cancellation
        .as_ref()
        .is_some_and(ExecutionCancellation::is_cancelled)
}

pub(crate) async fn cancelled(cancellation: &Option<ExecutionCancellation>) {
    match cancellation {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}
