use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use diesel_async::AsyncPgConnection;
use futures_util::FutureExt;
use lily_error::injection::InjectionError;
#[cfg(feature = "single")]
use lily_injectable_derive::Injectable;
use lily_injection::{ProcessContext, ServiceTrait};

use crate::{ExecutionCancellation, PgConnectionFuture, PgDatabaseService, PgError};

mod control;
mod error;
mod owner;
mod runtime;
#[cfg(test)]
mod tests;

use control::Control;
use error::{ContextError, ContextResult};
use runtime::{Admission, ContextRuntime};

/// Scoped PostgreSQL connection sharing for services and repositories.
///
/// Construction acquires no connection. Outside a transaction, each operation
/// briefly leases a connection. Inside one, repositories using this same
/// context borrow the transaction's connection. Overlapping queries fail with
/// [`PgError::ContextBusy`]; nested transactions are rejected.
///
/// Feature `single` registers this service with Lily's scoped lifetime. In
/// factory mode, construct it from the selected `Arc<PgDatabaseService>` in an
/// application-owned scoped service and forward that service's disposal here.
#[derive(Default)]
#[cfg_attr(feature = "single", derive(Injectable))]
#[cfg_attr(feature = "single", service(lifetime = "Scoped"))]
pub struct PgDbContext {
    #[cfg_attr(feature = "single", inject)]
    database: Arc<PgDatabaseService>,
    runtime: Arc<ContextRuntime>,
}

impl From<Arc<PgDatabaseService>> for PgDbContext {
    fn from(database: Arc<PgDatabaseService>) -> Self {
        Self {
            database,
            runtime: Arc::default(),
        }
    }
}

#[async_trait]
impl ServiceTrait for PgDbContext {
    async fn dispose(&self) -> Result<(), InjectionError> {
        self.runtime
            .dispose()
            .await
            .map_err(|error| InjectionError::DisposeError(error.to_string()))
    }
}

impl PgDbContext {
    /// Runs a query on the active transaction or on a short-lived pooled lease.
    ///
    /// Inside a transaction its optional token always wins, including `None`;
    /// outside one this method's token covers acquisition and execution. That
    /// selected view is forwarded to the callback. A callback error makes an
    /// active transaction rollback-only even if the workflow swallows it.
    /// Dropping an active query also prevents the transaction from committing.
    ///
    /// The callback may return an application error `E`, which is returned
    /// intact unless a framework stop reason supersedes it. Framework errors
    /// use `E::from` after releasing query locks and recording the outcome;
    /// pooled operations also release their lease and context admission first.
    /// If transaction work swallows a custom callback error, finalization
    /// returns [`PgError::TransactionRollbackOnly`] converted into its error
    /// type. Callbacks returning [`crate::PgResult`] retain their first `PgError`.
    #[lily_trace::lily_trace(name = "postgresql.context.with_connection")]
    pub async fn with_connection<T, E, Operation>(
        &self,
        operation: Operation,
        cancellation: Option<ExecutionCancellation>,
    ) -> Result<T, E>
    where
        T: Send,
        E: From<PgError> + Send + 'static,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
                Option<ExecutionCancellation>,
            ) -> PgConnectionFuture<'connection, T, E>
            + Send,
    {
        let result = match self
            .runtime
            .operation(cancellation, || self.database.cleanup_timeout())?
        {
            Admission::Transaction(control) => query(&control, operation).await,
            Admission::Pooled(completion) => {
                let control = Arc::clone(&completion.control);
                let acquired = tokio::select! {
                    biased;
                    _ = control.cancelled() => Err(control.reason().expect("stop has a reason")),
                    result = self.database.acquire_connection(control.cancellation.clone()) => result,
                };
                let connection = match acquired {
                    Ok(connection) => connection,
                    Err(error) => {
                        completion.finish(Ok(()));
                        return Err(error.into());
                    }
                };
                let _ = control.driver.set(connection.cancellation());
                *control.connection.lock().await = Some(connection);
                control.activate();
                let mut result = query(&control, operation).await;
                if let Some(reason) = control.reason() {
                    control.stop(reason.clone());
                    result = Err(reason.into());
                    if control.needs_query_cancel() {
                        let _ = tokio::time::timeout_at(control.deadline(), async {
                            if let Some(driver) = control.driver.get() {
                                driver.cancel_query().await;
                            }
                        })
                        .await;
                    }
                }
                if let Some(connection) = control.connection.lock().await.take() {
                    if control.reason().is_some() {
                        connection.discard();
                    } else {
                        drop(connection);
                    }
                }
                completion.finish(Ok(()));
                result
            }
        };
        result.map_err(ContextError::into_application)
    }

    /// Runs repository calls on one connection under a directly managed Diesel
    /// transaction. The callback receives the transaction's optional token.
    ///
    /// Work may return an application error `E`. A work error selects rollback
    /// and is returned unchanged when rollback succeeds, unless cancellation or
    /// another framework stop reason supersedes it. Framework errors are
    /// converted with `E::from`; a rollback failure takes precedence over the
    /// work error because successful cleanup could not be confirmed. Conversion
    /// happens after the owner's cleanup attempt and context state update.
    /// A recorded query failure still prevents commit if work returns `Ok`.
    ///
    /// An owned task completes rollback when the awaiting caller is dropped.
    /// It inherits Lily's current process context and tracing span. Cancellation
    /// before commit selects rollback; once COMMIT starts it is awaited without
    /// interruption and may succeed despite later cancellation or disposal.
    /// Rollback has an independent `transaction_cleanup_timeout_secs` budget.
    /// Cleanup failure closes the context and prevents connection reuse.
    #[lily_trace::lily_trace(name = "postgresql.context.transaction")]
    pub async fn transaction<T, E, Work, WorkFuture>(
        &self,
        work: Work,
        cancellation: Option<ExecutionCancellation>,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<PgError> + Send + 'static,
        Work: FnOnce(Option<ExecutionCancellation>) -> WorkFuture + Send + 'static,
        WorkFuture: Future<Output = Result<T, E>> + Send + 'static,
    {
        let completion = self
            .runtime
            .transaction(cancellation, || self.database.cleanup_timeout())?;
        let mut caller = owner::CallerGuard::new(Arc::clone(&completion.control));
        let database = Arc::clone(&self.database);
        let process = ProcessContext::current();
        let task = async move {
            let execution = owner::run(database, completion, work);
            match process {
                Some(process) => ProcessContext::scope(process, execution).await,
                None => execution.await,
            }
        };
        let result = tokio::spawn(tracing::Instrument::in_current_span(task)).await;
        caller.disarm();
        result
            .map_err(|_| PgError::TransactionFinalization)?
            .map_err(ContextError::into_application)
    }
}

async fn query<T, E, Operation>(control: &Arc<Control>, operation: Operation) -> ContextResult<T, E>
where
    T: Send,
    E: Send + 'static,
    Operation: for<'connection> FnOnce(
            &'connection mut AsyncPgConnection,
            Option<ExecutionCancellation>,
        ) -> PgConnectionFuture<'connection, T, E>
        + Send,
{
    let permit = control.admit()?;
    let mut slot = control
        .connection
        .try_lock()
        .map_err(|_| PgError::ContextBusy)?;
    let connection = slot.as_mut().ok_or(PgError::ContextClosed)?;
    let mut result = {
        let work = std::panic::AssertUnwindSafe(
            connection.run(|connection| operation(connection, control.cancellation.clone())),
        )
        .catch_unwind();
        tokio::select! {
            biased;
            _ = control.cancelled() => Err(control.reason().expect("stop has a reason").into()),
            result = work => match result {
                Ok(Ok(result)) => result.map_err(ContextError::Application),
                Ok(Err(error)) => Err(error.into()),
                Err(_) => {
                    control.stop(PgError::OperationPanicked);
                    Err(PgError::OperationPanicked.into())
                },
            },
        }
    };
    if let Some(reason) = control.reason() {
        control.stop(reason.clone());
        result = Err(reason.into());
    }
    let interrupted = connection.interrupted();
    drop(slot);
    permit.complete(
        result.as_ref().err().map(ContextError::query_failure),
        interrupted,
    );
    result
}
