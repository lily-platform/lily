use std::future::Future;
use std::sync::Arc;

use futures_util::FutureExt;

use super::control::Control;
use super::error::{ContextError, ContextResult};
use super::runtime::Completion;
use crate::{ExecutionCancellation, PgDatabaseService, PgError, PgResult};

pub(super) struct CallerGuard(Option<Arc<Control>>);

impl CallerGuard {
    pub(super) fn new(control: Arc<Control>) -> Self {
        Self(Some(control))
    }
    pub(super) fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for CallerGuard {
    fn drop(&mut self) {
        if let Some(control) = &self.0 {
            control.stop(PgError::TransactionOperationDetached);
        }
    }
}

pub(super) async fn run<T, E, Work, WorkFuture>(
    database: Arc<PgDatabaseService>,
    completion: Completion,
    work: Work,
) -> ContextResult<T, E>
where
    T: Send,
    E: Send,
    Work: FnOnce(Option<ExecutionCancellation>) -> WorkFuture + Send,
    WorkFuture: Future<Output = Result<T, E>> + Send,
{
    let control = Arc::clone(&completion.control);
    let (result, cleanup) = std::panic::AssertUnwindSafe(execute(&database, &control, work))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| failed(PgError::TransactionFinalization));
    if let Ok(mut slot) = control.connection.try_lock()
        && let Some(connection) = slot.take()
    {
        if cleanup.is_err() || control.reason().is_some() {
            connection.discard();
        } else {
            drop(connection);
        }
    }
    completion.finish(cleanup);
    result
}

async fn execute<T, E, Work, WorkFuture>(
    database: &PgDatabaseService,
    control: &Arc<Control>,
    work: Work,
) -> (ContextResult<T, E>, PgResult<()>)
where
    T: Send,
    E: Send,
    Work: FnOnce(Option<ExecutionCancellation>) -> WorkFuture + Send,
    WorkFuture: Future<Output = Result<T, E>> + Send,
{
    let acquired = tokio::select! {
        biased;
        _ = control.cancelled() => Err(control.reason().expect("stop has a reason")),
        result = database.acquire_connection(control.cancellation.clone()) => result,
    };
    let connection = match acquired {
        Ok(connection) => connection,
        Err(error) => return (Err(control.reason().unwrap_or(error).into()), Ok(())),
    };
    let _ = control.driver.set(connection.cancellation());
    *control.connection.lock().await = Some(connection);
    let begun = {
        let mut slot = control.connection.lock().await;
        let begin = slot.as_mut().expect("owner installed lease").begin();
        tokio::pin!(begin);
        tokio::select! {
            biased;
            _ = control.cancelled() => {
                control.stop(control.reason().expect("stop has a reason"));
                // Retain the native control future: dropping it breaks Diesel's
                // transaction manager and makes a subsequent rollback unsafe.
                tokio::time::timeout_at(control.deadline(), begin).await
                    .unwrap_or(Err(PgError::TransactionCleanupTimeout))
            },
            result = &mut begin => result,
        }
    };
    if let Err(error) = begun {
        return failed(error);
    }
    control.activate();
    let result = tokio::select! {
        biased;
        _ = control.cancelled() => {
            let reason = control.reason().expect("stop has a reason");
            control.stop(reason.clone());
            Err(reason.into())
        },
        result = std::panic::AssertUnwindSafe(async { work(control.cancellation.clone()).await }).catch_unwind() => {
            result.map(|result| result.map_err(ContextError::Application)).unwrap_or_else(|_| {
                control.stop(PgError::OperationPanicked);
                Err(PgError::OperationPanicked.into())
            })
        },
    };
    finalize(control, control.finalize(result)).await
}

async fn finalize<T, E>(
    control: &Arc<Control>,
    mut result: ContextResult<T, E>,
) -> (ContextResult<T, E>, PgResult<()>) {
    if result.is_ok() {
        // Finalizing rejects new operations, and no query is busy. This lock
        // must be immediately available. COMMIT does not start a cleanup timer.
        let Ok(mut slot) = control.connection.try_lock() else {
            return failed(PgError::TransactionFinalization);
        };
        if let Some(reason) = control.reason() {
            result = Err(reason.into());
        } else {
            return match slot
                .as_mut()
                .expect("owner installed lease")
                .finalize(true)
                .await
            {
                Ok(()) => (result, Ok(())),
                Err(error) => failed(error),
            };
        }
    }

    let rollback = async {
        let mut slot = control.connection.lock().await;
        slot.as_mut()
            .ok_or(PgError::TransactionFinalization)?
            .finalize(false)
            .await
    };
    let cancel = async {
        if control.needs_query_cancel()
            && let Some(driver) = control.driver.get()
        {
            driver.cancel_query().await;
        }
    };
    // Poll rollback first and retain it while driving cancellation. Sending a
    // cancel request to an idle backend before ROLLBACK can cancel ROLLBACK
    // itself. PostgreSQL has no acknowledgement that a cancel hit its target;
    // any rollback failure remains an error, and cancelled leases are discarded.
    let cleanup = tokio::time::timeout_at(control.deadline(), async {
        tokio::pin!(rollback);
        tokio::select! {
            biased;
            result = &mut rollback => result,
            () = cancel => rollback.await,
        }
    })
    .await
    .unwrap_or(Err(PgError::TransactionCleanupTimeout));
    match cleanup {
        Ok(()) => (result, Ok(())),
        Err(error) => failed(error),
    }
}

fn failed<T, E>(error: PgError) -> (ContextResult<T, E>, PgResult<()>) {
    (Err(error.clone().into()), Err(error))
}
