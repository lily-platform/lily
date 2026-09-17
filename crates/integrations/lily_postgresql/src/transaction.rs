use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;

use diesel_async::AsyncPgConnection;
use tokio::sync::{mpsc, oneshot};

use crate::{PgConnectionFuture, PgError, PgResult};

pub(crate) const TRANSACTION_COMMAND_CAPACITY: usize = 16;

type ErasedValue = Box<dyn Any + Send>;
type ErasedConnectionFuture<'connection> =
    Pin<Box<dyn Future<Output = PgResult<ErasedValue>> + Send + 'connection>>;
trait ErasedOperation: Send {
    fn execute(self: Box<Self>, connection: &mut AsyncPgConnection) -> ErasedConnectionFuture<'_>;
}

struct TypedOperation<Operation, T> {
    operation: Option<Operation>,
    output: PhantomData<fn() -> T>,
}

impl<Operation, T> ErasedOperation for TypedOperation<Operation, T>
where
    T: Send + 'static,
    Operation: for<'connection> FnOnce(
            &'connection mut AsyncPgConnection,
        ) -> PgConnectionFuture<'connection, T>
        + Send
        + 'static,
{
    fn execute(
        mut self: Box<Self>,
        connection: &mut AsyncPgConnection,
    ) -> ErasedConnectionFuture<'_> {
        let operation = self
            .operation
            .take()
            .expect("transaction operation can only execute once");
        Box::pin(async move {
            operation(connection)
                .await
                .map(|value| Box::new(value) as ErasedValue)
        })
    }
}

pub(crate) struct TransactionCommand {
    operation: Box<dyn ErasedOperation>,
    result: oneshot::Sender<PgResult<ErasedValue>>,
}

/// Cloneable access to the PostgreSQL transaction owned by a framework runner.
///
/// Every operation is executed serially on the exact pooled connection on
/// which the surrounding transaction was opened. The handle deliberately has
/// no commit or rollback method: the framework runner is the sole transaction
/// finalization authority.
///
/// The command queue is bounded. Holding or cloning this value does not reserve
/// another pool connection, and it is safe to place the handle in request- or
/// delivery-local state for the lifetime of the owning operation.
#[derive(Clone)]
pub struct PgTransaction {
    commands: mpsc::Sender<TransactionCommand>,
}

impl PgTransaction {
    pub(crate) fn channel() -> (Self, mpsc::Receiver<TransactionCommand>) {
        let (commands, receiver) = mpsc::channel(TRANSACTION_COMMAND_CAPACITY);
        (Self { commands }, receiver)
    }

    /// Runs one Diesel operation on the connection owned by this transaction.
    ///
    /// Concurrent callers are serialized. A full bounded command queue returns
    /// [`PgError::TransactionCommandSaturated`] immediately instead of adding
    /// unbounded retained work. Once the framework owner starts finalization,
    /// new operations return [`PgError::TransactionClosed`]. Cancelling the
    /// owning framework operation drops an in-progress query future before the
    /// owner rolls the transaction back.
    pub async fn with_connection<T, Operation>(&self, operation: Operation) -> PgResult<T>
    where
        T: Send + 'static,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
            ) -> PgConnectionFuture<'connection, T>
            + Send
            + 'static,
    {
        let operation = Box::new(TypedOperation {
            operation: Some(operation),
            output: PhantomData,
        });
        let (result, receiver) = oneshot::channel();
        self.commands
            .try_send(TransactionCommand { operation, result })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => PgError::TransactionCommandSaturated,
                mpsc::error::TrySendError::Closed(_) => PgError::TransactionClosed,
            })?;

        let value = receiver
            .await
            .map_err(|_| PgError::TransactionOperationDetached)??;
        value
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| PgError::TransactionFinalization)
    }
}

pub(crate) async fn drive_transaction<T, OperationFuture>(
    connection: &mut AsyncPgConnection,
    mut commands: mpsc::Receiver<TransactionCommand>,
    operation: OperationFuture,
    mut cancelled: oneshot::Receiver<()>,
) -> PgResult<T>
where
    T: Send + 'static,
    OperationFuture: Future<Output = PgResult<T>> + Send,
{
    tokio::pin!(operation);
    loop {
        tokio::select! {
            biased;
            _ = &mut cancelled => return Err(PgError::TransactionOperationDetached),
            result = &mut operation => {
                commands.close();
                return match commands.try_recv() {
                    Ok(_) => Err(PgError::TransactionOperationDetached),
                    Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => result,
                };
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    return Err(PgError::TransactionOperationDetached);
                };
                let TransactionCommand {
                    operation,
                    result: mut result_sender,
                } = command;
                if result_sender.is_closed() {
                    return Err(PgError::TransactionOperationDetached);
                }
                let execution = operation.execute(connection);
                tokio::pin!(execution);
                let operation_result = tokio::select! {
                    biased;
                    _ = &mut cancelled => {
                        return Err(PgError::TransactionOperationDetached);
                    }
                    () = result_sender.closed() => {
                        return Err(PgError::TransactionOperationDetached);
                    }
                    result = &mut execution => result,
                };
                if result_sender.send(operation_result).is_err() {
                    return Err(PgError::TransactionOperationDetached);
                }
            }
        }
    }
}

pub(crate) struct CancelTransactionOnDrop(Option<oneshot::Sender<()>>);

impl CancelTransactionOnDrop {
    pub(crate) fn new(sender: oneshot::Sender<()>) -> Self {
        Self(Some(sender))
    }

    pub(crate) fn disarm(&mut self) {
        self.0.take();
    }
}

impl Drop for CancelTransactionOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

struct TransactionFinalizationObserver<Finalized>
where
    Finalized: FnOnce(),
{
    finalized: Option<Finalized>,
}

impl<Finalized> TransactionFinalizationObserver<Finalized>
where
    Finalized: FnOnce(),
{
    fn new(finalized: Finalized) -> Self {
        Self {
            finalized: Some(finalized),
        }
    }
}

impl<Finalized> Drop for TransactionFinalizationObserver<Finalized>
where
    Finalized: FnOnce(),
{
    fn drop(&mut self) {
        if let Some(finalized) = self.finalized.take() {
            // Infrastructure observers must never replace the transaction's
            // real commit/rollback outcome or turn a second panic into abort.
            let _ = catch_unwind(AssertUnwindSafe(finalized));
        }
    }
}

/// Runs the exact owner future and notifies only after that future has
/// completed or has been dropped during unwind/cancellation.
pub(crate) async fn observe_transaction_finalization<Output, Owner, Finalized>(
    owner: Owner,
    finalized: Finalized,
) -> Output
where
    Owner: Future<Output = Output>,
    Finalized: FnOnce(),
{
    let _observer = TransactionFinalizationObserver::new(finalized);
    owner.await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit_operation(connection: &mut AsyncPgConnection) -> PgConnectionFuture<'_, ()> {
        let _ = connection;
        Box::pin(async { Ok(()) })
    }

    #[tokio::test]
    async fn closed_transaction_rejects_new_operations() {
        let (transaction, receiver) = PgTransaction::channel();
        drop(receiver);
        assert_eq!(
            transaction
                .with_connection::<(), _>(|_| Box::pin(async { Ok(()) }))
                .await,
            Err(PgError::TransactionClosed)
        );
    }

    #[tokio::test]
    async fn command_admission_is_bounded() {
        let (transaction, _receiver) = PgTransaction::channel();
        for _ in 0..TRANSACTION_COMMAND_CAPACITY {
            let (result, _) = oneshot::channel();
            transaction
                .commands
                .try_send(TransactionCommand {
                    operation: Box::new(TypedOperation {
                        operation: Some(unit_operation),
                        output: PhantomData,
                    }),
                    result,
                })
                .unwrap();
        }
        assert_eq!(
            transaction
                .with_connection::<(), _>(|_| Box::pin(async { Ok(()) }))
                .await,
            Err(PgError::TransactionCommandSaturated)
        );
    }

    #[test]
    fn cancellation_guard_signals_drop_and_can_be_disarmed() {
        let (sender, mut receiver) = oneshot::channel();
        drop(CancelTransactionOnDrop::new(sender));
        assert_eq!(receiver.try_recv(), Ok(()));

        let (sender, mut receiver) = oneshot::channel();
        let mut guard = CancelTransactionOnDrop::new(sender);
        guard.disarm();
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test]
    async fn finalization_observer_runs_after_the_owner_future_not_before() {
        let finalized = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let finalized_for_callback = std::sync::Arc::clone(&finalized);
        let (entered, entered_receiver) = oneshot::channel();
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let owner_release = std::sync::Arc::clone(&release);
        let owner = tokio::spawn(async move {
            observe_transaction_finalization(
                async move {
                    entered.send(()).unwrap();
                    owner_release.notified().await;
                    42
                },
                move || {
                    finalized_for_callback.store(true, std::sync::atomic::Ordering::Release);
                },
            )
            .await
        });

        entered_receiver.await.unwrap();
        assert!(!finalized.load(std::sync::atomic::Ordering::Acquire));
        release.notify_one();
        assert_eq!(owner.await.unwrap(), 42);
        assert!(finalized.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn dropping_an_owner_future_still_notifies_finalization_once() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_callback = std::sync::Arc::clone(&calls);
        let mut observed = Box::pin(observe_transaction_finalization(
            std::future::pending::<()>(),
            move || {
                calls_for_callback.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            },
        ));
        assert!(futures_util::poll!(observed.as_mut()).is_pending());
        drop(observed);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn finalization_observer_panic_cannot_replace_the_owner_result() {
        let result = observe_transaction_finalization(async { 42 }, || {
            panic!("qualification finalization observer panic")
        })
        .await;

        assert_eq!(result, 42);
    }
}
