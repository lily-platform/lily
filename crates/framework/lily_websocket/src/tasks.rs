//! Retained task joins. A waiter's lifetime is never the task's lifetime.
//! This module spawns no receipt drivers and never executes user futures while
//! reaping: it polls only actual Tokio JoinHandles.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::future::{BoxFuture, Shared};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tokio::task::{AbortHandle, Id, JoinError, JoinHandle};

type JoinResult<T> = Result<Arc<T>, Arc<JoinError>>;

pub(crate) struct TaskReceipt<T> {
    join: Shared<BoxFuture<'static, JoinResult<T>>>,
    abort: AbortHandle,
    abort_requested: Arc<AtomicBool>,
}

impl<T> Clone for TaskReceipt<T> {
    fn clone(&self) -> Self {
        Self {
            join: self.join.clone(),
            abort: self.abort.clone(),
            abort_requested: self.abort_requested.clone(),
        }
    }
}

impl<T: Send + Sync + 'static> From<JoinHandle<T>> for TaskReceipt<T> {
    fn from(task: JoinHandle<T>) -> Self {
        let abort = task.abort_handle();
        let join = async move { task.await.map(Arc::new).map_err(Arc::new) }
            .boxed()
            .shared();
        Self {
            join,
            abort,
            abort_requested: Default::default(),
        }
    }
}

impl<T> TaskReceipt<T> {
    pub(crate) fn abort(&self) {
        self.abort_requested.store(true, Ordering::Release);
        self.abort.abort();
    }
}

impl<T> Future for TaskReceipt<T> {
    type Output = JoinResult<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.join).poll(cx)
    }
}

#[derive(Clone, Copy)]
enum Joined {
    Completed,
    Cancelled,
    Panicked,
}

struct Entry {
    join: Shared<BoxFuture<'static, Joined>>,
    abort: AbortHandle,
    abort_requested: bool,
    request: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TaskSnapshot {
    pub(crate) outstanding: usize,
    pub(crate) completed: usize,
    pub(crate) cancelled: usize,
    pub(crate) panicked: usize,
    pub(crate) abort_requested: usize,
}

#[derive(Default)]
struct State {
    entries: HashMap<Id, Entry>,
    observed: TaskSnapshot,
}

impl State {
    fn reap(&mut self) {
        self.entries.retain(|_, entry| {
            if entry.request.load(Ordering::Acquire) && !entry.abort_requested {
                entry.abort_requested = true;
                self.observed.abort_requested += 1;
            }
            let Some(joined) = entry.join.clone().now_or_never() else {
                return true;
            };
            match joined {
                Joined::Completed => self.observed.completed += 1,
                Joined::Cancelled => self.observed.cancelled += 1,
                Joined::Panicked => self.observed.panicked += 1,
            }
            false
        });
    }
}

#[derive(Clone, Default)]
pub(crate) struct TaskRegistry(Arc<Mutex<State>>);

impl TaskRegistry {
    pub(crate) fn track<T: Send + Sync + 'static>(&self, task: JoinHandle<T>) -> TaskReceipt<T> {
        let id = task.id();
        let receipt = TaskReceipt::from(task);
        let observed = receipt.clone();
        let entry = Entry {
            abort: receipt.abort.clone(),
            abort_requested: false,
            request: receipt.abort_requested.clone(),
            join: async move {
                match observed.await {
                    Ok(_) => Joined::Completed,
                    Err(error) if error.is_cancelled() => Joined::Cancelled,
                    Err(_) => Joined::Panicked,
                }
            }
            .boxed()
            .shared(),
        };
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Amortized reaping also bounds completed receipts during normal use.
        state.reap();
        state.entries.insert(id, entry);
        receipt
    }

    pub(crate) fn snapshot(&self) -> TaskSnapshot {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reap();
        TaskSnapshot {
            outstanding: state.entries.len(),
            ..state.observed
        }
    }

    pub(crate) fn abort_all(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reap();
        let mut requested = 0;
        for entry in state.entries.values_mut() {
            if !entry.abort_requested {
                entry.abort_requested = true;
                entry.request.store(true, Ordering::Release);
                entry.abort.abort();
                requested += 1;
            }
        }
        state.observed.abort_requested += requested;
    }

    /// Cancellation-safe. The registry keeps every handle if this wait drops.
    /// Producers must be stopped before using an empty snapshot as a barrier.
    pub(crate) async fn wait(&self) {
        loop {
            let receipts = {
                let mut state = self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.reap();
                state
                    .entries
                    .values()
                    .map(|entry| entry.join.clone())
                    .collect::<Vec<_>>()
            };
            if receipts.is_empty() {
                return;
            }
            for receipt in receipts {
                let _ = receipt.await;
            }
        }
    }
}

/// Local completion stream backed by a separately retained registry. Dropping
/// the stream neither aborts the tasks nor loses their actual join receipts.
pub(crate) struct OwnedTaskSet {
    registry: TaskRegistry,
    pending: FuturesUnordered<TaskReceipt<()>>,
}

impl OwnedTaskSet {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::with_registry(TaskRegistry::default())
    }

    pub(crate) fn with_registry(registry: TaskRegistry) -> Self {
        Self {
            registry,
            pending: FuturesUnordered::new(),
        }
    }

    pub(crate) fn spawn(&mut self, future: impl Future<Output = ()> + Send + 'static) {
        self.pending.push(self.registry.track(tokio::spawn(future)));
    }

    pub(crate) fn len(&self) -> usize {
        self.pending.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
    pub(crate) fn abort_all(&self) {
        self.registry.abort_all();
    }
    pub(crate) async fn join_next(&mut self) -> Option<Result<(), Arc<JoinError>>> {
        self.pending.next().await.map(|result| result.map(|_| ()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn dropping_a_local_waiter_preserves_cooperative_execution_and_the_join() {
        let registry = TaskRegistry::default();
        let cancel = CancellationToken::new();
        let observed = Arc::new(AtomicBool::new(false));
        let mut local = OwnedTaskSet::with_registry(registry.clone());
        let token = cancel.clone();
        let flag = observed.clone();
        local.spawn(async move {
            token.cancelled().await;
            flag.store(true, Ordering::Release);
        });
        drop(local);
        assert_eq!(registry.snapshot().outstanding, 1);
        assert!(!observed.load(Ordering::Acquire));
        cancel.cancel();
        registry.wait().await;
        assert!(observed.load(Ordering::Acquire));
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.abort_requested, 0);
        assert_eq!(snapshot.outstanding, 0);
    }

    #[tokio::test]
    async fn abort_before_first_poll_is_not_confirmed_until_join_and_is_replayable() {
        let registry = TaskRegistry::default();
        let task = registry.track(tokio::spawn(std::future::pending::<()>()));
        registry.abort_all();
        let requested = registry.snapshot();
        assert_eq!(requested.abort_requested, 1);
        assert_eq!(requested.outstanding, 1);
        assert_eq!(requested.cancelled, 0);
        assert!(task.clone().await.unwrap_err().is_cancelled());
        registry.wait().await;
        assert!(task.await.unwrap_err().is_cancelled());
        registry.abort_all();
        let joined = registry.snapshot();
        assert_eq!(joined.abort_requested, 1);
        assert_eq!(joined.cancelled, 1);
        assert_eq!(joined.outstanding, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocked_drop_keeps_the_real_join_owned_after_the_absolute_deadline() {
        struct BlockingDrop(
            std::sync::mpsc::Receiver<()>,
            tokio::sync::oneshot::Sender<()>,
        );
        impl Drop for BlockingDrop {
            fn drop(&mut self) {
                // The test releases this destructor explicitly. The timeout
                // is only a fail-safe against a broken assertion hanging CI.
                let (replacement, _) = tokio::sync::oneshot::channel();
                let _ = std::mem::replace(&mut self.1, replacement).send(());
                self.0.recv_timeout(Duration::from_secs(2)).unwrap();
            }
        }
        let registry = TaskRegistry::default();
        let (release, blocked) = std::sync::mpsc::channel();
        let (dropping, entered_drop) = tokio::sync::oneshot::channel();
        let (started, entered_task) = tokio::sync::oneshot::channel();
        registry.track(tokio::spawn(async move {
            let _guard = BlockingDrop(blocked, dropping);
            let _ = started.send(());
            std::future::pending::<()>().await;
        }));
        entered_task.await.unwrap();
        registry.abort_all();
        entered_drop.await.unwrap();
        let budget = crate::shutdown::ShutdownBudget::default();
        let now = tokio::time::Instant::now();
        budget.configure(now, now + Duration::from_millis(10));
        assert!(budget.reconcile(registry.wait()).await.is_err());
        let unconfirmed = registry.snapshot();
        release.send(()).unwrap();
        registry.wait().await;
        assert_eq!(unconfirmed.outstanding, 1);
        assert_eq!(unconfirmed.cancelled, 0);
        assert_eq!(registry.snapshot().cancelled, 1);
        assert_eq!(registry.snapshot().outstanding, 0);
    }

    #[tokio::test]
    async fn successful_and_panicked_tasks_are_reaped_without_background_drivers() {
        let registry = TaskRegistry::default();
        let completed = registry.track(tokio::spawn(async { 42 }));
        let panicked = registry.track(tokio::spawn(async { panic!("task probe") }));
        assert_eq!(*completed.await.unwrap(), 42);
        assert!(panicked.await.unwrap_err().is_panic());
        registry.wait().await;
        for _ in 0..3 {
            let snapshot = registry.snapshot();
            assert_eq!(snapshot.completed, 1);
            assert_eq!(snapshot.panicked, 1);
            assert_eq!(snapshot.outstanding, 0);
        }
    }
}
