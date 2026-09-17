//! Retained HTTP task joins. No receipt-driver tasks are spawned here.
//!
//! A publication gate prevents user/protocol work from being polled before
//! both its local owner and (where applicable) application inventory retain
//! the actual JoinHandle. Dropping a waiter never drops that inventory.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use futures::future::{BoxFuture, Shared};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::task::{AbortHandle, Id, JoinError, JoinHandle};

use crate::lifecycle::{TaskJoinEvidence, TaskJoinOutcome};

pub(crate) type TaskResult<T> = Result<Arc<T>, Arc<JoinError>>;

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) struct TaskReceipt<T> {
    join: Shared<BoxFuture<'static, TaskResult<T>>>,
    abort: AbortHandle,
    evidence: Arc<Mutex<TaskJoinEvidence>>,
}

impl<T> Clone for TaskReceipt<T> {
    fn clone(&self) -> Self {
        Self {
            join: self.join.clone(),
            abort: self.abort.clone(),
            evidence: self.evidence.clone(),
        }
    }
}

impl<T: Send + Sync + 'static> TaskReceipt<T> {
    /// Poll only the retained JoinHandle. Neither abort nor is_finished is join evidence.
    pub(crate) fn snapshot(&self) -> TaskSnapshot {
        let _ = self.clone().now_or_never();
        let evidence = lock(&self.evidence);
        TaskSnapshot {
            registered: 1,
            completed: usize::from(matches!(
                evidence.outcome(),
                Some(TaskJoinOutcome::Completed | TaskJoinOutcome::Failed)
            )),
            cancelled: usize::from(evidence.outcome() == Some(TaskJoinOutcome::Cancelled)),
            panicked: usize::from(evidence.outcome() == Some(TaskJoinOutcome::Panicked)),
            outstanding: usize::from(!evidence.is_terminal()),
            abort_requested: usize::from(evidence.abort_requested()),
            rejected: 0,
        }
    }

    fn new(task: JoinHandle<T>) -> Self {
        let abort = task.abort_handle();
        let evidence = Arc::new(Mutex::new(TaskJoinEvidence::default()));
        let observed = evidence.clone();
        let join = async move {
            let result = task.await;
            let outcome = match &result {
                Ok(_) => TaskJoinOutcome::Completed,
                Err(error) if error.is_cancelled() => TaskJoinOutcome::Cancelled,
                Err(_) => TaskJoinOutcome::Panicked,
            };
            lock(&observed).observe_join(outcome);
            result.map(Arc::new).map_err(Arc::new)
        }
        .boxed()
        .shared();
        Self {
            join,
            abort,
            evidence,
        }
    }
}

impl<T> TaskReceipt<T> {
    pub(crate) fn abort(&self) {
        if lock(&self.evidence).request_abort() {
            self.abort.abort();
        }
    }
}

impl<T> Future for TaskReceipt<T> {
    type Output = TaskResult<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.join).poll(context)
    }
}

struct Entry {
    join: Shared<BoxFuture<'static, TaskJoinOutcome>>,
    abort: AbortHandle,
    evidence: Arc<Mutex<TaskJoinEvidence>>,
    abort_counted: bool,
}

impl Entry {
    fn new<T: Send + Sync + 'static>(receipt: &TaskReceipt<T>) -> Self {
        let observed = receipt.clone();
        let evidence = receipt.evidence.clone();
        Self {
            join: async move {
                let _ = observed.await;
                lock(&evidence)
                    .outcome()
                    .expect("an awaited task has join evidence")
            }
            .boxed()
            .shared(),
            abort: receipt.abort.clone(),
            evidence: receipt.evidence.clone(),
            abort_counted: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TaskSnapshot {
    pub(crate) registered: usize,
    pub(crate) completed: usize,
    pub(crate) cancelled: usize,
    pub(crate) panicked: usize,
    pub(crate) outstanding: usize,
    pub(crate) abort_requested: usize,
    pub(crate) rejected: usize,
}

impl TaskSnapshot {
    pub(crate) fn include(&mut self, other: Self) {
        self.registered += other.registered;
        self.completed += other.completed;
        self.cancelled += other.cancelled;
        self.panicked += other.panicked;
        self.outstanding += other.outstanding;
        self.abort_requested += other.abort_requested;
        self.rejected += other.rejected;
    }

    pub(crate) fn reconciles(self) -> bool {
        self.registered == self.completed + self.cancelled + self.panicked + self.outstanding
    }

    pub(crate) fn is_terminal(self) -> bool {
        self.reconciles() && self.outstanding == 0
    }
}

#[derive(Default)]
struct State {
    entries: HashMap<Id, Entry>,
    counts: TaskSnapshot,
    sealed: bool,
}

impl State {
    fn reap(&mut self) {
        self.entries.retain(|_, entry| {
            if lock(&entry.evidence).abort_requested() && !entry.abort_counted {
                entry.abort_counted = true;
                self.counts.abort_requested += 1;
            }
            // Polls the real JoinHandle only, never the task's user future.
            let Some(joined) = entry.join.clone().now_or_never() else {
                return true;
            };
            match joined {
                TaskJoinOutcome::Completed | TaskJoinOutcome::Failed => self.counts.completed += 1,
                TaskJoinOutcome::Cancelled => self.counts.cancelled += 1,
                TaskJoinOutcome::Panicked => self.counts.panicked += 1,
            }
            false
        });
    }

    fn snapshot(&mut self) -> TaskSnapshot {
        self.reap();
        TaskSnapshot {
            outstanding: self.entries.len(),
            ..self.counts
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct TaskRegistry(Arc<Mutex<State>>);

impl TaskRegistry {
    pub(crate) fn spawn<T, F>(&self, future: F) -> TaskReceipt<T>
    where
        T: Send + Sync + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        self.try_spawn(future, None)
            .expect("task producer must retain an open registry")
    }

    /// The optional parent retains the same join before the gate opens. Only
    /// the parent counts this task in the application aggregate; the local
    /// inventory supplies a connection-specific barrier.
    pub(crate) fn try_spawn<T, F>(&self, future: F, parent: Option<&Self>) -> Option<TaskReceipt<T>>
    where
        T: Send + Sync + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let parent = parent.filter(|parent| !Arc::ptr_eq(&parent.0, &self.0));
        // All dual registrations acquire application then connection locks.
        let mut parent_state = parent.map(|parent| lock(&parent.0));
        let mut state = lock(&self.0);
        if state.sealed || parent_state.as_ref().is_some_and(|state| state.sealed) {
            state.counts.rejected += 1;
            if let Some(parent) = parent_state.as_mut() {
                parent.counts.rejected += 1;
            }
            return None;
        }
        state.reap();
        if let Some(parent) = parent_state.as_mut() {
            parent.reap();
        }

        let (published, registered) = tokio::sync::oneshot::channel();
        // Outstanding work retains its registry even if all external waiters
        // disappear. This cycle ends when the work's future is destroyed.
        let retained = (self.clone(), parent.cloned());
        let task = lily_trace::spawn(async move {
            let _retained = retained;
            registered
                .await
                .expect("task receipt publication must complete synchronously");
            future.await
        });
        let id = task.id();
        let receipt = TaskReceipt::new(task);
        state.entries.insert(id, Entry::new(&receipt));
        state.counts.registered += 1;
        if let Some(parent) = parent_state.as_mut() {
            parent.entries.insert(id, Entry::new(&receipt));
            parent.counts.registered += 1;
        }
        drop(state);
        drop(parent_state);
        let _ = published.send(());
        Some(receipt)
    }

    /// Give protocol adapters their exact task receipt before their first
    /// poll. Association follows the executing task, never spawn order or
    /// opaque Hyper future types. The factory runs inside the registered task.
    pub(crate) fn try_spawn_with_receipt<T, F, Factory>(
        &self,
        factory: Factory,
        parent: Option<&Self>,
    ) -> Option<TaskReceipt<T>>
    where
        T: Send + Sync + 'static,
        F: Future<Output = T> + Send + 'static,
        Factory: FnOnce(TaskReceipt<T>) -> F + Send + 'static,
    {
        let (publish, published) = tokio::sync::oneshot::channel();
        let receipt = self.try_spawn(
            async move {
                let receipt = published
                    .await
                    .expect("task identity published before polling");
                factory(receipt).await
            },
            parent,
        )?;
        let _ = publish.send(receipt.clone());
        Some(receipt)
    }

    /// Adopts an already spawned framework monitor before exposing its waiter.
    /// That monitor's own installer owns its pre-adoption execution boundary.
    pub(crate) fn adopt<T: Send + Sync + 'static>(&self, task: JoinHandle<T>) -> TaskReceipt<T> {
        let mut state = lock(&self.0);
        assert!(
            !state.sealed,
            "cannot adopt work after the producer has been sealed"
        );
        state.reap();
        let id = task.id();
        let receipt = TaskReceipt::new(task);
        state.entries.insert(id, Entry::new(&receipt));
        state.counts.registered += 1;
        receipt
    }

    pub(crate) fn seal(&self) {
        lock(&self.0).sealed = true;
    }

    pub(crate) fn snapshot(&self) -> TaskSnapshot {
        lock(&self.0).snapshot()
    }

    pub(crate) fn abort_all(&self) {
        let mut state = lock(&self.0);
        state.reap();
        for entry in state.entries.values() {
            if lock(&entry.evidence).request_abort() {
                entry.abort.abort();
            }
        }
    }

    /// Cancellation-safe join observation. The producer must be sealed before
    /// an empty inventory is used as a terminal barrier.
    pub(crate) async fn wait(&self) -> TaskSnapshot {
        loop {
            let pending = {
                let mut state = lock(&self.0);
                let snapshot = state.snapshot();
                if snapshot.outstanding == 0 {
                    return snapshot;
                }
                state
                    .entries
                    .values()
                    .map(|entry| entry.join.clone())
                    .collect::<Vec<_>>()
            };
            let mut pending = pending.into_iter().collect::<FuturesUnordered<_>>();
            while pending.next().await.is_some() {}
        }
    }
}

/// A local completion stream whose actual joins are also retained by the root.
pub(crate) struct TaskSet<T> {
    registry: TaskRegistry,
    pending: FuturesUnordered<TaskReceipt<T>>,
}

impl<T: Send + Sync + 'static> TaskSet<T> {
    pub(crate) fn new(registry: TaskRegistry) -> Self {
        Self {
            registry,
            pending: FuturesUnordered::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn spawn(&mut self, future: impl Future<Output = T> + Send + 'static) {
        self.pending.push(self.registry.spawn(future));
    }

    pub(crate) fn spawn_with_receipt<F, Factory>(&mut self, factory: Factory)
    where
        F: Future<Output = T> + Send + 'static,
        Factory: FnOnce(TaskReceipt<T>) -> F + Send + 'static,
    {
        self.pending.push(
            self.registry
                .try_spawn_with_receipt(factory, None)
                .expect("connection producer must retain an open registry"),
        );
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

    pub(crate) async fn join_next(&mut self) -> Option<TaskResult<T>> {
        self.pending.next().await
    }
}

impl<T> Drop for TaskSet<T> {
    fn drop(&mut self) {
        self.registry.abort_all();
    }
}

#[derive(Clone, Default)]
pub(crate) struct HttpTaskInventory {
    pub(crate) listener: TaskRegistry,
    pub(crate) connections: TaskRegistry,
    pub(crate) protocol: TaskRegistry,
    pub(crate) monitors: TaskRegistry,
}

impl HttpTaskInventory {
    pub(crate) fn transport_is_terminal(&self) -> bool {
        self.listener.snapshot().is_terminal()
            && self.connections.snapshot().is_terminal()
            && self.protocol.snapshot().is_terminal()
    }

    pub(crate) fn abort_transport(&self) {
        self.listener.abort_all();
        self.connections.abort_all();
        self.protocol.abort_all();
    }

    pub(crate) fn transport_panicked(&self) -> bool {
        self.listener.snapshot().panicked != 0
            || self.connections.snapshot().panicked != 0
            || self.protocol.snapshot().panicked != 0
    }
}

#[cfg(test)]
mod tests;
