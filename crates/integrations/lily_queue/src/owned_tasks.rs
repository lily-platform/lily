//! Retained real joins for transaction, heartbeat and relay owners.

use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    task::{AbortHandle, JoinError},
    time::Instant,
};

use crate::shutdown_budget::QueueShutdownBudget;

type JoinReceipt = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

#[derive(Clone)]
pub(crate) struct TaskReceipt {
    join: JoinReceipt,
    abort: AbortHandle,
}

impl TaskReceipt {
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }
    pub(crate) async fn join(&self) -> Result<(), Arc<JoinError>> {
        self.join.clone().await
    }
    pub(crate) fn terminal(&self) -> bool {
        self.join.clone().now_or_never().is_some()
    }
}

#[derive(Default)]
pub(crate) struct OwnedTasks {
    receipts: Mutex<Vec<TaskReceipt>>,
    panicked: AtomicBool,
}

impl OwnedTasks {
    /// Adoption precedes the first poll of user/driver work. No task is spawned
    /// merely to drive a receipt, and a cancelled waiter never takes its join.
    pub(crate) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> TaskReceipt {
        let mut receipts = self.receipts.lock().unwrap_or_else(|p| p.into_inner());
        self.reap_locked(&mut receipts);
        let (adopted, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            if ready.await.is_ok() {
                future.await;
            }
        });
        let receipt = TaskReceipt {
            abort: task.abort_handle(),
            join: async move { task.await.map_err(Arc::new) }.boxed().shared(),
        };
        receipts.push(receipt.clone());
        let _ = adopted.send(());
        receipt
    }

    fn reap_locked(&self, receipts: &mut Vec<TaskReceipt>) {
        receipts.retain(|receipt| match receipt.join.clone().now_or_never() {
            Some(Err(error)) => {
                if error.is_panic() {
                    self.panicked.store(true, Ordering::Release);
                }
                false
            }
            Some(Ok(())) => false,
            None => true,
        });
    }

    pub(crate) fn reconciled(&self) -> bool {
        let mut receipts = self.receipts.lock().unwrap_or_else(|p| p.into_inner());
        self.reap_locked(&mut receipts);
        receipts.is_empty()
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub(crate) fn record_panic(&self) {
        self.panicked.store(true, Ordering::Release);
    }

    pub(crate) fn panicked(&self) -> bool {
        self.panicked.load(Ordering::Acquire)
    }

    pub(crate) async fn drain_before(
        &self,
        budget: &QueueShutdownBudget,
        deadline: Instant,
    ) -> bool {
        // A transaction can publish a heartbeat while its own join is pending.
        // Re-snapshot after each batch; never equate a snapshot with a seal.
        loop {
            if self.reconciled() {
                return true;
            }
            let receipts = self
                .receipts
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            for receipt in receipts {
                if !receipt.terminal() && budget.run_until(deadline, receipt.join()).await.is_err()
                {
                    return self.reconciled();
                }
            }
        }
    }
}

/// Owns transaction execution/finalization separately from its result waiter.
/// A force request installs a cutoff; it does not drop cooperative execution.
#[derive(Default)]
pub(crate) struct TransactionTasks {
    pub(crate) tasks: OwnedTasks,
    pub(crate) budget: QueueShutdownBudget,
    forced_at: Mutex<Option<Instant>>,
    forced: tokio_util::sync::CancellationToken,
    pub(crate) interrupted: AtomicBool,
}

impl TransactionTasks {
    pub(crate) fn request_force(&self) {
        self.forced_at
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_or_insert_with(Instant::now);
        self.forced.cancel();
    }

    pub(crate) async fn run<F: Future>(
        &self,
        local: Option<Instant>,
        future: F,
    ) -> Option<F::Output> {
        tokio::pin!(future);
        loop {
            let revision = self.budget.revision();
            let root = self.budget.deadlines();
            let forced_at = *self.forced_at.lock().unwrap_or_else(|p| p.into_inner());
            let forced_at = forced_at.or_else(|| {
                root.filter(|r| Instant::now() >= r.graceful())
                    .map(|r| r.graceful())
            });
            let cutoff = local
                .into_iter()
                .chain(root.map(|r| r.hard()))
                .chain(forced_at.map(|at| self.budget.forced_delivery_deadline(at)))
                .min();
            if cutoff.is_some_and(|cutoff| Instant::now() >= cutoff) {
                self.interrupted.store(true, Ordering::Release);
                return None;
            }
            let wake = cutoff
                .into_iter()
                .chain(root.filter(|_| forced_at.is_none()).map(|r| r.graceful()))
                .min();
            tokio::select! {
                biased;
                result = future.as_mut() => return Some(result),
                _ = async { match wake { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {},
                _ = self.budget.changed_since(revision) => {},
                _ = self.forced.cancelled(), if forced_at.is_none() => {},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shutdown_budget::QueueShutdownDeadlines;
    use std::{future::pending, sync::atomic::AtomicUsize, time::Duration};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn cancelled_drain_keeps_the_original_join_and_zero_active_is_not_terminal() {
        let tasks = Arc::new(OwnedTasks::default());
        let release = CancellationToken::new();
        let entered = CancellationToken::new();
        let active = Arc::new(AtomicUsize::new(1));
        let child_active = active.clone();
        let child_entered = entered.clone();
        let child_release = release.clone();
        tasks.spawn(async move {
            child_active.store(0, Ordering::Release);
            child_entered.cancel();
            child_release.cancelled().await;
        });
        entered.cancelled().await;
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert!(!tasks.reconciled());
        let waiter_tasks = tasks.clone();
        let waiter = tokio::spawn(async move {
            waiter_tasks
                .drain_before(
                    &QueueShutdownBudget::default(),
                    Instant::now() + Duration::from_secs(10),
                )
                .await
        });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(!tasks.reconciled());
        release.cancel();
        assert!(
            tasks
                .drain_before(
                    &QueueShutdownBudget::default(),
                    Instant::now() + Duration::from_secs(1)
                )
                .await
        );
    }

    #[tokio::test]
    async fn abort_request_requires_a_real_join_and_panic_is_retained() {
        let tasks = OwnedTasks::default();
        let receipt = tasks.spawn(pending());
        receipt.abort();
        assert!(
            !receipt.terminal(),
            "abort alone cannot resolve the join on this current-thread runtime"
        );
        assert!(receipt.join().await.unwrap_err().is_cancelled());
        assert!(tasks.reconciled());
        assert!(!tasks.panicked());
        let panic = tasks.spawn(async {
            panic!("task panic qualification");
        });
        assert!(panic.join().await.unwrap_err().is_panic());
        assert!(tasks.reconciled());
        assert!(tasks.panicked());
    }

    #[tokio::test]
    async fn parent_abort_retains_and_joins_its_heartbeat() {
        struct Heartbeat(TaskReceipt);
        impl Drop for Heartbeat {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let tasks = Arc::new(OwnedTasks::default());
        let ready = CancellationToken::new();
        let child_tasks = tasks.clone();
        let child_ready = ready.clone();
        let parent = tasks.spawn(async move {
            let _heartbeat = Heartbeat(child_tasks.spawn(pending()));
            child_ready.cancel();
            pending::<()>().await;
        });
        ready.cancelled().await;
        assert_eq!(tasks.receipts.lock().unwrap().len(), 2);
        parent.abort();
        assert!(parent.join().await.unwrap_err().is_cancelled());
        assert!(
            tasks
                .drain_before(
                    &QueueShutdownBudget::default(),
                    Instant::now() + Duration::from_secs(1)
                )
                .await
        );
        assert!(tasks.receipts.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn force_does_not_replace_a_cooperatively_completed_result() {
        let owners = Arc::new(TransactionTasks::default());
        let started = Instant::now();
        owners.budget.install(QueueShutdownDeadlines::before(
            started,
            started + Duration::from_secs(2),
        ));
        owners.request_force();
        let retained = owners.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = owners.tasks.spawn(async move {
            let result = retained
                .run(None, async {
                    tokio::time::sleep(Duration::from_millis(125)).await;
                    "committed"
                })
                .await;
            let _ = tx.send(result);
        });
        let result = rx.await.unwrap();
        task.join().await.unwrap();
        assert!(owners.tasks.reconciled());
        assert_eq!(result, Some("committed"));
        assert_eq!(Instant::now() - started, Duration::from_millis(125));
        assert!(!owners.interrupted.load(Ordering::Acquire));
    }

    #[tokio::test(start_paused = true)]
    async fn a_later_root_shortens_the_same_pending_owner_without_renewing_force() {
        let owners = Arc::new(TransactionTasks::default());
        let started = Instant::now();
        owners.request_force();
        let entered = CancellationToken::new();
        let child_entered = entered.clone();
        let child = owners.clone();
        let task = tokio::spawn(async move {
            child
                .run(None, async move {
                    child_entered.cancel();
                    pending::<()>().await;
                })
                .await
        });
        entered.cancelled().await;
        tokio::time::advance(Duration::from_millis(200)).await;
        owners.request_force();
        owners.budget.install(QueueShutdownDeadlines::before(
            started,
            started + Duration::from_millis(400),
        ));
        assert_eq!(task.await.unwrap(), None);
        assert_eq!(Instant::now() - started, Duration::from_millis(300));
        assert!(owners.interrupted.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn completed_receipts_do_not_accumulate_per_transaction() {
        let tasks = OwnedTasks::default();
        for _ in 0..200 {
            tasks.spawn(async {}).join().await.unwrap();
            assert_eq!(tasks.receipts.lock().unwrap().len(), 1);
        }
        assert!(tasks.reconciled());
        assert!(tasks.receipts.lock().unwrap().is_empty());
    }
}
