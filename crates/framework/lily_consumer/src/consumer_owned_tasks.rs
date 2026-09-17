//! Actual joins retained independently of public runtime/rollback waiters.

use futures_util::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use lily_error::application::consumer::ConsumerError;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::task::{AbortHandle, JoinError, JoinHandle};

type Receipt<T> = Shared<BoxFuture<'static, Result<T, Arc<JoinError>>>>;
type Pending = Shared<BoxFuture<'static, ()>>;

// Only live/unobserved joins are kept. Reaped at adoption and observation;
// this is per application attempt, never a per-delivery history or driver task.
struct PendingTask {
    #[cfg_attr(not(test), allow(dead_code))]
    id: tokio::task::Id,
    join: Pending,
}

static PENDING: Mutex<Vec<PendingTask>> = Mutex::new(Vec::new());

#[derive(Clone)]
pub(super) struct OwnedTask<T: Clone> {
    receipt: Receipt<T>,
    #[cfg_attr(not(any(test, feature = "fuzzing")), allow(dead_code))]
    abort: AbortHandle,
}

impl<T: Clone + Send + Sync + 'static> From<JoinHandle<T>> for OwnedTask<T> {
    fn from(task: JoinHandle<T>) -> Self {
        Self::map(task, |output| output)
    }
}

impl<T: Clone + Send + Sync + 'static> OwnedTask<T> {
    pub(super) fn map<R: Send + 'static>(
        task: JoinHandle<R>,
        map: impl FnOnce(R) -> T + Send + 'static,
    ) -> Self {
        let abort = task.abort_handle();
        let receipt = async move { task.await.map(map).map_err(Arc::new) }
            .boxed()
            .shared();
        let mut pending = PENDING.lock().unwrap_or_else(|p| p.into_inner());
        pending.retain(|receipt| receipt.join.clone().now_or_never().is_none());
        pending.push(PendingTask {
            id: abort.id(),
            join: receipt.clone().map(|_| ()).boxed().shared(),
        });
        Self { receipt, abort }
    }
}

impl<T: Clone> OwnedTask<T> {
    #[cfg(any(test, feature = "fuzzing"))]
    pub(super) fn abort_handle(&self) -> AbortHandle {
        self.abort.clone()
    }
}

impl<T: Clone + Send + Sync + 'static> Future for OwnedTask<T> {
    type Output = Result<T, Arc<JoinError>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = Pin::new(&mut self.receipt).poll(cx);
        if result.is_ready() {
            PENDING
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .retain(|receipt| receipt.join.clone().now_or_never().is_none());
        }
        result
    }
}

pub(super) fn join_failure(error: impl Into<Arc<JoinError>>) -> ConsumerError {
    ConsumerError::managed_task_shared(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn dropping_every_cleanup_waiter_keeps_a_recoverable_real_join() {
        let entered = CancellationToken::new();
        let release = CancellationToken::new();
        let child_entered = entered.clone();
        let child_release = release.clone();
        let task = OwnedTask::from(tokio::spawn(async move {
            child_entered.cancel();
            child_release.cancelled().await;
            17_u32
        }));
        let id = task.abort_handle().id();
        entered.cancelled().await;
        drop(task);
        let retained = PENDING
            .lock()
            .unwrap()
            .iter()
            .find(|p| p.id == id)
            .unwrap()
            .join
            .clone();
        assert!(retained.clone().now_or_never().is_none());
        release.cancel();
        retained.await;
        // The next adoption reaps completed attempts, without a cleanup driver task.
        OwnedTask::from(tokio::spawn(async {})).await.unwrap();
        assert!(!PENDING.lock().unwrap().iter().any(|p| p.id == id));
    }

    #[tokio::test]
    async fn abort_is_confirmed_by_join_and_same_panic_receipt_can_be_replayed() {
        let task = OwnedTask::from(tokio::spawn(std::future::pending::<()>()));
        task.abort_handle().abort();
        assert!(task.clone().now_or_never().is_none());
        assert!(task.await.unwrap_err().is_cancelled());
        let task = OwnedTask::from(tokio::spawn(async {
            panic!("private panic body");
        }));
        let first = task.clone().await.unwrap_err();
        let second = task.await.unwrap_err();
        assert!(first.is_panic());
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            join_failure(first).error_code(),
            "CONSUMER_RUNTIME_TASK_PANICKED"
        );
    }
}
