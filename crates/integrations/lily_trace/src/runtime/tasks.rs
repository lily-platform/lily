//! Actual worker receipts retained independently of shutdown waiters.

use std::future::Future;
use std::sync::{Mutex, OnceLock};

use futures_util::future::{BoxFuture, Shared};
use futures_util::FutureExt;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub(crate) type Receipt<T> = Shared<BoxFuture<'static, Result<T, String>>>;

pub(crate) struct AsyncWorker<T> {
    pub(crate) receipt: Receipt<T>,
    pub(crate) abort: tokio::task::AbortHandle,
}

impl<T: Clone + Send + Sync + 'static> AsyncWorker<T> {
    pub(crate) fn new(name: &'static str, task: JoinHandle<T>) -> Self {
        Self {
            abort: task.abort_handle(),
            receipt: adopt(name, task),
        }
    }
}

type WorkerEntries = Mutex<Vec<(&'static str, Receipt<()>)>>;

fn entries() -> &'static WorkerEntries {
    static ENTRIES: OnceLock<WorkerEntries> = OnceLock::new();
    ENTRIES.get_or_init(Mutex::default)
}

pub(crate) fn retain<T: Clone + Send + Sync + 'static>(
    name: &'static str,
    receipt: Receipt<T>,
) -> Receipt<T> {
    let observer = receipt.clone();
    entries().lock().unwrap_or_else(|p| p.into_inner()).push((
        name,
        async move { observer.await.map(|_| ()) }.boxed().shared(),
    ));
    receipt
}

pub(crate) fn adopt<T: Clone + Send + Sync + 'static>(
    name: &'static str,
    task: JoinHandle<T>,
) -> Receipt<T> {
    retain(
        name,
        async move { task.await.map_err(|e| e.to_string()) }
            .boxed()
            .shared(),
    )
}

/// Cancellation affects the observer only. A ready real receipt is consumable
/// at an elapsed cutoff; an unstarted user callback is never passed here.
pub(crate) async fn observe_before<F: Future>(deadline: Instant, receipt: F) -> Option<F::Output> {
    tokio::pin!(receipt);
    if let Some(result) = receipt.as_mut().now_or_never() {
        return Some(result);
    }
    tokio::time::timeout_at(deadline, receipt).await.ok()
}

/// Worker termination evidence, separate from exporter success and timeout.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TracingWorkerSnapshot {
    pub registered: usize,
    pub joined: usize,
    pub failed: usize,
    pub outstanding: usize,
}

impl TracingWorkerSnapshot {
    pub fn is_terminal(self) -> bool {
        self.registered == self.joined + self.outstanding && self.outstanding == 0
    }
}

pub(crate) fn snapshot() -> TracingWorkerSnapshot {
    let receipts = entries().lock().unwrap_or_else(|p| p.into_inner()).clone();
    let mut result = TracingWorkerSnapshot {
        registered: receipts.len(),
        ..Default::default()
    };
    for (_, receipt) in receipts {
        // Synchronous diagnostics outside Tokio must not construct a timer in
        // an unobserved OS-thread receipt. Unknown joins remain outstanding.
        let joined = if tokio::runtime::Handle::try_current().is_ok() {
            receipt.now_or_never()
        } else {
            receipt.peek().cloned()
        };
        match joined {
            Some(Ok(())) => result.joined += 1,
            Some(Err(_)) => {
                result.joined += 1;
                result.failed += 1;
            }
            None => result.outstanding += 1,
        }
    }
    result
}

pub(crate) async fn reconcile_before(deadline: Instant) -> TracingWorkerSnapshot {
    let receipts = entries().lock().unwrap_or_else(|p| p.into_inner()).clone();
    for (_, receipt) in receipts {
        let _ = observe_before(deadline, receipt).await;
    }
    snapshot()
}

/// Retain the real OS thread handle from creation. No detached waiter or
/// blocking-pool job is needed to observe it. Wait for the native completion
/// flag before joining. Arbitrary thread-local destructors, like other
/// synchronous user code, remain outside the preemption guarantee.
pub(crate) fn thread_receipt<T: Clone + Send + Sync + 'static>(
    name: &'static str,
    task: std::thread::JoinHandle<T>,
) -> Receipt<T> {
    async move {
        while !task.is_finished() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        task.join()
            .map_err(|_| format!("{name} worker thread panicked"))
    }
    .boxed()
    .shared()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn blocking_worker_timeout_keeps_its_real_join_until_release() {
        let (release, wait) = std::sync::mpsc::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let finished = dropped.clone();
        let task = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
            finished.store(true, Ordering::Release);
        });
        let abort = task.abort_handle();
        let receipt = adopt("test-blocking", task);
        ready.await.unwrap();
        abort.abort();
        assert!(observe_before(Instant::now(), receipt.clone())
            .await
            .is_none());
        assert!(!dropped.load(Ordering::Acquire));
        drop(receipt);
        let retained = entries()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .find(|(name, _)| *name == "test-blocking")
            .unwrap()
            .1
            .clone();
        assert!(retained.clone().now_or_never().is_none());
        release.send(()).unwrap();
        retained.await.unwrap();
        assert!(dropped.load(Ordering::Acquire));
    }
}
