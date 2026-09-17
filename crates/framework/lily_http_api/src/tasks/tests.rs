use super::*;
use crate::shutdown::{ShutdownBudget, ShutdownStage};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_poll_sees_both_local_and_application_receipts() {
    let local = TaskRegistry::default();
    let parent = TaskRegistry::default();
    let owner = local.clone();
    let application = parent.clone();
    let receipt = local
        .try_spawn(
            async move {
                assert_eq!(owner.snapshot().registered, 1);
                assert_eq!(application.snapshot().registered, 1);
            },
            Some(&parent),
        )
        .unwrap();
    receipt.await.unwrap();
    local.seal();
    parent.seal();
    assert_eq!(local.wait().await.completed, 1);
    assert_eq!(parent.wait().await.completed, 1);
}

#[tokio::test]
async fn dropping_every_local_waiter_does_not_cancel_or_detach_work() {
    let registry = TaskRegistry::default();
    let release = CancellationToken::new();
    let token = release.clone();
    let receipt = registry.spawn(async move { token.cancelled().await });
    drop(receipt);
    let pending = registry.snapshot();
    assert_eq!(pending.outstanding, 1);
    assert_eq!(pending.abort_requested, 0);
    registry.seal();
    release.cancel();
    let joined = registry.wait().await;
    assert!(joined.is_terminal());
    assert_eq!(joined.completed, 1);
}

#[tokio::test]
async fn abort_before_first_poll_stays_outstanding_until_the_actual_join() {
    let registry = TaskRegistry::default();
    let polled = Arc::new(AtomicBool::new(false));
    let started = polled.clone();
    let receipt = registry.spawn(async move {
        started.store(true, Ordering::SeqCst);
        std::future::pending::<()>().await;
    });
    receipt.abort();
    registry.abort_all();
    let requested = registry.snapshot();
    assert_eq!(requested.abort_requested, 1);
    assert_eq!(requested.cancelled, 0);
    assert_eq!(requested.outstanding, 1);
    assert!(receipt.clone().await.unwrap_err().is_cancelled());
    assert!(receipt.await.unwrap_err().is_cancelled());
    assert!(!polled.load(Ordering::SeqCst));
    registry.seal();
    registry.abort_all();
    let joined = registry.wait().await;
    assert_eq!(joined.abort_requested, 1);
    assert_eq!(joined.cancelled, 1);
    assert!(joined.is_terminal());
}

#[tokio::test]
async fn dropping_completion_stream_retains_cancelled_task_receipts() {
    let registry = TaskRegistry::default();
    let mut stream = TaskSet::new(registry.clone());
    stream.spawn(std::future::pending::<()>());
    drop(stream);
    assert_eq!(registry.snapshot().outstanding, 1);
    registry.seal();
    let joined = registry.wait().await;
    assert_eq!(joined.cancelled, 1);
    assert_eq!(joined.abort_requested, 1);
}

#[tokio::test]
async fn sealed_parent_rejects_unstarted_children_without_spawning() {
    let local = TaskRegistry::default();
    let parent = TaskRegistry::default();
    parent.seal();
    let polled = Arc::new(AtomicBool::new(false));
    let flag = polled.clone();
    assert!(local
        .try_spawn(
            async move {
                flag.store(true, Ordering::SeqCst);
            },
            Some(&parent)
        )
        .is_none());
    assert!(!polled.load(Ordering::SeqCst));
    for registry in [&local, &parent] {
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.registered, 0);
        assert_eq!(snapshot.rejected, 1);
        assert!(snapshot.is_terminal());
    }
}

#[tokio::test]
async fn joined_panic_and_returned_error_survive_reaping_and_replay() {
    let registry = TaskRegistry::default();
    let panicked = registry.spawn(async { panic!("expected task receipt test panic") });
    assert!(panicked.clone().await.unwrap_err().is_panic());
    let returned_error = registry.spawn(async { Err::<(), _>("operation failed") });
    assert_eq!(
        returned_error.clone().await.unwrap().as_ref(),
        &Err("operation failed")
    );
    registry.seal();
    let snapshot = registry.wait().await;
    assert_eq!(snapshot.panicked, 1);
    // Joined normally is not the success of the task's returned Result.
    assert_eq!(snapshot.completed, 1);
    assert!(snapshot.is_terminal());
    assert!(panicked.await.unwrap_err().is_panic());
    assert_eq!(
        returned_error.await.unwrap().as_ref(),
        &Err("operation failed")
    );
    assert_eq!(registry.snapshot(), snapshot);
}

#[tokio::test]
async fn concurrent_waiters_and_retirement_count_each_task_once() {
    let registry = TaskRegistry::default();
    let mut waiters = FuturesUnordered::new();
    for _ in 0..64 {
        let receipt = registry.spawn(async {});
        for _ in 0..4 {
            waiters.push(receipt.clone());
        }
    }
    registry.seal();
    while let Some(result) = waiters.next().await {
        result.unwrap();
    }
    registry.wait().await;
    let snapshot = registry.snapshot();
    assert_eq!(snapshot.registered, 64);
    assert_eq!(snapshot.completed, 64);
    assert_eq!(snapshot.abort_requested, 0);
    assert!(snapshot.is_terminal());
    assert!(lock(&registry.0).entries.is_empty());
}

struct BlockingDrop {
    entered: Option<oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}

impl Drop for BlockingDrop {
    fn drop(&mut self) {
        let _ = self.entered.take().unwrap().send(());
        let _ = self.release.recv();
    }
}

struct ReleaseOnDrop(std::sync::mpsc::Sender<()>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_destructor_preserves_the_join_after_the_root_cutoff() {
    let registry = TaskRegistry::default();
    let (started, active) = oneshot::channel();
    let (entered, dropping) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let release = ReleaseOnDrop(release);
    let task = registry.spawn(async move {
        let _drop = BlockingDrop {
            entered: Some(entered),
            release: blocked,
        };
        let _ = started.send(());
        std::future::pending::<()>().await;
    });
    tokio::time::timeout(Duration::from_secs(5), active)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    tokio::time::timeout(Duration::from_secs(5), dropping)
        .await
        .unwrap()
        .unwrap();
    let budget = ShutdownBudget::new(Duration::ZERO);
    budget.begin();
    assert!(budget
        .wait_for_receipt(ShutdownStage::Final, task.clone())
        .await
        .is_err());
    let pending = registry.snapshot();
    assert_eq!(pending.abort_requested, 1);
    assert_eq!(pending.cancelled, 0);
    assert_eq!(pending.outstanding, 1);
    drop(release);
    assert!(task.await.unwrap_err().is_cancelled());
    registry.seal();
    assert_eq!(registry.wait().await.cancelled, 1);
}
