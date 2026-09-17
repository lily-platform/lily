//! Shared absolute limits. Updating a limit may shorten it, never restart it.
//! Timers are polled by their owners; this module spawns no timer/cleanup tasks.

use std::future::Future;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

const COOPERATIVE_CAP: Duration = crate::extractor::MESSAGE_COOPERATIVE_CAP;
const RECONCILIATION_RESERVE: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, Default)]
struct Limits {
    graceful: Option<Instant>,
    hard: Option<Instant>,
    execution: Option<Instant>,
    transport: Option<Instant>,
    cleanup: Option<Instant>,
}

#[derive(Clone, Debug)]
pub(crate) struct ShutdownBudget {
    limits: watch::Sender<Limits>,
}

impl Default for ShutdownBudget {
    fn default() -> Self {
        Self {
            limits: watch::channel(Limits::default()).0,
        }
    }
}

#[derive(Clone, Copy)]
enum Boundary {
    Execution,
    Transport,
    Cleanup,
    Final,
}

fn earlier(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

impl ShutdownBudget {
    pub(crate) fn configure(&self, graceful: Instant, hard: Instant) {
        self.limits.send_modify(|limits| {
            limits.graceful = earlier(limits.graceful, Some(graceful.min(hard)));
            limits.hard = earlier(limits.hard, Some(hard));
        });
    }

    /// Called before publishing execution cancellation. The entire cooperative
    /// window and cleanup tail fit inside the coordinator's effective cap.
    pub(crate) fn force_before(&self, deadline: Instant) {
        let now = Instant::now();
        self.limits.send_modify(|limits| {
            let hard = limits.hard.map_or(deadline, |root| root.min(deadline));
            let remaining = hard.saturating_duration_since(now);
            limits.hard = Some(hard);
            limits.execution = earlier(
                limits.execution,
                Some(now + (remaining / 4).min(COOPERATIVE_CAP)),
            );
            // Leave a tail for connection lifecycle cleanup after message
            // execution/unwind and transport destruction have been observed.
            limits.transport = earlier(limits.transport, Some(now + remaining * 2 / 3));
            limits.cleanup = earlier(
                limits.cleanup,
                Some(hard - (remaining / 4).min(RECONCILIATION_RESERVE)),
            );
        });
    }

    pub(crate) fn hard_deadline(&self) -> Option<Instant> {
        self.limits.borrow().hard
    }

    pub(crate) fn is_forced(&self) -> bool {
        self.limits.borrow().execution.is_some()
    }

    pub(crate) fn cooperative_deadline(&self) -> Instant {
        let limits = *self.limits.borrow();
        let local = Instant::now() + COOPERATIVE_CAP;
        earlier(earlier(limits.execution, limits.hard), Some(local)).unwrap()
    }

    pub(crate) fn cleanup_deadline(&self, local: Instant) -> Instant {
        let limits = *self.limits.borrow();
        earlier(earlier(limits.cleanup, limits.hard), Some(local)).unwrap()
    }

    async fn wait(&self, boundary: Boundary, local: Option<Instant>) {
        let mut changes = self.limits.subscribe();
        loop {
            let limits = *changes.borrow_and_update();
            let phase = match boundary {
                Boundary::Execution => limits.execution,
                Boundary::Transport => limits.transport,
                Boundary::Cleanup => limits.cleanup,
                Boundary::Final => limits.hard,
            };
            let deadline = earlier(earlier(phase, limits.hard), local);
            let timer = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                () = timer => return,
                _ = changes.changed() => {}
            }
        }
    }

    pub(crate) async fn execution_expired(&self, local: Instant) {
        self.wait(Boundary::Execution, Some(local)).await;
    }

    pub(crate) async fn execution_limit_expired(&self) {
        self.wait(Boundary::Execution, None).await;
    }

    pub(crate) async fn transport_expired(&self) {
        self.wait(Boundary::Transport, None).await;
    }

    pub(crate) async fn transport<F: Future>(&self, future: F) -> Result<F::Output, ()> {
        tokio::select! {
            biased;
            () = self.transport_expired() => Err(()),
            result = future => Ok(result),
        }
    }

    pub(crate) async fn final_expired(&self) {
        self.wait(Boundary::Final, None).await;
    }

    pub(crate) async fn cleanup<F: Future>(
        &self,
        local: Option<Instant>,
        future: F,
    ) -> Result<F::Output, ()> {
        tokio::select! {
            biased;
            () = self.wait(Boundary::Cleanup, local) => Err(()),
            result = future => Ok(result),
        }
    }

    pub(crate) async fn reconcile<F: Future>(&self, future: F) -> Result<F::Output, ()> {
        tokio::select! {
            biased;
            // Reconciliation polls framework receipts, never fresh user hooks.
            // Already-terminal evidence remains valid at the deadline.
            result = future => Ok(result),
            () = self.final_expired() => Err(()),
        }
    }

    pub(crate) async fn observe_cleanup<F: Future>(&self, receipt: F) -> Result<F::Output, ()> {
        tokio::select! {
            biased;
            result = receipt => Ok(result),
            () = self.wait(Boundary::Cleanup, None) => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    #[tokio::test(start_paused = true)]
    async fn force_splits_the_existing_cap_without_restarting_it() {
        let budget = ShutdownBudget::default();
        let start = Instant::now();
        budget.configure(
            start + Duration::from_millis(750),
            start + Duration::from_secs(1),
        );
        tokio::time::advance(Duration::from_millis(750)).await;
        budget.force_before(start + Duration::from_secs(1));
        let execution = budget.cooperative_deadline();
        let cleanup = budget.cleanup_deadline(start + Duration::from_secs(10));
        assert_eq!(execution, start + Duration::from_micros(812_500));
        assert_eq!(cleanup, start + Duration::from_millis(975));
        tokio::time::advance(Duration::from_millis(20)).await;
        budget.configure(
            start + Duration::from_secs(3),
            start + Duration::from_secs(4),
        );
        budget.force_before(start + Duration::from_secs(4));
        assert_eq!(budget.cooperative_deadline(), execution);
        assert_eq!(
            budget.cleanup_deadline(start + Duration::from_secs(10)),
            cleanup
        );
        assert_eq!(budget.hard_deadline(), Some(start + Duration::from_secs(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn transport_has_a_bounded_tail_after_execution_and_before_cleanup_even_for_small_caps() {
        for cap in [Duration::from_millis(60), Duration::from_secs(2)] {
            let budget = ShutdownBudget::default();
            let start = Instant::now();
            budget.force_before(start + cap);
            let execution = budget.cooperative_deadline();
            let cleanup = budget.cleanup_deadline(start + cap);
            budget.transport_expired().await;
            let transport = Instant::now();
            assert!(execution < transport);
            assert!(transport < cleanup);
            assert!(cleanup < start + cap);
            assert!(
                budget
                    .transport(async { panic!("cannot start a write past the cutoff") })
                    .await
                    .is_err()
            );
            assert_eq!(
                budget
                    .cleanup(None, async { "connection cleanup" })
                    .await
                    .unwrap(),
                "connection cleanup"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pending_cleanup_observes_later_root_publication_and_shortening() {
        let budget = ShutdownBudget::default();
        let wait = budget.cleanup(None, pending::<()>());
        tokio::pin!(wait);
        assert!(matches!(futures_util::poll!(wait.as_mut()), Poll::Pending));
        let now = Instant::now();
        budget.configure(now + Duration::from_secs(1), now + Duration::from_secs(2));
        budget.force_before(now + Duration::from_millis(80));
        tokio::time::advance(Duration::from_millis(59)).await;
        assert!(matches!(futures_util::poll!(wait.as_mut()), Poll::Pending));
        tokio::time::advance(Duration::from_millis(2)).await;
        assert_eq!(wait.await, Err(()));
        assert_eq!(Instant::now(), now + Duration::from_millis(61));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_budget_does_not_poll_a_new_hook_and_nested_caps_do_not_add() {
        let budget = ShutdownBudget::default();
        let now = Instant::now();
        budget.configure(now, now + Duration::from_millis(40));
        budget.force_before(now + Duration::from_millis(40));
        assert!(
            budget
                .cleanup(Some(now + Duration::from_secs(5)), pending::<()>())
                .await
                .is_err()
        );
        let calls = AtomicUsize::new(0);
        for _ in 0..8 {
            assert!(
                budget
                    .cleanup(Some(Instant::now() + Duration::from_secs(5)), async {
                        calls.fetch_add(1, Ordering::AcqRel);
                    })
                    .await
                    .is_err()
            );
        }
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert!(Instant::now() <= now + Duration::from_millis(31));
    }

    #[tokio::test(start_paused = true)]
    async fn local_cleanup_timeout_cancels_only_its_invocation() {
        let budget = ShutdownBudget::default();
        let root = tokio_util::sync::CancellationToken::new();
        let first = root.child_token();
        let second = root.child_token();
        let view = crate::CleanupCancellation::new(first.clone());
        {
            let _authority = first.drop_guard();
            assert!(
                budget
                    .cleanup(
                        Some(Instant::now() + Duration::from_millis(5)),
                        pending::<()>()
                    )
                    .await
                    .is_err()
            );
        }
        assert!(view.is_cancelled());
        assert!(!second.is_cancelled());
        assert!(!root.is_cancelled());
        assert_eq!(budget.cleanup(None, async { 7 }).await, Ok(7));
    }
}
