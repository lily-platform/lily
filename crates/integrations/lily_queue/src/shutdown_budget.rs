//! One absolute shutdown attempt shared by the composition root and queue runtime.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

pub(crate) const DELIVERY_COOPERATIVE_CAP: Duration = Duration::from_millis(250);
pub(crate) const DELIVERY_TERMINATION_CAP: Duration = Duration::from_secs(1);
pub(crate) const CONSUMER_CHANNEL_CLOSE_RESERVE: Duration = Duration::from_millis(250);

/// One existing delivery budget, with one reserve shared by cooperation,
/// abnormal middleware unwind and exact DI disposal. Normal callbacks all
/// see the same cutoff; no callback starts another timer.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DeliveryExecutionBudget {
    pub(crate) pipeline: Instant,
    pub(crate) hard: Instant,
    started: Instant,
}

impl DeliveryExecutionBudget {
    pub(crate) fn before(hard: Instant, now: Instant) -> Self {
        let reserve = (hard.saturating_duration_since(now) / 4).min(DELIVERY_TERMINATION_CAP);
        Self {
            pipeline: hard.checked_sub(reserve).unwrap_or(now).min(hard),
            hard,
            started: now,
        }
    }

    pub(crate) fn cap(self, hard: Instant) -> Self {
        let cap = Self::before(self.hard.min(hard), self.started);
        Self {
            pipeline: self.pipeline.min(cap.pipeline),
            ..cap
        }
    }
}

/// Internal composition-root contract; child owners may shorten, never renew it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueShutdownDeadlines {
    graceful: Instant,
    hard: Instant,
}

impl QueueShutdownDeadlines {
    /// Anchor an attempt at its original initiation time, including scheduler delay.
    pub fn starting_at(started: Instant, total: Duration) -> Self {
        let hard = started.checked_add(total).unwrap_or(started);
        let reserve = (total / 4).min(Duration::from_secs(2));
        Self::before(
            hard.checked_sub(reserve).unwrap_or(started).max(started),
            hard,
        )
    }

    /// Adopt the root's deadlines without starting another duration.
    pub fn before(graceful: Instant, hard: Instant) -> Self {
        Self {
            graceful: graceful.min(hard),
            hard,
        }
    }

    /// Latest time for ordinary accepted-delivery drain.
    pub const fn graceful(self) -> Instant {
        self.graceful
    }

    /// Final application shutdown deadline, including cleanup and reconciliation.
    pub const fn hard(self) -> Instant {
        self.hard
    }

    /// Clamp an operation's existing deadline to the application root.
    pub fn cap(self, local: Instant) -> Instant {
        local.min(self.hard)
    }
}

#[derive(Debug, Default)]
struct BudgetState {
    deadlines: Mutex<Option<QueueShutdownDeadlines>>,
    revision: AtomicU64,
    changed: Notify,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct QueueShutdownBudget(Arc<BudgetState>);

impl QueueShutdownBudget {
    /// Poll the same owned operation against its original local cap and every
    /// later root shortening. A changed budget never constructs a new future.
    pub(crate) async fn run_until<F: std::future::Future>(
        &self,
        local: Instant,
        future: F,
    ) -> Result<F::Output, ()> {
        tokio::pin!(future);
        loop {
            let revision = self.revision();
            let deadline = self.cap(local);
            if Instant::now() >= deadline {
                return Err(());
            }
            tokio::select! {
                biased;
                result = future.as_mut() => return Ok(result),
                _ = tokio::time::sleep_until(deadline) => return Err(()),
                _ = self.changed_since(revision) => {},
            }
        }
    }

    pub(crate) fn install(&self, deadlines: QueueShutdownDeadlines) {
        let mut current = self.0.deadlines.lock().unwrap_or_else(|e| e.into_inner());
        *current = Some(match *current {
            Some(previous) => QueueShutdownDeadlines::before(
                previous.graceful.min(deadlines.graceful),
                previous.hard.min(deadlines.hard),
            ),
            None => deadlines,
        });
        self.0.revision.fetch_add(1, Ordering::Release);
        drop(current);
        self.0.changed.notify_waiters();
    }

    pub(crate) fn deadlines(&self) -> Option<QueueShutdownDeadlines> {
        *self.0.deadlines.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn revision(&self) -> u64 {
        self.0.revision.load(Ordering::Acquire)
    }

    pub(crate) async fn changed_since(&self, revision: u64) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.revision() != revision {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn cap(&self, deadline: Instant) -> Instant {
        self.deadlines().map_or(deadline, |root| root.cap(deadline))
    }

    /// One forced-delivery cutoff anchored to the first notification. Leave
    /// a quarter of the root's remaining time for final joins/dependencies.
    pub(crate) fn forced_delivery_deadline(&self, requested_at: Instant) -> Instant {
        let cap = self.deadlines().map_or(DELIVERY_TERMINATION_CAP, |root| {
            (root.hard().saturating_duration_since(requested_at) * 3 / 4)
                .min(DELIVERY_TERMINATION_CAP)
        });
        self.cap(requested_at + cap)
    }

    pub(crate) fn cooperative_deadline(
        &self,
        requested_at: Instant,
        owner_deadline: Instant,
    ) -> Instant {
        let hard = self.cap(owner_deadline);
        (requested_at
            + (hard.saturating_duration_since(requested_at) / 4).min(DELIVERY_COOPERATIVE_CAP))
        .min(hard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn expired_root_never_polls_a_new_transport_operation() {
        let budget = QueueShutdownBudget::default();
        let now = Instant::now();
        budget.install(QueueShutdownDeadlines::before(now, now));
        let result = budget
            .run_until(now + Duration::from_secs(60), async {
                panic!("expired root must not start broker I/O");
            })
            .await;
        assert_eq!(result, Err(()));
    }

    #[tokio::test(start_paused = true)]
    async fn root_installed_during_transport_wait_shortens_the_same_future_and_drops_it_once() {
        use std::sync::atomic::AtomicUsize;
        use tokio_util::sync::CancellationToken;
        struct Witness(Arc<AtomicUsize>);
        impl Drop for Witness {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let budget = QueueShutdownBudget::default();
        let entered = CancellationToken::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let started = Instant::now();
        let task_budget = budget.clone();
        let task_entered = entered.clone();
        let task_drops = drops.clone();
        let task = tokio::spawn(async move {
            task_budget
                .run_until(started + Duration::from_secs(30), async move {
                    let _witness = Witness(task_drops);
                    task_entered.cancel();
                    std::future::pending::<()>().await
                })
                .await
        });
        entered.cancelled().await;
        tokio::time::advance(Duration::from_millis(20)).await;
        budget.install(QueueShutdownDeadlines::before(
            started,
            started + Duration::from_millis(100),
        ));
        budget.install(QueueShutdownDeadlines::before(
            started,
            started + Duration::from_millis(40),
        ));
        budget.install(QueueShutdownDeadlines::starting_at(
            Instant::now(),
            Duration::from_secs(60),
        ));
        assert_eq!(task.await.unwrap(), Err(()));
        assert_eq!(Instant::now() - started, Duration::from_millis(40));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn publishing_roots_cannot_renew_an_operation_local_cap() {
        let budget = QueueShutdownBudget::default();
        let now = Instant::now();
        budget.install(QueueShutdownDeadlines::starting_at(
            now,
            Duration::from_secs(60),
        ));
        let result = budget
            .run_until(
                now + Duration::from_millis(20),
                std::future::pending::<()>(),
            )
            .await;
        assert_eq!(result, Err(()));
        assert_eq!(Instant::now() - now, Duration::from_millis(20));
    }

    #[test]
    fn delivery_tail_and_transaction_caps_never_restart_the_common_pipeline_budget() {
        let started = Instant::now();
        let short = DeliveryExecutionBudget::before(started + Duration::from_millis(100), started);
        assert_eq!(short.pipeline, started + Duration::from_millis(75));
        let long = DeliveryExecutionBudget::before(started + Duration::from_secs(30), started);
        assert_eq!(long.pipeline, started + Duration::from_secs(29));
        let capped = long.cap(short.hard);
        assert_eq!(capped.pipeline, short.pipeline);
        assert_eq!(capped.cap(long.hard).hard, short.hard);
        assert_eq!(capped.cap(long.hard).pipeline, short.pipeline);
        let expired = DeliveryExecutionBudget::before(started - Duration::from_secs(1), started);
        assert_eq!(expired.pipeline, expired.hard);
    }

    #[test]
    fn late_observers_and_repeated_publication_cannot_renew_the_root_budget() {
        let started = Instant::now() - Duration::from_secs(20);
        let original = QueueShutdownDeadlines::starting_at(started, Duration::from_secs(10));
        let budget = QueueShutdownBudget::default();
        let late_child = budget.clone();
        budget.install(original);
        budget.install(QueueShutdownDeadlines::starting_at(
            Instant::now(),
            Duration::from_secs(30),
        ));
        assert_eq!(late_child.deadlines(), Some(original));
        assert!(original.hard() < Instant::now());
        assert_eq!(
            original.cap(Instant::now() + Duration::from_secs(5)),
            original.hard()
        );
    }

    #[test]
    fn shorter_deadlines_win_and_zero_budget_never_grows() {
        let now = Instant::now();
        let budget = QueueShutdownBudget::default();
        budget.install(QueueShutdownDeadlines::starting_at(
            now,
            Duration::from_secs(30),
        ));
        let expired = QueueShutdownDeadlines::starting_at(now, Duration::ZERO);
        budget.install(expired);
        budget.install(QueueShutdownDeadlines::starting_at(
            now,
            Duration::from_secs(60),
        ));
        assert_eq!(budget.deadlines(), Some(expired));
        assert_eq!(expired.graceful(), now);
        assert_eq!(expired.hard(), now);
    }
}
