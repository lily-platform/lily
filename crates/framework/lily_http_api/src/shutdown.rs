//! One HTTP shutdown attempt and its absolute phase cutoffs.

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::Notify;
use tokio::time::{timeout_at, Instant};

#[derive(Debug, Clone, Copy)]
pub(crate) enum ShutdownStage {
    Graceful,
    Cooperative,
    ExecutionStop,
    Cleanup,
    TransportStop,
    Reconcile,
    Dependencies,
    Telemetry,
    Final,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ShutdownDeadlines {
    pub(crate) started: Instant,
    cutoffs: [Instant; 9],
}

impl ShutdownDeadlines {
    fn new(started: Instant, total: Duration) -> Self {
        let hard = started.checked_add(total).unwrap_or(started);
        let span = hard.saturating_duration_since(started);
        // Internal initial policy, not a new public timeout configuration.
        // Each later integration phase consumes these existing cutoffs.
        let policy = [
            (ShutdownStage::Graceful, 60),
            (ShutdownStage::Cooperative, 70),
            (ShutdownStage::ExecutionStop, 75),
            (ShutdownStage::Cleanup, 85),
            // Stop any remaining connection/encoder before the join cutoff.
            // Reconciliation must retain time to observe actual cancellation.
            (ShutdownStage::TransportStop, 88),
            (ShutdownStage::Reconcile, 90),
            (ShutdownStage::Dependencies, 95),
            (ShutdownStage::Telemetry, 98),
            (ShutdownStage::Final, 100),
        ];
        let mut cutoffs = [started; 9];
        for (stage, percentage) in policy {
            cutoffs[stage as usize] = if percentage == 100 {
                hard
            } else {
                started + (span / 100) * percentage
            };
        }
        Self { started, cutoffs }
    }

    pub(crate) fn at(self, stage: ShutdownStage) -> Instant {
        let cutoff = self.cutoffs[stage as usize];
        debug_assert!(cutoff >= self.started);
        cutoff
    }
}

struct BudgetState {
    total: Duration,
    source: Option<Arc<lily_shutdown::ShutdownState>>,
    deadlines: OnceLock<ShutdownDeadlines>,
    changed: Notify,
}

#[derive(Clone)]
pub(crate) struct ShutdownBudget(Arc<BudgetState>);

impl ShutdownBudget {
    pub(crate) fn from_started(total: Duration, started: Instant) -> Self {
        let deadlines = OnceLock::new();
        let _ = deadlines.set(ShutdownDeadlines::new(started, total));
        Self(Arc::new(BudgetState {
            total,
            source: None,
            deadlines,
            changed: Notify::new(),
        }))
    }
    #[cfg(test)]
    pub(crate) fn new(total: Duration) -> Self {
        Self(Arc::new(BudgetState {
            total,
            source: None,
            deadlines: OnceLock::new(),
            changed: Notify::new(),
        }))
    }

    pub(crate) fn for_state(total: Duration, source: Arc<lily_shutdown::ShutdownState>) -> Self {
        Self(Arc::new(BudgetState {
            total,
            source: Some(source),
            deadlines: OnceLock::new(),
            changed: Notify::new(),
        }))
    }

    pub(crate) fn begin(&self) -> ShutdownDeadlines {
        let deadlines = *self.0.deadlines.get_or_init(|| {
            let started = self
                .0
                .source
                .as_ref()
                .and_then(|source| source.initiation_started_at())
                .unwrap_or_else(Instant::now);
            ShutdownDeadlines::new(started, self.0.total)
        });
        self.0.changed.notify_waiters();
        deadlines
    }

    pub(crate) fn deadlines(&self) -> Option<ShutdownDeadlines> {
        self.0.deadlines.get().copied().or_else(|| {
            self.0
                .source
                .as_ref()
                .filter(|source| source.is_shutdown_initiated())
                .map(|_| self.begin())
        })
    }

    pub(crate) fn force_requested(&self) -> bool {
        self.0
            .source
            .as_ref()
            .is_some_and(|source| source.is_force_requested())
    }

    pub(crate) async fn wait_for_force(&self) {
        match &self.0.source {
            Some(source) => source.wait_for_force().await,
            None => std::future::pending().await,
        }
    }

    /// A control timer only: never wraps or destroys execution. A local
    /// absolute cutoff can be shortened when the root attempt is installed.
    pub(crate) async fn wait_until(&self, stage: ShutdownStage, local: Option<Instant>) {
        self.wait_until_with_reserve(stage, local, Duration::ZERO)
            .await;
    }

    /// Notify a cleanup invocation shortly before the same absolute cutoff.
    /// Installing a root deadline shortens both notification and stop timers.
    pub(crate) async fn wait_until_with_reserve(
        &self,
        stage: ShutdownStage,
        local: Option<Instant>,
        reserve: Duration,
    ) {
        let mut source = self.0.source.as_ref().map(|source| source.subscribe());
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let cutoff = match (local, self.deadlines()) {
                (Some(local), Some(root)) => Some(local.min(root.at(stage))),
                (Some(local), None) => Some(local),
                (None, Some(root)) => Some(root.at(stage)),
                (None, None) => None,
            }
            .map(|cutoff| cutoff.checked_sub(reserve).unwrap_or(cutoff));
            if cutoff.is_some_and(|cutoff| Instant::now() >= cutoff) {
                return;
            }
            tokio::select! {
                _ = async {
                    match cutoff {
                        Some(cutoff) => tokio::time::sleep_until(cutoff).await,
                        None => std::future::pending().await,
                    }
                } => return,
                _ = &mut changed => {},
                _ = async {
                    match &mut source {
                        Some(source) => { let _ = source.recv().await; },
                        None => std::future::pending().await,
                    }
                }, if self.0.deadlines.get().is_none() => { self.begin(); },
            }
        }
    }

    /// Wait only on retained join/receipt observers, not arbitrary user hooks.
    /// Before shutdown this can wait normally; installing H wakes the observer
    /// and clamps the very same wait. Timing out never destroys the real owner.
    pub(crate) async fn wait_for_receipt<F: Future>(
        &self,
        stage: ShutdownStage,
        receipt: F,
    ) -> Result<F::Output, ()> {
        tokio::pin!(receipt);
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(deadlines) = self.deadlines() {
                // A ready actual join may still be reconciled at a zero budget.
                // This never polls an unstarted callback body.
                if let Some(result) = receipt.as_mut().now_or_never() {
                    return Ok(result);
                }
                return timeout_at(deadlines.at(stage), receipt)
                    .await
                    .map_err(|_| ());
            }
            tokio::select! {
                result = &mut receipt => return Ok(result),
                _ = &mut changed => {},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::TaskRegistry;

    #[tokio::test(start_paused = true)]
    async fn delayed_native_signal_observation_uses_the_source_timestamp() {
        let source = Arc::new(lily_shutdown::ShutdownState::new());
        let budget = ShutdownBudget::for_state(Duration::from_secs(10), source.clone());
        let first = Instant::now();
        source
            .initiate_shutdown(lily_shutdown::ShutdownSignal::Interrupt)
            .unwrap();
        tokio::time::advance(Duration::from_secs(4)).await;
        source
            .initiate_shutdown(lily_shutdown::ShutdownSignal::Quit)
            .unwrap();
        source.request_force();
        let deadlines = budget.begin();
        assert_eq!(source.initiation_started_at(), Some(first));
        assert_eq!(deadlines.started, first);
        assert_eq!(
            deadlines.at(ShutdownStage::Final),
            first + Duration::from_secs(10)
        );
    }

    const STAGES: [ShutdownStage; 9] = [
        ShutdownStage::Graceful,
        ShutdownStage::Cooperative,
        ShutdownStage::ExecutionStop,
        ShutdownStage::Cleanup,
        ShutdownStage::TransportStop,
        ShutdownStage::Reconcile,
        ShutdownStage::Dependencies,
        ShutdownStage::Telemetry,
        ShutdownStage::Final,
    ];

    #[test]
    fn cutoffs_are_monotonic_within_one_root_including_zero_and_overflow() {
        let started = Instant::now();
        for total in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_secs(30),
            Duration::MAX,
        ] {
            let deadlines = ShutdownDeadlines::new(started, total);
            let mut previous = started;
            let hard = started.checked_add(total).unwrap_or(started);
            for stage in STAGES {
                let cutoff = deadlines.at(stage);
                assert!(cutoff >= previous && cutoff <= hard);
                previous = cutoff;
            }
            assert_eq!(previous, hard);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_begin_and_clones_never_restart_the_shutdown_attempt() {
        let budget = ShutdownBudget::new(Duration::from_secs(10));
        let first = budget.begin();
        tokio::time::advance(Duration::from_secs(20)).await;
        let repeated = budget.clone().begin();
        assert_eq!(first.started, repeated.started);
        for stage in STAGES {
            assert_eq!(first.at(stage), repeated.at(stage));
        }
        assert!(repeated.at(ShutdownStage::Final) < Instant::now());
    }

    #[tokio::test(start_paused = true)]
    async fn installing_root_deadline_clamps_an_existing_wait_without_dropping_its_task() {
        let tasks = TaskRegistry::default();
        let task = tasks.spawn(std::future::pending::<()>());
        let budget = ShutdownBudget::new(Duration::from_secs(10));
        let observer = budget.clone();
        let observed_task = task.clone();
        let waiter = tokio::spawn(async move {
            observer
                .wait_for_receipt(ShutdownStage::Reconcile, observed_task)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        budget.begin();
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(waiter.await.unwrap().is_err());
        assert_eq!(tasks.snapshot().outstanding, 1);
        assert_eq!(tasks.snapshot().abort_requested, 0);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tasks.seal();
        assert!(tasks.wait().await.is_terminal());
    }

    #[tokio::test(start_paused = true)]
    async fn nested_receipt_waits_cannot_add_another_budget() {
        let budget = ShutdownBudget::new(Duration::from_secs(10));
        let deadlines = budget.begin();
        let tasks = TaskRegistry::default();
        let task = tasks.spawn(std::future::pending::<()>());
        assert!(budget
            .wait_for_receipt(ShutdownStage::Graceful, task.clone())
            .await
            .is_err());
        assert_eq!(Instant::now(), deadlines.at(ShutdownStage::Graceful));
        assert!(budget
            .wait_for_receipt(ShutdownStage::Reconcile, task.clone())
            .await
            .is_err());
        assert_eq!(Instant::now(), deadlines.at(ShutdownStage::Reconcile));
        assert!(budget
            .wait_for_receipt(ShutdownStage::Final, task.clone())
            .await
            .is_err());
        assert_eq!(Instant::now(), deadlines.at(ShutdownStage::Final));
        assert!(budget
            .wait_for_receipt(ShutdownStage::Final, task.clone())
            .await
            .is_err());
        assert_eq!(Instant::now(), deadlines.at(ShutdownStage::Final));
        task.abort();
        let _ = task.await;
        tasks.seal();
        assert!(tasks.wait().await.is_terminal());
    }
}
