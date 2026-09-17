//! One request clock shared by dispatch, lazy production and protocol writing.
//! This authority owns no application resource, scope, callback or task.

use super::{lock, COOPERATIVE_CANCELLATION_CAP};
use crate::lifecycle::ExecutionStopReason;
use crate::shutdown::{ShutdownBudget, ShutdownStage};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// One bounded attempt to send an uncommitted error/rejection response. It is
/// not a renewed request timeout and is capped before root transport joins.
pub(crate) const RESPONSE_FINALIZATION_CAP: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
struct Stop {
    reason: ExecutionStopReason,
    started: Instant,
}

struct State {
    deadline: OnceLock<Instant>,
    first_stop: Mutex<Option<Stop>>,
    stopped: CancellationToken,
    signal: lily_cancellation::__private::ExecutionCancellationSource,
    changed: Notify,
    force: CancellationToken,
    root: ShutdownBudget,
}

#[derive(Clone)]
pub(crate) struct RequestDeadline(Arc<State>);

impl RequestDeadline {
    pub(crate) fn new(root: ShutdownBudget, force: CancellationToken) -> Self {
        Self(Arc::new(State {
            deadline: OnceLock::new(),
            first_stop: Mutex::new(None),
            stopped: CancellationToken::new(),
            signal: lily_cancellation::__private::ExecutionCancellationSource::default(),
            changed: Notify::new(),
            force,
            root,
        }))
    }

    pub(crate) fn start(&self, timeout: Duration) {
        let now = Instant::now();
        assert!(
            self.0
                .deadline
                .set(now.checked_add(timeout).unwrap_or(now))
                .is_ok(),
            "one absolute deadline per accepted request"
        );
        self.0.changed.notify_waiters();
    }

    pub(super) fn at(&self) -> Instant {
        *self
            .0
            .deadline
            .get()
            .expect("deadline published at admission")
    }

    pub(crate) fn cancel(&self, reason: ExecutionStopReason) {
        let mut first = lock(&self.0.first_stop);
        first.get_or_insert_with(|| Stop {
            reason,
            started: Instant::now(),
        });
        self.0.signal.cancel();
        self.0.stopped.cancel();
    }

    pub(crate) fn reason(&self) -> Option<ExecutionStopReason> {
        lock(&self.0.first_stop).map(|stop| stop.reason)
    }

    pub(crate) fn cancellation(&self) -> lily_cancellation::ExecutionCancellation {
        self.0.signal.view()
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.0.stopped.is_cancelled()
    }

    pub(crate) fn root(&self) -> &ShutdownBudget {
        &self.0.root
    }

    pub(crate) fn refresh(&self) {
        if self.is_cancelled() {
            return;
        }
        let now = Instant::now();
        let reason = if self.0.force.is_cancelled() || self.0.root.force_requested() {
            Some(ExecutionStopReason::ForcedShutdown)
        } else if self
            .0
            .root
            .deadlines()
            .is_some_and(|root| now >= root.at(ShutdownStage::Graceful))
        {
            Some(ExecutionStopReason::GracefulDeadline)
        } else if self
            .0
            .deadline
            .get()
            .is_some_and(|deadline| now >= *deadline)
        {
            Some(ExecutionStopReason::RequestTimeout)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.cancel(reason);
        }
    }

    pub(crate) async fn wait_for_stop(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.refresh();
            if self.is_cancelled() {
                return;
            }
            let reason = tokio::select! {
                biased;
                _ = self.0.stopped.cancelled() => return,
                _ = self.0.force.cancelled() => ExecutionStopReason::ForcedShutdown,
                _ = self.0.root.wait_for_force() => ExecutionStopReason::ForcedShutdown,
                _ = self.0.root.wait_until(ShutdownStage::Graceful, None) => ExecutionStopReason::GracefulDeadline,
                _ = async {
                    match self.0.deadline.get() {
                        Some(deadline) => tokio::time::sleep_until(*deadline).await,
                        None => std::future::pending().await,
                    }
                } => ExecutionStopReason::RequestTimeout,
                _ = changed => continue,
            };
            self.cancel(reason);
            return;
        }
    }

    pub(crate) fn cooperative_deadline(&self) -> Option<Instant> {
        let stop = (*lock(&self.0.first_stop))?;
        let local = stop.started + COOPERATIVE_CANCELLATION_CAP;
        Some(
            self.0
                .root
                .deadlines()
                .map_or(local, |root| local.min(root.at(ShutdownStage::Cooperative))),
        )
    }

    pub(crate) fn cooperative_expired(&self) -> bool {
        self.refresh();
        self.cooperative_deadline()
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub(crate) async fn cooperative_cutoff(&self) {
        self.wait_for_stop().await;
        let stop = lock(&self.0.first_stop).expect("signal publishes its timestamp");
        self.0
            .root
            .wait_until(
                ShutdownStage::Cooperative,
                Some(stop.started + COOPERATIVE_CANCELLATION_CAP),
            )
            .await;
    }
}
