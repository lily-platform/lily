//! Payload-free, bounded accounting. Counters cover this application's lifetime;
//! they are evidence, not a claim that every recorded failure occurred at shutdown.

use crate::lifecycle::{
    EnteredLifecycleLedger, LifecycleInterruption, LifecycleInvocation, LifecycleInvocationState,
    LifecycleOutcome,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DependencyState {
    NotOwned,
    NotStarted,
    Unconfirmed,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Panicked,
}

impl DependencyState {
    pub(crate) fn terminal(self) -> bool {
        !matches!(self, Self::NotStarted | Self::Unconfirmed)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct InvocationCounts {
    pub(crate) total: usize,
    pub(crate) completed: usize,
    pub(crate) failed: usize,
    pub(crate) cancelled: usize,
    pub(crate) timed_out: usize,
    pub(crate) aborted: usize,
    pub(crate) panicked: usize,
    pub(crate) outstanding: usize,
    // Orthogonal evidence, not additional outcomes. First poll is not proof of
    // any particular application side effect, or of reaching its first await.
    pub(crate) not_started: usize,
    pub(crate) started_incomplete: usize,
    pub(crate) cancellation_requested: usize,
    pub(crate) abort_requested: usize,
}

impl InvocationCounts {
    pub(crate) fn record(&mut self, invocation: LifecycleInvocation) {
        self.total += 1;
        self.cancellation_requested += usize::from(invocation.cancellation_requested);
        self.abort_requested += usize::from(invocation.abort_requested);
        let (started, complete) = match invocation.state {
            LifecycleInvocationState::Terminal { outcome, started } => {
                match outcome {
                    LifecycleOutcome::Completed => self.completed += 1,
                    LifecycleOutcome::Failed => self.failed += 1,
                    LifecycleOutcome::Panicked => self.panicked += 1,
                    LifecycleOutcome::Interrupted(LifecycleInterruption::Cancelled) => {
                        self.cancelled += 1
                    }
                    LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut) => {
                        self.timed_out += 1
                    }
                    LifecycleOutcome::Interrupted(LifecycleInterruption::Aborted) => {
                        self.aborted += 1
                    }
                }
                (started, outcome == LifecycleOutcome::Completed)
            }
            state => {
                self.outstanding += 1;
                (state == LifecycleInvocationState::Running, false)
            }
        };
        self.not_started += usize::from(!started);
        self.started_incomplete += usize::from(started && !complete);
    }

    pub(crate) fn reconciles(self) -> bool {
        self.total
            == self.completed
                + self.failed
                + self.cancelled
                + self.timed_out
                + self.aborted
                + self.panicked
                + self.outstanding
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.total += other.total;
        self.completed += other.completed;
        self.failed += other.failed;
        self.cancelled += other.cancelled;
        self.timed_out += other.timed_out;
        self.aborted += other.aborted;
        self.panicked += other.panicked;
        self.outstanding += other.outstanding;
        self.not_started += other.not_started;
        self.started_incomplete += other.started_incomplete;
        self.cancellation_requested += other.cancellation_requested;
        self.abort_requested += other.abort_requested;
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LedgerCounts {
    pub(crate) entered: usize,
    pub(crate) exits_completed: usize,
    pub(crate) exits_incomplete: usize,
    pub(crate) exits_not_started: usize,
    pub(crate) normal: InvocationCounts,
    pub(crate) termination: InvocationCounts,
}

impl LedgerCounts {
    pub(crate) fn observe(ledger: &EnteredLifecycleLedger) -> Self {
        let mut counts = Self::default();
        for exit in &ledger.entries {
            counts.entered += 1;
            for (invocation, target) in [
                (&exit.normal, &mut counts.normal),
                (&exit.termination, &mut counts.termination),
            ] {
                if invocation.state != LifecycleInvocationState::Pending {
                    target.record(*invocation);
                }
            }
            let complete = [&exit.normal, &exit.termination].iter().any(|invocation| {
                matches!(
                    invocation.state,
                    LifecycleInvocationState::Terminal {
                        outcome: LifecycleOutcome::Completed,
                        ..
                    }
                )
            });
            counts.exits_completed += usize::from(complete);
            counts.exits_incomplete += usize::from(!complete);
            counts.exits_not_started +=
                usize::from([&exit.normal, &exit.termination].iter().all(|invocation| {
                    matches!(
                        invocation.state,
                        LifecycleInvocationState::Pending
                            | LifecycleInvocationState::Claimed
                            | LifecycleInvocationState::Terminal { started: false, .. }
                    )
                }));
        }
        counts
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.entered += other.entered;
        self.exits_completed += other.exits_completed;
        self.exits_incomplete += other.exits_incomplete;
        self.exits_not_started += other.exits_not_started;
        self.normal.merge(other.normal);
        self.termination.merge(other.termination);
    }

    pub(crate) fn reconciles(self) -> bool {
        self.entered == self.exits_completed + self.exits_incomplete
            && self.normal.reconciles()
            && self.termination.reconciles()
    }
}
