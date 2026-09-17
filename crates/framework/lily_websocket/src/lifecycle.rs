//! Internal lifecycle accounting, independent of cancellation authorities.
//!
//! A claim reserves work; a first poll starts it; only observed termination
//! resolves it. Cancellation and abort requests are recorded separately. These
//! records do not own or stop futures: the execution owner must retain them
//! until it has observed the future's return/drop or a spawned task's join.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleInterruption {
    Cancelled,
    TimedOut,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleStopRequest {
    Cancellation,
    // Retained owners request execution-slot abort separately from confirmed
    // slot drop and task join observations.
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleOutcome {
    Completed,
    Failed,
    Panicked,
    Interrupted(LifecycleInterruption),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LifecycleInvocationState {
    #[default]
    Pending,
    Claimed,
    Running,
    Terminal {
        outcome: LifecycleOutcome,
        started: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct LifecycleInvocation {
    pub(crate) state: LifecycleInvocationState,
    pub(crate) cancellation_requested: bool,
    pub(crate) abort_requested: bool,
}

impl LifecycleInvocation {
    pub(crate) fn claim(&mut self) -> bool {
        if self.state != LifecycleInvocationState::Pending {
            return false;
        }
        self.state = LifecycleInvocationState::Claimed;
        true
    }

    /// Called inside the owned operation, when its first poll is observed.
    pub(crate) fn start(&mut self) -> bool {
        if self.state != LifecycleInvocationState::Claimed {
            return false;
        }
        self.state = LifecycleInvocationState::Running;
        true
    }

    /// A request is not evidence that the operation has stopped.
    pub(crate) fn request_stop(&mut self, request: LifecycleStopRequest) {
        match request {
            LifecycleStopRequest::Cancellation => self.cancellation_requested = true,
            LifecycleStopRequest::Abort => self.abort_requested = true,
        }
    }

    /// The caller must have observed return/drop, or joined the owned task.
    /// A ready/error return requires a first poll; an abort may precede it.
    pub(crate) fn finish(&mut self, outcome: LifecycleOutcome) -> bool {
        let started = match self.state {
            LifecycleInvocationState::Running => true,
            LifecycleInvocationState::Claimed
                if matches!(
                    outcome,
                    LifecycleOutcome::Interrupted(_) | LifecycleOutcome::Panicked
                ) =>
            {
                false
            }
            _ => return false,
        };
        self.state = LifecycleInvocationState::Terminal { outcome, started };
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleExitPath {
    Normal,
    Termination,
}

/// One obligation per successfully entered component. An interrupted normal
/// exit retains its evidence when a separate termination exit is claimed.
#[derive(Debug, PartialEq, Eq, Default)]
pub(crate) struct LifecycleExit {
    pub(crate) normal: LifecycleInvocation,
    pub(crate) termination: LifecycleInvocation,
}

impl LifecycleExit {
    fn claim(&mut self, path: LifecycleExitPath) -> bool {
        match path {
            LifecycleExitPath::Normal => {
                self.termination.state == LifecycleInvocationState::Pending && self.normal.claim()
            }
            LifecycleExitPath::Termination => {
                matches!(
                    self.normal.state,
                    LifecycleInvocationState::Pending
                        | LifecycleInvocationState::Terminal {
                            outcome: LifecycleOutcome::Interrupted(_),
                            ..
                        }
                ) && self.termination.claim()
            }
        }
    }

    fn invocation_mut(&mut self, path: LifecycleExitPath) -> &mut LifecycleInvocation {
        match path {
            LifecycleExitPath::Normal => &mut self.normal,
            LifecycleExitPath::Termination => &mut self.termination,
        }
    }
}

/// An entered prefix is retained after the exclusive unwind claim. Entries
/// absent from this ledger were never eligible for exit; pending entries have
/// an obligation which has not yet been attempted. The owner executes entries
/// serially in reverse order and keeps the ledger through reconciliation.
#[derive(Debug, PartialEq, Eq, Default)]
pub(crate) struct EnteredLifecycleLedger {
    pub(crate) entries: Vec<LifecycleExit>,
    pub(crate) cleanup_claimed: bool,
}

impl EnteredLifecycleLedger {
    pub(crate) fn record_entered(&mut self, next: usize) -> bool {
        if self.cleanup_claimed || self.entries.len().checked_add(1) != Some(next) {
            return false;
        }
        self.entries.push(LifecycleExit::default());
        true
    }

    pub(crate) fn claim_cleanup(&mut self) -> Option<usize> {
        if self.cleanup_claimed {
            return None;
        }
        self.cleanup_claimed = true;
        Some(self.entries.len())
    }

    pub(crate) fn claim_exit(&mut self, index: usize, path: LifecycleExitPath) -> bool {
        self.cleanup_claimed
            && self
                .entries
                .get_mut(index)
                .is_some_and(|entry| entry.claim(path))
    }

    pub(crate) fn start_exit(&mut self, index: usize, path: LifecycleExitPath) -> bool {
        self.entries
            .get_mut(index)
            .is_some_and(|entry| entry.invocation_mut(path).start())
    }

    pub(crate) fn finish_exit(
        &mut self,
        index: usize,
        path: LifecycleExitPath,
        outcome: LifecycleOutcome,
    ) -> bool {
        self.entries.get_mut(index).is_some_and(|entry| {
            let invocation = entry.invocation_mut(path);
            if !invocation.finish(outcome) {
                return false;
            }
            if outcome == LifecycleOutcome::Interrupted(LifecycleInterruption::Cancelled) {
                // The hook executor observed its cancellation signal. A
                // timeout alone does not prove that any signal was sent.
                invocation.request_stop(LifecycleStopRequest::Cancellation);
            }
            true
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_and_abort_requests_do_not_confirm_termination() {
        let mut invocation = LifecycleInvocation::default();
        assert!(invocation.claim());
        assert!(invocation.start());
        invocation.request_stop(LifecycleStopRequest::Cancellation);
        invocation.request_stop(LifecycleStopRequest::Abort);
        assert_eq!(invocation.state, LifecycleInvocationState::Running);
        assert!(invocation.cancellation_requested);
        assert!(invocation.abort_requested);
        // An operation can return normally before an abort takes effect.
        assert!(invocation.finish(LifecycleOutcome::Completed));
        assert_eq!(
            invocation.state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Completed,
                started: true,
            }
        );
        assert!(!invocation.finish(LifecycleOutcome::Interrupted(
            LifecycleInterruption::Aborted
        )));
    }

    #[test]
    fn never_polled_and_partially_executed_are_distinct() {
        for started in [false, true] {
            let mut invocation = LifecycleInvocation::default();
            assert!(!invocation.finish(LifecycleOutcome::Completed));
            assert!(invocation.claim());
            if started {
                assert!(invocation.start());
            } else {
                assert!(!invocation.finish(LifecycleOutcome::Completed));
            }
            let outcome = LifecycleOutcome::Interrupted(LifecycleInterruption::Aborted);
            assert!(invocation.finish(outcome));
            assert_eq!(
                invocation.state,
                LifecycleInvocationState::Terminal { outcome, started }
            );
            assert!(!invocation.claim());
            assert!(!invocation.start());
        }
    }

    #[test]
    fn claimed_unwind_retains_entered_and_pending_obligations() {
        let mut ledger = EnteredLifecycleLedger::default();
        assert!(!ledger.record_entered(2));
        assert!(ledger.record_entered(1));
        assert!(ledger.record_entered(2));
        assert!(!ledger.claim_exit(1, LifecycleExitPath::Normal));
        assert_eq!(ledger.claim_cleanup(), Some(2));
        assert_eq!(ledger.claim_cleanup(), None);
        assert!(!ledger.record_entered(3));
        assert_eq!(ledger.entries.len(), 2);
        assert_eq!(ledger.entries[0], LifecycleExit::default());
        assert!(!ledger.claim_exit(2, LifecycleExitPath::Termination));
    }

    #[test]
    fn terminal_normal_exit_cannot_be_terminated_again() {
        for outcome in [
            LifecycleOutcome::Completed,
            LifecycleOutcome::Failed,
            LifecycleOutcome::Panicked,
        ] {
            let mut entry = LifecycleExit::default();
            assert!(entry.claim(LifecycleExitPath::Normal));
            assert!(entry.normal.start());
            assert!(!entry.claim(LifecycleExitPath::Termination));
            assert!(entry.normal.finish(outcome));
            assert!(!entry.claim(LifecycleExitPath::Normal));
            assert!(!entry.claim(LifecycleExitPath::Termination));
        }
    }

    #[test]
    fn termination_retains_interrupted_normal_exit_and_cannot_be_claimed_twice() {
        let mut ledger = EnteredLifecycleLedger::default();
        assert!(ledger.record_entered(1));
        assert_eq!(ledger.claim_cleanup(), Some(1));
        assert!(ledger.claim_exit(0, LifecycleExitPath::Normal));
        assert!(ledger.start_exit(0, LifecycleExitPath::Normal));
        assert!(!ledger.claim_exit(0, LifecycleExitPath::Termination));
        assert!(ledger.finish_exit(
            0,
            LifecycleExitPath::Normal,
            LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut),
        ));
        let normal = ledger.entries[0].normal;
        assert!(!normal.cancellation_requested);
        assert!(ledger.claim_exit(0, LifecycleExitPath::Termination));
        assert!(!ledger.claim_exit(0, LifecycleExitPath::Termination));
        assert!(ledger.start_exit(0, LifecycleExitPath::Termination));
        assert!(ledger.finish_exit(
            0,
            LifecycleExitPath::Termination,
            LifecycleOutcome::Completed,
        ));
        assert_eq!(ledger.entries[0].normal, normal);
        assert!(!ledger.claim_exit(0, LifecycleExitPath::Normal));
        assert!(!ledger.claim_exit(0, LifecycleExitPath::Termination));
    }

    #[test]
    fn termination_can_claim_an_entered_obligation_without_normal_exit() {
        let mut entry = LifecycleExit::default();
        assert!(entry.claim(LifecycleExitPath::Termination));
        assert!(!entry.claim(LifecycleExitPath::Normal));
        assert!(entry.termination.start());
        assert!(entry.termination.finish(LifecycleOutcome::Failed));
        assert!(!entry.claim(LifecycleExitPath::Termination));
        assert_eq!(entry.normal.state, LifecycleInvocationState::Pending);
    }
}
