//! HTTP lifecycle evidence contracts, independent of runtime scheduling.
//!
//! Task, execution, response and middleware owners supply their observations. These records
//! neither own nor cancel work; real owners retain the execution slots, task
//! handles and generation-bound DI receipts behind each observation.
//! Updating a record is never a substitute for observing that resource.

/// The first reason the framework asked accepted execution to stop. The actual
/// return/drop/join outcome is recorded separately, even if it wins the race.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionStopReason {
    GracefulDeadline,
    ForcedShutdown,
    RequestTimeout,
    ResponseFinalizationTimeout,
    PeerDisconnect,
    TransportFailure,
    ServiceWaiterDropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotTermination {
    Returned,
    ReturnedError,
    Panicked,
    Dropped,
}

/// Evidence for an inline execution slot retained by a lifecycle owner.
/// `started` means a first poll was observed, not that a user side effect ran.
#[derive(Debug, Default)]
pub(crate) struct ExecutionEvidence {
    started: bool,
    cancellation_requested: Option<ExecutionStopReason>,
    termination: Option<SlotTermination>,
}

impl ExecutionEvidence {
    pub(crate) fn observe_first_poll(&mut self) -> bool {
        if self.started || self.is_terminal() {
            return false;
        }
        self.started = true;
        true
    }

    pub(crate) fn request_cancellation(&mut self, reason: ExecutionStopReason) -> bool {
        if self.is_terminal() || self.cancellation_requested.is_some() {
            return false;
        }
        self.cancellation_requested = Some(reason);
        true
    }

    /// Record observed return, contained panic or completed slot destruction.
    ///
    /// The owner must have released the slot before recording termination. A
    /// blocked/panicking destructor needs its own containment/evidence; an
    /// attempted drop is insufficient. Dropping a `JoinHandle` is never slot
    /// termination: spawned work additionally requires its real join receipt.
    pub(crate) fn observe_termination(&mut self, outcome: SlotTermination) -> bool {
        if self.is_terminal()
            || (!self.started
                && matches!(
                    outcome,
                    SlotTermination::Returned | SlotTermination::ReturnedError
                ))
        {
            return false;
        }
        self.termination = Some(outcome);
        true
    }

    pub(crate) fn started(&self) -> bool {
        self.started
    }

    pub(crate) fn cancellation_requested(&self) -> Option<ExecutionStopReason> {
        self.cancellation_requested
    }

    pub(crate) fn termination(&self) -> Option<SlotTermination> {
        self.termination
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.termination.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskJoinOutcome {
    Completed,
    Failed,
    Cancelled,
    Panicked,
}

/// One actual owned task's join, not the result of a waiter or abort request.
#[derive(Debug, Default)]
pub(crate) struct TaskJoinEvidence {
    abort_requested: bool,
    outcome: Option<TaskJoinOutcome>,
}

impl TaskJoinEvidence {
    pub(crate) fn request_abort(&mut self) -> bool {
        if self.abort_requested || self.is_terminal() {
            return false;
        }
        self.abort_requested = true;
        true
    }

    /// Only the holder of the actual retained `JoinHandle` may observe its
    /// result. Waiter timeout/drop and `JoinHandle::is_finished` do not qualify.
    pub(crate) fn observe_join(&mut self, outcome: TaskJoinOutcome) -> bool {
        if self.is_terminal() {
            return false;
        }
        self.outcome = Some(outcome);
        true
    }

    pub(crate) fn abort_requested(&self) -> bool {
        self.abort_requested
    }

    pub(crate) fn outcome(&self) -> Option<TaskJoinOutcome> {
        self.outcome
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.outcome.is_some()
    }
}

/// The owner-to-service handoff is earlier than committing a final response to
/// Hyper. The transport control records that separate encoder boundary.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseHeadEvidence {
    #[default]
    NotHandedOff,
    HandedOffToService,
}

/// Producer outcome, not delivery to the peer and not destruction of the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseBodyOutcome {
    Completed,
    NotStarted,
    Interrupted,
    Failed,
    Panicked,
}

#[derive(Debug, Default)]
pub(crate) struct ResponseEvidence {
    head: ResponseHeadEvidence,
    body: Option<ResponseBodyOutcome>,
    source_released: bool,
    bridge_detached: bool,
}

impl ResponseEvidence {
    pub(crate) fn observe_service_handoff(&mut self) {
        self.head = ResponseHeadEvidence::HandedOffToService;
    }

    /// Includes an explicit `NotStarted` disposition for a suppressed body
    /// (HEAD/204/304) or an unpolled source discarded during termination.
    pub(crate) fn observe_body_outcome(&mut self, outcome: ResponseBodyOutcome) -> bool {
        if self.body.is_some() {
            return false;
        }
        self.body = Some(outcome);
        true
    }

    /// EOF alone does not qualify: the owner must have released the source and
    /// its captures. Framework helper tasks have separate join prerequisites.
    pub(crate) fn observe_source_release(&mut self) {
        self.source_released = true;
    }

    /// No protocol-held callback/reference can still use the request scope.
    /// Independent encoded bytes and the keep-alive connection may remain.
    /// If no bridge was published, the owner must explicitly confirm that too.
    pub(crate) fn observe_bridge_detached(&mut self) {
        self.bridge_detached = true;
    }

    pub(crate) fn head(&self) -> ResponseHeadEvidence {
        self.head
    }

    pub(crate) fn body(&self) -> Option<ResponseBodyOutcome> {
        self.body
    }

    pub(crate) fn source_released(&self) -> bool {
        self.source_released
    }

    pub(crate) fn bridge_detached(&self) -> bool {
        self.bridge_detached
    }

    pub(crate) fn resources_terminal(&self) -> bool {
        self.body.is_some() && self.source_released && self.bridge_detached
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CleanupOutcome {
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Panicked,
    NotStarted,
    /// Termination was observed but a successful disposal result was not.
    Unknown,
}

/// One entered middleware obligation. `Settled` requires either its normal
/// handle to have returned or its eligible termination invocation to have a
/// final disposition. A pending normal-after interruption is still outstanding.
/// A normal returned request error is recorded by execution diagnostics; it
/// does not arm abnormal cleanup or prove anything about user cleanup code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MiddlewareCleanupEvidence {
    Outstanding,
    NormalReturned,
    TerminationSettled(CleanupOutcome),
}

impl MiddlewareCleanupEvidence {
    fn settled(self) -> bool {
        !matches!(self, Self::Outstanding)
    }

    fn succeeded(self) -> bool {
        matches!(
            self,
            Self::NormalReturned | Self::TerminationSettled(CleanupOutcome::Succeeded)
        )
    }
}

/// Evidence from the exact generation's DI cleanup receipt. Losing a receipt
/// or timing out its waiter is `Outstanding`, never `NotCreated`/`Terminated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeCleanupEvidence {
    NotCreated,
    Outstanding,
    Terminated(CleanupOutcome),
}

/// A coherent snapshot assembled by the real request owner. The owner must
/// include every entered middleware and execution/body helper receipt. These
/// predicates do not register work, authorize admission or supply a join.
///
/// Helpers here are execution/body children which may use request resources.
/// Cleanup invocation and DI workers have their own evidence above. The root
/// separately reconciles lifecycle-owner, connection and protocol task joins.
pub(crate) struct RequestTerminationEvidence<'a> {
    pub(crate) execution: &'a ExecutionEvidence,
    pub(crate) request_body_released: bool,
    pub(crate) response: &'a ResponseEvidence,
    pub(crate) middleware: &'a [MiddlewareCleanupEvidence],
    pub(crate) scope: ScopeCleanupEvidence,
    pub(crate) helpers: &'a [TaskJoinEvidence],
}

impl RequestTerminationEvidence<'_> {
    /// Same-scope user execution must have stopped before abnormal cleanup.
    /// Normal around-middleware exit remains part of the execution slot.
    pub(crate) fn may_begin_termination_cleanup(&self) -> bool {
        self.execution.is_terminal()
            && self.request_body_released
            && self.response.resources_terminal()
            && self.helpers.iter().all(TaskJoinEvidence::is_terminal)
    }

    pub(crate) fn may_close_scope(&self) -> bool {
        self.may_begin_termination_cleanup() && self.middleware.iter().all(|entry| entry.settled())
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.may_close_scope() && self.scope != ScopeCleanupEvidence::Outstanding
    }

    /// Cleanup success only. HTTP status, body delivery and the enclosing
    /// shutdown attempt's task failures are separate reporting dimensions.
    /// Normal-returned middleware has no abnormal-cleanup obligation; this
    /// predicate does not infer the success of arbitrary code inside `handle`.
    pub(crate) fn cleanup_succeeded(&self) -> bool {
        self.is_terminal()
            && self.middleware.iter().all(|entry| entry.succeeded())
            && matches!(
                self.scope,
                ScopeCleanupEvidence::NotCreated
                    | ScopeCleanupEvidence::Terminated(CleanupOutcome::Succeeded)
            )
    }
}

#[cfg(test)]
mod tests;
