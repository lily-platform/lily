//! Fixed-size accounting. Retiring an identity moves, never copies, its facts
//! into the totals. The shutdown cohort contains only owners still outstanding
//! when the admission gate closes; lifetime diagnostics remain separate.

use super::*;
use crate::lifecycle::{ResponseBodyOutcome, ResponseHeadEvidence};
use crate::tasks::TaskSnapshot;

macro_rules! counters {
    ($name:ident { $($field:ident),* $(,)? }) => {
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
        pub(crate) struct $name { $(pub(crate) $field: usize,)* }
        impl $name {
            fn include(&mut self, other: Self) { $(self.$field += other.$field;)* }
        }
    };
}

counters!(ExecutionSnapshot {
    created,
    started,
    cancellation_requested,
    returned,
    returned_error,
    panicked,
    dropped,
    outstanding,
});

impl ExecutionSnapshot {
    fn reconciles(self) -> bool {
        self.created
            == self.returned + self.returned_error + self.panicked + self.dropped + self.outstanding
            && self.started <= self.created
            && self.cancellation_requested <= self.created
    }
}

counters!(ResponseSnapshot {
    created,
    head_handed_off,
    completed,
    not_started,
    interrupted,
    failed,
    panicked,
    outcome_outstanding,
    source_released,
    bridge_detached,
    resources_outstanding,
    streaming,
    streaming_outstanding,
});

impl ResponseSnapshot {
    fn reconciles(self) -> bool {
        self.created
            == self.completed
                + self.not_started
                + self.interrupted
                + self.failed
                + self.panicked
                + self.outcome_outstanding
            && self.source_released <= self.created
            && self.bridge_detached <= self.created
            && self.resources_outstanding <= self.created
            && self.streaming <= self.created
            && self.streaming_outstanding <= self.streaming
    }
}

counters!(ScopeSnapshot {
    created,
    succeeded,
    failed,
    timed_out,
    unknown,
    outstanding
});

impl ScopeSnapshot {
    fn reconciles(self) -> bool {
        self.created
            == self.succeeded + self.failed + self.timed_out + self.unknown + self.outstanding
    }
}

counters!(ResourceSnapshot {
    helpers_registered,
    helpers_joined,
    helpers_failed,
    helpers_abort_requested,
    helpers_cancelled,
    inputs_outstanding,
    release_failed,
});

counters!(StopReasons {
    graceful_deadline,
    forced_shutdown,
    request_timeout,
    response_finalization_timeout,
    peer_disconnect,
    transport_failure,
    service_waiter_dropped,
});

impl StopReasons {
    fn total(self) -> usize {
        self.graceful_deadline
            + self.forced_shutdown
            + self.request_timeout
            + self.response_finalization_timeout
            + self.peer_disconnect
            + self.transport_failure
            + self.service_waiter_dropped
    }
}

impl ResourceSnapshot {
    fn reconciles(self) -> bool {
        self.helpers_joined <= self.helpers_registered
            && self.helpers_cancelled + self.helpers_failed <= self.helpers_joined
            && self.helpers_abort_requested <= self.helpers_registered
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestOwnerSnapshot {
    pub(crate) registered: usize,
    pub(crate) admitted: usize,
    // Rejected admission attempts include candidates and pre-registration
    // refusals. They are not another terminal category of registered owners.
    pub(crate) admission_rejected: usize,
    pub(crate) retired: usize,
    pub(crate) outstanding: usize,
    pub(crate) scopes_created: usize,
    pub(crate) scopes_terminal: usize,
    pub(crate) cleanup_failed: usize,
    pub(crate) owner_failed: usize,
    pub(crate) cancellation_requested: usize,
    pub(crate) shutdown_cancelled: usize,
    pub(crate) stop_reasons: StopReasons,
    pub(crate) execution: ExecutionSnapshot,
    pub(crate) response: ResponseSnapshot,
    pub(crate) scopes: ScopeSnapshot,
    pub(crate) resources: ResourceSnapshot,
    pub(crate) owners: TaskSnapshot,
    pub(crate) middleware: middleware::MiddlewareSnapshot,
}

impl RequestOwnerSnapshot {
    pub(crate) fn reconciles(self) -> bool {
        self.registered == self.retired + self.outstanding
            && self.admitted <= self.registered
            && self.execution.created <= self.registered
            && self.response.created <= self.registered
            && self.scopes_created <= self.registered
            && self.cancellation_requested == self.stop_reasons.total()
            && self.shutdown_cancelled
                == self.stop_reasons.graceful_deadline + self.stop_reasons.forced_shutdown
            && self.scopes_created == self.scopes.created
            && self.scopes_created == self.scopes_terminal + self.scopes.outstanding
            && self.owners.registered == self.registered
            && self.owners.reconciles()
            && self.execution.reconciles()
            && self.response.reconciles()
            && self.scopes.reconciles()
            && self.resources.reconciles()
            && self.middleware.reconciles()
    }

    pub(crate) fn is_terminal(self) -> bool {
        self.reconciles() && self.outstanding == 0
    }

    pub(crate) fn cleanup_succeeded(self) -> bool {
        self.is_terminal() && self.cleanup_failed == 0 && self.owner_failed == 0
    }

    fn include(&mut self, other: Self) {
        self.registered += other.registered;
        self.admitted += other.admitted;
        self.admission_rejected += other.admission_rejected;
        self.retired += other.retired;
        self.outstanding += other.outstanding;
        self.scopes_created += other.scopes_created;
        self.scopes_terminal += other.scopes_terminal;
        self.cleanup_failed += other.cleanup_failed;
        self.owner_failed += other.owner_failed;
        self.cancellation_requested += other.cancellation_requested;
        self.shutdown_cancelled += other.shutdown_cancelled;
        self.stop_reasons.include(other.stop_reasons);
        self.execution.include(other.execution);
        self.response.include(other.response);
        self.scopes.include(other.scopes);
        self.resources.include(other.resources);
        self.owners.include(other.owners);
        self.middleware.include(other.middleware);
    }
}

impl RequestEntry {
    fn snapshot(&self) -> RequestOwnerSnapshot {
        let owners = self.join.snapshot();
        let evidence = lock(&self.context.0.evidence);
        let middleware = self.context.0.middleware.snapshot();
        let children = self.context.0.children.snapshot();
        let scope = match &evidence.scope {
            Some(scope) => scope.evidence(),
            None if evidence.scope_created => ScopeCleanupEvidence::Outstanding,
            None => ScopeCleanupEvidence::NotCreated,
        };
        let scope_terminal = scope != ScopeCleanupEvidence::Outstanding;
        let terminal = owners.is_terminal()
            && scope_terminal
            && !evidence.resource_release_failed
            && (!evidence.execution_created || evidence.execution.is_terminal())
            && (!evidence.body_created || evidence.response.resources_terminal())
            && children.terminal()
            && middleware.is_terminal();
        let mut snapshot = RequestOwnerSnapshot {
            registered: 1,
            admitted: usize::from(evidence.admitted),
            retired: usize::from(terminal),
            outstanding: usize::from(!terminal),
            scopes_created: usize::from(evidence.scope_created),
            scopes_terminal: usize::from(evidence.scope_created && scope_terminal),
            cleanup_failed: usize::from(
                evidence.cleanup_wait_failed
                    || evidence.resource_release_failed
                    || children.release_failed
                    || children.helpers_failed != 0
                    || middleware.helpers_failed != 0
                    || middleware.failed
                        + middleware.timed_out
                        + middleware.panicked
                        + middleware.not_started
                        != 0
                    || matches!(scope, ScopeCleanupEvidence::Terminated(outcome) if outcome != CleanupOutcome::Succeeded),
            ),
            owner_failed: usize::from(
                evidence.owner_panicked || owners.panicked + owners.cancelled != 0,
            ),
            cancellation_requested: usize::from(self.context.cancellation_reason().is_some()),
            shutdown_cancelled: usize::from(matches!(
                self.context.cancellation_reason(),
                Some(ExecutionStopReason::GracefulDeadline | ExecutionStopReason::ForcedShutdown)
            )),
            owners,
            middleware,
            resources: ResourceSnapshot {
                helpers_registered: children.helpers_registered,
                helpers_joined: children.helpers_joined,
                helpers_failed: children.helpers_failed,
                helpers_abort_requested: children.helpers_abort_requested,
                helpers_cancelled: children.helpers_cancelled,
                inputs_outstanding: children.inputs_outstanding,
                release_failed: usize::from(
                    children.release_failed || evidence.resource_release_failed,
                ),
            },
            ..Default::default()
        };
        if let Some(reason) = self.context.cancellation_reason() {
            let count = match reason {
                ExecutionStopReason::GracefulDeadline => {
                    &mut snapshot.stop_reasons.graceful_deadline
                }
                ExecutionStopReason::ForcedShutdown => &mut snapshot.stop_reasons.forced_shutdown,
                ExecutionStopReason::RequestTimeout => &mut snapshot.stop_reasons.request_timeout,
                ExecutionStopReason::ResponseFinalizationTimeout => {
                    &mut snapshot.stop_reasons.response_finalization_timeout
                }
                ExecutionStopReason::PeerDisconnect => &mut snapshot.stop_reasons.peer_disconnect,
                ExecutionStopReason::TransportFailure => {
                    &mut snapshot.stop_reasons.transport_failure
                }
                ExecutionStopReason::ServiceWaiterDropped => {
                    &mut snapshot.stop_reasons.service_waiter_dropped
                }
            };
            *count = 1;
        }
        if evidence.execution_created {
            let e = &evidence.execution;
            snapshot.execution = ExecutionSnapshot {
                created: 1,
                started: usize::from(e.started()),
                cancellation_requested: usize::from(e.cancellation_requested().is_some()),
                returned: usize::from(e.termination() == Some(SlotTermination::Returned)),
                returned_error: usize::from(
                    e.termination() == Some(SlotTermination::ReturnedError),
                ),
                panicked: usize::from(e.termination() == Some(SlotTermination::Panicked)),
                dropped: usize::from(e.termination() == Some(SlotTermination::Dropped)),
                outstanding: usize::from(!e.is_terminal()),
            };
        }
        if evidence.body_created {
            let response = &evidence.response;
            snapshot.response = ResponseSnapshot {
                created: 1,
                head_handed_off: usize::from(
                    response.head() == ResponseHeadEvidence::HandedOffToService,
                ),
                completed: usize::from(response.body() == Some(ResponseBodyOutcome::Completed)),
                not_started: usize::from(response.body() == Some(ResponseBodyOutcome::NotStarted)),
                interrupted: usize::from(response.body() == Some(ResponseBodyOutcome::Interrupted)),
                failed: usize::from(response.body() == Some(ResponseBodyOutcome::Failed)),
                panicked: usize::from(response.body() == Some(ResponseBodyOutcome::Panicked)),
                outcome_outstanding: usize::from(response.body().is_none()),
                source_released: usize::from(response.source_released()),
                bridge_detached: usize::from(response.bridge_detached()),
                resources_outstanding: usize::from(!response.resources_terminal()),
                streaming: usize::from(evidence.body_streaming),
                streaming_outstanding: usize::from(
                    evidence.body_streaming && !response.resources_terminal(),
                ),
            };
        }
        if evidence.scope_created {
            snapshot.scopes = ScopeSnapshot {
                created: 1,
                succeeded: usize::from(
                    scope == ScopeCleanupEvidence::Terminated(CleanupOutcome::Succeeded),
                ),
                timed_out: usize::from(
                    scope == ScopeCleanupEvidence::Terminated(CleanupOutcome::TimedOut),
                ),
                unknown: usize::from(
                    scope == ScopeCleanupEvidence::Terminated(CleanupOutcome::Unknown),
                ),
                failed: usize::from(matches!(
                    scope,
                    ScopeCleanupEvidence::Terminated(
                        CleanupOutcome::Failed
                            | CleanupOutcome::Panicked
                            | CleanupOutcome::Cancelled
                            | CleanupOutcome::NotStarted
                    )
                )),
                outstanding: usize::from(!scope_terminal),
            };
        }
        snapshot
    }
}

impl RegistryState {
    pub(super) fn reap(&mut self) {
        self.snapshots();
    }

    fn snapshots(&mut self) -> (RequestOwnerSnapshot, RequestOwnerSnapshot) {
        let mut lifetime = self.retired;
        let mut attempt = self.attempt.unwrap_or_default();
        self.entries.retain(|_, entry| {
            let snapshot = entry.snapshot();
            lifetime.include(snapshot);
            if entry.in_attempt {
                attempt.include(snapshot);
            }
            if !snapshot.is_terminal() {
                return true;
            }
            self.retired.include(snapshot);
            if entry.in_attempt {
                self.attempt
                    .as_mut()
                    .expect("published attempt")
                    .include(snapshot);
            }
            // Scope/helper termination, not handler return, releases capacity.
            lock(&entry.context.0.permit).take();
            false
        });
        (lifetime, attempt)
    }

    pub(super) fn close_admission(&mut self) {
        if self.attempt.is_none() {
            // Retire already-terminal ordinary requests before fixing the
            // cohort, under the same mutex as admission and publication.
            self.reap();
            self.attempt = Some(RequestOwnerSnapshot::default());
            for entry in self.entries.values_mut() {
                entry.in_attempt = true;
            }
        }
        self.admission_closed = true;
    }

    pub(super) fn reject(&mut self) {
        self.retired.admission_rejected += 1;
        if let Some(attempt) = &mut self.attempt {
            attempt.admission_rejected += 1;
        }
    }

    pub(super) fn snapshot(&mut self) -> RequestOwnerSnapshot {
        self.snapshot_for(false)
    }

    pub(super) fn snapshot_for(&mut self, attempt: bool) -> RequestOwnerSnapshot {
        // One observation per identity. Re-polling after reap could observe
        // a late terminal join without releasing its capacity/App keepalive.
        let (lifetime, shutdown) = self.snapshots();
        if attempt {
            shutdown
        } else {
            lifetime
        }
    }
}
