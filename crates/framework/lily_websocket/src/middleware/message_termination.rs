//! Retained message obligations and their bounded, serial termination executor.

use super::*;
use crate::lifecycle::{LifecycleInvocationState, LifecycleStopRequest};

/// Observed reason why a message could not finish its normal execution/unwind.
/// These are framework observations, not categories inferred from user errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsMessageTerminationReason {
    /// The cooperative cancellation boundary stopped execution.
    Cancelled,
    /// A framework execution or normal-unwind deadline expired.
    TimedOut,
    /// The execution owner confirmed drop/abort of its execution slot.
    Aborted,
    /// A panic escaped execution; the owner observed its unwinding.
    Panicked,
}

impl From<LifecycleInterruption> for WsMessageTerminationReason {
    fn from(reason: LifecycleInterruption) -> Self {
        match reason {
            LifecycleInterruption::Cancelled => Self::Cancelled,
            LifecycleInterruption::TimedOut => Self::TimedOut,
            LifecycleInterruption::Aborted => Self::Aborted,
        }
    }
}

impl WsMessageTerminationReason {
    fn outcome(self) -> LifecycleOutcome {
        match self {
            Self::Cancelled => LifecycleOutcome::Interrupted(LifecycleInterruption::Cancelled),
            Self::TimedOut => LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut),
            Self::Aborted => LifecycleOutcome::Interrupted(LifecycleInterruption::Aborted),
            Self::Panicked => LifecycleOutcome::Panicked,
        }
    }

    fn error(self) -> WsMiddlewareError {
        match self {
            Self::Cancelled | Self::Aborted => WsMiddlewareError::cancelled(),
            Self::TimedOut => WsMiddlewareError::timeout(),
            Self::Panicked => BoundedHookFailure::panicked().error,
        }
    }
}

/// Evidence about this middleware's normal exit before termination starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsMessageNormalExit {
    /// No normal exit was claimed for this middleware.
    NotStarted,
    /// A claimed normal exit was cut short; its evidence is retained.
    Interrupted {
        /// Observed cause of the normal exit's interruption.
        reason: WsMessageTerminationReason,
        /// First poll was observed. This cannot identify individual side effects.
        started: bool,
    },
}

/// Invocation-scoped cleanup view of retained message state.
///
/// It exposes the same DI scope and locals used during execution, with cleanup
/// authority only. It cannot change the terminal response. Do not retain scoped
/// services or start untracked tasks to continue beyond this invocation.
pub struct WsMessageTerminationContext<'a> {
    exchange: &'a mut WsMessageExchange,
    outcome: WsMessageOutcome,
    reason: WsMessageTerminationReason,
    normal_exit: WsMessageNormalExit,
    cancellation: CleanupCancellation,
    deadline: tokio::time::Instant,
}

impl WsMessageTerminationContext<'_> {
    /// Retained connection metadata and capabilities; transport may be terminal.
    #[must_use]
    pub fn connection(&self) -> &WebSocketContext {
        self.exchange.connection()
    }

    /// Immutable decoded request; does not grant payload consumption authority.
    #[must_use]
    pub fn request(&self) -> &WsRequest {
        self.exchange.request()
    }

    /// Principal captured for the original message.
    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        self.exchange.principal()
    }

    /// Clones a value from the retained connection locals.
    #[must_use]
    pub fn connection_local<T: Clone + Send + Sync + 'static>(&self) -> Option<T> {
        self.exchange.connection_local::<T>()
    }

    /// Clones a value from the retained message locals.
    #[must_use]
    pub fn message_local<T: Clone + Send + Sync + 'static>(&self) -> Option<T> {
        self.exchange.message_local::<T>()
    }

    /// Publishes local cleanup state for the remaining outer termination hooks.
    pub fn insert_message_local<T: Send + Sync + 'static>(
        &mut self,
        value: T,
    ) -> Result<Option<T>, WebSocketMessageLocalError> {
        self.exchange.insert_message_local(value)
    }

    /// Removes an owned value from the retained message locals.
    pub fn remove_message_local<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.exchange.remove_message_local::<T>()
    }

    /// Resolves from the still-open message scope, before its DI close receipt.
    pub async fn service<T: ?Sized + Send + Sync + 'static>(
        &self,
    ) -> Result<Arc<T>, InjectionError> {
        self.exchange.service::<T>().await
    }

    /// Selected message outcome, provided for observation only.
    #[must_use]
    pub const fn outcome(&self) -> WsMessageOutcome {
        self.outcome
    }

    /// Why this message entered termination unwind.
    #[must_use]
    pub const fn reason(&self) -> WsMessageTerminationReason {
        self.reason
    }

    /// This middleware's normal exit evidence, including possible partial work.
    #[must_use]
    pub const fn normal_exit(&self) -> WsMessageNormalExit {
        self.normal_exit
    }

    /// The same read-only invocation authority passed as the callback argument.
    #[must_use]
    pub const fn cancellation(&self) -> &CleanupCancellation {
        &self.cancellation
    }

    /// Current effective deadline, clipped by later root-budget shortening.
    /// A copied instant is a snapshot and grants no authority to extend cleanup.
    #[must_use]
    pub fn deadline(&self) -> tokio::time::Instant {
        self.exchange
            .connection()
            .shutdown_budget()
            .cleanup_deadline(self.deadline)
    }
}

impl fmt::Debug for WsMessageTerminationContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WsMessageTerminationContext")
            .field("connection_id", &self.connection().connection_id())
            .field("event", &self.request().event())
            .field("reason", &self.reason)
            .field("normal_exit", &self.normal_exit)
            .field("deadline", &self.deadline())
            .finish_non_exhaustive()
    }
}

impl WsMessageLedger {
    pub(crate) fn request_termination(&mut self, reason: WsMessageTerminationReason) {
        self.termination_reason.get_or_insert(reason);
    }

    pub(crate) fn report(&self, fallback: WsMessageOutcome) -> WsMessageAfterReport {
        let mut report = WsMessageAfterReport {
            outcome: self.outcome.unwrap_or(fallback),
            attempted: 0,
            failed: 0,
            first_failure: self.first_failure,
        };
        for invocation in self
            .lifecycle
            .entries
            .iter()
            .flat_map(|entry| [&entry.normal, &entry.termination])
        {
            if invocation.state == LifecycleInvocationState::Pending {
                continue;
            }
            report.attempted += 1;
            if !matches!(
                invocation.state,
                LifecycleInvocationState::Terminal {
                    outcome: LifecycleOutcome::Completed,
                    ..
                }
            ) {
                report.failed += 1;
            }
        }
        report
    }
}

impl CompiledWsMessageChain {
    /// Called only after the execution slot's borrows/futures were dropped.
    /// A stop request alone cannot resolve a running normal-exit obligation.
    pub(crate) fn execution_stopped(
        &self,
        ledger: &mut WsMessageLedger,
        reason: WsMessageTerminationReason,
    ) {
        ledger.request_termination(reason);
        for (index, entry) in self
            .middlewares
            .iter()
            .take(ledger.lifecycle.entries.len())
            .enumerate()
        {
            if matches!(
                ledger.lifecycle.entries[index].normal.state,
                LifecycleInvocationState::Claimed | LifecycleInvocationState::Running
            ) {
                assert!(ledger.lifecycle.finish_exit(
                    index,
                    LifecycleExitPath::Normal,
                    reason.outcome()
                ));
                ledger
                    .first_failure
                    .get_or_insert(WsMiddlewareExecutionError::new(
                        entry.descriptor,
                        WsMiddlewareStage::MessageAfter,
                        reason.error(),
                    ));
            }
        }
    }

    /// One tail budget for the entire serial unwind. A pending hook receives a
    /// share of the remaining time so that outer eligible hooks retain a chance
    /// to start. Fast hooks leave their unused share to the remaining suffix.
    pub(crate) async fn terminate(
        &self,
        exchange: &mut WsMessageExchange,
        ledger: &mut WsMessageLedger,
        outcome: WsMessageOutcome,
        stage_timeout: Duration,
        authority: &CancellationToken,
    ) {
        let Some(reason) = ledger.termination_reason else {
            return;
        };
        let _ = ledger.lifecycle.claim_cleanup();
        let eligible = |state| {
            matches!(
                state,
                LifecycleInvocationState::Pending
                    | LifecycleInvocationState::Terminal {
                        outcome: LifecycleOutcome::Interrupted(_),
                        ..
                    }
            )
        };
        let mut remaining_hooks = ledger
            .lifecycle
            .entries
            .iter()
            .filter(|entry| {
                eligible(entry.normal.state)
                    && entry.termination.state == LifecycleInvocationState::Pending
            })
            .count();
        let budget = exchange.connection().shutdown_budget().clone();
        let owner_deadline = budget.cleanup_deadline(tokio::time::Instant::now() + stage_timeout);
        for (index, entry) in self
            .middlewares
            .iter()
            .take(ledger.lifecycle.entries.len())
            .enumerate()
            .rev()
        {
            if !ledger
                .lifecycle
                .claim_exit(index, LifecycleExitPath::Termination)
            {
                continue;
            }
            let normal_exit = match ledger.lifecycle.entries[index].normal.state {
                LifecycleInvocationState::Pending => WsMessageNormalExit::NotStarted,
                LifecycleInvocationState::Terminal {
                    outcome: LifecycleOutcome::Interrupted(reason),
                    started,
                } => WsMessageNormalExit::Interrupted {
                    reason: reason.into(),
                    started,
                },
                _ => unreachable!("only an eligible normal exit can claim termination"),
            };
            let now = tokio::time::Instant::now();
            let remaining = budget
                .cleanup_deadline(owner_deadline)
                .saturating_duration_since(now);
            let deadline = now
                + remaining / u32::try_from(remaining_hooks).expect("middleware count is bounded");
            remaining_hooks -= 1;
            let source = authority.child_token();
            let _cancel_on_drop = source.clone().drop_guard();
            let signal = CleanupCancellation::new(source.clone());
            let started = Instant::now();
            let dispatcher = exchange.connection().dispatcher().clone();
            let mut invocation = Box::pin(
                AssertUnwindSafe(dispatcher.cleanup_scope(
                    signal.clone(),
                    budget.clone(),
                    deadline,
                    async {
                        assert!(
                            ledger
                                .lifecycle
                                .start_exit(index, LifecycleExitPath::Termination)
                        );
                        let context = WsMessageTerminationContext {
                            exchange,
                            outcome,
                            reason,
                            normal_exit,
                            cancellation: signal.clone(),
                            deadline,
                        };
                        entry
                            .middleware
                            .on_message_termination(context, signal)
                            .await
                    },
                ))
                .catch_unwind(),
            );
            let result = tokio::select! {
                biased;
                () = source.cancelled() => Err(BoundedHookFailure::cancelled_by_token()),
                result = budget.cleanup(Some(deadline), &mut invocation) => match result {
                    Ok(Ok(result)) => result.map_err(BoundedHookFailure::regular),
                    Ok(Err(_)) => Err(BoundedHookFailure::panicked()),
                    Err(()) => Err(BoundedHookFailure::timed_out()),
                }
            };
            let stop_requested = result
                .as_ref()
                .is_err_and(|failure| failure.interruption.is_some());
            // Signal expiry before running the future's destructors. This is
            // not a promise of another poll, nor proof that cleanup completed.
            source.cancel();
            let result = match std::panic::catch_unwind(AssertUnwindSafe(|| drop(invocation))) {
                Ok(()) => result,
                Err(_) => Err(BoundedHookFailure::panicked()),
            };
            let observed = result.as_ref().map_or_else(
                |failure| failure.lifecycle_outcome(),
                |_| LifecycleOutcome::Completed,
            );
            assert!(
                ledger
                    .lifecycle
                    .finish_exit(index, LifecycleExitPath::Termination, observed)
            );
            if stop_requested {
                ledger.lifecycle.entries[index]
                    .termination
                    .request_stop(LifecycleStopRequest::Cancellation);
            }
            let invocation = ledger.lifecycle.entries[index].termination;
            tracing::debug!(
                lily.middleware = entry.descriptor.name(),
                lily.middleware_stage = WsMiddlewareStage::MessageTermination.as_str(),
                lily.lifecycle = ?invocation.state,
                lily.cancellation_requested = invocation.cancellation_requested,
                "WebSocket message termination invocation ended"
            );
            let observation = match result {
                Ok(()) => WsMiddlewareObservationOutcome::Completed,
                Err(failure) => {
                    ledger
                        .first_failure
                        .get_or_insert(WsMiddlewareExecutionError::new(
                            entry.descriptor,
                            WsMiddlewareStage::MessageTermination,
                            failure.error,
                        ));
                    WsMiddlewareObservationOutcome::from_error(failure.error)
                }
            };
            // Diagnostics must not strand the outer retained obligations.
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                self.observer
                    .observe(entry.descriptor, observation, started.elapsed())
            }));
        }
    }
}
