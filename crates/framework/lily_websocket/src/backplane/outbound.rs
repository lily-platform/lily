//! Private invocation authority. It exists only while Lily polls the callback;
//! a retained context/proxy or an application-spawned task cannot retain it.

use super::*;
use crate::shutdown::ShutdownBudget;
use crate::{CleanupCancellation, ExecutionCancellation};
use tokio::time::Instant as TokioInstant;

tokio::task_local! {
    static INVOCATION: Invocation;
}

struct Invocation {
    dispatcher: Weak<WebSocketDispatcherInner>,
    authority: DispatchAuthority,
}

#[derive(Clone)]
pub(super) enum DispatchAuthority {
    Execution {
        ended: tokio_util::sync::CancellationToken,
        cancellation: ExecutionCancellation,
        deadline: TokioInstant,
    },
    Cleanup {
        ended: tokio_util::sync::CancellationToken,
        cancellation: CleanupCancellation,
        budget: ShutdownBudget,
        deadline: TokioInstant,
    },
}

pub(super) fn current_authority(dispatcher: &WebSocketDispatcher) -> Option<DispatchAuthority> {
    INVOCATION
        .try_with(|invocation| {
            Weak::ptr_eq(&invocation.dispatcher, &Arc::downgrade(&dispatcher.0))
                .then(|| invocation.authority.clone())
        })
        .ok()
        .flatten()
}

impl DispatchAuthority {
    async fn stopped(&self) -> WebSocketBackplaneErrorKind {
        match self {
            Self::Execution {
                ended,
                cancellation,
                deadline,
            } => tokio::select! {
                biased;
                () = ended.cancelled() => WebSocketBackplaneErrorKind::Interrupted,
                () = cancellation.execution_stopped(Some(*deadline)) => WebSocketBackplaneErrorKind::Interrupted,
            },
            Self::Cleanup {
                ended,
                cancellation,
                budget,
                deadline,
            } => tokio::select! {
                biased;
                () = ended.cancelled() => WebSocketBackplaneErrorKind::Interrupted,
                () = cancellation.cancelled() => WebSocketBackplaneErrorKind::Interrupted,
                _ = budget.cleanup(Some(*deadline), std::future::pending::<()>()) => WebSocketBackplaneErrorKind::TimedOut,
            },
        }
    }
}

impl WebSocketDispatcher {
    pub(crate) async fn execution_scope<F: Future>(
        &self,
        cancellation: ExecutionCancellation,
        budget: ShutdownBudget,
        deadline: TokioInstant,
        future: F,
    ) -> F::Output {
        let ended = tokio_util::sync::CancellationToken::new();
        let _invocation_lifetime = ended.clone().drop_guard();
        let cancellation = cancellation.with_shutdown_budget(budget);
        INVOCATION
            .scope(
                Invocation {
                    dispatcher: Arc::downgrade(&self.0),
                    authority: DispatchAuthority::Execution {
                        ended,
                        cancellation,
                        deadline,
                    },
                },
                future,
            )
            .await
    }

    pub(crate) async fn cleanup_scope<F: Future>(
        &self,
        cancellation: CleanupCancellation,
        budget: ShutdownBudget,
        deadline: TokioInstant,
        future: F,
    ) -> F::Output {
        let ended = tokio_util::sync::CancellationToken::new();
        let _invocation_lifetime = ended.clone().drop_guard();
        INVOCATION
            .scope(
                Invocation {
                    dispatcher: Arc::downgrade(&self.0),
                    authority: DispatchAuthority::Cleanup {
                        ended,
                        cancellation,
                        budget,
                        deadline,
                    },
                },
                future,
            )
            .await
    }

    pub(super) async fn dispatch_stopped(
        &self,
        authority: Option<&DispatchAuthority>,
    ) -> WebSocketBackplaneErrorKind {
        let invocation_stopped = async {
            match authority {
                Some(authority) => authority.stopped().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            // Cleanup has its own authority even after execution force. Final
            // root expiry applies to every dispatch, including cleanup sends.
            () = self.0.shutdown_budget.final_expired() => WebSocketBackplaneErrorKind::TimedOut,
            reason = invocation_stopped => reason,
            () = self.0.force_dispatch.cancelled(), if authority.is_none() => WebSocketBackplaneErrorKind::Interrupted,
        }
    }

    pub(crate) fn set_shutdown_deadlines(&self, graceful: TokioInstant, hard: TokioInstant) {
        self.0.shutdown_budget.configure(graceful, hard);
    }

    pub(crate) fn set_force_deadline(&self, deadline: TokioInstant) {
        self.0.shutdown_budget.force_before(deadline);
    }
}
