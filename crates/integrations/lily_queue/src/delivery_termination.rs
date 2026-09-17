//! Read-only cleanup authority, independent of delivery execution cancellation.

use lily_injection::InjectionError;
use std::sync::Arc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    DeliveryCancellationReason, DeliveryContext, DeliveryInvocation,
    shutdown_budget::QueueShutdownBudget,
};

/// Why the framework could not complete a delivery's normal lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeliveryTerminationReason {
    /// Execution did not finish in its bounded cancellation window.
    ExecutionCancelled(DeliveryCancellationReason),
    /// A normal execution or normal exit future panicked.
    Panicked,
    /// The enclosing delivery/transaction future was dropped.
    OwnerDropped,
}

/// Normal exit evidence for a middleware eligible for termination cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeliveryNormalExit {
    /// Normal `after_delivery` was never started.
    NotStarted,
    /// Normal exit started but was interrupted before returning.
    Interrupted,
    /// Normal exit panicked rather than returning an application result.
    Panicked,
}

/// Read-only cancellation for one termination invocation.
///
/// It is independent of `DeliveryCancellation`. Each hook receives a fresh
/// sibling signal. A hook's expiration never cancels later hooks. Its deadline
/// may only shorten when the application root deadline is shortened.
#[derive(Debug, Clone)]
pub struct DeliveryCleanupCancellation {
    token: CancellationToken,
    deadline: Instant,
    budget: QueueShutdownBudget,
}

impl DeliveryCleanupCancellation {
    pub(crate) fn new(
        root: &CancellationToken,
        deadline: Instant,
        budget: QueueShutdownBudget,
    ) -> Self {
        Self {
            token: root.child_token(),
            deadline,
            budget,
        }
    }

    /// The current absolute invocation cutoff, capped by the shutdown root.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.budget.cap(self.deadline)
    }

    /// Whether this invocation has been stopped or its remaining budget expired.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled() || Instant::now() >= self.deadline()
    }

    /// Wait for invocation stop or deadline expiry, including root shortening.
    pub async fn cancelled(&self) {
        loop {
            let revision = self.budget.revision();
            if self.is_cancelled() {
                return;
            }
            tokio::select! {
                _ = self.token.cancelled() => return,
                _ = tokio::time::sleep_until(self.deadline()) => return,
                _ = self.budget.changed_since(revision) => {}
            }
        }
    }

    pub(crate) fn stop(&self) {
        self.token.cancel();
    }
}

/// Metadata passed through the private registration ABI. It owns no delivery
/// borrow, so the public context can be constructed at the typed boundary.
pub(crate) struct TerminationInvocation {
    pub(crate) reason: DeliveryTerminationReason,
    pub(crate) normal_exit: DeliveryNormalExit,
    pub(crate) cancellation: DeliveryCleanupCancellation,
}

impl Drop for TerminationInvocation {
    fn drop(&mut self) {
        self.cancellation.stop();
    }
}

/// Cleanup view for an entered middleware whose normal exit did not finish.
///
/// There is no mutable pipeline result or ACK/NACK authority. DI and local
/// state remain available until the framework finishes this reverse cleanup
/// chain. Application-created tasks/resources remain application-owned.
pub struct QueueDeliveryTerminationContext<'a> {
    invocation: &'a mut DeliveryInvocation,
    termination: &'a TerminationInvocation,
}

impl<'a> QueueDeliveryTerminationContext<'a> {
    pub(crate) fn new(
        invocation: &'a mut DeliveryInvocation,
        termination: &'a TerminationInvocation,
    ) -> Self {
        Self {
            invocation,
            termination,
        }
    }

    /// Immutable delivery metadata.
    #[must_use]
    pub fn context(&self) -> &DeliveryContext {
        self.invocation.context()
    }

    /// The interruption that made this middleware eligible for cleanup.
    #[must_use]
    pub fn reason(&self) -> DeliveryTerminationReason {
        self.termination.reason
    }

    /// Whether normal exit was absent, interrupted or panicked.
    #[must_use]
    pub fn normal_exit(&self) -> DeliveryNormalExit {
        self.termination.normal_exit
    }

    /// This invocation's cleanup signal; never the execution cancellation token.
    #[must_use]
    pub fn cancellation(&self) -> &DeliveryCleanupCancellation {
        &self.termination.cancellation
    }

    /// Current absolute cleanup invocation cutoff, from the same authority as the signal.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.cancellation().deadline()
    }

    /// Resolve a dependency from the still-live delivery scope.
    pub async fn service<T: ?Sized + Send + Sync + 'static>(
        &self,
    ) -> Result<Arc<T>, InjectionError> {
        self.invocation.extensions().get_service::<T>(None).await
    }

    /// Read an owned clone of middleware/extractor/handler local state.
    #[must_use]
    pub fn local<T: Clone + Send + Sync + 'static>(&self) -> Option<T> {
        self.invocation.local::<T>()
    }

    /// Release one delivery-local value before DI disposal.
    pub fn remove_local<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.invocation.remove_local::<T>()
    }
}
