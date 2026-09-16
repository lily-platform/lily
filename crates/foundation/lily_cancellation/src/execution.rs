use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Notify;

#[derive(Default)]
struct Signal {
    cancelled: AtomicBool,
    changed: Notify,
}

/// Observation of cancellation for one accepted execution.
///
/// Components participating in an execution share the same signal. A signal
/// asks normal execution to finish cooperatively; it is not proof of task
/// termination and does not cancel dependency-injection or resource cleanup.
///
/// Cloning or dropping this view cannot cancel its source or extend its budget.
/// There is no public constructor, `cancel`, raw token or source conversion.
/// Objects constructed outside a managed execution may carry an inactive view.
///
/// ```compile_fail
/// fn cannot_cancel(signal: lily_cancellation::ExecutionCancellation) {
///     signal.cancel();
/// }
/// ```
/// ```compile_fail
/// let signal = lily_cancellation::ExecutionCancellation::default();
/// ```
/// ```compile_fail
/// fn cannot_create_children(signal: lily_cancellation::ExecutionCancellation) {
///     signal.child_token();
/// }
/// ```
/// ```compile_fail
/// fn cannot_access_source(signal: lily_cancellation::ExecutionCancellation) {
///     let source = signal.0;
/// }
/// ```
#[derive(Clone)]
pub struct ExecutionCancellation(Option<Arc<Signal>>);

impl ExecutionCancellation {
    pub(crate) const fn inactive() -> Self {
        Self(None)
    }

    /// Whether the execution owner has requested cooperative cancellation.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|signal| signal.cancelled.load(Ordering::Acquire))
    }

    /// Waits for cancellation of this execution. Dropping this waiter is safe
    /// and has no effect on this or another view's signal.
    pub async fn cancelled(&self) {
        let Some(signal) = &self.0 else {
            return std::future::pending().await;
        };
        loop {
            let changed = signal.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            changed.await;
        }
    }
}

impl fmt::Debug for ExecutionCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Framework construction seam; never passed to application callbacks.
#[doc(hidden)]
#[derive(Default)]
pub struct ExecutionCancellationSource(Arc<Signal>);

impl ExecutionCancellationSource {
    /// Creates another read-only view of this source.
    pub fn view(&self) -> ExecutionCancellation {
        ExecutionCancellation(Some(self.0.clone()))
    }

    /// Requests cancellation once. It neither drops execution nor runs cleanup.
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[tokio::test]
    async fn views_replay_cancellation_without_owning_its_source() {
        let source = ExecutionCancellationSource::default();
        let view = source.view();
        assert!(view.cancelled().now_or_never().is_none());
        drop(view.clone());
        assert!(!view.is_cancelled());
        let other = source.view();
        source.cancel();
        view.cancelled().await;
        other.cancelled().await;
        assert!(source.view().is_cancelled());
        assert_eq!(
            format!("{view:?}"),
            "ExecutionCancellation { cancelled: true }"
        );
    }

    #[tokio::test]
    async fn independent_sources_and_inactive_views_do_not_cross_cancel() {
        let first = ExecutionCancellationSource::default();
        let second = ExecutionCancellationSource::default();
        first.cancel();
        assert!(!second.view().is_cancelled());
        let inactive = ExecutionCancellation::inactive();
        assert!(!inactive.is_cancelled());
        assert!(inactive.cancelled().now_or_never().is_none());
    }
}
