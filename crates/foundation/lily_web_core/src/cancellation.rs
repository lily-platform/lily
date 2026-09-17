//! Read-only HTTP cleanup cancellation, independent of execution authority.

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::Notify;
use tokio::time::Instant;

struct CleanupSignal {
    signal: Signal,
    deadline: std::sync::Mutex<Instant>,
}

/// Read-only cancellation of one HTTP termination callback.
///
/// This authority is independent of execution and sibling cleanup invocations.
/// Cancellation asks this callback to finish within its remaining deadline; it
/// is not evidence of completion. The deadline may shorten, never extend.
/// Cloning or dropping a view cannot cancel anything.
///
/// ```compile_fail
/// fn cannot_cancel(signal: lily_web_core::CleanupCancellation) { signal.cancel(); }
/// ```
/// ```compile_fail
/// let signal = lily_web_core::CleanupCancellation::default();
/// ```
/// ```compile_fail
/// fn cannot_convert(signal: lily_web_core::ExecutionCancellation) {
///     let _: lily_web_core::CleanupCancellation = signal.into();
/// }
/// ```
/// ```compile_fail
/// fn cannot_create_child(signal: lily_web_core::CleanupCancellation) { signal.child_token(); }
/// ```
#[derive(Clone)]
pub struct CleanupCancellation(Arc<CleanupSignal>);

impl CleanupCancellation {
    /// Whether the owner has requested this invocation to finish.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.signal.cancelled.load(Ordering::Acquire)
    }

    /// The latest published absolute cutoff for this invocation.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        *self
            .0
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Waits for this invocation's cancellation without owning its source.
    pub async fn cancelled(&self) {
        loop {
            let changed = self.0.signal.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            changed.await;
        }
    }
}

impl fmt::Debug for CleanupCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CleanupCancellation")
            .field("cancelled", &self.is_cancelled())
            .field("deadline", &self.deadline())
            .finish()
    }
}

/// Framework construction seam; never passed to application callbacks.
#[doc(hidden)]
pub struct CleanupCancellationSource(Arc<CleanupSignal>);

impl CleanupCancellationSource {
    /// Creates an independent invocation authority.
    pub fn new(deadline: Instant) -> Self {
        Self(Arc::new(CleanupSignal {
            signal: Signal::default(),
            deadline: std::sync::Mutex::new(deadline),
        }))
    }

    /// Creates a read-only view of this invocation.
    pub fn view(&self) -> CleanupCancellation {
        CleanupCancellation(self.0.clone())
    }

    /// Shortens the published cutoff without changing any sibling authority.
    pub fn cap_deadline(&self, cutoff: Instant) {
        let mut deadline = self
            .0
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *deadline = (*deadline).min(cutoff);
    }

    /// Requests completion of only this invocation.
    pub fn cancel(&self) {
        self.0.signal.cancelled.store(true, Ordering::Release);
        self.0.signal.changed.notify_waiters();
    }
}

#[derive(Default)]
struct Signal {
    cancelled: AtomicBool,
    changed: Notify,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use lily_cancellation::__private::ExecutionCancellationSource;

    #[tokio::test(start_paused = true)]
    async fn cleanup_authorities_are_read_only_isolated_and_only_shorten_deadlines() {
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        let first = CleanupCancellationSource::new(deadline);
        let sibling = CleanupCancellationSource::new(deadline);
        let execution = ExecutionCancellationSource::default();
        let view = first.view();
        execution.cancel();
        assert!(!view.is_cancelled());
        assert!(!sibling.view().is_cancelled());
        first.cap_deadline(deadline - std::time::Duration::from_secs(1));
        first.cap_deadline(deadline);
        assert_eq!(
            view.deadline(),
            deadline - std::time::Duration::from_secs(1)
        );
        assert_eq!(sibling.view().deadline(), deadline);
        assert!(view.cancelled().now_or_never().is_none());
        first.cancel();
        view.cancelled().await;
        assert!(view.is_cancelled());
        assert!(!sibling.view().is_cancelled());
        drop(view);
        assert!(!sibling.view().is_cancelled());
    }

    #[tokio::test]
    async fn request_local_clear_cannot_remove_execution_authority() {
        let mut request =
            crate::Request::from_transport_parts("GET".into(), "/".into(), vec![], &[])
                .await
                .unwrap();
        let source = ExecutionCancellationSource::default();
        crate::__private::bind_request_execution(&mut request, source.view());
        let retained = request.execution_cancellation();
        request.local_mut().insert(17_u32);
        request.local_mut().clear();
        source.cancel();
        assert!(request.execution_cancellation().is_cancelled());
        retained.cancelled().await;
    }
}
