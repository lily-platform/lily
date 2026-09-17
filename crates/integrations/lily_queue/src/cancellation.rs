//! Framework-owned cancellation sources. Public delivery callbacks only get a view.

use std::sync::{Arc, Mutex, OnceLock};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Why Lily requested that an accepted delivery stop executing.
///
/// This is a cancellation request, not proof that the handler, DI scope or
/// broker settlement has terminated. The first recorded reason is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeliveryCancellationReason {
    /// The delivery's common normal-pipeline deadline expired.
    DeliveryTimeout,
    /// The application's graceful shutdown deadline expired.
    ShutdownDeadline,
    /// The application explicitly requested forced shutdown.
    ForcedShutdown,
    /// A supervised framework task failed and its siblings must stop.
    RuntimeFailure,
    /// An underlying runtime or transaction authority cancelled the operation.
    RuntimeCancellation,
}

#[derive(Debug, Clone)]
pub(crate) struct DeliveryCancellationSource(Arc<Source>);

#[derive(Debug)]
struct Source {
    token: CancellationToken,
    reason: OnceLock<CancellationRequest>,
    parent: Option<DeliveryCancellationSource>,
    // One short synchronous critical section linearizes parent and local
    // requests. It retains no delivery registry or completed-delivery history.
    gate: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Copy)]
struct CancellationRequest {
    reason: DeliveryCancellationReason,
    at: Instant,
}

impl DeliveryCancellationSource {
    pub(crate) fn new() -> Self {
        Self::from_token(CancellationToken::new())
    }

    fn from_token(token: CancellationToken) -> Self {
        Self(Arc::new(Source {
            token,
            reason: OnceLock::new(),
            parent: None,
            gate: Arc::new(Mutex::new(())),
        }))
    }

    pub(crate) fn child(&self) -> Self {
        self.child_with_token(self.0.token.child_token())
    }

    // The supplied token must already be a descendant of this source. Used
    // by the transaction runtime, which has its own operation cancellation.
    pub(crate) fn child_with_token(&self, token: CancellationToken) -> Self {
        Self(Arc::new(Source {
            token,
            reason: OnceLock::new(),
            parent: Some(self.clone()),
            gate: Arc::clone(&self.0.gate),
        }))
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub(crate) fn raw_child_token(&self) -> CancellationToken {
        self.0.token.child_token()
    }

    pub(crate) fn cancel(&self, reason: DeliveryCancellationReason) {
        self.cancel_at(reason, Instant::now());
    }

    pub(crate) fn cancel_at(&self, reason: DeliveryCancellationReason, at: Instant) {
        {
            let _gate = self.0.gate.lock().unwrap_or_else(|e| e.into_inner());
            let reason = self
                .request_locked()
                .unwrap_or(CancellationRequest { reason, at });
            let _ = self.0.reason.set(reason);
        }
        // Publish evidence before waking observers.
        // Wakers may run arbitrary code; never invoke them under our gate.
        self.0.token.cancel();
    }

    pub(crate) fn reason(&self) -> Option<DeliveryCancellationReason> {
        let _gate = self.0.gate.lock().unwrap_or_else(|e| e.into_inner());
        self.request_locked().map(|request| request.reason)
    }

    pub(crate) fn requested_at(&self) -> Option<Instant> {
        let _gate = self.0.gate.lock().unwrap_or_else(|e| e.into_inner());
        self.request_locked().map(|request| request.at)
    }

    fn request_locked(&self) -> Option<CancellationRequest> {
        if let Some(reason) = self.0.reason.get() {
            return Some(*reason);
        }
        let reason = self
            .0
            .parent
            .as_ref()
            .and_then(Self::request_locked)
            .or_else(|| {
                self.0.token.is_cancelled().then(|| CancellationRequest {
                    reason: DeliveryCancellationReason::RuntimeCancellation,
                    at: Instant::now(),
                })
            });
        if let Some(reason) = reason {
            let _ = self.0.reason.set(reason);
        }
        reason
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.token.is_cancelled()
    }

    pub(crate) async fn cancelled(&self) {
        self.0.token.cancelled().await;
    }
}

#[cfg(test)]
impl From<CancellationToken> for DeliveryCancellationSource {
    fn from(token: CancellationToken) -> Self {
        Self::from_token(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delivery_timeout_cannot_cancel_siblings_or_the_runtime() {
        let runtime = DeliveryCancellationSource::new();
        let first = runtime.child();
        let second = runtime.child();
        first.cancel(DeliveryCancellationReason::DeliveryTimeout);
        first.cancelled().await;
        assert_eq!(
            first.reason(),
            Some(DeliveryCancellationReason::DeliveryTimeout)
        );
        assert!(!second.is_cancelled());
        assert_eq!(second.reason(), None);
        assert!(!runtime.is_cancelled());
    }

    #[tokio::test]
    async fn shutdown_reaches_children_without_replacing_a_prior_timeout() {
        let runtime = DeliveryCancellationSource::new();
        let timed_out = runtime.child();
        let active = runtime.child();
        timed_out.cancel(DeliveryCancellationReason::DeliveryTimeout);
        runtime.cancel(DeliveryCancellationReason::ForcedShutdown);
        active.cancelled().await;
        active.cancel(DeliveryCancellationReason::DeliveryTimeout);
        assert_eq!(
            timed_out.reason(),
            Some(DeliveryCancellationReason::DeliveryTimeout)
        );
        assert_eq!(
            active.reason(),
            Some(DeliveryCancellationReason::ForcedShutdown)
        );
        assert_eq!(
            runtime.child().reason(),
            Some(DeliveryCancellationReason::ForcedShutdown)
        );
    }

    #[test]
    fn concurrent_requests_publish_one_stable_reason_before_notification() {
        for _ in 0..64 {
            let runtime = DeliveryCancellationSource::new();
            let delivery = runtime.child();
            std::thread::scope(|scope| {
                scope.spawn(|| runtime.cancel(DeliveryCancellationReason::ForcedShutdown));
                scope.spawn(|| delivery.cancel(DeliveryCancellationReason::DeliveryTimeout));
            });
            let first = delivery.reason().expect("cancelled source has a reason");
            assert!(matches!(
                first,
                DeliveryCancellationReason::ForcedShutdown
                    | DeliveryCancellationReason::DeliveryTimeout
            ));
            delivery.cancel(DeliveryCancellationReason::RuntimeFailure);
            assert_eq!(delivery.reason(), Some(first));
            assert!(delivery.is_cancelled());
        }
    }
}
