use std::fmt;

use tokio_util::sync::CancellationToken;

/// Read-only cancellation signal for an accepted user execution.
///
/// Available to handshake/identity middleware, connection admission/open hooks,
/// message middleware, guards, message actions, and `#[connected]` handlers.
/// Stopping new message admission does not itself cancel accepted execution.
/// Observing this signal is not proof that execution or resource cleanup ended.
///
/// Only Lily holds the cancellation source. Cloning this view grants no right
/// to cancel it, obtain its underlying token, or create child authorities.
#[derive(Clone)]
pub struct ExecutionCancellation {
    token: CancellationToken,
    budget: crate::shutdown::ShutdownBudget,
    cooperative_deadline: std::sync::Arc<std::sync::Mutex<Option<tokio::time::Instant>>>,
    message: Option<std::sync::Arc<super::message_cancellation::MessageCancellation>>,
}

impl ExecutionCancellation {
    pub(crate) fn new(token: CancellationToken) -> Self {
        Self::with_budget(token, Default::default())
    }

    pub(crate) fn with_budget(
        token: CancellationToken,
        budget: crate::shutdown::ShutdownBudget,
    ) -> Self {
        Self {
            token,
            budget,
            cooperative_deadline: Default::default(),
            message: None,
        }
    }

    pub(crate) fn for_message(
        parent: CancellationToken,
        budget: crate::shutdown::ShutdownBudget,
    ) -> Self {
        let token = parent.child_token();
        let message = std::sync::Arc::new(super::message_cancellation::MessageCancellation::new(
            token.clone(),
        ));
        Self {
            message: Some(message),
            ..Self::with_budget(token, budget)
        }
    }

    pub(crate) fn set_message_deadline(&self, deadline: tokio::time::Instant) {
        self.message
            .as_ref()
            .expect("message authority")
            .set_deadline(deadline);
    }

    pub(crate) fn message_facts(&self) -> Option<super::MessageCancellationFacts> {
        self.message.as_ref().map(|message| message.observe())
    }

    pub(crate) fn recorded_message_facts(&self) -> Option<super::MessageCancellationFacts> {
        self.message
            .as_ref()
            .map(|message| message.recorded_facts())
    }

    pub(crate) fn finish_message(&self) {
        if let Some(message) = &self.message {
            message.finish();
        }
    }

    pub(crate) fn request_cancel(&self) {
        match &self.message {
            Some(message) => message.request_cancel(),
            None => self.token.cancel(),
        }
    }

    pub(crate) fn with_shutdown_budget(mut self, budget: crate::shutdown::ShutdownBudget) -> Self {
        self.budget = budget;
        self
    }

    /// Internal stop boundary; the public signal itself never drops execution.
    pub(crate) async fn termination_requested(&self) {
        self.cancelled().await;
        if let Some(message) = &self.message {
            let request = message.observe().request.expect("cancellation receipt");
            self.budget
                .execution_expired(request.cooperative_deadline)
                .await;
            return;
        }
        let deadline = *self
            .cooperative_deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(|| self.budget.cooperative_deadline());
        self.budget.execution_expired(deadline).await;
    }

    /// Normal message deadlines signal cancellation; only their shared
    /// cooperative cutoff stops execution. Other lifecycle invocations retain
    /// their own local cap, always clipped by the root budget.
    pub(crate) async fn execution_stopped(&self, local: Option<tokio::time::Instant>) {
        tokio::select! {
            biased;
            () = self.termination_requested() => {},
            () = async {
                if self.message.is_some() {
                    self.budget.execution_limit_expired().await;
                } else if let Some(deadline) = local {
                    self.budget.execution_expired(deadline).await;
                } else {
                    self.budget.execution_limit_expired().await;
                }
            } => { self.request_cancel(); }
        }
    }

    /// Whether the framework has signalled cancellation of this execution.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        if let Some(message) = &self.message {
            message.observe();
        }
        self.token.is_cancelled()
    }

    /// Waits for cancellation. Dropping this wait does not cancel execution.
    pub async fn cancelled(&self) {
        if let Some(message) = &self.message {
            loop {
                let facts = message.observe();
                if self.token.is_cancelled() {
                    return;
                }
                if facts.terminal {
                    self.token.cancelled().await;
                    return;
                }
                tokio::select! {
                    biased;
                    () = self.token.cancelled() => {},
                    () = async {
                        match message.deadline() {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending().await,
                        }
                    } => {}
                }
            }
        } else {
            self.token.cancelled().await;
        }
    }
}

impl fmt::Debug for ExecutionCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionCancellation")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Read-only cancellation signal for one framework-owned cleanup invocation.
///
/// Available to message `on_message_termination`, connection `closed`, and
/// controller `#[disconnected]` hooks.
/// Its source is independent of execution cancellation. A local invocation
/// ending or exhausting its budget does not cancel sibling cleanup invocations.
/// Cancellation grants no completion guarantee and does not report success.
/// Controller lifecycle handlers can also extract [`super::MessageDeadline`]
/// to inspect their invocation's remaining budget.
#[derive(Clone)]
pub struct CleanupCancellation {
    token: CancellationToken,
}

impl CleanupCancellation {
    pub(crate) fn new(token: CancellationToken) -> Self {
        Self { token }
    }

    /// Whether cleanup was cancelled or this invocation's authority ended.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Waits for cancellation. Dropping this wait does not cancel cleanup.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

impl fmt::Debug for CleanupCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CleanupCancellation")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Compatibility name for the read-only execution signal.
///
/// This alias no longer exposes a tuple field, `Deref`, `into_inner`, or a
/// cancellation source. It is not a disconnected-hook extractor.
#[deprecated(
    since = "0.1.0",
    note = "Use ExecutionCancellation for actions/connected hooks and CleanupCancellation for disconnected hooks"
)]
pub type Cancellation = ExecutionCancellation;

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::Poll;

    #[tokio::test]
    async fn execution_views_observe_the_source_without_owning_it() {
        let source = CancellationToken::new();
        let signal = ExecutionCancellation::new(source.clone());
        let clone = signal.clone();
        let wait = clone.cancelled();
        tokio::pin!(wait);
        assert!(matches!(futures_util::poll!(wait.as_mut()), Poll::Pending));
        assert!(!signal.is_cancelled());
        source.cancel();
        wait.await;
        assert!(signal.is_cancelled());
        signal.cancelled().await;
    }

    #[tokio::test]
    async fn cleanup_children_have_independent_local_authority_and_shared_root() {
        let execution_source = CancellationToken::new();
        let cleanup_root = CancellationToken::new();
        let first_source = cleanup_root.child_token();
        let second_source = cleanup_root.child_token();
        let first = CleanupCancellation::new(first_source.clone());
        let second = CleanupCancellation::new(second_source);
        execution_source.cancel();
        assert!(!first.is_cancelled());
        assert!(!second.is_cancelled());

        let first_guard = first_source.drop_guard();
        drop(first_guard);
        first.cancelled().await;
        assert!(!cleanup_root.is_cancelled());
        assert!(!second.is_cancelled());
        cleanup_root.cancel();
        second.cancelled().await;
    }

    #[tokio::test]
    async fn dropping_views_or_waits_does_not_cancel_the_source() {
        let source = CancellationToken::new();
        let signal = ExecutionCancellation::new(source.clone());
        {
            let wait = signal.cancelled();
            tokio::pin!(wait);
            assert!(matches!(futures_util::poll!(wait.as_mut()), Poll::Pending));
        }
        drop(signal);
        drop(CleanupCancellation::new(source.clone()));
        assert!(!source.is_cancelled());
    }

    #[test]
    fn signals_and_waits_are_send_and_diagnostics_contain_no_raw_token() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send_future(_: impl Future<Output = ()> + Send) {}
        assert_send_sync::<ExecutionCancellation>();
        assert_send_sync::<CleanupCancellation>();
        let execution = ExecutionCancellation::new(CancellationToken::new());
        let cleanup = CleanupCancellation::new(CancellationToken::new());
        assert_send_future(execution.cancelled());
        assert_send_future(cleanup.cancelled());
        assert_eq!(
            format!("{execution:?}"),
            "ExecutionCancellation { is_cancelled: false }"
        );
        assert_eq!(
            format!("{cleanup:?}"),
            "CleanupCancellation { is_cancelled: false }"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn local_timeout_notifies_only_its_message_and_never_restarts_cooperation() {
        let parent = CancellationToken::new();
        let first = ExecutionCancellation::for_message(parent.clone(), Default::default());
        let second = ExecutionCancellation::for_message(parent.clone(), Default::default());
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
        first.set_message_deadline(deadline);
        second.set_message_deadline(deadline + std::time::Duration::from_secs(1));
        first.cancelled().await;
        assert_eq!(tokio::time::Instant::now(), deadline);
        assert!(!parent.is_cancelled());
        assert!(!second.is_cancelled());
        let receipt = first.message_facts().unwrap().request.unwrap();
        assert_eq!(
            receipt.cause,
            super::super::MessageCancellationCause::MessageTimeout
        );
        assert_eq!(receipt.at, deadline);
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        first.clone().cancelled().await;
        parent.cancel();
        second.cancelled().await;
        let repeated = first.message_facts().unwrap().request.unwrap();
        assert_eq!(repeated.cause, receipt.cause);
        assert_eq!(repeated.cooperative_deadline, receipt.cooperative_deadline);
        first.execution_stopped(Some(deadline)).await;
        assert_eq!(
            tokio::time::Instant::now(),
            deadline + std::time::Duration::from_millis(250)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn connection_cancellation_remains_the_cause_when_local_deadline_later_expires() {
        let parent = CancellationToken::new();
        let signal = ExecutionCancellation::for_message(parent.clone(), Default::default());
        let start = tokio::time::Instant::now();
        signal.set_message_deadline(start + std::time::Duration::from_millis(100));
        parent.cancel();
        signal.cancelled().await;
        let first = signal.message_facts().unwrap().request.unwrap();
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let facts = signal.message_facts().unwrap();
        assert!(facts.deadline_exceeded);
        assert_eq!(
            facts.request.unwrap().cause,
            super::super::MessageCancellationCause::ConnectionCancelled
        );
        assert_eq!(
            facts.request.unwrap().cooperative_deadline,
            first.cooperative_deadline
        );
        signal.execution_stopped(None).await;
        assert_eq!(start.elapsed(), std::time::Duration::from_millis(250));
    }

    #[tokio::test(start_paused = true)]
    async fn force_shortens_an_already_running_local_cooperative_window() {
        let budget = crate::shutdown::ShutdownBudget::default();
        let signal = ExecutionCancellation::for_message(CancellationToken::new(), budget.clone());
        let start = tokio::time::Instant::now();
        signal.set_message_deadline(start + std::time::Duration::from_millis(100));
        signal.cancelled().await;
        let stop = signal.execution_stopped(None);
        tokio::pin!(stop);
        assert!(futures_util::poll!(stop.as_mut()).is_pending());
        tokio::time::advance(std::time::Duration::from_millis(20)).await;
        budget.force_before(start + std::time::Duration::from_millis(200));
        stop.await;
        assert_eq!(start.elapsed(), std::time::Duration::from_millis(140));
        assert_eq!(
            signal.message_facts().unwrap().request.unwrap().cause,
            super::super::MessageCancellationCause::MessageTimeout
        );
    }

    #[tokio::test(start_paused = true)]
    async fn completed_execution_freezes_deadline_evidence_and_retained_view_does_not_fire_a_late_timeout()
     {
        let parent = CancellationToken::new();
        let signal = ExecutionCancellation::for_message(parent.clone(), Default::default());
        signal.set_message_deadline(
            tokio::time::Instant::now() + std::time::Duration::from_millis(10),
        );
        signal.finish_message();
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        assert!(!signal.is_cancelled());
        assert!(!signal.message_facts().unwrap().deadline_exceeded);
        assert!(futures_util::poll!(Box::pin(signal.cancelled())).is_pending());
        parent.cancel();
        signal.cancelled().await;
        assert!(signal.message_facts().unwrap().request.is_none());
    }
}
