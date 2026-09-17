//! Bounded, payload-free evidence for routed messages. Output observation is
//! transferred to the connection after the message owner's join, so its counts
//! cannot be retired with that join. No task or per-message registry is needed.

use super::{PreparedWebSocketTerminal, WsMessageOutcome};
use std::sync::{Arc, Mutex};

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PipelineCounts {
    pub(super) handled: usize,
    pub(super) rejected: usize,
    pub(super) closed: usize,
    pub(super) failed: usize,
    pub(super) not_returned: usize,
    pub(super) unobserved: usize,
}

impl PipelineCounts {
    pub(super) fn record(&mut self, outcome: Option<Option<WsMessageOutcome>>) {
        match outcome {
            Some(Some(WsMessageOutcome::Handled)) => self.handled += 1,
            Some(Some(WsMessageOutcome::Rejected(_))) => self.rejected += 1,
            Some(Some(WsMessageOutcome::Close(_))) => self.closed += 1,
            Some(Some(WsMessageOutcome::Failed(_))) => self.failed += 1,
            Some(None) => self.not_returned += 1,
            None => self.unobserved += 1,
        }
    }

    pub(super) fn returned(self) -> usize {
        self.handled + self.rejected + self.closed + self.failed
    }

    pub(super) fn total(self) -> usize {
        self.returned() + self.not_returned + self.unobserved
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct OutputCounts {
    pub(super) total: usize,
    // Preparation is orthogonal evidence; it does not imply an attempt.
    pub(super) prepared_frames: usize,
    pub(super) prepared_closes: usize,
    pub(super) prepared_no_reply: usize,
    pub(super) attempts: usize,
    // Exclusive terminal dispositions and outstanding observations.
    pub(super) queued_frames: usize,
    pub(super) close_requests_completed: usize,
    pub(super) no_reply: usize,
    pub(super) failed: usize,
    pub(super) interrupted: usize,
    pub(super) panicked: usize,
    pub(super) suppressed: usize,
    pub(super) not_prepared: usize,
    pub(super) outstanding: usize,
}

impl OutputCounts {
    pub(super) fn reconciles(self) -> bool {
        self.total
            == self.queued_frames
                + self.close_requests_completed
                + self.no_reply
                + self.failed
                + self.interrupted
                + self.panicked
                + self.suppressed
                + self.not_prepared
                + self.outstanding
            && self.prepared_frames + self.prepared_closes + self.prepared_no_reply <= self.total
            && self.attempts <= self.prepared_frames + self.prepared_closes + self.prepared_no_reply
            && self.queued_frames <= self.prepared_frames
            && self.close_requests_completed <= self.prepared_closes
            && self.no_reply <= self.prepared_no_reply
            && self.queued_frames
                + self.close_requests_completed
                + self.no_reply
                + self.failed
                + self.interrupted
                + self.panicked
                <= self.attempts
            && self.suppressed + self.attempts
                <= self.prepared_frames + self.prepared_closes + self.prepared_no_reply
            && self.not_prepared
                + self.prepared_frames
                + self.prepared_closes
                + self.prepared_no_reply
                <= self.total
    }
}

#[derive(Clone, Default)]
pub(super) struct OutputRegistry(Arc<Mutex<OutputCounts>>);

impl OutputRegistry {
    pub(super) fn observe(&self) -> OutputObservation {
        let mut counts = lock(&self.0);
        counts.total += 1;
        counts.outstanding += 1;
        OutputObservation(Arc::new(OutputInner {
            registry: self.0.clone(),
            state: Mutex::new(OutputState::Unprepared),
        }))
    }
    pub(super) fn snapshot(&self) -> OutputCounts {
        *lock(&self.0)
    }
}

#[derive(Clone, Copy, Debug)]
enum OutputKind {
    Frame,
    Close,
    None,
}
#[derive(Debug)]
enum OutputState {
    Unprepared,
    Prepared(OutputKind),
    Attempting(OutputKind),
    Terminal,
}

#[derive(Debug)]
struct OutputInner {
    registry: Arc<Mutex<OutputCounts>>,
    state: Mutex<OutputState>,
}

#[derive(Clone, Debug)]
pub(super) struct OutputObservation(Arc<OutputInner>);

pub(super) struct OutputAttempt(OutputObservation);

impl OutputObservation {
    pub(super) fn prepared(&self, terminal: &Option<PreparedWebSocketTerminal>) {
        let mut state = lock(&self.0.state);
        assert!(
            matches!(*state, OutputState::Unprepared),
            "one prepared message decision"
        );
        let mut counts = lock(&self.0.registry);
        let kind = match terminal {
            Some(PreparedWebSocketTerminal::ApplicationFrame(_)) => {
                counts.prepared_frames += 1;
                OutputKind::Frame
            }
            Some(PreparedWebSocketTerminal::Close { .. }) => {
                counts.prepared_closes += 1;
                OutputKind::Close
            }
            None => {
                counts.prepared_no_reply += 1;
                OutputKind::None
            }
        };
        *state = OutputState::Prepared(kind);
    }

    pub(super) fn begin(self) -> OutputAttempt {
        let mut state = lock(&self.0.state);
        let OutputState::Prepared(kind) = *state else {
            panic!("one terminal output attempt");
        };
        lock(&self.0.registry).attempts += 1;
        *state = OutputState::Attempting(kind);
        drop(state);
        OutputAttempt(self)
    }
}

impl OutputAttempt {
    pub(super) fn finish(&self, result: &Result<(), crate::ConnectionError>) {
        let mut state = lock(&self.0.0.state);
        let OutputState::Attempting(kind) = *state else {
            panic!("output attempt must precede its result");
        };
        let mut counts = lock(&self.0.0.registry);
        match result {
            Ok(()) => match kind {
                OutputKind::Frame => counts.queued_frames += 1,
                OutputKind::Close => counts.close_requests_completed += 1,
                OutputKind::None => counts.no_reply += 1,
            },
            Err(crate::ConnectionError::DispatchInterrupted) => counts.interrupted += 1,
            Err(_) => counts.failed += 1,
        }
        counts.outstanding -= 1;
        *state = OutputState::Terminal;
    }
}

impl Drop for OutputAttempt {
    fn drop(&mut self) {
        let mut state = lock(&self.0.0.state);
        if matches!(*state, OutputState::Attempting(_)) {
            let mut counts = lock(&self.0.0.registry);
            if std::thread::panicking() {
                counts.panicked += 1;
            } else {
                counts.interrupted += 1;
            }
            counts.outstanding -= 1;
            *state = OutputState::Terminal;
        }
    }
}

impl Drop for OutputInner {
    fn drop(&mut self) {
        let state = lock(&self.state);
        let mut counts = lock(&self.registry);
        match *state {
            OutputState::Terminal => return,
            OutputState::Unprepared => counts.not_prepared += 1,
            OutputState::Prepared(_) => counts.suppressed += 1,
            OutputState::Attempting(_) if std::thread::panicking() => counts.panicked += 1,
            OutputState::Attempting(_) => counts.interrupted += 1,
        }
        counts.outstanding -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::WsApp;
    use crate::connection::ConnectionManager;
    use std::time::Duration;
    use tokio::time::Instant;
    use tokio_tungstenite::tungstenite::Message;
    use uuid::Uuid;

    fn frame() -> Option<PreparedWebSocketTerminal> {
        Some(PreparedWebSocketTerminal::ApplicationFrame(Message::Text(
            "terminal".into(),
        )))
    }

    #[tokio::test(start_paused = true)]
    async fn root_shortens_pending_terminal_queue_admission_without_claiming_a_write() {
        let manager = ConnectionManager::with_registered_namespaces(8, 8, ["orders".into()]);
        let id = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(id, tx, None, Some("orders".into()))
            .await
            .unwrap();
        manager
            .send_application_frame("orders", id, Message::Text("prefill".into()))
            .await
            .unwrap();
        let registry = OutputRegistry::default();
        let observation = registry.observe();
        let terminal = frame();
        observation.prepared(&terminal);
        let budget = crate::shutdown::ShutdownBudget::default();
        let mut send = Box::pin(WsApp::materialize_message_terminal(
            &manager,
            "orders",
            id,
            terminal,
            &budget,
            Some(observation),
        ));
        assert!(futures_util::poll!(send.as_mut()).is_pending());
        assert_eq!(registry.snapshot().attempts, 1);
        assert_eq!(registry.snapshot().outstanding, 1);
        let start = Instant::now();
        budget.force_before(start + Duration::from_millis(90));
        assert!(matches!(
            send.await,
            Err(crate::ConnectionError::DispatchInterrupted)
        ));
        assert_eq!(start.elapsed(), Duration::from_millis(60));
        let counts = registry.snapshot();
        assert!(counts.reconciles());
        assert_eq!(counts.prepared_frames, 1);
        assert_eq!(counts.queued_frames, 0);
        assert_eq!(counts.interrupted, 1);
        assert_eq!(counts.outstanding, 0);
        assert_eq!(rx.try_recv().unwrap(), Message::Text("prefill".into()));
        assert!(
            rx.try_recv().is_err(),
            "no detached terminal admission may survive the cutoff"
        );
        manager.remove_connection(id).await.unwrap();
    }

    #[tokio::test]
    async fn missing_transport_is_a_failed_attempt_and_unpolled_output_is_suppressed() {
        let manager = ConnectionManager::with_registered_namespaces(8, 8, ["orders".into()]);
        let registry = OutputRegistry::default();
        let budget = crate::shutdown::ShutdownBudget::default();
        for poll in [false, true] {
            let observation = registry.observe();
            let terminal = frame();
            observation.prepared(&terminal);
            let future = WsApp::materialize_message_terminal(
                &manager,
                "orders",
                Uuid::new_v4(),
                terminal,
                &budget,
                Some(observation),
            );
            if poll {
                assert!(future.await.is_err());
            } else {
                drop(future);
            }
        }
        let counts = registry.snapshot();
        assert!(counts.reconciles());
        assert_eq!(counts.total, 2);
        assert_eq!(counts.attempts, 1);
        assert_eq!(counts.failed, 1);
        assert_eq!(counts.suppressed, 1);
        assert_eq!(counts.queued_frames, 0);
        assert_eq!(counts.outstanding, 0);
    }

    #[test]
    fn dropped_and_panicked_attempts_are_terminal_even_when_an_observer_clone_survives() {
        let registry = OutputRegistry::default();
        for panic in [false, true] {
            let observation = registry.observe();
            observation.prepared(&frame());
            let clone = observation.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _attempt = observation.begin();
                if panic {
                    panic!("transport poll panic");
                }
            }));
            assert_eq!(result.is_err(), panic);
            assert_eq!(registry.snapshot().outstanding, 0);
            drop(clone);
        }
        let counts = registry.snapshot();
        assert_eq!(counts.interrupted, 1);
        assert_eq!(counts.panicked, 1);
        assert!(counts.reconciles());
    }
}
