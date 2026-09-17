//! One cancellation receipt for an accepted message, shared by its slot,
//! callbacks and outbound authority. Observers cannot restart cooperation.

use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(crate) const MESSAGE_COOPERATIVE_CAP: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MessageCancellationCause {
    MessageTimeout,
    ConnectionCancelled,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MessageCancellationRequest {
    pub(crate) cause: MessageCancellationCause,
    pub(crate) at: Instant,
    pub(crate) cooperative_deadline: Instant,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MessageCancellationFacts {
    pub(crate) request: Option<MessageCancellationRequest>,
    pub(crate) deadline_exceeded: bool,
    pub(crate) terminal: bool,
}

pub(super) struct MessageCancellation {
    token: CancellationToken,
    deadline: OnceLock<Instant>,
    facts: Mutex<MessageCancellationFacts>,
}

impl MessageCancellation {
    pub(super) fn new(token: CancellationToken) -> Self {
        Self {
            token,
            deadline: OnceLock::new(),
            facts: Mutex::default(),
        }
    }

    pub(super) fn set_deadline(&self, deadline: Instant) {
        assert_eq!(
            *self.deadline.get_or_init(|| deadline),
            deadline,
            "one message deadline"
        );
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        self.deadline.get().copied()
    }

    pub(super) fn recorded_facts(&self) -> MessageCancellationFacts {
        *self
            .facts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The first recorded signal wins. When both sources become observable in
    /// one poll, connection cancellation wins; elapsed local time still clips
    /// the window. The local timeout is anchored to its original deadline.
    pub(super) fn observe(&self) -> MessageCancellationFacts {
        let now = Instant::now();
        let mut facts = self
            .facts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if facts.terminal {
            return *facts;
        }
        let expired = self.deadline().filter(|deadline| now >= *deadline);
        facts.deadline_exceeded |= expired.is_some();
        if facts.request.is_none() {
            let request = if self.token.is_cancelled() {
                Some((MessageCancellationCause::ConnectionCancelled, now))
            } else {
                expired.map(|at| (MessageCancellationCause::MessageTimeout, at))
            };
            if let Some((cause, at)) = request {
                let cooperative_deadline =
                    expired.map_or(at + MESSAGE_COOPERATIVE_CAP, |deadline| {
                        (at + MESSAGE_COOPERATIVE_CAP).min(deadline + MESSAGE_COOPERATIVE_CAP)
                    });
                facts.request = Some(MessageCancellationRequest {
                    cause,
                    at,
                    cooperative_deadline,
                });
            }
        }
        let snapshot = *facts;
        drop(facts);
        if snapshot.request.is_some() {
            self.token.cancel();
        }
        snapshot
    }

    pub(super) fn request_cancel(&self) {
        // Observe an already elapsed deadline before publishing another cause.
        self.observe();
        self.token.cancel();
        self.observe();
    }

    pub(super) fn finish(&self) -> MessageCancellationFacts {
        self.observe();
        let mut facts = self
            .facts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        facts.terminal = true;
        *facts
    }
}
