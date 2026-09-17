//! Pull-driven response handoff. Only independent bytes cross into Hyper.
//! The request owner polls and destroys the source, even without transport polls.

use super::*;
use crate::lifecycle::ResponseBodyOutcome;
use bytes::Bytes;
use lily_web_core::{ResponseBodyError, ResponseBodyStream};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;

type Chunk = Option<Result<Bytes, ResponseBodyError>>;

/// Contains no request, scope or callback. Dropping it signals the retained
/// producer; it never drops that producer or implicitly completes its receipt.
pub(crate) struct BodyBridge {
    demand: mpsc::Sender<oneshot::Sender<Chunk>>,
    pending: Option<oneshot::Receiver<Chunk>>,
    disconnected: CancellationToken,
    remaining: Option<u64>,
    done: bool,
}

impl std::fmt::Debug for BodyBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BodyBridge")
            .field("remaining", &self.remaining)
            .field("done", &self.done)
            .finish()
    }
}

impl BodyBridge {
    pub(crate) fn poll_chunk(&mut self, cx: &mut Context<'_>) -> Poll<Chunk> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.pending.is_none() {
            let (send, receive) = oneshot::channel();
            // Exactly one outstanding demand; no speculative producer polls.
            if self.demand.try_send(send).is_err() {
                self.done = true;
                return Poll::Ready(Some(Err(ResponseBodyError::StreamInterrupted)));
            }
            self.pending = Some(receive);
        }
        let result = std::task::ready!(Pin::new(self.pending.as_mut().unwrap()).poll(cx));
        self.pending = None;
        let chunk = result.unwrap_or(Some(Err(ResponseBodyError::StreamInterrupted)));
        match &chunk {
            Some(Ok(bytes)) => {
                self.remaining = self.remaining.map(|n| n.saturating_sub(bytes.len() as u64));
            }
            _ => self.done = true,
        }
        Poll::Ready(chunk)
    }

    pub(crate) fn remaining(&self) -> Option<u64> {
        self.remaining
    }

    pub(crate) fn exact_length_consumed(&self) -> bool {
        // If a protocol has requested EOF and the source is still pending,
        // dropping that request must cancel it even after the last data byte.
        self.remaining == Some(0) && self.pending.is_none()
    }
}

impl Drop for BodyBridge {
    fn drop(&mut self) {
        // A fixed-length HTTP body may be released without a final EOF poll.
        // Closing demand is sufficient to release its source; do not turn a
        // fully consumed representation into execution cancellation. Bytes
        // queued in `pending` have not reduced `remaining` and still cancel.
        if !self.exact_length_consumed() {
            self.disconnected.cancel();
        }
    }
}

pub(super) struct ResponseProducer {
    source: ResponseBodyStream,
    demand: mpsc::Receiver<oneshot::Sender<Chunk>>,
    disconnected: CancellationToken,
    frame_bytes: usize,
    transport_reason: Option<Arc<Mutex<ExecutionStopReason>>>,
}

impl RequestExecutionContext {
    pub(crate) fn own_response_stream(
        &self,
        source: ResponseBodyStream,
        frame_bytes: usize,
        transport_reason: Option<Arc<Mutex<ExecutionStopReason>>>,
    ) -> BodyBridge {
        let remaining = source.exact_length();
        let (send, receive) = mpsc::channel(1);
        let disconnected = CancellationToken::new();
        let producer = ResponseProducer {
            source,
            demand: receive,
            disconnected: disconnected.clone(),
            frame_bytes,
            transport_reason,
        };
        let mut evidence = lock(&self.0.evidence);
        assert!(!evidence.body_created, "one response source per request");
        // Publish before returning the bridge, and while the handler owner is
        // still running. Execution-stop barriers include this new producer.
        evidence.body_created = true;
        evidence.body_streaming = true;
        *lock(&self.0.producer) = Some(producer);
        BodyBridge {
            demand: send,
            pending: None,
            disconnected,
            remaining,
            done: false,
        }
    }

    pub(crate) fn independent_response(&self) {
        let mut evidence = lock(&self.0.evidence);
        evidence.body_created = true;
        let outcome = if evidence.body_suppressed {
            ResponseBodyOutcome::NotStarted
        } else {
            ResponseBodyOutcome::Completed
        };
        evidence.response.observe_body_outcome(outcome);
        evidence.response.observe_source_release();
        evidence.response.observe_bridge_detached();
        self.0.execution_changed.notify_waiters();
    }

    pub(crate) fn suppress_response(&self) {
        lock(&self.0.evidence).body_suppressed = true;
    }

    async fn producer_stop(
        &self,
        disconnected: &CancellationToken,
        transport_reason: Option<&Arc<Mutex<ExecutionStopReason>>>,
    ) {
        let reason = tokio::select! {
            biased;
            _ = self.0.deadline.wait_for_stop() => self.cancellation_reason().expect("stop reason published"),
            _ = disconnected.cancelled() => transport_reason.map_or(ExecutionStopReason::PeerDisconnect, |reason| *lock(reason)),
        };
        self.stop(reason);
    }

    pub(super) async fn finish_response(&self) {
        let producer = lock(&self.0.producer).take();
        let Some(mut producer) = producer else {
            return;
        };
        let context = {
            let resources = self.0.resources.lock().await;
            resources
                .as_ref()
                .and_then(|r| r.scope.as_ref())
                .map(|scope| scope.context().clone())
                .unwrap_or_else(|| self.0.context.clone())
        };
        let work = async {
            let mut started = false;
            let mut reply = None;
            let poll_source = async {
                loop {
                    let Some(sender) = producer.demand.recv().await else {
                        return (
                            if !producer.disconnected.is_cancelled()
                                && producer.source.exact_length()
                                    == Some(producer.source.emitted_bytes())
                            {
                                ResponseBodyOutcome::Completed
                            } else if started {
                                ResponseBodyOutcome::Interrupted
                            } else {
                                ResponseBodyOutcome::NotStarted
                            },
                            Some(Err(ResponseBodyError::StreamInterrupted)),
                        );
                    };
                    reply = Some(sender);
                    // An unstarted body may be discarded without invoking user
                    // code, just like a suppressed HEAD/204/304 representation.
                    if !started && self.0.deadline.cooperative_expired() {
                        return (
                            ResponseBodyOutcome::NotStarted,
                            Some(Err(ResponseBodyError::StreamInterrupted)),
                        );
                    }
                    let chunk = futures::future::poll_fn(|cx| {
                        started = true;
                        Pin::new(&mut producer.source).poll_bounded_chunk(cx, producer.frame_bytes)
                    })
                    .await;
                    let outcome = match &chunk {
                        None => Some(ResponseBodyOutcome::Completed),
                        Some(Err(_)) => Some(ResponseBodyOutcome::Failed),
                        _ => None,
                    };
                    // Bytes::from_owner may retain arbitrary user resources.
                    // Copy the bounded frame so the protocol holds no scoped
                    // destructor or borrowed application allocation.
                    let chunk = match chunk {
                        Some(Ok(bytes)) => {
                            let independent = Bytes::copy_from_slice(&bytes);
                            if catch_unwind(AssertUnwindSafe(|| drop(bytes))).is_err() {
                                lock(&self.0.evidence).resource_release_failed = true;
                                return (
                                    ResponseBodyOutcome::Panicked,
                                    Some(Err(ResponseBodyError::StreamSourceFailed)),
                                );
                            }
                            Some(Ok(independent))
                        }
                        other => other,
                    };
                    if let Some(outcome) = outcome {
                        return (outcome, chunk);
                    }
                    if reply.take().unwrap().send(chunk).is_err() {
                        return (ResponseBodyOutcome::Interrupted, None);
                    }
                }
            };
            // Same pinned producer future survives the signal and keeps
            // polling in the cooperative window; only its slot is cut at C.
            let mut slot = Box::pin(poll_source);
            let result = {
                let invocation = AssertUnwindSafe(slot.as_mut()).catch_unwind();
                tokio::pin!(invocation);
                let stop =
                    self.producer_stop(&producer.disconnected, producer.transport_reason.as_ref());
                tokio::pin!(stop);
                tokio::select! {
                    biased;
                    _ = &mut stop => {
                        tokio::select! {
                            biased;
                            _ = self.cooperative_cutoff() => None,
                            result = &mut invocation => Some(result),
                        }
                    }
                    result = &mut invocation => Some(result),
                }
            };
            drop(slot); // Borrow adapter only; source release is explicit below.
            let (outcome, terminal_chunk) = match result {
                Some(Ok(result)) => result,
                Some(Err(_)) => (
                    ResponseBodyOutcome::Panicked,
                    Some(Err(ResponseBodyError::StreamSourceFailed)),
                ),
                None => (
                    if started {
                        ResponseBodyOutcome::Interrupted
                    } else {
                        ResponseBodyOutcome::NotStarted
                    },
                    Some(Err(ResponseBodyError::StreamInterrupted)),
                ),
            };
            {
                lock(&self.0.evidence)
                    .response
                    .observe_body_outcome(outcome);
            }
            let released = catch_unwind(AssertUnwindSafe(|| drop(producer.source))).is_ok();
            {
                let mut evidence = lock(&self.0.evidence);
                if released {
                    evidence.response.observe_source_release();
                } else {
                    evidence.resource_release_failed = true;
                }
                // The bridge contains only independent bytes and channels.
                evidence.response.observe_bridge_detached();
            }
            tracing::debug!(lily.event = "http.response.producer_terminal", lily.body_outcome = ?outcome,
                lily.source_released = released, "HTTP response source disposition observed");
            if let Some(reply) = reply {
                let _ = reply.send(terminal_chunk);
            }
            // Closing demand also wakes a protocol poll made after this owner
            // finishes. It cannot invoke the destroyed source again.
            drop(producer.demand);
            self.0.execution_changed.notify_waiters();
        };
        ProcessContext::scope(context, self.0.children.scope(work)).await;
        let now = Instant::now();
        let cap = self
            .0
            .cleanup_cap
            .get()
            .copied()
            .unwrap_or(COOPERATIVE_CANCELLATION_CAP);
        let _ = self
            .0
            .body_cleanup_deadline
            .set(now.checked_add(cap).unwrap_or(now));
    }
}
