//! Generated-macro implementation support, not an application API.

use std::time::Instant;

use tracing::Span;

use crate::result::Classification;
use crate::TraceResultError;

/// Fields passed to the callsite-owned event emitter.
#[doc(hidden)]
pub struct LifecycleEvent {
    pub phase: &'static str,
    pub duration_ms: Option<f64>,
    pub outcome: Option<&'static str>,
    pub error_code: Option<&'static str>,
}

/// Owns method execution timing independently of the span's reference lifetime.
#[doc(hidden)]
pub struct OperationGuard<Emit: Fn(&Span, LifecycleEvent)> {
    span: Span,
    started: Instant,
    finished: bool,
    emit: Emit,
}

impl<Emit: Fn(&Span, LifecycleEvent)> OperationGuard<Emit> {
    pub fn start(span: Span, emit: Emit) -> Self {
        let guard = Self {
            span,
            started: Instant::now(),
            finished: false,
            emit,
        };
        // The start event carries "started". Record only the terminal phase
        // on the span: tracing-opentelemetry appends repeated field records,
        // which otherwise exports two conflicting attributes with this key.
        guard.publish(LifecycleEvent {
            phase: "started",
            duration_ms: None,
            outcome: None,
            error_code: None,
        });
        guard
    }

    pub fn finish(mut self) {
        self.complete("completed", None);
    }

    pub fn finish_result<T, E: TraceResultError>(mut self, result: &Result<T, E>) {
        self.complete("completed", Some(Classification::of(result)));
    }

    fn complete(&mut self, phase: &'static str, classification: Option<Classification>) {
        // Mark terminal before recording: a subscriber panic must not emit twice.
        self.finished = true;
        let duration_ms = self.started.elapsed().as_secs_f64() * 1_000.0;
        self.span.record("lily.lifecycle", phase);
        self.span.record("lily.duration_ms", duration_ms);
        if let Some(classification) = classification {
            classification.record(&self.span);
        }
        self.publish(LifecycleEvent {
            phase,
            duration_ms: Some(duration_ms),
            outcome: classification.map(|value| value.outcome),
            error_code: classification.and_then(|value| value.code),
        });
        // An ERROR-level lifecycle event may influence tracing-opentelemetry.
        // Explicit result classification remains authoritative after that event.
        if let Some(classification) = classification {
            classification.record_status(&self.span);
        } else if phase == "panicked" {
            self.span.record("otel.status_code", "ERROR");
        }
    }

    fn publish(&self, event: LifecycleEvent) {
        // Drop can run outside the dispatch that originally polled the future.
        // Use the span's own dispatch and enter only for this synchronous event.
        // Entering also preserves context for custom layers using current spans.
        self.span.with_subscriber(|(_, dispatch)| {
            tracing::dispatcher::with_default(dispatch, || {
                self.span.in_scope(|| (self.emit)(&self.span, event));
            });
        });
    }
}

impl<Emit: Fn(&Span, LifecycleEvent)> Drop for OperationGuard<Emit> {
    fn drop(&mut self) {
        if !self.finished {
            self.complete(
                if std::thread::panicking() {
                    "panicked"
                } else {
                    "dropped"
                },
                None,
            );
        }
    }
}
