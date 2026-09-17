//! Explicit, application-owned classification of method results.

use tracing::Span;

/// A classified failure with an application-defined, static telemetry code.
///
/// Codes should be bounded identifiers such as `invalid_credentials`, never
/// error messages, credentials, or request data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceFailure {
    /// The operation was deliberately rejected according to application rules.
    Rejected {
        /// Stable application error code.
        code: &'static str,
    },
    /// A technical or otherwise unexpected application failure occurred.
    Error {
        /// Stable application error code.
        code: &'static str,
    },
}

/// Classifies errors for `#[lily_trace(result)]` without formatting their values.
///
/// Implementations should be cheap, deterministic mappings without side effects.
/// No `Debug`, `Display`, `Clone`, or `std::error::Error` bound is required.
/// This is separate from [`crate::TraceError`], the W3C propagation error type.
pub trait TraceResultError {
    /// Returns the outcome category and safe telemetry code for this error.
    fn trace_failure(&self) -> TraceFailure;
}

impl<E: TraceResultError + ?Sized> TraceResultError for &E {
    fn trace_failure(&self) -> TraceFailure {
        (**self).trace_failure()
    }
}

impl<E: TraceResultError + ?Sized> TraceResultError for Box<E> {
    fn trace_failure(&self) -> TraceFailure {
        (**self).trace_failure()
    }
}

impl<E: TraceResultError + ?Sized> TraceResultError for std::sync::Arc<E> {
    fn trace_failure(&self) -> TraceFailure {
        (**self).trace_failure()
    }
}

impl TraceResultError for std::convert::Infallible {
    fn trace_failure(&self) -> TraceFailure {
        match *self {}
    }
}

/// Records a result on the current span, without consuming or formatting it.
///
/// The span must declare `lily.outcome`, `lily.error_code`, and
/// `otel.status_code` in advance. Macro-created method spans declare these
/// fields automatically. For automatic classification and matching terminal
/// event fields, prefer `#[lily_trace(result)]`; this helper only records span
/// fields and does not finish a method or emit a terminal event.
///
/// A successful or rejected result sets OpenTelemetry status to `OK`; a
/// technical failure sets it to `ERROR`. The result is authoritative for that
/// span's status. An `Ok` result does not manufacture an error code.
/// Call this once with the final result of a span: tracing fields cannot be
/// cleared, so repeated calls cannot erase a previously recorded error code.
pub fn record_result<T, E: TraceResultError>(result: &Result<T, E>) {
    let span = Span::current();
    if !span.is_disabled() {
        Classification::of(result).record(&span);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Classification {
    pub(crate) outcome: &'static str,
    pub(crate) code: Option<&'static str>,
    status: &'static str,
}

impl Classification {
    pub(crate) fn of<T, E: TraceResultError>(result: &Result<T, E>) -> Self {
        match result {
            Ok(_) => Self {
                outcome: "success",
                code: None,
                status: "OK",
            },
            Err(error) => match error.trace_failure() {
                TraceFailure::Rejected { code } => Self {
                    outcome: "rejected",
                    code: Some(code),
                    status: "OK",
                },
                TraceFailure::Error { code } => Self {
                    outcome: "error",
                    code: Some(code),
                    status: "ERROR",
                },
            },
        }
    }

    pub(crate) fn record(self, span: &Span) {
        span.record("lily.outcome", self.outcome);
        if let Some(code) = self.code {
            span.record("lily.error_code", code);
        }
        self.record_status(span);
    }

    pub(crate) fn record_status(self, span: &Span) {
        span.record("otel.status_code", self.status);
    }
}
