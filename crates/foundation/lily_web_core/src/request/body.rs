use async_trait::async_trait;
use bytes::Bytes;
use lily_error::application::http_api::HttpApiError;
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};

use crate::BodyBudget;

/// A safe, protocol-independent failure produced while consuming a request
/// body. Transport implementation details are deliberately not retained so
/// reset codes, peer addresses, and codec errors cannot escape into an HTTP
/// response by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestBodyError {
    /// The advertised or received body exceeded the application limit.
    PayloadTooLarge {
        /// The effective request-body limit.
        limit_bytes: usize,
    },
    /// No frame arrived before the configured body-read deadline.
    ReadTimedOut,
    /// The peer reset the stream or the underlying connection was interrupted.
    TransportInterrupted,
    /// HTTP trailers violated the configured count, size, or encoding limits.
    InvalidTrailers,
    /// A consumer tried to switch body-consumption modes after transport bytes
    /// had already been consumed or whole-body buffering was cancelled.
    AlreadyStreaming,
    /// The configured request-buffer backend could not retain the body.
    BufferUnavailable,
}

impl std::fmt::Display for RequestBodyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadTooLarge { limit_bytes } => {
                write!(formatter, "request body exceeds {limit_bytes} bytes")
            }
            Self::ReadTimedOut => formatter.write_str("request body read deadline exceeded"),
            Self::TransportInterrupted => {
                formatter.write_str("request body transport was interrupted")
            }
            Self::InvalidTrailers => {
                formatter.write_str("request trailers violate configured limits")
            }
            Self::AlreadyStreaming => {
                formatter.write_str("request body consumption mode is already locked")
            }
            Self::BufferUnavailable => formatter.write_str("request body buffer is unavailable"),
        }
    }
}

impl std::error::Error for RequestBodyError {}

impl From<RequestBodyError> for HttpApiError {
    fn from(error: RequestBodyError) -> Self {
        match error {
            RequestBodyError::PayloadTooLarge { limit_bytes } => {
                HttpApiError::PayloadTooLarge(format!("request body exceeds {limit_bytes} bytes"))
            }
            RequestBodyError::ReadTimedOut => {
                HttpApiError::RequestTimeout("request body read deadline exceeded".to_string())
            }
            RequestBodyError::TransportInterrupted => HttpApiError::InvalidRequestBody(
                "request body transport was interrupted".to_string(),
            ),
            RequestBodyError::InvalidTrailers => HttpApiError::InvalidHttpHeader(
                "request trailers violate configured limits".to_string(),
            ),
            RequestBodyError::AlreadyStreaming => HttpApiError::InvalidRequestBody(
                "request body consumption mode is already locked".to_string(),
            ),
            RequestBodyError::BufferUnavailable => {
                HttpApiError::InternalError("request body buffer is unavailable".to_string())
            }
        }
    }
}

/// Protocol adapter used by [`crate::Request`] to pull request data only when
/// application code asks for it. Implementations must preserve transport
/// backpressure: one call may yield at most one data chunk.
#[doc(hidden)]
#[async_trait]
pub trait RequestBodyStream: Send + Sync {
    /// Pulls the next body chunk. `None` is a terminal, repeatable end state.
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError>;

    /// Lower and upper byte bounds known without consuming the stream.
    fn size_hint(&self) -> (u64, Option<u64>);
}

pub(crate) struct RequestBodyTracker {
    state: AtomicU8,
    streamed_bytes: AtomicUsize,
}

impl RequestBodyTracker {
    pub(crate) fn streaming() -> Self {
        Self {
            state: AtomicU8::new(RequestBodyState::Streaming.as_u8()),
            streamed_bytes: AtomicUsize::new(0),
        }
    }

    pub(crate) fn state(&self) -> RequestBodyState {
        RequestBodyState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub(crate) fn set_state(&self, state: RequestBodyState) {
        self.state.store(state.as_u8(), Ordering::Release);
    }

    pub(crate) fn streamed_bytes(&self) -> usize {
        self.streamed_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn set_streamed_bytes(&self, bytes: usize) {
        self.streamed_bytes.store(bytes, Ordering::Release);
    }
}

/// Owned, pull-based access to one request body's terminal consumption slot.
///
/// Framework adapters create this reader only after removing the body source
/// from [`crate::Request`]. It never starts a detached reader task: dropping
/// it also drops the HTTP/1 body or resets the associated HTTP/2 stream.
#[doc(hidden)]
pub struct RequestBodyReader {
    buffered: Option<Bytes>,
    stream: Option<Box<dyn RequestBodyStream>>,
    budget: BodyBudget,
    tracker: Arc<RequestBodyTracker>,
}

impl RequestBodyReader {
    pub(crate) fn new(
        buffered: Option<Bytes>,
        stream: Option<Box<dyn RequestBodyStream>>,
        budget: BodyBudget,
        tracker: Arc<RequestBodyTracker>,
    ) -> Self {
        Self {
            buffered,
            stream,
            budget,
            tracker,
        }
    }

    /// Pulls at most one bounded chunk from the selected body source.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
        if let Some(chunk) = self.buffered.take() {
            self.record_chunk(chunk.len())?;
            return Ok(Some(chunk));
        }

        let Some(stream) = self.stream.as_mut() else {
            self.tracker.set_state(RequestBodyState::Complete);
            return Ok(None);
        };

        match stream.next_chunk().await {
            Ok(Some(chunk)) => {
                if let Err(error) = self.record_chunk(chunk.len()) {
                    self.stream = None;
                    return Err(error);
                }
                Ok(Some(chunk))
            }
            Ok(None) => {
                self.stream = None;
                self.tracker.set_state(RequestBodyState::Complete);
                Ok(None)
            }
            Err(error) => {
                self.stream = None;
                self.tracker.set_state(RequestBodyState::Failed);
                Err(error)
            }
        }
    }

    /// Reports lower and upper remaining byte bounds without consuming data.
    #[must_use]
    pub fn size_hint(&self) -> (u64, Option<u64>) {
        if let Some(buffered) = &self.buffered {
            let length = buffered.len() as u64;
            return (length, Some(length));
        }
        self.stream
            .as_ref()
            .map_or((0, Some(0)), |stream| stream.size_hint())
    }

    /// Number of bytes yielded by this request's incremental authority.
    #[must_use]
    pub fn bytes_read(&self) -> usize {
        self.tracker.streamed_bytes()
    }

    fn record_chunk(&self, length: usize) -> Result<(), RequestBodyError> {
        let current = self.tracker.streamed_bytes();
        let next = match self.budget.checked_next_length(current, length) {
            Ok(next) => next,
            Err(_) => {
                self.tracker.set_state(RequestBodyState::Failed);
                return Err(RequestBodyError::PayloadTooLarge {
                    limit_bytes: self.budget.limit_bytes(),
                });
            }
        };
        self.tracker.set_streamed_bytes(next);
        self.tracker.set_state(RequestBodyState::Streaming);
        Ok(())
    }
}

impl std::fmt::Debug for RequestBodyReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (lower, upper) = self.size_hint();
        formatter
            .debug_struct("RequestBodyReader")
            .field("remaining_lower_bytes", &lower)
            .field("remaining_upper_bytes", &upper)
            .field("bytes_read", &self.bytes_read())
            .finish_non_exhaustive()
    }
}

/// Observable request-body state. It does not expose payload or transport
/// details and is therefore safe to use in diagnostics and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestBodyState {
    /// No body was supplied.
    Empty,
    /// The complete body is buffered.
    Buffered,
    /// A lazy transport stream is available but has not been polled.
    Pending,
    /// Whole-body buffering has begun and the transport is no longer
    /// replayable. Cancellation deliberately leaves this state in place.
    Buffering,
    /// A terminal streaming reader currently owns body consumption.
    Streaming,
    /// The selected body consumer reached end-of-stream.
    Complete,
    /// Body consumption failed.
    Failed,
}

impl RequestBodyState {
    pub(crate) const fn as_u8(self) -> u8 {
        match self {
            Self::Empty => 0,
            Self::Buffered => 1,
            Self::Pending => 2,
            Self::Streaming => 3,
            Self::Complete => 4,
            Self::Failed => 5,
            Self::Buffering => 6,
        }
    }

    pub(crate) const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Empty,
            1 => Self::Buffered,
            2 => Self::Pending,
            3 => Self::Streaming,
            4 => Self::Complete,
            5 => Self::Failed,
            6 => Self::Buffering,
            _ => Self::Failed,
        }
    }
}
