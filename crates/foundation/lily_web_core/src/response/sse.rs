use super::{write_error_response, ResponseWriteError};
use super::{IntoResponse, Response, ResponseWriteOutcome};
use crate::{Request, ResponseBodyError, ResponseBodyStream};
use bytes::Bytes;
use futures::Stream;
use lily_error::application::http_api::HttpApiError;
use pin_project_lite::pin_project;
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{sleep, Sleep},
};

/// Maximum encoded size of one semantic SSE event.
///
/// This matches the default CAP-04 source-chunk authority so a default server
/// can emit every event accepted by this codec as one source chunk.
pub const MAX_SSE_EVENT_BYTES: usize = 64 * 1024;
/// Maximum UTF-8 byte length of an SSE `event` field.
pub const MAX_SSE_EVENT_NAME_BYTES: usize = 1024;
/// Maximum UTF-8 byte length of an SSE `id` or `Last-Event-ID` value.
pub const MAX_SSE_EVENT_ID_BYTES: usize = 1024;
/// Maximum retry hint accepted from application code.
pub const MAX_SSE_RETRY: Duration = Duration::from_secs(24 * 60 * 60);
/// Smallest configurable keepalive interval.
pub const MIN_SSE_KEEP_ALIVE: Duration = Duration::from_secs(1);
/// Largest configurable keepalive interval.
pub const MAX_SSE_KEEP_ALIVE: Duration = Duration::from_secs(5 * 60);
/// Maximum event-count capacity of one built-in per-client channel.
///
/// Together with [`MAX_SSE_EVENT_BYTES`] this bounds worst-case retained event
/// storage to four MiB per client, excluding small channel bookkeeping.
pub const MAX_SSE_CHANNEL_CAPACITY: usize = 64;

const SSE_KEEP_ALIVE_BYTES: Bytes = Bytes::from_static(b": keep-alive\n\n");
const EVENT_FIELD: &[u8] = b"event:";
const ID_FIELD: &[u8] = b"id:";
const RETRY_FIELD: &[u8] = b"retry:";
const DATA_FIELD: &[u8] = b"data:";
const HAS_EVENT: u8 = 1 << 0;
const HAS_ID: u8 = 1 << 1;
const HAS_RETRY: u8 = 1 << 2;

/// A typed, bounded failure while constructing one semantic SSE event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SseEventError {
    /// Encoded `data` lines would exceed the event limit.
    #[error("SSE data exceeds the encoded {limit_bytes}-byte event limit")]
    DataTooLarge {
        /// Maximum encoded event bytes.
        limit_bytes: usize,
    },
    /// The optional event name exceeded its bound.
    #[error("SSE event name exceeds the configured {limit_bytes}-byte limit")]
    EventNameTooLong {
        /// Maximum event-name bytes.
        limit_bytes: usize,
    },
    /// The optional event identifier exceeded its bound.
    #[error("SSE event identifier exceeds the configured {limit_bytes}-byte limit")]
    EventIdTooLong {
        /// Maximum identifier bytes.
        limit_bytes: usize,
    },
    /// A single-line field contained CR or LF.
    #[error("SSE {field} field contains a forbidden line terminator")]
    LineTerminator {
        /// Stable field name.
        field: &'static str,
    },
    /// An event identifier contained NUL.
    #[error("SSE event identifier contains a forbidden null character")]
    NullEventId,
    /// A single-assignment field was configured twice.
    #[error("SSE {field} field was configured more than once")]
    DuplicateField {
        /// Stable duplicated field name.
        field: &'static str,
    },
    /// The retry duration was zero or exceeded its ceiling.
    #[error("SSE retry duration must be in 1ms..=24h")]
    InvalidRetry,
    /// Allocation failed while remaining within the event bound.
    #[error("SSE event allocation failed within the configured bound")]
    AllocationFailed,
}

impl From<SseEventError> for HttpApiError {
    fn from(error: SseEventError) -> Self {
        Self::ResponseEncodingError(error.to_string())
    }
}

/// A typed, bounded configuration failure for one SSE response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SseConfigError {
    /// The keepalive interval was outside the accepted range.
    #[error("SSE keepalive interval must be in 1s..=5m")]
    InvalidKeepAlive,
    /// The channel capacity was zero or exceeded its bound.
    #[error("SSE channel capacity must be in 1..={maximum}")]
    InvalidChannelCapacity {
        /// Maximum event capacity.
        maximum: usize,
    },
}

impl From<SseConfigError> for HttpApiError {
    fn from(error: SseConfigError) -> Self {
        Self::ResponseEncodingError(error.to_string())
    }
}

/// A fail-closed `Last-Event-ID` request-header failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LastEventIdError {
    /// More than one `Last-Event-ID` field was supplied.
    #[error("Last-Event-ID must occur at most once")]
    Duplicate,
    /// The cursor exceeded its byte bound.
    #[error("Last-Event-ID exceeds the configured {limit_bytes}-byte limit")]
    TooLong {
        /// Maximum cursor bytes.
        limit_bytes: usize,
    },
    /// The cursor contained CR, LF, or NUL.
    #[error("Last-Event-ID contains a forbidden character")]
    InvalidValue,
}

impl From<LastEventIdError> for HttpApiError {
    fn from(error: LastEventIdError) -> Self {
        Self::BadRequest(error.to_string())
    }
}

/// One validated, opaque browser reconnection cursor.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct LastEventId(String);

impl LastEventId {
    /// Validates an opaque event identifier without trimming or decoding it.
    pub fn new(value: impl Into<String>) -> Result<Self, LastEventIdError> {
        let value = value.into();
        validate_last_event_id(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    /// Returns the opaque cursor.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    /// Consumes the wrapper and returns the opaque cursor.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Debug for LastEventId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LastEventId")
            .field("byte_len", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl TryFrom<String> for LastEventId {
    type Error = LastEventIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for LastEventId {
    type Error = LastEventIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

fn validate_last_event_id(value: &str) -> Result<(), LastEventIdError> {
    if value.len() > MAX_SSE_EVENT_ID_BYTES {
        return Err(LastEventIdError::TooLong {
            limit_bytes: MAX_SSE_EVENT_ID_BYTES,
        });
    }
    if value
        .as_bytes()
        .iter()
        .any(|byte| matches!(byte, b'\r' | b'\n' | 0))
    {
        return Err(LastEventIdError::InvalidValue);
    }
    Ok(())
}

/// Parses one optional `Last-Event-ID` value from the exact request boundary.
pub(crate) fn request_last_event_id(
    request: &Request,
) -> Result<Option<LastEventId>, LastEventIdError> {
    let mut values = request
        .header_cache()
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case("last-event-id"));
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(LastEventIdError::Duplicate);
    }
    LastEventId::new(value.value.as_str()).map(Some)
}

/// One validated semantic server-sent event.
///
/// Its byte representation is created during construction and is therefore
/// infallible once response headers have committed. `Debug` never exposes
/// event data, names, or identifiers.
#[derive(Clone, PartialEq, Eq)]
pub struct SseEvent {
    encoded: Bytes,
    fields: u8,
}

impl SseEvent {
    /// Creates one UTF-8 SSE data event with canonical multi-line encoding.
    pub fn new(data: impl AsRef<str>) -> Result<Self, SseEventError> {
        Ok(Self {
            encoded: encode_data(data.as_ref())?,
            fields: 0,
        })
    }

    /// Adds one event type. CR and LF are rejected instead of being encoded.
    pub fn event(self, value: impl AsRef<str>) -> Result<Self, SseEventError> {
        let value = value.as_ref();
        if self.fields & HAS_EVENT != 0 {
            return Err(SseEventError::DuplicateField { field: "event" });
        }
        if value.len() > MAX_SSE_EVENT_NAME_BYTES {
            return Err(SseEventError::EventNameTooLong {
                limit_bytes: MAX_SSE_EVENT_NAME_BYTES,
            });
        }
        validate_no_line_terminator("event", value)?;
        self.append_field(EVENT_FIELD, value.as_bytes(), HAS_EVENT)
    }

    /// Adds one event identifier. CR, LF, and NUL are rejected.
    pub fn id(self, value: impl AsRef<str>) -> Result<Self, SseEventError> {
        let value = value.as_ref();
        if self.fields & HAS_ID != 0 {
            return Err(SseEventError::DuplicateField { field: "id" });
        }
        if value.len() > MAX_SSE_EVENT_ID_BYTES {
            return Err(SseEventError::EventIdTooLong {
                limit_bytes: MAX_SSE_EVENT_ID_BYTES,
            });
        }
        validate_no_line_terminator("id", value)?;
        if value.as_bytes().contains(&0) {
            return Err(SseEventError::NullEventId);
        }
        self.append_field(ID_FIELD, value.as_bytes(), HAS_ID)
    }

    /// Adds the browser reconnection hint in canonical integer milliseconds.
    pub fn retry(self, duration: Duration) -> Result<Self, SseEventError> {
        if self.fields & HAS_RETRY != 0 {
            return Err(SseEventError::DuplicateField { field: "retry" });
        }
        if duration < Duration::from_millis(1) || duration > MAX_SSE_RETRY {
            return Err(SseEventError::InvalidRetry);
        }
        let millis =
            u64::try_from(duration.as_millis()).map_err(|_| SseEventError::InvalidRetry)?;
        let mut buffer = itoa::Buffer::new();
        self.append_field(RETRY_FIELD, buffer.format(millis).as_bytes(), HAS_RETRY)
    }

    #[must_use]
    /// Returns the canonical encoded event length.
    pub fn encoded_len(&self) -> usize {
        self.encoded.len()
    }

    #[must_use]
    /// Consumes the event and returns its canonical wire bytes.
    pub fn into_bytes(self) -> Bytes {
        self.encoded
    }

    fn append_field(
        mut self,
        prefix: &'static [u8],
        value: &[u8],
        flag: u8,
    ) -> Result<Self, SseEventError> {
        let additional = prefix
            .len()
            .checked_add(value.len())
            .and_then(|length| length.checked_add(1))
            .ok_or(SseEventError::DataTooLarge {
                limit_bytes: MAX_SSE_EVENT_BYTES,
            })?;
        let encoded_len =
            self.encoded
                .len()
                .checked_add(additional)
                .ok_or(SseEventError::DataTooLarge {
                    limit_bytes: MAX_SSE_EVENT_BYTES,
                })?;
        if encoded_len > MAX_SSE_EVENT_BYTES {
            return Err(SseEventError::DataTooLarge {
                limit_bytes: MAX_SSE_EVENT_BYTES,
            });
        }
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(encoded_len)
            .map_err(|_| SseEventError::AllocationFailed)?;
        let terminator = self
            .encoded
            .len()
            .checked_sub(1)
            .expect("validated SSE events always end in one blank line");
        encoded.extend_from_slice(&self.encoded[..terminator]);
        encoded.extend_from_slice(prefix);
        encoded.extend_from_slice(value);
        encoded.extend_from_slice(b"\n\n");
        self.encoded = Bytes::from(encoded);
        self.fields |= flag;
        Ok(self)
    }
}

impl std::fmt::Debug for SseEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SseEvent")
            .field("encoded_len", &self.encoded.len())
            .field("has_event", &(self.fields & HAS_EVENT != 0))
            .field("has_id", &(self.fields & HAS_ID != 0))
            .field("has_retry", &(self.fields & HAS_RETRY != 0))
            .finish()
    }
}

fn validate_no_line_terminator(field: &'static str, value: &str) -> Result<(), SseEventError> {
    if value
        .as_bytes()
        .iter()
        .any(|byte| matches!(byte, b'\r' | b'\n'))
    {
        return Err(SseEventError::LineTerminator { field });
    }
    Ok(())
}

fn encode_data(data: &str) -> Result<Bytes, SseEventError> {
    let data = data.as_bytes();
    let encoded_len = encoded_data_len(data)?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(encoded_len)
        .map_err(|_| SseEventError::AllocationFailed)?;
    for_each_data_line(data, |line| {
        encoded.extend_from_slice(DATA_FIELD);
        encoded.extend_from_slice(line);
        encoded.extend_from_slice(b"\n");
    });
    encoded.extend_from_slice(b"\n");
    debug_assert_eq!(encoded.len(), encoded_len);
    Ok(Bytes::from(encoded))
}

fn encoded_data_len(data: &[u8]) -> Result<usize, SseEventError> {
    let mut total = 1usize;
    let mut overflow = false;
    for_each_data_line(data, |line| {
        total = total
            .checked_add(DATA_FIELD.len())
            .and_then(|length| length.checked_add(line.len()))
            .and_then(|length| length.checked_add(1))
            .unwrap_or_else(|| {
                overflow = true;
                usize::MAX
            });
    });
    if overflow || total > MAX_SSE_EVENT_BYTES {
        return Err(SseEventError::DataTooLarge {
            limit_bytes: MAX_SSE_EVENT_BYTES,
        });
    }
    Ok(total)
}

fn for_each_data_line(mut data: &[u8], mut visitor: impl FnMut(&[u8])) {
    loop {
        let separator = data.iter().position(|byte| matches!(byte, b'\r' | b'\n'));
        let Some(separator) = separator else {
            visitor(data);
            return;
        };
        visitor(&data[..separator]);
        let skip = usize::from(
            data[separator] == b'\r' && data.get(separator + 1).is_some_and(|byte| *byte == b'\n'),
        ) + 1;
        data = &data[separator + skip..];
    }
}

#[derive(Debug, Default)]
struct SseRuntimeConfig {
    keep_alive_millis: AtomicU64,
}

pin_project! {
    struct SseByteStream<S> {
        #[pin]
        source: S,
        config: Arc<SseRuntimeConfig>,
        #[pin]
        keep_alive_sleep: Option<Sleep>,
        terminal: bool,
    }
}

impl<S> SseByteStream<S> {
    fn new(source: S, config: Arc<SseRuntimeConfig>) -> Self {
        Self {
            source,
            config,
            keep_alive_sleep: None,
            terminal: false,
        }
    }
}

impl<S, E> Stream for SseByteStream<S>
where
    S: Stream<Item = Result<SseEvent, E>>,
    E: Into<ResponseBodyError>,
{
    type Item = Result<Bytes, ResponseBodyError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.terminal {
            return Poll::Ready(None);
        }

        match this.source.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(event))) => {
                reset_keep_alive(this.keep_alive_sleep.as_mut(), this.config);
                return Poll::Ready(Some(Ok(event.into_bytes())));
            }
            Poll::Ready(Some(Err(error))) => {
                *this.terminal = true;
                this.keep_alive_sleep.set(None);
                return Poll::Ready(Some(Err(error.into())));
            }
            Poll::Ready(None) => {
                *this.terminal = true;
                this.keep_alive_sleep.set(None);
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        let interval = keep_alive_interval(this.config);
        let Some(interval) = interval else {
            return Poll::Pending;
        };
        if this.keep_alive_sleep.as_ref().get_ref().is_none() {
            this.keep_alive_sleep.set(Some(sleep(interval)));
        }
        let Some(timer) = this.keep_alive_sleep.as_mut().as_pin_mut() else {
            unreachable!("SSE keepalive timer was initialized")
        };
        if timer.poll(context).is_ready() {
            this.keep_alive_sleep.set(Some(sleep(interval)));
            return Poll::Ready(Some(Ok(SSE_KEEP_ALIVE_BYTES)));
        }
        Poll::Pending
    }
}

fn keep_alive_interval(config: &SseRuntimeConfig) -> Option<Duration> {
    let millis = config.keep_alive_millis.load(Ordering::Acquire);
    (millis != 0).then(|| Duration::from_millis(millis))
}

fn reset_keep_alive(mut timer: Pin<&mut Option<Sleep>>, config: &SseRuntimeConfig) {
    timer.set(keep_alive_interval(config).map(sleep));
}

/// One concrete typed SSE response returned directly from a controller action.
pub struct SseResponse {
    body: ResponseBodyStream,
    config: Arc<SseRuntimeConfig>,
}

impl std::fmt::Debug for SseResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SseResponse")
            .field("keep_alive", &keep_alive_interval(&self.config))
            .field("body", &self.body)
            .finish()
    }
}

impl SseResponse {
    /// Type-erases one event source without polling it.
    pub fn new<S, E>(source: S) -> Self
    where
        S: Stream<Item = Result<SseEvent, E>> + Send + 'static,
        E: Into<ResponseBodyError> + 'static,
    {
        let config = Arc::new(SseRuntimeConfig::default());
        let body = ResponseBodyStream::new(SseByteStream::new(source, Arc::clone(&config)));
        Self { body, config }
    }

    /// Enables comment-based keepalive after the configured idle interval.
    pub fn keep_alive(self, interval: Duration) -> Result<Self, SseConfigError> {
        if !(MIN_SSE_KEEP_ALIVE..=MAX_SSE_KEEP_ALIVE).contains(&interval) {
            return Err(SseConfigError::InvalidKeepAlive);
        }
        let millis =
            u64::try_from(interval.as_millis()).map_err(|_| SseConfigError::InvalidKeepAlive)?;
        self.config
            .keep_alive_millis
            .store(millis, Ordering::Release);
        Ok(self)
    }

    /// Builds one bounded push channel and its single client response.
    pub fn channel(capacity: usize) -> Result<(SseSender, Self), SseConfigError> {
        if capacity == 0 || capacity > MAX_SSE_CHANNEL_CAPACITY {
            return Err(SseConfigError::InvalidChannelCapacity {
                maximum: MAX_SSE_CHANNEL_CAPACITY,
            });
        }
        let (sender, receiver) = mpsc::channel(capacity);
        let source = SseReceiverStream { receiver };
        Ok((SseSender { sender }, Self::new(source)))
    }
}

/// Wraps a fallible semantic event stream for direct controller return.
///
/// ```
/// use std::{convert::Infallible, time::Duration};
///
/// use lily_web_core::{sse, SseEvent, SseResponse};
///
/// # fn response() -> Result<SseResponse, Box<dyn std::error::Error>> {
/// let event = SseEvent::new("order completed")?
///     .event("order-status")?
///     .id("event-42")?
///     .retry(Duration::from_secs(5))?;
/// let source = futures::stream::iter([Ok::<_, Infallible>(event)]);
/// let response = sse(source).keep_alive(Duration::from_secs(15))?;
/// # Ok(response)
/// # }
/// ```
pub fn sse<S, E>(source: S) -> SseResponse
where
    S: Stream<Item = Result<SseEvent, E>> + Send + 'static,
    E: Into<ResponseBodyError> + 'static,
{
    SseResponse::new(source)
}

/// Creates one bounded per-client SSE push channel.
pub fn sse_channel(capacity: usize) -> Result<(SseSender, SseResponse), SseConfigError> {
    SseResponse::channel(capacity)
}

#[async_trait::async_trait]
impl IntoResponse for SseResponse {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        response.replace_streaming(
            200,
            "OK",
            [
                ("Content-Type", "text/event-stream"),
                ("Cache-Control", "no-cache, no-transform"),
                ("X-Accel-Buffering", "no"),
            ],
            self.body,
        )?;
        Ok(ResponseWriteOutcome::authoritative())
    }
}

#[async_trait::async_trait]
impl<E> IntoResponse for Result<SseResponse, E>
where
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Ok(value) => value.write_to_response(response, request).await,
            Err(error) => write_error_response(error, response, request).await,
        }
    }
}

struct SseReceiverStream {
    receiver: mpsc::Receiver<SseEvent>,
}

impl Stream for SseReceiverStream {
    type Item = Result<SseEvent, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver)
            .poll_recv(context)
            .map(|event| event.map(Ok))
    }
}

/// Cloneable producer for one bounded per-client SSE channel.
#[derive(Clone)]
pub struct SseSender {
    sender: mpsc::Sender<SseEvent>,
}

impl std::fmt::Debug for SseSender {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SseSender")
            .field("remaining_capacity", &self.sender.capacity())
            .field("closed", &self.sender.is_closed())
            .finish()
    }
}

impl SseSender {
    /// Waits for bounded capacity instead of growing or silently dropping.
    pub async fn send(&self, event: SseEvent) -> Result<(), SseSendError> {
        self.sender
            .send(event)
            .await
            .map_err(|error| SseSendError::Closed(error.0))
    }

    /// Attempts an immediate send and returns the event on full or closed state.
    pub fn try_send(&self, event: SseEvent) -> Result<(), SseTrySendError> {
        self.sender.try_send(event).map_err(|error| match error {
            mpsc::error::TrySendError::Full(event) => SseTrySendError::Full(event),
            mpsc::error::TrySendError::Closed(event) => SseTrySendError::Closed(event),
        })
    }

    #[must_use]
    /// Returns the number of events that can be enqueued immediately.
    pub fn remaining_capacity(&self) -> usize {
        self.sender.capacity()
    }

    #[must_use]
    /// Reports whether the client response has closed.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Completes when the client response has dropped its receiver.
    pub async fn closed(&self) {
        self.sender.closed().await;
    }
}

/// Failure from an awaited bounded SSE send.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum SseSendError {
    /// The client response closed; contains the unsent event.
    #[error("SSE client channel is closed")]
    Closed(SseEvent),
}

/// Failure from an immediate bounded SSE send.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum SseTrySendError {
    /// The bounded channel is full; contains the unsent event.
    #[error("SSE client channel is full")]
    Full(SseEvent),
    /// The client response closed; contains the unsent event.
    #[error("SSE client channel is closed")]
    Closed(SseEvent),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BodyBudget, RequestExt, ResponseLimits};
    use futures::{stream, StreamExt};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn event_codec_is_canonical_for_multiline_fields_and_retry() {
        let event = SseEvent::new("first\r\nsecond\rthird\nfourth\n")
            .unwrap()
            .event("notification")
            .unwrap()
            .id("event-42")
            .unwrap()
            .retry(Duration::from_millis(2500))
            .unwrap();
        assert_eq!(
            event.into_bytes(),
            Bytes::from_static(
                b"data:first\ndata:second\ndata:third\ndata:fourth\ndata:\nevent:notification\nid:event-42\nretry:2500\n\n"
            )
        );
    }

    #[test]
    fn event_fields_are_bounded_injection_safe_and_single_assignment() {
        assert_eq!(
            SseEvent::new("ok").unwrap().event("bad\nname"),
            Err(SseEventError::LineTerminator { field: "event" })
        );
        assert_eq!(
            SseEvent::new("ok").unwrap().id("bad\0id"),
            Err(SseEventError::NullEventId)
        );
        assert_eq!(
            SseEvent::new("ok")
                .unwrap()
                .id("first")
                .unwrap()
                .id("second"),
            Err(SseEventError::DuplicateField { field: "id" })
        );
        assert_eq!(
            SseEvent::new("ok").unwrap().retry(Duration::ZERO),
            Err(SseEventError::InvalidRetry)
        );
        assert_eq!(
            SseEvent::new("ok")
                .unwrap()
                .retry(Duration::from_micros(999)),
            Err(SseEventError::InvalidRetry)
        );
        assert_eq!(
            SseEvent::new("\n".repeat(MAX_SSE_EVENT_BYTES)),
            Err(SseEventError::DataTooLarge {
                limit_bytes: MAX_SSE_EVENT_BYTES,
            })
        );
    }

    #[test]
    fn event_and_last_id_debug_never_expose_application_values() {
        let event = SseEvent::new("payload-secret")
            .unwrap()
            .event("type-secret")
            .unwrap()
            .id("id-secret")
            .unwrap();
        let event_debug = format!("{event:?}");
        assert!(!event_debug.contains("payload-secret"));
        assert!(!event_debug.contains("type-secret"));
        assert!(!event_debug.contains("id-secret"));

        let id = LastEventId::new("last-secret").unwrap();
        assert!(!format!("{id:?}").contains("last-secret"));
    }

    #[test]
    fn last_event_id_is_opaque_bounded_and_duplicate_fail_closed() {
        let absent = Request::new_test("GET", "/events");
        assert_eq!(absent.last_event_id(), Ok(None));

        let mut present = Request::new_test("GET", "/events");
        present.add_test_header("Last-Event-ID", "tenant/42:event-7");
        assert_eq!(
            present.last_event_id().unwrap().unwrap().as_str(),
            "tenant/42:event-7"
        );

        let mut duplicate = Request::new_test("GET", "/events");
        duplicate.add_test_header("Last-Event-ID", "first");
        duplicate.add_test_header("last-event-id", "second");
        assert_eq!(duplicate.last_event_id(), Err(LastEventIdError::Duplicate));

        let mut invalid = Request::new_test("GET", "/events");
        invalid.add_test_header("Last-Event-ID", "bad\0id");
        assert_eq!(invalid.last_event_id(), Err(LastEventIdError::InvalidValue));

        let exact = "x".repeat(MAX_SSE_EVENT_ID_BYTES);
        assert_eq!(LastEventId::new(exact.clone()).unwrap().as_str(), exact);
        assert_eq!(
            LastEventId::new("x".repeat(MAX_SSE_EVENT_ID_BYTES + 1)),
            Err(LastEventIdError::TooLong {
                limit_bytes: MAX_SSE_EVENT_ID_BYTES,
            })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn keep_alive_is_idle_reset_comment_and_not_an_application_event() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let source = futures::stream::poll_fn(move |_context| {
            observed.fetch_add(1, Ordering::AcqRel);
            Poll::<Option<Result<SseEvent, Infallible>>>::Pending
        });
        let mut response = sse(source).keep_alive(Duration::from_secs(5)).unwrap();

        let frame = tokio::spawn(async move { response.body.next().await.unwrap().unwrap() });
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(frame.await.unwrap(), SSE_KEEP_ALIVE_BYTES);
        assert!(polls.load(Ordering::Acquire) >= 1);
    }

    #[tokio::test]
    async fn sse_response_is_lazy_and_sets_canonical_headers() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let source = futures::stream::poll_fn(move |_context| {
            observed.fetch_add(1, Ordering::AcqRel);
            Poll::Ready(Some(Ok::<_, Infallible>(SseEvent::new("hello").unwrap())))
        });
        let value: Result<SseResponse, HttpApiError> = Ok(sse(source));
        let limits = ResponseLimits::new(BodyBudget::new(1024).unwrap(), 16, 4096).unwrap();
        let mut response = Response::with_limits(limits).await.unwrap();
        let mut request = Request::new_test("GET", "/events");
        let outcome = value
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();
        assert_eq!(outcome, ResponseWriteOutcome::authoritative());
        assert_eq!(polls.load(Ordering::Acquire), 0);

        let mut parts = response.into_transport_parts().unwrap();
        assert_eq!(
            parts
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
                .map(|(_, value)| value.as_str()),
            Some("text/event-stream")
        );
        assert_eq!(
            parts
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("cache-control"))
                .map(|(_, value)| value.as_str()),
            Some("no-cache, no-transform")
        );
        let frame = parts
            .stream
            .as_mut()
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame, Bytes::from_static(b"data:hello\n\n"));
    }

    #[tokio::test]
    async fn bounded_channel_applies_backpressure_and_closes_with_the_response() {
        assert!(matches!(
            sse_channel(0),
            Err(SseConfigError::InvalidChannelCapacity { .. })
        ));
        assert!(matches!(
            sse_channel(MAX_SSE_CHANNEL_CAPACITY + 1),
            Err(SseConfigError::InvalidChannelCapacity { .. })
        ));
        assert!(matches!(
            sse(stream::empty::<Result<SseEvent, Infallible>>())
                .keep_alive(MIN_SSE_KEEP_ALIVE - Duration::from_millis(1)),
            Err(SseConfigError::InvalidKeepAlive)
        ));
        assert!(matches!(
            sse(stream::empty::<Result<SseEvent, Infallible>>())
                .keep_alive(MAX_SSE_KEEP_ALIVE + Duration::from_millis(1)),
            Err(SseConfigError::InvalidKeepAlive)
        ));

        let (sender, mut response) = sse_channel(1).unwrap();
        sender.try_send(SseEvent::new("first").unwrap()).unwrap();
        let second = SseEvent::new("second").unwrap();
        assert!(matches!(
            sender.try_send(second),
            Err(SseTrySendError::Full(_))
        ));

        let waiting_sender = sender.clone();
        let waiting_send =
            tokio::spawn(
                async move { waiting_sender.send(SseEvent::new("second").unwrap()).await },
            );
        tokio::task::yield_now().await;
        assert!(!waiting_send.is_finished());
        assert_eq!(
            response.body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"data:first\n\n")
        );
        waiting_send.await.unwrap().unwrap();

        drop(response);
        sender.closed().await;
        assert!(sender.is_closed());
        assert!(matches!(
            sender.send(SseEvent::new("third").unwrap()).await,
            Err(SseSendError::Closed(_))
        ));
    }

    #[tokio::test]
    async fn source_completion_stops_keepalive_and_channel_drains_in_order() {
        let source = stream::iter([
            Ok::<_, Infallible>(SseEvent::new("one").unwrap()),
            Ok(SseEvent::new("two").unwrap()),
        ]);
        let mut response = sse(source).keep_alive(Duration::from_secs(5)).unwrap();
        assert_eq!(
            response.body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"data:one\n\n")
        );
        assert_eq!(
            response.body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"data:two\n\n")
        );
        assert!(response.body.next().await.is_none());
    }
}
