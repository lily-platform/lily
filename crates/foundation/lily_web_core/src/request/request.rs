use std::collections::HashMap;
use std::fmt;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, OnceLock,
};

use crate::request::{
    multipart::{DEFAULT_MAX_PARTS, DEFAULT_MAX_PART_BYTES, DEFAULT_MAX_RETAINED_METADATA_BYTES},
    Principal, QueryParams, QueryParseError, QueryValues, RequestBodyError, RequestBodyReader,
    RequestBodyState, RequestBodyStream, RequestBodyTracker, RequestConnectionInfo,
    RequestExtensions,
};
use crate::{BodyBudget, HttpBuffer, RequestCookieError, RequestCookieJar};
use bytes::Bytes;

use crate::string_interner::{preserve_opaque_owned, InternedString};
use lily_cancellation::{__private::inactive_execution_cancellation, ExecutionCancellation};
use lily_core::structs::RawHeader;
use lily_error::{application::http_api::HttpApiError, LocalizationCatalog};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug, Clone, Copy)]
struct RequestMultipartLimits {
    max_part_bytes: usize,
    max_parts: usize,
    max_retained_metadata_bytes: usize,
}

impl RequestMultipartLimits {
    fn for_budget(body_budget: BodyBudget) -> Self {
        Self {
            max_part_bytes: body_budget.limit_bytes().min(DEFAULT_MAX_PART_BYTES),
            max_parts: DEFAULT_MAX_PARTS,
            max_retained_metadata_bytes: DEFAULT_MAX_RETAINED_METADATA_BYTES,
        }
    }
}

/// One parsed HTTP request and its request-scoped state.
///
/// The transport constructs this value. Application middleware and actions
/// inspect metadata, attach a verified [`Principal`], use [`RequestExtensions`]
/// for request-local values, and select exactly one body-consumption mode.
pub struct Request {
    // CACHE LINE 1 (64 bytes): HOT PATH - Most frequently accessed fields
    /// Cached HTTP method - accessed on every request operation
    method: String,
    /// Cached HTTP path - accessed on every request operation  
    path: String,
    /// Small field, frequently accessed - placed in hot cache line
    cached_content_length: Option<usize>,

    // CACHE LINE 2: WARM PATH - Moderately accessed fields
    /// Header fields retained in wire order for case-insensitive lookup.
    header_cache: Vec<RawHeader>,

    /// Immutable, fail-closed cookie parse result shared by all request lookups.
    cookie_cache: OnceLock<Result<Arc<RequestCookieJar>, RequestCookieError>>,

    // CACHE LINE 3: COLD PATH - Less frequently accessed fields
    /// Query parameters extracted from URL - accessed only when needed
    queries: QueryParams,
    /// Path parameters extracted from route matching - accessed only when needed
    params: std::collections::HashMap<InternedString, InternedString>,

    /// Identity established by a successful authentication guard.
    principal: Option<Principal>,

    /// Socket/trusted-proxy identity established by the transport adapter.
    connection_info: RequestConnectionInfo,

    /// Immutable localization snapshot owned by the handling application.
    localization_catalog: Option<Arc<LocalizationCatalog>>,

    /// Typed values owned by this request's middleware/handler lifecycle.
    local: RequestExtensions,

    /// Framework-owned execution observation; request-local mutation cannot
    /// clear or replace this authority.
    execution_cancellation: ExecutionCancellation,

    // CACHE LINE 4: BODY DATA - Request body for POST/PUT operations
    /// HTTP request body stored in the configured transport buffer backend.
    body: OnceLock<HttpBuffer>,

    /// Immutable application body limit copied from the HTTP transport before
    /// dispatch. Body consumers never read mutable server configuration.
    body_budget: BodyBudget,

    /// Immutable multipart policy copied from the effective HTTP transport
    /// before dispatch.
    multipart_limits: RequestMultipartLimits,

    /// Pull-based transport body owned by this request without a detached
    /// reader task. Cancelling a handler which only borrows this request does
    /// not itself release the reader or prove transport termination.
    body_stream: AsyncMutex<Option<Box<dyn RequestBodyStream>>>,

    /// Safe lifecycle state for enforcing a single body-consumption mode.
    body_state: AtomicU8,

    /// Bytes yielded to an incremental consumer. This is accounting only; the
    /// chunks themselves are not retained.
    streamed_body_bytes: usize,

    /// Allocated only when a `BodyStream` extractor takes terminal ownership.
    /// Normal buffered and unused-body routes keep the HTTP hot path allocation-free.
    body_stream_tracker: Option<Arc<RequestBodyTracker>>,

    /// Lets a buffered request participate in the incremental API without
    /// consuming or mutating its compatibility buffer.
    buffered_chunk_yielded: bool,
}

impl fmt::Debug for Request {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Request")
            .field("method", &self.method)
            .field("request_target", &"<redacted>")
            .field("request_target_bytes", &self.path.len())
            .field("header_count", &self.header_cache.len())
            .field(
                "cookie_cache_initialized",
                &self.cookie_cache.get().is_some(),
            )
            .field("query_pair_count", &self.queries.len())
            .field("path_parameter_count", &self.params.len())
            .field(
                "body_bytes",
                &self.body.get().map(HttpBuffer::len).unwrap_or_default(),
            )
            .field("body_state", &self.body_state())
            .field("max_request_body_bytes", &self.body_budget.limit_bytes())
            .field(
                "max_multipart_part_bytes",
                &self.multipart_limits.max_part_bytes,
            )
            .field("max_multipart_parts", &self.multipart_limits.max_parts)
            .field("streamed_body_bytes", &self.streamed_body_bytes())
            .field("authenticated", &self.principal.is_some())
            .field(
                "peer_ip_available",
                &self.connection_info.peer_ip().is_some(),
            )
            .field(
                "client_ip_available",
                &self.connection_info.client_ip().is_some(),
            )
            .field(
                "via_trusted_proxy",
                &self.connection_info.via_trusted_proxy(),
            )
            .field("localization_enabled", &self.localization_catalog.is_some())
            .field("request_local_entry_count", &self.local.len())
            .finish()
    }
}

impl Request {
    /// Read-only cancellation of this request's normal execution, including
    /// middleware response post-processing. Cleanup has separate authority.
    pub fn execution_cancellation(&self) -> ExecutionCancellation {
        self.execution_cancellation.clone()
    }

    pub(crate) fn set_execution_cancellation(&mut self, view: ExecutionCancellation) {
        self.execution_cancellation = view;
    }

    /// Builds a Lily request from a standards-compliant transport adapter.
    ///
    /// Hyper owns HTTP/1.1 and HTTP/2 framing; this compatibility constructor
    /// transfers normalized metadata and an already buffered body into Lily's
    /// router-facing type.
    #[doc(hidden)]
    pub async fn from_transport_parts(
        method: String,
        path: String,
        headers: Vec<RawHeader>,
        body: &[u8],
    ) -> Result<Self, HttpApiError> {
        let body_budget = BodyBudget::default();
        body_budget.ensure_length(body.len()).map_err(|_| {
            HttpApiError::PayloadTooLarge(format!(
                "request body exceeds {} bytes",
                body_budget.limit_bytes()
            ))
        })?;
        let queries = parse_transport_query(&path)?;
        let cached_content_length = Some(body.len());
        let buffered_body = if body.is_empty() {
            None
        } else {
            Some(HttpBuffer::with_data(body).await?)
        };
        let body = OnceLock::new();
        if let Some(buffered_body) = buffered_body {
            body.set(buffered_body)
                .expect("a new request body cell is empty");
        }
        let request = Self {
            method,
            path,
            cached_content_length,
            header_cache: headers,
            cookie_cache: OnceLock::new(),
            queries,
            params: HashMap::new(),
            principal: None,
            connection_info: RequestConnectionInfo::default(),
            localization_catalog: None,
            local: RequestExtensions::new(),
            execution_cancellation: inactive_execution_cancellation(),
            body,
            body_budget,
            multipart_limits: RequestMultipartLimits::for_budget(body_budget),
            body_stream: AsyncMutex::new(None),
            body_state: AtomicU8::new(
                if cached_content_length == Some(0) {
                    RequestBodyState::Empty
                } else {
                    RequestBodyState::Buffered
                }
                .as_u8(),
            ),
            streamed_body_bytes: 0,
            body_stream_tracker: None,
            buffered_chunk_yielded: false,
        };
        Ok(request)
    }

    /// Builds a request whose body remains owned by the HTTP transport until
    /// middleware or a handler pulls it. This is the canonical HTTP/1.1 and
    /// HTTP/2 constructor; it is hidden because protocol crates provide the
    /// bounded stream implementation.
    #[doc(hidden)]
    pub fn from_streaming_transport_parts(
        method: String,
        path: String,
        headers: Vec<RawHeader>,
        body_stream: Option<Box<dyn RequestBodyStream>>,
        max_request_body_bytes: usize,
    ) -> Result<Self, HttpApiError> {
        let body_budget = BodyBudget::new(max_request_body_bytes)
            .map_err(|error| HttpApiError::ConfigurationError(error.to_string()))?;
        let queries = parse_transport_query(&path)?;
        let cached_content_length = body_stream.as_ref().and_then(|body| {
            let (lower, upper) = body.size_hint();
            (upper == Some(lower))
                .then(|| usize::try_from(lower).ok())
                .flatten()
        });
        let body_state = if body_stream.is_some() {
            RequestBodyState::Pending
        } else {
            RequestBodyState::Empty
        };

        Ok(Self {
            method,
            path,
            cached_content_length,
            header_cache: headers,
            cookie_cache: OnceLock::new(),
            queries,
            params: HashMap::new(),
            principal: None,
            connection_info: RequestConnectionInfo::default(),
            localization_catalog: None,
            local: RequestExtensions::new(),
            execution_cancellation: inactive_execution_cancellation(),
            body: OnceLock::new(),
            body_budget,
            multipart_limits: RequestMultipartLimits::for_budget(body_budget),
            body_stream: AsyncMutex::new(body_stream),
            body_state: AtomicU8::new(body_state.as_u8()),
            streamed_body_bytes: 0,
            body_stream_tracker: None,
            buffered_chunk_yielded: false,
        })
    }

    /// Returns the exact parsed HTTP method.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Returns the parsed request target path, including any query suffix.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the case-sensitive route parameters selected by routing.
    pub fn params(&self) -> &std::collections::HashMap<InternedString, InternedString> {
        &self.params
    }

    /// Returns the verified identity for this request, if authentication has
    /// succeeded.
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// Returns the socket peer address established by the transport.
    pub fn peer_ip(&self) -> Option<std::net::IpAddr> {
        self.connection_info.peer_ip()
    }

    /// Returns the effective client address after trusted-proxy validation.
    pub fn client_ip(&self) -> Option<std::net::IpAddr> {
        self.connection_info.client_ip()
    }

    /// Reports whether the socket peer matched the configured trusted-proxy
    /// network set.
    pub fn via_trusted_proxy(&self) -> bool {
        self.connection_info.via_trusted_proxy()
    }

    /// Attaches network identity produced by the transport adapter.
    #[doc(hidden)]
    pub fn set_connection_info(&mut self, connection_info: RequestConnectionInfo) {
        self.connection_info = connection_info;
    }

    /// Attaches or clears the immutable catalog selected by the application.
    ///
    /// The HTTP composition root overwrites this value for every dispatch so
    /// a reused request cannot retain another application's localization
    /// context.
    #[doc(hidden)]
    pub fn set_localization_catalog(&mut self, catalog: Option<Arc<LocalizationCatalog>>) {
        self.localization_catalog = catalog;
    }

    pub(crate) fn localization_catalog(&self) -> Option<&LocalizationCatalog> {
        self.localization_catalog.as_deref()
    }

    /// Returns the typed values scoped to this request.
    #[must_use]
    pub fn local(&self) -> &RequestExtensions {
        &self.local
    }

    /// Returns mutable access to the typed values scoped to this request.
    pub fn local_mut(&mut self) -> &mut RequestExtensions {
        &mut self.local
    }

    /// Clears all middleware/handler state before reusing a request for a new
    /// dispatch.
    #[doc(hidden)]
    pub fn clear_local(&mut self) {
        self.local.clear();
    }

    /// Clears request identity before an authentication attempt.
    #[doc(hidden)]
    pub fn clear_principal(&mut self) {
        self.principal = None;
    }

    /// Attaches the identity established by application authentication.
    ///
    /// Call this only after a guard or middleware has validated the request's
    /// credentials. Lily does not validate tokens or session state when this
    /// value is assigned; the application verifier remains the authority.
    pub fn set_principal(&mut self, principal: Principal) {
        self.principal = Some(principal);
    }

    /// Returns the decoded, ordered query multimap.
    pub fn queries(&self) -> &QueryParams {
        &self.queries
    }

    /// Returns the first decoded value for `name` in wire order.
    pub fn query_first(&self, name: &str) -> Option<&InternedString> {
        self.queries.get(name)
    }

    /// Returns all decoded values for `name` in wire order.
    pub fn query_values(&self, name: &str) -> QueryValues<'_> {
        self.queries.get_all(name)
    }

    /// Returns parsed header field lines in wire order.
    ///
    /// This is a framework integration seam. Application code should prefer
    /// [`crate::RequestExt`] or typed action extractors.
    #[doc(hidden)]
    pub fn header_cache(&self) -> &Vec<RawHeader> {
        &self.header_cache
    }

    pub(crate) fn parsed_cookie_jar(&self) -> Result<&RequestCookieJar, RequestCookieError> {
        self.cached_cookie_jar()
            .as_ref()
            .map(Arc::as_ref)
            .map_err(|error| *error)
    }

    /// Returns shared ownership of the immutable parsed request cookie jar.
    ///
    /// This is a framework integration seam for owned action extractors. The
    /// parse result remains lazy and authoritative for every cookie API.
    #[doc(hidden)]
    pub fn shared_cookie_jar(&self) -> Result<Arc<RequestCookieJar>, RequestCookieError> {
        self.cached_cookie_jar()
            .as_ref()
            .map(Arc::clone)
            .map_err(|error| *error)
    }

    fn cached_cookie_jar(&self) -> &Result<Arc<RequestCookieJar>, RequestCookieError> {
        self.cookie_cache.get_or_init(|| {
            RequestCookieJar::parse(
                self.header_cache
                    .iter()
                    .filter(|header| header.name.eq_ignore_ascii_case("cookie"))
                    .map(|header| header.value.as_str()),
            )
            .map(Arc::new)
        })
    }

    /// Access to the request body without copying.
    /// Returns None if no body is present (e.g., GET requests)
    pub fn body(&self) -> Option<&HttpBuffer> {
        self.body.get()
    }

    /// Access to request body as raw bytes slice
    /// Returns None if no body is present (e.g., GET requests)
    pub fn body_bytes(&self) -> Option<&[u8]> {
        self.body.get().map(HttpBuffer::as_slice)
    }

    pub(crate) fn max_request_body_bytes(&self) -> usize {
        self.body_budget.limit_bytes()
    }

    pub(crate) fn multipart_limits(&self) -> (usize, usize, usize) {
        (
            self.multipart_limits.max_part_bytes,
            self.multipart_limits.max_parts,
            self.multipart_limits.max_retained_metadata_bytes,
        )
    }

    /// Replaces default codec limits with the validated effective server
    /// snapshot. Transport adapters call this before middleware dispatch.
    #[doc(hidden)]
    pub fn set_multipart_limits(
        &mut self,
        max_part_bytes: usize,
        max_parts: usize,
        max_retained_metadata_bytes: usize,
    ) -> Result<(), HttpApiError> {
        if max_part_bytes == 0 || max_part_bytes > self.body_budget.limit_bytes() {
            return Err(HttpApiError::ConfigurationError(
                "max_multipart_part_bytes must be in 1..=max_request_body_bytes".to_string(),
            ));
        }
        if max_parts == 0 || max_parts > 4096 {
            return Err(HttpApiError::ConfigurationError(
                "max_multipart_parts must be in 1..=4096".to_string(),
            ));
        }
        if max_retained_metadata_bytes == 0 || max_retained_metadata_bytes > 1024 * 1024 {
            return Err(HttpApiError::ConfigurationError(
                "max_multipart_metadata_bytes must be in 1..=1MiB".to_string(),
            ));
        }
        self.multipart_limits = RequestMultipartLimits {
            max_part_bytes,
            max_parts,
            max_retained_metadata_bytes,
        };
        Ok(())
    }

    /// Reports the current request-body consumption state without exposing
    /// body data or transport details.
    pub fn body_state(&self) -> RequestBodyState {
        self.body_stream_tracker.as_ref().map_or_else(
            || RequestBodyState::from_u8(self.body_state.load(Ordering::Acquire)),
            |tracker| tracker.state(),
        )
    }

    /// Returns true when a transport body is available for incremental reads.
    pub fn has_streaming_body(&self) -> bool {
        matches!(
            self.body_state(),
            RequestBodyState::Pending | RequestBodyState::Streaming
        )
    }

    /// Number of bytes delivered through [`Self::next_body_chunk`]. Chunks are
    /// not retained, so this remains independent of [`Self::body_bytes`].
    pub fn streamed_body_bytes(&self) -> usize {
        self.body_stream_tracker
            .as_ref()
            .map_or(self.streamed_body_bytes, |tracker| tracker.streamed_bytes())
    }

    /// Exact body length when the transport supplied one (for example through
    /// a valid `Content-Length`). Chunked and open-ended HTTP/2 bodies return
    /// `None` until [`Self::buffer_body`] completes.
    pub fn body_size_hint(&self) -> Option<usize> {
        self.body
            .get()
            .map(HttpBuffer::len)
            .or(self.cached_content_length)
    }

    /// Pulls at most one data chunk from the transport.
    ///
    /// Calling this method is the explicit opt-in to incremental consumption.
    /// Hyper will not release more HTTP/2 flow-control capacity until the
    /// returned [`Bytes`] value is consumed or dropped. A request may either
    /// be streamed or buffered, but cannot switch to buffering after its first
    /// transport chunk has been yielded.
    pub async fn next_body_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
        if self.body_stream_tracker.is_some() || self.body_state() == RequestBodyState::Buffering {
            return Err(RequestBodyError::AlreadyStreaming);
        }
        if let Some(body) = self.body.get() {
            if self.buffered_chunk_yielded || body.is_empty() {
                self.body_state
                    .store(RequestBodyState::Complete.as_u8(), Ordering::Release);
                return Ok(None);
            }
            self.buffered_chunk_yielded = true;
            self.streamed_body_bytes = body.len();
            self.body_state
                .store(RequestBodyState::Streaming.as_u8(), Ordering::Release);
            return Ok(Some(Bytes::copy_from_slice(body.as_slice())));
        }

        let stream_slot = self.body_stream.get_mut();
        let Some(stream) = stream_slot.as_mut() else {
            if self.body_state() != RequestBodyState::Failed {
                self.body_state
                    .store(RequestBodyState::Complete.as_u8(), Ordering::Release);
            }
            return Ok(None);
        };

        match stream.next_chunk().await {
            Ok(Some(chunk)) => {
                let streamed_body_bytes = self.streamed_body_bytes();
                let next_streamed_body_bytes = self
                    .body_budget
                    .checked_next_length(streamed_body_bytes, chunk.len())
                    .map_err(|_| RequestBodyError::PayloadTooLarge {
                        limit_bytes: self.body_budget.limit_bytes(),
                    })?;
                self.streamed_body_bytes = next_streamed_body_bytes;
                self.body_state
                    .store(RequestBodyState::Streaming.as_u8(), Ordering::Release);
                Ok(Some(chunk))
            }
            Ok(None) => {
                *stream_slot = None;
                self.body_state
                    .store(RequestBodyState::Complete.as_u8(), Ordering::Release);
                Ok(None)
            }
            Err(error) => {
                *stream_slot = None;
                self.body_state
                    .store(RequestBodyState::Failed.as_u8(), Ordering::Release);
                Err(error)
            }
        }
    }

    /// Drains an unread transport body into the configured compatibility
    /// buffer and returns it without copying.
    ///
    /// This keeps JSON, form, multipart, and existing `body()` consumers on a
    /// bounded path while allowing streaming handlers to avoid whole-body
    /// buffering. It fails closed if incremental consumption has already
    /// started, because silently returning only the remainder would be unsafe.
    pub async fn buffer_body(&self) -> Result<Option<&HttpBuffer>, RequestBodyError> {
        self.buffer_body_with_limit(self.body_budget).await
    }

    /// Transfers terminal incremental ownership to a generated action
    /// extractor without spawning a detached body-reader task.
    ///
    /// Once selected, whole-body buffering and raw request access to the body
    /// remain locked out. The returned reader shares only lifecycle counters
    /// with this request; it owns the transport body itself.
    #[doc(hidden)]
    pub fn take_body_reader(&mut self) -> Result<RequestBodyReader, RequestBodyError> {
        let state = self.body_state();
        if matches!(
            state,
            RequestBodyState::Streaming | RequestBodyState::Buffering | RequestBodyState::Failed
        ) || (state == RequestBodyState::Complete
            && (self.streamed_body_bytes() != 0 || self.buffered_chunk_yielded))
        {
            return Err(RequestBodyError::AlreadyStreaming);
        }

        let buffered = self.body.take().map(HttpBuffer::into_bytes);
        let stream = self.body_stream.get_mut().take();
        self.buffered_chunk_yielded = false;
        self.streamed_body_bytes = 0;
        let tracker = Arc::new(RequestBodyTracker::streaming());
        self.body_stream_tracker = Some(Arc::clone(&tracker));

        Ok(RequestBodyReader::new(
            buffered,
            stream,
            self.body_budget,
            tracker,
        ))
    }

    /// Buffers the transport body under a narrower integration-specific cap.
    ///
    /// Framework middleware uses this seam when it must inspect a small
    /// credential-bearing form before route dispatch. The transport-wide
    /// budget remains authoritative and callers cannot enlarge it.
    #[doc(hidden)]
    pub async fn buffer_body_bounded(
        &self,
        max_bytes: usize,
    ) -> Result<Option<&HttpBuffer>, RequestBodyError> {
        let requested =
            BodyBudget::new(max_bytes).map_err(|_| RequestBodyError::PayloadTooLarge {
                limit_bytes: self.body_budget.limit_bytes(),
            })?;
        let effective =
            BodyBudget::new(requested.limit_bytes().min(self.body_budget.limit_bytes()))
                .expect("the minimum of two valid body budgets is valid");
        self.buffer_body_with_limit(effective).await
    }

    /// Buffers a transport body under a consumer-specific limit. The
    /// transport's own bound remains authoritative; this narrower bound lets
    /// parsers stop chunked requests before allocating the general body cap.
    pub(crate) async fn buffer_body_with_limit(
        &self,
        budget: BodyBudget,
    ) -> Result<Option<&HttpBuffer>, RequestBodyError> {
        let limit_bytes = budget.limit_bytes();
        if let Some(body) = self.body.get() {
            if body.len() > limit_bytes {
                return Err(RequestBodyError::PayloadTooLarge { limit_bytes });
            }
            return Ok(Some(body));
        }
        if matches!(
            self.body_state(),
            RequestBodyState::Streaming | RequestBodyState::Buffering
        ) {
            return Err(RequestBodyError::AlreadyStreaming);
        }

        let mut stream_slot = self.body_stream.lock().await;
        if let Some(body) = self.body.get() {
            if body.len() > limit_bytes {
                return Err(RequestBodyError::PayloadTooLarge { limit_bytes });
            }
            return Ok(Some(body));
        }
        if matches!(
            self.body_state(),
            RequestBodyState::Streaming | RequestBodyState::Buffering
        ) {
            return Err(RequestBodyError::AlreadyStreaming);
        }
        let Some(stream) = stream_slot.as_mut() else {
            return Ok(None);
        };
        // Buffering consumes transport bytes just like the incremental API.
        // Mark it non-replayable before the first await so cancellation can
        // never make a partially consumed stream look fresh to a retry.
        self.body_state
            .store(RequestBodyState::Buffering.as_u8(), Ordering::Release);
        let (lower, upper) = stream.size_hint();
        if lower > limit_bytes as u64 {
            *stream_slot = None;
            self.body_state
                .store(RequestBodyState::Failed.as_u8(), Ordering::Release);
            return Err(RequestBodyError::PayloadTooLarge { limit_bytes });
        }
        let capacity = upper
            .unwrap_or(lower)
            .min(limit_bytes as u64)
            .min(64 * 1024) as usize;
        let mut buffer = match HttpBuffer::new(capacity).await {
            Ok(buffer) => buffer,
            Err(_) => {
                *stream_slot = None;
                self.body_state
                    .store(RequestBodyState::Failed.as_u8(), Ordering::Release);
                return Err(RequestBodyError::BufferUnavailable);
            }
        };

        loop {
            let next = stream.next_chunk().await;
            match next {
                Ok(Some(chunk)) => {
                    if budget
                        .checked_next_length(buffer.len(), chunk.len())
                        .is_err()
                    {
                        *stream_slot = None;
                        self.body_state
                            .store(RequestBodyState::Failed.as_u8(), Ordering::Release);
                        return Err(RequestBodyError::PayloadTooLarge { limit_bytes });
                    }
                    buffer.extend_from_slice(&chunk).await;
                }
                Ok(None) => {
                    *stream_slot = None;
                    let state = if buffer.is_empty() {
                        RequestBodyState::Empty
                    } else {
                        RequestBodyState::Buffered
                    };
                    if !buffer.is_empty() {
                        self.body
                            .set(buffer)
                            .expect("body buffering has exclusive stream ownership");
                    }
                    self.body_state.store(state.as_u8(), Ordering::Release);
                    return Ok(self.body.get());
                }
                Err(error) => {
                    *stream_slot = None;
                    self.body_state
                        .store(RequestBodyState::Failed.as_u8(), Ordering::Release);
                    return Err(error);
                }
            }
        }
    }

    /// Returns all parsed header field lines in wire order.
    ///
    /// Repeated fields remain distinct. Header names should be compared using
    /// ASCII case-insensitive semantics.
    pub fn headers(&self) -> &Vec<RawHeader> {
        &self.header_cache
    }

    /// Returns a borrowed header value using an ASCII case-insensitive name
    /// comparison. No allocation or string interning is performed.
    #[inline]
    pub fn header_value(&self, name: &str) -> Option<&str> {
        self.header_cache
            .iter()
            .find(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.as_str())
    }

    /// Replaces the path parameters selected by the router.
    #[doc(hidden)]
    pub fn set_params(&mut self, params: Vec<(String, String)>) {
        self.params.clear();
        for (key, value) in params {
            self.params
                .insert(preserve_opaque_owned(key), preserve_opaque_owned(value));
        }
    }

    /// Test-only constructor for creating mock Request objects
    #[cfg(test)]
    pub fn new_test(method: &str, path: &str) -> Self {
        Self {
            method: method.to_string(),
            path: path.to_string(),
            cached_content_length: None,
            header_cache: Vec::new(),
            cookie_cache: OnceLock::new(),
            queries: QueryParams::default(),
            params: std::collections::HashMap::new(),
            principal: None,
            connection_info: RequestConnectionInfo::default(),
            localization_catalog: None,
            local: RequestExtensions::new(),
            execution_cancellation: inactive_execution_cancellation(),
            body: OnceLock::new(), // No body for test requests by default
            body_budget: BodyBudget::default(),
            multipart_limits: RequestMultipartLimits::for_budget(BodyBudget::default()),
            body_stream: AsyncMutex::new(None),
            body_state: AtomicU8::new(RequestBodyState::Empty.as_u8()),
            streamed_body_bytes: 0,
            body_stream_tracker: None,
            buffered_chunk_yielded: false,
        }
    }

    /// Adds a header to a test request.
    #[cfg(test)]
    pub fn add_test_header(&mut self, name: &str, value: &str) {
        self.cookie_cache = OnceLock::new();
        self.header_cache.push(RawHeader {
            name: name.to_string(),
            value: value.to_string(),
            line_number: 0,
            raw_line: format!("{name}: {value}"),
        });
    }
}

fn parse_query(path: &str) -> Result<QueryParams, QueryParseError> {
    let encoded = path
        .split_once('?')
        .map(|(_, encoded)| encoded)
        .unwrap_or_default();
    QueryParams::parse(encoded)
}

fn parse_transport_query(path: &str) -> Result<QueryParams, HttpApiError> {
    parse_query(path)
        .map_err(|error| HttpApiError::InvalidQueryString(format!("invalid URI query: {error}")))
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<HTTP Request {} {}>", self.method(), self.path())
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_transport_query, Request};
    use crate::request::{Principal, RequestBodyState};
    use crate::RequestCookieError;
    use lily_error::application::http_api::HttpApiError;
    use serde_json::{json, Map};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[derive(Debug, PartialEq, Eq)]
    struct RequestMarker(usize);

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn owned_body_stream_tracking_is_allocated_only_when_selected() {
        let mut request = Request::from_transport_parts(
            "POST".to_string(),
            "/body".to_string(),
            Vec::new(),
            b"buffered",
        )
        .await
        .unwrap();

        assert!(request.body_stream_tracker.is_none());
        assert_eq!(request.buffer_body().await.unwrap().unwrap().len(), 8);
        assert!(request.body_stream_tracker.is_none());

        let mut reader = request.take_body_reader().unwrap();
        assert!(request.body_stream_tracker.is_some());
        assert_eq!(request.body_state(), RequestBodyState::Streaming);
        assert_eq!(reader.next_chunk().await.unwrap().unwrap(), b"buffered"[..]);
        assert!(reader.next_chunk().await.unwrap().is_none());
        assert_eq!(request.body_state(), RequestBodyState::Complete);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_requests_never_share_request_local_state() {
        const REQUEST_COUNT: usize = 32;
        let barrier = Arc::new(tokio::sync::Barrier::new(REQUEST_COUNT));
        let mut tasks = Vec::with_capacity(REQUEST_COUNT);

        for index in 0..REQUEST_COUNT {
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new_test("GET", "/local");
                request.local_mut().insert(RequestMarker(index));
                barrier.wait().await;
                tokio::task::yield_now().await;

                assert_eq!(
                    request.local().get::<RequestMarker>(),
                    Some(&RequestMarker(index))
                );
                assert_eq!(request.local().len(), 1);
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }
    }

    #[test]
    fn clearing_for_request_reuse_drops_and_resets_local_state() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut request = Request::new_test("GET", "/first-dispatch");
        request.local_mut().insert(DropProbe(Arc::clone(&drops)));
        request.local_mut().insert(RequestMarker(1));

        request.clear_local();

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(request.local().is_empty());
        request.local_mut().insert(RequestMarker(2));
        assert_eq!(
            request.local().get::<RequestMarker>(),
            Some(&RequestMarker(2))
        );
    }

    #[test]
    fn transport_query_failures_are_typed_bad_requests() {
        let error =
            parse_transport_query("/items?access_token=%GG").expect_err("invalid transport query");

        assert!(matches!(error, HttpApiError::InvalidQueryString(_)));
        assert_eq!(error.http_status().0, 400);
        assert_eq!(error.error_code(), "INVALID_QUERY_STRING");
        assert!(!error.to_string().contains("access_token"));
    }

    #[test]
    fn request_debug_redacts_headers_target_body_and_principal_values() {
        let mut request = Request::new_test("GET", "/private?token=query-secret");
        request.add_test_header("Authorization", "Bearer header-secret");
        let mut claims = Map::new();
        claims.insert("tenant".to_string(), json!("claim-secret"));
        request.set_principal(Principal::new(
            "subject-secret",
            ["role-secret".to_string()],
            ["scope-secret".to_string()],
            claims,
        ));
        request.set_localization_catalog(Some(std::sync::Arc::new(
            lily_error::LocalizationCatalog::from_translations(std::collections::HashMap::from([
                (
                    "en".to_string(),
                    std::collections::HashMap::from([(
                        "catalog-key-secret".to_string(),
                        "catalog-value-secret".to_string(),
                    )]),
                ),
            ]))
            .unwrap(),
        )));

        let output = format!("{request:?}");
        for sensitive in [
            "query-secret",
            "header-secret",
            "claim-secret",
            "subject-secret",
            "role-secret",
            "scope-secret",
            "catalog-key-secret",
            "catalog-value-secret",
        ] {
            assert!(!output.contains(sensitive));
        }
        assert!(output.contains("authenticated: true"));
        assert!(output.contains("header_count: 1"));
        assert!(output.contains("localization_enabled: true"));
    }

    #[test]
    fn malformed_cookie_parse_error_is_cached_without_retaining_error_payload() {
        let mut request = Request::new_test("GET", "/");
        request.add_test_header("Cookie", "session=first-secret; broken");
        assert!(request.cookie_cache.get().is_none());

        assert_eq!(
            request.parsed_cookie_jar(),
            Err(RequestCookieError::Malformed)
        );
        assert!(request.cookie_cache.get().is_some());
        assert_eq!(
            request.parsed_cookie_jar(),
            Err(RequestCookieError::Malformed)
        );

        let output = format!("{:?}", request.cookie_cache.get().unwrap());
        assert!(!output.contains("session"));
        assert!(!output.contains("first-secret"));
    }
}
