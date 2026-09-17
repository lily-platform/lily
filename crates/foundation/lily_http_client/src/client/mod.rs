//! Production HTTP/1.1 and HTTP/2 client transport.
//!
//! Hyper owns message framing, pooling, HTTP/2 multiplexing and flow control.
//! Hyper-Rustls owns certificate/hostname verification and ALPN for HTTPS.

use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock, Weak},
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime},
};

use bytes::{Bytes, BytesMut};
use http::{HeaderName, HeaderValue, Method as HttpMethod, Uri, Version};
use http_body_util::{BodyExt, Full};
use hyper::body::Body as HyperBody;
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client as HyperClient},
    rt::{TokioExecutor, TokioTimer},
};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, UpDownCounter},
    propagation::Injector,
    KeyValue,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower_service::Service;
use tracing::Instrument;
use url::Url;

use crate::{
    body::{BinaryBody, Body},
    error::{HttpClientError, Result},
    header::HeaderMap,
    request::{
        is_hop_by_hop_or_proxy_only_request_header, parse_request_url, same_origin,
        validate_request_url, Method, Request, RequestBuilder, RequestConfig,
    },
    response::{HttpVersion, Response, ResponseBuilder, ResponseMetadata, StatusCode},
};

type RequestBody = Full<Bytes>;
type BaseConnector = HttpsConnector<HttpConnector>;
type Connector = ConnectDeadlineConnector<BaseConnector>;
type TransportClient = HyperClient<Connector, RequestBody>;

pub(crate) const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
const MAX_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(3600);
const MAX_HTTP2_KEEP_ALIVE: Duration = Duration::from_secs(600);
const MIN_HTTP2_WINDOW_BYTES: u32 = 16 * 1024;
const MAX_HTTP2_WINDOW_BYTES: u32 = 64 * 1024 * 1024;
pub(crate) const MAX_ADDITIONAL_CA_BUNDLE_BYTES: usize = 1024 * 1024;
const MAX_ADDITIONAL_CA_CERTIFICATES: usize = 32;
const MAX_RETAINED_ORIGINS: usize = 1024;
const DEFAULT_MAX_HEADER_COUNT: usize = 128;
const MAX_HEADER_BYTES: usize = 1024 * 1024;
const MAX_HEADER_COUNT: usize = 1024;
const HYPER_HTTP1_DEFAULT_MAX_HEADERS: usize = 100;
const HTTP1_MIN_BUFFER_BYTES: usize = 8192;
const HTTP1_HEADER_LINE_FRAMING_BYTES: usize = 4;
const HTTP1_FINAL_HEADER_TERMINATOR_BYTES: usize = 2;
const HTTP1_STATUS_LINE_ALLOWANCE_BYTES: usize = 8 * 1024;
const HTTP2_HEADER_FIELD_OVERHEAD_BYTES: usize = 32;
const HTTP2_STATUS_NAME_BYTES: usize = 7;
const HTTP2_STATUS_VALUE_BYTES: usize = 3;
const HYPER_HAPPY_EYEBALLS_DEFAULT_DELAY: Duration = Duration::from_millis(300);
// h2 0.4 treats a decoded list whose size is equal to the advertised maximum
// as oversized, so the upstream parser needs one byte beyond the inclusive
// Lily application limit.
const HTTP2_STRICT_BOUND_ALLOWANCE_BYTES: usize = 1;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnectDeadlineError {
    DeadlineElapsed,
    ConnectionFailed,
}

impl fmt::Debug for ConnectDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectDeadlineError")
            .field(
                "code",
                &match self {
                    Self::DeadlineElapsed => "CONNECT_DEADLINE",
                    Self::ConnectionFailed => "CONNECT_FAILED",
                },
            )
            .finish()
    }
}

impl fmt::Display for ConnectDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineElapsed => formatter.write_str("connection deadline exceeded"),
            Self::ConnectionFailed => formatter.write_str("connection establishment failed"),
        }
    }
}

impl StdError for ConnectDeadlineError {}

fn classify_inner_connector_error<E>(error: E) -> ConnectDeadlineError
where
    E: Into<Box<dyn StdError + Send + Sync>>,
{
    let error: Box<dyn StdError + Send + Sync> = error.into();
    if connection_timeout_in_chain(error.as_ref()) {
        ConnectDeadlineError::DeadlineElapsed
    } else {
        ConnectDeadlineError::ConnectionFailed
    }
}

#[derive(Clone)]
struct ConnectDeadlineConnector<C> {
    inner: C,
    deadline: Duration,
}

impl<C> ConnectDeadlineConnector<C> {
    fn new(inner: C, deadline: Duration) -> Self {
        Self { inner, deadline }
    }
}

impl<C> Service<Uri> for ConnectDeadlineConnector<C>
where
    C: Service<Uri> + Send + 'static,
    C::Future: Send + 'static,
    C::Response: Send + 'static,
    C::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    type Response = C::Response;
    type Error = ConnectDeadlineError;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner
            .poll_ready(context)
            .map_err(classify_inner_connector_error)
    }

    fn call(&mut self, destination: Uri) -> Self::Future {
        let connect = self.inner.call(destination);
        let deadline = self.deadline;
        Box::pin(async move {
            match tokio::time::timeout(deadline, connect).await {
                Ok(Ok(connection)) => Ok(connection),
                Ok(Err(error)) => Err(classify_inner_connector_error(error)),
                Err(_) => Err(ConnectDeadlineError::DeadlineElapsed),
            }
        })
    }
}

fn connection_timeout_in_chain(error: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if matches!(
            error.downcast_ref::<ConnectDeadlineError>(),
            Some(ConnectDeadlineError::DeadlineElapsed)
        ) || error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
        {
            return true;
        }
        current = error.source();
    }
    false
}

fn map_transport_error(error: &(dyn StdError + 'static)) -> HttpClientError {
    if connection_timeout_in_chain(error) {
        HttpClientError::ConnectTimeout
    } else {
        HttpClientError::Connection("transport request failed".to_string())
    }
}

fn http_method_metric_label(method: &str) -> &'static str {
    match method {
        "CONNECT" => "CONNECT",
        "DELETE" => "DELETE",
        "GET" => "GET",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        "PATCH" => "PATCH",
        "POST" => "POST",
        "PUT" => "PUT",
        "TRACE" => "TRACE",
        _ => "_OTHER",
    }
}

#[derive(Debug, Clone)]
struct HttpClientTelemetry {
    requests: Counter<u64>,
    in_flight: UpDownCounter<i64>,
    request_duration: Histogram<f64>,
    dependency_duration: Histogram<f64>,
    admission_wait: Histogram<f64>,
    outcomes: Counter<u64>,
    timeouts: Counter<u64>,
    cancellations: Counter<u64>,
    http2_active_streams: UpDownCounter<i64>,
}

impl HttpClientTelemetry {
    fn new() -> Arc<Self> {
        let meter = global::meter("lily_http_client");
        Arc::new(Self {
            requests: meter.u64_counter("http.client.requests").build(),
            in_flight: meter
                .i64_up_down_counter("http.client.requests.in_flight")
                .build(),
            request_duration: meter
                .f64_histogram("http.client.request.duration")
                .with_unit("s")
                .build(),
            dependency_duration: meter
                .f64_histogram("http.client.dependency.duration")
                .with_unit("s")
                .build(),
            admission_wait: meter
                .f64_histogram("http.client.admission.wait.duration")
                .with_unit("s")
                .build(),
            outcomes: meter.u64_counter("http.client.outcomes").build(),
            timeouts: meter.u64_counter("http.client.timeouts").build(),
            cancellations: meter.u64_counter("http.client.cancellations").build(),
            http2_active_streams: meter
                .i64_up_down_counter("http.client.http2.streams.active")
                .build(),
        })
    }
}

struct HttpClientRequestGuard {
    telemetry: Arc<HttpClientTelemetry>,
    span: tracing::Span,
    started: Instant,
    method: &'static str,
    terminal: bool,
}

impl HttpClientRequestGuard {
    fn start(telemetry: Arc<HttpClientTelemetry>, span: tracing::Span, method: String) -> Self {
        let method = http_method_metric_label(&method);
        let attributes = [KeyValue::new("http.request.method", method)];
        telemetry.requests.add(1, &attributes);
        telemetry.in_flight.add(1, &attributes);
        Self {
            telemetry,
            span,
            started: Instant::now(),
            method,
            terminal: false,
        }
    }

    fn finish(&mut self, outcome: &'static str, error_code: Option<&'static str>) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        let error_code = error_code.unwrap_or("none");
        let in_flight_attributes = [KeyValue::new("http.request.method", self.method)];
        let terminal_attributes = [
            KeyValue::new("http.request.method", self.method),
            KeyValue::new("lily.outcome", outcome),
            KeyValue::new("lily.error_code", error_code),
        ];
        self.telemetry.in_flight.add(-1, &in_flight_attributes);
        self.telemetry
            .request_duration
            .record(self.started.elapsed().as_secs_f64(), &terminal_attributes);
        self.telemetry.outcomes.add(1, &terminal_attributes);
        if outcome == "timeout" {
            self.telemetry.timeouts.add(1, &terminal_attributes);
        }
        if outcome == "cancelled" {
            self.telemetry.cancellations.add(1, &terminal_attributes);
        }
    }
}

impl Drop for HttpClientRequestGuard {
    fn drop(&mut self) {
        if !self.terminal {
            self.span.record("lily.outcome", "cancelled");
            self.span.record("lily.error_code", "REQUEST_CANCELLED");
            self.span
                .record("lily.cancellation_category", "caller_cancelled");
            self.span.record("otel.status_code", "ERROR");
            self.finish("cancelled", Some("REQUEST_CANCELLED"));
        }
    }
}

struct Http2StreamMetricGuard(Option<Arc<HttpClientTelemetry>>);

impl Http2StreamMetricGuard {
    fn new(telemetry: Arc<HttpClientTelemetry>, active: bool) -> Self {
        if active {
            telemetry
                .http2_active_streams
                .add(1, &[KeyValue::new("network.protocol.version", "2")]);
            Self(Some(telemetry))
        } else {
            Self(None)
        }
    }
}

impl Drop for Http2StreamMetricGuard {
    fn drop(&mut self) {
        if let Some(telemetry) = self.0.as_ref() {
            telemetry
                .http2_active_streams
                .add(-1, &[KeyValue::new("network.protocol.version", "2")]);
        }
    }
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let _ = self.0.insert(key, &value);
    }
}

/// Protocol selection for one client or request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolPreference {
    /// HTTPS negotiates `h2` or HTTP/1.1 with ALPN. Plain HTTP uses HTTP/1.1.
    #[default]
    Auto,
    /// Require HTTP/1.1 and advertise only HTTP/1.1 over TLS.
    Http1Only,
    /// Require HTTP/2. Plain HTTP uses h2c prior knowledge; HTTPS requires ALPN `h2`.
    Http2Only,
}

/// Configuration for [`HttpClient`]. Every resource field is bounded and
/// validated before the first request is sent.
#[derive(Clone)]
pub struct ClientConfig {
    pub(crate) base_address: Option<String>,
    pub(crate) connect_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) max_redirects: u32,
    pub(crate) default_headers: HeaderMap,
    pub(crate) protocol: ProtocolPreference,
    pub(crate) max_in_flight_requests: usize,
    /// Application-level admission cap for one scheme/host/port origin.
    /// Hyper still honours the peer's HTTP/2 SETTINGS limit below this cap.
    pub(crate) max_in_flight_requests_per_origin: usize,
    pub(crate) max_request_body_bytes: usize,
    pub(crate) max_response_body_bytes: usize,
    pub(crate) max_header_count: usize,
    pub(crate) max_header_bytes: usize,
    pub(crate) pool_idle_timeout: Duration,
    pub(crate) pool_max_idle_per_host: usize,
    pub(crate) max_retained_origins: usize,
    pub(crate) http2_initial_stream_window_bytes: u32,
    pub(crate) http2_initial_connection_window_bytes: u32,
    pub(crate) http2_max_frame_bytes: u32,
    pub(crate) http2_keep_alive_interval: Duration,
    pub(crate) http2_keep_alive_timeout: Duration,
    /// Retry only requests Hyper proves were cancelled before any bytes were
    /// written on a reused connection. Partially sent requests are never
    /// replayed by this policy, regardless of HTTP method.
    pub(crate) retry_unstarted_requests: bool,
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientConfig")
            .field(
                "base_address",
                &self.base_address.as_ref().map(|_| "[REDACTED]"),
            )
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("max_redirects", &self.max_redirects)
            .field("default_headers", &self.default_headers)
            .field("protocol", &self.protocol)
            .field("max_in_flight_requests", &self.max_in_flight_requests)
            .field(
                "max_in_flight_requests_per_origin",
                &self.max_in_flight_requests_per_origin,
            )
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .field("max_response_body_bytes", &self.max_response_body_bytes)
            .field("max_header_count", &self.max_header_count)
            .field("max_header_bytes", &self.max_header_bytes)
            .field("pool_idle_timeout", &self.pool_idle_timeout)
            .field("pool_max_idle_per_host", &self.pool_max_idle_per_host)
            .field("max_retained_origins", &self.max_retained_origins)
            .field(
                "http2_initial_stream_window_bytes",
                &self.http2_initial_stream_window_bytes,
            )
            .field(
                "http2_initial_connection_window_bytes",
                &self.http2_initial_connection_window_bytes,
            )
            .field("http2_max_frame_bytes", &self.http2_max_frame_bytes)
            .field("http2_keep_alive_interval", &self.http2_keep_alive_interval)
            .field("http2_keep_alive_timeout", &self.http2_keep_alive_timeout)
            .field("retry_unstarted_requests", &self.retry_unstarted_requests)
            .finish()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        let mut default_headers = HeaderMap::new();
        let _ = default_headers.insert("User-Agent", "lily-http-client/1.0");
        Self {
            base_address: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_redirects: 5,
            default_headers,
            protocol: ProtocolPreference::Auto,
            max_in_flight_requests: 1024,
            max_in_flight_requests_per_origin: 256,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_response_body_bytes: 16 * 1024 * 1024,
            max_header_count: DEFAULT_MAX_HEADER_COUNT,
            max_header_bytes: 64 * 1024,
            pool_idle_timeout: Duration::from_secs(90),
            pool_max_idle_per_host: 32,
            max_retained_origins: 64,
            http2_initial_stream_window_bytes: 1024 * 1024,
            http2_initial_connection_window_bytes: 2 * 1024 * 1024,
            http2_max_frame_bytes: 16 * 1024,
            http2_keep_alive_interval: Duration::from_secs(30),
            http2_keep_alive_timeout: Duration::from_secs(10),
            retry_unstarted_requests: true,
        }
    }
}

impl ClientConfig {
    pub fn base_address(&self) -> Option<&str> {
        self.base_address.as_deref()
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn max_redirects(&self) -> u32 {
        self.max_redirects
    }

    pub fn default_headers(&self) -> &HeaderMap {
        &self.default_headers
    }

    pub fn protocol(&self) -> ProtocolPreference {
        self.protocol
    }

    pub fn max_in_flight_requests(&self) -> usize {
        self.max_in_flight_requests
    }

    pub fn max_in_flight_requests_per_origin(&self) -> usize {
        self.max_in_flight_requests_per_origin
    }

    pub fn max_request_body_bytes(&self) -> usize {
        self.max_request_body_bytes
    }

    pub fn max_response_body_bytes(&self) -> usize {
        self.max_response_body_bytes
    }

    pub fn max_header_count(&self) -> usize {
        self.max_header_count
    }

    pub fn max_header_bytes(&self) -> usize {
        self.max_header_bytes
    }

    pub fn pool_idle_timeout(&self) -> Duration {
        self.pool_idle_timeout
    }

    pub fn pool_max_idle_per_host(&self) -> usize {
        self.pool_max_idle_per_host
    }

    pub fn max_retained_origins(&self) -> usize {
        self.max_retained_origins
    }

    pub fn http2_initial_stream_window_bytes(&self) -> u32 {
        self.http2_initial_stream_window_bytes
    }

    pub fn http2_initial_connection_window_bytes(&self) -> u32 {
        self.http2_initial_connection_window_bytes
    }

    pub fn http2_max_frame_bytes(&self) -> u32 {
        self.http2_max_frame_bytes
    }

    pub fn http2_keep_alive_interval(&self) -> Duration {
        self.http2_keep_alive_interval
    }

    pub fn http2_keep_alive_timeout(&self) -> Duration {
        self.http2_keep_alive_timeout
    }

    pub fn retries_unstarted_requests(&self) -> bool {
        self.retry_unstarted_requests
    }

    pub fn with_base_address(mut self, base_address: impl Into<String>) -> Self {
        self.base_address = Some(base_address.into());
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_max_redirects(mut self, max: u32) -> Self {
        self.max_redirects = max;
        self
    }

    pub fn with_protocol(mut self, protocol: ProtocolPreference) -> Self {
        self.protocol = protocol;
        self
    }

    pub fn with_max_in_flight_requests(mut self, max: usize) -> Self {
        self.max_in_flight_requests = max;
        self
    }

    pub fn with_max_in_flight_requests_per_origin(mut self, max: usize) -> Self {
        self.max_in_flight_requests_per_origin = max;
        self
    }

    pub fn with_max_request_body_bytes(mut self, max: usize) -> Self {
        self.max_request_body_bytes = max;
        self
    }

    pub fn with_max_response_body_bytes(mut self, max: usize) -> Self {
        self.max_response_body_bytes = max;
        self
    }

    pub fn with_max_header_count(mut self, max: usize) -> Self {
        self.max_header_count = max;
        self
    }

    pub fn with_max_header_bytes(mut self, max: usize) -> Self {
        self.max_header_bytes = max;
        self
    }

    pub fn with_pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.pool_idle_timeout = timeout;
        self
    }

    pub fn with_pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.pool_max_idle_per_host = max;
        self
    }

    pub fn with_max_retained_origins(mut self, max: usize) -> Self {
        self.max_retained_origins = max;
        self
    }

    pub fn with_http2_initial_stream_window_bytes(mut self, bytes: u32) -> Self {
        self.http2_initial_stream_window_bytes = bytes;
        self
    }

    pub fn with_http2_initial_connection_window_bytes(mut self, bytes: u32) -> Self {
        self.http2_initial_connection_window_bytes = bytes;
        self
    }

    pub fn with_http2_max_frame_bytes(mut self, bytes: u32) -> Self {
        self.http2_max_frame_bytes = bytes;
        self
    }

    pub fn with_http2_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.http2_keep_alive_interval = interval;
        self
    }

    pub fn with_http2_keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.http2_keep_alive_timeout = timeout;
        self
    }

    pub fn with_retry_unstarted_requests(mut self, retry: bool) -> Self {
        self.retry_unstarted_requests = retry;
        self
    }

    pub fn try_with_default_header(mut self, key: &str, value: &str) -> Result<Self> {
        if is_transport_owned_default_header(key) {
            return Err(HttpClientError::Configuration(
                "default headers cannot set transport-owned framing or routing headers".to_string(),
            ));
        }
        self.default_headers.insert(key, value)?;
        Ok(self)
    }

    pub fn try_with_user_agent(self, user_agent: &str) -> Result<Self> {
        self.try_with_default_header("User-Agent", user_agent)
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(base_address) = &self.base_address {
            let parsed = Url::parse(base_address).map_err(|_| {
                HttpClientError::Configuration("base_address must be an absolute URL".to_string())
            })?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(HttpClientError::Configuration(
                    "base_address must use http or https and contain a host".to_string(),
                ));
            }
            if parsed.host_str().is_none() {
                return Err(HttpClientError::Configuration(
                    "base_address must use http or https and contain a host".to_string(),
                ));
            }
            if !parsed.username().is_empty() || parsed.password().is_some() {
                return Err(HttpClientError::Configuration(
                    "base_address must not contain URL credentials".to_string(),
                ));
            }
            if parsed.query().is_some() || parsed.fragment().is_some() {
                return Err(HttpClientError::Configuration(
                    "base_address must not contain a query or fragment".to_string(),
                ));
            }
        }
        if self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.pool_idle_timeout.is_zero()
        {
            return Err(HttpClientError::Configuration(
                "client and pool timeouts must be greater than zero".to_string(),
            ));
        }
        if self.connect_timeout > MAX_CONNECT_TIMEOUT {
            return Err(HttpClientError::Configuration(format!(
                "connect_timeout must be <= {} seconds",
                MAX_CONNECT_TIMEOUT.as_secs()
            )));
        }
        if self.request_timeout > MAX_REQUEST_TIMEOUT {
            return Err(HttpClientError::Configuration(format!(
                "request_timeout must be <= {} seconds",
                MAX_REQUEST_TIMEOUT.as_secs()
            )));
        }
        if self.pool_idle_timeout > MAX_POOL_IDLE_TIMEOUT {
            return Err(HttpClientError::Configuration(format!(
                "pool_idle_timeout must be <= {} seconds",
                MAX_POOL_IDLE_TIMEOUT.as_secs()
            )));
        }
        if self.max_redirects > 20 {
            return Err(HttpClientError::Configuration(
                "max_redirects must be <= 20".to_string(),
            ));
        }
        if self.max_in_flight_requests == 0 || self.max_in_flight_requests > 1_000_000 {
            return Err(HttpClientError::Configuration(
                "max_in_flight_requests must be in 1..=1_000_000".to_string(),
            ));
        }
        if self.max_in_flight_requests_per_origin == 0
            || self.max_in_flight_requests_per_origin > self.max_in_flight_requests
        {
            return Err(HttpClientError::Configuration(
                "max_in_flight_requests_per_origin must be in 1..=max_in_flight_requests"
                    .to_string(),
            ));
        }
        for (name, value) in [
            ("max_request_body_bytes", self.max_request_body_bytes),
            ("max_response_body_bytes", self.max_response_body_bytes),
        ] {
            if value == 0 || value > 1024 * 1024 * 1024 {
                return Err(HttpClientError::Configuration(format!(
                    "{name} must be in 1..=1GiB"
                )));
            }
        }
        if self.max_header_bytes == 0 || self.max_header_bytes > MAX_HEADER_BYTES {
            return Err(HttpClientError::Configuration(
                "max_header_bytes must be in 1..=1MiB".to_string(),
            ));
        }
        if self.max_header_count == 0 || self.max_header_count > MAX_HEADER_COUNT {
            return Err(HttpClientError::Configuration(
                "max_header_count must be in 1..=1024".to_string(),
            ));
        }
        validate_header_limits(
            &self.default_headers,
            self.max_header_count,
            self.max_header_bytes,
        )?;
        for (name, values) in self.default_headers.iter() {
            if is_transport_owned_default_header(name) {
                return Err(HttpClientError::Configuration(
                    "default headers cannot set transport-owned framing or routing headers"
                        .to_string(),
                ));
            }
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                HttpClientError::Configuration(
                    "default_headers contains an invalid HTTP header name".to_string(),
                )
            })?;
            for value in values {
                HeaderValue::from_str(value).map_err(|_| {
                    HttpClientError::Configuration(
                        "default_headers contains an invalid HTTP header value".to_string(),
                    )
                })?;
            }
        }
        if self.pool_max_idle_per_host == 0 || self.pool_max_idle_per_host > 10_000 {
            return Err(HttpClientError::Configuration(
                "pool_max_idle_per_host must be in 1..=10_000".to_string(),
            ));
        }
        if self.max_retained_origins == 0 || self.max_retained_origins > MAX_RETAINED_ORIGINS {
            return Err(HttpClientError::Configuration(
                "max_retained_origins must be in 1..=1024".to_string(),
            ));
        }
        if !(16_384..=16_777_215).contains(&self.http2_max_frame_bytes) {
            return Err(HttpClientError::Configuration(
                "http2_max_frame_bytes must be in 16384..=16777215".to_string(),
            ));
        }
        for (name, value) in [
            (
                "http2_initial_stream_window_bytes",
                self.http2_initial_stream_window_bytes,
            ),
            (
                "http2_initial_connection_window_bytes",
                self.http2_initial_connection_window_bytes,
            ),
        ] {
            if !(MIN_HTTP2_WINDOW_BYTES..=MAX_HTTP2_WINDOW_BYTES).contains(&value) {
                return Err(HttpClientError::Configuration(format!(
                    "{name} must be in {MIN_HTTP2_WINDOW_BYTES}..={MAX_HTTP2_WINDOW_BYTES}"
                )));
            }
        }
        if self.http2_keep_alive_interval.is_zero() || self.http2_keep_alive_timeout.is_zero() {
            return Err(HttpClientError::Configuration(
                "HTTP/2 keep-alive durations must be greater than zero".to_string(),
            ));
        }
        if self.http2_keep_alive_interval > MAX_HTTP2_KEEP_ALIVE
            || self.http2_keep_alive_timeout > MAX_HTTP2_KEEP_ALIVE
        {
            return Err(HttpClientError::Configuration(format!(
                "HTTP/2 keep-alive durations must be <= {} seconds",
                MAX_HTTP2_KEEP_ALIVE.as_secs()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResolvedRequestPolicy {
    request_timeout: Duration,
    connect_timeout: Duration,
    max_redirects: u32,
    follow_redirects: bool,
    protocol: ProtocolPreference,
}

impl ResolvedRequestPolicy {
    fn from_configs(client: &ClientConfig, request: &RequestConfig) -> Self {
        Self {
            request_timeout: request.timeout.unwrap_or(client.request_timeout),
            connect_timeout: request.connect_timeout.unwrap_or(client.connect_timeout),
            max_redirects: request
                .max_redirects
                .unwrap_or(client.max_redirects)
                .min(client.max_redirects),
            follow_redirects: request.follow_redirects,
            protocol: request.protocol.unwrap_or(client.protocol),
        }
    }

    fn should_follow_redirect(self, status: http::StatusCode) -> bool {
        self.follow_redirects
            && self.max_redirects > 0
            && matches!(
                status,
                http::StatusCode::MOVED_PERMANENTLY
                    | http::StatusCode::FOUND
                    | http::StatusCode::SEE_OTHER
                    | http::StatusCode::TEMPORARY_REDIRECT
                    | http::StatusCode::PERMANENT_REDIRECT
            )
    }

    fn redirect_limit_reached(self, redirects_followed: u32) -> bool {
        redirects_followed >= self.max_redirects
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransportBuildSettings {
    connect_timeout: Duration,
    tcp_happy_eyeballs_delay: Duration,
    protocol: ProtocolPreference,
    pool_idle_timeout: Duration,
    pool_max_idle_per_host: usize,
    http1_max_headers: usize,
    http1_max_buffer_bytes: usize,
    http2_initial_stream_window_bytes: u32,
    http2_initial_connection_window_bytes: u32,
    http2_max_frame_bytes: u32,
    http2_max_header_list_bytes: u32,
    http2_initial_max_send_streams: usize,
    http2_keep_alive_interval: Duration,
    http2_keep_alive_timeout: Duration,
    retry_unstarted_requests: bool,
}

impl TransportBuildSettings {
    fn from_config(
        config: &ClientConfig,
        connect_timeout: Duration,
        protocol: ProtocolPreference,
    ) -> Self {
        Self {
            connect_timeout,
            tcp_happy_eyeballs_delay: HYPER_HAPPY_EYEBALLS_DEFAULT_DELAY.min(connect_timeout / 4),
            protocol,
            pool_idle_timeout: config.pool_idle_timeout,
            pool_max_idle_per_host: config.pool_max_idle_per_host,
            http1_max_headers: config.max_header_count,
            http1_max_buffer_bytes: http1_max_buffer_bytes(
                config.max_header_bytes,
                config.max_header_count,
            ),
            http2_initial_stream_window_bytes: config.http2_initial_stream_window_bytes,
            http2_initial_connection_window_bytes: config.http2_initial_connection_window_bytes,
            http2_max_frame_bytes: config.http2_max_frame_bytes,
            http2_max_header_list_bytes: http2_max_header_list_bytes(
                config.max_header_bytes,
                config.max_header_count,
            ),
            http2_initial_max_send_streams: config.max_in_flight_requests_per_origin,
            http2_keep_alive_interval: config.http2_keep_alive_interval,
            http2_keep_alive_timeout: config.http2_keep_alive_timeout,
            retry_unstarted_requests: config.retry_unstarted_requests,
        }
    }
}

struct TransportClients {
    config: Arc<ClientConfig>,
    connect_timeout: Duration,
    roots: Arc<rustls::RootCertStore>,
    automatic: OnceLock<TransportClient>,
    http1: OnceLock<TransportClient>,
    http2: OnceLock<TransportClient>,
}

impl TransportClients {
    fn new(
        config: Arc<ClientConfig>,
        connect_timeout: Duration,
        roots: Arc<rustls::RootCertStore>,
    ) -> Self {
        Self {
            config,
            connect_timeout,
            roots,
            automatic: OnceLock::new(),
            http1: OnceLock::new(),
            http2: OnceLock::new(),
        }
    }

    fn build(
        config: &ClientConfig,
        connect_timeout: Duration,
        protocol: ProtocolPreference,
        roots: rustls::RootCertStore,
    ) -> TransportClient {
        let settings = TransportBuildSettings::from_config(config, connect_timeout, protocol);
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        // Hyper-util divides the configured deadline across same-family
        // addresses without shortening the budget for a single viable TCP
        // candidate. A mixed-family fallback starts by one quarter of short
        // deadlines (and keeps Hyper's 300ms default for longer ones). The
        // outer connector remains the authoritative DNS + TCP + TLS cap and
        // either timer winning maps to the same typed timeout.
        http.set_connect_timeout(Some(settings.connect_timeout));
        http.set_happy_eyeballs_timeout(Some(settings.tcp_happy_eyeballs_delay));
        let tls = Self::tls_config_with_roots(roots, settings.protocol);
        let connector = HttpsConnector::from((http, Arc::new(tls)));
        let connector = ConnectDeadlineConnector::new(connector, settings.connect_timeout);
        let mut builder = HyperClient::builder(TokioExecutor::new());
        builder
            .pool_timer(TokioTimer::new())
            .timer(TokioTimer::new())
            .pool_idle_timeout(settings.pool_idle_timeout)
            .pool_max_idle_per_host(settings.pool_max_idle_per_host)
            .http1_max_buf_size(settings.http1_max_buffer_bytes);
        if settings.http1_max_headers != HYPER_HTTP1_DEFAULT_MAX_HEADERS {
            builder.http1_max_headers(settings.http1_max_headers);
        }
        builder
            .http2_only(settings.protocol == ProtocolPreference::Http2Only)
            .http2_initial_stream_window_size(settings.http2_initial_stream_window_bytes)
            .http2_initial_connection_window_size(settings.http2_initial_connection_window_bytes)
            .http2_adaptive_window(false)
            .http2_max_frame_size(settings.http2_max_frame_bytes)
            .http2_max_header_list_size(settings.http2_max_header_list_bytes)
            .http2_initial_max_send_streams(settings.http2_initial_max_send_streams)
            .http2_keep_alive_interval(settings.http2_keep_alive_interval)
            .http2_keep_alive_timeout(settings.http2_keep_alive_timeout)
            .http2_keep_alive_while_idle(true)
            .http2_max_concurrent_reset_streams(128)
            // Hyper retries only an unstarted request rejected by a stale,
            // reused connection. It does not replay an accepted H2 stream or
            // an ambiguous transport failure after GOAWAY.
            .retry_canceled_requests(settings.retry_unstarted_requests);
        builder.build(connector)
    }

    fn public_root_store() -> rustls::RootCertStore {
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
    }

    /// Build the exact TLS policy used by the production connector. Keeping
    /// ALPN selection here (instead of relying on an opaque connector default)
    /// makes protocol/codec agreement directly testable without a public
    /// network dependency.
    fn tls_config_with_roots(
        roots: rustls::RootCertStore,
        protocol: ProtocolPreference,
    ) -> rustls::ClientConfig {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("ring provider must support rustls safe protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        tls.alpn_protocols = match protocol {
            ProtocolPreference::Auto => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
            ProtocolPreference::Http1Only => vec![b"http/1.1".to_vec()],
            ProtocolPreference::Http2Only => vec![b"h2".to_vec()],
        };
        tls
    }

    fn select(&self, preference: ProtocolPreference) -> &TransportClient {
        match preference {
            ProtocolPreference::Auto => self.automatic.get_or_init(|| {
                Self::build(
                    &self.config,
                    self.connect_timeout,
                    ProtocolPreference::Auto,
                    self.roots.as_ref().clone(),
                )
            }),
            ProtocolPreference::Http1Only => self.http1.get_or_init(|| {
                Self::build(
                    &self.config,
                    self.connect_timeout,
                    ProtocolPreference::Http1Only,
                    self.roots.as_ref().clone(),
                )
            }),
            ProtocolPreference::Http2Only => self.http2.get_or_init(|| {
                Self::build(
                    &self.config,
                    self.connect_timeout,
                    ProtocolPreference::Http2Only,
                    self.roots.as_ref().clone(),
                )
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OriginKey {
    scheme: String,
    host: String,
    port: u16,
}

impl OriginKey {
    fn from_url(url: &Url) -> Result<Self> {
        let host = url.host_str().ok_or_else(|| {
            HttpClientError::InvalidUrl("request URL does not contain a host".to_string())
        })?;
        let port = url.port_or_known_default().ok_or_else(|| {
            HttpClientError::InvalidUrl("request URL does not have a known port".to_string())
        })?;
        Ok(Self {
            scheme: url.scheme().to_ascii_lowercase(),
            host: host.to_ascii_lowercase(),
            port,
        })
    }
}

struct RetainedOriginEntry {
    transports: TransportClients,
}

struct RetainedOriginRecord {
    entry: Arc<RetainedOriginEntry>,
    active_requests: usize,
    last_used: u64,
}

#[derive(Default)]
struct RetainedOriginState {
    origins: HashMap<OriginKey, RetainedOriginRecord>,
    sequence: u64,
}

impl RetainedOriginState {
    fn next_sequence(&mut self) -> u64 {
        if self.sequence == u64::MAX {
            let mut order = self
                .origins
                .iter()
                .map(|(origin, record)| (origin.clone(), record.last_used))
                .collect::<Vec<_>>();
            order.sort_unstable_by_key(|(_, last_used)| *last_used);
            for (index, (origin, _)) in order.into_iter().enumerate() {
                if let Some(record) = self.origins.get_mut(&origin) {
                    record.last_used = index as u64;
                }
            }
            self.sequence = self.origins.len() as u64;
        }
        self.sequence += 1;
        self.sequence
    }
}

struct RetainedOriginRegistry {
    capacity: usize,
    config: Arc<ClientConfig>,
    roots: Arc<rustls::RootCertStore>,
    state: Mutex<RetainedOriginState>,
}

impl RetainedOriginRegistry {
    fn new(config: Arc<ClientConfig>, roots: Arc<rustls::RootCertStore>) -> Self {
        Self {
            capacity: config.max_retained_origins,
            config,
            roots,
            state: Mutex::new(RetainedOriginState::default()),
        }
    }

    fn acquire(self: &Arc<Self>, origin: OriginKey) -> Result<RetainedOriginLease> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.origins.contains_key(&origin) {
            let sequence = state.next_sequence();
            let Some(record) = state.origins.get_mut(&origin) else {
                return Err(HttpClientError::OriginCapacityExceeded {
                    limit: self.capacity,
                });
            };
            record.active_requests = record.active_requests.checked_add(1).ok_or(
                HttpClientError::OriginCapacityExceeded {
                    limit: self.capacity,
                },
            )?;
            record.last_used = sequence;
            return Ok(RetainedOriginLease {
                registry: Arc::clone(self),
                origin,
                entry: Arc::clone(&record.entry),
            });
        }

        if state.origins.len() == self.capacity {
            let eviction = state
                .origins
                .iter()
                .filter(|(_, record)| record.active_requests == 0)
                .min_by_key(|(_, record)| record.last_used)
                .map(|(origin, _)| origin.clone());
            let Some(eviction) = eviction else {
                return Err(HttpClientError::OriginCapacityExceeded {
                    limit: self.capacity,
                });
            };
            state.origins.remove(&eviction);
        }

        let entry = Arc::new(RetainedOriginEntry {
            transports: TransportClients::new(
                Arc::clone(&self.config),
                self.config.connect_timeout,
                Arc::clone(&self.roots),
            ),
        });
        let sequence = state.next_sequence();
        state.origins.insert(
            origin.clone(),
            RetainedOriginRecord {
                entry: Arc::clone(&entry),
                active_requests: 1,
                last_used: sequence,
            },
        );
        Ok(RetainedOriginLease {
            registry: Arc::clone(self),
            origin,
            entry,
        })
    }

    #[cfg(test)]
    fn retained_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .origins
            .len()
    }
}

struct RetainedOriginLease {
    registry: Arc<RetainedOriginRegistry>,
    origin: OriginKey,
    entry: Arc<RetainedOriginEntry>,
}

impl RetainedOriginLease {
    fn transports(&self) -> &TransportClients {
        &self.entry.transports
    }
}

impl Drop for RetainedOriginLease {
    fn drop(&mut self) {
        let mut state = self
            .registry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(record) = state.origins.get(&self.origin) else {
            return;
        };
        if !Arc::ptr_eq(&record.entry, &self.entry) {
            return;
        }
        let sequence = state.next_sequence();
        let Some(record) = state.origins.get_mut(&self.origin) else {
            return;
        };
        record.active_requests = record.active_requests.saturating_sub(1);
        record.last_used = sequence;
    }
}

/// Weak entries ensure the number of remembered origins cannot grow beyond
/// concurrently admitted work. Expired entries are removed on every lookup.
#[derive(Debug)]
struct PerOriginAdmission {
    permits: usize,
    origins: Mutex<HashMap<OriginKey, Weak<Semaphore>>>,
}

impl PerOriginAdmission {
    fn new(permits: usize) -> Self {
        Self {
            permits,
            origins: Mutex::new(HashMap::new()),
        }
    }

    fn semaphore(&self, origin: OriginKey) -> Arc<Semaphore> {
        let mut origins = self
            .origins
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        origins.retain(|_, semaphore| semaphore.strong_count() > 0);
        if let Some(semaphore) = origins.get(&origin).and_then(Weak::upgrade) {
            return semaphore;
        }

        let semaphore = Arc::new(Semaphore::new(self.permits));
        origins.insert(origin, Arc::downgrade(&semaphore));
        semaphore
    }

    async fn acquire(&self, origin: OriginKey) -> Result<OwnedSemaphorePermit> {
        self.semaphore(origin)
            .acquire_owned()
            .await
            .map_err(|_| HttpClientError::Client("HTTP client is shutting down".to_string()))
    }
}

/// Cloneable, pooled HTTP client. Dropping/cancelling a request releases its
/// response stream and lets Hyper reset HTTP/2 flow-control state.
#[derive(Clone)]
pub struct HttpClient {
    config: Arc<ClientConfig>,
    trust_roots: Arc<rustls::RootCertStore>,
    retained_origins: Arc<RetainedOriginRegistry>,
    admission: Arc<Semaphore>,
    per_origin_admission: Arc<PerOriginAdmission>,
    telemetry: Arc<HttpClientTelemetry>,
}

impl HttpClient {
    pub fn new() -> Self {
        Self::from_validated_config(
            ClientConfig::default(),
            TransportClients::public_root_store(),
        )
    }

    fn from_validated_config(config: ClientConfig, roots: rustls::RootCertStore) -> Self {
        let config = Arc::new(config);
        let admission = Arc::new(Semaphore::new(config.max_in_flight_requests));
        let per_origin_admission = Arc::new(PerOriginAdmission::new(
            config.max_in_flight_requests_per_origin,
        ));
        let trust_roots = Arc::new(roots);
        let retained_origins = Arc::new(RetainedOriginRegistry::new(
            Arc::clone(&config),
            Arc::clone(&trust_roots),
        ));
        Self {
            config,
            trust_roots,
            retained_origins,
            admission,
            per_origin_admission,
            telemetry: HttpClientTelemetry::new(),
        }
    }

    pub fn try_with_config(config: ClientConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self::from_validated_config(
            config,
            TransportClients::public_root_store(),
        ))
    }

    pub(crate) fn try_with_config_and_roots(
        config: ClientConfig,
        roots: rustls::RootCertStore,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self::from_validated_config(config, roots))
    }

    /// Create a GET request builder after resolving and validating `url`.
    pub fn get(&self, url: &str) -> Result<RequestBuilder> {
        self.request(Method::Get, url)
    }
    /// Create a POST request builder after resolving and validating `url`.
    pub fn post(&self, url: &str) -> Result<RequestBuilder> {
        self.request(Method::Post, url)
    }
    /// Create a PUT request builder after resolving and validating `url`.
    pub fn put(&self, url: &str) -> Result<RequestBuilder> {
        self.request(Method::Put, url)
    }
    /// Create a DELETE request builder after resolving and validating `url`.
    pub fn delete(&self, url: &str) -> Result<RequestBuilder> {
        self.request(Method::Delete, url)
    }
    /// Create a PATCH request builder after resolving and validating `url`.
    pub fn patch(&self, url: &str) -> Result<RequestBuilder> {
        self.request(Method::Patch, url)
    }
    /// Create a HEAD request builder after resolving and validating `url`.
    pub fn head(&self, url: &str) -> Result<RequestBuilder> {
        self.request(Method::Head, url)
    }

    /// Resolve a relative or absolute URL and create a request builder.
    ///
    /// URL syntax, scheme, host, credentials and ambiguous authority
    /// separators are rejected here, before a builder or network operation is
    /// published.
    pub fn request(&self, method: Method, url: &str) -> Result<RequestBuilder> {
        let resolved = resolve_request_url(self.config.base_address.as_deref(), url)?;
        let mut builder = RequestBuilder::new();
        builder.method(method);
        builder.url_parsed(resolved)?;
        let base_origin = self
            .config
            .base_address
            .as_deref()
            .map(parse_request_url)
            .transpose()?;
        builder.with_client_default_headers(self.config.default_headers.clone(), base_origin);
        Ok(builder)
    }

    pub async fn execute(&self, mut request: Request) -> Result<Response> {
        let policy = ResolvedRequestPolicy::from_configs(&self.config, request.config());
        let method = request.method().telemetry_name().to_string();
        let url = request.url().clone();
        let host = url.host_str().unwrap_or("unknown").to_string();
        let port = url.port_or_known_default().unwrap_or(0);
        let span = tracing::info_span!(
            "http.client.request",
            otel.kind = "client",
            http.request.method = %method,
            server.address = %host,
            server.port = port,
            http.response.status_code = tracing::field::Empty,
            network.protocol.version = tracing::field::Empty,
            http.request.body.size = tracing::field::Empty,
            http.response.body.size = tracing::field::Empty,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            lily.timeout_category = tracing::field::Empty,
            lily.cancellation_category = tracing::field::Empty,
            lily.queue_wait_ms = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let context = lily_trace::context_for_span(&span);
        lily_trace::inject_context(&context, &mut HeaderInjector(request.headers_mut()));
        let mut metric_guard = HttpClientRequestGuard::start(
            Arc::clone(&self.telemetry),
            span.clone(),
            method.clone(),
        );

        let result = if !url.username().is_empty() || url.password().is_some() {
            Err(HttpClientError::InvalidUrl(
                "request URL must not contain credentials".to_string(),
            ))
        } else {
            match tokio::time::timeout(policy.request_timeout, self.execute_inner(request, policy))
                .instrument(span.clone())
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    span.record("lily.timeout_category", "request_deadline");
                    Err(HttpClientError::Timeout)
                }
            }
        };
        match &result {
            Ok(response) => {
                span.record("http.response.status_code", response.status().as_u16());
                span.record("network.protocol.version", response.version().as_str());
                span.record(
                    "http.response.body.size",
                    response.metadata().content_length.unwrap_or(0),
                );
                if response.status().is_server_error() {
                    span.record("otel.status_code", "ERROR");
                    span.record("lily.outcome", "server_error");
                    span.record("lily.error_code", "HTTP_5XX");
                    metric_guard.finish("server_error", Some("HTTP_5XX"));
                } else {
                    span.record("lily.outcome", "success");
                    metric_guard.finish("success", None);
                }
            }
            Err(error) => {
                span.record("otel.status_code", "ERROR");
                span.record("lily.error_code", error.error_code());
                if matches!(error, HttpClientError::ConnectTimeout) {
                    span.record("lily.timeout_category", "connect_deadline");
                }
                let outcome = if error.is_timeout() {
                    "timeout"
                } else {
                    "error"
                };
                span.record("lily.outcome", outcome);
                metric_guard.finish(outcome, Some(error.error_code()));
            }
        }
        result
    }

    async fn execute_inner(
        &self,
        mut request: Request,
        policy: ResolvedRequestPolicy,
    ) -> Result<Response> {
        let origin = OriginKey::from_url(request.url())?;
        let retained_origin = if policy.connect_timeout == self.config.connect_timeout {
            Some(self.retained_origins.acquire(origin.clone())?)
        } else {
            None
        };
        let admission_started = Instant::now();
        // Acquire the origin permit first so one hot origin cannot occupy all
        // global permits while its own requests wait behind the origin cap.
        let mut first_origin_admission = Some(self.per_origin_admission.acquire(origin).await?);
        let _global_admission = self
            .admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| HttpClientError::Client("HTTP client is shutting down".to_string()))?;
        let admission_wait = admission_started.elapsed();
        self.telemetry.admission_wait.record(
            admission_wait.as_secs_f64(),
            &[KeyValue::new("lily.admission", "client_request")],
        );
        tracing::Span::current()
            .record("lily.queue_wait_ms", admission_wait.as_secs_f64() * 1_000.0);
        tracing::event!(
            name: "http.client.admission",
            tracing::Level::INFO,
            lily.outcome = "accepted",
            lily.wait_ms = admission_wait.as_secs_f64() * 1_000.0,
            "HTTP client admission acquired"
        );
        let connect_timeout = policy.connect_timeout;
        if connect_timeout.is_zero() {
            return Err(HttpClientError::Configuration(
                "connect timeout must be greater than zero".to_string(),
            ));
        }
        let local_transports;
        let transports = if let Some(retained_origin) = retained_origin.as_ref() {
            retained_origin.transports()
        } else {
            local_transports = TransportClients::new(
                Arc::clone(&self.config),
                connect_timeout,
                Arc::clone(&self.trust_roots),
            );
            &local_transports
        };

        let mut body = if let Some(mut body) = request.take_body() {
            Self::buffer_request_body(body.as_mut(), self.config.max_request_body_bytes).await?
        } else {
            Bytes::new()
        };
        tracing::Span::current().record("http.request.body.size", body.len() as u64);
        let mut method = request.method().clone();
        let mut url = request.url().clone();
        let mut headers = request.headers().clone();
        let max_redirects = policy.max_redirects;
        let preference = policy.protocol;
        let mut redirect_count = 0_u32;
        let request_start = SystemTime::now();
        let started = Instant::now();

        loop {
            // Redirects can change origin. Hold exactly one origin permit for
            // the request currently on the wire and release it before the
            // next redirect attempt.
            let _origin_admission = match first_origin_admission.take() {
                Some(permit) => permit,
                None => {
                    self.per_origin_admission
                        .acquire(OriginKey::from_url(&url)?)
                        .await?
                }
            };
            let hyper_request = Self::build_request_with_limits(
                &method,
                &url,
                &headers,
                body.clone(),
                self.config.max_header_count,
                self.config.max_header_bytes,
            )?;
            let dependency_span = tracing::info_span!(
                "http.client.dependency",
                otel.kind = "client",
                lily.dependency = "http_transport",
                lily.outcome = tracing::field::Empty,
                lily.error_code = tracing::field::Empty,
                lily.timeout_category = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );
            let dependency_started = Instant::now();
            let dependency_result = transports
                .select(preference)
                .request(hyper_request)
                .instrument(dependency_span.clone())
                .await;
            let mapped_transport_error = dependency_result
                .as_ref()
                .err()
                .map(|error| map_transport_error(error));
            let connect_timed_out = matches!(
                mapped_transport_error.as_ref(),
                Some(HttpClientError::ConnectTimeout)
            );
            let dependency_outcome = if dependency_result.is_ok() {
                "success"
            } else if connect_timed_out {
                "timeout"
            } else {
                "error"
            };
            self.telemetry.dependency_duration.record(
                dependency_started.elapsed().as_secs_f64(),
                &[KeyValue::new("lily.outcome", dependency_outcome)],
            );
            dependency_span.record("lily.outcome", dependency_outcome);
            let hyper_response = dependency_result.map_err(|_| {
                let error_code = if connect_timed_out {
                    dependency_span.record("lily.timeout_category", "connect_deadline");
                    "CONNECT_TIMEOUT"
                } else {
                    "CONNECTION_ERROR"
                };
                dependency_span.record("lily.error_code", error_code);
                dependency_span.record("otel.status_code", "ERROR");
                mapped_transport_error.unwrap_or_else(|| {
                    HttpClientError::Connection("transport request failed".to_string())
                })
            })?;
            let (parts, incoming) = hyper_response.into_parts();
            Self::validate_negotiated_protocol(preference, &url, parts.version)?;
            let _http2_stream = Http2StreamMetricGuard::new(
                Arc::clone(&self.telemetry),
                parts.version == Version::HTTP_2,
            );
            let response_headers = Self::convert_response_headers(
                &parts.headers,
                self.config.max_header_count,
                self.config.max_header_bytes,
            )?;
            let response_body =
                Self::collect_body(incoming, self.config.max_response_body_bytes).await?;

            if policy.should_follow_redirect(parts.status) {
                let location = parts
                    .headers
                    .get(http::header::LOCATION)
                    .and_then(|value| value.to_str().ok());
                if let Some(location) = location {
                    if policy.redirect_limit_reached(redirect_count) {
                        return Err(HttpClientError::ResponseParsing(format!(
                            "redirect limit of {max_redirects} exceeded"
                        )));
                    }
                    let previous = url.clone();
                    let next = previous.join(location).map_err(HttpClientError::from)?;
                    validate_redirect_target(&previous, &next)?;
                    url = next;
                    if parts.status == http::StatusCode::SEE_OTHER
                        || ((parts.status == http::StatusCode::MOVED_PERMANENTLY
                            || parts.status == http::StatusCode::FOUND)
                            && method == Method::Post)
                    {
                        method = Method::Get;
                        body = Bytes::new();
                        headers.remove("content-length");
                        headers.remove("content-type");
                    }
                    redirect_count += 1;
                    tracing::event!(
                        name: "http.client.redirect",
                        tracing::Level::INFO,
                        lily.outcome = "followed",
                        lily.redirect_count = redirect_count,
                        "HTTP redirect followed without recording its location"
                    );
                    continue;
                }
            }

            let version = match parts.version {
                Version::HTTP_10 => HttpVersion::Http10,
                Version::HTTP_2 => HttpVersion::Http2,
                _ => HttpVersion::Http11,
            };
            let metadata = ResponseMetadata::new()
                .with_request_start(request_start)
                .with_response_time(SystemTime::now())
                .with_duration(started.elapsed())
                .with_content_length(response_body.len() as u64)
                .with_redirect_count(redirect_count);
            let header_pairs = response_headers
                .iter()
                .flat_map(|(name, values)| {
                    values
                        .iter()
                        .map(move |value| (name.to_string(), value.clone()))
                })
                .collect::<Vec<_>>();
            return Ok(ResponseBuilder::new()
                .status(StatusCode::from_u16(parts.status.as_u16()))
                .headers(header_pairs)?
                .body(Box::new(BinaryBody::new(response_body)) as Box<dyn Body>)
                .url(url)
                .version(version)
                .metadata(metadata)
                .build());
        }
    }

    fn build_request_with_limits(
        method: &Method,
        url: &Url,
        headers: &HeaderMap,
        body: Bytes,
        max_header_count: usize,
        max_header_bytes: usize,
    ) -> Result<http::Request<RequestBody>> {
        Self::validate_request_header_limits(headers, max_header_count, max_header_bytes)?;
        let method = HttpMethod::from_bytes(method.as_str().as_bytes())
            .map_err(|_| HttpClientError::invalid_method("request method token is invalid"))?;
        let mut wire_url = url.clone();
        wire_url.set_fragment(None);
        let uri: Uri = wire_url
            .as_str()
            .parse()
            .map_err(|error| HttpClientError::RequestBuilding(format!("invalid URI: {error}")))?;
        let mut output = http::Request::builder().method(method).uri(uri);
        for (name, values) in headers.iter() {
            if is_hop_by_hop_or_proxy_only_request_header(name)
                || name.eq_ignore_ascii_case("content-length")
            {
                continue;
            }
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| HttpClientError::InvalidHeader(error.to_string()))?;
            for value in values {
                let value = HeaderValue::from_str(value)
                    .map_err(|error| HttpClientError::InvalidHeader(error.to_string()))?;
                output = output.header(name.clone(), value);
            }
        }
        output
            .body(Full::new(body))
            .map_err(|error| HttpClientError::RequestBuilding(error.to_string()))
    }

    #[cfg(test)]
    fn build_request(
        method: &Method,
        url: &Url,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<http::Request<RequestBody>> {
        Self::build_request_with_limits(method, url, headers, body, usize::MAX, usize::MAX)
    }

    fn validate_request_header_limits(
        headers: &HeaderMap,
        max_count: usize,
        max_bytes: usize,
    ) -> Result<()> {
        validate_header_limits(headers, max_count, max_bytes)
    }

    async fn buffer_request_body(body: &mut dyn Body, limit: usize) -> Result<Bytes> {
        if !body.is_repeatable() {
            return Err(HttpClientError::UnsupportedConfiguration(
                "streaming/non-repeatable request bodies are not supported by the bounded-buffered v1 transport"
                    .to_string(),
            ));
        }
        let declared = body.content_length().ok_or_else(|| {
            HttpClientError::UnsupportedConfiguration(
                "request body must declare its buffered length".to_string(),
            )
        })?;
        if declared > limit {
            return Err(HttpClientError::LimitExceeded {
                resource: "request body".to_string(),
                limit,
            });
        }

        let bytes = body.to_bytes().await?;
        if bytes.len() != declared {
            return Err(HttpClientError::UnsupportedConfiguration(
                "request body materialized length does not match its declared buffered length"
                    .to_string(),
            ));
        }
        if bytes.len() > limit {
            return Err(HttpClientError::LimitExceeded {
                resource: "request body".to_string(),
                limit,
            });
        }
        Ok(bytes)
    }

    fn validate_negotiated_protocol(
        preference: ProtocolPreference,
        url: &Url,
        negotiated: Version,
    ) -> Result<()> {
        let accepted = match preference {
            ProtocolPreference::Auto if url.scheme() == "https" => {
                matches!(negotiated, Version::HTTP_11 | Version::HTTP_2)
            }
            ProtocolPreference::Auto | ProtocolPreference::Http1Only => {
                negotiated == Version::HTTP_11
            }
            ProtocolPreference::Http2Only => negotiated == Version::HTTP_2,
        };
        if accepted {
            return Ok(());
        }

        Err(HttpClientError::ProtocolNegotiation {
            requested: match preference {
                ProtocolPreference::Auto if url.scheme() == "https" => "h2 or http/1.1",
                ProtocolPreference::Auto | ProtocolPreference::Http1Only => "http/1.1",
                ProtocolPreference::Http2Only => "h2",
            }
            .to_string(),
            negotiated: match negotiated {
                Version::HTTP_09 => "http/0.9",
                Version::HTTP_10 => "http/1.0",
                Version::HTTP_11 => "http/1.1",
                Version::HTTP_2 => "h2",
                Version::HTTP_3 => "h3",
                _ => "unknown",
            }
            .to_string(),
        })
    }

    fn convert_response_headers(
        headers: &http::HeaderMap,
        max_count: usize,
        max_bytes: usize,
    ) -> Result<HeaderMap> {
        if headers.len() > max_count {
            return Err(HttpClientError::LimitExceeded {
                resource: "response header count".to_string(),
                limit: max_count,
            });
        }
        let mut total = 0_usize;
        let mut output = HeaderMap::new();
        for (name, value) in headers {
            total = total
                .checked_add(name.as_str().len())
                .and_then(|size| size.checked_add(value.as_bytes().len()))
                .ok_or_else(|| HttpClientError::LimitExceeded {
                    resource: "response headers".to_string(),
                    limit: max_bytes,
                })?;
            if total > max_bytes {
                return Err(HttpClientError::LimitExceeded {
                    resource: "response headers".to_string(),
                    limit: max_bytes,
                });
            }
            let value = value
                .to_str()
                .map_err(|error| HttpClientError::InvalidHeader(error.to_string()))?;
            output.append(name.as_str(), value)?;
        }
        Ok(output)
    }

    async fn collect_body<B>(mut body: B, limit: usize) -> Result<Bytes>
    where
        B: HyperBody<Data = Bytes> + Unpin,
        B::Error: std::fmt::Display,
    {
        if body
            .size_hint()
            .upper()
            .is_some_and(|size| size > limit as u64)
        {
            return Err(HttpClientError::LimitExceeded {
                resource: "response body".to_string(),
                limit,
            });
        }
        let mut output =
            BytesMut::with_capacity(body.size_hint().lower().min(limit as u64) as usize);
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|error| HttpClientError::BodyReading(error.to_string()))?;
            if let Ok(data) = frame.into_data() {
                let next = output.len().checked_add(data.len()).ok_or_else(|| {
                    HttpClientError::LimitExceeded {
                        resource: "response body".to_string(),
                        limit,
                    }
                })?;
                if next > limit {
                    return Err(HttpClientError::LimitExceeded {
                        resource: "response body".to_string(),
                        limit,
                    });
                }
                output.extend_from_slice(&data);
            }
        }
        Ok(output.freeze())
    }

    pub fn config(&self) -> &ClientConfig {
        self.config.as_ref()
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub(crate) struct ValidatedClientProfile {
    config: ClientConfig,
    roots: rustls::RootCertStore,
}

impl ValidatedClientProfile {
    pub(crate) fn new(config: ClientConfig, additional_ca_pem: Option<&[u8]>) -> Result<Self> {
        config.validate()?;
        let mut roots = TransportClients::public_root_store();
        if let Some(pem) = additional_ca_pem {
            add_additional_ca_pem(&mut roots, 0, pem)?;
        }
        Ok(Self { config, roots })
    }

    pub(crate) fn build_client(&self) -> Result<HttpClient> {
        HttpClient::try_with_config_and_roots(self.config.clone(), self.roots.clone())
    }

    #[cfg(test)]
    pub(crate) fn config(&self) -> &ClientConfig {
        &self.config
    }
}

#[derive(Clone)]
pub struct HttpClientBuilder {
    config: ClientConfig,
    trust_roots: rustls::RootCertStore,
    additional_ca_certificates: usize,
}

impl std::fmt::Debug for HttpClientBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpClientBuilder")
            .field("config", &self.config)
            .field(
                "additional_ca_certificates",
                &self.additional_ca_certificates,
            )
            .finish_non_exhaustive()
    }
}

impl HttpClientBuilder {
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            trust_roots: TransportClients::public_root_store(),
            additional_ca_certificates: 0,
        }
    }
    pub fn base_address(mut self, base_address: impl Into<String>) -> Self {
        self.config.base_address = Some(base_address.into());
        self
    }
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }
    pub fn max_redirects(mut self, max: u32) -> Self {
        self.config.max_redirects = max;
        self
    }
    pub fn protocol(mut self, protocol: ProtocolPreference) -> Self {
        self.config.protocol = protocol;
        self
    }
    pub fn max_response_body_bytes(mut self, max: usize) -> Self {
        self.config.max_response_body_bytes = max;
        self
    }
    pub fn max_request_body_bytes(mut self, max: usize) -> Self {
        self.config.max_request_body_bytes = max;
        self
    }
    pub fn max_header_count(mut self, max: usize) -> Self {
        self.config.max_header_count = max;
        self
    }
    pub fn max_header_bytes(mut self, max: usize) -> Self {
        self.config.max_header_bytes = max;
        self
    }
    pub fn max_in_flight_requests(mut self, max: usize) -> Self {
        self.config.max_in_flight_requests = max;
        self
    }
    pub fn max_in_flight_requests_per_origin(mut self, max: usize) -> Self {
        self.config.max_in_flight_requests_per_origin = max;
        self
    }
    pub fn retry_unstarted_requests(mut self, retry: bool) -> Self {
        self.config.retry_unstarted_requests = retry;
        self
    }
    pub fn pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.config.pool_idle_timeout = timeout;
        self
    }
    pub fn pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.config.pool_max_idle_per_host = max;
        self
    }
    pub fn max_retained_origins(mut self, max: usize) -> Self {
        self.config.max_retained_origins = max;
        self
    }
    pub fn http2_initial_stream_window_bytes(mut self, bytes: u32) -> Self {
        self.config.http2_initial_stream_window_bytes = bytes;
        self
    }
    pub fn http2_initial_connection_window_bytes(mut self, bytes: u32) -> Self {
        self.config.http2_initial_connection_window_bytes = bytes;
        self
    }
    pub fn http2_max_frame_bytes(mut self, bytes: u32) -> Self {
        self.config.http2_max_frame_bytes = bytes;
        self
    }
    pub fn http2_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.config.http2_keep_alive_interval = interval;
        self
    }
    pub fn http2_keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.config.http2_keep_alive_timeout = timeout;
        self
    }
    pub fn user_agent(mut self, user_agent: &str) -> Result<Self> {
        self.config
            .default_headers
            .insert("User-Agent", user_agent)?;
        Ok(self)
    }
    pub fn default_header(mut self, key: &str, value: &str) -> Result<Self> {
        self.config.default_headers.insert(key, value)?;
        Ok(self)
    }
    /// Adds PEM encoded trust anchors while retaining the public WebPKI root
    /// set. Certificate and hostname verification cannot be disabled.
    pub fn additional_ca_pem(mut self, pem: impl AsRef<[u8]>) -> Result<Self> {
        let added = add_additional_ca_pem(
            &mut self.trust_roots,
            self.additional_ca_certificates,
            pem.as_ref(),
        )?;
        self.additional_ca_certificates += added;
        Ok(self)
    }
    pub fn try_build(self) -> Result<HttpClient> {
        HttpClient::try_with_config_and_roots(self.config, self.trust_roots)
    }
}

fn add_additional_ca_pem(
    roots: &mut rustls::RootCertStore,
    existing_certificates: usize,
    pem: &[u8],
) -> Result<usize> {
    if pem.is_empty() || pem.len() > MAX_ADDITIONAL_CA_BUNDLE_BYTES {
        return Err(HttpClientError::Tls(
            "additional CA bundle is outside the supported size bound".into(),
        ));
    }
    let mut added = 0_usize;
    use rustls_pki_types::pem::{PemObject as _, SectionKind};

    for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(pem) {
        let certificate = match item
            .map_err(|_| HttpClientError::Tls("additional CA bundle could not be parsed".into()))?
        {
            (SectionKind::Certificate, certificate) => {
                rustls_pki_types::CertificateDer::from(certificate)
            }
            _ => {
                return Err(HttpClientError::Tls(
                    "additional CA bundle contains non-certificate material".into(),
                ));
            }
        };
        roots
            .add(certificate)
            .map_err(|_| HttpClientError::Tls("additional CA certificate was rejected".into()))?;
        added = added.saturating_add(1);
        if existing_certificates.saturating_add(added) > MAX_ADDITIONAL_CA_CERTIFICATES {
            return Err(HttpClientError::Tls(
                "additional CA bundle contains too many certificates".into(),
            ));
        }
    }
    if added == 0 {
        return Err(HttpClientError::Tls(
            "additional CA bundle contains no certificates".into(),
        ));
    }
    Ok(added)
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn http1_max_buffer_bytes(max_header_bytes: usize, max_header_count: usize) -> usize {
    max_header_count
        .checked_mul(HTTP1_HEADER_LINE_FRAMING_BYTES)
        .and_then(|framing| max_header_bytes.checked_add(framing))
        .and_then(|headers| headers.checked_add(HTTP1_STATUS_LINE_ALLOWANCE_BYTES))
        .and_then(|head| head.checked_add(HTTP1_FINAL_HEADER_TERMINATOR_BYTES))
        .unwrap_or(usize::MAX)
        .max(HTTP1_MIN_BUFFER_BYTES)
}

fn http2_max_header_list_bytes(max_header_bytes: usize, max_header_count: usize) -> u32 {
    max_header_count
        .checked_mul(HTTP2_HEADER_FIELD_OVERHEAD_BYTES)
        .and_then(|field_overhead| max_header_bytes.checked_add(field_overhead))
        .and_then(|headers| {
            headers.checked_add(
                HTTP2_HEADER_FIELD_OVERHEAD_BYTES
                    + HTTP2_STATUS_NAME_BYTES
                    + HTTP2_STATUS_VALUE_BYTES,
            )
        })
        .and_then(|headers| headers.checked_add(HTTP2_STRICT_BOUND_ALLOWANCE_BYTES))
        .and_then(|headers| u32::try_from(headers).ok())
        .unwrap_or(u32::MAX)
}

fn resolve_request_url(base_address: Option<&str>, reference: &str) -> Result<Url> {
    if reference.contains('\\') {
        return Err(HttpClientError::InvalidUrl(
            "relative request URL contains an ambiguous authority separator".to_string(),
        ));
    }

    match Url::parse(reference) {
        Ok(absolute) => {
            validate_request_url(&absolute)?;
            return Ok(absolute);
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => {}
        Err(_) => {
            return Err(HttpClientError::InvalidUrl(
                "request URL is invalid".to_string(),
            ))
        }
    }

    let base_address = base_address.ok_or_else(|| {
        HttpClientError::InvalidUrl("relative URL requires a configured base_address".to_string())
    })?;
    let mut base = Url::parse(base_address).map_err(|_| {
        HttpClientError::Configuration("base_address must be an absolute URL".to_string())
    })?;

    // A base address is a path prefix in Lily's public API. Url::join treats a
    // final segment without '/' as a file, so make the prefix directory-like.
    if !base.path().ends_with('/') {
        base.path_segments_mut()
            .map_err(|_| {
                HttpClientError::Configuration(
                    "base_address cannot be used as a hierarchical URL".to_string(),
                )
            })?
            .push("");
    }

    // Leading slashes historically remained under the configured prefix. Keep
    // that contract while Url handles escaping, dot segments, query and fragment.
    let relative = reference.strip_prefix('/').unwrap_or(reference);
    if relative.starts_with('/') {
        return Err(HttpClientError::InvalidUrl(
            "relative request URL contains an ambiguous authority separator".to_string(),
        ));
    }
    let resolved = base
        .join(relative)
        .map_err(|_| HttpClientError::InvalidUrl("invalid relative request URL".to_string()))?;
    if !same_origin(&base, &resolved) {
        return Err(HttpClientError::InvalidUrl(
            "relative request URL must not override the base origin".to_string(),
        ));
    }
    validate_request_url(&resolved)?;
    Ok(resolved)
}

fn validate_header_limits(headers: &HeaderMap, max_count: usize, max_bytes: usize) -> Result<()> {
    let mut count = 0_usize;
    let mut total = 0_usize;
    for (name, values) in headers.iter() {
        for value in values {
            count = count
                .checked_add(1)
                .ok_or_else(|| HttpClientError::LimitExceeded {
                    resource: "request header count".to_string(),
                    limit: max_count,
                })?;
            total = total
                .checked_add(name.len())
                .and_then(|size| size.checked_add(value.len()))
                .ok_or_else(|| HttpClientError::LimitExceeded {
                    resource: "request headers".to_string(),
                    limit: max_bytes,
                })?;
            if count > max_count {
                return Err(HttpClientError::LimitExceeded {
                    resource: "request header count".to_string(),
                    limit: max_count,
                });
            }
            if total > max_bytes {
                return Err(HttpClientError::LimitExceeded {
                    resource: "request headers".to_string(),
                    limit: max_bytes,
                });
            }
        }
    }
    Ok(())
}

fn validate_redirect_target(previous: &Url, next: &Url) -> Result<()> {
    if previous.scheme() == "https" && next.scheme() != "https" {
        return Err(HttpClientError::RedirectRejected(
            "HTTPS-to-HTTP downgrade is forbidden".to_string(),
        ));
    }
    if !next.username().is_empty() || next.password().is_some() {
        return Err(HttpClientError::RedirectRejected(
            "redirect target must not contain URL credentials".to_string(),
        ));
    }
    if !same_origin(previous, next) {
        return Err(HttpClientError::RedirectRejected(
            "cross-origin redirects are forbidden".to_string(),
        ));
    }
    Ok(())
}

fn is_transport_owned_default_header(name: &str) -> bool {
    is_hop_by_hop_or_proxy_only_request_header(name)
        || name.eq_ignore_ascii_case("content-length")
        || name.eq_ignore_ascii_case("host")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use hyper::body::Incoming;
    use hyper::{service::service_fn, Response as HyperResponse};
    use hyper_util::rt::TokioIo;
    use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot};

    #[test]
    fn metric_method_labels_are_closed_over_standard_methods() {
        for method in [
            "CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "TRACE",
        ] {
            assert_eq!(http_method_metric_label(method), method);
        }
        for extension in ["PURGE", "SEARCH", "get", "PRIVATE-METHOD-SENTINEL"] {
            assert_eq!(http_method_metric_label(extension), "_OTHER");
        }
    }

    #[derive(Clone)]
    struct PendingConnector {
        future_dropped: Arc<AtomicBool>,
    }

    struct PendingConnectFuture {
        future_dropped: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct WrappedConnectError(ConnectDeadlineError);

    impl fmt::Display for WrappedConnectError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("wrapped connection failure")
        }
    }

    impl StdError for WrappedConnectError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(&self.0)
        }
    }

    #[derive(Debug)]
    struct WrappedIoTimeoutError(std::io::Error);

    impl fmt::Display for WrappedIoTimeoutError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("wrapped TCP attempt failure")
        }
    }

    impl StdError for WrappedIoTimeoutError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(&self.0)
        }
    }

    #[derive(Clone, Copy)]
    struct InnerTimedOutConnector;

    impl Service<Uri> for InnerTimedOutConnector {
        type Response = ();
        type Error = WrappedIoTimeoutError;
        type Future = std::future::Ready<std::result::Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _context: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _destination: Uri) -> Self::Future {
            std::future::ready(Err(WrappedIoTimeoutError(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "test timeout payload must be erased",
            ))))
        }
    }

    #[derive(Clone)]
    struct DeterministicAddressFailoverConnector {
        attempts: Arc<AtomicUsize>,
        tcp_failover_budget: Duration,
        second_address_succeeds: bool,
    }

    impl Service<Uri> for DeterministicAddressFailoverConnector {
        type Response = ();
        type Error = WrappedIoTimeoutError;
        type Future =
            Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(
            &mut self,
            _context: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _destination: Uri) -> Self::Future {
            let attempts = Arc::clone(&self.attempts);
            let per_address_budget = self.tcp_failover_budget / 2;
            let second_address_succeeds = self.second_address_succeeds;
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(per_address_budget).await;
                attempts.fetch_add(1, Ordering::SeqCst);
                if second_address_succeeds {
                    Ok(())
                } else {
                    tokio::time::sleep(per_address_budget).await;
                    Err(WrappedIoTimeoutError(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "all deterministic address attempts timed out",
                    )))
                }
            })
        }
    }

    #[derive(Clone, Copy)]
    struct DelayedSuccessConnector {
        delay: Duration,
    }

    impl Service<Uri> for DelayedSuccessConnector {
        type Response = ();
        type Error = Infallible;
        type Future =
            Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(
            &mut self,
            _context: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _destination: Uri) -> Self::Future {
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(())
            })
        }
    }

    impl Future for PendingConnectFuture {
        type Output = std::result::Result<(), Infallible>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingConnectFuture {
        fn drop(&mut self) {
            self.future_dropped.store(true, Ordering::SeqCst);
        }
    }

    impl Service<Uri> for PendingConnector {
        type Response = ();
        type Error = Infallible;
        type Future = PendingConnectFuture;

        fn poll_ready(
            &mut self,
            _context: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _destination: Uri) -> Self::Future {
            PendingConnectFuture {
                future_dropped: Arc::clone(&self.future_dropped),
            }
        }
    }

    async fn spawn_test_server(
        http2: bool,
        body: &'static [u8],
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(move |request: hyper::Request<Incoming>| async move {
                let version = match request.version() {
                    Version::HTTP_2 => "h2",
                    Version::HTTP_11 => "http/1.1",
                    _ => "other",
                };
                Ok::<_, Infallible>(
                    HyperResponse::builder()
                        .header("x-observed-protocol", version)
                        .body(Full::new(Bytes::from_static(body)))
                        .unwrap(),
                )
            });
            let io = TokioIo::new(stream);
            if http2 {
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .max_concurrent_streams(16)
                    .serve_connection(io, service)
                    .await;
            } else {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await;
            }
        });
        (format!("http://{address}/health"), task)
    }

    #[tokio::test]
    async fn connect_deadline_cancels_a_pending_inner_connector_future() {
        let future_dropped = Arc::new(AtomicBool::new(false));
        let inner = PendingConnector {
            future_dropped: Arc::clone(&future_dropped),
        };
        let mut connector = ConnectDeadlineConnector::new(inner, Duration::from_millis(20));
        let started = Instant::now();

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            connector.call(Uri::from_static("https://pending.example.test/")),
        )
        .await
        .expect("the connector's own deadline must complete the pending call")
        .unwrap_err();

        assert_eq!(error, ConnectDeadlineError::DeadlineElapsed);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            future_dropped.load(Ordering::SeqCst),
            "the deadline must drop, and therefore cancel, the in-flight connector future"
        );

        let mapped = map_transport_error(&WrappedConnectError(error));
        assert!(matches!(mapped, HttpClientError::ConnectTimeout));
        let ordinary =
            map_transport_error(&WrappedConnectError(ConnectDeadlineError::ConnectionFailed));
        assert!(
            matches!(ordinary, HttpClientError::Connection(message) if message == "transport request failed")
        );
    }

    #[tokio::test]
    async fn inner_tcp_timeout_and_outer_deadline_have_one_typed_outcome() {
        let mut connector =
            ConnectDeadlineConnector::new(InnerTimedOutConnector, Duration::from_secs(1));

        let error = connector
            .call(Uri::from_static("https://timed-out.example.test/"))
            .await
            .unwrap_err();

        assert_eq!(error, ConnectDeadlineError::DeadlineElapsed);
        let mapped = map_transport_error(&WrappedConnectError(error));
        assert!(matches!(mapped, HttpClientError::ConnectTimeout));
    }

    #[tokio::test]
    async fn divided_tcp_budget_attempts_a_second_address_within_the_total_deadline() {
        let total_deadline = Duration::from_millis(120);
        let uri = Uri::from_static("https://multi-address.example.test/");

        let successful_attempts = Arc::new(AtomicUsize::new(0));
        let inner = DeterministicAddressFailoverConnector {
            attempts: Arc::clone(&successful_attempts),
            tcp_failover_budget: total_deadline,
            second_address_succeeds: true,
        };
        let mut connector = ConnectDeadlineConnector::new(inner, total_deadline);
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(1), connector.call(uri.clone()))
            .await
            .expect("the total deadline must remain bounded")
            .expect("the healthy second address must connect");
        assert_eq!(successful_attempts.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() < total_deadline);

        let stalled_attempts = Arc::new(AtomicUsize::new(0));
        let inner = DeterministicAddressFailoverConnector {
            attempts: Arc::clone(&stalled_attempts),
            tcp_failover_budget: total_deadline,
            second_address_succeeds: false,
        };
        let mut connector = ConnectDeadlineConnector::new(inner, total_deadline);
        let started = Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(1), connector.call(uri))
            .await
            .expect("the authoritative total deadline must bound all address attempts")
            .unwrap_err();
        assert_eq!(error, ConnectDeadlineError::DeadlineElapsed);
        assert_eq!(stalled_attempts.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn one_viable_tcp_candidate_can_use_most_of_the_total_deadline() {
        let total_deadline = Duration::from_millis(200);
        let inner = DelayedSuccessConnector {
            delay: Duration::from_millis(150),
        };
        let mut connector = ConnectDeadlineConnector::new(inner, total_deadline);

        tokio::time::timeout(
            Duration::from_secs(1),
            connector.call(Uri::from_static("https://slow-viable.example.test/")),
        )
        .await
        .expect("the test itself must remain bounded")
        .expect("a viable candidate completing at 75% of D must not be cut off early");
    }

    #[test]
    fn production_connector_separates_tcp_failover_from_the_total_deadline() {
        let source = include_str!("mod.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source");

        assert_eq!(
            source
                .matches("http.set_connect_timeout(Some(settings.connect_timeout))")
                .count(),
            1,
            "hyper-util must divide the full TCP budget across resolved addresses"
        );
        assert_eq!(
            source
                .matches(
                    "http.set_happy_eyeballs_timeout(Some(settings.tcp_happy_eyeballs_delay))",
                )
                .count(),
            1,
            "mixed-family fallback must start within the authoritative total deadline"
        );
        assert_eq!(
            source
                .matches("ConnectDeadlineConnector::new(connector, settings.connect_timeout)")
                .count(),
            1,
            "one outer wrapper must own the authoritative full-establishment deadline"
        );
    }

    #[tokio::test]
    async fn request_connect_override_bounds_a_stalled_tls_handshake_and_reconciles_the_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (socket_closed_tx, socket_closed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = 0_usize;
            let mut buffer = [0_u8; 4096];
            loop {
                match stream.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(read) => received += read,
                    Err(_) => break,
                }
            }
            let _ = socket_closed_tx.send(received);
        });

        let client = HttpClientBuilder::new()
            .connect_timeout(Duration::from_secs(1))
            .request_timeout(Duration::from_secs(2))
            .try_build()
            .unwrap();
        let mut request = client.get(&format!("https://{address}/tls-stall")).unwrap();
        request
            .connect_timeout(Duration::from_millis(50))
            .timeout(Duration::from_secs(2));
        let request = request.build().unwrap();
        let started = Instant::now();

        let error = client.execute(request).await.unwrap_err();
        let elapsed = started.elapsed();

        assert!(matches!(error, HttpClientError::ConnectTimeout));
        assert_eq!(error.error_code(), "CONNECT_TIMEOUT");
        assert_eq!(error.diagnostic_code(), "CONNECT_DEADLINE");
        assert!(error.is_timeout());
        assert!(
            elapsed < Duration::from_secs(1),
            "the 50ms connect override must win over the 2s request deadline: {elapsed:?}"
        );
        let received = tokio::time::timeout(Duration::from_secs(1), socket_closed_rx)
            .await
            .expect("dropping the timed-out TLS handshake must close the socket")
            .expect("the stall server must report socket reconciliation");
        assert!(received > 0, "the server must observe a TLS ClientHello");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("the stall server task must terminate")
            .expect("the stall server task must not panic");
    }

    #[test]
    fn validates_supported_bounds_and_base_address() {
        let defaults = ClientConfig::default();
        assert_eq!(defaults.max_header_count(), DEFAULT_MAX_HEADER_COUNT);
        assert_eq!(defaults.max_retained_origins(), 64);
        assert!(defaults.validate().is_ok());
        assert!(ClientConfig::default()
            .with_base_address("https://api.example.test/root?token=invalid")
            .validate()
            .is_err());
        assert!(ClientConfig::default()
            .with_base_address("https://api.example.test/root#invalid")
            .validate()
            .is_err());
        let invalid_body_bound = ClientConfig {
            max_response_body_bytes: 0,
            ..ClientConfig::default()
        };
        assert!(invalid_body_bound.validate().is_err());
        let invalid_origin_bound = ClientConfig {
            max_in_flight_requests: 4,
            max_in_flight_requests_per_origin: 5,
            ..ClientConfig::default()
        };
        assert!(invalid_origin_bound.validate().is_err());
        let invalid_timeout = ClientConfig {
            request_timeout: MAX_REQUEST_TIMEOUT + Duration::from_secs(1),
            ..ClientConfig::default()
        };
        assert!(invalid_timeout.validate().is_err());
        let invalid_window = ClientConfig {
            http2_initial_stream_window_bytes: MIN_HTTP2_WINDOW_BYTES - 1,
            ..ClientConfig::default()
        };
        assert!(invalid_window.validate().is_err());

        assert!(ClientConfig::default()
            .with_max_header_bytes(MAX_HEADER_BYTES)
            .with_max_header_count(MAX_HEADER_COUNT)
            .validate()
            .is_ok());
        let invalid_header_bytes = ClientConfig::default()
            .with_max_header_bytes(MAX_HEADER_BYTES + 1)
            .validate()
            .unwrap_err();
        assert_eq!(
            invalid_header_bytes.diagnostic_code(),
            "CONFIG_MAX_HEADER_BYTES_RANGE"
        );
        let invalid_header_count = ClientConfig::default()
            .with_max_header_count(MAX_HEADER_COUNT + 1)
            .validate()
            .unwrap_err();
        assert_eq!(
            invalid_header_count.diagnostic_code(),
            "CONFIG_MAX_HEADER_COUNT_RANGE"
        );
    }

    #[test]
    fn client_config_accepts_inclusive_maxima_and_rejects_each_adjacent_value() {
        let gibibyte = 1024 * 1024 * 1024;

        for config in [
            ClientConfig {
                connect_timeout: MAX_CONNECT_TIMEOUT,
                ..ClientConfig::default()
            },
            ClientConfig {
                request_timeout: MAX_REQUEST_TIMEOUT,
                ..ClientConfig::default()
            },
            ClientConfig {
                pool_idle_timeout: MAX_POOL_IDLE_TIMEOUT,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_redirects: 20,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_in_flight_requests: 1_000_000,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_request_body_bytes: gibibyte,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_response_body_bytes: gibibyte,
                ..ClientConfig::default()
            },
            ClientConfig {
                pool_max_idle_per_host: 10_000,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_retained_origins: MAX_RETAINED_ORIGINS,
                ..ClientConfig::default()
            },
            ClientConfig {
                http2_keep_alive_interval: MAX_HTTP2_KEEP_ALIVE,
                ..ClientConfig::default()
            },
            ClientConfig {
                http2_keep_alive_timeout: MAX_HTTP2_KEEP_ALIVE,
                ..ClientConfig::default()
            },
        ] {
            assert!(config.validate().is_ok(), "inclusive maximum: {config:?}");
        }

        for config in [
            ClientConfig {
                connect_timeout: MAX_CONNECT_TIMEOUT + Duration::from_nanos(1),
                ..ClientConfig::default()
            },
            ClientConfig {
                request_timeout: MAX_REQUEST_TIMEOUT + Duration::from_nanos(1),
                ..ClientConfig::default()
            },
            ClientConfig {
                pool_idle_timeout: MAX_POOL_IDLE_TIMEOUT + Duration::from_nanos(1),
                ..ClientConfig::default()
            },
            ClientConfig {
                max_redirects: 21,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_in_flight_requests: 1_000_001,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_request_body_bytes: gibibyte + 1,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_response_body_bytes: gibibyte + 1,
                ..ClientConfig::default()
            },
            ClientConfig {
                pool_max_idle_per_host: 10_001,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_retained_origins: MAX_RETAINED_ORIGINS + 1,
                ..ClientConfig::default()
            },
            ClientConfig {
                http2_keep_alive_interval: MAX_HTTP2_KEEP_ALIVE + Duration::from_nanos(1),
                ..ClientConfig::default()
            },
            ClientConfig {
                http2_keep_alive_timeout: MAX_HTTP2_KEEP_ALIVE + Duration::from_nanos(1),
                ..ClientConfig::default()
            },
        ] {
            assert!(
                config.validate().is_err(),
                "value above maximum: {config:?}"
            );
        }
    }

    #[test]
    fn client_config_rejects_each_independent_zero_and_credential_condition() {
        for config in [
            ClientConfig {
                connect_timeout: Duration::ZERO,
                ..ClientConfig::default()
            },
            ClientConfig {
                request_timeout: Duration::ZERO,
                ..ClientConfig::default()
            },
            ClientConfig {
                pool_idle_timeout: Duration::ZERO,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_in_flight_requests: 0,
                ..ClientConfig::default()
            },
            ClientConfig {
                pool_max_idle_per_host: 0,
                ..ClientConfig::default()
            },
            ClientConfig {
                max_retained_origins: 0,
                ..ClientConfig::default()
            },
            ClientConfig {
                http2_keep_alive_interval: Duration::ZERO,
                ..ClientConfig::default()
            },
            ClientConfig {
                http2_keep_alive_timeout: Duration::ZERO,
                ..ClientConfig::default()
            },
        ] {
            assert!(config.validate().is_err(), "independent zero: {config:?}");
        }

        for address in [
            "https://user@api.example.test/root",
            "https://:password@api.example.test/root",
        ] {
            assert!(ClientConfig::default()
                .with_base_address(address)
                .validate()
                .is_err());
        }
    }

    #[test]
    fn retained_origin_registry_evicts_only_idle_lru_entries_and_fails_closed_at_capacity() {
        let config = ClientConfig::default().with_max_retained_origins(2);
        let config = Arc::new(config);
        let roots = Arc::new(TransportClients::public_root_store());
        let registry = Arc::new(RetainedOriginRegistry::new(config, Arc::clone(&roots)));
        let first =
            OriginKey::from_url(&Url::parse("https://first.example.test").unwrap()).unwrap();
        let second =
            OriginKey::from_url(&Url::parse("https://second.example.test").unwrap()).unwrap();
        let third =
            OriginKey::from_url(&Url::parse("https://third.example.test").unwrap()).unwrap();

        let first_lease = registry.acquire(first.clone()).unwrap();
        let second_lease = registry.acquire(second.clone()).unwrap();
        assert_eq!(registry.retained_count(), 2);
        let saturated = match registry.acquire(third.clone()) {
            Ok(_) => panic!("an all-active registry must reject a new origin"),
            Err(error) => error,
        };
        assert!(matches!(
            saturated,
            HttpClientError::OriginCapacityExceeded { limit: 2 }
        ));
        assert_eq!(saturated.error_code(), "ORIGIN_CAPACITY_EXCEEDED");
        assert_eq!(saturated.diagnostic_code(), "ORIGIN_CAPACITY_ACTIVE");

        drop(first_lease);
        let third_lease = registry.acquire(third.clone()).unwrap();
        assert_eq!(registry.retained_count(), 2);
        let state = registry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(!state.origins.contains_key(&first));
        assert!(state.origins.contains_key(&second));
        assert!(state.origins.contains_key(&third));
        drop(state);

        assert!(third_lease.entry.transports.automatic.get().is_none());
        let _ = third_lease
            .entry
            .transports
            .select(ProtocolPreference::Auto);
        assert!(third_lease.entry.transports.automatic.get().is_some());
        assert!(third_lease.entry.transports.http1.get().is_none());

        drop(second_lease);
        drop(third_lease);

        drop(registry.acquire(second.clone()).unwrap());
        drop(registry.acquire(first.clone()).unwrap());
        let state = registry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(state.origins.contains_key(&first));
        assert!(state.origins.contains_key(&second));
        assert!(!state.origins.contains_key(&third));
    }

    #[test]
    fn validated_factory_profile_preserves_the_exact_client_snapshot() {
        let config = ClientConfig::default()
            .with_protocol(ProtocolPreference::Http2Only)
            .with_max_retained_origins(17);
        let profile = ValidatedClientProfile::new(config, None).unwrap();

        assert_eq!(profile.config().protocol(), ProtocolPreference::Http2Only);
        assert_eq!(profile.config().max_retained_origins(), 17);
        let client = profile.build_client().unwrap();
        assert_eq!(client.config().protocol(), ProtocolPreference::Http2Only);
        assert_eq!(client.config().max_retained_origins(), 17);
    }

    #[test]
    fn safe_config_debug_output_is_structural_and_redacts_the_base_address() {
        const SECRET: &str = "LILY_SECRET_CLIENT_BASE_ADDRESS";
        let config =
            ClientConfig::default().with_base_address(format!("https://api.example.test/{SECRET}"));
        let output = format!("{config:?}");

        assert!(output.starts_with("ClientConfig"));
        assert!(output.contains("base_address: Some(\"[REDACTED]\")"));
        assert!(output.contains("max_response_body_bytes"));
        assert!(!output.contains(SECRET));
    }

    #[test]
    fn safe_builder_debug_output_reports_structure_without_trust_roots() {
        let output = format!("{:?}", HttpClientBuilder::new());

        assert!(output.starts_with("HttpClientBuilder"));
        assert!(output.contains("config: ClientConfig"));
        assert!(output.contains("additional_ca_certificates: 0"));
        assert!(!output.contains("trust_roots"));
    }

    #[test]
    fn validates_default_headers_before_transport_construction() {
        let too_many = ClientConfig::default()
            .try_with_default_header("x-contract", "enabled")
            .unwrap()
            .with_max_header_count(1);
        assert!(matches!(
            too_many.validate(),
            Err(HttpClientError::LimitExceeded { resource, .. })
                if resource == "request header count"
        ));

        let too_large = ClientConfig::default().with_max_header_bytes(1);
        assert!(matches!(
            too_large.validate(),
            Err(HttpClientError::LimitExceeded { resource, .. })
                if resource == "request headers"
        ));

        for name in [
            "connection",
            "keep-alive",
            "proxy-connection",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
            "content-length",
            "host",
        ] {
            let mut config = ClientConfig::default();
            config.default_headers.insert(name, "configured").unwrap();
            assert!(matches!(
                config.validate(),
                Err(HttpClientError::Configuration(_))
            ));
            assert!(matches!(
                ClientConfig::default().try_with_default_header(name, "configured"),
                Err(HttpClientError::Configuration(_))
            ));
        }
    }

    #[test]
    fn default_custom_header_reaches_the_wire_request() {
        let client = HttpClient::try_with_config(
            ClientConfig::default()
                .try_with_default_header("x-contract", "enabled")
                .unwrap(),
        )
        .unwrap();
        let request = client
            .get("https://api.example.test/health")
            .unwrap()
            .build()
            .unwrap();
        let wire = HttpClient::build_request(
            request.method(),
            request.url(),
            request.headers(),
            Bytes::new(),
        )
        .unwrap();

        assert_eq!(wire.headers().get("x-contract").unwrap(), "enabled");
    }

    #[test]
    fn custom_extension_method_case_reaches_the_wire_unchanged() {
        let request = {
            let mut builder = RequestBuilder::new();
            builder
                .method(Method::from("post"))
                .url("https://api.example.test/resource")
                .unwrap();
            builder.build().unwrap()
        };
        let wire = HttpClient::build_request(
            request.method(),
            request.url(),
            request.headers(),
            Bytes::new(),
        )
        .unwrap();

        assert_eq!(request.method().as_str(), "post");
        assert_eq!(wire.method().as_str(), "post");
    }

    #[test]
    fn base_address_resolves_relative_query_fragment_and_absolute_urls() {
        let client = HttpClient::try_with_config(
            ClientConfig::default().with_base_address("https://api.example.test/v1"),
        )
        .unwrap();

        let relative = client
            .get("orders?state=open#result")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            relative.url().as_str(),
            "https://api.example.test/v1/orders?state=open#result"
        );

        let leading_slash = client.get("/orders").unwrap().build().unwrap();
        assert_eq!(
            leading_slash.url().as_str(),
            "https://api.example.test/v1/orders"
        );

        let query_and_fragment = client.get("?page=2#result").unwrap().build().unwrap();
        assert_eq!(
            query_and_fragment.url().as_str(),
            "https://api.example.test/v1/?page=2#result"
        );

        let absolute = client
            .get("HTTPS://override.example.test/orders?state=closed#result")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            absolute.url().as_str(),
            "https://override.example.test/orders?state=closed#result"
        );

        let wire = HttpClient::build_request(
            query_and_fragment.method(),
            query_and_fragment.url(),
            query_and_fragment.headers(),
            Bytes::new(),
        )
        .unwrap();
        assert_eq!(
            wire.uri().to_string(),
            "https://api.example.test/v1/?page=2"
        );

        assert!(matches!(
            HttpClient::new().get("relative-only"),
            Err(HttpClientError::InvalidUrl(_))
        ));
    }

    #[test]
    fn base_scoped_default_headers_never_cross_origin_but_explicit_headers_can() {
        const SENTINEL: &str = "LILY_SECRET_BASE_SCOPED_API_KEY";
        const EXPLICIT: &str = "request-owned-value";

        let config = ClientConfig::default()
            .with_base_address("https://api.example.test/root")
            .try_with_default_header("X-Api-Key", SENTINEL)
            .unwrap();
        let client = HttpClient::try_with_config(config).unwrap();

        for target in [
            "/relative",
            "https://api.example.test/absolute",
            "https://api.example.test:443/explicit-default-port",
        ] {
            let request = client.get(target).unwrap().build().unwrap();
            assert_eq!(request.headers().get("X-Api-Key"), Some(SENTINEL));
            assert!(request.headers().get("User-Agent").is_some());
        }

        let mut same_origin_retarget = client.get("/initial").unwrap();
        same_origin_retarget
            .url("https://api.example.test/final")
            .unwrap();
        let same_origin_retarget = same_origin_retarget.build().unwrap();
        assert_eq!(
            same_origin_retarget.headers().get("X-Api-Key"),
            Some(SENTINEL)
        );

        let mut same_origin_explicit = client.get("/initial").unwrap();
        same_origin_explicit.header("x-api-key", EXPLICIT).unwrap();
        let same_origin_explicit = same_origin_explicit.build().unwrap();
        assert_eq!(
            same_origin_explicit.headers().get("X-Api-Key"),
            Some(EXPLICIT),
            "an explicit header must override a same-name applicable default"
        );

        let cross_origin = client.get("https://evil.example.test/collect").unwrap();
        let automatic = cross_origin.build().unwrap();
        assert!(automatic.headers().is_empty());

        let mut string_retarget = client.get("/initial").unwrap();
        string_retarget
            .url("https://evil.example.test/string-retarget")
            .unwrap();
        let string_retarget = string_retarget.build().unwrap();
        assert!(string_retarget.headers().get("X-Api-Key").is_none());
        assert!(string_retarget.headers().get("User-Agent").is_none());

        let mut parsed_retarget = client.get("/initial").unwrap();
        parsed_retarget
            .url_parsed(Url::parse("https://evil.example.test/parsed-retarget").unwrap())
            .unwrap();
        let parsed_retarget = parsed_retarget.build().unwrap();
        assert!(parsed_retarget.headers().get("X-Api-Key").is_none());
        assert!(parsed_retarget.headers().get("User-Agent").is_none());

        let mut cross_origin_body = client.post("/initial").unwrap();
        cross_origin_body
            .url("https://evil.example.test/body-retarget")
            .unwrap()
            .json_str("{}");
        let cross_origin_body = cross_origin_body.build().unwrap();
        assert!(cross_origin_body.headers().get("X-Api-Key").is_none());
        assert_eq!(
            cross_origin_body.headers().get("Content-Type"),
            Some("application/json; charset=utf-8"),
            "body-derived Content-Type must still be inferred after scoped defaults are dropped"
        );

        let mut explicit = client.get("/initial").unwrap();
        explicit.header("X-Api-Key", EXPLICIT).unwrap();
        explicit
            .url("https://evil.example.test/explicit-retarget")
            .unwrap();
        let explicit = explicit.build().unwrap();
        assert_eq!(explicit.headers().get("X-Api-Key"), Some(EXPLICIT));
        assert!(explicit.headers().get("User-Agent").is_none());

        let global = HttpClient::try_with_config(
            ClientConfig::default()
                .try_with_default_header("X-Api-Key", SENTINEL)
                .unwrap(),
        )
        .unwrap();
        let global_request = global
            .get("https://evil.example.test/collect")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(global_request.headers().get("X-Api-Key"), Some(SENTINEL));

        let mut global_retarget = global.get("https://first.example.test/initial").unwrap();
        global_retarget
            .url("https://evil.example.test/global-retarget")
            .unwrap();
        let global_retarget = global_retarget.build().unwrap();
        assert_eq!(global_retarget.headers().get("X-Api-Key"), Some(SENTINEL));
    }

    #[test]
    fn relative_url_cannot_escape_the_base_origin_with_mixed_separators() {
        let base = Some("https://api.example.test/v1");

        for reference in [
            r"\evil.example/path",
            r"\\evil.example/path",
            r"/\evil.example/path",
            r"\/evil.example/path",
            "//evil.example/path",
        ] {
            assert!(matches!(
                resolve_request_url(base, reference),
                Err(HttpClientError::InvalidUrl(_))
            ));
        }

        let leading_slash = resolve_request_url(base, "/orders").unwrap();
        assert_eq!(leading_slash.host_str(), Some("api.example.test"));
        assert_eq!(leading_slash.path(), "/v1/orders");
    }

    #[test]
    fn client_request_construction_preserves_typed_secret_safe_url_failures() {
        const SECRET: &str = "LILY_SECRET_CLIENT_URL_PASSWORD";
        let client = HttpClient::try_with_config(
            ClientConfig::default().with_base_address("https://api.example.test/v1"),
        )
        .unwrap();

        let cases = [
            (
                format!("https://user:{SECRET}@example.test/resource"),
                "REQUEST_URL_CREDENTIALS",
            ),
            (
                "ftp://example.test/resource".to_string(),
                "REQUEST_URL_SCHEME_UNSUPPORTED",
            ),
            ("https://[invalid".to_string(), "REQUEST_URL_INVALID"),
            (
                "//evil.example.test/resource".to_string(),
                "REQUEST_RELATIVE_URL_AMBIGUOUS_AUTHORITY",
            ),
        ];

        for (value, diagnostic) in cases {
            let error = client.get(&value).unwrap_err();
            assert!(matches!(error, HttpClientError::InvalidUrl(_)));
            assert_eq!(error.diagnostic_code(), diagnostic);
            assert!(!format!("{error:?}").contains(SECRET));
            assert!(!error.to_string().contains(SECRET));
        }
    }

    #[test]
    fn every_supported_client_config_field_has_a_typed_construction_path() {
        let config = ClientConfig::default()
            .with_base_address("https://api.example.test/root")
            .with_connect_timeout(Duration::from_secs(7))
            .with_request_timeout(Duration::from_secs(17))
            .with_max_redirects(3)
            .with_protocol(ProtocolPreference::Http1Only)
            .with_max_in_flight_requests(64)
            .with_max_in_flight_requests_per_origin(8)
            .with_max_request_body_bytes(1024)
            .with_max_response_body_bytes(2048)
            .with_max_header_count(32)
            .with_max_header_bytes(4096)
            .with_pool_idle_timeout(Duration::from_secs(45))
            .with_pool_max_idle_per_host(5)
            .with_max_retained_origins(9)
            .with_http2_initial_stream_window_bytes(64 * 1024)
            .with_http2_initial_connection_window_bytes(128 * 1024)
            .with_http2_max_frame_bytes(32 * 1024)
            .with_http2_keep_alive_interval(Duration::from_secs(20))
            .with_http2_keep_alive_timeout(Duration::from_secs(5))
            .with_retry_unstarted_requests(false)
            .try_with_user_agent("contract-test/1.0")
            .unwrap()
            .try_with_default_header("X-Contract", "enabled")
            .unwrap();

        assert_eq!(config.base_address(), Some("https://api.example.test/root"));
        assert_eq!(config.connect_timeout(), Duration::from_secs(7));
        assert_eq!(config.request_timeout(), Duration::from_secs(17));
        assert_eq!(config.max_redirects(), 3);
        assert_eq!(config.protocol(), ProtocolPreference::Http1Only);
        assert_eq!(config.max_in_flight_requests(), 64);
        assert_eq!(config.max_in_flight_requests_per_origin(), 8);
        assert_eq!(config.max_request_body_bytes(), 1024);
        assert_eq!(config.max_response_body_bytes(), 2048);
        assert_eq!(config.max_header_count(), 32);
        assert_eq!(config.max_header_bytes(), 4096);
        assert_eq!(config.pool_idle_timeout(), Duration::from_secs(45));
        assert_eq!(config.pool_max_idle_per_host(), 5);
        assert_eq!(config.max_retained_origins(), 9);
        assert_eq!(config.http2_initial_stream_window_bytes(), 64 * 1024);
        assert_eq!(config.http2_initial_connection_window_bytes(), 128 * 1024);
        assert_eq!(config.http2_max_frame_bytes(), 32 * 1024);
        assert_eq!(config.http2_keep_alive_interval(), Duration::from_secs(20));
        assert_eq!(config.http2_keep_alive_timeout(), Duration::from_secs(5));
        assert!(!config.retries_unstarted_requests());
        assert_eq!(
            config.default_headers().get("User-Agent"),
            Some("contract-test/1.0")
        );
        assert_eq!(config.default_headers().get("X-Contract"), Some("enabled"));
        assert!(HttpClient::try_with_config(config).is_ok());
    }

    #[test]
    fn request_policy_resolves_client_defaults_and_bounded_overrides() {
        let client = ClientConfig::default()
            .with_connect_timeout(Duration::from_secs(7))
            .with_request_timeout(Duration::from_secs(17))
            .with_max_redirects(4)
            .with_protocol(ProtocolPreference::Http1Only);

        assert_eq!(
            ResolvedRequestPolicy::from_configs(&client, &RequestConfig::default()),
            ResolvedRequestPolicy {
                request_timeout: Duration::from_secs(17),
                connect_timeout: Duration::from_secs(7),
                max_redirects: 4,
                follow_redirects: true,
                protocol: ProtocolPreference::Http1Only,
            }
        );

        let request = RequestConfig::new()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            .max_redirects(2)
            .follow_redirects(false)
            .protocol(ProtocolPreference::Http2Only);
        let overridden = ResolvedRequestPolicy::from_configs(&client, &request);
        assert_eq!(
            overridden,
            ResolvedRequestPolicy {
                request_timeout: Duration::from_secs(5),
                connect_timeout: Duration::from_secs(3),
                max_redirects: 2,
                follow_redirects: false,
                protocol: ProtocolPreference::Http2Only,
            }
        );
        assert!(!overridden.should_follow_redirect(http::StatusCode::FOUND));

        let request_above_client_cap = RequestConfig::new().max_redirects(10);
        let capped = ResolvedRequestPolicy::from_configs(&client, &request_above_client_cap);
        assert_eq!(capped.max_redirects, 4);
        for status in [
            http::StatusCode::MOVED_PERMANENTLY,
            http::StatusCode::FOUND,
            http::StatusCode::SEE_OTHER,
            http::StatusCode::TEMPORARY_REDIRECT,
            http::StatusCode::PERMANENT_REDIRECT,
        ] {
            assert!(capped.should_follow_redirect(status), "status {status}");
        }
        for status in [
            http::StatusCode::MULTIPLE_CHOICES,
            http::StatusCode::NOT_MODIFIED,
            http::StatusCode::from_u16(305).unwrap(),
            http::StatusCode::from_u16(306).unwrap(),
            http::StatusCode::from_u16(399).unwrap(),
            http::StatusCode::OK,
        ] {
            assert!(!capped.should_follow_redirect(status), "status {status}");
        }
        assert!(!capped.redirect_limit_reached(3));
        assert!(capped.redirect_limit_reached(4));

        let disabled_by_zero_limit = ResolvedRequestPolicy {
            max_redirects: 0,
            follow_redirects: true,
            ..capped
        };
        assert!(!disabled_by_zero_limit.should_follow_redirect(http::StatusCode::FOUND));
    }

    #[test]
    fn transport_build_settings_snapshot_covers_every_opaque_upstream_setting() {
        let config = ClientConfig::default()
            .with_max_in_flight_requests(64)
            .with_max_in_flight_requests_per_origin(8)
            .with_max_header_count(77)
            .with_max_header_bytes(4096)
            .with_pool_idle_timeout(Duration::from_secs(45))
            .with_pool_max_idle_per_host(5)
            .with_http2_initial_stream_window_bytes(64 * 1024)
            .with_http2_initial_connection_window_bytes(128 * 1024)
            .with_http2_max_frame_bytes(32 * 1024)
            .with_http2_keep_alive_interval(Duration::from_secs(20))
            .with_http2_keep_alive_timeout(Duration::from_secs(5))
            .with_retry_unstarted_requests(false);
        config.validate().unwrap();

        assert_eq!(
            TransportBuildSettings::from_config(
                &config,
                Duration::from_secs(3),
                ProtocolPreference::Http2Only,
            ),
            TransportBuildSettings {
                connect_timeout: Duration::from_secs(3),
                tcp_happy_eyeballs_delay: Duration::from_millis(300),
                protocol: ProtocolPreference::Http2Only,
                pool_idle_timeout: Duration::from_secs(45),
                pool_max_idle_per_host: 5,
                http1_max_headers: 77,
                http1_max_buffer_bytes: 8192 + 4096 + 77 * 4 + 2,
                http2_initial_stream_window_bytes: 64 * 1024,
                http2_initial_connection_window_bytes: 128 * 1024,
                http2_max_frame_bytes: 32 * 1024,
                http2_max_header_list_bytes: 4096 + 77 * 32 + 32 + 7 + 3 + 1,
                http2_initial_max_send_streams: 8,
                http2_keep_alive_interval: Duration::from_secs(20),
                http2_keep_alive_timeout: Duration::from_secs(5),
                retry_unstarted_requests: false,
            }
        );
    }

    #[test]
    fn short_connect_deadlines_bound_the_mixed_family_fallback_delay() {
        let settings = TransportBuildSettings::from_config(
            &ClientConfig::default(),
            Duration::from_millis(50),
            ProtocolPreference::Auto,
        );
        assert_eq!(
            settings.tcp_happy_eyeballs_delay,
            Duration::from_micros(12_500)
        );

        let smallest = TransportBuildSettings::from_config(
            &ClientConfig::default(),
            Duration::from_nanos(1),
            ProtocolPreference::Auto,
        );
        assert_eq!(smallest.tcp_happy_eyeballs_delay, Duration::ZERO);
    }

    #[test]
    fn http1_buffer_limit_accounts_for_wire_framing_without_panicking_below_hyper_minimum() {
        assert_eq!(http1_max_buffer_bytes(1, 1), 8192 + 1 + 4 + 2);
        assert_eq!(http1_max_buffer_bytes(4096, 77), 8192 + 4096 + 77 * 4 + 2);
        assert_eq!(http1_max_buffer_bytes(usize::MAX, 4096), usize::MAX);
    }

    #[test]
    fn http2_upstream_limit_accounts_for_field_and_status_overhead() {
        assert_eq!(http2_max_header_list_bytes(64, 2), 64 + 2 * 32 + 43);
        assert_eq!(
            http2_max_header_list_bytes(MAX_HEADER_BYTES, MAX_HEADER_COUNT),
            (MAX_HEADER_BYTES + MAX_HEADER_COUNT * 32 + 43) as u32
        );
        assert_eq!(http2_max_header_list_bytes(usize::MAX, 1), u32::MAX);
    }

    #[test]
    fn validated_admission_limits_reach_the_constructed_client() {
        let client = HttpClient::try_with_config(
            ClientConfig::default()
                .with_max_in_flight_requests(3)
                .with_max_in_flight_requests_per_origin(2),
        )
        .unwrap();

        assert_eq!(client.admission.available_permits(), 3);
        assert_eq!(client.per_origin_admission.permits, 2);

        let origin =
            OriginKey::from_url(&Url::parse("https://admission.example.test/resource").unwrap())
                .unwrap();
        assert_eq!(
            client
                .per_origin_admission
                .semaphore(origin)
                .available_permits(),
            2
        );
    }

    #[tokio::test]
    async fn client_and_request_deadlines_cover_admission_without_opening_a_socket() {
        async fn assert_admission_timeout(client: &HttpClient, request_timeout: Option<Duration>) {
            let held = client.admission.clone().acquire_owned().await.unwrap();
            let mut request = client
                .get("https://deadline.example.test/resource")
                .unwrap();
            if let Some(timeout) = request_timeout {
                request.timeout(timeout);
            }
            let request = request.build().unwrap();

            let result = tokio::time::timeout(Duration::from_secs(1), client.execute(request))
                .await
                .expect("the configured request deadline must bound admission waiting");
            assert!(matches!(result, Err(HttpClientError::Timeout)));
            drop(held);
        }

        let client_default = HttpClient::try_with_config(
            ClientConfig::default()
                .with_request_timeout(Duration::from_millis(15))
                .with_max_in_flight_requests(1)
                .with_max_in_flight_requests_per_origin(1),
        )
        .unwrap();
        assert_admission_timeout(&client_default, None).await;

        let request_override = HttpClient::try_with_config(
            ClientConfig::default()
                .with_request_timeout(Duration::from_secs(5))
                .with_max_in_flight_requests(1)
                .with_max_in_flight_requests_per_origin(1),
        )
        .unwrap();
        assert_admission_timeout(&request_override, Some(Duration::from_millis(15))).await;
    }

    #[test]
    fn request_defaults_inherit_the_validated_client_policy() {
        let client = HttpClientBuilder::new()
            .connect_timeout(Duration::from_secs(7))
            .request_timeout(Duration::from_secs(17))
            .max_redirects(3)
            .protocol(ProtocolPreference::Http1Only)
            .try_build()
            .unwrap();
        let request = client
            .get("https://api.example.test/health")
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(request.config().connect_timeout_override(), None);
        assert_eq!(request.config().timeout_override(), None);
        assert_eq!(request.config().max_redirects_override(), None);
        assert_eq!(request.config().protocol_preference(), None);
        assert!(request.config().follows_redirects());
        assert_eq!(client.config().connect_timeout(), Duration::from_secs(7));
        assert_eq!(client.config().request_timeout(), Duration::from_secs(17));
        assert_eq!(client.config().max_redirects(), 3);
        assert_eq!(client.config().protocol(), ProtocolPreference::Http1Only);
    }

    #[test]
    fn same_origin_compares_scheme_host_and_effective_port() {
        let left = Url::parse("https://api.example.test/a").unwrap();
        let same = Url::parse("https://api.example.test/b").unwrap();
        let other = Url::parse("https://login.example.test/b").unwrap();
        let other_scheme = Url::parse("http://api.example.test/b").unwrap();
        let other_port = Url::parse("https://api.example.test:8443/b").unwrap();
        assert!(same_origin(&left, &same));
        assert!(!same_origin(&left, &other));
        assert!(!same_origin(&left, &other_scheme));
        assert!(!same_origin(&left, &other_port));
    }

    #[test]
    fn redirects_are_same_origin_and_cannot_downgrade_or_embed_credentials() {
        let source = Url::parse("https://api.example.test/a").unwrap();
        let safe = Url::parse("https://api.example.test/b").unwrap();
        let cross_origin = Url::parse("https://login.example.test/b").unwrap();
        let plaintext = Url::parse("http://api.example.test/b").unwrap();
        let username_only = Url::parse("https://user@api.example.test/b").unwrap();
        let password_only = Url::parse("https://:secret@api.example.test/b").unwrap();

        assert!(validate_redirect_target(&source, &safe).is_ok());
        for (target, diagnostic) in [
            (&cross_origin, "REDIRECT_CROSS_ORIGIN"),
            (&plaintext, "REDIRECT_TLS_DOWNGRADE"),
            (&username_only, "REDIRECT_URL_CREDENTIALS"),
            (&password_only, "REDIRECT_URL_CREDENTIALS"),
        ] {
            let error = validate_redirect_target(&source, target).unwrap_err();
            assert!(matches!(&error, HttpClientError::RedirectRejected(_)));
            assert_eq!(error.diagnostic_code(), diagnostic);
        }
    }

    #[test]
    fn canonical_client_span_never_declares_uri_payload_or_error_message_fields() {
        let source = include_str!("mod.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source");
        for declaration in [
            "url.full =",
            "url.path =",
            "http.request.body =",
            "http.response.body =",
            "otel.status_message =",
        ] {
            assert!(
                !source.contains(declaration),
                "forbidden field: {declaration}"
            );
        }
        assert!(source.contains("\"http.client.request\""));
        assert!(source.contains("lily.error_code"));
    }

    #[tokio::test]
    #[ignore = "requires loopback sockets; v1-transport CI runs ignored wire tests"]
    async fn executes_a_real_http_1_1_exchange() {
        let (url, server) = spawn_test_server(false, b"http-one").await;
        let client = HttpClientBuilder::new()
            .protocol(ProtocolPreference::Http1Only)
            .try_build()
            .unwrap();
        let request = client.get(&url).unwrap().build().unwrap();
        let mut response = client.execute(request).await.unwrap();

        assert_eq!(response.version(), HttpVersion::Http11);
        assert_eq!(
            response.headers().get("x-observed-protocol"),
            Some("http/1.1")
        );
        assert_eq!(response.bytes().await.unwrap(), b"http-one");
        drop(client);
        server.abort();
    }

    #[tokio::test]
    #[ignore = "requires loopback sockets; v1-transport CI runs ignored wire tests"]
    async fn executes_a_real_h2c_exchange_with_flow_control() {
        let (url, server) = spawn_test_server(true, b"http-two").await;
        let client = HttpClientBuilder::new()
            .protocol(ProtocolPreference::Http2Only)
            .try_build()
            .unwrap();
        let request = client.get(&url).unwrap().build().unwrap();
        let mut response = client.execute(request).await.unwrap();

        assert_eq!(response.version(), HttpVersion::Http2);
        assert_eq!(response.headers().get("x-observed-protocol"), Some("h2"));
        assert_eq!(response.bytes().await.unwrap(), b"http-two");
        drop(client);
        server.abort();
    }

    #[tokio::test]
    #[ignore = "requires loopback sockets; v1-transport CI runs ignored wire tests"]
    async fn rejects_a_streamed_response_above_the_configured_limit() {
        let (url, server) = spawn_test_server(true, b"too-large").await;
        let client = HttpClientBuilder::new()
            .protocol(ProtocolPreference::Http2Only)
            .max_response_body_bytes(4)
            .try_build()
            .unwrap();
        let request = client.get(&url).unwrap().build().unwrap();
        let error = client.execute(request).await.unwrap_err();

        assert!(matches!(error, HttpClientError::LimitExceeded { .. }));
        drop(client);
        server.abort();
    }
}

#[cfg(test)]
mod http1_conformance_tests;

#[cfg(test)]
mod h2_socketless_tests;

#[cfg(test)]
mod tls_socketless_tests;
