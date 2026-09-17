use std::{
    collections::HashMap,
    convert::Infallible,
    error::Error as StdError,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use super::response_control::{
    ConnectionReceipt, ResponseFrames, ResponseStopped, ResponseTransportControl, TransportTally,
    CONNECTION_TASK, PROTOCOL_TASK,
};
use crate::app::{App, AppCallOutcome};
use crate::lifecycle::ExecutionStopReason;
use crate::request_lifecycle::{
    ExecutionInterrupted, RequestAdmissionError, RequestExecutionContext, RequestRegistry,
};
use crate::shutdown::{ShutdownBudget, ShutdownStage};
use crate::tasks::{TaskReceipt, TaskRegistry, TaskResult, TaskSet};

use bytes::Bytes;
#[cfg(test)]
use bytes::BytesMut;
use http::{HeaderName, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Frame, Incoming, SizeHint},
    service::{service_fn, Service},
    Request as HyperRequest, Response as HyperResponse, Version,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto,
    service::TowerToHyperService,
};
use lily_core::enums::HttpProtocol;
use lily_core::structs::RawHeader;
use lily_error::application::http_api::HttpApiError;
use lily_middleware::__private::{
    CorsLayerAdapter, CorsRequestKind, CorsRuntimeRejection, CorsService,
};
use lily_web_core::{
    debug_log, BodyBudget, Request, RequestBodyError, RequestBodyStream, RequestConnectionInfo,
    Response, ResponseBodyError, ResponseBodyStream, ResponseLimits, ResponseStreamingLimits,
    RustlsConfig, TransportResponseBody,
};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, UpDownCounter},
    propagation::Extractor,
    KeyValue,
};
#[cfg(test)]
use std::collections::HashSet;

use tokio::sync::{Notify, Semaphore};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tokio_util::sync::CancellationToken;
use tower_service::Service as TowerService;
use tracing::Instrument;

/// this is the generic type http server
///
pub struct HttpServer(pub Arc<App>);

#[derive(Clone)]
pub(super) struct TrackedHttpExecutor {
    local: TaskRegistry,
    parent: TaskRegistry,
    keep_alive: Arc<App>,
}

impl<F> hyper::rt::Executor<F> for TrackedHttpExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        // The two inventories retain the same actual join before first poll.
        // A sealed producer rejects unstarted work without creating a task.
        let keep_alive = self.keep_alive.clone();
        let _ = self.local.try_spawn_with_receipt(
            move |receipt| async move {
                // A body worker may have already dropped its service future. Keep
                // the application's dependency owners alive until the worker's
                // future (including its captures/destructors) is actually released.
                let _keep_alive = keep_alive;
                PROTOCOL_TASK.scope(receipt, future).await;
            },
            Some(&self.parent),
        );
    }
}

struct ConnectionProtocolOwner {
    tasks: TaskRegistry,
}

impl Drop for ConnectionProtocolOwner {
    fn drop(&mut self) {
        self.tasks.seal();
        self.tasks.abort_all();
    }
}

/// CORS-enabled Hyper/Tower bridge. The zero-CORS path continues to use
/// Hyper's `service_fn` directly and never constructs this service.
#[derive(Clone)]
struct CorsTransportService {
    runtime: HttpRequestRuntime,
    activity: Arc<ConnectionActivity>,
}

struct HttpConnectionRuntime {
    service: Arc<App>,
    transport: HttpTransportConfig,
    request_admission: Arc<Semaphore>,
    telemetry: Arc<HttpServerTelemetry>,
    cors_services: Option<SharedCorsDispatchServices>,
}

#[derive(Clone)]
struct HttpRequestRuntime {
    service: Arc<App>,
    transport: Arc<HttpTransportConfig>,
    request_admission: Arc<Semaphore>,
    peer_ip: Option<IpAddr>,
    telemetry: Arc<HttpServerTelemetry>,
    cors_services: Option<SharedCorsDispatchServices>,
}

impl TowerService<HyperRequest<Incoming>> for CorsTransportService {
    type Response = HyperResponse<BoundedResponseBody>;
    type Error = ResponseStopped;
    type Future = Pin<
        Box<
            dyn Future<Output = Result<HyperResponse<BoundedResponseBody>, ResponseStopped>>
                + Send
                + 'static,
        >,
    >;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HyperRequest<Incoming>) -> Self::Future {
        let request_guard = self.activity.enter();
        Box::pin(HttpServer::serve_request::<true>(
            request,
            self.runtime.clone(),
            request_guard,
        ))
    }
}

type SharedCorsDispatchService = Arc<Mutex<CorsService<CorsDispatchService>>>;
type SharedCorsDispatchServices = Arc<Vec<SharedCorsDispatchService>>;

#[derive(Clone)]
struct CorsDispatchContext {
    peer_ip: Option<IpAddr>,
    observed_request_bytes: Option<Arc<AtomicU64>>,
    owner: RequestExecutionContext,
}

#[derive(Clone)]
struct CorsDispatchService {
    service: Arc<App>,
    transport: Arc<HttpTransportConfig>,
}

impl TowerService<HyperRequest<Incoming>> for CorsDispatchService {
    type Response = HyperResponse<TransportResponseBody>;
    type Error = HttpApiError;
    type Future = Pin<
        Box<
            dyn Future<Output = Result<HyperResponse<TransportResponseBody>, HttpApiError>>
                + Send
                + 'static,
        >,
    >;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: HyperRequest<Incoming>) -> Self::Future {
        let service = Arc::clone(&self.service);
        let transport = self.transport.clone();
        let context = request.extensions_mut().remove::<CorsDispatchContext>();
        Box::pin(async move {
            let context = context.ok_or_else(|| {
                HttpApiError::StateError("CORS dispatch has no retained request owner".into())
            })?;
            HttpServer::dispatch_request(
                request,
                service,
                &transport,
                context.peer_ip,
                context.observed_request_bytes,
                context.owner,
            )
            .await
        })
    }
}

#[cfg(test)]
struct RequestTraceExtractor<'a>(&'a Request);

#[cfg(test)]
impl Extractor for RequestTraceExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.header_value(key)
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .headers()
            .iter()
            .map(|header| header.name.as_str())
            .collect()
    }
}

struct HyperRequestTraceExtractor<'a>(&'a HyperRequest<Incoming>);

impl Extractor for HyperRequestTraceExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .headers()
            .get(key)
            .and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.headers().keys().map(HeaderName::as_str).collect()
    }
}

#[derive(Debug, Clone)]
struct HttpServerTelemetry {
    requests: Counter<u64>,
    request_in_flight: UpDownCounter<i64>,
    request_duration: Histogram<f64>,
    request_body_bytes: Histogram<u64>,
    response_body_bytes: Histogram<u64>,
    admission_rejections: Counter<u64>,
    admission_wait: Histogram<f64>,
    timeouts: Counter<u64>,
    cancellations: Counter<u64>,
    active_connections: UpDownCounter<i64>,
    http2_active_streams: UpDownCounter<i64>,
    connection_terminations: Counter<u64>,
    graceful_drains: Counter<u64>,
    forced_drains: Counter<u64>,
    tls_handshakes: Counter<u64>,
}

struct HttpRequestTerminalMetric {
    protocol: &'static str,
    outcome: &'static str,
    status: u16,
    error_code: &'static str,
    duration: Duration,
    request_bytes: u64,
    response_bytes: u64,
}

impl HttpServerTelemetry {
    fn new() -> Arc<Self> {
        let meter = global::meter("lily_http_api");
        Arc::new(Self {
            requests: meter.u64_counter("http.server.requests").build(),
            request_in_flight: meter
                .i64_up_down_counter("http.server.requests.in_flight")
                .build(),
            request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .build(),
            request_body_bytes: meter
                .u64_histogram("http.server.request.body.size")
                .with_unit("By")
                .build(),
            response_body_bytes: meter
                .u64_histogram("http.server.response.body.size")
                .with_unit("By")
                .build(),
            admission_rejections: meter
                .u64_counter("http.server.admission.rejections")
                .build(),
            admission_wait: meter
                .f64_histogram("http.server.admission.wait.duration")
                .with_unit("s")
                .build(),
            timeouts: meter.u64_counter("http.server.timeouts").build(),
            cancellations: meter.u64_counter("http.server.cancellations").build(),
            active_connections: meter
                .i64_up_down_counter("http.server.connections.active")
                .build(),
            http2_active_streams: meter
                .i64_up_down_counter("http.server.http2.streams.active")
                .build(),
            connection_terminations: meter
                .u64_counter("http.server.connection.terminations")
                .build(),
            graceful_drains: meter.u64_counter("http.server.drains.graceful").build(),
            forced_drains: meter.u64_counter("http.server.drains.forced").build(),
            tls_handshakes: meter.u64_counter("http.server.tls.handshakes").build(),
        })
    }

    fn request_started(&self, protocol: &'static str, method: &'static str) {
        self.requests.add(
            1,
            &[
                KeyValue::new("network.protocol.version", protocol),
                KeyValue::new("http.request.method", method),
            ],
        );
        let in_flight_attributes = [KeyValue::new("network.protocol.version", protocol)];
        self.request_in_flight.add(1, &in_flight_attributes);
        if protocol == "2" {
            self.http2_active_streams.add(1, &in_flight_attributes);
        }
    }

    fn request_finished(&self, terminal: HttpRequestTerminalMetric) {
        let in_flight_attributes = [KeyValue::new("network.protocol.version", terminal.protocol)];
        let attributes = [
            KeyValue::new("network.protocol.version", terminal.protocol),
            KeyValue::new("lily.outcome", terminal.outcome),
            KeyValue::new("http.response.status_code", i64::from(terminal.status)),
            KeyValue::new("lily.error_code", terminal.error_code),
        ];
        self.request_in_flight.add(-1, &in_flight_attributes);
        self.request_duration
            .record(terminal.duration.as_secs_f64(), &attributes);
        self.request_body_bytes
            .record(terminal.request_bytes, &attributes);
        self.response_body_bytes
            .record(terminal.response_bytes, &attributes);
        if terminal.protocol == "2" {
            self.http2_active_streams.add(-1, &in_flight_attributes);
        }
        match terminal.outcome {
            "timeout" | "response_finalization_timeout" => self.timeouts.add(1, &attributes),
            "cancelled" | "client_disconnect" | "graceful_deadline" | "forced_shutdown"
            | "transport_failure" => self.cancellations.add(1, &attributes),
            _ => {}
        }
    }

    fn tls_handshake_finished(&self, outcome: &'static str) {
        self.tls_handshakes
            .add(1, &[KeyValue::new("lily.outcome", outcome)]);
    }

    fn connection_tasks_finished(&self, report: &ConnectionTaskReport) {
        for (category, count) in report.terminal_categories() {
            if count == 0 {
                continue;
            }
            self.connection_terminations.add(
                u64::try_from(count).unwrap_or(u64::MAX),
                &[KeyValue::new("lily.close_category", category)],
            );
        }
    }
}

struct ActiveConnectionMetricGuard {
    telemetry: Arc<HttpServerTelemetry>,
    protocol: &'static str,
}

impl ActiveConnectionMetricGuard {
    fn new(telemetry: Arc<HttpServerTelemetry>, protocol: &'static str) -> Self {
        telemetry
            .active_connections
            .add(1, &[KeyValue::new("network.protocol.version", protocol)]);
        Self {
            telemetry,
            protocol,
        }
    }
}

impl Drop for ActiveConnectionMetricGuard {
    fn drop(&mut self) {
        self.telemetry.active_connections.add(
            -1,
            &[KeyValue::new("network.protocol.version", self.protocol)],
        );
    }
}

fn protocol_version_label(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "0.9",
        Version::HTTP_10 => "1.0",
        Version::HTTP_11 => "1.1",
        Version::HTTP_2 => "2",
        Version::HTTP_3 => "3",
        _ => "unknown",
    }
}

fn configured_protocol_label(protocol: HttpProtocol) -> &'static str {
    match protocol {
        HttpProtocol::Http1_1 => "1.1",
        HttpProtocol::Http2 => "2",
        HttpProtocol::Auto => "auto",
    }
}

pub(crate) fn http_method_metric_label(method: &str) -> &'static str {
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

/// Pull-based Hyper body adapter. No reader task is spawned: polling is
/// driven by admitted execution or its retained response source, preserving
/// HTTP/2 flow-control backpressure. Actual release has a separate owner receipt.
/// The owning slot controls cancellation and deadlines; individual reads never
/// restart a timeout or preempt that slot's cooperative completion window.
struct BoundedRequestBody<B> {
    body: B,
    body_budget: BodyBudget,
    max_trailer_count: usize,
    max_trailer_bytes: usize,
    received_bytes: usize,
    observed_bytes: Option<Arc<AtomicU64>>,
    complete: bool,
}

impl<B> BoundedRequestBody<B>
where
    B: Body<Data = Bytes>,
{
    fn new(
        body: B,
        body_budget: BodyBudget,
        max_trailer_count: usize,
        max_trailer_bytes: usize,
    ) -> Result<Option<Self>, RequestBodyError> {
        let hint = body.size_hint();
        let limit_bytes = body_budget.limit_bytes();
        if hint.lower() > limit_bytes as u64
            || hint.upper().is_some_and(|size| size > limit_bytes as u64)
        {
            return Err(RequestBodyError::PayloadTooLarge { limit_bytes });
        }
        if hint.upper() == Some(0) {
            return Ok(None);
        }

        Ok(Some(Self {
            body,
            body_budget,
            max_trailer_count,
            max_trailer_bytes,
            received_bytes: 0,
            observed_bytes: None,
            complete: false,
        }))
    }

    fn with_observed_bytes(mut self, observed_bytes: Option<Arc<AtomicU64>>) -> Self {
        self.observed_bytes = observed_bytes;
        self
    }
}

#[async_trait::async_trait]
impl<B> RequestBodyStream for BoundedRequestBody<B>
where
    B: Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: Send,
{
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
        if self.complete {
            return Ok(None);
        }

        loop {
            let next = self.body.frame().await;
            let Some(frame) = next else {
                self.complete = true;
                return Ok(None);
            };
            let frame = frame.map_err(|_| RequestBodyError::TransportInterrupted)?;
            match frame.into_data() {
                Ok(data) => {
                    let next_length = self
                        .body_budget
                        .checked_next_length(self.received_bytes, data.len())
                        .map_err(|_| RequestBodyError::PayloadTooLarge {
                            limit_bytes: self.body_budget.limit_bytes(),
                        })?;
                    self.received_bytes = next_length;
                    if let Some(observed_bytes) = self.observed_bytes.as_ref() {
                        observed_bytes.store(
                            u64::try_from(next_length).unwrap_or(u64::MAX),
                            Ordering::Release,
                        );
                    }
                    return Ok(Some(data));
                }
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        HttpServer::convert_request_headers(
                            &trailers,
                            self.max_trailer_count,
                            self.max_trailer_bytes,
                        )
                        .map_err(|_| RequestBodyError::InvalidTrailers)?;
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (u64, Option<u64>) {
        let hint = self.body.size_hint();
        (hint.lower(), hint.upper())
    }
}

/// CIDR network allowed to supply forwarding metadata to the HTTP adapter.
/// Parse values such as `"10.0.0.0/8"` and add them to
/// [`HttpTransportConfig::trusted_proxy_cidrs`].
pub type TrustedProxyNetwork = ipnet::IpNet;

/// Bounded HTTP/1.1 and HTTP/2 transport controls.
///
/// Hyper/h2 owns framing and flow-control state. Lily supplies conservative
/// hard limits and application-level admission around that engine.
#[derive(Debug, Clone)]
pub struct HttpTransportConfig {
    /// Maximum simultaneously accepted TCP connections for this listener.
    ///
    /// Connections beyond the limit are rejected before TLS or HTTP parsing.
    pub max_connections: usize,
    /// Maximum request futures admitted across all active connections.
    ///
    /// Excess requests receive a bounded `503 Service Unavailable` response.
    pub max_in_flight_requests: usize,
    /// Maximum request representation retained or streamed for one request.
    pub max_request_body_bytes: usize,
    /// Maximum bytes retained for one multipart part. Must not exceed the
    /// request body limit.
    pub max_multipart_part_bytes: usize,
    /// Maximum multipart parts retained from one request.
    pub max_multipart_parts: usize,
    /// Maximum retained header metadata for one multipart part.
    pub max_multipart_metadata_bytes: usize,
    /// Maximum buffered response representation produced by one handler.
    pub max_response_body_bytes: usize,
    /// Maximum bytes accepted from one application response-stream item.
    ///
    /// Transport framing may split an accepted item into smaller HTTP/2 frames;
    /// the application item itself must still respect this bound.
    pub max_response_stream_chunk_bytes: usize,
    /// Optional cumulative response-stream budget. `None` permits long-lived
    /// streams while chunk, flow-control, and write-deadline bounds remain active.
    pub max_response_stream_total_bytes: Option<u64>,
    /// Maximum request header/trailer field-line count accepted by transport.
    pub max_request_header_count: usize,
    /// Maximum aggregate canonical field-line bytes for parsed request headers
    /// and trailers.
    ///
    /// Each field is charged as `name + ": " + value + "\r\n"`. Protocol
    /// codecs additionally enforce their own head/list limits; optional
    /// whitespace removed during parsing is intentionally not reconstructed.
    pub max_request_header_bytes: usize,
    /// Maximum application response header field-line count.
    pub max_response_header_count: usize,
    /// Maximum aggregate application response header wire bytes.
    pub max_response_header_bytes: usize,
    /// Maximum bytes accepted in the request target, including its query string.
    pub max_uri_bytes: usize,
    /// One absolute deadline from accepted admission through CORS, request-body
    /// consumption, middleware, guards, action execution and response writing
    /// (including lazy streaming and SSE).
    /// Expiry signals execution and allows a bounded cooperative window before
    /// stopping a pending dispatch slot. A pipeline that returns within that
    /// window keeps its actual response. A still-pending request produces 504.
    /// Response production/writing shares that same first-signal window. At its
    /// end, an uncommitted response may be replaced with a bounded 504; a
    /// committed incomplete response is terminated. Remote delivery is not
    /// guaranteed by source EOF, local flush or a protocol-task join.
    pub request_timeout: Duration,
    /// Time a connection may remain without an active request before Lily
    /// closes it.
    pub connection_idle_timeout: Duration,
    /// Bounds both a Lily-managed TLS handshake and the subsequent HTTP/1
    /// header read. Keeping the handshake under the existing connection
    /// deadline avoids introducing an unbounded pre-HTTP state.
    pub header_read_timeout: Duration,
    /// Socket networks allowed to supply `X-Forwarded-For`. The default is
    /// empty, so every forwarding header is ignored and stripped.
    pub trusted_proxy_cidrs: Vec<TrustedProxyNetwork>,
    /// Maximum number of comma-separated hops accepted from a trusted proxy.
    pub max_forwarded_hops: usize,
    /// Maximum concurrent HTTP/2 streams admitted by one connection.
    pub http2_max_concurrent_streams: u32,
    /// Initial HTTP/2 flow-control window for each stream, in bytes.
    pub http2_initial_stream_window_bytes: u32,
    /// Initial HTTP/2 connection-wide flow-control window, in bytes.
    pub http2_initial_connection_window_bytes: u32,
    /// Maximum HTTP/2 frame payload accepted by the codec, in bytes.
    pub http2_max_frame_bytes: u32,
    /// Maximum buffered outbound bytes retained by HTTP/2 for one stream.
    pub http2_max_send_buffer_bytes: usize,
    /// Maximum locally retained reset records for remotely initiated streams
    /// that are still pending application acceptance.
    pub http2_max_pending_accept_reset_streams: usize,
    /// Maximum locally retained reset records caused by application/protocol
    /// errors before older records are discarded.
    pub http2_max_local_error_reset_streams: usize,
    /// Interval between HTTP/2 keep-alive ping attempts.
    pub http2_keep_alive_interval: Duration,
    /// Time allowed for an HTTP/2 keep-alive acknowledgement before closing the
    /// connection.
    pub http2_keep_alive_timeout: Duration,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_in_flight_requests: 10_000,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_multipart_part_bytes: 8 * 1024 * 1024,
            max_multipart_parts: 128,
            max_multipart_metadata_bytes: 16 * 1024,
            max_response_body_bytes: 16 * 1024 * 1024,
            max_response_stream_chunk_bytes: 64 * 1024,
            max_response_stream_total_bytes: None,
            max_request_header_count: 64,
            max_request_header_bytes: 64 * 1024,
            max_response_header_count: 64,
            max_response_header_bytes: 64 * 1024,
            max_uri_bytes: 8 * 1024,
            request_timeout: Duration::from_secs(60),
            connection_idle_timeout: Duration::from_secs(120),
            header_read_timeout: Duration::from_secs(10),
            trusted_proxy_cidrs: Vec::new(),
            max_forwarded_hops: 16,
            http2_max_concurrent_streams: 100,
            http2_initial_stream_window_bytes: 1024 * 1024,
            http2_initial_connection_window_bytes: 2 * 1024 * 1024,
            http2_max_frame_bytes: 16 * 1024,
            http2_max_send_buffer_bytes: 1024 * 1024,
            http2_max_pending_accept_reset_streams: 20,
            http2_max_local_error_reset_streams: 128,
            http2_keep_alive_interval: Duration::from_secs(30),
            http2_keep_alive_timeout: Duration::from_secs(10),
        }
    }
}

impl HttpTransportConfig {
    /// Inclusive upper bound for every transport timeout and HTTP/2
    /// keep-alive duration.
    ///
    /// The bound keeps deadline arithmetic within the cross-platform range
    /// supported by [`Instant`] while preserving effectively unbounded
    /// operator choices for practical HTTP deployments.
    pub const MAX_TIMEOUT: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

    /// Validates every transport bound without opening a socket or allocating
    /// runtime admission state.
    ///
    /// [`crate::AppBuilder::build`] calls this automatically for the
    /// effective transport snapshot. Applications may call it earlier when
    /// assembling configuration and receive a stable, secret-free diagnostic
    /// string for the first invalid invariant.
    ///
    /// # Errors
    ///
    /// Returns an error when a limit is zero, exceeds its documented hard
    /// ceiling, conflicts with another limit, or when a timeout falls outside
    /// its documented inclusive range.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_connections == 0 || self.max_connections > 1_000_000 {
            return Err("max_connections must be in 1..=1_000_000".to_string());
        }
        if self.max_in_flight_requests == 0 || self.max_in_flight_requests > 1_000_000 {
            return Err("max_in_flight_requests must be in 1..=1_000_000".to_string());
        }
        BodyBudget::new(self.max_request_body_bytes)
            .map_err(|_| "max_request_body_bytes must be in 1..=1GiB".to_string())?;
        if self.max_multipart_part_bytes == 0
            || self.max_multipart_part_bytes > self.max_request_body_bytes
        {
            return Err(
                "max_multipart_part_bytes must be in 1..=max_request_body_bytes".to_string(),
            );
        }
        if self.max_multipart_parts == 0 || self.max_multipart_parts > 4096 {
            return Err("max_multipart_parts must be in 1..=4096".to_string());
        }
        if self.max_multipart_metadata_bytes == 0 || self.max_multipart_metadata_bytes > 1024 * 1024
        {
            return Err("max_multipart_metadata_bytes must be in 1..=1MiB".to_string());
        }
        let response_body_budget = BodyBudget::new(self.max_response_body_bytes)
            .map_err(|_| "max_response_body_bytes must be in 1..=1GiB".to_string())?;
        let response_streaming_limits = ResponseStreamingLimits::new(
            self.max_response_stream_chunk_bytes,
            self.max_response_stream_total_bytes,
        )
        .map_err(|error| error.to_string())?;
        ResponseLimits::new(
            response_body_budget,
            self.max_response_header_count,
            self.max_response_header_bytes,
        )
        .map(|limits| limits.with_streaming_limits(response_streaming_limits))
        .map_err(|error| error.to_string())?;
        if self.max_request_header_count == 0 || self.max_request_header_count > 1024 {
            return Err("max_request_header_count must be in 1..=1024".to_string());
        }
        if self.max_request_header_bytes == 0 || self.max_request_header_bytes > 1024 * 1024 {
            return Err("max_request_header_bytes must be in 1..=1MiB".to_string());
        }
        if self.max_uri_bytes == 0 || self.max_uri_bytes > 1024 * 1024 {
            return Err("max_uri_bytes must be in 1..=1MiB".to_string());
        }
        if self.request_timeout.is_zero()
            || self.connection_idle_timeout.is_zero()
            || self.header_read_timeout.is_zero()
            || self.request_timeout > Self::MAX_TIMEOUT
            || self.connection_idle_timeout > Self::MAX_TIMEOUT
            || self.header_read_timeout > Self::MAX_TIMEOUT
        {
            return Err(format!(
                "HTTP timeouts must be greater than zero and at most {} seconds",
                Self::MAX_TIMEOUT.as_secs()
            ));
        }
        if self.trusted_proxy_cidrs.len() > 64 {
            return Err("trusted_proxy_cidrs cannot contain more than 64 networks".to_string());
        }
        if self.max_forwarded_hops == 0 || self.max_forwarded_hops > 64 {
            return Err("max_forwarded_hops must be in 1..=64".to_string());
        }
        if self.http2_max_concurrent_streams == 0 || self.http2_max_concurrent_streams > 10_000 {
            return Err("http2_max_concurrent_streams must be in 1..=10000".to_string());
        }
        const MAX_HTTP2_WINDOW_BYTES: u32 = (1_u32 << 31) - 1;
        if self.http2_initial_stream_window_bytes == 0
            || self.http2_initial_stream_window_bytes > MAX_HTTP2_WINDOW_BYTES
        {
            return Err("http2_initial_stream_window_bytes must be in 1..=2147483647".to_string());
        }
        if self.http2_initial_connection_window_bytes == 0
            || self.http2_initial_connection_window_bytes > MAX_HTTP2_WINDOW_BYTES
        {
            return Err(
                "http2_initial_connection_window_bytes must be in 1..=2147483647".to_string(),
            );
        }
        if !(16_384..=16_777_215).contains(&self.http2_max_frame_bytes) {
            return Err("http2_max_frame_bytes must be in 16384..=16777215".to_string());
        }
        if self.http2_max_send_buffer_bytes == 0
            || self.http2_max_send_buffer_bytes > 16 * 1024 * 1024
        {
            return Err("http2_max_send_buffer_bytes must be in 1..=16MiB".to_string());
        }
        if self.http2_max_pending_accept_reset_streams == 0
            || self.http2_max_pending_accept_reset_streams > 1024
        {
            return Err("http2_max_pending_accept_reset_streams must be in 1..=1024".to_string());
        }
        if self.http2_max_local_error_reset_streams == 0
            || self.http2_max_local_error_reset_streams > 4096
        {
            return Err("http2_max_local_error_reset_streams must be in 1..=4096".to_string());
        }
        if self.http2_keep_alive_interval.is_zero()
            || self.http2_keep_alive_timeout.is_zero()
            || self.http2_keep_alive_interval > Self::MAX_TIMEOUT
            || self.http2_keep_alive_timeout > Self::MAX_TIMEOUT
        {
            return Err(format!(
                "HTTP/2 keep-alive durations must be greater than zero and at most {} seconds",
                Self::MAX_TIMEOUT.as_secs()
            ));
        }
        Ok(())
    }

    fn response_limits(&self) -> ResponseLimits {
        let limits = ResponseLimits::new(
            BodyBudget::new(self.max_response_body_bytes)
                .expect("validated HTTP transport response body limit"),
            self.max_response_header_count,
            self.max_response_header_bytes,
        )
        .expect("validated HTTP transport response header limits");
        let streaming = ResponseStreamingLimits::new(
            self.max_response_stream_chunk_bytes,
            self.max_response_stream_total_bytes,
        )
        .expect("validated HTTP transport response streaming limits");
        limits.with_streaming_limits(streaming)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectionCloseReason {
    PeerClosed,
    GracefulShutdown,
    IdleTimeout,
    ResponseFinalizationTimeout,
    ResponseStopped(ExecutionStopReason),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct RequestTaskSnapshot {
    pub(super) started: usize,
    pub(super) completed: usize,
    pub(super) cancelled: usize,
    pub(super) timed_out: usize,
    pub(super) active: usize,
}

/// Per-connection transport accounting, separate from request lifecycle ownership.
///
/// A guard is created before Hyper polls each service future and remains owned
/// by the response body until the final bounded chunk is accepted by Hyper.
/// Dropping a stream future (RST_STREAM, peer disconnect, task cancellation)
/// therefore reconciles transport cancellation without claiming source, scope
/// or helper cleanup. Those obligations remain in the retained request owner.
#[derive(Debug)]
pub(super) struct ConnectionActivity {
    next_request_id: AtomicUsize,
    active: AtomicUsize,
    started: AtomicUsize,
    completed: AtomicUsize,
    cancelled: AtomicUsize,
    timed_out: AtomicUsize,
    state: Mutex<ConnectionActivityState>,
    changed: Notify,
    termination_reason: Arc<Mutex<ExecutionStopReason>>,
    connection: Arc<ConnectionReceipt>,
}

#[derive(Debug)]
struct ConnectionActivityState {
    idle_since: Instant,
    #[cfg(test)]
    write_deadlines: HashMap<usize, Instant>,
    #[cfg(test)]
    timed_out_requests: HashSet<usize>,
    transports: HashMap<usize, ResponseTransportControl>,
    retired_transports: TransportTally,
}

impl ConnectionActivity {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            next_request_id: AtomicUsize::new(1),
            active: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            timed_out: AtomicUsize::new(0),
            state: Mutex::new(ConnectionActivityState {
                idle_since: Instant::now(),
                #[cfg(test)]
                write_deadlines: HashMap::new(),
                #[cfg(test)]
                timed_out_requests: HashSet::new(),
                transports: HashMap::new(),
                retired_transports: TransportTally::default(),
            }),
            changed: Notify::new(),
            termination_reason: Arc::new(Mutex::new(ExecutionStopReason::PeerDisconnect)),
            connection: ConnectionReceipt::current(),
        })
    }

    pub(super) fn enter(self: &Arc<Self>) -> RequestTaskGuard {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        self.started.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_waiters();
        RequestTaskGuard {
            activity: Arc::clone(self),
            request_id,
            terminal: false,
            observation: None,
            transport: None,
        }
    }

    pub(super) fn snapshot(&self) -> RequestTaskSnapshot {
        RequestTaskSnapshot {
            started: self.started.load(Ordering::Acquire),
            completed: self.completed.load(Ordering::Acquire),
            cancelled: self.cancelled.load(Ordering::Acquire),
            timed_out: self.timed_out.load(Ordering::Acquire),
            active: self.active.load(Ordering::Acquire),
        }
    }

    fn transport_failed(&self) {
        *self
            .termination_reason
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = ExecutionStopReason::TransportFailure;
        for control in self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .transports
            .values()
        {
            control.connection_failed();
        }
    }

    fn observe_flush(&self) {
        self.connection.observe_flush();
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut changed = false;
        for control in state.transports.values() {
            changed |= control.observe_http1_flush();
            control.wake_after_flush();
        }
        if changed {
            self.changed.notify_waiters();
        }
    }

    fn reap_transports(state: &mut ConnectionActivityState) {
        let had_transports = !state.transports.is_empty();
        state.transports.retain(|_id, control| {
            let snapshot = control.snapshot();
            if snapshot.is_terminal() {
                state.retired_transports.record(snapshot);
                #[cfg(test)]
                {
                    state.write_deadlines.remove(_id);
                    state.timed_out_requests.remove(_id);
                }
                false
            } else {
                true
            }
        });
        if had_transports && state.transports.is_empty() {
            // Time spent flushing the final frame is response activity, not
            // keep-alive idle time. Start the idle interval at this boundary.
            state.idle_since = Instant::now();
        }
    }

    fn transport_snapshot(&self) -> TransportTally {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::reap_transports(&mut state);
        let mut tally = state.retired_transports;
        for control in state.transports.values() {
            tally.record(control.snapshot());
        }
        tally
    }

    fn response_stop_close_reason(&self) -> ConnectionCloseReason {
        if self.connection.stop.is_cancelled() {
            self.connection.close_reason()
        } else {
            // Raw codec services do not bind a request-specific control.
            ConnectionCloseReason::ResponseFinalizationTimeout
        }
    }

    async fn wait_until_idle(&self, timeout: Duration) {
        loop {
            let changed = self.changed.notified();
            let transport_outstanding = {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                Self::reap_transports(&mut state);
                !state.transports.is_empty()
            };
            if self.active.load(Ordering::Acquire) != 0 || transport_outstanding {
                changed.await;
                continue;
            }

            let elapsed = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .idle_since
                .elapsed();
            if elapsed >= timeout {
                if self.active.load(Ordering::Acquire) == 0 {
                    return;
                }
                continue;
            }

            tokio::select! {
                _ = tokio::time::sleep(timeout - elapsed) => {}
                _ = changed => continue,
            }
        }
    }

    async fn wait_until_response_stop(&self) {
        use futures::StreamExt;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let controls = {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                Self::reap_transports(&mut state);
                state.transports.values().cloned().collect::<Vec<_>>()
            };
            let mut joins = controls
                .iter()
                .filter_map(ResponseTransportControl::worker)
                .collect::<futures::stream::FuturesUnordered<_>>();
            let mut watches = controls
                .into_iter()
                .filter(|control| control.snapshot().stop_requested.is_none())
                .map(|control| async move { control.enforce_deadline().await })
                .collect::<futures::stream::FuturesUnordered<_>>();
            tokio::select! {
                _ = joins.next(), if !joins.is_empty() => { self.changed.notify_waiters(); }
                _ = watches.next(), if !watches.is_empty() => { self.changed.notify_waiters(); }
                _ = self.raw_codec_stop() => return,
                _ = changed => {},
            }
        }
    }

    // Only codec conformance tests inject a timer without a request owner.
    #[cfg(not(test))]
    async fn raw_codec_stop(&self) {
        std::future::pending::<()>().await;
    }

    #[cfg(test)]
    async fn raw_codec_stop(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let deadline = self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .write_deadlines
                .iter()
                .min_by_key(|(_, at)| **at)
                .map(|(id, at)| (*id, *at));
            let Some((id, at)) = deadline else {
                changed.await;
                continue;
            };
            if Instant::now() >= at {
                let control = {
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    if state.write_deadlines.get(&id) != Some(&at) {
                        continue;
                    }
                    state.write_deadlines.remove(&id);
                    state.timed_out_requests.insert(id);
                    state.transports.get(&id).cloned()
                };
                if let Some(control) = control {
                    control.request_stop(ExecutionStopReason::ResponseFinalizationTimeout);
                    if control.worker().is_some() {
                        continue;
                    }
                }
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(at.saturating_duration_since(Instant::now())) => {},
                _ = changed => {},
            }
        }
    }

    #[cfg(test)]
    fn begin_response_write(&self, request_id: usize, timeout: Duration) {
        let started = Instant::now();
        // Validated transport values fit; direct internal callers still fail
        // closed instead of panicking on an unrepresentable deadline.
        let deadline = started.checked_add(timeout).unwrap_or(started);
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .write_deadlines
            .insert(request_id, deadline);
        self.changed.notify_waiters();
    }

    fn finish_request(&self, request_id: usize) -> Option<ExecutionStopReason> {
        let became_idle = self.active.fetch_sub(1, Ordering::AcqRel) == 1;
        let stop_reason = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // EOF/frame handoff is not the protocol task's join or a socket
            // flush. Bound controls keep their watchdog after body release.
            let stop_reason = state
                .transports
                .get(&request_id)
                .and_then(|control| control.snapshot().stop_requested);
            #[cfg(test)]
            let stop_reason = {
                if !state.transports.contains_key(&request_id) {
                    state.write_deadlines.remove(&request_id);
                }
                let injected = state.timed_out_requests.remove(&request_id);
                stop_reason.or(injected.then_some(ExecutionStopReason::ResponseFinalizationTimeout))
            };
            if became_idle {
                state.idle_since = Instant::now();
            }
            stop_reason
        };
        self.changed.notify_waiters();
        stop_reason
    }
}

/// Observes HTTP/1's final successful flush after Hyper has encoded the body.
/// TLS remains inside this adapter, so its flush is observed too. This is an
/// I/O completion boundary, never a TCP acknowledgement or browser receipt.
struct ResponseIo<I> {
    inner: I,
    activity: Arc<ConnectionActivity>,
}

impl<I: AsyncRead + Unpin> AsyncRead for ResponseIo<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.activity.connection.register_driver(cx);
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for ResponseIo<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.activity.connection.register_driver(cx);
        Pin::new(&mut self.inner).poll_write(cx, buffer)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.activity.connection.register_driver(cx);
        Pin::new(&mut self.inner).poll_write_vectored(cx, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.activity.connection.register_driver(cx);
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.activity.observe_flush();
        }
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub(super) struct RequestTaskGuard {
    activity: Arc<ConnectionActivity>,
    request_id: usize,
    terminal: bool,
    observation: Option<RequestObservation>,
    transport: Option<ResponseTransportControl>,
}

#[derive(Debug)]
struct RequestObservation {
    span: tracing::Span,
    telemetry: Arc<HttpServerTelemetry>,
    started: Instant,
    protocol: &'static str,
    request_bytes: Arc<AtomicU64>,
    response_bytes: u64,
    status: u16,
    outcome: &'static str,
    error_code: Option<&'static str>,
    application: Option<AppCallOutcome>,
}

impl RequestTaskGuard {
    fn bind_transport(&mut self, version: Version) -> ResponseTransportControl {
        let control = ResponseTransportControl::bind(version, self.activity.connection.clone());
        self.activity
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .transports
            .insert(self.request_id, control.clone());
        self.activity.changed.notify_waiters();
        self.transport = Some(control.clone());
        control
    }
    fn observe(
        &mut self,
        span: tracing::Span,
        telemetry: Arc<HttpServerTelemetry>,
        protocol: &'static str,
        method: &'static str,
    ) {
        telemetry.request_started(protocol, method);
        self.observation = Some(RequestObservation {
            span,
            telemetry,
            started: Instant::now(),
            protocol,
            request_bytes: Arc::new(AtomicU64::new(0)),
            response_bytes: 0,
            status: 0,
            outcome: "success",
            error_code: None,
            application: None,
        });
    }

    fn request_byte_counter(&self) -> Option<Arc<AtomicU64>> {
        self.observation
            .as_ref()
            .map(|observation| Arc::clone(&observation.request_bytes))
    }

    fn record_response(
        &mut self,
        status: u16,
        bytes: usize,
        outcome: &'static str,
        error_code: Option<&'static str>,
    ) {
        if let Some(observation) = self.observation.as_mut() {
            observation.response_bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
            observation.status = status;
            observation.outcome = outcome;
            observation.error_code = error_code;
        }
    }

    fn record_application(&mut self, application: AppCallOutcome) {
        if let Some(observation) = self.observation.as_mut() {
            observation.application = Some(application);
        }
    }

    fn add_response_bytes(&mut self, bytes: usize) {
        if let Some(observation) = self.observation.as_mut() {
            observation.response_bytes = observation
                .response_bytes
                .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        }
    }

    #[cfg(test)]
    fn begin_response_write(&mut self, timeout: Duration) {
        self.activity.begin_response_write(self.request_id, timeout);
    }

    fn finish_observation(&mut self, outcome_override: Option<&'static str>) {
        let Some(observation) = self.observation.take() else {
            return;
        };
        let outcome = outcome_override.unwrap_or(observation.outcome);
        let terminal_error_code = match outcome_override {
            Some("timeout") => "REQUEST_TIMEOUT",
            Some("response_finalization_timeout") => "RESPONSE_FINALIZATION_TIMEOUT",
            Some("graceful_deadline") => "GRACEFUL_DEADLINE",
            Some("forced_shutdown") => "FORCED_SHUTDOWN",
            Some("transport_failure") => "TRANSPORT_FAILURE",
            Some("client_disconnect") => "CLIENT_DISCONNECT",
            Some(_) => "REQUEST_CANCELLED",
            None => observation.error_code.unwrap_or("none"),
        };
        let request_bytes = observation.request_bytes.load(Ordering::Acquire);
        // Commit terminal fields once. The OTel layer appends ordinary field
        // updates, so recording provisional values exports duplicate keys and
        // can retain a success that a later stream failure invalidates.
        if observation.status != 0 {
            observation
                .span
                .record("http.response.status_code", i64::from(observation.status));
        }
        observation.span.record(
            "http.request.body.size",
            i64::try_from(request_bytes).unwrap_or(i64::MAX),
        );
        observation.span.record(
            "http.response.body.size",
            i64::try_from(observation.response_bytes).unwrap_or(i64::MAX),
        );
        observation.span.record("lily.outcome", outcome);
        let application_error = observation
            .application
            .is_some_and(AppCallOutcome::application_error);
        observation
            .span
            .record("lily.application_error", application_error);
        if let Some(application) = observation.application {
            if let Some(code) = application.application_error_code() {
                observation.span.record("lily.application_error_code", code);
            }
            if let Some(failure) = application.application_failure() {
                observation
                    .span
                    .record("lily.application_outcome", failure.as_str());
            }
        }
        // HTTP owns its final SERVER status, including recovery by middleware.
        // A diagnostic on an ordinary 4xx does not indicate transport failure.
        // Real writer/transport stops take precedence even after a 2xx commit.
        observation.span.record(
            "otel.status_code",
            if !matches!(outcome, "success" | "rejected") {
                "ERROR"
            } else {
                "UNSET"
            },
        );
        if outcome_override.is_some() || observation.error_code.is_some() {
            observation
                .span
                .record("lily.error_code", terminal_error_code);
        }
        tracing::event!(
            parent: &observation.span,
            tracing::Level::INFO,
            lily.event = "http.server.terminal",
            lily.outcome = %outcome,
            lily.error_code = %terminal_error_code,
            "HTTP response transport observation completed"
        );
        observation
            .telemetry
            .request_finished(HttpRequestTerminalMetric {
                protocol: observation.protocol,
                outcome,
                status: observation.status,
                error_code: terminal_error_code,
                duration: observation.started.elapsed(),
                request_bytes,
                response_bytes: observation.response_bytes,
            });
    }

    /// Counts an actual response stop separately from merely notifying the
    /// execution token. A cooperative result can still complete normally.
    fn finish_activity(&mut self) -> bool {
        let Some(reason) = self.activity.finish_request(self.request_id) else {
            return false;
        };
        let outcome = match reason {
            ExecutionStopReason::RequestTimeout => "timeout",
            ExecutionStopReason::ResponseFinalizationTimeout => "response_finalization_timeout",
            ExecutionStopReason::GracefulDeadline => "graceful_deadline",
            ExecutionStopReason::ForcedShutdown => "forced_shutdown",
            ExecutionStopReason::PeerDisconnect => "client_disconnect",
            ExecutionStopReason::TransportFailure => "transport_failure",
            ExecutionStopReason::ServiceWaiterDropped => "cancelled",
        };
        if matches!(
            reason,
            ExecutionStopReason::RequestTimeout | ExecutionStopReason::ResponseFinalizationTimeout
        ) {
            self.activity.timed_out.fetch_add(1, Ordering::Relaxed);
        } else {
            self.activity.cancelled.fetch_add(1, Ordering::Relaxed);
        }
        self.finish_observation(Some(outcome));
        true
    }

    fn complete(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        if let Some(control) = &self.transport {
            control.frames_finished(ResponseFrames::Completed);
        }
        if !self.finish_activity() {
            self.activity.completed.fetch_add(1, Ordering::Relaxed);
            self.finish_observation(None);
        }
    }

    fn fail_response(&mut self, error_code: &'static str) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        if let Some(control) = &self.transport {
            control.frames_finished(ResponseFrames::Failed);
        }
        if !self.finish_activity() {
            self.activity.completed.fetch_add(1, Ordering::Relaxed);
            if let Some(observation) = self.observation.as_mut() {
                observation.outcome = "error";
                observation.error_code = Some(error_code);
            }
            self.finish_observation(None);
        }
    }
}

impl Drop for RequestTaskGuard {
    fn drop(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        if let Some(control) = &self.transport {
            // Before commit the owner may still select a minimal fallback.
            if control.snapshot().commit
                == super::response_control::ResponseCommit::CommittedToProtocol
            {
                control.frames_finished(ResponseFrames::Dropped);
            }
        }
        if !self.finish_activity() {
            self.activity.cancelled.fetch_add(1, Ordering::Relaxed);
            self.finish_observation(Some("client_disconnect"));
        }
    }
}

/// Chunked, deadline-aware response body used by both HTTP/1.1 and HTTP/2.
///
/// Emitting at most one configured frame per poll lets Hyper/h2 apply its
/// bounded send buffer and flow-control windows instead of accepting one
/// application-sized allocation as a single frame.
#[derive(Debug)]
pub(super) struct BoundedResponseBody {
    source: BoundedResponseSource,
    max_frame_bytes: usize,
    request_guard: Option<RequestTaskGuard>,
    transport: Option<ResponseTransportControl>,
    eof_released: bool,
}

#[derive(Debug)]
enum BoundedResponseSource {
    Full(Bytes),
    Stream(ResponseBodyStream),
    Owned(crate::request_lifecycle::BodyBridge),
}

impl BoundedResponseBody {
    fn with_owner(mut self, owner: &RequestExecutionContext) -> Self {
        let source = std::mem::replace(&mut self.source, BoundedResponseSource::Full(Bytes::new()));
        self.source = match source {
            BoundedResponseSource::Stream(source) => BoundedResponseSource::Owned(
                owner.own_response_stream(
                    source,
                    self.max_frame_bytes,
                    owner
                        .response_transport()
                        .map(ResponseTransportControl::reason)
                        .or_else(|| {
                            self.request_guard
                                .as_ref()
                                .map(|guard| guard.activity.termination_reason.clone())
                        }),
                ),
            ),
            BoundedResponseSource::Full(bytes) => {
                // Arbitrary Bytes owners/destructors must not cross the DI
                // barrier. The protocol gets an independent bounded allocation.
                let independent = Bytes::copy_from_slice(&bytes);
                drop(bytes);
                owner.independent_response();
                BoundedResponseSource::Full(independent)
            }
            BoundedResponseSource::Owned(_) => unreachable!("response already owned"),
        };
        self
    }
    pub(super) fn new(
        body: impl Into<TransportResponseBody>,
        max_frame_bytes: usize,
        request_guard: RequestTaskGuard,
    ) -> Self {
        let source = match body.into() {
            TransportResponseBody::Full(body) => BoundedResponseSource::Full(body),
            TransportResponseBody::Stream(body) => BoundedResponseSource::Stream(body),
        };
        Self {
            source,
            max_frame_bytes,
            transport: request_guard.transport.clone(),
            request_guard: Some(request_guard),
            eof_released: false,
        }
    }

    /// Raw codec tests can install an explicit final protocol-stop timer.
    /// Managed responses use the admitted RequestDeadline instead.
    #[cfg(test)]
    pub(super) fn new_for_test(
        body: impl Into<TransportResponseBody>,
        max_frame_bytes: usize,
        timeout: Duration,
        mut request_guard: RequestTaskGuard,
    ) -> Self {
        request_guard.begin_response_write(timeout);
        Self::new(body, max_frame_bytes, request_guard)
    }

    fn replace_with_empty(&mut self, status: StatusCode) {
        self.source = BoundedResponseSource::Full(Bytes::new());
        self.eof_released = false;
        if let Some(guard) = &mut self.request_guard {
            guard.record_response(
                status.as_u16(),
                0,
                if status == StatusCode::GATEWAY_TIMEOUT {
                    "timeout"
                } else {
                    "cancelled"
                },
                Some(if status == StatusCode::GATEWAY_TIMEOUT {
                    "GATEWAY_TIMEOUT"
                } else {
                    "SERVICE_UNAVAILABLE"
                }),
            );
        }
    }

    fn complete(&mut self) {
        if let Some(mut guard) = self.request_guard.take() {
            guard.complete();
        }
    }

    fn fail(&mut self, error: ResponseBodyError) {
        if let Some(mut guard) = self.request_guard.take() {
            guard.fail_response(error.error_code());
        }
    }

    fn track_data(&self, bytes: Bytes) -> Bytes {
        match &self.transport {
            Some(control) => control.track_data(bytes),
            None => bytes,
        }
    }

    fn poll_eof(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, ResponseBodyError>>> {
        if let Some(control) = self.transport.as_ref().filter(|control| control.is_http2()) {
            std::task::ready!(control.poll_data_flush(cx));
        }
        self.eof_released = true;
        Poll::Ready(None)
    }
}

impl Body for BoundedResponseBody {
    type Data = Bytes;
    type Error = ResponseBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        if this.request_guard.is_none() {
            return this.poll_eof(context);
        }
        if let BoundedResponseSource::Full(remaining) = &mut this.source {
            if remaining.is_empty() {
                this.complete();
                return this.poll_eof(context);
            }

            let length = remaining.len().min(this.max_frame_bytes);
            let frame = remaining.split_to(length);
            if remaining.is_empty() {
                this.complete();
            }
            return Poll::Ready(Some(Ok(Frame::data(this.track_data(frame)))));
        }

        let polled = match &mut this.source {
            BoundedResponseSource::Stream(stream) => {
                Pin::new(stream).poll_bounded_chunk(context, this.max_frame_bytes)
            }
            BoundedResponseSource::Owned(bridge) => bridge.poll_chunk(context),
            BoundedResponseSource::Full(_) => {
                unreachable!("buffered response returned through its dedicated fast path")
            }
        };
        match polled {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => {
                if let Some(guard) = this.request_guard.as_mut() {
                    guard.add_response_bytes(chunk.len());
                }
                Poll::Ready(Some(Ok(Frame::data(this.track_data(chunk)))))
            }
            Poll::Ready(Some(Err(error))) => {
                this.fail(error);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.complete();
                this.poll_eof(context)
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        (self.request_guard.is_none()
            || matches!(&self.source, BoundedResponseSource::Full(bytes) if bytes.is_empty()))
            && (self.eof_released
                || !self
                    .transport
                    .as_ref()
                    .is_some_and(ResponseTransportControl::is_http2))
    }

    fn size_hint(&self) -> SizeHint {
        match &self.source {
            BoundedResponseSource::Full(remaining) => SizeHint::with_exact(remaining.len() as u64),
            BoundedResponseSource::Owned(bridge) => {
                if self.request_guard.is_none() {
                    return SizeHint::with_exact(0);
                }
                bridge
                    .remaining()
                    .map_or_else(SizeHint::default, SizeHint::with_exact)
            }
            BoundedResponseSource::Stream(stream) => {
                if self.request_guard.is_none() {
                    return SizeHint::with_exact(0);
                }
                if let Some(exact) = stream.exact_length() {
                    return SizeHint::with_exact(exact.saturating_sub(stream.emitted_bytes()));
                }
                let mut hint = SizeHint::default();
                if let Some(maximum) = stream.max_total_bytes() {
                    hint.set_upper(maximum.saturating_sub(stream.emitted_bytes()));
                }
                hint
            }
        }
    }
}

impl Drop for BoundedResponseBody {
    fn drop(&mut self) {
        // HTTP/1 may drop a body after exactly Content-Length bytes (including
        // zero), without polling EOF. Count only bytes handed to the protocol,
        // not bytes merely produced or queued in the owner's bridge. Actual
        // flush/join evidence remains the transport control's responsibility.
        // HTTP/2 still needs its explicit EOF/END_STREAM and flush path.
        let length_delimited = self
            .transport
            .as_ref()
            .is_none_or(|control| !control.is_http2());
        let fully_emitted = match &self.source {
            BoundedResponseSource::Full(bytes) => bytes.is_empty(),
            BoundedResponseSource::Stream(stream) => {
                length_delimited && stream.exact_length() == Some(stream.emitted_bytes())
            }
            BoundedResponseSource::Owned(bridge) => {
                length_delimited && bridge.exact_length_consumed()
            }
        };
        if fully_emitted
            && self.transport.as_ref().is_none_or(|control| {
                let snapshot = control.snapshot();
                snapshot.commit == super::response_control::ResponseCommit::CommittedToProtocol
                    && snapshot.stop_requested.is_none()
            })
        {
            self.complete();
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ConnectionTaskReport {
    pub(crate) accepted: usize,
    pub(crate) completed: usize,
    pub(crate) panicked: usize,
    pub(crate) cancelled: usize,
    pub(crate) forced: usize,
    pub(crate) rejected: usize,
    pub(crate) connection_errors: usize,
    pub(crate) tls_handshake_errors: usize,
    pub(crate) peer_closed: usize,
    pub(crate) graceful_shutdown: usize,
    pub(crate) idle_timeout: usize,
    pub(crate) response_finalization_timeout: usize,
    pub(crate) response_stopped: usize,
    pub(crate) outstanding: usize,
}

const MAX_CONSECUTIVE_ACCEPT_FAILURES: usize = 5;
const ACCEPT_FAILURE_INITIAL_BACKOFF: Duration = Duration::from_millis(25);
const ACCEPT_FAILURE_MAX_BACKOFF: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcceptFailureAction {
    RetryAfter(Duration),
    Stop { consecutive_failures: usize },
}

/// Bounded accept-error policy. A damaged/exhausted listener must neither
/// spin nor keep the application apparently healthy forever.
#[derive(Debug, Default)]
struct AcceptFailureBudget {
    consecutive_failures: usize,
}

impl AcceptFailureBudget {
    fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    fn record_failure(&mut self) -> AcceptFailureAction {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures >= MAX_CONSECUTIVE_ACCEPT_FAILURES {
            return AcceptFailureAction::Stop {
                consecutive_failures: self.consecutive_failures,
            };
        }

        let exponent = (self.consecutive_failures - 1).min(3) as u32;
        AcceptFailureAction::RetryAfter(
            ACCEPT_FAILURE_INITIAL_BACKOFF
                .saturating_mul(2_u32.saturating_pow(exponent))
                .min(ACCEPT_FAILURE_MAX_BACKOFF),
        )
    }
}

/// Typed terminal accept-loop failure retained as the source of the returned
/// `io::Error`. The report reconciles every connection task admitted before
/// the listener became unusable.
#[derive(Debug)]
struct AcceptLoopFailure {
    consecutive_failures: usize,
    report: ConnectionTaskReport,
}

impl std::fmt::Display for AcceptLoopFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "HTTP listener stopped after {} consecutive accept failures (accepted={}, completed={}, forced={}, cancelled={}, panicked={})",
            self.consecutive_failures,
            self.report.accepted,
            self.report.completed,
            self.report.forced,
            self.report.cancelled,
            self.report.panicked,
        )
    }
}

impl StdError for AcceptLoopFailure {}

/// Typed TLS negotiation failure retained below the public `io::Error`
/// boundary. It contains no configuration paths or private-key material.
#[derive(Debug)]
enum TlsHandshakeFailure {
    TimedOut,
    Negotiation(io::Error),
    Http2AlpnRequired,
}

impl TlsHandshakeFailure {
    fn category(&self) -> &'static str {
        match self {
            Self::TimedOut => "timeout",
            Self::Negotiation(_) => "negotiation",
            Self::Http2AlpnRequired => "alpn_required",
        }
    }
}

impl std::fmt::Display for TlsHandshakeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => formatter.write_str("TLS handshake timed out"),
            Self::Negotiation(error) => write!(formatter, "TLS handshake failed: {error}"),
            Self::Http2AlpnRequired => {
                formatter.write_str("TLS client did not negotiate the required h2 ALPN protocol")
            }
        }
    }
}

impl StdError for TlsHandshakeFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Negotiation(error) => Some(error),
            Self::TimedOut | Self::Http2AlpnRequired => None,
        }
    }
}

impl ConnectionTaskReport {
    fn terminal_categories(&self) -> [(&'static str, usize); 9] {
        [
            ("peer_closed", self.peer_closed),
            ("graceful_shutdown", self.graceful_shutdown),
            ("idle_timeout", self.idle_timeout),
            (
                "response_finalization_timeout",
                self.response_finalization_timeout,
            ),
            ("tls_error", self.tls_handshake_errors),
            (
                "transport_error",
                self.connection_errors
                    .saturating_sub(self.tls_handshake_errors),
            ),
            ("cancelled", self.cancelled),
            ("panicked", self.panicked),
            ("response_stopped", self.response_stopped),
        ]
    }

    pub(crate) fn reconciles(&self) -> bool {
        self.accepted
            == self
                .completed
                .saturating_add(self.connection_errors)
                .saturating_add(self.cancelled)
                .saturating_add(self.panicked)
                .saturating_add(self.outstanding)
    }

    pub(crate) fn shutdown_is_terminal(&self) -> bool {
        self.reconciles() && self.panicked == 0 && self.outstanding == 0
    }

    pub(crate) fn shutdown_is_clean(&self) -> bool {
        self.shutdown_is_terminal() && self.forced == 0
    }

    fn record_join(&mut self, result: TaskResult<io::Result<ConnectionCloseReason>>) {
        match result.as_ref().map(|result| result.as_ref()) {
            Ok(Ok(reason)) => {
                self.completed += 1;
                match reason {
                    ConnectionCloseReason::PeerClosed => self.peer_closed += 1,
                    ConnectionCloseReason::GracefulShutdown => self.graceful_shutdown += 1,
                    ConnectionCloseReason::IdleTimeout => self.idle_timeout += 1,
                    ConnectionCloseReason::ResponseFinalizationTimeout => {
                        self.response_finalization_timeout += 1
                    }
                    ConnectionCloseReason::ResponseStopped(reason) => {
                        self.response_stopped += 1;
                        tracing::debug!(?reason, "HTTP connection stopped for its response");
                    }
                }
            }
            Ok(Err(error)) => {
                self.connection_errors += 1;
                if let Some(failure) = error
                    .get_ref()
                    .and_then(|source| source.downcast_ref::<TlsHandshakeFailure>())
                {
                    self.tls_handshake_errors += 1;
                    // Invalid handshakes are attacker-controlled input. Keep the
                    // bounded category in debug logs and rely on the counter for
                    // production alerting rather than emitting one warning per peer.
                    tracing::debug!(
                        tls.error_category = failure.category(),
                        "TLS handshake rejected"
                    );
                } else {
                    tracing::warn!(error = %error, "HTTP connection closed with a protocol/transport error");
                }
            }
            Err(error) if error.is_cancelled() => self.cancelled += 1,
            Err(_) => self.panicked += 1,
        }
    }
}

/// Application-owned HTTP runtime.
///
/// `App::start` uses this handle to stop listener admission and join every
/// connection task before it closes the application's DI container.
pub(crate) struct ManagedHttpServer {
    bound_address: SocketAddr,
    shutdown: CancellationToken,
    force: CancellationToken,
    budget: ShutdownBudget,
    requests: RequestRegistry,
    task: TaskReceipt<io::Result<ConnectionTaskReport>>,
    outcome: Option<ManagedHttpServerOutcome>,
}

#[derive(Clone)]
enum ManagedHttpServerOutcome {
    Completed(ConnectionTaskReport),
    Failed {
        kind: io::ErrorKind,
        message: String,
    },
}

impl ManagedHttpServerOutcome {
    fn as_result(&self) -> io::Result<ConnectionTaskReport> {
        match self {
            Self::Completed(report) => Ok(*report),
            Self::Failed { kind, message } => Err(io::Error::new(*kind, message.clone())),
        }
    }
}

impl ManagedHttpServer {
    pub(crate) fn bound_address(&self) -> SocketAddr {
        self.bound_address
    }

    pub(crate) fn admission_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub(crate) fn force_token(&self) -> CancellationToken {
        self.force.clone()
    }

    pub(crate) async fn wait(&mut self) -> io::Result<ConnectionTaskReport> {
        if let Some(outcome) = &self.outcome {
            return outcome.as_result();
        }
        let outcome = match self.task.clone().await {
            Ok(result) => match result.as_ref() {
                Ok(report) => ManagedHttpServerOutcome::Completed(*report),
                Err(error) => ManagedHttpServerOutcome::Failed {
                    kind: error.kind(),
                    message: error.to_string(),
                },
            },
            Err(error) => ManagedHttpServerOutcome::Failed {
                kind: io::ErrorKind::Other,
                message: format!("Server task failed: {error}"),
            },
        };
        let result = outcome.as_result();
        self.outcome = Some(outcome);
        result
    }
}

impl Drop for ManagedHttpServer {
    fn drop(&mut self) {
        if self.outcome.is_some() {
            // wait() has consumed the actual listener join. Releasing its
            // completed owner is not a new force request; request/DI evidence
            // continues to belong to the retained application inventories.
            return;
        }
        // A prematurely dropped owner must not leave an accept loop or its
        // connection tasks detached from the application lifecycle.
        self.budget.begin();
        self.shutdown.cancel();
        self.requests
            .cancel_executions(ExecutionStopReason::ForcedShutdown);
        self.force.cancel();
        // The listener remains retained by the application task inventory. It
        // must drive cooperative response drain before final protocol stop;
        // aborting it here would also abort its entire connection TaskSet.
    }
}

impl HttpServer {
    pub(crate) async fn start_managed(self) -> io::Result<ManagedHttpServer> {
        let service = self.0;
        let listener = Self::bind(service.listen_address()).await?;
        let bound_address = listener.local_addr().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to inspect bound HTTP listener address: {error}"),
            )
        })?;
        let transport = service.transport_config().clone();
        let tls_acceptor = service
            .rustls_config()
            .map(|config| Self::tls_acceptor(config, service.protocol()));
        let budget = service.shutdown_budget().clone();
        let shutdown = service.transport_shutdown();
        let task_shutdown = shutdown.clone();
        let force = service.transport_force();
        let task_force = force.clone();
        let tasks = service.task_inventory().listener.clone();
        let requests = service.request_registry().clone();
        let task = tasks.spawn(async move {
            Self::run(
                listener,
                service,
                transport,
                tls_acceptor,
                task_shutdown,
                task_force,
            )
            .await
        });

        Ok(ManagedHttpServer {
            bound_address,
            shutdown,
            force,
            budget,
            requests,
            task,
            outcome: None,
        })
    }

    async fn bind(addr: &str) -> io::Result<TcpListener> {
        TcpListener::bind(addr)
            .await
            .map_err(|error| Self::bind_error(addr, error))
    }

    fn bind_error(addr: &str, error: io::Error) -> io::Error {
        io::Error::new(
            error.kind(),
            format!("failed to bind HTTP listener at {addr}: {error}"),
        )
    }

    async fn run(
        listener: TcpListener,
        service: Arc<App>,
        transport: HttpTransportConfig,
        tls_acceptor: Option<TlsAcceptor>,
        shutdown: CancellationToken,
        force: CancellationToken,
    ) -> io::Result<ConnectionTaskReport> {
        let budget = service.shutdown_budget().clone();
        let task_inventory = service.task_inventory().clone();
        let mut connection_tasks = TaskSet::new(task_inventory.connections.clone());
        let mut report = ConnectionTaskReport::default();
        let mut accept_failures = AcceptFailureBudget::default();
        let mut terminal_accept_failure = None;
        let telemetry = HttpServerTelemetry::new();
        let connection_admission = Arc::new(Semaphore::new(transport.max_connections));
        let request_admission = Arc::new(Semaphore::new(transport.max_in_flight_requests));
        let cors_services = Self::compile_cors_dispatch_service(&service, &transport);

        // Stop admission has priority when an accept and shutdown become ready
        // in the same scheduler turn.
        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => break,
                Some(result) = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    report.record_join(result);
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, socket)) => {
                            if shutdown.is_cancelled() || force.is_cancelled() {
                                drop(stream);
                                break;
                            }
                            accept_failures.record_success();
                            let connection_permit = match Arc::clone(&connection_admission).try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    report.rejected += 1;
                                    telemetry.admission_rejections.add(
                                        1,
                                        &[KeyValue::new("lily.admission", "connection")],
                                    );
                                    drop(stream);
                                    continue;
                                }
                            };
                            report.accepted += 1;
                            let service = Arc::clone(&service);
                            let transport = transport.clone();
                            let request_admission = Arc::clone(&request_admission);
                            let cors_services = cors_services.clone();
                            let telemetry = Arc::clone(&telemetry);
                            let connection_shutdown = shutdown.clone();
                            let tls_acceptor = tls_acceptor.clone();
                            let protocol = service.protocol();
                            let protocol_label = configured_protocol_label(protocol);
                            let connection_span = tracing::info_span!(
                                "http.server.connection",
                                network.protocol.version = protocol_label,
                                tls.enabled = tls_acceptor.is_some(),
                                lily.outcome = tracing::field::Empty,
                                lily.shutdown_category = tracing::field::Empty,
                            );
                            connection_tasks.spawn_with_receipt(
                                move |receipt| CONNECTION_TASK.scope(receipt, async move {
                                    let _connection_permit = connection_permit;
                                    let _connection_metric = ActiveConnectionMetricGuard::new(
                                        Arc::clone(&telemetry),
                                        protocol_label,
                                    );
                                    // HTTP responses are commonly split into small header/body
                                    // writes. Disable Nagle before either TLS or HTTP starts.
                                    stream.set_nodelay(true)?;
                                    let runtime = HttpConnectionRuntime {
                                        service,
                                        transport,
                                        request_admission,
                                        telemetry: Arc::clone(&telemetry),
                                        cors_services,
                                    };
                                    if let Some(acceptor) = tls_acceptor {
                                        let stream = match Self::accept_tls(
                                            stream,
                                            acceptor,
                                            protocol,
                                            runtime.transport.header_read_timeout,
                                            &connection_shutdown,
                                            &telemetry,
                                        )
                                        .await?
                                        {
                                            Some(stream) => stream,
                                            None => return Ok(ConnectionCloseReason::GracefulShutdown),
                                        };
                                        Self::serve_connection(
                                            stream,
                                            socket,
                                            runtime,
                                            connection_shutdown,
                                        )
                                        .await
                                    } else {
                                        Self::serve_connection(
                                            stream,
                                            socket,
                                            runtime,
                                            connection_shutdown,
                                        )
                                        .await
                                    }
                                }
                                .instrument(connection_span)),
                            );
                        }
                        Err(error) => {
                            debug_log!("❌ Accept error: {}", error);
                            match accept_failures.record_failure() {
                                AcceptFailureAction::RetryAfter(backoff) => {
                                    // A transient descriptor/resource failure is
                                    // retried with bounded exponential backoff.
                                    tokio::select! {
                                        biased;
                                        _ = shutdown.cancelled() => break,
                                        _ = force.cancelled() => break,
                                        _ = tokio::time::sleep(backoff) => {}
                                    }
                                }
                                AcceptFailureAction::Stop { consecutive_failures } => {
                                    terminal_accept_failure = Some((error.kind(), consecutive_failures));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Dropping the listener before draining guarantees that no new
        // connections enter after the composition root begins shutdown.
        drop(listener);
        budget.begin();
        task_inventory.connections.seal();
        Self::drain_connection_tasks_before(
            &mut connection_tasks,
            &budget,
            &force,
            &mut report,
            Some(service.request_registry()),
        )
        .await;
        task_inventory.protocol.seal();
        task_inventory.protocol.abort_all();
        let _ = budget
            .wait_for_receipt(ShutdownStage::Reconcile, task_inventory.protocol.wait())
            .await;
        telemetry.connection_tasks_finished(&report);
        if report.forced > 0 {
            telemetry.forced_drains.add(
                u64::try_from(report.forced).unwrap_or(u64::MAX),
                &[KeyValue::new("lily.shutdown_category", "deadline_or_force")],
            );
        } else {
            telemetry.graceful_drains.add(
                u64::try_from(report.graceful_shutdown).unwrap_or(u64::MAX),
                &[KeyValue::new("lily.shutdown_category", "graceful")],
            );
        }

        if let Some((kind, consecutive_failures)) = terminal_accept_failure {
            return Err(io::Error::new(
                kind,
                AcceptLoopFailure {
                    consecutive_failures,
                    report,
                },
            ));
        }
        Ok(report)
    }

    fn tls_acceptor(config: &RustlsConfig, protocol: HttpProtocol) -> TlsAcceptor {
        let mut config = (*config.server_config()).clone();
        config.alpn_protocols = Self::alpn_protocols(protocol);
        TlsAcceptor::from(Arc::new(config))
    }

    fn alpn_protocols(protocol: HttpProtocol) -> Vec<Vec<u8>> {
        match protocol {
            HttpProtocol::Http1_1 => vec![b"http/1.1".to_vec()],
            HttpProtocol::Http2 => vec![b"h2".to_vec()],
            HttpProtocol::Auto => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
        }
    }

    async fn accept_tls<I>(
        stream: I,
        acceptor: TlsAcceptor,
        protocol: HttpProtocol,
        handshake_timeout: Duration,
        shutdown: &CancellationToken,
        telemetry: &HttpServerTelemetry,
    ) -> io::Result<Option<TlsStream<I>>>
    where
        I: AsyncRead + AsyncWrite + Unpin,
    {
        let handshake = tokio::time::timeout(handshake_timeout, acceptor.accept(stream));
        let tls_stream = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                telemetry.tls_handshake_finished("cancelled");
                return Ok(None);
            }
            result = handshake => match result {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    telemetry.tls_handshake_finished("failed");
                    let kind = error.kind();
                    return Err(io::Error::new(
                        kind,
                        TlsHandshakeFailure::Negotiation(error),
                    ));
                }
                Err(_) => {
                    telemetry.tls_handshake_finished("timeout");
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        TlsHandshakeFailure::TimedOut,
                    ));
                }
            }
        };

        if protocol == HttpProtocol::Http2
            && tls_stream.get_ref().1.alpn_protocol() != Some(b"h2".as_slice())
        {
            telemetry.tls_handshake_finished("alpn_rejected");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                TlsHandshakeFailure::Http2AlpnRequired,
            ));
        }
        telemetry.tls_handshake_finished("succeeded");
        Ok(Some(tls_stream))
    }

    fn compile_cors_dispatch_service(
        service: &Arc<App>,
        transport: &HttpTransportConfig,
    ) -> Option<SharedCorsDispatchServices> {
        service.cors_adapters().map(|adapters| {
            let transport = Arc::new(transport.clone());
            Arc::new(
                adapters
                    .iter()
                    .map(|adapter| {
                        Arc::new(Mutex::new(adapter.layer(CorsDispatchService {
                            service: Arc::clone(service),
                            transport: Arc::clone(&transport),
                        })))
                    })
                    .collect(),
            )
        })
    }

    async fn serve_connection<I>(
        stream: I,
        peer: SocketAddr,
        runtime: HttpConnectionRuntime,
        shutdown: CancellationToken,
    ) -> io::Result<ConnectionCloseReason>
    where
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let _keep_alive = runtime.service.clone();
        let protocol = runtime.service.protocol();
        let transport = Arc::new(runtime.transport);
        let request_transport = Arc::clone(&transport);
        let activity = ConnectionActivity::new();
        let peer_ip = peer.ip();
        let budget = runtime.service.shutdown_budget().clone();
        let protocol_owner = ConnectionProtocolOwner {
            tasks: TaskRegistry::default(),
        };
        let protocol_tasks = protocol_owner.tasks.clone();
        let executor = TrackedHttpExecutor {
            local: protocol_tasks.clone(),
            parent: runtime.service.task_inventory().protocol.clone(),
            keep_alive: runtime.service.clone(),
        };
        let builder = Self::connection_builder_with_executor(&transport, protocol, executor);
        let request_runtime = HttpRequestRuntime {
            service: runtime.service,
            transport: request_transport,
            request_admission: runtime.request_admission,
            peer_ip: Some(peer_ip),
            telemetry: runtime.telemetry,
            cors_services: runtime.cors_services,
        };
        let result = if request_runtime.service.cors_adapters().is_some() {
            assert!(
                request_runtime.cors_services.is_some(),
                "built application has compiled CORS services"
            );
            let tower_service = CorsTransportService {
                runtime: request_runtime,
                activity: Arc::clone(&activity),
            };
            Self::drive_connection(
                stream,
                builder,
                TowerToHyperService::new(tower_service),
                shutdown,
                Arc::clone(&activity),
                transport.connection_idle_timeout,
            )
            .await
        } else {
            let request_activity = Arc::clone(&activity);
            let request_runtime = request_runtime;
            let http_service = service_fn(move |request| {
                let request_guard = request_activity.enter();
                Self::serve_request::<false>(request, request_runtime.clone(), request_guard)
            });
            Self::drive_connection(
                stream,
                builder,
                http_service,
                shutdown,
                Arc::clone(&activity),
                transport.connection_idle_timeout,
            )
            .await
        };
        // The connection driver is terminal. No scoped protocol child may
        // escape simply because the peer closed/reset its transport.
        drop(protocol_owner);
        let joined = budget
            .wait_for_receipt(ShutdownStage::Reconcile, protocol_tasks.wait())
            .await;
        if joined.is_err() || protocol_tasks.snapshot().panicked != 0 {
            return Err(io::Error::other(
                "HTTP protocol task termination was incomplete or panicked",
            ));
        }
        let requests = activity.snapshot();
        let response_transports = activity.transport_snapshot();
        tracing::debug!(
            started = requests.started,
            completed = requests.completed,
            cancelled = requests.cancelled,
            timed_out = requests.timed_out,
            active = requests.active,
            response_transports = ?response_transports,
            "HTTP connection request-task ledger reconciled"
        );
        result
    }

    pub(super) async fn drive_connection<I, S, B, E>(
        stream: I,
        builder: auto::Builder<E>,
        http_service: S,
        shutdown: CancellationToken,
        activity: Arc<ConnectionActivity>,
        idle_timeout: Duration,
    ) -> io::Result<ConnectionCloseReason>
    where
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        S: Service<HyperRequest<Incoming>, Response = HyperResponse<B>> + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>>,
        B: Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
        E: auto::HttpServerConnExec<S::Future, B> + Clone,
    {
        let result = async {
        let stream = ResponseIo { inner: stream, activity: activity.clone() };
        let connection = builder.serve_connection(TokioIo::new(stream), http_service);
        tokio::pin!(connection);
        let idle = activity.wait_until_idle(idle_timeout);
        tokio::pin!(idle);
        let response_stop = activity.wait_until_response_stop();
        tokio::pin!(response_stop);

        tokio::select! {
            _ = activity.connection.stop.cancelled() => Ok(activity.connection.close_reason()),
            result = &mut connection => {
                if result.is_err() { activity.transport_failed(); }
                result
                    .map(|()| ConnectionCloseReason::PeerClosed)
                    .map_err(io::Error::other)
            },
            _ = shutdown.cancelled() => {
                // Hyper sends GOAWAY for HTTP/2 and disables HTTP/1 keep-alive.
                connection.as_mut().graceful_shutdown();
                match tokio::select! {
                    _ = activity.connection.stop.cancelled() => return Ok(activity.connection.close_reason()),
                    result = &mut connection => result,
                    _ = &mut response_stop => {
                        activity.transport_failed();
                        return Ok(activity.response_stop_close_reason());
                    },
                } {
                    Ok(()) => Ok(ConnectionCloseReason::GracefulShutdown),
                    Err(error) if Self::is_expected_peer_disconnect(error.as_ref()) => {
                        Ok(ConnectionCloseReason::GracefulShutdown)
                    }
                    Err(error) => Err(io::Error::other(error)),
                }
            }
            _ = &mut idle => {
                // There are no application-owned stream futures at this
                // point. Hyper still emits GOAWAY/connection-close so the
                // peer receives a protocol-correct terminal signal.
                connection.as_mut().graceful_shutdown();
                match tokio::select! {
                    _ = activity.connection.stop.cancelled() => return Ok(activity.connection.close_reason()),
                    result = &mut connection => result,
                    _ = &mut response_stop => {
                        activity.transport_failed();
                        return Ok(activity.response_stop_close_reason());
                    },
                } {
                    Ok(()) => Ok(ConnectionCloseReason::IdleTimeout),
                    Err(error) if Self::is_expected_peer_disconnect(error.as_ref()) => {
                        Ok(ConnectionCloseReason::IdleTimeout)
                    }
                    Err(error) => Err(io::Error::other(error)),
                }
            }
            _ = &mut response_stop => {
                activity.transport_failed();
                // Bound H2 requests are stopped inside the watchdog through
                // their exact worker. Only HTTP/1 or an unbound conformance
                // service reaches this connection-wide fallback.
                Ok(activity.response_stop_close_reason())
            }
        }
        }.await;
        // The inline Hyper future and its I/O have actually been destroyed.
        // Managed connections still require the enclosing task's real join.
        activity.connection.release();
        activity.changed.notify_waiters();
        result
    }

    fn is_expected_peer_disconnect(error: &(dyn StdError + 'static)) -> bool {
        let mut current = Some(error);
        while let Some(source) = current {
            if let Some(error) = source.downcast_ref::<io::Error>() {
                return matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::NotConnected
                        | io::ErrorKind::UnexpectedEof
                );
            }
            current = source.source();
        }
        false
    }

    /// Build the exact Hyper codec configuration used by every accepted
    /// connection. Keeping this construction in one place lets socketless
    /// conformance tests exercise the production HTTP/1 parser and encoder.
    #[doc(hidden)]
    pub fn connection_builder(
        transport: &HttpTransportConfig,
        protocol: HttpProtocol,
    ) -> auto::Builder<TokioExecutor> {
        Self::connection_builder_with_executor(transport, protocol, TokioExecutor::new())
    }

    fn connection_builder_with_executor<E: Clone>(
        transport: &HttpTransportConfig,
        protocol: HttpProtocol,
        executor: E,
    ) -> auto::Builder<E> {
        let mut builder = auto::Builder::new(executor);
        builder
            .http1()
            .timer(TokioTimer::new())
            .header_read_timeout(transport.header_read_timeout)
            .max_headers(transport.max_request_header_count)
            .max_buf_size(transport.max_request_header_bytes.max(8192))
            .keep_alive(true);
        builder
            .http2()
            .timer(TokioTimer::new())
            .max_concurrent_streams(transport.http2_max_concurrent_streams)
            .initial_stream_window_size(transport.http2_initial_stream_window_bytes)
            .initial_connection_window_size(transport.http2_initial_connection_window_bytes)
            .max_frame_size(transport.http2_max_frame_bytes)
            .max_header_list_size(transport.max_request_header_bytes as u32)
            .max_send_buf_size(transport.http2_max_send_buffer_bytes)
            .max_pending_accept_reset_streams(transport.http2_max_pending_accept_reset_streams)
            .max_local_error_reset_streams(transport.http2_max_local_error_reset_streams)
            .keep_alive_interval(transport.http2_keep_alive_interval)
            .keep_alive_timeout(transport.http2_keep_alive_timeout);

        match protocol {
            HttpProtocol::Http1_1 => builder.http1_only(),
            HttpProtocol::Http2 => builder.http2_only(),
            HttpProtocol::Auto => builder,
        }
    }

    async fn serve_request<const CORS_ENABLED: bool>(
        request: HyperRequest<Incoming>,
        runtime: HttpRequestRuntime,
        mut request_guard: RequestTaskGuard,
    ) -> Result<HyperResponse<BoundedResponseBody>, ResponseStopped> {
        let registry = runtime.service.request_registry().clone();
        let version = request.version();
        let transport = request_guard.bind_transport(version);
        transport.bind_root(runtime.service.shutdown_budget().clone());
        let owner_transport = transport.clone();
        let waiter = registry.spawn(runtime.service.clone(), move |owner| {
            owner.bind_response_transport(owner_transport);
            Self::serve_owned_request::<CORS_ENABLED>(request, runtime, request_guard, owner)
        });
        let result = match waiter {
            Ok(waiter) => waiter.handoff().await,
            Err(error) => Err(error),
        };
        let response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(never)) => match never {},
            Err(error) => {
                // No response was handed to Hyper. The original request guard
                // belongs to the failed/unstarted owner; do not enter it twice.
                let denied = matches!(
                    error,
                    crate::request_lifecycle::RequestOwnerError::AdmissionClosed
                );
                if !denied {
                    tracing::error!("HTTP request lifecycle owner did not return a response");
                }
                // Even a minimal rejected/failed response has a controlled
                // header write. No request owner exists to arm it in this path.
                transport.begin_finalization();
                let mut response = HyperResponse::builder()
                    .status(if denied {
                        StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR
                    })
                    .body(BoundedResponseBody {
                        source: BoundedResponseSource::Full(Bytes::new()),
                        max_frame_bytes: 1,
                        request_guard: None,
                        transport: Some(transport.clone()),
                        eof_released: false,
                    })
                    .expect("static minimal response is valid");
                if denied && version != Version::HTTP_2 {
                    response
                        .headers_mut()
                        .insert(http::header::CONNECTION, HeaderValue::from_static("close"));
                }
                transport.frames_finished(ResponseFrames::Completed);
                response
            }
        };
        Self::commit_response(response, &transport)
    }

    /// Make the final selection after owner handoff, including time spent
    /// waiting for this service future to be scheduled by the protocol worker.
    fn commit_response(
        mut response: HyperResponse<BoundedResponseBody>,
        transport: &ResponseTransportControl,
    ) -> Result<HyperResponse<BoundedResponseBody>, ResponseStopped> {
        match transport.commit() {
            Ok(()) => {}
            Err(ResponseStopped::FallbackRequired(status)) => {
                if response.status() != status
                    || !matches!(response.body().source, BoundedResponseSource::Full(_))
                {
                    *response.status_mut() = status;
                    // Keep the already resolved CORS decision; never rerun
                    // user origin predicates or replay application middleware.
                    let cors_headers = response
                        .headers()
                        .iter()
                        .filter(|(name, _)| {
                            name.as_str().starts_with("access-control-")
                                || *name == http::header::VARY
                        })
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect::<Vec<_>>();
                    response.headers_mut().clear();
                    for (name, value) in cors_headers {
                        response.headers_mut().append(name, value);
                    }
                    response.body_mut().replace_with_empty(status);
                }
                transport.commit_fallback()?;
            }
            Err(error) => return Err(error),
        }
        Ok(response)
    }

    async fn serve_owned_request<const CORS_ENABLED: bool>(
        mut request: HyperRequest<Incoming>,
        runtime: HttpRequestRuntime,
        mut request_guard: RequestTaskGuard,
        owner: RequestExecutionContext,
    ) -> Result<HyperResponse<BoundedResponseBody>, Infallible> {
        let HttpRequestRuntime {
            service,
            transport,
            request_admission,
            peer_ip,
            telemetry,
            cors_services,
        } = runtime;
        let (cors_adapter, cors_service, mut cors_request_kind, mut cors_request) = if CORS_ENABLED
        {
            let request_kind = Some(CorsLayerAdapter::classify_request(&mut request));
            let target_is_bounded = request
                .uri()
                .path_and_query()
                .map_or(1, |target| target.as_str().len())
                <= transport.max_uri_bytes;
            let plan_id = match request_kind {
                Some(Ok(CorsRequestKind::Simple)) if target_is_bounded => {
                    service.cors_plan_id(request.method().as_str(), request.uri().path())
                }
                Some(Ok(CorsRequestKind::Preflight)) if target_is_bounded => request
                    .headers()
                    .get(http::header::ACCESS_CONTROL_REQUEST_METHOD)
                    .and_then(|method| method.to_str().ok())
                    .and_then(|method| service.cors_plan_id(method, request.uri().path())),
                Some(Ok(
                    CorsRequestKind::NonCors | CorsRequestKind::Simple | CorsRequestKind::Preflight,
                ))
                | Some(Err(_))
                | None => service.cors_fallback_plan_id(),
            }
            .expect("CORS-enabled transport has one selected policy plan");
            let adapter = service
                .cors_adapter(plan_id)
                .expect("selected CORS plan has one compiled adapter")
                .clone();
            let cors_service = cors_services
                .as_ref()
                .and_then(|services| services.get(plan_id))
                .cloned()
                .expect("selected CORS plan has one compiled transport service");
            let request_head = Some(Self::cors_request_head(&request, request_kind));
            (
                Some(adapter),
                Some(cors_service),
                request_kind,
                request_head,
            )
        } else {
            (None, None, None, None)
        };
        let protocol = protocol_version_label(request.version());
        let method = request.method().as_str().to_string();
        let method_metric = http_method_metric_label(&method);
        let remote_parent = lily_trace::extract_context(&HyperRequestTraceExtractor(&request));
        let request_span = tracing::info_span!(
            "http.server.request",
            otel.kind = "server",
            http.request.method = %method,
            http.route = tracing::field::Empty,
            network.protocol.version = protocol,
            http.response.status_code = tracing::field::Empty,
            http.request.body.size = tracing::field::Empty,
            http.response.body.size = tracing::field::Empty,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            lily.timeout_category = tracing::field::Empty,
            lily.application_error = tracing::field::Empty,
            lily.application_error_code = tracing::field::Empty,
            lily.application_outcome = tracing::field::Empty,
            lily.cancellation_category = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        lily_trace::set_parent(&request_span, remote_parent);
        request_guard.observe(
            request_span.clone(),
            Arc::clone(&telemetry),
            protocol,
            method_metric,
        );
        let request_byte_counter = request_guard.request_byte_counter();
        let terminal_span = request_span.clone();

        async move {
            let admission_started = Instant::now();
            let request_permit =
                owner.admit(&service, request_admission, transport.request_timeout);
            let admission_wait = admission_started.elapsed();
            let admission_result = if request_permit.is_ok() {
                "accepted"
            } else {
                "rejected"
            };
            telemetry.admission_wait.record(
                admission_wait.as_secs_f64(),
                &[
                    KeyValue::new("lily.admission", "request"),
                    KeyValue::new("lily.outcome", admission_result),
                ],
            );
            tracing::event!(
                name: "http.server.admission",
                tracing::Level::INFO,
                lily.outcome = %admission_result,
                lily.wait_ms = admission_wait.as_secs_f64() * 1_000.0,
                "HTTP request admission decision"
            );

            match request_permit {
                Ok(()) => {}
                Err(reason) => {
                    if let Some(control) = owner.response_transport() {
                        control.begin_finalization();
                    }
                    telemetry
                        .admission_rejections
                        .add(1, &[KeyValue::new("lily.admission", "request")]);
                    let mut response = Self::error_response(HttpApiError::ServiceUnavailable(
                        match reason {
                            RequestAdmissionError::Shutdown => "HTTP shutdown admission is closed",
                            RequestAdmissionError::Capacity => {
                                "HTTP request admission limit reached"
                            }
                        }
                        .to_string(),
                    ));
                    response
                        .headers_mut()
                        .insert(http::header::RETRY_AFTER, HeaderValue::from_static("1"));
                    if CORS_ENABLED {
                        Self::decorate_cors_outer_response(
                            cors_adapter.as_deref(),
                            cors_request.as_ref(),
                            &mut response,
                        );
                    }
                    let (response, cors_limit_error) = if CORS_ENABLED {
                        Self::finalize_cors_transport_response(
                            response,
                            cors_adapter.as_deref(),
                            cors_request.as_ref(),
                            &transport,
                        )
                    } else {
                        (response, None)
                    };
                    request_guard.record_response(
                        response.status().as_u16(),
                        response.body().buffered_length().unwrap_or(0),
                        if cors_limit_error.is_some() {
                            "error"
                        } else {
                            "rejected"
                        },
                        cors_limit_error.or(Some("ADMISSION_REJECTED")),
                    );
                    return Ok(Self::bound_response(response, &transport, request_guard).await);
                }
            };

            let dispatch = async {
                request.extensions_mut().insert(owner.cancellation());
                if CORS_ENABLED {
                    let dynamic_origin = cors_adapter
                        .as_deref()
                        .is_some_and(CorsLayerAdapter::has_dynamic_origin);
                    if matches!(
                        cors_request_kind,
                        Some(Ok(CorsRequestKind::Preflight)) | Some(Err(_))
                    ) || (dynamic_origin
                        && cors_request_kind == Some(Ok(CorsRequestKind::Simple)))
                    {
                        Self::validate_request_target(&request, &transport)?;
                        Self::validate_request_header_map(
                            request.headers(),
                            transport.max_request_header_count,
                            transport.max_request_header_bytes,
                        )?;
                    }
                    if cors_request_kind == Some(Ok(CorsRequestKind::Preflight)) {
                        Self::validate_cors_preflight_body(
                            &mut request,
                            request_byte_counter.as_ref(),
                        )
                        .await?;
                    }
                    if dynamic_origin
                        && matches!(
                            cors_request_kind,
                            Some(Ok(CorsRequestKind::Simple | CorsRequestKind::Preflight))
                        )
                    {
                        let adapter = cors_adapter.as_deref().expect("CORS adapter is present");
                        cors_request_kind = Some(adapter.resolve_origin(&mut request).await);
                        cors_request = Some(Self::cors_request_head(&request, cors_request_kind));
                    }
                    if matches!(
                        cors_request_kind,
                        Some(Ok(CorsRequestKind::NonCors | CorsRequestKind::Simple))
                    ) {
                        request.extensions_mut().insert(CorsDispatchContext {
                            peer_ip,
                            observed_request_bytes: request_byte_counter,
                            owner: owner.clone(),
                        });
                    }
                    let cors_service = cors_service.as_ref().ok_or_else(|| {
                        HttpApiError::StateError(
                            "compiled CORS transport service is unavailable".to_string(),
                        )
                    })?;
                    let future = {
                        let mut service = cors_service.lock().map_err(|_| {
                            HttpApiError::StateError(
                                "compiled CORS transport service is unavailable".to_string(),
                            )
                        })?;
                        TowerService::call(&mut *service, request)
                    };
                    future.await
                } else {
                    Self::dispatch_request(
                        request,
                        service,
                        &transport,
                        peer_ip,
                        request_byte_counter,
                        owner.clone(),
                    )
                    .await
                }
            };

            // Cancellation is a request to finish, not a replacement result.
            // Normal returns (including application errors) keep their result
            // throughout the cooperative window, regardless of stop cause.
            let dispatched = owner.execute(dispatch).await;
            let (mut response, outcome, error_code) = match dispatched {
                Ok(Ok(response)) => {
                    let cors_rejection = if CORS_ENABLED {
                        response
                            .extensions()
                            .get::<CorsRuntimeRejection>()
                            .map(|rejection| rejection.diagnostic_code())
                    } else {
                        None
                    };
                    let (outcome, error_code) = if let Some(code) = cors_rejection {
                        (
                            crate::telemetry::http_status_outcome(response.status().as_u16()),
                            Some(code),
                        )
                    } else {
                        Self::application_response_outcome(&response)
                    };
                    (response, outcome, error_code)
                }
                Ok(Err(error)) => {
                    let error_code = error.error_code();
                    let mut response = Self::error_response(error);
                    if CORS_ENABLED {
                        Self::decorate_cors_outer_response(
                            cors_adapter.as_deref(),
                            cors_request.as_ref(),
                            &mut response,
                        );
                    }
                    let outcome = crate::telemetry::http_status_outcome(response.status().as_u16());
                    (response, outcome, Some(error_code))
                }
                Err(ExecutionInterrupted::TimedOut) => {
                    terminal_span.record("lily.timeout_category", "request_deadline");
                    let mut response = Self::error_response(HttpApiError::GatewayTimeout(
                        "request deadline exceeded".to_string(),
                    ));
                    if CORS_ENABLED {
                        Self::decorate_cors_outer_response(
                            cors_adapter.as_deref(),
                            cors_request.as_ref(),
                            &mut response,
                        );
                    }
                    (response, "timeout", Some("GATEWAY_TIMEOUT"))
                }
                Err(interruption) => {
                    let error = match interruption {
                        ExecutionInterrupted::Stopped => HttpApiError::ServiceUnavailable(
                            "HTTP request execution was interrupted".into(),
                        ),
                        _ => HttpApiError::InternalError("HTTP request execution failed".into()),
                    };
                    let code = error.error_code();
                    let mut response = Self::error_response(error);
                    if CORS_ENABLED {
                        Self::decorate_cors_outer_response(
                            cors_adapter.as_deref(),
                            cors_request.as_ref(),
                            &mut response,
                        );
                    }
                    (response, "error", Some(code))
                }
            };
            if let Some(application) = response.extensions().get::<AppCallOutcome>().copied() {
                request_guard.record_application(application);
            }
            let (bounded_response, cors_limit_error) = if CORS_ENABLED {
                Self::finalize_cors_transport_response(
                    response,
                    cors_adapter.as_deref(),
                    cors_request.as_ref(),
                    &transport,
                )
            } else {
                (response, None)
            };
            response = bounded_response;
            let (outcome, error_code) = if let Some(error_code) = cors_limit_error {
                ("error", Some(error_code))
            } else {
                (outcome, error_code)
            };
            request_guard.record_response(
                response.status().as_u16(),
                response.body().buffered_length().unwrap_or(0),
                outcome,
                error_code,
            );

            Ok(Self::bound_response(response, &transport, request_guard)
                .await
                .map(|body| body.with_owner(&owner)))
        }
        .instrument(request_span)
        .await
    }

    fn cors_request_head(
        request: &HyperRequest<Incoming>,
        request_kind: Option<Result<CorsRequestKind, CorsRuntimeRejection>>,
    ) -> HyperRequest<()> {
        let mut head = HyperRequest::builder()
            .method(request.method().clone())
            .version(request.version())
            .body(())
            .expect("an already parsed HTTP request has valid metadata");
        if request_kind.is_some_and(|kind| kind.is_ok()) {
            for name in [
                http::header::ORIGIN,
                http::header::ACCESS_CONTROL_REQUEST_METHOD,
                http::header::ACCESS_CONTROL_REQUEST_HEADERS,
                http::header::HeaderName::from_static("access-control-request-private-network"),
            ] {
                if request.headers().get_all(&name).iter().count() == 1 {
                    if let Some(value) = request.headers().get(&name) {
                        head.headers_mut().insert(name, value.clone());
                    }
                }
            }
        }
        CorsLayerAdapter::copy_request_state(request, &mut head);
        head
    }

    fn decorate_cors_outer_response<Body>(
        adapter: Option<&CorsLayerAdapter>,
        request: Option<&HyperRequest<()>>,
        response: &mut HyperResponse<Body>,
    ) {
        if let (Some(adapter), Some(request)) = (adapter, request) {
            adapter.decorate_response(request, response);
        }
    }

    async fn validate_cors_preflight_body(
        request: &mut HyperRequest<Incoming>,
        observed_request_bytes: Option<&Arc<AtomicU64>>,
    ) -> Result<(), HttpApiError> {
        if request
            .headers()
            .contains_key(http::header::TRANSFER_ENCODING)
        {
            return Err(HttpApiError::InvalidRequestBody(
                "CORS preflight request body must be empty".to_string(),
            ));
        }
        for value in request.headers().get_all(http::header::CONTENT_LENGTH) {
            if value.as_bytes() != b"0" {
                return Err(HttpApiError::InvalidRequestBody(
                    "CORS preflight request body must be empty".to_string(),
                ));
            }
        }
        if request.body().size_hint().lower() != 0 {
            return Err(HttpApiError::InvalidRequestBody(
                "CORS preflight request body must be empty".to_string(),
            ));
        }
        while let Some(frame) = request.body_mut().frame().await {
            let frame = frame.map_err(|_| {
                HttpApiError::InvalidRequestBody(
                    "CORS preflight request body could not be read".to_string(),
                )
            })?;
            if let Some(data) = frame.data_ref() {
                if let Some(observed) = observed_request_bytes {
                    observed.fetch_add(data.len() as u64, Ordering::Relaxed);
                }
                if !data.is_empty() {
                    return Err(HttpApiError::InvalidRequestBody(
                        "CORS preflight request body must be empty".to_string(),
                    ));
                }
            }
            if frame.trailers_ref().is_some() {
                return Err(HttpApiError::InvalidRequestBody(
                    "CORS preflight request trailers are not accepted".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn finalize_cors_transport_response(
        mut response: HyperResponse<TransportResponseBody>,
        adapter: Option<&CorsLayerAdapter>,
        request: Option<&HyperRequest<()>>,
        transport: &HttpTransportConfig,
    ) -> (HyperResponse<TransportResponseBody>, Option<&'static str>) {
        let (Some(adapter), Some(request)) = (adapter, request) else {
            return (response, None);
        };

        response
            .headers_mut()
            .insert(http::header::SERVER, HeaderValue::from_static("lily"));
        if Self::transport_response_is_bounded(&response, transport) {
            return (response, None);
        }

        let mut fallback = Self::error_response(HttpApiError::ResponseEncodingError(
            "CORS response exceeded the configured transport limits".to_string(),
        ));
        adapter.decorate_response(request, &mut fallback);
        if Self::transport_response_is_bounded(&fallback, transport) {
            return (fallback, Some("CORS_RESPONSE_LIMIT_EXCEEDED"));
        }

        // Extremely small configured header/body limits may not fit even the
        // canonical JSON error. A headerless empty 500 remains bounded and
        // fail-closed for browsers because it grants no CORS capability.
        let minimal = HyperResponse::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(lily_web_core::TransportResponseBody::empty())
            .expect("static minimal response is valid");
        (minimal, Some("CORS_RESPONSE_LIMIT_EXCEEDED"))
    }

    fn transport_response_is_bounded(
        response: &HyperResponse<TransportResponseBody>,
        transport: &HttpTransportConfig,
    ) -> bool {
        let application_header_count = response
            .headers()
            .iter()
            .filter(|(name, _)| !Self::is_transport_controlled_response_header(name.as_str()))
            .count();
        if application_header_count > transport.max_response_header_count {
            return false;
        }
        let header_bytes = response
            .headers()
            .iter()
            .try_fold(0usize, |total, (name, value)| {
                if Self::is_transport_controlled_response_header(name.as_str()) {
                    return Some(total);
                }
                total
                    .checked_add(name.as_str().len())?
                    .checked_add(2)?
                    .checked_add(value.as_bytes().len())?
                    .checked_add(2)
            });
        header_bytes.is_some_and(|bytes| bytes <= transport.max_response_header_bytes)
            && match response.body() {
                TransportResponseBody::Full(body) => {
                    body.len() <= transport.max_response_body_bytes
                }
                TransportResponseBody::Stream(_) => true,
            }
    }

    async fn bound_response(
        response: HyperResponse<TransportResponseBody>,
        transport: &HttpTransportConfig,
        request_guard: RequestTaskGuard,
    ) -> HyperResponse<BoundedResponseBody> {
        let response = if Self::transport_response_is_bounded(&response, transport) {
            response
        } else {
            let fallback = Self::error_response(HttpApiError::ResponseEncodingError(
                "response exceeded the configured transport limits".to_string(),
            ));
            if Self::transport_response_is_bounded(&fallback, transport) {
                fallback
            } else {
                HyperResponse::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(TransportResponseBody::empty())
                    .expect("static minimal response is valid")
            }
        };
        let (parts, body) = response.into_parts();
        let max_frame_bytes = (transport.http2_max_frame_bytes as usize)
            .min(transport.http2_max_send_buffer_bytes)
            .max(1);
        HyperResponse::from_parts(
            parts,
            BoundedResponseBody::new(body, max_frame_bytes, request_guard),
        )
    }

    async fn dispatch_request(
        request: HyperRequest<Incoming>,
        service: Arc<App>,
        transport: &HttpTransportConfig,
        peer_ip: Option<IpAddr>,
        observed_request_bytes: Option<Arc<AtomicU64>>,
        owner: RequestExecutionContext,
    ) -> Result<HyperResponse<TransportResponseBody>, HttpApiError> {
        Self::validate_request_target(&request, transport)?;
        let (parts, body) = request.into_parts();
        let path = parts
            .uri
            .path_and_query()
            .map(|value| value.as_str())
            .unwrap_or("/")
            .to_string();
        let mut headers = Self::convert_request_headers(
            &parts.headers,
            transport.max_request_header_count,
            transport.max_request_header_bytes,
        )?;
        let connection_info = Self::resolve_connection_info(peer_ip, &mut headers, transport)?;
        let body = BoundedRequestBody::new(
            body,
            BodyBudget::new(transport.max_request_body_bytes)
                .expect("validated HTTP transport request body limit"),
            transport.max_request_header_count,
            transport.max_request_header_bytes,
        )
        .map_err(HttpApiError::from)?
        .map(|body| body.with_observed_bytes(observed_request_bytes))
        .map(|body| owner.track_input(Box::new(body)));
        let method = parts.method.as_str().to_string();
        let is_head = parts.method == http::Method::HEAD;
        let mut request = Request::from_streaming_transport_parts(
            method,
            path,
            headers,
            body,
            transport.max_request_body_bytes,
        )?;
        request.set_multipart_limits(
            transport.max_multipart_part_bytes,
            transport.max_multipart_parts,
            transport.max_multipart_metadata_bytes,
        )?;
        request.set_connection_info(connection_info);
        let response = Response::with_limits(transport.response_limits()).await?;
        let (response, app_outcome) = service.call_with_outcome(request, response, &owner).await?;
        let mut response_parts = response.into_transport_parts()?;
        let status = StatusCode::from_u16(response_parts.status).map_err(|_| {
            HttpApiError::ResponseEncodingError(format!(
                "handler produced invalid HTTP status {}",
                response_parts.status
            ))
        })?;
        let mut output = HyperResponse::builder().status(status);
        output = output.header(http::header::SERVER, "lily");
        for (name, value) in response_parts.headers.drain(..) {
            if Self::is_transport_controlled_response_header(&name)
                || Self::is_hop_by_hop_header(&name)
            {
                continue;
            }
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                HttpApiError::ResponseEncodingError(format!(
                    "handler produced invalid response header name: {error}"
                ))
            })?;
            let value = HeaderValue::from_str(&value).map_err(|error| {
                HttpApiError::ResponseEncodingError(format!(
                    "handler produced invalid response header value: {error}"
                ))
            })?;
            output = output.header(name, value);
        }
        if is_head || status == StatusCode::NO_CONTENT || status == StatusCode::NOT_MODIFIED {
            owner.suppress_response();
        }
        let (body, head_representation_bytes) =
            Self::select_application_response_body(is_head, status, response_parts.into_body());
        if is_head {
            if let Some(representation_bytes) = head_representation_bytes {
                output = output.header(http::header::CONTENT_LENGTH, representation_bytes);
            }
        }
        let mut output = output.body(body).map_err(|error| {
            HttpApiError::ResponseEncodingError(format!("failed to build response: {error}"))
        })?;
        Self::attach_application_response_outcome(&mut output, app_outcome);
        Ok(output)
    }

    pub(super) fn convert_request_headers(
        headers: &http::HeaderMap,
        max_count: usize,
        max_bytes: usize,
    ) -> Result<Vec<RawHeader>, HttpApiError> {
        Self::validate_request_header_map(headers, max_count, max_bytes)?;
        let mut output = Vec::with_capacity(headers.len());
        for (index, (name, value)) in headers.iter().enumerate() {
            let value = value
                .to_str()
                .expect("request header map was validated before allocation");
            output.push(RawHeader {
                name: name.as_str().to_string(),
                value: value.to_string(),
                line_number: index + 1,
                raw_line: format!("{}: {}", name.as_str(), value),
            });
        }
        Ok(output)
    }

    fn validate_request_target(
        request: &HyperRequest<Incoming>,
        transport: &HttpTransportConfig,
    ) -> Result<(), HttpApiError> {
        let target_bytes = request
            .uri()
            .path_and_query()
            .map_or(1, |value| value.as_str().len());
        if target_bytes > transport.max_uri_bytes {
            return Err(HttpApiError::UriTooLong(format!(
                "request URI exceeds {} bytes",
                transport.max_uri_bytes
            )));
        }
        Ok(())
    }

    fn validate_request_header_map(
        headers: &http::HeaderMap,
        max_count: usize,
        max_bytes: usize,
    ) -> Result<(), HttpApiError> {
        if headers.len() > max_count {
            return Err(HttpApiError::InvalidHttpHeader(format!(
                "request has more than {max_count} headers"
            )));
        }
        let mut total_bytes = 0_usize;
        for (name, value) in headers.iter() {
            // HeaderMap no longer retains peer-supplied delimiter whitespace,
            // so account the canonical serialized field-line representation.
            total_bytes = total_bytes
                .checked_add(name.as_str().len())
                .and_then(|size| size.checked_add(2))
                .and_then(|size| size.checked_add(value.as_bytes().len()))
                .and_then(|size| size.checked_add(2))
                .ok_or_else(|| {
                    HttpApiError::InvalidHttpHeader("header size overflow".to_string())
                })?;
            if total_bytes > max_bytes {
                return Err(HttpApiError::InvalidHttpHeader(format!(
                    "request headers exceed {max_bytes} bytes"
                )));
            }
            value.to_str().map_err(|_| {
                HttpApiError::InvalidHttpHeader(format!(
                    "header '{}' is not valid visible text",
                    name.as_str()
                ))
            })?;
        }
        Ok(())
    }

    /// Removes all client-controlled forwarding headers and derives a typed
    /// client address only when the socket peer belongs to an explicitly
    /// trusted network. The right-to-left walk stops at the first untrusted
    /// hop, so a client cannot prepend a spoofed address to a valid chain.
    fn resolve_connection_info(
        peer_ip: Option<IpAddr>,
        headers: &mut Vec<RawHeader>,
        transport: &HttpTransportConfig,
    ) -> Result<RequestConnectionInfo, HttpApiError> {
        let forwarded_for = headers
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("x-forwarded-for"))
            .map(|header| header.value.clone())
            .collect::<Vec<_>>();
        headers.retain(|header| {
            !header.name.eq_ignore_ascii_case("x-forwarded-for")
                && !header.name.eq_ignore_ascii_case("x-real-ip")
                && !header.name.eq_ignore_ascii_case("forwarded")
                && !Self::is_reserved_edge_identity_header(&header.name)
        });

        let Some(peer_ip) = peer_ip else {
            return Ok(RequestConnectionInfo::default());
        };
        let peer_is_trusted = transport
            .trusted_proxy_cidrs
            .iter()
            .any(|network| network.contains(&peer_ip));
        if !peer_is_trusted {
            return Ok(RequestConnectionInfo::direct(peer_ip));
        }
        if forwarded_for.len() > 1 {
            return Err(HttpApiError::InvalidHttpHeader(
                "trusted proxy supplied duplicate X-Forwarded-For headers".to_string(),
            ));
        }

        let mut effective_client = peer_ip;
        if let Some(value) = forwarded_for.first() {
            let hops = value.split(',').map(str::trim).collect::<Vec<_>>();
            if hops.is_empty() || hops.len() > transport.max_forwarded_hops {
                return Err(HttpApiError::InvalidHttpHeader(
                    "trusted proxy supplied an invalid forwarded hop count".to_string(),
                ));
            }
            let mut parsed_hops = Vec::with_capacity(hops.len());
            for hop in hops {
                if hop.is_empty() {
                    return Err(HttpApiError::InvalidHttpHeader(
                        "trusted proxy supplied an invalid forwarded address".to_string(),
                    ));
                }
                parsed_hops.push(hop.parse::<IpAddr>().map_err(|_| {
                    HttpApiError::InvalidHttpHeader(
                        "trusted proxy supplied an invalid forwarded address".to_string(),
                    )
                })?);
            }

            for hop in parsed_hops.into_iter().rev() {
                let current_is_trusted = transport
                    .trusted_proxy_cidrs
                    .iter()
                    .any(|network| network.contains(&effective_client));
                if !current_is_trusted {
                    break;
                }
                effective_client = hop;
            }
        }

        Ok(RequestConnectionInfo::from_trusted_transport(
            Some(peer_ip),
            Some(effective_client),
            true,
        ))
    }

    /// V1 has no trusted edge-identity-header profile. Authentication must
    /// travel in the end-to-end Authorization credential and be verified by
    /// a Lily guard. Reserving and stripping common proxy identity names keeps
    /// application code from accidentally treating a spoofable header as a
    /// framework-authenticated principal, even when client-IP forwarding is
    /// enabled for a trusted proxy network.
    fn is_reserved_edge_identity_header(name: &str) -> bool {
        matches!(
            name.to_ascii_lowercase().as_str(),
            "x-lily-principal"
                | "x-lily-subject"
                | "x-lily-roles"
                | "x-lily-scopes"
                | "x-auth-request-user"
                | "x-auth-request-email"
                | "x-forwarded-user"
                | "x-forwarded-email"
                | "x-remote-user"
                | "x-client-cert"
                | "x-ssl-client-cert"
                | "x-ssl-client-subject-dn"
                | "x-forwarded-client-cert"
        )
    }

    #[cfg(test)]
    pub(super) async fn collect_request_body<B>(
        body: B,
        max_body_bytes: usize,
        max_trailer_count: usize,
        max_trailer_bytes: usize,
    ) -> Result<Bytes, HttpApiError>
    where
        B: Body<Data = Bytes> + Send + Sync + Unpin,
        B::Error: Send,
    {
        let lower = body.size_hint().lower().min(max_body_bytes as u64) as usize;
        let Some(mut body) = BoundedRequestBody::new(
            body,
            BodyBudget::new(max_body_bytes)
                .map_err(|error| HttpApiError::StateError(error.to_string()))?,
            max_trailer_count,
            max_trailer_bytes,
        )
        .map_err(HttpApiError::from)?
        else {
            return Ok(Bytes::new());
        };
        let mut output = BytesMut::with_capacity(lower);
        while let Some(chunk) = body.next_chunk().await.map_err(HttpApiError::from)? {
            output.extend_from_slice(&chunk);
        }
        Ok(output.freeze())
    }

    fn error_response(error: HttpApiError) -> HyperResponse<TransportResponseBody> {
        tracing::warn!(error_code = error.error_code(), "HTTP request failed");
        let (status, _) = error.http_status();
        let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = error.to_public_json().unwrap_or_else(|_| {
            b"{\"error\":true,\"code\":\"INTERNAL_ERROR\",\"message\":\"An internal server error occurred.\",\"status\":500}".to_vec()
        });
        HyperResponse::builder()
            .status(status)
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::SERVER, "lily")
            .body(TransportResponseBody::Full(Bytes::from(body)))
            .expect("static HTTP error response is valid")
    }

    #[inline]
    fn status_allows_body(status: StatusCode) -> bool {
        !status.is_informational()
            && status != StatusCode::NO_CONTENT
            && status != StatusCode::NOT_MODIFIED
    }

    fn select_application_response_body(
        is_head: bool,
        status: StatusCode,
        body: TransportResponseBody,
    ) -> (TransportResponseBody, Option<u64>) {
        let head_representation_bytes = if is_head { body.exact_length() } else { None };
        if is_head || !Self::status_allows_body(status) {
            (TransportResponseBody::empty(), head_representation_bytes)
        } else {
            (body, None)
        }
    }

    fn is_hop_by_hop_header(name: &str) -> bool {
        [
            "connection",
            "keep-alive",
            "proxy-connection",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
        ]
        .iter()
        .any(|candidate| name.eq_ignore_ascii_case(candidate))
    }

    fn is_transport_controlled_response_header(name: &str) -> bool {
        name.eq_ignore_ascii_case("content-length") || name.eq_ignore_ascii_case("server")
    }

    fn application_response_outcome<B>(
        response: &HyperResponse<B>,
    ) -> (&'static str, Option<&'static str>) {
        if let Some(outcome) = response.extensions().get::<AppCallOutcome>().copied() {
            return (outcome.outcome(), outcome.error_code());
        }

        (
            crate::telemetry::http_status_outcome(response.status().as_u16()),
            None,
        )
    }

    #[inline]
    fn attach_application_response_outcome<B>(
        response: &mut HyperResponse<B>,
        outcome: AppCallOutcome,
    ) {
        // Ordinary success keeps the allocation-free empty extension map.
        // An error value can deliberately use a success status, or middleware
        // can recover its HTTP status; retain that independent application fact.
        if outcome.error_code().is_some() || outcome.application_error() {
            response.extensions_mut().insert(outcome);
        }
    }

    #[cfg(test)]
    async fn drain_connection_tasks(
        connection_tasks: &mut TaskSet<io::Result<ConnectionCloseReason>>,
        drain_timeout: Duration,
        report: &mut ConnectionTaskReport,
    ) {
        let force = CancellationToken::new();
        Self::drain_connection_tasks_with_force(connection_tasks, drain_timeout, &force, report)
            .await;
    }

    #[cfg(test)]
    async fn drain_connection_tasks_with_force(
        connection_tasks: &mut TaskSet<io::Result<ConnectionCloseReason>>,
        drain_timeout: Duration,
        force: &CancellationToken,
        report: &mut ConnectionTaskReport,
    ) {
        let budget = ShutdownBudget::new(drain_timeout);
        budget.begin();
        Self::drain_connection_tasks_before(connection_tasks, &budget, force, report, None).await;
    }

    async fn drain_connection_tasks_before(
        connection_tasks: &mut TaskSet<io::Result<ConnectionCloseReason>>,
        budget: &ShutdownBudget,
        force: &CancellationToken,
        report: &mut ConnectionTaskReport,
        requests: Option<&RequestRegistry>,
    ) {
        if connection_tasks.is_empty() {
            return;
        }

        let graceful_drain = async {
            while let Some(result) = connection_tasks.join_next().await {
                report.record_join(result);
            }
        };

        let drained = tokio::select! {
            biased;
            _ = force.cancelled() => false,
            _ = budget.wait_for_force() => false,
            result = budget.wait_for_receipt(ShutdownStage::Graceful, graceful_drain) => result.is_ok(),
        };
        if drained {
            return;
        }

        // Signal accepted execution before transport abort. Owners poll their
        // same slots cooperatively, and retain cleanup even when S expires
        // without confirmed execution destruction.
        if let Some(requests) = requests {
            requests.cancel_executions(if force.is_cancelled() || budget.force_requested() {
                ExecutionStopReason::ForcedShutdown
            } else {
                ExecutionStopReason::GracefulDeadline
            });
            let _ = budget
                .wait_for_receipt(ShutdownStage::ExecutionStop, requests.wait_for_executions())
                .await;
            // Execution/source release is NOT response completion. In
            // particular buffered bytes can outlive their request scope, and
            // an uncommitted 504/503 still has its bounded finalization attempt.
            // Keep polling the actual connections and their request controls;
            // each response enforces its own remaining C/finalization cutoff.
            let _ = budget
                .wait_for_receipt(ShutdownStage::TransportStop, async {
                    while let Some(result) = connection_tasks.join_next().await {
                        report.record_join(result);
                    }
                })
                .await;
        }
        report.forced += connection_tasks.len();
        connection_tasks.abort_all();
        let _ = budget
            .wait_for_receipt(ShutdownStage::Reconcile, async {
                while let Some(result) = connection_tasks.join_next().await {
                    report.record_join(result);
                }
            })
            .await;
        report.outstanding = connection_tasks.len();
    }

    pub(crate) fn log_drain_report(report: &ConnectionTaskReport) {
        debug_log!(
            "HTTP connection tasks: accepted={}, rejected={}, completed={}, connection_errors={}, tls_handshake_errors={}, peer_closed={}, graceful_shutdown={}, idle_timeout={}, response_finalization_timeout={}, response_stopped={}, forced={}, cancelled={}, panicked={}",
            report.accepted,
            report.rejected,
            report.completed,
            report.connection_errors,
            report.tls_handshake_errors,
            report.peer_closed,
            report.graceful_shutdown,
            report.idle_timeout,
            report.response_finalization_timeout,
            report.response_stopped,
            report.forced,
            report.cancelled,
            report.panicked
        );
    }
}

#[cfg(test)]
#[path = "task_ownership_tests.rs"]
mod task_ownership_tests;

#[cfg(test)]
#[path = "response_control_tests.rs"]
mod response_control_tests;

#[cfg(test)]
#[path = "response_deadline_tests.rs"]
mod response_deadline_tests;

#[cfg(test)]
#[path = "shutdown_response_tests.rs"]
mod shutdown_response_tests;

#[cfg(test)]
#[path = "telemetry_tests.rs"]
mod telemetry_tests;

#[cfg(test)]
mod tests {
    use super::{
        http_method_metric_label, AcceptFailureAction, AcceptFailureBudget, AcceptLoopFailure,
        BoundedRequestBody, BoundedResponseBody, ConnectionActivity, ConnectionCloseReason,
        ConnectionTaskReport, HttpConnectionRuntime, HttpServer, HttpServerTelemetry,
        HttpTransportConfig, ManagedHttpServer, RequestTraceExtractor, TlsHandshakeFailure,
        MAX_CONSECUTIVE_ACCEPT_FAILURES,
    };
    use crate::shutdown::ShutdownBudget;
    use crate::tasks::{TaskRegistry, TaskSet};
    use crate::{
        app::{AppBuildError, AppBuilder, AppCallOutcome},
        handler::Handler,
        private::CorsRoutePolicyRegistration,
        registry::RouteInfo,
    };
    use bytes::Bytes;
    use futures::StreamExt as _;
    use http::{HeaderMap, StatusCode};
    use http_body_util::{BodyExt, Empty, Full};
    use hyper::body::{Body, Frame, SizeHint};
    use hyper::{Request as HyperRequest, Response as HyperResponse, Version};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use lily_core::RawHeader;
    use lily_injection::Extensions;
    use lily_middleware::{
        CorsDisabled, CorsOriginContext, CorsOriginResolver, CorsOriginResolverError,
        CorsOriginResolverInitError, CorsPolicy, CorsPolicyProvider, HttpExchange, HttpMiddleware,
        HttpMiddlewareError, HttpMiddlewareInitError, HttpNext, MiddlewareDescriptor,
        MiddlewareErrorCode, MiddlewareKind,
    };
    use lily_web_core::{
        sse, streaming, BodyBudget, HttpProtocol, IntoResponse, Request, RequestBodyError,
        RequestBodyState, RequestBodyStream, RequestExt, Response, ResponseBodyError,
        ResponseLimits, ResponseStreamingLimits, RustlsConfig, SseEvent, StaticFileMount,
        StreamingResponse, TransportResponseBody,
    };
    use rcgen::{
        generate_simple_self_signed, BasicConstraints, CertificateParams, CertifiedIssuer,
        ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    };
    use rustls::{
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
        server::WebPkiClientVerifier,
        ClientConfig, RootCertStore, ServerConfig,
    };
    use std::{
        collections::VecDeque,
        future::{pending, Future},
        io,
        net::{IpAddr, Ipv4Addr},
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    #[test]
    fn transport_timeout_cap_is_inclusive_and_rejects_overflowing_values() {
        let maximum = HttpTransportConfig::MAX_TIMEOUT;
        let exact = HttpTransportConfig {
            request_timeout: maximum,
            connection_idle_timeout: maximum,
            header_read_timeout: maximum,
            http2_keep_alive_interval: maximum,
            http2_keep_alive_timeout: maximum,
            ..HttpTransportConfig::default()
        };
        exact
            .validate()
            .expect("the documented inclusive timeout maximum must validate");
        assert!(Instant::now().checked_add(maximum).is_some());

        let one_over = maximum + Duration::from_nanos(1);
        for invalid in [
            HttpTransportConfig {
                request_timeout: one_over,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                connection_idle_timeout: one_over,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                header_read_timeout: one_over,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                http2_keep_alive_interval: one_over,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                http2_keep_alive_timeout: one_over,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                request_timeout: Duration::MAX,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                http2_keep_alive_timeout: Duration::MAX,
                ..HttpTransportConfig::default()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn response_write_deadline_fails_closed_when_duration_is_unrepresentable() {
        let activity = ConnectionActivity::new();
        let before = Instant::now();
        activity.begin_response_write(7, Duration::MAX);
        let after = Instant::now();
        let deadline = activity
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .write_deadlines
            .get(&7)
            .copied()
            .expect("the response write deadline must be recorded");

        assert!((before..=after).contains(&deadline));
    }

    #[test]
    fn server_metric_method_and_connection_categories_are_bounded_and_reconciled() {
        for method in [
            "CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "TRACE",
        ] {
            assert_eq!(http_method_metric_label(method), method);
        }
        assert_eq!(
            http_method_metric_label("PRIVATE-METHOD-SENTINEL"),
            "_OTHER"
        );

        let report = ConnectionTaskReport {
            accepted: 8,
            completed: 4,
            peer_closed: 1,
            graceful_shutdown: 1,
            idle_timeout: 1,
            response_finalization_timeout: 1,
            connection_errors: 2,
            tls_handshake_errors: 1,
            cancelled: 1,
            panicked: 1,
            ..ConnectionTaskReport::default()
        };
        let categories = report.terminal_categories();
        assert_eq!(categories[4], ("tls_error", 1));
        assert_eq!(categories[5], ("transport_error", 1));
        assert_eq!(categories.iter().map(|(_, count)| count).sum::<usize>(), 8);
        assert!(report.reconciles());
    }
    use tokio::{
        io::{duplex, AsyncReadExt, AsyncWriteExt},
        sync::{Mutex, Semaphore},
        time::timeout,
    };

    use tokio_rustls::TlsConnector;
    use tokio_util::sync::CancellationToken;

    static COUNTING_MIDDLEWARE_CALLS: AtomicUsize = AtomicUsize::new(0);
    static STREAMING_TRANSPORT_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();
    static SSE_TRANSPORT_DROPPED: AtomicBool = AtomicBool::new(false);
    static SSE_LAST_EVENT_ID_OBSERVED: AtomicBool = AtomicBool::new(false);
    static STATIC_FILE_TRANSPORT_MOUNT: Mutex<Option<StaticFileMount>> = Mutex::const_new(None);

    struct SseTransportDropProbe;

    impl Drop for SseTransportDropProbe {
        fn drop(&mut self) {
            SSE_TRANSPORT_DROPPED.store(true, Ordering::Release);
        }
    }
    static DYNAMIC_CORS_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static DYNAMIC_CORS_DECISIONS: AtomicUsize = AtomicUsize::new(0);
    static ROUTED_CORS_PROVIDER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static ROUTED_CORS_SECOND_PROVIDER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static ROUTED_CORS_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static ROUTED_CORS_DECISIONS: AtomicUsize = AtomicUsize::new(0);
    static ROUTED_CORS_HANDLER_CALLS: AtomicUsize = AtomicUsize::new(0);

    struct ServerDynamicOriginResolver;
    struct FailingDynamicOriginResolver;
    struct RoutedControllerCorsPolicy;
    struct RoutedDynamicCorsPolicy;
    struct RoutedSecondDynamicCorsPolicy;
    struct RoutedDynamicOriginResolver;
    struct InvalidRoutedCorsPolicy;
    struct PanickingRoutedCorsPolicy;
    struct RoutedPlanCorsPolicy<const PLAN: u8>;

    impl CorsPolicyProvider for RoutedControllerCorsPolicy {
        fn policy() -> CorsPolicy {
            CorsPolicy::new()
                .allow_origins(["https://controller.example"])
                .allow_methods(["GET"])
        }
    }

    impl CorsPolicyProvider for RoutedDynamicCorsPolicy {
        fn policy() -> CorsPolicy {
            ROUTED_CORS_PROVIDER_CALLS.fetch_add(1, Ordering::AcqRel);
            CorsPolicy::new()
                .resolve_origins_with::<RoutedDynamicOriginResolver>()
                .allow_methods(["GET"])
                .allow_credentials(true)
                .allow_private_network(true)
        }
    }

    impl CorsPolicyProvider for RoutedSecondDynamicCorsPolicy {
        fn policy() -> CorsPolicy {
            ROUTED_CORS_SECOND_PROVIDER_CALLS.fetch_add(1, Ordering::AcqRel);
            CorsPolicy::new()
                .resolve_origins_with::<RoutedDynamicOriginResolver>()
                .allow_methods(["GET"])
                .allow_credentials(true)
        }
    }

    impl CorsPolicyProvider for InvalidRoutedCorsPolicy {
        fn policy() -> CorsPolicy {
            CorsPolicy::new()
                .allow_origins(["*"])
                .allow_credentials(true)
        }
    }

    impl CorsPolicyProvider for PanickingRoutedCorsPolicy {
        fn policy() -> CorsPolicy {
            panic!("private provider failure")
        }
    }

    impl<const PLAN: u8> CorsPolicyProvider for RoutedPlanCorsPolicy<PLAN> {
        fn policy() -> CorsPolicy {
            CorsPolicy::new()
        }
    }

    #[async_trait::async_trait]
    impl CorsOriginResolver for RoutedDynamicOriginResolver {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, CorsOriginResolverInitError> {
            ROUTED_CORS_INITIALIZATIONS.fetch_add(1, Ordering::AcqRel);
            Ok(Self)
        }

        async fn allows(
            &self,
            context: &CorsOriginContext<'_>,
        ) -> Result<bool, CorsOriginResolverError> {
            ROUTED_CORS_DECISIONS.fetch_add(1, Ordering::AcqRel);
            Ok(context.origin() == "https://dynamic.example")
        }
    }

    fn routed_cors_handler<'a>(
        _extensions: Arc<Extensions>,
        _request: &'a mut Request,
        _response: &'a mut Response,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(), lily_error::application::http_api::HttpApiError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            ROUTED_CORS_HANDLER_CALLS.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    }

    fn routed_cors_route(
        method: &str,
        path: &str,
        cors_policy_registration: CorsRoutePolicyRegistration,
    ) -> RouteInfo {
        RouteInfo {
            method: method.to_string(),
            path: path.to_string(),
            handler: Handler::new(Arc::new(routed_cors_handler), false),
            handler_name: "routed_cors_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration,
            route_plan_id: 0,
        }
    }

    fn streaming_transport_handler<'a>(
        _extensions: Arc<Extensions>,
        request: &'a mut Request,
        response: &'a mut Response,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(), lily_error::application::http_api::HttpApiError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let source = futures::stream::once(async {
                Ok::<_, ResponseBodyError>(Bytes::from_static(b"first"))
            })
            .chain(futures::stream::once(async {
                STREAMING_TRANSPORT_RELEASE.notified().await;
                Ok(Bytes::from_static(b"second"))
            }));
            streaming(source)
                .content_type("application/x-ndjson")
                .write_to_response(response, request)
                .await?;
            Ok(())
        })
    }

    fn streaming_transport_route() -> RouteInfo {
        RouteInfo {
            method: "GET".to_string(),
            path: "/stream".to_string(),
            handler: Handler::new(Arc::new(streaming_transport_handler), false),
            handler_name: "streaming_transport_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }
    }

    fn sse_transport_handler<'a>(
        _extensions: Arc<Extensions>,
        request: &'a mut Request,
        response: &'a mut Response,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(), lily_error::application::http_api::HttpApiError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let last_event_id = request.last_event_id()?;
            SSE_LAST_EVENT_ID_OBSERVED.store(
                last_event_id.as_ref().map(|id| id.as_str()) == Some("event-41"),
                Ordering::Release,
            );
            let event = SseEvent::new("first line\nsecond line")?
                .event("notification")?
                .id("event-42")?;
            let first = futures::stream::once(async move { Ok::<_, ResponseBodyError>(event) });
            let probe = SseTransportDropProbe;
            let pending = futures::stream::poll_fn(move |_context| {
                let _probe = &probe;
                Poll::<Option<Result<SseEvent, ResponseBodyError>>>::Pending
            });
            sse(first.chain(pending))
                .write_to_response(response, request)
                .await?;
            Ok(())
        })
    }

    fn sse_transport_route() -> RouteInfo {
        RouteInfo {
            method: "GET".to_string(),
            path: "/events".to_string(),
            handler: Handler::new(Arc::new(sse_transport_handler), false),
            handler_name: "sse_transport_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }
    }

    fn static_file_transport_handler<'a>(
        _extensions: Arc<Extensions>,
        request: &'a mut Request,
        response: &'a mut Response,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<(), lily_error::application::http_api::HttpApiError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mount = STATIC_FILE_TRANSPORT_MOUNT
                .lock()
                .await
                .as_ref()
                .cloned()
                .expect("static-file transport mount must be configured");
            mount
                .serve(request)
                .await?
                .write_to_response(response, request)
                .await?;
            Ok(())
        })
    }

    fn static_file_transport_route() -> RouteInfo {
        RouteInfo {
            method: "GET".to_string(),
            path: "/assets/*path".to_string(),
            handler: Handler::new(Arc::new(static_file_transport_handler), false),
            handler_name: "static_file_transport_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }
    }

    #[async_trait::async_trait]
    impl CorsOriginResolver for ServerDynamicOriginResolver {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, CorsOriginResolverInitError> {
            DYNAMIC_CORS_INITIALIZATIONS.fetch_add(1, Ordering::AcqRel);
            Ok(Self)
        }

        async fn allows(
            &self,
            context: &CorsOriginContext<'_>,
        ) -> Result<bool, CorsOriginResolverError> {
            DYNAMIC_CORS_DECISIONS.fetch_add(1, Ordering::AcqRel);
            if context.path() == "/resolver-timeout" {
                return pending::<Result<bool, CorsOriginResolverError>>().await;
            }
            if context.path() == "/resolver-unavailable" {
                return Err(CorsOriginResolverError::unavailable(
                    MiddlewareErrorCode::new("CORS_TEST_ORIGIN_STORE_UNAVAILABLE")
                        .expect("static test code"),
                ));
            }
            Ok(context.origin() == "https://allowed.example")
        }
    }

    #[async_trait::async_trait]
    impl CorsOriginResolver for FailingDynamicOriginResolver {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, CorsOriginResolverInitError> {
            Err(CorsOriginResolverInitError::missing_configuration(
                MiddlewareErrorCode::new("CORS_TEST_ORIGIN_CONFIGURATION_MISSING")
                    .expect("static test code"),
            ))
        }

        async fn allows(
            &self,
            _context: &CorsOriginContext<'_>,
        ) -> Result<bool, CorsOriginResolverError> {
            Ok(false)
        }
    }

    struct CountingMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for CountingMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("cors_test_counter", MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            COUNTING_MIDDLEWARE_CALLS.fetch_add(1, Ordering::AcqRel);
            if exchange.request().header_value("x-test-timeout") == Some("1") {
                return pending::<Result<(), HttpMiddlewareError>>().await;
            }
            let result = next.run(exchange).await;
            for (name, value) in [
                ("Access-Control-Allow-Origin", "https://evil.example"),
                ("Access-Control-Allow-Credentials", "false"),
                ("Access-Control-Allow-Methods", "DELETE"),
                ("Access-Control-Allow-Headers", "x-evil"),
                ("Access-Control-Expose-Headers", "x-evil"),
                ("Access-Control-Max-Age", "999999"),
                ("Access-Control-Allow-Private-Network", "true"),
            ] {
                exchange
                    .response_mut()
                    .try_insert_header(name, value)
                    .expect("static test response header is valid");
            }
            result
        }
    }

    #[test]
    fn response_transport_filter_covers_controlled_and_hop_by_hop_fields() {
        for name in ["Content-Length", "content-length", "Server", "SERVER"] {
            assert!(HttpServer::is_transport_controlled_response_header(name));
        }
        for name in [
            "Connection",
            "Keep-Alive",
            "Proxy-Connection",
            "TE",
            "Trailer",
            "Transfer-Encoding",
            "Upgrade",
        ] {
            assert!(HttpServer::is_hop_by_hop_header(name));
        }
        assert!(!HttpServer::is_hop_by_hop_header("Set-Cookie"));
        assert!(!HttpServer::is_transport_controlled_response_header(
            "Content-Type"
        ));
    }

    #[test]
    fn request_and_response_header_authorities_are_independent_and_validated() {
        let transport = HttpTransportConfig {
            max_request_header_count: 1,
            max_request_header_bytes: 128,
            max_response_header_count: 65,
            max_response_header_bytes: 96 * 1024,
            max_response_body_bytes: 4096,
            max_response_stream_chunk_bytes: 2048,
            max_response_stream_total_bytes: Some(8192),
            ..HttpTransportConfig::default()
        };

        transport.validate().unwrap();
        let response_limits = transport.response_limits();
        assert_eq!(response_limits.max_header_count(), 65);
        assert_eq!(response_limits.max_header_bytes(), 96 * 1024);
        assert_eq!(response_limits.body_budget().limit_bytes(), 4096);
        assert_eq!(response_limits.streaming().max_chunk_bytes(), 2048);
        assert_eq!(response_limits.streaming().max_total_bytes(), Some(8192));

        let invalid_stream_chunk = HttpTransportConfig {
            max_response_stream_chunk_bytes: 0,
            ..HttpTransportConfig::default()
        };
        assert!(invalid_stream_chunk.validate().is_err());
        let invalid_stream_total = HttpTransportConfig {
            max_response_stream_total_bytes: Some(0),
            ..HttpTransportConfig::default()
        };
        assert!(invalid_stream_total.validate().is_err());

        let mut exact_response = HyperResponse::builder()
            .header("x-only", "v")
            .body(lily_web_core::TransportResponseBody::empty())
            .unwrap();
        exact_response
            .headers_mut()
            .insert(http::header::SERVER, http::HeaderValue::from_static("lily"));
        let exact_transport = HttpTransportConfig {
            max_response_header_count: 1,
            ..HttpTransportConfig::default()
        };
        assert!(HttpServer::transport_response_is_bounded(
            &exact_response,
            &exact_transport
        ));
        exact_response
            .headers_mut()
            .insert("x-extra", http::HeaderValue::from_static("v"));
        assert!(!HttpServer::transport_response_is_bounded(
            &exact_response,
            &exact_transport
        ));

        let mut repeated = HyperResponse::new(lily_web_core::TransportResponseBody::empty());
        repeated.headers_mut().append(
            http::header::SET_COOKIE,
            http::HeaderValue::from_static("a=1"),
        );
        repeated.headers_mut().append(
            http::header::SET_COOKIE,
            http::HeaderValue::from_static("b=2"),
        );
        assert!(!HttpServer::transport_response_is_bounded(
            &repeated,
            &exact_transport
        ));

        for invalid in [
            HttpTransportConfig {
                max_request_header_count: 0,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                max_response_header_count: 0,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                max_response_header_bytes: 1024 * 1024 + 1,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                max_request_body_bytes: 1024,
                max_multipart_part_bytes: 1025,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                max_multipart_parts: 0,
                ..HttpTransportConfig::default()
            },
            HttpTransportConfig {
                max_multipart_metadata_bytes: 1024 * 1024 + 1,
                ..HttpTransportConfig::default()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }

    #[tokio::test]
    async fn application_response_extension_preserves_bounded_terminal_error_codes() {
        for (status, code, expected_outcome) in [
            (500_u16, "HANDLER_ERROR", "error"),
            (403_u16, "TEST_MIDDLEWARE_REJECTED", "rejected"),
        ] {
            let mut lily_response = Response::new().await.unwrap();
            lily_response.status(status, "Test");
            let app_outcome = AppCallOutcome::from_final_response(&lily_response, Some(code));
            let mut response = HyperResponse::builder()
                .status(status)
                .body(Full::new(Bytes::new()))
                .unwrap();
            HttpServer::attach_application_response_outcome(&mut response, app_outcome);

            assert_eq!(
                HttpServer::application_response_outcome(&response),
                (expected_outcome, Some(code))
            );
        }

        let fallback = HyperResponse::builder()
            .status(404)
            .body(Full::new(Bytes::new()))
            .unwrap();
        assert_eq!(
            HttpServer::application_response_outcome(&fallback),
            ("rejected", None)
        );

        let mut lily_response = Response::new().await.unwrap();
        lily_response.status(200, "OK");
        let app_outcome = AppCallOutcome::from_final_response(&lily_response, None);
        let mut success = HyperResponse::builder()
            .status(200)
            .body(Full::new(Bytes::new()))
            .unwrap();
        HttpServer::attach_application_response_outcome(&mut success, app_outcome);

        assert!(success.extensions().get::<AppCallOutcome>().is_none());
        assert_eq!(
            HttpServer::application_response_outcome(&success),
            ("success", None)
        );

        // A recovered/deliberately successful HTTP status still carries the
        // application's error origin to the request span.
        let app_outcome = app_outcome.with_application_error(Some("USER_MISSING"));
        HttpServer::attach_application_response_outcome(&mut success, app_outcome);
        let retained = success
            .extensions()
            .get::<AppCallOutcome>()
            .copied()
            .unwrap();
        assert!(retained.application_error());
        assert_eq!(retained.application_error_code(), Some("USER_MISSING"));
        assert_eq!(
            HttpServer::application_response_outcome(&success),
            ("success", None)
        );
    }

    #[test]
    fn canonical_server_span_uses_route_template_and_forbids_raw_uri_or_error_text() {
        let source = include_str!("server.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source");
        for declaration in [
            "url.full =",
            "url.path =",
            "http.target =",
            "http.request.body =",
            "http.response.body =",
            "otel.status_message =",
        ] {
            assert!(
                !source.contains(declaration),
                "forbidden field: {declaration}"
            );
        }
        assert!(source.contains("\"http.server.request\""));
        assert!(source.contains("http.route = tracing::field::Empty"));
        assert!(source.contains("lily.cancellation_category"));
    }

    fn test_tls_configs(client_alpn: Vec<Vec<u8>>) -> (RustlsConfig, Arc<ClientConfig>) {
        let certified = generate_simple_self_signed(["localhost".to_owned()])
            .expect("generate test TLS identity");
        let certificate = certified.cert.der().clone();
        let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified.signing_key.serialize_der(),
        ));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("server protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], private_key)
            .expect("server identity");

        let mut roots = RootCertStore::empty();
        roots.add(certificate).expect("client trust anchor");
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("client protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = client_alpn;
        (RustlsConfig::from(server), Arc::new(client))
    }

    fn test_mtls_configs() -> (RustlsConfig, Arc<ClientConfig>, Arc<ClientConfig>) {
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let ca =
            CertifiedIssuer::self_signed(ca_params, KeyPair::generate().expect("generate CA key"))
                .expect("generate CA");

        let server_key = KeyPair::generate().expect("generate server key");
        let mut server_params =
            CertificateParams::new(["localhost".to_owned()]).expect("server parameters");
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params
            .signed_by(&server_key, &ca)
            .expect("sign server certificate");

        let client_key = KeyPair::generate().expect("generate client key");
        let mut client_params =
            CertificateParams::new(["lily-http-client".to_owned()]).expect("client parameters");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_params
            .signed_by(&client_key, &ca)
            .expect("sign client certificate");

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut client_roots = RootCertStore::empty();
        client_roots.add(ca.der().clone()).expect("client root");
        let mut authenticated_client = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("client protocol versions")
            .with_root_certificates(client_roots.clone())
            .with_client_auth_cert(
                vec![client_certificate.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(client_key.serialize_der())),
            )
            .expect("client identity");
        authenticated_client.alpn_protocols = vec![b"http/1.1".to_vec()];
        let mut anonymous_client = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("client protocol versions")
            .with_root_certificates(client_roots)
            .with_no_client_auth();
        anonymous_client.alpn_protocols = vec![b"http/1.1".to_vec()];

        let mut server_roots = RootCertStore::empty();
        server_roots.add(ca.der().clone()).expect("server root");
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(server_roots), provider)
                .build()
                .expect("required client verifier");
        let server =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("server protocol versions")
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    vec![server_certificate.der().clone()],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
                )
                .expect("server identity");

        (
            RustlsConfig::from(server),
            Arc::new(authenticated_client),
            Arc::new(anonymous_client),
        )
    }

    async fn close_test_app(app: Arc<crate::app::App>) {
        let container = Arc::clone(app.container());
        drop(app);
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("test composition root must close cleanly");
    }

    #[tokio::test]
    async fn routed_cors_provider_failures_are_bounded_build_errors() {
        let mut app = AppBuilder::new("127.0.0.1:0")
            .tls_disabled()
            .build()
            .await
            .expect("build provider validation application");

        for (registration, expected_code) in [
            (
                CorsRoutePolicyRegistration::provider::<InvalidRoutedCorsPolicy>(),
                "CORS_CREDENTIALS_WILDCARD",
            ),
            (
                CorsRoutePolicyRegistration::provider::<PanickingRoutedCorsPolicy>(),
                "CORS_POLICY_PROVIDER_PANICKED",
            ),
        ] {
            let error = app
                .replace_routes_and_cors_for_test(
                    vec![routed_cors_route("GET", "/provider", registration)],
                    None,
                )
                .await
                .expect_err("invalid provider must fail application build");
            let AppBuildError::InvalidCorsConfiguration(error) = error else {
                panic!("unexpected routed CORS provider error: {error}");
            };
            assert_eq!(error.diagnostic_code(), expected_code);
        }

        close_test_app(Arc::new(app)).await;
    }

    #[tokio::test]
    async fn routed_cors_plan_graph_is_bounded_before_provider_execution() {
        macro_rules! routes {
            ($($plan:literal),+ $(,)?) => {
                vec![$(
                    routed_cors_route(
                        "GET",
                        &format!("/plan/{plan}", plan = $plan),
                        CorsRoutePolicyRegistration::provider::<RoutedPlanCorsPolicy<$plan>>(),
                    )
                ),+]
            };
        }

        let mut app = AppBuilder::new("127.0.0.1:0")
            .tls_disabled()
            .build()
            .await
            .expect("build plan-bound application");
        let error = app
            .replace_routes_and_cors_for_test(
                routes![
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                    22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
                    42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
                    62, 63,
                ],
                None,
            )
            .await
            .expect_err("64 routed providers plus the deny fallback must exceed the plan bound");
        let AppBuildError::InvalidCorsConfiguration(error) = error else {
            panic!("unexpected routed CORS plan-bound error: {error}");
        };
        assert_eq!(error.diagnostic_code(), "CORS_POLICY_PLAN_LIMIT");

        close_test_app(Arc::new(app)).await;
    }

    #[tokio::test]
    async fn routed_cors_selects_the_effective_route_plan_and_deduplicates_dynamic_state() {
        let initial_provider_calls = ROUTED_CORS_PROVIDER_CALLS.load(Ordering::Acquire);
        let initial_second_provider_calls =
            ROUTED_CORS_SECOND_PROVIDER_CALLS.load(Ordering::Acquire);
        let initial_initializations = ROUTED_CORS_INITIALIZATIONS.load(Ordering::Acquire);
        let initial_decisions = ROUTED_CORS_DECISIONS.load(Ordering::Acquire);
        let initial_handler_calls = ROUTED_CORS_HANDLER_CALLS.load(Ordering::Acquire);

        let mut app = AppBuilder::new("127.0.0.1:0")
            .protocol(HttpProtocol::Http1_1)
            .tls_disabled()
            .build()
            .await
            .expect("build routed CORS application");
        let global_policy = CorsPolicy::new()
            .allow_origins(["https://global.example"])
            .allow_methods(["GET", "PUT"]);
        app.replace_routes_and_cors_for_test(
            vec![
                routed_cors_route(
                    "GET",
                    "/controller/:id",
                    CorsRoutePolicyRegistration::provider::<RoutedControllerCorsPolicy>(),
                ),
                routed_cors_route(
                    "GET",
                    "/action",
                    CorsRoutePolicyRegistration::provider::<RoutedDynamicCorsPolicy>(),
                ),
                routed_cors_route(
                    "GET",
                    "/action-secondary",
                    CorsRoutePolicyRegistration::provider::<RoutedSecondDynamicCorsPolicy>(),
                ),
                routed_cors_route(
                    "GET",
                    "/disabled",
                    CorsRoutePolicyRegistration::provider::<CorsDisabled>(),
                ),
                routed_cors_route("GET", "/inherit", CorsRoutePolicyRegistration::inherit()),
                routed_cors_route(
                    "OPTIONS",
                    "/ordinary",
                    CorsRoutePolicyRegistration::provider::<RoutedDynamicCorsPolicy>(),
                ),
            ],
            Some(&global_policy),
        )
        .await
        .expect("compile routed CORS plans");
        assert_eq!(
            ROUTED_CORS_PROVIDER_CALLS.load(Ordering::Acquire),
            initial_provider_calls + 1
        );
        assert_eq!(
            ROUTED_CORS_SECOND_PROVIDER_CALLS.load(Ordering::Acquire),
            initial_second_provider_calls + 1
        );
        assert_eq!(
            ROUTED_CORS_INITIALIZATIONS.load(Ordering::Acquire),
            initial_initializations + 1,
            "two providers sharing one resolver type must initialize it once"
        );

        let app = Arc::new(app);
        let transport = app.transport_config().clone();
        let cors_services = HttpServer::compile_cors_dispatch_service(&app, &transport)
            .expect("all routed CORS services are compiled at application startup");
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_app = Arc::clone(&app);
        let (server_io, client_io) = duplex(128 * 1024);
        let server_task = tokio::spawn(async move {
            HttpServer::serve_connection(
                server_io,
                "127.0.0.1:12347".parse().expect("peer address"),
                HttpConnectionRuntime {
                    service: server_app,
                    transport,
                    request_admission: Arc::new(Semaphore::new(8)),
                    telemetry: HttpServerTelemetry::new(),
                    cors_services: Some(cors_services),
                },
                server_shutdown,
            )
            .await
        });
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                .await
                .expect("routed CORS client handshake");
        let client_driver = tokio::spawn(connection);

        for (uri, origin, expected_origin) in [
            (
                "http://localhost/controller/42?expand=true",
                "https://controller.example",
                Some("https://controller.example"),
            ),
            (
                "http://localhost/action",
                "https://dynamic.example",
                Some("https://dynamic.example"),
            ),
            (
                "http://localhost/action-secondary",
                "https://dynamic.example",
                Some("https://dynamic.example"),
            ),
            ("http://localhost/inherit", "https://dynamic.example", None),
            ("http://localhost/disabled", "https://global.example", None),
            (
                "http://localhost/inherit",
                "https://global.example",
                Some("https://global.example"),
            ),
        ] {
            let response = sender
                .send_request(
                    HyperRequest::builder()
                        .method(http::Method::GET)
                        .uri(uri)
                        .header(http::header::ORIGIN, origin)
                        .body(Full::new(Bytes::new()))
                        .expect("routed CORS request"),
                )
                .await
                .expect("routed CORS response");
            assert_eq!(response.status(), 200);
            assert_eq!(
                response
                    .headers()
                    .get(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .and_then(|value| value.to_str().ok()),
                expected_origin
            );
            let _ = response.into_body().collect().await.unwrap();
        }

        let head = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::HEAD)
                    .uri("http://localhost/action")
                    .header(http::header::ORIGIN, "https://dynamic.example")
                    .body(Full::new(Bytes::new()))
                    .expect("routed CORS HEAD request"),
            )
            .await
            .expect("routed CORS HEAD response");
        assert_eq!(head.status(), 200);
        assert_eq!(
            head.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://dynamic.example"
        );
        let _ = head.into_body().collect().await.unwrap();

        let method_not_allowed = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::PUT)
                    .uri("http://localhost/action")
                    .header(http::header::ORIGIN, "https://global.example")
                    .body(Full::new(Bytes::new()))
                    .expect("routed CORS method mismatch"),
            )
            .await
            .expect("routed CORS method mismatch response");
        assert_eq!(method_not_allowed.status(), 405);
        assert_eq!(
            method_not_allowed.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://global.example"
        );
        let _ = method_not_allowed.into_body().collect().await.unwrap();

        let missing = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://global.example")
                    .body(Full::new(Bytes::new()))
                    .expect("routed CORS missing request"),
            )
            .await
            .expect("routed CORS missing response");
        assert_eq!(missing.status(), 404);
        assert_eq!(
            missing.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://global.example"
        );
        let _ = missing.into_body().collect().await.unwrap();

        let calls_before_preflight = ROUTED_CORS_HANDLER_CALLS.load(Ordering::Acquire);
        let preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/action?ignored=true")
                    .header(http::header::ORIGIN, "https://dynamic.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header("access-control-request-private-network", "true")
                    .body(Full::new(Bytes::new()))
                    .expect("routed CORS preflight"),
            )
            .await
            .expect("routed CORS preflight response");
        assert_eq!(preflight.status(), 200);
        assert_eq!(
            preflight.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://dynamic.example"
        );
        assert_eq!(
            preflight.headers()["access-control-allow-private-network"],
            "true"
        );
        let _ = preflight.into_body().collect().await.unwrap();

        let fallback_preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/action")
                    .header(http::header::ORIGIN, "https://global.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "PUT")
                    .body(Full::new(Bytes::new()))
                    .expect("routed CORS fallback preflight"),
            )
            .await
            .expect("routed CORS fallback preflight response");
        assert_eq!(fallback_preflight.status(), 200);
        assert_eq!(
            fallback_preflight.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://global.example"
        );
        let _ = fallback_preflight.into_body().collect().await.unwrap();

        let missing_preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/missing-preflight")
                    .header(http::header::ORIGIN, "https://global.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Full::new(Bytes::new()))
                    .expect("missing target CORS preflight"),
            )
            .await
            .expect("missing target CORS preflight response");
        assert_eq!(missing_preflight.status(), 200);
        assert_eq!(
            missing_preflight.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://global.example"
        );
        let _ = missing_preflight.into_body().collect().await.unwrap();

        let unrelated_options_preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/ordinary")
                    .header(http::header::ORIGIN, "https://dynamic.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Full::new(Bytes::new()))
                    .expect("unrelated OPTIONS CORS preflight"),
            )
            .await
            .expect("unrelated OPTIONS CORS preflight response");
        assert_eq!(unrelated_options_preflight.status(), 200);
        assert!(!unrelated_options_preflight
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        let _ = unrelated_options_preflight
            .into_body()
            .collect()
            .await
            .unwrap();

        let parameterized_preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/controller/42?ignored=true")
                    .header(http::header::ORIGIN, "https://controller.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Full::new(Bytes::new()))
                    .expect("parameterized route CORS preflight"),
            )
            .await
            .expect("parameterized route CORS preflight response");
        assert_eq!(parameterized_preflight.status(), 200);
        assert_eq!(
            parameterized_preflight.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://controller.example"
        );
        let _ = parameterized_preflight.into_body().collect().await.unwrap();

        let disabled_preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/disabled")
                    .header(http::header::ORIGIN, "https://global.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Full::new(Bytes::new()))
                    .expect("disabled route CORS preflight"),
            )
            .await
            .expect("disabled route CORS preflight response");
        assert_eq!(disabled_preflight.status(), 200);
        assert!(!disabled_preflight
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        let _ = disabled_preflight.into_body().collect().await.unwrap();
        assert_eq!(
            ROUTED_CORS_HANDLER_CALLS.load(Ordering::Acquire),
            calls_before_preflight,
            "preflight must not reach application dispatch"
        );

        let ordinary_options = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/ordinary")
                    .body(Full::new(Bytes::new()))
                    .expect("ordinary OPTIONS request"),
            )
            .await
            .expect("ordinary OPTIONS response");
        assert_eq!(ordinary_options.status(), 200);
        assert!(!ordinary_options
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        let _ = ordinary_options.into_body().collect().await.unwrap();

        assert_eq!(
            ROUTED_CORS_HANDLER_CALLS.load(Ordering::Acquire),
            initial_handler_calls + 8
        );
        assert_eq!(
            ROUTED_CORS_DECISIONS.load(Ordering::Acquire),
            initial_decisions + 4,
            "only selected dynamic route plans may call the resolver"
        );

        shutdown.cancel();
        drop(sender);
        let close_reason = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("routed CORS server task timed out")
            .expect("routed CORS server task panicked")
            .expect("routed CORS server connection failed");
        assert!(matches!(
            close_reason,
            ConnectionCloseReason::GracefulShutdown | ConnectionCloseReason::PeerClosed
        ));
        let _ = timeout(Duration::from_secs(2), client_driver).await;
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn managed_cors_transport_classifies_once_and_preserves_request_limits() {
        COUNTING_MIDDLEWARE_CALLS.store(0, Ordering::Release);
        let transport = HttpTransportConfig {
            max_uri_bytes: 64,
            max_request_header_bytes: 512,
            request_timeout: Duration::from_millis(50),
            ..HttpTransportConfig::default()
        };
        let app = Arc::new(
            AppBuilder::new("127.0.0.1:0")
                .protocol(HttpProtocol::Http1_1)
                .tls_disabled()
                .transport_config(transport)
                .middleware::<CountingMiddleware>()
                .cors(
                    CorsPolicy::new()
                        .allow_origins(["https://client.example"])
                        .allow_methods(["GET"])
                        .allow_headers(["x-client-token"])
                        .expose_headers(["x-handler"])
                        .allow_credentials(true)
                        .allow_private_network(true)
                        .max_age(Duration::from_secs(600)),
                )
                .build()
                .await
                .expect("build managed CORS application"),
        );
        let transport = app.transport_config().clone();
        let cors_service = HttpServer::compile_cors_dispatch_service(&app, &transport)
            .expect("CORS service is compiled once at application startup");
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_app = Arc::clone(&app);
        let telemetry = HttpServerTelemetry::new();
        let request_admission = Arc::new(Semaphore::new(1));
        let held_admission = Arc::clone(&request_admission)
            .try_acquire_owned()
            .expect("reserve the only request permit");
        let (server_io, client_io) = duplex(128 * 1024);
        let server_task = tokio::spawn(async move {
            HttpServer::serve_connection(
                server_io,
                "127.0.0.1:12345".parse().expect("peer address"),
                HttpConnectionRuntime {
                    service: server_app,
                    transport,
                    request_admission,
                    telemetry,
                    cors_services: Some(cors_service),
                },
                server_shutdown,
            )
            .await
        });
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                .await
                .expect("HTTP/1.1 client handshake");
        let client_driver = tokio::spawn(connection);

        let admission_rejected = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://client.example")
                    .body(Full::new(Bytes::new()))
                    .expect("admission-rejected CORS request"),
            )
            .await
            .expect("admission rejection response");
        assert_eq!(admission_rejected.status(), 503);
        assert_eq!(
            admission_rejected.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        assert_eq!(admission_rejected.headers()[http::header::RETRY_AFTER], "1");
        let _ = admission_rejected
            .into_body()
            .collect()
            .await
            .expect("admission rejection body");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 0);
        drop(held_admission);

        let preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://client.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(
                        http::header::ACCESS_CONTROL_REQUEST_HEADERS,
                        "x-client-token",
                    )
                    .header("access-control-request-private-network", "true")
                    .body(Full::new(Bytes::new()))
                    .expect("valid preflight request"),
            )
            .await
            .expect("preflight response");
        assert_eq!(preflight.status(), 200);
        assert_eq!(
            preflight.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        assert_eq!(
            preflight.headers()[http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS],
            "true"
        );
        assert_eq!(
            preflight.headers()["access-control-allow-private-network"],
            "true"
        );
        assert_eq!(preflight.headers()[http::header::SERVER], "lily");
        let vary = preflight.headers()[http::header::VARY]
            .to_str()
            .expect("bounded Vary header")
            .to_ascii_lowercase();
        assert_eq!(
            vary.split(',')
                .filter(|value| value.trim() == "origin")
                .count(),
            1
        );
        assert!(preflight
            .into_body()
            .collect()
            .await
            .expect("preflight body")
            .to_bytes()
            .is_empty());
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 0);

        let normal = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://client.example")
                    .body(Full::new(Bytes::new()))
                    .expect("valid simple CORS request"),
            )
            .await
            .expect("simple CORS response");
        assert_eq!(normal.status(), 404);
        assert_eq!(
            normal.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        assert_eq!(
            normal.headers()[http::header::ACCESS_CONTROL_EXPOSE_HEADERS],
            "x-handler"
        );
        for removed in [
            http::header::ACCESS_CONTROL_ALLOW_METHODS,
            http::header::ACCESS_CONTROL_ALLOW_HEADERS,
            http::header::ACCESS_CONTROL_MAX_AGE,
            http::header::HeaderName::from_static("access-control-allow-private-network"),
        ] {
            assert!(!normal.headers().contains_key(removed));
        }
        let _ = normal.into_body().collect().await.expect("normal body");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 1);

        let denied = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://denied.example")
                    .body(Full::new(Bytes::new()))
                    .expect("denied simple CORS request"),
            )
            .await
            .expect("denied simple CORS response");
        assert_eq!(denied.status(), 404);
        for removed in [
            http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
            http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            http::header::ACCESS_CONTROL_ALLOW_METHODS,
            http::header::ACCESS_CONTROL_ALLOW_HEADERS,
            http::header::ACCESS_CONTROL_EXPOSE_HEADERS,
            http::header::ACCESS_CONTROL_MAX_AGE,
            http::header::HeaderName::from_static("access-control-allow-private-network"),
        ] {
            assert!(!denied.headers().contains_key(removed));
        }
        let _ = denied.into_body().collect().await.expect("denied body");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 2);

        let ordinary_options = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/missing")
                    .body(Full::new(Bytes::new()))
                    .expect("ordinary OPTIONS request"),
            )
            .await
            .expect("ordinary OPTIONS response");
        assert_eq!(ordinary_options.status(), 404);
        assert!(!ordinary_options
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        let _ = ordinary_options
            .into_body()
            .collect()
            .await
            .expect("ordinary OPTIONS body");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 3);

        let incomplete_preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://client.example")
                    .body(Full::new(Bytes::new()))
                    .expect("incomplete preflight request"),
            )
            .await
            .expect("incomplete preflight response");
        assert_eq!(incomplete_preflight.status(), 400);
        assert!(!incomplete_preflight
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        let _ = incomplete_preflight
            .into_body()
            .collect()
            .await
            .expect("incomplete preflight body");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 3);

        let body_rejected = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://client.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Full::new(Bytes::from_static(b"unexpected")))
                    .expect("preflight with body"),
            )
            .await
            .expect("preflight body rejection");
        assert_eq!(body_rejected.status(), 400);
        assert_eq!(
            body_rejected.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        let _ = body_rejected
            .into_body()
            .collect()
            .await
            .expect("body rejection response");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 3);

        let oversized_target = format!("http://localhost/{}", "x".repeat(80));
        let uri_rejected = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri(oversized_target)
                    .header(http::header::ORIGIN, "https://client.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Full::new(Bytes::new()))
                    .expect("oversized preflight target"),
            )
            .await
            .expect("oversized target response");
        assert_eq!(uri_rejected.status(), 414);
        assert_eq!(
            uri_rejected.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        let _ = uri_rejected
            .into_body()
            .collect()
            .await
            .expect("oversized target body");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 3);

        let oversized_header = "x".repeat(600);
        let header_rejected = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://client.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header("x-padding", oversized_header)
                    .body(Full::new(Bytes::new()))
                    .expect("oversized preflight header"),
            )
            .await
            .expect("oversized header response");
        assert_eq!(header_rejected.status(), 400);
        assert_eq!(
            header_rejected.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        let _ = header_rejected
            .into_body()
            .collect()
            .await
            .expect("oversized header body");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 3);

        let timed_out = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/missing")
                    .header(http::header::ORIGIN, "https://client.example")
                    .header("x-test-timeout", "1")
                    .body(Full::new(Bytes::new()))
                    .expect("timed-out CORS request"),
            )
            .await
            .expect("request timeout response");
        assert_eq!(timed_out.status(), 504);
        assert_eq!(
            timed_out.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        let _ = timed_out
            .into_body()
            .collect()
            .await
            .expect("request timeout body");
        assert_eq!(COUNTING_MIDDLEWARE_CALLS.load(Ordering::Acquire), 4);

        shutdown.cancel();
        drop(sender);
        let close_reason = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("server task timed out")
            .expect("server task panicked")
            .expect("server connection failed");
        assert!(matches!(
            close_reason,
            ConnectionCloseReason::GracefulShutdown | ConnectionCloseReason::PeerClosed
        ));
        let _ = timeout(Duration::from_secs(2), client_driver).await;
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn dynamic_cors_resolver_initialization_failure_is_a_typed_build_error() {
        let result = AppBuilder::new("127.0.0.1:0")
            .protocol(HttpProtocol::Http1_1)
            .tls_disabled()
            .cors(
                CorsPolicy::new()
                    .resolve_origins_with::<FailingDynamicOriginResolver>()
                    .allow_methods(["GET"]),
            )
            .build()
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("failing dynamic CORS resolver unexpectedly built"),
        };
        assert!(matches!(
            error,
            AppBuildError::CorsOriginResolverInitialization(
                CorsOriginResolverInitError::MissingConfiguration { .. }
            )
        ));
        assert_eq!(
            error.to_string(),
            "HTTP CORS origin resolver initialization failed: CORS origin resolver initialization missing configuration (CORS_TEST_ORIGIN_CONFIGURATION_MISSING)"
        );
    }

    #[tokio::test]
    async fn managed_dynamic_cors_is_di_initialized_admission_bounded_and_fail_closed() {
        let initial_initializations = DYNAMIC_CORS_INITIALIZATIONS.load(Ordering::Acquire);
        let initial_decisions = DYNAMIC_CORS_DECISIONS.load(Ordering::Acquire);
        let transport = HttpTransportConfig {
            request_timeout: Duration::from_millis(50),
            ..HttpTransportConfig::default()
        };
        let app = Arc::new(
            AppBuilder::new("127.0.0.1:0")
                .protocol(HttpProtocol::Http1_1)
                .tls_disabled()
                .transport_config(transport)
                .cors(
                    CorsPolicy::new()
                        .resolve_origins_with::<ServerDynamicOriginResolver>()
                        .allow_methods(["GET"])
                        .allow_credentials(true)
                        .allow_private_network(true),
                )
                .build()
                .await
                .expect("build dynamic CORS application"),
        );
        assert_eq!(
            DYNAMIC_CORS_INITIALIZATIONS.load(Ordering::Acquire),
            initial_initializations + 1
        );

        let transport = app.transport_config().clone();
        let cors_service = HttpServer::compile_cors_dispatch_service(&app, &transport)
            .expect("dynamic CORS service is compiled once");
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_app = Arc::clone(&app);
        let telemetry = HttpServerTelemetry::new();
        let request_admission = Arc::new(Semaphore::new(1));
        let held_admission = Arc::clone(&request_admission)
            .try_acquire_owned()
            .expect("reserve dynamic CORS admission permit");
        let (server_io, client_io) = duplex(128 * 1024);
        let server_task = tokio::spawn(async move {
            HttpServer::serve_connection(
                server_io,
                "127.0.0.1:12346".parse().expect("peer address"),
                HttpConnectionRuntime {
                    service: server_app,
                    transport,
                    request_admission,
                    telemetry,
                    cors_services: Some(cors_service),
                },
                server_shutdown,
            )
            .await
        });
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                .await
                .expect("dynamic CORS client handshake");
        let client_driver = tokio::spawn(connection);

        let admission_rejected = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/admission-rejected")
                    .header(http::header::ORIGIN, "https://allowed.example")
                    .body(Full::new(Bytes::new()))
                    .expect("dynamic CORS admission request"),
            )
            .await
            .expect("dynamic CORS admission response");
        assert_eq!(admission_rejected.status(), 503);
        assert!(!admission_rejected
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        let _ = admission_rejected.into_body().collect().await.unwrap();
        assert_eq!(
            DYNAMIC_CORS_DECISIONS.load(Ordering::Acquire),
            initial_decisions
        );
        drop(held_admission);

        let allowed = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/allowed")
                    .header(http::header::ORIGIN, "https://allowed.example")
                    .body(Full::new(Bytes::new()))
                    .expect("allowed dynamic CORS request"),
            )
            .await
            .expect("allowed dynamic CORS response");
        assert_eq!(allowed.status(), 404);
        assert_eq!(
            allowed.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://allowed.example"
        );
        assert_eq!(
            allowed.headers()[http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS],
            "true"
        );
        let _ = allowed.into_body().collect().await.unwrap();

        let denied = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/denied")
                    .header(http::header::ORIGIN, "https://denied.example")
                    .body(Full::new(Bytes::new()))
                    .expect("denied dynamic CORS request"),
            )
            .await
            .expect("denied dynamic CORS response");
        assert_eq!(denied.status(), 404);
        assert!(!denied
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        assert!(!denied
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS));
        let _ = denied.into_body().collect().await.unwrap();

        let preflight = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::OPTIONS)
                    .uri("http://localhost/private")
                    .header(http::header::ORIGIN, "https://allowed.example")
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header("access-control-request-private-network", "true")
                    .body(Full::new(Bytes::new()))
                    .expect("dynamic PNA preflight request"),
            )
            .await
            .expect("dynamic PNA preflight response");
        assert_eq!(preflight.status(), 200);
        assert_eq!(
            preflight.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://allowed.example"
        );
        assert_eq!(
            preflight.headers()["access-control-allow-private-network"],
            "true"
        );
        let _ = preflight.into_body().collect().await.unwrap();

        let unavailable = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/resolver-unavailable")
                    .header(http::header::ORIGIN, "https://allowed.example")
                    .body(Full::new(Bytes::new()))
                    .expect("unavailable resolver request"),
            )
            .await
            .expect("unavailable resolver response");
        assert_eq!(unavailable.status(), 503);
        assert!(!unavailable
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        let _ = unavailable.into_body().collect().await.unwrap();

        let timed_out = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/resolver-timeout")
                    .header(http::header::ORIGIN, "https://allowed.example")
                    .body(Full::new(Bytes::new()))
                    .expect("timed-out resolver request"),
            )
            .await
            .expect("timed-out resolver response");
        assert_eq!(timed_out.status(), 504);
        assert!(!timed_out
            .headers()
            .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN));
        let _ = timed_out.into_body().collect().await.unwrap();

        assert_eq!(
            DYNAMIC_CORS_DECISIONS.load(Ordering::Acquire),
            initial_decisions + 5
        );

        shutdown.cancel();
        drop(sender);
        let close_reason = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("dynamic CORS server task timed out")
            .expect("dynamic CORS server task panicked")
            .expect("dynamic CORS server connection failed");
        assert!(matches!(
            close_reason,
            ConnectionCloseReason::GracefulShutdown | ConnectionCloseReason::PeerClosed
        ));
        let _ = timeout(Duration::from_secs(2), client_driver).await;
        close_test_app(app).await;
    }

    fn reserve_loopback_address() -> std::net::SocketAddr {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a loopback test address");
        let address = listener.local_addr().expect("read loopback test address");
        drop(listener);
        address
    }

    #[tokio::test]
    async fn real_loopback_http11_reconciles_connection_and_request_resources() {
        let address = reserve_loopback_address();
        let app = Arc::new(
            AppBuilder::new(&address.to_string())
                .protocol(HttpProtocol::Http1_1)
                .tls_disabled()
                .build()
                .await
                .expect("build real HTTP/1.1 application"),
        );
        let mut server = HttpServer(Arc::clone(&app))
            .start_managed()
            .await
            .expect("bind real HTTP/1.1 listener");
        let shutdown = server.admission_token();
        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect real HTTP/1.1 client");

        client
            .write_all(b"HEAD /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write real HTTP/1.1 request");
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("real HTTP/1.1 response deadline")
            .expect("read real HTTP/1.1 response");
        assert!(response.starts_with(b"HTTP/1.1 404"));
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("HEAD response contains complete headers");
        let headers = String::from_utf8_lossy(&response[..header_end]).to_ascii_lowercase();
        assert!(headers.contains("content-length: 27"));
        assert!(response[header_end + 4..].is_empty());

        shutdown.cancel();
        let report = timeout(Duration::from_secs(2), server.wait())
            .await
            .expect("real HTTP/1.1 server shutdown deadline")
            .expect("real HTTP/1.1 server shutdown");
        assert!(report.shutdown_is_clean());
        assert_eq!(report.accepted, 1);
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn real_loopback_http2_reconciles_connection_stream_and_request_resources() {
        let address = reserve_loopback_address();
        let app = Arc::new(
            AppBuilder::new(&address.to_string())
                .protocol(HttpProtocol::Http2)
                .tls_disabled()
                .build()
                .await
                .expect("build real HTTP/2 application"),
        );
        let mut server = HttpServer(Arc::clone(&app))
            .start_managed()
            .await
            .expect("bind real HTTP/2 listener");
        let shutdown = server.admission_token();
        let client = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect real HTTP/2 client");
        let (mut sender, connection) = hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
            TokioExecutor::new(),
            TokioIo::new(client),
        )
        .await
        .expect("real HTTP/2 client handshake");
        let client_driver = tokio::spawn(connection);

        let response = sender
            .send_request(
                HyperRequest::builder()
                    .uri("http://localhost/missing")
                    .body(Empty::new())
                    .expect("real HTTP/2 request"),
            )
            .await
            .expect("real HTTP/2 response");
        assert_eq!(response.version(), Version::HTTP_2);
        assert_eq!(response.status(), 404);
        let _ = response.into_body().collect().await.expect("response body");

        shutdown.cancel();
        drop(sender);
        let report = timeout(Duration::from_secs(2), server.wait())
            .await
            .expect("real HTTP/2 server shutdown deadline")
            .expect("real HTTP/2 server shutdown");
        assert!(report.shutdown_is_clean());
        assert_eq!(report.accepted, 1);
        let _ = timeout(Duration::from_secs(2), client_driver).await;
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn real_loopback_https_reconciles_tls_connection_and_request_resources() {
        let address = reserve_loopback_address();
        let (server_tls, client_tls) = test_tls_configs(vec![b"http/1.1".to_vec()]);
        let app = Arc::new(
            AppBuilder::new(&address.to_string())
                .protocol(HttpProtocol::Http1_1)
                .rustls_config(server_tls)
                .build()
                .await
                .expect("build real HTTPS application"),
        );
        let mut server = HttpServer(Arc::clone(&app))
            .start_managed()
            .await
            .expect("bind real HTTPS listener");
        let shutdown = server.admission_token();
        let tcp = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect real HTTPS client");
        let connector = TlsConnector::from(client_tls);
        let mut client = connector
            .connect(ServerName::try_from("localhost").expect("server name"), tcp)
            .await
            .expect("real HTTPS TLS handshake");

        client
            .write_all(b"GET /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write real HTTPS request");
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("real HTTPS response deadline")
            .expect("read real HTTPS response");
        assert!(response.starts_with(b"HTTP/1.1 404"));

        shutdown.cancel();
        let report = timeout(Duration::from_secs(2), server.wait())
            .await
            .expect("real HTTPS server shutdown deadline")
            .expect("real HTTPS server shutdown");
        assert!(report.shutdown_is_clean());
        assert_eq!(report.accepted, 1);
        close_test_app(app).await;
    }

    #[test]
    fn shared_request_header_validation_rejects_non_visible_values_before_allocation() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::HeaderName::from_static("x-opaque"),
            http::HeaderValue::from_bytes(&[0x80]).expect("opaque header value"),
        );
        let error = HttpServer::validate_request_header_map(&headers, 64, 1024)
            .expect_err("non-visible request metadata must fail for every dispatch path");
        assert!(matches!(
            error,
            lily_error::application::http_api::HttpApiError::InvalidHttpHeader(_)
        ));
    }

    #[test]
    fn shared_request_header_validation_counts_field_line_delimiters() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, http::HeaderValue::from_static("q"));

        HttpServer::validate_request_header_map(&headers, 1, 9)
            .expect("`host: q\\r\\n` occupies exactly nine field-line bytes");
        let error = HttpServer::validate_request_header_map(&headers, 1, 8)
            .expect_err("field-line delimiters must participate in the request header budget");
        assert!(matches!(
            error,
            lily_error::application::http_api::HttpApiError::InvalidHttpHeader(_)
        ));
    }

    #[tokio::test]
    async fn real_http1_rejects_field_line_delimiters_over_request_header_budget() {
        let address = reserve_loopback_address();
        let app = Arc::new(
            AppBuilder::new(&address.to_string())
                .protocol(HttpProtocol::Http1_1)
                .tls_disabled()
                .transport_config(HttpTransportConfig {
                    max_request_header_bytes: 8,
                    ..HttpTransportConfig::default()
                })
                .build()
                .await
                .expect("build request header field-line limit application"),
        );
        let mut server = HttpServer(Arc::clone(&app))
            .start_managed()
            .await
            .expect("bind request header field-line limit listener");
        let shutdown = server.admission_token();
        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect request header field-line limit client");

        client
            .write_all(b"GET /missing HTTP/1.1\r\nHost: q\r\n\r\n")
            .await
            .expect("write one-over request header field-line");
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), async {
            let mut buffer = [0_u8; 512];
            while !response.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = client
                    .read(&mut buffer)
                    .await
                    .expect("read request header field-line response");
                assert_ne!(read, 0, "response closed before its header block");
                response.extend_from_slice(&buffer[..read]);
            }
        })
        .await
        .expect("request header field-line response deadline");
        assert!(
            response.starts_with(b"HTTP/1.1 400 Bad Request\r\n"),
            "unexpected response: {:?}",
            String::from_utf8_lossy(&response)
        );

        shutdown.cancel();
        drop(client);
        let report = timeout(Duration::from_secs(2), server.wait())
            .await
            .expect("request header field-line server shutdown deadline")
            .expect("request header field-line server shutdown");
        assert!(report.shutdown_is_clean());
        assert_eq!(report.accepted, 1);
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn tls_http1_connection_reaches_the_existing_request_pipeline() {
        let (server_tls, client_tls) = test_tls_configs(vec![b"http/1.1".to_vec()]);
        let app = Arc::new(
            AppBuilder::new("127.0.0.1:0")
                .protocol(HttpProtocol::Http1_1)
                .rustls_config(server_tls)
                .build()
                .await
                .expect("build TLS test application"),
        );
        let acceptor = HttpServer::tls_acceptor(
            app.rustls_config().expect("TLS configuration"),
            app.protocol(),
        );
        let transport = app.transport_config().clone();
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_app = Arc::clone(&app);
        let telemetry = HttpServerTelemetry::new();
        let (server_io, client_io) = duplex(64 * 1024);

        let server_task = tokio::spawn(async move {
            let tls_stream = HttpServer::accept_tls(
                server_io,
                acceptor,
                HttpProtocol::Http1_1,
                Duration::from_secs(1),
                &server_shutdown,
                &telemetry,
            )
            .await?
            .expect("server is not shutting down");
            HttpServer::serve_connection(
                tls_stream,
                "127.0.0.1:12345".parse().expect("peer address"),
                HttpConnectionRuntime {
                    service: server_app,
                    transport,
                    request_admission: Arc::new(Semaphore::new(8)),
                    telemetry,
                    cors_services: None,
                },
                server_shutdown,
            )
            .await
        });

        let connector = TlsConnector::from(client_tls);
        let mut client_stream = connector
            .connect(
                ServerName::try_from("localhost").expect("server name"),
                client_io,
            )
            .await
            .expect("TLS client handshake");
        assert_eq!(
            client_stream.get_ref().1.alpn_protocol(),
            Some(b"http/1.1".as_slice())
        );
        client_stream
            .write_all(b"GET /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write HTTPS request");
        let mut response = Vec::new();
        timeout(
            Duration::from_secs(2),
            client_stream.read_to_end(&mut response),
        )
        .await
        .expect("HTTPS response timed out")
        .expect("read HTTPS response");
        assert!(response.starts_with(b"HTTP/1.1 404"));

        let close_reason = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("server task timed out")
            .expect("server task panicked")
            .expect("server connection failed");
        assert_eq!(close_reason, ConnectionCloseReason::PeerClosed);
        shutdown.cancel();
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn tls_http2_requires_h2_alpn_and_serves_http2() {
        let (server_tls, client_tls) = test_tls_configs(vec![b"h2".to_vec()]);
        let app = Arc::new(
            AppBuilder::new("127.0.0.1:0")
                .protocol(HttpProtocol::Http2)
                .rustls_config(server_tls)
                .build()
                .await
                .expect("build TLS test application"),
        );
        let acceptor = HttpServer::tls_acceptor(
            app.rustls_config().expect("TLS configuration"),
            app.protocol(),
        );
        let transport = app.transport_config().clone();
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_app = Arc::clone(&app);
        let telemetry = HttpServerTelemetry::new();
        let (server_io, client_io) = duplex(64 * 1024);

        let server_task = tokio::spawn(async move {
            let tls_stream = HttpServer::accept_tls(
                server_io,
                acceptor,
                HttpProtocol::Http2,
                Duration::from_secs(1),
                &server_shutdown,
                &telemetry,
            )
            .await?
            .expect("server is not shutting down");
            HttpServer::serve_connection(
                tls_stream,
                "127.0.0.1:12345".parse().expect("peer address"),
                HttpConnectionRuntime {
                    service: server_app,
                    transport,
                    request_admission: Arc::new(Semaphore::new(8)),
                    telemetry,
                    cors_services: None,
                },
                server_shutdown,
            )
            .await
        });

        let connector = TlsConnector::from(client_tls);
        let client_stream = connector
            .connect(
                ServerName::try_from("localhost").expect("server name"),
                client_io,
            )
            .await
            .expect("TLS client handshake");
        assert_eq!(
            client_stream.get_ref().1.alpn_protocol(),
            Some(b"h2".as_slice())
        );
        let (mut sender, connection) = hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
            TokioExecutor::new(),
            TokioIo::new(client_stream),
        )
        .await
        .expect("HTTP/2 client handshake");
        let client_connection = tokio::spawn(connection);
        let response = sender
            .send_request(
                HyperRequest::builder()
                    .uri("https://localhost/missing")
                    .body(Empty::new())
                    .expect("HTTP/2 request"),
            )
            .await
            .expect("HTTP/2 response");
        assert_eq!(response.version(), Version::HTTP_2);
        assert_eq!(response.status(), 404);
        let _ = response.into_body().collect().await.expect("response body");

        shutdown.cancel();
        let close_reason = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("server task timed out")
            .expect("server task panicked")
            .expect("server connection failed");
        assert_eq!(close_reason, ConnectionCloseReason::GracefulShutdown);
        drop(sender);
        let _ = timeout(Duration::from_secs(2), client_connection).await;
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn strict_http2_tls_rejects_clients_without_h2_alpn() {
        let (server_tls, client_tls) = test_tls_configs(Vec::new());
        let acceptor = HttpServer::tls_acceptor(&server_tls, HttpProtocol::Http2);
        let connector = TlsConnector::from(client_tls);
        let shutdown = CancellationToken::new();
        let telemetry = HttpServerTelemetry::new();
        let (server_io, client_io) = duplex(16 * 1024);

        let (server_result, client_result) = tokio::join!(
            HttpServer::accept_tls(
                server_io,
                acceptor,
                HttpProtocol::Http2,
                Duration::from_secs(1),
                &shutdown,
                &telemetry,
            ),
            connector.connect(
                ServerName::try_from("localhost").expect("server name"),
                client_io,
            )
        );
        assert!(
            client_result.is_ok(),
            "Rustls handshake itself should complete"
        );
        let error = server_result.expect_err("HTTP/2 must require h2 ALPN");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(matches!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<TlsHandshakeFailure>()),
            Some(TlsHandshakeFailure::Http2AlpnRequired)
        ));
    }

    #[tokio::test]
    async fn http_tls_adapter_preserves_required_client_certificate_verification() {
        let (server_tls, authenticated_client, anonymous_client) = test_mtls_configs();
        let shutdown = CancellationToken::new();
        let telemetry = HttpServerTelemetry::new();

        let (server_io, client_io) = duplex(16 * 1024);
        let acceptor = HttpServer::tls_acceptor(&server_tls, HttpProtocol::Http1_1);
        let connector = TlsConnector::from(authenticated_client);
        let (server_result, client_result) = tokio::join!(
            HttpServer::accept_tls(
                server_io,
                acceptor,
                HttpProtocol::Http1_1,
                Duration::from_secs(1),
                &shutdown,
                &telemetry,
            ),
            connector.connect(
                ServerName::try_from("localhost").expect("server name"),
                client_io,
            )
        );
        assert!(server_result.is_ok());
        assert!(client_result.is_ok());

        let (server_io, client_io) = duplex(16 * 1024);
        let acceptor = HttpServer::tls_acceptor(&server_tls, HttpProtocol::Http1_1);
        let connector = TlsConnector::from(anonymous_client);
        let (server_result, _client_result) = tokio::join!(
            HttpServer::accept_tls(
                server_io,
                acceptor,
                HttpProtocol::Http1_1,
                Duration::from_secs(1),
                &shutdown,
                &telemetry,
            ),
            connector.connect(
                ServerName::try_from("localhost").expect("server name"),
                client_io,
            )
        );
        assert!(
            server_result.is_err(),
            "anonymous mTLS client must be rejected"
        );
    }

    #[tokio::test]
    async fn stalled_tls_handshake_is_bounded_and_reconciled_as_a_typed_error() {
        let (server_tls, _) = test_tls_configs(Vec::new());
        let acceptor = HttpServer::tls_acceptor(&server_tls, HttpProtocol::Auto);
        let shutdown = CancellationToken::new();
        let telemetry = HttpServerTelemetry::new();
        let (server_io, _stalled_client) = duplex(1024);

        let error = HttpServer::accept_tls(
            server_io,
            acceptor,
            HttpProtocol::Auto,
            Duration::from_millis(10),
            &shutdown,
            &telemetry,
        )
        .await
        .expect_err("a stalled TLS handshake must time out");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(matches!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<TlsHandshakeFailure>()),
            Some(TlsHandshakeFailure::TimedOut)
        ));

        let mut report = ConnectionTaskReport {
            accepted: 1,
            ..ConnectionTaskReport::default()
        };
        report.record_join(Ok(Arc::new(Err(error))));
        assert_eq!(report.connection_errors, 1);
        assert_eq!(report.tls_handshake_errors, 1);
        assert!(report.reconciles());
    }

    #[tokio::test]
    async fn shutdown_cancels_an_incomplete_tls_handshake_cleanly() {
        let (server_tls, _) = test_tls_configs(Vec::new());
        let acceptor = HttpServer::tls_acceptor(&server_tls, HttpProtocol::Auto);
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let telemetry = HttpServerTelemetry::new();
        let (server_io, _stalled_client) = duplex(1024);

        let result = HttpServer::accept_tls(
            server_io,
            acceptor,
            HttpProtocol::Auto,
            Duration::from_secs(30),
            &shutdown,
            &telemetry,
        )
        .await
        .expect("shutdown is not a handshake failure");
        assert!(result.is_none());
    }

    struct DropMarker(Arc<AtomicBool>);

    struct ScriptedBody {
        frames: VecDeque<Result<Frame<Bytes>, &'static str>>,
        hint: SizeHint,
        polls: Arc<AtomicUsize>,
        dropped: Option<Arc<AtomicBool>>,
    }

    impl ScriptedBody {
        fn new(frames: impl IntoIterator<Item = Result<Frame<Bytes>, &'static str>>) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                hint: SizeHint::default(),
                polls: Arc::new(AtomicUsize::new(0)),
                dropped: None,
            }
        }

        fn with_exact_hint(mut self, bytes: u64) -> Self {
            self.hint = SizeHint::with_exact(bytes);
            self
        }
    }

    impl Body for ScriptedBody {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.frames.pop_front())
        }

        fn size_hint(&self) -> SizeHint {
            self.hint
        }
    }

    impl Drop for ScriptedBody {
        fn drop(&mut self) {
            if let Some(dropped) = &self.dropped {
                dropped.store(true, Ordering::SeqCst);
            }
        }
    }

    fn raw_header(name: &str, value: &str) -> RawHeader {
        RawHeader {
            name: name.to_string(),
            value: value.to_string(),
            line_number: 1,
            raw_line: String::new(),
        }
    }

    #[test]
    fn hyper_request_boundary_extracts_the_remote_w3c_parent() {
        use opentelemetry::trace::TraceContextExt;

        lily_trace::install_w3c_propagator();
        let request = Request::from_streaming_transport_parts(
            "GET".to_string(),
            "/trace".to_string(),
            vec![raw_header(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )],
            None,
            16 * 1024 * 1024,
        )
        .unwrap();

        let context = lily_trace::extract_context(&RequestTraceExtractor(&request));
        let span = context.span();
        assert!(span.span_context().is_remote());
        assert_eq!(
            span.span_context().trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn connection_tasks_drain_gracefully_before_the_deadline() {
        let mut tasks = TaskSet::new(TaskRegistry::default());
        tasks.spawn(async { Ok(ConnectionCloseReason::PeerClosed) });
        let mut report = ConnectionTaskReport {
            accepted: 1,
            ..ConnectionTaskReport::default()
        };

        HttpServer::drain_connection_tasks(&mut tasks, Duration::from_secs(1), &mut report).await;

        assert!(tasks.is_empty());
        assert_eq!(report.completed, 1);
        assert_eq!(report.peer_closed, 1);
        assert_eq!(report.forced, 0);
        assert_eq!(report.cancelled, 0);
        assert!(report.shutdown_is_clean());
        assert!(report.shutdown_is_terminal());
    }

    #[tokio::test]
    async fn connection_tasks_are_aborted_and_joined_after_the_deadline() {
        let dropped = Arc::new(AtomicBool::new(false));
        let marker = DropMarker(Arc::clone(&dropped));
        let mut tasks = TaskSet::new(TaskRegistry::default());
        tasks.spawn(async move {
            let _marker = marker;
            pending::<std::io::Result<ConnectionCloseReason>>().await
        });
        tokio::task::yield_now().await;

        let mut report = ConnectionTaskReport {
            accepted: 1,
            ..ConnectionTaskReport::default()
        };
        HttpServer::drain_connection_tasks(&mut tasks, Duration::from_millis(10), &mut report)
            .await;

        assert!(tasks.is_empty());
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(report.forced, 1);
        assert_eq!(report.cancelled, 1);
        assert!(!report.shutdown_is_clean());
        assert!(report.shutdown_is_terminal());
    }

    #[tokio::test]
    async fn second_signal_force_path_aborts_and_joins_backpressured_tasks() {
        let dropped = Arc::new(AtomicBool::new(false));
        let marker = DropMarker(Arc::clone(&dropped));
        let mut tasks = TaskSet::new(TaskRegistry::default());
        tasks.spawn(async move {
            let _marker = marker;
            pending::<std::io::Result<ConnectionCloseReason>>().await
        });
        tokio::task::yield_now().await;

        let force = CancellationToken::new();
        force.cancel();
        let mut report = ConnectionTaskReport {
            accepted: 1,
            ..ConnectionTaskReport::default()
        };
        HttpServer::drain_connection_tasks_with_force(
            &mut tasks,
            Duration::from_secs(30),
            &force,
            &mut report,
        )
        .await;

        assert!(tasks.is_empty());
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(report.accepted, 1);
        assert_eq!(report.forced, 1);
        assert_eq!(report.cancelled, 1);
        assert_eq!(report.panicked, 0);
        assert!(!report.shutdown_is_clean());
        assert!(report.shutdown_is_terminal());
    }

    #[test]
    fn terminal_shutdown_accounting_rejects_panics_and_unreconciled_tasks() {
        let panicked = ConnectionTaskReport {
            accepted: 1,
            panicked: 1,
            ..ConnectionTaskReport::default()
        };
        assert!(panicked.reconciles());
        assert!(!panicked.shutdown_is_terminal());

        let unreconciled = ConnectionTaskReport {
            accepted: 1,
            ..ConnectionTaskReport::default()
        };
        assert!(!unreconciled.reconciles());
        assert!(!unreconciled.shutdown_is_terminal());
    }

    #[tokio::test]
    async fn managed_server_replays_one_terminal_result_to_lifecycle_callers() {
        let expected = ConnectionTaskReport {
            accepted: 1,
            completed: 1,
            graceful_shutdown: 1,
            ..ConnectionTaskReport::default()
        };
        let mut server = ManagedHttpServer {
            bound_address: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            shutdown: CancellationToken::new(),
            force: CancellationToken::new(),
            budget: ShutdownBudget::new(Duration::from_secs(1)),
            requests: crate::request_lifecycle::RequestRegistry::default(),
            task: TaskRegistry::default().spawn(async move { Ok(expected) }),
            outcome: None,
        };

        let first = server.wait().await.unwrap();
        let replay = server.wait().await.unwrap();
        assert_eq!(first, expected);
        assert_eq!(replay, expected);
        assert!(first.reconciles());
        assert!(first.shutdown_is_clean());
    }

    #[test]
    fn persistent_accept_failures_back_off_then_stop_with_a_typed_report() {
        let mut budget = AcceptFailureBudget::default();
        let mut delays = Vec::new();
        for attempt in 1..MAX_CONSECUTIVE_ACCEPT_FAILURES {
            match budget.record_failure() {
                AcceptFailureAction::RetryAfter(delay) => delays.push(delay),
                action => panic!("attempt {attempt} stopped too early: {action:?}"),
            }
        }
        assert_eq!(
            budget.record_failure(),
            AcceptFailureAction::Stop {
                consecutive_failures: MAX_CONSECUTIVE_ACCEPT_FAILURES
            }
        );
        assert_eq!(
            delays,
            vec![
                Duration::from_millis(25),
                Duration::from_millis(50),
                Duration::from_millis(100),
                Duration::from_millis(200),
            ]
        );

        budget.record_success();
        assert_eq!(
            budget.record_failure(),
            AcceptFailureAction::RetryAfter(Duration::from_millis(25))
        );

        let source = AcceptLoopFailure {
            consecutive_failures: MAX_CONSECUTIVE_ACCEPT_FAILURES,
            report: ConnectionTaskReport {
                accepted: 2,
                completed: 2,
                ..ConnectionTaskReport::default()
            },
        };
        let error = std::io::Error::other(source);
        let source = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<AcceptLoopFailure>())
            .expect("accept failure must remain a typed io::Error source");
        assert_eq!(source.consecutive_failures, MAX_CONSECUTIVE_ACCEPT_FAILURES);
        assert_eq!(source.report.accepted, 2);
        assert_eq!(source.report.completed, 2);
    }

    #[test]
    fn bind_failure_preserves_typed_error_kind_and_listener_context() {
        let address = "127.0.0.1:8080";
        let error = HttpServer::bind_error(
            address,
            std::io::Error::new(std::io::ErrorKind::AddrInUse, "occupied"),
        );

        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert!(error.to_string().contains(address));
    }

    #[tokio::test]
    #[ignore = "environment-restricted qualification: requires loopback port binding"]
    async fn port_in_use_preserves_addr_in_use_and_listener_context() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("qualification host must allow loopback bind");
        let address = occupied.local_addr().unwrap().to_string();

        let error = HttpServer::bind(&address)
            .await
            .expect_err("a second listener must not claim an occupied address");

        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert!(error.to_string().contains(&address));
    }

    #[tokio::test]
    async fn bounded_response_body_chunks_frames_and_reconciles_completion() {
        let activity = ConnectionActivity::new();
        let guard = activity.enter();
        let mut body = BoundedResponseBody::new_for_test(
            Bytes::from_static(b"0123456789"),
            4,
            Duration::from_secs(1),
            guard,
        );

        let mut chunks = Vec::new();
        while let Some(frame) = body.frame().await {
            chunks.push(frame.unwrap().into_data().unwrap());
        }
        assert_eq!(
            chunks,
            vec![
                Bytes::from_static(b"0123"),
                Bytes::from_static(b"4567"),
                Bytes::from_static(b"89")
            ]
        );
        assert_eq!(activity.snapshot().started, 1);
        assert_eq!(activity.snapshot().completed, 1);
        assert_eq!(activity.snapshot().active, 0);
    }

    #[tokio::test]
    async fn production_http11_sends_the_first_stream_chunk_before_completion() {
        let mut app = AppBuilder::new("127.0.0.1:0")
            .protocol(HttpProtocol::Http1_1)
            .tls_disabled()
            .build()
            .await
            .expect("build streaming transport application");
        app.replace_routes_and_cors_for_test(vec![streaming_transport_route()], None)
            .await
            .expect("compile streaming route");
        let app = Arc::new(app);
        let transport = app.transport_config().clone();
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_app = Arc::clone(&app);
        let (server_io, client_io) = duplex(128 * 1024);
        let server_task = tokio::spawn(async move {
            HttpServer::serve_connection(
                server_io,
                "127.0.0.1:12348".parse().expect("peer address"),
                HttpConnectionRuntime {
                    service: server_app,
                    transport,
                    request_admission: Arc::new(Semaphore::new(8)),
                    telemetry: HttpServerTelemetry::new(),
                    cors_services: None,
                },
                server_shutdown,
            )
            .await
        });
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                .await
                .expect("streaming client handshake");
        let client_driver = tokio::spawn(connection);

        let mut response = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/stream")
                    .body(Full::new(Bytes::new()))
                    .expect("streaming request"),
            )
            .await
            .expect("streaming response headers");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[http::header::CONTENT_TYPE],
            "application/x-ndjson"
        );
        let first = timeout(Duration::from_secs(1), response.body_mut().frame())
            .await
            .expect("first stream chunk deadline")
            .expect("stream must yield its first frame")
            .expect("first stream frame must succeed")
            .into_data()
            .expect("first stream frame is data");
        assert_eq!(first, Bytes::from_static(b"first"));

        STREAMING_TRANSPORT_RELEASE.notify_one();
        let second = timeout(Duration::from_secs(1), response.body_mut().frame())
            .await
            .expect("second stream chunk deadline")
            .expect("stream must yield its second frame")
            .expect("second stream frame must succeed")
            .into_data()
            .expect("second stream frame is data");
        assert_eq!(second, Bytes::from_static(b"second"));
        assert!(timeout(Duration::from_secs(1), response.body_mut().frame())
            .await
            .expect("stream completion deadline")
            .is_none());

        drop(response);
        drop(sender);
        let _ = timeout(Duration::from_secs(1), client_driver).await;
        shutdown.cancel();
        let close_reason = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("streaming server task deadline")
            .expect("streaming server task panicked")
            .expect("streaming server connection failed");
        assert!(matches!(
            close_reason,
            ConnectionCloseReason::GracefulShutdown | ConnectionCloseReason::PeerClosed
        ));
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn production_http11_sse_is_canonical_resumable_and_disconnect_cancels_source() {
        SSE_TRANSPORT_DROPPED.store(false, Ordering::Release);
        SSE_LAST_EVENT_ID_OBSERVED.store(false, Ordering::Release);
        let mut app = AppBuilder::new("127.0.0.1:0")
            .protocol(HttpProtocol::Http1_1)
            .tls_disabled()
            .build()
            .await
            .expect("build SSE transport application");
        app.replace_routes_and_cors_for_test(vec![sse_transport_route()], None)
            .await
            .expect("compile SSE route");
        let app = Arc::new(app);
        let transport = app.transport_config().clone();
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_app = Arc::clone(&app);
        let (server_io, client_io) = duplex(128 * 1024);
        let server_task = tokio::spawn(async move {
            HttpServer::serve_connection(
                server_io,
                "127.0.0.1:12349".parse().expect("peer address"),
                HttpConnectionRuntime {
                    service: server_app,
                    transport,
                    request_admission: Arc::new(Semaphore::new(8)),
                    telemetry: HttpServerTelemetry::new(),
                    cors_services: None,
                },
                server_shutdown,
            )
            .await
        });
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                .await
                .expect("SSE client handshake");
        let client_driver = tokio::spawn(connection);

        let mut response = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/events")
                    .header("Last-Event-ID", "event-41")
                    .body(Full::new(Bytes::new()))
                    .expect("SSE request"),
            )
            .await
            .expect("SSE response headers");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[http::header::CONTENT_TYPE],
            "text/event-stream"
        );
        assert_eq!(
            response.headers()[http::header::CACHE_CONTROL],
            "no-cache, no-transform"
        );
        assert_eq!(response.headers()["x-accel-buffering"], "no");
        assert!(SSE_LAST_EVENT_ID_OBSERVED.load(Ordering::Acquire));

        let first = timeout(Duration::from_secs(1), response.body_mut().frame())
            .await
            .expect("first SSE event deadline")
            .expect("SSE stream must yield its first frame")
            .expect("first SSE frame must succeed")
            .into_data()
            .expect("first SSE frame is data");
        assert_eq!(
            first,
            Bytes::from_static(
                b"data:first line\ndata:second line\nevent:notification\nid:event-42\n\n"
            )
        );

        drop(response);
        drop(sender);
        timeout(Duration::from_secs(1), async {
            while !SSE_TRANSPORT_DROPPED.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disconnect must drop the SSE source");
        let _ = timeout(Duration::from_secs(1), client_driver).await;
        shutdown.cancel();
        let server_result = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("SSE server task deadline")
            .expect("SSE server task panicked");
        match server_result {
            Ok(close_reason) => assert!(matches!(
                close_reason,
                ConnectionCloseReason::GracefulShutdown | ConnectionCloseReason::PeerClosed
            )),
            Err(error) => assert_eq!(
                error.kind(),
                io::ErrorKind::Other,
                "an interrupted HTTP/1 response body must remain a transport error"
            ),
        }
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn production_http11_static_files_preserve_get_head_range_and_conditionals() {
        let root = tempfile::TempDir::new().expect("static-file temp root");
        let public = root.path().join("public");
        std::fs::create_dir(&public).expect("static-file public root");
        let contents = b"0123456789static-asset";
        std::fs::write(public.join("asset.txt"), contents).expect("static-file fixture");
        std::fs::write(root.path().join("outside-secret.txt"), b"must-not-leak")
            .expect("outside fixture");
        let mount = StaticFileMount::new(&public, "/assets")
            .await
            .expect("static-file mount")
            .cache_control("public, max-age=60")
            .expect("static cache policy");
        *STATIC_FILE_TRANSPORT_MOUNT.lock().await = Some(mount);

        let mut app = AppBuilder::new("127.0.0.1:0")
            .protocol(HttpProtocol::Http1_1)
            .tls_disabled()
            .build()
            .await
            .expect("build static-file transport application");
        app.replace_routes_and_cors_for_test(vec![static_file_transport_route()], None)
            .await
            .expect("compile catch-all static-file route");
        let app = Arc::new(app);
        let transport = app.transport_config().clone();
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_app = Arc::clone(&app);
        let (server_io, client_io) = duplex(256 * 1024);
        let server_task = tokio::spawn(async move {
            HttpServer::serve_connection(
                server_io,
                "127.0.0.1:12350".parse().expect("peer address"),
                HttpConnectionRuntime {
                    service: server_app,
                    transport,
                    request_admission: Arc::new(Semaphore::new(8)),
                    telemetry: HttpServerTelemetry::new(),
                    cors_services: None,
                },
                server_shutdown,
            )
            .await
        });
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                .await
                .expect("static-file client handshake");
        let client_driver = tokio::spawn(connection);

        let response = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/assets/asset.txt")
                    .body(Full::new(Bytes::new()))
                    .expect("static GET request"),
            )
            .await
            .expect("static GET response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[http::header::CONTENT_TYPE], "text/plain");
        assert_eq!(response.headers()[http::header::ACCEPT_RANGES], "bytes");
        assert_eq!(
            response.headers()[http::header::CACHE_CONTROL],
            "public, max-age=60"
        );
        assert_eq!(
            response.headers()[http::header::CONTENT_LENGTH],
            contents.len().to_string()
        );
        let etag = response.headers()[http::header::ETAG].clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect static GET body")
            .to_bytes();
        assert_eq!(body.as_ref(), contents);

        let response = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::HEAD)
                    .uri("http://localhost/assets/asset.txt")
                    .body(Full::new(Bytes::new()))
                    .expect("static HEAD request"),
            )
            .await
            .expect("static HEAD response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[http::header::CONTENT_LENGTH],
            contents.len().to_string()
        );
        assert!(response
            .into_body()
            .collect()
            .await
            .expect("collect static HEAD body")
            .to_bytes()
            .is_empty());

        let response = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/assets/asset.txt")
                    .header(http::header::RANGE, "bytes=2-5")
                    .body(Full::new(Bytes::new()))
                    .expect("static range request"),
            )
            .await
            .expect("static range response");
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers()[http::header::CONTENT_RANGE],
            format!("bytes 2-5/{}", contents.len())
        );
        assert_eq!(
            response
                .into_body()
                .collect()
                .await
                .expect("collect static range body")
                .to_bytes(),
            Bytes::from_static(b"2345")
        );

        let response = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/assets/asset.txt")
                    .header(http::header::IF_NONE_MATCH, etag)
                    .body(Full::new(Bytes::new()))
                    .expect("static conditional request"),
            )
            .await
            .expect("static conditional response");
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert!(response
            .into_body()
            .collect()
            .await
            .expect("collect static conditional body")
            .to_bytes()
            .is_empty());

        let response = sender
            .send_request(
                HyperRequest::builder()
                    .method(http::Method::GET)
                    .uri("http://localhost/assets/%2e%2e/outside-secret.txt")
                    .body(Full::new(Bytes::new()))
                    .expect("static traversal request"),
            )
            .await
            .expect("static traversal response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect static traversal body")
            .to_bytes();
        assert!(!body
            .windows(b"must-not-leak".len())
            .any(|value| value == b"must-not-leak"));

        drop(sender);
        let _ = timeout(Duration::from_secs(1), client_driver).await;
        shutdown.cancel();
        let close_reason = timeout(Duration::from_secs(2), server_task)
            .await
            .expect("static-file server task deadline")
            .expect("static-file server task panicked")
            .expect("static-file server connection failed");
        assert!(matches!(
            close_reason,
            ConnectionCloseReason::GracefulShutdown | ConnectionCloseReason::PeerClosed
        ));
        *STATIC_FILE_TRANSPORT_MOUNT.lock().await = None;
        close_test_app(app).await;
    }

    async fn application_stream_body(value: StreamingResponse) -> TransportResponseBody {
        let limits = ResponseLimits::new(BodyBudget::new(1024).unwrap(), 16, 4096)
            .unwrap()
            .with_streaming_limits(ResponseStreamingLimits::new(16, None).unwrap());
        let mut response = Response::with_limits(limits).await.unwrap();
        let mut request = Request::from_streaming_transport_parts(
            "GET".to_string(),
            "/stream".to_string(),
            Vec::new(),
            None,
            1024,
        )
        .unwrap();
        value
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();
        response.into_transport_parts().unwrap().into_body()
    }

    #[tokio::test]
    async fn streaming_body_is_pull_driven_and_drains_pending_frames_before_polling_again() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let source = futures::stream::poll_fn(move |_context| {
            let index = observed.fetch_add(1, Ordering::AcqRel);
            Poll::Ready(match index {
                0 => Some(Ok::<_, ResponseBodyError>(Bytes::from_static(b"abcdef"))),
                1 => Some(Ok(Bytes::from_static(b"gh"))),
                _ => None,
            })
        });
        let transport_body = application_stream_body(
            streaming(source)
                .content_length(8)
                .max_chunk_bytes(8)
                .max_total_bytes(8),
        )
        .await;
        assert_eq!(polls.load(Ordering::Acquire), 0);

        let activity = ConnectionActivity::new();
        let mut body = BoundedResponseBody::new_for_test(
            transport_body,
            4,
            Duration::from_secs(1),
            activity.enter(),
        );
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            b"abcd".as_slice()
        );
        assert_eq!(polls.load(Ordering::Acquire), 1);
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            b"ef".as_slice()
        );
        assert_eq!(polls.load(Ordering::Acquire), 1);
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            b"gh".as_slice()
        );
        assert_eq!(polls.load(Ordering::Acquire), 2);
        assert!(body.frame().await.is_none());
        assert_eq!(polls.load(Ordering::Acquire), 3);
        assert_eq!(activity.snapshot().completed, 1);
        assert_eq!(activity.snapshot().active, 0);
    }

    #[tokio::test]
    async fn exact_length_stream_drop_completes_only_after_all_frames_and_without_a_stop() {
        for (length, reads, stop, completed) in [
            (Some(6), 2, false, true),
            (Some(0), 0, false, true),
            (Some(6), 1, false, false),
            (Some(6), 0, false, false),
            (None, 2, false, false),
            (Some(6), 2, true, false),
            (Some(0), 0, true, false),
        ] {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed = polls.clone();
            let mut value = streaming(futures::stream::poll_fn(move |_| {
                assert_eq!(
                    observed.fetch_add(1, Ordering::AcqRel),
                    0,
                    "no final EOF poll"
                );
                Poll::Ready(Some(Ok::<_, ResponseBodyError>(Bytes::from_static(
                    b"abcdef",
                ))))
            }));
            if let Some(length) = length {
                value = value.content_length(length);
            }
            let source = application_stream_body(value).await;
            let activity = ConnectionActivity::new();
            let mut guard = activity.enter();
            let control = guard.bind_transport(Version::HTTP_11);
            control.commit_fallback().unwrap();
            let mut body = BoundedResponseBody::new(source, 4, guard);
            for expected in [b"abcd".as_slice(), b"ef".as_slice()]
                .into_iter()
                .take(reads)
            {
                assert_eq!(
                    body.frame().await.unwrap().unwrap().into_data().unwrap(),
                    expected
                );
            }
            if stop {
                assert!(control.request_stop(crate::lifecycle::ExecutionStopReason::PeerDisconnect));
            }
            drop(body); // Hyper need not poll None after Content-Length bytes.
            assert_eq!(polls.load(Ordering::Acquire), usize::from(reads > 0));
            let snapshot = activity.snapshot();
            assert_eq!(snapshot.completed, usize::from(completed));
            assert_eq!(snapshot.cancelled, usize::from(!completed));
            assert_eq!(snapshot.active, 0);
            assert_eq!(
                control.snapshot().frames,
                if completed {
                    super::super::response_control::ResponseFrames::Completed
                } else {
                    super::super::response_control::ResponseFrames::Dropped
                }
            );
        }
    }

    #[tokio::test]
    async fn streaming_body_enforces_chunk_cumulative_and_exact_length_contracts() {
        let oversized = application_stream_body(
            streaming(futures::stream::iter([Ok::<_, ResponseBodyError>(
                Bytes::from_static(b"12345"),
            )]))
            .max_chunk_bytes(4),
        )
        .await;
        let activity = ConnectionActivity::new();
        let mut body = BoundedResponseBody::new_for_test(
            oversized,
            16,
            Duration::from_secs(1),
            activity.enter(),
        );
        assert_eq!(
            body.frame().await.unwrap().unwrap_err(),
            ResponseBodyError::StreamChunkTooLarge { limit_bytes: 4 }
        );
        assert_eq!(activity.snapshot().completed, 1);
        assert_eq!(activity.snapshot().active, 0);

        let cumulative = application_stream_body(
            streaming(futures::stream::iter([
                Ok::<_, ResponseBodyError>(Bytes::from_static(b"123")),
                Ok(Bytes::from_static(b"45")),
            ]))
            .max_total_bytes(4),
        )
        .await;
        let activity = ConnectionActivity::new();
        let mut body = BoundedResponseBody::new_for_test(
            cumulative,
            16,
            Duration::from_secs(1),
            activity.enter(),
        );
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            b"123".as_slice()
        );
        assert_eq!(
            body.frame().await.unwrap().unwrap_err(),
            ResponseBodyError::StreamLimitExceeded { limit_bytes: 4 }
        );

        let incomplete = application_stream_body(
            streaming(futures::stream::iter([Ok::<_, ResponseBodyError>(
                Bytes::from_static(b"123"),
            )]))
            .content_length(5),
        )
        .await;
        let activity = ConnectionActivity::new();
        let mut body = BoundedResponseBody::new_for_test(
            incomplete,
            16,
            Duration::from_secs(1),
            activity.enter(),
        );
        assert!(body.frame().await.unwrap().is_ok());
        assert_eq!(
            body.frame().await.unwrap().unwrap_err(),
            ResponseBodyError::StreamLengthIncomplete {
                expected_bytes: 5,
                actual_bytes: 3,
            }
        );

        let exceeded = application_stream_body(
            streaming(futures::stream::iter([Ok::<_, ResponseBodyError>(
                Bytes::from_static(b"123456"),
            )]))
            .content_length(5),
        )
        .await;
        let activity = ConnectionActivity::new();
        let mut body = BoundedResponseBody::new_for_test(
            exceeded,
            16,
            Duration::from_secs(1),
            activity.enter(),
        );
        assert_eq!(
            body.frame().await.unwrap().unwrap_err(),
            ResponseBodyError::StreamLengthExceeded { expected_bytes: 5 }
        );
    }

    struct StreamDropProbe(Arc<AtomicBool>);

    impl Drop for StreamDropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn head_and_body_forbidden_statuses_drop_without_polling_the_stream() {
        for (is_head, status, expected_length) in [
            (true, StatusCode::OK, Some(9)),
            (false, StatusCode::NO_CONTENT, None),
            (false, StatusCode::NOT_MODIFIED, None),
        ] {
            let polls = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicBool::new(false));
            let observed_polls = Arc::clone(&polls);
            let probe = StreamDropProbe(Arc::clone(&dropped));
            let source = futures::stream::poll_fn(move |_context| {
                let _probe = &probe;
                observed_polls.fetch_add(1, Ordering::AcqRel);
                Poll::<Option<Result<Bytes, ResponseBodyError>>>::Pending
            });
            let body = application_stream_body(streaming(source).content_length(9)).await;

            let (selected, length) =
                HttpServer::select_application_response_body(is_head, status, body);
            assert!(matches!(selected, TransportResponseBody::Full(ref body) if body.is_empty()));
            assert_eq!(length, expected_length);
            assert_eq!(polls.load(Ordering::Acquire), 0);
            assert!(dropped.load(Ordering::Acquire));
        }
    }

    #[tokio::test]
    async fn stream_source_failure_is_typed_and_reconciles_the_request() {
        let source =
            futures::stream::iter([Err::<Bytes, _>(ResponseBodyError::StreamSourceFailed)]);
        let transport_body = application_stream_body(streaming(source)).await;
        let activity = ConnectionActivity::new();
        let mut body = BoundedResponseBody::new_for_test(
            transport_body,
            16,
            Duration::from_secs(1),
            activity.enter(),
        );

        assert_eq!(
            body.frame().await.unwrap().unwrap_err(),
            ResponseBodyError::StreamSourceFailed
        );
        assert!(body.is_end_stream());
        assert_eq!(activity.snapshot().completed, 1);
        assert_eq!(activity.snapshot().active, 0);
    }

    #[tokio::test]
    async fn disconnect_and_write_timeout_drop_the_stream_producer() {
        let disconnect_drop = Arc::new(AtomicBool::new(false));
        let disconnect_probe = StreamDropProbe(Arc::clone(&disconnect_drop));
        let disconnect_source = futures::stream::poll_fn(move |_context| {
            let _probe = &disconnect_probe;
            Poll::<Option<Result<Bytes, ResponseBodyError>>>::Pending
        });
        let disconnect_body = application_stream_body(streaming(disconnect_source)).await;
        let disconnect_activity = ConnectionActivity::new();
        let body = BoundedResponseBody::new_for_test(
            disconnect_body,
            16,
            Duration::from_secs(1),
            disconnect_activity.enter(),
        );
        drop(body);
        assert!(disconnect_drop.load(Ordering::Acquire));
        assert_eq!(disconnect_activity.snapshot().cancelled, 1);
        assert_eq!(disconnect_activity.snapshot().active, 0);

        let timeout_drop = Arc::new(AtomicBool::new(false));
        let timeout_probe = StreamDropProbe(Arc::clone(&timeout_drop));
        let timeout_source = futures::stream::poll_fn(move |_context| {
            let _probe = &timeout_probe;
            Poll::<Option<Result<Bytes, ResponseBodyError>>>::Pending
        });
        let timeout_body = application_stream_body(streaming(timeout_source)).await;
        let timeout_activity = ConnectionActivity::new();
        let body = BoundedResponseBody::new_for_test(
            timeout_body,
            16,
            Duration::from_millis(5),
            timeout_activity.enter(),
        );
        tokio::time::timeout(
            Duration::from_millis(100),
            timeout_activity.wait_until_response_stop(),
        )
        .await
        .expect("the response write timeout must wake connection supervision");
        drop(body);
        assert!(timeout_drop.load(Ordering::Acquire));
        assert_eq!(timeout_activity.snapshot().timed_out, 1);
        assert_eq!(timeout_activity.snapshot().active, 0);
    }

    #[tokio::test]
    async fn request_activity_ledger_reconciles_every_terminal_outcome() {
        let activity = ConnectionActivity::new();

        let completed = BoundedResponseBody::new_for_test(
            Bytes::new(),
            4,
            Duration::from_secs(1),
            activity.enter(),
        );
        drop(completed);

        let cancelled = BoundedResponseBody::new_for_test(
            Bytes::from_static(b"cancelled"),
            4,
            Duration::from_secs(1),
            activity.enter(),
        );
        drop(cancelled);

        let timed_out = BoundedResponseBody::new_for_test(
            Bytes::from_static(b"backpressured"),
            4,
            Duration::from_millis(5),
            activity.enter(),
        );
        tokio::time::timeout(
            Duration::from_millis(100),
            activity.wait_until_response_stop(),
        )
        .await
        .expect("the write deadline must terminate the outstanding request");
        drop(timed_out);

        let snapshot = activity.snapshot();
        assert_eq!(snapshot.started, 3);
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.cancelled, 1);
        assert_eq!(snapshot.timed_out, 1);
        assert_eq!(snapshot.active, 0);
        assert_eq!(
            snapshot.started,
            snapshot.completed + snapshot.cancelled + snapshot.timed_out
        );
    }

    #[tokio::test]
    async fn stalled_response_body_has_a_typed_connection_deadline_and_raii_cancellation() {
        let activity = ConnectionActivity::new();
        let guard = activity.enter();
        let timed_out = BoundedResponseBody::new_for_test(
            Bytes::from_static(b"pending"),
            4,
            Duration::from_millis(5),
            guard,
        );
        tokio::time::timeout(
            Duration::from_millis(100),
            activity.wait_until_response_stop(),
        )
        .await
        .expect("the response write deadline must wake connection supervision");
        drop(timed_out);
        assert_eq!(activity.snapshot().timed_out, 1);
        assert_eq!(activity.snapshot().active, 0);

        let cancelled = BoundedResponseBody::new_for_test(
            Bytes::from_static(b"cancelled"),
            4,
            Duration::from_secs(1),
            activity.enter(),
        );
        drop(cancelled);
        assert_eq!(activity.snapshot().cancelled, 1);
        assert_eq!(activity.snapshot().active, 0);
    }

    #[test]
    fn untrusted_peer_cannot_spoof_forwarding_headers() {
        let transport = HttpTransportConfig::default();
        let peer = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));
        let mut headers = vec![
            raw_header("X-Forwarded-For", "198.51.100.40"),
            raw_header("X-Real-IP", "198.51.100.41"),
            raw_header("Forwarded", "for=198.51.100.42"),
            raw_header("Accept", "application/json"),
        ];

        let info = HttpServer::resolve_connection_info(Some(peer), &mut headers, &transport)
            .expect("untrusted forwarding input is ignored, not trusted");

        assert_eq!(info.peer_ip(), Some(peer));
        assert_eq!(info.client_ip(), Some(peer));
        assert!(!info.via_trusted_proxy());
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, "Accept");
    }

    #[test]
    fn trusted_proxy_chain_walks_right_to_left_and_stops_at_first_untrusted_hop() {
        let transport = HttpTransportConfig {
            trusted_proxy_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
            ..HttpTransportConfig::default()
        };
        let peer = "10.2.3.4".parse::<IpAddr>().unwrap();
        let mut headers = vec![raw_header(
            "X-Forwarded-For",
            "203.0.113.77, 198.51.100.20, 10.1.2.3",
        )];

        let info = HttpServer::resolve_connection_info(Some(peer), &mut headers, &transport)
            .expect("valid trusted chain must resolve");

        assert_eq!(info.peer_ip(), Some(peer));
        assert_eq!(
            info.client_ip(),
            Some("198.51.100.20".parse::<IpAddr>().unwrap())
        );
        assert!(info.via_trusted_proxy());
        assert!(headers.is_empty(), "raw forwarding headers must be removed");
    }

    #[test]
    fn malformed_or_duplicate_forwarding_data_from_trusted_proxy_is_rejected() {
        let transport = HttpTransportConfig {
            trusted_proxy_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
            ..HttpTransportConfig::default()
        };
        let peer = "10.2.3.4".parse::<IpAddr>().unwrap();

        for mut headers in [
            vec![raw_header("X-Forwarded-For", "not-an-ip")],
            vec![
                raw_header("X-Forwarded-For", "198.51.100.1"),
                raw_header("x-forwarded-for", "198.51.100.2"),
            ],
        ] {
            let error = HttpServer::resolve_connection_info(Some(peer), &mut headers, &transport)
                .expect_err("trusted proxy contract violation must fail closed");
            assert_eq!(error.http_status().0, 400);
            assert!(!error.to_string().contains("not-an-ip"));
            assert!(!error.to_string().contains("198.51.100"));
        }
    }

    #[tokio::test]
    async fn edge_identity_headers_never_create_or_reach_a_framework_principal() {
        let transport = HttpTransportConfig {
            trusted_proxy_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
            ..HttpTransportConfig::default()
        };
        let peer = "10.2.3.4".parse::<IpAddr>().unwrap();
        let mut headers = vec![
            raw_header("X-Forwarded-For", "198.51.100.20"),
            raw_header("X-Lily-Subject", "attacker"),
            raw_header("X-Lily-Roles", "admin"),
            raw_header("X-Auth-Request-User", "attacker"),
            raw_header("X-Forwarded-Client-Cert", "spoofed-certificate"),
            raw_header("Authorization", "Bearer end-to-end-token"),
        ];

        let info = HttpServer::resolve_connection_info(Some(peer), &mut headers, &transport)
            .expect("trusted client-IP forwarding must not enable identity headers");
        assert!(info.via_trusted_proxy());
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, "Authorization");

        let request =
            Request::from_transport_parts("GET".to_string(), "/identity".to_string(), headers, &[])
                .await
                .unwrap();
        assert!(request.principal().is_none());
        assert_eq!(
            request.header_value("authorization"),
            Some("Bearer end-to-end-token")
        );
        assert!(request.header_value("x-lily-subject").is_none());
    }

    #[tokio::test]
    async fn request_body_is_pulled_one_chunk_at_a_time_without_implicit_buffering() {
        let source = ScriptedBody::new([
            Ok(Frame::data(Bytes::from_static(b"first"))),
            Ok(Frame::data(Bytes::from_static(b"second"))),
        ]);
        let polls = Arc::clone(&source.polls);
        let body = BoundedRequestBody::new(source, BodyBudget::new(32).unwrap(), 4, 1024)
            .unwrap()
            .unwrap();
        let mut request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/stream".to_string(),
            Vec::new(),
            Some(Box::new(body)),
            32,
        )
        .unwrap();

        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(request.body_state(), RequestBodyState::Pending);
        assert_eq!(request.next_body_chunk().await.unwrap().unwrap(), "first");
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(request.body().is_none(), "streaming must not retain chunks");
        assert_eq!(
            request.buffer_body().await.unwrap_err(),
            RequestBodyError::AlreadyStreaming
        );
        assert_eq!(request.next_body_chunk().await.unwrap().unwrap(), "second");
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(request.streamed_body_bytes(), 11);
        assert!(request.next_body_chunk().await.unwrap().is_none());
        assert_eq!(request.body_state(), RequestBodyState::Complete);
    }

    #[tokio::test]
    async fn explicit_buffering_preserves_the_legacy_body_view() {
        let source = ScriptedBody::new([
            Ok(Frame::data(Bytes::from_static(b"legacy"))),
            Ok(Frame::data(Bytes::from_static(b"-body"))),
        ]);
        let body = BoundedRequestBody::new(source, BodyBudget::new(32).unwrap(), 4, 1024)
            .unwrap()
            .unwrap();
        let request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/buffer".to_string(),
            Vec::new(),
            Some(Box::new(body)),
            32,
        )
        .unwrap();

        let buffered = request.buffer_body().await.unwrap().unwrap();
        assert_eq!(buffered.as_slice(), b"legacy-body");
        assert_eq!(request.body_bytes(), Some(b"legacy-body".as_slice()));
        assert_eq!(request.body_state(), RequestBodyState::Buffered);
        assert!(!request.has_streaming_body());
    }

    #[tokio::test]
    async fn existing_json_helper_lazily_buffers_the_transport_stream() {
        let source = ScriptedBody::new([
            Ok(Frame::data(Bytes::from_static(b"{\"value\":"))),
            Ok(Frame::data(Bytes::from_static(b"42}"))),
        ]);
        let body = BoundedRequestBody::new(source, BodyBudget::new(64).unwrap(), 4, 1024)
            .unwrap()
            .unwrap();
        let request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/json".to_string(),
            vec![raw_header("Content-Type", "application/json")],
            Some(Box::new(body)),
            64,
        )
        .unwrap();

        let value: serde_json::Value = request.json().await.unwrap();
        assert_eq!(value["value"], 42);
        assert_eq!(request.body_bytes(), Some(b"{\"value\":42}".as_slice()));
        assert_eq!(request.body_state(), RequestBodyState::Buffered);
    }

    #[tokio::test]
    async fn body_limits_and_transport_failures_are_typed_and_redacted() {
        let advertised = ScriptedBody::new([]).with_exact_hint(9);
        assert!(matches!(
            BoundedRequestBody::new(advertised, BodyBudget::new(8).unwrap(), 4, 1024),
            Err(RequestBodyError::PayloadTooLarge { limit_bytes: 8 })
        ));

        let source = ScriptedBody::new([
            Ok(Frame::data(Bytes::from_static(b"123456"))),
            Ok(Frame::data(Bytes::from_static(b"789"))),
        ]);
        let mut body = BoundedRequestBody::new(source, BodyBudget::new(8).unwrap(), 4, 1024)
            .unwrap()
            .unwrap();
        assert_eq!(body.next_chunk().await.unwrap().unwrap(), "123456");
        assert_eq!(
            body.next_chunk().await.unwrap_err(),
            RequestBodyError::PayloadTooLarge { limit_bytes: 8 }
        );

        let source = ScriptedBody::new([Err("peer-reset-secret")]);
        let mut body = BoundedRequestBody::new(source, BodyBudget::new(8).unwrap(), 4, 1024)
            .unwrap()
            .unwrap();
        let error = body.next_chunk().await.unwrap_err();
        assert_eq!(error, RequestBodyError::TransportInterrupted);
        assert!(!error.to_string().contains("peer-reset-secret"));
        let http_error = lily_error::application::http_api::HttpApiError::from(error);
        assert_eq!(http_error.error_code(), "INVALID_REQUEST_BODY");
        assert_eq!(http_error.http_status().0, 400);
        assert!(!http_error.to_string().contains("peer-reset-secret"));
    }

    #[tokio::test]
    async fn request_trailer_limits_fail_closed() {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-first", "one".parse().unwrap());
        trailers.insert("x-second", "two".parse().unwrap());
        let source = ScriptedBody::new([Ok(Frame::trailers(trailers))]);
        let mut body = BoundedRequestBody::new(source, BodyBudget::new(8).unwrap(), 1, 1024)
            .unwrap()
            .unwrap();
        assert_eq!(
            body.next_chunk().await.unwrap_err(),
            RequestBodyError::InvalidTrailers
        );

        let mut trailers = HeaderMap::new();
        trailers.insert("x", "y".parse().unwrap());
        let source = ScriptedBody::new([Ok(Frame::trailers(trailers))]);
        let mut body = BoundedRequestBody::new(source, BodyBudget::new(8).unwrap(), 1, 5)
            .unwrap()
            .unwrap();
        assert_eq!(
            body.next_chunk().await.unwrap_err(),
            RequestBodyError::InvalidTrailers,
            "`x: y\\r\\n` must exceed a five-byte trailer field-line budget"
        );
    }

    #[tokio::test]
    async fn dropping_an_unconsumed_request_drops_the_transport_body() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut source = ScriptedBody::new([Ok(Frame::data(Bytes::from_static(b"unused")))]);
        source.dropped = Some(Arc::clone(&dropped));
        let body = BoundedRequestBody::new(source, BodyBudget::new(8).unwrap(), 4, 1024)
            .unwrap()
            .unwrap();
        let request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/cancel".to_string(),
            Vec::new(),
            Some(Box::new(body)),
            8,
        )
        .unwrap();

        assert!(!dropped.load(Ordering::SeqCst));
        drop(request);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
