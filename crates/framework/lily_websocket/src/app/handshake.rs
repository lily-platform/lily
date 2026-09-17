use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use lily_injection::{ApplicationContainer, ProcessContext};
use lily_middleware::MiddlewareErrorCode;
use lily_web_core::{RequestConnectionInfo, RequestExtensions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::tungstenite::handshake::{
    machine::TryParse,
    server::{Request, Response, create_response, write_response},
};
use tokio_tungstenite::tungstenite::http::{
    HeaderValue, StatusCode, Version,
    header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE},
};
use tokio_util::sync::CancellationToken;

use super::ownership::ScopeCleanupRegistry;
use super::{collect_handshake_headers, has_ambiguous_handshake_headers};
use crate::controller::WebSocketActionTable;
use crate::middleware::{
    WebSocketIdentity, WsHandshakeExchange, WsHandshakeExecutionFailure,
    WsHandshakeExecutionFailureKind, WsHandshakeRejection, WsHandshakeRequest,
};
use crate::request::WsHeaders;
use crate::server::{
    HandshakeRejection, ServerConfig, ServerError, WsTransportSecurity, parse_handshake_namespace,
};
use uuid::Uuid;

const MAX_UPGRADE_REQUEST_BYTES: usize = 64 * 1024;

pub(super) struct PreparedHandshake {
    pub(super) namespace: String,
    pub(super) headers: WsHeaders,
    pub(super) identity: Option<crate::WebSocketIdentitySnapshot>,
    pub(super) connection_locals: Option<Arc<RequestExtensions>>,
    pub(super) subprotocol: Option<String>,
    pub(super) connection_info: RequestConnectionInfo,
    pub(super) transport_security: WsTransportSecurity,
}

pub(super) enum PreUpgradeOutcome {
    Accepted(Box<PreparedHandshake>),
    Rejected(MiddlewareErrorCode),
    Cancelled,
    Disconnected,
}

enum RequestReadOutcome {
    Request(Request),
    Rejected(WsHandshakeRejection),
    Cancelled,
    Disconnected,
}

enum PipelineFailure {
    Execution(WsHandshakeExecutionFailure),
    Internal,
}

enum FinalUpgradeProbeOutcome {
    Clear,
    EarlyData,
    Cancelled,
    Disconnected,
}

#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "the private Upgrade boundary receives each independently owned runtime authority explicitly"
)]
pub(super) async fn perform_pre_upgrade<S>(
    stream: &mut S,
    peer_addr: SocketAddr,
    action_table: &Arc<WebSocketActionTable>,
    container: &Arc<ApplicationContainer>,
    config: &ServerConfig,
    transport_security: WsTransportSecurity,
    cancellation: &CancellationToken,
    deadline: Instant,
    handshake_timeout: Duration,
    scopes: &ScopeCleanupRegistry,
    connection_id: Uuid,
) -> Result<PreUpgradeOutcome, ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    match read_pre_upgrade(stream, cancellation, deadline, handshake_timeout).await? {
        UpgradeRequestOutcome::Parsed(request) => {
            perform_parsed_upgrade(
                request,
                stream,
                peer_addr,
                action_table,
                container,
                config,
                transport_security,
                cancellation,
                deadline,
                handshake_timeout,
                scopes,
                connection_id,
            )
            .await
        }
        UpgradeRequestOutcome::Terminal(outcome) => Ok(outcome),
    }
}

pub(super) enum UpgradeRequestOutcome {
    Parsed(Request),
    Terminal(PreUpgradeOutcome),
}

/// Read under the original TLS/Upgrade deadline before creating remote spans.
pub(super) async fn read_pre_upgrade<S>(
    stream: &mut S,
    cancellation: &CancellationToken,
    deadline: Instant,
    handshake_timeout: Duration,
) -> Result<UpgradeRequestOutcome, ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let outcome = match read_request(stream, cancellation, deadline, handshake_timeout).await? {
        RequestReadOutcome::Request(request) => return Ok(UpgradeRequestOutcome::Parsed(request)),
        RequestReadOutcome::Rejected(rejection) => {
            write_rejection(
                stream,
                &rejection,
                cancellation,
                deadline,
                handshake_timeout,
            )
            .await?
        }
        RequestReadOutcome::Cancelled => PreUpgradeOutcome::Cancelled,
        RequestReadOutcome::Disconnected => PreUpgradeOutcome::Disconnected,
    };
    Ok(UpgradeRequestOutcome::Terminal(outcome))
}

/// Use only unambiguous UTF-8 propagation headers. This neither authorizes
/// the upgrade nor exposes arbitrary request headers to telemetry.
pub(super) fn request_trace_context(request: &Request) -> opentelemetry::Context {
    struct Carrier<'a>(&'a Request);
    impl opentelemetry::propagation::Extractor for Carrier<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            let mut values = self.0.headers().get_all(key).iter();
            let first = values.next()?;
            if values.next().is_some() {
                return None;
            }
            first.to_str().ok()
        }
        fn keys(&self) -> Vec<&str> {
            vec!["traceparent", "tracestate"]
        }
    }
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract_with_context(&opentelemetry::Context::new(), &Carrier(request))
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the private Upgrade boundary receives each independently owned runtime authority explicitly"
)]
pub(super) async fn perform_parsed_upgrade<S>(
    request: Request,
    stream: &mut S,
    peer_addr: SocketAddr,
    action_table: &Arc<WebSocketActionTable>,
    container: &Arc<ApplicationContainer>,
    config: &ServerConfig,
    transport_security: WsTransportSecurity,
    cancellation: &CancellationToken,
    deadline: Instant,
    handshake_timeout: Duration,
    scopes: &ScopeCleanupRegistry,
    connection_id: Uuid,
) -> Result<PreUpgradeOutcome, ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Tungstenite remains the protocol authority for method/version/key
    // presence and Sec-WebSocket-Accept generation. Lily performs the strict
    // key format/nonce-length check below because the pinned transport version
    // predates that server-side validation. Application code cannot run until
    // both checks succeed.
    let mut response = match create_response(&request) {
        Ok(response) => response,
        Err(_) => {
            let rejection = transport_rejection(
                StatusCode::BAD_REQUEST,
                "WS_UPGRADE_INVALID",
                "The WebSocket upgrade request is invalid.",
            );
            return write_rejection(
                stream,
                &rejection,
                cancellation,
                deadline,
                handshake_timeout,
            )
            .await;
        }
    };

    if request.uri().path() != config.endpoint_path {
        let rejection = transport_rejection(
            StatusCode::NOT_FOUND,
            "WS_ENDPOINT_NOT_FOUND",
            "WebSocket endpoint was not found.",
        );
        return write_rejection(
            stream,
            &rejection,
            cancellation,
            deadline,
            handshake_timeout,
        )
        .await;
    }
    if has_ambiguous_handshake_headers(request.headers()) {
        let rejection = transport_rejection(
            StatusCode::BAD_REQUEST,
            "WS_HEADERS_AMBIGUOUS",
            "WebSocket handshake headers are ambiguous.",
        );
        return write_rejection(
            stream,
            &rejection,
            cancellation,
            deadline,
            handshake_timeout,
        )
        .await;
    }
    let mut raw_headers = match collect_handshake_headers(request.headers()) {
        Ok(headers) => headers,
        Err(_) => {
            let rejection = transport_rejection(
                StatusCode::BAD_REQUEST,
                "WS_HEADERS_INVALID",
                "WebSocket handshake headers are invalid.",
            );
            return write_rejection(
                stream,
                &rejection,
                cancellation,
                deadline,
                handshake_timeout,
            )
            .await;
        }
    };
    let connection_info = match resolve_connection_info(peer_addr.ip(), &mut raw_headers, config) {
        Ok(connection_info) => connection_info,
        Err(()) => {
            let rejection = transport_rejection(
                StatusCode::BAD_REQUEST,
                "WS_FORWARDED_HEADERS_INVALID",
                "Trusted proxy forwarding metadata is invalid.",
            );
            return write_rejection(
                stream,
                &rejection,
                cancellation,
                deadline,
                handshake_timeout,
            )
            .await;
        }
    };
    let headers = match WsHeaders::try_from_http_headers(&raw_headers) {
        Ok(headers) => headers,
        Err(_) => {
            let rejection = transport_rejection(
                StatusCode::BAD_REQUEST,
                "WS_HEADERS_INVALID",
                "WebSocket handshake headers are invalid.",
            );
            return write_rejection(
                stream,
                &rejection,
                cancellation,
                deadline,
                handshake_timeout,
            )
            .await;
        }
    };
    if headers.validate_handshake().is_err() {
        let rejection = transport_rejection(
            StatusCode::BAD_REQUEST,
            "WS_HEADERS_INVALID",
            "WebSocket handshake headers are invalid.",
        );
        return write_rejection(
            stream,
            &rejection,
            cancellation,
            deadline,
            handshake_timeout,
        )
        .await;
    }
    let namespace = match parse_handshake_namespace(request.uri().query()) {
        Ok(namespace) => namespace,
        Err(rejection) => {
            let rejection = server_rejection(rejection);
            return write_rejection(
                stream,
                &rejection,
                cancellation,
                deadline,
                handshake_timeout,
            )
            .await;
        }
    };
    if !action_table.contains_namespace(&namespace) {
        let rejection = server_rejection(HandshakeRejection::NamespaceNotFound);
        return write_rejection(
            stream,
            &rejection,
            cancellation,
            deadline,
            handshake_timeout,
        )
        .await;
    }

    // Origin and subprotocol policy are transport-owned and run before any
    // application identity or scoped dependency lookup.
    let decision = match config.evaluate_handshake(&headers) {
        Ok(decision) => decision,
        Err(rejection) => {
            let rejection = server_rejection(rejection);
            return write_rejection(
                stream,
                &rejection,
                cancellation,
                deadline,
                handshake_timeout,
            )
            .await;
        }
    };

    let chain = action_table
        .handshake_middleware(&namespace)
        .expect("validated namespace has one materialized handshake plan");
    let identity_middleware = action_table.identity_middleware();
    let identity = if chain.is_empty() && identity_middleware.is_none() {
        None
    } else {
        // Peer termination stops application execution without revoking the
        // transport's authority to send an early-data rejection response.
        let pipeline_cancellation = cancellation.child_token();
        let pipeline = execute_pipeline(
            Arc::clone(container),
            chain,
            identity_middleware,
            WsHandshakeRequest::new(
                namespace.clone(),
                headers.clone(),
                peer_addr,
                connection_info,
                transport_security,
            ),
            pipeline_cancellation.clone(),
            deadline,
            Duration::from_secs(config.connection_middleware_timeout_secs),
            (scopes.clone(), connection_id),
        );
        tokio::pin!(pipeline);
        let execution_signal = crate::ExecutionCancellation::with_budget(
            pipeline_cancellation.clone(),
            scopes.budget.clone(),
        );
        let mut peer_probe = [0_u8; 1];
        let pipeline_result = tokio::select! {
            biased;
            _ = execution_signal.termination_requested() => return Ok(PreUpgradeOutcome::Cancelled),
            peer = stream.read(&mut peer_probe) => {
                // A peer/force race cannot bypass the cooperative window by
                // dropping the whole pipeline on the transport branch.
                pipeline_cancellation.cancel();
                tokio::select! {
                    biased;
                    () = execution_signal.termination_requested() => {}
                    _ = timeout_at(deadline, &mut pipeline) => {}
                }
                match peer {
                Ok(0) => return Ok(PreUpgradeOutcome::Disconnected),
                Ok(_) => {
                    let rejection = early_data_rejection();
                    return write_rejection(
                        stream,
                        &rejection,
                        cancellation,
                        deadline,
                        handshake_timeout,
                    )
                    .await;
                }
                Err(error) if is_peer_disconnect(&error) => {
                    return Ok(PreUpgradeOutcome::Disconnected);
                }
                Err(error) => return Err(ServerError::IoError(error)),
                }
            },
            result = timeout_at(deadline, &mut pipeline) => match result {
                Ok(result) => result,
                Err(_) => return Err(ServerError::HandshakeTimeout(handshake_timeout)),
            },
        };
        match pipeline_result {
            Ok(identity) => identity,
            Err(PipelineFailure::Execution(failure)) => {
                tracing::warn!(
                    lily.phase = "async_handshake",
                    lily.middleware = failure.descriptor().name(),
                    lily.outcome = "rejected",
                    "WebSocket async handshake stage did not complete"
                );
                match failure.kind() {
                    WsHandshakeExecutionFailureKind::Rejected(rejection) => {
                        return write_rejection(
                            stream,
                            rejection,
                            cancellation,
                            deadline,
                            handshake_timeout,
                        )
                        .await;
                    }
                    WsHandshakeExecutionFailureKind::Timeout => {
                        return Err(ServerError::HandshakeTimeout(handshake_timeout));
                    }
                    WsHandshakeExecutionFailureKind::Cancelled => {
                        return Ok(PreUpgradeOutcome::Cancelled);
                    }
                    WsHandshakeExecutionFailureKind::Panicked => {
                        let rejection = transport_rejection(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "WS_HANDSHAKE_INTERNAL",
                            "The WebSocket upgrade request could not be completed.",
                        );
                        return write_rejection(
                            stream,
                            &rejection,
                            cancellation,
                            deadline,
                            handshake_timeout,
                        )
                        .await;
                    }
                }
            }
            Err(PipelineFailure::Internal) => {
                let rejection = transport_rejection(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "WS_HANDSHAKE_SCOPE",
                    "The WebSocket upgrade request could not be completed.",
                );
                return write_rejection(
                    stream,
                    &rejection,
                    cancellation,
                    deadline,
                    handshake_timeout,
                )
                .await;
            }
        }
    };
    let (identity, connection_locals) = finalize_identity(identity);
    if identity
        .as_ref()
        .and_then(crate::WebSocketIdentitySnapshot::expiry)
        .is_some_and(|expires_at| expires_at <= Instant::now())
    {
        let rejection = identity_expired_rejection();
        return write_rejection(
            stream,
            &rejection,
            cancellation,
            deadline,
            handshake_timeout,
        )
        .await;
    }

    // The request parser can finish exactly at the bounded read capacity while
    // a peer byte remains queued in the transport. The application-pipeline
    // probe above cannot cover the no-pipeline path, so every successful path
    // gets one final, non-blocking peer poll before the HTTP 101 commit.
    match probe_final_upgrade_boundary(stream, cancellation, deadline, handshake_timeout).await? {
        FinalUpgradeProbeOutcome::Clear => {}
        FinalUpgradeProbeOutcome::EarlyData => {
            let rejection = early_data_rejection();
            return write_rejection(
                stream,
                &rejection,
                cancellation,
                deadline,
                handshake_timeout,
            )
            .await;
        }
        FinalUpgradeProbeOutcome::Cancelled => return Ok(PreUpgradeOutcome::Cancelled),
        FinalUpgradeProbeOutcome::Disconnected => return Ok(PreUpgradeOutcome::Disconnected),
    }

    if let Some(protocol) = decision.subprotocol.as_deref() {
        let value = HeaderValue::from_str(protocol).map_err(|_| {
            ServerError::configuration("configured WebSocket subprotocol is invalid".to_owned())
        })?;
        response
            .headers_mut()
            .insert("sec-websocket-protocol", value);
    }
    match write_switching_protocols(stream, &response, cancellation, deadline, handshake_timeout)
        .await?
    {
        WriteOutcome::Complete => Ok(PreUpgradeOutcome::Accepted(Box::new(PreparedHandshake {
            namespace,
            headers,
            identity,
            connection_locals,
            subprotocol: decision.subprotocol,
            connection_info,
            transport_security,
        }))),
        WriteOutcome::Cancelled => Ok(PreUpgradeOutcome::Cancelled),
        WriteOutcome::Disconnected => Ok(PreUpgradeOutcome::Disconnected),
    }
}

/// Resolves one effective client IP and removes all proxy-owned identity
/// headers before application middleware can observe the Upgrade request.
fn resolve_connection_info(
    peer_ip: IpAddr,
    headers: &mut HashMap<String, String>,
    config: &ServerConfig,
) -> Result<RequestConnectionInfo, ()> {
    let forwarded_for = headers.remove("x-forwarded-for");
    headers.retain(|name, _| {
        name != "x-real-ip" && name != "forwarded" && !is_reserved_edge_identity_header(name)
    });

    let peer_is_trusted = config
        .trusted_proxy_cidrs
        .iter()
        .any(|network| network.contains(&peer_ip));
    if !peer_is_trusted {
        return Ok(RequestConnectionInfo::direct(peer_ip));
    }

    let Some(value) = forwarded_for else {
        return Ok(RequestConnectionInfo::direct(peer_ip));
    };
    let hops = value.split(',').map(str::trim).collect::<Vec<_>>();
    if hops.is_empty() || hops.len() > config.max_forwarded_hops {
        return Err(());
    }
    let mut parsed_hops = Vec::with_capacity(hops.len());
    for hop in hops {
        if hop.is_empty() {
            return Err(());
        }
        parsed_hops.push(hop.parse::<IpAddr>().map_err(|_| ())?);
    }

    let mut effective_client = peer_ip;
    for hop in parsed_hops.into_iter().rev() {
        let current_is_trusted = config
            .trusted_proxy_cidrs
            .iter()
            .any(|network| network.contains(&effective_client));
        if !current_is_trusted {
            break;
        }
        effective_client = hop;
    }

    Ok(RequestConnectionInfo::from_trusted_transport(
        Some(peer_ip),
        Some(effective_client),
        true,
    ))
}

fn is_reserved_edge_identity_header(name: &str) -> bool {
    matches!(
        name,
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

#[allow(
    clippy::too_many_arguments,
    reason = "Upgrade execution carries a retained scope receipt owner"
)]
async fn execute_pipeline(
    container: Arc<ApplicationContainer>,
    chain: Arc<crate::middleware::CompiledWsHandshakeChain>,
    identity_middleware: Option<Arc<crate::middleware::CompiledWsIdentityMiddleware>>,
    request: WsHandshakeRequest,
    cancellation: CancellationToken,
    deadline: Instant,
    stage_timeout: Duration,
    scope_owner: (ScopeCleanupRegistry, Uuid),
) -> Result<Option<WebSocketIdentity>, PipelineFailure> {
    let mut scope = scope_owner
        .0
        .create_scope(scope_owner.1, &container, ProcessContext::new())
        .map_err(|_| PipelineFailure::Internal)?;
    let extensions = container.services();
    let execution = scope
        .run(async {
            let mut exchange =
                WsHandshakeExchange::new(extensions, request, cancellation, deadline)
                    .with_shutdown_budget(scope_owner.0.budget.clone());
            chain
                .execute(&mut exchange, stage_timeout)
                .await
                .map_err(PipelineFailure::Execution)?;
            match identity_middleware {
                Some(identity) => identity
                    .execute(&mut exchange, stage_timeout)
                    .await
                    .map(Some)
                    .map_err(PipelineFailure::Execution),
                None => Ok(None),
            }
        })
        .await
        .map_err(|_| PipelineFailure::Internal)?;

    match scope_owner
        .0
        .budget
        .cleanup(Some(deadline), scope.close())
        .await
    {
        Ok(Ok(())) => execution,
        Ok(Err(_)) | Err(_) => Err(PipelineFailure::Internal),
    }
}

fn finalize_identity(
    identity: Option<WebSocketIdentity>,
) -> (
    Option<crate::WebSocketIdentitySnapshot>,
    Option<Arc<RequestExtensions>>,
) {
    match identity {
        Some(identity) => {
            let (identity, connection_locals) = identity.into_parts();
            (Some(identity), Some(connection_locals))
        }
        None => (None, None),
    }
}

async fn probe_final_upgrade_boundary<S>(
    stream: &mut S,
    cancellation: &CancellationToken,
    deadline: Instant,
    handshake_timeout: Duration,
) -> Result<FinalUpgradeProbeOutcome, ServerError>
where
    S: AsyncRead + Unpin,
{
    let mut peer_probe = [0_u8; 1];
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Ok(FinalUpgradeProbeOutcome::Cancelled),
        peer = stream.read(&mut peer_probe) => match peer {
            Ok(0) => Ok(FinalUpgradeProbeOutcome::Disconnected),
            Ok(_) => Ok(FinalUpgradeProbeOutcome::EarlyData),
            Err(error) if is_peer_disconnect(&error) => {
                Ok(FinalUpgradeProbeOutcome::Disconnected)
            }
            Err(error) => Err(ServerError::IoError(error)),
        },
        _ = tokio::time::sleep_until(deadline) => {
            Err(ServerError::HandshakeTimeout(handshake_timeout))
        }
        _ = std::future::ready(()) => Ok(FinalUpgradeProbeOutcome::Clear),
    }
}

async fn read_request<S>(
    stream: &mut S,
    cancellation: &CancellationToken,
    deadline: Instant,
    handshake_timeout: Duration,
) -> Result<RequestReadOutcome, ServerError>
where
    S: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(2048);
    let mut chunk = [0_u8; 2048];
    loop {
        match Request::try_parse(&bytes) {
            Ok(Some((consumed, request))) => {
                if consumed != bytes.len() {
                    return Ok(RequestReadOutcome::Rejected(early_data_rejection()));
                }
                return Ok(RequestReadOutcome::Request(request));
            }
            Ok(None) => {}
            Err(_) => {
                return Ok(RequestReadOutcome::Rejected(transport_rejection(
                    StatusCode::BAD_REQUEST,
                    "WS_UPGRADE_INVALID",
                    "The WebSocket upgrade request is invalid.",
                )));
            }
        }
        if bytes.len() >= MAX_UPGRADE_REQUEST_BYTES {
            return Ok(RequestReadOutcome::Rejected(transport_rejection(
                StatusCode::BAD_REQUEST,
                "WS_UPGRADE_TOO_LARGE",
                "The WebSocket upgrade request is too large.",
            )));
        }
        let remaining = MAX_UPGRADE_REQUEST_BYTES - bytes.len();
        let read_capacity = remaining.min(chunk.len());
        let read = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(RequestReadOutcome::Cancelled),
            result = timeout_at(deadline, stream.read(&mut chunk[..read_capacity])) => {
                match result {
                    Ok(result) => result,
                    Err(_) => return Err(ServerError::HandshakeTimeout(handshake_timeout)),
                }
            }
        };
        match read {
            Ok(0) => return Ok(RequestReadOutcome::Disconnected),
            Ok(count) => bytes.extend_from_slice(&chunk[..count]),
            Err(error) if is_peer_disconnect(&error) => {
                return Ok(RequestReadOutcome::Disconnected);
            }
            Err(error) => return Err(ServerError::IoError(error)),
        }
    }
}

fn server_rejection(rejection: HandshakeRejection) -> WsHandshakeRejection {
    transport_rejection(
        rejection.status(),
        "WS_HANDSHAKE_REJECTED",
        rejection.response_message(),
    )
}

fn early_data_rejection() -> WsHandshakeRejection {
    transport_rejection(
        StatusCode::BAD_REQUEST,
        "WS_EARLY_DATA",
        "Data cannot be sent before the WebSocket upgrade completes.",
    )
}

fn transport_rejection(
    status: StatusCode,
    code: &'static str,
    body: &'static str,
) -> WsHandshakeRejection {
    let code = MiddlewareErrorCode::new(code).unwrap_or(MiddlewareErrorCode::INTERNAL);
    WsHandshakeRejection::try_new(status.as_u16(), code)
        .expect("transport rejection uses a valid HTTP error status")
        .try_with_body(body)
        .expect("transport rejection uses a bounded static body")
}

fn identity_expired_rejection() -> WsHandshakeRejection {
    // Lily does not own the application's authentication scheme and therefore
    // cannot synthesize the mandatory WWW-Authenticate challenge required by
    // a 401 response. The identity middleware can return its own typed 401
    // before this point; an identity that expires at the final Upgrade
    // boundary is a bounded 403 policy rejection.
    transport_rejection(
        StatusCode::FORBIDDEN,
        "WS_IDENTITY_EXPIRED",
        "The WebSocket identity has expired.",
    )
}

async fn write_rejection<S>(
    stream: &mut S,
    rejection: &WsHandshakeRejection,
    cancellation: &CancellationToken,
    deadline: Instant,
    handshake_timeout: Duration,
) -> Result<PreUpgradeOutcome, ServerError>
where
    S: AsyncWrite + Unpin,
{
    let mut response = tokio_tungstenite::tungstenite::http::Response::builder()
        .status(rejection.status())
        .version(Version::HTTP_11)
        .header(CONNECTION, "close")
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(CONTENT_LENGTH, rejection.public_body().len().to_string())
        .body(())
        .map_err(|_| {
            ServerError::connection_error("handshake rejection build failed".to_owned())
        })?;
    for (name, value) in rejection.headers() {
        response.headers_mut().append(name, value.clone());
    }
    let mut bytes = Vec::with_capacity(256 + rejection.public_body().len());
    write_response(&mut bytes, &response).map_err(ServerError::from)?;
    bytes.extend_from_slice(rejection.public_body().as_bytes());
    match write_bytes(stream, &bytes, cancellation, deadline, handshake_timeout).await? {
        WriteOutcome::Complete => Ok(PreUpgradeOutcome::Rejected(rejection.code())),
        WriteOutcome::Cancelled => Ok(PreUpgradeOutcome::Cancelled),
        WriteOutcome::Disconnected => Ok(PreUpgradeOutcome::Disconnected),
    }
}

async fn write_switching_protocols<S>(
    stream: &mut S,
    response: &Response,
    cancellation: &CancellationToken,
    deadline: Instant,
    handshake_timeout: Duration,
) -> Result<WriteOutcome, ServerError>
where
    S: AsyncWrite + Unpin,
{
    let mut bytes = Vec::with_capacity(256);
    write_response(&mut bytes, response).map_err(ServerError::from)?;
    write_bytes(stream, &bytes, cancellation, deadline, handshake_timeout).await
}

enum WriteOutcome {
    Complete,
    Cancelled,
    Disconnected,
}

async fn write_bytes<S>(
    stream: &mut S,
    bytes: &[u8],
    cancellation: &CancellationToken,
    deadline: Instant,
    handshake_timeout: Duration,
) -> Result<WriteOutcome, ServerError>
where
    S: AsyncWrite + Unpin,
{
    let write = async {
        stream.write_all(bytes).await?;
        stream.flush().await
    };
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Ok(WriteOutcome::Cancelled),
        result = timeout_at(deadline, write) => match result {
            Ok(Ok(())) => Ok(WriteOutcome::Complete),
            Ok(Err(error)) if is_peer_disconnect(&error) => Ok(WriteOutcome::Disconnected),
            Ok(Err(error)) => Err(ServerError::IoError(error)),
            Err(_) => Err(ServerError::HandshakeTimeout(handshake_timeout)),
        },
    }
}

fn is_peer_disconnect(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::NotConnected
    )
}

#[cfg(test)]
pub(super) mod test_support {
    use std::io;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::task::{Context, Poll, Waker};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::sync::Notify;

    pub(crate) const CONTROLLED_WRITE_ERROR: &str = "DEC-009 controlled HTTP 101 write failure";
    pub(crate) const EXPECTED_SWITCHING_PROTOCOLS_RESPONSE: &[u8] =
        b"HTTP/1.1 101 Switching Protocols\r\n\
connection: Upgrade\r\n\
upgrade: websocket\r\n\
sec-websocket-accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
\r\n";

    pub(crate) fn valid_upgrade_request(namespace: &str) -> Vec<u8> {
        format!(
            "GET /ws?namespace={namespace} HTTP/1.1\r\n\
Host: localhost\r\n\
Connection: Upgrade\r\n\
Upgrade: websocket\r\n\
Sec-WebSocket-Version: 13\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
\r\n"
        )
        .into_bytes()
    }

    pub(crate) fn exact_size_upgrade_request(namespace: &str, target_bytes: usize) -> Vec<u8> {
        let mut request = format!(
            "GET /ws?namespace={namespace} HTTP/1.1\r\n\
Host: localhost\r\n\
Connection: Upgrade\r\n\
Upgrade: websocket\r\n\
Sec-WebSocket-Version: 13\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        );
        const PAD_HEADERS: usize = 8;
        const MAX_PAD_VALUE_BYTES: usize = 8 * 1024;
        let overhead = (0..PAD_HEADERS)
            .map(|index| format!("X-Pad-{index}: \r\n").len())
            .sum::<usize>();
        let value_bytes = target_bytes
            .checked_sub(request.len() + overhead + 2)
            .expect("target can contain the canonical Upgrade request");
        assert!(
            value_bytes <= PAD_HEADERS * MAX_PAD_VALUE_BYTES,
            "padding stays within the per-header value bound"
        );

        let mut remaining = value_bytes;
        for index in 0..PAD_HEADERS {
            let value_len = remaining.min(MAX_PAD_VALUE_BYTES);
            remaining -= value_len;
            request.push_str(&format!("X-Pad-{index}: "));
            request.extend(std::iter::repeat_n('p', value_len));
            request.push_str("\r\n");
        }
        assert_eq!(remaining, 0);
        request.push_str("\r\n");
        assert_eq!(request.len(), target_bytes);
        request.into_bytes()
    }

    #[derive(Clone, Default)]
    pub(crate) struct ExactKWriteCapture {
        bytes: Arc<StdMutex<Vec<u8>>>,
        flushes: Arc<AtomicUsize>,
        flush_started: Arc<AtomicBool>,
        flush_started_notify: Arc<Notify>,
        flush_released: Arc<AtomicBool>,
        flush_waker: Arc<StdMutex<Option<Waker>>>,
        response_fully_flushed: Arc<AtomicBool>,
    }

    impl ExactKWriteCapture {
        pub(crate) fn bytes(&self) -> Vec<u8> {
            self.bytes.lock().unwrap().clone()
        }

        pub(crate) fn flush_count(&self) -> usize {
            self.flushes.load(Ordering::SeqCst)
        }

        pub(crate) async fn wait_for_flush_started(&self) {
            loop {
                let notified = self.flush_started_notify.notified();
                if self.flush_started.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }

        pub(crate) fn release_flush(&self) {
            self.flush_released.store(true, Ordering::Release);
            if let Some(waker) = self.flush_waker.lock().unwrap().take() {
                waker.wake();
            }
        }

        pub(crate) fn response_fully_flushed(&self) -> bool {
            self.response_fully_flushed.load(Ordering::Acquire)
        }
    }

    pub(crate) struct ExactKHandshakeStream {
        request: Vec<u8>,
        request_offset: usize,
        fail_after: Option<usize>,
        gate_flush: bool,
        capture: ExactKWriteCapture,
    }

    impl ExactKHandshakeStream {
        pub(crate) fn fail_after(
            request: Vec<u8>,
            accepted_bytes: usize,
        ) -> (Self, ExactKWriteCapture) {
            Self::new(request, Some(accepted_bytes), false)
        }

        pub(crate) fn gated_complete(request: Vec<u8>) -> (Self, ExactKWriteCapture) {
            Self::new(request, None, true)
        }

        pub(crate) fn complete(request: Vec<u8>) -> (Self, ExactKWriteCapture) {
            Self::new(request, None, false)
        }

        fn new(
            request: Vec<u8>,
            fail_after: Option<usize>,
            gate_flush: bool,
        ) -> (Self, ExactKWriteCapture) {
            let capture = ExactKWriteCapture::default();
            (
                Self {
                    request,
                    request_offset: 0,
                    fail_after,
                    gate_flush,
                    capture: capture.clone(),
                },
                capture,
            )
        }
    }

    impl AsyncRead for ExactKHandshakeStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.request_offset == this.request.len() {
                return Poll::Pending;
            }

            let count = buffer
                .remaining()
                .min(this.request.len() - this.request_offset);
            let end = this.request_offset + count;
            buffer.put_slice(&this.request[this.request_offset..end]);
            this.request_offset = end;
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for ExactKHandshakeStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let mut captured = this.capture.bytes.lock().unwrap();
            let accepted = match this.fail_after {
                Some(limit) if captured.len() >= limit => {
                    return Poll::Ready(Err(io::Error::other(CONTROLLED_WRITE_ERROR)));
                }
                Some(limit) => bytes.len().min(limit - captured.len()),
                None => bytes.len(),
            };
            captured.extend_from_slice(&bytes[..accepted]);
            Poll::Ready(Ok(accepted))
        }

        fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.capture.flush_started.store(true, Ordering::Release);
            this.capture.flush_started_notify.notify_waiters();

            if this.gate_flush && !this.capture.flush_released.load(Ordering::Acquire) {
                let mut waker = this.capture.flush_waker.lock().unwrap();
                *waker = Some(context.waker().clone());
                if !this.capture.flush_released.load(Ordering::Acquire) {
                    return Poll::Pending;
                }
                waker.take();
            }

            if !this
                .capture
                .response_fully_flushed
                .swap(true, Ordering::AcqRel)
            {
                this.capture.flushes.fetch_add(1, Ordering::SeqCst);
            }
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn upgrade_trace_extraction_is_unambiguous_and_independent_of_ambient_context() {
        use opentelemetry::trace::TraceContextExt;
        lily_trace::install_w3c_propagator();
        let mut request = Request::builder().uri("/ws").body(()).unwrap();
        request.headers_mut().insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
                .parse()
                .unwrap(),
        );
        request
            .headers_mut()
            .insert("tracestate", "vendor=test".parse().unwrap());
        let incoming = request_trace_context(&request);
        let reference = incoming.span();
        let context = reference.span_context();
        assert!(context.is_valid());
        assert!(context.is_remote());
        assert!(!context.is_sampled());
        assert_eq!(context.trace_state().header(), "vendor=test");
        let _ambient = incoming.clone().attach();
        request
            .headers_mut()
            .append("traceparent", "duplicate".parse().unwrap());
        assert!(
            !request_trace_context(&request)
                .span()
                .span_context()
                .is_valid()
        );
        request
            .headers_mut()
            .insert("traceparent", "invalid".parse().unwrap());
        assert!(
            !request_trace_context(&request)
                .span()
                .span_context()
                .is_valid()
        );
        request.headers_mut().remove("traceparent");
        assert!(
            !request_trace_context(&request)
                .span()
                .span_context()
                .is_valid()
        );
    }

    use super::test_support::{
        CONTROLLED_WRITE_ERROR, EXPECTED_SWITCHING_PROTOCOLS_RESPONSE, ExactKHandshakeStream,
        exact_size_upgrade_request, valid_upgrade_request,
    };
    use super::*;
    use crate::app::{WsApp, WsAppBuilder};
    use async_trait::async_trait;
    use lily_error::injection::InjectionError;
    use lily_injection::Injectable;
    use lily_injection::{Extensions, ServiceTrait};
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, duplex};

    static PIPELINE_SCOPE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static SCOPED_PROBE_INITIALIZED: AtomicUsize = AtomicUsize::new(0);
    static SCOPED_PROBE_DISPOSED: AtomicUsize = AtomicUsize::new(0);
    static SCOPED_PROBE_RESOLVED: AtomicUsize = AtomicUsize::new(0);

    #[derive(Default, Injectable)]
    #[service(lifetime = "Scoped")]
    struct HandshakeScopedProbe;

    #[async_trait]
    impl ServiceTrait for HandshakeScopedProbe {
        async fn initialize(&mut self) -> Result<(), InjectionError> {
            assert!(ProcessContext::current().is_some());
            SCOPED_PROBE_INITIALIZED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn dispose(&self) -> Result<(), InjectionError> {
            assert!(ProcessContext::current().is_some());
            SCOPED_PROBE_DISPOSED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    enum ScopedProbeBehavior {
        Success,
        Reject,
        Pending,
    }

    struct ScopedProbeHandshake {
        behavior: ScopedProbeBehavior,
    }

    #[async_trait]
    impl crate::middleware::WebSocketHandshakeMiddleware for ScopedProbeHandshake {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self {
                behavior: ScopedProbeBehavior::Success,
            })
        }

        fn descriptor(&self) -> crate::middleware::MiddlewareDescriptor {
            crate::middleware::MiddlewareDescriptor::new(
                "handshake_scoped_probe",
                crate::middleware::MiddlewareKind::WebSocketHandshake,
            )
        }

        async fn handle(
            &self,
            exchange: &mut WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WsHandshakeRejection> {
            let _service = exchange
                .service::<HandshakeScopedProbe>()
                .await
                .expect("handshake scope must resolve its scoped service");
            SCOPED_PROBE_RESOLVED.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                ScopedProbeBehavior::Success => Ok(()),
                ScopedProbeBehavior::Reject => Err(WsHandshakeRejection::forbidden(
                    MiddlewareErrorCode::new("WS_TEST_SCOPED_REJECTED").unwrap(),
                )),
                ScopedProbeBehavior::Pending => pending().await,
            }
        }
    }

    fn reset_scoped_probe() {
        SCOPED_PROBE_INITIALIZED.store(0, Ordering::SeqCst);
        SCOPED_PROBE_DISPOSED.store(0, Ordering::SeqCst);
        SCOPED_PROBE_RESOLVED.store(0, Ordering::SeqCst);
    }

    fn scoped_probe_chain(
        behavior: ScopedProbeBehavior,
    ) -> Arc<crate::middleware::CompiledWsHandshakeChain> {
        Arc::new(
            crate::middleware::CompiledWsHandshakeChain::compile(vec![Arc::new(
                ScopedProbeHandshake { behavior },
            )])
            .expect("compile scoped handshake probe"),
        )
    }

    static COOPERATIVE_HANDSHAKE_RETURNED: AtomicUsize = AtomicUsize::new(0);
    struct CooperativeHandshake;
    #[async_trait]
    impl crate::middleware::WebSocketHandshakeMiddleware for CooperativeHandshake {
        async fn new(_: Arc<Extensions>) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }
        fn descriptor(&self) -> crate::middleware::MiddlewareDescriptor {
            crate::middleware::MiddlewareDescriptor::new(
                "cooperative_handshake",
                crate::middleware::MiddlewareKind::WebSocketHandshake,
            )
        }
        async fn handle(
            &self,
            exchange: &mut WsHandshakeExchange,
            signal: crate::ExecutionCancellation,
        ) -> Result<(), WsHandshakeRejection> {
            exchange.service::<HandshakeScopedProbe>().await.unwrap();
            SCOPED_PROBE_RESOLVED.fetch_add(1, Ordering::SeqCst);
            signal.cancelled().await;
            assert!(exchange.cancellation().is_cancelled());
            tokio::time::sleep(Duration::from_millis(5)).await;
            COOPERATIVE_HANDSHAKE_RETURNED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn peer_or_early_data_with_force_race_cancels_handshake_cooperatively() {
        let _lock = PIPELINE_SCOPE_TEST_LOCK.lock().await;
        for (early_data, forced) in [(false, false), (true, false), (false, true)] {
            reset_scoped_probe();
            COOPERATIVE_HANDSHAKE_RETURNED.store(0, Ordering::SeqCst);
            let app = Arc::new(
                WsAppBuilder::new("127.0.0.1:0")
                    .config(ServerConfig {
                        allow_missing_origin: true,
                        ..ServerConfig::default()
                    })
                    .handshake_middleware::<CooperativeHandshake>()
                    .build()
                    .await
                    .unwrap(),
            );
            let (mut client, mut server) = tokio::io::duplex(4096);
            let source = CancellationToken::new();
            let pipeline_source = source.clone();
            let runtime = Arc::clone(&app);
            let task = tokio::spawn(async move {
                perform_pre_upgrade(
                    &mut server,
                    "127.0.0.1:12345".parse().unwrap(),
                    &runtime.action_table,
                    &runtime.container,
                    runtime.server_config(),
                    WsTransportSecurity::Plaintext,
                    &pipeline_source,
                    Instant::now() + Duration::from_secs(1),
                    Duration::from_secs(1),
                    &runtime.scope_cleanup_registry,
                    Uuid::new_v4(),
                )
                .await
            });
            client
                .write_all(&test_support::valid_upgrade_request("orders"))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), async {
                while SCOPED_PROBE_RESOLVED.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if forced {
                app.scope_cleanup_registry
                    .budget
                    .force_before(Instant::now() + Duration::from_millis(200));
                source.cancel();
            }
            if early_data {
                client.write_all(&[0x81]).await.unwrap();
            } else {
                client.shutdown().await.unwrap();
            }
            let result = tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(COOPERATIVE_HANDSHAKE_RETURNED.load(Ordering::SeqCst), 1);
            if early_data {
                assert!(matches!(result, PreUpgradeOutcome::Rejected(_)));
                let mut response = Vec::new();
                client.read_to_end(&mut response).await.unwrap();
                assert!(response.starts_with(b"HTTP/1.1 400 Bad Request"));
            } else {
                assert!(matches!(result, PreUpgradeOutcome::Disconnected));
            }
            assert_eq!(
                source.is_cancelled(),
                forced,
                "peer execution stop must not revoke transport response authority"
            );
            assert_eq!(app.scope_cleanup_registry.drain().await.outstanding, 0);
            assert_eq!(SCOPED_PROBE_DISPOSED.load(Ordering::SeqCst), 1);
            app.container.close().await.unwrap();
        }
    }

    fn test_handshake_request() -> WsHandshakeRequest {
        let peer_addr = "127.0.0.1:12345".parse().unwrap();
        WsHandshakeRequest::new(
            "orders".to_owned(),
            WsHeaders::default(),
            peer_addr,
            RequestConnectionInfo::direct(peer_addr.ip()),
            WsTransportSecurity::Plaintext,
        )
    }

    async fn wait_for_scope_cleanup(container: &ApplicationContainer, expected_disposals: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if container.active_scope_count() == 0
                    && SCOPED_PROBE_DISPOSED.load(Ordering::SeqCst) == expected_disposals
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("handshake scope cleanup deadline");
    }

    async fn bare_pre_upgrade_runtime() -> WsApp {
        WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build bare pre-upgrade runtime")
    }

    async fn perform_test_pre_upgrade(
        stream: &mut ExactKHandshakeStream,
        app: &WsApp,
    ) -> Result<PreUpgradeOutcome, ServerError> {
        let handshake_timeout = Duration::from_secs(1);
        tokio::time::timeout(
            Duration::from_secs(2),
            perform_pre_upgrade(
                stream,
                "127.0.0.1:12345".parse().unwrap(),
                &app.action_table,
                &app.container,
                app.server_config(),
                WsTransportSecurity::Plaintext,
                &CancellationToken::new(),
                Instant::now() + handshake_timeout,
                handshake_timeout,
                &app.scope_cleanup_registry,
                Uuid::new_v4(),
            ),
        )
        .await
        .expect("pre-upgrade operation must finish within its bounded deadline")
    }

    #[test]
    fn absent_identity_does_not_materialize_a_second_connection_local_map() {
        let (identity, connection_locals) = finalize_identity(None);

        assert!(identity.is_none());
        assert!(connection_locals.is_none());
    }

    #[test]
    fn configured_identity_retains_its_connection_local_map_even_when_empty() {
        let (identity, connection_locals) = finalize_identity(Some(WebSocketIdentity::anonymous()));

        assert!(identity.is_some());
        assert!(identity.unwrap().principal().is_none());
        assert!(connection_locals.is_some());
        assert!(connection_locals.unwrap().is_empty());
    }

    #[test]
    fn framework_generated_expired_identity_rejection_is_a_bounded_403() {
        let rejection = identity_expired_rejection();

        assert_eq!(rejection.status(), StatusCode::FORBIDDEN);
        assert_eq!(rejection.code().as_str(), "WS_IDENTITY_EXPIRED");
        assert_eq!(
            rejection.public_body(),
            "The WebSocket identity has expired."
        );
        assert!(rejection.headers().is_empty());
    }

    #[test]
    fn untrusted_peer_cannot_spoof_forwarding_or_edge_identity_headers() {
        let peer = "203.0.113.9".parse::<IpAddr>().unwrap();
        let mut headers = HashMap::from([
            ("x-forwarded-for".to_owned(), "198.51.100.40".to_owned()),
            ("x-real-ip".to_owned(), "198.51.100.41".to_owned()),
            ("forwarded".to_owned(), "for=198.51.100.42".to_owned()),
            ("x-lily-subject".to_owned(), "attacker".to_owned()),
            ("x-auth-request-user".to_owned(), "attacker".to_owned()),
            (
                "x-forwarded-client-cert".to_owned(),
                "spoofed-certificate".to_owned(),
            ),
            (
                "authorization".to_owned(),
                "Bearer end-to-end-token".to_owned(),
            ),
        ]);

        let info = resolve_connection_info(peer, &mut headers, &ServerConfig::default())
            .expect("untrusted forwarding input is ignored, not trusted");

        assert_eq!(info.peer_ip(), Some(peer));
        assert_eq!(info.client_ip(), Some(peer));
        assert!(!info.via_trusted_proxy());
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("authorization").map(String::as_str),
            Some("Bearer end-to-end-token")
        );
    }

    #[test]
    fn trusted_proxy_chain_walks_right_to_left_and_stops_at_first_untrusted_hop() {
        let config = ServerConfig {
            trusted_proxy_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
            ..ServerConfig::default()
        };
        let peer = "10.2.3.4".parse::<IpAddr>().unwrap();
        let mut headers = HashMap::from([(
            "x-forwarded-for".to_owned(),
            "203.0.113.77, 198.51.100.20, 10.1.2.3".to_owned(),
        )]);

        let info = resolve_connection_info(peer, &mut headers, &config)
            .expect("valid trusted chain must resolve");

        assert_eq!(info.peer_ip(), Some(peer));
        assert_eq!(
            info.client_ip(),
            Some("198.51.100.20".parse::<IpAddr>().unwrap())
        );
        assert!(info.via_trusted_proxy());
        assert!(headers.is_empty(), "raw forwarding header must be removed");
    }

    #[test]
    fn trusted_peer_without_forwarded_for_remains_a_direct_connection() {
        let config = ServerConfig {
            trusted_proxy_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
            ..ServerConfig::default()
        };
        let peer = "10.2.3.4".parse::<IpAddr>().unwrap();
        let mut headers = HashMap::from([
            ("x-real-ip".to_owned(), "198.51.100.41".to_owned()),
            ("forwarded".to_owned(), "for=198.51.100.42".to_owned()),
            ("x-lily-subject".to_owned(), "attacker".to_owned()),
        ]);

        let info = resolve_connection_info(peer, &mut headers, &config)
            .expect("missing X-Forwarded-For keeps socket-peer authority");

        assert_eq!(info.peer_ip(), Some(peer));
        assert_eq!(info.client_ip(), Some(peer));
        assert!(!info.via_trusted_proxy());
        assert!(headers.is_empty());
    }

    #[test]
    fn malformed_or_oversized_forwarding_data_from_trusted_proxy_fails_closed() {
        let config = ServerConfig {
            trusted_proxy_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
            max_forwarded_hops: 2,
            ..ServerConfig::default()
        };
        let peer = "10.2.3.4".parse::<IpAddr>().unwrap();

        for value in [
            "not-an-ip",
            "198.51.100.1,,10.1.2.3",
            "198.51.100.1,10.1.2.3,10.2.3.4",
        ] {
            let mut headers = HashMap::from([("x-forwarded-for".to_owned(), value.to_owned())]);
            assert!(
                resolve_connection_info(peer, &mut headers, &config).is_err(),
                "trusted proxy contract violation must fail closed"
            );
            assert!(
                !headers.contains_key("x-forwarded-for"),
                "untrusted forwarding data must be stripped before failure"
            );
        }
    }

    #[tokio::test]
    async fn handshake_scope_resolves_and_disposes_scoped_service_after_success() {
        let _lock = PIPELINE_SCOPE_TEST_LOCK.lock().await;
        reset_scoped_probe();
        let container = Arc::new(ApplicationContainer::build().await.unwrap());

        let result = execute_pipeline(
            Arc::clone(&container),
            scoped_probe_chain(ScopedProbeBehavior::Success),
            None,
            test_handshake_request(),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            (ScopeCleanupRegistry::default(), Uuid::new_v4()),
        )
        .await;

        assert!(matches!(result, Ok(None)));
        assert_eq!(SCOPED_PROBE_INITIALIZED.load(Ordering::SeqCst), 1);
        assert_eq!(SCOPED_PROBE_RESOLVED.load(Ordering::SeqCst), 1);
        assert_eq!(SCOPED_PROBE_DISPOSED.load(Ordering::SeqCst), 1);
        assert_eq!(container.active_scope_count(), 0);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn handshake_scope_disposes_scoped_service_after_rejection() {
        let _lock = PIPELINE_SCOPE_TEST_LOCK.lock().await;
        reset_scoped_probe();
        let container = Arc::new(ApplicationContainer::build().await.unwrap());

        let result = execute_pipeline(
            Arc::clone(&container),
            scoped_probe_chain(ScopedProbeBehavior::Reject),
            None,
            test_handshake_request(),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            (ScopeCleanupRegistry::default(), Uuid::new_v4()),
        )
        .await;

        assert!(matches!(
            result,
            Err(PipelineFailure::Execution(ref failure))
                if matches!(failure.kind(), WsHandshakeExecutionFailureKind::Rejected(_))
        ));
        assert_eq!(SCOPED_PROBE_INITIALIZED.load(Ordering::SeqCst), 1);
        assert_eq!(SCOPED_PROBE_RESOLVED.load(Ordering::SeqCst), 1);
        assert_eq!(SCOPED_PROBE_DISPOSED.load(Ordering::SeqCst), 1);
        assert_eq!(container.active_scope_count(), 0);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn handshake_scope_disposes_scoped_service_after_stage_timeout() {
        let _lock = PIPELINE_SCOPE_TEST_LOCK.lock().await;
        reset_scoped_probe();
        let container = Arc::new(ApplicationContainer::build().await.unwrap());

        let result = execute_pipeline(
            Arc::clone(&container),
            scoped_probe_chain(ScopedProbeBehavior::Pending),
            None,
            test_handshake_request(),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(2),
            Duration::from_millis(100),
            (ScopeCleanupRegistry::default(), Uuid::new_v4()),
        )
        .await;

        assert!(matches!(
            result,
            Err(PipelineFailure::Execution(ref failure))
                if matches!(failure.kind(), WsHandshakeExecutionFailureKind::Timeout)
        ));
        assert_eq!(SCOPED_PROBE_INITIALIZED.load(Ordering::SeqCst), 1);
        assert_eq!(SCOPED_PROBE_RESOLVED.load(Ordering::SeqCst), 1);
        assert_eq!(SCOPED_PROBE_DISPOSED.load(Ordering::SeqCst), 1);
        assert_eq!(container.active_scope_count(), 0);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn pre_cancelled_handshake_closes_scope_without_polling_application_code() {
        let _lock = PIPELINE_SCOPE_TEST_LOCK.lock().await;
        reset_scoped_probe();
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let result = execute_pipeline(
            Arc::clone(&container),
            scoped_probe_chain(ScopedProbeBehavior::Success),
            None,
            test_handshake_request(),
            cancellation,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            (ScopeCleanupRegistry::default(), Uuid::new_v4()),
        )
        .await;

        assert!(matches!(
            result,
            Err(PipelineFailure::Execution(ref failure))
                if matches!(failure.kind(), WsHandshakeExecutionFailureKind::Cancelled)
        ));
        assert_eq!(SCOPED_PROBE_INITIALIZED.load(Ordering::SeqCst), 0);
        assert_eq!(SCOPED_PROBE_RESOLVED.load(Ordering::SeqCst), 0);
        assert_eq!(SCOPED_PROBE_DISPOSED.load(Ordering::SeqCst), 0);
        assert_eq!(container.active_scope_count(), 0);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn dropped_handshake_pipeline_owns_cleanup_after_peer_disconnect() {
        let _lock = PIPELINE_SCOPE_TEST_LOCK.lock().await;
        reset_scoped_probe();
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let task = tokio::spawn(execute_pipeline(
            Arc::clone(&container),
            scoped_probe_chain(ScopedProbeBehavior::Pending),
            None,
            test_handshake_request(),
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(10),
            Duration::from_secs(10),
            (ScopeCleanupRegistry::default(), Uuid::new_v4()),
        ));

        tokio::time::timeout(Duration::from_secs(2), async {
            while SCOPED_PROBE_RESOLVED.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scoped service resolution deadline");
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));

        wait_for_scope_cleanup(&container, 1).await;
        assert_eq!(SCOPED_PROBE_INITIALIZED.load(Ordering::SeqCst), 1);
        assert_eq!(SCOPED_PROBE_RESOLVED.load(Ordering::SeqCst), 1);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_final_write_never_publishes_upgrade_bytes() {
        let (mut server, mut client) = duplex(1024);
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let outcome = write_bytes(
            &mut server,
            b"HTTP/1.1 101 Switching Protocols\r\n\r\n",
            &cancellation,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await
        .expect("cancellation is not a transport failure");

        assert!(matches!(outcome, WriteOutcome::Cancelled));
        drop(server);

        let mut published = Vec::new();
        client
            .read_to_end(&mut published)
            .await
            .expect("read final write observation");
        assert!(
            published.is_empty(),
            "a cancelled handshake must not publish a partial or complete HTTP response"
        );
    }

    #[tokio::test]
    async fn hsk_04_exact_request_limit_with_queued_early_byte_is_rejected_before_101() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let app = bare_pre_upgrade_runtime().await;
            let mut request = exact_size_upgrade_request("orders", MAX_UPGRADE_REQUEST_BYTES);
            request.push(0x81);
            let (mut stream, capture) = ExactKHandshakeStream::complete(request);

            let result = perform_test_pre_upgrade(&mut stream, &app).await;

            assert!(
                matches!(
                    result,
                    Ok(PreUpgradeOutcome::Rejected(ref code))
                        if code.as_str() == "WS_EARLY_DATA"
                ),
                "a queued peer byte at the exact request limit must fail closed"
            );
            let response = String::from_utf8(capture.bytes()).expect("ASCII HTTP rejection");
            assert!(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
            assert!(!response.contains("101 Switching Protocols"));
            assert!(
                response.ends_with("Data cannot be sent before the WebSocket upgrade completes.")
            );
            assert_eq!(capture.flush_count(), 1);
            assert!(capture.response_fully_flushed());

            app.container.close().await.unwrap();
        })
        .await
        .expect("HSK-04 exact-limit early-data regression exceeded its bounded deadline");
    }

    #[tokio::test]
    async fn hsk_04_exact_request_limit_without_early_data_still_accepts() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let app = bare_pre_upgrade_runtime().await;
            let request = exact_size_upgrade_request("orders", MAX_UPGRADE_REQUEST_BYTES);
            let (mut stream, capture) = ExactKHandshakeStream::complete(request);

            let result = perform_test_pre_upgrade(&mut stream, &app).await;

            assert!(matches!(result, Ok(PreUpgradeOutcome::Accepted(_))));
            assert_eq!(capture.bytes(), EXPECTED_SWITCHING_PROTOCOLS_RESPONSE);
            assert_eq!(capture.flush_count(), 1);
            assert!(capture.response_fully_flushed());

            app.container.close().await.unwrap();
        })
        .await
        .expect("HSK-04 exact-limit control exceeded its bounded deadline");
    }

    #[tokio::test]
    async fn dec_009_exact_k_partial_101_write_never_returns_accepted() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let app = bare_pre_upgrade_runtime().await;
            let response_len = EXPECTED_SWITCHING_PROTOCOLS_RESPONSE.len();

            for accepted_bytes in [0, 1, response_len - 1] {
                let (mut stream, capture) = ExactKHandshakeStream::fail_after(
                    valid_upgrade_request("orders"),
                    accepted_bytes,
                );
                let result = perform_test_pre_upgrade(&mut stream, &app).await;

                assert!(
                    !matches!(&result, Ok(PreUpgradeOutcome::Accepted(_))),
                    "a partial HTTP 101 response must never publish an accepted handshake"
                );
                assert!(
                    matches!(
                        &result,
                        Err(ServerError::IoError(error))
                            if error.kind() == std::io::ErrorKind::Other
                                && error.to_string() == CONTROLLED_WRITE_ERROR
                    ),
                    "the exact-K writer must surface its controlled I/O failure"
                );
                assert_eq!(
                    capture.bytes(),
                    EXPECTED_SWITCHING_PROTOCOLS_RESPONSE[..accepted_bytes],
                    "the transport may retain exactly the accepted TCP prefix"
                );
                assert_eq!(
                    capture.flush_count(),
                    0,
                    "a partial response must fail before the final flush"
                );
            }

            app.container.close().await.unwrap();
        })
        .await
        .expect("DEC-009 exact-K matrix exceeded its bounded deadline");
    }

    #[tokio::test]
    async fn dec_009_complete_101_write_returns_accepted_after_exact_response_and_flush() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let app = bare_pre_upgrade_runtime().await;
            let (mut stream, capture) =
                ExactKHandshakeStream::gated_complete(valid_upgrade_request("orders"));
            let pre_upgrade = perform_test_pre_upgrade(&mut stream, &app);
            tokio::pin!(pre_upgrade);

            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    () = capture.wait_for_flush_started() => {}
                    _ = &mut pre_upgrade => {
                        panic!("the handshake must not be accepted while the final flush is gated")
                    }
                }
            })
            .await
            .expect("the complete response must reach its gated flush");
            assert_eq!(capture.bytes(), EXPECTED_SWITCHING_PROTOCOLS_RESPONSE);
            assert!(!capture.response_fully_flushed());
            assert_eq!(capture.flush_count(), 0);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut pre_upgrade)
                    .await
                    .is_err(),
                "Accepted must remain pending until the complete response flush is released"
            );

            capture.release_flush();
            let result = pre_upgrade.await;

            assert!(matches!(result, Ok(PreUpgradeOutcome::Accepted(_))));
            assert_eq!(capture.bytes(), EXPECTED_SWITCHING_PROTOCOLS_RESPONSE);
            assert!(capture.response_fully_flushed());
            assert_eq!(
                capture.flush_count(),
                1,
                "Accepted is returned only after the complete response is flushed"
            );

            app.container.close().await.unwrap();
        })
        .await
        .expect("DEC-009 complete response control exceeded its bounded deadline");
    }

    #[tokio::test]
    async fn completed_rejection_preserves_its_safe_diagnostic_code() {
        let (mut server, mut client) = duplex(1024);
        let code = MiddlewareErrorCode::new("WS_TEST_REJECTED").unwrap();
        let rejection = WsHandshakeRejection::forbidden(code);
        let cancellation = CancellationToken::new();

        let server_task = tokio::spawn(async move {
            write_rejection(
                &mut server,
                &rejection,
                &cancellation,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .await
        });

        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read rejection response");
        let outcome = server_task
            .await
            .expect("rejection writer task did not panic")
            .expect("rejection write succeeded");

        assert!(matches!(outcome, PreUpgradeOutcome::Rejected(actual) if actual == code));
        assert!(!String::from_utf8_lossy(&response).contains(code.as_str()));
    }
}
