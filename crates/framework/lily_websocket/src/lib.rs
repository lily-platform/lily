//! Production WebSocket server and struct-controller routing for Lily applications.
//!
//! Lily exposes one configured Upgrade endpoint. A client selects an exact
//! namespace during the handshake and sends strict Lily v2 envelopes whose
//! event is written as `namespace:action`. Application code derives
//! [`WebSocketController`], implements [`WebSocketControllerTrait`], declares
//! operations with [`websocket_controller`], and builds one [`WsApp`] with
//! [`WsAppBuilder`]. Each controller is constructed exactly once per app.
//!
//! Define DI services through `lily_websocket::{Injectable, ServiceTrait}`.
//! The root also exposes the DI error, container and scope APIs. No additional
//! derive, registry or `linkme` dependency is needed for service registration.
//!
//! Register host-owned workers with [`WsAppBuilder::add_background_service`].
//! They implement [`BackgroundServiceTrait`] and receive an
//! `Arc<ApplicationScopeFactory>` for isolated job scopes. Constructors run
//! during build; execution starts once after listener bind and required
//! backplane readiness. An unhandled worker error or panic stops this host.
//! [`BackgroundCancellation`] is the shared host-shutdown signal; the existing
//! [`ExecutionCancellation`] remains specific to WebSocket callbacks and message
//! deadlines. Worker and scope termination precede dependency disposal and
//! telemetry flush under the same application shutdown budget.
//!
//! # Minimal server
//!
//! ```no_run
//! use std::sync::Arc;
//! use lily_websocket::{
//!     Extensions, NoReply, Payload, ServerConfig, WebSocketActionError, WebSocketContext,
//!     WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait,
//!     WsAppBuilder, async_trait, websocket_controller,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize, Serialize)]
//! struct SendMessage {
//!     text: String,
//! }
//!
//! #[derive(WebSocketController)]
//! #[namespace("chat")]
//! struct ChatController;
//!
//! #[async_trait]
//! impl WebSocketControllerTrait for ChatController {
//!     async fn new(
//!         _extensions: Arc<Extensions>,
//!     ) -> Result<Self, WebSocketControllerInitError> {
//!         Ok(Self)
//!     }
//! }
//!
//! #[websocket_controller]
//! impl ChatController {
//!     #[message("send")]
//!     async fn send(
//!         &self,
//!         context: WebSocketContext,
//!         Payload(input): Payload<SendMessage>,
//!     ) -> Result<NoReply, WebSocketActionError> {
//!         context
//!             .clients()
//!             .others()
//!             .send("chat:received", &input)
//!             .await
//!             .map_err(WebSocketActionError::internal)?;
//!         Ok(NoReply)
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let app = WsAppBuilder::new("127.0.0.1:8080")
//!     .config(ServerConfig::default())
//!     .build()
//!     .await?;
//! app.start().await?;
//! # Ok(())
//! # }
//! ```
//!
//! The canonical middleware contracts are
//! [`middleware::WebSocketHandshakeMiddleware`],
//! [`middleware::WsConnectionMiddleware`], and
//! [`middleware::WsMessageMiddleware`]. Every middleware type is constructed
//! once from the application DI container. Handshake hooks then execute in a
//! per-upgrade DI scope after transport and Origin validation but before HTTP
//! `101`; connection middleware starts only after a successful Upgrade.
//!
//! Application messages execute sequentially per connection. While one action
//! is pending, Lily continues reading Ping, Pong, and Close control frames and
//! retains a count-and-byte bounded queue of complete application frames for
//! later dispatch. [`ServerConfig::inbound_queue_capacity`] defaults to 16 and
//! [`ServerConfig::inbound_queue_max_bytes`] defaults to 1 MiB. Waiting frames
//! create neither action tasks nor DI scopes. Crossing either limit closes the
//! connection with the bounded policy-violation reason instead of pausing
//! socket reads or growing remote-controlled memory. Graceful shutdown discards
//! the whole waiting queue, finishes the already-admitted action and its scope
//! cleanup, then publishes the server-shutdown Close.
//!
//! Outbound Text and Binary application messages have independent
//! per-connection count and byte admission bounds. Once acquired, the byte
//! reservation remains charged while the frame waits for a message-count slot,
//! while it is queued, and through completion of the socket write/flush;
//! protocol-control traffic uses its separate priority path.
//! [`ServerConfig::outbound_admission_timeout_millis`] bounds the aggregate
//! wait for both capacities. Broadcast recipients are admitted concurrently,
//! so one saturated connection does not serialize admission to healthy peers.
//! If the deadline expires, Lily stops only that live transport generation from
//! accepting application frames and makes a best-effort Close `1013` request
//! with reason `lily.v2.slow_consumer`. Transport failure can prevent the peer
//! from observing that Close; the send still reports the target as
//! backpressured rather than successful. This admission deadline is distinct
//! from [`ServerConfig::write_timeout_millis`], which bounds socket I/O after
//! admission.
//!
//! This policy governs only currently live connections. Lily deliberately
//! keeps no per-connection event log, reconnect sequence, durable delivery
//! ACK/checkpoint ledger, or replay buffer. This is distinct from Lily v2's
//! exchange-local request/response correlation: an inbound event can provide
//! an `ack_id` and its response can be an ACK, but that ACK is not a reconnect
//! delivery receipt. Applications requiring loss-intolerant delivery must own
//! durable outbox/event-log plus sequence/ACK replay above the WebSocket
//! transport boundary.
//!
//! Forwarding metadata is untrusted and stripped unless the socket peer belongs
//! to an explicit [`ServerConfig::trusted_proxy_cidrs`] network. Valid trusted
//! `X-Forwarded-For` chains become a typed [`RequestConnectionInfo`]; invalid
//! trusted input is rejected before application identity code. WS/WSS state is
//! selected by the listener and exposed as [`WsTransportSecurity`], never
//! inferred from the client-controlled `Origin` header.
//!
//! Authentication remains application-owned. An app may install one
//! [`middleware::WebSocketIdentityMiddleware`] with
//! [`WsAppBuilder::identity_middleware`]. It returns the shared
//! [`Principal`] inside an [`AuthenticatedWebSocketIdentity`] plus immutable
//! connection-local state, or a bounded [`middleware::WsHandshakeRejection`]
//! before the socket is upgraded. The identity's exact, bounded [`PrincipalId`]
//! comes from `Principal::subject` and is redacted in `Debug` output. Lily does
//! not provide a session store, JWT/OIDC verifier, login flow, or refresh flow.
//! Guards are message authorization boundaries and are constructed through
//! [`guard::WsGuard::new`]; they do not authenticate the Upgrade.
//!
//! [`AuthenticatedWebSocketIdentity::expires_at`] optionally gives Lily a hard
//! monotonic deadline. The application may verify a refreshed credential and
//! publish it with revision-checked [`WebSocketContext::reauthenticate`]. One
//! message captures one immutable principal snapshot before its middleware,
//! guards, extractors, and action execute; a successful concurrent refresh is
//! visible starting with the next message. If the current revision expires,
//! Lily removes its targeting membership and sends the bounded
//! `lily.v2.identity_expired` policy close. Without an identity middleware no
//! principal-index map or identity deadline is maintained.
//!
//! [`WebSocketClients::principal`] targets every online connection for one
//! exact principal in the current controller namespace. It uses the same local
//! dispatcher and optional distributed backplane as other targets. Lily never
//! infers whether the caller is authorized to address that principal; this
//! decision remains in application guards and services.
//!
//! Registration descriptors are link-time metadata only. Live controllers,
//! guards, and handlers are materialized into an immutable app-local table;
//! applications should not use the hidden registration ABI directly.
//!
//! Shutdown responsibility follows ownership: Lily manages its DI resources,
//! lifecycle scopes, and framework-tracked tasks. Resources directly created
//! by application code and raw `tokio::spawn` tasks are application-owned and
//! excluded from Lily's shutdown report. Register app-lifetime async resources
//! as DI-managed services. Controllers, middleware, and guards may retain
//! singleton DI services; scoped and transient services belong to the active
//! invocation and must not be retained by those application-lived instances.
//! This boundary adds no async disposal hook for the instances themselves.
//!
//! Callback cancellation uses read-only [`ExecutionCancellation`] and
//! [`CleanupCancellation`] views. Actions and connected hooks may extract the
//! execution view; disconnected hooks may extract only the cleanup view.
//! Middleware and guard callbacks receive the corresponding view as their last
//! parameter. The views expose no raw token or cancellation source. Cleanup
//! invocations have independent child signals under the cleanup authority.
//! Cooperative cancellation windows and retained execution owners remain later
//! shutdown implementation phases; a signal alone does not promise another poll.
//!
//! Inbound application frames have one decoding authority:
//! [`RawEnvelope::try_from_message`] applies the transport-frame bound, then
//! the selected [`WebSocketFrameCodec::decode_frame`] implementation parses
//! the envelope. [`LilyEnvelopeCodec`] is the built-in strict Lily v2 codec,
//! while [`WsRequest`] is the already-decoded controller and middleware view.
//! There is deliberately no root-level request-decoder API.
//!
//! [`WsHeaders::get_protocols`] exposes the client-offered subprotocol list;
//! [`WsRequest::negotiated_subprotocol`] exposes the single protocol selected
//! during Upgrade, which is the authority used by
//! [`WsRequestExt::supports_protocol`]. [`WsRequest::message_age`] measures the
//! current decoded request, while [`WsRequest::connection_age`] and
//! [`WsRequestExt::uptime`] measure from connection-manager admission.
//!
//! ```compile_fail
//! use lily_websocket::WsRequestDecoder;
//! ```
//!
//! # Known transport limitation: partial-frame UTF-8 fail-fast
//!
//! Lily rejects an invalid UTF-8 Text message before application dispatch and
//! maps the transport error to WebSocket Close code `1007`. The current
//! Tungstenite transport, however, assembles the complete payload of one
//! WebSocket frame before validating its text. If an invalid UTF-8 sequence
//! becomes conclusive in an earlier TCP read while that same declared frame is
//! still incomplete, Lily does not reject it at that read boundary; rejection
//! occurs only after the frame completes or another connection-termination
//! condition wins. Application code never receives the partial or invalid
//! message.
//!
//! This is the fail-fast timing exercised by Autobahn cases `6.4.3` and
//! `6.4.4`, which are currently classified as `NON-STRICT`. The configured
//! [`ServerConfig::max_frame_size`] and [`ServerConfig::max_message_size`]
//! bound accepted payload sizes, but they are not a per-frame assembly
//! deadline. Incremental UTF-8 validation would also not, by itself, stop a
//! peer from slowly sending a valid prefix. Deployments with stronger
//! slow-client threat models must therefore apply bounded connection
//! admission and transport/read-progress controls independently.
//!
//! The deferred parser work and its acceptance criteria are tracked as
//! [`LWS-TD-001`](https://github.com/akincisoftware/framework/blob/main/crates/lily_websocket/TECHNICAL_DEBT.md#lws-td-001).
//!
//! Distributed online fan-out is opt-in through
//! [`WsAppBuilder::backplane`]. Lily constructs the selected
//! [`WebSocketBackplane`] once from the same application [`Extensions`], keeps
//! connection and room membership node-local, and owns a private versioned
//! routing envelope. Custom adapters transport opaque
//! [`WebSocketBackplaneFrame`] values; they do not create application events,
//! global presence, replay, or exactly-once delivery. Construction is bounded
//! by the existing lifecycle shutdown timeout; outbound publish is bounded by
//! [`ServerConfig::write_timeout_millis`]. Lily invokes one sequential
//! subscription through [`WebSocketBackplane::receive`] and can invoke
//! [`WebSocketBackplane::publish`] concurrently. Both operations, construction,
//! and close must be cancellation-safe.
//!
//! The private backplane envelope is currently v2 and includes bounded,
//! namespace-scoped principal targeting. Nodes that share one logical
//! deployment must switch protocol generation and provider channel/ACL
//! patterns together. V1/v2 mixed operation is deliberately isolated rather
//! than partially delivering principal-targeted messages. Transport adapters,
//! including Redis, still see only opaque bytes.
//!
//! A custom receiver must apply the
//! [`WebSocketBackplaneInboundAdmission`] limit before allocating the inbound
//! frame. Publisher and subscriber health are independent; a required app does
//! not become ready before its first
//! [`WebSocketBackplaneEvent::SubscriptionReady`]. Provider-observed
//! `Publisher*` transitions can lower or restore publisher readiness without
//! waiting for application traffic. Shutdown closes dispatcher admission,
//! drains already admitted operations, owns/aborts ingress, waits for the
//! receive future to terminate, and replays the provider close operation's
//! terminal result to concurrent close callers before DI disposal.
//!
//! Lily's envelope validation is not publisher authentication. An adapter must
//! use an application- and environment-isolated channel with broker ACLs and
//! authenticated encrypted transport. Provider diagnostics cross Lily only as
//! stable redacted error kinds, and transport timeout/cancellation must not be
//! blindly retried because acceptance can be ambiguous. Backplane telemetry is
//! restricted to fixed low-cardinality publish/validation/dedupe outcome
//! categories.
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(clippy::print_stderr, clippy::print_stdout)]

mod app;
mod backplane;
mod codec;
mod connection;
mod controller;
/// Typed WebSocket message and lifecycle extractor contracts.
pub mod extractor;
mod groups;
/// Action authorization contracts for WebSocket messages.
pub mod guard;
mod lifecycle;
/// Handshake, connection-lifecycle, and message middleware contracts.
pub mod middleware;
mod outcome;
mod reporting;
mod request;
mod server;
mod shutdown;
mod tasks;

extern crate self as lily_websocket;

#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod __fuzzing;

// Re-export main types for convenience
pub use app::*;
pub use backplane::*;
pub use codec::*;
pub use connection::*;
pub use controller::{
    ClientProxy, WebSocketClients, WebSocketContext, WebSocketControllerInitError,
    WebSocketControllerMaterializationError, WebSocketControllerTrait, WebSocketLifecycleError,
    WebSocketRooms,
};
pub use extractor::*;
pub use groups::GroupError;
pub use outcome::*;
pub use request::*;
pub use server::{
    ServerConfig, ServerError, TrustedProxyNetwork, WsEffectiveConfigError, WsHandshakeContext,
    WsTransportSecurity,
};

// Re-export commonly used external types
pub use tokio_tungstenite::tungstenite::{
    Message,
    http::{HeaderName, HeaderValue, StatusCode},
};
pub use uuid::Uuid;

// Re-export for convenience
pub use async_trait::async_trait;
// Message execution has a distinct cancellation authority. This alias keeps
// existing action signatures intact while exposing the shared worker contract.
pub use lily_background_service::{
    BackgroundServiceTrait, ExecutionCancellation as BackgroundCancellation,
};
pub use lily_injection::{
    ApplicationContainer, ApplicationContainerBuilder, ApplicationScope, ApplicationScopeFactory,
    BUILD_ROLLBACK_TIMEOUT_ENV, ContainerShutdownReport, DEFAULT_SHUTDOWN_TIMEOUT, Extensions,
    Injectable, InjectionError, MAX_BUILD_ROLLBACK_TIMEOUT_SECS, ProcessContext, ServiceLifetime,
    ServiceScope, ServiceTrait, ShutdownOutcome, ShutdownOutcomeStatus, ShutdownRemainingWork,
};
pub use lily_web_core::{
    Principal, RequestConnectionInfo, RustlsConfig, TlsConfigError, TlsFileRole,
};
pub use lily_websocket_derive::{WebSocketController, websocket_controller};

/// Private expansion ABI used by `lily_websocket_derive`.
///
/// This module is public solely because procedural macro output is compiled in
/// downstream crates. It is not an application API and may change without a
/// compatibility promise.
#[doc(hidden)]
pub mod __private {
    pub use crate::controller::{
        BoundWebSocketOperation, ErasedWebSocketController,
        PENDING_WEBSOCKET_OPERATION_REGISTRATIONS, PendingWebSocketOperation,
        PendingWebSocketOperationRegistrationFn, WEBSOCKET_CONTROLLER_REGISTRATIONS,
        WebSocketActionFuture, WebSocketActionHandler, WebSocketActionRegistration,
        WebSocketAsyncApiRegistration, WebSocketAsyncApiStatus,
        WebSocketConnectionMiddlewareRegistration, WebSocketControllerBindingError,
        WebSocketControllerDefinition, WebSocketControllerRegistration,
        WebSocketControllerRegistrationFn, WebSocketFrameCodecRegistration,
        WebSocketGuardRegistration, WebSocketHandshakeMiddlewareRegistration,
        WebSocketLifecycleAction, WebSocketLifecycleFuture, WebSocketLifecycleHandler,
        WebSocketMessageAction, WebSocketMessageMiddlewareRegistration, WebSocketOperationKind,
        WebSocketOperationMetadata, WebSocketPayloadCodecRegistration,
        downcast_websocket_controller,
    };
    pub use crate::extractor::{
        WebSocketLifecycleInvocation, WebSocketMessageInvocation, extract_connected_arguments,
        extract_disconnected_arguments, extract_message_arguments,
    };
    pub use crate::outcome::{PendingWebSocketActionOutcome, into_websocket_action_outcome};
    pub use lily_injection;
    pub use lily_injection::Extensions;
    pub use linkme;
}
