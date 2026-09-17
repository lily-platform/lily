# Lily WebSocket

`lily_websocket` is Lily's production WebSocket server. It exposes one HTTP
Upgrade endpoint and routes strict Lily v2 envelopes to struct controller
methods through an immutable, application-local action table.

## Canonical application model

- `WsAppBuilder` is the composition root and `WsApp` owns the listener.
- `#[derive(WebSocketController)]` declares one exact namespace.
- `WebSocketControllerTrait::new` constructs the controller once per built app
  from that app's `Extensions`.
- `#[websocket_controller]` registers explicit message and lifecycle methods.
- `WebSocketContext` provides caller/all/others/client/room delivery and room
  membership operations.
- Lily never generates status, group, send, subscription, or CRUD relay
  actions. Applications declare every wire operation explicitly.

```rust,no_run
use std::sync::Arc;

use lily_websocket::{
    DisconnectReason, Extensions, NoReply, Payload, ServerConfig, WebSocketActionError,
    WebSocketContext,
    WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait,
    WebSocketLifecycleError, WsAppBuilder, async_trait, websocket_controller,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct SendMessage {
    text: String,
}

#[derive(WebSocketController)]
#[namespace("chat")]
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(
        _extensions: Arc<Extensions>,
    ) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl ChatController {
    #[connected]
    async fn connected(
        &self,
        _context: WebSocketContext,
    ) -> Result<(), WebSocketLifecycleError> {
        Ok(())
    }

    #[message("send")]
    async fn send(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<SendMessage>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .others()
            .send("chat:received", &input)
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[disconnected]
    async fn disconnected(
        &self,
        _context: WebSocketContext,
        _reason: DisconnectReason,
    ) -> Result<(), WebSocketLifecycleError> {
        Ok(())
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let app = WsAppBuilder::new("127.0.0.1:8080")
    .config(ServerConfig {
        endpoint_path: "/ws".into(),
        allowed_origins: vec!["https://app.example".into()],
        ..ServerConfig::default()
    })
    .build()
    .await?;
app.start().await?;
# Ok(())
# }
```

## Background services

Register workers on the same application builder:

```rust,ignore
let app = WsAppBuilder::new("127.0.0.1:8080")
    .add_background_service::<ProcessingWorker>()
    .build()
    .await?;
app.start().await?;
```

The worker contract and scoped DI APIs are available from `lily_websocket`:

```rust,no_run
use std::sync::Arc;
use lily_websocket::{
    ApplicationScopeFactory, BackgroundCancellation, BackgroundServiceTrait,
    Injectable, InjectionError, ProcessContext, ServiceTrait, async_trait,
};

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ProcessingService;
impl ServiceTrait for ProcessingService {}
impl ProcessingService {
    async fn process_next(&self, _stopping: BackgroundCancellation) -> Result<(), InjectionError> {
        // Perform one application-owned unit of work.
        Ok(())
    }
}

struct ProcessingWorker {
    scopes: Arc<ApplicationScopeFactory>,
}

#[async_trait]
impl BackgroundServiceTrait for ProcessingWorker {
    type Error = InjectionError;

    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error> {
        Ok(Self { scopes })
    }

    async fn execute_async(&mut self, stopping: BackgroundCancellation) -> Result<(), Self::Error> {
        while !stopping.is_cancelled() {
            let signal = stopping.clone();
            self.scopes.create_scope(ProcessContext::new())?
                .run(move |extensions| Box::pin(async move {
                    extensions.get_service::<ProcessingService>(None).await?
                        .process_next(signal).await
                }))
                .await?;
            tokio::select! {
                _ = stopping.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            }
        }
        Ok(())
    }
}
```

The same concrete type is registered once per application. Registering it in
both an HTTP and a WebSocket application creates two independent workers.
`new` runs during build; `execute_async` runs once after listener bind and any
required backplane subscription becomes ready. Optional backplane readiness
does not delay it. Failed startup and close-before-start never execute workers.
Readiness does not wait for their first job to complete. `Ok(())` is a normal
one-shot or disabled completion. Unhandled errors and panics stop the host;
there is no automatic retry or restart. Expected shutdown cancellation should
be handled by the worker and returned as `Ok(())`.

`BackgroundCancellation` is an alias of the shared
`lily_cancellation::ExecutionCancellation`. It is distinct from the existing
`lily_websocket::ExecutionCancellation` used by actions and middleware. A message
timeout or peer disconnect does not cancel a background worker. Only host
shutdown signals its token. An unhandled worker failure can initiate that
shutdown.

Create a fresh scope per job. `ServiceScope::run` supplies `&Extensions` inside
the correct `ProcessContext` and waits for disposal. A worker may create a
finalization scope while cooperating with shutdown. Scope admission closes
when workers terminate or forced stopping begins. Do not retain scoped services
after their scope closes or detach tasks that use them.

All workers share the application's original shutdown budget. Cancellation is
followed by bounded cooperation, abort/actual joins, scope disposal, backplane
and owned DI disposal, then telemetry flush. Dropping build/start/close waiters
does not abandon these owners. Dropping an unstarted app also cleans prepared
workers. A caller-owned container and its unrelated scopes remain caller-owned.
Background build rollback uses one absolute budget for its workers and
dependencies. A blocking destructor or non-yielding task cannot be forcibly
terminated by Tokio: unconfirmed termination produces incomplete shutdown and
retains the dependencies. Repeated close calls replay the original outcome.

During shutdown the dispatcher rejects new external sends. Worker cooperation
permits database finalization; it does not grant a WebSocket callback's private
outbound continuation authority or guarantee delivery to closing peers.

Background lifecycle events use the existing tracing runtime. Annotate a job
method with `#[lily_trace(...)]` for a trace per job; scope cleanup retains that
job's trace context. The pre-flush shutdown checkpoint includes numeric worker
and scope counters. See [shutdown qualification](SHUTDOWN_QUALIFICATION.md) for
the integration test matrix.

## Sequential dispatch and bounded inbound admission

Lily runs one application action at a time for each connection. While that
action is pending, the socket reader remains active so Ping, Pong, and Close
control frames cannot be hidden behind application work. Complete Text and
Binary application messages wait in a per-connection queue bounded by both
message count and aggregate payload bytes:

```rust
# use lily_websocket::ServerConfig;
let config = ServerConfig {
    inbound_queue_capacity: 16,
    inbound_queue_max_bytes: 1024 * 1024,
    ..ServerConfig::default()
};
```

The active action is not part of these queue limits. Waiting messages allocate
no action task and no DI scope; those are created only when sequential dispatch
begins. `max_message_size` remains the limit for one decoded WebSocket message,
whereas `inbound_queue_max_bytes` limits the sum retained behind the active
action. If either inbound queue limit is crossed, Lily sends a bounded policy
violation Close and cancels the active action. It does not stop socket reads for
backpressure, because doing so could make Ping, Pong, and Close unobservable.

Graceful shutdown clears every waiting message, lets the already-admitted action
and its scoped cleanup finish, and then sends the server-shutdown Close. Queue
limits are per connection, so deployments should size them together with
`max_connections` and their memory budget.

## Bounded outbound admission and slow consumers

Outbound Text and Binary application messages are admitted independently for
each live connection. Lily first enforces the single-message limit, then waits
for both the connection's message-count slot and aggregate byte budget:

```rust
# use lily_websocket::ServerConfig;
let config = ServerConfig {
    max_outbound_message_size: 1024 * 1024,
    outbound_queue_capacity: 256,
    outbound_queue_max_bytes: 1024 * 1024,
    outbound_admission_timeout_millis: 5_000,
    ..ServerConfig::default()
};
```

Once acquired, the byte reservation remains charged while the frame waits for
a message-count slot, while it is queued, and while its socket write/flush is
in flight. Protocol-control traffic uses its own priority path and does not
consume this application byte budget. A producer waits at most
`outbound_admission_timeout_millis` (accepted range `100..=300000`) for both
capacities. When one broadcast recipient is full, other recipients are admitted
concurrently instead of waiting behind it.

If the deadline expires, Lily stops only that live transport generation from
accepting application frames and makes a best-effort WebSocket Close `1013`
request with reason `lily.v2.slow_consumer`. A transport failure or write
timeout can prevent the peer from observing that Close. The send still reports
backpressure/partial target admission and never reports the expired admission
as successful. This deadline is separate from `write_timeout_millis`, which
bounds a write or flush after admission.

This is intentionally a live-connection flow-control boundary. Lily does not
retain an event log, assign reconnect sequences, maintain a durable delivery
ACK/checkpoint ledger, or replay missed messages after the client reconnects.
This is distinct from Lily v2's exchange-local request/response correlation: an
inbound event may supply an `ack_id` and its response may be an ACK, but that ACK
is not a reconnect delivery receipt. Financial or other loss-intolerant
applications must place durable outbox/event-log and sequence/ACK replay
semantics in their application or messaging layer. Keeping that durable history
inside every WebSocket connection would turn transport flow control into
unbounded, attacker-influenced retention.

## Transport identity and TLS authority

Forwarding headers are untrusted by default. `ServerConfig::default()` has an
empty `trusted_proxy_cidrs` list, strips `X-Forwarded-For`, `X-Real-IP`,
`Forwarded`, and reserved edge-identity headers, and uses the accepted socket
peer as the effective client IP. When Lily is deployed behind a known proxy,
configure only that proxy network and a bounded hop count:

```rust
# use lily_websocket::ServerConfig;
let config = ServerConfig {
    trusted_proxy_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
    max_forwarded_hops: 4,
    ..ServerConfig::default()
};
```

For a trusted peer, Lily parses one `X-Forwarded-For` list from right to left
and stops at the first untrusted hop. Malformed or oversized trusted input is
rejected with HTTP 400 before application identity middleware runs. Raw
forwarding and reserved identity headers never reach middleware.
`WsHandshakeRequest`, `WsHandshakeContext`, `WsRequest`, `WebSocketContext`,
and `ConnectionInfo` expose the resulting typed `RequestConnectionInfo`.
Their `is_secure()` value is listener-owned: only a Lily-terminated TLS
connection is secure; a client-controlled `Origin: https://...` cannot change
WS into WSS.

Registration contains only immutable type metadata and function pointers.
During `build`, Lily validates all definitions, creates each required
controller exactly once, constructs each concrete handshake, identity,
connection and message middleware or guard type once, then binds every method
to the same `Arc<Controller>`. Constructors run in a fresh root task and cannot
inherit the caller's ambient `ProcessContext`; init errors and panics become
secret-safe typed build errors. No live controller is stored in global state,
and exact route/action lookup performs no controller-registry lock or runtime
downcast.

## Controller metadata

Controller metadata accepts active `#[handshake_middleware(...)]`,
`#[connection_middleware(...)]`, `#[message_middleware(...)]`, `#[guard(...)]`,
`#[frame_codec(Type)]`, `#[payload_codec(Type)]`, `#[timeout(seconds = N)]`,
and `#[asyncapi(...)]` declarations. Handshake and connection middleware are
controller-level because no message action exists during Upgrade. A message
method may add message middleware, guards, a payload-codec override, timeout,
and AsyncAPI metadata. Effective plans are immutable: handshake and connection
plans use `global -> controller`, while message middleware and guards use
`global -> controller -> action`. A message action timeout overrides the controller message timeout, then
`message_timeout_secs` (default: 30 seconds). This selects one absolute deadline
for the **whole message pipeline**, starting before its first middleware.
Connection lifecycle callbacks have their own operation override or
`connection_lifecycle_timeout_secs` cap; they do not inherit the controller
message timeout.

## Wire contract

Connect to `ws://127.0.0.1:8080/ws?namespace=chat` with the `lily.v2`
subprotocol. Namespace is required, exact, and authoritative. A `chat`
connection cannot invoke another controller. Text and Binary application
frames carry the same strict UTF-8 JSON schema:

Inbound decoding has one authority. Lily first applies the configured frame
bound with `RawEnvelope::try_from_message`, then invokes the selected
`WebSocketFrameCodec::decode_frame`; `LilyEnvelopeCodec` is the built-in strict
v2 implementation. `WsRequest` is the already-decoded action/middleware view,
not a parser, and no root-level `WsRequestDecoder` API is supported. Subprotocol
negotiation remains in `ServerConfig` and the Upgrade pipeline.

The normalized `WsHeaders::get_protocols()` list records every subprotocol the
client offered. `WsRequest::negotiated_subprotocol()` records the single value
the server selected, and `WsRequestExt::supports_protocol()` performs an exact,
case-sensitive comparison only against that selected value.

Request timing has two explicit clocks. `WsRequest::created_at` and
`WsRequest::message_age()` describe the current decoded message;
`WsRequest::connection_age()` and `WsRequestExt::uptime()` measure from the
timestamp at which the connection manager admitted the transport.

```json
{
  "protocol_version": 2,
  "msg_type": "event",
  "event": "chat:send",
  "content_kind": "json",
  "content_type": "application/json",
  "encoding": "identity",
  "data": { "text": "hello" },
  "namespace": "chat",
  "timestamp": 1
}
```

Unknown namespaces fail the Upgrade. Malformed, unknown, or cross-namespace
application events receive bounded protocol errors.

### Known transport limitation: partial-frame UTF-8 fail-fast

Lily rejects invalid UTF-8 Text messages before application dispatch and maps
the transport error to WebSocket Close code `1007`. Its current Tungstenite
transport first assembles the complete payload of one WebSocket frame. If an
invalid UTF-8 sequence becomes conclusive in an earlier TCP read while the same
declared frame is still incomplete, rejection is delayed until that frame
completes or another connection-termination condition wins. No partial or
invalid application message is dispatched.

Autobahn cases `6.4.3` and `6.4.4` therefore currently classify this timing as
`NON-STRICT`. `max_frame_size` and `max_message_size` bound payload sizes, but
they are not a per-frame assembly deadline, and incremental UTF-8 validation
alone would not stop a peer that slowly sends a valid prefix. Deployments with
stronger slow-client threat models should combine Lily's bounded connection
admission with transport/read-progress controls appropriate to their edge.

The deferred implementation, security boundary, and completion criteria are
recorded in [`TECHNICAL_DEBT.md`](TECHNICAL_DEBT.md#lws-td-001).

Remote-controlled state is admitted under fixed limits. One Upgrade accepts at
most 64 raw header fields, 128-byte names, 8 KiB individual values, and 16 KiB
after merging only `Sec-WebSocket-Protocol` or
`Sec-WebSocket-Extensions`. Non-list duplicates fail closed. Message metadata
and connection metadata each accept at most 32 entries with 128-byte keys and
1 KiB values; duplicate JSON metadata keys are rejected before map collapse.
Handshake-local, connection-local, and per-message typed state each admit at
most 32 distinct concrete types. Replacing an existing key/type does not
consume another slot. Debug output exposes metadata/header keys or counts, not
their values.

## Authentication, guards, and middleware

Upgrade identity is an asynchronous, application-owned boundary. Implement
`WebSocketIdentityMiddleware`, register it once with
`WsAppBuilder::identity_middleware::<Type>()`, and return either
`WebSocketIdentity::authenticated(AuthenticatedWebSocketIdentity)` or
`WebSocketIdentity::anonymous()`. The identity may publish bounded typed
connection-local values with `insert_connection_local`; after Upgrade the same
immutable values are visible through `WebSocketContext`, message middleware,
guards, lifecycle handlers, and `ConnectionLocal<T>` extractors. Lily does not
provide a session store, JWT/OIDC verifier, login flow, or refresh flow.
Connection-local values must be owned snapshots or app-lived handles; a
scope-managed service itself must not escape the per-Upgrade DI scope.

`AuthenticatedWebSocketIdentity::try_new(principal)` derives a `PrincipalId`
from the application-verified `Principal::subject`. The identifier is exact,
case-sensitive, at most 256 UTF-8 bytes, and neither normalized nor interpreted
by Lily. Empty values, surrounding whitespace, control characters, and values
over the byte limit fail closed. `Debug` output redacts both the principal and
its identifier. This identifier is an online routing key, not proof that Lily
authenticated the user.

The identity hook runs only after endpoint, header, exact namespace, Origin,
and subprotocol validation plus every effective handshake middleware. It can
perform non-blocking Redis, database, or remote-key lookup before HTTP `101`.
It receives `WsHandshakeExchange`, can resolve concrete or trait services from
that Upgrade's DI scope through `service::<T>()`, and can read typed values
published by earlier handshake middleware. An authentication failure returns
`WsHandshakeRejection`; Lily sends that HTTP response without upgrading the
socket. Rejections accept an HTTP error status, bounded public text, and only
the allowlisted `WWW-Authenticate`, `Retry-After`, `Set-Cookie`, or
`Cache-Control` headers. Internal errors and credentials must never be placed
in the public body. Construct HTTP `401` with
`WsHandshakeRejection::unauthorized(code, challenge)`; the fallible constructor
validates and installs the mandatory `WWW-Authenticate` challenge atomically.
The generic constructor rejects `401`, plus `405`, `407`, and `426` whose
mandatory response headers are outside this bounded API.

The identity registration is application-global and optional. An
implementation serving both public and authenticated namespaces can inspect
`exchange.request().namespace()` and return anonymous identity for its public
namespaces. When no identity middleware and no effective handshake middleware
exist, Lily skips the application handshake DI scope and proceeds directly
from transport validation to Upgrade.

```rust,ignore
use std::sync::Arc;
use lily_websocket::{
    AuthenticatedWebSocketIdentity, Extensions, HeaderValue, Principal, WsAppBuilder, async_trait,
};
use lily_websocket::middleware::{
    MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind,
    WebSocketIdentity, WebSocketIdentityMiddleware, WsHandshakeExchange,
    WsHandshakeRejection, WsMiddlewareInitError,
};

struct SessionIdentity {
    sessions: Arc<SessionService>,
}

#[async_trait]
impl WebSocketIdentityMiddleware for SessionIdentity {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        let sessions = extensions
            .get_service::<SessionService>(None)
            .await
            .map_err(WsMiddlewareInitError::dependency)?;
        Ok(Self { sessions })
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("session_identity", MiddlewareKind::WebSocketHandshake)
    }

    async fn identify(
        &self,
        exchange: &mut WsHandshakeExchange,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<WebSocketIdentity, WsHandshakeRejection> {
        let authorization = exchange
            .request()
            .headers()
            .get_custom_header("authorization")
            .ok_or_else(|| {
                WsHandshakeRejection::unauthorized(
                    MiddlewareErrorCode::new("AUTH_REQUIRED").expect("static code is valid"),
                    HeaderValue::from_static("Bearer"),
                )
                .expect("static authentication challenge is valid")
            })?;

        let session = self.sessions.verify(authorization).await.map_err(|_| {
            WsHandshakeRejection::unauthorized(
                MiddlewareErrorCode::new("AUTH_INVALID").expect("static code is valid"),
                HeaderValue::from_static("Bearer"),
            )
            .expect("static authentication challenge is valid")
        })?;
        // `expiry_deadline` is an application-computed `tokio::time::Instant`.
        // The application still owns credential verification and refresh.
        let verified_identity = AuthenticatedWebSocketIdentity::try_new(session.principal)
            .map_err(|_| {
                WsHandshakeRejection::try_new(
                    500,
                    MiddlewareErrorCode::new("IDENTITY_STATE_INVALID")
                        .expect("static code is valid"),
                )
                .expect("500 is a valid rejection status")
            })?
            .expires_at(session.expiry_deadline);
        let mut identity = WebSocketIdentity::authenticated(verified_identity);
        identity
            .insert_connection_local(session.tenant)
            .map_err(|_| {
                WsHandshakeRejection::try_new(
                    500,
                    MiddlewareErrorCode::new("IDENTITY_STATE_INVALID")
                        .expect("static code is valid"),
                )
                .expect("500 is a valid rejection status")
            })?;
        Ok(identity)
    }
}

let app = WsAppBuilder::new("127.0.0.1:8080")
    .handshake_middleware::<GlobalUpgradeAdmission>()
    .identity_middleware::<SessionIdentity>()
    .connection_middleware::<ConnectionAudit>()
    .build()
    .await?;
```

### Identity snapshots, refresh, and expiry

An authenticated connection owns one immutable, revisioned
`WebSocketIdentitySnapshot`. `WebSocketContext::identity_snapshot()` captures
its current revision. After the application verifies a refreshed credential,
it may build another `AuthenticatedWebSocketIdentity` and call
`WebSocketContext::reauthenticate(observed.revision(), replacement)`. This is a
compare-and-replace operation: `Updated` publishes the new snapshot atomically,
`Stale` means another refresh already won, and `AlreadyClosing` means terminal
connection cleanup has begun. Lily updates its principal index and expiry
watcher atomically with the accepted replacement, but it never verifies the
credential or decides whether refresh is allowed.

Each inbound application message captures the current principal exactly once
before middleware, guards, extraction, and the action run. Every stage of that
message observes the same immutable principal, even if a concurrent refresh
publishes a newer revision. The next message sees the replacement. The
`Principal` stored in `WsHandshakeContext` remains the original Upgrade result
for audit; use `WebSocketContext::principal()` or the typed `Principal`
extractor for the current message snapshot.

`AuthenticatedWebSocketIdentity::expires_at(deadline)` is optional. Without a
deadline, identity remains valid until application refresh, transport close, or
server shutdown. With a deadline, Lily rejects an already-expired admission or
replacement and schedules a first-writer-wins policy close. If the application
reauthenticates before expiry, the old revision's timer becomes stale and the
new deadline takes authority. Otherwise Lily removes principal membership
before emitting close reason `lily.v2.identity_expired`; inbound admission and
outbound sends also fail closed once the deadline has elapsed.

Principal delivery is namespace-scoped and reaches every currently online
connection/device bound to that exact identifier:

```rust,ignore
use lily_websocket::PrincipalId;

let recipient = PrincipalId::try_new("user-42")?;
context
    .clients()
    .principal(recipient)
    .send("chat:notification", &notification)
    .await?;
```

The same `ClientProxy` path is local-only without a backplane and distributed
online fan-out when a backplane is configured. Lily does not infer tenant,
role, relationship, or per-recipient authorization; an application guard or
service must approve the target before calling `principal`. When no identity
middleware is registered, the manager does not allocate a principal-index map,
maintain principal membership, or schedule identity deadlines. Principal
targeting then fails with the typed identity-lifecycle-unavailable outcome
instead of creating a second identity authority.

`WsGuard` is application-defined message admission. `new(Arc<Extensions>)` runs once
per concrete guard type during app build. `can_activate(&mut
WsMessageExchange)` runs after message middleware and before payload extraction;
it can inspect the verified principal and connection-local state, resolve a
message-scoped service, publish a message-local value, or return a typed
ACK/error/close rejection. Guards do not authenticate the HTTP Upgrade.

Builder-installed middleware has three explicit boundaries:

- `handshake_middleware::<Type>()` installs a global async pre-Upgrade type;
  controller `#[handshake_middleware(Type)]` entries run after it;
- `connection_middleware::<Type>()` installs global post-Upgrade
  admission/open/close behavior; controller `#[connection_middleware(Type)]`
  entries extend the selected namespace's lifecycle plan;
- `message_middleware::<Type>()` installs a global message middleware type;
  controller/action `#[message_middleware(Type)]` metadata extends that route's
  plan.

The complete successful boundary order is:

```text
capacity/TLS/deadline
  -> endpoint + bounded duplicate-safe headers + exact namespace
  -> Origin + subprotocol validation
  -> global handshake middleware
  -> selected controller handshake middleware
  -> optional application identity middleware
  -> HTTP 101
  -> global connection admission
  -> selected controller connection admission
  -> manager publication
  -> global/controller opened hooks
  -> controller #[connected]
  -> message loop
```

Handshake hooks run in one request DI scope and share handshake-local values;
those temporary values are destroyed when the Upgrade decision finishes. Only
the `Principal` and connection-local state explicitly returned by
`WebSocketIdentity` cross the 101 boundary. Connection middleware never runs
for a rejected Upgrade. Successfully admitted connection middleware retains a
reverse `controller -> global` close obligation on every terminal path; user
cleanup runs within its budget after termination prerequisites are confirmed.

Manager publication transfers framework cleanup ownership. User
`#[disconnected]` is armed only when an actual `#[connected]` returns `Ok(())`,
before awaiting connect-scope disposal. An `opened` or connected failure does
not arm it; a disconnected-only controller is not implicitly armed. Before the
callback starts, Lily confirms startup/session and transport destruction,
message-owner joins and DI scope termination. The current caller is terminal;
other healthy connections remain usable targets. Missing prerequisite evidence
causes dependent user cleanup to be skipped and reported as incomplete. See the
[Phase 6 contract and flows](SHUTDOWN_ARCHITECTURE.md#phase-6-connected-eligibility-and-connection-termination-barriers).

Message middleware implements `new(Arc<Extensions>)` plus `before_message` and
normal `after_message`, with optional `on_message_termination` for interrupted
execution/unwind. Lily records only the prefix whose `before_message` returned
`Continue`; a rejecting, failing, panicking, or timed-out hook is not itself
entered. The executor claims that prefix once and attempts reverse hooks in
`action -> controller -> global` order. Normal reverse hooks run within the
execution slot and its cooperative boundary. If interrupted, the retained owner
uses separate termination hooks for the interrupted and outstanding exits;
completed normal exits are not invoked again. The termination tail has one
aggregate `message_cleanup_timeout_secs` cap clipped by the root cleanup deadline,
with a separate child signal and a share of remaining time per hook. Expired
hooks are reported incomplete; even a first poll is not guaranteed. A dropped
connection waiter does not discard the owner or its DI receipt. A rejection prevents remaining
middleware, every guard, payload extraction and the action from running. An
exact route is selected before a message DI scope or user hook is created.

Graceful shutdown closes message admission without cancelling the action that
is already running on a connection. That action may finish reverse middleware,
message-scope cleanup and its terminal frame; Lily writes those admitted frames
before sending the canonical `1001 Going Away` (`lily.v2.server_shutdown`). It
then keeps reading protocol traffic without dispatching later Text/Binary frames
and waits for the peer Close response. Terminal-frame drain, Close write and
peer acknowledgement share `write_timeout_millis`, while the composition-root
shutdown deadline and force path remain the hard authority. The `ExecutionCancellation`
extractor therefore signals transport/force/deadline termination for an active
message, not the ordinary admission barrier.

Callback cancellation is read-only. Handshake/identity, connection admission/open,
message before/after, and guard callbacks accept `ExecutionCancellation` as their
last parameter. Message actions and `#[connected]` may extract that same type.
Connection `closed` accepts `CleanupCancellation`; `#[disconnected]` may extract
it. `on_message_termination(WsMessageTerminationContext<'_>, CleanupCancellation)`
also receives cleanup authority, and returns `Result<(), WsMiddlewareError>`;
it cannot rewrite the response. Cleanup signals use a separate framework authority and one child per
invocation, so a local cleanup timeout does not cancel sibling hooks. Message
execution uses one child source per message, shared by its complete pipeline;
local message expiry cannot cancel the connection or other messages. Exchange/context
accessors also return read-only views. Neither type exposes Tokio's token,
`cancel`, `child_token`, `Deref`, or `into_inner`.

The old `Cancellation` name is a deprecated alias for `ExecutionCancellation`
and cannot be extracted in `#[disconnected]`. See the
[Phase 2 migration and current runtime limits](SHUTDOWN_ARCHITECTURE.md#phase-2-migration).
Phase 4 lets accepted execution observe cancellation and return during a bounded
cooperative window inside the root deadline. Only a remaining execution slot is
then stopped; its owner retains termination cleanup and scope receipts. Cleanup and
join waits use the same absolute root limits and report unfinished work. The
separate message termination hook is implemented in Phase 5. See the
[Phase 4 budget policy and current limits](SHUTDOWN_ARCHITECTURE.md#phase-4-absolute-deadlines-and-cooperative-execution-cancellation).
Move abnormal cleanup formerly placed only in `after_message` into
`on_message_termination`; use `context.normal_exit()` to handle partial normal
work. See the [Phase 5 contract and migration](SHUTDOWN_ARCHITECTURE.md#phase-5-separate-normal-unwind-and-message-termination).

Phase 3 retains the message ledger and DI scope outside the abortable execution
slot. Connection cleanup waits for its message-owner joins and scope disposal
receipts, and receipt drivers are tracked. Scope termination and successful
cleanup remain distinct observations. See the [Phase 3 ownership model and
remaining boundaries](SHUTDOWN_ARCHITECTURE.md#phase-3-retained-lifecycle-owners-and-execution-slots).

During drain, a send awaited inside an admitted connection/message callback can
still attempt outbound queue/backplane admission. Message execution cancellation
leaves that authority usable until the shared cooperative cutoff (at most
250 ms, clipped by root limits). A pipeline that returns within this window
retains its actual result for both local timeout and forced shutdown. Its one
terminal reply is attempted after middleware/DI cleanup and the actual owner
join, using independent bounded framework authority. Termination,
disconnected and closed callbacks use separate bounded cleanup authority to
target other healthy connections. Retaining a context/proxy or spawning a raw
task does not grant later drain admission. A successful send reports acceptance,
not client delivery. Interrupted local admission returns the new
`ConnectionError::DispatchInterrupted`; partial local effects are possible and
no complete local report is claimed. See the [Phase 7 contract and migration](SHUTDOWN_ARCHITECTURE.md#phase-7-bounded-outbound-continuation-during-drain).

```rust,ignore
#[async_trait]
impl WsMessageMiddleware for TenantContextMiddleware {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        let policy = extensions
            .get_service::<TenantPolicy>(None)
            .await
            .map_err(WsMiddlewareInitError::dependency)?;
        Ok(Self { policy })
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("tenant_context", MiddlewareKind::Custom)
    }

    async fn before_message(
        &self,
        exchange: &mut WsMessageExchange,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        let tenant = self.policy.resolve(exchange.principal()).await?;
        exchange
            .insert_message_local(tenant)
            .map_err(|_| WsMiddlewareError::internal(
                MiddlewareErrorCode::new("WS_MESSAGE_LOCAL_LIMIT")
                    .expect("static middleware error code is valid"),
            ))?;
        Ok(WsMessageDecision::Continue)
    }
}

let app = WsAppBuilder::new("127.0.0.1:8080")
    .message_middleware::<TenantContextMiddleware>()
    .guard::<GlobalMessageGuard>()
    .build()
    .await?;
```

The app-owned instance created by `new` may retain singleton DI services as
`Arc<T>`. Scoped and transient services must be resolved inside the active
invocation, for example through `exchange.service::<T>()`, and must not be
retained by a controller, middleware, or guard instance. Build-time enforcement
of incorrect retention is deferred.

Shutdown completion also waits for actual framework task joins, including cleanup
receipt drivers. Backplane, owned DI and telemetry shutdown require their users to
be terminal; an unconfirmed task produces an incomplete result and keeps dependent
cleanup from starting. Dropping a `start`/`close` waiter preserves the supervised
shutdown. See the [Phase 8 ownership and deadline contract](SHUTDOWN_ARCHITECTURE.md#phase-8-final-task-reconciliation-and-dependency-barriers).

Lily's shutdown responsibility covers resources owned by its DI container,
lifecycle scopes, and framework task registries. Resources directly constructed
by a controller, middleware, or guard remain application-owned; app-lifetime
async resources should be registered as DI-managed services. Raw `tokio::spawn`
inside a constructor, hook, or action is also application-owned and excluded from
Lily's shutdown report. There is no additional async disposal hook for these
app-owned instances. The [shutdown architecture and phase status](SHUTDOWN_ARCHITECTURE.md)
describes all nine completed phases. Use the [shutdown migration guide](SHUTDOWN_MIGRATION.md)
for callback/API changes and the [qualification matrix](SHUTDOWN_QUALIFICATION.md)
for the tested guarantees. Internal aggregate diagnostics distinguish cleanup
failure, unstarted hooks and unconfirmed task joins; a repeated close does not
rewrite an incomplete shutdown attempt as success.

Before any constructor runs, Lily validates effective `TypeId` uniqueness and
the 64-middleware limit. Constructed/cached middleware descriptor and policy
validation then runs while compiling the immutable plan, still before listener
publication. `message_timeout_secs` and `#[timeout(seconds = N)]` are restricted
to 1..=300 seconds. Before middleware, all guards, argument extraction, the
action, response preparation and normal `after_message` share one absolute
message deadline. Neither a stage boundary nor reverse exit refreshes it.
`WsMessageExchange::deadline()` and the `MessageDeadline` extractor expose that
same instant. Buffered messages waiting behind another execution have no running
message deadline; each selected dispatch receives its own.

`connection_middleware_timeout_secs` caps handshake/identity and connection
middleware invocations. `connection_lifecycle_timeout_secs` supplies the default
connected/disconnected cap. `message_cleanup_timeout_secs` caps the aggregate
termination tail independently of normal execution. These caps also accept
1..=300 seconds; cleanup remains clipped by the current root shutdown deadline.
Transport/write and heartbeat limits are separate resource policies.

The [message timeout contract and session status](MESSAGE_TIMEOUT.md) describes
the implemented whole-pipeline deadline, cooperative result preservation and
healthy connection continuation after local timeout. Shutdown diagnostics
separate the first cancellation cause, actual pipeline result, middleware/DI
termination and terminal-output disposition. A queued frame is not a socket-write
or delivery receipt; suppressed/interrupted output stays visible even after its
message owner joins. See the [qualification matrix](SHUTDOWN_QUALIFICATION.md#message-deadline-and-cooperative-result-qualification).

Reverse decisions are deterministic: `Continue` preserves the current outcome;
`Reject` changes only a successful outcome; and among successful reverse-hook
decisions the first selected `Close` is authoritative. Because reverse hooks run
from the innermost entered middleware to the outermost, a later outer hook
observes but cannot replace an existing close reason or application close frame.
An `after_message` error changes only a successful outcome and cannot overwrite
an existing rejection/close. Observing execution cancellation does not replace
a completed pipeline result. An incomplete local-timeout execution instead
produces `MESSAGE_TIMEOUT`; the healthy connection may continue only after
required termination cleanup, DI close and the owner join are confirmed. Guard error/ACK/close decisions use the
selected payload and frame codecs. ACK never invents an ID: without an inbound
`ack_id`, Lily emits the bounded missing-authority error instead. A custom frame
decoder error or panic cannot escape the boundary and becomes an
`InvalidEnvelope` protocol close.
Pre-route rejection frames use the controller/app payload codec plus selected
frame codec; after exact action selection, rejection/output uses the effective
action payload codec plus selected frame codec. Payload extraction and output
codec failures become their stage-appropriate bounded typed outcome.

Custom codecs are trusted, cooperative application extensions. Their async
constructor has cancellation containment—cancelling application build aborts
the constructor task—but no framework-owned construction deadline. Constructors
must not block a runtime worker and must bound their own dependency I/O. The
synchronous frame and payload encode/decode methods run on the message task;
panic/error containment does not make a blocking implementation preemptible, so
those methods must also remain non-blocking and bounded.

## Rooms, delivery, and operations

`ConnectionManager::broadcast`, `WebSocketDispatcher::dispatch`, and controller
client proxies share one outbound validation contract. Invalid namespace/room
tokens and oversized encoded messages fail before local queue admission or
provider publication. Explicit connection lists, room lists and exclusion lists
each accept at most 256 raw entries, checked before sorting and deduplication.
Repeated entries therefore count toward the input bound; accepted duplicates
still produce at most one delivery per connection. This bounds input lists,
not the number of members selected by a namespace, room or principal.

The manager remains node-local. The dispatcher also checks its backplane
envelope limit and handles provider availability/publication; these transport
outcomes are separate from shared input validation. A valid but absent target
keeps its empty/missing accounting. Operational callers that previously passed
invalid names or more than 256 list entries directly to the manager now receive
the same typed errors as controller proxies. The existing
`InvalidBackplaneTarget`, `BackplaneTargetLimitExceeded`, and
`BackplaneExclusionLimitExceeded` variant names remain for API compatibility and
also apply to local broadcasts.

Rooms are public membership groups, not an ACL. Application guards or services
must authorize membership before calling `WebSocketContext::rooms()`.
Empty explicit selections (`clients(vec![])`, `rooms(vec![])`, or explicit IDs
fully excluded by `except`) are successful no-ops after message, target, and
cardinality validation. The local receipt has all counters zero and the
backplane receipt is `NoTargets`, even if a provider is configured but unavailable.
No frame is published. A namespace, room, or principal with no local members is
still published when a backplane is active because it may have remote members.
Inbound backplane frames with empty explicit targets remain invalid; these
no-ops are resolved at the sending node and never enter the private protocol.

`context.clients().caller()`, `.client(id)`, and `.clients(ids)` retain the
controller namespace through `BroadcastTarget::NamespaceConnections`, including
when a backplane forwards the selection. A UUID registered in another namespace
is a missing target in this scope and receives no frame. Duplicate targets and
exclusions retain the same per-target accounting. All lower-level targets also
require an explicit namespace. `BroadcastTarget::All` and the unscoped
`BroadcastTarget::Connections` no longer exist; use `Namespace(namespace)` or
`NamespaceConnections { namespace, connection_ids }`. Application broadcasts
never select across namespaces. The event route is payload, not routing authority.

Direct manager `send_to_connection`, `send_binary_to_connection`, `join_room`,
`leave_room`, and `set_connection_metadata` now take `namespace: &str` before
the connection ID. A foreign UUID returns `ConnectionNotFound` without sending,
changing memberships or metadata, or claiming another namespace's expired
identity. Controller room operations, including `join_connection` and
`leave_connection`, retain the controller namespace. Queue admission rechecks
the namespace after waiting for capacity. Read-only application registry
snapshots and operational lifecycle ownership retain their existing scope.

For example, an orders service sends with
`manager.send_to_connection("orders", id, body).await` and joins with
`manager.join_room("orders", id, "priority").await`. A billing UUID remains
outside both operations even when the caller knows that UUID.

`context.clients().count().await` counts published connections in the current
controller namespace on this node, including the caller while registered. It
uses the same registry snapshot boundary as the app-wide
`WsApp::active_connection_count()` and `ConnectionManager::connection_count()`;
closing connections remain counted until registry removal. For example, two
`orders` connections and one `billing` connection produce facade counts of two
and one, and an app total of three. Backplane nodes are not included in any of
these counts. The facade previously returned the app total; callers that need
that scope should use the application-level API.

`get_namespace_connections` and `get_room_connections` return unique node-local
ID snapshots with unspecified order. Compare them as sets; sort the returned
IDs when deterministic presentation is needed. UUID order does not imply
connection age or delivery order, and successive queries can observe membership
changes.

For room existence, `ConnectionManager::get_room_snapshot(namespace, room)`
returns `Option<WebSocketRoomSnapshot>`. `None` means that exact room entry is
absent; `Some` contains its namespace, name and unique unordered connection IDs,
including an empty list if an empty entry were retained. Existence and membership
come from the same index read. `context.rooms().snapshot(room)` provides this
query within the controller namespace. These exact-key, node-local queries do
not validate broadcast targets or include remote backplane rooms. The existing
`get_room_connections` keeps returning an empty list for an absent room.
The last leave or lifecycle removal deletes the room entry; a concurrent join
can recreate it after a snapshot is taken.

Connection removal is owned by Lily's lifecycle. Applications request closure
with `WebSocketContext::close`; the runtime reconciles the session, controller
and middleware cleanup before removing registry/index entries. The low-level
`ConnectionManager::remove_connection` is crate-private. Code that previously
used it to disconnect a live caller must migrate to the close-request API.

`ClientProxy::send` requires full admission to every selected target; an
outbound admission deadline or closed connection reports partial admission
rather than fabricating success. With a configured backplane, a remote-only
connection is considered accepted only when the transport accepts Lily's
opaque frame; this is not a remote client delivery acknowledgement. Use
`send_with_receipt` when the application must inspect node-local terminal
accounting separately from backplane acceptance.

Distributed online fan-out is opt-in and type-based:

```rust,ignore
let app = WsAppBuilder::new("127.0.0.1:8080")
    .backplane::<ApplicationBackplane>(BackplaneRequirement::Required)
    .build()
    .await?;
```

`WebSocketBackplane::new` receives the same app-owned `Arc<Extensions>` exactly
once. The implementation chooses its own DI services and configuration
authority; the builder accepts neither a ready instance nor a second config
object. Construction is bounded by the existing application lifecycle shutdown
timeout, and a successful return means the outbound publisher is usable.
Custom transports publish `WebSocketBackplaneFrame::as_bytes()` and return
inbound bytes through `WebSocketBackplaneEvent::Frame` without parsing Lily's
private protocol. Lily calls `receive` sequentially from one owned ingress task
and may call `publish` concurrently. Every `receive` gets a
`WebSocketBackplaneInboundAdmission`; the provider must enforce its maximum
frame size before copying broker payload bytes into a `Vec<u8>`. Broker
prefetch, subscription backlog, and any adapter-owned inbound channel must also
be bounded; the provider must not turn broker overload into an unbounded memory
queue. The first frame is accepted only after `SubscriptionReady`. Recoverable
outages stay inside the adapter and emit subscription state events. An adapter
that can observe outbound connectivity independently also multiplexes
`PublisherReady`, `PublisherReconnecting`, and `PublisherUnavailable` through
this same runtime event stream; returning a receive error is terminal and Lily
does not restart that receive loop.

Provider construction, publish, receive, and close futures must be
cancellation-safe and must not detach work that can outlive the app. Publish is
bounded by `ServerConfig::write_timeout_millis`. A timeout or cancellation can
leave transport acceptance ambiguous, so neither Lily nor the adapter may
blindly retry and claim exactly-once delivery. Provider diagnostics cross the
framework boundary only as stable `WebSocketBackplaneErrorKind` and
`WebSocketBackplaneInitErrorKind` values; raw provider errors, credentials, and
payloads are not retained by framework `Debug`, `Display`, or tracing.

Publisher and subscriber health have separate authority:
`backplane.publisher` reports outbound acceptance and
`backplane.subscriber` follows `SubscriptionReady`,
`SubscriptionReconnecting`, and `SubscriptionUnavailable`. A `Required`
backplane rejects build on initialization failure and the listener does not
publish ready until the initial `SubscriptionReady` event arrives within the
existing lifecycle timeout. Subscriber loss lowers readiness from its state
event. Publisher loss lowers readiness immediately when an adapter reports a
`PublisherReconnecting`/`PublisherUnavailable` event; otherwise it remains
demand-detected at the next publish attempt. `Optional` permits local-only
startup and uses non-critical degraded health, but distributed sends still
report that the configured transport is unavailable.

Connection, namespace, room, and principal membership remain node-local. The
private v4 envelope carries target, exclusions, message ID, origin node,
payload, and bounded W3C correlation data. Its principal target contains only
the canonical namespace and bounded `PrincipalId`; Redis and custom providers
continue to transport the frame as opaque bytes. Lily validates
size/cardinality, suppresses origin loopback and bounded duplicates, then
dispatches only to the receiving node's local manager. It does not claim global
presence/counts, synchronous remote ACK, offline replay, or exactly-once
delivery. Direct `WsApp::connection_manager().broadcast(...)` remains intentionally local;
application code outside a controller uses `WsApp::dispatcher()` when it needs
the configured backplane semantics.

The private envelope is a routing schema, not authentication or authorization.
A custom adapter must publish and subscribe on an application- and
environment-isolated channel protected by broker ACLs plus authenticated,
encrypted transport such as TLS. Do not use a shared wildcard topic or accept
frames from an untrusted publisher merely because their JSON validates. All
participating nodes must run a compatible Lily backplane protocol version.
Mandatory namespace authority for every target uses private protocol v4. Earlier
backplane generations are rejected; all nodes and ACL channel patterns must
switch in one coordinated cutover. Lily's Redis adapter enforces this isolation
with the `lily.websocket.v4.<environment>.<application>.<channel>` prefix.
Private v1, v2 and v3 frames, the legacy `all`/unscoped `connections` targets,
missing namespaces, and unknown routing fields are rejected without delivery
or publication. There is no cross-generation fallback. During migration, stop
and drain the old generation, update every node and the broker's exact channel
ACL to v4, then start and verify subscriptions before enabling traffic. Custom
adapters must provide the equivalent versioned topic isolation. Keep traffic
paused during the cutover; Pub/Sub does not replay messages missed by a node.
The client-facing Lily v2 envelope and `lily.v2` subprotocol are unchanged.

`WsApp::health_snapshot` and `WsApp::metrics_snapshot` expose bounded local
runtime snapshots. A built app is live but not ready until its listener binds.
The snapshot then combines the listener, materialized controller registry,
message dispatcher, DI lifecycle, and every application dependency registered
before `start` through `WsApp::required_dependency_health`. Registering a new
required dependency after `start` or `close` fails; updating an already
registered dependency remains available to its owner.

`WsApp::close` is concurrent and idempotent even when the app was built but
never started. It disposes only a DI container created by that app. Dropping a
pending `start` waiter requests the same supervised cleanup rather than
detaching listener resources. When the application supplies an identity
deadline, Lily owns its connection watcher and requests the bounded
`lily.v2.identity_expired` policy close when the current revision expires.
Application code may still request an earlier close through
`WebSocketContext::close(CloseConnection)`; the bounded control slot is
first-writer-wins, so a racing expiry, timeout, or server shutdown cannot
publish a second Close frame.

The cloneable `WebSocketDispatcher` stops accepting new external dispatches as
soon as shutdown begins. Framework-owned callbacks have invocation-scoped
continuation authority within their deadlines. Each started send remains
counted; execution force interrupts local admission and provider publish while
cleanup sends use independent authority. Dependency close seals all new
dispatch, including cleanup continuations, before waiting for existing sends.
The ingress task has an abort-on-drop owner plus dispatcher-owned completion
tracking, so an aborted or panicking parent cannot leave a detached subscriber
using app dependencies. Provider close waits until the receive future has
actually dropped, then runs before DI disposal. Concurrent or later close
callers observe the same terminal success or typed failure instead of receiving
a fabricated success after an interrupted close.

WebSocket propagation happens immediately after the bounded HTTP header read,
before protocol/policy evaluation, identity middleware or scoped resolution.
`websocket.connection` adopts the incoming W3C parent once, before creating
`websocket.handshake` or message children. Missing, invalid or ambiguous
`traceparent` starts a fresh root; it cannot inherit an ambient transport trace.
The same connection identity follows disconnect callbacks and scoped disposal.
`tracing_external()` requires the external owner to install its propagator, as
for HTTP; the canonical Lily tracing runtime installs W3C propagation itself.

Telemetry timing boundaries are explicit:

| Span | Boundary |
| --- | --- |
| `websocket.transport` | Accepted socket through connection termination; local context |
| `websocket.upgrade.read` | TLS negotiation and bounded HTTP header read; local context |
| `websocket.connection` | Parsed Upgrade through connection cleanup; incoming remote parent |
| `websocket.handshake` | Parsed protocol/policy/identity evaluation and Upgrade write; child of connection |
| `websocket.connection.cleanup` | Owned disconnect and middleware cleanup; child of connection |
| `di.scope.dispose` | Managed disposal; child of the scope creation owner |

The handshake metric still measures from before TLS under the original single
deadline. Span-duration dashboards that previously included TLS/header waiting
in `websocket.handshake` should use `websocket.upgrade.read` for that phase.
Header size limits, early-data rejection, policy order and lifecycle ownership
are unchanged. Cleanup retains parent IDs and dispatcher rather than an open
parent span; JSONL's registry-name stack is not a substitute for OTel parent IDs.

`telemetry_parent` exercises twelve concurrent connections, sampled/unsampled,
missing/invalid/duplicate context, identity/origin/protocol rejection and abrupt
TCP drop using real SDK spans. `telemetry_collector` reuses the network fixture
against a pinned real Collector and checks raw OTLP parents plus JSONL identity:

```bash
cargo test -p lily_websocket --test telemetry_parent
cargo test -p lily_websocket --test telemetry_collector -- --ignored --nocapture
```

The second command requires Docker and the preinstalled image specified in
`lily_trace/tests/support/collector.rs`. It owns and removes its container.
`build_cancellation_deadline` additionally checks actual exporter/provider joins
after a cancelled eager initializer and a disposer that reaches the rollback
deadline. `TracingRuntimeStatus::Shutdown` alone is not completion evidence.
Its normal test uses a paused clock and JSONL; the opt-in Collector test uses a
real clock and verifies exact terminal span/log identities and metric delivery:

```bash
cargo test -p lily_websocket --test build_cancellation_deadline -- --include-ignored --nocapture
```

Backplane ingress qualification also inspects the SDK's raw attribute lists;
duplicate keys and incorrect value types cannot be hidden by a capture map.

The pinned tracing-opentelemetry 0.31 dependency drops `tracestate` in sampled-out
child contexts; IDs and the unsampled flag survive. Tests distinguish this
dependency behavior from the incoming header extraction, which preserves state.

The inbound `websocket.message` span records `lily.message_size` as an OTLP
integer containing the received message's byte length (UTF-8 bytes for text,
not its character count). Listener capacity fields are also numeric. Values
above `i64::MAX` saturate at that limit without wrapping negative.

OpenTelemetry metrics use the provider selected by `lily_trace`. CAP-09 defines
one low-cardinality backplane outcome metric with fixed direction/outcome
labels. `WebSocketServerMetricSnapshot` exposes
`backplane_publish_accepted`, `backplane_publish_saturated`,
`backplane_publish_unavailable`, `backplane_publish_failed`,
`backplane_invalid_frames`, `backplane_local_dispatch_failed`,
`backplane_duplicates_suppressed`, and
`backplane_origin_loops_suppressed`. No provider, target, message ID, payload,
or raw error becomes a metric label. Valid inbound frames that fail node-local
broadcast use the `inbound` / `local_dispatch_failed` outcome, independently of
protocol validation failures. Their consumer span and warning expose only a
bounded `lily.error_category`: `identity_lifecycle_unavailable` for a missing
identity lifecycle, or `other` for another typed local dispatch error. Partial
per-recipient results remain delivery counters, including `channel_closed` and
`backpressured`. These diagnostics do not change dedupe insertion or retry policy.
The ready-to-use Redis implementation is a
separate adapter checkpoint. For outbound reconnecting clients use
`lily_websocket_client`.
