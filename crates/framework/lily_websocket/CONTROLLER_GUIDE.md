# Struct WebSocket controller guide

For shutdown callback changes, read the [migration guide](SHUTDOWN_MIGRATION.md).
The [final shutdown flow and reporting contract](SHUTDOWN_ARCHITECTURE.md#phase-9-aggregate-evidence-and-release-qualification)
and [qualification matrix](SHUTDOWN_QUALIFICATION.md) define the tested lifecycle boundaries.

Every Lily server operation belongs to a struct controller. The controller is
the application-owned place for long-lived injected services and is created
once for each `WsApp` that contains its registrations.

```rust,ignore
#[derive(WebSocketController)]
#[namespace("orders")]
#[handshake_middleware(OrderUpgradeAdmission)]
#[connection_middleware(OrderConnectionAudit)]
#[message_middleware(TenantContextMiddleware)]
#[guard(AuthenticatedGuard)]
#[timeout(seconds = 10)]
struct OrderController {
    orders: Arc<OrderService>,
}

#[async_trait]
impl WebSocketControllerTrait for OrderController {
    async fn new(
        extensions: Arc<Extensions>,
    ) -> Result<Self, WebSocketControllerInitError> {
        let orders = extensions
            .get_service::<OrderService>(None)
            .await
            .map_err(WebSocketControllerInitError::dependency)?;
        Ok(Self { orders })
    }
}

#[websocket_controller]
impl OrderController {
    #[message("subscribe")]
    #[message_middleware(OrderAuditMiddleware)]
    #[guard(OrderReadGuard)]
    async fn subscribe(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<SubscribeOrder>,
        audit: Service<dyn RequestAudit>,
    ) -> Result<Ack<SubscriptionReceipt>, WebSocketActionError> {
        // Authorize application-specific room membership, then use
        // context.rooms() or context.clients(). The scoped audit service is
        // resolved for this message rather than retained by the controller.
        let _ = (context, input, audit);
        unimplemented!()
    }
}
```

An app may additionally register
`WsAppBuilder::handshake_middleware::<GlobalUpgradeAdmission>()`,
`WsAppBuilder::connection_middleware::<GlobalConnectionAudit>()`,
`WsAppBuilder::message_middleware::<GlobalMiddleware>()` and
`WsAppBuilder::guard::<GlobalGuard>()`. For the action above, Lily freezes and
executes separate lifecycle plans:

```text
pre-101: transport/origin -> global handshake -> controller handshake
identity: optional app identity middleware -> Principal + connection locals
post-101: global connection -> controller connection -> #[connected]
before: global middleware -> controller middleware -> action middleware
guards: global guard      -> controller guard      -> action guard
action: typed extraction  -> controller method
after:  action middleware -> controller middleware -> global middleware
```

`WebSocketHandshakeMiddleware::handle` runs in one per-Upgrade DI scope after
Origin validation. It may resolve scoped services through
`WsHandshakeExchange::service`, publish handshake-local values for later
handshake/identity stages, or return a bounded `WsHandshakeRejection` before
the socket is upgraded. An optional application-global
`WebSocketIdentityMiddleware` is registered with
`WsAppBuilder::identity_middleware::<Type>()`; it returns the common
`Principal` and immutable `ConnectionLocal<T>` seed. Lily supplies no session,
JWT, OIDC, login, or refresh implementation. A mixed public/private app can
branch on the exact namespace and return `WebSocketIdentity::anonymous()` for
public controllers. Connection-local state must be an owned snapshot or an
app-lived handle; do not retain a service whose per-Upgrade DI scope is about
to close.

`WsConnectionMiddleware` begins only after HTTP `101`. The effective
`global -> controller` admission/open prefix is unwound in reverse on every
terminal path; rejected handshakes never enter it. Handshake, identity, and
connection middleware constructors run once per app, like message middleware,
and do not retain the per-Upgrade DI scope.

Middleware and guard types are metadata, not ready instances. Their async
`new(Arc<Extensions>)` constructor runs once per concrete type during app
build in a fresh root task that does not inherit the caller's ambient DI scope.
Controller, middleware, and guard instances may retain singleton DI services
as `Arc<T>`. Resolve scoped and transient services through the active invocation;
do not retain them on an application-lived instance. Build-time enforcement of
incorrect retention is deferred. Directly constructed resources and raw
`tokio::spawn` tasks remain application-owned and are not included in Lily's
shutdown accounting. Register app-lifetime async resources as DI-managed
services; Lily adds no application-instance async disposal hook. See the
[shutdown ownership contract and implementation status](SHUTDOWN_ARCHITECTURE.md).

Message-scoped services are resolved from `WsMessageExchange::service`
while the message scope is active. Middleware and guards can share an owned
typed value with later stages using `insert_message_local`; the action reads it
with `MessageLocal<T>`. Duplicate types in one effective plan, constructor
failures, invalid middleware descriptors and plans above 64 entries abort app
build. Count and `TypeId` duplicate validation occurs before constructors;
descriptor and policy validation follows instance construction during immutable
plan compilation, still before listener publication.

Guard rejection is typed. It may emit one safe error envelope, an ACK tied to
the inbound `ack_id`, or a bounded close decision. Once a guard rejects, no
later guard, payload extractor or action executes. Authentication of the HTTP
Upgrade remains a separate handshake concern. Lily never invents an ACK ID: a
missing inbound `ack_id` becomes a bounded missing-authority error. Error/ACK
frames use the effective action payload codec and controller/app frame codec;
codec error or panic fails closed with one bounded internal close.

Each forward middleware hook receives the configured middleware timeout, the
whole guard plan shares one guard timeout, and action timeout resolves as
action -> controller -> app. All are restricted to 1..=300 seconds. Entered
middleware normal reverse hooks share one aggregate middleware deadline and
run inside accepted execution. Interruption transfers outstanding exits to
`on_message_termination`, using a separate bounded tail, independent cleanup
authority and a share of the remaining time per invocation. Both paths retain
reverse order. A dropped connection waiter cannot discard that retained owner;
expired cleanup obligations are reported incomplete.
During reverse unwind, `Continue` preserves the outcome, `Reject` changes only
success, and the first `Close` among successful reverse-hook decisions is
authoritative. Since unwind runs inner-to-outer, an outer hook cannot replace
the selected close reason or application close frame. An `after_message` error
changes only a successful outcome and does not erase an existing rejection or
close, except that a cancelled normal callback result elevates the result to
a server-shutdown close.

On graceful server shutdown, Lily stops admitting another message but allows
the one already selected for dispatch to complete. Its reverse middleware,
message scope and terminal frame finish before the server sends `1001 Going
Away`. Complete application messages waiting behind that action are bounded by
both `ServerConfig::inbound_queue_capacity` and
`ServerConfig::inbound_queue_max_bytes`; they own no task or message DI scope.
Crossing either limit fails closed with a policy-violation Close while the
reader remains available to protocol controls. Shutdown discards the entire
waiting queue, then ignores further application frames while waiting for the
peer Close acknowledgement. The admitted terminal drain, Close write and
acknowledgement share `write_timeout_millis` and are also bounded by the total
application shutdown deadline. `ExecutionCancellation` is reserved for connection
termination, force, or deadline cancellation of active work; the ordinary
admission stop does not cancel that active action.

Use `ExecutionCancellation` as an action or `#[connected]` argument and
`CleanupCancellation` as a `#[disconnected]` argument. The macro/extractor
contract rejects the opposite phase. The old `Cancellation` name is deprecated
and aliases execution cancellation only. Both types permit observing and waiting
for cancellation; their source, raw token, and child authority are private.

`#[disconnected]` becomes eligible only after an actual `#[connected]` returns
`Ok(())`. A missing connected method, `opened` failure, or unsuccessful connected
callback does not arm it. If your controller currently defines only
`#[disconnected]`, add a successful `#[connected]` (a no-op is sufficient) when
that cleanup is required. Eligibility is retained before connect-scope disposal.
Lily waits for session/transport destruction, message-owner joins and DI scope
termination before invoking it. Sends to the closing caller fail; another
healthy connection can still be targeted, without a delivery guarantee during
shutdown. Unconfirmed prerequisites skip the callback and produce incomplete
cleanup diagnostics. See the [Phase 6 flow and limits](SHUTDOWN_ARCHITECTURE.md#phase-6-connected-eligibility-and-connection-termination-barriers).

Await outbound sends inside the callback. During drain, Lily permits attempts
from admitted connection/message execution and bounded termination callbacks.
Execution cancellation can make a send return an error while the action keeps
its cooperative cancellation window; cleanup callbacks use their own signal
and deadline. A stored context/proxy or raw spawned task cannot obtain that
invocation's later drain authority. Each target can close or reject admission,
and success means queue/backplane acceptance rather than client delivery.
`ConnectionError::DispatchInterrupted` denotes interrupted local admission with
possible partial effects and no complete local report; exhaustive error matches
must handle this new variant. See the [Phase 7 flow and result contract](SHUTDOWN_ARCHITECTURE.md#phase-7-bounded-outbound-continuation-during-drain).

Cancellation of accepted execution first opens a bounded cooperative window
inside the shutdown deadline; the signal itself does not immediately drop the
handler. Local handler timeouts still apply. If execution remains pending at the
cutoff, Lily stops its execution slot and retains the lifecycle owner for bounded
cleanup and DI termination observation. A cleanup timeout or unconfirmed join is
reported as incomplete work. See the [Phase 4 policy and remaining boundaries](SHUTDOWN_ARCHITECTURE.md#phase-4-absolute-deadlines-and-cooperative-execution-cancellation).

Middleware and guard callback implementations now take the signal as their last
parameter. `closed` receives cleanup cancellation; forward callbacks and normal
`after_message` receive execution cancellation. Exchange accessors agree with
that parameter. The additive `on_message_termination` receives a borrowed
`WsMessageTerminationContext<'_>` by value and `CleanupCancellation`; its context
accessor returns that same cleanup view. It can use the retained message scope
and locals, but returns `Result<(), WsMiddlewareError>` without response-decision
authority. Normal exits that completed, returned an error or panicked are not
terminated again. An interrupted normal exit retains its first-poll evidence
for `context.normal_exit()`; cleanup must tolerate partial prior work. See the
[Phase 5 flow and migration](SHUTDOWN_ARCHITECTURE.md#phase-5-separate-normal-unwind-and-message-termination).
Custom lifecycle extractors use the phase-specific optional
`execution_cancellation()` / `cleanup_cancellation()` accessors instead of the
former raw `cancellation()` accessor. See the
[Phase 2 migration and implementation limits](SHUTDOWN_ARCHITECTURE.md#phase-2-migration).

The namespace and local event are bounded canonical tokens. Clients select the
namespace at Upgrade and send the full `namespace:event` envelope value. Lily
performs exact admission and lookup; `/` is not a wildcard namespace.

For subprotocol inspection, `request.headers().get_protocols()` is the
client-offered list, while `request.negotiated_subprotocol()` is the one value
selected during Upgrade. `request.supports_protocol(name)` compares exactly and
case-sensitively with that selected value; an offered-but-unselected value does
not count as supported.

`request.created_at` and `request.message_age()` are per-message timing.
`request.connection_age()` and `request.uptime()` measure from the connection
manager's transport-admission timestamp. They therefore diverge normally on
connections that have already been alive before the current message arrives.

There are no generated application actions. If an application needs status,
room join/leave, send, subscription, or CRUD relay events, it declares those
methods explicitly and owns their DTO, authorization, and semantics.

Lifecycle methods use `#[connected]` and `#[disconnected]`; only one of each is
allowed per controller. They use a separate body-free extractor matrix and
return `Result<(), WebSocketLifecycleError>`; `DisconnectReason` is available
only to `#[disconnected]`.

Message methods accept at most sixteen owned typed extractors. Body-free
extractors may appear on either side of one payload authority, so
`Payload<T>, Service<S>` remains valid. Two payload consumers, lifecycle
payloads, references, and raw request/invocation parameters fail at compile
time. A message returns an explicit `Ack<T>`, `Emit<T>`, `NoReply`, or
`CloseConnection` outcome inside `Result<_, WebSocketActionError>`.
