# lily_http_api

`lily_http_api` is Lily's application-facing HTTP facade. It owns application
composition, struct-controller registration, typed request extraction,
middleware and guard execution, CORS and CSRF policy integration, OpenAPI
publication, bounded HTTP/1.1 and HTTP/2 transport, streaming responses, and
graceful shutdown.

Application crates should depend on this crate instead of depending directly on
its HTTP implementation crates: `lily_web_core`, `lily_middleware`, and
`lily_http_api_macros`. The facade re-exports the supported HTTP values, DI
contracts, controller derives, policy types, and hidden dependencies used by
generated controller and DI code. Define services with
`lily_http_api::{Injectable, ServiceTrait}`; DI does not require additional
derive, registry, error or `linkme` dependencies. Independent application
layers can use `lily_injection::{Injectable, ServiceTrait}` instead. Database
operations still belong to their respective repository/provider crates.

```toml
[dependencies]
lily_http_api = "0.1.0"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

All production HTTP capabilities are available in the default build. The only
Cargo feature, `fuzzing`, exposes repository-owned fuzz harnesses and is not an
application feature.

## Minimal application

Controllers are discovered at link time and materialized once for each built
application. `ControllerTrait::new` is the controller's app-scoped constructor;
resolve long-lived dependencies there and retain them in the struct.

```no_run
use std::sync::Arc;

use lily_http_api::async_trait::async_trait;
use lily_http_api::{
    controller, AppBuilder, Controller, ControllerInitError, ControllerTrait,
    Extensions, HttpApiError, Json, Path,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct UserPath {
    id: String,
}

#[derive(Deserialize)]
struct UpdateUser {
    display_name: String,
}

#[derive(Serialize)]
struct UserView {
    id: String,
    display_name: String,
}

#[derive(Controller)]
#[base_path("/api/users")]
struct UsersController;

#[async_trait]
impl ControllerTrait for UsersController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl UsersController {
    #[put("/:id")]
    async fn update(
        &self,
        Path(path): Path<UserPath>,
        Json(input): Json<UpdateUser>,
    ) -> Result<UserView, HttpApiError> {
        Ok(UserView {
            id: path.id,
            display_name: input.display_name,
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    lily_http_api::tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            AppBuilder::new("127.0.0.1:8080")
                .tls_disabled()
                .build()
                .await?
                .start()
                .await?;
            Ok::<(), Box<dyn std::error::Error>>(())
        })?;
    Ok(())
}
```

The facade re-exports Tokio runtime types. The upstream `#[tokio::main]`
attribute itself expands through the crate name `tokio`, so applications that
prefer that attribute must also declare their own compatible `tokio`
dependency. The builder form above works with the dependency list shown here.

`AppBuilder::default()` obtains the listener address and transport/TLS values
from Lily configuration. `AppBuilder::new("host:port")` overrides only the
listener address. Use `transport_config`, `rustls_config`, or `tls_disabled`
when the composition root must override those other sources explicitly.

The minimal application still loads configuration during DI initialization.
Create `lily.toml` with:

```toml
[server]
host = "127.0.0.1"
port = 8080
```

Then run with the bootstrap pair (replace the path with your file):

```bash
LILY_CONFIG_PATH=./lily.toml LILY_CONFIG_MODE=development cargo run
```

Use `production` with deployment configuration. A custom DI container can seed
its own `ConfigService` instead. The facade alternative is `lilyrs` with
feature `http-api`, imported through `lilyrs::http_api`.

## Controller and action attributes

A controller consists of exactly three pieces:

1. `#[derive(Controller)]` and one `#[base_path("/...")]` on a non-generic
   struct.
2. An implementation of `ControllerTrait`.
3. One or more `#[controller]` inherent impl blocks containing only async
   actions with an immutable `&self` receiver.

Put ordinary helper methods in a separate inherent impl. Controller-level
attributes are optional:

- `#[middleware(TypeA, TypeB)]` applies ordered middleware to every action.
- `#[cors(PolicyProvider)]` selects a controller CORS policy.
- `#[openapi(...)]` supplies inherited OpenAPI metadata.

Each action uses exactly one method attribute: `get`, `post`, `put`, `patch`,
`delete`, `head`, `options`, or `#[route(method = "CUSTOM", path = "/...")]`.
It may additionally declare one each of:

- `#[guard(GuardA, GuardB)]`;
- `#[middleware(TypeA, TypeB)]`;
- `#[cors(PolicyProvider)]` or `#[cors(CorsDisabled)]`;
- `#[openapi(...)]` or `#[openapi(skip)]`.

Controller middleware runs before action middleware. Guards run after both.
Duplicated attributes or duplicated middleware/guard types fail at compile or
build time instead of silently changing precedence.

## Typed action parameters

Declare only the parameters an action needs and keep them in this order:
request-parts/service extractors, then at most one body consumer, then an
optional raw request or response capability permitted by the response mode.

| Parameter | Meaning | Absence or malformed input |
| --- | --- | --- |
| `Path<T>` | Strictly deserialize route parameters | rejection |
| `Query<T>` | Deserialize the URI query with repeated-key support | rejection |
| `TypedHeader<T>` | Required `headers::Header` value | missing or invalid rejection |
| `Option<TypedHeader<T>>` | Optional typed header | `None` only when absent; malformed rejects |
| `RequestCookies` | Parsed, immutable request cookie jar | malformed header rejects |
| `ExecutionCancellation` | Read-only cancellation for this accepted execution | supplied by the HTTP owner |
| `Principal` | Verified principal previously attached by application policy | missing rejects |
| `Option<Principal>` | Optional verified principal | absent becomes `None` |
| `Local<T>` | Clone a typed request-local value | missing is an internal-state rejection |
| `Option<Local<T>>` | Optional typed request-local value | absent becomes `None` |
| `ClientIp` | Effective transport-authenticated client IP | missing rejects |
| `Option<ClientIp>` | Optional effective client IP | absent becomes `None` |
| `Service<T>` | Resolve a concrete DI service in the active request scope | DI failure rejects |
| `Service<dyn Trait>` | Resolve a registered DI interface | DI failure rejects |
| `Json<T>` | Bounded JSON body | body consumer |
| `Form<T>` | Bounded URL-encoded body | body consumer |
| `MultipartForm<T>` | Strict bounded multipart DTO | body consumer |
| `RawBody` | Bounded buffered bytes | body consumer |
| `BodyStream` | Pull-driven terminal body stream | body consumer |
| `&mut Request` | Advanced access to the framework request | no second raw request allowed |
| `&mut Response` | Manual response authority | only with manual/unit result mode |
| `PassthroughResponseContext<'_>` | Stage status, headers, or cookies around a typed return | only with a typed return |

Parts extractors never consume the body. An action has at most one body
consumer. Buffered consumers may coexist with `&mut Request` after extraction;
`BodyStream + &mut Request` is rejected because streaming transfers terminal
body ownership. Custom extractors implement `FromRequestParts`,
`OptionalFromRequestParts`, or `FromRequest` and must return bounded,
secret-safe rejections. A custom `FromRequest` body extractor must be the last
typed action parameter so Lily can select the terminal body contract.

`Json<T>` requires `application/json` or an `application/*+json` media type.
`Form<T>` requires `application/x-www-form-urlencoded`; neither silently
guesses from bytes. An absent `RawBody` is an empty bounded byte buffer.
`FormFile::filename()` and `content_type()` are untrusted client metadata: do
not use either as a filesystem path or authorization decision.

`Local<T>` returns an owned clone. Prefer `Local<Arc<T>>` for large values.
Authentication middleware or guards may publish `Principal` with
`Request::set_principal` and arbitrary typed state with
`Request::local_mut().insert(value)`.

For multipart DTOs, derive `MultipartForm`. Text fields support `String`,
`Option<String>`, and `Vec<String>`; file fields support `FormFile`,
`Option<FormFile>`, and `Vec<FormFile>` and require `#[form_file]`.

```ignore
#[derive(lily_http_api::MultipartForm)]
struct UploadInput {
    title: String,
    #[form_file]
    avatar: lily_http_api::FormFile,
}
```

## Response modes

Application results use `Result<T, E>` with `E: IntoResponse + Send`.
`HttpApiError` remains a ready-made error type with safe public messages and
localization. The action signature determines one response authority:

- Return `Result<T, E>` to serialize an ordinary `T: Serialize` DTO
  as JSON. Return `Json<T>` for an explicit JSON response. `String` and
  `&str` are plain text, while `bool` is JSON and `Vec<u8>` is binary.
- Return `PlainText`, `BinaryData`, `ResponseBuilder`, or their supported
  `Result<_, E>` forms when that representation must be explicit.
  In particular, `Result<String, E>` is a JSON string and
  `Result<Vec<u8>, E>` is a JSON array; use
  `Result<PlainText, E>` or `Result<BinaryData, E>` for
  fallible text or binary responses.
- Return `NoContent` for an intentional empty response.
  `Result<(), E>` is a manual/no-representation success and is not
  an alias for `NoContent`.
- Return `Created<T>` for `201 Created` or `Accepted<T>` for `202 Accepted`.
  Both support JSON DTOs, optional `Location` headers, and `Result<_, E>` with
  any application error implementing `IntoResponse + Send`.
- Accept `&mut Response` only when the action itself writes the complete
  response and returns `()` or `Result<(), E>`.
- Accept `PassthroughResponseContext<'_>` when returning a typed body but also
  setting success status, application headers, or cookies. Typed conversion
  owns the representation; passthrough metadata is committed atomically around
  it.

```ignore
async fn create(
    &self,
    mut response: lily_http_api::PassthroughResponseContext<'_>,
) -> Result<UserView, lily_http_api::HttpApiError> {
    let cookie = lily_http_api::ResponseCookie::new("session", "opaque-id")
        .map_err(lily_http_api::PassthroughResponseError::from)?
        .secure(true)
        .http_only(true)
        .same_site(lily_http_api::CookieSameSite::Lax)
        .path("/")
        .map_err(lily_http_api::PassthroughResponseError::from)?;
    response.status(201)?;
    response.set_cookie(&cookie)?;
    Ok(UserView { /* ... */ })
}
```

Do not write a header or cookie to raw `Response` and then return a typed body;
raw response mode and typed response mode are deliberately separate contracts.
Passthrough cannot be combined with a unit/manual response, cannot set
representation or transport-owned headers, and accepts only body-permitting
success statuses. A status override is rejected for authoritative
`NoContent`, `Created`, `Accepted`, streaming, SSE, static-file, or complete `ResponseBuilder`
responses. If the action or serialization fails, every staged status, header,
and cookie is discarded.

Use raw `&mut Response` only when the action owns the complete response:

```ignore
async fn accepted(
    &self,
    response: &mut lily_http_api::Response,
) -> Result<(), lily_http_api::HttpApiError> {
    response.status(202, "Accepted");
    response
        .try_insert_header("X-Job-State", "queued")
        .map_err(|_| lily_http_api::HttpApiError::ResponseEncodingError(
            "response header was rejected".to_owned(),
        ))?;
    response.set_body_vec(b"queued".to_vec())?;
    Ok(())
}
```

`Created::new(location, value)` and `Accepted::new(location, value)` serialize
the DTO directly as JSON; no `Json<T>` wrapper is needed. Use
`without_location(value)` to omit `Location`, or `empty().with_location(location)`
to return only a status and location. The bare `Created` and `Accepted` types
default to `EmptyBody`; unlike `NoContent`, they retain their `201`/`202` status.
An explicit `Created<()>`, `Accepted<()>`, or `None` JSON value produces `null`,
not an absent body. Header validation and bounded JSON serialization happen
during response conversion and return `ResponseWriteError` on failure.

```rust
use lily_http_api::{Accepted, Created, HttpApiError};
use serde::Serialize;

#[derive(Serialize)]
struct UserView { id: u64 }

async fn create_user() -> Result<Created<UserView>, HttpApiError> {
    let user = UserView { id: 42 };
    Ok(Created::new(format!("/users/{}", user.id), user))
}

async fn queue_report() -> Accepted {
    Accepted::empty().with_location("/jobs/42")
}
```

`HttpApiError` in this example can be replaced by any `E: IntoResponse + Send`.
`Accepted` describes work that the application has accepted for processing;
it neither queues the job nor guarantees its completion. Its location should
identify an endpoint for monitoring the operation.

Applications may implement `IntoResponse` for a domain response wrapper. The
implementation must use the supplied bounded `Response` and return the
`ResponseWriteOutcome` from one atomic framework response conversion; it must
not create an independent transport response.

```rust
struct CustomCreated<T>(T);

#[lily_http_api::async_trait::async_trait]
impl<T> lily_http_api::IntoResponse for CustomCreated<T>
where
    T: serde::Serialize + Send + 'static,
{
    async fn write_to_response(
        self,
        response: &mut lily_http_api::Response,
        request: &mut lily_http_api::Request,
    ) -> Result<lily_http_api::ResponseWriteOutcome, lily_http_api::ResponseWriteError> {
        let value = lily_http_api::ResponseBuilder::new()
            .status(201, "Created")
            .json(self.0);
        lily_http_api::IntoResponse::write_to_response(value, response, request).await
    }
}
```

Generic streaming, SSE, and static files are typed action returns:

- `StreamingResponse::new(stream)` or `streaming(stream)` for bounded byte
  chunks. A source yields `Result<Bytes, E>`; use `content_length`,
  `max_chunk_bytes`, and `max_total_bytes` when the application knows narrower
  limits than the server snapshot.
- `SseResponse::new(stream)`, `sse(stream)`, or bounded `sse_channel(capacity)`
  for validated `SseEvent` values and optional keep-alive. Awaited channel
  sends apply backpressure; `try_send` returns the unsent event when full, and
  `closed` observes client disconnect. `SseEvent` supports validated `event`,
  `id`, and `retry` fields; `RequestExt::last_event_id()` reads the browser's
  bounded, duplicate-rejecting reconnection cursor.
- create a capability-confined `StaticFileMount` at startup and return the
  `StaticFileResponse` from `mount.serve(request).await`. The mount handles
  `GET`/`HEAD`, conditional requests, byte ranges, MIME selection, cache policy,
  traversal rejection, and its configured symlink policy.

These paths preserve transport backpressure, response byte limits, disconnect
cancellation, and response-write timeouts. They do not buffer an unbounded
stream in memory. Streaming source code can use the facade's `Bytes` export;
the chosen stream combinator crate remains an application dependency.

An application error needs one response converter for all supported `Result`
forms, including JSON DTOs, explicit wrappers, manual unit results, streams,
SSE and static files:

```rust
use lily_http_api::{
    IntoResponse, Request, Response, ResponseBuilder,
    ResponseWriteError, ResponseWriteOutcome,
};

pub enum AppError {
    UserNotFound,
}

#[lily_http_api::async_trait::async_trait]
impl IntoResponse for AppError {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Self::UserNotFound => ResponseBuilder::new()
                .status(404, "Not Found")
                .json(serde_json::json!({ "message": "User not found" }))
                .write_to_response(response, request)
                .await,
        }
    }
}
```

Actions can now return `Result<UserView, AppError>`. `Result::Err` writes the
error on a clean response with the same limits and automatically returns
an error-marked `Ok(ResponseWriteOutcome)` (preserving explicit classification
when supplied). Only a failure to build that response returns
`Err(ResponseWriteError)`. A failing converter cannot
return `Preserved` instead of writing its response. Response streams remain
lazy; failures after headers are sent follow the existing body/transport path.

An optional validated `HttpErrorCode` can be supplied with
`ResponseWriteOutcome::error(Some(code))`. This existing constructor records
error origin without asserting a technical failure. Without a code, Lily uses a
fixed status-based diagnostic. A JSON `code` field is not automatically a trace
label. Direct error returns outside `Result` should explicitly return an error
outcome when they need the same origin metadata. Applications can also classify
the successfully rendered error without implementing any tracing trait:

```rust
use lily_http_api::{HttpErrorCode, ResponseFailureKind, ResponseWriteOutcome};

let conflict = ResponseWriteOutcome::rejected(Some(HttpErrorCode::new("CONFLICT").unwrap()));
let unavailable = ResponseWriteOutcome::classified_error(
    ResponseFailureKind::Error,
    Some(HttpErrorCode::new("SERVICE_UNAVAILABLE").unwrap()),
);
```

Return this outcome from the converter **after** its response write succeeds.
`Result`, manual actions and passthrough actions preserve the classification;
passthrough success status/headers cannot overwrite an error representation.
Legacy converters retain their signatures. Their handler classification falls
back to 4xx = `rejected`, 5xx = `error`, other statuses = `success`; an `Err`
rendered as 200 still records application error origin. Explicit classification
can describe an application failure independently of its chosen HTTP status.

Request telemetry keeps the final HTTP status classification and separately
records `lily.application_error`, `lily.application_error_code` and, when known,
`lily.application_outcome`. Handler completion events use INFO for success,
WARN for rejection, and ERROR for technical or response-writing failure.
Ordinary 4xx responses leave the HTTP SERVER OTel status `Unset`; 5xx and actual
writer/transport failures mark it `Error`. General guard outcome metrics use
`rejected` / `error` instead of the former combined `denied` label; the dedicated
CSRF outcome vocabulary is unchanged.

A middleware that handles an application error and changes its status to 200
changes the HTTP outcome to success while the application error remains
observable. A failed writer or a later stream/transport stop takes precedence,
even if a 200 status has already been selected. The request's final status,
outcome, application metadata and byte counts are recorded once at terminal
observation; earlier events need not contain these terminal span fields.

HTTP `duration_ms` / `duration_us` event fields are numeric floating-point
values in their named units. Status and byte span attributes are OTLP integers;
counts above `i64::MAX` saturate at that limit rather than becoming negative.
Update log indexes expecting string durations and alert/metric queries that
previously treated all application errors as technical failures. Metric duration
histograms keep their existing seconds unit.

Migration: existing `Result<T, HttpApiError>` actions and infrastructure error
conversions remain supported. Existing custom `IntoResponse` implementations
must change their method error type from `HttpApiError` to
`ResponseWriteError`. Header/body/serialization failures propagate through
that technical channel; application errors are rendered using their converter.
Application-specific `From<InjectionError>` or database-error mappings for `?`
remain the application's responsibility. Hidden `HttpAction` implementations
must also return the new outcome/write-error future instead of discarding the
outcome with `.map(|_| ())`.

## Dependency injection ownership

`AppBuilder` creates and owns an `ApplicationContainer` unless a caller-owned
container is supplied with `container(Arc<ApplicationContainer>)`. A supplied
container may be shared with another adapter such as WebSocket, but only one
HTTP application lifecycle may attach to the same container.

- Resolve controller-wide singleton dependencies in `ControllerTrait::new`.
- Use `Service<T>` in an action for scoped or transient resolution in the
  active request context. Concrete and registered `dyn Trait` services use the
  same syntax.
- Middleware and guards receive `Arc<Extensions>` in their `new` constructor.
  They are constructed once per application. Retaining a transient there makes
  that instance application-lived; request-scoped dependencies must be
  resolved during request handling.
- `App::container()` exposes the composition root and `App::extensions()` a
  read-only provider handle. The application must not close an App-owned
  container while its HTTP lifecycle is running.

Clients/resources created directly by controller, middleware or guard code,
and work started with raw `tokio::spawn`, remain application-owned. Register
application-lifetime async resources as DI-managed services when Lily should
own their disposal. A user-created task is not implicitly part of Lily's
shutdown task inventory.

`secret_resolver(resolver)` seeds an App-owned composition root before config
loading. It cannot be combined with `container(...)`; configure secrets while
constructing a caller-owned container instead.

## Request pipeline, middleware, and guards

The production order is:

```text
connection/request admission
→ request deadline
→ CORS transport policy
→ request-scoped DI context
→ optional session middleware
→ optional global CSRF enforcement
→ application middleware
→ route lookup
→ controller middleware
→ action middleware
→ guards
→ action
→ bounded response transport
```

Register global custom middleware by type:

```ignore
AppBuilder::new("127.0.0.1:8080")
    .middleware::<RequestTracingMiddleware>()
```

`HttpMiddleware::new(Arc<Extensions>)` builds one instance. In `handle`, call
`next.run(exchange).await` exactly once to continue, or return/write a response
to short-circuit. Use `HttpMiddlewareRejection` for deliberate application
rejections and bounded `HttpMiddlewareError` categories for runtime failures.

Lily provides the guard mechanism, not application authentication,
authorization, or rate-limit policy. Implement `GuardTrait`, attach it with
`#[guard(MyGuard)]`, and publish verified identity as `Principal` or
request-local state. `AppBuilder::guard(instance)` is the explicit path for a
prebuilt guard whose configuration cannot be obtained in `GuardTrait::new`.
Both guard and middleware rejections support bounded status, headers,
`Retry-After`, problem detail, JSON/text payloads, and safe diagnostic codes.

## CORS

Install one global/default policy with `AppBuilder::cors(CorsPolicy)`. The
policy may use a static allowlist or a dynamic resolver. CORS runs at the
transport boundary inside the request deadline; accepted preflight requests
may finish before ordinary middleware, guards, and actions.

`CorsPolicy` supports exact or wildcard origins, exact or wildcard methods and
headers, exposed headers, credentials, `max_age`, and opt-in Private Network
Access. Wildcards cannot be combined with credentials. For tenant-aware origin
decisions, implement `CorsOriginResolver` and select it with
`resolve_origins_with::<Resolver>()`. Lily builds one DI-aware resolver instance
per resolver type and App: policies using the same type share that instance,
while different resolver types may coexist.

```ignore
use std::{sync::Arc, time::Duration};
use lily_http_api::{
    CorsOriginContext, CorsOriginResolver, CorsOriginResolverError,
    CorsOriginResolverInitError, CorsPolicy, Extensions,
};

struct TenantOriginResolver;

#[lily_http_api::async_trait::async_trait]
impl CorsOriginResolver for TenantOriginResolver {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, CorsOriginResolverInitError> {
        Ok(Self)
    }

    async fn allows(
        &self,
        context: &CorsOriginContext<'_>,
    ) -> Result<bool, CorsOriginResolverError> {
        Ok(matches!(
            context.origin(),
            "https://tenant-a.example" | "https://tenant-b.example"
        ))
    }
}

let cors = CorsPolicy::new()
    .resolve_origins_with::<TenantOriginResolver>()
    .allow_methods(["GET", "POST"])
    .allow_headers(["content-type", "authorization"])
    .expose_headers(["x-request-id"])
    .allow_credentials(true)
    .allow_private_network(false)
    .max_age(Duration::from_secs(600));
```

Use `allow_origins([...])` instead of a resolver for a static allowlist, and
`allow_any_origin()` only for a genuinely public, credential-free resource.
`permissive_for_development()` is not a production default. Resolver failures
fail closed; do not replace a failed tenant lookup with `true`.

The remaining policy switches are explicit: `allow_null_origin()` admits the
literal opaque origin, `vary_by(...)` extends response cache variance,
`without_max_age()` omits preflight caching, and `expose_any_header()` is
subject to the same credential/wildcard validation as the other wildcard
settings.

Controller/action `#[cors(Provider)]` policies select the policy for that route,
whether or not a global policy exists. Routes without either use the
deny-by-default fallback. A provider implements `CorsPolicyProvider` and is
evaluated while building the App. `#[cors(CorsDisabled)]` cuts off inherited
CORS configuration and selects that empty/deny policy; it does not bypass the
transport's CORS processing. Application-produced CORS/PNA response headers
are sanitized; the compiled policy remains authoritative.

## Session slot and CSRF

Lily does not invent an application session model or store. If global CSRF
needs verified session state, register exactly one application-defined session
middleware with `session_middleware::<M>()`; it always runs before CSRF and
ordinary middleware. The middleware may validate a cookie/store entry and
publish `CsrfSessionId` or another application session value into request-local
state.

CSRF is opt-in and has two enforcement placements:

- `csrf(policy)` protects unsafe requests globally at the beginning of the
  managed middleware chain. Exact `policy.bypass(method, path)` entries are
  explicit exceptions.
- `csrf_route_scoped(policy)` creates the same runtime without global
  enforcement; only actions declaring `#[guard(CsrfGuard)]` are protected.

Policy profiles are:

- `CsrfPolicy::cross_origin()` for browser `Origin`/`Sec-Fetch-Site` defense;
- `signed_double_submit(secret, binding)` for stateless session-bound tokens;
- `defense_in_depth(secret, binding)` for both checks;
- `synchronizer(secret, binding, store)` for an application-provided stateful
  token store;
- `synchronizer_defense_in_depth(...)` for both checks.

`cross_origin()` accepts an unsafe request when both `Origin` and
`Sec-Fetch-Site` are absent, because there is no browser-origin signal to
validate. Use a token profile when clients or proxies may omit those headers,
or when protection must not depend solely on browser fetch metadata.

For an opaque session cookie that is already authoritative at the request
boundary, `CsrfSessionCookieBinding::new(cookie_name)` is the smallest binding:

```ignore
let policy = lily_http_api::CsrfPolicy::defense_in_depth(
    lily_http_api::CsrfSecret::new(application_secret)?,
    lily_http_api::CsrfSessionCookieBinding::new("__Host-lily-session")?,
)
.trust_origins(["https://app.example"])
.token_header("x-csrf-token")
.token_ttl(std::time::Duration::from_secs(3600));

let app = lily_http_api::AppBuilder::new("127.0.0.1:8080")
    .csrf(policy)
    .build()
    .await?;
```

That cookie binding parses and binds the exact cookie value; it does not load
or validate an application session store. When store validation must happen
before global CSRF, register an application session middleware and use
`CsrfRequestLocalBinding::new()`. The middleware validates/loads the session,
constructs `CsrfSessionId::new(opaque_id)`, inserts it with
`request.local_mut().insert(id)`, and only then calls `next`:

```ignore
let policy = lily_http_api::CsrfPolicy::defense_in_depth(
    lily_http_api::CsrfSecret::new(application_secret)?,
    lily_http_api::CsrfRequestLocalBinding::new(),
);

lily_http_api::AppBuilder::new("127.0.0.1:8080")
    .session_middleware::<ApplicationSessionMiddleware>()
    .csrf(policy)
```

In route-scoped mode, put `#[guard(CsrfGuard)]` after any guard that publishes
the required session binding. A token endpoint resolves `CsrfService`, calls
`issue(request, response)`, returns the token to the client, and preserves the
cookie written by signed mode by using manual `&mut Response` authority.
Unsafe requests send that public token in the configured header. Do not obtain
the token by reading the HTTP-only CSRF cookie in browser code.

Key rotation accepts bounded old signing keys through
`previous_verification_key`. `cookie_policy` controls the signed-token cookie;
`allow_form_field` additionally accepts a URL-encoded form field while
rejecting ambiguous header-plus-form input. `token_ttl` bounds issued tokens,
and stateful stores additionally use the hard-bounded `store_timeout`.

`CsrfService` is always available as an injectable singleton. Without a CSRF
runtime its operations return `NotConfigured`; with the cross-origin-only
profile, token operations return `TokenModeRequired`. In a token profile, a
token endpoint calls `issue`; login/privilege changes use `rotate`; logout or
session invalidation uses `clear` according to the selected mode. The
application owns session creation, rotation, persistence, and authentication.
Never place CSRF secrets or tokens in logs or error messages.

## Cookies

`RequestCookies` exposes the request's authoritative parsed `RequestCookieJar`.
Duplicate requested cookie names and malformed cookie input fail closed. Values
are not silently percent-decoded.

Use `ResponseCookie` for typed `Secure`, `HttpOnly`, `SameSite`, `Path`,
`Domain`, `Max-Age`, `Expires`, and `Partitioned` attributes, and
`CookieRemoval` for deletion. Signed and private request jars and response
operations use application-owned `CookieKeyRing` material. Lily preserves and
serializes the cookie; the application owns refresh-token validation,
rotation, revocation, session persistence, and expiry policy.

With a typed return, write cookies through `PassthroughResponseContext`. With a
manual response, use the corresponding `Response` methods.

Build one immutable `CookieKeyRing` from application-owned secret material.
Its primary key signs/encrypts new cookies; bounded previous keys verify old
cookies during rotation. A verified `SecureCookieValue::needs_rotation()`
signals that the response should reissue the value with the primary key.

```ignore
let key_ring = lily_http_api::CookieKeyRing::new(primary_secret)?
    .with_previous(previous_secret)?;

let verified = cookies
    .private(&key_ring)
    .get("session")
    .map_err(|_| lily_http_api::HttpApiError::Unauthorized(
        "session cookie is invalid".to_owned(),
    ))?;

if let Some(value) = verified.filter(|value| value.needs_rotation()) {
    let replacement = lily_http_api::ResponseCookie::new("session", value.value())
        .map_err(lily_http_api::PassthroughResponseError::from)?
        .secure(true)
        .http_only(true)
        .same_site(lily_http_api::CookieSameSite::Lax)
        .path("/")
        .map_err(lily_http_api::PassthroughResponseError::from)?;
    response.set_private_cookie(&replacement, &key_ring)?;
}
```

Use `signed` when integrity/authenticity is sufficient and `private` when the
cookie value also needs confidentiality. Deletion uses `CookieRemoval` and
must repeat the original `Path`, `Domain`, and `Partitioned` scope; otherwise a
browser may retain a different cookie. Key storage, refresh-token/session
revocation, and rotation timing remain application responsibilities.

## OpenAPI 3.1

OpenAPI generation is opt-in at both application and route metadata levels:

1. Add `#[openapi(...)]` to documented controllers/actions. Import the
   upstream derives through `use lily_http_api::utoipa::{self, ToSchema,
   IntoParams, IntoResponses, ToResponse};`. Keeping `utoipa::{self, ...}` in
   scope is required by the upstream derive expansion; no direct `utoipa`
   dependency is needed.
2. Build an `OpenApiConfig` and pass it to `AppBuilder::openapi(config)`.
3. Resolve injectable `OpenApiService` and explicitly publish
   `service.json()` from the endpoint of your choice.

Lily does not silently add a JSON route or Swagger UI. `#[openapi(skip)]`
explicitly excludes an action. Once `AppBuilder::openapi` is enabled, every
accepted route must be documented—directly or through inherited controller
metadata—or explicitly skipped. Typed path/query/body/multipart/response
metadata is inferred where runtime semantics are unambiguous:

- ordinary `Result<T, E>`, direct/result `Json<T>`, and
  direct/result `bool` use JSON schemas;
- direct `String`/`&str`, and direct/result `PlainText`, use `text/plain`;
- direct `Vec<u8>`, and direct/result `BinaryData`, use an octet-stream binary
  schema;
- direct/result `NoContent` uses `204`;
- direct/result `Created<T>` and `Accepted<T>` use `201` and `202`, with a JSON
  schema for `T` and an optional `Location` response header. Bare `Created` /
  `Accepted` (or explicit `EmptyBody`) omit response content metadata.

The ordinary blanket matters: `Result<String, E>` is a JSON string
and `Result<Vec<u8>, E>` a JSON array, matching their runtime
representations rather than their direct-return representations.

Custom error response schemas must be declared through `responses(...)` metadata;
implementing `IntoResponse` does not require `ToSchema` on the error type.

`RequestCookies`, custom extractors, raw bodies, `ResponseBuilder`, custom
direct `IntoResponse` types, and manual/passthrough/streaming/SSE/static-file
responses require explicit `parameters`, `request_body`, or `responses`
contracts. This is also important for passthrough status overrides: the macro
cannot infer a status selected inside the action body. A non-standard
`#[route(method = ...)]` must use `#[openapi(skip)]`, because OpenAPI Path Items
cannot represent arbitrary method tokens.

Controller metadata may supply `tag`, `description`, inherited `responses`,
and inherited `security`. Action metadata supports `operation_id`, `summary`,
`description`, `deprecated`, `parameters(...)`, `request_body(...)`,
`responses(...)`, and `security(...)`. Empty `security()` deliberately clears
an inherited requirement for one public action. Responses may be inline,
reuse a `ToResponse` type with `response = Type`, or expand an
`IntoResponses` type with `responses(Type)`.

```ignore
#[derive(lily_http_api::Controller)]
#[base_path("/api/contracts")]
#[openapi(
    tag = "Contracts",
    description = "Contract operations",
    security(("bearer" = []))
)]
struct ContractsController;

#[lily_http_api::controller]
impl ContractsController {
    #[post("/raw")]
    #[openapi(
        operation_id = "contracts.import",
        summary = "Import a contract",
        description = "Accepts a bounded opaque payload",
        deprecated,
        parameters((
            name = "tenant",
            in = "header",
            schema = String,
            required = true
        )),
        request_body(
            content_type = "application/octet-stream",
            schema = ImportPayload
        ),
        responses(
            (status = 202, description = "Accepted", schema = ContractView),
            (status = 400, response = ProblemResponse)
        ),
        security()
    )]
    async fn import(
        &self,
        body: lily_http_api::RawBody,
        mut response: lily_http_api::PassthroughResponseContext<'_>,
    ) -> Result<ContractView, lily_http_api::HttpApiError> {
        response.status(202)?;
        # unimplemented!()
    }
}
```

The OpenAPI service may be retained during `ControllerTrait::new`, but its
document is attached only after every route has been materialized. Read
`snapshot()` or `json()` from an action after `build()` has returned:

```ignore
#[get("/openapi.json")]
#[openapi(skip)]
async fn openapi(
    &self,
    lily_http_api::Service(openapi): lily_http_api::Service<lily_http_api::OpenApiService>,
) -> Result<lily_http_api::OpenApiJson, lily_http_api::HttpApiError> {
    Ok(openapi.json()?)
}
```

Build fails on unspecified documented routes, duplicate operation IDs, path
collisions, component collisions, unresolved direct local schema/response
component references, response-component cycles, or invalid security
declarations.

```ignore
let mut config = lily_http_api::OpenApiConfig::new("Example API", "1.0.0")?;
config
    .description("Application HTTP contract")?
    .register_server("https://api.example.com", Some("production"))?
    .register_tag("Users", Some("User operations"))?;
config.register_security_scheme(lily_http_api::OpenApiSecurityScheme::bearer(
    "bearer",
    Some("JWT"),
    Some("Application access token"),
)?)?;

let app = lily_http_api::AppBuilder::new("127.0.0.1:8080")
    .openapi(config)
    .build()
    .await?;
```

Security schemes describe the document; guards/middleware still perform the
actual authentication and authorization.

`OpenApiSecurityScheme::open_id_connect` accepts absolute `http` and `https`
discovery URLs. Use HTTPS for production deployments; HTTP is explicitly
supported for local development and trusted private networks. Userinfo,
passwords, and URL fragments are rejected for either scheme.

## Transport, TLS, proxies, and limits

`HttpProtocol::{Http1_1, Http2, Auto}` selects protocol behavior. `Auto` uses
ALPN when TLS is enabled and the managed Hyper auto-connection builder for
plaintext. `HttpTransportConfig` contains bounded connection, in-flight
request, header/body, multipart, buffered/streaming response, timeout, trusted
proxy, and HTTP/2 flow-control limits. Invalid values fail during `build`.

`rustls_config` accepts a complete validated Lily `RustlsConfig` and adjusts
ALPN to the selected protocol. `tls_disabled` explicitly overrides configured
certificate material. When using configuration-backed TLS, certificate and key
paths are loaded and validated before listener publication.

`ServerConfig` is the central `lily.toml` server DTO consumed by the
configuration service. `HttpTransportConfig` is the already-typed, explicit
builder override for HTTP transport limits; it is not a second configuration
file schema.

`App::listen_address()` remains the immutable configured address. After the
managed listener successfully binds, `App::bound_address()` publishes the
actual socket address selected by the operating system to every App clone. It
returns `None` before bind, including a pre-cancelled start.

`ClientIp` trusts forwarding headers only when the direct peer belongs to a
configured `TrustedProxyNetwork`; otherwise it reports the transport peer.
Deployments must preserve the effective Host/Origin and configure trusted
proxies narrowly.

## Localization, tracing, health, and shutdown

Localization is disabled by default. Select exactly one immutable source with
`localization_catalog` or `localization_path`; malformed catalogs fail before
startup. Error negotiation uses the request locale while preserving stable
error codes.

Tracing is disabled by default and never probes the working directory. Use
`tracing_config`, strict `tracing_config_path`, or `tracing_external`. External
mode requires the process composition root to install the OpenTelemetry/tracing
runtime before `AppBuilder::build` and to own its shutdown. App-owned tracing is
installed before DI construction and flushed during the managed lifecycle.
`TraceConfig::default()` is also disabled; explicitly enable it when using an
in-memory configuration:

```ignore
let mut tracing = lily_http_api::TraceConfig::default();
tracing.enabled = true;
tracing.service_name = "users-api".to_owned();
tracing.export.console = true; // or configure bounded file / OTLP export

let app = lily_http_api::AppBuilder::new("127.0.0.1:8080")
    .tracing_config(tracing)
    .build()
    .await?;
```

`HttpHealthService` is an injectable, read-only snapshot of framework-owned
HTTP lifecycle checks. Lily does not register a health endpoint; the
application chooses its response schema and route.

```ignore
#[get("/health")]
async fn health(
    &self,
    lily_http_api::Service(health): lily_http_api::Service<lily_http_api::HttpHealthService>,
) -> Result<lily_http_api::HealthSnapshot, lily_http_api::HttpApiError> {
    health.snapshot().map_err(|_| {
        lily_http_api::HttpApiError::InternalError(
            "HTTP health snapshot is unavailable".to_owned(),
        )
    })
}
```

`App::start()` owns platform signal handling. `start_with_cancellation(token)`
uses a caller-owned cancellation source. Both observe one retained lifecycle
root that owns the listener, connection and Hyper worker task receipts.
`App::close().await` requests shutdown of that root, or closes a built but
unstarted App without binding a listener. Keep an App clone for a separate close
handle. Repeated/concurrent close shares one absolute deadline and result.

After a start future's first poll registers the root, dropping that waiter
requests shutdown while the root continues cleanup. Dropping a polled close
waiter does not cancel cleanup. Entirely unpolled futures install no root.
A caller-owned container and external tracing runtime remain the caller's
shutdown responsibility. `build()` already initializes DI and optional tracing;
there is no synchronous `Drop` substitute for their async cleanup. Do not build
and abandon an App: await a managed `start` method or `close` for every successful
build. Task abort requests are distinct from actual joined termination; a root
timeout returns an error, retains outstanding receipts and is not overwritten
by a later successful join.

The [HTTP shutdown architecture](SHUTDOWN_ARCHITECTURE.md) documents the current
limits and the implemented request/body/scope ownership model. **Phases 1–9
provide evidence contracts, retained root/task/request ownership, exact DI
receipts, atomic admission, bounded cooperative cancellation and retained
response/input/file-helper ownership and reverse abnormal middleware cleanup.**
A request owner survives dispatch
timeout or service-waiter loss, retains its state and observes actual scope
termination before parent dependencies may close. Its capacity permit remains
held through owner/scope termination. Missing or failed cleanup is not success.
Request scopes now remain live through streaming/SSE source release, transferred
input release and actual framework file-helper joins. A pull-driven body bridge
preserves backpressure and contains only independent bytes/channels. Connection
drain alone does not prove complete application cleanup. The
[phase roadmap](SHUTDOWN_ROADMAP.md),
[callback migration](SHUTDOWN_MIGRATION.md), and
[qualification matrix](SHUTDOWN_QUALIFICATION.md) distinguish implemented
behavior and its verified boundaries. Force first signals accepted
execution, then keeps the same future polled for
at most 250ms, clamped by the original root cutoff. A pending slot can then be
dropped while its owner retains bounded scope cleanup. Admission closure alone
does not cancel accepted execution. Middleware `handle` and guard `can_activate`
now take a final `ExecutionCancellation` parameter. The same read-only view is
available through `Request`, `HttpExchange`, dynamic CORS context and a typed
action extractor. It cannot cancel DI cleanup. The request's absolute
deadline starts at admission and covers CORS, middleware, guards, extraction,
request body reads, action execution, lazy response production and protocol
writing. It is never restarted at dispatch, body handoff or for another chunk.
A pipeline that returns during the
cooperative window keeps its actual response, including application errors and
normal middleware post-processing, for both timeout and shutdown cancellation.
The returned response must finish production/writing within the same remaining
window; a new stream does not receive another 250ms. After that cutoff an
uncommitted response can become **504 Gateway Timeout** for local timeout or
**503 Service Unavailable** for shutdown. Sending that fallback has one internal
100ms maximum allowance, clipped by the root transport-stop cutoff; failed
finalization terminates the response transport. A committed response keeps its original
status and is truncated if it cannot finish. SSE heartbeats do not reset time.
Managed responses retain request-specific transport controls: HTTP/1 write
termination closes its connection, while HTTP/2 write termination aborts the
corresponding registered stream worker and observes its actual join. Other H2
streams can continue. The last DATA frame keeps that worker alive through queued
payload release and local I/O flush. Response selection for Hyper's encoder,
body EOF, task join and client delivery are separate observations; a committed
response cannot be replaced with another status. See the
[transport contract](SHUTDOWN_ARCHITECTURE.md#request-linked-response-transport-control).
Lazy producers capture the same view and can be stopped even without another
Hyper body poll. Source EOF,
source destruction, scope disposal and transport delivery remain separate facts.
The resource-free request clock remains with protocol control after a buffered
response's production resources and request scope have closed.
Started blocking file work cannot be preempted; missing joins block DI disposal.
During shutdown, execution/source completion does not abort a connection whose
response still has cooperative or fallback time. Remaining connections stop at
the root transport cutoff, leaving a separate reserve to observe actual joins
before dependency disposal. Dropping an unfinished managed server requests this
same drain; its listener remains owned by the application inventory.
Middleware now has a default no-op `on_request_termination(context, cancellation)`.
Only first-polled invocations that did not return normally qualify, in reverse
order after execution/body/input/helper termination and before DI disposal.
Use `exchange.termination_state_mut()` for data that must survive execution drop;
the cleanup context exposes this state, bounded metadata and the original DI
provider, without a body reader, response writer or `next`. `CleanupCancellation`
is independent of execution and sibling hooks. Its internal 250ms cap and up to
25ms notification tail remain within the same request/root cleanup deadline.
Failed, timed-out, panicked or unstarted cleanup is never recorded as successful.
Phase 7 reconciles owned DI, monitors and actual telemetry worker joins under
the same root deadline, including force and build rollback. Phase 8 adds
immutable HTTP attempt evidence and the
`http.shutdown` health check. Its final reasons distinguish graceful/forced
completion, terminal failure and unconfirmed termination. Historical retired
request failures remain separate from the shutdown cohort. No new public
callback or report API is introduced by the reporting phase. Phase 9 qualifies
the combined managed runtime, including generated callbacks, HTTP/2, SSE,
static-file helper joins and final DI/telemetry evidence. Completed server-owner
release preserves graceful intent; forced stages use their existing absolute
R/D/T cutoffs inside the same root deadline.

## Public boundary

The supported application surface is the crate root. `__private` is
generated-macro and conformance ABI; it is deliberately hidden and may change
without becoming an application contract. Applications must not construct
handlers, route registries, route tables, or the low-level Hyper server
directly.

Authentication, authorization, rate limiting, server-side session stores,
session schemas, token persistence, and Swagger UI hosting are intentionally
application/ecosystem policy. Lily supplies the guarded/middleware request
pipeline, typed rejection model, cookie/CSRF primitives, DI access, and explicit
publication hooks required to implement them without bypassing transport
invariants.

Repository/service derives such as `Repository` and `CrudService` belong to
their data-layer crates and are not HTTP controller capabilities. Add those
crates explicitly when an application chooses them; do not infer them from the
HTTP facade.

`tests/shutdown_dependencies.rs` checks normal disposal, a failed scoped
`dispose`, and a disposer that reaches the HTTP shutdown deadline and is aborted.
Each case runs in a separate process with the owned JSONL exporter. The test
requires the scope future to finish/drop before root dependency disposal, exactly
one terminal lifecycle pair under its captured W3C identity, and telemetry flush
before shutdown returns. A failed or timed-out scope keeps the failed shutdown
health result on repeated close; exporter success does not erase that failure.

## Background services

Register workers with `AppBuilder::add_background_service::<T>()`, where `T`
implements `BackgroundServiceTrait`. The trait's asynchronous `new` receives
`Arc<ApplicationScopeFactory>`; its `execute_async(&mut self, ExecutionCancellation)`
runs once after listener bind. Duplicate registration of one type is deduplicated.
Normal completion leaves the host running; an unhandled error/panic starts host
shutdown. Workers own their polling, retry and recovery loops.

Use `scopes.create_scope(ProcessContext::new())?.run(|extensions| Box::pin(async
move { /* resolve and use scoped services */ Ok::<(), WorkerError>(()) }))` for
each unit of work. The closure receives `&Extensions` inside that scope's context.
`WorkerError: From<InjectionError>` lets both application and scope-disposal
failures propagate. Existing `ApplicationScope::run` is unchanged.

Workers share the application's original shutdown budget: cancellation,
cooperation, execution abort/join, exact scope cleanup, DI disposal, then telemetry
flush. Cancellation of a build/start/close waiter does not lose the resource owner.
An incomplete worker join prevents DI/telemetry shutdown; timeout alone never
counts as termination. A caller-owned container remains caller-owned.

See [`lily_background_service`](../../foundation/lily_background_service/README.md) for a complete
worker example, standalone host integration, scope rules and blocking-work limits.
The HTTP background tests include real listener bind failure, worker faults,
cancelled observers, external-container isolation and blocked destructors.
`tests/background_telemetry.rs` verifies cooperative/forced shutdown, disposer
error/timeout, actual job/cleanup trace identities and file-exporter flush order.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_http_api](https://docs.rs/lily_http_api).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
