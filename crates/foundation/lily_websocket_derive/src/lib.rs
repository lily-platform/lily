//! Procedural macros for Lily WebSocket controllers and outbound gateways.
//!
//! [`BaseGateway`] generates an application-owned gateway backed by
//! `lily_websocket_client::TokioWsClient`. [`WebSocketController`] and
//! [`websocket_controller`] form the server-side, struct-controller surface.
//!
//! ```no_run
//! use lily_websocket_client::TokioWsClient;
//! use lily_websocket_derive::BaseGateway;
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! struct UserDto {
//!     id: String,
//! }
//!
//! #[derive(BaseGateway)]
//! #[gateway(url = "wss://events.example.com/ws", namespace = "user")]
//! #[dto_type(UserDto)]
//! struct UserGateway {
//!     #[gateway_client]
//!     client: TokioWsClient,
//!     entity_name: String,
//! }
//!
//! let gateway = UserGateway::new();
//! assert_eq!(gateway.entity_name, "user");
//! ```
//!
//! The `BaseGateway` implementation uses downstream types from
//! `async-trait`, `lily_error`, `lily_injection`, `lily_websocket_client`,
//! `serde`, and `tokio-util`. Applications using this derive must declare
//! those crates directly until Lily provides an umbrella facade.
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(clippy::print_stderr, clippy::print_stdout)]

use proc_macro::TokenStream;

mod asyncapi;
mod base_gateway;
mod controller_impl;
mod derive_controller;
mod runtime_path;
mod syntax;

/// Generates Lily's app-scoped definition and static registration for a
/// struct WebSocket controller.
///
/// Use this derive through `lily_websocket::WebSocketController`. It accepts
/// only a non-generic struct with exactly one `#[namespace("...")]`
/// declaration. Optional controller metadata is declared with
/// `#[handshake_middleware(...)]`, `#[connection_middleware(...)]`,
/// `#[message_middleware(...)]`, `#[guard(...)]`,
/// `#[frame_codec(Type)]`, `#[payload_codec(Type)]`,
/// `#[timeout(seconds = N)]`, and
/// `#[asyncapi(...)]`.
///
/// The application remains responsible for implementing
/// `WebSocketControllerTrait`; the derive emits only immutable registration
/// metadata and never constructs a live controller in a process-global
/// registry. Every `handshake_middleware`, `connection_middleware`, and
/// `message_middleware` entry must respectively implement Lily's typed
/// `WebSocketHandshakeMiddleware`, `WsConnectionMiddleware`, and
/// `WsMessageMiddleware` contract; every `guard` entry must implement
/// `WsGuard`. Their generated registrations construct one app-owned instance
/// through the application DI container during `WsAppBuilder::build`.
#[proc_macro_derive(
    WebSocketController,
    attributes(
        namespace,
        handshake_middleware,
        connection_middleware,
        message_middleware,
        guard,
        frame_codec,
        payload_codec,
        timeout,
        asyncapi
    )
)]
pub fn derive_websocket_controller(input: TokenStream) -> TokenStream {
    derive_controller::derive(input)
}

/// Expands one inherent WebSocket controller impl into app-local operation
/// registrations.
///
/// The attribute accepts no arguments. Every method in the impl must declare
/// exactly one of `#[message("event")]`, `#[connected]`, or
/// `#[disconnected]`. At most one connected and one disconnected lifecycle
/// method may be declared. Message methods may additionally carry
/// `#[message_middleware(...)]`, `#[guard(...)]`,
/// `#[payload_codec(Type)]`,
/// `#[timeout(seconds = N)]`, and
/// `#[asyncapi(...)]` metadata.
///
/// `#[timeout(seconds = N)]` on a message sets the deadline for its entire
/// pipeline, starting before the first message middleware and including normal
/// reverse exits. It overrides the controller message timeout and then the
/// server's `message_timeout_secs`. On `#[connected]`/`#[disconnected]`, it caps
/// only that lifecycle invocation; the controller message default is not inherited.
///
/// Operation parameters are owned typed extractors. A message accepts at most
/// sixteen extractors; the runtime's type-level tuple plan permits at most one
/// payload authority while allowing body-free extractors on either side of
/// that payload parameter. This means an application-defined payload
/// extractor is checked by its trait implementation rather than by a macro
/// type-name allowlist. References and raw invocation/request types are
/// rejected.
///
/// Message methods must return `Result<O, WebSocketActionError>`, where `O`
/// implements Lily's typed action-outcome contract. Unit success is rejected.
/// Lifecycle methods use their separate, payload-free extractor matrix and
/// return `Result<(), WebSocketLifecycleError>`. Message middleware and guards
/// are message-only metadata; handshake and connection middleware remain
/// controller-level authorities and are rejected on operation methods.
#[proc_macro_attribute]
pub fn websocket_controller(args: TokenStream, input: TokenStream) -> TokenStream {
    controller_impl::expand(args, input)
}

/// Derives an outbound WebSocket CRUD notification gateway.
///
/// The generated implementation validates a literal `ws://` or `wss://` URL,
/// connects through `lily_websocket_client`, emits CRUD notification events,
/// and participates in Lily DI lifecycle through
/// `lily_injection::ServiceTrait`. Exactly one named field must carry
/// `#[gateway_client]`, and `#[dto_type(...)]` is mandatory.
/// The derive never installs logging or lifecycle-event callbacks. Applications
/// that need `on_connect`, `on_disconnect`, or `on_error` observers register
/// those handlers explicitly on the generated `lily_websocket_client` client.
///
/// # Example
/// ```rust,no_run
/// use lily_websocket_client::TokioWsClient;
/// use lily_websocket_derive::BaseGateway;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct ApplicationDto {
///     id: String,
/// }
///
/// #[derive(BaseGateway)]
/// #[gateway(url = "ws://localhost:8080", namespace = "application")]
/// #[dto_type(ApplicationDto)]
/// pub struct ApplicationGateway {
///     #[gateway_client]
///     pub client: TokioWsClient,
///     pub entity_name: String,
/// }
///
/// let gateway = ApplicationGateway::new();
/// assert_eq!(gateway.entity_name, "application");
/// ```
#[proc_macro_derive(BaseGateway, attributes(gateway, gateway_client, dto_type))]
pub fn derive_base_gateway(input: TokenStream) -> TokenStream {
    base_gateway::derive_impl(input)
}
