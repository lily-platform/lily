//! Typed middleware contracts used by Lily's HTTP and WebSocket runtimes.
//!
//! This crate is framework infrastructure, not a standalone middleware
//! application. HTTP applications should depend on `lily_http_api`, which
//! re-exports the supported CORS, CSRF, rejection and custom middleware
//! contracts from its crate root. WebSocket applications obtain the shared
//! descriptor and diagnostic types through `lily_websocket::middleware`.
//! Neither application style needs a direct `lily_middleware` dependency. The
//! transport adapters in `__private` are a cross-crate implementation seam and
//! are not an application API.
//!
//! The public root contains three groups of stable contracts:
//!
//! - [`HttpMiddleware`] and its bounded descriptor/error types for custom HTTP
//!   middleware;
//! - [`CorsPolicy`] and the optional [`CorsOriginResolver`] extension point;
//! - [`CsrfPolicy`] plus session-binding and token-store extension points.
//!
//! Lily validates policy metadata while building the application. CORS is
//! evaluated at the HTTP boundary, while custom middleware runs as an
//! around-style chain through [`HttpNext`]. Applications should normally use
//! these names through `lily_http_api` rather than adding this crate directly.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod cors;
mod csrf;
mod http;
mod termination;

/// Reserved cross-crate integration SPI for Lily transport adapters.
///
/// This module is not part of the user middleware contract and may change
/// before Lily 1.0. Applications should use the root-level facade types.
#[doc(hidden)]
pub mod __private {
    pub use crate::cors::{
        CorsLayerAdapter, CorsOriginResolverRegistry, CorsRequestKind, CorsResponseFuture,
        CorsRuntimeRejection, CorsService,
    };
    pub use crate::csrf::{CompiledCsrfPolicy, CsrfOperationError, CsrfRuntimeRejection};
    pub use crate::http::HttpNextService;
}

// Re-export commonly used middleware
pub use cors::{
    CorsDisabled, CorsOriginContext, CorsOriginResolver, CorsOriginResolverError,
    CorsOriginResolverInitError, CorsPolicy, CorsPolicyProvider,
};
pub use csrf::{
    CSRF_STORE_KEY_BYTES, CsrfBindingError, CsrfCookiePolicy, CsrfMode, CsrfPolicy,
    CsrfRequestLocalBinding, CsrfSecret, CsrfSecretError, CsrfSessionBinding,
    CsrfSessionCookieBinding, CsrfSessionId, CsrfStoreKey, CsrfToken, CsrfTokenStore,
    CsrfTokenStoreError, MAX_CSRF_STORE_TIMEOUT, StoredCsrfToken,
};
pub use http::{
    HttpExchange, HttpMiddleware, HttpMiddlewareError, HttpMiddlewareFailureKind,
    HttpMiddlewareInitError, HttpMiddlewareRejection, HttpNext, MAX_HTTP_MIDDLEWARES,
    MiddlewareConfigError, MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind,
    validate_middleware_count, validate_middleware_descriptors,
};
pub use termination::{
    HttpMiddlewareStage, HttpRequestInterruption, HttpRequestTerminationContext,
    HttpRequestTerminationMetadata,
};
