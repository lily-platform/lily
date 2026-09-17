#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

extern crate self as lily_http_api;

pub use async_trait;
pub use bytes::Bytes;
pub use headers;
pub use lily_background_service::BackgroundServiceTrait;
pub use lily_cancellation::ExecutionCancellation;
pub use lily_config::{
    ConfigError, ConfigMode, ConfigOptions, ConfigService, ConfigSnapshot, EffectiveConfigMetadata,
    FromTomlValue, LifecycleConfig, LilyConfig, RedactedEffectiveConfig, ResolvedSecret,
    SecretBinding, SecretResolver, ServerConfig,
};
pub use lily_error::application::http_api::HttpApiError;
pub use lily_error::{LocalizationCatalog, LocalizationError};
pub use lily_http_api_macros::{controller, Controller, MultipartForm};
pub use lily_injection::{
    ApplicationContainer, ApplicationContainerBuilder, ApplicationScope, ApplicationScopeFactory,
    ContainerShutdownReport, Extensions, Injectable, InjectionError, ProcessContext,
    ServiceLifetime, ServiceScope, ServiceTrait, ShutdownOutcome, ShutdownOutcomeStatus,
    ShutdownRemainingWork, BUILD_ROLLBACK_TIMEOUT_ENV, DEFAULT_SHUTDOWN_TIMEOUT,
    MAX_BUILD_ROLLBACK_TIMEOUT_SECS,
};
pub use lily_middleware::{
    CorsDisabled, CorsOriginContext, CorsOriginResolver, CorsOriginResolverError,
    CorsOriginResolverInitError, CorsPolicy, CorsPolicyProvider, CsrfBindingError,
    CsrfCookiePolicy, CsrfMode, CsrfPolicy, CsrfRequestLocalBinding, CsrfSecret, CsrfSecretError,
    CsrfSessionBinding, CsrfSessionCookieBinding, CsrfSessionId, CsrfStoreKey, CsrfToken,
    CsrfTokenStore, CsrfTokenStoreError, HttpExchange, HttpMiddleware, HttpMiddlewareError,
    HttpMiddlewareInitError, HttpMiddlewareRejection, HttpMiddlewareStage, HttpNext,
    HttpRequestInterruption, HttpRequestTerminationContext, HttpRequestTerminationMetadata,
    MiddlewareConfigError, MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind,
    StoredCsrfToken, CSRF_STORE_KEY_BYTES, MAX_CSRF_STORE_TIMEOUT, MAX_HTTP_MIDDLEWARES,
};
pub use lily_monitoring::{
    HealthCheckKind, HealthCheckSnapshot, HealthCriticality, HealthRegistryError, HealthSnapshot,
    HealthStatus,
};
pub use lily_trace::{
    ExportConfig, FileExportConfig, FileRotation, FilterRule, OtlpExportConfig, SamplingConfig,
    SamplingStrategy, TraceCellConfig, TraceConfig, TraceConfigLoadError,
};
pub use lily_web_core::{
    json, sse, sse_channel, streaming, Accepted, BinaryData, BodyBudget, BodyBudgetError,
    CleanupCancellation, CookieExpires, CookieKeyRing, CookieKeyRingError, CookieRemoval,
    CookieSameSite, Created, EmptyBody, FormData, FormIter, FormValues, HttpBuffer, HttpErrorCode,
    HttpMethod, HttpProtocol, HttpRejection, HttpRejectionBodyKind, HttpRejectionBuilder,
    HttpRejectionError, InternedString, IntoResponse, Json, LastEventId, LastEventIdError,
    MultipartData, MultipartField, MultipartTextError, NoContent, PassthroughResponseContext,
    PassthroughResponseError, PlainText, Principal, PrivateRequestCookieJar, QueryComponent,
    QueryIter, QueryParams, QueryParseError, QueryParseErrorKind, QueryValues, RawHeader, Request,
    RequestBodyError, RequestBodyState, RequestConnectionInfo, RequestCookie, RequestCookieError,
    RequestCookieJar, RequestExt, RequestExtensions, RequestLocal, Response, ResponseBodyError,
    ResponseBuilder, ResponseCookie, ResponseCookieError, ResponseCookieWriteError,
    ResponseFailureKind, ResponseHeaderError, ResponseLimits, ResponseLimitsError,
    ResponseStatusAuthority, ResponseStreamingLimits, ResponseWriteError, ResponseWriteOutcome,
    RustlsConfig, SecureCookieError, SecureCookieValue, SignedRequestCookieJar, SseConfigError,
    SseEvent, SseEventError, SseResponse, SseSendError, SseSender, SseTrySendError,
    StaticFileError, StaticFileMount, StaticFileResponse, StaticFileSymlinkPolicy,
    StreamingResponse, TlsConfigError, TlsFileRole, DEFAULT_BODY_LIMIT_BYTES,
    DEFAULT_MAX_RESPONSE_HEADERS, DEFAULT_MAX_RESPONSE_HEADER_BYTES, HARD_MAX_RESPONSE_HEADERS,
    HARD_MAX_RESPONSE_HEADER_BYTES, MAX_BODY_LIMIT_BYTES, MAX_COOKIE_DOMAIN_BYTES,
    MAX_COOKIE_NAME_BYTES, MAX_COOKIE_PATH_BYTES, MAX_COOKIE_PREVIOUS_KEYS, MAX_COOKIE_VALUE_BYTES,
    MAX_HTTP_ERROR_CODE_BYTES, MAX_HTTP_REJECTION_BODY_BYTES, MAX_HTTP_REJECTION_DETAIL_BYTES,
    MAX_HTTP_REJECTION_HEADERS, MAX_HTTP_REJECTION_HEADER_BYTES, MAX_REQUEST_COOKIE_PAIRS,
    MAX_REQUEST_COOKIE_RETAINED_BYTES, MAX_SET_COOKIE_BYTES, MAX_SSE_CHANNEL_CAPACITY,
    MAX_SSE_EVENT_BYTES, MAX_SSE_EVENT_ID_BYTES, MAX_SSE_EVENT_NAME_BYTES, MAX_SSE_KEEP_ALIVE,
    MAX_SSE_RETRY, MAX_STATIC_FILE_CACHE_CONTROL_BYTES, MAX_STATIC_FILE_CONDITION_BYTES,
    MAX_STATIC_FILE_PATH_BYTES, MAX_STATIC_FILE_ROUTE_PREFIX_BYTES, MIN_COOKIE_KEY_BYTES,
    MIN_SSE_KEEP_ALIVE, STATIC_FILE_CHUNK_BYTES,
};
pub use tokio;
pub use tokio_util::sync::CancellationToken;
/// OpenAPI model and derive namespace used by Lily controller metadata.
///
/// Import derives with `use lily_http_api::utoipa::{self, ToSchema};` so the
/// upstream derive expansion can also resolve its `utoipa::...` runtime path.
pub use utoipa;
mod app;
mod controller;
mod csrf;
mod extractor;
mod guard;
mod handler;
mod health;
// Phase 1 defines the evidence contract; later phases attach runtime owners.
#[cfg_attr(not(test), allow(dead_code))]
mod lifecycle;
mod openapi;
mod registry;
mod request_lifecycle;
mod route_table;
mod server;
mod shutdown;
mod shutdown_report;
mod tasks;
mod telemetry;

pub use app::{
    App, AppBuildError, AppBuilder, EffectiveHttpServerConfig, HttpServerConfigError,
    HttpTlsConfigError, MissingRouteGuardError, RouteMiddlewareBuildError,
    RouteMiddlewareBuildErrorCause,
};
pub use controller::{
    ControllerBindingError, ControllerInitError, ControllerMaterializationError, ControllerTrait,
};
pub use csrf::{CsrfGuard, CsrfService, CsrfServiceError};
pub use extractor::{
    BodyStream, ClientIp, Form, FormFile, FromRequest, FromRequestParts, Local, MultipartForm,
    MultipartFormRejection, OptionalFromRequestParts, Path, Query, RawBody, RequestBodyRejection,
    RequestCookies, RequestPartsRejection, Service, TypedHeader,
};
pub use health::HttpHealthService;
pub use openapi::{
    OpenApiApiKeyLocation, OpenApiConfig, OpenApiConfigError, OpenApiJson,
    OpenApiSecurityConfigError, OpenApiSecurityScheme, OpenApiSecurityValidationError,
    OpenApiService, OpenApiServiceError, OpenApiSnapshot,
};
pub use registry::{
    OpenApiComponentKind, OpenApiDuplicateDocumentOperation, OpenApiDuplicateOperationId,
    OpenApiRouteRegistryError,
};
pub use route_table::{
    AmbiguousRouteError, DuplicateRouteError, InvalidRouteError, InvalidRouteReason,
    RouteTableStats,
};
pub use server::{HttpTransportConfig, TrustedProxyNetwork};

pub use guard::{GuardInitError, GuardRejection, GuardTrait};
// Private module for internal framework utilities
mod private;

// Re-export private module for use in macros
#[doc(hidden)]
pub mod __private {
    pub use crate::guard::guard_registry::{register_guard_metadata, GuardMetadata};
    pub use crate::handler::{Handler, HttpAction, HttpActionFuture};
    pub use crate::private::*;
    pub use crate::registry::{
        get_pending_controller_routes, get_struct_controller_registrations,
        materialize_controller_routes, ControllerRouteMaterialization, OpenApiRouteBuildError,
        OpenApiRouteMetadataStatus, OpenApiRouteRegistry, OpenApiRouteRegistryError,
        PendingControllerRoute, PendingControllerRouteRegistrationFn, RouteInfo,
        StructControllerRegistrationFn, PENDING_CONTROLLER_ROUTE_REGISTRATIONS,
        STRUCT_CONTROLLER_REGISTRATIONS,
    };
    pub use crate::server::server::HttpServer;
    pub use lily_injection;
    pub use lily_injection::Extensions;
    pub use linkme;
    pub use serde_json;
    pub use utoipa;

    /// Repository-owned fuzz adapters; unavailable in normal application builds.
    #[cfg(feature = "fuzzing")]
    #[doc(hidden)]
    pub mod fuzzing {
        pub use crate::app::fuzzing::*;
        pub use lily_web_core::RequestBodyStream;
    }
}
