//! HTTP error categories, status mapping, and redacted public envelopes.

/// Request decoding errors shared with `lily_web_core` and typed extractors.
pub mod request;
pub use request::*;

/// Stable, serialization-safe error envelope exposed to HTTP clients.
///
/// Internal causes deliberately do not appear in this type. Applications
/// should log [`HttpApiError`] with their request/trace identifier and return
/// this envelope to the caller.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PublicHttpErrorBody {
    /// Always `true`, allowing clients to distinguish this envelope.
    pub error: bool,
    /// Stable machine-readable error category.
    pub code: &'static str,
    /// Redacted, client-safe description.
    pub message: &'static str,
    /// HTTP status code returned with the envelope.
    pub status: u16,
}

/// Application error types that can be converted to HTTP responses.
///
/// Notes:
/// - This keeps the existing `String` payload style to avoid a large breaking refactor.
/// - Variants are intentionally categorized by HTTP/API concerns, not by project-specific domain rules.
/// - Project/domain errors should usually be converted into one of these variants with `From<AppError>`.
#[derive(Debug, PartialEq, Clone)]
pub enum HttpApiError {
    // ---------------------------------------------------------------------
    // Generic client/request errors - 4xx
    // ---------------------------------------------------------------------
    /// The request is syntactically or semantically invalid.
    BadRequest(String),
    /// Application input validation failed.
    ValidationError(String),
    /// The requested resource does not exist.
    NotFound(String),
    /// The route does not accept the request method.
    MethodNotAllowed(String),
    /// The requested representation cannot be produced.
    NotAcceptable(String),
    /// The request conflicts with current resource state.
    Conflict(String),
    /// The requested resource is permanently unavailable.
    Gone(String),
    /// A request precondition evaluated to false.
    PreconditionFailed(String),
    /// The request media type is unsupported.
    UnsupportedMediaType(String),
    /// The request payload exceeds its configured limit.
    PayloadTooLarge(String),
    /// The request target exceeds its configured length limit.
    UriTooLong(String),
    /// The requested byte or item range cannot be satisfied.
    RangeNotSatisfiable(String),
    /// The request is well formed but cannot be processed.
    UnprocessableEntity(String),
    /// Reading or handling the request exceeded its deadline.
    RequestTimeout(String),
    /// A rate or quota limit rejected the request.
    TooManyRequests(String),

    // ---------------------------------------------------------------------
    // Authentication / authorization errors - 401 / 403
    // ---------------------------------------------------------------------
    /// Valid authentication is required.
    Unauthorized(String),
    /// The authenticated principal may not perform the operation.
    Forbidden(String),
    /// Supplied credentials are invalid.
    InvalidCredentials(String),
    /// Supplied authentication token is invalid.
    InvalidToken(String),
    /// Supplied authentication token has expired.
    TokenExpired(String),
    /// No required authentication material was supplied.
    MissingAuthentication(String),
    /// The principal lacks a required permission.
    InsufficientPermissions(String),
    /// CSRF enforcement rejected the request.
    CsrfError(String),
    /// CORS enforcement rejected the request.
    CorsError(String),

    // ---------------------------------------------------------------------
    // HTTP parsing / protocol errors
    // ---------------------------------------------------------------------
    /// The HTTP message could not be parsed.
    HttpParseError(String),
    /// An HTTP header is malformed or forbidden.
    InvalidHttpHeader(String),
    /// The HTTP method token is invalid.
    InvalidHttpMethod(String),
    /// The HTTP protocol version is invalid or unsupported.
    InvalidHttpVersion(String),
    /// The content type is malformed or incompatible with the body.
    InvalidContentType(String),
    /// The request body cannot be decoded as required.
    InvalidRequestBody(String),
    /// The query string cannot be decoded.
    InvalidQueryString(String),
    /// A path parameter cannot be decoded or converted.
    InvalidPathParameter(String),
    /// Multipart parsing or validation failed.
    MultipartError(String),

    // ---------------------------------------------------------------------
    // Routing / controller / middleware pipeline errors
    // ---------------------------------------------------------------------
    /// No route matched the request.
    RouteNotFound(String),
    /// Controller initialization or dispatch failed.
    ControllerError(String),
    /// User handler execution failed.
    HandlerError(String),
    /// HTTP middleware execution failed.
    MiddlewareError(String),
    /// The response could not be encoded.
    ResponseEncodingError(String),

    // ---------------------------------------------------------------------
    // Serialization / deserialization errors
    // ---------------------------------------------------------------------
    /// A value could not be serialized.
    SerializationError(String),
    /// Input could not be deserialized into its target type.
    DeserializationError(String),
    /// JSON parsing or generation failed.
    JsonError(String),
    /// Input is not valid UTF-8.
    Utf8Error(String),

    // ---------------------------------------------------------------------
    // Database / persistence errors
    // ---------------------------------------------------------------------
    /// A general persistence operation failed.
    DatabaseError(String),
    /// A database connection could not be established or retained.
    DatabaseConnectionError(String),
    /// Database-side validation rejected a value.
    DatabaseValidationError(String),
    /// A database operation exceeded its deadline.
    DatabaseTimeout(String),
    /// Concurrent database state conflicts with the operation.
    DatabaseConflict(String),
    /// A database transaction failed.
    DatabaseTransactionError(String),
    /// A database migration failed.
    DatabaseMigrationError(String),
    /// A uniqueness rule rejected a duplicate resource.
    DuplicateResource(String),
    /// A persistence constraint was violated.
    ConstraintViolation(String),

    // ---------------------------------------------------------------------
    // External dependency / infrastructure errors
    // ---------------------------------------------------------------------
    /// An external service call failed without a narrower category.
    ExternalServiceError(String),
    /// An external service is unavailable.
    ExternalServiceUnavailable(String),
    /// An external service exceeded its operation deadline.
    ExternalServiceTimeout(String),
    /// An upstream service returned an invalid response.
    BadGateway(String),
    /// An upstream service failed to respond before its deadline.
    GatewayTimeout(String),
    /// This application is temporarily unavailable.
    ServiceUnavailable(String),
    /// A required dependency is unavailable.
    DependencyUnavailable(String),

    // ---------------------------------------------------------------------
    // Configuration / dependency injection / application lifecycle errors
    // ---------------------------------------------------------------------
    /// Application configuration is invalid or unavailable.
    ConfigurationError(String),
    /// Dependency registration or resolution failed.
    DependencyInjectionError(String),
    /// Application or component initialization failed.
    InitializationError(String),
    /// Graceful shutdown failed.
    ShutdownError(String),
    /// Application state does not permit the operation.
    StateError(String),

    // ---------------------------------------------------------------------
    // IO / filesystem / network errors
    // ---------------------------------------------------------------------
    /// A general I/O operation failed.
    IoError(String),
    /// A filesystem operation failed.
    FileSystemError(String),
    /// A network operation failed.
    NetworkError(String),
    /// DNS resolution failed.
    DnsError(String),
    /// TLS setup or negotiation failed.
    TlsError(String),
    /// A general operation exceeded its deadline.
    Timeout(String),

    // ---------------------------------------------------------------------
    // Async/runtime/resource management errors
    // ---------------------------------------------------------------------
    /// An unexpected internal invariant or operation failed.
    InternalError(String),
    /// The operation cannot currently make progress.
    WouldBlock(String),
    /// A bounded resource pool has no available entry.
    PoolExhausted(String),
    /// A bounded buffer limit was exceeded.
    BufferSizeExceeded(String),
    /// Memory allocation failed.
    MemoryAllocationFailed(String),
    /// The requested operation is not supported.
    UnsupportedOperation(String),
    /// A checked pointer or offset calculation failed.
    PointerCalculationFailed(String),
    /// Joining an asynchronous task failed.
    TaskJoinError(String),
    /// A background task failed.
    BackgroundTaskError(String),
    /// An asynchronous channel operation failed.
    ChannelError(String),
    /// A synchronization lock operation failed.
    LockError(String),

    // ---------------------------------------------------------------------
    // Messaging / cache / realtime integrations
    // ---------------------------------------------------------------------
    /// A cache operation failed.
    CacheError(String),
    /// A queue operation failed.
    QueueError(String),
    /// A message broker operation failed.
    MessageBrokerError(String),
    /// A WebSocket operation failed.
    WebSocketError(String),
}

impl std::fmt::Display for HttpApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (status_code, status_text) = self.http_status();
        write!(
            f,
            "{} ({}): {}",
            self.error_code(),
            status_code,
            status_text
        )?;
        let message = self.message();
        if !message.is_empty() {
            write!(f, " - {message}")?;
        }
        Ok(())
    }
}

impl std::error::Error for HttpApiError {}

impl From<crate::application::base_service::BaseServiceError> for HttpApiError {
    fn from(error: crate::application::base_service::BaseServiceError) -> Self {
        use crate::application::base_service::BaseServiceError;

        let message = error.to_string();
        match error {
            BaseServiceError::NotFound { .. } => Self::NotFound(message),
            BaseServiceError::OperationFailed { source, .. } => match Self::from(source) {
                Self::DatabaseConnectionError(_) => Self::DatabaseConnectionError(message),
                Self::NotFound(_) => Self::NotFound(message),
                Self::DatabaseValidationError(_) => Self::DatabaseValidationError(message),
                Self::DuplicateResource(_) => Self::DuplicateResource(message),
                Self::DatabaseTransactionError(_) => Self::DatabaseTransactionError(message),
                Self::DatabaseTimeout(_) => Self::DatabaseTimeout(message),
                Self::DatabaseConflict(_) => Self::DatabaseConflict(message),
                Self::DatabaseError(_) => Self::DatabaseError(message),
                _ => Self::InternalError(message),
            },
        }
    }
}

/// Convert MongoDB errors to HTTP API errors
impl From<crate::application::mongodb::MongoDbError> for HttpApiError {
    fn from(error: crate::application::mongodb::MongoDbError) -> Self {
        use crate::application::mongodb::MongoDbError;

        match error {
            // Connection errors -> 503 Service Unavailable
            MongoDbError::ConnectionFailed(_)
            | MongoDbError::ConnectionTimeout(_)
            | MongoDbError::AuthenticationFailed(_) => {
                HttpApiError::DatabaseConnectionError(error.to_string())
            }

            // Not found errors -> 404 Not Found
            MongoDbError::DatabaseNotFound(_)
            | MongoDbError::CollectionNotFound(_)
            | MongoDbError::DocumentNotFound(_)
            | MongoDbError::NotFound(_) => HttpApiError::NotFound(error.to_string()),

            // Validation errors -> 400 Bad Request
            MongoDbError::SchemaValidationFailed(_)
            | MongoDbError::InvalidConfiguration(_)
            | MongoDbError::InvalidCollectionName(_)
            | MongoDbError::InvalidDocumentId(_)
            | MongoDbError::InvalidFilter(_)
            | MongoDbError::InvalidBatch(_)
            | MongoDbError::InvalidPage(_)
            | MongoDbError::InvalidOperationContext(_)
            | MongoDbError::SerializationError(_)
            | MongoDbError::DeserializationError(_)
            | MongoDbError::BsonError(_) => {
                HttpApiError::DatabaseValidationError(error.to_string())
            }

            // Duplicate key -> 409 Conflict
            MongoDbError::DuplicateKey(_) => {
                HttpApiError::DuplicateResource(format!("Duplicate entry: {error}"))
            }

            // Transaction errors
            MongoDbError::TransactionFailed(_)
            | MongoDbError::TransactionAborted(_)
            | MongoDbError::TransientTransaction
            | MongoDbError::UnknownTransactionCommitResult => {
                HttpApiError::DatabaseTransactionError(error.to_string())
            }

            MongoDbError::OperationCancelled | MongoDbError::OperationTimedOut => {
                HttpApiError::DatabaseTimeout(error.to_string())
            }

            MongoDbError::ConcurrencyConflict => HttpApiError::DatabaseConflict(error.to_string()),

            // All other database errors -> 500 Internal Server Error
            MongoDbError::InsertFailed(_)
            | MongoDbError::UpdateFailed(_)
            | MongoDbError::DeleteFailed(_)
            | MongoDbError::QueryFailed(_)
            | MongoDbError::IndexCreationFailed(_)
            | MongoDbError::MigrationLockUnavailable
            | MongoDbError::MigrationLockLost
            | MongoDbError::MigrationHistoryDiverged(_)
            | MongoDbError::MigrationChecksumMismatch(_)
            | MongoDbError::InternalError(_)
            | MongoDbError::Unknown(_) => HttpApiError::DatabaseError(error.to_string()),
        }
    }
}

// From implementations for error conversion
impl From<std::io::Error> for HttpApiError {
    fn from(error: std::io::Error) -> Self {
        let kind = error.kind();
        let diagnostic = error.raw_os_error().map_or_else(
            || format!("I/O operation failed: kind={kind:?}"),
            |os_code| format!("I/O operation failed: kind={kind:?}, os_code={os_code}"),
        );
        HttpApiError::IoError(diagnostic)
    }
}

impl From<crate::injection::InjectionError> for HttpApiError {
    fn from(error: crate::injection::InjectionError) -> Self {
        match error {
            crate::injection::InjectionError::ServiceNotFound(msg) => {
                HttpApiError::DependencyInjectionError(format!("Service not found: {msg}"))
            }
            crate::injection::InjectionError::ServiceResolutionFailed(msg) => {
                HttpApiError::DependencyInjectionError(format!("Service resolution failed: {msg}"))
            }
            crate::injection::InjectionError::InitError(msg) => {
                HttpApiError::InitializationError(format!("Service initialization error: {msg}"))
            }
            crate::injection::InjectionError::DisposeError(msg) => {
                HttpApiError::ShutdownError(format!("Service disposal error: {msg}"))
            }
            crate::injection::InjectionError::NewError(msg) => {
                HttpApiError::DependencyInjectionError(format!("Service creation error: {msg}"))
            }
            crate::injection::InjectionError::General(msg) => {
                HttpApiError::DependencyInjectionError(format!("Dependency injection error: {msg}"))
            }
            error @ crate::injection::InjectionError::ServiceInitializationFailed { .. } => {
                HttpApiError::InitializationError(error.to_string())
            }
            error => HttpApiError::DependencyInjectionError(error.to_string()),
        }
    }
}

/// Convert Configuration errors to HTTP API errors
impl From<crate::config::ConfigError> for HttpApiError {
    fn from(error: crate::config::ConfigError) -> Self {
        use crate::config::ConfigError;

        match error {
            // Key not found is a server configuration problem in most APIs.
            ConfigError::KeyNotFound(key) => {
                HttpApiError::ConfigurationError(format!("Configuration key not found: {key}"))
            }

            ConfigError::TypeCastError {
                key,
                expected_type,
                error,
            } => HttpApiError::ConfigurationError(format!(
                "Configuration type cast error for key '{key}': expected {expected_type}, error: {error}"
            )),
            ConfigError::ValidationError(msg) => {
                HttpApiError::ConfigurationError(format!("Configuration validation error: {msg}"))
            }
            ConfigError::ParseError(msg) => {
                HttpApiError::ConfigurationError(format!("Configuration parse error: {msg}"))
            }
            ConfigError::IoError(msg) => {
                HttpApiError::ConfigurationError(format!("Configuration IO error: {msg}"))
            }
            ConfigError::SerializationError(msg) => HttpApiError::ConfigurationError(format!(
                "Configuration serialization error: {msg}"
            )),
            ConfigError::SecretResolveError { key, error } => HttpApiError::ConfigurationError(
                format!("Failed to resolve configuration secret '{key}': {error}"),
            ),
        }
    }
}

/// Convert Request errors to HTTP API errors
impl From<crate::application::http_api::request::RequestError> for HttpApiError {
    fn from(error: crate::application::http_api::request::RequestError) -> Self {
        use crate::application::http_api::request::RequestError;

        match error {
            // JSON and UTF-8 parsing errors -> 400 Bad Request
            RequestError::InvalidJson(msg) => {
                HttpApiError::JsonError(format!("Invalid JSON: {msg}"))
            }
            RequestError::InvalidUtf8(msg) => {
                HttpApiError::Utf8Error(format!("Invalid UTF-8: {msg}"))
            }
            RequestError::InvalidContentType(msg) => {
                HttpApiError::UnsupportedMediaType(format!("Invalid content type: {msg}"))
            }

            // Missing body -> 400 Bad Request
            RequestError::NoBody(msg) => {
                HttpApiError::InvalidRequestBody(format!("Missing request body: {msg}"))
            }

            // Body too large -> 413 Payload Too Large
            RequestError::BodyTooLarge { limit_bytes } => {
                HttpApiError::PayloadTooLarge(format!("request body exceeds {limit_bytes} bytes"))
            }

            RequestError::BodyReadTimedOut => {
                HttpApiError::RequestTimeout("request body read deadline exceeded".to_string())
            }

            RequestError::BodyBufferUnavailable => {
                HttpApiError::InternalError("request body buffer is unavailable".to_string())
            }

            // Transport/reset details have already been discarded by the
            // request body adapter; keep the client-facing classification
            // stable and safe.
            RequestError::BodyReadFailed(msg) => {
                HttpApiError::InvalidRequestBody(format!("Request body read failed: {msg}"))
            }

            RequestError::Form(error) => {
                use crate::application::http_api::request::FormError;

                match error {
                    FormError::WrongContentType => HttpApiError::UnsupportedMediaType(
                        "request is not application/x-www-form-urlencoded".to_string(),
                    ),
                    _ => HttpApiError::InvalidRequestBody(error.to_string()),
                }
            }

            RequestError::Multipart(error) => {
                use crate::application::http_api::request::MultipartError;

                if error.is_payload_too_large() {
                    HttpApiError::PayloadTooLarge(error.to_string())
                } else {
                    match error {
                        MultipartError::WrongContentType => HttpApiError::UnsupportedMediaType(
                            "request is not multipart/form-data".to_string(),
                        ),
                        MultipartError::AllocationFailed | MultipartError::ParserInvariant => {
                            HttpApiError::InternalError(error.to_string())
                        }
                        _ => HttpApiError::MultipartError(error.to_string()),
                    }
                }
            }
        }
    }
}

impl HttpApiError {
    /// Get the inner message of any error variant without allocation.
    #[inline]
    pub fn message(&self) -> &str {
        match self {
            HttpApiError::BadRequest(msg)
            | HttpApiError::ValidationError(msg)
            | HttpApiError::NotFound(msg)
            | HttpApiError::MethodNotAllowed(msg)
            | HttpApiError::NotAcceptable(msg)
            | HttpApiError::Conflict(msg)
            | HttpApiError::Gone(msg)
            | HttpApiError::PreconditionFailed(msg)
            | HttpApiError::UnsupportedMediaType(msg)
            | HttpApiError::PayloadTooLarge(msg)
            | HttpApiError::UriTooLong(msg)
            | HttpApiError::RangeNotSatisfiable(msg)
            | HttpApiError::UnprocessableEntity(msg)
            | HttpApiError::RequestTimeout(msg)
            | HttpApiError::TooManyRequests(msg)
            | HttpApiError::Unauthorized(msg)
            | HttpApiError::Forbidden(msg)
            | HttpApiError::InvalidCredentials(msg)
            | HttpApiError::InvalidToken(msg)
            | HttpApiError::TokenExpired(msg)
            | HttpApiError::MissingAuthentication(msg)
            | HttpApiError::InsufficientPermissions(msg)
            | HttpApiError::CsrfError(msg)
            | HttpApiError::CorsError(msg)
            | HttpApiError::HttpParseError(msg)
            | HttpApiError::InvalidHttpHeader(msg)
            | HttpApiError::InvalidHttpMethod(msg)
            | HttpApiError::InvalidHttpVersion(msg)
            | HttpApiError::InvalidContentType(msg)
            | HttpApiError::InvalidRequestBody(msg)
            | HttpApiError::InvalidQueryString(msg)
            | HttpApiError::InvalidPathParameter(msg)
            | HttpApiError::MultipartError(msg)
            | HttpApiError::RouteNotFound(msg)
            | HttpApiError::ControllerError(msg)
            | HttpApiError::HandlerError(msg)
            | HttpApiError::MiddlewareError(msg)
            | HttpApiError::ResponseEncodingError(msg)
            | HttpApiError::SerializationError(msg)
            | HttpApiError::DeserializationError(msg)
            | HttpApiError::JsonError(msg)
            | HttpApiError::Utf8Error(msg)
            | HttpApiError::DatabaseError(msg)
            | HttpApiError::DatabaseConnectionError(msg)
            | HttpApiError::DatabaseValidationError(msg)
            | HttpApiError::DatabaseTimeout(msg)
            | HttpApiError::DatabaseConflict(msg)
            | HttpApiError::DatabaseTransactionError(msg)
            | HttpApiError::DatabaseMigrationError(msg)
            | HttpApiError::DuplicateResource(msg)
            | HttpApiError::ConstraintViolation(msg)
            | HttpApiError::ExternalServiceError(msg)
            | HttpApiError::ExternalServiceUnavailable(msg)
            | HttpApiError::ExternalServiceTimeout(msg)
            | HttpApiError::BadGateway(msg)
            | HttpApiError::GatewayTimeout(msg)
            | HttpApiError::ServiceUnavailable(msg)
            | HttpApiError::DependencyUnavailable(msg)
            | HttpApiError::ConfigurationError(msg)
            | HttpApiError::DependencyInjectionError(msg)
            | HttpApiError::InitializationError(msg)
            | HttpApiError::ShutdownError(msg)
            | HttpApiError::StateError(msg)
            | HttpApiError::IoError(msg)
            | HttpApiError::FileSystemError(msg)
            | HttpApiError::NetworkError(msg)
            | HttpApiError::DnsError(msg)
            | HttpApiError::TlsError(msg)
            | HttpApiError::Timeout(msg)
            | HttpApiError::InternalError(msg)
            | HttpApiError::WouldBlock(msg)
            | HttpApiError::PoolExhausted(msg)
            | HttpApiError::BufferSizeExceeded(msg)
            | HttpApiError::MemoryAllocationFailed(msg)
            | HttpApiError::UnsupportedOperation(msg)
            | HttpApiError::PointerCalculationFailed(msg)
            | HttpApiError::TaskJoinError(msg)
            | HttpApiError::BackgroundTaskError(msg)
            | HttpApiError::ChannelError(msg)
            | HttpApiError::LockError(msg)
            | HttpApiError::CacheError(msg)
            | HttpApiError::QueueError(msg)
            | HttpApiError::MessageBrokerError(msg)
            | HttpApiError::WebSocketError(msg) => msg,
        }
    }

    /// Stable machine-readable error code for response bodies, logs and frontend mapping.
    #[inline]
    pub fn error_code(&self) -> &'static str {
        match self {
            HttpApiError::BadRequest(_) => "BAD_REQUEST",
            HttpApiError::ValidationError(_) => "VALIDATION_ERROR",
            HttpApiError::NotFound(_) => "NOT_FOUND",
            HttpApiError::MethodNotAllowed(_) => "METHOD_NOT_ALLOWED",
            HttpApiError::NotAcceptable(_) => "NOT_ACCEPTABLE",
            HttpApiError::Conflict(_) => "CONFLICT",
            HttpApiError::Gone(_) => "GONE",
            HttpApiError::PreconditionFailed(_) => "PRECONDITION_FAILED",
            HttpApiError::UnsupportedMediaType(_) => "UNSUPPORTED_MEDIA_TYPE",
            HttpApiError::PayloadTooLarge(_) => "PAYLOAD_TOO_LARGE",
            HttpApiError::UriTooLong(_) => "URI_TOO_LONG",
            HttpApiError::RangeNotSatisfiable(_) => "RANGE_NOT_SATISFIABLE",
            HttpApiError::UnprocessableEntity(_) => "UNPROCESSABLE_ENTITY",
            HttpApiError::RequestTimeout(_) => "REQUEST_TIMEOUT",
            HttpApiError::TooManyRequests(_) => "TOO_MANY_REQUESTS",
            HttpApiError::Unauthorized(_) => "UNAUTHORIZED",
            HttpApiError::Forbidden(_) => "FORBIDDEN",
            HttpApiError::InvalidCredentials(_) => "INVALID_CREDENTIALS",
            HttpApiError::InvalidToken(_) => "INVALID_TOKEN",
            HttpApiError::TokenExpired(_) => "TOKEN_EXPIRED",
            HttpApiError::MissingAuthentication(_) => "MISSING_AUTHENTICATION",
            HttpApiError::InsufficientPermissions(_) => "INSUFFICIENT_PERMISSIONS",
            HttpApiError::CsrfError(_) => "CSRF_ERROR",
            HttpApiError::CorsError(_) => "CORS_ERROR",
            HttpApiError::HttpParseError(_) => "HTTP_PARSE_ERROR",
            HttpApiError::InvalidHttpHeader(_) => "INVALID_HTTP_HEADER",
            HttpApiError::InvalidHttpMethod(_) => "INVALID_HTTP_METHOD",
            HttpApiError::InvalidHttpVersion(_) => "INVALID_HTTP_VERSION",
            HttpApiError::InvalidContentType(_) => "INVALID_CONTENT_TYPE",
            HttpApiError::InvalidRequestBody(_) => "INVALID_REQUEST_BODY",
            HttpApiError::InvalidQueryString(_) => "INVALID_QUERY_STRING",
            HttpApiError::InvalidPathParameter(_) => "INVALID_PATH_PARAMETER",
            HttpApiError::MultipartError(_) => "MULTIPART_ERROR",
            HttpApiError::RouteNotFound(_) => "ROUTE_NOT_FOUND",
            HttpApiError::ControllerError(_) => "CONTROLLER_ERROR",
            HttpApiError::HandlerError(_) => "HANDLER_ERROR",
            HttpApiError::MiddlewareError(_) => "MIDDLEWARE_ERROR",
            HttpApiError::ResponseEncodingError(_) => "RESPONSE_ENCODING_ERROR",
            HttpApiError::SerializationError(_) => "SERIALIZATION_ERROR",
            HttpApiError::DeserializationError(_) => "DESERIALIZATION_ERROR",
            HttpApiError::JsonError(_) => "JSON_ERROR",
            HttpApiError::Utf8Error(_) => "UTF8_ERROR",
            HttpApiError::DatabaseError(_) => "DATABASE_ERROR",
            HttpApiError::DatabaseConnectionError(_) => "DATABASE_CONNECTION_ERROR",
            HttpApiError::DatabaseValidationError(_) => "DATABASE_VALIDATION_ERROR",
            HttpApiError::DatabaseTimeout(_) => "DATABASE_TIMEOUT",
            HttpApiError::DatabaseConflict(_) => "DATABASE_CONFLICT",
            HttpApiError::DatabaseTransactionError(_) => "DATABASE_TRANSACTION_ERROR",
            HttpApiError::DatabaseMigrationError(_) => "DATABASE_MIGRATION_ERROR",
            HttpApiError::DuplicateResource(_) => "DUPLICATE_RESOURCE",
            HttpApiError::ConstraintViolation(_) => "CONSTRAINT_VIOLATION",
            HttpApiError::ExternalServiceError(_) => "EXTERNAL_SERVICE_ERROR",
            HttpApiError::ExternalServiceUnavailable(_) => "EXTERNAL_SERVICE_UNAVAILABLE",
            HttpApiError::ExternalServiceTimeout(_) => "EXTERNAL_SERVICE_TIMEOUT",
            HttpApiError::BadGateway(_) => "BAD_GATEWAY",
            HttpApiError::GatewayTimeout(_) => "GATEWAY_TIMEOUT",
            HttpApiError::ServiceUnavailable(_) => "SERVICE_UNAVAILABLE",
            HttpApiError::DependencyUnavailable(_) => "DEPENDENCY_UNAVAILABLE",
            HttpApiError::ConfigurationError(_) => "CONFIGURATION_ERROR",
            HttpApiError::DependencyInjectionError(_) => "DEPENDENCY_INJECTION_ERROR",
            HttpApiError::InitializationError(_) => "INITIALIZATION_ERROR",
            HttpApiError::ShutdownError(_) => "SHUTDOWN_ERROR",
            HttpApiError::StateError(_) => "STATE_ERROR",
            HttpApiError::IoError(_) => "IO_ERROR",
            HttpApiError::FileSystemError(_) => "FILE_SYSTEM_ERROR",
            HttpApiError::NetworkError(_) => "NETWORK_ERROR",
            HttpApiError::DnsError(_) => "DNS_ERROR",
            HttpApiError::TlsError(_) => "TLS_ERROR",
            HttpApiError::Timeout(_) => "TIMEOUT",
            HttpApiError::InternalError(_) => "INTERNAL_ERROR",
            HttpApiError::WouldBlock(_) => "WOULD_BLOCK",
            HttpApiError::PoolExhausted(_) => "POOL_EXHAUSTED",
            HttpApiError::BufferSizeExceeded(_) => "BUFFER_SIZE_EXCEEDED",
            HttpApiError::MemoryAllocationFailed(_) => "MEMORY_ALLOCATION_FAILED",
            HttpApiError::UnsupportedOperation(_) => "UNSUPPORTED_OPERATION",
            HttpApiError::PointerCalculationFailed(_) => "POINTER_CALCULATION_FAILED",
            HttpApiError::TaskJoinError(_) => "TASK_JOIN_ERROR",
            HttpApiError::BackgroundTaskError(_) => "BACKGROUND_TASK_ERROR",
            HttpApiError::ChannelError(_) => "CHANNEL_ERROR",
            HttpApiError::LockError(_) => "LOCK_ERROR",
            HttpApiError::CacheError(_) => "CACHE_ERROR",
            HttpApiError::QueueError(_) => "QUEUE_ERROR",
            HttpApiError::MessageBrokerError(_) => "MESSAGE_BROKER_ERROR",
            HttpApiError::WebSocketError(_) => "WEBSOCKET_ERROR",
        }
    }

    /// Get HTTP status code and status text for this error.
    /// This centralizes status mapping to avoid duplication between response writers and encoders.
    pub fn http_status(&self) -> (u16, &'static str) {
        match self {
            // 400 Bad Request
            HttpApiError::BadRequest(_)
            | HttpApiError::HttpParseError(_)
            | HttpApiError::InvalidHttpHeader(_)
            | HttpApiError::InvalidHttpMethod(_)
            | HttpApiError::InvalidHttpVersion(_)
            | HttpApiError::InvalidContentType(_)
            | HttpApiError::InvalidRequestBody(_)
            | HttpApiError::InvalidQueryString(_)
            | HttpApiError::InvalidPathParameter(_)
            | HttpApiError::MultipartError(_)
            | HttpApiError::JsonError(_)
            | HttpApiError::Utf8Error(_)
            | HttpApiError::DatabaseValidationError(_) => (400, "Bad Request"),

            // 401 Unauthorized
            HttpApiError::Unauthorized(_)
            | HttpApiError::InvalidCredentials(_)
            | HttpApiError::InvalidToken(_)
            | HttpApiError::TokenExpired(_)
            | HttpApiError::MissingAuthentication(_) => (401, "Unauthorized"),

            // 403 Forbidden
            HttpApiError::Forbidden(_)
            | HttpApiError::InsufficientPermissions(_)
            | HttpApiError::CsrfError(_)
            | HttpApiError::CorsError(_) => (403, "Forbidden"),

            HttpApiError::NotFound(_) | HttpApiError::RouteNotFound(_) => (404, "Not Found"),
            HttpApiError::MethodNotAllowed(_) => (405, "Method Not Allowed"),
            HttpApiError::NotAcceptable(_) => (406, "Not Acceptable"),
            HttpApiError::RequestTimeout(_) => (408, "Request Timeout"),
            HttpApiError::Conflict(_)
            | HttpApiError::DatabaseConflict(_)
            | HttpApiError::DuplicateResource(_)
            | HttpApiError::ConstraintViolation(_) => (409, "Conflict"),
            HttpApiError::Gone(_) => (410, "Gone"),
            HttpApiError::PreconditionFailed(_) => (412, "Precondition Failed"),
            HttpApiError::PayloadTooLarge(_) | HttpApiError::BufferSizeExceeded(_) => {
                (413, "Payload Too Large")
            }
            HttpApiError::UriTooLong(_) => (414, "URI Too Long"),
            HttpApiError::UnsupportedMediaType(_) => (415, "Unsupported Media Type"),
            HttpApiError::RangeNotSatisfiable(_) => (416, "Range Not Satisfiable"),
            HttpApiError::ValidationError(_) | HttpApiError::UnprocessableEntity(_) => {
                (422, "Unprocessable Entity")
            }
            HttpApiError::TooManyRequests(_) => (429, "Too Many Requests"),

            // 500 Internal Server Error
            HttpApiError::InternalError(_)
            | HttpApiError::ControllerError(_)
            | HttpApiError::HandlerError(_)
            | HttpApiError::MiddlewareError(_)
            | HttpApiError::ResponseEncodingError(_)
            | HttpApiError::SerializationError(_)
            | HttpApiError::DeserializationError(_)
            | HttpApiError::DatabaseError(_)
            | HttpApiError::DatabaseTransactionError(_)
            | HttpApiError::DatabaseMigrationError(_)
            | HttpApiError::ConfigurationError(_)
            | HttpApiError::DependencyInjectionError(_)
            | HttpApiError::InitializationError(_)
            | HttpApiError::ShutdownError(_)
            | HttpApiError::StateError(_)
            | HttpApiError::IoError(_)
            | HttpApiError::FileSystemError(_)
            | HttpApiError::NetworkError(_)
            | HttpApiError::DnsError(_)
            | HttpApiError::TlsError(_)
            | HttpApiError::Timeout(_)
            | HttpApiError::WouldBlock(_)
            | HttpApiError::MemoryAllocationFailed(_)
            | HttpApiError::PointerCalculationFailed(_)
            | HttpApiError::TaskJoinError(_)
            | HttpApiError::BackgroundTaskError(_)
            | HttpApiError::ChannelError(_)
            | HttpApiError::LockError(_)
            | HttpApiError::CacheError(_)
            | HttpApiError::QueueError(_)
            | HttpApiError::MessageBrokerError(_)
            | HttpApiError::WebSocketError(_) => (500, "Internal Server Error"),

            HttpApiError::UnsupportedOperation(_) => (501, "Not Implemented"),
            HttpApiError::BadGateway(_) | HttpApiError::ExternalServiceError(_) => {
                (502, "Bad Gateway")
            }
            HttpApiError::PoolExhausted(_)
            | HttpApiError::DatabaseConnectionError(_)
            | HttpApiError::ExternalServiceUnavailable(_)
            | HttpApiError::ServiceUnavailable(_)
            | HttpApiError::DependencyUnavailable(_) => (503, "Service Unavailable"),
            HttpApiError::DatabaseTimeout(_)
            | HttpApiError::ExternalServiceTimeout(_)
            | HttpApiError::GatewayTimeout(_) => (504, "Gateway Timeout"),
        }
    }

    /// A deliberately generic message that is safe to expose outside the
    /// process trust boundary.
    ///
    /// The owned `String` carried by the enum remains the internal diagnostic
    /// cause. It can contain SQL, file paths, dependency messages or secrets
    /// and therefore must not be copied into an HTTP body.
    pub fn public_message(&self) -> &'static str {
        match self.http_status().0 {
            400 => "The request is invalid.",
            401 => "Authentication is required.",
            403 => "You are not allowed to perform this operation.",
            404 => "The requested resource was not found.",
            405 => "The HTTP method is not allowed for this resource.",
            406 => "The requested representation is not available.",
            408 => "The request timed out.",
            409 => "The request conflicts with the current resource state.",
            410 => "The requested resource is no longer available.",
            412 => "A request precondition failed.",
            413 => "The request payload is too large.",
            414 => "The request URI is too long.",
            415 => "The request media type is not supported.",
            416 => "The requested range cannot be served.",
            422 => "The request could not be processed.",
            429 => "Too many requests.",
            501 => "This operation is not supported.",
            502 => "An upstream service returned an invalid response.",
            503 => "The service is temporarily unavailable.",
            504 => "An upstream service timed out.",
            _ => "An internal server error occurred.",
        }
    }

    /// Builds the public error representation without cloning the internal
    /// diagnostic cause.
    pub fn public_body(&self) -> PublicHttpErrorBody {
        PublicHttpErrorBody {
            error: true,
            code: self.error_code(),
            message: self.public_message(),
            status: self.http_status().0,
        }
    }

    /// Serializes the public envelope with `serde_json`, including complete
    /// control-character and Unicode escaping.
    pub fn to_public_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.public_body())
    }
}

#[cfg(test)]
mod public_error_tests {
    use super::*;
    use crate::application::http_api::request::{
        FormComponent, FormError, MultipartError, RequestError,
    };

    #[test]
    fn internal_diagnostics_never_reach_public_json() {
        let error = HttpApiError::DatabaseError(
            "postgres://admin:secret@db/private path=/srv/app query=SELECT *".to_string(),
        );

        let encoded = error.to_public_json().unwrap();
        let text = String::from_utf8(encoded).unwrap();

        assert!(!text.contains("secret"));
        assert!(!text.contains("/srv/app"));
        assert!(!text.contains("SELECT"));
        assert!(text.contains("DATABASE_ERROR"));
        assert!(text.contains("internal server error"));
    }

    #[test]
    fn public_envelope_is_valid_json_for_hostile_internal_text() {
        let error = HttpApiError::BadRequest("\"\n\t\u{0000}İ😀".to_string());
        let encoded = error.to_public_json().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(value["status"], 400);
        assert_eq!(value["code"], "BAD_REQUEST");
        assert_eq!(value["message"], "The request is invalid.");
    }

    #[test]
    fn io_errors_are_generic_500s_with_secret_safe_diagnostics() {
        let sensitive = "permission denied path=/srv/private/key.pem";
        let error = HttpApiError::from(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            sensitive,
        ));

        assert!(matches!(error, HttpApiError::IoError(_)));
        assert_eq!(error.http_status().0, 500);
        assert_eq!(error.error_code(), "IO_ERROR");
        assert!(error.message().contains("PermissionDenied"));
        assert!(!error.message().contains(sensitive));

        let public = String::from_utf8(error.to_public_json().unwrap()).unwrap();
        assert!(public.contains("An internal server error occurred."));
        assert!(!public.contains("/srv/private"));
        assert!(!public.contains("permission denied"));
    }

    #[test]
    fn multipart_failures_have_stable_http_categories() {
        for (multipart, expected_status, expected_code) in [
            (MultipartError::MalformedBody, 400, "MULTIPART_ERROR"),
            (
                MultipartError::WholeStreamTooLarge { limit_bytes: 1024 },
                413,
                "PAYLOAD_TOO_LARGE",
            ),
            (
                MultipartError::WrongContentType,
                415,
                "UNSUPPORTED_MEDIA_TYPE",
            ),
            (MultipartError::ParserInvariant, 500, "INTERNAL_ERROR"),
        ] {
            let error = HttpApiError::from(RequestError::Multipart(multipart));
            assert_eq!(error.http_status().0, expected_status);
            assert_eq!(error.error_code(), expected_code);
            assert!(!error.public_body().message.is_empty());
        }
    }

    #[test]
    fn form_failures_have_stable_http_categories_without_input_values() {
        for (form, expected_status, expected_code) in [
            (FormError::WrongContentType, 415, "UNSUPPORTED_MEDIA_TYPE"),
            (FormError::DuplicateContentType, 400, "INVALID_REQUEST_BODY"),
            (
                FormError::InvalidPercentEncoding {
                    pair_index: 1,
                    component: FormComponent::Value,
                    byte_offset: 2,
                },
                400,
                "INVALID_REQUEST_BODY",
            ),
        ] {
            let error = HttpApiError::from(RequestError::Form(form));
            assert_eq!(error.http_status().0, expected_status);
            assert_eq!(error.error_code(), expected_code);
        }
    }

    #[test]
    fn request_body_timeout_and_buffer_failures_keep_their_http_semantics() {
        for (request, expected_status, expected_code) in [
            (
                RequestError::BodyTooLarge { limit_bytes: 1024 },
                413,
                "PAYLOAD_TOO_LARGE",
            ),
            (RequestError::BodyReadTimedOut, 408, "REQUEST_TIMEOUT"),
            (RequestError::BodyBufferUnavailable, 500, "INTERNAL_ERROR"),
            (
                RequestError::BodyReadFailed("request body transport was interrupted".to_string()),
                400,
                "INVALID_REQUEST_BODY",
            ),
        ] {
            let error = HttpApiError::from(request);
            assert_eq!(error.http_status().0, expected_status);
            assert_eq!(error.error_code(), expected_code);
        }
    }

    #[test]
    fn mongodb_transaction_labels_keep_the_storage_failure_category() {
        use crate::application::mongodb::MongoDbError;

        for source in [
            MongoDbError::TransientTransaction,
            MongoDbError::UnknownTransactionCommitResult,
        ] {
            let error = HttpApiError::from(source);
            assert!(matches!(&error, HttpApiError::DatabaseTransactionError(_)));
            assert_eq!(error.http_status().0, 500);
            assert_eq!(error.error_code(), "DATABASE_TRANSACTION_ERROR");
        }
    }
}
