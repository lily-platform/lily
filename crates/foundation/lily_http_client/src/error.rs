use std::fmt;

/// Main error type for the HTTP client
#[derive(Clone)]
pub enum HttpClientError {
    /// Network connection errors
    Connection(String),

    /// DNS, TCP, or TLS connection establishment exceeded its deadline.
    ConnectTimeout,

    /// TLS/SSL errors
    Tls(String),

    /// URL parsing errors
    InvalidUrl(String),

    /// HTTP parsing errors
    HttpParsing(String),

    /// Request building errors
    RequestBuilding(String),

    /// Response parsing errors
    ResponseParsing(String),

    /// JSON serialization/deserialization errors
    Json(String),

    /// Timeout errors
    Timeout,

    /// Invalid header errors
    InvalidHeader(String),

    /// Invalid method errors
    InvalidMethod(String),

    /// Body reading errors
    BodyReading(String),

    /// Body processing errors
    Body(String),

    /// Multipart body construction failed before any request was sent.
    InvalidMultipart(MultipartBuildError),

    /// Generic client errors
    Client(String),

    /// Server errors (5xx status codes)
    Server { status: u16, message: String },

    /// Client errors (4xx status codes)  
    ClientStatus { status: u16, message: String },

    /// IO errors
    Io(String),

    /// Parse errors
    Parse(String),

    /// Configuration errors
    Configuration(String),

    /// A configured request or response resource limit was exceeded.
    LimitExceeded { resource: String, limit: usize },

    /// Every retained origin slot is currently occupied by active work.
    OriginCapacityExceeded { limit: usize },

    /// A legacy option was requested which the production transport cannot
    /// safely or truthfully provide.
    UnsupportedConfiguration(String),

    /// The wire protocol selected by the transport does not satisfy the
    /// request's explicit protocol policy.
    ProtocolNegotiation {
        requested: String,
        negotiated: String,
    },

    /// Redirect was syntactically valid but crossed a forbidden trust
    /// boundary (for example HTTPS to plaintext HTTP).
    RedirectRejected(String),
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, HttpClientError>;

/// Identifies untrusted multipart metadata without retaining its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultipartMetadataField {
    Name,
    Filename,
    ContentType,
}

/// Stable, secret-free multipart construction failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MultipartBuildError {
    InvalidBoundary,
    EmptyFieldName,
    MetadataTooLong {
        field: MultipartMetadataField,
        limit_bytes: usize,
    },
    ControlCharacter {
        field: MultipartMetadataField,
    },
    InvalidContentType,
    LengthOverflow,
    AllocationFailed,
}

impl MultipartBuildError {
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::InvalidBoundary => "MULTIPART_INVALID_BOUNDARY",
            Self::EmptyFieldName => "MULTIPART_EMPTY_FIELD_NAME",
            Self::MetadataTooLong { .. } => "MULTIPART_METADATA_TOO_LONG",
            Self::ControlCharacter { .. } => "MULTIPART_METADATA_CONTROL_CHARACTER",
            Self::InvalidContentType => "MULTIPART_INVALID_CONTENT_TYPE",
            Self::LengthOverflow => "MULTIPART_LENGTH_OVERFLOW",
            Self::AllocationFailed => "MULTIPART_ALLOCATION_FAILED",
        }
    }
}

impl fmt::Display for MultipartBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidBoundary => "multipart boundary is invalid",
            Self::EmptyFieldName => "multipart field name is empty",
            Self::MetadataTooLong { .. } => "multipart metadata exceeds its byte limit",
            Self::ControlCharacter { .. } => "multipart metadata contains a control character",
            Self::InvalidContentType => "multipart content type is invalid",
            Self::LengthOverflow => "multipart encoded length overflowed",
            Self::AllocationFailed => "multipart body allocation failed",
        })
    }
}

impl std::error::Error for MultipartBuildError {}

impl From<MultipartBuildError> for HttpClientError {
    fn from(error: MultipartBuildError) -> Self {
        Self::InvalidMultipart(error)
    }
}

const HOP_BY_HOP_REQUEST_HEADER_MESSAGE: &str =
    "request headers cannot set hop-by-hop or proxy-only fields";
const TRANSPORT_OWNED_REQUEST_HEADER_MESSAGE: &str =
    "request headers cannot set transport-owned framing fields";

impl fmt::Debug for HttpClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut output = f.debug_struct("HttpClientError");
        output.field("code", &self.error_code());
        output.field("diagnostic_code", &self.diagnostic_code());
        match self {
            Self::Server { status, .. } | Self::ClientStatus { status, .. } => {
                output.field("status", status);
            }
            Self::LimitExceeded { limit, .. } | Self::OriginCapacityExceeded { limit } => {
                output.field("limit", limit);
            }
            _ => {}
        }
        output.finish_non_exhaustive()
    }
}

impl fmt::Display for HttpClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Server { status, .. } | Self::ClientStatus { status, .. } => write!(
                f,
                "HTTP client error (category={}, diagnostic={}, status={status})",
                self.error_code(),
                self.diagnostic_code()
            ),
            Self::LimitExceeded { limit, .. } | Self::OriginCapacityExceeded { limit } => write!(
                f,
                "HTTP client error (category={}, diagnostic={}, limit={limit})",
                self.error_code(),
                self.diagnostic_code()
            ),
            _ => write!(
                f,
                "HTTP client error (category={}, diagnostic={})",
                self.error_code(),
                self.diagnostic_code()
            ),
        }
    }
}

impl std::error::Error for HttpClientError {}

impl HttpClientError {
    /// Stable, secret-free terminal category used by traces and metrics.
    pub const fn error_code(&self) -> &'static str {
        match self {
            Self::Connection(_) => "CONNECTION_ERROR",
            Self::ConnectTimeout => "CONNECT_TIMEOUT",
            Self::Tls(_) => "TLS_ERROR",
            Self::InvalidUrl(_) => "INVALID_URL",
            Self::HttpParsing(_) => "HTTP_PARSE_ERROR",
            Self::RequestBuilding(_) => "REQUEST_BUILD_ERROR",
            Self::ResponseParsing(_) => "RESPONSE_PARSE_ERROR",
            Self::Json(_) => "JSON_ERROR",
            Self::Timeout => "REQUEST_TIMEOUT",
            Self::InvalidHeader(_) => "INVALID_HEADER",
            Self::InvalidMethod(_) => "INVALID_METHOD",
            Self::BodyReading(_) => "BODY_READ_ERROR",
            Self::Body(_) => "BODY_ERROR",
            Self::InvalidMultipart(_) => "MULTIPART_BUILD_ERROR",
            Self::Client(_) => "CLIENT_STATE_ERROR",
            Self::Server { .. } => "SERVER_STATUS_ERROR",
            Self::ClientStatus { .. } => "CLIENT_STATUS_ERROR",
            Self::Io(_) => "IO_ERROR",
            Self::Parse(_) => "PARSE_ERROR",
            Self::Configuration(_) => "CONFIGURATION_ERROR",
            Self::LimitExceeded { .. } => "LIMIT_EXCEEDED",
            Self::OriginCapacityExceeded { .. } => "ORIGIN_CAPACITY_EXCEEDED",
            Self::UnsupportedConfiguration(_) => "UNSUPPORTED_CONFIGURATION",
            Self::ProtocolNegotiation { .. } => "PROTOCOL_NEGOTIATION_ERROR",
            Self::RedirectRejected(_) => "REDIRECT_REJECTED",
        }
    }

    /// Stable, bounded diagnostic detail for logs and startup errors.
    ///
    /// Internally produced messages are mapped through an allow-list. Publicly
    /// constructed or otherwise unknown string payloads always collapse to
    /// `UNSPECIFIED`; the raw payload remains available only through explicit
    /// enum destructuring.
    pub fn diagnostic_code(&self) -> &'static str {
        match self {
            Self::Configuration(message) => configuration_diagnostic_code(message),
            Self::LimitExceeded { resource, .. } => limit_diagnostic_code(resource),
            Self::OriginCapacityExceeded { .. } => "ORIGIN_CAPACITY_ACTIVE",
            Self::Tls(message) => tls_diagnostic_code(message),
            Self::InvalidHeader(message) | Self::HttpParsing(message) => {
                header_diagnostic_code(message)
            }
            Self::RequestBuilding(message) => request_building_diagnostic_code(message),
            Self::InvalidMultipart(error) => error.diagnostic_code(),
            Self::InvalidUrl(message) => invalid_url_diagnostic_code(message),
            Self::InvalidMethod(message) => invalid_method_diagnostic_code(message),
            Self::UnsupportedConfiguration(message) => {
                unsupported_configuration_diagnostic_code(message)
            }
            Self::RedirectRejected(message) => redirect_diagnostic_code(message),
            Self::ProtocolNegotiation { .. } => "PROTOCOL_POLICY_MISMATCH",
            Self::ConnectTimeout => "CONNECT_DEADLINE",
            _ => "UNSPECIFIED",
        }
    }

    pub(crate) fn hop_by_hop_request_header() -> Self {
        Self::InvalidHeader(HOP_BY_HOP_REQUEST_HEADER_MESSAGE.to_string())
    }

    pub(crate) fn transport_owned_request_header() -> Self {
        Self::InvalidHeader(TRANSPORT_OWNED_REQUEST_HEADER_MESSAGE.to_string())
    }

    /// Create a new TLS error
    pub fn tls(msg: impl Into<String>) -> Self {
        HttpClientError::Tls(msg.into())
    }

    /// Create a new HTTP parsing error
    pub fn http_parsing(msg: impl Into<String>) -> Self {
        HttpClientError::HttpParsing(msg.into())
    }

    /// Create a new request building error
    pub fn request_building(msg: impl Into<String>) -> Self {
        HttpClientError::RequestBuilding(msg.into())
    }

    /// Create a new response parsing error
    pub fn response_parsing(msg: impl Into<String>) -> Self {
        Self::ResponseParsing(msg.into())
    }

    /// Create a body error
    pub fn body(msg: impl Into<String>) -> Self {
        Self::Body(msg.into())
    }

    /// Create a new invalid header error
    pub fn invalid_header(msg: impl Into<String>) -> Self {
        HttpClientError::InvalidHeader(msg.into())
    }

    /// Create a new invalid method error
    pub fn invalid_method(msg: impl Into<String>) -> Self {
        HttpClientError::InvalidMethod(msg.into())
    }

    /// Create a new body reading error
    pub fn body_reading(msg: impl Into<String>) -> Self {
        HttpClientError::BodyReading(msg.into())
    }

    /// Create a new client error
    pub fn client(msg: impl Into<String>) -> Self {
        HttpClientError::Client(msg.into())
    }

    /// Create a new server error
    pub fn server(status: u16, message: impl Into<String>) -> Self {
        HttpClientError::Server {
            status,
            message: message.into(),
        }
    }

    /// Create a new client status error
    pub fn client_status(status: u16, message: impl Into<String>) -> Self {
        HttpClientError::ClientStatus {
            status,
            message: message.into(),
        }
    }

    /// Check if error is a network/connection error
    pub fn is_network_error(&self) -> bool {
        matches!(
            self,
            HttpClientError::Connection(_)
                | HttpClientError::ConnectTimeout
                | HttpClientError::Tls(_)
        )
    }

    /// Check if error is a timeout error
    pub fn is_timeout(&self) -> bool {
        matches!(
            self,
            HttpClientError::Timeout | HttpClientError::ConnectTimeout
        )
    }

    /// Check if error is a client error (4xx)
    pub fn is_client_error(&self) -> bool {
        matches!(self, HttpClientError::ClientStatus { .. })
    }

    /// Check if error is a server error (5xx)
    pub fn is_server_error(&self) -> bool {
        matches!(self, HttpClientError::Server { .. })
    }

    /// Get the status code if this is an HTTP status error
    pub fn status_code(&self) -> Option<u16> {
        match self {
            HttpClientError::Server { status, .. } => Some(*status),
            HttpClientError::ClientStatus { status, .. } => Some(*status),
            _ => None,
        }
    }
}

fn configuration_diagnostic_code(message: &str) -> &'static str {
    if message == "Factory not initialized. Call initialize() first." {
        "CONFIG_FACTORY_NOT_INITIALIZED"
    } else if message.starts_with("HTTP client configuration not found for prefix:") {
        "CONFIG_FACTORY_CLIENT_NOT_FOUND"
    } else if message == "default headers cannot set transport-owned framing or routing headers" {
        "CONFIG_DEFAULT_HEADER_TRANSPORT_OWNED"
    } else if message == "base_address must be an absolute URL" {
        "CONFIG_BASE_ADDRESS_ABSOLUTE"
    } else if message == "base_address must use http or https and contain a host" {
        "CONFIG_BASE_ADDRESS_SCHEME_OR_HOST"
    } else if message == "base_address must not contain URL credentials" {
        "CONFIG_BASE_ADDRESS_CREDENTIALS"
    } else if message == "base_address must not contain a query or fragment" {
        "CONFIG_BASE_ADDRESS_QUERY_OR_FRAGMENT"
    } else if message == "base_address cannot be used as a hierarchical URL" {
        "CONFIG_BASE_ADDRESS_NOT_HIERARCHICAL"
    } else if message == "client and pool timeouts must be greater than zero" {
        "CONFIG_CLIENT_OR_POOL_TIMEOUT_ZERO"
    } else if message.starts_with("connect_timeout must be <=")
        || message.starts_with("connect timeout must be in")
        || message == "connect timeout must be greater than zero"
    {
        "CONFIG_CONNECT_TIMEOUT_RANGE"
    } else if message.starts_with("request_timeout must be <=")
        || message.starts_with("request timeout must be in")
    {
        "CONFIG_REQUEST_TIMEOUT_RANGE"
    } else if message.starts_with("pool_idle_timeout must be <=") {
        "CONFIG_POOL_IDLE_TIMEOUT_RANGE"
    } else if message == "max_redirects must be <= 20" {
        "CONFIG_MAX_REDIRECTS_RANGE"
    } else if message == "max_in_flight_requests must be in 1..=1_000_000" {
        "CONFIG_MAX_IN_FLIGHT_RANGE"
    } else if message == "max_in_flight_requests_per_origin must be in 1..=max_in_flight_requests" {
        "CONFIG_MAX_IN_FLIGHT_PER_ORIGIN_RANGE"
    } else if message.starts_with("max_request_body_bytes must be in") {
        "CONFIG_MAX_REQUEST_BODY_BYTES_RANGE"
    } else if message.starts_with("max_response_body_bytes must be in") {
        "CONFIG_MAX_RESPONSE_BODY_BYTES_RANGE"
    } else if message.starts_with("max_header_bytes must be in") {
        "CONFIG_MAX_HEADER_BYTES_RANGE"
    } else if message == "max_header_count must be in 1..=1024" {
        "CONFIG_MAX_HEADER_COUNT_RANGE"
    } else if message == "default_headers contains an invalid HTTP header name" {
        "CONFIG_DEFAULT_HEADER_NAME_INVALID"
    } else if message == "default_headers contains an invalid HTTP header value" {
        "CONFIG_DEFAULT_HEADER_VALUE_INVALID"
    } else if message == "pool_max_idle_per_host must be in 1..=10_000" {
        "CONFIG_POOL_MAX_IDLE_PER_HOST_RANGE"
    } else if message == "max_retained_origins must be in 1..=1024" {
        "CONFIG_MAX_RETAINED_ORIGINS_RANGE"
    } else if message == "http2_max_frame_bytes must be in 16384..=16777215" {
        "CONFIG_HTTP2_MAX_FRAME_BYTES_RANGE"
    } else if message.starts_with("http2_initial_stream_window_bytes must be in") {
        "CONFIG_HTTP2_STREAM_WINDOW_RANGE"
    } else if message.starts_with("http2_initial_connection_window_bytes must be in") {
        "CONFIG_HTTP2_CONNECTION_WINDOW_RANGE"
    } else if message == "HTTP/2 keep-alive durations must be greater than zero" {
        "CONFIG_HTTP2_KEEP_ALIVE_ZERO"
    } else if message.starts_with("HTTP/2 keep-alive durations must be <=") {
        "CONFIG_HTTP2_KEEP_ALIVE_RANGE"
    } else {
        "UNSPECIFIED"
    }
}

fn limit_diagnostic_code(resource: &str) -> &'static str {
    match resource {
        "request body" => "LIMIT_REQUEST_BODY_BYTES",
        "response body" => "LIMIT_RESPONSE_BODY_BYTES",
        "request header count" => "LIMIT_REQUEST_HEADER_COUNT",
        "request headers" => "LIMIT_REQUEST_HEADER_BYTES",
        "response header count" => "LIMIT_RESPONSE_HEADER_COUNT",
        "response headers" => "LIMIT_RESPONSE_HEADER_BYTES",
        _ => "UNSPECIFIED",
    }
}

fn tls_diagnostic_code(message: &str) -> &'static str {
    match message {
        "additional CA bundle is outside the supported size bound" => "TLS_CA_BUNDLE_SIZE",
        "additional CA bundle could not be parsed" => "TLS_CA_BUNDLE_PARSE",
        "additional CA bundle contains non-certificate material" => "TLS_CA_BUNDLE_NON_CERTIFICATE",
        "additional CA certificate was rejected" => "TLS_CA_CERTIFICATE_REJECTED",
        "additional CA bundle contains too many certificates" => "TLS_CA_CERTIFICATE_COUNT",
        "additional CA bundle contains no certificates" => "TLS_CA_BUNDLE_EMPTY",
        "additional CA bundle path must be absolute and canonical" => "TLS_CA_PATH_INVALID",
        "additional CA bundle path must reference a regular non-symlink file" => {
            "TLS_CA_PATH_NOT_REGULAR"
        }
        "additional CA bundle file is unavailable" => "TLS_CA_FILE_UNAVAILABLE",
        _ => "UNSPECIFIED",
    }
}

fn header_diagnostic_code(message: &str) -> &'static str {
    if message == HOP_BY_HOP_REQUEST_HEADER_MESSAGE {
        "REQUEST_HOP_BY_HOP_HEADER"
    } else if message == TRANSPORT_OWNED_REQUEST_HEADER_MESSAGE {
        "REQUEST_TRANSPORT_OWNED_HEADER"
    } else if message == "Header name cannot be empty" {
        "HEADER_NAME_EMPTY"
    } else if message.starts_with("Invalid header name character:") {
        "HEADER_NAME_INVALID"
    } else if message.starts_with("Invalid header value character:") {
        "HEADER_VALUE_INVALID"
    } else if message == "Invalid HTTP header name" {
        "HEADER_NAME_INVALID"
    } else if message == "Invalid HTTP header value" {
        "HEADER_VALUE_INVALID"
    } else if message == "duplicate HTTP header name after normalization" {
        "HEADER_NAME_DUPLICATE"
    } else {
        "UNSPECIFIED"
    }
}

fn request_building_diagnostic_code(message: &str) -> &'static str {
    if message.starts_with("Invalid URL:") {
        "REQUEST_URL_INVALID"
    } else if message == "Cannot add query parameters without setting URL first" {
        "REQUEST_QUERY_WITHOUT_URL"
    } else if message == "HTTP method is required" {
        "REQUEST_METHOD_MISSING"
    } else if message == "URL is required" {
        "REQUEST_URL_MISSING"
    } else if message.starts_with("Unsupported URL scheme:") {
        "REQUEST_URL_SCHEME_UNSUPPORTED"
    } else if message.starts_with("invalid URI:") {
        "REQUEST_URI_INVALID"
    } else {
        "UNSPECIFIED"
    }
}

fn invalid_url_diagnostic_code(message: &str) -> &'static str {
    match message {
        "request URL is invalid" => "REQUEST_URL_INVALID",
        "request URL scheme must be http or https" => "REQUEST_URL_SCHEME_UNSUPPORTED",
        "request URL must contain a host" => "REQUEST_URL_HOST_MISSING",
        "request URL must not contain credentials" => "REQUEST_URL_CREDENTIALS",
        "relative URL requires a configured base_address" => "REQUEST_RELATIVE_URL_REQUIRES_BASE",
        "relative request URL contains an ambiguous authority separator" => {
            "REQUEST_RELATIVE_URL_AMBIGUOUS_AUTHORITY"
        }
        "invalid relative request URL" => "REQUEST_RELATIVE_URL_INVALID",
        "relative request URL must not override the base origin" => {
            "REQUEST_RELATIVE_URL_ORIGIN_OVERRIDE"
        }
        _ => "UNSPECIFIED",
    }
}

fn invalid_method_diagnostic_code(message: &str) -> &'static str {
    match message {
        "request method token is invalid" => "REQUEST_METHOD_TOKEN_INVALID",
        _ => "UNSPECIFIED",
    }
}

fn unsupported_configuration_diagnostic_code(message: &str) -> &'static str {
    match message {
        "streaming/non-repeatable request bodies are not supported by the bounded-buffered v1 transport" => {
            "REQUEST_BODY_NOT_REPEATABLE"
        }
        "request body must declare its buffered length" => "REQUEST_BODY_LENGTH_REQUIRED",
        "request body materialized length does not match its declared buffered length" => {
            "REQUEST_BODY_LENGTH_MISMATCH"
        }
        _ => "UNSPECIFIED",
    }
}

fn redirect_diagnostic_code(message: &str) -> &'static str {
    match message {
        "HTTPS-to-HTTP downgrade is forbidden" => "REDIRECT_TLS_DOWNGRADE",
        "redirect target must not contain URL credentials" => "REDIRECT_URL_CREDENTIALS",
        "cross-origin redirects are forbidden" => "REDIRECT_CROSS_ORIGIN",
        _ => "UNSPECIFIED",
    }
}

impl From<url::ParseError> for HttpClientError {
    fn from(err: url::ParseError) -> Self {
        HttpClientError::InvalidUrl(err.to_string())
    }
}

impl From<serde_json::Error> for HttpClientError {
    fn from(err: serde_json::Error) -> Self {
        HttpClientError::Json(err.to_string())
    }
}

impl From<std::io::Error> for HttpClientError {
    fn from(err: std::io::Error) -> Self {
        HttpClientError::Io(err.to_string())
    }
}

/// Converts the client-owned transport error into Lily's HTTP API boundary.
impl From<HttpClientError> for lily_error::application::http_api::HttpApiError {
    fn from(error: HttpClientError) -> Self {
        use lily_error::application::http_api::HttpApiError;

        let safe_message = error.to_string();
        match error {
            HttpClientError::Connection(_) => HttpApiError::BadGateway(safe_message),
            HttpClientError::ConnectTimeout | HttpClientError::Timeout => {
                HttpApiError::GatewayTimeout(safe_message)
            }
            HttpClientError::Tls(_) => HttpApiError::TlsError(safe_message),
            HttpClientError::InvalidUrl(_)
            | HttpClientError::RequestBuilding(_)
            | HttpClientError::InvalidMethod(_)
            | HttpClientError::InvalidMultipart(_) => HttpApiError::BadRequest(safe_message),
            HttpClientError::HttpParsing(_)
            | HttpClientError::ResponseParsing(_)
            | HttpClientError::BodyReading(_)
            | HttpClientError::Body(_)
            | HttpClientError::Server { .. }
            | HttpClientError::ClientStatus { .. }
            | HttpClientError::LimitExceeded { .. }
            | HttpClientError::ProtocolNegotiation { .. }
            | HttpClientError::RedirectRejected(_) => HttpApiError::BadGateway(safe_message),
            HttpClientError::Json(_) | HttpClientError::Parse(_) => {
                HttpApiError::DeserializationError(safe_message)
            }
            HttpClientError::InvalidHeader(_) => HttpApiError::InvalidHttpHeader(safe_message),
            HttpClientError::Client(_)
            | HttpClientError::Configuration(_)
            | HttpClientError::OriginCapacityExceeded { .. }
            | HttpClientError::UnsupportedConfiguration(_) => {
                HttpApiError::ExternalServiceError(safe_message)
            }
            HttpClientError::Io(_) => HttpApiError::IoError(safe_message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = HttpClientError::client("test error");
        assert_eq!(
            err.to_string(),
            "HTTP client error (category=CLIENT_STATE_ERROR, diagnostic=UNSPECIFIED)"
        );
    }

    #[test]
    fn test_error_categories() {
        let network_err = HttpClientError::Connection("connection refused".to_string());
        assert!(network_err.is_network_error());

        let timeout_err = HttpClientError::Timeout;
        assert!(timeout_err.is_timeout());

        let connect_timeout = HttpClientError::ConnectTimeout;
        assert!(connect_timeout.is_timeout());
        assert!(connect_timeout.is_network_error());
        assert_eq!(connect_timeout.error_code(), "CONNECT_TIMEOUT");
        assert_eq!(connect_timeout.diagnostic_code(), "CONNECT_DEADLINE");

        let client_err = HttpClientError::client_status(404, "Not Found");
        assert!(client_err.is_client_error());
        assert_eq!(client_err.status_code(), Some(404));

        let server_err = HttpClientError::server(500, "Internal Server Error");
        assert!(server_err.is_server_error());
        assert_eq!(server_err.status_code(), Some(500));
    }

    #[test]
    fn configuration_diagnostics_identify_the_invalid_field() {
        let cases = [
            (
                "base_address must be an absolute URL",
                "CONFIG_BASE_ADDRESS_ABSOLUTE",
            ),
            (
                "base_address must not contain URL credentials",
                "CONFIG_BASE_ADDRESS_CREDENTIALS",
            ),
            (
                "connect_timeout must be <= 300 seconds",
                "CONFIG_CONNECT_TIMEOUT_RANGE",
            ),
            (
                "request timeout must be in 1ns..=86400 seconds",
                "CONFIG_REQUEST_TIMEOUT_RANGE",
            ),
            (
                "pool_idle_timeout must be <= 3600 seconds",
                "CONFIG_POOL_IDLE_TIMEOUT_RANGE",
            ),
            (
                "max_request_body_bytes must be in 1..=1GiB",
                "CONFIG_MAX_REQUEST_BODY_BYTES_RANGE",
            ),
            (
                "max_response_body_bytes must be in 1..=1GiB",
                "CONFIG_MAX_RESPONSE_BODY_BYTES_RANGE",
            ),
            (
                "max_header_bytes must be in 1..=1MiB",
                "CONFIG_MAX_HEADER_BYTES_RANGE",
            ),
            (
                "max_header_count must be in 1..=1024",
                "CONFIG_MAX_HEADER_COUNT_RANGE",
            ),
            (
                "max_retained_origins must be in 1..=1024",
                "CONFIG_MAX_RETAINED_ORIGINS_RANGE",
            ),
            (
                "http2_initial_stream_window_bytes must be in 65535..=2147483647",
                "CONFIG_HTTP2_STREAM_WINDOW_RANGE",
            ),
            (
                "http2_initial_connection_window_bytes must be in 65535..=2147483647",
                "CONFIG_HTTP2_CONNECTION_WINDOW_RANGE",
            ),
            (
                "HTTP/2 keep-alive durations must be greater than zero",
                "CONFIG_HTTP2_KEEP_ALIVE_ZERO",
            ),
        ];

        for (message, expected) in cases {
            let error = HttpClientError::Configuration(message.to_string());
            assert_eq!(error.diagnostic_code(), expected, "message: {message}");
            assert!(error.to_string().contains(expected));
            assert!(!error.to_string().contains(message));
        }
    }

    #[test]
    fn limit_diagnostics_identify_the_bounded_resource() {
        let cases = [
            ("request body", "LIMIT_REQUEST_BODY_BYTES"),
            ("response body", "LIMIT_RESPONSE_BODY_BYTES"),
            ("request header count", "LIMIT_REQUEST_HEADER_COUNT"),
            ("request headers", "LIMIT_REQUEST_HEADER_BYTES"),
            ("response header count", "LIMIT_RESPONSE_HEADER_COUNT"),
            ("response headers", "LIMIT_RESPONSE_HEADER_BYTES"),
        ];

        for (resource, expected) in cases {
            let error = HttpClientError::LimitExceeded {
                resource: resource.to_string(),
                limit: 42,
            };
            assert_eq!(error.diagnostic_code(), expected);
            assert_eq!(
                error.to_string(),
                format!(
                    "HTTP client error (category=LIMIT_EXCEEDED, diagnostic={expected}, limit=42)"
                )
            );
            assert!(!format!("{error:?}").contains(resource));
        }
    }

    #[test]
    fn variant_specific_diagnostics_do_not_collapse_to_unspecified() {
        let cases = [
            (
                HttpClientError::Tls("additional CA bundle contains no certificates".to_string()),
                "TLS_CA_BUNDLE_EMPTY",
            ),
            (
                HttpClientError::RequestBuilding("HTTP method is required".to_string()),
                "REQUEST_METHOD_MISSING",
            ),
            (
                HttpClientError::ProtocolNegotiation {
                    requested: "h2".to_string(),
                    negotiated: "http/1.1".to_string(),
                },
                "PROTOCOL_POLICY_MISMATCH",
            ),
            (
                HttpClientError::InvalidMultipart(MultipartBuildError::InvalidBoundary),
                "MULTIPART_INVALID_BOUNDARY",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.diagnostic_code(), expected);
        }
    }

    #[test]
    fn status_and_bounded_capacity_are_the_only_variant_metadata_in_safe_formatting() {
        let status = HttpClientError::ClientStatus {
            status: 401,
            message: "LILY_SECRET_STATUS_MESSAGE".to_string(),
        };
        assert_eq!(
            status.to_string(),
            "HTTP client error (category=CLIENT_STATUS_ERROR, diagnostic=UNSPECIFIED, status=401)"
        );
        assert!(format!("{status:?}").contains("status: 401"));
        assert!(!format!("{status:?}").contains("LILY_SECRET_STATUS_MESSAGE"));
        let converted_status: lily_error::application::http_api::HttpApiError =
            status.clone().into();
        assert!(converted_status.to_string().contains("status=401"));
        assert!(!converted_status
            .to_string()
            .contains("LILY_SECRET_STATUS_MESSAGE"));

        let limit = HttpClientError::LimitExceeded {
            resource: "LILY_SECRET_LIMIT_RESOURCE".to_string(),
            limit: 73,
        };
        assert_eq!(
            limit.to_string(),
            "HTTP client error (category=LIMIT_EXCEEDED, diagnostic=UNSPECIFIED, limit=73)"
        );
        assert!(format!("{limit:?}").contains("limit: 73"));
        assert!(!format!("{limit:?}").contains("LILY_SECRET_LIMIT_RESOURCE"));
        let converted_limit: lily_error::application::http_api::HttpApiError = limit.into();
        assert!(converted_limit.to_string().contains("limit=73"));
        assert!(!converted_limit
            .to_string()
            .contains("LILY_SECRET_LIMIT_RESOURCE"));

        let origin_capacity = HttpClientError::OriginCapacityExceeded { limit: 64 };
        assert_eq!(
            origin_capacity.to_string(),
            "HTTP client error (category=ORIGIN_CAPACITY_EXCEEDED, diagnostic=ORIGIN_CAPACITY_ACTIVE, limit=64)"
        );
        assert!(format!("{origin_capacity:?}").contains("limit: 64"));
    }

    #[test]
    fn dynamic_factory_context_and_unknown_payloads_are_redacted_after_conversion() {
        let sentinel = "LILY_SECRET_FACTORY_CLIENT";
        let known = HttpClientError::Configuration(format!(
            "HTTP client configuration not found for prefix: {sentinel}"
        ));
        assert_eq!(known.diagnostic_code(), "CONFIG_FACTORY_CLIENT_NOT_FOUND");

        let unknown = HttpClientError::Configuration(sentinel.to_string());
        assert_eq!(unknown.diagnostic_code(), "UNSPECIFIED");

        for error in [known, unknown] {
            assert!(!error.to_string().contains(sentinel));
            assert!(!format!("{error:?}").contains(sentinel));

            let converted: lily_error::application::http_api::HttpApiError = error.into();
            assert!(!converted.to_string().contains(sentinel));
            assert!(!format!("{converted:?}").contains(sentinel));
        }
    }

    #[test]
    fn http_api_conversion_keeps_client_and_upstream_failures_distinct() {
        use lily_error::application::http_api::HttpApiError;

        let cases = [
            (HttpClientError::InvalidUrl("invalid".into()), 400),
            (HttpClientError::InvalidHeader("invalid".into()), 400),
            (HttpClientError::Connection("refused".into()), 502),
            (HttpClientError::ConnectTimeout, 504),
            (
                HttpClientError::ClientStatus {
                    status: 401,
                    message: "upstream rejected request".into(),
                },
                502,
            ),
        ];

        for (error, expected_status) in cases {
            let converted = HttpApiError::from(error);
            assert_eq!(converted.http_status().0, expected_status);
        }
    }
}
