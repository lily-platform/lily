use super::response::{
    is_singleton_response_header_name, validate_response_header_input, Response, ResponseBodyError,
    ResponseHeaderError, ResponseWriteError,
};
use serde::{de::IgnoredAny, Deserialize, Serialize};
use std::{fmt, io, io::Write, time::Duration};

/// Maximum bytes accepted for one stable rejection code.
pub const MAX_HTTP_ERROR_CODE_BYTES: usize = 64;
/// Maximum application-managed header field lines carried by one rejection.
pub const MAX_HTTP_REJECTION_HEADERS: usize = 16;
/// Maximum aggregate wire bytes retained for rejection header field lines.
pub const MAX_HTTP_REJECTION_HEADER_BYTES: usize = 16 * 1024;
/// Maximum bytes retained for an explicit public rejection body.
pub const MAX_HTTP_REJECTION_BODY_BYTES: usize = 64 * 1024;
/// Maximum bytes retained for a public Problem Details `detail` member.
pub const MAX_HTTP_REJECTION_DETAIL_BYTES: usize = 4 * 1024;

const MAX_RETRY_AFTER: Duration = Duration::from_secs(86_400);

/// A validated static application error code safe for bounded telemetry labels.
///
/// Codes must begin with an ASCII uppercase letter and may then contain only
/// ASCII uppercase letters, digits, and underscores. Request data must never be
/// converted into a leaked static string to construct this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HttpErrorCode(&'static str);

impl HttpErrorCode {
    /// Framework-owned middleware timeout code.
    pub const TIMEOUT: Self = Self("MIDDLEWARE_TIMEOUT");
    /// Framework-owned middleware internal-failure code.
    pub const INTERNAL: Self = Self("MIDDLEWARE_INTERNAL");

    /// Validates a static telemetry-safe application error code.
    pub fn new(code: &'static str) -> Result<Self, HttpRejectionError> {
        if code.is_empty() {
            return Err(HttpRejectionError::EmptyErrorCode);
        }
        if code.len() > MAX_HTTP_ERROR_CODE_BYTES {
            return Err(HttpRejectionError::ErrorCodeTooLong {
                limit_bytes: MAX_HTTP_ERROR_CODE_BYTES,
                actual_bytes: code.len(),
            });
        }
        if !code.as_bytes()[0].is_ascii_uppercase()
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(HttpRejectionError::InvalidErrorCode);
        }
        Ok(Self(code))
    }

    #[must_use]
    /// Returns the validated static code.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for HttpErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// A secret-safe construction or validation failure for [`HttpRejection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HttpRejectionError {
    /// The error code was empty.
    #[error("HTTP rejection error code is empty")]
    EmptyErrorCode,
    /// The error code exceeded its telemetry bound.
    #[error("HTTP rejection error code has {actual_bytes} bytes; limit is {limit_bytes}")]
    ErrorCodeTooLong {
        /// Maximum accepted code bytes.
        limit_bytes: usize,
        /// Supplied code bytes.
        actual_bytes: usize,
    },
    /// The code did not use the required uppercase ASCII grammar.
    #[error("HTTP rejection error code contains invalid characters")]
    InvalidErrorCode,
    /// The supplied status was not a client or server error.
    #[error("HTTP rejection status {status} is outside 400..=599")]
    InvalidStatus {
        /// Rejected status code.
        status: u16,
    },
    /// The retry delay was zero or exceeded the framework ceiling.
    #[error("HTTP rejection Retry-After must be within 1ms..=86400s")]
    InvalidRetryAfter,
    /// The status does not define Lily's typed `Retry-After` helper.
    #[error("HTTP status {status} does not support the typed Retry-After helper")]
    RetryAfterNotSupported {
        /// Rejected status code.
        status: u16,
    },
    /// The rejection exceeded its header-count bound.
    #[error("HTTP rejection header count exceeds the limit of {limit}")]
    TooManyHeaders {
        /// Maximum rejection header field-line count.
        limit: usize,
    },
    /// The rejection exceeded its aggregate header-byte bound.
    #[error("HTTP rejection header bytes exceed the limit of {limit_bytes}")]
    HeadersTooLarge {
        /// Maximum aggregate header wire bytes.
        limit_bytes: usize,
    },
    /// A caller attempted to override representation-owned fields.
    #[error("HTTP rejection representation controls Content-Type and Retry-After")]
    RepresentationControlledHeader,
    /// A singleton header was declared more than once.
    #[error("HTTP rejection contains a duplicate singleton header")]
    DuplicateSingletonHeader,
    /// General response-header validation failed.
    #[error("HTTP rejection response header is invalid: {0}")]
    InvalidHeader(ResponseHeaderError),
    /// A status-specific mandatory field was absent.
    #[error("HTTP {status} rejection requires the {header} response header")]
    MissingRequiredHeader {
        /// Status requiring the field.
        status: u16,
        /// Required field name.
        header: &'static str,
    },
    /// Public Problem Details detail exceeded its bound.
    #[error("HTTP rejection detail has {actual_bytes} bytes; limit is {limit_bytes}")]
    DetailTooLarge {
        /// Maximum detail bytes.
        limit_bytes: usize,
        /// Supplied detail bytes.
        actual_bytes: usize,
    },
    /// An explicit public body exceeded its bound.
    #[error("HTTP rejection body has {actual_bytes} bytes; limit is {limit_bytes}")]
    BodyTooLarge {
        /// Maximum body bytes.
        limit_bytes: usize,
        /// Supplied body bytes.
        actual_bytes: usize,
    },
    /// Allocation failed while remaining inside configured bounds.
    #[error("HTTP rejection allocation failed within its configured bounds")]
    AllocationFailed,
    /// A public JSON body could not be serialized.
    #[error("HTTP rejection JSON serialization failed")]
    JsonSerializationFailed,
    /// Explicit JSON bytes were not one valid JSON representation.
    #[error("HTTP rejection JSON body is invalid")]
    InvalidJsonBody,
}

/// Public representation selected for one rejection response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HttpRejectionBodyKind {
    /// RFC Problem Details JSON generated from status and code.
    ProblemDetails,
    /// Caller-supplied JSON bytes.
    Json,
    /// Caller-supplied UTF-8 plain text.
    Text,
    /// No public body.
    Empty,
}

#[derive(Clone, PartialEq, Eq)]
struct RejectionHeader {
    name: String,
    value: String,
}

#[derive(Clone, PartialEq, Eq)]
struct RejectionBody {
    kind: HttpRejectionBodyKind,
    content_type: Option<&'static str>,
    bytes: Vec<u8>,
}

/// Canonical fail-closed HTTP response contract shared by guards and
/// middleware.
///
/// The status and static code are safe observability fields. Headers and body
/// are explicit public response data and are deliberately omitted from
/// `Debug`/`Display` so normal error logging cannot disclose them.
#[derive(Clone, PartialEq, Eq)]
pub struct HttpRejection {
    status: u16,
    code: HttpErrorCode,
    headers: Vec<RejectionHeader>,
    header_bytes: usize,
    retry_after: Option<Duration>,
    body: RejectionBody,
}

impl HttpRejection {
    /// Builds a rejection with RFC Problem Details as its default body.
    ///
    /// Statuses with mandatory response headers (401, 405, and 407) must be
    /// created through [`Self::builder`] or their dedicated convenience
    /// constructor.
    pub fn new(status: u16, code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::builder(status, code)?.build()
    }

    /// Starts a bounded rejection builder for an error status.
    pub fn builder(
        status: u16,
        code: HttpErrorCode,
    ) -> Result<HttpRejectionBuilder, HttpRejectionError> {
        validate_status(status)?;
        Ok(HttpRejectionBuilder {
            status,
            code,
            headers: Vec::new(),
            header_bytes: 0,
            retry_after: None,
            body: RejectionBodyDraft::ProblemDetails { detail: None },
        })
    }

    /// Creates a `400 Bad Request` rejection.
    pub fn bad_request(code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::new(400, code)
    }

    /// Creates a `401 Unauthorized` rejection with `WWW-Authenticate`.
    pub fn unauthorized(code: HttpErrorCode, challenge: &str) -> Result<Self, HttpRejectionError> {
        Self::builder(401, code)?
            .with_header("WWW-Authenticate", challenge)?
            .build()
    }

    /// Creates a `403 Forbidden` rejection.
    pub fn forbidden(code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::new(403, code)
    }

    /// Creates a `404 Not Found` rejection.
    pub fn not_found(code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::new(404, code)
    }

    /// Creates a `405 Method Not Allowed` rejection with `Allow`.
    pub fn method_not_allowed(
        code: HttpErrorCode,
        allow: &str,
    ) -> Result<Self, HttpRejectionError> {
        Self::builder(405, code)?
            .with_header("Allow", allow)?
            .build()
    }

    /// Creates a `409 Conflict` rejection.
    pub fn conflict(code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::new(409, code)
    }

    /// Creates a `413 Content Too Large` rejection.
    pub fn payload_too_large(code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::new(413, code)
    }

    /// Creates a `415 Unsupported Media Type` rejection.
    pub fn unsupported_media_type(code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::new(415, code)
    }

    /// Creates a `422 Unprocessable Content` rejection.
    pub fn unprocessable_content(code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::new(422, code)
    }

    /// Creates a `429 Too Many Requests` rejection with `Retry-After`.
    pub fn too_many_requests(
        code: HttpErrorCode,
        retry_after: Duration,
    ) -> Result<Self, HttpRejectionError> {
        Self::new(429, code)?.with_retry_after(retry_after)
    }

    /// Creates a `503 Service Unavailable` rejection.
    pub fn service_unavailable(code: HttpErrorCode) -> Result<Self, HttpRejectionError> {
        Self::new(503, code)
    }

    /// Appends one bounded application header.
    pub fn with_header(mut self, name: &str, value: &str) -> Result<Self, HttpRejectionError> {
        push_header(
            &mut self.headers,
            &mut self.header_bytes,
            name,
            value,
            false,
        )?;
        validate_required_headers(self.status, &self.headers)?;
        Ok(self)
    }

    /// Adds a bounded Retry-After delta for 429 or 503 and rounds it up to
    /// whole seconds on the wire.
    pub fn with_retry_after(mut self, retry_after: Duration) -> Result<Self, HttpRejectionError> {
        add_retry_after(
            self.status,
            &mut self.headers,
            &mut self.header_bytes,
            &mut self.retry_after,
            retry_after,
        )?;
        Ok(self)
    }

    /// Replaces the default Problem Details detail with bounded public text.
    pub fn with_problem_detail(mut self, detail: &str) -> Result<Self, HttpRejectionError> {
        self.body = problem_details_body(self.status, self.code, Some(detail))?;
        Ok(self)
    }

    /// Replaces the public representation with serialized JSON.
    pub fn with_json_body<T: Serialize + ?Sized>(
        mut self,
        value: &T,
    ) -> Result<Self, HttpRejectionError> {
        self.body = json_body(value)?;
        Ok(self)
    }

    /// Replaces the public representation with validated JSON bytes.
    pub fn with_json_bytes(mut self, bytes: Vec<u8>) -> Result<Self, HttpRejectionError> {
        self.body = validated_json_bytes(bytes)?;
        Ok(self)
    }

    /// Replaces the public representation with bounded plain text.
    pub fn with_text_body(mut self, text: &str) -> Result<Self, HttpRejectionError> {
        self.body = text_body(text)?;
        Ok(self)
    }

    #[must_use]
    /// Removes the public response body.
    pub fn without_body(mut self) -> Self {
        self.body = empty_body();
        self
    }

    #[must_use]
    /// Returns the rejection status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    #[must_use]
    /// Returns the validated application error code.
    pub const fn code(&self) -> HttpErrorCode {
        self.code
    }

    #[must_use]
    /// Returns the static code used by telemetry and the public body.
    pub const fn error_code(&self) -> &'static str {
        self.code.as_str()
    }

    #[must_use]
    /// Returns the status code and canonical reason phrase.
    pub fn http_status(&self) -> (u16, &'static str) {
        (self.status, status_reason(self.status))
    }

    #[must_use]
    /// Returns the exact configured retry delay.
    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    #[must_use]
    /// Returns the wire `Retry-After` delta rounded up to whole seconds.
    pub fn retry_after_seconds(&self) -> Option<u64> {
        self.retry_after.map(duration_seconds_ceil)
    }

    #[must_use]
    /// Returns the selected public representation kind.
    pub const fn body_kind(&self) -> HttpRejectionBodyKind {
        self.body.kind
    }

    #[must_use]
    /// Returns Lily's generic status-based public message.
    pub fn public_message(&self) -> &'static str {
        default_public_message(self.status)
    }

    /// Iterates over application-managed rejection headers.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|header| (header.name.as_str(), header.value.as_str()))
    }

    /// Atomically replaces a buffered response with this rejection.
    ///
    /// Effective server response limits are checked again here. A failure
    /// leaves the destination untouched so the caller can materialize its
    /// fixed internal-error fallback without exposing a partial response.
    pub async fn write_to_response(
        &self,
        response: &mut Response,
    ) -> Result<(), ResponseWriteError> {
        let mut safe_response = Response::from_limits(response.limits());
        let (_, reason) = self.http_status();
        safe_response.status(self.status, reason);

        if let Some(content_type) = self.body.content_type {
            safe_response.try_insert_header("Content-Type", content_type)?;
        }
        for header in &self.headers {
            safe_response.try_append_header(&header.name, &header.value)?;
        }

        let mut body = Vec::new();
        body.try_reserve_exact(self.body.bytes.len()).map_err(|_| {
            ResponseBodyError::AllocationFailed {
                limit_bytes: response.body_budget().limit_bytes(),
            }
        })?;
        body.extend_from_slice(&self.body.bytes);
        safe_response.set_body_vec(body)?;
        *response = safe_response;
        Ok(())
    }
}

impl fmt::Debug for HttpRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRejection")
            .field("status", &self.status)
            .field("code", &self.code)
            .field("header_count", &self.headers.len())
            .field("header_bytes", &self.header_bytes)
            .field("retry_after", &self.retry_after)
            .field("body_kind", &self.body.kind)
            .field("body_bytes", &self.body.bytes.len())
            .finish()
    }
}

impl fmt::Display for HttpRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "HTTP request rejected with status {} and code {}",
            self.status, self.code
        )
    }
}

impl std::error::Error for HttpRejection {}

enum RejectionBodyDraft {
    ProblemDetails { detail: Option<String> },
    Json(Vec<u8>),
    Text(Vec<u8>),
    Empty,
}

/// Fallible builder used when a rejection requires mandatory response headers
/// or multiple public representation customizations.
pub struct HttpRejectionBuilder {
    status: u16,
    code: HttpErrorCode,
    headers: Vec<RejectionHeader>,
    header_bytes: usize,
    retry_after: Option<Duration>,
    body: RejectionBodyDraft,
}

impl HttpRejectionBuilder {
    /// Appends one bounded application header.
    pub fn with_header(mut self, name: &str, value: &str) -> Result<Self, HttpRejectionError> {
        push_header(
            &mut self.headers,
            &mut self.header_bytes,
            name,
            value,
            false,
        )?;
        Ok(self)
    }

    /// Adds a typed retry delay for status 429 or 503.
    pub fn with_retry_after(mut self, retry_after: Duration) -> Result<Self, HttpRejectionError> {
        add_retry_after(
            self.status,
            &mut self.headers,
            &mut self.header_bytes,
            &mut self.retry_after,
            retry_after,
        )?;
        Ok(self)
    }

    /// Adds bounded public detail to the generated Problem Details body.
    pub fn with_problem_detail(mut self, detail: &str) -> Result<Self, HttpRejectionError> {
        if detail.len() > MAX_HTTP_REJECTION_DETAIL_BYTES {
            return Err(HttpRejectionError::DetailTooLarge {
                limit_bytes: MAX_HTTP_REJECTION_DETAIL_BYTES,
                actual_bytes: detail.len(),
            });
        }
        self.body = RejectionBodyDraft::ProblemDetails {
            detail: Some(copy_string(detail)?),
        };
        Ok(self)
    }

    /// Selects a serialized JSON public body.
    pub fn with_json_body<T: Serialize + ?Sized>(
        mut self,
        value: &T,
    ) -> Result<Self, HttpRejectionError> {
        self.body = RejectionBodyDraft::Json(serialize_json_bounded(value)?);
        Ok(self)
    }

    /// Selects an already-encoded, validated JSON public body.
    pub fn with_json_bytes(mut self, bytes: Vec<u8>) -> Result<Self, HttpRejectionError> {
        self.body = RejectionBodyDraft::Json(validated_json_bytes(bytes)?.bytes);
        Ok(self)
    }

    /// Selects a bounded UTF-8 plain-text public body.
    pub fn with_text_body(mut self, text: &str) -> Result<Self, HttpRejectionError> {
        self.body = RejectionBodyDraft::Text(copy_body(text.as_bytes())?);
        Ok(self)
    }

    #[must_use]
    /// Selects an empty public body.
    pub fn without_body(mut self) -> Self {
        self.body = RejectionBodyDraft::Empty;
        self
    }

    /// Validates mandatory fields and materializes the rejection atomically.
    pub fn build(self) -> Result<HttpRejection, HttpRejectionError> {
        validate_required_headers(self.status, &self.headers)?;
        let body = match self.body {
            RejectionBodyDraft::ProblemDetails { detail } => {
                problem_details_body(self.status, self.code, detail.as_deref())?
            }
            RejectionBodyDraft::Json(bytes) => RejectionBody {
                kind: HttpRejectionBodyKind::Json,
                content_type: Some("application/json"),
                bytes,
            },
            RejectionBodyDraft::Text(bytes) => RejectionBody {
                kind: HttpRejectionBodyKind::Text,
                content_type: Some("text/plain; charset=utf-8"),
                bytes,
            },
            RejectionBodyDraft::Empty => empty_body(),
        };
        Ok(HttpRejection {
            status: self.status,
            code: self.code,
            headers: self.headers,
            header_bytes: self.header_bytes,
            retry_after: self.retry_after,
            body,
        })
    }
}

impl fmt::Debug for HttpRejectionBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRejectionBuilder")
            .field("status", &self.status)
            .field("code", &self.code)
            .field("header_count", &self.headers.len())
            .field("header_bytes", &self.header_bytes)
            .field("retry_after", &self.retry_after)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct ProblemDetails<'a> {
    #[serde(rename = "type")]
    type_uri: &'static str,
    title: &'static str,
    status: u16,
    code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<&'a str>,
}

fn problem_details_body(
    status: u16,
    code: HttpErrorCode,
    detail: Option<&str>,
) -> Result<RejectionBody, HttpRejectionError> {
    if let Some(detail) = detail {
        if detail.len() > MAX_HTTP_REJECTION_DETAIL_BYTES {
            return Err(HttpRejectionError::DetailTooLarge {
                limit_bytes: MAX_HTTP_REJECTION_DETAIL_BYTES,
                actual_bytes: detail.len(),
            });
        }
    }
    let bytes = serialize_json_bounded(&ProblemDetails {
        type_uri: "about:blank",
        title: status_reason(status),
        status,
        code: code.as_str(),
        detail,
    })?;
    Ok(RejectionBody {
        kind: HttpRejectionBodyKind::ProblemDetails,
        content_type: Some("application/problem+json"),
        bytes,
    })
}

fn json_body<T: Serialize + ?Sized>(value: &T) -> Result<RejectionBody, HttpRejectionError> {
    Ok(RejectionBody {
        kind: HttpRejectionBodyKind::Json,
        content_type: Some("application/json"),
        bytes: serialize_json_bounded(value)?,
    })
}

fn validated_json_bytes(bytes: Vec<u8>) -> Result<RejectionBody, HttpRejectionError> {
    validate_body_len(bytes.len())?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    IgnoredAny::deserialize(&mut deserializer).map_err(|_| HttpRejectionError::InvalidJsonBody)?;
    deserializer
        .end()
        .map_err(|_| HttpRejectionError::InvalidJsonBody)?;
    Ok(RejectionBody {
        kind: HttpRejectionBodyKind::Json,
        content_type: Some("application/json"),
        bytes,
    })
}

fn text_body(text: &str) -> Result<RejectionBody, HttpRejectionError> {
    Ok(RejectionBody {
        kind: HttpRejectionBodyKind::Text,
        content_type: Some("text/plain; charset=utf-8"),
        bytes: copy_body(text.as_bytes())?,
    })
}

fn empty_body() -> RejectionBody {
    RejectionBody {
        kind: HttpRejectionBodyKind::Empty,
        content_type: None,
        bytes: Vec::new(),
    }
}

fn validate_status(status: u16) -> Result<(), HttpRejectionError> {
    if !(400..=599).contains(&status) {
        return Err(HttpRejectionError::InvalidStatus { status });
    }
    Ok(())
}

fn validate_required_headers(
    status: u16,
    headers: &[RejectionHeader],
) -> Result<(), HttpRejectionError> {
    let required = match status {
        401 => Some(("www-authenticate", "WWW-Authenticate")),
        405 => Some(("allow", "Allow")),
        407 => Some(("proxy-authenticate", "Proxy-Authenticate")),
        _ => None,
    };
    if let Some((normalized, public_name)) = required {
        let present = headers.iter().any(|header| {
            header.name.eq_ignore_ascii_case(normalized) && !header.value.trim().is_empty()
        });
        if !present {
            return Err(HttpRejectionError::MissingRequiredHeader {
                status,
                header: public_name,
            });
        }
    }
    Ok(())
}

fn push_header(
    headers: &mut Vec<RejectionHeader>,
    header_bytes: &mut usize,
    name: &str,
    value: &str,
    policy_controlled: bool,
) -> Result<(), HttpRejectionError> {
    if !policy_controlled
        && (name.eq_ignore_ascii_case("content-type") || name.eq_ignore_ascii_case("retry-after"))
    {
        return Err(HttpRejectionError::RepresentationControlledHeader);
    }
    if headers.len() >= MAX_HTTP_REJECTION_HEADERS {
        return Err(HttpRejectionError::TooManyHeaders {
            limit: MAX_HTTP_REJECTION_HEADERS,
        });
    }
    let wire_bytes =
        validate_response_header_input(name, value).map_err(HttpRejectionError::InvalidHeader)?;
    let next_bytes =
        header_bytes
            .checked_add(wire_bytes)
            .ok_or(HttpRejectionError::HeadersTooLarge {
                limit_bytes: MAX_HTTP_REJECTION_HEADER_BYTES,
            })?;
    if next_bytes > MAX_HTTP_REJECTION_HEADER_BYTES {
        return Err(HttpRejectionError::HeadersTooLarge {
            limit_bytes: MAX_HTTP_REJECTION_HEADER_BYTES,
        });
    }
    if is_singleton_response_header_name(name)
        && headers
            .iter()
            .any(|header| header.name.eq_ignore_ascii_case(name))
    {
        return Err(HttpRejectionError::DuplicateSingletonHeader);
    }

    headers
        .try_reserve(1)
        .map_err(|_| HttpRejectionError::AllocationFailed)?;
    headers.push(RejectionHeader {
        name: copy_string(name)?,
        value: copy_string(value)?,
    });
    *header_bytes = next_bytes;
    Ok(())
}

fn add_retry_after(
    status: u16,
    headers: &mut Vec<RejectionHeader>,
    header_bytes: &mut usize,
    retained: &mut Option<Duration>,
    retry_after: Duration,
) -> Result<(), HttpRejectionError> {
    if !matches!(status, 429 | 503) {
        return Err(HttpRejectionError::RetryAfterNotSupported { status });
    }
    if retry_after < Duration::from_millis(1) || retry_after > MAX_RETRY_AFTER {
        return Err(HttpRejectionError::InvalidRetryAfter);
    }
    if retained.is_some() {
        return Err(HttpRejectionError::DuplicateSingletonHeader);
    }
    let value = duration_seconds_ceil(retry_after).to_string();
    push_header(headers, header_bytes, "Retry-After", &value, true)?;
    *retained = Some(retry_after);
    Ok(())
}

fn duration_seconds_ceil(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() != 0))
}

fn validate_body_len(actual_bytes: usize) -> Result<(), HttpRejectionError> {
    if actual_bytes > MAX_HTTP_REJECTION_BODY_BYTES {
        return Err(HttpRejectionError::BodyTooLarge {
            limit_bytes: MAX_HTTP_REJECTION_BODY_BYTES,
            actual_bytes,
        });
    }
    Ok(())
}

fn copy_string(value: &str) -> Result<String, HttpRejectionError> {
    let mut copied = String::new();
    copied
        .try_reserve_exact(value.len())
        .map_err(|_| HttpRejectionError::AllocationFailed)?;
    copied.push_str(value);
    Ok(copied)
}

fn copy_body(value: &[u8]) -> Result<Vec<u8>, HttpRejectionError> {
    validate_body_len(value.len())?;
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(value.len())
        .map_err(|_| HttpRejectionError::AllocationFailed)?;
    copied.extend_from_slice(value);
    Ok(copied)
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    failure: Option<HttpRejectionError>,
}

impl BoundedJsonWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            failure: None,
        }
    }
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.failure.is_some() {
            return Err(io::Error::other("HTTP rejection JSON writer failed"));
        }
        let Some(next_len) = self.bytes.len().checked_add(buffer.len()) else {
            self.failure = Some(HttpRejectionError::BodyTooLarge {
                limit_bytes: MAX_HTTP_REJECTION_BODY_BYTES,
                actual_bytes: usize::MAX,
            });
            return Err(io::Error::other("HTTP rejection JSON body is too large"));
        };
        if next_len > MAX_HTTP_REJECTION_BODY_BYTES {
            self.failure = Some(HttpRejectionError::BodyTooLarge {
                limit_bytes: MAX_HTTP_REJECTION_BODY_BYTES,
                actual_bytes: next_len,
            });
            return Err(io::Error::other("HTTP rejection JSON body is too large"));
        }
        if self.bytes.try_reserve(buffer.len()).is_err() {
            self.failure = Some(HttpRejectionError::AllocationFailed);
            return Err(io::Error::other(
                "HTTP rejection JSON body allocation failed",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialize_json_bounded<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, HttpRejectionError> {
    let mut writer = BoundedJsonWriter::new();
    let result = serde_json::to_writer(&mut writer, value);
    if let Some(error) = writer.failure {
        return Err(error);
    }
    result.map_err(|_| HttpRejectionError::JsonSerializationFailed)?;
    Ok(writer.bytes)
}

fn status_reason(status: u16) -> &'static str {
    http::StatusCode::from_u16(status)
        .ok()
        .and_then(|status| status.canonical_reason())
        .unwrap_or("HTTP Error")
}

fn default_public_message(status: u16) -> &'static str {
    match status {
        400 => "The request is invalid.",
        401 => "Authentication is required.",
        403 => "You are not allowed to perform this operation.",
        404 => "The requested resource was not found.",
        405 => "The request method is not allowed.",
        408 => "The request timed out.",
        409 => "The request conflicts with current state.",
        413 => "The request payload is too large.",
        415 => "The request media type is not supported.",
        422 => "The request content could not be processed.",
        429 => "Too many requests.",
        503 => "The service is temporarily unavailable.",
        _ => "The request was rejected.",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn code(value: &'static str) -> HttpErrorCode {
        HttpErrorCode::new(value).unwrap()
    }

    #[test]
    fn codes_and_statuses_are_strictly_bounded() {
        assert_eq!(code("RATE_LIMITED").as_str(), "RATE_LIMITED");
        assert_eq!(
            HttpErrorCode::new(""),
            Err(HttpRejectionError::EmptyErrorCode)
        );
        assert_eq!(
            HttpErrorCode::new("lower-case"),
            Err(HttpRejectionError::InvalidErrorCode)
        );
        assert_eq!(
            HttpRejection::new(399, code("INVALID")),
            Err(HttpRejectionError::InvalidStatus { status: 399 })
        );
        assert_eq!(
            HttpRejection::new(600, code("INVALID")),
            Err(HttpRejectionError::InvalidStatus { status: 600 })
        );
    }

    #[test]
    fn required_headers_and_retry_after_semantics_are_enforced() {
        for (status, header) in [
            (401, "WWW-Authenticate"),
            (405, "Allow"),
            (407, "Proxy-Authenticate"),
        ] {
            assert_eq!(
                HttpRejection::new(status, code("REQUIRED_HEADER_MISSING")),
                Err(HttpRejectionError::MissingRequiredHeader { status, header })
            );
        }
        let unauthorized =
            HttpRejection::unauthorized(code("AUTH_REQUIRED"), "Bearer realm=api").unwrap();
        assert_eq!(
            unauthorized.headers().collect::<Vec<_>>(),
            vec![("WWW-Authenticate", "Bearer realm=api")]
        );

        let rate_limited =
            HttpRejection::too_many_requests(code("RATE_LIMITED"), Duration::from_millis(1_001))
                .unwrap();
        assert_eq!(rate_limited.retry_after_seconds(), Some(2));
        assert_eq!(
            rate_limited.headers().collect::<Vec<_>>(),
            vec![("Retry-After", "2")]
        );
        assert_eq!(
            HttpRejection::forbidden(code("FORBIDDEN"))
                .unwrap()
                .with_retry_after(Duration::from_secs(1)),
            Err(HttpRejectionError::RetryAfterNotSupported { status: 403 })
        );
    }

    #[test]
    fn unsafe_headers_and_unbounded_public_bodies_are_rejected() {
        assert!(matches!(
            HttpRejection::forbidden(code("FORBIDDEN"))
                .unwrap()
                .with_header("Connection", "close"),
            Err(HttpRejectionError::InvalidHeader(
                ResponseHeaderError::HopByHop
            ))
        ));
        assert_eq!(
            HttpRejection::forbidden(code("FORBIDDEN"))
                .unwrap()
                .with_header("Content-Type", "text/html"),
            Err(HttpRejectionError::RepresentationControlledHeader)
        );
        let oversized = "x".repeat(MAX_HTTP_REJECTION_BODY_BYTES + 1);
        assert!(matches!(
            HttpRejection::forbidden(code("FORBIDDEN"))
                .unwrap()
                .with_text_body(&oversized),
            Err(HttpRejectionError::BodyTooLarge { .. })
        ));
        assert_eq!(
            HttpRejection::forbidden(code("FORBIDDEN"))
                .unwrap()
                .with_json_bytes(b"not-json".to_vec()),
            Err(HttpRejectionError::InvalidJsonBody)
        );
    }

    #[tokio::test]
    async fn default_problem_details_and_custom_representations_materialize_atomically() {
        let rejection =
            HttpRejection::too_many_requests(code("RATE_LIMITED"), Duration::from_secs(5))
                .unwrap()
                .with_problem_detail("Use a lower request rate.")
                .unwrap()
                .with_header("X-RateLimit-Limit", "100")
                .unwrap();
        let debug = format!("{rejection:?}");
        assert!(!debug.contains("Use a lower request rate"));
        assert!(!debug.contains("X-RateLimit-Limit"));

        let mut response = Response::new().await.unwrap();
        response.write_body(b"partial secret").unwrap();
        rejection.write_to_response(&mut response).await.unwrap();
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 429);
        assert!(parts.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-type") && value == "application/problem+json"
        }));
        assert!(parts
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("retry-after") && value == "5"));
        let body: serde_json::Value = serde_json::from_slice(&parts.body).unwrap();
        assert_eq!(body["type"], "about:blank");
        assert_eq!(body["title"], "Too Many Requests");
        assert_eq!(body["status"], 429);
        assert_eq!(body["code"], "RATE_LIMITED");
        assert_eq!(body["detail"], "Use a lower request rate.");

        let custom = HttpRejection::forbidden(code("TENANT_BLOCKED"))
            .unwrap()
            .with_json_body(&json!({"reason": "tenant_blocked"}))
            .unwrap();
        let mut response = Response::new().await.unwrap();
        custom.write_to_response(&mut response).await.unwrap();
        let parts = response.into_transport_parts().unwrap();
        assert!(parts.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-type") && value == "application/json"
        }));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&parts.body).unwrap(),
            json!({"reason": "tenant_blocked"})
        );
    }

    #[tokio::test]
    async fn failed_materialization_keeps_the_existing_response_untouched() {
        let limits =
            crate::ResponseLimits::new(crate::BodyBudget::new(1024).unwrap(), 1, 1024).unwrap();
        let rejection = HttpRejection::forbidden(code("FORBIDDEN"))
            .unwrap()
            .with_header("X-Policy", "denied")
            .unwrap();
        let mut response = Response::with_limits(limits).await.unwrap();
        response.status(202, "Accepted");
        response
            .try_insert_header("X-Existing", "preserved")
            .unwrap();
        response.write_body(b"existing").unwrap();

        assert!(rejection.write_to_response(&mut response).await.is_err());
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 202);
        assert_eq!(parts.body.as_ref(), b"existing");
        assert_eq!(
            parts.headers,
            vec![("x-existing".into(), "preserved".into())]
        );
    }
}
