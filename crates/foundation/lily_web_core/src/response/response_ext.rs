use crate::request::Request;
use crate::response::response::{
    is_singleton_response_header_name, validate_response_header_input, BoundedBodyWriter,
    ResponseBodyError, ResponseBodyStream, ResponseHeaderError, ResponseWriteError,
    HARD_MAX_RESPONSE_HEADERS, HARD_MAX_RESPONSE_HEADER_BYTES,
};
use crate::response::{HttpErrorCode, Response};
use crate::{CookieKeyRing, CookieRemoval, ResponseCookie, ResponseCookieError};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::StatusCode;
use lily_error::application::http_api::HttpApiError;
use serde::Serialize;
use smallvec::SmallVec;

const INLINE_PASSTHROUGH_HEADERS: usize = 4;

/// A secret-safe failure while staging or committing passthrough response metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PassthroughResponseError {
    /// Header validation or bounded insertion failed.
    #[error(transparent)]
    Header(#[from] ResponseHeaderError),
    /// Cookie validation or serialization failed.
    #[error(transparent)]
    Cookie(#[from] ResponseCookieError),
    /// `Set-Cookie` was supplied through an untyped header API.
    #[error("Set-Cookie must be written through the typed cookie API")]
    UntypedCookieHeader,
    /// A typed return value owns the attempted representation header.
    #[error("response representation headers are owned by the typed response")]
    RepresentationHeader,
    /// The requested passthrough status cannot compose with a typed success body.
    #[error("passthrough status must be a successful status that permits a response body")]
    InvalidStatus,
    /// The typed response declares an authoritative status.
    #[error("the typed response owns its HTTP status")]
    AuthoritativeStatus,
    /// Unit return preserved the existing response rather than materializing a typed one.
    #[error("passthrough response metadata requires a materialized typed response")]
    ResponseNotMaterialized,
}

impl From<PassthroughResponseError> for HttpApiError {
    fn from(error: PassthroughResponseError) -> Self {
        HttpApiError::ResponseEncodingError(error.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassthroughHeaderMode {
    Insert,
    Append,
}

struct PassthroughHeader {
    mode: PassthroughHeaderMode,
    name: String,
    value: String,
    wire_bytes: usize,
}

/// Stack-owned staging state emitted by the controller macro only for actions
/// that bind [`PassthroughResponseContext`].
#[doc(hidden)]
pub struct PassthroughResponseState {
    headers: SmallVec<[PassthroughHeader; INLINE_PASSTHROUGH_HEADERS]>,
    header_bytes: usize,
    status: Option<StatusCode>,
}

impl PassthroughResponseState {
    #[doc(hidden)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            headers: SmallVec::new(),
            header_bytes: 0,
            status: None,
        }
    }

    fn stage_header(
        &mut self,
        mode: PassthroughHeaderMode,
        name: &str,
        value: &str,
        typed_cookie: bool,
    ) -> Result<(), PassthroughResponseError> {
        let wire_bytes = validate_response_header_input(name, value)?;
        if name.eq_ignore_ascii_case("set-cookie") && !typed_cookie {
            return Err(PassthroughResponseError::UntypedCookieHeader);
        }
        if is_body_coupled_representation_header(name) {
            return Err(PassthroughResponseError::RepresentationHeader);
        }

        let mut removed_count = 0usize;
        let mut removed_bytes = 0usize;
        if mode == PassthroughHeaderMode::Insert {
            for header in &self.headers {
                if header.name.eq_ignore_ascii_case(name) {
                    removed_count += 1;
                    removed_bytes = removed_bytes.checked_add(header.wire_bytes).ok_or(
                        ResponseHeaderError::HeadersTooLarge {
                            limit_bytes: HARD_MAX_RESPONSE_HEADER_BYTES,
                        },
                    )?;
                }
            }
        } else if is_singleton_response_header_name(name)
            && self
                .headers
                .iter()
                .any(|header| header.name.eq_ignore_ascii_case(name))
        {
            return Err(ResponseHeaderError::DuplicateSingleton.into());
        }

        let next_count = self
            .headers
            .len()
            .checked_sub(removed_count)
            .and_then(|count| count.checked_add(1))
            .ok_or(ResponseHeaderError::TooManyHeaders {
                limit: HARD_MAX_RESPONSE_HEADERS,
            })?;
        if next_count > HARD_MAX_RESPONSE_HEADERS {
            return Err(ResponseHeaderError::TooManyHeaders {
                limit: HARD_MAX_RESPONSE_HEADERS,
            }
            .into());
        }
        let next_bytes = self
            .header_bytes
            .checked_sub(removed_bytes)
            .and_then(|bytes| bytes.checked_add(wire_bytes))
            .ok_or(ResponseHeaderError::HeadersTooLarge {
                limit_bytes: HARD_MAX_RESPONSE_HEADER_BYTES,
            })?;
        if next_bytes > HARD_MAX_RESPONSE_HEADER_BYTES {
            return Err(ResponseHeaderError::HeadersTooLarge {
                limit_bytes: HARD_MAX_RESPONSE_HEADER_BYTES,
            }
            .into());
        }

        if removed_count != 0 {
            self.headers
                .retain(|header| !header.name.eq_ignore_ascii_case(name));
        }
        self.headers.push(PassthroughHeader {
            mode,
            name: name.to_owned(),
            value: value.to_owned(),
            wire_bytes,
        });
        self.header_bytes = next_bytes;
        Ok(())
    }

    fn apply(
        self,
        response: &mut Response,
        outcome: ResponseWriteOutcome,
    ) -> Result<(), PassthroughResponseError> {
        let Some(status_authority) = outcome.status_authority() else {
            return Err(PassthroughResponseError::ResponseNotMaterialized);
        };
        // Error values own their response even when an application deliberately
        // selects a 2xx/3xx status. Direct HTTP error statuses are also terminal.
        if outcome.is_error_response() || response.status_code_value() >= 400 {
            return Ok(());
        }
        if let Some(status) = self.status {
            if status_authority == ResponseStatusAuthority::Authoritative {
                return Err(PassthroughResponseError::AuthoritativeStatus);
            }
            response.status(
                status.as_u16(),
                status.canonical_reason().unwrap_or("Successful"),
            );
        }

        for header in self.headers {
            match header.mode {
                PassthroughHeaderMode::Insert => {
                    response.try_insert_header(&header.name, &header.value)?;
                }
                PassthroughHeaderMode::Append => {
                    response.try_append_header(&header.name, &header.value)?;
                }
            }
        }
        Ok(())
    }
}

impl Default for PassthroughResponseState {
    fn default() -> Self {
        Self::new()
    }
}

/// Stages response metadata while leaving typed response-body ownership to Lily.
///
/// The context deliberately exposes no body mutation API. Staged values are
/// committed only after the action's typed return value has been materialized
/// successfully.
pub struct PassthroughResponseContext<'a> {
    state: &'a mut PassthroughResponseState,
}

impl std::fmt::Debug for PassthroughResponseContext<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PassthroughResponseContext")
            .finish_non_exhaustive()
    }
}

impl<'a> PassthroughResponseContext<'a> {
    #[doc(hidden)]
    #[must_use]
    pub fn new(state: &'a mut PassthroughResponseState) -> Self {
        Self { state }
    }

    /// Inserts one application header, replacing staged/final values with the
    /// same case-insensitive name.
    pub fn insert_header(
        &mut self,
        name: &str,
        value: &str,
    ) -> Result<&mut Self, PassthroughResponseError> {
        self.state
            .stage_header(PassthroughHeaderMode::Insert, name, value, false)?;
        Ok(self)
    }

    /// Appends one application header. Duplicate singleton fields fail closed.
    pub fn append_header(
        &mut self,
        name: &str,
        value: &str,
    ) -> Result<&mut Self, PassthroughResponseError> {
        self.state
            .stage_header(PassthroughHeaderMode::Append, name, value, false)?;
        Ok(self)
    }

    /// Stages one typed response cookie.
    pub fn set_cookie(
        &mut self,
        cookie: &ResponseCookie,
    ) -> Result<&mut Self, PassthroughResponseError> {
        let value = cookie.to_header_value()?;
        self.state
            .stage_header(PassthroughHeaderMode::Append, "Set-Cookie", &value, true)?;
        Ok(self)
    }

    /// Stages one typed cookie signed with the key ring's primary key.
    pub fn set_signed_cookie(
        &mut self,
        cookie: &ResponseCookie,
        key_ring: &CookieKeyRing,
    ) -> Result<&mut Self, PassthroughResponseError> {
        let value = cookie.to_signed_header_value(key_ring)?;
        self.state
            .stage_header(PassthroughHeaderMode::Append, "Set-Cookie", &value, true)?;
        Ok(self)
    }

    /// Stages one encrypted and authenticated typed cookie.
    pub fn set_private_cookie(
        &mut self,
        cookie: &ResponseCookie,
        key_ring: &CookieKeyRing,
    ) -> Result<&mut Self, PassthroughResponseError> {
        let value = cookie.to_private_header_value(key_ring)?;
        self.state
            .stage_header(PassthroughHeaderMode::Append, "Set-Cookie", &value, true)?;
        Ok(self)
    }

    /// Stages a typed cookie removal field.
    pub fn delete_cookie(
        &mut self,
        removal: &CookieRemoval,
    ) -> Result<&mut Self, PassthroughResponseError> {
        let value = removal.to_header_value()?;
        self.state
            .stage_header(PassthroughHeaderMode::Append, "Set-Cookie", &value, true)?;
        Ok(self)
    }

    /// Stages a signed-cookie removal. Removal fields carry no secret payload.
    pub fn delete_signed_cookie(
        &mut self,
        removal: &CookieRemoval,
    ) -> Result<&mut Self, PassthroughResponseError> {
        self.delete_cookie(removal)
    }

    /// Stages a private-cookie removal. Removal fields carry no secret payload.
    pub fn delete_private_cookie(
        &mut self,
        removal: &CookieRemoval,
    ) -> Result<&mut Self, PassthroughResponseError> {
        self.delete_cookie(removal)
    }

    /// Overrides an overrideable typed response's default success status.
    ///
    /// Informational, redirect, error, body-forbidden, and representation-
    /// specific (`206`/`226`) statuses are rejected because this context
    /// composes with a complete typed success body.
    pub fn status(&mut self, code: u16) -> Result<&mut Self, PassthroughResponseError> {
        let status = StatusCode::from_u16(code)
            .ok()
            .filter(StatusCode::is_success)
            .filter(|status| !matches!(status.as_u16(), 204 | 205 | 206 | 226))
            .ok_or(PassthroughResponseError::InvalidStatus)?;
        self.state.status = Some(status);
        Ok(self)
    }
}

fn is_body_coupled_representation_header(name: &str) -> bool {
    [
        "accept-ranges",
        "content-encoding",
        "content-length",
        "content-range",
        "content-type",
        "etag",
        "last-modified",
    ]
    .iter()
    .any(|forbidden| name.eq_ignore_ascii_case(forbidden))
}

/// Materializes a typed response and passthrough metadata into an isolated
/// candidate, then commits the complete response in one move.
#[doc(hidden)]
pub async fn write_passthrough_response<T>(
    value: T,
    state: PassthroughResponseState,
    response: &mut Response,
    request: &mut Request,
) -> Result<ResponseWriteOutcome, ResponseWriteError>
where
    T: IntoResponse,
{
    let mut candidate = Response::from_limits(response.limits());
    let outcome = value.write_to_response(&mut candidate, request).await?;
    if let Some(error) = candidate.body_failure() {
        return Err(error.into());
    }
    state.apply(&mut candidate, outcome)?;
    *response = candidate;
    Ok(outcome)
}

/// Converts an error value into an isolated response and commits it only after
/// the converter has selected a valid representation within the effective limits.
#[doc(hidden)]
pub async fn write_error_response<E>(
    error: E,
    response: &mut Response,
    request: &mut Request,
) -> Result<ResponseWriteOutcome, ResponseWriteError>
where
    E: IntoResponse + Send,
{
    let mut candidate = Response::from_limits(response.limits());
    let outcome = error.write_to_response(&mut candidate, request).await?;
    if let Some(error) = candidate.body_failure() {
        return Err(error.into());
    }
    let outcome = outcome.into_error_response()?;
    *response = candidate;
    Ok(outcome)
}

#[derive(Serialize)]
struct LocalizedPublicHttpErrorBody<'a> {
    error: bool,
    code: &'static str,
    message: &'a str,
    status: u16,
}

/// Declares whether passthrough metadata may override a materialized success
/// status in a later composition stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResponseStatusAuthority {
    /// The response type selected a default success status that may be safely
    /// replaced by a validated, body-allowed application status.
    Overrideable,
    /// The response type owns its status semantics. Conditional/range,
    /// streaming protocol, explicit builder and body-forbidden statuses use
    /// this authority.
    Authoritative,
}

/// Application-owned classification of an error that was successfully rendered.
///
/// This describes the application result, independently of response construction
/// failures and of the final HTTP status (which middleware may change).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFailureKind {
    /// An expected refusal, such as invalid input, authorization or conflict.
    Rejected,
    /// A technical or unexpected application failure.
    Error,
}

impl ResponseFailureKind {
    /// Returns the bounded telemetry label for this application failure.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Error => "error",
        }
    }
}

/// Describes what a successful [`IntoResponse`] conversion did at the
/// controller boundary.
///
/// `Preserved` is reserved for the unit return contract: the handler/default
/// response remains untouched. `Materialized` means the returned value
/// atomically selected the terminal representation and records whether its
/// status can participate in later passthrough composition.
/// `ErrorMaterialized` retains the error origin after a successful conversion;
/// `ClassifiedErrorMaterialized` additionally carries application classification.
/// Only a failed conversion returns [`ResponseWriteError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResponseWriteOutcome {
    /// The conversion intentionally left the existing response untouched.
    Preserved,
    /// The conversion selected a complete buffered or streaming representation.
    Materialized {
        /// Whether later passthrough composition may replace the success status.
        status_authority: ResponseStatusAuthority,
    },
    /// An error value was successfully written as an HTTP response.
    ErrorMaterialized {
        /// Optional bounded application diagnostic; independent of body fields.
        error_code: Option<HttpErrorCode>,
    },
    /// An explicitly classified application error was successfully rendered.
    ClassifiedErrorMaterialized {
        /// The application's interpretation of the error, independent of HTTP status.
        kind: ResponseFailureKind,
        /// Optional bounded diagnostic; never inferred from response body fields.
        error_code: Option<HttpErrorCode>,
    },
}

impl ResponseWriteOutcome {
    /// Creates a materialized response whose default status may be overridden.
    #[must_use]
    pub const fn overrideable() -> Self {
        Self::Materialized {
            status_authority: ResponseStatusAuthority::Overrideable,
        }
    }

    /// Creates a materialized response whose status belongs to the response type.
    #[must_use]
    pub const fn authoritative() -> Self {
        Self::Materialized {
            status_authority: ResponseStatusAuthority::Authoritative,
        }
    }

    /// Returns the status authority for a materialized response.
    #[must_use]
    pub const fn status_authority(self) -> Option<ResponseStatusAuthority> {
        match self {
            Self::Preserved => None,
            Self::Materialized { status_authority } => Some(status_authority),
            Self::ErrorMaterialized { .. } | Self::ClassifiedErrorMaterialized { .. } => {
                Some(ResponseStatusAuthority::Authoritative)
            }
        }
    }

    /// Marks a successfully written error response with an optional diagnostic.
    ///
    /// This retains the error origin without asserting a technical failure.
    /// HTTP observation infers rejected/error from 4xx/5xx when classification
    /// is absent. Use [`Self::classified_error`] to provide application context.
    #[must_use]
    pub const fn error(error_code: Option<HttpErrorCode>) -> Self {
        Self::ErrorMaterialized { error_code }
    }

    /// Marks a successfully rendered, expected application rejection.
    #[must_use]
    pub const fn rejected(error_code: Option<HttpErrorCode>) -> Self {
        Self::classified_error(ResponseFailureKind::Rejected, error_code)
    }

    /// Marks a rendered error with the application's explicit interpretation.
    ///
    /// No tracing trait is required on the error type. This metadata is retained
    /// through `Result::Err` and passthrough composition even when tracing is off.
    #[must_use]
    pub const fn classified_error(
        kind: ResponseFailureKind,
        error_code: Option<HttpErrorCode>,
    ) -> Self {
        Self::ClassifiedErrorMaterialized { kind, error_code }
    }

    /// Returns explicit application classification, or `None` for legacy outcomes.
    #[must_use]
    pub const fn failure_kind(self) -> Option<ResponseFailureKind> {
        match self {
            Self::ClassifiedErrorMaterialized { kind, .. } => Some(kind),
            _ => None,
        }
    }

    /// Reports whether the materialized value represented an application error.
    #[must_use]
    pub const fn is_error_response(self) -> bool {
        matches!(
            self,
            Self::ErrorMaterialized { .. } | Self::ClassifiedErrorMaterialized { .. }
        )
    }

    /// Returns an explicitly supplied application diagnostic, if any.
    #[must_use]
    pub const fn error_code(self) -> Option<HttpErrorCode> {
        match self {
            Self::ErrorMaterialized { error_code }
            | Self::ClassifiedErrorMaterialized { error_code, .. } => error_code,
            _ => None,
        }
    }

    /// Marks a materialized representation as an error, preserving its code.
    /// An error converter must write a response; unit preservation is invalid.
    pub fn into_error_response(self) -> Result<Self, ResponseWriteError> {
        match self {
            Self::Preserved => Err(ResponseWriteError::InvalidOutcome),
            Self::Materialized { .. } => Ok(Self::error(None)),
            Self::ErrorMaterialized { .. } | Self::ClassifiedErrorMaterialized { .. } => Ok(self),
        }
    }
}

fn commit_buffered_response<'a, I>(
    response: &mut Response,
    status: u16,
    reason: &'static str,
    headers: I,
    body: Vec<u8>,
    status_authority: ResponseStatusAuthority,
) -> Result<ResponseWriteOutcome, ResponseWriteError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    response.replace_buffered(status, reason, headers, body)?;
    Ok(ResponseWriteOutcome::Materialized { status_authority })
}

fn bounded_body_copy(response: &mut Response, body: &[u8]) -> Result<Vec<u8>, ResponseWriteError> {
    use std::io::Write;

    let mut writer = BoundedBodyWriter::new(response.body_budget());
    if writer.write_all(body).is_err() {
        let error = writer
            .failure()
            .unwrap_or(ResponseBodyError::AllocationFailed {
                limit_bytes: response.body_budget().limit_bytes(),
            });
        response.record_body_failure(error);
        return Err(error.into());
    }
    writer.finish().map_err(ResponseWriteError::from)
}

fn serialize_json_bounded<T: Serialize + ?Sized>(
    response: &mut Response,
    value: &T,
) -> Result<Vec<u8>, ResponseWriteError> {
    let mut writer = BoundedBodyWriter::new(response.body_budget());
    let serialization = serde_json::to_writer(&mut writer, value);
    if let Some(error) = writer.failure() {
        response.record_body_failure(error);
        return Err(error.into());
    }
    serialization.map_err(|_| ResponseWriteError::Serialization)?;
    writer.finish().map_err(ResponseWriteError::from)
}

#[async_trait::async_trait]
/// Converts controller return values into one terminal response.
///
/// The handler contract is explicit:
///
/// - `()` and `Result<(), E>::Ok(())` preserve the response already
///   written through `&mut Response`.
/// - `Result<T, E>::Ok(T)` serializes `T` as JSON.
/// - `String`, `&str`, `PlainText`, `Vec<u8>`, `BinaryData`, `Json<T>`, and
///   `ResponseBuilder` materialize their documented representation.
/// - [`Created`] and [`Accepted`] select an authoritative `201` or `202`,
///   with an optional location and a typed JSON body or [`EmptyBody`].
/// - [`StreamingResponse`] transfers one lazy, fallible stream without polling it.
/// - [`crate::SseResponse`] transfers one typed server-sent event stream with
///   canonical headers and encoding.
/// - [`crate::StaticFileResponse`] transfers one capability-confined file
///   representation with typed conditional and range semantics.
/// - `Result<_, E>::Err` delegates to `E: IntoResponse + Send`, replacing any
///   partial response with the error representation and retaining its origin.
///   Only failure to construct that representation returns `ResponseWriteError`.
pub trait IntoResponse {
    /// Writes this value to the response selected for the current request.
    ///
    /// The request lets implementations inspect metadata such as the requested
    /// localization language.
    ///
    /// The returned outcome explicitly distinguishes an already-written
    /// response from a terminal buffered or streaming representation selected
    /// by this conversion.
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError>;
}

#[async_trait::async_trait]
impl IntoResponse for HttpApiError {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        let (status_code, status_text) = self.http_status();
        let error_code = HttpErrorCode::new(self.error_code()).ok();
        let public_body = self.public_body();
        let public_body = LocalizedPublicHttpErrorBody {
            error: public_body.error,
            code: public_body.code,
            message: localized_public_error_message(&self, request),
            status: public_body.status,
        };
        let json_buffer = serialize_json_bounded(response, &public_body)?;

        commit_buffered_response(
            response,
            status_code,
            status_text,
            [("Content-Type", "application/json")],
            json_buffer,
            ResponseStatusAuthority::Authoritative,
        )?;
        Ok(ResponseWriteOutcome::error(error_code))
    }
}

/// Returns the first language range from a localization or Accept-Language
/// header without allocating. Quality parameters are intentionally ignored;
/// clients should place their preferred language first.
#[inline]
fn preferred_language(value: &str) -> &str {
    value
        .split(',')
        .next()
        .unwrap_or(value)
        .split(';')
        .next()
        .unwrap_or(value)
        .trim()
}

#[inline]
fn localized_public_error_message<'request>(
    error: &HttpApiError,
    request: &'request Request,
) -> &'request str {
    let language_header = request
        .header_value("localization")
        .or_else(|| request.header_value("accept-language"));
    let language = language_header
        .map(preferred_language)
        .filter(|value| !value.is_empty())
        .unwrap_or("en");
    let localization_key = error.error_code();

    request
        .localization_catalog()
        .and_then(|catalog| catalog.translate(language, localization_key))
        .unwrap_or_else(|| error.public_message())
}

#[async_trait::async_trait]
/// Implementation for String responses
impl IntoResponse for String {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        commit_buffered_response(
            response,
            200,
            "OK",
            [("Content-Type", "text/plain; charset=utf-8")],
            self.into_bytes(),
            ResponseStatusAuthority::Overrideable,
        )
    }
}

/// Implementation for &str responses
#[async_trait::async_trait]
///
impl IntoResponse for &str {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        let body = bounded_body_copy(response, self.as_bytes())?;
        commit_buffered_response(
            response,
            200,
            "OK",
            [("Content-Type", "text/plain; charset=utf-8")],
            body,
            ResponseStatusAuthority::Overrideable,
        )
    }
}

/// Implementation for bool responses
#[async_trait::async_trait]
impl IntoResponse for bool {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        let json_response = bounded_body_copy(response, if self { b"true" } else { b"false" })?;
        commit_buffered_response(
            response,
            200,
            "OK",
            [("Content-Type", "application/json")],
            json_response,
            ResponseStatusAuthority::Overrideable,
        )
    }
}

/// Implementation for `Vec<u8>` responses (raw bytes)
#[async_trait::async_trait]
///
impl IntoResponse for Vec<u8> {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        commit_buffered_response(
            response,
            200,
            "OK",
            [("Content-Type", "application/octet-stream")],
            self,
            ResponseStatusAuthority::Overrideable,
        )
    }
}

/// Implementation for std::io::Error
#[async_trait::async_trait]
impl IntoResponse for std::io::Error {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        tracing::error!(
            error_kind = ?self.kind(),
            raw_os_error = ?self.raw_os_error(),
            "HTTP handler returned an I/O error"
        );
        HttpApiError::IoError("handler response failed with an I/O error".to_string())
            .write_to_response(response, request)
            .await
    }
}

/// Converts successful JSON values and application-defined errors into responses.
#[async_trait::async_trait]
impl<T, E> IntoResponse for Result<T, E>
where
    T: Serialize + Send,
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            // `Result<(), E>` is the explicit already-written
            // response contract. `type_name` avoids imposing a `'static`
            // bound on otherwise borrowable JSON response values.
            Ok(value) if std::any::type_name::<T>() == std::any::type_name::<()>() => {
                drop(value);
                Ok(ResponseWriteOutcome::Preserved)
            }
            Ok(value) => Json(value).write_to_response(response, request).await,
            Err(err) => write_error_response(err, response, request).await,
        }
    }
}

/// Implementation for unit type (empty 200 OK response)
/// Note: Body should already be set by the handler using response.set_body_vec() or similar methods
#[async_trait::async_trait]
impl IntoResponse for () {
    async fn write_to_response(
        self,
        _response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        Ok(ResponseWriteOutcome::Preserved)
    }
}

/// Explicit successful response with no representation body.
///
/// Unlike `()`, which preserves the current/default response, `NoContent`
/// atomically selects `204 No Content`, removes prior representation headers
/// and commits an empty body. Its status is authoritative.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoContent;

#[async_trait::async_trait]
impl IntoResponse for NoContent {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        commit_buffered_response(
            response,
            204,
            "No Content",
            std::iter::empty::<(&str, &str)>(),
            Vec::new(),
            ResponseStatusAuthority::Authoritative,
        )
    }
}

#[async_trait::async_trait]
impl<E> IntoResponse for Result<NoContent, E>
where
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Ok(value) => value.write_to_response(response, request).await,
            Err(error) => write_error_response(error, response, request).await,
        }
    }
}

/// An absent representation body for [`Created`] and [`Accepted`].
///
/// This marker has no HTTP status of its own. It is distinct from a JSON unit
/// or `None` value, both of which serialize to `null`. Normally it is selected
/// by `Created::empty()` or `Accepted::empty()` and need not be named.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EmptyBody;

fn prepare_location_response(
    response: &Response,
    status: u16,
    reason: &'static str,
    location: Option<&str>,
    json: bool,
) -> Result<Response, ResponseWriteError> {
    if let Some(error) = response.body_failure() {
        return Err(error.into());
    }
    let mut candidate = Response::from_limits(response.limits());
    candidate.status(status, reason);
    if json {
        candidate.try_insert_header("Content-Type", "application/json")?;
    }
    if let Some(location) = location {
        candidate.try_insert_header("Location", location)?;
    }
    Ok(candidate)
}

// Keep the two location-bearing results on the same bounded, atomic write path.
// EmptyBody intentionally does not implement Serialize: empty responses and
// JSON values have distinct type contracts, including in OpenAPI metadata.
macro_rules! location_response {
    ($(#[$meta:meta])* $name:ident, $status:literal, $reason:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name<T = EmptyBody> {
            location: Option<String>,
            value: T,
        }

        impl<T> $name<T> {
            /// Creates a response with a `Location` header.
            ///
            /// Serializable values become JSON; [`EmptyBody`] omits the body.
            /// The location may be a relative or absolute address. Header
            /// validation and effective response limits are checked during
            /// conversion; failures return [`ResponseWriteError`].
            pub fn new(location: impl Into<String>, value: T) -> Self {
                Self::without_location(value).with_location(location)
            }

            /// Creates a response without a `Location` header.
            ///
            /// Serializable values become JSON; [`EmptyBody`] omits the body.
            pub fn without_location(value: T) -> Self {
                Self { location: None, value }
            }

            /// Sets the location while retaining the response's body type.
            pub fn with_location(mut self, location: impl Into<String>) -> Self {
                self.location = Some(location.into());
                self
            }

            /// Returns the configured location, if any.
            pub fn location(&self) -> Option<&str> {
                self.location.as_deref()
            }

            /// Returns the response value, or the empty-body marker.
            pub fn value(&self) -> &T {
                &self.value
            }
        }

        impl $name<EmptyBody> {
            /// Creates an empty response without a `Location` header.
            ///
            /// Use `.with_location(address)` to include a location. The
            /// response retains its status and has no JSON content type.
            pub fn empty() -> Self {
                Self::without_location(EmptyBody)
            }
        }

        #[async_trait::async_trait]
        impl IntoResponse for $name<EmptyBody> {
            async fn write_to_response(
                self,
                response: &mut Response,
                _request: &mut Request,
            ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
                let candidate = prepare_location_response(
                    response, $status, $reason, self.location.as_deref(), false,
                )?;
                *response = candidate;
                Ok(ResponseWriteOutcome::authoritative())
            }
        }

        #[async_trait::async_trait]
        impl<T> IntoResponse for $name<T>
        where
            T: Serialize + Send,
        {
            async fn write_to_response(
                self,
                response: &mut Response,
                _request: &mut Request,
            ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
                // Validate headers before serializing, and publish neither
                // headers nor body until the whole representation is valid.
                let mut candidate = prepare_location_response(
                    response, $status, $reason, self.location.as_deref(), true,
                )?;
                let body = serialize_json_bounded(response, &self.value)?;
                candidate.set_body_vec(body)?;
                *response = candidate;
                Ok(ResponseWriteOutcome::authoritative())
            }
        }

        #[async_trait::async_trait]
        impl<T, E> IntoResponse for Result<$name<T>, E>
        where
            T: Send,
            $name<T>: IntoResponse,
            E: IntoResponse + Send,
        {
            async fn write_to_response(
                self,
                response: &mut Response,
                request: &mut Request,
            ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
                match self {
                    Ok(value) => value.write_to_response(response, request).await,
                    Err(error) => write_error_response(error, response, request).await,
                }
            }
        }
    };
}

location_response! {
    /// An authoritative `201 Created` response for a newly created resource.
    ///
    /// `Created::new(location, value)` serializes `value` as JSON and identifies
    /// the resource through `Location`. `Created::without_location(value)`
    /// omits that header. `Created::empty()` returns a bodyless `201`; add
    /// `.with_location(location)` when the new resource has a separate address.
    /// Both direct returns and `Result<Created<T>, E>` accept these forms.
    Created, 201, "Created"
}

location_response! {
    /// An authoritative `202 Accepted` response for work accepted for processing.
    ///
    /// `Accepted::new(location, value)` serializes `value` as JSON and provides
    /// a status-monitor address in `Location`. `Accepted::without_location(value)`
    /// omits that header. `Accepted::empty()` returns a bodyless `202`; add
    /// `.with_location(location)` to provide a monitor address without a body.
    /// This result does not start a job or guarantee that processing completes.
    /// Both direct returns and `Result<Accepted<T>, E>` accept these forms.
    Accepted, 202, "Accepted"
}

/// Newtype wrapper for plain text responses (text/plain instead of application/json)
pub struct PlainText(pub String);

#[async_trait::async_trait]
impl IntoResponse for PlainText {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        commit_buffered_response(
            response,
            200,
            "OK",
            [("Content-Type", "text/plain; charset=utf-8")],
            self.0.into_bytes(),
            ResponseStatusAuthority::Overrideable,
        )
    }
}

// Plain text results delegate both success and error conversion.
#[async_trait::async_trait]
impl<E> IntoResponse for Result<PlainText, E>
where
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Ok(value) => value.write_to_response(response, request).await,
            Err(err) => write_error_response(err, response, request).await,
        }
    }
}

/// Newtype wrapper for binary data responses (application/octet-stream)
pub struct BinaryData(pub Vec<u8>);

#[async_trait::async_trait]
impl IntoResponse for BinaryData {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        commit_buffered_response(
            response,
            200,
            "OK",
            [("Content-Type", "application/octet-stream")],
            self.0,
            ResponseStatusAuthority::Overrideable,
        )
    }
}

// Binary results delegate both success and error conversion.
#[async_trait::async_trait]
impl<E> IntoResponse for Result<BinaryData, E>
where
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Ok(value) => value.write_to_response(response, request).await,
            Err(err) => write_error_response(err, response, request).await,
        }
    }
}

/// One lazy, fallible response stream returned directly from a controller action.
///
/// Construct this with [`streaming`]. The source is type-erased once on the
/// streaming-only path; buffered responses retain their allocation-free body
/// selection path.
pub struct StreamingResponse {
    body: ResponseBodyStream,
    status_code: u16,
    status_text: &'static str,
    content_type: String,
}

impl std::fmt::Debug for StreamingResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamingResponse")
            .field("status_code", &self.status_code)
            .field("status_text", &self.status_text)
            .field("content_type", &self.content_type)
            .field("body", &self.body)
            .finish()
    }
}

impl StreamingResponse {
    /// Type-erases one application stream without polling it.
    pub fn new<S, E>(source: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: Into<ResponseBodyError> + 'static,
    {
        let source = source.map(|item| item.map_err(Into::into));
        Self {
            body: ResponseBodyStream::new(source),
            status_code: 200,
            status_text: "OK",
            content_type: "application/octet-stream".to_string(),
        }
    }

    /// Overrides the default `200 OK` status metadata.
    #[must_use]
    pub fn status(mut self, code: u16, reason: &'static str) -> Self {
        self.status_code = code;
        self.status_text = reason;
        self
    }

    /// Overrides the default `application/octet-stream` content type.
    ///
    /// Validation occurs atomically at the [`IntoResponse`] boundary.
    #[must_use]
    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Declares the exact representation length without consuming the stream.
    #[must_use]
    pub fn content_length(mut self, length: u64) -> Self {
        self.body.set_exact_length(Some(length));
        self
    }

    /// Narrows the server-authoritative maximum size of one source chunk.
    #[must_use]
    pub fn max_chunk_bytes(mut self, limit_bytes: usize) -> Self {
        self.body.set_max_chunk_bytes(Some(limit_bytes));
        self
    }

    /// Adds or narrows an optional cumulative byte budget for this response.
    #[must_use]
    pub fn max_total_bytes(mut self, limit_bytes: u64) -> Self {
        self.body.set_max_total_bytes(Some(limit_bytes));
        self
    }
}

/// Wraps a fallible byte stream for direct controller return.
///
/// ```
/// use std::convert::Infallible;
///
/// use bytes::Bytes;
/// use lily_web_core::{streaming, StreamingResponse};
///
/// let source = futures::stream::iter([
///     Ok::<_, Infallible>(Bytes::from_static(b"first\n")),
///     Ok(Bytes::from_static(b"second\n")),
/// ]);
/// let response: StreamingResponse = streaming(source)
///     .content_type("application/x-ndjson")
///     .max_chunk_bytes(1024);
/// ```
pub fn streaming<S, E>(source: S) -> StreamingResponse
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Into<ResponseBodyError> + 'static,
{
    StreamingResponse::new(source)
}

#[async_trait::async_trait]
impl IntoResponse for StreamingResponse {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        response.replace_streaming(
            self.status_code,
            self.status_text,
            [("Content-Type", self.content_type.as_str())],
            self.body,
        )?;
        Ok(ResponseWriteOutcome::authoritative())
    }
}

#[async_trait::async_trait]
impl<E> IntoResponse for Result<StreamingResponse, E>
where
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Ok(value) => value.write_to_response(response, request).await,
            Err(error) => write_error_response(error, response, request).await,
        }
    }
}

/// Fluent builder for a complete buffered response representation.
///
/// Header validation failures are retained and returned atomically when the
/// builder reaches [`IntoResponse::write_to_response`].
pub struct ResponseBuilder {
    // Most frequently accessed fields first (hot data)
    status_code: u16,          // 2 bytes, very frequent
    status_text: &'static str, // 16 bytes, frequent

    // Medium frequency fields
    headers: smallvec::SmallVec<[(&'static str, &'static str); 8]>, // Stack-first allocation
    header_count: usize,
    header_bytes: usize,
    header_error: Option<ResponseHeaderError>,

    // Less frequent but potentially large fields last (cold data)
    dynamic_headers: Vec<(String, String)>, // Fallback for dynamic headers
    body: ResponseBuilderBody,
}

type DeferredJsonBody =
    Box<dyn FnOnce(&mut Response) -> Result<Vec<u8>, ResponseWriteError> + Send>;

enum ResponseBuilderBody {
    Empty,
    Bytes(Vec<u8>),
    Text(std::borrow::Cow<'static, str>),
    Json(DeferredJsonBody),
}

impl ResponseBuilder {
    /// Creates a `200 OK` builder with no headers or body.
    pub fn new() -> Self {
        Self {
            status_code: 200,
            status_text: "OK",
            headers: smallvec::SmallVec::new(),
            header_count: 0,
            header_bytes: 0,
            header_error: None,
            dynamic_headers: Vec::new(),
            body: ResponseBuilderBody::Empty,
        }
    }

    /// Selects the response status and reason phrase.
    pub fn status(mut self, code: u16, text: &'static str) -> Self {
        self.status_code = code;
        self.status_text = text;
        self
    }

    /// Adds one application-managed response header.
    ///
    /// The fluent signature is retained for source compatibility. Validation
    /// is performed immediately and the first failure is retained internally;
    /// [`IntoResponse::write_to_response`] then returns that typed, secret-safe
    /// failure before mutating the destination response.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        if self.header_error.is_some() {
            return self;
        }

        // Retain the existing static fast path, but run its final normalized
        // representation through the same validation and accounting path as
        // dynamic input.
        if let Some((name, value)) = static_builder_header(name, value) {
            self.push_static_header(name, value);
        } else {
            self.push_dynamic_header(name, value);
        }
        self
    }

    /// Configures a JSON body.
    ///
    /// Serialization is deferred until the builder reaches a response with an
    /// effective body budget. A failure is returned as a `ResponseWriteError`;
    /// it is never replaced with a successful fallback body.
    pub fn json<T>(mut self, value: T) -> Self
    where
        T: serde::Serialize + Send + 'static,
    {
        self.push_static_header("Content-Type", "application/json");
        if self.header_error.is_some() {
            return self;
        }
        self.body = ResponseBuilderBody::Json(Box::new(move |response| {
            serialize_json_bounded(response, &value)
        }));
        self
    }

    /// Selects already-encoded JSON bytes as the body.
    pub fn json_bytes(mut self, json_bytes: Vec<u8>) -> Self {
        self.push_static_header("Content-Type", "application/json");
        if self.header_error.is_some() {
            return self;
        }
        self.body = ResponseBuilderBody::Bytes(json_bytes);
        self
    }

    /// Selects a UTF-8 plain-text body.
    pub fn text(mut self, text: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        self.push_static_header("Content-Type", "text/plain; charset=utf-8");
        if self.header_error.is_some() {
            return self;
        }
        self.body = ResponseBuilderBody::Text(text.into());
        self
    }

    fn push_static_header(&mut self, name: &'static str, value: &'static str) {
        if self.reserve_header(name, value).is_ok() {
            self.headers.push((name, value));
        }
    }

    fn push_dynamic_header(&mut self, name: &str, value: &str) {
        if self.reserve_header(name, value).is_ok() {
            // Validation and aggregate limits have completed before either
            // caller-controlled string is cloned.
            self.dynamic_headers
                .push((name.to_owned(), value.to_owned()));
        }
    }

    fn reserve_header(&mut self, name: &str, value: &str) -> Result<(), ResponseHeaderError> {
        if let Some(error) = self.header_error {
            return Err(error);
        }

        let (next_count, next_bytes) = match checked_builder_header_totals(
            self.header_count,
            self.header_bytes,
            name.len(),
            value.len(),
            HARD_MAX_RESPONSE_HEADERS,
            HARD_MAX_RESPONSE_HEADER_BYTES,
        ) {
            Ok(totals) => totals,
            Err(error) => {
                self.header_error = Some(error);
                return Err(error);
            }
        };

        if let Err(error) = validate_response_header_input(name, value) {
            self.header_error = Some(error);
            return Err(error);
        }

        if is_singleton_response_header_name(name) && self.contains_header_name(name) {
            self.header_error = Some(ResponseHeaderError::DuplicateSingleton);
            return Err(ResponseHeaderError::DuplicateSingleton);
        }

        self.header_count = next_count;
        self.header_bytes = next_bytes;
        Ok(())
    }

    fn contains_header_name(&self, name: &str) -> bool {
        self.headers
            .iter()
            .any(|(stored_name, _)| stored_name.eq_ignore_ascii_case(name))
            || self
                .dynamic_headers
                .iter()
                .any(|(stored_name, _)| stored_name.eq_ignore_ascii_case(name))
    }
}

fn static_builder_header(name: &str, value: &str) -> Option<(&'static str, &'static str)> {
    match (name, value) {
        ("Content-Type", "application/json") => Some(("Content-Type", "application/json")),
        ("Content-Type", "text/plain") | ("Content-Type", "text/plain; charset=utf-8") => {
            Some(("Content-Type", "text/plain; charset=utf-8"))
        }
        ("Content-Type", "text/html") => Some(("Content-Type", "text/html; charset=utf-8")),
        ("Cache-Control", "no-cache") => Some(("Cache-Control", "no-cache")),
        ("Access-Control-Allow-Origin", "*") => Some(("Access-Control-Allow-Origin", "*")),
        _ => None,
    }
}

fn checked_builder_header_totals(
    current_count: usize,
    current_bytes: usize,
    name_bytes: usize,
    value_bytes: usize,
    max_headers: usize,
    max_bytes: usize,
) -> Result<(usize, usize), ResponseHeaderError> {
    let next_count = current_count
        .checked_add(1)
        .ok_or(ResponseHeaderError::TooManyHeaders { limit: max_headers })?;
    if next_count > max_headers {
        return Err(ResponseHeaderError::TooManyHeaders { limit: max_headers });
    }

    let wire_bytes = name_bytes
        .checked_add(2)
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .and_then(|bytes| bytes.checked_add(2))
        .ok_or(ResponseHeaderError::HeadersTooLarge {
            limit_bytes: max_bytes,
        })?;
    let next_bytes =
        current_bytes
            .checked_add(wire_bytes)
            .ok_or(ResponseHeaderError::HeadersTooLarge {
                limit_bytes: max_bytes,
            })?;
    if wire_bytes > max_bytes || next_bytes > max_bytes {
        return Err(ResponseHeaderError::HeadersTooLarge {
            limit_bytes: max_bytes,
        });
    }

    Ok((next_count, next_bytes))
}

#[async_trait::async_trait]
impl IntoResponse for ResponseBuilder {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        // A fluent call cannot return an error without breaking the existing
        // API, so the first header error is sticky and materialization always
        // fails before mutating the destination response.
        if let Some(error) = self.header_error {
            return Err(error.into());
        }
        debug_assert_eq!(
            self.header_count,
            self.headers.len() + self.dynamic_headers.len()
        );
        debug_assert!(self.header_count <= HARD_MAX_RESPONSE_HEADERS);
        debug_assert!(self.header_bytes <= HARD_MAX_RESPONSE_HEADER_BYTES);

        let headers = self.headers.iter().copied().chain(
            self.dynamic_headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        );
        let body = match self.body {
            ResponseBuilderBody::Empty => Vec::new(),
            ResponseBuilderBody::Bytes(body) => body,
            ResponseBuilderBody::Text(body) => bounded_body_copy(response, body.as_bytes())?,
            ResponseBuilderBody::Json(serialize) => serialize(response)?,
        };
        commit_buffered_response(
            response,
            self.status_code,
            self.status_text,
            headers,
            body,
            ResponseStatusAuthority::Authoritative,
        )
    }
}

impl Default for ResponseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl<E> IntoResponse for Result<ResponseBuilder, E>
where
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Ok(value) => value.write_to_response(response, request).await,
            Err(error) => write_error_response(error, response, request).await,
        }
    }
}

/// Explicit JSON response wrapper.
pub struct Json<T>(pub T);

/// Wraps a serializable value as an explicit JSON response.
pub fn json<T: Serialize>(value: T) -> Json<T> {
    Json(value)
}

#[async_trait::async_trait]
impl<T> IntoResponse for Json<T>
where
    T: Serialize + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        let buf = serialize_json_bounded(response, &self.0)?;

        commit_buffered_response(
            response,
            200,
            "OK",
            [("Content-Type", "application/json")],
            buf,
            ResponseStatusAuthority::Overrideable,
        )
    }
}

#[async_trait::async_trait]
impl<T, E> IntoResponse for Result<Json<T>, E>
where
    T: Serialize + Send,
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Ok(value) => value.write_to_response(response, request).await,
            Err(error) => write_error_response(error, response, request).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BodyBudget, ResponseLimits};
    use lily_error::LocalizationCatalog;
    use serde::Serializer;
    use std::{
        collections::HashMap,
        convert::Infallible,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    // Deliberately Send but not Sync, and neither Serialize nor std::error::Error.
    struct ApplicationError {
        status: std::cell::Cell<u16>,
        code: Option<HttpErrorCode>,
    }

    impl ApplicationError {
        fn not_found() -> Self {
            Self {
                status: std::cell::Cell::new(404),
                code: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl IntoResponse for ApplicationError {
        async fn write_to_response(
            self,
            response: &mut Response,
            request: &mut Request,
        ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
            let status = self.status.get();
            let outcome = ResponseBuilder::new()
                .status(status, if status == 200 { "OK" } else { "Not Found" })
                .header("X-Application-Error", "custom")
                .json(serde_json::json!({"detail": "custom error"}))
                .write_to_response(response, request)
                .await?;
            Ok(match self.code {
                Some(code) => ResponseWriteOutcome::error(Some(code)),
                None => outcome,
            })
        }
    }

    #[tokio::test]
    async fn one_application_error_converter_serves_every_result_shape() {
        macro_rules! check {
            ($success:ty) => {{
                let mut response = Response::from_limits(ResponseLimits::default());
                let mut request = Request::new_test("GET", "/custom-error");
                let result: Result<$success, ApplicationError> = Err(ApplicationError::not_found());
                let outcome = result
                    .write_to_response(&mut response, &mut request)
                    .await
                    .unwrap();
                assert_eq!(outcome, ResponseWriteOutcome::error(None));
                assert_eq!(response.status_code_value(), 404);
                assert_eq!(response.header("x-application-error"), Some("custom"));
                assert_eq!(
                    response.into_transport_parts().unwrap().body.as_ref(),
                    br#"{"detail":"custom error"}"#
                );
            }};
        }
        check!(serde_json::Value);
        check!(());
        check!(Json<serde_json::Value>);
        check!(NoContent);
        check!(Created);
        check!(Created<serde_json::Value>);
        check!(Accepted);
        check!(Accepted<serde_json::Value>);
        check!(PlainText);
        check!(BinaryData);
        check!(ResponseBuilder);
        check!(StreamingResponse);
        check!(crate::SseResponse);
        check!(crate::StaticFileResponse);
    }

    struct ClassifiedApplicationError(ResponseFailureKind);

    #[async_trait::async_trait]
    impl IntoResponse for ClassifiedApplicationError {
        async fn write_to_response(
            self,
            response: &mut Response,
            request: &mut Request,
        ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
            // Deliberately use 200: application classification is independent
            // of the HTTP transport status, and must survive Result adapters.
            ResponseBuilder::new()
                .json(serde_json::json!({"failure": true}))
                .write_to_response(response, request)
                .await?;
            Ok(ResponseWriteOutcome::classified_error(
                self.0,
                Some(HttpErrorCode::INTERNAL),
            ))
        }
    }

    #[tokio::test]
    async fn explicit_failure_metadata_survives_every_result_adapter() {
        for kind in [ResponseFailureKind::Rejected, ResponseFailureKind::Error] {
            macro_rules! check {
                ($success:ty) => {{
                    let mut response = Response::from_limits(ResponseLimits::default());
                    response.status(201, "Created");
                    response.try_insert_header("X-Partial", "discard").unwrap();
                    response.write_body(b"discard").unwrap();
                    let mut request = Request::new_test("GET", "/classified-error");
                    let result: Result<$success, ClassifiedApplicationError> =
                        Err(ClassifiedApplicationError(kind));
                    let outcome = result
                        .write_to_response(&mut response, &mut request)
                        .await
                        .unwrap();
                    assert_eq!(
                        outcome,
                        ResponseWriteOutcome::classified_error(kind, Some(HttpErrorCode::INTERNAL))
                    );
                    assert!(outcome.is_error_response());
                    assert_eq!(outcome.failure_kind(), Some(kind));
                    assert_eq!(
                        outcome.status_authority(),
                        Some(ResponseStatusAuthority::Authoritative)
                    );
                    assert_eq!(response.status_code_value(), 200);
                    assert_eq!(response.header("x-partial"), None);
                    assert_eq!(
                        response.into_transport_parts().unwrap().body.as_ref(),
                        br#"{"failure":true}"#
                    );
                }};
            }
            check!(serde_json::Value);
            check!(());
            check!(Json<serde_json::Value>);
            check!(NoContent);
            check!(Created);
            check!(Created<serde_json::Value>);
            check!(Accepted);
            check!(Accepted<serde_json::Value>);
            check!(PlainText);
            check!(BinaryData);
            check!(ResponseBuilder);
            check!(StreamingResponse);
            check!(crate::SseResponse);
            check!(crate::StaticFileResponse);
        }
        assert_eq!(ResponseWriteOutcome::error(None).failure_kind(), None);
        assert_eq!(
            ResponseWriteOutcome::rejected(None).failure_kind(),
            Some(ResponseFailureKind::Rejected)
        );
    }

    async fn assert_location_result(
        value: impl IntoResponse,
        status: u16,
        location: Option<&str>,
        body: Option<&[u8]>,
    ) {
        let mut response = Response::from_limits(ResponseLimits::default());
        response.status(203, "Non-Authoritative Information");
        response
            .try_insert_header("Content-Type", "text/plain")
            .unwrap();
        response.try_insert_header("Location", "/old").unwrap();
        response.try_insert_header("X-Old", "discard").unwrap();
        response.write_body(b"old body").unwrap();
        let mut request = Request::new_test("POST", "/location-result");
        let outcome = value
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();
        assert_eq!(outcome, ResponseWriteOutcome::authoritative());
        assert_eq!(response.status_code_value(), status);
        assert_eq!(response.header("location"), location);
        assert_eq!(
            response.header("content-type"),
            body.map(|_| "application/json")
        );
        assert!(response.header("x-old").is_none());
        assert_eq!(
            response.into_transport_parts().unwrap().body.as_ref(),
            body.unwrap_or_default()
        );
    }

    #[tokio::test]
    async fn location_results_distinguish_json_null_and_absent_bodies() {
        // Borrowed and Send-but-not-Sync DTOs need neither 'static nor Sync.
        #[derive(Serialize)]
        struct View<'a> {
            label: &'a str,
            id: std::cell::Cell<u8>,
        }
        let label = String::from("created");
        macro_rules! check {
            ($name:ident, $status:literal) => {
                let value = $name::new(
                    "/resources/7",
                    View {
                        label: &label,
                        id: std::cell::Cell::new(7),
                    },
                );
                assert_location_result(
                    Result::<_, ApplicationError>::Ok(value),
                    $status,
                    Some("/resources/7"),
                    Some(br#"{"label":"created","id":7}"#),
                )
                .await;
                assert_location_result(
                    $name::without_location("text"),
                    $status,
                    None,
                    Some(br#""text""#),
                )
                .await;
                assert_location_result(
                    $name::without_location(None::<u8>),
                    $status,
                    None,
                    Some(b"null"),
                )
                .await;
                assert_location_result($name::without_location(()), $status, None, Some(b"null"))
                    .await;
                assert_location_result($name::empty(), $status, None, None).await;
                assert_location_result(
                    Result::<_, ApplicationError>::Ok(
                        $name::empty().with_location("https://example.test/jobs/7"),
                    ),
                    $status,
                    Some("https://example.test/jobs/7"),
                    None,
                )
                .await;
            };
        }
        check!(Created, 201);
        check!(Accepted, 202);
    }

    #[tokio::test]
    async fn location_results_validate_headers_before_serialization_and_commit() {
        macro_rules! check {
            ($name:ident) => {
                for (limits, location, expected) in [
                    (
                        ResponseLimits::default(),
                        "/private\r\nX-Injected: yes".to_owned(),
                        ResponseHeaderError::InvalidValue,
                    ),
                    (
                        ResponseLimits::new(BodyBudget::default(), 1, 1024).unwrap(),
                        "/valid".to_owned(),
                        ResponseHeaderError::TooManyHeaders { limit: 1 },
                    ),
                    (
                        ResponseLimits::new(BodyBudget::default(), 8, 64).unwrap(),
                        "x".repeat(64),
                        ResponseHeaderError::HeadersTooLarge { limit_bytes: 64 },
                    ),
                ] {
                    let mut response = Response::from_limits(limits);
                    response.status(203, "Non-Authoritative Information");
                    response.try_insert_header("X-Old", "keep").unwrap();
                    response.write_body(b"old").unwrap();
                    let mut request = Request::new_test("POST", "/invalid-location");
                    let error = $name::new(location, FailingSerialize)
                        .write_to_response(&mut response, &mut request)
                        .await
                        .unwrap_err();
                    assert_eq!(error, ResponseWriteError::Header(expected));
                    assert!(!error.to_string().contains("private"));
                    assert_eq!(response.header("x-old"), Some("keep"));
                    assert!(response.header("location").is_none());
                    let parts = response.into_transport_parts().unwrap();
                    assert_eq!(parts.status, 203);
                    assert_eq!(parts.body.as_ref(), b"old");
                }
            };
        }
        check!(Created);
        check!(Accepted);
    }

    #[tokio::test]
    async fn location_results_keep_serialization_and_body_failures_in_the_write_channel() {
        macro_rules! check {
            ($name:ident) => {
                let mut request = Request::new_test("POST", "/failed-location-body");
                let mut response = Response::from_limits(ResponseLimits::default());
                response.write_body(b"old").unwrap();
                let result: Result<_, ApplicationError> =
                    Ok($name::new("/resource", FailingSerialize));
                assert_eq!(
                    result
                        .write_to_response(&mut response, &mut request)
                        .await
                        .unwrap_err(),
                    ResponseWriteError::Serialization
                );
                assert_eq!(response.status_code_value(), 200);
                assert!(response.header("location").is_none());
                assert_eq!(
                    response.into_transport_parts().unwrap().body.as_ref(),
                    b"old"
                );

                let limits = ResponseLimits::new(BodyBudget::new(4).unwrap(), 8, 1024).unwrap();
                let mut response = Response::from_limits(limits);
                let expected = ResponseBodyError::LimitExceeded { limit_bytes: 4 };
                assert_eq!(
                    $name::new("/resource", "too long")
                        .write_to_response(&mut response, &mut request)
                        .await
                        .unwrap_err(),
                    ResponseWriteError::Body(expected),
                );
                assert_eq!(response.body_failure(), Some(expected));
                assert_eq!(response.status_code_value(), 200);
                assert!(response.header("location").is_none());
                // A later empty success must not hide an ignored body failure.
                assert_eq!(
                    $name::empty()
                        .write_to_response(&mut response, &mut request)
                        .await
                        .unwrap_err(),
                    ResponseWriteError::Body(expected),
                );
            };
        }
        check!(Created);
        check!(Accepted);
    }

    #[tokio::test]
    async fn location_results_keep_status_authority_during_passthrough() {
        macro_rules! check {
            ($name:ident, $status:literal) => {
                let mut request = Request::new_test("POST", "/passthrough-location");
                let mut response = Response::from_limits(ResponseLimits::default());
                let mut state = PassthroughResponseState::new();
                PassthroughResponseContext::new(&mut state)
                    .insert_header("X-Job", "queued")
                    .unwrap();
                let result: Result<_, ApplicationError> = Ok($name::new("/jobs/7", 7));
                let outcome =
                    write_passthrough_response(result, state, &mut response, &mut request)
                        .await
                        .unwrap();
                assert_eq!(outcome, ResponseWriteOutcome::authoritative());
                assert_eq!(response.status_code_value(), $status);
                assert_eq!(response.header("location"), Some("/jobs/7"));
                assert_eq!(response.header("x-job"), Some("queued"));

                let mut state = PassthroughResponseState::new();
                PassthroughResponseContext::new(&mut state)
                    .status(200)
                    .unwrap();
                assert_eq!(
                    write_passthrough_response($name::empty(), state, &mut response, &mut request)
                        .await
                        .unwrap_err(),
                    ResponseWriteError::Passthrough(PassthroughResponseError::AuthoritativeStatus),
                );
                assert_eq!(response.status_code_value(), $status);
                assert_eq!(response.header("location"), Some("/jobs/7"));

                let mut state = PassthroughResponseState::new();
                PassthroughResponseContext::new(&mut state)
                    .insert_header("Location", "/success-only")
                    .unwrap();
                let result: Result<$name<()>, _> = Err(ApplicationError::not_found());
                let outcome =
                    write_passthrough_response(result, state, &mut response, &mut request)
                        .await
                        .unwrap();
                assert_eq!(outcome, ResponseWriteOutcome::error(None));
                assert_eq!(response.status_code_value(), 404);
                assert!(response.header("location").is_none());
            };
        }
        check!(Created, 201);
        check!(Accepted, 202);
    }

    struct BorrowedError<'a>(&'a str);

    #[async_trait::async_trait]
    impl IntoResponse for BorrowedError<'_> {
        async fn write_to_response(
            self,
            response: &mut Response,
            request: &mut Request,
        ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
            self.0.write_to_response(response, request).await
        }
    }

    #[tokio::test]
    async fn result_conversion_accepts_borrowed_values_and_errors_without_sync() {
        #[derive(Serialize)]
        struct BorrowedView<'a> {
            value: &'a str,
        }
        let owned = String::from("borrowed");
        let mut response = Response::from_limits(ResponseLimits::default());
        let mut request = Request::new_test("GET", "/borrowed");
        let result: Result<_, ApplicationError> = Ok(BorrowedView { value: &owned });
        assert_eq!(
            result
                .write_to_response(&mut response, &mut request)
                .await
                .unwrap(),
            ResponseWriteOutcome::overrideable()
        );
        let result: Result<(), _> = Err(BorrowedError(&owned));
        assert_eq!(
            result
                .write_to_response(&mut response, &mut request)
                .await
                .unwrap(),
            ResponseWriteOutcome::error(None)
        );
        assert_eq!(
            response.into_transport_parts().unwrap().body.as_ref(),
            b"borrowed"
        );
    }

    #[tokio::test]
    async fn error_conversion_replaces_poisoned_body_but_keeps_effective_limits() {
        let limits = ResponseLimits::new(BodyBudget::new(64).unwrap(), 8, 1024).unwrap();
        let mut response = Response::from_limits(limits);
        let mut request = Request::new_test("GET", "/poisoned");
        response.try_insert_header("X-Partial", "discard").unwrap();
        assert!(response.write_body(&[b'x'; 65]).is_err());
        let error = ApplicationError {
            code: Some(HttpErrorCode::new("CUSTOM_NOT_FOUND").unwrap()),
            ..ApplicationError::not_found()
        };
        let outcome = Result::<(), _>::Err(error)
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();
        assert_eq!(outcome.error_code().unwrap().as_str(), "CUSTOM_NOT_FOUND");
        assert_eq!(response.limits(), limits);
        assert_eq!(response.body_failure(), None);
        assert_eq!(response.header("x-partial"), None);

        let limits = ResponseLimits::new(BodyBudget::new(8).unwrap(), 8, 1024).unwrap();
        let mut response = Response::from_limits(limits);
        let error = Result::<(), _>::Err(ApplicationError::not_found())
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap_err();
        assert_eq!(
            error,
            ResponseWriteError::Body(ResponseBodyError::LimitExceeded { limit_bytes: 8 })
        );
        assert_eq!(response.limits(), limits);
        assert!(response.into_transport_parts().unwrap().body.is_empty());
    }

    enum BrokenError {
        Preserved,
        Poisoned,
        Serialization,
        Header,
    }

    #[async_trait::async_trait]
    impl IntoResponse for BrokenError {
        async fn write_to_response(
            self,
            response: &mut Response,
            request: &mut Request,
        ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
            match self {
                Self::Preserved => Ok(ResponseWriteOutcome::Preserved),
                Self::Poisoned => {
                    let _ = response.write_body(&[b'x'; 65]);
                    Ok(ResponseWriteOutcome::overrideable())
                }
                Self::Serialization => {
                    Json(FailingSerialize)
                        .write_to_response(response, request)
                        .await
                }
                Self::Header => {
                    ResponseBuilder::new()
                        .header("X-Invalid", "bad\r\nvalue")
                        .write_to_response(response, request)
                        .await
                }
            }
        }
    }

    #[tokio::test]
    async fn invalid_error_converters_cannot_commit_partial_responses() {
        for error in [
            BrokenError::Preserved,
            BrokenError::Poisoned,
            BrokenError::Serialization,
            BrokenError::Header,
        ] {
            let limits = ResponseLimits::new(BodyBudget::new(64).unwrap(), 8, 1024).unwrap();
            let mut response = Response::from_limits(limits);
            let mut request = Request::new_test("GET", "/broken");
            response
                .try_insert_header("X-Original", "retained")
                .unwrap();
            response.write_body(b"original").unwrap();
            let expected = match error {
                BrokenError::Preserved => ResponseWriteError::InvalidOutcome,
                BrokenError::Poisoned => {
                    ResponseBodyError::LimitExceeded { limit_bytes: 64 }.into()
                }
                BrokenError::Serialization => ResponseWriteError::Serialization,
                BrokenError::Header => ResponseHeaderError::InvalidValue.into(),
            };
            assert_eq!(
                Result::<(), _>::Err(error)
                    .write_to_response(&mut response, &mut request)
                    .await
                    .unwrap_err(),
                expected
            );
            assert_eq!(response.header("x-original"), Some("retained"));
            assert_eq!(
                response.into_transport_parts().unwrap().body.as_ref(),
                b"original"
            );
        }
    }

    #[tokio::test]
    async fn passthrough_does_not_apply_success_metadata_to_a_200_error_value() {
        let mut state = PassthroughResponseState::new();
        let mut context = PassthroughResponseContext::new(&mut state);
        context.status(201).unwrap();
        context.insert_header("X-Staged", "discard").unwrap();
        context
            .set_cookie(&ResponseCookie::new("session", "discard").unwrap())
            .unwrap();
        let error = ApplicationError {
            status: std::cell::Cell::new(200),
            code: None,
        };
        let mut response = Response::from_limits(ResponseLimits::default());
        let mut request = Request::new_test("GET", "/custom-200");
        let outcome = write_passthrough_response(
            Result::<(), _>::Err(error),
            state,
            &mut response,
            &mut request,
        )
        .await
        .unwrap();
        assert_eq!(outcome, ResponseWriteOutcome::error(None));
        assert_eq!(response.status_code_value(), 200);
        assert_eq!(response.header("x-staged"), None);
        assert_eq!(response.header("set-cookie"), None);
    }

    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(serde::ser::Error::custom(
                "credential=secret path=/srv/private",
            ))
        }
    }

    fn expect_builder_header_error(builder: &ResponseBuilder) -> ResponseHeaderError {
        builder
            .header_error
            .expect("response builder unexpectedly accepted an invalid header")
    }

    fn catalog(turkish_message: &str) -> Arc<LocalizationCatalog> {
        Arc::new(
            LocalizationCatalog::from_translations(HashMap::from([
                (
                    "en".to_string(),
                    HashMap::from([(
                        "BAD_REQUEST".to_string(),
                        "The request is invalid.".to_string(),
                    )]),
                ),
                (
                    "tr".to_string(),
                    HashMap::from([("BAD_REQUEST".to_string(), turkish_message.to_string())]),
                ),
            ]))
            .unwrap(),
        )
    }

    fn localized_message(language: &str, catalog: Arc<LocalizationCatalog>) -> String {
        let mut request = Request::new_test("GET", "/test");
        request.add_test_header("Localization", language);
        request.set_localization_catalog(Some(catalog));
        let error = HttpApiError::BadRequest("internal parser detail".to_string());

        localized_public_error_message(&error, &request).to_string()
    }

    #[test]
    fn localizes_http_errors_with_request_catalog_and_language_fallbacks() {
        let catalog = catalog("İstek geçersizdir.");

        assert_eq!(
            localized_message("tr-TR", Arc::clone(&catalog)),
            "İstek geçersizdir."
        );
        assert_eq!(
            localized_message("de", Arc::clone(&catalog)),
            "The request is invalid."
        );

        let mut request = Request::new_test("GET", "/test");
        request.add_test_header("Accept-Language", "tr;q=1.0,en;q=0.8");
        request.set_localization_catalog(Some(catalog));
        let error = HttpApiError::BadRequest("database password=secret".to_string());
        assert_eq!(
            localized_public_error_message(&error, &request),
            "İstek geçersizdir."
        );
    }

    #[test]
    fn disabled_and_distinct_request_catalogs_never_share_global_state() {
        let error = HttpApiError::BadRequest("secret diagnostic".to_string());
        let disabled = Request::new_test("GET", "/disabled");
        assert_eq!(
            localized_public_error_message(&error, &disabled),
            "The request is invalid."
        );

        let first = localized_message("tr", catalog("Birinci uygulama"));
        let second = localized_message("tr", catalog("İkinci uygulama"));
        assert_eq!(first, "Birinci uygulama");
        assert_eq!(second, "İkinci uygulama");
    }

    #[tokio::test]
    async fn localized_unicode_and_control_characters_use_safe_json_encoding() {
        let mut request = Request::new_test("GET", "/unicode");
        request.add_test_header("Accept-Language", "tr-TR");
        request.set_localization_catalog(Some(catalog("Satır 1\nDünya 🌍")));
        let mut response = Response::new().await.unwrap();

        let outcome = HttpApiError::BadRequest("password=must-not-leak".to_string())
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ResponseWriteOutcome::error(HttpErrorCode::new("BAD_REQUEST").ok())
        );
        let body = response.into_transport_parts().unwrap().body;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["message"], "Satır 1\nDünya 🌍");
        assert!(!String::from_utf8_lossy(&body).contains("must-not-leak"));
    }

    #[tokio::test]
    async fn unit_result_preserves_handler_owned_status_headers_and_body() {
        for (status, reason, body) in [
            (201_u16, "Created", b"created".as_slice()),
            (204_u16, "No Content", b"".as_slice()),
        ] {
            let mut request = Request::new_test("POST", "/resource");
            let mut response = Response::new().await.unwrap();
            response.status(status, reason);
            response.try_insert_header("X-Handler", "retained").unwrap();
            response.write_body(body).unwrap();

            let outcome = Result::<(), HttpApiError>::Ok(())
                .write_to_response(&mut response, &mut request)
                .await
                .unwrap();

            assert_eq!(outcome, ResponseWriteOutcome::Preserved);
            let parts = response.into_transport_parts().unwrap();
            assert_eq!(parts.status, status);
            assert_eq!(
                parts.headers,
                vec![("x-handler".to_string(), "retained".to_string())]
            );
            assert_eq!(parts.body.as_ref(), body);
        }
    }

    #[tokio::test]
    async fn no_content_is_an_authoritative_empty_204_not_a_unit_alias() {
        let mut request = Request::new_test("DELETE", "/resource");
        let mut response = Response::new().await.unwrap();
        response.status(201, "Created");
        response
            .try_insert_header("Content-Type", "text/plain")
            .unwrap();
        response.write_body(b"must-be-removed").unwrap();

        let outcome = Result::<NoContent, HttpApiError>::Ok(NoContent)
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();

        assert_eq!(outcome, ResponseWriteOutcome::authoritative());
        assert_eq!(
            outcome.status_authority(),
            Some(ResponseStatusAuthority::Authoritative)
        );
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 204);
        assert!(parts.headers.is_empty());
        assert!(parts.body.is_empty());
        assert!(parts.stream.is_none());
    }

    #[tokio::test]
    async fn passthrough_commits_typed_json_cookie_header_and_status_together() {
        let mut state = PassthroughResponseState::new();
        let cookie = ResponseCookie::new("refresh", "opaque")
            .unwrap()
            .http_only(true)
            .secure(true);
        {
            let mut passthrough = PassthroughResponseContext::new(&mut state);
            passthrough.set_cookie(&cookie).unwrap();
            passthrough.insert_header("X-Resource", "created").unwrap();
            passthrough.status(201).unwrap();
        }
        let mut request = Request::new_test("POST", "/resource");
        let mut response = Response::new().await.unwrap();

        write_passthrough_response(
            Result::<_, HttpApiError>::Ok(serde_json::json!({"id": 42})),
            state,
            &mut response,
            &mut request,
        )
        .await
        .unwrap();

        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 201);
        assert!(parts.headers.iter().any(|(name, value)| {
            name == "set-cookie"
                && value.contains("refresh=opaque")
                && value.contains("HttpOnly")
                && value.contains("Secure")
        }));
        assert!(parts
            .headers
            .iter()
            .any(|(name, value)| name == "x-resource" && value == "created"));
        assert_eq!(parts.body.as_ref(), br#"{"id":42}"#);
    }

    #[tokio::test]
    async fn passthrough_discards_staged_metadata_on_action_or_serialization_failure() {
        for serialization_failure in [false, true] {
            let mut state = PassthroughResponseState::new();
            PassthroughResponseContext::new(&mut state)
                .insert_header("X-Staged", "must-not-commit")
                .unwrap();
            let mut request = Request::new_test("POST", "/resource");
            let mut response = Response::new().await.unwrap();
            response
                .try_insert_header("X-Original", "retained")
                .unwrap();
            if serialization_failure {
                let error = write_passthrough_response(
                    Result::<FailingSerialize, HttpApiError>::Ok(FailingSerialize),
                    state,
                    &mut response,
                    &mut request,
                )
                .await
                .unwrap_err();
                assert_eq!(error, ResponseWriteError::Serialization);
                assert_eq!(response.header("x-original"), Some("retained"));
            } else {
                let outcome = write_passthrough_response(
                    Result::<serde_json::Value, HttpApiError>::Err(HttpApiError::BadRequest(
                        "private diagnostic".to_string(),
                    )),
                    state,
                    &mut response,
                    &mut request,
                )
                .await
                .expect("an application error is successfully materialized");
                assert!(outcome.is_error_response());
                assert_eq!(response.status_code_value(), 400);
                assert_eq!(response.header("x-original"), None);
            }
            assert_eq!(response.header("x-staged"), None);
        }
    }

    #[tokio::test]
    async fn passthrough_final_header_validation_is_atomic() {
        let limits = ResponseLimits::new(BodyBudget::default(), 1, 1024).unwrap();
        let mut response = Response::with_limits(limits).await.unwrap();
        response
            .try_insert_header("X-Original", "retained")
            .unwrap();
        let mut request = Request::new_test("GET", "/bounded");
        let mut state = PassthroughResponseState::new();
        PassthroughResponseContext::new(&mut state)
            .insert_header("X-Staged", "value")
            .unwrap();

        let error = write_passthrough_response(
            Json(serde_json::json!({"ok": true})),
            state,
            &mut response,
            &mut request,
        )
        .await
        .expect_err("typed and staged headers together exceed the effective limit");

        assert!(matches!(
            error,
            ResponseWriteError::Passthrough(PassthroughResponseError::Header(_))
        ));
        assert_eq!(response.header("x-original"), Some("retained"));
        assert_eq!(response.header("x-staged"), None);
        assert_eq!(response.header("content-type"), None);
    }

    #[tokio::test]
    async fn passthrough_final_header_byte_limit_is_atomic() {
        let content_type_bytes =
            validate_response_header_input("Content-Type", "application/json").unwrap();
        let staged_bytes = validate_response_header_input("X-Staged", "value").unwrap();
        let limits = ResponseLimits::new(
            BodyBudget::default(),
            8,
            content_type_bytes + staged_bytes - 1,
        )
        .unwrap();
        let mut response = Response::with_limits(limits).await.unwrap();
        response
            .try_insert_header("X-Original", "retained")
            .unwrap();
        let mut request = Request::new_test("GET", "/bounded-bytes");
        let mut state = PassthroughResponseState::new();
        PassthroughResponseContext::new(&mut state)
            .insert_header("X-Staged", "value")
            .unwrap();

        let error = write_passthrough_response(
            Json(serde_json::json!({"ok": true})),
            state,
            &mut response,
            &mut request,
        )
        .await
        .expect_err("typed and staged header bytes exceed the effective limit");

        assert!(matches!(error, ResponseWriteError::Passthrough(_)));
        assert_eq!(response.header("x-original"), Some("retained"));
        assert_eq!(response.header("x-staged"), None);
        assert_eq!(response.header("content-type"), None);
    }

    #[tokio::test]
    async fn passthrough_rejects_final_duplicate_singleton_without_partial_commit() {
        let mut response = Response::new().await.unwrap();
        response
            .try_insert_header("X-Original", "retained")
            .unwrap();
        let mut request = Request::new_test("GET", "/duplicate");
        let mut state = PassthroughResponseState::new();
        PassthroughResponseContext::new(&mut state)
            .append_header("Location", "/second")
            .unwrap();

        let error = write_passthrough_response(
            ResponseBuilder::new()
                .status(201, "Created")
                .header("Location", "/first")
                .json(serde_json::json!({"id": 1})),
            state,
            &mut response,
            &mut request,
        )
        .await
        .expect_err("duplicate singleton must fail before final commit");

        assert!(matches!(error, ResponseWriteError::Passthrough(_)));
        assert_eq!(response.header("x-original"), Some("retained"));
        assert_eq!(response.header("location"), None);
    }

    #[tokio::test]
    async fn passthrough_preserves_authoritative_no_content_metadata_but_not_status_override() {
        let cookie = ResponseCookie::new("session", "opaque").unwrap();
        let mut state = PassthroughResponseState::new();
        {
            let mut passthrough = PassthroughResponseContext::new(&mut state);
            passthrough.set_cookie(&cookie).unwrap();
            passthrough.append_header("X-Deleted", "true").unwrap();
        }
        let mut request = Request::new_test("DELETE", "/resource");
        let mut response = Response::new().await.unwrap();
        write_passthrough_response(NoContent, state, &mut response, &mut request)
            .await
            .unwrap();
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 204);
        assert!(parts.body.is_empty());
        assert!(parts.headers.iter().any(|(name, _)| name == "set-cookie"));
        assert!(parts
            .headers
            .iter()
            .any(|(name, value)| name == "x-deleted" && value == "true"));

        let mut state = PassthroughResponseState::new();
        PassthroughResponseContext::new(&mut state)
            .status(201)
            .unwrap();
        let mut response = Response::new().await.unwrap();
        response
            .try_insert_header("X-Original", "retained")
            .unwrap();
        let error = write_passthrough_response(NoContent, state, &mut response, &mut request)
            .await
            .expect_err("NoContent owns its 204 status");
        assert!(matches!(error, ResponseWriteError::Passthrough(_)));
        assert_eq!(response.header("x-original"), Some("retained"));
    }

    #[tokio::test]
    async fn passthrough_delegates_every_secure_cookie_write_and_removal_mode() {
        let ring = CookieKeyRing::new(&[0x41; crate::MIN_COOKIE_KEY_BYTES]).unwrap();
        let cookie = ResponseCookie::new("session", "opaque")
            .unwrap()
            .http_only(true);
        let removal = CookieRemoval::new("session").unwrap();
        let mut state = PassthroughResponseState::new();
        {
            let mut passthrough = PassthroughResponseContext::new(&mut state);
            passthrough.set_signed_cookie(&cookie, &ring).unwrap();
            passthrough.set_private_cookie(&cookie, &ring).unwrap();
            passthrough.delete_cookie(&removal).unwrap();
            passthrough.delete_signed_cookie(&removal).unwrap();
            passthrough.delete_private_cookie(&removal).unwrap();
        }
        let mut request = Request::new_test("POST", "/cookies");
        let mut response = Response::new().await.unwrap();
        write_passthrough_response(
            Json(serde_json::json!({"ok": true})),
            state,
            &mut response,
            &mut request,
        )
        .await
        .unwrap();

        let parts = response.into_transport_parts().unwrap();
        let cookies: Vec<_> = parts
            .headers
            .iter()
            .filter(|(name, _)| name == "set-cookie")
            .collect();
        assert_eq!(cookies.len(), 5);
        assert!(cookies
            .iter()
            .skip(2)
            .all(|(_, value)| { value.contains("Max-Age=0") && value.contains("Expires=") }));
    }

    #[tokio::test]
    async fn passthrough_cannot_override_an_authoritative_stream_status() {
        let source = futures::stream::empty::<Result<Bytes, ResponseBodyError>>();
        let mut state = PassthroughResponseState::new();
        PassthroughResponseContext::new(&mut state)
            .status(201)
            .unwrap();
        let mut request = Request::new_test("GET", "/stream");
        let mut response = Response::new().await.unwrap();
        response
            .try_insert_header("X-Original", "retained")
            .unwrap();

        let error = write_passthrough_response(
            streaming(source).content_type("application/x-ndjson"),
            state,
            &mut response,
            &mut request,
        )
        .await
        .expect_err("streaming status authority must be retained");

        assert!(matches!(error, ResponseWriteError::Passthrough(_)));
        assert_eq!(response.header("x-original"), Some("retained"));
        assert_eq!(response.header("content-type"), None);
    }

    #[tokio::test]
    async fn direct_error_materialization_discards_passthrough_success_metadata() {
        let mut state = PassthroughResponseState::new();
        {
            let mut passthrough = PassthroughResponseContext::new(&mut state);
            passthrough
                .insert_header("X-Staged", "must-not-commit")
                .unwrap();
            passthrough.status(201).unwrap();
        }
        let mut request = Request::new_test("GET", "/direct-error");
        let mut response = Response::new().await.unwrap();
        write_passthrough_response(
            HttpApiError::BadRequest("private diagnostic".to_string()),
            state,
            &mut response,
            &mut request,
        )
        .await
        .unwrap();

        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 400);
        assert!(!parts.headers.iter().any(|(name, _)| name == "x-staged"));
    }

    #[test]
    fn passthrough_rejects_untyped_cookie_representation_headers_and_body_forbidden_statuses() {
        let mut state = PassthroughResponseState::new();
        let mut passthrough = PassthroughResponseContext::new(&mut state);
        assert_eq!(
            passthrough
                .append_header("Set-Cookie", "secret=must-not-leak")
                .unwrap_err(),
            PassthroughResponseError::UntypedCookieHeader
        );
        assert_eq!(
            passthrough
                .insert_header("Content-Type", "text/plain")
                .unwrap_err(),
            PassthroughResponseError::RepresentationHeader
        );
        for code in [101, 204, 205, 206, 226, 302, 304, 400, 500] {
            assert_eq!(
                passthrough.status(code).unwrap_err(),
                PassthroughResponseError::InvalidStatus
            );
        }
    }

    #[tokio::test]
    async fn json_response_atomically_replaces_previous_response_state() {
        let mut request = Request::new_test("GET", "/json");
        let mut response = Response::new().await.unwrap();
        response.status(202, "Accepted");
        response.try_insert_header("X-Old", "discarded").unwrap();
        response.write_body(b"old-body").unwrap();

        let outcome = Json(serde_json::json!({"ok": true}))
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();

        assert_eq!(outcome, ResponseWriteOutcome::overrideable());
        assert_eq!(
            outcome.status_authority(),
            Some(ResponseStatusAuthority::Overrideable)
        );
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 200);
        assert_eq!(
            parts.headers,
            vec![("content-type".to_string(), "application/json".to_string())]
        );
        assert_eq!(parts.body.as_ref(), br#"{"ok":true}"#);
    }

    #[tokio::test]
    async fn serialization_failure_is_typed_500_and_keeps_previous_response_atomic() {
        let mut request = Request::new_test("GET", "/serialization-error");
        let mut response = Response::new().await.unwrap();
        response.status(201, "Created");
        response.try_insert_header("X-Old", "retained").unwrap();
        response.write_body(b"retained-body").unwrap();

        let error = Result::<FailingSerialize, HttpApiError>::Ok(FailingSerialize)
            .write_to_response(&mut response, &mut request)
            .await
            .expect_err("serialization must fail before terminal commit");

        assert!(matches!(error, ResponseWriteError::Serialization));
        assert_eq!(HttpApiError::from(error).http_status().0, 500);
        assert_eq!(
            HttpApiError::from(error).error_code(),
            "RESPONSE_ENCODING_ERROR"
        );
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 201);
        assert_eq!(
            parts.headers,
            vec![("x-old".to_string(), "retained".to_string())]
        );
        assert_eq!(parts.body.as_ref(), b"retained-body");
        assert!(!error.to_string().contains("credential=secret"));
        assert!(!error.to_string().contains("/srv/private"));
    }

    #[tokio::test]
    async fn response_builder_serialization_failure_never_becomes_200() {
        let builder = ResponseBuilder::new().json(FailingSerialize);
        let mut request = Request::new_test("GET", "/builder-serialization-error");
        let mut response = Response::new().await.unwrap();

        let error = builder
            .write_to_response(&mut response, &mut request)
            .await
            .expect_err("builder serialization must fail closed");

        assert!(matches!(error, ResponseWriteError::Serialization));
        assert_eq!(HttpApiError::from(error).http_status().0, 500);
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 200);
        assert!(parts.headers.is_empty());
        assert!(parts.body.is_empty());
    }

    #[tokio::test]
    async fn json_serialization_stops_at_the_effective_body_budget() {
        let limits = ResponseLimits::new(BodyBudget::new(9).unwrap(), 8, 1024).unwrap();
        let mut response = Response::with_limits(limits).await.unwrap();
        let mut request = Request::new_test("GET", "/bounded-json");

        let error = Json("12345678")
            .write_to_response(&mut response, &mut request)
            .await
            .expect_err("quoted JSON representation is larger than nine bytes");

        assert!(matches!(
            error,
            ResponseWriteError::Body(ResponseBodyError::LimitExceeded { limit_bytes: 9 })
        ));
        assert_eq!(
            response.body_failure(),
            Some(ResponseBodyError::LimitExceeded { limit_bytes: 9 })
        );
        assert_eq!(
            response.into_transport_parts().unwrap_err(),
            ResponseBodyError::LimitExceeded { limit_bytes: 9 }
        );
    }

    #[tokio::test]
    async fn io_error_becomes_generic_public_500_without_internal_message() {
        let mut request = Request::new_test("GET", "/io-error");
        let mut response = Response::new().await.unwrap();
        let sensitive = "permission denied path=/srv/private/key.pem";

        let outcome = std::io::Error::new(std::io::ErrorKind::PermissionDenied, sensitive)
            .write_to_response(&mut response, &mut request)
            .await
            .expect("an I/O error value is successfully materialized");
        assert_eq!(
            outcome,
            ResponseWriteOutcome::error(HttpErrorCode::new("IO_ERROR").ok())
        );
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 500);
        let body = String::from_utf8(parts.body.to_vec()).unwrap();
        assert!(body.contains("IO_ERROR"));
        assert!(body.contains("An internal server error occurred."));
        assert!(!body.contains("/srv/private"));
        assert!(!body.contains("permission denied"));
    }

    #[test]
    fn parses_first_accept_language_value_without_allocation() {
        assert_eq!(preferred_language("tr;q=1.0,en;q=0.8"), "tr");
        assert_eq!(preferred_language(" de , en;q=0.8"), "de");
    }

    #[test]
    fn dynamic_header_rejection_does_not_clone_or_retain_raw_input() {
        let oversized = "s".repeat(HARD_MAX_RESPONSE_HEADER_BYTES);
        let oversized_builder = ResponseBuilder::new().header("X-Lily-Secret", &oversized);

        assert_eq!(
            expect_builder_header_error(&oversized_builder),
            ResponseHeaderError::HeadersTooLarge {
                limit_bytes: HARD_MAX_RESPONSE_HEADER_BYTES
            }
        );
        assert!(oversized_builder.headers.is_empty());
        assert!(oversized_builder.dynamic_headers.is_empty());
        assert_eq!(oversized_builder.dynamic_headers.capacity(), 0);
        assert_eq!(oversized_builder.header_count, 0);
        assert_eq!(oversized_builder.header_bytes, 0);

        let injected = "credential=lily-secret\r\nX-Injected: yes";
        let invalid_builder = ResponseBuilder::new().header("X-Lily-Secret", injected);
        let error = expect_builder_header_error(&invalid_builder);
        assert_eq!(error, ResponseHeaderError::InvalidValue);
        assert!(invalid_builder.dynamic_headers.is_empty());
        assert_eq!(invalid_builder.dynamic_headers.capacity(), 0);

        let output = format!("{error:?} {error}");
        assert!(!output.contains("lily-secret"));
        assert!(!output.contains("X-Injected"));
    }

    #[test]
    fn conceptual_large_header_attacks_fail_using_only_length_arithmetic() {
        const HUNDRED_MIB: usize = 100 * 1024 * 1024;
        const MILLION_HEADERS: usize = 1_000_000;

        // This helper takes lengths, not buffers. It proves a conceptual
        // 100-MiB value and one-million-field attack are rejected without
        // allocating either payload or a million header entries.
        assert_eq!(
            checked_builder_header_totals(0, 0, 8, HUNDRED_MIB, 64, 1024),
            Err(ResponseHeaderError::HeadersTooLarge { limit_bytes: 1024 })
        );
        assert_eq!(
            checked_builder_header_totals(MILLION_HEADERS, 0, 8, 8, 64, 1024),
            Err(ResponseHeaderError::TooManyHeaders { limit: 64 })
        );
        assert_eq!(
            checked_builder_header_totals(0, usize::MAX, 8, 8, 64, usize::MAX),
            Err(ResponseHeaderError::HeadersTooLarge {
                limit_bytes: usize::MAX
            })
        );
    }

    #[test]
    fn count_overflow_is_sticky_without_cloning_the_rejected_header() {
        let mut builder = ResponseBuilder::new();
        for index in 0..HARD_MAX_RESPONSE_HEADERS {
            builder = builder.header(&format!("x-lily-{index}"), "value");
        }
        let length_before = builder.dynamic_headers.len();
        let capacity_before = builder.dynamic_headers.capacity();
        let allocation_before = builder.dynamic_headers.as_ptr();

        builder = builder.header("x-one-too-many", "credential=must-not-be-cloned");

        assert_eq!(
            expect_builder_header_error(&builder),
            ResponseHeaderError::TooManyHeaders {
                limit: HARD_MAX_RESPONSE_HEADERS
            }
        );
        assert_eq!(builder.dynamic_headers.len(), length_before);
        assert_eq!(builder.dynamic_headers.capacity(), capacity_before);
        assert_eq!(builder.dynamic_headers.as_ptr(), allocation_before);
        assert_eq!(builder.header_count, HARD_MAX_RESPONSE_HEADERS);
    }

    #[test]
    fn static_dynamic_and_implicit_headers_share_accounting_and_duplicate_policy() {
        let builder = ResponseBuilder::new()
            .header("Content-Type", "text/plain")
            .header("Set-Cookie", "first=one")
            .header("set-cookie", "second=two");
        let expected_bytes =
            validate_response_header_input("Content-Type", "text/plain; charset=utf-8").unwrap()
                + validate_response_header_input("Set-Cookie", "first=one").unwrap()
                + validate_response_header_input("set-cookie", "second=two").unwrap();

        assert_eq!(builder.header_count, 3);
        assert_eq!(builder.header_bytes, expected_bytes);
        assert!(builder.header_error.is_none());

        let duplicate = ResponseBuilder::new()
            .header("content-type", "application/custom")
            .json_bytes(vec![1, 2, 3]);
        assert_eq!(
            expect_builder_header_error(&duplicate),
            ResponseHeaderError::DuplicateSingleton
        );
        assert!(matches!(duplicate.body, ResponseBuilderBody::Empty));
    }

    #[tokio::test]
    async fn sticky_header_error_fails_before_response_materialization() {
        let builder = ResponseBuilder::new()
            .status(201, "Created")
            .header("Connection", "credential=lily-secret")
            .text("must not be materialized");
        assert_eq!(
            expect_builder_header_error(&builder),
            ResponseHeaderError::HopByHop
        );

        let mut response = Response::new().await.unwrap();
        response.try_insert_header("X-Retained", "yes").unwrap();
        let mut request = Request::new_test("GET", "/builder-error");

        let error = builder
            .write_to_response(&mut response, &mut request)
            .await
            .expect_err("write must report the sticky header error");
        assert_eq!(response.status_code_value(), 200);
        assert_eq!(response.header("x-retained"), Some("yes"));
        let parts = response.into_transport_parts().unwrap();
        assert!(parts.body.is_empty());
        let output = format!("{error:?} {error}");
        assert!(!output.contains("lily-secret"));
        assert!(!error.to_string().contains("Connection"));
    }

    #[tokio::test]
    async fn valid_repeatable_builder_headers_materialize_without_silent_drop() {
        let builder = ResponseBuilder::new()
            .status(202, "Accepted")
            .header("Set-Cookie", "first=one")
            .header("set-cookie", "second=two")
            .text("accepted");
        let mut response = Response::new().await.unwrap();
        let mut request = Request::new_test("GET", "/builder-success");

        let outcome = builder
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();
        assert_eq!(outcome, ResponseWriteOutcome::authoritative());
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 202);
        assert_eq!(
            parts.headers,
            vec![
                (
                    "content-type".to_string(),
                    "text/plain; charset=utf-8".to_string()
                ),
                ("set-cookie".to_string(), "first=one".to_string()),
                ("set-cookie".to_string(), "second=two".to_string()),
            ]
        );
        assert_eq!(parts.body.as_ref(), b"accepted");
    }

    #[tokio::test]
    async fn streaming_response_is_returned_directly_without_eager_polling() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let source = futures::stream::poll_fn(move |_context| {
            let poll = observed.fetch_add(1, Ordering::AcqRel);
            std::task::Poll::Ready(match poll {
                0 => Some(Ok::<_, Infallible>(Bytes::from_static(b"chunk"))),
                _ => None,
            })
        });
        let value: Result<StreamingResponse, HttpApiError> = Ok(streaming(source)
            .status(202, "Accepted")
            .content_type("application/x-ndjson")
            .content_length(5)
            .max_chunk_bytes(8)
            .max_total_bytes(5));
        let mut response = Response::new().await.unwrap();
        let mut request = Request::new_test("GET", "/stream");

        assert_eq!(
            value
                .write_to_response(&mut response, &mut request)
                .await
                .unwrap(),
            ResponseWriteOutcome::authoritative()
        );
        assert_eq!(polls.load(Ordering::Acquire), 0);
        assert_eq!(response.status_code_value(), 202);
        assert_eq!(
            response.header("content-type"),
            Some("application/x-ndjson")
        );

        let mut parts = response.into_transport_parts().unwrap();
        assert!(parts.body.is_empty());
        assert_eq!(parts.exact_length(), Some(5));
        let mut body = parts.stream.take().expect("streaming body is selected");
        assert_eq!(body.max_chunk_bytes(), 8);
        assert_eq!(body.max_total_bytes(), Some(5));
        assert_eq!(body.next().await.unwrap().unwrap().as_ref(), b"chunk");
        assert_eq!(polls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn invalid_stream_metadata_fails_before_replacing_the_response() {
        let value = streaming(futures::stream::empty::<Result<Bytes, ResponseBodyError>>())
            .content_type("text/plain\r\nx-secret: must-not-leak");
        let mut response = Response::new().await.unwrap();
        response.try_insert_header("X-Retained", "yes").unwrap();
        response.write_body(b"retained").unwrap();
        let mut request = Request::new_test("GET", "/stream-invalid");

        let error = value
            .write_to_response(&mut response, &mut request)
            .await
            .expect_err("invalid metadata must fail atomically");
        assert_eq!(response.header("x-retained"), Some("yes"));
        assert_eq!(
            response.into_transport_parts().unwrap().body.as_ref(),
            b"retained"
        );
        assert!(!format!("{error:?} {error}").contains("must-not-leak"));
    }

    #[tokio::test]
    async fn invalid_stream_limits_fail_before_response_selection() {
        for (value, expected) in [
            (
                streaming(futures::stream::empty::<Result<Bytes, ResponseBodyError>>())
                    .max_chunk_bytes(0),
                ResponseBodyError::InvalidStreamChunkLimit,
            ),
            (
                streaming(futures::stream::empty::<Result<Bytes, ResponseBodyError>>())
                    .max_total_bytes(0),
                ResponseBodyError::InvalidStreamTotalLimit,
            ),
        ] {
            let mut response = Response::new().await.unwrap();
            let mut request = Request::new_test("GET", "/stream-invalid-limit");
            let error = value
                .write_to_response(&mut response, &mut request)
                .await
                .expect_err("invalid stream limits must fail before selection");

            assert_eq!(error, ResponseWriteError::Body(expected));
            assert!(response.into_transport_parts().unwrap().stream.is_none());
        }
    }

    #[tokio::test]
    async fn middleware_can_mutate_stream_headers_but_cannot_materialize_its_body() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let source = futures::stream::poll_fn(move |_context| {
            observed.fetch_add(1, Ordering::AcqRel);
            std::task::Poll::Ready(None::<Result<Bytes, ResponseBodyError>>)
        });
        let mut response = Response::new().await.unwrap();
        let mut request = Request::new_test("GET", "/stream-middleware");

        streaming(source)
            .write_to_response(&mut response, &mut request)
            .await
            .unwrap();
        response
            .try_insert_header("X-Middleware", "observed")
            .unwrap();
        let error = match response.write_body(b"must-not-buffer") {
            Ok(_) => panic!("middleware must not materialize a streaming body"),
            Err(error) => error,
        };
        assert_eq!(error, ResponseBodyError::BodyModeConflict);
        assert_eq!(polls.load(Ordering::Acquire), 0);

        let parts = response.into_transport_parts().unwrap();
        assert_eq!(
            parts.headers,
            vec![
                (
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ),
                ("x-middleware".to_string(), "observed".to_string()),
            ]
        );
        assert!(parts.stream.is_some());
        assert_eq!(polls.load(Ordering::Acquire), 0);
    }
}
