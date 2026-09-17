use std::fmt;
use std::ops::Deref;

use lily_error::application::http_api::request::{MultipartError, RequestError};
use lily_error::application::http_api::HttpApiError;
use lily_injection::Extensions;
use lily_web_core::{MultipartField, Request, RequestExt};

use super::FromRequest;

/// Typed `multipart/form-data` binding produced from one bounded parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartForm<T>(pub T);

impl<T> MultipartForm<T> {
    /// Returns the bound multipart DTO.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// One owned file field from a bounded multipart request.
///
/// `filename` and `content_type` are untrusted sender metadata. Lily never
/// treats either value as a filesystem path, MIME verdict or authorization
/// signal. Applications must validate the payload and choose their own
/// storage name.
pub struct FormFile {
    filename: Option<String>,
    content_type: Option<String>,
    bytes: Vec<u8>,
}

impl FormFile {
    /// Returns the untrusted sender-provided filename metadata.
    #[must_use]
    pub fn filename(&self) -> Option<&str> {
        self.filename.as_deref()
    }

    /// Returns the untrusted sender-provided content-type metadata.
    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// Borrows the exact bounded field payload.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Transfers the exact bounded field payload without copying it.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Returns the payload size in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the payload is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl AsRef<[u8]> for FormFile {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Deref for FormFile {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl fmt::Debug for FormFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FormFile")
            .field("filename_present", &self.filename.is_some())
            .field(
                "filename_bytes",
                &self.filename.as_ref().map_or(0, String::len),
            )
            .field("content_type_present", &self.content_type.is_some())
            .field(
                "content_type_bytes",
                &self.content_type.as_ref().map_or(0, String::len),
            )
            .field("payload_bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Converts ordered multipart fields into one application DTO.
///
/// The `MultipartForm` derive implements this trait for named structs whose
/// fields use `String`, `Option<String>`, `Vec<String>` and the corresponding
/// `FormFile` shapes marked with `#[form_file]`.
#[doc(hidden)]
pub trait FromMultipartForm: Sized {
    /// Consumes all parsed fields without cloning their payload buffers.
    fn from_multipart_form(fields: Vec<MultipartField>) -> Result<Self, MultipartFormRejection>;
}

/// OpenAPI schema contract emitted by Lily's `MultipartForm` derive.
///
/// This is a macro integration seam, not an independent user model. Keeping
/// it beside `FromMultipartForm` makes the same derive field plan authoritative
/// for runtime binding and documentation.
#[doc(hidden)]
pub trait MultipartFormOpenApi {
    fn openapi_schema_name() -> std::borrow::Cow<'static, str>;

    fn openapi_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>;
}

/// Stable, value-redacting failures produced by typed multipart binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MultipartFormRejection {
    /// The request is not a valid `multipart/form-data` representation.
    InvalidMultipart,
    /// The request media type is not `multipart/form-data`.
    UnsupportedMediaType,
    /// A required request body was absent.
    MissingBody,
    /// A required DTO field was absent.
    MissingField,
    /// A scalar DTO field appeared more than once.
    DuplicateField,
    /// The request declared a field absent from the DTO schema.
    UnknownField,
    /// A text field was not valid UTF-8.
    InvalidText,
    /// A configured multipart/request limit was exceeded.
    LimitExceeded,
    /// The body-read deadline expired.
    ReadTimedOut,
    /// The request body transport was interrupted.
    TransportInterrupted,
    /// The bounded body buffer could not be retained.
    BufferUnavailable,
    /// The parser or allocation backend violated an internal invariant.
    InternalInvariant,
}

impl fmt::Display for MultipartFormRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidMultipart => "multipart request representation is invalid",
            Self::UnsupportedMediaType => "request is not multipart/form-data",
            Self::MissingBody => "multipart request body is required",
            Self::MissingField => "a required multipart field is missing",
            Self::DuplicateField => "a scalar multipart field is duplicated",
            Self::UnknownField => "multipart request contains an unknown field",
            Self::InvalidText => "multipart text field is not valid UTF-8",
            Self::LimitExceeded => "multipart request exceeds configured limits",
            Self::ReadTimedOut => "multipart request body read deadline exceeded",
            Self::TransportInterrupted => "multipart request body transport was interrupted",
            Self::BufferUnavailable => "request body buffer is unavailable",
            Self::InternalInvariant => "multipart parser invariant failed",
        })
    }
}

impl std::error::Error for MultipartFormRejection {}

impl From<RequestError> for MultipartFormRejection {
    fn from(error: RequestError) -> Self {
        match error {
            RequestError::NoBody(_) => Self::MissingBody,
            RequestError::BodyTooLarge { .. } => Self::LimitExceeded,
            RequestError::BodyReadTimedOut => Self::ReadTimedOut,
            RequestError::BodyBufferUnavailable => Self::BufferUnavailable,
            RequestError::BodyReadFailed(_) => Self::TransportInterrupted,
            RequestError::InvalidContentType(_) => Self::UnsupportedMediaType,
            RequestError::Multipart(error) if error.is_payload_too_large() => Self::LimitExceeded,
            RequestError::Multipart(MultipartError::WrongContentType) => Self::UnsupportedMediaType,
            RequestError::Multipart(
                MultipartError::AllocationFailed | MultipartError::ParserInvariant,
            ) => Self::InternalInvariant,
            RequestError::Multipart(_) => Self::InvalidMultipart,
            RequestError::InvalidJson(_) | RequestError::InvalidUtf8(_) | RequestError::Form(_) => {
                Self::InvalidMultipart
            }
        }
    }
}

impl From<MultipartFormRejection> for HttpApiError {
    fn from(rejection: MultipartFormRejection) -> Self {
        match rejection {
            MultipartFormRejection::UnsupportedMediaType => {
                Self::UnsupportedMediaType("request is not multipart/form-data".to_string())
            }
            MultipartFormRejection::MissingBody => {
                Self::InvalidRequestBody("multipart request body is required".to_string())
            }
            MultipartFormRejection::LimitExceeded => {
                Self::PayloadTooLarge("multipart request exceeds configured limits".to_string())
            }
            MultipartFormRejection::ReadTimedOut => {
                Self::RequestTimeout("multipart request body read deadline exceeded".to_string())
            }
            MultipartFormRejection::BufferUnavailable
            | MultipartFormRejection::InternalInvariant => {
                Self::InternalError("multipart request processing failed".to_string())
            }
            MultipartFormRejection::TransportInterrupted => {
                Self::MultipartError("multipart request body transport was interrupted".to_string())
            }
            MultipartFormRejection::InvalidMultipart
            | MultipartFormRejection::MissingField
            | MultipartFormRejection::DuplicateField
            | MultipartFormRejection::UnknownField
            | MultipartFormRejection::InvalidText => Self::MultipartError(rejection.to_string()),
        }
    }
}

impl<T> FromRequest for MultipartForm<T>
where
    T: FromMultipartForm + Send,
{
    type Rejection = MultipartFormRejection;

    async fn from_request(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        let multipart = request
            .multipart()
            .await
            .map_err(MultipartFormRejection::from)?;
        T::from_multipart_form(multipart.into_fields()).map(Self)
    }
}

/// Converts one multipart field into text without copying its payload bytes.
#[doc(hidden)]
pub fn multipart_text_field(field: MultipartField) -> Result<String, MultipartFormRejection> {
    String::from_utf8(field.into_data()).map_err(|_| MultipartFormRejection::InvalidText)
}

/// Converts one multipart field into a file without copying its payload.
#[doc(hidden)]
#[must_use]
pub fn multipart_file_field(field: MultipartField) -> FormFile {
    let (_name, filename, content_type, bytes) = field.into_parts();
    FormFile {
        filename,
        content_type,
        bytes,
    }
}

/// Reserves one repeated-field slot with a typed internal failure.
#[doc(hidden)]
pub fn reserve_multipart_slot<T>(values: &mut Vec<T>) -> Result<(), MultipartFormRejection> {
    values
        .try_reserve(1)
        .map_err(|_| MultipartFormRejection::InternalInvariant)
}

#[cfg(test)]
mod tests {
    use lily_error::application::http_api::HttpApiError;

    use super::{FormFile, MultipartFormRejection};

    #[test]
    fn form_file_debug_output_redacts_sender_metadata_and_payload() {
        let file = FormFile {
            filename: Some("sensitive-name.txt".to_string()),
            content_type: Some("application/secret".to_string()),
            bytes: b"sensitive-payload".to_vec(),
        };

        let debug = format!("{file:?}");

        assert!(debug.contains("filename_present: true"));
        assert!(debug.contains("payload_bytes: 17"));
        assert!(!debug.contains("sensitive-name"));
        assert!(!debug.contains("application/secret"));
        assert!(!debug.contains("sensitive-payload"));
    }

    #[test]
    fn typed_rejections_preserve_the_public_http_status_contract() {
        for (rejection, expected_status) in [
            (MultipartFormRejection::InvalidMultipart, 400),
            (MultipartFormRejection::MissingBody, 400),
            (MultipartFormRejection::MissingField, 400),
            (MultipartFormRejection::DuplicateField, 400),
            (MultipartFormRejection::UnknownField, 400),
            (MultipartFormRejection::InvalidText, 400),
            (MultipartFormRejection::UnsupportedMediaType, 415),
            (MultipartFormRejection::LimitExceeded, 413),
            (MultipartFormRejection::ReadTimedOut, 408),
            (MultipartFormRejection::TransportInterrupted, 400),
            (MultipartFormRejection::BufferUnavailable, 500),
            (MultipartFormRejection::InternalInvariant, 500),
        ] {
            let error = HttpApiError::from(rejection);
            assert_eq!(error.http_status().0, expected_status);
        }
    }
}
