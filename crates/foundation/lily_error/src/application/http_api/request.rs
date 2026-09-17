/// Safe, Lily-owned multipart failure categories.
///
/// Dynamic field names, filenames, header values, boundaries and payload
/// bytes are deliberately not retained. This keeps `Debug`, `Display`, error
/// chains and HTTP responses free of upload data.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[non_exhaustive]
pub enum MultipartError {
    /// The request is not `multipart/form-data`.
    WrongContentType,
    /// The multipart content type has no boundary parameter.
    MissingBoundary,
    /// The supplied boundary violates multipart syntax or limits.
    InvalidBoundary,
    /// The multipart byte stream is malformed.
    MalformedBody,
    /// A part has no field-name parameter.
    MissingFieldName,
    /// A part has an empty field name.
    EmptyFieldName,
    /// The complete multipart stream exceeded its byte limit.
    WholeStreamTooLarge {
        /// Configured maximum stream size.
        limit_bytes: usize,
    },
    /// One part exceeded its byte limit.
    FieldTooLarge {
        /// Configured maximum field size.
        limit_bytes: usize,
    },
    /// The request contained more parts than permitted.
    TooManyParts {
        /// Configured maximum part count.
        limit: usize,
    },
    /// Retained multipart metadata exceeded its aggregate byte limit.
    RetainedMetadataTooLarge {
        /// Configured maximum retained metadata size.
        limit_bytes: usize,
    },
    /// A field name exceeded its byte limit.
    FieldNameTooLong {
        /// Configured maximum field-name size.
        limit_bytes: usize,
    },
    /// A filename exceeded its byte limit.
    FilenameTooLong {
        /// Configured maximum filename size.
        limit_bytes: usize,
    },
    /// A part content type exceeded its byte limit.
    ContentTypeTooLong {
        /// Configured maximum content-type size.
        limit_bytes: usize,
    },
    /// Retaining bounded multipart data failed to allocate.
    AllocationFailed,
    /// The parser returned a state forbidden by Lily's adapter contract.
    ParserInvariant,
}

impl MultipartError {
    /// Returns a stable, payload-free diagnostic category.
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::WrongContentType => "MULTIPART_WRONG_CONTENT_TYPE",
            Self::MissingBoundary => "MULTIPART_MISSING_BOUNDARY",
            Self::InvalidBoundary => "MULTIPART_INVALID_BOUNDARY",
            Self::MalformedBody => "MULTIPART_MALFORMED_BODY",
            Self::MissingFieldName => "MULTIPART_MISSING_FIELD_NAME",
            Self::EmptyFieldName => "MULTIPART_EMPTY_FIELD_NAME",
            Self::WholeStreamTooLarge { .. } => "MULTIPART_STREAM_TOO_LARGE",
            Self::FieldTooLarge { .. } => "MULTIPART_FIELD_TOO_LARGE",
            Self::TooManyParts { .. } => "MULTIPART_TOO_MANY_PARTS",
            Self::RetainedMetadataTooLarge { .. } => "MULTIPART_RETAINED_METADATA_TOO_LARGE",
            Self::FieldNameTooLong { .. } => "MULTIPART_FIELD_NAME_TOO_LONG",
            Self::FilenameTooLong { .. } => "MULTIPART_FILENAME_TOO_LONG",
            Self::ContentTypeTooLong { .. } => "MULTIPART_CONTENT_TYPE_TOO_LONG",
            Self::AllocationFailed => "MULTIPART_ALLOCATION_FAILED",
            Self::ParserInvariant => "MULTIPART_PARSER_INVARIANT",
        }
    }

    /// Returns whether the failure should map to HTTP 413.
    #[must_use]
    pub const fn is_payload_too_large(self) -> bool {
        matches!(
            self,
            Self::WholeStreamTooLarge { .. }
                | Self::FieldTooLarge { .. }
                | Self::TooManyParts { .. }
                | Self::RetainedMetadataTooLarge { .. }
                | Self::FieldNameTooLong { .. }
                | Self::FilenameTooLong { .. }
                | Self::ContentTypeTooLong { .. }
        )
    }
}

impl std::fmt::Display for MultipartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongContentType => formatter.write_str("request is not multipart/form-data"),
            Self::MissingBoundary => formatter.write_str("multipart boundary is missing"),
            Self::InvalidBoundary => formatter.write_str("multipart boundary is invalid"),
            Self::MalformedBody => formatter.write_str("multipart body is malformed"),
            Self::MissingFieldName => formatter.write_str("multipart field name is missing"),
            Self::EmptyFieldName => formatter.write_str("multipart field name is empty"),
            Self::WholeStreamTooLarge { limit_bytes } => {
                write!(formatter, "multipart body exceeds {limit_bytes} bytes")
            }
            Self::FieldTooLarge { limit_bytes } => {
                write!(formatter, "multipart field exceeds {limit_bytes} bytes")
            }
            Self::TooManyParts { limit } => {
                write!(formatter, "multipart body exceeds {limit} parts")
            }
            Self::RetainedMetadataTooLarge { limit_bytes } => {
                write!(
                    formatter,
                    "multipart retained metadata exceeds {limit_bytes} bytes"
                )
            }
            Self::FieldNameTooLong { limit_bytes } => {
                write!(
                    formatter,
                    "multipart field name exceeds {limit_bytes} bytes"
                )
            }
            Self::FilenameTooLong { limit_bytes } => {
                write!(formatter, "multipart filename exceeds {limit_bytes} bytes")
            }
            Self::ContentTypeTooLong { limit_bytes } => {
                write!(
                    formatter,
                    "multipart content type exceeds {limit_bytes} bytes"
                )
            }
            Self::AllocationFailed => formatter.write_str("multipart data could not be retained"),
            Self::ParserInvariant => formatter.write_str("multipart parser invariant failed"),
        }
    }
}

impl std::error::Error for MultipartError {}

/// Identifies which side of a URL-encoded form pair failed validation.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FormComponent {
    /// The key/name side of a pair.
    Name,
    /// The value side of a pair.
    Value,
}

impl std::fmt::Display for FormComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Name => "name",
            Self::Value => "value",
        })
    }
}

/// Safe, Lily-owned URL-encoded form failure categories.
///
/// Encoded names and values are deliberately not retained in the error.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[non_exhaustive]
pub enum FormError {
    /// The request is not `application/x-www-form-urlencoded`.
    WrongContentType,
    /// More than one content-type header was supplied.
    DuplicateContentType,
    /// A percent escape is incomplete or contains non-hexadecimal bytes.
    InvalidPercentEncoding {
        /// Zero-based form-pair index.
        pair_index: usize,
        /// Side of the pair containing the invalid escape.
        component: FormComponent,
        /// Byte offset within that encoded component.
        byte_offset: usize,
    },
    /// Percent-decoded bytes are not valid UTF-8.
    InvalidUtf8 {
        /// Zero-based form-pair index.
        pair_index: usize,
        /// Side of the pair containing invalid UTF-8.
        component: FormComponent,
        /// Byte offset within the decoded component.
        byte_offset: usize,
    },
}

impl FormError {
    /// Returns a stable category that excludes submitted form data.
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::WrongContentType => "FORM_WRONG_CONTENT_TYPE",
            Self::DuplicateContentType => "FORM_DUPLICATE_CONTENT_TYPE",
            Self::InvalidPercentEncoding { .. } => "FORM_INVALID_PERCENT_ENCODING",
            Self::InvalidUtf8 { .. } => "FORM_INVALID_UTF8",
        }
    }
}

impl std::fmt::Display for FormError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongContentType => {
                formatter.write_str("request is not application/x-www-form-urlencoded")
            }
            Self::DuplicateContentType => {
                formatter.write_str("request contains more than one content-type header")
            }
            Self::InvalidPercentEncoding {
                pair_index,
                component,
                byte_offset,
            } => write!(
                formatter,
                "form pair {pair_index} {component} has an invalid percent escape at byte {byte_offset}"
            ),
            Self::InvalidUtf8 {
                pair_index,
                component,
                byte_offset,
            } => write!(
                formatter,
                "form pair {pair_index} {component} is not UTF-8 at decoded byte {byte_offset}"
            ),
        }
    }
}

impl std::error::Error for FormError {}

/// Request parsing errors
#[derive(Debug, PartialEq, Clone)]
pub enum RequestError {
    /// JSON decoding failed.
    InvalidJson(String),
    /// Request bytes are not valid UTF-8.
    InvalidUtf8(String),
    /// A required request body is absent.
    NoBody(String),
    /// The body content type is invalid for the requested extractor.
    InvalidContentType(String),
    /// The buffered body exceeded its configured byte limit.
    BodyTooLarge {
        /// Configured maximum body size.
        limit_bytes: usize,
    },
    /// Reading the request body exceeded its deadline.
    BodyReadTimedOut,
    /// The body buffer is unavailable because ownership was already consumed.
    BodyBufferUnavailable,
    /// The transport failed while reading the body.
    BodyReadFailed(String),
    /// URL-encoded form decoding failed.
    Form(FormError),
    /// Multipart decoding failed.
    Multipart(MultipartError),
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestError::InvalidJson(msg) => write!(f, "Invalid JSON: {msg}"),
            RequestError::InvalidUtf8(msg) => write!(f, "Invalid UTF-8: {msg}"),
            RequestError::NoBody(msg) => write!(f, "No request body: {msg}"),
            RequestError::InvalidContentType(msg) => write!(f, "Invalid content type: {msg}"),
            RequestError::BodyTooLarge { limit_bytes } => {
                write!(f, "Request body exceeds {limit_bytes} bytes")
            }
            RequestError::BodyReadTimedOut => f.write_str("Request body read timed out"),
            RequestError::BodyBufferUnavailable => {
                f.write_str("Request body buffer is unavailable")
            }
            RequestError::BodyReadFailed(msg) => write!(f, "Request body read failed: {msg}"),
            RequestError::Form(error) => write!(f, "Form error: {error}"),
            RequestError::Multipart(error) => write!(f, "Multipart error: {error}"),
        }
    }
}

impl std::error::Error for RequestError {}
