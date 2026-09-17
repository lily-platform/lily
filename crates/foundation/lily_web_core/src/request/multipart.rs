//! Bounded `multipart/form-data` compatibility adapter.
//!
//! Wire parsing is delegated to `multer`. All public data and error types are
//! Lily-owned so the implementation dependency does not become part of the
//! framework's semver surface.
//!
//! Multer 3.1 normalizes repeated same-name headers within one part by
//! retaining the final value. Lily exposes that deterministic value only as
//! untrusted metadata and never uses it to classify the exact body bytes.

use std::fmt;

use crate::BodyBudget;
use lily_error::application::http_api::request::MultipartError;
use multer::{Constraints, Multipart, SizeLimit};

pub(crate) const DEFAULT_MAX_PART_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_PARTS: usize = 128;
pub(crate) const DEFAULT_MAX_RETAINED_METADATA_BYTES: usize = 16 * 1024;
const HARD_MAX_FIELD_NAME_BYTES: usize = 256;
const HARD_MAX_FILENAME_BYTES: usize = 1024;
const HARD_MAX_CONTENT_TYPE_BYTES: usize = 256;
const RFC_MAX_BOUNDARY_BYTES: usize = 70;

/// A parsed multipart body in original wire order.
///
/// Repeated field names remain separate entries. Multipart parts are exposed
/// as exact bytes because a sender-provided `filename` parameter does not
/// reliably distinguish text from files.
pub struct MultipartData {
    fields: Vec<MultipartField>,
    total_data_bytes: usize,
}

impl MultipartData {
    #[must_use]
    /// Returns all parsed fields in wire order.
    pub fn fields(&self) -> &[MultipartField] {
        &self.fields
    }

    /// Consumes the parsed body and returns its fields without copying their
    /// bounded payload buffers.
    #[must_use]
    pub fn into_fields(self) -> Vec<MultipartField> {
        self.fields
    }

    #[must_use]
    /// Returns the field count, including repeated names.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    #[must_use]
    /// Returns `true` when the body contained no parts.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    #[must_use]
    /// Returns the aggregate retained payload bytes.
    pub fn total_data_bytes(&self) -> usize {
        self.total_data_bytes
    }

    /// Iterates over fields whose case-sensitive name equals `name`.
    pub fn named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a MultipartField> + 'a {
        self.fields.iter().filter(move |field| field.name == name)
    }
}

impl fmt::Debug for MultipartData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let filename_metadata_count = self
            .fields
            .iter()
            .filter(|field| field.filename.is_some())
            .count();
        formatter
            .debug_struct("MultipartData")
            .field("field_count", &self.fields.len())
            .field("filename_metadata_count", &filename_metadata_count)
            .field("total_data_bytes", &self.total_data_bytes)
            .finish()
    }
}

/// One multipart part retained as exact, bounded bytes.
///
/// `filename` and `content_type` are optional, untrusted sender metadata.
/// Their presence or value must never be used as a security classification;
/// inspect `data` independently. A filename is never interpreted as a server
/// path, so applications must choose their own storage name after validation.
pub struct MultipartField {
    name: String,
    filename: Option<String>,
    content_type: Option<String>,
    data: Vec<u8>,
}

impl MultipartField {
    #[must_use]
    /// Returns the decoded field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    /// Returns untrusted sender metadata, never a server path or file verdict.
    pub fn filename(&self) -> Option<&str> {
        self.filename.as_deref()
    }

    #[must_use]
    /// Returns untrusted sender metadata, not verified content identification.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    #[must_use]
    /// Returns the exact retained payload bytes.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Consumes the field and returns its exact bytes without copying.
    #[must_use]
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    /// Transfers all retained field components without copying the payload.
    ///
    /// This is a framework-adapter seam. `filename` and `content_type` remain
    /// untrusted sender metadata after ownership is transferred.
    #[doc(hidden)]
    #[must_use]
    pub fn into_parts(self) -> (String, Option<String>, Option<String>, Vec<u8>) {
        (self.name, self.filename, self.content_type, self.data)
    }

    /// Interprets this field as UTF-8 without copying.
    ///
    /// Multipart parsing itself remains bytes-first. Callers opt into text
    /// semantics explicitly, independently of filename metadata.
    pub fn text(&self) -> Result<&str, MultipartTextError> {
        std::str::from_utf8(&self.data).map_err(|_| MultipartTextError::InvalidUtf8)
    }

    #[must_use]
    /// Returns the number of exact payload bytes.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[must_use]
    /// Returns `true` when this part has an empty payload.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl fmt::Debug for MultipartField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MultipartField")
            .field("name_bytes", &self.name.len())
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
            .field("data_bytes", &self.data.len())
            .finish()
    }
}

/// Safe failure returned by [`MultipartField::text`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MultipartTextError {
    /// The exact part bytes are not valid UTF-8.
    InvalidUtf8,
}

impl fmt::Display for MultipartTextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("multipart field is not valid UTF-8")
    }
}

impl std::error::Error for MultipartTextError {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MultipartLimits {
    whole_stream_bytes: usize,
    per_field_bytes: usize,
    max_parts: usize,
    max_retained_metadata_bytes: usize,
    max_field_name_bytes: usize,
    max_filename_bytes: usize,
    max_content_type_bytes: usize,
}

impl MultipartLimits {
    pub(crate) fn production(
        request_body_budget: BodyBudget,
        max_part_bytes: usize,
        max_parts: usize,
        max_retained_metadata_bytes: usize,
    ) -> Self {
        let whole_stream_bytes = request_body_budget.limit_bytes();
        Self {
            whole_stream_bytes,
            per_field_bytes: whole_stream_bytes.min(max_part_bytes),
            max_parts,
            max_retained_metadata_bytes,
            max_field_name_bytes: HARD_MAX_FIELD_NAME_BYTES,
            max_filename_bytes: HARD_MAX_FILENAME_BYTES,
            max_content_type_bytes: HARD_MAX_CONTENT_TYPE_BYTES,
        }
    }
}

pub(crate) async fn parse_multipart(
    content_type: &str,
    body: &[u8],
    limits: MultipartLimits,
) -> Result<MultipartData, MultipartError> {
    if body.len() > limits.whole_stream_bytes {
        return Err(MultipartError::WholeStreamTooLarge {
            limit_bytes: limits.whole_stream_bytes,
        });
    }

    let boundary = parse_and_validate_boundary(content_type)?;
    let constraints = Constraints::new().size_limit(
        SizeLimit::new()
            .whole_stream(limits.whole_stream_bytes as u64)
            .per_field(limits.per_field_bytes as u64),
    );
    let mut multipart = Multipart::with_reader_with_constraints(body, boundary, constraints);

    let mut fields = Vec::new();
    let mut total_data_bytes = 0_usize;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| map_multer_error(error, limits))?
    {
        if fields.len() >= limits.max_parts {
            return Err(MultipartError::TooManyParts {
                limit: limits.max_parts,
            });
        }
        let disposition = validate_part_headers(&field, limits)?;
        let name = disposition.name;
        let filename = disposition.filename;
        let content_type = exact_content_type(&field, limits)?;
        let data = collect_field(&mut field, limits).await?;
        total_data_bytes = total_data_bytes
            .checked_add(data.len())
            .ok_or(MultipartError::AllocationFailed)?;
        fields
            .try_reserve(1)
            .map_err(|_| MultipartError::AllocationFailed)?;
        fields.push(MultipartField {
            name,
            filename,
            content_type,
            data,
        });
    }

    Ok(MultipartData {
        fields,
        total_data_bytes,
    })
}

pub(crate) fn validate_multipart_content_type(content_type: &str) -> Result<(), MultipartError> {
    parse_and_validate_boundary(content_type).map(drop)
}

fn parse_and_validate_boundary(content_type: &str) -> Result<String, MultipartError> {
    reject_duplicate_boundary_parameters(content_type.as_bytes())?;
    let boundary = multer::parse_boundary(content_type).map_err(|error| match error {
        multer::Error::NoMultipart => MultipartError::WrongContentType,
        multer::Error::NoBoundary => MultipartError::MissingBoundary,
        _ => MultipartError::InvalidBoundary,
    })?;

    let bytes = boundary.as_bytes();
    let valid_boundary_char = |byte: u8| {
        byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'\''
                    | b'('
                    | b')'
                    | b'+'
                    | b'_'
                    | b','
                    | b'-'
                    | b'.'
                    | b'/'
                    | b':'
                    | b'='
                    | b'?'
                    | b' '
            )
    };
    let valid_final_char = |byte: u8| valid_boundary_char(byte) && byte != b' ';
    if bytes.is_empty()
        || bytes.len() > RFC_MAX_BOUNDARY_BYTES
        || !bytes.iter().copied().all(valid_boundary_char)
        || !bytes.last().copied().is_some_and(valid_final_char)
    {
        return Err(MultipartError::InvalidBoundary);
    }
    Ok(boundary)
}

fn reject_duplicate_boundary_parameters(input: &[u8]) -> Result<(), MultipartError> {
    let Some(mut cursor) = input.iter().position(|byte| *byte == b';') else {
        return Ok(());
    };
    let mut boundary_seen = false;
    while cursor < input.len() {
        if input[cursor] != b';' {
            return Err(MultipartError::InvalidBoundary);
        }
        cursor += 1;
        skip_ows(input, &mut cursor);
        let parameter_start = cursor;
        consume_token(input, &mut cursor).map_err(|_| MultipartError::InvalidBoundary)?;
        let parameter_name = &input[parameter_start..cursor];
        skip_ows(input, &mut cursor);
        if input.get(cursor) != Some(&b'=') {
            return Err(MultipartError::InvalidBoundary);
        }
        cursor += 1;
        skip_ows(input, &mut cursor);
        consume_parameter_value(input, &mut cursor).map_err(|_| MultipartError::InvalidBoundary)?;
        skip_ows(input, &mut cursor);
        if parameter_name.eq_ignore_ascii_case(b"boundary") {
            if boundary_seen {
                return Err(MultipartError::InvalidBoundary);
            }
            boundary_seen = true;
        }
    }
    Ok(())
}

struct DispositionMetadata {
    name: String,
    filename: Option<String>,
}

fn validate_part_headers(
    field: &multer::Field<'_>,
    limits: MultipartLimits,
) -> Result<DispositionMetadata, MultipartError> {
    let headers = field.headers();
    // multer enforces its own 32-line wire cap, then deliberately collapses
    // duplicate names into HeaderMap. This cap therefore describes only the
    // metadata retained for Lily; overwritten wire bytes remain covered by
    // the whole-body limit.
    let retained_metadata_bytes = headers.iter().try_fold(0_usize, |total, (name, value)| {
        total
            .checked_add(name.as_str().len())
            .and_then(|total| total.checked_add(value.as_bytes().len()))
            .and_then(|total| total.checked_add(4))
    });
    if retained_metadata_bytes.is_none_or(|bytes| bytes > limits.max_retained_metadata_bytes) {
        return Err(MultipartError::RetainedMetadataTooLarge {
            limit_bytes: limits.max_retained_metadata_bytes,
        });
    }

    headers
        .get("content-disposition")
        .ok_or(MultipartError::MissingFieldName)
        .and_then(|value| parse_content_disposition(value.as_bytes(), limits))
}

fn parse_content_disposition(
    input: &[u8],
    limits: MultipartLimits,
) -> Result<DispositionMetadata, MultipartError> {
    let mut cursor = 0_usize;
    skip_ows(input, &mut cursor);
    let disposition_start = cursor;
    consume_token(input, &mut cursor)?;
    if !input[disposition_start..cursor].eq_ignore_ascii_case(b"form-data") {
        return Err(MultipartError::MalformedBody);
    }
    skip_ows(input, &mut cursor);

    let mut name = None;
    let mut filename = None;
    while cursor < input.len() {
        if input[cursor] != b';' {
            return Err(MultipartError::MalformedBody);
        }
        cursor += 1;
        skip_ows(input, &mut cursor);
        if cursor == input.len() {
            return Err(MultipartError::MalformedBody);
        }

        let parameter_start = cursor;
        consume_token(input, &mut cursor)?;
        let parameter_name = &input[parameter_start..cursor];
        skip_ows(input, &mut cursor);
        if input.get(cursor) != Some(&b'=') {
            return Err(MultipartError::MalformedBody);
        }
        cursor += 1;
        skip_ows(input, &mut cursor);
        let value = consume_parameter_value(input, &mut cursor)?;
        skip_ows(input, &mut cursor);

        if parameter_name.eq_ignore_ascii_case(b"name") {
            if name.is_some() {
                return Err(MultipartError::MalformedBody);
            }
            let value = metadata_string(
                value,
                limits.max_field_name_bytes,
                MultipartError::FieldNameTooLong {
                    limit_bytes: limits.max_field_name_bytes,
                },
            )?;
            if value.is_empty() {
                return Err(MultipartError::EmptyFieldName);
            }
            name = Some(value);
        } else if parameter_name.eq_ignore_ascii_case(b"filename") {
            if filename.is_some() {
                return Err(MultipartError::MalformedBody);
            }
            let value = metadata_string(
                value,
                limits.max_filename_bytes,
                MultipartError::FilenameTooLong {
                    limit_bytes: limits.max_filename_bytes,
                },
            )?;
            filename = Some(value);
        }
    }

    Ok(DispositionMetadata {
        name: name.ok_or(MultipartError::MissingFieldName)?,
        filename,
    })
}

fn skip_ows(input: &[u8], cursor: &mut usize) {
    while input
        .get(*cursor)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        *cursor += 1;
    }
}

fn consume_token(input: &[u8], cursor: &mut usize) -> Result<(), MultipartError> {
    let start = *cursor;
    let consumed = input
        .get(start..)
        .unwrap_or_default()
        .iter()
        .take_while(|byte| is_token(**byte))
        .count();
    if consumed == 0 {
        Err(MultipartError::MalformedBody)
    } else {
        *cursor = start
            .checked_add(consumed)
            .ok_or(MultipartError::MalformedBody)?;
        Ok(())
    }
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

struct ParameterValue<'a> {
    raw: &'a [u8],
    has_escapes: bool,
}

fn consume_parameter_value<'a>(
    input: &'a [u8],
    cursor: &mut usize,
) -> Result<ParameterValue<'a>, MultipartError> {
    if input.get(*cursor) == Some(&b'"') {
        let start = cursor
            .checked_add(1)
            .filter(|start| *start <= input.len())
            .ok_or(MultipartError::MalformedBody)?;
        let mut has_escapes = false;
        let mut escaped = false;
        for (offset, byte) in input[start..].iter().copied().enumerate() {
            if escaped {
                if !is_quoted_value_byte(byte) {
                    return Err(MultipartError::MalformedBody);
                }
                escaped = false;
                continue;
            }
            match byte {
                b'"' => {
                    let end = start
                        .checked_add(offset)
                        .ok_or(MultipartError::MalformedBody)?;
                    *cursor = end.checked_add(1).ok_or(MultipartError::MalformedBody)?;
                    let raw = &input[start..end];
                    return Ok(ParameterValue { raw, has_escapes });
                }
                b'\\' => {
                    has_escapes = true;
                    escaped = true;
                }
                byte if is_quoted_value_byte(byte) => {}
                _ => return Err(MultipartError::MalformedBody),
            }
        }
        Err(MultipartError::MalformedBody)
    } else {
        let start = *cursor;
        let consumed = input
            .get(start..)
            .unwrap_or_default()
            .iter()
            .take_while(|byte| is_token(**byte))
            .count();
        if consumed == 0 {
            return Err(MultipartError::MalformedBody);
        }
        *cursor = start
            .checked_add(consumed)
            .ok_or(MultipartError::MalformedBody)?;
        Ok(ParameterValue {
            raw: &input[start..*cursor],
            has_escapes: false,
        })
    }
}

fn is_quoted_value_byte(byte: u8) -> bool {
    byte == b'\t' || byte == b' ' || (b'!'..=b'~').contains(&byte) || byte >= 0x80
}

fn metadata_string(
    value: ParameterValue<'_>,
    limit_bytes: usize,
    too_large: MultipartError,
) -> Result<String, MultipartError> {
    if !value.has_escapes && value.raw.len() > limit_bytes {
        return Err(too_large);
    }

    let mut decoded = Vec::new();
    decoded
        .try_reserve(value.raw.len().min(limit_bytes.saturating_add(1)))
        .map_err(|_| MultipartError::AllocationFailed)?;
    let mut cursor = 0_usize;
    while cursor < value.raw.len() {
        let byte = value.raw[cursor];
        cursor += 1;
        let byte = if value.has_escapes && byte == b'\\' {
            let escaped = *value.raw.get(cursor).ok_or(MultipartError::MalformedBody)?;
            cursor += 1;
            escaped
        } else {
            byte
        };
        if decoded.len() >= limit_bytes {
            return Err(too_large);
        }
        decoded.push(byte);
    }
    String::from_utf8(decoded).map_err(|_| MultipartError::MalformedBody)
}

fn exact_content_type(
    field: &multer::Field<'_>,
    limits: MultipartLimits,
) -> Result<Option<String>, MultipartError> {
    match field.headers().get("content-type") {
        None => Ok(None),
        Some(header_value) => {
            if field.content_type().is_none() {
                return Err(MultipartError::MalformedBody);
            }
            let value = header_value
                .to_str()
                .map_err(|_| MultipartError::MalformedBody)?;
            if value.len() > limits.max_content_type_bytes {
                return Err(MultipartError::ContentTypeTooLong {
                    limit_bytes: limits.max_content_type_bytes,
                });
            }
            let mut retained = String::new();
            retained
                .try_reserve(value.len())
                .map_err(|_| MultipartError::AllocationFailed)?;
            retained.push_str(value);
            Ok(Some(retained))
        }
    }
}

async fn collect_field(
    field: &mut multer::Field<'_>,
    limits: MultipartLimits,
) -> Result<Vec<u8>, MultipartError> {
    let mut data = Vec::new();
    let mut field_bytes = 0_usize;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|error| map_multer_error(error, limits))?
    {
        field_bytes = field_bytes
            .checked_add(chunk.len())
            .ok_or(MultipartError::AllocationFailed)?;
        if field_bytes > limits.per_field_bytes {
            return Err(MultipartError::FieldTooLarge {
                limit_bytes: limits.per_field_bytes,
            });
        }
        data.try_reserve(chunk.len())
            .map_err(|_| MultipartError::AllocationFailed)?;
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

fn map_multer_error(error: multer::Error, limits: MultipartLimits) -> MultipartError {
    match error {
        multer::Error::NoMultipart => MultipartError::WrongContentType,
        multer::Error::NoBoundary => MultipartError::MissingBoundary,
        multer::Error::DecodeContentType(_) => MultipartError::InvalidBoundary,
        multer::Error::FieldSizeExceeded { .. } => MultipartError::FieldTooLarge {
            limit_bytes: limits.per_field_bytes,
        },
        multer::Error::StreamSizeExceeded { .. } => MultipartError::WholeStreamTooLarge {
            limit_bytes: limits.whole_stream_bytes,
        },
        multer::Error::LockFailure | multer::Error::StreamReadFailed(_) => {
            MultipartError::ParserInvariant
        }
        _ => MultipartError::MalformedBody,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        consume_parameter_value, is_quoted_value_byte, map_multer_error,
        parse_and_validate_boundary, parse_content_disposition, parse_multipart,
        reject_duplicate_boundary_parameters, MultipartLimits, MultipartTextError,
        DEFAULT_MAX_PARTS, DEFAULT_MAX_PART_BYTES, DEFAULT_MAX_RETAINED_METADATA_BYTES,
    };
    use crate::BodyBudget;
    use lily_error::application::http_api::request::MultipartError;

    const BOUNDARY: &str = "LILY-BOUNDARY";

    fn limits() -> MultipartLimits {
        MultipartLimits::production(
            BodyBudget::default(),
            DEFAULT_MAX_PART_BYTES,
            DEFAULT_MAX_PARTS,
            DEFAULT_MAX_RETAINED_METADATA_BYTES,
        )
    }

    fn body(parts: &[(&str, &[u8])]) -> Vec<u8> {
        let mut output = Vec::new();
        for (headers, data) in parts {
            output.extend_from_slice(format!("--{BOUNDARY}\r\n{headers}\r\n\r\n").as_bytes());
            output.extend_from_slice(data);
            output.extend_from_slice(b"\r\n");
        }
        output.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
        output
    }

    fn text_header(name: &str) -> String {
        format!("Content-Disposition: form-data; name=\"{name}\"")
    }

    fn file_header(name: &str, filename: &str, content_type: &str) -> String {
        format!(
            "Content-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\nContent-Type: {content_type}"
        )
    }

    #[tokio::test]
    async fn preserves_wire_order_duplicates_empty_filename_and_binary_bytes() {
        let first = text_header("item");
        let empty_file = file_header("upload", "", "Application/Octet-Stream");
        let second = text_header("item");
        let named_file = file_header("upload", "safe-name.bin", "application/octet-stream");
        let binary = b"\0\xffabc--LILY-BOUNDARYXdef";
        let payload = body(&[
            (&first, b"Alpha"),
            (&empty_file, binary),
            (&second, b"Beta"),
            (&named_file, b"tail"),
        ]);

        let parsed = parse_multipart(
            "Multipart/Form-Data; charset=utf-8; BOUNDARY=\"LILY-BOUNDARY\"",
            &payload,
            limits(),
        )
        .await
        .unwrap();

        assert_eq!(parsed.len(), 4);
        let fields = parsed.fields();
        assert_eq!(fields[0].name(), "item");
        assert_eq!(fields[0].text().unwrap(), "Alpha");
        assert_eq!(fields[1].name(), "upload");
        assert_eq!(fields[1].filename(), Some(""));
        assert_eq!(fields[1].content_type(), Some("Application/Octet-Stream"));
        assert_eq!(fields[1].data(), binary);
        assert_eq!(fields[2].name(), "item");
        assert_eq!(fields[2].text().unwrap(), "Beta");
        assert_eq!(fields[3].filename(), Some("safe-name.bin"));
        assert_eq!(
            parsed
                .named("item")
                .map(|field| field.text().unwrap())
                .collect::<Vec<_>>(),
            ["Alpha", "Beta"]
        );
        assert_eq!(parsed.total_data_bytes(), 5 + binary.len() + 4 + 4);
    }

    #[tokio::test]
    async fn into_parts_transfers_the_payload_allocation_without_copying() {
        let upload = file_header("upload", "sender-name.bin", "application/octet-stream");
        let payload = body(&[(&upload, b"exact-payload")]);
        let mut fields = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &payload,
            limits(),
        )
        .await
        .unwrap()
        .into_fields();
        let field = fields.pop().expect("one parsed upload field");
        let payload_pointer = field.data().as_ptr();

        let (name, filename, content_type, bytes) = field.into_parts();

        assert_eq!(name, "upload");
        assert_eq!(filename.as_deref(), Some("sender-name.bin"));
        assert_eq!(content_type.as_deref(), Some("application/octet-stream"));
        assert_eq!(bytes, b"exact-payload");
        assert_eq!(payload_pointer, bytes.as_ptr());
    }

    #[tokio::test]
    async fn accepts_quoted_unquoted_reordered_and_case_varied_boundary_parameters() {
        let header = text_header("value");
        let payload = body(&[(&header, b"ok")]);
        for content_type in [
            "multipart/form-data; boundary=LILY-BOUNDARY",
            "multipart/form-data; boundary=\"LILY-BOUNDARY\"",
            "multipart/form-data; charset=utf-8; boundary=LILY-BOUNDARY",
            "Multipart/Form-Data; BOUNDARY=LILY-BOUNDARY",
        ] {
            let parsed = parse_multipart(content_type, &payload, limits())
                .await
                .unwrap();
            assert_eq!(parsed.named("value").next().unwrap().text().unwrap(), "ok");
        }
    }

    #[test]
    fn boundary_length_and_character_rules_match_the_rfc_limit() {
        let maximum = "a".repeat(70);
        assert_eq!(
            parse_and_validate_boundary(&format!("multipart/form-data; boundary={maximum}"))
                .unwrap(),
            maximum
        );
        let oversized = "a".repeat(71);
        assert_eq!(
            parse_and_validate_boundary(&format!("multipart/form-data; boundary={oversized}"))
                .unwrap_err(),
            MultipartError::InvalidBoundary
        );
        assert_eq!(
            parse_and_validate_boundary("multipart/form-data; boundary=bad@value").unwrap_err(),
            MultipartError::InvalidBoundary
        );
    }

    #[test]
    fn parameter_scanners_are_progress_bounded_and_fail_closed() {
        assert!(reject_duplicate_boundary_parameters(
            b"multipart/form-data; charset=utf-8; boundary=safe"
        )
        .is_ok());
        assert_eq!(
            reject_duplicate_boundary_parameters(
                b"multipart/form-data; boundary=first; BOUNDARY=second"
            ),
            Err(MultipartError::InvalidBoundary)
        );

        let parsed = parse_content_disposition(
            br#"form-data; name="value"; filename="a\";b.bin""#,
            limits(),
        )
        .unwrap();
        assert_eq!(parsed.name, "value");
        assert_eq!(parsed.filename.as_deref(), Some("a\";b.bin"));

        for byte in [b'\t', b' ', b'!', b'~', 0x80, 0xff] {
            assert!(is_quoted_value_byte(byte));
        }
        for byte in [0, b'\r', b'\n', 0x7f] {
            assert!(!is_quoted_value_byte(byte));
        }

        let mut cursor = 0;
        let quoted = consume_parameter_value(br#""a\";b""#, &mut cursor).unwrap();
        assert_eq!(quoted.raw, br#"a\";b"#);
        assert!(quoted.has_escapes);
        assert_eq!(cursor, 7);
        let mut cursor = 0;
        assert!(consume_parameter_value(b"\"bad\rvalue\"", &mut cursor).is_err());
        let mut cursor = 0;
        let token = consume_parameter_value(b"token;next", &mut cursor).unwrap();
        assert_eq!(token.raw, b"token");
        assert!(!token.has_escapes);
        assert_eq!(cursor, 5);
    }

    #[test]
    fn multer_errors_map_to_stable_framework_categories() {
        let limits = limits();
        assert_eq!(
            map_multer_error(multer::Error::NoMultipart, limits),
            MultipartError::WrongContentType
        );
        assert_eq!(
            map_multer_error(multer::Error::NoBoundary, limits),
            MultipartError::MissingBoundary
        );
        assert_eq!(
            map_multer_error(multer::parse_boundary("not a mime @").unwrap_err(), limits,),
            MultipartError::InvalidBoundary
        );
        assert_eq!(
            map_multer_error(multer::Error::StreamSizeExceeded { limit: 1 }, limits),
            MultipartError::WholeStreamTooLarge {
                limit_bytes: limits.whole_stream_bytes,
            }
        );
        assert_eq!(
            map_multer_error(multer::Error::LockFailure, limits),
            MultipartError::ParserInvariant
        );
        assert_eq!(
            map_multer_error(
                multer::Error::StreamReadFailed(Box::new(std::io::Error::other("secret"))),
                limits,
            ),
            MultipartError::ParserInvariant
        );
    }

    #[tokio::test]
    async fn empty_field_data_is_preserved_as_an_empty_byte_slice() {
        let header = text_header("empty");
        let parsed = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &body(&[(&header, b"")]),
            limits(),
        )
        .await
        .unwrap();

        assert!(parsed.fields()[0].is_empty());
        assert_eq!(parsed.fields()[0].data(), b"");
        assert_eq!(parsed.fields()[0].text().unwrap(), "");
    }

    #[tokio::test]
    async fn disposition_parameters_are_case_insensitive_and_strictly_decoded() {
        let upper_text = "Content-Disposition: FORM-DATA; NAME=unquoted; ignored=token";
        let upper_filename = "Content-Disposition: form-data; NAME=\"upload\"; FILENAME=\"a\\\";b.bin\"; ignored=\"x;y\"";
        let payload = body(&[(upper_text, b"text"), (upper_filename, b"binary")]);
        let parsed = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &payload,
            limits(),
        )
        .await
        .unwrap();

        assert_eq!(parsed.fields()[0].name(), "unquoted");
        assert_eq!(parsed.fields()[0].filename(), None);
        assert_eq!(parsed.fields()[1].name(), "upload");
        assert_eq!(parsed.fields()[1].filename(), Some("a\";b.bin"));
    }

    #[tokio::test]
    async fn many_unknown_disposition_parameters_are_skipped_without_capture() {
        let mut header = "Content-Disposition: form-data".to_string();
        for index in 0..128 {
            header.push_str(&format!("; unknown-{index}=\"ignored;value\""));
        }
        header.push_str("; name=\"retained\"");
        let parsed = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &body(&[(&header, b"exact")]),
            limits(),
        )
        .await
        .unwrap();

        assert_eq!(parsed.fields()[0].name(), "retained");
        assert_eq!(parsed.fields()[0].data(), b"exact");
    }

    #[tokio::test]
    async fn duplicate_and_malformed_disposition_parameters_fail_closed() {
        for header in [
            "Content-Disposition: form-data; name=\"a\"; NAME=\"b\"",
            "Content-Disposition: form-data; name=\"a\"; filename=\"x\"; FILENAME=\"y\"",
            "Content-Disposition: form-data; name",
            "Content-Disposition: form-data; name=\"unterminated",
            "Content-Disposition: form-data; name=\"valid\"junk",
            "Content-Disposition: form-data; name=\"valid\"; broken@=value",
            "Content-Disposition: form-data; name=\"valid\"; filename=",
        ] {
            let payload = body(&[(header, b"data")]);
            assert_eq!(
                parse_multipart(
                    "multipart/form-data; boundary=LILY-BOUNDARY",
                    &payload,
                    limits(),
                )
                .await
                .unwrap_err(),
                MultipartError::MalformedBody
            );
        }
    }

    #[tokio::test]
    async fn content_type_and_boundary_failures_are_typed() {
        let header = text_header("value");
        let payload = body(&[(&header, b"ok")]);
        for (content_type, expected) in [
            ("application/json", MultipartError::WrongContentType),
            ("multipart/form-data", MultipartError::MissingBoundary),
            (
                "multipart/form-data; boundary=\"bad boundary \"",
                MultipartError::InvalidBoundary,
            ),
            (
                "multipart/form-data; boundary=LILY-BOUNDARY; BOUNDARY=other",
                MultipartError::InvalidBoundary,
            ),
            (
                "multipart/form-data; boundary=LILY-BOUNDARY; boundary=LILY-BOUNDARY",
                MultipartError::InvalidBoundary,
            ),
        ] {
            assert_eq!(
                parse_multipart(content_type, &payload, limits())
                    .await
                    .unwrap_err(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn malformed_parts_fail_the_entire_request() {
        let valid = text_header("valid");
        let payload = body(&[
            (&valid, b"kept"),
            ("Content-Disposition: form-data", b"must-not-be-skipped"),
        ]);
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &payload,
                limits(),
            )
            .await
            .unwrap_err(),
            MultipartError::MissingFieldName
        );

        let attachment = body(&[("Content-Disposition: attachment; name=\"value\"", b"no")]);
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &attachment,
                limits(),
            )
            .await
            .unwrap_err(),
            MultipartError::MalformedBody
        );
    }

    #[tokio::test]
    async fn truncated_bare_lf_and_boundary_prefix_line_fail_closed() {
        let header = text_header("value");
        let mut truncated = body(&[(&header, b"ok")]);
        truncated.truncate(truncated.len() - (BOUNDARY.len() + 6));
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &truncated,
                limits(),
            )
            .await
            .unwrap_err(),
            MultipartError::MalformedBody
        );

        let bare_lf = format!(
            "--{BOUNDARY}\nContent-Disposition: form-data; name=\"value\"\n\nok\n--{BOUNDARY}--\n"
        );
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                bare_lf.as_bytes(),
                limits(),
            )
            .await
            .unwrap_err(),
            MultipartError::MalformedBody
        );

        let invalid_prefix = body(&[(&header, b"before\r\n--LILY-BOUNDARYXafter")]);
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &invalid_prefix,
                limits(),
            )
            .await
            .unwrap_err(),
            MultipartError::MalformedBody
        );
    }

    #[tokio::test]
    async fn empty_field_name_is_rejected_and_binary_fields_are_exact() {
        let empty_name = text_header("");
        let payload = body(&[(&empty_name, b"value")]);
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &payload,
                limits(),
            )
            .await
            .unwrap_err(),
            MultipartError::EmptyFieldName
        );

        let text = text_header("value");
        let invalid_utf8 = body(&[(&text, b"\xff")]);
        let parsed = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &invalid_utf8,
            limits(),
        )
        .await
        .unwrap();
        assert_eq!(parsed.fields()[0].data(), b"\xff");
        assert_eq!(
            parsed.fields()[0].text().unwrap_err(),
            MultipartTextError::InvalidUtf8
        );
    }

    #[tokio::test]
    async fn whole_stream_and_per_field_limits_are_enforced() {
        let header = text_header("value");
        let payload = body(&[(&header, b"four")]);
        let mut whole = limits();
        whole.whole_stream_bytes = payload.len() - 1;
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &payload,
                whole,
            )
            .await
            .unwrap_err(),
            MultipartError::WholeStreamTooLarge {
                limit_bytes: payload.len() - 1,
            }
        );

        let mut field = limits();
        field.per_field_bytes = 3;
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &payload,
                field,
            )
            .await
            .unwrap_err(),
            MultipartError::FieldTooLarge { limit_bytes: 3 }
        );
    }

    #[tokio::test]
    async fn part_count_limit_is_uniform_and_enforced_before_collection() {
        let a = text_header("a");
        let file_a = file_header("file", "a", "application/octet-stream");

        let mut part_limits = limits();
        part_limits.max_parts = 1;
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &body(&[(&a, b"1"), (&file_a, b"2")]),
                part_limits,
            )
            .await
            .unwrap_err(),
            MultipartError::TooManyParts { limit: 1 }
        );
    }

    #[tokio::test]
    async fn exposed_metadata_byte_limits_are_enforced() {
        let retained = text_header("value");
        let mut retained_limits = limits();
        retained_limits.max_retained_metadata_bytes = 8;
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &body(&[(&retained, b"x")]),
                retained_limits,
            )
            .await
            .unwrap_err(),
            MultipartError::RetainedMetadataTooLarge { limit_bytes: 8 }
        );

        let long_name = text_header("long");
        let mut name_limits = limits();
        name_limits.max_field_name_bytes = 3;
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &body(&[(&long_name, b"x")]),
                name_limits,
            )
            .await
            .unwrap_err(),
            MultipartError::FieldNameTooLong { limit_bytes: 3 }
        );

        let long_filename = file_header("f", "long", "application/octet-stream");
        let mut filename_limits = limits();
        filename_limits.max_filename_bytes = 3;
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &body(&[(&long_filename, b"x")]),
                filename_limits,
            )
            .await
            .unwrap_err(),
            MultipartError::FilenameTooLong { limit_bytes: 3 }
        );

        let long_content_type = file_header("f", "x", "application/octet-stream");
        let mut content_type_limits = limits();
        content_type_limits.max_content_type_bytes = 3;
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &body(&[(&long_content_type, b"x")]),
                content_type_limits,
            )
            .await
            .unwrap_err(),
            MultipartError::ContentTypeTooLong { limit_bytes: 3 }
        );
    }

    #[tokio::test]
    async fn exact_metadata_and_data_limits_are_inclusive() {
        let retained = text_header("value");
        let mut retained_limits = limits();
        retained_limits.max_retained_metadata_bytes =
            "content-disposition".len() + br#"form-data; name="value""#.len() + 4;
        assert!(parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &body(&[(&retained, b"four")]),
            retained_limits,
        )
        .await
        .is_ok());

        let content_type = "application/octet-stream";
        let file = file_header("file", "name", content_type);
        let mut content_type_limits = limits();
        content_type_limits.max_content_type_bytes = content_type.len();
        content_type_limits.per_field_bytes = 4;
        assert!(parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &body(&[(&file, b"four")]),
            content_type_limits,
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn duplicate_part_headers_have_a_documented_last_wins_policy() {
        // multer 3.1 collapses wire duplicates into HeaderMap with `insert`.
        // Lily therefore treats only the final value as untrusted metadata;
        // bytes are never classified as text or file from these headers.
        let headers = concat!(
            "Content-Disposition: form-data; name=\"first\"\r\n",
            "Content-Disposition: form-data; name=\"last\"\r\n",
            "Content-Type: text/plain\r\n",
            "Content-Type: application/octet-stream"
        );
        let parsed = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &body(&[(headers, b"exact-bytes")]),
            limits(),
        )
        .await
        .unwrap();

        assert_eq!(parsed.fields()[0].name(), "last");
        assert_eq!(
            parsed.fields()[0].content_type(),
            Some("application/octet-stream")
        );
        assert_eq!(parsed.fields()[0].data(), b"exact-bytes");
    }

    #[tokio::test]
    async fn multer_raw_part_header_count_cap_is_fail_closed() {
        let mut accepted = text_header("value");
        for index in 0..31 {
            accepted.push_str(&format!("\r\nX-Lily-{index}: value"));
        }
        let parsed = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &body(&[(&accepted, b"ok")]),
            limits(),
        )
        .await
        .unwrap();
        assert_eq!(parsed.fields()[0].data(), b"ok");

        let mut rejected = accepted;
        rejected.push_str("\r\nX-Lily-overflow: value");
        assert_eq!(
            parse_multipart(
                "multipart/form-data; boundary=LILY-BOUNDARY",
                &body(&[(&rejected, b"no")]),
                limits(),
            )
            .await
            .unwrap_err(),
            MultipartError::MalformedBody
        );
    }

    #[tokio::test]
    async fn safe_debug_and_errors_do_not_expose_upload_metadata_or_payload() {
        const NAME: &str = "LILY_SECRET_FIELD";
        const FILENAME: &str = "LILY_SECRET_FILENAME";
        const BODY: &[u8] = b"LILY_SECRET_BODY";
        let file = file_header(NAME, FILENAME, "application/octet-stream");
        let payload = body(&[(&file, BODY)]);
        let parsed = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &payload,
            limits(),
        )
        .await
        .unwrap();

        for safe in [format!("{parsed:?}"), format!("{:?}", parsed.fields()[0])] {
            assert!(!safe.contains(NAME));
            assert!(!safe.contains(FILENAME));
            assert!(!safe.contains(std::str::from_utf8(BODY).unwrap()));
        }

        let malformed = body(&[(
            "Content-Disposition: form-data; filename=\"LILY_SECRET_FILENAME\"",
            BODY,
        )]);
        let error = parse_multipart(
            "multipart/form-data; boundary=LILY-BOUNDARY",
            &malformed,
            limits(),
        )
        .await
        .unwrap_err();
        let safe = format!("{error:?} {error}");
        assert!(!safe.contains(FILENAME));
        assert!(!safe.contains(std::str::from_utf8(BODY).unwrap()));
        assert_eq!(error.diagnostic_code(), "MULTIPART_MISSING_FIELD_NAME");
    }
}
