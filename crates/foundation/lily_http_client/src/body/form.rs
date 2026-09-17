use crate::body::Body;
use crate::error::{MultipartBuildError, MultipartMetadataField, Result};
use async_trait::async_trait;
use bytes::Bytes;
use lily_web_core::{FormData, FormValues};
use std::fmt;

/// Form data body implementation for URL-encoded form data
#[derive(Clone)]
pub struct FormBody {
    data: FormData,
    content_type: String,
}

impl fmt::Debug for FormBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FormBody")
            .field("field_count", &self.data.len())
            .finish()
    }
}

impl FormBody {
    /// Create a new form body
    pub fn new() -> Self {
        Self {
            data: FormData::new(),
            content_type: "application/x-www-form-urlencoded".to_string(),
        }
    }

    /// Create a form body from ordered key-value pairs. Duplicate names are
    /// retained rather than overwritten.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            data: FormData::from_pairs(pairs),
            content_type: "application/x-www-form-urlencoded".to_string(),
        }
    }

    /// Add a field to the form
    pub fn field(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.data.push(name, value);
        self
    }

    /// Add a field to the form (mutable version)
    pub fn add_field(&mut self, name: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.data.push(name, value);
        self
    }

    /// Remove every occurrence of a field name.
    pub fn remove_fields(&mut self, name: &str) -> usize {
        self.data.remove_all(name)
    }

    /// Get a field value
    pub fn get_field(&self, name: &str) -> Option<&str> {
        self.data.get(name)
    }

    /// Get every value for a field name in insertion order.
    pub fn get_all_fields<'a>(&'a self, name: &str) -> FormValues<'a> {
        self.data.get_all(name)
    }

    /// Check if a field exists
    pub fn has_field(&self, name: &str) -> bool {
        self.data.get(name).is_some()
    }

    /// Get all field names
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.data.iter().map(|(name, _)| name)
    }

    /// Get the number of fields
    pub fn field_count(&self) -> usize {
        self.data.len()
    }

    /// Get the number of distinct field names.
    pub fn field_name_count(&self) -> usize {
        self.data.name_count()
    }

    /// Clear all fields
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Get the ordered form data without copying.
    pub fn as_data(&self) -> &FormData {
        &self.data
    }

    /// Convert to URL-encoded string
    pub fn to_url_encoded(&self) -> String {
        self.data.to_url_encoded()
    }
}

impl Default for FormBody {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Body for FormBody {
    fn content_type(&self) -> Option<&str> {
        Some(&self.content_type)
    }

    fn content_length(&self) -> Option<usize> {
        Some(self.to_url_encoded().len())
    }

    async fn to_bytes(&mut self) -> Result<Bytes> {
        Ok(Bytes::from(self.to_url_encoded()))
    }

    fn try_clone(&self) -> Option<Box<dyn Body>> {
        Some(Box::new(self.clone()))
    }
}

/// Multipart form data body implementation
#[derive(Clone)]
pub struct MultipartBody {
    fields: Vec<MultipartField>,
    boundary: String,
    content_type: String,
    encoded_len: usize,
}

#[derive(Clone)]
struct MultipartField {
    name: String,
    value: MultipartValue,
    content_type: Option<String>,
    filename: Option<String>,
}

#[derive(Clone)]
enum MultipartValue {
    Text(String),
    Binary(Bytes),
}

impl fmt::Debug for MultipartBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartBody")
            .field("field_count", &self.fields.len())
            .finish()
    }
}

impl fmt::Debug for MultipartField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartField")
            .field("value", &self.value)
            .field("has_content_type", &self.content_type.is_some())
            .field("has_filename", &self.filename.is_some())
            .finish()
    }
}

impl fmt::Debug for MultipartValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(value) => f
                .debug_struct("MultipartText")
                .field("content_length", &value.len())
                .finish(),
            Self::Binary(value) => f
                .debug_struct("MultipartBinary")
                .field("content_length", &value.len())
                .finish(),
        }
    }
}

impl MultipartBody {
    /// Create a new multipart body with a framework-generated valid boundary.
    pub fn new() -> Self {
        let boundary = format!("----formdata-lily-{}", generate_boundary());
        debug_assert!(validate_boundary(&boundary).is_ok());
        Self::from_valid_boundary(boundary)
    }

    /// Create a multipart body with a validated custom boundary.
    pub fn with_boundary(boundary: impl Into<String>) -> Result<Self> {
        let boundary = boundary.into();
        validate_boundary(&boundary)?;
        Ok(Self::from_valid_boundary(boundary))
    }

    fn from_valid_boundary(boundary: String) -> Self {
        let content_type = format!("multipart/form-data; boundary=\"{boundary}\"");
        let encoded_len = boundary
            .len()
            .checked_add(6)
            .expect("an RFC-bounded multipart boundary length cannot overflow");
        Self {
            fields: Vec::new(),
            boundary,
            content_type,
            encoded_len,
        }
    }

    /// Add a validated text field.
    pub fn text_field(mut self, name: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let name = name.into();
        validate_metadata(
            &name,
            MultipartMetadataField::Name,
            MAX_FIELD_NAME_BYTES,
            false,
        )?;
        self.push_field(MultipartField {
            name,
            value: MultipartValue::Text(value.into()),
            content_type: None,
            filename: None,
        })?;
        Ok(self)
    }

    /// Add a validated binary field with untrusted filename metadata.
    pub fn file_field(
        mut self,
        name: impl Into<String>,
        filename: impl Into<String>,
        content_type: impl Into<String>,
        data: impl Into<Bytes>,
    ) -> Result<Self> {
        let name = name.into();
        let filename = filename.into();
        let content_type = content_type.into();
        validate_metadata(
            &name,
            MultipartMetadataField::Name,
            MAX_FIELD_NAME_BYTES,
            false,
        )?;
        validate_metadata(
            &filename,
            MultipartMetadataField::Filename,
            MAX_FILENAME_BYTES,
            true,
        )?;
        validate_metadata(
            &content_type,
            MultipartMetadataField::ContentType,
            MAX_CONTENT_TYPE_BYTES,
            false,
        )?;
        content_type
            .parse::<mime::Mime>()
            .map_err(|_| MultipartBuildError::InvalidContentType)?;
        self.push_field(MultipartField {
            name,
            value: MultipartValue::Binary(data.into()),
            content_type: Some(content_type),
            filename: Some(filename),
        })?;
        Ok(self)
    }

    /// Get the boundary string
    pub fn boundary(&self) -> &str {
        &self.boundary
    }

    /// Convert to exact multipart bytes after reserving the already-computed
    /// representation length once.
    pub fn to_multipart_bytes(&self) -> Result<Bytes> {
        let mut result = Vec::new();
        result
            .try_reserve_exact(self.encoded_len)
            .map_err(|_| MultipartBuildError::AllocationFailed)?;

        for field in &self.fields {
            result.extend_from_slice(b"--");
            result.extend_from_slice(self.boundary.as_bytes());
            result.extend_from_slice(b"\r\n");
            result.extend_from_slice(DISPOSITION_PREFIX);
            append_quoted(&mut result, &field.name);
            if let Some(filename) = &field.filename {
                result.extend_from_slice(FILENAME_SEPARATOR);
                append_quoted(&mut result, filename);
                result.extend_from_slice(b"\"\r\n");
            } else {
                result.extend_from_slice(b"\"\r\n");
            }

            if let Some(content_type) = &field.content_type {
                result.extend_from_slice(CONTENT_TYPE_PREFIX);
                result.extend_from_slice(content_type.as_bytes());
                result.extend_from_slice(b"\r\n");
            }

            result.extend_from_slice(b"\r\n");
            match &field.value {
                MultipartValue::Text(text) => result.extend_from_slice(text.as_bytes()),
                MultipartValue::Binary(data) => result.extend_from_slice(data),
            }
            result.extend_from_slice(b"\r\n");
        }

        result.extend_from_slice(b"--");
        result.extend_from_slice(self.boundary.as_bytes());
        result.extend_from_slice(b"--\r\n");
        debug_assert_eq!(result.len(), self.encoded_len);

        Ok(Bytes::from(result))
    }

    fn push_field(&mut self, field: MultipartField) -> Result<()> {
        let field_len = encoded_field_len(&self.boundary, &field)?;
        let next_len = self
            .encoded_len
            .checked_add(field_len)
            .ok_or(MultipartBuildError::LengthOverflow)?;
        self.fields
            .try_reserve(1)
            .map_err(|_| MultipartBuildError::AllocationFailed)?;
        self.fields.push(field);
        self.encoded_len = next_len;
        Ok(())
    }
}

impl Default for MultipartBody {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Body for MultipartBody {
    fn content_type(&self) -> Option<&str> {
        Some(&self.content_type)
    }

    fn content_length(&self) -> Option<usize> {
        Some(self.encoded_len)
    }

    async fn to_bytes(&mut self) -> Result<Bytes> {
        self.to_multipart_bytes()
    }

    fn try_clone(&self) -> Option<Box<dyn Body>> {
        Some(Box::new(self.clone()))
    }
}

const MAX_BOUNDARY_BYTES: usize = 70;
const MAX_FIELD_NAME_BYTES: usize = 256;
const MAX_FILENAME_BYTES: usize = 1024;
const MAX_CONTENT_TYPE_BYTES: usize = 256;
const DISPOSITION_PREFIX: &[u8] = b"Content-Disposition: form-data; name=\"";
const FILENAME_SEPARATOR: &[u8] = b"\"; filename=\"";
const CONTENT_TYPE_PREFIX: &[u8] = b"Content-Type: ";

fn validate_boundary(boundary: &str) -> std::result::Result<(), MultipartBuildError> {
    let bytes = boundary.as_bytes();
    let valid = |byte: u8| {
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
    if bytes.is_empty()
        || bytes.len() > MAX_BOUNDARY_BYTES
        || !bytes.iter().copied().all(valid)
        || bytes.last() == Some(&b' ')
    {
        return Err(MultipartBuildError::InvalidBoundary);
    }
    Ok(())
}

fn validate_metadata(
    value: &str,
    field: MultipartMetadataField,
    limit_bytes: usize,
    allow_empty: bool,
) -> std::result::Result<(), MultipartBuildError> {
    if !allow_empty && value.is_empty() {
        return Err(MultipartBuildError::EmptyFieldName);
    }
    if value.len() > limit_bytes {
        return Err(MultipartBuildError::MetadataTooLong { field, limit_bytes });
    }
    if value.chars().any(char::is_control) {
        return Err(MultipartBuildError::ControlCharacter { field });
    }
    Ok(())
}

fn encoded_field_len(
    boundary: &str,
    field: &MultipartField,
) -> std::result::Result<usize, MultipartBuildError> {
    let mut length = 0_usize;
    checked_add(&mut length, 2)?;
    checked_add(&mut length, boundary.len())?;
    checked_add(&mut length, 2)?;
    checked_add(&mut length, DISPOSITION_PREFIX.len())?;
    checked_add(&mut length, escaped_len(&field.name)?)?;
    if let Some(filename) = &field.filename {
        checked_add(&mut length, FILENAME_SEPARATOR.len())?;
        checked_add(&mut length, escaped_len(filename)?)?;
    }
    checked_add(&mut length, 3)?;
    if let Some(content_type) = &field.content_type {
        checked_add(&mut length, CONTENT_TYPE_PREFIX.len())?;
        checked_add(&mut length, content_type.len())?;
        checked_add(&mut length, 2)?;
    }
    checked_add(&mut length, 2)?;
    checked_add(&mut length, field.value.len())?;
    checked_add(&mut length, 2)?;
    Ok(length)
}

fn escaped_len(value: &str) -> std::result::Result<usize, MultipartBuildError> {
    value.bytes().try_fold(0_usize, |length, byte| {
        length
            .checked_add(if matches!(byte, b'"' | b'\\') { 2 } else { 1 })
            .ok_or(MultipartBuildError::LengthOverflow)
    })
}

fn checked_add(total: &mut usize, amount: usize) -> std::result::Result<(), MultipartBuildError> {
    *total = total
        .checked_add(amount)
        .ok_or(MultipartBuildError::LengthOverflow)?;
    Ok(())
}

fn append_quoted(output: &mut Vec<u8>, value: &str) {
    for byte in value.bytes() {
        if matches!(byte, b'"' | b'\\') {
            output.push(b'\\');
        }
        output.push(byte);
    }
}

impl MultipartValue {
    fn len(&self) -> usize {
        match self {
            Self::Text(value) => value.len(),
            Self::Binary(value) => value.len(),
        }
    }
}

/// Generate a process-unique boundary suffix.
fn generate_boundary() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);

    format!("{timestamp:x}-{counter:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpClientError;

    #[tokio::test]
    async fn test_form_body_creation() {
        let body = FormBody::new()
            .field("name", "Alice")
            .field("age", "30")
            .field("active", "true");

        assert_eq!(
            body.content_type(),
            Some("application/x-www-form-urlencoded")
        );
        assert_eq!(body.field_count(), 3);
        assert_eq!(body.get_field("name"), Some("Alice"));
        assert_eq!(body.get_field("age"), Some("30"));
        assert!(body.has_field("active"));
    }

    #[tokio::test]
    async fn test_form_body_from_pairs() {
        let pairs = vec![("name", "Bob"), ("email", "bob@example.com")];
        let body = FormBody::from_pairs(pairs);

        assert_eq!(body.field_count(), 2);
        assert_eq!(body.get_field("name"), Some("Bob"));
        assert_eq!(body.get_field("email"), Some("bob@example.com"));
    }

    #[tokio::test]
    async fn form_body_uses_the_shared_utf8_codec_and_preserves_duplicates() {
        let body = FormBody::new()
            .field("city", "İzmir")
            .field("tag", "first value")
            .field("tag", "+second");

        let encoded = body.to_url_encoded();
        assert_eq!(encoded, "city=%C4%B0zmir&tag=first+value&tag=%2Bsecond");
        assert_eq!(body.get_field("tag"), Some("first value"));
        assert_eq!(
            body.get_all_fields("tag").collect::<Vec<_>>(),
            ["first value", "+second"]
        );
    }

    #[tokio::test]
    async fn test_form_body_to_bytes() {
        let mut body = FormBody::new()
            .field("name", "Charlie")
            .field("value", "123");

        let bytes = body.to_bytes().await.unwrap();
        let content = String::from_utf8(bytes.to_vec()).unwrap();

        // Should contain both fields
        assert!(content.contains("name=Charlie"));
        assert!(content.contains("value=123"));
        assert!(content.contains("&"));
    }

    #[tokio::test]
    async fn test_multipart_body_creation() {
        let body = MultipartBody::new()
            .text_field("name", "David")
            .unwrap()
            .text_field("message", "Hello")
            .unwrap();

        assert!(body
            .content_type()
            .unwrap()
            .starts_with("multipart/form-data"));
        assert!(!body.boundary().is_empty());
    }

    #[tokio::test]
    async fn test_multipart_body_with_file() {
        let file_data = b"file content here";
        let mut body = MultipartBody::new()
            .text_field("description", "Test file")
            .unwrap()
            .file_field("upload", "test.txt", "text/plain", file_data.as_ref())
            .unwrap();

        let bytes = body.to_bytes().await.unwrap();
        let content = String::from_utf8_lossy(&bytes);

        // Should contain multipart structure
        assert!(content.contains("Content-Disposition: form-data"));
        assert!(content.contains("name=\"description\""));
        assert!(content.contains("name=\"upload\""));
        assert!(content.contains("filename=\"test.txt\""));
        assert!(content.contains("Content-Type: text/plain"));
        assert!(content.contains("file content here"));
    }

    #[tokio::test]
    async fn test_multipart_boundary() {
        let body = MultipartBody::with_boundary("custom-boundary-123").unwrap();

        assert_eq!(body.boundary(), "custom-boundary-123");
        assert!(body
            .content_type()
            .unwrap()
            .contains("boundary=\"custom-boundary-123\""));
    }

    #[test]
    fn multipart_rejects_header_injection_and_invalid_boundary_values() {
        assert!(matches!(
            MultipartBody::with_boundary("safe\r\nX-Injected: yes"),
            Err(HttpClientError::InvalidMultipart(
                MultipartBuildError::InvalidBoundary
            ))
        ));
        assert!(matches!(
            MultipartBody::new().text_field("name\r\nX-Injected: yes", "value"),
            Err(HttpClientError::InvalidMultipart(
                MultipartBuildError::ControlCharacter {
                    field: MultipartMetadataField::Name
                }
            ))
        ));
        assert!(matches!(
            MultipartBody::new().file_field("file", "safe", "text/plain\r\nx: y", b"x".as_slice()),
            Err(HttpClientError::InvalidMultipart(
                MultipartBuildError::ControlCharacter {
                    field: MultipartMetadataField::ContentType
                }
            ))
        ));
        assert!(matches!(
            MultipartBody::new().file_field("file", "safe", "not-a-media-type", b"x".as_slice()),
            Err(HttpClientError::InvalidMultipart(
                MultipartBuildError::InvalidContentType
            ))
        ));
        assert!(matches!(
            MultipartBody::new().text_field("", "value"),
            Err(HttpClientError::InvalidMultipart(
                MultipartBuildError::EmptyFieldName
            ))
        ));
    }

    #[tokio::test]
    async fn multipart_escapes_quoted_metadata_and_declares_exact_length() {
        let mut body = MultipartBody::with_boundary("safe boundary")
            .unwrap()
            .file_field(
                "a\"b\\c",
                "f\"g\\h.txt",
                "text/plain",
                b"payload".as_slice(),
            )
            .unwrap();
        let declared = body.content_length().unwrap();
        let encoded = body.to_bytes().await.unwrap();
        assert_eq!(encoded.len(), declared);
        let encoded = String::from_utf8(encoded.to_vec()).unwrap();
        assert!(encoded.contains("name=\"a\\\"b\\\\c\""));
        assert!(encoded.contains("filename=\"f\\\"g\\\\h.txt\""));
    }

    #[test]
    fn multipart_length_arithmetic_fails_closed() {
        let mut length = usize::MAX;
        assert_eq!(
            checked_add(&mut length, 1),
            Err(MultipartBuildError::LengthOverflow)
        );
    }

    #[tokio::test]
    async fn test_form_body_operations() {
        let mut body = FormBody::new();

        body.add_field("initial", "value");
        assert_eq!(body.field_count(), 1);

        body.add_field("second", "another");
        assert_eq!(body.field_count(), 2);

        body.add_field("initial", "second");
        let removed = body.remove_fields("initial");
        assert_eq!(removed, 2);
        assert_eq!(body.field_count(), 1);

        body.clear();
        assert_eq!(body.field_count(), 0);
    }

    #[tokio::test]
    async fn lily_client_form_output_roundtrips_through_the_server_codec() {
        use lily_web_core::{RawHeader, Request, RequestExt};

        let mut body = FormBody::new()
            .field("city", "İzmir")
            .field("tag", "first value")
            .field("tag", "+second");
        let bytes = body.to_bytes().await.unwrap();
        let request = Request::from_transport_parts(
            "POST".to_string(),
            "/form".to_string(),
            vec![RawHeader {
                name: "Content-Type".to_string(),
                value: "application/x-www-form-urlencoded; charset=UTF-8".to_string(),
                line_number: 1,
                raw_line: "Content-Type: application/x-www-form-urlencoded; charset=UTF-8"
                    .to_string(),
            }],
            &bytes,
        )
        .await
        .unwrap();

        let parsed = request.form().await.unwrap();
        assert_eq!(parsed, body.data);
    }

    #[tokio::test]
    async fn lily_client_multipart_output_roundtrips_through_multer() {
        use lily_web_core::{RawHeader, Request, RequestExt};

        let mut body = MultipartBody::with_boundary("lily-safe-boundary")
            .unwrap()
            .text_field("tag", "first")
            .unwrap()
            .text_field("tag", "second")
            .unwrap()
            .file_field(
                "a\"b",
                "f\\name.txt",
                "application/octet-stream",
                Bytes::from_static(&[0, 255, 1]),
            )
            .unwrap();
        let content_type = body.content_type().unwrap().to_string();
        let bytes = body.to_bytes().await.unwrap();
        let request = Request::from_transport_parts(
            "POST".to_string(),
            "/multipart".to_string(),
            vec![RawHeader {
                name: "Content-Type".to_string(),
                value: content_type.clone(),
                line_number: 1,
                raw_line: format!("Content-Type: {content_type}"),
            }],
            &bytes,
        )
        .await
        .unwrap();

        let parsed = request.multipart().await.unwrap();
        assert_eq!(
            parsed
                .named("tag")
                .map(|field| field.data())
                .collect::<Vec<_>>(),
            [b"first".as_slice(), b"second".as_slice()]
        );
        assert_eq!(parsed.fields()[2].name(), "a\"b");
        assert_eq!(parsed.fields()[2].filename(), Some("f\\name.txt"));
        assert_eq!(parsed.fields()[2].data(), [0, 255, 1]);
    }
}
