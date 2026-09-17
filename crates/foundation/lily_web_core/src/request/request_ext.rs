use crate::string_interner::{preserve_opaque, preserve_opaque_owned, InternedString};
use crate::{
    cookie::{validate_request_name, RequestCookieError, RequestCookieJar},
    request::form::{is_form_content_type, FormData},
    request::multipart::{
        parse_multipart, validate_multipart_content_type, MultipartData, MultipartLimits,
    },
    request::{Request, RequestBodyError},
    response::{request_last_event_id, LastEventId, LastEventIdError},
    HttpBuffer,
};
use lily_error::application::http_api::request::{FormError, MultipartError, RequestError};

/// Small classification used by [`RequestExt::http_method`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
    /// `DELETE`.
    Delete,
    /// Any other method, including extension methods.
    Other,
}

impl HttpMethod {
    /// Classifies an exact method token without allocating.
    #[inline]
    fn from_str(method: &str) -> Self {
        match method {
            "GET" => HttpMethod::Get,
            "POST" => HttpMethod::Post,
            "PUT" => HttpMethod::Put,
            "DELETE" => HttpMethod::Delete,
            _ => HttpMethod::Other,
        }
    }
}

fn map_body_read_error(error: RequestBodyError) -> RequestError {
    match error {
        RequestBodyError::PayloadTooLarge { limit_bytes } => {
            RequestError::BodyTooLarge { limit_bytes }
        }
        RequestBodyError::ReadTimedOut => RequestError::BodyReadTimedOut,
        RequestBodyError::TransportInterrupted => {
            RequestError::BodyReadFailed("request body transport was interrupted".to_string())
        }
        RequestBodyError::InvalidTrailers => {
            RequestError::BodyReadFailed("request trailers violate configured limits".to_string())
        }
        RequestBodyError::AlreadyStreaming => RequestError::BodyReadFailed(
            "request body consumption mode is already locked".to_string(),
        ),
        RequestBodyError::BufferUnavailable => RequestError::BodyBufferUnavailable,
    }
}

fn is_json_content_type(value: &str) -> bool {
    let Ok(media_type) = value.parse::<mime::Mime>() else {
        return false;
    };
    media_type.type_() == mime::APPLICATION
        && (media_type.subtype() == mime::JSON || media_type.suffix() == Some(mime::JSON))
}

/// Convenience accessors and bounded body decoders for [`Request`].
///
/// Header values, route parameters, cookie values, and credentials remain
/// opaque. Query names and values are decoded exactly once using URI
/// `application/x-www-form-urlencoded` rules (`+` becomes a space and valid
/// `%XX` escapes become their decoded UTF-8 representation); these accessors
/// do not apply a second decoding or silently lowercase application data.
#[async_trait::async_trait]
pub trait RequestExt {
    /// Returns the first matching header value using case-insensitive name matching.
    fn header(&self, name: &str) -> Option<InternedString>;

    /// Returns the first form-url-decoded value for a decoded query name.
    fn query(&self, name: &str) -> Option<&InternedString>;

    /// Returns one case-sensitive route parameter.
    fn param(&self, name: &str) -> Option<&InternedString>;

    /// Looks up one cookie without percent-decoding its wire value.
    ///
    /// Absence is `Ok(None)`. Malformed `Cookie` fields and duplicate matches
    /// are distinct fail-closed errors.
    fn cookie(&self, name: &str) -> Result<Option<String>, RequestCookieError>;

    /// Returns the immutable, once-parsed request cookie jar.
    fn cookies(&self) -> Result<&RequestCookieJar, RequestCookieError>;

    /// Returns the single opaque browser SSE reconnection cursor.
    ///
    /// Absence is `Ok(None)`. Duplicate, oversized, or structurally invalid
    /// `Last-Event-ID` fields are distinct fail-closed client errors. The value
    /// is neither trimmed nor decoded.
    fn last_event_id(&self) -> Result<Option<LastEventId>, LastEventIdError>;

    /// Buffers and decodes one JSON representation.
    async fn json<T>(&self) -> Result<T, RequestError>
    where
        T: serde::de::DeserializeOwned;

    /// Parses a URL-encoded body into an ordered multimap. Repeated names and
    /// wire order are preserved.
    async fn form(&self) -> Result<FormData, RequestError>;

    /// Parses `multipart/form-data` into ordered, exact byte fields.
    ///
    /// The effective transport body limit remains authoritative. The effective
    /// request snapshot additionally bounds per-part bytes, total part count,
    /// and retained metadata per part. Filename and content-type values remain
    /// untrusted metadata and never select a different resource policy.
    async fn multipart(&self) -> Result<MultipartData, RequestError>;

    /// Returns the first `Content-Type` value.
    fn content_type(&self) -> Option<InternedString>;

    /// Check if content type is JSON
    fn is_json(&self) -> bool;

    /// Check if content type is form data
    fn is_form(&self) -> bool;

    /// Check if content type is multipart
    fn is_multipart(&self) -> bool;

    /// Returns the transport-authenticated peer IP as text.
    fn ip(&self) -> Option<InternedString>;

    /// Returns the first `User-Agent` value without normalization.
    fn user_agent(&self) -> Option<InternedString>;

    /// Classifies the exact HTTP method token.
    fn http_method(&self) -> HttpMethod;

    /// Check if request method is GET
    fn is_get(&self) -> bool;

    /// Check if request method is POST
    fn is_post(&self) -> bool;

    /// Check if request method is PUT
    fn is_put(&self) -> bool;

    /// Check if request method is DELETE
    fn is_delete(&self) -> bool;

    /// Access to the configured request body buffer without copying.
    fn raw_body(&self) -> Option<&HttpBuffer>;

    /// Access to request body as raw bytes slice
    fn raw_body_bytes(&self) -> Option<&[u8]>;

    /// Check if request has body data
    fn has_body(&self) -> bool;

    /// Get body size in bytes
    fn body_size(&self) -> usize;
}

#[async_trait::async_trait]
impl RequestExt for Request {
    /// Uses pre-populated header cache from request parsing
    fn header(&self, name: &str) -> Option<InternedString> {
        // Search through RawHeader vec for matching header name
        for header in self.header_cache() {
            if header.name.eq_ignore_ascii_case(name) {
                return Some(preserve_opaque(&header.value));
            }
        }
        None
    }

    /// Looks up the first value in the pre-decoded query multimap.
    fn query(&self, name: &str) -> Option<&InternedString> {
        self.query_first(name)
    }

    fn param(&self, name: &str) -> Option<&InternedString> {
        self.params().get(&preserve_opaque(name))
    }

    fn cookie(&self, name: &str) -> Result<Option<String>, RequestCookieError> {
        validate_request_name(name)?;
        Ok(self.cookies()?.get(name)?.map(str::to_owned))
    }

    fn cookies(&self) -> Result<&RequestCookieJar, RequestCookieError> {
        self.parsed_cookie_jar()
    }

    fn last_event_id(&self) -> Result<Option<LastEventId>, LastEventIdError> {
        request_last_event_id(self)
    }

    async fn json<T>(&self) -> Result<T, RequestError>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut content_types = self
            .header_cache()
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("content-type"));
        let content_type = content_types
            .next()
            .map(|header| header.value.as_str())
            .ok_or_else(|| {
                RequestError::InvalidContentType("request is not a JSON media type".to_string())
            })?;
        if content_types.next().is_some() {
            return Err(RequestError::InvalidContentType(
                "request contains more than one content-type header".to_string(),
            ));
        }
        if !is_json_content_type(content_type) {
            return Err(RequestError::InvalidContentType(
                "request is not a JSON media type".to_string(),
            ));
        }

        self.buffer_body().await.map_err(map_body_read_error)?;

        // Body al
        let body_bytes = self
            .raw_body_bytes()
            .ok_or_else(|| RequestError::NoBody("Request body is required".to_string()))?;

        serde_json::from_slice::<T>(body_bytes).map_err(|_| {
            RequestError::InvalidJson("request JSON representation is malformed".to_string())
        })
    }

    /// Parse form data asynchronously (URL-encoded)
    async fn form(&self) -> Result<FormData, RequestError> {
        let mut content_types = self
            .header_cache()
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("content-type"));
        let content_type = content_types
            .next()
            .map(|header| header.value.as_str())
            .ok_or(RequestError::Form(FormError::WrongContentType))?;
        if content_types.next().is_some() {
            return Err(RequestError::Form(FormError::DuplicateContentType));
        }
        if !is_form_content_type(content_type) {
            return Err(RequestError::Form(FormError::WrongContentType));
        }

        self.buffer_body().await.map_err(map_body_read_error)?;

        // Get body bytes
        // An empty URL-encoded representation is a valid empty form, whether
        // transport represented it as an empty buffer or no retained buffer.
        FormData::parse(self.raw_body_bytes().unwrap_or_default()).map_err(RequestError::Form)
    }

    /// Parse multipart form data asynchronously
    async fn multipart(&self) -> Result<MultipartData, RequestError> {
        let mut content_types = self
            .header_cache()
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("content-type"));
        let content_type = content_types
            .next()
            .map(|header| header.value.as_str())
            .ok_or(RequestError::Multipart(MultipartError::WrongContentType))?;
        if content_types.next().is_some() {
            return Err(RequestError::Multipart(MultipartError::MalformedBody));
        }
        validate_multipart_content_type(content_type).map_err(RequestError::Multipart)?;
        let body_budget = crate::BodyBudget::new(self.max_request_body_bytes())
            .expect("request body budgets are validated at construction");
        let (max_part_bytes, max_parts, max_retained_metadata_bytes) = self.multipart_limits();
        let limits = MultipartLimits::production(
            body_budget,
            max_part_bytes,
            max_parts,
            max_retained_metadata_bytes,
        );
        self.buffer_body_with_limit(body_budget)
            .await
            .map_err(|error| match error {
                RequestBodyError::PayloadTooLarge { limit_bytes } => {
                    RequestError::Multipart(MultipartError::WholeStreamTooLarge { limit_bytes })
                }
                error => map_body_read_error(error),
            })?;
        let body_bytes = self
            .raw_body_bytes()
            .ok_or(RequestError::NoBody("Request body is required".to_string()))?;
        parse_multipart(content_type, body_bytes, limits)
            .await
            .map_err(RequestError::Multipart)
    }

    #[inline]
    fn content_type(&self) -> Option<InternedString> {
        self.header("content-type")
    }

    #[inline(always)]
    fn is_json(&self) -> bool {
        let mut content_types = self
            .header_cache()
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("content-type"));
        let Some(content_type) = content_types.next() else {
            return false;
        };
        content_types.next().is_none() && is_json_content_type(&content_type.value)
    }

    #[inline(always)]
    fn is_form(&self) -> bool {
        self.content_type()
            .map(|content_type| is_form_content_type(content_type.as_str()))
            .unwrap_or(false)
    }

    #[inline(always)]
    fn is_multipart(&self) -> bool {
        self.content_type()
            .and_then(|content_type| {
                content_type.as_str().split(';').next().map(|media_type| {
                    media_type
                        .trim()
                        .eq_ignore_ascii_case("multipart/form-data")
                })
            })
            .unwrap_or(false)
    }

    fn ip(&self) -> Option<InternedString> {
        self.client_ip()
            .map(|client_ip| preserve_opaque_owned(client_ip.to_string()))
    }

    fn user_agent(&self) -> Option<InternedString> {
        self.header("user-agent")
    }

    #[inline]
    fn http_method(&self) -> HttpMethod {
        HttpMethod::from_str(self.method())
    }

    #[inline(always)]
    fn is_get(&self) -> bool {
        self.http_method() == HttpMethod::Get
    }

    #[inline(always)]
    fn is_post(&self) -> bool {
        self.http_method() == HttpMethod::Post
    }

    #[inline(always)]
    fn is_put(&self) -> bool {
        self.http_method() == HttpMethod::Put
    }

    #[inline(always)]
    fn is_delete(&self) -> bool {
        self.http_method() == HttpMethod::Delete
    }

    /// Access to the configured request body buffer without copying.
    /// Returns None if no body is present (e.g., GET requests)
    fn raw_body(&self) -> Option<&HttpBuffer> {
        self.body()
    }

    /// Access to request body as raw bytes slice
    /// Returns None if no body is present (e.g., GET requests)
    fn raw_body_bytes(&self) -> Option<&[u8]> {
        self.body_bytes()
    }

    /// Check if request has body data
    #[inline(always)]
    fn has_body(&self) -> bool {
        self.body().is_some() || self.has_streaming_body()
    }

    /// Get body size in bytes
    /// Returns 0 if no body is present
    #[inline(always)]
    fn body_size(&self) -> usize {
        self.body_bytes()
            .map(|bytes| bytes.len())
            .or_else(|| self.body_size_hint())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::RequestExt;
    use crate::cookie::{RequestCookieError, MAX_REQUEST_COOKIE_PAIRS};
    use crate::request::{Request, RequestBodyError, RequestBodyState, RequestBodyStream};
    use bytes::Bytes;
    use lily_core::structs::RawHeader;
    use lily_error::application::http_api::request::{FormError, MultipartError, RequestError};
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::Duration;

    const MULTIPART_BOUNDARY: &str = "LILY-INTEGRATION";

    fn raw_header(name: &str, value: &str) -> RawHeader {
        RawHeader {
            name: name.to_string(),
            value: value.to_string(),
            line_number: 1,
            raw_line: String::new(),
        }
    }

    fn multipart_body(value: &[u8]) -> Vec<u8> {
        let mut body = format!(
            "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; name=\"value\"\r\n\r\n"
        )
        .into_bytes();
        body.extend_from_slice(value);
        body.extend_from_slice(format!("\r\n--{MULTIPART_BOUNDARY}--\r\n").as_bytes());
        body
    }

    struct ChunkStream {
        chunks: VecDeque<Result<Bytes, RequestBodyError>>,
        hint: (u64, Option<u64>),
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ChunkStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl RequestBodyStream for ChunkStream {
        async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
            self.chunks.pop_front().transpose()
        }

        fn size_hint(&self) -> (u64, Option<u64>) {
            self.hint
        }
    }

    struct PendingDropStream(Arc<AtomicBool>);

    impl Drop for PendingDropStream {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl RequestBodyStream for PendingDropStream {
        async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
            pending::<()>().await;
            unreachable!("pending body stream cannot complete")
        }

        fn size_hint(&self) -> (u64, Option<u64>) {
            (0, None)
        }
    }

    struct ChunkThenPendingStream {
        first: Option<Bytes>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ChunkThenPendingStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl RequestBodyStream for ChunkThenPendingStream {
        async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
            if let Some(first) = self.first.take() {
                return Ok(Some(first));
            }
            pending::<()>().await;
            unreachable!("pending body stream cannot complete")
        }

        fn size_hint(&self) -> (u64, Option<u64>) {
            (0, None)
        }
    }

    #[test]
    fn header_values_are_preserved_and_names_remain_case_insensitive() {
        let mut request = Request::new_test("GET", "/");
        request.add_test_header("X-Content-Canonical", "Content-Type");
        request.add_test_header("X-Content-Upper", "CONTENT-TYPE");
        request.add_test_header("X-Auth-Canonical", "Authorization");
        request.add_test_header("X-Auth-Upper", "AUTHORIZATION");
        request.add_test_header("X-MiXeD-Name", "Ürün-İ");

        for (name, expected) in [
            ("x-content-canonical", "Content-Type"),
            ("X-CONTENT-UPPER", "CONTENT-TYPE"),
            ("x-auth-canonical", "Authorization"),
            ("X-AUTH-UPPER", "AUTHORIZATION"),
            ("x-mixed-name", "Ürün-İ"),
        ] {
            let value = request.header(name).expect("header must be found");
            assert_eq!(value.as_str(), expected);
        }
    }

    #[test]
    fn cookie_lookup_distinguishes_absence_and_preserves_wire_value() {
        let mut request = Request::new_test("GET", "/");
        request.add_test_header("Cookie", "theme=dark; session=opaque%2Ftoken");

        assert_eq!(
            request.cookie("session").unwrap().as_deref(),
            Some("opaque%2Ftoken")
        );
        assert_eq!(request.cookie("missing"), Ok(None));
    }

    #[test]
    fn cookie_lookup_and_whole_jar_share_one_immutable_parse_result() {
        let mut request = Request::new_test("GET", "/");
        request.add_test_header("Cookie", "theme=dark; session=opaque%2Ftoken");
        request.add_test_header("cookie", "locale=tr");

        let first = request.cookies().unwrap();
        let second = request.cookies().unwrap();
        assert!(std::ptr::eq(first, second));
        assert_eq!(
            first
                .iter()
                .map(|cookie| (cookie.name(), cookie.value()))
                .collect::<Vec<_>>(),
            vec![
                ("theme", "dark"),
                ("session", "opaque%2Ftoken"),
                ("locale", "tr")
            ]
        );
        assert_eq!(
            request.cookie("session").unwrap().as_deref(),
            first.get("session").unwrap()
        );
    }

    #[test]
    fn cookie_lookup_rejects_duplicates_across_every_field_line() {
        let mut same_line = Request::new_test("GET", "/");
        same_line.add_test_header("Cookie", "session=first; session=second");
        assert_eq!(
            same_line.cookie("session"),
            Err(RequestCookieError::Duplicate)
        );

        let mut multiple_lines = Request::new_test("GET", "/");
        multiple_lines.add_test_header("Cookie", "theme=dark; session=first");
        multiple_lines.add_test_header("cookie", "session=second; locale=tr");
        assert_eq!(
            multiple_lines.cookie("session"),
            Err(RequestCookieError::Duplicate)
        );
    }

    #[test]
    fn cookie_lookup_fails_closed_on_malformed_or_excessive_input() {
        let mut malformed = Request::new_test("GET", "/");
        malformed.add_test_header("Cookie", "session=opaque; missing-pair");
        assert_eq!(
            malformed.cookie("session"),
            Err(RequestCookieError::Malformed)
        );

        let mut empty = Request::new_test("GET", "/");
        empty.add_test_header("Cookie", "");
        assert_eq!(empty.cookie("session"), Err(RequestCookieError::Malformed));

        assert_eq!(
            malformed.cookie("invalid name"),
            Err(RequestCookieError::InvalidName)
        );

        let mut excessive = Request::new_test("GET", "/");
        let header = (0..=MAX_REQUEST_COOKIE_PAIRS)
            .map(|index| format!("c{index}=v"))
            .collect::<Vec<_>>()
            .join("; ");
        excessive.add_test_header("Cookie", &header);
        assert_eq!(
            excessive.cookie("session"),
            Err(RequestCookieError::TooManyPairs {
                limit: MAX_REQUEST_COOKIE_PAIRS
            })
        );
    }

    #[test]
    fn cookie_lookup_errors_never_expose_cookie_material() {
        let mut request = Request::new_test("GET", "/");
        request.add_test_header("Cookie", "session=first-secret; session=second-secret");
        let error = request.cookie("session").unwrap_err();
        let output = format!("{error:?} {error}");

        assert!(!output.contains("session"));
        assert!(!output.contains("first-secret"));
        assert!(!output.contains("second-secret"));
    }

    #[test]
    fn route_parameter_names_and_values_are_exact_and_case_sensitive() {
        let mut request = Request::new_test("GET", "/");
        request.set_params(vec![
            ("Content-Type".to_string(), "AUTHORIZATION".to_string()),
            ("content-type".to_string(), "Content-Type".to_string()),
        ]);

        assert_eq!(
            request.param("Content-Type").map(|value| value.as_str()),
            Some("AUTHORIZATION")
        );
        assert_eq!(
            request.param("content-type").map(|value| value.as_str()),
            Some("Content-Type")
        );
        assert!(request.param("CONTENT-TYPE").is_none());
        assert!(request
            .params()
            .keys()
            .any(|key| key.as_str() == "Content-Type"));
    }

    #[tokio::test]
    async fn multipart_helper_preserves_the_existing_buffered_request_api() {
        let body = multipart_body(b"exact-value");
        let request = Request::from_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "Content-Type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            &body,
        )
        .await
        .unwrap();

        let parsed = request.multipart().await.unwrap();
        assert_eq!(
            parsed
                .named("value")
                .map(|field| field.text().unwrap())
                .collect::<Vec<_>>(),
            ["exact-value"]
        );
        assert_eq!(request.body_state(), RequestBodyState::Buffered);
    }

    #[tokio::test]
    async fn multipart_helper_lazily_buffers_and_releases_a_transport_stream() {
        let body = multipart_body(b"streamed-value");
        let split = body.len() / 2;
        let dropped = Arc::new(AtomicBool::new(false));
        let stream = ChunkStream {
            chunks: [
                Ok(Bytes::copy_from_slice(&body[..split])),
                Ok(Bytes::copy_from_slice(&body[split..])),
            ]
            .into_iter()
            .collect(),
            hint: (body.len() as u64, Some(body.len() as u64)),
            dropped: Arc::clone(&dropped),
        };
        let request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "content-type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            Some(Box::new(stream)),
            body.len(),
        )
        .unwrap();

        let parsed = request.multipart().await.unwrap();
        assert_eq!(
            parsed.named("value").next().unwrap().text().unwrap(),
            "streamed-value"
        );
        assert_eq!(request.body_state(), RequestBodyState::Buffered);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn multipart_helper_enforces_the_transport_limit_snapshot() {
        let body = multipart_body(b"larger-than-snapshot");
        let limit = body.len() - 1;
        let dropped = Arc::new(AtomicBool::new(false));
        let stream = ChunkStream {
            chunks: [Ok(Bytes::copy_from_slice(&body))].into_iter().collect(),
            hint: (body.len() as u64, Some(body.len() as u64)),
            dropped: Arc::clone(&dropped),
        };
        let request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "content-type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            Some(Box::new(stream)),
            limit,
        )
        .unwrap();

        assert_eq!(
            request.multipart().await.unwrap_err(),
            RequestError::Multipart(MultipartError::WholeStreamTooLarge { limit_bytes: limit })
        );
        assert_eq!(request.body_state(), RequestBodyState::Failed);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn multipart_transport_failures_are_typed_and_safe() {
        let dropped = Arc::new(AtomicBool::new(false));
        let stream = ChunkStream {
            chunks: [Err(RequestBodyError::TransportInterrupted)]
                .into_iter()
                .collect(),
            hint: (0, None),
            dropped: Arc::clone(&dropped),
        };
        let request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "content-type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            Some(Box::new(stream)),
            1024,
        )
        .unwrap();

        let error = request.multipart().await.unwrap_err();
        assert_eq!(
            error,
            RequestError::BodyReadFailed("request body transport was interrupted".to_string())
        );
        assert_eq!(request.body_state(), RequestBodyState::Failed);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn multipart_timeout_and_buffer_failures_keep_typed_statuses() {
        for (body_error, expected_request, expected_status) in [
            (
                RequestBodyError::ReadTimedOut,
                RequestError::BodyReadTimedOut,
                408,
            ),
            (
                RequestBodyError::BufferUnavailable,
                RequestError::BodyBufferUnavailable,
                500,
            ),
        ] {
            let dropped = Arc::new(AtomicBool::new(false));
            let stream = ChunkStream {
                chunks: [Err(body_error)].into_iter().collect(),
                hint: (0, None),
                dropped: Arc::clone(&dropped),
            };
            let request = Request::from_streaming_transport_parts(
                "POST".to_string(),
                "/upload".to_string(),
                vec![raw_header(
                    "content-type",
                    &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
                )],
                Some(Box::new(stream)),
                1024,
            )
            .unwrap();

            let error = request.multipart().await.unwrap_err();
            assert_eq!(error, expected_request);
            assert_eq!(
                lily_error::application::http_api::HttpApiError::from(error)
                    .http_status()
                    .0,
                expected_status
            );
            assert_eq!(request.body_state(), RequestBodyState::Failed);
            assert!(dropped.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn multipart_rejects_switching_from_incremental_consumption() {
        let body = multipart_body(b"value");
        let dropped = Arc::new(AtomicBool::new(false));
        let stream = ChunkStream {
            chunks: [Ok(Bytes::copy_from_slice(&body))].into_iter().collect(),
            hint: (body.len() as u64, Some(body.len() as u64)),
            dropped,
        };
        let mut request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "content-type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            Some(Box::new(stream)),
            body.len(),
        )
        .unwrap();

        assert!(request.next_body_chunk().await.unwrap().is_some());
        assert_eq!(
            request.multipart().await.unwrap_err(),
            RequestError::BodyReadFailed(
                "request body consumption mode is already locked".to_string()
            )
        );
    }

    #[tokio::test]
    async fn cancelling_multipart_keeps_stream_ownership_with_the_request() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "content-type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            Some(Box::new(ChunkThenPendingStream {
                first: Some(Bytes::from_static(b"partially-consumed")),
                dropped: Arc::clone(&dropped),
            })),
            1024,
        )
        .unwrap();

        let mut parse = Box::pin(request.multipart());
        assert!(
            tokio::time::timeout(Duration::from_millis(5), parse.as_mut())
                .await
                .is_err()
        );
        drop(parse);
        assert_eq!(request.body_state(), RequestBodyState::Buffering);
        assert!(!dropped.load(Ordering::SeqCst));
        assert_eq!(
            request.multipart().await.unwrap_err(),
            RequestError::BodyReadFailed(
                "request body consumption mode is already locked".to_string()
            )
        );
        assert_eq!(
            request.next_body_chunk().await.unwrap_err(),
            RequestBodyError::AlreadyStreaming
        );
        drop(request);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn invalid_content_type_fails_before_polling_the_body() {
        let dropped = Arc::new(AtomicBool::new(false));
        let request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header("content-type", "application/json")],
            Some(Box::new(PendingDropStream(Arc::clone(&dropped)))),
            1024,
        )
        .unwrap();

        let result = tokio::time::timeout(Duration::from_millis(50), request.multipart())
            .await
            .expect("content-type validation must complete before polling a pending body");
        assert_eq!(
            result.unwrap_err(),
            RequestError::Multipart(MultipartError::WrongContentType)
        );
        assert_eq!(request.body_state(), RequestBodyState::Pending);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(request);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn json_content_type_is_mime_aware_and_value_redacting() {
        for content_type in [
            "application/json",
            "Application/JSON; charset=utf-8",
            "application/problem+json",
        ] {
            let request = Request::from_transport_parts(
                "POST".to_string(),
                "/json".to_string(),
                vec![raw_header("Content-Type", content_type)],
                br#"{"accepted":true}"#,
            )
            .await
            .unwrap();
            let value = request.json::<serde_json::Value>().await.unwrap();
            assert_eq!(value["accepted"], true);
        }

        const SECRET_CONTENT_TYPE: &str = "application/body-secret-83f1";
        let request = Request::from_transport_parts(
            "POST".to_string(),
            "/json".to_string(),
            vec![raw_header("Content-Type", SECRET_CONTENT_TYPE)],
            b"{}",
        )
        .await
        .unwrap();
        let error = request.json::<serde_json::Value>().await.unwrap_err();
        assert_eq!(
            error,
            RequestError::InvalidContentType("request is not a JSON media type".to_string())
        );
        assert!(!format!("{error:?} {error}").contains(SECRET_CONTENT_TYPE));
    }

    #[tokio::test]
    async fn json_rejects_duplicate_content_type_before_polling_the_body() {
        let dropped = Arc::new(AtomicBool::new(false));
        let request = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/json".to_string(),
            vec![
                raw_header("Content-Type", "application/json"),
                raw_header("content-type", "application/problem+json"),
            ],
            Some(Box::new(PendingDropStream(Arc::clone(&dropped)))),
            1024,
        )
        .unwrap();

        assert!(!request.is_json());
        assert_eq!(
            request.json::<serde_json::Value>().await.unwrap_err(),
            RequestError::InvalidContentType(
                "request contains more than one content-type header".to_string()
            )
        );
        assert_eq!(request.body_state(), RequestBodyState::Pending);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(request);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn duplicate_content_type_headers_fail_before_polling_the_body() {
        for second in [
            format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            "application/json".to_string(),
        ] {
            let dropped = Arc::new(AtomicBool::new(false));
            let request = Request::from_streaming_transport_parts(
                "POST".to_string(),
                "/upload".to_string(),
                vec![
                    raw_header(
                        "Content-Type",
                        &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
                    ),
                    raw_header("content-type", &second),
                ],
                Some(Box::new(PendingDropStream(Arc::clone(&dropped)))),
                1024,
            )
            .unwrap();

            assert_eq!(
                request.multipart().await.unwrap_err(),
                RequestError::Multipart(MultipartError::MalformedBody)
            );
            assert_eq!(request.body_state(), RequestBodyState::Pending);
            assert!(!dropped.load(Ordering::SeqCst));
            drop(request);
            assert!(dropped.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn form_rejects_duplicate_content_type_before_polling_and_preserves_order() {
        let dropped = Arc::new(AtomicBool::new(false));
        let duplicate = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/form".to_string(),
            vec![
                raw_header("Content-Type", "application/x-www-form-urlencoded"),
                raw_header("content-type", "application/x-www-form-urlencoded"),
            ],
            Some(Box::new(PendingDropStream(Arc::clone(&dropped)))),
            1024,
        )
        .unwrap();
        assert_eq!(
            duplicate.form().await.unwrap_err(),
            RequestError::Form(FormError::DuplicateContentType)
        );
        assert_eq!(duplicate.body_state(), RequestBodyState::Pending);
        drop(duplicate);
        assert!(dropped.load(Ordering::SeqCst));

        let ordered = Request::from_transport_parts(
            "POST".to_string(),
            "/form".to_string(),
            vec![raw_header(
                "Content-Type",
                "application/x-www-form-urlencoded; charset=UTF-8",
            )],
            b"tag=first+value&tag=%2Bsecond&city=%C4%B0zmir",
        )
        .await
        .unwrap();
        let form = ordered.form().await.unwrap();
        assert_eq!(
            form.iter().collect::<Vec<_>>(),
            [
                ("tag", "first value"),
                ("tag", "+second"),
                ("city", "İzmir")
            ]
        );
    }

    #[tokio::test]
    async fn effective_multipart_snapshot_controls_the_parser() {
        let body = multipart_body(b"four");
        let mut request = Request::from_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "Content-Type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            &body,
        )
        .await
        .unwrap();
        request.set_multipart_limits(3, 1, 1024).unwrap();
        assert_eq!(
            request.multipart().await.unwrap_err(),
            RequestError::Multipart(MultipartError::FieldTooLarge { limit_bytes: 3 })
        );

        let two_parts = format!(
            "--{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\n1\r\n\
             --{MULTIPART_BOUNDARY}\r\nContent-Disposition: form-data; name=\"b\"\r\n\r\n2\r\n\
             --{MULTIPART_BOUNDARY}--\r\n"
        )
        .into_bytes();
        let mut request = Request::from_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "Content-Type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            &two_parts,
        )
        .await
        .unwrap();
        request.set_multipart_limits(1024, 1, 1024).unwrap();
        assert_eq!(
            request.multipart().await.unwrap_err(),
            RequestError::Multipart(MultipartError::TooManyParts { limit: 1 })
        );

        let mut request = Request::from_transport_parts(
            "POST".to_string(),
            "/upload".to_string(),
            vec![raw_header(
                "Content-Type",
                &format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
            )],
            &body,
        )
        .await
        .unwrap();
        request.set_multipart_limits(1024, 1, 8).unwrap();
        assert_eq!(
            request.multipart().await.unwrap_err(),
            RequestError::Multipart(MultipartError::RetainedMetadataTooLarge { limit_bytes: 8 })
        );
    }

    #[test]
    fn streaming_constructor_rejects_invalid_body_limit_snapshots() {
        for limit in [0, 1024 * 1024 * 1024 + 1] {
            let error = Request::from_streaming_transport_parts(
                "POST".to_string(),
                "/upload".to_string(),
                Vec::new(),
                None,
                limit,
            )
            .unwrap_err();
            assert_eq!(error.error_code(), "CONFIGURATION_ERROR");
        }
    }
}
