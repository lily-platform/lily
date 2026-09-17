use std::fmt;
use std::future::Future;
use std::ops::Deref;

use bytes::Bytes;
use lily_error::application::http_api::request::{FormError, RequestError};
use lily_error::application::http_api::HttpApiError;
use lily_injection::Extensions;
use lily_web_core::{Json, Request, RequestBodyError, RequestBodyReader, RequestExt};
use serde::de::DeserializeOwned;

use super::FromRequest;

/// Typed URL-encoded form binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Form<T>(
    /// Deserialized application form value.
    pub T,
);

impl<T> Form<T> {
    /// Returns the bound form value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Owned, bounded bytes retained by the request's whole-body authority.
#[derive(Clone, PartialEq, Eq)]
pub struct RawBody(Bytes);

impl RawBody {
    /// Borrows the exact request representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// Transfers the exact request representation without another copy.
    #[must_use]
    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

impl AsRef<[u8]> for RawBody {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Deref for RawBody {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl fmt::Debug for RawBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawBody")
            .field("bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// Terminal, pull-based ownership of one request body.
///
/// Every call yields at most one transport chunk. Dropping this value drops
/// the underlying HTTP body; Lily never starts a detached reader task.
pub struct BodyStream {
    reader: RequestBodyReader,
}

impl BodyStream {
    /// Pulls the next bounded chunk. `None` is a repeatable terminal state.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
        self.reader.next_chunk().await
    }

    /// Returns the remaining lower and upper byte bounds known by transport.
    #[must_use]
    pub fn size_hint(&self) -> (u64, Option<u64>) {
        self.reader.size_hint()
    }

    /// Returns the number of bytes yielded so far.
    #[must_use]
    pub fn bytes_read(&self) -> usize {
        self.reader.bytes_read()
    }
}

impl fmt::Debug for BodyStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BodyStream")
            .field("size_hint", &self.size_hint())
            .field("bytes_read", &self.bytes_read())
            .finish_non_exhaustive()
    }
}

/// Stable, value-redacting failures produced while selecting or decoding an
/// action request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestBodyRejection {
    /// The request media type is absent, duplicated, malformed or unsupported.
    UnsupportedMediaType,
    /// A required typed representation had no request body.
    MissingBody,
    /// JSON syntax or DTO binding failed.
    InvalidJson,
    /// URL-encoded form parsing or DTO binding failed.
    InvalidForm,
    /// Another body representation was structurally invalid.
    InvalidBody,
    /// The effective transport byte limit was exceeded.
    PayloadTooLarge {
        /// Effective maximum body size configured for this request.
        limit_bytes: usize,
    },
    /// The body-read deadline expired.
    ReadTimedOut,
    /// The peer interrupted the body stream.
    TransportInterrupted,
    /// Request trailers violated transport limits.
    InvalidTrailers,
    /// A caller attempted to switch or repeat body consumption modes.
    AlreadyStreaming,
    /// The configured buffer backend could not retain the body.
    BufferUnavailable,
}

impl fmt::Display for RequestBodyRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedMediaType => {
                formatter.write_str("request media type is not supported")
            }
            Self::MissingBody => formatter.write_str("request body is required"),
            Self::InvalidJson => formatter.write_str("request JSON representation is invalid"),
            Self::InvalidForm => formatter.write_str("request form representation is invalid"),
            Self::InvalidBody => formatter.write_str("request body representation is invalid"),
            Self::PayloadTooLarge { limit_bytes } => {
                write!(formatter, "request body exceeds {limit_bytes} bytes")
            }
            Self::ReadTimedOut => formatter.write_str("request body read deadline exceeded"),
            Self::TransportInterrupted => {
                formatter.write_str("request body transport was interrupted")
            }
            Self::InvalidTrailers => {
                formatter.write_str("request trailers violate configured limits")
            }
            Self::AlreadyStreaming => {
                formatter.write_str("request body consumption mode is already locked")
            }
            Self::BufferUnavailable => formatter.write_str("request body buffer is unavailable"),
        }
    }
}

impl std::error::Error for RequestBodyRejection {}

impl From<RequestBodyError> for RequestBodyRejection {
    fn from(error: RequestBodyError) -> Self {
        match error {
            RequestBodyError::PayloadTooLarge { limit_bytes } => {
                Self::PayloadTooLarge { limit_bytes }
            }
            RequestBodyError::ReadTimedOut => Self::ReadTimedOut,
            RequestBodyError::TransportInterrupted => Self::TransportInterrupted,
            RequestBodyError::InvalidTrailers => Self::InvalidTrailers,
            RequestBodyError::AlreadyStreaming => Self::AlreadyStreaming,
            RequestBodyError::BufferUnavailable => Self::BufferUnavailable,
        }
    }
}

impl From<RequestError> for RequestBodyRejection {
    fn from(error: RequestError) -> Self {
        match error {
            RequestError::InvalidJson(_) => Self::InvalidJson,
            RequestError::InvalidUtf8(_) => Self::InvalidBody,
            RequestError::NoBody(_) => Self::MissingBody,
            RequestError::InvalidContentType(_) => Self::UnsupportedMediaType,
            RequestError::BodyTooLarge { limit_bytes } => Self::PayloadTooLarge { limit_bytes },
            RequestError::BodyReadTimedOut => Self::ReadTimedOut,
            RequestError::BodyBufferUnavailable => Self::BufferUnavailable,
            RequestError::BodyReadFailed(_) => Self::InvalidBody,
            RequestError::Form(FormError::WrongContentType) => Self::UnsupportedMediaType,
            RequestError::Form(_) => Self::InvalidForm,
            RequestError::Multipart(_) => Self::InvalidBody,
        }
    }
}

impl From<RequestBodyRejection> for HttpApiError {
    fn from(rejection: RequestBodyRejection) -> Self {
        match rejection {
            RequestBodyRejection::UnsupportedMediaType => {
                Self::UnsupportedMediaType("request media type is not supported".to_string())
            }
            RequestBodyRejection::MissingBody => {
                Self::InvalidRequestBody("request body is required".to_string())
            }
            RequestBodyRejection::InvalidJson => {
                Self::JsonError("request JSON representation is invalid".to_string())
            }
            RequestBodyRejection::InvalidForm => {
                Self::InvalidRequestBody("request form representation is invalid".to_string())
            }
            RequestBodyRejection::InvalidBody => {
                Self::InvalidRequestBody("request body representation is invalid".to_string())
            }
            RequestBodyRejection::PayloadTooLarge { limit_bytes } => {
                Self::PayloadTooLarge(format!("request body exceeds {limit_bytes} bytes"))
            }
            RequestBodyRejection::ReadTimedOut => {
                Self::RequestTimeout("request body read deadline exceeded".to_string())
            }
            RequestBodyRejection::TransportInterrupted => {
                Self::InvalidRequestBody("request body transport was interrupted".to_string())
            }
            RequestBodyRejection::InvalidTrailers => {
                Self::InvalidHttpHeader("request trailers violate configured limits".to_string())
            }
            RequestBodyRejection::AlreadyStreaming => Self::InvalidRequestBody(
                "request body consumption mode is already locked".to_string(),
            ),
            RequestBodyRejection::BufferUnavailable => {
                Self::InternalError("request body buffer is unavailable".to_string())
            }
        }
    }
}

impl<T> FromRequest for Json<T>
where
    T: DeserializeOwned + Send,
{
    type Rejection = RequestBodyRejection;

    async fn from_request(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        request
            .json::<T>()
            .await
            .map(Self)
            .map_err(RequestBodyRejection::from)
    }
}

impl<T> FromRequest for Form<T>
where
    T: DeserializeOwned + Send,
{
    type Rejection = RequestBodyRejection;

    async fn from_request(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        let form = request.form().await.map_err(RequestBodyRejection::from)?;
        serde_html_form::from_str(&form.to_url_encoded())
            .map(Self)
            .map_err(|_| RequestBodyRejection::InvalidForm)
    }
}

impl FromRequest for RawBody {
    type Rejection = RequestBodyRejection;

    async fn from_request(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        request
            .buffer_body()
            .await
            .map(|body| {
                Self(
                    body.map(|body| Bytes::copy_from_slice(body.as_slice()))
                        .unwrap_or_default(),
                )
            })
            .map_err(RequestBodyRejection::from)
    }
}

impl FromRequest for BodyStream {
    type Rejection = RequestBodyRejection;

    fn from_request(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let reader = request
            .take_body_reader()
            .map(|reader| Self { reader })
            .map_err(RequestBodyRejection::from);
        std::future::ready(reader)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use lily_core::RawHeader;
    use lily_web_core::{Json, Request, RequestBodyError, RequestBodyState, RequestBodyStream};
    use serde::Deserialize;

    use super::{BodyStream, Form, RawBody};
    use crate::extractor::extract_request;
    use crate::ApplicationContainer;

    const SECRET_VALUE: &str = "body-secret-4c81";

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct JsonInput {
        name: String,
        count: u32,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct FormInput {
        enabled: bool,
        #[serde(default)]
        tag: Vec<String>,
    }

    struct FixtureStream {
        next: Option<Result<Bytes, RequestBodyError>>,
    }

    #[lily_http_api::async_trait::async_trait]
    impl RequestBodyStream for FixtureStream {
        async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
            match self.next.take() {
                Some(Ok(bytes)) => Ok(Some(bytes)),
                Some(Err(error)) => Err(error),
                None => Ok(None),
            }
        }

        fn size_hint(&self) -> (u64, Option<u64>) {
            (0, None)
        }
    }

    fn header(name: &str, value: &str) -> RawHeader {
        RawHeader {
            name: name.to_string(),
            value: value.to_string(),
            line_number: 1,
            raw_line: String::new(),
        }
    }

    async fn request(content_type: Option<&str>, body: &[u8]) -> Request {
        let headers = content_type
            .map(|value| vec![header("Content-Type", value)])
            .unwrap_or_default();
        Request::from_transport_parts("POST".to_string(), "/body".to_string(), headers, body)
            .await
            .expect("fixture request is valid")
    }

    #[tokio::test]
    async fn json_and_form_bind_through_the_authoritative_bounded_parsers() {
        let container = ApplicationContainer::build()
            .await
            .expect("test container builds");
        let extensions = container.services();

        let json_wire = br#"{"name":"lily","count":7}"#;
        let mut json = request(Some("application/problem+json; charset=utf-8"), json_wire).await;
        let Json(input) = extract_request::<Json<JsonInput>>(&mut json, &extensions)
            .await
            .expect("structured-suffix JSON binds");
        assert_eq!(
            input,
            JsonInput {
                name: "lily".to_string(),
                count: 7,
            }
        );
        assert_eq!(json.body_bytes(), Some(json_wire.as_slice()));

        let mut form = request(
            Some("application/x-www-form-urlencoded; charset=UTF-8"),
            b"enabled=true&tag=first+value&tag=%2Bsecond",
        )
        .await;
        let Form(input) = extract_request::<Form<FormInput>>(&mut form, &extensions)
            .await
            .expect("URL-encoded form binds");
        assert!(input.enabled);
        assert_eq!(input.tag, ["first value", "+second"]);

        let mut duplicate_scalar = request(
            Some("application/x-www-form-urlencoded"),
            b"enabled=true&enabled=false",
        )
        .await;
        assert_eq!(
            extract_request::<Form<FormInput>>(&mut duplicate_scalar, &extensions)
                .await
                .expect_err("duplicate scalar form field is rejected")
                .http_status()
                .0,
            400
        );

        container.close().await.expect("test container closes");
    }

    #[tokio::test]
    async fn typed_body_rejections_are_status_exact_and_value_redacting() {
        let container = ApplicationContainer::build()
            .await
            .expect("test container builds");
        let extensions = container.services();

        let mut wrong_type = request(Some("text/plain"), br#"{"name":"lily","count":7}"#).await;
        assert_eq!(
            extract_request::<Json<JsonInput>>(&mut wrong_type, &extensions)
                .await
                .err()
                .expect("wrong JSON media type is rejected")
                .http_status()
                .0,
            415
        );

        let mut empty = request(Some("application/json"), b"").await;
        assert_eq!(
            extract_request::<Json<JsonInput>>(&mut empty, &extensions)
                .await
                .err()
                .expect("empty JSON body is rejected")
                .http_status()
                .0,
            400
        );

        let mut malformed = request(
            Some("application/json"),
            format!(r#"{{"name":"{SECRET_VALUE}","count":}}"#).as_bytes(),
        )
        .await;
        let error = extract_request::<Json<JsonInput>>(&mut malformed, &extensions)
            .await
            .err()
            .expect("malformed JSON is rejected");
        assert_eq!(error.http_status().0, 400);
        for output in [
            format!("{error}"),
            format!("{error:?}"),
            String::from_utf8(error.to_public_json().unwrap()).unwrap(),
        ] {
            assert!(!output.contains(SECRET_VALUE));
        }

        let mut oversized = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/body".to_string(),
            Vec::new(),
            Some(Box::new(FixtureStream {
                next: Some(Ok(Bytes::from_static(b"12345"))),
            })),
            4,
        )
        .expect("bounded request builds");
        assert_eq!(
            extract_request::<RawBody>(&mut oversized, &extensions)
                .await
                .expect_err("oversized body is rejected")
                .http_status()
                .0,
            413
        );

        let mut timeout = Request::from_streaming_transport_parts(
            "POST".to_string(),
            "/body".to_string(),
            Vec::new(),
            Some(Box::new(FixtureStream {
                next: Some(Err(RequestBodyError::ReadTimedOut)),
            })),
            32,
        )
        .expect("bounded request builds");
        assert_eq!(
            extract_request::<RawBody>(&mut timeout, &extensions)
                .await
                .expect_err("body timeout is typed")
                .http_status()
                .0,
            408
        );

        container.close().await.expect("test container closes");
    }

    #[tokio::test]
    async fn raw_and_streaming_bodies_preserve_the_single_authority_contract() {
        let container = ApplicationContainer::build()
            .await
            .expect("test container builds");
        let extensions = container.services();

        let mut raw_request = request(None, b"exact-raw-body").await;
        let raw = extract_request::<RawBody>(&mut raw_request, &extensions)
            .await
            .expect("raw body extracts");
        assert_eq!(raw.as_bytes(), b"exact-raw-body");
        assert_eq!(raw_request.body_bytes(), Some(b"exact-raw-body".as_slice()));

        let mut stream_request = request(None, b"pull-body").await;
        let mut body_stream = extract_request::<BodyStream>(&mut stream_request, &extensions)
            .await
            .expect("streaming ownership transfers");
        assert_eq!(body_stream.size_hint(), (9, Some(9)));
        assert_eq!(
            stream_request.buffer_body().await.unwrap_err(),
            RequestBodyError::AlreadyStreaming
        );
        assert_eq!(
            body_stream.next_chunk().await.unwrap().unwrap(),
            Bytes::from_static(b"pull-body")
        );
        assert_eq!(body_stream.bytes_read(), 9);
        assert!(body_stream.next_chunk().await.unwrap().is_none());
        assert_eq!(stream_request.body_state(), RequestBodyState::Complete);
        assert_eq!(stream_request.streamed_body_bytes(), 9);

        container.close().await.expect("test container closes");
    }
}
