//! Response Builder module
//!
//! Provides a fluent API for constructing HTTP responses, particularly useful
//! for testing, mocking, and creating custom responses.

use serde::Serialize;
use std::{
    fmt,
    time::{Duration, SystemTime},
};
use url::Url;

use super::{HttpVersion, Response, ResponseMetadata, StatusCode};
use crate::body::{BinaryBody, Body, JsonBody, TextBody};
use crate::error::{HttpClientError, Result};
use crate::header::HeaderMap;

/// Builder for constructing HTTP responses with a fluent API
///
/// Provides convenient methods for setting status codes, headers, bodies,
/// and metadata when creating responses programmatically.
pub struct ResponseBuilder {
    status: StatusCode,
    headers: HeaderMap,
    body: Option<Box<dyn Body>>,
    version: HttpVersion,
    url: Option<Url>,
    metadata: ResponseMetadata,
}

impl fmt::Debug for ResponseBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseBuilder")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("body_present", &self.body.is_some())
            .field("version", &self.version)
            .field("url", &self.url.as_ref().map(|_| "[REDACTED]"))
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl ResponseBuilder {
    /// Create a new ResponseBuilder with default values
    pub fn new() -> Self {
        Self {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: None,
            version: HttpVersion::Http11,
            url: None,
            metadata: ResponseMetadata::default(),
        }
    }

    /// Set the status code
    pub fn status(mut self, status: StatusCode) -> Self {
        self.status = status;
        self
    }

    /// Set the status code from a u16
    pub fn status_code(mut self, code: u16) -> Self {
        self.status = StatusCode::from_u16(code);
        self
    }

    /// Set the HTTP version
    pub fn version(mut self, version: HttpVersion) -> Self {
        self.version = version;
        self
    }

    /// Set the final URL
    pub fn url(mut self, url: Url) -> Self {
        self.url = Some(url);
        self
    }

    /// Set the final URL from a string
    pub fn url_str(mut self, url: &str) -> Result<Self> {
        let parsed_url = Url::parse(url)
            .map_err(|e| HttpClientError::request_building(format!("Invalid URL: {e}")))?;
        self.url = Some(parsed_url);
        Ok(self)
    }

    /// Add a header
    pub fn header(mut self, name: &str, value: &str) -> Result<Self> {
        self.headers.insert(name, value)?;
        Ok(self)
    }

    /// Add multiple headers from an iterator
    pub fn headers<I, K, V>(mut self, headers: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        for (name, value) in headers {
            self.headers.insert(name.as_ref(), value.as_ref())?;
        }
        Ok(self)
    }

    /// Set the Content-Type header
    pub fn content_type(mut self, content_type: &str) -> Result<Self> {
        self.headers.insert("Content-Type", content_type)?;
        Ok(self)
    }

    /// Set a JSON body from a serializable value
    pub fn json<T: Serialize>(mut self, value: &T) -> Result<Self> {
        let json_body = JsonBody::new(value)?;
        self.body = Some(Box::new(json_body));
        Ok(self)
    }

    /// Set a JSON body from a string
    pub fn json_str(mut self, json: &str) -> Self {
        let json_body = JsonBody::from_string(json);
        self.body = Some(Box::new(json_body));
        self
    }

    /// Set a text body
    pub fn text(mut self, text: impl Into<String>) -> Self {
        let text_body = TextBody::new(text);
        self.body = Some(Box::new(text_body));
        self
    }

    /// Set a binary body
    pub fn binary(mut self, data: impl Into<Vec<u8>>) -> Self {
        let bytes_data = data.into();
        let binary_body = BinaryBody::new(bytes::Bytes::from(bytes_data));
        self.body = Some(Box::new(binary_body));
        self
    }

    /// Set a custom body
    pub fn body(mut self, body: Box<dyn Body>) -> Self {
        self.body = Some(body);
        self
    }

    /// Set response metadata
    pub fn metadata(mut self, metadata: ResponseMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Set request start time
    pub fn request_start(mut self, start: SystemTime) -> Self {
        self.metadata.request_start = Some(start);
        self
    }

    /// Set response time
    pub fn response_time(mut self, time: SystemTime) -> Self {
        self.metadata.response_time = Some(time);
        self
    }

    /// Set duration
    pub fn duration(mut self, duration: Duration) -> Self {
        self.metadata.duration = Some(duration);
        self
    }

    /// Set content length
    pub fn content_length(mut self, length: u64) -> Self {
        self.metadata.content_length = Some(length);
        self
    }

    /// Set from cache flag
    pub fn from_cache(mut self, from_cache: bool) -> Self {
        self.metadata.from_cache = from_cache;
        self
    }

    /// Set redirect count
    pub fn redirect_count(mut self, count: u32) -> Self {
        self.metadata.redirect_count = count;
        self
    }

    /// Set remote address
    pub fn remote_addr(mut self, addr: String) -> Self {
        self.metadata.remote_addr = Some(addr);
        self
    }

    /// Set local address
    pub fn local_addr(mut self, addr: String) -> Self {
        self.metadata.local_addr = Some(addr);
        self
    }

    /// Build the Response
    pub fn build(self) -> Response {
        Response::with_metadata(
            self.status,
            self.headers,
            self.body,
            self.version,
            self.url,
            self.metadata,
        )
    }

    // Convenience methods for common response types

    /// Create a 200 OK response
    pub fn ok() -> Self {
        Self::new().status(StatusCode::OK)
    }

    /// Create a 201 Created response
    pub fn created() -> Self {
        Self::new().status(StatusCode::CREATED)
    }

    /// Create a 204 No Content response
    pub fn no_content() -> Self {
        Self::new().status(StatusCode::NO_CONTENT)
    }

    /// Create a 400 Bad Request response
    pub fn bad_request() -> Self {
        Self::new().status(StatusCode::BAD_REQUEST)
    }

    /// Create a 401 Unauthorized response
    pub fn unauthorized() -> Self {
        Self::new().status(StatusCode::UNAUTHORIZED)
    }

    /// Create a 403 Forbidden response
    pub fn forbidden() -> Self {
        Self::new().status(StatusCode::FORBIDDEN)
    }

    /// Create a 404 Not Found response
    pub fn not_found() -> Self {
        Self::new().status(StatusCode::NOT_FOUND)
    }

    /// Create a 500 Internal Server Error response
    pub fn internal_server_error() -> Self {
        Self::new().status(StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// Create a JSON response with 200 OK status
    pub fn json_ok<T: Serialize>(value: &T) -> Result<Response> {
        Ok(Self::ok()
            .json(value)?
            .content_type("application/json")?
            .build())
    }

    /// Create a text response with 200 OK status
    pub fn text_ok(text: impl Into<String>) -> Result<Response> {
        Ok(Self::ok().text(text).content_type("text/plain")?.build())
    }

    /// Create an HTML response with 200 OK status
    pub fn html_ok(html: impl Into<String>) -> Result<Response> {
        Ok(Self::ok().text(html).content_type("text/html")?.build())
    }
}

impl Default for ResponseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_response_builder_basic() {
        let response = ResponseBuilder::new().status(StatusCode::OK).build();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.is_success());
    }

    #[test]
    fn test_response_builder_with_headers() -> Result<()> {
        let response = ResponseBuilder::new()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")?
            .header("X-Custom-Header", "custom-value")?
            .build();

        assert_eq!(
            response.headers().get("Content-Type"),
            Some("application/json")
        );
        assert_eq!(
            response.headers().get("X-Custom-Header"),
            Some("custom-value")
        );
        Ok(())
    }

    #[test]
    fn test_response_builder_json() -> Result<()> {
        let data = json!({"message": "Hello World"});
        let response = ResponseBuilder::new().json(&data)?.build();

        assert!(response.body().is_some());
        Ok(())
    }

    #[test]
    fn test_response_builder_text() -> Result<()> {
        let response = ResponseBuilder::new()
            .text("Hello World")
            .content_type("text/plain")?
            .build();

        assert!(response.body().is_some());
        assert_eq!(response.content_type(), Some("text/plain"));
        Ok(())
    }

    #[test]
    fn test_response_builder_convenience_methods() -> Result<()> {
        let ok_response = ResponseBuilder::ok().build();
        assert_eq!(ok_response.status(), StatusCode::OK);

        let not_found_response = ResponseBuilder::not_found().build();
        assert_eq!(not_found_response.status(), StatusCode::NOT_FOUND);
        assert!(not_found_response.is_client_error());

        let server_error_response = ResponseBuilder::internal_server_error().build();
        assert_eq!(
            server_error_response.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(server_error_response.is_server_error());

        Ok(())
    }

    #[test]
    fn test_response_builder_metadata() {
        let start_time = SystemTime::now();
        let duration = Duration::from_millis(100);

        let response = ResponseBuilder::new()
            .request_start(start_time)
            .duration(duration)
            .from_cache(true)
            .redirect_count(2)
            .remote_addr("192.168.1.1:80".to_string())
            .build();

        let metadata = response.metadata();
        assert_eq!(metadata.request_start, Some(start_time));
        assert_eq!(metadata.duration, Some(duration));
        assert!(metadata.from_cache);
        assert_eq!(metadata.redirect_count, 2);
        assert_eq!(metadata.remote_addr, Some("192.168.1.1:80".to_string()));
    }

    #[test]
    fn test_convenience_response_creators() -> Result<()> {
        let json_response = ResponseBuilder::json_ok(&json!({"test": "data"}))?;
        assert!(json_response.is_success());
        assert_eq!(json_response.content_type(), Some("application/json"));

        let text_response = ResponseBuilder::text_ok("Hello World")?;
        assert!(text_response.is_success());
        assert_eq!(text_response.content_type(), Some("text/plain"));

        let html_response = ResponseBuilder::html_ok("<h1>Hello</h1>")?;
        assert!(html_response.is_success());
        assert_eq!(html_response.content_type(), Some("text/html"));

        Ok(())
    }
}
