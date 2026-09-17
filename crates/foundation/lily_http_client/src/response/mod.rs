//! HTTP Response module
//!
//! This module contains types and functionality for handling HTTP responses.
//! Provides comprehensive response handling with status codes, headers, bodies,
//! and metadata for HTTP client operations.
//!
//! ## Example
//!
//! ```rust,no_run
//! use lily_http_client::response::{Response, StatusCode};
//! use lily_http_client::header::HeaderMap;
//! use lily_http_client::body::TextBody;
//!
//! // Create a response
//! let mut headers = HeaderMap::new();
//! headers.insert("Content-Type", "application/json").unwrap();
//!
//! let body = TextBody::new("{\"message\": \"Hello World\"}");
//! let response = Response::new(StatusCode::OK, headers, Some(Box::new(body)));
//!
//! // Check response properties
//! assert!(response.status().is_success());
//! assert_eq!(response.headers().get("Content-Type"), Some("application/json"));
//! ```

use std::{
    fmt,
    time::{Duration, SystemTime},
};
use url::Url;

use crate::body::Body;
use crate::error::{HttpClientError, Result};
use crate::header::HeaderMap;

pub mod builder;
pub mod status;

pub use builder::ResponseBuilder;
pub use status::StatusCode;

/// HTTP Response structure representing a complete HTTP response
///
/// Contains all the essential components of an HTTP response including
/// status code, headers, body, and metadata about the response.
pub struct Response {
    /// HTTP status code
    status: StatusCode,
    /// Response headers
    headers: HeaderMap,
    /// Response body (optional)
    body: Option<Box<dyn Body>>,
    /// HTTP version used for the response
    version: HttpVersion,
    /// Final URL after redirects
    url: Option<Url>,
    /// Response metadata
    metadata: ResponseMetadata,
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("body_present", &self.body.is_some())
            .field("version", &self.version)
            .field("url", &self.url.as_ref().map(|_| "[REDACTED]"))
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// HTTP version enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpVersion {
    /// HTTP/1.0
    Http10,
    /// HTTP/1.1
    Http11,
    /// HTTP/2.0
    Http2,
    /// HTTP/3.0
    Http3,
}

/// Response metadata containing timing and connection information
#[derive(Clone, Default)]
pub struct ResponseMetadata {
    /// Time when the request was initiated
    pub request_start: Option<SystemTime>,
    /// Time when the response was received
    pub response_time: Option<SystemTime>,
    /// Total duration of the request
    pub duration: Option<Duration>,
    /// Size of the response body in bytes
    pub content_length: Option<u64>,
    /// Whether the response was served from cache
    pub from_cache: bool,
    /// Number of redirects followed
    pub redirect_count: u32,
    /// Remote address of the server
    pub remote_addr: Option<String>,
    /// Local address used for the connection
    pub local_addr: Option<String>,
}

impl fmt::Debug for ResponseMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseMetadata")
            .field("request_start_present", &self.request_start.is_some())
            .field("response_time_present", &self.response_time.is_some())
            .field("duration", &self.duration)
            .field("content_length", &self.content_length)
            .field("from_cache", &self.from_cache)
            .field("redirect_count", &self.redirect_count)
            .field("remote_addr_present", &self.remote_addr.is_some())
            .field("local_addr_present", &self.local_addr.is_some())
            .finish()
    }
}

impl Response {
    /// Create a new Response
    pub fn new(status: StatusCode, headers: HeaderMap, body: Option<Box<dyn Body>>) -> Self {
        Self {
            status,
            headers,
            body,
            version: HttpVersion::Http11, // Default to HTTP/1.1
            url: None,
            metadata: ResponseMetadata::default(),
        }
    }

    /// Create a new Response with all parameters
    pub fn with_metadata(
        status: StatusCode,
        headers: HeaderMap,
        body: Option<Box<dyn Body>>,
        version: HttpVersion,
        url: Option<Url>,
        metadata: ResponseMetadata,
    ) -> Self {
        Self {
            status,
            headers,
            body,
            version,
            url,
            metadata,
        }
    }

    /// Get the status code
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Set the status code
    pub fn set_status(&mut self, status: StatusCode) {
        self.status = status;
    }

    /// Get the headers
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Get mutable reference to headers
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Get the body
    pub fn body(&self) -> Option<&dyn Body> {
        self.body.as_ref().map(|b| b.as_ref())
    }

    /// Take the body, consuming it
    pub fn take_body(&mut self) -> Option<Box<dyn Body>> {
        self.body.take()
    }

    /// Set the body
    pub fn set_body(&mut self, body: Option<Box<dyn Body>>) {
        self.body = body;
    }

    /// Get the HTTP version
    pub fn version(&self) -> HttpVersion {
        self.version
    }

    /// Set the HTTP version
    pub fn set_version(&mut self, version: HttpVersion) {
        self.version = version;
    }

    /// Get the final URL (after redirects)
    pub fn url(&self) -> Option<&Url> {
        self.url.as_ref()
    }

    /// Set the final URL
    pub fn set_url(&mut self, url: Option<Url>) {
        self.url = url;
    }

    /// Get the response metadata
    pub fn metadata(&self) -> &ResponseMetadata {
        &self.metadata
    }

    /// Get mutable reference to metadata
    pub fn metadata_mut(&mut self) -> &mut ResponseMetadata {
        &mut self.metadata
    }

    /// Check if the response indicates success (2xx status)
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Check if the response indicates an error (4xx or 5xx status)
    pub fn is_error(&self) -> bool {
        self.status.is_error()
    }

    /// Check if the response indicates a client error (4xx status)
    pub fn is_client_error(&self) -> bool {
        self.status.is_client_error()
    }

    /// Check if the response indicates a server error (5xx status)
    pub fn is_server_error(&self) -> bool {
        self.status.is_server_error()
    }

    /// Check if the response indicates a redirection (3xx status)
    pub fn is_redirection(&self) -> bool {
        self.status.is_redirection()
    }

    /// Get the content type from headers
    pub fn content_type(&self) -> Option<&str> {
        self.headers.get("Content-Type")
    }

    /// Get the content length from headers or body
    pub fn content_length(&self) -> Option<u64> {
        // First try to get from headers
        if let Some(length_str) = self.headers.get("Content-Length") {
            if let Ok(length) = length_str.parse::<u64>() {
                return Some(length);
            }
        }

        // Then try to get from body
        if let Some(ref body) = self.body {
            return body.content_length().map(|len| len as u64);
        }

        None
    }

    /// Get response body as bytes (consumes the body)
    pub async fn bytes(&mut self) -> Result<Vec<u8>> {
        match self.body.as_mut() {
            Some(body) => {
                let bytes = body.to_bytes().await?;
                Ok(bytes.to_vec())
            }
            None => Ok(Vec::new()),
        }
    }

    /// Get response body as text (consumes the body)
    pub async fn text(&mut self) -> Result<String> {
        let bytes = self.bytes().await?;
        String::from_utf8(bytes)
            .map_err(|e| HttpClientError::body(format!("Invalid UTF-8 in response body: {e}")))
    }

    /// Get response body as JSON (consumes the body)
    pub async fn json<T>(&mut self) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let text = self.text().await?;
        serde_json::from_str(&text)
            .map_err(|e| HttpClientError::body(format!("JSON deserialization error: {e}")))
    }

    /// Create a builder for constructing responses
    pub fn builder() -> ResponseBuilder {
        ResponseBuilder::new()
    }
}

impl HttpVersion {
    /// Get the version as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            HttpVersion::Http10 => "HTTP/1.0",
            HttpVersion::Http11 => "HTTP/1.1",
            HttpVersion::Http2 => "HTTP/2.0",
            HttpVersion::Http3 => "HTTP/3.0",
        }
    }
}

impl std::fmt::Display for HttpVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ResponseMetadata {
    /// Create new empty metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Set request start time
    pub fn with_request_start(mut self, start: SystemTime) -> Self {
        self.request_start = Some(start);
        self
    }

    /// Set response time
    pub fn with_response_time(mut self, time: SystemTime) -> Self {
        self.response_time = Some(time);
        self
    }

    /// Set duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = Some(duration);
        self
    }

    /// Set content length
    pub fn with_content_length(mut self, length: u64) -> Self {
        self.content_length = Some(length);
        self
    }

    /// Set from cache flag
    pub fn with_from_cache(mut self, from_cache: bool) -> Self {
        self.from_cache = from_cache;
        self
    }

    /// Set redirect count
    pub fn with_redirect_count(mut self, count: u32) -> Self {
        self.redirect_count = count;
        self
    }

    /// Set remote address
    pub fn with_remote_addr(mut self, addr: String) -> Self {
        self.remote_addr = Some(addr);
        self
    }

    /// Set local address
    pub fn with_local_addr(mut self, addr: String) -> Self {
        self.local_addr = Some(addr);
        self
    }
}
