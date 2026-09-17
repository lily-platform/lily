//! HTTP Request module
//!
//! This module contains types and functionality for building and handling HTTP requests.

use std::fmt;
use url::Url;

use crate::body::Body;
use crate::error::{HttpClientError, Result};
use crate::header::HeaderMap;

pub mod builder;
pub mod method;
pub mod request_config;

pub use builder::RequestBuilder;
pub use method::Method;
pub use request_config::RequestConfig;

const FORBIDDEN_REQUEST_HEADERS: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub(crate) fn is_hop_by_hop_or_proxy_only_request_header(name: &str) -> bool {
    FORBIDDEN_REQUEST_HEADERS
        .iter()
        .any(|forbidden| name.eq_ignore_ascii_case(forbidden))
}

pub(crate) fn validate_request_header(name: &str) -> Result<()> {
    if is_hop_by_hop_or_proxy_only_request_header(name) {
        return Err(HttpClientError::hop_by_hop_request_header());
    }
    if name.eq_ignore_ascii_case("content-length") {
        return Err(HttpClientError::transport_owned_request_header());
    }
    Ok(())
}

pub(crate) fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

pub(crate) fn parse_request_url(value: &str) -> Result<Url> {
    let url = Url::parse(value)
        .map_err(|_| HttpClientError::InvalidUrl("request URL is invalid".to_string()))?;
    validate_request_url(&url)?;
    Ok(url)
}

pub(crate) fn validate_request_url(url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(HttpClientError::InvalidUrl(
            "request URL scheme must be http or https".to_string(),
        ));
    }
    if url.host_str().is_none() {
        return Err(HttpClientError::InvalidUrl(
            "request URL must contain a host".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(HttpClientError::InvalidUrl(
            "request URL must not contain credentials".to_string(),
        ));
    }
    Ok(())
}

/// HTTP Request structure
///
/// Represents a complete HTTP request with method, URL, headers, body, and configuration.
pub struct Request {
    /// HTTP method
    method: Method,
    /// Request URL
    url: Url,
    /// HTTP headers
    headers: HeaderMap,
    /// Request body
    body: Option<Box<dyn Body>>,
    /// Request configuration (timeouts, redirects, etc.)
    config: RequestConfig,
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("method", &self.method)
            .field("url", &"[REDACTED]")
            .field("headers", &self.headers)
            .field("body_present", &self.body.is_some())
            .field("config", &self.config)
            .finish()
    }
}

impl Request {
    /// Create a new request builder
    pub fn builder() -> RequestBuilder {
        RequestBuilder::new()
    }

    /// Create a GET request
    pub fn get(url: impl AsRef<str>) -> Result<RequestBuilder> {
        let mut builder = RequestBuilder::new();
        builder.method(Method::Get).url(url)?;
        Ok(builder)
    }

    /// Create a POST request
    pub fn post(url: impl AsRef<str>) -> Result<RequestBuilder> {
        let mut builder = RequestBuilder::new();
        builder.method(Method::Post).url(url)?;
        Ok(builder)
    }

    /// Create a PUT request
    pub fn put(url: impl AsRef<str>) -> Result<RequestBuilder> {
        let mut builder = RequestBuilder::new();
        builder.method(Method::Put).url(url)?;
        Ok(builder)
    }

    /// Create a DELETE request
    pub fn delete(url: impl AsRef<str>) -> Result<RequestBuilder> {
        let mut builder = RequestBuilder::new();
        builder.method(Method::Delete).url(url)?;
        Ok(builder)
    }

    /// Create a PATCH request
    pub fn patch(url: impl AsRef<str>) -> Result<RequestBuilder> {
        let mut builder = RequestBuilder::new();
        builder.method(Method::Patch).url(url)?;
        Ok(builder)
    }

    /// Create a HEAD request
    pub fn head(url: impl AsRef<str>) -> Result<RequestBuilder> {
        let mut builder = RequestBuilder::new();
        builder.method(Method::Head).url(url)?;
        Ok(builder)
    }

    /// Get the HTTP method
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Get the request URL
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Get the headers
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Get mutable headers for crate-owned instrumentation.
    pub(crate) fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Get the request body
    pub fn body(&self) -> Option<&dyn Body> {
        self.body.as_ref().map(|b| b.as_ref())
    }

    /// Take the request body (consuming it)
    pub fn take_body(&mut self) -> Option<Box<dyn Body>> {
        self.body.take()
    }

    /// Get the request configuration
    pub fn config(&self) -> &RequestConfig {
        &self.config
    }

    /// Set a header
    pub fn set_header(&mut self, name: &str, value: &str) -> Result<()> {
        validate_request_header(name)?;
        self.headers.insert(name, value)
    }

    /// Add a header (allows multiple values)
    pub fn add_header(&mut self, name: &str, value: &str) -> Result<()> {
        validate_request_header(name)?;
        self.headers.append(name, value)
    }

    /// Remove a header
    pub fn remove_header(&mut self, name: &str) -> Option<Vec<String>> {
        self.headers.remove(name)
    }

    /// Get a header value
    pub fn get_header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)
    }

    /// Check if request has a body
    pub fn has_body(&self) -> bool {
        self.body.is_some()
    }

    /// Get the content length if available
    pub fn content_length(&self) -> Option<usize> {
        self.body.as_ref().and_then(|b| b.content_length())
    }

    /// Get the content type if available
    pub fn content_type(&self) -> Option<&str> {
        self.body.as_ref().and_then(|b| b.content_type())
    }

    /// Clone the request (note: body is not cloned if not cloneable)
    pub fn try_clone(&self) -> Option<Request> {
        let cloned_body = self.body.as_ref().and_then(|b| b.try_clone());

        // Only clone if body is cloneable or there's no body
        if self.body.is_none() || cloned_body.is_some() {
            Some(Request {
                method: self.method.clone(),
                url: self.url.clone(),
                headers: self.headers.clone(),
                body: cloned_body,
                config: self.config.clone(),
            })
        } else {
            None
        }
    }
}

/// Internal constructor for Request (used by RequestBuilder)
impl Request {
    pub(crate) fn new(
        method: Method,
        url: Url,
        headers: HeaderMap,
        body: Option<Box<dyn Body>>,
        config: RequestConfig,
    ) -> Self {
        Self {
            method,
            url,
            headers,
            body,
            config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FORBIDDEN_CASE_VARIANTS: [&str; 9] = [
        "Connection",
        "kEeP-aLiVe",
        "Proxy-Connection",
        "Proxy-Authenticate",
        "Proxy-Authorization",
        "TE",
        "Trailer",
        "Transfer-Encoding",
        "Upgrade",
    ];

    fn request() -> Request {
        Request::get("https://example.test")
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn public_request_mutators_reject_forbidden_headers_case_insensitively() {
        for name in FORBIDDEN_CASE_VARIANTS {
            let mut set_request = request();
            let set_error = set_request
                .set_header(name, "LILY_SECRET_SET_HEADER")
                .unwrap_err();
            assert_eq!(set_error.diagnostic_code(), "REQUEST_HOP_BY_HOP_HEADER");
            assert!(!set_request.headers().contains_key(name));

            let mut add_request = request();
            let add_error = add_request
                .add_header(name, "LILY_SECRET_ADD_HEADER")
                .unwrap_err();
            assert_eq!(add_error.diagnostic_code(), "REQUEST_HOP_BY_HOP_HEADER");
            assert!(!add_request.headers().contains_key(name));
        }
    }

    #[test]
    fn content_length_is_rejected_by_both_public_mutators() {
        let mut set_request = request();
        let set_error = set_request.set_header("Content-Length", "3").unwrap_err();
        assert_eq!(
            set_error.diagnostic_code(),
            "REQUEST_TRANSPORT_OWNED_HEADER"
        );
        assert!(!set_request.headers().contains_key("Content-Length"));

        let mut add_request = request();
        let add_error = add_request.add_header("cOnTeNt-LeNgTh", "4").unwrap_err();
        assert_eq!(
            add_error.diagnostic_code(),
            "REQUEST_TRANSPORT_OWNED_HEADER"
        );
        assert!(!add_request.headers().contains_key("Content-Length"));
    }

    #[test]
    fn request_urls_reject_username_and_password_credentials_independently() {
        for value in [
            "https://user@example.test/resource",
            "https://:password@example.test/resource",
        ] {
            let error = parse_request_url(value).unwrap_err();
            assert_eq!(error.diagnostic_code(), "REQUEST_URL_CREDENTIALS");
        }
    }
}
