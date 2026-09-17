use crate::error::{HttpClientError, Result};
use crate::header::{common, HeaderMap};
use std::collections::HashMap;

/// Builder for constructing HeaderMap instances with a fluent API
#[derive(Debug, Clone)]
pub struct HeaderBuilder {
    headers: HeaderMap,
}

impl HeaderBuilder {
    /// Create a new header builder
    pub fn new() -> Self {
        Self {
            headers: HeaderMap::new(),
        }
    }

    /// Create a header builder with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            headers: HeaderMap::with_capacity(capacity),
        }
    }

    /// Set a header, replacing any existing values
    pub fn header(mut self, name: &str, value: &str) -> Result<Self> {
        self.headers.insert(name, value)?;
        Ok(self)
    }

    /// Append a header value to existing values
    pub fn append_header(mut self, name: &str, value: &str) -> Result<Self> {
        self.headers.append(name, value)?;
        Ok(self)
    }

    /// Set multiple headers from a HashMap
    pub fn headers(mut self, headers: HashMap<String, String>) -> Result<Self> {
        let headers = HeaderMap::try_from(headers)?;
        self.headers.overlay(headers);
        Ok(self)
    }

    /// Set Content-Type header
    pub fn content_type(self, content_type: &str) -> Result<Self> {
        self.header(common::CONTENT_TYPE, content_type)
    }

    /// Set Content-Type header with charset
    pub fn content_type_with_charset(self, content_type: &str, charset: &str) -> Result<Self> {
        let value = common::HeaderValue::content_type_with_charset(content_type, charset);
        self.header(common::CONTENT_TYPE, &value)
    }

    /// Set JSON content type
    pub fn json(self) -> Result<Self> {
        self.content_type(common::content_type::APPLICATION_JSON)
    }

    /// Set form content type
    pub fn form(self) -> Result<Self> {
        self.content_type(common::content_type::APPLICATION_FORM_URLENCODED)
    }

    /// Set text content type
    pub fn text(self) -> Result<Self> {
        self.content_type(common::content_type::TEXT_PLAIN)
    }

    /// Set HTML content type
    pub fn html(self) -> Result<Self> {
        self.content_type(common::content_type::TEXT_HTML)
    }

    /// Set Authorization header with Bearer token
    pub fn bearer_auth(self, token: &str) -> Result<Self> {
        let value = common::HeaderValue::bearer_auth(token);
        self.header(common::AUTHORIZATION, &value)
    }

    /// Set Authorization header with Basic auth
    pub fn basic_auth(self, username: &str, password: &str) -> Result<Self> {
        let value = common::HeaderValue::basic_auth(username, password);
        self.header(common::AUTHORIZATION, &value)
    }

    /// Set User-Agent header
    pub fn user_agent(self, user_agent: &str) -> Result<Self> {
        self.header(common::USER_AGENT, user_agent)
    }

    /// Set Accept header
    pub fn accept(self, accept: &str) -> Result<Self> {
        self.header(common::ACCEPT, accept)
    }

    /// Set Accept-Encoding header
    pub fn accept_encoding(self, encoding: &str) -> Result<Self> {
        self.header(common::ACCEPT_ENCODING, encoding)
    }

    /// Set Cache-Control header
    pub fn cache_control(self, cache_control: &str) -> Result<Self> {
        self.header(common::CACHE_CONTROL, cache_control)
    }

    /// Set Cache-Control with max-age
    pub fn max_age(self, seconds: u32) -> Result<Self> {
        let value = common::HeaderValue::max_age(seconds);
        self.header(common::CACHE_CONTROL, &value)
    }

    /// Set no-cache directive
    pub fn no_cache(self) -> Result<Self> {
        self.header(common::CACHE_CONTROL, common::cache_control::NO_CACHE)
    }

    /// Set Host header
    pub fn host(self, host: &str) -> Result<Self> {
        self.header(common::HOST, host)
    }

    /// Set Connection header to keep-alive
    pub fn keep_alive(self) -> Result<Self> {
        self.header(common::CONNECTION, "keep-alive")
    }

    /// Set Connection header to close
    pub fn close(self) -> Result<Self> {
        self.header(common::CONNECTION, "close")
    }

    /// Set Content-Length header
    pub fn content_length(self, length: usize) -> Result<Self> {
        self.header(common::CONTENT_LENGTH, &length.to_string())
    }

    /// Set custom header (convenience method)
    pub fn custom(self, name: &str, value: &str) -> Result<Self> {
        self.header(name, value)
    }

    /// Build the final HeaderMap
    pub fn build(self) -> HeaderMap {
        self.headers
    }
    #[allow(clippy::should_implement_trait)]
    /// Get a reference to the current headers (for inspection)
    pub fn as_ref(&self) -> &HeaderMap {
        &self.headers
    }

    /// Check if a header exists
    pub fn has_header(&self, name: &str) -> bool {
        self.headers.contains_key(name)
    }

    /// Remove a header
    pub fn remove_header(mut self, name: &str) -> Self {
        self.headers.remove(name);
        self
    }

    /// Clear all headers
    pub fn clear(mut self) -> Self {
        self.headers.clear();
        self
    }
}

impl Default for HeaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<HeaderMap> for HeaderBuilder {
    fn from(headers: HeaderMap) -> Self {
        Self { headers }
    }
}

impl TryFrom<HashMap<String, String>> for HeaderBuilder {
    type Error = HttpClientError;

    fn try_from(map: HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            headers: HeaderMap::try_from(map)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_builder_basic() {
        let headers = HeaderBuilder::new()
            .content_type("application/json")
            .unwrap()
            .user_agent("test-client/1.0")
            .unwrap()
            .build();

        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.get("user-agent"), Some("test-client/1.0"));
    }

    #[test]
    fn test_header_builder_auth() {
        let headers = HeaderBuilder::new()
            .bearer_auth("token123")
            .unwrap()
            .build();

        assert_eq!(headers.get("authorization"), Some("Bearer token123"));

        let headers = HeaderBuilder::new()
            .basic_auth("user", "pass")
            .unwrap()
            .build();

        let auth = headers.get("authorization").unwrap();
        assert!(auth.starts_with("Basic "));
    }

    #[test]
    fn test_header_builder_content_types() {
        let headers = HeaderBuilder::new().json().unwrap().build();
        assert_eq!(headers.get("content-type"), Some("application/json"));

        let headers = HeaderBuilder::new().form().unwrap().build();
        assert_eq!(
            headers.get("content-type"),
            Some("application/x-www-form-urlencoded")
        );

        let headers = HeaderBuilder::new().text().unwrap().build();
        assert_eq!(headers.get("content-type"), Some("text/plain"));

        let headers = HeaderBuilder::new().html().unwrap().build();
        assert_eq!(headers.get("content-type"), Some("text/html"));
    }

    #[test]
    fn test_header_builder_cache_control() {
        let headers = HeaderBuilder::new().max_age(3600).unwrap().build();
        assert_eq!(headers.get("cache-control"), Some("max-age=3600"));

        let headers = HeaderBuilder::new().no_cache().unwrap().build();
        assert_eq!(headers.get("cache-control"), Some("no-cache"));
    }

    #[test]
    fn test_header_builder_connection() {
        let headers = HeaderBuilder::new().keep_alive().unwrap().build();
        assert_eq!(headers.get("connection"), Some("keep-alive"));

        let headers = HeaderBuilder::new().close().unwrap().build();
        assert_eq!(headers.get("connection"), Some("close"));
    }

    #[test]
    fn test_header_builder_append() {
        let headers = HeaderBuilder::new()
            .accept("text/html")
            .unwrap()
            .append_header("accept", "application/json")
            .unwrap()
            .build();

        let accept_values = headers.get_all("accept").unwrap();
        assert_eq!(accept_values.len(), 2);
        assert!(accept_values.contains(&"text/html".to_string()));
        assert!(accept_values.contains(&"application/json".to_string()));
    }

    #[test]
    fn test_header_builder_from_hashmap() {
        let mut map = HashMap::new();
        map.insert("Content-Type".to_string(), "application/json".to_string());
        map.insert("Accept".to_string(), "text/html".to_string());

        let headers = HeaderBuilder::try_from(map).unwrap().build();
        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.get("accept"), Some("text/html"));
    }

    #[test]
    fn hashmap_conversion_rejects_invalid_headers() {
        let mut map = HashMap::new();
        map.insert("Accept".to_string(), "application/json".to_string());
        map.insert("invalid name".to_string(), "hidden".to_string());

        assert!(matches!(
            HeaderBuilder::try_from(map),
            Err(HttpClientError::InvalidHeader(_))
        ));
    }

    #[test]
    fn bulk_hashmap_rejects_normalized_name_collisions() {
        let mut map = HashMap::new();
        map.insert("X-Trace".to_string(), "first".to_string());
        map.insert("x-trace".to_string(), "second".to_string());

        let error = HeaderBuilder::new().headers(map).unwrap_err();
        assert!(matches!(error, HttpClientError::InvalidHeader(_)));
        assert_eq!(error.diagnostic_code(), "HEADER_NAME_DUPLICATE");
    }

    #[test]
    fn test_header_builder_inspection() {
        let builder = HeaderBuilder::new()
            .json()
            .unwrap()
            .user_agent("test")
            .unwrap();

        assert!(builder.has_header("content-type"));
        assert!(builder.has_header("user-agent"));
        assert!(!builder.has_header("authorization"));

        let headers_ref = builder.as_ref();
        assert_eq!(headers_ref.get("content-type"), Some("application/json"));
    }

    #[test]
    fn test_header_builder_remove_and_clear() {
        let builder = HeaderBuilder::new()
            .json()
            .unwrap()
            .user_agent("test")
            .unwrap()
            .remove_header("user-agent");

        let headers = builder.build();
        assert!(headers.contains_key("content-type"));
        assert!(!headers.contains_key("user-agent"));

        let builder = HeaderBuilder::new()
            .json()
            .unwrap()
            .user_agent("test")
            .unwrap()
            .clear();

        let headers = builder.build();
        assert!(headers.is_empty());
    }
}
