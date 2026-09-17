use serde::Serialize;
use std::{fmt, time::Duration};
use url::Url;

use super::{
    parse_request_url, same_origin, validate_request_header, validate_request_url, Method, Request,
    RequestConfig,
};
use crate::body::{BinaryBody, Body, FormBody, JsonBody, TextBody};
use crate::client::ProtocolPreference;
use crate::error::{HttpClientError, Result};
use crate::header::HeaderMap;

/// Builder for constructing HTTP requests
///
/// Provides a fluent interface for building requests with method chaining.
/// Supports setting method, URL, headers, body, and configuration options.
pub struct RequestBuilder {
    method: Option<Method>,
    url: Option<Url>,
    headers: HeaderMap,
    client_default_headers: Option<ClientDefaultHeaders>,
    body: Option<Box<dyn Body>>,
    config: RequestConfig,
}

struct ClientDefaultHeaders {
    headers: HeaderMap,
    base_origin: Option<Url>,
}

impl ClientDefaultHeaders {
    fn applies_to(&self, target: &Url) -> bool {
        self.base_origin
            .as_ref()
            .is_none_or(|base| same_origin(base, target))
    }
}

impl fmt::Debug for RequestBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestBuilder")
            .field("method", &self.method)
            .field("url", &self.url.as_ref().map(|_| "[REDACTED]"))
            .field("headers", &self.headers)
            .field(
                "client_default_headers",
                &self
                    .client_default_headers
                    .as_ref()
                    .map(|defaults| &defaults.headers),
            )
            .field(
                "client_defaults_are_origin_scoped",
                &self
                    .client_default_headers
                    .as_ref()
                    .map(|defaults| defaults.base_origin.is_some()),
            )
            .field("body_present", &self.body.is_some())
            .field("config", &self.config)
            .finish()
    }
}

impl Default for RequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestBuilder {
    /// Create a new request builder
    pub fn new() -> Self {
        Self {
            method: None,
            url: None,
            headers: HeaderMap::new(),
            client_default_headers: None,
            body: None,
            config: RequestConfig::default(),
        }
    }

    pub(crate) fn with_client_default_headers(
        &mut self,
        headers: HeaderMap,
        base_origin: Option<Url>,
    ) -> &mut Self {
        self.client_default_headers = Some(ClientDefaultHeaders {
            headers,
            base_origin,
        });
        self
    }

    /// Set the HTTP method
    pub fn method(&mut self, method: Method) -> &mut Self {
        self.method = Some(method);
        self
    }

    /// Set the request URL
    pub fn url(&mut self, url: impl AsRef<str>) -> Result<&mut Self> {
        self.url = Some(parse_request_url(url.as_ref())?);
        Ok(self)
    }

    /// Set the request URL from a parsed Url
    pub fn url_parsed(&mut self, url: Url) -> Result<&mut Self> {
        validate_request_url(&url)?;
        self.url = Some(url);
        Ok(self)
    }

    /// Add a header
    pub fn header(&mut self, name: &str, value: &str) -> Result<&mut Self> {
        validate_request_header(name)?;
        self.headers.insert(name, value)?;
        Ok(self)
    }

    /// Add multiple headers from an iterator
    pub fn headers<I, K, V>(&mut self, headers: I) -> Result<&mut Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        for (name, value) in headers {
            self.header(name.as_ref(), value.as_ref())?;
        }
        Ok(self)
    }

    /// Set the User-Agent header
    pub fn user_agent(&mut self, user_agent: &str) -> Result<&mut Self> {
        self.header("User-Agent", user_agent)
    }

    /// Set the Authorization header with Bearer token
    pub fn bearer_auth(&mut self, token: &str) -> Result<&mut Self> {
        let auth_value = format!("Bearer {token}");
        self.header("Authorization", &auth_value)
    }

    /// Set the Authorization header with Basic auth
    pub fn basic_auth(&mut self, username: &str, password: Option<&str>) -> Result<&mut Self> {
        let credentials = match password {
            Some(pass) => format!("{username}:{pass}"),
            None => username.to_string(),
        };
        // Use base64 0.21 API
        use base64::{engine::general_purpose, Engine as _};
        let encoded = general_purpose::STANDARD.encode(credentials);
        let auth_value = format!("Basic {encoded}");
        self.header("Authorization", &auth_value)
    }

    /// Set the Content-Type header
    pub fn content_type(&mut self, content_type: &str) -> Result<&mut Self> {
        self.header("Content-Type", content_type)
    }

    /// Set the Accept header
    pub fn accept(&mut self, accept: &str) -> Result<&mut Self> {
        self.header("Accept", accept)
    }

    /// Set a JSON body from a serializable value
    pub fn json<T: Serialize>(&mut self, value: &T) -> Result<&mut Self> {
        let json_body = JsonBody::new(value)?;
        self.body = Some(Box::new(json_body));
        Ok(self)
    }

    /// Set a JSON body from a string
    pub fn json_str(&mut self, json: &str) -> &mut Self {
        let json_body = JsonBody::from_string(json);
        self.body = Some(Box::new(json_body));
        self
    }

    /// Set a text body
    pub fn text(&mut self, text: impl Into<String>) -> &mut Self {
        let text_body = TextBody::new(text);
        self.body = Some(Box::new(text_body));
        self
    }

    /// Set a binary body
    pub fn binary(&mut self, data: impl Into<bytes::Bytes>) -> &mut Self {
        let binary_body = BinaryBody::new(data);
        self.body = Some(Box::new(binary_body));
        self
    }

    /// Set a form body from key-value pairs
    pub fn form<I, K, V>(&mut self, form_data: I) -> Result<&mut Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut form_body = FormBody::new();
        for (key, value) in form_data {
            form_body.add_field(key.into(), value.into());
        }
        self.body = Some(Box::new(form_body));
        Ok(self)
    }

    /// Set a custom body
    pub fn body(&mut self, body: Box<dyn Body>) -> &mut Self {
        self.body = Some(body);
        self
    }

    /// Set request timeout
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.config.timeout = Some(timeout);
        self
    }

    /// Set connection timeout
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.config.connect_timeout = Some(timeout);
        self
    }

    /// Enable or disable redirect following
    pub fn follow_redirects(&mut self, follow: bool) -> &mut Self {
        self.config.follow_redirects = follow;
        self
    }

    /// Set maximum number of redirects
    pub fn max_redirects(&mut self, max: u32) -> &mut Self {
        self.config.max_redirects = Some(max);
        self
    }

    /// Select an exact protocol policy for this request.
    pub fn protocol(&mut self, protocol: ProtocolPreference) -> &mut Self {
        self.config.protocol = Some(protocol);
        self
    }

    /// Apply a typed request configuration. It is validated by [`Self::build`].
    pub fn config(&mut self, config: RequestConfig) -> &mut Self {
        self.config = config;
        self
    }

    /// Add query parameters to the URL
    pub fn query<I, K, V>(&mut self, params: I) -> Result<&mut Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        if let Some(ref mut url) = self.url {
            let mut query_pairs = url.query_pairs_mut();
            for (key, value) in params {
                query_pairs.append_pair(key.as_ref(), value.as_ref());
            }
            query_pairs.finish();
        } else {
            return Err(HttpClientError::request_building(
                "Cannot add query parameters without setting URL first",
            ));
        }
        Ok(self)
    }

    /// Build the request
    pub fn build(self) -> Result<Request> {
        self.config.validate()?;

        let method = self
            .method
            .ok_or_else(|| HttpClientError::request_building("HTTP method is required"))?;
        http::Method::from_bytes(method.as_str().as_bytes())
            .map_err(|_| HttpClientError::invalid_method("request method token is invalid"))?;

        let url = self
            .url
            .ok_or_else(|| HttpClientError::request_building("URL is required"))?;
        validate_request_url(&url)?;

        if let Some(body) = self.body.as_ref() {
            if !body.is_repeatable() {
                return Err(HttpClientError::UnsupportedConfiguration(
                    "streaming/non-repeatable request bodies are not supported by the bounded-buffered v1 transport"
                        .to_string(),
                ));
            }
            if body.content_length().is_none() {
                return Err(HttpClientError::UnsupportedConfiguration(
                    "request body must declare its buffered length".to_string(),
                ));
            }
        }

        // Client defaults are scoped to the final URL, not the URL initially
        // used to create this mutable builder. Explicit request headers are
        // authoritative and replace defaults with the same normalized name.
        let mut headers = match self.client_default_headers {
            Some(defaults) if defaults.applies_to(&url) => defaults.headers,
            _ => HeaderMap::new(),
        };
        headers.overlay(self.headers);

        // Hyper owns Content-Length framing; Lily only infers a missing Content-Type.
        if let Some(ref body) = self.body {
            if headers.get("Content-Type").is_none() {
                if let Some(content_type) = body.content_type() {
                    headers.insert("Content-Type", content_type)?;
                }
            }
        }

        Ok(Request::new(method, url, headers, self.body, self.config))
    }
}

/// Convenience methods for common request types
impl RequestBuilder {
    /// Create a GET request builder
    pub fn get(url: impl AsRef<str>) -> Result<Self> {
        let mut builder = Self::new();
        builder.method(Method::Get);
        builder.url(url)?;
        Ok(builder)
    }

    /// Create a POST request builder
    pub fn post(url: impl AsRef<str>) -> Result<Self> {
        let mut builder = Self::new();
        builder.method(Method::Post);
        builder.url(url)?;
        Ok(builder)
    }

    /// Create a PUT request builder
    pub fn put(url: impl AsRef<str>) -> Result<Self> {
        let mut builder = Self::new();
        builder.method(Method::Put).url(url)?;
        Ok(builder)
    }

    /// Create a DELETE request builder
    pub fn delete(url: impl AsRef<str>) -> Result<Self> {
        let mut builder = Self::new();
        builder.method(Method::Delete).url(url)?;
        Ok(builder)
    }

    /// Create a PATCH request builder
    pub fn patch(url: impl AsRef<str>) -> Result<Self> {
        let mut builder = Self::new();
        builder.method(Method::Patch).url(url)?;
        Ok(builder)
    }

    /// Create a HEAD request builder
    pub fn head(url: impl AsRef<str>) -> Result<Self> {
        let mut builder = Self::new();
        builder.method(Method::Head).url(url)?;
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    #[derive(Debug)]
    struct NonRepeatableBody;

    #[async_trait::async_trait]
    impl Body for NonRepeatableBody {
        fn content_type(&self) -> Option<&str> {
            None
        }

        fn content_length(&self) -> Option<usize> {
            Some(0)
        }

        async fn to_bytes(&mut self) -> Result<Bytes> {
            Ok(Bytes::new())
        }

        fn is_repeatable(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct UnknownLengthBody;

    #[async_trait::async_trait]
    impl Body for UnknownLengthBody {
        fn content_type(&self) -> Option<&str> {
            None
        }

        fn content_length(&self) -> Option<usize> {
            None
        }

        async fn to_bytes(&mut self) -> Result<Bytes> {
            Ok(Bytes::new())
        }
    }

    #[test]
    fn test_basic_request_building() {
        let mut builder = RequestBuilder::new();
        builder.method(Method::Get);
        builder.url("https://api.example.com/users").unwrap();
        builder.header("Accept", "application/json").unwrap();
        let request = builder.build().unwrap();

        assert_eq!(request.method(), &Method::Get);
        assert_eq!(request.url().as_str(), "https://api.example.com/users");
        assert_eq!(request.headers().get("Accept"), Some("application/json"));
    }

    #[test]
    fn test_json_request() {
        let data = json!({
            "name": "John Doe",
            "email": "john@example.com"
        });

        let mut builder = RequestBuilder::post("https://api.example.com/users").unwrap();
        builder.json(&data).unwrap();
        let request = builder.build().unwrap();

        assert_eq!(request.method(), &Method::Post);
        assert!(request.body().is_some());
        assert_eq!(
            request.headers().get("Content-Type"),
            Some("application/json; charset=utf-8")
        );
    }

    #[test]
    fn test_form_request() {
        let form_data = vec![("name", "John Doe"), ("email", "john@example.com")];

        let mut builder = RequestBuilder::post("https://api.example.com/users").unwrap();
        builder.form(form_data).unwrap();
        let request = builder.build().unwrap();

        assert_eq!(request.method(), &Method::Post);
        assert!(request.body().is_some());
        assert_eq!(
            request.headers().get("Content-Type"),
            Some("application/x-www-form-urlencoded")
        );
    }

    #[test]
    fn test_authentication() {
        let mut builder = RequestBuilder::get("https://api.example.com/protected").unwrap();
        builder.bearer_auth("token123").unwrap();
        let request = builder.build().unwrap();

        assert_eq!(
            request.headers().get("Authorization"),
            Some("Bearer token123")
        );

        let mut builder2 = RequestBuilder::get("https://api.example.com/protected").unwrap();
        builder2.basic_auth("user", Some("pass")).unwrap();
        let request2 = builder2.build().unwrap();

        // Use base64 0.21 API
        use base64::{engine::general_purpose, Engine as _};
        let expected_basic = format!("Basic {}", general_purpose::STANDARD.encode("user:pass"));
        assert_eq!(
            request2.headers().get("Authorization"),
            Some(expected_basic.as_str())
        );
    }

    #[test]
    fn test_query_parameters() {
        let mut builder = RequestBuilder::get("https://api.example.com/search").unwrap();
        builder.query(vec![("q", "rust"), ("limit", "10")]).unwrap();
        let request = builder.build().unwrap();

        assert!(request.url().query().unwrap().contains("q=rust"));
        assert!(request.url().query().unwrap().contains("limit=10"));
    }

    #[test]
    fn test_configuration() {
        let mut builder = RequestBuilder::get("https://api.example.com/test").unwrap();
        builder.timeout(Duration::from_secs(30));
        builder.follow_redirects(false);
        let request = builder.build().unwrap();

        assert_eq!(
            request.config().timeout_override(),
            Some(Duration::from_secs(30))
        );
        assert!(!request.config().follows_redirects());
    }

    #[test]
    fn test_convenience_methods() {
        let get_request = RequestBuilder::get("https://example.com")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(get_request.method(), &Method::Get);

        let post_request = RequestBuilder::post("https://example.com")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(post_request.method(), &Method::Post);
    }

    #[test]
    fn test_missing_required_fields() {
        let result = RequestBuilder::new().build();
        assert!(result.is_err());

        let mut builder = RequestBuilder::new();
        builder.method(Method::Get);
        let result = builder.build();
        assert!(result.is_err());
    }

    #[test]
    fn rejects_non_repeatable_custom_body_during_build() {
        let mut builder = RequestBuilder::post("https://example.test").unwrap();
        builder.body(Box::new(NonRepeatableBody));

        let error = builder.build().unwrap_err();
        assert!(matches!(
            error,
            HttpClientError::UnsupportedConfiguration(_)
        ));
        assert_eq!(error.diagnostic_code(), "REQUEST_BODY_NOT_REPEATABLE");
    }

    #[test]
    fn rejects_unknown_length_custom_body_during_build() {
        let mut builder = RequestBuilder::post("https://example.test").unwrap();
        builder.body(Box::new(UnknownLengthBody));

        let error = builder.build().unwrap_err();
        assert!(matches!(
            error,
            HttpClientError::UnsupportedConfiguration(_)
        ));
        assert_eq!(error.diagnostic_code(), "REQUEST_BODY_LENGTH_REQUIRED");
    }

    #[test]
    fn url_setters_reject_unsafe_urls_at_the_fallible_builder_boundary() {
        const SECRET: &str = "LILY_SECRET_URL_PASSWORD";
        let cases = [
            (
                format!("https://user:{SECRET}@example.test/resource"),
                "REQUEST_URL_CREDENTIALS",
            ),
            (
                "ftp://example.test/resource".to_string(),
                "REQUEST_URL_SCHEME_UNSUPPORTED",
            ),
            ("not-an-absolute-url".to_string(), "REQUEST_URL_INVALID"),
        ];

        for (value, diagnostic) in cases {
            let mut builder = RequestBuilder::new();
            let error = builder.url(&value).unwrap_err();
            assert!(matches!(error, HttpClientError::InvalidUrl(_)));
            assert_eq!(error.diagnostic_code(), diagnostic);
            assert!(!format!("{error:?}").contains(SECRET));
            assert!(!error.to_string().contains(SECRET));
        }

        let mut builder = RequestBuilder::new();
        let parsed = Url::parse(&format!("https://user:{SECRET}@example.test/resource")).unwrap();
        let error = builder.url_parsed(parsed).unwrap_err();
        assert_eq!(error.diagnostic_code(), "REQUEST_URL_CREDENTIALS");
        assert!(!format!("{error:?}").contains(SECRET));
        assert!(!error.to_string().contains(SECRET));
    }

    #[test]
    fn custom_method_tokens_remain_case_sensitive_and_fail_fast_when_invalid() {
        let request = {
            let mut builder = RequestBuilder::new();
            builder
                .method(Method::from("post"))
                .url("https://example.test")
                .unwrap();
            builder.build().unwrap()
        };
        assert_eq!(request.method().as_str(), "post");
        assert!(matches!(request.method(), Method::Custom(value) if value == "post"));

        let mut builder = RequestBuilder::new();
        builder
            .method(Method::custom("INVALID METHOD"))
            .url("https://example.test")
            .unwrap();
        let error = builder.build().unwrap_err();
        assert!(matches!(error, HttpClientError::InvalidMethod(_)));
        assert_eq!(error.diagnostic_code(), "REQUEST_METHOD_TOKEN_INVALID");
    }

    #[test]
    fn rejects_hop_by_hop_and_proxy_only_request_headers_case_insensitively() {
        for name in [
            "Connection",
            "kEeP-aLiVe",
            "Proxy-Connection",
            "Proxy-Authenticate",
            "Proxy-Authorization",
            "TE",
            "Trailer",
            "Transfer-Encoding",
            "Upgrade",
        ] {
            let mut builder = RequestBuilder::get("https://example.test").unwrap();
            let error = builder
                .header(name, "LILY_SECRET_HEADER_VALUE")
                .unwrap_err();

            assert!(matches!(error, HttpClientError::InvalidHeader(_)));
            assert_eq!(
                error.diagnostic_code(),
                "REQUEST_HOP_BY_HOP_HEADER",
                "header {name}"
            );
            assert!(!error.to_string().contains("LILY_SECRET_HEADER_VALUE"));
            assert!(!builder.headers.contains_key(name));
        }
    }

    #[test]
    fn bulk_headers_use_the_same_hop_by_hop_rejection() {
        let mut builder = RequestBuilder::get("https://example.test").unwrap();
        let error = builder
            .headers([("Accept", "application/json"), ("uPgRaDe", "websocket")])
            .unwrap_err();

        assert_eq!(error.diagnostic_code(), "REQUEST_HOP_BY_HOP_HEADER");
        assert_eq!(builder.headers.get("Accept"), Some("application/json"));
        assert!(!builder.headers.contains_key("Upgrade"));
    }

    #[test]
    fn content_length_is_rejected_and_body_framing_stays_transport_owned() {
        let mut builder = RequestBuilder::post("https://example.test").unwrap();
        let error = builder.header("cOnTeNt-LeNgTh", "999").unwrap_err();
        assert_eq!(error.diagnostic_code(), "REQUEST_TRANSPORT_OWNED_HEADER");

        builder.text("abc");
        let request = builder.build().unwrap();
        assert!(!request.headers().contains_key("Content-Length"));

        let mut bulk = RequestBuilder::post("https://example.test").unwrap();
        let error = bulk.headers([("CONTENT-LENGTH", "999")]).unwrap_err();
        assert_eq!(error.diagnostic_code(), "REQUEST_TRANSPORT_OWNED_HEADER");
        assert!(!bulk.headers.contains_key("Content-Length"));
    }
}
