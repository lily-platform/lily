/// Common HTTP header names as constants
///
/// This module provides constants for commonly used HTTP headers
/// to avoid typos and provide better IDE support.
// Request headers
pub const ACCEPT: &str = "accept";
pub const ACCEPT_CHARSET: &str = "accept-charset";
pub const ACCEPT_ENCODING: &str = "accept-encoding";
pub const ACCEPT_LANGUAGE: &str = "accept-language";
pub const AUTHORIZATION: &str = "authorization";
pub const CACHE_CONTROL: &str = "cache-control";
pub const CONNECTION: &str = "connection";
pub const CONTENT_LENGTH: &str = "content-length";
pub const CONTENT_TYPE: &str = "content-type";
pub const COOKIE: &str = "cookie";
pub const HOST: &str = "host";
pub const IF_MATCH: &str = "if-match";
pub const IF_MODIFIED_SINCE: &str = "if-modified-since";
pub const IF_NONE_MATCH: &str = "if-none-match";
pub const IF_RANGE: &str = "if-range";
pub const IF_UNMODIFIED_SINCE: &str = "if-unmodified-since";
pub const ORIGIN: &str = "origin";
pub const PRAGMA: &str = "pragma";
pub const RANGE: &str = "range";
pub const REFERER: &str = "referer";
pub const USER_AGENT: &str = "user-agent";

// Response headers
pub const ACCESS_CONTROL_ALLOW_CREDENTIALS: &str = "access-control-allow-credentials";
pub const ACCESS_CONTROL_ALLOW_HEADERS: &str = "access-control-allow-headers";
pub const ACCESS_CONTROL_ALLOW_METHODS: &str = "access-control-allow-methods";
pub const ACCESS_CONTROL_ALLOW_ORIGIN: &str = "access-control-allow-origin";
pub const ACCESS_CONTROL_EXPOSE_HEADERS: &str = "access-control-expose-headers";
pub const ACCESS_CONTROL_MAX_AGE: &str = "access-control-max-age";
pub const AGE: &str = "age";
pub const ALLOW: &str = "allow";
pub const CONTENT_DISPOSITION: &str = "content-disposition";
pub const CONTENT_ENCODING: &str = "content-encoding";
pub const CONTENT_LANGUAGE: &str = "content-language";
pub const CONTENT_LOCATION: &str = "content-location";
pub const CONTENT_RANGE: &str = "content-range";
pub const DATE: &str = "date";
pub const ETAG: &str = "etag";
pub const EXPIRES: &str = "expires";
pub const LAST_MODIFIED: &str = "last-modified";
pub const LOCATION: &str = "location";
pub const RETRY_AFTER: &str = "retry-after";
pub const SERVER: &str = "server";
pub const SET_COOKIE: &str = "set-cookie";
pub const TRANSFER_ENCODING: &str = "transfer-encoding";
pub const VARY: &str = "vary";
pub const WWW_AUTHENTICATE: &str = "www-authenticate";

// Common content types
pub mod content_type {
    pub const APPLICATION_JSON: &str = "application/json";
    pub const APPLICATION_XML: &str = "application/xml";
    pub const APPLICATION_FORM_URLENCODED: &str = "application/x-www-form-urlencoded";
    pub const APPLICATION_OCTET_STREAM: &str = "application/octet-stream";
    pub const MULTIPART_FORM_DATA: &str = "multipart/form-data";
    pub const TEXT_PLAIN: &str = "text/plain";
    pub const TEXT_HTML: &str = "text/html";
    pub const TEXT_CSS: &str = "text/css";
    pub const TEXT_JAVASCRIPT: &str = "text/javascript";
    pub const IMAGE_JPEG: &str = "image/jpeg";
    pub const IMAGE_PNG: &str = "image/png";
    pub const IMAGE_GIF: &str = "image/gif";
    pub const IMAGE_SVG: &str = "image/svg+xml";
}

// Common encoding types
pub mod encoding {
    pub const GZIP: &str = "gzip";
    pub const DEFLATE: &str = "deflate";
    pub const BR: &str = "br";
    pub const COMPRESS: &str = "compress";
    pub const IDENTITY: &str = "identity";
}

// Common cache control directives
pub mod cache_control {
    pub const NO_CACHE: &str = "no-cache";
    pub const NO_STORE: &str = "no-store";
    pub const MUST_REVALIDATE: &str = "must-revalidate";
    pub const PUBLIC: &str = "public";
    pub const PRIVATE: &str = "private";
    pub const MAX_AGE: &str = "max-age";
    pub const S_MAXAGE: &str = "s-maxage";
}

/// Helper struct for building common header values
#[derive(Debug, Clone)]
pub struct HeaderValue;

impl HeaderValue {
    /// Create a Bearer authorization header value
    pub fn bearer_auth(token: &str) -> String {
        format!("Bearer {token}")
    }

    /// Create a Basic authorization header value
    pub fn basic_auth(username: &str, password: &str) -> String {
        use base64::Engine;
        let credentials = format!("{username}:{password}");
        let encoded = base64::engine::general_purpose::STANDARD.encode(credentials.as_bytes());
        format!("Basic {encoded}")
    }

    /// Create a max-age cache control value
    pub fn max_age(seconds: u32) -> String {
        format!("max-age={seconds}")
    }

    /// Create a content type with charset
    pub fn content_type_with_charset(content_type: &str, charset: &str) -> String {
        format!("{content_type}; charset={charset}")
    }

    /// Create a multipart form data content type with boundary
    pub fn multipart_form_data(boundary: &str) -> String {
        format!("multipart/form-data; boundary={boundary}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_constants() {
        assert_eq!(CONTENT_TYPE, "content-type");
        assert_eq!(AUTHORIZATION, "authorization");
        assert_eq!(USER_AGENT, "user-agent");
    }

    #[test]
    fn test_content_type_constants() {
        assert_eq!(content_type::APPLICATION_JSON, "application/json");
        assert_eq!(content_type::TEXT_HTML, "text/html");
        assert_eq!(
            content_type::APPLICATION_FORM_URLENCODED,
            "application/x-www-form-urlencoded"
        );
    }

    #[test]
    fn test_header_value_helpers() {
        assert_eq!(HeaderValue::bearer_auth("token123"), "Bearer token123");
        assert_eq!(HeaderValue::max_age(3600), "max-age=3600");
        assert_eq!(
            HeaderValue::content_type_with_charset("text/html", "utf-8"),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            HeaderValue::multipart_form_data("boundary123"),
            "multipart/form-data; boundary=boundary123"
        );
    }

    #[test]
    fn test_basic_auth() {
        let auth = HeaderValue::basic_auth("user", "pass");
        assert!(auth.starts_with("Basic "));

        // Decode and verify
        let encoded = &auth[6..]; // Remove "Basic " prefix
        let decoded =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).unwrap();
        let credentials = String::from_utf8(decoded).unwrap();
        assert_eq!(credentials, "user:pass");
    }
}
