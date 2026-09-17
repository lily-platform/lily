use std::fmt;

/// HTTP status codes as defined in RFC 7231 and related RFCs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatusCode(u16);

impl StatusCode {
    // 1xx Informational
    pub const CONTINUE: StatusCode = StatusCode(100);
    pub const SWITCHING_PROTOCOLS: StatusCode = StatusCode(101);
    pub const PROCESSING: StatusCode = StatusCode(102);

    // 2xx Success
    pub const OK: StatusCode = StatusCode(200);
    pub const CREATED: StatusCode = StatusCode(201);
    pub const ACCEPTED: StatusCode = StatusCode(202);
    pub const NON_AUTHORITATIVE_INFORMATION: StatusCode = StatusCode(203);
    pub const NO_CONTENT: StatusCode = StatusCode(204);
    pub const RESET_CONTENT: StatusCode = StatusCode(205);
    pub const PARTIAL_CONTENT: StatusCode = StatusCode(206);

    // 3xx Redirection
    pub const MULTIPLE_CHOICES: StatusCode = StatusCode(300);
    pub const MOVED_PERMANENTLY: StatusCode = StatusCode(301);
    pub const FOUND: StatusCode = StatusCode(302);
    pub const SEE_OTHER: StatusCode = StatusCode(303);
    pub const NOT_MODIFIED: StatusCode = StatusCode(304);
    pub const USE_PROXY: StatusCode = StatusCode(305);
    pub const TEMPORARY_REDIRECT: StatusCode = StatusCode(307);
    pub const PERMANENT_REDIRECT: StatusCode = StatusCode(308);

    // 4xx Client Error
    pub const BAD_REQUEST: StatusCode = StatusCode(400);
    pub const UNAUTHORIZED: StatusCode = StatusCode(401);
    pub const PAYMENT_REQUIRED: StatusCode = StatusCode(402);
    pub const FORBIDDEN: StatusCode = StatusCode(403);
    pub const NOT_FOUND: StatusCode = StatusCode(404);
    pub const METHOD_NOT_ALLOWED: StatusCode = StatusCode(405);
    pub const NOT_ACCEPTABLE: StatusCode = StatusCode(406);
    pub const PROXY_AUTHENTICATION_REQUIRED: StatusCode = StatusCode(407);
    pub const REQUEST_TIMEOUT: StatusCode = StatusCode(408);
    pub const CONFLICT: StatusCode = StatusCode(409);
    pub const GONE: StatusCode = StatusCode(410);
    pub const LENGTH_REQUIRED: StatusCode = StatusCode(411);
    pub const PRECONDITION_FAILED: StatusCode = StatusCode(412);
    pub const PAYLOAD_TOO_LARGE: StatusCode = StatusCode(413);
    pub const URI_TOO_LONG: StatusCode = StatusCode(414);
    pub const UNSUPPORTED_MEDIA_TYPE: StatusCode = StatusCode(415);
    pub const RANGE_NOT_SATISFIABLE: StatusCode = StatusCode(416);
    pub const EXPECTATION_FAILED: StatusCode = StatusCode(417);
    pub const IM_A_TEAPOT: StatusCode = StatusCode(418);
    pub const UNPROCESSABLE_ENTITY: StatusCode = StatusCode(422);
    pub const TOO_MANY_REQUESTS: StatusCode = StatusCode(429);

    // 5xx Server Error
    pub const INTERNAL_SERVER_ERROR: StatusCode = StatusCode(500);
    pub const NOT_IMPLEMENTED: StatusCode = StatusCode(501);
    pub const BAD_GATEWAY: StatusCode = StatusCode(502);
    pub const SERVICE_UNAVAILABLE: StatusCode = StatusCode(503);
    pub const GATEWAY_TIMEOUT: StatusCode = StatusCode(504);
    pub const HTTP_VERSION_NOT_SUPPORTED: StatusCode = StatusCode(505);

    /// Create a new status code
    pub const fn from_u16(code: u16) -> StatusCode {
        StatusCode(code)
    }

    /// Get the numeric status code
    pub const fn as_u16(&self) -> u16 {
        self.0
    }

    /// Get the canonical reason phrase for this status code
    pub fn canonical_reason(&self) -> Option<&'static str> {
        match self.0 {
            100 => Some("Continue"),
            101 => Some("Switching Protocols"),
            102 => Some("Processing"),
            200 => Some("OK"),
            201 => Some("Created"),
            202 => Some("Accepted"),
            203 => Some("Non-Authoritative Information"),
            204 => Some("No Content"),
            205 => Some("Reset Content"),
            206 => Some("Partial Content"),
            300 => Some("Multiple Choices"),
            301 => Some("Moved Permanently"),
            302 => Some("Found"),
            303 => Some("See Other"),
            304 => Some("Not Modified"),
            305 => Some("Use Proxy"),
            307 => Some("Temporary Redirect"),
            308 => Some("Permanent Redirect"),
            400 => Some("Bad Request"),
            401 => Some("Unauthorized"),
            402 => Some("Payment Required"),
            403 => Some("Forbidden"),
            404 => Some("Not Found"),
            405 => Some("Method Not Allowed"),
            406 => Some("Not Acceptable"),
            407 => Some("Proxy Authentication Required"),
            408 => Some("Request Timeout"),
            409 => Some("Conflict"),
            410 => Some("Gone"),
            411 => Some("Length Required"),
            412 => Some("Precondition Failed"),
            413 => Some("Payload Too Large"),
            414 => Some("URI Too Long"),
            415 => Some("Unsupported Media Type"),
            416 => Some("Range Not Satisfiable"),
            417 => Some("Expectation Failed"),
            418 => Some("I'm a teapot"),
            422 => Some("Unprocessable Entity"),
            429 => Some("Too Many Requests"),
            500 => Some("Internal Server Error"),
            501 => Some("Not Implemented"),
            502 => Some("Bad Gateway"),
            503 => Some("Service Unavailable"),
            504 => Some("Gateway Timeout"),
            505 => Some("HTTP Version Not Supported"),
            _ => None,
        }
    }

    /// Check if status code is informational (1xx)
    pub const fn is_informational(&self) -> bool {
        self.0 >= 100 && self.0 < 200
    }

    /// Check if status code is success (2xx)
    pub const fn is_success(&self) -> bool {
        self.0 >= 200 && self.0 < 300
    }

    /// Check if status code is redirection (3xx)
    pub const fn is_redirection(&self) -> bool {
        self.0 >= 300 && self.0 < 400
    }

    /// Check if status code is client error (4xx)
    pub const fn is_client_error(&self) -> bool {
        self.0 >= 400 && self.0 < 500
    }

    /// Check if status code is server error (5xx)
    pub const fn is_server_error(&self) -> bool {
        self.0 >= 500 && self.0 < 600
    }

    /// Check if status code represents an error (4xx or 5xx)
    pub const fn is_error(&self) -> bool {
        self.0 >= 400
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for StatusCode {
    fn from(code: u16) -> Self {
        StatusCode(code)
    }
}

impl From<StatusCode> for u16 {
    fn from(status: StatusCode) -> Self {
        status.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_code_creation() {
        let status = StatusCode::from_u16(200);
        assert_eq!(status.as_u16(), 200);
        assert_eq!(status, StatusCode::OK);
    }

    #[test]
    fn test_status_code_categories() {
        assert!(StatusCode::OK.is_success());
        assert!(!StatusCode::OK.is_error());

        assert!(StatusCode::NOT_FOUND.is_client_error());
        assert!(StatusCode::NOT_FOUND.is_error());

        assert!(StatusCode::INTERNAL_SERVER_ERROR.is_server_error());
        assert!(StatusCode::INTERNAL_SERVER_ERROR.is_error());
    }

    #[test]
    fn test_canonical_reason() {
        assert_eq!(StatusCode::OK.canonical_reason(), Some("OK"));
        assert_eq!(StatusCode::NOT_FOUND.canonical_reason(), Some("Not Found"));
        assert_eq!(StatusCode::from_u16(999).canonical_reason(), None);
    }
}
