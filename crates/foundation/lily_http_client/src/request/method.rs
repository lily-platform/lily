use std::fmt;

/// HTTP request methods as defined in RFC 7231 and related RFCs
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Method {
    /// GET method - retrieve data
    Get,
    /// POST method - submit data
    Post,
    /// PUT method - update/replace resource
    Put,
    /// DELETE method - remove resource
    Delete,
    /// HEAD method - retrieve headers only
    Head,
    /// OPTIONS method - check allowed methods
    Options,
    /// PATCH method - partial update
    Patch,
    /// TRACE method - diagnostic trace
    Trace,
    /// CONNECT method - establish tunnel
    Connect,
    /// Custom method for non-standard HTTP methods
    Custom(String),
}

impl Method {
    /// Create a custom HTTP method
    pub fn custom(method: impl Into<String>) -> Self {
        Method::Custom(method.into())
    }

    /// Get the method as a string slice
    pub fn as_str(&self) -> &str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
            Method::Head => "HEAD",
            Method::Options => "OPTIONS",
            Method::Patch => "PATCH",
            Method::Trace => "TRACE",
            Method::Connect => "CONNECT",
            Method::Custom(method) => method,
        }
    }

    /// Bounded method value for telemetry and other implicit diagnostic
    /// surfaces. The exact custom token remains available through [`Self::as_str`]
    /// for explicit access and wire serialization.
    pub(crate) const fn telemetry_name(&self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
            Method::Head => "HEAD",
            Method::Options => "OPTIONS",
            Method::Patch => "PATCH",
            Method::Trace => "TRACE",
            Method::Connect => "CONNECT",
            Method::Custom(_) => "_OTHER",
        }
    }

    /// Check if method is safe (read-only)
    pub fn is_safe(&self) -> bool {
        matches!(
            self,
            Method::Get | Method::Head | Method::Options | Method::Trace
        )
    }

    /// Check if method is idempotent
    pub fn is_idempotent(&self) -> bool {
        matches!(
            self,
            Method::Get
                | Method::Head
                | Method::Put
                | Method::Delete
                | Method::Options
                | Method::Trace
        )
    }
}

impl fmt::Debug for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Method::Custom(_) => f.debug_tuple("Custom").field(&"[REDACTED]").finish(),
            _ => f.write_str(self.telemetry_name()),
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for Method {
    fn from(method: &str) -> Self {
        match method {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "DELETE" => Method::Delete,
            "HEAD" => Method::Head,
            "OPTIONS" => Method::Options,
            "PATCH" => Method::Patch,
            "TRACE" => Method::Trace,
            "CONNECT" => Method::Connect,
            _ => Method::Custom(method.to_string()),
        }
    }
}

impl From<String> for Method {
    fn from(method: String) -> Self {
        match method.as_str() {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "DELETE" => Method::Delete,
            "HEAD" => Method::Head,
            "OPTIONS" => Method::Options,
            "PATCH" => Method::Patch,
            "TRACE" => Method::Trace,
            "CONNECT" => Method::Connect,
            _ => Method::Custom(method),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method_as_str() {
        assert_eq!(Method::Get.as_str(), "GET");
        assert_eq!(Method::Post.as_str(), "POST");
        assert_eq!(Method::Custom("CUSTOM".to_string()).as_str(), "CUSTOM");
    }

    #[test]
    fn test_method_from_str() {
        assert_eq!(Method::from("GET"), Method::Get);
        assert_eq!(Method::from("post"), Method::Custom("post".to_string()));
        assert_eq!(Method::from("CUSTOM"), Method::Custom("CUSTOM".to_string()));
        assert_eq!(
            Method::from("gEt".to_string()),
            Method::Custom("gEt".to_string())
        );
    }

    #[test]
    fn test_method_properties() {
        assert!(Method::Get.is_safe());
        assert!(!Method::Post.is_safe());

        assert!(Method::Get.is_idempotent());
        assert!(!Method::Post.is_idempotent());
    }

    #[test]
    fn custom_method_diagnostics_are_bounded_but_wire_value_is_preserved() {
        const SECRET: &str = "LILY_SECRET_IN_CUSTOM_METHOD";
        let method = Method::custom(SECRET);

        assert_eq!(method.as_str(), SECRET);
        assert_eq!(method.to_string(), SECRET);
        assert_eq!(method.telemetry_name(), "_OTHER");
        assert!(!format!("{method:?}").contains(SECRET));
    }
}
