use crate::error::{HttpClientError, Result};
use http::header::{HeaderName, HeaderValue};
use std::collections::HashMap;
use std::fmt;

/// A case-insensitive HTTP header map
#[derive(Clone, PartialEq)]
pub struct HeaderMap {
    headers: HashMap<String, Vec<String>>,
}

impl HeaderMap {
    /// Create a new empty header map
    pub fn new() -> Self {
        Self {
            headers: HashMap::new(),
        }
    }

    /// Create a header map with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            headers: HashMap::with_capacity(capacity),
        }
    }

    /// Insert a header value, replacing any existing values
    pub fn insert(&mut self, name: &str, value: &str) -> Result<()> {
        let name = Self::normalize_name(name)?;
        Self::validate_value(value)?;
        self.headers.insert(name, vec![value.to_string()]);
        Ok(())
    }

    /// Append a header value to existing values
    pub fn append(&mut self, name: &str, value: &str) -> Result<()> {
        let name = Self::normalize_name(name)?;
        Self::validate_value(value)?;
        self.headers
            .entry(name)
            .or_default()
            .push(value.to_string());
        Ok(())
    }

    /// Get the first value for a header
    pub fn get(&self, name: &str) -> Option<&str> {
        let name = Self::normalize_name(name).ok()?;
        self.headers.get(&name)?.first().map(|s| s.as_str())
    }

    /// Get all values for a header
    pub fn get_all(&self, name: &str) -> Option<&Vec<String>> {
        let name = Self::normalize_name(name).ok()?;
        self.headers.get(&name)
    }

    /// Remove all values for a header
    pub fn remove(&mut self, name: &str) -> Option<Vec<String>> {
        let name = Self::normalize_name(name).ok()?;
        self.headers.remove(&name)
    }

    /// Check if a header exists
    pub fn contains_key(&self, name: &str) -> bool {
        if let Ok(name) = Self::normalize_name(name) {
            self.headers.contains_key(&name)
        } else {
            false
        }
    }

    /// Get the number of headers
    pub fn len(&self) -> usize {
        self.headers.len()
    }

    /// Check if the header map is empty
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Clear all headers
    pub fn clear(&mut self) {
        self.headers.clear();
    }

    /// Merge an internally owned map, replacing all values with the same
    /// normalized name. This keeps client defaults separate until a request's
    /// final URL and explicit header precedence are known.
    pub(crate) fn overlay(&mut self, headers: Self) {
        self.headers.extend(headers.headers);
    }

    /// Iterate over all headers
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Vec<String>)> {
        self.headers.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Get all header names
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.headers.keys().map(|s| s.as_str())
    }

    /// Normalize header name to lowercase for case-insensitive comparison
    fn normalize_name(name: &str) -> Result<String> {
        HeaderName::from_bytes(name.as_bytes())
            .map(|name| name.as_str().to_string())
            .map_err(|_| HttpClientError::invalid_header("Invalid HTTP header name"))
    }

    /// Validate header value
    fn validate_value(value: &str) -> Result<()> {
        HeaderValue::from_str(value)
            .map(|_| ())
            .map_err(|_| HttpClientError::invalid_header("Invalid HTTP header value"))
    }
}

impl Default for HeaderMap {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for HeaderMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value_count = self.headers.values().map(Vec::len).sum::<usize>();
        let value_bytes = self
            .headers
            .values()
            .flatten()
            .map(String::len)
            .sum::<usize>();

        f.debug_struct("HeaderMap")
            .field("names", &self.headers.keys().collect::<Vec<_>>())
            .field("value_count", &value_count)
            .field("value_bytes", &value_bytes)
            .finish()
    }
}

impl fmt::Display for HeaderMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, values) in &self.headers {
            for _ in values {
                writeln!(f, "{name}: [REDACTED]")?;
            }
        }
        Ok(())
    }
}

impl TryFrom<HashMap<String, String>> for HeaderMap {
    type Error = HttpClientError;

    fn try_from(map: HashMap<String, String>) -> Result<Self> {
        let mut header_map = HeaderMap::new();
        for (name, value) in map {
            if header_map.contains_key(&name) {
                return Err(HttpClientError::invalid_header(
                    "duplicate HTTP header name after normalization",
                ));
            }
            header_map.insert(&name, &value)?;
        }
        Ok(header_map)
    }
}

impl TryFrom<Vec<(String, String)>> for HeaderMap {
    type Error = HttpClientError;

    fn try_from(headers: Vec<(String, String)>) -> Result<Self> {
        let mut header_map = HeaderMap::new();
        for (name, value) in headers {
            header_map.append(&name, &value)?;
        }
        Ok(header_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_map_basic_operations() {
        let mut headers = HeaderMap::new();

        // Test insert and get
        headers.insert("Content-Type", "application/json").unwrap();
        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.get("Content-Type"), Some("application/json"));

        // Test case insensitivity
        assert_eq!(headers.get("CONTENT-TYPE"), Some("application/json"));
    }

    #[test]
    fn test_header_map_append() {
        let mut headers = HeaderMap::new();

        headers.insert("Accept", "text/html").unwrap();
        headers.append("Accept", "application/json").unwrap();

        let values = headers.get_all("accept").unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&"text/html".to_string()));
        assert!(values.contains(&"application/json".to_string()));
    }

    #[test]
    fn test_header_validation() {
        let mut headers = HeaderMap::new();

        // Test empty name
        assert!(headers.insert("", "value").is_err());

        // Test invalid characters in name
        assert!(headers.insert("invalid\nname", "value").is_err());
        assert!(headers.insert("invalid\tname", "value").is_err());
        assert!(headers.insert("invalid:name", "value").is_err());
        assert!(headers.insert("invalid(name)", "value").is_err());
    }

    #[test]
    fn test_header_map_from_hashmap() {
        let mut map = HashMap::new();
        map.insert("Content-Type".to_string(), "application/json".to_string());
        map.insert("Accept".to_string(), "text/html".to_string());

        let headers = HeaderMap::try_from(map).unwrap();
        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.get("accept"), Some("text/html"));
    }

    #[test]
    fn collection_conversions_reject_invalid_headers_without_silent_drop() {
        let mut map = HashMap::new();
        map.insert("Accept".to_string(), "application/json".to_string());
        map.insert("invalid\nname".to_string(), "hidden".to_string());

        assert!(matches!(
            HeaderMap::try_from(map),
            Err(HttpClientError::InvalidHeader(_))
        ));

        let headers = vec![
            ("Accept".to_string(), "application/json".to_string()),
            ("X-Test".to_string(), "invalid\r\nvalue".to_string()),
        ];
        assert!(matches!(
            HeaderMap::try_from(headers),
            Err(HttpClientError::InvalidHeader(_))
        ));
    }

    #[test]
    fn vec_conversion_preserves_ordered_values_for_normalized_duplicate_names() {
        let headers = HeaderMap::try_from(vec![
            ("X-Trace".to_string(), "first".to_string()),
            ("x-trace".to_string(), "second".to_string()),
        ])
        .unwrap();

        assert_eq!(
            headers.get_all("X-TRACE").unwrap(),
            &vec!["first".to_string(), "second".to_string()]
        );
    }

    #[test]
    fn hashmap_conversion_rejects_normalization_collisions() {
        let mut map = HashMap::new();
        map.insert("X-Trace".to_string(), "first".to_string());
        map.insert("x-trace".to_string(), "second".to_string());

        let error = HeaderMap::try_from(map).unwrap_err();
        assert!(matches!(error, HttpClientError::InvalidHeader(_)));
        assert_eq!(error.diagnostic_code(), "HEADER_NAME_DUPLICATE");
    }

    #[test]
    fn safe_representations_hide_values_without_changing_explicit_access() {
        const SECRET: &str = "LILY_SECRET_IN_AUTH_HEADER";
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", SECRET).unwrap();

        let debug = format!("{headers:?}");
        let display = headers.to_string();

        assert!(!debug.contains(SECRET));
        assert!(!display.contains(SECRET));
        assert!(debug.contains("authorization"));
        assert!(display.contains("authorization: [REDACTED]"));
        assert_eq!(headers.get("Authorization"), Some(SECRET));
        assert!(crate::header::HeaderParser::format_headers(&headers).contains(SECRET));
    }
}
