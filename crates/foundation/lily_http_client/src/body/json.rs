use crate::body::Body;
use crate::error::{HttpClientError, Result};
use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use std::fmt;

/// JSON body implementation for serializable types
#[derive(Clone)]
pub struct JsonBody {
    json_string: String,
    content_type: String,
}

impl fmt::Debug for JsonBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonBody")
            .field("content_length", &self.json_string.len())
            .finish()
    }
}

impl JsonBody {
    /// Create a new JSON body from a serializable value
    pub fn new<T: Serialize>(value: &T) -> Result<Self> {
        let json_string = serde_json::to_string(value)
            .map_err(|e| HttpClientError::body(format!("Failed to serialize JSON: {e}")))?;

        Ok(Self {
            json_string,
            content_type: "application/json; charset=utf-8".to_string(),
        })
    }

    /// Create a JSON body from a raw JSON string
    pub fn from_string(json: impl Into<String>) -> Self {
        Self {
            json_string: json.into(),
            content_type: "application/json; charset=utf-8".to_string(),
        }
    }

    /// Create a JSON body with pretty printing
    pub fn pretty<T: Serialize>(value: &T) -> Result<Self> {
        let json_string = serde_json::to_string_pretty(value)
            .map_err(|e| HttpClientError::body(format!("Failed to serialize JSON: {e}")))?;

        Ok(Self {
            json_string,
            content_type: "application/json; charset=utf-8".to_string(),
        })
    }

    /// Create a JSON body with custom content type
    pub fn with_content_type<T: Serialize>(
        value: &T,
        content_type: impl Into<String>,
    ) -> Result<Self> {
        let json_string = serde_json::to_string(value)
            .map_err(|e| HttpClientError::body(format!("Failed to serialize JSON: {e}")))?;

        Ok(Self {
            json_string,
            content_type: content_type.into(),
        })
    }

    /// Get the JSON string
    pub fn as_str(&self) -> &str {
        &self.json_string
    }

    /// Parse the JSON back to a deserializable type
    pub fn parse<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_str(&self.json_string)
            .map_err(|e| HttpClientError::body(format!("Failed to parse JSON: {e}")))
    }

    /// Validate that the JSON is well-formed
    pub fn validate(&self) -> Result<()> {
        serde_json::from_str::<serde_json::Value>(&self.json_string)
            .map_err(|e| HttpClientError::body(format!("Invalid JSON: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Body for JsonBody {
    fn content_type(&self) -> Option<&str> {
        Some(&self.content_type)
    }

    fn content_length(&self) -> Option<usize> {
        Some(self.json_string.len())
    }

    async fn to_bytes(&mut self) -> Result<Bytes> {
        Ok(Bytes::from(self.json_string.clone()))
    }

    fn try_clone(&self) -> Option<Box<dyn Body>> {
        Some(Box::new(self.clone()))
    }
}

/// Helper macro for creating JSON bodies
#[macro_export]
macro_rules! json_body {
    ($value:expr) => {
        $crate::body::JsonBody::new(&$value)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestData {
        name: String,
        age: u32,
        active: bool,
    }

    #[tokio::test]
    async fn test_json_body_creation() {
        let data = TestData {
            name: "Alice".to_string(),
            age: 30,
            active: true,
        };

        let mut body = JsonBody::new(&data).unwrap();

        assert_eq!(body.content_type(), Some("application/json; charset=utf-8"));
        assert!(body.content_length().unwrap() > 0);
        assert!(!body.is_empty());

        let bytes = body.to_bytes().await.unwrap();
        let json_str = String::from_utf8(bytes.to_vec()).unwrap();

        // Verify we can parse it back
        let parsed: TestData = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed, data);
    }

    #[tokio::test]
    async fn test_json_body_from_string() {
        let json_str = r#"{"name":"Bob","age":25,"active":false}"#;
        let mut body = JsonBody::from_string(json_str);

        assert_eq!(body.as_str(), json_str);
        assert_eq!(body.content_length(), Some(json_str.len()));

        let bytes = body.to_bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from(json_str));
    }

    #[tokio::test]
    async fn test_json_body_pretty() {
        let data = TestData {
            name: "Charlie".to_string(),
            age: 35,
            active: true,
        };

        let body = JsonBody::pretty(&data).unwrap();
        let json_str = body.as_str();

        // Pretty printed JSON should contain newlines
        assert!(json_str.contains('\n'));

        // Should still be valid JSON
        body.validate().unwrap();
    }

    #[tokio::test]
    async fn test_json_body_parse() {
        let data = TestData {
            name: "David".to_string(),
            age: 40,
            active: false,
        };

        let body = JsonBody::new(&data).unwrap();
        let parsed: TestData = body.parse().unwrap();

        assert_eq!(parsed, data);
    }

    #[tokio::test]
    async fn test_json_body_with_hashmap() {
        let mut map = HashMap::new();
        map.insert("key1", "value1");
        map.insert("key2", "value2");

        let body = JsonBody::new(&map).unwrap();

        assert!(body.content_length().unwrap() > 0);
        body.validate().unwrap();

        let parsed: HashMap<String, String> = body.parse().unwrap();
        assert_eq!(parsed.get("key1"), Some(&"value1".to_string()));
        assert_eq!(parsed.get("key2"), Some(&"value2".to_string()));
    }

    #[tokio::test]
    async fn test_json_body_custom_content_type() {
        let data = vec![1, 2, 3];
        let body = JsonBody::with_content_type(&data, "application/vnd.api+json").unwrap();

        assert_eq!(body.content_type(), Some("application/vnd.api+json"));
    }

    #[tokio::test]
    async fn test_json_body_validation() {
        let valid_body = JsonBody::from_string(r#"{"valid": true}"#);
        assert!(valid_body.validate().is_ok());

        let invalid_body = JsonBody::from_string(r#"{"invalid": json"#);
        assert!(invalid_body.validate().is_err());
    }

    #[tokio::test]
    async fn test_json_body_clone() {
        let data = TestData {
            name: "Eve".to_string(),
            age: 28,
            active: true,
        };

        let mut body = JsonBody::new(&data).unwrap();
        let mut cloned = body.try_clone().unwrap();

        let original_bytes = body.to_bytes().await.unwrap();
        let cloned_bytes = cloned.to_bytes().await.unwrap();

        assert_eq!(original_bytes, cloned_bytes);
    }

    #[test]
    fn test_json_body_macro() {
        let data = TestData {
            name: "Frank".to_string(),
            age: 45,
            active: true,
        };

        let body = json_body!(data).unwrap();
        assert!(body.content_length().unwrap() > 0);
    }
}
