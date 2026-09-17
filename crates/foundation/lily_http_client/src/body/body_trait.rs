use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;

/// Trait for HTTP request/response bodies
///
/// This trait provides the bounded-buffered representation consumed by the
/// v1 transport. A request body must report its exact buffered length, be
/// repeatable, and materialize no more than the configured request-body limit.
/// Streaming and reader-backed request bodies are intentionally not part of
/// this API.
#[async_trait]
pub trait Body: Send + Sync + fmt::Debug {
    /// Get the content type of this body
    fn content_type(&self) -> Option<&str>;

    /// Get the content length if known
    fn content_length(&self) -> Option<usize>;

    /// Materialize the buffered body bytes.
    async fn to_bytes(&mut self) -> Result<Bytes>;

    /// Check if the body is empty
    fn is_empty(&self) -> bool {
        self.content_length() == Some(0)
    }

    /// Check if the body is repeatable (can be read multiple times).
    ///
    /// The v1 request transport accepts only repeatable bodies.
    fn is_repeatable(&self) -> bool {
        true // Most bodies are repeatable by default
    }

    /// Clone the body if possible
    fn try_clone(&self) -> Option<Box<dyn Body>> {
        None // Default implementation returns None
    }
}

/// Empty body implementation
#[derive(Debug, Clone, Default)]
pub struct EmptyBody;

#[async_trait]
impl Body for EmptyBody {
    fn content_type(&self) -> Option<&str> {
        None
    }

    fn content_length(&self) -> Option<usize> {
        Some(0)
    }

    async fn to_bytes(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn try_clone(&self) -> Option<Box<dyn Body>> {
        Some(Box::new(EmptyBody))
    }
}

/// Text body implementation
#[derive(Clone)]
pub struct TextBody {
    content: String,
    content_type: String,
}

impl fmt::Debug for TextBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TextBody")
            .field("content_length", &self.content.len())
            .finish()
    }
}

impl TextBody {
    /// Create a new text body
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            content_type: "text/plain; charset=utf-8".to_string(),
        }
    }

    /// Create a text body with custom content type
    pub fn with_content_type(content: impl Into<String>, content_type: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            content_type: content_type.into(),
        }
    }

    /// Get the text content
    pub fn as_str(&self) -> &str {
        &self.content
    }
}

#[async_trait]
impl Body for TextBody {
    fn content_type(&self) -> Option<&str> {
        Some(&self.content_type)
    }

    fn content_length(&self) -> Option<usize> {
        Some(self.content.len())
    }

    async fn to_bytes(&mut self) -> Result<Bytes> {
        Ok(Bytes::from(self.content.clone()))
    }

    fn try_clone(&self) -> Option<Box<dyn Body>> {
        Some(Box::new(self.clone()))
    }
}

/// Binary body implementation
#[derive(Clone)]
pub struct BinaryBody {
    data: Bytes,
    content_type: String,
}

impl fmt::Debug for BinaryBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinaryBody")
            .field("content_length", &self.data.len())
            .finish()
    }
}

impl BinaryBody {
    /// Create a new binary body
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self {
            data: data.into(),
            content_type: "application/octet-stream".to_string(),
        }
    }

    /// Create a binary body with custom content type
    pub fn with_content_type(data: impl Into<Bytes>, content_type: impl Into<String>) -> Self {
        Self {
            data: data.into(),
            content_type: content_type.into(),
        }
    }

    /// Get the binary data
    pub fn as_bytes(&self) -> &Bytes {
        &self.data
    }
}

#[async_trait]
impl Body for BinaryBody {
    fn content_type(&self) -> Option<&str> {
        Some(&self.content_type)
    }

    fn content_length(&self) -> Option<usize> {
        Some(self.data.len())
    }

    async fn to_bytes(&mut self) -> Result<Bytes> {
        Ok(self.data.clone())
    }

    fn try_clone(&self) -> Option<Box<dyn Body>> {
        Some(Box::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_empty_body() {
        let mut body = EmptyBody;

        assert_eq!(body.content_type(), None);
        assert_eq!(body.content_length(), Some(0));
        assert!(body.is_empty());
        assert!(body.is_repeatable());

        let bytes = body.to_bytes().await.unwrap();
        assert!(bytes.is_empty());

        assert!(body.try_clone().is_some());
    }

    #[tokio::test]
    async fn test_text_body() {
        let mut body = TextBody::new("Hello, World!");

        assert_eq!(body.content_type(), Some("text/plain; charset=utf-8"));
        assert_eq!(body.content_length(), Some(13));
        assert!(!body.is_empty());
        assert_eq!(body.as_str(), "Hello, World!");

        let bytes = body.to_bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from("Hello, World!"));

        assert!(body.try_clone().is_some());
    }

    #[tokio::test]
    async fn test_text_body_with_content_type() {
        let body = TextBody::with_content_type("Hello", "text/html");

        assert_eq!(body.content_type(), Some("text/html"));
        assert_eq!(body.content_length(), Some(5));
    }

    #[tokio::test]
    async fn test_binary_body() {
        let data = vec![1, 2, 3, 4, 5];
        let mut body = BinaryBody::new(data.clone());

        assert_eq!(body.content_type(), Some("application/octet-stream"));
        assert_eq!(body.content_length(), Some(5));
        assert!(!body.is_empty());

        let bytes = body.to_bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from(data));

        assert!(body.try_clone().is_some());
    }

    #[tokio::test]
    async fn test_binary_body_with_content_type() {
        let data = b"image data";
        let body = BinaryBody::with_content_type(data.as_ref(), "image/png");

        assert_eq!(body.content_type(), Some("image/png"));
        assert_eq!(body.content_length(), Some(10));
    }
}
