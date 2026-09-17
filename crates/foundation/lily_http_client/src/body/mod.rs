//! HTTP Body module
//!
//! This module provides the request and response body's bounded-buffered
//! representation. Every body offered by this module has a known byte length,
//! is repeatable, and can be materialized before transport. Hyper owns HTTP
//! wire framing; this module does not expose streaming or transfer-encoding
//! body types.
//!
//! ## Example
//!
//! ```rust,no_run
//! use lily_http_client::body::{FormBody, JsonBody, MultipartBody, TextBody};
//! use serde_json::json;
//!
//! // JSON body
//! let json_body = JsonBody::new(&json!({"name": "Alice"})).unwrap();
//!
//! // Form body
//! let form_body = FormBody::new()
//!     .field("name", "Bob")
//!     .field("email", "bob@example.com");
//!
//! // Text body
//! let text_body = TextBody::new("Hello, World!");
//!
//! // Multipart body
//! let multipart_body = MultipartBody::new()
//!     .text_field("description", "File upload")
//!     .unwrap()
//!     .file_field(
//!         "upload",
//!         "file.txt",
//!         "text/plain",
//!         bytes::Bytes::from_static(b"file content"),
//!     )
//!     .unwrap();
//! ```

// Core body functionality
mod body_trait;
mod form;
mod json;

// Explicit production exports. Keep this list aligned with the buffered body
// contract above so adding a module cannot accidentally create public API.
pub use body_trait::{BinaryBody, Body, EmptyBody, TextBody};
pub use form::{FormBody, MultipartBody};
pub use json::JsonBody;
pub use lily_web_core::FormData;

// Convenience type aliases
pub type BoxBody = Box<dyn Body>;

/// Builder facade for the supported buffered body types.
#[derive(Debug, Default)]
pub struct BodyBuilder;

impl BodyBuilder {
    /// Create a new body builder
    pub fn new() -> Self {
        Self
    }

    /// Create an empty body
    pub fn empty() -> EmptyBody {
        EmptyBody
    }

    /// Create a text body
    pub fn text(content: impl Into<String>) -> TextBody {
        TextBody::new(content)
    }

    /// Create a JSON body
    pub fn json<T: serde::Serialize>(value: &T) -> crate::error::Result<JsonBody> {
        JsonBody::new(value)
    }

    /// Create a form body
    pub fn form() -> FormBody {
        FormBody::new()
    }

    /// Create a multipart body
    pub fn multipart() -> MultipartBody {
        MultipartBody::new()
    }

    /// Create a binary body
    pub fn binary(data: impl Into<bytes::Bytes>) -> BinaryBody {
        BinaryBody::new(data)
    }
}
