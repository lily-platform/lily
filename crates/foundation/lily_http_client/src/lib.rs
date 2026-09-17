//! # Lily HTTP Client
//!
//! A bounded-buffered, async HTTP client library built with Rust.
//!
//! ## Features
//!
//! - Async/await support with Tokio
//! - TLS/SSL support with RustTLS
//! - Modular architecture with separate concerns
//! - Type-safe HTTP methods and status codes
//! - Bounded, repeatable request bodies (JSON, form, text, and binary)
//! - Comprehensive error handling
//!
//! ## Example
//!
//! ```rust,no_run
//! use lily_http_client::HttpClient;
//!
//! #[tokio::main]
//! async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
//!     let client = HttpClient::new();
//!     let builder = client.get("https://httpbin.org/get")?;
//!     let request = builder.build()?;
//!     let response = client.execute(request).await?;
//!     println!("Response: {:?}", response);
//!     Ok(())
//! }
//! ```

// Core modules
pub mod error;

// HTTP components
pub mod body;
pub mod client;
pub mod factory;
pub mod header;
pub mod request;
pub mod response;

// Re-exports for convenience
pub use body::Body;
pub use client::{ClientConfig, HttpClient, HttpClientBuilder, ProtocolPreference};
pub use error::{HttpClientError, MultipartBuildError, MultipartMetadataField, Result};
pub use factory::LilyHttpClientFactory;
pub use header::HeaderMap;
pub use request::{Method, Request, RequestBuilder};
pub use response::{Response, StatusCode};
