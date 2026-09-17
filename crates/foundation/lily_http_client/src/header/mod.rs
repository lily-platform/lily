//! Header management module
//!
//! This module provides comprehensive HTTP header handling functionality:
//! - HeaderMap for storing and managing headers with case-insensitive access
//! - Common header constants and utilities
//! - Header parsing and formatting for HTTP messages
//! - Header builder for fluent API construction
//!
//! ## Example
//!
//! ```rust,no_run
//! use lily_http_client::header::{HeaderBuilder, HeaderMap};
//!
//! // Using the builder pattern
//! let headers = HeaderBuilder::new()
//!     .json().unwrap()
//!     .user_agent("my-client/1.0").unwrap()
//!     .bearer_auth("token123").unwrap()
//!     .build();
//!
//! // Direct HeaderMap usage
//! let mut headers = HeaderMap::new();
//! headers.insert("Content-Type", "application/json").unwrap();
//! ```

// Core header functionality
mod builder;
pub mod common;
mod map;
mod parser;

// Re-exports
pub use builder::HeaderBuilder;
pub use common::HeaderValue;
pub use map::HeaderMap;
pub use parser::HeaderParser;
