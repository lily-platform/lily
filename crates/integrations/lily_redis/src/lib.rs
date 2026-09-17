#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Redis-backed cache integration for Lily applications.
//!
//! The crate deliberately supports Redis only. It owns connection pooling,
//! namespaced keys, bounded deadlines, cancellation, typed JSON/binary
//! envelopes and graceful shutdown. It does not expose a generic provider or
//! failover abstraction.
//!
//! # Application use
//!
//! With the default `single` feature, [`CacheService`] is an injectable
//! singleton initialized from Lily's validated cache configuration. Inject an
//! `Arc<CacheService>` into a domain service and import [`ICache`] to call the
//! normal cache operations:
//!
//! ```ignore
//! use std::sync::Arc;
//! use lily_redis::{CacheService, ICache};
//! use lily_injectable_derive::Injectable;
//! use lily_injection::ServiceTrait;
//!
//! #[derive(Injectable, Default)]
//! #[service(lifetime = "Singleton")]
//! struct SessionCache {
//!     #[inject]
//!     cache: Arc<CacheService>,
//! }
//!
//! impl ServiceTrait for SessionCache {}
//!
//! impl SessionCache {
//!     async fn load(&self, key: &str) -> Result<Option<Session>, lily_redis::CacheError> {
//!         self.cache.get(key).await
//!     }
//! }
//! ```
//!
//! [`ICache::get`] and [`ICache::set_json`] are the canonical typed JSON
//! operations. [`ICache::get_bytes`] and [`ICache::set_bytes`] are the binary
//! pair. JSON and binary entries carry distinct versioned envelopes; reading
//! an entry with the wrong operation fails with [`CacheError::IncompatiblePayload`]
//! rather than guessing its representation.
//! [`ICache::set_json_text_if_absent_with_ttl`] provides a Redis-wide atomic
//! conditional insert for coordination contracts such as synchronizer tokens.
//!
//! Use [`CacheService::operation_context`] and the explicit `*_with_context`
//! methods when request cancellation must propagate to Redis. The simpler
//! trait methods still use the configured operation deadline, but create a
//! detached cancellation token.
//!
//! # Named cells
//!
//! Applications requiring named Redis cells disable default features and
//! enable `factory`. They inject `CacheFactory` and select a configured cell
//! with `CacheFactory::get`. `single` and `factory` are mutually exclusive.
//!
//! ```toml
//! lily_redis = { version = "0.1", default-features = false, features = ["factory"] }
//! ```
//!
//! # Standalone ownership
//!
//! [`CacheService::connect`] and, in factory mode, `CacheFactory::connect`
//! support composition outside Lily DI. A standalone owner must also close the
//! resource through [`ICache::dispose`] or `CacheFactory::dispose_all`.
//! [`CacheShutdownHandle`] adapts either owner to Lily's shutdown coordinator.
//!
//! Cache keys are qualified with the configured namespace. [`ICache::scan_page`]
//! uses Redis `SCAN`: it is bounded and non-blocking, but it is not a stable
//! snapshot while keys are changing.

#[cfg(all(feature = "single", feature = "factory"))]
compile_error!(
    "lily_redis features `single` and `factory` are mutually exclusive; select exactly one cache composition mode"
);

mod envelope;
mod error;
mod lifecycle;
mod operation;
mod options;
mod pool;

mod cache_service;
mod cache_trait;

// Factory module - only available with factory feature
#[cfg(feature = "factory")]
mod cache_factory;

pub use cache_service::{CacheTtl, FixedWindowRateLimit};
pub use cache_trait::ICache;
pub use error::CacheError;
pub use lifecycle::CacheShutdownHandle;
pub use operation::{CacheOperationContext, CacheScanPage, CacheScanRequest};
pub use options::RedisCachePlan;

// Single mode exports (default)
#[cfg(feature = "single")]
pub use cache_service::CacheService;

// Factory mode exports
#[cfg(feature = "factory")]
pub use cache_factory::CacheFactory;
#[cfg(all(feature = "factory", not(feature = "single")))]
pub use cache_service::CacheService;
