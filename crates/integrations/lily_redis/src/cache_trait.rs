use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    CacheError, CacheOperationContext, CacheScanPage, CacheScanRequest, CacheTtl,
    FixedWindowRateLimit,
};

/// Redis-only V1 cache contract.
///
/// A cache miss is `Ok(None)`. Configuration, lifecycle, pool, transport and
/// serialization failures always remain typed errors.
#[async_trait]
pub trait ICache: Send + Sync {
    /// Reads a JSON-text entry without deserializing it into an application type.
    async fn get_json_text(&self, key: &str) -> Result<Option<String>, CacheError>;

    /// Reads and deserializes a typed JSON entry, returning `None` on a miss.
    async fn get<T: DeserializeOwned + Send + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>, CacheError>;

    /// Reads typed JSON with a caller-provided deadline and cancellation context.
    async fn get_with_context<T: DeserializeOwned + Send + 'static>(
        &self,
        key: &str,
        operation: &CacheOperationContext,
    ) -> Result<Option<T>, CacheError>;

    /// Reads a binary entry, returning `None` on a miss.
    async fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;

    /// Stores JSON text using the configured default TTL.
    ///
    /// Valid JSON remains JSON. Other text is stored as a JSON string. Prefer
    /// [`ICache::set_json`] for typed application data.
    async fn set_json_text(&self, key: &str, json: &str) -> Result<(), CacheError>;

    /// Serializes and stores a typed JSON value using the configured default TTL.
    async fn set_json<T: Serialize + Sync + ?Sized>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), CacheError>;

    /// Stores binary data using the configured default TTL.
    async fn set_bytes(&self, key: &str, value: &[u8]) -> Result<(), CacheError>;

    /// Stores JSON text with an explicit TTL in seconds.
    async fn set_json_text_with_ttl(
        &self,
        key: &str,
        json: &str,
        ttl_seconds: u64,
    ) -> Result<(), CacheError>;

    /// Atomically stores JSON text with an explicit TTL only when the key is absent.
    ///
    /// Returns `true` when this call inserted the value. A `false` result means
    /// the key already existed; its value and TTL remain unchanged.
    async fn set_json_text_if_absent_with_ttl(
        &self,
        key: &str,
        json: &str,
        ttl_seconds: u64,
    ) -> Result<bool, CacheError>;

    /// Removes a key and reports whether it existed.
    async fn remove(&self, key: &str) -> Result<bool, CacheError>;

    /// Reports whether a key exists.
    async fn exists(&self, key: &str) -> Result<bool, CacheError>;

    /// Replaces a key's TTL and reports whether the key existed.
    async fn expire(&self, key: &str, ttl_seconds: u64) -> Result<bool, CacheError>;

    /// Reads the Redis TTL state of a key.
    async fn ttl(&self, key: &str) -> Result<CacheTtl, CacheError>;

    /// Reads one bounded Redis `SCAN` page within the configured namespace.
    async fn scan_page(&self, request: CacheScanRequest) -> Result<CacheScanPage, CacheError>;

    /// Applies an atomic Redis-backed fixed-window admission decision.
    async fn fixed_window_rate_limit(
        &self,
        key: &str,
        max_requests: u64,
        window: std::time::Duration,
    ) -> Result<FixedWindowRateLimit, CacheError>;

    /// Verifies that the configured Redis endpoint is reachable.
    async fn ping(&self) -> Result<(), CacheError>;

    /// Cancels new work, drains in-flight operations and closes the pool.
    ///
    /// Lily DI invokes this automatically. Call it directly only when the
    /// service was created with [`crate::CacheService::connect`].
    async fn dispose(&self) -> Result<(), CacheError>;
}
