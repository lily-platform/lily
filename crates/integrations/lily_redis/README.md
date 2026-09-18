# lily_redis

Redis-backed caching for Lilyrs applications. The crate owns pooling, namespaced
keys, bounded operations, cancellation, typed payload envelopes and shutdown.
It is a Redis integration rather than a generic cache-provider abstraction.

```toml
[dependencies]
lily_redis = "0.1.0"
```

The umbrella path is `lilyrs::redis` with feature `redis`. With the default
`single` feature, inject `Arc<CacheService>` into a service and import `ICache`
for the cache operations. Initialization uses validated `[cache]` configuration.

```rust
use lily_redis::{CacheError, CacheService, ICache};

async fn store_label(cache: &CacheService) -> Result<Option<String>, CacheError> {
    cache.set_json("greeting", "hello").await?;
    cache.get::<String>("greeting").await
}
```

`set_json`/`get` store typed JSON; `set_bytes`/`get_bytes` store binary values.
The versioned envelopes distinguish the two formats and return
`CacheError::IncompatiblePayload` for a mismatched read. Ordinary writes use the
configured default TTL. Explicit TTL and conditional-insert methods are also
available, including `set_json_text_if_absent_with_ttl` for atomic insertion.

Convenience methods have a configured deadline but detached cancellation. Use
`CacheService::operation_context` and `*_with_context` methods to propagate an
execution signal. `scan_page` uses bounded Redis SCAN and does not promise a
stable snapshot while keys change.

## Composition modes

Named cells use `CacheFactory::get`:

```toml
lily_redis = { version = "0.1.0", default-features = false, features = ["factory"] }
```

`single` and `factory` are mutually exclusive. For ownership outside Lily DI,
`CacheService::connect` accepts a `RedisCachePlan`; the owner must await
`ICache::dispose`. Factory owners use `dispose_all`. `CacheShutdownHandle`
integrates the resource with Lily's shutdown coordinator.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_redis](https://docs.rs/lily_redis).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
