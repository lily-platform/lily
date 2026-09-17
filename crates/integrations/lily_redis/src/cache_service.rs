use std::sync::{Arc, RwLock};

use async_trait::async_trait;
#[cfg(feature = "single")]
use lily_config::ConfigService;
#[cfg(feature = "single")]
use lily_error::injection::InjectionError;
#[cfg(feature = "single")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "single")]
use lily_injection::ServiceTrait;
use redis::AsyncCommands;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::cache_trait::ICache;
use crate::envelope;
use crate::pool::{RedisConnection, RedisPool};
use crate::{CacheError, CacheOperationContext, CacheScanPage, CacheScanRequest, RedisCachePlan};

struct RedisRuntime {
    pool: RedisPool,
    plan: RedisCachePlan,
    shutdown: CancellationToken,
    operation_slots: Arc<Semaphore>,
}

struct RedisLease {
    connection: RedisConnection,
    _permit: OwnedSemaphorePermit,
}

impl RedisRuntime {
    async fn connect(plan: RedisCachePlan) -> Result<Arc<Self>, CacheError> {
        let pool = plan.create_pool()?;
        let runtime = Arc::new(Self {
            pool,
            operation_slots: Arc::new(Semaphore::new(plan.pool_size())),
            shutdown: CancellationToken::new(),
            plan,
        });
        let readiness = CacheOperationContext::new(
            CancellationToken::new(),
            runtime.plan.connection_timeout(),
        )?;
        runtime.ping(&readiness).await?;
        Ok(runtime)
    }

    async fn lease(&self, operation: &CacheOperationContext) -> Result<RedisLease, CacheError> {
        operation
            .execute(&self.shutdown, async {
                let permit = Arc::clone(&self.operation_slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| CacheError::Disposed)?;
                let connection = self
                    .pool
                    .get()
                    .await
                    .map_err(|error| CacheError::Pool(error.to_string()))?;
                Ok(RedisLease {
                    connection,
                    _permit: permit,
                })
            })
            .await
    }

    async fn ping(&self, operation: &CacheOperationContext) -> Result<(), CacheError> {
        let mut lease = self.lease(operation).await?;
        let response: String = operation
            .execute(&self.shutdown, async {
                redis::cmd("PING")
                    .query_async(&mut *lease.connection)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await?;
        if response == "PONG" {
            Ok(())
        } else {
            Err(CacheError::Backend(
                "Redis returned an unexpected PING response".into(),
            ))
        }
    }

    async fn shutdown_and_drain(&self) -> Result<(), CacheError> {
        self.shutdown.cancel();
        let permits = u32::try_from(self.plan.pool_size()).map_err(|_| {
            CacheError::InvalidConfiguration("pool_size exceeds semaphore limits".into())
        })?;
        let drain = tokio::time::timeout(
            self.plan.operation_timeout(),
            self.operation_slots.acquire_many(permits),
        )
        .await
        .map_err(|_| CacheError::ShutdownTimedOut)?
        .map_err(|_| CacheError::Disposed)?;
        self.pool.close();
        drop(drain);
        Ok(())
    }
}

enum CacheState {
    Uninitialized,
    Ready(Arc<RedisRuntime>),
    Disposed,
}

#[cfg_attr(feature = "single", derive(Injectable))]
#[cfg_attr(feature = "single", service(lifetime = "Singleton"))]
#[derive(Clone)]
/// Redis cache service used either through Lily DI or standalone composition.
///
/// Import [`crate::ICache`] for the normal typed operations. In `single` mode,
/// DI initializes this service from `ConfigService`; callers must not also call
/// [`CacheService::connect`] for that container-owned instance.
pub struct CacheService {
    #[cfg(feature = "single")]
    #[inject]
    config_service: Arc<ConfigService>,
    state: Arc<RwLock<CacheState>>,
}

impl Default for CacheService {
    fn default() -> Self {
        Self {
            #[cfg(feature = "single")]
            config_service: Arc::new(ConfigService::default()),
            state: Arc::new(RwLock::new(CacheState::Uninitialized)),
        }
    }
}

impl CacheService {
    /// Connects a standalone cache service from a validated Redis plan.
    ///
    /// The caller owns the returned service and must eventually invoke
    /// [`crate::ICache::dispose`] or register a [`crate::CacheShutdownHandle`].
    pub async fn connect(plan: RedisCachePlan) -> Result<Self, CacheError> {
        let runtime = RedisRuntime::connect(plan).await?;
        let service = Self::default();
        service.install(runtime)?;
        Ok(service)
    }

    fn install(&self, runtime: Arc<RedisRuntime>) -> Result<(), CacheError> {
        let mut state = self.state.write().map_err(|_| CacheError::StatePoisoned)?;
        match &*state {
            CacheState::Uninitialized => {
                *state = CacheState::Ready(runtime);
                Ok(())
            }
            CacheState::Ready(_) => Err(CacheError::InvalidConfiguration(
                "cache service was initialized more than once".into(),
            )),
            CacheState::Disposed => Err(CacheError::Disposed),
        }
    }

    fn runtime(&self) -> Result<Arc<RedisRuntime>, CacheError> {
        match &*self.state.read().map_err(|_| CacheError::StatePoisoned)? {
            CacheState::Uninitialized => Err(CacheError::NotInitialized),
            CacheState::Ready(runtime) => Ok(Arc::clone(runtime)),
            CacheState::Disposed => Err(CacheError::Disposed),
        }
    }

    /// Creates an operation context using the configured timeout and caller cancellation.
    pub fn operation_context(
        &self,
        cancellation: CancellationToken,
    ) -> Result<CacheOperationContext, CacheError> {
        CacheOperationContext::new(cancellation, self.runtime()?.plan.operation_timeout())
    }

    fn default_operation(runtime: &RedisRuntime) -> Result<CacheOperationContext, CacheError> {
        CacheOperationContext::new(CancellationToken::new(), runtime.plan.operation_timeout())
    }

    async fn read_raw_with_context(
        &self,
        key: &str,
        operation: &CacheOperationContext,
    ) -> Result<Option<Vec<u8>>, CacheError> {
        let runtime = self.runtime()?;
        let key = runtime.plan.qualified_key(key)?;
        let mut lease = runtime.lease(operation).await?;
        operation
            .execute(&runtime.shutdown, async {
                redis::cmd("GET")
                    .arg(key)
                    .query_async(&mut *lease.connection)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await
    }

    async fn write_raw_with_context(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_seconds: u64,
        operation: &CacheOperationContext,
    ) -> Result<(), CacheError> {
        validate_ttl_seconds(ttl_seconds)?;
        let runtime = self.runtime()?;
        let key = runtime.plan.qualified_key(key)?;
        let mut lease = runtime.lease(operation).await?;
        operation
            .execute(&runtime.shutdown, async {
                redis::cmd("SETEX")
                    .arg(key)
                    .arg(ttl_seconds)
                    .arg(value)
                    .query_async::<()>(&mut *lease.connection)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await
    }

    async fn write_raw_if_absent_with_context(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_seconds: u64,
        operation: &CacheOperationContext,
    ) -> Result<bool, CacheError> {
        validate_ttl_seconds(ttl_seconds)?;
        let runtime = self.runtime()?;
        let key = runtime.plan.qualified_key(key)?;
        let mut lease = runtime.lease(operation).await?;
        let response: Option<String> = operation
            .execute(&runtime.shutdown, async {
                redis::cmd("SET")
                    .arg(key)
                    .arg(value)
                    .arg("NX")
                    .arg("EX")
                    .arg(ttl_seconds)
                    .query_async(&mut *lease.connection)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await?;

        match response.as_deref() {
            Some("OK") => Ok(true),
            None => Ok(false),
            Some(_) => Err(CacheError::Backend(
                "Redis returned an unexpected conditional SET response".into(),
            )),
        }
    }

    /// Serializes and stores JSON with caller-provided cancellation and deadline.
    pub async fn set_json_with_context<T: Serialize + Sync + ?Sized>(
        &self,
        key: &str,
        value: &T,
        operation: &CacheOperationContext,
    ) -> Result<(), CacheError> {
        let runtime = self.runtime()?;
        self.write_raw_with_context(
            key,
            envelope::encode_json(value)?,
            runtime.plan.default_ttl().as_secs(),
            operation,
        )
        .await
    }

    /// Stores binary data with caller-provided cancellation and deadline.
    pub async fn set_bytes_with_context(
        &self,
        key: &str,
        value: &[u8],
        operation: &CacheOperationContext,
    ) -> Result<(), CacheError> {
        let runtime = self.runtime()?;
        self.write_raw_with_context(
            key,
            envelope::encode_bytes(value),
            runtime.plan.default_ttl().as_secs(),
            operation,
        )
        .await
    }

    /// Atomically stores JSON text with an explicit TTL when the key is absent.
    ///
    /// Redis owns the existence check and write as one `SET NX EX` operation.
    /// A `false` result leaves the existing value and TTL unchanged.
    pub async fn set_json_text_if_absent_with_ttl_with_context(
        &self,
        key: &str,
        json: &str,
        ttl_seconds: u64,
        operation: &CacheOperationContext,
    ) -> Result<bool, CacheError> {
        self.write_raw_if_absent_with_context(
            key,
            envelope::encode_json_text(json)?,
            ttl_seconds,
            operation,
        )
        .await
    }

    /// Removes a key using the supplied operation context.
    pub async fn remove_with_context(
        &self,
        key: &str,
        operation: &CacheOperationContext,
    ) -> Result<bool, CacheError> {
        let runtime = self.runtime()?;
        let key = runtime.plan.qualified_key(key)?;
        let mut lease = runtime.lease(operation).await?;
        let removed: u64 = operation
            .execute(&runtime.shutdown, async {
                lease
                    .connection
                    .del(key)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await?;
        Ok(removed > 0)
    }

    /// Checks key existence using the supplied operation context.
    pub async fn exists_with_context(
        &self,
        key: &str,
        operation: &CacheOperationContext,
    ) -> Result<bool, CacheError> {
        let runtime = self.runtime()?;
        let key = runtime.plan.qualified_key(key)?;
        let mut lease = runtime.lease(operation).await?;
        operation
            .execute(&runtime.shutdown, async {
                lease
                    .connection
                    .exists(key)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await
    }

    /// Replaces a key's TTL using the supplied operation context.
    pub async fn expire_with_context(
        &self,
        key: &str,
        ttl_seconds: u64,
        operation: &CacheOperationContext,
    ) -> Result<bool, CacheError> {
        if ttl_seconds == 0 || ttl_seconds > 31_536_000 {
            return Err(CacheError::InvalidConfiguration(
                "ttl_seconds must be between 1 and 31536000".into(),
            ));
        }
        let runtime = self.runtime()?;
        let key = runtime.plan.qualified_key(key)?;
        let seconds = i64::try_from(ttl_seconds).map_err(|_| {
            CacheError::InvalidConfiguration("ttl_seconds exceeds Redis integer range".into())
        })?;
        let mut lease = runtime.lease(operation).await?;
        operation
            .execute(&runtime.shutdown, async {
                lease
                    .connection
                    .expire(key, seconds)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await
    }

    /// Reads a key's TTL state using the supplied operation context.
    pub async fn ttl_with_context(
        &self,
        key: &str,
        operation: &CacheOperationContext,
    ) -> Result<CacheTtl, CacheError> {
        let runtime = self.runtime()?;
        let key = runtime.plan.qualified_key(key)?;
        let mut lease = runtime.lease(operation).await?;
        let ttl: i64 = operation
            .execute(&runtime.shutdown, async {
                lease
                    .connection
                    .ttl(key)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await?;
        match ttl {
            -2 => Ok(CacheTtl::Missing),
            -1 => Ok(CacheTtl::Persistent),
            value if value >= 0 => {
                Ok(CacheTtl::ExpiresIn(u64::try_from(value).map_err(|_| {
                    CacheError::Backend("Redis returned an invalid TTL".into())
                })?))
            }
            _ => Err(CacheError::Backend("Redis returned an invalid TTL".into())),
        }
    }

    /// Reads one bounded, non-snapshot `SCAN` page using the supplied context.
    pub async fn scan_page_with_context(
        &self,
        request: CacheScanRequest,
        operation: &CacheOperationContext,
    ) -> Result<CacheScanPage, CacheError> {
        let runtime = self.runtime()?;
        if request.page_size() > runtime.plan.scan_page_size() {
            return Err(CacheError::InvalidScan(format!(
                "requested page_size exceeds configured scan_page_size {}",
                runtime.plan.scan_page_size()
            )));
        }
        let pattern = runtime.plan.qualified_pattern(request.pattern())?;
        let mut lease = runtime.lease(operation).await?;
        let (next_cursor, keys): (u64, Vec<String>) = operation
            .execute(&runtime.shutdown, async {
                redis::cmd("SCAN")
                    .arg(request.cursor())
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(request.page_size())
                    .query_async(&mut *lease.connection)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await?;
        if keys.len() > runtime.plan.max_scan_results() {
            return Err(CacheError::ScanLimitExceeded(
                runtime.plan.max_scan_results(),
            ));
        }
        let keys = keys
            .into_iter()
            .map(|key| runtime.plan.strip_namespace(key))
            .collect::<Result<_, _>>()?;
        Ok(CacheScanPage { keys, next_cursor })
    }

    /// Performs a Redis readiness check using the supplied operation context.
    pub async fn ping_with_context(
        &self,
        operation: &CacheOperationContext,
    ) -> Result<(), CacheError> {
        self.runtime()?.ping(operation).await
    }

    /// Atomically increments a Redis-backed fixed window and returns the
    /// resulting admission decision. Backend errors remain fail-closed typed
    /// errors; callers must not silently replace them with process-local state.
    pub async fn fixed_window_rate_limit_with_context(
        &self,
        key: &str,
        max_requests: u64,
        window: std::time::Duration,
        operation: &CacheOperationContext,
    ) -> Result<FixedWindowRateLimit, CacheError> {
        let window_ms = validate_fixed_window(max_requests, window)?;
        let runtime = self.runtime()?;
        let key = runtime.plan.qualified_key(key)?;
        let mut lease = runtime.lease(operation).await?;
        const SCRIPT: &str = r#"
local current = redis.call('INCR', KEYS[1])
local ttl = redis.call('PTTL', KEYS[1])
if current == 1 or ttl < 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  ttl = tonumber(ARGV[1])
end
return {current, ttl}
"#;
        let (count, ttl_ms): (u64, i64) = operation
            .execute(&runtime.shutdown, async {
                redis::cmd("EVAL")
                    .arg(SCRIPT)
                    .arg(1)
                    .arg(key)
                    .arg(window_ms)
                    .query_async(&mut *lease.connection)
                    .await
                    .map_err(|error| CacheError::Backend(error.to_string()))
            })
            .await?;
        let ttl_ms = u64::try_from(ttl_ms)
            .map_err(|_| CacheError::Backend("Redis returned an invalid rate-limit TTL".into()))?;
        Ok(FixedWindowRateLimit {
            allowed: count <= max_requests,
            count,
            remaining: max_requests.saturating_sub(count),
            retry_after: std::time::Duration::from_millis(ttl_ms.max(1)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// TTL state reported by Redis.
pub enum CacheTtl {
    /// The key does not exist.
    Missing,
    /// The key exists without an expiry.
    Persistent,
    /// The key expires after the reported number of seconds.
    ExpiresIn(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of one atomic fixed-window rate-limit increment.
pub struct FixedWindowRateLimit {
    allowed: bool,
    count: u64,
    remaining: u64,
    retry_after: std::time::Duration,
}

impl FixedWindowRateLimit {
    /// Returns whether the increment remains within the configured limit.
    pub const fn allowed(self) -> bool {
        self.allowed
    }

    /// Returns the counter value after this request was recorded.
    pub const fn count(self) -> u64 {
        self.count
    }

    /// Returns the remaining requests in the current window.
    pub const fn remaining(self) -> u64 {
        self.remaining
    }

    /// Returns the remaining lifetime of the current fixed window.
    pub const fn retry_after(self) -> std::time::Duration {
        self.retry_after
    }
}

#[async_trait]
impl ICache for CacheService {
    async fn get_json_text(&self, key: &str) -> Result<Option<String>, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.read_raw_with_context(key, &operation)
            .await?
            .map(|value| envelope::decode_json_text(&value))
            .transpose()
    }

    async fn get<T: DeserializeOwned + Send + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.get_with_context(key, &operation).await
    }

    async fn get_with_context<T: DeserializeOwned + Send + 'static>(
        &self,
        key: &str,
        operation: &CacheOperationContext,
    ) -> Result<Option<T>, CacheError> {
        self.read_raw_with_context(key, operation)
            .await?
            .map(|value| envelope::decode_json(&value))
            .transpose()
    }

    async fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.read_raw_with_context(key, &operation)
            .await?
            .map(|value| envelope::decode_bytes(&value))
            .transpose()
    }

    async fn set_json_text(&self, key: &str, json: &str) -> Result<(), CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.write_raw_with_context(
            key,
            envelope::encode_json_text(json)?,
            runtime.plan.default_ttl().as_secs(),
            &operation,
        )
        .await
    }

    async fn set_json<T: Serialize + Sync + ?Sized>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.set_json_with_context(key, value, &operation).await
    }

    async fn set_bytes(&self, key: &str, value: &[u8]) -> Result<(), CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.set_bytes_with_context(key, value, &operation).await
    }

    async fn set_json_text_with_ttl(
        &self,
        key: &str,
        json: &str,
        ttl_seconds: u64,
    ) -> Result<(), CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.write_raw_with_context(
            key,
            envelope::encode_json_text(json)?,
            ttl_seconds,
            &operation,
        )
        .await
    }

    async fn set_json_text_if_absent_with_ttl(
        &self,
        key: &str,
        json: &str,
        ttl_seconds: u64,
    ) -> Result<bool, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.set_json_text_if_absent_with_ttl_with_context(key, json, ttl_seconds, &operation)
            .await
    }

    async fn remove(&self, key: &str) -> Result<bool, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.remove_with_context(key, &operation).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.exists_with_context(key, &operation).await
    }

    async fn expire(&self, key: &str, ttl_seconds: u64) -> Result<bool, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.expire_with_context(key, ttl_seconds, &operation).await
    }

    async fn ttl(&self, key: &str) -> Result<CacheTtl, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.ttl_with_context(key, &operation).await
    }

    async fn scan_page(&self, request: CacheScanRequest) -> Result<CacheScanPage, CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.scan_page_with_context(request, &operation).await
    }

    async fn fixed_window_rate_limit(
        &self,
        key: &str,
        max_requests: u64,
        window: std::time::Duration,
    ) -> Result<FixedWindowRateLimit, CacheError> {
        validate_fixed_window(max_requests, window)?;
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        self.fixed_window_rate_limit_with_context(key, max_requests, window, &operation)
            .await
    }

    async fn ping(&self) -> Result<(), CacheError> {
        let runtime = self.runtime()?;
        let operation = Self::default_operation(&runtime)?;
        runtime.ping(&operation).await
    }

    async fn dispose(&self) -> Result<(), CacheError> {
        let runtime = {
            let mut state = self.state.write().map_err(|_| CacheError::StatePoisoned)?;
            match std::mem::replace(&mut *state, CacheState::Disposed) {
                CacheState::Uninitialized | CacheState::Disposed => None,
                CacheState::Ready(runtime) => Some(runtime),
            }
        };
        if let Some(runtime) = runtime {
            runtime.shutdown_and_drain().await?;
        }
        Ok(())
    }
}

fn validate_fixed_window(
    max_requests: u64,
    window: std::time::Duration,
) -> Result<u64, CacheError> {
    if max_requests == 0 || max_requests > 1_000_000_000 {
        return Err(CacheError::InvalidConfiguration(
            "max_requests must be in 1..=1000000000".into(),
        ));
    }
    let window_ms = u64::try_from(window.as_millis())
        .map_err(|_| CacheError::InvalidConfiguration("rate-limit window is too large".into()))?;
    if !(1_000..=86_400_000).contains(&window_ms) {
        return Err(CacheError::InvalidConfiguration(
            "rate-limit window must be between one second and one day".into(),
        ));
    }
    Ok(window_ms)
}

fn validate_ttl_seconds(ttl_seconds: u64) -> Result<(), CacheError> {
    if ttl_seconds == 0 || ttl_seconds > 31_536_000 {
        return Err(CacheError::InvalidConfiguration(
            "ttl_seconds must be between 1 and 31536000".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "single")]
#[async_trait]
impl ServiceTrait for CacheService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        let config = self.config_service.get_lily_config().await;
        let cache = config
            .cache
            .as_ref()
            .ok_or_else(|| InjectionError::InitError("[cache] configuration is required".into()))?;
        let plan = RedisCachePlan::from_single(cache)
            .map_err(|error| InjectionError::InitError(error.to_string()))?;
        let runtime = RedisRuntime::connect(plan)
            .await
            .map_err(|error| InjectionError::InitError(error.to_string()))?;
        self.install(runtime)
            .map_err(|error| InjectionError::InitError(error.to_string()))
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        ICache::dispose(self)
            .await
            .map_err(|error| InjectionError::General(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pre_initialization_and_post_dispose_are_typed() {
        let service = CacheService::default();
        assert_eq!(
            service.get::<String>("key").await,
            Err(CacheError::NotInitialized)
        );
        ICache::dispose(&service).await.unwrap();
        assert_eq!(
            service.get::<String>("key").await,
            Err(CacheError::Disposed)
        );
        assert!(ICache::dispose(&service).await.is_ok());
    }

    #[tokio::test]
    async fn fixed_window_rate_limit_validates_bounds_before_backend_use() {
        let service = CacheService::default();
        assert!(matches!(
            service
                .fixed_window_rate_limit("key", 0, std::time::Duration::from_secs(60))
                .await,
            Err(CacheError::InvalidConfiguration(_))
        ));
    }

    #[tokio::test]
    async fn conditional_insert_validates_ttl_before_backend_use() {
        let service = CacheService::default();
        let operation =
            CacheOperationContext::new(CancellationToken::new(), std::time::Duration::from_secs(1))
                .unwrap();

        for ttl_seconds in [0, 31_536_001] {
            assert!(matches!(
                service
                    .set_json_text_if_absent_with_ttl_with_context(
                        "key",
                        r#"{"value":1}"#,
                        ttl_seconds,
                        &operation,
                    )
                    .await,
                Err(CacheError::InvalidConfiguration(_))
            ));
        }
    }
}
