#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
/// Fail-closed errors returned by Redis configuration, operations and lifecycle.
pub enum CacheError {
    /// Configuration failed validation before a usable runtime was published.
    #[error("invalid cache configuration: {0}")]
    InvalidConfiguration(String),
    /// The DI or standalone service has not completed initialization.
    #[error("cache service is not initialized")]
    NotInitialized,
    /// The service no longer accepts work because shutdown completed or began.
    #[error("cache service has been disposed")]
    Disposed,
    /// A caller-provided key violates Lily's key boundary.
    #[error("invalid cache key: {0}")]
    InvalidKey(String),
    /// A `SCAN` request violates its pattern, cursor or page boundary.
    #[error("invalid cache scan request: {0}")]
    InvalidScan(String),
    /// Redis returned more keys than the configured defensive limit.
    #[error("cache scan response exceeded the configured bound of {0} keys")]
    ScanLimitExceeded(usize),
    /// JSON serialization or deserialization failed.
    #[error("cache payload serialization failed: {0}")]
    Serialization(String),
    /// A stored entry has an unknown envelope version or the wrong data kind.
    #[error("cache payload is incompatible: {0}")]
    IncompatiblePayload(String),
    /// A pooled connection could not be acquired or recycled.
    #[error("Redis pool is unavailable: {0}")]
    Pool(String),
    /// Redis rejected or failed an operation.
    #[error("Redis operation failed: {0}")]
    Backend(String),
    /// The configured operation deadline elapsed.
    #[error("cache operation timed out")]
    TimedOut,
    /// Caller or service shutdown cancellation won the operation race.
    #[error("cache operation was cancelled")]
    Cancelled,
    /// Graceful shutdown could not drain in-flight operations in time.
    #[error("cache shutdown timed out while draining in-flight operations")]
    ShutdownTimedOut,
    /// Factory initialization failed and cleanup of already-opened cells also failed.
    #[error("cache factory initialization failed and rollback also failed: {cleanup}")]
    InitializationRollbackFailed {
        /// Original cell initialization error.
        #[source]
        initialization: Box<CacheError>,
        /// Redacted summary of cleanup failures.
        cleanup: String,
    },
    /// An internal lifecycle lock was poisoned.
    #[error("cache state lock is poisoned")]
    StatePoisoned,
}
