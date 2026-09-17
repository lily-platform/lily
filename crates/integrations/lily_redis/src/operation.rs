use std::{future::Future, time::Duration};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::CacheError;

pub(crate) const MAX_SCAN_PAGE_SIZE: usize = 1_000;

#[derive(Clone)]
/// Caller cancellation and absolute deadline applied to one cache operation.
pub struct CacheOperationContext {
    cancellation: CancellationToken,
    deadline: Instant,
}

impl CacheOperationContext {
    /// Creates a context with a deadline measured from the current instant.
    pub fn new(cancellation: CancellationToken, timeout: Duration) -> Result<Self, CacheError> {
        if timeout.is_zero() {
            return Err(CacheError::InvalidConfiguration(
                "operation timeout must be greater than zero".into(),
            ));
        }
        Ok(Self {
            cancellation,
            deadline: Instant::now() + timeout,
        })
    }

    pub(crate) async fn execute<T, F>(
        &self,
        shutdown: &CancellationToken,
        future: F,
    ) -> Result<T, CacheError>
    where
        F: Future<Output = Result<T, CacheError>>,
    {
        tokio::pin!(future);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => Err(CacheError::Cancelled),
            _ = self.cancellation.cancelled() => Err(CacheError::Cancelled),
            _ = tokio::time::sleep_until(self.deadline) => Err(CacheError::TimedOut),
            result = &mut future => result,
        }
    }
}

impl std::fmt::Debug for CacheOperationContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CacheOperationContext")
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("deadline", &"<monotonic>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Validated request for one Redis `SCAN` page.
pub struct CacheScanRequest {
    cursor: u64,
    pattern: String,
    page_size: usize,
}

impl CacheScanRequest {
    /// Validates a cursor, glob pattern and bounded page size.
    pub fn new(
        cursor: u64,
        pattern: impl Into<String>,
        page_size: usize,
    ) -> Result<Self, CacheError> {
        let pattern = pattern.into();
        if pattern.is_empty() || pattern.len() > 256 || pattern.contains('\0') {
            return Err(CacheError::InvalidScan(
                "pattern must contain 1..=256 non-NUL bytes".into(),
            ));
        }
        if page_size == 0 || page_size > MAX_SCAN_PAGE_SIZE {
            return Err(CacheError::InvalidScan(format!(
                "page_size must be between 1 and {MAX_SCAN_PAGE_SIZE}"
            )));
        }
        Ok(Self {
            cursor,
            pattern,
            page_size,
        })
    }

    /// Returns the Redis cursor from which this page starts.
    pub const fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Returns the namespace-relative Redis glob pattern.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Returns the requested maximum number of keys for this page.
    pub const fn page_size(&self) -> usize {
        self.page_size
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One namespace-relative page returned by Redis `SCAN`.
pub struct CacheScanPage {
    /// Keys returned by this page, with Lily's configured namespace removed.
    pub keys: Vec<String>,
    /// Cursor for the next request; zero means the scan cycle is complete.
    pub next_cursor: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_requests_are_bounded() {
        assert!(CacheScanRequest::new(0, "*", 1).is_ok());
        assert!(CacheScanRequest::new(0, "", 1).is_err());
        assert!(CacheScanRequest::new(0, "*", 0).is_err());
        assert!(CacheScanRequest::new(0, "*", MAX_SCAN_PAGE_SIZE + 1).is_err());
    }

    #[tokio::test]
    async fn cancellation_is_typed() {
        let caller = CancellationToken::new();
        caller.cancel();
        let operation = CacheOperationContext::new(caller, Duration::from_secs(1)).unwrap();
        let result = operation
            .execute(&CancellationToken::new(), async {
                std::future::pending::<()>().await;
                Ok(())
            })
            .await;
        assert_eq!(result, Err(CacheError::Cancelled));
    }
}
