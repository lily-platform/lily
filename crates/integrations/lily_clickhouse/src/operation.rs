use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::ClickhouseError;

pub(crate) const MAX_CLICKHOUSE_PAGE_SIZE: u64 = 1_000;
pub(crate) const MAX_CLICKHOUSE_WRITE_BATCH_SIZE: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Validated `LIMIT` and `OFFSET` pair for ClickHouse reads.
pub struct ClickhousePageRequest {
    limit: u64,
    offset: u64,
}

impl ClickhousePageRequest {
    /// Creates a page request with `limit` in `1..=1000`.
    pub fn new(limit: u64, offset: u64) -> Result<Self, ClickhouseError> {
        if limit == 0 || limit > MAX_CLICKHOUSE_PAGE_SIZE {
            return Err(ClickhouseError::InvalidPage(format!(
                "limit must be between 1 and {MAX_CLICKHOUSE_PAGE_SIZE}"
            )));
        }
        Ok(Self { limit, offset })
    }

    /// Returns the maximum rows requested.
    pub const fn limit(self) -> u64 {
        self.limit
    }

    /// Returns the number of rows skipped before this page.
    pub const fn offset(self) -> u64 {
        self.offset
    }
}

#[derive(Clone)]
/// Caller cancellation and absolute deadline applied to one ClickHouse operation.
pub struct ClickhouseOperationContext {
    cancellation: CancellationToken,
    deadline: Instant,
}

impl ClickhouseOperationContext {
    /// Creates a context with a deadline measured from the current instant.
    pub fn new(
        cancellation: CancellationToken,
        timeout: Duration,
    ) -> Result<Self, ClickhouseError> {
        if timeout.is_zero() {
            return Err(ClickhouseError::InvalidConfiguration(
                "operation timeout must be greater than zero".into(),
            ));
        }
        Ok(Self {
            cancellation,
            deadline: Instant::now() + timeout,
        })
    }

    pub(crate) async fn execute<T, F>(&self, future: F) -> Result<T, ClickhouseError>
    where
        F: Future<Output = Result<T, ClickhouseError>>,
    {
        tokio::pin!(future);
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(ClickhouseError::OperationCancelled),
            _ = tokio::time::sleep_until(self.deadline) => Err(ClickhouseError::OperationTimedOut),
            result = &mut future => result,
        }
    }
}

impl std::fmt::Debug for ClickhouseOperationContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClickhouseOperationContext")
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("deadline", &"<monotonic>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_is_typed() {
        let token = CancellationToken::new();
        token.cancel();
        let context = ClickhouseOperationContext::new(token, Duration::from_secs(1)).unwrap();
        let result = context
            .execute(async {
                std::future::pending::<()>().await;
                Ok(())
            })
            .await;
        assert_eq!(result, Err(ClickhouseError::OperationCancelled));
    }

    #[test]
    fn page_is_bounded() {
        assert!(ClickhousePageRequest::new(0, 0).is_err());
        assert!(ClickhousePageRequest::new(MAX_CLICKHOUSE_PAGE_SIZE + 1, 0).is_err());
        assert_eq!(ClickhousePageRequest::new(50, 10).unwrap().limit(), 50);
    }
}
