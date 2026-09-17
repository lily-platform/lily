//! ClickHouse error types.

use std::fmt;

/// Fail-closed errors returned by ClickHouse configuration, queries and lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickhouseError {
    /// Readiness or connection establishment failed.
    ConnectionError(String),

    /// ClickHouse rejected or failed a query.
    QueryError(String),

    /// Internal lifecycle state could not be accessed safely.
    InternalError(String),

    /// A connection plan violates the supported operational profile.
    InvalidConfiguration(String),
    /// A database, table or column identifier is unsafe or not allowlisted.
    InvalidIdentifier(String),
    /// Pagination or write-batch bounds are invalid.
    InvalidPage(String),
    /// A constrained select plan is structurally invalid or too large.
    InvalidQueryPlan(String),
    /// A migration definition or ordered plan is invalid.
    InvalidMigrationPlan(String),
    /// Another deployment process currently owns the migration lock.
    MigrationLockUnavailable,
    /// Database history contains a version absent from the deployment plan.
    MigrationHistoryDiverged(i64),
    /// An applied version's content differs from the current deployment plan.
    MigrationChecksumMismatch(i64),
    /// Caller cancellation won the operation race.
    OperationCancelled,
    /// The configured query deadline elapsed.
    OperationTimedOut,
    /// The service has not completed initialization or has already closed.
    NotInitialized,
}

impl fmt::Display for ClickhouseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClickhouseError::ConnectionError(msg) => {
                write!(f, "ClickHouse connection error: {}", msg)
            }
            ClickhouseError::QueryError(msg) => write!(f, "ClickHouse query error: {}", msg),
            ClickhouseError::InternalError(msg) => write!(f, "ClickHouse internal error: {}", msg),
            ClickhouseError::InvalidConfiguration(msg) => {
                write!(f, "Invalid ClickHouse configuration: {msg}")
            }
            ClickhouseError::InvalidIdentifier(msg) => {
                write!(f, "Invalid ClickHouse identifier: {msg}")
            }
            ClickhouseError::InvalidPage(msg) => write!(f, "Invalid ClickHouse page: {msg}"),
            ClickhouseError::InvalidQueryPlan(msg) => {
                write!(f, "Invalid ClickHouse query plan: {msg}")
            }
            ClickhouseError::InvalidMigrationPlan(msg) => {
                write!(f, "Invalid ClickHouse migration plan: {msg}")
            }
            ClickhouseError::MigrationLockUnavailable => {
                write!(f, "ClickHouse migration lock is unavailable")
            }
            ClickhouseError::MigrationHistoryDiverged(version) => write!(
                f,
                "ClickHouse migration history contains unknown version {version}"
            ),
            ClickhouseError::MigrationChecksumMismatch(version) => write!(
                f,
                "ClickHouse migration checksum differs for version {version}"
            ),
            ClickhouseError::OperationCancelled => write!(f, "ClickHouse operation cancelled"),
            ClickhouseError::OperationTimedOut => write!(f, "ClickHouse operation timed out"),
            ClickhouseError::NotInitialized => write!(f, "ClickHouse service is not initialized"),
        }
    }
}

impl std::error::Error for ClickhouseError {}

// Conversion from clickhouse::error::Error
impl From<clickhouse::error::Error> for ClickhouseError {
    fn from(err: clickhouse::error::Error) -> Self {
        ClickhouseError::QueryError(err.to_string())
    }
}
