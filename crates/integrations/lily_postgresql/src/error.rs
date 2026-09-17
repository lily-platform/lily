//! Typed, secret-safe PostgreSQL integration errors.

use deadpool::managed::{PoolError, TimeoutType};
use diesel_async::pooled_connection;

/// Result type returned by Lily's PostgreSQL boundary.
pub type PgResult<T> = Result<T, PgError>;

/// Stable category for a Diesel query failure.
///
/// Driver messages and statement values are intentionally not retained so
/// logs and transport mappings cannot expose query or credential material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgQueryErrorKind {
    /// A query expecting one row matched none.
    NotFound,
    /// A persisted value or domain invariant is invalid.
    InvalidData,
    /// Caller input was rejected by the transaction workflow.
    InvalidInput,
    /// The operation conflicts with an existing record, including a unique constraint.
    Conflict,
    /// The workflow could not authenticate the account or credentials.
    Unauthorized,
    /// The authenticated account is not eligible for the requested operation.
    Forbidden,
    /// A database constraint rejected the operation.
    ConstraintViolation,
    /// Diesel could not serialize a Rust value for PostgreSQL.
    Serialization,
    /// Diesel could not deserialize a PostgreSQL value.
    Deserialization,
    /// Transaction state or rollback handling failed.
    Transaction,
    /// Another PostgreSQL database error occurred.
    Database,
    /// A Diesel error outside the stable categories occurred.
    Other,
}

/// Stable category for TLS configuration and trust failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTlsErrorKind {
    /// TLS settings could not produce a valid client configuration.
    InvalidConfiguration,
    /// The configured additional CA bundle could not be read.
    CaBundleRead,
    /// The additional CA bundle contained an invalid or unsupported certificate.
    InvalidCaCertificate,
    /// The CA bundle contained private-key material and was rejected.
    PrivateKeyInCaBundle,
}

/// Pool stage in which a configured timeout elapsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgPoolTimeoutPhase {
    /// Waiting for an available pooled connection.
    Acquire,
    /// Creating a new PostgreSQL connection.
    Connect,
    /// Verifying or recycling an existing connection.
    Recycle,
}

/// Typed, secret-safe failure returned by Lily's PostgreSQL integration.
///
/// Variants expose stable categories and bounded operational metadata; raw
/// connection strings, SQL, bind values and driver messages are not retained.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum PgError {
    /// The compiled `single`/`factory` feature disagrees with configuration.
    #[error(
        "PostgreSQL Cargo feature mode '{compiled}' does not match configured mode '{configured}'"
    )]
    FeatureModeMismatch {
        /// Cargo feature mode compiled into the application.
        compiled: &'static str,
        /// Mode supplied by validated configuration.
        configured: &'static str,
    },
    /// A validated connection plan rejected an unsupported configuration.
    #[error("invalid PostgreSQL configuration ({code})")]
    InvalidConfiguration {
        /// Stable machine-readable validation category.
        code: &'static str,
    },
    /// The container or factory has not initialized the selected service.
    #[error("PostgreSQL service is not initialized")]
    NotInitialized,
    /// A factory-mode repository initializer did not select its database cell.
    #[error("PostgreSQL repository database has not been set by its ServiceTrait initializer")]
    RepositoryDatabaseNotSet,
    /// No configured factory cell has the requested exact name.
    #[error("PostgreSQL factory cell '{name}' does not exist")]
    UnknownCell {
        /// Requested cell name.
        name: String,
    },
    /// A lifecycle owner attempted to initialize the same service twice.
    #[error("PostgreSQL service has already been initialized")]
    AlreadyInitialized,
    /// The pool has stopped accepting new operations.
    #[error("PostgreSQL pool is closed")]
    PoolClosed,
    /// Execution cancellation stopped connection acquisition or ordinary work.
    #[error("PostgreSQL operation was cancelled")]
    OperationCancelled,
    /// An interrupted callback made further use of this lease unsafe.
    #[error("PostgreSQL connection operation was interrupted")]
    ConnectionInterrupted,
    /// User callback code panicked; its connection is cleaned up or discarded.
    #[error("PostgreSQL operation panicked")]
    OperationPanicked,
    /// The scoped context has stopped accepting operations.
    #[error("PostgreSQL context is closed")]
    ContextClosed,
    /// An overlapping operation or a transaction transition occupies the context.
    #[error("PostgreSQL context is busy")]
    ContextBusy,
    /// A second or nested transaction was requested on the same context.
    #[error("PostgreSQL context already has a transaction")]
    ContextTransactionActive,
    /// A context callback returned an application error. If transaction work
    /// swallows it, the original error cannot be recovered, but commit is forbidden.
    #[error("PostgreSQL transaction is rollback-only after a callback error")]
    TransactionRollbackOnly,
    /// Scope disposal could not observe completed cleanup within its budget.
    #[error("PostgreSQL context disposal timed out")]
    ContextCleanupTimeout,
    /// A bounded pool operation exceeded its configured timeout.
    #[error("PostgreSQL pool timed out during {phase:?}")]
    PoolTimeout {
        /// Pool stage that timed out.
        phase: PgPoolTimeoutPhase,
    },
    /// Execution cancellation prevented the transaction from committing.
    /// If a transaction had begun, Diesel's rollback was awaited. A rollback
    /// failure or cleanup timeout takes precedence over this error.
    #[error("PostgreSQL transaction was cancelled")]
    TransactionCancelled,
    /// Cancellation cleanup exceeded its budget; the connection cannot be reused.
    /// An abandoned operation may retain the lease until its future is dropped.
    /// Completion of rollback on the server has not been confirmed.
    #[error("PostgreSQL transaction cancellation cleanup timed out")]
    TransactionCleanupTimeout,
    /// The driver could not establish or prepare a PostgreSQL connection.
    #[error("PostgreSQL connection could not be established")]
    ConnectionFailed,
    /// The authenticated startup `SELECT 1` readiness check failed.
    #[error("PostgreSQL authenticated readiness probe failed")]
    ReadinessFailed,
    /// A Diesel query failed in the reported stable category.
    #[error("PostgreSQL query failed ({kind:?})")]
    Query {
        /// Stable query failure category.
        kind: PgQueryErrorKind,
    },
    /// TLS trust or client configuration failed.
    #[error("PostgreSQL TLS setup failed ({kind:?})")]
    Tls {
        /// Stable TLS failure category.
        kind: PgTlsErrorKind,
    },
    /// The configured connection pool could not be constructed.
    #[error("PostgreSQL pool could not be built")]
    PoolBuild,
    /// Graceful shutdown elapsed while scoped operations were still active.
    #[error("PostgreSQL shutdown timed out with {remaining} operation(s) still active")]
    ShutdownTimeout {
        /// Number of active operations observed at timeout.
        remaining: usize,
    },
    /// A named factory cell failed readiness during startup.
    #[error("PostgreSQL factory startup failed for cell '{cell}'")]
    FactoryStartup {
        /// Cell whose startup failed.
        cell: String,
    },
    /// Factory startup and cleanup of already opened cells both failed.
    #[error(
        "PostgreSQL factory startup failed for cell '{cell}' and rollback failed for cells: {cells:?}"
    )]
    FactoryStartupRollback {
        /// Cell whose startup failed.
        cell: String,
        /// Previously opened cells whose cleanup also failed.
        cells: Vec<String>,
    },
    /// One or more named cells failed graceful factory shutdown.
    #[error("PostgreSQL factory shutdown failed for cells: {cells:?}")]
    FactoryShutdown {
        /// Cells whose shutdown failed, sorted lexically.
        cells: Vec<String>,
    },
    /// Explicit Diesel migration execution failed.
    #[error("Diesel migration failed ({code})")]
    Migration {
        /// Stable migration failure category.
        code: &'static str,
    },
    /// The bounded transaction command queue has no remaining capacity.
    #[error("PostgreSQL transaction command queue is saturated")]
    TransactionCommandSaturated,
    /// The framework transaction owner has stopped accepting operations.
    #[error("PostgreSQL transaction is closed")]
    TransactionClosed,
    /// Transaction work was detached from its caller or framework owner.
    #[error("PostgreSQL transaction operation was detached")]
    TransactionOperationDetached,
    /// The framework transaction owner could not finalize deterministically.
    #[error("PostgreSQL transaction finalization failed")]
    TransactionFinalization,
}

impl From<diesel::result::Error> for PgError {
    fn from(error: diesel::result::Error) -> Self {
        use diesel::result::{DatabaseErrorKind, Error};

        let kind = match error {
            Error::NotFound => PgQueryErrorKind::NotFound,
            Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                PgQueryErrorKind::Conflict
            }
            Error::DatabaseError(
                DatabaseErrorKind::ForeignKeyViolation
                | DatabaseErrorKind::NotNullViolation
                | DatabaseErrorKind::CheckViolation
                | DatabaseErrorKind::ExclusionViolation,
                _,
            ) => PgQueryErrorKind::ConstraintViolation,
            Error::SerializationError(_) => PgQueryErrorKind::Serialization,
            Error::DeserializationError(_) => PgQueryErrorKind::Deserialization,
            Error::RollbackTransaction
            | Error::AlreadyInTransaction
            | Error::NotInTransaction
            | Error::BrokenTransactionManager => PgQueryErrorKind::Transaction,
            Error::DatabaseError(_, _) => PgQueryErrorKind::Database,
            _ => PgQueryErrorKind::Other,
        };
        Self::Query { kind }
    }
}

impl From<PoolError<pooled_connection::PoolError>> for PgError {
    fn from(error: PoolError<pooled_connection::PoolError>) -> Self {
        match error {
            PoolError::Timeout(TimeoutType::Wait) => Self::PoolTimeout {
                phase: PgPoolTimeoutPhase::Acquire,
            },
            PoolError::Timeout(TimeoutType::Create) => Self::PoolTimeout {
                phase: PgPoolTimeoutPhase::Connect,
            },
            PoolError::Timeout(TimeoutType::Recycle) => Self::PoolTimeout {
                phase: PgPoolTimeoutPhase::Recycle,
            },
            PoolError::Closed => Self::PoolClosed,
            PoolError::Backend(_)
            | PoolError::NoRuntimeSpecified
            | PoolError::PostCreateHook(_) => Self::ConnectionFailed,
        }
    }
}
