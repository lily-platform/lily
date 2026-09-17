use std::fmt;

use lily_postgresql::PgError;

use crate::transactional::TransactionalOutboxContractError;

/// Secret-safe failure reported by Lily's PostgreSQL inbox/outbox boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PostgresReliabilityError {
    /// A public value did not satisfy Lily's bounded canonical contract.
    InvalidContract {
        /// Stable validation category.
        code: &'static str,
    },
    /// The selected PostgreSQL dependency could not be resolved.
    DependencyUnavailable,
    /// The Lily-owned schema has not been installed explicitly.
    SchemaMissing,
    /// The installed Lily-owned schema is older than this runtime.
    SchemaOutdated {
        /// Installed schema version.
        installed: i64,
        /// Version required by this runtime.
        required: i64,
    },
    /// The installed Lily-owned schema is newer than this runtime.
    SchemaTooNew {
        /// Installed schema version.
        installed: i64,
        /// Newest version understood by this runtime.
        supported: i64,
    },
    /// The namespaced objects exist but do not match Lily's exact schema contract.
    SchemaDrift,
    /// A relay mutation did not own the supplied outbox claim.
    OutboxClaimLost,
    /// The oldest eligible durable row cannot fit under the active relay byte policy.
    OutboxRecordTooLarge {
        /// Persisted body length in bytes.
        bytes: usize,
        /// Active per-batch relay byte limit.
        maximum: usize,
    },
    /// Queue shutdown has stopped transactional delivery admission.
    TransactionAdmissionClosed,
    /// An owner was joined after panic or interruption without confirmed finalization.
    TransactionTerminationIncomplete,
    /// Graceful shutdown elapsed while transactions were still active.
    TransactionDrainTimeout {
        /// Active framework-owned transactions still awaiting finalization.
        remaining: usize,
    },
    /// The PostgreSQL integration returned a secret-safe typed failure.
    Database(PgError),
}

impl PostgresReliabilityError {
    /// Returns a stable code suitable for telemetry and broker error mapping.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidContract { code } => code,
            Self::DependencyUnavailable => "QUEUE_POSTGRES_DEPENDENCY_UNAVAILABLE",
            Self::SchemaMissing => "QUEUE_POSTGRES_SCHEMA_MISSING",
            Self::SchemaOutdated { .. } => "QUEUE_POSTGRES_SCHEMA_OUTDATED",
            Self::SchemaTooNew { .. } => "QUEUE_POSTGRES_SCHEMA_TOO_NEW",
            Self::SchemaDrift => "QUEUE_POSTGRES_SCHEMA_DRIFT",
            Self::OutboxClaimLost => "QUEUE_POSTGRES_OUTBOX_CLAIM_LOST",
            Self::OutboxRecordTooLarge { .. } => "QUEUE_OUTBOX_RELAY_RECORD_TOO_LARGE",
            Self::TransactionAdmissionClosed => "QUEUE_POSTGRES_TRANSACTION_ADMISSION_CLOSED",
            Self::TransactionTerminationIncomplete => {
                "QUEUE_POSTGRES_TRANSACTION_TERMINATION_INCOMPLETE"
            }
            Self::TransactionDrainTimeout { .. } => "QUEUE_POSTGRES_TRANSACTION_DRAIN_TIMEOUT",
            Self::Database(_) => "QUEUE_POSTGRES_STORAGE_FAILED",
        }
    }
}

impl fmt::Display for PostgresReliabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for PostgresReliabilityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PgError> for PostgresReliabilityError {
    fn from(error: PgError) -> Self {
        Self::Database(error)
    }
}

impl From<TransactionalOutboxContractError> for PostgresReliabilityError {
    fn from(error: TransactionalOutboxContractError) -> Self {
        Self::InvalidContract { code: error.code() }
    }
}
