use std::fmt;

use lily_mongodb::MongoDbError;

use crate::transactional::TransactionalOutboxContractError;

/// Secret-safe failure reported by Lily's MongoDB inbox/outbox boundary.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum MongoReliabilityError {
    /// A public value did not satisfy Lily's bounded canonical contract.
    InvalidContract {
        /// Stable validation category.
        code: &'static str,
    },
    /// The selected MongoDB dependency or factory cell could not be resolved.
    DependencyUnavailable,
    /// The Lily-owned collections have not been installed explicitly.
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
    /// Collections or indexes do not match Lily's exact schema fingerprint.
    SchemaDrift,
    /// The selected MongoDB deployment does not support multi-document transactions.
    TransactionTopologyUnsupported,
    /// Another process owns the live inbox lease for this event.
    InboxLeaseOwned,
    /// The transaction attempt lost its external inbox lease.
    InboxLeaseLost,
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
    /// Graceful shutdown elapsed while transaction owners remained active.
    TransactionDrainTimeout {
        /// Active framework-owned transaction owners still awaiting finalization.
        remaining: usize,
    },
    /// Every configured whole-transaction retry was consumed.
    TransactionRetryExhausted,
    /// The absolute queue-delivery execution deadline elapsed.
    TransactionDeadlineExceeded,
    /// Framework shutdown or delivery cancellation interrupted the owner.
    TransactionCancelled,
    /// The bounded commit-only retry budget expired without a known outcome.
    CommitOutcomeUnknown,
    /// MongoDB requested a complete transaction-body retry.
    #[doc(hidden)]
    TransientTransaction,
    /// MongoDB could not yet prove the result of the current commit.
    #[doc(hidden)]
    UnknownCommitResult,
    /// The MongoDB adapter returned a storage failure.
    ///
    /// Driver diagnostics are deliberately not retained because they may
    /// contain endpoints, commands or application document fragments.
    Database,
}

impl MongoReliabilityError {
    /// Returns a stable code suitable for telemetry and broker error mapping.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidContract { code } => code,
            Self::DependencyUnavailable => "QUEUE_MONGODB_DEPENDENCY_UNAVAILABLE",
            Self::SchemaMissing => "QUEUE_MONGODB_SCHEMA_MISSING",
            Self::SchemaOutdated { .. } => "QUEUE_MONGODB_SCHEMA_OUTDATED",
            Self::SchemaTooNew { .. } => "QUEUE_MONGODB_SCHEMA_TOO_NEW",
            Self::SchemaDrift => "QUEUE_MONGODB_SCHEMA_DRIFT",
            Self::TransactionTopologyUnsupported => {
                "QUEUE_MONGODB_TRANSACTION_TOPOLOGY_UNSUPPORTED"
            }
            Self::InboxLeaseOwned => "QUEUE_MONGODB_INBOX_IN_PROGRESS",
            Self::InboxLeaseLost => "QUEUE_MONGODB_INBOX_LEASE_LOST",
            Self::OutboxClaimLost => "QUEUE_MONGODB_OUTBOX_CLAIM_LOST",
            Self::OutboxRecordTooLarge { .. } => "QUEUE_OUTBOX_RELAY_RECORD_TOO_LARGE",
            Self::TransactionAdmissionClosed => "QUEUE_MONGODB_TRANSACTION_ADMISSION_CLOSED",
            Self::TransactionTerminationIncomplete => {
                "QUEUE_MONGODB_TRANSACTION_TERMINATION_INCOMPLETE"
            }
            Self::TransactionDrainTimeout { .. } => "QUEUE_MONGODB_TRANSACTION_DRAIN_TIMEOUT",
            Self::TransactionRetryExhausted => "QUEUE_MONGODB_TRANSACTION_RETRY_EXHAUSTED",
            Self::TransactionDeadlineExceeded => "QUEUE_MONGODB_TRANSACTION_DEADLINE_EXCEEDED",
            Self::TransactionCancelled => "QUEUE_MONGODB_TRANSACTION_CANCELLED",
            Self::CommitOutcomeUnknown => "QUEUE_MONGODB_COMMIT_OUTCOME_UNKNOWN",
            Self::TransientTransaction => "QUEUE_MONGODB_TRANSACTION_TRANSIENT",
            Self::UnknownCommitResult => "QUEUE_MONGODB_COMMIT_RESULT_UNKNOWN",
            Self::Database => "QUEUE_MONGODB_STORAGE_FAILED",
        }
    }

    pub(crate) const fn is_transient_transaction(&self) -> bool {
        matches!(self, Self::TransientTransaction)
    }

    pub(crate) const fn is_unknown_commit_result(&self) -> bool {
        matches!(self, Self::UnknownCommitResult)
    }
}

impl fmt::Display for MongoReliabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for MongoReliabilityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl From<MongoDbError> for MongoReliabilityError {
    fn from(error: MongoDbError) -> Self {
        match error {
            MongoDbError::TransientTransaction => Self::TransientTransaction,
            MongoDbError::UnknownTransactionCommitResult => Self::UnknownCommitResult,
            MongoDbError::OperationCancelled => Self::TransactionCancelled,
            MongoDbError::OperationTimedOut => Self::TransactionDeadlineExceeded,
            _ => Self::Database,
        }
    }
}

impl From<TransactionalOutboxContractError> for MongoReliabilityError {
    fn from(error: TransactionalOutboxContractError) -> Self {
        Self::InvalidContract { code: error.code() }
    }
}
