use std::fmt;

/// MongoDB specific errors for lily framework
#[derive(Debug, Clone, PartialEq)]
pub enum MongoDbError {
    /// Establishing or retaining a MongoDB connection failed.
    ConnectionFailed(String),
    /// Establishing a MongoDB connection exceeded its deadline.
    ConnectionTimeout(String),
    /// MongoDB authentication rejected the configured identity.
    AuthenticationFailed(String),

    /// The requested database does not exist.
    DatabaseNotFound(String),
    /// The requested collection does not exist.
    CollectionNotFound(String),
    /// A requested document does not exist.
    DocumentNotFound(String),
    /// A requested persistence resource does not exist.
    NotFound(String),

    /// Inserting a document failed.
    InsertFailed(String),
    /// Updating one or more documents failed.
    UpdateFailed(String),
    /// Deleting one or more documents failed.
    DeleteFailed(String),
    /// Executing a query failed.
    QueryFailed(String),

    /// Creating an index failed.
    IndexCreationFailed(String),
    /// MongoDB schema validation rejected a document.
    SchemaValidationFailed(String),
    /// A unique-key constraint rejected a write.
    DuplicateKey(String),

    /// Serializing a Rust value for MongoDB failed.
    SerializationError(String),
    /// Deserializing a MongoDB value failed.
    DeserializationError(String),
    /// Encoding or decoding BSON failed.
    BsonError(String),

    /// A MongoDB transaction failed.
    TransactionFailed(String),
    /// A MongoDB transaction was aborted.
    TransactionAborted(String),
    /// The server labelled an operation so the complete transaction body may
    /// be retried. No driver diagnostic or connection detail is retained.
    TransientTransaction,
    /// The server could not prove the outcome of a commit operation. The
    /// caller may retry that same commit, but must not rerun the transaction
    /// body solely because of this result.
    UnknownTransactionCommitResult,

    /// A document identifier violates the repository contract.
    InvalidDocumentId(String),
    /// A query filter violates the repository contract.
    InvalidFilter(String),
    /// A bulk operation violates configured batch limits.
    InvalidBatch(String),
    /// A pagination request is invalid.
    InvalidPage(String),
    /// A repository operation context is invalid.
    InvalidOperationContext(String),
    /// The operation was cancelled by its owner.
    OperationCancelled,
    /// The operation exceeded its deadline.
    OperationTimedOut,
    /// Optimistic concurrency rejected the write.
    ConcurrencyConflict,

    /// Another migration runner currently owns the lease.
    MigrationLockUnavailable,
    /// The migration runner lost its lease.
    MigrationLockLost,
    /// Applied migration history is absent from the application plan.
    MigrationHistoryDiverged(i64),
    /// An applied migration checksum differs from the application plan.
    MigrationChecksumMismatch(i64),

    /// MongoDB configuration is invalid.
    InvalidConfiguration(String),
    /// A collection name violates MongoDB naming rules.
    InvalidCollectionName(String),

    /// An internal MongoDB adapter invariant failed.
    InternalError(String),
    /// The driver returned an unclassified failure.
    Unknown(String),
}

impl fmt::Display for MongoDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MongoDbError::ConnectionFailed(msg) => write!(f, "MongoDB connection failed: {msg}"),
            MongoDbError::ConnectionTimeout(msg) => write!(f, "MongoDB connection timeout: {msg}"),
            MongoDbError::AuthenticationFailed(msg) => {
                write!(f, "MongoDB authentication failed: {msg}")
            }

            MongoDbError::DatabaseNotFound(msg) => write!(f, "Database not found: {msg}"),
            MongoDbError::CollectionNotFound(msg) => write!(f, "Collection not found: {msg}"),
            MongoDbError::DocumentNotFound(msg) => write!(f, "Document not found: {msg}"),
            MongoDbError::NotFound(msg) => write!(f, "Not found: {msg}"),

            MongoDbError::InsertFailed(msg) => write!(f, "Insert operation failed: {msg}"),
            MongoDbError::UpdateFailed(msg) => write!(f, "Update operation failed: {msg}"),
            MongoDbError::DeleteFailed(msg) => write!(f, "Delete operation failed: {msg}"),
            MongoDbError::QueryFailed(msg) => write!(f, "Query operation failed: {msg}"),

            MongoDbError::IndexCreationFailed(msg) => write!(f, "Index creation failed: {msg}"),
            MongoDbError::SchemaValidationFailed(msg) => {
                write!(f, "Schema validation failed: {msg}")
            }
            MongoDbError::DuplicateKey(msg) => write!(f, "Duplicate key error: {msg}"),

            MongoDbError::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            MongoDbError::DeserializationError(msg) => write!(f, "Deserialization error: {msg}"),
            MongoDbError::BsonError(msg) => write!(f, "BSON error: {msg}"),

            MongoDbError::TransactionFailed(msg) => write!(f, "Transaction failed: {msg}"),
            MongoDbError::TransactionAborted(msg) => write!(f, "Transaction aborted: {msg}"),
            MongoDbError::TransientTransaction => {
                write!(f, "MongoDB transaction may be retried")
            }
            MongoDbError::UnknownTransactionCommitResult => {
                write!(f, "MongoDB transaction commit result is unknown")
            }

            MongoDbError::InvalidDocumentId(msg) => write!(f, "Invalid document id: {msg}"),
            MongoDbError::InvalidFilter(msg) => write!(f, "Invalid MongoDB filter: {msg}"),
            MongoDbError::InvalidBatch(msg) => write!(f, "Invalid MongoDB batch: {msg}"),
            MongoDbError::InvalidPage(msg) => write!(f, "Invalid MongoDB page: {msg}"),
            MongoDbError::InvalidOperationContext(msg) => {
                write!(f, "Invalid MongoDB operation context: {msg}")
            }
            MongoDbError::OperationCancelled => write!(f, "MongoDB operation cancelled"),
            MongoDbError::OperationTimedOut => write!(f, "MongoDB operation timed out"),
            MongoDbError::ConcurrencyConflict => {
                write!(f, "MongoDB optimistic concurrency conflict")
            }
            MongoDbError::MigrationLockUnavailable => {
                write!(f, "MongoDB migration lock is held by another runner")
            }
            MongoDbError::MigrationLockLost => write!(f, "MongoDB migration lock was lost"),
            MongoDbError::MigrationHistoryDiverged(version) => write!(
                f,
                "MongoDB migration version {version} is absent from the application plan"
            ),
            MongoDbError::MigrationChecksumMismatch(version) => write!(
                f,
                "MongoDB migration version {version} has a different checksum"
            ),

            MongoDbError::InvalidConfiguration(msg) => write!(f, "Invalid configuration: {msg}"),
            MongoDbError::InvalidCollectionName(msg) => write!(f, "Invalid collection name: {msg}"),

            MongoDbError::InternalError(msg) => write!(f, "Internal MongoDB error: {msg}"),
            MongoDbError::Unknown(msg) => write!(f, "Unknown MongoDB error: {msg}"),
        }
    }
}

impl std::error::Error for MongoDbError {}

/// Convert from mongodb::error::Error to our MongoDbError
#[cfg(feature = "mongodb-driver")]
impl From<mongodb::error::Error> for MongoDbError {
    fn from(error: mongodb::error::Error) -> Self {
        use mongodb::error::{
            ErrorKind, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT,
        };

        // Labels carry stronger transaction semantics than the broad driver
        // error kind. Classify them before formatting the driver error so the
        // public value remains secret-safe and machine-actionable.
        if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
            return MongoDbError::UnknownTransactionCommitResult;
        }
        if error.contains_label(TRANSIENT_TRANSACTION_ERROR) {
            return MongoDbError::TransientTransaction;
        }

        match error.kind.as_ref() {
            ErrorKind::Authentication { .. } => {
                MongoDbError::AuthenticationFailed(error.to_string())
            }
            ErrorKind::BsonDeserialization(_) => {
                MongoDbError::DeserializationError(error.to_string())
            }
            ErrorKind::BsonSerialization(_) => MongoDbError::SerializationError(error.to_string()),
            ErrorKind::Command(_) => MongoDbError::QueryFailed(error.to_string()),
            ErrorKind::InvalidArgument { .. } => {
                MongoDbError::InvalidConfiguration(error.to_string())
            }
            ErrorKind::Io(_) => MongoDbError::ConnectionFailed(error.to_string()),
            ErrorKind::ServerSelection { .. } => MongoDbError::ConnectionTimeout(error.to_string()),
            ErrorKind::Transaction { .. } => MongoDbError::TransactionFailed(error.to_string()),
            ErrorKind::Write(_) => {
                let error_str = error.to_string();
                if error_str.contains("duplicate key") || error_str.contains("E11000") {
                    MongoDbError::DuplicateKey(error_str)
                } else {
                    MongoDbError::InsertFailed(error_str)
                }
            }
            _ => MongoDbError::Unknown(error.to_string()),
        }
    }
}

/// Convert from serde errors
impl From<serde_json::Error> for MongoDbError {
    fn from(error: serde_json::Error) -> Self {
        MongoDbError::SerializationError(error.to_string())
    }
}

/// Convert from BSON errors
#[cfg(feature = "mongodb-driver")]
impl From<mongodb::bson::ser::Error> for MongoDbError {
    fn from(error: mongodb::bson::ser::Error) -> Self {
        MongoDbError::BsonError(error.to_string())
    }
}

#[cfg(feature = "mongodb-driver")]
impl From<mongodb::bson::de::Error> for MongoDbError {
    fn from(error: mongodb::bson::de::Error) -> Self {
        MongoDbError::BsonError(error.to_string())
    }
}
