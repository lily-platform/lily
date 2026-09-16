use std::fmt;

use super::mongodb::MongoDbError;

/// CRUD operation that failed inside a generated or manually implemented
/// `lily_base_service::BaseService` boundary.
///
/// The error crate deliberately owns this provider-independent operation
/// vocabulary. A concrete service may retain its typed persistence cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseServiceOperation {
    /// One DTO create operation.
    Create,
    /// Bounded multi-DTO create operation.
    CreateMany,
    /// One full-entity update operation.
    Update,
    /// Delete selected by a DTO's entity identifier.
    Delete,
    /// Bounded multi-delete selected by DTO identifiers.
    DeleteMany,
    /// Required lookup by external string identifier.
    FindById,
    /// Delete selected by external string identifier.
    DeleteById,
    /// Bounded lookup by external string identifiers.
    FindByIds,
}

impl fmt::Display for BaseServiceOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Create => "create",
            Self::CreateMany => "create_many",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::DeleteMany => "delete_many",
            Self::FindById => "find_by_id",
            Self::DeleteById => "delete_by_id",
            Self::FindByIds => "find_by_ids",
        })
    }
}

/// Stable error emitted by the DTO-facing BaseService contract.
///
/// `entity` is supplied by `#[entity_type(Entity)]` in generated services, so
/// callers receive useful context without seeing repository filters, sessions
/// or operation contexts.
#[derive(Clone, Debug, PartialEq)]
pub enum BaseServiceError {
    /// A repository-backed operation failed before producing its service
    /// result.
    OperationFailed {
        /// Static entity type name supplied by the generated or manual service.
        entity: String,
        /// CRUD operation being executed.
        operation: BaseServiceOperation,
        /// Typed MongoDB repository cause retained for error classification.
        source: MongoDbError,
    },
    /// A required single-item lookup completed successfully but found no
    /// entity.
    NotFound {
        /// Static entity type name supplied by the service.
        entity: String,
        /// External identifier requested by the caller.
        identifier: String,
    },
}

impl BaseServiceError {
    /// Creates an operation failure while preserving entity, operation and
    /// typed repository cause.
    pub fn operation_failed(
        entity: impl Into<String>,
        operation: BaseServiceOperation,
        source: MongoDbError,
    ) -> Self {
        Self::OperationFailed {
            entity: entity.into(),
            operation,
            source,
        }
    }

    /// Creates the required-lookup not-found result.
    pub fn not_found(entity: impl Into<String>, identifier: impl Into<String>) -> Self {
        Self::NotFound {
            entity: entity.into(),
            identifier: identifier.into(),
        }
    }

    /// Returns the entity type name attached by the service implementation.
    pub fn entity_name(&self) -> &str {
        match self {
            Self::OperationFailed { entity, .. } | Self::NotFound { entity, .. } => entity,
        }
    }

    /// Returns the CRUD operation associated with this error.
    ///
    /// [`Self::NotFound`] is the terminal outcome of `find_by_id`, so it maps
    /// to [`BaseServiceOperation::FindById`].
    pub const fn operation(&self) -> BaseServiceOperation {
        match self {
            Self::OperationFailed { operation, .. } => *operation,
            Self::NotFound { .. } => BaseServiceOperation::FindById,
        }
    }

    /// Returns the typed repository cause for operation failures.
    pub fn repository_error(&self) -> Option<&MongoDbError> {
        match self {
            Self::OperationFailed { source, .. } => Some(source),
            Self::NotFound { .. } => None,
        }
    }
}

impl fmt::Display for BaseServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperationFailed {
                entity,
                operation,
                source,
            } => write!(formatter, "{entity} {operation} failed: {source}"),
            Self::NotFound { entity, identifier } => {
                write!(formatter, "{entity} with id '{identifier}' was not found")
            }
        }
    }
}

impl std::error::Error for BaseServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.repository_error()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{consumer::ConsumerError, http_api::HttpApiError};

    #[test]
    fn operation_error_preserves_entity_operation_and_source() {
        let error = BaseServiceError::operation_failed(
            "AppManager",
            BaseServiceOperation::Create,
            MongoDbError::DuplicateKey("name".to_string()),
        );

        assert_eq!(error.entity_name(), "AppManager");
        assert_eq!(error.operation(), BaseServiceOperation::Create);
        assert!(matches!(
            error.repository_error(),
            Some(MongoDbError::DuplicateKey(_))
        ));
        assert!(error.to_string().starts_with("AppManager create failed:"));
    }

    #[test]
    fn conversions_preserve_service_context_and_http_classification() {
        let not_found = BaseServiceError::not_found("AppManager", "507f1f77bcf86cd799439011");
        assert!(matches!(
            HttpApiError::from(not_found.clone()),
            HttpApiError::NotFound(message)
                if message.contains("AppManager") && message.contains("507f1f77bcf86cd799439011")
        ));
        assert!(matches!(
            ConsumerError::from(not_found),
            ConsumerError::BaseService(BaseServiceError::NotFound { entity, .. })
                if entity == "AppManager"
        ));

        let duplicate = BaseServiceError::operation_failed(
            "AppManager",
            BaseServiceOperation::Create,
            MongoDbError::DuplicateKey("name".to_string()),
        );
        assert!(matches!(
            HttpApiError::from(duplicate),
            HttpApiError::DuplicateResource(message)
                if message.contains("AppManager create failed")
        ));
    }
}
