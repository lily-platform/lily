use std::any::Any;

use crate::PgError;

// Keep user conversions outside cleanup and locking paths. Application errors
// are only moved; they need not implement Clone, Sync or std::error::Error.
pub(super) enum ContextError<E> {
    Application(E),
    Framework(PgError),
}

pub(super) type ContextResult<T, E> = Result<T, ContextError<E>>;

impl<E> From<PgError> for ContextError<E> {
    fn from(error: PgError) -> Self {
        Self::Framework(error)
    }
}

impl<E: From<PgError>> ContextError<E> {
    pub(super) fn into_application(self) -> E {
        match self {
            Self::Application(error) => error,
            Self::Framework(error) => E::from(error),
        }
    }
}

impl<E: 'static> ContextError<E> {
    pub(super) fn query_failure(&self) -> PgError {
        match self {
            Self::Framework(error) => error.clone(),
            // Preserve the first PgError for existing PgResult callbacks. An
            // arbitrary E cannot be cloned or converted back into PgError, so
            // retain only the rollback obligation while returning E intact.
            Self::Application(error) => (error as &dyn Any)
                .downcast_ref::<PgError>()
                .cloned()
                .unwrap_or(PgError::TransactionRollbackOnly),
        }
    }
}
