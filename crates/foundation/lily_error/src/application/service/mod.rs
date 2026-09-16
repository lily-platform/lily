/// Failures returned by queue handler service functions.
#[derive(Debug, PartialEq)]
pub enum ServiceError {
    /// A handler failed without a narrower category.
    General(String),
    /// A handler I/O operation failed.
    Io(String),
    /// A handler TLS operation failed.
    Tls(String),
    /// A handler database operation failed.
    DatabaseError(String),
    /// The delivered message violates the handler contract.
    ValidationError(String),
    /// A resource required by the message does not exist.
    NotFound(String),
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceError::General(msg) => write!(f, "General Error: {msg}"),
            ServiceError::Io(msg) => write!(f, "IO Error: {msg}"),
            ServiceError::Tls(msg) => write!(f, "TLS Error: {msg}"),
            ServiceError::DatabaseError(msg) => write!(f, "Database Error: {msg}"),
            ServiceError::ValidationError(msg) => write!(f, "Validation Error: {msg}"),
            ServiceError::NotFound(msg) => write!(f, "Not Found: {msg}"),
        }
    }
}

impl std::error::Error for ServiceError {}

impl From<std::io::Error> for ServiceError {
    fn from(error: std::io::Error) -> Self {
        ServiceError::Io(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceError;

    #[test]
    fn io_errors_preserve_the_typed_category() {
        let error = ServiceError::from(std::io::Error::other("disk unavailable"));
        assert!(matches!(error, ServiceError::Io(_)));
    }
}
