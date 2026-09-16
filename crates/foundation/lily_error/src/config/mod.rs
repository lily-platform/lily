//! Configuration loading, decoding, validation, and secret-resolution errors.

/// Failure returned by `lily_config` while producing an application snapshot.
#[derive(Debug, PartialEq, Clone)]
pub enum ConfigError {
    /// A required configuration key is absent.
    KeyNotFound(String),

    /// A stored value cannot be converted to its requested type.
    TypeCastError {
        /// Configuration key being converted.
        key: String,
        /// Requested target type.
        expected_type: String,
        /// Bounded conversion diagnostic.
        error: String,
    },

    /// Reading a configuration source failed.
    IoError(String),

    /// A configuration source could not be parsed.
    ParseError(String),

    /// A configuration value could not be serialized.
    SerializationError(String),

    /// The decoded configuration violates its validation contract.
    ValidationError(String),

    /// An application-supplied secret resolver failed.
    SecretResolveError {
        /// Configuration key whose secret could not be resolved.
        key: String,
        /// Bounded resolver diagnostic.
        error: String,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::KeyNotFound(key) => write!(f, "Configuration key not found: {key}"),
            ConfigError::TypeCastError {
                key,
                expected_type,
                error,
            } => {
                write!(
                    f,
                    "Type cast error for key '{key}': expected {expected_type}, error: {error}"
                )
            }
            ConfigError::IoError(msg) => write!(f, "IO error: {msg}"),
            ConfigError::ParseError(msg) => write!(f, "Parse error: {msg}"),
            ConfigError::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            ConfigError::ValidationError(msg) => write!(f, "Validation error: {msg}"),
            ConfigError::SecretResolveError { key, error } => {
                write!(f, "Failed to resolve secret for key '{key}': {error}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        ConfigError::IoError(err.to_string())
    }
}
