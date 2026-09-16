use std::path::PathBuf;

/// Errors that can occur while loading localization files at startup.
#[derive(Debug)]
pub enum LocalizationError {
    /// The configured locale directory does not exist or is not a directory.
    DirectoryNotFound {
        /// Configured locale directory.
        path: PathBuf,
    },
    /// The locale directory could not be enumerated.
    DirectoryReadError {
        /// Configured locale directory.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: std::io::Error,
    },
    /// A locale file could not be read as UTF-8 text.
    FileReadError {
        /// Locale file that could not be read.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: std::io::Error,
    },
    /// A locale file did not contain a valid `String -> String` JSON object.
    JsonParseError {
        /// Locale file that could not be decoded.
        path: PathBuf,
        /// Underlying JSON failure.
        source: serde_json::Error,
    },
    /// A locale filename or its contents violate the localization contract.
    InvalidLanguageFile {
        /// Invalid locale file.
        path: PathBuf,
        /// Contract violation.
        reason: String,
    },
    /// A programmatically supplied immutable catalog violates the contract.
    InvalidCatalog {
        /// Contract violation.
        reason: String,
    },
}

impl std::fmt::Display for LocalizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirectoryNotFound { path } => {
                write!(
                    formatter,
                    "localization directory not found: {}",
                    path.display()
                )
            }
            Self::DirectoryReadError { path, source } => write!(
                formatter,
                "failed to read localization directory '{}': {source}",
                path.display()
            ),
            Self::FileReadError { path, source } => write!(
                formatter,
                "failed to read localization file '{}': {source}",
                path.display()
            ),
            Self::JsonParseError { path, source } => write!(
                formatter,
                "failed to parse localization file '{}': {source}",
                path.display()
            ),
            Self::InvalidLanguageFile { path, reason } => write!(
                formatter,
                "invalid localization file '{}': {reason}",
                path.display()
            ),
            Self::InvalidCatalog { reason } => {
                write!(formatter, "invalid localization catalog: {reason}")
            }
        }
    }
}

impl std::error::Error for LocalizationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DirectoryReadError { source, .. } | Self::FileReadError { source, .. } => {
                Some(source)
            }
            Self::JsonParseError { source, .. } => Some(source),
            Self::DirectoryNotFound { .. }
            | Self::InvalidLanguageFile { .. }
            | Self::InvalidCatalog { .. } => None,
        }
    }
}
