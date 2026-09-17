//! Canonical process-environment classification.

/// The only process variable used by Lily's low-level environment detection.
pub const LILY_ENVIRONMENT_VARIABLE: &str = "LILY_ENV";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeEnvironment {
    Production,
    Development,
    Test,
}

impl RuntimeEnvironment {
    /// Parse an explicit environment value. Unknown values are rejected so a
    /// typo cannot silently enable development diagnostics.
    pub fn parse(value: &str) -> Result<Self, EnvironmentError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "production" | "prod" => Ok(Self::Production),
            "development" | "dev" => Ok(Self::Development),
            "test" => Ok(Self::Test),
            other => Err(EnvironmentError::Unsupported(other.to_string())),
        }
    }

    /// Read `LILY_ENV`. An absent or invalid value is fail-safe production.
    pub fn current() -> Self {
        std::env::var(LILY_ENVIRONMENT_VARIABLE)
            .ok()
            .and_then(|value| Self::parse(&value).ok())
            .unwrap_or(Self::Production)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvironmentError {
    Unsupported(String),
}

impl std::fmt::Display for EnvironmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(value) => write!(formatter, "unsupported LILY_ENV value: {value}"),
        }
    }
}

impl std::error::Error for EnvironmentError {}

#[doc(hidden)]
pub fn debug_diagnostics_enabled() -> bool {
    RuntimeEnvironment::current() == RuntimeEnvironment::Development
}

#[cfg(test)]
mod tests {
    use super::RuntimeEnvironment;

    #[test]
    fn parses_only_the_documented_environment_values() {
        assert_eq!(
            RuntimeEnvironment::parse("production").unwrap(),
            RuntimeEnvironment::Production
        );
        assert_eq!(
            RuntimeEnvironment::parse("DEV").unwrap(),
            RuntimeEnvironment::Development
        );
        assert_eq!(
            RuntimeEnvironment::parse("test").unwrap(),
            RuntimeEnvironment::Test
        );
        assert!(RuntimeEnvironment::parse("debug-ish").is_err());
    }
}
