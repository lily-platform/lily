//! Scalar conversion used by dynamic configuration lookup.

use crate::ConfigError;
use std::path::PathBuf;

/// Converts one flattened scalar configuration value into an application type.
///
/// [`crate::ConfigService::get`] uses this trait for dot-notation lookup.
/// Implementations should return [`ConfigError::TypeCastError`] when the value
/// is malformed and must not include secret material in custom diagnostics.
/// Collections and nested sections are not scalar values; access them through
/// [`crate::ConfigService::get_lily_config`] or [`crate::ConfigSnapshot::config`].
pub trait FromTomlValue: Sized {
    /// Converts `value` into `Self`.
    fn from_toml_value(value: &str) -> Result<Self, ConfigError>;
}

impl FromTomlValue for String {
    fn from_toml_value(value: &str) -> Result<Self, ConfigError> {
        Ok(value.to_string())
    }
}

macro_rules! impl_integer_value {
    ($($type:ty),+ $(,)?) => {
        $(
            impl FromTomlValue for $type {
                fn from_toml_value(value: &str) -> Result<Self, ConfigError> {
                    value.parse().map_err(|_| ConfigError::TypeCastError {
                        key: "unknown".to_string(),
                        expected_type: stringify!($type).to_string(),
                        error: format!("cannot parse value as {}", stringify!($type)),
                    })
                }
            }
        )+
    };
}

impl_integer_value!(u8, u16, u32, u64, usize);

impl FromTomlValue for bool {
    fn from_toml_value(value: &str) -> Result<Self, ConfigError> {
        value.parse().map_err(|_| ConfigError::TypeCastError {
            key: "unknown".to_string(),
            expected_type: "bool".to_string(),
            error: "expected true or false".to_string(),
        })
    }
}

impl FromTomlValue for PathBuf {
    fn from_toml_value(value: &str) -> Result<Self, ConfigError> {
        Ok(Self::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_conversions_are_strict() {
        assert_eq!(u8::from_toml_value("255").unwrap(), 255);
        assert!(u8::from_toml_value("256").is_err());
        assert!(bool::from_toml_value("true").unwrap());
        assert!(bool::from_toml_value("TRUE").is_err());
    }
}
