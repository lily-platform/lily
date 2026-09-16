//! Secret reference parsing and resolver abstractions.
//!
//! This module deliberately contains no vendor-specific provider implementation.
//! Applications can implement [`SecretResolver`] for Vault, AWS Secrets Manager,
//! Azure Key Vault, Kubernetes, or any other secret store.

use async_trait::async_trait;
use std::path::PathBuf;

use crate::ConfigError;

const REFERENCE_PREFIX: &str = "${";
const REFERENCE_SUFFIX: &str = "}";
const SECRET_SCHEME: &str = "secret:";
const FILE_SCHEME: &str = "file:";

/// A runtime configuration reference.
///
/// Exact references are resolved before the typed immutable snapshot is
/// published, without coupling consumers to a concrete provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConfigReference {
    /// A `${secret:<key>}` reference.
    Secret(String),
    /// An exact, bounded UTF-8 value loaded from an absolute secret file.
    File(PathBuf),
}

/// Parses a supported runtime configuration reference.
///
/// Exact `${secret:<key>}` and `${file:/absolute/path}` values are recognized.
/// Embedded, empty, or whitespace-padded references are intentionally
/// rejected.
pub(crate) fn parse_config_reference(value: &str) -> Option<ConfigReference> {
    let expression = value
        .strip_prefix(REFERENCE_PREFIX)?
        .strip_suffix(REFERENCE_SUFFIX)?;
    if let Some(key) = expression.strip_prefix(SECRET_SCHEME) {
        if key.is_empty() || key.trim() != key {
            return None;
        }
        return Some(ConfigReference::Secret(key.to_string()));
    }

    let path = expression.strip_prefix(FILE_SCHEME)?;
    if path.is_empty() || path.trim() != path {
        return None;
    }
    Some(ConfigReference::File(PathBuf::from(path)))
}

/// Resolves secret keys without making `ConfigService` provider-specific.
#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Resolves `key` to its runtime value.
    ///
    /// Implementations must never include the resolved value in logs or error
    /// messages.
    async fn resolve(&self, key: &str) -> Result<String, ConfigError>;

    /// Resolves a secret together with optional provider version/lease data.
    ///
    /// The default delegates to [`Self::resolve`] without version metadata.
    /// Providers that expose generations or leases should override this method
    /// so startup evidence can identify the resolved generation without
    /// exposing its value.
    async fn resolve_versioned(&self, key: &str) -> Result<ResolvedSecret, ConfigError> {
        self.resolve(key).await.map(ResolvedSecret::unversioned)
    }

    /// Stable, non-sensitive provider identifier used in startup evidence.
    fn provider_name(&self) -> &'static str {
        "custom"
    }
}

/// A resolved secret plus non-sensitive rotation/lease metadata.
///
/// `value` is intentionally private and has no `Debug` implementation. Config
/// loading consumes it immediately; snapshots expose only [`SecretBinding`]
/// metadata.
pub struct ResolvedSecret {
    value: String,
    version: Option<String>,
    lease_expires_at_unix_ms: Option<u64>,
}

impl ResolvedSecret {
    /// Creates a secret when the provider exposes no version or lease metadata.
    pub fn unversioned(value: String) -> Self {
        Self {
            value,
            version: None,
            lease_expires_at_unix_ms: None,
        }
    }

    /// Creates a versioned secret and optional absolute lease expiry.
    pub fn versioned(
        value: String,
        version: impl Into<String>,
        lease_expires_at_unix_ms: Option<u64>,
    ) -> Self {
        Self {
            value,
            version: Some(version.into()),
            lease_expires_at_unix_ms,
        }
    }

    pub(crate) fn into_parts(self) -> (String, Option<String>, Option<u64>) {
        (self.value, self.version, self.lease_expires_at_unix_ms)
    }
}

/// Non-sensitive evidence for one secret injected into the effective config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretBinding {
    /// Dot-notation configuration path whose value was resolved.
    pub config_key: String,
    /// Provider-specific secret identifier (never the resolved value).
    pub secret_key: String,
    /// Stable resolver/provider name.
    pub provider: String,
    /// Provider generation/version, when available.
    pub version: Option<String>,
    /// Absolute lease expiry, when supplied by the provider.
    pub lease_expires_at_unix_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_secret_reference() {
        assert_eq!(
            parse_config_reference("${secret:database.password}"),
            Some(ConfigReference::Secret("database.password".to_string()))
        );
    }

    #[test]
    fn ignores_regular_and_malformed_values() {
        for value in [
            "postgres",
            "prefix-${secret:database.password}",
            "${secret:}",
            "${secret: database.password}",
            "${env:DATABASE_PASSWORD}",
        ] {
            assert_eq!(parse_config_reference(value), None, "value: {value}");
        }
    }

    #[test]
    fn parses_exact_file_reference_without_treating_it_as_a_secret_provider_key() {
        assert_eq!(
            parse_config_reference("${file:/run/secrets/cache.url}"),
            Some(ConfigReference::File(PathBuf::from(
                "/run/secrets/cache.url"
            )))
        );
    }
}
