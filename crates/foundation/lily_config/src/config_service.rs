//! Strict, snapshot-based configuration loading.

use crate::secret::{ConfigReference, SecretBinding, SecretResolver, parse_config_reference};
use crate::{ConfigError, FromTomlValue, LilyConfig};
use async_trait::async_trait;
use lily_error::injection::InjectionError;
use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};

const CANONICAL_ENV_PREFIX: &str = "LILY__";
const LEGACY_ENV_PREFIX: &str = "LILY_";
const BOOTSTRAP_PATH_ENV: &str = "LILY_CONFIG_PATH";
const BOOTSTRAP_MODE_ENV: &str = "LILY_CONFIG_MODE";
const REDACTED: &str = "<redacted>";
const MAX_FILE_REFERENCE_BYTES: u64 = 8 * 1024;
const MAX_QUEUE_TRANSPORT_IDENTITY_BYTES: usize = 200;

fn queue_transport_identity_is_valid(value: &str) -> bool {
    (1..=MAX_QUEUE_TRANSPORT_IDENTITY_BYTES).contains(&value.len())
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

/// Runtime strictness used while producing an effective configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigMode {
    /// Local development; a missing file yields schema defaults and legacy
    /// environment names may be enabled for migration.
    Development,
    /// Deterministic tests with the same missing-file behavior as development.
    Test,
    /// Fail-closed startup: the explicit file must exist and migration-only
    /// syntax, unresolved protected references and unsupported values are
    /// rejected.
    Production,
}

impl fmt::Display for ConfigMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Development => "development",
            Self::Test => "test",
            Self::Production => "production",
        })
    }
}

/// Explicit source and validation policy for [`ConfigService`].
#[derive(Debug, Clone)]
pub struct ConfigOptions {
    path: PathBuf,
    mode: ConfigMode,
    allow_legacy_environment: bool,
    required_keys: Vec<String>,
}

impl ConfigOptions {
    /// Creates options for an explicit path and mode.
    pub fn new(path: impl Into<PathBuf>, mode: ConfigMode) -> Self {
        Self {
            path: path.into(),
            mode,
            allow_legacy_environment: mode != ConfigMode::Production,
            required_keys: Vec::new(),
        }
    }

    /// Creates development options for `path`.
    pub fn development(path: impl Into<PathBuf>) -> Self {
        Self::new(path, ConfigMode::Development)
    }

    /// Creates deterministic test options for `path`.
    pub fn test(path: impl Into<PathBuf>) -> Self {
        Self::new(path, ConfigMode::Test)
    }

    /// Creates fail-closed production options for `path`.
    pub fn production(path: impl Into<PathBuf>) -> Self {
        Self::new(path, ConfigMode::Production)
    }

    /// Reads the two bootstrap-only variables used by link-time DI factories:
    /// `LILY_CONFIG_PATH` and `LILY_CONFIG_MODE`.
    ///
    /// They must be provided together. When neither exists, development CWD
    /// defaults are returned for source compatibility.
    pub fn from_bootstrap_environment() -> Result<Self, ConfigError> {
        let path = std::env::var_os(BOOTSTRAP_PATH_ENV).map(PathBuf::from);
        let mode = std::env::var_os(BOOTSTRAP_MODE_ENV)
            .map(|value| {
                value.into_string().map_err(|_| {
                    ConfigError::ValidationError(format!(
                        "{BOOTSTRAP_MODE_ENV} must contain valid UTF-8"
                    ))
                })
            })
            .transpose()?;
        Self::from_bootstrap_values(path, mode.as_deref())
    }

    fn from_bootstrap_values(
        path: Option<PathBuf>,
        mode: Option<&str>,
    ) -> Result<Self, ConfigError> {
        match (path, mode) {
            (None, None) => Ok(Self::default()),
            (Some(path), Some(mode)) => {
                let mode = match mode.to_ascii_lowercase().as_str() {
                    "development" => ConfigMode::Development,
                    "test" => ConfigMode::Test,
                    "production" => ConfigMode::Production,
                    _ => {
                        return Err(ConfigError::ValidationError(format!(
                            "{BOOTSTRAP_MODE_ENV} must be development, test or production"
                        )));
                    }
                };
                Ok(Self::new(path, mode))
            }
            _ => Err(ConfigError::ValidationError(format!(
                "{BOOTSTRAP_PATH_ENV} and {BOOTSTRAP_MODE_ENV} must be provided together"
            ))),
        }
    }

    /// Enables or disables migration-only `LILY_SECTION_FIELD` matching.
    /// Canonical deployments should use `LILY__SECTION__FIELD`.
    pub fn with_legacy_environment(mut self, enabled: bool) -> Self {
        self.allow_legacy_environment = enabled;
        self
    }

    /// Declares an application-specific required dot-notation key.
    pub fn require_key(mut self, key: impl Into<String>) -> Self {
        self.required_keys.push(key.into());
        self
    }

    /// Returns the configured TOML path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the startup strictness mode.
    pub fn mode(&self) -> ConfigMode {
        self.mode
    }

    /// Returns application-required dot-notation keys.
    pub fn required_keys(&self) -> &[String] {
        &self.required_keys
    }
}

impl Default for ConfigOptions {
    fn default() -> Self {
        Self::development("lily.toml")
    }
}

/// Non-sensitive evidence describing one immutable effective config version.
#[derive(Debug, Clone)]
pub struct EffectiveConfigMetadata {
    /// Monotonic snapshot version. Lily V1 publishes version 1 at startup.
    pub version: u64,
    /// Configured source path.
    pub source_path: PathBuf,
    /// Startup strictness used to build the snapshot.
    pub mode: ConfigMode,
    /// Unix timestamp, in milliseconds, at which loading completed.
    pub loaded_at_unix_ms: u128,
    /// Whether the configured TOML file existed.
    pub file_present: bool,
    /// Sorted dot-notation keys overridden by the environment.
    pub environment_overrides: Vec<String>,
    /// Non-sensitive metadata for resolved secret and file references.
    pub secret_bindings: Vec<SecretBinding>,
    /// Sorted flattened keys represented in the effective snapshot.
    pub consumed_keys: Vec<String>,
    /// Sorted keys hidden by the operational redacted view.
    pub redacted_keys: Vec<String>,
    /// Non-fatal startup and migration notices.
    pub warnings: Vec<String>,
    /// Runtime reload is intentionally unsupported in Lily V1.
    pub reload_supported: bool,
}

/// One atomically published, immutable typed + dynamic configuration view.
pub struct ConfigSnapshot {
    config: LilyConfig,
    values: HashMap<String, String>,
    metadata: EffectiveConfigMetadata,
}

impl ConfigSnapshot {
    /// Returns this immutable snapshot's version.
    pub fn version(&self) -> u64 {
        self.metadata.version
    }

    /// Returns the typed effective configuration.
    ///
    /// This value can contain resolved secrets and must not be logged wholesale.
    pub fn config(&self) -> &LilyConfig {
        &self.config
    }

    /// Returns flattened effective values keyed by dot notation.
    ///
    /// This map can contain resolved secrets. Use [`Self::redacted_values`] for
    /// diagnostics and telemetry.
    pub fn values(&self) -> &HashMap<String, String> {
        &self.values
    }

    /// Returns non-value evidence describing how this snapshot was built.
    pub fn metadata(&self) -> &EffectiveConfigMetadata {
        &self.metadata
    }

    /// Produces a flattened copy with operationally sensitive values removed.
    pub fn redacted_values(&self) -> HashMap<String, String> {
        self.values
            .iter()
            .map(|(key, value)| {
                let resolved_secret = self
                    .metadata
                    .secret_bindings
                    .iter()
                    .any(|binding| binding.config_key == *key);
                let value = if is_operationally_sensitive_key(key) || resolved_secret {
                    REDACTED.to_string()
                } else {
                    value.clone()
                };
                (key.clone(), value)
            })
            .collect()
    }
}

impl fmt::Debug for ConfigSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigSnapshot")
            .field("metadata", &self.metadata)
            .field("value_count", &self.values.len())
            .finish_non_exhaustive()
    }
}

/// Safe operational view for diagnostics and startup evidence.
#[derive(Debug, Clone)]
pub struct RedactedEffectiveConfig {
    /// Flattened values with sensitive entries replaced by `<redacted>`.
    pub values: HashMap<String, String>,
    /// Non-sensitive evidence for the same immutable snapshot.
    pub metadata: EffectiveConfigMetadata,
}

/// Injectable configuration service.
///
/// Parsing and overlays happen off to the side. Readers see either the old
/// snapshot or the complete new snapshot; typed and flat values can never come
/// from different versions. Standalone callers must await [`Self::load`] before
/// reading values. The DI container invokes that lifecycle automatically.
#[derive(Injectable)]
#[service(lifetime = "Singleton")]
pub struct ConfigService {
    snapshot: Arc<RwLock<Arc<ConfigSnapshot>>>,
    load_gate: Arc<Mutex<()>>,
    options: ConfigOptions,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
    bootstrap_error: Option<ConfigError>,
}

impl ConfigService {
    /// Creates a service using an explicit config source/mode.
    pub fn new(options: ConfigOptions) -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(Arc::new(empty_snapshot(&options)))),
            load_gate: Arc::new(Mutex::new(())),
            options,
            secret_resolver: None,
            bootstrap_error: None,
        }
    }

    /// Creates a service with development startup policy.
    pub fn development(path: impl Into<PathBuf>) -> Self {
        Self::new(ConfigOptions::development(path))
    }

    /// Creates a service with fail-closed production startup policy.
    pub fn production(path: impl Into<PathBuf>) -> Self {
        Self::new(ConfigOptions::production(path))
    }

    /// Creates a service from the bootstrap path/mode environment and an
    /// application-owned secret resolver.
    pub fn with_secret_resolver<R>(resolver: R) -> Self
    where
        R: SecretResolver + 'static,
    {
        Self::with_shared_secret_resolver(Arc::new(resolver))
    }

    /// Creates a service from explicit options and an application-owned secret
    /// resolver.
    pub fn with_options_and_secret_resolver<R>(options: ConfigOptions, resolver: R) -> Self
    where
        R: SecretResolver + 'static,
    {
        Self::with_options_and_shared_secret_resolver(options, Arc::new(resolver))
    }

    /// Shared-resolver form of [`Self::with_secret_resolver`].
    pub fn with_shared_secret_resolver(resolver: Arc<dyn SecretResolver>) -> Self {
        match ConfigOptions::from_bootstrap_environment() {
            Ok(options) => Self::with_options_and_shared_secret_resolver(options, resolver),
            Err(error) => {
                let mut service = Self::with_options_and_shared_secret_resolver(
                    ConfigOptions::default(),
                    resolver,
                );
                service.bootstrap_error = Some(error);
                service
            }
        }
    }

    /// Shared-resolver form with explicit config options.
    pub fn with_options_and_shared_secret_resolver(
        options: ConfigOptions,
        resolver: Arc<dyn SecretResolver>,
    ) -> Self {
        let mut service = Self::new(options);
        service.secret_resolver = Some(resolver);
        service
    }

    /// Returns the immutable source and validation policy.
    pub fn options(&self) -> &ConfigOptions {
        &self.options
    }

    /// Loads and atomically publishes the effective startup configuration.
    pub async fn load(&self) -> Result<Arc<ConfigSnapshot>, ConfigError> {
        let _load_guard = self.load_gate.lock().await;
        let current = self.snapshot().await;
        if current.version() != 0 {
            // Startup configuration is immutable in V1. Repeated DI
            // initialization or concurrent callers observe the same Arc and
            // never re-read a partially changed file/environment.
            return Ok(current);
        }
        // Environment iterators are not `Send`; materialize them before this
        // future crosses any filesystem/secret-provider await point. Use the
        // OS-string iterator so an unrelated non-Unicode variable cannot panic
        // production startup. A Lily-owned key with a non-Unicode value is a
        // typed validation error instead of being silently ignored.
        let environment = materialize_environment(std::env::vars_os())?;
        let snapshot = Arc::new(self.build_snapshot(1, environment).await?);
        *self.snapshot.write().await = Arc::clone(&snapshot);
        Ok(snapshot)
    }

    /// Returns the currently published immutable version.
    ///
    /// Before standalone [`Self::load`] or DI initialization, this is the
    /// version-0 bootstrap snapshot rather than effective configuration.
    pub async fn snapshot(&self) -> Arc<ConfigSnapshot> {
        let snapshot = self.snapshot.read().await;
        Arc::clone(&snapshot)
    }

    /// Reads one flattened scalar value by dot-notation key.
    ///
    /// Nested tables and collections belong to the typed [`LilyConfig`] view.
    /// Missing keys and conversion failures are returned distinctly.
    pub async fn get<T>(&self, key: &str) -> Result<T, ConfigError>
    where
        T: FromTomlValue,
    {
        let snapshot = self.snapshot.read().await;
        let value = snapshot
            .values
            .get(key)
            .ok_or_else(|| ConfigError::KeyNotFound(key.to_string()))?;

        T::from_toml_value(value).map_err(|mut error| {
            if let ConfigError::TypeCastError { key: error_key, .. } = &mut error {
                *error_key = key.to_string();
            }
            error
        })
    }

    /// Reads an optional scalar, using `default` only when `key` is absent.
    ///
    /// A present but malformed value remains an error. Required startup values
    /// should instead be declared through [`ConfigOptions::require_key`].
    pub async fn get_or_default<T>(&self, key: &str, default: T) -> Result<T, ConfigError>
    where
        T: FromTomlValue,
    {
        match self.get(key).await {
            Ok(value) => Ok(value),
            Err(ConfigError::KeyNotFound(_)) => Ok(default),
            Err(error) => Err(error),
        }
    }

    /// Reads an optional scalar, returning `None` only when `key` is absent.
    pub async fn get_optional<T>(&self, key: &str) -> Result<Option<T>, ConfigError>
    where
        T: FromTomlValue,
    {
        match self.get(key).await {
            Ok(value) => Ok(Some(value)),
            Err(ConfigError::KeyNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Reports whether a flattened dot-notation key exists.
    pub async fn has_key(&self, key: &str) -> bool {
        self.snapshot.read().await.values.contains_key(key)
    }

    /// Returns all flattened keys in deterministic sorted order.
    pub async fn get_all_keys(&self) -> Vec<String> {
        let mut keys: Vec<_> = self.snapshot.read().await.values.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Returns a clone of the typed effective configuration.
    ///
    /// The value can contain resolved secrets and must not be logged wholesale.
    pub async fn get_lily_config(&self) -> LilyConfig {
        self.snapshot.read().await.config.clone()
    }

    /// Returns the diagnostics-safe flattened configuration and its metadata.
    pub async fn redacted_effective_config(&self) -> RedactedEffectiveConfig {
        let snapshot = self.snapshot().await;
        RedactedEffectiveConfig {
            values: snapshot.redacted_values(),
            metadata: snapshot.metadata.clone(),
        }
    }

    async fn build_snapshot<I>(
        &self,
        version: u64,
        environment: I,
    ) -> Result<ConfigSnapshot, ConfigError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        if let Some(error) = &self.bootstrap_error {
            return Err(error.clone());
        }

        let file_present = tokio::fs::try_exists(&self.options.path)
            .await
            .map_err(|error| {
                ConfigError::IoError(format!("cannot inspect config path: {error}"))
            })?;

        if !file_present && self.options.mode == ConfigMode::Production {
            return Err(ConfigError::IoError(format!(
                "production configuration file is missing: {}",
                self.options.path.display()
            )));
        }

        let mut warnings = Vec::new();
        let mut root = if file_present {
            let content = tokio::fs::read_to_string(&self.options.path)
                .await
                .map_err(|error| {
                    ConfigError::IoError(format!(
                        "cannot read configuration file {}: {error}",
                        self.options.path.display()
                    ))
                })?;
            parse_standard_toml(&content)?
        } else {
            warnings.push(format!(
                "configuration file {} is absent; development defaults are active",
                self.options.path.display()
            ));
            toml::Value::Table(toml::map::Map::new())
        };

        migrate_development_top_level(&mut root, self.options.mode, &mut warnings)?;
        let environment_overrides =
            self.apply_environment_overrides(&mut root, environment, &mut warnings)?;
        validate_required_keys(&root, &self.options.required_keys)?;
        let secret_bindings = self.resolve_config_references(&mut root).await?;
        normalize_custom_values(&mut root)?;

        let sensitive_values = collect_sensitive_values(&root);
        let config: LilyConfig = root.clone().try_into().map_err(|error| {
            if secret_bindings.is_empty() {
                ConfigError::ValidationError(format!(
                    "configuration schema validation failed: {}",
                    redact_literals(&error.to_string(), &sensitive_values)
                ))
            } else {
                ConfigError::ValidationError(
                    "configuration schema validation failed after resolving protected values"
                        .to_string(),
                )
            }
        })?;

        validate_config(&config, self.options.mode)?;

        let effective_tree = toml::Value::try_from(&config).map_err(|error| {
            ConfigError::SerializationError(format!(
                "cannot create effective configuration snapshot: {error}"
            ))
        })?;
        let mut values = HashMap::new();
        flatten_value(&effective_tree, "", &mut values);

        let mut consumed_keys: Vec<_> = values.keys().cloned().collect();
        consumed_keys.sort();
        let mut redacted_keys: Vec<_> = consumed_keys
            .iter()
            .filter(|key| {
                is_operationally_sensitive_key(key)
                    || secret_bindings
                        .iter()
                        .any(|binding| binding.config_key.as_str() == (*key).as_str())
            })
            .cloned()
            .collect();
        redacted_keys.sort();

        Ok(ConfigSnapshot {
            config,
            values,
            metadata: EffectiveConfigMetadata {
                version,
                source_path: self.options.path.clone(),
                mode: self.options.mode,
                loaded_at_unix_ms: now_unix_ms(),
                file_present,
                environment_overrides,
                secret_bindings,
                consumed_keys,
                redacted_keys,
                warnings,
                reload_supported: false,
            },
        })
    }

    fn apply_environment_overrides<I>(
        &self,
        root: &mut toml::Value,
        environment: I,
        warnings: &mut Vec<String>,
    ) -> Result<Vec<String>, ConfigError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let legacy_candidates = legacy_environment_candidates(root)?;
        let mut applied = Vec::new();
        let mut sources: HashMap<String, (bool, String)> = HashMap::new();
        let mut environment: Vec<_> = environment.into_iter().collect();
        // Legacy values are applied first. Canonical values deterministically
        // win when both forms are temporarily present during migration.
        environment.sort_by(|left, right| {
            let left_canonical = left.0.starts_with(CANONICAL_ENV_PREFIX);
            let right_canonical = right.0.starts_with(CANONICAL_ENV_PREFIX);
            left_canonical
                .cmp(&right_canonical)
                .then_with(|| left.0.cmp(&right.0))
        });

        for (name, raw_value) in environment {
            if name == BOOTSTRAP_PATH_ENV || name == BOOTSTRAP_MODE_ENV {
                continue;
            }
            let is_canonical = name.starts_with(CANONICAL_ENV_PREFIX);
            let path = if let Some(suffix) = name.strip_prefix(CANONICAL_ENV_PREFIX) {
                let segments: Vec<String> = suffix
                    .split("__")
                    .map(|segment| segment.to_ascii_lowercase())
                    .collect();
                if segments.is_empty()
                    || segments.iter().any(|segment| {
                        segment.is_empty()
                            || !segment.chars().all(|character| {
                                character.is_ascii_alphanumeric() || character == '_'
                            })
                    })
                {
                    return Err(ConfigError::ValidationError(format!(
                        "invalid canonical environment key {name}; use LILY__SECTION__FIELD"
                    )));
                }
                segments
            } else if let Some(suffix) = name.strip_prefix(LEGACY_ENV_PREFIX) {
                let normalized = suffix.to_ascii_lowercase();
                let Some(candidate) = legacy_candidates.get(&normalized) else {
                    // Other Lily subsystems may own legacy LILY_* keys;
                    // ConfigService must not consume them.
                    continue;
                };
                if !self.options.allow_legacy_environment {
                    return Err(ConfigError::ValidationError(format!(
                        "legacy environment key {name} is disabled; use LILY__{}",
                        candidate
                            .iter()
                            .map(|part| part.to_ascii_uppercase())
                            .collect::<Vec<_>>()
                            .join("__")
                    )));
                }
                warnings.push(format!(
                    "legacy environment key {name} was applied; migrate to LILY__{}",
                    candidate
                        .iter()
                        .map(|part| part.to_ascii_uppercase())
                        .collect::<Vec<_>>()
                        .join("__")
                ));
                candidate.clone()
            } else {
                continue;
            };

            if path.iter().any(|segment| segment.parse::<usize>().is_ok()) {
                return Err(ConfigError::ValidationError(format!(
                    "array-index environment overrides are unsupported in Lily V1: {name}"
                )));
            }

            let existing = value_at_path(root, &path);
            let value = parse_environment_value(&raw_value, existing, &path)?;
            let config_key = path.join(".");
            if let Some((previous_canonical, previous_name)) = sources.get(&config_key) {
                if *previous_canonical == is_canonical {
                    return Err(ConfigError::ValidationError(format!(
                        "multiple environment keys target {config_key}: {previous_name}, {name}"
                    )));
                }
                warnings.push(format!(
                    "canonical environment key {name} overrides legacy key {previous_name}"
                ));
            }
            set_value_at_path(root, &path, value)?;
            sources.insert(config_key.clone(), (is_canonical, name));
            applied.push(config_key);
        }

        applied.sort();
        applied.dedup();
        Ok(applied)
    }

    async fn resolve_config_references(
        &self,
        root: &mut toml::Value,
    ) -> Result<Vec<SecretBinding>, ConfigError> {
        let mut references = Vec::new();
        collect_config_references(root, &mut Vec::new(), &mut references, self.options.mode)?;

        if references.is_empty() {
            return Ok(Vec::new());
        }

        let mut bindings = Vec::with_capacity(references.len());
        for (path, reference) in references {
            match reference {
                ConfigReference::Secret(secret_key) => {
                    let resolver = self.secret_resolver.as_ref().ok_or_else(|| {
                        ConfigError::SecretResolveError {
                            key: secret_key.clone(),
                            error: "secret resolver is not configured".to_string(),
                        }
                    })?;
                    let resolved = resolver.resolve_versioned(&secret_key).await.map_err(|_| {
                        ConfigError::SecretResolveError {
                            key: secret_key.clone(),
                            error: "secret provider returned an error (details redacted)"
                                .to_string(),
                        }
                    })?;
                    let (value, version, lease_expires_at_unix_ms) = resolved.into_parts();

                    if lease_expires_at_unix_ms.is_some_and(|expiry| expiry <= now_unix_ms() as u64)
                    {
                        return Err(ConfigError::SecretResolveError {
                            key: secret_key,
                            error: "secret lease is already expired".to_string(),
                        });
                    }

                    set_value_at_path(root, &path, toml::Value::String(value))?;
                    bindings.push(SecretBinding {
                        config_key: path.join("."),
                        secret_key,
                        provider: resolver.provider_name().to_string(),
                        version,
                        lease_expires_at_unix_ms,
                    });
                }
                ConfigReference::File(file_path) => {
                    let value = read_bounded_file_reference(&file_path).await?;
                    set_value_at_path(root, &path, toml::Value::String(value))?;
                    bindings.push(SecretBinding {
                        config_key: path.join("."),
                        secret_key: file_path.display().to_string(),
                        provider: "file".to_string(),
                        version: None,
                        lease_expires_at_unix_ms: None,
                    });
                }
            }
        }

        bindings.sort_by(|left, right| left.config_key.cmp(&right.config_key));
        Ok(bindings)
    }
}

fn materialize_environment<I>(environment: I) -> Result<Vec<(String, String)>, ConfigError>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    let mut materialized = Vec::new();
    for (name, value) in environment {
        let Ok(name) = name.into_string() else {
            // A non-Unicode name cannot match Lily's ASCII configuration
            // namespace through the supported public contract.
            continue;
        };
        if name == BOOTSTRAP_PATH_ENV || name == BOOTSTRAP_MODE_ENV {
            // These two values have already been consumed by ConfigOptions.
            continue;
        }
        match value.into_string() {
            Ok(value) => materialized.push((name, value)),
            Err(_) if is_lily_environment_key(&name) => {
                return Err(ConfigError::ValidationError(format!(
                    "configuration environment key {name} must contain valid UTF-8"
                )));
            }
            Err(_) => {}
        }
    }
    Ok(materialized)
}

fn is_lily_environment_key(name: &str) -> bool {
    name.starts_with(CANONICAL_ENV_PREFIX) || name.starts_with(LEGACY_ENV_PREFIX)
}

#[async_trait]
impl ServiceTrait for ConfigService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.load()
            .await
            .map(|_| ())
            .map_err(|error| InjectionError::ServiceInitializationFailed {
                service: "ConfigService".to_string(),
                source: Box::new(InjectionError::ServiceNotFound(error.to_string())),
            })
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        Ok(())
    }
}

impl Default for ConfigService {
    fn default() -> Self {
        match ConfigOptions::from_bootstrap_environment() {
            Ok(options) => Self::new(options),
            Err(error) => {
                let mut service = Self::new(ConfigOptions::default());
                service.bootstrap_error = Some(error);
                service
            }
        }
    }
}

fn empty_snapshot(options: &ConfigOptions) -> ConfigSnapshot {
    ConfigSnapshot {
        config: LilyConfig::default(),
        values: HashMap::new(),
        metadata: EffectiveConfigMetadata {
            version: 0,
            source_path: options.path.clone(),
            mode: options.mode,
            loaded_at_unix_ms: 0,
            file_present: false,
            environment_overrides: Vec::new(),
            secret_bindings: Vec::new(),
            consumed_keys: Vec::new(),
            redacted_keys: Vec::new(),
            warnings: Vec::new(),
            reload_supported: false,
        },
    }
}

fn parse_standard_toml(content: &str) -> Result<toml::Value, ConfigError> {
    toml::from_str(content).map_err(|error: toml::de::Error| {
        let location = error
            .span()
            .map(|span| format!(" near byte range {}..{}", span.start, span.end))
            .unwrap_or_default();
        ConfigError::ParseError(format!(
            "invalid TOML{location}; no fallback parser is used"
        ))
    })
}

fn migrate_development_top_level(
    root: &mut toml::Value,
    mode: ConfigMode,
    warnings: &mut Vec<String>,
) -> Result<(), ConfigError> {
    let table = root.as_table_mut().ok_or_else(|| {
        ConfigError::ValidationError("configuration root must be a TOML table".to_string())
    })?;

    if table.contains_key("messagebroker") || table.contains_key("message_broker") {
        return Err(ConfigError::ValidationError(
            "legacy RabbitMQ consumer sections are unsupported; use canonical [rabbitmq.consumer]"
                .to_string(),
        ));
    }
    if table.contains_key("rabbitmq_topology") {
        return Err(ConfigError::ValidationError(
            "legacy [rabbitmq_topology] is unsupported; use canonical [rabbitmq.topology]"
                .to_string(),
        ));
    }

    if mode == ConfigMode::Production {
        return Ok(());
    }

    const KNOWN: &[&str] = &[
        "server",
        "lifecycle",
        "database",
        "postgresql",
        "clickhouse",
        "logging",
        "cache",
        "rabbitmq",
        "queue_client",
        "websocket",
        "websocket_client",
        "security",
        "http_client_factory",
        "custom",
    ];

    let unknown: Vec<_> = table
        .keys()
        .filter(|key| !KNOWN.contains(&key.as_str()))
        .cloned()
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }

    let mut migrated = toml::map::Map::new();
    for section in unknown {
        if let Some(value) = table.remove(&section) {
            let mut flattened = HashMap::new();
            flatten_value(&value, &section, &mut flattened);
            for (key, value) in flattened {
                migrated.insert(key, toml::Value::String(value));
            }
            warnings.push(format!(
                "legacy top-level [{section}] was moved into [custom]; production rejects it"
            ));
        }
    }

    let custom = table
        .entry("custom".to_string())
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
    let custom_table = custom
        .as_table_mut()
        .ok_or_else(|| ConfigError::ValidationError("[custom] must be a TOML table".to_string()))?;
    custom_table.extend(migrated);
    Ok(())
}

fn normalize_custom_values(root: &mut toml::Value) -> Result<(), ConfigError> {
    let Some(custom) = root.get_mut("custom") else {
        return Ok(());
    };
    let table = custom
        .as_table_mut()
        .ok_or_else(|| ConfigError::ValidationError("[custom] must be a TOML table".to_string()))?;
    for (_, value) in table.iter_mut() {
        if !value.is_str() {
            *value = toml::Value::String(value_to_string(value));
        }
    }
    Ok(())
}

fn legacy_environment_candidates(
    root: &toml::Value,
) -> Result<HashMap<String, Vec<String>>, ConfigError> {
    let mut values = HashMap::new();
    flatten_value(root, "", &mut values);
    let defaults = toml::Value::try_from(LilyConfig::default()).map_err(|error| {
        ConfigError::SerializationError(format!("cannot build environment schema: {error}"))
    })?;
    flatten_value(&defaults, "", &mut values);

    let mut candidates = HashMap::new();
    for key in values.keys() {
        let environment_key = key.replace('.', "_").to_ascii_lowercase();
        let path = key.split('.').map(str::to_string).collect::<Vec<_>>();
        if let Some(previous) = candidates.insert(environment_key.clone(), path.clone())
            && previous != path
        {
            return Err(ConfigError::ValidationError(format!(
                "legacy environment mapping is ambiguous for {environment_key}; use LILY__ delimiters"
            )));
        }
    }
    Ok(candidates)
}

fn parse_environment_value(
    raw: &str,
    existing: Option<&toml::Value>,
    path: &[String],
) -> Result<toml::Value, ConfigError> {
    if path.first().is_some_and(|part| part == "custom") {
        return Ok(toml::Value::String(raw.to_string()));
    }

    match existing {
        Some(toml::Value::String(_)) => Ok(toml::Value::String(raw.to_string())),
        Some(toml::Value::Integer(_)) => raw
            .parse::<i64>()
            .map(toml::Value::Integer)
            .map_err(|_| environment_type_error(path, "integer")),
        Some(toml::Value::Float(_)) => raw
            .parse::<f64>()
            .map(toml::Value::Float)
            .map_err(|_| environment_type_error(path, "float")),
        Some(toml::Value::Boolean(_)) => raw
            .parse::<bool>()
            .map(toml::Value::Boolean)
            .map_err(|_| environment_type_error(path, "boolean")),
        Some(toml::Value::Datetime(_)) => raw
            .parse::<toml::value::Datetime>()
            .map(toml::Value::Datetime)
            .map_err(|_| environment_type_error(path, "datetime")),
        Some(toml::Value::Array(_)) => parse_toml_literal(raw, path).and_then(|value| {
            if value.is_array() {
                Ok(value)
            } else {
                Err(environment_type_error(path, "array"))
            }
        }),
        Some(toml::Value::Table(_)) => Err(ConfigError::ValidationError(format!(
            "environment override cannot replace table {}",
            path.join(".")
        ))),
        None => {
            if let Ok(value) = parse_toml_literal(raw, path)
                && !value.is_table()
            {
                return Ok(value);
            }
            Ok(toml::Value::String(raw.to_string()))
        }
    }
}

fn parse_toml_literal(raw: &str, path: &[String]) -> Result<toml::Value, ConfigError> {
    let document: toml::Value = toml::from_str(&format!("value = {raw}"))
        .map_err(|_| environment_type_error(path, "valid TOML scalar"))?;
    document
        .get("value")
        .cloned()
        .ok_or_else(|| environment_type_error(path, "valid TOML scalar"))
}

fn environment_type_error(path: &[String], expected: &str) -> ConfigError {
    ConfigError::ValidationError(format!(
        "environment override for {} must be {expected}",
        path.join(".")
    ))
}

fn validate_required_keys(root: &toml::Value, required: &[String]) -> Result<(), ConfigError> {
    for key in required {
        let path: Vec<_> = key.split('.').map(str::to_string).collect();
        if value_at_path(root, &path).is_none() {
            return Err(ConfigError::ValidationError(format!(
                "required configuration key is missing: {key}"
            )));
        }
    }
    Ok(())
}

fn validate_config(config: &LilyConfig, mode: ConfigMode) -> Result<(), ConfigError> {
    if config.lifecycle.shutdown_timeout_secs == 0
        || config.lifecycle.shutdown_timeout_secs
            > crate::LifecycleConfig::MAX_SHUTDOWN_TIMEOUT_SECS
    {
        return Err(ConfigError::ValidationError(format!(
            "lifecycle.shutdown_timeout_secs must be between 1 and {} seconds",
            crate::LifecycleConfig::MAX_SHUTDOWN_TIMEOUT_SECS
        )));
    }

    validate_mode_and_cells(
        "database",
        config
            .database
            .as_ref()
            .and_then(|value| value.mode.as_deref()),
        config
            .database
            .as_ref()
            .and_then(|value| value.cells.as_ref()),
        |cell| &cell.name,
    )?;
    validate_mode_and_cells(
        "clickhouse",
        config
            .clickhouse
            .as_ref()
            .and_then(|value| value.mode.as_deref()),
        config
            .clickhouse
            .as_ref()
            .and_then(|value| value.cells.as_ref()),
        |cell| &cell.name,
    )?;
    validate_mode_and_cells(
        "cache",
        config
            .cache
            .as_ref()
            .and_then(|value| value.mode.as_deref()),
        config.cache.as_ref().and_then(|value| value.cells.as_ref()),
        |cell| &cell.name,
    )?;
    validate_mode_and_cells(
        "queue_client",
        config
            .queue_client
            .as_ref()
            .and_then(|value| value.mode.as_deref()),
        config
            .queue_client
            .as_ref()
            .and_then(|value| value.cells.as_ref()),
        |cell| &cell.name,
    )?;
    validate_mode_and_cells(
        "websocket_client",
        config
            .websocket_client
            .as_ref()
            .and_then(|value| value.mode.as_deref()),
        config
            .websocket_client
            .as_ref()
            .and_then(|value| value.cells.as_ref()),
        |cell| &cell.name,
    )?;
    validate_rabbitmq_topology(&config.rabbitmq.topology)?;
    if let Some(postgresql) = &config.postgresql {
        validate_postgresql(postgresql, mode)?;
    }
    if let Some(backplane) = config
        .websocket
        .as_ref()
        .and_then(|websocket| websocket.backplane.as_ref())
    {
        validate_redis_websocket_backplane(backplane, mode)?;
    }

    if let Some(factory) = &config.http_client_factory {
        validate_unique_names(
            "http_client_factory.clients",
            factory.clients.keys().map(String::as_str),
        )?;
    }

    if mode == ConfigMode::Production {
        if let Some(cache) = &config.cache {
            if cache
                .provider
                .as_deref()
                .is_some_and(|provider| !matches!(provider, "redis" | "none"))
            {
                return Err(ConfigError::ValidationError(
                    "only the Redis cache provider is supported in the Lily V1 production profile"
                        .to_string(),
                ));
            }
            if let Some(cells) = &cache.cells
                && cells.iter().any(|cell| cell.provider != "redis")
            {
                return Err(ConfigError::ValidationError(
                    "only Redis cache cells are supported in the Lily V1 production profile"
                        .to_string(),
                ));
            }
        }
        if let Some(consumer) = &config.rabbitmq.consumer {
            validate_rabbitmq_source(
                "rabbitmq.consumer",
                consumer.connection_string.as_deref(),
                consumer.username.as_deref(),
                consumer.password.as_deref(),
                consumer.hostname.as_deref(),
                consumer.port,
                consumer.vhost.as_deref(),
                consumer.use_tls,
                &consumer.tls,
            )?;
            if config.rabbitmq.topology.queues.is_empty() {
                return Err(ConfigError::ValidationError(
                    "rabbitmq.topology.queues must be non-empty for the RabbitMQ consumer profile"
                        .into(),
                ));
            }
        }
        if let Some(client) = &config.queue_client {
            match client.mode.as_deref().unwrap_or("single") {
                "single" => {
                    validate_rabbitmq_source(
                        "queue_client",
                        client.connection_string.as_deref(),
                        client.username.as_deref(),
                        client.password.as_deref(),
                        client.hostname.as_deref(),
                        client.port,
                        client.vhost.as_deref(),
                        client.use_tls,
                        &client.tls,
                    )?;
                }
                "factory" => {
                    for cell in client.cells.as_deref().unwrap_or_default() {
                        validate_rabbitmq_source(
                            &format!("queue_client.cells.{}", cell.name),
                            cell.connection_string.as_deref(),
                            cell.username.as_deref(),
                            cell.password.as_deref(),
                            cell.hostname.as_deref(),
                            cell.port,
                            cell.vhost.as_deref(),
                            cell.use_tls,
                            &cell.tls,
                        )?;
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}

fn validate_rabbitmq_topology(topology: &crate::RabbitMqTopologyConfig) -> Result<(), ConfigError> {
    validate_unique_names(
        "rabbitmq.topology.queues",
        topology.queues.iter().map(|queue| queue.name.as_str()),
    )?;

    let mut exchanges = HashMap::new();
    for queue in &topology.queues {
        for (field, value) in [
            ("name", Some(queue.name.as_str())),
            ("exchange_name", Some(queue.exchange_name.as_str())),
            ("routing_key", Some(queue.routing_key.as_str())),
            (
                "dead_letter_exchange",
                queue.dead_letter_exchange.as_deref(),
            ),
            (
                "dead_letter_routing_key",
                queue.dead_letter_routing_key.as_deref(),
            ),
        ] {
            if value.is_some_and(|value| !queue_transport_identity_is_valid(value)) {
                return Err(ConfigError::ValidationError(format!(
                    "rabbitmq.topology queue {field} must contain 1..={MAX_QUEUE_TRANSPORT_IDENTITY_BYTES} trimmed, control-free bytes"
                )));
            }
        }

        #[cfg(any(
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-postgresql"
        ))]
        if let Some(binding) = queue.transactional_inbox.as_ref() {
            validate_transactional_inbox(&queue.name, binding)?;
        }

        if let Some((exchange_kind, ownership)) = exchanges.insert(
            queue.exchange_name.as_str(),
            (queue.exchange_kind, queue.topology_ownership),
        ) && (exchange_kind != queue.exchange_kind || ownership != queue.topology_ownership)
        {
            return Err(ConfigError::ValidationError(format!(
                "rabbitmq.topology exchange {:?} has conflicting exchange_kind or topology_ownership",
                queue.exchange_name
            )));
        }

        let retention = queue.retention.as_ref().ok_or_else(|| {
            ConfigError::ValidationError(format!(
                "rabbitmq.topology queue {:?} must define explicit retention bounds",
                queue.name
            ))
        })?;
        for (field, value) in [
            ("main_max_messages", retention.main_max_messages),
            ("main_max_bytes", retention.main_max_bytes),
            (
                "retry_bucket_max_messages",
                retention.retry_bucket_max_messages,
            ),
            ("retry_bucket_max_bytes", retention.retry_bucket_max_bytes),
            (
                "dead_letter_max_messages",
                retention.dead_letter_max_messages,
            ),
            ("dead_letter_max_bytes", retention.dead_letter_max_bytes),
        ] {
            if value == 0 || value > i64::MAX as u64 {
                return Err(ConfigError::ValidationError(format!(
                    "rabbitmq.topology queue {:?} retention.{field} must be between 1 and {}",
                    queue.name,
                    i64::MAX
                )));
            }
        }

        match queue.queue_type {
            crate::RabbitMqQueueType::Classic => {
                if queue
                    .max_priority
                    .is_some_and(|priority| !(1..=16).contains(&priority))
                {
                    return Err(ConfigError::ValidationError(format!(
                        "rabbitmq.topology classic queue {:?} max_priority must be between 1 and 16",
                        queue.name
                    )));
                }
            }
            crate::RabbitMqQueueType::Quorum => {
                if !queue.durable || queue.exclusive || queue.auto_delete {
                    return Err(ConfigError::ValidationError(format!(
                        "rabbitmq.topology quorum queue {:?} must be durable, non-exclusive and non-auto-delete",
                        queue.name
                    )));
                }
                if queue.max_priority.is_some() {
                    return Err(ConfigError::ValidationError(format!(
                        "rabbitmq.topology quorum queue {:?} cannot declare max_priority",
                        queue.name
                    )));
                }
            }
        }
    }

    crate::RabbitMqTopologyPlan::compile(topology)
        .map_err(|error| ConfigError::ValidationError(error.to_string()))?;

    Ok(())
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-postgresql"
))]
fn validate_transactional_inbox(
    queue_name: &str,
    config: &crate::TransactionalInboxConfig,
) -> Result<(), ConfigError> {
    config.validate_contract().map_err(|rule| {
        ConfigError::ValidationError(format!(
            "rabbitmq.topology queue {queue_name:?} transactional_inbox violates canonical rule {rule}"
        ))
    })
}

fn validate_redis_websocket_backplane(
    config: &crate::RedisWebSocketBackplaneConfig,
    mode: ConfigMode,
) -> Result<(), ConfigError> {
    const SECTION: &str = "websocket.backplane";

    if config.redis_url.is_empty()
        || config.redis_url.len() > 4096
        || config.redis_url.contains('\0')
    {
        return Err(ConfigError::ValidationError(format!(
            "{SECTION}.redis_url is invalid"
        )));
    }
    let url = url::Url::parse(&config.redis_url)
        .map_err(|_| ConfigError::ValidationError(format!("{SECTION}.redis_url is invalid")))?;
    if url.host_str().is_none() || url.fragment().is_some() {
        return Err(ConfigError::ValidationError(format!(
            "{SECTION}.redis_url must contain a host and cannot use insecure fragments"
        )));
    }
    let expected_scheme = if config.use_tls { "rediss" } else { "redis" };
    if url.scheme() != expected_scheme {
        return Err(ConfigError::ValidationError(format!(
            "{SECTION}.use_tls={} requires {expected_scheme}://",
            config.use_tls
        )));
    }
    if config.custom_ca_bundle.is_some() && !config.use_tls {
        return Err(ConfigError::ValidationError(format!(
            "{SECTION}.custom_ca_bundle cannot be used when TLS is disabled"
        )));
    }
    if let Some(path) = &config.custom_ca_bundle
        && !path.is_absolute()
    {
        return Err(ConfigError::ValidationError(format!(
            "{SECTION}.custom_ca_bundle must be an absolute path"
        )));
    }
    if mode == ConfigMode::Production {
        if !config.use_tls {
            return Err(ConfigError::ValidationError(format!(
                "{SECTION}.use_tls must be true in production"
            )));
        }
        if url.password().is_none_or(str::is_empty) {
            return Err(ConfigError::ValidationError(format!(
                "{SECTION}.redis_url must contain an ACL credential in production"
            )));
        }
    }

    for (field, value) in [
        (
            "application_namespace",
            config.application_namespace.as_str(),
        ),
        (
            "environment_namespace",
            config.environment_namespace.as_str(),
        ),
        ("channel_namespace", config.channel_namespace.as_str()),
    ] {
        if !is_redis_websocket_namespace(value) {
            return Err(ConfigError::ValidationError(format!(
                "{SECTION}.{field} must contain 1..=64 ASCII alphanumeric, '-' or '_' characters; '.' is reserved as the channel delimiter"
            )));
        }
    }

    if !(1..=1024).contains(&config.publish_capacity) {
        return Err(ConfigError::ValidationError(format!(
            "{SECTION}.publish_capacity must be between 1 and 1024"
        )));
    }
    if !(1..=4096).contains(&config.ingress_capacity) {
        return Err(ConfigError::ValidationError(format!(
            "{SECTION}.ingress_capacity must be between 1 and 4096"
        )));
    }
    for (field, value, minimum, maximum) in [
        (
            "connection_timeout_millis",
            config.connection_timeout_millis,
            100,
            120_000,
        ),
        (
            "operation_timeout_millis",
            config.operation_timeout_millis,
            10,
            300_000,
        ),
        (
            "reconnect_initial_delay_millis",
            config.reconnect_initial_delay_millis,
            10,
            60_000,
        ),
        (
            "reconnect_max_delay_millis",
            config.reconnect_max_delay_millis,
            config.reconnect_initial_delay_millis,
            300_000,
        ),
    ] {
        if value < minimum || value > maximum {
            return Err(ConfigError::ValidationError(format!(
                "{SECTION}.{field} must be between {minimum} and {maximum} milliseconds"
            )));
        }
    }
    if !config.reconnect_jitter_ratio.is_finite()
        || !(0.0..=1.0).contains(&config.reconnect_jitter_ratio)
    {
        return Err(ConfigError::ValidationError(format!(
            "{SECTION}.reconnect_jitter_ratio must be a finite value between 0.0 and 1.0"
        )));
    }
    Ok(())
}

fn is_redis_websocket_namespace(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn validate_postgresql(config: &crate::PgConfig, mode: ConfigMode) -> Result<(), ConfigError> {
    match config.mode {
        crate::PgMode::Single => {
            if config
                .connection_string
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(ConfigError::ValidationError(
                    "postgresql.connection_string is required in single mode".into(),
                ));
            }
            if !config.cells.is_empty() {
                return Err(ConfigError::ValidationError(
                    "postgresql.cells is only valid in factory mode".into(),
                ));
            }
            validate_postgresql_pool("postgresql.pool", &config.pool)?;
            validate_postgresql_tls("postgresql.tls", &config.tls, mode)?;
        }
        crate::PgMode::Factory => {
            if config.connection_string.is_some() {
                return Err(ConfigError::ValidationError(
                    "postgresql.connection_string is forbidden in factory mode".into(),
                ));
            }
            if config.cells.is_empty() {
                return Err(ConfigError::ValidationError(
                    "postgresql.cells must be non-empty in factory mode".into(),
                ));
            }
            validate_unique_names(
                "postgresql.cells",
                config.cells.iter().map(|cell| cell.name.as_str()),
            )?;
            for cell in &config.cells {
                let section = format!("postgresql.cells.{}", cell.name);
                if cell.connection_string.trim().is_empty() {
                    return Err(ConfigError::ValidationError(format!(
                        "{section}.connection_string is required"
                    )));
                }
                validate_postgresql_pool(&format!("{section}.pool"), &cell.pool)?;
                validate_postgresql_tls(&format!("{section}.tls"), &cell.tls, mode)?;
            }
        }
    }
    Ok(())
}

fn validate_postgresql_pool(section: &str, pool: &crate::PgPoolConfig) -> Result<(), ConfigError> {
    if pool.max_size == 0 {
        return Err(ConfigError::ValidationError(format!(
            "{section}.max_size must be greater than zero"
        )));
    }
    for (field, value) in [
        ("connect_timeout_secs", pool.connect_timeout_secs),
        ("acquire_timeout_secs", pool.acquire_timeout_secs),
        ("recycle_timeout_secs", pool.recycle_timeout_secs),
        (
            "transaction_cleanup_timeout_secs",
            pool.transaction_cleanup_timeout_secs,
        ),
        ("shutdown_timeout_secs", pool.shutdown_timeout_secs),
    ] {
        if value == 0 {
            return Err(ConfigError::ValidationError(format!(
                "{section}.{field} must be greater than zero"
            )));
        }
    }
    Ok(())
}

fn validate_postgresql_tls(
    section: &str,
    tls: &crate::PgTlsConfig,
    mode: ConfigMode,
) -> Result<(), ConfigError> {
    if tls.mode == crate::PgTlsMode::Disable && tls.additional_ca_bundle.is_some() {
        return Err(ConfigError::ValidationError(format!(
            "{section}.additional_ca_bundle cannot be used when TLS is disabled"
        )));
    }
    if mode == ConfigMode::Production && tls.mode != crate::PgTlsMode::VerifyFull {
        return Err(ConfigError::ValidationError(format!(
            "{section}.mode must be verify-full in production"
        )));
    }
    if let Some(path) = &tls.additional_ca_bundle
        && !path.is_absolute()
    {
        return Err(ConfigError::ValidationError(format!(
            "{section}.additional_ca_bundle must be an absolute path"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_rabbitmq_source(
    section: &str,
    connection_string: Option<&str>,
    username: Option<&str>,
    password: Option<&str>,
    hostname: Option<&str>,
    port: Option<u16>,
    vhost: Option<&str>,
    use_tls: Option<bool>,
    tls: &crate::RabbitMqTlsConfig,
) -> Result<(), ConfigError> {
    let use_tls = use_tls.ok_or_else(|| {
        ConfigError::ValidationError(format!(
            "{section}.use_tls must be explicit so AMQP cannot silently downgrade"
        ))
    })?;
    let expected_scheme = if use_tls { "amqps" } else { "amqp" };
    if let Some(connection_string) = connection_string {
        if username.is_some()
            || password.is_some()
            || hostname.is_some()
            || port.is_some()
            || vhost.is_some()
        {
            return Err(ConfigError::ValidationError(format!(
                "{section}.connection_string cannot be combined with individual RabbitMQ connection fields"
            )));
        }
        let url = url::Url::parse(connection_string).map_err(|_| {
            ConfigError::ValidationError(format!("{section}.connection_string is invalid"))
        })?;
        if url.scheme() != expected_scheme {
            return Err(ConfigError::ValidationError(format!(
                "{section}.use_tls={use_tls} requires {expected_scheme}://"
            )));
        }
        if url.username().is_empty() || url.password().is_none() || url.host_str().is_none() {
            return Err(ConfigError::ValidationError(format!(
                "{section}.connection_string requires username, password and host"
            )));
        }
        if url.username().eq_ignore_ascii_case("guest")
            || url
                .password()
                .is_some_and(|password| password.eq_ignore_ascii_case("guest"))
        {
            return Err(ConfigError::ValidationError(format!(
                "{section} cannot use RabbitMQ guest credentials in production"
            )));
        }
    } else {
        let username = username.filter(|value| !value.is_empty()).ok_or_else(|| {
            ConfigError::ValidationError(format!("{section}.username is required"))
        })?;
        let password = password.filter(|value| !value.is_empty()).ok_or_else(|| {
            ConfigError::ValidationError(format!("{section}.password is required"))
        })?;
        if hostname.is_none_or(str::is_empty) {
            return Err(ConfigError::ValidationError(format!(
                "{section}.hostname is required"
            )));
        }
        if username.eq_ignore_ascii_case("guest") || password.eq_ignore_ascii_case("guest") {
            return Err(ConfigError::ValidationError(format!(
                "{section} cannot use RabbitMQ guest credentials in production"
            )));
        }
    }
    validate_rabbitmq_tls(section, use_tls, tls)
}

fn validate_rabbitmq_tls(
    section: &str,
    use_tls: bool,
    tls: &crate::RabbitMqTlsConfig,
) -> Result<(), ConfigError> {
    let has_tls_material = tls.additional_ca_bundle.is_some()
        || tls.client_certificate_chain.is_some()
        || tls.client_private_key.is_some();
    if !use_tls && has_tls_material {
        return Err(ConfigError::ValidationError(format!(
            "{section}.tls cannot be configured when use_tls=false"
        )));
    }
    if tls.client_certificate_chain.is_some() != tls.client_private_key.is_some() {
        return Err(ConfigError::ValidationError(format!(
            "{section}.tls requires both client_certificate_chain and client_private_key"
        )));
    }
    for (field, path) in [
        ("additional_ca_bundle", tls.additional_ca_bundle.as_ref()),
        (
            "client_certificate_chain",
            tls.client_certificate_chain.as_ref(),
        ),
        ("client_private_key", tls.client_private_key.as_ref()),
    ] {
        if path.is_some_and(|path| !path.is_absolute()) {
            return Err(ConfigError::ValidationError(format!(
                "{section}.tls.{field} must be an absolute path"
            )));
        }
    }
    Ok(())
}

fn validate_mode_and_cells<T, F>(
    section: &str,
    mode: Option<&str>,
    cells: Option<&Vec<T>>,
    name: F,
) -> Result<(), ConfigError>
where
    F: Fn(&T) -> &String,
{
    if mode.is_some_and(|value| !matches!(value, "single" | "factory")) {
        return Err(ConfigError::ValidationError(format!(
            "{section}.mode must be single or factory"
        )));
    }
    if mode == Some("factory") && cells.is_none_or(Vec::is_empty) {
        return Err(ConfigError::ValidationError(format!(
            "{section}.cells must be non-empty in factory mode"
        )));
    }
    if mode != Some("factory") && cells.is_some() {
        return Err(ConfigError::ValidationError(format!(
            "{section}.cells is only valid in factory mode"
        )));
    }
    if let Some(cells) = cells {
        validate_unique_names(
            &format!("{section}.cells"),
            cells.iter().map(|cell| name(cell).as_str()),
        )?;
    }
    Ok(())
}

fn validate_unique_names<'a>(
    section: &str,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), ConfigError> {
    let mut seen = HashSet::new();
    for name in names {
        if name.trim().is_empty() {
            return Err(ConfigError::ValidationError(format!(
                "{section} contains an empty name"
            )));
        }
        if !seen.insert(name) {
            return Err(ConfigError::ValidationError(format!(
                "{section} contains duplicate name '{name}'"
            )));
        }
    }
    Ok(())
}

fn collect_config_references(
    value: &toml::Value,
    path: &mut Vec<String>,
    references: &mut Vec<(Vec<String>, ConfigReference)>,
    mode: ConfigMode,
) -> Result<(), ConfigError> {
    match value {
        toml::Value::Table(table) => {
            for (key, value) in table {
                path.push(key.clone());
                collect_config_references(value, path, references, mode)?;
                path.pop();
            }
        }
        toml::Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                path.push(index.to_string());
                collect_config_references(value, path, references, mode)?;
                path.pop();
            }
        }
        toml::Value::String(value) => {
            if let Some(reference) = parse_config_reference(value) {
                if is_rabbitmq_topology_identity_path(path) {
                    return Err(ConfigError::ValidationError(format!(
                        "protected config references are not permitted for RabbitMQ topology identity at {}",
                        path.join(".")
                    )));
                }
                if matches!(
                    path.as_slice(),
                    [section, subsection, field]
                        if section == "websocket"
                            && subsection == "backplane"
                            && field == "custom_ca_bundle"
                ) {
                    return Err(ConfigError::ValidationError(
                        "websocket.backplane.custom_ca_bundle must be a direct absolute path, not a config reference"
                            .to_owned(),
                    ));
                }
                references.push((path.clone(), reference));
            } else if mode == ConfigMode::Production && value.contains("${") {
                return Err(ConfigError::ValidationError(format!(
                    "unsupported or malformed config reference at {}",
                    path.join(".")
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_rabbitmq_topology_identity_path(path: &[String]) -> bool {
    matches!(
        path,
        [rabbitmq, topology, queues, _index, field]
            if rabbitmq == "rabbitmq"
                && topology == "topology"
                && queues == "queues"
                && matches!(
                    field.as_str(),
                    "name"
                        | "exchange_name"
                        | "routing_key"
                        | "dead_letter_exchange"
                        | "dead_letter_routing_key"
                )
    )
}

async fn read_bounded_file_reference(path: &Path) -> Result<String, ConfigError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(ConfigError::ValidationError(
            "file configuration references must use an absolute normalized path".to_string(),
        ));
    }
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|_| {
        ConfigError::IoError(format!(
            "cannot inspect file configuration reference {}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ConfigError::ValidationError(format!(
            "file configuration reference must be a regular non-symlink file: {}",
            path.display()
        )));
    }
    if metadata.len() == 0 || metadata.len() > MAX_FILE_REFERENCE_BYTES {
        return Err(ConfigError::ValidationError(format!(
            "file configuration reference is outside the 1..={MAX_FILE_REFERENCE_BYTES} byte bound: {}",
            path.display()
        )));
    }
    let canonical = tokio::fs::canonicalize(path).await.map_err(|_| {
        ConfigError::IoError(format!(
            "cannot canonicalize file configuration reference {}",
            path.display()
        ))
    })?;
    if canonical != path {
        return Err(ConfigError::ValidationError(format!(
            "file configuration reference must use its canonical path: {}",
            path.display()
        )));
    }
    let bytes = tokio::fs::read(path).await.map_err(|_| {
        ConfigError::IoError(format!(
            "cannot read file configuration reference {}",
            path.display()
        ))
    })?;
    let mut value = String::from_utf8(bytes).map_err(|_| {
        ConfigError::ValidationError(format!(
            "file configuration reference must contain UTF-8 text: {}",
            path.display()
        ))
    })?;
    if value.ends_with("\r\n") {
        value.truncate(value.len() - 2);
    } else if value.ends_with('\n') {
        value.pop();
    }
    if value.is_empty() || value.contains(['\r', '\n', '\0']) {
        return Err(ConfigError::ValidationError(format!(
            "file configuration reference must contain exactly one non-empty line: {}",
            path.display()
        )));
    }
    Ok(value)
}

fn value_at_path<'a>(root: &'a toml::Value, path: &[String]) -> Option<&'a toml::Value> {
    let mut current = root;
    for segment in path {
        current = match current {
            toml::Value::Table(table) => table.get(segment)?,
            toml::Value::Array(values) => values.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

fn set_value_at_path(
    root: &mut toml::Value,
    path: &[String],
    value: toml::Value,
) -> Result<(), ConfigError> {
    if path.is_empty() {
        return Err(ConfigError::ValidationError(
            "configuration path cannot be empty".to_string(),
        ));
    }
    set_value_at_path_inner(root, path, value)
}

fn set_value_at_path_inner(
    current: &mut toml::Value,
    path: &[String],
    value: toml::Value,
) -> Result<(), ConfigError> {
    if path.len() == 1 {
        return match current {
            toml::Value::Table(table) => {
                table.insert(path[0].clone(), value);
                Ok(())
            }
            toml::Value::Array(values) => {
                let index = path[0].parse::<usize>().map_err(|_| {
                    ConfigError::ValidationError(format!(
                        "configuration array index is invalid: {}",
                        path[0]
                    ))
                })?;
                let target = values.get_mut(index).ok_or_else(|| {
                    ConfigError::ValidationError(format!(
                        "configuration array index is out of bounds: {index}"
                    ))
                })?;
                *target = value;
                Ok(())
            }
            _ => Err(ConfigError::ValidationError(
                "configuration path crosses a scalar value".to_string(),
            )),
        };
    }

    let next = match current {
        toml::Value::Table(table) => table
            .entry(path[0].clone())
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new())),
        toml::Value::Array(values) => {
            let index = path[0].parse::<usize>().map_err(|_| {
                ConfigError::ValidationError(format!(
                    "configuration array index is invalid: {}",
                    path[0]
                ))
            })?;
            values.get_mut(index).ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "configuration array index is out of bounds: {index}"
                ))
            })?
        }
        _ => {
            return Err(ConfigError::ValidationError(format!(
                "configuration path crosses non-container value at {}",
                path[0]
            )));
        }
    };
    set_value_at_path_inner(next, &path[1..], value)
}

fn flatten_value(value: &toml::Value, prefix: &str, output: &mut HashMap<String, String>) {
    match value {
        toml::Value::Table(table) => {
            for (key, value) in table {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_value(value, &path, output);
            }
        }
        toml::Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                flatten_value(value, &format!("{prefix}.{index}"), output);
            }
            if values.is_empty() && !prefix.is_empty() {
                output.insert(prefix.to_string(), "[]".to_string());
            }
        }
        _ if !prefix.is_empty() => {
            output.insert(prefix.to_string(), value_to_string(value));
        }
        _ => {}
    }
}

fn value_to_string(value: &toml::Value) -> String {
    match value {
        toml::Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    normalized.split('.').any(|part| {
        part == "password"
            || part.contains("secret")
            || part.contains("token")
            || part.contains("api_key")
            || part.contains("private_key")
            || part.contains("credential")
            || part == "connection_string"
            || part == "redis_url"
            || part == "custom_ca_bundle"
            || part == "additional_ca_bundle"
            || part == "client_certificate_chain"
    })
}

fn is_operationally_sensitive_key(key: &str) -> bool {
    if is_sensitive_key(key) {
        return true;
    }

    let Some(client_path) = key.strip_prefix("http_client_factory.clients.") else {
        return false;
    };

    client_path.ends_with(".base_address") || client_path.contains(".headers.")
}

fn collect_sensitive_values(root: &toml::Value) -> Vec<String> {
    let mut values = HashMap::new();
    flatten_value(root, "", &mut values);
    values
        .into_iter()
        .filter_map(|(key, value)| is_operationally_sensitive_key(&key).then_some(value))
        .filter(|value| !value.is_empty())
        .collect()
}

fn redact_literals(message: &str, sensitive_values: &[String]) -> String {
    sensitive_values
        .iter()
        .fold(message.to_string(), |message, value| {
            message.replace(value, REDACTED)
        })
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::ResolvedSecret;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn lifecycle_shutdown_timeout_has_an_inclusive_timer_bound() {
        let maximum = crate::LifecycleConfig::MAX_SHUTDOWN_TIMEOUT_SECS;
        let exact = LilyConfig {
            lifecycle: crate::LifecycleConfig {
                shutdown_timeout_secs: maximum,
            },
            ..LilyConfig::default()
        };
        validate_config(&exact, ConfigMode::Development)
            .expect("the documented inclusive shutdown maximum must validate");

        for shutdown_timeout_secs in [0, maximum + 1, u64::MAX] {
            let invalid = LilyConfig {
                lifecycle: crate::LifecycleConfig {
                    shutdown_timeout_secs,
                },
                ..LilyConfig::default()
            };
            assert!(matches!(
                validate_config(&invalid, ConfigMode::Development),
                Err(ConfigError::ValidationError(message))
                    if message.contains("lifecycle.shutdown_timeout_secs")
            ));
        }
    }

    #[test]
    fn http_listener_and_tls_validation_are_deferred_to_the_effective_adapter() {
        let config = LilyConfig {
            server: crate::ServerConfig {
                port: 0,
                tls_enabled: Some(true),
                ..crate::ServerConfig::default()
            },
            ..LilyConfig::default()
        };

        validate_config(&config, ConfigMode::Production)
            .expect("builder TLS precedence must be resolved by the HTTP adapter");
    }

    #[test]
    fn queue_retention_is_explicit_and_non_zero_in_every_mode() {
        let queue = crate::QueueDefinition {
            name: "orders".into(),
            exchange_name: "orders".into(),
            routing_key: "orders".into(),
            ..crate::QueueDefinition::default()
        };
        let config = LilyConfig {
            rabbitmq: crate::RabbitMqConfig {
                topology: crate::RabbitMqTopologyConfig {
                    queues: vec![queue.clone()],
                },
                ..crate::RabbitMqConfig::default()
            },
            ..LilyConfig::default()
        };
        assert!(matches!(
            validate_config(&config, ConfigMode::Development),
            Err(ConfigError::ValidationError(message)) if message.contains("explicit retention")
        ));

        let mut queue = queue;
        queue.retention = Some(crate::QueueRetentionConfig {
            main_max_messages: 1,
            main_max_bytes: 1,
            retry_bucket_max_messages: 1,
            retry_bucket_max_bytes: 1,
            dead_letter_max_messages: 1,
            dead_letter_max_bytes: 0,
        });
        let config = LilyConfig {
            rabbitmq: crate::RabbitMqConfig {
                topology: crate::RabbitMqTopologyConfig {
                    queues: vec![queue],
                },
                ..crate::RabbitMqConfig::default()
            },
            ..LilyConfig::default()
        };
        assert!(matches!(
            validate_config(&config, ConfigMode::Development),
            Err(ConfigError::ValidationError(message))
                if message.contains("retention.dead_letter_max_bytes")
        ));
    }

    #[test]
    fn queue_transport_identities_are_validated_before_publication() {
        let retained = crate::QueueRetentionConfig {
            main_max_messages: 1,
            main_max_bytes: 1,
            retry_bucket_max_messages: 1,
            retry_bucket_max_bytes: 1,
            dead_letter_max_messages: 1,
            dead_letter_max_bytes: 1,
        };
        let exact_queue = "q".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES - ".dlq.v2".len());
        let exact_exchange = "e".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES - ".retry.v2".len());
        let exact_routing = "r".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES);
        let exact_dead_letter = "d".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES);
        let valid = LilyConfig {
            rabbitmq: crate::RabbitMqConfig {
                topology: crate::RabbitMqTopologyConfig {
                    queues: vec![crate::QueueDefinition {
                        name: exact_queue,
                        exchange_name: exact_exchange,
                        routing_key: exact_routing,
                        dead_letter_exchange: Some(exact_dead_letter.clone()),
                        dead_letter_routing_key: Some(exact_dead_letter),
                        retention: Some(retained.clone()),
                        ..crate::QueueDefinition::default()
                    }],
                },
                ..crate::RabbitMqConfig::default()
            },
            ..LilyConfig::default()
        };
        assert!(validate_config(&valid, ConfigMode::Development).is_ok());

        for invalid in [
            " queue".to_string(),
            "queue ".to_string(),
            "queue\nforged".to_string(),
            "q".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES + 1),
        ] {
            let config = LilyConfig {
                rabbitmq: crate::RabbitMqConfig {
                    topology: crate::RabbitMqTopologyConfig {
                        queues: vec![crate::QueueDefinition {
                            name: invalid,
                            exchange_name: "worker".into(),
                            routing_key: "queue".into(),
                            retention: Some(retained.clone()),
                            ..crate::QueueDefinition::default()
                        }],
                    },
                    ..crate::RabbitMqConfig::default()
                },
                ..LilyConfig::default()
            };
            assert!(matches!(
                validate_config(&config, ConfigMode::Development),
                Err(ConfigError::ValidationError(_))
            ));
        }

        for field in 0..5 {
            let mut queue = crate::QueueDefinition {
                name: "queue".into(),
                exchange_name: "worker".into(),
                routing_key: "queue".into(),
                retention: Some(retained.clone()),
                ..crate::QueueDefinition::default()
            };
            match field {
                0 => queue.name = "queue\nforged".into(),
                1 => queue.exchange_name = "worker\nforged".into(),
                2 => queue.routing_key = "queue\nforged".into(),
                3 => queue.dead_letter_exchange = Some("exchange\nforged".into()),
                4 => queue.dead_letter_routing_key = Some("routing\nforged".into()),
                _ => unreachable!(),
            }
            let config = LilyConfig {
                rabbitmq: crate::RabbitMqConfig {
                    topology: crate::RabbitMqTopologyConfig {
                        queues: vec![queue],
                    },
                    ..crate::RabbitMqConfig::default()
                },
                ..LilyConfig::default()
            };
            assert!(matches!(
                validate_config(&config, ConfigMode::Development),
                Err(ConfigError::ValidationError(_))
            ));
        }
    }

    #[test]
    fn rabbitmq_queue_type_and_priority_compatibility_is_fail_closed() {
        let retained = crate::QueueRetentionConfig {
            main_max_messages: 1,
            main_max_bytes: 1,
            retry_bucket_max_messages: 1,
            retry_bucket_max_bytes: 1,
            dead_letter_max_messages: 1,
            dead_letter_max_bytes: 1,
        };
        let queue = || crate::QueueDefinition {
            name: "orders".into(),
            exchange_name: "events".into(),
            routing_key: "orders".into(),
            retention: Some(retained.clone()),
            ..crate::QueueDefinition::default()
        };
        let validate = |queue| {
            validate_rabbitmq_topology(&crate::RabbitMqTopologyConfig {
                queues: vec![queue],
            })
        };

        let mut classic = queue();
        classic.max_priority = Some(16);
        assert!(validate(classic).is_ok());
        let mut invalid_classic = queue();
        invalid_classic.max_priority = Some(0);
        assert!(matches!(
            validate(invalid_classic),
            Err(ConfigError::ValidationError(message)) if message.contains("max_priority")
        ));

        let mut quorum = queue();
        quorum.queue_type = crate::RabbitMqQueueType::Quorum;
        quorum.single_active_consumer = true;
        assert!(validate(quorum.clone()).is_ok());
        for invalid in [
            crate::QueueDefinition {
                durable: false,
                ..quorum.clone()
            },
            crate::QueueDefinition {
                exclusive: true,
                ..quorum.clone()
            },
            crate::QueueDefinition {
                auto_delete: true,
                ..quorum.clone()
            },
            crate::QueueDefinition {
                max_priority: Some(1),
                ..quorum
            },
        ] {
            assert!(validate(invalid).is_err());
        }
    }

    #[test]
    fn shared_exchange_requires_consistent_kind_and_ownership() {
        let retention = crate::QueueRetentionConfig {
            main_max_messages: 1,
            main_max_bytes: 1,
            retry_bucket_max_messages: 1,
            retry_bucket_max_bytes: 1,
            dead_letter_max_messages: 1,
            dead_letter_max_bytes: 1,
        };
        let queue = |name: &str, ownership| crate::QueueDefinition {
            name: name.into(),
            exchange_name: "events".into(),
            routing_key: name.into(),
            topology_ownership: ownership,
            retention: Some(retention.clone()),
            ..crate::QueueDefinition::default()
        };
        let topology = crate::RabbitMqTopologyConfig {
            queues: vec![
                queue("orders", crate::RabbitMqTopologyOwnership::FrameworkManaged),
                queue("payments", crate::RabbitMqTopologyOwnership::External),
            ],
        };

        assert!(matches!(
            validate_rabbitmq_topology(&topology),
            Err(ConfigError::ValidationError(message))
                if message.contains("conflicting exchange_kind or topology_ownership")
        ));
    }

    #[test]
    fn production_consumer_requires_a_non_empty_shared_topology() {
        let config = LilyConfig {
            rabbitmq: crate::RabbitMqConfig {
                consumer: Some(crate::RabbitMqConsumerConfig {
                    connection_string: Some(
                        "amqps://consumer:secret@rabbit.internal/%2f".to_string(),
                    ),
                    use_tls: Some(true),
                    ..crate::RabbitMqConsumerConfig::default()
                }),
                ..crate::RabbitMqConfig::default()
            },
            ..LilyConfig::default()
        };

        assert!(matches!(
            validate_config(&config, ConfigMode::Production),
            Err(ConfigError::ValidationError(message))
                if message == "rabbitmq.topology.queues must be non-empty for the RabbitMQ consumer profile"
        ));
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-postgresql"
    ))]
    #[test]
    fn transactional_inbox_deserialization_requires_an_explicit_backend() {
        let direct = toml::from_str::<crate::TransactionalInboxConfig>(
            "database_cell = 'primary'\nrelay_batch_size = 10",
        );
        assert!(
            direct
                .as_ref()
                .is_err_and(|error| error.to_string().contains("backend")),
            "the selected Cargo feature set must never become a serialized backend default: {direct:?}"
        );

        let queue = toml::from_str::<crate::QueueDefinition>(
            "name = 'orders'\n[transactional_inbox]\ndatabase_cell = 'primary'",
        );
        assert!(
            queue
                .as_ref()
                .is_err_and(|error| error.to_string().contains("backend")),
            "queue configuration must fail closed when backend authority is omitted: {queue:?}"
        );
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-postgresql"
    ))]
    #[test]
    fn transactional_inbox_policy_accepts_each_exact_maximum_and_rejects_maximum_plus_one() {
        use crate::TransactionalInboxConfig as Policy;

        let validate = |policy: &Policy| validate_transactional_inbox("orders", policy);
        let exact_cell = "c".repeat(Policy::MAX_DATABASE_CELL_BYTES);
        let mut policy = Policy {
            database_cell: Some(exact_cell),
            inbox_lock_timeout_millis: Policy::MAX_INBOX_LOCK_TIMEOUT_MILLIS,
            outbox_claim_lease_millis: Policy::MAX_OUTBOX_CLAIM_LEASE_MILLIS,
            relay_batch_size: Policy::MAX_RELAY_BATCH_SIZE,
            relay_max_in_flight_bytes: Policy::MAX_RELAY_IN_FLIGHT_BYTES,
            relay_poll_interval_millis: Policy::MAX_RELAY_POLL_INTERVAL_MILLIS,
            relay_publish_timeout_millis: Policy::MAX_RELAY_PUBLISH_TIMEOUT_MILLIS,
            relay_max_publish_attempts: Policy::MAX_RELAY_PUBLISH_ATTEMPTS,
            relay_retry_initial_backoff_millis: Policy::MAX_RELAY_INITIAL_BACKOFF_MILLIS,
            relay_retry_max_backoff_millis: Policy::MAX_RELAY_BACKOFF_MILLIS,
            inbox_retention_secs: Policy::MAX_RETENTION_SECS,
            outbox_retention_secs: Policy::MAX_RETENTION_SECS,
            cleanup_interval_secs: Policy::MAX_CLEANUP_INTERVAL_SECS,
            shutdown_drain_timeout_millis: Policy::MAX_SHUTDOWN_DRAIN_TIMEOUT_MILLIS,
            ..Policy::default()
        };
        validate(&policy).expect("every documented inclusive maximum must validate");

        macro_rules! rejects_maximum_plus_one {
            ($field:ident, $maximum:expr) => {{
                let previous = policy.$field;
                policy.$field = $maximum + 1;
                assert!(
                    validate(&policy).is_err(),
                    "{} maximum + 1",
                    stringify!($field)
                );
                policy.$field = previous;
            }};
        }

        policy.database_cell = Some("c".repeat(Policy::MAX_DATABASE_CELL_BYTES + 1));
        assert!(validate(&policy).is_err());
        policy.database_cell = None;
        rejects_maximum_plus_one!(
            inbox_lock_timeout_millis,
            Policy::MAX_INBOX_LOCK_TIMEOUT_MILLIS
        );
        rejects_maximum_plus_one!(
            outbox_claim_lease_millis,
            Policy::MAX_OUTBOX_CLAIM_LEASE_MILLIS
        );
        rejects_maximum_plus_one!(relay_batch_size, Policy::MAX_RELAY_BATCH_SIZE);
        rejects_maximum_plus_one!(relay_max_in_flight_bytes, Policy::MAX_RELAY_IN_FLIGHT_BYTES);
        rejects_maximum_plus_one!(
            relay_poll_interval_millis,
            Policy::MAX_RELAY_POLL_INTERVAL_MILLIS
        );
        rejects_maximum_plus_one!(
            relay_publish_timeout_millis,
            Policy::MAX_RELAY_PUBLISH_TIMEOUT_MILLIS
        );
        rejects_maximum_plus_one!(
            relay_max_publish_attempts,
            Policy::MAX_RELAY_PUBLISH_ATTEMPTS
        );
        rejects_maximum_plus_one!(
            relay_retry_initial_backoff_millis,
            Policy::MAX_RELAY_INITIAL_BACKOFF_MILLIS
        );
        rejects_maximum_plus_one!(
            relay_retry_max_backoff_millis,
            Policy::MAX_RELAY_BACKOFF_MILLIS
        );
        rejects_maximum_plus_one!(inbox_retention_secs, Policy::MAX_RETENTION_SECS);
        rejects_maximum_plus_one!(outbox_retention_secs, Policy::MAX_RETENTION_SECS);
        rejects_maximum_plus_one!(cleanup_interval_secs, Policy::MAX_CLEANUP_INTERVAL_SECS);
        rejects_maximum_plus_one!(
            shutdown_drain_timeout_millis,
            Policy::MAX_SHUTDOWN_DRAIN_TIMEOUT_MILLIS
        );
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-postgresql"
    ))]
    #[test]
    fn transactional_inbox_policy_is_strict_and_relationally_fail_closed() {
        #[cfg(feature = "transactional-inbox-postgresql")]
        let backend = "postgresql";
        #[cfg(all(
            not(feature = "transactional-inbox-postgresql"),
            feature = "transactional-inbox-mongodb"
        ))]
        let backend = "mongodb";
        let unknown = toml::from_str::<crate::TransactionalInboxConfig>(&format!(
            "backend = '{backend}'\nunknown = true"
        ));
        assert!(unknown.is_err());

        let mut policy = crate::TransactionalInboxConfig::default();
        policy.outbox_claim_lease_millis = policy.relay_publish_timeout_millis - 1;
        assert!(validate_transactional_inbox("orders", &policy).is_err());

        let mut policy = crate::TransactionalInboxConfig::default();
        policy.relay_retry_max_backoff_millis = policy.relay_retry_initial_backoff_millis - 1;
        assert!(validate_transactional_inbox("orders", &policy).is_err());

        let mut policy = crate::TransactionalInboxConfig::default();
        policy.cleanup_interval_secs = policy.inbox_retention_secs + 1;
        assert!(validate_transactional_inbox("orders", &policy).is_err());
    }

    #[cfg(feature = "transactional-inbox-postgresql")]
    #[test]
    fn queue_definition_accepts_the_feature_gated_transactional_binding() {
        let queue = toml::from_str::<crate::QueueDefinition>(
            "name = 'orders'\n[transactional_inbox]\nbackend = 'postgresql'\ndatabase_cell = 'primary'",
        )
        .expect("enabled feature must publish the typed binding");
        let binding = queue.transactional_inbox.expect("binding");
        assert_eq!(
            binding.backend,
            crate::TransactionalInboxBackend::PostgreSql
        );
        assert_eq!(binding.database_cell.as_deref(), Some("primary"));
    }

    #[cfg(feature = "transactional-inbox-postgresql")]
    #[test]
    fn explicit_postgresql_transactional_backend_round_trips_without_inference() {
        let policy = toml::from_str::<crate::TransactionalInboxConfig>(
            "backend = 'postgresql'\ndatabase_cell = 'primary'",
        )
        .expect("an explicit compiled PostgreSQL backend must deserialize");
        assert_eq!(policy.backend, crate::TransactionalInboxBackend::PostgreSql);
        policy
            .validate_contract()
            .expect("deserialized defaults must satisfy the canonical contract");

        let encoded = toml::to_string(&policy).expect("policy must serialize");
        assert!(encoded.contains("backend = \"postgresql\""));
        let decoded = toml::from_str::<crate::TransactionalInboxConfig>(&encoded)
            .expect("serialized explicit PostgreSQL authority must deserialize");
        assert_eq!(decoded, policy);
    }

    #[cfg(feature = "transactional-inbox-mongodb")]
    #[test]
    fn mongodb_transaction_policy_accepts_exact_maxima_and_rejects_maximum_plus_one() {
        use crate::{MongoTransactionalInboxConfig as MongoPolicy, TransactionalInboxConfig};

        let exact = MongoPolicy {
            max_transaction_attempts: MongoPolicy::MAX_TRANSACTION_ATTEMPTS,
            retry_initial_backoff_millis: MongoPolicy::MAX_RETRY_INITIAL_BACKOFF_MILLIS,
            retry_max_backoff_millis: MongoPolicy::MAX_RETRY_BACKOFF_MILLIS,
            commit_retry_timeout_millis: MongoPolicy::MAX_COMMIT_RETRY_TIMEOUT_MILLIS,
        };
        let mut policy = TransactionalInboxConfig {
            backend: crate::TransactionalInboxBackend::MongoDb,
            inbox_lock_timeout_millis:
                TransactionalInboxConfig::MIN_MONGODB_INBOX_LOCK_TIMEOUT_MILLIS,
            mongodb: Some(exact),
            ..TransactionalInboxConfig::default()
        };
        validate_transactional_inbox("orders", &policy)
            .expect("every MongoDB inclusive maximum must validate");

        macro_rules! rejects_maximum_plus_one {
            ($field:ident, $maximum:expr) => {{
                let mongo = policy.mongodb.as_mut().expect("MongoDB policy");
                let previous = mongo.$field;
                mongo.$field = $maximum + 1;
                assert!(
                    validate_transactional_inbox("orders", &policy).is_err(),
                    "{} maximum + 1",
                    stringify!($field)
                );
                policy.mongodb.as_mut().expect("MongoDB policy").$field = previous;
            }};
        }

        rejects_maximum_plus_one!(
            max_transaction_attempts,
            MongoPolicy::MAX_TRANSACTION_ATTEMPTS
        );
        rejects_maximum_plus_one!(
            retry_initial_backoff_millis,
            MongoPolicy::MAX_RETRY_INITIAL_BACKOFF_MILLIS
        );
        rejects_maximum_plus_one!(
            retry_max_backoff_millis,
            MongoPolicy::MAX_RETRY_BACKOFF_MILLIS
        );
        rejects_maximum_plus_one!(
            commit_retry_timeout_millis,
            MongoPolicy::MAX_COMMIT_RETRY_TIMEOUT_MILLIS
        );
    }

    #[cfg(feature = "transactional-inbox-mongodb")]
    #[test]
    fn mongodb_transaction_policy_is_strict_bounded_and_defaults_when_omitted() {
        let unknown = toml::from_str::<crate::MongoTransactionalInboxConfig>(
            "max_transaction_attempts = 3\nunknown = true",
        );
        assert!(
            unknown.is_err(),
            "MongoDB policy must reject unknown fields"
        );

        let mut policy = crate::TransactionalInboxConfig {
            backend: crate::TransactionalInboxBackend::MongoDb,
            inbox_lock_timeout_millis:
                crate::TransactionalInboxConfig::MIN_MONGODB_INBOX_LOCK_TIMEOUT_MILLIS,
            mongodb: None,
            ..crate::TransactionalInboxConfig::default()
        };
        validate_transactional_inbox("orders", &policy)
            .expect("omitting MongoDB policy must select bounded defaults");

        policy.inbox_lock_timeout_millis =
            crate::TransactionalInboxConfig::MIN_MONGODB_INBOX_LOCK_TIMEOUT_MILLIS - 1;
        assert!(validate_transactional_inbox("orders", &policy).is_err());

        policy.inbox_lock_timeout_millis =
            crate::TransactionalInboxConfig::MIN_MONGODB_INBOX_LOCK_TIMEOUT_MILLIS;
        policy.mongodb = Some(crate::MongoTransactionalInboxConfig {
            retry_initial_backoff_millis: 2,
            retry_max_backoff_millis: 1,
            ..crate::MongoTransactionalInboxConfig::default()
        });
        assert!(validate_transactional_inbox("orders", &policy).is_err());
    }

    #[cfg(feature = "transactional-inbox-mongodb")]
    #[test]
    fn queue_definition_accepts_the_mongodb_backend_and_nested_policy() {
        let queue = toml::from_str::<crate::QueueDefinition>(
            "name = 'orders'\n[transactional_inbox]\nbackend = 'mongodb'\ndatabase_cell = 'primary'\n[transactional_inbox.mongodb]\nmax_transaction_attempts = 4",
        )
        .expect("enabled feature must publish the MongoDB binding");
        let binding = queue.transactional_inbox.expect("binding");
        assert_eq!(binding.backend, crate::TransactionalInboxBackend::MongoDb);
        assert_eq!(binding.database_cell.as_deref(), Some("primary"));
        assert_eq!(
            binding
                .mongodb
                .expect("explicit MongoDB policy")
                .max_transaction_attempts,
            4
        );
    }

    #[cfg(feature = "transactional-inbox-mongodb")]
    #[test]
    fn explicit_mongodb_transactional_backend_round_trips_without_inference() {
        let policy = toml::from_str::<crate::TransactionalInboxConfig>(
            "backend = 'mongodb'\ndatabase_cell = 'primary'\n[mongodb]\nmax_transaction_attempts = 4",
        )
        .expect("an explicit compiled MongoDB backend must deserialize");
        assert_eq!(policy.backend, crate::TransactionalInboxBackend::MongoDb);
        policy
            .validate_contract()
            .expect("deserialized defaults must satisfy the canonical contract");

        let encoded = toml::to_string(&policy).expect("policy must serialize");
        assert!(encoded.contains("backend = \"mongodb\""));
        let decoded = toml::from_str::<crate::TransactionalInboxConfig>(&encoded)
            .expect("serialized explicit MongoDB authority must deserialize");
        assert_eq!(decoded, policy);
    }

    #[cfg(all(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-postgresql"
    ))]
    #[test]
    fn dual_backend_build_rejects_mongodb_policy_on_a_postgresql_binding() {
        let policy = crate::TransactionalInboxConfig {
            backend: crate::TransactionalInboxBackend::PostgreSql,
            mongodb: Some(crate::MongoTransactionalInboxConfig::default()),
            ..crate::TransactionalInboxConfig::default()
        };
        assert!(matches!(
            policy.validate_contract(),
            Err("mongodb_policy_for_postgresql_backend")
        ));
    }

    #[cfg(not(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-postgresql"
    )))]
    #[test]
    fn queue_definition_rejects_transactional_binding_when_feature_is_disabled() {
        let result = toml::from_str::<crate::QueueDefinition>(
            "name = 'orders'\n[transactional_inbox]\nbackend = 'postgresql'",
        );
        assert!(
            result.is_err(),
            "disabled feature must reject, not ignore, the field"
        );
    }

    #[test]
    fn http_websocket_and_publisher_only_profiles_do_not_enable_a_consumer() {
        let profiles = [
            LilyConfig::default(),
            LilyConfig {
                websocket: Some(crate::WebSocketConfig::default()),
                ..LilyConfig::default()
            },
            LilyConfig {
                queue_client: Some(crate::QueueClientConfig {
                    connection_string: Some(
                        "amqps://publisher:secret@rabbit.internal/%2f".to_string(),
                    ),
                    use_tls: Some(true),
                    ..crate::QueueClientConfig::default()
                }),
                ..LilyConfig::default()
            },
        ];

        for profile in profiles {
            assert!(profile.rabbitmq.consumer.is_none());
            assert!(profile.rabbitmq.topology.queues.is_empty());
            for mode in [
                ConfigMode::Development,
                ConfigMode::Test,
                ConfigMode::Production,
            ] {
                validate_config(&profile, mode).unwrap();
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_environment_never_panics_or_silently_corrupts_lily_values() {
        use std::os::unix::ffi::OsStringExt;

        let unrelated = materialize_environment(vec![(
            OsString::from("UNRELATED"),
            OsString::from_vec(vec![0xff]),
        )])
        .unwrap();
        assert!(unrelated.is_empty());

        let error = materialize_environment(vec![(
            OsString::from("LILY__SERVER__HOST"),
            OsString::from_vec(vec![0xff]),
        )])
        .unwrap_err();
        assert!(matches!(error, ConfigError::ValidationError(_)));
    }

    static NEXT_FILE: AtomicU64 = AtomicU64::new(1);

    struct VersionedSecretResolver;

    #[async_trait]
    impl SecretResolver for VersionedSecretResolver {
        async fn resolve(&self, key: &str) -> Result<String, ConfigError> {
            Ok(format!("legacy:{key}"))
        }

        async fn resolve_versioned(&self, _key: &str) -> Result<ResolvedSecret, ConfigError> {
            Ok(ResolvedSecret::versioned(
                "do-not-print-me".to_string(),
                "v7",
                None,
            ))
        }

        fn provider_name(&self) -> &'static str {
            "test-vault"
        }
    }

    const RESOLVED_BINDING_SENTINEL: &str = r#"resolved-binding-"quoted"-\slash
second-line-secret-sentinel"#;

    struct SentinelSecretResolver;

    #[async_trait]
    impl SecretResolver for SentinelSecretResolver {
        async fn resolve(&self, _key: &str) -> Result<String, ConfigError> {
            Ok(RESOLVED_BINDING_SENTINEL.to_string())
        }

        fn provider_name(&self) -> &'static str {
            "sentinel-test-provider"
        }
    }

    struct ExpiredSecretResolver;

    #[async_trait]
    impl SecretResolver for ExpiredSecretResolver {
        async fn resolve(&self, _key: &str) -> Result<String, ConfigError> {
            Ok("never-publish-this-value".to_string())
        }

        async fn resolve_versioned(&self, _key: &str) -> Result<ResolvedSecret, ConfigError> {
            Ok(ResolvedSecret::versioned(
                "never-publish-this-value".to_string(),
                "expired-v1",
                Some(0),
            ))
        }

        fn provider_name(&self) -> &'static str {
            "expired-test-provider"
        }
    }

    struct RedisBackplaneSecretResolver;

    #[async_trait]
    impl SecretResolver for RedisBackplaneSecretResolver {
        async fn resolve(&self, key: &str) -> Result<String, ConfigError> {
            assert_eq!(key, "websocket.redis_url");
            Ok("rediss://publisher:do-not-print-me@redis.internal:6380/0".to_owned())
        }

        fn provider_name(&self) -> &'static str {
            "redis-backplane-test-vault"
        }
    }

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "lily-config-{name}-{}-{}.toml",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn write_config(name: &str, content: &str) -> PathBuf {
        let path = test_path(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    fn assert_error_chain_does_not_contain(
        error: &(dyn std::error::Error + 'static),
        sentinel: &str,
    ) {
        let mut current = Some(error);
        while let Some(error) = current {
            let display = error.to_string();
            let debug = format!("{error:?}");
            assert!(!display.contains(sentinel), "Display leaked {sentinel}");
            assert!(!debug.contains(sentinel), "Debug leaked {sentinel}");
            current = error.source();
        }
    }

    #[tokio::test]
    async fn repeated_load_replays_the_same_immutable_startup_snapshot() {
        let path = write_config("immutable", "[server]\nhost = '127.0.0.1'\nport = 8080\n");
        let service = ConfigService::new(ConfigOptions::test(&path));
        let first = service.load().await.unwrap();
        std::fs::write(&path, "[server]\nhost = '127.0.0.1'\nport = 9090\n").unwrap();

        let second = service.load().await.unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(second.version(), 1);
        assert_eq!(second.config().server.port, 8080);
    }

    #[tokio::test]
    async fn canonical_env_updates_typed_and_flat_same_version() {
        let path = write_config("env", "[server]\nhost = '127.0.0.1'\nport = 8080\n");
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service
            .build_snapshot(4, [("LILY__SERVER__PORT".to_string(), "9090".to_string())])
            .await
            .unwrap();

        assert_eq!(snapshot.version(), 4);
        assert_eq!(snapshot.config().server.port, 9090);
        assert_eq!(snapshot.values()["server.port"], "9090");
        assert_eq!(
            snapshot.metadata().environment_overrides,
            vec!["server.port"]
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn canonical_rabbitmq_consumer_environment_path_is_nested_and_typed() {
        let path = write_config("rabbitmq-consumer-env", "");
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service
            .build_snapshot(
                1,
                [(
                    "LILY__RABBITMQ__CONSUMER__POOL_SIZE".to_string(),
                    "7".to_string(),
                )],
            )
            .await
            .unwrap();

        assert_eq!(
            snapshot
                .config()
                .rabbitmq
                .consumer
                .as_ref()
                .unwrap()
                .pool_size,
            7
        );
        assert_eq!(snapshot.values()["rabbitmq.consumer.pool_size"], "7");
        assert_eq!(
            snapshot.metadata().environment_overrides,
            vec!["rabbitmq.consumer.pool_size"]
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn removed_rabbitmq_sections_and_environment_path_fail_closed_in_every_mode() {
        for (name, source) in [
            (
                "legacy-message-broker",
                "[message_broker]\nconnection_string='amqps://consumer:secret@rabbit/%2f'\n",
            ),
            (
                "legacy-rabbitmq-topology",
                "[[rabbitmq_topology.queues]]\nname='orders'\n",
            ),
        ] {
            let path = write_config(name, source);
            for mode in [
                ConfigMode::Development,
                ConfigMode::Test,
                ConfigMode::Production,
            ] {
                let error = ConfigService::new(ConfigOptions::new(&path, mode))
                    .build_snapshot(1, [])
                    .await
                    .unwrap_err();
                assert!(matches!(error, ConfigError::ValidationError(_)));
            }
            let _ = std::fs::remove_file(path);
        }

        for (name, key, value) in [
            (
                "legacy-rabbitmq-consumer-env",
                "LILY__MESSAGE_BROKER__POOL_SIZE",
                "7",
            ),
            (
                "legacy-rabbitmq-topology-env",
                "LILY__RABBITMQ_TOPOLOGY__QUEUES",
                "[]",
            ),
        ] {
            let path = write_config(name, "");
            for mode in [
                ConfigMode::Development,
                ConfigMode::Test,
                ConfigMode::Production,
            ] {
                let error = ConfigService::new(ConfigOptions::new(&path, mode))
                    .build_snapshot(1, [(key.to_string(), value.to_string())])
                    .await
                    .unwrap_err();
                assert!(matches!(error, ConfigError::ValidationError(_)));
            }
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn websocket_inbound_queue_limits_accept_canonical_env_overrides() {
        let path = write_config("websocket-inbound-env", "");
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service
            .build_snapshot(
                5,
                [
                    (
                        "LILY__WEBSOCKET__INBOUND_QUEUE_CAPACITY".to_string(),
                        "7".to_string(),
                    ),
                    (
                        "LILY__WEBSOCKET__INBOUND_QUEUE_MAX_BYTES".to_string(),
                        "65536".to_string(),
                    ),
                ],
            )
            .await
            .unwrap();

        let websocket = snapshot
            .config()
            .websocket
            .as_ref()
            .expect("environment overrides create the WebSocket section");
        assert_eq!(websocket.inbound_queue_capacity, 7);
        assert_eq!(websocket.inbound_queue_max_bytes, 65_536);
        assert_eq!(snapshot.values()["websocket.inbound_queue_capacity"], "7");
        assert_eq!(
            snapshot.values()["websocket.inbound_queue_max_bytes"],
            "65536"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn websocket_outbound_policy_accepts_canonical_env_overrides() {
        let path = write_config("websocket-outbound-env", "");
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service
            .build_snapshot(
                6,
                [
                    (
                        "LILY__WEBSOCKET__MAX_OUTBOUND_MESSAGE_SIZE_BYTES".to_string(),
                        "262144".to_string(),
                    ),
                    (
                        "LILY__WEBSOCKET__OUTBOUND_QUEUE_MAX_BYTES".to_string(),
                        "524288".to_string(),
                    ),
                    (
                        "LILY__WEBSOCKET__OUTBOUND_ADMISSION_TIMEOUT_MILLIS".to_string(),
                        "1750".to_string(),
                    ),
                ],
            )
            .await
            .unwrap();

        let websocket = snapshot
            .config()
            .websocket
            .as_ref()
            .expect("environment overrides create the WebSocket section");
        assert_eq!(websocket.max_outbound_message_size_bytes, 262_144);
        assert_eq!(websocket.outbound_queue_max_bytes, 524_288);
        assert_eq!(websocket.outbound_admission_timeout_millis, 1_750);
        assert_eq!(
            snapshot.values()["websocket.max_outbound_message_size_bytes"],
            "262144"
        );
        assert_eq!(
            snapshot.values()["websocket.outbound_queue_max_bytes"],
            "524288"
        );
        assert_eq!(
            snapshot.values()["websocket.outbound_admission_timeout_millis"],
            "1750"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn optional_and_default_lookup_only_treat_missing_keys_as_absent() {
        let path = write_config(
            "optional-scalar",
            "[custom]\nmalformed_port = 'not-a-number'\n",
        );
        let service = ConfigService::new(ConfigOptions::test(&path));
        service.load().await.unwrap();

        assert_eq!(
            service
                .get_or_default::<u16>("custom.missing_port", 8080)
                .await
                .unwrap(),
            8080
        );
        assert_eq!(
            service
                .get_optional::<u16>("custom.missing_port")
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            service
                .get_or_default::<u16>("custom.malformed_port", 8080)
                .await,
            Err(ConfigError::TypeCastError { .. })
        ));
        assert!(matches!(
            service.get_optional::<u16>("custom.malformed_port").await,
            Err(ConfigError::TypeCastError { .. })
        ));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn single_underscore_stays_inside_field_name() {
        let path = write_config(
            "underscore",
            "[database]\ndatabase_type = 'postgresql'\nconnection_string = 'old'\n",
        );
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service
            .build_snapshot(
                1,
                [(
                    "LILY__DATABASE__CONNECTION_STRING".to_string(),
                    "postgresql://new".to_string(),
                )],
            )
            .await
            .unwrap();

        assert_eq!(
            snapshot
                .config()
                .database
                .as_ref()
                .unwrap()
                .connection_string
                .as_deref(),
            Some("postgresql://new")
        );
        assert_eq!(
            snapshot.values()["database.connection_string"],
            "postgresql://new"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn malformed_toml_never_falls_back() {
        let path = write_config("malformed", "[server\nport = 8080");
        let service = ConfigService::new(ConfigOptions::test(&path));
        let error = service.build_snapshot(1, []).await.unwrap_err();
        assert!(matches!(error, ConfigError::ParseError(_)));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn production_missing_file_is_fail_closed() {
        let service = ConfigService::production(test_path("missing"));
        let error = service.build_snapshot(1, []).await.unwrap_err();
        assert!(matches!(error, ConfigError::IoError(_)));
    }

    #[tokio::test]
    async fn production_unknown_field_is_rejected() {
        let path = write_config(
            "unknown",
            "[server]\nhost = '127.0.0.1'\nport = 8080\nunknown = true\n",
        );
        let service = ConfigService::production(&path);
        let error = service.build_snapshot(1, []).await.unwrap_err();
        assert!(matches!(error, ConfigError::ValidationError(_)));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn production_rabbitmq_transport_and_credentials_are_fail_closed() {
        let insecure = write_config(
            "rabbitmq-guest",
            r#"
[rabbitmq.consumer]
connection_string = "amqp://guest:guest@rabbit.internal/%2f"
use_tls = false
"#,
        );
        let error = ConfigService::production(&insecure)
            .build_snapshot(1, [])
            .await
            .unwrap_err();
        assert!(
            matches!(error, ConfigError::ValidationError(message) if message.contains("guest credentials"))
        );
        let _ = std::fs::remove_file(insecure);

        let mismatch = write_config(
            "rabbitmq-tls-mismatch",
            r#"
[queue_client]
connection_string = "amqp://publisher:secret@rabbit.internal/%2f"
use_tls = true
"#,
        );
        let error = ConfigService::production(&mismatch)
            .build_snapshot(1, [])
            .await
            .unwrap_err();
        assert!(
            matches!(error, ConfigError::ValidationError(message) if message.contains("requires amqps://"))
        );
        let _ = std::fs::remove_file(mismatch);

        let valid = write_config(
            "rabbitmq-valid",
            r#"
[queue_client]
connection_string = "amqps://publisher:secret@rabbit.internal/%2f"
use_tls = true
"#,
        );
        ConfigService::production(&valid)
            .build_snapshot(1, [])
            .await
            .unwrap();
        let _ = std::fs::remove_file(valid);
    }

    #[tokio::test]
    async fn websocket_backplane_defaults_are_typed_and_redis_url_is_redacted() {
        let path = write_config(
            "websocket-backplane-defaults",
            r#"
[websocket.backplane]
redis_url = "redis://publisher:do-not-print-me@127.0.0.1:6379/4"
use_tls = false
application_namespace = "orders-api"
environment_namespace = "test"
"#,
        );
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service.build_snapshot(1, []).await.unwrap();
        let backplane = snapshot
            .config()
            .websocket
            .as_ref()
            .and_then(|websocket| websocket.backplane.as_ref())
            .expect("typed WebSocket backplane config");

        assert_eq!(backplane.application_namespace, "orders-api");
        assert_eq!(backplane.environment_namespace, "test");
        assert_eq!(backplane.channel_namespace, "events");
        assert_eq!(backplane.publish_capacity, 64);
        assert_eq!(backplane.ingress_capacity, 256);
        assert_eq!(backplane.connection_timeout_millis, 5_000);
        assert_eq!(backplane.operation_timeout_millis, 2_000);
        assert_eq!(backplane.reconnect_initial_delay_millis, 100);
        assert_eq!(backplane.reconnect_max_delay_millis, 5_000);
        assert_eq!(backplane.reconnect_jitter_ratio, 0.2);
        assert_eq!(
            snapshot.redacted_values()["websocket.backplane.redis_url"],
            REDACTED
        );
        let debug = format!("{backplane:?}");
        assert!(!debug.contains("do-not-print-me"));
        assert!(!debug.contains("127.0.0.1"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn websocket_backplane_credential_uses_the_existing_secret_resolver_pipeline() {
        let path = write_config(
            "websocket-backplane-secret",
            r#"
[websocket.backplane]
redis_url = "${secret:websocket.redis_url}"
use_tls = true
application_namespace = "orders-api"
environment_namespace = "production"
"#,
        );
        let service = ConfigService::with_options_and_secret_resolver(
            ConfigOptions::production(&path),
            RedisBackplaneSecretResolver,
        );
        let snapshot = service.build_snapshot(1, []).await.unwrap();
        let backplane = snapshot
            .config()
            .websocket
            .as_ref()
            .and_then(|websocket| websocket.backplane.as_ref())
            .expect("resolved typed WebSocket backplane config");

        assert!(backplane.redis_url.starts_with("rediss://publisher:"));
        assert_eq!(
            snapshot.redacted_values()["websocket.backplane.redis_url"],
            REDACTED
        );
        assert_eq!(
            snapshot.metadata().secret_bindings[0].config_key,
            "websocket.backplane.redis_url"
        );
        assert_error_chain_does_not_contain(
            &ConfigError::ValidationError(format!("{backplane:?}")),
            "do-not-print-me",
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn websocket_backplane_custom_ca_path_rejects_config_references() {
        for (name, reference) in [
            ("secret", "${secret:websocket.redis_ca_path}"),
            ("file", "${file:/run/secrets/redis-ca-path}"),
        ] {
            let path = write_config(
                &format!("websocket-backplane-ca-reference-{name}"),
                &format!(
                    r#"
[websocket.backplane]
redis_url = "rediss://publisher:secret@redis.internal:6380/0"
use_tls = true
custom_ca_bundle = "{reference}"
application_namespace = "orders-api"
environment_namespace = "test"
"#
                ),
            );
            let error = ConfigService::new(ConfigOptions::test(&path))
                .build_snapshot(1, [])
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                ConfigError::ValidationError(message)
                    if message.contains("websocket.backplane.custom_ca_bundle")
                        && message.contains("direct absolute path")
            ));
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn websocket_backplane_transport_and_production_credentials_are_fail_closed() {
        let tls_mismatch = write_config(
            "websocket-backplane-tls-mismatch",
            r#"
[websocket.backplane]
redis_url = "redis://127.0.0.1:6379/0"
use_tls = true
application_namespace = "orders-api"
environment_namespace = "test"
"#,
        );
        let error = ConfigService::new(ConfigOptions::test(&tls_mismatch))
            .build_snapshot(1, [])
            .await
            .unwrap_err();
        assert!(
            matches!(error, ConfigError::ValidationError(message) if message.contains("requires rediss://"))
        );
        let _ = std::fs::remove_file(tls_mismatch);

        let missing_credentials = write_config(
            "websocket-backplane-production-credentials",
            r#"
[websocket.backplane]
redis_url = "rediss://redis.internal:6380/0"
use_tls = true
application_namespace = "orders-api"
environment_namespace = "production"
"#,
        );
        let error = ConfigService::production(&missing_credentials)
            .build_snapshot(1, [])
            .await
            .unwrap_err();
        assert!(
            matches!(error, ConfigError::ValidationError(message) if message.contains("ACL credential"))
        );
        let _ = std::fs::remove_file(missing_credentials);

        let valid = write_config(
            "websocket-backplane-production-valid",
            r#"
[websocket.backplane]
redis_url = "rediss://publisher:secret@redis.internal:6380/0"
use_tls = true
application_namespace = "orders-api"
environment_namespace = "production"
"#,
        );
        ConfigService::production(&valid)
            .build_snapshot(1, [])
            .await
            .unwrap();
        let _ = std::fs::remove_file(valid);
    }

    #[tokio::test]
    async fn websocket_backplane_capacity_namespace_and_backoff_are_bounded() {
        for (name, field, value, expected) in [
            (
                "publish-capacity",
                "publish_capacity",
                "1025",
                "publish_capacity",
            ),
            (
                "ingress-capacity",
                "ingress_capacity",
                "0",
                "ingress_capacity",
            ),
            (
                "initial-delay",
                "reconnect_initial_delay_millis",
                "9",
                "reconnect_initial_delay_millis",
            ),
        ] {
            let path = write_config(
                name,
                &format!(
                    r#"
[websocket.backplane]
redis_url = "redis://127.0.0.1:6379/0"
use_tls = false
application_namespace = "orders-api"
environment_namespace = "test"
{field} = {value}
"#
                ),
            );
            let error = ConfigService::new(ConfigOptions::test(&path))
                .build_snapshot(1, [])
                .await
                .unwrap_err();
            assert!(
                matches!(error, ConfigError::ValidationError(message) if message.contains(expected))
            );
            let _ = std::fs::remove_file(path);
        }

        let invalid_namespace = write_config(
            "websocket-backplane-namespace",
            r#"
[websocket.backplane]
redis_url = "redis://127.0.0.1:6379/0"
use_tls = false
application_namespace = "orders api"
environment_namespace = "test"
"#,
        );
        let error = ConfigService::new(ConfigOptions::test(&invalid_namespace))
            .build_snapshot(1, [])
            .await
            .unwrap_err();
        assert!(
            matches!(error, ConfigError::ValidationError(message) if message.contains("application_namespace"))
        );
        let _ = std::fs::remove_file(invalid_namespace);

        let inverted_backoff = write_config(
            "websocket-backplane-backoff-order",
            r#"
[websocket.backplane]
redis_url = "redis://127.0.0.1:6379/0"
use_tls = false
application_namespace = "orders-api"
environment_namespace = "test"
reconnect_initial_delay_millis = 200
reconnect_max_delay_millis = 100
"#,
        );
        let error = ConfigService::new(ConfigOptions::test(&inverted_backoff))
            .build_snapshot(1, [])
            .await
            .unwrap_err();
        assert!(
            matches!(error, ConfigError::ValidationError(message) if message.contains("between 200 and 300000"))
        );
        let _ = std::fs::remove_file(inverted_backoff);

        for value in ["-0.1", "1.1", "nan"] {
            let invalid_jitter = write_config(
                &format!("websocket-backplane-jitter-{value}"),
                &format!(
                    r#"
[websocket.backplane]
redis_url = "redis://127.0.0.1:6379/0"
use_tls = false
application_namespace = "orders-api"
environment_namespace = "test"
reconnect_jitter_ratio = {value}
"#
                ),
            );
            let error = ConfigService::new(ConfigOptions::test(&invalid_jitter))
                .build_snapshot(1, [])
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                ConfigError::ValidationError(message)
                    if message.contains("reconnect_jitter_ratio")
            ));
            let _ = std::fs::remove_file(invalid_jitter);
        }
    }

    #[tokio::test]
    async fn websocket_backplane_rejects_period_in_each_namespace_segment() {
        for (name, field, application, environment, channel) in [
            (
                "application-period",
                "application_namespace",
                "orders.api",
                "test",
                "events",
            ),
            (
                "environment-period",
                "environment_namespace",
                "orders-api",
                "test.blue",
                "events",
            ),
            (
                "channel-period",
                "channel_namespace",
                "orders-api",
                "test",
                "events.v2",
            ),
        ] {
            let path = write_config(
                name,
                &format!(
                    r#"
[websocket.backplane]
redis_url = "redis://127.0.0.1:6379/0"
use_tls = false
application_namespace = "{application}"
environment_namespace = "{environment}"
channel_namespace = "{channel}"
"#
                ),
            );
            let error = ConfigService::new(ConfigOptions::test(&path))
                .build_snapshot(1, [])
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                ConfigError::ValidationError(message)
                    if message.contains(&format!("websocket.backplane.{field}"))
                        && message.contains("'.' is reserved as the channel delimiter")
            ));
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn websocket_backplane_dot_delimited_channel_segments_cannot_alias() {
        let ambiguous_segments = [
            ("orders.api", "prod", "events", "application_namespace"),
            ("api", "prod.orders", "events", "environment_namespace"),
        ];
        let first_channel = format!(
            "{}.{}.{}",
            ambiguous_segments[0].1, ambiguous_segments[0].0, ambiguous_segments[0].2
        );
        let second_channel = format!(
            "{}.{}.{}",
            ambiguous_segments[1].1, ambiguous_segments[1].0, ambiguous_segments[1].2
        );
        assert_eq!(first_channel, second_channel);

        for (index, (application, environment, channel, invalid_field)) in
            ambiguous_segments.into_iter().enumerate()
        {
            let path = write_config(
                &format!("websocket-backplane-segment-alias-{index}"),
                &format!(
                    r#"
[websocket.backplane]
redis_url = "redis://127.0.0.1:6379/0"
use_tls = false
application_namespace = "{application}"
environment_namespace = "{environment}"
channel_namespace = "{channel}"
"#
                ),
            );
            let error = ConfigService::new(ConfigOptions::test(&path))
                .build_snapshot(1, [])
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                ConfigError::ValidationError(message) if message.contains(invalid_field)
            ));
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn production_rejects_recognized_legacy_environment_key() {
        let path = write_config(
            "legacy-production",
            "[server]\nhost = '127.0.0.1'\nport = 8080\n",
        );
        let service = ConfigService::production(&path);
        let error = service
            .build_snapshot(1, [("LILY_SERVER_PORT".to_string(), "9090".to_string())])
            .await
            .unwrap_err();
        assert!(
            matches!(error, ConfigError::ValidationError(message) if message.contains("legacy environment key"))
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn duplicate_factory_cell_name_is_rejected() {
        let path = write_config(
            "duplicates",
            r#"
[database]
mode = "factory"

[[database.cells]]
name = "main"
database_type = "postgresql"
database_name = "app"

[[database.cells]]
name = "main"
database_type = "postgresql"
database_name = "audit"
"#,
        );
        let service = ConfigService::new(ConfigOptions::test(&path));
        let error = service.build_snapshot(1, []).await.unwrap_err();
        assert!(
            matches!(error, ConfigError::ValidationError(message) if message.contains("duplicate"))
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn standard_redis_factory_cells_preserve_typed_runtime_fields() {
        let path = write_config(
            "redis-factory-cells",
            r#"
[cache]
mode = "factory"

[[cache.cells]]
name = "sessions"
provider = "redis"
redis_url = "rediss://user:secret@cache-a.example:6380/1"
use_tls = true
key_namespace = "sessions"
default_ttl_secs = 900
pool_size = 12
connection_timeout_secs = 4
operation_timeout_secs = 2
scan_page_size = 50
max_scan_results = 500

[[cache.cells]]
name = "ratelimits"
provider = "redis"
redis_url = "redis://cache-b.internal:6379/2"
use_tls = false
key_namespace = "ratelimits"
default_ttl_secs = 60
pool_size = 4
connection_timeout_secs = 3
operation_timeout_secs = 1
scan_page_size = 20
max_scan_results = 100
"#,
        );
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service.build_snapshot(1, []).await.unwrap();
        let cells = snapshot
            .config()
            .cache
            .as_ref()
            .unwrap()
            .cells
            .as_ref()
            .unwrap();

        assert_eq!(cells.len(), 2);
        assert_eq!(cells[0].name, "sessions");
        assert_eq!(cells[0].pool_size, Some(12));
        assert_eq!(cells[0].operation_timeout_secs, Some(2));
        assert_eq!(
            snapshot.redacted_values()["cache.cells.0.redis_url"],
            REDACTED
        );
        assert_eq!(
            cells[1].redis_url.as_deref(),
            Some("redis://cache-b.internal:6379/2")
        );
        assert_eq!(cells[1].key_namespace.as_deref(), Some("ratelimits"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn production_file_reference_loads_and_redacts_a_factory_cell_secret() {
        let secret_path = test_path("redis-url-secret");
        std::fs::write(
            &secret_path,
            "rediss://cache-user:cache-password@cache.internal:6380/1\n",
        )
        .unwrap();
        let ca_path = test_path("redis-ca").with_extension("pem");
        std::fs::write(&ca_path, "public-ca-placeholder").unwrap();
        let path = write_config(
            "redis-file-reference",
            &format!(
                r#"
[cache]
mode = "factory"

[[cache.cells]]
name = "sessions"
provider = "redis"
redis_url = "${{file:{}}}"
use_tls = true
additional_ca_bundle = "{}"
key_namespace = "sessions"
"#,
                secret_path.display(),
                ca_path.display()
            ),
        );

        let snapshot = ConfigService::production(&path)
            .build_snapshot(1, [])
            .await
            .unwrap();
        let cell = &snapshot
            .config()
            .cache
            .as_ref()
            .unwrap()
            .cells
            .as_ref()
            .unwrap()[0];

        assert_eq!(
            cell.redis_url.as_deref(),
            Some("rediss://cache-user:cache-password@cache.internal:6380/1")
        );
        assert_eq!(
            cell.additional_ca_bundle.as_deref(),
            Some(ca_path.as_path())
        );
        assert_eq!(
            snapshot.redacted_values()["cache.cells.0.redis_url"],
            REDACTED
        );
        assert_eq!(snapshot.metadata().secret_bindings.len(), 1);
        assert_eq!(snapshot.metadata().secret_bindings[0].provider, "file");

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(secret_path);
        let _ = std::fs::remove_file(ca_path);
    }

    #[tokio::test]
    async fn file_reference_is_fail_closed_for_relative_multiline_or_missing_files() {
        assert!(matches!(
            read_bounded_file_reference(Path::new("relative.secret")).await,
            Err(ConfigError::ValidationError(_))
        ));

        let multiline = test_path("multiline-secret");
        std::fs::write(&multiline, "first\nsecond\n").unwrap();
        assert!(matches!(
            read_bounded_file_reference(&multiline).await,
            Err(ConfigError::ValidationError(_))
        ));
        let _ = std::fs::remove_file(multiline);

        assert!(matches!(
            read_bounded_file_reference(&test_path("missing-secret")).await,
            Err(ConfigError::IoError(_))
        ));
    }

    #[tokio::test]
    async fn secret_reference_without_resolver_is_rejected_in_every_mode() {
        let path = write_config(
            "missing-resolver",
            "[server]\nhost='127.0.0.1'\nport=8080\n[custom]\njwt_secret='${secret:jwt}'\n",
        );
        for service in [
            ConfigService::development(&path),
            ConfigService::new(ConfigOptions::test(&path)),
            ConfigService::production(&path),
        ] {
            let error = service.build_snapshot(1, []).await.unwrap_err();
            assert!(matches!(error, ConfigError::SecretResolveError { .. }));
        }
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rabbitmq_topology_identities_reject_protected_references_before_resolution() {
        let fields = [
            "name",
            "exchange_name",
            "routing_key",
            "dead_letter_exchange",
            "dead_letter_routing_key",
        ];
        for reference in [
            "${secret:rabbitmq.topology.identity}",
            "${file:/run/secrets/rabbitmq-topology-identity}",
        ] {
            for (field_index, field) in fields.iter().enumerate() {
                let mut identities = [
                    "orders.created",
                    "orders",
                    "orders.created",
                    "orders.dlx",
                    "orders.failed",
                ];
                identities[field_index] = reference;
                let source = format!(
                    r#"
[[rabbitmq.topology.queues]]
name = "{}"
exchange_name = "{}"
routing_key = "{}"
dead_letter_exchange = "{}"
dead_letter_routing_key = "{}"
retention = {{ main_max_messages = 100, main_max_bytes = 1048576, retry_bucket_max_messages = 10, retry_bucket_max_bytes = 262144, dead_letter_max_messages = 10, dead_letter_max_bytes = 262144 }}
"#,
                    identities[0], identities[1], identities[2], identities[3], identities[4],
                );
                let path = write_config("rabbitmq-protected-topology-identity", &source);

                for mode in [
                    ConfigMode::Development,
                    ConfigMode::Test,
                    ConfigMode::Production,
                ] {
                    let service = ConfigService::new(ConfigOptions::new(&path, mode));
                    let error = service.build_snapshot(1, []).await.unwrap_err();
                    assert!(matches!(
                        error,
                        ConfigError::ValidationError(message)
                            if message == format!(
                                "protected config references are not permitted for RabbitMQ topology identity at rabbitmq.topology.queues.0.{field}"
                            )
                    ));
                }

                let _ = std::fs::remove_file(path);
            }
        }
    }

    #[tokio::test]
    async fn production_without_secret_references_does_not_require_a_resolver() {
        let path = write_config("no-secrets", "[server]\nhost='127.0.0.1'\nport=8080\n");
        let service = ConfigService::production(&path);
        let snapshot = service.build_snapshot(1, []).await.unwrap();
        assert!(snapshot.metadata().secret_bindings.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn effective_config_redacts_resolved_secret_and_records_version() {
        let path = write_config(
            "redaction",
            "[server]\nhost='127.0.0.1'\nport=8080\n[custom]\njwt_secret='${secret:jwt}'\n",
        );
        let service = ConfigService::with_options_and_secret_resolver(
            ConfigOptions::production(&path),
            VersionedSecretResolver,
        );
        let snapshot = service.build_snapshot(3, []).await.unwrap();

        assert_eq!(snapshot.values()["custom.jwt_secret"], "do-not-print-me");
        assert_eq!(snapshot.redacted_values()["custom.jwt_secret"], REDACTED);
        assert_eq!(
            snapshot.metadata().secret_bindings[0].version.as_deref(),
            Some("v7")
        );
        assert_eq!(
            snapshot.metadata().secret_bindings[0].provider,
            "test-vault"
        );
        assert!(!format!("{snapshot:?}").contains("do-not-print-me"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn resolved_secret_wrong_type_never_reaches_schema_error_surfaces() {
        let path = write_config(
            "wrong-type-secret-binding",
            "[server]\nport = '${secret:server.port}'\n",
        );
        let service = ConfigService::with_options_and_secret_resolver(
            ConfigOptions::production(&path),
            SentinelSecretResolver,
        );
        let config_error = service.build_snapshot(1, []).await.unwrap_err();

        assert!(matches!(
            &config_error,
            ConfigError::ValidationError(message)
                if message == "configuration schema validation failed after resolving protected values"
        ));
        for sentinel in [
            RESOLVED_BINDING_SENTINEL,
            "resolved-binding-",
            "quoted",
            "second-line-secret-sentinel",
        ] {
            assert_error_chain_does_not_contain(&config_error, sentinel);
            let converted = InjectionError::from(config_error.clone());
            assert_error_chain_does_not_contain(&converted, sentinel);
        }

        let mut injectable_service = ConfigService::with_options_and_secret_resolver(
            ConfigOptions::production(&path),
            SentinelSecretResolver,
        );
        let injection_error = injectable_service.initialize().await.unwrap_err();
        for sentinel in [
            RESOLVED_BINDING_SENTINEL,
            "resolved-binding-",
            "quoted",
            "second-line-secret-sentinel",
        ] {
            assert_error_chain_does_not_contain(&injection_error, sentinel);
        }

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn resolved_file_wrong_type_never_reaches_schema_error_surfaces() {
        const FILE_SENTINEL: &str = r#"resolved-file-"quoted"-\slash-secret-sentinel"#;

        let secret_path = test_path("wrong-type-file-value");
        std::fs::write(&secret_path, FILE_SENTINEL).unwrap();
        let path = write_config(
            "wrong-type-file-binding",
            &format!("[server]\nport = '${{file:{}}}'\n", secret_path.display()),
        );
        let service = ConfigService::production(&path);
        let config_error = service.build_snapshot(1, []).await.unwrap_err();

        assert!(matches!(
            &config_error,
            ConfigError::ValidationError(message)
                if message == "configuration schema validation failed after resolving protected values"
        ));
        for sentinel in [
            FILE_SENTINEL,
            "resolved-file-",
            "quoted",
            "slash-secret-sentinel",
        ] {
            assert_error_chain_does_not_contain(&config_error, sentinel);
            let converted = InjectionError::from(config_error.clone());
            assert_error_chain_does_not_contain(&converted, sentinel);
        }

        let mut injectable_service = ConfigService::production(&path);
        let injection_error = injectable_service.initialize().await.unwrap_err();
        for sentinel in [
            FILE_SENTINEL,
            "resolved-file-",
            "quoted",
            "slash-secret-sentinel",
        ] {
            assert_error_chain_does_not_contain(&injection_error, sentinel);
        }

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(secret_path);
    }

    #[tokio::test]
    async fn full_config_debug_redacts_bound_http_client_user_agents() {
        const FILE_USER_AGENT_SENTINEL: &str = "http-file-user-agent-secret-sentinel";

        let user_agent_path = test_path("http-client-user-agent");
        std::fs::write(&user_agent_path, FILE_USER_AGENT_SENTINEL).unwrap();
        let path = write_config(
            "http-client-user-agent-redaction",
            &format!(
                r#"
[http_client_factory]
user_agent = "${{secret:http.factory_user_agent}}"

[http_client_factory.clients.inventory]
base_address = "https://inventory.example.test/api"
user_agent = "${{file:{}}}"
"#,
                user_agent_path.display()
            ),
        );
        let service = ConfigService::with_options_and_secret_resolver(
            ConfigOptions::production(&path),
            SentinelSecretResolver,
        );
        let snapshot = service.build_snapshot(1, []).await.unwrap();
        let factory = snapshot.config().http_client_factory.as_ref().unwrap();
        let client = &factory.clients["inventory"];

        assert_eq!(factory.user_agent, RESOLVED_BINDING_SENTINEL);
        assert_eq!(client.user_agent.as_deref(), Some(FILE_USER_AGENT_SENTINEL));
        assert!(snapshot.metadata().secret_bindings.iter().any(|binding| {
            binding.config_key == "http_client_factory.user_agent"
                && binding.provider == "sentinel-test-provider"
        }));
        assert!(snapshot.metadata().secret_bindings.iter().any(|binding| {
            binding.config_key == "http_client_factory.clients.inventory.user_agent"
                && binding.provider == "file"
        }));

        let debug = format!("{:#?}", snapshot.config());
        assert!(!debug.contains(RESOLVED_BINDING_SENTINEL));
        assert!(!debug.contains(FILE_USER_AGENT_SENTINEL));
        assert!(debug.contains(REDACTED));

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(user_agent_path);
    }

    #[tokio::test]
    async fn http_client_operational_view_redacts_base_address_and_every_header_value() {
        const BASE_ADDRESS_SENTINEL: &str = "http-base-address-sentinel";
        const CUSTOM_HEADER_SENTINEL: &str = "http-custom-header-sentinel";
        const GENERIC_CUSTOM_VALUE: &str = "generic-custom-value-sentinel";

        let path = write_config(
            "http-client-operational-redaction",
            r#"
[http_client_factory.clients.inventory]
base_address = "https://http-base-address-sentinel.example.test/api"
headers = { Authorization = "${secret:http.authorization}", "X-Lily-Context" = "http-custom-header-sentinel" }

[custom]
public_label = "generic-custom-value-sentinel"
"#,
        );
        let service = ConfigService::with_options_and_secret_resolver(
            ConfigOptions::production(&path),
            VersionedSecretResolver,
        );
        let snapshot = Arc::new(service.build_snapshot(1, []).await.unwrap());
        *service.snapshot.write().await = Arc::clone(&snapshot);

        let base_address_key = "http_client_factory.clients.inventory.base_address";
        let authorization_key = "http_client_factory.clients.inventory.headers.Authorization";
        let custom_header_key = "http_client_factory.clients.inventory.headers.X-Lily-Context";

        assert!(snapshot.values()[base_address_key].contains(BASE_ADDRESS_SENTINEL));
        assert_eq!(snapshot.values()[authorization_key], "do-not-print-me");
        assert_eq!(snapshot.values()[custom_header_key], CUSTOM_HEADER_SENTINEL);
        assert_eq!(
            snapshot.values()["custom.public_label"],
            GENERIC_CUSTOM_VALUE
        );

        let operational_view = service.redacted_effective_config().await;
        assert_eq!(operational_view.values[base_address_key], REDACTED);
        assert_eq!(operational_view.values[authorization_key], REDACTED);
        assert_eq!(operational_view.values[custom_header_key], REDACTED);
        assert_eq!(
            operational_view.values["custom.public_label"],
            GENERIC_CUSTOM_VALUE
        );
        assert!(
            operational_view
                .metadata
                .redacted_keys
                .contains(&base_address_key.to_string())
        );
        assert!(
            operational_view
                .metadata
                .redacted_keys
                .contains(&authorization_key.to_string())
        );
        assert!(
            operational_view
                .metadata
                .redacted_keys
                .contains(&custom_header_key.to_string())
        );

        let binding = operational_view
            .metadata
            .secret_bindings
            .iter()
            .find(|binding| binding.config_key == authorization_key)
            .expect("Authorization secret binding");
        assert_eq!(binding.version.as_deref(), Some("v7"));
        assert_eq!(binding.provider, "test-vault");

        let debug = format!("{operational_view:?}");
        for secret in [
            BASE_ADDRESS_SENTINEL,
            CUSTOM_HEADER_SENTINEL,
            "do-not-print-me",
        ] {
            assert!(!debug.contains(secret), "operational Debug leaked {secret}");
        }
        assert!(debug.contains(GENERIC_CUSTOM_VALUE));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn production_rejects_an_already_expired_secret_lease_without_leaking_its_value() {
        let path = write_config(
            "expired-secret-lease",
            "[server]\nhost='127.0.0.1'\nport=8080\n[custom]\njwt_secret='${secret:jwt}'\n",
        );
        let service = ConfigService::with_options_and_secret_resolver(
            ConfigOptions::production(&path),
            ExpiredSecretResolver,
        );

        let error = service.build_snapshot(1, []).await.unwrap_err();
        let rendered = error.to_string();
        assert!(matches!(
            error,
            ConfigError::SecretResolveError { ref key, ref error }
                if key == "jwt" && error.contains("expired")
        ));
        assert!(!rendered.contains("never-publish-this-value"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn secret_inside_typed_factory_cell_is_resolved_and_redacted() {
        let path = write_config(
            "cell-secret",
            r#"
[database]
mode = "factory"

[[database.cells]]
name = "primary"
database_type = "postgresql"
database_name = "app"
connection_string = "${secret:database.primary_url}"
"#,
        );
        let service = ConfigService::with_options_and_secret_resolver(
            ConfigOptions::production(&path),
            VersionedSecretResolver,
        );
        let snapshot = service.build_snapshot(1, []).await.unwrap();

        let cell = &snapshot
            .config()
            .database
            .as_ref()
            .unwrap()
            .cells
            .as_ref()
            .unwrap()[0];
        assert_eq!(cell.connection_string.as_deref(), Some("do-not-print-me"));
        assert_eq!(
            snapshot.redacted_values()["database.cells.0.connection_string"],
            REDACTED
        );
        assert_eq!(
            snapshot.metadata().secret_bindings[0].config_key,
            "database.cells.0.connection_string"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn development_legacy_environment_is_mapped_without_splitting_field_underscores() {
        let path = write_config("legacy-env", "[database]\nconnection_string='old'\n");
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service
            .build_snapshot(
                1,
                [(
                    "LILY_DATABASE_CONNECTION_STRING".to_string(),
                    "new".to_string(),
                )],
            )
            .await
            .unwrap();
        assert_eq!(snapshot.values()["database.connection_string"], "new");
        assert_eq!(snapshot.metadata().warnings.len(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn canonical_environment_wins_over_legacy_deterministically() {
        let path = write_config("env-precedence", "[server]\nport=8080\n");
        let service = ConfigService::new(ConfigOptions::test(&path));
        let snapshot = service
            .build_snapshot(
                1,
                [
                    ("LILY__SERVER__PORT".to_string(), "9090".to_string()),
                    ("LILY_SERVER_PORT".to_string(), "7070".to_string()),
                ],
            )
            .await
            .unwrap();

        assert_eq!(snapshot.config().server.port, 9090);
        assert_eq!(snapshot.values()["server.port"], "9090");
        assert!(
            snapshot
                .metadata()
                .warnings
                .iter()
                .any(|warning| warning.contains("overrides legacy"))
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn bootstrap_path_and_mode_must_be_explicit_together() {
        assert!(matches!(
            ConfigOptions::from_bootstrap_values(Some("lily.toml".into()), None),
            Err(ConfigError::ValidationError(_))
        ));
        assert!(matches!(
            ConfigOptions::from_bootstrap_values(None, Some("production")),
            Err(ConfigError::ValidationError(_))
        ));

        let options = ConfigOptions::from_bootstrap_values(
            Some("/etc/lily/lily.toml".into()),
            Some("production"),
        )
        .unwrap();
        assert_eq!(options.mode(), ConfigMode::Production);
        assert_eq!(options.path(), Path::new("/etc/lily/lily.toml"));
    }

    #[test]
    fn postgresql_mode_and_pool_contract_is_fail_closed() {
        let mut config = LilyConfig {
            postgresql: Some(crate::PgConfig::default()),
            ..LilyConfig::default()
        };
        assert!(matches!(
            validate_config(&config, ConfigMode::Development),
            Err(ConfigError::ValidationError(message))
                if message == "postgresql.connection_string is required in single mode"
        ));

        let postgresql = config.postgresql.as_mut().unwrap();
        postgresql.mode = crate::PgMode::Factory;
        postgresql.cells.push(crate::PgCellConfig {
            name: "orders".into(),
            connection_string: "postgresql://db/orders".into(),
            ..crate::PgCellConfig::default()
        });
        assert!(validate_config(&config, ConfigMode::Development).is_ok());

        let postgresql = config.postgresql.as_mut().unwrap();
        postgresql.cells.push(crate::PgCellConfig {
            name: "orders".into(),
            connection_string: "postgresql://db/duplicate".into(),
            ..crate::PgCellConfig::default()
        });
        assert!(matches!(
            validate_config(&config, ConfigMode::Development),
            Err(ConfigError::ValidationError(message))
                if message.contains("duplicate name")
        ));
    }

    #[test]
    fn canonical_postgresql_section_is_not_migrated_to_custom_in_development() {
        let mut root: toml::Value = toml::from_str(
            "[postgresql]\nmode = 'single'\nconnection_string = 'postgresql://db/app'\n",
        )
        .unwrap();
        let mut warnings = Vec::new();

        migrate_development_top_level(&mut root, ConfigMode::Development, &mut warnings).unwrap();

        assert!(root.get("postgresql").is_some());
        assert!(root.get("custom").is_none());
        assert!(warnings.is_empty());
    }

    #[test]
    fn postgresql_transaction_cleanup_budget_defaults_and_rejects_zero() {
        let mut pool: crate::PgPoolConfig = toml::from_str("max_size = 1").unwrap();
        assert_eq!(pool.transaction_cleanup_timeout_secs, 5);
        pool.transaction_cleanup_timeout_secs = 0;
        assert!(matches!(
            validate_postgresql_pool("postgresql.pool", &pool),
            Err(ConfigError::ValidationError(message))
                if message.contains("transaction_cleanup_timeout_secs")
        ));
    }

    #[tokio::test]
    async fn canonical_rabbitmq_topology_stays_in_the_typed_snapshot_in_development_and_test() {
        let path = write_config(
            "canonical-rabbitmq-topology",
            r#"
[[rabbitmq.topology.queues]]
name = "orders.created"
exchange_name = "orders"
routing_key = "orders.created"
retention = { main_max_messages = 100, main_max_bytes = 1048576, retry_bucket_max_messages = 10, retry_bucket_max_bytes = 262144, dead_letter_max_messages = 10, dead_letter_max_bytes = 262144 }
"#,
        );

        for mode in [ConfigMode::Development, ConfigMode::Test] {
            let service = ConfigService::new(ConfigOptions::new(&path, mode));
            let snapshot = service.build_snapshot(1, []).await.unwrap();
            let topology = &snapshot.config().rabbitmq.topology;
            assert_eq!(topology.queues.len(), 1);
            assert_eq!(topology.queues[0].name, "orders.created");
            assert_eq!(topology.queues[0].exchange_name, "orders");
            assert_eq!(topology.queues[0].routing_key, "orders.created");
            assert!(snapshot.config().custom.is_empty());
            assert!(snapshot.metadata().warnings.is_empty());
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn postgresql_production_requires_verify_full_tls() {
        let config = LilyConfig {
            postgresql: Some(crate::PgConfig {
                connection_string: Some("postgresql://db/app".into()),
                tls: crate::PgTlsConfig {
                    mode: crate::PgTlsMode::Disable,
                    additional_ca_bundle: None,
                },
                ..crate::PgConfig::default()
            }),
            ..LilyConfig::default()
        };

        assert!(matches!(
            validate_config(&config, ConfigMode::Production),
            Err(ConfigError::ValidationError(message))
                if message == "postgresql.tls.mode must be verify-full in production"
        ));
    }

    #[test]
    fn postgresql_connection_and_ca_fields_are_operationally_redacted() {
        assert!(is_sensitive_key("postgresql.connection_string"));
        assert!(is_sensitive_key(
            "postgresql.cells.0.tls.additional_ca_bundle"
        ));
    }

    #[test]
    fn rabbitmq_tls_contract_is_fail_closed() {
        let mut tls = crate::RabbitMqTlsConfig {
            additional_ca_bundle: Some("/run/secrets/rabbit-ca.pem".into()),
            client_certificate_chain: None,
            client_private_key: None,
        };
        assert!(matches!(
            validate_rabbitmq_tls("queue_client", false, &tls),
            Err(ConfigError::ValidationError(message))
                if message == "queue_client.tls cannot be configured when use_tls=false"
        ));

        tls.additional_ca_bundle = None;
        tls.client_certificate_chain = Some("/run/secrets/rabbit-client.pem".into());
        assert!(matches!(
            validate_rabbitmq_tls("queue_client", true, &tls),
            Err(ConfigError::ValidationError(message))
                if message.contains("requires both client_certificate_chain and client_private_key")
        ));

        tls.client_private_key = Some("relative-client-key.pem".into());
        assert!(matches!(
            validate_rabbitmq_tls("queue_client", true, &tls),
            Err(ConfigError::ValidationError(message))
                if message == "queue_client.tls.client_private_key must be an absolute path"
        ));
    }

    #[test]
    fn factory_cell_tls_is_attached_to_the_current_cell_and_redacted() {
        let config: LilyConfig = toml::from_str(
            r#"
[queue_client]
mode = "factory"

[[queue_client.cells]]
name = "orders"
connection_string = "amqps://orders:secret@rabbit.internal/%2f"
use_tls = true

[queue_client.cells.tls]
additional_ca_bundle = "/run/secrets/orders-ca.pem"
client_certificate_chain = "/run/secrets/orders-client.pem"
client_private_key = "/run/secrets/orders-client-key.pem"

[[queue_client.cells]]
name = "products"
connection_string = "amqps://products:secret@rabbit.internal/%2f"
use_tls = true
"#,
        )
        .expect("parse RabbitMQ factory TLS configuration");
        let cells = config
            .queue_client
            .as_ref()
            .and_then(|client| client.cells.as_ref())
            .expect("queue client cells");
        assert!(cells[0].tls.additional_ca_bundle.is_some());
        assert!(cells[0].tls.client_certificate_chain.is_some());
        assert!(cells[1].tls.additional_ca_bundle.is_none());

        let debug = format!("{:?}", config.queue_client.as_ref().unwrap());
        assert!(!debug.contains("orders-ca.pem"));
        assert!(!debug.contains("orders-client-key.pem"));
        assert!(is_sensitive_key(
            "queue_client.cells.0.tls.client_certificate_chain"
        ));
        assert!(is_sensitive_key(
            "queue_client.cells.0.tls.client_private_key"
        ));
    }
}
