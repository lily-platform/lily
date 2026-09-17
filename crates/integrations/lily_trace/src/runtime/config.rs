use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};

/// Tracing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceConfig {
    /// Enable/disable tracing
    #[serde(default)]
    pub enabled: bool,

    /// Minimum level to trace
    #[serde(default = "default_level")]
    pub level: String,

    /// Service name for traces
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// Service version
    #[serde(default)]
    pub service_version: Option<String>,

    /// OpenTelemetry `deployment.environment` resource label. This never
    /// selects Lily's configuration or secret-resolution mode.
    #[serde(default)]
    pub environment: Option<String>,

    /// Sampling configuration
    #[serde(default)]
    pub sampling: SamplingConfig,

    /// Export configuration
    #[serde(default)]
    pub export: ExportConfig,

    /// Dynamic filtering rules
    #[serde(default)]
    pub filters: Vec<FilterRule>,

    /// Custom attributes to add to all spans
    #[serde(default)]
    pub global_attributes: HashMap<String, String>,

    /// Enable span events
    #[serde(default = "default_true")]
    pub enable_events: bool,

    /// Maximum span attributes
    #[serde(default = "default_max_attributes")]
    pub max_attributes: usize,

    /// Maximum event attributes
    #[serde(default = "default_max_event_attributes")]
    pub max_event_attributes: usize,

    /// Maximum events retained by one span when events are enabled.
    #[serde(default = "default_max_events_per_span")]
    pub max_events_per_span: usize,

    /// Component identities for workers hosted by this process.
    #[serde(default)]
    pub cells: Vec<TraceCellConfig>,
}

/// Explicit tracing ownership policy for an application adapter.
///
/// `Disabled` never probes the current working directory. `OwnedPath` is the
/// strict file-backed production mode, while `External` means a higher-level
/// process composition root owns installation and shutdown.
#[derive(Debug, Clone, Default)]
pub enum TracingMode {
    /// Do not install or look for tracing configuration.
    #[default]
    Disabled,
    /// Install a runtime from an already constructed configuration.
    OwnedConfig(Box<TraceConfig>),
    /// Strictly load configuration from the specified path and install it.
    OwnedPath(PathBuf),
    /// A higher process composition root owns tracing installation and shutdown.
    External,
}

impl TracingMode {
    /// Creates an application-owned mode from an in-memory configuration.
    pub fn owned_config(config: TraceConfig) -> Self {
        Self::OwnedConfig(Box::new(config))
    }

    /// Creates an application-owned mode that strictly loads `path` at startup.
    pub fn owned_path(path: impl Into<PathBuf>) -> Self {
        Self::OwnedPath(path.into())
    }

    /// Resolve configuration only for modes that request an owned runtime.
    /// File-backed loading is strict and therefore never degrades to disabled
    /// tracing on a missing, malformed, unknown, or unsupported value.
    pub fn resolve_owned_config(&self) -> Result<Option<TraceConfig>, TraceConfigLoadError> {
        match self {
            Self::Disabled | Self::External => Ok(None),
            Self::OwnedConfig(config) => Ok(Some(config.as_ref().clone())),
            Self::OwnedPath(path) => TraceConfig::try_load_from_path(path).map(Some),
        }
    }
}

/// Configuration identity for a component hosted inside the current service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TraceCellConfig {
    /// Stable, process-unique component identifier.
    pub id: String,
    /// Rust worker or component type used to select the identity.
    pub worker_type: String,
    /// Low-cardinality configured component name.
    pub worker_name: String,
    /// Low-cardinality component category such as `manage_worker`.
    pub kind: String,
}

/// Sampling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SamplingConfig {
    /// Sampling strategy
    #[serde(default)]
    pub strategy: SamplingStrategy,

    /// Sample rate for probability sampling (0.0 - 1.0)
    #[serde(default = "default_sample_rate")]
    pub rate: f64,
}

/// Sampling strategy
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SamplingStrategy {
    /// Always sample all traces
    #[default]
    Always,
    /// Never sample (disabled)
    Never,
    /// Probability-based sampling
    Probability,
}

/// Export configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportConfig {
    /// Export to console (for development)
    #[serde(default)]
    pub console: bool,

    /// Export to JSON file
    #[serde(default)]
    pub file: Option<FileExportConfig>,

    /// OpenTelemetry OTLP export
    #[serde(default)]
    pub otlp: Option<OtlpExportConfig>,
}

/// Bounded JSONL file export configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileExportConfig {
    /// Active JSONL file path. The deployment must give each concurrently
    /// running process an exclusive path; cross-process rotation is not shared.
    pub path: String,

    /// Time boundary that rotates the active file in addition to its byte cap.
    #[serde(default)]
    pub rotation: FileRotation,

    /// Hard maximum size for one active or retained file. A record that would
    /// cross this bound is written only after the active file is rotated.
    #[serde(default = "default_file_max_bytes")]
    pub max_file_bytes: u64,

    /// Maximum total file count, including the active file.
    #[serde(default = "default_file_max_files")]
    pub max_files: usize,

    /// Maximum number of complete JSONL records waiting for the writer thread.
    #[serde(default = "default_file_buffered_lines")]
    pub buffered_lines: usize,

    /// Maximum serialized bytes accepted for one JSONL record.
    #[serde(default = "default_file_max_record_bytes")]
    pub max_record_bytes: usize,
}

/// Supported time-based file rotation boundary.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FileRotation {
    /// Rotate at each hourly boundary in addition to the byte limit.
    Hourly,
    /// Rotate at each daily boundary in addition to the byte limit.
    #[default]
    Daily,
}

/// OTLP export configuration
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpExportConfig {
    /// OTLP endpoint
    pub endpoint: String,

    /// Transport protocol. Only `grpc` (or its `tonic` alias) is supported.
    #[serde(default = "default_otlp_protocol")]
    pub protocol: String,

    /// Headers for authentication
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Timeout for one OTLP export request, in seconds.
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Hard per-signal item capacity.
    #[serde(default = "default_export_queue_items")]
    pub max_queue_items: usize,

    /// Hard per-signal estimated byte capacity.
    #[serde(default = "default_export_queue_bytes")]
    pub max_queue_bytes: u64,

    /// Maximum number of records in one export request.
    #[serde(default = "default_export_batch_size")]
    pub max_batch_size: usize,

    /// Maximum batching delay, in milliseconds.
    #[serde(default = "default_export_flush_interval_millis")]
    pub flush_interval_millis: u64,

    /// Period between metric export attempts, in milliseconds.
    #[serde(default = "default_metrics_export_interval_millis")]
    pub metrics_export_interval_millis: u64,

    /// Maximum retries after the initial exporter attempt.
    #[serde(default = "default_export_retries")]
    pub max_export_retries: usize,

    /// Initial exponential retry delay, in milliseconds.
    #[serde(default = "default_export_retry_backoff_millis")]
    pub retry_backoff_millis: u64,
}

impl fmt::Debug for OtlpExportConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct RedactedHeaders<'a>(&'a HashMap<String, String>);

        impl fmt::Debug for RedactedHeaders<'_> {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_map()
                    .entries(self.0.keys().map(|key| (key, "<redacted>")))
                    .finish()
            }
        }

        formatter
            .debug_struct("OtlpExportConfig")
            .field("endpoint", &self.endpoint)
            .field("protocol", &self.protocol)
            .field("headers", &RedactedHeaders(&self.headers))
            .field("timeout", &self.timeout)
            .field("max_queue_items", &self.max_queue_items)
            .field("max_queue_bytes", &self.max_queue_bytes)
            .field("max_batch_size", &self.max_batch_size)
            .field("flush_interval_millis", &self.flush_interval_millis)
            .field(
                "metrics_export_interval_millis",
                &self.metrics_export_interval_millis,
            )
            .field("max_export_retries", &self.max_export_retries)
            .field("retry_backoff_millis", &self.retry_backoff_millis)
            .finish()
    }
}

/// Dynamic filter rule
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilterRule {
    /// Target module path
    pub target: String,

    /// Level for this target
    pub level: String,
}

fn default_level() -> String {
    "info".to_string()
}

fn default_service_name() -> String {
    "lily-service".to_string()
}

fn default_sample_rate() -> f64 {
    1.0
}

fn default_true() -> bool {
    true
}

fn default_max_attributes() -> usize {
    128
}

fn default_max_event_attributes() -> usize {
    128
}

fn default_max_events_per_span() -> usize {
    128
}

fn default_otlp_protocol() -> String {
    "grpc".to_string()
}

fn default_timeout() -> u64 {
    10
}

fn default_file_max_bytes() -> u64 {
    128 * 1024 * 1024
}

fn default_file_max_files() -> usize {
    7
}

fn default_file_buffered_lines() -> usize {
    4_096
}

fn default_file_max_record_bytes() -> usize {
    16 * 1024
}

fn default_export_queue_items() -> usize {
    2_048
}

fn default_export_queue_bytes() -> u64 {
    8 * 1024 * 1024
}

fn default_export_batch_size() -> usize {
    512
}

fn default_export_flush_interval_millis() -> u64 {
    5_000
}

fn default_metrics_export_interval_millis() -> u64 {
    30_000
}

fn default_export_retries() -> usize {
    2
}

fn default_export_retry_backoff_millis() -> u64 {
    200
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            strategy: SamplingStrategy::Always,
            rate: 1.0,
        }
    }
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            level: default_level(),
            service_name: default_service_name(),
            service_version: None,
            environment: None,
            sampling: SamplingConfig::default(),
            export: ExportConfig::default(),
            filters: Vec::new(),
            global_attributes: HashMap::new(),
            enable_events: true,
            max_attributes: 128,
            max_event_attributes: 128,
            max_events_per_span: 128,
            cells: Vec::new(),
        }
    }
}

/// Failure while strictly loading and validating a tracing configuration file.
#[derive(Debug, thiserror::Error)]
pub enum TraceConfigLoadError {
    /// The requested configuration file could not be read.
    #[error("failed to read tracing config {path}: {source}")]
    Read {
        /// Display form of the requested path.
        path: String,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// TOML parsing or unknown-field validation failed.
    #[error("failed to parse tracing config {path}: {source}")]
    Parse {
        /// Display form of the requested path.
        path: String,
        /// Underlying TOML deserialization error.
        #[source]
        source: toml::de::Error,
    },
    /// The parsed configuration violates a Lily runtime constraint.
    #[error("invalid tracing config {path}: {message}")]
    Validation {
        /// Display form of the requested path.
        path: String,
        /// Validation failure without secret-bearing configuration values.
        message: String,
    },
}

impl TraceConfig {
    /// Strict production loader. Missing, unreadable, malformed, unknown, and
    /// unsupported configuration is returned to the caller instead of being
    /// silently converted into disabled tracing.
    pub fn try_load() -> Result<Self, TraceConfigLoadError> {
        Self::try_load_from_path("lily_trace.toml")
    }

    /// Strictly loads and validates tracing configuration from `path`.
    pub fn try_load_from_path<P: AsRef<Path>>(path: P) -> Result<Self, TraceConfigLoadError> {
        let path = path.as_ref();
        let display = path.display().to_string();
        let content =
            std::fs::read_to_string(path).map_err(|source| TraceConfigLoadError::Read {
                path: display.clone(),
                source,
            })?;
        let config = toml::from_str::<TraceConfig>(&content).map_err(|source| {
            TraceConfigLoadError::Parse {
                path: display.clone(),
                source,
            }
        })?;
        config
            .validate()
            .map_err(|message| TraceConfigLoadError::Validation {
                path: display,
                message,
            })?;
        Ok(config)
    }

    /// Returns whether this configuration requests runtime installation.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Validates bounds, exporter compatibility, filters, and component identity.
    pub fn validate(&self) -> Result<(), String> {
        if !self.sampling.rate.is_finite() || self.sampling.rate < 0.0 || self.sampling.rate > 1.0 {
            return Err("sampling.rate must be between 0.0 and 1.0".to_string());
        }

        if self.service_name.trim().is_empty() {
            return Err("service_name cannot be empty".to_string());
        }

        if !matches!(
            self.level.to_ascii_lowercase().as_str(),
            "trace" | "debug" | "info" | "warn" | "error"
        ) {
            return Err(format!("unsupported trace level: {}", self.level));
        }

        if self.max_attributes == 0 || self.max_attributes > u32::MAX as usize {
            return Err("max_attributes must be between 1 and u32::MAX".to_string());
        }
        if self.max_event_attributes == 0 || self.max_event_attributes > u32::MAX as usize {
            return Err("max_event_attributes must be between 1 and u32::MAX".to_string());
        }
        if self.max_events_per_span == 0 || self.max_events_per_span > 4_096 {
            return Err("max_events_per_span must be between 1 and 4096".to_string());
        }

        match self.sampling.strategy {
            SamplingStrategy::Always if self.sampling.rate != 1.0 => {
                return Err("sampling.rate must be 1.0 for the always strategy".to_string());
            }
            SamplingStrategy::Never if self.sampling.rate != 0.0 => {
                return Err("sampling.rate must be 0.0 for the never strategy".to_string());
            }
            SamplingStrategy::Always | SamplingStrategy::Never | SamplingStrategy::Probability => {}
        }
        if self.export.file.is_some() && self.export.otlp.is_some() {
            return Err("file and OTLP exporters cannot be enabled together".to_string());
        }
        if self.export.file.is_some() && self.export.console {
            return Err("file and console exporters cannot be enabled together".to_string());
        }

        if let Some(file) = &self.export.file {
            if file.path.trim().is_empty() {
                return Err("export.file.path cannot be empty".to_string());
            }
            if Path::new(&file.path).file_name().is_none() {
                return Err("export.file.path must name a JSONL file".to_string());
            }
            if file.max_file_bytes < 1024 * 1024 || file.max_file_bytes > 1024 * 1024 * 1024 {
                return Err(
                    "export.file.max_file_bytes must be between 1048576 and 1073741824".to_string(),
                );
            }
            if file.max_files < 2 || file.max_files > 100 {
                return Err("export.file.max_files must be between 2 and 100".to_string());
            }
            if file.buffered_lines == 0 || file.buffered_lines > 65_536 {
                return Err("export.file.buffered_lines must be between 1 and 65536".to_string());
            }
            if file.max_record_bytes < 1024
                || file.max_record_bytes > 256 * 1024
                || file.max_record_bytes as u64 > file.max_file_bytes
            {
                return Err(
                    "export.file.max_record_bytes must be between 1024 and 262144 and not exceed max_file_bytes"
                        .to_string(),
                );
            }
            let maximum_buffer_bytes = file
                .buffered_lines
                .checked_mul(file.max_record_bytes)
                .ok_or_else(|| "export.file buffer byte bound overflowed".to_string())?;
            if maximum_buffer_bytes > 64 * 1024 * 1024 {
                return Err(
                    "export.file buffered_lines * max_record_bytes must not exceed 67108864"
                        .to_string(),
                );
            }
        }

        if let Some(otlp) = &self.export.otlp {
            if otlp.endpoint.trim().is_empty()
                || !(otlp.endpoint.starts_with("http://") || otlp.endpoint.starts_with("https://"))
            {
                return Err("export.otlp.endpoint must be an http(s) URL".to_string());
            }
            if !matches!(
                otlp.protocol.to_ascii_lowercase().as_str(),
                "grpc" | "tonic"
            ) {
                return Err(format!(
                    "unsupported OTLP protocol '{}'; only gRPC is implemented",
                    otlp.protocol
                ));
            }
            build_otlp_metadata(&otlp.headers)?;
            if otlp.timeout == 0 {
                return Err("export.otlp.timeout must be greater than zero".to_string());
            }
            if otlp.max_queue_items == 0 || otlp.max_queue_items > 65_536 {
                return Err("export.otlp.max_queue_items must be between 1 and 65536".to_string());
            }
            if otlp.max_queue_bytes < 64 * 1024 || otlp.max_queue_bytes > 256 * 1024 * 1024 {
                return Err(
                    "export.otlp.max_queue_bytes must be between 65536 and 268435456".to_string(),
                );
            }
            if otlp.max_batch_size == 0 || otlp.max_batch_size > otlp.max_queue_items {
                return Err(
                    "export.otlp.max_batch_size must be between 1 and max_queue_items".to_string(),
                );
            }
            if otlp.flush_interval_millis == 0 || otlp.flush_interval_millis > 60_000 {
                return Err(
                    "export.otlp.flush_interval_millis must be between 1 and 60000".to_string(),
                );
            }
            if otlp.metrics_export_interval_millis < 1_000
                || otlp.metrics_export_interval_millis > 300_000
            {
                return Err(
                    "export.otlp.metrics_export_interval_millis must be between 1000 and 300000"
                        .to_string(),
                );
            }
            if otlp.max_export_retries > 10 {
                return Err("export.otlp.max_export_retries must not exceed 10".to_string());
            }
            if otlp.retry_backoff_millis == 0 || otlp.retry_backoff_millis > 60_000 {
                return Err(
                    "export.otlp.retry_backoff_millis must be between 1 and 60000".to_string(),
                );
            }
        }

        for rule in &self.filters {
            if rule.target.trim().is_empty() {
                return Err("trace filter target cannot be empty".to_string());
            }
            let directive = format!("{}={}", rule.target, rule.level);
            directive
                .parse::<tracing_subscriber::filter::Directive>()
                .map_err(|error| format!("invalid trace filter '{directive}': {error}"))?;
        }

        for key in self.global_attributes.keys() {
            if key.trim().is_empty() {
                return Err("global attribute keys cannot be empty".to_string());
            }
        }

        let mut worker_types = std::collections::HashSet::new();
        let mut cell_ids = std::collections::HashSet::new();
        for cell in &self.cells {
            if cell.id.trim().is_empty() {
                return Err("trace cell id cannot be empty".to_string());
            }
            if cell.worker_type.trim().is_empty() {
                return Err("trace cell worker_type cannot be empty".to_string());
            }
            if cell.worker_name.trim().is_empty() {
                return Err("trace cell worker_name cannot be empty".to_string());
            }
            if cell.kind.trim().is_empty() {
                return Err("trace cell kind cannot be empty".to_string());
            }
            if !cell_ids.insert(cell.id.as_str()) {
                return Err(format!("duplicate trace cell id: {}", cell.id));
            }
            if !worker_types.insert(cell.worker_type.as_str()) {
                return Err(format!(
                    "duplicate trace cell worker_type: {}",
                    cell.worker_type
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn export_targets(&self) -> Vec<String> {
        let mut targets = Vec::new();
        if self.export.console {
            targets.push("console".to_string());
        }
        if self.export.file.is_some() {
            targets.push("file".to_string());
        }
        if self.export.otlp.is_some() {
            targets.push("otlp".to_string());
        }
        targets
    }
}

/// Convert configured ASCII OTLP headers into tonic metadata without ever
/// including a header value in a diagnostic.
pub(crate) fn build_otlp_metadata(
    headers: &HashMap<String, String>,
) -> Result<MetadataMap, String> {
    const MAX_HEADERS: usize = 32;
    const MAX_HEADER_NAME_BYTES: usize = 128;
    const MAX_HEADER_VALUE_BYTES: usize = 8 * 1024;

    if headers.len() > MAX_HEADERS {
        return Err(format!(
            "export.otlp.headers must not contain more than {MAX_HEADERS} entries"
        ));
    }

    let mut ordered = headers.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(name, _)| *name);
    let mut metadata = MetadataMap::with_capacity(ordered.len());
    for (name, value) in ordered {
        if name.is_empty() || name.len() > MAX_HEADER_NAME_BYTES {
            return Err(format!(
                "export.otlp.headers contains a header name outside the 1..={MAX_HEADER_NAME_BYTES} byte bound"
            ));
        }
        if value.len() > MAX_HEADER_VALUE_BYTES {
            return Err(format!(
                "export.otlp.headers.{name} exceeds the {MAX_HEADER_VALUE_BYTES} byte value bound"
            ));
        }
        let normalized = name.to_ascii_lowercase();
        if normalized.ends_with("-bin")
            || normalized.starts_with("grpc-")
            || matches!(normalized.as_str(), "content-type" | "te" | "user-agent")
        {
            return Err(format!(
                "export.otlp.headers.{name} is reserved or requires binary metadata"
            ));
        }
        let key = MetadataKey::<Ascii>::from_bytes(normalized.as_bytes())
            .map_err(|_| format!("export.otlp.headers contains invalid header name '{name}'"))?;
        let value = MetadataValue::<Ascii>::try_from(value.as_str()).map_err(|_| {
            format!("export.otlp.headers.{name} contains an invalid ASCII metadata value")
        })?;
        metadata.insert(key, value);
    }
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TraceConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.level, "info");
        assert_eq!(config.sampling.rate, 1.0);
    }

    #[test]
    fn test_validate() {
        let mut config = TraceConfig::default();
        assert!(config.validate().is_ok());

        config.sampling.rate = 1.5;
        assert!(config.validate().is_err());

        config.sampling.rate = 0.5;
        config.service_name = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn parses_and_validates_trace_cells() {
        let config: TraceConfig = toml::from_str(
            r#"
enabled = true
service_name = "consumer"

[[cells]]
id = "product-worker-id"
worker_type = "ProductManageWorker"
worker_name = "product"
kind = "manage_worker"
"#,
        )
        .unwrap();

        assert_eq!(config.cells.len(), 1);
        assert_eq!(config.cells[0].worker_type, "ProductManageWorker");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_duplicate_worker_type_cells() {
        let cell = TraceCellConfig {
            id: "first".to_string(),
            worker_type: "ProductManageWorker".to_string(),
            worker_name: "product".to_string(),
            kind: "manage_worker".to_string(),
        };
        let config = TraceConfig {
            cells: vec![
                cell.clone(),
                TraceCellConfig {
                    id: "second".to_string(),
                    ..cell
                },
            ],
            ..TraceConfig::default()
        };

        assert!(config.validate().unwrap_err().contains("duplicate"));
    }

    #[test]
    fn rejects_unsupported_protocol_and_inert_sampling_rates() {
        let config = TraceConfig {
            export: ExportConfig {
                otlp: Some(OtlpExportConfig {
                    endpoint: "http://localhost:4317".to_owned(),
                    protocol: "http/protobuf".to_owned(),
                    headers: HashMap::new(),
                    timeout: 10,
                    max_queue_items: default_export_queue_items(),
                    max_queue_bytes: default_export_queue_bytes(),
                    max_batch_size: default_export_batch_size(),
                    flush_interval_millis: default_export_flush_interval_millis(),
                    metrics_export_interval_millis: default_metrics_export_interval_millis(),
                    max_export_retries: default_export_retries(),
                    retry_backoff_millis: default_export_retry_backoff_millis(),
                }),
                ..ExportConfig::default()
            },
            ..TraceConfig::default()
        };
        assert!(config.validate().unwrap_err().contains("OTLP protocol"));

        let config = TraceConfig {
            sampling: SamplingConfig {
                strategy: SamplingStrategy::Never,
                rate: 1.0,
            },
            ..TraceConfig::default()
        };
        assert!(config.validate().unwrap_err().contains("never strategy"));
    }

    #[test]
    fn removed_pseudo_configuration_is_rejected_during_deserialization() {
        for removed in [
            "[sampling]\nstrategy = 'ratebased'\nrate = 1.0",
            "[sampling]\nstrategy = 'always'\nrate = 1.0\nrate_limit = 10",
            "[sampling]\nstrategy = 'always'\nrate = 1.0\nalways_sample_errors = true",
            "[export.jaeger]\nendpoint = 'http://localhost:14250'",
            "[export.file]\npath = 'trace.jsonl'\nformat = 'jsonlines'",
            "[export.file]\npath = 'trace.jsonl'\npretty = false",
        ] {
            assert!(toml::from_str::<TraceConfig>(removed).is_err(), "{removed}");
        }
    }

    #[test]
    fn file_export_defaults_are_bounded_and_validate() {
        let config: TraceConfig = toml::from_str(
            r#"
enabled = true

[export.file]
path = "trace.jsonl"
"#,
        )
        .unwrap();
        assert!(config.validate().is_ok());
        let file = config.export.file.unwrap();
        assert_eq!(file.rotation, FileRotation::Daily);
        assert_eq!(file.max_file_bytes, 128 * 1024 * 1024);
        assert_eq!(file.max_files, 7);
        assert_eq!(file.buffered_lines, 4_096);
        assert_eq!(file.max_record_bytes, 16 * 1024);
    }

    #[test]
    fn file_export_rejects_an_excessive_worst_case_queue_allocation() {
        let config: TraceConfig = toml::from_str(
            r#"
enabled = true

[export.file]
path = "trace.jsonl"
buffered_lines = 65536
max_record_bytes = 262144
"#,
        )
        .unwrap();

        assert!(config.validate().unwrap_err().contains("67108864"));
    }

    #[test]
    fn otlp_headers_are_validated_and_debug_values_are_redacted() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "Bearer secret-token".to_string(),
        );
        let mut config = OtlpExportConfig {
            endpoint: "https://collector.example:4317".to_string(),
            protocol: "grpc".to_string(),
            headers,
            timeout: default_timeout(),
            max_queue_items: default_export_queue_items(),
            max_queue_bytes: default_export_queue_bytes(),
            max_batch_size: default_export_batch_size(),
            flush_interval_millis: default_export_flush_interval_millis(),
            metrics_export_interval_millis: default_metrics_export_interval_millis(),
            max_export_retries: default_export_retries(),
            retry_backoff_millis: default_export_retry_backoff_millis(),
        };

        assert_eq!(build_otlp_metadata(&config.headers).unwrap().len(), 1);
        let debug = format!("{config:?}");
        assert!(debug.contains("authorization"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-token"));

        config.headers.insert(
            "authorization".to_string(),
            "Bearer secret-token\ninvalid".to_string(),
        );
        let error = build_otlp_metadata(&config.headers).unwrap_err();
        assert!(error.contains("authorization"));
        assert!(!error.contains("secret-token"));
    }

    #[test]
    fn serde_rejects_unknown_configuration_instead_of_downgrading() {
        let error = toml::from_str::<TraceConfig>(
            r#"
enabled = true
service_name = "api"
console_export = true
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn shipped_trace_profiles_parse_and_validate_strictly() {
        for (name, source) in [
            (
                "distributed/gateway",
                include_str!(
                    "../../tests/fixtures/examples/distributed_trace_system/config/gateway/lily_trace.toml"
                ),
            ),
            (
                "distributed/get-worker",
                include_str!(
                    "../../tests/fixtures/examples/distributed_trace_system/config/get-worker/lily_trace.toml"
                ),
            ),
            (
                "distributed/manage-worker",
                include_str!(
                    "../../tests/fixtures/examples/distributed_trace_system/config/manage-worker/lily_trace.toml"
                ),
            ),
            (
                "distributed/probe",
                include_str!(
                    "../../tests/fixtures/examples/distributed_trace_system/config/probe/lily_trace.toml"
                ),
            ),
            (
                "distributed/save-worker",
                include_str!(
                    "../../tests/fixtures/examples/distributed_trace_system/config/save-worker/lily_trace.toml"
                ),
            ),
            (
                "gateway_example_advanced",
                include_str!(
                    "../../tests/fixtures/examples/gateway_example_advanced/lily_trace.toml"
                ),
            ),
            (
                "get_worker_example_advanced",
                include_str!(
                    "../../tests/fixtures/examples/get_worker_example_advanced/lily_trace.toml"
                ),
            ),
            (
                "save_worker_example_advanced",
                include_str!(
                    "../../tests/fixtures/examples/save_worker_example_advanced/lily_trace.toml"
                ),
            ),
            (
                "test_tcp_concurrency",
                include_str!("../../tests/fixtures/examples/test_tcp_concurrency/lily_trace.toml"),
            ),
            (
                "trace_advanced",
                include_str!("../../tests/fixtures/examples/trace_advanced/lily_trace.toml"),
            ),
            (
                "trace_demo",
                include_str!("../../tests/fixtures/examples/trace_demo/lily_trace.toml"),
            ),
            (
                "trace_distributed",
                include_str!("../../tests/fixtures/examples/trace_distributed/lily_trace.toml"),
            ),
            (
                "trace_otlp",
                include_str!("../../tests/fixtures/examples/trace_otlp/lily_trace.toml"),
            ),
            (
                "trace_rotation",
                include_str!("../../tests/fixtures/examples/trace_rotation/lily_trace.toml"),
            ),
        ] {
            let config = toml::from_str::<TraceConfig>(source)
                .unwrap_or_else(|error| panic!("{name} failed to parse: {error}"));
            config
                .validate()
                .unwrap_or_else(|error| panic!("{name} failed validation: {error}"));
        }
    }

    #[test]
    fn strict_loader_reports_malformed_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lily_trace.toml");
        std::fs::write(&path, "enabled = definitely-not-a-bool").unwrap();

        assert!(matches!(
            TraceConfig::try_load_from_path(path),
            Err(TraceConfigLoadError::Parse { .. })
        ));
    }
}
