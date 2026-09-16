use super::correlation::{CloseContextLayer, ConsoleFormat};
use super::file_worker::{ErrorCounter, FileWorker, NonBlocking};
use super::json_format::{JsonFieldsLayer, JsonFormat, NoFields};
use super::tasks::{self, AsyncWorker, Receipt};
use crate::runtime::config::{build_otlp_metadata, FileExportConfig, FileRotation, TraceConfig};
use crate::runtime::otlp_log_layer::{
    LogExportHandle, LogExportMetricSnapshot, LogExportReport, LogExportTask, OtlpLogLayer,
};
use crate::runtime::otlp_span_processor::{
    BoundedSpanProcessor, ExportQueuePolicy, SpanExportHandle, SpanExportMetricSnapshot,
    SpanExportReport, SpanExportTask,
};
use futures_util::FutureExt;
use opentelemetry::Context as OtelContext;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SpanData, SpanProcessor};
use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

static TRACE_INIT_LOCK: Mutex<()> = Mutex::new(());
static TRACE_INITIALIZED: OnceLock<u64> = OnceLock::new();
static TRACE_OWNER_CLAIMED: AtomicBool = AtomicBool::new(false);
static TRACE_SHUTDOWN: AtomicBool = AtomicBool::new(false);
static LOG_EXPORT_HANDLE: OnceLock<LogExportHandle> = OnceLock::new();
static LOG_EXPORT_TASK: OnceLock<AsyncWorker<LogExportReport>> = OnceLock::new();
static SPAN_EXPORT_HANDLE: OnceLock<SpanExportHandle> = OnceLock::new();
static SPAN_EXPORT_TASK: OnceLock<AsyncWorker<SpanExportReport>> = OnceLock::new();
static TRACER_PROVIDER: OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> = OnceLock::new();
static METER_PROVIDER: OnceLock<opentelemetry_sdk::metrics::SdkMeterProvider> = OnceLock::new();
static METRIC_READER: OnceLock<super::metric_reader::OwnedMetricReader> = OnceLock::new();
static SPAN_PROCESSOR: OnceLock<ControlledSpanProcessor> = OnceLock::new();
static FILE_EXPORT_RUNTIME: OnceLock<Mutex<Option<FileExportRuntime>>> = OnceLock::new();
static TRACE_SHUTDOWN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static TRACE_SHUTDOWN_REPORT: OnceLock<TraceShutdownReport> = OnceLock::new();
static TRACE_SHUTDOWN_RECEIPT: OnceLock<Receipt<TraceShutdownReport>> = OnceLock::new();

struct FileExportRuntime {
    guard: FileWorker,
    counters: FileExportCounters,
}

struct PreparedFileExport {
    writer: BoundedFileMakeWriter,
    runtime: FileExportRuntime,
}

#[derive(Clone)]
struct FileExportCounters {
    queue_full: ErrorCounter,
    oversized: Arc<AtomicUsize>,
    write_failed: Arc<AtomicUsize>,
}

impl FileExportCounters {
    fn snapshot(&self) -> FileExportMetricSnapshot {
        FileExportMetricSnapshot {
            queue_full: self.queue_full.dropped_lines(),
            oversized: self.oversized.load(Ordering::Acquire),
            write_failed: self.write_failed.load(Ordering::Acquire),
        }
    }
}

#[derive(Clone)]
struct BoundedFileMakeWriter {
    writer: NonBlocking,
    max_record_bytes: usize,
    oversized: Arc<AtomicUsize>,
}

struct BoundedFileEventWriter {
    writer: NonBlocking,
    buffer: Vec<u8>,
    max_record_bytes: usize,
    oversized: Arc<AtomicUsize>,
    dropped: bool,
    emitted: bool,
}

impl BoundedFileEventWriter {
    fn emit(&mut self) -> io::Result<()> {
        if self.emitted || self.dropped {
            return Ok(());
        }
        self.emitted = true;
        self.writer.write_all(&self.buffer)
    }
}

impl Write for BoundedFileEventWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.dropped && self.buffer.len().saturating_add(buf.len()) <= self.max_record_bytes {
            self.buffer.extend_from_slice(buf);
        } else if !self.dropped {
            self.buffer.clear();
            self.dropped = true;
            increment_saturating(&self.oversized);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit()
    }
}

impl Drop for BoundedFileEventWriter {
    fn drop(&mut self) {
        let _ = self.emit();
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BoundedFileMakeWriter {
    type Writer = BoundedFileEventWriter;

    fn make_writer(&'a self) -> Self::Writer {
        BoundedFileEventWriter {
            writer: self.writer.clone(),
            buffer: Vec::with_capacity(self.max_record_bytes.min(1024)),
            max_record_bytes: self.max_record_bytes,
            oversized: Arc::clone(&self.oversized),
            dropped: false,
            emitted: false,
        }
    }
}

struct FailClosedRollingFile {
    inner: BasicRollingFileAppender,
    path: PathBuf,
    max_file_bytes: u64,
    write_failed: Arc<AtomicUsize>,
    poisoned: bool,
}

impl FailClosedRollingFile {
    fn reject(&mut self, error: io::Error) -> io::Result<usize> {
        self.poisoned = true;
        increment_saturating(&self.write_failed);
        Err(error)
    }
}

impl Write for FailClosedRollingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.poisoned {
            return self.reject(io::Error::other(
                "file exporter is fail-closed after a previous write or rotation failure",
            ));
        }

        let record_bytes = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        let current_bytes = match std::fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) => return self.reject(error),
        };
        if current_bytes.saturating_add(record_bytes) > self.max_file_bytes {
            if let Err(error) = self.inner.rollover() {
                return self.reject(error);
            }
        }

        let written = match self.inner.write(buf) {
            Ok(written) => written,
            Err(error) => return self.reject(error),
        };
        match std::fs::metadata(&self.path) {
            Ok(metadata) if metadata.len() <= self.max_file_bytes => Ok(written),
            Ok(_) => self.reject(io::Error::other(
                "file exporter exceeded its hard active-file byte bound",
            )),
            Err(error) => self.reject(error),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "file exporter is fail-closed after a previous write or rotation failure",
            ));
        }
        self.inner.flush()
    }
}

fn increment_saturating(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        (value != usize::MAX).then(|| value + 1)
    });
}

/// A provider-owned processor proxy whose actual batch processor can also be
/// shut down by the application lifecycle. OpenTelemetry shuts processors down
/// only when the final provider clone is dropped; a process-global
/// tracing subscriber keeps such a clone alive forever. Taking the processor
/// through this shared proxy stops and joins its worker while leaving the
/// immutable global subscriber in a harmless drop-only state.
#[derive(Clone)]
struct ControlledSpanProcessor {
    inner: std::sync::Arc<Mutex<Option<Box<dyn SpanProcessor>>>>,
}

impl std::fmt::Debug for ControlledSpanProcessor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlledSpanProcessor")
            .field("running", &self.is_running())
            .finish()
    }
}

impl ControlledSpanProcessor {
    fn new(processor: impl SpanProcessor + 'static) -> Self {
        Self {
            inner: std::sync::Arc::new(Mutex::new(Some(Box::new(processor)))),
        }
    }

    fn is_running(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }

    fn shutdown_owned(&self) -> OTelSdkResult {
        let processor = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match processor {
            Some(processor) => processor.shutdown(),
            None => Ok(()),
        }
    }
}

impl SpanProcessor for ControlledSpanProcessor {
    fn on_start(&self, span: &mut opentelemetry_sdk::trace::Span, context: &OtelContext) {
        if let Some(processor) = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            processor.on_start(span, context);
        }
    }

    fn on_end(&self, span: SpanData) {
        if let Some(processor) = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            processor.on_end(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map_or(Ok(()), |processor| processor.force_flush())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shutdown_owned()
    }
}

fn config_fingerprint(config: &TraceConfig) -> Result<u64, serde_json::Error> {
    let encoded = serde_json::to_vec(config)?;
    let mut hasher = DefaultHasher::new();
    encoded.hash(&mut hasher);
    Ok(hasher.finish())
}

fn mark_initialized(config: &TraceConfig) -> Result<(), Box<dyn std::error::Error>> {
    TRACE_INITIALIZED
        .set(config_fingerprint(config)?)
        .map_err(|_| std::io::Error::other("tracing runtime initialization raced"))?;
    Ok(())
}

fn ensure_runtime_slots_available() -> Result<(), Box<dyn std::error::Error>> {
    if TRACER_PROVIDER.get().is_some()
        || METER_PROVIDER.get().is_some()
        || SPAN_PROCESSOR.get().is_some()
        || LOG_EXPORT_HANDLE.get().is_some()
        || LOG_EXPORT_TASK.get().is_some()
        || SPAN_EXPORT_HANDLE.get().is_some()
        || SPAN_EXPORT_TASK.get().is_some()
        || FILE_EXPORT_RUNTIME.get().is_some()
    {
        return Err(
            std::io::Error::other("tracing runtime ownership was partially initialized").into(),
        );
    }
    Ok(())
}

fn publish_tracer_provider(
    provider: opentelemetry_sdk::trace::SdkTracerProvider,
    processor: Option<ControlledSpanProcessor>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(processor) = processor {
        SPAN_PROCESSOR.set(processor).map_err(|_| {
            std::io::Error::other("span processor ownership was already initialized")
        })?;
    }
    TRACER_PROVIDER
        .set(provider.clone())
        .map_err(|_| std::io::Error::other("tracer provider ownership was already initialized"))?;
    let _global_tracer = opentelemetry::global::set_tracer_provider(provider);
    Ok(())
}

/// Failure to acquire the single process-global tracing runtime owner.
#[derive(Debug, thiserror::Error)]
pub enum TraceInstallError {
    /// Another owner already installed or claimed the process-global runtime.
    #[error(
        "tracing is already initialized; application adapters must use TracingMode::External when a process composition root owns telemetry"
    )]
    AlreadyInitialized,
    /// The bounded JSONL exporter could not be constructed.
    #[error(transparent)]
    FileExporter(#[from] FileExportInitError),
    /// Configuration validation or exporter/provider installation failed.
    #[error("failed to initialize tracing: {0}")]
    Initialization(String),
}

/// File exporter startup failure. The path is retained for diagnostics while
/// writer construction remains fallible and never panics.
#[derive(Debug, thiserror::Error)]
#[error("failed to initialize JSONL file exporter at {path}: {source}")]
pub struct FileExportInitError {
    path: PathBuf,
    #[source]
    source: std::io::Error,
}

impl FileExportInitError {
    /// Returns the configured JSONL path whose writer could not be created.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

struct TraceOwnerClaimRollback {
    committed: bool,
}

impl TraceOwnerClaimRollback {
    fn acquire() -> Result<Self, TraceInstallError> {
        TRACE_OWNER_CLAIMED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| TraceInstallError::AlreadyInitialized)?;
        Ok(Self { committed: false })
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for TraceOwnerClaimRollback {
    fn drop(&mut self) {
        if !self.committed {
            TRACE_OWNER_CLAIMED.store(false, Ordering::Release);
        }
    }
}

/// Result of installing an explicitly owned tracing configuration.
#[must_use = "an enabled tracing runtime owner must be retained and shut down"]
#[derive(Debug)]
pub enum TraceInstallOutcome {
    /// The validated configuration had tracing disabled; no owner is required.
    Disabled,
    /// The runtime was installed and this token exclusively owns shutdown.
    Owned(TracingRuntimeOwner),
}

/// Exclusive owner of Lily's process-global tracing providers and exporter
/// tasks. It is intentionally non-Clone: sharing is expressed by configuring
/// application adapters as `TracingMode::External` and retaining this token in
/// the outer process composition root.
#[must_use = "the tracing runtime owner must be shut down and its report checked"]
pub struct TracingRuntimeOwner {
    shutdown_started: AtomicBool,
}

impl std::fmt::Debug for TracingRuntimeOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TracingRuntimeOwner")
            .field(
                "shutdown_started",
                &self.shutdown_started.load(Ordering::Acquire),
            )
            .finish()
    }
}

impl TracingRuntimeOwner {
    /// Validate and install one enabled process-global tracing runtime.
    /// Disabled configuration is a validated no-op and returns no owner.
    pub fn install(config: &TraceConfig) -> Result<TraceInstallOutcome, TraceInstallError> {
        let _init_guard = TRACE_INIT_LOCK.lock().map_err(|_| {
            TraceInstallError::Initialization("initialization lock poisoned".to_string())
        })?;

        config
            .validate()
            .map_err(TraceInstallError::Initialization)?;
        if !config.is_enabled() {
            crate::core::install_w3c_propagator();
            return Ok(TraceInstallOutcome::Disabled);
        }
        if TRACE_INITIALIZED.get().is_some() {
            return Err(TraceInstallError::AlreadyInitialized);
        }

        let claim = TraceOwnerClaimRollback::acquire()?;
        init_tracing_locked(config).map_err(|error| {
            match error.downcast::<FileExportInitError>() {
                Ok(error) => TraceInstallError::FileExporter(*error),
                Err(error) => TraceInstallError::Initialization(error.to_string()),
            }
        })?;
        claim.commit();

        Ok(TraceInstallOutcome::Owned(Self {
            shutdown_started: AtomicBool::new(false),
        }))
    }

    /// Flush and stop every exporter/provider within one deadline. Consuming
    /// the owner prevents a second application adapter from issuing shutdown.
    pub async fn shutdown(self, timeout_budget: Duration) -> TraceShutdownReport {
        let started = tokio::time::Instant::now();
        self.shutdown_before(started.checked_add(timeout_budget).unwrap_or(started))
            .await
    }

    /// Composition-root seam: scheduling delay consumes this existing deadline.
    #[doc(hidden)]
    pub async fn shutdown_before(self, deadline: tokio::time::Instant) -> TraceShutdownReport {
        self.shutdown_started.store(true, Ordering::Release);
        match tracing_shutdown_receipt(deadline).await {
            Ok(report) => report,
            Err(error) => {
                let mut report = TraceShutdownReport::uninitialized();
                report.was_initialized = TRACE_INITIALIZED.get().is_some();
                report.tracer_flush_errors.push(error);
                report
            }
        }
    }
}

impl Drop for TracingRuntimeOwner {
    fn drop(&mut self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }

        // The fallback retains the same actual owner join in process state.
        // It still is not evidence that its caller awaited cleanup.
        eprintln!(
            "tracing runtime owner dropped without awaited shutdown; scheduling best-effort flush"
        );
        if tokio::runtime::Handle::try_current().is_ok() {
            drop(tracing_shutdown_receipt(
                tokio::time::Instant::now() + Duration::from_secs(10),
            ));
        }
    }
}

fn prepare_file_export(
    config: &FileExportConfig,
) -> Result<PreparedFileExport, FileExportInitError> {
    let condition = match config.rotation {
        FileRotation::Hourly => RollingConditionBasic::new().hourly(),
        FileRotation::Daily => RollingConditionBasic::new().daily(),
    };

    // `rolling-file` counts historical files separately from the active file;
    // Lily's public `max_files` is the total on-disk count.
    let historical_files = config.max_files.saturating_sub(1);
    let appender = BasicRollingFileAppender::new_with_buffer_capacity(
        &config.path,
        condition,
        historical_files,
        0,
    )
    .map_err(|source| FileExportInitError {
        path: PathBuf::from(&config.path),
        source,
    })?;
    let oversized = Arc::new(AtomicUsize::new(0));
    let write_failed = Arc::new(AtomicUsize::new(0));
    let bounded_file = FailClosedRollingFile {
        inner: appender,
        path: PathBuf::from(&config.path),
        max_file_bytes: config.max_file_bytes,
        write_failed: Arc::clone(&write_failed),
        poisoned: false,
    };
    let (non_blocking, guard) =
        FileWorker::start(bounded_file, config.buffered_lines).map_err(|source| {
            FileExportInitError {
                path: PathBuf::from(&config.path),
                source,
            }
        })?;
    let queue_full = non_blocking.error_counter();
    let writer = BoundedFileMakeWriter {
        writer: non_blocking,
        max_record_bytes: config.max_record_bytes,
        oversized: Arc::clone(&oversized),
    };

    Ok(PreparedFileExport {
        writer,
        runtime: FileExportRuntime {
            guard,
            counters: FileExportCounters {
                queue_full,
                oversized,
                write_failed,
            },
        },
    })
}

fn init_tracing_locked(config: &TraceConfig) -> Result<(), Box<dyn std::error::Error>> {
    crate::core::install_w3c_propagator();

    // Validate config
    config.validate()?;

    if !config.is_enabled() {
        // Disabled configuration is a no-op, not a process-global decision.
        // A later application-owned enabled configuration may still install
        // the subscriber.
        return Ok(());
    }

    if TRACE_SHUTDOWN.load(Ordering::Acquire) {
        return Err(std::io::Error::other(
            "tracing was shut down and the process-global subscriber cannot be reinitialized",
        )
        .into());
    }

    let fingerprint = config_fingerprint(config)?;
    if let Some(initialized) = TRACE_INITIALIZED.get() {
        if initialized == &fingerprint {
            return Ok(());
        }
        return Err(std::io::Error::other(
            "tracing is already initialized with a different configuration",
        )
        .into());
    }
    ensure_runtime_slots_available()?;

    eprintln!("🚀 Initializing tracing system...");
    eprintln!("   Service: {}", config.service_name);
    eprintln!("   Level: {}", config.level);
    eprintln!(
        "   Sampling: {:?} (rate: {})",
        config.sampling.strategy, config.sampling.rate
    );

    let export_targets = config.export_targets();
    if !export_targets.is_empty() {
        eprintln!("   Exports: {}", export_targets.join(", "));
    }

    // Build env filter with dynamic rules
    // TraceConfig is authoritative. A process-level RUST_LOG such as `warn`
    // must not silently disable the info-level SERVER/CLIENT/PRODUCER/CONSUMER
    // spans required for distributed context propagation.
    let mut env_filter = EnvFilter::new(config.level.clone());

    // Apply dynamic filter rules
    for rule in &config.filters {
        let directive = format!("{}={}", rule.target, rule.level);
        env_filter = env_filter.add_directive(directive.parse()?);
    }

    // Build subscriber with conditional layers
    let registry = Registry::default().with(env_filter);

    // The validated profiles are mutually exclusive except that OTLP may also
    // opt into the development console layer.
    if let Some(ref file_config) = config.export.file {
        eprintln!(
            "   📁 File export: {} ({:?}, {} bytes/file, {} files, {} buffered lines, {} bytes/record)",
            file_config.path,
            file_config.rotation,
            file_config.max_file_bytes,
            file_config.max_files,
            file_config.buffered_lines,
            file_config.max_record_bytes,
        );
        let prepared = prepare_file_export(file_config)?;
        let (tracer, provider) = init_local_tracer(config);
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(prepared.writer)
            .with_ansi(false)
            .fmt_fields(NoFields)
            .event_format(JsonFormat(tracer.clone()));

        // Even without an OTLP exporter we install an SDK tracer layer so
        // spans have valid W3C trace/span IDs for downstream propagation.
        let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = registry
            .with(JsonFieldsLayer)
            .with(otel_trace_layer)
            .with(file_layer);
        tracing::subscriber::set_global_default(subscriber)?;
        publish_tracer_provider(provider, None)?;
        FILE_EXPORT_RUNTIME
            .set(Mutex::new(Some(prepared.runtime)))
            .map_err(|_| {
                std::io::Error::other("file exporter ownership was already initialized")
            })?;
        mark_initialized(config)?;
        return Ok(());
    }

    // OpenTelemetry OTLP layer. Every requested signal (traces, metrics, and
    // logs) must initialize successfully; partial silent downgrade is unsafe
    // because operators would believe telemetry exists when it does not.
    if let Some(ref otlp_config) = config.export.otlp {
        eprintln!(
            "   🌐 OTLP endpoint: {} (protocol: {})",
            otlp_config.endpoint, otlp_config.protocol
        );

        tokio::runtime::Handle::try_current().map_err(|_| {
            std::io::Error::other("OTLP tracing initialization requires a running Tokio runtime")
        })?;

        let PreparedOtlpTracer {
            tracer,
            provider: tracer_provider,
            processor: span_processor,
            task: span_task,
            handle: span_handle,
        } = init_otlp_tracer(config, otlp_config)?;
        let (meter_provider, metric_reader) = init_otlp_metrics(config, otlp_config)?;
        let (log_layer, log_task, log_handle) =
            init_otlp_log_layer(config, otlp_config, tracer.clone())?;
        let registry = registry.with(
            config
                .export
                .console
                .then(|| CloseContextLayer(tracer.clone())),
        );
        let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer.clone());

        if config.export.console {
            let console_layer = tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true)
                .map_event_format(|inner| ConsoleFormat {
                    inner,
                    tracer: tracer.clone(),
                });
            tracing::subscriber::set_global_default(
                registry
                    .with(otel_trace_layer)
                    .with(log_layer)
                    .with(console_layer),
            )?;
        } else {
            tracing::subscriber::set_global_default(
                registry.with(otel_trace_layer).with(log_layer),
            )?;
        }

        publish_tracer_provider(tracer_provider, Some(span_processor))?;
        METER_PROVIDER
            .set(meter_provider.clone())
            .map_err(|_| std::io::Error::other("meter provider was already initialized"))?;
        METRIC_READER
            .set(metric_reader)
            .map_err(|_| io::Error::other("metrics reader already initialized"))?;
        opentelemetry::global::set_meter_provider(meter_provider);
        LOG_EXPORT_HANDLE
            .set(log_handle)
            .map_err(|_| std::io::Error::other("log exporter handle was already initialized"))?;
        LOG_EXPORT_TASK
            .set(AsyncWorker::new("log", tokio::spawn(log_task.run())))
            .map_err(|_| std::io::Error::other("log exporter task was already initialized"))?;
        SPAN_EXPORT_HANDLE
            .set(span_handle)
            .map_err(|_| std::io::Error::other("span exporter handle was already initialized"))?;
        SPAN_EXPORT_TASK
            .set(AsyncWorker::new("span", tokio::spawn(span_task.run())))
            .map_err(|_| std::io::Error::other("span exporter task was already initialized"))?;
        mark_initialized(config)?;
        return Ok(());
    }

    // Console layer (for development) - fallback if no OTLP
    if config.export.console {
        eprintln!("   📺 Console export: enabled");
        let (tracer, provider) = init_local_tracer(config);
        let console_layer = tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .with_line_number(true)
            .with_file(true)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .map_event_format(|inner| {
                super::method_format::MethodFormat(ConsoleFormat {
                    inner,
                    tracer: tracer.clone(),
                })
            });

        let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer.clone());
        let subscriber = registry
            .with(CloseContextLayer(tracer))
            .with(otel_trace_layer)
            .with(console_layer);
        tracing::subscriber::set_global_default(subscriber)?;
        publish_tracer_provider(provider, None)?;
        mark_initialized(config)?;
        return Ok(());
    }

    // Default: just registry with filter
    let (tracer, provider) = init_local_tracer(config);
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing::subscriber::set_global_default(registry.with(otel_trace_layer))?;
    publish_tracer_provider(provider, None)?;

    eprintln!("✅ Tracing system initialized successfully");

    mark_initialized(config)?;
    Ok(())
}

struct PreparedOtlpTracer {
    tracer: opentelemetry_sdk::trace::Tracer,
    provider: opentelemetry_sdk::trace::SdkTracerProvider,
    processor: ControlledSpanProcessor,
    task: SpanExportTask,
    handle: SpanExportHandle,
}

fn init_otlp_tracer(
    config: &TraceConfig,
    otlp_config: &crate::runtime::config::OtlpExportConfig,
) -> Result<PreparedOtlpTracer, Box<dyn std::error::Error>> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};

    let root_sampler = match config.sampling.strategy {
        crate::runtime::config::SamplingStrategy::Always => Sampler::AlwaysOn,
        crate::runtime::config::SamplingStrategy::Never => Sampler::AlwaysOff,
        crate::runtime::config::SamplingStrategy::Probability => {
            Sampler::TraceIdRatioBased(config.sampling.rate)
        }
    };
    let sampler = Sampler::ParentBased(Box::new(root_sampler));

    let exporter = otlp_span_exporter(otlp_config)?;
    let resource = telemetry_resource(config);
    let (processor, task, handle) =
        BoundedSpanProcessor::new(exporter, &resource, export_queue_policy(otlp_config));
    let controlled_processor = ControlledSpanProcessor::new(processor);
    let provider = SdkTracerProvider::builder()
        .with_span_processor(controlled_processor.clone())
        .with_sampler(sampler)
        .with_resource(resource)
        .with_max_attributes_per_span(config.max_attributes as u32)
        .with_max_attributes_per_event(config.max_event_attributes as u32)
        .with_max_events_per_span(if config.enable_events {
            config.max_events_per_span as u32
        } else {
            0
        })
        .build();
    let tracer = provider.tracer("lily_trace");

    Ok(PreparedOtlpTracer {
        tracer,
        provider,
        processor: controlled_processor,
        task,
        handle,
    })
}

/// Creates an SDK tracer without an exporter. This keeps propagation and
/// parent/child semantics functional for console/file-only deployments.
fn init_local_tracer(
    config: &TraceConfig,
) -> (
    opentelemetry_sdk::trace::Tracer,
    opentelemetry_sdk::trace::SdkTracerProvider,
) {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};

    let root_sampler = match config.sampling.strategy {
        crate::runtime::config::SamplingStrategy::Always => Sampler::AlwaysOn,
        crate::runtime::config::SamplingStrategy::Never => Sampler::AlwaysOff,
        crate::runtime::config::SamplingStrategy::Probability => {
            Sampler::TraceIdRatioBased(config.sampling.rate)
        }
    };

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(root_sampler)))
        .with_resource(telemetry_resource(config))
        .with_max_attributes_per_span(config.max_attributes as u32)
        .with_max_attributes_per_event(config.max_event_attributes as u32)
        .with_max_events_per_span(if config.enable_events {
            config.max_events_per_span as u32
        } else {
            0
        })
        .build();
    let tracer = provider.tracer("lily_trace");
    (tracer, provider)
}

fn telemetry_resource(config: &TraceConfig) -> opentelemetry_sdk::Resource {
    use opentelemetry::KeyValue;

    let mut attributes = vec![KeyValue::new("service.name", config.service_name.clone())];
    if let Some(version) = &config.service_version {
        attributes.push(KeyValue::new("service.version", version.clone()));
    }
    if let Some(environment) = &config.environment {
        attributes.push(KeyValue::new("deployment.environment", environment.clone()));
    }
    attributes.extend(
        config
            .global_attributes
            .iter()
            .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
    );
    opentelemetry_sdk::Resource::builder_empty()
        .with_attributes(attributes)
        .build()
}

fn init_otlp_metrics(
    config: &TraceConfig,
    otlp_config: &crate::runtime::config::OtlpExportConfig,
) -> Result<
    (
        opentelemetry_sdk::metrics::SdkMeterProvider,
        super::metric_reader::OwnedMetricReader,
    ),
    Box<dyn std::error::Error>,
> {
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use std::time::Duration;

    // Create OTLP metrics exporter with proper aggregation and temporality selectors
    let exporter = otlp_metric_exporter(otlp_config)?;

    // The application config is the authority for metric export latency and
    // collector load; no hidden dependency default is used.
    let reader = super::metric_reader::OwnedMetricReader::new(
        exporter,
        Duration::from_millis(otlp_config.metrics_export_interval_millis),
    )?;

    // Build meter provider with reader and resource
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader.clone())
        .with_resource(telemetry_resource(config))
        .build();
    Ok((meter_provider, reader))
}

fn init_otlp_log_layer(
    config: &TraceConfig,
    otlp_config: &crate::runtime::config::OtlpExportConfig,
    tracer: opentelemetry_sdk::trace::Tracer,
) -> Result<(OtlpLogLayer, LogExportTask, LogExportHandle), Box<dyn std::error::Error>> {
    // Create OTLP log exporter
    let exporter = otlp_log_exporter(otlp_config)?;

    // Create custom log layer and export task
    let (layer, task, handle) = OtlpLogLayer::with_limits(
        exporter,
        telemetry_resource(config),
        tracer,
        export_queue_policy(otlp_config),
    );

    Ok((layer, task, handle))
}

fn export_queue_policy(config: &crate::runtime::config::OtlpExportConfig) -> ExportQueuePolicy {
    ExportQueuePolicy {
        max_queue_items: config.max_queue_items,
        max_queue_bytes: config.max_queue_bytes,
        max_batch_size: config.max_batch_size,
        flush_interval: Duration::from_millis(config.flush_interval_millis),
        max_export_retries: config.max_export_retries,
        retry_backoff: Duration::from_millis(config.retry_backoff_millis),
    }
}

fn configure_otlp_exporter<B>(
    exporter: B,
    config: &crate::runtime::config::OtlpExportConfig,
) -> Result<B, Box<dyn std::error::Error>>
where
    B: opentelemetry_otlp::WithExportConfig + opentelemetry_otlp::WithTonicConfig,
{
    let metadata = build_otlp_metadata(&config.headers).map_err(std::io::Error::other)?;
    let exporter = exporter
        .with_endpoint(config.endpoint.clone())
        .with_timeout(Duration::from_secs(config.timeout))
        .with_metadata(metadata);
    Ok(if endpoint_requires_tls(&config.endpoint) {
        exporter.with_tls_config(tonic::transport::ClientTlsConfig::new())
    } else {
        exporter
    })
}

fn otlp_span_exporter(
    config: &crate::runtime::config::OtlpExportConfig,
) -> Result<opentelemetry_otlp::SpanExporter, Box<dyn std::error::Error>> {
    configure_otlp_exporter(
        opentelemetry_otlp::SpanExporter::builder().with_tonic(),
        config,
    )?
    .build()
    .map_err(Into::into)
}

fn otlp_log_exporter(
    config: &crate::runtime::config::OtlpExportConfig,
) -> Result<opentelemetry_otlp::LogExporter, Box<dyn std::error::Error>> {
    configure_otlp_exporter(
        opentelemetry_otlp::LogExporter::builder().with_tonic(),
        config,
    )?
    .build()
    .map_err(Into::into)
}

fn otlp_metric_exporter(
    config: &crate::runtime::config::OtlpExportConfig,
) -> Result<opentelemetry_otlp::MetricExporter, Box<dyn std::error::Error>> {
    configure_otlp_exporter(
        opentelemetry_otlp::MetricExporter::builder().with_tonic(),
        config,
    )?
    .build()
    .map_err(Into::into)
}

fn endpoint_requires_tls(endpoint: &str) -> bool {
    endpoint.starts_with("https://")
}

/// Observable lifecycle state of the process-global tracing runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TracingRuntimeStatus {
    /// No enabled runtime has been installed in this process.
    Uninitialized,
    /// A runtime is installed and has not begun terminal shutdown.
    Initialized,
    /// The installed runtime completed or attempted terminal shutdown.
    Shutdown,
}

/// Returns the process-global tracing runtime's observable lifecycle state.
pub fn tracing_runtime_status() -> TracingRuntimeStatus {
    if TRACE_SHUTDOWN.load(Ordering::Acquire) {
        TracingRuntimeStatus::Shutdown
    } else if TRACE_INITIALIZED.get().is_some() {
        TracingRuntimeStatus::Initialized
    } else {
        TracingRuntimeStatus::Uninitialized
    }
}

/// Terminal status of the owned OTLP log-export task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportTaskShutdownStatus {
    /// OTLP log export was not configured.
    NotConfigured,
    /// The task drained and returned its terminal report.
    Completed(LogExportReport),
    /// The task did not stop within the shared shutdown deadline.
    TimedOut,
    /// Joining the task failed.
    JoinFailed(String),
}

/// Terminal status of the owned OTLP span-export task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpanExportTaskShutdownStatus {
    /// OTLP span export was not configured.
    NotConfigured,
    /// The task drained and returned its terminal report.
    Completed(SpanExportReport),
    /// The task did not stop within the shared shutdown deadline.
    TimedOut,
    /// Joining the task failed.
    JoinFailed(String),
}

/// Terminal loss counters for the bounded JSONL exporter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileExportMetricSnapshot {
    /// Records rejected because the writer queue was full.
    pub queue_full: usize,
    /// Records rejected because their serialized size exceeded the configured bound.
    pub oversized: usize,
    /// Records lost after the file writer entered a failed state.
    pub write_failed: usize,
}

impl FileExportMetricSnapshot {
    /// Returns the saturating sum of all dropped-record categories.
    pub fn total_dropped(&self) -> usize {
        self.queue_full
            .saturating_add(self.oversized)
            .saturating_add(self.write_failed)
    }
}

/// Terminal status of the bounded JSONL exporter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileExportShutdownStatus {
    /// File export was not configured.
    NotConfigured,
    /// The writer stopped within the deadline.
    Completed(FileExportMetricSnapshot),
    /// The writer did not stop within the deadline.
    TimedOut(FileExportMetricSnapshot),
    /// Joining the writer failed while retaining the last observed metrics.
    JoinFailed {
        /// Last observed loss counters.
        metrics: FileExportMetricSnapshot,
        /// Bounded diagnostic from the failed join.
        error: String,
    },
}

async fn shutdown_file_export(deadline: tokio::time::Instant) -> FileExportShutdownStatus {
    let runtime = FILE_EXPORT_RUNTIME.get().and_then(|runtime| {
        runtime
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    });
    let Some(runtime) = runtime else {
        return FileExportShutdownStatus::NotConfigured;
    };

    let metrics = runtime.counters.snapshot();
    runtime.guard.close();
    match tasks::observe_before(deadline, runtime.guard.receipt.clone()).await {
        Some(Ok(())) => FileExportShutdownStatus::Completed(runtime.counters.snapshot()),
        Some(Err(error)) => FileExportShutdownStatus::JoinFailed { metrics, error },
        None => FileExportShutdownStatus::TimedOut(metrics),
    }
}

fn store_shutdown_report(report: TraceShutdownReport) -> TraceShutdownReport {
    let terminal = report.clone();
    match TRACE_SHUTDOWN_REPORT.set(report) {
        Ok(()) => terminal,
        Err(report) => TRACE_SHUTDOWN_REPORT.get().cloned().unwrap_or(report),
    }
}

/// Aggregate shutdown-attempt evidence for tracing exporters and providers.
/// A timeout is not proof that its retained worker has joined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceShutdownReport {
    /// Whether an enabled tracing runtime had been installed.
    pub was_initialized: bool,
    /// Whether shutdown had already been requested before this report.
    pub already_shutdown: bool,
    /// Terminal OTLP log-task status.
    pub log_export: ExportTaskShutdownStatus,
    /// Last observed OTLP log counters, when configured.
    pub log_metrics: Option<LogExportMetricSnapshot>,
    /// Terminal OTLP span-task status.
    pub span_export: SpanExportTaskShutdownStatus,
    /// Last observed OTLP span counters, when configured.
    pub span_metrics: Option<SpanExportMetricSnapshot>,
    /// Terminal bounded JSONL writer status.
    pub file_export: FileExportShutdownStatus,
    /// Errors returned while flushing or shutting down tracer providers.
    pub tracer_flush_errors: Vec<String>,
    /// Error returned while force-flushing the meter provider.
    pub meter_flush_error: Option<String>,
    /// Error returned while shutting down the meter provider.
    pub meter_shutdown_error: Option<String>,
    /// Whether provider work or its worker join missed the shared deadline.
    pub provider_timed_out: bool,
}

impl TraceShutdownReport {
    fn uninitialized() -> Self {
        Self {
            was_initialized: false,
            already_shutdown: false,
            log_export: ExportTaskShutdownStatus::NotConfigured,
            log_metrics: None,
            span_export: SpanExportTaskShutdownStatus::NotConfigured,
            span_metrics: None,
            file_export: FileExportShutdownStatus::NotConfigured,
            tracer_flush_errors: Vec::new(),
            meter_flush_error: None,
            meter_shutdown_error: None,
            provider_timed_out: false,
        }
    }

    /// Returns `true` when all configured workers stopped and admitted records
    /// have a terminal exported/dropped/in-flight accounting state.
    pub fn is_success(&self) -> bool {
        !self.provider_timed_out
            && self.tracer_flush_errors.is_empty()
            && self.meter_flush_error.is_none()
            && self.meter_shutdown_error.is_none()
            && matches!(
                self.log_export,
                ExportTaskShutdownStatus::NotConfigured | ExportTaskShutdownStatus::Completed(_)
            )
            && matches!(
                self.span_export,
                SpanExportTaskShutdownStatus::NotConfigured
                    | SpanExportTaskShutdownStatus::Completed(_)
            )
            && matches!(
                self.file_export,
                FileExportShutdownStatus::NotConfigured | FileExportShutdownStatus::Completed(_)
            )
            && self.log_metrics.is_none_or(|metrics| {
                metrics.accepted
                    == metrics
                        .exported
                        .saturating_add(metrics.dropped)
                        .saturating_add(metrics.in_flight)
            })
            && self.span_metrics.is_none_or(|metrics| {
                metrics.accepted
                    == metrics
                        .exported
                        .saturating_add(metrics.dropped)
                        .saturating_add(metrics.in_flight)
            })
    }

    /// Returns file-export loss counters from any configured terminal state.
    pub fn file_metrics(&self) -> Option<&FileExportMetricSnapshot> {
        match &self.file_export {
            FileExportShutdownStatus::Completed(metrics)
            | FileExportShutdownStatus::TimedOut(metrics)
            | FileExportShutdownStatus::JoinFailed { metrics, .. } => Some(metrics),
            FileExportShutdownStatus::NotConfigured => None,
        }
    }
}

/// Requests exporter shutdown, awaits the owned log task, and bounds provider
/// flush work by a single deadline.
fn tracing_shutdown_receipt(deadline: tokio::time::Instant) -> Receipt<TraceShutdownReport> {
    TRACE_SHUTDOWN_RECEIPT
        .get_or_init(|| {
            let task = crate::spawn(shutdown_tracing_before(deadline));
            async move { task.await.map_err(|e| e.to_string()) }
                .boxed()
                .shared()
        })
        .clone()
}

pub(crate) async fn reconcile_shutdown_before(deadline: tokio::time::Instant) -> bool {
    let owner = match TRACE_SHUTDOWN_RECEIPT.get() {
        Some(receipt) => tasks::observe_before(deadline, receipt.clone())
            .await
            .is_some(),
        None => true,
    };
    let workers = tasks::reconcile_before(deadline).await;
    owner && workers.is_terminal()
}

async fn shutdown_tracing_before(deadline: tokio::time::Instant) -> TraceShutdownReport {
    if TRACE_INITIALIZED.get().is_none() {
        return TraceShutdownReport::uninitialized();
    }

    // Serialize concurrent callers and replay the first terminal report. This
    // avoids a second caller observing a false success while the first caller
    // is still flushing or after it timed out.
    let _shutdown_guard = TRACE_SHUTDOWN_LOCK.lock().await;
    if let Some(report) = TRACE_SHUTDOWN_REPORT.get() {
        let mut report = report.clone();
        report.already_shutdown = true;
        return report;
    }

    let already_shutdown = TRACE_SHUTDOWN.swap(true, Ordering::AcqRel);
    if let Some(reader) = METRIC_READER.get() {
        reader.cap_deadline(deadline);
    }
    let mut report = TraceShutdownReport {
        was_initialized: true,
        already_shutdown,
        log_export: ExportTaskShutdownStatus::NotConfigured,
        log_metrics: LOG_EXPORT_HANDLE.get().map(LogExportHandle::metrics),
        span_export: SpanExportTaskShutdownStatus::NotConfigured,
        span_metrics: SPAN_EXPORT_HANDLE.get().map(SpanExportHandle::metrics),
        file_export: FileExportShutdownStatus::NotConfigured,
        tracer_flush_errors: Vec::new(),
        meter_flush_error: None,
        meter_shutdown_error: None,
        provider_timed_out: false,
    };
    if let Some(handle) = LOG_EXPORT_HANDLE.get() {
        handle.shutdown();
    }
    if let Some(handle) = SPAN_EXPORT_HANDLE.get() {
        handle.shutdown();
    }

    if let Some(task) = LOG_EXPORT_TASK.get() {
        report.log_export = match tasks::observe_before(deadline, task.receipt.clone()).await {
            Some(Ok(export_report)) => ExportTaskShutdownStatus::Completed(export_report),
            Some(Err(error)) => ExportTaskShutdownStatus::JoinFailed(error),
            None => {
                task.abort.abort();
                // Intent only. The inventory retains the actual join; final
                // reconciliation may confirm it within the root reserve.
                ExportTaskShutdownStatus::TimedOut
            }
        };
    }
    report.log_metrics = LOG_EXPORT_HANDLE.get().map(LogExportHandle::metrics);

    if let Some(task) = SPAN_EXPORT_TASK.get() {
        report.span_export = match tasks::observe_before(deadline, task.receipt.clone()).await {
            Some(Ok(export_report)) => SpanExportTaskShutdownStatus::Completed(export_report),
            Some(Err(error)) => SpanExportTaskShutdownStatus::JoinFailed(error),
            None => {
                task.abort.abort();
                SpanExportTaskShutdownStatus::TimedOut
            }
        };
    }
    report.span_metrics = SPAN_EXPORT_HANDLE.get().map(SpanExportHandle::metrics);

    let span_processor = SPAN_PROCESSOR.get().cloned();
    let tracer = TRACER_PROVIDER.get().cloned();
    let meter = METER_PROVIDER.get().cloned();
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let exporters_terminal = LOG_EXPORT_TASK
        .get()
        .is_none_or(|task| task.receipt.clone().now_or_never().is_some())
        && SPAN_EXPORT_TASK
            .get()
            .is_none_or(|task| task.receipt.clone().now_or_never().is_some());
    if remaining.is_zero() || !exporters_terminal {
        report.provider_timed_out = tracer.is_some() || meter.is_some();
        if let Some(reader) = METRIC_READER.get() {
            reader.request_stop();
        }
    } else {
        let provider_shutdown = tokio::task::spawn_blocking(move || {
            let mut tracer_errors = span_processor
                .and_then(|processor| {
                    processor
                        .shutdown_owned()
                        .err()
                        .map(|error| format!("span processor shutdown failed: {error}"))
                })
                .into_iter()
                .collect::<Vec<_>>();
            tracer_errors.extend(tracer.map_or_else(Vec::new, |provider| {
                let mut errors = Vec::new();
                if let Err(error) = provider.force_flush() {
                    errors.push(error.to_string());
                }
                if let Err(error) = provider.shutdown() {
                    errors.push(error.to_string());
                }
                errors
            }));
            let (meter_flush_error, meter_shutdown_error) =
                meter.map_or((None, None), |provider| {
                    (
                        provider.force_flush().err().map(|error| error.to_string()),
                        provider.shutdown().err().map(|error| error.to_string()),
                    )
                });
            (tracer_errors, meter_flush_error, meter_shutdown_error)
        });

        let receipt = tasks::adopt("providers", provider_shutdown);
        match tasks::observe_before(deadline, receipt).await {
            Some(Ok((tracer_errors, meter_flush_error, meter_shutdown_error))) => {
                report.tracer_flush_errors = tracer_errors;
                report.meter_flush_error = meter_flush_error;
                report.meter_shutdown_error = meter_shutdown_error;
            }
            Some(Err(error)) => report
                .tracer_flush_errors
                .push(format!("provider shutdown task failed: {error}")),
            None => report.provider_timed_out = true,
        }
    }
    report.file_export = shutdown_file_export(deadline).await;
    // SDK acknowledgements and blocking-wrapper return are not the OS metric
    // worker join. All owned workers use the same T; final root observation
    // may later reap them, but cannot replace this failed attempt report.
    let workers = tasks::reconcile_before(deadline).await;
    if !workers.is_terminal() && report.is_success() {
        report.provider_timed_out = true;
    }
    if workers.failed != 0 && report.is_success() {
        report
            .tracer_flush_errors
            .push("tracing worker termination failed".into());
    }
    store_shutdown_report(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct ShutdownCountingProcessor(Arc<AtomicUsize>);

    impl SpanProcessor for ShutdownCountingProcessor {
        fn on_start(&self, _span: &mut opentelemetry_sdk::trace::Span, _context: &OtelContext) {}

        fn on_end(&self, _span: SpanData) {}

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            self.0.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn disabled_initialization_does_not_install_or_lock_the_global_subscriber() {
        assert_eq!(
            tracing_runtime_status(),
            TracingRuntimeStatus::Uninitialized
        );
        assert!(matches!(
            TracingRuntimeOwner::install(&TraceConfig::default()).unwrap(),
            TraceInstallOutcome::Disabled
        ));
        assert_eq!(
            tracing_runtime_status(),
            TracingRuntimeStatus::Uninitialized
        );
    }

    #[tokio::test]
    async fn shutdown_without_initialization_is_an_idempotent_noop() {
        let report =
            shutdown_tracing_before(tokio::time::Instant::now() + Duration::from_millis(10)).await;
        assert!(!report.was_initialized);
        assert!(report.is_success());
        assert_eq!(
            tracing_runtime_status(),
            TracingRuntimeStatus::Uninitialized
        );
    }

    #[test]
    fn controlled_span_processor_stops_its_worker_exactly_once() {
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let processor =
            ControlledSpanProcessor::new(ShutdownCountingProcessor(Arc::clone(&shutdowns)));

        assert!(processor.is_running());
        processor.shutdown_owned().unwrap();
        processor.shutdown_owned().unwrap();

        assert!(!processor.is_running());
        assert_eq!(shutdowns.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    fn https_otlp_endpoint_requires_tonic_tls() {
        assert!(endpoint_requires_tls("https://collector.example:4317"));
        assert!(!endpoint_requires_tls("http://collector.example:4317"));
    }

    #[test]
    fn file_export_initialization_is_fallible_and_path_typed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing").join("trace.jsonl");
        let config = FileExportConfig {
            path: path.display().to_string(),
            rotation: FileRotation::Daily,
            max_file_bytes: 1024 * 1024,
            max_files: 2,
            buffered_lines: 16,
            max_record_bytes: 1024,
        };

        let error = prepare_file_export(&config).err().unwrap();
        assert_eq!(error.path(), path);
    }

    #[tokio::test]
    async fn file_export_writes_jsonl_off_thread_and_flushes_with_its_guard() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("trace.jsonl");
        let config = FileExportConfig {
            path: path.display().to_string(),
            rotation: FileRotation::Daily,
            max_file_bytes: 1024 * 1024,
            max_files: 2,
            buffered_lines: 16,
            max_record_bytes: 1024,
        };
        let PreparedFileExport { writer, runtime } = prepare_file_export(&config).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .json()
            .with_writer(writer)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(lily.test = "file-export", "bounded JSONL event");
        });
        runtime.guard.close();
        runtime.guard.receipt.clone().await.unwrap();

        assert_eq!(runtime.counters.snapshot().total_dropped(), 0);
        let output = fs::read_to_string(path).unwrap();
        let records = output.lines().collect::<Vec<_>>();
        assert_eq!(records.len(), 1);
        let record: serde_json::Value = serde_json::from_str(records[0]).unwrap();
        assert_eq!(record["fields"]["lily.test"], "file-export");
    }

    #[tokio::test]
    async fn jsonl_repeated_event_fields_are_unique_without_losing_value_types() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("repeated.jsonl");
        let config = FileExportConfig {
            path: path.display().to_string(),
            rotation: FileRotation::Daily,
            max_file_bytes: 1024 * 1024,
            max_files: 2,
            buffered_lines: 16,
            max_record_bytes: 4096,
        };
        let (tracer, _provider) = init_local_tracer(&TraceConfig::default());
        let PreparedFileExport { writer, runtime } = prepare_file_export(&config).unwrap();
        let subscriber = tracing_subscriber::registry().with(JsonFieldsLayer).with(
            tracing_subscriber::fmt::layer()
                .with_writer(writer)
                .fmt_fields(NoFields)
                .event_format(JsonFormat(tracer)),
        );
        tracing::subscriber::with_default(subscriber, || {
            // This is the shape generated by opentelemetry's internal macros:
            // an implicit empty message followed by its explicit diagnostic.
            tracing::debug!(target: "opentelemetry", name = "NoopMeterProvider.MeterCreation",
                message = "explicit SDK diagnostic", duration_ms = 0.5, duration_ms = 1.25,
                count = u64::MAX, count = tracing::field::Empty, active = true, "");
            tracing::info!(
                duration_ms = 1.25,
                count = u64::MAX,
                active = true,
                "ordinary event"
            );
        });
        runtime.guard.close();
        runtime.guard.receipt.clone().await.unwrap();
        assert_eq!(runtime.counters.snapshot().total_dropped(), 0);
        let text = fs::read_to_string(path).unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        for (index, line) in lines.iter().enumerate() {
            for key in ["message", "duration_ms", "count", "active"] {
                assert_eq!(
                    line.matches(&format!("\"{key}\":")).count(),
                    1,
                    "raw JSON key {key}"
                );
            }
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(
                value["fields"]["message"],
                if index == 0 {
                    "explicit SDK diagnostic"
                } else {
                    "ordinary event"
                }
            );
            assert_eq!(value["fields"]["duration_ms"].as_f64(), Some(1.25));
            assert_eq!(value["fields"]["count"].as_u64(), Some(u64::MAX));
            assert_eq!(value["fields"]["active"].as_bool(), Some(true));
        }
    }

    #[tokio::test]
    async fn correlation_metadata_counts_toward_the_file_record_limit() {
        fn emit(writer: BoundedFileMakeWriter, tracer: opentelemetry_sdk::trace::Tracer) {
            let subscriber = tracing_subscriber::registry()
                .with(JsonFieldsLayer)
                .with(tracing_opentelemetry::layer().with_tracer(tracer.clone()))
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(writer)
                        .fmt_fields(NoFields)
                        .event_format(JsonFormat(tracer)),
                );
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!("bounded");
                span.in_scope(|| tracing::info!(target: "limit", marker = "inside"));
                tracing::info!(target: "limit", marker = "outside");
            });
        }

        let directory = tempfile::tempdir().unwrap();
        let (tracer, _provider) = init_local_tracer(&TraceConfig::default());
        let mut config = FileExportConfig {
            path: directory
                .path()
                .join("calibration.jsonl")
                .display()
                .to_string(),
            rotation: FileRotation::Daily,
            max_file_bytes: 1024 * 1024,
            max_files: 2,
            buffered_lines: 16,
            max_record_bytes: 4096,
        };
        let PreparedFileExport { writer, runtime } = prepare_file_export(&config).unwrap();
        emit(writer, tracer.clone());
        runtime.guard.close();
        runtime.guard.receipt.clone().await.unwrap();
        assert_eq!(runtime.counters.snapshot().total_dropped(), 0);
        let output = fs::read_to_string(&config.path).unwrap();
        let lines: Vec<_> = output.lines().collect();
        assert_eq!(lines.len(), 2);
        let mut without_ids: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        for key in ["trace_id", "span_id", "trace_flags"] {
            assert!(without_ids.as_object_mut().unwrap().remove(key).is_some());
        }
        let base_bytes = serde_json::to_vec(&without_ids).unwrap().len() + 1;
        let full_bytes = lines[0].len() + 1;
        assert!(full_bytes > base_bytes + 80);
        config.max_record_bytes = (base_bytes + full_bytes) / 2;
        assert!(lines[1].len() < config.max_record_bytes);
        config.path = directory.path().join("bounded.jsonl").display().to_string();
        let PreparedFileExport { writer, runtime } = prepare_file_export(&config).unwrap();
        emit(writer, tracer);
        runtime.guard.close();
        runtime.guard.receipt.clone().await.unwrap();
        let metrics = runtime.counters.snapshot();
        assert_eq!(metrics.oversized, 1);
        assert_eq!(metrics.total_dropped(), 1);
        let output = fs::read_to_string(config.path).unwrap();
        assert_eq!(
            output.lines().count(),
            1,
            "no partial oversized record may be queued"
        );
        let record: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert_eq!(record["fields"]["marker"], "outside");
    }

    #[tokio::test]
    async fn file_export_rejects_oversized_records_before_the_queue() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("trace.jsonl");
        let config = FileExportConfig {
            path: path.display().to_string(),
            rotation: FileRotation::Daily,
            max_file_bytes: 1024 * 1024,
            max_files: 2,
            buffered_lines: 16,
            max_record_bytes: 128,
        };
        let PreparedFileExport { writer, runtime } = prepare_file_export(&config).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .json()
            .with_writer(writer)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(payload = %"x".repeat(512), "oversized event");
        });
        runtime.guard.close();
        runtime.guard.receipt.clone().await.unwrap();

        let metrics = runtime.counters.snapshot();
        assert_eq!(metrics.oversized, 1);
        assert_eq!(metrics.total_dropped(), 1);
        assert!(fs::read_to_string(path).unwrap().is_empty());
    }

    #[tokio::test]
    async fn file_export_enforces_size_rotation_and_total_retention() {
        use tracing_subscriber::fmt::MakeWriter as _;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("trace.jsonl");
        let config = FileExportConfig {
            path: path.display().to_string(),
            rotation: FileRotation::Daily,
            max_file_bytes: 512,
            max_files: 2,
            buffered_lines: 16,
            max_record_bytes: 300,
        };
        let PreparedFileExport { writer, runtime } = prepare_file_export(&config).unwrap();
        for _ in 0..5 {
            let mut event = writer.make_writer();
            event.write_all(&vec![b'x'; 300]).unwrap();
        }
        drop(writer);
        runtime.guard.close();
        runtime.guard.receipt.clone().await.unwrap();

        let files = fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 2);
        assert!(files
            .iter()
            .all(|file| fs::metadata(file).unwrap().len() <= 512));
        assert_eq!(runtime.counters.snapshot().total_dropped(), 0);
    }
}
