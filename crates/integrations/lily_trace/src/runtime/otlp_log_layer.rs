//! Internal bounded OTLP log-event exporter.
//!
//! This module provides a custom `tracing-subscriber` Layer that captures
//! tracing events (info!, debug!, etc.) and exports them as OpenTelemetry
//! logs via OTLP to the configured collector.

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::{trace::SpanContext, InstrumentationScope, Key};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    logs::{LogBatch, LogExporter, SdkLogRecord, SdkLogger, SdkLoggerProvider},
    Resource,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

use super::otlp_span_processor::ExportQueuePolicy;

/// Custom layer that exports tracing events as OpenTelemetry logs
pub(crate) struct OtlpLogLayer {
    log_sender: mpsc::Sender<QueuedLog>,
    logger: SdkLogger,
    tracer: opentelemetry_sdk::trace::Tracer,
    ledger: Arc<LogExportLedger>,
    accepting: Arc<AtomicBool>,
    max_queued_bytes: u64,
}

/// Handle used by the runtime to request a graceful log exporter shutdown.
#[derive(Clone)]
pub(crate) struct LogExportHandle {
    shutdown_sender: watch::Sender<bool>,
    ledger: Arc<LogExportLedger>,
    accepting: Arc<AtomicBool>,
}

impl LogExportHandle {
    /// Requests a final drain, flush, and exporter shutdown.
    pub(crate) fn shutdown(&self) {
        // Close producer admission before waking the consumer. Combined with
        // `Receiver::close` in the task, this prevents an event racing with
        // shutdown from being accepted after the final drain.
        self.accepting.store(false, Ordering::Release);
        self.shutdown_sender.send_replace(true);
    }

    pub(crate) fn metrics(&self) -> LogExportMetricSnapshot {
        self.ledger.snapshot()
    }
}

/// Point-in-time accounting for the bounded OTLP log exporter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogExportMetricSnapshot {
    /// Log records admitted to the bounded exporter queue.
    pub accepted: u64,
    /// Log records successfully exported.
    pub exported: u64,
    /// Admitted records discarded after an unrecoverable export failure.
    pub dropped: u64,
    /// Records rejected before admission because the exporter was stopping or full.
    pub rejected: u64,
    /// Admitted records not yet accounted for as exported or dropped.
    pub in_flight: u64,
    /// Estimated serialized bytes currently admitted to the queue.
    pub queued_bytes: u64,
    /// Failed exporter calls, including failed retry attempts.
    pub export_failures: u64,
    /// Export attempts made after an initial failure.
    pub retry_attempts: u64,
}

/// Final result returned by the owned OTLP log export task.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogExportReport {
    /// Terminal exporter counters.
    pub metrics: LogExportMetricSnapshot,
    /// Exporter calls that failed during the task lifetime.
    pub export_failures: u64,
}

#[derive(Default)]
struct LogExportLedger {
    accepted: AtomicU64,
    exported: AtomicU64,
    dropped: AtomicU64,
    rejected: AtomicU64,
    queued_bytes: AtomicU64,
    export_failures: AtomicU64,
    retry_attempts: AtomicU64,
}

impl LogExportLedger {
    fn snapshot(&self) -> LogExportMetricSnapshot {
        let accepted = self.accepted.load(Ordering::Acquire);
        let exported = self.exported.load(Ordering::Acquire);
        let dropped = self.dropped.load(Ordering::Acquire);
        LogExportMetricSnapshot {
            accepted,
            exported,
            dropped,
            rejected: self.rejected.load(Ordering::Acquire),
            in_flight: accepted.saturating_sub(exported.saturating_add(dropped)),
            queued_bytes: self.queued_bytes.load(Ordering::Acquire),
            export_failures: self.export_failures.load(Ordering::Acquire),
            retry_attempts: self.retry_attempts.load(Ordering::Acquire),
        }
    }

    fn try_reserve(&self, bytes: u64, maximum: u64) -> bool {
        self.queued_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(bytes).filter(|next| *next <= maximum)
            })
            .is_ok()
    }
}

struct QueuedLog {
    data: LogData,
    estimated_bytes: u64,
}

struct LogData {
    record: SdkLogRecord,
    instrumentation: InstrumentationScope,
}

trait DynLogExporter: Send + Sync + std::fmt::Debug {
    fn export<'a>(
        &'a self,
        batch: &'a [LogData],
    ) -> Pin<Box<dyn Future<Output = OTelSdkResult> + Send + 'a>>;
    fn shutdown(&self) -> OTelSdkResult;
}

impl<T> DynLogExporter for T
where
    T: LogExporter + 'static,
{
    fn export<'a>(
        &'a self,
        batch: &'a [LogData],
    ) -> Pin<Box<dyn Future<Output = OTelSdkResult> + Send + 'a>> {
        Box::pin(async move {
            let borrowed = batch
                .iter()
                .map(|data| (&data.record, &data.instrumentation))
                .collect::<Vec<_>>();
            LogExporter::export(self, LogBatch::new(&borrowed)).await
        })
    }

    fn shutdown(&self) -> OTelSdkResult {
        LogExporter::shutdown(self)
    }
}

impl OtlpLogLayer {
    #[cfg(test)]
    fn with_config(
        exporter: impl LogExporter + 'static,
        resource: Resource,
        tracer: opentelemetry_sdk::trace::Tracer,
        channel_capacity: usize,
        batch_size: usize,
        flush_interval: Duration,
    ) -> (Self, LogExportTask, LogExportHandle) {
        Self::with_limits(
            exporter,
            resource,
            tracer,
            ExportQueuePolicy {
                max_queue_items: channel_capacity,
                max_queue_bytes: 4 * 1024 * 1024,
                max_batch_size: batch_size,
                flush_interval,
                max_export_retries: 0,
                retry_backoff: Duration::from_millis(1),
            },
        )
    }

    pub(crate) fn with_limits(
        mut exporter: impl LogExporter + 'static,
        resource: Resource,
        tracer: opentelemetry_sdk::trace::Tracer,
        policy: ExportQueuePolicy,
    ) -> (Self, LogExportTask, LogExportHandle) {
        let (tx, rx) = mpsc::channel(policy.max_queue_items);
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let ledger = Arc::new(LogExportLedger::default());
        let accepting = Arc::new(AtomicBool::new(true));

        exporter.set_resource(&resource);
        let task = LogExportTask {
            receiver: rx,
            shutdown_receiver,
            exporter: Box::new(exporter),
            batch: Vec::with_capacity(policy.max_batch_size),
            batch_size: policy.max_batch_size,
            flush_interval: policy.flush_interval,
            ledger: Arc::clone(&ledger),
            export_failures: 0,
            max_export_retries: policy.max_export_retries,
            retry_backoff: policy.retry_backoff,
        };

        let layer = Self {
            log_sender: tx,
            logger: SdkLoggerProvider::builder().build().logger("lily_trace"),
            tracer,
            ledger: Arc::clone(&ledger),
            accepting: Arc::clone(&accepting),
            max_queued_bytes: policy.max_queue_bytes,
        };

        let handle = LogExportHandle {
            shutdown_sender,
            ledger,
            accepting,
        };
        (layer, task, handle)
    }
}

impl<S> Layer<S> for OtlpLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if !self.accepting.load(Ordering::Acquire) {
            self.ledger.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Look up the event's actual parent without reentering the dispatcher.
        // Span::current() inside on_event can see a disabled dispatch, and also
        // ignores explicit event parents. Use the same tracer as the OTel layer
        // so IDs and sampling flags agree with the eventually exported span.
        let span_context = ctx
            .event_span(event)
            .and_then(|span| super::correlation::span_context(&span, &self.tracer))
            .unwrap_or_else(SpanContext::empty_context);

        // Convert tracing event to OpenTelemetry LogRecord
        let (log_record, estimated_bytes) = event_to_log_record(event, &span_context, &self.logger);

        // Both item count and estimated bytes are hard-capped. Producers never
        // block request tasks behind an exporter outage.
        if !self
            .ledger
            .try_reserve(estimated_bytes, self.max_queued_bytes)
        {
            self.ledger.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let item = QueuedLog {
            data: log_record,
            estimated_bytes,
        };
        if self.log_sender.try_send(item).is_err() {
            self.ledger
                .queued_bytes
                .fetch_sub(estimated_bytes, Ordering::AcqRel);
            self.ledger.rejected.fetch_add(1, Ordering::Relaxed);
        } else {
            self.ledger.accepted.fetch_add(1, Ordering::Release);
        }
    }
}

/// Convert a tracing Event to an OpenTelemetry LogRecord
fn event_to_log_record(
    event: &Event<'_>,
    span_context: &SpanContext,
    logger: &SdkLogger,
) -> (LogData, u64) {
    let metadata = event.metadata();

    // Map tracing level to OpenTelemetry severity
    let severity = match *metadata.level() {
        tracing::Level::ERROR => Severity::Error,
        tracing::Level::WARN => Severity::Warn,
        tracing::Level::INFO => Severity::Info,
        tracing::Level::DEBUG => Severity::Debug,
        tracing::Level::TRACE => Severity::Trace,
    };

    // Extract message and attributes from event
    let mut visitor = LogVisitor::default();
    event.record(&mut visitor);

    enrich_with_current_component(&mut visitor.attributes);
    let estimated_bytes = visitor.estimated_bytes(metadata.target()) as u64;

    // Build attributes vector
    let mut attributes = Vec::new();

    // Add target
    attributes.push((Key::new("target"), AnyValue::from(metadata.target())));

    // Add message as body attribute

    // Add all other attributes
    for (key, value) in visitor.attributes {
        attributes.push((Key::new(key), value));
    }

    // Build LogRecord with all attributes
    let mut record = logger.create_log_record();
    record.set_severity_number(severity);
    record.set_trace_context(
        span_context.trace_id(),
        span_context.span_id(),
        Some(span_context.trace_flags()),
    );
    record.set_severity_text(metadata.level().as_str());
    record.set_observed_timestamp(std::time::SystemTime::now());

    if let Some(message) = visitor.message {
        record.set_body(message.into());
    }

    for (key, value) in attributes {
        record.add_attribute(key, value);
    }

    (
        LogData {
            record,
            instrumentation: Default::default(),
        },
        estimated_bytes,
    )
}

fn enrich_with_current_component(attributes: &mut HashMap<String, AnyValue>) {
    if let Some(component) = crate::current_component() {
        attributes.insert(
            "lily.component.id".to_string(),
            AnyValue::String(component.id.clone().into()),
        );
        attributes.insert(
            "lily.component.kind".to_string(),
            AnyValue::String(component.kind.clone().into()),
        );
        attributes.insert(
            "lily.component.name".to_string(),
            AnyValue::String(component.worker_name.clone().into()),
        );
        attributes.insert(
            "lily.component.service.name".to_string(),
            AnyValue::String(component.service_name.clone().into()),
        );
        attributes.insert(
            "lily.worker.type".to_string(),
            AnyValue::String(component.worker_type.clone().into()),
        );
    }
}

fn estimate_any_value_bytes(value: &AnyValue) -> usize {
    match value {
        AnyValue::Int(_) | AnyValue::Double(_) => std::mem::size_of::<u64>(),
        AnyValue::Boolean(_) => std::mem::size_of::<bool>(),
        AnyValue::String(value) => value.as_str().len(),
        AnyValue::Bytes(value) => value.len(),
        AnyValue::ListAny(values) => values.iter().map(estimate_any_value_bytes).sum(),
        AnyValue::Map(values) => values
            .iter()
            .map(|(key, value)| key.as_str().len() + estimate_any_value_bytes(value))
            .sum(),
        _ => std::mem::size_of::<AnyValue>(),
    }
}

/// Visitor to extract fields from tracing events
#[derive(Default)]
struct LogVisitor {
    message: Option<String>,
    attributes: HashMap<String, AnyValue>,
}

impl LogVisitor {
    fn estimated_bytes(&self, target: &str) -> usize {
        let message = self.message.as_ref().map_or(0, String::len);
        let attributes = self
            .attributes
            .iter()
            .map(|(key, value)| key.len() + estimate_any_value_bytes(value))
            .sum::<usize>();
        std::mem::size_of::<LogData>() + target.len() + message + attributes
    }
}

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let value_str = format!("{:?}", value);

        if field.name() == "message" {
            self.message = Some(value_str);
        } else {
            self.attributes
                .insert(field.name().to_string(), AnyValue::String(value_str.into()));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.attributes.insert(
                field.name().to_string(),
                AnyValue::String(value.to_string().into()),
            );
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.attributes
            .insert(field.name().to_string(), AnyValue::Int(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        // OTLP only has signed 64-bit integers. Saturate counts at the
        // representable maximum; a positive count must never wrap negative.
        self.attributes.insert(
            field.name().to_string(),
            AnyValue::Int(i64::try_from(value).unwrap_or(i64::MAX)),
        );
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.attributes
            .insert(field.name().to_string(), AnyValue::Boolean(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.attributes
            .insert(field.name().to_string(), AnyValue::Double(value));
    }
}

/// Background task that batches and exports logs
pub(crate) struct LogExportTask {
    receiver: mpsc::Receiver<QueuedLog>,
    shutdown_receiver: watch::Receiver<bool>,
    exporter: Box<dyn DynLogExporter>,
    batch: Vec<QueuedLog>,
    batch_size: usize,
    flush_interval: Duration,
    ledger: Arc<LogExportLedger>,
    export_failures: u64,
    max_export_retries: usize,
    retry_backoff: Duration,
}

impl LogExportTask {
    /// Run the export task (should be spawned on tokio runtime)
    pub(crate) async fn run(mut self) -> LogExportReport {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                changed = self.shutdown_receiver.changed() => {
                    if changed.is_err() || *self.shutdown_receiver.borrow() {
                        // Refuse any sender which passed the admission check
                        // immediately before shutdown. `try_send` then returns
                        // the item to the layer, which releases its byte
                        // reservation instead of leaving unaccounted work.
                        self.receiver.close();
                        break;
                    }
                }
                item = self.receiver.recv() => {
                    match item {
                        Some(log_data) => {
                            self.batch.push(log_data);
                            if self.batch.len() >= self.batch_size {
                                self.flush().await;
                            }
                        }
                        None => break,
                    }
                }
                _ = interval.tick() => self.flush().await,
            }
        }

        while let Ok(log_data) = self.receiver.try_recv() {
            self.batch.push(log_data);
            if self.batch.len() >= self.batch_size {
                self.flush().await;
            }
        }

        self.flush().await;
        let _ = self.exporter.shutdown();
        LogExportReport {
            metrics: self.ledger.snapshot(),
            export_failures: self.export_failures,
        }
    }

    async fn flush(&mut self) {
        if self.batch.is_empty() {
            return;
        }

        let batch = std::mem::take(&mut self.batch);
        let count = batch.len() as u64;
        let bytes = batch.iter().map(|item| item.estimated_bytes).sum::<u64>();
        let export = batch.into_iter().map(|item| item.data).collect::<Vec<_>>();
        let mut exported = false;
        for attempt in 0..=self.max_export_retries {
            if attempt > 0 {
                self.ledger.retry_attempts.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(retry_delay(self.retry_backoff, attempt - 1)).await;
            }
            match self.exporter.export(&export).await {
                Ok(()) => {
                    exported = true;
                    break;
                }
                Err(error) => {
                    eprintln!("OTLP log export attempt failed: {error}");
                    self.export_failures += 1;
                    self.ledger.export_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if exported {
            self.ledger.exported.fetch_add(count, Ordering::Release);
        } else {
            self.ledger.dropped.fetch_add(count, Ordering::Release);
        }
        self.ledger.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }
}

fn retry_delay(initial: Duration, retry_index: usize) -> Duration {
    initial.saturating_mul(
        1_u32
            .checked_shl(retry_index.min(31) as u32)
            .unwrap_or(u32::MAX),
    )
}

#[cfg(test)]
#[path = "method_export_tests.rs"]
mod method_export_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
    use std::sync::{atomic::AtomicBool, Mutex};

    #[derive(Debug)]
    struct RecordingExporter {
        batches: Arc<Mutex<Vec<usize>>>,
        shutdown: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct FlakyExporter {
        attempts: Arc<AtomicU64>,
    }

    impl LogExporter for FlakyExporter {
        fn export(&self, _batch: LogBatch<'_>) -> impl Future<Output = OTelSdkResult> + Send {
            let should_fail = self.attempts.fetch_add(1, Ordering::AcqRel) == 0;
            async move {
                if should_fail {
                    Err(OTelSdkError::InternalFailure(
                        "expected exporter failure".to_string(),
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    impl LogExporter for RecordingExporter {
        fn export(&self, batch: LogBatch<'_>) -> impl Future<Output = OTelSdkResult> + Send {
            self.batches.lock().unwrap().push(batch.iter().count());
            async { Ok(()) }
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            self.shutdown.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn log_data() -> LogData {
        LogData {
            record: SdkLoggerProvider::builder()
                .build()
                .logger("lily_trace.test")
                .create_log_record(),
            instrumentation: Default::default(),
        }
    }

    #[tokio::test]
    async fn adds_current_component_to_log_attributes() {
        let component = Arc::new(crate::ComponentIdentity {
            id: "product-worker-id".to_string(),
            worker_type: "ProductManageWorker".to_string(),
            worker_name: "product".to_string(),
            kind: "manage_worker".to_string(),
            service_name: "product-worker-id_manage_worker.product".to_string(),
        });

        let attributes = crate::scope_component(Some(component), async {
            let mut attributes = HashMap::new();
            enrich_with_current_component(&mut attributes);
            attributes
        })
        .await;

        assert_eq!(
            attributes.get("lily.component.service.name"),
            Some(&AnyValue::String(
                "product-worker-id_manage_worker.product".to_string().into()
            ))
        );
    }

    #[tokio::test]
    async fn periodically_flushes_and_gracefully_shuts_down() {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let exporter_shutdown = Arc::new(AtomicBool::new(false));
        let exporter = RecordingExporter {
            batches: batches.clone(),
            shutdown: exporter_shutdown.clone(),
        };
        let (layer, task, handle) = OtlpLogLayer::with_config(
            exporter,
            Resource::builder_empty().build(),
            opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .build()
                .tracer("test"),
            4,
            3,
            Duration::from_millis(20),
        );

        let task = tokio::spawn(task.run());
        let estimated_bytes = 128;
        assert!(layer
            .ledger
            .try_reserve(estimated_bytes, layer.max_queued_bytes));
        layer.ledger.accepted.fetch_add(1, Ordering::Release);
        layer
            .log_sender
            .try_send(QueuedLog {
                data: log_data(),
                estimated_bytes,
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(*batches.lock().unwrap(), vec![1]);

        handle.shutdown();
        let report = task.await.unwrap();
        assert!(exporter_shutdown.load(Ordering::SeqCst));
        assert!(!layer.accepting.load(Ordering::Acquire));
        assert!(layer
            .log_sender
            .try_send(QueuedLog {
                data: log_data(),
                estimated_bytes: 1,
            })
            .is_err());
        assert_eq!(report.metrics.accepted, 1);
        assert_eq!(report.metrics.exported, 1);
        assert_eq!(report.metrics.dropped, 0);
        assert_eq!(report.metrics.in_flight, 0);
        assert_eq!(report.metrics.queued_bytes, 0);
    }

    #[test]
    fn byte_ledger_rejects_capacity_overflow_without_overcommitting() {
        let ledger = LogExportLedger::default();
        assert!(ledger.try_reserve(64, 64));
        assert!(!ledger.try_reserve(1, 64));
        assert_eq!(ledger.snapshot().queued_bytes, 64);
    }

    #[tokio::test]
    async fn failed_log_export_is_retried_with_terminal_accounting() {
        let attempts = Arc::new(AtomicU64::new(0));
        let (_layer, mut task, handle) = OtlpLogLayer::with_limits(
            FlakyExporter {
                attempts: Arc::clone(&attempts),
            },
            Resource::builder_empty().build(),
            opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .build()
                .tracer("test"),
            ExportQueuePolicy {
                max_queue_items: 4,
                max_queue_bytes: 64 * 1024,
                max_batch_size: 1,
                flush_interval: Duration::from_secs(1),
                max_export_retries: 2,
                retry_backoff: Duration::from_millis(1),
            },
        );
        let estimated_bytes = 128;
        assert!(task.ledger.try_reserve(estimated_bytes, 64 * 1024));
        task.ledger.accepted.fetch_add(1, Ordering::Release);
        task.batch.push(QueuedLog {
            data: log_data(),
            estimated_bytes,
        });
        handle.shutdown();

        let report = task.run().await;
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert_eq!(report.metrics.accepted, 1);
        assert_eq!(report.metrics.exported, 1);
        assert_eq!(report.metrics.dropped, 0);
        assert_eq!(report.metrics.export_failures, 1);
        assert_eq!(report.metrics.retry_attempts, 1);
        assert_eq!(report.metrics.in_flight, 0);
        assert_eq!(report.metrics.queued_bytes, 0);
    }
}
