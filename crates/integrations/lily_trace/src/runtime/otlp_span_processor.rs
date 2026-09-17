//! Bounded, application-owned OTLP span processor.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::Context;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Span, SpanData, SpanExporter, SpanProcessor};
use opentelemetry_sdk::Resource;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::{mpsc, watch};

/// Point-in-time accounting for the bounded OTLP span exporter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpanExportMetricSnapshot {
    /// Spans admitted to the bounded exporter queue.
    pub accepted: u64,
    /// Spans successfully exported.
    pub exported: u64,
    /// Admitted spans discarded after an unrecoverable export failure.
    pub dropped: u64,
    /// Spans rejected before admission because the exporter was stopping or full.
    pub rejected: u64,
    /// Admitted spans not yet accounted for as exported or dropped.
    pub in_flight: u64,
    /// Estimated serialized bytes currently admitted to the queue.
    pub queued_bytes: u64,
    /// Failed exporter calls, including failed retry attempts.
    pub export_failures: u64,
    /// Export attempts made after an initial failure.
    pub retry_attempts: u64,
}

/// Final result returned by the owned OTLP span export task.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpanExportReport {
    /// Terminal exporter counters.
    pub metrics: SpanExportMetricSnapshot,
    /// Exporter calls that failed during the task lifetime.
    pub export_failures: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExportQueuePolicy {
    pub(crate) max_queue_items: usize,
    pub(crate) max_queue_bytes: u64,
    pub(crate) max_batch_size: usize,
    pub(crate) flush_interval: Duration,
    pub(crate) max_export_retries: usize,
    pub(crate) retry_backoff: Duration,
}

#[derive(Default)]
struct SpanExportLedger {
    accepted: AtomicU64,
    exported: AtomicU64,
    dropped: AtomicU64,
    rejected: AtomicU64,
    queued_bytes: AtomicU64,
    export_failures: AtomicU64,
    retry_attempts: AtomicU64,
}

impl SpanExportLedger {
    fn snapshot(&self) -> SpanExportMetricSnapshot {
        let accepted = self.accepted.load(Ordering::Acquire);
        let exported = self.exported.load(Ordering::Acquire);
        let dropped = self.dropped.load(Ordering::Acquire);
        SpanExportMetricSnapshot {
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

struct QueuedSpan {
    data: SpanData,
    estimated_bytes: u64,
}

/// Synchronous SDK hook with non-blocking admission into a bounded queue.
#[derive(Clone)]
pub(crate) struct BoundedSpanProcessor {
    sender: mpsc::Sender<QueuedSpan>,
    accepting: Arc<AtomicBool>,
    ledger: Arc<SpanExportLedger>,
    max_queued_bytes: u64,
    shutdown_sender: watch::Sender<bool>,
}

impl fmt::Debug for BoundedSpanProcessor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedSpanProcessor")
            .field("accepting", &self.accepting.load(Ordering::Acquire))
            .field("metrics", &self.ledger.snapshot())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct SpanExportHandle {
    accepting: Arc<AtomicBool>,
    ledger: Arc<SpanExportLedger>,
    shutdown_sender: watch::Sender<bool>,
}

impl SpanExportHandle {
    pub(crate) fn shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        self.shutdown_sender.send_replace(true);
    }

    pub(crate) fn metrics(&self) -> SpanExportMetricSnapshot {
        self.ledger.snapshot()
    }
}

pub(crate) struct SpanExportTask {
    receiver: mpsc::Receiver<QueuedSpan>,
    shutdown_receiver: watch::Receiver<bool>,
    exporter: Box<dyn DynSpanExporter>,
    batch: Vec<QueuedSpan>,
    batch_size: usize,
    flush_interval: Duration,
    ledger: Arc<SpanExportLedger>,
    export_failures: u64,
    max_export_retries: usize,
    retry_backoff: Duration,
}

trait DynSpanExporter: Send + Sync + fmt::Debug {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> Pin<Box<dyn Future<Output = OTelSdkResult> + Send + '_>>;
    fn shutdown(&mut self) -> OTelSdkResult;
}

impl<T> DynSpanExporter for T
where
    T: SpanExporter + 'static,
{
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> Pin<Box<dyn Future<Output = OTelSdkResult> + Send + '_>> {
        Box::pin(SpanExporter::export(self, batch))
    }

    fn shutdown(&mut self) -> OTelSdkResult {
        SpanExporter::shutdown(self)
    }
}

impl BoundedSpanProcessor {
    pub(crate) fn new(
        mut exporter: impl SpanExporter + 'static,
        resource: &Resource,
        policy: ExportQueuePolicy,
    ) -> (Self, SpanExportTask, SpanExportHandle) {
        exporter.set_resource(resource);
        let (sender, receiver) = mpsc::channel(policy.max_queue_items);
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let accepting = Arc::new(AtomicBool::new(true));
        let ledger = Arc::new(SpanExportLedger::default());
        let processor = Self {
            sender,
            accepting: Arc::clone(&accepting),
            ledger: Arc::clone(&ledger),
            max_queued_bytes: policy.max_queue_bytes,
            shutdown_sender: shutdown_sender.clone(),
        };
        let task = SpanExportTask {
            receiver,
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
        let handle = SpanExportHandle {
            accepting,
            ledger,
            shutdown_sender,
        };
        (processor, task, handle)
    }
}

impl SpanProcessor for BoundedSpanProcessor {
    fn on_start(&self, _span: &mut Span, _context: &Context) {}

    fn on_end(&self, span: SpanData) {
        if !self.accepting.load(Ordering::Acquire) {
            self.ledger.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let estimated_bytes = estimate_span_bytes(&span);
        if !self
            .ledger
            .try_reserve(estimated_bytes, self.max_queued_bytes)
        {
            self.ledger.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self
            .sender
            .try_send(QueuedSpan {
                data: span,
                estimated_bytes,
            })
            .is_err()
        {
            self.ledger
                .queued_bytes
                .fetch_sub(estimated_bytes, Ordering::AcqRel);
            self.ledger.rejected.fetch_add(1, Ordering::Relaxed);
        } else {
            self.ledger.accepted.fetch_add(1, Ordering::Release);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        // The process composition root owns the async flush task. A synchronous
        // provider hook must not block a Tokio worker or create a nested runtime.
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.accepting.store(false, Ordering::Release);
        self.shutdown_sender.send_replace(true);
        Ok(())
    }
}

impl SpanExportTask {
    pub(crate) async fn run(mut self) -> SpanExportReport {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                changed = self.shutdown_receiver.changed() => {
                    if changed.is_err() || *self.shutdown_receiver.borrow() {
                        self.receiver.close();
                        break;
                    }
                }
                item = self.receiver.recv() => match item {
                    Some(item) => {
                        self.batch.push(item);
                        if self.batch.len() >= self.batch_size {
                            self.flush().await;
                        }
                    }
                    None => break,
                },
                _ = interval.tick() => self.flush().await,
            }
        }
        while let Ok(item) = self.receiver.try_recv() {
            self.batch.push(item);
            if self.batch.len() >= self.batch_size {
                self.flush().await;
            }
        }
        self.flush().await;
        let _ = self.exporter.shutdown();
        SpanExportReport {
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
        let spans = batch.into_iter().map(|item| item.data).collect::<Vec<_>>();
        let mut exported = false;
        for attempt in 0..=self.max_export_retries {
            if attempt > 0 {
                self.ledger.retry_attempts.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(retry_delay(self.retry_backoff, attempt - 1)).await;
            }
            match self.exporter.export(spans.clone()).await {
                Ok(()) => {
                    exported = true;
                    break;
                }
                Err(_) => {
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

fn estimate_span_bytes(span: &SpanData) -> u64 {
    let attribute_bytes = span
        .attributes
        .iter()
        .map(|attribute| attribute.key.as_str().len() + attribute.value.to_string().len())
        .sum::<usize>();
    let event_bytes = span
        .events
        .iter()
        .map(|event| {
            event.name.len()
                + event
                    .attributes
                    .iter()
                    .map(|attribute| {
                        attribute.key.as_str().len() + attribute.value.to_string().len()
                    })
                    .sum::<usize>()
        })
        .sum::<usize>();
    (std::mem::size_of::<SpanData>()
        + span.name.len()
        + attribute_bytes
        + event_bytes
        + span.links.len() * 64) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use std::time::SystemTime;

    use opentelemetry::trace::{
        SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
    };
    use opentelemetry::InstrumentationScope;
    use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
    use opentelemetry_sdk::trace::{SpanEvents, SpanLinks};
    use opentelemetry_sdk::Resource;

    #[derive(Debug)]
    struct RecordingExporter {
        attempts: Arc<AtomicUsize>,
        batch_sizes: Arc<Mutex<Vec<usize>>>,
        fail_attempts: usize,
        shutdown: Arc<AtomicBool>,
    }

    impl SpanExporter for RecordingExporter {
        fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
            let attempt = self.attempts.fetch_add(1, Ordering::AcqRel) + 1;
            self.batch_sizes.lock().unwrap().push(batch.len());
            let should_fail = attempt <= self.fail_attempts;
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

        fn shutdown_with_timeout(&mut self, _timeout: Duration) -> OTelSdkResult {
            self.shutdown.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn span_data() -> SpanData {
        SpanData {
            span_context: SpanContext::new(
                TraceId::from_bytes([1; 16]),
                SpanId::from_bytes([1; 8]),
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::INVALID,
            span_kind: SpanKind::Internal,
            name: Cow::Borrowed("bounded-span"),
            start_time: SystemTime::now(),
            end_time: SystemTime::now(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: Status::Unset,
            instrumentation_scope: InstrumentationScope::default(),
        }
    }

    fn exporter(fail_attempts: usize) -> (RecordingExporter, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        (
            RecordingExporter {
                attempts: Arc::clone(&attempts),
                batch_sizes: Arc::new(Mutex::new(Vec::new())),
                fail_attempts,
                shutdown: Arc::clone(&shutdown),
            },
            attempts,
            shutdown,
        )
    }

    #[test]
    fn byte_reservation_is_hard_bounded_and_reconciles() {
        let ledger = SpanExportLedger::default();
        assert!(ledger.try_reserve(64, 64));
        ledger.accepted.fetch_add(1, Ordering::Release);
        assert!(!ledger.try_reserve(1, 64));
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.accepted, 1);
        assert_eq!(snapshot.in_flight, 1);
        assert_eq!(snapshot.queued_bytes, 64);
    }

    #[tokio::test]
    async fn item_overflow_is_rejected_and_accepted_span_drains() {
        let (exporter, attempts, shutdown) = exporter(0);
        let (processor, task, handle) = BoundedSpanProcessor::new(
            exporter,
            &Resource::builder_empty().build(),
            ExportQueuePolicy {
                max_queue_items: 1,
                max_queue_bytes: 64 * 1024,
                max_batch_size: 1,
                flush_interval: Duration::from_secs(1),
                max_export_retries: 0,
                retry_backoff: Duration::from_millis(1),
            },
        );

        processor.on_end(span_data());
        processor.on_end(span_data());
        assert_eq!(handle.metrics().accepted, 1);
        assert_eq!(handle.metrics().rejected, 1);

        let task = tokio::spawn(task.run());
        handle.shutdown();
        let report = task.await.unwrap();
        assert_eq!(attempts.load(Ordering::Acquire), 1);
        assert!(shutdown.load(Ordering::Acquire));
        assert_eq!(report.metrics.exported, 1);
        assert_eq!(report.metrics.dropped, 0);
        assert_eq!(report.metrics.in_flight, 0);
        assert_eq!(report.metrics.queued_bytes, 0);
    }

    #[tokio::test]
    async fn bounded_retry_is_counted_and_reconciles_after_success() {
        let (exporter, attempts, shutdown) = exporter(1);
        let (processor, task, handle) = BoundedSpanProcessor::new(
            exporter,
            &Resource::builder_empty().build(),
            ExportQueuePolicy {
                max_queue_items: 4,
                max_queue_bytes: 64 * 1024,
                max_batch_size: 1,
                flush_interval: Duration::from_secs(1),
                max_export_retries: 2,
                retry_backoff: Duration::from_millis(1),
            },
        );
        let task = tokio::spawn(task.run());
        processor.on_end(span_data());
        handle.shutdown();

        let report = task.await.unwrap();
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert!(shutdown.load(Ordering::Acquire));
        assert_eq!(report.metrics.accepted, 1);
        assert_eq!(report.metrics.exported, 1);
        assert_eq!(report.metrics.dropped, 0);
        assert_eq!(report.metrics.export_failures, 1);
        assert_eq!(report.metrics.retry_attempts, 1);
        assert_eq!(report.export_failures, 1);
    }
}
