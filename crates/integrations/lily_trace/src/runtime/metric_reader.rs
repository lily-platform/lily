//! SDK metric collection with a framework-owned periodic worker and real join.
//!
//! The SDK's periodic reader only exposes a shutdown acknowledgement. Use its
//! ManualReader for aggregation, and keep scheduling/export ownership here.

use std::sync::{mpsc, Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures_util::FutureExt;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::{
    data::ResourceMetrics, exporter::PushMetricExporter, reader::MetricReader, InstrumentKind,
    ManualReader, Pipeline, Temporality,
};
use tokio::time::Instant;

use super::tasks;

type Reply = mpsc::SyncSender<Result<(), String>>;
enum Command {
    Flush(Reply),
    Shutdown(Reply),
}

#[derive(Clone)]
pub(super) struct OwnedMetricReader(Arc<Inner>);

struct Inner {
    reader: Arc<ManualReader>,
    sender: Mutex<Option<mpsc::SyncSender<Command>>>,
    deadline: Arc<Mutex<Option<Instant>>>,
    receipt: OnceLock<tasks::Receipt<()>>,
}

impl std::fmt::Debug for OwnedMetricReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedMetricReader").finish_non_exhaustive()
    }
}

impl OwnedMetricReader {
    pub(super) fn new<E: PushMetricExporter>(
        exporter: E,
        interval: Duration,
    ) -> std::io::Result<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(std::io::Error::other)?;
        let reader = Arc::new(
            ManualReader::builder()
                .with_temporality(exporter.temporality())
                .build(),
        );
        let (sender, receiver) = mpsc::sync_channel(8);
        let deadline = Arc::new(Mutex::new(None));
        let worker_deadline = deadline.clone();
        let owner = Self(Arc::new(Inner {
            reader: reader.clone(),
            sender: Mutex::new(Some(sender)),
            deadline,
            receipt: OnceLock::new(),
        }));
        let worker = std::thread::Builder::new()
            .name("lily-trace-metrics".into())
            .spawn(move || {
                let _suppress = opentelemetry::Context::enter_telemetry_suppressed_scope();
                let mut metrics = ResourceMetrics::default();
                let mut next = std::time::Instant::now() + interval;
                loop {
                    let command = receiver
                        .recv_timeout(next.saturating_duration_since(std::time::Instant::now()));
                    let shutdown = matches!(
                        command,
                        Ok(Command::Shutdown(_)) | Err(mpsc::RecvTimeoutError::Disconnected)
                    );
                    let expired = worker_deadline
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .is_some_and(|end| Instant::now() >= end);
                    let result = (if expired {
                        Err(OTelSdkError::Timeout(Duration::ZERO))
                    } else {
                        reader
                            .collect(&mut metrics)
                            .and_then(|()| runtime.block_on(exporter.export(&metrics)))
                    })
                    .map_err(|e| e.to_string());
                    if shutdown {
                        let result = result.and(exporter.shutdown().map_err(|e| e.to_string()));
                        let result = result.and(reader.shutdown().map_err(|e| e.to_string()));
                        if let Ok(Command::Shutdown(reply)) = command {
                            let _ = reply.send(result.clone());
                        }
                        return result;
                    }
                    if let Ok(Command::Flush(reply)) = command {
                        let _ = reply.send(result);
                    } else if let Err(error) = result {
                        tracing::debug!(%error, "periodic metric export failed");
                    }
                    if std::time::Instant::now() >= next {
                        next = std::time::Instant::now() + interval;
                    }
                }
            })?;
        let thread = tasks::thread_receipt("metrics", worker);
        let receipt = tasks::retain("metrics", async move { thread.await? }.boxed().shared());
        assert!(owner.0.receipt.set(receipt).is_ok());
        Ok(owner)
    }

    pub(super) fn cap_deadline(&self, deadline: Instant) {
        let mut current = self.0.deadline.lock().unwrap_or_else(|p| p.into_inner());
        *current = Some(current.map_or(deadline, |old| old.min(deadline)));
    }

    /// Synchronous producer stop even if no provider callback can be started
    /// at T. The thread remains in the inventory until its actual join.
    pub(super) fn request_stop(&self) {
        if let Some(sender) = self
            .0
            .sender
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let (reply, _) = mpsc::sync_channel(1);
            let _ = sender.try_send(Command::Shutdown(reply));
        }
    }

    fn request(&self, shutdown: bool, timeout: Duration) -> OTelSdkResult {
        let started = Instant::now();
        let local = started.checked_add(timeout).unwrap_or(started);
        let deadline = self
            .0
            .deadline
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .map_or(local, |root| root.min(local));
        let (reply, receive) = mpsc::sync_channel(1);
        {
            let mut sender = self.0.sender.lock().unwrap_or_else(|p| p.into_inner());
            let sender = if shutdown {
                sender.take()
            } else {
                sender.clone()
            }
            .ok_or(OTelSdkError::AlreadyShutdown)?;
            sender
                .try_send(if shutdown {
                    Command::Shutdown(reply)
                } else {
                    Command::Flush(reply)
                })
                .map_err(|e| OTelSdkError::InternalFailure(e.to_string()))?;
        }
        match receive.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(result) => result.map_err(OTelSdkError::InternalFailure),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(OTelSdkError::Timeout(
                deadline.saturating_duration_since(started),
            )),
            Err(error) => Err(OTelSdkError::InternalFailure(error.to_string())),
        }
    }
}

impl MetricReader for OwnedMetricReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.0.reader.register_pipeline(pipeline);
    }
    fn collect(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
        self.0.reader.collect(metrics)
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.request(false, Duration::from_secs(5))
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.request(true, timeout)
    }
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.0.reader.temporality(kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct ExportState {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        calls: AtomicUsize,
        shutdowns: AtomicUsize,
        block_first: bool,
        metrics: AtomicUsize,
    }
    struct Exporter(Arc<ExportState>);
    impl PushMetricExporter for Exporter {
        async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
            let first = self.0.calls.fetch_add(1, Ordering::AcqRel) == 0;
            self.0.metrics.fetch_add(
                metrics
                    .scope_metrics()
                    .map(|s| s.metrics().count())
                    .sum::<usize>(),
                Ordering::AcqRel,
            );
            self.0.entered.notify_one();
            if first && self.0.block_first {
                self.0.release.notified().await;
            }
            Ok(())
        }
        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }
        fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
            self.0.shutdowns.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

    #[tokio::test]
    async fn metric_ack_timeout_does_not_claim_the_worker_thread_join() {
        let state = Arc::new(ExportState {
            block_first: true,
            ..Default::default()
        });
        let reader =
            OwnedMetricReader::new(Exporter(state.clone()), Duration::from_millis(1)).unwrap();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        state.entered.notified().await;
        reader.cap_deadline(Instant::now() + Duration::from_millis(30));
        let flushing = provider.clone();
        let flush = tokio::task::spawn_blocking(move || flushing.force_flush());
        // Retain the provider separately so Drop cannot add a second blocking
        // shutdown wait to this timeout experiment.
        let result = flush.await.unwrap();
        reader.request_stop();
        let receipt = reader.0.receipt.get().unwrap().clone();
        let outstanding = tasks::observe_before(Instant::now(), receipt.clone())
            .await
            .is_none();
        state.release.notify_one();
        let terminal = receipt.await;
        assert!(result.is_err());
        assert!(outstanding);
        assert!(terminal.is_err()); // Final export missed the original cutoff.
        assert_eq!(state.shutdowns.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn owned_metric_reader_collects_real_sdk_metrics_and_joins_on_shutdown() {
        let state = Arc::new(ExportState::default());
        let reader =
            OwnedMetricReader::new(Exporter(state.clone()), Duration::from_secs(60)).unwrap();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        let counter = provider.meter("phase7").u64_counter("requests").build();
        counter.add(3, &[]);
        reader.cap_deadline(Instant::now() + Duration::from_secs(2));
        tokio::task::spawn_blocking(move || provider.shutdown())
            .await
            .unwrap()
            .unwrap();
        reader.0.receipt.get().unwrap().clone().await.unwrap();
        assert!(state.metrics.load(Ordering::Acquire) > 0);
        assert_eq!(state.shutdowns.load(Ordering::Acquire), 1);
        assert!(matches!(
            reader.force_flush(),
            Err(OTelSdkError::AlreadyShutdown)
        ));
    }
}
