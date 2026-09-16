//! Exercise generated methods through the real log layer and OTel span layer.

use super::*;
use opentelemetry::trace::{Status, TracerProvider as _};
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::sync::Mutex;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;

#[derive(Clone, Debug, Default)]
struct Exports {
    logs: Arc<Mutex<Vec<SdkLogRecord>>>,
    spans: Arc<Mutex<Vec<SpanData>>>,
}

impl LogExporter for Exports {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        self.logs
            .lock()
            .unwrap()
            .extend(batch.iter().map(|(record, _)| record.clone()));
        Ok(())
    }
}

impl SpanExporter for Exports {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.spans.lock().unwrap().extend(batch);
        Ok(())
    }
}

fn setup() -> (
    Exports,
    SdkTracerProvider,
    tracing::Dispatch,
    LogExportTask,
    LogExportHandle,
) {
    let exports = Exports::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exports.clone())
        .build();
    let (logs, task, handle) = OtlpLogLayer::with_config(
        exports.clone(),
        Resource::builder_empty().build(),
        provider.tracer("method-tests"),
        16,
        16,
        Duration::from_secs(60),
    );
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("method-tests")))
        .with(logs);
    (
        exports,
        provider,
        tracing::Dispatch::new(subscriber),
        task,
        handle,
    )
}

struct Failure(bool);
impl crate::TraceResultError for Failure {
    fn trace_failure(&self) -> crate::TraceFailure {
        if self.0 {
            crate::TraceFailure::Error {
                code: "service_unavailable",
            }
        } else {
            crate::TraceFailure::Rejected {
                code: "invalid_credentials",
            }
        }
    }
}

// ERROR-level events must not override the explicit result classification.
#[crate::lily_trace(
    name = "export.classified",
    level = "error",
    result,
    crate_path = "crate"
)]
fn classified(outcome: &str) -> Result<(), Failure> {
    match outcome {
        "success" => Ok(()),
        "rejected" => Err(Failure(false)),
        "error" => Err(Failure(true)),
        _ => unreachable!(),
    }
}

fn attribute<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a Value> {
    attributes
        .iter()
        .find(|pair| pair.key.as_str() == key)
        .map(|pair| &pair.value)
}

fn log_attribute<'a>(record: &'a SdkLogRecord, key: &str) -> Option<&'a AnyValue> {
    record
        .attributes_iter()
        .find(|(name, _)| name.as_str() == key)
        .map(|(_, value)| value)
}

#[tokio::test]
async fn unsigned_log_values_saturate_at_the_otlp_signed_integer_boundary() {
    let (exports, provider, dispatch, task, handle) = setup();
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("export.unsigned");
        let _entered = span.enter();
        for value in [0_u64, i64::MAX as u64, (i64::MAX as u64) + 1, u64::MAX] {
            tracing::info!(bytes = value, "numeric boundary");
        }
    });
    handle.shutdown();
    let report = task.run().await;
    assert_eq!(report.metrics.accepted, 4);
    assert_eq!(report.metrics.exported, 4);
    assert_eq!(report.metrics.dropped, 0);
    assert_eq!(report.metrics.rejected, 0);
    assert_eq!(report.metrics.in_flight, 0);
    provider.force_flush().unwrap();
    let logs = exports.logs.lock().unwrap();
    assert_eq!(logs.len(), 4);
    let spans = exports.spans.lock().unwrap();
    assert_eq!(spans.len(), 1);
    for (record, expected) in logs.iter().zip([0, i64::MAX, i64::MAX, i64::MAX]) {
        assert_eq!(
            log_attribute(record, "bytes"),
            Some(&AnyValue::Int(expected))
        );
        let context = record.trace_context().unwrap();
        assert_eq!(context.trace_id, spans[0].span_context.trace_id());
        assert_eq!(context.span_id, spans[0].span_context.span_id());
    }
}

fn assert_correlation(logs: &[SdkLogRecord], span: &SpanData) {
    assert!(span.span_context.is_valid());
    let keys: std::collections::HashSet<_> = span
        .attributes
        .iter()
        .map(|pair| pair.key.as_str())
        .collect();
    assert_eq!(
        keys.len(),
        span.attributes.len(),
        "duplicate exported span attributes: {:?}",
        span.attributes
    );
    assert_eq!(logs.len(), 2);
    assert_eq!(span.events.len(), 2);
    assert_eq!(span.events.dropped_count, 0);
    for (index, record) in logs.iter().enumerate() {
        let context = record
            .trace_context()
            .expect("method log must carry OTel context");
        assert_eq!(context.trace_id, span.span_context.trace_id());
        assert_eq!(context.span_id, span.span_context.span_id());
        assert_eq!(context.trace_flags, Some(span.span_context.trace_flags()));
        assert_eq!(
            log_attribute(record, "target"),
            Some(&AnyValue::from(module_path!()))
        );
        let phase = log_attribute(record, "lily.lifecycle").unwrap();
        assert_eq!(
            phase,
            &AnyValue::from(
                attribute(&span.events[index].attributes, "lily.lifecycle")
                    .unwrap()
                    .as_str()
                    .to_string()
            )
        );
    }
    assert_eq!(
        log_attribute(&logs[0], "lily.lifecycle"),
        Some(&AnyValue::from("started"))
    );
    assert!(log_attribute(&logs[0], "lily.duration_ms").is_none());
    assert!(log_attribute(&logs[0], "lily.outcome").is_none());
    assert!(log_attribute(&logs[0], "lily.error_code").is_none());
    assert_eq!(
        log_attribute(&logs[1], "lily.lifecycle"),
        Some(&AnyValue::from(
            attribute(&span.attributes, "lily.lifecycle")
                .unwrap()
                .as_str()
                .to_string()
        ))
    );
    let Value::F64(duration) = attribute(&span.attributes, "lily.duration_ms").unwrap() else {
        panic!("span duration must be numeric milliseconds");
    };
    assert!(duration.is_finite() && *duration >= 0.0);
    assert_eq!(
        log_attribute(&logs[1], "lily.duration_ms"),
        Some(&AnyValue::Double(*duration))
    );
    assert_eq!(
        attribute(&span.events[1].attributes, "lily.duration_ms"),
        Some(&Value::F64(*duration))
    );
}

#[tokio::test]
async fn classified_method_exports_exact_lifecycle_codes_status_and_correlation() {
    for outcome in ["success", "rejected", "error"] {
        let (exports, provider, dispatch, task, handle) = setup();
        let result = tracing::dispatcher::with_default(&dispatch, || classified(outcome));
        assert_eq!(result.is_ok(), outcome == "success");
        handle.shutdown();
        let report = task.run().await;
        assert_eq!(report.metrics.accepted, 2);
        assert_eq!(report.metrics.exported, 2);
        assert_eq!(report.metrics.dropped, 0);
        assert_eq!(report.metrics.rejected, 0);
        assert_eq!(report.metrics.in_flight, 0);
        provider.force_flush().unwrap();
        let logs = exports.logs.lock().unwrap();
        let spans = exports.spans.lock().unwrap();
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, "export.classified");
        assert_correlation(&logs, span);
        assert_eq!(
            attribute(&span.attributes, "lily.outcome"),
            Some(&Value::from(outcome))
        );
        assert_eq!(
            log_attribute(&logs[1], "lily.outcome"),
            Some(&AnyValue::from(outcome))
        );
        assert_eq!(
            log_attribute(&logs[1], "lily.lifecycle"),
            Some(&AnyValue::from("completed"))
        );
        if outcome == "success" {
            assert!(attribute(&span.attributes, "lily.error_code").is_none());
            assert!(log_attribute(&logs[1], "lily.error_code").is_none());
        } else {
            let code = if outcome == "error" {
                "service_unavailable"
            } else {
                "invalid_credentials"
            };
            assert_eq!(
                attribute(&span.attributes, "lily.error_code"),
                Some(&Value::from(code))
            );
            assert_eq!(
                log_attribute(&logs[1], "lily.error_code"),
                Some(&AnyValue::from(code))
            );
            assert_eq!(
                attribute(&span.events[1].attributes, "lily.error_code"),
                Some(&Value::from(code))
            );
        }
        if outcome == "error" {
            assert!(matches!(span.status, Status::Error { .. }));
        } else {
            assert_eq!(span.status, Status::Ok);
        }
    }
}

#[crate::lily_trace(name = "export.pending", result, crate_path = "crate")]
async fn pending() -> Result<(), Failure> {
    std::future::pending().await
}

#[tokio::test]
async fn future_drop_outside_original_dispatch_exports_terminal_to_original_span() {
    let (exports, provider, dispatch, task, handle) = setup();
    let mut future = Box::pin(pending().with_subscriber(dispatch));
    assert!(futures_util::poll!(&mut future).is_pending());
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        drop(future)
    });
    handle.shutdown();
    let report = task.run().await;
    assert_eq!(report.metrics.exported, 2);
    assert_eq!(report.metrics.dropped, 0);
    assert_eq!(report.metrics.rejected, 0);
    provider.force_flush().unwrap();
    let spans = exports.spans.lock().unwrap();
    let logs = exports.logs.lock().unwrap();
    assert_eq!(spans.len(), 1);
    assert_correlation(&logs, &spans[0]);
    assert_eq!(
        log_attribute(&logs[1], "lily.lifecycle"),
        Some(&AnyValue::from("dropped"))
    );
    assert!(log_attribute(&logs[1], "lily.outcome").is_none());
    assert!(log_attribute(&logs[1], "lily.error_code").is_none());
}

#[tokio::test]
async fn manual_events_respect_explicit_contextual_and_root_parents() {
    let (exports, provider, dispatch, task, handle) = setup();
    tracing::dispatcher::with_default(&dispatch, || {
        let intended = tracing::info_span!("intended");
        let current = tracing::info_span!("current");
        current.in_scope(|| {
            tracing::info!(parent: &intended, "explicit parent");
            tracing::info!("contextual parent");
            tracing::info!(parent: None, "root event");
        });
    });
    handle.shutdown();
    let report = task.run().await;
    assert_eq!(report.metrics.exported, 3);
    assert_eq!(report.metrics.rejected, 0);
    assert_eq!(report.metrics.dropped, 0);
    provider.force_flush().unwrap();
    let logs = exports.logs.lock().unwrap();
    let spans = exports.spans.lock().unwrap();
    assert_eq!(logs.len(), 3);
    assert_eq!(spans.len(), 2);
    let intended = spans.iter().find(|span| span.name == "intended").unwrap();
    let current = spans.iter().find(|span| span.name == "current").unwrap();
    assert_ne!(
        intended.span_context.trace_id(),
        current.span_context.trace_id()
    );
    for (record, span) in [(&logs[0], intended), (&logs[1], current)] {
        assert_eq!(
            record.trace_context().unwrap().trace_id,
            span.span_context.trace_id()
        );
        assert_eq!(
            record.trace_context().unwrap().span_id,
            span.span_context.span_id()
        );
    }
    let root = logs[2].trace_context().unwrap();
    assert_eq!(root.trace_id, opentelemetry::trace::TraceId::INVALID);
    assert_eq!(root.span_id, opentelemetry::trace::SpanId::INVALID);
}
