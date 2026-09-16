use super::{
    correlation::{CloseContextLayer, ConsoleFormat},
    json_format::{JsonFieldsLayer, JsonFormat, NoFields},
    otlp_log_layer::OtlpLogLayer,
    otlp_span_processor::ExportQueuePolicy,
};
use opentelemetry::{
    trace::{SpanContext, TraceContextExt, TracerProvider as _},
    Context,
};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    logs::{LogBatch, LogExporter, SdkLogRecord},
    trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter},
    Resource,
};
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};

#[derive(Clone, Debug, Default)]
struct Exports {
    spans: Arc<Mutex<Vec<SpanData>>>,
    logs: Arc<Mutex<Vec<SdkLogRecord>>>,
}
impl SpanExporter for Exports {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.spans.lock().unwrap().extend(batch);
        Ok(())
    }
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

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);
impl io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Buffer {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

fn identity(span: &tracing::Span) -> SpanContext {
    crate::context_for_span(span).span().span_context().clone()
}

fn remote(id: u128, sampled: bool) -> Context {
    Context::new().with_remote_span_context(SpanContext::new(
        id.into(),
        17u64.into(),
        opentelemetry::trace::TraceFlags::default().with_sampled(sampled),
        true,
        Default::default(),
    ))
}

#[tokio::test]
async fn json_console_and_otlp_use_the_events_actual_parent_and_final_close_context() {
    let exports = Exports::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exports.clone())
        .build();
    let tracer = provider.tracer("correlation-test");
    let (logs, task, handle) = OtlpLogLayer::with_limits(
        exports.clone(),
        Resource::builder_empty().build(),
        tracer.clone(),
        ExportQueuePolicy {
            max_queue_items: 32,
            max_queue_bytes: 1_000_000,
            max_batch_size: 32,
            flush_interval: Duration::from_secs(60),
            max_export_retries: 0,
            retry_backoff: Duration::from_millis(1),
        },
    );
    let json = Buffer::default();
    let console = Buffer::default();
    let json_writer = json.clone();
    let console_writer = console.clone();
    let subscriber = tracing_subscriber::registry()
        .with(JsonFieldsLayer)
        .with(CloseContextLayer(tracer.clone()))
        .with(tracing_opentelemetry::layer().with_tracer(tracer.clone()))
        .with(logs)
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || json_writer.clone())
                .fmt_fields(NoFields)
                .event_format(JsonFormat(tracer.clone())),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .without_time()
                .with_writer(move || console_writer.clone())
                .with_span_events(FmtSpan::CLOSE)
                .map_event_format(|inner| ConsoleFormat {
                    inner,
                    tracer: tracer.clone(),
                }),
        );

    let expected = tracing::subscriber::with_default(subscriber, || {
        let a = tracing::info_span!(parent: None, "parent_a", count = tracing::field::Empty,
            trace_id = "application-span-value", name = "application-name", enabled = true);
        // This assignment happens AFTER every layer's on_new_span.
        crate::set_parent(&a, remote(101, true));
        let b = tracing::info_span!(parent: None, "parent_b");
        crate::set_parent(&b, remote(202, true));
        let a_id = identity(&a);
        let b_id = identity(&b);
        a.record("count", 1u64);
        a.record("count", u64::MAX);
        a.in_scope(|| {
            tracing::info!(
                marker = "contextual",
                count = u64::MAX,
                negative = -3i64,
                duration = 1.25f64,
                flag = true,
                text = "escaped\n\"Türkçe",
                bytes = &[1u8, 2][..],
                trace_id = "application-event-value",
                span_id = "application-span-id",
                trace_flags = "ff"
            );
            tracing::info!(parent: &b, marker = "explicit");
            tracing::info!(parent: None, marker = "unparented");
        });
        drop(a);
        drop(b);
        // No enter, real event, or context() call can prime the close snapshot.
        let late = tracing::info_span!(parent: None, "close_only");
        crate::set_parent(&late, remote(303, true));
        drop(late);
        (a_id, b_id)
    });
    handle.shutdown();
    let report = tokio::time::timeout(Duration::from_secs(5), task.run())
        .await
        .unwrap();
    assert_eq!(report.metrics.exported, 3);
    assert_eq!(report.metrics.dropped + report.metrics.rejected, 0);
    provider.force_flush().unwrap();

    let records: Vec<serde_json::Value> = json
        .text()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 3);
    for (marker, context, name) in [
        ("contextual", &expected.0, "parent_a"),
        ("explicit", &expected.1, "parent_b"),
    ] {
        let record = records
            .iter()
            .find(|r| r["fields"]["marker"] == marker)
            .unwrap();
        assert_eq!(record["trace_id"], context.trace_id().to_string());
        assert_eq!(record["span_id"], context.span_id().to_string());
        assert_eq!(record["trace_flags"], "01");
        assert_eq!(record["span"]["name"], name);
        assert_eq!(record["spans"].as_array().unwrap().len(), 1);
        assert_eq!(record["spans"][0]["name"], name);
        let console = console.text();
        let line = console
            .lines()
            .find(|line| line.contains(&format!("marker=\"{marker}\"")))
            .unwrap();
        assert!(
            line.starts_with(&format!(
                "trace_id={} span_id={} trace_flags=01 ",
                context.trace_id(),
                context.span_id()
            )),
            "{line}"
        );
    }
    let contextual = &records[0];
    assert_eq!(contextual["span"]["count"], u64::MAX);
    assert_eq!(contextual["span"]["enabled"], true);
    assert_eq!(contextual["span"]["trace_id"], "application-span-value");
    assert_eq!(contextual["fields"]["trace_id"], "application-event-value");
    assert_eq!(contextual["fields"]["span_id"], "application-span-id");
    assert_eq!(contextual["fields"]["trace_flags"], "ff");
    assert_eq!(contextual["fields"]["count"], u64::MAX);
    assert_eq!(contextual["fields"]["negative"], -3);
    assert_eq!(contextual["fields"]["duration"], 1.25);
    assert_eq!(contextual["fields"]["flag"], true);
    assert_eq!(contextual["fields"]["text"], "escaped\n\"Türkçe");
    assert!(contextual["fields"]["bytes"].is_string());
    let unparented = records
        .iter()
        .find(|r| r["fields"]["marker"] == "unparented")
        .unwrap();
    for key in ["trace_id", "span_id", "trace_flags", "span", "spans"] {
        assert!(unparented.get(key).is_none(), "{unparented}");
    }
    let text = console.text();
    let unparented_console = text
        .lines()
        .find(|line| line.contains("marker=\"unparented\""))
        .unwrap();
    assert!(!unparented_console.starts_with("trace_id="));

    let spans = exports.spans.lock().unwrap();
    assert_eq!(spans.len(), 3);
    let closes: Vec<_> = text
        .lines()
        .filter(|line| line.contains(": close"))
        .collect();
    assert_eq!(closes.len(), 3);
    for span in spans.iter() {
        let line = closes
            .iter()
            .find(|line| line.contains(&format!("{}", span.name)))
            .unwrap();
        assert!(
            line.starts_with(&format!(
                "trace_id={} span_id={} trace_flags=01 ",
                span.span_context.trace_id(),
                span.span_context.span_id()
            )),
            "{line}"
        );
        assert_eq!(span.parent_span_id, 17u64.into());
    }
    assert_eq!(
        spans
            .iter()
            .find(|s| s.name == "close_only")
            .unwrap()
            .span_context
            .trace_id(),
        303u128.into()
    );
    let logs = exports.logs.lock().unwrap();
    for (record, expected) in logs
        .iter()
        .zip([Some(&expected.0), Some(&expected.1), None])
    {
        let actual = record.trace_context().unwrap();
        if let Some(expected) = expected {
            assert_eq!(actual.trace_id, expected.trace_id());
            assert_eq!(actual.span_id, expected.span_id());
            assert_eq!(actual.trace_flags, Some(expected.trace_flags()));
        } else {
            assert_eq!(actual.trace_id, opentelemetry::trace::TraceId::INVALID);
            assert_eq!(actual.span_id, opentelemetry::trace::SpanId::INVALID);
        }
    }
}

#[test]
fn unsampled_context_is_valid_and_filtering_does_not_fabricate_an_identity() {
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOff)
        .build();
    let tracer = provider.tracer("unsampled-test");
    let buffer = Buffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            "info,filtered_correlation=off",
        ))
        .with(JsonFieldsLayer)
        .with(tracing_opentelemetry::layer().with_tracer(tracer.clone()))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(move || writer.clone())
                .fmt_fields(NoFields)
                .event_format(JsonFormat(tracer)),
        );
    let expected = tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!(parent: None, "unsampled");
        crate::set_parent(&span, remote(404, false));
        let expected = identity(&span);
        span.in_scope(|| {
            let disabled = tracing::info_span!(target: "filtered_correlation", "disabled");
            assert!(disabled.is_disabled());
            disabled.in_scope(|| tracing::info!(marker = "inherits_parent"));
        });
        tracing::info!(marker = "outside");
        expected
    });
    assert!(expected.is_valid() && !expected.is_sampled());
    let records: Vec<serde_json::Value> = buffer
        .text()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["trace_id"], expected.trace_id().to_string());
    assert_eq!(records[0]["span_id"], expected.span_id().to_string());
    assert_eq!(records[0]["trace_flags"], "00");
    assert_eq!(records[0]["span"]["name"], "unsampled");
    assert!(records[1].get("trace_id").is_none());
}

#[test]
fn json_preserves_the_existing_field_encoding_and_log_bridge_metadata() {
    let provider = SdkTracerProvider::builder().build();
    let tracer = provider.tracer("json-compatibility");
    let original = Buffer::default();
    let correlated = Buffer::default();
    let original_writer = original.clone();
    let correlated_writer = correlated.clone();
    let subscriber = tracing_subscriber::registry()
        .with(JsonFieldsLayer)
        .with(tracing_opentelemetry::layer().with_tracer(tracer.clone()))
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(move || original_writer.clone()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .fmt_fields(NoFields)
                .event_format(JsonFormat(tracer))
                .with_writer(move || correlated_writer.clone()),
        );
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("compatibility", bytes = &[0u8, 255][..], count = tracing::field::Empty,
            text = "unicode 🦀\n\"", debug = ?vec![1, 2], r#type = ?"example");
        span.record("count", u64::MAX);
        span.in_scope(|| {
            tracing::info!(bytes = &[0u8, 255][..], huge = i128::MAX, count = u64::MAX,
                fraction = 0.7498090000000001f64, nan = f64::NAN, infinity = f64::INFINITY,
                error = &std::io::Error::other("test error") as &dyn std::error::Error,
                debug = ?vec![1, 2], r#type = ?"event-example", trace_id = "user-field");
            tracing_log::format_trace(
                &tracing_log::log::Record::builder()
                    .args(format_args!("bridged message"))
                    .level(tracing_log::log::Level::Warn)
                    .target("bridge_target")
                    .module_path(Some("bridge_module"))
                    .file(Some("bridge.rs"))
                    .line(Some(73))
                    .build(),
            )
            .unwrap();
        });
    });
    let original: Vec<serde_json::Value> = original
        .text()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let correlated: Vec<serde_json::Value> = correlated
        .text()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(original.len(), 2);
    assert_eq!(correlated.len(), original.len());
    for (mut old, mut new) in original.into_iter().zip(correlated) {
        for key in ["trace_id", "span_id", "trace_flags"] {
            assert!(new.as_object_mut().unwrap().remove(key).is_some());
        }
        old.as_object_mut().unwrap().remove("timestamp");
        new.as_object_mut().unwrap().remove("timestamp");
        assert_eq!(old, new);
    }
}

#[test]
fn clearing_a_local_parent_keeps_one_root_identity_for_children_and_export() {
    let exports = Exports::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exports.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("detached-roots")));
    let mut expected_roots = Vec::new();
    tracing::subscriber::with_default(subscriber, || {
        let transport = tracing::info_span!(parent: None, "transport");
        let transport_identity = identity(&transport);
        transport.in_scope(|| {
            for _ in 0..2 {
                let request = tracing::info_span!("request");
                crate::set_parent(&request, Context::new());
                // Create the first child before anyone explicitly asks for the
                // request's context, just like the HTTP handler boundary.
                let child = tracing::info_span!(parent: &request, "handler");
                let child_identity = identity(&child);
                let request_identity = identity(&request);
                assert!(request_identity.is_valid());
                assert_ne!(request_identity.trace_id(), transport_identity.trace_id());
                assert_eq!(child_identity.trace_id(), request_identity.trace_id());
                for _ in 0..3 {
                    assert_eq!(identity(&request), request_identity);
                }
                expected_roots.push(request_identity);
            }
        });
    });
    assert_ne!(expected_roots[0].trace_id(), expected_roots[1].trace_id());
    provider.force_flush().unwrap();
    let spans = exports.spans.lock().unwrap();
    assert_eq!(spans.len(), 5);
    for expected in expected_roots {
        let request = spans
            .iter()
            .find(|s| s.span_context.span_id() == expected.span_id())
            .unwrap();
        assert_eq!(request.name, "request");
        assert_eq!(request.span_context, expected);
        assert_eq!(
            request.parent_span_id,
            opentelemetry::trace::SpanId::INVALID
        );
        let child = spans
            .iter()
            .find(|s| s.parent_span_id == expected.span_id())
            .unwrap();
        assert_eq!(child.name, "handler");
        assert_eq!(child.span_context.trace_id(), expected.trace_id());
    }
}
