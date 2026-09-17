use super::*;
use opentelemetry::trace::{Status, TracerProvider as _};
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing_subscriber::prelude::*;

fn observe(finish: impl FnOnce(&mut RequestTaskGuard)) -> SpanData {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("http-terminal-test")));
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!(
            "http.server.request",
            http.response.status_code = tracing::field::Empty,
            http.request.body.size = tracing::field::Empty,
            http.response.body.size = tracing::field::Empty,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            lily.application_error = tracing::field::Empty,
            lily.application_error_code = tracing::field::Empty,
            lily.application_outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let activity = ConnectionActivity::new();
        let mut guard = activity.enter();
        guard.observe(span, HttpServerTelemetry::new(), "1.1", "GET");
        guard
            .request_byte_counter()
            .unwrap()
            .store(7, Ordering::Release);
        finish(&mut guard);
    });
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 1, "one request must export exactly one span");
    spans.into_iter().next().unwrap()
}

fn one<'a>(attributes: &'a [KeyValue], key: &str) -> &'a Value {
    let values: Vec<_> = attributes
        .iter()
        .filter(|p| p.key.as_str() == key)
        .collect();
    assert_eq!(values.len(), 1, "{key} must occur once: {attributes:?}");
    &values[0].value
}

#[test]
fn rejected_response_is_not_an_otel_error() {
    let span = observe(|guard| {
        guard.record_response(409, 11, "rejected", Some("CONFLICT"));
        guard.complete();
    });
    assert_eq!(span.status, Status::Unset);
}

#[test]
fn timeout_response_without_a_transport_override_still_reports_a_technical_failure() {
    let span = observe(|guard| {
        guard.record_response(504, 0, "timeout", Some("GATEWAY_TIMEOUT"));
        guard.complete();
    });
    assert!(matches!(span.status, Status::Error { .. }));
    assert_eq!(
        one(&span.attributes, "lily.outcome"),
        &Value::from("timeout")
    );
}

#[test]
fn terminal_snapshot_has_unique_typed_attributes_and_one_event() {
    let span = observe(|guard| {
        guard.record_response(200, 0, "success", None);
        guard.add_response_bytes(3);
        guard.add_response_bytes(8);
        guard.complete();
        guard.complete(); // repeated completion and subsequent Drop must be inert
    });
    assert_eq!(
        one(&span.attributes, "http.response.body.size"),
        &Value::I64(11)
    );
    assert_eq!(
        one(&span.attributes, "http.request.body.size"),
        &Value::I64(7)
    );
    assert_eq!(
        one(&span.attributes, "http.response.status_code"),
        &Value::I64(200)
    );
    assert_eq!(
        one(&span.attributes, "lily.outcome"),
        &Value::from("success")
    );
    assert_eq!(span.events.len(), 1);
    assert_eq!(span.events.dropped_count, 0);
    assert_eq!(span.dropped_attributes_count, 0);
}

#[test]
fn stream_failure_replaces_provisional_success_without_duplicate_attributes() {
    let span = observe(|guard| {
        guard.record_response(200, 0, "success", None);
        guard.add_response_bytes(5);
        guard.fail_response("STREAM_FAILURE");
        guard.complete();
    });
    assert!(matches!(span.status, Status::Error { .. }));
    assert_eq!(one(&span.attributes, "lily.outcome"), &Value::from("error"));
    assert_eq!(
        one(&span.attributes, "lily.error_code"),
        &Value::from("STREAM_FAILURE")
    );
    assert_eq!(
        one(&span.attributes, "http.response.body.size"),
        &Value::I64(5)
    );
    assert_eq!(span.events.len(), 1);
}

#[test]
fn transport_stop_wins_over_prepared_4xx_and_drop_does_not_complete_twice() {
    for (reason, outcome, code) in [
        (
            ExecutionStopReason::RequestTimeout,
            "timeout",
            "REQUEST_TIMEOUT",
        ),
        (
            ExecutionStopReason::ResponseFinalizationTimeout,
            "response_finalization_timeout",
            "RESPONSE_FINALIZATION_TIMEOUT",
        ),
        (
            ExecutionStopReason::PeerDisconnect,
            "client_disconnect",
            "CLIENT_DISCONNECT",
        ),
        (
            ExecutionStopReason::TransportFailure,
            "transport_failure",
            "TRANSPORT_FAILURE",
        ),
        (
            ExecutionStopReason::GracefulDeadline,
            "graceful_deadline",
            "GRACEFUL_DEADLINE",
        ),
        (
            ExecutionStopReason::ForcedShutdown,
            "forced_shutdown",
            "FORCED_SHUTDOWN",
        ),
        (
            ExecutionStopReason::ServiceWaiterDropped,
            "cancelled",
            "REQUEST_CANCELLED",
        ),
    ] {
        for explicit_completion in [false, true] {
            let span = observe(|guard| {
                let control = guard.bind_transport(Version::HTTP_11);
                guard.record_response(409, 0, "rejected", Some("CONFLICT"));
                guard.add_response_bytes(5);
                assert!(control.request_stop(reason));
                if explicit_completion {
                    guard.complete();
                    guard.fail_response("LATE_FAILURE_MUST_NOT_REPLACE_TERMINAL");
                    guard.complete();
                }
                // The other variant exercises implicit Drop with a pending stop.
            });
            assert!(matches!(span.status, Status::Error { .. }), "{reason:?}");
            assert_eq!(
                one(&span.attributes, "http.response.status_code"),
                &Value::I64(409)
            );
            assert_eq!(
                one(&span.attributes, "http.response.body.size"),
                &Value::I64(5)
            );
            assert_eq!(one(&span.attributes, "lily.outcome"), &Value::from(outcome));
            assert_eq!(one(&span.attributes, "lily.error_code"), &Value::from(code));
            assert_eq!(span.events.len(), 1);
            assert_eq!(
                one(&span.events[0].attributes, "lily.outcome"),
                &Value::from(outcome)
            );
            assert_eq!(
                one(&span.events[0].attributes, "lily.error_code"),
                &Value::from(code)
            );
        }
    }
}

#[test]
fn terminal_byte_counts_saturate_without_becoming_negative() {
    for (bytes, expected) in [
        (0, 0),
        (i64::MAX as u64, i64::MAX),
        ((i64::MAX as u64) + 1, i64::MAX),
        (u64::MAX, i64::MAX),
    ] {
        let span = observe(|guard| {
            guard
                .request_byte_counter()
                .unwrap()
                .store(bytes, Ordering::Release);
            guard.record_response(200, 0, "success", None);
            // The accumulator is u64 even on 32-bit hosts; exercise its
            // exporter boundary without allocating an impossible response.
            guard.observation.as_mut().unwrap().response_bytes = bytes;
            guard.complete();
        });
        assert_eq!(
            one(&span.attributes, "http.request.body.size"),
            &Value::I64(expected)
        );
        assert_eq!(
            one(&span.attributes, "http.response.body.size"),
            &Value::I64(expected)
        );
    }
}
