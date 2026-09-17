//! Real SDK assertions over concurrent Upgrade, identity, message and cleanup.
#[path = "support/telemetry_fixture.rs"]
mod fixture;
use fixture::{IDENTITIES, Identity, bounded, exercise, parent_id, trace_id};
use lily_websocket::{ServerConfig, WsAppBuilder};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider};
use std::sync::Arc;
use tracing_subscriber::prelude::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_parent_precedes_identity_and_all_scoped_work() {
    let exports = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
        .with_simple_exporter(exports.clone())
        .build();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("ws-parent-test"))),
    )
    .unwrap();
    lily_trace::install_w3c_propagator();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let app = Arc::new(
        WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allowed_origins: vec!["https://parent.test".into()],
                ..Default::default()
            })
            .identity_middleware::<Identity>()
            .tracing_external()
            .build()
            .await
            .unwrap(),
    );
    let running = app.clone();
    let server = tokio::spawn(async move { running.start().await });
    bounded(async {
        while !app.health_snapshot().unwrap().accepting_new_work {
            assert!(!server.is_finished());
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Distinct concurrent owners detect cross-connection context contamination.
    futures_util::future::join_all((1..=12).map(|case| exercise(address, case))).await;
    bounded(app.close()).await.unwrap();
    bounded(server).await.unwrap().unwrap();
    provider.force_flush().unwrap();
    let spans = exports.get_finished_spans().unwrap();
    let identities = IDENTITIES.lock().unwrap().clone();
    assert_eq!(
        identities.len(),
        9,
        "protocol/origin/ambiguous-header rejects never run identity"
    );
    let connections: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "websocket.connection")
        .collect();
    assert_eq!(
        connections.len(),
        11,
        "one connection per parsed Upgrade except the unsampled trace"
    );
    assert!(connections.iter().all(|span| span.span_context.is_valid()));
    assert_eq!(
        connections
            .iter()
            .filter(|span| span.parent_span_id == opentelemetry::trace::SpanId::INVALID)
            .count(),
        3,
        "missing, malformed and duplicate parents each start an independent root"
    );
    for case in 1..=12 {
        let observed = identities
            .iter()
            .find(|(id, _)| *id == case)
            .map(|(_, cx)| cx);
        let trace = if matches!(case, 3 | 4) {
            let context = observed.unwrap();
            assert!(context.is_valid());
            assert_eq!(context.trace_state().header(), "");
            context.trace_id().to_string()
        } else {
            trace_id(case)
        };
        if case == 5 {
            let identity = observed.unwrap();
            assert_eq!(identity.trace_id().to_string(), trace);
            assert!(!identity.is_sampled());
            // tracing-opentelemetry 0.31's Drop sampling path replaces
            // tracestate with its default. Verify its pinned behavior without
            // mistaking the missing export for propagation failure. The
            // request-boundary test separately verifies incoming tracestate.
            assert_eq!(identity.trace_state().header(), "");
            assert!(
                !spans
                    .iter()
                    .any(|s| s.span_context.trace_id().to_string() == trace),
                "parent-based sampler must not export unsampled descendants"
            );
            continue;
        }
        if case == 9 {
            assert!(observed.is_none());
            assert!(
                !spans
                    .iter()
                    .any(|s| s.span_context.trace_id().to_string() == trace),
                "ambiguous parent must not be adopted"
            );
            continue;
        }
        let owned: Vec<_> = spans
            .iter()
            .filter(|s| s.span_context.trace_id().to_string() == trace)
            .collect();
        let one = |name: &str| {
            let matching: Vec<_> = owned.iter().filter(|span| span.name == name).collect();
            assert_eq!(matching.len(), 1, "case {case}: exactly one {name}");
            *matching[0]
        };
        let connection = one("websocket.connection");
        if matches!(case, 3 | 4) {
            assert_eq!(
                connection.parent_span_id,
                opentelemetry::trace::SpanId::INVALID
            );
        } else {
            assert_eq!(connection.parent_span_id.to_string(), parent_id(case));
        }
        let handshake = one("websocket.handshake");
        assert_eq!(handshake.parent_span_id, connection.span_context.span_id());
        if let Some(identity) = observed {
            assert_eq!(
                identity.trace_id().to_string(),
                trace,
                "identity sees the final ID while executing"
            );
            if !matches!(case, 3 | 4) {
                assert_eq!(identity.trace_state().header(), "vendor=test");
            }
            assert_eq!(
                one("test.ws.identity").parent_span_id,
                handshake.span_context.span_id()
            );
        }
        let accepted = !matches!(case, 6..=8);
        if accepted {
            assert_eq!(
                one("websocket.message").parent_span_id,
                connection.span_context.span_id()
            );
            assert_eq!(
                one("websocket.connection.cleanup").parent_span_id,
                connection.span_context.span_id()
            );
            // Remote identity is stable even across detached disconnect callbacks.
            assert_eq!(
                owned
                    .iter()
                    .flat_map(|s| s.events.iter())
                    .filter(|e| e.attributes.iter().any(|kv| kv.key.as_str() == "probe"
                        && kv.value.as_str() == "controller_disconnected"))
                    .count(),
                1
            );
        }
        let disposals: Vec<_> = owned
            .iter()
            .filter(|span| span.name == "test.ws.resource.dispose")
            .collect();
        assert_eq!(
            disposals.len(),
            if accepted {
                4
            } else if case == 6 {
                1
            } else {
                0
            },
            "case {case}: exact scoped disposal count"
        );
        for dispose in disposals {
            let cleanup = one_by_id(&spans, dispose.parent_span_id);
            assert_eq!(cleanup.name, "di.scope.dispose");
            let owner = one_by_id(&spans, cleanup.parent_span_id);
            assert_eq!(owner.span_context.trace_id().to_string(), trace);
        }
        // Inspect raw attributes before any map conversion could hide duplicates.
        for span in owned {
            let mut keys = std::collections::HashSet::new();
            for kv in &span.attributes {
                assert!(
                    keys.insert(kv.key.as_str()),
                    "case {case}: duplicate {} on {}",
                    kv.key,
                    span.name
                );
            }
        }
    }
    assert!(!format!("{spans:?}").contains("telemetry-parent-private-canary"));
    assert_eq!(app.active_connection_count().await, 0);
    provider.shutdown().unwrap();
}

fn one_by_id(
    spans: &[opentelemetry_sdk::trace::SpanData],
    id: opentelemetry::trace::SpanId,
) -> &opentelemetry_sdk::trace::SpanData {
    let found: Vec<_> = spans
        .iter()
        .filter(|s| s.span_context.span_id() == id)
        .collect();
    assert_eq!(found.len(), 1, "parent must be exported exactly once");
    found[0]
}
