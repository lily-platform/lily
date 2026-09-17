//! Real resolution, span export, and metric collection under INFO/DEBUG/OFF.
use std::{any::type_name, sync::Arc, time::Duration};

use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, InjectionError, ProcessContext, ServiceTrait};
use opentelemetry::{
    Value,
    trace::{SpanId, Status, TraceContextExt, TracerProvider as _},
};
use opentelemetry_sdk::{
    metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData},
    },
    trace::{InMemorySpanExporter, SdkTracerProvider, SpanData},
};
use tracing::{Instrument, instrument::WithSubscriber};
use tracing_subscriber::{filter::LevelFilter, prelude::*};

const SECRET: &str = "resolution-error-and-context-secret-canary";

trait Interface: Send + Sync {}
#[derive(Default, Injectable)]
#[service(lifetime = "Singleton", interface = dyn Interface)]
struct Singleton;
impl Interface for Singleton {}
impl ServiceTrait for Singleton {}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Scoped;
impl ServiceTrait for Scoped {}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct Transient;
impl ServiceTrait for Transient {}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Failing;
#[async_trait::async_trait]
impl ServiceTrait for Failing {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Err(InjectionError::InitError(SECRET.into()))
    }
}
struct Missing;

struct Expected {
    parent: SpanId,
    requested: &'static str,
    implementation: Option<&'static str>,
    lifetime: Option<&'static str>,
    code: Option<&'static str>,
}

async fn resolve<T: ?Sized + Send + Sync + 'static>(
    container: &ApplicationContainer,
    dispatch: &tracing::Dispatch,
    scope: Option<&ProcessContext>,
    implementation: Option<&'static str>,
    lifetime: Option<&'static str>,
    code: Option<&'static str>,
) -> (Result<Arc<T>, InjectionError>, Expected) {
    let parent = tracing::dispatcher::with_default(
        dispatch,
        || tracing::info_span!(parent: None, "test.resolve.owner"),
    );
    let parent_id = lily_trace::context_for_span(&parent)
        .span()
        .span_context()
        .span_id();
    let result = container
        .services()
        .get_service::<T>(scope)
        .instrument(parent)
        .with_subscriber(dispatch.clone())
        .await;
    assert_eq!(result.is_err(), code.is_some());
    (
        result,
        Expected {
            parent: parent_id,
            requested: type_name::<T>(),
            implementation,
            lifetime,
            code,
        },
    )
}

fn value<'a>(attributes: &'a [opentelemetry::KeyValue], key: &str) -> Option<&'a Value> {
    let matches: Vec<_> = attributes
        .iter()
        .filter(|kv| kv.key.as_str() == key)
        .collect();
    assert!(matches.len() <= 1, "duplicate {key}: {attributes:?}");
    matches.first().map(|kv| &kv.value)
}

fn verify(spans: &[SpanData], expected: &[Expected], level: LevelFilter) {
    assert!(!format!("{spans:?}").contains(SECRET));
    let resolutions: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "di.service.resolve")
        .collect();
    assert_eq!(
        resolutions.len(),
        if level == LevelFilter::DEBUG {
            expected.len()
        } else {
            0
        }
    );
    let failures: Vec<_> = spans
        .iter()
        .flat_map(|span| span.events.iter().map(move |event| (span, event)))
        .filter(|(_, event)| event.name == "DI service resolution failed")
        .collect();
    assert_eq!(
        failures.len(),
        if level == LevelFilter::OFF {
            0
        } else {
            expected.iter().filter(|e| e.code.is_some()).count()
        }
    );
    for expected in expected {
        if level == LevelFilter::OFF {
            continue;
        }
        let owner: Vec<_> = spans
            .iter()
            .filter(|span| span.span_context.span_id() == expected.parent)
            .collect();
        assert_eq!(owner.len(), 1);
        let span = if level == LevelFilter::DEBUG {
            let matching: Vec<_> = resolutions
                .iter()
                .filter(|span| span.parent_span_id == expected.parent)
                .collect();
            assert_eq!(
                matching.len(),
                1,
                "one invocation under each independent owner"
            );
            let span = *matching[0];
            assert_eq!(
                span.span_context.trace_id(),
                owner[0].span_context.trace_id()
            );
            for (key, expected) in [
                ("di.requested_type", Some(expected.requested)),
                ("di.implementation_type", expected.implementation),
                ("di.lifetime", expected.lifetime),
                ("lily.lifecycle", Some("completed")),
                (
                    "lily.outcome",
                    Some(if expected.code.is_some() {
                        "error"
                    } else {
                        "success"
                    }),
                ),
                ("lily.error_code", expected.code),
            ] {
                assert_eq!(
                    value(&span.attributes, key).map(|v| v.as_str()),
                    expected.map(Into::into),
                    "{key}"
                );
            }
            assert!(
                matches!(value(&span.attributes, "lily.duration_ms"), Some(Value::F64(v)) if v.is_finite() && *v >= 0.0)
            );
            assert_eq!(
                matches!(span.status, Status::Error { .. }),
                expected.code.is_some()
            );
            for phase in ["started", "completed"] {
                let events: Vec<_> = span
                    .events
                    .iter()
                    .filter(|event| {
                        value(&event.attributes, "lily.lifecycle")
                            .is_some_and(|v| v.as_str() == phase)
                    })
                    .collect();
                assert_eq!(events.len(), 1);
                assert_eq!(
                    value(&events[0].attributes, "level").unwrap().as_str(),
                    "DEBUG"
                );
            }
            span
        } else {
            owner[0]
        };
        let errors: Vec<_> = failures
            .iter()
            .filter(|(parent, _)| parent.span_context.span_id() == span.span_context.span_id())
            .collect();
        assert_eq!(errors.len(), usize::from(expected.code.is_some()));
        if let Some(code) = expected.code {
            let attrs = &errors[0].1.attributes;
            for (key, expected) in [
                ("lily.error_code", Some(code)),
                ("level", Some("ERROR")),
                ("di.requested_type", Some(expected.requested)),
                ("di.implementation_type", expected.implementation),
                ("di.lifetime", expected.lifetime),
            ] {
                assert_eq!(
                    value(attrs, key).map(|v| v.as_str()),
                    expected.map(Into::into),
                    "{key}"
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolution_filtering_preserves_failures_metadata_identity_and_metrics() {
    for level in [LevelFilter::INFO, LevelFilter::DEBUG, LevelFilter::OFF] {
        let exports = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exports.clone())
            .build();
        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(level).with(
                tracing_opentelemetry::layer().with_tracer(provider.tracer("di-resolution")),
            ));
        let metrics = InMemoryMetricExporter::default();
        let meter = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(metrics.clone())
                    .with_interval(Duration::from_secs(3600))
                    .build(),
            )
            .build();
        opentelemetry::global::set_meter_provider(meter.clone());
        let container = ApplicationContainer::builder().build().await.unwrap();
        let context =
            ProcessContext::with_process_id(3100).with_metadata("private".into(), SECRET.into());
        let mut scope = container.create_scope(context.clone()).unwrap();
        let mut expected = Vec::new();
        let (concrete, event) = resolve::<Singleton>(
            &container,
            &dispatch,
            None,
            Some(type_name::<Singleton>()),
            Some("singleton"),
            None,
        )
        .await;
        expected.push(event);
        let (interface, event) = resolve::<dyn Interface>(
            &container,
            &dispatch,
            None,
            Some(type_name::<Singleton>()),
            Some("singleton"),
            None,
        )
        .await;
        expected.push(event);
        assert_eq!(
            Arc::as_ptr(&concrete.unwrap()) as *const (),
            Arc::as_ptr(&interface.unwrap()) as *const ()
        );
        let (_, event) = resolve::<Scoped>(
            &container,
            &dispatch,
            Some(&context),
            Some(type_name::<Scoped>()),
            Some("scoped"),
            None,
        )
        .await;
        expected.push(event);
        // Independent concurrent invocations cannot be paired merely by span name.
        let calls = (0..8).map(|_| {
            resolve::<Transient>(
                &container,
                &dispatch,
                None,
                Some(type_name::<Transient>()),
                Some("transient"),
                None,
            )
        });
        let transients = futures::future::join_all(calls).await;
        let instances: Vec<_> = transients
            .into_iter()
            .map(|(result, event)| {
                expected.push(event);
                result.unwrap()
            })
            .collect();
        for (index, instance) in instances.iter().enumerate() {
            assert!(
                instances[..index]
                    .iter()
                    .all(|prior| !Arc::ptr_eq(prior, instance))
            );
        }
        let (result, event) = resolve::<Scoped>(
            &container,
            &dispatch,
            None,
            Some(type_name::<Scoped>()),
            Some("scoped"),
            Some("di.scope_required"),
        )
        .await;
        assert!(matches!(result, Err(InjectionError::ScopeRequired { .. })));
        expected.push(event);
        let (result, event) = resolve::<Missing>(
            &container,
            &dispatch,
            None,
            None,
            None,
            Some("di.service_not_found"),
        )
        .await;
        assert!(matches!(result, Err(InjectionError::ServiceNotFound(_))));
        expected.push(event);
        let (result, event) = resolve::<Failing>(
            &container,
            &dispatch,
            Some(&context),
            Some(type_name::<Failing>()),
            Some("scoped"),
            Some("di.service_initialization_failed"),
        )
        .await;
        assert!(matches!(
            result,
            Err(InjectionError::ServiceInitializationFailed { .. })
        ));
        expected.push(event);
        scope.close().await.unwrap();
        container.close().await.unwrap();
        let (result, event) = resolve::<Singleton>(
            &container,
            &dispatch,
            None,
            Some(type_name::<Singleton>()),
            Some("singleton"),
            Some("di.container_closed"),
        )
        .await;
        assert!(matches!(result, Err(InjectionError::ContainerClosed)));
        expected.push(event);

        provider.force_flush().unwrap();
        verify(&exports.get_finished_spans().unwrap(), &expected, level);
        meter.force_flush().unwrap();
        let resources = metrics.get_finished_metrics().unwrap();
        let exported: Vec<_> = resources
            .iter()
            .flat_map(|r| r.scope_metrics())
            .flat_map(|s| s.metrics())
            .collect();
        for (name, count) in [
            ("di.service.resolutions.total", 14),
            ("di.service.cache_hits.total", 2),
        ] {
            let metric: Vec<_> = exported.iter().filter(|m| m.name() == name).collect();
            assert_eq!(metric.len(), 1);
            let AggregatedMetrics::U64(MetricData::Sum(data)) = metric[0].data() else {
                panic!("counter type")
            };
            assert_eq!(data.data_points().map(|p| p.value()).sum::<u64>(), count);
            for p in data.data_points() {
                let attributes: Vec<_> = p.attributes().map(|kv| kv.key.as_str()).collect();
                assert_eq!(
                    attributes,
                    ["service_type"],
                    "no new high-cardinality labels"
                );
            }
        }
        let metric: Vec<_> = exported
            .iter()
            .filter(|m| m.name() == "di.service.resolution.duration")
            .collect();
        assert_eq!(metric.len(), 1);
        let AggregatedMetrics::F64(MetricData::Histogram(data)) = metric[0].data() else {
            panic!("duration histogram type")
        };
        // Preserve the established admission/duration semantics: missing route
        // and closed-container early exits did not previously record duration.
        assert_eq!(data.data_points().map(|p| p.count()).sum::<u64>(), 13);
        for p in data.data_points() {
            let mut keys: Vec<_> = p.attributes().map(|kv| kv.key.as_str()).collect();
            keys.sort_unstable();
            assert_eq!(keys, ["service_type", "status"]);
        }
        meter.shutdown().unwrap();
        provider.shutdown().unwrap();
    }
}
