pub(super) use lily_trace::__private::TraceContextSnapshot as ScopeTrace;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ScopeManager;
    use lily_process::ProcessContext;
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use std::{
        any::{Any, TypeId},
        sync::Arc,
    };
    use std::{future::Future, pin::Pin};
    use tracing::Dispatch;
    use tracing_subscriber::prelude::*;

    fn subscriber() -> (SdkTracerProvider, InMemorySpanExporter, Dispatch) {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let dispatch =
            Dispatch::new(tracing_subscriber::registry().with(
                tracing_opentelemetry::layer().with_tracer(provider.tracer("scope-owner-test")),
            ));
        (provider, exporter, dispatch)
    }

    struct DropProbe;
    impl Drop for DropProbe {
        fn drop(&mut self) {
            tracing::info!(probe = "future_dropped");
        }
    }

    #[test]
    fn cleanup_without_an_active_owner_gets_a_valid_sdk_root() {
        let (provider, exports, dispatch) = subscriber();
        let trace = tracing::dispatcher::with_default(&dispatch, ScopeTrace::capture);
        drop(trace.bind(
            async {},
            || tracing::info_span!(parent: None, "di.scope.dispose"),
        ));
        let spans = exports.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert!(spans[0].span_context.is_valid());
        assert_eq!(
            spans[0].parent_span_id,
            opentelemetry::trace::SpanId::INVALID
        );
        provider.shutdown().unwrap();
    }

    #[test]
    fn unpolled_cleanup_drop_restores_original_dispatcher_without_retaining_owner() {
        let (provider, exports, dispatch) = subscriber();
        let owner = tracing::dispatcher::with_default(
            &dispatch,
            || tracing::info_span!(parent: None, "owner"),
        );
        let identity = lily_trace::context_for_span(&owner)
            .span()
            .span_context()
            .clone();
        let trace =
            tracing::dispatcher::with_default(&dispatch, || owner.in_scope(ScopeTrace::capture));
        drop(owner);
        assert_eq!(exports.get_finished_spans().unwrap().len(), 1);
        let probe = DropProbe;
        let future = trace.bind(
            async move {
                let _probe = probe;
                std::future::pending::<()>().await;
            },
            || tracing::info_span!(parent: None, "di.scope.dispose"),
        );
        let alien = Dispatch::new(tracing_subscriber::registry());
        tracing::dispatcher::with_default(&alien, || drop(future));
        let spans = exports.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2);
        let cleanup = spans
            .iter()
            .find(|span| span.name == "di.scope.dispose")
            .unwrap();
        assert_eq!(cleanup.span_context.trace_id(), identity.trace_id());
        assert_eq!(cleanup.parent_span_id, identity.span_id());
        assert_eq!(
            cleanup.events.len(),
            1,
            "unpolled future destructor used the owner's subscriber"
        );
        assert!(
            cleanup.events[0]
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == "probe" && kv.value.as_str() == "future_dropped")
        );
        provider.shutdown().unwrap();
    }

    fn dispose_context(
        instance: Arc<dyn Any + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<(), lily_error::injection::InjectionError>> + Send>>
    {
        Box::pin(async move {
            let expected = instance.downcast::<ProcessContext>().unwrap();
            let current = ProcessContext::current().expect("forced cleanup retains ProcessContext");
            assert_eq!(current.process_id, expected.process_id);
            assert_eq!(current.metadata, expected.metadata);
            tracing::info!(probe = "forced_disposal");
            Ok(())
        })
    }

    #[tokio::test]
    async fn forced_manager_cleanup_and_unpolled_abort_keep_generation_identity() {
        let (provider, exports, dispatch) = subscriber();
        let manager = ScopeManager::new();
        let mut expected = Vec::new();
        for id in [8101, 8102] {
            let owner = tracing::dispatcher::with_default(
                &dispatch,
                || tracing::info_span!(parent: None, "owner"),
            );
            expected.push(
                lily_trace::context_for_span(&owner)
                    .span()
                    .span_context()
                    .clone(),
            );
            let context = ProcessContext::with_process_id(id)
                .with_metadata("marker".into(), format!("scope-{id}"));
            let scope = tracing::dispatcher::with_default(&dispatch, || {
                owner.in_scope(|| manager.create_application_scope(context.clone()).unwrap())
            });
            scope.write().unwrap().cache_any(
                TypeId::of::<ProcessContext>(),
                "test-context",
                Arc::new(context),
                Some(dispose_context),
            );
            drop(owner);
        }
        assert_eq!(
            exports.get_finished_spans().unwrap().len(),
            2,
            "open scopes do not delay owner export"
        );
        manager.stop_accepting_scopes();
        assert_eq!(manager.begin_cleanup_all_scopes(), 2);
        manager.drain_cleanup_tasks().await.unwrap();
        assert_eq!(manager.active_scope_count(), 0);
        assert_eq!(manager.cleanup_task_count(), 0);
        let spans = exports.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 4);
        for identity in expected {
            let matching: Vec<_> = spans
                .iter()
                .filter(|span| {
                    span.name == "di.scope.dispose"
                        && span.span_context.trace_id() == identity.trace_id()
                })
                .collect();
            assert_eq!(matching.len(), 1);
            assert_eq!(matching[0].parent_span_id, identity.span_id());
            assert_eq!(matching[0].events.len(), 1);
        }

        let manager = ScopeManager::new();
        let owner = tracing::dispatcher::with_default(
            &dispatch,
            || tracing::info_span!(parent: None, "aborted_owner"),
        );
        let identity = lily_trace::context_for_span(&owner)
            .span()
            .span_context()
            .clone();
        tracing::dispatcher::with_default(&dispatch, || {
            owner.in_scope(|| {
                manager
                    .create_application_scope(ProcessContext::with_process_id(8103))
                    .unwrap()
            })
        });
        drop(owner);
        // Current-thread runtime and no await: abort is registered before the
        // cleanup task gets its first poll. Awaiting the tracker proves release.
        drop(manager.begin_scope_cleanup("8103", None).unwrap());
        assert_eq!(manager.abort_cleanup_tasks(), 1);
        assert!(manager.drain_cleanup_tasks().await.is_err());
        assert_eq!(manager.cleanup_task_count(), 0);
        assert_eq!(manager.active_scope_count(), 0);
        let spans = exports.get_finished_spans().unwrap();
        let matching: Vec<_> = spans
            .iter()
            .filter(|span| {
                span.name == "di.scope.dispose"
                    && span.span_context.trace_id() == identity.trace_id()
            })
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].parent_span_id, identity.span_id());
        assert!(matching[0].events.is_empty());
        provider.shutdown().unwrap();
    }
}
