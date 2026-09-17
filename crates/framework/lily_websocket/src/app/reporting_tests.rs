use super::*;
use crate::lifecycle::{EnteredLifecycleLedger, LifecycleExitPath, LifecycleOutcome};

fn completed_ledger() -> crate::reporting::LedgerCounts {
    let mut ledger = EnteredLifecycleLedger::default();
    assert!(ledger.record_entered(1));
    ledger.claim_cleanup().unwrap();
    assert!(ledger.claim_exit(0, LifecycleExitPath::Normal));
    assert!(ledger.start_exit(0, LifecycleExitPath::Normal));
    assert!(ledger.finish_exit(0, LifecycleExitPath::Normal, LifecycleOutcome::Completed));
    crate::reporting::LedgerCounts::observe(&ledger)
}

#[tokio::test]
async fn panicking_diagnostics_cannot_prevent_terminal_result_publication() {
    struct PanickingSubscriber(Arc<AtomicBool>);
    impl tracing::Subscriber for PanickingSubscriber {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {
            self.0.store(true, Ordering::Release);
            panic!("diagnostic subscriber panic probe");
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    let app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
    WsApp::close_never_started(app.runtime_clone())
        .await
        .unwrap();
    let invoked = Arc::new(AtomicBool::new(false));
    tracing::subscriber::with_default(PanickingSubscriber(invoked.clone()), || {
        assert!(app.finish_shutdown(Ok(())).now_or_never().is_some());
    });
    assert!(invoked.load(Ordering::Acquire));
    app.await_terminal().await.unwrap();
    assert_eq!(
        app.lifecycle.shutdown_report.get().unwrap().completion,
        FrameworkShutdownCompletion::GracefulCompleted
    );
}

#[tokio::test]
async fn disconnected_evidence_survives_stage_drop_and_retains_unstarted_siblings() {
    struct PendingHandler;
    impl crate::controller::WebSocketLifecycleAction for PendingHandler {
        fn call(
            &self,
            _: WebSocketLifecycleInvocation,
        ) -> crate::controller::WebSocketLifecycleFuture {
            Box::pin(std::future::pending())
        }
    }
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let scopes = ScopeCleanupRegistry::default();
    let id = Uuid::new_v4();
    let context = Arc::new(WebSocketContext::new(
        id,
        Arc::new(ConnectionManager::new()),
        "orders".into(),
    ));
    let mut armed = DisconnectLedger::default();
    armed.arm(Arc::new(PendingHandler), Duration::from_secs(1));
    armed.arm(Arc::new(PendingHandler), Duration::from_secs(1));
    let ledger = Arc::new(StdMutex::new(armed));
    let mut future = Box::pin(WsApp::run_websocket_disconnect_ledger(
        id,
        container.clone(),
        container.services(),
        context,
        ledger.clone(),
        crate::middleware::WsConnectionCloseCategory::ServerShutdown,
        CancellationToken::new(),
        scopes.clone(),
    ));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    drop(future);
    let counts = ledger.lock().unwrap().accounting();
    assert!(counts.reconciles());
    assert_eq!(counts.total, 2);
    assert_eq!(counts.completed, 0);
    assert_eq!(counts.cancelled, 1);
    assert_eq!(counts.outstanding, 1);
    assert_eq!(counts.not_started, 1);
    assert_eq!(counts.started_incomplete, 1);
    assert_eq!(ledger.lock().unwrap().obligations(), 2);
    assert_eq!(scopes.drain().await.outstanding, 0);
    container.close().await.unwrap();
}

#[tokio::test]
async fn empty_shutdown_report_replays_once_and_preserves_container_ownership() {
    for caller_owned in [false, true] {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let builder = WsAppBuilder::new("127.0.0.1:0");
        let builder = if caller_owned {
            builder.container(container.clone())
        } else {
            builder
        };
        let app = builder.build().await.unwrap();
        assert!(app.lifecycle.shutdown_report.get().is_none());
        let (first, second) = tokio::join!(app.close(), app.close());
        first.unwrap();
        second.unwrap();
        let report = app.lifecycle.shutdown_report.get().unwrap().clone();
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::GracefulCompleted
        );
        assert!(report.evidence.quiescent());
        assert!(report.evidence.reconciles());
        assert_eq!(report.evidence.messages.owners, 0);
        assert_eq!(report.evidence.connections.owners, 0);
        assert_eq!(
            report.evidence.container,
            if caller_owned {
                DependencyState::NotOwned
            } else {
                DependencyState::Completed
            }
        );
        assert!(app.lifecycle.root_join_observed.load(Ordering::Acquire));
        app.close().await.unwrap();
        assert_eq!(&report, app.lifecycle.shutdown_report.get().unwrap());
        container.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_message_joins_retain_accounting_after_receivers_and_entries_disappear() {
    let registry = MessageDispatchRegistry::default();
    let release = Arc::new(tokio::sync::Barrier::new(33));
    for _ in 0..32 {
        let release = release.clone();
        drop(registry.spawn_owner(Uuid::new_v4(), |slot| async move {
            let observation = slot.cleanup_observation();
            let _ = slot
                .run(async {
                    release.wait().await;
                })
                .await;
            observation.record(completed_ledger());
        }));
    }
    assert_eq!(registry.accounting().0.owners, 32);
    release.wait().await;
    assert!(registry.reconcile().await);
    let (counts, drivers) = registry.accounting();
    assert_eq!(counts.owners, 32);
    assert_eq!(counts.joined, 32);
    assert_eq!(counts.outstanding, 0);
    assert_eq!(counts.execution.completed, 32);
    assert_eq!(counts.middleware.normal.completed, 32);
    assert_eq!(counts.middleware.entered, 32);
    assert_eq!(counts.ledgers_unobserved, 0);
    assert_eq!(counts.output.total, 32);
    assert_eq!(
        counts.output.not_prepared, 32,
        "test owners returned no routed message decision"
    );
    assert_eq!(counts.output.outstanding, 0);
    assert_eq!(counts.pipeline.unobserved, 32);
    assert!(counts.reconciles());
    assert_eq!(drivers.outstanding, 0);
    for _ in 0..3 {
        assert!(registry.reconcile().await);
        assert_eq!(registry.accounting(), (counts, drivers));
    }
}

#[test]
fn inconsistent_message_output_and_cooperative_counts_cannot_reconcile() {
    use crate::app::message_reporting::{OutputCounts, PipelineCounts};
    let messages = ownership::MessageAccounting {
        owners: 1,
        joined: 1,
        execution: crate::reporting::InvocationCounts {
            total: 1,
            completed: 1,
            cancellation_requested: 1,
            ..Default::default()
        },
        deadline_exceeded: 1,
        completed_after_cancellation: 1,
        completed_after_deadline: 1,
        timeout_cancellation: 1,
        pipeline: PipelineCounts {
            handled: 1,
            ..Default::default()
        },
        output: OutputCounts {
            total: 1,
            prepared_frames: 1,
            attempts: 1,
            queued_frames: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(messages.reconciles());
    let mut invalid = messages;
    invalid.output.attempts = 0;
    assert!(
        !invalid.reconciles(),
        "queued is impossible without an attempt"
    );
    let mut invalid = messages;
    invalid.output.prepared_frames = 0;
    assert!(
        !invalid.reconciles(),
        "queued is impossible without a prepared frame"
    );
    let mut invalid = messages;
    invalid.completed_after_cancellation = 0;
    assert!(
        !invalid.reconciles(),
        "completed after deadline must retain cancellation evidence"
    );
    let mut invalid = messages;
    invalid.connection_cancellation = 1;
    assert!(
        !invalid.reconciles(),
        "one message cannot have two first cancellation causes"
    );
    let mut invalid = messages;
    invalid.pipeline.rejected = 1;
    assert!(
        !invalid.reconciles(),
        "one pipeline cannot return two results"
    );
}

#[tokio::test]
async fn task_abort_requests_and_confirmed_joins_are_distinct_in_aggregate_evidence() {
    let app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
    let receipt = app
        .lifecycle
        .connection_tasks
        .track(tokio::spawn(std::future::pending::<()>()));
    receipt.abort();
    let evidence = app.shutdown_evidence();
    let tasks = evidence
        .tasks
        .iter()
        .find(|(name, _)| *name == "connection")
        .unwrap()
        .1;
    assert_eq!(tasks.abort_requested, 1);
    assert_eq!(tasks.cancelled, 0);
    assert_eq!(tasks.outstanding, 1);
    assert!(!evidence.quiescent());
    assert!(receipt.await.unwrap_err().is_cancelled());
    let tasks = app.lifecycle.connection_tasks.snapshot();
    assert_eq!(tasks.cancelled, 1);
    assert_eq!(tasks.abort_requested, 1);
    app.close().await.unwrap();
    assert!(
        app.lifecycle
            .shutdown_report
            .get()
            .unwrap()
            .evidence
            .quiescent()
    );
}

#[tokio::test(start_paused = true)]
async fn expired_root_keeps_an_immutable_incomplete_report_after_a_late_owner_join() {
    let mut app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
    app.shutdown_timeout = Duration::from_millis(100);
    let (release, released) = tokio::sync::oneshot::channel();
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let output = app
        .message_dispatch_registry
        .spawn_owner(Uuid::new_v4(), |slot| async move {
            let observation = slot.cleanup_observation();
            let _ = slot.run(async {}).await;
            started.send(()).unwrap();
            let _ = released.await;
            observation.record(completed_ledger());
        });
    started_rx.await.unwrap();
    let start = Instant::now();
    let error = app.close().await.unwrap_err().to_string();
    assert!(start.elapsed() <= Duration::from_millis(100));
    let report = app.lifecycle.shutdown_report.get().unwrap().clone();
    assert_eq!(report.completion, FrameworkShutdownCompletion::Incomplete);
    assert_eq!(report.evidence.messages.outstanding, 1);
    assert!(!report.evidence.quiescent());
    assert_eq!(report.evidence.backplane, DependencyState::NotStarted);
    assert_eq!(report.evidence.container, DependencyState::NotStarted);
    assert!(app.lifecycle.root_join_observed.load(Ordering::Acquire));
    release.send(()).unwrap();
    output.await.unwrap();
    for _ in 0..10 {
        if app.message_dispatch_registry.is_terminal() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(app.message_dispatch_registry.is_terminal());
    assert_eq!(app.shutdown_evidence().messages.outstanding, 0);
    assert_eq!(app.close().await.unwrap_err().to_string(), error);
    assert_eq!(app.lifecycle.shutdown_report.get().unwrap(), &report);
    app.container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn joined_owner_with_unpublished_output_blocks_dependencies_and_freezes_incomplete_report() {
    let mut app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
    app.shutdown_timeout = Duration::from_millis(100);
    let prepared = app
        .message_dispatch_registry
        .spawn_owner(Uuid::new_v4(), |slot| async move {
            let observation = slot.cleanup_observation();
            assert!(matches!(
                slot.run(async {}).await,
                ExecutionExit::Completed(())
            ));
            observation.pipeline_result(Some(WsMessageOutcome::Handled));
            observation.record(Default::default());
            let terminal = Some(PreparedWebSocketTerminal::ApplicationFrame(Message::Text(
                "prepared".into(),
            )));
            let output = observation.output.unwrap();
            output.prepared(&terminal);
            ScopedWebSocketDispatch {
                outcome: WsMessageOutcome::Handled,
                terminal,
                output: Some(output),
            }
        })
        .await
        .unwrap();
    let start = Instant::now();
    let error = app.close().await.unwrap_err().to_string();
    assert!(start.elapsed() <= Duration::from_millis(100));
    let report = app.lifecycle.shutdown_report.get().unwrap().clone();
    assert_eq!(report.completion, FrameworkShutdownCompletion::Incomplete);
    assert_eq!(report.evidence.messages.joined, 1);
    assert_eq!(report.evidence.messages.pipeline.handled, 1);
    assert_eq!(report.evidence.messages.output.outstanding, 1);
    assert_eq!(report.evidence.messages.output.queued_frames, 0);
    assert_eq!(report.evidence.container, DependencyState::NotStarted);
    assert!(report.evidence.reconciles());
    assert!(!report.evidence.quiescent());
    drop(prepared);
    assert_eq!(app.shutdown_evidence().messages.output.suppressed, 1);
    assert_eq!(app.shutdown_evidence().messages.output.outstanding, 0);
    assert_eq!(app.close().await.unwrap_err().to_string(), error);
    assert_eq!(app.lifecycle.shutdown_report.get().unwrap(), &report);
    app.container.close().await.unwrap();
}
