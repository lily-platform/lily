use super::*;
use crate::shutdown::ShutdownBudget;
use crate::{CleanupCancellation, ExecutionCancellation};
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;

fn manager() -> Arc<ConnectionManager> {
    Arc::new(ConnectionManager::with_registered_namespaces(
        8,
        128,
        ["orders".to_owned()],
    ))
}

fn rejected(result: Result<WebSocketDispatchReceipt, ConnectionError>) {
    assert!(matches!(
        result,
        Err(ConnectionError::InvalidOperation(
            crate::connection::ConnectionOperationError::DispatcherNotAccepting
        ))
    ));
}

async fn target(manager: &ConnectionManager, capacity: usize) -> (Uuid, mpsc::Receiver<Message>) {
    let id = Uuid::new_v4();
    let (tx, rx) = mpsc::channel(capacity);
    manager
        .add_connection(id, tx, None, Some("orders".into()))
        .await
        .unwrap();
    (id, rx)
}

#[tokio::test]
#[allow(
    clippy::async_yields_async,
    reason = "qualification deliberately returns an unpolled send beyond its invocation"
)]
async fn drain_continuation_is_poll_scoped_app_specific_and_not_inherited_by_spawn() {
    let manager = manager();
    let (id, mut rx) = target(&manager, 8).await;
    let dispatcher = Arc::new(WebSocketDispatcher::local(manager.clone()));
    let other_app = WebSocketDispatcher::local(manager.clone());
    let context = crate::WebSocketContext::new_with_dispatcher(
        id,
        manager,
        dispatcher.clone(),
        "orders".into(),
    );
    let retained_proxy = context.clients().caller();
    dispatcher.begin_drain();
    other_app.begin_drain();
    rejected(
        dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await,
    );
    dispatcher
        .execution_scope(
            ExecutionCancellation::new(CancellationToken::new()),
            ShutdownBudget::default(),
            TokioInstant::now() + Duration::from_secs(1),
            async {
                retained_proxy
                    .send("orders:created", json!({}))
                    .await
                    .unwrap();
                rejected(
                    other_app
                        .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
                        .await,
                );
                let escaped_proxy = context.clients().caller();
                assert!(matches!(
                    tokio::spawn(
                        async move { escaped_proxy.send("orders:created", json!({})).await }
                    )
                    .await
                    .unwrap(),
                    Err(ConnectionError::InvalidOperation(
                        crate::connection::ConnectionOperationError::DispatcherNotAccepting
                    ))
                ));
            },
        )
        .await;
    assert!(rx.try_recv().is_ok());
    assert!(rx.try_recv().is_err());
    assert!(
        retained_proxy
            .send("orders:created", json!({}))
            .await
            .is_err()
    );
    // Merely constructing a send future in a callback grants no later permit.
    let future = dispatcher
        .execution_scope(
            ExecutionCancellation::new(CancellationToken::new()),
            ShutdownBudget::default(),
            TokioInstant::now() + Duration::from_secs(1),
            async { dispatcher.dispatch(message(BroadcastTarget::Namespace("orders".to_owned()))) },
        )
        .await;
    rejected(future.await);
    dispatcher.wait_drained().await;
    assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 0);
}

#[tokio::test(start_paused = true)]
async fn force_stops_expired_execution_but_cleanup_uses_its_own_signal_and_deadline() {
    let manager = manager();
    let (caller, mut caller_rx) = target(&manager, 8).await;
    let (other, mut other_rx) = target(&manager, 8).await;
    let (backplane, _events) = RecordingBackplane::channel();
    let dispatcher = active_dispatcher(manager.clone(), backplane.clone());
    dispatcher.force_drain();
    manager.mark_closing(caller).await.unwrap();
    let deadline = TokioInstant::now() + Duration::from_secs(1);
    let signal = ExecutionCancellation::for_message(CancellationToken::new(), Default::default());
    signal.set_message_deadline(TokioInstant::now() - Duration::from_millis(250));
    dispatcher
        .execution_scope(signal, Default::default(), deadline, async {
            rejected(
                dispatcher
                    .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
                    .await,
            );
        })
        .await;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    dispatcher
        .cleanup_scope(
            CleanupCancellation::new(cancelled),
            Default::default(),
            deadline,
            async {
                rejected(
                    dispatcher
                        .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
                        .await,
                );
            },
        )
        .await;
    let live_cleanup = CancellationToken::new();
    let receipt = dispatcher
        .cleanup_scope(
            CleanupCancellation::new(live_cleanup),
            Default::default(),
            deadline,
            async {
                dispatcher
                    .dispatch(message(BroadcastTarget::NamespaceConnections {
                        namespace: "orders".to_owned(),
                        connection_ids: vec![caller, other],
                    }))
                    .await
                    .unwrap()
            },
        )
        .await;
    assert_eq!(receipt.local().sent, 1);
    assert_eq!(receipt.local().closed, 1);
    assert!(!receipt.is_fully_accepted());
    assert!(caller_rx.try_recv().is_err());
    assert!(other_rx.try_recv().is_ok());
    assert_eq!(backplane.published.lock().await.len(), 1);
    // Expiry is effective even for an immediately ready provider.
    dispatcher
        .cleanup_scope(
            CleanupCancellation::new(CancellationToken::new()),
            Default::default(),
            TokioInstant::now(),
            async {
                rejected(
                    dispatcher
                        .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
                        .await,
                );
            },
        )
        .await;
    assert_eq!(backplane.published.lock().await.len(), 1);
    dispatcher.close_backplane().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn accepted_message_can_publish_after_cancellation_until_its_shared_cutoff() {
    let manager = manager();
    let (_, mut rx) = target(&manager, 8).await;
    let (backplane, _events) = RecordingBackplane::channel();
    let dispatcher = active_dispatcher(manager, backplane.clone());
    let budget = ShutdownBudget::default();
    let signal = ExecutionCancellation::for_message(CancellationToken::new(), budget.clone());
    let deadline = TokioInstant::now() + Duration::from_millis(100);
    signal.set_message_deadline(deadline);
    signal.cancelled().await;
    dispatcher.force_drain();
    budget.force_before(TokioInstant::now() + Duration::from_millis(400));
    dispatcher
        .execution_scope(signal.clone(), budget, deadline, async {
            let receipt = dispatcher
                .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
                .await
                .unwrap();
            assert!(receipt.is_fully_accepted());
            assert!(rx.try_recv().is_ok());
            assert_eq!(backplane.published.lock().await.len(), 1);
            signal.execution_stopped(Some(deadline)).await;
            rejected(
                dispatcher
                    .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
                    .await,
            );
            assert!(rx.try_recv().is_err());
            assert_eq!(backplane.published.lock().await.len(), 1);
        })
        .await;
    dispatcher.wait_drained().await;
    assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 0);
    dispatcher.close_backplane().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn local_partial_acceptance_interrupted_by_force_releases_pending_admissions() {
    let manager = manager();
    let (fast, mut fast_rx) = target(&manager, 2).await;
    let (slow, mut slow_rx) = target(&manager, 1).await;
    manager
        .broadcast(message(BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: vec![slow],
        }))
        .await
        .unwrap();
    let (backplane, _events) = RecordingBackplane::channel();
    let dispatcher = active_dispatcher(manager.clone(), backplane.clone());
    let mut sending = Box::pin(dispatcher.dispatch(message(
        BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: vec![fast, slow],
        },
    )));
    assert!(futures_util::poll!(sending.as_mut()).is_pending());
    assert!(
        fast_rx.try_recv().is_ok(),
        "one recipient accepted before cancellation"
    );
    dispatcher.force_drain();
    assert!(matches!(
        sending.await,
        Err(ConnectionError::DispatchInterrupted)
    ));
    assert!(
        backplane.published.lock().await.is_empty(),
        "remote publication was never reached"
    );
    assert!(slow_rx.try_recv().is_ok());
    assert!(slow_rx.try_recv().is_err());
    // No detached admission remains and reservations were released.
    let report = manager
        .broadcast(message(BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: vec![slow],
        }))
        .await
        .unwrap();
    assert_eq!(report.sent, 1);
    dispatcher.wait_drained().await;
    assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 0);
    dispatcher.close_backplane().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cleanup_publish_obeys_shortened_root_budget_and_retains_completed_local_report() {
    let manager = manager();
    let (id, mut rx) = target(&manager, 4).await;
    let backplane = GatedBackplane::new();
    let dispatcher = WebSocketDispatcher::active(
        manager,
        BackplaneRequirement::Required,
        backplane.clone(),
        Duration::from_secs(10),
        test_health(BackplaneRequirement::Required),
    );
    dispatcher.force_drain();
    let budget = ShutdownBudget::default();
    let mut sending = Box::pin(dispatcher.cleanup_scope(
        CleanupCancellation::new(CancellationToken::new()),
        budget.clone(),
        TokioInstant::now() + Duration::from_secs(60),
        dispatcher.dispatch(message(BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: vec![id],
        })),
    ));
    assert!(futures_util::poll!(sending.as_mut()).is_pending());
    assert!(rx.try_recv().is_ok());
    let now = TokioInstant::now();
    budget.force_before(now + Duration::from_millis(100));
    let error = sending.await.unwrap_err();
    let ConnectionError::BackplanePublish { local, source } = error else {
        panic!("must retain the completed local report");
    };
    assert_eq!(local.sent, 1);
    assert_eq!(source.kind(), WebSocketBackplaneErrorKind::TimedOut);
    assert!(TokioInstant::now() <= now + Duration::from_millis(100));
    assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 0);
    backplane.close_release.notify_one();
    dispatcher.close_backplane().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn external_dispatch_uses_root_deadline_and_never_restarts_the_budget() {
    let manager = manager();
    let (id, mut rx) = target(&manager, 1).await;
    manager
        .broadcast(message(BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: vec![id],
        }))
        .await
        .unwrap();
    let dispatcher = WebSocketDispatcher::local(manager);
    let start = TokioInstant::now();
    dispatcher.set_shutdown_deadlines(
        start + Duration::from_millis(20),
        start + Duration::from_millis(40),
    );
    let result = dispatcher
        .dispatch(message(BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: vec![id],
        }))
        .await;
    assert!(matches!(result, Err(ConnectionError::DispatchInterrupted)));
    assert!(TokioInstant::now() <= start + Duration::from_millis(40));
    dispatcher.set_shutdown_deadlines(
        start + Duration::from_secs(1),
        start + Duration::from_secs(2),
    );
    rejected(
        dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await,
    );
    assert!(rx.try_recv().is_ok());
    assert!(rx.try_recv().is_err());
    dispatcher.wait_drained().await;
}

#[tokio::test]
async fn dependency_close_seals_new_cleanup_sends_before_waiting_for_existing_publish() {
    let backplane = GatedBackplane::new();
    let dispatcher = WebSocketDispatcher::active(
        manager(),
        BackplaneRequirement::Required,
        backplane.clone(),
        Duration::from_secs(1),
        test_health(BackplaneRequirement::Required),
    );
    let mut sending =
        Box::pin(dispatcher.dispatch(message(BroadcastTarget::Namespace("orders".to_owned()))));
    assert!(futures_util::poll!(sending.as_mut()).is_pending());
    let closing_dispatcher = dispatcher.clone();
    let closing = tokio::spawn(async move { closing_dispatcher.close_backplane().await });
    timeout(Duration::from_secs(1), async {
        while dispatcher.0.dispatch_state.load(Ordering::Acquire) < DISPATCHER_CLOSING {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(backplane.close_calls.load(Ordering::Acquire), 0);
    dispatcher
        .cleanup_scope(
            CleanupCancellation::new(CancellationToken::new()),
            Default::default(),
            TokioInstant::now() + Duration::from_secs(1),
            async {
                rejected(
                    dispatcher
                        .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
                        .await,
                );
            },
        )
        .await;
    backplane.publish_release.notify_one();
    sending.await.unwrap();
    timeout(Duration::from_secs(1), backplane.close_started.notified())
        .await
        .unwrap();
    backplane.close_release.notify_one();
    closing.await.unwrap().unwrap();
    assert_eq!(backplane.close_calls.load(Ordering::Acquire), 1);
    assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn panic_or_drop_restores_task_local_authority_without_affecting_the_next_cleanup() {
    let manager = manager();
    let (_id, mut rx) = target(&manager, 2).await;
    let dispatcher = WebSocketDispatcher::local(manager);
    dispatcher.begin_drain();
    let deadline = TokioInstant::now() + Duration::from_secs(1);
    let result = AssertUnwindSafe(dispatcher.execution_scope(
        ExecutionCancellation::new(CancellationToken::new()),
        Default::default(),
        deadline,
        async {
            panic!("intentional invocation panic");
        },
    ))
    .catch_unwind()
    .await;
    assert!(result.is_err());
    let mut pending = Box::pin(dispatcher.execution_scope(
        ExecutionCancellation::new(CancellationToken::new()),
        Default::default(),
        deadline,
        std::future::pending::<()>(),
    ));
    assert!(futures_util::poll!(pending.as_mut()).is_pending());
    drop(pending);
    rejected(
        dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await,
    );
    let receipt = dispatcher
        .cleanup_scope(
            CleanupCancellation::new(CancellationToken::new()),
            Default::default(),
            deadline,
            dispatcher.dispatch(message(BroadcastTarget::Namespace("orders".to_owned()))),
        )
        .await
        .unwrap();
    assert_eq!(receipt.local().sent, 1);
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
#[allow(
    clippy::async_yields_async,
    reason = "qualification deliberately escapes a partially polled send to verify revocation"
)]
async fn already_polled_send_cannot_outlive_its_invocation_authority() {
    for cleanup in [false, true] {
        let manager = manager();
        let (id, mut rx) = target(&manager, 4).await;
        let backplane = GatedBackplane::new();
        let dispatcher = WebSocketDispatcher::active(
            manager,
            BackplaneRequirement::Required,
            backplane.clone(),
            Duration::from_secs(10),
            test_health(BackplaneRequirement::Required),
        );
        dispatcher.begin_drain();
        let body = async {
            let mut sending = Box::pin(dispatcher.dispatch(message(
                BroadcastTarget::NamespaceConnections {
                    namespace: "orders".to_owned(),
                    connection_ids: vec![id],
                },
            )));
            assert!(futures_util::poll!(sending.as_mut()).is_pending());
            sending
        };
        let deadline = TokioInstant::now() + Duration::from_secs(60);
        let escaped = if cleanup {
            dispatcher
                .cleanup_scope(
                    CleanupCancellation::new(CancellationToken::new()),
                    Default::default(),
                    deadline,
                    body,
                )
                .await
        } else {
            dispatcher
                .execution_scope(
                    ExecutionCancellation::new(CancellationToken::new()),
                    Default::default(),
                    deadline,
                    body,
                )
                .await
        };
        assert!(rx.try_recv().is_ok());
        let error = timeout(Duration::from_secs(1), escaped)
            .await
            .unwrap()
            .unwrap_err();
        let ConnectionError::BackplanePublish { local, source } = error else {
            panic!("local acceptance must survive invocation revocation");
        };
        assert_eq!(local.sent, 1);
        assert_eq!(source.kind(), WebSocketBackplaneErrorKind::Interrupted);
        dispatcher.wait_drained().await;
        assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 0);
        backplane.close_release.notify_one();
        dispatcher.close_backplane().await.unwrap();
    }
}
