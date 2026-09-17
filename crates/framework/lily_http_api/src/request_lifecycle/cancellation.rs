use super::*;

fn termination(context: &RequestExecutionContext) -> Option<SlotTermination> {
    lock(&context.0.evidence).execution.termination()
}

#[tokio::test(start_paused = true)]
async fn admitted_deadline_includes_time_before_dispatch_and_preserves_a_returned_error() {
    let (app, _) = application(true).await;
    let runtime = app.clone();
    let waiter = app
        .request_registry()
        .spawn(app.clone(), move |owner| async move {
            owner
                .admit(
                    &runtime,
                    Arc::new(Semaphore::new(1)),
                    Duration::from_millis(100),
                )
                .unwrap();
            let admitted_deadline = owner.execution_deadline();
            tokio::time::sleep(Duration::from_millis(40)).await;
            owner
                .execute(async {
                    owner.cancellation().cancelled().await;
                    assert_eq!(Instant::now(), admitted_deadline);
                    Err::<(), _>(HttpApiError::Conflict(
                        "cooperative application error".into(),
                    ))
                })
                .await
        })
        .unwrap();
    let context = waiter.context.clone();
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Ok(Err(HttpApiError::Conflict(message))) if message == "cooperative application error"
    ));
    assert_eq!(Instant::now(), context.execution_deadline());
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::RequestTimeout)
    );
    assert_eq!(termination(&context), Some(SlotTermination::ReturnedError));
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn an_expired_admitted_deadline_cannot_restart_or_first_poll_execution() {
    let (app, _) = application(true).await;
    let runtime = app.clone();
    let waiter = app
        .request_registry()
        .spawn(app.clone(), move |owner| async move {
            owner
                .admit(
                    &runtime,
                    Arc::new(Semaphore::new(1)),
                    Duration::from_millis(100),
                )
                .unwrap();
            tokio::time::sleep(Duration::from_millis(101)).await;
            owner
                .execute::<()>(async {
                    panic!("expired admission must not first-enter user work");
                })
                .await
        })
        .unwrap();
    let context = waiter.context.clone();
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::TimedOut)
    ));
    assert!(!lock(&context.0.evidence).execution.started());
    assert_eq!(termination(&context), Some(SlotTermination::Dropped));
    assert_eq!(app.request_registry().snapshot().scopes_created, 0);
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn actual_request_deadline_then_force_keeps_the_first_cooperative_cutoff() {
    let (app, state) = application(true).await;
    state.mode.store(6, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_millis(100)).await;
    let context = waiter.context.clone();
    // Park on the signal so Tokio's paused clock can reach the real timer.
    context.cancellation().cancelled().await;
    event(&state, "cancellation-observed", 1).await;
    let deadline = context.execution_deadline();
    assert_eq!(Instant::now(), deadline);
    tokio::time::advance(Duration::from_millis(100)).await;
    app.shutdown_budget().begin();
    context.stop(ExecutionStopReason::ForcedShutdown);
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::TimedOut)
    ));
    assert_eq!(Instant::now(), deadline + COOPERATIVE_CANCELLATION_CAP);
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::RequestTimeout)
    );
    assert_eq!(termination(&context), Some(SlotTermination::Dropped));
    assert_eq!(app.request_registry().snapshot().cleanup_failed, 0);
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn graceful_gate_closure_does_not_cancel_accepted_execution() {
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    app.request_registry().close_admission();
    let root = app.shutdown_budget().begin();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(Instant::now() < root.at(ShutdownStage::Graceful));
    assert!(lock(&state.views).iter().all(|view| !view.is_cancelled()));
    assert_eq!(termination(&context), None);
    state.execution_release.cancel();
    waiter.wait().await.unwrap().unwrap().unwrap();
    assert_eq!(termination(&context), Some(SlotTermination::Returned));
    assert_eq!(context.cancellation_reason(), None);
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn graceful_cutoff_signals_the_same_future_and_all_context_views() {
    let (app, state) = application(true).await;
    state.mode.store(5, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    let root = app.shutdown_budget().begin();
    let (response, _) = waiter.wait().await.unwrap().unwrap().unwrap();
    assert_eq!(
        response.status_code_value(),
        404,
        "preserve the actual route result at G"
    );
    assert_eq!(Instant::now(), root.at(ShutdownStage::Graceful));
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::GracefulDeadline)
    );
    assert_eq!(termination(&context), Some(SlotTermination::Returned));
    assert!(lock(&state.views).iter().all(|view| view.is_cancelled()));
    assert_eq!(
        lock(&state.events)
            .iter()
            .filter(|(_, event)| *event == "entered")
            .count(),
        1
    );
    assert_eq!(
        lock(&state.events)
            .iter()
            .filter(|(_, event)| *event == "execution-released")
            .count(),
        1
    );
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn force_notifies_before_drop_and_allows_a_controlled_normal_return() {
    let (app, state) = application(true).await;
    state.mode.store(6, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    event(&state, "cancellation-observed", 1).await;
    assert_eq!(termination(&context), None);
    assert_eq!(app.container().active_scope_count(), 1);
    assert!(!lock(&state.events)
        .iter()
        .any(|(_, event)| *event == "dispose"));
    tokio::time::advance(Duration::from_millis(50)).await;
    state.execution_release.cancel();
    let (response, _) = waiter.wait().await.unwrap().unwrap().unwrap();
    assert_eq!(
        response.status_code_value(),
        404,
        "preserve the actual route result after force"
    );
    assert_eq!(termination(&context), Some(SlotTermination::Returned));
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::ForcedShutdown)
    );
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn ignored_signal_stops_only_the_slot_and_leaves_di_cleanup_independent() {
    let (app, state) = application(true).await;
    state.mode.store(6, Ordering::Release);
    state.disposal_mode.store(1, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    let started = Instant::now();
    context.stop(ExecutionStopReason::ForcedShutdown);
    event(&state, "cancellation-observed", 1).await;
    assert_eq!(termination(&context), None);
    state.disposing.notified().await;
    assert_eq!(Instant::now(), started + COOPERATIVE_CANCELLATION_CAP);
    assert_eq!(termination(&context), Some(SlotTermination::Dropped));
    assert_eq!(app.request_registry().snapshot().scopes_terminal, 0);
    assert!(context.cancellation().is_cancelled());
    state.disposal_release.cancel();
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::Stopped)
    ));
    assert_eq!(app.request_registry().snapshot().cleanup_failed, 0);
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn repeated_stop_requests_cannot_extend_the_cooperative_window_or_reason() {
    let (app, state) = application(true).await;
    state.mode.store(6, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    let started = Instant::now();
    context.stop(ExecutionStopReason::RequestTimeout);
    event(&state, "cancellation-observed", 1).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    app.shutdown_budget().begin();
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::TimedOut)
    ));
    assert_eq!(Instant::now(), started + COOPERATIVE_CANCELLATION_CAP);
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::RequestTimeout)
    );
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn delayed_cancellation_observation_cannot_run_past_root_c() {
    let (app, state) = application(true).await;
    state.mode.store(6, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    let root = app.shutdown_budget().begin();
    // Simulate a delayed scheduler observing G only just before C. It cannot
    // create a fresh 250ms grant extending that original root cutoff.
    let late = root.at(ShutdownStage::Cooperative) - Duration::from_millis(1);
    tokio::time::advance(late - Instant::now()).await;
    event(&state, "cancellation-observed", 1).await;
    assert_eq!(termination(&context), None);
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::Stopped)
    ));
    assert_eq!(Instant::now(), root.at(ShutdownStage::Cooperative));
    assert_eq!(termination(&context), Some(SlotTermination::Dropped));
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn a_spent_root_window_does_not_poll_an_unstarted_callback() {
    let (app, state) = application(true).await;
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    let root = app.shutdown_budget().begin();
    tokio::time::advance(root.at(ShutdownStage::Cooperative) - Instant::now()).await;
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::Stopped)
    ));
    assert!(!lock(&context.0.evidence).execution.started());
    assert_eq!(app.request_registry().snapshot().scopes_created, 0);
    assert!(lock(&state.events).is_empty());
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn normal_after_code_observes_execution_cancellation_and_can_return() {
    let (app, state) = application(true).await;
    state.mode.store(7, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    event(&state, "after-pending", 1).await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    let (response, _) = waiter.wait().await.unwrap().unwrap().unwrap();
    assert_eq!(
        response.status_code_value(),
        404,
        "normal-after completion keeps the response"
    );
    assert!(lock(&state.events)
        .iter()
        .any(|(_, event)| *event == "after-returned"));
    assert_eq!(termination(&context), Some(SlotTermination::Returned));
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn a_panic_during_cooperative_polling_is_contained_before_di_cleanup() {
    let (app, state) = application(true).await;
    state.mode.store(8, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::Panicked)
    ));
    assert_eq!(termination(&context), Some(SlotTermination::Panicked));
    assert_eq!(app.request_registry().snapshot().cleanup_failed, 0);
    finish(&app, true).await;
}

#[tokio::test]
async fn gate_rejects_registered_candidates_and_late_work_without_user_execution() {
    let (app, state) = application(true).await;
    let capacity = Arc::new(Semaphore::new(1));
    let held = capacity.clone().acquire_owned().await.unwrap();
    let runtime = app.clone();
    let cap = capacity.clone();
    let first = app
        .request_registry()
        .spawn(app.clone(), move |owner| async move {
            owner.admit(&runtime, cap, runtime.transport_config().request_timeout)
        })
        .unwrap();
    assert_eq!(
        first.wait().await.unwrap(),
        Err(RequestAdmissionError::Capacity)
    );
    drop(held);
    let runtime = app.clone();
    let cap = capacity.clone();
    let candidate = app
        .request_registry()
        .spawn(app.clone(), move |owner| async move {
            owner.admit(&runtime, cap, runtime.transport_config().request_timeout)
        })
        .unwrap();
    app.request_registry().close_admission();
    assert_eq!(
        candidate.wait().await.unwrap(),
        Err(RequestAdmissionError::Shutdown)
    );
    assert!(app
        .request_registry()
        .spawn(app.clone(), |_| async {
            panic!("late work must never poll")
        })
        .is_err());
    let snapshot = app.request_registry().snapshot();
    assert_eq!(snapshot.admitted, 0);
    assert_eq!(snapshot.admission_rejected, 3);
    assert_eq!(snapshot.scopes_created, 0);
    assert_eq!(capacity.available_permits(), 1);
    assert!(lock(&state.events).is_empty());
    finish(&app, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_admission_closure_balances_accepted_and_rejected_identities() {
    let (app, _) = application(true).await;
    let capacity = Arc::new(Semaphore::new(64));
    let mut tasks = tokio::task::JoinSet::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(33));
    for _ in 0..32 {
        let app = app.clone();
        let cap = capacity.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let runtime = app.clone();
            match app
                .request_registry()
                .spawn(app.clone(), move |owner| async move {
                    owner.admit(&runtime, cap, runtime.transport_config().request_timeout)
                }) {
                Ok(waiter) => waiter.wait().await.unwrap().is_ok(),
                Err(_) => false,
            }
        });
    }
    barrier.wait().await;
    app.request_registry().close_admission();
    let mut accepted = 0;
    while let Some(result) = tasks.join_next().await {
        accepted += usize::from(result.unwrap());
    }
    let snapshot = app.request_registry().wait().await;
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.admitted, accepted);
    assert_eq!(snapshot.admitted + snapshot.admission_rejected, 32);
    assert_eq!(capacity.available_permits(), 64);
    finish(&app, true).await;
}

#[tokio::test]
async fn http1_keep_alive_and_new_connections_cannot_bypass_closed_admission() {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::TokioIo;
    let (app, _) = application(true).await;
    let root = start_server(&app).await;
    let address = app.bound_address().unwrap();
    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let request = || {
        hyper::Request::builder()
            .uri("/owner")
            .header("host", "lily.test")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let first = sender.send_request(request()).await.unwrap();
    assert_eq!(first.status(), 404);
    first.into_body().collect().await.unwrap();
    assert_eq!(app.request_registry().snapshot().admitted, 1);
    app.request_registry().close_admission();
    let denied = sender.send_request(request()).await.unwrap();
    assert_eq!(denied.status(), 503);
    assert_eq!(denied.headers()[http::header::CONNECTION], "close");
    denied.into_body().collect().await.unwrap();
    drop(sender);
    client.await.unwrap().unwrap();

    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let denied = sender.send_request(request()).await.unwrap();
    assert_eq!(denied.status(), 503);
    denied.into_body().collect().await.unwrap();
    drop(sender);
    client.await.unwrap().unwrap();
    let snapshot = app.request_registry().snapshot();
    assert_eq!(snapshot.admitted, 1);
    assert_eq!(snapshot.admission_rejected, 2);
    assert_eq!(snapshot.scopes_created, 1);
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn http2_late_stream_is_denied_while_an_accepted_stream_can_finish() {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    let (app, state) = application_with_protocol(true, lily_core::enums::HttpProtocol::Http2).await;
    state.mode.store(1, Ordering::Release);
    let root = start_server(&app).await;
    let socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(socket))
            .await
            .unwrap();
    let client = tokio::spawn(connection);
    let request = || {
        hyper::Request::builder()
            .uri("http://lily.test/owner")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let first = tokio::spawn(sender.send_request(request()));
    event(&state, "entered", 1).await;
    app.request_registry().close_admission();
    let late = sender.send_request(request()).await.unwrap();
    assert_eq!(late.status(), 503);
    assert!(!late.headers().contains_key(http::header::CONNECTION));
    late.into_body().collect().await.unwrap();
    assert!(!first.is_finished());
    assert!(lock(&state.views).iter().all(|view| !view.is_cancelled()));
    state.execution_release.cancel();
    let response = first.await.unwrap().unwrap();
    assert_eq!(response.status(), 404);
    response.into_body().collect().await.unwrap();
    assert_eq!(app.request_registry().snapshot().scopes_created, 1);
    assert_eq!(app.request_registry().snapshot().admitted, 1);
    assert_eq!(app.request_registry().snapshot().admission_rejected, 1);
    drop(sender);
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
    client.await.unwrap().unwrap();
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn actual_app_close_drains_an_accepted_request_without_signalling_it() {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::TokioIo;
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    let root = start_server(&app).await;
    let socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let response = tokio::spawn(async move {
        let response = sender
            .send_request(
                hyper::Request::builder()
                    .uri("/owner")
                    .header("host", "lily.test")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        response.into_body().collect().await.unwrap();
        status
    });
    event(&state, "entered", 1).await;
    let runtime = app.clone();
    let close = tokio::spawn(async move { runtime.close().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !app.request_admission_stopping() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!close.is_finished());
    assert!(lock(&state.views).iter().all(|view| !view.is_cancelled()));
    state.execution_release.cancel();
    assert_eq!(response.await.unwrap(), 404);
    close.await.unwrap().unwrap();
    root.await.unwrap().unwrap();
    client.await.unwrap().unwrap();
    assert!(app.request_registry().snapshot().is_terminal());
    app.container().close().await.unwrap();
}
