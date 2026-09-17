use super::*;
use crate::shutdown_report::{DependencyDisposition, HttpShutdownCompletion};
use futures::stream::{FuturesUnordered, StreamExt};

#[tokio::test]
async fn retired_failures_stay_in_lifetime_diagnostics_outside_the_attempt() {
    let (app, state) = application(true).await;
    state.disposal_mode.store(2, Ordering::Release);
    dispatch(&app, Duration::from_secs(5))
        .await
        .wait()
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(app.request_registry().wait().await.cleanup_failed, 1);
    app.close().await.unwrap();
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.requests.registered, 0);
    assert_eq!(report.requests_lifetime.registered, 1);
    assert_eq!(report.requests_lifetime.cleanup_failed, 1);
    assert_eq!(report.requests_lifetime.scopes.failed, 1);
    assert_eq!(report.completion, HttpShutdownCompletion::GracefulCompleted);
    assert!(report.terminal() && report.reconciles());
    let _ = app.container().close().await;
}

#[tokio::test]
async fn active_failed_scope_is_terminal_failed_not_outstanding_or_http_error() {
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    state.disposal_mode.store(2, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    state.entered.notified().await;
    app.request_registry().close_admission();
    state.execution_release.cancel();
    waiter.wait().await.unwrap().unwrap().unwrap();
    app.request_registry().wait().await;
    let error = app.close().await.unwrap_err().to_string();
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.requests.registered, 1);
    assert_eq!(report.requests.outstanding, 0);
    assert_eq!(report.requests.execution.returned, 1);
    assert_eq!(report.requests.scopes.failed, 1);
    assert_eq!(report.completion, HttpShutdownCompletion::TerminalFailed);
    assert!(report.terminal() && report.reconciles());
    assert_eq!(
        report.dependencies.di.disposition,
        DependencyDisposition::NotOwned
    );
    assert_eq!(app.close().await.unwrap_err().to_string(), error);
    assert_eq!(app.shutdown_report().unwrap(), report);
    let _ = app.container().close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_observers_move_each_cohort_identity_to_totals_once() {
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    let mut waiters = FuturesUnordered::new();
    for _ in 0..32 {
        waiters.push(dispatch(&app, Duration::from_secs(10)).await.wait());
    }
    event(&state, "entered", 32).await;
    app.request_registry().close_admission();
    let mut observers = Vec::new();
    for _ in 0..8 {
        let registry = app.request_registry().clone();
        observers.push(tokio::spawn(async move {
            for _ in 0..32 {
                let snapshot = registry.attempt_snapshot();
                assert!(snapshot.reconciles());
                assert_eq!(snapshot.registered, 32);
                tokio::task::yield_now().await;
            }
        }));
    }
    state.execution_release.cancel();
    while let Some(result) = waiters.next().await {
        result.unwrap().unwrap().unwrap();
    }
    for observer in observers {
        observer.await.unwrap();
    }
    app.close().await.unwrap();
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.requests.retired, 32);
    assert_eq!(report.requests.owners.completed, 32);
    assert_eq!(report.requests.scopes.succeeded, 32);
    assert_eq!(report.requests.middleware.normal_returned, 32);
    assert!(report.reconciles() && report.succeeded());
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn candidate_and_rejected_admission_are_not_double_counted_as_requests() {
    let (app, _) = application(true).await;
    let entered = Arc::new(Notify::new());
    let release = CancellationToken::new();
    let runtime = app.clone();
    let notify = entered.clone();
    let resume = release.clone();
    let waiter = app
        .request_registry()
        .spawn(app.clone(), move |context| async move {
            notify.notify_one();
            resume.cancelled().await;
            context.admit(
                &runtime,
                Arc::new(Semaphore::new(1)),
                runtime.transport_config().request_timeout,
            )
        })
        .unwrap();
    entered.notified().await;
    app.request_registry().close_admission();
    assert!(matches!(
        app.request_registry().spawn(app.clone(), |_| async {}),
        Err(RequestOwnerError::AdmissionClosed)
    ));
    release.cancel();
    assert_eq!(
        waiter.wait().await.unwrap(),
        Err(RequestAdmissionError::Shutdown)
    );
    app.close().await.unwrap();
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.requests.registered, 1);
    assert_eq!(report.requests.admitted, 0);
    assert_eq!(report.requests.admission_rejected, 2);
    assert_eq!(report.requests.execution.created, 0);
    assert_eq!(report.requests.scopes.created, 0);
    assert!(report.succeeded() && report.reconciles());
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn caller_scopes_never_enter_the_http_attempt_inventory() {
    let (app, _) = application(true).await;
    let mut unrelated = app.container().create_scope(ProcessContext::new()).unwrap();
    app.close().await.unwrap();
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.requests.scopes.created, 0);
    assert_eq!(
        report.dependencies.di.disposition,
        DependencyDisposition::NotOwned
    );
    assert_eq!(app.container().active_scope_count(), 1);
    assert!(report.terminal() && report.succeeded());
    unrelated.close().await.unwrap();
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn handler_return_does_not_imply_its_scope_is_terminal() {
    let (app, state) = application(true).await;
    state.disposal_mode.store(1, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    state.disposing.notified().await;
    app.request_registry().close_admission();
    let pending = app.request_registry().attempt_snapshot();
    assert_eq!(pending.execution.returned, 1);
    assert_eq!(pending.scopes.outstanding, 1);
    assert_eq!(pending.outstanding, 1);
    assert!(pending.reconciles());
    assert!(!pending.is_terminal());
    state.disposal_release.cancel();
    waiter.wait().await.unwrap().unwrap().unwrap();
    app.close().await.unwrap();
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.requests.scopes.succeeded, 1);
    assert!(report.succeeded());
    app.container().close().await.unwrap();
}
