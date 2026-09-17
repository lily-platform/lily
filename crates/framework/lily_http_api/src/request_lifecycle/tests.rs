use super::*;
use crate::{
    AppBuilder, Extensions, HttpExchange, HttpMiddleware, HttpMiddlewareError,
    HttpMiddlewareInitError, HttpNext, MiddlewareDescriptor, MiddlewareErrorCode,
};
use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
struct ProbeState {
    mode: AtomicUsize,
    disposal_mode: AtomicUsize,
    entered: Notify,
    disposing: Notify,
    execution_release: CancellationToken,
    disposal_release: CancellationToken,
    released_requests: Mutex<HashSet<u64>>,
    events: Mutex<Vec<(u64, &'static str)>>,
    views: Mutex<Vec<crate::ExecutionCancellation>>,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct OwnerProbe {
    state: Arc<ProbeState>,
}

impl ServiceTrait for OwnerProbe {}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ScopedProbe {
    #[inject]
    owner: Arc<OwnerProbe>,
    context_id: u64,
}

#[async_trait::async_trait]
impl ServiceTrait for ScopedProbe {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.context_id = ProcessContext::current().unwrap().process_id;
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        let context = ProcessContext::current().expect("DI disposal keeps request context");
        assert_eq!(context.process_id, self.context_id);
        assert_eq!(
            context.metadata.get("transport").map(String::as_str),
            Some("http")
        );
        let state = &self.owner.state;
        assert!(
            lock(&state.released_requests).contains(&self.context_id),
            "request captures must release after execution and before scope disposal"
        );
        lock(&state.events).push((self.context_id, "dispose"));
        state.disposing.notify_one();
        match state.disposal_mode.load(Ordering::Acquire) {
            1 => state.disposal_release.cancelled().await,
            2 => panic!("contained scoped disposer panic"),
            _ => {}
        }
        lock(&state.events).push((self.context_id, "disposed"));
        Ok(())
    }
}

struct RequestCapture(Arc<ProbeState>, u64);
impl Drop for RequestCapture {
    fn drop(&mut self) {
        assert_eq!(ProcessContext::current().unwrap().process_id, self.1);
        lock(&self.0.released_requests).insert(self.1);
        lock(&self.0.events).push((self.1, "request-released"));
    }
}

struct ExecutionCapture(Arc<ProbeState>, u64);
impl Drop for ExecutionCapture {
    fn drop(&mut self) {
        assert!(
            !lock(&self.0.released_requests).contains(&self.1),
            "execution only borrows owner-retained request state"
        );
        lock(&self.0.events).push((self.1, "execution-released"));
    }
}

struct OwnerMiddleware(Arc<Extensions>);

#[async_trait::async_trait]
impl HttpMiddleware for OwnerMiddleware {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self(extensions))
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("request-owner-probe", crate::MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        cancellation: crate::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        let scoped = self.0.get_service::<ScopedProbe>(None).await.unwrap();
        let state = scoped.owner.state.clone();
        let id = ProcessContext::current().unwrap().process_id;
        assert_eq!(id, scoped.context_id);
        exchange
            .request_mut()
            .local_mut()
            .insert(RequestCapture(state.clone(), id));
        let _execution_capture = ExecutionCapture(state.clone(), id);
        lock(&state.views).extend([
            cancellation.clone(),
            exchange.execution_cancellation(),
            exchange.request().execution_cancellation(),
        ]);
        lock(&state.events).push((id, "entered"));
        state.entered.notify_one();
        tokio::task::yield_now().await;
        assert_eq!(ProcessContext::current().unwrap().process_id, id);
        match state.mode.load(Ordering::Acquire) {
            1 => state.execution_release.cancelled().await,
            2 => panic!("contained action/middleware execution panic"),
            3 => return Err(HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL)),
            4 => {
                while exchange
                    .request_mut()
                    .next_body_chunk()
                    .await
                    .map_err(|_| HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))?
                    .is_some()
                {}
            }
            5 | 6 | 8 => {
                cancellation.cancelled().await;
                assert!(exchange.execution_cancellation().is_cancelled());
                assert!(exchange.request().execution_cancellation().is_cancelled());
                lock(&state.events).push((id, "cancellation-observed"));
                if state.mode.load(Ordering::Acquire) == 6 {
                    state.execution_release.cancelled().await;
                }
                if state.mode.load(Ordering::Acquire) == 8 {
                    panic!("contained panic during cooperative cancellation");
                }
            }
            7 => {
                next.run(exchange).await?;
                lock(&state.events).push((id, "after-pending"));
                cancellation.cancelled().await;
                lock(&state.events).push((id, "after-returned"));
                return Ok(());
            }
            _ => {}
        }
        next.run(exchange).await
    }
}

#[path = "cancellation.rs"]
mod cancellation;

#[path = "body_tests.rs"]
mod body_tests;

#[path = "middleware_tests.rs"]
mod middleware_tests;

#[path = "report_tests.rs"]
mod report_tests;

async fn application(caller_owned: bool) -> (Arc<App>, Arc<ProbeState>) {
    application_with_protocol(caller_owned, lily_core::enums::HttpProtocol::Http1_1).await
}

async fn application_with_protocol(
    caller_owned: bool,
    protocol: lily_core::enums::HttpProtocol,
) -> (Arc<App>, Arc<ProbeState>) {
    let mut builder = AppBuilder::new("127.0.0.1:0")
        .protocol(protocol)
        .middleware::<OwnerMiddleware>();
    if caller_owned {
        builder = builder.container(Arc::new(ApplicationContainer::build().await.unwrap()));
    }
    let app = Arc::new(builder.build().await.unwrap());
    let probe = app.container().resolve::<OwnerProbe>(None).await.unwrap();
    (app, probe.state.clone())
}

type DispatchResult =
    Result<Result<(Response, crate::app::AppCallOutcome), HttpApiError>, ExecutionInterrupted>;

async fn dispatch(app: &Arc<App>, timeout: Duration) -> RequestWaiter<DispatchResult> {
    let request = Request::from_transport_parts("GET".into(), "/owner".into(), Vec::new(), &[])
        .await
        .unwrap();
    let response = Response::new().await.unwrap();
    let runtime = app.clone();
    app.request_registry()
        .spawn(app.clone(), move |owner| async move {
            owner
                .execute_for_test(
                    timeout,
                    runtime.call_with_outcome(request, response, &owner),
                )
                .await
        })
        .unwrap()
}

async fn finish(app: &App, caller_owned: bool) {
    app.close().await.unwrap();
    if caller_owned {
        assert!(!lily_injection::__private::container_shutdown_started(
            app.container()
        ));
        app.container().close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_identity_and_exact_scope_are_published_before_user_poll() {
    let (app, state) = application(false).await;
    state.mode.store(1, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    let snapshot = app.request_registry().snapshot();
    assert_eq!(snapshot.registered, 1);
    assert_eq!(snapshot.scopes_created, 1);
    assert_eq!(snapshot.scopes_terminal, 0);
    assert_eq!(app.container().active_scope_count(), 1);
    assert!(lock(&context.0.evidence).execution.started());
    state.execution_release.cancel();
    waiter.wait().await.unwrap().unwrap().unwrap();
    assert!(app.request_registry().snapshot().is_terminal());
    assert_eq!(
        lock(&state.events)
            .iter()
            .map(|(_, event)| *event)
            .collect::<Vec<_>>(),
        [
            "entered",
            "execution-released",
            "request-released",
            "dispose",
            "disposed"
        ]
    );
    finish(&app, false).await;
}

#[tokio::test]
async fn dropped_service_waiter_stops_only_execution_and_retains_pending_di() {
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    state.disposal_mode.store(1, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    drop(waiter);
    state.disposing.notified().await;
    {
        let evidence = lock(&context.0.evidence);
        assert_eq!(
            evidence.execution.termination(),
            Some(SlotTermination::Dropped)
        );
        assert_eq!(
            evidence.execution.cancellation_requested(),
            Some(ExecutionStopReason::ServiceWaiterDropped)
        );
    }
    let snapshot = app.request_registry().snapshot();
    assert_eq!(snapshot.outstanding, 1);
    assert_eq!(snapshot.scopes_terminal, 0);
    assert!(!lily_injection::__private::container_shutdown_started(
        app.container()
    ));
    state.disposal_release.cancel();
    assert!(app.request_registry().wait().await.is_terminal());
    assert_eq!(app.container().active_scope_count(), 0);
    finish(&app, true).await;
}

#[tokio::test]
async fn dropping_an_unpolled_service_waiter_does_not_enter_user_execution() {
    let (app, state) = application(false).await;
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    let context = waiter.context.clone();
    drop(waiter);
    let snapshot = app.request_registry().wait().await;
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.scopes_created, 0);
    assert!(!lock(&context.0.evidence).execution.started());
    assert_eq!(
        lock(&context.0.evidence).execution.termination(),
        Some(SlotTermination::Dropped)
    );
    assert!(lock(&state.events).is_empty());
    finish(&app, false).await;
}

#[tokio::test]
async fn execution_panic_is_contained_before_request_and_scope_release() {
    let (app, state) = application(false).await;
    state.mode.store(2, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    let context = waiter.context.clone();
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::Panicked)
    ));
    assert_eq!(
        lock(&context.0.evidence).execution.termination(),
        Some(SlotTermination::Panicked)
    );
    let snapshot = app.request_registry().snapshot();
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.cleanup_failed, 0);
    assert_eq!(snapshot.owner_failed, 0);
    assert!(ProcessContext::current().is_none());
    finish(&app, false).await;
}

#[tokio::test]
async fn returned_middleware_error_remains_a_normal_execution_return() {
    let (app, state) = application(false).await;
    state.mode.store(3, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    let context = waiter.context.clone();
    let (_, outcome) = waiter.wait().await.unwrap().unwrap().unwrap();
    assert_eq!(outcome.status(), 500);
    assert_eq!(
        lock(&context.0.evidence).execution.termination(),
        Some(SlotTermination::Returned)
    );
    assert_eq!(
        lock(&context.0.evidence).execution.cancellation_requested(),
        None
    );
    finish(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn request_timeout_preserves_its_scope_receipt_after_slot_destruction() {
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(1)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::TimedOut)
    ));
    {
        let evidence = lock(&context.0.evidence);
        assert_eq!(
            evidence.execution.termination(),
            Some(SlotTermination::Dropped)
        );
        assert_eq!(
            evidence.execution.cancellation_requested(),
            Some(ExecutionStopReason::RequestTimeout)
        );
        assert!(evidence.scope.is_some());
    }
    assert!(app.request_registry().wait().await.is_terminal());
    // Execution expiry does not spend the independent cleanup allowance.
    assert_eq!(
        lock(&context.0.evidence).scope.as_ref().unwrap().evidence(),
        ScopeCleanupEvidence::Terminated(CleanupOutcome::Succeeded)
    );
    app.close().await.unwrap();
    assert!(!lily_injection::__private::container_shutdown_started(
        app.container()
    ));
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn missing_scope_receipt_blocks_reconciliation_instead_of_becoming_no_scope() {
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    let context = waiter.context.clone();
    state.entered.notified().await;
    let receipt = lock(&context.0.evidence).scope.take().unwrap();
    state.execution_release.cancel();
    waiter.wait().await.unwrap().unwrap().unwrap();
    assert!(!app.request_registry().snapshot().is_terminal());
    assert_eq!(app.container().active_scope_count(), 1);
    // Restore the deliberately removed observation for deterministic teardown.
    lock(&context.0.evidence).scope = Some(receipt);
    context.close_scope().await.unwrap();
    assert!(app.request_registry().wait().await.is_terminal());
    finish(&app, true).await;
}

#[tokio::test]
async fn a_late_scope_observer_cannot_attach_to_a_reused_process_id() {
    let (app, _) = application(true).await;
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    let context = waiter.context.clone();
    waiter.wait().await.unwrap().unwrap().unwrap();
    let old_receipt = lock(&context.0.evidence)
        .scope
        .as_ref()
        .unwrap()
        .terminal
        .clone();
    let mut replacement = app
        .container()
        .create_scope(context.0.context.clone())
        .unwrap();
    old_receipt.await;
    context.close_scope().await.unwrap();
    assert_eq!(app.container().active_scope_count(), 1);
    app.close().await.unwrap();
    assert_eq!(app.container().active_scope_count(), 1);
    replacement.close().await.unwrap();
    app.container().close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_request_owners_reconcile_distinct_scopes_and_contexts() {
    let (app, state) = application(false).await;
    let mut waiters = futures::stream::FuturesUnordered::new();
    for _ in 0..32 {
        waiters.push(dispatch(&app, Duration::from_secs(5)).await.wait());
    }
    use futures::StreamExt;
    while let Some(result) = waiters.next().await {
        result.unwrap().unwrap().unwrap();
    }
    let snapshot = app.request_registry().wait().await;
    assert_eq!(snapshot.registered, 32);
    assert_eq!(snapshot.retired, 32);
    assert_eq!(snapshot.scopes_terminal, 32);
    assert_eq!(lock(&state.released_requests).len(), 32);
    assert!(snapshot.is_terminal());
    assert!(ProcessContext::current().is_none());
    finish(&app, false).await;
}

async fn start_server(app: &Arc<App>) -> tokio::task::JoinHandle<std::io::Result<()>> {
    let runtime = app.as_ref().clone();
    let root = tokio::spawn(async move {
        runtime
            .start_with_cancellation(CancellationToken::new())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while app.bound_address().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed listener readiness");
    root
}

async fn event(state: &ProbeState, name: &'static str, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while lock(&state.events)
            .iter()
            .filter(|(_, event)| *event == name)
            .count()
            < count
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request lifecycle barrier");
}

#[tokio::test]
async fn http1_peer_disconnect_keeps_request_cleanup_owned_after_transport_join() {
    use tokio::io::AsyncWriteExt;
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    state.disposal_mode.store(1, Ordering::Release);
    let root = start_server(&app).await;
    let mut peer = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    peer.write_all(b"GET /owner HTTP/1.1\r\nHost: lily.test\r\n\r\n")
        .await
        .unwrap();
    event(&state, "entered", 1).await;
    drop(peer);
    event(&state, "dispose", 1).await;
    assert_eq!(app.request_registry().snapshot().outstanding, 1);
    assert_eq!(app.request_registry().snapshot().scopes_terminal, 0);
    assert!(!lily_injection::__private::container_shutdown_started(
        app.container()
    ));
    state.disposal_release.cancel();
    assert!(app.request_registry().wait().await.is_terminal());
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
    assert!(!lily_injection::__private::container_shutdown_started(
        app.container()
    ));
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn pending_http_request_body_disconnect_cannot_lose_its_scope() {
    use tokio::io::AsyncWriteExt;
    let (app, state) = application(true).await;
    state.mode.store(4, Ordering::Release);
    state.disposal_mode.store(1, Ordering::Release);
    let root = start_server(&app).await;
    let mut peer = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    peer.write_all(b"POST /owner HTTP/1.1\r\nHost: lily.test\r\nContent-Length: 20\r\n\r\nx")
        .await
        .unwrap();
    event(&state, "entered", 1).await;
    assert_eq!(app.container().active_scope_count(), 1);
    drop(peer);
    event(&state, "dispose", 1).await;
    assert_eq!(app.request_registry().snapshot().scopes_terminal, 0);
    state.disposal_release.cancel();
    assert!(app.request_registry().wait().await.is_terminal());
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn http2_connection_loss_retains_each_concurrent_request_owner() {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    let (app, state) = application_with_protocol(true, lily_core::enums::HttpProtocol::Http2).await;
    state.mode.store(1, Ordering::Release);
    state.disposal_mode.store(1, Ordering::Release);
    let root = start_server(&app).await;
    let peer = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap()
        .into_std()
        .unwrap();
    let disconnect = peer.try_clone().unwrap();
    let peer = tokio::net::TcpStream::from_std(peer).unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(peer))
            .await
            .unwrap();
    let client = tokio::spawn(connection);
    let first = sender.send_request(
        hyper::Request::builder()
            .uri("http://lily.test/owner")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    );
    let second = sender.send_request(
        hyper::Request::builder()
            .uri("http://lily.test/owner")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    );
    let first = tokio::spawn(first);
    let second = tokio::spawn(second);
    event(&state, "entered", 2).await;
    // Aborting Hyper's client waiter alone is not TCP closure: its executor
    // can still own protocol work. Shut the actual socket in both directions.
    disconnect.shutdown(std::net::Shutdown::Both).unwrap();
    client.abort();
    assert!(client.await.unwrap_err().is_cancelled());
    let _ = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .unwrap()
        .unwrap();
    drop(sender);
    event(&state, "dispose", 2).await;
    let snapshot = app.request_registry().snapshot();
    assert_eq!(snapshot.registered, 2);
    assert_eq!(snapshot.outstanding, 2);
    assert_eq!(snapshot.scopes_created, 2);
    assert_eq!(snapshot.scopes_terminal, 0);
    state.disposal_release.cancel();
    assert!(app.request_registry().wait().await.is_terminal());
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
    app.container().close().await.unwrap();
}

#[tokio::test]
async fn forced_transport_stop_waits_for_request_scope_before_owned_dependencies() {
    use tokio::io::AsyncWriteExt;
    let (app, state) = application(false).await;
    state.mode.store(1, Ordering::Release);
    state.disposal_mode.store(1, Ordering::Release);
    let root = start_server(&app).await;
    let mut peer = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    peer.write_all(b"GET /owner HTTP/1.1\r\nHost: lily.test\r\n\r\n")
        .await
        .unwrap();
    event(&state, "entered", 1).await;
    let observer = app.clone();
    let close = tokio::spawn(async move { observer.close().await });
    app.transport_force().cancel();
    event(&state, "dispose", 1).await;
    assert!(!close.is_finished());
    assert!(!lily_injection::__private::container_shutdown_started(
        app.container()
    ));
    assert_eq!(app.request_registry().snapshot().outstanding, 1);
    state.disposal_release.cancel();
    drop(peer);
    close.await.unwrap().unwrap();
    root.await.unwrap().unwrap();
    assert!(app.request_registry().snapshot().is_terminal());
    assert!(lily_injection::__private::container_shutdown_quiescent(
        app.container()
    ));
}

#[tokio::test(start_paused = true)]
async fn root_cleanup_cutoff_stops_only_the_retained_di_generation_and_preserves_failure() {
    let (app, state) = application(true).await;
    state.disposal_mode.store(1, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(120)).await;
    let context = waiter.context.clone();
    state.disposing.notified().await;
    drop(waiter);
    assert!(app.close().await.is_err());
    let deadlines = app.shutdown_budget().deadlines().unwrap();
    let snapshot = app.request_registry().wait().await;
    assert!(Instant::now() <= deadlines.at(ShutdownStage::Reconcile));
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.cleanup_failed, 1);
    assert_eq!(
        lock(&context.0.evidence).scope.as_ref().unwrap().evidence(),
        ScopeCleanupEvidence::Terminated(CleanupOutcome::TimedOut)
    );
    let frozen = app.shutdown_report().unwrap();
    assert_eq!(frozen.requests.scopes.timed_out, 1);
    assert_eq!(frozen.requests.scopes.outstanding, 0);
    assert!(frozen.terminal() && !frozen.succeeded());
    assert!(app.close().await.is_err());
    assert_eq!(app.shutdown_report().unwrap(), frozen);
    assert!(!lily_injection::__private::container_shutdown_started(
        app.container()
    ));
    let _ = app.container().close().await;
}

#[tokio::test]
async fn scope_disposer_panic_is_terminal_failure_even_with_caller_owned_di() {
    let (app, state) = application(true).await;
    state.disposal_mode.store(2, Ordering::Release);
    let waiter = dispatch(&app, Duration::from_secs(5)).await;
    waiter.wait().await.unwrap().unwrap().unwrap();
    let snapshot = app.request_registry().wait().await;
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.cleanup_failed, 1);
    // This failure retired before shutdown; retain it in lifetime diagnostics.
    app.close().await.unwrap();
    assert_eq!(app.request_registry().attempt_snapshot().registered, 0);
    assert!(!lily_injection::__private::container_shutdown_started(
        app.container()
    ));
    let _ = app.container().close().await;
}

#[tokio::test]
async fn request_capacity_is_retained_until_scope_and_owner_join_are_terminal() {
    let (app, state) = application(true).await;
    state.disposal_mode.store(1, Ordering::Release);
    let capacity = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = capacity.clone().try_acquire_owned().unwrap();
    let request = Request::from_transport_parts("GET".into(), "/owner".into(), Vec::new(), &[])
        .await
        .unwrap();
    let response = Response::new().await.unwrap();
    let runtime = app.clone();
    let waiter = app
        .request_registry()
        .spawn(app.clone(), move |owner| async move {
            owner.retain_permit(permit);
            owner
                .execute_for_test(
                    Duration::from_secs(5),
                    runtime.call_with_outcome(request, response, &owner),
                )
                .await
        })
        .unwrap();
    state.disposing.notified().await;
    drop(waiter);
    assert_eq!(capacity.available_permits(), 0);
    assert_eq!(app.request_registry().snapshot().outstanding, 1);
    state.disposal_release.cancel();
    assert!(app.request_registry().wait().await.is_terminal());
    assert_eq!(capacity.available_permits(), 1);
    finish(&app, true).await;
}

struct BlockingExecutionDrop {
    started: Option<oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}
impl Drop for BlockingExecutionDrop {
    fn drop(&mut self) {
        let _ = self.started.take().unwrap().send(());
        self.release
            .recv_timeout(Duration::from_secs(5))
            .expect("test must release the deliberately blocked destructor");
    }
}

struct ReleaseBlockedDrop(Option<std::sync::mpsc::Sender<()>>);
impl Drop for ReleaseBlockedDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_in_progress_execution_destructor_blocks_scope_close_and_terminal_evidence() {
    let (app, state) = application(true).await;
    state.mode.store(1, Ordering::Release);
    let (dropping, dropped) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let release = ReleaseBlockedDrop(Some(release));
    let capture = BlockingExecutionDrop {
        started: Some(dropping),
        release: released,
    };
    let request = Request::from_transport_parts("GET".into(), "/owner".into(), Vec::new(), &[])
        .await
        .unwrap();
    let response = Response::new().await.unwrap();
    let runtime = app.clone();
    let waiter = app
        .request_registry()
        .spawn(app.clone(), move |owner| async move {
            owner
                .execute_for_test(Duration::from_secs(5), async {
                    let _capture = capture;
                    runtime.call_with_outcome(request, response, &owner).await
                })
                .await
        })
        .unwrap();
    let context = waiter.context.clone();
    state.entered.notified().await;
    drop(waiter);
    tokio::time::timeout(Duration::from_secs(5), dropped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lock(&context.0.evidence).execution.termination(), None);
    assert_eq!(app.request_registry().snapshot().outstanding, 1);
    assert_eq!(app.container().active_scope_count(), 1);
    assert!(!lock(&state.events)
        .iter()
        .any(|(_, event)| *event == "dispose"));
    // Observe a pending real join without depending on a timer driver while
    // a runtime worker is deliberately blocked inside synchronous Drop.
    assert!(app.request_registry().wait().now_or_never().is_none());
    drop(release);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), app.request_registry().wait())
            .await
            .unwrap()
            .is_terminal()
    );
    assert_eq!(
        lock(&context.0.evidence).execution.termination(),
        Some(SlotTermination::Dropped)
    );
    finish(&app, true).await;
}

#[tokio::test]
async fn external_scope_close_is_terminal_without_inventing_a_disposal_result() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let app = Arc::new(
        AppBuilder::new("127.0.0.1:0")
            .container(container.clone())
            .build()
            .await
            .unwrap(),
    );
    let request = Request::from_transport_parts("GET".into(), "/owner".into(), Vec::new(), &[])
        .await
        .unwrap();
    let response = Response::new().await.unwrap();
    let (published, received) = oneshot::channel();
    let resume = CancellationToken::new();
    let proceed = resume.clone();
    let runtime = app.clone();
    let waiter = app
        .request_registry()
        .spawn(app.clone(), move |owner| async move {
            let output = owner
                .execute_for_test(
                    Duration::from_secs(5),
                    runtime.call_with_outcome(request, response, &owner),
                )
                .await;
            published
                .send(owner.clone())
                .unwrap_or_else(|_| panic!("request observer must exist"));
            proceed.cancelled().await;
            output
        })
        .unwrap();
    let context = received.await.unwrap();
    {
        let mut resources = context.0.resources.lock().await;
        resources
            .as_mut()
            .unwrap()
            .scope
            .as_mut()
            .unwrap()
            .close()
            .await
            .unwrap();
    }
    resume.cancel();
    waiter.wait().await.unwrap().unwrap().unwrap();
    let snapshot = app.request_registry().wait().await;
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.cleanup_failed, 1);
    assert_eq!(
        lock(&context.0.evidence).scope.as_ref().unwrap().evidence(),
        ScopeCleanupEvidence::Terminated(CleanupOutcome::Unknown)
    );
    // This failure retired before shutdown; retain it in lifetime diagnostics.
    app.close().await.unwrap();
    assert_eq!(app.request_registry().attempt_snapshot().registered, 0);
    container.close().await.unwrap();
}
