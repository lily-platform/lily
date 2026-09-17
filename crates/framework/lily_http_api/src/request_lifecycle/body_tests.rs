use super::*;
use crate::lifecycle::{ResponseBodyOutcome, ResponseHeadEvidence};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use lily_web_core::{
    streaming, IntoResponse, RequestBodyError, RequestBodyStream, ResponseBodyError,
    TransportResponseBody,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

async fn response<V, F>(
    app: &Arc<App>,
    input: Option<Box<dyn RequestBodyStream>>,
    factory: F,
) -> RequestWaiter<BodyBridge>
where
    V: IntoResponse + Send + 'static,
    F: FnOnce(&mut Request, crate::ExecutionCancellation) -> V + Send + 'static,
{
    let runtime = app.clone();
    app.request_registry()
        .spawn(app.clone(), move |owner| async move {
            let request = Request::from_streaming_transport_parts(
                "GET".into(),
                "/body-owner".into(),
                vec![],
                input.map(|input| owner.track_input(input)),
                1024,
            )
            .unwrap();
            let source = owner
                .execute_for_test(Duration::from_secs(2), async {
                    let (mut response, _) = runtime
                        .call_with_outcome(request, Response::new().await.unwrap(), &owner)
                        .await?;
                    let mut resources = owner.0.resources.lock().await;
                    let resources = resources.as_mut().unwrap();
                    let context = resources.scope.as_ref().unwrap().context().clone();
                    let request = resources.request.as_mut().unwrap();
                    ProcessContext::scope(context, async {
                        factory(request, owner.cancellation())
                            .write_to_response(&mut response, request)
                            .await
                    })
                    .await?;
                    Ok(response.into_transport_parts()?.into_body())
                })
                .await
                .unwrap()
                .unwrap();
            let TransportResponseBody::Stream(source) = source else {
                panic!("expected streaming source")
            };
            owner.own_response_stream(source, 4, None)
        })
        .unwrap()
}

async fn chunk(bridge: &mut BodyBridge) -> Option<Result<Bytes, ResponseBodyError>> {
    futures::future::poll_fn(|cx| bridge.poll_chunk(cx)).await
}

#[tokio::test(start_paused = true)]
async fn report_keeps_handler_return_body_handoff_and_source_termination_separate() {
    let (app, state) = application(true).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let source_state = state.clone();
    let waiter = response(&app, None, move |_, _| {
        streaming(Source {
            state: source_state,
            polls,
            pending: true,
            panic: false,
        })
    })
    .await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    app.request_registry().close_admission();
    assert!(chunk(&mut bridge).now_or_never().is_none());
    event(&state, "source-polled", 1).await;
    let pending = app.request_registry().attempt_snapshot();
    assert_eq!(pending.execution.returned, 1);
    assert_eq!(pending.response.head_handed_off, 1);
    assert_eq!(pending.response.streaming_outstanding, 1);
    assert_eq!(pending.response.source_released, 0);
    assert_eq!(pending.scopes.outstanding, 1);
    assert!(pending.reconciles() && !pending.is_terminal());
    context.stop(ExecutionStopReason::ForcedShutdown);
    app.request_registry().wait().await;
    app.close().await.unwrap();
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.requests.response.interrupted, 1);
    assert_eq!(report.requests.response.source_released, 1);
    assert_eq!(report.requests.response.bridge_detached, 1);
    assert_eq!(report.requests.scopes.succeeded, 1);
    assert_eq!(report.requests.response.streaming_outstanding, 0);
    assert_eq!(
        report.completion,
        crate::shutdown_report::HttpShutdownCompletion::ForcedCompleted
    );
    assert!(report.reconciles() && report.succeeded());
    // This bridge can still exist. Only independent bytes cross the seam;
    // neither this report nor its handoff counter promises wire delivery.
    drop(bridge);
    app.container().close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn report_records_an_unpolled_forced_source_as_not_started() {
    let (app, state) = application(true).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let observed_polls = polls.clone();
    let waiter = response(&app, None, move |_, _| {
        streaming(Source {
            state,
            polls,
            pending: true,
            panic: false,
        })
    })
    .await;
    let context = waiter.context.clone();
    let bridge = waiter.handoff().await.unwrap();
    app.request_registry().close_admission();
    context.stop(ExecutionStopReason::ForcedShutdown);
    app.request_registry().wait().await;
    app.close().await.unwrap();
    let report = app.shutdown_report().unwrap();
    assert_eq!(observed_polls.load(Ordering::Acquire), 0);
    assert_eq!(report.requests.response.not_started, 1);
    assert_eq!(report.requests.response.interrupted, 0);
    assert_eq!(report.requests.response.source_released, 1);
    assert!(report.succeeded());
    drop(bridge);
    app.container().close().await.unwrap();
}

struct Source {
    state: Arc<ProbeState>,
    polls: Arc<AtomicUsize>,
    pending: bool,
    panic: bool,
}
impl Stream for Source {
    type Item = Result<Bytes, ResponseBodyError>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let id = ProcessContext::current()
            .expect("body uses original DI context")
            .process_id;
        assert!(!lock(&self.state.events).contains(&(id, "dispose")));
        lock(&self.state.events).push((id, "source-polled"));
        assert!(!self.panic, "contained producer poll panic");
        let index = self.polls.fetch_add(1, Ordering::AcqRel);
        self.state.entered.notify_one();
        if self.pending {
            return Poll::Pending;
        }
        Poll::Ready(if index == 0 {
            Some(Ok(Bytes::from_static(b"abcdef")))
        } else {
            None
        })
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        let id = ProcessContext::current()
            .expect("source destruction has request context")
            .process_id;
        assert!(!lock(&self.state.events).contains(&(id, "dispose")));
        lock(&self.state.events).push((id, "source-released"));
    }
}

#[tokio::test]
async fn handoff_preserves_scope_and_source_backpressure_until_actual_release() {
    let (app, state) = application(true).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let source = Source {
        state: state.clone(),
        polls: polls.clone(),
        pending: false,
        panic: false,
    };
    let waiter = response(&app, None, move |_, _| streaming(source)).await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(polls.load(Ordering::Acquire), 0);
    assert_eq!(app.container().active_scope_count(), 1);
    assert_eq!(
        lock(&context.0.evidence).response.head(),
        ResponseHeadEvidence::HandedOffToService
    );
    assert!(lock(&context.0.evidence).execution.is_terminal());
    assert!(!lock(&context.0.evidence).response.resources_terminal());
    assert!(context.close_scope().await.is_err());
    assert_eq!(chunk(&mut bridge).await.unwrap().unwrap(), "abcd");
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(polls.load(Ordering::Acquire), 1);
    assert_eq!(chunk(&mut bridge).await.unwrap().unwrap(), "ef");
    assert_eq!(polls.load(Ordering::Acquire), 1);
    assert!(chunk(&mut bridge).await.is_none());
    assert!(app.request_registry().wait().await.is_terminal());
    {
        let events = lock(&state.events);
        let source = events
            .iter()
            .position(|(_, e)| *e == "source-released")
            .unwrap();
        let dispose = events.iter().position(|(_, e)| *e == "dispose").unwrap();
        assert!(source < dispose);
    }
    // Retaining an inert protocol bridge does not retain this DI generation.
    assert_eq!(app.container().active_scope_count(), 0);
    assert!(lock(&context.0.evidence).response.resources_terminal());
    finish(&app, true).await;
}

#[tokio::test]
async fn exact_length_bridge_drop_releases_source_and_scope_without_cancelling_execution() {
    for (length, reads, completed) in [(6, 2, true), (0, 0, true), (6, 1, false)] {
        let (app, state) = application(true).await;
        let polls = Arc::new(AtomicUsize::new(0));
        let source = Source {
            state: state.clone(),
            polls: polls.clone(),
            pending: false,
            panic: false,
        };
        let waiter = response(&app, None, move |_, _| {
            streaming(source).content_length(length)
        })
        .await;
        let context = waiter.context.clone();
        let mut bridge = waiter.handoff().await.unwrap();
        for expected in ["abcd", "ef"].into_iter().take(reads) {
            assert_eq!(chunk(&mut bridge).await.unwrap().unwrap(), expected);
        }
        drop(bridge); // The HTTP/1 encoder stops at Content-Length, without requesting None.
        let snapshot = tokio::time::timeout(Duration::from_secs(2), app.request_registry().wait())
            .await
            .expect("source and scope must terminate without another body poll");
        assert!(snapshot.is_terminal());
        assert_eq!(polls.load(Ordering::Acquire), usize::from(reads > 0));
        assert_eq!(context.cancellation().is_cancelled(), !completed);
        assert_eq!(
            lock(&context.0.evidence).response.body(),
            Some(if completed {
                ResponseBodyOutcome::Completed
            } else {
                ResponseBodyOutcome::Interrupted
            })
        );
        assert!(lock(&context.0.evidence).response.resources_terminal());
        assert_eq!(app.container().active_scope_count(), 0);
        {
            let events = lock(&state.events);
            let released = events
                .iter()
                .position(|(_, event)| *event == "source-released")
                .unwrap();
            let disposed = events
                .iter()
                .position(|(_, event)| *event == "dispose")
                .unwrap();
            assert!(released < disposed);
            assert_eq!(
                events
                    .iter()
                    .filter(|(_, event)| *event == "source-released")
                    .count(),
                1
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|(_, event)| *event == "dispose")
                    .count(),
                1
            );
        }
        finish(&app, true).await;
    }
}

#[tokio::test]
async fn exact_length_bridge_drop_cancels_an_outstanding_eof_poll() {
    let (app, state) = application(true).await;
    let source = Source {
        state: state.clone(),
        polls: Arc::new(AtomicUsize::new(0)),
        pending: false,
        panic: false,
    };
    let waiter = response(&app, None, move |_, _| {
        streaming(source.chain(futures::stream::pending())).content_length(6)
    })
    .await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert_eq!(chunk(&mut bridge).await.unwrap().unwrap(), "abcd");
    assert_eq!(chunk(&mut bridge).await.unwrap().unwrap(), "ef");
    // HTTP/2 can request EOF after the data. That pending source poll must
    // still be cancellable if its protocol worker is then dropped/reset.
    assert!(chunk(&mut bridge).now_or_never().is_none());
    event(&state, "source-polled", 2).await;
    drop(bridge);
    let snapshot = tokio::time::timeout(Duration::from_secs(1), app.request_registry().wait())
        .await
        .expect("peer disconnect must interrupt the pending EOF poll");
    assert!(snapshot.is_terminal());
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::PeerDisconnect)
    );
    assert_eq!(
        lock(&context.0.evidence).response.body(),
        Some(ResponseBodyOutcome::Interrupted)
    );
    assert!(lock(&context.0.evidence).response.resources_terminal());
    assert_eq!(app.container().active_scope_count(), 0);
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn pending_producer_observes_signal_without_another_hyper_poll() {
    let (app, state) = application(true).await;
    let begun = Arc::new(Notify::new());
    let observed = Arc::new(Notify::new());
    let release = CancellationToken::new();
    let (b, o, r) = (begun.clone(), observed.clone(), release.clone());
    let waiter = response(&app, None, move |_, token| {
        streaming(futures::stream::unfold(token, move |token| {
            let (b, o, r) = (b.clone(), o.clone(), r.clone());
            async move {
                b.notify_one();
                token.cancelled().await;
                o.notify_one();
                r.cancelled().await;
                None::<(Result<Bytes, ResponseBodyError>, _)>
            }
        }))
    })
    .await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert!(chunk(&mut bridge).now_or_never().is_none());
    begun.notified().await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    observed.notified().await;
    assert_eq!(app.container().active_scope_count(), 1);
    assert!(!lock(&state.events).iter().any(|(_, e)| *e == "dispose"));
    tokio::time::advance(Duration::from_millis(50)).await;
    release.cancel();
    assert!(app.request_registry().wait().await.is_terminal());
    assert!(chunk(&mut bridge).await.is_none());
    assert_eq!(
        lock(&context.0.evidence).response.body(),
        Some(ResponseBodyOutcome::Completed)
    );
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::ForcedShutdown)
    );
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn force_stops_unpolled_and_ignoring_sources_independently_of_bridge_polls() {
    for start_source in [false, true] {
        let (app, state) = application(true).await;
        let polls = Arc::new(AtomicUsize::new(0));
        let source = Source {
            state: state.clone(),
            polls: polls.clone(),
            pending: true,
            panic: false,
        };
        let waiter = response(&app, None, move |_, _| streaming(source)).await;
        let context = waiter.context.clone();
        let mut bridge = waiter.handoff().await.unwrap();
        if start_source {
            assert!(chunk(&mut bridge).now_or_never().is_none());
            while polls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        }
        let signal_at = Instant::now();
        context.stop(ExecutionStopReason::ForcedShutdown);
        assert!(app.request_registry().wait().await.is_terminal());
        assert_eq!(Instant::now(), signal_at + COOPERATIVE_CANCELLATION_CAP);
        assert_eq!(
            lock(&context.0.evidence).response.body(),
            Some(if start_source {
                ResponseBodyOutcome::Interrupted
            } else {
                ResponseBodyOutcome::NotStarted
            })
        );
        assert_eq!(polls.load(Ordering::Acquire) == 0, !start_source);
        assert_eq!(
            chunk(&mut bridge).await.unwrap().unwrap_err(),
            ResponseBodyError::StreamInterrupted
        );
        assert!(lock(&state.events).iter().any(|(_, e)| *e == "disposed"));
        finish(&app, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn body_uses_the_admitted_deadline_and_shutdown_cannot_restart_its_window() {
    let (app, state) = application(true).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let source = Source {
        state,
        polls: polls.clone(),
        pending: true,
        panic: false,
    };
    let waiter = response(&app, None, move |_, _| streaming(source)).await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert!(chunk(&mut bridge).now_or_never().is_none());
    while polls.load(Ordering::Acquire) == 0 {
        tokio::task::yield_now().await;
    }
    context.cancellation().cancelled().await;
    assert_eq!(Instant::now(), context.execution_deadline());
    tokio::time::advance(Duration::from_millis(100)).await;
    let root = app.shutdown_budget().begin();
    context.stop(ExecutionStopReason::ForcedShutdown);
    assert!(app.request_registry().wait().await.is_terminal());
    assert!(Instant::now() <= root.at(ShutdownStage::Cooperative));
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::RequestTimeout)
    );
    assert_eq!(
        Instant::now(),
        context.execution_deadline() + COOPERATIVE_CANCELLATION_CAP
    );
    assert_eq!(
        lock(&context.0.evidence).response.body(),
        Some(ResponseBodyOutcome::Interrupted)
    );
    finish(&app, true).await;
}

#[tokio::test]
async fn producer_poll_panic_releases_source_before_scope_and_reports_panicked() {
    let (app, state) = application(true).await;
    let source = Source {
        state,
        polls: Arc::new(AtomicUsize::new(0)),
        pending: false,
        panic: true,
    };
    let waiter = response(&app, None, move |_, _| streaming(source)).await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert_eq!(
        chunk(&mut bridge).await.unwrap().unwrap_err(),
        ResponseBodyError::StreamSourceFailed
    );
    assert!(app.request_registry().wait().await.is_terminal());
    assert_eq!(
        lock(&context.0.evidence).response.body(),
        Some(ResponseBodyOutcome::Panicked)
    );
    assert!(lock(&context.0.evidence).response.resources_terminal());
    finish(&app, true).await;
}

struct BlockingSourceDrop(BlockingExecutionDrop);
impl Stream for BlockingSourceDrop {
    type Item = Result<Bytes, ResponseBodyError>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let _ = &self.0;
        Poll::Ready(None)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_and_blocked_source_destructor_cannot_authorize_di_cleanup() {
    let (app, state) = application(true).await;
    let (dropping, dropped) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let release = ReleaseBlockedDrop(Some(release));
    let source = BlockingSourceDrop(BlockingExecutionDrop {
        started: Some(dropping),
        release: released,
    });
    let waiter = response(&app, None, move |_, _| streaming(source)).await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert!(chunk(&mut bridge).now_or_never().is_none());
    dropped.await.unwrap();
    assert_eq!(
        lock(&context.0.evidence).response.body(),
        Some(ResponseBodyOutcome::Completed)
    );
    assert!(!lock(&context.0.evidence).response.resources_terminal());
    assert!(context.close_scope().await.is_err());
    assert_eq!(app.container().active_scope_count(), 1);
    assert!(!lock(&state.events).iter().any(|(_, e)| *e == "dispose"));
    assert!(app.request_registry().wait().now_or_never().is_none());
    drop(release);
    assert!(app.request_registry().wait().await.is_terminal());
    assert!(chunk(&mut bridge).await.is_none());
    finish(&app, true).await;
}

struct Input(bool);
#[async_trait::async_trait]
impl RequestBodyStream for Input {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
        if std::mem::replace(&mut self.0, false) {
            Ok(Some(Bytes::from_static(b"input")))
        } else {
            futures::future::pending().await
        }
    }
    fn size_hint(&self) -> (u64, Option<u64>) {
        (0, None)
    }
}

#[tokio::test(start_paused = true)]
async fn input_ownership_follows_reader_into_returned_body_until_source_drop() {
    let (app, _) = application(true).await;
    let waiter = response(&app, Some(Box::new(Input(true))), |request, _| {
        let reader = request.take_body_reader().unwrap();
        streaming(futures::stream::try_unfold(
            reader,
            |mut reader| async move {
                reader
                    .next_chunk()
                    .await
                    .map(|chunk| chunk.map(|chunk| (chunk, reader)))
                    .map_err(|_| ResponseBodyError::StreamSourceFailed)
            },
        ))
    })
    .await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert_eq!(context.0.children.snapshot().inputs_outstanding, 1);
    assert_eq!(chunk(&mut bridge).await.unwrap().unwrap(), "inpu");
    assert_eq!(chunk(&mut bridge).await.unwrap().unwrap(), "t");
    assert_eq!(context.0.children.snapshot().inputs_outstanding, 1);
    assert!(chunk(&mut bridge).now_or_never().is_none());
    drop(bridge);
    assert!(app.request_registry().wait().await.is_terminal());
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::PeerDisconnect)
    );
    assert!(context.0.children.snapshot().terminal());
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn escaped_input_receipt_blocks_scope_and_preserves_wait_failure() {
    let (app, _) = application(true).await;
    let escaped = Arc::new(Mutex::new(None));
    let target = escaped.clone();
    let waiter = response(&app, Some(Box::new(Input(true))), move |request, _| {
        *lock(&target) = Some(request.take_body_reader().unwrap());
        streaming(futures::stream::empty::<Result<Bytes, ResponseBodyError>>())
    })
    .await;
    let context = waiter.context.clone();
    let join = waiter.join.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert!(chunk(&mut bridge).await.is_none());
    join.await.unwrap();
    assert_eq!(app.container().active_scope_count(), 1);
    assert_eq!(app.request_registry().snapshot().outstanding, 1);
    assert!(lock(&context.0.evidence).cleanup_wait_failed);
    assert_eq!(context.0.children.snapshot().inputs_outstanding, 1);
    // Releasing the escaped reader is enough: the registry must resume the
    // retained finalizer itself after its original owner task has returned.
    // Late termination cannot erase the original failure.
    lock(&escaped).take();
    let snapshot = app.request_registry().wait().await;
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.cleanup_failed, 1);
    // The request failed and retired before the shutdown cohort was fixed.
    app.close().await.unwrap();
    assert_eq!(app.request_registry().attempt_snapshot().cleanup_failed, 0);
    // DI also retains the original expired cleanup result after termination.
    assert!(app.container().close().await.is_err());
}

#[tokio::test(start_paused = true)]
async fn sse_keep_alive_remains_bounded_and_releases_its_captured_source() {
    let (app, state) = application(true).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let source = Source {
        state: state.clone(),
        polls: polls.clone(),
        pending: true,
        panic: false,
    };
    let waiter = response(&app, None, move |_, _| {
        lily_web_core::SseResponse::new(
            source.map(|item| item.map(|_| lily_web_core::SseEvent::new("event").unwrap())),
        )
        .keep_alive(Duration::from_secs(1))
        .unwrap()
    })
    .await;
    let context = waiter.context.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert_eq!(polls.load(Ordering::Acquire), 0);
    assert!(chunk(&mut bridge).await.unwrap().unwrap().len() <= 4);
    assert!(polls.load(Ordering::Acquire) > 0);
    assert_eq!(app.container().active_scope_count(), 1);
    context.stop(ExecutionStopReason::ForcedShutdown);
    assert!(app.request_registry().wait().await.is_terminal());
    assert!(lock(&state.events)
        .iter()
        .any(|(_, e)| *e == "source-released"));
    assert_eq!(
        lock(&context.0.evidence).response.body(),
        Some(ResponseBodyOutcome::Interrupted)
    );
    finish(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn protocol_bytes_cannot_retain_scoped_user_destructors() {
    use std::sync::atomic::AtomicBool;
    struct ByteOwner(Arc<AtomicBool>, bool);
    impl AsRef<[u8]> for ByteOwner {
        fn as_ref(&self) -> &[u8] {
            b"data"
        }
    }
    impl Drop for ByteOwner {
        fn drop(&mut self) {
            assert!(ProcessContext::current().is_some());
            self.0.store(true, Ordering::Release);
            assert!(!self.1, "custom byte owner destructor failed");
        }
    }
    let (app, _) = application(true).await;
    let released = Arc::new(AtomicBool::new(false));
    let byte_owner = ByteOwner(released.clone(), false);
    let waiter = response(&app, None, move |_, _| {
        streaming(futures::stream::once(async move {
            Ok::<_, ResponseBodyError>(Bytes::from_owner(byte_owner))
        }))
    })
    .await;
    let mut bridge = waiter.handoff().await.unwrap();
    let bytes = chunk(&mut bridge).await.unwrap().unwrap();
    assert_eq!(bytes, "data");
    assert!(released.load(Ordering::Acquire));
    assert!(chunk(&mut bridge).await.is_none());
    assert!(app.request_registry().wait().await.is_terminal());
    // Still holding protocol bytes while DI has closed is safe.
    assert_eq!(app.container().active_scope_count(), 0);
    drop(bytes);
    finish(&app, true).await;

    // An actual custom allocation destructor failure cannot be mistaken for
    // a normal source poll panic followed by successful resource destruction.
    let (app, _) = application(true).await;
    let byte_owner = ByteOwner(Arc::new(AtomicBool::new(false)), true);
    let waiter = response(&app, None, move |_, _| {
        streaming(futures::stream::once(async move {
            Ok::<_, ResponseBodyError>(Bytes::from_owner(byte_owner))
        }))
    })
    .await;
    let context = waiter.context.clone();
    let join = waiter.join.clone();
    let mut bridge = waiter.handoff().await.unwrap();
    assert_eq!(
        chunk(&mut bridge).await.unwrap().unwrap_err(),
        ResponseBodyError::StreamSourceFailed
    );
    join.await.unwrap();
    assert!(lock(&context.0.evidence).resource_release_failed);
    assert!(context.close_scope().await.is_err());
    assert_eq!(app.container().active_scope_count(), 1);
    assert_eq!(app.request_registry().snapshot().outstanding, 1);
    assert!(app.close().await.is_err());
    // Explicit caller-owned test teardown after proving HTTP refused disposal.
    // No HTTP evidence is changed into success by this external cleanup.
    let resources = context.0.resources.lock().await.take().unwrap();
    let mut scope = resources.scope.unwrap();
    ProcessContext::scope(scope.context().clone(), async {
        drop(resources.request);
        drop(resources.response);
    })
    .await;
    scope.close().await.unwrap();
    app.container().close().await.unwrap();
}

struct WireBodyMiddleware(Arc<Extensions>);
#[async_trait::async_trait]
impl HttpMiddleware for WireBodyMiddleware {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self(extensions))
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("phase5-wire-body", crate::MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        _next: HttpNext<'_>,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        let state = self
            .0
            .get_service::<ScopedProbe>(None)
            .await
            .unwrap()
            .owner
            .state
            .clone();
        let source = Source {
            state: state.clone(),
            polls: Arc::new(AtomicUsize::new(0)),
            pending: exchange.request().path() == "/pending",
            panic: exchange.request().path() == "/panic",
        };
        let path = exchange.request().path().to_string();
        let (request, response) = exchange.parts_mut();
        let (status, reason) = match path.as_str() {
            "/204" => (204, "No Content"),
            "/304" => (304, "Not Modified"),
            _ => (200, "OK"),
        };
        streaming(source)
            .status(status, reason)
            .write_to_response(response, request)
            .await
            .unwrap();
        Ok(())
    }
}

async fn wire_application(protocol: lily_core::enums::HttpProtocol) -> (Arc<App>, Arc<ProbeState>) {
    let app = Arc::new(
        crate::AppBuilder::new("127.0.0.1:0")
            .protocol(protocol)
            .middleware::<OwnerMiddleware>()
            .middleware::<WireBodyMiddleware>()
            .build()
            .await
            .unwrap(),
    );
    let state = app
        .container()
        .resolve::<OwnerProbe>(None)
        .await
        .unwrap()
        .state
        .clone();
    (app, state)
}

#[tokio::test]
async fn http1_suppressed_sources_release_unpolled_before_scope_on_keep_alive() {
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::TokioIo;
    let (app, state) = wire_application(lily_core::enums::HttpProtocol::Http1_1).await;
    let root = start_server(&app).await;
    let socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    for (method, path, status) in [
        ("HEAD", "/head", 200),
        ("GET", "/204", 204),
        ("GET", "/304", 304),
    ] {
        let before = lock(&state.events)
            .iter()
            .filter(|(_, e)| *e == "source-released")
            .count();
        let response = client
            .send_request(
                hyper::Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        assert!(response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty());
        assert!(app.request_registry().wait().await.is_terminal());
        assert_eq!(app.container().active_scope_count(), 0);
        assert_eq!(
            lock(&state.events)
                .iter()
                .filter(|(_, e)| *e == "source-released")
                .count(),
            before + 1
        );
    }
    {
        let events = lock(&state.events);
        assert!(!events.iter().any(|(_, e)| *e == "source-polled"));
        assert_eq!(events.iter().filter(|(_, e)| *e == "disposed").count(), 3);
    }
    drop(client);
    driver.await.unwrap().unwrap();
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
}

#[tokio::test]
async fn http2_body_failure_is_stream_local_and_request_scopes_release_independently() {
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    let (app, _) = wire_application(lily_core::enums::HttpProtocol::Http2).await;
    let root = start_server(&app).await;
    let socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(socket))
            .await
            .unwrap();
    let driver = tokio::spawn(connection);
    let mut pending = client.clone();
    let pending = pending.send_request(
        hyper::Request::builder()
            .uri("/pending")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    );
    let mut broken = client.clone();
    let broken = broken.send_request(
        hyper::Request::builder()
            .uri("/panic")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    );
    let (pending, broken) = tokio::join!(pending, broken);
    let pending = pending.unwrap();
    if let Ok(response) = broken {
        assert!(response.into_body().collect().await.is_err());
    }
    let healthy = client
        .send_request(
            hyper::Request::builder()
                .uri("/healthy")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        healthy.into_body().collect().await.unwrap().to_bytes(),
        "abcdef"
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while app.request_registry().snapshot().scopes_terminal < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // One pending sibling still owns its generation; two independent streams
    // released theirs without waiting for the shared TCP connection.
    assert_eq!(app.container().active_scope_count(), 1);
    drop(pending);
    assert!(app.request_registry().wait().await.is_terminal());
    drop(client);
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
    driver.await.unwrap().unwrap();
}
