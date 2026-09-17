use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_caller_retains_started_blocking_join() {
    let resources = HttpResources::default();
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let caller = tokio::spawn({
        let resources = resources.clone();
        async move {
            resources
                .scope(blocking(move || {
                    entered.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                }))
                .await
        }
    });
    entered_rx.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    resources.seal(true);
    assert_eq!(resources.snapshot().helpers_registered, 1);
    assert_eq!(resources.snapshot().helpers_abort_requested, 1);
    assert_eq!(resources.snapshot().helpers_cancelled, 0);
    assert_eq!(resources.snapshot().helpers_joined, 0);
    assert!(resources.wait().now_or_never().is_none());
    release.send(()).unwrap();
    let snapshot = resources.wait().await;
    assert!(snapshot.terminal());
    assert_eq!(snapshot.helpers_joined, 1);
    assert_eq!(snapshot.helpers_failed, 0);
    assert_eq!(snapshot.helpers_cancelled, 0); // started blocking work returned normally
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_helper_result_is_destroyed_before_actual_join() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct ResultDrop(Arc<AtomicBool>);
    impl Drop for ResultDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let resources = HttpResources::default();
    let dropped = Arc::new(AtomicBool::new(false));
    let result = ResultDrop(dropped.clone());
    let (send, entered) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let caller = tokio::spawn({
        let resources = resources.clone();
        async move {
            resources
                .scope(blocking(move || {
                    send.send(()).unwrap();
                    released
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    result
                }))
                .await
        }
    });
    entered.await.unwrap();
    caller.abort();
    assert!(caller.await.is_err());
    resources.seal(true);
    assert!(!resources.snapshot().terminal());
    assert!(!dropped.load(Ordering::Acquire));
    release.send(()).unwrap();
    assert!(resources.wait().await.terminal());
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn actual_static_file_open_and_lazy_reads_retain_helper_joins() {
    use crate::{IntoResponse, Request, Response, StaticFileMount, TransportResponseBody};
    use futures::StreamExt;
    let dir = tempfile::tempdir().unwrap();
    let data = vec![b'x'; crate::STATIC_FILE_CHUNK_BYTES * 2 + 19];
    std::fs::write(dir.path().join("file.bin"), &data).unwrap();
    let mount = StaticFileMount::new(dir.path(), "/files").await.unwrap();
    let resources = HttpResources::default();
    resources
        .scope(async {
            let mut request =
                Request::from_transport_parts("GET".into(), "/files/file.bin".into(), vec![], &[])
                    .await
                    .unwrap();
            let file = mount.serve(&request).await.unwrap();
            assert_eq!(resources.snapshot().helpers_registered, 1); // no speculative read
            let mut response = Response::new().await.unwrap();
            file.write_to_response(&mut response, &mut request)
                .await
                .unwrap();
            let TransportResponseBody::Stream(mut stream) =
                response.into_transport_parts().unwrap().into_body()
            else {
                panic!("stream expected")
            };
            let mut received = Vec::new();
            while let Some(chunk) = stream.next().await {
                received.extend_from_slice(&chunk.unwrap());
            }
            drop(stream);
            assert_eq!(received, data);
            assert_eq!(resources.snapshot().helpers_registered, 4); // open + 3 bounded reads
        })
        .await;
    resources.seal(false);
    let snapshot = resources.wait().await;
    assert!(snapshot.terminal());
    assert_eq!(snapshot.helpers_joined, 4);
    assert_eq!(snapshot.helpers_failed, 0);
}

struct InputDrop(bool);
#[async_trait::async_trait]
impl crate::RequestBodyStream for InputDrop {
    async fn next_chunk(&mut self) -> Result<Option<bytes::Bytes>, crate::RequestBodyError> {
        Ok(None)
    }
    fn size_hint(&self) -> (u64, Option<u64>) {
        (0, Some(0))
    }
}
impl Drop for InputDrop {
    fn drop(&mut self) {
        assert!(!self.0, "input destructor panic");
    }
}

#[test]
fn input_release_distinguishes_handler_unwind_from_actual_destructor_failure() {
    let resources = HttpResources::default();
    let input = resources.track_input(Box::new(InputDrop(false)));
    assert!(catch_unwind(AssertUnwindSafe(move || {
        let _input = input;
        panic!("handler panic");
    }))
    .is_err());
    assert!(resources.snapshot().terminal());
    drop(resources.track_input(Box::new(InputDrop(true))));
    assert!(resources.snapshot().release_failed);
    assert!(!resources.snapshot().terminal());
}

#[tokio::test]
async fn helper_panic_is_observed_and_does_not_become_success() {
    let resources = HttpResources::default();
    assert!(resources
        .scope(blocking(|| panic!("file helper panic")))
        .await
        .is_err());
    resources.seal(false);
    let snapshot = resources.wait().await;
    assert!(snapshot.terminal());
    assert_eq!(snapshot.helpers_failed, 1);
}
