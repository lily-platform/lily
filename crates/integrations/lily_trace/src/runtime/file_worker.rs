//! Bounded JSONL queue with an owned, joinable OS worker.

use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use futures_util::FutureExt;

use super::tasks::{self, Receipt};

#[derive(Clone, Default)]
pub(super) struct ErrorCounter(Arc<AtomicUsize>);

impl ErrorCounter {
    pub(super) fn dropped_lines(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
    fn increment(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1));
    }
}

type Sender = Arc<Mutex<Option<mpsc::SyncSender<Vec<u8>>>>>;

#[derive(Clone)]
pub(super) struct NonBlocking {
    sender: Sender,
    rejected: ErrorCounter,
}

impl NonBlocking {
    pub(super) fn error_counter(&self) -> ErrorCounter {
        self.rejected.clone()
    }
}

impl Write for NonBlocking {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let sender = self.sender.lock().unwrap_or_else(|p| p.into_inner());
        if sender
            .as_ref()
            .is_none_or(|sender| sender.try_send(bytes.to_vec()).is_err())
        {
            self.rejected.increment();
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) struct FileWorker {
    sender: Sender,
    pub(super) receipt: Receipt<()>,
}

impl FileWorker {
    pub(super) fn start(
        mut writer: impl Write + Send + 'static,
        capacity: usize,
    ) -> io::Result<(NonBlocking, Self)> {
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(capacity);
        let task = std::thread::Builder::new()
            .name("lily-trace-file".into())
            .spawn(move || {
                let mut failure = None;
                for record in receiver {
                    if let Err(error) = writer.write_all(&record) {
                        failure = Some(error.to_string());
                    }
                    if let Err(error) = writer.flush() {
                        failure = Some(error.to_string());
                    }
                }
                if let Err(error) = writer.flush() {
                    failure = Some(error.to_string());
                }
                // Writer and receiver destruction happens before the actual join.
                failure.map_or(Ok(()), Err)
            })?;
        let thread = tasks::thread_receipt("file", task);
        let receipt = tasks::retain("file", async move { thread.await? }.boxed().shared());
        let sender = Arc::new(Mutex::new(Some(sender)));
        Ok((
            NonBlocking {
                sender: sender.clone(),
                rejected: ErrorCounter::default(),
            },
            Self { sender, receipt },
        ))
    }

    pub(super) fn close(&self) {
        // Every writer clone shares this gate; none can enqueue after close.
        self.sender.lock().unwrap_or_else(|p| p.into_inner()).take();
    }
}

impl Drop for FileWorker {
    fn drop(&mut self) {
        self.close();
        // The process inventory retains the actual thread receipt even on
        // initialization rollback or if a shutdown observer disappears.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    struct PendingWriter {
        started: Option<tokio::sync::oneshot::Sender<()>>,
        release: mpsc::Receiver<()>,
    }
    impl Write for PendingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.started.take().unwrap().send(()).unwrap();
            self.release.recv().unwrap();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn closing_file_queue_does_not_claim_a_blocked_thread_join() {
        let (release, wait) = mpsc::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (mut writer, worker) = FileWorker::start(
            PendingWriter {
                started: Some(started),
                release: wait,
            },
            1,
        )
        .unwrap();
        writer.write_all(b"record").unwrap();
        ready.await.unwrap();
        let receipt = worker.receipt.clone();
        drop(worker);
        assert!(tasks::observe_before(Instant::now(), receipt.clone())
            .await
            .is_none());
        writer.write_all(b"late").unwrap();
        assert_eq!(writer.error_counter().dropped_lines(), 1);
        release.send(()).unwrap();
        receipt.await.unwrap();
    }
}
