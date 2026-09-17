//! Framework HTTP resources which survive a cancelled caller future.
//!
//! This is an internal cross-crate seam. Raw application tasks are not adopted.

#![allow(missing_docs)]

use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use std::{
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{oneshot, Notify},
    task::AbortHandle,
};

#[derive(Clone, Copy)]
enum HelperOutcome {
    Completed,
    Cancelled,
    Panicked,
}
type Join = Shared<BoxFuture<'static, HelperOutcome>>;

#[derive(Default)]
struct State {
    helpers: Vec<(Join, AbortHandle, bool)>,
    registered: usize,
    joined: usize,
    failed: usize,
    abort_requested: usize,
    cancelled: usize,
    inputs: usize,
    release_failed: bool,
    sealed: bool,
}

/// Actual helper joins and input-source releases for one HTTP owner.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct HttpResources(Arc<ResourcesInner>);

#[derive(Default)]
struct ResourcesInner {
    state: Mutex<State>,
    changed: Notify,
}

/// A resource observation; requests and aborts do not constitute termination.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct HttpResourceSnapshot {
    pub helpers_registered: usize,
    pub helpers_joined: usize,
    pub helpers_failed: usize,
    pub helpers_abort_requested: usize,
    pub helpers_cancelled: usize,
    pub inputs_outstanding: usize,
    pub release_failed: bool,
}

impl HttpResourceSnapshot {
    pub fn terminal(self) -> bool {
        self.helpers_registered == self.helpers_joined
            && self.inputs_outstanding == 0
            && !self.release_failed
    }
}

tokio::task_local! { static CURRENT: HttpResources; }

impl HttpResources {
    /// Does not propagate into raw user-spawned tasks.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        CURRENT.scope(self.clone(), future).await
    }

    pub fn snapshot(&self) -> HttpResourceSnapshot {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut joined = 0;
        let mut failed = 0;
        let mut cancelled = 0;
        state
            .helpers
            .retain(|(join, _, _)| match join.clone().now_or_never() {
                None => true,
                Some(outcome) => {
                    joined += 1;
                    failed += usize::from(matches!(outcome, HelperOutcome::Panicked));
                    cancelled += usize::from(matches!(outcome, HelperOutcome::Cancelled));
                    false
                }
            });
        state.joined += joined;
        state.failed += failed;
        state.cancelled += cancelled;
        HttpResourceSnapshot {
            helpers_registered: state.registered,
            helpers_joined: state.joined,
            helpers_failed: state.failed,
            helpers_abort_requested: state.abort_requested,
            helpers_cancelled: state.cancelled,
            inputs_outstanding: state.inputs,
            release_failed: state.release_failed,
        }
    }

    /// Only after execution/source destruction, so no framework producer can
    /// publish another helper. A started blocking operation cannot be preempted.
    pub fn seal(&self, abort: bool) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        state.sealed = true;
        if abort {
            let mut requested = 0;
            for (join, handle, abort_requested) in &mut state.helpers {
                if !*abort_requested && join.clone().now_or_never().is_none() {
                    *abort_requested = true;
                    requested += 1;
                    handle.abort();
                }
            }
            state.abort_requested += requested;
        }
    }

    /// The HTTP owner bounds this retained observation with its existing budget.
    pub async fn wait(&self) -> HttpResourceSnapshot {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let snapshot = self.snapshot();
            if snapshot.terminal() || snapshot.release_failed {
                return snapshot;
            }
            let joins = self
                .0
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .helpers
                .iter()
                .map(|(join, _, _)| join.clone())
                .collect::<Vec<_>>();
            if joins.is_empty() {
                changed.await;
            } else {
                futures::future::join_all(joins).await;
            }
        }
    }

    pub fn track_input(
        &self,
        input: Box<dyn crate::RequestBodyStream>,
    ) -> Box<dyn crate::RequestBodyStream> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .inputs += 1;
        Box::new(TrackedInput {
            input: Some(input),
            resources: self.clone(),
        })
    }
}

struct TrackedInput {
    input: Option<Box<dyn crate::RequestBodyStream>>,
    resources: HttpResources,
}
impl Drop for TrackedInput {
    fn drop(&mut self) {
        // Observe this particular destructor, independently of a surrounding
        // handler panic. A blocked drop cannot publish release; a panic fails
        // closed without erasing the owner's other receipts.
        let released = catch_unwind(AssertUnwindSafe(|| drop(self.input.take()))).is_ok();
        let mut state = self
            .resources
            .0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if released {
            state.inputs -= 1;
        } else {
            state.release_failed = true;
        }
        self.resources.0.changed.notify_waiters();
    }
}
#[async_trait::async_trait]
impl crate::RequestBodyStream for TrackedInput {
    async fn next_chunk(&mut self) -> Result<Option<bytes::Bytes>, crate::RequestBodyError> {
        self.input.as_mut().unwrap().next_chunk().await
    }
    fn size_hint(&self) -> (u64, Option<u64>) {
        self.input.as_ref().unwrap().size_hint()
    }
}

/// Framework file operations publish their actual join before executing. The
/// typed result is dropped inside the worker if its original caller disappeared.
pub(crate) async fn blocking<F, T>(work: F) -> Result<T, ()>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let resources = CURRENT.try_with(Clone::clone).ok();
    let Some(resources) = resources else {
        return tokio::task::spawn_blocking(work).await.map_err(|_| ());
    };
    resources.snapshot();
    let (publish, published) = oneshot::channel();
    let (send, receive) = oneshot::channel();
    {
        let mut state = resources.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.sealed {
            return Err(());
        }
        let task = tokio::task::spawn_blocking(move || {
            if published.blocking_recv().is_ok() {
                // Do not retain T in a completed join receipt or another task.
                drop(send.send(work()));
            }
        });
        let abort = task.abort_handle();
        let join = async move {
            match task.await {
                Ok(()) => HelperOutcome::Completed,
                Err(error) if error.is_cancelled() => HelperOutcome::Cancelled,
                Err(_) => HelperOutcome::Panicked,
            }
        }
        .boxed()
        .shared();
        state.registered += 1;
        state.helpers.push((join, abort, false));
    }
    let _ = publish.send(());
    receive.await.map_err(|_| ())
}

#[cfg(test)]
mod tests;
