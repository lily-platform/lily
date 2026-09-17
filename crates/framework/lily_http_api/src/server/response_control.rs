//! Request-specific protocol authority. It contains no user callback or scope.
//!
//! Commit means the final response is being returned to Hyper's encoder; it
//! does not mean headers reached the peer. A body frame/EOF handed to Hyper is
//! separate from the protocol worker's actual join and from HTTP/1 I/O flush.

use std::fmt;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, OnceLock, Weak,
};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::task::AtomicWaker;
use http::StatusCode;
use hyper::Version;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::server::ConnectionCloseReason;
use crate::lifecycle::ExecutionStopReason;
use crate::request_lifecycle::deadline::{RequestDeadline, RESPONSE_FINALIZATION_CAP};
use crate::shutdown::{ShutdownBudget, ShutdownStage};
use crate::tasks::{TaskReceipt, TaskSnapshot};

type ConnectionTask = TaskReceipt<std::io::Result<ConnectionCloseReason>>;

tokio::task_local! {
    pub(super) static PROTOCOL_TASK: TaskReceipt<()>;
    pub(super) static CONNECTION_TASK: ConnectionTask;
}

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The inline driver releases its I/O before setting `released`. Managed
/// connections additionally retain the real enclosing task's join receipt.
pub(super) struct ConnectionReceipt {
    pub(super) stop: CancellationToken,
    released: AtomicBool,
    task: Option<ConnectionTask>,
    stop_reason: OnceLock<ExecutionStopReason>,
    flush_generation: AtomicU64,
    driver_waker: AtomicWaker,
}

impl fmt::Debug for ConnectionReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionReceipt")
            .field("released", &self.released.load(Ordering::Acquire))
            .field("managed", &self.task.is_some())
            .finish()
    }
}

impl ConnectionReceipt {
    pub(super) fn current() -> Arc<Self> {
        Arc::new(Self {
            stop: CancellationToken::new(),
            released: AtomicBool::new(false),
            task: CONNECTION_TASK.try_with(Clone::clone).ok(),
            stop_reason: OnceLock::new(),
            flush_generation: AtomicU64::new(0),
            driver_waker: AtomicWaker::new(),
        })
    }

    pub(super) fn release(&self) {
        self.released.store(true, Ordering::Release);
    }

    fn request_stop(&self, reason: ExecutionStopReason) {
        let _ = self.stop_reason.set(reason);
        self.stop.cancel();
    }

    pub(super) fn close_reason(&self) -> ConnectionCloseReason {
        match self
            .stop_reason
            .get()
            .expect("reason published before stop")
        {
            ExecutionStopReason::ResponseFinalizationTimeout => {
                ConnectionCloseReason::ResponseFinalizationTimeout
            }
            reason => ConnectionCloseReason::ResponseStopped(*reason),
        }
    }

    fn joined(&self) -> Option<TaskSnapshot> {
        self.task.as_ref().map(TaskReceipt::snapshot)
    }

    pub(super) fn register_driver(&self, cx: &Context<'_>) {
        self.driver_waker.register(cx.waker());
    }

    pub(super) fn observe_flush(&self) {
        self.flush_generation.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseCommit {
    Uncommitted,
    CommittedToProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseFrames {
    Pending,
    Completed,
    Failed,
    Dropped,
}

#[derive(Clone)]
enum Target {
    Http1(Arc<ConnectionReceipt>),
    Http2(TaskReceipt<()>),
}

struct State {
    commit: ResponseCommit,
    frames: ResponseFrames,
    stop_requested: Option<ExecutionStopReason>,
    http1_flushed: bool,
    data_in_flight: usize,
    flush_after_data: u64,
    eof_wait_started: bool,
    finalization: Option<Instant>,
}

/// Immutable observations. `task` describes protocol execution only, not
/// delivery/acknowledgement of bytes already buffered by h2 or the socket.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResponseTransportSnapshot {
    pub(crate) http2: bool,
    pub(crate) commit: ResponseCommit,
    pub(crate) frames: ResponseFrames,
    pub(crate) stop_requested: Option<ExecutionStopReason>,
    pub(crate) http1_flushed: bool,
    pub(crate) driver_released: bool,
    pub(crate) task: Option<TaskSnapshot>,
}

impl ResponseTransportSnapshot {
    pub(crate) fn is_terminal(self) -> bool {
        if self.http2 {
            self.task.is_some_and(TaskSnapshot::is_terminal)
        } else {
            self.http1_flushed
                || self.driver_released
                || self.task.is_some_and(TaskSnapshot::is_terminal)
        }
    }
}

/// Retired requests keep counters only; live identities keep the controls.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct TransportTally {
    pub(super) registered: usize,
    pub(super) committed: usize,
    pub(super) frames_completed: usize,
    pub(super) stop_requested: usize,
    pub(super) tasks_joined: usize,
    pub(super) tasks_aborted: usize,
    pub(super) outstanding: usize,
}

impl TransportTally {
    pub(super) fn record(&mut self, snapshot: ResponseTransportSnapshot) {
        self.registered += 1;
        self.committed += usize::from(snapshot.commit == ResponseCommit::CommittedToProtocol);
        self.frames_completed += usize::from(snapshot.frames == ResponseFrames::Completed);
        self.stop_requested += usize::from(snapshot.stop_requested.is_some());
        self.tasks_joined += usize::from(snapshot.task.is_some_and(TaskSnapshot::is_terminal));
        self.tasks_aborted += snapshot.task.map_or(0, |task| task.cancelled);
        self.outstanding += usize::from(!snapshot.is_terminal());
    }
}

struct Control {
    target: Target,
    connection: Arc<ConnectionReceipt>,
    state: Mutex<State>,
    eof_waker: AtomicWaker,
    deadline: OnceLock<RequestDeadline>,
    root: OnceLock<ShutdownBudget>,
    changed: Notify,
    // Body producers observe this request's reason, not an unrelated H2
    // sibling's write failure. Connection failures propagate to every member.
    reason: Arc<Mutex<ExecutionStopReason>>,
}

#[derive(Clone)]
pub(crate) struct ResponseTransportControl(Arc<Control>);

impl fmt::Debug for ResponseTransportControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseTransportControl")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl ResponseTransportControl {
    pub(super) fn bind(version: Version, connection: Arc<ConnectionReceipt>) -> Self {
        let target = if version == Version::HTTP_2 {
            Target::Http2(
                PROTOCOL_TASK
                    .try_with(Clone::clone)
                    .expect("managed HTTP/2 service must run inside its registered protocol task"),
            )
        } else {
            Target::Http1(connection.clone())
        };
        Self(Arc::new(Control {
            target,
            connection,
            state: Mutex::new(State {
                commit: ResponseCommit::Uncommitted,
                frames: ResponseFrames::Pending,
                stop_requested: None,
                http1_flushed: false,
                data_in_flight: 0,
                flush_after_data: 0,
                eof_wait_started: false,
                finalization: None,
            }),
            eof_waker: AtomicWaker::new(),
            deadline: OnceLock::new(),
            root: OnceLock::new(),
            changed: Notify::new(),
            reason: Arc::new(Mutex::new(ExecutionStopReason::PeerDisconnect)),
        }))
    }

    /// Linearize final response selection with a transport stop. Once this
    /// succeeds, an alternate 504/503 cannot replace the response. Hyper may
    /// still be encoding/buffering its headers; no wire-delivery claim is made.
    pub(super) fn commit(&self) -> Result<(), ResponseStopped> {
        self.commit_selected(false)
    }

    pub(crate) fn bind_deadline(&self, deadline: RequestDeadline) {
        assert!(
            self.0.deadline.set(deadline).is_ok(),
            "one request clock per transport"
        );
        self.0.changed.notify_waiters();
    }

    pub(super) fn bind_root(&self, root: ShutdownBudget) {
        assert!(
            self.0.root.set(root).is_ok(),
            "one root budget per transport"
        );
        self.0.changed.notify_waiters();
    }

    fn root(&self) -> Option<&ShutdownBudget> {
        self.0
            .root
            .get()
            .or_else(|| self.0.deadline.get().map(RequestDeadline::root))
    }

    fn finalization_deadline(&self, state: &mut State) -> Instant {
        let local = *state.finalization.get_or_insert_with(|| {
            let now = Instant::now();
            let started = self
                .0
                .deadline
                .get()
                .and_then(RequestDeadline::cooperative_deadline)
                .filter(|at| *at <= now)
                .unwrap_or(now);
            started + RESPONSE_FINALIZATION_CAP
        });
        self.root()
            .and_then(ShutdownBudget::deadlines)
            .map_or(local, |root| {
                local.min(root.at(ShutdownStage::TransportStop))
            })
    }

    /// Rejections and framework fallbacks get one short absolute finalization
    /// allowance. Repeated selection or a later root can never restart it.
    pub(super) fn begin_finalization(&self) {
        self.finalization_deadline(&mut lock(&self.0.state));
        self.0.changed.notify_waiters();
    }

    fn timeout_status(&self) -> StatusCode {
        match self.0.deadline.get().and_then(RequestDeadline::reason) {
            Some(ExecutionStopReason::RequestTimeout) => StatusCode::GATEWAY_TIMEOUT,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub(super) fn commit_fallback(&self) -> Result<(), ResponseStopped> {
        self.commit_selected(true)
    }

    fn commit_selected(&self, fallback: bool) -> Result<(), ResponseStopped> {
        let expired = self
            .0
            .deadline
            .get()
            .is_some_and(RequestDeadline::cooperative_expired);
        let mut state = lock(&self.0.state);
        if state.stop_requested.is_some() {
            return Err(ResponseStopped::Stopped);
        }
        if fallback || expired || state.finalization.is_some() {
            if Instant::now() >= self.finalization_deadline(&mut state) {
                return Err(ResponseStopped::Stopped);
            }
            if expired && !fallback {
                self.0.changed.notify_waiters();
                return Err(ResponseStopped::FallbackRequired(self.timeout_status()));
            }
        }
        assert_eq!(
            state.commit,
            ResponseCommit::Uncommitted,
            "one final response per request"
        );
        state.commit = ResponseCommit::CommittedToProtocol;
        self.0.changed.notify_waiters();
        Ok(())
    }

    /// Polled inline by the connection's retained watchdog. This future owns
    /// no execution slot and creates no task; it only signals the common clock
    /// and eventually requests a protocol stop. Actual joins remain separate.
    pub(super) async fn enforce_deadline(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.snapshot().stop_requested.is_some() {
                return;
            }
            let finalization = lock(&self.0.state).finalization;
            if let Some(deadline) = finalization {
                match self.root() {
                    Some(root) => {
                        root.wait_until(ShutdownStage::TransportStop, Some(deadline))
                            .await
                    }
                    None => tokio::time::sleep_until(deadline).await,
                }
                let reason = self
                    .0
                    .deadline
                    .get()
                    .and_then(RequestDeadline::reason)
                    .unwrap_or(ExecutionStopReason::ResponseFinalizationTimeout);
                self.request_stop(reason);
                return;
            }
            let Some(deadline) = self.0.deadline.get() else {
                changed.await;
                continue;
            };
            tokio::select! {
                biased;
                _ = &mut changed => continue,
                _ = deadline.wait_for_stop() => {},
            }
            tokio::select! {
                biased;
                _ = &mut changed => continue,
                _ = deadline.cooperative_cutoff() => {},
            }
            if self.snapshot().is_terminal() {
                return;
            }
            let mut state = lock(&self.0.state);
            if state.finalization.is_some() {
                continue;
            }
            if state.commit == ResponseCommit::Uncommitted {
                self.finalization_deadline(&mut state);
                continue;
            }
            drop(state);
            self.request_stop(deadline.reason().expect("deadline published stop reason"));
            return;
        }
    }

    pub(super) fn frames_finished(&self, outcome: ResponseFrames) {
        let mut state = lock(&self.0.state);
        if state.frames == ResponseFrames::Pending {
            state.frames = outcome;
        }
    }

    pub(super) fn is_http2(&self) -> bool {
        matches!(self.0.target, Target::Http2(_))
    }

    /// Hyper can enqueue a whole DATA chunk after receiving only one byte of
    /// send capacity. Keep END_STREAM pending until h2 releases all such chunks
    /// and the connection subsequently flushes. The worker therefore remains
    /// abortable while the last payload is blocked by flow control or I/O.
    /// These buffers contain framework-owned bytes, never lifecycle resources.
    pub(super) fn track_data(&self, bytes: Bytes) -> Bytes {
        if !self.is_http2() || bytes.is_empty() {
            return bytes;
        }
        lock(&self.0.state).data_in_flight += 1;
        Bytes::from_owner(TrackedData {
            bytes: Some(bytes),
            control: Arc::downgrade(&self.0),
        })
    }

    pub(super) fn poll_data_flush(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.0.eof_waker.register(cx.waker());
        let mut state = lock(&self.0.state);
        if !state.eof_wait_started {
            state.eof_wait_started = true;
            // Hyper has queued the response head before polling this body.
            // Even an empty response must keep its worker until a subsequent
            // flush; there may be no DATA owner to supply that notification.
            state.flush_after_data = self
                .0
                .connection
                .flush_generation
                .load(Ordering::Acquire)
                .saturating_add(1);
            self.0.connection.driver_waker.wake();
        }
        if state.data_in_flight == 0
            && self.0.connection.flush_generation.load(Ordering::Acquire) >= state.flush_after_data
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    pub(super) fn wake_after_flush(&self) {
        self.0.eof_waker.wake();
    }

    pub(super) fn observe_http1_flush(&self) -> bool {
        if matches!(self.0.target, Target::Http2(_)) {
            return false;
        }
        let mut state = lock(&self.0.state);
        if state.commit == ResponseCommit::CommittedToProtocol
            && state.frames == ResponseFrames::Completed
            && state.stop_requested.is_none()
            && !state.http1_flushed
        {
            state.http1_flushed = true;
            return true;
        }
        false
    }

    pub(crate) fn reason(&self) -> Arc<Mutex<ExecutionStopReason>> {
        self.0.reason.clone()
    }

    pub(super) fn connection_failed(&self) {
        if lock(&self.0.state).stop_requested.is_none() {
            *lock(&self.0.reason) = ExecutionStopReason::TransportFailure;
        }
    }

    /// Final protocol stop, not cooperative execution cancellation. The
    /// lifecycle owner stays elsewhere. A returned true means a stop request
    /// was issued; only snapshot/join observation can establish termination.
    pub(crate) fn request_stop(&self, reason: ExecutionStopReason) -> bool {
        if self.snapshot().is_terminal() {
            return false;
        }
        let mut state = lock(&self.0.state);
        // A flush may have completed since the snapshot above. Never let an
        // expired request close a reused HTTP/1 keep-alive connection.
        if state.stop_requested.is_some() || state.http1_flushed {
            return false;
        }
        state.stop_requested = Some(reason);
        *lock(&self.0.reason) = reason;
        tracing::debug!(lily.event = "http.response.stop_requested", lily.reason = ?reason,
            lily.commit = ?state.commit, "HTTP response protocol stop requested; join remains separate");
        match &self.0.target {
            Target::Http1(connection) => connection.request_stop(reason),
            Target::Http2(task) => task.abort(),
        }
        self.0.changed.notify_waiters();
        true
    }

    pub(super) fn worker(&self) -> Option<TaskReceipt<()>> {
        match &self.0.target {
            Target::Http2(task) => Some(task.clone()),
            Target::Http1(_) => None,
        }
    }

    pub(crate) fn snapshot(&self) -> ResponseTransportSnapshot {
        let (http2, task, driver_released) = match &self.0.target {
            Target::Http1(connection) => (
                false,
                connection.joined(),
                connection.released.load(Ordering::Acquire),
            ),
            Target::Http2(task) => (true, Some(task.snapshot()), false),
        };
        let state = lock(&self.0.state);
        ResponseTransportSnapshot {
            http2,
            commit: state.commit,
            frames: state.frames,
            stop_requested: state.stop_requested,
            http1_flushed: state.http1_flushed,
            driver_released,
            task,
        }
    }
}

struct TrackedData {
    bytes: Option<Bytes>,
    control: Weak<Control>,
}

impl AsRef<[u8]> for TrackedData {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_deref().expect("live transport data")
    }
}

impl Drop for TrackedData {
    fn drop(&mut self) {
        drop(self.bytes.take());
        if let Some(control) = self.control.upgrade() {
            {
                let mut state = lock(&control.state);
                state.data_in_flight -= 1;
                state.flush_after_data = control
                    .connection
                    .flush_generation
                    .load(Ordering::Acquire)
                    .saturating_add(1);
            }
            // h2 may release a large frame after its flush, but a small frame
            // before flushing its encoder buffer. Require a subsequent flush
            // in both cases. Polling the live connection also flushes an empty
            // codec; one wake per released chunk makes that progress possible.
            control.connection.driver_waker.wake();
            control.eof_waker.wake();
        }
    }
}

#[derive(Debug)]
pub(super) enum ResponseStopped {
    Stopped,
    FallbackRequired(StatusCode),
}

impl fmt::Display for ResponseStopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HTTP response transport was terminated")
    }
}
impl std::error::Error for ResponseStopped {}
