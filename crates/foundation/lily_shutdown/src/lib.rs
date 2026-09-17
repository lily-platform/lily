//! Application-owned graceful shutdown and in-flight work accounting.
//!
//! [`ShutdownState`] is the single lifecycle authority for one application
//! composition root. A shutdown request is durable even when no task has
//! subscribed yet. Track admitted work with [`ShutdownState::connection_guard`]
//! or [`ShutdownState::job_guard`]; the returned RAII guard prevents counter
//! leaks when a task is cancelled or panics.
//!
//! Product adapters implement [`FrameworkShutdownComponent`] and are executed
//! by [`FrameworkShutdownCoordinator`] in [`FrameworkShutdownPhase::ORDERED`]
//! order. Most Lily applications receive that integration from their HTTP,
//! WebSocket, queue, and telemetry builders. Use [`SignalHandler`] only when
//! assembling a custom process composition root.
//!
//! # Minimal state flow
//!
//! ```
//! use std::sync::Arc;
//! use lily_shutdown::{ShutdownSignal, ShutdownState};
//!
//! let state = Arc::new(ShutdownState::new_not_ready());
//! state.publish_ready()?;
//!
//! let work = state.job_guard()?;
//! state.initiate_shutdown(ShutdownSignal::Manual)?;
//! assert!(!state.is_ready());
//! assert!(!state.is_accepting_connections());
//! drop(work);
//! assert_eq!(state.active_job_count(), 0);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::watch;

mod framework;
mod signal_handler;

pub use framework::{
    FrameworkComponentReport, FrameworkComponentStatus, FrameworkForceShutdownFuture,
    FrameworkForcedCleanupStatus, FrameworkPhaseReport, FrameworkShutdownCompletion,
    FrameworkShutdownComponent, FrameworkShutdownCoordinator, FrameworkShutdownMetricSnapshot,
    FrameworkShutdownPhase, FrameworkShutdownReport, OwnedTaskSet, ShutdownAction,
    ShutdownActionTerminal,
};
pub use signal_handler::{SignalHandler, SignalMonitor};

/// Shutdown signal types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// Interactive interrupt (`SIGINT` on Unix or Ctrl+C).
    Interrupt,
    /// Orchestrator/process termination request (`SIGTERM` on Unix).
    Terminate,
    /// Immediate bounded force request (`SIGQUIT` on Unix).
    Quit,
    /// Programmatic graceful shutdown request.
    Manual,
}

impl ShutdownSignal {
    /// Returns a human-readable description for logs and diagnostics.
    pub fn description(&self) -> &'static str {
        match self {
            Self::Interrupt => "Interrupt signal (SIGINT/Ctrl+C)",
            Self::Terminate => "Terminate signal (SIGTERM)",
            Self::Quit => "Quit signal (SIGQUIT)",
            Self::Manual => "Manual shutdown",
        }
    }

    /// Returns whether this signal begins directly on the bounded force path.
    pub fn is_force(&self) -> bool {
        matches!(self, Self::Quit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownPhase {
    StopAccepting,
    DrainConnections,
    Cleanup,
    Force,
}

/// Result of an idempotent shutdown request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownInitiation {
    /// This request performed the one-time shutdown state transition.
    Initiated {
        /// Signal retained as the durable initial reason.
        signal: ShutdownSignal,
    },
    /// Shutdown had already been initiated; the original signal is retained.
    AlreadyInitiated {
        /// Signal which won the initial transition.
        initial_signal: ShutdownSignal,
        /// Signal supplied by this later request.
        requested_signal: ShutdownSignal,
    },
}

/// Error returned when admission has already closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("shutdown admission is closed")]
pub struct AdmissionClosed;

/// Error returned when a listener tries to publish readiness after lifecycle
/// shutdown or admission closure has already won the race.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("readiness publication is closed")]
pub struct ReadinessPublicationClosed;

/// A replaying shutdown receiver. Late subscribers observe the first signal.
pub struct ShutdownReceiver {
    receiver: watch::Receiver<Option<ShutdownSignal>>,
}

impl ShutdownReceiver {
    /// Waits for and returns the durable initial shutdown signal.
    ///
    /// Late subscribers receive a signal that was published before they
    /// subscribed.
    pub async fn recv(&mut self) -> Result<ShutdownSignal, ShutdownReceiveError> {
        loop {
            if let Some(signal) = *self.receiver.borrow_and_update() {
                return Ok(signal);
            }
            self.receiver
                .changed()
                .await
                .map_err(|_| ShutdownReceiveError)?;
        }
    }
}

/// Error returned when the shared state disappears before shutdown begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("shutdown state was dropped before a signal was published")]
pub struct ShutdownReceiveError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivityKind {
    Connection,
    Job,
}

/// RAII-owned in-flight connection or job counter.
#[derive(Debug)]
pub struct ActivityGuard {
    state: Arc<ShutdownState>,
    kind: ActivityKind,
    active: bool,
}

impl ActivityGuard {
    /// Releases the tracked activity early instead of waiting for `Drop`.
    pub fn release(mut self) {
        self.decrement();
    }

    fn decrement(&mut self) {
        if self.active {
            self.state.decrement_activity(self.kind);
            self.active = false;
        }
    }
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.decrement();
    }
}

/// Metrics emitted by the shared shutdown state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShutdownMetricSnapshot {
    /// Number of attempted decrements observed when a manual/internal counter
    /// was already zero.
    pub counter_underflows: usize,
}

/// Shutdown state shared by an application composition root.
#[derive(Debug)]
pub struct ShutdownState {
    shutdown_initiated: AtomicBool,
    ready: AtomicBool,
    accepting_connections: AtomicBool,
    active_connections: AtomicUsize,
    active_jobs: AtomicUsize,
    shutdown_tx: watch::Sender<Option<ShutdownSignal>>,
    current_phase: Mutex<Option<ShutdownPhase>>,
    initial_signal: Mutex<Option<ShutdownSignal>>,
    initiation_started_at: OnceLock<tokio::time::Instant>,
    force_requested: AtomicBool,
    force_tx: watch::Sender<bool>,
    counter_underflows: AtomicUsize,
}

impl ShutdownState {
    /// Creates a state that is initially ready and accepting work.
    ///
    /// Prefer [`Self::new_not_ready`] when listener binding or another startup
    /// gate must complete before readiness can be published.
    pub fn new() -> Self {
        let (shutdown_tx, _) = watch::channel(None);
        let (force_tx, _) = watch::channel(false);
        Self {
            shutdown_initiated: AtomicBool::new(false),
            ready: AtomicBool::new(true),
            accepting_connections: AtomicBool::new(true),
            active_connections: AtomicUsize::new(0),
            active_jobs: AtomicUsize::new(0),
            shutdown_tx,
            current_phase: Mutex::new(None),
            initial_signal: Mutex::new(None),
            initiation_started_at: OnceLock::new(),
            force_requested: AtomicBool::new(false),
            force_tx,
            counter_underflows: AtomicUsize::new(0),
        }
    }

    /// Creates a composition root which is live but cannot be advertised as
    /// ready until its listener or equivalent admission boundary is open.
    pub fn new_not_ready() -> Self {
        let state = Self::new();
        state.ready.store(false, Ordering::Release);
        state
    }

    /// Returns whether the durable shutdown transition has occurred.
    pub fn is_shutdown_initiated(&self) -> bool {
        self.shutdown_initiated.load(Ordering::Acquire)
    }

    /// Whether this application instance may still be advertised as ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Lowers readiness before transport admission and drain. Once shutdown or
    /// admission closure begins, [`Self::publish_ready`] cannot raise it again.
    pub fn mark_not_ready(&self) {
        self.ready.store(false, Ordering::Release);
    }

    /// Publishes readiness only while lifecycle shutdown and admission closure
    /// are both absent. The second check closes the publication-vs-shutdown
    /// race; shutdown always wins and leaves the final state not ready.
    pub fn publish_ready(&self) -> Result<(), ReadinessPublicationClosed> {
        if self.is_shutdown_initiated() || !self.is_accepting_connections() {
            return Err(ReadinessPublicationClosed);
        }

        self.ready.store(true, Ordering::Release);
        if self.is_shutdown_initiated() || !self.is_accepting_connections() {
            self.mark_not_ready();
            Err(ReadinessPublicationClosed)
        } else {
            Ok(())
        }
    }

    /// Returns whether new connections or jobs may still be admitted.
    pub fn is_accepting_connections(&self) -> bool {
        self.accepting_connections.load(Ordering::Acquire)
    }

    /// Returns the current number of guarded in-flight connections.
    pub fn active_connection_count(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    /// Returns the current number of guarded in-flight jobs.
    pub fn active_job_count(&self) -> usize {
        self.active_jobs.load(Ordering::Acquire)
    }

    /// Acquires an in-flight connection counter tied to the returned guard.
    pub fn connection_guard(self: &Arc<Self>) -> Result<ActivityGuard, AdmissionClosed> {
        self.activity_guard(ActivityKind::Connection)
    }

    /// Acquires an in-flight job counter tied to the returned guard.
    pub fn job_guard(self: &Arc<Self>) -> Result<ActivityGuard, AdmissionClosed> {
        self.activity_guard(ActivityKind::Job)
    }

    fn activity_guard(
        self: &Arc<Self>,
        kind: ActivityKind,
    ) -> Result<ActivityGuard, AdmissionClosed> {
        if !self.is_accepting_connections() {
            return Err(AdmissionClosed);
        }

        self.increment_activity(kind);
        // Close the check-vs-increment race. Admission linearizes at this
        // second acquire load; work losing the race is immediately removed.
        if !self.is_accepting_connections() {
            self.decrement_activity(kind);
            return Err(AdmissionClosed);
        }

        Ok(ActivityGuard {
            state: Arc::clone(self),
            kind,
            active: true,
        })
    }

    fn increment_connections(&self) -> usize {
        self.increment_counter(&self.active_connections)
    }

    fn decrement_connections(&self) -> usize {
        self.decrement_counter(&self.active_connections)
    }

    fn increment_jobs(&self) -> usize {
        self.increment_counter(&self.active_jobs)
    }

    fn decrement_jobs(&self) -> usize {
        self.decrement_counter(&self.active_jobs)
    }

    fn increment_activity(&self, kind: ActivityKind) -> usize {
        match kind {
            ActivityKind::Connection => self.increment_connections(),
            ActivityKind::Job => self.increment_jobs(),
        }
    }

    fn decrement_activity(&self, kind: ActivityKind) -> usize {
        match kind {
            ActivityKind::Connection => self.decrement_connections(),
            ActivityKind::Job => self.decrement_jobs(),
        }
    }

    fn increment_counter(&self, counter: &AtomicUsize) -> usize {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map(|previous| previous + 1)
            .unwrap_or(usize::MAX)
    }

    fn decrement_counter(&self, counter: &AtomicUsize) -> usize {
        match counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_sub(1)
        }) {
            Ok(previous) => previous - 1,
            Err(_) => {
                self.counter_underflows.fetch_add(1, Ordering::Relaxed);
                0
            }
        }
    }

    /// Durably initiates shutdown. Notification receiver presence is not part
    /// of the operation's success criteria.
    pub fn initiate_shutdown(
        &self,
        signal: ShutdownSignal,
    ) -> Result<ShutdownInitiation, ShutdownError> {
        // Capture the request at its source, before publication. A delayed
        // composition-root observer must not create a later shutdown budget.
        self.initiation_started_at
            .get_or_init(tokio::time::Instant::now);
        match self.shutdown_initiated.compare_exchange(
            false,
            true,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                *self
                    .initial_signal
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(signal);
                self.mark_not_ready();
                self.stop_accepting_connections();
                self.shutdown_tx.send_replace(Some(signal));
                if signal.is_force() {
                    self.request_force();
                }
                Ok(ShutdownInitiation::Initiated { signal })
            }
            Err(_) => Ok(ShutdownInitiation::AlreadyInitiated {
                initial_signal: self.initial_signal().unwrap_or(signal),
                requested_signal: signal,
            }),
        }
    }

    /// Returns the signal retained by the first successful shutdown request.
    pub fn initial_signal(&self) -> Option<ShutdownSignal> {
        *self
            .initial_signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Monotonic start of the first shutdown initiation request. Repeated
    /// requests and force escalation do not move this timestamp. Composition
    /// roots derive their absolute deadlines from it even when they observe
    /// a native signal later.
    #[doc(hidden)]
    pub fn initiation_started_at(&self) -> Option<tokio::time::Instant> {
        self.initiation_started_at.get().copied()
    }

    /// Escalates an in-progress shutdown. This is used by the second OS signal
    /// and is itself idempotent.
    pub fn request_force(&self) -> bool {
        let newly_requested = !self.force_requested.swap(true, Ordering::AcqRel);
        self.stop_accepting_connections();
        self.set_phase(ShutdownPhase::Force);
        if newly_requested {
            self.force_tx.send_replace(true);
        }
        newly_requested
    }

    /// Returns whether graceful shutdown has escalated to its force path.
    pub fn is_force_requested(&self) -> bool {
        self.force_requested.load(Ordering::Acquire)
    }

    /// Waits until the shutdown lifecycle requests its bounded force path.
    pub async fn wait_for_force(&self) {
        let mut receiver = self.force_tx.subscribe();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }

    /// Closes new-work admission without publishing a shutdown signal.
    ///
    /// Framework coordinators call this at the stop-admission boundary. Once
    /// closed, readiness cannot be published again.
    pub fn stop_accepting_connections(&self) {
        self.accepting_connections.store(false, Ordering::Release);
    }

    fn set_phase(&self, phase: ShutdownPhase) {
        *self
            .current_phase
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(phase);
    }

    /// Subscribes to the durable initial shutdown signal.
    pub fn subscribe(&self) -> ShutdownReceiver {
        ShutdownReceiver {
            receiver: self.shutdown_tx.subscribe(),
        }
    }

    /// Returns a point-in-time snapshot of shutdown accounting diagnostics.
    pub fn metrics(&self) -> ShutdownMetricSnapshot {
        ShutdownMetricSnapshot {
            counter_underflows: self.counter_underflows.load(Ordering::Relaxed),
        }
    }
}

impl Default for ShutdownState {
    fn default() -> Self {
        Self::new()
    }
}

/// Error produced by shutdown components and closure-backed actions.
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    /// A configured shutdown deadline elapsed.
    #[error("shutdown timeout exceeded")]
    Timeout,
    /// Installing or receiving an operating-system signal failed.
    #[error("signal handling error: {0}")]
    Signal(#[from] std::io::Error),
    /// Shutdown and cleanup of its signal monitor both failed.
    #[error("{shutdown}; signal monitor cleanup also failed: {signal}")]
    SignalCleanup {
        /// Primary shutdown failure.
        #[source]
        shutdown: Box<ShutdownError>,
        /// Signal monitor cleanup failure.
        signal: std::io::Error,
    },
    /// A non-idempotent adapter rejected a repeated shutdown request.
    #[error("shutdown already in progress")]
    AlreadyInProgress,
    /// A framework component failed to stop, drain, dispose, or flush.
    #[error("component shutdown failed: {0}")]
    Component(String),
    /// A closure-backed action returned an error.
    #[error("shutdown action `{name}` failed: {error}")]
    ActionFailed {
        /// Registered component name.
        name: String,
        /// Stable error text retained in the shutdown report.
        error: String,
    },
    /// A closure-backed action panicked.
    #[error("shutdown action `{name}` panicked: {message}")]
    ActionPanicked {
        /// Registered component name.
        name: String,
        /// Captured panic message.
        message: String,
    },
    /// A one-shot action future disappeared before recording an outcome.
    #[error("shutdown action `{name}` {stage} future was interrupted before a terminal result")]
    ActionInterrupted {
        /// Registered component name.
        name: String,
        /// Lifecycle stage (`graceful` or `force`).
        stage: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transition_is_idempotent_and_replayed_to_late_subscribers() {
        let state = ShutdownState::new();
        assert_eq!(
            state.initiate_shutdown(ShutdownSignal::Manual).unwrap(),
            ShutdownInitiation::Initiated {
                signal: ShutdownSignal::Manual
            }
        );
        assert!(matches!(
            state.initiate_shutdown(ShutdownSignal::Terminate).unwrap(),
            ShutdownInitiation::AlreadyInitiated { .. }
        ));

        let mut late = state.subscribe();
        assert_eq!(late.recv().await.unwrap(), ShutdownSignal::Manual);
    }

    #[test]
    fn counters_never_wrap_and_raii_handles_panic_cleanup() {
        let state = Arc::new(ShutdownState::new());
        assert_eq!(state.decrement_connections(), 0);
        assert_eq!(state.active_connection_count(), 0);

        let panic_state = Arc::clone(&state);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = panic_state.connection_guard().unwrap();
            panic!("simulated handler panic");
        }));

        assert_eq!(state.active_connection_count(), 0);
        assert_eq!(state.metrics().counter_underflows, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_hundred_racing_callers_publish_one_transition() {
        for _ in 0..100 {
            let state = Arc::new(ShutdownState::new());
            let mut callers = Vec::new();
            for _ in 0..16 {
                let state = Arc::clone(&state);
                callers.push(tokio::spawn(async move {
                    state.initiate_shutdown(ShutdownSignal::Manual).unwrap()
                }));
            }

            let mut initiated = 0;
            for caller in callers {
                if matches!(caller.await.unwrap(), ShutdownInitiation::Initiated { .. }) {
                    initiated += 1;
                }
            }
            assert_eq!(initiated, 1);
        }
    }

    #[test]
    fn admission_is_closed_as_part_of_the_state_transition() {
        let state = Arc::new(ShutdownState::new());
        state.initiate_shutdown(ShutdownSignal::Manual).unwrap();
        assert_eq!(state.connection_guard().unwrap_err(), AdmissionClosed);
        assert_eq!(state.job_guard().unwrap_err(), AdmissionClosed);
    }

    #[test]
    fn readiness_can_only_be_published_before_shutdown() {
        let state = ShutdownState::new_not_ready();
        assert!(!state.is_ready());

        state.publish_ready().unwrap();
        assert!(state.is_ready());

        state.initiate_shutdown(ShutdownSignal::Manual).unwrap();
        assert!(!state.is_ready());
        assert_eq!(state.publish_ready(), Err(ReadinessPublicationClosed));
        assert!(!state.is_ready());
    }

    #[test]
    fn shutdown_wins_every_readiness_publication_race() {
        use std::sync::Barrier;

        for _ in 0..100 {
            let state = Arc::new(ShutdownState::new_not_ready());
            let barrier = Arc::new(Barrier::new(3));
            let ready_state = Arc::clone(&state);
            let ready_barrier = Arc::clone(&barrier);
            let ready = std::thread::spawn(move || {
                ready_barrier.wait();
                let _ = ready_state.publish_ready();
            });
            let shutdown_state = Arc::clone(&state);
            let shutdown_barrier = Arc::clone(&barrier);
            let shutdown = std::thread::spawn(move || {
                shutdown_barrier.wait();
                shutdown_state
                    .initiate_shutdown(ShutdownSignal::Manual)
                    .unwrap();
            });

            barrier.wait();
            ready.join().unwrap();
            shutdown.join().unwrap();
            assert!(!state.is_ready());
            assert!(!state.is_accepting_connections());
        }
    }
}
