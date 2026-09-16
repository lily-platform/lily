//! Framework-wide, application-owned shutdown orchestration.
//!
//! Product adapters register their real stop, drain, dispose, and flush
//! handles in five ordered phases. Every component gets a bounded deadline
//! and every outcome is retained in a single report.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

use crate::{ShutdownError, ShutdownPhase, ShutdownSignal, ShutdownState};

const MAX_FORCE_RESERVE: Duration = Duration::from_secs(2);

/// Canonical profile shutdown order. Declaration order is execution order and
/// is part of the V1 lifecycle contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FrameworkShutdownPhase {
    /// Withdraw process readiness before changing admission.
    ReadinessDown,
    /// Stop accepting new connections, deliveries, and jobs.
    StopAdmission,
    /// Wait for already-admitted work to finish.
    DrainInFlight,
    /// Close application dependencies and clients.
    DisposeDependencies,
    /// Flush telemetry after other components have emitted their final data.
    FlushTelemetry,
}

impl FrameworkShutdownPhase {
    /// Canonical phase order used by [`FrameworkShutdownCoordinator`].
    pub const ORDERED: [Self; 5] = [
        Self::ReadinessDown,
        Self::StopAdmission,
        Self::DrainInFlight,
        Self::DisposeDependencies,
        Self::FlushTelemetry,
    ];

    /// Returns a short human-readable phase description.
    pub const fn description(self) -> &'static str {
        match self {
            Self::ReadinessDown => "readiness down",
            Self::StopAdmission => "new-work admission stopped",
            Self::DrainInFlight => "in-flight work drained",
            Self::DisposeDependencies => "dependencies and clients disposed",
            Self::FlushTelemetry => "telemetry flushed",
        }
    }
}

/// Mutually exclusive terminal outcome for one registered stop handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameworkComponentStatus {
    /// Graceful component shutdown completed.
    Completed,
    /// The component returned an error.
    Failed {
        /// Captured component error text.
        error: String,
    },
    /// The component panicked.
    Panicked {
        /// Captured panic message.
        message: String,
    },
    /// The component exceeded its graceful budget.
    TimedOut,
    /// Graceful work was cancelled because force shutdown was requested.
    CancelledByForce,
}

/// Terminal evidence for the explicit force handle of one component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameworkForcedCleanupStatus {
    /// No force cleanup was required.
    NotAttempted,
    /// Explicit force cleanup completed.
    Completed,
    /// Explicit force cleanup returned an error.
    Failed {
        /// Captured cleanup error text.
        error: String,
    },
    /// Explicit force cleanup panicked.
    Panicked {
        /// Captured panic message.
        message: String,
    },
    /// Explicit force cleanup exceeded its bounded budget.
    TimedOut,
    /// The component did not provide an explicit force handle.
    Unavailable,
}

impl FrameworkForcedCleanupStatus {
    /// Returns whether force cleanup was required, including unavailable
    /// cleanup.
    pub const fn was_requested(&self) -> bool {
        !matches!(self, Self::NotAttempted)
    }

    /// Returns whether an explicit cleanup future was actually attempted.
    pub const fn was_attempted(&self) -> bool {
        !matches!(self, Self::NotAttempted | Self::Unavailable)
    }

    /// Returns whether explicit force cleanup completed successfully.
    pub const fn completed(&self) -> bool {
        matches!(self, Self::Completed)
    }
}

/// Terminal evidence for one registered component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameworkComponentReport {
    /// Phase in which the component ran.
    pub phase: FrameworkShutdownPhase,
    /// Stable component name supplied by the component.
    pub name: String,
    /// Component-local timeout before it was capped by the application
    /// deadline.
    pub configured_timeout: Duration,
    /// Observed component execution time.
    pub elapsed: Duration,
    /// Graceful terminal outcome.
    pub status: FrameworkComponentStatus,
    /// Result of an explicit idempotent force handle after cancellation,
    /// timeout, failure, or panic. The primary status is retained so forced
    /// completion cannot be mistaken for graceful completion.
    pub forced_cleanup: FrameworkForcedCleanupStatus,
}

/// Evidence for one canonical shutdown phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameworkPhaseReport {
    /// Executed phase.
    pub phase: FrameworkShutdownPhase,
    /// Total elapsed time for this phase.
    pub elapsed: Duration,
    /// Component reports in actual execution order.
    pub components: Vec<FrameworkComponentReport>,
}

/// Bounded aggregate counters derived from component reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrameworkShutdownMetricSnapshot {
    /// Gracefully completed components.
    pub completed: usize,
    /// Components which returned graceful errors.
    pub failed: usize,
    /// Components which panicked during graceful shutdown.
    pub panicked: usize,
    /// Components which exceeded their graceful budget.
    pub timed_out: usize,
    /// Components interrupted by force escalation.
    pub cancelled_by_force: usize,
    /// Explicit force cleanup futures attempted.
    pub forced_cleanup_attempted: usize,
    /// Explicit force cleanup futures completed.
    pub forced_cleanup_completed: usize,
    /// Explicit force cleanup futures which returned errors.
    pub forced_cleanup_failed: usize,
    /// Explicit force cleanup futures which panicked.
    pub forced_cleanup_panicked: usize,
    /// Explicit force cleanup futures which timed out.
    pub forced_cleanup_timed_out: usize,
    /// Components requiring cleanup but lacking an explicit force handle.
    pub forced_cleanup_unavailable: usize,
}

/// Aggregate terminal class for the application-owned shutdown lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameworkShutdownCompletion {
    /// Every component completed without using force cleanup.
    GracefulCompleted,
    /// Every component reached a terminal state and at least one used the
    /// force path.
    ForcedCompleted,
    /// At least one component could not prove terminal cleanup.
    Incomplete,
}

impl FrameworkShutdownMetricSnapshot {
    /// Returns the number of primary component outcomes.
    pub const fn total(self) -> usize {
        self.completed + self.failed + self.panicked + self.timed_out + self.cancelled_by_force
    }
}

/// Aggregate lifecycle evidence for one application composition root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameworkShutdownReport {
    /// Durable signal that initiated this lifecycle.
    pub signal: ShutdownSignal,
    /// Whether force escalation or explicit force cleanup was used.
    pub forced: bool,
    /// Aggregate completion class.
    pub completion: FrameworkShutdownCompletion,
    /// Application-wide hard deadline.
    pub deadline: Duration,
    /// Portion of the hard deadline reserved for force cleanup.
    pub force_reserve: Duration,
    /// Total observed coordinator duration.
    pub elapsed: Duration,
    /// Reports for every canonical phase, including empty phases.
    pub phases: Vec<FrameworkPhaseReport>,
    /// Aggregate component outcome counters.
    pub metrics: FrameworkShutdownMetricSnapshot,
}

impl FrameworkShutdownReport {
    /// Returns whether every component completed gracefully.
    pub const fn is_graceful(&self) -> bool {
        matches!(
            self.completion,
            FrameworkShutdownCompletion::GracefulCompleted
        )
    }

    /// Returns whether every component proved graceful or forced cleanup.
    pub const fn is_terminal_complete(&self) -> bool {
        !matches!(self.completion, FrameworkShutdownCompletion::Incomplete)
    }

    /// Returns the number of component reports across all phases.
    pub fn component_count(&self) -> usize {
        self.phases.iter().map(|phase| phase.components.len()).sum()
    }

    /// Returns whether aggregate metrics account for every component report.
    pub fn reconciles(&self) -> bool {
        self.metrics.total() == self.component_count()
    }
}

/// Boxed explicit force handle borrowing its owning component.
pub type FrameworkForceShutdownFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ShutdownError>> + Send + 'a>>;

/// A real product stop/drain/dispose handle owned by the process composition
/// root. Graceful shutdown is never invoked twice. Components which can
/// reconcile cancellation must expose that work through `force_shutdown`.
#[async_trait]
pub trait FrameworkShutdownComponent: Send {
    /// Performs graceful component cleanup. The coordinator calls this at
    /// most once and enforces [`Self::timeout`].
    async fn shutdown(&mut self) -> Result<(), ShutdownError>;

    /// Returns the stable name used in reports.
    fn name(&self) -> &str;

    /// Returns the canonical phase in which this component executes.
    fn phase(&self) -> FrameworkShutdownPhase;

    /// Returns the component-local graceful timeout.
    fn timeout(&self) -> Duration;

    /// Publishes the composition root's absolute limits before any component
    /// starts shutdown. Owners may retain these limits across dropped waiters.
    fn set_shutdown_deadlines(&mut self, _graceful: Instant, _hard: Instant) {}

    /// Publishes this force attempt's effective absolute cap before requesting
    /// force. It is always inside the root deadline and never supplies a new
    /// application budget.
    fn set_force_deadline(&mut self, _deadline: Instant) {}

    /// An owner with its own composition-root phase reservations may supply
    /// the existing absolute force cutoff. The coordinator still clamps it
    /// to its hard deadline and component timeout. `None` keeps the default
    /// short force reserve; a supplied cutoff never starts a fresh budget.
    #[doc(hidden)]
    fn force_deadline(&self) -> Option<Instant> {
        None
    }

    /// Returns an explicit idempotent force handle. `None` means the component
    /// cannot prove forced cleanup and must be reported as unavailable.
    fn force_shutdown(&mut self) -> Option<FrameworkForceShutdownFuture<'_>> {
        None
    }
}

/// Replayable terminal result retained by a closure-backed shutdown action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownActionTerminal {
    /// The closure completed successfully.
    Completed,
    /// The closure returned an error.
    Failed {
        /// Captured error text.
        error: String,
    },
    /// The closure panicked.
    Panicked {
        /// Captured panic message.
        message: String,
    },
    /// A one-shot future disappeared before a terminal result was retained.
    Interrupted,
}

impl ShutdownActionTerminal {
    fn result(&self, name: &str, stage: &'static str) -> Result<(), ShutdownError> {
        match self {
            Self::Completed => Ok(()),
            Self::Failed { error } => Err(ShutdownError::ActionFailed {
                name: name.to_owned(),
                error: error.clone(),
            }),
            Self::Panicked { message } => Err(ShutdownError::ActionPanicked {
                name: name.to_owned(),
                message: message.clone(),
            }),
            Self::Interrupted => Err(ShutdownError::ActionInterrupted {
                name: name.to_owned(),
                stage,
            }),
        }
    }
}

type BoxShutdownActionFuture =
    Pin<Box<dyn Future<Output = Result<(), ShutdownError>> + Send + 'static>>;
type BoxForceAction = Box<dyn FnOnce() -> BoxShutdownActionFuture + Send>;

/// Closure-backed adapter for a resource which already exposes an owned async
/// close method.
pub struct ShutdownAction<F> {
    name: String,
    phase: FrameworkShutdownPhase,
    timeout: Duration,
    action: Option<F>,
    terminal: Option<ShutdownActionTerminal>,
    force_action: Option<BoxForceAction>,
    force_started: bool,
    force_terminal: Option<ShutdownActionTerminal>,
}

impl<F> ShutdownAction<F> {
    /// Adapts an owned `FnOnce() -> Future` cleanup operation into a shutdown
    /// component.
    pub fn new(
        name: impl Into<String>,
        phase: FrameworkShutdownPhase,
        timeout: Duration,
        action: F,
    ) -> Self {
        Self {
            name: name.into(),
            phase,
            timeout,
            action: Some(action),
            terminal: None,
            force_action: None,
            force_started: false,
            force_terminal: None,
        }
    }

    /// Attaches a distinct idempotent force handle. The graceful `FnOnce` is
    /// never re-invoked after cancellation or failure.
    pub fn with_force<G, Fut>(mut self, force_action: G) -> Self
    where
        G: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ShutdownError>> + Send + 'static,
    {
        self.force_action = Some(Box::new(move || Box::pin(force_action())));
        self
    }

    /// Returns the retained graceful terminal outcome after execution.
    pub fn terminal_outcome(&self) -> Option<&ShutdownActionTerminal> {
        self.terminal.as_ref()
    }

    /// Returns the retained explicit-force terminal outcome after execution.
    pub fn force_terminal_outcome(&self) -> Option<&ShutdownActionTerminal> {
        self.force_terminal.as_ref()
    }

    async fn run_force_action(&mut self) -> Result<(), ShutdownError> {
        if let Some(terminal) = &self.force_terminal {
            return terminal.result(&self.name, "force");
        }
        let Some(action) = self.force_action.take() else {
            let terminal = ShutdownActionTerminal::Interrupted;
            self.force_terminal = Some(terminal.clone());
            return terminal.result(&self.name, "force");
        };
        self.force_started = true;
        let terminal = match std::panic::AssertUnwindSafe(action()).catch_unwind().await {
            Ok(Ok(())) => ShutdownActionTerminal::Completed,
            Ok(Err(error)) => ShutdownActionTerminal::Failed {
                error: error.to_string(),
            },
            Err(payload) => ShutdownActionTerminal::Panicked {
                message: panic_message(payload),
            },
        };
        self.force_terminal = Some(terminal.clone());
        terminal.result(&self.name, "force")
    }
}

#[async_trait]
impl<F, Fut> FrameworkShutdownComponent for ShutdownAction<F>
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<(), ShutdownError>> + Send,
{
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        if let Some(terminal) = &self.terminal {
            return terminal.result(&self.name, "graceful");
        }
        let Some(action) = self.action.take() else {
            let terminal = ShutdownActionTerminal::Interrupted;
            self.terminal = Some(terminal.clone());
            return terminal.result(&self.name, "graceful");
        };
        let terminal = match std::panic::AssertUnwindSafe(action()).catch_unwind().await {
            Ok(Ok(())) => ShutdownActionTerminal::Completed,
            Ok(Err(error)) => ShutdownActionTerminal::Failed {
                error: error.to_string(),
            },
            Err(payload) => ShutdownActionTerminal::Panicked {
                message: panic_message(payload),
            },
        };
        self.terminal = Some(terminal.clone());
        terminal.result(&self.name, "graceful")
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        self.phase
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn force_shutdown(&mut self) -> Option<FrameworkForceShutdownFuture<'_>> {
        if self.force_action.is_none() && !self.force_started && self.force_terminal.is_none() {
            return None;
        }
        Some(Box::pin(self.run_force_action()))
    }
}

/// Cancellation token plus every task spawned for one runtime component.
/// Graceful shutdown cancels and joins; the explicit force handle aborts and
/// joins stragglers without re-invoking the graceful future.
pub struct OwnedTaskSet {
    name: String,
    phase: FrameworkShutdownPhase,
    timeout: Duration,
    cancellation: CancellationToken,
    tasks: JoinSet<Result<(), ShutdownError>>,
}

impl OwnedTaskSet {
    /// Creates an empty task owner with a shared cancellation token.
    pub fn new(name: impl Into<String>, phase: FrameworkShutdownPhase, timeout: Duration) -> Self {
        Self {
            name: name.into(),
            phase,
            timeout,
            cancellation: CancellationToken::new(),
            tasks: JoinSet::new(),
        }
    }

    /// Returns a child handle to the cancellation signal owned by this set.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Spawns and takes ownership of one fallible asynchronous task.
    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = Result<(), ShutdownError>> + Send + 'static,
    {
        self.tasks.spawn(task);
    }

    /// Returns the number of tasks that have not yet been joined.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
}

#[async_trait]
impl FrameworkShutdownComponent for OwnedTaskSet {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        self.cancellation.cancel();
        let mut failures = Vec::new();
        while let Some(result) = self.tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error.to_string()),
                Err(error) if error.is_cancelled() => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(ShutdownError::Component(failures.join("; ")))
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        self.phase
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn force_shutdown(&mut self) -> Option<FrameworkForceShutdownFuture<'_>> {
        Some(Box::pin(async move {
            self.cancellation.cancel();
            self.tasks.abort_all();
            self.shutdown().await
        }))
    }
}

/// Executes all registered product handles in the canonical phase order.
pub struct FrameworkShutdownCoordinator {
    state: Arc<ShutdownState>,
    components: Vec<Box<dyn FrameworkShutdownComponent>>,
    deadline: Duration,
    absolute_deadlines: Option<(Instant, Instant)>,
    terminal_report: Option<FrameworkShutdownReport>,
}

impl FrameworkShutdownCoordinator {
    /// Creates the canonical coordinator with the composition root's total
    /// lifecycle deadline. Signal type never supplies a second time budget.
    pub fn new(state: Arc<ShutdownState>, deadline: Duration) -> Self {
        Self {
            state,
            components: Vec::new(),
            deadline,
            absolute_deadlines: None,
            terminal_report: None,
        }
    }

    /// Attach an existing composition-root attempt without restarting its
    /// budget. Expired deadlines remain expired; graceful is clamped to hard.
    #[doc(hidden)]
    pub fn before(
        state: Arc<ShutdownState>,
        configured_timeout: Duration,
        graceful_deadline: Instant,
        hard_deadline: Instant,
    ) -> Self {
        let mut coordinator = Self::new(state, configured_timeout);
        coordinator.absolute_deadlines =
            Some((graceful_deadline.min(hard_deadline), hard_deadline));
        coordinator
    }

    /// Registers one component. Components within the same phase execute in
    /// reverse registration order.
    pub fn register<T>(&mut self, component: T)
    where
        T: FrameworkShutdownComponent + 'static,
    {
        self.components.push(Box::new(component));
    }

    /// Returns the lifecycle state governed by this coordinator.
    pub fn state(&self) -> &Arc<ShutdownState> {
        &self.state
    }

    /// Returns the application-wide hard deadline.
    pub const fn deadline(&self) -> Duration {
        self.deadline
    }

    /// Executes once and replays the immutable terminal report to later
    /// callers. Components in the same phase run in reverse registration
    /// order, preserving dependency teardown semantics.
    pub async fn execute_report(&mut self, signal: ShutdownSignal) -> FrameworkShutdownReport {
        if let Some(report) = &self.terminal_report {
            return report.clone();
        }

        let started = Instant::now();
        // ConfigService bounds canonical deadlines. Direct Duration callers
        // still fail closed instead of panicking before shutdown begins.
        let default_hard_deadline = started.checked_add(self.deadline).unwrap_or(started);
        // Keep a bounded tail inside the application deadline for abort + join.
        // Without this reserve a component may consume the entire deadline in
        // its graceful future, leaving no time to reap the tasks it owns.
        let force_reserve = force_reserve(self.deadline);
        let default_graceful_deadline = default_hard_deadline
            .checked_sub(force_reserve)
            .unwrap_or(started);
        let (graceful_deadline, hard_deadline) = self
            .absolute_deadlines
            .unwrap_or((default_graceful_deadline, default_hard_deadline));
        let force_reserve =
            force_reserve.min(hard_deadline.saturating_duration_since(graceful_deadline));
        for component in &mut self.components {
            component.set_shutdown_deadlines(graceful_deadline, hard_deadline);
        }
        self.state.mark_not_ready();
        let mut initiation_failure = self
            .state
            .initiate_shutdown(signal)
            .err()
            .map(|error| error.to_string());
        if signal.is_force() {
            self.state.request_force();
        }

        let mut phases = Vec::with_capacity(FrameworkShutdownPhase::ORDERED.len());
        let mut metrics = FrameworkShutdownMetricSnapshot::default();
        for phase in FrameworkShutdownPhase::ORDERED {
            let phase_started = Instant::now();
            match phase {
                FrameworkShutdownPhase::ReadinessDown => self.state.mark_not_ready(),
                FrameworkShutdownPhase::StopAdmission => {
                    self.state.stop_accepting_connections();
                    self.state.set_phase(ShutdownPhase::StopAccepting);
                }
                FrameworkShutdownPhase::DrainInFlight => {
                    self.state.set_phase(ShutdownPhase::DrainConnections);
                }
                FrameworkShutdownPhase::DisposeDependencies
                | FrameworkShutdownPhase::FlushTelemetry => {
                    self.state.set_phase(ShutdownPhase::Cleanup);
                }
            }

            let mut reports = Vec::new();
            let state_error = if phase == FrameworkShutdownPhase::ReadinessDown {
                initiation_failure.take()
            } else {
                None
            };
            if let Some(error) = state_error {
                let report = FrameworkComponentReport {
                    phase,
                    name: "shutdown.state".into(),
                    configured_timeout: Duration::ZERO,
                    elapsed: Duration::ZERO,
                    status: FrameworkComponentStatus::Failed { error },
                    forced_cleanup: FrameworkForcedCleanupStatus::NotAttempted,
                };
                record_metric(&mut metrics, &report.status);
                reports.push(report);
            }
            for component in self
                .components
                .iter_mut()
                .rev()
                .filter(|component| component.phase() == phase)
            {
                let report = run_component(
                    &self.state,
                    component.as_mut(),
                    graceful_deadline,
                    hard_deadline,
                    force_reserve,
                )
                .await;
                record_metric(&mut metrics, &report.status);
                record_forced_cleanup_metric(&mut metrics, &report.forced_cleanup);
                reports.push(report);
            }
            phases.push(FrameworkPhaseReport {
                phase,
                elapsed: phase_started.elapsed(),
                components: reports,
            });
        }

        let force_path_used = self.state.is_force_requested()
            || phases
                .iter()
                .flat_map(|phase| &phase.components)
                .any(|component| component.forced_cleanup.was_requested());
        let terminal_complete =
            phases
                .iter()
                .flat_map(|phase| &phase.components)
                .all(|component| {
                    component.status == FrameworkComponentStatus::Completed
                        || component.forced_cleanup.completed()
                });
        let completion = if !terminal_complete {
            FrameworkShutdownCompletion::Incomplete
        } else if force_path_used {
            FrameworkShutdownCompletion::ForcedCompleted
        } else {
            FrameworkShutdownCompletion::GracefulCompleted
        };
        let report = FrameworkShutdownReport {
            signal,
            forced: force_path_used,
            completion,
            deadline: self.deadline,
            force_reserve,
            elapsed: started.elapsed(),
            phases,
            metrics,
        };
        self.terminal_report = Some(report.clone());
        report
    }
}

async fn run_component(
    state: &Arc<ShutdownState>,
    component: &mut dyn FrameworkShutdownComponent,
    graceful_deadline: Instant,
    hard_deadline: Instant,
    force_reserve: Duration,
) -> FrameworkComponentReport {
    let name = component.name().to_owned();
    let phase = component.phase();
    let configured_timeout = component.timeout();
    let started = Instant::now();
    let remaining = graceful_deadline.saturating_duration_since(Instant::now());
    let budget = configured_timeout.min(remaining);

    let (status, forced_cleanup) = if state.is_force_requested() {
        (
            FrameworkComponentStatus::CancelledByForce,
            run_forced(
                component,
                force_budget(configured_timeout, force_reserve, hard_deadline),
                configured_timeout,
                hard_deadline,
            )
            .await,
        )
    } else if budget.is_zero() {
        (
            FrameworkComponentStatus::TimedOut,
            run_forced(
                component,
                force_budget(configured_timeout, force_reserve, hard_deadline),
                configured_timeout,
                hard_deadline,
            )
            .await,
        )
    } else {
        enum InitialOutcome {
            Finished(FrameworkComponentStatus),
            ForceRequested,
        }

        let initial = {
            let shutdown = std::panic::AssertUnwindSafe(component.shutdown()).catch_unwind();
            tokio::pin!(shutdown);
            tokio::select! {
                _ = state.wait_for_force() => InitialOutcome::ForceRequested,
                result = timeout_at((started + budget).min(graceful_deadline), &mut shutdown) => {
                    InitialOutcome::Finished(map_result(result))
                }
            }
        };
        let status = match initial {
            InitialOutcome::Finished(status) => status,
            InitialOutcome::ForceRequested => {
                return FrameworkComponentReport {
                    phase,
                    name,
                    configured_timeout,
                    elapsed: started.elapsed(),
                    status: FrameworkComponentStatus::CancelledByForce,
                    forced_cleanup: run_forced(
                        component,
                        force_budget(configured_timeout, force_reserve, hard_deadline),
                        configured_timeout,
                        hard_deadline,
                    )
                    .await,
                };
            }
        };
        if matches!(
            status,
            FrameworkComponentStatus::Failed { .. }
                | FrameworkComponentStatus::Panicked { .. }
                | FrameworkComponentStatus::TimedOut
        ) {
            let cleanup = run_forced(
                component,
                force_budget(configured_timeout, force_reserve, hard_deadline),
                configured_timeout,
                hard_deadline,
            )
            .await;
            (status, cleanup)
        } else {
            (status, FrameworkForcedCleanupStatus::NotAttempted)
        }
    };

    FrameworkComponentReport {
        phase,
        name,
        configured_timeout,
        elapsed: started.elapsed(),
        status,
        forced_cleanup,
    }
}

fn force_reserve(deadline: Duration) -> Duration {
    (deadline / 4).min(MAX_FORCE_RESERVE)
}

fn force_budget(
    configured_timeout: Duration,
    reserve: Duration,
    hard_deadline: Instant,
) -> Duration {
    configured_timeout
        .min(reserve)
        .min(hard_deadline.saturating_duration_since(Instant::now()))
}

async fn run_forced(
    component: &mut dyn FrameworkShutdownComponent,
    budget: Duration,
    configured_timeout: Duration,
    hard_deadline: Instant,
) -> FrameworkForcedCleanupStatus {
    let now = Instant::now();
    let deadline = component
        .force_deadline()
        .unwrap_or(now + budget)
        .min(now.checked_add(configured_timeout).unwrap_or(hard_deadline))
        .min(hard_deadline);
    component.set_force_deadline(deadline);
    let Some(shutdown) = component.force_shutdown() else {
        return FrameworkForcedCleanupStatus::Unavailable;
    };
    if Instant::now() >= deadline {
        return FrameworkForcedCleanupStatus::TimedOut;
    }
    let shutdown = std::panic::AssertUnwindSafe(shutdown).catch_unwind();
    map_forced_result(timeout_at(deadline, shutdown).await)
}

type ShutdownFutureResult =
    Result<Result<Result<(), ShutdownError>, Box<dyn Any + Send>>, tokio::time::error::Elapsed>;

fn map_result(result: ShutdownFutureResult) -> FrameworkComponentStatus {
    match result {
        Ok(Ok(Ok(()))) => FrameworkComponentStatus::Completed,
        Ok(Ok(Err(ShutdownError::ActionPanicked { message, .. }))) => {
            FrameworkComponentStatus::Panicked { message }
        }
        Ok(Ok(Err(error))) => FrameworkComponentStatus::Failed {
            error: error.to_string(),
        },
        Ok(Err(payload)) => FrameworkComponentStatus::Panicked {
            message: panic_message(payload),
        },
        Err(_) => FrameworkComponentStatus::TimedOut,
    }
}

fn map_forced_result(result: ShutdownFutureResult) -> FrameworkForcedCleanupStatus {
    match result {
        Ok(Ok(Ok(()))) => FrameworkForcedCleanupStatus::Completed,
        Ok(Ok(Err(ShutdownError::ActionPanicked { message, .. }))) => {
            FrameworkForcedCleanupStatus::Panicked { message }
        }
        Ok(Ok(Err(error))) => FrameworkForcedCleanupStatus::Failed {
            error: error.to_string(),
        },
        Ok(Err(payload)) => FrameworkForcedCleanupStatus::Panicked {
            message: panic_message(payload),
        },
        Err(_) => FrameworkForcedCleanupStatus::TimedOut,
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn record_metric(metrics: &mut FrameworkShutdownMetricSnapshot, status: &FrameworkComponentStatus) {
    match status {
        FrameworkComponentStatus::Completed => metrics.completed += 1,
        FrameworkComponentStatus::Failed { .. } => metrics.failed += 1,
        FrameworkComponentStatus::Panicked { .. } => metrics.panicked += 1,
        FrameworkComponentStatus::TimedOut => metrics.timed_out += 1,
        FrameworkComponentStatus::CancelledByForce => metrics.cancelled_by_force += 1,
    }
}

fn record_forced_cleanup_metric(
    metrics: &mut FrameworkShutdownMetricSnapshot,
    status: &FrameworkForcedCleanupStatus,
) {
    if status.was_attempted() {
        metrics.forced_cleanup_attempted += 1;
    }
    match status {
        FrameworkForcedCleanupStatus::NotAttempted => {}
        FrameworkForcedCleanupStatus::Completed => metrics.forced_cleanup_completed += 1,
        FrameworkForcedCleanupStatus::Failed { .. } => metrics.forced_cleanup_failed += 1,
        FrameworkForcedCleanupStatus::Panicked { .. } => metrics.forced_cleanup_panicked += 1,
        FrameworkForcedCleanupStatus::TimedOut => metrics.forced_cleanup_timed_out += 1,
        FrameworkForcedCleanupStatus::Unavailable => metrics.forced_cleanup_unavailable += 1,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    struct ReservedForceOwner {
        cutoff: Option<Instant>,
        finish: Option<Instant>,
        observed: Arc<Mutex<Option<Instant>>>,
        entered: Arc<AtomicBool>,
    }

    #[async_trait]
    impl FrameworkShutdownComponent for ReservedForceOwner {
        async fn shutdown(&mut self) -> Result<(), ShutdownError> {
            unreachable!("test starts with force already requested")
        }
        fn name(&self) -> &str {
            "reserved-force-owner"
        }
        fn phase(&self) -> FrameworkShutdownPhase {
            FrameworkShutdownPhase::DrainInFlight
        }
        fn timeout(&self) -> Duration {
            Duration::from_secs(1)
        }
        fn force_deadline(&self) -> Option<Instant> {
            self.cutoff
        }
        fn set_force_deadline(&mut self, deadline: Instant) {
            *self.observed.lock().unwrap() = Some(deadline);
        }
        fn force_shutdown(&mut self) -> Option<FrameworkForceShutdownFuture<'_>> {
            Some(Box::pin(async move {
                self.entered.store(true, Ordering::Release);
                match self.finish {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
                Ok(())
            }))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn owner_phase_force_cutoff_preserves_an_early_cooperative_window() {
        let started = Instant::now();
        let state = Arc::new(ShutdownState::new());
        state.request_force();
        let observed = Arc::new(Mutex::new(None));
        let entered = Arc::new(AtomicBool::new(false));
        let mut coordinator = FrameworkShutdownCoordinator::before(
            state,
            Duration::from_secs(1),
            started + Duration::from_millis(95),
            started + Duration::from_millis(98),
        );
        coordinator.register(ReservedForceOwner {
            cutoff: Some(started + Duration::from_millis(90)),
            finish: Some(started + Duration::from_millis(60)),
            observed: observed.clone(),
            entered: entered.clone(),
        });
        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert_eq!(report.force_reserve, Duration::from_millis(3));
        assert_eq!(
            *observed.lock().unwrap(),
            Some(started + Duration::from_millis(90))
        );
        assert!(entered.load(Ordering::Acquire));
        assert!(report.is_terminal_complete());
        assert!(Instant::now() < started + Duration::from_millis(90));
    }

    #[tokio::test(start_paused = true)]
    async fn owner_force_cutoff_is_absolute_clamped_and_default_policy_stays_bounded() {
        for (offset, effective, polls) in [
            (Some(70), 70, true),
            (Some(1000), 98, true),
            (Some(0), 0, false),
            (None, 3, true),
        ] {
            let started = Instant::now();
            let state = Arc::new(ShutdownState::new());
            state.request_force();
            let observed = Arc::new(Mutex::new(None));
            let entered = Arc::new(AtomicBool::new(false));
            let mut coordinator = FrameworkShutdownCoordinator::before(
                state,
                Duration::from_secs(1),
                started + Duration::from_millis(95),
                started + Duration::from_millis(98),
            );
            coordinator.register(ReservedForceOwner {
                cutoff: offset.map(|millis| started + Duration::from_millis(millis)),
                finish: None,
                observed: observed.clone(),
                entered: entered.clone(),
            });
            let report = coordinator.execute_report(ShutdownSignal::Manual).await;
            assert!(!report.is_terminal_complete());
            assert_eq!(
                *observed.lock().unwrap(),
                Some(started + Duration::from_millis(effective))
            );
            assert_eq!(entered.load(Ordering::Acquire), polls);
            tokio::time::advance(Duration::from_secs(1)).await;
            let now = Instant::now();
            let replay = coordinator.execute_report(ShutdownSignal::Quit).await;
            assert_eq!(replay.elapsed, report.elapsed);
            assert_eq!(Instant::now(), now);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn existing_absolute_deadlines_do_not_restart_at_coordinator_entry() {
        let start = Instant::now();
        let graceful = start + Duration::from_millis(30);
        let hard = start + Duration::from_millis(50);
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut coordinator = FrameworkShutdownCoordinator::before(
            Arc::new(ShutdownState::new()),
            Duration::from_secs(30),
            graceful,
            hard,
        );
        coordinator.register(component(
            "absolute-probe",
            FrameworkShutdownPhase::DrainInFlight,
            &order,
            Behavior::WaitForForce,
        ));
        tokio::time::advance(Duration::from_millis(20)).await;
        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert_eq!(Instant::now(), graceful);
        assert_eq!(report.force_reserve, Duration::from_millis(20));
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::ForcedCompleted
        );
        assert_eq!(
            order.lock().unwrap().as_slice(),
            &["absolute-probe", "absolute-probe"]
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        let replay = coordinator.execute_report(ShutdownSignal::Quit).await;
        assert_eq!(replay.signal, ShutdownSignal::Manual);
        assert_eq!(order.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_absolute_deadline_cannot_start_a_normal_callback() {
        let deadline = Instant::now();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut coordinator = FrameworkShutdownCoordinator::before(
            Arc::new(ShutdownState::new()),
            Duration::from_secs(30),
            deadline + Duration::from_secs(10),
            deadline,
        );
        coordinator.register(component(
            "unstarted",
            FrameworkShutdownPhase::DrainInFlight,
            &order,
            Behavior::Complete,
        ));
        tokio::time::advance(Duration::from_secs(1)).await;
        let now = Instant::now();
        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert_eq!(Instant::now(), now);
        assert!(order.lock().unwrap().is_empty());
        assert!(!report.is_terminal_complete());
        assert_eq!(report.force_reserve, Duration::ZERO);
    }

    struct RecordingComponent {
        name: &'static str,
        phase: FrameworkShutdownPhase,
        order: Arc<Mutex<Vec<&'static str>>>,
        behavior: Behavior,
        force_cancelled: Arc<AtomicBool>,
    }

    enum Behavior {
        Complete,
        Fail,
        Panic,
        WaitForForce,
    }

    #[async_trait]
    impl FrameworkShutdownComponent for RecordingComponent {
        async fn shutdown(&mut self) -> Result<(), ShutdownError> {
            self.order.lock().unwrap().push(self.name);
            match self.behavior {
                Behavior::Complete => Ok(()),
                Behavior::Fail => Err(ShutdownError::Component("expected failure".into())),
                Behavior::Panic => panic!("expected panic"),
                Behavior::WaitForForce => {
                    if self.force_cancelled.load(Ordering::Acquire) {
                        Ok(())
                    } else {
                        std::future::pending().await
                    }
                }
            }
        }

        fn name(&self) -> &str {
            self.name
        }

        fn phase(&self) -> FrameworkShutdownPhase {
            self.phase
        }

        fn timeout(&self) -> Duration {
            Duration::from_millis(100)
        }

        fn force_shutdown(&mut self) -> Option<FrameworkForceShutdownFuture<'_>> {
            if !matches!(self.behavior, Behavior::WaitForForce) {
                return None;
            }
            Some(Box::pin(async move {
                self.force_cancelled.store(true, Ordering::Release);
                self.shutdown().await
            }))
        }
    }

    fn component(
        name: &'static str,
        phase: FrameworkShutdownPhase,
        order: &Arc<Mutex<Vec<&'static str>>>,
        behavior: Behavior,
    ) -> RecordingComponent {
        RecordingComponent {
            name,
            phase,
            order: Arc::clone(order),
            behavior,
            force_cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retained_owners_receive_root_limits_and_the_exact_component_force_cap() {
        struct Probe {
            observed: Arc<Mutex<Vec<(Instant, Instant)>>>,
            root: Option<(Instant, Instant)>,
            force: Option<Instant>,
        }
        #[async_trait]
        impl FrameworkShutdownComponent for Probe {
            async fn shutdown(&mut self) -> Result<(), ShutdownError> {
                assert!(
                    self.root.is_some(),
                    "limits must precede the first shutdown poll"
                );
                std::future::pending().await
            }
            fn name(&self) -> &str {
                "deadline-probe"
            }
            fn phase(&self) -> FrameworkShutdownPhase {
                FrameworkShutdownPhase::DrainInFlight
            }
            fn timeout(&self) -> Duration {
                Duration::from_millis(100)
            }
            fn set_shutdown_deadlines(&mut self, graceful: Instant, hard: Instant) {
                self.root = Some((graceful, hard));
                self.observed.lock().unwrap().push((graceful, hard));
            }
            fn set_force_deadline(&mut self, deadline: Instant) {
                self.force = Some(deadline);
            }
            fn force_shutdown(&mut self) -> Option<FrameworkForceShutdownFuture<'_>> {
                let deadline = self
                    .force
                    .expect("force cap must precede its synchronous request");
                assert!(deadline <= self.root.unwrap().1);
                self.observed
                    .lock()
                    .unwrap()
                    .push((Instant::now(), deadline));
                Some(Box::pin(async { Ok(()) }))
            }
        }
        let start = Instant::now();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::new(ShutdownState::new()),
            Duration::from_millis(400),
        );
        coordinator.register(Probe {
            observed: Arc::clone(&observed),
            root: None,
            force: None,
        });
        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::ForcedCompleted
        );
        let limits = observed.lock().unwrap();
        assert_eq!(
            limits[0],
            (
                start + Duration::from_millis(300),
                start + Duration::from_millis(400)
            )
        );
        assert_eq!(limits[1].1, limits[1].0 + Duration::from_millis(100));
        assert!(limits[1].0 <= start + Duration::from_millis(101));
    }

    #[tokio::test]
    async fn canonical_phases_and_lifo_components_produce_one_reconciled_report() {
        let state = Arc::new(ShutdownState::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut coordinator =
            FrameworkShutdownCoordinator::new(Arc::clone(&state), Duration::from_secs(60));
        coordinator.register(component(
            "dependency-a",
            FrameworkShutdownPhase::DisposeDependencies,
            &order,
            Behavior::Complete,
        ));
        coordinator.register(component(
            "dependency-b",
            FrameworkShutdownPhase::DisposeDependencies,
            &order,
            Behavior::Fail,
        ));
        coordinator.register(component(
            "telemetry",
            FrameworkShutdownPhase::FlushTelemetry,
            &order,
            Behavior::Panic,
        ));

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert!(!state.is_ready());
        assert!(!state.is_accepting_connections());
        assert_eq!(
            report
                .phases
                .iter()
                .map(|phase| phase.phase)
                .collect::<Vec<_>>(),
            FrameworkShutdownPhase::ORDERED
        );
        assert_eq!(
            *order.lock().unwrap(),
            vec!["dependency-b", "dependency-a", "telemetry"]
        );
        assert_eq!(report.metrics.completed, 1);
        assert_eq!(report.metrics.failed, 1);
        assert_eq!(report.metrics.panicked, 1);
        assert_eq!(report.metrics.forced_cleanup_attempted, 0);
        assert_eq!(report.metrics.forced_cleanup_unavailable, 2);
        assert_eq!(report.completion, FrameworkShutdownCompletion::Incomplete);
        assert!(!report.is_terminal_complete());
        assert!(report.reconciles());

        let replay = coordinator.execute_report(ShutdownSignal::Quit).await;
        assert_eq!(replay, report);
        assert_eq!(
            *order.lock().unwrap(),
            vec!["dependency-b", "dependency-a", "telemetry"]
        );
    }

    #[tokio::test]
    async fn second_signal_forces_the_active_component_then_awaits_cleanup() {
        let state = Arc::new(ShutdownState::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut coordinator =
            FrameworkShutdownCoordinator::new(Arc::clone(&state), Duration::from_secs(1));
        coordinator.register(component(
            "consumer-tasks",
            FrameworkShutdownPhase::DrainInFlight,
            &order,
            Behavior::WaitForForce,
        ));
        let force_state = Arc::clone(&state);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            force_state.request_force();
        });

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert!(report.forced);
        assert_eq!(report.metrics.cancelled_by_force, 1);
        assert_eq!(report.metrics.forced_cleanup_completed, 1);
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::ForcedCompleted
        );
        assert!(report.is_terminal_complete());
        assert!(!report.is_graceful());
        assert!(report.reconciles());
    }

    #[tokio::test]
    async fn owned_task_set_cancels_and_joins_every_task() {
        let state = Arc::new(ShutdownState::new());
        let exited = Arc::new(AtomicUsize::new(0));
        let mut tasks = OwnedTaskSet::new(
            "background-workers",
            FrameworkShutdownPhase::DrainInFlight,
            Duration::from_secs(1),
        );
        for _ in 0..3 {
            let cancellation = tasks.cancellation_token();
            let exited = Arc::clone(&exited);
            tasks.spawn(async move {
                cancellation.cancelled().await;
                exited.fetch_add(1, Ordering::AcqRel);
                Ok(())
            });
        }
        let mut coordinator = FrameworkShutdownCoordinator::new(state, Duration::from_secs(1));
        coordinator.register(tasks);

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert!(report.is_graceful());
        assert!(report.is_terminal_complete());
        assert_eq!(exited.load(Ordering::Acquire), 3);
        assert!(report.reconciles());
    }

    #[tokio::test]
    async fn graceful_timeout_reserves_time_to_abort_and_join_owned_tasks() {
        struct DropWitness(Arc<AtomicUsize>);

        impl Drop for DropWitness {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let state = Arc::new(ShutdownState::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut tasks = OwnedTaskSet::new(
            "uncooperative-task",
            FrameworkShutdownPhase::DrainInFlight,
            Duration::from_millis(5),
        );
        let witness = DropWitness(Arc::clone(&dropped));
        tasks.spawn(async move {
            let _witness = witness;
            std::future::pending::<()>().await;
            Ok(())
        });
        tokio::task::yield_now().await;

        let mut coordinator = FrameworkShutdownCoordinator::new(state, Duration::from_millis(40));
        coordinator.register(tasks);
        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        let component = &report
            .phases
            .iter()
            .find(|phase| phase.phase == FrameworkShutdownPhase::DrainInFlight)
            .unwrap()
            .components[0];

        assert_eq!(component.status, FrameworkComponentStatus::TimedOut);
        assert_eq!(
            component.forced_cleanup,
            FrameworkForcedCleanupStatus::Completed
        );
        assert_eq!(dropped.load(Ordering::Acquire), 1);
        assert_eq!(report.metrics.timed_out, 1);
        assert_eq!(report.metrics.forced_cleanup_attempted, 1);
        assert_eq!(report.metrics.forced_cleanup_completed, 1);
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::ForcedCompleted
        );
        assert!(report.is_terminal_complete());
        assert!(report.reconciles());
    }

    #[tokio::test]
    async fn configured_deadline_is_not_clamped_by_signal_type() {
        let deadline = Duration::from_secs(120);
        for signal in [
            ShutdownSignal::Interrupt,
            ShutdownSignal::Terminate,
            ShutdownSignal::Manual,
        ] {
            let state = Arc::new(ShutdownState::new());
            let mut coordinator = FrameworkShutdownCoordinator::new(state, deadline);
            let report = coordinator.execute_report(signal).await;
            assert_eq!(report.deadline, deadline);
            assert_eq!(report.force_reserve, MAX_FORCE_RESERVE);
            assert!(report.is_graceful());
        }
    }

    #[tokio::test]
    async fn unrepresentable_deadline_fails_closed_without_panicking() {
        let state = Arc::new(ShutdownState::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut coordinator = FrameworkShutdownCoordinator::new(state, Duration::MAX);
        coordinator.register(component(
            "unrepresentable-deadline",
            FrameworkShutdownPhase::DisposeDependencies,
            &order,
            Behavior::Complete,
        ));

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        let component = &report
            .phases
            .iter()
            .find(|phase| phase.phase == FrameworkShutdownPhase::DisposeDependencies)
            .expect("the dependency phase must be reported")
            .components[0];

        assert_eq!(report.deadline, Duration::MAX);
        assert_eq!(component.status, FrameworkComponentStatus::TimedOut);
        assert!(order.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn shutdown_action_replays_failure_without_reinvoking_fn_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let action_calls = Arc::clone(&calls);
        let mut action = ShutdownAction::new(
            "replay-failure",
            FrameworkShutdownPhase::DisposeDependencies,
            Duration::from_secs(1),
            move || async move {
                action_calls.fetch_add(1, Ordering::AcqRel);
                Err(ShutdownError::Component("expected".into()))
            },
        );

        let first = action.shutdown().await.unwrap_err().to_string();
        let replay = action.shutdown().await.unwrap_err().to_string();
        assert_eq!(first, replay);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert!(matches!(
            action.terminal_outcome(),
            Some(ShutdownActionTerminal::Failed { .. })
        ));
    }

    #[tokio::test]
    async fn consumed_action_without_force_handle_is_never_false_completed() {
        let state = Arc::new(ShutdownState::new());
        let action = ShutdownAction::new(
            "one-shot",
            FrameworkShutdownPhase::DisposeDependencies,
            Duration::from_millis(5),
            || async {
                std::future::pending::<()>().await;
                Ok(())
            },
        );
        let mut coordinator = FrameworkShutdownCoordinator::new(state, Duration::from_millis(40));
        coordinator.register(action);

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        let component = &report
            .phases
            .iter()
            .find(|phase| phase.phase == FrameworkShutdownPhase::DisposeDependencies)
            .unwrap()
            .components[0];
        assert_eq!(component.status, FrameworkComponentStatus::TimedOut);
        assert_eq!(
            component.forced_cleanup,
            FrameworkForcedCleanupStatus::Unavailable
        );
        assert_eq!(report.completion, FrameworkShutdownCompletion::Incomplete);
        assert_eq!(report.metrics.completed, 0);
        assert_eq!(report.metrics.forced_cleanup_unavailable, 1);
    }

    #[tokio::test]
    async fn shutdown_action_uses_only_its_explicit_force_handle() {
        let graceful_calls = Arc::new(AtomicUsize::new(0));
        let force_calls = Arc::new(AtomicUsize::new(0));
        let graceful_witness = Arc::clone(&graceful_calls);
        let force_witness = Arc::clone(&force_calls);
        let action = ShutdownAction::new(
            "forceable-action",
            FrameworkShutdownPhase::DisposeDependencies,
            Duration::from_millis(5),
            move || async move {
                graceful_witness.fetch_add(1, Ordering::AcqRel);
                std::future::pending::<()>().await;
                Ok(())
            },
        )
        .with_force(move || async move {
            force_witness.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::new(ShutdownState::new()),
            Duration::from_millis(40),
        );
        coordinator.register(action);

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert_eq!(graceful_calls.load(Ordering::Acquire), 1);
        assert_eq!(force_calls.load(Ordering::Acquire), 1);
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::ForcedCompleted
        );
        assert!(report.is_terminal_complete());
    }
}
