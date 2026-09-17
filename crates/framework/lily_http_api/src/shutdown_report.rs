//! HTTP-specific, fixed-size shutdown evidence. No handles, URLs, request IDs,
//! provider errors or application labels enter this report or public health.
//! It describes observed resource boundaries, never peer delivery or rollback.

use crate::{request_lifecycle::RequestOwnerSnapshot, tasks::TaskSnapshot};
use lily_trace::lifecycle::TracingWorkerSnapshot;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DependencyDisposition {
    NotOwned,
    NotStarted,
    Outstanding,
    Succeeded,
    Failed,
    Cancelled,
    Panicked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DependencySnapshot {
    pub(crate) disposition: DependencyDisposition,
    pub(crate) terminal: bool,
    // Waiter expiry is sticky even if the retained receipt later succeeds.
    pub(crate) wait_failed: bool,
    pub(crate) tasks: TaskSnapshot,
}

impl DependencySnapshot {
    pub(crate) fn succeeded(self) -> bool {
        self.terminal
            && !self.wait_failed
            && self.tasks.is_terminal()
            && self.tasks.panicked == 0
            && self.tasks.cancelled == 0
            && matches!(
                self.disposition,
                DependencyDisposition::NotOwned | DependencyDisposition::Succeeded
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TelemetrySnapshot {
    pub(crate) dependency: DependencySnapshot,
    pub(crate) owner_joined: bool,
    pub(crate) workers: TracingWorkerSnapshot,
    pub(crate) dropped: u64,
    pub(crate) rejected: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DependencyInventory {
    pub(crate) background: lily_background_service::BackgroundServiceSnapshot,
    pub(crate) di: DependencySnapshot,
    pub(crate) telemetry: TelemetrySnapshot,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameworkSnapshot {
    pub(crate) forced: bool,
    pub(crate) complete: bool,
    pub(crate) reconciles: bool,
    pub(crate) components: usize,
    pub(crate) metrics: lily_shutdown::FrameworkShutdownMetricSnapshot,
}

impl From<&lily_shutdown::FrameworkShutdownReport> for FrameworkSnapshot {
    fn from(report: &lily_shutdown::FrameworkShutdownReport) -> Self {
        Self {
            forced: report.forced,
            complete: report.is_terminal_complete(),
            reconciles: report.reconciles(),
            components: report.component_count(),
            metrics: report.metrics,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HttpShutdownCompletion {
    GracefulCompleted,
    ForcedCompleted,
    TerminalFailed,
    Incomplete,
}

impl HttpShutdownCompletion {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::GracefulCompleted => "graceful_completed",
            Self::ForcedCompleted => "forced_completed",
            Self::TerminalFailed => "terminal_failed",
            Self::Incomplete => "incomplete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HttpShutdownReport {
    pub(crate) observed_at: Instant,
    pub(crate) deadline: Instant,
    pub(crate) deadline_expired: bool,
    pub(crate) completion: HttpShutdownCompletion,
    pub(crate) forced: bool,
    pub(crate) admission_closed: bool,
    // Same identity set at admission close. Lifetime totals are separately
    // retained for diagnostics, never used to fail this shutdown attempt.
    pub(crate) requests: RequestOwnerSnapshot,
    pub(crate) requests_lifetime: RequestOwnerSnapshot,
    // Canonical application registries, not sums of local/parent duplicates.
    // These are lifetime task joins; ordinary HTTP error statuses aren't task failures.
    pub(crate) listener: TaskSnapshot,
    pub(crate) connections: TaskSnapshot,
    pub(crate) protocol: TaskSnapshot,
    pub(crate) monitors: TaskSnapshot,
    // The outcome of this exact observer, not a later is_finished probe.
    pub(crate) root: TaskSnapshot,
    pub(crate) dependencies: DependencyInventory,
    pub(crate) framework: Option<FrameworkSnapshot>,
}

impl HttpShutdownReport {
    pub(crate) fn reconciles(&self) -> bool {
        self.dependencies.background.reconciles()
            && self.requests.reconciles()
            && self.requests_lifetime.reconciles()
            && self
                .task_snapshots()
                .into_iter()
                .all(TaskSnapshot::reconciles)
            && self.dependencies.di.tasks.reconciles()
            && self.dependencies.telemetry.dependency.tasks.reconciles()
            && self.dependencies.telemetry.workers.registered
                == self.dependencies.telemetry.workers.joined
                    + self.dependencies.telemetry.workers.outstanding
            && self.framework.is_none_or(|report| {
                report.reconciles && report.metrics.total() == report.components
            })
    }

    fn task_snapshots(&self) -> [TaskSnapshot; 5] {
        [
            self.listener,
            self.connections,
            self.protocol,
            self.monitors,
            self.root,
        ]
    }

    pub(crate) fn terminal(&self) -> bool {
        self.reconciles()
            && self.admission_closed
            && self.root.registered == 1
            && self
                .task_snapshots()
                .into_iter()
                .all(TaskSnapshot::is_terminal)
            && self.requests_lifetime.is_terminal()
            && self.requests.is_terminal()
            && self.dependencies.di.terminal
            && self.dependencies.background.is_terminal()
            && self.dependencies.telemetry.dependency.terminal
            && self.dependencies.di.tasks.is_terminal()
            && self.dependencies.telemetry.dependency.tasks.is_terminal()
            && self.dependencies.telemetry.workers.is_terminal()
    }

    pub(crate) fn classify(&mut self, root_succeeded: bool) {
        self.completion = if !self.terminal() {
            HttpShutdownCompletion::Incomplete
        } else if !root_succeeded
            || self.deadline_expired
            || !self.requests.cleanup_succeeded()
            || self
                .task_snapshots()
                .iter()
                .any(|tasks| tasks.panicked != 0)
            || !self.dependencies.di.succeeded()
            || !self.dependencies.background.succeeded()
            || !self.dependencies.telemetry.dependency.succeeded()
            || self.dependencies.telemetry.workers.failed != 0
            || self.framework.is_some_and(|report| !report.complete)
        {
            HttpShutdownCompletion::TerminalFailed
        } else if self.forced {
            HttpShutdownCompletion::ForcedCompleted
        } else {
            HttpShutdownCompletion::GracefulCompleted
        };
    }

    pub(crate) fn succeeded(&self) -> bool {
        matches!(
            self.completion,
            HttpShutdownCompletion::GracefulCompleted | HttpShutdownCompletion::ForcedCompleted
        )
    }

    /// Owned telemetry gets only the preliminary checkpoint. After its close,
    /// the retained health/report is authoritative, without generating new loss.
    pub(crate) fn emit(&self, checkpoint: &'static str) {
        tracing::info!(
            target: "lily_http_api::shutdown",
            checkpoint, completion = self.completion.reason(), forced = self.forced,
            reconciles = self.reconciles(), terminal = self.terminal(),
            requests = self.requests.registered, requests_outstanding = self.requests.outstanding,
            execution_dropped = self.requests.execution.dropped,
            middleware_completed = self.requests.middleware.completed,
            middleware_failed = self.requests.middleware.failed,
            middleware_timed_out = self.requests.middleware.timed_out,
            middleware_panicked = self.requests.middleware.panicked,
            middleware_not_started = self.requests.middleware.not_started,
            cleanup_failed = self.requests.cleanup_failed,
            body_interrupted = self.requests.response.interrupted,
            scope_outstanding = self.requests.scopes.outstanding,
            root_joined = self.root.is_terminal(),
            transport_outstanding = self.listener.outstanding + self.connections.outstanding + self.protocol.outstanding,
            transport_abort_requested = self.listener.abort_requested + self.connections.abort_requested + self.protocol.abort_requested,
            transport_cancelled_joined = self.listener.cancelled + self.connections.cancelled + self.protocol.cancelled,
            di_terminal = self.dependencies.di.terminal,
            background_joined = self.dependencies.background.joined,
            background_outstanding = self.dependencies.background.outstanding,
            background_aborted = self.dependencies.background.aborted,
            background_scope_outstanding = self.dependencies.background.scopes.outstanding,
            telemetry_terminal = self.dependencies.telemetry.dependency.terminal,
            "HTTP shutdown evidence"
        );
    }
}
