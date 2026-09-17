use super::*;
use crate::shutdown_report::{HttpShutdownCompletion, HttpShutdownReport};
use crate::tasks::TaskSnapshot;

/// One immutable result and evidence publication, including root join outcome.
/// Late reconciliation updates the real owners only, never this attempt.
pub(super) struct HttpFrozenAttempt {
    pub(super) outcome: HttpLifecycleOutcome,
    pub(super) report: HttpShutdownReport,
}

impl HttpFrozenAttempt {
    pub(super) fn as_result(&self) -> io::Result<()> {
        self.outcome.as_result()
    }
}

#[cfg(test)]
impl App {
    pub(crate) fn shutdown_report(&self) -> Option<HttpShutdownReport> {
        self.lifecycle.terminal.get().map(|attempt| attempt.report)
    }
}

impl AppLifecycleState {
    pub(super) fn install_report_observer(
        self: &Arc<Self>,
        receipt: TaskReceipt<HttpLifecycleOutcome>,
    ) {
        let lifecycle = Arc::downgrade(self);
        let health = Arc::downgrade(&self.health);
        let terminal = self.terminal.clone();
        let pre_join = self.pre_join_report.clone();
        self.health.observe_shutdown_with(move || {
            if terminal.get().is_some() {
                return;
            }
            if let Some(lifecycle) = lifecycle
                .upgrade()
                .filter(|_| tokio::runtime::Handle::try_current().is_ok())
            {
                let observed = receipt.clone().now_or_never();
                if let Some(result) = observed {
                    lifecycle.root_join_observed.store(true, Ordering::Release);
                    lifecycle.freeze_attempt(join_result(result), receipt.snapshot());
                } else if lifecycle
                    .budget
                    .deadlines()
                    .is_some_and(|d| tokio::time::Instant::now() >= d.at(ShutdownStage::Final))
                {
                    lifecycle.freeze_attempt(
                        join_timeout(),
                        TaskSnapshot {
                            registered: 1,
                            outstanding: 1,
                            ..Default::default()
                        },
                    );
                }
            } else if let (Some(mut report), Some(result), Some(health)) = (
                pre_join.get().copied(),
                receipt.clone().now_or_never(),
                health.upgrade(),
            ) {
                // A consumed App and every start/close waiter may be gone.
                // Retain just values and this actual root receipt, not the App
                // or its DI graph. No detached receipt-driver task is needed.
                report.root = receipt.snapshot();
                report.observed_at = tokio::time::Instant::now();
                report.deadline_expired |= report.observed_at > report.deadline;
                let attempt = terminal.get_or_init(|| freeze_report(join_result(result), report));
                health.record_http_shutdown(&attempt.report);
            }
        });
    }

    pub(super) fn observe_report(
        &self,
        root: TaskSnapshot,
        root_succeeded: bool,
    ) -> HttpShutdownReport {
        let deadline = self.budget.begin().at(ShutdownStage::Final);
        let framework = self.framework_report.get().copied();
        let requests_lifetime = self.requests.snapshot();
        let requests = self.requests.attempt_snapshot();
        let mut report = HttpShutdownReport {
            observed_at: tokio::time::Instant::now(),
            deadline,
            deadline_expired: false,
            completion: HttpShutdownCompletion::Incomplete,
            forced: self.force.is_cancelled()
                || self.dependencies.snapshot().background.abort_requested != 0
                || self.shutdown_state.is_force_requested()
                || requests.shutdown_cancelled != 0
                || framework.is_some_and(|report| report.forced),
            admission_closed: self.requests.admission_closed(),
            requests,
            requests_lifetime,
            listener: self.tasks.listener.snapshot(),
            connections: self.tasks.connections.snapshot(),
            protocol: self.tasks.protocol.snapshot(),
            monitors: self.tasks.monitors.snapshot(),
            root,
            dependencies: self.dependencies.snapshot(),
            framework,
        };
        report.observed_at = tokio::time::Instant::now();
        report.deadline_expired = report.observed_at > deadline;
        report.classify(root_succeeded);
        report
    }

    pub(super) fn freeze_attempt(
        &self,
        outcome: HttpLifecycleOutcome,
        root: TaskSnapshot,
    ) -> &HttpFrozenAttempt {
        let attempt = self.terminal.get_or_init(|| {
            let report =
                self.observe_report(root, matches!(outcome, HttpLifecycleOutcome::Completed));
            freeze_report(outcome, report)
        });
        self.health.record_http_shutdown(&attempt.report);
        attempt
    }
}

fn join_result(result: crate::tasks::TaskResult<HttpLifecycleOutcome>) -> HttpLifecycleOutcome {
    match result {
        Ok(outcome) => outcome.as_ref().clone(),
        Err(error) => HttpLifecycleOutcome::from_result(Err(io::Error::other(format!(
            "HTTP lifecycle root join failed: {error}"
        )))),
    }
}

fn join_timeout() -> HttpLifecycleOutcome {
    HttpLifecycleOutcome::from_result(Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "HTTP lifecycle root join remains outstanding at the absolute shutdown deadline",
    )))
}

fn freeze_report(
    outcome: HttpLifecycleOutcome,
    mut report: HttpShutdownReport,
) -> HttpFrozenAttempt {
    report.classify(matches!(outcome, HttpLifecycleOutcome::Completed));
    let outcome = if matches!(outcome, HttpLifecycleOutcome::Completed) && !report.succeeded() {
        HttpLifecycleOutcome::Failed {
            kind: if report.deadline_expired {
                io::ErrorKind::TimedOut
            } else {
                io::ErrorKind::Other
            },
            message: format!("HTTP shutdown evidence: {}", report.completion.reason()),
        }
    } else {
        outcome
    };
    HttpFrozenAttempt { outcome, report }
}
