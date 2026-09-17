//! Composition-root barriers and dependency disposal. All waits borrow
//! retained receipts; cancelling a coordinator attempt cannot detach a task.

use super::*;

const DEPENDENCY_RESERVE: Duration = Duration::from_millis(250);
const JOIN_RESERVE: Duration = Duration::from_millis(10);

pub(super) fn users_deadline(graceful: Instant, hard: Instant) -> Instant {
    // Keep dependency disposal inside H even when connection cleanup uses all
    // its allowance. Never shorten the graceful portion to reserve this tail.
    hard - (hard.saturating_duration_since(graceful) / 4).min(DEPENDENCY_RESERVE)
}

struct ReconciliationHandle {
    runtime: WsApp,
}

#[async_trait::async_trait]
impl FrameworkShutdownComponent for ReconciliationHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        self.runtime.reconcile_framework_tasks().await
    }
    fn name(&self) -> &str {
        "websocket-task-reconciliation"
    }
    fn phase(&self) -> FrameworkShutdownPhase {
        FrameworkShutdownPhase::DrainInFlight
    }
    fn timeout(&self) -> Duration {
        self.runtime.shutdown_timeout
    }
    fn force_deadline(&self) -> Option<Instant> {
        self.runtime
            .lifecycle
            .background
            .as_ref()
            .and_then(|background| background.deadlines.get().map(|limits| limits.reconcile))
    }
    fn force_shutdown(&mut self) -> Option<lily_shutdown::FrameworkForceShutdownFuture<'_>> {
        // This synchronous request also runs when the coordinator has no time
        // left to poll a force future. It never fabricates a terminal receipt.
        self.runtime.request_final_abort_if_due();
        Some(Box::pin(self.shutdown()))
    }
}

#[derive(Clone, Copy)]
enum Dependency {
    Backplane,
    Container,
    Tracing,
}

struct DependencyHandle {
    runtime: WsApp,
    dependency: Dependency,
}

#[async_trait::async_trait]
impl FrameworkShutdownComponent for DependencyHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        self.runtime.close_dependency(self.dependency).await
    }
    fn name(&self) -> &str {
        match self.dependency {
            Dependency::Backplane => "websocket-backplane",
            Dependency::Container => "websocket-di-container",
            Dependency::Tracing => "tracing-runtime",
        }
    }
    fn phase(&self) -> FrameworkShutdownPhase {
        match self.dependency {
            Dependency::Tracing => FrameworkShutdownPhase::FlushTelemetry,
            _ => FrameworkShutdownPhase::DisposeDependencies,
        }
    }
    fn timeout(&self) -> Duration {
        self.runtime.shutdown_timeout
    }
    fn set_shutdown_deadlines(&mut self, graceful: Instant, hard: Instant) {
        self.runtime.lifecycle.root_budget.configure(graceful, hard);
        // Built-but-never-started and startup-error paths have no server
        // component to configure the retained scope/receipt authority.
        self.runtime
            .scope_cleanup_registry
            .budget
            .configure(graceful, users_deadline(graceful, hard));
        self.runtime
            .dispatcher
            .set_shutdown_deadlines(graceful, hard);
        self.runtime.begin_background_shutdown(graceful, hard);
    }
    fn set_force_deadline(&mut self, deadline: Instant) {
        self.runtime.lifecycle.root_budget.force_before(deadline);
        self.runtime.dispatcher.set_force_deadline(deadline);
    }
    fn force_shutdown(&mut self) -> Option<lily_shutdown::FrameworkForceShutdownFuture<'_>> {
        Some(Box::pin(self.shutdown()))
    }
}

impl WsApp {
    pub(super) async fn recover_root_failure(&self) -> String {
        self.begin_shutdown_reporting();
        // Configure only an unpublished budget. Recovery never restarts an
        // existing root deadline, including when panic happened at its end.
        if self.lifecycle.root_budget.hard_deadline().is_none() {
            let now = Instant::now();
            let hard = now + self.shutdown_timeout;
            self.lifecycle.root_budget.configure(now, hard);
            self.scope_cleanup_registry
                .budget
                .configure(now, users_deadline(now, hard));
            self.dispatcher.set_shutdown_deadlines(now, hard);
        }
        let hard = self
            .lifecycle
            .root_budget
            .hard_deadline()
            .expect("recovery root configured");
        self.begin_background_shutdown(Instant::now().min(hard), hard);
        if let Some(background) = &self.lifecycle.background {
            background.runtime.force_stop();
        }
        self.lifecycle.root_budget.force_before(hard);
        self.dispatcher.set_force_deadline(hard);
        self.request_execution_stop();
        self.dispatcher.begin_drain();
        let mut failures = Vec::new();
        if let Err(error) = self.reconcile_framework_tasks().await {
            failures.push(error.to_string());
        }
        for dependency in [
            Dependency::Backplane,
            Dependency::Container,
            Dependency::Tracing,
        ] {
            if matches!(dependency, Dependency::Container) && !self.owns_container {
                continue;
            }
            // A second framework failure cannot suppress attempts for other
            // independent receipts, nor bypass their prerequisite gates.
            match AssertUnwindSafe(self.close_dependency(dependency))
                .catch_unwind()
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error.to_string()),
                Err(_) => failures.push("dependency recovery panicked".into()),
            }
        }
        if failures.is_empty() {
            "owned tasks and dependencies reconciled".into()
        } else {
            format!("recovery incomplete: {}", failures.join("; "))
        }
    }

    fn request_execution_stop(&self) {
        let budget = &self.scope_cleanup_registry.budget;
        if !budget.is_forced() {
            budget.force_before(budget.hard_deadline().expect("root budget configured"));
        }
        if let Some(force) = self.lifecycle.execution_force.get() {
            force.cancel();
        }
    }

    fn request_final_abort_if_due(&self) {
        let budget = &self.scope_cleanup_registry.budget;
        if budget.hard_deadline().is_some()
            && budget.cleanup_deadline(Instant::now() + self.shutdown_timeout) <= Instant::now()
        {
            self.request_execution_stop();
            self.lifecycle.server_tasks.abort_all();
            self.lifecycle.connection_tasks.abort_all();
            self.lifecycle.maintenance_tasks.abort_all();
            self.dispatcher.stop_ingress();
        }
    }

    pub(super) fn register_reconciliation(&self, coordinator: &mut FrameworkShutdownCoordinator) {
        coordinator.register(ReconciliationHandle {
            runtime: self.runtime_clone(),
        });
    }

    pub(super) fn register_dependencies(&self, coordinator: &mut FrameworkShutdownCoordinator) {
        // Components in the same phase execute in reverse registration order.
        if self.owns_container {
            coordinator.register(DependencyHandle {
                runtime: self.runtime_clone(),
                dependency: Dependency::Container,
            });
        }
        coordinator.register(DependencyHandle {
            runtime: self.runtime_clone(),
            dependency: Dependency::Backplane,
        });
        coordinator.register(DependencyHandle {
            runtime: self.runtime_clone(),
            dependency: Dependency::Tracing,
        });
    }

    fn framework_tasks_terminal(&self) -> bool {
        self.lifecycle
            .background
            .as_ref()
            .is_none_or(|background| background.is_terminal())
            && self.lifecycle.server_tasks.snapshot().outstanding == 0
            && self.lifecycle.connection_tasks.snapshot().outstanding == 0
            && self.lifecycle.maintenance_tasks.snapshot().outstanding == 0
            && self.message_dispatch_registry.is_terminal()
            && self.connection_cleanup_registry.tasks_terminal()
            && self.scope_cleanup_registry.is_terminal()
    }

    async fn reconcile_framework_tasks(&self) -> Result<(), ShutdownError> {
        let budget = &self.scope_cleanup_registry.budget;
        // A server panic can leave children running even though the server's
        // own join is ready. Message slots enforce their cooperative cutoff;
        // connection cleanup/output retains the later bounded transport tail,
        // just as it does under the normal connection drainer.
        if self.lifecycle.server_tasks.snapshot().outstanding == 0
            && self.lifecycle.connection_tasks.snapshot().outstanding > 0
        {
            self.request_execution_stop();
            tokio::select! {
                biased;
                () = self.lifecycle.connection_tasks.wait() => {},
                () = budget.transport_expired() => {},
            }
        }
        // Normal server return already includes its child cleanup. Only after
        // that allowance expires may the root stop the remaining transport and
        // supervisor tasks. Message/connection ledgers and DI receipts stay in
        // their independent registries throughout this final abort.
        if budget
            .observe_cleanup(self.lifecycle.server_tasks.wait())
            .await
            .is_err()
        {
            self.lifecycle.server_tasks.abort_all();
        }
        self.lifecycle.connection_tasks.abort_all();
        self.lifecycle.maintenance_tasks.abort_all();
        self.dispatcher.stop_ingress();
        let _ = budget
            .reconcile(async {
                self.lifecycle.server_tasks.wait().await;
                self.lifecycle.connection_tasks.wait().await;
                self.lifecycle.maintenance_tasks.wait().await;
            })
            .await;

        // No connection producer can now register another message or cleanup
        // worker. An unconfirmed abort cannot authorize dependency disposal.
        let producers_terminal = self.lifecycle.server_tasks.snapshot().outstanding == 0
            && self.lifecycle.connection_tasks.snapshot().outstanding == 0
            && self.lifecycle.maintenance_tasks.snapshot().outstanding == 0;
        if producers_terminal {
            self.message_dispatch_registry.reconcile().await;
            let force = CancellationToken::new();
            force.cancel();
            let report = self
                .connection_cleanup_registry
                .finalize_all_with_force(
                    crate::middleware::WsConnectionCloseCategory::ServerShutdown,
                    None,
                    &force,
                )
                .await;
            let scopes = self.scope_cleanup_registry.drain().await;
            self.connection_cleanup_registry.reconcile(budget).await;
            let cleanup_failed = report.prerequisites_incomplete > 0
                || report.unreconciled_hook_failures() > 0
                || report.unreconciled_terminal_failures() > 0
                || report.manager_cleanup_failed > 0
                || report.panicked > 0
                || scopes.deadline_failures > 0;
            if cleanup_failed {
                self.lifecycle
                    .reconciliation_failed
                    .store(true, Ordering::Release);
                tracing::error!(
                    ?report,
                    "WebSocket final cleanup obligations remain incomplete"
                );
            }
        }
        let ingress_terminal = self.dispatcher.reconcile_ingress().await;
        let background_result = self.reconcile_background().await;
        if !self.framework_tasks_terminal() || !ingress_terminal {
            return Err(ShutdownError::Component(format!(
                "framework task termination unconfirmed; dependencies retained (server={:?}, connections={:?}, maintenance={:?}, message_terminal={}, cleanup_terminal={}, scopes_terminal={}, ingress_terminal={ingress_terminal})",
                self.lifecycle.server_tasks.snapshot(),
                self.lifecycle.connection_tasks.snapshot(),
                self.lifecycle.maintenance_tasks.snapshot(),
                self.message_dispatch_registry.is_terminal(),
                self.connection_cleanup_registry.tasks_terminal(),
                self.scope_cleanup_registry.is_terminal(),
            )));
        }
        // A manager sweep is safe only after every possible publisher/user and
        // cleanup worker has an observed terminal receipt.
        budget
            .reconcile(self.connection_manager.remove_all_connections())
            .await
            .map_err(|_| {
                ShutdownError::Component("manager reconciliation deadline elapsed".into())
            })?;
        self.connection_cleanup_registry.release_terminal_entries();
        background_result?;
        if self.lifecycle.reconciliation_failed.load(Ordering::Acquire)
            || self.message_dispatch_registry.cleanup_failures() > 0
        {
            return Err(ShutdownError::Component(
                "framework task joins completed with incomplete lifecycle cleanup".into(),
            ));
        }
        if self.lifecycle.server_tasks.snapshot().panicked > 0
            || self.lifecycle.connection_tasks.snapshot().panicked > 0
            || self.lifecycle.maintenance_tasks.snapshot().panicked > 0
            || self.message_dispatch_registry.owner_panics() > 0
            || self.dispatcher.ingress_panicked()
        {
            return Err(ShutdownError::Component(
                "framework task panic observed during final joins".into(),
            ));
        }
        Ok(())
    }

    async fn close_dependency(&self, dependency: Dependency) -> Result<(), ShutdownError> {
        let failure = |detail: &str| ShutdownError::Component(detail.to_owned());
        if !self.framework_tasks_terminal() {
            return Err(failure(
                "dependency cleanup not started: framework tasks are not terminal",
            ));
        }
        let budget = &self.lifecycle.root_budget;
        let hard = budget
            .hard_deadline()
            .expect("root deadline configured before dependency cleanup");
        match dependency {
            Dependency::Backplane => {
                if Instant::now() >= hard && !self.dispatcher.close_terminal() {
                    // Reap an existing owner only. No provider-close task may
                    // be introduced after the composition-root deadline.
                    let _ = self.dispatcher.abort_and_join_backplane_close().await;
                    return Err(failure(
                        "backplane close not started or unconfirmed at root deadline",
                    ));
                }
                let result = match budget
                    .observe_cleanup(self.dispatcher.close_backplane())
                    .await
                {
                    Ok(result) => result,
                    Err(()) => self.dispatcher.abort_and_join_backplane_close().await,
                };
                result.map_err(|error| {
                    failure(&format!(
                        "WebSocket backplane close failed ({})",
                        error.kind().as_str()
                    ))
                })
            }
            Dependency::Container => {
                if !self.dispatcher.close_terminal() || !self.dispatcher.dependency_users_terminal()
                {
                    return Err(failure(
                        "DI cleanup not started: backplane users/close task are not terminal",
                    ));
                }
                if Instant::now() >= hard
                    && !lily_injection::__private::container_shutdown_started(&self.container)
                {
                    return Err(failure("DI cleanup not started: root deadline elapsed"));
                }
                // The container owns its close task. Keep its absolute work
                // cutoff before H, and observe the retained actual join at H.
                let close_at =
                    hard - (hard.saturating_duration_since(Instant::now()) / 4).min(JOIN_RESERVE);
                budget
                    .reconcile(self.container.close_before(close_at))
                    .await
                    .map_err(|_| failure("DI close termination unconfirmed; receipt retained"))?
                    .map(|_| ())
                    .map_err(|error| failure(&error.to_string()))
            }
            Dependency::Tracing => {
                let background = self.background_snapshot();
                reporting::diagnostic(|| {
                    tracing::info!(target: "lily_websocket::shutdown", evidence = ?self.shutdown_evidence(),
                    checkpoint = "before_telemetry",
                    background_joined = background.joined as u64,
                    background_failed = background.failed as u64,
                    background_panicked = background.panicked as u64,
                    background_aborted = background.aborted as u64,
                    background_outstanding = background.outstanding as u64,
                    background_scope_outstanding = background.scopes.outstanding as u64,
                    "WebSocket shutdown evidence before telemetry flush")
                });
                if !self.dispatcher.close_terminal()
                    || !self.dispatcher.dependency_users_terminal()
                    || (self.owns_container
                        && !lily_injection::__private::container_shutdown_quiescent(
                            &self.container,
                        ))
                {
                    return Err(failure(
                        "telemetry cleanup not started: dependency termination unconfirmed",
                    ));
                }
                let mut owner = self.lifecycle.tracing_owner.lock().await;
                if self.lifecycle.tracing_task.get().is_none() {
                    if owner.is_none() {
                        return Ok(());
                    }
                    if Instant::now() >= hard {
                        return Err(failure(
                            "telemetry cleanup not started: root deadline elapsed",
                        ));
                    }
                    let owned = owner
                        .take()
                        .expect("tracing owner retained until prerequisites pass");
                    let remaining = hard.saturating_duration_since(Instant::now());
                    let work_budget = remaining - (remaining / 4).min(JOIN_RESERVE);
                    let task = TaskReceipt::from(lily_trace::spawn(owned.shutdown(work_budget)));
                    assert!(self.lifecycle.tracing_task.set(task).is_ok());
                }
                drop(owner);
                let task = self
                    .lifecycle
                    .tracing_task
                    .get()
                    .expect("tracing receipt installed")
                    .clone();
                let result = budget
                    .reconcile(task)
                    .await
                    .map_err(|_| {
                        failure("telemetry close termination unconfirmed; receipt retained")
                    })?
                    .map_err(|_| failure("telemetry close task panicked or was cancelled"))?;
                if result.is_success() {
                    Ok(())
                } else {
                    Err(failure(&format!("tracing shutdown incomplete: {result:?}")))
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "reconciliation_tests.rs"]
mod tests;
