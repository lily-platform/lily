//! WebSocket ownership of the protocol-independent background runtime.

use super::*;
use lily_background_service::{
    BackgroundServiceRuntime, BackgroundServiceSnapshot, BackgroundShutdownDeadlines,
};
use std::sync::OnceLock;

pub(super) const HEALTH_CHECK: &str = "websocket.background_services";

/// Keep workers inside the root's original D/H interval, with a dependency
/// tail reserved by the same policy as WebSocket connection cleanup.
pub(super) fn deadlines(graceful: Instant, hard: Instant) -> BackgroundShutdownDeadlines {
    let cooperative = graceful.min(hard);
    let reconcile = reconciliation::users_deadline(cooperative, hard);
    let tail = reconcile.saturating_duration_since(cooperative);
    BackgroundShutdownDeadlines {
        cooperative,
        execution_stop: cooperative + tail / 3,
        cleanup: cooperative + tail * 2 / 3,
        reconcile,
    }
}

pub(super) struct BackgroundHost {
    pub(super) runtime: Arc<BackgroundServiceRuntime>,
    pub(super) deadlines: OnceLock<BackgroundShutdownDeadlines>,
    pub(super) monitors: TaskRegistry,
    force_bridge: OnceLock<TaskReceipt<()>>,
    executor: tokio::runtime::Handle,
    // A terminal report can precede a real join (e.g. a blocking destructor).
    // Keep the actual application resources alive until quiescence is proven.
    retained: StdMutex<Option<WsApp>>,
}

impl BackgroundHost {
    pub(super) fn new(runtime: Arc<BackgroundServiceRuntime>) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            deadlines: OnceLock::new(),
            monitors: TaskRegistry::default(),
            force_bridge: OnceLock::new(),
            executor: tokio::runtime::Handle::current(),
            retained: StdMutex::new(None),
        })
    }

    fn retain(&self, app: &WsApp) {
        self.retained
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_or_insert_with(|| app.runtime_clone());
    }

    pub(super) fn release(&self) {
        // Drop the retained application outside the lock.
        let retained = self
            .retained
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        drop(retained);
    }

    pub(super) fn is_terminal(&self) -> bool {
        self.runtime.snapshot().is_terminal() && self.monitors.snapshot().outstanding == 0
    }
}

impl WsApp {
    pub(super) fn background_snapshot(&self) -> BackgroundServiceSnapshot {
        self.lifecycle
            .background
            .as_ref()
            .map_or_else(BackgroundServiceSnapshot::empty, |background| {
                background.runtime.snapshot()
            })
    }

    pub(super) fn begin_background_shutdown(&self, graceful: Instant, hard: Instant) {
        let Some(background) = &self.lifecycle.background else {
            return;
        };
        background.retain(self);
        let limits = *background
            .deadlines
            .get_or_init(|| deadlines(graceful, hard));
        background.runtime.begin_shutdown(limits);
        if self.lifecycle.shutdown_state.is_force_requested() {
            background.runtime.force_stop();
        }
        background.force_bridge.get_or_init(|| {
            let runtime = background.runtime.clone();
            let shutdown = self.lifecycle.shutdown_state.clone();
            background.monitors.track(lily_trace::spawn(async move {
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for_force() => runtime.force_stop(),
                    _ = runtime.wait_stopped_before(limits.reconcile) => {},
                }
            }))
        });
    }

    pub(super) async fn wait_for_background_failure(&self) {
        match &self.lifecycle.background {
            Some(background) => background.runtime.wait_for_failure().await,
            None => std::future::pending().await,
        }
    }

    pub(super) async fn reconcile_background(&self) -> Result<(), ShutdownError> {
        let Some(background) = &self.lifecycle.background else {
            return Ok(());
        };
        let limits = background
            .deadlines
            .get()
            .expect("root deadlines published");
        let stopped = background
            .runtime
            .wait_stopped_before(limits.reconcile)
            .await;
        tokio::select! {
            biased;
            () = background.monitors.wait() => {},
            () = tokio::time::sleep_until(limits.reconcile) => {},
        }
        if !stopped || !background.is_terminal() {
            return Err(ShutdownError::Component(
                "WebSocket background tasks/scopes outstanding; dependencies retained".into(),
            ));
        }
        if !background.runtime.snapshot().cleanup_succeeded()
            || background.monitors.snapshot().panicked != 0
        {
            return Err(ShutdownError::Component(
                "WebSocket background scope cleanup or task reconciliation failed".into(),
            ));
        }
        Ok(())
    }
}

impl Drop for WsApp {
    fn drop(&mut self) {
        // Only the last application handle may close an abandoned, prepared
        // runtime. Internal runtime clones and an active lifecycle root cannot
        // request an unrelated shutdown when they are dropped.
        let Some(background) = &self.lifecycle.background else {
            return;
        };
        if Arc::strong_count(&self.lifecycle) != 1
            || self.lifecycle.root_task.get().is_some()
            || self
                .lifecycle
                .phase
                .compare_exchange(
                    WS_APP_BUILT,
                    WS_APP_CLOSING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return;
        }
        background.retain(self);
        self.close_dependency_registration();
        let runtime = self.runtime_clone();
        let (registered, ready) = tokio::sync::oneshot::channel();
        let task = background.executor.spawn(async move {
            let _ = ready.await;
            let result = match AssertUnwindSafe(WsApp::close_never_started(runtime.runtime_clone()))
                .catch_unwind()
                .await
            {
                Ok(result) => result,
                Err(_) => Err(format!(
                    "WebSocket abandoned application cleanup panicked; {}",
                    runtime.recover_root_failure().await,
                )),
            };
            runtime.finish_shutdown(result).await;
        });
        assert!(
            self.lifecycle
                .root_task
                .set(TaskReceipt::from(task))
                .is_ok()
        );
        let _ = registered.send(());
    }
}

/// Build rollback has no `WsApp` yet. Retain the same dependency obligations
/// when an actual constructor/scope join cannot be observed by the deadline.
pub(super) struct BuildResources {
    background: Arc<BackgroundServiceRuntime>,
    container: Option<Arc<ApplicationContainer>>,
    dispatcher: Option<Arc<WebSocketDispatcher>>,
    tracing: tokio::sync::Mutex<Option<lily_trace::lifecycle::TracingShutdownHandle>>,
    tracing_owner: tokio::sync::Mutex<Option<TracingRuntimeOwner>>,
    retained: StdMutex<Option<Arc<Self>>>,
}

impl BuildResources {
    pub(super) fn new(
        background: Arc<BackgroundServiceRuntime>,
        container: Option<Arc<ApplicationContainer>>,
        dispatcher: Option<Arc<WebSocketDispatcher>>,
        tracing_owner: Option<TracingRuntimeOwner>,
    ) -> Arc<Self> {
        let resources = Arc::new(Self {
            background,
            container,
            dispatcher,
            tracing: tokio::sync::Mutex::new(None),
            tracing_owner: tokio::sync::Mutex::new(tracing_owner),
            retained: StdMutex::new(None),
        });
        *resources.retained.lock().unwrap_or_else(|p| p.into_inner()) = Some(resources.clone());
        resources
    }

    pub(super) async fn close(self: Arc<Self>, started: Instant, total: Duration) -> Vec<String> {
        let hard = started.checked_add(total).unwrap_or(started);
        let span = hard.saturating_duration_since(started);
        let at = |percent| started + (span / 100) * percent;
        let mut failures = Vec::new();
        self.background.begin_shutdown(BackgroundShutdownDeadlines {
            // A constructor has no stopping token; prepared execution has
            // never started. Abort these tasks promptly on failed build.
            cooperative: started,
            execution_stop: at(25),
            cleanup: at(55),
            reconcile: at(60),
        });
        if !self.background.wait_stopped_before(at(60)).await {
            return vec![
                "background construction/scopes unconfirmed; build dependencies retained".into(),
            ];
        }
        if !self.background.snapshot().cleanup_succeeded() {
            failures.push("background scope cleanup failed".into());
        }
        if let Some(dispatcher) = &self.dispatcher {
            dispatcher.set_shutdown_deadlines(started, at(80));
            match timeout_at(
                at(75),
                AssertUnwindSafe(dispatcher.close_backplane()).catch_unwind(),
            )
            .await
            {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    failures.push(format!("backplane: {}", error.kind().as_str()))
                }
                Ok(Err(_)) => failures.push("backplane close panicked".into()),
                Err(_) => failures.push("backplane close deadline elapsed".into()),
            }
            if !dispatcher.close_terminal() {
                let _ = dispatcher.abort_and_join_backplane_close().await;
            }
            if !dispatcher.close_terminal() || !dispatcher.dependency_users_terminal() {
                failures
                    .push("backplane termination unconfirmed; build dependencies retained".into());
                return failures;
            }
        }
        if let Some(container) = &self.container {
            if Instant::now() >= at(90) {
                failures.push(
                    "DI close not started: build deadline elapsed; dependencies retained".into(),
                );
                return failures;
            }
            match timeout_at(
                at(93),
                AssertUnwindSafe(container.close_before(at(90))).catch_unwind(),
            )
            .await
            {
                Ok(Ok(Ok(_))) => {}
                Ok(Ok(Err(error))) => failures.push(format!("DI container: {error}")),
                Ok(Err(_)) => failures.push("DI container close panicked".into()),
                Err(_) => failures.push("DI container close deadline elapsed".into()),
            }
            if !lily_injection::__private::container_shutdown_quiescent(container) {
                failures.push("DI termination unconfirmed; build telemetry retained".into());
                return failures;
            }
        }
        if let Some(owner) = self.tracing_owner.lock().await.take() {
            let (handle, evidence) =
                lily_trace::lifecycle::TracingShutdownHandle::before(owner, total, at(98));
            let mut tracing = self.tracing.lock().await;
            *tracing = Some(handle);
            match timeout_at(
                hard,
                AssertUnwindSafe(tracing.as_mut().unwrap().shutdown()).catch_unwind(),
            )
            .await
            {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => failures.push(format!("tracing runtime: {error}")),
                Ok(Err(_)) => failures.push("tracing shutdown panicked".into()),
                Err(_) => failures.push("tracing shutdown deadline elapsed".into()),
            }
            if !evidence.reconcile_before(hard).await {
                failures.push("tracing joins outstanding; build owners retained".into());
                return failures;
            }
        }
        self.retained
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        failures
    }
}

#[cfg(test)]
#[path = "background_tests.rs"]
mod tests;
