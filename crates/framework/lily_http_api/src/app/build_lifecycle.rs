//! Pre-App resource ownership. Startup stays on the original polling task;
//! cancelled construction hands its resources to a retained rollback owner.

use super::*;

pub(super) struct HttpBuildGuard {
    pub(super) background: Option<Arc<lily_background_service::BackgroundServiceRuntime>>,
    tracing: Option<TracingRuntimeOwner>,
    pub(super) container: Option<Arc<ApplicationContainer>>,
    pub(super) di_build: Option<lily_injection::__private::ApplicationContainerBuild>,
    pub(super) timeout: Duration,
    owns_container: bool,
    runtime: Option<tokio::runtime::Handle>,
    armed: bool,
}

impl HttpBuildGuard {
    pub(super) fn new(tracing: Option<TracingRuntimeOwner>, owns_container: bool) -> Self {
        Self {
            background: None,
            tracing,
            owns_container,
            container: None,
            di_build: None,
            timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            runtime: tokio::runtime::Handle::try_current().ok(),
            armed: true,
        }
    }

    /// Synchronous handoff to either the built App or the explicit rollback
    /// path. Neither path yields before retaining these resources itself.
    pub(super) fn handoff(&mut self) -> Option<TracingRuntimeOwner> {
        self.armed = false;
        self.tracing.take()
    }
}

impl Drop for HttpBuildGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let owns_container = self.owns_container && self.container.is_some();
        let dependencies = HttpDependencies::with_container(
            self.container.take(),
            owns_container,
            self.tracing.take(),
        );
        if let Some(background) = self.background.take() {
            dependencies.attach_background(background);
        }
        dependencies.retain();
        let mut build = self.di_build.take();
        let budget = if let Some(build) = build.as_mut() {
            // Drop the exact initializer before its reverse rollback, keeping
            // that rollback's real join and original deadline in this owner.
            build.cancel();
            ShutdownBudget::from_started(
                build.rollback_timeout(),
                build
                    .rollback_started_at()
                    .unwrap_or_else(tokio::time::Instant::now),
            )
        } else {
            ShutdownBudget::from_started(self.timeout, tokio::time::Instant::now())
        };
        let Some(runtime) = self.runtime.as_ref() else {
            tracing::error!(
                "HTTP build cancellation retained resources without an available runtime"
            );
            return;
        };
        let _runtime = runtime.enter();
        BUILD_ROLLBACK_TASKS
            .get_or_init(TaskRegistry::default)
            .spawn(async move {
                let mut failures = Vec::new();
                if let Some(mut build) = build {
                    let outcome = build.wait().await;
                    if !build.rollback_quiescent() {
                        tracing::error!(
                            ?outcome,
                            "HTTP build rollback users remain outstanding; telemetry retained"
                        );
                        return vec!["HTTP DI build rollback termination unconfirmed".to_string()];
                    }
                    match outcome {
                        lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(
                            Err(error),
                        )
                        | lily_injection::__private::ApplicationContainerBuildOutcome::Failed(
                            error,
                        ) => failures.push(error.to_string()),
                        _ => {}
                    }
                }
                failures.extend(close_build_dependencies(dependencies, budget).await);
                if !failures.is_empty() {
                    tracing::warn!(?failures, "cancelled HTTP build rollback incomplete");
                }
                failures
            });
    }
}
