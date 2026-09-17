use crate::{ServerError, WebSocketDispatcher};
use futures_util::FutureExt;
use lily_injection::{
    __private::{
        ApplicationContainerBuild, ApplicationContainerBuildOutcome,
        begin_application_container_build,
    },
    ApplicationContainer, ApplicationContainerBuilder, ProcessContext,
};
use lily_trace::TracingRuntimeOwner;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tracing::Instrument;

/// Private ownership boundary for resources created while `WsApp` is built.
///
/// The caller continues to poll all application composition work, preserving
/// task-local process and tracing semantics. If that caller is cancelled, the
/// transaction moves every Lily-owned resource into one detached rollback
/// task. Caller-owned DI containers are deliberately never adopted here.
pub(super) struct WsAppBuildTransaction {
    background: Option<Arc<lily_background_service::BackgroundServiceRuntime>>,
    runtime: Handle,
    di_build: Option<ApplicationContainerBuild>,
    owned_container: Option<Arc<ApplicationContainer>>,
    tracing_owner: Option<TracingRuntimeOwner>,
    dispatcher: Option<Arc<WebSocketDispatcher>>,
    active_activity: Option<Arc<BuildActivityState>>,
    shutdown_timeout: Duration,
    process_context: Option<ProcessContext>,
    rollback_span: tracing::Span,
}

impl WsAppBuildTransaction {
    pub(super) fn new(
        runtime: Handle,
        tracing_owner: Option<TracingRuntimeOwner>,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            background: None,
            runtime,
            di_build: None,
            owned_container: None,
            tracing_owner,
            dispatcher: None,
            active_activity: None,
            shutdown_timeout,
            process_context: ProcessContext::current(),
            rollback_span: tracing::Span::current(),
        }
    }

    /// Build and immediately adopt a framework-owned DI container.
    ///
    /// Keeping the hidden DI transaction in this owner lets `Drop` cancel the
    /// exact in-progress initializer and then wait for its rollback before
    /// shutting down tracing.
    pub(super) async fn build_owned_container(
        &mut self,
        builder: ApplicationContainerBuilder,
    ) -> Result<Arc<ApplicationContainer>, ServerError> {
        debug_assert!(self.di_build.is_none());
        debug_assert!(self.owned_container.is_none());
        self.di_build = Some(begin_application_container_build(builder));

        let outcome = self
            .di_build
            .as_mut()
            .expect("WebSocket DI build transaction must be present")
            .wait()
            .await;

        match outcome {
            ApplicationContainerBuildOutcome::Built(container) => {
                let container = Arc::new(container);
                self.owned_container = Some(Arc::clone(&container));
                self.di_build.take();
                Ok(container)
            }
            ApplicationContainerBuildOutcome::Failed(error) => {
                self.di_build.take();
                Err(ServerError::connection_error(format!(
                    "DI initialization failed: {error}"
                )))
            }
            ApplicationContainerBuildOutcome::Cancelled(result) => {
                self.di_build.take();
                let detail = match result {
                    Ok(()) => "DI initialization was cancelled internally".to_owned(),
                    Err(error) => format!("DI initialization cancellation failed: {error}"),
                };
                Err(ServerError::connection_error(detail))
            }
        }
    }

    pub(super) fn set_shutdown_timeout(&mut self, shutdown_timeout: Duration) {
        self.shutdown_timeout = shutdown_timeout;
    }

    /// Register one caller-polled composition future with the rollback owner.
    ///
    /// Its inner future is synchronously dropped before completion is
    /// published. A cleanup task spawned from this transaction's `Drop` waits
    /// for that publication, so a multi-thread runtime cannot begin DI
    /// disposal before a pending constructor has observed cancellation.
    pub(super) fn track<F>(&mut self, future: F) -> TrackedBuildActivity<F>
    where
        F: Future,
    {
        debug_assert!(
            self.active_activity
                .as_ref()
                .is_none_or(|activity| activity.is_complete()),
            "WebSocket build activities must not overlap"
        );
        let state = Arc::new(BuildActivityState::default());
        self.active_activity = Some(Arc::clone(&state));
        TrackedBuildActivity {
            future: Some(Box::pin(future)),
            state,
        }
    }

    /// Retain an active backplane until the finished `WsApp` assumes it.
    pub(super) fn retain_dispatcher(&mut self, dispatcher: Arc<WebSocketDispatcher>) {
        debug_assert!(self.dispatcher.is_none());
        self.dispatcher = Some(dispatcher);
    }

    pub(super) fn retain_background(
        &mut self,
        background: Arc<lily_background_service::BackgroundServiceRuntime>,
    ) {
        assert!(self.background.replace(background).is_none());
    }

    /// Commit every build-time owner and transfer tracing to `WsApp`.
    pub(super) fn commit(&mut self) -> Option<TracingRuntimeOwner> {
        debug_assert!(self.di_build.is_none());
        debug_assert!(
            self.active_activity
                .as_ref()
                .is_none_or(|activity| activity.is_complete())
        );
        self.active_activity.take();
        self.background.take();
        self.dispatcher.take();
        self.owned_container.take();
        self.tracing_owner.take()
    }

    /// Start the cancellation-safe rollback task and await its report.
    ///
    /// Dropping this wait only detaches the `JoinHandle`; the spawned task
    /// already owns all cleanup resources and continues independently.
    pub(super) async fn rollback(&mut self, original: ServerError) -> ServerError {
        let cleanup_failures = match self.start_cleanup() {
            Some(task) => match task.await {
                Ok(failures) => failures,
                Err(error) => vec![format!("WebSocket build rollback task failed: {error}")],
            },
            None => Vec::new(),
        };

        if cleanup_failures.is_empty() {
            original
        } else {
            ServerError::connection_error(format!(
                "{original}; startup rollback incomplete: {}",
                cleanup_failures.join("; ")
            ))
        }
    }

    fn start_cleanup(&mut self) -> Option<JoinHandle<Vec<String>>> {
        let mut di_build = self.di_build.take();
        if let Some(build) = di_build.as_mut() {
            // This synchronously closes provider admission and drops the
            // active singleton initializer before ownership is detached.
            build.cancel();
        }

        let owned_container = self.owned_container.take();
        let tracing_owner = self.tracing_owner.take();
        let dispatcher = self.dispatcher.take();
        let active_activity = self.active_activity.take();
        if let Some(background) = self.background.take() {
            // Workers are constructed only after DI and backplane build have
            // completed. Their separate supervisor must stop before either
            // dependency can be disposed, even when this waiter is dropped.
            assert!(di_build.is_none());
            let resources = super::background::BuildResources::new(
                background,
                owned_container,
                dispatcher,
                tracing_owner,
            );
            let timeout = self.shutdown_timeout;
            let started = tokio::time::Instant::now();
            let context = self.process_context.take();
            let span = self.rollback_span.clone();
            let cleanup = async move {
                if let Some(activity) = active_activity {
                    activity.wait().await;
                }
                resources.close(started, timeout).await
            };
            return Some(
                self.runtime.spawn(
                    async move {
                        match context {
                            Some(context) => ProcessContext::scope(context, cleanup).await,
                            None => cleanup.await,
                        }
                    }
                    .instrument(span),
                ),
            );
        }
        if di_build.is_none()
            && owned_container.is_none()
            && tracing_owner.is_none()
            && dispatcher.is_none()
        {
            return None;
        }

        let shutdown_timeout = self.shutdown_timeout;
        let process_context = self.process_context.take();
        let rollback_span = self.rollback_span.clone();
        let cleanup = async move {
            if let Some(activity) = active_activity {
                activity.wait().await;
            }

            let mut failures = Vec::new();
            if let Some(dispatcher) = dispatcher {
                match timeout(
                    shutdown_timeout,
                    AssertUnwindSafe(dispatcher.close_backplane()).catch_unwind(),
                )
                .await
                {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(error))) => {
                        failures.push(format!("backplane: {}", error.kind().as_str()));
                    }
                    Ok(Err(_)) => failures.push("backplane: close panicked".to_owned()),
                    Err(_) => {
                        failures.push("backplane: close timed out".to_owned());
                        match dispatcher.abort_and_join_backplane_close().await {
                            Ok(()) => failures.push(
                                "backplane: owner completed during hard-timeout reconciliation"
                                    .to_owned(),
                            ),
                            Err(error) => failures.push(format!(
                                "backplane: owner terminalized as {} during hard-timeout reconciliation",
                                error.kind().as_str()
                            )),
                        }
                    }
                }
            }

            if let Some(mut build) = di_build {
                match AssertUnwindSafe(build.wait()).catch_unwind().await {
                    Ok(ApplicationContainerBuildOutcome::Cancelled(Ok(()))) => {}
                    Ok(ApplicationContainerBuildOutcome::Cancelled(Err(error))) => {
                        failures.push(format!("DI container: {error}"));
                    }
                    Ok(ApplicationContainerBuildOutcome::Failed(error)) => {
                        failures.push(format!("DI initialization: {error}"));
                    }
                    Ok(ApplicationContainerBuildOutcome::Built(container)) => {
                        close_container(&container, shutdown_timeout, &mut failures).await;
                    }
                    Err(_) => failures.push("DI container: build rollback panicked".to_owned()),
                }
            } else if let Some(container) = owned_container {
                close_container(&container, shutdown_timeout, &mut failures).await;
            }

            if let Some(owner) = tracing_owner {
                match AssertUnwindSafe(owner.shutdown(shutdown_timeout))
                    .catch_unwind()
                    .await
                {
                    Ok(report) if report.is_success() => {}
                    Ok(report) => failures.push(format!("tracing runtime: {report:?}")),
                    Err(_) => failures.push("tracing runtime: shutdown panicked".to_owned()),
                }
            }

            if !failures.is_empty() {
                tracing::warn!(
                    errors = ?failures,
                    "WebSocket build rollback completed with errors"
                );
            }
            failures
        };
        let cleanup = async move {
            if let Some(context) = process_context {
                ProcessContext::scope(context, cleanup).await
            } else {
                cleanup.await
            }
        }
        .instrument(rollback_span);
        Some(self.runtime.spawn(cleanup))
    }
}

impl Drop for WsAppBuildTransaction {
    fn drop(&mut self) {
        // Dropping the JoinHandle detaches the already-owned rollback task.
        let _ = self.start_cleanup();
    }
}

async fn close_container(
    container: &ApplicationContainer,
    shutdown_timeout: Duration,
    failures: &mut Vec<String>,
) {
    match AssertUnwindSafe(container.close_with_timeout(shutdown_timeout))
        .catch_unwind()
        .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => failures.push(format!("DI container: {error}")),
        Err(_) => failures.push("DI container: shutdown panicked".to_owned()),
    }
}

#[derive(Default)]
struct BuildActivityState {
    complete: AtomicBool,
    notify: Notify,
}

impl BuildActivityState {
    fn complete(&self) {
        self.complete.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register this waiter before reading `complete`. `notify_waiters`
            // does not retain a permit, so checking first would leave a gap in
            // which completion could be published and then lost forever.
            notified.as_mut().enable();
            if self.is_complete() {
                return;
            }
            notified.as_mut().await;
        }
    }
}

pub(super) struct TrackedBuildActivity<F>
where
    F: Future,
{
    future: Option<Pin<Box<F>>>,
    state: Arc<BuildActivityState>,
}

impl<F> Future for TrackedBuildActivity<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this
            .future
            .as_mut()
            .expect("completed WebSocket build activity was polled again")
            .as_mut()
            .poll(context);
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => {
                // Drop the constructor/composition future before allowing
                // rollback to observe activity completion. The unwind guard
                // also publishes completion if user-owned Drop code panics.
                let completion = BuildActivityCompletion(Arc::clone(&this.state));
                this.future.take();
                drop(completion);
                Poll::Ready(output)
            }
        }
    }
}

impl<F> Drop for TrackedBuildActivity<F>
where
    F: Future,
{
    fn drop(&mut self) {
        let completion = BuildActivityCompletion(Arc::clone(&self.state));
        self.future.take();
        drop(completion);
    }
}

struct BuildActivityCompletion(Arc<BuildActivityState>);

impl Drop for BuildActivityCompletion {
    fn drop(&mut self) {
        self.0.complete();
    }
}

#[cfg(test)]
mod tests {
    use super::BuildActivityState;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn build_activity_wait_replays_and_observes_completion() {
        let completed = Arc::new(BuildActivityState::default());
        completed.complete();
        timeout(Duration::from_secs(1), completed.wait())
            .await
            .expect("completion published before wait must be replayed");

        let pending = Arc::new(BuildActivityState::default());
        let waiter_state = Arc::clone(&pending);
        let waiter = tokio::spawn(async move { waiter_state.wait().await });
        tokio::task::yield_now().await;
        pending.complete();
        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("subscribed waiter must observe completion")
            .expect("waiter task must not panic");
    }
}
