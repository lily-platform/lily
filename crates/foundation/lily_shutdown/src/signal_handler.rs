use super::{ShutdownInitiation, ShutdownSignal, ShutdownState};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Platform signal handler. The first signal starts graceful shutdown and the
/// second signal requests the bounded force path; the library never calls
/// `process::exit` from a background task.
pub struct SignalHandler {
    shutdown_state: Arc<ShutdownState>,
}

impl SignalHandler {
    /// Creates a handler that publishes into `shutdown_state`.
    pub fn new(shutdown_state: Arc<ShutdownState>) -> Self {
        Self { shutdown_state }
    }

    /// Listens on the current task until a force-class signal ends the loop or
    /// signal reception fails.
    pub async fn listen_for_signals(&self) -> Result<(), std::io::Error> {
        listen(Arc::clone(&self.shutdown_state)).await
    }

    /// Starts an owned signal task. Dropping the monitor aborts the task;
    /// callers can use [`SignalMonitor::stop`] to await that cancellation.
    pub async fn install(
        shutdown_state: Arc<ShutdownState>,
    ) -> Result<SignalMonitor, std::io::Error> {
        // Register streams and wait until the listener task has reached its
        // receive loop. This closes the immediate-signal startup window.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let task = spawn_listener(Arc::clone(&shutdown_state), ready_tx)?;
        ready_rx.await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "signal listener stopped before becoming ready",
            )
        })?;
        Ok(SignalMonitor {
            shutdown_state,
            task: Some(task),
        })
    }

    /// Compatibility helper for callers interested only in the first signal.
    pub async fn install_and_wait(
        shutdown_state: Arc<ShutdownState>,
    ) -> Result<ShutdownSignal, std::io::Error> {
        let mut monitor = Self::install(shutdown_state).await?;
        let signal = monitor.wait_for_first().await;
        let stop = monitor.stop().await;
        match (signal, stop) {
            (Ok(signal), Ok(())) => Ok(signal),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(signal_error), Err(stop_error)) => Err(std::io::Error::new(
                signal_error.kind(),
                format!("{signal_error}; signal monitor cleanup failed: {stop_error}"),
            )),
        }
    }
}

/// Owned background signal listener installed by [`SignalHandler::install`].
pub struct SignalMonitor {
    shutdown_state: Arc<ShutdownState>,
    task: Option<JoinHandle<Result<(), std::io::Error>>>,
}

impl SignalMonitor {
    /// Waits for the durable first signal while also observing listener
    /// failures.
    pub async fn wait_for_first(&mut self) -> Result<ShutdownSignal, std::io::Error> {
        let mut receiver = self.shutdown_state.subscribe();
        let task = self
            .task
            .as_mut()
            .expect("signal monitor task is present until stop");

        tokio::select! {
            signal = receiver.recv() => signal.map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, error)
            }),
            result = task => match result {
                // A force-class signal publishes the durable state and then
                // intentionally ends the listener. If both become ready in
                // the same scheduler turn, prefer the already-published
                // signal over a false monitor-stopped error.
                Ok(Ok(())) => self.shutdown_state.initial_signal().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "signal monitor stopped before publishing a shutdown signal",
                    )
                }),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(std::io::Error::other(format!(
                    "signal monitor task failed: {error}"
                ))),
            },
        }
    }

    /// Transfer the actual signal-listener join to a composition-root registry.
    /// The abort request alone does not prove that the listener has terminated.
    #[doc(hidden)]
    pub fn into_stop_task(self) -> Option<JoinHandle<Result<(), std::io::Error>>> {
        let task = self.into_task()?;
        task.abort();
        Some(task)
    }

    /// Transfer the live monitor to a composition-root receipt registry.
    /// The registry becomes responsible for stopping and observing this task;
    /// transfer itself neither requests cancellation nor confirms termination.
    #[doc(hidden)]
    pub fn into_task(mut self) -> Option<JoinHandle<Result<(), std::io::Error>>> {
        self.task.take()
    }

    /// Aborts and joins the owned signal listener.
    pub async fn stop(mut self) -> Result<(), std::io::Error> {
        if let Some(task) = self.task.take() {
            task.abort();
            match task.await {
                Ok(result) => result,
                Err(error) if error.is_cancelled() => Ok(()),
                Err(error) => Err(std::io::Error::other(format!(
                    "signal monitor task join failed: {error}"
                ))),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for SignalMonitor {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn dispatch_signal(state: &ShutdownState, signal: ShutdownSignal) -> bool {
    match state.initiate_shutdown(signal) {
        Ok(ShutdownInitiation::Initiated { .. }) => signal.is_force(),
        Ok(ShutdownInitiation::AlreadyInitiated { .. }) => {
            state.request_force();
            true
        }
        Err(_) => {
            // The current state transition is infallible, but keeping the
            // branch makes future persistence errors fail towards force.
            state.request_force();
            true
        }
    }
}

#[cfg(unix)]
fn spawn_listener(
    state: Arc<ShutdownState>,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<JoinHandle<Result<(), std::io::Error>>, std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};

    let interrupt = signal(SignalKind::interrupt())?;
    let terminate = signal(SignalKind::terminate())?;
    let quit = signal(SignalKind::quit())?;
    Ok(tokio::spawn(async move {
        ready.send(()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "signal monitor owner dropped before readiness",
            )
        })?;
        listen_unix(state, interrupt, terminate, quit).await
    }))
}

#[cfg(unix)]
async fn listen(state: Arc<ShutdownState>) -> Result<(), std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};

    listen_unix(
        state,
        signal(SignalKind::interrupt())?,
        signal(SignalKind::terminate())?,
        signal(SignalKind::quit())?,
    )
    .await
}

#[cfg(unix)]
async fn listen_unix(
    state: Arc<ShutdownState>,
    mut interrupt: tokio::signal::unix::Signal,
    mut terminate: tokio::signal::unix::Signal,
    mut quit: tokio::signal::unix::Signal,
) -> Result<(), std::io::Error> {
    loop {
        let signal = tokio::select! {
            value = interrupt.recv() => value.map(|_| ShutdownSignal::Interrupt),
            value = terminate.recv() => value.map(|_| ShutdownSignal::Terminate),
            value = quit.recv() => value.map(|_| ShutdownSignal::Quit),
        }
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "all Unix shutdown signal streams closed",
            )
        })?;

        if dispatch_signal(&state, signal) {
            return Ok(());
        }
    }
}

#[cfg(windows)]
fn spawn_listener(
    state: Arc<ShutdownState>,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<JoinHandle<Result<(), std::io::Error>>, std::io::Error> {
    use tokio::signal::windows;

    let ctrl_c = windows::ctrl_c()?;
    let ctrl_break = windows::ctrl_break()?;
    let ctrl_close = windows::ctrl_close()?;
    let ctrl_shutdown = windows::ctrl_shutdown()?;
    Ok(tokio::spawn(async move {
        ready.send(()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "signal monitor owner dropped before readiness",
            )
        })?;
        listen_windows(state, ctrl_c, ctrl_break, ctrl_close, ctrl_shutdown).await
    }))
}

#[cfg(windows)]
async fn listen(state: Arc<ShutdownState>) -> Result<(), std::io::Error> {
    use tokio::signal::windows;

    listen_windows(
        state,
        windows::ctrl_c()?,
        windows::ctrl_break()?,
        windows::ctrl_close()?,
        windows::ctrl_shutdown()?,
    )
    .await
}

#[cfg(windows)]
async fn listen_windows(
    state: Arc<ShutdownState>,
    mut ctrl_c: tokio::signal::windows::CtrlC,
    mut ctrl_break: tokio::signal::windows::CtrlBreak,
    mut ctrl_close: tokio::signal::windows::CtrlClose,
    mut ctrl_shutdown: tokio::signal::windows::CtrlShutdown,
) -> Result<(), std::io::Error> {
    loop {
        let received = tokio::select! {
            value = ctrl_c.recv() => value,
            value = ctrl_break.recv() => value,
            value = ctrl_close.recv() => value,
            value = ctrl_shutdown.recv() => value,
        };
        if received.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Windows shutdown signal stream closed",
            ));
        }
        if dispatch_signal(&state, ShutdownSignal::Interrupt) {
            return Ok(());
        }
    }
}

#[cfg(not(any(unix, windows)))]
fn spawn_listener(
    state: Arc<ShutdownState>,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<JoinHandle<Result<(), std::io::Error>>, std::io::Error> {
    Ok(tokio::spawn(async move {
        ready.send(()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "signal monitor owner dropped before readiness",
            )
        })?;
        listen(state).await
    }))
}

#[cfg(not(any(unix, windows)))]
async fn listen(state: Arc<ShutdownState>) -> Result<(), std::io::Error> {
    loop {
        tokio::signal::ctrl_c().await?;
        if dispatch_signal(&state, ShutdownSignal::Interrupt) {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_signal_monitor_transfer_does_not_abort_its_task() {
        let (release, released) = tokio::sync::oneshot::channel();
        let monitor = SignalMonitor {
            shutdown_state: Arc::new(ShutdownState::new()),
            task: Some(tokio::spawn(async move {
                released.await.unwrap();
                Ok(())
            })),
        };
        let task = monitor.into_task().unwrap();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        release.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn root_can_retain_the_signal_join_separately_from_the_stop_request() {
        let monitor = SignalMonitor {
            shutdown_state: Arc::new(ShutdownState::new()),
            task: Some(tokio::spawn(std::future::pending::<
                Result<(), std::io::Error>,
            >())),
        };
        let join = monitor.into_stop_task().unwrap();
        assert!(
            !join.is_finished(),
            "abort is still pending on this runtime thread"
        );
        assert!(join.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn dispatches_first_signal_and_escalates_the_second() {
        let state = ShutdownState::new();
        assert!(!dispatch_signal(&state, ShutdownSignal::Interrupt));
        assert!(state.is_shutdown_initiated());
        assert!(dispatch_signal(&state, ShutdownSignal::Terminate));
        assert!(state.is_force_requested());
    }

    #[tokio::test]
    async fn completed_force_listener_replays_its_durable_signal() {
        let state = Arc::new(ShutdownState::new());
        state
            .initiate_shutdown(ShutdownSignal::Quit)
            .expect("force signal transition is infallible");
        let task = tokio::spawn(async { Ok(()) });
        let mut monitor = SignalMonitor {
            shutdown_state: Arc::clone(&state),
            task: Some(task),
        };

        assert_eq!(
            monitor.wait_for_first().await.unwrap(),
            ShutdownSignal::Quit
        );
        monitor.stop().await.unwrap();
    }
}
