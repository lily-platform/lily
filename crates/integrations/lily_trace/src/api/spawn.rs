use std::future::Future;
use tokio::task::JoinHandle;

/// Spawns a `Send` future instrumented with the currently entered span.
///
/// Use this when a child Tokio task should remain correlated with its caller.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let span = tracing::Span::current();
    tokio::spawn(tracing::instrument::Instrument::instrument(future, span))
}

/// Runs blocking work while the worker thread has the current span entered.
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _guard = span.enter();
        f()
    })
}

/// Spawns a non-`Send` future on the current [`tokio::task::LocalSet`],
/// instrumented with the currently entered span.
pub fn spawn_local<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_local(tracing::instrument::Instrument::instrument(future, span))
}
