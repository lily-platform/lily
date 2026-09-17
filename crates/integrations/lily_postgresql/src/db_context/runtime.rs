use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use super::control::{Control, Kind, lock};
use crate::database::lease::is_cancelled;
use crate::{ExecutionCancellation, PgError, PgResult};

#[derive(Default)]
struct State {
    closed: bool,
    active: Option<Arc<Control>>,
    cleanup_error: Option<PgError>,
    close_deadline: Option<Instant>,
}

#[derive(Default)]
pub(super) struct ContextRuntime {
    state: Mutex<State>,
    changed: Notify,
}

pub(super) enum Admission {
    Pooled(Completion),
    Transaction(Arc<Control>),
}

impl ContextRuntime {
    pub(super) fn operation(
        self: &Arc<Self>,
        cancellation: Option<ExecutionCancellation>,
        timeout: impl FnOnce() -> PgResult<Duration>,
    ) -> PgResult<Admission> {
        let mut state = lock(&self.state);
        if state.closed {
            return Err(PgError::ContextClosed);
        }
        if let Some(active) = &state.active {
            return match active.kind {
                // Incoming cancellation is deliberately ignored, even when the
                // transaction's own view is None and the incoming view is cancelled.
                Kind::Transaction => Ok(Admission::Transaction(Arc::clone(active))),
                Kind::Operation => Err(PgError::ContextBusy),
            };
        }
        if is_cancelled(&cancellation) {
            return Err(PgError::OperationCancelled);
        }
        let control = Arc::new(Control::new(Kind::Operation, cancellation, timeout()?));
        state.active = Some(Arc::clone(&control));
        Ok(Admission::Pooled(Completion {
            control,
            runtime: Arc::clone(self),
            finished: false,
        }))
    }

    pub(super) fn transaction(
        self: &Arc<Self>,
        cancellation: Option<ExecutionCancellation>,
        timeout: impl FnOnce() -> PgResult<Duration>,
    ) -> PgResult<Completion> {
        let mut state = lock(&self.state);
        if state.closed {
            return Err(PgError::ContextClosed);
        }
        if let Some(active) = &state.active {
            return Err(match active.kind {
                Kind::Operation => PgError::ContextBusy,
                Kind::Transaction => PgError::ContextTransactionActive,
            });
        }
        if is_cancelled(&cancellation) {
            return Err(PgError::TransactionCancelled);
        }
        let control = Arc::new(Control::new(Kind::Transaction, cancellation, timeout()?));
        state.active = Some(Arc::clone(&control));
        Ok(Completion {
            control,
            runtime: Arc::clone(self),
            finished: false,
        })
    }

    fn finish(&self, control: &Arc<Control>, cleanup: PgResult<()>) {
        let mut state = lock(&self.state);
        if state
            .active
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, control))
        {
            if let Err(error) = cleanup {
                state.closed = true;
                state.cleanup_error.get_or_insert(error);
            }
            control.finish();
            state.active = None;
        }
        self.changed.notify_waiters();
    }

    pub(super) async fn dispose(&self) -> PgResult<()> {
        let deadline = {
            let mut state = lock(&self.state);
            state.closed = true;
            let Some(active) = &state.active else {
                return state.cleanup_error.clone().map_or(Ok(()), Err);
            };
            active.stop(PgError::ContextClosed);
            let deadline = active.deadline();
            *state.close_deadline.get_or_insert(deadline)
        };
        let wait = async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let state = lock(&self.state);
                    if let Some(error) = &state.cleanup_error {
                        return Err(error.clone());
                    }
                    if state.active.is_none() {
                        return Ok(());
                    }
                }
                notified.await;
            }
        };
        match tokio::time::timeout_at(deadline, wait).await {
            Ok(result) => result,
            Err(_) => {
                let mut state = lock(&self.state);
                let error = state
                    .cleanup_error
                    .get_or_insert(PgError::ContextCleanupTimeout)
                    .clone();
                self.changed.notify_waiters();
                Err(error)
            }
        }
    }
}

pub(super) struct Completion {
    pub(super) control: Arc<Control>,
    runtime: Arc<ContextRuntime>,
    finished: bool,
}

impl Completion {
    pub(super) fn finish(mut self, cleanup: PgResult<()>) {
        self.runtime.finish(&self.control, cleanup);
        self.finished = true;
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if !self.finished {
            self.control.stop(PgError::TransactionOperationDetached);
            let released = if let Ok(mut connection) = self.control.connection.try_lock() {
                if let Some(connection) = connection.take() {
                    connection.discard();
                }
                true
            } else {
                false
            };
            // Ordinary work needs no transaction finalization. An owner task
            // disappearing without its finalizer leaves the context closed.
            let cleanup = if self.control.kind == Kind::Operation && released {
                Ok(())
            } else {
                Err(PgError::TransactionFinalization)
            };
            self.runtime.finish(&self.control, cleanup);
        }
    }
}
