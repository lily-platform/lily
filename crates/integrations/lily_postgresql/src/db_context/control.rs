use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use lily_cancellation::__private::ExecutionCancellationSource;
use tokio::time::Instant;

use crate::database::lease::{PgQueryCancellation, cancelled, is_cancelled};
use crate::{ExecutionCancellation, PgConnectionLease, PgError, PgResult};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Operation,
    Transaction,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Starting,
    Active,
    Finalizing,
    Finished,
}

struct State {
    phase: Phase,
    busy: bool,
    interrupted: bool,
    failure: Option<PgError>,
    stop_reason: Option<PgError>,
    cleanup_deadline: Option<Instant>,
}

pub(super) struct Control {
    pub(super) kind: Kind,
    pub(super) connection: tokio::sync::Mutex<Option<PgConnectionLease>>,
    pub(super) driver: OnceLock<PgQueryCancellation>,
    pub(super) cancellation: Option<ExecutionCancellation>,
    cleanup_timeout: Duration,
    stop: ExecutionCancellationSource,
    state: Mutex<State>,
}

impl Control {
    pub(super) fn new(
        kind: Kind,
        cancellation: Option<ExecutionCancellation>,
        cleanup_timeout: Duration,
    ) -> Self {
        Self {
            kind,
            cancellation,
            cleanup_timeout,
            connection: tokio::sync::Mutex::new(None),
            driver: OnceLock::new(),
            stop: ExecutionCancellationSource::default(),
            state: Mutex::new(State {
                phase: Phase::Starting,
                busy: false,
                interrupted: false,
                failure: None,
                stop_reason: None,
                cleanup_deadline: None,
            }),
        }
    }

    pub(super) fn activate(&self) {
        lock(&self.state).phase = Phase::Active;
    }

    pub(super) fn finish(&self) {
        lock(&self.state).phase = Phase::Finished;
    }

    pub(super) fn stop(&self, reason: PgError) {
        let mut state = lock(&self.state);
        state.stop_reason.get_or_insert(reason);
        state
            .cleanup_deadline
            .get_or_insert_with(|| Instant::now() + self.cleanup_timeout);
        self.stop.cancel();
    }

    fn external_reason(&self) -> Option<PgError> {
        is_cancelled(&self.cancellation).then_some(match self.kind {
            Kind::Operation => PgError::OperationCancelled,
            Kind::Transaction => PgError::TransactionCancelled,
        })
    }

    pub(super) fn reason(&self) -> Option<PgError> {
        self.external_reason()
            .or_else(|| lock(&self.state).stop_reason.clone())
    }

    pub(super) async fn cancelled(&self) {
        let stop = self.stop.view();
        tokio::select! {
            biased;
            _ = cancelled(&self.cancellation) => {},
            _ = stop.cancelled() => {},
        }
    }

    pub(super) fn deadline(&self) -> Instant {
        *lock(&self.state)
            .cleanup_deadline
            .get_or_insert_with(|| Instant::now() + self.cleanup_timeout)
    }

    pub(super) fn needs_query_cancel(&self) -> bool {
        let state = lock(&self.state);
        // A completed query needs no driver cancellation. On panic, use bounded
        // rollback directly; a late cancel request could instead hit ROLLBACK.
        state.stop_reason != Some(PgError::OperationPanicked) && (state.busy || state.interrupted)
    }

    pub(super) fn admit(self: &Arc<Self>) -> PgResult<QueryPermit> {
        let mut state = lock(&self.state);
        if let Some(reason) = self.external_reason().or_else(|| state.stop_reason.clone()) {
            return Err(reason);
        }
        if state.phase != Phase::Active || state.busy {
            return Err(PgError::ContextBusy);
        }
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        state.busy = true;
        Ok(QueryPermit {
            control: Arc::clone(self),
            finished: false,
        })
    }

    pub(super) fn finalize<T, E: From<PgError>>(&self, result: Result<T, E>) -> Result<T, E> {
        let mut state = lock(&self.state);
        state.phase = Phase::Finalizing;
        if state.busy {
            state
                .stop_reason
                .get_or_insert(PgError::TransactionOperationDetached);
            state
                .cleanup_deadline
                .get_or_insert_with(|| Instant::now() + self.cleanup_timeout);
            self.stop.cancel();
        }
        if let Some(reason) = self.external_reason().or_else(|| state.stop_reason.clone()) {
            return Err(reason.into());
        }
        // A mapped application error wins; a swallowed query error cannot commit.
        result.and_then(|value| {
            state
                .failure
                .clone()
                .map_or(Ok(value), |error| Err(error.into()))
        })
    }
}

pub(super) struct QueryPermit {
    control: Arc<Control>,
    finished: bool,
}

impl QueryPermit {
    pub(super) fn complete(mut self, failure: Option<PgError>, interrupted: bool) {
        let mut state = lock(&self.control.state);
        if let Some(error) = failure {
            state.failure.get_or_insert(error);
        }
        state.interrupted |= interrupted;
        state.busy = false;
        self.finished = true;
    }
}

impl Drop for QueryPermit {
    fn drop(&mut self) {
        if !self.finished {
            self.control.stop(PgError::TransactionOperationDetached);
            let mut state = lock(&self.control.state);
            state.interrupted = true;
            state.busy = false;
        }
    }
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
