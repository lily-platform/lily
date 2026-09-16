use crate::storage::{ScopeContext, ScopeResolutionDrainHandoff, ScopedInstanceKind};
use futures::FutureExt;
use lily_error::injection::{InjectionError, ShutdownOutcome, ShutdownOutcomeStatus};
use std::{
    any::Any,
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
};
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tokio_util::task::TaskTracker;

type ScopeTaskJoin = futures::future::Shared<
    futures::future::BoxFuture<'static, Result<(), Arc<tokio::task::JoinError>>>,
>;

struct ScopeJoinEntry {
    scope: Arc<RwLock<ScopeContext>>,
    join: ScopeTaskJoin,
}

/// Actual joins are retained separately from the task-owned scope reservation.
/// Reaping polls JoinHandles only and requires no background receipt driver.
#[derive(Default)]
struct ScopeTaskJoins(Mutex<HashMap<u64, ScopeJoinEntry>>);

impl std::fmt::Debug for ScopeTaskJoins {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeTaskJoins")
            .field("outstanding", &self.pending())
            .finish()
    }
}

impl ScopeTaskJoins {
    fn reap(entries: &mut HashMap<u64, ScopeJoinEntry>) {
        entries.retain(|_, entry| entry.join.clone().now_or_never().is_none());
    }

    fn insert(&self, id: u64, scope: Arc<RwLock<ScopeContext>>, join: ScopeTaskJoin) {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::reap(&mut entries);
        entries.insert(id, ScopeJoinEntry { scope, join });
    }

    fn pending(&self) -> usize {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::reap(&mut entries);
        entries.len()
    }

    async fn wait(&self, scope: Option<&Arc<RwLock<ScopeContext>>>) {
        let joins = {
            let mut entries = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Self::reap(&mut entries);
            entries
                .values()
                .filter(|entry| scope.is_none_or(|scope| Arc::ptr_eq(scope, &entry.scope)))
                .map(|entry| entry.join.clone())
                .collect::<Vec<_>>()
        };
        for join in joins {
            let _ = join.await;
        }
        self.pending();
    }
}

const CLEANUP_TERMINAL_OPEN: u8 = 0;
const CLEANUP_TERMINAL_NORMAL: u8 = 1;
const CLEANUP_TERMINAL_DEADLINE: u8 = 2;
const CLEANUP_TERMINAL_AGGREGATE_ABORT: u8 = 3;
const CLEANUP_TERMINAL_UNEXPECTED_STOP: u8 = 4;

#[derive(Clone, Debug)]
struct ScopeCleanupTaskControl {
    abort_handle: AbortHandle,
    scope_id: String,
    terminal: Arc<AtomicU8>,
    scope: Arc<RwLock<ScopeContext>>,
    /// Becomes true only after every admitted scope resolution is terminal and
    /// the complete lifecycle ledger has been detached from the closing scope.
    /// Before this point aborting the task could orphan a late publication.
    abort_safe: Arc<AtomicBool>,
}

type ScopeCleanupAbortHandles = Arc<Mutex<HashMap<u64, ScopeCleanupTaskControl>>>;
type ClosingScopes = Arc<Mutex<HashMap<String, Arc<RwLock<ScopeContext>>>>>;

impl ScopeCleanupTaskControl {
    fn is_abort_safe(&self) -> bool {
        if self.abort_safe.load(Ordering::Acquire) {
            return true;
        }
        let resolutions_drained = self
            .scope
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .in_flight_resolution_count()
            == 0;
        if resolutions_drained {
            // Closing prevents a new guard from entering, so zero is stable.
            self.abort_safe.store(true, Ordering::Release);
        }
        resolutions_drained
    }
}

/// Awaitable observation handle for cleanup owned by [`ScopeManager`].
///
/// The manager, not this ticket, owns the spawned task. Dropping the ticket
/// (for example because a request future was cancelled) therefore cannot
/// cancel resource disposal.
pub(crate) struct ScopeCleanupTicket {
    result: oneshot::Receiver<Result<(), InjectionError>>,
    abort: Option<ScopeCleanupAbort>,
}

struct ScopeCleanupAbort {
    task_id: u64,
    scope_id: String,
    abort_handles: ScopeCleanupAbortHandles,
    cleanup_failures: Arc<Mutex<Vec<InjectionError>>>,
    cleanup_outcomes: Arc<Mutex<Vec<ShutdownOutcome>>>,
    scopes_changed: Arc<Notify>,
    join: ScopeTaskJoin,
}

/// Opaque identity for one concrete scope generation.
///
/// Scope IDs may be reused after cleanup. Waiting through this observation
/// can therefore never attach to or claim a later generation with the same
/// application-provided ID.
#[derive(Clone)]
pub(crate) struct ScopeCleanupObservation {
    scope_id: String,
    scope: Arc<RwLock<ScopeContext>>,
}

impl ScopeCleanupObservation {
    pub(crate) fn new(scope_id: String, scope: Arc<RwLock<ScopeContext>>) -> Self {
        Self { scope_id, scope }
    }
}

/// RAII registration owned by the cleanup task future itself.
///
/// Tokio drops this value only after the task future has reached a terminal
/// state, including abort-before-first-poll and panic unwinding. Releasing the
/// closing-scope reservation from this guard therefore proves that no disposer
/// future for the scope can still be polled.
struct ScopeCleanupTaskRegistration {
    task_id: u64,
    scope_id: String,
    abort_handles: ScopeCleanupAbortHandles,
    closing_scopes: ClosingScopes,
    cleanup_failures: Arc<Mutex<Vec<InjectionError>>>,
    cleanup_outcomes: Arc<Mutex<Vec<ShutdownOutcome>>>,
    scopes_changed: Arc<Notify>,
    terminal: Arc<AtomicU8>,
    scope: Arc<RwLock<ScopeContext>>,
    ledger_detached: Arc<AtomicBool>,
    cleanup_tasks: TaskTracker,
    released: bool,
}

fn remove_cleanup_registration(
    task_id: u64,
    scope_id: &str,
    abort_handles: &ScopeCleanupAbortHandles,
    closing_scopes: &ClosingScopes,
) -> bool {
    // The control and ID reservation form one generation. Always remove the
    // control while the reservation is still held so a replacement generation
    // can never coexist with a stale control carrying the same scope ID.
    let mut abort_handles = abort_handles
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut closing_scopes = closing_scopes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(control) = abort_handles.get(&task_id) else {
        return false;
    };
    if control.scope_id != scope_id
        || !closing_scopes
            .get(scope_id)
            .is_some_and(|scope| Arc::ptr_eq(scope, &control.scope))
    {
        // A mismatched generation is an invariant failure. Retain both
        // authorities so shutdown fails closed instead of releasing an ID
        // still owned by another cleanup task.
        return false;
    }
    abort_handles.remove(&task_id);
    closing_scopes.remove(scope_id);
    true
}

fn forced_scope_outcomes(
    scope_id: &str,
    instances: &[super::scope_context::ScopedInstance],
    detail: &'static str,
) -> Vec<ShutdownOutcome> {
    instances
        .iter()
        .map(move |instance| {
            let kind = match instance.kind {
                ScopedInstanceKind::Scoped => "scoped",
                ScopedInstanceKind::Transient => "transient",
                ScopedInstanceKind::PartialInitialization => "partial-initialization",
            };
            ShutdownOutcome::with_detail(
                format!("scope:{scope_id}:{kind}:{}", instance.type_name),
                ShutdownOutcomeStatus::Cancelled,
                detail,
            )
        })
        .collect()
}

impl ScopeCleanupTicket {
    pub(crate) async fn wait(mut self) -> Result<(), InjectionError> {
        let result = Self::wait_result(self.result).await;
        if let Some(abort) = self.abort.take() {
            abort.wait_for_cleanup_completion().await;
        }
        result
    }

    pub(crate) async fn wait_until(
        mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), InjectionError> {
        tokio::select! {
            biased;
            result = &mut self.result => {
                let result = Self::decode_result(result);
                if let Some(abort) = self.abort.take() {
                    abort.wait_for_cleanup_completion().await;
                }
                result
            },
            _ = tokio::time::sleep_until(deadline) => {
                let Some(abort) = self.abort.take() else {
                    return Self::wait_result(self.result).await;
                };
                let deadline_error = abort.abort_for_deadline();
                // The cleanup task owns the disposer future directly. Waiting
                // for its result sender to close proves that no async disposer
                // remains active before the caller materializes an outcome.
                let result = Self::wait_result(self.result).await;
                abort.wait_for_cleanup_completion().await;
                deadline_error.map_or(result, Err)
            }
        }
    }

    async fn wait_result(
        result: oneshot::Receiver<Result<(), InjectionError>>,
    ) -> Result<(), InjectionError> {
        Self::decode_result(result.await)
    }

    fn decode_result(
        result: Result<Result<(), InjectionError>, oneshot::error::RecvError>,
    ) -> Result<(), InjectionError> {
        result.unwrap_or_else(|error| {
            Err(InjectionError::DisposeError(format!(
                "Scope cleanup task stopped before reporting its result: {error}"
            )))
        })
    }
}

impl ScopeCleanupAbort {
    fn abort_for_deadline(&self) -> Option<InjectionError> {
        let control = {
            let abort_handles = self
                .abort_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let control = abort_handles.get(&self.task_id)?;
            if control
                .terminal
                .compare_exchange(
                    CLEANUP_TERMINAL_OPEN,
                    CLEANUP_TERMINAL_DEADLINE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return None;
            }
            control.clone()
        };
        debug_assert_eq!(control.scope_id, self.scope_id);

        let error = InjectionError::ScopeCleanupTimedOut {
            scope_id: self.scope_id.clone(),
        };
        self.cleanup_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(error.clone());
        self.cleanup_outcomes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(ShutdownOutcome::with_detail(
                format!("scope:{}", self.scope_id),
                ShutdownOutcomeStatus::TimedOut,
                "scope cleanup task exceeded its owner deadline",
            ));
        if control.is_abort_safe() {
            control.abort_handle.abort();
        }
        Some(error)
    }

    async fn wait_for_cleanup_completion(&self) {
        loop {
            let changed = self.scopes_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .abort_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains_key(&self.task_id)
            {
                break;
            }
            changed.as_mut().await;
        }
        let _ = self.join.clone().await;
    }
}

impl ScopeCleanupTaskRegistration {
    fn claim_normal_completion(&self) -> bool {
        if self
            .terminal
            .compare_exchange(
                CLEANUP_TERMINAL_OPEN,
                CLEANUP_TERMINAL_NORMAL,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        true
    }

    fn finish(mut self) {
        self.release();
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;

        if self
            .terminal
            .compare_exchange(
                CLEANUP_TERMINAL_OPEN,
                CLEANUP_TERMINAL_UNEXPECTED_STOP,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            let (error, status, detail) = if std::thread::panicking() {
                (
                    InjectionError::DisposalPanicked {
                        service: "application scope".to_string(),
                        message: "scope cleanup task panicked outside its disposal boundary"
                            .to_string(),
                    },
                    ShutdownOutcomeStatus::Panicked,
                    "scope cleanup task panicked outside its disposal boundary",
                )
            } else {
                (
                    InjectionError::DisposeError(format!(
                        "Scope cleanup task for '{}' stopped without a terminal outcome",
                        self.scope_id
                    )),
                    ShutdownOutcomeStatus::Cancelled,
                    "scope cleanup task stopped without a terminal outcome",
                )
            };
            self.cleanup_failures
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(error);
            self.cleanup_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(ShutdownOutcome::with_detail(
                    format!("scope:{}", self.scope_id),
                    status,
                    detail,
                ));
        }

        if !self.ledger_detached.load(Ordering::Acquire) {
            let handoff_installed = {
                let mut scope = self
                    .scope
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if scope.in_flight_resolution_count() == 0 {
                    false
                } else {
                    // Acquire the replacement tracker ownership before this
                    // tracked future can drop its own token. The tracker can
                    // therefore never appear empty between owners.
                    let tracker_token = self.cleanup_tasks.token();
                    let task_id = self.task_id;
                    let scope_id = self.scope_id.clone();
                    let abort_handles = Arc::clone(&self.abort_handles);
                    let closing_scopes = Arc::clone(&self.closing_scopes);
                    let cleanup_outcomes = Arc::clone(&self.cleanup_outcomes);
                    let scopes_changed = Arc::clone(&self.scopes_changed);
                    let ledger_detached = Arc::clone(&self.ledger_detached);
                    let handoff = ScopeResolutionDrainHandoff::new(move |instances| {
                        ledger_detached.store(true, Ordering::Release);
                        cleanup_outcomes
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .extend(forced_scope_outcomes(
                                &scope_id,
                                &instances,
                                "scope lifecycle entry was force-reconciled by the final admitted resolution",
                            ));
                        drop(instances);
                        if remove_cleanup_registration(
                            task_id,
                            &scope_id,
                            &abort_handles,
                            &closing_scopes,
                        ) {
                            scopes_changed.notify_waiters();
                        }
                        drop(tracker_token);
                    });
                    let installed = scope.install_resolution_drain_handoff(handoff);
                    debug_assert!(
                        installed.is_ok(),
                        "one scope generation cannot own two resolution-drain handoffs"
                    );
                    true
                }
            };
            if handoff_installed {
                // A lost task must never release an ID while an admitted
                // factory can still publish into this closing generation.
                return;
            }

            let instances = {
                let mut scope = self
                    .scope
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                scope.mark_closed();
                scope.take_instances_for_disposal()
            };
            self.cleanup_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .extend(forced_scope_outcomes(
                    &self.scope_id,
                    &instances,
                    "scope lifecycle entry was force-reconciled before cleanup task drop",
                ));
            drop(instances);
            self.ledger_detached.store(true, Ordering::Release);
        }

        if remove_cleanup_registration(
            self.task_id,
            &self.scope_id,
            &self.abort_handles,
            &self.closing_scopes,
        ) {
            self.scopes_changed.notify_waiters();
        }
    }
}

impl Drop for ScopeCleanupTaskRegistration {
    fn drop(&mut self) {
        self.release();
    }
}

/// Container-owned, crate-internal manager for request/job scopes.
#[derive(Debug)]
pub(crate) struct ScopeManager {
    /// Map of process_id -> ScopeContext
    /// Uses RwLock for thread-safe concurrent access
    scopes: RwLock<HashMap<String, Arc<RwLock<ScopeContext>>>>,
    /// IDs remain reserved while their asynchronous cleanup is running. This
    /// prevents a new scope from reusing an identifier while the previous
    /// scope's disposer is still executing with the same process context.
    closing_scopes: ClosingScopes,
    scopes_changed: Arc<Notify>,
    accepting_scopes: AtomicBool,
    /// All asynchronous scope cleanup work is owned by the application
    /// container through this tracker and the actual join ledger below. The
    /// ledger is reaped on registration and observation, without driver tasks.
    cleanup_tasks: TaskTracker,
    cleanup_joins: ScopeTaskJoins,
    /// Abort handles make the DI shutdown deadline cover disposal itself, not
    /// only the wait for active request/job guards. Entries are removed by a
    /// normally completed task and atomically drained by forced cancellation.
    cleanup_abort_handles: ScopeCleanupAbortHandles,
    next_cleanup_task_id: AtomicU64,
    completed_cleanup_tasks: Arc<AtomicU64>,
    cleanup_failures: Arc<Mutex<Vec<InjectionError>>>,
    cleanup_outcomes: Arc<Mutex<Vec<ShutdownOutcome>>>,
    record_shutdown_outcomes: Arc<AtomicBool>,
    /// Captured while the application container is built. It allows a scope
    /// guard dropped on a non-runtime thread to schedule cleanup on the owning
    /// runtime instead of silently leaking its resources.
    runtime: Option<tokio::runtime::Handle>,
}

impl ScopeManager {
    /// Create a new ScopeManager
    pub(crate) fn new() -> Self {
        Self {
            scopes: RwLock::new(HashMap::new()),
            closing_scopes: Arc::new(Mutex::new(HashMap::new())),
            scopes_changed: Arc::new(Notify::new()),
            accepting_scopes: AtomicBool::new(true),
            cleanup_tasks: TaskTracker::new(),
            cleanup_joins: ScopeTaskJoins::default(),
            cleanup_abort_handles: Arc::new(Mutex::new(HashMap::new())),
            next_cleanup_task_id: AtomicU64::new(0),
            completed_cleanup_tasks: Arc::new(AtomicU64::new(0)),
            cleanup_failures: Arc::new(Mutex::new(Vec::new())),
            cleanup_outcomes: Arc::new(Mutex::new(Vec::new())),
            record_shutdown_outcomes: Arc::new(AtomicBool::new(false)),
            runtime: tokio::runtime::Handle::try_current().ok(),
        }
    }

    /// Create a new managed scope. Reusing a live process ID is rejected so
    /// two request/job guards can never silently share scoped state.
    #[cfg(test)]
    pub(crate) fn create_scope(
        &self,
        process_id: &str,
    ) -> Result<Arc<RwLock<ScopeContext>>, InjectionError> {
        self.create_scope_with_context(process_id, None)
    }

    pub(crate) fn create_application_scope(
        &self,
        context: lily_process::ProcessContext,
    ) -> Result<Arc<RwLock<ScopeContext>>, InjectionError> {
        self.create_scope_with_context(&context.process_id_string(), Some(context))
    }

    fn create_scope_with_context(
        &self,
        process_id: &str,
        context: Option<lily_process::ProcessContext>,
    ) -> Result<Arc<RwLock<ScopeContext>>, InjectionError> {
        let mut scopes_write = self
            .scopes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.accepting_scopes.load(Ordering::Acquire) {
            return Err(InjectionError::ContainerClosing);
        }
        let closing_scopes = self
            .closing_scopes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if scopes_write.contains_key(process_id) || closing_scopes.contains_key(process_id) {
            return Err(InjectionError::ScopeAlreadyActive {
                scope_id: process_id.to_string(),
            });
        }

        let mut new_scope = ScopeContext::new(process_id.to_string());
        new_scope.owner_context = context;
        let new_scope = Arc::new(RwLock::new(new_scope));
        scopes_write.insert(process_id.to_string(), Arc::clone(&new_scope));
        drop(closing_scopes);
        self.scopes_changed.notify_waiters();
        Ok(new_scope)
    }

    /// Atomically close scope admission relative to `create_scope`.
    /// Container shutdown must call this before observing/draining live scopes.
    pub(crate) fn stop_accepting_scopes(&self) {
        let _scopes = self
            .scopes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.accepting_scopes.store(false, Ordering::Release);
        self.record_shutdown_outcomes.store(true, Ordering::Release);
        self.scopes_changed.notify_waiters();
    }

    /// Find a scope previously opened by an [`ApplicationScope`](crate::ApplicationScope).
    /// Resolution must use this lookup and must never create an unmanaged
    /// scope as a side effect.
    pub(crate) fn get_scope(&self, process_id: &str) -> Option<Arc<RwLock<ScopeContext>>> {
        self.scopes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(process_id)
            .cloned()
    }

    /// Find an open scope or one whose cleanup is waiting for an admitted
    /// resolution. This is intentionally limited to lifecycle bookkeeping:
    /// ordinary resolution must never enter a Closing scope.
    pub(crate) fn get_scope_for_lifecycle_registration(
        &self,
        process_id: &str,
    ) -> Option<Arc<RwLock<ScopeContext>>> {
        self.get_scope(process_id).or_else(|| {
            self.closing_scopes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(process_id)
                .cloned()
        })
    }

    /// Remove a scope (useful for cleanup when a request/process ends)
    #[allow(dead_code, reason = "retained for crate-local scope contract tests")]
    pub(crate) fn remove_scope(&self, process_id: &str) -> Option<Arc<RwLock<ScopeContext>>> {
        let mut scopes_write = self.scopes.write().unwrap();
        let removed = scopes_write.remove(process_id);
        if removed.is_some() {
            self.scopes_changed.notify_waiters();
        }
        removed
    }

    /// Remove a live scope and schedule its cleanup as container-owned work.
    ///
    /// The returned ticket is only an observer. Cleanup continues when the
    /// ticket or the request task awaiting it is dropped.
    pub(crate) fn begin_scope_cleanup(
        &self,
        process_id: &str,
        context: Option<lily_process::ProcessContext>,
    ) -> Option<ScopeCleanupTicket> {
        self.begin_scope_cleanup_matching(process_id, context, None)
    }

    pub(crate) fn begin_observed_scope_cleanup(
        &self,
        observation: &ScopeCleanupObservation,
        context: Option<lily_process::ProcessContext>,
    ) -> Option<ScopeCleanupTicket> {
        self.begin_scope_cleanup_matching(&observation.scope_id, context, Some(&observation.scope))
    }

    fn begin_scope_cleanup_matching(
        &self,
        process_id: &str,
        context: Option<lily_process::ProcessContext>,
        expected: Option<&Arc<RwLock<ScopeContext>>>,
    ) -> Option<ScopeCleanupTicket> {
        // Keep the map's write lock until the cleanup task has been registered.
        // A concurrent shutdown drain can therefore never observe zero live
        // scopes before this scope's cleanup is owned by the task tracker.
        let mut scopes = self
            .scopes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let scope = scopes.get(process_id)?.clone();
        if expected.is_some_and(|expected| !Arc::ptr_eq(expected, &scope)) {
            return None;
        }
        scope
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .begin_closing();
        self.closing_scopes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(process_id.to_string(), Arc::clone(&scope));
        scopes.remove(process_id);
        let cleanup = self.schedule_scope_cleanup(scope, context);
        drop(scopes);
        self.scopes_changed.notify_waiters();
        Some(cleanup)
    }

    /// Atomically remove every live scope and register its disposal with the
    /// container-owned cleanup tracker. This is the forced-cleanup path used
    /// after the bounded active-work drain deadline expires.
    pub(crate) fn begin_cleanup_all_scopes(&self) -> usize {
        debug_assert!(
            !self.accepting_scopes.load(Ordering::Acquire),
            "scope admission must stop before forced cleanup"
        );
        let mut scopes = self
            .scopes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let removed: Vec<_> = scopes.drain().collect();
        let count = removed.len();
        for (scope_id, scope) in removed {
            scope
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .begin_closing();
            self.closing_scopes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(scope_id, Arc::clone(&scope));
            // The manager owns the task; forced shutdown observes aggregate
            // failures through `drain_cleanup_tasks` rather than a per-scope
            // ticket.
            drop(self.schedule_scope_cleanup(scope, None));
        }
        drop(scopes);
        self.scopes_changed.notify_waiters();
        count
    }

    fn schedule_scope_cleanup(
        &self,
        scope: Arc<RwLock<ScopeContext>>,
        context: Option<lily_process::ProcessContext>,
    ) -> ScopeCleanupTicket {
        let (result_tx, result_rx) = oneshot::channel();

        let (scope_id, trace, context) = {
            let scope = scope
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                scope.process_id.clone(),
                scope.trace.clone(),
                scope.owner_context.clone().or(context),
            )
        };

        let Some(runtime) = self.runtime.clone() else {
            let error = InjectionError::DisposeError(
                "Scope cleanup cannot run because its container was created outside a Tokio runtime"
                    .to_string(),
            );
            self.record_cleanup_failure(error.clone());
            self.closing_scopes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&scope_id);
            self.scopes_changed.notify_waiters();
            let _ = result_tx.send(Err(error));
            return ScopeCleanupTicket {
                result: result_rx,
                abort: None,
            };
        };

        let task_id = self.next_cleanup_task_id.fetch_add(1, Ordering::Relaxed);
        let terminal = Arc::new(AtomicU8::new(CLEANUP_TERMINAL_OPEN));
        let ledger_detached = Arc::new(AtomicBool::new(false));
        // Closing already rejects new resolution guards. If no admitted guard
        // exists at registration time, aborting the task drops the scope-owned
        // ledger itself and is immediately safe even before its first poll.
        let abort_safe = Arc::new(AtomicBool::new(
            scope
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .in_flight_resolution_count()
                == 0,
        ));
        let failures = Arc::clone(&self.cleanup_failures);
        let outcomes = Arc::clone(&self.cleanup_outcomes);
        let record_outcomes = Arc::clone(&self.record_shutdown_outcomes);
        let completed_cleanup_tasks = Arc::clone(&self.completed_cleanup_tasks);
        let task_scope_id = scope_id.clone();
        let ticket_scope_id = scope_id.clone();
        let registration = ScopeCleanupTaskRegistration {
            task_id,
            scope_id: task_scope_id.clone(),
            abort_handles: Arc::clone(&self.cleanup_abort_handles),
            closing_scopes: Arc::clone(&self.closing_scopes),
            cleanup_failures: Arc::clone(&self.cleanup_failures),
            cleanup_outcomes: Arc::clone(&self.cleanup_outcomes),
            scopes_changed: Arc::clone(&self.scopes_changed),
            terminal: Arc::clone(&terminal),
            scope: Arc::clone(&scope),
            ledger_detached: Arc::clone(&ledger_detached),
            cleanup_tasks: self.cleanup_tasks.clone(),
            released: false,
        };
        let task_terminal = Arc::clone(&terminal);
        let task_abort_safe = Arc::clone(&abort_safe);
        let task_ledger_detached = Arc::clone(&ledger_detached);
        let control_scope = Arc::clone(&scope);
        // Do not let a very short cleanup finish before its AbortHandle is in
        // the registry. The barrier closes that otherwise observable race.
        let (start_tx, start_rx) = oneshot::channel();
        let cleanup_task = self.cleanup_tasks.spawn_on(
            trace.bind(
                async move {
                    let _ = start_rx.await;
                    // Disposal runs directly inside the tracked task. Aborting the
                    // tracker-owned task therefore drops the disposer future too;
                    // no detached child can race singleton shutdown afterwards.
                    let (result, instance_outcomes) =
                        match AssertUnwindSafe(Self::dispose_removed_scope(
                            scope,
                            context,
                            task_abort_safe,
                            task_terminal,
                            task_ledger_detached,
                        ))
                        .catch_unwind()
                        .await
                        {
                            Ok(result) => result,
                            Err(payload) => (
                                Err(InjectionError::DisposalPanicked {
                                    service: "application scope".to_string(),
                                    message: Self::panic_payload_message(payload),
                                }),
                                Vec::new(),
                            ),
                        };

                    if !registration.claim_normal_completion() {
                        if !instance_outcomes.is_empty() {
                            outcomes
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .extend(instance_outcomes);
                        }
                        registration.finish();
                        return;
                    }
                    if let Err(error) = &result {
                        failures
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .push(error.clone());
                    }
                    if record_outcomes.load(Ordering::Acquire) || result.is_err() {
                        let outcome = match &result {
                            Ok(()) => ShutdownOutcome::completed(format!("scope:{task_scope_id}")),
                            Err(InjectionError::DisposalPanicked { message, .. }) => {
                                ShutdownOutcome::with_detail(
                                    format!("scope:{task_scope_id}"),
                                    ShutdownOutcomeStatus::Panicked,
                                    message.clone(),
                                )
                            }
                            Err(error) => ShutdownOutcome::with_detail(
                                format!("scope:{task_scope_id}"),
                                ShutdownOutcomeStatus::Failed,
                                error.to_string(),
                            ),
                        };
                        let mut outcomes = outcomes
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        outcomes.extend(instance_outcomes);
                        outcomes.push(outcome);
                    }
                    completed_cleanup_tasks.fetch_add(1, Ordering::Release);
                    registration.finish();
                    let _ = result_tx.send(result);
                },
                || tracing::info_span!(parent: None, "di.scope.dispose", otel.kind = "internal"),
            ),
            &runtime,
        );
        self.cleanup_abort_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                task_id,
                ScopeCleanupTaskControl {
                    abort_handle: cleanup_task.abort_handle(),
                    scope_id,
                    terminal,
                    scope: Arc::clone(&control_scope),
                    abort_safe,
                },
            );
        let join = async move { cleanup_task.await.map_err(Arc::new) }
            .boxed()
            .shared();
        self.cleanup_joins
            .insert(task_id, control_scope, join.clone());
        let _ = start_tx.send(());
        ScopeCleanupTicket {
            result: result_rx,
            abort: Some(ScopeCleanupAbort {
                task_id,
                scope_id: ticket_scope_id,
                abort_handles: Arc::clone(&self.cleanup_abort_handles),
                cleanup_failures: Arc::clone(&self.cleanup_failures),
                cleanup_outcomes: Arc::clone(&self.cleanup_outcomes),
                scopes_changed: Arc::clone(&self.scopes_changed),
                join,
            }),
        }
    }

    /// Cancel every still-running scope cleanup task after the configured
    /// shutdown deadline. The returned count lets the shutdown report mark
    /// the affected work as `Cancelled` without relying on string parsing.
    pub(crate) fn abort_cleanup_tasks(&self) -> usize {
        let abort_handles = {
            let registered = self
                .cleanup_abort_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            registered
                .values()
                .filter_map(|control| {
                    control
                        .terminal
                        .compare_exchange(
                            CLEANUP_TERMINAL_OPEN,
                            CLEANUP_TERMINAL_AGGREGATE_ABORT,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .ok()
                        .map(|_| control.clone())
                })
                .collect::<Vec<_>>()
        };
        let cancelled = abort_handles.len();
        for control in abort_handles {
            self.cleanup_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(ShutdownOutcome::with_detail(
                    format!("scope:{}", control.scope_id),
                    ShutdownOutcomeStatus::Cancelled,
                    "scope cleanup task was cancelled after the shutdown deadline",
                ));
            if control.is_abort_safe() {
                control.abort_handle.abort();
            }
        }
        if cancelled > 0 {
            self.record_cleanup_failure(InjectionError::DisposeError(format!(
                "{cancelled} scope cleanup task(s) cancelled after the shutdown deadline"
            )));
        }
        cancelled
    }

    /// Stop accepting new cleanup registrations and await every cleanup task
    /// already owned by this manager. The application container calls this
    /// only after scope admission has stopped and all live scopes were removed.
    pub(crate) async fn drain_cleanup_tasks(&self) -> Result<(), InjectionError> {
        self.cleanup_tasks.close();
        self.cleanup_tasks.wait().await;
        self.cleanup_joins.wait(None).await;

        let failures = std::mem::take(
            &mut *self
                .cleanup_failures
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        if failures.is_empty() {
            Ok(())
        } else {
            let panicked = failures
                .iter()
                .any(|error| matches!(error, InjectionError::DisposalPanicked { .. }));
            let message = failures
                .into_iter()
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            if panicked {
                Err(InjectionError::DisposalPanicked {
                    service: "scope cleanup tasks".to_string(),
                    message,
                })
            } else {
                Err(InjectionError::DisposeError(message))
            }
        }
    }

    pub(crate) fn cleanup_task_count(&self) -> usize {
        // The tracker also includes resolution-drain handoffs that can outlive
        // their original task. Zero requires both forms of ownership to end.
        self.cleanup_tasks.len().max(self.cleanup_joins.pending())
    }

    /// Count deadline-claimed cleanup tasks that still own admitted scope
    /// resolutions and therefore cannot yet be safely aborted.
    pub(crate) fn retained_resolution_drain_count(&self) -> usize {
        self.cleanup_abort_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|control| {
                !control.is_abort_safe()
                    && matches!(
                        control.terminal.load(Ordering::Acquire),
                        CLEANUP_TERMINAL_DEADLINE | CLEANUP_TERMINAL_AGGREGATE_ABORT
                    )
            })
            .count()
    }

    pub(crate) fn completed_cleanup_task_count(&self) -> usize {
        self.completed_cleanup_tasks.load(Ordering::Acquire) as usize
    }

    pub(crate) fn take_cleanup_outcomes(&self) -> Vec<ShutdownOutcome> {
        std::mem::take(
            &mut *self
                .cleanup_outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    fn record_cleanup_failure(&self, error: InjectionError) {
        self.cleanup_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(error);
    }

    /// Remove a scope from the live map and run every registered lifecycle
    /// callback in reverse construction order.
    #[allow(
        dead_code,
        reason = "legacy internal wrapper retained for DI API stability"
    )]
    pub(crate) async fn dispose_scope(&self, process_id: &str) -> Result<bool, InjectionError> {
        let Some(cleanup) = self.begin_scope_cleanup(process_id, None) else {
            return Ok(false);
        };
        cleanup.wait().await?;
        Ok(true)
    }

    pub(crate) async fn dispose_removed_scope(
        scope: Arc<RwLock<ScopeContext>>,
        context: Option<lily_process::ProcessContext>,
        abort_safe: Arc<AtomicBool>,
        terminal: Arc<AtomicU8>,
        ledger_detached: Arc<AtomicBool>,
    ) -> (Result<(), InjectionError>, Vec<ShutdownOutcome>) {
        // Factories admitted while the scope was Open own a resolution guard.
        // Closing rejects newcomers and waits here until those factories have
        // either entered the lifecycle ledger or failed, so no instance can be
        // published after the ledger has been drained.
        loop {
            let notify = {
                scope
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .in_flight_notify()
            };
            let in_flight = scope
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .in_flight_resolution_count();
            if in_flight == 0 {
                break;
            }
            notify.notified().await;
        }

        let (scope_id, instances) = {
            let mut scope = scope
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            scope.mark_closed();
            let scope_id = scope.process_id.clone();
            let instances = scope.take_instances_for_disposal();
            // Publish terminal ledger ownership before releasing the scope
            // lock. A concurrent deadline claimant must either observe the
            // pre-drain ledger or block until this bit is visible.
            ledger_detached.store(true, Ordering::Release);
            (scope_id, instances)
        };

        // No resolution can publish after this point: Closing rejected new
        // guards, every admitted guard reached terminal, and the complete
        // ledger is now owned by this task future. An abort can therefore drop
        // the disposer/remaining values without orphaning a late instance.
        abort_safe.store(true, Ordering::Release);
        if terminal.load(Ordering::Acquire) != CLEANUP_TERMINAL_OPEN {
            let outcomes = instances
                .iter()
                .map(|instance| {
                    let kind = match instance.kind {
                        ScopedInstanceKind::Scoped => "scoped",
                        ScopedInstanceKind::Transient => "transient",
                        ScopedInstanceKind::PartialInitialization => "partial-initialization",
                    };
                    ShutdownOutcome::with_detail(
                        format!("scope:{scope_id}:{kind}:{}", instance.type_name),
                        ShutdownOutcomeStatus::Cancelled,
                        "scope lifecycle entry was force-reconciled after its owner deadline",
                    )
                })
                .collect();
            drop(instances);
            return (Ok(()), outcomes);
        }

        let mut errors = Vec::new();
        let mut panicked = false;
        let mut outcomes = Vec::with_capacity(instances.len());
        for instance in instances {
            let kind = match instance.kind {
                ScopedInstanceKind::Scoped => "scoped",
                ScopedInstanceKind::Transient => "transient",
                ScopedInstanceKind::PartialInitialization => "partial-initialization",
            };
            let component = format!("scope:{scope_id}:{kind}:{}", instance.type_name);
            let Some(dispose_fn) = instance.dispose_fn else {
                outcomes.push(ShutdownOutcome::completed(component));
                continue;
            };
            let disposal = async {
                let disposal = dispose_fn(instance.value);
                if let Some(context) = context.as_ref() {
                    lily_process::ProcessContext::scope(context.clone(), disposal).await
                } else {
                    disposal.await
                }
            };
            match AssertUnwindSafe(disposal).catch_unwind().await {
                Ok(Ok(())) => outcomes.push(ShutdownOutcome::completed(component)),
                Ok(Err(error)) => {
                    let detail = error.to_string();
                    errors.push(detail.clone());
                    outcomes.push(ShutdownOutcome::with_detail(
                        component,
                        ShutdownOutcomeStatus::Failed,
                        detail,
                    ));
                }
                Err(payload) => {
                    panicked = true;
                    let detail = Self::panic_payload_message(payload);
                    errors.push(detail.clone());
                    outcomes.push(ShutdownOutcome::with_detail(
                        component,
                        ShutdownOutcomeStatus::Panicked,
                        detail,
                    ));
                }
            }
        }

        let result = if errors.is_empty() {
            Ok(())
        } else if panicked {
            Err(InjectionError::DisposalPanicked {
                service: "application scope".to_string(),
                message: errors.join("; "),
            })
        } else {
            Err(InjectionError::DisposeError(errors.join("; ")))
        };
        (result, outcomes)
    }

    fn panic_payload_message(payload: Box<dyn Any + Send>) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "non-string panic payload".to_string()
        }
    }

    /// Get the number of active scopes
    pub(crate) fn active_scope_count(&self) -> usize {
        let scopes_read = self.scopes.read().unwrap();
        scopes_read.len()
    }

    pub(crate) fn closing_scope_count(&self) -> usize {
        self.closing_scopes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Wait without polling until every managed request/job scope is closed.
    /// Admission must be stopped by the owning container before it awaits this
    /// method; otherwise new scopes may keep arriving indefinitely.
    pub(crate) async fn wait_for_no_active_scopes(&self) {
        loop {
            // Register the waiter before checking the count so a removal
            // between the check and `.await` cannot be missed.
            let changed = self.scopes_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active_scope_count() == 0 {
                return;
            }
            changed.as_mut().await;
        }
    }

    /// Wait until one exact scope ID is absent from both the live and closing
    /// maps. Registering the notification before inspecting both maps avoids a
    /// lost wake-up at the cleanup completion boundary.
    pub(crate) async fn wait_for_scope_cleanup(&self, scope_id: &str) {
        let Some(observation) = self.observe_scope_cleanup(scope_id) else {
            return;
        };
        self.wait_for_observed_scope_cleanup(observation).await;
    }

    pub(crate) fn observe_scope_cleanup(&self, scope_id: &str) -> Option<ScopeCleanupObservation> {
        let scope = self
            .scopes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(scope_id)
            .cloned()
            .or_else(|| {
                self.closing_scopes
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(scope_id)
                    .cloned()
            })?;
        Some(ScopeCleanupObservation {
            scope_id: scope_id.to_string(),
            scope,
        })
    }

    pub(crate) async fn wait_for_observed_scope_cleanup(
        &self,
        observation: ScopeCleanupObservation,
    ) {
        loop {
            let changed = self.scopes_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.scope_generation_is_active_or_closing(&observation) {
                self.cleanup_joins.wait(Some(&observation.scope)).await;
                return;
            }
            changed.as_mut().await;
        }
    }

    /// Wait for one exact scope through an adapter-owned absolute deadline.
    ///
    /// If the deadline expires while the scope is still live, this waiter
    /// follows the live-to-closing transition and claims only that scope's
    /// cleanup task as soon as it is registered. Returning after an abort is
    /// delayed until the task-owned RAII registration releases the closing
    /// reservation, which proves that the disposer future was dropped.
    ///
    /// The result describes ownership reconciliation, not the business result
    /// of a normally completed disposer. Normal disposer errors and panics stay
    /// in the canonical DI aggregate ledger and are reported by container
    /// shutdown; deadline, aggregate cancellation and unexpected task loss are
    /// returned here because they prevent graceful reconciliation.
    pub(crate) async fn wait_for_scope_cleanup_before(
        &self,
        scope_id: &str,
        deadline: tokio::time::Instant,
    ) -> Result<(), InjectionError> {
        let Some(observation) = self.observe_scope_cleanup(scope_id) else {
            return Ok(());
        };
        self.wait_for_observed_scope_cleanup_before(observation, deadline)
            .await
    }

    pub(crate) async fn wait_for_observed_scope_cleanup_before(
        &self,
        observation: ScopeCleanupObservation,
        deadline: tokio::time::Instant,
    ) -> Result<(), InjectionError> {
        let mut deadline_reached = tokio::time::Instant::now() >= deadline;
        let mut terminal_result = None;

        loop {
            // Register before inspecting either lifecycle map or the cleanup
            // control so no live->closing or closing->removed transition can
            // be missed.
            let changed = self.scopes_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.scope_generation_is_active_or_closing(&observation) {
                self.cleanup_joins.wait(Some(&observation.scope)).await;
                return terminal_result.unwrap_or(Ok(()));
            }

            if deadline_reached && terminal_result.is_none() {
                terminal_result = self.claim_scope_cleanup_deadline(&observation);
            }

            if terminal_result.is_some() {
                changed.as_mut().await;
                continue;
            }

            if deadline_reached {
                // The observer can be created before `ApplicationScope::Drop`
                // moves the ID from live to closing and registers its cleanup
                // control. Wait for that transition instead of repeatedly
                // selecting an already-expired timer.
                changed.as_mut().await;
                continue;
            }

            tokio::select! {
                biased;
                _ = changed.as_mut() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    deadline_reached = true;
                }
            }
        }
    }

    fn scope_generation_is_active_or_closing(&self, observation: &ScopeCleanupObservation) -> bool {
        if self
            .scopes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&observation.scope_id)
            .is_some_and(|scope| Arc::ptr_eq(scope, &observation.scope))
        {
            return true;
        }
        self.closing_scopes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&observation.scope_id)
            .is_some_and(|scope| Arc::ptr_eq(scope, &observation.scope))
    }

    fn claim_scope_cleanup_deadline(
        &self,
        observation: &ScopeCleanupObservation,
    ) -> Option<Result<(), InjectionError>> {
        let scope_id = &observation.scope_id;
        let control = self
            .cleanup_abort_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .find(|control| {
                control.scope_id == *scope_id && Arc::ptr_eq(&control.scope, &observation.scope)
            })
            .cloned()?;

        loop {
            match control.terminal.load(Ordering::Acquire) {
                CLEANUP_TERMINAL_OPEN => {
                    if control
                        .terminal
                        .compare_exchange(
                            CLEANUP_TERMINAL_OPEN,
                            CLEANUP_TERMINAL_DEADLINE,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    let error = InjectionError::ScopeCleanupTimedOut {
                        scope_id: scope_id.to_string(),
                    };
                    self.cleanup_failures
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(error.clone());
                    self.cleanup_outcomes
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(ShutdownOutcome::with_detail(
                            format!("scope:{scope_id}"),
                            ShutdownOutcomeStatus::TimedOut,
                            "scope cleanup task exceeded its owner deadline",
                        ));
                    if control.is_abort_safe() {
                        control.abort_handle.abort();
                    }
                    return Some(Err(error));
                }
                CLEANUP_TERMINAL_NORMAL => return Some(Ok(())),
                CLEANUP_TERMINAL_DEADLINE => {
                    return Some(Err(InjectionError::ScopeCleanupTimedOut {
                        scope_id: scope_id.to_string(),
                    }));
                }
                CLEANUP_TERMINAL_AGGREGATE_ABORT => {
                    return Some(Err(InjectionError::DisposeError(format!(
                        "Scope cleanup task for '{scope_id}' was cancelled by aggregate shutdown"
                    ))));
                }
                CLEANUP_TERMINAL_UNEXPECTED_STOP => {
                    return Some(Err(InjectionError::DisposeError(format!(
                        "Scope cleanup task for '{scope_id}' stopped without a terminal result"
                    ))));
                }
                _ => unreachable!("unknown scope cleanup terminal authority"),
            }
        }
    }

    /// Clear all scopes (useful for testing or shutdown)
    #[allow(dead_code, reason = "retained for crate-local scope contract tests")]
    pub(crate) fn clear_all_scopes(&self) {
        let mut scopes_write = self.scopes.write().unwrap();
        let count = scopes_write.len();
        scopes_write.clear();
        self.scopes_changed.notify_waiters();
        println!("🧹 Cleared {count} scopes");
    }

    /// Get statistics about cached services across all scopes
    #[allow(
        dead_code,
        reason = "retained for crate-local scope diagnostics and tests"
    )]
    pub(crate) fn get_cache_stats(&self) -> ScopeManagerStats {
        let scopes_read = self.scopes.read().unwrap();
        let mut total_cached_services = 0;
        let mut scope_details = Vec::new();

        for (process_id, scope_arc) in scopes_read.iter() {
            let scope = scope_arc.read().unwrap();
            let cached_count = scope.cached_count();
            total_cached_services += cached_count;

            scope_details.push(ScopeStats {
                process_id: process_id.clone(),
                cached_services: cached_count,
            });
        }

        ScopeManagerStats {
            active_scopes: scopes_read.len(),
            total_cached_services,
            scope_details,
        }
    }
}

impl Default for ScopeManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Crate-internal diagnostics for the scope manager.
#[derive(Debug, Clone)]
#[allow(
    dead_code,
    reason = "retained for crate-local scope diagnostics and tests"
)]
pub(crate) struct ScopeManagerStats {
    pub(crate) active_scopes: usize,
    pub(crate) total_cached_services: usize,
    pub(crate) scope_details: Vec<ScopeStats>,
}

/// Crate-internal diagnostics for one scope.
#[derive(Debug, Clone)]
#[allow(
    dead_code,
    reason = "retained for crate-local scope diagnostics and tests"
)]
pub(crate) struct ScopeStats {
    pub(crate) process_id: String,
    pub(crate) cached_services: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ScopeResolutionGuard;
    use lily_injection_registry::ServiceDisposeFn;
    use std::any::{Any, TypeId};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;
    use tokio::sync::Notify;

    struct ManagedTestService {
        disposed: Arc<AtomicUsize>,
        release: Arc<Notify>,
        fail: bool,
    }

    struct HangingManagedTestService {
        started: Arc<Notify>,
        release: Arc<Notify>,
        disposed: Arc<AtomicUsize>,
    }

    fn managed_test_disposer(
        instance: Arc<dyn Any + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<(), InjectionError>> + Send + 'static>> {
        Box::pin(async move {
            let instance = instance.downcast::<ManagedTestService>().map_err(|_| {
                InjectionError::DisposeError("test service downcast failed".to_string())
            })?;
            instance.release.notified().await;
            instance.disposed.fetch_add(1, Ordering::SeqCst);
            if instance.fail {
                Err(InjectionError::DisposeError(
                    "intentional cleanup failure".to_string(),
                ))
            } else {
                Ok(())
            }
        })
    }

    fn hanging_managed_test_disposer(
        instance: Arc<dyn Any + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<(), InjectionError>> + Send + 'static>> {
        Box::pin(async move {
            let instance = instance
                .downcast::<HangingManagedTestService>()
                .map_err(|_| {
                    InjectionError::DisposeError("hanging test service downcast failed".to_string())
                })?;
            instance.started.notify_one();
            instance.release.notified().await;
            instance.disposed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn register_managed_test_service(
        scope: &Arc<RwLock<ScopeContext>>,
        disposed: Arc<AtomicUsize>,
        release: Arc<Notify>,
        fail: bool,
    ) {
        let instance: Arc<dyn Any + Send + Sync> = Arc::new(ManagedTestService {
            disposed,
            release,
            fail,
        });
        scope.write().unwrap().cache_any(
            TypeId::of::<ManagedTestService>(),
            std::any::type_name::<ManagedTestService>(),
            instance,
            Some(managed_test_disposer as ServiceDisposeFn),
        );
    }

    #[test]
    fn test_scope_manager_creation() {
        let manager = ScopeManager::new();
        assert_eq!(manager.active_scope_count(), 0);
    }

    #[test]
    fn test_create_scope_rejects_duplicate_process_id() {
        let manager = ScopeManager::new();

        let scope1 = manager.create_scope("test-process-1").unwrap();
        assert_eq!(manager.active_scope_count(), 1);

        assert!(matches!(
            manager.create_scope("test-process-1"),
            Err(InjectionError::ScopeAlreadyActive { scope_id })
                if scope_id == "test-process-1"
        ));
        assert_eq!(manager.active_scope_count(), 1);

        let scope3 = manager.create_scope("test-process-2").unwrap();
        assert!(!Arc::ptr_eq(&scope1, &scope3));
        assert_eq!(manager.active_scope_count(), 2);
    }

    #[test]
    fn stopped_admission_rejects_new_scopes() {
        let manager = ScopeManager::new();
        manager.stop_accepting_scopes();

        assert!(matches!(
            manager.create_scope("too-late"),
            Err(InjectionError::ContainerClosing)
        ));
        assert_eq!(manager.active_scope_count(), 0);
    }

    #[test]
    fn test_remove_scope() {
        let manager = ScopeManager::new();

        let _scope1 = manager.create_scope("remove-test").unwrap();
        assert_eq!(manager.active_scope_count(), 1);

        let removed = manager.remove_scope("remove-test");
        assert!(removed.is_some());
        assert_eq!(manager.active_scope_count(), 0);

        // Removing non-existent scope should return None
        let removed2 = manager.remove_scope("non-existent");
        assert!(removed2.is_none());
    }

    #[test]
    fn test_clear_all_scopes() {
        let manager = ScopeManager::new();

        let _scope1 = manager.create_scope("clear-test-1").unwrap();
        let _scope2 = manager.create_scope("clear-test-2").unwrap();
        assert_eq!(manager.active_scope_count(), 2);

        manager.clear_all_scopes();
        assert_eq!(manager.active_scope_count(), 0);
    }

    #[test]
    fn test_cache_stats() {
        let manager = ScopeManager::new();

        let scope1 = manager.create_scope("stats-test-1").unwrap();
        let scope2 = manager.create_scope("stats-test-2").unwrap();

        // Add some mock cached services
        {
            let mut scope1_write = scope1.write().unwrap();
            scope1_write.cache_instance(Arc::new("test-service-1"));
            scope1_write.cache_instance(Arc::new(2_u32));
        }

        {
            let mut scope2_write = scope2.write().unwrap();
            scope2_write.cache_instance(Arc::new("test-service-3"));
        }

        let stats = manager.get_cache_stats();
        assert_eq!(stats.active_scopes, 2);
        assert_eq!(stats.total_cached_services, 3);
        assert_eq!(stats.scope_details.len(), 2);
    }

    #[tokio::test]
    async fn cleanup_remains_owned_after_observation_ticket_is_dropped() {
        let manager = ScopeManager::new();
        let disposed = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let scope = manager.create_scope("cancelled-waiter").unwrap();
        register_managed_test_service(&scope, Arc::clone(&disposed), Arc::clone(&release), false);

        let ticket = manager
            .begin_scope_cleanup("cancelled-waiter", None)
            .unwrap();
        assert_eq!(manager.active_scope_count(), 0);
        drop(ticket);
        release.notify_one();

        manager.drain_cleanup_tasks().await.unwrap();
        assert_eq!(disposed.load(Ordering::SeqCst), 1);
        assert_eq!(manager.cleanup_task_count(), 0);
        assert_eq!(manager.abort_cleanup_tasks(), 0);
    }

    #[tokio::test]
    async fn drain_aggregates_cleanup_failures_even_without_a_waiting_request() {
        let manager = ScopeManager::new();
        let disposed = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let scope = manager.create_scope("failing-cleanup").unwrap();
        register_managed_test_service(&scope, Arc::clone(&disposed), Arc::clone(&release), true);

        drop(
            manager
                .begin_scope_cleanup("failing-cleanup", None)
                .unwrap(),
        );
        release.notify_one();

        let error = manager.drain_cleanup_tasks().await.unwrap_err();
        assert!(error.to_string().contains("intentional cleanup failure"));
        assert_eq!(disposed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn no_active_scope_wait_is_event_driven() {
        let manager = Arc::new(ScopeManager::new());
        manager.create_scope("drain-me").unwrap();
        let waiting_manager = Arc::clone(&manager);
        let waiter = tokio::spawn(async move {
            waiting_manager.wait_for_no_active_scopes().await;
        });

        tokio::task::yield_now().await;
        let ticket = manager.begin_scope_cleanup("drain-me", None).unwrap();
        ticket.wait().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("scope drain waiter did not observe removal")
            .unwrap();
        manager.drain_cleanup_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn forced_cleanup_atomically_tracks_every_live_scope() {
        let manager = ScopeManager::new();
        manager.create_scope("forced-a").unwrap();
        manager.create_scope("forced-b").unwrap();
        manager.stop_accepting_scopes();

        assert_eq!(manager.begin_cleanup_all_scopes(), 2);
        assert_eq!(manager.active_scope_count(), 0);
        manager.drain_cleanup_tasks().await.unwrap();
        assert_eq!(manager.cleanup_task_count(), 0);
    }

    #[tokio::test]
    async fn deadline_abort_cancels_the_disposer_future_and_joins_the_tracker() {
        let manager = ScopeManager::new();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let disposed = Arc::new(AtomicUsize::new(0));
        let scope = manager.create_scope("deadline-abort").unwrap();
        let instance: Arc<dyn Any + Send + Sync> = Arc::new(HangingManagedTestService {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            disposed: Arc::clone(&disposed),
        });
        scope.write().unwrap().cache_any(
            TypeId::of::<HangingManagedTestService>(),
            std::any::type_name::<HangingManagedTestService>(),
            instance,
            Some(hanging_managed_test_disposer as ServiceDisposeFn),
        );

        let started_wait = started.notified();
        drop(manager.begin_scope_cleanup("deadline-abort", None).unwrap());
        started_wait.await;
        assert_eq!(manager.cleanup_task_count(), 1);

        assert_eq!(manager.abort_cleanup_tasks(), 1);
        assert_eq!(
            manager.cleanup_joins.pending(),
            1,
            "abort request is not a join"
        );
        let error = manager.drain_cleanup_tasks().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cancelled after the shutdown deadline")
        );
        assert_eq!(manager.cleanup_task_count(), 0);
        assert_eq!(
            manager.cleanup_joins.pending(),
            0,
            "actual disposer task join was observed"
        );
        let outcomes = manager.take_cleanup_outcomes();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| {
                    outcome.component == "scope:deadline-abort"
                        && outcome.status == ShutdownOutcomeStatus::Cancelled
                })
                .count(),
            1,
            "aggregate abort must publish one terminal outcome per cleanup"
        );

        // Releasing the disposer after the tracker has joined cannot resume
        // it: aborting the tracked task dropped the disposer future itself.
        release.notify_waiters();
        tokio::task::yield_now().await;
        assert_eq!(disposed.load(Ordering::SeqCst), 0);
        assert_eq!(manager.abort_cleanup_tasks(), 0);
    }

    #[tokio::test]
    async fn exact_deadline_waiter_follows_live_scope_and_joins_only_its_cleanup() {
        let manager = Arc::new(ScopeManager::new());
        let release = Arc::new(Notify::new());
        let disposed = Arc::new(AtomicUsize::new(0));
        let unrelated_release = Arc::new(Notify::new());
        let unrelated_disposed = Arc::new(AtomicUsize::new(0));
        let scope = manager.create_scope("exact-deadline-live").unwrap();
        let instance: Arc<dyn Any + Send + Sync> = Arc::new(HangingManagedTestService {
            started: Arc::new(Notify::new()),
            release: Arc::clone(&release),
            disposed: Arc::clone(&disposed),
        });
        scope.write().unwrap().cache_any(
            TypeId::of::<HangingManagedTestService>(),
            std::any::type_name::<HangingManagedTestService>(),
            instance,
            Some(hanging_managed_test_disposer as ServiceDisposeFn),
        );
        let unrelated = manager.create_scope("unrelated-cleanup").unwrap();
        register_managed_test_service(
            &unrelated,
            Arc::clone(&unrelated_disposed),
            Arc::clone(&unrelated_release),
            false,
        );
        drop(
            manager
                .begin_scope_cleanup("unrelated-cleanup", None)
                .unwrap(),
        );

        // This models the queue lease drop order: its observer can begin
        // before ApplicationScope::Drop has registered cleanup.
        let waiting_manager = Arc::clone(&manager);
        let waiter = tokio::spawn(async move {
            waiting_manager
                .wait_for_scope_cleanup_before("exact-deadline-live", tokio::time::Instant::now())
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(
            manager
                .begin_scope_cleanup("exact-deadline-live", None)
                .unwrap(),
        );
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("exact cleanup deadline waiter did not finish")
            .unwrap()
            .expect_err("an expired exact cleanup deadline must fail closed");
        assert!(matches!(
            error,
            InjectionError::ScopeCleanupTimedOut { scope_id }
                if scope_id == "exact-deadline-live"
        ));
        assert!(matches!(
            manager.create_scope("unrelated-cleanup"),
            Err(InjectionError::ScopeAlreadyActive { .. })
        ));
        unrelated_release.notify_one();
        manager.wait_for_scope_cleanup("unrelated-cleanup").await;
        assert_eq!(unrelated_disposed.load(Ordering::SeqCst), 1);

        // Returning from the waiter proves the task-owned guard released the
        // reservation; the same ID is reusable and the disposer cannot resume.
        let replacement = manager.create_scope("exact-deadline-live").unwrap();
        assert_eq!(disposed.load(Ordering::SeqCst), 0);
        release.notify_waiters();
        tokio::task::yield_now().await;
        assert_eq!(disposed.load(Ordering::SeqCst), 0);

        drop(
            manager
                .begin_scope_cleanup("exact-deadline-live", None)
                .unwrap(),
        );
        drop(replacement);
        drop(unrelated);
        let error = manager.drain_cleanup_tasks().await.unwrap_err();
        assert!(error.to_string().contains("ScopeCleanupTimedOut"));
        let outcomes = manager.take_cleanup_outcomes();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| {
                    outcome.component == "scope:exact-deadline-live"
                        && outcome.status == ShutdownOutcomeStatus::TimedOut
                })
                .count(),
            1
        );
        assert_eq!(manager.abort_cleanup_tasks(), 0);
    }

    #[tokio::test]
    async fn cancelling_exact_waiter_does_not_cancel_container_owned_cleanup() {
        let manager = Arc::new(ScopeManager::new());
        let disposed = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let scope = manager.create_scope("cancelled-exact-waiter").unwrap();
        register_managed_test_service(&scope, Arc::clone(&disposed), Arc::clone(&release), false);
        drop(
            manager
                .begin_scope_cleanup("cancelled-exact-waiter", None)
                .unwrap(),
        );

        let waiting_manager = Arc::clone(&manager);
        let waiter = tokio::spawn(async move {
            waiting_manager
                .wait_for_scope_cleanup_before(
                    "cancelled-exact-waiter",
                    tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                )
                .await
        });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(matches!(
            manager.create_scope("cancelled-exact-waiter"),
            Err(InjectionError::ScopeAlreadyActive { .. })
        ));

        release.notify_one();
        manager
            .wait_for_scope_cleanup("cancelled-exact-waiter")
            .await;
        manager.drain_cleanup_tasks().await.unwrap();
        assert_eq!(disposed.load(Ordering::SeqCst), 1);
        assert_eq!(manager.abort_cleanup_tasks(), 0);
    }

    #[tokio::test]
    async fn exact_ticket_waits_for_the_final_resolution_handoff() {
        let manager = Arc::new(ScopeManager::new());
        let scope = manager.create_scope("continuation-handoff").unwrap();
        let resolution = ScopeResolutionGuard::enter(Arc::clone(&scope)).unwrap();
        let ticket = manager
            .begin_scope_cleanup("continuation-handoff", None)
            .unwrap();
        let waiter = tokio::spawn(
            ticket.wait_until(tokio::time::Instant::now() + std::time::Duration::from_secs(30)),
        );
        let abort_handle = manager
            .cleanup_abort_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .next()
            .expect("cleanup control was not registered")
            .abort_handle
            .clone();
        abort_handle.abort();
        tokio::task::yield_now().await;

        assert!(
            !waiter.is_finished(),
            "a closed result sender must not bypass final-resolution ownership proof"
        );
        assert!(matches!(
            manager.create_scope("continuation-handoff"),
            Err(InjectionError::ScopeAlreadyActive { .. })
        ));
        assert!(manager.cleanup_task_count() >= 1);

        drop(resolution);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            manager.wait_for_scope_cleanup("continuation-handoff"),
        )
        .await
        .expect("final resolution handoff did not release the closing reservation");
        let error = waiter
            .await
            .expect("exact waiter task must join")
            .expect_err("unexpected cleanup stop must remain visible");
        assert!(
            error
                .to_string()
                .contains("stopped before reporting its result")
        );
        manager.create_scope("continuation-handoff").unwrap();
        let error = manager.drain_cleanup_tasks().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("stopped without a terminal outcome")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closing_generation_is_not_released_before_its_control() {
        let manager = Arc::new(ScopeManager::new());
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let disposed = Arc::new(AtomicUsize::new(0));
        let scope = manager.create_scope("generation-order").unwrap();
        let instance: Arc<dyn Any + Send + Sync> = Arc::new(HangingManagedTestService {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            disposed,
        });
        scope.write().unwrap().cache_any(
            TypeId::of::<HangingManagedTestService>(),
            std::any::type_name::<HangingManagedTestService>(),
            instance,
            Some(hanging_managed_test_disposer as ServiceDisposeFn),
        );

        let started_wait = started.notified();
        drop(
            manager
                .begin_scope_cleanup("generation-order", None)
                .unwrap(),
        );
        started_wait.await;
        let controls = manager
            .cleanup_abort_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        release.notify_one();
        let wait_started = std::time::Instant::now();
        while manager.completed_cleanup_task_count() == 0
            && wait_started.elapsed() < std::time::Duration::from_secs(1)
        {
            std::thread::yield_now();
        }
        assert_eq!(manager.completed_cleanup_task_count(), 1);
        std::thread::sleep(std::time::Duration::from_millis(10));

        assert!(matches!(
            manager.create_scope("generation-order"),
            Err(InjectionError::ScopeAlreadyActive { .. })
        ));
        drop(controls);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            manager.wait_for_scope_cleanup("generation-order"),
        )
        .await
        .expect("old generation did not release after its control lock");
        manager.create_scope("generation-order").unwrap();
        manager.drain_cleanup_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn unexpected_abort_hands_tracker_ownership_to_the_final_resolution_guard() {
        let manager = Arc::new(ScopeManager::new());
        let scope = manager.create_scope("resolution-handoff").unwrap();
        let resolution = ScopeResolutionGuard::enter(Arc::clone(&scope)).unwrap();
        drop(
            manager
                .begin_scope_cleanup("resolution-handoff", None)
                .unwrap(),
        );
        let original = manager
            .cleanup_abort_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .next()
            .expect("original cleanup control must exist")
            .abort_handle
            .clone();
        original.abort();
        tokio::task::yield_now().await;

        assert_eq!(manager.closing_scope_count(), 1);
        assert_eq!(manager.cleanup_abort_handles.lock().unwrap().len(), 1);
        assert_eq!(
            manager.cleanup_task_count(),
            1,
            "handoff token must prevent a false-empty cleanup tracker"
        );
        assert!(matches!(
            manager.create_scope("resolution-handoff"),
            Err(InjectionError::ScopeAlreadyActive { .. })
        ));

        let draining_manager = Arc::clone(&manager);
        let drain = tokio::spawn(async move { draining_manager.drain_cleanup_tasks().await });
        tokio::task::yield_now().await;
        assert!(
            !drain.is_finished(),
            "tracker drain must remain pending while a resolution owns the handoff"
        );

        drop(resolution);
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), drain)
            .await
            .expect("final resolution did not reconcile cleanup ownership")
            .expect("drain task panicked")
            .expect_err("unexpected cleanup loss must remain in the aggregate ledger");
        assert!(
            error
                .to_string()
                .contains("stopped without a terminal outcome")
        );
        assert_eq!(manager.cleanup_task_count(), 0);
        assert_eq!(manager.closing_scope_count(), 0);
        assert_eq!(manager.cleanup_abort_handles.lock().unwrap().len(), 0);
        manager.create_scope("resolution-handoff").unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observed_generation_does_not_join_or_claim_a_replacement_with_the_same_id() {
        let manager = Arc::new(ScopeManager::new());
        manager.create_scope("generation-observer").unwrap();
        let observation = manager
            .observe_scope_cleanup("generation-observer")
            .expect("live generation must be observable");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(50);
        let wait = manager.wait_for_observed_scope_cleanup_before(observation, deadline);
        tokio::pin!(wait);
        assert!(matches!(futures::poll!(wait.as_mut()), Poll::Pending));

        manager
            .begin_scope_cleanup("generation-observer", None)
            .unwrap()
            .wait()
            .await
            .unwrap();

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let disposed = Arc::new(AtomicUsize::new(0));
        let replacement = manager.create_scope("generation-observer").unwrap();
        let instance: Arc<dyn Any + Send + Sync> = Arc::new(HangingManagedTestService {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            disposed: Arc::clone(&disposed),
        });
        replacement.write().unwrap().cache_any(
            TypeId::of::<HangingManagedTestService>(),
            std::any::type_name::<HangingManagedTestService>(),
            instance,
            Some(hanging_managed_test_disposer as ServiceDisposeFn),
        );
        let replacement_ticket = manager
            .begin_scope_cleanup("generation-observer", None)
            .unwrap();
        started.notified().await;
        tokio::time::sleep_until(deadline).await;

        wait.await
            .expect("old generation observer must ignore its replacement");
        assert_eq!(disposed.load(Ordering::SeqCst), 0);
        assert_eq!(manager.closing_scope_count(), 1);
        release.notify_one();
        replacement_ticket.wait().await.unwrap();
        assert_eq!(disposed.load(Ordering::SeqCst), 1);
        manager.drain_cleanup_tasks().await.unwrap();
    }
}
