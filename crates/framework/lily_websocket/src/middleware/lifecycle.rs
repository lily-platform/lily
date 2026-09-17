//! Server-owned reconciliation for connection middleware cleanup.
//!
//! A connection task may be cancelled or panic at any await point. Cleanup is
//! therefore claimed by an owned task recorded in this registry, not by the
//! connection future that happened to notice the terminal event.

use super::{
    CompiledWsConnectionChain, WsConnectionCloseCategory, WsConnectionLedger,
    WsMiddlewareCleanupReport,
};
use crate::WebSocketContext;
use crate::connection::ConnectionManager;
#[cfg(test)]
use crate::lifecycle::LifecycleStopRequest;
use crate::lifecycle::{
    LifecycleInterruption, LifecycleInvocation, LifecycleInvocationState, LifecycleOutcome,
};
use futures_util::{FutureExt, future::BoxFuture, future::Shared};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionCleanupRegistrationError {
    DuplicateConnection,
    ManagerAlreadyAttached,
    FinalizationStarted,
    RuntimeUnavailable,
    ZeroTimeout,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ConnectionCleanupOutcome {
    Completed(ConnectionCleanupReport),
    Panicked {
        terminal: ConnectionTerminalHookReport,
        manager_cleanup_failed: bool,
    },
}

pub(crate) type ConnectionTerminalHook = Arc<
    dyn Fn(
            WsConnectionCloseCategory,
            CancellationToken,
        ) -> BoxFuture<'static, ConnectionTerminalHookReport>
        + Send
        + Sync,
>;

/// Bounded, secret-free terminal lifecycle result returned by the app-owned
/// disconnect adapter. Individual handler failures are represented only by
/// counters; dynamic errors never cross into the registry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ConnectionTerminalHookReport {
    attempted: usize,
    completed: usize,
    failed: usize,
    timed_out: usize,
    cancelled: usize,
    panicked: usize,
    not_started: usize,
}

impl ConnectionTerminalHookReport {
    pub(crate) const fn new(
        attempted: usize,
        completed: usize,
        failed: usize,
        timed_out: usize,
        panicked: usize,
    ) -> Self {
        Self {
            attempted,
            completed,
            failed,
            timed_out,
            cancelled: 0,
            panicked,
            not_started: 0,
        }
    }

    pub(crate) const fn attempted(self) -> usize {
        self.attempted
    }

    pub(crate) const fn completed(self) -> usize {
        self.completed
    }

    pub(crate) const fn failed(self) -> usize {
        self.failed
    }

    pub(crate) const fn timed_out(self) -> usize {
        self.timed_out
    }

    pub(crate) const fn cancelled(self) -> usize {
        self.cancelled
    }

    pub(crate) const fn panicked(self) -> usize {
        self.panicked
    }

    pub(crate) const fn not_started(self) -> usize {
        self.not_started
    }

    fn skipped(obligations: usize) -> Self {
        Self {
            failed: obligations,
            not_started: obligations,
            ..Self::default()
        }
    }

    const fn panicked_stage() -> Self {
        Self::new(1, 0, 1, 0, 1)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionCleanupReport {
    middleware: WsMiddlewareCleanupReport,
    terminal: ConnectionTerminalHookReport,
    prerequisites_incomplete: bool,
    session_incomplete: bool,
    manager_mark_failed: bool,
    manager_remove_failed: bool,
}

impl ConnectionCleanupReport {
    pub(crate) const fn middleware(self) -> WsMiddlewareCleanupReport {
        self.middleware
    }

    pub(crate) const fn manager_cleanup_failed(self) -> bool {
        self.manager_mark_failed || self.manager_remove_failed
    }

    pub(crate) const fn terminal(self) -> ConnectionTerminalHookReport {
        self.terminal
    }
}

impl ConnectionCleanupOutcome {
    pub(crate) const fn prerequisites_incomplete(self) -> bool {
        matches!(self, Self::Completed(report) if report.prerequisites_incomplete)
    }

    pub(crate) const fn session_incomplete(self) -> bool {
        matches!(self, Self::Completed(report) if report.session_incomplete)
    }

    pub(crate) fn report(self) -> Option<WsMiddlewareCleanupReport> {
        match self {
            Self::Completed(report) => Some(report.middleware()),
            Self::Panicked { .. } => None,
        }
    }

    pub(crate) const fn manager_cleanup_failed(self) -> bool {
        match self {
            Self::Completed(report) => report.manager_cleanup_failed(),
            Self::Panicked {
                manager_cleanup_failed,
                ..
            } => manager_cleanup_failed,
        }
    }

    pub(crate) const fn terminal_report(self) -> ConnectionTerminalHookReport {
        match self {
            Self::Completed(report) => report.terminal(),
            Self::Panicked { terminal, .. } => terminal,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ConnectionCleanupDrainReport {
    pub(crate) prerequisites_incomplete: usize,
    pub(crate) session_incomplete: usize,
    pub(crate) connections: usize,
    pub(crate) completed: usize,
    pub(crate) panicked: usize,
    pub(crate) forced: bool,
    pub(crate) deadline_elapsed: bool,
    pub(crate) task_join_cancelled: usize,
    pub(crate) task_join_panicked: usize,
    pub(crate) registry_entries_remaining: usize,
    pub(crate) hooks_attempted: usize,
    pub(crate) hooks_failed: usize,
    pub(crate) hooks_timed_out: usize,
    pub(crate) hooks_cancelled: usize,
    pub(crate) hooks_cancellation_observed: usize,
    pub(crate) hooks_panicked: usize,
    pub(crate) terminal_attempted: usize,
    pub(crate) terminal_completed: usize,
    pub(crate) terminal_failed: usize,
    pub(crate) terminal_timed_out: usize,
    pub(crate) terminal_cancelled: usize,
    pub(crate) terminal_panicked: usize,
    pub(crate) terminal_not_started: usize,
    pub(crate) manager_cleanup_failed: usize,
}

/// Install prerequisite evidence before publishing an owner to the registry.
/// The obligation counter reads framework state only; it never invokes user code.
pub(crate) struct ConnectionCleanupReceipts {
    pub(crate) session: Shared<BoxFuture<'static, bool>>,
    pub(crate) children: ConnectionCleanupPrerequisites,
    pub(crate) terminal_obligations: Arc<dyn Fn() -> usize + Send + Sync>,
    pub(crate) terminal_accounting:
        Option<Arc<dyn Fn() -> crate::reporting::InvocationCounts + Send + Sync>>,
}

#[derive(Clone)]
pub(crate) struct ConnectionCleanupRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    entries: Mutex<HashMap<Uuid, Arc<ConnectionLifecycleOwner>>>,
    retired: Mutex<ConnectionAccounting>,
    shutdown_reconciliation: AtomicBool,
    runtime: Handle,
    receipt_drivers: TaskTracker,
    driver_joins: crate::tasks::TaskRegistry,
}

struct ConnectionLifecycleOwner {
    trace: lily_trace::__private::TraceContextSnapshot,
    connection_id: Uuid,
    chain: Arc<CompiledWsConnectionChain>,
    context: Arc<WebSocketContext>,
    ledger: WsConnectionLedger,
    terminal_hook: Option<ConnectionTerminalHook>,
    terminal_obligations: OnceLock<Arc<dyn Fn() -> usize + Send + Sync>>,
    terminal_accounting:
        OnceLock<Arc<dyn Fn() -> crate::reporting::InvocationCounts + Send + Sync>>,
    prerequisites: OnceLock<ConnectionCleanupPrerequisites>,
    session: OnceLock<Shared<BoxFuture<'static, bool>>>,
    manager: OnceLock<Arc<ConnectionManager>>,
    timeout: Duration,
    hard_cancellation: CancellationToken,
    state: Mutex<ConnectionLifecycleOwnerState>,
    completion: OnceLock<ConnectionCleanupOutcome>,
    completion_observed: AtomicBool,
    completion_notify: Notify,
}

pub(crate) type ConnectionCleanupPrerequisites =
    Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

type CleanupTaskJoinReceipt = Shared<BoxFuture<'static, CleanupTaskJoinStatus>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupTaskJoinStatus {
    Completed,
    Cancelled,
    Panicked,
}

#[derive(Default)]
struct ConnectionLifecycleOwnerState {
    worker: LifecycleInvocation,
    task: Option<CleanupTaskJoinReceipt>,
    #[cfg(test)]
    task_abort: Option<tokio::task::AbortHandle>,
}

/// RAII ownership for one registry entry.
///
/// Dropping this value before explicit finalization starts the same idempotent
/// owned cleanup used by normal and server-wide finalization.
pub(crate) struct ConnectionCleanupLease {
    entry: Arc<ConnectionLifecycleOwner>,
    registry: Weak<RegistryInner>,
}

impl ConnectionCleanupRegistry {
    pub(crate) fn new() -> Result<Self, ConnectionCleanupRegistrationError> {
        let runtime = Handle::try_current()
            .map_err(|_| ConnectionCleanupRegistrationError::RuntimeUnavailable)?;
        Ok(Self {
            inner: Arc::new(RegistryInner {
                entries: Mutex::new(HashMap::new()),
                retired: Mutex::new(ConnectionAccounting::default()),
                shutdown_reconciliation: AtomicBool::new(false),
                runtime,
                receipt_drivers: TaskTracker::new(),
                driver_joins: Default::default(),
            }),
        })
    }

    pub(crate) fn runtime_handle(&self) -> Handle {
        self.inner.runtime.clone()
    }

    /// Establishes the server shutdown ownership barrier. Cleanup tasks that
    /// become terminal after this point retain their compact outcome in the
    /// registry until `finalize_all_with_force` has replayed it.
    pub(crate) fn begin_shutdown_reconciliation(&self) {
        // Serialize the mode transition with receipt-driven map removal. A
        // completed entry is therefore either removed before this barrier or
        // retained for the shutdown snapshot; there is no observation gap.
        let _entries = mutex_guard(&self.inner.entries);
        self.inner
            .shutdown_reconciliation
            .store(true, Ordering::Release);
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn register(
        &self,
        connection_id: Uuid,
        chain: Arc<CompiledWsConnectionChain>,
        context: Arc<WebSocketContext>,
        terminal_hook: Option<ConnectionTerminalHook>,
        timeout: Duration,
    ) -> Result<ConnectionCleanupLease, ConnectionCleanupRegistrationError> {
        self.register_with_receipts(connection_id, chain, context, terminal_hook, timeout, None)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "publish the complete connection owner and its prerequisite receipts atomically"
    )]
    pub(crate) fn register_with_receipts(
        &self,
        connection_id: Uuid,
        chain: Arc<CompiledWsConnectionChain>,
        context: Arc<WebSocketContext>,
        terminal_hook: Option<ConnectionTerminalHook>,
        timeout: Duration,
        receipts: Option<ConnectionCleanupReceipts>,
    ) -> Result<ConnectionCleanupLease, ConnectionCleanupRegistrationError> {
        if timeout.is_zero() {
            return Err(ConnectionCleanupRegistrationError::ZeroTimeout);
        }
        let entry = Arc::new(ConnectionLifecycleOwner {
            trace: lily_trace::__private::TraceContextSnapshot::capture(),
            connection_id,
            chain,
            context,
            ledger: WsConnectionLedger::default(),
            terminal_hook,
            terminal_obligations: OnceLock::new(),
            terminal_accounting: OnceLock::new(),
            prerequisites: OnceLock::new(),
            session: OnceLock::new(),
            manager: OnceLock::new(),
            timeout,
            hard_cancellation: CancellationToken::new(),
            state: Mutex::new(ConnectionLifecycleOwnerState::default()),
            completion: OnceLock::new(),
            completion_observed: AtomicBool::new(false),
            completion_notify: Notify::new(),
        });
        if let Some(receipts) = receipts {
            if let Some(accounting) = receipts.terminal_accounting {
                assert!(entry.terminal_accounting.set(accounting).is_ok());
            }
            assert!(entry.session.set(receipts.session).is_ok());
            assert!(entry.prerequisites.set(receipts.children).is_ok());
            assert!(
                entry
                    .terminal_obligations
                    .set(receipts.terminal_obligations)
                    .is_ok()
            );
        }
        let mut entries = mutex_guard(&self.inner.entries);
        if entries.contains_key(&connection_id) {
            return Err(ConnectionCleanupRegistrationError::DuplicateConnection);
        }
        entries.insert(connection_id, Arc::clone(&entry));
        Ok(ConnectionCleanupLease {
            entry,
            registry: Arc::downgrade(&self.inner),
        })
    }

    /// Reconcile every entry that existed at the server shutdown barrier.
    /// The shared join receipt removes each entry only after its worker task is
    /// terminal; the snapshot keeps those receipts replayable across waiters.
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) async fn finalize_all(
        &self,
        category: WsConnectionCloseCategory,
        deadline: Instant,
    ) -> ConnectionCleanupDrainReport {
        // Standalone reconciliation receives a total cap too. Reserve its
        // receipt tail inside that cap instead of adding a fallback timeout.
        for entry in mutex_guard(&self.inner.entries).values() {
            entry
                .context
                .shutdown_budget()
                .configure(Instant::now(), deadline);
            entry.context.shutdown_budget().force_before(deadline);
        }
        let force = CancellationToken::new();
        let mut report = self
            .finalize_all_inner(category, Some(deadline), &force)
            .await;
        report.deadline_elapsed |= report.hooks_timed_out > 0
            || report.terminal_timed_out > 0
            || report.prerequisites_incomplete > 0;
        report
    }

    /// Reconcile retained owners within the root cap. Force narrows cleanup
    /// budgets; it does not cancel an owner or wait unboundedly for first poll.
    pub(crate) async fn finalize_all_with_force(
        &self,
        category: WsConnectionCloseCategory,
        autonomous_failure_deadline: Option<Instant>,
        force: &CancellationToken,
    ) -> ConnectionCleanupDrainReport {
        self.finalize_all_inner(category, autonomous_failure_deadline, force)
            .await
    }

    async fn finalize_all_inner(
        &self,
        category: WsConnectionCloseCategory,
        fallback_deadline: Option<Instant>,
        force: &CancellationToken,
    ) -> ConnectionCleanupDrainReport {
        self.begin_shutdown_reconciliation();
        let entries = mutex_guard(&self.inner.entries)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in &entries {
            ConnectionLifecycleOwner::ensure_started(entry, Arc::downgrade(&self.inner), category);
        }

        let mut report = ConnectionCleanupDrainReport {
            connections: entries.len(),
            ..ConnectionCleanupDrainReport::default()
        };
        let mut deadline_signal: BoxFuture<'static, ()> = match fallback_deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).boxed(),
            None => std::future::pending().boxed(),
        };
        let budget = entries
            .first()
            .map(|entry| entry.context.shutdown_budget().clone())
            .unwrap_or_default();
        let mut force_applied = false;
        for entry in &entries {
            if !force_applied {
                tokio::select! {
                    biased;
                    () = force.cancelled() => { report.forced = true; force_applied = true; }
                    () = &mut deadline_signal => { report.deadline_elapsed = true; force_applied = true; }
                    _ = entry.wait() => {}
                }
                if force_applied {
                    let deadline = budget
                        .hard_deadline()
                        .unwrap_or_else(|| Instant::now() + Duration::from_millis(500));
                    for entry in &entries {
                        let owner_budget = entry.context.shutdown_budget();
                        if !owner_budget.is_forced() {
                            owner_budget.force_before(deadline);
                        }
                    }
                }
            }
            let Ok(outcome) = budget.reconcile(entry.wait()).await else {
                report.deadline_elapsed = true;
                break;
            };
            report.add_outcome(outcome);
            match budget.reconcile(entry.join_owned_task()).await {
                Ok(CleanupTaskJoinStatus::Completed) => self.remove_reconciled_entry(entry),
                Ok(CleanupTaskJoinStatus::Cancelled) => report.task_join_cancelled += 1,
                Ok(CleanupTaskJoinStatus::Panicked) => report.task_join_panicked += 1,
                Err(()) => {
                    report.deadline_elapsed = true;
                    break;
                }
            }
        }
        self.inner.receipt_drivers.close();
        if budget
            .reconcile(self.inner.receipt_drivers.wait())
            .await
            .is_err()
        {
            report.deadline_elapsed = true;
        }
        report.registry_entries_remaining = mutex_guard(&self.inner.entries).len();
        let _ = budget.reconcile(self.inner.driver_joins.wait()).await;
        report
    }

    /// Final task proof is independent of hook success. Failed workers retain
    /// their obligations until their actual join and the manager reconciliation.
    pub(crate) async fn reconcile(&self, budget: &crate::shutdown::ShutdownBudget) -> bool {
        let _ = budget.reconcile(self.inner.driver_joins.wait()).await;
        self.tasks_terminal()
    }

    pub(crate) fn tasks_terminal(&self) -> bool {
        self.inner.driver_joins.snapshot().outstanding == 0
            && mutex_guard(&self.inner.entries).values().all(|entry| {
                matches!(
                    mutex_guard(&entry.state).worker.state,
                    LifecycleInvocationState::Terminal { .. }
                )
            })
    }

    pub(crate) fn release_terminal_entries(&self) {
        mutex_guard(&self.inner.entries).retain(|_, entry| {
            let terminal = matches!(
                mutex_guard(&entry.state).worker.state,
                LifecycleInvocationState::Terminal { .. }
            );
            if terminal {
                mutex_guard(&self.inner.retired).record(entry);
            }
            !terminal
        });
    }

    fn remove_reconciled_entry(&self, entry: &Arc<ConnectionLifecycleOwner>) {
        let mut entries = mutex_guard(&self.inner.entries);
        if entries
            .get(&entry.connection_id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
            && let Some(entry) = entries.remove(&entry.connection_id)
        {
            mutex_guard(&self.inner.retired).record(&entry);
        }
    }

    /// Finalize one registry-owned connection. `None` means the identifier is
    /// already terminal or was never registered.
    pub(crate) async fn finalize_connection(
        &self,
        connection_id: Uuid,
        category: WsConnectionCloseCategory,
    ) -> Option<ConnectionCleanupOutcome> {
        let entry = mutex_guard(&self.inner.entries)
            .get(&connection_id)
            .cloned()?;
        ConnectionLifecycleOwner::ensure_started(&entry, Arc::downgrade(&self.inner), category);
        let outcome = entry.wait().await;
        let _ = entry.join_owned_task().await;
        Some(outcome)
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn entry_count(&self) -> usize {
        mutex_guard(&self.inner.entries).len()
    }

    pub(crate) fn accounting(&self) -> (ConnectionAccounting, crate::tasks::TaskSnapshot) {
        let drivers = self.inner.driver_joins.snapshot();
        let entries = mutex_guard(&self.inner.entries);
        let mut counts = *mutex_guard(&self.inner.retired);
        for entry in entries.values() {
            counts.record(entry);
        }
        (counts, drivers)
    }
}

/// Cumulative evidence survives compact registry removal. Worker completion,
/// cleanup success, and callback invocation are deliberately separate facts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConnectionAccounting {
    pub(crate) owners: usize,
    pub(crate) published: usize,
    pub(crate) forced: usize,
    pub(crate) stages_unobserved: usize,
    pub(crate) workers: crate::reporting::InvocationCounts,
    pub(crate) middleware: crate::reporting::LedgerCounts,
    pub(crate) disconnected: crate::reporting::InvocationCounts,
    pub(crate) stages: ConnectionCleanupDrainReport,
}

impl ConnectionAccounting {
    fn record(&mut self, entry: &ConnectionLifecycleOwner) {
        self.owners += 1;
        self.published += usize::from(entry.manager.get().is_some());
        self.forced += usize::from(
            entry.context.shutdown_budget().is_forced() || entry.hard_cancellation.is_cancelled(),
        );
        self.workers.record(mutex_guard(&entry.state).worker);
        self.middleware.merge(entry.ledger.accounting());
        if let Some(accounting) = entry.terminal_accounting.get() {
            self.disconnected.merge(accounting());
        }
        if entry.completion_observed.load(Ordering::Acquire)
            && let Some(outcome) = entry.completion.get()
        {
            self.stages.connections += 1;
            self.stages.add_outcome(*outcome);
        } else {
            self.stages_unobserved += 1;
        }
    }
}

impl ConnectionCleanupDrainReport {
    pub(crate) const fn unreconciled_hook_failures(&self) -> usize {
        self.hooks_failed
            .saturating_sub(self.hooks_cancellation_observed)
    }

    pub(crate) const fn unreconciled_terminal_failures(&self) -> usize {
        // Terminal cancellation is emitted only by the registry's private
        // hard-cancellation token; user hook failures arrive in `failed`
        // without incrementing this counter.
        self.terminal_failed.saturating_sub(self.terminal_cancelled)
    }

    fn add_outcome(&mut self, outcome: ConnectionCleanupOutcome) {
        match outcome {
            ConnectionCleanupOutcome::Completed(cleanup) => {
                self.session_incomplete += usize::from(cleanup.session_incomplete);
                if cleanup.prerequisites_incomplete {
                    self.prerequisites_incomplete += 1;
                } else {
                    self.completed = self.completed.saturating_add(1);
                }
                let middleware = cleanup.middleware();
                self.hooks_attempted = self.hooks_attempted.saturating_add(middleware.attempted());
                self.hooks_failed = self.hooks_failed.saturating_add(middleware.failed());
                self.hooks_timed_out = self.hooks_timed_out.saturating_add(middleware.timed_out());
                self.hooks_cancelled = self.hooks_cancelled.saturating_add(middleware.cancelled());
                self.hooks_cancellation_observed = self
                    .hooks_cancellation_observed
                    .saturating_add(middleware.cancellation_observed());
                self.hooks_panicked = self.hooks_panicked.saturating_add(middleware.panicked());
                if cleanup.manager_cleanup_failed() {
                    self.manager_cleanup_failed = self.manager_cleanup_failed.saturating_add(1);
                }
                self.add_terminal(cleanup.terminal());
            }
            ConnectionCleanupOutcome::Panicked {
                terminal,
                manager_cleanup_failed,
            } => {
                self.panicked = self.panicked.saturating_add(1);
                self.add_terminal(terminal);
                if manager_cleanup_failed {
                    self.manager_cleanup_failed = self.manager_cleanup_failed.saturating_add(1);
                }
            }
        }
    }

    fn add_terminal(&mut self, terminal: ConnectionTerminalHookReport) {
        self.terminal_attempted = self.terminal_attempted.saturating_add(terminal.attempted());
        self.terminal_completed = self.terminal_completed.saturating_add(terminal.completed());
        self.terminal_failed = self.terminal_failed.saturating_add(terminal.failed());
        self.terminal_timed_out = self.terminal_timed_out.saturating_add(terminal.timed_out());
        self.terminal_cancelled = self.terminal_cancelled.saturating_add(terminal.cancelled());
        self.terminal_panicked = self.terminal_panicked.saturating_add(terminal.panicked());
        self.terminal_not_started = self
            .terminal_not_started
            .saturating_add(terminal.not_started());
    }
}

impl ConnectionCleanupLease {
    /// The session owns startup execution and the complete transport. Its
    /// receipt must be installed before polling any connection user code.
    #[cfg(test)]
    pub(crate) fn attach_session(&self, receipt: Shared<BoxFuture<'static, bool>>) {
        let state = mutex_guard(&self.entry.state);
        assert_eq!(state.worker.state, LifecycleInvocationState::Pending);
        assert!(self.entry.session.set(receipt).is_ok());
    }

    /// Install child-owner receipts before user execution can enter. The
    /// registry retains this barrier after the transport/connection task drops.
    #[cfg(test)]
    pub(crate) fn attach_prerequisites(&self, prerequisites: ConnectionCleanupPrerequisites) {
        let state = mutex_guard(&self.entry.state);
        assert_eq!(state.worker.state, LifecycleInvocationState::Pending);
        assert!(self.entry.prerequisites.set(prerequisites).is_ok());
    }

    pub(crate) fn ledger(&self) -> WsConnectionLedger {
        self.entry.ledger.clone()
    }

    /// Transfers manager cleanup into the same exactly-once terminal task as
    /// middleware cleanup. This must happen immediately after manager admission.
    pub(crate) fn attach_manager(
        &self,
        manager: Arc<ConnectionManager>,
    ) -> Result<(), ConnectionCleanupRegistrationError> {
        let state = mutex_guard(&self.entry.state);
        if state.worker.state != LifecycleInvocationState::Pending {
            return Err(ConnectionCleanupRegistrationError::FinalizationStarted);
        }
        self.entry
            .manager
            .set(manager)
            .map_err(|_| ConnectionCleanupRegistrationError::ManagerAlreadyAttached)
    }

    pub(crate) async fn finalize(
        self,
        category: WsConnectionCloseCategory,
    ) -> ConnectionCleanupOutcome {
        ConnectionLifecycleOwner::ensure_started(&self.entry, self.registry.clone(), category);
        let outcome = self.entry.wait().await;
        let _ = self.entry.join_owned_task().await;
        outcome
    }
}

impl Drop for ConnectionCleanupLease {
    fn drop(&mut self) {
        let category = if std::thread::panicking() {
            WsConnectionCloseCategory::InternalError
        } else {
            WsConnectionCloseCategory::Cancelled
        };
        ConnectionLifecycleOwner::ensure_started(&self.entry, self.registry.clone(), category);
    }
}

impl ConnectionLifecycleOwner {
    fn ensure_started(
        entry: &Arc<Self>,
        registry: Weak<RegistryInner>,
        category: WsConnectionCloseCategory,
    ) {
        let Some(registry_inner) = registry.upgrade() else {
            return;
        };
        let mut state = mutex_guard(&entry.state);
        if !state.worker.claim() {
            return;
        }

        let cleanup_entry = Arc::clone(entry);
        let task = registry_inner.runtime.spawn(entry.trace.bind(async move {
            assert!(mutex_guard(&cleanup_entry.state).worker.start());
            // Freeze the producer before snapshotting message/scope receipts;
            // otherwise a still-running reader could register a later child.
            let session_terminal = cleanup_entry.wait_for_session().await;
            let children_terminal = session_terminal && cleanup_entry.wait_for_children().await;
            let manager_mark_failed = cleanup_entry.mark_manager_closing().await;
            // Controller lifecycle owns the first terminal callback after the
            // manager entry becomes Closing. Middleware then unwinds in
            // reverse order before the connection is removed from the
            // manager. Each stage remains independently panic/cancellation
            // contained so a failed disconnect handler cannot skip cleanup.
            let terminal = if children_terminal {
                AssertUnwindSafe(cleanup_entry.run_terminal_hook(category))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| ConnectionTerminalHookReport::panicked_stage())
            } else {
                ConnectionTerminalHookReport::skipped(cleanup_entry.armed_terminal_obligations())
            };
            // A dropped disconnected invocation can still have DI-owned
            // disposal in flight. Its receipt survives the invocation future.
            let scopes_terminal = children_terminal && cleanup_entry.wait_for_children().await;
            let cleanup = if scopes_terminal {
                AssertUnwindSafe(cleanup_entry.chain.cleanup(
                    Arc::clone(&cleanup_entry.context),
                    &cleanup_entry.ledger,
                    category,
                    cleanup_entry.timeout,
                    &cleanup_entry.hard_cancellation,
                ))
                .catch_unwind()
                .await
            } else {
                Ok(WsMiddlewareCleanupReport::default())
            };
            let manager_remove_failed = cleanup_entry.remove_from_manager().await;
            let outcome = match cleanup {
                Ok(middleware) => ConnectionCleanupOutcome::Completed(ConnectionCleanupReport {
                    middleware,
                    terminal,
                    prerequisites_incomplete: !scopes_terminal,
                    session_incomplete: !session_terminal,
                    manager_mark_failed,
                    manager_remove_failed,
                }),
                Err(_) => ConnectionCleanupOutcome::Panicked {
                    terminal,
                    manager_cleanup_failed: if manager_mark_failed {
                        true
                    } else {
                        manager_remove_failed
                    },
                },
            };
            let _ = cleanup_entry.completion.set(outcome);
            cleanup_entry
                .completion_observed
                .store(true, Ordering::Release);
            cleanup_entry.completion_notify.notify_waiters();
        }, || tracing::info_span!(parent: None, "websocket.connection.cleanup", otel.kind = "internal", lily.connection_id = %entry.connection_id)));
        #[cfg(test)]
        {
            state.task_abort = Some(task.abort_handle());
        }

        // The shared receipt, not an arbitrary waiter, owns the JoinHandle.
        // A runtime-owned driver keeps polling it if a connection waiter is
        // aborted. Registry removal occurs inside the receipt after the worker
        // has actually joined, so absence from the map is termination proof.
        let joined_entry = Arc::clone(entry);
        let joined_registry = Arc::downgrade(&registry_inner);
        let receipt = async move {
            let status = match task.await {
                Ok(()) => CleanupTaskJoinStatus::Completed,
                Err(error) if error.is_cancelled() => CleanupTaskJoinStatus::Cancelled,
                Err(_) => CleanupTaskJoinStatus::Panicked,
            };
            let outcome = match status {
                CleanupTaskJoinStatus::Completed => LifecycleOutcome::Completed,
                CleanupTaskJoinStatus::Cancelled => {
                    LifecycleOutcome::Interrupted(LifecycleInterruption::Aborted)
                }
                CleanupTaskJoinStatus::Panicked => LifecycleOutcome::Panicked,
            };
            // Publishing a cleanup report above is not task termination.
            // Only this shared join receipt may resolve the worker record.
            assert!(mutex_guard(&joined_entry.state).worker.finish(outcome));
            if status != CleanupTaskJoinStatus::Completed && joined_entry.completion.get().is_none()
            {
                let _ = joined_entry
                    .completion
                    .set(ConnectionCleanupOutcome::Panicked {
                        terminal: ConnectionTerminalHookReport::default(),
                        manager_cleanup_failed: true,
                    });
                joined_entry.completion_notify.notify_waiters();
            }
            if status == CleanupTaskJoinStatus::Completed
                && let Some(registry) = joined_registry.upgrade()
            {
                let mut entries = mutex_guard(&registry.entries);
                if !registry.shutdown_reconciliation.load(Ordering::Acquire)
                    && entries
                        .get(&joined_entry.connection_id)
                        .is_some_and(|current| Arc::ptr_eq(current, &joined_entry))
                    && let Some(entry) = entries.remove(&joined_entry.connection_id)
                {
                    mutex_guard(&registry.retired).record(&entry);
                }
            }
            status
        }
        .boxed()
        .shared();
        state.task = Some(receipt.clone());
        registry_inner
            .driver_joins
            .track(registry_inner.receipt_drivers.spawn_on(
                async move {
                    let _ = receipt.await;
                },
                &registry_inner.runtime,
            ));
    }

    async fn wait_for_session(&self) -> bool {
        match self.session.get() {
            Some(receipt) => self
                .context
                .shutdown_budget()
                .observe_cleanup(receipt.clone())
                .await
                .unwrap_or(false),
            // Synthetic registry users have no transport/session. Production
            // installs a receipt before admission or publication can occur.
            None => true,
        }
    }

    async fn wait_for_children(&self) -> bool {
        if let Some(prerequisites) = self.prerequisites.get() {
            return self
                .context
                .shutdown_budget()
                .observe_cleanup(prerequisites())
                .await
                .is_ok();
        }
        true
    }

    async fn mark_manager_closing(&self) -> bool {
        let Some(manager) = self.manager.get() else {
            return false;
        };
        self.context
            .shutdown_budget()
            .reconcile(AssertUnwindSafe(manager.mark_closing(self.connection_id)).catch_unwind())
            .await
            .map_or(true, |result| result.map_or(true, |result| result.is_err()))
    }

    async fn run_terminal_hook(
        &self,
        category: WsConnectionCloseCategory,
    ) -> ConnectionTerminalHookReport {
        let Some(terminal_hook) = self.terminal_hook.as_ref().cloned() else {
            return ConnectionTerminalHookReport::default();
        };
        let cancellation = self.hard_cancellation.child_token();
        let _invocation_authority = cancellation.clone().drop_guard();
        let mut started = false;
        let invocation = AssertUnwindSafe(async {
            started = true;
            terminal_hook(category, cancellation).await
        })
        .catch_unwind();
        match self
            .context
            .shutdown_budget()
            .cleanup(None, invocation)
            .await
        {
            Ok(Ok(report)) => report,
            Ok(Err(_)) => ConnectionTerminalHookReport::panicked_stage(),
            Err(()) if !started => {
                let mut report =
                    ConnectionTerminalHookReport::skipped(self.armed_terminal_obligations());
                report.timed_out = report.failed;
                report
            }
            Err(()) => ConnectionTerminalHookReport::new(1, 0, 1, 1, 0),
        }
    }

    fn armed_terminal_obligations(&self) -> usize {
        self.terminal_obligations.get().map_or_else(
            || usize::from(self.terminal_hook.is_some()),
            |count| count(),
        )
    }

    async fn remove_from_manager(&self) -> bool {
        let Some(manager) = self.manager.get() else {
            return false;
        };
        self.context
            .shutdown_budget()
            .reconcile(
                AssertUnwindSafe(manager.remove_connection(self.connection_id)).catch_unwind(),
            )
            .await
            .map_or(true, |result| result.map_or(true, |result| result.is_err()))
    }

    async fn wait(&self) -> ConnectionCleanupOutcome {
        loop {
            if let Some(outcome) = self.completion.get() {
                return *outcome;
            }
            let notified = self.completion_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(outcome) = self.completion.get() {
                return *outcome;
            }
            let receipt = self.owned_task_receipt();
            let wait = async {
                tokio::select! {
                    () = notified => {}
                    _ = receipt => {}
                }
            };
            #[cfg(test)]
            tokio::time::timeout(Duration::from_secs(1), wait)
                .await
                .expect("test cleanup completion remained pending");
            #[cfg(not(test))]
            wait.await;
        }
    }

    fn owned_task_receipt(&self) -> CleanupTaskJoinReceipt {
        mutex_guard(&self.state)
            .task
            .as_ref()
            .cloned()
            .expect("started WebSocket cleanup entry has no owned task receipt")
    }

    async fn join_owned_task(&self) -> CleanupTaskJoinStatus {
        self.owned_task_receipt().await
    }

    #[cfg(test)]
    fn abort_owned_task(&self) {
        let mut state = mutex_guard(&self.state);
        state.worker.request_stop(LifecycleStopRequest::Abort);
        state
            .task_abort
            .as_ref()
            .expect("started WebSocket cleanup entry has no abort handle")
            .abort();
    }
}

fn mutex_guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionManager;
    use crate::controller::{
        WebSocketControllerInitError, WebSocketControllerTrait, WebSocketLifecycleAction,
    };
    use crate::extractor::{
        DisconnectReason, Disconnected, FromWebSocketLifecycleParts, WebSocketLifecycleInvocation,
    };
    use crate::middleware::{WsConnectionMiddleware, WsMiddlewareError, WsMiddlewareInitError};
    use crate::request::ConnectionState;
    use async_trait::async_trait;
    use lily_injection::{ApplicationContainer, Extensions};
    use lily_middleware::{MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Semaphore;
    use tokio::time::timeout;

    #[derive(Clone)]
    enum CloseBehavior {
        Complete,
        Fail,
        Cancelled,
        Wait(Arc<Semaphore>),
        Panic,
    }

    struct TestConnectionMiddleware {
        name: &'static str,
        behavior: CloseBehavior,
        events: Arc<Mutex<Vec<(&'static str, WsConnectionCloseCategory)>>>,
        close_calls: Arc<AtomicUsize>,
        started: Option<Arc<Semaphore>>,
    }

    struct RejectAdmissionMiddleware {
        close_calls: Arc<AtomicUsize>,
    }

    struct ManagerOrderingMiddleware {
        manager: Arc<ConnectionManager>,
        connection_id: Uuid,
        observed_closing: Arc<std::sync::atomic::AtomicBool>,
        observed_category: Arc<Mutex<Option<WsConnectionCloseCategory>>>,
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }

    struct TerminalDropEvent {
        event: &'static str,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    struct PendingDisconnectedExtractor;

    #[derive(crate::WebSocketController)]
    #[namespace("force-pending-extractor")]
    struct PendingExtractorController;

    static PENDING_EXTRACTOR_STARTED: AtomicUsize = AtomicUsize::new(0);
    static PENDING_EXTRACTOR_DROPPED: AtomicUsize = AtomicUsize::new(0);
    static PENDING_EXTRACTOR_CONTROLLER_STARTED: AtomicUsize = AtomicUsize::new(0);
    static PENDING_EXTRACTOR_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct PendingExtractorDrop;

    impl Drop for PendingExtractorDrop {
        fn drop(&mut self) {
            PENDING_EXTRACTOR_DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl FromWebSocketLifecycleParts<Disconnected> for PendingDisconnectedExtractor {
        type Rejection = crate::controller::WebSocketLifecycleError;

        fn from_lifecycle_parts(
            _invocation: &mut WebSocketLifecycleInvocation,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            async {
                PENDING_EXTRACTOR_STARTED.fetch_add(1, Ordering::SeqCst);
                let _drop = PendingExtractorDrop;
                std::future::pending::<Result<Self, Self::Rejection>>().await
            }
        }
    }

    #[async_trait]
    impl WebSocketControllerTrait for PendingExtractorController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            Ok(Self)
        }
    }

    #[crate::websocket_controller]
    impl PendingExtractorController {
        #[disconnected]
        async fn disconnected(
            &self,
            _extractor: PendingDisconnectedExtractor,
        ) -> Result<(), crate::controller::WebSocketLifecycleError> {
            PENDING_EXTRACTOR_CONTROLLER_STARTED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl Drop for TerminalDropEvent {
        fn drop(&mut self) {
            mutex_guard(&self.events).push(self.event);
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for TestConnectionMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(self.name, MiddlewareKind::WebSocketConnection)
        }

        async fn closed(
            &self,
            _context: Arc<WebSocketContext>,
            category: WsConnectionCloseCategory,
            _cancellation: crate::CleanupCancellation,
        ) -> Result<(), WsMiddlewareError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            mutex_guard(&self.events).push((self.name, category));
            if let Some(started) = &self.started {
                started.add_permits(1);
            }
            match &self.behavior {
                CloseBehavior::Complete => Ok(()),
                CloseBehavior::Fail => Err(WsMiddlewareError::internal(
                    MiddlewareErrorCode::new("TEST_CLOSE_FAILURE").unwrap(),
                )),
                CloseBehavior::Cancelled => Err(WsMiddlewareError::cancelled()),
                CloseBehavior::Wait(release) => {
                    let permit = release.acquire().await.expect("test semaphore open");
                    permit.forget();
                    Ok(())
                }
                CloseBehavior::Panic => panic!("test-only middleware panic"),
            }
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for RejectAdmissionMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("admission_reject", MiddlewareKind::WebSocketConnection)
        }

        async fn admit(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WsMiddlewareError> {
            Err(WsMiddlewareError::rejected(
                MiddlewareErrorCode::new("TEST_ADMISSION_REJECTED").unwrap(),
            ))
        }

        async fn closed(
            &self,
            _context: Arc<WebSocketContext>,
            _category: WsConnectionCloseCategory,
            _cancellation: crate::CleanupCancellation,
        ) -> Result<(), WsMiddlewareError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for ManagerOrderingMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("manager_order", MiddlewareKind::WebSocketConnection)
        }

        async fn closed(
            &self,
            _context: Arc<WebSocketContext>,
            category: WsConnectionCloseCategory,
            _cancellation: crate::CleanupCancellation,
        ) -> Result<(), WsMiddlewareError> {
            let closing = self
                .manager
                .get_connection(self.connection_id)
                .await
                .is_some_and(|connection| connection.state == ConnectionState::Closing);
            self.observed_closing.store(closing, Ordering::SeqCst);
            *mutex_guard(&self.observed_category) = Some(category);
            self.started.add_permits(1);
            let permit = self.release.acquire().await.expect("test semaphore open");
            permit.forget();
            Ok(())
        }
    }

    fn test_manager() -> Arc<ConnectionManager> {
        Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["test".into()],
        ))
    }

    fn context(connection_id: Uuid) -> Arc<WebSocketContext> {
        Arc::new(WebSocketContext::new(
            connection_id,
            test_manager(),
            "test".into(),
        ))
    }

    fn test_deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    async fn wait_for_test_permit(semaphore: &Semaphore, context: &'static str) {
        timeout(Duration::from_secs(1), semaphore.acquire())
            .await
            .expect(context)
            .expect("test semaphore open")
            .forget();
    }

    fn middleware(
        name: &'static str,
        behavior: CloseBehavior,
        events: &Arc<Mutex<Vec<(&'static str, WsConnectionCloseCategory)>>>,
        close_calls: &Arc<AtomicUsize>,
        started: Option<Arc<Semaphore>>,
    ) -> Arc<dyn WsConnectionMiddleware> {
        Arc::new(TestConnectionMiddleware {
            name,
            behavior,
            events: Arc::clone(events),
            close_calls: Arc::clone(close_calls),
            started,
        })
    }

    async fn admitted_lease(
        middlewares: Vec<Arc<dyn WsConnectionMiddleware>>,
        cleanup_timeout: Duration,
    ) -> (
        ConnectionCleanupRegistry,
        ConnectionCleanupLease,
        Arc<CompiledWsConnectionChain>,
    ) {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let chain = Arc::new(CompiledWsConnectionChain::compile(middlewares).unwrap());
        let connection_id = Uuid::new_v4();
        let context = context(connection_id);
        let lease = registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                None,
                cleanup_timeout,
            )
            .unwrap();
        chain
            .admit(
                context,
                &lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        (registry, lease, chain)
    }

    #[test]
    fn cleanup_reports_preserve_every_bounded_counter_and_failure_source() {
        let middleware = WsMiddlewareCleanupReport {
            claimed: true,
            attempted: 7,
            completed: 2,
            failed: 5,
            timed_out: 3,
            cancelled: 1,
            cancellation_observed: 1,
            panicked: 4,
            first_failure: None,
        };
        let terminal = ConnectionTerminalHookReport::new(9, 2, 7, 3, 4);
        assert_eq!(terminal.attempted(), 9);
        assert_eq!(terminal.completed(), 2);
        assert_eq!(terminal.failed(), 7);
        assert_eq!(terminal.timed_out(), 3);
        assert_eq!(terminal.cancelled(), 0);
        assert_eq!(terminal.panicked(), 4);
        assert_eq!(middleware.cancellation_observed(), 1);

        for (manager_mark_failed, manager_remove_failed, expected) in [
            (false, false, false),
            (true, false, true),
            (false, true, true),
            (true, true, true),
        ] {
            let report = ConnectionCleanupReport {
                middleware,
                terminal,
                prerequisites_incomplete: false,
                session_incomplete: false,
                manager_mark_failed,
                manager_remove_failed,
            };
            assert_eq!(report.manager_cleanup_failed(), expected);

            let outcome = ConnectionCleanupOutcome::Completed(report);
            assert_eq!(outcome.report(), Some(middleware));
            assert_eq!(outcome.manager_cleanup_failed(), expected);
            assert_eq!(outcome.terminal_report(), terminal);
        }

        for manager_cleanup_failed in [false, true] {
            let outcome = ConnectionCleanupOutcome::Panicked {
                terminal,
                manager_cleanup_failed,
            };
            assert_eq!(outcome.report(), None);
            assert_eq!(outcome.manager_cleanup_failed(), manager_cleanup_failed);
            assert_eq!(outcome.terminal_report(), terminal);
        }
    }

    #[tokio::test]
    async fn registry_accessors_finalization_and_task_join_are_observable() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let connection_id = Uuid::new_v4();
        let lease = registry
            .register(
                connection_id,
                Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                context(connection_id),
                None,
                Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(registry.entry_count(), 1);

        let ledger = lease.ledger();
        assert!(ledger.record_entered(1));
        assert_eq!(lease.ledger().entered(), 1);
        let entry = Arc::clone(&lease.entry);
        let outcome = timeout(
            Duration::from_secs(1),
            lease.finalize(WsConnectionCloseCategory::NormalPeer),
        )
        .await
        .expect("single cleanup finalization remained bounded");
        assert!(matches!(outcome, ConnectionCleanupOutcome::Completed(_)));
        assert_eq!(registry.entry_count(), 0);
        assert_eq!(
            entry.join_owned_task().await,
            CleanupTaskJoinStatus::Completed
        );
        assert!(mutex_guard(&entry.state).task.is_some());

        let second_id = Uuid::new_v4();
        let second_lease = registry
            .register(
                second_id,
                Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                context(second_id),
                None,
                Duration::from_secs(1),
            )
            .unwrap();
        let outcome = timeout(
            Duration::from_secs(1),
            registry.finalize_connection(second_id, WsConnectionCloseCategory::NormalPeer),
        )
        .await
        .expect("registry finalization remained bounded");
        assert!(matches!(
            outcome,
            Some(ConnectionCleanupOutcome::Completed(_))
        ));
        assert_eq!(registry.entry_count(), 0);
        drop(second_lease);
    }

    #[tokio::test]
    async fn cleanup_is_reverse_best_effort_and_bounded_across_fail_timeout_and_panic() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let never_release = Arc::new(Semaphore::new(0));
        let middlewares = vec![
            middleware(
                "cleanup_a",
                CloseBehavior::Complete,
                &events,
                &close_calls,
                None,
            ),
            middleware(
                "cleanup_b",
                CloseBehavior::Fail,
                &events,
                &close_calls,
                None,
            ),
            middleware(
                "cleanup_c",
                CloseBehavior::Wait(never_release),
                &events,
                &close_calls,
                None,
            ),
            middleware(
                "cleanup_d",
                CloseBehavior::Panic,
                &events,
                &close_calls,
                None,
            ),
        ];
        let (registry, lease, _) = admitted_lease(middlewares, Duration::from_millis(10)).await;

        let outcome = timeout(
            Duration::from_secs(1),
            lease.finalize(WsConnectionCloseCategory::HandlerError),
        )
        .await
        .expect("owned cleanup remained bounded");
        let ConnectionCleanupOutcome::Completed(report) = outcome else {
            panic!("per-hook panic must be contained by the compiled chain");
        };
        let report = report.middleware();
        assert!(report.claimed());
        assert_eq!(report.attempted(), 4);
        assert_eq!(report.completed(), 1);
        assert_eq!(report.failed(), 3);
        assert_eq!(report.timed_out(), 1);
        assert_eq!(report.panicked(), 1);
        assert_eq!(close_calls.load(Ordering::SeqCst), 4);
        assert_eq!(
            mutex_guard(&events)
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            ["cleanup_d", "cleanup_c", "cleanup_b", "cleanup_a"]
        );
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn admission_failure_cleans_only_the_successfully_entered_prefix() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let entered_close_calls = Arc::new(AtomicUsize::new(0));
        let rejected_close_calls = Arc::new(AtomicUsize::new(0));
        let chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![
                middleware(
                    "entered",
                    CloseBehavior::Complete,
                    &events,
                    &entered_close_calls,
                    None,
                ),
                Arc::new(RejectAdmissionMiddleware {
                    close_calls: Arc::clone(&rejected_close_calls),
                }),
            ])
            .unwrap(),
        );
        let connection_id = Uuid::new_v4();
        let context = context(connection_id);
        let lease = registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                None,
                Duration::from_secs(1),
            )
            .unwrap();

        assert!(
            chain
                .admit(
                    context,
                    &lease.ledger(),
                    Duration::from_secs(1),
                    &CancellationToken::new(),
                )
                .await
                .is_err()
        );
        let ConnectionCleanupOutcome::Completed(report) = lease
            .finalize(WsConnectionCloseCategory::PolicyRejected)
            .await
        else {
            panic!("compiled cleanup must contain middleware panics");
        };
        let report = report.middleware();
        assert_eq!(report.attempted(), 1);
        assert_eq!(report.completed(), 1);
        assert_eq!(entered_close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(rejected_close_calls.load(Ordering::SeqCst), 0);
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn dropped_connection_retains_child_barrier_even_when_force_is_already_requested() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let (registry, lease, _) = admitted_lease(
            vec![middleware(
                "child_barrier",
                CloseBehavior::Complete,
                &events,
                &close_calls,
                None,
            )],
            Duration::from_secs(1),
        )
        .await;
        let children_ended = CancellationToken::new();
        let barrier = children_ended.clone();
        lease.attach_prerequisites(Arc::new(move || {
            let barrier = barrier.clone();
            async move { barrier.cancelled().await }.boxed()
        }));
        registry.begin_shutdown_reconciliation();
        drop(lease);
        let force = CancellationToken::new();
        force.cancel();
        let draining_registry = registry.clone();
        let draining = tokio::spawn(async move {
            draining_registry
                .finalize_all_with_force(WsConnectionCloseCategory::Cancelled, None, &force)
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(close_calls.load(Ordering::SeqCst), 0);
        assert!(!draining.is_finished());
        children_ended.cancel();
        let report = tokio::time::timeout(Duration::from_secs(1), draining)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(report.registry_entries_remaining, 0);
        assert_eq!(report.task_join_cancelled, 0);
        assert_eq!(report.task_join_panicked, 0);
        assert!(registry.inner.receipt_drivers.is_empty());
    }

    #[tokio::test]
    async fn normal_and_server_finalizers_race_but_start_one_owned_cleanup() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let (registry, lease, _) = admitted_lease(
            vec![middleware(
                "race",
                CloseBehavior::Wait(Arc::clone(&release)),
                &events,
                &close_calls,
                Some(Arc::clone(&started)),
            )],
            Duration::from_secs(1),
        )
        .await;

        let normal = tokio::spawn(lease.finalize(WsConnectionCloseCategory::NormalPeer));
        wait_for_test_permit(&started, "race cleanup did not start").await;
        let server_registry = registry.clone();
        let server = tokio::spawn(async move {
            server_registry
                .finalize_all(WsConnectionCloseCategory::ServerShutdown, test_deadline())
                .await
        });
        tokio::task::yield_now().await;
        release.add_permits(1);

        assert!(matches!(
            normal.await.unwrap(),
            ConnectionCleanupOutcome::Completed(_)
        ));
        let drain = server.await.unwrap();
        assert_eq!(drain.connections, 1);
        assert_eq!(drain.panicked, 0);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            mutex_guard(&events)[0].1,
            WsConnectionCloseCategory::NormalPeer
        );
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_deadline_with_unconfirmed_children_skips_user_hooks_and_reports_incomplete() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let connection_id = Uuid::new_v4();
        let calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = Arc::clone(&calls);
        let hook: ConnectionTerminalHook = Arc::new(move |_, _| {
            let calls = Arc::clone(&hook_calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
            }
            .boxed()
        });
        let lease = registry
            .register(
                connection_id,
                Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                context(connection_id),
                Some(hook),
                Duration::from_secs(30),
            )
            .unwrap();
        lease.attach_prerequisites(Arc::new(|| std::future::pending().boxed()));
        let start = Instant::now();
        let report = registry
            .finalize_all(
                WsConnectionCloseCategory::ServerShutdown,
                start + Duration::from_millis(100),
            )
            .await;
        assert_eq!(report.prerequisites_incomplete, 1);
        assert_eq!(report.completed, 0);
        assert_eq!(report.terminal_attempted, 0);
        assert_eq!(report.terminal_not_started, 1);
        assert_eq!(report.terminal_failed, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(report.registry_entries_remaining, 0);
        assert_eq!(report.task_join_cancelled, 0);
        assert!(Instant::now() <= start + Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn unconfirmed_or_failed_session_receipt_never_starts_user_termination() {
        for lost_sender in [false, true] {
            let registry = ConnectionCleanupRegistry::new().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let hook_calls = calls.clone();
            let hook: ConnectionTerminalHook = Arc::new(move |_, _| {
                let calls = hook_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
                }
                .boxed()
            });
            let id = Uuid::new_v4();
            let lease = registry
                .register(
                    id,
                    Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                    context(id),
                    Some(hook),
                    Duration::from_secs(1),
                )
                .unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            lease.attach_session(async move { rx.await.is_ok() }.boxed().shared());
            let sender = if lost_sender {
                drop(tx);
                None
            } else {
                Some(tx)
            };
            let child_snapshots = Arc::new(AtomicUsize::new(0));
            let snapshots = child_snapshots.clone();
            lease.attach_prerequisites(Arc::new(move || {
                snapshots.fetch_add(1, Ordering::SeqCst);
                async {}.boxed()
            }));
            let report = registry
                .finalize_all(
                    WsConnectionCloseCategory::Cancelled,
                    Instant::now() + Duration::from_millis(100),
                )
                .await;
            assert_eq!(report.completed, 0);
            assert_eq!(report.session_incomplete, 1);
            assert_eq!(report.prerequisites_incomplete, 1);
            assert_eq!(report.terminal_not_started, 1);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                child_snapshots.load(Ordering::SeqCst),
                0,
                "child snapshots require a stopped producer"
            );
            assert_eq!(report.task_join_cancelled, 0);
            drop(sender);
        }
    }

    #[tokio::test]
    async fn atomic_session_receipts_keep_concurrent_connection_cleanup_independent() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        let mut senders = Vec::new();
        for _ in 0..2 {
            let id = Uuid::new_v4();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let hook_calls = calls.clone();
            let hook: ConnectionTerminalHook = Arc::new(move |_, _| {
                let calls = hook_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
                }
                .boxed()
            });
            let lease = registry
                .register_with_receipts(
                    id,
                    Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                    context(id),
                    Some(hook),
                    Duration::from_secs(1),
                    Some(ConnectionCleanupReceipts {
                        session: async move { rx.await.is_ok() }.boxed().shared(),
                        children: Arc::new(|| async {}.boxed()),
                        terminal_obligations: Arc::new(|| 1),
                        terminal_accounting: None,
                    }),
                )
                .unwrap();
            // Registry-driven finalization can start immediately after insert;
            // it already sees all receipt gates, even before the lease drops.
            let cleanup = registry.clone();
            tasks.push(tokio::spawn(async move {
                let outcome = cleanup
                    .finalize_connection(id, WsConnectionCloseCategory::Cancelled)
                    .await
                    .unwrap();
                drop(lease);
                outcome
            }));
            senders.push(tx);
        }
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        senders.pop().unwrap().send(()).unwrap();
        let second = timeout(Duration::from_secs(1), tasks.pop().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.terminal_report().completed(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!tasks[0].is_finished());
        senders.pop().unwrap().send(()).unwrap();
        let first = timeout(Duration::from_secs(1), tasks.pop().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.terminal_report().completed(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn aborted_waiter_does_not_cancel_cleanup_and_server_can_reconcile_it() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let (registry, lease, _) = admitted_lease(
            vec![middleware(
                "abort",
                CloseBehavior::Wait(Arc::clone(&release)),
                &events,
                &close_calls,
                Some(Arc::clone(&started)),
            )],
            Duration::from_secs(1),
        )
        .await;

        let waiter = tokio::spawn(lease.finalize(WsConnectionCloseCategory::Cancelled));
        wait_for_test_permit(&started, "aborted waiter cleanup did not start").await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());

        let server_registry = registry.clone();
        let reconciliation = tokio::spawn(async move {
            server_registry
                .finalize_all(WsConnectionCloseCategory::ServerShutdown, test_deadline())
                .await
        });
        tokio::task::yield_now().await;
        release.add_permits(1);
        let drain = reconciliation.await.unwrap();
        assert_eq!(drain.connections, 1);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(registry.entry_count(), 0);

        // Retrying reconciliation after terminal self-removal is idempotent.
        let retry = registry
            .finalize_all(WsConnectionCloseCategory::ServerShutdown, test_deadline())
            .await;
        assert_eq!(retry.connections, 0);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropping_lease_starts_cleanup_and_duplicate_or_zero_timeout_registration_fails() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![middleware(
                "drop",
                CloseBehavior::Wait(Arc::clone(&release)),
                &events,
                &close_calls,
                Some(Arc::clone(&started)),
            )])
            .unwrap(),
        );
        let connection_id = Uuid::new_v4();
        let context = context(connection_id);
        let lease = registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                None,
                Duration::from_secs(1),
            )
            .unwrap();
        chain
            .admit(
                Arc::clone(&context),
                &lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(matches!(
            registry.register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                None,
                Duration::from_secs(1),
            ),
            Err(ConnectionCleanupRegistrationError::DuplicateConnection)
        ));
        assert!(matches!(
            registry.register(
                Uuid::new_v4(),
                Arc::clone(&chain),
                context,
                None,
                Duration::ZERO,
            ),
            Err(ConnectionCleanupRegistrationError::ZeroTimeout)
        ));

        drop(lease);
        wait_for_test_permit(&started, "dropped lease cleanup did not start").await;
        let server_registry = registry.clone();
        let reconciliation = tokio::spawn(async move {
            server_registry
                .finalize_all(WsConnectionCloseCategory::ServerShutdown, test_deadline())
                .await
        });
        tokio::task::yield_now().await;
        release.add_permits(1);
        reconciliation.await.unwrap();

        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            mutex_guard(&events)[0].1,
            WsConnectionCloseCategory::Cancelled
        );
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn connection_task_panic_starts_internal_cleanup_without_leaking_registry_state() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let (registry, lease, _) = admitted_lease(
            vec![middleware(
                "task_panic",
                CloseBehavior::Wait(Arc::clone(&release)),
                &events,
                &close_calls,
                Some(Arc::clone(&started)),
            )],
            Duration::from_secs(1),
        )
        .await;

        let connection_task = tokio::spawn(async move {
            let _cleanup_owner = lease;
            panic!("test-only connection task panic");
        });
        assert!(connection_task.await.unwrap_err().is_panic());
        wait_for_test_permit(&started, "panicked task cleanup did not start").await;

        let server_registry = registry.clone();
        let reconciliation = tokio::spawn(async move {
            server_registry
                .finalize_all(WsConnectionCloseCategory::ServerShutdown, test_deadline())
                .await
        });
        tokio::task::yield_now().await;
        release.add_permits(1);
        let drain = reconciliation.await.unwrap();

        assert_eq!(drain.connections, 1);
        assert_eq!(drain.completed, 1);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            mutex_guard(&events)[0].1,
            WsConnectionCloseCategory::InternalError
        );
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn attached_manager_is_closing_during_hooks_and_absent_after_owned_cleanup() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let observed_closing = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed_category = Arc::new(Mutex::new(None));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![Arc::new(ManagerOrderingMiddleware {
                manager: Arc::clone(&manager),
                connection_id,
                observed_closing: Arc::clone(&observed_closing),
                observed_category: Arc::clone(&observed_category),
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            })])
            .unwrap(),
        );
        let context = context(connection_id);
        let lease = registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                None,
                Duration::from_secs(1),
            )
            .unwrap();
        chain
            .admit(
                context,
                &lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        lease.attach_manager(Arc::clone(&manager)).unwrap();
        assert!(matches!(
            lease.attach_manager(Arc::clone(&manager)),
            Err(ConnectionCleanupRegistrationError::ManagerAlreadyAttached)
        ));

        // Simulate an outer connection-task abort/early return.
        drop(lease);
        wait_for_test_permit(&started, "manager cleanup did not start").await;
        assert!(observed_closing.load(Ordering::SeqCst));
        assert_eq!(
            *mutex_guard(&observed_category),
            Some(WsConnectionCloseCategory::Cancelled)
        );
        assert_eq!(manager.connection_count().await, 1);
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Closing
        );

        let server_registry = registry.clone();
        let reconciliation = tokio::spawn(async move {
            server_registry
                .finalize_all(WsConnectionCloseCategory::ServerShutdown, test_deadline())
                .await
        });
        tokio::task::yield_now().await;
        release.add_permits(1);
        let drain = reconciliation.await.unwrap();

        assert_eq!(drain.connections, 1);
        assert_eq!(drain.manager_cleanup_failed, 0);
        assert_eq!(manager.connection_count().await, 0);
        assert_eq!(registry.entry_count(), 0);
    }

    async fn assert_idle_queue_failure_preserves_registry_cleanup_order(queue_closed: bool) {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let observed_closing = Arc::new(AtomicBool::new(false));
        let observed_category = Arc::new(Mutex::new(None));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![Arc::new(ManagerOrderingMiddleware {
                manager: Arc::clone(&manager),
                connection_id,
                observed_closing: Arc::clone(&observed_closing),
                observed_category: Arc::clone(&observed_category),
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            })])
            .unwrap(),
        );
        let context = context(connection_id);
        let lease = registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                None,
                Duration::from_secs(1),
            )
            .unwrap();
        chain
            .admit(
                context,
                &lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let receiver = if queue_closed {
            drop(receiver);
            None
        } else {
            sender
                .try_send(tokio_tungstenite::tungstenite::Message::Ping(Vec::new()))
                .unwrap();
            Some(receiver)
        };
        let (control_sender, control_receiver) = crate::connection::connection_control_channel();
        drop(control_receiver);
        manager
            .add_connection_with_control(
                connection_id,
                sender,
                control_sender,
                lily_web_core::RequestConnectionInfo::default(),
                crate::server::WsTransportSecurity::Plaintext,
                Some("test".into()),
            )
            .await
            .unwrap();
        lease.attach_manager(Arc::clone(&manager)).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let claim = manager.cleanup_inactive_connections(Duration::ZERO).await;
        assert_eq!(claim.claimed_ids(), &[connection_id]);
        assert_eq!(claim.close_not_queued_ids(), &[connection_id]);
        assert_eq!(manager.connection_count().await, 1);
        assert_eq!(
            manager.get_connection(connection_id).await.unwrap().state,
            ConnectionState::Closing
        );

        let server_registry = registry.clone();
        let finalizer = tokio::spawn(async move {
            server_registry
                .finalize_connection(connection_id, WsConnectionCloseCategory::IdleTimeout)
                .await
                .expect("registry-owned idle connection")
        });
        wait_for_test_permit(&started, "idle cleanup did not start").await;
        assert!(observed_closing.load(Ordering::SeqCst));
        assert_eq!(
            *mutex_guard(&observed_category),
            Some(WsConnectionCloseCategory::IdleTimeout)
        );
        assert_eq!(manager.connection_count().await, 1);

        release.add_permits(1);
        assert!(matches!(
            finalizer.await.unwrap(),
            ConnectionCleanupOutcome::Completed(_)
        ));
        assert_eq!(manager.connection_count().await, 0);
        assert_eq!(registry.entry_count(), 0);
        drop(receiver);
        drop(lease);
    }

    #[tokio::test]
    async fn idle_closed_control_with_full_data_queue_runs_registry_cleanup() {
        assert_idle_queue_failure_preserves_registry_cleanup_order(false).await;
    }

    #[tokio::test]
    async fn idle_closed_control_with_closed_data_queue_runs_registry_cleanup() {
        assert_idle_queue_failure_preserves_registry_cleanup_order(true).await;
    }

    #[tokio::test]
    async fn force_gives_terminal_invocations_a_bounded_budget_and_observes_their_joins() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut leases = Vec::new();
        let mut entries = Vec::new();

        for (started_event, dropped_event) in [
            ("terminal_a_started", "terminal_a_dropped"),
            ("terminal_b_started", "terminal_b_dropped"),
        ] {
            let connection_id = Uuid::new_v4();
            let terminal_hook: ConnectionTerminalHook = {
                let events = Arc::clone(&events);
                Arc::new(move |_, _| {
                    let events = Arc::clone(&events);
                    async move {
                        mutex_guard(&events).push(started_event);
                        let _drop_event = TerminalDropEvent {
                            event: dropped_event,
                            events: Arc::clone(&events),
                        };
                        std::future::pending::<()>().await;
                        ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
                    }
                    .boxed()
                })
            };
            let lease = registry
                .register(
                    connection_id,
                    Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                    context(connection_id),
                    Some(terminal_hook),
                    Duration::from_secs(30),
                )
                .unwrap();
            let (sender, _receiver) = tokio::sync::mpsc::channel(1);
            manager
                .add_connection(connection_id, sender, None, Some("test".into()))
                .await
                .unwrap();
            lease.attach_manager(Arc::clone(&manager)).unwrap();
            entries.push(Arc::clone(&lease.entry));
            leases.push(lease);
        }

        let force = CancellationToken::new();
        force.cancel();
        let drain = timeout(
            Duration::from_secs(1),
            registry.finalize_all_with_force(WsConnectionCloseCategory::Cancelled, None, &force),
        )
        .await
        .expect("force cleanup did not reach its terminal join receipts");

        let observed = mutex_guard(&events).clone();
        let first_drop = observed
            .iter()
            .position(|event| event.ends_with("_dropped"))
            .expect("force did not drop pending terminal invocations");
        assert!(
            observed[..first_drop]
                .iter()
                .filter(|event| event.ends_with("_started"))
                .count()
                == 2,
            "both independently owned connections should start within the available budget: {observed:?}"
        );
        assert_eq!(
            observed
                .iter()
                .filter(|event| event.ends_with("_dropped"))
                .count(),
            2
        );
        assert!(drain.forced);
        assert!(!drain.deadline_elapsed);
        assert_eq!(drain.connections, 2);
        assert_eq!(drain.completed, 2);
        assert_eq!(drain.terminal_attempted, 2);
        assert_eq!(drain.terminal_cancelled, 0);
        assert_eq!(drain.terminal_timed_out, 2);
        assert_eq!(drain.task_join_cancelled, 0);
        assert_eq!(drain.task_join_panicked, 0);
        assert_eq!(drain.manager_cleanup_failed, 0);
        assert_eq!(drain.registry_entries_remaining, 0);
        assert_eq!(registry.entry_count(), 0);
        assert_eq!(manager.connection_count().await, 0);
        for entry in entries {
            assert_eq!(
                entry.join_owned_task().await,
                CleanupTaskJoinStatus::Completed
            );
        }
        drop(leases);
    }

    #[tokio::test]
    async fn shutdown_barrier_retains_returned_failures_and_forced_cleanup_timeouts() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));

        let direct_id = Uuid::new_v4();
        let direct_chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![middleware(
                "direct_cancelled",
                CloseBehavior::Cancelled,
                &events,
                &close_calls,
                None,
            )])
            .unwrap(),
        );
        let direct_context = context(direct_id);
        let direct_lease = registry
            .register(
                direct_id,
                Arc::clone(&direct_chain),
                Arc::clone(&direct_context),
                None,
                Duration::from_secs(30),
            )
            .unwrap();
        direct_chain
            .admit(
                direct_context,
                &direct_lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let direct_entry = Arc::clone(&direct_lease.entry);

        let pending_id = Uuid::new_v4();
        let pending_chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![middleware(
                "force_cancelled",
                CloseBehavior::Wait(Arc::new(Semaphore::new(0))),
                &events,
                &close_calls,
                None,
            )])
            .unwrap(),
        );
        let pending_context = context(pending_id);
        let pending_lease = registry
            .register(
                pending_id,
                Arc::clone(&pending_chain),
                Arc::clone(&pending_context),
                None,
                Duration::from_secs(30),
            )
            .unwrap();
        pending_chain
            .admit(
                pending_context,
                &pending_lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        registry.begin_shutdown_reconciliation();
        drop(direct_lease);
        drop(pending_lease);
        assert_eq!(
            direct_entry.join_owned_task().await,
            CleanupTaskJoinStatus::Completed
        );
        assert_eq!(
            registry.entry_count(),
            2,
            "the shutdown barrier must retain a completed failure receipt"
        );

        let force = CancellationToken::new();
        force.cancel();
        let drain = timeout(
            Duration::from_secs(1),
            registry.finalize_all_with_force(WsConnectionCloseCategory::Cancelled, None, &force),
        )
        .await
        .expect("force reconciliation did not join both cleanup owners");

        assert!(drain.forced);
        assert_eq!(drain.connections, 2);
        assert_eq!(drain.completed, 2);
        assert_eq!(drain.hooks_failed, 2);
        assert_eq!(drain.hooks_cancelled, 1);
        assert_eq!(drain.hooks_timed_out, 1);
        assert_eq!(drain.hooks_cancellation_observed, 0);
        assert_eq!(drain.unreconciled_hook_failures(), 2);
        assert_eq!(drain.task_join_cancelled, 0);
        assert_eq!(drain.task_join_panicked, 0);
        assert_eq!(drain.registry_entries_remaining, 0);
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn cleanup_abort_before_first_poll_requires_join_confirmation() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let connection_id = Uuid::new_v4();
        let lease = registry
            .register(
                connection_id,
                Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                context(connection_id),
                None,
                Duration::from_secs(30),
            )
            .unwrap();
        let entry = Arc::clone(&lease.entry);

        // This current-thread test has not yielded since the worker was
        // scheduled. The abort request cannot itself establish termination.
        drop(lease);
        entry.abort_owned_task();
        let worker = mutex_guard(&entry.state).worker;
        assert!(worker.abort_requested);
        assert_eq!(worker.state, LifecycleInvocationState::Claimed);
        assert!(entry.completion.get().is_none());
        assert_eq!(registry.entry_count(), 1);

        assert_eq!(
            entry.join_owned_task().await,
            CleanupTaskJoinStatus::Cancelled
        );
        assert_eq!(
            mutex_guard(&entry.state).worker.state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Interrupted(LifecycleInterruption::Aborted),
                started: false,
            }
        );
        // Joined does not mean the worker performed the cleanup obligation.
        assert_eq!(registry.entry_count(), 1);
    }

    #[tokio::test]
    async fn cancelled_cleanup_worker_is_replayed_as_unresolved_join_evidence() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let started = Arc::new(Semaphore::new(0));
        let terminal_hook: ConnectionTerminalHook = {
            let started = Arc::clone(&started);
            Arc::new(move |_, _| {
                let started = Arc::clone(&started);
                async move {
                    started.add_permits(1);
                    std::future::pending::<()>().await;
                    ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
                }
                .boxed()
            })
        };
        let lease = registry
            .register(
                connection_id,
                Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                context(connection_id),
                Some(terminal_hook),
                Duration::from_secs(30),
            )
            .unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        lease.attach_manager(Arc::clone(&manager)).unwrap();
        let entry = Arc::clone(&lease.entry);

        drop(lease);
        wait_for_test_permit(&started, "cleanup worker terminal hook did not start").await;
        entry.abort_owned_task();
        let worker = mutex_guard(&entry.state).worker;
        assert!(worker.abort_requested);
        assert_eq!(worker.state, LifecycleInvocationState::Running);

        let force = CancellationToken::new();
        force.cancel();
        let drain = timeout(
            Duration::from_secs(1),
            registry.finalize_all_with_force(WsConnectionCloseCategory::Cancelled, None, &force),
        )
        .await
        .expect("cancelled cleanup worker receipt was not replayed");

        assert!(drain.forced);
        assert_eq!(drain.connections, 1);
        assert_eq!(drain.panicked, 1);
        assert_eq!(drain.task_join_cancelled, 1);
        assert_eq!(drain.task_join_panicked, 0);
        assert_eq!(drain.manager_cleanup_failed, 1);
        assert_eq!(drain.registry_entries_remaining, 1);
        assert_eq!(registry.entry_count(), 1);
        assert_eq!(manager.connection_count().await, 1);
        assert_eq!(
            mutex_guard(&entry.state).worker.state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Interrupted(LifecycleInterruption::Aborted),
                started: true,
            }
        );
    }

    #[tokio::test]
    async fn absolute_deadline_cancels_pending_middleware_and_terminal_cleanup_then_joins_all() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let middleware_connection_id = Uuid::new_v4();
        let terminal_connection_id = Uuid::new_v4();

        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let middleware_started = Arc::new(Semaphore::new(0));
        let middleware_release = Arc::new(Semaphore::new(0));
        let middleware_chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![middleware(
                "deadline_cleanup",
                CloseBehavior::Wait(Arc::clone(&middleware_release)),
                &events,
                &close_calls,
                Some(Arc::clone(&middleware_started)),
            )])
            .unwrap(),
        );
        let middleware_context = context(middleware_connection_id);
        let middleware_lease = registry
            .register(
                middleware_connection_id,
                Arc::clone(&middleware_chain),
                Arc::clone(&middleware_context),
                None,
                Duration::from_secs(30),
            )
            .unwrap();
        middleware_chain
            .admit(
                middleware_context,
                &middleware_lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let (middleware_sender, _middleware_receiver) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(
                middleware_connection_id,
                middleware_sender,
                None,
                Some("test".into()),
            )
            .await
            .unwrap();
        middleware_lease
            .attach_manager(Arc::clone(&manager))
            .unwrap();

        let terminal_started = Arc::new(Semaphore::new(0));
        let terminal_release = Arc::new(Semaphore::new(0));
        let terminal_hook: ConnectionTerminalHook = {
            let terminal_started = Arc::clone(&terminal_started);
            let terminal_release = Arc::clone(&terminal_release);
            Arc::new(move |_, _| {
                let terminal_started = Arc::clone(&terminal_started);
                let terminal_release = Arc::clone(&terminal_release);
                async move {
                    terminal_started.add_permits(1);
                    terminal_release.acquire().await.unwrap().forget();
                    ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
                }
                .boxed()
            })
        };
        let terminal_lease = registry
            .register(
                terminal_connection_id,
                Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                context(terminal_connection_id),
                Some(terminal_hook),
                Duration::from_secs(30),
            )
            .unwrap();
        let (terminal_sender, _terminal_receiver) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(
                terminal_connection_id,
                terminal_sender,
                None,
                Some("test".into()),
            )
            .await
            .unwrap();
        terminal_lease.attach_manager(Arc::clone(&manager)).unwrap();

        let deadline = Instant::now() + Duration::from_millis(100);
        let started_at = Instant::now();
        let server_registry = registry.clone();
        let finalizer = tokio::spawn(async move {
            server_registry
                .finalize_all(WsConnectionCloseCategory::ServerShutdown, deadline)
                .await
        });
        timeout(Duration::from_millis(50), middleware_started.acquire())
            .await
            .expect("middleware cleanup started before deadline")
            .unwrap()
            .forget();
        timeout(Duration::from_millis(50), terminal_started.acquire())
            .await
            .expect("terminal cleanup started before deadline")
            .unwrap()
            .forget();

        let drain = timeout(Duration::from_secs(1), finalizer)
            .await
            .expect("global cleanup remained bounded")
            .unwrap();
        assert!(started_at.elapsed() < Duration::from_millis(500));
        assert!(!drain.forced);
        assert!(drain.deadline_elapsed);
        assert_eq!(drain.connections, 2);
        assert_eq!(drain.completed, 2);
        assert_eq!(drain.hooks_cancelled, 0);
        assert_eq!(drain.hooks_timed_out, 1);
        assert_eq!(drain.hooks_cancellation_observed, 0);
        assert_eq!(drain.unreconciled_hook_failures(), 1);
        assert_eq!(drain.terminal_cancelled, 0);
        assert_eq!(drain.terminal_timed_out, 1);
        assert_eq!(drain.manager_cleanup_failed, 0);
        assert_eq!(drain.task_join_cancelled, 0);
        assert_eq!(drain.task_join_panicked, 0);
        assert_eq!(drain.registry_entries_remaining, 0);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(manager.connection_count().await, 0);
        assert_eq!(registry.entry_count(), 0);
        drop(middleware_lease);
        drop(terminal_lease);
    }

    #[tokio::test]
    async fn terminal_callback_precedes_reverse_middleware_cleanup_and_manager_removal() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let observed_closing = Arc::new(AtomicBool::new(false));
        let callback_started = Arc::new(Semaphore::new(0));
        let callback_release = Arc::new(Semaphore::new(0));
        let callback: ConnectionTerminalHook = {
            let manager = Arc::clone(&manager);
            let callback_calls = Arc::clone(&callback_calls);
            let observed_closing = Arc::clone(&observed_closing);
            let callback_started = Arc::clone(&callback_started);
            let callback_release = Arc::clone(&callback_release);
            let events = Arc::clone(&events);
            Arc::new(move |category, _| {
                let manager = Arc::clone(&manager);
                let callback_calls = Arc::clone(&callback_calls);
                let observed_closing = Arc::clone(&observed_closing);
                let callback_started = Arc::clone(&callback_started);
                let callback_release = Arc::clone(&callback_release);
                let events = Arc::clone(&events);
                async move {
                    assert_eq!(category, WsConnectionCloseCategory::NormalPeer);
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                    mutex_guard(&events).push(("controller_disconnected", category));
                    let closing = manager
                        .get_connection(connection_id)
                        .await
                        .is_some_and(|connection| connection.state == ConnectionState::Closing);
                    observed_closing.store(closing, Ordering::SeqCst);
                    callback_started.add_permits(1);
                    callback_release.acquire().await.unwrap().forget();
                    ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
                }
                .boxed()
            })
        };
        let chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![middleware(
                "middleware_closed",
                CloseBehavior::Complete,
                &events,
                &close_calls,
                None,
            )])
            .unwrap(),
        );
        let context = context(connection_id);
        let lease = registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                Some(callback),
                Duration::from_secs(1),
            )
            .unwrap();
        chain
            .admit(
                context,
                &lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        lease.attach_manager(Arc::clone(&manager)).unwrap();

        let normal = tokio::spawn(lease.finalize(WsConnectionCloseCategory::NormalPeer));
        wait_for_test_permit(&callback_started, "terminal callback did not start").await;
        assert!(observed_closing.load(Ordering::SeqCst));
        assert_eq!(manager.connection_count().await, 1);
        assert_eq!(
            mutex_guard(&events)
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            ["controller_disconnected"]
        );
        let server_registry = registry.clone();
        let server = tokio::spawn(async move {
            server_registry
                .finalize_all(WsConnectionCloseCategory::ServerShutdown, test_deadline())
                .await
        });
        tokio::task::yield_now().await;
        callback_release.add_permits(1);

        let outcome = normal.await.unwrap();
        assert_eq!(outcome.terminal_report().attempted(), 1);
        assert_eq!(outcome.terminal_report().completed(), 1);
        let drain = server.await.unwrap();
        assert_eq!(drain.connections, 1);
        assert_eq!(drain.terminal_attempted, 1);
        assert_eq!(drain.terminal_completed, 1);
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            mutex_guard(&events)
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            ["controller_disconnected", "middleware_closed"]
        );
        assert_eq!(manager.connection_count().await, 0);
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn dropped_lease_and_server_finalizer_run_terminal_callback_once() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let observed_closing = Arc::new(AtomicBool::new(false));
        let callback_started = Arc::new(Semaphore::new(0));
        let callback_release = Arc::new(Semaphore::new(0));
        let callback: ConnectionTerminalHook = {
            let manager = Arc::clone(&manager);
            let callback_calls = Arc::clone(&callback_calls);
            let observed_closing = Arc::clone(&observed_closing);
            let callback_started = Arc::clone(&callback_started);
            let callback_release = Arc::clone(&callback_release);
            Arc::new(move |category, _| {
                let manager = Arc::clone(&manager);
                let callback_calls = Arc::clone(&callback_calls);
                let observed_closing = Arc::clone(&observed_closing);
                let callback_started = Arc::clone(&callback_started);
                let callback_release = Arc::clone(&callback_release);
                async move {
                    assert_eq!(category, WsConnectionCloseCategory::Cancelled);
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                    let closing = manager
                        .get_connection(connection_id)
                        .await
                        .is_some_and(|connection| connection.state == ConnectionState::Closing);
                    observed_closing.store(closing, Ordering::SeqCst);
                    callback_started.add_permits(1);
                    callback_release.acquire().await.unwrap().forget();
                    ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
                }
                .boxed()
            })
        };
        let chain = Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap());
        let context = context(connection_id);
        let lease = registry
            .register(
                connection_id,
                chain,
                context,
                Some(callback),
                Duration::from_secs(1),
            )
            .unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        lease.attach_manager(Arc::clone(&manager)).unwrap();

        drop(lease);
        wait_for_test_permit(&callback_started, "dropped lease callback did not start").await;
        assert!(observed_closing.load(Ordering::SeqCst));
        let server_registry = registry.clone();
        let server = tokio::spawn(async move {
            server_registry
                .finalize_all(WsConnectionCloseCategory::ServerShutdown, test_deadline())
                .await
        });
        tokio::task::yield_now().await;
        callback_release.add_permits(1);

        let drain = server.await.unwrap();
        assert_eq!(drain.connections, 1);
        assert_eq!(drain.terminal_attempted, 1);
        assert_eq!(drain.terminal_completed, 1);
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
        assert_eq!(manager.connection_count().await, 0);
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn terminal_callback_panic_does_not_skip_middleware_cleanup_or_manager_removal() {
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let callback: ConnectionTerminalHook = {
            let events = Arc::clone(&events);
            Arc::new(move |category, _| {
                let events = Arc::clone(&events);
                async move {
                    mutex_guard(&events).push(("controller_disconnected", category));
                    panic!("test-only terminal callback panic");
                }
                .boxed()
            })
        };
        let chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![middleware(
                "middleware_closed",
                CloseBehavior::Complete,
                &events,
                &close_calls,
                None,
            )])
            .unwrap(),
        );
        let lifecycle_context = context(connection_id);
        let lease = registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&lifecycle_context),
                Some(callback),
                Duration::from_secs(1),
            )
            .unwrap();
        chain
            .admit(
                lifecycle_context,
                &lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        lease.attach_manager(Arc::clone(&manager)).unwrap();

        let outcome = lease
            .finalize(WsConnectionCloseCategory::InternalError)
            .await;
        let terminal = outcome.terminal_report();
        assert_eq!(terminal.attempted(), 1);
        assert_eq!(terminal.failed(), 1);
        assert_eq!(terminal.panicked(), 1);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            mutex_guard(&events)
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            ["controller_disconnected", "middleware_closed"]
        );
        assert_eq!(manager.connection_count().await, 0);
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn force_times_out_pending_generated_disconnected_extraction_and_reconciles() {
        let _test_lock = PENDING_EXTRACTOR_TEST_LOCK.lock().await;
        PENDING_EXTRACTOR_STARTED.store(0, Ordering::SeqCst);
        PENDING_EXTRACTOR_DROPPED.store(0, Ordering::SeqCst);
        PENDING_EXTRACTOR_CONTROLLER_STARTED.store(0, Ordering::SeqCst);

        let registry = ConnectionCleanupRegistry::new().unwrap();
        let manager = test_manager();
        let connection_id = Uuid::new_v4();
        let context = context(connection_id);
        let container = Arc::new(ApplicationContainer::builder().build().await.unwrap());
        let extensions = container.services();
        let handler: Arc<dyn WebSocketLifecycleAction> = Arc::new(
            __LilyWebSocketOperation_PendingExtractorController_disconnected {
                controller: Arc::new(PendingExtractorController),
            },
        );
        let terminal_hook: ConnectionTerminalHook = {
            let context = Arc::clone(&context);
            Arc::new(move |_, _| {
                let context = Arc::clone(&context);
                let extensions = Arc::clone(&extensions);
                let handler = Arc::clone(&handler);
                async move {
                    let invocation = WebSocketLifecycleInvocation::disconnected(
                        extensions,
                        context,
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(30),
                        DisconnectReason::ServerShutdown,
                    );
                    match handler.call(invocation).await {
                        Ok(()) => ConnectionTerminalHookReport::new(1, 1, 0, 0, 0),
                        Err(_) => ConnectionTerminalHookReport::new(1, 0, 1, 0, 0),
                    }
                }
                .boxed()
            })
        };
        let lease = registry
            .register(
                connection_id,
                Arc::new(CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
                Arc::clone(&context),
                Some(terminal_hook),
                Duration::from_secs(30),
            )
            .unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("test".into()))
            .await
            .unwrap();
        lease.attach_manager(Arc::clone(&manager)).unwrap();

        let force = CancellationToken::new();
        force.cancel();
        let drain = timeout(
            Duration::from_secs(1),
            registry.finalize_all_with_force(WsConnectionCloseCategory::Cancelled, None, &force),
        )
        .await
        .expect("pre-cancelled cleanup did not join");

        assert_eq!(PENDING_EXTRACTOR_STARTED.load(Ordering::SeqCst), 1);
        assert_eq!(PENDING_EXTRACTOR_DROPPED.load(Ordering::SeqCst), 1);
        assert_eq!(drain.terminal_attempted, 1);
        assert_eq!(drain.terminal_timed_out, 1);
        assert_eq!(drain.terminal_cancelled, 0);
        assert_eq!(manager.connection_count().await, 0);
        assert_eq!(registry.entry_count(), 0);
        assert_eq!(
            PENDING_EXTRACTOR_CONTROLLER_STARTED.load(Ordering::SeqCst),
            0,
            "a permanently pending extractor must remain cancellable before the real #[disconnected] controller body starts"
        );
    }

    #[tokio::test]
    async fn force_runs_ready_connection_hooks_in_reverse_within_its_cleanup_budget() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let registry = ConnectionCleanupRegistry::new().unwrap();
        let chain = Arc::new(
            CompiledWsConnectionChain::compile(vec![
                middleware(
                    "outer",
                    CloseBehavior::Complete,
                    &events,
                    &close_calls,
                    None,
                ),
                middleware(
                    "inner",
                    CloseBehavior::Complete,
                    &events,
                    &close_calls,
                    None,
                ),
            ])
            .unwrap(),
        );
        let connection_id = Uuid::new_v4();
        let context = context(connection_id);
        let terminal_hook: ConnectionTerminalHook = Arc::new(move |_, _| {
            async move { ConnectionTerminalHookReport::new(1, 1, 0, 0, 0) }.boxed()
        });
        let lease = registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                Some(terminal_hook),
                Duration::from_secs(30),
            )
            .unwrap();
        chain
            .admit(
                context,
                &lease.ledger(),
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        let force = CancellationToken::new();
        force.cancel();
        let drain = timeout(
            Duration::from_secs(1),
            registry.finalize_all_with_force(WsConnectionCloseCategory::Cancelled, None, &force),
        )
        .await
        .expect("pre-cancelled cleanup did not join");

        assert_eq!(drain.hooks_attempted, 2);
        assert_eq!(drain.hooks_failed, 0);
        assert_eq!(drain.hooks_cancelled, 0);
        assert_eq!(drain.hooks_cancellation_observed, 0);
        assert_eq!(registry.entry_count(), 0);
        assert_eq!(
            mutex_guard(&events).as_slice(),
            [
                ("inner", WsConnectionCloseCategory::Cancelled),
                ("outer", WsConnectionCloseCategory::Cancelled),
            ],
            "force must preserve reverse order while cleanup authority still has budget"
        );
        assert_eq!(close_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn expired_cleanup_authority_records_not_started_without_polling_either_hook() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::new(AtomicUsize::new(0));
        let pending_release = Arc::new(Semaphore::new(0));
        let chain = CompiledWsConnectionChain::compile(vec![
            middleware(
                "outer",
                CloseBehavior::Complete,
                &events,
                &close_calls,
                None,
            ),
            middleware(
                "inner",
                CloseBehavior::Wait(pending_release),
                &events,
                &close_calls,
                None,
            ),
        ])
        .unwrap();
        let ledger = WsConnectionLedger::new();
        let context = context(Uuid::new_v4());
        chain
            .admit(
                Arc::clone(&context),
                &ledger,
                Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let report = timeout(
            Duration::from_secs(1),
            chain.cleanup(
                context,
                &ledger,
                WsConnectionCloseCategory::Cancelled,
                Duration::from_secs(30),
                &cancellation,
            ),
        )
        .await
        .expect("pre-cancelled reverse cleanup did not remain bounded");

        assert_eq!(report.attempted(), 2);
        assert_eq!(report.completed(), 0);
        assert_eq!(report.failed(), 2);
        assert_eq!(report.cancelled(), 2);
        assert_eq!(report.cancellation_observed(), 2);
        assert!(mutex_guard(&events).is_empty());
        ledger.with_state(|state| {
            for entry in &state.entries {
                assert!(matches!(
                    entry.termination.state,
                    LifecycleInvocationState::Terminal { started: false, .. }
                ));
            }
        });
        assert_eq!(close_calls.load(Ordering::SeqCst), 0);
    }
}
