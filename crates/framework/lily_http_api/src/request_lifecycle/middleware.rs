//! Retained around-invocation obligations. No cleanup is run by stack Drop.

use super::*;
use crate::lifecycle::MiddlewareCleanupEvidence;
use lily_middleware::{
    HttpMiddleware, HttpMiddlewareStage, HttpRequestInterruption, HttpRequestTerminationContext,
    HttpRequestTerminationMetadata, MiddlewareDescriptor,
};
use lily_web_core::{__private::HttpResources, RequestExtensions};
use std::sync::atomic::{AtomicBool, Ordering};

// Internal policy, always clipped to the existing owner/root cutoff. The last
// 25ms permits cooperative cleanup after notification, not a fresh timeout.
pub(crate) const TERMINATION_HOOK_CAP: Duration = Duration::from_millis(250);
const CLEANUP_NOTICE: Duration = Duration::from_millis(25);

tokio::task_local! {
    static CURRENT: MiddlewareLedger;
}

struct InvocationEvidence {
    entered: bool,
    stage: HttpMiddlewareStage,
    cleanup: MiddlewareCleanupEvidence,
    termination_started: bool,
    state_released: bool,
    release_failed: bool,
    error_code: Option<lily_middleware::MiddlewareErrorCode>,
}

pub(crate) struct Invocation {
    id: usize,
    middleware: Arc<dyn HttpMiddleware>,
    descriptor: MiddlewareDescriptor,
    evidence: Mutex<InvocationEvidence>,
    pub(crate) state: tokio::sync::Mutex<RequestExtensions>,
    children: HttpResources,
    cleanup_cutoff: OnceLock<Instant>,
}

impl Invocation {
    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn enter(&self) {
        lock(&self.evidence).entered = true;
    }

    pub(crate) fn stage(&self, stage: HttpMiddlewareStage) {
        lock(&self.evidence).stage = stage;
    }

    pub(crate) fn normal_returned(&self) {
        lock(&self.evidence).cleanup = MiddlewareCleanupEvidence::NormalReturned;
    }

    fn settle(&self, outcome: CleanupOutcome) {
        let (stage, started, error_code) = {
            let mut evidence = lock(&self.evidence);
            evidence.cleanup = MiddlewareCleanupEvidence::TerminationSettled(outcome);
            (
                evidence.stage,
                evidence.termination_started,
                evidence.error_code,
            )
        };
        tracing::debug!(
            lily.event = "http.middleware.termination",
            middleware = self.descriptor.name(),
            invocation = self.id,
            stage = ?stage,
            started,
            outcome = ?outcome,
            error_code = error_code.map(|code| code.as_str()),
            "HTTP middleware termination disposition"
        );
    }

    async fn release_state(&self) -> bool {
        let mut state = self.state.lock().await;
        if lock(&self.evidence).state_released {
            return true;
        }
        if lock(&self.evidence).release_failed {
            return false;
        }
        let value = std::mem::take(&mut *state);
        let released = catch_unwind(AssertUnwindSafe(|| drop(value))).is_ok();
        let mut evidence = lock(&self.evidence);
        evidence.state_released = released;
        evidence.release_failed |= !released;
        released
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MiddlewareSnapshot {
    pub(crate) registered: usize,
    pub(crate) entered: usize,
    pub(crate) normal_returned: usize,
    pub(crate) before_interrupted: usize,
    pub(crate) delegation_interrupted: usize,
    pub(crate) normal_exit_interrupted: usize,
    pub(crate) unknown_stage_interrupted: usize,
    pub(crate) termination_started: usize,
    pub(crate) completed: usize,
    pub(crate) failed: usize,
    pub(crate) timed_out: usize,
    pub(crate) panicked: usize,
    pub(crate) not_started: usize,
    pub(crate) outstanding: usize,
    pub(crate) resources_outstanding: usize,
    pub(crate) helpers_registered: usize,
    pub(crate) helpers_joined: usize,
    pub(crate) helpers_failed: usize,
    pub(crate) helpers_abort_requested: usize,
    pub(crate) helpers_cancelled: usize,
}

impl MiddlewareSnapshot {
    pub(crate) fn reconciles(self) -> bool {
        self.entered <= self.registered
            && self.entered
                == self.normal_returned
                    + self.completed
                    + self.failed
                    + self.timed_out
                    + self.panicked
                    + self.not_started
                    + self.outstanding
            && self.helpers_joined <= self.helpers_registered
            && self.helpers_cancelled + self.helpers_failed <= self.helpers_joined
    }

    pub(crate) fn is_terminal(self) -> bool {
        self.reconciles() && self.outstanding == 0 && self.resources_outstanding == 0
    }

    pub(crate) fn include(&mut self, other: Self) {
        self.registered += other.registered;
        self.entered += other.entered;
        self.normal_returned += other.normal_returned;
        self.before_interrupted += other.before_interrupted;
        self.delegation_interrupted += other.delegation_interrupted;
        self.normal_exit_interrupted += other.normal_exit_interrupted;
        self.unknown_stage_interrupted += other.unknown_stage_interrupted;
        self.termination_started += other.termination_started;
        self.completed += other.completed;
        self.failed += other.failed;
        self.timed_out += other.timed_out;
        self.panicked += other.panicked;
        self.not_started += other.not_started;
        self.outstanding += other.outstanding;
        self.resources_outstanding += other.resources_outstanding;
        self.helpers_registered += other.helpers_registered;
        self.helpers_joined += other.helpers_joined;
        self.helpers_failed += other.helpers_failed;
        self.helpers_abort_requested += other.helpers_abort_requested;
        self.helpers_cancelled += other.helpers_cancelled;
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Finalization {
    pub(crate) safe_to_close: bool,
    pub(crate) failed: bool,
}

type Finalizer = Shared<BoxFuture<'static, Finalization>>;

#[derive(Default)]
struct LedgerState {
    entries: Mutex<Vec<Arc<Invocation>>>,
    sealed: AtomicBool,
    metadata: OnceLock<HttpRequestTerminationMetadata>,
    finalizer: Mutex<Option<Finalizer>>,
}

#[derive(Default, Clone)]
pub(crate) struct MiddlewareLedger(Arc<LedgerState>);

impl MiddlewareLedger {
    pub(crate) fn initialize(&self, id: u64, request: &Request) {
        let _ = self.0.metadata.set(HttpRequestTerminationMetadata::new(
            id,
            request.method(),
            request.path(),
        ));
    }

    pub(crate) async fn scope<F: Future>(&self, future: F) -> F::Output {
        CURRENT.scope(self.clone(), future).await
    }

    /// Registration is not first poll. A dropped/unpolled callback cannot arm
    /// cleanup, and clearing Request.local cannot change this ledger.
    pub(crate) fn register_current(
        middleware: Arc<dyn HttpMiddleware>,
        descriptor: MiddlewareDescriptor,
    ) -> Option<Arc<Invocation>> {
        CURRENT
            .try_with(|ledger| {
                assert!(
                    !ledger.0.sealed.load(Ordering::Acquire),
                    "middleware entered after execution terminal"
                );
                let mut entries = lock(&ledger.0.entries);
                let entry = Arc::new(Invocation {
                    id: entries.len(),
                    middleware,
                    descriptor,
                    evidence: Mutex::new(InvocationEvidence {
                        entered: false,
                        stage: HttpMiddlewareStage::Before,
                        cleanup: MiddlewareCleanupEvidence::Outstanding,
                        termination_started: false,
                        state_released: false,
                        release_failed: false,
                        error_code: None,
                    }),
                    state: tokio::sync::Mutex::new(RequestExtensions::new()),
                    children: HttpResources::default(),
                    cleanup_cutoff: OnceLock::new(),
                });
                entries.push(entry.clone());
                entry
            })
            .ok()
    }

    pub(crate) fn snapshot(&self) -> MiddlewareSnapshot {
        let entries = lock(&self.0.entries);
        let mut result = MiddlewareSnapshot {
            registered: entries.len(),
            ..Default::default()
        };
        for entry in entries.iter() {
            let evidence = lock(&entry.evidence);
            result.entered += usize::from(evidence.entered);
            result.termination_started += usize::from(evidence.termination_started);
            // Finalization seals only after execution/source termination. A
            // still-running normal-after future is pending, not interrupted.
            if self.0.sealed.load(Ordering::Acquire)
                && evidence.entered
                && evidence.cleanup != MiddlewareCleanupEvidence::NormalReturned
            {
                match evidence.stage {
                    HttpMiddlewareStage::Before => result.before_interrupted += 1,
                    HttpMiddlewareStage::DelegatingToNext => result.delegation_interrupted += 1,
                    HttpMiddlewareStage::After => result.normal_exit_interrupted += 1,
                    _ => result.unknown_stage_interrupted += 1,
                }
            }
            if evidence.entered {
                match evidence.cleanup {
                    MiddlewareCleanupEvidence::Outstanding => result.outstanding += 1,
                    MiddlewareCleanupEvidence::NormalReturned => result.normal_returned += 1,
                    MiddlewareCleanupEvidence::TerminationSettled(CleanupOutcome::Succeeded) => {
                        result.completed += 1
                    }
                    MiddlewareCleanupEvidence::TerminationSettled(CleanupOutcome::TimedOut) => {
                        result.timed_out += 1
                    }
                    MiddlewareCleanupEvidence::TerminationSettled(CleanupOutcome::Panicked) => {
                        result.panicked += 1
                    }
                    MiddlewareCleanupEvidence::TerminationSettled(CleanupOutcome::NotStarted) => {
                        result.not_started += 1
                    }
                    MiddlewareCleanupEvidence::TerminationSettled(_) => result.failed += 1,
                }
            }
            let children = entry.children.snapshot();
            result.resources_outstanding += usize::from(
                !evidence.state_released || evidence.release_failed || !children.terminal(),
            );
            result.helpers_registered += children.helpers_registered;
            result.helpers_joined += children.helpers_joined;
            result.helpers_failed += children.helpers_failed;
            result.helpers_abort_requested += children.helpers_abort_requested;
            result.helpers_cancelled += children.helpers_cancelled;
        }
        result
    }

    /// A retained, serialized finalizer, not a spawned cleanup task. Losing a
    /// waiter cannot drop its current callback or restart an invocation. The
    /// captured entries do not point back to this ledger (no Arc cycle).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn finalize(
        &self,
        context: ProcessContext,
        extensions: Arc<lily_injection::Extensions>,
        reason: HttpRequestInterruption,
        budget: ShutdownBudget,
        local: Instant,
        execution_children: HttpResources,
        stop: bool,
    ) -> Finalization {
        let receipt = {
            let mut receipt = lock(&self.0.finalizer);
            // A completed prerequisite wait may be reconciled after late
            // resource release. Keep all per-invocation dispositions; this
            // resumes barriers and never invokes an already-settled hook.
            if receipt
                .as_ref()
                .and_then(Shared::peek)
                .is_some_and(|result| !result.safe_to_close)
            {
                *receipt = None;
            }
            receipt
                .get_or_insert_with(|| {
                    self.0.sealed.store(true, Ordering::Release);
                    let entries = lock(&self.0.entries).clone();
                    // The immutable snapshot is small and independent of Request and
                    // the finalizer's owning ledger.
                    let metadata = self
                        .0
                        .metadata
                        .get()
                        .map(|metadata| {
                            HttpRequestTerminationMetadata::new(
                                metadata.request_id(),
                                metadata.method(),
                                metadata.path(),
                            )
                        })
                        .unwrap_or_else(|| {
                            HttpRequestTerminationMetadata::new(context.process_id, "", "")
                        });
                    ProcessContext::scope(context, async move {
                        finalize(
                            entries,
                            metadata,
                            extensions,
                            reason,
                            budget,
                            local,
                            execution_children,
                            stop,
                        )
                        .await
                    })
                    .instrument(tracing::Span::current())
                    .boxed()
                    .shared()
                })
                .clone()
        };
        receipt.await
    }
}

async fn children_terminal(
    children: &HttpResources,
    budget: &ShutdownBudget,
    local: Instant,
) -> Option<bool> {
    tokio::select! {
        biased;
        snapshot = children.wait() => snapshot.terminal().then_some(snapshot.helpers_failed != 0),
        _ = budget.wait_until(ShutdownStage::Reconcile, Some(local)) => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn finalize(
    entries: Vec<Arc<Invocation>>,
    metadata: HttpRequestTerminationMetadata,
    extensions: Arc<lily_injection::Extensions>,
    reason: HttpRequestInterruption,
    budget: ShutdownBudget,
    local: Instant,
    execution_children: HttpResources,
    stop: bool,
) -> Finalization {
    // Retained invocation state is cleanup data, never an execution input or
    // response producer. Preserve it through the child-resource barrier and
    // release it in reverse order, including normally returned outer frames
    // whose application code abandoned an inner `next`.
    execution_children.seal(stop);
    let Some(mut failed) = children_terminal(&execution_children, &budget, local).await else {
        return Finalization {
            safe_to_close: false,
            failed: true,
        };
    };
    for entry in entries.iter().rev() {
        let (entered, cleanup) = {
            let evidence = lock(&entry.evidence);
            (evidence.entered, evidence.cleanup)
        };
        if !entered || cleanup == MiddlewareCleanupEvidence::NormalReturned {
            if !entry.release_state().await {
                return Finalization {
                    safe_to_close: false,
                    failed: true,
                };
            }
            continue;
        }
        let cap = *entry
            .cleanup_cutoff
            .get_or_init(|| (Instant::now() + TERMINATION_HOOK_CAP).min(local));
        let cutoff = effective_cutoff(&budget, cap);
        let (outcome, released) = match cleanup {
            MiddlewareCleanupEvidence::TerminationSettled(outcome) => {
                (outcome, !lock(&entry.evidence).release_failed)
            }
            _ if Instant::now() >= cutoff => {
                entry.settle(CleanupOutcome::NotStarted);
                (CleanupOutcome::NotStarted, true)
            }
            _ => {
                let result = invoke(entry, &metadata, &extensions, reason, &budget, cap).await;
                entry.settle(result.0);
                result
            }
        };
        failed |= outcome != CleanupOutcome::Succeeded;
        if !released {
            return Finalization {
                safe_to_close: false,
                failed: true,
            };
        }
        // Cleanup can itself call framework file helpers. Keep their actual
        // joins separate from the sealed execution inventory and from peers.
        entry.children.seal(true);
        match children_terminal(&entry.children, &budget, cap).await {
            Some(helper_failed) => failed |= helper_failed,
            None => {
                return Finalization {
                    safe_to_close: false,
                    failed: true,
                }
            }
        }
        if !entry.release_state().await {
            return Finalization {
                safe_to_close: false,
                failed: true,
            };
        }
    }
    Finalization {
        safe_to_close: true,
        failed,
    }
}

fn effective_cutoff(budget: &ShutdownBudget, local: Instant) -> Instant {
    budget
        .deadlines()
        .map_or(local, |root| local.min(root.at(ShutdownStage::Cleanup)))
}

async fn invoke(
    entry: &Invocation,
    metadata: &HttpRequestTerminationMetadata,
    extensions: &Arc<lily_injection::Extensions>,
    reason: HttpRequestInterruption,
    budget: &ShutdownBudget,
    local: Instant,
) -> (CleanupOutcome, bool) {
    let source =
        lily_web_core::__private::CleanupCancellationSource::new(effective_cutoff(budget, local));
    let stage = lock(&entry.evidence).stage;
    let mut state = entry.state.lock().await;
    let mut context = HttpRequestTerminationContext::new(
        metadata,
        entry.id,
        stage,
        reason,
        &mut state,
        extensions,
        source.view(),
    );
    let mut slot = Box::pin(entry.children.scope(async {
        lock(&entry.evidence).termination_started = true;
        entry
            .middleware
            .on_request_termination(&mut context, source.view())
            .await
    }));
    let outcome = {
        let invocation = AssertUnwindSafe(futures::future::poll_fn(|cx| {
            source.cap_deadline(effective_cutoff(budget, local));
            slot.as_mut().poll(cx)
        }))
        .catch_unwind();
        tokio::pin!(invocation);
        let control = async {
            budget
                .wait_until_with_reserve(ShutdownStage::Cleanup, Some(local), CLEANUP_NOTICE)
                .await;
            source.cap_deadline(effective_cutoff(budget, local));
            source.cancel();
            budget.wait_until(ShutdownStage::Cleanup, Some(local)).await;
        };
        tokio::pin!(control);
        tokio::select! {
            biased;
            _ = &mut control => {
                if lock(&entry.evidence).termination_started { CleanupOutcome::TimedOut }
                else { CleanupOutcome::NotStarted }
            },
            result = &mut invocation => match result {
                Ok(Ok(())) => CleanupOutcome::Succeeded,
                Ok(Err(error)) => {
                    lock(&entry.evidence).error_code = Some(error.diagnostic_code());
                    CleanupOutcome::Failed
                },
                Err(_) => CleanupOutcome::Panicked,
            },
        }
    };
    let released = catch_unwind(AssertUnwindSafe(|| drop(slot))).is_ok();
    if !released {
        lock(&entry.evidence).release_failed = true;
    }
    (
        if released {
            outcome
        } else {
            CleanupOutcome::Panicked
        },
        released,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct Hook {
        calls: AtomicUsize,
        drops: Arc<AtomicUsize>,
        view: Mutex<Option<lily_web_core::CleanupCancellation>>,
    }
    struct Capture(Arc<AtomicUsize>);
    impl Drop for Capture {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[async_trait::async_trait]
    impl HttpMiddleware for Hook {
        async fn new(
            _: Arc<lily_injection::Extensions>,
        ) -> Result<Self, lily_middleware::HttpMiddlewareInitError> {
            unreachable!()
        }
        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "retained_finalizer_probe",
                lily_middleware::MiddlewareKind::Custom,
            )
        }
        async fn handle(
            &self,
            _: &mut lily_middleware::HttpExchange<'_>,
            _: lily_middleware::HttpNext<'_>,
            _: lily_cancellation::ExecutionCancellation,
        ) -> Result<(), lily_middleware::HttpMiddlewareError> {
            unreachable!()
        }
        async fn on_request_termination(
            &self,
            context: &mut HttpRequestTerminationContext<'_>,
            signal: lily_web_core::CleanupCancellation,
        ) -> Result<(), lily_middleware::HttpMiddlewareError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let _capture = Capture(self.drops.clone());
            *lock(&self.view) = Some(signal.clone());
            signal.cancelled().await;
            assert_eq!(context.cancellation().deadline(), signal.deadline());
            assert!(context.cancellation().is_cancelled());
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn finalizer_waiter_drop_retains_the_same_hook_and_adopts_a_later_root_deadline() {
        let app = crate::AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let ledger = MiddlewareLedger::default();
        let hook = Arc::new(Hook {
            calls: AtomicUsize::new(0),
            drops: Arc::new(AtomicUsize::new(0)),
            view: Mutex::new(None),
        });
        ledger
            .scope(async {
                let invocation =
                    MiddlewareLedger::register_current(hook.clone(), hook.descriptor()).unwrap();
                invocation.enter();
            })
            .await;
        let context = ProcessContext::new();
        let local = Instant::now() + Duration::from_secs(2);
        let budget = ShutdownBudget::new(Duration::from_millis(100));
        let mut waiter = Box::pin(ledger.finalize(
            context.clone(),
            app.extensions(),
            HttpRequestInterruption::ForcedShutdown,
            budget.clone(),
            local,
            HttpResources::default(),
            true,
        ));
        assert!(waiter.as_mut().now_or_never().is_none());
        drop(waiter);
        assert_eq!(hook.calls.load(Ordering::Acquire), 1);
        assert_eq!(hook.drops.load(Ordering::Acquire), 0);
        let root = budget.begin();
        let result = ledger
            .finalize(
                context.clone(),
                app.extensions(),
                HttpRequestInterruption::ForcedShutdown,
                budget.clone(),
                local,
                HttpResources::default(),
                true,
            )
            .await;
        assert!(result.safe_to_close);
        assert!(!result.failed);
        assert!(Instant::now() < root.at(ShutdownStage::Cleanup));
        assert_eq!(
            lock(&hook.view).as_ref().unwrap().deadline(),
            root.at(ShutdownStage::Cleanup)
        );
        let _ = ledger
            .finalize(
                context,
                app.extensions(),
                HttpRequestInterruption::ForcedShutdown,
                budget,
                local,
                HttpResources::default(),
                true,
            )
            .await;
        assert_eq!(hook.calls.load(Ordering::Acquire), 1);
        assert_eq!(hook.drops.load(Ordering::Acquire), 1);
        app.close().await.unwrap();
    }
}
