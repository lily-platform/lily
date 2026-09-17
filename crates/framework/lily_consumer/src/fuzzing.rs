//! Internal qualification adapters for the consumer fuzz workspace.
//!
//! These adapters only translate owned fuzz inputs. Validation, plan building,
//! shutdown state transitions, task ownership, and terminal reporting stay in
//! the same production code used by the consumer runtime.

use std::{sync::Arc, time::Duration};

use lily_config::QueueDefinition;
use lily_queue::__private::{DeliveryGuarantee, QueuePayloadKind};
use lily_shutdown::{
    FrameworkShutdownCoordinator, FrameworkShutdownPhase, OwnedTaskSet, ShutdownAction,
    ShutdownError, ShutdownInitiation, ShutdownSignal, ShutdownState,
};
use lily_trace::runtime::TraceCellConfig;
use tokio::sync::Semaphore;

use crate::consumer::exercise_runtime_ownership_for_fuzz;
use crate::plan::{ConsumerPlan, HandlerDescriptor, ValidatedTraceCells};

const MAX_SHUTDOWN_EVENTS: usize = 64;
const MAX_SHUTDOWN_TASKS: usize = 32;
const SHUTDOWN_TASK_PERMITS: usize = 8;
const SHUTDOWN_COMPONENT_TIMEOUT: Duration = Duration::from_millis(2);
const SHUTDOWN_DEADLINE: Duration = Duration::from_millis(4);

#[derive(Debug, Clone)]
pub struct ConsumerHandlerInput {
    pub queue_name: String,
    pub service_type_name: String,
    pub component_kind: Option<String>,
    pub method_name: String,
    pub handler_name: String,
    pub schema_version: u16,
    pub content_kind: String,
    pub payload_kind: u8,
    pub payload_type_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConsumerTraceCellInput {
    pub id: String,
    pub worker_type: String,
    pub worker_name: String,
    pub kind: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerPlanHandlerSummary {
    pub handler_index: usize,
    pub component_resolved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerPlanBindingSummary {
    pub queue_index: usize,
    pub handlers: Vec<ConsumerPlanHandlerSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerPlanSummary {
    pub bindings: Vec<ConsumerPlanBindingSummary>,
}

/// Runs consumer queue validation through the canonical production plan seam.
pub fn validate_consumer_config(definitions: &[QueueDefinition]) -> Result<usize, &'static str> {
    ConsumerPlan::validate_queue_definitions(definitions)
        .map(|()| definitions.len())
        .map_err(|error| error.diagnostic_code())
}

/// Builds the exact plan consumed by `Consumer::run_initialized`.
pub fn build_consumer_plan(
    definitions: &[QueueDefinition],
    handlers: &[ConsumerHandlerInput],
    trace_cells: Vec<ConsumerTraceCellInput>,
) -> Result<ConsumerPlanSummary, &'static str> {
    let trace_cells = trace_cells
        .into_iter()
        .map(|cell| TraceCellConfig {
            id: cell.id,
            worker_type: cell.worker_type,
            worker_name: cell.worker_name,
            kind: cell.kind,
        })
        .collect();
    let trace_cells =
        ValidatedTraceCells::try_new(trace_cells).map_err(|error| error.diagnostic_code())?;
    let descriptors = handlers
        .iter()
        .map(|handler| HandlerDescriptor {
            queue_name: &handler.queue_name,
            service_type_name: &handler.service_type_name,
            component_kind: handler.component_kind.as_deref(),
            method_name: &handler.method_name,
            handler_name: &handler.handler_name,
            schema_version: handler.schema_version,
            content_kind: &handler.content_kind,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            payload_kind: match handler.payload_kind {
                0 => QueuePayloadKind::None,
                1 => QueuePayloadKind::Json,
                2 => QueuePayloadKind::Text,
                3 => QueuePayloadKind::Binary,
                4 => QueuePayloadKind::Raw,
                _ => QueuePayloadKind::Custom,
            },
            payload_type_name: handler.payload_type_name.as_deref(),
        })
        .collect::<Vec<_>>();
    let plan = ConsumerPlan::build(definitions, &descriptors, &trace_cells)
        .map_err(|error| error.diagnostic_code())?;

    Ok(ConsumerPlanSummary {
        bindings: plan
            .bindings()
            .iter()
            .map(|binding| ConsumerPlanBindingSummary {
                queue_index: binding.queue_index,
                handlers: binding
                    .handlers
                    .iter()
                    .map(|handler| ConsumerPlanHandlerSummary {
                        handler_index: handler.handler_index,
                        component_resolved: handler.component.is_some(),
                    })
                    .collect(),
            })
            .collect(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerShutdownEvent {
    SpawnTask {
        fail_after_cancellation: bool,
        ignore_cancellation: bool,
    },
    ManualStop,
    Interrupt,
    Terminate,
    Quit,
    SecondSignal,
    ProviderFailure,
    RegistrationFailure,
    Cancellation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerShutdownSummary {
    pub shutdown_initiated: bool,
    pub initial_signal: ShutdownSignal,
    pub forced: bool,
    pub report_success: bool,
    pub report_reconciles: bool,
    pub terminal_report_replayed: bool,
    pub active_jobs: usize,
    pub available_permits: usize,
    pub permit_capacity: usize,
    pub spawned_tasks: usize,
    pub component_outcomes: usize,
}

/// One bounded event applied to the real private Consumer runtime owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerRuntimeOwnershipEvent {
    /// Cancel the configured direct or managed startup trigger.
    CancelTrigger,
    /// Complete the transport-free provider runtime wait.
    CompleteProvider,
    /// Publish durable manual shutdown directly.
    ManualShutdown,
    /// Abort the outer waiter after it has adopted the supervisor handle.
    AbortWaiter,
    /// Request force escalation on the shared shutdown state.
    RequestForce,
    /// Yield once so the current ownership state can advance.
    Yield,
    /// Make provider completion return a typed failure.
    FailProvider,
    /// Make graceful queue drain return a typed failure.
    FailDrain,
    /// Make graceful queue drain panic.
    PanicDrain,
    /// Hold graceful queue drain until its real owner deadline uses force.
    PauseDrain,
}

/// Terminal, payload-free evidence from one runtime-owner event sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerRuntimeOwnershipSummary {
    /// Whether one durable shutdown transition was published.
    pub shutdown_initiated: bool,
    /// Whether the private supervisor task reached terminal state.
    pub supervisor_finished: bool,
    /// Whether every queue lifecycle phase stayed within its once-only bound.
    pub exactly_once: bool,
    /// Whether releasing cancelled gates after terminal state added no calls.
    pub no_late_calls: bool,
    /// Whether the queue runtime proved all delivery work reconciled.
    pub drain_reconciled: bool,
    /// Remaining jobs retained by the shared shutdown state.
    pub active_jobs: usize,
    /// Total bounded queue lifecycle calls observed.
    pub lifecycle_calls: usize,
}

/// Exercise cancellation/completion ordering through the actual runtime owner.
///
/// `profile % 3` selects direct cancellation, retained managed readiness, or
/// dropped managed readiness. At most 32 events are interpreted. The caller
/// should still wrap this async adapter in an independent outer deadline.
pub async fn exercise_consumer_runtime_ownership(
    profile: u8,
    events: &[ConsumerRuntimeOwnershipEvent],
) -> Result<ConsumerRuntimeOwnershipSummary, &'static str> {
    if events.len() > 32 {
        return Err("CONSUMER_RUNTIME_OWNERSHIP_EVENT_LIMIT");
    }
    let events = events
        .iter()
        .map(|event| match event {
            ConsumerRuntimeOwnershipEvent::CancelTrigger => 0,
            ConsumerRuntimeOwnershipEvent::CompleteProvider => 1,
            ConsumerRuntimeOwnershipEvent::ManualShutdown => 2,
            ConsumerRuntimeOwnershipEvent::AbortWaiter => 3,
            ConsumerRuntimeOwnershipEvent::RequestForce => 4,
            ConsumerRuntimeOwnershipEvent::Yield => 5,
            ConsumerRuntimeOwnershipEvent::FailProvider => 6,
            ConsumerRuntimeOwnershipEvent::FailDrain => 7,
            ConsumerRuntimeOwnershipEvent::PanicDrain => 8,
            ConsumerRuntimeOwnershipEvent::PauseDrain => 9,
        })
        .collect::<Vec<_>>();
    let evidence = exercise_runtime_ownership_for_fuzz(profile, &events).await;
    Ok(ConsumerRuntimeOwnershipSummary {
        shutdown_initiated: evidence.shutdown_initiated,
        supervisor_finished: evidence.supervisor_finished,
        exactly_once: evidence.exactly_once,
        no_late_calls: evidence.no_late_calls,
        drain_reconciled: evidence.drain_reconciled,
        active_jobs: evidence.active_jobs,
        lifecycle_calls: evidence.lifecycle_calls,
    })
}

/// Exercises the actual framework shutdown coordinator and owned-task type.
/// The event and task limits apply before any task allocation or spawning.
pub async fn exercise_consumer_shutdown(
    events: &[ConsumerShutdownEvent],
) -> Result<ConsumerShutdownSummary, &'static str> {
    if events.len() > MAX_SHUTDOWN_EVENTS {
        return Err("CONSUMER_SHUTDOWN_EVENT_LIMIT");
    }

    let state = Arc::new(ShutdownState::new());
    let permits = Arc::new(Semaphore::new(SHUTDOWN_TASK_PERMITS));
    let mut tasks = OwnedTaskSet::new(
        "consumer-fuzz-tasks",
        FrameworkShutdownPhase::DrainInFlight,
        SHUTDOWN_COMPONENT_TIMEOUT,
    );
    let cancellation = tasks.cancellation_token();
    let mut spawned_tasks = 0usize;
    let mut provider_failure = false;
    let mut registration_failure = false;

    for event in events {
        match *event {
            ConsumerShutdownEvent::SpawnTask {
                fail_after_cancellation,
                ignore_cancellation,
            } if spawned_tasks < MAX_SHUTDOWN_TASKS => {
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    continue;
                };
                let Ok(job) = state.job_guard() else {
                    continue;
                };
                let task_cancellation = cancellation.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let _job = job;
                    if ignore_cancellation {
                        std::future::pending::<()>().await;
                    } else {
                        task_cancellation.cancelled().await;
                    }
                    if fail_after_cancellation {
                        Err(ShutdownError::Component(
                            "consumer fuzz task failure".to_string(),
                        ))
                    } else {
                        Ok(())
                    }
                });
                spawned_tasks += 1;
            }
            ConsumerShutdownEvent::SpawnTask { .. } => {}
            ConsumerShutdownEvent::ManualStop => initiate_manual(&state),
            ConsumerShutdownEvent::Interrupt => apply_os_signal(&state, ShutdownSignal::Interrupt),
            ConsumerShutdownEvent::Terminate => apply_os_signal(&state, ShutdownSignal::Terminate),
            ConsumerShutdownEvent::Quit => apply_os_signal(&state, ShutdownSignal::Quit),
            ConsumerShutdownEvent::SecondSignal => {
                if !state.is_shutdown_initiated() {
                    let _ = state.initiate_shutdown(ShutdownSignal::Interrupt);
                }
                state.request_force();
            }
            ConsumerShutdownEvent::ProviderFailure => {
                provider_failure = true;
                initiate_manual(&state);
            }
            ConsumerShutdownEvent::RegistrationFailure => {
                registration_failure = true;
                initiate_manual(&state);
            }
            ConsumerShutdownEvent::Cancellation => cancellation.cancel(),
        }
    }

    let signal = state.initial_signal().unwrap_or(ShutdownSignal::Manual);
    let mut coordinator = FrameworkShutdownCoordinator::new(Arc::clone(&state), SHUTDOWN_DEADLINE);
    coordinator.register(tasks);
    if provider_failure {
        coordinator.register(failing_action(
            "consumer-provider-failure",
            "provider failure",
        ));
    }
    if registration_failure {
        coordinator.register(failing_action(
            "consumer-registration-failure",
            "registration failure",
        ));
    }

    let report = coordinator.execute_report(signal).await;
    let replay = coordinator.execute_report(ShutdownSignal::Quit).await;
    let terminal_report_replayed = replay == report;

    Ok(ConsumerShutdownSummary {
        shutdown_initiated: state.is_shutdown_initiated(),
        initial_signal: state.initial_signal().unwrap_or(ShutdownSignal::Manual),
        forced: report.forced,
        report_success: report.is_terminal_complete(),
        report_reconciles: report.reconciles(),
        terminal_report_replayed,
        active_jobs: state.active_job_count(),
        available_permits: permits.available_permits(),
        permit_capacity: SHUTDOWN_TASK_PERMITS,
        spawned_tasks,
        component_outcomes: report.component_count(),
    })
}

fn apply_os_signal(state: &Arc<ShutdownState>, signal: ShutdownSignal) {
    if matches!(
        state.initiate_shutdown(signal),
        Ok(ShutdownInitiation::AlreadyInitiated { .. })
    ) {
        state.request_force();
    }
}

fn initiate_manual(state: &Arc<ShutdownState>) {
    let _ = state.initiate_shutdown(ShutdownSignal::Manual);
}

fn failing_action(
    name: &'static str,
    message: &'static str,
) -> ShutdownAction<impl FnOnce() -> std::future::Ready<Result<(), ShutdownError>> + Send> {
    ShutdownAction::new(
        name,
        FrameworkShutdownPhase::DisposeDependencies,
        SHUTDOWN_COMPONENT_TIMEOUT,
        move || std::future::ready(Err(ShutdownError::Component(message.to_string()))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_facade_reconciles_tasks_permits_and_terminal_replay() {
        let summary = exercise_consumer_shutdown(&[
            ConsumerShutdownEvent::SpawnTask {
                fail_after_cancellation: false,
                ignore_cancellation: false,
            },
            ConsumerShutdownEvent::Interrupt,
            ConsumerShutdownEvent::SecondSignal,
        ])
        .await
        .unwrap();

        assert!(summary.shutdown_initiated);
        assert!(summary.forced);
        assert!(summary.report_reconciles);
        assert!(summary.terminal_report_replayed);
        assert_eq!(summary.active_jobs, 0);
        assert_eq!(summary.available_permits, summary.permit_capacity);
    }
}
