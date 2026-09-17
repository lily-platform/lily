//! One deadline/cancellation driver for the complete normal delivery pipeline.

use crate::{
    DeliveryCancellationReason,
    cancellation::DeliveryCancellationSource,
    delivery_lifecycle::{DeliveryExecutionExit, DeliveryExecutionSlot},
    shutdown_budget::{DeliveryExecutionBudget, QueueShutdownBudget},
};
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::time::Instant;

/// Control-flow evidence, never inferred from an application error code.
#[derive(Clone, Default)]
pub(crate) struct FrameworkStopReceipt(Arc<AtomicBool>);

impl FrameworkStopReceipt {
    pub(crate) fn begin_attempt(&self) {
        self.0.store(false, Ordering::Release);
    }

    pub(crate) fn interrupted(&self, reason: DeliveryCancellationReason) {
        if reason != DeliveryCancellationReason::DeliveryTimeout {
            self.0.store(true, Ordering::Release);
        }
    }

    pub(crate) fn classify_error(
        &self,
        error: lily_error::application::QueueHandlerError,
    ) -> crate::queue_engine_trait::QueueExecutionError {
        if self.0.load(Ordering::Acquire) {
            // A surrounding transaction may report its own finalization error.
            // Keep that diagnostic; only the stop receipt selects control flow.
            crate::queue_engine_trait::QueueExecutionError::framework_cancelled(error.code())
        } else {
            error.into()
        }
    }
}

pub(crate) async fn run_cooperative<F: Future>(
    slot: DeliveryExecutionSlot,
    future: F,
    cancellation: &DeliveryCancellationSource,
    local: DeliveryExecutionBudget,
    root: &QueueShutdownBudget,
) -> DeliveryExecutionExit<F::Output> {
    let receipt = slot.receipt();
    let mut execution = Box::pin(slot.run(future));
    loop {
        let revision = root.revision();
        let root_deadlines = root.deadlines();
        let notification =
            root_deadlines.map_or(local.pipeline, |root| local.pipeline.min(root.graceful()));
        if Instant::now() >= notification && !cancellation.is_cancelled() {
            let reason = if root_deadlines.is_some_and(|root| root.graceful() <= local.pipeline) {
                DeliveryCancellationReason::ShutdownDeadline
            } else {
                DeliveryCancellationReason::DeliveryTimeout
            };
            cancellation.cancel_at(reason, notification);
        }
        let request = cancellation.reason().zip(cancellation.requested_at());
        let deadline = if let Some((reason, at)) = request {
            let hard = if reason == DeliveryCancellationReason::DeliveryTimeout {
                local.hard
            } else {
                local.hard.min(root.forced_delivery_deadline(at))
            };
            root.cooperative_deadline(at, hard)
        } else {
            notification
        };

        if request.is_some() && Instant::now() >= deadline {
            drop(execution);
            return if receipt.terminal() {
                DeliveryExecutionExit::Interrupted(request.expect("request present").0)
            } else {
                DeliveryExecutionExit::Panicked
            };
        }
        tokio::select! {
            biased;
            // A complete pipeline keeps its actual result, including ordinary
            // application errors. Cancellation is a notification, not a result.
            result = execution.as_mut() => return result,
            _ = cancellation.cancelled(), if request.is_none() => {},
            _ = tokio::time::sleep_until(deadline) => {},
            _ = root.changed_since(revision) => {},
        }
    }
}
