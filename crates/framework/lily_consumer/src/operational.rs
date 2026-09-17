use std::sync::Mutex;

use lily_error::application::consumer::ConsumerError;
use lily_queue::{ConsumerRuntimeState, DeliveryTerminalSnapshot};
use serde::Serialize;

/// Aggregate lifecycle state of one managed Consumer runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerLifecycleState {
    /// Runtime construction has started but queue admission is not ready.
    Starting,
    /// Every registered physical queue currently accepts deliveries.
    Ready,
    /// At least one queue is restoring a lost broker delivery stream.
    Recovering,
    /// New delivery admission is closed and accepted work is draining.
    Draining,
    /// The complete managed lifecycle stopped normally.
    Stopped,
    /// The managed lifecycle terminated with an operational failure.
    Failed,
}

/// Coarse, transport-safe RabbitMQ connectivity state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerBrokerState {
    /// Broker resources are being constructed.
    Connecting,
    /// Every registered queue currently owns a ready delivery stream.
    Connected,
    /// At least one delivery stream is reconnecting.
    Recovering,
    /// Broker admission has closed and resources are being drained or closed.
    Closing,
    /// Broker resources have reached a normal terminal state.
    Closed,
    /// Broker supervision reached a failed terminal state.
    Failed,
}

/// Aggregate state of the topology required by this Consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerTopologyState {
    /// Topology validation or broker verification has not completed.
    Pending,
    /// Every selected queue completed its configured topology contract.
    Verified,
    /// A queue runtime is restoring its broker delivery path.
    Recovering,
    /// The topology is retained while the Consumer drains accepted work.
    Draining,
    /// Topology verification or use failed terminally.
    Failed,
    /// The Consumer using this topology has stopped.
    Closed,
}

/// Aggregate new-delivery admission state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerAdmissionState {
    /// No selected queue currently accepts new deliveries.
    Closed,
    /// Every selected queue accepts new deliveries.
    Open,
    /// Some selected queues accept deliveries while another recovers.
    PartiallyOpen,
    /// Admission is closed and previously accepted deliveries are draining.
    Draining,
}

/// Shutdown facts for one managed Consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConsumerShutdownSnapshot {
    /// Whether graceful or forced shutdown has been requested.
    pub requested: bool,
    /// Whether explicit force was requested or the final coordinator report
    /// records force. Deadline-driven force is authoritative in the final report.
    pub forced: bool,
    /// Whether the actual managed runtime task joined. This can be true after
    /// failure with unconfirmed resource cleanup; inspect `report` separately.
    pub completed: bool,
    /// Whether the queue runtime proved every framework-owned delivery task was joined.
    pub drain_reconciled: bool,
    /// Original final evidence, available after runtime join when the
    /// coordinator produced a report. Missing evidence does not imply success.
    pub report: Option<crate::ConsumerShutdownReport>,
}

/// Immutable, payload-free operational view of one [`crate::ManagedConsumer`].
///
/// The snapshot intentionally contains no message body, event identifier,
/// routing key, broker endpoint or credential. Applications may expose it
/// through their own health endpoint, but Lily does not create an HTTP route.
/// Its lock-free runtime counters are sampled as one best-effort point-in-time
/// operational view rather than a transactional accounting barrier; lifecycle
/// precedence prevents contradictory ready/terminal classifications.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConsumerOperationalSnapshot {
    /// Aggregate Consumer lifecycle.
    pub lifecycle: ConsumerLifecycleState,
    /// Process-liveness opinion. Broker recovery does not make the process dead.
    pub live: bool,
    /// Traffic-readiness opinion derived from queue and shutdown admission.
    pub ready: bool,
    /// Aggregate RabbitMQ connectivity state.
    pub broker: ConsumerBrokerState,
    /// Aggregate topology contract state.
    pub topology: ConsumerTopologyState,
    /// Aggregate new-delivery admission state.
    pub admission: ConsumerAdmissionState,
    /// Physical queue consumers registered with the runtime.
    pub registered_queues: u64,
    /// Version/content handlers materialized in the local dispatch tables.
    pub registered_handlers: usize,
    /// Physical queue consumers currently ready.
    pub ready_queues: u64,
    /// Sampling-independent delivery, retry and dead-letter counters.
    pub deliveries: DeliveryTerminalSnapshot,
    /// Sampling-independent transactional inbox execution evidence.
    ///
    /// This aggregate contains only bounded counters and one stable failure
    /// code; event, handler, database-cell and credential identities are never
    /// retained.
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub transactional_inbox: lily_queue::TransactionalInboxSnapshot,
    /// Transactional outbox relay state and bounded counters.
    ///
    /// This field exists only when a transactional inbox backend feature is
    /// compiled. Relay readiness participates in the aggregate `ready`
    /// opinion; payloads, event IDs, database cells and broker destinations are
    /// deliberately absent.
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub transactional_outbox: lily_queue::TransactionalOutboxRelaySnapshot,
    /// Managed shutdown facts.
    pub shutdown: ConsumerShutdownSnapshot,
    /// Most recent stable operational failure code. Recovery intentionally
    /// retains this historical evidence; `ready`, not this field's presence,
    /// is the authority for current admission. A terminal Consumer failure
    /// takes precedence over retained recovery evidence.
    pub last_failure_code: Option<&'static str>,
}

#[derive(Debug, Default)]
pub(crate) struct ConsumerOperationalState {
    terminal: Mutex<ConsumerTerminalState>,
}

#[derive(Debug, Default, Clone, Copy)]
struct ConsumerTerminalState {
    completed: bool,
    failure_code: Option<&'static str>,
}

impl ConsumerOperationalState {
    pub(crate) fn record_terminal(&self, result: &Result<(), ConsumerError>) {
        let mut terminal = self
            .terminal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        terminal.completed = true;
        terminal.failure_code = result.as_ref().err().map(ConsumerError::error_code);
    }

    pub(crate) fn terminal(&self) -> (bool, Option<&'static str>) {
        let terminal = *self
            .terminal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (terminal.completed, terminal.failure_code)
    }
}

pub(crate) fn classify_runtime(
    runtime: ConsumerRuntimeState,
    completed: bool,
    failed: bool,
) -> (
    ConsumerLifecycleState,
    ConsumerBrokerState,
    ConsumerTopologyState,
) {
    if completed {
        return if failed {
            (
                ConsumerLifecycleState::Failed,
                ConsumerBrokerState::Failed,
                ConsumerTopologyState::Failed,
            )
        } else {
            (
                ConsumerLifecycleState::Stopped,
                ConsumerBrokerState::Closed,
                ConsumerTopologyState::Closed,
            )
        };
    }

    match runtime {
        ConsumerRuntimeState::Starting => (
            ConsumerLifecycleState::Starting,
            ConsumerBrokerState::Connecting,
            ConsumerTopologyState::Pending,
        ),
        ConsumerRuntimeState::Ready => (
            ConsumerLifecycleState::Ready,
            ConsumerBrokerState::Connected,
            ConsumerTopologyState::Verified,
        ),
        ConsumerRuntimeState::Recovering => (
            ConsumerLifecycleState::Recovering,
            ConsumerBrokerState::Recovering,
            ConsumerTopologyState::Recovering,
        ),
        ConsumerRuntimeState::Draining => (
            ConsumerLifecycleState::Draining,
            ConsumerBrokerState::Closing,
            ConsumerTopologyState::Draining,
        ),
        ConsumerRuntimeState::Stopped => (
            ConsumerLifecycleState::Stopped,
            ConsumerBrokerState::Closed,
            ConsumerTopologyState::Closed,
        ),
        ConsumerRuntimeState::Failed => (
            ConsumerLifecycleState::Failed,
            ConsumerBrokerState::Failed,
            ConsumerTopologyState::Failed,
        ),
    }
}

pub(crate) fn classify_admission(
    runtime: ConsumerRuntimeState,
    ready_queues: u64,
    registered_queues: u64,
) -> ConsumerAdmissionState {
    match runtime {
        ConsumerRuntimeState::Ready => ConsumerAdmissionState::Open,
        ConsumerRuntimeState::Recovering
            if ready_queues > 0 && ready_queues < registered_queues =>
        {
            ConsumerAdmissionState::PartiallyOpen
        }
        ConsumerRuntimeState::Draining => ConsumerAdmissionState::Draining,
        ConsumerRuntimeState::Starting
        | ConsumerRuntimeState::Recovering
        | ConsumerRuntimeState::Stopped
        | ConsumerRuntimeState::Failed => ConsumerAdmissionState::Closed,
    }
}

pub(crate) fn aggregate_readiness(
    deliveries_ready: bool,
    transactional_outbox_ready: bool,
    shutdown_ready: bool,
    accepting: bool,
    shutdown_requested: bool,
    completed: bool,
) -> bool {
    deliveries_ready
        && transactional_outbox_ready
        && shutdown_ready
        && accepting
        && !shutdown_requested
        && !completed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporary_broker_recovery_is_live_but_not_ready() {
        let (lifecycle, broker, topology) =
            classify_runtime(ConsumerRuntimeState::Recovering, false, false);
        assert_eq!(lifecycle, ConsumerLifecycleState::Recovering);
        assert_eq!(broker, ConsumerBrokerState::Recovering);
        assert_eq!(topology, ConsumerTopologyState::Recovering);
        assert_eq!(
            classify_admission(ConsumerRuntimeState::Recovering, 1, 2),
            ConsumerAdmissionState::PartiallyOpen
        );
    }

    #[test]
    fn terminal_error_has_authority_over_a_stale_stopped_runtime_snapshot() {
        let (lifecycle, broker, topology) =
            classify_runtime(ConsumerRuntimeState::Stopped, true, true);
        assert_eq!(lifecycle, ConsumerLifecycleState::Failed);
        assert_eq!(broker, ConsumerBrokerState::Failed);
        assert_eq!(topology, ConsumerTopologyState::Failed);
    }

    #[test]
    fn terminal_success_has_authority_over_a_stale_ready_runtime_snapshot() {
        let (lifecycle, broker, topology) =
            classify_runtime(ConsumerRuntimeState::Ready, true, false);
        assert_eq!(lifecycle, ConsumerLifecycleState::Stopped);
        assert_eq!(broker, ConsumerBrokerState::Closed);
        assert_eq!(topology, ConsumerTopologyState::Closed);
    }

    #[test]
    fn transactional_outbox_readiness_is_part_of_the_aggregate_gate() {
        assert!(aggregate_readiness(true, true, true, true, false, false));
        assert!(!aggregate_readiness(true, false, true, true, false, false));
    }
}
