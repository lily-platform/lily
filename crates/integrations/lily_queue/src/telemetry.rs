//! Sampling-independent delivery settlement accounting.

#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Histogram, UpDownCounter},
};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

/// Lifecycle state of the RabbitMQ consumer runtime.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum ConsumerRuntimeState {
    /// Runtime assembly has begun but no readiness claim is made.
    #[default]
    Starting = 0,
    /// Every registered consumer is accepting deliveries.
    Ready = 1,
    /// At least one consumer is restoring a lost broker stream.
    Recovering = 2,
    /// Delivery admission has stopped and in-flight work is draining.
    Draining = 3,
    /// All supervised queue tasks stopped normally.
    Stopped = 4,
    /// The supervised runtime observed a terminal failure.
    Failed = 5,
}

impl ConsumerRuntimeState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Ready,
            2 => Self::Recovering,
            3 => Self::Draining,
            4 => Self::Stopped,
            5 => Self::Failed,
            _ => Self::Starting,
        }
    }
}

/// Sampling-independent aggregate settlement state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DeliveryTerminalSnapshot {
    /// Total broker deliveries admitted by the runtime.
    pub deliveries: u64,
    /// Deliveries acknowledged after handler success.
    pub acked_handler_success: u64,
    /// Deliveries acknowledged after a confirmed retry or dead-letter handoff.
    pub acked_confirmed_handoff: u64,
    /// Deliveries explicitly nacked or requeued.
    pub nacked_or_requeued: u64,
    /// Deliveries released without settlement during controlled shutdown and
    /// therefore pending broker redelivery.
    pub buffered_pending_redelivery: u64,
    /// Deliveries whose terminal settlement could not be proven.
    pub unresolved: u64,
    /// Deliveries currently executing or awaiting settlement.
    pub in_flight: u64,
    /// Confirmed retry handoffs.
    pub retry_confirmed: u64,
    /// Confirmed dead-letter handoffs.
    pub dead_letter_confirmed: u64,
    /// Handler successes before broker settlement.
    pub handler_success: u64,
    /// Handler errors before broker settlement.
    pub handler_failure: u64,
    /// Handler panics caught at the execution boundary.
    pub handler_panic: u64,
    /// Number of configured consumers registered with the runtime.
    pub registered_consumers: u64,
    /// Number of consumers the accepted runtime plan requires before readiness.
    pub expected_consumers: u64,
    /// Number of registered consumers currently ready.
    pub ready_consumers: u64,
    /// Most recent stable runtime/provider failure category, retained across
    /// successful recovery and never populated with broker reply text.
    pub last_operational_failure_code: Option<&'static str>,
    /// Current aggregate runtime state.
    pub runtime_state: ConsumerRuntimeState,
}

/// Terminal settlement category for one observed delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryTerminalOutcome {
    /// Handler completed and ACK succeeded.
    HandlerSuccess,
    /// Retry publication was confirmed before the original ACK.
    RetryConfirmed,
    /// Dead-letter publication was confirmed before the original ACK.
    DeadLetterConfirmed,
    /// The original delivery was explicitly NACKed to RabbitMQ's configured
    /// dead-letter path without an application-side publish handoff.
    BrokerDeadLettered,
    /// The broker was asked to requeue the original delivery.
    NackRequeue,
    /// Local shutdown left the broker delivery unacked for redelivery.
    BufferedPendingRedelivery,
    /// A terminal broker outcome could not be proven.
    Unresolved,
}

impl DeliveryTerminalOutcome {
    /// Stable low-cardinality label used by telemetry exporters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HandlerSuccess => "business_success",
            Self::RetryConfirmed => "confirmed_retry_handoff",
            Self::DeadLetterConfirmed => "confirmed_dlq_handoff",
            Self::BrokerDeadLettered => "broker_dead_lettered",
            Self::NackRequeue => "nack_requeue",
            Self::BufferedPendingRedelivery => "buffered_pending_redelivery",
            Self::Unresolved => "unresolved",
        }
    }
}

/// Bounded per-delivery settlement evidence without payload data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryTerminalObservation {
    /// Stable event identity, when the envelope supplied one.
    pub event_id: Option<String>,
    /// One-based delivery attempt number.
    pub delivery_attempt: u32,
    /// Proven terminal outcome.
    pub outcome: DeliveryTerminalOutcome,
    /// Observation time measured from the Unix epoch.
    pub observed_at_unix_micros: u128,
}

/// Read-only copy of bounded settlement observations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeliveryTerminalObservationsSnapshot {
    /// Retained observations, capped at 4,096 for one runtime.
    pub observations: Vec<DeliveryTerminalObservation>,
    /// Observations omitted because the bound or lock was unavailable.
    pub dropped: u64,
}

const MAX_DELIVERY_TERMINAL_OBSERVATIONS: usize = 4_096;

impl DeliveryTerminalSnapshot {
    /// Count deliveries that are not yet proven settled.
    pub const fn unacked_or_in_flight(self) -> u64 {
        self.unresolved.saturating_add(self.in_flight)
    }

    /// Whether every admitted delivery belongs to exactly one terminal bucket.
    pub const fn is_reconciled(self) -> bool {
        self.deliveries
            == self
                .acked_handler_success
                .saturating_add(self.acked_confirmed_handoff)
                .saturating_add(self.nacked_or_requeued)
                .saturating_add(self.buffered_pending_redelivery)
                .saturating_add(self.unacked_or_in_flight())
    }

    /// Whether every registered consumer currently claims readiness.
    pub const fn is_ready(self) -> bool {
        matches!(self.runtime_state, ConsumerRuntimeState::Ready)
            && self.expected_consumers > 0
            && self.registered_consumers == self.expected_consumers
            && self.ready_consumers == self.expected_consumers
    }
}

#[derive(Debug)]
pub(crate) struct DeliveryTerminalLedger {
    deliveries: AtomicU64,
    acked_handler_success: AtomicU64,
    acked_confirmed_handoff: AtomicU64,
    nacked_or_requeued: AtomicU64,
    buffered_pending_redelivery: AtomicU64,
    unresolved: AtomicU64,
    in_flight: AtomicU64,
    retry_confirmed: AtomicU64,
    dead_letter_confirmed: AtomicU64,
    handler_success: AtomicU64,
    handler_failure: AtomicU64,
    handler_panic: AtomicU64,
    registered_consumers: AtomicU64,
    expected_consumers: AtomicU64,
    ready_consumers: AtomicU64,
    last_operational_failure_code: Mutex<Option<&'static str>>,
    runtime_state: AtomicU8,
    deliveries_metric: Counter<u64>,
    in_flight_metric: UpDownCounter<i64>,
    settlements_metric: Counter<u64>,
    handler_outcomes_metric: Counter<u64>,
    queue_wait_metric: Histogram<f64>,
    semaphore_wait_metric: Histogram<f64>,
    handler_duration_metric: Histogram<f64>,
    queue_depth_metric: Histogram<u64>,
    poison_metric: Counter<u64>,
    redeliveries_metric: Counter<u64>,
    admission_wait_metric: Histogram<f64>,
    consumer_recovery_attempts_metric: Counter<u64>,
    consumer_recovery_outcomes_metric: Counter<u64>,
    consumer_connection_losses_metric: Counter<u64>,
    broker_depth_metric: Gauge<u64>,
    broker_consumers_metric: Gauge<u64>,
    terminal_observations: Mutex<Vec<DeliveryTerminalObservation>>,
    terminal_observations_dropped: AtomicU64,
    #[cfg(test)]
    panic_before_next_metric_observation: AtomicBool,
    #[cfg(test)]
    panic_before_next_terminal_observation: AtomicBool,
}

impl Default for DeliveryTerminalLedger {
    fn default() -> Self {
        let meter = global::meter("lily_queue");
        Self {
            deliveries: AtomicU64::new(0),
            acked_handler_success: AtomicU64::new(0),
            acked_confirmed_handoff: AtomicU64::new(0),
            nacked_or_requeued: AtomicU64::new(0),
            buffered_pending_redelivery: AtomicU64::new(0),
            unresolved: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            retry_confirmed: AtomicU64::new(0),
            dead_letter_confirmed: AtomicU64::new(0),
            handler_success: AtomicU64::new(0),
            handler_failure: AtomicU64::new(0),
            handler_panic: AtomicU64::new(0),
            registered_consumers: AtomicU64::new(0),
            expected_consumers: AtomicU64::new(0),
            ready_consumers: AtomicU64::new(0),
            last_operational_failure_code: Mutex::new(None),
            runtime_state: AtomicU8::new(ConsumerRuntimeState::Starting as u8),
            deliveries_metric: meter.u64_counter("messaging.consume.deliveries").build(),
            in_flight_metric: meter
                .i64_up_down_counter("messaging.consume.in_flight")
                .build(),
            settlements_metric: meter.u64_counter("messaging.consume.settlements").build(),
            handler_outcomes_metric: meter
                .u64_counter("messaging.consume.handler.outcomes")
                .build(),
            queue_wait_metric: meter
                .f64_histogram("messaging.consume.handler.queue.wait.duration")
                .with_unit("s")
                .build(),
            semaphore_wait_metric: meter
                .f64_histogram("messaging.consume.handler.semaphore.wait.duration")
                .with_unit("s")
                .build(),
            handler_duration_metric: meter
                .f64_histogram("messaging.consume.handler.duration")
                .with_unit("s")
                .build(),
            queue_depth_metric: meter
                .u64_histogram("messaging.consume.dispatch.queue.depth")
                .build(),
            poison_metric: meter.u64_counter("messaging.consume.poison").build(),
            redeliveries_metric: meter.u64_counter("messaging.consume.redeliveries").build(),
            admission_wait_metric: meter
                .f64_histogram("messaging.consume.admission.wait.duration")
                .with_unit("s")
                .build(),
            consumer_recovery_attempts_metric: meter
                .u64_counter("messaging.consume.recovery.attempts")
                .build(),
            consumer_recovery_outcomes_metric: meter
                .u64_counter("messaging.consume.recovery.outcomes")
                .build(),
            consumer_connection_losses_metric: meter
                .u64_counter("messaging.consume.connection.losses")
                .build(),
            broker_depth_metric: meter.u64_gauge("messaging.rabbitmq.queue.depth").build(),
            broker_consumers_metric: meter
                .u64_gauge("messaging.rabbitmq.queue.consumers")
                .build(),
            terminal_observations: Mutex::new(Vec::new()),
            terminal_observations_dropped: AtomicU64::new(0),
            #[cfg(test)]
            panic_before_next_metric_observation: AtomicBool::new(false),
            #[cfg(test)]
            panic_before_next_terminal_observation: AtomicBool::new(false),
        }
    }
}

impl DeliveryTerminalLedger {
    fn all_expected_consumers_ready(&self) -> bool {
        let expected = self.expected_consumers.load(Ordering::Acquire);
        expected > 0
            && self.registered_consumers.load(Ordering::Acquire) == expected
            && self.ready_consumers.load(Ordering::Acquire) == expected
    }

    fn transition(&self, state: ConsumerRuntimeState) {
        let mut current = self.runtime_state.load(Ordering::Acquire);
        loop {
            let current_state = ConsumerRuntimeState::from_u8(current);
            if current_state == state || !runtime_transition_allowed(current_state, state) {
                return;
            }
            match self.runtime_state.compare_exchange_weak(
                current,
                state as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn configure_expected_consumers(&self, expected: usize) {
        self.expected_consumers.store(
            u64::try_from(expected).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    pub(crate) fn snapshot(&self) -> DeliveryTerminalSnapshot {
        DeliveryTerminalSnapshot {
            deliveries: self.deliveries.load(Ordering::Acquire),
            acked_handler_success: self.acked_handler_success.load(Ordering::Acquire),
            acked_confirmed_handoff: self.acked_confirmed_handoff.load(Ordering::Acquire),
            nacked_or_requeued: self.nacked_or_requeued.load(Ordering::Acquire),
            buffered_pending_redelivery: self.buffered_pending_redelivery.load(Ordering::Acquire),
            unresolved: self.unresolved.load(Ordering::Acquire),
            in_flight: self.in_flight.load(Ordering::Acquire),
            retry_confirmed: self.retry_confirmed.load(Ordering::Acquire),
            dead_letter_confirmed: self.dead_letter_confirmed.load(Ordering::Acquire),
            handler_success: self.handler_success.load(Ordering::Acquire),
            handler_failure: self.handler_failure.load(Ordering::Acquire),
            handler_panic: self.handler_panic.load(Ordering::Acquire),
            registered_consumers: self.registered_consumers.load(Ordering::Acquire),
            expected_consumers: self.expected_consumers.load(Ordering::Acquire),
            ready_consumers: self.ready_consumers.load(Ordering::Acquire),
            last_operational_failure_code: self
                .last_operational_failure_code
                .lock()
                .map(|failure_code| *failure_code)
                .unwrap_or(None),
            runtime_state: ConsumerRuntimeState::from_u8(
                self.runtime_state.load(Ordering::Acquire),
            ),
        }
    }

    pub(crate) fn observations_snapshot(&self) -> DeliveryTerminalObservationsSnapshot {
        let observations = self
            .terminal_observations
            .lock()
            .map(|observations| observations.clone())
            .unwrap_or_default();
        DeliveryTerminalObservationsSnapshot {
            observations,
            dropped: self.terminal_observations_dropped.load(Ordering::Acquire),
        }
    }

    fn record_terminal_observation(
        &self,
        event_id: Option<&str>,
        delivery_attempt: u32,
        outcome: DeliveryTerminalOutcome,
    ) {
        #[cfg(test)]
        if self
            .panic_before_next_terminal_observation
            .swap(false, Ordering::AcqRel)
        {
            panic!("injected terminal observation panic");
        }
        // Detail capture must not block a settlement task (or its Drop path)
        // behind a diagnostic reader cloning the bounded history. Aggregate
        // terminal evidence was committed before this optional observation.
        let Ok(mut observations) = self.terminal_observations.try_lock() else {
            self.terminal_observations_dropped
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        if observations.len() >= MAX_DELIVERY_TERMINAL_OBSERVATIONS {
            self.terminal_observations_dropped
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        observations.push(DeliveryTerminalObservation {
            event_id: event_id.map(str::to_owned),
            delivery_attempt,
            outcome,
            observed_at_unix_micros: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| duration.as_micros()),
        });
    }

    pub(crate) fn begin(&self) {
        self.deliveries.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.deliveries_metric.add(1, &[]);
        self.in_flight_metric.add(1, &[]);
    }

    fn commit_handler_success(&self) {
        self.handler_success.fetch_add(1, Ordering::Relaxed);
    }

    fn observe_handler_success(&self) {
        self.before_metric_observation();
        self.handler_outcomes_metric
            .add(1, &[KeyValue::new("lily.outcome", "success")]);
    }

    fn commit_handler_success_ack(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.acked_handler_success.fetch_add(1, Ordering::Relaxed);
    }

    fn observe_handler_success_ack(&self) {
        self.before_metric_observation();
        self.in_flight_metric.add(-1, &[]);
        self.settlements_metric
            .add(1, &[KeyValue::new("lily.ack_outcome", "handler_success")]);
    }

    pub(crate) fn handler_failure(&self, outcome: &'static str) {
        if outcome == "panic" {
            self.handler_panic.fetch_add(1, Ordering::Relaxed);
        } else {
            self.handler_failure.fetch_add(1, Ordering::Relaxed);
        }
        self.record_handler_outcome(outcome);
    }

    pub(crate) fn record_confirmed_handoff(&self, handoff: ConfirmedHandoff) {
        if matches!(handoff, ConfirmedHandoff::Retry) {
            self.retry_confirmed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.dead_letter_confirmed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn commit_confirmed_handoff_ack(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.acked_confirmed_handoff.fetch_add(1, Ordering::Relaxed);
    }

    fn observe_confirmed_handoff_ack(&self, handoff: ConfirmedHandoff) {
        self.before_metric_observation();
        self.in_flight_metric.add(-1, &[]);
        self.settlements_metric.add(
            1,
            &[KeyValue::new(
                "lily.ack_outcome",
                if matches!(handoff, ConfirmedHandoff::Retry) {
                    "retry_confirmed"
                } else {
                    "dlq_confirmed"
                },
            )],
        );
    }

    fn commit_broker_dead_letter(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.nacked_or_requeued.fetch_add(1, Ordering::Relaxed);
    }

    fn observe_broker_dead_letter(&self) {
        self.before_metric_observation();
        self.in_flight_metric.add(-1, &[]);
        self.settlements_metric.add(
            1,
            &[KeyValue::new("lily.ack_outcome", "broker_dead_lettered")],
        );
    }

    fn commit_nack_requeue(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.nacked_or_requeued.fetch_add(1, Ordering::Relaxed);
    }

    fn observe_nack_requeue(&self) {
        self.before_metric_observation();
        self.in_flight_metric.add(-1, &[]);
        self.settlements_metric
            .add(1, &[KeyValue::new("lily.ack_outcome", "nack_requeue")]);
    }

    fn commit_buffered_pending_redelivery(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.buffered_pending_redelivery
            .fetch_add(1, Ordering::Relaxed);
    }

    fn observe_buffered_pending_redelivery(&self) {
        self.before_metric_observation();
        self.in_flight_metric.add(-1, &[]);
        self.settlements_metric.add(
            1,
            &[KeyValue::new(
                "lily.ack_outcome",
                "buffered_pending_redelivery",
            )],
        );
    }

    fn commit_unresolved(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.unresolved.fetch_add(1, Ordering::Relaxed);
    }

    fn observe_unresolved(&self) {
        self.before_metric_observation();
        self.in_flight_metric.add(-1, &[]);
        self.settlements_metric
            .add(1, &[KeyValue::new("lily.ack_outcome", "unresolved")]);
    }

    fn before_metric_observation(&self) {
        #[cfg(test)]
        if self
            .panic_before_next_metric_observation
            .swap(false, Ordering::AcqRel)
        {
            panic!("injected metric observation panic");
        }
    }

    pub(crate) fn record_queue_wait(&self, duration: Duration) {
        self.queue_wait_metric.record(duration.as_secs_f64(), &[]);
    }

    pub(crate) fn record_semaphore_wait(&self, duration: Duration) {
        self.semaphore_wait_metric
            .record(duration.as_secs_f64(), &[]);
    }

    pub(crate) fn record_handler_duration(&self, duration: Duration, outcome: &'static str) {
        self.handler_duration_metric.record(
            duration.as_secs_f64(),
            &[KeyValue::new("lily.outcome", outcome)],
        );
    }

    pub(crate) fn record_queue_depth(&self, depth: usize) {
        self.queue_depth_metric
            .record(u64::try_from(depth).unwrap_or(u64::MAX), &[]);
    }

    pub(crate) fn record_poison(&self) {
        self.poison_metric.add(1, &[]);
    }

    pub(crate) fn record_redelivery(&self) {
        self.redeliveries_metric.add(1, &[]);
    }

    pub(crate) fn record_admission_wait(&self, duration: Duration) {
        self.admission_wait_metric
            .record(duration.as_secs_f64(), &[]);
    }

    pub(crate) fn record_consumer_recovery_attempt(&self) {
        self.consumer_recovery_attempts_metric.add(1, &[]);
    }

    pub(crate) fn record_consumer_recovery_outcome(&self, outcome: &'static str) {
        self.consumer_recovery_outcomes_metric
            .add(1, &[KeyValue::new("lily.outcome", outcome)]);
    }

    pub(crate) fn record_consumer_connection_loss(&self) {
        self.consumer_connection_losses_metric.add(1, &[]);
    }

    pub(crate) fn record_operational_failure(&self, failure_code: &'static str) {
        let mut last_failure_code = self
            .last_operational_failure_code
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *last_failure_code = Some(failure_code);
    }

    pub(crate) fn record_broker_state(&self, queue: &str, depth: u32, consumers: u32) {
        let attributes = [KeyValue::new(
            "messaging.destination.name",
            queue.to_owned(),
        )];
        self.broker_depth_metric
            .record(u64::from(depth), &attributes);
        self.broker_consumers_metric
            .record(u64::from(consumers), &attributes);
    }

    pub(crate) fn record_handler_outcome(&self, outcome: &'static str) {
        self.handler_outcomes_metric
            .add(1, &[KeyValue::new("lily.outcome", outcome)]);
    }

    pub(crate) fn begin_draining(&self) {
        self.transition(ConsumerRuntimeState::Draining);
    }

    pub(crate) fn stopped(&self) {
        self.transition(ConsumerRuntimeState::Stopped);
    }

    pub(crate) fn failed(&self) {
        self.runtime_state
            .store(ConsumerRuntimeState::Failed as u8, Ordering::Release);
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.runtime_state.load(Ordering::Acquire) == ConsumerRuntimeState::Failed as u8
    }
}

const fn runtime_transition_allowed(
    current: ConsumerRuntimeState,
    next: ConsumerRuntimeState,
) -> bool {
    match current {
        ConsumerRuntimeState::Starting => matches!(
            next,
            ConsumerRuntimeState::Ready
                | ConsumerRuntimeState::Recovering
                | ConsumerRuntimeState::Draining
                | ConsumerRuntimeState::Stopped
                | ConsumerRuntimeState::Failed
        ),
        ConsumerRuntimeState::Ready => matches!(
            next,
            ConsumerRuntimeState::Recovering
                | ConsumerRuntimeState::Draining
                | ConsumerRuntimeState::Stopped
                | ConsumerRuntimeState::Failed
        ),
        ConsumerRuntimeState::Recovering => matches!(
            next,
            ConsumerRuntimeState::Ready
                | ConsumerRuntimeState::Draining
                | ConsumerRuntimeState::Stopped
                | ConsumerRuntimeState::Failed
        ),
        ConsumerRuntimeState::Draining => {
            matches!(
                next,
                ConsumerRuntimeState::Stopped | ConsumerRuntimeState::Failed
            )
        }
        ConsumerRuntimeState::Stopped => matches!(next, ConsumerRuntimeState::Failed),
        ConsumerRuntimeState::Failed => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConfirmedHandoff {
    Retry,
    DeadLetter,
}

pub(crate) struct DeliveryAttemptGuard {
    ledger: Arc<DeliveryTerminalLedger>,
    event_id: Option<String>,
    delivery_attempt: u32,
    handler_success_recorded: bool,
    confirmed_handoff: Option<ConfirmedHandoff>,
    force_redelivery: Option<CancellationToken>,
    settlement_started: bool,
    finished: bool,
}

impl DeliveryAttemptGuard {
    pub(crate) fn begin(
        ledger: Arc<DeliveryTerminalLedger>,
        event_id: Option<String>,
        delivery_attempt: u32,
    ) -> Self {
        ledger.begin();
        Self {
            ledger,
            event_id,
            delivery_attempt,
            handler_success_recorded: false,
            confirmed_handoff: None,
            force_redelivery: None,
            settlement_started: false,
            finished: false,
        }
    }

    pub(crate) fn arm_force_redelivery(&mut self, force: CancellationToken) {
        if !self.finished {
            self.force_redelivery = Some(force);
        }
    }

    pub(crate) fn mark_settlement_started(&mut self) {
        if !self.finished {
            self.settlement_started = true;
        }
    }

    pub(crate) fn record_handler_success(&mut self) {
        if self.finished {
            return;
        }
        if self.handler_success_recorded {
            return;
        }
        self.handler_success_recorded = true;
        self.ledger.commit_handler_success();
        self.ledger.observe_handler_success();
    }

    pub(crate) fn finish_handler_success_ack(&mut self) {
        if self.finished {
            return;
        }
        let commit_handler_success = !self.handler_success_recorded;
        self.finished = true;
        self.handler_success_recorded = true;
        if commit_handler_success {
            self.ledger.commit_handler_success();
        }
        self.ledger.commit_handler_success_ack();
        if commit_handler_success {
            self.ledger.observe_handler_success();
        }
        self.ledger.observe_handler_success_ack();
        self.ledger.record_terminal_observation(
            self.event_id.as_deref(),
            self.delivery_attempt,
            DeliveryTerminalOutcome::HandlerSuccess,
        );
    }

    #[cfg(test)]
    pub(crate) fn finish_handler_success(&mut self) {
        self.finish_handler_success_ack();
    }

    pub(crate) fn record_confirmed_handoff(&mut self, retry: bool) {
        if self.finished {
            return;
        }
        if self.confirmed_handoff.is_some() {
            return;
        }
        let handoff = if retry {
            ConfirmedHandoff::Retry
        } else {
            ConfirmedHandoff::DeadLetter
        };
        self.confirmed_handoff = Some(handoff);
        self.ledger.record_confirmed_handoff(handoff);
    }

    pub(crate) fn finish_confirmed_handoff_ack(&mut self) {
        if self.finished {
            return;
        }
        let Some(handoff) = self.confirmed_handoff else {
            return;
        };
        self.finished = true;
        self.ledger.commit_confirmed_handoff_ack();
        self.ledger.observe_confirmed_handoff_ack(handoff);
        self.ledger.record_terminal_observation(
            self.event_id.as_deref(),
            self.delivery_attempt,
            match handoff {
                ConfirmedHandoff::Retry => DeliveryTerminalOutcome::RetryConfirmed,
                ConfirmedHandoff::DeadLetter => DeliveryTerminalOutcome::DeadLetterConfirmed,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn finish_handoff(&mut self, retry: bool) {
        self.record_confirmed_handoff(retry);
        self.finish_confirmed_handoff_ack();
    }

    pub(crate) fn finish_broker_dead_letter(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.ledger.commit_broker_dead_letter();
        self.ledger.observe_broker_dead_letter();
        self.ledger.record_terminal_observation(
            self.event_id.as_deref(),
            self.delivery_attempt,
            DeliveryTerminalOutcome::BrokerDeadLettered,
        );
    }

    pub(crate) fn finish_nack_requeue(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.ledger.commit_nack_requeue();
        self.ledger.observe_nack_requeue();
        self.ledger.record_terminal_observation(
            self.event_id.as_deref(),
            self.delivery_attempt,
            DeliveryTerminalOutcome::NackRequeue,
        );
    }

    pub(crate) fn finish_buffered_pending_redelivery(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.ledger.commit_buffered_pending_redelivery();
        self.ledger.observe_buffered_pending_redelivery();
        self.ledger.record_terminal_observation(
            self.event_id.as_deref(),
            self.delivery_attempt,
            DeliveryTerminalOutcome::BufferedPendingRedelivery,
        );
    }

    pub(crate) fn finish_unresolved(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.ledger.commit_unresolved();
        self.ledger.observe_unresolved();
        self.ledger.record_terminal_observation(
            self.event_id.as_deref(),
            self.delivery_attempt,
            DeliveryTerminalOutcome::Unresolved,
        );
    }

    /// Classify interruption from the same evidence whether execution returns
    /// a framework cancellation or is dropped. Controlled force before
    /// settlement leaves the original pending redelivery; it does not prove
    /// that the broker has already redelivered it.
    pub(crate) fn finish_interrupted(&mut self) -> Option<DeliveryTerminalOutcome> {
        if self.finished {
            return None;
        }
        let outcome = if self
            .force_redelivery
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
            && !self.settlement_started
        {
            self.finish_buffered_pending_redelivery();
            DeliveryTerminalOutcome::BufferedPendingRedelivery
        } else {
            self.finish_unresolved();
            DeliveryTerminalOutcome::Unresolved
        };
        Some(outcome)
    }
}

impl Drop for DeliveryAttemptGuard {
    fn drop(&mut self) {
        let _ = self.finish_interrupted();
    }
}

pub(crate) struct ConsumerReadinessGuard {
    ledger: Arc<DeliveryTerminalLedger>,
    registered: bool,
    ready: bool,
}

impl ConsumerReadinessGuard {
    pub(crate) fn opened(ledger: Arc<DeliveryTerminalLedger>) -> Self {
        ledger.registered_consumers.fetch_add(1, Ordering::AcqRel);
        ledger.ready_consumers.fetch_add(1, Ordering::AcqRel);
        if ledger.all_expected_consumers_ready() {
            ledger.transition(ConsumerRuntimeState::Ready);
        }
        Self {
            ledger,
            registered: true,
            ready: true,
        }
    }

    /// Withdraw a registration which was published provisionally but whose
    /// startup observer disappeared before the commit result was delivered.
    ///
    /// Normal receiver exit retains immutable registration evidence. This is
    /// the narrower pre-commit rollback boundary and therefore removes both
    /// counters exactly once.
    pub(crate) fn rollback_uncommitted(&mut self) {
        if !self.registered {
            return;
        }
        let withdrew_full_readiness = self.ledger.all_expected_consumers_ready();
        if self.ready {
            self.ledger.ready_consumers.fetch_sub(1, Ordering::AcqRel);
            self.ready = false;
        }
        self.ledger
            .registered_consumers
            .fetch_sub(1, Ordering::AcqRel);
        self.registered = false;
        if withdrew_full_readiness {
            self.ledger.transition(ConsumerRuntimeState::Recovering);
        }
    }

    pub(crate) fn recovering(&mut self) {
        if self.ready {
            self.ledger.ready_consumers.fetch_sub(1, Ordering::AcqRel);
            self.ready = false;
        }
        self.ledger.transition(ConsumerRuntimeState::Recovering);
    }

    pub(crate) fn ready(&mut self) {
        if !self.ready {
            self.ledger.ready_consumers.fetch_add(1, Ordering::AcqRel);
            self.ready = true;
        }
        if self.ledger.all_expected_consumers_ready() {
            self.ledger.transition(ConsumerRuntimeState::Ready);
        }
    }
}

impl Drop for ConsumerReadinessGuard {
    fn drop(&mut self) {
        if self.registered && self.ready {
            self.ledger.ready_consumers.fetch_sub(1, Ordering::AcqRel);
        }
        // Registration is immutable runtime-plan evidence. Receiver exit only
        // withdraws readiness; the engine publishes `Stopped` after it joins
        // the receiver, dispatcher and every nested delivery task.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_runtime_has_no_legacy_metrics_macro_backend() {
        for source in [
            include_str!("lifecycle.rs"),
            include_str!("providers/rabbitmq/queue_engine.rs"),
            include_str!("providers/rabbitmq/retry_engine.rs"),
        ] {
            for legacy_macro in [
                concat!("metrics::", "increment_counter!"),
                concat!("metrics::", "decrement_counter!"),
                concat!("metrics::", "increment_gauge!"),
                concat!("metrics::", "decrement_gauge!"),
                concat!("metrics::", "gauge!"),
                concat!("metrics::", "histogram!"),
            ] {
                assert!(
                    !source.contains(legacy_macro),
                    "legacy metric backend call remains: {legacy_macro}"
                );
            }
        }
    }

    #[test]
    fn dropped_and_terminal_deliveries_always_reconcile() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        drop(DeliveryAttemptGuard::begin(
            Arc::clone(&ledger),
            Some("event-unresolved".into()),
            1,
        ));
        let mut success =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("event-success".into()), 1);
        success.finish_handler_success();
        let mut handoff =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("event-retry".into()), 1);
        handoff.finish_handoff(true);
        let mut dead_letter =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("event-dlq".into()), 1);
        dead_letter.finish_handoff(false);
        let mut nack =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("event-nack".into()), 1);
        nack.finish_nack_requeue();
        let mut buffered =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("event-buffered".into()), 1);
        buffered.finish_buffered_pending_redelivery();
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 6);
        assert_eq!(snapshot.unresolved, 1);
        assert_eq!(snapshot.retry_confirmed, 1);
        assert_eq!(snapshot.dead_letter_confirmed, 1);
        assert_eq!(snapshot.nacked_or_requeued, 1);
        assert_eq!(snapshot.buffered_pending_redelivery, 1);
        assert!(snapshot.is_reconciled());
        let observations = ledger.observations_snapshot();
        assert_eq!(observations.dropped, 0);
        assert_eq!(observations.observations.len(), 6);
        assert!(observations.observations.iter().any(|observation| {
            observation.event_id.as_deref() == Some("event-dlq")
                && observation.outcome == DeliveryTerminalOutcome::DeadLetterConfirmed
        }));
        assert!(observations.observations.iter().any(|observation| {
            observation.event_id.as_deref() == Some("event-nack")
                && observation.outcome == DeliveryTerminalOutcome::NackRequeue
        }));
    }

    #[test]
    fn only_pre_settlement_framework_force_drop_is_classified_as_pending_redelivery() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());

        let inactive_force = CancellationToken::new();
        let mut ordinary_drop =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("ordinary-drop".into()), 1);
        ordinary_drop.arm_force_redelivery(inactive_force);
        drop(ordinary_drop);

        let active_force = CancellationToken::new();
        let mut forced_drop =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("forced-drop".into()), 1);
        forced_drop.arm_force_redelivery(active_force.clone());
        active_force.cancel();
        drop(forced_drop);

        let settlement_force = CancellationToken::new();
        let mut settlement_drop =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("settlement-drop".into()), 1);
        settlement_drop.arm_force_redelivery(settlement_force.clone());
        settlement_drop.mark_settlement_started();
        settlement_force.cancel();
        drop(settlement_drop);

        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 3);
        assert_eq!(snapshot.unresolved, 2);
        assert_eq!(snapshot.buffered_pending_redelivery, 1);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
    }

    #[test]
    fn returned_and_dropped_interruptions_use_identical_force_and_settlement_evidence() {
        for force_state in 0..3 {
            for settlement_started in [false, true] {
                for explicit_return in [false, true] {
                    let ledger = Arc::new(DeliveryTerminalLedger::default());
                    let mut attempt = DeliveryAttemptGuard::begin(
                        ledger.clone(),
                        Some("framework-interrupted".into()),
                        1,
                    );
                    if force_state != 0 {
                        let force = CancellationToken::new();
                        attempt.arm_force_redelivery(force.clone());
                        if force_state == 2 {
                            force.cancel();
                        }
                    }
                    if settlement_started {
                        attempt.mark_settlement_started();
                    }
                    let pending_redelivery = force_state == 2 && !settlement_started;
                    let expected = if pending_redelivery {
                        DeliveryTerminalOutcome::BufferedPendingRedelivery
                    } else {
                        DeliveryTerminalOutcome::Unresolved
                    };
                    if explicit_return {
                        assert_eq!(attempt.finish_interrupted(), Some(expected));
                        assert_eq!(attempt.finish_interrupted(), None);
                    }
                    drop(attempt);
                    let snapshot = ledger.snapshot();
                    assert_eq!(snapshot.deliveries, 1);
                    assert_eq!(snapshot.unresolved, u64::from(!pending_redelivery));
                    assert_eq!(
                        snapshot.buffered_pending_redelivery,
                        u64::from(pending_redelivery)
                    );
                    assert_eq!(snapshot.in_flight, 0);
                    assert_eq!(snapshot.acked_handler_success, 0);
                    assert_eq!(snapshot.acked_confirmed_handoff, 0);
                    assert_eq!(snapshot.nacked_or_requeued, 0);
                    assert_eq!(snapshot.retry_confirmed, 0);
                    assert_eq!(snapshot.dead_letter_confirmed, 0);
                    assert!(snapshot.is_reconciled());
                    let details = ledger.observations_snapshot();
                    assert_eq!(details.dropped, 0);
                    assert_eq!(details.observations.len(), 1);
                    assert_eq!(details.observations[0].outcome, expected);
                }
            }
        }
    }

    #[test]
    fn confirmed_handoff_survives_an_unresolved_original_ack() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let mut attempt = DeliveryAttemptGuard::begin(
            Arc::clone(&ledger),
            Some("event-confirmed-before-ack".into()),
            2,
        );

        attempt.record_confirmed_handoff(true);

        let before_ack = ledger.snapshot();
        assert_eq!(before_ack.deliveries, 1);
        assert_eq!(before_ack.retry_confirmed, 1);
        assert_eq!(before_ack.dead_letter_confirmed, 0);
        assert_eq!(before_ack.acked_confirmed_handoff, 0);
        assert_eq!(before_ack.in_flight, 1);

        // Dropping the owner models an ACK error, panic, timeout or cancelled
        // caller. The confirmed publish remains evidence, while the original
        // broker delivery is conservatively terminalized as unresolved.
        drop(attempt);

        let terminal = ledger.snapshot();
        assert_eq!(terminal.retry_confirmed, 1);
        assert_eq!(terminal.dead_letter_confirmed, 0);
        assert_eq!(terminal.acked_confirmed_handoff, 0);
        assert_eq!(terminal.unresolved, 1);
        assert_eq!(terminal.in_flight, 0);
        assert!(terminal.is_reconciled());
        let observations = ledger.observations_snapshot();
        assert_eq!(observations.dropped, 0);
        assert_eq!(observations.observations.len(), 1);
        assert_eq!(
            observations.observations[0].outcome,
            DeliveryTerminalOutcome::Unresolved
        );
    }

    #[test]
    fn handler_success_survives_an_unresolved_original_ack() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let mut attempt = DeliveryAttemptGuard::begin(
            Arc::clone(&ledger),
            Some("event-handler-success-before-ack".into()),
            1,
        );

        attempt.record_handler_success();
        let before_ack = ledger.snapshot();
        assert_eq!(before_ack.handler_success, 1);
        assert_eq!(before_ack.acked_handler_success, 0);
        assert_eq!(before_ack.in_flight, 1);

        drop(attempt);

        let terminal = ledger.snapshot();
        assert_eq!(terminal.handler_success, 1);
        assert_eq!(terminal.acked_handler_success, 0);
        assert_eq!(terminal.unresolved, 1);
        assert_eq!(terminal.in_flight, 0);
        assert!(terminal.is_reconciled());
        assert_eq!(
            ledger.observations_snapshot().observations[0].outcome,
            DeliveryTerminalOutcome::Unresolved
        );
    }

    #[test]
    fn confirmed_handoff_and_terminal_ack_are_first_writer_idempotent() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let mut attempt =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("event-idempotent".into()), 1);

        attempt.record_confirmed_handoff(true);
        attempt.record_confirmed_handoff(true);
        attempt.record_confirmed_handoff(false);
        attempt.finish_confirmed_handoff_ack();
        attempt.finish_confirmed_handoff_ack();
        attempt.finish_handoff(false);

        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.retry_confirmed, 1);
        assert_eq!(snapshot.dead_letter_confirmed, 0);
        assert_eq!(snapshot.acked_confirmed_handoff, 1);
        assert_eq!(snapshot.unresolved, 0);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
        let observations = ledger.observations_snapshot();
        assert_eq!(observations.dropped, 0);
        assert_eq!(observations.observations.len(), 1);
        assert_eq!(
            observations.observations[0].outcome,
            DeliveryTerminalOutcome::RetryConfirmed
        );
    }

    #[test]
    fn handler_phase_commits_before_a_metric_observation_panic() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let mut attempt = DeliveryAttemptGuard::begin(
            Arc::clone(&ledger),
            Some("event-handler-metric-panic".into()),
            1,
        );
        ledger
            .panic_before_next_metric_observation
            .store(true, Ordering::Release);

        let observation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            attempt.record_handler_success();
        }));
        assert!(observation.is_err(), "fault injection must execute");

        // Retrying the callback after its optional metric panicked must not
        // count the already-committed handler phase twice.
        attempt.record_handler_success();
        assert_eq!(ledger.snapshot().handler_success, 1);
        drop(attempt);

        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.handler_success, 1);
        assert_eq!(snapshot.unresolved, 1);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
    }

    #[test]
    fn terminal_atomics_commit_before_a_metric_observation_panic() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let mut attempt = DeliveryAttemptGuard::begin(
            Arc::clone(&ledger),
            Some("event-terminal-metric-panic".into()),
            1,
        );
        ledger
            .panic_before_next_metric_observation
            .store(true, Ordering::Release);

        let observation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            attempt.finish_handler_success_ack();
        }));
        assert!(observation.is_err(), "fault injection must execute");

        // The terminal callback is first-writer-wins even though its optional
        // metric did not complete.
        attempt.finish_nack_requeue();
        drop(attempt);

        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.handler_success, 1);
        assert_eq!(snapshot.acked_handler_success, 1);
        assert_eq!(snapshot.nacked_or_requeued, 0);
        assert_eq!(snapshot.unresolved, 0);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
        assert!(
            ledger.observations_snapshot().observations.is_empty(),
            "the panic occurred before optional terminal observation"
        );
    }

    #[test]
    fn terminal_flag_commits_before_optional_observation_panics() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let mut attempt = DeliveryAttemptGuard::begin(
            Arc::clone(&ledger),
            Some("event-terminal-observation-panic".into()),
            1,
        );
        ledger
            .panic_before_next_terminal_observation
            .store(true, Ordering::Release);

        let observation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            attempt.finish_buffered_pending_redelivery();
        }));
        assert!(observation.is_err(), "fault injection must execute");

        attempt.finish_handler_success_ack();
        drop(attempt);

        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.buffered_pending_redelivery, 1);
        assert_eq!(snapshot.acked_handler_success, 0);
        assert_eq!(snapshot.unresolved, 0);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
        assert!(ledger.observations_snapshot().observations.is_empty());
    }

    #[test]
    fn poisoned_observation_sink_cannot_change_terminal_counters() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let ledger = Arc::clone(&ledger);
            move || {
                let _observations = ledger
                    .terminal_observations
                    .lock()
                    .expect("observation lock begins healthy");
                panic!("inject observation sink poison");
            }
        }));
        assert!(poison_result.is_err(), "fault injection must execute");

        let mut attempt = DeliveryAttemptGuard::begin(
            Arc::clone(&ledger),
            Some("event-poisoned-telemetry".into()),
            1,
        );
        attempt.record_confirmed_handoff(false);
        attempt.finish_confirmed_handoff_ack();

        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.retry_confirmed, 0);
        assert_eq!(snapshot.dead_letter_confirmed, 1);
        assert_eq!(snapshot.acked_confirmed_handoff, 1);
        assert_eq!(snapshot.unresolved, 0);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
        let observations = ledger.observations_snapshot();
        assert!(observations.observations.is_empty());
        assert_eq!(observations.dropped, 1);
    }

    #[test]
    fn long_running_delivery_accounting_keeps_bounded_history_without_losing_aggregate_evidence() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let total = MAX_DELIVERY_TERMINAL_OBSERVATIONS * 3;
        for index in 0..total {
            let mut attempt =
                DeliveryAttemptGuard::begin(ledger.clone(), Some(format!("event-{index}")), 1);
            if index % 2 == 0 {
                attempt.finish_handler_success();
            }
            // Odd attempts intentionally lose settlement proof; aggregate
            // unresolved evidence must survive after detail storage fills.
        }
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, total as u64);
        assert_eq!(snapshot.handler_success, (total / 2) as u64);
        assert_eq!(snapshot.acked_handler_success, (total / 2) as u64);
        assert_eq!(snapshot.unresolved, (total / 2) as u64);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
        let details = ledger.observations_snapshot();
        assert_eq!(
            details.observations.len(),
            MAX_DELIVERY_TERMINAL_OBSERVATIONS
        );
        assert_eq!(
            details.dropped,
            (total - MAX_DELIVERY_TERMINAL_OBSERVATIONS) as u64
        );
        for (index, observation) in details.observations.iter().enumerate() {
            assert_eq!(observation.event_id, Some(format!("event-{index}")));
            assert_eq!(observation.delivery_attempt, 1);
            assert_eq!(
                observation.outcome,
                if index % 2 == 0 {
                    DeliveryTerminalOutcome::HandlerSuccess
                } else {
                    DeliveryTerminalOutcome::Unresolved
                }
            );
        }
        assert_eq!(ledger.snapshot(), snapshot);
        assert_eq!(ledger.observations_snapshot(), details);
    }

    #[test]
    fn slow_diagnostic_reader_cannot_block_settlement_or_erase_terminal_evidence() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let reader = ledger.terminal_observations.lock().unwrap();
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let worker_ledger = ledger.clone();
        let worker = std::thread::spawn(move || {
            let mut attempt = DeliveryAttemptGuard::begin(
                worker_ledger,
                Some("acked-while-reader-holds-lock".into()),
                2,
            );
            attempt.record_confirmed_handoff(true);
            attempt.finish_confirmed_handoff_ack();
            completed_tx.send(()).unwrap();
        });
        let completed = completed_rx.recv_timeout(Duration::from_secs(1));
        // Release the fixture lock even on failure so a blocking-lock
        // regression produces a bounded assertion failure, not a hung suite.
        drop(reader);
        worker.join().unwrap();
        assert_eq!(
            completed,
            Ok(()),
            "diagnostic contention must never delay settlement"
        );
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.retry_confirmed, 1);
        assert_eq!(snapshot.acked_confirmed_handoff, 1);
        assert_eq!(snapshot.acked_handler_success, 0);
        assert_eq!(snapshot.unresolved, 0);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
        let details = ledger.observations_snapshot();
        assert_eq!(details.dropped, 1);
        assert!(details.observations.is_empty());
    }

    #[test]
    fn broker_dead_letter_does_not_claim_an_application_confirmed_handoff() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        let mut attempt =
            DeliveryAttemptGuard::begin(Arc::clone(&ledger), Some("event-broker-dlx".into()), 1);

        attempt.finish_broker_dead_letter();

        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.acked_confirmed_handoff, 0);
        assert_eq!(snapshot.retry_confirmed, 0);
        assert_eq!(snapshot.dead_letter_confirmed, 0);
        assert_eq!(snapshot.nacked_or_requeued, 1);
        assert_eq!(snapshot.unresolved, 0);
        assert!(snapshot.is_reconciled());
        let observations = ledger.observations_snapshot();
        assert_eq!(observations.dropped, 0);
        assert_eq!(observations.observations.len(), 1);
        assert_eq!(
            observations.observations[0].outcome,
            DeliveryTerminalOutcome::BrokerDeadLettered
        );
    }

    #[test]
    fn readiness_tracks_recovery_and_terminal_state() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        ledger.configure_expected_consumers(1);
        let mut readiness = ConsumerReadinessGuard::opened(Arc::clone(&ledger));
        assert!(ledger.snapshot().is_ready());
        readiness.recovering();
        assert_eq!(
            ConsumerRuntimeState::Recovering,
            ledger.snapshot().runtime_state
        );
        readiness.ready();
        assert!(ledger.snapshot().is_ready());
        ledger.failed();
        ledger.begin_draining();
        ledger.stopped();
        assert_eq!(
            ConsumerRuntimeState::Failed,
            ledger.snapshot().runtime_state
        );
        assert!(!ledger.snapshot().is_ready());

        let ledger = Arc::new(DeliveryTerminalLedger::default());
        ledger.configure_expected_consumers(1);
        let mut readiness = ConsumerReadinessGuard::opened(Arc::clone(&ledger));
        ledger.begin_draining();
        assert_eq!(
            ConsumerRuntimeState::Draining,
            ledger.snapshot().runtime_state
        );
        readiness.recovering();
        readiness.ready();
        assert_eq!(
            ConsumerRuntimeState::Draining,
            ledger.snapshot().runtime_state
        );
        assert!(!ledger.snapshot().is_ready());
        drop(readiness);
        assert_eq!(
            ConsumerRuntimeState::Draining,
            ledger.snapshot().runtime_state
        );
        ledger.stopped();
        assert_eq!(
            ConsumerRuntimeState::Stopped,
            ledger.snapshot().runtime_state
        );
    }

    #[test]
    fn readiness_waits_for_every_expected_consumer_and_retains_registration_evidence() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        ledger.configure_expected_consumers(2);

        let first = ConsumerReadinessGuard::opened(Arc::clone(&ledger));
        let partial = ledger.snapshot();
        assert_eq!(partial.expected_consumers, 2);
        assert_eq!(partial.registered_consumers, 1);
        assert_eq!(partial.ready_consumers, 1);
        assert!(!partial.is_ready());

        let second = ConsumerReadinessGuard::opened(Arc::clone(&ledger));
        assert!(ledger.snapshot().is_ready());
        drop(first);

        let exited = ledger.snapshot();
        assert_eq!(exited.registered_consumers, 2);
        assert_eq!(exited.ready_consumers, 1);
        assert!(!exited.is_ready());
        drop(second);
    }

    #[test]
    fn uncommitted_readiness_rollback_removes_both_provisional_counters_once() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        ledger.configure_expected_consumers(1);
        let mut provisional = ConsumerReadinessGuard::opened(Arc::clone(&ledger));
        assert!(ledger.snapshot().is_ready());

        provisional.rollback_uncommitted();
        provisional.rollback_uncommitted();
        drop(provisional);

        let rolled_back = ledger.snapshot();
        assert_eq!(rolled_back.registered_consumers, 0);
        assert_eq!(rolled_back.ready_consumers, 0);
        assert_eq!(rolled_back.runtime_state, ConsumerRuntimeState::Recovering);
        assert!(!rolled_back.is_ready());

        let committed = ConsumerReadinessGuard::opened(Arc::clone(&ledger));
        assert!(ledger.snapshot().is_ready());
        drop(committed);
        assert_eq!(ledger.snapshot().registered_consumers, 1);
        assert_eq!(ledger.snapshot().ready_consumers, 0);
    }

    #[test]
    fn operational_failure_code_is_payload_free_and_survives_recovery() {
        let ledger = Arc::new(DeliveryTerminalLedger::default());
        ledger.configure_expected_consumers(1);
        let mut readiness = ConsumerReadinessGuard::opened(Arc::clone(&ledger));

        ledger.record_operational_failure("BROKER_TRANSPORT");
        readiness.recovering();
        readiness.ready();

        let recovered = ledger.snapshot();
        assert!(recovered.is_ready());
        assert_eq!(
            recovered.last_operational_failure_code,
            Some("BROKER_TRANSPORT")
        );
    }
}
