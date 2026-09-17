//! Sampling-independent publisher terminal accounting.
//!
//! The counters deliberately retain no destination, routing key, event ID or
//! payload. Per-attempt correlation remains on the `messaging.publish` span.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Histogram, UpDownCounter},
    KeyValue,
};

#[derive(Debug)]
pub(crate) struct RabbitMqClientMetrics {
    connection_attempts: Counter<u64>,
    connection_recovery_outcomes: Counter<u64>,
    connection_recoveries: Counter<u64>,
    active_connections: Gauge<u64>,
    channel_recoveries: Counter<u64>,
    active_channels: Gauge<u64>,
}

impl Default for RabbitMqClientMetrics {
    fn default() -> Self {
        let meter = global::meter("lily_queue_client");
        Self {
            connection_attempts: meter
                .u64_counter("messaging.rabbitmq.connection.attempts")
                .build(),
            connection_recovery_outcomes: meter
                .u64_counter("messaging.rabbitmq.connection.recovery.outcomes")
                .build(),
            connection_recoveries: meter
                .u64_counter("messaging.rabbitmq.connection.recoveries")
                .build(),
            active_connections: meter
                .u64_gauge("messaging.rabbitmq.connections.active")
                .build(),
            channel_recoveries: meter
                .u64_counter("messaging.rabbitmq.channel.recoveries")
                .build(),
            active_channels: meter
                .u64_gauge("messaging.rabbitmq.channels.active")
                .build(),
        }
    }
}

impl RabbitMqClientMetrics {
    pub(crate) fn connection_attempt(&self) {
        self.connection_attempts.add(1, &[]);
    }

    pub(crate) fn connection_recovery_outcome(&self, outcome: &'static str) {
        self.connection_recovery_outcomes
            .add(1, &[KeyValue::new("lily.outcome", outcome)]);
    }

    pub(crate) fn connection_recovery(&self) {
        self.connection_recoveries.add(1, &[]);
    }

    pub(crate) fn active_connections(&self, count: usize) {
        self.active_connections
            .record(u64::try_from(count).unwrap_or(u64::MAX), &[]);
    }

    pub(crate) fn channel_recovery(&self, confirm: bool) {
        self.channel_recoveries.add(
            1,
            &[KeyValue::new(
                "messaging.rabbitmq.publisher.confirm",
                confirm,
            )],
        );
    }

    pub(crate) fn active_channels(&self, confirm: bool, count: usize) {
        self.active_channels.record(
            u64::try_from(count).unwrap_or(u64::MAX),
            &[KeyValue::new(
                "messaging.rabbitmq.publisher.confirm",
                confirm,
            )],
        );
    }
}

/// A point-in-time view of publisher terminal accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishTerminalSnapshot {
    /// Publish attempts admitted by the client.
    pub attempts: u64,
    /// Attempts acknowledged by RabbitMQ without a mandatory Return.
    pub broker_ack_no_return: u64,
    /// Attempts rejected by NACK or mandatory Return.
    pub nack_or_return: u64,
    /// Attempts ending in timeout, cancellation, transport failure, or a
    /// dropped future.
    pub timeout_or_unresolved: u64,
    /// Attempts without a terminal classification at snapshot time.
    pub in_flight: u64,
}

impl PublishTerminalSnapshot {
    /// All admitted attempts have exactly one terminal bucket, except work
    /// that is still in flight at the instant of the snapshot.
    pub const fn is_reconciled(self) -> bool {
        self.attempts
            == self
                .broker_ack_no_return
                .saturating_add(self.nack_or_return)
                .saturating_add(self.timeout_or_unresolved)
                .saturating_add(self.in_flight)
    }
}

/// Process-local monotonic publisher ledger. Metrics and qualification tests
/// can read it even when the corresponding trace was not sampled or exported.
#[derive(Debug)]
pub(crate) struct PublishTerminalLedger {
    attempts: AtomicU64,
    broker_ack_no_return: AtomicU64,
    nack_or_return: AtomicU64,
    timeout_or_unresolved: AtomicU64,
    in_flight: AtomicU64,
    attempts_metric: Counter<u64>,
    outcomes_metric: Counter<u64>,
    in_flight_metric: UpDownCounter<i64>,
    acquisition_wait_metric: Histogram<f64>,
    publish_duration_metric: Histogram<f64>,
    confirm_wait_metric: Histogram<f64>,
}

impl Default for PublishTerminalLedger {
    fn default() -> Self {
        let meter = global::meter("lily_queue_client");
        Self {
            attempts: AtomicU64::new(0),
            broker_ack_no_return: AtomicU64::new(0),
            nack_or_return: AtomicU64::new(0),
            timeout_or_unresolved: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            attempts_metric: meter.u64_counter("messaging.publish.attempts").build(),
            outcomes_metric: meter.u64_counter("messaging.publish.outcomes").build(),
            in_flight_metric: meter
                .i64_up_down_counter("messaging.publish.in_flight")
                .build(),
            acquisition_wait_metric: meter
                .f64_histogram("messaging.publish.acquisition.wait.duration")
                .with_unit("s")
                .build(),
            publish_duration_metric: meter
                .f64_histogram("messaging.publish.duration")
                .with_unit("s")
                .build(),
            confirm_wait_metric: meter
                .f64_histogram("messaging.publish.confirm.wait.duration")
                .with_unit("s")
                .build(),
        }
    }
}

impl PublishTerminalLedger {
    pub(crate) fn snapshot(&self) -> PublishTerminalSnapshot {
        PublishTerminalSnapshot {
            attempts: self.attempts.load(Ordering::Acquire),
            broker_ack_no_return: self.broker_ack_no_return.load(Ordering::Acquire),
            nack_or_return: self.nack_or_return.load(Ordering::Acquire),
            timeout_or_unresolved: self.timeout_or_unresolved.load(Ordering::Acquire),
            in_flight: self.in_flight.load(Ordering::Acquire),
        }
    }

    pub(crate) fn begin(&self) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.attempts_metric.add(1, &[]);
        self.in_flight_metric.add(1, &[]);
    }

    pub(crate) fn finish_ack(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.broker_ack_no_return.fetch_add(1, Ordering::Relaxed);
        self.in_flight_metric.add(-1, &[]);
        self.outcomes_metric
            .add(1, &[KeyValue::new("lily.outcome", "broker_ack_no_return")]);
    }

    pub(crate) fn finish_nack_or_return(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.nack_or_return.fetch_add(1, Ordering::Relaxed);
        self.in_flight_metric.add(-1, &[]);
        self.outcomes_metric
            .add(1, &[KeyValue::new("lily.outcome", "nack_or_return")]);
    }

    pub(crate) fn finish_timeout_or_unresolved(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.timeout_or_unresolved.fetch_add(1, Ordering::Relaxed);
        self.in_flight_metric.add(-1, &[]);
        self.outcomes_metric
            .add(1, &[KeyValue::new("lily.outcome", "timeout_or_unresolved")]);
    }

    pub(crate) fn record_acquisition_wait(&self, duration: Duration) {
        self.acquisition_wait_metric
            .record(duration.as_secs_f64(), &[]);
    }

    pub(crate) fn record_confirm_wait(&self, duration: Duration) {
        self.confirm_wait_metric.record(duration.as_secs_f64(), &[]);
    }

    pub(crate) fn record_publish_duration(&self, duration: Duration, outcome: &'static str) {
        self.publish_duration_metric.record(
            duration.as_secs_f64(),
            &[KeyValue::new("lily.outcome", outcome)],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_client_runtime_has_no_legacy_metrics_macro_backend() {
        for source in [
            include_str!("providers/rabbitmq/channel_manager.rs"),
            include_str!("providers/rabbitmq/connection_manager.rs"),
            include_str!("providers/rabbitmq/publisher_engine.rs"),
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
    fn terminal_buckets_reconcile_with_in_flight_work() {
        let ledger = PublishTerminalLedger::default();
        ledger.begin();
        ledger.begin();
        assert!(ledger.snapshot().is_reconciled());
        ledger.finish_ack();
        ledger.finish_nack_or_return();
        let snapshot = ledger.snapshot();
        assert!(snapshot.is_reconciled());
        assert_eq!(snapshot.attempts, 2);
        assert_eq!(snapshot.broker_ack_no_return, 1);
        assert_eq!(snapshot.nack_or_return, 1);
        assert_eq!(snapshot.in_flight, 0);
    }
}
