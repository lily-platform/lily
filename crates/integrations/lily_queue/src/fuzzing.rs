//! Internal bounded adapters for the queue fuzz qualification workspace.
//!
//! The adapters expose observations only. Envelope admission, AMQP metadata
//! bounds, typed extractor plans and settlement transitions remain owned by
//! the production implementation.

use std::time::Duration;

use async_trait::async_trait;
pub use lapin::{
    BasicProperties,
    types::{AMQPValue, FieldTable, ShortString},
};
use lily_error::application::{MessageBrokerError, message_broker::RabbitMQError};
use lily_queue_registry::QueuePayloadKind;
use tokio_util::sync::CancellationToken;

use crate::{
    providers::rabbitmq::{
        FuzzTransportAdmissionFailure, fuzz_amqp_metadata_footprint, fuzz_canonical_retry_count,
        fuzz_projected_handoff_properties, fuzz_transport_admission_failure,
        fuzz_validate_amqp_metadata, fuzz_validate_delivery_envelope,
        fuzz_validate_handoff_metadata_headroom,
    },
    retry_engine_trait::FailureClass,
    settlement::{
        ExecutionOutcome, HandoffDestination, HandoffPlan, HandoffReceipt, SettlementAuthority,
        SettlementObserver, SettlementObserverFailure, SettlementPort, SettlementTerminal,
        materialize_delivery,
    },
};

/// Bounded transport-admission result observed by a fuzz target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryAdmissionSummary {
    /// Body, metadata and canonical envelope were accepted.
    Accepted,
    /// Body exceeded the configured delivery bound.
    PayloadTooLarge,
    /// AMQP metadata or reserved handoff headroom exceeded its bound.
    MetadataBoundsExceeded,
    /// Required Lily envelope fields were missing or malformed.
    InvalidEnvelope,
}

/// Exercises the exact RabbitMQ transport admission order.
#[must_use]
pub fn exercise_delivery_envelope(
    body_len: usize,
    max_message_size_bytes: usize,
    properties: &BasicProperties,
) -> DeliveryAdmissionSummary {
    match fuzz_transport_admission_failure(body_len, max_message_size_bytes, properties) {
        None => DeliveryAdmissionSummary::Accepted,
        Some(FuzzTransportAdmissionFailure::PayloadTooLarge) => {
            DeliveryAdmissionSummary::PayloadTooLarge
        }
        Some(FuzzTransportAdmissionFailure::MetadataBoundsExceeded) => {
            DeliveryAdmissionSummary::MetadataBoundsExceeded
        }
        Some(FuzzTransportAdmissionFailure::InvalidEnvelope) => {
            DeliveryAdmissionSummary::InvalidEnvelope
        }
    }
}

/// Independent production observations for one AMQP metadata value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmqpMetadataSummary {
    /// Received metadata satisfies the raw count/byte/nesting bound.
    pub received_valid: bool,
    /// Received metadata also reserves worst-case framework handoff headers.
    pub handoff_headroom_valid: bool,
    /// Header count when the received metadata is valid.
    pub header_entries: Option<usize>,
    /// Aggregate metadata bytes when the received metadata is valid.
    pub aggregate_bytes: Option<usize>,
    /// Canonical retry value, when absent or represented as bounded LongInt.
    pub retry_count: Option<u32>,
    /// Worst-case projected handoff metadata validates independently.
    pub projected_valid: bool,
}

/// Exercises received, projected-handoff and retry metadata authorities.
#[must_use]
pub fn exercise_amqp_metadata(properties: &BasicProperties) -> AmqpMetadataSummary {
    let footprint = fuzz_amqp_metadata_footprint(properties).ok();
    let projected = fuzz_projected_handoff_properties(properties);
    AmqpMetadataSummary {
        received_valid: fuzz_validate_amqp_metadata(properties).is_ok(),
        handoff_headroom_valid: fuzz_validate_handoff_metadata_headroom(properties).is_ok(),
        header_entries: footprint.map(|footprint| footprint.header_entries),
        aggregate_bytes: footprint.map(|footprint| footprint.aggregate_bytes),
        retry_count: fuzz_canonical_retry_count(properties.headers().as_ref()).ok(),
        projected_valid: fuzz_validate_amqp_metadata(&projected).is_ok(),
    }
}

/// Result of one statically selected built-in extractor plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractorPlanSummary {
    /// Payload authority inferred by the production tuple implementation.
    pub payload_kind: QueuePayloadKind,
    /// Whether a terminal payload type is present in registry metadata.
    pub has_payload_type: bool,
    /// Accepted payload length or a stable typed rejection code.
    pub extraction: Result<usize, &'static str>,
    /// Whether the sole body authority was consumed.
    pub body_consumed: bool,
}

/// Exercises production tuple-contract inference and built-in payload decoding.
pub async fn exercise_extractor_plan(
    selector: u8,
    content_kind: &str,
    body: &[u8],
) -> ExtractorPlanSummary {
    let (contract, extraction, body_consumed) =
        crate::extractor::fuzz_extractor_plan(selector, content_kind, body).await;
    ExtractorPlanSummary {
        payload_kind: contract.payload_kind,
        has_payload_type: contract.payload_type_name.is_some(),
        extraction,
        body_consumed,
    }
}

/// Ready-only provider behavior used to explore settlement transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementPortBehavior {
    /// Provider accepts the operation.
    Accept,
    /// ACK/NACK is declined by the provider.
    Decline,
    /// Provider returns a typed transport failure.
    Error,
}

/// Fuzz-controlled settlement execution input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementStateInput {
    /// `0=success`, `1=retryable`, `2=permanent`, all others framework cancellation.
    pub outcome: u8,
    /// Current bounded retry generation supplied to policy.
    pub current_retry_count: u32,
    /// Maximum additional retry handoffs.
    pub retry_attempts: u32,
    /// Handoff provider behavior (`Decline` is treated as a provider error).
    pub handoff: SettlementPortBehavior,
    /// Original ACK provider behavior.
    pub ack: SettlementPortBehavior,
    /// Requeue NACK provider behavior after failed handoff.
    pub nack_requeue: SettlementPortBehavior,
    /// Return the opposite handoff receipt to test mismatch handling.
    pub mismatched_receipt: bool,
    /// Cancel before materialization begins.
    pub pre_cancelled: bool,
    /// Attempt a second materialization through the same authority.
    pub replay: bool,
}

/// Provider operations observed during one state-machine execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementCall {
    /// Publish the retry or dead-letter handoff.
    Handoff,
    /// ACK the original delivery.
    Ack,
    /// Requeue-NACK the original delivery.
    NackRequeue,
}

/// Stable terminal states exposed to the fuzz harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementStateTerminal {
    /// Original delivery ACKed after handler success.
    AckedSuccess,
    /// Retry handoff confirmed and original delivery ACKed.
    AckedRetry,
    /// Dead-letter handoff confirmed and original delivery ACKed.
    AckedDeadLetter,
    /// Failed handoff followed by a requeue NACK.
    NackRequeue,
    /// Original delivery remained unresolved.
    Unresolved,
}

/// Observations from the production settlement coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementStateSummary {
    /// First materialization terminal state.
    pub terminal: SettlementStateTerminal,
    /// Primary stable failure code, if any.
    pub failure_code: Option<&'static str>,
    /// Ordered provider call ledger.
    pub calls: Vec<SettlementCall>,
    /// Number of terminal observer callbacks.
    pub terminal_observations: usize,
    /// Replay failure code when a second caller was requested.
    pub replay_failure_code: Option<&'static str>,
}

struct FuzzSettlementPort {
    handoff: SettlementPortBehavior,
    ack: SettlementPortBehavior,
    nack_requeue: SettlementPortBehavior,
    mismatched_receipt: bool,
    calls: Vec<SettlementCall>,
}

fn fuzz_provider_error() -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Io(
        "fuzz settlement provider failure".to_string(),
    ))
}

fn materialize_boolean_behavior(
    behavior: SettlementPortBehavior,
) -> Result<bool, MessageBrokerError> {
    match behavior {
        SettlementPortBehavior::Accept => Ok(true),
        SettlementPortBehavior::Decline => Ok(false),
        SettlementPortBehavior::Error => Err(fuzz_provider_error()),
    }
}

#[async_trait]
impl SettlementPort for FuzzSettlementPort {
    async fn handoff(&mut self, plan: HandoffPlan) -> Result<HandoffReceipt, MessageBrokerError> {
        self.calls.push(SettlementCall::Handoff);
        if self.handoff != SettlementPortBehavior::Accept {
            return Err(fuzz_provider_error());
        }
        let retry = plan.destination() == HandoffDestination::Retry;
        Ok(match (retry, self.mismatched_receipt) {
            (true, false) | (false, true) => HandoffReceipt::RetryConfirmed,
            (false, false) | (true, true) => HandoffReceipt::DeadLetterConfirmed,
        })
    }

    async fn ack(&mut self) -> Result<bool, MessageBrokerError> {
        self.calls.push(SettlementCall::Ack);
        materialize_boolean_behavior(self.ack)
    }

    async fn nack_requeue(&mut self) -> Result<bool, MessageBrokerError> {
        self.calls.push(SettlementCall::NackRequeue);
        materialize_boolean_behavior(self.nack_requeue)
    }

    async fn nack_dead_letter(&mut self) -> Result<bool, MessageBrokerError> {
        unreachable!("delivery outcome materialization never uses broker-native dead-lettering")
    }
}

#[derive(Default)]
struct FuzzSettlementObserver {
    terminal_observations: usize,
}

impl SettlementObserver for FuzzSettlementObserver {
    fn terminal(&mut self, _terminal: SettlementTerminal) -> Result<(), SettlementObserverFailure> {
        self.terminal_observations += 1;
        Ok(())
    }
}

fn fuzz_execution_outcome(selector: u8) -> ExecutionOutcome {
    match selector % 4 {
        0 => ExecutionOutcome::Success,
        1 => ExecutionOutcome::Failure {
            class: FailureClass::Retryable,
            code: "QUEUE_FUZZ_RETRYABLE",
        },
        2 => ExecutionOutcome::Failure {
            class: FailureClass::Permanent,
            code: "QUEUE_FUZZ_PERMANENT",
        },
        _ => ExecutionOutcome::FrameworkCancelled,
    }
}

fn fuzz_terminal(terminal: SettlementTerminal) -> SettlementStateTerminal {
    match terminal {
        SettlementTerminal::AckedSuccess => SettlementStateTerminal::AckedSuccess,
        SettlementTerminal::AckedRetry => SettlementStateTerminal::AckedRetry,
        SettlementTerminal::AckedDeadLetter => SettlementStateTerminal::AckedDeadLetter,
        SettlementTerminal::NackRequeue => SettlementStateTerminal::NackRequeue,
        SettlementTerminal::NackDeadLetter => {
            unreachable!("delivery outcome materialization cannot broker-dead-letter")
        }
        SettlementTerminal::Unresolved => SettlementStateTerminal::Unresolved,
    }
}

/// Exercises the exact production settlement authority and coordinator.
pub async fn exercise_settlement_state(input: SettlementStateInput) -> SettlementStateSummary {
    let cancellation = CancellationToken::new();
    if input.pre_cancelled {
        cancellation.cancel();
    }
    let authority = SettlementAuthority::new();
    let mut observer = FuzzSettlementObserver::default();
    let mut port = FuzzSettlementPort {
        handoff: input.handoff,
        ack: input.ack,
        nack_requeue: input.nack_requeue,
        mismatched_receipt: input.mismatched_receipt,
        calls: Vec::new(),
    };
    let timeout = Duration::from_millis(10);
    let report = materialize_delivery(
        &mut port,
        &authority,
        &mut observer,
        fuzz_execution_outcome(input.outcome),
        input.current_retry_count,
        input.retry_attempts,
        &cancellation,
        timeout,
        timeout,
    )
    .await;
    let terminal = fuzz_terminal(report.terminal());
    let failure_code = report.failure().map(|failure| failure.stable_code());

    let replay_failure_code = if input.replay {
        let replay = materialize_delivery(
            &mut port,
            &authority,
            &mut observer,
            ExecutionOutcome::Success,
            input.current_retry_count,
            input.retry_attempts,
            &cancellation,
            timeout,
            timeout,
        )
        .await;
        replay.failure().map(|failure| failure.stable_code())
    } else {
        None
    };

    SettlementStateSummary {
        terminal,
        failure_code,
        calls: port.calls,
        terminal_observations: observer.terminal_observations,
        replay_failure_code,
    }
}

/// Whether the canonical envelope itself validates independently of body size.
#[must_use]
pub fn delivery_envelope_is_valid(properties: &BasicProperties) -> bool {
    fuzz_validate_delivery_envelope(properties).is_ok()
}

#[cfg(test)]
mod tests {
    use lapin::types::{AMQPValue, FieldTable};

    use super::*;

    fn canonical_properties() -> BasicProperties {
        let mut headers = FieldTable::default();
        headers.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString("11111111-1111-4111-8111-111111111111".into()),
        );
        headers.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("1".into()),
        );
        headers.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString("json".into()),
        );
        BasicProperties::default().with_headers(headers)
    }

    #[test]
    fn envelope_adapter_preserves_production_admission_order() {
        let valid = canonical_properties();
        assert_eq!(
            exercise_delivery_envelope(8, 8, &valid),
            DeliveryAdmissionSummary::Accepted
        );

        let malformed = BasicProperties::default();
        assert_eq!(
            exercise_delivery_envelope(9, 8, &malformed),
            DeliveryAdmissionSummary::PayloadTooLarge
        );
        assert_eq!(
            exercise_delivery_envelope(8, 8, &malformed),
            DeliveryAdmissionSummary::InvalidEnvelope
        );

        let mut excessive_headers = valid.headers().as_ref().unwrap().clone();
        for index in 0..64 {
            excessive_headers.insert(format!("application-{index}").into(), AMQPValue::Void);
        }
        let excessive = BasicProperties::default().with_headers(excessive_headers);
        assert_eq!(
            exercise_delivery_envelope(8, 8, &excessive),
            DeliveryAdmissionSummary::MetadataBoundsExceeded
        );
    }

    #[test]
    fn metadata_adapter_preserves_projection_invariants() {
        let properties = canonical_properties();
        let summary = exercise_amqp_metadata(&properties);
        assert!(summary.received_valid);
        assert!(summary.handoff_headroom_valid);
        assert!(summary.projected_valid);
        assert_eq!(summary.retry_count, Some(0));
        assert_eq!(summary.header_entries, Some(3));
    }

    #[tokio::test]
    async fn extractor_adapter_covers_every_selector_and_body_authority() {
        let parts = exercise_extractor_plan(0, "json", br#"{"value":1}"#).await;
        assert_eq!(parts.payload_kind, QueuePayloadKind::None);
        assert!(!parts.body_consumed);

        for (selector, kind, body, payload_kind) in [
            (
                1,
                "json",
                br#"{"value":1}"#.as_slice(),
                QueuePayloadKind::Json,
            ),
            (2, "text", b"hello".as_slice(), QueuePayloadKind::Text),
            (3, "binary", b"\0\xFF".as_slice(), QueuePayloadKind::Binary),
            (
                4,
                "application.raw",
                b"raw".as_slice(),
                QueuePayloadKind::Raw,
            ),
        ] {
            let summary = exercise_extractor_plan(selector, kind, body).await;
            assert_eq!(summary.payload_kind, payload_kind);
            assert!(summary.extraction.is_ok());
            assert!(summary.body_consumed);
        }

        let mismatch = exercise_extractor_plan(1, "text", br#"{"value":1}"#).await;
        assert_eq!(mismatch.extraction, Err("QUEUE_CONTENT_KIND_MISMATCH"));
        assert!(!mismatch.body_consumed);
    }

    #[tokio::test]
    async fn settlement_adapter_proves_exact_one_replay_authority() {
        let summary = exercise_settlement_state(SettlementStateInput {
            outcome: 1,
            current_retry_count: 0,
            retry_attempts: 1,
            handoff: SettlementPortBehavior::Accept,
            ack: SettlementPortBehavior::Accept,
            nack_requeue: SettlementPortBehavior::Accept,
            mismatched_receipt: false,
            pre_cancelled: false,
            replay: true,
        })
        .await;

        assert_eq!(summary.terminal, SettlementStateTerminal::AckedRetry);
        assert_eq!(
            summary.calls,
            vec![SettlementCall::Handoff, SettlementCall::Ack]
        );
        assert_eq!(summary.terminal_observations, 1);
        assert_eq!(
            summary.replay_failure_code,
            Some("QUEUE_SETTLEMENT_AUTHORITY_CONSUMED")
        );
    }
}
