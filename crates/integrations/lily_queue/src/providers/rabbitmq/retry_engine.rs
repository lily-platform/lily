use lapin::{
    Channel,
    message::Delivery,
    options::BasicPublishOptions,
    types::{AMQPValue, FieldTable, ShortString},
};
use opentelemetry::propagation::Injector;
use std::sync::Arc;
use tracing::Instrument;

use crate::{
    channel_manager_trait::ChannelManager,
    providers::rabbitmq::metadata::{
        MAX_W3C_TRACEPARENT_BYTES, MAX_W3C_TRACESTATE_BYTES, canonical_retry_count,
        validate_handoff_metadata_headroom,
    },
    retry_engine_trait::{RetryEngine, RetryHandoff},
    setting::MessageBrokerSetting,
    settlement::{HandoffDestination, HandoffPlan},
};
use lily_queue_client::await_publisher_confirm;

pub(crate) struct RabbitMQRetryEngine {
    channel_manager: Arc<dyn ChannelManager<Channel>>,
    settings: MessageBrokerSetting,
}

struct AmqpHeaderInjector<'a> {
    headers: &'a mut FieldTable,
    invalid: bool,
}

impl<'a> AmqpHeaderInjector<'a> {
    fn new(headers: &'a mut FieldTable) -> Self {
        Self {
            headers,
            invalid: false,
        }
    }

    fn finish(self) -> Result<(), &'static str> {
        if self.invalid {
            Err("trace propagation metadata is outside the bounded handoff contract")
        } else {
            Ok(())
        }
    }
}

impl Injector for AmqpHeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let max_bytes = match key {
            "traceparent" => MAX_W3C_TRACEPARENT_BYTES,
            "tracestate" => MAX_W3C_TRACESTATE_BYTES,
            _ => {
                self.invalid = true;
                return;
            }
        };
        if value.len() > max_bytes {
            self.invalid = true;
            return;
        }
        self.headers
            .insert(key.into(), AMQPValue::LongString(value.into()));
    }
}

fn materialize_handoff_metadata(
    headers: &mut FieldTable,
    plan: HandoffPlan,
) -> Result<(), &'static str> {
    let current_retry_count = canonical_retry_count(Some(headers))?;
    if current_retry_count != plan.current_retry_count() {
        return Err("settlement handoff retry count changed after policy materialization");
    }

    let retry_count = i32::try_from(plan.next_retry_count())
        .map_err(|_| "retry count exceeds the AMQP LongInt range")?;
    let delivery_attempt = i32::try_from(plan.delivery_attempt())
        .map_err(|_| "delivery attempt exceeds the AMQP LongInt range")?;

    headers.insert("x-retry-count".into(), AMQPValue::LongInt(retry_count));
    headers.insert(
        "x-lily-failure-delivery-attempt".into(),
        AMQPValue::LongInt(delivery_attempt),
    );
    headers.insert(
        "x-lily-rejection-code".into(),
        AMQPValue::LongString(plan.failure_code().into()),
    );

    Ok(())
}

fn canonical_retry_event_id(headers: &FieldTable) -> Result<&str, &'static str> {
    let event_id = match headers.inner().get("x-lily-event-id") {
        Some(AMQPValue::LongString(value)) if value.len() <= 36 => {
            std::str::from_utf8(value.as_bytes()).ok()
        }
        Some(AMQPValue::ShortString(value)) if value.as_str().len() <= 36 => Some(value.as_str()),
        _ => None,
    }
    .ok_or("retry routing requires a canonical x-lily-event-id")?;
    let parsed = uuid::Uuid::parse_str(event_id)
        .map_err(|_| "retry routing requires a canonical x-lily-event-id")?;
    if parsed.is_nil() || parsed.hyphenated().to_string() != event_id {
        return Err("retry routing requires a canonical x-lily-event-id");
    }
    Ok(event_id)
}

impl RabbitMQRetryEngine {
    pub(crate) fn new(
        channel_manager: Arc<dyn ChannelManager<Channel>>,
        settings: MessageBrokerSetting,
    ) -> Self {
        Self {
            channel_manager,
            settings,
        }
    }
}
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use lily_error::application::{MessageBrokerError, message_broker::RabbitMQError};

#[async_trait]
impl RetryEngine<Delivery> for RabbitMQRetryEngine {
    async fn retry(
        &self,
        _worker: &str,
        queue: &str,
        body: &[u8],
        delivery: &Delivery,
        plan: HandoffPlan,
        ct: CancellationToken,
    ) -> Result<RetryHandoff, MessageBrokerError> {
        let span = tracing::info_span!(
            "messaging.consume.retry",
            messaging.system = "rabbitmq",
            messaging.destination.name = %queue,
            lily.delivery_attempt = tracing::field::Empty,
            lily.handoff_outcome = tracing::field::Empty,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let result: Result<RetryHandoff, MessageBrokerError> = async {
            let mut headers = delivery
                .properties
                .headers()
                .as_ref()
                .cloned()
                .unwrap_or_else(FieldTable::default);

            let topology = self.settings.topology(queue)?;
            materialize_handoff_metadata(&mut headers, plan).map_err(|reason| {
                MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(reason.into()))
            })?;
            let retry_routing_key = if plan.destination() == HandoffDestination::Retry {
                let event_id = canonical_retry_event_id(&headers).map_err(|reason| {
                    MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(reason.into()))
                })?;
                Some(
                    topology
                        .retry_bucket(event_id, plan.next_retry_count())
                        .ok_or_else(|| {
                            MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(
                                format!("retry delay bucket is missing for queue {queue:?}"),
                            ))
                        })?
                        .routing_key
                        .clone(),
                )
            } else {
                None
            };
            tracing::Span::current()
                .record("lily.delivery_attempt", u64::from(plan.delivery_attempt()));

            let mut injector = AmqpHeaderInjector::new(&mut headers);
            lily_trace::inject_current_context(&mut injector);
            injector.finish().map_err(|reason| {
                MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(reason.into()))
            })?;

            // Admission reserves this exact framework-owned headroom. Verify
            // the materialized result before acquiring a channel so an internal
            // regression cannot publish metadata that its own consumer rejects.
            let props = delivery.properties.clone().with_headers(headers);
            validate_handoff_metadata_headroom(&props).map_err(|reason| {
                MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(reason.into()))
            })?;

            let ch = self
                .channel_manager
                .get_channel(&topology.main_exchange, ct.clone(), true)
                .await?;

            match plan.destination() {
                HandoffDestination::DeadLetter => {
                    let confirm = ch
                        .basic_publish(
                            ShortString::from(topology.dead_letter_exchange.clone()),
                            ShortString::from(topology.dead_letter_routing_key.clone()),
                            BasicPublishOptions {
                                mandatory: true,
                                ..Default::default()
                            },
                            body,
                            props,
                        )
                        .await
                        .map_err(|f| {
                            MessageBrokerError::RabbitMQError(RabbitMQError::General(f.to_string()))
                        })?;
                    await_publisher_confirm(confirm, self.settings.confirm_timeout, &ct).await?;
                    Ok(RetryHandoff::DeadLetterConfirmed)
                }
                HandoffDestination::Retry => {
                    let routing_key = retry_routing_key.ok_or_else(|| {
                        MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(format!(
                            "retry delay bucket is missing for queue {queue:?}"
                        )))
                    })?;

                    let confirm = ch
                        .basic_publish(
                            ShortString::from(topology.retry_exchange.clone()),
                            ShortString::from(routing_key),
                            BasicPublishOptions {
                                mandatory: true,
                                ..Default::default()
                            },
                            body,
                            props,
                        )
                        .await
                        .map_err(|f| {
                            MessageBrokerError::RabbitMQError(RabbitMQError::General(f.to_string()))
                        })?;
                    await_publisher_confirm(confirm, self.settings.confirm_timeout, &ct).await?;
                    Ok(RetryHandoff::RetryConfirmed)
                }
            }
        }
        .instrument(span.clone())
        .await;
        match &result {
            Ok(RetryHandoff::RetryConfirmed) => {
                span.record("lily.outcome", "success");
                span.record("lily.handoff_outcome", "retry_confirmed");
            }
            Ok(RetryHandoff::DeadLetterConfirmed) => {
                span.record("lily.outcome", "success");
                span.record("lily.handoff_outcome", "dlq_confirmed");
            }
            Err(error) => {
                span.record("lily.outcome", "error");
                span.record("lily.error_code", error.error_code());
                span.record("otel.status_code", "ERROR");
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        providers::rabbitmq::metadata::{
            MAX_AMQP_HEADER_ENTRIES, MAX_AMQP_HEADER_KEY_BYTES, MAX_AMQP_METADATA_BYTES,
            MAX_AMQP_VALUE_BYTES, amqp_metadata_footprint, projected_handoff_properties,
        },
        retry_engine_trait::FailureClass,
    };
    use lily_config::{QueueDefinition, QueueRetentionConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    const EVENT_ID: &str = "11111111-1111-4111-8111-111111111111";
    const MAX_SCHEMA_VERSION: &str = "65535";
    const CUSTOM_CONTENT_KIND: &str = "vendor.order-event+protobuf";
    const REJECTION_CODE: &str = "BROKER_INVALID_MESSAGE";
    const MAX_REJECTION_CODE: &str =
        "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

    fn event_headers() -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString(EVENT_ID.into()),
        );
        headers.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString("1".into()),
        );
        headers.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString("json".into()),
        );
        headers
    }

    fn versioned_custom_event_headers() -> FieldTable {
        let mut headers = event_headers();
        headers.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString(MAX_SCHEMA_VERSION.into()),
        );
        headers.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString(CUSTOM_CONTENT_KIND.into()),
        );
        headers
    }

    fn headers_with_retry_count(retry_count: i32) -> FieldTable {
        let mut headers = event_headers();
        headers.insert("x-retry-count".into(), AMQPValue::LongInt(retry_count));
        headers
    }

    fn long_int(headers: &FieldTable, key: &str) -> i32 {
        match headers.inner().get(key) {
            Some(AMQPValue::LongInt(value)) => *value,
            other => panic!("expected {key} LongInt header, got {other:?}"),
        }
    }

    fn long_string<'a>(headers: &'a FieldTable, key: &str) -> &'a [u8] {
        match headers.inner().get(key) {
            Some(AMQPValue::LongString(value)) => value.as_bytes(),
            other => panic!("expected {key} LongString header, got {other:?}"),
        }
    }

    fn assert_versioned_custom_identity(headers: &FieldTable) {
        assert_eq!(long_string(headers, "x-lily-event-id"), EVENT_ID.as_bytes());
        assert_eq!(
            long_string(headers, "x-lily-schema-version"),
            MAX_SCHEMA_VERSION.as_bytes()
        );
        assert_eq!(
            long_string(headers, "x-lily-content-kind"),
            CUSTOM_CONTENT_KIND.as_bytes()
        );
    }

    fn application_headers_at_exact_handoff_byte_boundary() -> FieldTable {
        let mut headers = event_headers();
        let fixed_footprint = amqp_metadata_footprint(&projected_handoff_properties(
            &lapin::BasicProperties::default().with_headers(headers.clone()),
        ))
        .expect("framework handoff metadata projection must be valid")
        .aggregate_bytes;
        let mut remaining = MAX_AMQP_METADATA_BYTES
            .checked_sub(fixed_footprint)
            .expect("framework handoff metadata must fit within its own bound");
        let mut index = 0usize;

        while remaining > 0 {
            if remaining <= MAX_AMQP_HEADER_KEY_BYTES {
                // A Void entry contributes only its key bytes, allowing the
                // aggregate boundary to be reached exactly without exceeding
                // the per-value limit.
                headers.insert("z".repeat(remaining).into(), AMQPValue::Void);
                remaining = 0;
                continue;
            }

            let key = format!("application-{index}");
            let key_len = key.len();
            let value_len = MAX_AMQP_VALUE_BYTES.min(
                remaining
                    .checked_sub(key_len)
                    .expect("remaining metadata must include the filler key"),
            );
            headers.insert(
                key.into(),
                AMQPValue::LongString(vec![b'a'; value_len].into()),
            );
            remaining -= key_len + value_len;
            index += 1;
        }

        headers
    }

    fn canonical_headers_without_handoff_entry_headroom() -> FieldTable {
        let mut headers = event_headers();
        // Sixty non-framework headers are valid in isolation, but replacing
        // the five framework-owned handoff headers projects sixty-five entries
        // and must therefore fail before broker I/O.
        let target_entries = MAX_AMQP_HEADER_ENTRIES - 4;
        for index in 0..target_entries - headers.inner().len() {
            headers.insert(format!("application-{index}").into(), AMQPValue::Void);
        }
        assert_eq!(headers.inner().len(), target_entries);
        headers
    }

    fn inject_maximum_trace_context(headers: &mut FieldTable) {
        let mut injector = AmqpHeaderInjector::new(headers);
        injector.set("traceparent", "0".repeat(MAX_W3C_TRACEPARENT_BYTES));
        injector.set("tracestate", "x".repeat(MAX_W3C_TRACESTATE_BYTES));
        injector
            .finish()
            .expect("exact trace propagation bounds must be accepted");
    }

    #[derive(Default)]
    struct CountingFailingChannelManager {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ChannelManager<Channel> for CountingFailingChannelManager {
        async fn get_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "test channel manager must not be reached".into(),
            )))
        }

        async fn get_dedicated_channel(
            &self,
            _worker: &str,
            _cancel_token: CancellationToken,
            _confirm_channel: bool,
        ) -> Result<Channel, MessageBrokerError> {
            unreachable!("retry tests must not acquire a dedicated consumer channel")
        }
    }

    fn test_broker_setting(queue: &str, worker: &str) -> MessageBrokerSetting {
        let definition = QueueDefinition {
            name: queue.into(),
            exchange_name: worker.into(),
            routing_key: queue.into(),
            retry_attempts: 1,
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100,
                main_max_bytes: 1024 * 1024,
                retry_bucket_max_messages: 100,
                retry_bucket_max_bytes: 1024 * 1024,
                dead_letter_max_messages: 100,
                dead_letter_max_bytes: 1024 * 1024,
            }),
            ..QueueDefinition::default()
        };
        MessageBrokerSetting::from_definitions(Duration::from_secs(1), &[definition])
            .expect("test broker setting")
    }

    #[test]
    fn permanent_handoff_keeps_original_attempt_and_event_id() {
        let mut headers = event_headers();

        let plan = HandoffPlan::for_failure(FailureClass::Permanent, REJECTION_CODE, 0, 2);
        materialize_handoff_metadata(&mut headers, plan).unwrap();

        assert_eq!(plan.destination(), HandoffDestination::DeadLetter);
        assert_eq!(plan.next_retry_count(), 0);
        assert_eq!(long_int(&headers, "x-retry-count"), 0);
        assert_eq!(long_int(&headers, "x-lily-failure-delivery-attempt"), 1);
        assert_eq!(
            long_string(&headers, "x-lily-event-id"),
            EVENT_ID.as_bytes()
        );
        assert_eq!(
            long_string(&headers, "x-lily-rejection-code"),
            REJECTION_CODE.as_bytes()
        );
    }

    #[test]
    fn retry_handoff_increments_retry_count_and_preserves_event_id() {
        for (current_retry_count, expected_retry_count, expected_attempt) in [(0, 1, 1), (1, 2, 2)]
        {
            let mut headers = headers_with_retry_count(current_retry_count);

            let plan = HandoffPlan::for_failure(
                FailureClass::Retryable,
                "BROKER_GENERAL",
                current_retry_count as u32,
                2,
            );
            materialize_handoff_metadata(&mut headers, plan).unwrap();

            assert_eq!(plan.destination(), HandoffDestination::Retry);
            assert_eq!(plan.next_retry_count(), expected_retry_count as u32);
            assert_eq!(plan.delivery_attempt(), expected_attempt as u32);
            assert_eq!(long_int(&headers, "x-retry-count"), expected_retry_count);
            assert_eq!(
                long_int(&headers, "x-lily-failure-delivery-attempt"),
                expected_attempt
            );
            assert_eq!(
                long_string(&headers, "x-lily-event-id"),
                EVENT_ID.as_bytes()
            );
        }
    }

    #[test]
    fn retry_then_terminal_dlq_preserves_exact_versioned_custom_dispatch_identity() {
        let mut headers = versioned_custom_event_headers();
        let original_identity = [
            long_string(&headers, "x-lily-event-id").to_vec(),
            long_string(&headers, "x-lily-schema-version").to_vec(),
            long_string(&headers, "x-lily-content-kind").to_vec(),
        ];

        let retry_plan =
            HandoffPlan::for_failure(FailureClass::Retryable, "HANDLER_RETRYABLE", 0, 1);
        materialize_handoff_metadata(&mut headers, retry_plan).unwrap();

        assert_eq!(retry_plan.destination(), HandoffDestination::Retry);
        assert_eq!(long_int(&headers, "x-retry-count"), 1);
        assert_eq!(long_int(&headers, "x-lily-failure-delivery-attempt"), 1);
        assert_versioned_custom_identity(&headers);
        validate_handoff_metadata_headroom(
            &lapin::BasicProperties::default().with_headers(headers.clone()),
        )
        .expect("the retry generation must remain admissible");

        let terminal_plan =
            HandoffPlan::for_failure(FailureClass::Retryable, "HANDLER_RETRY_EXHAUSTED", 1, 1);
        materialize_handoff_metadata(&mut headers, terminal_plan).unwrap();

        assert_eq!(terminal_plan.destination(), HandoffDestination::DeadLetter);
        assert_eq!(long_int(&headers, "x-retry-count"), 1);
        assert_eq!(long_int(&headers, "x-lily-failure-delivery-attempt"), 2);
        assert_eq!(
            long_string(&headers, "x-lily-rejection-code"),
            b"HANDLER_RETRY_EXHAUSTED"
        );
        assert_versioned_custom_identity(&headers);
        assert_eq!(
            [
                long_string(&headers, "x-lily-event-id").to_vec(),
                long_string(&headers, "x-lily-schema-version").to_vec(),
                long_string(&headers, "x-lily-content-kind").to_vec(),
            ],
            original_identity,
            "retry and terminal DLQ handoffs must not rewrite dispatch identity"
        );
        validate_handoff_metadata_headroom(
            &lapin::BasicProperties::default().with_headers(headers),
        )
        .expect("the terminal DLQ generation must remain admissible");
    }

    #[test]
    fn zero_retry_policy_dead_letters_the_first_retryable_failure() {
        let mut headers = event_headers();

        let plan = HandoffPlan::for_failure(FailureClass::Retryable, "HANDLER_ERROR", 0, 0);
        materialize_handoff_metadata(&mut headers, plan).unwrap();

        assert_eq!(plan.destination(), HandoffDestination::DeadLetter);
        assert_eq!(plan.next_retry_count(), 0);
        assert_eq!(long_int(&headers, "x-retry-count"), 0);
        assert_eq!(long_int(&headers, "x-lily-failure-delivery-attempt"), 1);
    }

    #[test]
    fn retry_exhaustion_records_exact_terminal_attempt_without_phantom_increment() {
        let mut headers = headers_with_retry_count(2);

        let plan = HandoffPlan::for_failure(FailureClass::Retryable, "HANDLER_TIMEOUT", 2, 2);
        materialize_handoff_metadata(&mut headers, plan).unwrap();

        assert_eq!(plan.destination(), HandoffDestination::DeadLetter);
        assert_eq!(plan.next_retry_count(), 2);
        assert_eq!(long_int(&headers, "x-retry-count"), 2);
        assert_eq!(long_int(&headers, "x-lily-failure-delivery-attempt"), 3);
        assert_eq!(
            long_string(&headers, "x-lily-event-id"),
            EVENT_ID.as_bytes()
        );
    }

    #[test]
    fn malformed_negative_and_out_of_policy_retry_counts_fail_before_mutation() {
        let invalid_counts = [
            AMQPValue::LongInt(-1),
            AMQPValue::LongInt(
                i32::try_from(crate::setting::MAX_QUEUE_RETRY_ATTEMPTS + 1).unwrap(),
            ),
            AMQPValue::LongString(b"1".to_vec().into()),
        ];

        for invalid_count in invalid_counts {
            let mut headers = event_headers();
            headers.insert("x-retry-count".into(), invalid_count);
            let original = headers.clone();
            let plan = HandoffPlan::for_failure(FailureClass::Retryable, "HANDLER_ERROR", 0, 2);

            assert!(materialize_handoff_metadata(&mut headers, plan).is_err());
            assert_eq!(headers, original, "failed planning must be atomic");
            assert!(!headers.inner().contains_key("x-lily-rejection-code"));
            assert!(
                !headers
                    .inner()
                    .contains_key("x-lily-failure-delivery-attempt")
            );
        }
    }

    #[test]
    fn materialization_rejects_a_delivery_that_no_longer_matches_the_coordinator_plan() {
        let mut headers = headers_with_retry_count(1);
        let original = headers.clone();
        let plan = HandoffPlan::for_failure(FailureClass::Retryable, "HANDLER_ERROR", 0, 2);

        assert_eq!(
            materialize_handoff_metadata(&mut headers, plan),
            Err("settlement handoff retry count changed after policy materialization")
        );
        assert_eq!(headers, original, "rejected plans must not mutate headers");
    }

    #[test]
    fn maximum_canonical_retry_count_is_materialized_without_signed_conversion_drift() {
        let maximum = crate::setting::MAX_QUEUE_RETRY_ATTEMPTS;
        let mut headers = headers_with_retry_count(i32::try_from(maximum).unwrap());

        let plan =
            HandoffPlan::for_failure(FailureClass::Retryable, "HANDLER_TIMEOUT", maximum, maximum);
        materialize_handoff_metadata(&mut headers, plan).unwrap();

        assert_eq!(plan.destination(), HandoffDestination::DeadLetter);
        assert_eq!(plan.next_retry_count(), maximum);
        assert_eq!(plan.delivery_attempt(), maximum + 1);
        assert_eq!(
            long_int(&headers, "x-retry-count"),
            i32::try_from(maximum).unwrap()
        );
        assert_eq!(
            long_int(&headers, "x-lily-failure-delivery-attempt"),
            i32::try_from(maximum + 1).unwrap()
        );
    }

    #[test]
    fn trace_injector_accepts_exact_bounds_and_rejects_unknown_or_overlong_fields() {
        let mut exact = FieldTable::default();
        inject_maximum_trace_context(&mut exact);
        assert_eq!(
            long_string(&exact, "traceparent").len(),
            MAX_W3C_TRACEPARENT_BYTES
        );
        assert_eq!(
            long_string(&exact, "tracestate").len(),
            MAX_W3C_TRACESTATE_BYTES
        );

        for (key, value) in [
            ("baggage", "bounded".to_owned()),
            ("traceparent", "0".repeat(MAX_W3C_TRACEPARENT_BYTES + 1)),
            ("tracestate", "x".repeat(MAX_W3C_TRACESTATE_BYTES + 1)),
        ] {
            let mut headers = FieldTable::default();
            let mut injector = AmqpHeaderInjector::new(&mut headers);
            injector.set(key, value);
            assert!(injector.finish().is_err());
            assert!(
                !headers.inner().contains_key(key),
                "invalid propagation fields must not be retained"
            );
        }
    }

    #[test]
    fn exact_handoff_metadata_boundary_survives_full_materialization() {
        let mut headers = application_headers_at_exact_handoff_byte_boundary();
        let admitted = lapin::BasicProperties::default().with_headers(headers.clone());
        validate_handoff_metadata_headroom(&admitted)
            .expect("exact projected handoff boundary must be admitted");

        let plan = HandoffPlan::for_failure(FailureClass::Retryable, MAX_REJECTION_CODE, 0, 1);
        materialize_handoff_metadata(&mut headers, plan).unwrap();
        inject_maximum_trace_context(&mut headers);

        let materialized = lapin::BasicProperties::default().with_headers(headers);
        let footprint = amqp_metadata_footprint(&materialized)
            .expect("fully materialized exact-boundary handoff must remain valid");
        assert_eq!(footprint.aggregate_bytes, MAX_AMQP_METADATA_BYTES);
        validate_handoff_metadata_headroom(&materialized)
            .expect("post-materialization guard must accept the exact boundary");
        let materialized_headers = materialized
            .headers()
            .as_ref()
            .expect("materialized canonical envelope headers");
        assert_eq!(
            long_string(materialized_headers, "x-lily-event-id"),
            EVENT_ID.as_bytes()
        );
        assert_eq!(
            long_string(materialized_headers, "x-lily-schema-version"),
            b"1"
        );
        assert_eq!(
            long_string(materialized_headers, "x-lily-content-kind"),
            b"json"
        );
        assert_eq!(plan.destination(), HandoffDestination::Retry);
        assert_eq!(plan.next_retry_count(), 1);
        assert_eq!(plan.delivery_attempt(), 1);
    }

    #[test]
    fn post_materialization_guard_rejects_metadata_without_reserved_entry_headroom() {
        let mut headers = canonical_headers_without_handoff_entry_headroom();
        let unmaterialized = lapin::BasicProperties::default().with_headers(headers.clone());
        assert!(amqp_metadata_footprint(&unmaterialized).is_ok());
        assert!(validate_handoff_metadata_headroom(&unmaterialized).is_err());

        let plan = HandoffPlan::for_failure(FailureClass::Permanent, REJECTION_CODE, 0, 0);
        materialize_handoff_metadata(&mut headers, plan).unwrap();
        inject_maximum_trace_context(&mut headers);
        let materialized = lapin::BasicProperties::default().with_headers(headers);

        assert!(validate_handoff_metadata_headroom(&materialized).is_err());
    }

    #[tokio::test]
    async fn retry_rejects_invalid_materialized_metadata_before_channel_acquisition() {
        let worker = "OrderWorker";
        let queue = "orders";
        let manager = Arc::new(CountingFailingChannelManager::default());
        let channel_manager: Arc<dyn ChannelManager<Channel>> = manager.clone();
        let retry_engine =
            RabbitMQRetryEngine::new(channel_manager, test_broker_setting(queue, worker));
        let mut delivery = Delivery::mock(
            1,
            ShortString::from(worker),
            ShortString::from(queue),
            false,
            Vec::new(),
        );
        delivery.properties = lapin::BasicProperties::default()
            .with_headers(canonical_headers_without_handoff_entry_headroom());
        assert!(amqp_metadata_footprint(&delivery.properties).is_ok());
        let plan = HandoffPlan::for_failure(FailureClass::Permanent, REJECTION_CODE, 0, 0);

        let error = retry_engine
            .retry(
                worker,
                queue,
                &delivery.data,
                &delivery,
                plan,
                CancellationToken::new(),
            )
            .await
            .expect_err("invalid materialized metadata must fail before broker I/O");

        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(reason))
                if reason == "amqp_metadata_bounds_exceeded"
        ));
        assert_eq!(
            manager.calls.load(Ordering::SeqCst),
            0,
            "metadata rejection must precede channel acquisition"
        );
    }

    #[tokio::test]
    async fn retry_bucket_selection_requires_canonical_event_identity_before_channel_io() {
        let worker = "OrderWorker";
        let queue = "orders";
        for event_id in [None, Some("00000000-0000-0000-0000-000000000000")] {
            let manager = Arc::new(CountingFailingChannelManager::default());
            let channel_manager: Arc<dyn ChannelManager<Channel>> = manager.clone();
            let retry_engine =
                RabbitMQRetryEngine::new(channel_manager, test_broker_setting(queue, worker));
            let mut headers = FieldTable::default();
            headers.insert(
                "x-lily-schema-version".into(),
                AMQPValue::LongString("1".into()),
            );
            headers.insert(
                "x-lily-content-kind".into(),
                AMQPValue::LongString("json".into()),
            );
            if let Some(event_id) = event_id {
                headers.insert(
                    "x-lily-event-id".into(),
                    AMQPValue::LongString(event_id.into()),
                );
            }
            let mut delivery = Delivery::mock(
                1,
                ShortString::from(worker),
                ShortString::from(queue),
                false,
                Vec::new(),
            );
            delivery.properties = lapin::BasicProperties::default().with_headers(headers);
            let plan = HandoffPlan::for_failure(FailureClass::Retryable, REJECTION_CODE, 0, 1);

            let error = retry_engine
                .retry(
                    worker,
                    queue,
                    &delivery.data,
                    &delivery,
                    plan,
                    CancellationToken::new(),
                )
                .await
                .expect_err("invalid event identity must fail before retry routing");

            assert!(matches!(
                error,
                MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(reason))
                    if reason == "retry routing requires a canonical x-lily-event-id"
            ));
            assert_eq!(manager.calls.load(Ordering::SeqCst), 0);
        }
    }
}
