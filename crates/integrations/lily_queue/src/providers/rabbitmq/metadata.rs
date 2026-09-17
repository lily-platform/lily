use lapin::{
    BasicProperties,
    types::{AMQPValue, FieldTable},
};

pub(super) const MAX_AMQP_HEADER_ENTRIES: usize = 64;
pub(super) const MAX_AMQP_HEADER_KEY_BYTES: usize = 128;
pub(super) const MAX_AMQP_VALUE_BYTES: usize = 8 * 1024;
pub(super) const MAX_AMQP_METADATA_BYTES: usize = 32 * 1024;
pub(super) const MAX_AMQP_NESTING_DEPTH: usize = 4;
pub(super) const MAX_AMQP_NESTED_ELEMENTS: usize = 256;

pub(super) const MAX_W3C_TRACEPARENT_BYTES: usize = 55;
pub(super) const MAX_W3C_TRACESTATE_BYTES: usize = 512;
const MAX_REJECTION_CODE_BYTES: usize = 64;

const FRAMEWORK_HANDOFF_HEADERS: [&str; 5] = [
    "x-retry-count",
    "x-lily-failure-delivery-attempt",
    "x-lily-rejection-code",
    "traceparent",
    "tracestate",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AmqpMetadataFootprint {
    pub(super) header_entries: usize,
    pub(super) aggregate_bytes: usize,
}

fn checked_metadata_add(total: &mut usize, amount: usize) -> Result<(), &'static str> {
    *total = total
        .checked_add(amount)
        .ok_or("amqp_metadata_bounds_exceeded")?;
    if *total > MAX_AMQP_METADATA_BYTES {
        return Err("amqp_metadata_bounds_exceeded");
    }
    Ok(())
}

fn validate_amqp_key(key: &str) -> Result<(), &'static str> {
    if key.is_empty()
        || key.len() > MAX_AMQP_HEADER_KEY_BYTES
        || !key.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err("amqp_metadata_bounds_exceeded");
    }
    Ok(())
}

fn amqp_value_size(
    value: &AMQPValue,
    depth: usize,
    elements: &mut usize,
) -> Result<usize, &'static str> {
    *elements = elements
        .checked_add(1)
        .ok_or("amqp_metadata_bounds_exceeded")?;
    if *elements > MAX_AMQP_NESTED_ELEMENTS {
        return Err("amqp_metadata_bounds_exceeded");
    }

    let size = match value {
        AMQPValue::Boolean(_) | AMQPValue::ShortShortInt(_) | AMQPValue::ShortShortUInt(_) => 1,
        AMQPValue::ShortInt(_) | AMQPValue::ShortUInt(_) => 2,
        AMQPValue::LongInt(_) | AMQPValue::LongUInt(_) | AMQPValue::Float(_) => 4,
        AMQPValue::LongLongInt(_) | AMQPValue::Double(_) | AMQPValue::Timestamp(_) => 8,
        AMQPValue::DecimalValue(_) => 5,
        AMQPValue::ShortString(value) => value.as_str().len(),
        AMQPValue::LongString(value) => value.len(),
        AMQPValue::ByteArray(value) => value.len(),
        AMQPValue::Void => 0,
        AMQPValue::FieldArray(values) => {
            if depth > MAX_AMQP_NESTING_DEPTH {
                return Err("amqp_metadata_bounds_exceeded");
            }
            let mut size = 0usize;
            for value in values.as_slice() {
                size = size
                    .checked_add(amqp_value_size(value, depth + 1, elements)?)
                    .ok_or("amqp_metadata_bounds_exceeded")?;
            }
            size
        }
        AMQPValue::FieldTable(table) => {
            if depth > MAX_AMQP_NESTING_DEPTH {
                return Err("amqp_metadata_bounds_exceeded");
            }
            let mut size = 0usize;
            for (key, value) in table.inner() {
                validate_amqp_key(key.as_str())?;
                let value_size = amqp_value_size(value, depth + 1, elements)?;
                size = size
                    .checked_add(key.as_str().len())
                    .and_then(|size| size.checked_add(value_size))
                    .ok_or("amqp_metadata_bounds_exceeded")?;
            }
            size
        }
    };
    if size > MAX_AMQP_VALUE_BYTES {
        return Err("amqp_metadata_bounds_exceeded");
    }
    Ok(size)
}

pub(super) fn amqp_metadata_footprint(
    properties: &BasicProperties,
) -> Result<AmqpMetadataFootprint, &'static str> {
    let mut aggregate = 0usize;
    let mut elements = 0usize;
    let header_entries = properties
        .headers()
        .as_ref()
        .map_or(0, |headers| headers.inner().len());
    if header_entries > MAX_AMQP_HEADER_ENTRIES {
        return Err("amqp_metadata_bounds_exceeded");
    }
    if let Some(headers) = properties.headers().as_ref() {
        for (key, value) in headers.inner() {
            validate_amqp_key(key.as_str())?;
            checked_metadata_add(&mut aggregate, key.as_str().len())?;
            checked_metadata_add(&mut aggregate, amqp_value_size(value, 1, &mut elements)?)?;
        }
    }

    for property in [
        properties.content_type().as_ref(),
        properties.content_encoding().as_ref(),
        properties.correlation_id().as_ref(),
        properties.reply_to().as_ref(),
        properties.expiration().as_ref(),
        properties.message_id().as_ref(),
        properties.kind().as_ref(),
        properties.user_id().as_ref(),
        properties.app_id().as_ref(),
        properties.cluster_id().as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        if property.as_str().len() > MAX_AMQP_VALUE_BYTES {
            return Err("amqp_metadata_bounds_exceeded");
        }
        checked_metadata_add(&mut aggregate, property.as_str().len())?;
    }
    if properties.delivery_mode().is_some() {
        checked_metadata_add(&mut aggregate, 1)?;
    }
    if properties.priority().is_some() {
        checked_metadata_add(&mut aggregate, 1)?;
    }
    if properties.timestamp().is_some() {
        checked_metadata_add(&mut aggregate, 8)?;
    }

    Ok(AmqpMetadataFootprint {
        header_entries,
        aggregate_bytes: aggregate,
    })
}

pub(crate) fn validate_amqp_metadata(properties: &BasicProperties) -> Result<(), &'static str> {
    amqp_metadata_footprint(properties).map(|_| ())
}

fn is_framework_handoff_header(key: &str) -> bool {
    FRAMEWORK_HANDOFF_HEADERS.contains(&key)
}

pub(super) fn projected_handoff_properties(properties: &BasicProperties) -> BasicProperties {
    let mut projected = FieldTable::default();
    if let Some(headers) = properties.headers().as_ref() {
        for (key, value) in headers.inner() {
            if !is_framework_handoff_header(key.as_str()) {
                projected.insert(key.clone(), value.clone());
            }
        }
    }

    projected.insert("x-retry-count".into(), AMQPValue::LongInt(i32::MAX));
    projected.insert(
        "x-lily-failure-delivery-attempt".into(),
        AMQPValue::LongInt(i32::MAX),
    );
    projected.insert(
        "x-lily-rejection-code".into(),
        AMQPValue::LongString(vec![b'X'; MAX_REJECTION_CODE_BYTES].into()),
    );
    projected.insert(
        "traceparent".into(),
        AMQPValue::LongString(vec![b'0'; MAX_W3C_TRACEPARENT_BYTES].into()),
    );
    projected.insert(
        "tracestate".into(),
        AMQPValue::LongString(vec![b'x'; MAX_W3C_TRACESTATE_BYTES].into()),
    );

    properties.clone().with_headers(projected)
}

/// Validate both the received metadata and the worst-case metadata retained by
/// a retry or dead-letter handoff. Existing framework-owned headers are
/// replaced in the projection, so every retry generation consumes the same
/// fixed headroom instead of shrinking the application budget repeatedly.
pub(super) fn validate_handoff_metadata_headroom(
    properties: &BasicProperties,
) -> Result<(), &'static str> {
    validate_amqp_metadata(properties)?;
    validate_amqp_metadata(&projected_handoff_properties(properties))
}

/// Canonical retry count shared by admission, handler context and handoff
/// planning. Missing means the first delivery; malformed or negative values
/// are never silently reinterpreted as a new attempt.
pub(super) fn canonical_retry_count(headers: Option<&FieldTable>) -> Result<u32, &'static str> {
    match headers.and_then(|headers| headers.inner().get("x-retry-count")) {
        None => Ok(0),
        Some(AMQPValue::LongInt(value)) => u32::try_from(*value)
            .ok()
            .filter(|value| *value <= crate::setting::MAX_QUEUE_RETRY_ATTEMPTS)
            .ok_or("x-retry-count is outside the canonical retry range"),
        Some(_) => Err("x-retry-count is not a canonical LongInt"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVENT_ID: &str = "11111111-1111-4111-8111-111111111111";
    const SCHEMA_VERSION: &str = "1";
    const CONTENT_KIND: &str = "json";

    fn canonical_headers() -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert(
            "x-lily-event-id".into(),
            AMQPValue::LongString(EVENT_ID.into()),
        );
        headers.insert(
            "x-lily-schema-version".into(),
            AMQPValue::LongString(SCHEMA_VERSION.into()),
        );
        headers.insert(
            "x-lily-content-kind".into(),
            AMQPValue::LongString(CONTENT_KIND.into()),
        );
        headers
    }

    fn canonical_headers_with_entry_filler(count: usize) -> FieldTable {
        let mut headers = canonical_headers();
        for index in 0..count {
            headers.insert(format!("application-{index}").into(), AMQPValue::Void);
        }
        headers
    }

    fn canonical_headers_with_aggregate_filler(mut target: usize) -> FieldTable {
        let mut headers = canonical_headers();
        let mut index = 0_u8;

        while target != 0 {
            assert!(
                index < 26,
                "aggregate boundary fixture exhausted its one-byte keys"
            );
            let key = char::from(b'a' + index).to_string();
            let value_len = target.saturating_sub(key.len()).min(MAX_AMQP_VALUE_BYTES);
            assert!(
                value_len != 0,
                "aggregate fixture must reserve one byte for a non-empty value"
            );
            headers.insert(
                key.clone().into(),
                AMQPValue::LongString(vec![b'x'; value_len].into()),
            );
            target -= key.len() + value_len;
            index += 1;
        }

        headers
    }

    fn assert_canonical_headers_survive_projection(properties: &BasicProperties) {
        let headers = properties
            .headers()
            .as_ref()
            .expect("projected canonical delivery must retain headers");
        assert_eq!(
            headers.inner().get("x-lily-event-id"),
            Some(&AMQPValue::LongString(EVENT_ID.into()))
        );
        assert_eq!(
            headers.inner().get("x-lily-schema-version"),
            Some(&AMQPValue::LongString(SCHEMA_VERSION.into()))
        );
        assert_eq!(
            headers.inner().get("x-lily-content-kind"),
            Some(&AMQPValue::LongString(CONTENT_KIND.into()))
        );
    }

    fn retry_headers(value: AMQPValue) -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert("x-retry-count".into(), value);
        headers
    }

    #[test]
    fn projected_header_count_accepts_exact_maximum_and_rejects_maximum_plus_one() {
        let canonical_entry_count = canonical_headers().inner().len();
        let filler_entry_budget = MAX_AMQP_HEADER_ENTRIES
            .checked_sub(FRAMEWORK_HANDOFF_HEADERS.len())
            .and_then(|remaining| remaining.checked_sub(canonical_entry_count))
            .unwrap();
        let exact = BasicProperties::default()
            .with_headers(canonical_headers_with_entry_filler(filler_entry_budget));
        let projected_exact = projected_handoff_properties(&exact);

        assert!(validate_handoff_metadata_headroom(&exact).is_ok());
        assert_eq!(
            amqp_metadata_footprint(&projected_exact)
                .unwrap()
                .header_entries,
            MAX_AMQP_HEADER_ENTRIES
        );
        assert_canonical_headers_survive_projection(&projected_exact);

        let above = BasicProperties::default()
            .with_headers(canonical_headers_with_entry_filler(filler_entry_budget + 1));
        assert!(validate_amqp_metadata(&above).is_ok());
        assert_eq!(
            validate_handoff_metadata_headroom(&above),
            Err("amqp_metadata_bounds_exceeded")
        );
    }

    #[test]
    fn projected_aggregate_accepts_exact_maximum_and_rejects_maximum_plus_one() {
        let canonical = BasicProperties::default().with_headers(canonical_headers());
        let canonical_footprint = amqp_metadata_footprint(&canonical).unwrap();
        let canonical_projection_footprint =
            amqp_metadata_footprint(&projected_handoff_properties(&canonical)).unwrap();
        let filler_byte_budget = MAX_AMQP_METADATA_BYTES
            .checked_sub(canonical_projection_footprint.aggregate_bytes)
            .unwrap();
        let exact = BasicProperties::default()
            .with_headers(canonical_headers_with_aggregate_filler(filler_byte_budget));
        let projected_exact = projected_handoff_properties(&exact);

        assert_eq!(
            amqp_metadata_footprint(&exact).unwrap().aggregate_bytes,
            canonical_footprint.aggregate_bytes + filler_byte_budget
        );
        assert!(validate_handoff_metadata_headroom(&exact).is_ok());
        assert_eq!(
            amqp_metadata_footprint(&projected_exact)
                .unwrap()
                .aggregate_bytes,
            MAX_AMQP_METADATA_BYTES
        );
        assert_canonical_headers_survive_projection(&projected_exact);

        let above = BasicProperties::default().with_headers(
            canonical_headers_with_aggregate_filler(filler_byte_budget + 1),
        );
        assert!(validate_amqp_metadata(&above).is_ok());
        assert_eq!(
            validate_handoff_metadata_headroom(&above),
            Err("amqp_metadata_bounds_exceeded")
        );
    }

    #[test]
    fn projection_replaces_framework_headers_and_is_stable_across_generations() {
        let mut application_headers = FieldTable::default();
        application_headers.insert(
            "application-header".into(),
            AMQPValue::LongString("preserved".into()),
        );
        let application = BasicProperties::default().with_headers(application_headers.clone());
        let first_generation = projected_handoff_properties(&application);

        let mut existing_framework_headers = application_headers;
        existing_framework_headers.insert("x-retry-count".into(), AMQPValue::LongInt(2));
        existing_framework_headers.insert(
            "x-lily-failure-delivery-attempt".into(),
            AMQPValue::LongInt(3),
        );
        existing_framework_headers.insert(
            "x-lily-rejection-code".into(),
            AMQPValue::LongString("OLD_CODE".into()),
        );
        existing_framework_headers.insert(
            "traceparent".into(),
            AMQPValue::LongString("old-parent".into()),
        );
        existing_framework_headers.insert(
            "tracestate".into(),
            AMQPValue::LongString("old=state".into()),
        );
        let existing = BasicProperties::default().with_headers(existing_framework_headers);
        let replaced_generation = projected_handoff_properties(&existing);
        let next_generation = projected_handoff_properties(&first_generation);

        let first_headers = first_generation.headers().as_ref().unwrap().inner();
        let replaced_headers = replaced_generation.headers().as_ref().unwrap().inner();
        let next_headers = next_generation.headers().as_ref().unwrap().inner();
        assert_eq!(first_headers, replaced_headers);
        assert_eq!(first_headers, next_headers);
        assert_eq!(
            first_headers.len(),
            1 + FRAMEWORK_HANDOFF_HEADERS.len(),
            "framework-owned headers must replace, not accumulate"
        );

        let first_footprint = amqp_metadata_footprint(&first_generation).unwrap();
        assert_eq!(
            first_footprint,
            amqp_metadata_footprint(&replaced_generation).unwrap()
        );
        assert_eq!(
            first_footprint,
            amqp_metadata_footprint(&next_generation).unwrap()
        );
    }

    #[test]
    fn canonical_retry_count_accepts_only_the_bounded_long_int_contract() {
        assert_eq!(canonical_retry_count(None), Ok(0));
        assert_eq!(canonical_retry_count(Some(&FieldTable::default())), Ok(0));

        for value in [0, 99, 100] {
            let headers = retry_headers(AMQPValue::LongInt(value));
            assert_eq!(canonical_retry_count(Some(&headers)), Ok(value as u32));
        }

        for value in [-1, 101, i32::MAX] {
            let headers = retry_headers(AMQPValue::LongInt(value));
            assert_eq!(
                canonical_retry_count(Some(&headers)),
                Err("x-retry-count is outside the canonical retry range")
            );
        }

        for value in [
            AMQPValue::Boolean(false),
            AMQPValue::ShortInt(0),
            AMQPValue::LongUInt(0),
            AMQPValue::LongLongInt(0),
            AMQPValue::LongString("0".into()),
            AMQPValue::Void,
        ] {
            let headers = retry_headers(value);
            assert_eq!(
                canonical_retry_count(Some(&headers)),
                Err("x-retry-count is not a canonical LongInt")
            );
        }
    }
}
