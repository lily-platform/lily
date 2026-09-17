#![no_main]

mod amqp_input;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use lily_queue::fuzzing::{
    delivery_envelope_is_valid, exercise_amqp_metadata, exercise_delivery_envelope,
    DeliveryAdmissionSummary,
};
use serde::Deserialize;

use amqp_input::PropertiesInput;

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
struct EnvelopeInput {
    body_len: usize,
    max_message_size_bytes: usize,
    properties: PropertiesInput,
}

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let Some((&encoding, bytes)) = bytes.split_first() else {
        return;
    };
    let input = if encoding == b'j' {
        serde_json::from_slice::<EnvelopeInput>(bytes).ok()
    } else {
        EnvelopeInput::arbitrary(&mut Unstructured::new(bytes)).ok()
    };
    let Some(input) = input else {
        return;
    };
    let properties = input.properties.into_properties();
    let metadata = exercise_amqp_metadata(&properties);
    let result =
        exercise_delivery_envelope(input.body_len, input.max_message_size_bytes, &properties);

    match result {
        DeliveryAdmissionSummary::Accepted => {
            assert!(input.body_len <= input.max_message_size_bytes);
            assert!(metadata.handoff_headroom_valid);
            assert!(delivery_envelope_is_valid(&properties));
        }
        DeliveryAdmissionSummary::PayloadTooLarge => {
            assert!(input.body_len > input.max_message_size_bytes);
        }
        DeliveryAdmissionSummary::MetadataBoundsExceeded => {
            assert!(input.body_len <= input.max_message_size_bytes);
            assert!(!metadata.handoff_headroom_valid);
        }
        DeliveryAdmissionSummary::InvalidEnvelope => {
            assert!(input.body_len <= input.max_message_size_bytes);
            assert!(metadata.handoff_headroom_valid);
            assert!(!delivery_envelope_is_valid(&properties));
        }
    }
});
