#![no_main]

mod amqp_input;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use lily_queue::fuzzing::exercise_amqp_metadata;
use serde::Deserialize;

use amqp_input::PropertiesInput;

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
struct MetadataInput {
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
        serde_json::from_slice::<MetadataInput>(bytes).ok()
    } else {
        MetadataInput::arbitrary(&mut Unstructured::new(bytes)).ok()
    };
    let Some(input) = input else {
        return;
    };
    let properties = input.properties.into_properties();
    let first = exercise_amqp_metadata(&properties);
    let second = exercise_amqp_metadata(&properties);

    assert_eq!(first, second);
    assert_eq!(first.received_valid, first.header_entries.is_some());
    assert_eq!(first.received_valid, first.aggregate_bytes.is_some());
    if first.handoff_headroom_valid {
        assert!(first.received_valid);
        assert!(first.projected_valid);
    }
});
