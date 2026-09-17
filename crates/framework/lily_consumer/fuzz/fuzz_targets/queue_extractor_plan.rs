#![no_main]

use std::sync::OnceLock;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use lily_queue::{fuzzing::exercise_extractor_plan, QueuePayloadKind};
use serde::Deserialize;

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
struct ExtractorInput {
    selector: u8,
    content_kind: String,
    body: Vec<u8>,
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("bounded queue extractor fuzz runtime must build")
    })
}

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let Some((&encoding, bytes)) = bytes.split_first() else {
        return;
    };
    let input = if encoding == b'j' {
        serde_json::from_slice::<ExtractorInput>(bytes).ok()
    } else {
        ExtractorInput::arbitrary(&mut Unstructured::new(bytes)).ok()
    };
    let Some(input) = input else {
        return;
    };
    let selector = input.selector % 5;
    let summary = runtime().block_on(exercise_extractor_plan(
        selector,
        &input.content_kind,
        &input.body,
    ));
    let expected_kind = match selector {
        0 => QueuePayloadKind::None,
        1 => QueuePayloadKind::Json,
        2 => QueuePayloadKind::Text,
        3 => QueuePayloadKind::Binary,
        _ => QueuePayloadKind::Raw,
    };

    assert_eq!(summary.payload_kind, expected_kind);
    assert_eq!(summary.has_payload_type, selector != 0);
    if selector == 0 {
        assert_eq!(summary.extraction, Ok(0));
        assert!(!summary.body_consumed);
    }
    if summary.extraction.is_ok() && selector != 0 {
        assert!(summary.body_consumed);
    }
});
