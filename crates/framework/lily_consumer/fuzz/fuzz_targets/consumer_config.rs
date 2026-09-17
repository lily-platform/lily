#![no_main]

use libfuzzer_sys::fuzz_target;
use lily_config::LilyConfig;
use lily_consumer::fuzzing::validate_consumer_config;

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let Some((&selector, document)) = input.split_first() else {
        return;
    };

    let config = if selector == b'j' {
        serde_json::from_slice::<LilyConfig>(document).ok()
    } else {
        std::str::from_utf8(document)
            .ok()
            .and_then(|document| toml::from_str::<LilyConfig>(document).ok())
    };
    let Some(config) = config else {
        return;
    };
    if config.rabbitmq.consumer.is_none() {
        return;
    }

    let _ = validate_consumer_config(&config.rabbitmq.topology.queues);
});
