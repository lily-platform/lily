#![no_main]

use libfuzzer_sys::fuzz_target;
use lily_http_api::__private::fuzzing::{exercise_middleware_chain, MiddlewareDecision};
use std::sync::OnceLock;

const MAX_CHAIN_INPUT_BYTES: usize = 64;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fuzz runtime must build")
    })
}

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_CHAIN_INPUT_BYTES {
        return;
    }
    let decisions = input
        .iter()
        .copied()
        .map(|byte| {
            if byte == b'P' || byte == u8::MAX {
                MiddlewareDecision::Pending
            } else {
                match byte % 5 {
                    0 => MiddlewareDecision::Continue,
                    1 => MiddlewareDecision::ShortCircuit,
                    2 => MiddlewareDecision::Reject,
                    3 => MiddlewareDecision::ErrorBeforeNext,
                    _ => MiddlewareDecision::ErrorAfterNext,
                }
            }
        })
        .collect();
    let report = runtime().block_on(exercise_middleware_chain(decisions));
    assert!(report.terminal_calls <= 1);
    assert!(report.error_writes <= 1);
    assert!((100..=599).contains(&report.status));
});
