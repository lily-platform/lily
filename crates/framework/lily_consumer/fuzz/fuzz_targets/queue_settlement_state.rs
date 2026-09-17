#![no_main]

use std::sync::OnceLock;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use lily_queue::fuzzing::{
    exercise_settlement_state, SettlementCall, SettlementPortBehavior, SettlementStateInput,
};
use serde::Deserialize;

const MAX_FUZZ_INPUT_BYTES: usize = 256;

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
struct StateInput {
    outcome: u8,
    current_retry_count: u32,
    retry_attempts: u32,
    handoff: u8,
    ack: u8,
    nack_requeue: u8,
    mismatched_receipt: bool,
    pre_cancelled: bool,
    replay: bool,
}

fn behavior(value: u8) -> SettlementPortBehavior {
    match value % 3 {
        0 => SettlementPortBehavior::Accept,
        1 => SettlementPortBehavior::Decline,
        _ => SettlementPortBehavior::Error,
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("bounded queue settlement fuzz runtime must build")
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
        serde_json::from_slice::<StateInput>(bytes).ok()
    } else {
        StateInput::arbitrary(&mut Unstructured::new(bytes)).ok()
    };
    let Some(input) = input else {
        return;
    };
    let replay = input.replay;
    let summary = runtime().block_on(exercise_settlement_state(SettlementStateInput {
        outcome: input.outcome,
        current_retry_count: input.current_retry_count,
        retry_attempts: input.retry_attempts,
        handoff: behavior(input.handoff),
        ack: behavior(input.ack),
        nack_requeue: behavior(input.nack_requeue),
        mismatched_receipt: input.mismatched_receipt,
        pre_cancelled: input.pre_cancelled,
        replay,
    }));

    assert!(summary.calls.len() <= 2);
    for operation in [
        SettlementCall::Handoff,
        SettlementCall::Ack,
        SettlementCall::NackRequeue,
    ] {
        assert!(
            summary
                .calls
                .iter()
                .filter(|call| **call == operation)
                .count()
                <= 1
        );
    }
    assert_eq!(summary.terminal_observations, 1);
    assert_eq!(
        summary.replay_failure_code,
        replay.then_some("QUEUE_SETTLEMENT_AUTHORITY_CONSUMED")
    );
});
