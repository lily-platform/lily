#![no_main]

use libfuzzer_sys::fuzz_target;
use lily_websocket::__fuzzing::{
    FuzzInitialMessageOutcome, FuzzMessageAction, FuzzMessageChainPlan, FuzzMessageMiddlewarePlan,
    MESSAGE_MAX_MIDDLEWARES, exercise_message_chain,
};
use std::{sync::OnceLock, time::Duration};

const MAX_INPUT_BYTES: usize = 64 * 1024;

fn action(value: u8) -> FuzzMessageAction {
    match value % 5 {
        0 => FuzzMessageAction::Continue,
        1 => FuzzMessageAction::Reject,
        2 => FuzzMessageAction::Close,
        3 => FuzzMessageAction::Error,
        _ => FuzzMessageAction::Pending,
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime must build")
    })
}

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_INPUT_BYTES {
        return;
    }
    let controls = input.get(1..).unwrap_or_default();
    let middlewares = controls
        .chunks(2)
        .take(MESSAGE_MAX_MIDDLEWARES)
        .map(|pair| FuzzMessageMiddlewarePlan {
            before: action(pair[0]),
            after: action(pair.get(1).copied().unwrap_or_default()),
        })
        .collect::<Vec<_>>();
    let head = input.first().copied().unwrap_or_default();
    let initial_outcome = match (head >> 2) & 3 {
        0 => FuzzInitialMessageOutcome::Handled,
        1 => FuzzInitialMessageOutcome::Rejected,
        2 => FuzzInitialMessageOutcome::Close,
        _ => FuzzInitialMessageOutcome::Failed,
    };
    let plan = FuzzMessageChainPlan {
        middlewares,
        initial_outcome,
        cancel_before: head & 1 != 0,
        cancel_after: head & 2 != 0,
    };
    runtime().block_on(async move {
        let mut task = tokio::spawn(exercise_message_chain(plan));
        let snapshot = match tokio::time::timeout(Duration::from_millis(250), &mut task).await {
            Ok(result) => result.expect("message exercise task must not panic"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("message exercise exceeded its absolute deadline");
            }
        };
        assert!(snapshot.entered <= MESSAGE_MAX_MIDDLEWARES);
        assert!(snapshot.after_attempted <= snapshot.entered);
        assert!(snapshot.after_order.len() <= snapshot.entered);
        assert!(snapshot.termination_order.len() <= snapshot.entered);
        assert!(
            snapshot
                .termination_order
                .windows(2)
                .all(|pair| pair[0] > pair[1])
        );
        assert!(
            snapshot
                .exit_order
                .windows(2)
                .all(|pair| pair[0] >= pair[1]),
            "an outer exit must not overtake an interrupted inner exit's termination"
        );
        assert!(
            snapshot
                .after_order
                .windows(2)
                .all(|pair| pair[0] > pair[1])
        );
    });
});
