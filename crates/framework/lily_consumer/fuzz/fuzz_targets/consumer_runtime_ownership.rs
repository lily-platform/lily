#![no_main]

use std::{cell::RefCell, time::Duration};

use libfuzzer_sys::fuzz_target;
use lily_consumer::fuzzing::{exercise_consumer_runtime_ownership, ConsumerRuntimeOwnershipEvent};

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;
const MAX_EVENTS: usize = 32;
const OUTER_DEADLINE: Duration = Duration::from_millis(250);

thread_local! {
    static RUNTIME: RefCell<tokio::runtime::Runtime> = RefCell::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            // This target is transport-free. Logical time keeps the production
            // 10 ms cleanup/force ordering exact without turning host
            // scheduling pauses into ownership crashes. libFuzzer's external
            // `-timeout` remains the independent wall-clock watchdog.
            .start_paused(true)
            .build()
            .expect("bounded runtime-owner fuzz runtime must build")
    );
}

fuzz_target!(|input: &[u8]| {
    if input.is_empty() || input.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let profile = match input[0] {
        b'd' => 0,
        b'm' => 1,
        b'x' => 2,
        value => value % 3,
    };
    let events = input[1..]
        .iter()
        .copied()
        .take(MAX_EVENTS)
        .map(event_from_byte)
        .collect::<Vec<_>>();

    RUNTIME.with(|runtime| {
        let runtime = runtime.borrow_mut();
        let mut task = runtime
            .spawn(async move { exercise_consumer_runtime_ownership(profile, &events).await });
        let outcome =
            runtime.block_on(async { tokio::time::timeout(OUTER_DEADLINE, &mut task).await });
        let summary = match outcome {
            Ok(Ok(Ok(summary))) => summary,
            Ok(Ok(Err(_bounded_rejection))) => return,
            Ok(Err(join_error)) => panic!("runtime-owner qualification task failed: {join_error}"),
            Err(_) => {
                task.abort();
                let joined = runtime.block_on(task);
                assert!(joined.is_err_and(|error| error.is_cancelled()));
                panic!("consumer runtime ownership exceeded its outer deadline");
            }
        };

        assert!(summary.shutdown_initiated);
        assert!(summary.supervisor_finished);
        assert!(summary.exactly_once);
        assert!(summary.no_late_calls);
        assert!(summary.drain_reconciled);
        assert_eq!(summary.active_jobs, 0);
        assert!((1..=6).contains(&summary.lifecycle_calls));
    });
});

fn event_from_byte(byte: u8) -> ConsumerRuntimeOwnershipEvent {
    match byte {
        b'c' => ConsumerRuntimeOwnershipEvent::CancelTrigger,
        b'p' => ConsumerRuntimeOwnershipEvent::CompleteProvider,
        b'm' => ConsumerRuntimeOwnershipEvent::ManualShutdown,
        b'a' => ConsumerRuntimeOwnershipEvent::AbortWaiter,
        b'f' => ConsumerRuntimeOwnershipEvent::RequestForce,
        b'y' => ConsumerRuntimeOwnershipEvent::Yield,
        b'P' => ConsumerRuntimeOwnershipEvent::FailProvider,
        b'D' => ConsumerRuntimeOwnershipEvent::FailDrain,
        b'X' => ConsumerRuntimeOwnershipEvent::PanicDrain,
        b'T' => ConsumerRuntimeOwnershipEvent::PauseDrain,
        value => match value % 10 {
            0 => ConsumerRuntimeOwnershipEvent::CancelTrigger,
            1 => ConsumerRuntimeOwnershipEvent::CompleteProvider,
            2 => ConsumerRuntimeOwnershipEvent::ManualShutdown,
            3 => ConsumerRuntimeOwnershipEvent::AbortWaiter,
            4 => ConsumerRuntimeOwnershipEvent::RequestForce,
            5 => ConsumerRuntimeOwnershipEvent::Yield,
            6 => ConsumerRuntimeOwnershipEvent::FailProvider,
            7 => ConsumerRuntimeOwnershipEvent::FailDrain,
            8 => ConsumerRuntimeOwnershipEvent::PanicDrain,
            _ => ConsumerRuntimeOwnershipEvent::PauseDrain,
        },
    }
}
