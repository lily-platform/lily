#![no_main]

use std::{cell::RefCell, time::Duration};

use libfuzzer_sys::fuzz_target;
use lily_consumer::fuzzing::{exercise_consumer_shutdown, ConsumerShutdownEvent};

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;
const MAX_EVENTS: usize = 64;
const OUTER_DEADLINE: Duration = Duration::from_millis(100);

thread_local! {
    static RUNTIME: RefCell<tokio::runtime::Runtime> = RefCell::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("bounded consumer fuzz runtime must build")
    );
}

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let events = input
        .iter()
        .copied()
        .take(MAX_EVENTS)
        .map(event_from_byte)
        .collect::<Vec<_>>();

    RUNTIME.with(|runtime| {
        let runtime = runtime.borrow_mut();
        let mut task = runtime.spawn(async move { exercise_consumer_shutdown(&events).await });
        let outcome =
            runtime.block_on(async { tokio::time::timeout(OUTER_DEADLINE, &mut task).await });
        let summary = match outcome {
            Ok(Ok(Ok(summary))) => summary,
            Ok(Ok(Err(_bounded_rejection))) => return,
            Ok(Err(join_error)) => panic!("shutdown qualification task failed: {join_error}"),
            Err(_) => {
                task.abort();
                let joined = runtime.block_on(task);
                assert!(joined.is_err_and(|error| error.is_cancelled()));
                panic!("consumer shutdown exceeded its outer deadline");
            }
        };

        assert!(summary.shutdown_initiated);
        assert!(summary.report_reconciles);
        assert!(summary.terminal_report_replayed);
        assert_eq!(summary.active_jobs, 0);
        assert_eq!(summary.available_permits, summary.permit_capacity);
        assert!(summary.spawned_tasks <= summary.permit_capacity);
        assert!(summary.component_outcomes >= 1);
    });
});

fn event_from_byte(byte: u8) -> ConsumerShutdownEvent {
    match byte {
        b'S' => ConsumerShutdownEvent::SpawnTask {
            fail_after_cancellation: false,
            ignore_cancellation: false,
        },
        b's' => ConsumerShutdownEvent::SpawnTask {
            fail_after_cancellation: true,
            ignore_cancellation: false,
        },
        b'h' => ConsumerShutdownEvent::SpawnTask {
            fail_after_cancellation: false,
            ignore_cancellation: true,
        },
        b'm' => ConsumerShutdownEvent::ManualStop,
        b'i' => ConsumerShutdownEvent::Interrupt,
        b't' => ConsumerShutdownEvent::Terminate,
        b'q' => ConsumerShutdownEvent::Quit,
        b'2' => ConsumerShutdownEvent::SecondSignal,
        b'p' => ConsumerShutdownEvent::ProviderFailure,
        b'r' => ConsumerShutdownEvent::RegistrationFailure,
        b'c' => ConsumerShutdownEvent::Cancellation,
        _ => match byte % 9 {
            0 => ConsumerShutdownEvent::ManualStop,
            1 => ConsumerShutdownEvent::Interrupt,
            2 => ConsumerShutdownEvent::Terminate,
            3 => ConsumerShutdownEvent::Quit,
            4 => ConsumerShutdownEvent::SecondSignal,
            5 => ConsumerShutdownEvent::ProviderFailure,
            6 => ConsumerShutdownEvent::RegistrationFailure,
            7 => ConsumerShutdownEvent::Cancellation,
            _ => ConsumerShutdownEvent::SpawnTask {
                fail_after_cancellation: byte & 0x10 != 0,
                ignore_cancellation: byte == u8::MAX,
            },
        },
    }
}
