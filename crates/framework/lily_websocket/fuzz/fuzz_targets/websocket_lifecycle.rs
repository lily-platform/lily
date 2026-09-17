#![no_main]

use libfuzzer_sys::fuzz_target;
use lily_websocket::__fuzzing::{
    FuzzConnectionAction, FuzzConnectionMiddlewarePlan, FuzzLifecycleEvent, FuzzLifecyclePlan,
    LIFECYCLE_MAX_EVENTS, LIFECYCLE_MAX_MIDDLEWARES, exercise_lifecycle,
};
use std::{sync::OnceLock, time::Duration};

const MAX_INPUT_BYTES: usize = 64 * 1024;

fn connection_action(value: u8) -> FuzzConnectionAction {
    match value % 4 {
        0 => FuzzConnectionAction::Continue,
        1 => FuzzConnectionAction::Reject,
        2 => FuzzConnectionAction::Error,
        _ => FuzzConnectionAction::Pending,
    }
}

fn lifecycle_event(value: u8) -> FuzzLifecycleEvent {
    match value % 10 {
        0 => FuzzLifecycleEvent::Connect,
        1 => FuzzLifecycleEvent::Opened,
        2 => FuzzLifecycleEvent::MessageContinue,
        3 => FuzzLifecycleEvent::MessageReject,
        4 => FuzzLifecycleEvent::MessageClose,
        5 => FuzzLifecycleEvent::ClientClose,
        6 => FuzzLifecycleEvent::Reset,
        7 => FuzzLifecycleEvent::IdleTimeout,
        8 => FuzzLifecycleEvent::ServerShutdown,
        _ => FuzzLifecycleEvent::Cancellation,
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime must build")
    })
}

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_INPUT_BYTES {
        return;
    }
    let head = input.first().copied().unwrap_or_default();
    let middlewares = input
        .get(1..)
        .unwrap_or_default()
        .chunks(3)
        .take(LIFECYCLE_MAX_MIDDLEWARES)
        .map(|chunk| FuzzConnectionMiddlewarePlan {
            admit: connection_action(chunk[0]),
            opened: connection_action(chunk.get(1).copied().unwrap_or_default()),
            closed: connection_action(chunk.get(2).copied().unwrap_or_default()),
        })
        .collect::<Vec<_>>();
    let events = input
        .iter()
        .rev()
        .take(LIFECYCLE_MAX_EVENTS)
        .copied()
        .map(lifecycle_event)
        .collect();
    let plan = FuzzLifecyclePlan {
        middlewares,
        events,
        cancel_before_admission: head & 1 != 0,
        cancel_before_opened: head & 2 != 0,
    };
    runtime().block_on(async move {
        let mut task = tokio::spawn(exercise_lifecycle(plan));
        let snapshot = match tokio::time::timeout(Duration::from_millis(500), &mut task).await {
            Ok(result) => result.expect("lifecycle exercise task must not panic"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("lifecycle exercise exceeded its absolute deadline");
            }
        };
        assert_eq!(snapshot.manager_connections, 0);
        assert_eq!(snapshot.registry_entries, 0);
        assert_eq!(snapshot.available_permits, 1);
        assert!(snapshot.entered <= LIFECYCLE_MAX_MIDDLEWARES);
        assert!(
            snapshot
                .closed_order
                .windows(2)
                .all(|pair| pair[0] > pair[1])
        );
    });
});
