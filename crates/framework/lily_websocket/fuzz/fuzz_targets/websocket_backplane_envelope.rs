#![no_main]

use libfuzzer_sys::fuzz_target;
use lily_websocket::__fuzzing::{BACKPLANE_MAX_INPUT_BYTES, exercise_backplane_envelope};
use std::sync::OnceLock;
use std::time::Duration;

const ABSOLUTE_DEADLINE: Duration = Duration::from_secs(1);

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
    if input.len() > BACKPLANE_MAX_INPUT_BYTES {
        return;
    }

    runtime().block_on(async move {
        let snapshot = tokio::time::timeout(
            ABSOLUTE_DEADLINE,
            exercise_backplane_envelope(input.to_vec()),
        )
        .await
        .expect("backplane envelope fuzz case exceeded its absolute deadline");

        assert!(snapshot.invalid_frames <= 1);
        assert!(snapshot.duplicates_suppressed <= 1);
        assert!(snapshot.origin_loops_suppressed <= 1);
        assert!(
            snapshot.invalid_frames
                + snapshot.duplicates_suppressed
                + snapshot.origin_loops_suppressed
                <= 1
        );
    });
});
