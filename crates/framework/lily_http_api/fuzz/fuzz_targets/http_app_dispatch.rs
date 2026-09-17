#![no_main]

use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
use libfuzzer_sys::fuzz_target;
use lily_http_api::__private::fuzzing::{
    exercise_app_dispatch, AppDispatchInput, AppDispatchResult,
};
use std::sync::OnceLock;
use std::time::Duration;

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

#[derive(Debug)]
struct DispatchCase(AppDispatchInput);

impl<'a> Arbitrary<'a> for DispatchCase {
    fn arbitrary(input: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        let bytes = input.bytes(input.len())?;
        let control = |index: usize| bytes.get(index).copied().unwrap_or_default();
        let method = match control(0) % 5 {
            0 => "GET".to_string(),
            1 => "POST".to_string(),
            2 => "DELETE".to_string(),
            3 => "PATCH".to_string(),
            _ => format!("X{}", control(4)),
        };
        let mut target = match control(1) % 5 {
            0 => "/ok".to_string(),
            1 => "/guarded".to_string(),
            2 => "/error".to_string(),
            3 => format!("/items/{}", control(5)),
            _ => format!("/missing/{}", control(5)),
        };
        if control(2) & 1 != 0 {
            target.push_str("?q=");
            for byte in bytes.iter().skip(6).take(64) {
                target.push(char::from(b'a' + (byte % 26)));
            }
        }

        let header_count = usize::from(control(3) % 8);
        let headers = (0..header_count)
            .map(|index| {
                let byte = control(6 + index);
                (format!("X-Fuzz-{index}"), format!("v{}", byte))
            })
            .collect();
        let body = bytes.iter().skip(16).copied().take(32 * 1024).collect();
        Ok(Self(AppDispatchInput {
            method,
            target,
            headers,
            body,
        }))
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fuzz runtime must build")
    })
}

fuzz_target!(|case: DispatchCase| {
    let DispatchCase(input) = case;
    if input.body.len()
        + input.method.len()
        + input.target.len()
        + input
            .headers
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum::<usize>()
        > MAX_FUZZ_INPUT_BYTES
    {
        return;
    }
    let result = runtime().block_on(async move {
        let mut task = tokio::spawn(exercise_app_dispatch(input));

        match tokio::time::timeout(Duration::from_millis(100), &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Ok(Err(error)) => panic!("app dispatch fuzz task failed: {error}"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("app dispatch fuzz iteration exceeded 100 ms");
            }
        }
    });
    if let AppDispatchResult::Completed { status, body_bytes } = result {
        assert!((100..=599).contains(&status));
        assert!(body_bytes <= MAX_FUZZ_INPUT_BYTES);
    }
});
