extern crate self as lily_trace;

use lily_trace_macros::lily_trace;

pub use trace_runtime::{__private, environment_matches, tracing};

#[lily_trace(name = "audit.record", level = "debug", fields(value), skip(secret))]
fn record(value: u64, secret: &str) -> u64 {
    let _ = secret;
    value
}

fn main() {
    assert_eq!(record(42, "not-recorded"), 42);
}
