extern crate trace_runtime as lily_trace;
use lily_trace::lily_trace;

#[lily_trace(result)]
fn operation() -> u64 { 42 }

fn main() {}
