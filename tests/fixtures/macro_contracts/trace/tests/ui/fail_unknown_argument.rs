use lily_trace_macros::lily_trace;

#[lily_trace(target = "secret")]
fn record() {}

fn main() {}
