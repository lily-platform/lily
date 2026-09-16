extern crate trace_runtime as lily_trace;
use lily_trace::lily_trace;

struct ApplicationError;

#[lily_trace(result)]
fn operation() -> Result<(), ApplicationError> { Err(ApplicationError) }

fn main() {}
