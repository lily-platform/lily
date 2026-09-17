use lily_trace_macros::lily_trace;

#[lily_trace(fields(password))]
fn record(user_id: u64) {
    let _ = user_id;
}

fn main() {}
