use lily_trace_macros::lily_trace;

mod provider {
    pub mod lily_trace {
        pub use trace_runtime::*;
    }
}

#[lily_trace(
    name = "provider.operation",
    crate_path = "crate::provider::lily_trace",
    result
)]
fn operation() -> Result<u64, std::convert::Infallible> {
    Ok(42)
}

fn main() {
    assert_eq!(operation(), Ok(42));
}
