use runtime::{TraceFailure, TraceResultError, lily_trace};

#[derive(Debug, PartialEq)]
struct Rejected;
impl TraceResultError for Rejected {
    fn trace_failure(&self) -> TraceFailure {
        TraceFailure::Rejected {
            code: "invalid_input",
        }
    }
}

#[lily_trace(name = "facade.sync", result)]
fn sync_operation(fail: bool) -> Result<u64, Rejected> {
    if fail {
        return Err(Rejected);
    }
    Ok(42)
}

#[lily_trace(name = "facade.async", result)]
async fn async_operation(fail: bool) -> Result<u64, Rejected> {
    sync_operation(fail)
}

// Explicit paths continue to take precedence over automatic discovery.
mod provider {
    pub use crate::runtime as telemetry;
}
#[lily_trace(name = "facade.override", crate_path = "crate::provider::telemetry")]
fn explicit_runtime() -> bool {
    true
}

#[cfg(test)]
#[tokio::test]
async fn facade_macro_preserves_success_rejection_and_explicit_override() {
    assert_eq!(sync_operation(false), Ok(42));
    assert_eq!(sync_operation(true), Err(Rejected));
    assert_eq!(async_operation(false).await, Ok(42));
    assert_eq!(async_operation(true).await, Err(Rejected));
    assert!(explicit_runtime());
}
