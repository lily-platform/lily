//! Shared, read-only cancellation of an accepted execution.
//!
//! Execution owners signal cooperative cancellation; application callbacks and
//! infrastructure observe the same [`ExecutionCancellation`] view. Observation
//! does not terminate a task or complete resource cleanup. The crate is
//! independent of HTTP, database, and error contracts.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod execution;

pub use execution::ExecutionCancellation;

/// Cross-crate framework construction seams, not application callback APIs.
#[doc(hidden)]
pub mod __private {
    pub use crate::execution::ExecutionCancellationSource;

    /// Creates an inactive view for an object outside a managed execution.
    pub const fn inactive_execution_cancellation() -> crate::ExecutionCancellation {
        crate::ExecutionCancellation::inactive()
    }
}
