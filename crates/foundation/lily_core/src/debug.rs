//! Structured development diagnostics.
//!
//! Diagnostics are disabled unless the canonical `LILY_ENV` value is
//! `development`.

/// Emit a structured debug event in the development environment.
///
/// Values supplied by a caller are still subject to that call site's
/// redaction policy.
#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {{
        if $crate::environment::debug_diagnostics_enabled() {
            $crate::__private::tracing::debug!(
                target: "lily::diagnostic",
                message = %format_args!($($arg)*)
            );
        }
    }};
}
