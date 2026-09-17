#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Attribute macros for instrumenting Lily application functions.

use proc_macro::TokenStream;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{parse_macro_input, ItemFn, Meta};

mod trace;
mod utils;

/// Instruments a synchronous or asynchronous function with one `tracing` span.
///
/// Each enabled invocation emits `lily.method.started` and
/// `lily.method.finished` events. `lily.duration_ms` measures elapsed wall time
/// using `Instant`, including time spent awaiting. `async_trait` methods are
/// instrumented inside their generated future, preserving its bounds and box.
/// A future that is never polled emits nothing. An unfinished future's drop is
/// recorded as `lily.lifecycle = "dropped"`; unwinding records `"panicked"`.
/// These lifecycle states do not invent application error codes or outcomes.
///
/// Bare `result` opts into `Result<T, E>` classification using
/// `lily_trace::TraceResultError`: `Ok` records `success`; `TraceFailure` chooses
/// `rejected` or `error` and a static `lily.error_code`. Without this flag, the
/// return value is not inspected and no error trait is required. Return values,
/// `?`, and early returns are preserved. Result values are never formatted.
///
/// Arguments are never recorded implicitly. `fields(...)` is an explicit
/// allow-list and records the named function arguments with `Debug`; callers
/// must not include secrets, credentials, request bodies, or unbounded user
/// input. `skip(...)` is accepted as an explicit non-recording declaration and
/// cannot overlap `fields(...)`.
///
/// `env = "development"` or `env = ["development", "test"]` conditionally
/// creates the span using Lily's process-level `LILY_ENV` classification. It
/// does not inspect `TraceConfig::environment`.
///
/// # Options
///
/// - `name = "..."`: static span name; defaults to the Rust function name.
/// - `level = "..."`: `trace`, `debug`, `info`, `warn`, or `error`; defaults to `info`.
/// - `fields(arg, ...)`: argument names explicitly recorded using `Debug`.
/// - `skip(arg, ...)`: argument names explicitly documented as not recorded.
/// - `result`: classify the returned `Result` through `TraceResultError`.
/// - `env = "..."` or `env = ["...", ...]`: runtime environment allow-list.
/// - `crate_path = "::path::to::lily_trace"`: generated-code support for facade crates.
///
/// # Examples
///
/// ```ignore
/// #[lily_trace]
/// async fn health_check() {}
///
/// #[lily_trace(
///     name = "product.lookup",
///     level = "debug",
///     fields(product_id),
///     skip(authorization),
///     env = ["development", "test"]
/// )]
/// async fn find_product(product_id: u64, authorization: &str) {}
/// ```
///
/// Framework derive macros may route generated references through a provider
/// crate's `lily_trace` re-export so downstream applications do not need a
/// hidden direct dependency:
/// ```ignore
/// #[lily_trace(
///     name = "database.operation",
///     crate_path = "::provider_crate::lily_trace"
/// )]
/// async fn operation() {}
/// ```
#[proc_macro_attribute]
pub fn lily_trace(args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemFn);

    let args = if args.is_empty() {
        Vec::new()
    } else {
        let parser = Punctuated::<Meta, Comma>::parse_terminated;
        match syn::parse::Parser::parse(parser, args) {
            Ok(parsed) => parsed.into_iter().collect(),
            Err(error) => return error.to_compile_error().into(),
        }
    };

    trace::lily_trace_impl(args, input)
}
