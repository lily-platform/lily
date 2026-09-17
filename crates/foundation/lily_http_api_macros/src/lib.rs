//! Implementation macros for Lily's HTTP facade.
//!
//! Application code should depend on and import these macros from
//! `lily_http_api`; this companion crate is not a standalone HTTP API. Macro
//! expansion resolves the application's `lily_http_api` dependency (including
//! a Cargo rename) and reaches generated-code dependencies through that
//! facade. Applications therefore do not need direct dependencies on this
//! crate, `linkme`, `utoipa`, `headers`, or `serde_json` merely to use these
//! macros.
//!
//! The canonical controller shape combines:
//!
//! - `#[derive(Controller)]` on a non-generic controller struct;
//! - an application implementation of `ControllerTrait`; and
//! - `#[controller]` on an inherent impl containing its async HTTP actions.
//!
//! `#[derive(MultipartForm)]` is the corresponding strict multipart DTO
//! binding implementation. See this package's README and the `lily_http_api`
//! documentation for the complete application-facing contract.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

use proc_macro::TokenStream;

mod action_signature;
mod controller_impl;
mod derive_controller;
mod multipart_form;
mod openapi;
mod runtime_path;
mod syntax;

/// Generates Lily's app-scoped definition and static registration for a
/// struct controller.
///
/// Use this derive through `lily_http_api::Controller`. The controller must be
/// a non-generic struct and must also implement `ControllerTrait`; the derive
/// does not construct the controller or resolve its dependencies.
///
/// Supported helper attributes are:
///
/// - `#[base_path("/api/items")]` — required, exactly once;
/// - `#[middleware(First, Second)]` — optional controller middleware types;
/// - `#[cors(PolicyProvider)]` — optional controller CORS policy provider;
/// - `#[openapi(...)]` — optional controller-level OpenAPI defaults.
///
/// Registration metadata is an internal ABI consumed by `lily_http_api`.
#[proc_macro_derive(Controller, attributes(base_path, middleware, cors, openapi))]
pub fn derive_controller(input: TokenStream) -> TokenStream {
    derive_controller::derive(input)
}

/// Generates strict multipart DTO decoding and its matching OpenAPI schema.
///
/// Use this derive through `lily_http_api::MultipartForm`. It accepts a
/// non-generic struct with named fields. Text fields support `String`,
/// `Option<String>`, and `Vec<String>`. File fields must carry
/// `#[form_file]` and support `FormFile`, `Option<FormFile>`, and
/// `Vec<FormFile>`. Unknown fields, duplicate scalar fields, unsupported
/// shapes, and missing required fields are rejected instead of being silently
/// ignored.
///
/// `#[schema(as = path::Name)]` may be placed on the DTO to choose its OpenAPI
/// component name. Field-level `#[schema(...)]` is intentionally unsupported;
/// field schemas follow the runtime binding plan.
#[proc_macro_derive(MultipartForm, attributes(form_file, schema))]
pub fn derive_multipart_form(input: TokenStream) -> TokenStream {
    multipart_form::derive(input)
}

/// Expands one inherent controller impl into typed action adapters and route
/// registrations.
///
/// Use this attribute through `lily_http_api::controller`, after deriving
/// `Controller` for the same type. The attribute accepts no arguments. Its
/// impl block must be non-generic, contain at least one method, and contain
/// only HTTP actions; helper methods belong in a separate inherent impl.
///
/// Each action must be `async`, receive immutable `&self` first, and declare
/// exactly one route attribute: `#[get]`, `#[post]`, `#[put]`, `#[patch]`,
/// `#[delete]`, `#[head]`, `#[options]`, or
/// `#[route(method = "...", path = "/...")]`. Optional action helpers are
/// `#[guard(...)]`, `#[middleware(...)]`, `#[cors(...)]`, and
/// `#[openapi(...)]`.
///
/// The macro validates typed extractor order and body ownership, response
/// ownership (`&mut Response` versus passthrough response metadata), route
/// syntax, and OpenAPI authority conflicts at compile time.
#[proc_macro_attribute]
pub fn controller(args: TokenStream, input: TokenStream) -> TokenStream {
    controller_impl::expand(args, input)
}
