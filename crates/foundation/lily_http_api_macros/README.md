# Lily HTTP API macros

`lily_http_api_macros` is the procedural-macro implementation companion for
`lily_http_api`. It is not a standalone framework surface. Applications should
depend only on `lily_http_api` and import `Controller`, `controller`, and
`MultipartForm` from that facade.

```toml
[dependencies]
lily_http_api = "0.1"
```

Do not add `lily_http_api_macros` directly. Generated code locates the
application's `lily_http_api` dependency even when it has been renamed in
`Cargo.toml`. Dependencies required by generated registration and OpenAPI code
(`linkme`, `utoipa`, `headers`, and `serde_json`) are reached through the HTTP
facade and do not need to be repeated by the application.

## Struct controllers

A controller has three parts:

1. `#[derive(Controller)]` declares controller-level routing policy.
2. `ControllerTrait::new` creates the single app-scoped controller instance.
3. `#[controller]` turns one inherent impl into typed HTTP actions.

```rust,ignore
use std::sync::Arc;

use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, Extensions,
    HttpApiError,
};
use lily_http_api::async_trait::async_trait;

#[derive(Controller)]
#[base_path("/api/items")]
struct ItemsController;

#[async_trait]
impl ControllerTrait for ItemsController {
    async fn new(
        _extensions: Arc<Extensions>,
    ) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl ItemsController {
    #[get("/")]
    async fn list(&self) -> Result<String, HttpApiError> {
        Ok("items".to_owned())
    }
}

// Ordinary helpers must live outside the #[controller] impl.
impl ItemsController {
    fn audit_label(&self) -> &'static str {
        "items"
    }
}
```

`Controller` requires exactly one `#[base_path("/...")]`. Controller-level
`#[middleware(Type, ...)]`, `#[cors(PolicyProvider)]`, and `#[openapi(...)]`
are optional. The controller and its derive input cannot be generic.

The `#[controller]` impl accepts only async action methods with immutable
`&self`. Each action needs exactly one of `get`, `post`, `put`, `patch`,
`delete`, `head`, `options`, or the general form
`#[route(method = "CUSTOM", path = "/...")]`. Action-level `guard`,
`middleware`, `cors`, and `openapi` attributes are optional. Put ordinary
helper methods in another inherent impl.

The macro enforces the HTTP facade's typed-extractor contract at compile time:
request-parts and service extractors precede the body extractor, an action has
at most one body consumer, and `BodyStream` cannot share raw request ownership.
It also distinguishes framework-managed typed responses, manual
`&mut Response`, and `PassthroughResponseContext`.

## Multipart DTOs

`MultipartForm` creates both strict runtime decoding and the corresponding
OpenAPI schema from one named-field DTO.

```rust,ignore
use lily_http_api::{FormFile, MultipartForm};

#[derive(MultipartForm)]
#[schema(as = api::UploadInput)]
struct UploadInput {
    title: String,
    note: Option<String>,
    tags: Vec<String>,
    #[form_file]
    avatar: FormFile,
    #[form_file]
    attachments: Vec<FormFile>,
}
```

Supported text shapes are `String`, `Option<String>`, and `Vec<String>`.
Supported file shapes are `FormFile`, `Option<FormFile>`, and `Vec<FormFile>`;
all file fields require `#[form_file]`. Unknown fields, duplicate scalar
fields, missing required fields, and unsupported types fail closed.

## Public boundary

The only Rust-public items in this crate are the three procedural-macro entry
points required by the compiler. Parser plans, generated adapter builders,
runtime path resolution, and OpenAPI metadata parsers are private
implementation details. Application-facing traits, extractors, errors, and
registration behavior belong to `lily_http_api`.
