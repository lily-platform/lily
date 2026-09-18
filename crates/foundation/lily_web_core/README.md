# lily_web_core

`lily_web_core` owns Lily's protocol-neutral HTTP values: requests, responses,
typed cookies, bounded request and response bodies, SSE, static-file responses,
and shared Rustls configuration.

Application servers should depend on `lily_http_api`, which re-exports the
supported types from this crate:

```rust
use lily_http_api::{Json, Request, ResponseCookie};
```

A direct dependency on `lily_web_core` is not a second server API. It exists
for Lily's internal crate boundaries:

- `lily_http_api` constructs requests and writes responses.
- `lily_middleware` shares request, response, cookie, and rejection contracts.
- `lily_http_client` reuses the standards-based `FormData` representation.
- `lily_websocket` reuses the validated Rustls server configuration.

This crate has no optional Cargo features. Its response types include
`Json<T>`, `NoContent`, `Created<T>` and `Accepted<T>`; use their HTTP facade
exports in actions so response construction and request lifecycle share the
same owner.

## Ownership boundaries

- A request body is either buffered or transferred into terminal streaming
  ownership. Consumers cannot switch modes after bytes have been consumed.
- A typed `IntoResponse` value owns its representation. Use
  `PassthroughResponseContext` when an action needs to add success metadata,
  such as a cookie, while returning a typed body.
- Successful error rendering is an `Ok(ResponseWriteOutcome)`, with optional
  `ResponseFailureKind::Rejected` / `Error` application metadata. Use
  `ResponseWriteOutcome::rejected(code)` or `classified_error(kind, code)` after
  writing the representation. Existing `error(code)` stays unclassified; only
  failure to construct the response returns `Err(ResponseWriteError)`. All
  `Result` adapters retain explicit classification and error status authority.
- `RequestCookieJar` parses browser cookie fields once and fails closed on a
  duplicate requested name. `ResponseCookie` and `CookieRemoval` provide the
  typed response surface.
- Applications perform content negotiation and credential validation through
  their own policy. In HTTP actions, prefer `lily_http_api::TypedHeader<T>`
  (including the `headers` crate types re-exported by that facade) when a
  standards-aware typed header is required.
- Applications own sessions, authentication, authorization, persistence, and
  cookie rotation policy. This crate only supplies the HTTP and cryptographic
  cookie primitives.

Implementation modules are private. Stable contracts are exported from the
crate root and, for application code, from `lily_http_api`.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_web_core](https://docs.rs/lily_web_core).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
