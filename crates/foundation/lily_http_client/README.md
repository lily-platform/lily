# Lily HTTP Client

`lily_http_client` is Lily's bounded asynchronous HTTP/1.1 and HTTP/2 client. Hyper/h2 owns protocol framing, pooling, multiplexing and flow control; Rustls owns HTTPS certificate/hostname verification and ALPN.

```rust,no_run
use lily_http_client::{HttpClientBuilder, ProtocolPreference};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClientBuilder::new()
        .protocol(ProtocolPreference::Auto)
        .max_response_body_bytes(8 * 1024 * 1024)
        .max_retained_origins(64)
        .try_build()?;

    let mut response = client
        .execute(client.get("https://example.com/health")?.build()?)
        .await?;
    let version = response.version();
    let byte_count = response.bytes().await?.len();
    println!("{byte_count} bytes over {version}");
    Ok(())
}
```

## Protocol policy

| Preference | Plain HTTP | HTTPS |
|---|---|---|
| `Auto` | HTTP/1.1 | ALPN `h2` or `http/1.1` |
| `Http1Only` | HTTP/1.1 | ALPN `http/1.1` only |
| `Http2Only` | h2c prior knowledge | ALPN `h2` only |

The negotiated protocol is checked against this policy. Plain `Auto` does not silently switch to h2c.

## Safety contract

- Global and per-origin request admission limits.
- Bounded request/response bodies, header count/bytes, total deadline and HTTP/2 frame/window settings.
- Bounded LRU scheme/host/port registry; protocol-specific Hyper pools are
  created lazily and idle-only origin eviction drops their retained pools.
- A full registry evicts the least-recent idle origin. If every slot has active
  work, a new origin fails fast without cancelling or replaying existing work.
- Exact binary response bytes; UTF-8 is required only by `Response::text`.
- Cross-origin redirects strip authorization/cookies; HTTPS downgrade and URL credentials are rejected.
- Certificate verification cannot be disabled in the supported profile.
- Proxy, custom DNS/local bind and automatic decompression are not exposed by the v1 API.
- Hyper retries only requests it proves were never written on a stale reused connection. Accepted or ambiguous requests are not replayed.

Central factory defaults and named-client overrides expose the same protocol,
body/header, admission, pool and HTTP/2 settings as the direct builder. A
private CA bundle is named-client-only, bounded and loaded once during factory
initialization; it extends public roots and never disables certificate or
hostname verification.

The v1 request and response APIs are bounded-buffered, not streaming. Request
bodies must have a known size and be repeatable; Hyper chooses the correct wire
framing. Public CA/hostname/ALPN interoperability, independent wire conformance
and long-running load/fault tests remain release qualification gates;
implementation alone is not a production approval.

See the [HTTP client guide](../../../docs/http/http-client.md) and [transport ADR](../../../docs/adr/ADR-HTTP-001-http1-http2-edge-transport.md).
