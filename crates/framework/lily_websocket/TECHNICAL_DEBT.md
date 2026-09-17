# Lily WebSocket technical debt

This file records deliberately deferred work whose current behavior is part of
the framework's published operational boundary. An entry is removed only after
its acceptance criteria pass in both framework tests and the applicable
qualification suite.

<a id="lws-td-001"></a>

## LWS-TD-001 — Incremental UTF-8 validation for an incomplete WebSocket frame

- Status: deferred; frozen for the initial release
- Recorded: 2026-09-01
- Area: transport parser, strict WebSocket conformance, slow-client hardening
- Qualification: `wire-codec`, Autobahn cases `6.4.3` and `6.4.4`
- Current result: `NON-STRICT`; Close behavior remains accepted

### Decision

Keep `tokio-tungstenite` as Lily's WebSocket stream backend for the initial
release. Do not replace the backend or add a second WebSocket parser inside
Lily solely to make these two Autobahn cases strict. Revisit the work through
an upstream-first Tungstenite change; if delivery becomes urgent before an
upstream release, a narrowly audited and revision-pinned fork is the preferred
temporary bridge.

This deferral is not a declaration that fail-fast validation is unnecessary.
It records that a complete backend migration would be disproportionate to the
isolated parser behavior and would affect Lily's public Tungstenite `Message`
type, control-frame semantics, buffering, error mapping, and connection-loop
regressions.

### Current behavior

After Lily has completed its own HTTP Upgrade boundary, it constructs a
`tokio_tungstenite::WebSocketStream` with `from_raw_socket`. Tungstenite parses
the frame header and enforces the declared frame-size limit, but waits for the
complete frame payload before unmasking it and producing a frame for text
validation.

Consequently:

- invalid UTF-8 is rejected before an application message, action, or
  middleware dispatch can observe it;
- once Tungstenite reports `Error::Utf8`, Lily requests WebSocket Close code
  `1007`;
- if invalidity becomes conclusive in the second TCP chop of one still
  incomplete frame, Lily cannot report that error at the second-chop boundary;
- application code never receives partial frame bytes or the invalid Text
  message; and
- Autobahn `6.4.3` and `6.4.4` observe the extra wait and classify the timing as
  `NON-STRICT` rather than `OK`.

The ecosystem comparison supporting the initial-release decision is
source-level, not an independent Autobahn certification of those frameworks:

- Axum performs its HTTP Upgrade and then constructs
  `tokio_tungstenite::WebSocketStream::from_raw_socket`, matching Lily's parser
  boundary.
- Actix Web uses its own `actix_http::ws::Codec`; that parser also waits for the
  complete declared frame before returning its Text payload, after which
  `actix-ws` performs UTF-8 conversion.
- The Tungstenite parser observed when this debt was recorded still waited for
  the complete payload before unmasking and returning a frame. A dependency
  version bump alone is therefore not evidence that this debt is resolved.

### Security boundary

An adversarial peer can retain a connection, its task and its bounded frame
buffer longer by slowly completing a frame after sending a prefix that is
already provably invalid UTF-8. Lily's connection, frame, and message limits
bound resource counts and payload sizes, and its independent idle/heartbeat
paths can terminate a connection, but none of those controls is an
incremental UTF-8 decision at the TCP-read boundary.

The future parser fix narrows that retention window only for a prefix that is
already provably invalid. It does not prevent a peer from slowly sending a
valid UTF-8 prefix. General Slowloris-style resistance remains a separate
operational and design concern: connection admission and a bounded
transport/read-progress policy must not be represented as solved merely
because `6.4.3` and `6.4.4` become strict.

This debt does **not** mean that invalid UTF-8 reaches application handlers,
that frame/message memory is unbounded, or that Lily accepts an invalid Text
message as a successful application event.

### Target design

Prefer a private Tungstenite parser change with these properties:

1. Track the number of payload bytes already inspected for the in-progress
   frame.
2. Remove the client mask for newly available bytes using the correct rolling
   four-byte mask offset before validation.
3. Maintain streaming UTF-8 state across TCP reads and Text continuation
   frames, while allowing a read to end in a potentially valid partial code
   point.
4. Reject as soon as the observed prefix is conclusively invalid and surface
   the existing `Error::Utf8` provenance.
5. Preserve interleaved control-frame behavior, Binary frames, frame/message
   size limits, buffering/backpressure semantics, and Lily's public API.
6. Avoid a Lily-owned duplicate wire parser. If upstream cannot yet carry the
   change, keep any fork minimal, audited, revision-pinned, and temporary.

### Acceptance criteria

The debt is complete only when all of the following hold:

1. Autobahn `6.4.3` and `6.4.4` report `OK`, not `NON-STRICT`.
2. A deterministic chopped-input regression proves that the second chop which
   makes UTF-8 invalid causes Close code `1007` before the remaining declared
   frame bytes are supplied.
3. Valid multi-byte UTF-8 code points split at every byte boundary remain
   accepted.
4. Mask offsets, fragmented Text messages, continuation frames, and
   interleaved Ping/Pong frames have explicit regressions; Binary payloads are
   unaffected.
5. No invalid or partial message reaches message middleware, guards,
   extractors, or an action.
6. Connection-manager publication and lifecycle cleanup remain exact-once and
   complete within bounded deadlines on both rejection and successful control
   paths.
7. Lily's full test suite and every WebSocket qualification suite pass without
   weakening existing expectations.
8. The release documentation states separately whether a frame-assembly or
   read-progress deadline exists; strict UTF-8 validation must not be described
   as complete slow-client protection.

### Revisit triggers

Re-evaluate this decision when any of the following occurs:

- Tungstenite adds an incremental payload-validation seam or ships equivalent
  behavior.
- The project is prepared to maintain a narrow upstreamable parser patch.
- A security review demonstrates unacceptable resource retention under Lily's
  deployment limits.
- Lily introduces a private transport abstraction that makes backend changes
  possible without changing its public message contract.

### Source references

- [Lily transport construction](src/app/mod.rs)
- [Lily UTF-8 Close mapping](src/app/mod.rs)
- [Lily raw-wire qualification tests](src/app/wire_qualification_tests.rs)
- [Axum WebSocket Upgrade and stream construction](https://docs.rs/axum/latest/src/axum/extract/ws.rs.html)
- [Actix WebSocket message stream](https://docs.rs/actix-ws/latest/src/actix_ws/stream.rs.html)
- [Actix HTTP WebSocket frame parser](https://github.com/actix/actix-web/blob/main/actix-http/src/ws/frame.rs)
- [Tungstenite frame codec](https://docs.rs/tungstenite/latest/src/tungstenite/protocol/frame/mod.rs.html)
