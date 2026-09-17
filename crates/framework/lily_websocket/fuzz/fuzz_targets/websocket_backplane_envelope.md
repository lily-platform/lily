# `websocket_backplane_envelope`

- Maximum input: 512 KiB (`-max_len=524288`). The complete input is supplied
  as one opaque inbound backplane frame; no control prefix is removed.
- Production path: one real `WebSocketDispatcher` ingress task, the private
  v4 backplane decoder, mandatory namespaces on every target, explicit UUID and principal
  target/message/trace validation, deduplication, and node-local terminal
  broadcast.
- The cargo-fuzz-only provider emits `SubscriptionReady`, the input frame, and
  then blocks at its next bounded receive point so the harness can prove the
  frame completed before shutdown. It does not reimplement the envelope codec.
- Oracle: malformed or oversized input may increment exactly one bounded
  rejection/suppression counter. Panic, allocation failure, deadline expiry,
  multiple terminal classifications, or a task-lifecycle leak is a finding.
- Seeds are synthetic protocol examples only. They contain no application
  payload, credential, broker endpoint, or production correlation value.

```bash
websocket_fuzz_tmp="$(mktemp -d)"
cp -R corpus/websocket_backplane_envelope "$websocket_fuzz_tmp/corpus"
mkdir -p "$websocket_fuzz_tmp/artifacts"
cargo +nightly-2026-08-15 fuzz run websocket_backplane_envelope \
  "$websocket_fuzz_tmp/corpus" -- \
  -runs=10000 -max_len=524288 -timeout=2 \
  -artifact_prefix="$websocket_fuzz_tmp/artifacts/"
```
