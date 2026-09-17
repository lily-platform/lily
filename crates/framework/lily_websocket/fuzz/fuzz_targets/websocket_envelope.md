# `websocket_envelope`

- Maksimum input: 1 MiB (`-max_len=1048576`). İlk byte message tipi ve
  transport-frame limiti için control byte'tır; kalan byte'lar payload'dur.
- Production yolu: `RawEnvelope::try_from_message` ile aynı frame bound/type
  kararı, ardından built-in `LilyEnvelopeCodec::decode_frame` ve geçerli bir
  envelope için `decode_payload`. Fuzz target ayrı bir request decoder veya
  envelope parser'ı içermez.
- Oracle: malformed JSON, unsupported protocol version ve limit aşımı typed
  rejection olabilir; panic, OOM veya hang bulgudur. Ping/Pong payload'u RFC
  control-frame sınırı olan 125 byte'ta tutulur.
- Seed kaynağı: yalnız sentetik geçerli/unsupported envelope ve message-control
  sınır örnekleri.

```bash
websocket_fuzz_tmp="$(mktemp -d)"
cp -R corpus/websocket_envelope "$websocket_fuzz_tmp/corpus"
mkdir -p "$websocket_fuzz_tmp/artifacts"
cargo +nightly-2026-08-15 fuzz run websocket_envelope "$websocket_fuzz_tmp/corpus" -- \
  -runs=10000 -max_len=1048576 -timeout=2 \
  -artifact_prefix="$websocket_fuzz_tmp/artifacts/"
```
