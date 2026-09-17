# `websocket_message_chain`

- Maksimum input: 64 KiB (`-max_len=65536`); middleware sayısı production
  fuzz seam'i tarafından sabit üst sınırda tutulur.
- Production yolu: gerçek `CompiledWsMessageMiddlewareChain`; before/handler/
  after sonuçları ile rejection, close, error ve pending cancellation yolları.
- Oracle: entered frame sayısı sınırı aşmaz, after yalnız entered frame'lerde
  denenir ve unwind sırası LIFO'dur. Her iteration 250 ms absolute deadline'a
  sahiptir; timeout taskı abort+await eder.
- Seed kaynağı: sentetik full-unwind, rejection ve pending-cancellation kontrol
  dizileri.

```bash
websocket_fuzz_tmp="$(mktemp -d)"
cp -R corpus/websocket_message_chain "$websocket_fuzz_tmp/corpus"
mkdir -p "$websocket_fuzz_tmp/artifacts"
cargo +nightly-2026-08-15 fuzz run websocket_message_chain "$websocket_fuzz_tmp/corpus" -- \
  -runs=10000 -max_len=65536 -timeout=2 \
  -artifact_prefix="$websocket_fuzz_tmp/artifacts/"
```
