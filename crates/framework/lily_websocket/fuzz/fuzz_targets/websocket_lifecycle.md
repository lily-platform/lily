# `websocket_lifecycle`

- Maksimum input: 64 KiB (`-max_len=65536`); event ve connection middleware
  sayıları production fuzz seam'inde sabit üst sınırlarla tutulur.
- Production yolu: gerçek compiled connection middleware zinciri,
  `ConnectionManager` ve `ConnectionCleanupRegistry` lifecycle geçişleri.
- Oracle: iteration sonunda manager connection, cleanup registry kaydı ve alınmış
  semaphore permit kalmaz; close unwind LIFO'dur. Her iteration 500 ms absolute
  deadline'a sahiptir; timeout taskı abort+await eder.
- Seed kaynağı: sentetik connect/shutdown, admission rejection ve pending
  cancellation kontrol dizileri.

```bash
websocket_fuzz_tmp="$(mktemp -d)"
cp -R corpus/websocket_lifecycle "$websocket_fuzz_tmp/corpus"
mkdir -p "$websocket_fuzz_tmp/artifacts"
cargo +nightly-2026-08-15 fuzz run websocket_lifecycle "$websocket_fuzz_tmp/corpus" -- \
  -runs=10000 -max_len=65536 -timeout=2 \
  -artifact_prefix="$websocket_fuzz_tmp/artifacts/"
```
