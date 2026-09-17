# `websocket_handshake_policy`

- Maksimum input: 64 KiB (`-max_len=65536`); header sayısı 20, middleware sayısı
  8 ve üretilen header değeri 128 byte ile ayrıca sınırlandırılır.
- Production yolu: gerçek header normalizasyonu, security-header duplicate
  denetimi, origin/subprotocol doğrulaması, async handshake middleware zinciri
  ve application-owned identity middleware'i.
- Oracle: bütün kabul/red yolları `100..=599` status üretmelidir. Sentetik secret
  sentinel hiçbir diagnostic snapshot'a sızmamalıdır; `secret_safe` zorunludur.
- Seed kaynağı: sentetik missing-origin, accepted-origin, duplicate security
  header, credential-sentinel ve protocol-header redaction kontrol dizileri.

```bash
websocket_fuzz_tmp="$(mktemp -d)"
cp -R corpus/websocket_handshake_policy "$websocket_fuzz_tmp/corpus"
mkdir -p "$websocket_fuzz_tmp/artifacts"
cargo +nightly-2026-08-15 fuzz run websocket_handshake_policy "$websocket_fuzz_tmp/corpus" -- \
  -runs=10000 -max_len=65536 -timeout=2 \
  -artifact_prefix="$websocket_fuzz_tmp/artifacts/"
```
