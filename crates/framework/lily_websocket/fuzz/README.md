# Lily WebSocket robustness targets

Bu ayrı workspace, WebSocket transport ve private backplane envelope'ları ile
handshake, connection lifecycle ve message middleware production seam'lerini
beş bounded target ile çalıştırır. Target'lar production davranışını kopyalamaz;
`lily_websocket` içindeki canonical frame/payload codec API'lerini, dispatcher
ingress'i, transport-policy helper'larını ve compiled async middleware
chain'lerini çağırır.

## Dondurulmuş araç zinciri

- `cargo-fuzz 0.13.2`
- `nightly-2026-08-15` (`rust-toolchain.toml`, `minimal` profil)
- `websocket_envelope`: en fazla 1 MiB
- `websocket_backplane_envelope`: en fazla 512 KiB
- diğer hedefler: en fazla 64 KiB

Kurulum ve sürüm doğrulama:

```bash
cargo install cargo-fuzz --version 0.13.2 --locked
cd crates/framework/lily_websocket/fuzz
cargo +nightly-2026-08-15 fuzz --version
cargo +nightly-2026-08-15 fuzz list
```

Pinli build:

```bash
cargo +nightly-2026-08-15 fuzz build websocket_envelope
cargo +nightly-2026-08-15 fuzz build websocket_handshake_policy
cargo +nightly-2026-08-15 fuzz build websocket_message_chain
cargo +nightly-2026-08-15 fuzz build websocket_lifecycle
cargo +nightly-2026-08-15 fuzz build websocket_backplane_envelope
```

## Tracked corpus'u kirletmeyen smoke

Her koşu tracked seed'leri geçici bir dizine kopyalar; crash/leak/timeout
artifact'ları da yalnız aynı geçici dizine yazılır:

```bash
cd crates/framework/lily_websocket/fuzz
websocket_fuzz_tmp="$(mktemp -d)"
cp -R ../tests/fixtures/fuzz_corpus "$websocket_fuzz_tmp/corpus"
mkdir -p "$websocket_fuzz_tmp/artifacts/websocket_envelope"
mkdir -p "$websocket_fuzz_tmp/artifacts/websocket_handshake_policy"
mkdir -p "$websocket_fuzz_tmp/artifacts/websocket_message_chain"
mkdir -p "$websocket_fuzz_tmp/artifacts/websocket_lifecycle"
mkdir -p "$websocket_fuzz_tmp/artifacts/websocket_backplane_envelope"
cargo +nightly-2026-08-15 fuzz run websocket_envelope "$websocket_fuzz_tmp/corpus/websocket_envelope" -- -runs=10000 -max_len=1048576 -timeout=2 -artifact_prefix="$websocket_fuzz_tmp/artifacts/websocket_envelope/"
cargo +nightly-2026-08-15 fuzz run websocket_handshake_policy "$websocket_fuzz_tmp/corpus/websocket_handshake_policy" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$websocket_fuzz_tmp/artifacts/websocket_handshake_policy/"
cargo +nightly-2026-08-15 fuzz run websocket_message_chain "$websocket_fuzz_tmp/corpus/websocket_message_chain" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$websocket_fuzz_tmp/artifacts/websocket_message_chain/"
cargo +nightly-2026-08-15 fuzz run websocket_lifecycle "$websocket_fuzz_tmp/corpus/websocket_lifecycle" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$websocket_fuzz_tmp/artifacts/websocket_lifecycle/"
cargo +nightly-2026-08-15 fuzz run websocket_backplane_envelope "$websocket_fuzz_tmp/corpus/websocket_backplane_envelope" -- -runs=10000 -max_len=524288 -timeout=2 -artifact_prefix="$websocket_fuzz_tmp/artifacts/websocket_backplane_envelope/"
printf '%s\n' "Qualification output: $websocket_fuzz_tmp"
```

Tracked corpus `../tests/fixtures/fuzz_corpus` altında tutulur. Bu tek kaynak
hem paketlenen regression testleri hem de fuzz koşuları tarafından kullanılır.
Cargo alt workspace olan `fuzz/` dizinini yayın arşivine dahil etmez.

Tracked corpus küçük, deterministik ve sentetiktir. V2 backplane corpus'u
namespace-scoped principal target'ın valid ve fail-closed örneklerini de taşır.
Corpus'a production payload'u, kullanıcı verisi, credential, token veya
certificate eklenmez.
`artifacts/`, `coverage/` ve `target/` git tarafından yok sayılır. Bir bulgu
triage edilmeden veya hassas veri kontrolünden geçmeden repoya taşınmaz.

Target sözleşmeleri için `fuzz_targets/*.md` dosyalarına bakın.
