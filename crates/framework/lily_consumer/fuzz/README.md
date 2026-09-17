# Lily Consumer robustness targets

Bu ayrı workspace, Consumer/Queue production
doğrulama/planlama/lifecycle/settlement seam'lerini sekiz bounded target ile
çalıştırır. Target'lar runtime davranışını kopyalamaz; `lily_consumer` ve
`lily_queue` içindeki canonical admission, metadata, extractor, settlement,
`ConsumerPlan` ve `lily_shutdown` tiplerine yalnız owned fuzz input adaptasyonu
yapar.

## Dondurulmuş araç zinciri

- `cargo-fuzz 0.13.2`
- `nightly-2026-08-15` (`rust-toolchain.toml`, `minimal` profil)
- hedef başına azami input: 64 KiB

Kurulum ve sürüm doğrulama:

```bash
cargo install cargo-fuzz --version 0.13.2 --locked
cd crates/framework/lily_consumer/fuzz
cargo +nightly-2026-08-15 fuzz --version
cargo +nightly-2026-08-15 fuzz list
```

Pinli build:

```bash
cargo +nightly-2026-08-15 fuzz build consumer_config
cargo +nightly-2026-08-15 fuzz build consumer_plan
cargo +nightly-2026-08-15 fuzz build consumer_shutdown_state
cargo +nightly-2026-08-15 fuzz build consumer_runtime_ownership
cargo +nightly-2026-08-15 fuzz build queue_delivery_envelope
cargo +nightly-2026-08-15 fuzz build queue_amqp_metadata
cargo +nightly-2026-08-15 fuzz build queue_extractor_plan
cargo +nightly-2026-08-15 fuzz build queue_settlement_state
```

## CAP-Q-06G operator handoff

`consumer_runtime_ownership` target'ının pinli build'i 2026-08-31 tarihinde
başarılı oldu. Build sonucu fuzz yürütme sonucu değildir. Son operatör
koşusunda `managed-timeout-force` replay'i 2 ms, `dropped-readiness-panic-drain`
replay'i 4 ms içinde geçti. İlk 10.000 iterasyon yaklaşık `#8192`'de target'ın
dahili 250 ms dış deadline'ına ulaştı ve şu 13 byte artifact'ı üretti:

- tracked ad: `waiter-abort-paused-drain`
- hex: `48 2b 63 09 00 00 00 00 00 00 00 2b 0a`
- base64: `SCtjCQAAAAAAAAArCg==`
- SHA-256: `cfd7c7e876b61d2389bcb705ce9f8a04ee83049848b389fe645e6b25f2bf947f`
- canonical printable olay eşdeğeri: `dacTcccccccac`

Artifact sabit input olarak 20 kez replay edildi ve 20/20 geçti; toplam süre
193 ms oldu. Bu kanıt deterministik production kilitlenmesine değil, gerçek
duvar saatini kullanan eski fuzz runtime'ında host scheduling'e bağlı target
deadline dalgalanmasına işaret eder. Exact artifact tracked corpus'a alındı.
Transport-free target current-thread Tokio runtime'ını paused logical time ile
başlatır; production 10 ms cleanup/force bütçesi ve target'ın 250 ms outer
deadline'ı değişmez. libFuzzer `-timeout=2` watchdog'u bağımsız gerçek duvar
saati sınırı olarak kalır.

Ortam fuzz yürütmesini operatöre bıraktığı için üç tracked seed replay'i, temiz
10.000 iterasyon ve boş artifact dizini görülmeden CAP-Q-06G tamamlanmış
sayılmaz. Aşağıdaki kapılar tracked corpus'u değiştirmez:

```bash
cd crates/framework/lily_consumer/fuzz
cap_q_06g_fuzz_tmp="$(mktemp -d)"
cp -R corpus/consumer_runtime_ownership "$cap_q_06g_fuzz_tmp/corpus"
mkdir -p "$cap_q_06g_fuzz_tmp/artifacts"

cargo +nightly-2026-08-15 fuzz run consumer_runtime_ownership "$cap_q_06g_fuzz_tmp/corpus/managed-timeout-force" -- -runs=1 -max_len=65536 -timeout=2 -artifact_prefix="$cap_q_06g_fuzz_tmp/artifacts/"
cargo +nightly-2026-08-15 fuzz run consumer_runtime_ownership "$cap_q_06g_fuzz_tmp/corpus/dropped-readiness-panic-drain" -- -runs=1 -max_len=65536 -timeout=2 -artifact_prefix="$cap_q_06g_fuzz_tmp/artifacts/"
cargo +nightly-2026-08-15 fuzz run consumer_runtime_ownership "$cap_q_06g_fuzz_tmp/corpus/waiter-abort-paused-drain" -- -runs=1 -max_len=65536 -timeout=2 -artifact_prefix="$cap_q_06g_fuzz_tmp/artifacts/"
cargo +nightly-2026-08-15 fuzz run consumer_runtime_ownership "$cap_q_06g_fuzz_tmp/corpus" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$cap_q_06g_fuzz_tmp/artifacts/"

find "$cap_q_06g_fuzz_tmp/artifacts" -maxdepth 1 -type f -print
artifact_count="$(find "$cap_q_06g_fuzz_tmp/artifacts" -maxdepth 1 -type f | wc -l)"
test "$artifact_count" -eq 0
printf '%s\n' "CAP-Q-06G fuzz output: $cap_q_06g_fuzz_tmp"
```

İlk komut `6d 54 63 66 79 0a` girdisini tekrar oynatır. Beklenen production
cleanup sırası `StopAdmissionAsync -> ForceDrainAsync -> CloseAsync`; her çağrı
tam bir kez, sonuç `ForcedCompleted` ve shutdown accounting sonucu reconciled
olmalıdır. İkinci komut `/x:Pp\n` girdisini tekrar oynatır. Bu girdi test-support
drain panic enjeksiyonunun libFuzzer'ın abort eden panic hook'una takılmadan
production `catch_unwind` containment ve forced reconciliation yoluna ulaşmasını
doğrular. Enjeksiyon bunun için sabit ve uygulama verisi içermeyen bir payload
ile `resume_unwind` kullanır; lifecycle invariant'leri gevşetilmez. İki replay
yanında üçüncü komut exact waiter-abort/paused-drain artifact'ını sanal zamanda
tekrar oynatır. Üç replay, 10.000 iterasyon ve sıfır artifact birlikte PASS
olmadan CAP-Q-06G tamamlanmış sayılmaz.

## Tracked corpus'u kirletmeyen smoke

Her koşu tracked seed'leri geçici bir dizine kopyalar; crash/leak/timeout
artifact'ları da yalnız aynı geçici dizine yazılır:

```bash
cd crates/framework/lily_consumer/fuzz
consumer_fuzz_tmp="$(mktemp -d)"
cp -R corpus "$consumer_fuzz_tmp/corpus"
mkdir -p "$consumer_fuzz_tmp/artifacts/consumer_config"
mkdir -p "$consumer_fuzz_tmp/artifacts/consumer_plan"
mkdir -p "$consumer_fuzz_tmp/artifacts/consumer_shutdown_state"
mkdir -p "$consumer_fuzz_tmp/artifacts/consumer_runtime_ownership"
mkdir -p "$consumer_fuzz_tmp/artifacts/queue_delivery_envelope"
mkdir -p "$consumer_fuzz_tmp/artifacts/queue_amqp_metadata"
mkdir -p "$consumer_fuzz_tmp/artifacts/queue_extractor_plan"
mkdir -p "$consumer_fuzz_tmp/artifacts/queue_settlement_state"
cargo +nightly-2026-08-15 fuzz run consumer_config "$consumer_fuzz_tmp/corpus/consumer_config" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$consumer_fuzz_tmp/artifacts/consumer_config/"
cargo +nightly-2026-08-15 fuzz run consumer_plan "$consumer_fuzz_tmp/corpus/consumer_plan" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$consumer_fuzz_tmp/artifacts/consumer_plan/"
cargo +nightly-2026-08-15 fuzz run consumer_shutdown_state "$consumer_fuzz_tmp/corpus/consumer_shutdown_state" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$consumer_fuzz_tmp/artifacts/consumer_shutdown_state/"
cargo +nightly-2026-08-15 fuzz run consumer_runtime_ownership "$consumer_fuzz_tmp/corpus/consumer_runtime_ownership" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$consumer_fuzz_tmp/artifacts/consumer_runtime_ownership/"
cargo +nightly-2026-08-15 fuzz run queue_delivery_envelope "$consumer_fuzz_tmp/corpus/queue_delivery_envelope" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$consumer_fuzz_tmp/artifacts/queue_delivery_envelope/"
cargo +nightly-2026-08-15 fuzz run queue_amqp_metadata "$consumer_fuzz_tmp/corpus/queue_amqp_metadata" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$consumer_fuzz_tmp/artifacts/queue_amqp_metadata/"
cargo +nightly-2026-08-15 fuzz run queue_extractor_plan "$consumer_fuzz_tmp/corpus/queue_extractor_plan" -- -runs=10000 -max_len=65536 -timeout=2 -artifact_prefix="$consumer_fuzz_tmp/artifacts/queue_extractor_plan/"
cargo +nightly-2026-08-15 fuzz run queue_settlement_state "$consumer_fuzz_tmp/corpus/queue_settlement_state" -- -runs=10000 -max_len=256 -timeout=2 -artifact_prefix="$consumer_fuzz_tmp/artifacts/queue_settlement_state/"
printf '%s\n' "Qualification output: $consumer_fuzz_tmp"
```

AMQP input adapter'ı flat node biçimi kullanır. Arbitrary decode recursive
değildir; production tiplerine çeviri ayrıca 6 nesting level, 512 toplam node,
80 top-level header ve composite başına 32 child ile sınırlıdır.

Tracked corpus küçük, deterministik ve sentetiktir. Corpus'a production
payload'u, kullanıcı verisi, credential, token veya certificate eklenmez.
`consumer_plan` seed'leri exact duplicate rejection'ı, sıfır/azami schema
version sınırlarını ve tek physical queue üzerinde birden fazla version/content
handler'ının gruplanmasını da dondurur.
`artifacts/`, `coverage/` ve `target/` git tarafından yok sayılır. Bir bulgu
triage edilmeden veya hassas veri kontrolünden geçmeden repoya taşınmaz.

Yalnız CAP-Q-01E'nin dört yeni queue target'ını terminali erken kapatmadan,
target bazında sonuç toplayarak çalıştırmak için workspace kökünden:

```bash
bash crates/framework/lily_consumer/fuzz/run_cap_q_01e_smoke.sh
```

Target sözleşmeleri için `fuzz_targets/*.md` dosyalarına bakın.
