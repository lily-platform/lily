# Standalone `Injectable` downstream fixture

Bu crate, Lily ana workspace'inden bilerek ayrılmıştır. `Cargo.toml` içindeki
`[workspace]` bölümü sayesinde workspace dependency inheritance kullanamaz;
DI için yalnızca `lily_injection` doğrudan listelenir. Tokio uygulamanın async
çalıştırıcısıdır; macro destek bağımlılığı değildir.

Repository kökünden release kapısı:

```bash
cargo run \
  --manifest-path tests/fixtures/downstream_injectable/Cargo.toml \
  --locked
```

Fixture şu sözleşmeleri birlikte derler:

- `#[service(interface = dyn PaymentService)]` kaydı,
- `Arc<dyn PaymentService>` constructor injection,
- concrete ve interface resolution'ın aynı `Arc` kimliğini taşıması,
- yalnız dependency alanları taşıyan consumer struct'ında `Default` gerekmemesi,
- uygulama container'ının açık shutdown ile kapatılması.

Ek runtime state alanları olan servislerde geriye uyumluluk için mevcut
struct-level `Default` sözleşmesi sürer; macro önce bu state'i kurar, ardından
`#[inject]` alanlarını container'dan gelen değerlerle değiştirir.

## Sürüm uyumluluğu politikası

Lily crate'leri henüz yayımlanmış kararlı bir N-1 sürüme sahip olmadığı için bu
fixture bugün repository path dependency'leriyle yalnız **current/N**
sözleşmesini doğrular. Bu durum N-1 uyumluluk kanıtı olarak raporlanmamalıdır.
İlk stable yayınla birlikte ikinci fixture'ın dependency'leri gerçek yayımlanmış
N-1 sürümlerine sabitlenecek; current derive/runtime ile `cargo check --locked`
release kapısına eklenecektir.
