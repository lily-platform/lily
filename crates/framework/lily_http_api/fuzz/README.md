# Lily HTTP cargo-fuzz workspace

Bu bağımsız workspace yalnızca gerçek `cargo-fuzz` target'larını içerir. Araç
zinciri bilinçli olarak `cargo-fuzz 0.13.2` ve `nightly-2026-08-15` sürümlerine
sabitlenmiştir. Beklenen HTTP/parser/policy reddi crash değildir; panic, deadline
aşımı, OOM ve cleanup/invariant ihlali bulgudur.

Target sözleşmeleri:

- [`http_transport`](docs/http_transport.md)
- [`http_app_dispatch`](docs/http_app_dispatch.md)
- [`http_multipart`](docs/http_multipart.md)
- [`http_middleware_chain`](docs/http_middleware_chain.md)

Ön koşul:

```bash
rustup toolchain install nightly-2026-08-15 --profile minimal
cargo install cargo-fuzz --version 0.13.2 --locked
```

Her smoke koşusunda tracked corpus önce geçici dizine kopyalanır; fuzzer'ın yeni
coverage girdileri ve artifact'leri repository'ye yazılmaz. PR/local kapısı target
başına `10_000` run'dır. Nightly/manual profil target başına en az 10 dakika;
release profili target başına 60 dakika veya `10^7` input'tur.

Corpus yalnız sentetik protokol örnekleri içerir. Credential, production request
ve kişisel veri eklenmez. `artifacts/`, coverage çıktıları ve profiler dosyaları
ignore edilir; kalıcı hale getirilecek bir bulgu önce minimize edilip küçük,
isimlendirilmiş bir regression seed'ine çevrilir.
