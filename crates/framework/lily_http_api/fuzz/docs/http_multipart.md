# `http_multipart`

- Maksimum input: 64 KiB; structured case production request-body limitini
  64/256/1024/4096 byte olarak seçer.
- Production yolu: streaming `RequestBodyStream` -> `RequestExt::multipart` ->
  Lily'nin Multer tabanlı bounded adapter'ı.
- Oracle: typed malformed/oversized rejection kabul edilir; 100 ms deadline
  aşımı artifact'tir. Timeout yolu taskı abort edip await eder ve stream Drop
  sayacının sıfıra dönmesini doğrular.
- Seed kaynağı: truncated/quoted/embedded boundary, duplicate field/content-type,
  invalid header, oversized part ve sentetik binary file.

```bash
tmp_dir="$(mktemp -d)"
cp -R corpus/http_multipart "$tmp_dir/corpus"
mkdir -p "$tmp_dir/artifacts"
cargo +nightly-2026-08-15 fuzz run http_multipart "$tmp_dir/corpus" -- \
  -runs=10000 -max_len=65536 -artifact_prefix="$tmp_dir/artifacts/"
```
