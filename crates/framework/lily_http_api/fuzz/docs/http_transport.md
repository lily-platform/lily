# `http_transport`

- Maksimum input: 64 KiB (`-max_len=65536`).
- Production yolu: `HttpServer::connection_builder` üzerinden Hyper HTTP/1 +
  HTTP/2 auto codec.
- Oracle: parse/protocol reddi kabul edilir; panic, 100 ms'yi aşan server taskı
  veya abort sonrası join edilmeyen task bulgudur.
- Seed kaynağı: sentetik CL, chunked, TE+CL, conflicting CL, duplicate header,
  invalid target ve HTTP/2 preface örnekleri.

```bash
tmp_dir="$(mktemp -d)"
cp -R corpus/http_transport "$tmp_dir/corpus"
mkdir -p "$tmp_dir/artifacts"
cargo +nightly-2026-08-15 fuzz run http_transport "$tmp_dir/corpus" -- \
  -runs=10000 -max_len=65536 -artifact_prefix="$tmp_dir/artifacts/"
```
