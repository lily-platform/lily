# `http_app_dispatch`

- Maksimum input: 64 KiB; body 32 KiB, header 8 adetle ayrıca sınırlandırılır.
- Production yolu: gerçek Lily `Request::from_transport_parts`, App route lookup,
  parametre route'u, guard, terminal handler ve safe error materializer.
- Oracle: request rejection kabul edilir; completed status `100..=599`, body <=64
  KiB ve request-local state iteration sonunda boş olmalıdır.
- Seed kaynağı: sentetik success, guard 403, handler 500 ve 404 vakaları.

```bash
tmp_dir="$(mktemp -d)"
cp -R corpus/http_app_dispatch "$tmp_dir/corpus"
mkdir -p "$tmp_dir/artifacts"
cargo +nightly-2026-08-15 fuzz run http_app_dispatch "$tmp_dir/corpus" -- \
  -runs=10000 -max_len=65536 -artifact_prefix="$tmp_dir/artifacts/"
```
