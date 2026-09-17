# `http_middleware_chain`

- Maksimum input: 64 karar/layer (`-max_len=64`).
- Production yolu: `HttpMiddlewareChain`, typed error materialization ve gerçek
  around-style `HttpNext` recursion.
- Oracle: terminal ve error writer en fazla bir kez çalışır; entered frames LIFO
  reconcile olur. `Pending` kararı 50 ms'de abort+await edilir; active frame ve
  semaphore permit başlangıç değerine dönmeden iteration tamamlanmaz.
- Seed kaynağı: reverse unwind, short circuit, rejection, nested after-error ve
  pending cancellation dizileri. Fuzz bulgularından yalnız küçük ve sentetik
  kontrol byte'ları regression corpus'una alınır.

```bash
tmp_dir="$(mktemp -d)"
cp -R corpus/http_middleware_chain "$tmp_dir/corpus"
mkdir -p "$tmp_dir/artifacts"
cargo +nightly-2026-08-15 fuzz run http_middleware_chain "$tmp_dir/corpus" -- \
  -runs=10000 -max_len=64 -artifact_prefix="$tmp_dir/artifacts/"
```
