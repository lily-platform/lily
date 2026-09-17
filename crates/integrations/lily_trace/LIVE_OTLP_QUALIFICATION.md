# Canlı OTLP method doğrulaması

Bu test `lily_trace` runtime'ını gerçek OpenTelemetry Collector Contrib ile
çalıştırır. Akış: makro → Lily'nin bounded exporter görevleri → OTLP gRPC →
Collector receiver → Collector file exporter → JSON içerik doğrulaması.
Exporter sayaçlarının başarılı olması tek başına testi geçirmez.

## Çalıştırma

Docker daemon erişimi ve aşağıdaki sabit Collector imajı gerekir. Test imaj
indirmez; eksik imajı veya Docker erişimini atlayarak başarılı sayılmaz.
Referans imaj Collector Contrib **0.155.0 / linux-amd64**'dır:

```sh
docker pull otel/opentelemetry-collector-contrib@sha256:4935caa35e9a4cb387e35732e8fb22b2b5759af8d12e7043357f03837f6e8df5
cargo test -p lily_trace --test live_otlp_qualification --offline --locked -- \
  --ignored --exact full_sampling_exports_and_awaits_terminal_ledger --nocapture
cargo test -p lily_trace --test live_otlp_qualification --offline --locked -- \
  --ignored --exact console_identities_match_the_live_otlp_method_records --nocapture
```

Her çalışma kendi Collector container'ını oluşturur; yalnız loopback üzerinde
rastgele portlar açar. Config Docker `cp` ile aktarılır; host dizinleri container'a
bağlanmaz. Health endpoint hazır olmadan iş yükü başlamaz. Başarıda ve yakalanan
test panic/assertion hatalarında container temizlenir. Docker komutları, hazır
olma ve exporter shutdown adımları süre sınırına sahiptir. Test sürecinin zorla
sonlandırılması gibi cleanup çalıştırmayan durumlarda `lily.qualification=otlp-methods`
etiketi, geride kalan test container'ını belirlemeye yardımcı olur.

Önceki testte kullanılan `LILY_TEST_OTLP_ENDPOINT` artık gerekli değildir:
test, çıktısını okuyamadığı harici bir endpoint'e gönderim yapmaz. Evidence
üst dizini isteğe bağlı `LILY_TEST_OTLP_REPORT_DIR` ile değiştirilebilir.

## Zorunlu kontroller

Dokuz method çağrısı, üç request parent'ı, **tam 12 span ve 18 log** doğrulanır:

| Case | İşlem | Beklenen sonuç |
| --- | --- | --- |
| 1 | `async_trait` başarı | `completed / success`, kod yok, OTel OK |
| 2 | `async_trait` uygulama reddi | `completed / rejected / invalid_credentials`, OTel OK |
| 3 | `async_trait` teknik hata | `completed / error / service_unavailable`, OTel ERROR |
| 4 | Native async başarı | `completed / success` |
| 5 | `result` olmayan sync `Err` | `completed`, outcome ve kod yok |
| 6 | Başlamış future başka dispatcher altında düşürülür | `dropped`, outcome ve kod yok |
| 7 | Başlamış Tokio task iptal edilir | `dropped`, outcome ve kod yok |
| 8 | Async gövdede panic | `panicked`, OTel ERROR, uygulama kodu yok |
| 9 | Shutdown öncesi son sync çağrı | Kuyruktaki son span/loglar da teslim edilir |

Ayrıca hiç poll edilmeyen bir future için kayıt üretilmediği doğrulanır.
İlk üç çağrı aynı anda askıda tutulur; oneshot bariyerleriyle hepsinin girdiği
görüldükten sonra serbest bırakılır. Her çağrının kendi request parent'ı vardır.
Beklenen trace/span kimlikleri metot çalışırken alınır, Collector çıktısından
türetilmez. Request ilişkileri, kimliklerin await boyunca korunması ve log/span
eşleşmeleri bu bağımsız kimliklere karşı kontrol edilir.

Her method için tam bir başlangıç ve bir terminal event hem span içinde hem log
sinyalinde aranır. Outcome/kod, numeric `doubleValue` süre, timestamp sırası,
sampling flag'i, service resource, caller target ve kayıp alan/event sayaçları
kontrol edilir. Süre; `Instant` ile bağımsız ölçülen caller üst sınırı ve askıda
kalma alt sınırıyla karşılaştırılır. Span/event/log süreleri eşit olmalıdır.
Uygulama dönüş değeri ve hata payload'ındaki gizli test işareti hiçbir sinyalde
bulunmamalıdır. Hata sınıflandırması yalnız iki tamamlanmış `Err` için çalışır.

İkinci komut aynı gerçek Collector senaryosunu ayrı process'te OTLP + console
profiliyle çalıştırır. OTLP kontrollerine ek olarak dokuz invocation'ın console
başlangıç/terminal çiftlerini bağımsız SDK trace/span kimlikleri ve sampling
flag'iyle birebir eşleştirir. `console.stdout`, `console.stderr` ve
`console-correlation.json` aynı evidence dizininde saklanır.

Test tek Tokio thread'i kullanır. Son senkron metot ile shutdown çağrısı
arasında `await` bulunmaz; böylece o metodun kuyruğa koyduğu kayıtlar exporter
görevleri tarafından henüz işlenmemiştir. Shutdown raporunda kabul edilen ve
export edilen sayılar tam eşleşmeli; kayıp, reddedilen, bekleyen byte/kayıt,
retry ve export hatası sıfır olmalıdır. Metric probe da shutdown ile teslim
edilir. Ardından Collector düzgün kapatılıp dosyaları flush edilir ve JSON
dosyalarının tamamı okunur; ilk eşleşmeyi bulunca erken başarılı sayılmaz.

Doğrulayıcının hataları yakaladığı, **gerçek alınan verinin kopyaları** üzerinde
13 olumsuz kontrolle sınanır: eksik/çift log, çift span/attribute, yanlış trace kimliği,
hata kodu, outcome, süre, status, eksik span event'i, kayıp attribute,
payload sızıntısı ve eksik metric. Bunlardan biri kabul edilirse test başarısızdır.

## Evidence

Her çalışmanın dosyaları `target/otlp-qualification/<pid>-<timestamp>/` altında
korunur: `summary.json`, `expected.json`, `shutdown.txt`, Collector config,
imaj digest/kimlik bilgisi, binary sürümü, kapanış durumu, Collector logu ve
`capture/{traces,logs,metrics}.jsonl`. `summary.json` ancak doğrulama ve container
cleanup başarılıysa `passed` olur.

Bu kontrol belirli sürümdeki yerel gRPC aktarımı ve method sözleşmesini kapsar;
uzak backend depolaması, TLS/auth, ağ kesintisi/retry veya yük/soak testi değildir.

Collector file exporter davranışı için kullanılan kaynak:
[0.155.0 file exporter dokümanı](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.155.0/exporter/fileexporter/README.md).
