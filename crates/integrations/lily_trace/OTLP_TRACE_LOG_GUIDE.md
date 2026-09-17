# Lily OTLP trace, log ve metric export rehberi

`lily_trace` tek bir owned OTLP profili kurduğunda span, structured log ve
OpenTelemetry metric sinyallerini aynı collector endpoint'ine gönderir. Console
çıktısı development görünürlüğü için aynı OTLP profiline eklenebilir; bounded
JSONL file profili ise OTLP'den ayrı bir export profilidir.

## Strict başlangıç ve lifecycle ownership

```rust
use lily_trace::{TraceConfig, TraceInstallOutcome, TracingRuntimeOwner};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = TraceConfig::try_load()?;
    let owner = match TracingRuntimeOwner::install(&config)? {
        TraceInstallOutcome::Disabled => None,
        TraceInstallOutcome::Owned(owner) => Some(owner),
    };

    let span = tracing::info_span!("api.request", http.route = "/users/:id");
    let _entered = span.enter();
    tracing::info!(lily.outcome = "success", "request completed");

    if let Some(owner) = owner {
        let report = owner.shutdown(Duration::from_secs(30)).await;
        if !report.is_success() {
            return Err(format!("telemetry shutdown incomplete: {report:?}").into());
        }
    }
    Ok(())
}
```

HTTP, WebSocket ve Consumer composition root'larında doğrudan kurulum yerine
builder'ın `tracing_config(...)`, `tracing_config_path(...)` veya
`tracing_external()` seçimi kullanılmalıdır. Owned adapter, merkezi
`lifecycle.shutdown_timeout_secs` bütçesi içinde exporter'ı flush eder.

## OTLP profili

```toml
enabled = true
level = "info"
service_name = "orders-api"
service_version = "1.0.0"
environment = "production"

[sampling]
strategy = "probability"
rate = 0.25

[export]
console = false

[export.otlp]
endpoint = "https://collector.example:4317"
protocol = "grpc"
timeout = 10
max_queue_items = 2048
max_queue_bytes = 8388608
max_batch_size = 512
flush_interval_millis = 5000
metrics_export_interval_millis = 30000
max_export_retries = 2
retry_backoff_millis = 200

[export.otlp.headers]
authorization = "Bearer deployment-injected-value"
```

Header adları ve ASCII değerleri startup sırasında doğrulanır. Header değerleri
`Debug` çıktısında redacted edilir ve hiçbir initialization mesajına yazılmaz.
`lily_trace.toml` placeholder çözmez; dosya tabanlı profilde header değeri
deployment tarafından güvenli biçimde üretilmeli veya uygulama çözülmüş değeri
`OwnedConfig` ile vermelidir. Secret değeri repository'ye yazılmamalıdır.
HTTPS endpoint platform trust root'larını kullanır. Custom CA veya mTLS için
public config alanı yoktur; desteklenmeyen bir seçenek parse ediliyor gibi
gösterilmez.

`environment`, yalnız OpenTelemetry `deployment.environment` resource
attribute'ıdır. Lily'nin development/production config modunu değiştirmez.

## Sayısal alanların export sözleşmesi

HTTP event'lerinin `duration_ms` ve `duration_us` alanları `Instant` üzerinden
hesaplanan sayısal değerlerdir; alanın birimi adında belirtilir. OTLP karşılığı
`doubleValue`, JSONL karşılığı JSON number'dır. HTTP status/byte ve WebSocket
mesaj boyutu span alanları `intValue` olarak üretilir. OTLP JSON'un `intValue`
içeriğini ondalık metinle kodlaması, alanın `stringValue` olması anlamına gelmez.

OTLP integer alanı signed 64 bittir. Lily'nin bu boyut span'leri ve OTLP log
visitor'ının `u64` değerleri `i64::MAX` üstünde bu sınıra doyurulur; negatif sayıya
sarılmaz. Bu sınırda tam unsigned kesinlik vaat edilmez. File JSON formatter'ın
unsigned değerleri saklama davranışı değişmez. Genel kullanıcı `Debug` alanları
ve sayıya benzeyen string'ler otomatik olarak sayıya çevrilmez; kullanıcı span'ı
oluştururken export'a uygun sayısal tip vermelidir.

## Bounded JSONL file profili

```toml
enabled = true
level = "info"
service_name = "orders-api"

[sampling]
strategy = "always"
rate = 1.0

[export.file]
path = "/var/log/orders/application.jsonl"
rotation = "daily"
max_file_bytes = 134217728
max_files = 7
buffered_lines = 4096
max_record_bytes = 16384
```

File profili:

- yalnız JSONL üretir;
- hourly/daily sınırına ve byte sınırına göre rotation yapar;
- aktif dosya dâhil `max_files` kadar dosya tutar;
- request path'inde dosya I/O'su yapmaz;
- bounded ve lossy queue kullanır;
- oversized, queue-full ve writer-failure kayıplarını shutdown raporlar;
- writer veya rotation hatasında fail-closed davranır.

File profili console veya OTLP ile aynı config içinde etkinleştirilemez. Log
dosyasının parent dizini önceden var olmalı ve process tarafından yazılabilir
olmalıdır; aksi hâlde startup typed error ile durur. Aynı host/filesystem
üzerinde eşzamanlı çalışan her process farklı bir `path` kullanmalıdır; file
rotation process-local'dır ve paylaşılan dosya için cross-process kilit sunmaz.

## File ve console kimlik korelasyonu

Lily'nin kurduğu file ve console profilleri, event'in bağlı olduğu span'dan
OpenTelemetry kimliklerini otomatik alır. Attribute'a yeni bir seçenek veya
uygulama tarafından elle kaydedilmiş kimlik alanı eklemek gerekmez.

JSONL kaydının üst seviyesinde `trace_id` 32 küçük hex karakter, `span_id` 16
küçük hex karakter, `trace_flags` iki küçük hex karakter olarak bulunur.
Console satırı aynı üç alanla başlar. OTLP yanındaki console çıktısı da aynı
SDK context'ini kullanır. Uygulama alanları JSONL'de `fields` veya `span` içinde
kalır; aynı isimli uygulama alanları üst seviyedeki kimlikleri ezmez.

Geçerli incoming W3C context aynı trace'i sürdürür; yeni span kendi span ID'sini
alır. Protokol sınırlarında `lily_trace::set_parent` span kullanılmadan ve child
oluşturulmadan çağrılır. Eksik/geçersiz başlık için boş context atanması, önceden
local parent'ı olan request span'ında da tek ve sabit bir root kimliği oluşturur;
handler, terminal kayıt ve son span export'u aynı trace ID'yi kullanır.
Event explicit parent belirtiyorsa kimlik o span'a aittir. `parent: None`
veya span dışında oluşan bir event için kimlik alanları atlanır. Sampling flag'i
kapalı bir context geçerli olabilir ve `trace_flags = "00"` olarak yazılır;
kimlik bulunması span'ın OTLP'ye export edildiğini garanti etmez.

`span` / `spans` alanları tracing registry hiyerarşisini korur. Remote parent
ataması OpenTelemetry parent ilişkisini değiştirebildiğinden, bu isim listesi
tek başına W3C parent ağacının kanıtı değildir. Aynı adlı eşzamanlı metotları
başlangıç/bitiş kayıtlarında `(trace_id, span_id)` ile eşleştirin.

DI scope'u trace sahibini oluşturulduğu anda yakalar. Ayrı görevdeki normal
close, Drop ve manager shutdown yolları `di.scope.dispose` altında aynı trace'e
bağlanır; disposer future iptal edilirken üretilen event'ler de orijinal
subscriber'a gider. Bu snapshot parent span'ı açık tutmaz. Bu nedenle cleanup
JSONL kaydının `spans` isim listesinde tamamlanmış request bulunmayabilir;
OTel `parentSpanId` ilişkisi ve JSONL kimlikleri korunur.

WebSocket'te `websocket.transport` ve `websocket.upgrade.read` uzak parent henüz
bilinmediği için yerel kalır. Header okunduktan sonra `websocket.connection`
parent'ı atanır; handshake/identity/message ve `websocket.connection.cleanup`
bu kimliği izler. Parent sonradan değiştirilmez. Handshake histogramının süresi
TLS öncesinden ölçülmeye devam eder; `websocket.handshake` span süresi artık
header okumasından sonraki aşamayı ölçer.

Manuel span'ların console CLOSE kaydı da kapanan span'ın kimliğini taşır.
Makrolu metotların mevcut başlangıç/terminal çifti korunur; ikinci bir CLOSE
kaydı üretilmez. Kimlik byte'ları `max_record_bytes` sınırına dahildir; sınırlı
dosya kuyruğunun kabul, kayıp sayımı ve shutdown davranışı aynı yoldan yürür.

Bu çıktı sözleşmesi Lily'nin kurduğu profillere aittir. `tracing_external()`
ile uygulamanın sahip olduğu özel formatter ayrıca bu davranışı sağlamalıdır.
JSONL şeması katı doğrulanıyorsa üç yeni üst seviye alan kabul edilmelidir.

## Sinyal sonucu

- Span'lar OTLP trace exporter'a gider.
- `tracing` event'leri OTLP log exporter'a gider.
- Framework OpenTelemetry instruments OTLP metric exporter'a gider.
- Console profili yalnız stdout görünürlüğü sağlar.
- File profili yalnız local JSONL span/event çıktısı sağlar; metric export etmez.

Jaeger kullanılacaksa Jaeger'ın OTLP receiver'ı hedeflenir. Lily ayrı bir
Jaeger-native exporter veya paralel `[logging]` config yüzeyi sunmaz.

## DI çözümleme tanısı

`Extensions::get_service` ve onu kullanan typed resolve çağrıları,
`di.service.resolve` span/lifecycle kayıtlarını DEBUG seviyesinde üretir.
INFO profilinde başarılı çözümleme başına log yazılmaz. DEBUG açıldığında
`di.requested_type`, `di.implementation_type` ve `di.lifetime`
(`singleton`, `scoped`, `transient`) alanları görünür. Kayıt bulunamadığında
implementation/lifetime yoktur; framework tarafından eklenen handle'lar singleton
olarak tanımlanır. Type isimleri Rust metadata'sıdır, refactor ile değişebilir.

Erken dönüşler dahil çözümleme hataları, filtre izin verdiğinde ERROR seviyesinde
`DI service resolution failed` üretir. `lily.error_code`, `di.scope_required`
gibi sabit bir framework kodudur; hata mesajı, servis nesnesi veya ProcessContext
metadata'sı biçimlendirilmez. DEBUG kapalıysa bu event çağıranın aktif span'ına
bağlıdır. Aynı hatanın bağımlılık zincirinde birden fazla çözümlemeyi başarısız
kılması, her başarısız invocation için ayrı tanı üretebilir.

Bu değişiklik DI metric label'larını genişletmez; sayaçlar ve histogramlar log
filtresinden bağımsızdır. Build, scope disposal ve shutdown seviyeleri korunur.
`lily_trace_macros` parser'ına expression-field desteği eklemek gerekmez;
DI'nin yerel span tanımı mevcut ortak lifecycle mekanizmasını kullanır.

Kalıcı kabul testleri:

```sh
cargo test -p lily_injection --test resolution_telemetry --test resolution_exporters
cargo test -p lily_injection --test resolution_exporters -- --ignored --exact live_collector_preserves_di_filtering_and_shutdown_delivery --nocapture
cargo test -p lily_websocket --test build_cancellation_deadline -- --ignored --exact build_deadline_flushes_terminal_spans_logs_and_metrics_to_collector --nocapture
```

İlk komut INFO/DEBUG/OFF altında gerçek DI çözümlemesi, eşzamanlı invocation,
metadata, erken hata ve metric tip/adetlerini; ayrı process'lerde kurulu JSONL
profillerini sınar. Diğer komutlar sabitlenmiş Collector ile ham OTLP teslimini,
console kimlik eşleşmesini ve iptal edilen build'in rollback deadline'ından sonra
span/log/metric flush işlemini doğrular. Docker ve önceden bulunan qualification
image'ı gerekir. Genel method matrisi [LIVE_OTLP_QUALIFICATION.md](LIVE_OTLP_QUALIFICATION.md)
üzerinden ayrıca çalışır.

DEBUG seviyesinde OpenTelemetry'nin kendi tanı makroları hem örtük hem açık
`message` alanı verebilir. JSONL formatter böyle bir event'i son değer korunacak
şekilde tek alan olarak yazar; normal event'ler doğrudan serialize edilir.
Bu, span instrumentation'ında yinelenen terminal alanlarını kabul etmek anlamına
gelmez: ham OTLP testleri bunları ayrıca reddeder. Sayı/bool tipleri korunur;
JSON nesnesinde aynı anahtarı tekrar yazmaya güvenen tüketiciler yerine tek alan
sözleşmesi kullanılır.

`TracingRuntimeStatus::Shutdown`, shutdown girişiminin başladığını da kapsar.
Flush/teslim kanıtı için sahiplik API'sinin terminal raporu ve gerçek worker
join sonuçları gerekir. Canlı build testi, iptal sırasında yalnız bu bayrağa
bakarak process'i bitirmenin metric flush'ı yarıda bırakabildiğini doğrular;
framework'ün exporter sahipliğini değiştirmeden testin bekleme koşulu güçlendirilmiştir.
