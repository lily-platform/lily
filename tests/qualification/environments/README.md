# V1 qualification environment

`v1-matrix.json`, support profile ile çalıştırılacak oldest/latest aday cell'leri eşler. `v1.compose.yml` ise bunları dış ağa port açmayan ephemeral servisler olarak tanımlar.

Compose dosyasında moving tag veya varsayılan credential yoktur. Bir RC çalışması seçilen `LILY_Q_*_IMAGE` değerlerini `name@sha256:...` biçiminde, credential'ları da yalnız ephemeral secret kaynağından vermelidir. Runner; resolve edilmiş digest'leri, Compose config hash'ini, servislerin raporladığı exact sürümleri ve seçilen matrix cell'lerini evidence bundle'a yazar. Bu kayıtlar oluşana kadar support profile `qualification: pending` kalır.

Bu manifest test altyapısıdır; repository içinde image seçilmiş veya canlı qualification yapılmış olduğu anlamına gelmez.

## Consumer için disposable canlı ortam

`consumer.compose.yml` ve `consumer_live.py`, Consumer lifecycle fixture'ları
için ayrı RabbitMQ management, PostgreSQL 17 ve MongoDB replica-set servislerini
hazırlar. Mevcut container'ları kullanmaz. Portlar yalnız `127.0.0.1` üzerinde
dinamik atanır; servis verileri tmpfs üzerindedir. MongoDB, transaction/fault
testleri için tek üyeli replica set ve `enableTestCommands=1` ile çalışır.

Çalışan Docker daemon, Docker Compose, Python 3 ve repository Rust toolchain'i
gereklidir. Sandbox kullanılıyorsa Docker socket ve localhost test bağlantılarına
erişim sağlanmalıdır. Host'un Docker socket izinlerini değiştirmek gerekmez.

Üç image'ı `name@sha256:...` biçiminde seçin. RabbitMQ image'ı management plugin'i
içermelidir; PostgreSQL fixture'ı sürüm 17'nin data directory düzenini kullanır.
Image'ların digest'leri `docker image inspect IMAGE --format '{{index .RepoDigests 0}}'`
ile alınabilir. Bu helper tek seçilmiş ortamı doğrular; V1 minimum/maximum support
matrix'inin tamamının geçtiği anlamına gelmez.

```sh
CONSUMER_FIXTURE_DIR="$(mktemp -d /tmp/lily-consumer-live-XXXXXXXX)"
python3 tests/qualification/environments/consumer_live.py up "$CONSUMER_FIXTURE_DIR" \
  --rabbitmq-image "$RABBITMQ_IMAGE" \
  --postgresql-image "$POSTGRESQL_IMAGE" \
  --mongodb-image "$MONGODB_IMAGE"

python3 tests/qualification/environments/consumer_live.py run "$CONSUMER_FIXTURE_DIR"
python3 tests/qualification/environments/consumer_live.py down "$CONSUMER_FIXTURE_DIR"
```

`run --suite rabbitmq|postgresql|mongodb|mongodb-factory` yalnız ilgili profili
çalıştırır. Varsayılan `all`, profilleri sırayla ve ayrı Cargo feature kümeleriyle
çalıştırır. İki database feature'ını tek profile birleştirmek, yalnız bir backend
yapılandıran fixture'a ilgisiz DI initialization zorunlulukları ekler.

Helper geçici credential üretir; `services.env` ve fixture'ların beklediği
değişkenleri içeren `test.env` dosyaları özel state dizininde tutulur. Credential'lar
repository'ye veya stdout'a yazılmaz. `images.json` image seçimlerini, her testin
ayrı log'u gerçek sonucu korur. `source "$CONSUMER_FIXTURE_DIR/test.env"` ile
ilgili ignored fixture elle de çalıştırılabilir.

Başarısız testte diğer suite'lere geçilmez. Log'ları inceleyip yine aynı state
dizinini kullanabilirsiniz. Test watchdog'u cargo/test child process grubunu
birlikte sonlandırır. `down` yalnız state dosyasındaki bu qualification projesini
siler, log'ları korur. Servisler test sonrasında otomatik silinmez; iş bitince
`down` çağrılmalıdır. Bu fixture production servisi veya kalıcı geliştirme
veritabanı olarak kullanılmamalıdır.
