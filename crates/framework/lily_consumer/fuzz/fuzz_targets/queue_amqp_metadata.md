# `queue_amqp_metadata`

Received AMQP metadata, worst-case framework handoff projection'ı ve canonical
retry count aynı production authority üzerinden çalıştırılır. Tekrarlı çağrı
aynı sonucu vermeli; handoff headroom kabulü received metadata ve projected
metadata kabulünü zorunlu kılmalıdır. Flat fuzz node'ları recursive decode
oluşturmaz; çeviri README'deki ayrı depth/node/child sınırlarına tabidir.
