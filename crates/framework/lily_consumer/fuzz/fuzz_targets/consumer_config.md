# `consumer_config`

İlk byte format seçicisidir: `j` JSON, diğer değerler TOML. Kalan bounded ham
belge gerçek `lily_config::LilyConfig` tipine parse edilir. Mevcut
`rabbitmq.topology.queues` koleksiyonu production `ConsumerPlan` validator'ına
verilir; concurrency, prefetch, retry/backoff, handler deadline, DLX/routing,
TTL, queue/exchange/routing adı ve duplicate queue kuralları canonical queue setting
doğrulamasıyla birlikte çalışır. Parse veya typed validation reddi normaldir;
panic, OOM ve hang bulgudur.
