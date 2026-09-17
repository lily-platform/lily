# `queue_extractor_plan`

Beş gerçek tuple/payload yolu seçilir: parts-only, JSON, UTF-8 text, binary ve
raw delivery. Registry `QueueHandlerInputContract` üretimi ve payload decode
aynı production trait implementasyonlarını kullanır. Başarılı terminal payload
body authority'yi tam bir kez tüketmeli; parts-only plan body tüketmemelidir.
