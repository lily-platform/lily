# `consumer_plan`

`j` seçicisi okunabilir JSON seed biçimini, diğer değerler `arbitrary`
structured biçimini kullanır. Queue tanımları, handler metadata descriptor'ları
ve trace cell'ler runtime'ın kullandığı aynı `ConsumerPlan::build` yoluna girer.
Başarılı planda her configured queue tam bir binding üretmeli; binding queue
sırası deterministik olmalı ve handler index'i mevcut descriptor aralığında
kalmalıdır. Missing/duplicate handler, schema, bounded queue policy ve trace
cell/kind uyuşmazlıkları typed statik diagnostic ile reddedilir.
