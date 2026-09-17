# `queue_settlement_state`

Success/retryable/permanent/framework-cancelled outcome'ları; handoff,
ACK/NACK kabul/red/error sonuçları; receipt mismatch, pre-cancellation ve replay
aynı production `SettlementAuthority` + `materialize_delivery` state-machine'ine
girer. Provider panic kasıtlı üretilmez; panic/timeout dalları deterministic unit
contract suite'indedir. Bir iteration en fazla iki provider operasyonu ve tek
terminal observation üretmelidir; replay provider'a ikinci kez ulaşmamalıdır.
