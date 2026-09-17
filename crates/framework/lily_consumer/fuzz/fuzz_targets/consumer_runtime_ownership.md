# `consumer_runtime_ownership`

İlk byte direct cancellation, retained managed readiness veya dropped managed
readiness profilini seçer. Sonraki en fazla 32 byte gerçek private
`ConsumerRuntimeOwner` üzerinde trigger cancellation, provider completion,
manual/force shutdown, outer waiter abort, provider/drain failure, drain panic
ve graceful-drain timeout sıralarını sürer.

Adapter yalnız `fuzzing -> lily_queue/test-support` feature zincirinde gerçek
transport-free queue seam'ini kurar. Runtime trigger seçimi, biased
completion/cancellation arbitration, waiter Drop guard'ı, supervisor,
`FrameworkShutdownCoordinator` ve queue lifecycle action'ları production
kodudur; target bunların state machine'ini kopyalamaz.

Transport-free current-thread Tokio runtime'ı paused logical time ile başlar.
Her input production 10 ms cleanup/force zamanlamasını ve target'ın 250 ms dış
deadline'ını bu mantıksal saat üzerinde çalıştırır; değerler büyütülmez ve
lifecycle invariant'leri gevşetilmez. Tokio, bekleyen timer dışındaki işler
durduğunda mantıksal saati deterministik biçimde ilerletir. libFuzzer'ın
`-timeout=2` process watchdog'u ise bağımsız gerçek duvar saati sınırı olarak
kalır. Supervisor terminal olmalı;
`StopAdmission`, graceful `Drain`, `ForceDrain` ve `Close` çağrılarının her biri
en fazla bir kez çalışmalı; tamamlanan shutdown'da `Close` ve completion tam bir
kez görülmelidir. Drain reconcile edilmeli, terminal sonrası gate release yeni
call/completion üretmemeli ve active job sayısı sıfıra dönmelidir. Özellikle
tracked `managed-timeout-force` girdisi (`6d 54 63 66 79 0a`) cleanup başlamadan
force state'ini etkinleştirir ve exact
`StopAdmissionAsync -> ForceDrainAsync -> CloseAsync` sırasını bekler. Bu
invariant ikinci `ForceDrain` çağrısına izin verecek biçimde gevşetilemez.
Tracked `dropped-readiness-panic-drain` girdisi (`2f 78 3a 50 70 0a`), dropped
managed readiness profilinde graceful drain panic'ini provider failure ve
cancellation yarışlarıyla birlikte tekrar oynatır. Test-support seam'i bu
sentetik fault için sabit payload ile `resume_unwind` kullanır; böylece
libFuzzer'ın abort eden panic hook'u atlanır ve production `catch_unwind`
containment/forced reconciliation yolu gerçek haliyle fuzz edilir. Bunun
dışındaki beklenmeyen panic'ler libFuzzer tarafından crash olarak kalır;
exactly-once ve reconciliation invariant'leri değiştirilmez.

Tracked `waiter-abort-paused-drain` seed'i önceki gerçek-zamanlı fuzz
runtime'ının yaklaşık `#8192`'de 250 ms target deadline'ına ulaştığı exact
artifact'tır. Byte dizisi
`48 2b 63 09 00 00 00 00 00 00 00 2b 0a`, base64 gösterimi
`SCtjCQAAAAAAAAArCg==` ve SHA-256 değeri
`cfd7c7e876b61d2389bcb705ce9f8a04ee83049848b389fe645e6b25f2bf947f`'dir.
Direct profile altında olay dizisi `AbortWaiter -> CancelTrigger -> PauseDrain
-> 7 x CancelTrigger -> AbortWaiter -> CancelTrigger`; canonical printable
eşdeğeri `dacTcccccccac`'dir. Exact artifact 20 sabit replay'in 20'sinde de
geçti (toplam 193 ms); bu yüzden bulgu deterministik production deadlock'u
değil, host scheduling'e bağlı harness deadline dalgalanması olarak
sınıflandırıldı. Seed exact binary haliyle korunur.

`waiter_abort_and_cancellation_force_one_stalled_drain_to_reconciled_close`
qualification testi aynı production owner'ı paused Tokio zamanında gerçek
10 ms bütçeyle çalıştırır. Beklenen sıra `StartAsync -> WaitForShutdown ->
StopAdmissionAsync -> DrainAsync -> ForceDrainAsync -> CloseAsync`'dir.
Stop-admission, drain, force-drain ve close çağrıları tam bir kez; graceful
drain completion sıfır, force-drain ve close completion bir olmalı,
`drain_reconciled=true` kalmalı ve terminal gate release hiçbir late call veya
completion üretmemelidir.
