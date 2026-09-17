# `consumer_shutdown_state`

Her byte bounded bir lifecycle event'ine çevrilir; en fazla 64 event, 32 spawn
isteği ve sekiz eşzamanlı permit vardır. Target gerçek `ShutdownState`,
`OwnedTaskSet` ve `FrameworkShutdownCoordinator` tipleriyle manual/OS/ikinci
sinyal, provider/registration failure, cancellation ve cooperative/uncooperative
task yollarını çalıştırır. Coordinator deadline'ına ek olarak dış 100 ms timeout
vardır; timeout'ta task abort edilir ve join edilir. Terminal rapor tek ve
replayable olmalı, metric/component sayıları reconcile etmeli, active job ve
semaphore permit'leri başlangıç değerine dönmelidir.
