use lily_background_service::{
    BackgroundServiceTrait, BackgroundServices, BackgroundShutdownDeadlines, ExecutionCancellation,
};
use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ApplicationScopeFactory, InjectionError, ProcessContext, ServiceTrait,
};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Started {
    signal: CancellationToken,
}
impl ServiceTrait for Started {}

struct Worker {
    started: Arc<Started>,
}
#[async_trait::async_trait]
impl BackgroundServiceTrait for Worker {
    type Error = InjectionError;
    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error> {
        let started = scopes
            .create_scope(ProcessContext::new())?
            .run(|extensions| {
                Box::pin(async move { extensions.get_service::<Started>(None).await })
            })
            .await?;
        Ok(Self { started })
    }
    async fn execute_async(&mut self, _: ExecutionCancellation) -> Result<(), Self::Error> {
        self.started.signal.cancel();
        futures::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn scheduler_delay_cannot_turn_a_late_join_into_timely_shutdown() {
    let container = ApplicationContainer::build().await.unwrap();
    let started = container.resolve::<Started>(None).await.unwrap();
    let mut services = BackgroundServices::default();
    services.add::<Worker>();
    let runtime = services.into_runtime(&container);
    runtime.initialize().await.unwrap();
    runtime.start().unwrap();
    started.signal.cancelled().await;
    let now = Instant::now();
    let deadlines = BackgroundShutdownDeadlines {
        cooperative: now + Duration::from_millis(600),
        execution_stop: now + Duration::from_millis(750),
        cleanup: now + Duration::from_millis(850),
        reconcile: now + Duration::from_millis(900),
    };
    runtime.begin_shutdown(deadlines);
    // Neither due timer nor ready join gets observed before this time jump.
    tokio::time::advance(Duration::from_millis(800)).await;
    assert!(runtime.wait_stopped_before(deadlines.reconcile).await);
    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.joined, 1);
    assert_eq!(snapshot.aborted, 1);
    assert!(snapshot.is_terminal());
    assert!(snapshot.execution_deadline_missed);
    assert!(!snapshot.cleanup_succeeded());
    assert!(!snapshot.succeeded());
    container.close().await.unwrap();
}
