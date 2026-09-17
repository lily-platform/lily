use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ApplicationScopeFactory, InjectionError, ProcessContext, ServiceTrait,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Probe {
    disposed: Mutex<Vec<u64>>,
    entered: CancellationToken,
    release: CancellationToken,
    block: AtomicBool,
    fail: AtomicBool,
}
impl ServiceTrait for Probe {}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Scoped {
    #[inject]
    probe: Arc<Probe>,
    id: u64,
}
#[async_trait::async_trait]
impl ServiceTrait for Scoped {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.id = ProcessContext::current().unwrap().process_id;
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(ProcessContext::current().unwrap().process_id, self.id);
        self.probe.entered.cancel();
        if self.probe.block.load(Ordering::Acquire) {
            self.probe.release.cancelled().await;
        }
        self.probe.disposed.lock().unwrap().push(self.id);
        if self.probe.fail.load(Ordering::Acquire) {
            return Err(InjectionError::DisposeError("test disposal".into()));
        }
        Ok(())
    }
}

// Deliberately not Clone: application errors must not inherit receipt bounds.
#[derive(Debug)]
enum BusinessError {
    Rejected,
    Infrastructure(InjectionError),
}
impl From<InjectionError> for BusinessError {
    fn from(error: InjectionError) -> Self {
        Self::Infrastructure(error)
    }
}

#[tokio::test]
async fn provider_and_callback_construction_share_exact_context_and_dispose_before_return() {
    let container = ApplicationContainer::build().await.unwrap();
    let probe = container.resolve::<Probe>(None).await.unwrap();
    let factory = Arc::new(ApplicationScopeFactory::new(&container));
    for id in [100, 101, 100] {
        let result = factory
            .create_scope(ProcessContext::with_process_id(id))
            .unwrap()
            .run(move |extensions| {
                assert_eq!(ProcessContext::current().unwrap().process_id, id);
                Box::pin(async move {
                    let first = extensions.get_service::<Scoped>(None).await?;
                    let second = extensions.get_service::<Scoped>(None).await?;
                    assert!(Arc::ptr_eq(&first, &second));
                    assert_eq!(first.id, id);
                    Ok::<_, BusinessError>(first.id)
                })
            })
            .await
            .unwrap();
        assert_eq!(result, id);
        assert_eq!(*probe.disposed.lock().unwrap().last().unwrap(), id);
        assert!(ProcessContext::current().is_none());
        assert_eq!(factory.snapshot().outstanding, 0);
    }
    assert_eq!(probe.disposed.lock().unwrap().as_slice(), [100, 101, 100]);
    assert_eq!(factory.snapshot().completed, 3);
    factory.seal();
    assert!(factory.snapshot().is_terminal());
    assert!(factory.create_scope(ProcessContext::new()).is_err());
    container.close().await.unwrap();
}

#[tokio::test]
async fn application_error_is_preserved_unless_disposal_itself_fails() {
    for fails in [false, true] {
        let container = ApplicationContainer::build().await.unwrap();
        let probe = container.resolve::<Probe>(None).await.unwrap();
        probe.fail.store(fails, Ordering::Release);
        let factory = Arc::new(ApplicationScopeFactory::new(&container));
        let result = factory
            .create_scope(ProcessContext::new())
            .unwrap()
            .run(|extensions| {
                Box::pin(async move {
                    extensions.get_service::<Scoped>(None).await?;
                    Err::<(), _>(BusinessError::Rejected)
                })
            })
            .await;
        if fails {
            assert!(matches!(
                result,
                Err(BusinessError::Infrastructure(InjectionError::DisposeError(
                    _
                )))
            ));
        } else {
            assert!(matches!(result, Err(BusinessError::Rejected)));
        }
        let report = factory.drain_before(tokio::time::Instant::now()).await;
        assert!(report.is_terminal());
        assert_eq!(report.failed, usize::from(fails));
        assert_eq!(container.close().await.is_err(), fails);
    }
}

#[tokio::test]
async fn cancelled_run_keeps_cleanup_and_another_factorys_scope_is_not_claimed() {
    let container = ApplicationContainer::build().await.unwrap();
    let probe = container.resolve::<Probe>(None).await.unwrap();
    probe.block.store(true, Ordering::Release);
    let factory = Arc::new(ApplicationScopeFactory::new(&container));
    let other_factory = Arc::new(ApplicationScopeFactory::new(&container));
    let other = other_factory.create_scope(ProcessContext::new()).unwrap();
    let entered = CancellationToken::new();
    let started = entered.clone();
    let scope = factory.create_scope(ProcessContext::new()).unwrap();
    let task = tokio::spawn(scope.run(move |extensions| {
        Box::pin(async move {
            extensions.get_service::<Scoped>(None).await?;
            started.cancel();
            futures::future::pending::<Result<(), InjectionError>>().await
        })
    }));
    entered.cancelled().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    probe.entered.cancelled().await;
    assert_eq!(factory.snapshot().outstanding, 1);
    assert!(probe.disposed.lock().unwrap().is_empty());
    probe.release.cancel();
    let report = factory
        .drain_before(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
        .await;
    assert!(report.is_terminal());
    assert_eq!(report.completed, 1);
    assert_eq!(other_factory.snapshot().outstanding, 1);
    assert_eq!(container.active_scope_count(), 1);
    other
        .run(|_| Box::pin(async { Ok::<_, InjectionError>(()) }))
        .await
        .unwrap();
    other_factory.seal();
    assert!(other_factory.snapshot().is_terminal());
    container.close().await.unwrap();
}

#[tokio::test]
async fn panic_is_resumed_only_after_cleanup_and_unused_scopes_are_reconciled() {
    let container = ApplicationContainer::build().await.unwrap();
    let probe = container.resolve::<Probe>(None).await.unwrap();
    let factory = Arc::new(ApplicationScopeFactory::new(&container));
    let scope = factory.create_scope(ProcessContext::new()).unwrap();
    let task = tokio::spawn(scope.run(|extensions| {
        Box::pin(async move {
            extensions.get_service::<Scoped>(None).await?;
            panic!("test application panic");
            #[allow(unreachable_code)]
            Ok::<(), InjectionError>(())
        })
    }));
    assert!(task.await.unwrap_err().is_panic());
    assert_eq!(probe.disposed.lock().unwrap().len(), 1);
    drop(factory.create_scope(ProcessContext::new()).unwrap());
    let snapshot = factory
        .drain_before(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
        .await;
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.completed, 2);
    container.close().await.unwrap();
}
