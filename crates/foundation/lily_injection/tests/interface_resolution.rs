use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ProcessContext, ServiceLifetime, ServiceTrait, async_trait::async_trait,
};
use std::any::TypeId;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

static SINGLETON_STARTS: AtomicUsize = AtomicUsize::new(0);
static SINGLETON_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static SCOPED_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_STARTS: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_DISPOSES: AtomicUsize = AtomicUsize::new(0);

trait Greeting: Send + Sync {
    fn message(&self) -> &'static str;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton", interface = dyn Greeting)]
struct GreetingService;

impl Greeting for GreetingService {
    fn message(&self) -> &'static str {
        "hello"
    }
}

#[async_trait]
impl ServiceTrait for GreetingService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        SINGLETON_STARTS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        SINGLETON_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct GreetingConsumer {
    #[inject]
    greeting: Arc<dyn Greeting>,
}

impl ServiceTrait for GreetingConsumer {}

trait RequestValue: Send + Sync {
    fn value(&self) -> usize;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped", interface = dyn RequestValue)]
struct ScopedValue;

impl RequestValue for ScopedValue {
    fn value(&self) -> usize {
        42
    }
}

#[async_trait]
impl ServiceTrait for ScopedValue {
    async fn dispose(&self) -> Result<(), InjectionError> {
        SCOPED_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

trait TransientValue: Send + Sync {
    fn sequence(&self) -> usize;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient", interface = dyn TransientValue)]
struct TransientValueService {
    sequence: usize,
}

impl TransientValue for TransientValueService {
    fn sequence(&self) -> usize {
        self.sequence
    }
}

#[async_trait]
impl ServiceTrait for TransientValueService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.sequence = TRANSIENT_STARTS.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        TRANSIENT_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn concrete_and_interface_share_one_lifecycle_owner() {
    SINGLETON_STARTS.store(0, Ordering::SeqCst);
    SINGLETON_DISPOSES.store(0, Ordering::SeqCst);
    SCOPED_DISPOSES.store(0, Ordering::SeqCst);
    TRANSIENT_STARTS.store(0, Ordering::SeqCst);
    TRANSIENT_DISPOSES.store(0, Ordering::SeqCst);

    let container = ApplicationContainer::build().await.unwrap();
    let services = container.services();
    assert_eq!(
        lily_injection::__private::service_registration_lifetime(
            services.as_ref(),
            TypeId::of::<GreetingService>(),
        ),
        Some(ServiceLifetime::Singleton)
    );
    assert_eq!(
        lily_injection::__private::service_registration_lifetime(
            services.as_ref(),
            TypeId::of::<dyn Greeting>(),
        ),
        Some(ServiceLifetime::Singleton)
    );
    assert_eq!(
        lily_injection::__private::service_registration_lifetime(
            services.as_ref(),
            TypeId::of::<dyn RequestValue>(),
        ),
        Some(ServiceLifetime::Scoped)
    );
    assert_eq!(
        lily_injection::__private::service_registration_lifetime(
            services.as_ref(),
            TypeId::of::<dyn TransientValue>(),
        ),
        Some(ServiceLifetime::Transient)
    );
    assert_eq!(
        lily_injection::__private::service_registration_lifetime(
            services.as_ref(),
            TypeId::of::<u128>(),
        ),
        None
    );
    let concrete = container.resolve::<GreetingService>(None).await.unwrap();
    let interface = container.resolve::<dyn Greeting>(None).await.unwrap();
    let consumer = container.resolve::<GreetingConsumer>(None).await.unwrap();

    assert_eq!(interface.message(), "hello");
    assert_eq!(consumer.greeting.message(), "hello");
    assert_eq!(
        Arc::as_ptr(&concrete) as *const (),
        Arc::as_ptr(&interface) as *const ()
    );
    assert_eq!(
        Arc::as_ptr(&concrete) as *const (),
        Arc::as_ptr(&consumer.greeting) as *const ()
    );
    assert_eq!(SINGLETON_STARTS.load(Ordering::SeqCst), 1);

    container
        .run_scoped(ProcessContext::with_process_id(7001), async {
            let concrete = container.resolve::<ScopedValue>(None).await.unwrap();
            let interface = container.resolve::<dyn RequestValue>(None).await.unwrap();
            assert_eq!(interface.value(), 42);
            assert_eq!(
                Arc::as_ptr(&concrete) as *const (),
                Arc::as_ptr(&interface) as *const ()
            );

            let first = container.resolve::<dyn TransientValue>(None).await.unwrap();
            let second = container.resolve::<dyn TransientValue>(None).await.unwrap();
            assert_ne!(first.sequence(), second.sequence());
        })
        .await
        .unwrap();

    assert_eq!(SCOPED_DISPOSES.load(Ordering::SeqCst), 1);
    assert_eq!(TRANSIENT_STARTS.load(Ordering::SeqCst), 2);
    assert_eq!(TRANSIENT_DISPOSES.load(Ordering::SeqCst), 2);

    assert!(matches!(
        container
            .resolve_by_type_id(TypeId::of::<dyn RequestValue>(), None)
            .await,
        Err(InjectionError::ScopeRequired { service })
            if service.contains("RequestValue")
    ));
    container.close().await.unwrap();
    assert_eq!(SINGLETON_DISPOSES.load(Ordering::SeqCst), 1);
    assert!(matches!(
        container.resolve::<GreetingService>(None).await,
        Err(InjectionError::ContainerClosed)
    ));
}
