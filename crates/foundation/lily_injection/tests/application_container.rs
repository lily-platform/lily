use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ProcessContext, ServiceTrait, async_trait::async_trait,
};
use std::any::TypeId;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct AppOwnedDependency {
    starts: usize,
}

#[async_trait]
impl ServiceTrait for AppOwnedDependency {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.starts += 1;
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct AppOwnedService {
    #[inject]
    dependency: Arc<AppOwnedDependency>,
    starts: usize,
}

#[async_trait]
impl ServiceTrait for AppOwnedService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.starts += 1;
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct AppOwnedScopedService {
    starts: usize,
}

#[async_trait]
impl ServiceTrait for AppOwnedScopedService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.starts += 1;
        Ok(())
    }
}

#[derive(Default)]
struct UnregisteredService;

struct FrameworkHandle {
    value: usize,
}

#[async_trait]
impl ServiceTrait for UnregisteredService {}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct SeededLifecycleService {
    starts: Arc<AtomicUsize>,
    disposals: Arc<AtomicUsize>,
}

#[async_trait]
impl ServiceTrait for SeededLifecycleService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.disposals.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn application_containers_own_isolated_singletons_and_start_each_once() {
    let first = ApplicationContainer::build().await.unwrap();
    let first_dependency = first.resolve::<AppOwnedDependency>(None).await.unwrap();
    let first_service = first.resolve::<AppOwnedService>(None).await.unwrap();

    assert_eq!(first_dependency.starts, 1);
    assert_eq!(first_service.starts, 1);
    assert!(Arc::ptr_eq(&first_dependency, &first_service.dependency));

    let second = ApplicationContainer::build().await.unwrap();
    let second_dependency = second.resolve::<AppOwnedDependency>(None).await.unwrap();
    let second_service = second.resolve::<AppOwnedService>(None).await.unwrap();

    assert_eq!(second_dependency.starts, 1);
    assert_eq!(second_service.starts, 1);
    assert!(!Arc::ptr_eq(&first_dependency, &second_dependency));
    assert!(!Arc::ptr_eq(&first_service, &second_service));

    let dynamic_service = first
        .resolve_by_type_id(TypeId::of::<AppOwnedService>(), None)
        .await
        .unwrap()
        .downcast::<AppOwnedService>()
        .unwrap();
    assert!(Arc::ptr_eq(&first_service, &dynamic_service));

    let context = ProcessContext::with_process_id(42);
    let (first_scoped, first_scoped_again) = first
        .run_scoped(context.clone(), async {
            let service = first.resolve::<AppOwnedScopedService>(None).await?;
            let again = first.resolve::<AppOwnedScopedService>(None).await?;
            Ok::<_, InjectionError>((service, again))
        })
        .await
        .unwrap()
        .unwrap();
    let second_scoped = second
        .run_scoped(context, async {
            second.resolve::<AppOwnedScopedService>(None).await
        })
        .await
        .unwrap()
        .unwrap();

    assert_eq!(first_scoped.starts, 1);
    assert_eq!(second_scoped.starts, 1);
    assert!(Arc::ptr_eq(&first_scoped, &first_scoped_again));
    assert!(!Arc::ptr_eq(&first_scoped, &second_scoped));
    assert_eq!(first.active_scope_count(), 0);
    assert_eq!(second.active_scope_count(), 0);
    first.close().await.unwrap();
    second.close().await.unwrap();
}

#[tokio::test]
async fn seeded_singleton_uses_the_normal_lifecycle_and_stays_container_scoped() {
    let starts = Arc::new(AtomicUsize::new(0));
    let disposals = Arc::new(AtomicUsize::new(0));
    let first = ApplicationContainer::builder()
        .seed_singleton(AppOwnedDependency { starts: 40 })
        .seed_singleton(SeededLifecycleService {
            starts: Arc::clone(&starts),
            disposals: Arc::clone(&disposals),
        })
        .build()
        .await
        .unwrap();
    let second = ApplicationContainer::builder()
        .seed_singleton(AppOwnedDependency { starts: 90 })
        .build()
        .await
        .unwrap();

    let first_dependency = first.resolve::<AppOwnedDependency>(None).await.unwrap();
    let first_service = first.resolve::<AppOwnedService>(None).await.unwrap();
    let second_dependency = second.resolve::<AppOwnedDependency>(None).await.unwrap();

    assert_eq!(first_dependency.starts, 41);
    assert_eq!(second_dependency.starts, 91);
    assert!(Arc::ptr_eq(&first_dependency, &first_service.dependency));
    assert!(!Arc::ptr_eq(&first_dependency, &second_dependency));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(disposals.load(Ordering::SeqCst), 0);

    first.close().await.unwrap();
    assert_eq!(disposals.load(Ordering::SeqCst), 1);
    second.close().await.unwrap();
}

#[tokio::test]
async fn singleton_seed_validation_happens_before_service_startup() {
    let duplicate = ApplicationContainer::builder()
        .seed_singleton(AppOwnedDependency::default())
        .seed_singleton(AppOwnedDependency::default())
        .build()
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate,
        InjectionError::InvalidRegistrationPlan(message)
            if message.contains("seeded more than once")
    ));

    let scoped = ApplicationContainer::builder()
        .seed_singleton(AppOwnedScopedService::default())
        .build()
        .await
        .unwrap_err();
    assert!(matches!(
        scoped,
        InjectionError::InvalidRegistrationPlan(message)
            if message.contains("not a singleton")
    ));

    let unknown = ApplicationContainer::builder()
        .seed_singleton(UnregisteredService)
        .build()
        .await
        .unwrap_err();
    assert!(matches!(
        unknown,
        InjectionError::InvalidRegistrationPlan(message)
            if message.contains("no link-time service registration")
    ));
}

#[tokio::test]
async fn framework_singleton_attachment_is_identity_safe_and_rolls_back() {
    let container = ApplicationContainer::build().await.unwrap();
    let services = container.services();
    assert!(matches!(
        container.resolve::<FrameworkHandle>(None).await,
        Err(InjectionError::ServiceNotFound(_))
    ));

    let handle = Arc::new(FrameworkHandle { value: 42 });
    let attachment =
        lily_injection::__private::attach_framework_singleton(&services, Arc::clone(&handle))
            .unwrap();
    let resolved = container.resolve::<FrameworkHandle>(None).await.unwrap();
    assert_eq!(resolved.value, 42);
    assert!(Arc::ptr_eq(&handle, &resolved));
    assert!(matches!(
        lily_injection::__private::attach_framework_singleton(
            &services,
            Arc::new(FrameworkHandle { value: 7 })
        ),
        Err(InjectionError::InvalidRegistrationPlan(_))
    ));

    drop(attachment);
    assert!(matches!(
        container.resolve::<FrameworkHandle>(None).await,
        Err(InjectionError::ServiceNotFound(_))
    ));

    lily_injection::__private::attach_framework_singleton(&services, Arc::clone(&handle))
        .unwrap()
        .commit();
    assert!(Arc::ptr_eq(
        &handle,
        &container.resolve::<FrameworkHandle>(None).await.unwrap()
    ));
    assert!(matches!(
        lily_injection::__private::attach_framework_singleton(
            &services,
            Arc::new(AppOwnedDependency::default())
        ),
        Err(InjectionError::InvalidRegistrationPlan(_))
    ));

    container.close().await.unwrap();
}

#[test]
fn application_container_requires_an_active_tokio_runtime() {
    let error = futures::executor::block_on(ApplicationContainer::build()).unwrap_err();
    assert!(matches!(
        error,
        InjectionError::RuntimeUnavailable { operation }
            if operation == "ApplicationContainer::build"
    ));
}
