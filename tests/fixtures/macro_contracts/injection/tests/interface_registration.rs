use std::any::TypeId;
use std::sync::Arc;

use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, InjectionError, ServiceTrait};
use lily_injection_registry::{get_all_service_metadata, ServiceRouteKind};

trait GreetingService: Send + Sync {
    fn greeting(&self) -> &'static str;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton", interface = dyn GreetingService)]
struct ActiveGreetingService;

impl GreetingService for ActiveGreetingService {
    fn greeting(&self) -> &'static str {
        "active"
    }
}

impl ServiceTrait for ActiveGreetingService {}

#[derive(Default, Injectable)]
#[service(
    lifetime = "Singleton",
    interface = dyn GreetingService,
    disabled
)]
struct DisabledGreetingService;

impl GreetingService for DisabledGreetingService {
    fn greeting(&self) -> &'static str {
        "disabled"
    }
}

impl ServiceTrait for DisabledGreetingService {}

#[test]
fn disabled_candidate_emits_no_distributed_registration() {
    let metadata = get_all_service_metadata();
    assert!(metadata
        .iter()
        .any(|entry| entry.type_id == TypeId::of::<ActiveGreetingService>()));
    assert!(metadata
        .iter()
        .all(|entry| entry.type_id != TypeId::of::<DisabledGreetingService>()));

    let routes = lily_injection_registry::get_all_service_route_metadata();
    assert_eq!(
        routes
            .iter()
            .filter(|route| {
                route.implementation_type_id == TypeId::of::<ActiveGreetingService>()
            })
            .count(),
        2
    );
    assert!(routes
        .iter()
        .all(|route| { route.implementation_type_id != TypeId::of::<DisabledGreetingService>() }));

    assert!(
        lily_injection_registry::get_service_disposer(TypeId::of::<ActiveGreetingService>())
            .is_some()
    );
    assert!(
        lily_injection_registry::get_service_disposer(TypeId::of::<DisabledGreetingService>())
            .is_none()
    );
    lily_injection_registry::validate_service_route_metadata(&routes).unwrap();
}

#[test]
fn generated_interface_projector_preserves_arc_identity() {
    let route = lily_injection_registry::get_all_service_route_metadata()
        .into_iter()
        .find(|route| {
            route.requested_type_id == TypeId::of::<dyn GreetingService>()
                && route.kind == ServiceRouteKind::Interface
        })
        .expect("active interface route must be registered");

    let concrete = Arc::new(ActiveGreetingService);
    let erased: Arc<dyn std::any::Any + Send + Sync> = concrete.clone();
    let projected = (route.project_fn)(erased).unwrap();
    let interface = *projected
        .downcast::<Arc<dyn GreetingService>>()
        .expect("projector must box Arc<dyn GreetingService>");

    assert_eq!(interface.greeting(), "active");
    assert_eq!(
        Arc::as_ptr(&concrete) as *const (),
        Arc::as_ptr(&interface) as *const ()
    );
}

#[tokio::test]
async fn disabled_candidate_is_not_runtime_resolvable_and_active_interface_is_deterministic() {
    let container = ApplicationContainer::build().await.unwrap();
    let active = container
        .resolve::<ActiveGreetingService>(None)
        .await
        .unwrap();
    let interface = container
        .resolve::<dyn GreetingService>(None)
        .await
        .unwrap();

    assert_eq!(interface.greeting(), "active");
    assert_eq!(
        Arc::as_ptr(&active) as *const (),
        Arc::as_ptr(&interface) as *const ()
    );
    assert!(matches!(
        container.resolve::<DisabledGreetingService>(None).await,
        Err(InjectionError::ServiceNotFound(_))
    ));
    container.close().await.unwrap();
}
