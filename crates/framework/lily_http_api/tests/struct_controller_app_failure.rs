use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerMaterializationError, ControllerTrait,
    Extensions, HttpApiError, ServiceTrait,
};
use lily_http_api::{AppBuildError, AppBuilder};
use lily_injection::Injectable;

static ROLLBACK_DISPOSES: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct RollbackProbe;

#[lily_http_api::async_trait::async_trait]
impl ServiceTrait for RollbackProbe {
    async fn dispose(&self) -> Result<(), lily_http_api::InjectionError> {
        ROLLBACK_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ConstructorScopedDependency;

impl ServiceTrait for ConstructorScopedDependency {}

#[derive(Controller)]
#[base_path("/failing-controller")]
struct FailingController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for FailingController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        extensions
            .get_service::<ConstructorScopedDependency>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        Ok(Self)
    }
}

#[controller]
impl FailingController {
    #[get("/")]
    async fn action(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

#[tokio::test]
async fn scoped_constructor_dependency_fails_closed_before_listener_start() {
    ROLLBACK_DISPOSES.store(0, Ordering::SeqCst);
    let occupied_listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener binds");
    let address = occupied_listener
        .local_addr()
        .expect("fixture listener has an address")
        .to_string();

    let error = match AppBuilder::new(&address).build().await {
        Ok(_) => panic!("a scoped constructor dependency must fail App build"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        AppBuildError::ControllerMaterialization(ControllerMaterializationError::Initialization {
            source: ControllerInitError::Dependency,
            ..
        })
    ));
    assert!(!error.to_string().contains("ConstructorScopedDependency"));
    assert_eq!(ROLLBACK_DISPOSES.load(Ordering::SeqCst), 1);

    // The original listener remains the only owner of the address; build did
    // not attempt to publish a competing server before reporting the error.
    assert_eq!(
        occupied_listener
            .local_addr()
            .expect("listener remains open"),
        address.parse().expect("fixture address parses")
    );
}
