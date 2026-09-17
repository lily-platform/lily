use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, Extensions, HttpApiError,
    ServiceTrait,
};
use lily_injection::Injectable;

static CONTROLLER_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static CONTROLLER_DEPENDENCIES: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct ControllerSingletonDependency;

impl ServiceTrait for ControllerSingletonDependency {}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct ControllerTransientDependency {
    sequence: usize,
}

#[lily_http_api::async_trait::async_trait]
impl ServiceTrait for ControllerTransientDependency {
    async fn initialize(&mut self) -> Result<(), lily_http_api::InjectionError> {
        self.sequence = TRANSIENT_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }
}

#[derive(Controller)]
#[base_path("/materialized")]
struct MaterializedController {
    _singleton: Arc<ControllerSingletonDependency>,
    _transient: Arc<ControllerTransientDependency>,
}

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for MaterializedController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        let singleton = extensions
            .get_service::<ControllerSingletonDependency>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        let transient = extensions
            .get_service::<ControllerTransientDependency>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        CONTROLLER_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
        CONTROLLER_DEPENDENCIES
            .lock()
            .expect("controller dependency log is available")
            .push((
                Arc::as_ptr(&singleton) as usize,
                Arc::as_ptr(&transient) as usize,
            ));
        Ok(Self {
            _singleton: singleton,
            _transient: transient,
        })
    }
}

#[controller]
impl MaterializedController {
    #[get("/one")]
    async fn one(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/two")]
    async fn two(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

#[tokio::test]
async fn app_build_materializes_one_controller_per_app_without_opening_the_listener() {
    CONTROLLER_INITIALIZATIONS.store(0, Ordering::SeqCst);
    TRANSIENT_INITIALIZATIONS.store(0, Ordering::SeqCst);
    CONTROLLER_DEPENDENCIES
        .lock()
        .expect("controller dependency log is available")
        .clear();

    // Keeping the address occupied proves AppBuilder::build only materializes
    // the application. Listener ownership still begins in App::start.
    let occupied_listener = TcpListener::bind("127.0.0.1:0").expect("fixture listener binds");
    let address = occupied_listener
        .local_addr()
        .expect("fixture listener has an address")
        .to_string();

    let first_app = AppBuilder::new(&address)
        .build()
        .await
        .expect("first controller application builds before listener startup");
    let second_app = AppBuilder::new(&address)
        .build()
        .await
        .expect("second controller application builds before listener startup");
    assert_eq!(first_app.route_stats().total_routes, 2);
    assert_eq!(second_app.route_stats().total_routes, 2);

    let first_singleton = first_app
        .container()
        .resolve::<ControllerSingletonDependency>(None)
        .await
        .expect("first singleton remains resolvable");
    let second_singleton = second_app
        .container()
        .resolve::<ControllerSingletonDependency>(None)
        .await
        .expect("second singleton remains resolvable");

    assert_eq!(CONTROLLER_INITIALIZATIONS.load(Ordering::SeqCst), 2);
    // Two actions do not create two controllers or resolve two transients.
    // A transient captured in ControllerTrait::new is reused for that App's
    // complete controller lifetime.
    assert_eq!(TRANSIENT_INITIALIZATIONS.load(Ordering::SeqCst), 2);
    let dependencies = CONTROLLER_DEPENDENCIES
        .lock()
        .expect("controller dependency log is available")
        .clone();
    assert_eq!(dependencies.len(), 2);
    assert_eq!(dependencies[0].0, Arc::as_ptr(&first_singleton) as usize);
    assert_eq!(dependencies[1].0, Arc::as_ptr(&second_singleton) as usize);
    assert_ne!(dependencies[0].0, dependencies[1].0);
    assert_ne!(dependencies[0].1, dependencies[1].1);

    drop(first_singleton);
    drop(second_singleton);
    let first_container = Arc::clone(first_app.container());
    let second_container = Arc::clone(second_app.container());
    drop(first_app);
    drop(second_app);
    first_container
        .close()
        .await
        .expect("first controller application container closes");
    second_container
        .close()
        .await
        .expect("second controller application container closes");
    drop(occupied_listener);
}
