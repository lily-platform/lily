use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use once_cell::sync::Lazy;

use super::{materialize_controller_routes, PendingControllerRoute};
use crate::controller::{
    ControllerBindingError, ControllerInitError, ControllerMaterializationError, ControllerTrait,
};
use crate::handler::Handler;
use crate::private::{
    downcast_controller, ControllerActionRegistration, ControllerRegistration,
    CorsRoutePolicyRegistration, ErasedController, OpenApiOperationMetadata,
};
use lily_injection::{ApplicationContainer, Extensions};

static ISOLATED_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static UNUSED_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static MATERIALIZATION_FACTORY_CALLS: AtomicUsize = AtomicUsize::new(0);
static ACCEPTED_FACTORY_CALLS: AtomicUsize = AtomicUsize::new(0);
static DUPLICATE_FACTORY_CALLS: AtomicUsize = AtomicUsize::new(0);
static ISOLATED_BOUND_POINTERS: Lazy<Mutex<Vec<usize>>> = Lazy::new(|| Mutex::new(Vec::new()));
static ISOLATED_WEAK_INSTANCES: Lazy<Mutex<Vec<Weak<IsolatedController>>>> =
    Lazy::new(|| Mutex::new(Vec::new()));

struct IsolatedController;
struct UnusedController;
struct OpenApiFactoryController;

#[async_trait]
impl ControllerTrait for IsolatedController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        ISOLATED_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
        Ok(Self)
    }
}

#[async_trait]
impl ControllerTrait for UnusedController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        UNUSED_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
        Ok(Self)
    }
}

#[async_trait]
impl ControllerTrait for OpenApiFactoryController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

fn retaining_handler<C>(controller: Arc<C>) -> Handler
where
    C: Send + Sync + 'static,
{
    Handler::new(
        Arc::new(move |_extensions, _request, _response| {
            // The generated action adapter will retain this reference and
            // borrow it on each call. This test handler models that ownership
            // without resolving or downcasting again on the request path.
            let _strong_count = Arc::strong_count(&controller);
            Box::pin(async { Ok(()) })
        }),
        false,
    )
}

fn record_isolated_binding(
    controller: ErasedController,
) -> Result<Handler, ControllerBindingError> {
    let controller = downcast_controller::<IsolatedController>(controller)?;
    ISOLATED_BOUND_POINTERS
        .lock()
        .expect("isolated pointer fixture is available")
        .push(Arc::as_ptr(&controller) as usize);
    ISOLATED_WEAK_INSTANCES
        .lock()
        .expect("isolated weak fixture is available")
        .push(Arc::downgrade(&controller));
    Ok(retaining_handler(controller))
}

fn isolated_metadata() -> (Vec<ControllerRegistration>, Vec<PendingControllerRoute>) {
    let action = || ControllerActionRegistration::of::<IsolatedController>(record_isolated_binding);
    (
        vec![
            ControllerRegistration::of::<UnusedController>(),
            ControllerRegistration::of::<IsolatedController>(),
        ],
        vec![
            PendingControllerRoute::new(
                "GET",
                "/isolated/one",
                "fixture::IsolatedController::one",
                Vec::new(),
                Vec::new(),
                CorsRoutePolicyRegistration::inherit(),
                action(),
            ),
            PendingControllerRoute::new(
                "POST",
                "/isolated/two",
                "fixture::IsolatedController::two",
                Vec::new(),
                Vec::new(),
                CorsRoutePolicyRegistration::inherit(),
                action(),
            ),
        ],
    )
}

fn materialization_openapi_factory() -> OpenApiOperationMetadata {
    MATERIALIZATION_FACTORY_CALLS.fetch_add(1, Ordering::SeqCst);
    empty_openapi_metadata()
}

fn accepted_openapi_factory() -> OpenApiOperationMetadata {
    ACCEPTED_FACTORY_CALLS.fetch_add(1, Ordering::SeqCst);
    empty_openapi_metadata()
}

fn duplicate_openapi_factory() -> OpenApiOperationMetadata {
    DUPLICATE_FACTORY_CALLS.fetch_add(1, Ordering::SeqCst);
    empty_openapi_metadata()
}

fn empty_openapi_metadata() -> OpenApiOperationMetadata {
    let responses = utoipa::openapi::response::ResponsesBuilder::new()
        .response(
            "204",
            utoipa::openapi::response::Response::new("No Content"),
        )
        .build();
    OpenApiOperationMetadata::new(
        utoipa::openapi::path::OperationBuilder::new()
            .responses(responses)
            .build(),
        utoipa::openapi::schema::Components::new(),
    )
}

#[tokio::test]
async fn controller_materialization_does_not_execute_openapi_factories() {
    MATERIALIZATION_FACTORY_CALLS.store(0, Ordering::SeqCst);
    let app = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let pending = PendingControllerRoute::new(
        "GET",
        "/openapi-factory",
        "fixture::OpenApiFactoryController::action",
        Vec::new(),
        Vec::new(),
        CorsRoutePolicyRegistration::inherit(),
        ControllerActionRegistration::of::<OpenApiFactoryController>(
            bind_noop::<OpenApiFactoryController>,
        ),
    )
    .with_openapi_operation_factory(materialization_openapi_factory);

    let materialized = materialize_controller_routes(
        vec![ControllerRegistration::of::<OpenApiFactoryController>()],
        vec![pending],
        app.services(),
    )
    .await
    .expect("controller routes materialize without OpenAPI generation");
    materialized
        .into_route_table()
        .expect("normal route table accepts the route");

    assert_eq!(MATERIALIZATION_FACTORY_CALLS.load(Ordering::SeqCst), 0);
    app.close()
        .await
        .expect("application container closes cleanly");
}

#[tokio::test]
async fn openapi_factory_runs_only_after_the_route_table_accepts_the_route() {
    ACCEPTED_FACTORY_CALLS.store(0, Ordering::SeqCst);
    let app = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let pending = PendingControllerRoute::new(
        "GET",
        "/openapi-accepted",
        "fixture::OpenApiFactoryController::accepted",
        Vec::new(),
        Vec::new(),
        CorsRoutePolicyRegistration::inherit(),
        ControllerActionRegistration::of::<OpenApiFactoryController>(
            bind_noop::<OpenApiFactoryController>,
        ),
    )
    .with_openapi_operation_factory(accepted_openapi_factory);

    let materialized = materialize_controller_routes(
        vec![ControllerRegistration::of::<OpenApiFactoryController>()],
        vec![pending],
        app.services(),
    )
    .await
    .expect("controller route materializes");
    assert_eq!(ACCEPTED_FACTORY_CALLS.load(Ordering::SeqCst), 0);

    let (_, registry) = materialized
        .into_openapi_route_table()
        .expect("accepted route produces OpenAPI registry");
    assert_eq!(ACCEPTED_FACTORY_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(registry.routes().count(), 1);

    app.close()
        .await
        .expect("application container closes cleanly");
}

#[tokio::test]
async fn duplicate_route_fails_before_any_openapi_factory_runs() {
    DUPLICATE_FACTORY_CALLS.store(0, Ordering::SeqCst);
    let app = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let pending = || {
        PendingControllerRoute::new(
            "GET",
            "/openapi-duplicate",
            "fixture::OpenApiFactoryController::duplicate",
            Vec::new(),
            Vec::new(),
            CorsRoutePolicyRegistration::inherit(),
            ControllerActionRegistration::of::<OpenApiFactoryController>(
                bind_noop::<OpenApiFactoryController>,
            ),
        )
        .with_openapi_operation_factory(duplicate_openapi_factory)
    };

    let materialized = materialize_controller_routes(
        vec![ControllerRegistration::of::<OpenApiFactoryController>()],
        vec![pending(), pending()],
        app.services(),
    )
    .await
    .expect("controller routes materialize");
    assert!(matches!(
        materialized.into_openapi_route_table(),
        Err(super::OpenApiRouteBuildError::Route(_))
    ));
    assert_eq!(DUPLICATE_FACTORY_CALLS.load(Ordering::SeqCst), 0);

    app.close()
        .await
        .expect("application container closes cleanly");
}

#[tokio::test]
async fn controller_is_initialized_once_per_app_and_shared_by_its_actions() {
    ISOLATED_INITIALIZATIONS.store(0, Ordering::SeqCst);
    UNUSED_INITIALIZATIONS.store(0, Ordering::SeqCst);
    ISOLATED_BOUND_POINTERS
        .lock()
        .expect("isolated pointer fixture is available")
        .clear();
    ISOLATED_WEAK_INSTANCES
        .lock()
        .expect("isolated weak fixture is available")
        .clear();

    let first_app = ApplicationContainer::build()
        .await
        .expect("first application container builds");
    let (first_registrations, first_pending) = isolated_metadata();
    let first_routes =
        materialize_controller_routes(first_registrations, first_pending, first_app.services())
            .await
            .expect("first application routes materialize");

    let second_app = ApplicationContainer::build()
        .await
        .expect("second application container builds");
    let (second_registrations, second_pending) = isolated_metadata();
    let second_routes =
        materialize_controller_routes(second_registrations, second_pending, second_app.services())
            .await
            .expect("second application routes materialize");

    assert_eq!(ISOLATED_INITIALIZATIONS.load(Ordering::SeqCst), 2);
    assert_eq!(UNUSED_INITIALIZATIONS.load(Ordering::SeqCst), 0);
    assert_eq!(first_routes.len(), 2);
    assert_eq!(second_routes.len(), 2);

    let pointers = ISOLATED_BOUND_POINTERS
        .lock()
        .expect("isolated pointer fixture is available")
        .clone();
    assert_eq!(pointers.len(), 4);
    assert_eq!(pointers[0], pointers[1]);
    assert_eq!(pointers[2], pointers[3]);
    assert_ne!(pointers[0], pointers[2]);

    let weak_instances = ISOLATED_WEAK_INSTANCES
        .lock()
        .expect("isolated weak fixture is available")
        .clone();
    assert!(weak_instances
        .iter()
        .all(|controller| controller.upgrade().is_some()));

    // Registration values contain function pointers only. Once the App-owned
    // handlers disappear, no global/static metadata keeps a controller alive.
    drop(first_routes);
    assert!(weak_instances[..2]
        .iter()
        .all(|controller| controller.upgrade().is_none()));
    assert!(weak_instances[2..]
        .iter()
        .all(|controller| controller.upgrade().is_some()));
    drop(second_routes);
    assert!(weak_instances
        .iter()
        .all(|controller| controller.upgrade().is_none()));

    first_app
        .close()
        .await
        .expect("first application container closes");
    second_app
        .close()
        .await
        .expect("second application container closes");
}

static INITIALIZATION_ORDER: Lazy<Mutex<Vec<&'static str>>> = Lazy::new(|| Mutex::new(Vec::new()));

struct AlphaController;
struct ZuluController;

#[async_trait]
impl ControllerTrait for AlphaController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        INITIALIZATION_ORDER
            .lock()
            .expect("order fixture is available")
            .push(std::any::type_name::<Self>());
        Ok(Self)
    }
}

#[async_trait]
impl ControllerTrait for ZuluController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        INITIALIZATION_ORDER
            .lock()
            .expect("order fixture is available")
            .push(std::any::type_name::<Self>());
        Ok(Self)
    }
}

#[tokio::test]
async fn controller_initialization_order_is_deterministic_by_type_name() {
    INITIALIZATION_ORDER
        .lock()
        .expect("order fixture is available")
        .clear();
    let app = ApplicationContainer::build()
        .await
        .expect("application container builds");

    materialize_controller_routes(
        vec![
            ControllerRegistration::of::<ZuluController>(),
            ControllerRegistration::of::<AlphaController>(),
        ],
        vec![
            pending_route::<ZuluController>(
                "fixture::ZuluController::action",
                bind_noop::<ZuluController>,
            ),
            pending_route::<AlphaController>(
                "fixture::AlphaController::action",
                bind_noop::<AlphaController>,
            ),
        ],
        app.services(),
    )
    .await
    .expect("controller registrations materialize");

    assert_eq!(
        INITIALIZATION_ORDER
            .lock()
            .expect("order fixture is available")
            .as_slice(),
        [
            std::any::type_name::<AlphaController>(),
            std::any::type_name::<ZuluController>(),
        ]
    );
    app.close()
        .await
        .expect("application container closes cleanly");
}

struct DuplicateController;
struct MissingController;
struct DeclaredController;
struct ExpectedController;
struct FailingController;
struct PanickingController;

macro_rules! infallible_controller {
    ($controller:ty) => {
        #[async_trait]
        impl ControllerTrait for $controller {
            async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
                Ok(Self)
            }
        }
    };
}

infallible_controller!(DuplicateController);
infallible_controller!(MissingController);
infallible_controller!(DeclaredController);
infallible_controller!(ExpectedController);

#[async_trait]
impl ControllerTrait for FailingController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Err(ControllerInitError::dependency(
            "postgres://administrator:do-not-publish@database.internal/production",
        ))
    }
}

#[async_trait]
impl ControllerTrait for PanickingController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        panic!("controller initialization fixture panic")
    }
}

fn bind_noop<C>(controller: ErasedController) -> Result<Handler, ControllerBindingError>
where
    C: ControllerTrait,
{
    Ok(retaining_handler(downcast_controller::<C>(controller)?))
}

fn bind_missing(controller: ErasedController) -> Result<Handler, ControllerBindingError> {
    bind_noop::<MissingController>(controller)
}

fn bind_declared_as_expected(
    controller: ErasedController,
) -> Result<Handler, ControllerBindingError> {
    bind_noop::<ExpectedController>(controller)
}

fn pending_route<C>(
    action_name: &'static str,
    binder: fn(ErasedController) -> Result<Handler, ControllerBindingError>,
) -> PendingControllerRoute
where
    C: ControllerTrait,
{
    PendingControllerRoute::new(
        "GET",
        "/metadata",
        action_name,
        Vec::new(),
        Vec::new(),
        CorsRoutePolicyRegistration::inherit(),
        ControllerActionRegistration::of::<C>(binder),
    )
}

#[tokio::test]
async fn invalid_controller_metadata_and_initialization_fail_closed() {
    let app = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let extensions = app.services();

    let duplicate = materialize_controller_routes(
        vec![
            ControllerRegistration::of::<DuplicateController>(),
            ControllerRegistration::of::<DuplicateController>(),
        ],
        Vec::new(),
        Arc::clone(&extensions),
    )
    .await
    .expect_err("duplicate controller metadata must fail");
    assert_eq!(
        duplicate,
        ControllerMaterializationError::DuplicateRegistration {
            controller: std::any::type_name::<DuplicateController>(),
        }
    );

    let missing = materialize_controller_routes(
        Vec::new(),
        vec![pending_route::<MissingController>(
            "fixture::MissingController::action",
            bind_missing,
        )],
        Arc::clone(&extensions),
    )
    .await
    .expect_err("missing controller metadata must fail");
    assert_eq!(
        missing,
        ControllerMaterializationError::MissingRegistration {
            controller: std::any::type_name::<MissingController>(),
            action: "fixture::MissingController::action",
        }
    );

    let incompatible = materialize_controller_routes(
        vec![ControllerRegistration::of::<DeclaredController>()],
        vec![pending_route::<DeclaredController>(
            "fixture::DeclaredController::action",
            bind_declared_as_expected,
        )],
        Arc::clone(&extensions),
    )
    .await
    .expect_err("an incompatible action binder must fail");
    assert_eq!(
        incompatible,
        ControllerMaterializationError::Binding {
            controller: std::any::type_name::<DeclaredController>(),
            action: "fixture::DeclaredController::action",
            source: ControllerBindingError::type_mismatch(
                std::any::type_name::<ExpectedController>()
            ),
        }
    );

    let initialization = materialize_controller_routes(
        vec![ControllerRegistration::of::<FailingController>()],
        vec![pending_route::<FailingController>(
            "fixture::FailingController::action",
            bind_noop::<FailingController>,
        )],
        extensions,
    )
    .await
    .expect_err("controller initialization errors must fail the build");
    assert_eq!(
        initialization,
        ControllerMaterializationError::Initialization {
            controller: std::any::type_name::<FailingController>(),
            source: ControllerInitError::Dependency,
        }
    );
    let public_message = initialization.to_string();
    assert!(!public_message.contains("administrator"));
    assert!(!public_message.contains("do-not-publish"));
    assert!(!public_message.contains("database.internal"));

    let panicked = materialize_controller_routes(
        vec![ControllerRegistration::of::<PanickingController>()],
        vec![pending_route::<PanickingController>(
            "fixture::PanickingController::action",
            bind_noop::<PanickingController>,
        )],
        app.services(),
    )
    .await
    .expect_err("controller initialization panics must cross a typed boundary");
    assert_eq!(
        panicked,
        ControllerMaterializationError::Initialization {
            controller: std::any::type_name::<PanickingController>(),
            source: ControllerInitError::Internal,
        }
    );

    app.close()
        .await
        .expect("application container closes cleanly");
}
