use std::sync::Arc;

use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, ApplicationContainer, Controller, ControllerInitError, ControllerTrait,
    CorsDisabled, CorsPolicy, CorsPolicyProvider, Extensions, GuardInitError, GuardRejection,
    GuardTrait, HttpApiError, HttpExchange, HttpMiddleware, HttpMiddlewareError,
    HttpMiddlewareInitError, HttpNext, Json, MiddlewareDescriptor, MiddlewareKind, Request,
};

struct ControllerMiddleware;
struct ActionMiddleware;
struct FirstGuard;
struct SecondGuard;
struct ControllerCors;
struct ActionCors;

#[lily_http_api::async_trait::async_trait]
impl HttpMiddleware for ControllerMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("struct_controller", MiddlewareKind::Custom)
    }

    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        _cancellation: lily_http_api::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        next.run(exchange).await
    }
}

#[lily_http_api::async_trait::async_trait]
impl HttpMiddleware for ActionMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("struct_action", MiddlewareKind::Custom)
    }

    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        _cancellation: lily_http_api::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        next.run(exchange).await
    }
}

macro_rules! fixture_guard {
    ($guard:ty) => {
        #[lily_http_api::async_trait::async_trait]
        impl GuardTrait for $guard {
            async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitError> {
                Ok(Self)
            }

            async fn can_activate(
                &self,
                _request: &mut Request,
                _cancellation: lily_http_api::ExecutionCancellation,
            ) -> Result<(), GuardRejection> {
                Ok(())
            }
        }
    };
}

fixture_guard!(FirstGuard);
fixture_guard!(SecondGuard);

impl CorsPolicyProvider for ControllerCors {
    fn policy() -> CorsPolicy {
        CorsPolicy::new().allow_origins(["https://controller.example"])
    }
}

impl CorsPolicyProvider for ActionCors {
    fn policy() -> CorsPolicy {
        CorsPolicy::new().allow_origins(["https://action.example"])
    }
}

#[derive(Controller)]
#[base_path("/api/struct-controller")]
#[middleware(ControllerMiddleware)]
#[cors(ControllerCors)]
struct FixtureController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for FixtureController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl FixtureController {
    #[get("/")]
    async fn list(&self) -> Result<Json<&'static str>, HttpApiError> {
        Ok(Json("ok"))
    }

    #[post("/write")]
    #[guard(FirstGuard, SecondGuard)]
    #[middleware(ActionMiddleware)]
    #[cors(ActionCors)]
    async fn write(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/disabled")]
    #[cors(CorsDisabled)]
    async fn disabled(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[route(method = "PURGE", path = "/cache")]
    async fn purge(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/items/:id")]
    async fn parameterized(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/assets/*path")]
    async fn catch_all(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

#[test]
fn struct_controller_metadata_preserves_route_policy_order_and_identity() {
    let registrations = lily_http_api::__private::get_struct_controller_registrations();
    assert_eq!(registrations.len(), 1);
    assert_eq!(
        registrations[0].type_id(),
        std::any::TypeId::of::<FixtureController>()
    );

    let mut routes = lily_http_api::__private::get_pending_controller_routes();
    routes.sort_by(|left, right| left.path().cmp(right.path()));
    assert_eq!(routes.len(), 6);
    assert!(routes
        .iter()
        .all(|route| route.controller_type_id() == std::any::TypeId::of::<FixtureController>()));

    let list = routes
        .iter()
        .find(|route| route.path() == "/api/struct-controller")
        .expect("list metadata exists");
    assert_eq!(list.method(), "GET");
    assert_eq!(
        list.middleware_type_ids().collect::<Vec<_>>(),
        vec![std::any::TypeId::of::<ControllerMiddleware>()]
    );
    assert_eq!(
        list.cors_policy_type_id(),
        Some(std::any::TypeId::of::<ControllerCors>())
    );

    let write = routes
        .iter()
        .find(|route| route.path() == "/api/struct-controller/write")
        .expect("write metadata exists");
    assert_eq!(write.method(), "POST");
    assert_eq!(
        write.guard_type_ids().collect::<Vec<_>>(),
        vec![
            std::any::TypeId::of::<FirstGuard>(),
            std::any::TypeId::of::<SecondGuard>(),
        ]
    );
    assert_eq!(
        write.middleware_type_ids().collect::<Vec<_>>(),
        vec![
            std::any::TypeId::of::<ControllerMiddleware>(),
            std::any::TypeId::of::<ActionMiddleware>(),
        ]
    );
    assert_eq!(
        write.cors_policy_type_id(),
        Some(std::any::TypeId::of::<ActionCors>())
    );
    assert!(write.handler_name().ends_with("::FixtureController::write"));

    let disabled = routes
        .iter()
        .find(|route| route.path() == "/api/struct-controller/disabled")
        .expect("disabled metadata exists");
    assert_eq!(
        disabled.cors_policy_type_id(),
        Some(std::any::TypeId::of::<CorsDisabled>())
    );

    let purge = routes
        .iter()
        .find(|route| route.path() == "/api/struct-controller/cache")
        .expect("custom method metadata exists");
    assert_eq!(purge.method(), "PURGE");

    assert!(routes
        .iter()
        .any(|route| route.path() == "/api/struct-controller/items/:id"));
    assert!(routes
        .iter()
        .any(|route| route.path() == "/api/struct-controller/assets/*path"));
}

#[tokio::test]
async fn generated_metadata_materializes_without_app_builder_integration() {
    let app = ApplicationContainer::build()
        .await
        .expect("fixture container builds");
    let routes = lily_http_api::__private::materialize_controller_routes(
        lily_http_api::__private::get_struct_controller_registrations(),
        lily_http_api::__private::get_pending_controller_routes(),
        app.services(),
    )
    .await
    .expect("generated routes materialize");
    assert_eq!(routes.len(), 6);
    app.close().await.expect("fixture container closes");
}

#[tokio::test]
async fn app_route_table_accepts_exact_parameter_and_final_catch_all_actions() {
    let app = AppBuilder::new("127.0.0.1:0")
        .build()
        .await
        .expect("struct-controller parity application builds");
    let stats = app.route_stats();
    assert_eq!(stats.total_routes, 6);
    assert_eq!(stats.exact_routes, 4);
    assert_eq!(stats.param_routes, 2);

    app.close()
        .await
        .expect("struct-controller parity application closes");
}
