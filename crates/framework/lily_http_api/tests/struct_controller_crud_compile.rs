use async_trait::async_trait;
use lily_mongo_service::{BaseService, BaseServiceError};
use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, CorsDisabled, CorsPolicy,
    CorsPolicyProvider, Extensions, GuardInitError, GuardRejection, GuardTrait, HttpApiError,
    HttpExchange, HttpMiddleware, HttpMiddlewareError, HttpMiddlewareInitError, HttpNext, Json,
    MiddlewareDescriptor, MiddlewareKind, Path, Request, ServiceTrait,
};
use lily_injection::Injectable;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
pub struct UserDto {
    id: Option<String>,
}

#[derive(Deserialize)]
struct UserPath {
    id: String,
}

struct UserEntity;

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct UserService;

impl ServiceTrait for UserService {}

struct FixtureGuard;
struct ControllerMiddleware;
struct ActionMiddleware;
struct ControllerCorsPolicy;
struct ActionCorsPolicy;

impl CorsPolicyProvider for ControllerCorsPolicy {
    fn policy() -> CorsPolicy {
        CorsPolicy::new()
            .allow_origins(["https://controller.example"])
            .allow_methods(["GET", "POST", "PUT", "DELETE"])
    }
}

impl CorsPolicyProvider for ActionCorsPolicy {
    fn policy() -> CorsPolicy {
        CorsPolicy::new()
            .allow_origins(["https://action.example"])
            .allow_methods(["GET"])
    }
}

#[async_trait]
impl HttpMiddleware for ControllerMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("fixture_controller", MiddlewareKind::Custom)
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

#[async_trait]
impl HttpMiddleware for ActionMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("fixture_action", MiddlewareKind::Custom)
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

#[async_trait]
impl GuardTrait for FixtureGuard {
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

#[async_trait]
impl BaseService<UserDto, UserEntity> for UserService {
    async fn create(&self, dto: UserDto) -> Result<UserDto, BaseServiceError> {
        Ok(dto)
    }

    async fn create_many(&self, dtos: Vec<UserDto>) -> Result<Vec<UserDto>, BaseServiceError> {
        Ok(dtos)
    }

    async fn update(&self, dto: UserDto) -> Result<UserDto, BaseServiceError> {
        Ok(dto)
    }

    async fn delete(&self, _dto: UserDto) -> Result<bool, BaseServiceError> {
        Ok(true)
    }

    async fn delete_many(&self, dtos: Vec<UserDto>) -> Result<u64, BaseServiceError> {
        Ok(dtos.len() as u64)
    }

    async fn find_by_id(&self, id: &str) -> Result<UserDto, BaseServiceError> {
        Err(BaseServiceError::not_found("UserEntity", id))
    }

    async fn delete_by_id(&self, _id: &str) -> Result<bool, BaseServiceError> {
        Ok(false)
    }

    async fn find_by_ids(&self, _ids: Vec<String>) -> Result<Vec<UserDto>, BaseServiceError> {
        Ok(Vec::new())
    }
}

async fn find_user(service: &UserService, id: &str) -> Result<UserDto, HttpApiError> {
    Ok(service.find_by_id(id).await?)
}

async fn create_user(service: &UserService, entity: UserDto) -> Result<UserDto, HttpApiError> {
    Ok(service.create(entity).await?)
}

async fn update_user(
    service: &UserService,
    path_id: &str,
    entity: UserDto,
) -> Result<UserDto, HttpApiError> {
    if entity.id.as_deref() != Some(path_id) {
        return Err(HttpApiError::BadRequest(
            "path id and body identity must match".to_string(),
        ));
    }
    Ok(service.update(entity).await?)
}

async fn delete_user(service: &UserService, id: &str) -> Result<bool, HttpApiError> {
    Ok(service.delete_by_id(id).await?)
}

#[derive(Controller)]
#[base_path("/api/users")]
struct UserController {
    service: Arc<UserService>,
}

#[async_trait]
impl ControllerTrait for UserController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        let service = extensions
            .get_service::<UserService>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        Ok(Self { service })
    }
}

#[controller]
impl UserController {
    #[get("/:id")]
    #[guard(FixtureGuard)]
    async fn find_by_id(&self, Path(path): Path<UserPath>) -> Result<UserDto, HttpApiError> {
        find_user(&self.service, &path.id).await
    }

    #[post("/")]
    #[guard(FixtureGuard)]
    async fn create(&self, Json(entity): Json<UserDto>) -> Result<UserDto, HttpApiError> {
        create_user(&self.service, entity).await
    }

    #[put("/:id")]
    #[guard(FixtureGuard)]
    async fn update(
        &self,
        Path(path): Path<UserPath>,
        Json(entity): Json<UserDto>,
    ) -> Result<UserDto, HttpApiError> {
        update_user(&self.service, &path.id, entity).await
    }

    #[delete("/:id")]
    #[guard(FixtureGuard)]
    async fn delete(&self, Path(path): Path<UserPath>) -> Result<bool, HttpApiError> {
        delete_user(&self.service, &path.id).await
    }
}

#[derive(Controller)]
#[base_path("/api/overridden-users")]
#[middleware(ControllerMiddleware)]
struct OverriddenUserController {
    service: Arc<UserService>,
}

#[async_trait]
impl ControllerTrait for OverriddenUserController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        let service = extensions
            .get_service::<UserService>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        Ok(Self { service })
    }
}

#[controller]
impl OverriddenUserController {
    #[get("/:id")]
    #[middleware(ActionMiddleware)]
    async fn find_by_id(&self) -> Result<UserDto, HttpApiError> {
        Ok(UserDto { id: None })
    }

    #[post("/")]
    async fn create(&self, Json(entity): Json<UserDto>) -> Result<UserDto, HttpApiError> {
        create_user(&self.service, entity).await
    }

    #[put("/:id")]
    async fn update(
        &self,
        Path(path): Path<UserPath>,
        Json(entity): Json<UserDto>,
    ) -> Result<UserDto, HttpApiError> {
        update_user(&self.service, &path.id, entity).await
    }

    #[delete("/:id")]
    async fn delete(&self, Path(path): Path<UserPath>) -> Result<bool, HttpApiError> {
        delete_user(&self.service, &path.id).await
    }
}

#[derive(Controller)]
#[base_path("/api/reports")]
#[middleware(ControllerMiddleware)]
struct ReportsController;

#[async_trait]
impl ControllerTrait for ReportsController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl ReportsController {
    #[get("/")]
    #[guard(FixtureGuard)]
    #[middleware(ActionMiddleware)]
    async fn list_reports(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/public")]
    async fn list_public_reports(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

#[derive(Controller)]
#[base_path("/api/cors-reports")]
#[cors(ControllerCorsPolicy)]
#[middleware(ControllerMiddleware)]
struct CorsReportsController;

#[async_trait]
impl ControllerTrait for CorsReportsController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl CorsReportsController {
    #[get("/")]
    async fn default(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/action")]
    #[cors(ActionCorsPolicy)]
    #[middleware(ActionMiddleware)]
    async fn action(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/disabled")]
    #[cors(CorsDisabled)]
    async fn disabled(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

#[derive(Controller)]
#[base_path("/api/audit-exports")]
struct AuditExportsController;

#[async_trait]
impl ControllerTrait for AuditExportsController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl AuditExportsController {
    #[get("/")]
    #[middleware(ActionMiddleware)]
    async fn export(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

#[derive(Controller)]
#[base_path("/api/scoped-users")]
#[middleware(ControllerMiddleware)]
struct ScopedUserController {
    service: Arc<UserService>,
}

#[async_trait]
impl ControllerTrait for ScopedUserController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        let service = extensions
            .get_service::<UserService>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        Ok(Self { service })
    }
}

#[controller]
impl ScopedUserController {
    #[get("/:id")]
    #[guard(FixtureGuard)]
    #[middleware(ActionMiddleware)]
    async fn find_by_id(&self, Path(path): Path<UserPath>) -> Result<UserDto, HttpApiError> {
        find_user(&self.service, &path.id).await
    }

    #[post("/")]
    #[guard(FixtureGuard)]
    async fn create(&self, Json(entity): Json<UserDto>) -> Result<UserDto, HttpApiError> {
        create_user(&self.service, entity).await
    }

    #[put("/:id")]
    #[guard(FixtureGuard)]
    async fn update(
        &self,
        Path(path): Path<UserPath>,
        Json(entity): Json<UserDto>,
    ) -> Result<UserDto, HttpApiError> {
        update_user(&self.service, &path.id, entity).await
    }

    #[delete("/:id")]
    #[guard(FixtureGuard)]
    async fn delete(&self, Path(path): Path<UserPath>) -> Result<bool, HttpApiError> {
        delete_user(&self.service, &path.id).await
    }

    #[get("/export")]
    #[guard(FixtureGuard)]
    #[middleware(ActionMiddleware)]
    async fn export(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

#[derive(Controller)]
#[base_path("/api/cors-users")]
#[cors(ControllerCorsPolicy)]
#[middleware(ControllerMiddleware)]
struct CorsUserController {
    service: Arc<UserService>,
}

#[async_trait]
impl ControllerTrait for CorsUserController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        let service = extensions
            .get_service::<UserService>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        Ok(Self { service })
    }
}

#[controller]
impl CorsUserController {
    #[get("/:id")]
    #[guard(FixtureGuard)]
    #[middleware(ActionMiddleware)]
    #[cors(ActionCorsPolicy)]
    async fn find_by_id(&self, Path(path): Path<UserPath>) -> Result<UserDto, HttpApiError> {
        find_user(&self.service, &path.id).await
    }

    #[post("/")]
    #[guard(FixtureGuard)]
    #[cors(CorsDisabled)]
    async fn create(&self, Json(entity): Json<UserDto>) -> Result<UserDto, HttpApiError> {
        create_user(&self.service, entity).await
    }

    #[put("/:id")]
    #[guard(FixtureGuard)]
    async fn update(
        &self,
        Path(path): Path<UserPath>,
        Json(entity): Json<UserDto>,
    ) -> Result<UserDto, HttpApiError> {
        update_user(&self.service, &path.id, entity).await
    }

    #[delete("/:id")]
    #[guard(FixtureGuard)]
    async fn delete(&self, Path(path): Path<UserPath>) -> Result<bool, HttpApiError> {
        delete_user(&self.service, &path.id).await
    }

    #[get("/export")]
    #[guard(FixtureGuard)]
    #[middleware(ActionMiddleware)]
    #[cors(ActionCorsPolicy)]
    async fn export(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

#[test]
fn explicit_crud_surface_has_no_unbounded_list_route_and_keeps_guards() {
    let routes = lily_http_api::__private::get_pending_controller_routes();
    let routes = routes
        .iter()
        .filter(|route| route.path().starts_with("/api/users"))
        .collect::<Vec<_>>();

    assert_eq!(routes.len(), 4);
    assert!(!routes
        .iter()
        .any(|route| route.method() == "GET" && route.path() == "/api/users"));
    for route in routes {
        assert_eq!(
            route.guard_type_ids().collect::<Vec<_>>(),
            vec![std::any::TypeId::of::<FixtureGuard>()]
        );
    }
}

#[test]
fn explicit_actions_preserve_scope_then_action_middleware_order_and_override_identity() {
    let routes = lily_http_api::__private::get_pending_controller_routes();
    let expected = vec![
        std::any::TypeId::of::<ControllerMiddleware>(),
        std::any::TypeId::of::<ActionMiddleware>(),
    ];

    for (method, path) in [
        ("GET", "/api/scoped-users/:id"),
        ("GET", "/api/scoped-users/export"),
        ("GET", "/api/overridden-users/:id"),
        ("GET", "/api/reports"),
    ] {
        let route = routes
            .iter()
            .find(|route| route.method() == method && route.path() == path)
            .unwrap();
        assert_eq!(route.middleware_type_ids().collect::<Vec<_>>(), expected);
    }

    let create = routes
        .iter()
        .find(|route| route.method() == "POST" && route.path() == "/api/scoped-users")
        .unwrap();
    assert_eq!(
        create.middleware_type_ids().collect::<Vec<_>>(),
        vec![std::any::TypeId::of::<ControllerMiddleware>()]
    );

    let overridden = routes
        .iter()
        .find(|route| route.method() == "GET" && route.path() == "/api/overridden-users/:id")
        .unwrap();
    assert!(overridden
        .handler_name()
        .ends_with("::OverriddenUserController::find_by_id"));
}

#[test]
fn explicit_actions_preserve_cors_inheritance_override_disable_and_extra_route() {
    let routes = lily_http_api::__private::get_pending_controller_routes();

    for (method, path) in [
        ("GET", "/api/cors-users/:id"),
        ("POST", "/api/cors-users"),
        ("PUT", "/api/cors-users/:id"),
        ("DELETE", "/api/cors-users/:id"),
        ("GET", "/api/cors-users/export"),
    ] {
        assert!(
            routes
                .iter()
                .any(|route| route.method() == method && route.path() == path),
            "missing explicit CORS fixture route {method} {path}"
        );
    }

    for (method, path, policy) in [
        (
            "GET",
            "/api/cors-users/:id",
            std::any::TypeId::of::<ActionCorsPolicy>(),
        ),
        (
            "POST",
            "/api/cors-users",
            std::any::TypeId::of::<CorsDisabled>(),
        ),
        (
            "PUT",
            "/api/cors-users/:id",
            std::any::TypeId::of::<ControllerCorsPolicy>(),
        ),
        (
            "GET",
            "/api/cors-users/export",
            std::any::TypeId::of::<ActionCorsPolicy>(),
        ),
    ] {
        let route = routes
            .iter()
            .find(|route| route.method() == method && route.path() == path)
            .unwrap();
        assert_eq!(route.cors_policy_type_id(), Some(policy));
    }
}

#[tokio::test]
async fn explicit_action_bodies_call_the_existing_base_service_contract() {
    let service = UserService;
    assert_eq!(
        create_user(
            &service,
            UserDto {
                id: Some("created".to_string()),
            },
        )
        .await
        .unwrap()
        .id
        .as_deref(),
        Some("created")
    );

    assert_eq!(
        update_user(
            &service,
            "updated",
            UserDto {
                id: Some("updated".to_string()),
            },
        )
        .await
        .unwrap()
        .id
        .as_deref(),
        Some("updated")
    );

    assert!(matches!(
        update_user(
            &service,
            "path-id",
            UserDto {
                id: Some("body-id".to_string()),
            },
        )
        .await,
        Err(HttpApiError::BadRequest(_))
    ));

    assert!(!delete_user(&service, "deleted").await.unwrap());
    assert!(matches!(
        find_user(&service, "deleted").await,
        Err(HttpApiError::NotFound(_))
    ));
}

#[tokio::test]
async fn explicit_crud_actions_materialize_one_immutable_application_route_table() {
    let app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
    let stats = app.route_stats();
    assert_eq!(stats.total_routes, 24);
    assert_eq!(stats.exact_routes, 12);
    assert_eq!(stats.param_routes, 12);

    app.close().await.unwrap();
}
