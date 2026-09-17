use std::sync::Arc;

use lily_http_api::async_trait::async_trait;
use lily_http_api::utoipa::{self, IntoParams, ToSchema};
use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, Extensions, HttpApiError, Json,
    Path,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, IntoParams)]
struct UserPath {
    id: String,
}

#[derive(Deserialize, ToSchema)]
struct UserInput {
    name: String,
}

#[derive(Serialize, ToSchema)]
struct UserView {
    id: String,
    name: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct ErrorView {
    code: String,
}

#[derive(Controller)]
#[base_path("/users")]
#[openapi(
    tag = "Users",
    responses((status = 401, description = "Unauthorized", schema = ErrorView)),
    security(("bearer" = []))
)]
struct UsersController;

#[async_trait]
impl ControllerTrait for UsersController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl UsersController {
    /// Update a user.
    #[put("/:id")]
    #[openapi(
        operation_id = "users.update",
        responses((status = 404, description = "Not found", schema = ErrorView))
    )]
    async fn update(
        &self,
        Path(path): Path<UserPath>,
        Json(input): Json<UserInput>,
    ) -> Result<UserView, HttpApiError> {
        Ok(UserView {
            id: path.id,
            name: input.name,
        })
    }

    #[get("/internal")]
    #[openapi(skip)]
    async fn internal(&self) -> String {
        "internal".to_owned()
    }
}

fn main() {}
