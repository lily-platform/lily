use std::sync::Arc;

use lily_http_api::async_trait::async_trait;
use lily_http_api::utoipa::{self, ToSchema};
use lily_http_api::{controller, Controller, ControllerInitError, ControllerTrait, Extensions};
use serde::Deserialize;

#[derive(Deserialize, ToSchema)]
struct Input {
    value: String,
}

#[derive(Controller)]
#[base_path("/invalid")]
struct InvalidController;

#[async_trait]
impl ControllerTrait for InvalidController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl InvalidController {
    #[post("/")]
    #[openapi(request_body(content_type = "application/json", schema = Input))]
    async fn action(&self, lily_http_api::Json(_input): lily_http_api::Json<Input>) -> String {
        String::new()
    }
}

fn main() {}
