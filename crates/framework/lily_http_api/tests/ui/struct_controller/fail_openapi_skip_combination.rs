use std::sync::Arc;

use lily_http_api::async_trait::async_trait;
use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, Extensions,
};

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
    #[get("/")]
    #[openapi(skip, summary = "cannot be combined")]
    async fn action(&self) -> String {
        String::new()
    }
}

fn main() {}
