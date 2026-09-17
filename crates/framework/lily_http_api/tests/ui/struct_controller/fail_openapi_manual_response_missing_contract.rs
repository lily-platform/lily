use std::sync::Arc;

use lily_http_api::async_trait::async_trait;
use lily_http_api::{controller, Controller, ControllerInitError, ControllerTrait, Extensions};

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
    #[openapi]
    async fn action(
        &self,
        _response: &mut lily_http_api::Response,
    ) -> Result<(), lily_http_api::HttpApiError> {
        Ok(())
    }
}

fn main() {}
