use std::sync::Arc;

use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, Extensions, HttpApiError,
    StaticFileResponse,
};

#[derive(Controller)]
#[base_path("/api/static_file_response_compile")]
struct StaticFileResponseController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for StaticFileResponseController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl StaticFileResponseController {
    #[get("/assets/*path")]
    async fn static_file_action(&self) -> Result<StaticFileResponse, HttpApiError> {
        unreachable!("compile-only controller contract")
    }
}

#[test]
fn controller_accepts_direct_static_file_response_result() {
    let routes = lily_http_api::__private::get_pending_controller_routes();
    assert_eq!(routes.len(), 1);
    assert_eq!(
        routes[0].path(),
        "/api/static_file_response_compile/assets/*path"
    );
}
