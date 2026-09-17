use std::{convert::Infallible, sync::Arc, time::Duration};

use lily_http_api::{
    controller, sse, Controller, ControllerInitError, ControllerTrait, Extensions, HttpApiError,
    SseEvent, SseResponse,
};

#[derive(Controller)]
#[base_path("/api/sse_response_compile")]
struct SseResponseController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for SseResponseController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl SseResponseController {
    #[get("/events")]
    async fn sse_action(&self) -> Result<SseResponse, HttpApiError> {
        let event = SseEvent::new("first line\nsecond line")?
            .event("notification")?
            .id("event-42")?
            .retry(Duration::from_secs(5))?;
        let source = futures::stream::iter([Ok::<_, Infallible>(event)]);
        Ok(sse(source).keep_alive(Duration::from_secs(15))?)
    }
}

#[test]
fn controller_accepts_direct_sse_response_result() {
    let routes = lily_http_api::__private::get_pending_controller_routes();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].path(), "/api/sse_response_compile/events");
}
