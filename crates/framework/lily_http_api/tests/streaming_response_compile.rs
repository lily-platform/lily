use std::{convert::Infallible, sync::Arc};

use bytes::Bytes;
use lily_http_api::{
    controller, streaming, Controller, ControllerInitError, ControllerTrait, Extensions,
    HttpApiError, StreamingResponse,
};

#[derive(Controller)]
#[base_path("/api/streaming_response_compile")]
struct StreamingResponseController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for StreamingResponseController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl StreamingResponseController {
    #[get("/stream")]
    async fn streaming_action(&self) -> Result<StreamingResponse, HttpApiError> {
        let source = futures::stream::iter([
            Ok::<_, Infallible>(Bytes::from_static(b"first")),
            Ok(Bytes::from_static(b"second")),
        ]);
        let stream = streaming(source)
            .content_type("application/x-ndjson")
            .max_chunk_bytes(1024);
        Ok(stream)
    }
}

#[test]
fn controller_accepts_direct_streaming_response_result() {
    let routes = lily_http_api::__private::get_pending_controller_routes();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].path(), "/api/streaming_response_compile/stream");
}
