#![allow(dead_code, unused_imports, unused_macros)]

pub(crate) use std::sync::Arc;

pub(crate) use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, Extensions, HttpApiError,
    PassthroughResponseContext, Request, Response,
};

macro_rules! impl_controller_trait {
    ($controller:ty) => {
        #[lily_http_api::async_trait::async_trait]
        impl ControllerTrait for $controller {
            async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
                Ok(Self)
            }
        }
    };
}

pub(crate) use impl_controller_trait;
