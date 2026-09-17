use std::sync::Arc;

use lily_websocket::guard::{GuardInitializationError, WebSocketGuardRejection, WsGuard};
use lily_websocket::middleware::{
    MiddlewareDescriptor, MiddlewareKind, WsMessageExchange, WsMessageMiddleware,
    WsMiddlewareInitError,
};
use lily_websocket::{
    Extensions, NoReply, WebSocketActionError, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, async_trait, websocket_controller,
};

struct ControllerMiddleware;
struct ActionMiddleware;
struct ControllerGuard;
struct ActionGuard;

macro_rules! message_middleware {
    ($middleware:ty, $name:literal) => {
        #[async_trait]
        impl WsMessageMiddleware for $middleware {
            async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
                Ok(Self)
            }

            fn descriptor(&self) -> MiddlewareDescriptor {
                MiddlewareDescriptor::new($name, MiddlewareKind::Custom)
            }

            async fn on_message_termination(
                &self,
                mut context: lily_websocket::middleware::WsMessageTerminationContext<'_>,
                signal: lily_websocket::CleanupCancellation,
            ) -> Result<(), lily_websocket::middleware::WsMiddlewareError> {
                let _: lily_websocket::CleanupCancellation = context.cancellation().clone();
                let _ = (signal.is_cancelled(), context.deadline(), context.reason(), context.normal_exit());
                context.remove_message_local::<String>();
                Ok(())
            }
        }
    };
}

macro_rules! guard {
    ($guard:ty) => {
        #[async_trait]
        impl WsGuard for $guard {
            async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
                Ok(Self)
            }

            async fn can_activate(
                &self,
                _exchange: &mut WsMessageExchange,
                _cancellation: lily_websocket::ExecutionCancellation,
            ) -> Result<(), WebSocketGuardRejection> {
                Ok(())
            }
        }
    };
}

message_middleware!(ControllerMiddleware, "controller_middleware");
message_middleware!(ActionMiddleware, "action_middleware");
guard!(ControllerGuard);
guard!(ActionGuard);

#[derive(WebSocketController)]
#[namespace("chat")]
#[message_middleware(ControllerMiddleware)]
#[guard(ControllerGuard)]
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl ChatController {
    #[message("send")]
    #[message_middleware(ActionMiddleware)]
    #[guard(ActionGuard)]
    async fn send(&self) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

fn main() {}
