#![cfg(not(feature = "asyncapi"))]

use std::{any::Any, sync::Arc};

use lily_error::application::{QueueHandlerError, QueueHandlerFailureClass};
use queue_runtime::{Json, QueuePayloadKind, queue, queue_service};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AuditMessage {
    _value: String,
}

#[derive(Debug)]
struct TypedPermanentError;

impl From<TypedPermanentError> for QueueHandlerError {
    fn from(_: TypedPermanentError) -> Self {
        Self::permanent("AUDIT_MESSAGE_REJECTED")
    }
}

struct CategoryAuditService;

#[queue_service]
impl CategoryAuditService {
    #[queue("audit.handler-category", version = 1, content = "json")]
    async fn reject(&self, _message: Json<AuditMessage>) -> Result<(), TypedPermanentError> {
        Err(TypedPermanentError)
    }
}

type AliasHandlerResult = Result<(), QueueHandlerError>;

struct RawMethodService;

#[queue_service]
impl RawMethodService {
    #[queue_runtime::queue("audit.raw-method", version = 1, content = "json")]
    async fn r#match(&self, _message: Json<AuditMessage>) -> AliasHandlerResult {
        Ok(())
    }
}

#[test]
fn real_facade_collects_exact_generated_metadata() {
    let handlers = queue_runtime::__private::get_all_queue_handlers();
    let raw = handlers
        .iter()
        .find(|handler| handler.queue_name == "audit.raw-method")
        .expect("raw-method handler metadata");

    assert_eq!(raw.method_name, "r#match");
    assert!(raw.handler_name.ends_with("::RawMethodService::r#match"));
    assert_eq!(raw.schema_version, 1);
    assert_eq!(raw.content_kind, "json");
    assert_eq!(raw.input_contract.payload_kind, QueuePayloadKind::Json);
    assert!(
        raw.input_contract
            .payload_type_name
            .is_some_and(|name| name.contains("Json") && name.contains("AuditMessage"))
    );
}

#[tokio::test]
async fn generated_wrapper_rejects_erased_invocation_type_without_panicking() {
    let service: Arc<dyn Any + Send + Sync> = Arc::new(CategoryAuditService);
    let mut invocation = ();

    let error = __queue_handler_CategoryAuditService_reject(service, &mut invocation)
        .await
        .expect_err("an unrelated erased invocation must be rejected");

    assert_eq!(error.class(), QueueHandlerFailureClass::Retryable);
    assert_eq!(error.code(), "QUEUE_HANDLER_INVOCATION_TYPE_MISMATCH");
}

#[tokio::test]
async fn generated_wrapper_rejects_service_type_mismatch_without_type_details() {
    let wrong_service: Arc<dyn Any + Send + Sync> = Arc::new(());
    let mut invocation = ();

    let error = __queue_handler_CategoryAuditService_reject(wrong_service, &mut invocation)
        .await
        .expect_err("an unrelated erased service must be rejected");

    assert_eq!(error.class(), QueueHandlerFailureClass::Retryable);
    assert_eq!(error.code(), "QUEUE_HANDLER_SERVICE_TYPE_MISMATCH");
    assert!(!error.to_string().contains("CategoryAuditService"));
}
