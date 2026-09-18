use lily_error::application::QueueHandlerError;
use queue_runtime::{queue, queue_service, Json};

type HandlerResult = Result<(), QueueHandlerError>;

struct AuditService;

#[queue_service]
impl AuditService {
    #[queue("audit.created", version = 1, content = "json")]
    async fn handle(&self, _message: Json<String>) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue_runtime::queue("audit.matched", version = 1, content = "json")]
    async fn r#match(&self, _message: Json<String>) -> HandlerResult {
        Ok(())
    }
}

fn main() {
    let handlers = queue_runtime::__private::get_all_queue_handlers();
    assert!(handlers
        .iter()
        .any(|handler| handler.queue_name == "audit.created"));
    assert!(handlers
        .iter()
        .any(|handler| handler.queue_name == "audit.matched" && handler.method_name == "r#match"));
}
