use lily_consumer::{Consumer, Injectable, ServiceTrait};
use lily_queue::{queue, queue_service, Json, QueueHandlerError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct UserCreated {
    user_id: String,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct UserEventsConsumer;

impl ServiceTrait for UserEventsConsumer {}

#[queue_service]
impl UserEventsConsumer {
    #[queue("users.created", version = 1, content = "json")]
    async fn handle_user_created(
        &self,
        Json(_message): Json<UserCreated>,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

/// Type-check queue metadata generation, DI registration and the canonical
/// startup facade without loading config or opening a broker connection.
#[allow(dead_code)]
fn compile_consumer_application() {
    let run = Consumer::run();
    drop(run);
}

/// Compile-only proof that an application can own a Mongo adapter explicitly.
/// No Mongo type leaks into `lily_consumer` or the provider-neutral handler.
#[cfg(feature = "mongodb-adapter")]
#[allow(dead_code)]
fn compile_application_owned_mongodb_adapter(
    id: &str,
) -> Result<(), lily_base_repository::MongoRepositoryError> {
    let _document_id = lily_base_repository::MongoDocumentId::parse(id)?;
    let _client_plan: Option<lily_mongodb::MongoClientPlan> = None;
    Ok(())
}

fn main() {}
