use lily_consumer::{Consumer, ConsumerError};
use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use lily_queue::{Json, QueueHandlerError, queue, queue_service};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct UserCreated {
    user_id: String,
    email: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct UserDeleted {
    user_id: String,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct UserService;

impl ServiceTrait for UserService {}

#[queue_service]
impl UserService {
    #[queue("user.created", version = 1, content = "json")]
    async fn handle_user_created(
        &self,
        Json(message): Json<UserCreated>,
    ) -> Result<(), QueueHandlerError> {
        println!(
            "created user {}: {} <{}>",
            message.user_id, message.name, message.email
        );
        Ok(())
    }

    #[queue("user.deleted", version = 1, content = "json")]
    async fn handle_user_deleted(
        &self,
        Json(message): Json<UserDeleted>,
    ) -> Result<(), QueueHandlerError> {
        println!("deleted user {}", message.user_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), ConsumerError> {
    Consumer::run().await
}
