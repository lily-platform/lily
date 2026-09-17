use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleError, RecycleResult};
use redis::{Client, RedisError, aio::MultiplexedConnection};

#[derive(Debug)]
pub(crate) struct RedisPoolManager {
    client: Client,
}

impl RedisPoolManager {
    pub(crate) const fn new(client: Client) -> Self {
        Self { client }
    }
}

impl Manager for RedisPoolManager {
    type Type = MultiplexedConnection;
    type Error = RedisError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        self.client.get_multiplexed_async_connection().await
    }

    async fn recycle(
        &self,
        connection: &mut Self::Type,
        _metrics: &Metrics,
    ) -> RecycleResult<Self::Error> {
        let response: String = redis::cmd("PING")
            .query_async(connection)
            .await
            .map_err(RecycleError::Backend)?;
        if response == "PONG" {
            Ok(())
        } else {
            Err(RecycleError::Message(
                "Redis returned an invalid recycle response".into(),
            ))
        }
    }
}

pub(crate) type RedisPool = Pool<RedisPoolManager>;
pub(crate) type RedisConnection = Object<RedisPoolManager>;
