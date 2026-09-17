//! MongoDB database handle owned by the application DI/container lifecycle.

use std::sync::{Arc, RwLock};

#[cfg(feature = "single")]
use async_trait::async_trait;
#[cfg(feature = "single")]
use lily_config::ConfigService;
use lily_error::application::mongodb::MongoDbError;
#[cfg(feature = "single")]
use lily_error::injection::InjectionError;
#[cfg(feature = "single")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "single")]
use lily_injection::ServiceTrait;
use mongodb::{
    Client, Collection, Database as MongoDatabase,
    bson::{Bson, Document, doc},
    options::{Acknowledgment, ReadConcern, WriteConcern},
};
use tokio_util::sync::CancellationToken;

use crate::options::MongoClientPlan;

const MAX_TRANSACTION_COMMIT_TIME: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Default)]
struct DatabaseState {
    client: RwLock<Option<Client>>,
    database: RwLock<Option<MongoDatabase>>,
    name: RwLock<Option<String>>,
    operation_timeout: RwLock<Option<std::time::Duration>>,
}

/// Application-owned MongoDB client and database handle.
///
/// With the default `single` feature this is an injectable singleton. Its
/// lifecycle reads `lily_config::DatabaseConfig`, performs a startup ping and
/// releases every client handle during disposal. In `factory` mode instances
/// are created and owned by `MongoFactory` instead of being registered as
/// independent services.
///
/// Clones share the same underlying state. `Default` exists so Lily's derive
/// can construct the service before initialization; an application must not
/// use a default value as a connected database.
#[cfg_attr(feature = "single", derive(Injectable))]
#[cfg_attr(feature = "single", service(lifetime = "Singleton"))]
#[derive(Clone, Default)]
pub struct DatabaseService {
    #[cfg(feature = "single")]
    #[inject]
    config_service: Arc<ConfigService>,
    state: Arc<DatabaseState>,
    #[cfg(all(feature = "test-support", feature = "single"))]
    test_container_fixture: bool,
}

impl DatabaseService {
    /// Builds an inert singleton seed for transport-free framework tests.
    ///
    /// This fixture performs no driver construction or network I/O. Database
    /// operations remain fail-closed as uninitialized; only the DI lifecycle
    /// initializer is bypassed so unrelated composition/lifecycle tests can
    /// retain the production registration graph.
    #[cfg(all(feature = "test-support", feature = "single"))]
    #[doc(hidden)]
    #[must_use]
    pub fn test_container_fixture() -> Self {
        Self {
            test_container_fixture: true,
            ..Self::default()
        }
    }

    /// Connects an explicitly validated plan and verifies server readiness.
    ///
    /// Normal `single` applications resolve the container-managed service and
    /// do not call this method. It is public for standalone composition roots,
    /// qualification programs and manually owned clients. The returned service
    /// must eventually be closed by its owner.
    pub async fn connect(plan: MongoClientPlan) -> Result<Self, MongoDbError> {
        let client = plan.connect_and_ping().await?;
        let service = Self::default();
        service.install(client, &plan);
        Ok(service)
    }

    fn install(&self, client: Client, plan: &MongoClientPlan) {
        *self.state.database.write().expect("database state lock") =
            Some(client.database(plan.database_name()));
        *self.state.client.write().expect("client state lock") = Some(client);
        *self.state.name.write().expect("database name lock") =
            Some(plan.database_name().to_owned());
        *self
            .state
            .operation_timeout
            .write()
            .expect("database timeout lock") = Some(plan.operation_timeout());
    }

    fn database(&self) -> Result<MongoDatabase, MongoDbError> {
        self.state
            .database
            .read()
            .map_err(|_| MongoDbError::InternalError("database state lock is poisoned".into()))?
            .clone()
            .ok_or_else(|| {
                MongoDbError::ConnectionFailed("DatabaseService is not initialized".into())
            })
    }

    /// Returns the configured database name after successful initialization.
    ///
    /// An uninitialized or already closed service returns
    /// `MongoDbError::ConnectionFailed`.
    pub fn name(&self) -> Result<String, MongoDbError> {
        self.state
            .name
            .read()
            .map_err(|_| MongoDbError::InternalError("database name lock is poisoned".into()))?
            .clone()
            .ok_or_else(|| {
                MongoDbError::ConnectionFailed("DatabaseService is not initialized".into())
            })
    }

    /// Creates a repository operation context with the configured deadline.
    ///
    /// The caller owns the cancellation token and may further attach a
    /// transaction or optimistic-concurrency revision to the returned context.
    pub fn operation_context(
        &self,
        cancellation: CancellationToken,
    ) -> Result<lily_mongo_repository::MongoOperationContext<'static>, MongoDbError> {
        let timeout = self
            .state
            .operation_timeout
            .read()
            .map_err(|_| MongoDbError::InternalError("database timeout lock is poisoned".into()))?
            .ok_or_else(|| {
                MongoDbError::ConnectionFailed("DatabaseService is not initialized".into())
            })?;
        lily_mongo_repository::MongoOperationContext::new(cancellation).with_deadline(timeout)
    }

    /// Starts a MongoDB transaction governed by `operation`.
    ///
    /// MongoDB transactions require a compatible deployment such as a replica
    /// set. The caller owns the returned transaction and must commit or abort
    /// it explicitly through the base-repository transaction API.
    pub async fn begin_transaction(
        &self,
        operation: &lily_mongo_repository::MongoOperationContext<'_>,
    ) -> Result<lily_mongo_repository::MongoTransaction, MongoDbError> {
        let max_commit_time = self.operation_timeout()?.min(MAX_TRANSACTION_COMMIT_TIME);
        self.begin_transaction_with_commit_timeout(operation, max_commit_time)
            .await
    }

    /// Starts a reliable transaction with an explicit bounded commit timeout.
    ///
    /// This hidden framework seam lets a transaction owner apply its own
    /// validated retry policy without changing the database-wide operation
    /// timeout. A zero timeout is rejected before any driver work begins.
    #[doc(hidden)]
    pub async fn begin_transaction_with_commit_timeout(
        &self,
        operation: &lily_mongo_repository::MongoOperationContext<'_>,
        max_commit_time: std::time::Duration,
    ) -> Result<lily_mongo_repository::MongoTransaction, MongoDbError> {
        if max_commit_time.is_zero() || max_commit_time > MAX_TRANSACTION_COMMIT_TIME {
            return Err(MongoDbError::InvalidConfiguration(
                "MongoDB transaction max commit time must be between 1 millisecond and 60 seconds"
                    .into(),
            ));
        }
        let client = self
            .state
            .client
            .read()
            .map_err(|_| MongoDbError::InternalError("MongoDB client lock is poisoned".into()))?
            .clone()
            .ok_or_else(|| {
                MongoDbError::ConnectionFailed("DatabaseService is not initialized".into())
            })?;
        let observation = operation
            .transaction_observation()
            .cloned()
            .unwrap_or_default();
        operation
            .execute(async {
                let mut session = client
                    .start_session()
                    .await
                    .map_err(|error| operation.map_driver_error(error))?;
                session
                    .start_transaction()
                    .read_concern(ReadConcern::snapshot())
                    .write_concern(
                        WriteConcern::builder()
                            .w(Acknowledgment::Majority)
                            .journal(true)
                            .build(),
                    )
                    .max_commit_time(max_commit_time)
                    .await
                    .map_err(|error| operation.map_driver_error(error))?;
                Ok(
                    lily_mongo_repository::MongoTransaction::from_started_session_with_observation(
                        session,
                        observation,
                    ),
                )
            })
            .await
    }

    /// Verifies that the connected deployment can execute multi-document
    /// transactions without creating or modifying application storage.
    ///
    /// A compatible deployment requires logical sessions and wire version 8
    /// or newer (MongoDB 4.2+). This common floor covers both multi-document
    /// transactions and framework components which use server-time update
    /// pipelines for distributed lease authority. Standalone servers fail
    /// closed with a secret-safe typed configuration error.
    pub async fn verify_transaction_capability(&self) -> Result<(), MongoDbError> {
        let database = self.database()?;
        let operation = self.operation_context(CancellationToken::new())?;
        let hello = operation
            .execute(async {
                database
                    .run_command(doc! { "hello": 1 })
                    .await
                    .map_err(map_capability_probe_error)
            })
            .await?;
        verify_transaction_hello(&hello)
    }

    /// Lists collection names, optionally restricting the driver command with
    /// a MongoDB filter document.
    ///
    /// Schema creation remains the responsibility of `MongoMigrationRunner`;
    /// this inspection method never creates a collection.
    pub async fn list_collection_names(
        &self,
        filter: Option<Document>,
    ) -> Result<Vec<String>, MongoDbError> {
        let database = self.database()?;
        let action = database.list_collection_names();
        let names = match filter {
            Some(filter) => action.filter(filter).await?,
            None => action.await?,
        };
        Ok(names)
    }

    /// Returns bounded read-only collection metadata for framework readiness
    /// checks without creating or modifying storage.
    ///
    /// The returned driver value includes collection options such as the JSON
    /// schema validator. This is hidden framework ABI rather than the
    /// application CRUD surface.
    #[doc(hidden)]
    pub async fn list_collection_specifications(
        &self,
        filter: Option<Document>,
    ) -> Result<Vec<crate::CollectionSpecification>, MongoDbError> {
        use futures::TryStreamExt;

        let database = self.database()?;
        let operation = self.operation_context(CancellationToken::new())?;
        operation
            .execute(async {
                let action = database.list_collections();
                let cursor = match filter {
                    Some(filter) => action.filter(filter).await,
                    None => action.await,
                }
                .map_err(map_collection_inspection_error)?;
                cursor
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(map_collection_inspection_error)
            })
            .await
    }

    /// Returns a typed lightweight driver handle for a validated collection.
    ///
    /// Application CRUD may use the public methods generated by
    /// `#[derive(MongoCollection)]` directly or the optional common contract
    /// generated by `#[derive(Repository)]`. Use this lower-level escape hatch
    /// for deliberate driver operations outside both generated APIs. It does
    /// not create the collection.
    pub fn collection<T>(&self, name: &str) -> Result<Collection<T>, MongoDbError>
    where
        T: Send + Sync,
    {
        validate_collection_name(name)?;
        Ok(self.database()?.collection::<T>(name))
    }

    pub(crate) fn raw_database(&self) -> Result<MongoDatabase, MongoDbError> {
        self.database()
    }

    fn operation_timeout(&self) -> Result<std::time::Duration, MongoDbError> {
        self.state
            .operation_timeout
            .read()
            .map_err(|_| MongoDbError::InternalError("database timeout lock is poisoned".into()))?
            .ok_or_else(|| {
                MongoDbError::ConnectionFailed("DatabaseService is not initialized".into())
            })
    }

    /// Idempotently releases this service's MongoDB client handles.
    ///
    /// Container-managed applications rely on `ServiceTrait::dispose`; a
    /// standalone owner calls this explicitly. Existing cloned collection
    /// handles may keep driver resources alive until those clones are dropped.
    pub fn close(&self) -> Result<(), MongoDbError> {
        *self.state.client.write().map_err(|_| {
            MongoDbError::InternalError("MongoDB client state lock poisoned".into())
        })? = None;
        *self.state.database.write().map_err(|_| {
            MongoDbError::InternalError("MongoDB database state lock poisoned".into())
        })? = None;
        *self.state.name.write().map_err(|_| {
            MongoDbError::InternalError("MongoDB database name lock poisoned".into())
        })? = None;
        *self.state.operation_timeout.write().map_err(|_| {
            MongoDbError::InternalError("MongoDB timeout state lock poisoned".into())
        })? = None;
        Ok(())
    }
}

fn validate_collection_name(name: &str) -> Result<(), MongoDbError> {
    if name.is_empty()
        || name.len() > 120
        || name.starts_with("system.")
        || name
            .chars()
            .any(|character| character == '$' || character == '\0')
    {
        return Err(MongoDbError::InvalidCollectionName(
            "collection name is empty, reserved, or contains a forbidden character".into(),
        ));
    }
    Ok(())
}

fn verify_transaction_hello(hello: &Document) -> Result<(), MongoDbError> {
    let sessions = match hello.get("logicalSessionTimeoutMinutes") {
        Some(Bson::Int32(value)) => *value > 0,
        Some(Bson::Int64(value)) => *value > 0,
        _ => false,
    };
    let replica_set = hello
        .get_str("setName")
        .is_ok_and(|name| !name.trim().is_empty());
    let mongos = hello
        .get_str("msg")
        .is_ok_and(|message| message == "isdbgrid");
    let wire_version = match hello.get("maxWireVersion") {
        Some(Bson::Int32(value)) => i64::from(*value),
        Some(Bson::Int64(value)) => *value,
        _ => -1,
    };
    let compatible = sessions && (replica_set || mongos) && wire_version >= 8;
    compatible.then_some(()).ok_or_else(|| {
        MongoDbError::InvalidConfiguration(
            "MongoDB deployment does not support required multi-document transactions".into(),
        )
    })
}

fn map_capability_probe_error(error: mongodb::error::Error) -> MongoDbError {
    use mongodb::error::ErrorKind;

    match error.kind.as_ref() {
        ErrorKind::Authentication { .. } => MongoDbError::AuthenticationFailed(
            "MongoDB transaction capability probe authentication failed".into(),
        ),
        ErrorKind::Io(_) | ErrorKind::ServerSelection { .. } => MongoDbError::ConnectionFailed(
            "MongoDB transaction capability probe could not reach the deployment".into(),
        ),
        _ => MongoDbError::QueryFailed(
            "MongoDB transaction capability probe did not complete".into(),
        ),
    }
}

fn map_collection_inspection_error(error: mongodb::error::Error) -> MongoDbError {
    use mongodb::error::ErrorKind;

    match error.kind.as_ref() {
        ErrorKind::Authentication { .. } => MongoDbError::AuthenticationFailed(
            "MongoDB collection metadata inspection authentication failed".into(),
        ),
        ErrorKind::Io(_) | ErrorKind::ServerSelection { .. } => MongoDbError::ConnectionFailed(
            "MongoDB collection metadata inspection could not reach the deployment".into(),
        ),
        _ => MongoDbError::QueryFailed(
            "MongoDB collection metadata inspection did not complete".into(),
        ),
    }
}

#[cfg(feature = "single")]
#[async_trait]
impl ServiceTrait for DatabaseService {
    #[lily_trace::prelude::instrument(name = "database.service.initialize", skip(self))]
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        #[cfg(feature = "test-support")]
        if self.test_container_fixture {
            return Ok(());
        }
        let config = self.config_service.get_lily_config().await;
        let database_config = config.database.as_ref().ok_or_else(|| {
            InjectionError::InitError("MongoDB database configuration is missing".into())
        })?;
        let plan = MongoClientPlan::from_database(database_config)
            .map_err(|error| InjectionError::InitError(error.to_string()))?;
        let client = plan.connect_and_ping().await.map_err(|error| {
            tracing::error!(error = %error, "MongoDB startup readiness probe failed");
            InjectionError::InitError("MongoDB startup readiness probe failed".into())
        })?;
        self.install(client, &plan);
        tracing::info!(database = %plan.database_name(), "DatabaseService initialized");
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.close()
            .map_err(|error| InjectionError::General(error.to_string()))?;
        tracing::info!("DatabaseService disposed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uninitialized_service_is_typed_and_collection_names_are_validated() {
        let service = DatabaseService::default();
        assert!(matches!(
            service.name(),
            Err(MongoDbError::ConnectionFailed(_))
        ));
        assert!(matches!(
            service.collection::<Document>("system.users"),
            Err(MongoDbError::InvalidCollectionName(_))
        ));
    }

    #[test]
    fn transaction_capability_requires_sessions_and_supported_topology() {
        assert!(
            verify_transaction_hello(&doc! {
                "logicalSessionTimeoutMinutes": 30,
                "setName": "rs0",
                "maxWireVersion": 8,
            })
            .is_ok()
        );
        assert!(
            verify_transaction_hello(&doc! {
                "logicalSessionTimeoutMinutes": 30_i64,
                "msg": "isdbgrid",
                "maxWireVersion": 8_i64,
            })
            .is_ok()
        );
        for unsupported in [
            doc! { "setName": "rs0", "maxWireVersion": 7 },
            doc! {
                "logicalSessionTimeoutMinutes": 30,
                "maxWireVersion": 25,
            },
            doc! {
                "logicalSessionTimeoutMinutes": 30,
                "setName": "rs0",
                "maxWireVersion": 7,
            },
            doc! {
                "logicalSessionTimeoutMinutes": 30,
                "msg": "isdbgrid",
                "maxWireVersion": 7,
            },
        ] {
            assert!(verify_transaction_hello(&unsupported).is_err());
        }
    }

    #[tokio::test]
    async fn explicit_transaction_commit_timeout_is_bounded_before_driver_work() {
        let service = DatabaseService::default();
        let operation = lily_mongo_repository::MongoOperationContext::detached();

        for timeout in [
            std::time::Duration::ZERO,
            MAX_TRANSACTION_COMMIT_TIME + std::time::Duration::from_nanos(1),
        ] {
            assert!(matches!(
                service
                    .begin_transaction_with_commit_timeout(&operation, timeout)
                    .await,
                Err(MongoDbError::InvalidConfiguration(_))
            ));
        }
        assert!(matches!(
            service
                .begin_transaction_with_commit_timeout(&operation, MAX_TRANSACTION_COMMIT_TIME)
                .await,
            Err(MongoDbError::ConnectionFailed(_))
        ));
    }
}
