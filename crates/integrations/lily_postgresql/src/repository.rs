use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use diesel::associations::HasTable;
use diesel::dsl::{AsSelect, Find, Select};
use diesel::pg::Pg;
use diesel::prelude::{OptionalExtension, Selectable, SelectableHelper};
use diesel::query_builder::QueryId;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::methods::LoadQuery;

use crate::{ExecutionCancellation, PgConnectionFuture, PgDatabaseService, PgResult};

/// Optional repository connection boundary implemented by
/// `#[derive(PgRepository)]`.
///
/// Lily does not add entity metadata or a query abstraction here. `Entity` is
/// the user's ordinary Diesel model and custom repository methods continue to
/// use Diesel's public API inside these scoped callbacks. Applications that do
/// not need generated entity CRUD may inject [`PgDatabaseService`] directly and
/// skip this trait and derive entirely.
///
/// Ordinary generated methods acquire their own connection. To share a
/// transaction across repositories, pass its connection or [`crate::PgTransaction`]
/// to the generated `*_in` methods. Those methods use the supplied
/// [`crate::PgExecutor`] directly, independently of this repository's selected
/// database, and leave transaction finalization to its owner.
pub trait PgRepository: Send + Sync {
    /// Ordinary Diesel entity owned by this repository adapter.
    type Entity: Send + Sync + 'static;

    /// Generated macro ABI for locating the repository's selected database.
    #[doc(hidden)]
    fn database_service(&self) -> PgResult<Arc<PgDatabaseService>>;

    /// Runs a custom Diesel query on the repository's selected database.
    ///
    /// The callback has the same scoped connection contract as
    /// [`PgDatabaseService::with_connection`]. This method is intended for
    /// application-defined repository methods that complement generated CRUD.
    fn with_connection<'repository, T, Operation>(
        &'repository self,
        operation: Operation,
        cancellation: Option<ExecutionCancellation>,
    ) -> Pin<Box<dyn Future<Output = PgResult<T>> + Send + 'repository>>
    where
        Self: Sized,
        T: Send + 'repository,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
                Option<ExecutionCancellation>,
            ) -> PgConnectionFuture<'connection, T>
            + Send
            + 'repository,
    {
        Box::pin(async move {
            self.database_service()?
                .with_connection(operation, cancellation)
                .await
        })
    }

    /// Runs a custom Diesel operation in a transaction on the selected database.
    ///
    /// Returning `Err` rolls back and returning `Ok` commits according to
    /// diesel-async's transaction contract. The optional cancellation view is
    /// forwarded to the callback with the same cleanup and finalization contract
    /// as [`PgDatabaseService::transaction`].
    /// Use generated `*_in` methods with the callback's connection to enlist
    /// repository operations; ordinary CRUD methods acquire another connection.
    fn transaction<'repository, T, Operation>(
        &'repository self,
        cancellation: Option<ExecutionCancellation>,
        operation: Operation,
    ) -> Pin<Box<dyn Future<Output = PgResult<T>> + Send + 'repository>>
    where
        Self: Sized,
        T: Send + 'repository,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
                Option<ExecutionCancellation>,
            ) -> PgConnectionFuture<'connection, T>
            + Send
            + 'repository,
    {
        Box::pin(async move {
            self.database_service()?
                .transaction(cancellation, operation)
                .await
        })
    }
}

/// Entity-only primary-key capability used by generated CRUD methods.
///
/// This trait belongs to the repository implementation, not to the entity:
/// model metadata still comes exclusively from Diesel.
#[doc(hidden)]
pub trait PgRepositoryId<Id>: PgRepository {
    /// Loads the entity on an already acquired connection using Diesel's
    /// primary-key contract.
    fn find_on_connection<'connection>(
        connection: &'connection mut AsyncPgConnection,
        id: Id,
    ) -> PgConnectionFuture<'connection, Option<Self::Entity>>
    where
        Id: 'connection;
}

impl<Repository, Id> PgRepositoryId<Id> for Repository
where
    Repository: PgRepository,
    Repository::Entity: HasTable + Selectable<Pg> + Send + 'static,
    <Repository::Entity as HasTable>::Table: diesel::query_dsl::methods::FindDsl<Id>,
    Find<<Repository::Entity as HasTable>::Table, Id>:
        diesel::query_dsl::methods::SelectDsl<AsSelect<Repository::Entity, Pg>>,
    Select<Find<<Repository::Entity as HasTable>::Table, Id>, AsSelect<Repository::Entity, Pg>>:
        for<'query> LoadQuery<'query, AsyncPgConnection, Repository::Entity> + Send,
    <Repository::Entity as Selectable<Pg>>::SelectExpression: QueryId,
    Id: Send,
{
    fn find_on_connection<'connection>(
        connection: &'connection mut AsyncPgConnection,
        id: Id,
    ) -> PgConnectionFuture<'connection, Option<Self::Entity>>
    where
        Id: 'connection,
    {
        Box::pin(find_entity_by_id::<Self::Entity, Id>(connection, id))
    }
}

/// Shared implementation detail for entity-only primary-key methods generated
/// by the proc macro. The bounds use only Diesel's public traits.
#[doc(hidden)]
pub async fn find_entity_by_id<Entity, Id>(
    connection: &mut AsyncPgConnection,
    id: Id,
) -> PgResult<Option<Entity>>
where
    Entity: HasTable + Selectable<Pg> + Send + 'static,
    Entity::Table: diesel::query_dsl::methods::FindDsl<Id>,
    Find<Entity::Table, Id>: diesel::query_dsl::methods::SelectDsl<AsSelect<Entity, Pg>>,
    Select<Find<Entity::Table, Id>, AsSelect<Entity, Pg>>:
        for<'query> LoadQuery<'query, AsyncPgConnection, Entity> + Send,
    <Entity as Selectable<Pg>>::SelectExpression: QueryId,
    Id: Send,
{
    diesel::query_dsl::methods::SelectDsl::select(
        diesel::query_dsl::methods::FindDsl::find(Entity::table(), id),
        Entity::as_select(),
    )
    .get_result(connection)
    .await
    .optional()
    .map_err(crate::PgError::from)
}
