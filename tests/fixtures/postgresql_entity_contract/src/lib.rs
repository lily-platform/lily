#![allow(dead_code)]

use diesel::associations::HasTable;
use diesel::dsl::{AsSelect, Find, Select};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::QueryId;
use diesel::result::QueryResult;
use diesel_async::AsyncConnection;
use diesel_async::methods::LoadQuery;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use lily_injectable_derive::Injectable;
use lily_injection::{InjectionError, ServiceTrait, async_trait::async_trait};
use lily_postgresql::PgRepository as _;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

#[cfg(feature = "single")]
use std::sync::Arc;
#[cfg(feature = "factory")]
use std::sync::{Arc, OnceLock};

diesel::table! {
    orders (id) {
        id -> Int8,
        customer_id -> Int8,
        status -> Text,
    }
}

diesel::table! {
    uuid_orders (id) {
        id -> Uuid,
        status -> Text,
    }
}

diesel::table! {
    tenant_orders (tenant_id, id) {
        tenant_id -> Int8,
        id -> Int8,
        status -> Text,
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Insertable, AsChangeset)]
#[diesel(table_name = orders)]
#[diesel(primary_key(id))]
#[diesel(check_for_backend(Pg))]
pub struct Order {
    #[diesel(skip_insertion)]
    #[diesel(skip_update)]
    pub id: i64,
    pub customer_id: i64,
    pub status: String,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Insertable, AsChangeset)]
#[diesel(table_name = uuid_orders)]
#[diesel(primary_key(id))]
#[diesel(check_for_backend(Pg))]
pub struct UuidOrder {
    #[diesel(skip_update)]
    pub id: Uuid,
    pub status: String,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Insertable, AsChangeset)]
#[diesel(table_name = tenant_orders)]
#[diesel(primary_key(tenant_id, id))]
#[diesel(check_for_backend(Pg))]
pub struct TenantOrder {
    #[diesel(skip_update)]
    pub tenant_id: i64,
    #[diesel(skip_update)]
    pub id: i64,
    pub status: String,
}

#[cfg(feature = "single")]
#[derive(Default, Injectable, lily_postgresql::PgRepository)]
#[service(lifetime = "Singleton")]
#[pg(entity = Order)]
pub struct OrderRepository {
    #[inject]
    database: Arc<lily_postgresql::PgDatabaseService>,
}

#[cfg(feature = "single")]
#[derive(lily_postgresql::PgRepository)]
#[pg(entity = UuidOrder)]
pub struct UuidOrderRepository {
    database: Arc<lily_postgresql::PgDatabaseService>,
}

#[cfg(feature = "single")]
#[derive(lily_postgresql::PgRepository)]
#[pg(entity = TenantOrder)]
pub struct TenantOrderRepository {
    database: Arc<lily_postgresql::PgDatabaseService>,
}

#[cfg(feature = "factory")]
#[derive(Default, Injectable, lily_postgresql::PgRepository)]
#[service(lifetime = "Singleton")]
#[pg(entity = Order)]
pub struct OrderRepository {
    #[inject]
    factory: Arc<lily_postgresql::PgFactory>,
    database: OnceLock<Arc<lily_postgresql::PgDatabaseService>>,
}

#[async_trait]
impl ServiceTrait for OrderRepository {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        #[cfg(feature = "factory")]
        {
            let database = self.factory.get("orders").map_err(|_| {
                InjectionError::InitError("orders PostgreSQL cell is unavailable".into())
            })?;
            self.database.set(database).map_err(|_| {
                InjectionError::InitError("orders repository is already initialized".into())
            })?;
        }
        Ok(())
    }
}

impl OrderRepository {
    pub async fn custom_find_status(&self, order_id: i64) -> lily_postgresql::PgResult<String> {
        self.with_connection(
            |connection, _| {
                Box::pin(async move {
                    let query = orders::table.find(order_id).select(orders::status);
                    diesel_async::RunQueryDsl::first(query, connection)
                        .await
                        .map_err(lily_postgresql::PgError::from)
                })
            },
            None,
        )
        .await
    }
}

#[cfg(feature = "single")]
pub async fn generated_repository_contracts(
    orders: &OrderRepository,
    uuid_orders: &UuidOrderRepository,
    tenant_orders: &TenantOrderRepository,
    order: Order,
    uuid_order: UuidOrder,
    tenant_order: TenantOrder,
) {
    let _ = orders.create(order.clone()).await;
    let _ = orders.create_many(vec![order.clone()]).await;
    let _ = orders.find_by_id(42_i64).await;
    let _ = orders.update(order).await;
    let _ = orders.delete_by_id(42_i64).await;
    let _ = orders.find_by_ids(vec![1_i64, 2_i64]).await;
    let _ = orders.count().await;
    let _ = orders.exists(42_i64).await;

    let uuid = uuid_order.id;
    let _ = uuid_orders.find_by_id(uuid).await;
    let _ = uuid_orders.exists(uuid).await;
    let _ = uuid_orders.create(uuid_order).await;

    let id = (tenant_order.tenant_id, tenant_order.id);
    let _ = tenant_orders.find_by_id(id).await;
    let _ = tenant_orders.find_by_ids(vec![id]).await;
    let _ = tenant_orders.exists(id).await;
    let _ = tenant_orders.create(tenant_order).await;
}

#[cfg(all(feature = "single", feature = "wrong-id"))]
pub async fn wrong_id_must_not_compile(repository: &OrderRepository) {
    let _ = repository.find_by_id("not-an-i64").await;
}

#[cfg(feature = "single")]
pub async fn transaction_primary_key_contracts(
    uuid_orders: &UuidOrderRepository,
    tenant_orders: &TenantOrderRepository,
    connection: &mut AsyncPgConnection,
    transaction: &lily_postgresql::PgTransaction,
    uuid: Uuid,
    composite: (i64, i64),
) {
    let _ = uuid_orders.find_by_id_in(&mut *connection, uuid).await;
    let _ = uuid_orders.find_by_ids_in(transaction, vec![uuid]).await;
    let _ = uuid_orders.exists_in(transaction, uuid).await;
    let _ = uuid_orders.delete_by_id_in(transaction, uuid).await;
    let _ = tenant_orders.find_by_id_in(transaction, composite).await;
    let _ = tenant_orders
        .find_by_ids_in(&mut *connection, vec![composite])
        .await;
    let _ = tenant_orders.exists_in(transaction, composite).await;
    let _ = tenant_orders.delete_by_id_in(transaction, composite).await;
}

pub async fn find_entity_by_id<Entity, Id>(
    connection: &mut AsyncPgConnection,
    id: Id,
) -> QueryResult<Option<Entity>>
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
}

pub async fn i64_primary_key(connection: &mut AsyncPgConnection) -> QueryResult<Option<Order>> {
    find_entity_by_id::<Order, _>(connection, 42_i64).await
}

pub async fn concrete_order_by_id<Id>(
    connection: &mut AsyncPgConnection,
    id: Id,
) -> QueryResult<Option<Order>>
where
    orders::table: diesel::query_dsl::methods::FindDsl<Id>,
    Find<orders::table, Id>: diesel::query_dsl::methods::SelectDsl<AsSelect<Order, Pg>>,
    Select<Find<orders::table, Id>, AsSelect<Order, Pg>>:
        for<'query> LoadQuery<'query, AsyncPgConnection, Order> + Send,
    <Order as Selectable<Pg>>::SelectExpression: QueryId,
    Id: Send,
{
    find_entity_by_id::<Order, Id>(connection, id).await
}

pub async fn uuid_primary_key(
    connection: &mut AsyncPgConnection,
    id: Uuid,
) -> QueryResult<Option<UuidOrder>> {
    find_entity_by_id::<UuidOrder, _>(connection, id).await
}

pub async fn composite_primary_key(
    connection: &mut AsyncPgConnection,
) -> QueryResult<Option<TenantOrder>> {
    find_entity_by_id::<TenantOrder, _>(connection, (7_i64, 42_i64)).await
}

pub async fn i64_primary_keys(
    connection: &mut AsyncPgConnection,
    ids: Vec<i64>,
) -> QueryResult<Vec<Order>> {
    orders::table
        .filter(orders::id.eq_any(ids))
        .select(Order::as_select())
        .load(connection)
        .await
}

pub async fn uuid_primary_keys(
    connection: &mut AsyncPgConnection,
    ids: Vec<Uuid>,
) -> QueryResult<Vec<UuidOrder>> {
    uuid_orders::table
        .filter(uuid_orders::id.eq_any(ids))
        .select(UuidOrder::as_select())
        .load(connection)
        .await
}

#[cfg(feature = "missing-insertable")]
mod missing_insertable_contract {
    use super::*;

    diesel::table! {
        missing_insertable_orders (id) {
            id -> Int8,
            status -> Text,
        }
    }

    #[derive(Queryable, Selectable, Identifiable, AsChangeset)]
    #[diesel(table_name = missing_insertable_orders)]
    struct Entity {
        #[diesel(skip_update)]
        id: i64,
        status: String,
    }

    #[derive(lily_postgresql::PgRepository)]
    #[pg(entity = Entity)]
    struct Repository {
        database: Arc<lily_postgresql::PgDatabaseService>,
    }
}

#[cfg(feature = "missing-changeset")]
mod missing_changeset_contract {
    use super::*;

    diesel::table! {
        missing_changeset_orders (id) {
            id -> Int8,
            status -> Text,
        }
    }

    #[derive(Queryable, Selectable, Identifiable, Insertable)]
    #[diesel(table_name = missing_changeset_orders)]
    struct Entity {
        id: i64,
        status: String,
    }

    #[derive(lily_postgresql::PgRepository)]
    #[pg(entity = Entity)]
    struct Repository {
        database: Arc<lily_postgresql::PgDatabaseService>,
    }
}

#[cfg(feature = "missing-identifiable")]
mod missing_identifiable_contract {
    use super::*;

    diesel::table! {
        missing_identifiable_orders (id) {
            id -> Int8,
            status -> Text,
        }
    }

    #[derive(Queryable, Selectable, Insertable, AsChangeset)]
    #[diesel(table_name = missing_identifiable_orders)]
    struct Entity {
        id: i64,
        status: String,
    }

    #[derive(lily_postgresql::PgRepository)]
    #[pg(entity = Entity)]
    struct Repository {
        database: Arc<lily_postgresql::PgDatabaseService>,
    }
}

#[cfg(feature = "missing-selectable")]
mod missing_selectable_contract {
    use super::*;

    diesel::table! {
        missing_selectable_orders (id) {
            id -> Int8,
            status -> Text,
        }
    }

    #[derive(Queryable, Identifiable, Insertable, AsChangeset)]
    #[diesel(table_name = missing_selectable_orders)]
    struct Entity {
        id: i64,
        status: String,
    }

    #[derive(lily_postgresql::PgRepository)]
    #[pg(entity = Entity)]
    struct Repository {
        database: Arc<lily_postgresql::PgDatabaseService>,
    }
}

pub async fn composite_primary_keys(
    connection: &mut AsyncPgConnection,
    ids: Vec<(i64, i64)>,
) -> QueryResult<Vec<TenantOrder>> {
    let mut entities = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(entity) = find_entity_by_id::<TenantOrder, _>(connection, id).await? {
            entities.push(entity);
        }
    }
    Ok(entities)
}

pub type TransactionFuture<'connection, T> =
    Pin<Box<dyn Future<Output = QueryResult<T>> + Send + 'connection>>;

pub async fn transaction_callback<T, Operation>(
    connection: &mut AsyncPgConnection,
    operation: Operation,
) -> QueryResult<T>
where
    T: Send,
    Operation: for<'connection> FnOnce(
            &'connection mut AsyncPgConnection,
        ) -> TransactionFuture<'connection, T>
        + Send,
{
    connection
        .transaction(async move |transaction| operation(transaction).await)
        .await
}
