use std::sync::Arc;

#[cfg(feature = "factory")]
use std::sync::OnceLock;

use diesel::prelude::*;
use lily_postgresql::{PgDatabaseService, PgRepository, PgTransaction};

#[cfg(feature = "factory")]
use lily_postgresql::PgFactory;

diesel::table! {
    derive_contract_orders (id) {
        id -> BigInt,
        status -> Text,
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Insertable, AsChangeset)]
#[diesel(table_name = derive_contract_orders)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct Order {
    #[diesel(skip_insertion)]
    #[diesel(skip_update)]
    id: i64,
    status: String,
}

#[cfg(feature = "single")]
#[derive(PgRepository)]
#[pg(entity = Order)]
struct OrderRepository {
    database: Arc<PgDatabaseService>,
}

#[cfg(feature = "factory")]
#[derive(PgRepository)]
#[pg(entity = Order)]
struct OrderRepository {
    factory: Arc<PgFactory>,
    database: OnceLock<Arc<PgDatabaseService>>,
}

fn assert_generated_methods(repository: &OrderRepository) {
    let _ = repository.create(Order {
        id: 0,
        status: String::new(),
    });
    let _ = repository.find_by_id(1_i64);
    let _ = repository.count();
}

async fn assert_transaction_methods(
    repository: &OrderRepository,
    connection: &mut lily_postgresql::diesel_async::AsyncPgConnection,
    transaction: &PgTransaction,
) -> lily_postgresql::PgResult<()> {
    let order = Order {
        id: 1,
        status: "created".into(),
    };
    repository
        .create_in(&mut *connection, order.clone())
        .await?;
    repository
        .create_many_in(&mut *connection, vec![order.clone()])
        .await?;
    repository.find_by_id_in(&mut *connection, 1_i64).await?;
    repository
        .find_by_ids_in(&mut *connection, vec![1_i64])
        .await?;
    repository
        .update_in(&mut *connection, order.clone())
        .await?;
    repository.delete_by_id_in(&mut *connection, 1_i64).await?;
    repository.count_in(&mut *connection).await?;
    repository.exists_in(&mut *connection, 1_i64).await?;

    repository.create_in(transaction, order.clone()).await?;
    repository
        .create_many_in(transaction, vec![order.clone()])
        .await?;
    repository.find_by_id_in(transaction, 1_i64).await?;
    repository.find_by_ids_in(transaction, vec![1_i64]).await?;
    repository.update_in(transaction, order).await?;
    repository.delete_by_id_in(transaction, 1_i64).await?;
    repository.count_in(transaction).await?;
    repository.exists_in(transaction, 1_i64).await?;
    Ok(())
}

fn assert_send<T: Send>(_: T) {}

fn assert_transaction_callback(database: &PgDatabaseService, repository: Arc<OrderRepository>) {
    assert_send(database.transaction(None, move |connection, cancellation| {
        Box::pin(async move {
            let _: Option<lily_postgresql::ExecutionCancellation> = cancellation;
            repository
                .create_in(
                    &mut *connection,
                    Order {
                        id: 1,
                        status: "created".into(),
                    },
                )
                .await?;
            repository.count_in(&mut *connection).await
        })
    }));
}

fn main() {}

fn assert_optional_cancellation(
    database: &PgDatabaseService,
    repository: &OrderRepository,
    cancellation: lily_postgresql::ExecutionCancellation,
) {
    assert_send(database.transaction(Some(cancellation.clone()), |_, cancellation| {
        Box::pin(async move { Ok(cancellation.unwrap().is_cancelled()) })
    }));
    assert_send(lily_postgresql::PgRepository::transaction(
        repository,
        Some(cancellation),
        |_, cancellation| Box::pin(async move { Ok(cancellation.unwrap().is_cancelled()) }),
    ));
}
