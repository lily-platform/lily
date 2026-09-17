use std::sync::Arc;

use diesel::prelude::*;

use crate::{PgDatabaseService, PgError, PgQueryErrorKind, PgRepository, PgTransaction};

diesel::table! {
    repository_transaction_orders (id) {
        id -> BigInt,
        status -> Text,
    }
}

#[derive(Debug, Clone, PartialEq, Queryable, Selectable, Identifiable, Insertable, AsChangeset)]
#[diesel(table_name = repository_transaction_orders)]
struct Order {
    #[diesel(skip_update)]
    id: i64,
    status: String,
}

#[derive(PgRepository)]
#[pg(entity = Order)]
struct OrderRepository {
    database: Arc<PgDatabaseService>,
}

#[derive(PgRepository)]
#[pg(entity = Order)]
struct OtherOrderRepository {
    database: Arc<PgDatabaseService>,
}

fn order(id: i64) -> Order {
    Order {
        id,
        status: "created".into(),
    }
}

#[tokio::test]
async fn every_generated_operation_uses_the_supplied_transaction_without_pool_fallback() {
    let repository = OrderRepository {
        database: Arc::new(PgDatabaseService::default()),
    };
    let (transaction, receiver) = PgTransaction::channel();
    drop(receiver);

    // A pool fallback would return NotInitialized. Every nonempty operation
    // must instead preserve the supplied transaction's admission error.
    assert_eq!(
        repository.create_in(&transaction, order(1)).await,
        Err(PgError::TransactionClosed)
    );
    assert_eq!(
        repository
            .create_many_in(&transaction, vec![order(1)])
            .await,
        Err(PgError::TransactionClosed)
    );
    assert_eq!(
        repository.find_by_id_in(&transaction, 1_i64).await,
        Err(PgError::TransactionClosed)
    );
    assert_eq!(
        repository.find_by_ids_in(&transaction, vec![1_i64]).await,
        Err(PgError::TransactionClosed)
    );
    assert_eq!(
        repository.update_in(&transaction, order(1)).await,
        Err(PgError::TransactionClosed)
    );
    assert_eq!(
        repository.delete_by_id_in(&transaction, 1_i64).await,
        Err(PgError::TransactionClosed)
    );
    assert_eq!(
        repository.count_in(&transaction).await,
        Err(PgError::TransactionClosed)
    );
    assert_eq!(
        repository.exists_in(&transaction, 1_i64).await,
        Err(PgError::TransactionClosed)
    );

    assert_eq!(
        repository.create_many_in(&transaction, vec![]).await,
        Ok(vec![])
    );
    assert_eq!(
        repository
            .find_by_ids_in(&transaction, Vec::<i64>::new())
            .await,
        Ok(vec![])
    );
    assert_eq!(repository.create_many(vec![]).await, Ok(vec![]));
    assert_eq!(repository.find_by_ids(Vec::<i64>::new()).await, Ok(vec![]));
    assert_eq!(repository.count().await, Err(PgError::NotInitialized));
}

/// Uses only a connection-local temporary table. The URL must identify a
/// dedicated test database; the fixture intentionally uses plaintext transport.
#[cfg(any(feature = "single", feature = "factory"))]
#[tokio::test]
#[ignore = "requires LILY_PG_TRANSACTION_TEST_URL pointing to a dedicated PostgreSQL database"]
async fn generated_crud_shares_commit_and_rollback_with_a_single_connection_pool() {
    use crate::{PgPoolConfig, PgTlsConfig, PgTlsMode};

    let connection_string = std::env::var("LILY_PG_TRANSACTION_TEST_URL")
        .expect("set LILY_PG_TRANSACTION_TEST_URL for this dedicated database test");
    let pool = PgPoolConfig {
        max_size: 1,
        acquire_timeout_secs: 1,
        ..Default::default()
    };
    let tls = PgTlsConfig {
        mode: PgTlsMode::Disable,
        ..Default::default()
    };
    #[cfg(feature = "single")]
    let plan = super::PgConnectionPlan::from_single(&crate::PgConfig {
        connection_string: Some(connection_string),
        pool,
        tls,
        ..Default::default()
    })
    .unwrap();
    #[cfg(feature = "factory")]
    let plan = super::PgConnectionPlan::from_cell(&crate::PgCellConfig {
        name: "repository-transactions".into(),
        connection_string,
        pool,
        tls,
    })
    .unwrap();
    let database = Arc::new(PgDatabaseService::default());
    database.install_ready(plan).await.unwrap();
    database.with_connection(|connection, _| Box::pin(async move {
        diesel_async::RunQueryDsl::execute(
            diesel::sql_query("CREATE TEMP TABLE repository_transaction_orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)"),
            connection,
        ).await.map(|_| ()).map_err(PgError::from)
    }), None).await.unwrap();

    let repository = Arc::new(OrderRepository {
        database: Arc::clone(&database),
    });
    let other = Arc::new(OtherOrderRepository {
        database: Arc::clone(&database),
    });
    let first = Arc::clone(&repository);
    let second = Arc::clone(&other);
    // All eight operations run while the only pool connection is checked out.
    // Reacquiring through either repository would time out.
    database
        .transaction(None, move |connection, cancellation| {
            Box::pin(async move {
                assert!(cancellation.is_none());
                assert_eq!(first.create_in(&mut *connection, order(1)).await?, order(1));
                assert_eq!(
                    second
                        .create_many_in(&mut *connection, vec![order(2), order(3)])
                        .await?
                        .len(),
                    2
                );
                assert_eq!(
                    first.find_by_id_in(&mut *connection, 1_i64).await?,
                    Some(order(1))
                );
                assert_eq!(
                    first
                        .find_by_ids_in(&mut *connection, vec![2_i64, 99, 1, 2])
                        .await?,
                    vec![order(2), order(1), order(2)]
                );
                let changed = Order {
                    id: 1,
                    status: "updated".into(),
                };
                assert_eq!(
                    first.update_in(&mut *connection, changed.clone()).await?,
                    changed
                );
                assert_eq!(first.count_in(&mut *connection).await?, 3);
                assert!(second.delete_by_id_in(&mut *connection, 3_i64).await?);
                assert!(!second.delete_by_id_in(&mut *connection, 99_i64).await?);
                assert!(!first.exists_in(&mut *connection, 3_i64).await?);
                assert!(first.exists_in(&mut *connection, 1_i64).await?);
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(repository.count().await, Ok(2));
    assert_eq!(
        repository.find_by_id(1_i64).await.unwrap().unwrap().status,
        "updated"
    );

    let first = Arc::clone(&repository);
    let second = Arc::clone(&other);
    let rollback = database
        .transaction(None, move |connection, _| {
            Box::pin(async move {
                first.create_in(&mut *connection, order(4)).await?;
                // A constraint failure in another repository must undo the first insert.
                second.create_in(&mut *connection, order(1)).await?;
                Ok(())
            })
        })
        .await;
    let constraint = PgError::Query {
        kind: PgQueryErrorKind::Conflict,
    };
    assert_eq!(rollback, Err(constraint.clone()));
    assert!(!repository.exists(4_i64).await.unwrap());

    let first = Arc::clone(&repository);
    let second = Arc::clone(&other);
    let rollback = database
        .transaction_with_handle(move |transaction| async move {
            first.create_in(&transaction, order(5)).await?;
            second.create_in(&transaction, order(1)).await?;
            Ok(())
        })
        .await;
    assert_eq!(rollback, Err(constraint));
    assert!(!repository.exists(5_i64).await.unwrap());

    let first = Arc::clone(&repository);
    let second = Arc::clone(&other);
    let finished = database
        .transaction_with_handle(move |transaction| async move {
            first.create_in(&transaction, order(6)).await?;
            second.create_in(&transaction, order(7)).await?;
            Ok(transaction)
        })
        .await
        .unwrap();
    assert_eq!(repository.count().await, Ok(4));
    assert_eq!(
        repository.count_in(&finished).await,
        Err(PgError::TransactionClosed)
    );
    assert_eq!(database.status().unwrap().size, 1);
    database.close().await.unwrap();
}
