use lily_cancellation::__private::ExecutionCancellationSource;

use super::*;

#[tokio::test]
async fn precancelled_transaction_does_not_enter_the_database_or_invoke_the_callback() {
    let database = PgDatabaseService::default();
    let source = ExecutionCancellationSource::default();
    source.cancel();
    let invoked = AtomicBool::new(false);
    let result = database
        .transaction(Some(source.view()), |_, _| {
            invoked.store(true, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        })
        .await;
    assert_eq!(result, Err(PgError::TransactionCancelled));
    assert!(!AtomicBool::load(&invoked, Ordering::Relaxed));

    for cancellation in [None, Some(ExecutionCancellationSource::default().view())] {
        assert_eq!(
            database
                .transaction(cancellation, |_, _| Box::pin(async { Ok(()) }))
                .await,
            Err(PgError::NotInitialized)
        );
    }
}

#[cfg(any(feature = "single", feature = "factory"))]
mod live {
    use diesel::{
        QueryableByName,
        sql_types::{BigInt, Bool, Integer},
    };
    use diesel_async::RunQueryDsl;

    use super::*;
    use crate::{PgPoolConfig, PgQueryErrorKind, PgTlsConfig, PgTlsMode};

    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        count: i64,
    }

    #[derive(QueryableByName)]
    struct Pid {
        #[diesel(sql_type = Integer)]
        pid: i32,
    }

    #[derive(QueryableByName)]
    struct Active {
        #[diesel(sql_type = Bool)]
        active: bool,
    }

    struct Fixture {
        database: PgDatabaseService,
        observer: PgDatabaseService,
        schema: String,
    }

    impl Fixture {
        async fn new() -> Self {
            let connection_string = std::env::var("LILY_PG_TRANSACTION_TEST_URL")
                .expect("set LILY_PG_TRANSACTION_TEST_URL to a dedicated test database");
            let ca = std::env::var_os("LILY_PG_TRANSACTION_TEST_CA").map(Into::into);
            let tls = PgTlsConfig {
                mode: if ca.is_some() {
                    PgTlsMode::VerifyFull
                } else {
                    PgTlsMode::Disable
                },
                additional_ca_bundle: ca,
            };
            let pool = PgPoolConfig {
                max_size: 1,
                transaction_cleanup_timeout_secs: 1,
                ..Default::default()
            };
            #[cfg(feature = "single")]
            let plan = PgConnectionPlan::from_single(&crate::PgConfig {
                connection_string: Some(connection_string),
                pool,
                tls,
                ..Default::default()
            })
            .unwrap();
            #[cfg(feature = "factory")]
            let plan = PgConnectionPlan::from_cell(&crate::PgCellConfig {
                name: "cancellation-test".into(),
                connection_string,
                pool,
                tls,
            })
            .unwrap();
            let database = PgDatabaseService::default();
            let observer = PgDatabaseService::default();
            database.install_ready(plan.clone()).await.unwrap();
            observer.install_ready(plan).await.unwrap();
            let schema = format!("lily_cancellation_test_{}", backend_pid(&observer).await);
            execute(&observer, format!("CREATE SCHEMA {schema}")).await;
            execute(
                &observer,
                format!("CREATE TABLE {schema}.orders (id BIGINT PRIMARY KEY)"),
            )
            .await;
            Self {
                database,
                observer,
                schema,
            }
        }

        fn insert(&self, id: i64) -> String {
            format!("INSERT INTO {}.orders VALUES ({id})", self.schema)
        }

        async fn count(&self) -> i64 {
            let sql = format!("SELECT count(*) AS count FROM {}.orders", self.schema);
            self.observer
                .with_connection(
                    |connection, _| {
                        Box::pin(async move {
                            Ok(diesel::sql_query(sql)
                                .get_result::<Count>(connection)
                                .await?
                                .count)
                        })
                    },
                    None,
                )
                .await
                .unwrap()
        }

        async fn finish(self) {
            execute(
                &self.observer,
                format!("DROP SCHEMA {} CASCADE", self.schema),
            )
            .await;
            self.database.close().await.unwrap();
            self.observer.close().await.unwrap();
        }
    }

    async fn execute(database: &PgDatabaseService, sql: String) {
        database
            .with_connection(
                |connection, _| {
                    Box::pin(async move {
                        diesel::sql_query(sql)
                            .execute(connection)
                            .await
                            .map_err(PgError::from)
                    })
                },
                None,
            )
            .await
            .unwrap();
    }

    async fn backend_pid(database: &PgDatabaseService) -> i32 {
        database
            .with_connection(
                |connection, _| {
                    Box::pin(async move {
                        Ok(diesel::sql_query("SELECT pg_backend_pid() AS pid")
                            .get_result::<Pid>(connection)
                            .await?
                            .pid)
                    })
                },
                None,
            )
            .await
            .unwrap()
    }

    async fn wait_for_sleep(observer: &PgDatabaseService, pid: i32, query: &'static str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let active = observer.with_connection(|connection, _| Box::pin(async move {
                    Ok(diesel::sql_query("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active' AND wait_event = 'PgSleep' AND query = $2) AS active")
                        .bind::<Integer, _>(pid)
                        .bind::<diesel::sql_types::Text, _>(query)
                        .get_result::<Active>(connection).await?.active)
                }), None).await.unwrap();
                if active { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.expect("backend did not reach the intended cancellation boundary");
    }

    #[tokio::test]
    #[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL; optional LILY_PG_TRANSACTION_TEST_CA enables TLS"]
    async fn optional_cancellation_preserves_results_and_rolls_back_application_and_sql_work() {
        let fixture = Fixture::new().await;
        let database = &fixture.database;
        let source = ExecutionCancellationSource::default();
        let result = database
            .transaction(Some(source.view()), |_, cancellation| {
                Box::pin(async move {
                    assert!(!cancellation.unwrap().is_cancelled());
                    Ok(42)
                })
            })
            .await;
        assert_eq!(result, Ok(42));

        // An ordinary error is preserved after rollback with an active token.
        let sql = fixture.insert(1);
        let result = database
            .transaction(Some(source.view()), |connection, _| {
                Box::pin(async move {
                    diesel::sql_query(sql).execute(connection).await?;
                    Err::<(), _>(PgError::RepositoryDatabaseNotSet)
                })
            })
            .await;
        assert_eq!(result, Err(PgError::RepositoryDatabaseNotSet));
        assert_eq!(fixture.count().await, 0);

        // Cancellation stops application work even if it never checks its view.
        let (started, ready) = tokio::sync::oneshot::channel();
        let sql = fixture.insert(2);
        let transaction = database.transaction(Some(source.view()), |connection, cancellation| {
            Box::pin(async move {
                diesel::sql_query(sql).execute(connection).await?;
                started.send(cancellation.unwrap()).unwrap();
                std::future::pending::<PgResult<()>>().await
            })
        });
        let cancel = async {
            let view = ready.await.unwrap();
            source.cancel();
            assert!(view.is_cancelled());
        };
        let (result, ()) = tokio::join!(transaction, cancel);
        assert_eq!(result, Err(PgError::TransactionCancelled));
        assert_eq!(database.status().unwrap().in_flight, 0);
        assert_eq!(database.status().unwrap().size, 0);
        assert_eq!(fixture.count().await, 0);

        // A server-side sleep must be cancelled, not merely its Rust waiter.
        let source = ExecutionCancellationSource::default();
        let pid = backend_pid(database).await;
        let sql = fixture.insert(3);
        let transaction = database.transaction(Some(source.view()), |connection, _| {
            Box::pin(async move {
                diesel::sql_query(sql).execute(connection).await?;
                diesel::sql_query("SELECT pg_sleep(30)")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        });
        let cancel = async {
            wait_for_sleep(&fixture.observer, pid, "SELECT pg_sleep(30)").await;
            source.cancel();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(7), async {
            tokio::join!(transaction, cancel)
        })
        .await
        .expect("query cancellation must not wait for the 30-second statement");
        assert_eq!(result, Err(PgError::TransactionCancelled));
        assert_eq!(fixture.count().await, 0);
        assert_eq!(database.status().unwrap().size, 0);
        assert_ne!(backend_pid(database).await, pid);

        // Cancellation and Ok in the same callback poll must select rollback.
        let source = Arc::new(ExecutionCancellationSource::default());
        let cancellation = source.view();
        let sql = fixture.insert(4);
        let result = database
            .transaction(Some(cancellation), |connection, _| {
                Box::pin(async move {
                    diesel::sql_query(sql).execute(connection).await?;
                    source.cancel();
                    Ok(99)
                })
            })
            .await;
        assert_eq!(result, Err(PgError::TransactionCancelled));
        assert_eq!(fixture.count().await, 0);
        fixture.finish().await;
    }

    #[tokio::test]
    #[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL; optional LILY_PG_TRANSACTION_TEST_CA enables TLS"]
    async fn cancellation_while_waiting_for_the_only_connection_does_not_invoke_operation() {
        let fixture = Fixture::new().await;
        let runtime = fixture.database.runtime().unwrap();
        let held = runtime.pool.get().await.unwrap();
        let source = ExecutionCancellationSource::default();
        let invoked = AtomicBool::new(false);
        let transaction = fixture.database.transaction(Some(source.view()), |_, _| {
            invoked.store(true, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        });
        let cancel = async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while fixture.database.status().unwrap().waiting == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            source.cancel();
        };
        let (result, ()) = tokio::join!(transaction, cancel);
        assert_eq!(result, Err(PgError::TransactionCancelled));
        assert!(!AtomicBool::load(&invoked, Ordering::Relaxed));
        assert_eq!(fixture.database.status().unwrap().in_flight, 0);
        drop(held);
        assert_eq!(fixture.database.status().unwrap().size, 1);
        fixture.finish().await;
    }

    #[tokio::test]
    #[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL; optional LILY_PG_TRANSACTION_TEST_CA enables TLS"]
    async fn cancellation_during_commit_preserves_the_actual_commit_result() {
        let fixture = Fixture::new().await;
        let schema = &fixture.schema;
        execute(&fixture.observer, format!("CREATE FUNCTION {schema}.delay_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(3); RETURN NEW; END $$")).await;
        execute(&fixture.observer, format!("CREATE CONSTRAINT TRIGGER delay_commit AFTER INSERT ON {schema}.orders DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {schema}.delay_commit()")).await;
        let pid = backend_pid(&fixture.database).await;
        let source = ExecutionCancellationSource::default();
        let sql = fixture.insert(1);
        let transaction = fixture
            .database
            .transaction(Some(source.view()), |connection, _| {
                Box::pin(async move {
                    diesel::sql_query(sql).execute(connection).await?;
                    Ok(17)
                })
            });
        let cancel = async {
            wait_for_sleep(&fixture.observer, pid, "COMMIT").await;
            source.cancel();
        };
        let (result, ()) = tokio::join!(transaction, cancel);
        // COMMIT outlives the one-second cleanup budget and still succeeds.
        assert!(source.view().is_cancelled());
        assert_eq!(result, Ok(17));
        assert_eq!(fixture.count().await, 1);
        assert_eq!(backend_pid(&fixture.database).await, pid);
        fixture.finish().await;
    }

    #[tokio::test]
    #[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL; optional LILY_PG_TRANSACTION_TEST_CA enables TLS"]
    async fn rollback_failure_is_not_reported_as_successful_cancellation_cleanup() {
        let fixture = Fixture::new().await;
        let pid = backend_pid(&fixture.database).await;
        let source = ExecutionCancellationSource::default();
        let (started, ready) = tokio::sync::oneshot::channel();
        let transaction = fixture.database.transaction(Some(source.view()), |_, _| {
            Box::pin(async move {
                started.send(()).unwrap();
                std::future::pending::<PgResult<()>>().await
            })
        });
        let cancel = async {
            ready.await.unwrap();
            execute(
                &fixture.observer,
                format!("SELECT pg_terminate_backend({pid})"),
            )
            .await;
            source.cancel();
        };
        let (result, ()) = tokio::join!(transaction, cancel);
        assert!(
            matches!(
                result,
                Err(PgError::Query {
                    kind: PgQueryErrorKind::Database | PgQueryErrorKind::Transaction
                })
            ),
            "{result:?}"
        );
        assert_eq!(fixture.database.status().unwrap().size, 0);
        assert_ne!(backend_pid(&fixture.database).await, pid);
        fixture.finish().await;
    }

    #[tokio::test]
    #[ignore = "requires dedicated TLS PostgreSQL with LILY_PG_TRANSACTION_TEST_URL and LILY_PG_TRANSACTION_TEST_CA"]
    async fn cleanup_timeout_discards_connection_when_cancel_transport_fails() {
        let mut fixture = Fixture::new().await;
        let pid = backend_pid(&fixture.database).await;
        // Fault injection: the established connection and its pool manager still
        // use TLS, but cancel requests cannot negotiate it. This exercises failed
        // server cancellation followed by rollback blocked behind a running query.
        let runtime = Arc::get_mut(
            Arc::get_mut(&mut fixture.database.state)
                .unwrap()
                .runtime
                .get_mut()
                .unwrap(),
        )
        .unwrap();
        assert!(
            runtime.cancellation_tls.take().is_some(),
            "this fault test requires TLS"
        );

        let source = ExecutionCancellationSource::default();
        let sql = fixture.insert(1);
        let transaction = fixture
            .database
            .transaction(Some(source.view()), |connection, _| {
                Box::pin(async move {
                    diesel::sql_query(sql).execute(connection).await?;
                    diesel::sql_query("SELECT pg_sleep(30)")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            });
        let cancel = async {
            wait_for_sleep(&fixture.observer, pid, "SELECT pg_sleep(30)").await;
            source.cancel();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(7), async {
            tokio::join!(transaction, cancel)
        })
        .await
        .expect("cleanup must be bounded even when server cancellation fails");
        assert_eq!(result, Err(PgError::TransactionCleanupTimeout));
        assert_eq!(fixture.database.status().unwrap().size, 0);
        assert_eq!(fixture.database.status().unwrap().in_flight, 0);
        assert_ne!(backend_pid(&fixture.database).await, pid);
        // A cleanup timeout deliberately makes no claim about server completion.
        // Terminate this test backend so fixture DDL never waits for its sleep.
        execute(
            &fixture.observer,
            format!("SELECT pg_terminate_backend({pid})"),
        )
        .await;
        assert_eq!(fixture.count().await, 0);
        fixture.finish().await;
    }
}
