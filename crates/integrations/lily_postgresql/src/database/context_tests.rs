use std::sync::Arc;
use std::time::Duration;

use diesel::{
    QueryableByName,
    sql_types::{BigInt, Bool, Integer},
};
use diesel_async::RunQueryDsl;
use lily_cancellation::__private::ExecutionCancellationSource;
use lily_error::injection::InjectionError;
use lily_injection::ServiceTrait;
use tokio::sync::oneshot;

use super::{PgConnectionPlan, PgDatabaseService};
use crate::{
    ExecutionCancellation, PgDbContext, PgError, PgPoolConfig, PgQueryErrorKind, PgResult,
    PgTlsConfig, PgTlsMode,
};

mod application_errors;
#[cfg(all(feature = "single", feature = "test-support"))]
mod production;

async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("qualification phase did not finish within ten seconds")
}

fn assert_dispose_error(result: Result<(), InjectionError>, expected: PgError) {
    let Err(InjectionError::DisposeError(message)) = result else {
        panic!("expected DisposeError({expected}), got {result:?}");
    };
    assert_eq!(message, expected.to_string());
}

#[derive(QueryableByName)]
struct Pid {
    #[diesel(sql_type = Integer)]
    pid: i32,
}
#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    count: i64,
}
#[derive(QueryableByName)]
struct Active {
    #[diesel(sql_type = Bool)]
    active: bool,
}

#[derive(Debug, PartialEq, Eq, QueryableByName)]
struct RowId {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, QueryableByName)]
struct Identity {
    #[diesel(sql_type = Integer)]
    pid: i32,
    #[diesel(sql_type = BigInt)]
    xid: i64,
}

async fn identity(connection: &mut diesel_async::AsyncPgConnection) -> PgResult<Identity> {
    diesel::sql_query("SELECT pg_backend_pid() AS pid, txid_current() AS xid")
        .get_result(connection)
        .await
        .map_err(Into::into)
}

struct Fixture {
    database: Arc<PgDatabaseService>,
    observer: PgDatabaseService,
    context: Arc<PgDbContext>,
    schema: String,
    observer_pid: i32,
}

impl Fixture {
    async fn new() -> Self {
        let connection_string = std::env::var("LILY_PG_TRANSACTION_TEST_URL")
            .expect("set LILY_PG_TRANSACTION_TEST_URL to a dedicated test database");
        let ca = std::env::var_os("LILY_PG_TRANSACTION_TEST_CA").map(Into::into);
        let expected_tls = ca.is_some();
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
            acquire_timeout_secs: 1,
            transaction_cleanup_timeout_secs: 1,
            shutdown_timeout_secs: 1,
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
            name: "context-test".into(),
            connection_string,
            pool,
            tls,
        })
        .unwrap();
        let database = Arc::new(PgDatabaseService::default());
        let observer = PgDatabaseService::default();
        database.install_ready(plan.clone()).await.unwrap();
        observer.install_ready(plan).await.unwrap();
        for service in [database.as_ref(), &observer] {
            let encrypted = within(service.with_connection(
                |connection, _| {
                    Box::pin(async move {
                        Ok(diesel::sql_query(
                            "SELECT ssl AS active FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
                        )
                        .get_result::<Active>(connection)
                        .await?
                        .active)
                    })
                },
                None,
            ))
            .await
            .unwrap();
            assert_eq!(encrypted, expected_tls, "unexpected PostgreSQL transport");
        }
        let pid = observer
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
            .unwrap();
        let context = Arc::new(PgDbContext::from(Arc::clone(&database)));
        let fixture = Self {
            database,
            observer,
            context,
            schema: format!("lily_context_test_{pid}"),
            observer_pid: pid,
        };
        fixture
            .run_sql(format!("CREATE SCHEMA {}", fixture.schema))
            .await;
        fixture
            .run_sql(format!(
                "CREATE TABLE {}.orders (id BIGINT PRIMARY KEY)",
                fixture.schema
            ))
            .await;
        fixture
    }

    fn repository(&self) -> Repository {
        Repository {
            context: Arc::clone(&self.context),
            schema: self.schema.clone(),
        }
    }

    async fn run_sql(&self, sql: String) {
        self.observer
            .with_connection(
                |connection, _| {
                    Box::pin(async move {
                        diesel::sql_query(sql).execute(connection).await?;
                        Ok(())
                    })
                },
                None,
            )
            .await
            .unwrap();
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

    async fn ids(&self) -> Vec<i64> {
        let sql = format!("SELECT id FROM {}.orders ORDER BY id", self.schema);
        within(self.observer.with_connection(
            |connection, _| {
                Box::pin(async move {
                    Ok(diesel::sql_query(sql)
                        .get_results::<RowId>(connection)
                        .await?
                        .into_iter()
                        .map(|row| row.id)
                        .collect())
                })
            },
            None,
        ))
        .await
        .unwrap()
    }

    async fn lock_gate(&self) {
        self.run_sql(format!("SELECT pg_advisory_lock({}, 1)", self.observer_pid))
            .await;
    }

    async fn unlock_gate(&self) {
        let namespace = self.observer_pid;
        let released = within(self.observer.with_connection(
            |connection, _| {
                Box::pin(async move {
                    Ok(
                        diesel::sql_query("SELECT pg_advisory_unlock($1, 1) AS active")
                            .bind::<Integer, _>(namespace)
                            .get_result::<Active>(connection)
                            .await?
                            .active,
                    )
                })
            },
            None,
        ))
        .await
        .unwrap();
        assert!(
            released,
            "the observer must own exactly the gate being released"
        );
    }

    #[cfg(all(feature = "single", feature = "test-support"))]
    fn gate_query(&self) -> String {
        format!("SELECT pg_advisory_xact_lock({}, 1)", self.observer_pid)
    }

    async fn gate_commit(&self) {
        self.lock_gate().await;
        self.run_sql(format!("CREATE FUNCTION {}.gated_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({}, 1); RETURN NEW; END $$", self.schema, self.observer_pid)).await;
        self.run_sql(format!("CREATE CONSTRAINT TRIGGER gated_commit AFTER INSERT ON {}.orders DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {}.gated_commit()", self.schema, self.schema)).await;
    }

    async fn wait_gate(&self, pid: i32) {
        within(async {
            loop {
                let blocked = self.observer.with_connection(|connection, _| Box::pin(async move {
                    Ok(diesel::sql_query("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active' AND wait_event_type = 'Lock' AND wait_event = 'advisory') AS active")
                        .bind::<Integer, _>(pid).get_result::<Active>(connection).await?.active)
                }), None).await.unwrap();
                if blocked { break; }
                tokio::task::yield_now().await;
            }
        }).await;
    }

    #[cfg(all(feature = "single", feature = "test-support"))]
    async fn wait_pool(&self, in_flight: usize, waiting: usize) {
        within(async {
            loop {
                let status = self.database.status().unwrap();
                assert_eq!(status.max_size, 1);
                assert!(status.size <= 1);
                if status.in_flight == in_flight && status.waiting == waiting {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
    }

    async fn wait_query(&self, pid: i32, query: &'static str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let active = self.observer.with_connection(|connection, _| Box::pin(async move {
                    Ok(diesel::sql_query("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active' AND query = $2) AS active")
                        .bind::<Integer, _>(pid).bind::<diesel::sql_types::Text, _>(query)
                        .get_result::<Active>(connection).await?.active)
                }), None).await.unwrap();
                if active { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.expect("backend did not reach the intended boundary");
    }

    async fn idle(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.database.status().unwrap().in_flight != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned connection did not drain");
    }

    async fn finish(self) {
        self.idle().await;
        self.run_sql("SELECT pg_advisory_unlock_all()".into()).await;
        self.run_sql(format!("DROP SCHEMA {} CASCADE", self.schema))
            .await;
        self.database.close().await.unwrap();
        self.observer.close().await.unwrap();
    }
}

struct Repository {
    context: Arc<PgDbContext>,
    schema: String,
}

impl Repository {
    async fn insert(&self, id: i64) -> PgResult<i32> {
        let sql = format!("INSERT INTO {}.orders VALUES ($1)", self.schema);
        self.context
            .with_connection(
                |connection, _| {
                    Box::pin(async move {
                        diesel::sql_query(sql)
                            .bind::<BigInt, _>(id)
                            .execute(connection)
                            .await?;
                        Ok(diesel::sql_query("SELECT pg_backend_pid() AS pid")
                            .get_result::<Pid>(connection)
                            .await?
                            .pid)
                    })
                },
                None,
            )
            .await
    }
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL; optional LILY_PG_TRANSACTION_TEST_CA enables TLS"]
async fn lease_owns_accounting_and_cancelled_waiters_release_reservations() {
    let fixture = Fixture::new().await;
    let mut lease = fixture.database.acquire_connection(None).await.unwrap();
    assert_eq!(fixture.database.status().unwrap().in_flight, 1);
    assert_eq!(fixture.database.status().unwrap().available, 0);
    let source = ExecutionCancellationSource::default();
    {
        let waiting = fixture
            .database
            .with_connection(|_, _| panic!("pool is occupied"), Some(source.view()));
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
        assert_eq!(fixture.database.status().unwrap().in_flight, 2);
        source.cancel();
        assert_eq!(waiting.await, Err::<(), _>(PgError::OperationCancelled));
    }
    assert_eq!(fixture.database.status().unwrap().in_flight, 1);
    {
        let pending = lease.with_connection(
            |_, _| Box::pin(std::future::pending::<PgResult<()>>()),
            None,
        );
        tokio::pin!(pending);
        assert!(futures_util::poll!(&mut pending).is_pending());
    }
    assert_eq!(
        lease
            .with_connection(|_, _| panic!("interrupted lease"), None)
            .await,
        Err::<(), _>(PgError::ConnectionInterrupted)
    );
    drop(lease);
    assert_eq!(fixture.database.status().unwrap().size, 0);
    assert_eq!(fixture.database.status().unwrap().in_flight, 0);
    let lease = fixture.database.acquire_connection(None).await.unwrap();
    assert!(matches!(
        fixture.database.close().await,
        Err(PgError::ShutdownTimeout { remaining: 1 })
    ));
    drop(lease);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn repositories_share_one_transaction_and_errors_cannot_be_swallowed_into_commit() {
    let fixture = Fixture::new().await;
    let expected_pid = fixture
        .database
        .with_connection(
            |connection, _| Box::pin(async move { Ok(identity(connection).await?.pid) }),
            None,
        )
        .await
        .unwrap();
    let first = fixture.repository();
    let second = fixture.repository();
    let result: PgResult<_> = fixture
        .context
        .transaction(
            move |_| async move {
                let pid = first.insert(1).await?;
                assert_eq!(pid, second.insert(2).await?);
                Ok(pid)
            },
            None,
        )
        .await;
    assert_eq!(result, Ok(expected_pid));
    assert_eq!(fixture.ids().await, vec![1, 2]);
    let repository = fixture.repository();
    assert_eq!(
        fixture
            .context
            .transaction(
                move |_| async move {
                    repository.insert(3).await?;
                    Err::<(), _>(PgError::RepositoryDatabaseNotSet)
                },
                None
            )
            .await,
        Err(PgError::RepositoryDatabaseNotSet)
    );
    assert_eq!(fixture.ids().await, vec![1, 2]);
    let repository = fixture.repository();
    let conflict = PgError::Query {
        kind: PgQueryErrorKind::Conflict,
    };
    let callback_conflict = conflict.clone();
    assert_eq!(
        fixture
            .context
            .transaction(
                move |_| async move {
                    repository.insert(3).await?;
                    assert_eq!(repository.insert(3).await, Err(callback_conflict));
                    Ok(())
                },
                None
            )
            .await,
        Err(conflict)
    );
    assert_eq!(fixture.ids().await, vec![1, 2]);
    let repository = fixture.repository();
    assert_eq!(
        fixture
            .context
            .transaction(
                move |_| async move {
                    repository
                        .insert(1)
                        .await
                        .map_err(|_| PgError::RepositoryDatabaseNotSet)?;
                    Ok(())
                },
                None
            )
            .await,
        Err(PgError::RepositoryDatabaseNotSet)
    );
    fixture.repository().insert(4).await.unwrap();
    assert_eq!(fixture.ids().await, vec![1, 2, 4]);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn transaction_token_including_none_overrides_repository_token() {
    let fixture = Fixture::new().await;
    for supplied in [false, true] {
        let source = ExecutionCancellationSource::default();
        let incoming = ExecutionCancellationSource::default();
        incoming.cancel();
        let token = supplied.then(|| source.view());
        let context = Arc::clone(&fixture.context);
        fixture
            .context
            .transaction(
                move |transaction_token| async move {
                    assert_eq!(transaction_token.is_some(), supplied);
                    context
                        .with_connection(
                            |_, selected| {
                                Box::pin(async move {
                                    assert_eq!(selected.is_some(), supplied);
                                    assert!(
                                        !selected
                                            .as_ref()
                                            .is_some_and(ExecutionCancellation::is_cancelled)
                                    );
                                    Ok::<_, PgError>(())
                                })
                            },
                            Some(incoming.view()),
                        )
                        .await
                },
                token,
            )
            .await
            .unwrap();
    }
    let source = ExecutionCancellationSource::default();
    let token = source.view();
    let repository = fixture.repository();
    assert_eq!(
        fixture
            .context
            .transaction(
                move |_| async move {
                    repository.insert(1).await?;
                    source.cancel();
                    Ok(())
                },
                Some(token)
            )
            .await,
        Err(PgError::TransactionCancelled)
    );
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn busy_context_rejects_overlap_and_dropped_query_forces_rollback() {
    let fixture = Fixture::new().await;
    {
        let (started, ready) = oneshot::channel();
        let pending = fixture.context.with_connection(
            |_, _| {
                Box::pin(async move {
                    started.send(()).unwrap();
                    std::future::pending::<PgResult<()>>().await
                })
            },
            None,
        );
        tokio::pin!(pending);
        within(async {
            tokio::select! {
                result = &mut pending => panic!("callback should stay pending: {result:?}"),
                ready = ready => ready.unwrap(),
            }
        })
        .await;
        assert_eq!(
            fixture
                .context
                .with_connection(|_, _| panic!("busy"), None)
                .await,
            Err::<(), _>(PgError::ContextBusy)
        );
        assert_eq!(
            fixture
                .context
                .transaction(|_| async { Ok(()) }, None)
                .await,
            Err(PgError::ContextBusy)
        );
    }
    fixture.idle().await;
    let context = Arc::clone(&fixture.context);
    let repository = fixture.repository();
    assert_eq!(
        fixture
            .context
            .transaction(
                move |_| async move {
                    repository.insert(1).await?;
                    assert_eq!(
                        context.transaction(|_| async { Ok(()) }, None).await,
                        Err(PgError::ContextTransactionActive)
                    );
                    {
                        let pending = context.with_connection(
                            |_, _| Box::pin(std::future::pending::<PgResult<()>>()),
                            None,
                        );
                        tokio::pin!(pending);
                        assert!(futures_util::poll!(&mut pending).is_pending());
                        assert_eq!(
                            context.with_connection(|_, _| panic!("busy"), None).await,
                            Err::<(), _>(PgError::ContextBusy)
                        );
                    }
                    Ok(())
                },
                None
            )
            .await,
        Err(PgError::TransactionOperationDetached)
    );
    assert_eq!(fixture.count().await, 0);
    fixture.finish().await;
}

async fn sleep_query(
    connection: &mut diesel_async::AsyncPgConnection,
    token: Option<ExecutionCancellation>,
    started: oneshot::Sender<i32>,
) -> PgResult<()> {
    assert!(token.is_some(), "the selected view must reach the callback");
    let pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(connection)
        .await?
        .pid;
    let _ = started.send(pid);
    diesel::sql_query("SELECT pg_sleep(30)")
        .execute(connection)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL; optional TLS CA"]
async fn database_and_context_cancel_running_queries_and_discard_connections() {
    let fixture = Fixture::new().await;
    for use_context in [false, true] {
        let source = ExecutionCancellationSource::default();
        let token = source.view();
        let (started, ready) = oneshot::channel();
        let database = Arc::clone(&fixture.database);
        let context = Arc::clone(&fixture.context);
        let query = tokio::spawn(async move {
            if use_context {
                context
                    .with_connection(
                        |connection, token| Box::pin(sleep_query(connection, token, started)),
                        Some(token),
                    )
                    .await
            } else {
                database
                    .with_connection(
                        |connection, token| Box::pin(sleep_query(connection, token, started)),
                        Some(token),
                    )
                    .await
            }
        });
        let pid = ready.await.unwrap();
        fixture.wait_query(pid, "SELECT pg_sleep(30)").await;
        source.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), query)
                .await
                .unwrap()
                .unwrap(),
            Err(PgError::OperationCancelled)
        );
        assert_eq!(fixture.database.status().unwrap().size, 0);
        assert_eq!(fixture.database.status().unwrap().in_flight, 0);
    }
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL; optional TLS CA"]
async fn context_transaction_cancels_pool_wait_and_rolls_back_running_sql() {
    let fixture = Fixture::new().await;
    let lease = fixture.database.acquire_connection(None).await.unwrap();
    for transaction in [false, true] {
        let source = ExecutionCancellationSource::default();
        let context = Arc::clone(&fixture.context);
        let token = source.view();
        let task = tokio::spawn(async move {
            if transaction {
                context
                    .transaction(|_| async { panic!("pool is occupied") }, Some(token))
                    .await
            } else {
                context
                    .with_connection(|_, _| panic!("pool is occupied"), Some(token))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.database.status().unwrap().in_flight < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        source.cancel();
        let expected = if transaction {
            PgError::TransactionCancelled
        } else {
            PgError::OperationCancelled
        };
        assert_eq!(task.await.unwrap(), Err::<(), _>(expected));
        assert_eq!(fixture.database.status().unwrap().in_flight, 1);
    }
    drop(lease);
    let source = ExecutionCancellationSource::default();
    let token = source.view();
    let (started, ready) = oneshot::channel();
    let context = Arc::clone(&fixture.context);
    let repository = fixture.repository();
    let task = tokio::spawn(async move {
        let queries = Arc::clone(&context);
        context
            .transaction(
                move |_| async move {
                    repository.insert(1).await?;
                    queries
                        .with_connection(
                            |connection, token| Box::pin(sleep_query(connection, token, started)),
                            None,
                        )
                        .await
                },
                Some(token),
            )
            .await
    });
    let pid = ready.await.unwrap();
    fixture.wait_query(pid, "SELECT pg_sleep(30)").await;
    source.cancel();
    assert_eq!(task.await.unwrap(), Err(PgError::TransactionCancelled));
    assert_eq!(fixture.count().await, 0);
    assert_eq!(fixture.database.status().unwrap().size, 0);
    fixture.repository().insert(2).await.unwrap();
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn dropped_transaction_caller_keeps_owner_alive_until_rollback() {
    let fixture = Fixture::new().await;
    let repository = fixture.repository();
    let context = Arc::clone(&fixture.context);
    let (started, ready) = oneshot::channel();
    let caller = tokio::spawn(async move {
        context
            .transaction(
                move |_| async move {
                    repository.insert(1).await?;
                    let _ = started.send(());
                    std::future::pending::<PgResult<()>>().await
                },
                None,
            )
            .await
    });
    ready.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    fixture.idle().await;
    assert_eq!(fixture.count().await, 0);
    fixture.repository().insert(2).await.unwrap();
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn disposal_joins_owner_and_closes_only_its_context() {
    for transaction in [false, true] {
        let fixture = Fixture::new().await;
        let context = Arc::clone(&fixture.context);
        let repository = fixture.repository();
        let (started, ready) = oneshot::channel();
        let task = tokio::spawn(async move {
            if transaction {
                context
                    .transaction(
                        move |_| async move {
                            repository.insert(1).await?;
                            let _ = started.send(());
                            std::future::pending::<PgResult<()>>().await
                        },
                        None,
                    )
                    .await
            } else {
                context
                    .with_connection(
                        |_, token| {
                            Box::pin(async move {
                                assert!(token.is_none());
                                let _ = started.send(());
                                std::future::pending::<PgResult<()>>().await
                            })
                        },
                        None,
                    )
                    .await
            }
        });
        ready.await.unwrap();
        let (first, second) = tokio::join!(fixture.context.dispose(), fixture.context.dispose());
        first.unwrap();
        second.unwrap();
        assert_eq!(task.await.unwrap(), Err(PgError::ContextClosed));
        assert_eq!(fixture.count().await, 0);
        assert!(!fixture.database.status().unwrap().closed);
        assert_eq!(
            fixture
                .context
                .with_connection(|_, _| panic!("closed"), None)
                .await,
            Err::<(), _>(PgError::ContextClosed)
        );
        fixture.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn commit_is_not_interrupted_and_disposal_deadline_starts_at_disposal() {
    let fixture = Fixture::new().await;
    fixture.gate_commit().await;
    let source = ExecutionCancellationSource::default();
    let token = source.view();
    let repository = fixture.repository();
    let context = Arc::clone(&fixture.context);
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        context
            .transaction(
                move |_| async move {
                    let pid = repository.insert(1).await?;
                    let _ = started.send(pid);
                    Ok::<_, PgError>(123)
                },
                Some(token),
            )
            .await
    });
    let pid = within(ready).await.unwrap();
    fixture.wait_query(pid, "COMMIT").await;
    fixture.wait_gate(pid).await;
    source.cancel();
    // The one-second cleanup budget must not already be running during COMMIT.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let before = tokio::time::Instant::now();
    assert_dispose_error(
        within(fixture.context.dispose()).await,
        PgError::ContextCleanupTimeout,
    );
    assert!(before.elapsed() >= Duration::from_secs(1));
    assert_eq!(fixture.database.status().unwrap().in_flight, 1);
    assert_eq!(fixture.database.status().unwrap().available, 0);
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    assert!(!task.is_finished());
    fixture.unlock_gate().await;
    assert_eq!(within(task).await.unwrap(), Ok(123));
    assert_eq!(fixture.ids().await, vec![1]);
    assert_dispose_error(
        fixture.context.dispose().await,
        PgError::ContextCleanupTimeout,
    );
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn panics_rollback_and_native_state_corruption_closes_the_context() {
    let fixture = Fixture::new().await;
    for in_query in [false, true] {
        let repository = fixture.repository();
        let context = Arc::clone(&fixture.context);
        let result = fixture
            .context
            .transaction(
                move |_| async move {
                    repository.insert(1).await?;
                    if in_query {
                        context
                            .with_connection(|_, _| panic!("intentional query panic"), None)
                            .await
                    } else {
                        panic!("intentional workflow panic")
                    }
                },
                None,
            )
            .await;
        assert_eq!(result, Err::<(), _>(PgError::OperationPanicked));
        assert_eq!(fixture.count().await, 0);
    }
    let context = Arc::clone(&fixture.context);
    let result = fixture.context.transaction(move |_| async move {
        context.with_connection(|connection, _| Box::pin(async move {
            use diesel_async::{AsyncConnection, TransactionManager};
            // Leaving an unmatched native savepoint must not return apparent
            // successful COMMIT after releasing only that savepoint.
            <diesel_async::AsyncPgConnection as AsyncConnection>::TransactionManager::begin_transaction(connection).await?;
            Ok(())
        }), None).await
    }, None).await;
    assert_eq!(result, Err(PgError::TransactionFinalization));
    assert_eq!(
        fixture
            .context
            .transaction(|_| async { Ok(()) }, None)
            .await,
        Err(PgError::ContextClosed)
    );
    assert_eq!(fixture.database.status().unwrap().size, 0);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires dedicated TLS PostgreSQL with LILY_PG_TRANSACTION_TEST_URL and LILY_PG_TRANSACTION_TEST_CA"]
async fn failed_cancel_transport_bounds_rollback_and_permanently_closes_context() {
    let mut fixture = Fixture::new().await;
    drop(std::mem::replace(
        &mut fixture.context,
        Arc::new(PgDbContext::default()),
    ));
    let database = Arc::get_mut(&mut fixture.database).unwrap();
    let runtime = Arc::get_mut(
        Arc::get_mut(&mut database.state)
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
    fixture.context = Arc::new(PgDbContext::from(Arc::clone(&fixture.database)));
    let source = ExecutionCancellationSource::default();
    let token = source.view();
    let context = Arc::clone(&fixture.context);
    let repository = fixture.repository();
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        let queries = Arc::clone(&context);
        context
            .transaction(
                move |_| async move {
                    repository.insert(1).await?;
                    queries
                        .with_connection(
                            |connection, token| Box::pin(sleep_query(connection, token, started)),
                            None,
                        )
                        .await
                },
                Some(token),
            )
            .await
    });
    let pid = ready.await.unwrap();
    fixture.wait_query(pid, "SELECT pg_sleep(30)").await;
    source.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap(),
        Err(PgError::TransactionCleanupTimeout)
    );
    assert_eq!(fixture.database.status().unwrap().size, 0);
    assert_eq!(fixture.database.status().unwrap().in_flight, 0);
    assert_eq!(
        fixture
            .context
            .transaction(|_| async { Ok(()) }, None)
            .await,
        Err(PgError::ContextClosed)
    );
    assert_dispose_error(
        fixture.context.dispose().await,
        PgError::TransactionCleanupTimeout,
    );
    // A timeout cannot confirm server rollback; terminate only this test's backend.
    fixture
        .run_sql(format!("SELECT pg_terminate_backend({pid})"))
        .await;
    assert_eq!(fixture.count().await, 0);
    fixture.finish().await;
}

#[cfg(all(feature = "single", feature = "test-support"))]
mod di {
    use lily_config::{ConfigOptions, ConfigService};
    use lily_injectable_derive::Injectable;
    use lily_injection::{ApplicationContainer, ProcessContext};

    use super::*;

    #[derive(Default, Injectable)]
    #[service(lifetime = "Scoped")]
    struct FirstRepository {
        #[inject]
        context: Arc<PgDbContext>,
    }
    impl ServiceTrait for FirstRepository {}

    #[derive(Default, Injectable)]
    #[service(lifetime = "Scoped")]
    struct SecondRepository {
        #[inject]
        context: Arc<PgDbContext>,
    }
    impl ServiceTrait for SecondRepository {}

    #[derive(Default, Injectable)]
    #[service(lifetime = "Scoped")]
    struct Workflow {
        #[inject]
        first: Arc<FirstRepository>,
        #[inject]
        second: Arc<SecondRepository>,
        #[inject]
        context: Arc<PgDbContext>,
    }
    impl ServiceTrait for Workflow {}

    #[tokio::test]
    #[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
    async fn injected_repositories_share_scope_and_owner_preserves_process_context() {
        let fixture = Fixture::new().await;
        let path = std::env::temp_dir().join(format!("{}-di.toml", fixture.schema));
        std::fs::write(&path, "[server]\nhost = '127.0.0.1'\nport = 8080\n").unwrap();
        let database = PgDatabaseService {
            state: Arc::clone(&fixture.database.state),
            test_container_fixture: true,
            ..Default::default()
        };
        let container = ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(ConfigOptions::test(&path)))
            .seed_singleton(database)
            .build()
            .await
            .unwrap();
        let mut first_scope = container
            .create_scope(ProcessContext::with_process_id(81001))
            .unwrap();
        let mut second_scope = container
            .create_scope(ProcessContext::with_process_id(81002))
            .unwrap();
        let first = first_scope
            .run(container.resolve::<Workflow>(None))
            .await
            .unwrap()
            .unwrap();
        let second = second_scope
            .run(container.resolve::<Workflow>(None))
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&first.context, &first.first.context));
        assert!(Arc::ptr_eq(&first.context, &first.second.context));
        assert!(!Arc::ptr_eq(&first.context, &second.context));
        assert_eq!(
            fixture.database.status().unwrap().in_flight,
            0,
            "initialization must not acquire"
        );
        let services = container.services();
        let expected = Arc::clone(&first.context);
        first_scope
            .run(first.context.transaction(
                move |_| async move {
                    assert_eq!(ProcessContext::current().unwrap().process_id, 81001);
                    let dynamic = services.get_service::<PgDbContext>(None).await.unwrap();
                    assert!(Arc::ptr_eq(&dynamic, &expected));
                    dynamic
                        .with_connection(
                            |connection, _| {
                                Box::pin(async move {
                                    diesel::sql_query("SELECT 1").execute(connection).await?;
                                    Ok::<_, PgError>(())
                                })
                            },
                            None,
                        )
                        .await
                },
                None,
            ))
            .await
            .unwrap()
            .unwrap();
        first_scope.close().await.unwrap();
        assert_eq!(
            first
                .context
                .with_connection(|_, _| panic!("closed"), None)
                .await,
            Err::<(), _>(PgError::ContextClosed)
        );
        assert!(!fixture.database.status().unwrap().closed);
        second
            .context
            .with_connection(|_, _| Box::pin(async { Ok::<_, PgError>(()) }), None)
            .await
            .unwrap();
        second_scope.close().await.unwrap();
        container.close().await.unwrap();
        std::fs::remove_file(path).unwrap();
        fixture.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn abandoned_unpolled_query_retains_dirty_lease_until_dropped() {
    let fixture = Fixture::new().await;
    let context = Arc::clone(&fixture.context);
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        let queries = Arc::clone(&context);
        context
            .transaction(
                move |_| async move {
                    let mut query = Box::pin(async move {
                        queries
                            .with_connection(
                                |_, _| Box::pin(std::future::pending::<PgResult<()>>()),
                                None,
                            )
                            .await
                    });
                    assert!(futures_util::poll!(&mut query).is_pending());
                    assert!(started.send(query).is_ok());
                    Ok(())
                },
                None,
            )
            .await
    });
    let retained = ready.await.unwrap();
    assert_eq!(task.await.unwrap(), Err(PgError::TransactionCleanupTimeout));
    assert_eq!(fixture.database.status().unwrap().in_flight, 1);
    assert_eq!(fixture.database.status().unwrap().available, 0);
    assert_eq!(
        fixture
            .context
            .transaction(|_| async { Ok(()) }, None)
            .await,
        Err(PgError::ContextClosed)
    );
    drop(retained);
    fixture.idle().await;
    assert_eq!(fixture.database.status().unwrap().size, 0);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn failed_native_rollback_overrides_work_error_and_closes_context() {
    let fixture = Fixture::new().await;
    let context = Arc::clone(&fixture.context);
    let repository = fixture.repository();
    let (started, ready) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let task = tokio::spawn(async move {
        context
            .transaction(
                move |_| async move {
                    let pid = repository.insert(1).await?;
                    let _ = started.send(pid);
                    resumed.await.unwrap();
                    Err::<(), _>(PgError::RepositoryDatabaseNotSet)
                },
                None,
            )
            .await
    });
    let pid = ready.await.unwrap();
    fixture
        .run_sql(format!("SELECT pg_terminate_backend({pid})"))
        .await;
    resume.send(()).unwrap();
    let result = task.await.unwrap();
    assert_eq!(
        result,
        Err(PgError::Query {
            kind: PgQueryErrorKind::Database
        })
    );
    assert_eq!(fixture.count().await, 0);
    assert_eq!(fixture.database.status().unwrap().size, 0);
    assert_eq!(
        fixture
            .context
            .transaction(|_| async { Ok(()) }, None)
            .await,
        Err(PgError::ContextClosed)
    );
    fixture.finish().await;
}
