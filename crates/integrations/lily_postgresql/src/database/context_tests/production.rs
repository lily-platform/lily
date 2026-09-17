//! Real transport qualification. Barriers and PostgreSQL lock observations
//! establish race boundaries; elapsed time is never used to guess readiness.

use std::sync::atomic::{AtomicUsize, Ordering};

use lily_config::{ConfigOptions, ConfigService};
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ProcessContext};
use tokio::sync::{Barrier, Notify, mpsc};

use super::*;
use crate::PgPoolTimeoutPhase;

const CONTENDERS: usize = 16;
const ROUNDS: i64 = 8;

/// Keep the winner inside its callback until every other caller has returned.
/// This proves admission, rather than depending on scheduler luck for overlap.
async fn race_queries(fixture: &Fixture, base: i64, transaction: Option<Identity>) -> i64 {
    let start = Arc::new(Barrier::new(CONTENDERS + 1));
    let release = Arc::new(Notify::new());
    let invoked = Arc::new(AtomicUsize::new(0));
    let (entered, mut entry) = mpsc::unbounded_channel();
    let (finished, mut results) = mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for index in 0..CONTENDERS {
        let context = Arc::clone(&fixture.context);
        let start = Arc::clone(&start);
        let release = Arc::clone(&release);
        let invoked = Arc::clone(&invoked);
        let entered = entered.clone();
        let finished = finished.clone();
        let id = base + index as i64;
        let sql = format!("INSERT INTO {}.orders VALUES ($1)", fixture.schema);
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            let result = context
                .with_connection(
                    move |connection, token| {
                        Box::pin(async move {
                            assert!(token.is_none());
                            assert_eq!(
                                invoked.fetch_add(1, Ordering::SeqCst),
                                0,
                                "two callbacks were admitted concurrently"
                            );
                            let before = identity(connection).await?;
                            entered.send((id, before)).unwrap();
                            release.notified().await;
                            diesel::sql_query(sql)
                                .bind::<BigInt, _>(id)
                                .execute(connection)
                                .await?;
                            let after = identity(connection).await?;
                            assert_eq!(after.pid, before.pid);
                            if let Some(expected) = transaction {
                                assert_eq!(before, expected);
                                assert_eq!(after, expected);
                            } else {
                                assert_ne!(
                                    before.xid, after.xid,
                                    "ordinary operations must not introduce an implicit transaction"
                                );
                            }
                            Ok(after.pid)
                        })
                    },
                    None,
                )
                .await;
            finished.send((id, result)).unwrap();
        }));
    }
    drop(entered);
    drop(finished);
    within(start.wait()).await;
    let (winner, active) = within(entry.recv()).await.unwrap();
    for _ in 1..CONTENDERS {
        let (loser, result) = within(results.recv()).await.unwrap();
        assert_ne!(loser, winner);
        assert_eq!(result, Err(PgError::ContextBusy));
    }
    assert_eq!(AtomicUsize::load(&invoked, Ordering::SeqCst), 1);
    assert!(matches!(
        entry.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let status = fixture.database.status().unwrap();
    assert_eq!(
        (
            status.max_size,
            status.size,
            status.available,
            status.in_flight,
            status.waiting
        ),
        (1, 1, 0, 1, 0)
    );
    assert_eq!(
        fixture
            .context
            .transaction(|_| async { panic!("second transaction entered") }, None)
            .await,
        Err::<(), _>(if transaction.is_some() {
            PgError::ContextTransactionActive
        } else {
            PgError::ContextBusy
        })
    );
    release.notify_one();
    assert_eq!(
        within(results.recv()).await.unwrap(),
        (winner, Ok(active.pid))
    );
    for task in tasks {
        within(task).await.unwrap();
    }
    assert!(results.recv().await.is_none());
    winner
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn concurrent_ordinary_queries_admit_exactly_one_callback_and_release_the_lease() {
    let fixture = Fixture::new().await;
    let mut expected = Vec::new();
    for round in 0..ROUNDS {
        let winner = race_queries(&fixture, round * 100, None).await;
        expected.push(winner);
        assert_eq!(fixture.ids().await, expected);
        let status = fixture.database.status().unwrap();
        assert_eq!(
            (status.in_flight, status.available, status.waiting),
            (0, 1, 0)
        );
    }
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn concurrent_transaction_queries_share_backend_and_xid_with_exact_commit_and_rollback_images()
 {
    let fixture = Fixture::new().await;
    let mut committed = Vec::new();
    let mut previous_xid = None;
    let mut previous_pid = None;
    for round in 0..ROUNDS {
        let (started, ready) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let context = Arc::clone(&fixture.context);
        let repository = fixture.repository();
        let base = round * 100;
        let task = tokio::spawn(async move {
            let queries = Arc::clone(&context);
            context
                .transaction(
                    move |_| async move {
                        let first_pid = repository.insert(base).await?;
                        let current = queries
                            .with_connection(|connection, _| Box::pin(identity(connection)), None)
                            .await?;
                        assert_eq!(current.pid, first_pid);
                        started.send(current).unwrap();
                        finished.await.unwrap()
                    },
                    None,
                )
                .await
        });
        let transaction = within(ready).await.unwrap();
        if let Some(pid) = previous_pid {
            assert_eq!(
                pid, transaction.pid,
                "healthy finalization should recycle the only backend"
            );
        }
        assert_ne!(previous_xid, Some(transaction.xid));
        previous_pid = Some(transaction.pid);
        previous_xid = Some(transaction.xid);
        assert_eq!(
            fixture.ids().await,
            committed,
            "uncommitted writes escaped to another connection"
        );
        let winner = race_queries(&fixture, base + 1, Some(transaction)).await;
        assert_eq!(
            fixture.ids().await,
            committed,
            "the second repository wrote outside the transaction"
        );
        if round % 2 == 0 {
            finish.send(Ok(round)).unwrap();
            assert_eq!(within(task).await.unwrap(), Ok(round));
            committed.extend([base, winner]);
        } else {
            finish.send(Err(PgError::RepositoryDatabaseNotSet)).unwrap();
            assert_eq!(
                within(task).await.unwrap(),
                Err(PgError::RepositoryDatabaseNotSet)
            );
        }
        assert_eq!(fixture.ids().await, committed);
        let status = fixture.database.status().unwrap();
        assert_eq!(
            (status.in_flight, status.available, status.waiting),
            (0, 1, 0)
        );
    }
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn live_repository_token_cannot_cancel_a_transaction_and_selected_view_tracks_its_owner() {
    for supplied in [false, true] {
        let fixture = Fixture::new().await;
        fixture.lock_gate().await;
        let transaction_source = ExecutionCancellationSource::default();
        let operation_source = ExecutionCancellationSource::default();
        let transaction_token = supplied.then(|| transaction_source.view());
        let operation_token = operation_source.view();
        let context = Arc::clone(&fixture.context);
        let repository = fixture.repository();
        let gate = fixture.gate_query();
        let (started, ready) = oneshot::channel();
        let task = tokio::spawn(async move {
            let queries = Arc::clone(&context);
            context
                .transaction(
                    move |_| async move {
                        let pid = repository.insert(1).await?;
                        queries
                            .with_connection(
                                |connection, selected| {
                                    Box::pin(async move {
                                        started.send((pid, selected)).unwrap();
                                        diesel::sql_query(gate).execute(connection).await?;
                                        Ok(())
                                    })
                                },
                                Some(operation_token),
                            )
                            .await
                    },
                    transaction_token,
                )
                .await
        });
        let (pid, selected) = within(ready).await.unwrap();
        fixture.wait_gate(pid).await;
        operation_source.cancel();
        assert_eq!(selected.is_some(), supplied);
        assert!(
            !selected
                .as_ref()
                .is_some_and(ExecutionCancellation::is_cancelled)
        );
        assert_eq!(fixture.ids().await, Vec::<i64>::new());
        assert!(!task.is_finished());
        if supplied {
            transaction_source.cancel();
            within(selected.unwrap().cancelled()).await;
            assert_eq!(
                within(task).await.unwrap(),
                Err(PgError::TransactionCancelled)
            );
            assert_eq!(fixture.ids().await, Vec::<i64>::new());
            assert_eq!(fixture.database.status().unwrap().size, 0);
        } else {
            fixture.unlock_gate().await;
            assert_eq!(within(task).await.unwrap(), Ok(()));
            assert_eq!(fixture.ids().await, vec![1]);
            assert_eq!(fixture.database.status().unwrap().available, 1);
        }
        fixture.finish().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn acquisition_timeout_releases_admission_without_invoking_work() {
    let fixture = Fixture::new().await;
    let invoked = Arc::new(AtomicUsize::new(0));
    for transaction in [false, true] {
        let lease = fixture.database.acquire_connection(None).await.unwrap();
        let context = Arc::clone(&fixture.context);
        let counter = Arc::clone(&invoked);
        let task = tokio::spawn(async move {
            if transaction {
                context
                    .transaction(
                        move |_| async move {
                            counter.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        },
                        None,
                    )
                    .await
            } else {
                context
                    .with_connection(
                        move |_, _| {
                            Box::pin(async move {
                                counter.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })
                        },
                        None,
                    )
                    .await
            }
        });
        fixture.wait_pool(2, 1).await;
        assert_eq!(
            fixture
                .context
                .with_connection(|_, _| panic!("starting operation must exclude work"), None)
                .await,
            Err::<(), _>(PgError::ContextBusy)
        );
        assert_eq!(
            within(task).await.unwrap(),
            Err(PgError::PoolTimeout {
                phase: PgPoolTimeoutPhase::Acquire
            })
        );
        assert_eq!(AtomicUsize::load(&invoked, Ordering::SeqCst), 0);
        let status = fixture.database.status().unwrap();
        assert_eq!(
            (status.in_flight, status.waiting, status.available),
            (1, 0, 0)
        );
        drop(lease);
        // The timeout must not poison an otherwise open context.
        let repository = fixture.repository();
        assert_eq!(
            fixture
                .context
                .transaction(
                    move |_| async move {
                        repository.insert(if transaction { 2 } else { 1 }).await?;
                        Ok::<_, PgError>(())
                    },
                    None
                )
                .await,
            Ok(())
        );
    }
    assert_eq!(fixture.ids().await, vec![1, 2]);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn aborting_caller_during_blocked_sql_rolls_back_and_discards_the_backend() {
    let fixture = Fixture::new().await;
    fixture.lock_gate().await;
    let repository = fixture.repository();
    let context = Arc::clone(&fixture.context);
    let gate = fixture.gate_query();
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        let queries = Arc::clone(&context);
        context
            .transaction(
                move |token| async move {
                    assert!(
                        token.is_none(),
                        "caller-drop cleanup must also work without an external token"
                    );
                    let pid = repository.insert(1).await?;
                    started.send(pid).unwrap();
                    queries
                        .with_connection(
                            |connection, _| {
                                Box::pin(async move {
                                    diesel::sql_query(gate).execute(connection).await?;
                                    Ok::<_, PgError>(())
                                })
                            },
                            None,
                        )
                        .await
                },
                None,
            )
            .await
    });
    let pid = within(ready).await.unwrap();
    fixture.wait_gate(pid).await;
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    // Observe cleanup before dispose: asking disposal to cancel here would mask
    // a broken caller-drop guard while the real SQL remains blocked.
    fixture.idle().await;
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    let status = fixture.database.status().unwrap();
    assert_eq!((status.in_flight, status.size, status.waiting), (0, 0, 0));
    let replacement = fixture
        .database
        .with_connection(|connection, _| Box::pin(identity(connection)), None)
        .await
        .unwrap();
    assert_ne!(replacement.pid, pid);
    within(fixture.context.dispose()).await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn caller_abort_after_commit_started_preserves_committed_data_and_shutdown_accounting() {
    let fixture = Fixture::new().await;
    fixture.gate_commit().await;
    let repository = fixture.repository();
    let context = Arc::clone(&fixture.context);
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        context
            .transaction(
                move |_| async move {
                    started.send(repository.insert(1).await?).unwrap();
                    Ok::<_, PgError>(())
                },
                None,
            )
            .await
    });
    let pid = within(ready).await.unwrap();
    fixture.wait_query(pid, "COMMIT").await;
    fixture.wait_gate(pid).await;
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    assert_eq!(fixture.database.status().unwrap().in_flight, 1);
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    fixture.unlock_gate().await;
    fixture.idle().await;
    assert_eq!(
        fixture.ids().await,
        vec![1],
        "a COMMIT already sent must not be reported or treated as rollback"
    );
    assert_eq!(fixture.database.status().unwrap().in_flight, 0);
    assert!(!fixture.database.status().unwrap().closed);
    within(fixture.context.dispose()).await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn deferred_constraint_failure_during_commit_returns_exact_error_and_never_reports_success() {
    let fixture = Fixture::new().await;
    fixture
        .run_sql(format!(
            "ALTER TABLE {}.orders DROP CONSTRAINT orders_pkey",
            fixture.schema
        ))
        .await;
    fixture.run_sql(format!("ALTER TABLE {}.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED", fixture.schema)).await;
    let first = fixture.repository();
    let second = fixture.repository();
    let completed = Arc::new(AtomicUsize::new(0));
    let callback_completed = Arc::clone(&completed);
    let result = within(fixture.context.transaction(
        move |_| async move {
            let pid = first.insert(1).await?;
            assert_eq!(
                second.insert(1).await?,
                pid,
                "the constraint must be deferred until COMMIT"
            );
            callback_completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
        None,
    ))
    .await;
    let expected = PgError::Query {
        kind: PgQueryErrorKind::Conflict,
    };
    assert_eq!(result, Err(expected.clone()));
    assert_eq!(AtomicUsize::load(&completed, Ordering::SeqCst), 1);
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    let status = fixture.database.status().unwrap();
    assert_eq!((status.in_flight, status.available, status.size), (0, 0, 0));
    assert_eq!(
        fixture
            .context
            .transaction(|_| async { panic!("closed context") }, None)
            .await,
        Err::<(), _>(PgError::ContextClosed)
    );
    assert_dispose_error(fixture.context.dispose().await, expected);
    fixture.finish().await;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ScopedRepository {
    #[inject]
    context: Arc<PgDbContext>,
}
impl ServiceTrait for ScopedRepository {}

impl ScopedRepository {
    async fn insert(&self, schema: String, id: i64) -> PgResult<i32> {
        Repository {
            context: Arc::clone(&self.context),
            schema,
        }
        .insert(id)
        .await
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ScopedWorkflow {
    #[inject]
    repository: Arc<ScopedRepository>,
    #[inject]
    context: Arc<PgDbContext>,
}
impl ServiceTrait for ScopedWorkflow {}

async fn container(fixture: &Fixture) -> (ApplicationContainer, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("{}-qualification.toml", fixture.schema));
    std::fs::write(&path, "[server]\nhost = '127.0.0.1'\nport = 8080\n").unwrap();
    let database = PgDatabaseService {
        state: Arc::clone(&fixture.database.state),
        test_container_fixture: true,
        ..Default::default()
    };
    let container = within(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(ConfigOptions::test(&path)))
            .seed_singleton(database)
            .build(),
    )
    .await
    .unwrap();
    (container, path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn closing_one_di_scope_rolls_back_its_sql_and_unblocks_an_independent_waiting_scope() {
    let fixture = Fixture::new().await;
    fixture.lock_gate().await;
    let (container, config) = container(&fixture).await;
    let mut first_scope = container
        .create_scope(ProcessContext::with_process_id(91001))
        .unwrap();
    let mut second_scope = container
        .create_scope(ProcessContext::with_process_id(91002))
        .unwrap();
    let first = within(first_scope.run(container.resolve::<ScopedWorkflow>(None)))
        .await
        .unwrap()
        .unwrap();
    let second = within(second_scope.run(container.resolve::<ScopedWorkflow>(None)))
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&first.context, &first.repository.context));
    assert!(Arc::ptr_eq(&second.context, &second.repository.context));
    assert!(!Arc::ptr_eq(&first.context, &second.context));
    assert_eq!(container.active_scope_count(), 2);
    assert_eq!(fixture.database.status().unwrap().in_flight, 0);
    let (started, ready) = oneshot::channel();
    let workflow = Arc::clone(&first);
    let schema = fixture.schema.clone();
    let gate = fixture.gate_query();
    let services = container.services();
    let first_task = tokio::spawn(ProcessContext::scope(
        first_scope.context().clone(),
        async move {
            let context = Arc::clone(&workflow.context);
            context
                .transaction(
                    move |_| async move {
                        assert_eq!(ProcessContext::current().unwrap().process_id, 91001);
                        let dynamic = services
                            .get_service::<ScopedRepository>(None)
                            .await
                            .unwrap();
                        assert!(Arc::ptr_eq(&dynamic, &workflow.repository));
                        let pid = workflow.repository.insert(schema, 1).await?;
                        started.send(pid).unwrap();
                        workflow
                            .context
                            .with_connection(
                                |connection, token| {
                                    Box::pin(async move {
                                        assert!(token.is_none());
                                        diesel::sql_query(gate).execute(connection).await?;
                                        Ok(())
                                    })
                                },
                                None,
                            )
                            .await
                    },
                    None,
                )
                .await
        },
    ));
    let first_pid = within(ready).await.unwrap();
    fixture.wait_gate(first_pid).await;
    let entered = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&entered);
    let workflow = Arc::clone(&second);
    let schema = fixture.schema.clone();
    let second_task = tokio::spawn(ProcessContext::scope(
        second_scope.context().clone(),
        async move {
            let context = Arc::clone(&workflow.context);
            context
                .transaction(
                    move |_| async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(ProcessContext::current().unwrap().process_id, 91002);
                        workflow.repository.insert(schema, 2).await
                    },
                    None,
                )
                .await
        },
    ));
    fixture.wait_pool(2, 1).await;
    assert_eq!(
        AtomicUsize::load(&entered, Ordering::SeqCst),
        0,
        "another scope joined the first scope's transaction"
    );
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    within(first_scope.close()).await.unwrap();
    assert_eq!(
        within(first_task).await.unwrap(),
        Err(PgError::ContextClosed)
    );
    let second_pid = within(second_task).await.unwrap().unwrap();
    assert_ne!(
        second_pid, first_pid,
        "the interrupted backend must be discarded"
    );
    assert_eq!(AtomicUsize::load(&entered, Ordering::SeqCst), 1);
    assert_eq!(fixture.ids().await, vec![2]);
    assert_eq!(container.active_scope_count(), 1);
    assert_eq!(
        first
            .context
            .with_connection(|_, _| panic!("disposed scope"), None)
            .await,
        Err::<(), _>(PgError::ContextClosed)
    );
    assert_eq!(
        second
            .context
            .with_connection(
                |connection, _| Box::pin(async move {
                    Ok::<_, PgError>(identity(connection).await?.pid)
                }),
                None
            )
            .await,
        Ok(second_pid)
    );
    let status = fixture.database.status().unwrap();
    assert_eq!(
        (
            status.in_flight,
            status.available,
            status.waiting,
            status.closed
        ),
        (0, 1, 0, false)
    );
    within(second_scope.close()).await.unwrap();
    assert_eq!(container.active_scope_count(), 0);
    within(container.close()).await.unwrap();
    std::fs::remove_file(config).unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn dropping_di_scope_automatically_disposes_context_while_sql_is_blocked() {
    let fixture = Fixture::new().await;
    fixture.lock_gate().await;
    let (container, config) = container(&fixture).await;
    let scope = container
        .create_scope(ProcessContext::with_process_id(92001))
        .unwrap();
    let workflow = within(scope.run(container.resolve::<ScopedWorkflow>(None)))
        .await
        .unwrap()
        .unwrap();
    let retained = Arc::clone(&workflow.context);
    let gate = fixture.gate_query();
    let schema = fixture.schema.clone();
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(ProcessContext::scope(scope.context().clone(), async move {
        let context = Arc::clone(&workflow.context);
        context
            .transaction(
                move |_| async move {
                    let pid = workflow.repository.insert(schema, 1).await?;
                    started.send(pid).unwrap();
                    workflow
                        .context
                        .with_connection(
                            |connection, _| {
                                Box::pin(async move {
                                    diesel::sql_query(gate).execute(connection).await?;
                                    Ok(())
                                })
                            },
                            None,
                        )
                        .await
                },
                None,
            )
            .await
    }));
    let pid = within(ready).await.unwrap();
    fixture.wait_gate(pid).await;
    drop(scope);
    // Do not call context.dispose here: that would mask broken scope-drop cleanup.
    assert_eq!(within(task).await.unwrap(), Err(PgError::ContextClosed));
    assert_eq!(container.active_scope_count(), 0);
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    assert_eq!(
        retained
            .with_connection(|_, _| panic!("retained disposed service"), None)
            .await,
        Err::<(), _>(PgError::ContextClosed)
    );
    let status = fixture.database.status().unwrap();
    assert_eq!(
        (status.in_flight, status.size, status.waiting, status.closed),
        (0, 0, 0, false)
    );
    within(container.close()).await.unwrap();
    std::fs::remove_file(config).unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn cancelling_ordinary_work_preserves_already_committed_statements_and_forwards_the_actual_view()
 {
    let fixture = Fixture::new().await;
    fixture.lock_gate().await;
    let source = ExecutionCancellationSource::default();
    let token = source.view();
    let context = Arc::clone(&fixture.context);
    let sql = format!("INSERT INTO {}.orders VALUES (1)", fixture.schema);
    let gate = fixture.gate_query();
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        context
            .with_connection(
                |connection, selected| {
                    Box::pin(async move {
                        diesel::sql_query(sql).execute(connection).await?;
                        let current = identity(connection).await?;
                        started
                            .send((
                                current,
                                selected.expect("ordinary callback must receive its own view"),
                            ))
                            .unwrap();
                        diesel::sql_query(gate).execute(connection).await?;
                        Ok(())
                    })
                },
                Some(token),
            )
            .await
    });
    let (current, selected) = within(ready).await.unwrap();
    fixture.wait_gate(current.pid).await;
    assert_eq!(
        fixture.ids().await,
        vec![1],
        "ordinary statements should be committed before callback completion"
    );
    source.cancel();
    within(selected.cancelled()).await;
    assert_eq!(
        within(task).await.unwrap(),
        Err(PgError::OperationCancelled)
    );
    assert_eq!(
        fixture.ids().await,
        vec![1],
        "cancellation must not claim to undo an autocommitted statement"
    );
    assert_eq!(fixture.database.status().unwrap().size, 0);
    let replacement_pid = fixture.repository().insert(2).await.unwrap();
    assert_ne!(replacement_pid, current.pid);
    assert_eq!(fixture.ids().await, vec![1, 2]);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated PostgreSQL and LILY_PG_TRANSACTION_TEST_URL"]
async fn cancellation_during_native_begin_awaits_begin_then_rolls_back_without_entering_work() {
    use diesel::connection::InstrumentationEvent;
    use diesel_async::AsyncConnection;

    let fixture = Fixture::new().await;
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = Arc::clone(&events);
    let (started, ready) = oneshot::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    let pid = fixture
        .database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    let pid = identity(connection).await?.pid;
                    let mut started = Some(started);
                    connection.set_instrumentation(move |event: InstrumentationEvent<'_>| {
                        match event {
                            InstrumentationEvent::BeginTransaction { .. } => {
                                recorded.lock().unwrap().push("begin");
                                started
                                    .take()
                                    .expect("only one BEGIN expected")
                                    .send(())
                                    .unwrap();
                                // A bounded test-only pause at Diesel's real native BEGIN
                                // entry point. Other Tokio workers deliver cancellation.
                                resumed
                                    .recv_timeout(Duration::from_secs(5))
                                    .expect("BEGIN test gate was not released");
                            }
                            InstrumentationEvent::CommitTransaction { .. } => {
                                recorded.lock().unwrap().push("commit")
                            }
                            InstrumentationEvent::RollbackTransaction { .. } => {
                                recorded.lock().unwrap().push("rollback")
                            }
                            _ => {}
                        }
                    });
                    Ok(pid)
                })
            },
            None,
        )
        .await
        .unwrap();
    let source = ExecutionCancellationSource::default();
    let token = source.view();
    let invoked = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&invoked);
    let context = Arc::clone(&fixture.context);
    let task = tokio::spawn(async move {
        context
            .transaction(
                move |_| async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                Some(token),
            )
            .await
    });
    within(ready).await.unwrap();
    assert_eq!(
        fixture
            .context
            .with_connection(|_, _| panic!("query entered during BEGIN"), None)
            .await,
        Err::<(), _>(PgError::ContextBusy)
    );
    assert_eq!(fixture.database.status().unwrap().in_flight, 1);
    source.cancel();
    resume.send(()).unwrap();
    assert_eq!(
        within(task).await.unwrap(),
        Err(PgError::TransactionCancelled)
    );
    assert_eq!(AtomicUsize::load(&invoked, Ordering::SeqCst), 0);
    assert_eq!(*events.lock().unwrap(), vec!["begin", "rollback"]);
    assert_eq!(fixture.database.status().unwrap().size, 0);
    assert_eq!(fixture.database.status().unwrap().in_flight, 0);
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    assert_ne!(fixture.repository().insert(1).await.unwrap(), pid);
    fixture.finish().await;
}
