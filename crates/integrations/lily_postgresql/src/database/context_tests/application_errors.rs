//! Application error contracts over the real, one-connection PostgreSQL pool.

use std::cell::Cell;

use futures_util::FutureExt;

use super::*;
use crate::PgPoolTimeoutPhase;

mod connection;

// No Clone or std::error::Error implementation; Cell also makes this !Sync.
#[derive(Debug, PartialEq, Eq)]
enum WorkflowError {
    Rejected(Box<Rejection>),
    Database(PgError),
}

#[derive(Debug, PartialEq, Eq)]
struct Rejection {
    order_id: i64,
    code: String,
    attempts: Cell<u32>,
}

impl From<PgError> for WorkflowError {
    fn from(error: PgError) -> Self {
        Self::Database(error)
    }
}

fn rejection() -> Box<Rejection> {
    Box::new(Rejection {
        order_id: 42,
        code: "subscription_required".into(),
        attempts: Cell::new(3),
    })
}

fn conflict() -> PgError {
    PgError::Query {
        kind: PgQueryErrorKind::Conflict,
    }
}

fn assert_released(fixture: &Fixture, available: usize) {
    let status = fixture.database.status().unwrap();
    assert_eq!(
        (
            status.in_flight,
            status.waiting,
            status.available,
            status.size
        ),
        (0, 0, available, available)
    );
}

async fn assert_closed(fixture: &Fixture, cleanup: PgError) {
    let result: Result<(), WorkflowError> = fixture
        .context
        .transaction(
            |_| async { panic!("closed context must not enter work") },
            None,
        )
        .await;
    assert_eq!(result, Err(WorkflowError::Database(PgError::ContextClosed)));
    assert_dispose_error(fixture.context.dispose().await, cleanup.clone());
    assert_dispose_error(fixture.context.dispose().await, cleanup);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn custom_error_preserves_payload_and_rolls_back_while_success_commits() {
    let fixture = Fixture::new().await;
    let first = fixture.repository();
    let second = fixture.repository();
    let committed: Result<i32, WorkflowError> = fixture
        .context
        .transaction(
            move |_| async move {
                let pid = first.insert(1).await?;
                assert_eq!(second.insert(2).await?, pid);
                Ok(pid)
            },
            None,
        )
        .await;
    let pid = committed.unwrap();
    assert_eq!(fixture.ids().await, vec![1, 2]);
    assert_released(&fixture, 1);

    let payload = rejection();
    let address = (&*payload as *const Rejection) as usize;
    let context = Arc::clone(&fixture.context);
    let first = fixture.repository();
    let second = fixture.repository();
    let (started, ready) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let task = tokio::spawn(async move {
        context
            .transaction(
                move |_| async move {
                    assert_eq!(first.insert(3).await?, pid);
                    assert_eq!(second.insert(4).await?, pid);
                    started.send(()).unwrap();
                    resumed.await.unwrap();
                    Err::<(), _>(WorkflowError::Rejected(payload))
                },
                None,
            )
            .await
    });
    within(ready).await.unwrap();
    assert_eq!(
        fixture.ids().await,
        vec![1, 2],
        "uncommitted application writes escaped"
    );
    resume.send(()).unwrap();
    let Err(WorkflowError::Rejected(returned)) = within(task).await.unwrap() else {
        panic!("rollback must preserve the application's original error");
    };
    assert_eq!((&*returned as *const Rejection) as usize, address);
    assert_eq!(
        returned,
        rejection(),
        "the complete error payload must survive"
    );
    assert_eq!(fixture.ids().await, vec![1, 2]);
    assert_released(&fixture, 1);
    assert_eq!(fixture.repository().insert(5).await.unwrap(), pid);
    assert_eq!(fixture.ids().await, vec![1, 2, 5]);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn mapped_propagated_and_swallowed_query_errors_all_prevent_commit() {
    let fixture = Fixture::new().await;
    let pid = fixture.repository().insert(1).await.unwrap();
    for handling in 0..3 {
        let repository = fixture.repository();
        let result: Result<(), WorkflowError> = fixture
            .context
            .transaction(
                move |_| async move {
                    assert_eq!(repository.insert(2).await?, pid);
                    let duplicate = repository.insert(2).await;
                    assert_eq!(duplicate, Err(conflict()));
                    match handling {
                        0 => {
                            duplicate?;
                        }
                        1 => {
                            duplicate.map_err(|_| WorkflowError::Rejected(rejection()))?;
                        }
                        2 => {} // Swallowed SQL errors must still make the transaction rollback-only.
                        _ => unreachable!(),
                    }
                    Ok(())
                },
                None,
            )
            .await;
        let expected = if handling == 1 {
            WorkflowError::Rejected(rejection())
        } else {
            WorkflowError::Database(conflict())
        };
        assert_eq!(result, Err(expected));
        assert_eq!(fixture.ids().await, vec![1]);
        assert_released(&fixture, 1);
    }
    assert_eq!(fixture.repository().insert(3).await.unwrap(), pid);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn acquisition_errors_convert_without_entering_application_work() {
    let fixture = Fixture::new().await;
    let lease = fixture.database.acquire_connection(None).await.unwrap();
    let result: Result<(), WorkflowError> = within(fixture.context.transaction(
        |_| async { panic!("occupied pool must prevent work") },
        None,
    ))
    .await;
    assert_eq!(
        result,
        Err(WorkflowError::Database(PgError::PoolTimeout {
            phase: PgPoolTimeoutPhase::Acquire
        }))
    );
    let status = fixture.database.status().unwrap();
    assert_eq!(
        (
            status.in_flight,
            status.waiting,
            status.size,
            status.available
        ),
        (1, 0, 1, 0)
    );
    drop(lease);
    fixture.repository().insert(1).await.unwrap();
    assert_eq!(fixture.ids().await, vec![1]);
    assert_released(&fixture, 1);
    fixture.database.close().await.unwrap();
    let result: Result<(), WorkflowError> = fixture
        .context
        .transaction(|_| async { panic!("closed pool must prevent work") }, None)
        .await;
    assert_eq!(result, Err(WorkflowError::Database(PgError::PoolClosed)));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn cancellation_disposal_and_panic_convert_after_rollback() {
    for stop in 0..3 {
        let fixture = Fixture::new().await;
        let source = ExecutionCancellationSource::default();
        let token = source.view();
        let context = Arc::clone(&fixture.context);
        let repository = fixture.repository();
        let (started, ready) = oneshot::channel();
        let (resume, resumed) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            context
                .transaction::<(), WorkflowError, _, _>(
                    move |_| async move {
                        repository.insert(1).await?;
                        started.send(()).unwrap();
                        resumed.await.unwrap();
                        panic!("intentional workflow panic")
                    },
                    Some(token),
                )
                .await
        });
        within(ready).await.unwrap();
        assert_eq!(fixture.ids().await, Vec::<i64>::new());
        let expected = match stop {
            0 => {
                source.cancel();
                PgError::TransactionCancelled
            }
            1 => {
                within(fixture.context.dispose()).await.unwrap();
                PgError::ContextClosed
            }
            2 => {
                resume.send(()).unwrap();
                PgError::OperationPanicked
            }
            _ => unreachable!(),
        };
        assert_eq!(
            within(task).await.unwrap(),
            Err(WorkflowError::Database(expected))
        );
        assert_eq!(fixture.ids().await, Vec::<i64>::new());
        assert_released(&fixture, 0);
        if stop != 1 {
            fixture.repository().insert(2).await.unwrap();
            assert_eq!(fixture.ids().await, vec![2]);
        }
        fixture.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn commit_failure_converts_and_closes_context_despite_successful_work() {
    let fixture = Fixture::new().await;
    fixture
        .run_sql(format!(
            "ALTER TABLE {}.orders DROP CONSTRAINT orders_pkey",
            fixture.schema
        ))
        .await;
    fixture.run_sql(format!("ALTER TABLE {}.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED", fixture.schema)).await;
    let repository = fixture.repository();
    let result: Result<(), WorkflowError> = within(fixture.context.transaction(
        move |_| async move {
            let pid = repository.insert(1).await?;
            assert_eq!(
                repository.insert(1).await?,
                pid,
                "duplicate must be accepted until COMMIT"
            );
            Ok(())
        },
        None,
    ))
    .await;
    assert_eq!(result, Err(WorkflowError::Database(conflict())));
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    assert_released(&fixture, 0);
    assert_closed(&fixture, conflict()).await;
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn rollback_failure_overrides_custom_error_and_retains_cleanup_failure() {
    let fixture = Fixture::new().await;
    let context = Arc::clone(&fixture.context);
    let repository = fixture.repository();
    let (started, ready) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let task = tokio::spawn(async move {
        context
            .transaction(
                move |_| async move {
                    started.send(repository.insert(1).await?).unwrap();
                    resumed.await.unwrap();
                    Err::<(), _>(WorkflowError::Rejected(rejection()))
                },
                None,
            )
            .await
    });
    let pid = within(ready).await.unwrap();
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    fixture
        .run_sql(format!("SELECT pg_terminate_backend({pid})"))
        .await;
    resume.send(()).unwrap();
    let expected = PgError::Query {
        kind: PgQueryErrorKind::Database,
    };
    assert_eq!(
        within(task).await.unwrap(),
        Err(WorkflowError::Database(expected.clone()))
    );
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    assert_released(&fixture, 0);
    assert_closed(&fixture, expected).await;
    assert_ne!(
        fixture
            .database
            .with_connection(|connection, _| Box::pin(identity(connection)), None)
            .await
            .unwrap()
            .pid,
        pid
    );
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn cleanup_timeout_overrides_custom_error_without_releasing_a_busy_lease() {
    let fixture = Fixture::new().await;
    let context = Arc::clone(&fixture.context);
    let repository = fixture.repository();
    let (started, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        let queries = Arc::clone(&context);
        context
            .transaction(
                move |_| async move {
                    repository.insert(1).await?;
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
                    Err::<(), _>(WorkflowError::Rejected(rejection()))
                },
                None,
            )
            .await
    });
    let retained = within(ready).await.unwrap();
    assert_eq!(
        within(task).await.unwrap(),
        Err(WorkflowError::Database(PgError::TransactionCleanupTimeout))
    );
    let status = fixture.database.status().unwrap();
    assert_eq!((status.in_flight, status.available, status.size), (1, 0, 1));
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    assert_closed(&fixture, PgError::TransactionCleanupTimeout).await;
    drop(retained);
    fixture.idle().await;
    assert_released(&fixture, 0);
    assert_eq!(fixture.ids().await, Vec::<i64>::new());
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn panicking_error_conversion_cannot_interrupt_rollback_or_poison_context() {
    struct PanickingConversion;
    impl From<PgError> for PanickingConversion {
        fn from(error: PgError) -> Self {
            assert_eq!(error, conflict());
            panic!("intentional From<PgError> panic");
        }
    }
    let fixture = Fixture::new().await;
    let pid = fixture.repository().insert(1).await.unwrap();
    let repository = fixture.repository();
    let result = std::panic::AssertUnwindSafe(
        fixture
            .context
            .transaction::<(), PanickingConversion, _, _>(
                move |_| async move {
                    // The framework must convert the swallowed query error only AFTER rollback.
                    assert_eq!(repository.insert(2).await.unwrap(), pid);
                    assert_eq!(repository.insert(2).await, Err(conflict()));
                    Ok(())
                },
                None,
            ),
    )
    .catch_unwind()
    .await;
    let panic = result.err().expect("the application conversion must run");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"intentional From<PgError> panic")
    );
    assert_eq!(fixture.ids().await, vec![1]);
    assert_released(&fixture, 1);
    assert_eq!(fixture.repository().insert(3).await.unwrap(), pid);
    assert_eq!(fixture.ids().await, vec![1, 3]);
    fixture.finish().await;
}
