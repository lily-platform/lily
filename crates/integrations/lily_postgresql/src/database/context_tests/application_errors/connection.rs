//! Generic connection callbacks retain payloads and the rollback obligation.

use super::*;

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn custom_callback_preserves_payload_and_releases_the_pool() {
    let fixture = Fixture::new().await;
    let mut invoked = false;
    let success: Result<Identity, WorkflowError> = fixture
        .context
        .with_connection(
            |connection, _| {
                invoked = true;
                Box::pin(async move { Ok(identity(connection).await?) })
            },
            None,
        )
        .await;
    let pid = success.unwrap().pid;
    assert!(invoked, "callbacks may borrow local state");
    assert_released(&fixture, 1);

    let payload = rejection();
    let address = (&*payload as *const Rejection) as usize;
    let sql = format!("INSERT INTO {}.orders VALUES (1)", fixture.schema);
    let result: Result<(), WorkflowError> = fixture
        .context
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    assert_eq!(identity(connection).await?.pid, pid);
                    diesel::sql_query(sql)
                        .execute(connection)
                        .await
                        .map_err(PgError::from)?;
                    Err(WorkflowError::Rejected(payload))
                })
            },
            None,
        )
        .await;
    let Err(WorkflowError::Rejected(returned)) = result else {
        panic!("callback must return its original application error");
    };
    assert_eq!((&*returned as *const Rejection) as usize, address);
    assert_eq!(returned, rejection());
    assert_eq!(fixture.ids().await, vec![1], "ordinary SQL autocommits");
    assert_released(&fixture, 1);

    let sql = format!("INSERT INTO {}.orders VALUES (1)", fixture.schema);
    let duplicate: Result<(), WorkflowError> = fixture
        .context
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    diesel::sql_query(sql)
                        .execute(connection)
                        .await
                        .map_err(PgError::from)?;
                    Ok(())
                })
            },
            None,
        )
        .await;
    assert_eq!(duplicate, Err(WorkflowError::Database(conflict())));
    assert_released(&fixture, 1);
    assert_eq!(fixture.repository().insert(2).await.unwrap(), pid);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn propagated_mapped_and_swallowed_custom_callbacks_all_roll_back() {
    let fixture = Fixture::new().await;
    let pid = fixture.repository().insert(1).await.unwrap();
    for sql_failure in [false, true] {
        for handling in 0..3 {
            let queries = Arc::clone(&fixture.context);
            let sql = format!("INSERT INTO {}.orders VALUES (2)", fixture.schema);
            let payload = rejection();
            let address = (&*payload as *const Rejection) as usize;
            let result: Result<(), WorkflowError> = fixture
                .context
                .transaction(
                    move |_| async move {
                        let result: Result<(), WorkflowError> = queries
                            .with_connection(
                                |connection, _| {
                                    Box::pin(async move {
                                        assert_eq!(identity(connection).await?.pid, pid);
                                        diesel::sql_query(&sql)
                                            .execute(connection)
                                            .await
                                            .map_err(PgError::from)?;
                                        if sql_failure {
                                            diesel::sql_query(sql)
                                                .execute(connection)
                                                .await
                                                .map_err(PgError::from)?;
                                        }
                                        Err(WorkflowError::Rejected(payload))
                                    })
                                },
                                None,
                            )
                            .await;
                        if sql_failure {
                            assert_eq!(result, Err(WorkflowError::Database(conflict())));
                        } else {
                            assert_eq!(result, Err(WorkflowError::Rejected(rejection())));
                        }
                        // A different callback error type still observes the
                        // rollback obligation without borrowing the original E.
                        let rejected: PgResult<()> = queries
                            .with_connection(|_, _| panic!("rollback-only callback entered"), None)
                            .await;
                        assert_eq!(rejected, Err(PgError::TransactionRollbackOnly));
                        match handling {
                            0 => result?,
                            1 => result.map_err(|_| WorkflowError::Rejected(rejection()))?,
                            2 => {}
                            _ => unreachable!(),
                        }
                        Ok(())
                    },
                    None,
                )
                .await;
            if handling == 2 {
                assert_eq!(
                    result,
                    Err(WorkflowError::Database(PgError::TransactionRollbackOnly))
                );
            } else if sql_failure && handling == 0 {
                assert_eq!(result, Err(WorkflowError::Database(conflict())));
            } else {
                let Err(WorkflowError::Rejected(returned)) = result else {
                    panic!("explicit application error must win after rollback");
                };
                if handling == 0 {
                    assert_eq!((&*returned as *const Rejection) as usize, address);
                }
                assert_eq!(returned, rejection());
            }
            assert_eq!(fixture.ids().await, vec![1]);
            assert_released(&fixture, 1);
        }
    }
    assert_eq!(fixture.repository().insert(3).await.unwrap(), pid);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn connection_acquisition_errors_convert_and_release_admission() {
    let fixture = Fixture::new().await;
    let lease = fixture.database.acquire_connection(None).await.unwrap();
    let result: Result<(), WorkflowError> = within(
        fixture
            .context
            .with_connection(|_, _| panic!("occupied pool must prevent callback"), None),
    )
    .await;
    assert_eq!(
        result,
        Err(WorkflowError::Database(PgError::PoolTimeout {
            phase: PgPoolTimeoutPhase::Acquire,
        }))
    );
    let status = fixture.database.status().unwrap();
    assert_eq!(
        (status.in_flight, status.waiting, status.available),
        (1, 0, 0)
    );
    drop(lease);
    fixture.repository().insert(1).await.unwrap();
    assert_released(&fixture, 1);
    fixture.database.close().await.unwrap();
    let result: Result<(), WorkflowError> = fixture
        .context
        .with_connection(|_, _| panic!("closed pool must prevent callback"), None)
        .await;
    assert_eq!(result, Err(WorkflowError::Database(PgError::PoolClosed)));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn cancellation_and_disposal_convert_after_discarding_the_connection() {
    for dispose in [false, true] {
        let fixture = Fixture::new().await;
        fixture.lock_gate().await;
        let context = Arc::clone(&fixture.context);
        let source = ExecutionCancellationSource::default();
        let token = source.view();
        let sql = format!("INSERT INTO {}.orders VALUES (1)", fixture.schema);
        let gate = format!("SELECT pg_advisory_xact_lock({}, 1)", fixture.observer_pid);
        let (started, ready) = oneshot::channel();
        let task = tokio::spawn(async move {
            context
                .with_connection::<(), WorkflowError, _>(
                    |connection, selected| {
                        Box::pin(async move {
                            assert!(!selected.unwrap().is_cancelled());
                            diesel::sql_query(sql)
                                .execute(connection)
                                .await
                                .map_err(PgError::from)?;
                            started.send(identity(connection).await?.pid).unwrap();
                            diesel::sql_query(gate)
                                .execute(connection)
                                .await
                                .map_err(PgError::from)?;
                            Ok(())
                        })
                    },
                    Some(token),
                )
                .await
        });
        let pid = within(ready).await.unwrap();
        fixture.wait_gate(pid).await;
        let expected = if dispose {
            within(fixture.context.dispose()).await.unwrap();
            PgError::ContextClosed
        } else {
            source.cancel();
            PgError::OperationCancelled
        };
        assert_eq!(
            within(task).await.unwrap(),
            Err(WorkflowError::Database(expected))
        );
        assert_released(&fixture, 0);
        assert_eq!(fixture.ids().await, vec![1]);
        fixture.unlock_gate().await;
        if !dispose {
            assert_ne!(fixture.repository().insert(2).await.unwrap(), pid);
        }
        fixture.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn synchronous_and_async_callback_panics_convert_without_poisoning_context() {
    let fixture = Fixture::new().await;
    for synchronous in [false, true] {
        let result: Result<(), WorkflowError> = fixture
            .context
            .with_connection(
                |_, _| {
                    assert!(!synchronous, "intentional synchronous callback panic");
                    Box::pin(async { panic!("intentional async callback panic") })
                },
                None,
            )
            .await;
        assert_eq!(
            result,
            Err(WorkflowError::Database(PgError::OperationPanicked))
        );
        assert_released(&fixture, 0);
    }
    fixture.repository().insert(1).await.unwrap();
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires a dedicated LILY_PG_TRANSACTION_TEST_URL"]
async fn panicking_connection_conversion_runs_after_releasing_lease_and_admission() {
    struct PanickingConversion;
    impl From<PgError> for PanickingConversion {
        fn from(error: PgError) -> Self {
            assert!(matches!(
                error,
                PgError::OperationPanicked
                    | PgError::PoolTimeout {
                        phase: PgPoolTimeoutPhase::Acquire,
                    }
            ));
            panic!("intentional connection conversion panic");
        }
    }
    let fixture = Fixture::new().await;
    for occupied in [false, true] {
        let lease = if occupied {
            Some(fixture.database.acquire_connection(None).await.unwrap())
        } else {
            None
        };
        let result = std::panic::AssertUnwindSafe(
            fixture
                .context
                .with_connection::<(), PanickingConversion, _>(
                    |_, _| panic!("intentional callback panic"),
                    None,
                ),
        )
        .catch_unwind()
        .await;
        let panic = result.err().expect("application conversion must panic");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"intentional connection conversion panic")
        );
        if occupied {
            assert_eq!(fixture.database.status().unwrap().in_flight, 1);
        } else {
            assert_released(&fixture, 0);
        }
        drop(lease);
        fixture
            .repository()
            .insert(i64::from(occupied) + 1)
            .await
            .unwrap();
        assert_released(&fixture, 1);
    }
    assert_eq!(fixture.ids().await, vec![1, 2]);
    fixture.finish().await;
}
