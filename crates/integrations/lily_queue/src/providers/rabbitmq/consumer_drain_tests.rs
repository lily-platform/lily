//! Subscription barriers exercised with the real dispatcher and real task joins.
use super::propagation_tests::{canonical_delivery, started_lifecycle_engine};
use super::*;
use std::sync::atomic::AtomicUsize;

struct GenerationProbe {
    cancel_started: CancellationToken,
    close_count: Arc<AtomicUsize>,
    evidence: Arc<QueuePairEvidence>,
}

async fn install_generation(
    engine: &RabbitMQQueueEngine,
    handler: QueueDeliveryHandler,
    deliveries: usize,
    cancel_confirmed: bool,
    close_error: Option<&'static str>,
) -> GenerationProbe {
    install_named_generation(
        engine,
        "queue",
        handler,
        deliveries,
        1,
        cancel_confirmed,
        close_error,
    )
    .await
}

async fn install_named_generation(
    engine: &RabbitMQQueueEngine,
    queue: &'static str,
    handler: QueueDeliveryHandler,
    deliveries: usize,
    concurrency: usize,
    cancel_confirmed: bool,
    close_error: Option<&'static str>,
) -> GenerationProbe {
    let tokens = engine.runtime_tokens.lock().await.clone().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(deliveries.max(1));
    for _ in 0..deliveries {
        RabbitMQQueueEngine::materialize_and_buffer_delivery(
            "worker",
            queue,
            canonical_delivery("exchange", "route"),
            &tx,
            &tokens.settlement,
            &engine.retry_engine,
            &engine.terminal_ledger,
            0,
            Duration::from_secs(10),
            Duration::from_secs(10),
            &engine.shutdown_budget,
        )
        .await
        .unwrap();
    }
    let evidence = Arc::new(QueuePairEvidence::default());
    let ownership = Arc::new(ConsumerChannelOwnership::default());
    ownership.track_pair(evidence.clone());
    engine
        .channel_owners
        .insert(("worker".into(), queue.into()), ownership);
    let dispatcher = engine.dispatch_loop(
        "worker",
        queue,
        handler,
        rx,
        tokens.admission.clone(),
        tokens.force.clone(),
        tokens.settlement.clone(),
        DispatchPolicy {
            handler_concurrency: concurrency,
            retry_attempts: 0,
            delivery_execution_timeout: Duration::from_secs(30),
            handoff_timeout: Duration::from_secs(10),
            settlement_timeout: Duration::from_secs(10),
        },
    );
    let receiver_admission = tokens.admission.clone();
    let pair = supervise_queue_pair(
        queue.into(),
        tokens.admission.clone(),
        tokens.force,
        engine.execution.clone(),
        engine.terminal_ledger.clone(),
        async move {
            // Retain the sender just as the subscription receiver does.
            receiver_admission.cancelled().await;
            drop(tx);
            Ok(())
        },
        dispatcher,
        evidence.clone(),
    );
    let cancel_started = CancellationToken::new();
    let cancel_probe = cancel_started.clone();
    let close_count = Arc::new(AtomicUsize::new(0));
    let close_probe = close_count.clone();
    let owner_evidence = evidence.clone();
    let ledger = engine.terminal_ledger.clone();
    let owner = async move {
        finish_consumer_generation(
            pair,
            async move {
                tokens.admission.cancelled().await;
                cancel_probe.cancel();
                cancel_confirmed
            },
            async move {
                assert!(
                    ledger.snapshot().is_reconciled(),
                    "delivery task evidence before close"
                );
                close_probe.fetch_add(1, Ordering::SeqCst);
                close_error.map_or(Ok(()), |code| {
                    Err(RabbitMqConsumerTaskFailure {
                        queue: queue.into(),
                        role: RabbitMqConsumerTaskRole::Receiver,
                        kind: RabbitMqConsumerTaskFailureKind::OperationFailed,
                        operation_error_code: Some(code),
                    })
                })
            },
            &owner_evidence,
        )
        .await
    };
    adopt_supervised_queue_task(
        &mut *engine.background_tasks.lock().await,
        queue.into(),
        owner,
    );
    GenerationProbe {
        cancel_started,
        close_count,
        evidence,
    }
}

#[tokio::test]
async fn graceful_cancel_does_not_close_channel_before_accepted_ack_and_buffer_release() {
    let engine = started_lifecycle_engine().await;
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let handler: QueueDeliveryHandler = {
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move |input| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.cancel();
                release.cancelled().await;
                assert!(!input.cancellation.is_cancelled());
                Ok(())
            })
        })
    };
    let probe = install_generation(&engine, handler, 2, true, None).await;
    entered.cancelled().await;
    engine.stop_admission().await.unwrap();
    probe.cancel_started.cancelled().await;
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 0);
    assert_eq!(engine.delivery_terminal_snapshot().acked_handler_success, 0);
    assert!(!probe.evidence.reconciled.load(Ordering::Acquire));
    release.cancel();
    engine.drain().await.unwrap();
    let snapshot = engine.delivery_terminal_snapshot();
    assert_eq!(snapshot.deliveries, 2);
    assert_eq!(snapshot.acked_handler_success, 1);
    assert_eq!(snapshot.buffered_pending_redelivery, 1);
    assert_eq!(snapshot.unacked_or_in_flight(), 0);
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
    assert!(probe.evidence.reconciled.load(Ordering::Acquire));
    assert!(engine.drain_reconciled());
}

#[tokio::test(start_paused = true)]
async fn force_preserves_settlement_until_cooperative_success_has_been_acked_and_joined() {
    let engine = started_lifecycle_engine().await;
    let settlement = engine
        .runtime_tokens
        .lock()
        .await
        .as_ref()
        .unwrap()
        .settlement
        .clone();
    let entered = CancellationToken::new();
    let handler: QueueDeliveryHandler = {
        let entered = entered.clone();
        let settlement = settlement.clone();
        Arc::new(move |input| {
            let entered = entered.clone();
            let settlement = settlement.clone();
            Box::pin(async move {
                entered.cancel();
                input.cancellation.cancelled().await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert!(
                    !settlement.is_cancelled(),
                    "force notification cannot revoke ACK authority"
                );
                Ok(())
            })
        })
    };
    let probe = install_generation(&engine, handler, 1, true, None).await;
    entered.cancelled().await;
    let started = tokio::time::Instant::now();
    engine
        .shutdown_budget
        .install(QueueShutdownDeadlines::before(
            started,
            started + Duration::from_secs(2),
        ));
    engine.force_drain().await.unwrap();
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_millis(50)
    );
    assert_eq!(engine.delivery_terminal_snapshot().acked_handler_success, 1);
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
    assert!(probe.evidence.reconciled.load(Ordering::Acquire));
    assert!(
        settlement.is_cancelled(),
        "final authority retirement follows joins"
    );
}

#[tokio::test(start_paused = true)]
async fn returned_framework_interruption_is_never_acked_and_only_force_records_pending_redelivery()
{
    for forced in [false, true] {
        let engine = started_lifecycle_engine().await;
        let entered = CancellationToken::new();
        let release = CancellationToken::new();
        let handler: QueueDeliveryHandler = {
            let entered = entered.clone();
            let release = release.clone();
            Arc::new(move |input| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.cancel();
                    if forced {
                        input.cancellation.cancelled().await;
                        assert_eq!(
                            input.cancellation.reason(),
                            Some(DeliveryCancellationReason::ForcedShutdown)
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    } else {
                        release.cancelled().await;
                        assert!(!input.cancellation.is_cancelled());
                    }
                    Err(QueueExecutionError::framework_cancelled(
                        "QUEUE_DELIVERY_INTERRUPTED",
                    ))
                })
            })
        };
        let probe = install_generation(&engine, handler, 1, true, None).await;
        entered.cancelled().await;
        let began = tokio::time::Instant::now();
        if forced {
            engine
                .shutdown_budget
                .install(QueueShutdownDeadlines::before(
                    began + Duration::from_secs(1),
                    began + Duration::from_secs(2),
                ));
            engine.force_drain().await.unwrap();
            assert_eq!(
                tokio::time::Instant::now() - began,
                Duration::from_millis(20)
            );
        } else {
            release.cancel();
            engine.drain().await.unwrap();
        }
        let snapshot = engine.delivery_terminal_snapshot();
        assert_eq!(snapshot.deliveries, 1);
        assert_eq!(snapshot.handler_success, 0);
        assert_eq!(snapshot.handler_failure, 0);
        assert_eq!(snapshot.handler_panic, 0);
        assert_eq!(snapshot.acked_handler_success, 0);
        assert_eq!(snapshot.acked_confirmed_handoff, 0);
        assert_eq!(snapshot.retry_confirmed, 0);
        assert_eq!(snapshot.dead_letter_confirmed, 0);
        assert_eq!(snapshot.nacked_or_requeued, 0);
        assert_eq!(snapshot.buffered_pending_redelivery, u64::from(forced));
        assert_eq!(snapshot.unresolved, u64::from(!forced));
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.is_reconciled());
        assert!(engine.background_tasks.lock().await.is_empty());
        assert!(engine.drain_reconciled());
        assert!(probe.evidence.reconciled.load(Ordering::Acquire));
        assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
        let observations = engine.delivery_terminal_observations();
        assert_eq!(observations.observations.len(), 1);
        assert_eq!(observations.dropped, 0);
        assert_eq!(
            observations.observations[0].outcome,
            if forced {
                crate::DeliveryTerminalOutcome::BufferedPendingRedelivery
            } else {
                crate::DeliveryTerminalOutcome::Unresolved
            }
        );
    }
}

#[tokio::test(start_paused = true)]
async fn noncooperative_execution_is_aborted_and_joined_before_channel_close() {
    struct Witness {
        settlement: CancellationToken,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for Witness {
        fn drop(&mut self) {
            assert!(
                self.settlement.is_cancelled(),
                "final stop must revoke settlement first"
            );
            self.dropped.store(true, Ordering::Release);
        }
    }
    let engine = started_lifecycle_engine().await;
    let settlement = engine
        .runtime_tokens
        .lock()
        .await
        .as_ref()
        .unwrap()
        .settlement
        .clone();
    let entered = CancellationToken::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let handler: QueueDeliveryHandler = {
        let entered = entered.clone();
        let dropped = dropped.clone();
        Arc::new(move |_| {
            let witness = Witness {
                settlement: settlement.clone(),
                dropped: dropped.clone(),
            };
            let entered = entered.clone();
            Box::pin(async move {
                let _witness = witness;
                entered.cancel();
                std::future::pending().await
            })
        })
    };
    let probe = install_generation(&engine, handler, 1, false, None).await;
    entered.cancelled().await;
    let now = tokio::time::Instant::now();
    engine
        .shutdown_budget
        .install(QueueShutdownDeadlines::before(
            now,
            now + Duration::from_secs(2),
        ));
    engine.force_drain().await.unwrap();
    assert_eq!(tokio::time::Instant::now() - now, Duration::from_secs(1));
    assert!(dropped.load(Ordering::Acquire));
    assert!(probe.evidence.reconciled.load(Ordering::Acquire));
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
    assert_eq!(engine.delivery_terminal_snapshot().acked_handler_success, 0);
    assert!(engine.background_tasks.lock().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn concurrent_queue_generations_share_one_force_budget_and_account_for_every_delivery() {
    struct ExecutionWitness(Arc<AtomicUsize>);
    impl Drop for ExecutionWitness {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let engine = started_lifecycle_engine().await;
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut probes = Vec::new();
    let mut started_counts = Vec::new();
    for queue in ["payments", "inventory", "notifications"] {
        let started = Arc::new(AtomicUsize::new(0));
        started_counts.push(started.clone());
        let entered = entered_tx.clone();
        let dropped = dropped.clone();
        let handler: QueueDeliveryHandler = Arc::new(move |input| {
            let ordinal = started.fetch_add(1, Ordering::SeqCst);
            let entered = entered.clone();
            let witness = ExecutionWitness(dropped.clone());
            Box::pin(async move {
                let _witness = witness;
                entered.send((queue, ordinal)).unwrap();
                input.cancellation.cancelled().await;
                assert_eq!(
                    input.cancellation.reason(),
                    Some(DeliveryCancellationReason::ForcedShutdown)
                );
                match ordinal {
                    0 => tokio::time::sleep(Duration::from_millis(25)).await,
                    1 => tokio::time::sleep(Duration::from_millis(75)).await,
                    2 => std::future::pending::<()>().await,
                    _ => panic!("buffered work cannot enter execution after force"),
                }
                Ok(())
            })
        });
        probes.push(install_named_generation(&engine, queue, handler, 5, 3, true, None).await);
    }
    drop(entered_tx);
    let mut entered = std::collections::BTreeSet::new();
    for _ in 0..9 {
        assert!(entered.insert(entered_rx.recv().await.unwrap()));
    }
    for queue in ["payments", "inventory", "notifications"] {
        for ordinal in 0..3 {
            assert!(entered.contains(&(queue, ordinal)));
        }
    }
    assert_eq!(engine.delivery_terminal_snapshot().deliveries, 15);
    assert_eq!(engine.delivery_terminal_snapshot().in_flight, 15);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(engine.background_tasks.lock().await.len(), 3);

    let began = tokio::time::Instant::now();
    engine
        .shutdown_budget
        .install(QueueShutdownDeadlines::before(
            began + Duration::from_secs(1),
            began + Duration::from_secs(2),
        ));
    engine.force_drain().await.unwrap();

    assert_eq!(
        tokio::time::Instant::now() - began,
        Duration::from_secs(1),
        "three queue owners must not allocate three successive force windows"
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 9);
    assert!(
        started_counts
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 3)
    );
    let snapshot = engine.delivery_terminal_snapshot();
    assert_eq!(snapshot.deliveries, 15);
    assert_eq!(snapshot.handler_success, 6);
    assert_eq!(snapshot.acked_handler_success, 6);
    assert_eq!(snapshot.acked_confirmed_handoff, 0);
    assert_eq!(snapshot.retry_confirmed, 0);
    assert_eq!(snapshot.dead_letter_confirmed, 0);
    assert_eq!(snapshot.nacked_or_requeued, 0);
    assert_eq!(snapshot.handler_failure, 0);
    assert_eq!(snapshot.handler_panic, 0);
    assert_eq!(snapshot.buffered_pending_redelivery, 9);
    assert_eq!(snapshot.unresolved, 0);
    assert_eq!(snapshot.in_flight, 0);
    assert!(snapshot.is_reconciled());
    assert!(engine.background_tasks.lock().await.is_empty());
    assert!(engine.drain_reconciled());
    for probe in probes {
        assert!(probe.cancel_started.is_cancelled());
        assert!(probe.evidence.reconciled.load(Ordering::Acquire));
        assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancelled_drain_waiter_retains_generation_owner_and_does_not_close_early() {
    let engine = Arc::new(started_lifecycle_engine().await);
    let entered = CancellationToken::new();
    let release = CancellationToken::new();
    let handler: QueueDeliveryHandler = {
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move |_| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.cancel();
                release.cancelled().await;
                Ok(())
            })
        })
    };
    let probe = install_generation(&engine, handler, 1, true, None).await;
    entered.cancelled().await;
    let drain_engine = engine.clone();
    let waiter = tokio::spawn(async move { drain_engine.drain().await });
    probe.cancel_started.cancelled().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(engine.background_tasks.lock().await.len(), 1);
    assert_eq!(engine.channel_owners.len(), 1);
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 0);
    release.cancel();
    engine.drain().await.unwrap();
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
    assert_eq!(engine.delivery_terminal_snapshot().acked_handler_success, 1);
}

#[tokio::test(start_paused = true)]
async fn cancelled_force_waiter_keeps_original_children_and_cannot_restart_their_deadline() {
    let engine = Arc::new(started_lifecycle_engine().await);
    let entered = CancellationToken::new();
    let cancellation_seen = CancellationToken::new();
    let handler: QueueDeliveryHandler = {
        let entered = entered.clone();
        let cancellation_seen = cancellation_seen.clone();
        Arc::new(move |input| {
            let entered = entered.clone();
            let cancellation_seen = cancellation_seen.clone();
            Box::pin(async move {
                entered.cancel();
                input.cancellation.cancelled().await;
                assert_eq!(
                    input.cancellation.reason(),
                    Some(DeliveryCancellationReason::ForcedShutdown)
                );
                cancellation_seen.cancel();
                std::future::pending().await
            })
        })
    };
    let probe = install_generation(&engine, handler, 1, true, None).await;
    entered.cancelled().await;
    let began = tokio::time::Instant::now();
    engine
        .shutdown_budget
        .install(QueueShutdownDeadlines::before(
            began + Duration::from_secs(1),
            began + Duration::from_secs(2),
        ));
    let waiter_engine = engine.clone();
    let waiter = tokio::spawn(async move { waiter_engine.force_drain().await });
    cancellation_seen.cancelled().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(engine.background_tasks.lock().await.len(), 1);
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 0);
    assert!(!probe.evidence.reconciled.load(Ordering::Acquire));
    assert!(!engine.drain_reconciled());

    tokio::time::advance(Duration::from_millis(400)).await;
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 0);
    engine.force_drain().await.unwrap();
    assert_eq!(tokio::time::Instant::now() - began, Duration::from_secs(1));
    assert!(engine.background_tasks.lock().await.is_empty());
    assert!(probe.evidence.reconciled.load(Ordering::Acquire));
    assert!(engine.drain_reconciled());
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
    let snapshot = engine.delivery_terminal_snapshot();
    assert_eq!(snapshot.deliveries, 1);
    assert_eq!(snapshot.in_flight, 0);
    assert_eq!(snapshot.buffered_pending_redelivery, 1);
    assert_eq!(snapshot.unresolved, 0);
    assert_eq!(snapshot.acked_handler_success, 0);
    assert_eq!(snapshot.acked_confirmed_handoff, 0);
    assert_eq!(snapshot.nacked_or_requeued, 0);
}

#[tokio::test]
async fn first_receiver_error_cannot_hide_later_dispatcher_panic_or_open_parent_barrier() {
    let engine = started_lifecycle_engine().await;
    let tokens = engine.runtime_tokens.lock().await.clone().unwrap();
    let evidence = Arc::new(QueuePairEvidence::default());
    let owner = Arc::new(ConsumerChannelOwnership::default());
    owner.track_pair(evidence.clone());
    engine
        .channel_owners
        .insert(("worker".into(), "queue".into()), owner);
    let admission = tokens.admission.clone();
    let pair = supervise_queue_pair(
        "queue".into(),
        tokens.admission,
        tokens.force,
        engine.execution.clone(),
        engine.terminal_ledger.clone(),
        async { Err("RECEIVER_FAILED_FIRST") },
        async move {
            admission.cancelled().await;
            panic!("dispatcher panics after the primary failure");
        },
        evidence.clone(),
    );
    let close_count = Arc::new(AtomicUsize::new(0));
    let count = close_count.clone();
    adopt_supervised_queue_task(
        &mut *engine.background_tasks.lock().await,
        "queue".into(),
        async move {
            finish_consumer_generation(
                pair,
                async { true },
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                &evidence,
            )
            .await
        },
    );
    let error = engine.drain_to_terminal().await.unwrap_err();
    let MessageBrokerError::RabbitMQError(RabbitMQError::ConsumerTaskFailed(failure)) = error
    else {
        panic!("typed failure required")
    };
    assert_eq!(failure.operation_error_code, Some("RECEIVER_FAILED_FIRST"));
    assert!(!engine.drain_reconciled());
    assert_eq!(close_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn channel_close_failure_is_reported_even_when_every_child_was_joined() {
    let engine = started_lifecycle_engine().await;
    let handler: QueueDeliveryHandler = Arc::new(|_| Box::pin(async { Ok(()) }));
    let probe = install_generation(&engine, handler, 0, false, Some("CHANNEL_CLOSE_TIMEOUT")).await;
    let error = engine.drain().await.unwrap_err();
    let MessageBrokerError::RabbitMQError(RabbitMQError::ConsumerTaskFailed(failure)) = error
    else {
        panic!("typed failure required")
    };
    assert_eq!(failure.operation_error_code, Some("CHANNEL_CLOSE_TIMEOUT"));
    assert!(
        engine.drain_reconciled(),
        "task reconciliation differs from successful channel cleanup"
    );
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
    assert!(probe.evidence.reconciled.load(Ordering::Acquire));
}

#[tokio::test(start_paused = true)]
async fn pending_basic_cancel_runs_beside_child_drain_and_cannot_restart_the_root() {
    let now = tokio::time::Instant::now();
    let root = QueueShutdownBudget::default();
    root.install(QueueShutdownDeadlines::before(
        now,
        now + Duration::from_millis(80),
    ));
    let evidence = Arc::new(QueuePairEvidence::default());
    let admission = CancellationToken::new();
    admission.cancel();
    let pair = supervise_queue_pair(
        "queue".into(),
        admission,
        CancellationToken::new(),
        DeliveryCancellationSource::new(),
        Arc::new(DeliveryTerminalLedger::default()),
        async { Ok(()) },
        async { Ok(()) },
        evidence.clone(),
    );
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            evidence.reconciled.load(Ordering::Acquire),
            "Cancel-Ok cannot block child joins"
        );
        root.run_until(now + Duration::from_secs(30), std::future::pending::<()>())
            .await
            .is_ok()
    };
    let closed = AtomicBool::new(false);
    finish_consumer_generation(
        pair,
        cancel,
        async {
            assert_eq!(tokio::time::Instant::now() - now, Duration::from_millis(80));
            // Model the already terminal channel: observing closure requires no I/O.
            closed.store(true, Ordering::Release);
            Ok(())
        },
        &evidence,
    )
    .await
    .unwrap();
    assert!(closed.load(Ordering::Acquire));
}

#[tokio::test]
async fn fatal_generation_failure_notifies_sibling_queues_before_channel_cleanup() {
    let admission = CancellationToken::new();
    let force = CancellationToken::new();
    let execution = DeliveryCancellationSource::new();
    let evidence = Arc::new(QueuePairEvidence {
        reconciled: AtomicBool::new(false),
        runtime_failure: Some(QueueRuntimeFailureAuthority {
            admission: admission.clone(),
            execution: execution.clone(),
            force: force.clone(),
        }),
    });
    let generation_admission = admission.child_token();
    let generation_force = force.child_token();
    let release = CancellationToken::new();
    let dispatcher_release = release.clone();
    let task_evidence = evidence.clone();
    let pair = tokio::spawn(supervise_queue_pair(
        "failing-queue".into(),
        generation_admission,
        generation_force,
        execution.child(),
        Arc::new(DeliveryTerminalLedger::default()),
        async { Err("BROKER_ACK") },
        async move {
            dispatcher_release.cancelled().await;
            Ok(())
        },
        task_evidence,
    ));
    admission.cancelled().await;
    assert!(execution.is_cancelled());
    assert_eq!(
        execution.reason(),
        Some(DeliveryCancellationReason::RuntimeFailure)
    );
    assert!(force.is_cancelled());
    assert!(
        !pair.is_finished(),
        "sibling notification precedes pending child drain"
    );
    assert!(!evidence.reconciled.load(Ordering::Acquire));
    release.cancel();
    let failure = pair.await.unwrap().unwrap_err();
    assert_eq!(failure.operation_error_code, Some("BROKER_ACK"));
    assert!(evidence.reconciled.load(Ordering::Acquire));
}

#[tokio::test]
async fn transport_loss_stops_only_its_generation_and_replacement_requires_old_join_proof() {
    let admission = CancellationToken::new();
    let force = CancellationToken::new();
    let execution = DeliveryCancellationSource::new();
    let settlement = CancellationToken::new();
    let old_admission = admission.child_token();
    let old_force = force.child_token();
    let old_execution = execution.child();
    let old_settlement = settlement.child_token();
    let old_evidence = Arc::new(QueuePairEvidence::default());
    let ownership = ConsumerChannelOwnership::default();
    ownership.track_pair(old_evidence.clone());
    let handler_entered = CancellationToken::new();
    let release = CancellationToken::new();
    let dispatcher_entered = handler_entered.clone();
    let dispatcher_release = release.clone();
    old_admission.cancel();
    old_execution.cancel(DeliveryCancellationReason::RuntimeCancellation);
    old_settlement.cancel();
    old_force.cancel();
    let pair = tokio::spawn(supervise_queue_pair(
        "queue".into(),
        old_admission,
        old_force,
        old_execution,
        Arc::new(DeliveryTerminalLedger::default()),
        async { Ok(()) },
        async move {
            dispatcher_entered.cancel();
            dispatcher_release.cancelled().await;
            Ok(())
        },
        old_evidence,
    ));
    handler_entered.cancelled().await;
    assert!(!ownership.children_reconciled());
    assert!(!execution.is_cancelled());
    assert!(!admission.is_cancelled());
    assert!(!force.is_cancelled());
    assert!(!settlement.is_cancelled());
    release.cancel();
    pair.await.unwrap().unwrap();
    assert!(ownership.children_reconciled());
    ownership.track_pair(Arc::new(QueuePairEvidence::default()));
    assert!(
        !ownership.children_reconciled(),
        "each replacement needs fresh join evidence"
    );
    assert!(!execution.child().is_cancelled());
    assert!(!settlement.child_token().is_cancelled());
}
