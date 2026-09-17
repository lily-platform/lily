#![cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]

//! Environment-qualified CAP-08-1 proof against a dedicated MongoDB replica set.
//!
//! The fixture is destructive and deliberately ignored. It requires a replica
//! set started with MongoDB test commands enabled so the transaction-label
//! fault matrix can use the server's `failCommand` failpoint. Runtime startup
//! remains read-only; this target applies Lily's migration explicitly.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use lily_mongo_repository::{MongoOperationContext, MongoRepositoryError};
use lily_config::{
    DatabaseConfig, MongoTransactionalInboxConfig, TransactionalInboxBackend,
    TransactionalInboxConfig,
};
use lily_injection::ServiceTrait;
use lily_mongodb::{
    Collection, DatabaseService, MongoClientPlan, MongoCollection, MongoMigration,
    MongoMigrationRunner,
    bson::{Document, doc, oid::ObjectId},
};
use lily_queue::__private::{MongoTransactionalRuntime, TransactionalExecution};
use lily_queue::{
    MongoInboxOutboxMigrator, MongoReliabilityError, PublishContentKind, QueueHandlerError,
    TransactionalOutboxMessage,
};
use mongodb::{Client, IndexModel, options::IndexOptions};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DATABASE_NAME: &str = "lily_cap081_qualification";
const BUSINESS_COLLECTION: &str = "cap081_qualification_effects";
const APPLICATION_MIGRATION_COMPONENT: &str = "lily.cap081.qualification";
const BUSINESS_MIGRATION_VERSION: i64 = 8_201_001;

const FRAMEWORK_COLLECTIONS: &[&str] = &[
    "_lily_queue_transactional_schema",
    "_lily_queue_inbox",
    "_lily_queue_inbox_leases",
    "_lily_queue_outbox",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QualificationEffect {
    #[serde(skip_serializing_if = "Option::is_none")]
    _id: Option<ObjectId>,
    event_id: String,
    marker: String,
    _revision: i64,
}

#[derive(MongoCollection)]
#[collection("cap081_qualification_effects")]
#[collection_type(QualificationEffect)]
#[unique_field("event_id")]
struct QualificationEffects {
    db: Arc<DatabaseService>,
    collection: Option<Collection<QualificationEffect>>,
}

fn database_config(uri: String) -> DatabaseConfig {
    DatabaseConfig {
        mode: Some("single".into()),
        database_type: Some("mongodb".into()),
        connection_string: Some(uri),
        pool_size: Some(8),
        connection_timeout_secs: Some(10),
        query_timeout_secs: Some(30),
        pooling_enabled: Some(true),
        database_name: Some(DATABASE_NAME.into()),
        app_name: Some("lily-cap081-qualification".into()),
        ..DatabaseConfig::default()
    }
}

fn policy() -> TransactionalInboxConfig {
    TransactionalInboxConfig {
        backend: TransactionalInboxBackend::MongoDb,
        inbox_lock_timeout_millis: 2_000,
        outbox_claim_lease_millis: 2_000,
        relay_publish_timeout_millis: 1_000,
        relay_poll_interval_millis: 25,
        shutdown_drain_timeout_millis: 2_000,
        mongodb: Some(MongoTransactionalInboxConfig {
            max_transaction_attempts: 3,
            retry_initial_backoff_millis: 1,
            retry_max_backoff_millis: 5,
            commit_retry_timeout_millis: 2_000,
        }),
        ..TransactionalInboxConfig::default()
    }
}

fn operation(database: &DatabaseService) -> MongoOperationContext<'static> {
    database
        .operation_context(CancellationToken::new())
        .expect("ready MongoDB operation context")
}

async fn reset_fixture(observer: &Client) {
    observer
        .database(DATABASE_NAME)
        .drop()
        .await
        .expect("drop dedicated CAP-08-1 database");
}

async fn install_application_schema(database: &DatabaseService) {
    let migration = MongoMigration::new(
        BUSINESS_MIGRATION_VERSION,
        "install CAP-08-1 qualification business collection",
        QualificationEffects::migration_steps().expect("business migration steps"),
    )
    .expect("business migration contract");
    MongoMigrationRunner::for_component(database, APPLICATION_MIGRATION_COMPONENT, vec![migration])
        .expect("business migration runner")
        .apply(&operation(database))
        .await
        .expect("explicit business migration");
}

async fn initialized_effects(database: Arc<DatabaseService>) -> Arc<QualificationEffects> {
    let mut effects = QualificationEffects {
        db: database,
        collection: None,
    };
    effects
        .initialize()
        .await
        .expect("initialize migrated business collection handle");
    Arc::new(effects)
}

async fn effect_count(database: &DatabaseService, event_id: Uuid) -> u64 {
    database
        .collection::<Document>(BUSINESS_COLLECTION)
        .expect("business collection")
        .count_documents(doc! { "event_id": event_id.to_string() })
        .await
        .expect("count committed business effects")
}

async fn outbox_event_count(database: &DatabaseService, event_id: Uuid) -> u64 {
    database
        .collection::<Document>("_lily_queue_outbox")
        .expect("framework outbox collection")
        .count_documents(doc! { "event_id": event_id.to_string() })
        .await
        .expect("count durable outbox events")
}

async fn configure_fail_command(
    observer: &Client,
    command: &str,
    error_code: i32,
    labels: &[&str],
    times: i32,
    namespace: Option<&str>,
) {
    let mut data = doc! {
        "failCommands": [command],
        "errorCode": error_code,
        "errorLabels": labels,
    };
    if let Some(namespace) = namespace {
        data.insert("namespace", namespace);
    }
    observer
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": { "times": times },
            "data": data,
        })
        .await
        .expect("fixture must allow MongoDB failCommand");
}

async fn configure_blocked_command(observer: &Client, command: &str, times: i32, millis: i32) {
    observer
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": { "times": times },
            "data": {
                "failCommands": [command],
                "blockConnection": true,
                "blockTimeMS": millis,
            },
        })
        .await
        .expect("fixture must allow delayed MongoDB commands");
}

async fn disable_fail_command(observer: &Client) {
    observer
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": "off",
        })
        .await
        .expect("disable MongoDB failCommand");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires LILY_CAP081_DISPOSABLE=1 and LILY_CAP081_MONGODB_URL for a dedicated replica set with enableTestCommands=1"]
async fn mongodb_transactional_inbox_outbox_fault_and_lifecycle_matrix() {
    assert_eq!(
        std::env::var("LILY_CAP081_DISPOSABLE").as_deref(),
        Ok("1"),
        "refusing destructive qualification without explicit disposable-fixture opt-in"
    );
    let uri = std::env::var("LILY_CAP081_MONGODB_URL")
        .expect("LILY_CAP081_MONGODB_URL must identify a dedicated replica set");
    let observer = Client::with_uri_str(&uri)
        .await
        .expect("connect MongoDB qualification observer");
    disable_fail_command(&observer).await;
    reset_fixture(&observer).await;

    let database = Arc::new(
        DatabaseService::connect(
            MongoClientPlan::from_database(&database_config(uri))
                .expect("valid MongoDB qualification plan"),
        )
        .await
        .expect("connect Lily MongoDB service"),
    );
    database
        .verify_transaction_capability()
        .await
        .expect("qualification requires transaction-capable topology");

    // Listener preparation must be read-only and fail before any framework
    // collection is created implicitly.
    assert!(matches!(
        MongoTransactionalRuntime::from_database(Arc::clone(&database), policy()).await,
        Err(MongoReliabilityError::SchemaMissing)
    ));
    let names = database
        .list_collection_names(None)
        .await
        .expect("inspect missing framework schema");
    assert!(
        FRAMEWORK_COLLECTIONS
            .iter()
            .all(|collection| !names.iter().any(|name| name == collection)),
        "runtime readiness must not run migration DDL"
    );

    // A deployment interruption can leave idempotent collection steps behind
    // without recording the migration in the component ledger. Reapplying the
    // exact migration must resume safely and converge; runtime readiness must
    // not mistake the partial schema for an installed one.
    configure_fail_command(&observer, "createIndexes", 91, &[], 1, None).await;
    let partial = MongoInboxOutboxMigrator::new(&database)
        .expect("partial-failure framework migrator")
        .apply(&operation(&database))
        .await;
    disable_fail_command(&observer).await;
    assert!(partial.is_err(), "injected migration failure must surface");
    assert!(matches!(
        MongoTransactionalRuntime::from_database(Arc::clone(&database), policy()).await,
        Err(MongoReliabilityError::SchemaMissing | MongoReliabilityError::SchemaDrift)
    ));

    let migration_a = MongoInboxOutboxMigrator::new(&database).expect("first framework migrator");
    let migration_b = MongoInboxOutboxMigrator::new(&database).expect("second framework migrator");
    let operation_a = operation(&database);
    let operation_b = operation(&database);
    let (migration_a, migration_b) = tokio::join!(
        migration_a.apply(&operation_a),
        migration_b.apply(&operation_b)
    );
    assert!(
        migration_a.is_ok() || migration_b.is_ok(),
        "at least one concurrent migration owner must converge: a={migration_a:?}, b={migration_b:?}"
    );
    for result in [migration_a, migration_b] {
        assert!(
            result.is_ok() || matches!(result, Err(MongoRepositoryError::MigrationLockUnavailable)),
            "concurrent migration must either converge or lose the bounded lease: {result:?}"
        );
    }
    let idempotent = MongoInboxOutboxMigrator::new(&database)
        .expect("idempotent framework migrator")
        .apply(&operation(&database))
        .await
        .expect("idempotent framework migration");
    assert_eq!(idempotent.previously_applied(), 1);
    assert!(idempotent.newly_applied().is_empty());
    install_application_schema(&database).await;

    // Runtime admission distinguishes an unsupported old schema from a
    // future schema and never attempts an implicit repair. Restore the exact
    // supported marker before continuing with the transaction matrix.
    let schema = database
        .collection::<Document>("_lily_queue_transactional_schema")
        .expect("framework schema marker collection");
    schema
        .update_one(
            doc! { "_id": "lily.queue.transactional_inbox" },
            doc! { "$set": { "version": 0_i64 } },
        )
        .await
        .expect("install old schema marker");
    assert!(matches!(
        MongoTransactionalRuntime::from_database(Arc::clone(&database), policy()).await,
        Err(MongoReliabilityError::SchemaOutdated {
            installed: 0,
            required: 1,
        })
    ));
    schema
        .update_one(
            doc! { "_id": "lily.queue.transactional_inbox" },
            doc! { "$set": { "version": 2_i64 } },
        )
        .await
        .expect("install future schema marker");
    assert!(matches!(
        MongoTransactionalRuntime::from_database(Arc::clone(&database), policy()).await,
        Err(MongoReliabilityError::SchemaTooNew {
            installed: 2,
            supported: 1,
        })
    ));
    schema
        .update_one(
            doc! { "_id": "lily.queue.transactional_inbox" },
            doc! { "$set": { "version": 1_i64 } },
        )
        .await
        .expect("restore supported schema marker");

    // Readiness queries only the four Lily-owned collection names. Even a
    // deliberately non-canonical application collection must stay outside
    // the bounded readiness materialization and schema contract.
    observer
        .database(DATABASE_NAME)
        .run_command(doc! {
            "create": "cap081_unrelated_capped",
            "capped": true,
            "size": 1_024_i64,
        })
        .await
        .expect("create unrelated application collection");

    let runtime = Arc::new(
        MongoTransactionalRuntime::from_database(Arc::clone(&database), policy())
            .await
            .expect("schema-ready MongoDB transactional runtime"),
    );
    let effects = initialized_effects(Arc::clone(&database)).await;

    // Concurrent and serial duplicates never enter application code twice.
    let event_id = Uuid::new_v4();
    let output_id = Uuid::new_v4();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let first_runtime = Arc::clone(&runtime);
    let first_effects = Arc::clone(&effects);
    let first_entered = Arc::clone(&entered);
    let first_release = Arc::clone(&release);
    let first = tokio::spawn(async move {
        first_runtime
            .execute("qualification.orders", event_id, move |transaction| {
                let effects = Arc::clone(&first_effects);
                let entered = Arc::clone(&first_entered);
                let release = Arc::clone(&first_release);
                async move {
                    effects
                        .insert_one(
                            QualificationEffect {
                                _id: None,
                                event_id: event_id.to_string(),
                                marker: "first".into(),
                                _revision: 0,
                            },
                            &transaction
                                .operation_context()
                                .map_err(|error| QueueHandlerError::retryable(error.code()))?,
                        )
                        .await
                        .map_err(|_| {
                            QueueHandlerError::retryable("CAP081_BUSINESS_WRITE_FAILED")
                        })?;
                    transaction
                        .enqueue(
                            TransactionalOutboxMessage::try_new(
                                output_id,
                                "qualification.events",
                                "orders.applied",
                                1,
                                PublishContentKind::Json,
                                br#"{"ok":true}"#.to_vec(),
                            )
                            .map_err(|_| QueueHandlerError::permanent("CAP081_OUTBOX_INVALID"))?,
                        )
                        .await
                        .map_err(|error| QueueHandlerError::retryable(error.code()))?;
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                }
            })
            .await
    });
    entered.notified().await;
    assert_eq!(
        runtime
            .execute("qualification.orders", event_id, |_| async move {
                Err::<(), _>(QueueHandlerError::permanent(
                    "CAP081_CONCURRENT_DUPLICATE_HANDLER_RAN",
                ))
            })
            .await
            .expect("standalone duplicate observation"),
        TransactionalExecution::InProgress,
        "standalone storage callers retain the bounded immediate contention contract"
    );
    release.notify_one();
    assert_eq!(
        first
            .await
            .expect("join first owner")
            .expect("first transaction"),
        TransactionalExecution::Applied(())
    );
    assert!(
        runtime
            .transactional_inbox_snapshot()
            .lease_contentions_total
            >= 1
    );
    assert_eq!(
        runtime
            .execute("qualification.orders", event_id, |_| async move {
                Err::<(), _>(QueueHandlerError::permanent(
                    "CAP081_SERIAL_DUPLICATE_HANDLER_RAN",
                ))
            })
            .await
            .expect("serial duplicate"),
        TransactionalExecution::AlreadyCompleted
    );
    assert_eq!(effect_count(&database, event_id).await, 1);

    // A handler may clone the public transaction handle, but its authority is
    // scoped to the body future. Detached work released only after commit must
    // observe cancellation and cannot perform an autocommit business write or
    // enqueue a durable outbox event through the stale session handle.
    let escaped_event = Uuid::new_v4();
    let escaped_output = Uuid::new_v4();
    let escaped_release = Arc::new(tokio::sync::Notify::new());
    let escaped_spawned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (escaped_result_tx, mut escaped_result_rx) = tokio::sync::mpsc::unbounded_channel();
    let escaped_effects = Arc::clone(&effects);
    let escaped_release_body = Arc::clone(&escaped_release);
    let escaped_spawned_body = Arc::clone(&escaped_spawned);
    assert_eq!(
        runtime
            .execute(
                "qualification.escaped-handle",
                escaped_event,
                move |transaction| {
                    let effects = Arc::clone(&escaped_effects);
                    let release = Arc::clone(&escaped_release_body);
                    let result_tx = escaped_result_tx.clone();
                    if !escaped_spawned_body.swap(true, Ordering::AcqRel) {
                        tokio::spawn(async move {
                            release.notified().await;
                            let business_cancelled = match transaction.operation_context() {
                                Ok(operation) => matches!(
                                    effects
                                        .insert_one(
                                            QualificationEffect {
                                                _id: None,
                                                event_id: escaped_event.to_string(),
                                                marker: "escaped".into(),
                                                _revision: 0,
                                            },
                                            &operation,
                                        )
                                        .await,
                                    Err(MongoRepositoryError::OperationCancelled)
                                ),
                                Err(MongoReliabilityError::TransactionCancelled) => true,
                                Err(_) => false,
                            };
                            let outbox = transaction
                                .enqueue(
                                    TransactionalOutboxMessage::try_new(
                                        escaped_output,
                                        "qualification.events",
                                        "orders.escaped",
                                        1,
                                        PublishContentKind::Json,
                                        br#"{"escaped":true}"#.to_vec(),
                                    )
                                    .expect("valid escaped-handle outbox contract"),
                                )
                                .await;
                            let _ = result_tx.send((business_cancelled, outbox));
                        });
                    }
                    async { Ok(()) }
                },
            )
            .await
            .expect("commit body before releasing escaped transaction clone"),
        TransactionalExecution::Applied(())
    );
    escaped_release.notify_one();
    let (business_cancelled, outbox_result) =
        tokio::time::timeout(Duration::from_secs(2), escaped_result_rx.recv())
            .await
            .expect("escaped transaction result deadline")
            .expect("escaped transaction task result");
    assert!(business_cancelled);
    assert!(matches!(
        outbox_result,
        Err(MongoReliabilityError::TransactionCancelled)
    ));
    assert_eq!(effect_count(&database, escaped_event).await, 0);
    assert_eq!(outbox_event_count(&database, escaped_output).await, 0);

    // `TransientTransactionError` reruns the complete transaction body with a
    // fresh session; all first-attempt writes remain invisible.
    let transient_event = Uuid::new_v4();
    let transient_calls = Arc::new(AtomicUsize::new(0));
    configure_fail_command(
        &observer,
        "insert",
        112,
        &["TransientTransactionError"],
        1,
        Some(&format!("{DATABASE_NAME}.{BUSINESS_COLLECTION}")),
    )
    .await;
    let transient_effects = Arc::clone(&effects);
    let transient_counter = Arc::clone(&transient_calls);
    let transient_result = runtime
        .execute(
            "qualification.transient",
            transient_event,
            move |transaction| {
                let effects = Arc::clone(&transient_effects);
                let calls = Arc::clone(&transient_counter);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    effects
                        .insert_one(
                            QualificationEffect {
                                _id: None,
                                event_id: transient_event.to_string(),
                                marker: "transient".into(),
                                _revision: 0,
                            },
                            &transaction
                                .operation_context()
                                .map_err(|error| QueueHandlerError::retryable(error.code()))?,
                        )
                        .await
                        .map_err(|_| {
                            QueueHandlerError::retryable("CAP081_BUSINESS_WRITE_FAILED")
                        })?;
                    Ok(())
                }
            },
        )
        .await;
    disable_fail_command(&observer).await;
    assert_eq!(
        transient_result.expect("bounded transient transaction retry"),
        TransactionalExecution::Applied(())
    );
    assert_eq!(transient_calls.load(Ordering::SeqCst), 2);
    assert_eq!(effect_count(&database, transient_event).await, 1);

    // `UnknownTransactionCommitResult` retries commit only; application code
    // is never replayed for an ambiguous commit response.
    let commit_event = Uuid::new_v4();
    let commit_calls = Arc::new(AtomicUsize::new(0));
    let commit_snapshot_before = runtime.transactional_inbox_snapshot();
    configure_fail_command(
        &observer,
        "commitTransaction",
        64,
        &["UnknownTransactionCommitResult"],
        2,
        None,
    )
    .await;
    let commit_effects = Arc::clone(&effects);
    let commit_counter = Arc::clone(&commit_calls);
    let commit_result = runtime
        .execute("qualification.commit", commit_event, move |transaction| {
            let effects = Arc::clone(&commit_effects);
            let calls = Arc::clone(&commit_counter);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                effects
                    .insert_one(
                        QualificationEffect {
                            _id: None,
                            event_id: commit_event.to_string(),
                            marker: "commit".into(),
                            _revision: 0,
                        },
                        &transaction
                            .operation_context()
                            .map_err(|error| QueueHandlerError::retryable(error.code()))?,
                    )
                    .await
                    .map_err(|_| QueueHandlerError::retryable("CAP081_BUSINESS_WRITE_FAILED"))?;
                Ok(())
            }
        })
        .await;
    disable_fail_command(&observer).await;
    assert_eq!(
        commit_result.expect("bounded unknown-commit reconciliation"),
        TransactionalExecution::Applied(())
    );
    assert_eq!(commit_calls.load(Ordering::SeqCst), 1);
    assert_eq!(effect_count(&database, commit_event).await, 1);
    let commit_snapshot_after = runtime.transactional_inbox_snapshot();
    assert_eq!(
        commit_snapshot_after.body_attempts_total - commit_snapshot_before.body_attempts_total,
        1
    );
    assert!(
        commit_snapshot_after.commit_attempts_total - commit_snapshot_before.commit_attempts_total
            > 1
    );
    assert!(
        commit_snapshot_after.unknown_commit_retries_total
            - commit_snapshot_before.unknown_commit_retries_total
            >= 1
    );

    // A commit command which outlives Lily's commit-only deadline has an
    // unknown result even when the driver surfaces only a local operation
    // timeout. It must never degrade into a generic storage error or replay
    // the transaction body.
    let mut short_commit_policy = policy();
    short_commit_policy
        .mongodb
        .as_mut()
        .expect("MongoDB policy")
        .commit_retry_timeout_millis = 25;
    let short_commit_runtime =
        MongoTransactionalRuntime::from_database(Arc::clone(&database), short_commit_policy)
            .await
            .expect("short commit timeout runtime");
    let short_commit_before = short_commit_runtime.transactional_inbox_snapshot();
    configure_blocked_command(&observer, "commitTransaction", 1, 250).await;
    let short_commit_error = short_commit_runtime
        .execute(
            "qualification.commit_timeout",
            Uuid::new_v4(),
            |_| async move { Ok(()) },
        )
        .await
        .expect_err("delayed commit must remain an unknown outcome");
    disable_fail_command(&observer).await;
    assert_eq!(
        short_commit_error.code(),
        "QUEUE_MONGODB_COMMIT_OUTCOME_UNKNOWN"
    );
    let short_commit_after = short_commit_runtime.transactional_inbox_snapshot();
    assert_eq!(
        short_commit_after.body_attempts_total - short_commit_before.body_attempts_total,
        1
    );
    assert_eq!(
        short_commit_after.commit_outcome_unknown_total
            - short_commit_before.commit_outcome_unknown_total,
        1
    );
    assert_eq!(
        short_commit_after.last_failure_code,
        Some("QUEUE_MONGODB_COMMIT_OUTCOME_UNKNOWN")
    );

    // External lease deletion is advisory after the MongoDB transaction has
    // committed. A failed delete neither rewrites Applied nor prevents an
    // immediate duplicate from observing the committed inbox record.
    let release_failure_event = Uuid::new_v4();
    let release_snapshot_before = runtime.transactional_inbox_snapshot();
    let failpoint_observer = observer.clone();
    let release_failure_result = runtime
        .execute(
            "qualification.release_failure",
            release_failure_event,
            move |_| {
                let failpoint_observer = failpoint_observer.clone();
                async move {
                    configure_fail_command(&failpoint_observer, "delete", 91, &[], 1, None).await;
                    Ok(())
                }
            },
        )
        .await;
    disable_fail_command(&observer).await;
    assert_eq!(
        release_failure_result.expect("committed result survives lease delete failure"),
        TransactionalExecution::Applied(())
    );
    assert_eq!(
        runtime
            .transactional_inbox_snapshot()
            .post_commit_release_failures_total
            - release_snapshot_before.post_commit_release_failures_total,
        1
    );
    assert_eq!(
        runtime
            .execute(
                "qualification.release_failure",
                release_failure_event,
                |_| async move {
                    Err::<(), _>(QueueHandlerError::permanent(
                        "CAP081_RELEASE_FAILURE_DUPLICATE_HANDLER_RAN",
                    ))
                },
            )
            .await
            .expect("duplicate rechecks committed inbox despite retained lease"),
        TransactionalExecution::AlreadyCompleted
    );

    // Cancelling only the delivery waiter does not detach finalization. The
    // framework owner remains visible to drain and commits after the handler
    // finishes, exactly like a broker task disappearing during shutdown.
    let cancelled_waiter_event = Uuid::new_v4();
    let waiter_entered = Arc::new(tokio::sync::Notify::new());
    let waiter_release = Arc::new(tokio::sync::Notify::new());
    let waiter_runtime = Arc::clone(&runtime);
    let waiter_effects = Arc::clone(&effects);
    let waiter_entered_task = Arc::clone(&waiter_entered);
    let waiter_release_task = Arc::clone(&waiter_release);
    let waiter = tokio::spawn(async move {
        waiter_runtime
            .execute(
                "qualification.cancelled_waiter",
                cancelled_waiter_event,
                move |transaction| {
                    let effects = Arc::clone(&waiter_effects);
                    let entered = Arc::clone(&waiter_entered_task);
                    let release = Arc::clone(&waiter_release_task);
                    async move {
                        effects
                            .insert_one(
                                QualificationEffect {
                                    _id: None,
                                    event_id: cancelled_waiter_event.to_string(),
                                    marker: "cancelled-waiter".into(),
                                    _revision: 0,
                                },
                                &transaction
                                    .operation_context()
                                    .map_err(|error| QueueHandlerError::retryable(error.code()))?,
                            )
                            .await
                            .map_err(|_| {
                                QueueHandlerError::retryable("CAP081_BUSINESS_WRITE_FAILED")
                            })?;
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    }
                },
            )
            .await
    });
    waiter_entered.notified().await;
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(runtime.active_transactions(), 1);
    waiter_release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime.active_transactions() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached waiter transaction owner must finalize");
    assert_eq!(effect_count(&database, cancelled_waiter_event).await, 1);

    // Outbox claims are token-fenced. The wrong token cannot consume a publish
    // attempt or mark the durable row delivered.
    let claim_owner = Uuid::new_v4();
    let batch = runtime
        .claim_outbox_batch(claim_owner)
        .await
        .expect("claim transactional outbox batch");
    let record = batch
        .iter()
        .find(|record| record.event_id() == output_id)
        .expect("committed outbox row");
    let claim_token = record.claim_token();
    let wrong_token = Uuid::new_v4();
    assert_eq!(
        runtime
            .begin_outbox_publish(record.record_id(), wrong_token)
            .await,
        Err(MongoReliabilityError::OutboxClaimLost)
    );
    assert_eq!(
        runtime
            .mark_outbox_delivered(record.record_id(), wrong_token)
            .await,
        Err(MongoReliabilityError::OutboxClaimLost)
    );
    assert_eq!(
        runtime
            .begin_outbox_publish(record.record_id(), claim_token)
            .await
            .expect("fenced publish admission"),
        1
    );
    runtime
        .mark_outbox_delivered(record.record_id(), claim_token)
        .await
        .expect("fenced delivered mark");

    // Both an unexpected application index and a missing Lily index are exact
    // schema drift. Runtime readiness never accepts or silently repairs either.
    let outbox = database
        .collection::<Document>("_lily_queue_outbox")
        .expect("framework outbox collection");
    outbox
        .create_index(
            IndexModel::builder()
                .keys(doc! { "unexpected": 1 })
                .options(
                    IndexOptions::builder()
                        .name("cap081_unexpected_index".to_owned())
                        .build(),
                )
                .build(),
        )
        .await
        .expect("install unexpected qualification index");
    assert!(matches!(
        MongoTransactionalRuntime::from_database(Arc::clone(&database), policy()).await,
        Err(MongoReliabilityError::SchemaDrift)
    ));
    outbox
        .drop_index("cap081_unexpected_index")
        .await
        .expect("remove unexpected qualification index");
    runtime
        .ensure_schema_ready()
        .await
        .expect("exact schema recovers after unexpected index removal");
    outbox
        .drop_index("lily_queue_outbox_ready")
        .await
        .expect("install qualification schema drift");
    assert!(matches!(
        MongoTransactionalRuntime::from_database(Arc::clone(&database), policy()).await,
        Err(MongoReliabilityError::SchemaDrift)
    ));
    outbox
        .create_index(
            IndexModel::builder()
                .keys(doc! {
                    "delivered_at": 1,
                    "available_at": 1,
                    "claimed_until": 1,
                    "created_at": 1,
                    "_id": 1,
                })
                .options(
                    IndexOptions::builder()
                        .name("lily_queue_outbox_ready".to_owned())
                        .build(),
                )
                .build(),
        )
        .await
        .expect("restore exact qualification index");
    runtime
        .ensure_schema_ready()
        .await
        .expect("exact schema recovers after expected index restoration");

    // A non-cooperative handler is aborted only by the explicit force phase;
    // active ownership reaches zero before the MongoDB DI owner is closed.
    let forced_event = Uuid::new_v4();
    let forced_entered = Arc::new(tokio::sync::Notify::new());
    let forced_runtime = Arc::clone(&runtime);
    let forced_effects = Arc::clone(&effects);
    let forced_entered_task = Arc::clone(&forced_entered);
    let forced_waiter = tokio::spawn(async move {
        forced_runtime
            .execute("qualification.force", forced_event, move |transaction| {
                let effects = Arc::clone(&forced_effects);
                let entered = Arc::clone(&forced_entered_task);
                async move {
                    effects
                        .insert_one(
                            QualificationEffect {
                                _id: None,
                                event_id: forced_event.to_string(),
                                marker: "forced".into(),
                                _revision: 0,
                            },
                            &transaction
                                .operation_context()
                                .map_err(|error| QueueHandlerError::retryable(error.code()))?,
                        )
                        .await
                        .map_err(|_| {
                            QueueHandlerError::retryable("CAP081_BUSINESS_WRITE_FAILED")
                        })?;
                    entered.notify_one();
                    std::future::pending::<()>().await;
                    #[allow(unreachable_code)]
                    Ok(())
                }
            })
            .await
    });
    forced_entered.notified().await;
    runtime
        .force_drain_transactions(Duration::from_secs(2))
        .await
        .expect("forced transaction drain");
    assert_eq!(runtime.active_transactions(), 0);
    let forced_result = forced_waiter.await.expect("join forced waiter");
    assert_eq!(
        forced_result
            .expect_err("forced owner must not report a commit")
            .code(),
        "QUEUE_MONGODB_TRANSACTION_OWNER_INTERRUPTED"
    );
    assert_eq!(effect_count(&database, forced_event).await, 0);

    runtime.stop_transaction_admission();
    runtime
        .drain_transactions(Duration::from_secs(2))
        .await
        .expect("bounded transaction owner drain");
    assert_eq!(runtime.active_transactions(), 0);

    // Collection options are part of the exact readiness contract. Converting
    // an otherwise canonical inbox collection to capped storage would permit
    // MongoDB to evict deduplication rows, so readiness must fail closed even
    // though its validator and indexes remain present.
    observer
        .database(DATABASE_NAME)
        .run_command(doc! {
            "convertToCapped": "_lily_queue_inbox",
            "size": 1_048_576_i64,
        })
        .await
        .expect("convert dedicated qualification inbox to capped storage");
    assert!(matches!(
        MongoTransactionalRuntime::from_database(Arc::clone(&database), policy()).await,
        Err(MongoReliabilityError::SchemaDrift)
    ));
    database
        .close()
        .expect("close MongoDB qualification service");
}
