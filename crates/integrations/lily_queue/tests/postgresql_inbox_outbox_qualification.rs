#![cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]

//! Environment-qualified PostgreSQL transactional inbox/outbox proof.
//!
//! The fixture must be dedicated: the test explicitly migrates Lily's schema
//! and creates a namespaced business-effect table. The fixture config may
//! also contain RabbitMQ because the full application container initializes
//! every linked singleton, but this test itself performs no broker publish.

use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use lily_config::TransactionalInboxConfig;
use lily_injection::ApplicationContainer;
#[cfg(feature = "transactional-inbox-postgresql")]
use lily_postgresql::PgDatabaseService;
use lily_postgresql::{diesel, diesel_async};
use lily_queue::__private::{PostgresTransactionalRuntime, TransactionalExecution};
use lily_queue::{
    PostgresInboxOutboxMigrator, PublishContentKind, QueueHandlerError, TransactionalOutboxMessage,
};
use uuid::Uuid;

static ENVIRONMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct BootstrapEnvironment {
    path: Option<OsString>,
    mode: Option<OsString>,
}

impl BootstrapEnvironment {
    fn install(path: &OsStr) -> Self {
        let previous = Self {
            path: std::env::var_os("LILY_CONFIG_PATH"),
            mode: std::env::var_os("LILY_CONFIG_MODE"),
        };
        // SAFETY: this integration binary serializes all environment mutation
        // with ENVIRONMENT and restores the values before releasing the lock.
        unsafe {
            std::env::set_var("LILY_CONFIG_PATH", path);
            // The disposable fixture intentionally uses plaintext localhost
            // PostgreSQL. Production validation correctly requires
            // verify-full TLS, so this environment-qualified storage test uses
            // the deterministic test profile instead.
            std::env::set_var("LILY_CONFIG_MODE", "test");
        }
        previous
    }
}

impl Drop for BootstrapEnvironment {
    fn drop(&mut self) {
        // SAFETY: see BootstrapEnvironment::install.
        unsafe {
            restore("LILY_CONFIG_PATH", self.path.take());
            restore("LILY_CONFIG_MODE", self.mode.take());
        }
    }
}

unsafe fn restore(name: &str, value: Option<OsString>) {
    if let Some(value) = value {
        // SAFETY: the caller holds ENVIRONMENT.
        unsafe { std::env::set_var(name, value) };
    } else {
        // SAFETY: the caller holds ENVIRONMENT.
        unsafe { std::env::remove_var(name) };
    }
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(diesel::QueryableByName)]
struct BoolRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    value: bool,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LILY_CAP08_POSTGRES_CONFIG pointing to a dedicated disposable fixture"]
async fn duplicate_rollback_commit_and_outbox_relay_storage_are_atomic() {
    let _environment = ENVIRONMENT.lock().await;
    let path = std::env::var_os("LILY_CAP08_POSTGRES_CONFIG")
        .expect("LILY_CAP08_POSTGRES_CONFIG must point to the dedicated fixture");
    let _bootstrap = BootstrapEnvironment::install(&path);
    let application = Arc::new(
        ApplicationContainer::build()
            .await
            .expect("dedicated fixture must initialize"),
    );

    #[cfg(feature = "transactional-inbox-postgresql")]
    let (database, policy) = (
        application
            .resolve::<PgDatabaseService>(None)
            .await
            .expect("single PostgreSQL service"),
        TransactionalInboxConfig::default(),
    );

    #[cfg(feature = "transactional-inbox-postgresql-factory")]
    let (database, policy) = {
        let cell = std::env::var("LILY_CAP08_POSTGRES_CELL")
            .expect("factory qualification requires LILY_CAP08_POSTGRES_CELL");
        let factory = application
            .resolve::<lily_postgresql::PgFactory>(None)
            .await
            .expect("PostgreSQL factory");
        let policy = TransactionalInboxConfig {
            database_cell: Some(cell.clone()),
            ..TransactionalInboxConfig::default()
        };
        (factory.get(&cell).expect("exact PostgreSQL cell"), policy)
    };

    // A foreign object occupying Lily's migration-ledger name is classified
    // from catalogs before CREATE TABLE or SELECT assumes its shape.
    database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query("DROP SCHEMA IF EXISTS lily_queue CASCADE")
                        .execute(connection)
                        .await?;
                    diesel::sql_query("CREATE SCHEMA lily_queue")
                        .execute(connection)
                        .await?;
                    diesel::sql_query(
                        "CREATE TABLE lily_queue.schema_migrations (foreign_value INTEGER)",
                    )
                    .execute(connection)
                    .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("install malformed ledger collision");
    assert!(matches!(
        PostgresTransactionalRuntime::from_database(Arc::clone(&database), policy.clone()).await,
        Err(lily_queue::PostgresReliabilityError::SchemaDrift)
    ));
    assert_eq!(
        PostgresInboxOutboxMigrator::new(Arc::clone(&database))
            .migrate()
            .await,
        Err(lily_queue::PostgresReliabilityError::SchemaDrift)
    );
    database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query("DROP SCHEMA lily_queue CASCADE")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("remove malformed ledger collision");

    // A colliding relation must never be adopted merely because the migrator
    // uses namespaced object names. The failed pass must not stamp its ledger.
    database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query("DROP SCHEMA IF EXISTS lily_queue CASCADE")
                        .execute(connection)
                        .await?;
                    diesel::sql_query("CREATE SCHEMA lily_queue")
                        .execute(connection)
                        .await?;
                    diesel::sql_query("CREATE TABLE lily_queue.inbox (wrong INTEGER NOT NULL)")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("install malformed collision fixture");
    assert_eq!(
        PostgresInboxOutboxMigrator::new(Arc::clone(&database))
            .migrate()
            .await,
        Err(lily_queue::PostgresReliabilityError::SchemaDrift)
    );
    database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    let ledger_exists = diesel::sql_query(
                        "SELECT to_regclass('lily_queue.schema_migrations') IS NOT NULL AS value",
                    )
                    .get_result::<BoolRow>(connection)
                    .await?
                    .value;
                    if ledger_exists {
                        return Err(lily_postgresql::PgError::Migration {
                            code: "collision_stamped_ledger",
                        });
                    }
                    diesel::sql_query("DROP SCHEMA lily_queue CASCADE")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("failed collision migration must roll back its ledger");

    PostgresInboxOutboxMigrator::new(Arc::clone(&database))
        .migrate()
        .await
        .expect("explicit migration");

    // A correct ledger version is not sufficient: catalog drift must fail both
    // runtime readiness and a supposedly idempotent migration pass.
    database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query("ALTER TABLE lily_queue.inbox DROP COLUMN updated_at")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("install V1 schema drift");
    assert!(matches!(
        PostgresTransactionalRuntime::from_database(Arc::clone(&database), policy.clone()).await,
        Err(lily_queue::PostgresReliabilityError::SchemaDrift)
    ));
    assert_eq!(
        PostgresInboxOutboxMigrator::new(Arc::clone(&database))
            .migrate()
            .await,
        Err(lily_queue::PostgresReliabilityError::SchemaDrift)
    );
    database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query("DROP SCHEMA lily_queue CASCADE")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("remove drift fixture");
    PostgresInboxOutboxMigrator::new(Arc::clone(&database))
        .migrate()
        .await
        .expect("reinstall canonical schema after drift proof");

    // The V1 ledger also cannot bless additional write-affecting constraints.
    // Removing the foreign constraint restores the exact fingerprint without
    // requiring a new Lily migration.
    database
        .with_connection(|connection, _| {
            Box::pin(async move {
                use diesel_async::RunQueryDsl as _;
                diesel::sql_query(
                    "ALTER TABLE lily_queue.inbox ADD CONSTRAINT foreign_semantics CHECK (char_length(handler_identity) > 3)",
                )
                .execute(connection)
                .await?;
                Ok(())
            })
        }, None)
        .await
        .expect("install additional semantic constraint");
    assert!(matches!(
        PostgresTransactionalRuntime::from_database(Arc::clone(&database), policy.clone()).await,
        Err(lily_queue::PostgresReliabilityError::SchemaDrift)
    ));
    assert_eq!(
        PostgresInboxOutboxMigrator::new(Arc::clone(&database))
            .migrate()
            .await,
        Err(lily_queue::PostgresReliabilityError::SchemaDrift)
    );
    database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query(
                        "ALTER TABLE lily_queue.inbox DROP CONSTRAINT foreign_semantics",
                    )
                    .execute(connection)
                    .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("remove additional semantic constraint");
    let canonical = PostgresInboxOutboxMigrator::new(Arc::clone(&database))
        .migrate()
        .await
        .expect("canonical schema remains idempotently ready");
    assert!(!canonical.applied);
    PostgresTransactionalRuntime::from_database(Arc::clone(&database), policy.clone())
        .await
        .expect("canonical runtime readiness after drift removal");
    database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query(
                        r#"CREATE TABLE IF NOT EXISTS lily_queue.qualification_effects (
                        event_id UUID PRIMARY KEY
                    )"#,
                    )
                    .execute(connection)
                    .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("qualification business table");

    let runtime = Arc::new(
        PostgresTransactionalRuntime::from_database(Arc::clone(&database), policy.clone())
            .await
            .expect("schema-ready runtime"),
    );

    // The first transaction owns the exact inbox identity while application
    // work is deliberately paused. A duplicate must be classified without
    // waiting for the configured PostgreSQL lock timeout or entering its
    // handler.
    let contested = Uuid::new_v4();
    let (entered_sender, entered_receiver) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let first_runtime = Arc::clone(&runtime);
    let first_release = Arc::clone(&release);
    let first = tokio::spawn(async move {
        first_runtime
            .execute("qualification.concurrent", contested, move |_| async move {
                entered_sender
                    .send(())
                    .expect("qualification observer must remain attached");
                first_release.notified().await;
                Ok(())
            })
            .await
    });
    entered_receiver
        .await
        .expect("first transaction must acquire the inbox identity");
    let contender = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        runtime.execute("qualification.concurrent", contested, |_| async move {
            Err::<(), _>(QueueHandlerError::permanent(
                "QUALIFICATION_CONCURRENT_HANDLER_RAN",
            ))
        }),
    )
    .await
    .expect("concurrent duplicate classification must be non-blocking")
    .expect("concurrent duplicate lookup");
    assert_eq!(contender, TransactionalExecution::InProgress);
    release.notify_one();
    assert_eq!(
        first
            .await
            .expect("first task must join")
            .expect("first transaction"),
        TransactionalExecution::Applied(())
    );
    let committed_duplicate = runtime
        .execute("qualification.concurrent", contested, |_| async move {
            Err::<(), _>(QueueHandlerError::permanent(
                "QUALIFICATION_COMMITTED_DUPLICATE_HANDLER_RAN",
            ))
        })
        .await
        .expect("committed duplicate lookup");
    assert_eq!(
        committed_duplicate,
        TransactionalExecution::AlreadyCompleted
    );

    let incoming = Uuid::new_v4();
    let outgoing = Uuid::new_v4();
    let failed = runtime
        .execute(
            "qualification.orders",
            incoming,
            move |transaction| async move {
                transaction
                    .with_connection(move |connection| {
                        Box::pin(async move {
                            use diesel_async::RunQueryDsl as _;
                            diesel::sql_query(
                                "INSERT INTO lily_queue.qualification_effects (event_id) VALUES ($1)",
                            )
                            .bind::<diesel::sql_types::Uuid, _>(incoming)
                            .execute(connection)
                            .await?;
                            Ok(())
                        })
                    })
                    .await?;
                transaction
                    .enqueue(TransactionalOutboxMessage::try_new(
                        outgoing,
                        "qualification.events",
                        "orders.created",
                        1,
                        PublishContentKind::Json,
                        br#"{"ok":true}"#.to_vec(),
                    )?)
                    .await?;
                Err::<(), _>(QueueHandlerError::retryable(
                    "QUALIFICATION_HANDLER_ROLLBACK",
                ))
            },
        )
        .await;
    assert!(failed.is_err(), "handler failure must escape as retryable");

    let rolled_back_effects = database
        .with_connection(move |connection, _| {
            Box::pin(async move {
                use diesel_async::RunQueryDsl as _;
                diesel::sql_query(
                    "SELECT COUNT(*)::BIGINT AS count FROM lily_queue.qualification_effects WHERE event_id = $1",
                )
                .bind::<diesel::sql_types::Uuid, _>(incoming)
                .get_result::<CountRow>(connection)
                .await
                .map(|row| row.count)
                .map_err(Into::into)
            })
        }, None)
        .await
        .expect("rolled-back business-effect count");
    let rolled_back_outbox = database
        .with_connection(move |connection, _| {
            Box::pin(async move {
                use diesel_async::RunQueryDsl as _;
                diesel::sql_query(
                    "SELECT COUNT(*)::BIGINT AS count FROM lily_queue.outbox WHERE source_event_id = $1",
                )
                .bind::<diesel::sql_types::Uuid, _>(incoming)
                .get_result::<CountRow>(connection)
                .await
                .map(|row| row.count)
                .map_err(Into::into)
            })
        }, None)
        .await
        .expect("rolled-back outbox count");
    assert_eq!(rolled_back_effects, 0);
    assert_eq!(rolled_back_outbox, 0);

    // The same identity can be retried because the failed transaction also
    // rolled back its inbox claim.
    let applied = runtime
        .execute(
            "qualification.orders",
            incoming,
            move |transaction| async move {
                transaction
                    .with_connection(move |connection| {
                        Box::pin(async move {
                            use diesel_async::RunQueryDsl as _;
                            diesel::sql_query(
                            "INSERT INTO lily_queue.qualification_effects (event_id) VALUES ($1)",
                        )
                        .bind::<diesel::sql_types::Uuid, _>(incoming)
                        .execute(connection)
                        .await?;
                            Ok(())
                        })
                    })
                    .await?;
                transaction
                    .enqueue(TransactionalOutboxMessage::try_new(
                        outgoing,
                        "qualification.events",
                        "orders.created",
                        1,
                        PublishContentKind::Json,
                        br#"{"ok":true}"#.to_vec(),
                    )?)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("first transactional execution");
    assert_eq!(applied, TransactionalExecution::Applied(()));

    let duplicate = runtime
        .execute("qualification.orders", incoming, |_| async move {
            Err::<(), _>(QueueHandlerError::permanent(
                "QUALIFICATION_DUPLICATE_HANDLER_RAN",
            ))
        })
        .await
        .expect("duplicate lookup");
    assert_eq!(duplicate, TransactionalExecution::AlreadyCompleted);

    let count = database
        .with_connection(|connection, _| {
            Box::pin(async move {
                use diesel_async::RunQueryDsl as _;
                diesel::sql_query(
                    "SELECT COUNT(*)::BIGINT AS count FROM lily_queue.qualification_effects WHERE event_id = $1",
                )
                .bind::<diesel::sql_types::Uuid, _>(incoming)
                .get_result::<CountRow>(connection)
                .await
                .map(|row| row.count)
                .map_err(Into::into)
            })
        }, None)
        .await
        .expect("business-effect count");
    assert_eq!(count, 1);

    let claim_token = Uuid::new_v4();
    let batch = runtime
        .claim_outbox_batch(claim_token)
        .await
        .expect("bounded outbox claim");
    let record = batch
        .iter()
        .find(|record| record.event_id() == outgoing)
        .expect("transactional outbox row");
    assert_eq!(record.claim_token(), claim_token);
    assert_eq!(
        record.publish_attempts(),
        0,
        "batch leasing must not consume a broker publish attempt"
    );
    assert_eq!(
        runtime
            .begin_outbox_publish(record.record_id(), record.claim_token())
            .await
            .expect("claim-fenced publish admission"),
        1
    );
    runtime
        .mark_outbox_delivered(record.record_id(), record.claim_token())
        .await
        .expect("conditional delivered mark");

    // Simulate a deployment which lowers its byte policy below an existing
    // durable row. The oldest row must produce a typed failure without being
    // leased; otherwise it would silently keep every later row behind it.
    let oversized_record = Uuid::new_v4();
    let oversized_source = Uuid::new_v4();
    let oversized_event = Uuid::new_v4();
    database
        .with_connection(
            move |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query(
                        r#"INSERT INTO lily_queue.outbox (
                        record_id, source_handler_identity, source_event_id, event_id,
                        exchange_name, routing_key, schema_version, content_kind,
                        content_type, body
                    ) VALUES ($1, 'qualification.oversized', $2, $3,
                              'qualification.events', 'orders.oversized', 1,
                              'binary', 'application/octet-stream', $4)"#,
                    )
                    .bind::<diesel::sql_types::Uuid, _>(oversized_record)
                    .bind::<diesel::sql_types::Uuid, _>(oversized_source)
                    .bind::<diesel::sql_types::Uuid, _>(oversized_event)
                    .bind::<diesel::sql_types::Binary, _>(vec![0_u8; 9])
                    .execute(connection)
                    .await?;
                    Ok(())
                })
            },
            None,
        )
        .await
        .expect("install persisted oversized row");
    let constrained_runtime = PostgresTransactionalRuntime::from_database(
        Arc::clone(&database),
        TransactionalInboxConfig {
            relay_max_in_flight_bytes: 8,
            ..policy
        },
    )
    .await
    .expect("schema-ready constrained runtime");
    assert_eq!(
        constrained_runtime.claim_outbox_batch(Uuid::new_v4()).await,
        Err(lily_queue::PostgresReliabilityError::OutboxRecordTooLarge {
            bytes: 9,
            maximum: 8,
        })
    );
    let oversized_unclaimed = database
        .with_connection(
            move |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;
                    diesel::sql_query(
                        r#"SELECT claim_token IS NULL AND publish_attempts = 0 AS value
                       FROM lily_queue.outbox WHERE record_id = $1"#,
                    )
                    .bind::<diesel::sql_types::Uuid, _>(oversized_record)
                    .get_result::<BoolRow>(connection)
                    .await
                    .map(|row| row.value)
                    .map_err(Into::into)
                })
            },
            None,
        )
        .await
        .expect("inspect oversized claim state");
    assert!(oversized_unclaimed);

    application.close().await.expect("bounded DI shutdown");
}
