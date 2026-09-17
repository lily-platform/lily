#![cfg(feature = "single")]

//! Live MongoDB qualification fixture.
//!
//! Run against a dedicated replica-set database:
//! `LILY_TEST_MONGODB_URL=... cargo test -p lily_mongodb --test live_mongodb -- --ignored`

use std::sync::Arc;

use lily_mongo_repository::{
    MongoFilter, MongoOperationContext, MongoRepository, MongoRepositoryError,
};
use lily_config::DatabaseConfig;
use lily_injection::ServiceTrait;
use lily_mongodb::{
    Collection, DatabaseService, MongoClientPlan, MongoCollection, MongoMigration,
    MongoMigrationRunner, Repository,
};
use mongodb::bson::{doc, oid::ObjectId};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContractUser {
    _id: Option<ObjectId>,
    email: String,
    display_name: String,
    _revision: i64,
}

#[derive(MongoCollection)]
#[collection("lily_mongo_v1_contract_users")]
#[collection_type(ContractUser)]
#[unique_field("email")]
struct ContractUsers {
    db: Arc<DatabaseService>,
    collection: Option<Collection<ContractUser>>,
}

#[derive(Repository)]
#[entity_type(ContractUser)]
#[collection_type(ContractUsers)]
struct ContractUserRepository {
    collection: Arc<ContractUsers>,
}

fn database_config() -> DatabaseConfig {
    DatabaseConfig {
        mode: Some("single".into()),
        database_type: Some("mongodb".into()),
        connection_string: Some(
            std::env::var("LILY_TEST_MONGODB_URL")
                .expect("LILY_TEST_MONGODB_URL must identify a dedicated replica-set database"),
        ),
        pool_size: Some(4),
        connection_timeout_secs: Some(10),
        query_timeout_secs: Some(30),
        pooling_enabled: Some(true),
        database_name: Some("lily_v1_qualification".into()),
        host: None,
        port: None,
        username: None,
        password: None,
        auth_database: None,
        use_tls: None,
        app_name: Some("lily-mongodb-qualification".into()),
        cells: None,
    }
}

fn operation(database: &DatabaseService) -> MongoOperationContext<'static> {
    database
        .operation_context(CancellationToken::new())
        .expect("ready operation context")
}

#[tokio::test]
#[ignore = "qualification: requires a dedicated live MongoDB replica set"]
async fn migration_crud_duplicate_transaction_cancellation_and_reconnect_contract() {
    let plan = MongoClientPlan::from_database(&database_config()).expect("valid MongoDB plan");
    let database = DatabaseService::connect(plan)
        .await
        .expect("connect and readiness ping");
    database
        .verify_transaction_capability()
        .await
        .expect("replica set supports bounded transactions");

    let migration = MongoMigration::new(
        8_201_001,
        "create Lily MongoDB contract users",
        ContractUsers::migration_steps().expect("derive migration steps"),
    )
    .expect("valid migration");
    let runner = MongoMigrationRunner::new(&database, vec![migration]).expect("migration runner");
    let first_operation = operation(&database);
    let second_operation = operation(&database);
    let (first, second) = tokio::join!(
        runner.apply(&first_operation),
        runner.apply(&second_operation)
    );
    assert!(first.is_ok() || second.is_ok());
    for result in [first, second] {
        assert!(
            result.is_ok() || matches!(result, Err(MongoRepositoryError::MigrationLockUnavailable))
        );
    }

    let component_migration = MongoMigration::new(
        8_201_002,
        "qualify component-owned MongoDB migration ledger",
        ContractUsers::migration_steps().expect("derive component migration steps"),
    )
    .expect("valid component migration");
    let component_runner = MongoMigrationRunner::for_component(
        &database,
        "lily.mongodb.qualification",
        vec![component_migration],
    )
    .expect("component migration runner");
    component_runner
        .apply(&operation(&database))
        .await
        .expect("apply component migration");
    let component_rerun = component_runner
        .apply(&operation(&database))
        .await
        .expect("rerun component migration");
    assert_eq!(component_rerun.previously_applied(), 1);
    assert!(component_rerun.newly_applied().is_empty());

    let mut collection = ContractUsers {
        db: Arc::new(database.clone()),
        collection: None,
    };
    collection
        .initialize()
        .await
        .expect("initialize handle without DDL");
    let repository = ContractUserRepository {
        collection: Arc::new(collection),
    };

    let unique = uuid::Uuid::new_v4();
    let email = format!("{unique}@lily.invalid");
    let user = ContractUser {
        _id: None,
        email: email.clone(),
        display_name: "before".into(),
        _revision: 0,
    };
    let mut inserted = repository
        .create(user.clone(), &operation(&database))
        .await
        .expect("generated repository create");
    assert!(matches!(
        repository.create(user, &operation(&database)).await,
        Err(MongoRepositoryError::DuplicateKey(_))
    ));

    inserted.display_name = "after".into();
    inserted = repository
        .update(
            inserted,
            &operation(&database)
                .with_expected_revision(0)
                .expect("valid revision"),
        )
        .await
        .expect("optimistic update");
    assert_eq!(inserted._revision, 1);

    let loaded = repository
        .find_one(
            MongoFilter::new(doc! { "email": &email }).unwrap(),
            &operation(&database),
        )
        .await
        .expect("find updated entity")
        .expect("entity exists");
    assert_eq!(loaded.display_name, "after");

    let transaction = database
        .begin_transaction(&operation(&database))
        .await
        .expect("start transaction");
    let transaction_email = format!("transaction-{unique}@lily.invalid");
    repository
        .create(
            ContractUser {
                _id: None,
                email: transaction_email.clone(),
                display_name: "rollback".into(),
                _revision: 0,
            },
            &operation(&database).with_transaction(&transaction),
        )
        .await
        .expect("transactional insert");
    transaction
        .abort(&operation(&database))
        .await
        .expect("abort transaction");
    assert!(
        repository
            .find_one(
                MongoFilter::new(doc! { "email": transaction_email }).unwrap(),
                &operation(&database),
            )
            .await
            .expect("query aborted transaction")
            .is_none()
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        repository
            .count(
                MongoFilter::all(),
                &database.operation_context(cancelled).unwrap(),
            )
            .await,
        Err(MongoRepositoryError::OperationCancelled)
    ));

    repository
        .delete_by_id(
            inserted._id.expect("generated id").into(),
            &operation(&database),
        )
        .await
        .expect("delete fixture entity");
    database.dispose().await.expect("dispose database handle");
}
