//! Environment-restricted ClickHouse provider qualification fixture.

#![cfg(feature = "single")]

use std::sync::Arc;

use lily_clickhouse::{
    ClickhouseClientPlan, ClickhouseError, ClickhouseMigration, ClickhouseMigrationRunner,
    ClickhousePageRequest, DatabaseService,
};
use lily_config::ClickhouseConfig;
use lily_injection::ServiceTrait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, clickhouse::Row)]
struct LiveEvent {
    id: u64,
    tenant: String,
}

#[tokio::test]
#[ignore = "requires a disposable ClickHouse database; qualification pending"]
async fn migration_bound_query_cancellation_and_dispose_contract() {
    assert_eq!(
        std::env::var("LILY_CLICKHOUSE_LIVE_ACK").as_deref(),
        Ok("dedicated-disposable"),
        "set LILY_CLICKHOUSE_LIVE_ACK=dedicated-disposable for the destructive fixture"
    );
    let config = live_config();
    let service = Arc::new(
        DatabaseService::connect(ClickhouseClientPlan::from_config(&config).unwrap())
            .await
            .unwrap(),
    );
    let operation = service.bounded_operation().unwrap();
    let migrations = vec![
        ClickhouseMigration::new(
            1,
            "live events",
            vec![
                "CREATE TABLE `lily_live_events` (`id` UInt64, `tenant` String) \
             ENGINE = MergeTree() ORDER BY `id`"
                    .into(),
            ],
        )
        .unwrap(),
    ];
    let runner = ClickhouseMigrationRunner::new(Arc::clone(&service), migrations).unwrap();
    runner.apply(&operation).await.unwrap();
    let second = runner
        .apply(&service.bounded_operation().unwrap())
        .await
        .unwrap();
    assert!(second.newly_applied.is_empty());

    let event = LiveEvent {
        id: 1,
        tenant: "tenant' OR 1=1".into(),
    };
    service
        .insert_one("lily_live_events", &event, &operation)
        .await
        .unwrap();
    let rows = service
        .find_equal::<LiveEvent, _>(
            "lily_live_events",
            &["id", "tenant"],
            "tenant",
            &event.tenant,
            ClickhousePageRequest::new(10, 0).unwrap(),
            &operation,
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![event]);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = service.operation_context(cancellation).unwrap();
    assert_eq!(
        service.count("lily_live_events", &cancelled).await,
        Err(ClickhouseError::OperationCancelled)
    );

    ServiceTrait::dispose(service.as_ref()).await.unwrap();
    assert_eq!(
        service.database_name(),
        Err(ClickhouseError::NotInitialized)
    );
}

fn live_config() -> ClickhouseConfig {
    let host =
        std::env::var("LILY_CLICKHOUSE_LIVE_HOST").expect("LILY_CLICKHOUSE_LIVE_HOST is required");
    let database = std::env::var("LILY_CLICKHOUSE_LIVE_DATABASE")
        .expect("LILY_CLICKHOUSE_LIVE_DATABASE is required");
    let username = std::env::var("LILY_CLICKHOUSE_LIVE_USERNAME").ok();
    let password = std::env::var("LILY_CLICKHOUSE_LIVE_PASSWORD").ok();
    ClickhouseConfig {
        mode: Some("single".into()),
        host: Some(host),
        port: std::env::var("LILY_CLICKHOUSE_LIVE_PORT")
            .ok()
            .and_then(|value| value.parse().ok()),
        database: Some(database),
        username,
        password,
        pool_size: Some(4),
        connection_timeout_secs: Some(5),
        query_timeout_secs: Some(20),
        use_tls: Some(std::env::var("LILY_CLICKHOUSE_LIVE_TLS").as_deref() == Ok("true")),
        compression_enabled: Some(true),
        cells: None,
    }
}
