//! Dedicated Redis qualification fixture.
//!
//! These tests require disposable Redis instances and are intentionally
//! ignored in the default suite. CI qualification supplies the documented
//! environment variables and runs them explicitly.

#[cfg(feature = "single")]
use lily_redis::CacheScanRequest;
use lily_redis::{ICache, RedisCachePlan};
#[cfg(feature = "factory")]
use lily_config::CacheCellConfig;
#[cfg(feature = "single")]
use lily_config::CacheConfig;
#[cfg(feature = "single")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "single")]
use tokio_util::sync::CancellationToken;

#[cfg(feature = "single")]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Session {
    subject: String,
    roles: Vec<String>,
}

#[cfg(feature = "single")]
fn single_plan(variable: &str, namespace: &str) -> RedisCachePlan {
    let redis_url = std::env::var(variable)
        .unwrap_or_else(|_| panic!("{variable} must point to a disposable Redis instance"));
    let plan = RedisCachePlan::from_single(&CacheConfig {
        redis_url: Some(redis_url.clone()),
        use_tls: Some(redis_url.starts_with("rediss://")),
        key_namespace: Some(namespace.into()),
        default_ttl_secs: Some(60),
        pool_size: Some(4),
        connection_timeout_secs: Some(3),
        operation_timeout_secs: Some(2),
        scan_page_size: Some(50),
        max_scan_results: Some(100),
        ..CacheConfig::default()
    })
    .expect("qualification Redis config must be valid");
    with_fixture_ca(plan, &redis_url)
}

#[cfg(feature = "single")]
#[tokio::test]
#[ignore = "ENVIRONMENT-RESTRICTED: requires LILY_REDIS_URL and a disposable Redis fixture"]
async fn redis_envelope_ttl_scan_cancellation_and_shutdown_contract() {
    let cache = lily_redis::CacheService::connect(single_plan(
        "LILY_REDIS_URL",
        "lily-qualification-single",
    ))
    .await
    .unwrap();
    let session = Session {
        subject: "user-1".into(),
        roles: vec!["reader".into()],
    };

    cache.set_json("session:1", &session).await.unwrap();
    assert_eq!(
        cache.get::<Session>("session:1").await.unwrap(),
        Some(session)
    );
    cache.set_bytes("binary:1", &[0, 1, 255]).await.unwrap();
    assert_eq!(
        cache.get_bytes("binary:1").await.unwrap(),
        Some(vec![0, 1, 255])
    );
    assert!(cache.get::<Session>("binary:1").await.is_err());

    let first = cache
        .fixed_window_rate_limit("rate:1", 2, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let second = cache
        .fixed_window_rate_limit("rate:1", 2, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let rejected = cache
        .fixed_window_rate_limit("rate:1", 2, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    assert!(first.allowed());
    assert!(second.allowed());
    assert!(!rejected.allowed());
    assert_eq!(rejected.count(), 3);
    assert_eq!(rejected.remaining(), 0);
    assert!(!rejected.retry_after().is_zero());

    let page = cache
        .scan_page(CacheScanRequest::new(0, "*", 50).unwrap())
        .await
        .unwrap();
    assert!(
        page.keys
            .iter()
            .all(|key| !key.starts_with("lily-qualification"))
    );

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let operation = cache.operation_context(cancellation).unwrap();
    assert!(
        cache
            .get_with_context::<Session>("session:1", &operation)
            .await
            .is_err()
    );

    cache.dispose().await.unwrap();
    assert!(cache.get::<Session>("session:1").await.is_err());
}

#[cfg(feature = "single")]
#[tokio::test]
#[ignore = "ENVIRONMENT-RESTRICTED: requires LILY_REDIS_URL and a disposable Redis fixture"]
async fn redis_conditional_insert_is_atomic_across_service_instances() {
    let namespace = "lily-qualification-conditional-insert";
    let first = lily_redis::CacheService::connect(single_plan("LILY_REDIS_URL", namespace))
        .await
        .unwrap();
    let second = lily_redis::CacheService::connect(single_plan("LILY_REDIS_URL", namespace))
        .await
        .unwrap();
    let key = "csrf:load-or-insert";
    first.remove(key).await.unwrap();

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let first_task = {
        let cache = first.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            cache
                .set_json_text_if_absent_with_ttl(key, r#"{"candidate":"first"}"#, 60)
                .await
        })
    };
    let second_task = {
        let cache = second.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            cache
                .set_json_text_if_absent_with_ttl(key, r#"{"candidate":"second"}"#, 60)
                .await
        })
    };

    let first_inserted = first_task.await.unwrap().unwrap();
    let second_inserted = second_task.await.unwrap().unwrap();
    assert_ne!(
        first_inserted, second_inserted,
        "exactly one Redis service instance must win the conditional insert"
    );

    let stored = first.get_json_text(key).await.unwrap().unwrap();
    let expected = if first_inserted {
        r#"{"candidate":"first"}"#
    } else {
        r#"{"candidate":"second"}"#
    };
    assert_eq!(stored, expected);
    assert!(
        !second
            .set_json_text_if_absent_with_ttl(key, r#"{"candidate":"replacement"}"#, 1)
            .await
            .unwrap(),
        "an existing key must reject later conditional inserts"
    );
    assert_eq!(
        first.get_json_text(key).await.unwrap().as_deref(),
        Some(expected)
    );
    assert!(matches!(
        first.ttl(key).await.unwrap(),
        lily_redis::CacheTtl::ExpiresIn(2..=60)
    ));

    first.remove(key).await.unwrap();
    first.dispose().await.unwrap();
    second.dispose().await.unwrap();
}

#[cfg(feature = "factory")]
fn cell_plan(variable: &str, name: &str) -> RedisCachePlan {
    let redis_url = std::env::var(variable)
        .unwrap_or_else(|_| panic!("{variable} must point to a disposable Redis instance"));
    let plan = RedisCachePlan::from_cell(&CacheCellConfig {
        name: name.into(),
        provider: "redis".into(),
        redis_url: Some(redis_url.clone()),
        use_tls: Some(redis_url.starts_with("rediss://")),
        additional_ca_bundle: None,
        key_namespace: Some(format!("lily-qualification-{name}")),
        default_ttl_secs: Some(60),
        pool_size: Some(2),
        connection_timeout_secs: Some(3),
        operation_timeout_secs: Some(2),
        scan_page_size: Some(20),
        max_scan_results: Some(50),
    })
    .unwrap();
    with_fixture_ca(plan, &redis_url)
}

fn with_fixture_ca(plan: RedisCachePlan, redis_url: &str) -> RedisCachePlan {
    if !redis_url.starts_with("rediss://") {
        return plan;
    }
    let path = std::env::var("LILY_REDIS_CA_FILE")
        .expect("LILY_REDIS_CA_FILE is required for fixture rediss:// URLs");
    let pem = std::fs::read(path).expect("read fixture Redis CA bundle");
    plan.additional_ca_pem(pem)
        .expect("fixture Redis CA bundle must be valid")
}

#[cfg(feature = "factory")]
#[tokio::test]
#[ignore = "ENVIRONMENT-RESTRICTED: requires two disposable Redis fixtures"]
async fn factory_publishes_two_ready_cells_atomically() {
    let factory = lily_redis::CacheFactory::connect([
        ("sessions".into(), cell_plan("LILY_REDIS_URL_A", "sessions")),
        (
            "ratelimits".into(),
            cell_plan("LILY_REDIS_URL_B", "ratelimits"),
        ),
    ])
    .await
    .unwrap();

    let sessions = factory.try_get("sessions").unwrap().unwrap();
    let ratelimits = factory.try_get("ratelimits").unwrap().unwrap();
    sessions.set_json("probe", &"a").await.unwrap();
    ratelimits.set_json("probe", &"b").await.unwrap();
    assert_eq!(
        sessions.get::<String>("probe").await.unwrap().as_deref(),
        Some("a")
    );
    assert_eq!(
        ratelimits.get::<String>("probe").await.unwrap().as_deref(),
        Some("b")
    );
    let first = ratelimits
        .fixed_window_rate_limit("factory-rate", 1, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let rejected = ratelimits
        .fixed_window_rate_limit("factory-rate", 1, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    assert!(first.allowed());
    assert!(!rejected.allowed());

    lily_injection::ServiceTrait::dispose(&factory)
        .await
        .unwrap();
}
