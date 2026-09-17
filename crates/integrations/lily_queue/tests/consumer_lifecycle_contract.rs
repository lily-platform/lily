//! Source-level regression gates for lifecycle ordering that is otherwise only
//! observable with a live RabbitMQ channel.
//!
//! The behavioral live variants remain ignored qualification fixtures. These
//! short tests keep the local implementation from silently reintroducing the
//! registration and shutdown races.

const ENGINE_SOURCE: &str = include_str!("../src/providers/rabbitmq/queue_engine.rs");
const TELEMETRY_SOURCE: &str = include_str!("../src/telemetry.rs");

fn production_engine_source() -> &'static str {
    ENGINE_SOURCE
        .split("// #[async_trait]")
        .next()
        .expect("production queue engine source")
}

#[test]
fn consumer_registration_is_published_only_after_open_succeeds() {
    let source = production_engine_source();
    let registration = source
        .split("async fn create_consumer(")
        .nth(1)
        .and_then(|source| source.split("fn delivery_terminal_snapshot").next())
        .expect("create_consumer implementation");
    let open = registration
        .find("open_consumer")
        .expect("consumer open operation");
    let publish = registration
        .find("task_buffers.insert")
        .expect("successful registration publication");

    assert!(
        open < publish,
        "a failed initial open must not leave a published registration"
    );
    assert!(
        !registration.contains("if task_buffers.insert"),
        "duplicate registration must not replace an active sender"
    );

    let adopt = registration
        .find("adopt_supervised_queue_task")
        .expect("engine-owned registration task adoption");
    let observe = registration
        .find("startup_rx.await")
        .expect("caller startup observation");
    assert!(
        adopt < observe,
        "the engine must retain the registration task before caller readiness can be awaited"
    );

    let committed = &registration[publish..];
    let startup_commit = committed
        .find("publish_registration_readiness")
        .expect("atomic startup/readiness commit");
    let gate_release = committed
        .find("drop(registration)")
        .expect("registration gate release");
    assert!(
        startup_commit < gate_release,
        "shutdown may cross the registration gate only after startup and readiness commit"
    );

    let readiness_commit = source
        .split("fn publish_registration_readiness(")
        .nth(1)
        .and_then(|source| source.split("struct BackgroundDrainOutcome").next())
        .expect("registration readiness commit helper");
    let send = readiness_commit
        .find("startup.send(Ok(()))")
        .expect("startup result publication");
    let ready = readiness_commit
        .find("ConsumerReadinessGuard::opened")
        .expect("readiness publication");
    assert!(
        ready < send,
        "readiness must commit before another runtime worker can observe startup success"
    );
}

#[test]
fn accepted_buffered_deliveries_have_an_explicit_terminal_bucket() {
    for required in [
        "buffered_pending_redelivery",
        "finish_buffered_pending_redelivery",
    ] {
        assert!(
            TELEMETRY_SOURCE.contains(required) && ENGINE_SOURCE.contains(required),
            "missing accepted-delivery accounting marker: {required}"
        );
    }
}

#[test]
fn runtime_recovery_exposes_state_and_uses_bounded_configured_backoff() {
    let source = production_engine_source();
    for required in [
        "ConsumerRuntimeState",
        "Recovering",
        "Draining",
        "ready_consumers",
    ] {
        assert!(
            source.contains(required) || TELEMETRY_SOURCE.contains(required),
            "missing machine-readable recovery state marker: {required}"
        );
    }
    assert!(
        !source.contains("Duration::from_millis(250)"),
        "runtime recovery must use the bounded configured backoff, not a hardcoded delay"
    );
}
