use lily_shutdown::FrameworkShutdownComponent;
use lily_trace::{
    tracing_runtime_status, TraceConfig, TraceInstallError, TraceInstallOutcome,
    TracingRuntimeOwner, TracingRuntimeStatus, TracingShutdownHandle,
};
use std::process::Command;
use std::time::Duration;

const CHILD_CASE: &str = "LILY_TRACE_OWNER_CHILD";

#[test]
fn tracing_owner_contract_is_verified_in_an_isolated_process() {
    if std::env::var_os(CHILD_CASE).is_some() {
        run_owner_contract();
        return;
    }

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("tracing_owner_contract_is_verified_in_an_isolated_process")
        .arg("--nocapture")
        .env(CHILD_CASE, "1")
        .status()
        .expect("spawn isolated tracing contract test");
    assert!(status.success(), "isolated tracing contract failed");
}

fn run_owner_contract() {
    assert!(matches!(
        TracingRuntimeOwner::install(&TraceConfig::default()).unwrap(),
        TraceInstallOutcome::Disabled
    ));
    assert_eq!(
        tracing_runtime_status(),
        TracingRuntimeStatus::Uninitialized
    );

    let config = TraceConfig {
        enabled: true,
        service_name: "owner-contract".to_string(),
        ..TraceConfig::default()
    };
    let owner = match TracingRuntimeOwner::install(&config).unwrap() {
        TraceInstallOutcome::Owned(owner) => owner,
        TraceInstallOutcome::Disabled => panic!("enabled config did not produce an owner"),
    };
    assert_eq!(tracing_runtime_status(), TracingRuntimeStatus::Initialized);
    assert!(matches!(
        TracingRuntimeOwner::install(&config),
        Err(TraceInstallError::AlreadyInitialized)
    ));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (mut shutdown, evidence) = TracingShutdownHandle::new(owner, Duration::from_secs(2));
    runtime.block_on(async {
        shutdown.shutdown().await.expect("tracing handle shutdown");
        shutdown
            .force_shutdown()
            .expect("tracing handle exposes force reconciliation")
            .await
            .expect("force waiter replays terminal success");
        assert!(evidence.owner_joined());
        assert!(
            evidence
                .reconcile_before(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
        );
        assert!(evidence.workers().is_terminal());
    });
    let report = evidence.report().expect("tracing terminal evidence");
    assert!(report.is_success(), "shutdown report: {report:?}");
    assert_eq!(tracing_runtime_status(), TracingRuntimeStatus::Shutdown);
}
