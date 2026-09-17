//! Process isolation for the real, process-global file tracing exporter.
use lily_http_api::{
    AppBuilder, ApplicationScopeFactory, BackgroundServiceTrait, CancellationToken,
    ExecutionCancellation, InjectionError, ProcessContext,
};
use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use lily_trace::{
    runtime::{ExportConfig, FileExportConfig, FileRotation},
    TraceConfig,
};
use std::{sync::Arc, time::Duration};

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Probe {
    entered: CancellationToken,
    disposed: CancellationToken,
}
#[async_trait::async_trait]
impl ServiceTrait for Probe {
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert!(self.disposed.is_cancelled());
        tracing::info!(background_stage = "dependency-disposed");
        Ok(())
    }
}
#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Scoped {
    #[inject]
    probe: Arc<Probe>,
}
struct EndDisposal<'a>(&'a Probe);
impl Drop for EndDisposal<'_> {
    fn drop(&mut self) {
        self.0.disposed.cancel();
        tracing::info!(background_stage = "scope-ended");
    }
}
#[async_trait::async_trait]
impl ServiceTrait for Scoped {
    #[lily_trace::lily_trace(name = "background.scope.dispose")]
    async fn dispose(&self) -> Result<(), InjectionError> {
        let _end = EndDisposal(&self.probe);
        tracing::info!(background_stage = "scope-disposing");
        match std::env::var("LILY_BACKGROUND_EXPORT_CASE")
            .unwrap()
            .as_str()
        {
            "cleanup_timeout" => futures::future::pending().await,
            "cleanup_error" => Err(InjectionError::DisposeError(
                "fixture cleanup failure".into(),
            )),
            _ => Ok(()),
        }
    }
}
struct Worker {
    scopes: Arc<ApplicationScopeFactory>,
}
#[async_trait::async_trait]
impl BackgroundServiceTrait for Worker {
    type Error = InjectionError;
    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error> {
        Ok(Self { scopes })
    }
    async fn execute_async(&mut self, token: ExecutionCancellation) -> Result<(), Self::Error> {
        self.job(token).await
    }
}
impl Worker {
    #[lily_trace::lily_trace(name = "background.job")]
    async fn job(&self, token: ExecutionCancellation) -> Result<(), InjectionError> {
        self.scopes
            .create_scope(ProcessContext::new())?
            .run(move |extensions| {
                Box::pin(async move {
                    let scoped = extensions.get_service::<Scoped>(None).await?;
                    tracing::info!(background_stage = "job-entered");
                    scoped.probe.entered.cancel();
                    if std::env::var("LILY_BACKGROUND_EXPORT_CASE").as_deref() == Ok("forced") {
                        futures::future::pending::<()>().await;
                    }
                    token.cancelled().await;
                    Ok::<_, InjectionError>(())
                })
            })
            .await
    }
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .unwrap()
}

#[tokio::test]
async fn background_scope_and_dependency_events_are_flushed_with_actual_trace_identity() {
    let Ok(case) = std::env::var("LILY_BACKGROUND_EXPORT_CASE") else {
        for case in ["cooperative", "forced", "cleanup_error", "cleanup_timeout"] {
            let result = bounded(tokio::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "background_scope_and_dependency_events_are_flushed_with_actual_trace_identity", "--nocapture"])
                .env("LILY_BACKGROUND_EXPORT_CASE", case)
                .env("LILY__LIFECYCLE__SHUTDOWN_TIMEOUT_SECS", "2")
                .kill_on_drop(true).output()).await.unwrap();
            assert!(
                result.status.success(),
                "{case}: {}{}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
        }
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("background.jsonl");
    let config = TraceConfig {
        enabled: true,
        export: ExportConfig {
            console: false,
            file: Some(FileExportConfig {
                path: path.display().to_string(),
                rotation: FileRotation::Daily,
                max_file_bytes: 1024 * 1024,
                max_files: 2,
                buffered_lines: 64,
                max_record_bytes: 16 * 1024,
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    let app = AppBuilder::new("127.0.0.1:0")
        .tracing_config(config)
        .add_background_service::<Worker>()
        .build()
        .await
        .unwrap();
    let probe = app.container().resolve::<Probe>(None).await.unwrap();
    let health = app
        .container()
        .resolve::<lily_http_api::HttpHealthService>(None)
        .await
        .unwrap();
    let runtime = app.clone();
    let root = tokio::spawn(async move {
        runtime
            .start_with_cancellation(CancellationToken::new())
            .await
    });
    bounded(probe.entered.cancelled()).await;
    let closed = bounded(app.close()).await;
    let root = bounded(root).await.unwrap();
    assert_eq!(closed.is_err(), case.starts_with("cleanup_"), "{closed:?}");
    assert_eq!(root.is_err(), case.starts_with("cleanup_"), "{root:?}");
    assert_eq!(
        lily_trace::tracing_runtime_status(),
        lily_trace::TracingRuntimeStatus::Shutdown
    );
    let snapshot = health.snapshot().unwrap();
    assert_eq!(
        snapshot
            .checks
            .iter()
            .find(|check| check.name == "http.shutdown")
            .unwrap()
            .reason_code,
        match case.as_str() {
            "cooperative" => "graceful_completed",
            "forced" => "forced_completed",
            _ => "terminal_failed",
        }
    );
    let records: Vec<serde_json::Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let exactly = |field: &str, value: &str| {
        let positions: Vec<_> = records
            .iter()
            .enumerate()
            .filter_map(|(i, r)| (r["fields"][field] == value).then_some(i))
            .collect();
        assert_eq!(positions.len(), 1, "exactly one {field}={value}");
        positions[0]
    };
    let job = exactly("background_stage", "job-entered");
    let disposing = exactly("background_stage", "scope-disposing");
    let ended = exactly("background_stage", "scope-ended");
    let dependency = exactly("background_stage", "dependency-disposed");
    let checkpoint = exactly("checkpoint", "before_telemetry");
    assert!(job < disposing && disposing < ended && ended < dependency && dependency < checkpoint);
    let trace_id = records[job]["trace_id"].as_str().unwrap();
    assert_eq!(trace_id.len(), 32);
    assert_ne!(trace_id, "00000000000000000000000000000000");
    assert_eq!(records[disposing]["trace_id"], trace_id);
    assert_eq!(records[ended]["trace_id"], trace_id);
    assert_eq!(records[disposing]["span_id"], records[ended]["span_id"]);
    assert_ne!(records[job]["span_id"], records[disposing]["span_id"]);
    assert_eq!(records[checkpoint]["fields"]["background_outstanding"], 0);
    assert_eq!(
        records[checkpoint]["fields"]["background_scope_outstanding"],
        0
    );
    let terminal: Vec<_> = records
        .iter()
        .filter(|r| {
            r["target"] == "lily_background_service"
                && r["fields"]["lifecycle"] != "started"
                && r["fields"]["lifecycle"].is_string()
        })
        .collect();
    assert_eq!(terminal.len(), 1);
    assert!(terminal[0]["fields"]["duration_ms"]
        .as_f64()
        .unwrap()
        .is_finite());
    assert_eq!(app.close().await.is_err(), case.starts_with("cleanup_"));
    assert_eq!(health.snapshot().unwrap(), snapshot);
}
