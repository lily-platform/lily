use lily_trace::runtime::{ExportConfig, FileExportConfig, FileRotation};
use lily_trace::{
    tracing_runtime_status, FileExportShutdownStatus, TraceConfig, TraceInstallError,
    TraceInstallOutcome, TracingRuntimeOwner, TracingRuntimeStatus,
};
use std::time::Duration;

#[test]
fn file_export_failure_rolls_back_the_claim_and_valid_runtime_flushes() {
    let directory = tempfile::tempdir().unwrap();
    let missing_path = directory.path().join("missing").join("trace.jsonl");
    let invalid = file_config(missing_path.display().to_string());
    match TracingRuntimeOwner::install(&invalid) {
        Err(TraceInstallError::FileExporter(error)) => {
            assert_eq!(error.path(), missing_path);
        }
        other => panic!("expected typed file exporter error, got {other:?}"),
    }
    assert_eq!(
        tracing_runtime_status(),
        TracingRuntimeStatus::Uninitialized
    );

    let path = directory.path().join("trace.jsonl");
    let valid = file_config(path.display().to_string());
    let owner = match TracingRuntimeOwner::install(&valid).unwrap() {
        TraceInstallOutcome::Owned(owner) => owner,
        TraceInstallOutcome::Disabled => panic!("enabled file config did not produce an owner"),
    };
    tracing::info!(
        lily.test = "file-runtime",
        "file runtime qualification event"
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let report = runtime.block_on(owner.shutdown(Duration::from_secs(3)));
    assert!(report.is_success(), "shutdown report: {report:?}");
    match report.file_export {
        FileExportShutdownStatus::Completed(metrics) => {
            assert_eq!(metrics.total_dropped(), 0);
        }
        status => panic!("unexpected file shutdown status: {status:?}"),
    }

    let output = std::fs::read_to_string(path).unwrap();
    let line = output.lines().next().expect("one JSONL record");
    let record: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(record["fields"]["lily.test"], "file-runtime");
}

fn file_config(path: String) -> TraceConfig {
    TraceConfig {
        enabled: true,
        service_name: "file-runtime-contract".to_string(),
        export: ExportConfig {
            file: Some(FileExportConfig {
                path,
                rotation: FileRotation::Daily,
                max_file_bytes: 1024 * 1024,
                max_files: 2,
                buffered_lines: 64,
                max_record_bytes: 16 * 1024,
            }),
            ..ExportConfig::default()
        },
        ..TraceConfig::default()
    }
}
