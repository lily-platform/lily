use lilyrs::trace::{ExportConfig, FileExportConfig, FileRotation, TraceConfig};

/// Each process owns a distinct file and lets its host flush tracing on shutdown.
pub fn tracing_config(service: &str) -> Result<TraceConfig, std::io::Error> {
    let directory = std::env::var("EXAMPLE_LOG_DIR").unwrap_or_else(|_| "logs".into());
    let console = std::env::var("EXAMPLE_TRACE_OUTPUT").is_ok_and(|value| value == "console");
    if !console {
        std::fs::create_dir_all(&directory)?;
    }
    Ok(TraceConfig {
        enabled: true,
        service_name: format!("lily-example-{service}"),
        environment: Some("development".into()),
        level: "info".into(),
        export: ExportConfig {
            console,
            file: (!console).then(|| FileExportConfig {
                path: format!("{directory}/{service}.jsonl"),
                rotation: FileRotation::Daily,
                max_file_bytes: 8 * 1024 * 1024,
                max_files: 3,
                buffered_lines: 1024,
                max_record_bytes: 64 * 1024,
            }),
            ..Default::default()
        },
        ..Default::default()
    })
}
