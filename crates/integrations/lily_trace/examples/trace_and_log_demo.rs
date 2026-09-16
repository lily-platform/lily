use std::time::Duration;

use lily_trace::{TraceConfig, TraceInstallOutcome, TracingRuntimeOwner};
use tracing::{error, info, info_span, warn, Instrument};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = TraceConfig::try_load()?;
    let owner = match TracingRuntimeOwner::install(&config)? {
        TraceInstallOutcome::Disabled => None,
        TraceInstallOutcome::Owned(owner) => Some(owner),
    };

    async {
        info!(lily.outcome = "success", "request accepted");

        async {
            info!(db.system = "mongodb", "dependency call started");
            tokio::time::sleep(Duration::from_millis(45)).await;
            info!(lily.outcome = "success", "dependency call completed");
        }
        .instrument(info_span!("database.query", db.system = "mongodb"))
        .await;

        warn!(lily.outcome = "throttled", "bounded warning example");
        error!(error.type = "insufficient_funds", "operation failed");
    }
    .instrument(info_span!(
        "http.server.request",
        http.route = "/api/users/{id}",
        http.request.method = "GET"
    ))
    .await;

    if let Some(owner) = owner {
        let report = owner.shutdown(Duration::from_secs(10)).await;
        if !report.is_success() {
            return Err(format!("telemetry shutdown incomplete: {report:?}").into());
        }
    }

    Ok(())
}
