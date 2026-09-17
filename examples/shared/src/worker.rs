use crate::{DemoError, JobRepository};
use lily::{
    background_service::{BackgroundServiceTrait, ExecutionCancellation},
    config::ConfigService,
    injection::{ApplicationScopeFactory, ProcessContext, async_trait::async_trait},
    trace::{lily_trace, tracing},
};
use std::{sync::Arc, time::Duration};

/// No scoped repository is retained by this long-lived worker.
pub struct SummaryWorker {
    scopes: Arc<ApplicationScopeFactory>,
    interval: Duration,
}
#[async_trait]
impl BackgroundServiceTrait for SummaryWorker {
    type Error = DemoError;
    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, DemoError> {
        let interval = scopes
            .create_scope(ProcessContext::new())?
            .run(|extensions| {
                Box::pin(async move {
                    let config = extensions.get_service::<ConfigService>(None).await?;
                    let value: String = config
                        .get("custom.worker_interval_ms")
                        .await
                        .map_err(|_| DemoError::Unavailable)?;
                    let millis = value.parse::<u64>().map_err(|_| DemoError::InvalidInput)?;
                    if !(100..=60_000).contains(&millis) {
                        return Err(DemoError::InvalidInput);
                    }
                    Ok::<_, DemoError>(Duration::from_millis(millis))
                })
            })
            .await?;
        Ok(Self { scopes, interval })
    }
    async fn execute_async(&mut self, stopping: ExecutionCancellation) -> Result<(), DemoError> {
        while !stopping.is_cancelled() {
            match self.run_once(stopping.clone()).await {
                Ok(()) => {}
                Err(DemoError::Cancelled) if stopping.is_cancelled() => break,
                // A worker owns its retry policy. Do not accidentally stop the host
                // for a transient statistics query failure.
                Err(error) => tracing::warn!(code = error.code(), "Summary iteration failed"),
            }
            tokio::select! {
                _ = stopping.cancelled() => break,
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
        tracing::info!("Summary worker stopped cooperatively");
        Ok(())
    }
}
impl SummaryWorker {
    #[lily_trace(name = "example.worker.summary", result)]
    async fn run_once(&self, token: ExecutionCancellation) -> Result<(), DemoError> {
        self.scopes
            .create_scope(ProcessContext::new())?
            .run(move |extensions| {
                Box::pin(async move {
                    let repository = extensions.get_service::<JobRepository>(None).await?;
                    let completed = repository.count(Some(token)).await?;
                    tracing::info!(completed_jobs = completed, "Job summary");
                    Ok::<_, DemoError>(())
                })
            })
            .await
    }
}
