use crate::{DemoError, JobRepository, error::validate_id, validate_job};
use lily::{
    injection::{Injectable, ServiceTrait, async_trait::async_trait},
    postgresql::{ExecutionCancellation, PgDbContext},
    queue_client::{PublishMetadata, PublishSchemaVersion, QueueClientService},
    redis::{CacheService, ICache},
    trace::{lily_trace, tracing},
};
use lily_example_models::{JobRequested, JobTicket, JobView, SubmitJob};
use std::sync::Arc;

#[async_trait]
pub trait JobOperations: Send + Sync {
    async fn submit(&self, job: SubmitJob) -> Result<JobTicket, DemoError>;
    async fn get(&self, id: String) -> Result<JobView, DemoError>;
    async fn process(&self, job: JobRequested) -> Result<(), DemoError>;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped", interface = dyn JobOperations)]
pub struct JobService {
    #[inject]
    context: Arc<PgDbContext>,
    #[inject]
    repository: Arc<JobRepository>,
    #[inject]
    cache: Arc<CacheService>,
    #[inject]
    publisher: Arc<QueueClientService>,
}
impl ServiceTrait for JobService {}

#[async_trait]
impl JobOperations for JobService {
    // Result classification works with async_trait-expanded methods too.
    #[lily_trace(name = "example.job.submit", result)]
    async fn submit(&self, job: SubmitJob) -> Result<JobTicket, DemoError> {
        let event_id = validate_job(&job)?;
        if let Some(existing) = self.repository.find(job.id.clone(), None).await? {
            if existing.text != job.text {
                return Err(DemoError::Conflict);
            }
        }
        let ticket = JobTicket { id: job.id.clone() };
        let metadata = PublishMetadata::try_new(event_id, PublishSchemaVersion::V1)
            .map_err(|_| DemoError::InvalidInput)?;
        // Await RabbitMQ publisher confirmation before returning HTTP 202.
        self.publisher
            .publish_json_with_metadata("lily.examples", "jobs", metadata, job)
            .await
            .map_err(|_| DemoError::Unavailable)?;
        Ok(ticket)
    }

    #[lily_trace(name = "example.job.get", result)]
    async fn get(&self, id: String) -> Result<JobView, DemoError> {
        validate_id(&id)?;
        let key = format!("job:{id}");
        match self.cache.get::<JobView>(&key).await {
            Ok(Some(job)) => {
                tracing::debug!(cache_hit = true, "Completed job loaded from cache");
                return Ok(job);
            }
            Ok(None) => {}
            Err(_) => tracing::warn!(code = "cache_unavailable", "Reading job from PostgreSQL"),
        }
        let job = self
            .repository
            .find(id, None)
            .await?
            .ok_or(DemoError::NotFound)?;
        // Only immutable completed results are cached; a pending miss is never cached.
        if self.cache.set_json(&key, &job).await.is_err() {
            tracing::warn!(
                code = "cache_unavailable",
                "Completed job could not be cached"
            );
        }
        Ok(job)
    }

    #[lily_trace(name = "example.job.process", result)]
    async fn process(&self, job: JobRequested) -> Result<(), DemoError> {
        validate_job(&job)?;
        let repository = Arc::clone(&self.repository);
        let job_id = job.id.clone();
        self.context
            .transaction(
                move |token: Option<ExecutionCancellation>| async move {
                    // Both repository methods resolve the connection held by the same scoped context.
                    repository.insert(job.clone(), token.clone()).await?;
                    let stored = repository
                        .find(job.id.clone(), token.clone())
                        .await?
                        .ok_or(DemoError::NotFound)?;
                    if stored.text != job.text {
                        return Err(DemoError::Conflict);
                    }
                    repository.record_processed(job.id, token).await?;
                    Ok(())
                },
                None,
            )
            .await?;
        tracing::info!(job_id = %job_id, "Job processed");
        Ok(())
    }
}
