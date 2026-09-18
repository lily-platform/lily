use crate::DemoError;
use diesel::{
    OptionalExtension,
    sql_types::{BigInt, Text},
};
use lily_example_models::{JobRequested, JobView};
use lilyrs::{
    injection::{Injectable, ServiceTrait},
    postgresql::{ExecutionCancellation, PgDbContext, PgError, diesel, diesel_async::RunQueryDsl},
    trace::lily_trace,
};
use std::sync::Arc;

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
pub struct JobRepository {
    #[inject]
    context: Arc<PgDbContext>,
}
impl ServiceTrait for JobRepository {}

#[derive(diesel::QueryableByName)]
struct JobRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    text: String,
    #[diesel(sql_type = Text)]
    result: String,
}
#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

impl JobRepository {
    #[lily_trace(name = "example.job.find", result)]
    pub async fn find(
        &self,
        id: String,
        token: Option<ExecutionCancellation>,
    ) -> Result<Option<JobView>, DemoError> {
        self.context
            .with_connection(
                move |connection, _| {
                    Box::pin(async move {
                        let row = diesel::sql_query(
                            "SELECT id, text, result FROM example_jobs WHERE id = $1",
                        )
                        .bind::<Text, _>(id)
                        .get_result::<JobRow>(connection)
                        .await
                        .optional()
                        .map_err(PgError::from)?;
                        Ok(row.map(|r| JobView {
                            id: r.id,
                            text: r.text,
                            result: r.result,
                        }))
                    })
                },
                token,
            )
            .await
    }

    pub async fn insert(
        &self,
        job: JobRequested,
        token: Option<ExecutionCancellation>,
    ) -> Result<(), DemoError> {
        self.context.with_connection(move |connection, _| Box::pin(async move {
            diesel::sql_query("INSERT INTO example_jobs (id, text, result) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING")
                .bind::<Text, _>(&job.id).bind::<Text, _>(&job.text).bind::<Text, _>(job.text.to_uppercase())
                .execute(connection).await.map_err(PgError::from)?;
            Ok(())
        }), token).await
    }

    pub async fn record_processed(
        &self,
        id: String,
        token: Option<ExecutionCancellation>,
    ) -> Result<(), DemoError> {
        self.context.with_connection(move |connection, _| Box::pin(async move {
            diesel::sql_query("INSERT INTO example_processed_events (job_id) VALUES ($1) ON CONFLICT (job_id) DO NOTHING")
                .bind::<Text, _>(id).execute(connection).await.map_err(PgError::from)?;
            Ok(())
        }), token).await
    }

    pub async fn count(&self, token: Option<ExecutionCancellation>) -> Result<i64, DemoError> {
        self.context
            .with_connection(
                move |connection, _| {
                    Box::pin(async move {
                        let row = diesel::sql_query(
                            "SELECT COUNT(*) AS count FROM example_processed_events",
                        )
                        .get_result::<CountRow>(connection)
                        .await
                        .map_err(PgError::from)?;
                        Ok(row.count)
                    })
                },
                token,
            )
            .await
    }
}
