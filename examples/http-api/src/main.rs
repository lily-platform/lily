use lily_example_models::{ErrorBody, JobTicket, JobView, NoteInput, NoteView, SubmitJob};
use lily_example_shared::{DemoError, JobOperations, NoteService, SummaryWorker, tracing_config};
use lilyrs::http_api::{
    Accepted, AppBuilder, Controller, ControllerInitError, ControllerTrait, Created, Extensions,
    HttpErrorCode, IntoResponse, Json, NoContent, Path, Request, Response, ResponseBuilder,
    ResponseFailureKind, ResponseWriteError, ResponseWriteOutcome, Service,
    async_trait::async_trait, controller,
};
use serde::Deserialize;
use std::sync::Arc;

struct ApiError(DemoError);
impl From<DemoError> for ApiError {
    fn from(error: DemoError) -> Self {
        Self(error)
    }
}

#[async_trait]
impl IntoResponse for ApiError {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        let (status, reason) = match self.0 {
            DemoError::InvalidInput => (400, "Bad Request"),
            DemoError::NotFound => (404, "Not Found"),
            DemoError::Conflict => (409, "Conflict"),
            DemoError::Unavailable | DemoError::Cancelled => (503, "Service Unavailable"),
        };
        let kind = if self.0.is_rejected() {
            ResponseFailureKind::Rejected
        } else {
            ResponseFailureKind::Error
        };
        ResponseBuilder::new()
            .status(status, reason)
            .json(ErrorBody {
                code: self.0.code().into(),
                message: self.0.to_string(),
            })
            .write_to_response(response, request)
            .await?;
        Ok(ResponseWriteOutcome::classified_error(
            kind,
            Some(HttpErrorCode::new(self.0.code()).expect("static error code")),
        ))
    }
}

#[derive(Deserialize)]
struct IdPath {
    id: String,
}

#[derive(Controller)]
#[base_path("/")]
struct DemoController;
#[async_trait]
impl ControllerTrait for DemoController {
    async fn new(_: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}
#[controller]
impl DemoController {
    #[get("/health")]
    async fn health(&self) -> Json<serde_json::Value> {
        Json(serde_json::json!({"status": "ready"}))
    }

    #[post("/jobs")]
    async fn submit(
        &self,
        service: Service<dyn JobOperations>,
        Json(input): Json<SubmitJob>,
    ) -> Result<Accepted<JobTicket>, ApiError> {
        let ticket = service.submit(input).await?;
        Ok(Accepted::new(format!("/jobs/{}", ticket.id), ticket))
    }

    #[get("/jobs/:id")]
    async fn get_job(
        &self,
        service: Service<dyn JobOperations>,
        Path(path): Path<IdPath>,
    ) -> Result<JobView, ApiError> {
        Ok(service.get(path.id).await?)
    }

    #[post("/notes")]
    async fn create_note(
        &self,
        service: Service<NoteService>,
        Json(input): Json<NoteInput>,
    ) -> Result<Created<NoteView>, ApiError> {
        let note = service.add(input).await?;
        Ok(Created::new(format!("/notes/{}", note.id), note))
    }
    #[get("/notes/:id")]
    async fn get_note(
        &self,
        service: Service<NoteService>,
        Path(path): Path<IdPath>,
    ) -> Result<NoteView, ApiError> {
        Ok(service.get(&path.id).await?)
    }
    #[put("/notes/:id")]
    async fn replace_note(
        &self,
        service: Service<NoteService>,
        Path(path): Path<IdPath>,
        Json(input): Json<NoteInput>,
    ) -> Result<NoteView, ApiError> {
        Ok(service.replace(&path.id, input).await?)
    }
    #[delete("/notes/:id")]
    async fn delete_note(
        &self,
        service: Service<NoteService>,
        Path(path): Path<IdPath>,
    ) -> Result<NoContent, ApiError> {
        service.remove(&path.id).await?;
        Ok(NoContent)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("EXAMPLE_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:58100".into());
    AppBuilder::new(&address)
        .tls_disabled()
        .tracing_config(tracing_config("http")?)
        .add_background_service::<SummaryWorker>()
        .build()
        .await?
        .start()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn application_codes_satisfy_http_transport_contract() {
        for error in [
            DemoError::InvalidInput,
            DemoError::NotFound,
            DemoError::Conflict,
            DemoError::Unavailable,
            DemoError::Cancelled,
        ] {
            assert!(HttpErrorCode::new(error.code()).is_ok(), "{}", error.code());
        }
    }
}
