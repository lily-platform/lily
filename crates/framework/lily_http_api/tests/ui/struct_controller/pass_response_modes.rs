mod support;

use lily_http_api::{
    NoContent, PassthroughResponseContext, PassthroughResponseError, ResponseCookie,
};
use support::*;

#[derive(Controller)]
#[base_path("/api/response-modes")]
struct ResponseModeController;

impl_controller_trait!(ResponseModeController);

#[controller]
impl ResponseModeController {
    #[get("/unit")]
    async fn unit(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[delete("/no-content")]
    async fn no_content(&self) -> NoContent {
        NoContent
    }

    #[delete("/no-content-result")]
    async fn no_content_result(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[post("/manual")]
    async fn manual(&self, response: &mut Response) -> Result<(), HttpApiError> {
        response.status(201, "Created");
        Ok(())
    }

    #[post("/passthrough")]
    async fn passthrough(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<String, HttpApiError> {
        response.status(201)?;
        response.insert_header("X-Created", "true")?;
        let cookie =
            ResponseCookie::new("session", "opaque").map_err(PassthroughResponseError::from)?;
        response.set_cookie(&cookie)?;
        Ok("created".to_string())
    }

    #[delete("/passthrough-no-content")]
    async fn passthrough_no_content(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<NoContent, HttpApiError> {
        response.insert_header("X-Deleted", "true")?;
        Ok(NoContent)
    }
}

fn main() {}
