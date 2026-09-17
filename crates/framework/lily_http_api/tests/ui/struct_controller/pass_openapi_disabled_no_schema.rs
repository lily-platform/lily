mod support;

use lily_http_api::Json;
use serde::{Deserialize, Serialize};
use support::*;

#[derive(Deserialize)]
struct UndocumentedInput {
    value: String,
}

#[derive(Serialize)]
struct UndocumentedView {
    value: String,
}

#[derive(Controller)]
#[base_path("/api/openapi-disabled")]
struct OpenApiDisabledController;

impl_controller_trait!(OpenApiDisabledController);

#[controller]
impl OpenApiDisabledController {
    #[post("/")]
    async fn create(
        &self,
        Json(input): Json<UndocumentedInput>,
    ) -> Result<UndocumentedView, HttpApiError> {
        Ok(UndocumentedView { value: input.value })
    }
}

fn main() {}
