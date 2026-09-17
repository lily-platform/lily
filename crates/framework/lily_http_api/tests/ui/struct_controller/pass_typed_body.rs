mod support;

use lily_http_api::{BodyStream, Form, Json, RawBody};
use serde::Deserialize;
use support::*;

#[derive(Deserialize)]
struct Input {
    value: String,
}

#[derive(Controller)]
#[base_path("/api/typed-body")]
struct TypedBodyController;

impl_controller_trait!(TypedBodyController);

#[controller]
impl TypedBodyController {
    #[post("/json")]
    async fn json(
        &self,
        Json(_input): Json<Input>,
        _request: &mut Request,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[post("/form")]
    async fn form(
        &self,
        Form(_input): Form<Input>,
        _request: &mut Request,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[post("/raw")]
    async fn raw(&self, _body: RawBody, _request: &mut Request) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[post("/stream")]
    async fn stream(&self, mut body: BodyStream) -> Result<(), HttpApiError> {
        while body.next_chunk().await?.is_some() {}
        Ok(())
    }
}

fn main() {}
