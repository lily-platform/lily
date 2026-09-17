mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/raw-response-typed")]
struct RawResponseTypedController;

impl_controller_trait!(RawResponseTypedController);

#[controller]
impl RawResponseTypedController {
    #[get("/")]
    async fn index(
        &self,
        _response: &mut Response,
    ) -> Result<String, HttpApiError> {
        Ok("typed".to_string())
    }
}

fn main() {}
