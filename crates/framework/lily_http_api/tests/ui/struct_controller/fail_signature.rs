mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/signature")]
struct SignatureController;

impl_controller_trait!(SignatureController);

#[controller]
impl SignatureController {
    #[get("/")]
    async fn index(
        &self,
        _request: &Request,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
