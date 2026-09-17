mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/duplicate-response")]
struct DuplicateResponseController;

impl_controller_trait!(DuplicateResponseController);

#[controller]
impl DuplicateResponseController {
    #[get("/")]
    async fn index(
        &self,
        _first: &mut Response,
        _second: &mut Response,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
