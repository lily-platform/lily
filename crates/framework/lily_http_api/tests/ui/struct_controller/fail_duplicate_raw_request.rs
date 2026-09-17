mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/duplicate-request")]
struct DuplicateRequestController;

impl_controller_trait!(DuplicateRequestController);

#[controller]
impl DuplicateRequestController {
    #[get("/")]
    async fn index(&self, _first: &mut Request, _second: &mut Request) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
