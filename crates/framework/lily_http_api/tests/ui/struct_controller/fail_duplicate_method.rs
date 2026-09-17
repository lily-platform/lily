mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/duplicate-method")]
struct DuplicateMethodController;

impl_controller_trait!(DuplicateMethodController);

#[controller]
impl DuplicateMethodController {
    #[get("/")]
    #[post("/")]
    async fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
