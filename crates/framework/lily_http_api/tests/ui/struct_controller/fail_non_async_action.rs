mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/non-async")]
struct NonAsyncController;

impl_controller_trait!(NonAsyncController);

#[controller]
impl NonAsyncController {
    #[get("/")]
    fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
