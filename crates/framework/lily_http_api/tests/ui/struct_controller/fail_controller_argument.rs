mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/controller-argument")]
struct ControllerArgumentController;

impl_controller_trait!(ControllerArgumentController);

#[controller(unexpected)]
impl ControllerArgumentController {
    #[get("/")]
    async fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
