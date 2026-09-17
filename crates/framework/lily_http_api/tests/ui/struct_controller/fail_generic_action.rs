mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/generic-action")]
struct GenericActionController;

impl_controller_trait!(GenericActionController);

#[controller]
impl GenericActionController {
    #[get("/")]
    async fn index<T>(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
