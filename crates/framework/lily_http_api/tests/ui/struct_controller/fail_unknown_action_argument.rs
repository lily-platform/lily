mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/unknown-argument")]
struct UnknownArgumentController;

impl_controller_trait!(UnknownArgumentController);

#[controller]
impl UnknownArgumentController {
    #[get("/", unexpected)]
    async fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
