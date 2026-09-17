mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/missing-method")]
struct MissingMethodController;

impl_controller_trait!(MissingMethodController);

#[controller]
impl MissingMethodController {
    async fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
