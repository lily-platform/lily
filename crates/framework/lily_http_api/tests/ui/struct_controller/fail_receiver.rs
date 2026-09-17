mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/receiver")]
struct ReceiverController;

impl_controller_trait!(ReceiverController);

#[controller]
impl ReceiverController {
    #[get("/")]
    async fn index(&mut self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
