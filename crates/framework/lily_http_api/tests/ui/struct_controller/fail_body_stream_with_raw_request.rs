mod support;

use support::*;

struct BodyStream;

#[derive(Controller)]
#[base_path("/api/body-stream-request")]
struct BodyStreamRequestController;

impl_controller_trait!(BodyStreamRequestController);

#[controller]
impl BodyStreamRequestController {
    #[post("/")]
    async fn index(&self, _body: BodyStream, _request: &mut Request) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
