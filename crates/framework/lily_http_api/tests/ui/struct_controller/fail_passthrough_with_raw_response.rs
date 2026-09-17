mod support;

use support::*;

#[derive(Controller)]
#[base_path("/invalid")]
struct InvalidController;

impl_controller_trait!(InvalidController);

#[controller]
impl InvalidController {
    #[get("/")]
    async fn invalid(
        &self,
        _raw: &mut Response,
        _passthrough: PassthroughResponseContext<'_>,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
