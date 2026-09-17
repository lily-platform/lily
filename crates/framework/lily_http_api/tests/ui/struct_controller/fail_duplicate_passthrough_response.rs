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
        _first: PassthroughResponseContext<'_>,
        _second: PassthroughResponseContext<'_>,
    ) -> Result<String, HttpApiError> {
        Ok(String::new())
    }
}

fn main() {}
