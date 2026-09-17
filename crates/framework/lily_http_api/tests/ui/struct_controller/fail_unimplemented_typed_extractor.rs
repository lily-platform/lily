mod support;

use support::*;

struct UnknownExtractor;

#[derive(Controller)]
#[base_path("/api/unknown-extractor")]
struct UnknownExtractorController;

impl_controller_trait!(UnknownExtractorController);

#[controller]
impl UnknownExtractorController {
    #[get("/")]
    async fn index(
        &self,
        _unknown: UnknownExtractor,
        _request: &mut Request,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
