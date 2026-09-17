mod support;

use support::*;

struct FirstCors;
struct SecondCors;

#[derive(Controller)]
#[base_path("/api/duplicate-cors")]
struct DuplicateCorsController;

impl_controller_trait!(DuplicateCorsController);

#[controller]
impl DuplicateCorsController {
    #[get("/")]
    #[cors(FirstCors)]
    #[cors(SecondCors)]
    async fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
