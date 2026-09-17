mod support;

use support::*;

struct FirstGuard;
struct SecondGuard;

#[derive(Controller)]
#[base_path("/api/duplicate-guard")]
struct DuplicateGuardController;

impl_controller_trait!(DuplicateGuardController);

#[controller]
impl DuplicateGuardController {
    #[get("/")]
    #[guard(FirstGuard)]
    #[guard(SecondGuard)]
    async fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
