use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Injectable)]
#[service(config = AppConfig)]
struct ConfiguredService;

struct AppConfig;

impl ServiceTrait for ConfiguredService {}

fn main() {}
