use lily_queue_derive::queue_service;

struct AuditService;

#[queue_service]
impl AuditService {
    #[queue("audit.created", version = 1, content = "json")]
    #[allow(clippy::too_many_arguments)]
    async fn handle(
        &self,
        _a: String,
        _b: String,
        _c: String,
        _d: String,
        _e: String,
        _f: String,
        _g: String,
        _h: String,
        _i: String,
        _j: String,
        _k: String,
        _l: String,
        _m: String,
        _n: String,
        _o: String,
        _p: String,
        _q: String,
    ) -> Result<(), String> {
        Ok(())
    }
}

fn main() {}
