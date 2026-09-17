use lily_websocket_derive::BaseGateway;

struct AuditDto;

#[derive(BaseGateway)]
#[gateway(url = "wss://example.invalid", namespace = "audit")]
#[dto_type(AuditDto)]
struct AuditGateway {
    entity_name: String,
}

fn main() {}
