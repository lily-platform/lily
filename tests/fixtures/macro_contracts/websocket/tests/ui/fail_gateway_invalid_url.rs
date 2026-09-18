use lily_websocket_derive::BaseGateway;

struct AuditDto;

#[derive(BaseGateway)]
#[gateway(url = "https://example.invalid", namespace = "audit")]
#[dto_type(AuditDto)]
struct AuditGateway {
    #[gateway_client]
    client: (),
    entity_name: String,
}

fn main() {}
