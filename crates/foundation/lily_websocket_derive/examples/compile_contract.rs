use lily_websocket_client::TokioWsClient;
use lily_websocket_derive::BaseGateway;
use serde::Serialize;

#[derive(Serialize)]
struct AuditDto {
    id: String,
}

#[derive(BaseGateway)]
#[gateway(url = "wss://gateway.example.invalid", namespace = "audit")]
#[dto_type(AuditDto)]
struct AuditGateway {
    #[gateway_client]
    client: TokioWsClient,
    entity_name: String,
}

fn main() {
    let _dto = AuditDto { id: String::new() };
    let gateway = AuditGateway::new();
    assert!(!gateway.entity_name.is_empty());
}
