//! Real facade clients, also used by the bounded end-to-end smoke check.
use lily_example_models::{GetJob, JobTicket, JobView, NoteInput, NoteView, SubmitJob};
use lilyrs::{
    http_client::{HttpClient, HttpClientBuilder},
    websocket_client::{
        DecodedPayload, TokioWsClient, WebSocketClientConfig, WebSocketReply, WsClient,
    },
};
use serde_json::{Value, json};
use std::{error::Error, time::Duration};
use tokio_util::sync::CancellationToken;

type AnyError = Box<dyn Error + Send + Sync>;

fn base_url() -> String {
    std::env::var("EXAMPLE_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:58100".into())
}
fn client() -> Result<HttpClient, AnyError> {
    Ok(HttpClientBuilder::new()
        .connect_timeout(Duration::from_secs(3))
        .request_timeout(Duration::from_secs(5))
        .try_build()?)
}
async fn http(
    method: &str,
    path: &str,
    body: Option<Value>,
) -> Result<(u16, Value, Option<String>), AnyError> {
    let client = client()?;
    let url = format!("{}{path}", base_url());
    let mut builder = match method {
        "GET" => client.get(&url)?,
        "POST" => client.post(&url)?,
        "PUT" => client.put(&url)?,
        "DELETE" => client.delete(&url)?,
        _ => return Err("unsupported method".into()),
    };
    if let Some(body) = body {
        builder.json(&body)?;
    }
    let mut response = client.execute(builder.build()?).await?;
    let status = response.status().as_u16();
    let location = response.headers().get("location").map(str::to_owned);
    let payload = if status == 204 {
        Value::Null
    } else {
        response.json::<Value>().await?
    };
    Ok((status, payload, location))
}

async fn ws(event: &str, payload: Value) -> Result<WebSocketReply, AnyError> {
    let client = TokioWsClient::with_config(WebSocketClientConfig {
        url: std::env::var("EXAMPLE_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:58101/ws".into()),
        namespace: Some("jobs".into()),
        connect_timeout_secs: 3,
        ..Default::default()
    })?;
    let cancellation = CancellationToken::new();
    client.connect(cancellation.clone()).await?;
    let result = client.request(event, payload, Duration::from_secs(5)).await;
    // Join the runtime even when the action is rejected or its deadline expires.
    let closed = client.disconnect().await;
    cancellation.cancel();
    let reply = result?;
    closed?;
    Ok(reply)
}

fn require(condition: bool, message: &str) -> Result<(), AnyError> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
async fn queue_status() -> Result<Value, AnyError> {
    let client = client()?;
    let base =
        std::env::var("EXAMPLE_RABBIT_URL").unwrap_or_else(|_| "http://127.0.0.1:55673".into());
    let mut builder = client.get(&format!("{base}/api/queues/%2F/lily.examples.jobs"))?;
    builder.basic_auth("lily", Some("lily_example"))?;
    let mut response = client.execute(builder.build()?).await?;
    require(
        response.status().as_u16() == 200,
        "RabbitMQ queue is not provisioned",
    )?;
    Ok(response.json().await?)
}

async fn smoke() -> Result<(), AnyError> {
    let (status, health, _) = http("GET", "/health", None).await?;
    require(
        status == 200 && health["status"] == "ready",
        "HTTP health contract failed",
    )?;
    require(
        matches!(ws("jobs:ping", json!({})).await?, WebSocketReply::Acknowledgement(DecodedPayload::Json(v)) if v == "pong"),
        "WebSocket ping failed",
    )?;

    let id = uuid::Uuid::new_v4().to_string();
    let job = SubmitJob {
        id: id.clone(),
        text: "hello lily".into(),
    };
    let (status, payload, location) =
        http("POST", "/jobs", Some(serde_json::to_value(&job)?)).await?;
    let ticket: JobTicket = serde_json::from_value(payload)?;
    require(
        status == 202 && ticket.id == id && location == Some(format!("/jobs/{id}")),
        "HTTP 202/Location contract failed",
    )?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let completed: JobView = loop {
        let (status, body, _) = http("GET", &format!("/jobs/{id}"), None).await?;
        if status == 200 {
            break serde_json::from_value(body)?;
        }
        require(
            status == 404 && body["code"] == "NOT_FOUND",
            "Unexpected response while awaiting consumer",
        )?;
        require(
            tokio::time::Instant::now() < deadline,
            "Consumer did not complete within 30 seconds",
        )?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    require(
        completed
            == JobView {
                id: id.clone(),
                text: job.text.clone(),
                result: "HELLO LILY".into(),
            },
        "Consumer result differs from submitted text",
    )?;
    // Repeated reads exercise the shared Redis-backed service, including via WebSocket.
    let (status, body, _) = http("GET", &format!("/jobs/{id}"), None).await?;
    require(
        status == 200 && serde_json::from_value::<JobView>(body)? == completed,
        "Repeated read mismatch",
    )?;
    match ws("jobs:get", serde_json::to_value(GetJob { id: id.clone() })?).await? {
        WebSocketReply::Acknowledgement(DecodedPayload::Json(body)) => require(
            serde_json::from_value::<JobView>(body)? == completed,
            "WebSocket result mismatch",
        )?,
        _ => return Err("Expected a correlated JSON acknowledgement".into()),
    }
    // Same event ID is published again; storage verification checks exactly one row.
    let (status, _, _) = http("POST", "/jobs", Some(serde_json::to_value(&job)?)).await?;
    require(status == 202, "Duplicate submission should be accepted")?;
    let (status, body, _) = http(
        "POST",
        "/jobs",
        Some(json!({"id": id, "text": "different"})),
    )
    .await?;
    require(
        status == 409 && body["code"] == "CONFLICT",
        "Conflicting job must be rejected",
    )?;
    let (status, body, _) = http("POST", "/jobs", Some(json!({"id": id, "text": " "}))).await?;
    require(
        status == 400 && body["code"] == "INVALID_INPUT",
        "Blank text must be rejected",
    )?;
    match ws("jobs:get", json!({"id": "invalid"})).await? {
        WebSocketReply::Rejection(DecodedPayload::Json(body)) => require(
            body["code"] == "INVALID_INPUT",
            "Wrong WebSocket rejection code",
        )?,
        _ => return Err("Invalid ID must produce a correlated WebSocket rejection".into()),
    }

    let (status, body, location) = http(
        "POST",
        "/notes",
        Some(serde_json::to_value(NoteInput {
            text: "first".into(),
        })?),
    )
    .await?;
    let note: NoteView = serde_json::from_value(body)?;
    require(
        status == 201 && note.text == "first" && location == Some(format!("/notes/{}", note.id)),
        "MongoDB create/Location failed",
    )?;
    let path = format!("/notes/{}", note.id);
    let (status, body, _) = http("GET", &path, None).await?;
    require(
        status == 200 && serde_json::from_value::<NoteView>(body)? == note,
        "MongoDB read failed",
    )?;
    let (status, body, _) = http("PUT", &path, Some(json!({"text": "updated"}))).await?;
    require(
        status == 200 && body["text"] == "updated" && body["id"] == note.id,
        "MongoDB update failed",
    )?;
    let (status, body, _) = http("GET", &path, None).await?;
    require(
        status == 200 && body["text"] == "updated",
        "MongoDB update was not persisted",
    )?;
    let (status, _, _) = http("DELETE", &path, None).await?;
    require(status == 204, "MongoDB delete should return 204")?;
    let (status, body, _) = http("GET", &path, None).await?;
    require(
        status == 404 && body["code"] == "NOT_FOUND",
        "Deleted MongoDB document must be absent",
    )?;
    println!(
        "{}",
        json!({"smoke": "passed", "job_id": id, "deleted_note_id": note.id})
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("smoke") => smoke().await?,
        Some("consumer-health") => require(queue_status().await?["consumers"].as_u64().unwrap_or(0) >= 1, "Consumer is not attached to its queue")?,
        Some("queue-status") => println!("{}", queue_status().await?),
        Some("health") => {
            let (status, body, _) = http("GET", "/health", None).await?;
            require(status == 200 && body["status"] == "ready", "HTTP is not ready")?;
        }
        Some("ws-health") => require(matches!(ws("jobs:ping", json!({})).await?, WebSocketReply::Acknowledgement(DecodedPayload::Json(v)) if v == "pong"), "WebSocket is not ready")?,
        Some("submit") if args.len() == 2 => {
            let id = uuid::Uuid::new_v4().to_string();
            let (status, body, _) = http("POST", "/jobs", Some(json!({"id": id, "text": args[1]}))).await?;
            require(status == 202, "Job submission failed")?;
            println!("{body}");
        }
        Some("get") if args.len() == 2 => {
            let (status, body, _) = http("GET", &format!("/jobs/{}", args[1]), None).await?;
            require(status == 200, "Job is not completed or was not found")?;
            println!("{body}");
        }
        Some("ws-get") if args.len() == 2 => println!("{:?}", ws("jobs:get", json!({"id": args[1]})).await?),
        _ => return Err("Usage: lily-example-client smoke | health | ws-health | submit TEXT | get UUID | ws-get UUID".into()),
    }
    Ok(())
}
