//! Basic usage examples for lily_http_client
//!
//! This example demonstrates how to use the HTTP client for common tasks.

use lily_http_client::{HttpClient, HttpClientBuilder};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Lily HTTP Client - Basic Usage Examples\n");

    // Example 1: Simple GET request
    println!("📡 Example 1: Simple GET Request");
    simple_get_request().await?;

    // Example 2: POST with JSON
    println!("\n📡 Example 2: POST with JSON");
    post_json_request().await?;

    // Example 3: Custom client configuration
    println!("\n📡 Example 3: Custom Client Configuration");
    custom_client_config().await?;

    // Example 4: Headers and authentication
    println!("\n📡 Example 4: Headers and Authentication");
    headers_and_auth().await?;

    // Example 5: Error handling
    println!("\n📡 Example 5: Error Handling");
    error_handling_example().await?;

    println!("\n✅ All examples completed successfully!");
    Ok(())
}

/// Example 1: Simple GET request
async fn simple_get_request() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    // Build and execute a GET request
    let mut builder = client.get("https://httpbin.org/get")?;
    builder.header("User-Agent", "lily-http-client-example/1.0")?;
    let request = builder.build()?;

    println!("  Request URL configured: {}", request.url().has_host());
    println!("  Method: {}", request.method().as_str());
    println!("  Header count: {}", request.headers().len());

    // Note: For this example, we'll just show the request structure
    // In a real scenario, you would call: client.execute(request).await?
    println!("  ✅ Request built successfully");

    Ok(())
}

/// Example 2: POST with JSON data
async fn post_json_request() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    // Create JSON data
    let user_data = json!({
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30
    });

    // Build POST request with JSON body
    let mut builder = client.post("https://httpbin.org/post")?;
    builder.json(&user_data)?;
    builder.header("Authorization", "Bearer token123")?;
    let request = builder.build()?;

    println!("  Request URL configured: {}", request.url().has_host());
    println!(
        "  Content-Type present: {}",
        request.headers().get("content-type").is_some()
    );
    println!("  Has body: {}", request.body().is_some());

    println!("  ✅ JSON POST request built successfully");

    Ok(())
}

/// Example 3: Custom client configuration
async fn custom_client_config() -> Result<(), Box<dyn std::error::Error>> {
    // Build a custom HTTP client
    let client = HttpClientBuilder::new()
        .connect_timeout(Duration::from_secs(5))
        .request_timeout(Duration::from_secs(30))
        .user_agent("MyApp/2.0")?
        .max_redirects(3)
        .default_header("Accept", "application/json")?
        .try_build()?;

    println!("  Custom client configuration:");
    println!(
        "    Connect timeout: {:?}",
        client.config().connect_timeout()
    );
    println!(
        "    Request timeout: {:?}",
        client.config().request_timeout()
    );
    println!(
        "    User agent configured: {}",
        client
            .config()
            .default_headers()
            .get("user-agent")
            .is_some()
    );
    println!("    Max redirects: {}", client.config().max_redirects());

    // Use the configured client
    let builder = client.get("https://api.github.com/users/octocat")?;
    let _request = builder.build()?;

    println!("  ✅ Custom client configured successfully");

    Ok(())
}

/// Example 4: Headers and authentication
async fn headers_and_auth() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    // Example with Bearer token authentication
    let mut builder = client.get("https://api.example.com/protected")?;
    builder.bearer_auth("your-jwt-token-here")?;
    builder.header("Accept", "application/json")?;
    builder.header("X-API-Version", "v1")?;
    let request = builder.build()?;

    println!("  Bearer auth request:");
    println!(
        "    Authorization header present: {}",
        request.headers().get("authorization").is_some()
    );

    // Example with Basic authentication
    let mut builder2 = client.get("https://api.example.com/basic-auth")?;
    builder2.basic_auth("username", Some("password"))?;
    let request2 = builder2.build()?;

    println!("  Basic auth request:");
    println!(
        "    Authorization header present: {}",
        request2.headers().get("authorization").is_some()
    );

    println!("  ✅ Authentication examples completed");

    Ok(())
}

/// Example 5: Error handling scenarios
async fn error_handling_example() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    // Example 1: Invalid URL
    println!("  Testing invalid URL handling...");
    match client.get("not-a-valid-url") {
        Ok(_) => println!("    Unexpected success"),
        Err(e) => println!("    ✅ Caught expected error: {e}"),
    }

    // Example 2: Missing required fields
    println!("  Testing missing URL handling...");
    let builder = lily_http_client::request::RequestBuilder::new();
    match builder.build() {
        Ok(_) => println!("    Unexpected success"),
        Err(e) => println!("    ✅ Caught expected error: {e}"),
    }

    // Example 3: Invalid header values
    println!("  Testing invalid header handling...");
    let mut builder = client.get("https://example.com")?;
    match builder.header("Invalid\nHeader", "value") {
        Ok(_) => println!("    Unexpected success"),
        Err(e) => println!("    ✅ Caught expected error: {e}"),
    }

    println!("  ✅ Error handling examples completed");

    Ok(())
}
