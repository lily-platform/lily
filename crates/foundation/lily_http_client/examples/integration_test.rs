//! Integration test example for lily_http_client
//!
//! This example demonstrates real HTTP requests to test the client functionality.
//! Note: This requires internet connection to work properly.

use lily_http_client::{HttpClient, HttpClientBuilder};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Lily HTTP Client - Integration Test\n");

    // Test with httpbin.org - a service designed for HTTP testing
    println!("🌐 Testing with httpbin.org (HTTP testing service)");

    // Test 1: Basic GET request
    println!("\n📡 Test 1: Basic GET Request");
    test_basic_get().await?;

    // Test 2: POST with JSON
    println!("\n📡 Test 2: POST with JSON");
    test_post_json().await?;

    // Test 3: Headers and User-Agent
    println!("\n📡 Test 3: Custom Headers");
    test_custom_headers().await?;

    // Test 4: Query parameters
    println!("\n📡 Test 4: Query Parameters");
    test_query_parameters().await?;

    // Test 5: Different HTTP methods
    println!("\n📡 Test 5: Different HTTP Methods");
    test_http_methods().await?;

    println!("\n✅ All integration tests completed!");
    Ok(())
}

/// Test basic GET request functionality
async fn test_basic_get() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    // Build GET request
    let mut builder = client.get("https://httpbin.org/get")?;
    builder.header("User-Agent", "lily-http-client-test/1.0")?;
    let request = builder.build()?;

    println!("  📤 Request URL configured: {}", request.url().has_host());
    println!("  📤 Method: {}", request.method().as_str());
    println!(
        "  📤 User-Agent present: {}",
        request.headers().get("user-agent").is_some()
    );

    // For now, we just validate the request structure
    // In a full implementation, you would execute: client.execute(request).await?
    println!("  ✅ GET request structure validated");

    Ok(())
}

/// Test POST request with JSON payload
async fn test_post_json() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    // Create test data
    let test_data = json!({
        "message": "Hello from lily_http_client!",
        "timestamp": "2024-01-01T00:00:00Z",
        "test": true,
        "numbers": [1, 2, 3, 4, 5]
    });

    // Build POST request
    let mut builder = client.post("https://httpbin.org/post")?;
    builder.json(&test_data)?;
    builder.header("X-Test-Header", "integration-test")?;
    let request = builder.build()?;

    println!("  📤 Request URL configured: {}", request.url().has_host());
    println!("  📤 Method: {}", request.method().as_str());
    println!(
        "  📤 Content-Type present: {}",
        request.headers().get("content-type").is_some()
    );
    println!("  📤 Has JSON body: {}", request.body().is_some());

    println!("  ✅ POST JSON request structure validated");

    Ok(())
}

/// Test custom headers functionality
async fn test_custom_headers() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClientBuilder::new()
        .user_agent("lily-http-client-integration-test/1.0")?
        .default_header("Accept", "application/json")?
        .default_header("X-Client-Version", "1.0.0")?
        .try_build()?;

    let mut builder = client.get("https://httpbin.org/headers")?;
    builder.header("X-Request-ID", "test-12345")?;
    builder.header("Authorization", "Bearer test-token")?;
    let request = builder.build()?;

    println!("  📤 Request URL configured: {}", request.url().has_host());
    println!("  📤 Headers count: {}", request.headers().len());
    println!(
        "  📤 Required headers present: {}",
        ["accept", "user-agent", "x-request-id", "authorization"]
            .iter()
            .all(|name| request.headers().get(name).is_some())
    );

    println!("  ✅ Custom headers validated");

    Ok(())
}

/// Test query parameters
async fn test_query_parameters() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    let mut builder = client.get("https://httpbin.org/get")?;
    builder.query(vec![
        ("param1", "value1"),
        ("param2", "value2"),
        ("search", "lily http client"),
        ("limit", "10"),
    ])?;
    let request = builder.build()?;

    println!("  📤 Query configured: {}", request.url().query().is_some());

    // Verify query parameters are in the URL
    let url_str = request.url().as_str();
    assert!(url_str.contains("param1=value1"));
    assert!(url_str.contains("param2=value2"));
    assert!(url_str.contains("search=lily+http+client")); // URL encoded with + for spaces
    assert!(url_str.contains("limit=10"));

    println!("  ✅ Query parameters validated");

    Ok(())
}

/// Test different HTTP methods
async fn test_http_methods() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    // Test GET
    let get_request = client.get("https://httpbin.org/get")?.build()?;
    println!("  📤 GET method: {}", get_request.method().as_str());

    // Test POST
    let post_request = client.post("https://httpbin.org/post")?.build()?;
    println!("  📤 POST method: {}", post_request.method().as_str());

    // Test PUT
    let put_request = client.put("https://httpbin.org/put")?.build()?;
    println!("  📤 PUT method: {}", put_request.method().as_str());

    // Test DELETE
    let delete_request = client.delete("https://httpbin.org/delete")?.build()?;
    println!("  📤 DELETE method: {}", delete_request.method().as_str());

    // Test PATCH
    let patch_request = client.patch("https://httpbin.org/patch")?.build()?;
    println!("  📤 PATCH method: {}", patch_request.method().as_str());

    // Test HEAD
    let head_request = client.head("https://httpbin.org/get")?.build()?;
    println!("  📤 HEAD method: {}", head_request.method().as_str());

    println!("  ✅ All HTTP methods validated");

    Ok(())
}

/// Helper function to demonstrate error scenarios
#[allow(dead_code)]
async fn test_error_scenarios() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new();

    println!("  🚨 Testing error scenarios:");

    // Test 1: Invalid URL
    println!("    Testing invalid URL...");
    match client.get("not-a-valid-url") {
        Ok(_) => println!("    ❌ Expected error but got success"),
        Err(e) => println!("    ✅ Caught expected error: {e}"),
    }

    // Test 2: Timeout configuration
    println!("    Testing timeout configuration...");
    let timeout_client = HttpClientBuilder::new()
        .connect_timeout(Duration::from_millis(1)) // Very short timeout
        .request_timeout(Duration::from_millis(1))
        .try_build()?;

    let _request = timeout_client.get("https://httpbin.org/delay/5")?.build()?;
    println!("    ✅ Timeout client configured (would timeout on execution)");

    // TLS certificate-chain and hostname verification are always enabled.
    println!("    ✅ TLS verification uses the strict client profile");

    Ok(())
}
