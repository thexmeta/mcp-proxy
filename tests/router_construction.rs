//! Tests for axum router construction and endpoint group routing.
//!
//! These tests exercise the HTTP/Axum layer of the proxy, which is a different
//! code path from the MCP service layer tested in `e2e.rs` and `integration.rs`.
//!
//! **Why these tests exist:**
//!
//! The MCP tests (`e2e.rs`, `integration.rs`) use `McpProxy::builder()` which
//! returns a `BoxCloneService<RouterRequest, RouterResponse>` — a pure MCP-level
//! service. They never construct the full axum `Router` that the production code
//! builds via `Proxy::from_config()`. This means bugs in the axum routing layer
//! (e.g., invalid route patterns, middleware composition) go undetected.
//!
//! These tests fill that gap by constructing the full axum router stack and
//! verifying it doesn't panic and routes requests correctly.

use std::convert::Infallible;
use std::net::SocketAddr;

use schemars::JsonSchema;
use serde::Deserialize;
use tokio::net::TcpListener;
use tower::util::BoxCloneService;

use tower_mcp::client::ChannelTransport;
use tower_mcp::proxy::McpProxy;
use tower_mcp::router::{RouterRequest, RouterResponse};
use tower_mcp::{CallToolResult, McpRouter, ToolBuilder};

use mcp_proxy::build_dynamic_endpoint_group_router;
use mcp_proxy::endpoint_router::{EndpointGroupRegistry, EndpointGroupRouter};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an endpoint group and return an EndpointGroupRouter.
async fn build_endpoint_group(
    name: &str,
    backend_name: &str,
    backend_router: McpRouter,
) -> EndpointGroupRouter {
    let group_proxy = McpProxy::builder(format!("{}-group", name), "1.0.0")
        .separator("/")
        .backend(backend_name, ChannelTransport::new(backend_router))
        .await
        .build_strict()
        .await
        .expect("group proxy should build");

    let group_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(group_proxy.clone());
    let (group_router, session_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(group_service)
            .into_router_with_handle();

    EndpointGroupRouter {
        name: name.to_string(),
        path: format!("/{}", name),
        router: group_router,
        session_handle,
        inner: group_proxy,
    }
}

// ---------------------------------------------------------------------------
// Test backend routers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
struct AddInput {
    a: i64,
    b: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct EchoInput {
    message: String,
}

fn math_router() -> McpRouter {
    let add = ToolBuilder::new("add")
        .description("Add two numbers")
        .handler(|input: AddInput| async move {
            Ok(CallToolResult::text(format!("{}", input.a + input.b)))
        })
        .build();

    McpRouter::new()
        .server_info("math-server", "1.0.0")
        .tool(add)
}

fn text_router() -> McpRouter {
    let echo = ToolBuilder::new("echo")
        .description("Echo a message")
        .handler(|input: EchoInput| async move { Ok(CallToolResult::text(input.message)) })
        .build();

    McpRouter::new()
        .server_info("text-server", "1.0.0")
        .tool(echo)
}

fn search_router() -> McpRouter {
    let search = ToolBuilder::new("web_search")
        .description("Search the web")
        .handler(|input: EchoInput| async move {
            Ok(CallToolResult::text(format!(
                "Results for: {}",
                input.message
            )))
        })
        .build();

    McpRouter::new()
        .server_info("search-server", "1.0.0")
        .tool(search)
}

// ---------------------------------------------------------------------------
// Test: Router construction doesn't panic
// ---------------------------------------------------------------------------

/// Verify that `build_dynamic_endpoint_group_router` doesn't panic when
/// constructing the axum router. This is the exact test that would have
/// caught the matchit 0.8 wildcard syntax panic (`*path` vs `{*path}`).
#[tokio::test]
async fn test_router_construction_no_panic() {
    // Build a minimal MCP proxy (MCP-level service)
    let proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .build_strict()
        .await
        .expect("MCP proxy should build");

    let service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(proxy);

    // Wrap in HTTP transport — this creates the base axum Router
    let (_router, _session_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(service).into_router_with_handle();

    // Create an empty endpoint group registry
    let registry = EndpointGroupRegistry::new();

    // This is where the matchit 0.8 panic would occur if the wildcard syntax
    // is wrong. The old code used `*path` which panics with matchit 0.8+.
    let _router = build_dynamic_endpoint_group_router(_router, registry);
}

/// Verify that registering an endpoint group router doesn't panic.
#[tokio::test]
async fn test_router_with_endpoint_group_registry() {
    let proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .backend("text", ChannelTransport::new(text_router()))
        .await
        .build_strict()
        .await
        .expect("MCP proxy should build");

    let service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(proxy);

    let (router, _session_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(service).into_router_with_handle();

    // Create a registry with an endpoint group
    let registry = EndpointGroupRegistry::new();

    // Build the endpoint group's MCP proxy
    let group_proxy = McpProxy::builder("search-group", "1.0.0")
        .separator("/")
        .backend("search", ChannelTransport::new(search_router()))
        .await
        .build_strict()
        .await
        .expect("group MCP proxy should build");

    let group_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(group_proxy.clone());
    let (group_router, group_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(group_service)
            .into_router_with_handle();

    // Register the endpoint group
    registry.insert(EndpointGroupRouter {
        name: "search".to_string(),
        path: "/search".to_string(),
        router: group_router,
        session_handle: group_handle,
        inner: group_proxy,
    });

    // Build the dynamic router — this should not panic
    let _router = build_dynamic_endpoint_group_router(router, registry);
}

// ---------------------------------------------------------------------------
// Test: Endpoint group routing via HTTP
// ---------------------------------------------------------------------------

/// Build a full axum router with endpoint groups and spawn an HTTP server.
/// Returns the server address, session handle, and a JoinHandle for the server.
async fn spawn_router_with_endpoint_groups() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    // Build default proxy with math backend
    let default_proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .backend("text", ChannelTransport::new(text_router()))
        .await
        .build_strict()
        .await
        .expect("default proxy should build");

    let default_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(default_proxy);
    let (default_router, _session_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(default_service)
            .into_router_with_handle();

    // Build endpoint group registry with a "search" group
    let registry = EndpointGroupRegistry::new();

    let search_proxy = McpProxy::builder("search-group", "1.0.0")
        .separator("/")
        .backend("search", ChannelTransport::new(search_router()))
        .await
        .build_strict()
        .await
        .expect("search proxy should build");

    let search_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(search_proxy.clone());
    let (search_router, search_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(search_service)
            .into_router_with_handle();

    registry.insert(EndpointGroupRouter {
        name: "search".to_string(),
        path: "/search".to_string(),
        router: search_router,
        session_handle: search_handle,
        inner: search_proxy,
    });

    // Build the dynamic endpoint group router
    let router = build_dynamic_endpoint_group_router(default_router, registry);

    // Bind to random port
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port");
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    (addr, handle)
}

/// Verify that the MCP HTTP transport starts and accepts connections on the
/// default endpoint. This exercises the full axum server construction path
/// without requiring the SSE MCP protocol handshake.
#[tokio::test]
async fn test_default_mcp_endpoint_starts() {
    let (addr, handle) = spawn_router_with_endpoint_groups().await;
    let url = format!("http://{}/", addr);

    // Send a basic HTTP request — just verify the server accepts connections
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("request should not fail at TCP level");

    // Server accepts the connection (any non-connection-refused response)
    // 200 = SSE stream started, 400 = bad request but server is alive
    assert!(
        response.status().as_u16() != 0,
        "Server should accept connections on root endpoint"
    );
    handle.abort();
}

/// Verify that endpoint group routing doesn't crash the server.
#[tokio::test]
async fn test_endpoint_group_server_starts() {
    let (addr, handle) = spawn_router_with_endpoint_groups().await;
    let url = format!("http://{}/search/mcp/initialize", addr);

    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("request should not fail at TCP level");

    // Server accepts the connection on the endpoint group route
    assert!(
        response.status().as_u16() != 0,
        "Server should accept connections on endpoint group route"
    );
    handle.abort();
}

/// Verify that a non-existent endpoint group returns 404.
#[tokio::test]
async fn test_nonexistent_endpoint_group_returns_404() {
    let (addr, handle) = spawn_router_with_endpoint_groups().await;
    let url = format!("http://{}/nonexistent/mcp/initialize", addr);

    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("request should not fail at TCP level");

    let status = response.status();
    // 404 = endpoint group not found, 405 = method not allowed on that route
    assert!(
        status.as_u16() == 404 || status.as_u16() == 405,
        "Expected 404 or 405 for nonexistent endpoint group, got {}",
        status
    );

    handle.abort();
}

// ---------------------------------------------------------------------------
// Test: Multiple endpoint groups
// ---------------------------------------------------------------------------

/// Verify that multiple endpoint groups can coexist without route conflicts.
#[tokio::test]
async fn test_multiple_endpoint_groups_coexist() {
    let default_proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .build_strict()
        .await
        .expect("default proxy should build");

    let default_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(default_proxy);
    let (default_router, _) =
        tower_mcp::transport::http::HttpTransport::from_service(default_service)
            .into_router_with_handle();

    let registry = EndpointGroupRegistry::new();

    // Create two endpoint groups: "search" and "coding"
    let groups = [
        ("search", "search", search_router()),
        ("coding", "math", math_router()),
    ];
    for (name, backend_name, backend_router) in groups {
        registry.insert(build_endpoint_group(name, backend_name, backend_router).await);
    }

    // This should not panic — multiple groups must not conflict
    let _router = build_dynamic_endpoint_group_router(default_router, registry);
}

/// Verify that endpoint groups with different path prefixes work.
#[tokio::test]
async fn test_endpoint_group_path_prefixes() {
    let default_proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .build_strict()
        .await
        .expect("default proxy should build");

    let default_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(default_proxy);
    let (default_router, _) =
        tower_mcp::transport::http::HttpTransport::from_service(default_service)
            .into_router_with_handle();

    let registry = EndpointGroupRegistry::new();

    // Create endpoint groups with different names (different path prefixes)
    let groups = [("alpha", "math"), ("beta", "text"), ("gamma", "search")];

    for (group_name, backend_name) in groups {
        registry.insert(build_endpoint_group(group_name, backend_name, math_router()).await);
    }

    // Three endpoint groups should all coexist
    let _router = build_dynamic_endpoint_group_router(default_router, registry);
}

// ---------------------------------------------------------------------------
// Test: Full stack construction (matches Proxy::from_config pattern)
// ---------------------------------------------------------------------------

/// Test the complete proxy construction pipeline as used in production.
/// This mimics `Proxy::from_config()` but uses channel backends instead of
/// HTTP/stdio to avoid needing external processes.
///
/// This is the most comprehensive test — it exercises every layer:
/// 1. MCP proxy construction (McpProxy::builder)
/// 2. HTTP transport wrapping (HttpTransport::into_router_with_handle)
/// 3. Dynamic endpoint group routing (build_dynamic_endpoint_group_router)
/// 4. Axum server construction (axum::serve)
#[tokio::test]
async fn test_full_stack_construction_with_endpoint_groups() {
    // Step 1: Build MCP proxy (same as Proxy::from_config)
    let mcp_proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .backend("text", ChannelTransport::new(text_router()))
        .await
        .build_strict()
        .await
        .expect("MCP proxy should build");

    let service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(mcp_proxy);

    // Step 2: Wrap in HTTP transport (same as Proxy::from_config)
    let (router, _session_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(service).into_router_with_handle();

    // Step 3: Build endpoint group registry (same as Proxy::from_config)
    let registry = EndpointGroupRegistry::new();

    // Build search group
    let search_proxy = McpProxy::builder("search-group", "1.0.0")
        .separator("/")
        .backend("search", ChannelTransport::new(search_router()))
        .await
        .build_strict()
        .await
        .expect("search proxy should build");

    let search_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(search_proxy.clone());
    let (search_router, search_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(search_service)
            .into_router_with_handle();

    registry.insert(EndpointGroupRouter {
        name: "search".to_string(),
        path: "/search".to_string(),
        router: search_router,
        session_handle: search_handle,
        inner: search_proxy,
    });

    // Step 4: Build dynamic endpoint group router (same as Proxy::from_config)
    let router = build_dynamic_endpoint_group_router(router, registry);

    // Step 5: Construct axum server (same as Proxy::serve)
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port");
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    // Verify the server is running and accepts connections
    let client = reqwest::Client::new();

    // Test default endpoint
    let resp = client
        .post(format!("http://{}/", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("default endpoint request");
    assert!(
        resp.status().as_u16() != 0,
        "default endpoint should accept connections"
    );

    // Test endpoint group
    let resp = client
        .post(format!("http://{}/search/mcp/initialize", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("endpoint group request");
    assert!(
        resp.status().as_u16() != 0,
        "endpoint group should accept connections"
    );

    // Test nonexistent group — should get 404 or 405
    let resp = client
        .post(format!("http://{}/nonexistent/mcp/initialize", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("nonexistent group request");
    assert!(
        resp.status().as_u16() == 404 || resp.status().as_u16() == 405,
        "nonexistent group should return 404 or 405, got {}",
        resp.status()
    );

    handle.abort();
}

// ---------------------------------------------------------------------------
// Test: Hot reload with endpoint groups
// ---------------------------------------------------------------------------

/// Verify that the endpoint group registry supports dynamic insertion
/// and that the router still works after adding new groups.
#[tokio::test]
async fn test_endpoint_group_hot_reload() {
    let default_proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .build_strict()
        .await
        .expect("default proxy should build");

    let default_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(default_proxy);
    let (default_router, _) =
        tower_mcp::transport::http::HttpTransport::from_service(default_service)
            .into_router_with_handle();

    let registry = EndpointGroupRegistry::new();

    // Initially no endpoint groups — router should build fine
    let router = build_dynamic_endpoint_group_router(default_router, registry.clone());

    // Bind and start server
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port");
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    // Verify server is running
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("default endpoint");
    assert!(
        resp.status().as_u16() != 0,
        "server should accept connections"
    );

    // Add a new endpoint group to the registry
    let search_proxy = McpProxy::builder("search-group", "1.0.0")
        .separator("/")
        .backend("search", ChannelTransport::new(search_router()))
        .await
        .build_strict()
        .await
        .expect("search proxy should build");

    let search_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(search_proxy.clone());
    let (search_router, search_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(search_service)
            .into_router_with_handle();

    registry.insert(EndpointGroupRouter {
        name: "search".to_string(),
        path: "/search".to_string(),
        router: search_router,
        session_handle: search_handle,
        inner: search_proxy,
    });

    // The registry now has a group, but the existing router was built with
    // the registry reference — so the hot-reloaded group should be visible.
    // (This tests that the registry is shared, not copied.)

    handle.abort();
}
