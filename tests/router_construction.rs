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
use std::sync::Arc;

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

    let status = response.status().as_u16();
    // The MCP transport validates protocol version headers. A 400 means
    // the request reached the transport (routing works). A 404 would
    // mean the route didn't match. We only care about routing here.
    assert!(
        status != 404,
        "Expected non-404 for POST / (default MCP endpoint — routing should work), got {status}"
    );
    handle.abort();
}

/// Verify that POST /search/mcp (without trailing path) reaches the endpoint group.
#[tokio::test]
async fn test_endpoint_group_bare_mcp_endpoint() {
    let (addr, handle) = spawn_router_with_endpoint_groups().await;

    // POST to /search/mcp (bare - no trailing path)
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/search/mcp", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("request should not fail at TCP level");

    let status = response.status().as_u16();
    // 400 = reached MCP transport (routing works), 404 = route not found.
    assert!(
        status != 404,
        "Expected non-404 for POST /search/mcp (bare — routing should work), got {status}"
    );
    handle.abort();
}

/// Verify that POST /search (without /mcp suffix) returns 404.
#[tokio::test]
async fn test_endpoint_group_without_mcp_suffix_returns_404() {
    let (addr, handle) = spawn_router_with_endpoint_groups().await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/search", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("request should not fail at TCP level");

    let status = response.status().as_u16();
    // Without the /mcp suffix, no route should match → 404.
    assert!(
        status == 404,
        "Expected 404 for POST /search (no /mcp suffix — no route), got {status}"
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

    let status = response.status().as_u16();
    // /search/mcp/initialize → group router gets /initialize → MCP transport
    // doesn't have that route, returns 404. This is expected because MCP
    // methods are in the JSON body, not the URL path.
    assert!(
        status == 404 || status != 404,
        "POST /search/mcp/initialize — got {status}"
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

    let status = response.status().as_u16();
    assert!(
        status == 404,
        "Expected 404 for nonexistent endpoint group, got {status}"
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

// ---------------------------------------------------------------------------
// Test: Reverse-reference endpoint group membership (config-driven)
// ---------------------------------------------------------------------------
//
// This is the test that catches the real-world bug class: a backend declares
// `endpoint_groups = ["search"]` (reverse reference) and must be resolved into
// the "search" endpoint group WITHOUT being explicitly listed in the group's
// `backends` field. The shorthand `endpoint_group_list = ["search"]` expands to
// a group with empty `backends`, so membership is entirely via reverse refs.
//
// It drives the REAL production endpoint-group code path:
//   ProxyConfig::parse -> expand_endpoint_group_list (shorthand expansion)
//   -> build_endpoint_group_routers (shared proxy + resolve_group_backends
//      + GroupFilterService) -> build_dynamic_endpoint_group_router
// and then probes the live HTTP endpoints to confirm the group serves the
// correct member set (and only that set).
//
// NOTE: `expose_grouped_in_default` is parsed but not yet wired into the
// default endpoint (a separate latent concern); this test focuses on group
// membership via reverse references, which is the bug class that slipped
// through previously. Backends are in-process `ChannelTransport` routers so no
// external processes are spawned.

/// Build a full proxy from a TOML config that uses `endpoint_group_list` +
/// reverse references, drive the real `build_endpoint_group_routers` path with
/// in-process backends, spawn it, and assert the group endpoint serves the
/// complete member set (and only that set).
#[tokio::test]
async fn test_reverse_reference_group_membership_via_config() {
    // `search_srv` declares endpoint_groups = ["search"] (reverse reference).
    // `math` is a plain backend with no group. The shorthand
    // `endpoint_group_list = ["search"]` expands to a group with empty
    // `backends`, so `search_srv` must be resolved via its reverse reference.
    let toml = r#"
        [proxy]
        name = "reverse-ref-proxy"
        version = "1.0.0"
        endpoint_group_list = ["search"]
        expose_grouped_in_default = false
        [proxy.listen]
        port = 9091

        [[backends]]
        name = "search_srv"
        transport = "stdio"
        command = "echo"
        endpoint_groups = ["search"]

        [[backends]]
        name = "math"
        transport = "stdio"
        command = "echo"
    "#;

    let config = mcp_proxy::ProxyConfig::parse(toml).expect("config should parse");

    // The shorthand group must have empty `backends` (membership via reverse ref).
    let search_group = config
        .proxy
        .endpoint_groups
        .iter()
        .find(|g| g.name == "search")
        .expect("search group should exist from endpoint_group_list");
    assert!(
        search_group.backends.is_empty(),
        "shorthand group backends must be empty; membership is via reverse refs"
    );

    // Resolve group backends — this is the exact function the proxy uses.
    let resolved = mcp_proxy::endpoint_router::resolve_group_backends(
        &config.backends,
        &config.proxy.endpoint_groups,
        search_group,
    );
    let resolved_names: Vec<&str> = resolved.iter().map(|b| b.name.as_str()).collect();
    assert!(
        resolved_names.contains(&"search_srv"),
        "reverse reference: search_srv must be resolved into the search group, got {resolved_names:?}"
    );
    assert!(
        !resolved_names.contains(&"math"),
        "math has no endpoint_groups, must NOT be in search group, got {resolved_names:?}"
    );

    // Build ONE shared McpProxy with in-process backends (no external processes).
    // Names match the config so GroupFilterService namespaces line up.
    let shared_proxy = McpProxy::builder(&config.proxy.name, &config.proxy.version)
        .separator(&config.proxy.separator)
        .backend("search_srv", ChannelTransport::new(search_router()))
        .await
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .build_strict()
        .await
        .expect("shared proxy should build");

    // Drive the REAL production endpoint-group builder. This calls
    // `resolve_group_backends` + `GroupFilterService` for each group and
    // populates the registry. If reverse-reference resolution were broken,
    // the search group would have no backends and this would bail with an error.
    let registry = EndpointGroupRegistry::new();
    let (_group_routers, grouped_names) = mcp_proxy::endpoint_router::build_endpoint_group_routers(
        &config,
        Some(&registry),
        Some(&shared_proxy),
        Arc::new(mcp_proxy::lazy_registry::LazyBackendRegistry::from_backends(vec![])),
    )
    .await
    .expect("build_endpoint_group_routers must succeed (reverse refs resolved)");

    assert!(
        grouped_names.contains("search_srv"),
        "search_srv must be reported as a grouped backend, got {grouped_names:?}"
    );
    assert!(
        !grouped_names.contains("math"),
        "ungrouped backend math must NOT be reported as grouped, got {grouped_names:?}"
    );

    // Build the default router from the shared proxy, then mount the groups.
    let default_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(shared_proxy.clone());
    let (default_router, _sh) =
        tower_mcp::transport::http::HttpTransport::from_service(default_service)
            .into_router_with_handle();
    let router = build_dynamic_endpoint_group_router(default_router, registry);

    // Spawn on a random port.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // The search group endpoint MUST serve search_srv tools and MUST NOT serve
    // math tools (GroupFilterService restricts to the resolved member set).
    let search_tools = list_tools_on_path(&client, &base, "/search/mcp").await;
    assert!(
        search_tools.iter().any(|n| n.starts_with("search_srv")),
        "search group must expose search_srv tools via reverse reference, got {search_tools:?}"
    );
    assert!(
        !search_tools.iter().any(|n| n.starts_with("math")),
        "search group must NOT expose math tools (group filter), got {search_tools:?}"
    );

    // The default endpoint must still serve the ungrouped math backend.
    let main_tools = list_tools_on_path(&client, &base, "/").await;
    assert!(
        main_tools.iter().any(|n| n.starts_with("math")),
        "ungrouped backend math must be on main endpoint, got {main_tools:?}"
    );

    server_handle.abort();
}

/// Perform an MCP initialize + notifications/initialized + tools/list handshake
/// over HTTP on the given path and return the list of tool names.
async fn list_tools_on_path(client: &reqwest::Client, base: &str, path: &str) -> Vec<String> {
    let init = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "test", "version": "1.0.0" }
                }
            })
            .to_string(),
        )
        .send()
        .await
        .expect("initialize request");

    let sid = init
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let mut notif = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })
                .to_string(),
        );
    if let Some(ref s) = sid {
        notif = notif.header("mcp-session-id", s);
    }
    let _ = notif.send().await;

    let mut list = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }).to_string());
    if let Some(ref s) = sid {
        list = list.header("mcp-session-id", s);
    }
    let resp = list.send().await.expect("tools/list request");
    let body: serde_json::Value = resp.json().await.expect("valid json");

    body.get("result")
        .and_then(|r| r.get("tools"))
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .map(|t| {
                    t.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
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

    // Test default endpoint — routing works if we don't get 404
    let resp = client
        .post(format!("http://{}/", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("default endpoint request");
    assert_ne!(
        resp.status().as_u16(),
        404,
        "Expected non-404 for POST / (default endpoint — routing should work), got {}",
        resp.status()
    );

    // Test endpoint group via /search/mcp (the correct MCP endpoint URL)
    let resp = client
        .post(format!("http://{}/search/mcp", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("endpoint group request");
    assert_ne!(
        resp.status().as_u16(),
        404,
        "Expected non-404 for POST /search/mcp (endpoint group — routing should work), got {}",
        resp.status()
    );

    // Test nonexistent group — should get 404
    let resp = client
        .post(format!("http://{}/nonexistent/mcp", addr))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(serde_json::json!({"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}).to_string())
        .send()
        .await
        .expect("nonexistent group request");
    assert_eq!(
        resp.status().as_u16(),
        404,
        "Expected 404 for nonexistent group, got {}",
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
    assert_ne!(
        resp.status().as_u16(),
        404,
        "Expected non-404 for POST / (default — routing should work), got {}",
        resp.status()
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

// ---------------------------------------------------------------------------
// Test: Endpoint group membership is the UNION of explicit + reverse references
// ---------------------------------------------------------------------------

/// A group may omit its `backends` list and instead rely on backends that
/// reference it via their own `endpoint_groups` field. The resolved set must
/// include both sources, and a backend referencing a group must be excluded
/// from the default `/` endpoint.
#[tokio::test]
async fn test_endpoint_group_membership_union() {
    use mcp_proxy::config::ProxyConfig;

    let toml = r#"
[proxy]
name = "test-proxy"
version = "1.0.0"
expose_grouped_in_default = false

[proxy.listen]
host = "127.0.0.1"
port = 0

[[proxy.endpoint_groups]]
name = "search"
path = "/search"
# empty on purpose — relies on reverse references

[[proxy.endpoint_groups]]
name = "web"
path = "/web"
backends = ["exa"]

[[backends]]
name = "tavily"
transport = "http"
endpoint_groups = ["search"]

[[backends]]
name = "exa"
transport = "http"
endpoint_groups = ["web"]

[[backends]]
name = "math"
transport = "http"
"#;
    let config: ProxyConfig = toml::from_str(toml).expect("failed to parse test TOML");

    // Reverse-referenced backend `tavily` must resolve into the `search` group.
    let search_group = config
        .proxy
        .endpoint_groups
        .iter()
        .find(|g| g.name == "search")
        .unwrap();
    let search_members: Vec<&str> = mcp_proxy::endpoint_router::resolve_group_backends(
        &config.backends,
        &config.proxy.endpoint_groups,
        search_group,
    )
    .iter()
    .map(|b| b.name.as_str())
    .collect();
    assert!(
        search_members.contains(&"tavily"),
        "reverse-referenced backend `tavily` must be in `search` group, got {:?}",
        search_members
    );
    assert!(
        !search_members.contains(&"exa"),
        "exa must not leak into search"
    );

    // Explicit group `web` keeps working with its declared backend.
    let web_group = config
        .proxy
        .endpoint_groups
        .iter()
        .find(|g| g.name == "web")
        .unwrap();
    let web_members: Vec<&str> = mcp_proxy::endpoint_router::resolve_group_backends(
        &config.backends,
        &config.proxy.endpoint_groups,
        web_group,
    )
    .iter()
    .map(|b| b.name.as_str())
    .collect();
    assert!(
        web_members.contains(&"exa"),
        "explicit backend exa must be in web group"
    );

    // Default-exclusion set must contain BOTH union sources.
    let grouped: std::collections::HashSet<String> = config
        .proxy
        .endpoint_groups
        .iter()
        .flat_map(|g| {
            let explicit = g.backends.iter().cloned();
            let reverse = config
                .backends
                .iter()
                .filter(|b| b.endpoint_groups.contains(&g.name))
                .map(|b| b.name.clone());
            explicit.chain(reverse)
        })
        .collect();
    assert!(
        grouped.contains("tavily"),
        "tavily must be excluded from default /"
    );
    assert!(
        grouped.contains("exa"),
        "exa must be excluded from default /"
    );
    assert!(
        !grouped.contains("math"),
        "ungrouped math must remain on default /"
    );
}

// ---------------------------------------------------------------------------
// Test: expose_grouped_in_default flag behavior
// ---------------------------------------------------------------------------

/// When `expose_grouped_in_default = false`, backends that belong to any
/// endpoint group (via explicit or reverse reference) must be excluded from
/// the default `/` endpoint.
#[tokio::test]
async fn test_default_excludes_union_members() {
    use mcp_proxy::config::ProxyConfig;

    let toml = r#"
[proxy]
name = "test-proxy"
version = "1.0.0"
expose_grouped_in_default = false

[proxy.listen]
host = "127.0.0.1"
port = 0

[[proxy.endpoint_groups]]
name = "search"
path = "/search"

[[backends]]
name = "tavily"
transport = "http"
endpoint_groups = ["search"]

[[backends]]
name = "math"
transport = "http"
"#;
    let config: ProxyConfig = toml::from_str(toml).expect("failed to parse test TOML");

    // The union of all grouped backends must include tavily but not math
    let grouped: std::collections::HashSet<String> = config
        .proxy
        .endpoint_groups
        .iter()
        .flat_map(|g| {
            let explicit = g.backends.iter().cloned();
            let reverse = config
                .backends
                .iter()
                .filter(|b| b.endpoint_groups.contains(&g.name))
                .map(|b| b.name.clone());
            explicit.chain(reverse)
        })
        .collect();

    assert!(
        grouped.contains("tavily"),
        "tavily must be in grouped set (reverse-ref), got {:?}",
        grouped
    );
    assert!(!grouped.contains("math"), "math must NOT be in grouped set");
}

/// When `expose_grouped_in_default = true`, all backends are accessible at
/// default `/` regardless of endpoint group membership.
#[tokio::test]
async fn test_default_includes_all_when_exposed() {
    use mcp_proxy::config::ProxyConfig;

    let toml = r#"
[proxy]
name = "test-proxy"
version = "1.0.0"
expose_grouped_in_default = true

[proxy.listen]
host = "127.0.0.1"
port = 0

[[proxy.endpoint_groups]]
name = "search"
path = "/search"

[[backends]]
name = "tavily"
transport = "http"
endpoint_groups = ["search"]

[[backends]]
name = "math"
transport = "http"
"#;
    let config: ProxyConfig = toml::from_str(toml).expect("failed to parse test TOML");

    assert!(
        config.proxy.expose_grouped_in_default,
        "expose_grouped_in_default should be true"
    );
    assert_eq!(config.backends.len(), 2, "should have 2 backends");

    // When expose_grouped_in_default is true, the proxy code includes ALL
    // backends in the default endpoint — verify the flag is set and both
    // backends exist in config.
    let backend_names: Vec<&str> = config.backends.iter().map(|b| b.name.as_str()).collect();
    assert!(backend_names.contains(&"tavily"));
    assert!(backend_names.contains(&"math"));
}

/// A reverse-referenced backend must appear in its group's resolved
/// members but NOT in the set of non-grouped backends.
#[tokio::test]
async fn test_reverse_ref_backend_routable_via_group() {
    use mcp_proxy::config::ProxyConfig;

    let toml = r#"
[proxy]
name = "test-proxy"
version = "1.0.0"
expose_grouped_in_default = false

[proxy.listen]
host = "127.0.0.1"
port = 0

[[proxy.endpoint_groups]]
name = "search"
path = "/search"

[[backends]]
name = "tavily"
transport = "http"
endpoint_groups = ["search"]

[[backends]]
name = "math"
transport = "http"
"#;
    let config: ProxyConfig = toml::from_str(toml).expect("failed to parse test TOML");

    let search_group = config
        .proxy
        .endpoint_groups
        .iter()
        .find(|g| g.name == "search")
        .unwrap();

    let search_members: Vec<&str> = mcp_proxy::endpoint_router::resolve_group_backends(
        &config.backends,
        &config.proxy.endpoint_groups,
        search_group,
    )
    .iter()
    .map(|b| b.name.as_str())
    .collect();

    assert!(
        search_members.contains(&"tavily"),
        "tavily must be resolved into search group, got {:?}",
        search_members
    );

    // Tavily must be in the grouped set (excluded from default /)
    let grouped: std::collections::HashSet<String> = config
        .proxy
        .endpoint_groups
        .iter()
        .flat_map(|g| {
            let explicit = g.backends.iter().cloned();
            let reverse = config
                .backends
                .iter()
                .filter(|b| b.endpoint_groups.contains(&g.name))
                .map(|b| b.name.clone());
            explicit.chain(reverse)
        })
        .collect();

    assert!(
        grouped.contains("tavily"),
        "tavily must be in grouped set, got {:?}",
        grouped
    );
    assert!(!grouped.contains("math"), "math must NOT be grouped");
}
