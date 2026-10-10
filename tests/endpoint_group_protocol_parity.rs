//! Regression tests: endpoint-group routes (`/{group}/mcp`) must serve MCP
//! 2026-07-28 at parity with the root `/` route — specifically `server/discover`
//! (SEP-2575), `subscriptions/listen`, and must honor `[proxy.protocol_support]`
//! versions instead of the tower_mcp build-time defaults.
//!
//! These tests drive the REAL production endpoint-group builder
//! (`build_endpoint_group_routers`) with in-process `ChannelTransport` backends
//! and probe the live HTTP endpoints, mirroring the user-reported symptom
//! ("/os/mcp can only serve the older version").
//!
//! Root cause these tests lock in: endpoint-group routes previously omitted the
//! three innermost 2026-07-28 layers (SubscriptionsListen, Discover,
//! MetaValidation) that root applies, AND the group `HttpTransport` never called
//! `.protocol_support(...)`, so `server/discover` returned -32601 on groups.

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
use mcp_proxy::endpoint_router::EndpointGroupRegistry;
use mcp_proxy::mcp_compat::inject_mcp_compat_headers;

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
// Harness: build a config-driven proxy + endpoint groups, spawn, probe
// ---------------------------------------------------------------------------

/// Build a config with a `search` endpoint group (via `endpoint_group_list`
/// shorthand + reverse reference) and the given protocol-support versions.
fn config_with_versions(versions: &[&str]) -> String {
    let versions_toml = if versions.is_empty() {
        String::new()
    } else {
        let list = versions
            .iter()
            .map(|v| format!("\"{v}\""))
            .collect::<Vec<_>>()
            .join(", ");
        format!("\n  versions = [{list}]")
    };

    format!(
        r#"
        [proxy]
        name = "parity-proxy"
        version = "1.0.0"
        endpoint_group_list = ["search"]
        expose_grouped_in_default = false
        [proxy.listen]
        port = 9099
        [proxy.protocol_support]{versions_toml}

        [[backends]]
        name = "search_srv"
        transport = "stdio"
        command = "echo"
        endpoint_groups = ["search"]

        [[backends]]
        name = "math"
        transport = "stdio"
        command = "echo"
        "#
    )
}

/// Drive the real production endpoint-group builder with in-process backends,
/// mount on the root router, spawn on a random port, and return the address.
async fn spawn_parity_server(versions: &[&str]) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let config = mcp_proxy::ProxyConfig::parse(&config_with_versions(versions))
        .expect("config should parse");

    // One shared McpProxy with in-process backends (no external processes).
    let shared_proxy = McpProxy::builder(&config.proxy.name, &config.proxy.version)
        .separator(&config.proxy.separator)
        .backend("search_srv", ChannelTransport::new(search_router()))
        .await
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .build_strict()
        .await
        .expect("shared proxy should build");

    let registry = EndpointGroupRegistry::new();
    let (_group_routers, _grouped_names) =
        mcp_proxy::endpoint_router::build_endpoint_group_routers(
            &config,
            Some(&registry),
            Some(&shared_proxy),
            Arc::new(mcp_proxy::lazy_registry::LazyBackendRegistry::from_backends(vec![])),
        )
        .await
        .expect("build_endpoint_group_routers must succeed");

    let default_service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(shared_proxy.clone());

    // Build the root `/` router faithfully to production (proxy.rs): apply the
    // 2026-07-28 layers, set protocol_support from config, and inject the
    // Mcp-Method / MCP-Protocol-Version compat headers. This makes the root a
    // valid parity baseline instead of an artificially weaker router.
    let default_service = mcp_proxy::apply_2026_layers(default_service, &config);
    let (default_router, _sh) =
        tower_mcp::transport::http::HttpTransport::from_service(default_service)
            .protocol_support(mcp_proxy::build_protocol_support(&config).unwrap())
            .into_router_with_handle();
    let default_router = default_router.layer(axum::middleware::from_fn(inject_mcp_compat_headers));
    let router = build_dynamic_endpoint_group_router(default_router, registry);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    (addr, handle)
}

/// POST `server/discover` (stateless 2026-07-28) on the given path and return
/// the HTTP status plus the parsed `supportedVersions` (if present).
///
/// Sends the full modern `_meta` envelope + `MCP-Protocol-Version` header that
/// the tower_mcp HTTP transport requires for 2026-07-28 requests (matching the
/// live probe that reproduced the user's symptom).
async fn discover_on_path(client: &reqwest::Client, base: &str, path: &str) -> (u16, Vec<String>) {
    discover_on_path_with_version(client, base, path, "2026-07-28").await
}

/// Like [`discover_on_path`] but the discover request advertises the given
/// protocol version in both the `MCP-Protocol-Version` header and the `_meta`
/// envelope. Used to probe a server that may be restricted to an older version.
async fn discover_on_path_with_version(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    version: &str,
) -> (u16, Vec<String>) {
    let resp = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", version)
        .body(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": {
                    "protocolVersion": version,
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": version,
                        "io.modelcontextprotocol/clientInfo": { "name": "test", "version": "1.0.0" },
                        "io.modelcontextprotocol/clientCapabilities": {}
                    }
                }
            })
            .to_string(),
        )
        .send()
        .await
        .expect("server/discover request");

    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.expect("valid json");
    let versions = body
        .get("result")
        .and_then(|r| r.get("supportedVersions"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|v| v.as_str().unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default();

    (status, versions)
}

/// POST `subscriptions/listen` (2026-07-28) on the given path and return the
/// JSON-RPC error code (if any). Used to confirm the method is registered
/// (reachable) on the route — a -32601 means "method not found" (the bug).
async fn subscriptions_listen_error_code(
    client: &reqwest::Client,
    base: &str,
    path: &str,
) -> Option<i64> {
    let resp = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2026-07-28")
        .body(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "subscriptions/listen",
                "params": {
                    "protocolVersion": "2026-07-28",
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientInfo": { "name": "test", "version": "1.0.0" },
                        "io.modelcontextprotocol/clientCapabilities": {}
                    }
                }
            })
            .to_string(),
        )
        .send()
        .await
        .expect("subscriptions/listen request");

    let body: serde_json::Value = resp.json().await.expect("valid json");
    body.get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_i64())
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The endpoint group `/search/mcp` must serve `server/discover` (SEP-2575)
/// with the same `supportedVersions` as the root `/` route. This is the exact
/// symptom the user reported: a client that detects 2026-07-28 support via
/// `server/discover` concluded the group was legacy.
#[tokio::test]
async fn test_endpoint_group_server_discover_parity_with_root() {
    let (addr, handle) = spawn_parity_server(&["2026-07-28", "2025-11-25"]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let (root_status, root_versions) = discover_on_path(&client, &base, "/").await;
    let (group_status, group_versions) = discover_on_path(&client, &base, "/search/mcp").await;

    assert_eq!(
        root_status, 200,
        "root server/discover should return 200, got {root_status}"
    );
    assert_eq!(
        group_status, 200,
        "group server/discover should return 200 (regression: was -32601), got {group_status}"
    );

    assert_eq!(
        root_versions,
        vec!["2026-07-28".to_string(), "2025-11-25".to_string()],
        "root should advertise configured versions"
    );
    assert_eq!(
        group_versions, root_versions,
        "group must advertise the SAME supportedVersions as root (protocol parity)"
    );
    assert!(
        group_versions.contains(&"2026-07-28".to_string()),
        "group must advertise 2026-07-28, got {group_versions:?}"
    );

    handle.abort();
}

/// Restricted config (`versions = ["2025-11-25"]`) must NOT advertise
/// 2026-07-28 on the group route. This is the primary regression guard: if the
/// group silently falls back to tower_mcp's compiled defaults (which include
/// 2026-07-28) or to the root's version set, this fails.
#[tokio::test]
async fn test_endpoint_group_honors_restricted_protocol_versions() {
    let (addr, handle) = spawn_parity_server(&["2025-11-25"]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // Probe with a 2025-11-25 discover request (the only version the server
    // advertises). A 2026-07-28 request would be legitimately rejected with 400
    // by BOTH root and group, so it cannot distinguish a regression.
    let (root_status, root_versions) =
        discover_on_path_with_version(&client, &base, "/", "2025-11-25").await;
    let (group_status, group_versions) =
        discover_on_path_with_version(&client, &base, "/search/mcp", "2025-11-25").await;

    assert_eq!(root_status, 200);
    assert_eq!(group_status, 200);

    assert_eq!(
        root_versions,
        vec!["2025-11-25".to_string()],
        "root must honor restricted versions"
    );
    assert_eq!(
        group_versions, root_versions,
        "group must honor the SAME restricted versions as root"
    );
    assert!(
        !group_versions.contains(&"2026-07-28".to_string()),
        "group must NOT advertise 2026-07-28 when config restricts to 2025-11-25, got {group_versions:?}"
    );

    handle.abort();
}

/// `subscriptions/listen` (2026-07-28) must be reachable on the group route.
/// A -32601 ("method not found") would indicate the 2026-07-28 method registry
/// is missing on the group (the bug class). We expect a -32602 (missing
/// `notifications` param) which proves the method is registered and the
/// 2026-07-28 layers are wired.
#[tokio::test]
async fn test_endpoint_group_subscriptions_listen_reachable() {
    let (addr, handle) = spawn_parity_server(&["2026-07-28", "2025-11-25"]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let root_code = subscriptions_listen_error_code(&client, &base, "/").await;
    let group_code = subscriptions_listen_error_code(&client, &base, "/search/mcp").await;

    // Both should reach the handler (registered method) and reject the missing
    // `notifications` param with -32602, NOT -32601 (method not found).
    assert_eq!(
        root_code,
        Some(-32602),
        "root subscriptions/listen should be registered (-32602), got {root_code:?}"
    );
    assert_eq!(
        group_code,
        Some(-32602),
        "group subscriptions/listen should be registered (-32602), got {group_code:?} (regression: -32601)"
    );

    handle.abort();
}

/// Group tool scoping (GroupFilterService) must be preserved after the 2026
/// layers are added. The `search` group must expose only `search_srv` tools and
/// must NOT leak the ungrouped `math` backend.
#[tokio::test]
async fn test_endpoint_group_tool_scoping_preserved() {
    let (addr, handle) = spawn_parity_server(&["2026-07-28", "2025-11-25"]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let group_tools = list_tools_on_path(&client, &base, "/search/mcp").await;
    assert!(
        group_tools.iter().any(|n| n.starts_with("search_srv")),
        "group must expose search_srv tools, got {group_tools:?}"
    );
    assert!(
        !group_tools.iter().any(|n| n.starts_with("math")),
        "group must NOT expose math tools (group filter), got {group_tools:?}"
    );

    let main_tools = list_tools_on_path(&client, &base, "/").await;
    assert!(
        main_tools.iter().any(|n| n.starts_with("math")),
        "ungrouped backend math must be on main endpoint, got {main_tools:?}"
    );

    handle.abort();
}

/// The warm-catalog serving layer (C11/C19) must be wired into the endpoint-group
/// middleware stack WITHOUT breaking the build when a lazy backend is a member
/// of the group. This locks in stack parity: the same `WarmCatalogLayer` that
/// wraps the root `/` stack also wraps every endpoint-group stack.
///
/// We drive the real `build_endpoint_group_routers` with a lazy stdio backend
/// (`spawn_mode = "lazy"`) placed in the group and assert the group routers
/// build successfully (the layer is present and the registry is threaded
/// through). A regression that dropped the layer or its registry argument would
/// either fail to compile or panic here.
#[tokio::test]
async fn test_endpoint_group_builds_with_lazy_backend_in_group() {
    let config_toml = r#"
        [proxy]
        name = "parity-proxy"
        version = "1.0.0"
        endpoint_group_list = ["search"]
        expose_grouped_in_default = false
        [proxy.listen]
        port = 9099
        [proxy.protocol_support]
        versions = ["2026-07-28", "2025-11-25"]

        [[backends]]
        name = "search_srv"
        transport = "stdio"
        command = "echo"
        endpoint_groups = ["search"]

        [[backends]]
        name = "lazy_files"
        transport = "stdio"
        command = "echo"
        spawn_mode = "lazy"
        endpoint_groups = ["search"]
        "#;

    let config = mcp_proxy::ProxyConfig::parse(config_toml).expect("config should parse");

    // Shared proxy with in-process backends (lazy stdio backend is still
    // eagerly spawned in Wave 3, so ChannelTransport stands in for it here).
    let shared_proxy = McpProxy::builder(&config.proxy.name, &config.proxy.version)
        .separator(&config.proxy.separator)
        .backend("search_srv", ChannelTransport::new(search_router()))
        .await
        .backend("lazy_files", ChannelTransport::new(math_router()))
        .await
        .build_strict()
        .await
        .expect("shared proxy should build");

    // A warm catalog registry with a cached tool for the lazy backend. This is
    // the data the WarmCatalogLayer will append to List* responses.
    let warm_catalog = mcp_proxy::warm_cache::WarmCatalog::from_probe_result(
        "lazy_files",
        &config.proxy.separator,
        vec![tower_mcp_types::protocol::ToolDefinition {
            name: "read".to_string(),
            title: None,
            description: Some("read file".to_string()),
            input_schema: serde_json::json!({"type": "object"}),
            output_schema: None,
            icons: None,
            annotations: None,
            execution: None,
            meta: None,
        }],
        vec![],
        vec![],
        vec![],
        Some("2025-11-25".to_string()),
        "testhash".to_string(),
    );
    let lazy_backend = mcp_proxy::lazy_registry::LazyBackend {
        config: config
            .backends
            .iter()
            .find(|b| b.name == "lazy_files")
            .unwrap()
            .clone(),
        catalog: Some(warm_catalog),
        protocol_version: None,
    };
    let lazy_registry =
        Arc::new(mcp_proxy::lazy_registry::LazyBackendRegistry::from_backends(vec![lazy_backend]));

    let registry = EndpointGroupRegistry::new();
    let result = mcp_proxy::endpoint_router::build_endpoint_group_routers(
        &config,
        Some(&registry),
        Some(&shared_proxy),
        lazy_registry,
    )
    .await;

    assert!(
        result.is_ok(),
        "endpoint-group routers (with lazy backend + warm-catalog layer) must build: {:?}",
        result.err()
    );
    let (group_routers, grouped_names) = result.unwrap();
    assert_eq!(
        group_routers.len(),
        1,
        "exactly one endpoint group expected"
    );
    assert!(
        grouped_names.contains("lazy_files"),
        "lazy_files must be reported as a grouped backend, got {grouped_names:?}"
    );
}
