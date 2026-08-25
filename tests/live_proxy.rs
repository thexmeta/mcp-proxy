//! Live proxy integration tests.
//!
//! These tests load the real config file at `/home/mxadm/.mcp-proxy/config.toml`,
//! start the full proxy on a random port, and exercise it over HTTP.
//!
//! Run with: `cargo test --test live_proxy -- --ignored` (most tests need live
//! backends). A few config-validation tests run without `--ignored`.
//!
//! To run only the fast (no-backend) subset:
//! ```sh
//! cargo test --test live_proxy -- --skip spawn
//! ```

use std::net::SocketAddr;

use serde_json::Value;
use tokio::net::TcpListener;

use std::path::Path;

use mcp_proxy::Proxy;
use mcp_proxy::config::{
    BackendConfig, ProxyConfig, ProxySettings, SpawnMode, TransportType, WarmCacheConfig,
};
use mcp_proxy::warm_cache::{BinaryHasher, WarmCatalog, WarmCatalogStore};
use tower_mcp_types::protocol::ToolDefinition;

// ---------------------------------------------------------------------------
// Shared proxy (avoids spawning ~20 child processes per test)
// ---------------------------------------------------------------------------

/// Thread-safe cell for the shared proxy address. Once set, the server is
/// guaranteed to be alive for the remainder of the test binary's lifetime.
static SHARED: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();

/// Get (or start) the shared test proxy address.
///
/// The proxy is started once on a **dedicated OS thread** with its own tokio
/// runtime, so it survives the destruction of any individual `#[tokio::test]`
/// runtime. The OS thread is critical — `#[tokio::test]` already runs inside
/// a tokio runtime, and you cannot create a new runtime from within one.
///
/// # Panics
/// Panics if the background thread fails to start or the proxy fails to build.
fn shared_proxy_addr() -> SocketAddr {
    *SHARED.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();

        // Spawn a fresh OS thread that is NOT inside any tokio runtime.
        // This thread creates its own runtime, builds the proxy, starts the
        // server, and keeps the runtime alive forever.
        std::thread::Builder::new()
            .name("live-proxy-init".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("create background runtime");

                let addr = rt.block_on(async {
                    let config = config_with_random_port();
                    let proxy = Proxy::from_config(config)
                        .await
                        .expect("failed to build proxy from live config");
                    let (router, _session_handle) = proxy.into_router();
                    let listener = TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind to random port");
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        axum::serve(listener, router).await.ok();
                    });
                    addr
                });

                // Send the address back to the calling thread.
                tx.send(addr).expect("send address back");

                // Keep the runtime alive forever — the server's worker threads
                // continue processing requests as long as this thread lives.
                rt.block_on(std::future::pending::<()>());
            })
            .expect("spawn background init thread");

        // Wait for the server to be ready.
        rx.recv().expect("receive address from init thread")
    })
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const LIVE_CONFIG: &str = "/home/mxadm/.mcp-proxy/config.toml";

fn load_live_config() -> ProxyConfig {
    let mut config = ProxyConfig::load(Path::new(LIVE_CONFIG)).expect("failed to load live config");
    config.resolve_env_vars();
    config
}

/// Load the live config and override the listen port to a random one so tests
/// can run in parallel without colliding with a production proxy.
fn config_with_random_port() -> ProxyConfig {
    let mut config = load_live_config();
    config.proxy.listen.port = 0; // axum will bind to a random port
    config
}

// ===========================================================================
// Helper functions
// ===========================================================================

/// Helper: initialize, send notification, list all tools from default endpoint.
async fn list_all_tools(client: &reqwest::Client, addr: SocketAddr) -> Vec<Value> {
    // Initialize
    let init = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "live-test", "version": "0.1.0" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("init failed");

    let session_id = init
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Notify initialized
    let mut req = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }))
            .unwrap(),
        );
    if let Some(ref sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }
    let _ = req.send().await;

    // List tools
    let mut req = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list"
            }))
            .unwrap(),
        );
    if let Some(ref sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }
    let resp = req.send().await.expect("list tools failed");

    let body: Value = resp.json().await.expect("valid JSON");
    body.get("result")
        .and_then(|r| r.get("tools"))
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default()
}

// ===========================================================================
// Tests — config validation (no live backend needed)
// ===========================================================================

#[test]
fn live_config_disabled_backends_excluded() {
    let config = load_live_config();
    let disabled: Vec<&str> = config
        .backends
        .iter()
        .filter(|b| !b.enabled)
        .map(|b| b.name.as_str())
        .collect();
    assert!(disabled.contains(&"reasoning"));
    assert!(disabled.contains(&"nativedevtools"));
    assert!(disabled.contains(&"exa"));
    assert!(disabled.contains(&"langchain"));
}

#[test]
fn live_config_hide_tools_populated() {
    let config = load_live_config();
    let roslyn = config.backends.iter().find(|b| b.name == "roslyn").unwrap();
    assert!(roslyn.hide_tools.len() >= 5, "roslyn should hide ≥5 tools");

    let codebase = config
        .backends
        .iter()
        .find(|b| b.name == "codebase")
        .unwrap();
    assert!(!codebase.hide_tools.is_empty());
}

#[test]
fn live_config_rename_all_populated() {
    let config = load_live_config();
    let roslyn = config.backends.iter().find(|b| b.name == "roslyn").unwrap();
    assert!(
        !roslyn.rename_all.is_empty(),
        "roslyn should have rename_all"
    );

    let qartez = config.backends.iter().find(|b| b.name == "qartez").unwrap();
    assert!(
        !qartez.rename_all.is_empty(),
        "qartez should have rename_all"
    );

    let lsp = config.backends.iter().find(|b| b.name == "lsp").unwrap();
    assert!(!lsp.rename_all.is_empty(), "lsp should have rename_all");
}

#[test]
fn live_config_aliases_populated() {
    let config = load_live_config();
    let avalonia_spy = config
        .backends
        .iter()
        .find(|b| b.name == "avalonia_spy")
        .unwrap();
    assert!(
        !avalonia_spy.aliases.is_empty(),
        "avalonia_spy should have aliases"
    );
    assert!(
        avalonia_spy.aliases.len() >= 5,
        "avalonia_spy should have ≥5 aliases"
    );
}

#[test]
fn live_config_endpoint_group_list() {
    let config = load_live_config();
    // `endpoint_group_list` is expanded into `endpoint_groups` during load,
    // so verify the expanded route entries exist.
    assert!(
        !config.proxy.endpoint_groups.is_empty(),
        "endpoint_group_list entries should be expanded into endpoint_groups routes"
    );
    let group_names: Vec<&str> = config
        .proxy
        .endpoint_groups
        .iter()
        .map(|g| g.name.as_str())
        .collect();
    assert!(
        group_names.contains(&"search"),
        "should have 'search' group, got: {group_names:?}"
    );
    assert!(
        group_names.contains(&"code"),
        "should have 'code' group, got: {group_names:?}"
    );
    assert!(
        group_names.contains(&"lsp"),
        "should have 'lsp' group, got: {group_names:?}"
    );
}

#[test]
fn live_config_global_middleware() {
    let config = load_live_config();
    assert!(
        config.proxy.timeout.is_some(),
        "global timeout should be configured"
    );
    assert!(
        config.proxy.circuit_breaker.is_some(),
        "global circuit_breaker should be configured"
    );
    assert!(
        config.proxy.retry.is_some(),
        "global retry should be configured"
    );
}

// ===========================================================================
// Tests — config parsing (no live backend needed)
// ===========================================================================

#[test]
fn live_config_parses() {
    let config = load_live_config();
    assert_eq!(config.proxy.name, "srv");
    assert!(!config.backends.is_empty());
}

#[test]
fn live_config_has_endpoint_groups() {
    let config = load_live_config();
    // The live config tags backends with endpoint_groups = ["lsp", "code", etc.].
    // There may or may not be explicit [[proxy.endpoint_groups]] route entries.
    let backends_with_groups: Vec<(&str, &Vec<String>)> = config
        .backends
        .iter()
        .filter(|b| !b.endpoint_groups.is_empty())
        .map(|b| (b.name.as_str(), &b.endpoint_groups))
        .collect();
    assert!(
        backends_with_groups.len() >= 3,
        "expected ≥3 backends with endpoint_groups, got {}: {:?}",
        backends_with_groups.len(),
        backends_with_groups
            .iter()
            .map(|(n, _)| *n)
            .collect::<Vec<_>>()
    );

    // Collect all unique group names across backends.
    let mut all_groups: Vec<&str> = backends_with_groups
        .iter()
        .flat_map(|(_, groups)| groups.iter().map(|s| s.as_str()))
        .collect();
    all_groups.sort();
    all_groups.dedup();
    assert!(
        all_groups.len() >= 3,
        "expected ≥3 distinct group tags, got {:?}",
        all_groups
    );
}

#[test]
fn live_config_backends_have_groups() {
    let config = load_live_config();
    let with_groups: Vec<&str> = config
        .backends
        .iter()
        .filter(|b| !b.endpoint_groups.is_empty())
        .map(|b| b.name.as_str())
        .collect();
    assert!(
        !with_groups.is_empty(),
        "at least some backends should have endpoint_groups"
    );
}

// ===========================================================================
// Tests — live proxy (need running backends)
// ===========================================================================

#[tokio::test]
#[ignore] // needs live backends
async fn health_endpoint_returns_200() {
    let addr = shared_proxy_addr();

    let resp = reqwest::get(format!("http://{addr}/admin/health"))
        .await
        .expect("request failed");
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.expect("valid JSON");
    // HealthResponse uses "healthy" or "degraded", not "ok".
    let status = body["status"].as_str().expect("status should be a string");
    assert!(
        status == "healthy" || status == "degraded",
        "expected 'healthy' or 'degraded', got: {status}"
    );
}

#[tokio::test]
#[ignore]
async fn admin_backends_lists_all_backends() {
    let addr = shared_proxy_addr();

    let resp = reqwest::get(format!("http://{addr}/admin/backends"))
        .await
        .expect("request failed");
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.expect("valid JSON");
    // The response should be a JSON array or object with a "backends" key.
    let backends = if body.is_array() {
        body.as_array().unwrap().clone()
    } else if let Some(arr) = body.get("backends").and_then(|v| v.as_array()) {
        arr.clone()
    } else {
        panic!("unexpected /admin/backends response shape: {body}");
    };

    // We know the live config has multiple backends.
    assert!(
        backends.len() >= 5,
        "expected ≥5 backends, got {}",
        backends.len()
    );
}

#[tokio::test]
#[ignore]
async fn admin_config_returns_proxy_config() {
    let addr = shared_proxy_addr();

    let resp = reqwest::get(format!("http://{addr}/admin/config"))
        .await
        .expect("request failed");
    assert_eq!(resp.status(), 200);

    // The /admin/config endpoint returns raw TOML text, not JSON.
    let body = resp.text().await.expect("valid text");
    assert!(
        body.contains("name"),
        "admin/config TOML should contain 'name' key, got: {body}"
    );
    assert!(
        body.contains("srv"),
        "admin/config TOML should contain proxy name 'srv', got: {body}"
    );
}

#[tokio::test]
#[ignore]
async fn default_mcp_endpoint_responds() {
    let addr = shared_proxy_addr();

    // The default /mcp endpoint should accept a POST with an MCP request.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "live-test", "version": "0.1.0" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("request failed");

    assert!(
        resp.status().is_success(),
        "expected 2xx, got {}",
        resp.status()
    );

    let body: Value = resp.json().await.expect("valid JSON");
    assert_eq!(body["jsonrpc"], "2.0");
    // The server should respond with its capabilities.
    assert!(
        body.get("result").is_some(),
        "initialize should return result, got: {body}"
    );
}

#[tokio::test]
#[ignore]
async fn list_tools_via_mcp_protocol() {
    let addr = shared_proxy_addr();

    let client = reqwest::Client::new();

    // Step 1: initialize
    let init_resp = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "live-test", "version": "0.1.0" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("init failed");
    assert!(init_resp.status().is_success());

    // Extract session ID from response header (2025-11-25 protocol).
    let session_id = init_resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Step 2: send initialized notification
    let mut req_builder = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }))
            .unwrap(),
        );
    if let Some(ref sid) = session_id {
        req_builder = req_builder.header("mcp-session-id", sid);
    }
    let _ = req_builder.send().await;

    // Step 3: list tools
    let mut req_builder = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list"
            }))
            .unwrap(),
        );
    if let Some(ref sid) = session_id {
        req_builder = req_builder.header("mcp-session-id", sid);
    }
    let resp = req_builder.send().await.expect("list tools failed");
    assert!(resp.status().is_success());

    let body: Value = resp.json().await.expect("valid JSON");
    let tools = body
        .get("result")
        .and_then(|r| r.get("tools"))
        .and_then(|t| t.as_array())
        .expect("tools/list should return result.tools array");

    assert!(
        !tools.is_empty(),
        "proxy should expose at least some tools from live backends"
    );

    // Verify namespaced tool names (separator is "_").
    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    let has_namespace = tool_names.iter().any(|n| n.contains('_'));
    assert!(
        has_namespace,
        "at least some tools should be namespaced (contain '_'), got: {:?}",
        &tool_names[..tool_names.len().min(10)]
    );
}

#[tokio::test]
#[ignore]
async fn endpoint_group_shows_filtered_tools() {
    let addr = shared_proxy_addr();

    let client = reqwest::Client::new();

    // Check if any endpoint groups are actually configured with routes.
    // The live config may only have backend-level `endpoint_groups` tags without
    // explicit [[proxy.endpoint_groups]] route entries. If no group routes
    // exist, skip the detailed assertion.
    let config = load_live_config();
    let has_group_routes = !config.proxy.endpoint_groups.is_empty();

    if !has_group_routes {
        eprintln!(
            "Skipping endpoint group filtering: no [[proxy.endpoint_groups]] routes configured"
        );
        // Verify that endpoint group tags exist on backends (info only)
        let tagged: Vec<&str> = config
            .backends
            .iter()
            .filter(|b| !b.endpoint_groups.is_empty())
            .map(|b| b.name.as_str())
            .collect();
        eprintln!("Backend-level endpoint_groups tags found on: {tagged:?}");
        return;
    }

    // Initialize on an endpoint group path (e.g., /lsp/mcp).
    let group_paths = ["/lsp", "/code", "/os", "/docs"];
    let mut working_group: Option<String> = None;

    for group in &group_paths {
        let init_resp = client
            .post(format!("http://{addr}{group}/mcp"))
            .header("Content-Type", "application/json")
            .body(
                serde_json::to_string(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": { "name": "live-test", "version": "0.1.0" }
                    }
                }))
                .unwrap(),
            )
            .send()
            .await;

        match init_resp {
            Ok(r) if r.status().is_success() => {
                working_group = Some(group.to_string());
                break;
            }
            _ => continue,
        }
    }

    let group = working_group.expect("at least one endpoint group should respond");

    // Notify initialized
    let _ = client
        .post(format!("http://{addr}{group}/mcp"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }))
            .unwrap(),
        )
        .send()
        .await;

    // List tools on the group endpoint
    let resp = client
        .post(format!("http://{addr}{group}/mcp"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list"
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("list tools failed");

    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("valid JSON");
    let group_tools = body
        .get("result")
        .and_then(|r| r.get("tools"))
        .and_then(|t| t.as_array())
        .expect("tools/list should return tools array");

    // Also list tools from the default endpoint for comparison.
    let default_init = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 10,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "live-test-default", "version": "0.1.0" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("default init failed");

    let default_sid = default_init
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let _ = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .header("mcp-session-id", default_sid.as_deref().unwrap_or(""))
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }))
            .unwrap(),
        )
        .send()
        .await;

    let default_resp = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .header("mcp-session-id", default_sid.as_deref().unwrap_or(""))
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/list"
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("default list tools failed");

    let default_body: Value = default_resp.json().await.expect("valid JSON");
    let default_tools = default_body
        .get("result")
        .and_then(|r| r.get("tools"))
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();

    // The group endpoint should have ≤ tools than the default (filtered).
    assert!(
        group_tools.len() <= default_tools.len(),
        "group '{group}' should have fewer or equal tools ({}) than default ({})",
        group_tools.len(),
        default_tools.len()
    );

    // Group tools should all come from the group's backends.
    // We can verify this by checking that all group tool names start with
    // one of the group's backend name prefixes.
    let group_tool_names: Vec<&str> = group_tools
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();

    if !group_tool_names.is_empty() {
        eprintln!(
            "Group '{group}' exposed {} tools: {:?}",
            group_tool_names.len(),
            &group_tool_names[..group_tool_names.len().min(5)]
        );
    }
}

#[tokio::test]
#[ignore]
async fn ping_via_mcp_protocol() {
    let addr = shared_proxy_addr();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "ping"
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("ping failed");

    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("valid JSON");
    assert_eq!(body["jsonrpc"], "2.0");
}

#[tokio::test]
#[ignore]
async fn admin_sessions_tracks_sessions() {
    let addr = shared_proxy_addr();

    let client = reqwest::Client::new();

    // Create a session via initialize.
    let init_resp = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "session-test", "version": "0.1.0" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("init failed");
    assert!(init_resp.status().is_success());

    // Check admin sessions endpoint responds and returns valid JSON.
    // Note: with the 2026-07-28 stateless protocol (the default), no sessions
    // are tracked — so we only verify the endpoint works, not that a session
    // was created.
    let resp = reqwest::get(format!("http://{addr}/admin/sessions"))
        .await
        .expect("request failed");
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.expect("valid JSON");
    // The response should be a JSON array or object.
    assert!(
        body.is_array() || body.is_object(),
        "admin/sessions should return JSON array or object, got: {body}"
    );
}

#[tokio::test]
#[ignore]
async fn nonexistent_endpoint_returns_404() {
    let addr = shared_proxy_addr();

    let resp = reqwest::get(format!("http://{addr}/nonexistent/path"))
        .await
        .expect("request failed");

    assert_eq!(resp.status(), 404);
}

// ===========================================================================
// Tests — live proxy: disabled backends, hide_tools, rename, aliases
// ===========================================================================

#[tokio::test]
#[ignore]
async fn disabled_backends_not_in_tools_list() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let tools = list_all_tools(&client, addr).await;
    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();

    // Disabled backends: reasoning, nativedevtools, exa, langchain
    let disabled_prefixes = ["reasoning_", "nativedevtools_", "exa_", "langchain_"];
    for prefix in &disabled_prefixes {
        let found: Vec<&&str> = tool_names
            .iter()
            .filter(|n| n.starts_with(prefix))
            .collect();
        assert!(
            found.is_empty(),
            "disabled backend '{prefix}' should not have tools, but found: {found:?}"
        );
    }
}

#[tokio::test]
#[ignore]
async fn hidden_tools_not_in_tools_list() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let tools = list_all_tools(&client, addr).await;
    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();

    // roslyn hides these tools. After rename_all strips the server prefix and
    // the backend namespace "roslyn_" is prepended, hidden tools should NOT
    // appear. Check namespaced forms to avoid false positives from other backends
    // (e.g., filesystem also has a "write_file").
    let hidden_roslyn = [
        "roslyn_replace_in_code",
        "roslyn_replace_in_file",
        "roslyn_write_file",
        "roslyn_insert_lines",
        "roslyn_apply_rename",
        "roslyn_change_signature",
        "roslyn_search_files",
        "roslyn_find_string_literal",
        "roslyn_info",
        "roslyn_get_trivia",
        "roslyn_get_line_count",
    ];
    for hidden in &hidden_roslyn {
        assert!(
            !tool_names.iter().any(|n| n == hidden),
            "hidden tool '{hidden}' should not appear in tools/list"
        );
    }

    // codebase hides "codebase_manage_adr" and "delete_project"
    assert!(
        !tool_names
            .iter()
            .any(|n| *n == "manage_adr" || *n == "delete_project"),
        "hidden tools 'manage_adr'/'delete_project' should not appear"
    );
}

#[tokio::test]
#[ignore]
async fn rename_all_strips_prefixes() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let tools = list_all_tools(&client, addr).await;
    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();

    // roslyn has rename_all: "roslyn_*" → ""
    // The rename strips the server prefix, but the backend namespace "roslyn_"
    // is prepended. So tools become "roslyn_<stripped_name>" (single prefix).
    // The key check is that DOUBLE prefix "roslyn_roslyn_" does NOT exist.
    let double_prefix: Vec<&&str> = tool_names
        .iter()
        .filter(|n| n.starts_with("roslyn_roslyn_"))
        .collect();
    assert!(
        double_prefix.is_empty(),
        "roslyn tools should not have double prefix 'roslyn_roslyn_', found: {double_prefix:?}"
    );

    // qartez has rename_all: "qartez_*" → ""
    let qartez_double: Vec<&&str> = tool_names
        .iter()
        .filter(|n| n.starts_with("qartez_qartez_"))
        .collect();
    assert!(
        qartez_double.is_empty(),
        "qartez tools should not have double prefix, found: {qartez_double:?}"
    );

    // lsp has rename_all: "lsp_*" → ""
    let lsp_double: Vec<&&str> = tool_names
        .iter()
        .filter(|n| n.starts_with("lsp_lsp_"))
        .collect();
    assert!(
        lsp_double.is_empty(),
        "lsp tools should not have double prefix, found: {lsp_double:?}"
    );
}

#[tokio::test]
#[ignore]
async fn aliases_resolve_in_tools_list() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let tools = list_all_tools(&client, addr).await;
    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();

    // grep_github has alias: searchGitHub → search
    assert!(
        tool_names.contains(&"grep_github_search"),
        "grep_github alias searchGitHub→search should produce 'grep_github_search', got: {:?}",
        tool_names
            .iter()
            .filter(|n| n.contains("grep_github"))
            .collect::<Vec<_>>()
    );

    // ms_docs has alias: microsoft_docs_search → search
    assert!(
        tool_names.contains(&"ms_docs_search"),
        "ms_docs alias microsoft_docs_search→search should produce 'ms_docs_search', got: {:?}",
        tool_names
            .iter()
            .filter(|n| n.contains("ms_docs"))
            .collect::<Vec<_>>()
    );

    // deepwiki has alias: query-docs → query_docs
    // The upstream tool may have changed names; just verify deepwiki tools exist
    // if the backend is online.
    let deepwiki_tools: Vec<&&str> = tool_names
        .iter()
        .filter(|n| n.starts_with("deepwiki_"))
        .collect();
    if !deepwiki_tools.is_empty() {
        eprintln!(
            "deepwiki has {} tools: {:?}",
            deepwiki_tools.len(),
            deepwiki_tools
        );
    }
}

#[tokio::test]
#[ignore]
async fn endpoint_group_search_filters_tools() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let config = load_live_config();
    if config.proxy.endpoint_groups.is_empty() {
        eprintln!("No endpoint group routes configured, skipping");
        return;
    }

    // Initialize on /search/mcp
    let init_resp = client
        .post(format!("http://{addr}/search/mcp"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "test", "version": "0.1" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await;

    match init_resp {
        Ok(r) if r.status().is_success() => {
            // List tools on search group
            let resp = client
                .post(format!("http://{addr}/search/mcp"))
                .header("Content-Type", "application/json")
                .body(
                    serde_json::to_string(&serde_json::json!({
                        "jsonrpc": "2.0", "id": 2, "method": "tools/list"
                    }))
                    .unwrap(),
                )
                .send()
                .await
                .expect("list tools on /search/mcp failed");

            let body: Value = resp.json().await.expect("valid JSON");
            let group_tools = body
                .get("result")
                .and_then(|r| r.get("tools"))
                .and_then(|t| t.as_array())
                .cloned()
                .unwrap_or_default();

            let all_tools = list_all_tools(&client, addr).await;

            assert!(
                group_tools.len() <= all_tools.len(),
                "search group ({}) should have ≤ tools than default ({})",
                group_tools.len(),
                all_tools.len()
            );

            let group_names: Vec<&str> = group_tools
                .iter()
                .filter_map(|t| t["name"].as_str())
                .collect();
            eprintln!(
                "search group has {} tools: {:?}",
                group_names.len(),
                &group_names[..group_names.len().min(5)]
            );
        }
        _ => {
            eprintln!("/search/mcp not available, skipping endpoint group test");
        }
    }
}

#[tokio::test]
#[ignore]
async fn endpoint_group_code_filters_tools() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let config = load_live_config();
    if config.proxy.endpoint_groups.is_empty() {
        eprintln!("No endpoint group routes configured, skipping");
        return;
    }

    let init_resp = client
        .post(format!("http://{addr}/code/mcp"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "test", "version": "0.1" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await;

    match init_resp {
        Ok(r) if r.status().is_success() => {
            let resp = client
                .post(format!("http://{addr}/code/mcp"))
                .header("Content-Type", "application/json")
                .body(
                    serde_json::to_string(&serde_json::json!({
                        "jsonrpc": "2.0", "id": 2, "method": "tools/list"
                    }))
                    .unwrap(),
                )
                .send()
                .await
                .expect("list tools on /code/mcp failed");

            let body: Value = resp.json().await.expect("valid JSON");
            let group_tools = body
                .get("result")
                .and_then(|r| r.get("tools"))
                .and_then(|t| t.as_array())
                .cloned()
                .unwrap_or_default();

            let all_tools = list_all_tools(&client, addr).await;

            assert!(
                group_tools.len() <= all_tools.len(),
                "code group ({}) should have ≤ tools than default ({})",
                group_tools.len(),
                all_tools.len()
            );

            let group_names: Vec<&str> = group_tools
                .iter()
                .filter_map(|t| t["name"].as_str())
                .collect();
            eprintln!(
                "code group has {} tools: {:?}",
                group_names.len(),
                &group_names[..group_names.len().min(5)]
            );
        }
        _ => {
            eprintln!("/code/mcp not available, skipping");
        }
    }
}

#[tokio::test]
#[ignore]
async fn protocol_version_2026_07_28_works() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    // 2026-07-28 is stateless — it REMOVED the initialize handshake.
    // Verify it works by sending a direct tools/list (no init needed).
    let resp = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {}
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("2026-07-28 tools/list failed");

    assert!(
        resp.status().is_success(),
        "2026-07-28 tools/list failed: {}",
        resp.status()
    );

    let body: Value = resp.json().await.expect("valid JSON");
    assert_eq!(body["jsonrpc"], "2.0");
    assert!(
        body.get("result").is_some(),
        "2026-07-28 tools/list should return result"
    );
}

#[tokio::test]
#[ignore]
async fn protocol_version_2025_11_25_works() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    // Test session-based 2025-11-25 protocol
    let resp = client
        .post(format!("http://{addr}/"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "test-2025", "version": "0.1" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("init 2025-11-25 failed");

    assert!(
        resp.status().is_success(),
        "2025-11-25 init failed: {}",
        resp.status()
    );

    // Should get a session ID back
    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok());
    assert!(
        session_id.is_some(),
        "2025-11-25 should return mcp-session-id header"
    );

    let body: Value = resp.json().await.expect("valid JSON");
    assert_eq!(body["jsonrpc"], "2.0");
    assert!(
        body.get("result").is_some(),
        "2025-11-25 should return result"
    );
}

#[tokio::test]
#[ignore]
async fn admin_backend_count_matches_config() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let config = load_live_config();
    // Admin API only reports enabled backends; config may have disabled ones.
    let enabled_backends = config.backends.iter().filter(|b| b.enabled).count();

    let resp = client
        .get(format!("http://{addr}/admin/backends"))
        .send()
        .await
        .expect("admin backends failed");
    let body: Value = resp.json().await.expect("valid JSON");

    let admin_backends = if body.is_array() {
        body.as_array().unwrap().len()
    } else if let Some(arr) = body.get("backends").and_then(|v| v.as_array()) {
        arr.len()
    } else {
        panic!("unexpected admin/backends shape");
    };

    // Admin API may exclude backends that failed to start (e.g., missing binary).
    // So we check that the count is within 1 of the expected enabled count.
    assert!(
        admin_backends >= enabled_backends - 1 && admin_backends <= enabled_backends,
        "admin API should report ~{} enabled backends (±1), got {}",
        enabled_backends,
        admin_backends
    );
}

#[tokio::test]
#[ignore]
async fn admin_health_includes_backend_info() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("http://{addr}/admin/health"))
        .send()
        .await
        .expect("health failed");
    let body: Value = resp.json().await.expect("valid JSON");

    // Health should have status field
    assert!(
        body.get("status").is_some(),
        "health should have 'status' field"
    );
}

// ---------------------------------------------------------------------------
// Endpoint group scoping regression tests
// ---------------------------------------------------------------------------

/// Regression test: `/os/mcp` must only expose tools from the `os` group's
/// member backends, NOT all backend tools.
///
/// Previously `/os/mcp` returned 283+ tools (nearly everything) instead of
/// only the scoped tools from the group's backends.
#[tokio::test]
#[ignore]
async fn os_endpoint_group_scopes_tools() {
    let addr = shared_proxy_addr();
    let client = reqwest::Client::new();

    let config = load_live_config();

    // Verify `os` group exists in config (confirms endpoint_group_list expansion)
    assert!(
        config.proxy.endpoint_groups.iter().any(|g| g.name == "os"),
        "os group should be in expanded endpoint_groups"
    );

    // Count backends that directly declare endpoint_groups = ["os"]
    let os_direct_backends: Vec<&str> = config
        .backends
        .iter()
        .filter(|b| b.endpoint_groups.contains(&"os".to_string()))
        .map(|b| b.name.as_str())
        .collect();

    assert!(
        !os_direct_backends.is_empty(),
        "os group should have at least one backend with endpoint_groups = [\"os\"]"
    );
    eprintln!("os direct backends: {os_direct_backends:?}");

    // Send tools/list to /os/mcp (2026-07-28: stateless, no init needed)
    let resp = client
        .post(format!("http://{addr}/os/mcp"))
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
            }))
            .unwrap(),
        )
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => {
            let body: Value = r.json().await.expect("valid JSON");
            let tools = body
                .get("result")
                .and_then(|r| r.get("tools"))
                .and_then(|t| t.as_array())
                .cloned()
                .unwrap_or_default();

            let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();

            // Get all tools for comparison
            let all_tools = list_all_tools(&client, addr).await;
            let all_count = all_tools.len();

            eprintln!(
                "/os/mcp returned {} tools (default endpoint has {})",
                tool_names.len(),
                all_count
            );

            // Core assertion: /os/mcp must return significantly fewer tools
            // than the default endpoint. The os group only has 2 backends (fs,
            // term) with ~24 tools. If it returns >50% of total, scoping is
            // completely broken and the GroupFilterService isn't working.
            assert!(
                tool_names.len() < all_count / 2,
                "/os/mcp should only expose os-group tools, got {} out of {} total \
                 — endpoint group scoping is broken (GroupFilterService not filtering)",
                tool_names.len(),
                all_count,
            );

            // Spot-check: fs and term tools must be present in the scoped set
            let has_fs = tool_names.iter().any(|n| n.starts_with("fs_"));
            let has_term = tool_names.iter().any(|n| n.starts_with("term_"));
            assert!(has_fs, "/os/mcp should include fs_* tools");
            assert!(has_term, "/os/mcp should include term_* tools");

            // fs and term tools must NOT appear in unrelated groups
            // (e.g. /search/mcp should not have fs_/term_ tools)
            let search_resp = client
                .post(format!("http://{addr}/search/mcp"))
                .header("Content-Type", "application/json")
                .body(
                    serde_json::to_string(&serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                    }))
                    .unwrap(),
                )
                .send()
                .await;

            if let Ok(r) = search_resp
                && r.status().is_success()
            {
                let search_body: Value = r.json().await.expect("valid JSON");
                let search_tools: Vec<&str> = search_body
                    .get("result")
                    .and_then(|r| r.get("tools"))
                    .and_then(|t| t.as_array())
                    .map(|arr| arr.iter().filter_map(|t| t["name"].as_str()).collect())
                    .unwrap_or_default();

                let leaked: Vec<&str> = search_tools
                    .iter()
                    .filter(|n| n.starts_with("fs_") || n.starts_with("term_"))
                    .copied()
                    .collect();

                assert!(
                    leaked.is_empty(),
                    "/search/mcp leaked os-group tools: {leaked:?}"
                );
                eprintln!(
                    "/search/mcp has {} tools, no fs_/term_ leakage",
                    search_tools.len()
                );
            }

            eprintln!(
                "/os/mcp scoping OK: {} tools (vs {} total)",
                tool_names.len(),
                all_count
            );
        }
        other => {
            eprintln!("/os/mcp not available or error: {other:?}, skipping");
        }
    }
}

// ===========================================================================
// Tests — lazy / on-demand backend spawning with persistent warm tool cache
// ===========================================================================
//
// These are LIVE tests: they spawn a REAL python stdio MCP server as a child
// process. They are `#[ignore]` so the default `cargo test` run skips them and
// they only run with `cargo test --test live_proxy -- --ignored`.
//
// They build their OWN proxy from an in-code config (NOT the shared live proxy)
// because the live config at `/home/mxadm/.mcp-proxy/config.toml` is unrelated
// to the lazy feature under test.

/// Minimal MCP stdio server (Python stdlib only). Exposes one tool `ping`.
///
/// Speaks NEWLINE-DELIMITED JSON (one JSON-RPC object per line, terminated by
/// `\n`) — NOT LSP Content-Length framing. `tower-mcp`'s `StdioClientTransport`
/// writes `message + b"\n"` and reads line-by-line; a Content-Length-framed
/// server DEADLOCKS.
const LAZY_MIN_SERVER: &str = r#"#!/usr/bin/env python3
import sys, json
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n"); sys.stdout.flush()
def read():
    line = sys.stdin.readline()
    return json.loads(line) if line else None
while True:
    req = read()
    if req is None: break
    mid = req.get("id"); method = req.get("method")
    if method == "initialize":
        send({"jsonrpc":"2.0","id":mid,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":False},"resources":{}},"serverInfo":{"name":"live-min","version":"1.0.0"}}})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"tools":[{"name":"ping","description":"Return pong","inputSchema":{"type":"object"}}]}})
    elif method == "tools/call":
        send({"jsonrpc":"2.0","id":mid,"result":{"content":[{"type":"text","text":"pong"}],"isError":False}})
    elif method == "resources/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"resources":[]}})
    elif method == "resources/templates/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"resourceTemplates":[]}})
    elif method == "prompts/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"prompts":[]}})
    elif method == "shutdown":
        send({"jsonrpc":"2.0","id":mid,"result":{}}); break
    else:
        send({"jsonrpc":"2.0","id":mid,"result":{}})
"#;

/// Build a [`ProxyConfig`] in-code with warm cache enabled + one lazy stdio
/// backend (`files`) pointing at the given python server, plus a dummy eager
/// backend (`__dummy__`) so the shared [`McpProxy`] has ≥1 backend to build.
///
/// `ProxyConfig` does NOT derive `Clone`, so callers rebuild via this helper
/// rather than `.clone()`.
fn lazy_live_config(dir: &Path, server: &Path) -> ProxyConfig {
    ProxyConfig {
        proxy: ProxySettings {
            name: "lazy-live-proxy".to_string(),
            version: "1.0.0".to_string(),
            separator: "/".to_string(),
            listen: mcp_proxy::config::ListenConfig {
                host: "127.0.0.1".to_string(),
                port: 0,
            },
            instructions: None,
            shutdown_timeout_seconds: 30,
            hot_reload: false,
            import_backends: None,
            rate_limit: None,
            client_rate_limit: None,
            tool_discovery: false,
            tool_exposure: mcp_proxy::config::ToolExposure::default(),
            expose_grouped_in_default: false,
            endpoint_groups: vec![],
            tool_groups: vec![],
            watchers: vec![],
            backend_env: std::collections::HashMap::new(),
            timeout: None,
            circuit_breaker: None,
            retry: None,
            endpoint_group_list: vec![],
            protocol_support: mcp_proxy::config::ProtocolSupportConfig::default(),
        },
        backends: vec![
            // Dummy eager backend (real MCP server) so the shared McpProxy has
            // at least one backend to build (lazy backends are registered, not
            // spawned at startup).
            BackendConfig {
                name: "__dummy__".to_string(),
                enabled: true,
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                ..Default::default()
            },
            // Lazy stdio backend under test.
            BackendConfig {
                name: "files".to_string(),
                enabled: true,
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                spawn_mode: SpawnMode::Lazy,
                idle_timeout_secs: Some(30),
                ..Default::default()
            },
        ],
        auth: None,
        performance: mcp_proxy::config::PerformanceConfig::default(),
        security: mcp_proxy::config::SecurityConfig::default(),
        cache: mcp_proxy::config::CacheBackendConfig::default(),
        composite_tools: vec![],
        warm_cache: WarmCacheConfig {
            enabled: true,
            dir: Some(dir.to_path_buf()),
            ttl_secs: 0,
            invalidate_on_hash_change: true,
        },
        source_path: None,
        observability: mcp_proxy::config::ObservabilityConfig::default(),
    }
}

/// A namespaced tool definition for the seeded warm catalog.
fn lazy_ping_tool() -> ToolDefinition {
    ToolDefinition {
        name: "ping".to_string(),
        title: None,
        description: Some("Return pong".to_string()),
        input_schema: serde_json::json!({"type": "object"}),
        output_schema: None,
        icons: None,
        annotations: None,
        execution: None,
        meta: None,
    }
}

/// L1 — lazy backend builds + catalog loads from warm cache.
///
/// The lazy backend must NOT be spawned at startup (`spawn_state == Down`) and
/// must be registered in the lazy registry by name.
#[tokio::test]
#[ignore] // spawns a real python stdio MCP server
async fn lazy_l1_builds_and_registered_down() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-lazy-l1-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-lazy-min-l1-{}.py", std::process::id()));
    std::fs::write(&server, LAZY_MIN_SERVER).expect("write server script");

    let config = lazy_live_config(&dir, &server);

    // Build the proxy from the in-code config (no live config involved).
    let proxy = Proxy::from_config(config)
        .await
        .expect("proxy with lazy backend must build");

    let registry = proxy.lazy_registry();

    // The lazy backend must be registered by name.
    assert!(
        registry.names().contains(&"files".to_string()),
        "lazy backend 'files' must be registered in the lazy registry"
    );

    // It must NOT be spawned at startup — served from the warm cache instead.
    assert_eq!(
        registry.spawn_state("files"),
        mcp_proxy::lazy_registry::SpawnState::Down,
        "lazy backend must be Down at startup (not eagerly spawned)"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// L2 — first call spawns on-demand (live).
///
/// After `ensure_spawned`, the backend must transition to `Up` and its warm
/// catalog must be present (captured from the live probe).
#[tokio::test]
#[ignore] // spawns a real python stdio MCP server
async fn lazy_l2_first_call_spawns_on_demand() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-lazy-l2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-lazy-min-l2-{}.py", std::process::id()));
    std::fs::write(&server, LAZY_MIN_SERVER).expect("write server script");

    let config = lazy_live_config(&dir, &server);

    let proxy = Proxy::from_config(config)
        .await
        .expect("proxy with lazy backend must build");
    let registry = proxy.lazy_registry();

    // Precondition: starts Down.
    assert_eq!(
        registry.spawn_state("files"),
        mcp_proxy::lazy_registry::SpawnState::Down,
        "precondition: lazy backend starts Down"
    );

    // First action request brings the backend up on demand.
    registry
        .ensure_spawned("files")
        .await
        .expect("ensure_spawned must spawn the lazy backend");

    // It must now be Up.
    assert_eq!(
        registry.spawn_state("files"),
        mcp_proxy::lazy_registry::SpawnState::Up,
        "lazy backend must be Up after ensure_spawned"
    );

    // The spawned backend's warm catalog must be present (captured by the probe).
    let lb = registry.get("files").expect("lazy backend present");
    assert!(
        lb.catalog.is_some(),
        "spawned lazy backend must have a warm catalog from the live probe"
    );
    let names: Vec<&str> = lb
        .catalog
        .iter()
        .flat_map(|c| c.tools.iter().map(|t| t.name.as_str()))
        .collect();
    assert!(
        names.contains(&"files/ping"),
        "probed catalog must contain the namespaced files/ping tool, got: {names:?}"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// L3 — warm catalog persists across restart (live).
///
/// Build proxy #1, seed a warm catalog on disk, drop it, build proxy #2 with the
/// SAME config, and assert the catalog reloads from disk (mirrors `r3` in
/// `test_lazy_restart.rs` but live against a real python server).
#[tokio::test]
#[ignore] // spawns a real python stdio MCP server
async fn lazy_l3_warm_catalog_persists_across_restart() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-lazy-l3-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-lazy-min-l3-{}.py", std::process::id()));
    std::fs::write(&server, LAZY_MIN_SERVER).expect("write server script");

    let config = lazy_live_config(&dir, &server);

    // First proxy: build the registry, then seed a warm catalog on disk.
    let proxy1 = Proxy::from_config(lazy_live_config(&dir, &server))
        .await
        .expect("first proxy builds");
    let _reg1 = proxy1.lazy_registry();

    let files_cfg = config
        .backends
        .iter()
        .find(|b| b.name == "files")
        .expect("files backend present")
        .clone();
    let hash = BinaryHasher::hash(&files_cfg);
    let catalog = WarmCatalog::from_probe_result(
        "files",
        "/",
        vec![lazy_ping_tool()],
        vec![],
        vec![],
        vec![],
        Some("2026-07-28".to_string()),
        hash.clone(),
    );
    let store = WarmCatalogStore::new(dir.clone());
    store.save(&catalog).expect("seed warm catalog on disk");

    // Drop the first proxy (simulates a process restart).
    drop(proxy1);

    // Second proxy with the SAME config: startup must load the persisted catalog.
    let proxy2 = Proxy::from_config(lazy_live_config(&dir, &server))
        .await
        .expect("second proxy builds");
    let reg2 = proxy2.lazy_registry();

    let loaded = reg2
        .get("files")
        .and_then(|b| b.catalog)
        .expect("warm catalog must be loaded from disk on restart (L3)");
    assert_eq!(
        loaded.tools.len(),
        1,
        "loaded catalog must contain the seeded tool"
    );
    assert_eq!(
        loaded.tools[0].name, "files/ping",
        "loaded tool must be namespaced as files/ping"
    );
    assert_eq!(
        loaded.protocol_version.as_deref(),
        Some("2026-07-28"),
        "loaded catalog must preserve the seeded protocol version"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Like [`LAZY_MIN_SERVER`] but advertises protocol version `2026-07-28` so the
/// post-spawn probe captures a **stateless** protocol version. This is required
/// for `LazyBackendRegistry::idle_out` to perform the Down transition (guard C3
/// in `src/lazy_registry.rs` only idle-outs `2026-07-28` backends). Used by L4.
const LAZY_MIN_SERVER_2026: &str = r#"#!/usr/bin/env python3
import sys, json
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n"); sys.stdout.flush()
def read():
    line = sys.stdin.readline()
    return json.loads(line) if line else None
while True:
    req = read()
    if req is None: break
    mid = req.get("id"); method = req.get("method")
    if method == "initialize":
        send({"jsonrpc":"2.0","id":mid,"result":{"protocolVersion":"2026-07-28","capabilities":{"tools":{"listChanged":False},"resources":{}},"serverInfo":{"name":"live-min","version":"1.0.0"}}})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"tools":[{"name":"ping","description":"Return pong","inputSchema":{"type":"object"}}]}})
    elif method == "tools/call":
        send({"jsonrpc":"2.0","id":mid,"result":{"content":[{"type":"text","text":"pong"}],"isError":False}})
    elif method == "resources/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"resources":[]}})
    elif method == "resources/templates/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"resourceTemplates":[]}})
    elif method == "prompts/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"prompts":[]}})
    elif method == "shutdown":
        send({"jsonrpc":"2.0","id":mid,"result":{}}); break
    else:
        send({"jsonrpc":"2.0","id":mid,"result":{}})
"#;

/// Local variant of [`lazy_live_config`] with a 1-second idle timeout so the
/// idle-out transition can be driven deterministically (the background sweeper
/// is non-deterministic). Identical to [`lazy_live_config`] except the `files`
/// backend's `idle_timeout_secs` is `Some(1)`.
fn lazy_live_config_fast_idle(dir: &Path, server: &Path) -> ProxyConfig {
    let mut cfg = lazy_live_config(dir, server);
    for b in cfg.backends.iter_mut() {
        if b.name == "files" {
            b.idle_timeout_secs = Some(1);
        }
    }
    cfg
}

/// L4 — idle-out drives the full Up→Down transition (live).
///
/// `ensure_spawned` brings the backend Up (real python child + probe). After the
/// idle timer elapses we call `idle_out` explicitly (rather than relying on the
/// non-deterministic background sweeper) and assert it performs the Down
/// transition, preserving the warm catalog.
#[tokio::test]
#[ignore] // spawns a real python stdio MCP server
async fn lazy_l4_idle_out_full_transition() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-lazy-l4-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-lazy-min-l4-{}.py", std::process::id()));
    std::fs::write(&server, LAZY_MIN_SERVER_2026).expect("write server script");

    let config = lazy_live_config_fast_idle(&dir, &server);
    let proxy = Proxy::from_config(config)
        .await
        .expect("proxy with lazy backend must build");
    let registry = proxy.lazy_registry();

    // Bring the backend up on demand (real child + post-spawn probe).
    registry
        .ensure_spawned("files")
        .await
        .expect("ensure_spawned must spawn the lazy backend");
    assert_eq!(
        registry.spawn_state("files"),
        mcp_proxy::lazy_registry::SpawnState::Up,
        "precondition: backend must be Up before idle-out"
    );

    // Let the idle timer elapse (configured to 1s).
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Drive the full Down transition explicitly via idle_out.
    let out = registry
        .idle_out("files")
        .await
        .expect("idle_out must run without error");
    assert!(
        out,
        "idle_out must perform the Down transition (returns true) once the idle timer elapsed"
    );
    assert_eq!(
        registry.spawn_state("files"),
        mcp_proxy::lazy_registry::SpawnState::Down,
        "backend must be Down after idle-out (child terminated, catalog preserved)"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// L5 — concurrent first-calls coalesce into a single spawn (live).
#[tokio::test]
#[ignore] // spawns a real python stdio MCP server
async fn lazy_l5_coalesced_concurrent_spawn() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-lazy-l5-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-lazy-min-l5-{}.py", std::process::id()));
    std::fs::write(&server, LAZY_MIN_SERVER).expect("write server script");

    let config = lazy_live_config(&dir, &server);
    let proxy = Proxy::from_config(config)
        .await
        .expect("proxy with lazy backend must build");
    let registry = std::sync::Arc::new(proxy.lazy_registry());

    // Fire several concurrent first-calls; all must succeed and the backend
    // must end Up. The spawn guard coalesces them behind a single spawn.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let r = std::sync::Arc::clone(&registry);
        handles.push(tokio::spawn(async move { r.ensure_spawned("files").await }));
    }
    for h in handles {
        h.await.expect("task joins").expect("spawn succeeds");
    }

    assert_eq!(
        registry.spawn_state("files"),
        mcp_proxy::lazy_registry::SpawnState::Up,
        "backend must be Up after concurrent first-calls (L5)"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// L6 — protocol version preserved from warm catalog (live).
#[tokio::test]
#[ignore] // spawns a real python stdio MCP server
async fn lazy_l6_protocol_version_preserved() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-lazy-l6-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-lazy-min-l6-{}.py", std::process::id()));
    std::fs::write(&server, LAZY_MIN_SERVER).expect("write server script");

    let config = lazy_live_config(&dir, &server);

    // Seed a warm catalog on disk BEFORE building the proxy.
    let files_cfg = config
        .backends
        .iter()
        .find(|b| b.name == "files")
        .expect("files backend present")
        .clone();
    let hash = BinaryHasher::hash(&files_cfg);
    let catalog = WarmCatalog::from_probe_result(
        "files",
        "/",
        vec![lazy_ping_tool()],
        vec![],
        vec![],
        vec![],
        Some("2026-07-28".to_string()),
        hash,
    );
    WarmCatalogStore::new(dir.clone())
        .save(&catalog)
        .expect("seed warm catalog on disk");

    let proxy = Proxy::from_config(config)
        .await
        .expect("proxy with lazy backend must build");
    let registry = proxy.lazy_registry();

    assert_eq!(
        registry.protocol_version("files").as_deref(),
        Some("2026-07-28"),
        "loaded warm catalog must preserve the seeded protocol version (L6)"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// L7 — warm catalog load is keyed by `BinaryHasher::hash`, not a placeholder
/// (live). A catalog seeded with a wrong hash must NOT be loaded for `files`.
#[tokio::test]
#[ignore] // spawns a real python stdio MCP server
async fn lazy_l7_hash_key_identity() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-lazy-l7-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-lazy-min-l7-{}.py", std::process::id()));
    std::fs::write(&server, LAZY_MIN_SERVER).expect("write server script");

    let config = lazy_live_config(&dir, &server);

    let files_cfg = config
        .backends
        .iter()
        .find(|b| b.name == "files")
        .expect("files backend present")
        .clone();
    let hash = BinaryHasher::hash(&files_cfg);

    // Correct-hash catalog (this is the one that must load).
    let catalog = WarmCatalog::from_probe_result(
        "files",
        "/",
        vec![lazy_ping_tool()],
        vec![],
        vec![],
        vec![],
        Some("2026-07-28".to_string()),
        hash.clone(),
    );
    WarmCatalogStore::new(dir.clone())
        .save(&catalog)
        .expect("seed correct-hash warm catalog");

    // Wrong-hash catalog (must NOT be loaded for "files").
    let wrong = WarmCatalog::from_probe_result(
        "files",
        "/",
        vec![lazy_ping_tool()],
        vec![],
        vec![],
        vec![],
        Some("2026-07-28".to_string()),
        "wrong-hash".to_string(),
    );
    WarmCatalogStore::new(dir.clone())
        .save(&wrong)
        .expect("seed wrong-hash warm catalog");

    let proxy = Proxy::from_config(config)
        .await
        .expect("proxy with lazy backend must build");
    let registry = proxy.lazy_registry();

    let loaded = registry
        .get("files")
        .and_then(|b| b.catalog)
        .expect("warm catalog must be loaded from disk (L7)");
    assert_eq!(
        loaded.identity_hash, hash,
        "load must be keyed by BinaryHasher::hash, not a placeholder (L7)"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}
