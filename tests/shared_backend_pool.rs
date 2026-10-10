//! Integration tests for the shared backend pool behavior.
//!
//! Verifies that:
//! - A single McpProxy is shared across endpoint groups (each backend spawns once)
//! - GroupFilterService restricts tool visibility per group
//! - Hot-reload adds backends to the shared proxy (visible in all groups)
//! - Same backend in multiple groups works correctly
//! - Canary/failover middleware works with shared proxy
//! - Endpoint group shorthand syntax works
//! - Global backend_env is passed to backends

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;

use schemars::JsonSchema;
use serde::Deserialize;
use tower::Service;

use tower_mcp::client::ChannelTransport;
use tower_mcp::protocol::{CallToolParams, McpRequest, McpResponse, RequestId, ToolDefinition};
use tower_mcp::proxy::McpProxy;
use tower_mcp::router::{Extensions, RouterRequest, RouterResponse};
use tower_mcp::{CallToolResult, McpRouter, ToolBuilder};

use mcp_proxy::config::ProxyConfig;
use mcp_proxy::filter::GroupFilterService;

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
// Helpers
// ---------------------------------------------------------------------------

async fn call(
    svc: &mut impl Service<RouterRequest, Response = RouterResponse, Error = Infallible>,
    request: McpRequest,
) -> RouterResponse {
    let req = RouterRequest {
        id: RequestId::Number(1),
        inner: request,
        extensions: Extensions::new(),
    };
    svc.call(req).await.expect("infallible")
}

fn tool_call(name: &str, args: serde_json::Value) -> McpRequest {
    McpRequest::CallTool(CallToolParams {
        name: name.to_string(),
        arguments: args,
        meta: None,
        task: None,
        input_responses: None,
        request_state: None,
    })
}

fn list_tools_names(resp: RouterResponse) -> Vec<String> {
    match resp.inner.unwrap() {
        McpResponse::ListTools(result) => result.tools.into_iter().map(|t| t.name).collect(),
        other => panic!("expected ListTools, got: {:?}", other),
    }
}

/// Build a shared McpProxy with math, text, and search backends.
async fn build_shared_proxy() -> McpProxy {
    McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("math", ChannelTransport::new(math_router()))
        .await
        .backend("text", ChannelTransport::new(text_router()))
        .await
        .backend("search", ChannelTransport::new(search_router()))
        .await
        .build_strict()
        .await
        .expect("shared proxy should build")
}

/// Wrap a cloned McpProxy with GroupFilterService for the given namespaces.
fn wrap_with_group_filter(proxy: McpProxy, namespaces: &[&str]) -> GroupFilterService<McpProxy> {
    let ns_set: HashSet<String> = namespaces.iter().map(|s| s.to_string()).collect();
    GroupFilterService::new(proxy, ns_set)
}

// ---------------------------------------------------------------------------
// Test 1: Single process per backend (shared proxy pattern)
// ---------------------------------------------------------------------------

/// Verify the shared proxy pattern: ONE McpProxy clone shared by multiple
/// endpoint groups. Both groups see tools from the same underlying proxy.
///
/// In production this means each stdio backend spawns exactly 1 OS process.
#[tokio::test]
async fn test_single_process_per_backend() {
    let shared = build_shared_proxy().await;

    // Clone the shared proxy for two endpoint groups (same as production does)
    let group_a_proxy = shared.clone();
    let group_b_proxy = shared.clone();

    // Both groups wrap with their own GroupFilterService
    let mut group_a = wrap_with_group_filter(group_a_proxy, &["math/", "text/"]);
    let mut group_b = wrap_with_group_filter(group_b_proxy, &["search/"]);

    // Group A should see math/ and text/ tools
    let resp_a = call(&mut group_a, McpRequest::ListTools(Default::default())).await;
    let tools_a = list_tools_names(resp_a);
    assert!(
        tools_a.contains(&"math/add".to_string()),
        "group A should have math/add: {:?}",
        tools_a
    );
    assert!(
        tools_a.contains(&"text/echo".to_string()),
        "group A should have text/echo: {:?}",
        tools_a
    );
    assert!(
        !tools_a.iter().any(|t| t.starts_with("search/")),
        "group A should NOT have search tools: {:?}",
        tools_a
    );

    // Group B should see only search/ tools
    let resp_b = call(&mut group_b, McpRequest::ListTools(Default::default())).await;
    let tools_b = list_tools_names(resp_b);
    assert!(
        tools_b.contains(&"search/web_search".to_string()),
        "group B should have search/web_search: {:?}",
        tools_b
    );
    assert!(
        !tools_b.iter().any(|t| t.starts_with("math/")),
        "group B should NOT have math tools: {:?}",
        tools_b
    );
    assert!(
        !tools_b.iter().any(|t| t.starts_with("text/")),
        "group B should NOT have text tools: {:?}",
        tools_b
    );

    // Both groups can call tools — proving the shared proxy is functional
    let resp_add = call(
        &mut group_a,
        tool_call("math/add", serde_json::json!({"a": 3, "b": 4})),
    )
    .await;
    match resp_add.inner.unwrap() {
        McpResponse::CallTool(result) => assert_eq!(result.all_text(), "7"),
        other => panic!("expected CallTool, got: {:?}", other),
    }

    let resp_search = call(
        &mut group_b,
        tool_call("search/web_search", serde_json::json!({"message": "rust"})),
    )
    .await;
    match resp_search.inner.unwrap() {
        McpResponse::CallTool(result) => assert!(result.all_text().contains("rust")),
        other => panic!("expected CallTool, got: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 2: Endpoint groups filter tools correctly
// ---------------------------------------------------------------------------

/// Verify that endpoint groups only show tools from their member backends.
#[tokio::test]
async fn test_endpoint_group_filters_tools() {
    let shared = build_shared_proxy().await;

    // Group "devtools" has math + text backends
    let mut devtools = wrap_with_group_filter(shared.clone(), &["math/", "text/"]);

    // Group "research" has search backend only
    let mut research = wrap_with_group_filter(shared.clone(), &["search/"]);

    // Group "devtools" should see math and text tools
    let resp = call(&mut devtools, McpRequest::ListTools(Default::default())).await;
    let names = list_tools_names(resp);
    assert_eq!(
        names.len(),
        2,
        "devtools should have exactly 2 tools: {:?}",
        names
    );
    assert!(names.contains(&"math/add".to_string()));
    assert!(names.contains(&"text/echo".to_string()));

    // Group "research" should see only search tools
    let resp = call(&mut research, McpRequest::ListTools(Default::default())).await;
    let names = list_tools_names(resp);
    assert_eq!(
        names.len(),
        1,
        "research should have exactly 1 tool: {:?}",
        names
    );
    assert!(names.contains(&"search/web_search".to_string()));

    // Calling a tool outside the group's namespace is rejected
    let resp = call(
        &mut devtools,
        tool_call("search/web_search", serde_json::json!({"message": "test"})),
    )
    .await;
    assert!(
        resp.inner.is_err(),
        "calling non-member tool should be rejected: {:?}",
        resp.inner
    );
    let err = resp.inner.unwrap_err();
    assert!(
        err.message.contains("not available in this endpoint group"),
        "error should mention endpoint group: {}",
        err.message
    );
}

// ---------------------------------------------------------------------------
// Test 3: Hot-reload adds backend to shared proxy (visible in all groups)
// ---------------------------------------------------------------------------

/// Verify that adding a backend dynamically to the shared proxy makes it
/// visible in all endpoint groups that include it.
#[tokio::test]
async fn test_hot_reload_shares_proxy() {
    let shared = build_shared_proxy().await;

    // Create a group that sees all backends
    let mut group_a = wrap_with_group_filter(shared.clone(), &["math/", "text/", "search/"]);
    let resp = call(&mut group_a, McpRequest::ListTools(Default::default())).await;
    assert_eq!(list_tools_names(resp).len(), 3, "should start with 3 tools");

    // Dynamically add a "utils" backend to the shared proxy
    let utils_router = McpRouter::new().server_info("utils-server", "1.0.0").tool(
        ToolBuilder::new("timestamp")
            .description("Get current timestamp")
            .handler(|_: tower_mcp::NoParams| async move {
                Ok(CallToolResult::text("1234567890".to_string()))
            })
            .build(),
    );
    let utils_transport = ChannelTransport::new(utils_router);
    shared
        .add_backend("utils", utils_transport)
        .await
        .expect("add utils backend via hot-reload");

    // Update group filters to include the new backend
    let mut group_a =
        wrap_with_group_filter(shared.clone(), &["math/", "text/", "search/", "utils/"]);
    let mut group_b =
        wrap_with_group_filter(shared.clone(), &["math/", "text/", "search/", "utils/"]);

    // Both groups should now see 4 tools (including utils/timestamp)
    let resp_a = call(&mut group_a, McpRequest::ListTools(Default::default())).await;
    let names_a = list_tools_names(resp_a);
    assert_eq!(
        names_a.len(),
        4,
        "group A should see 4 tools after hot-reload: {:?}",
        names_a
    );
    assert!(
        names_a.contains(&"utils/timestamp".to_string()),
        "group A should see utils/timestamp: {:?}",
        names_a
    );

    let resp_b = call(&mut group_b, McpRequest::ListTools(Default::default())).await;
    let names_b = list_tools_names(resp_b);
    assert_eq!(
        names_b.len(),
        4,
        "group B should see 4 tools after hot-reload: {:?}",
        names_b
    );
    assert!(
        names_b.contains(&"utils/timestamp".to_string()),
        "group B should see utils/timestamp: {:?}",
        names_b
    );

    // Both groups can call the new tool
    let resp_a_call = call(
        &mut group_a,
        tool_call("utils/timestamp", serde_json::json!({})),
    )
    .await;
    match resp_a_call.inner.unwrap() {
        McpResponse::CallTool(result) => assert_eq!(result.all_text(), "1234567890"),
        other => panic!("expected CallTool, got: {:?}", other),
    }

    let resp_b_call = call(
        &mut group_b,
        tool_call("utils/timestamp", serde_json::json!({})),
    )
    .await;
    match resp_b_call.inner.unwrap() {
        McpResponse::CallTool(result) => assert_eq!(result.all_text(), "1234567890"),
        other => panic!("expected CallTool, got: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 4: Multiple groups share the same backend
// ---------------------------------------------------------------------------

/// Verify that the same backend in 2+ groups works correctly — both can call
/// tools and get correct results from the shared proxy.
#[tokio::test]
async fn test_multiple_groups_share_backend() {
    let shared = build_shared_proxy().await;

    // Group "math-only" has only math backend
    let mut math_only = wrap_with_group_filter(shared.clone(), &["math/"]);

    // Group "math-and-search" has math + search
    let mut math_and_search = wrap_with_group_filter(shared.clone(), &["math/", "search/"]);

    // Both groups see math/add
    let resp_math = call(
        &mut math_only,
        tool_call("math/add", serde_json::json!({"a": 10, "b": 20})),
    )
    .await;
    match resp_math.inner.unwrap() {
        McpResponse::CallTool(result) => assert_eq!(result.all_text(), "30"),
        other => panic!("expected CallTool, got: {:?}", other),
    }

    let resp_shared = call(
        &mut math_and_search,
        tool_call("math/add", serde_json::json!({"a": 10, "b": 20})),
    )
    .await;
    match resp_shared.inner.unwrap() {
        McpResponse::CallTool(result) => assert_eq!(result.all_text(), "30"),
        other => panic!("expected CallTool, got: {:?}", other),
    }

    // math-and-search can also call search, but math-only cannot
    let resp_search = call(
        &mut math_and_search,
        tool_call("search/web_search", serde_json::json!({"message": "hello"})),
    )
    .await;
    match resp_search.inner.unwrap() {
        McpResponse::CallTool(result) => assert!(result.all_text().contains("hello")),
        other => panic!("expected CallTool, got: {:?}", other),
    }

    let resp_denied = call(
        &mut math_only,
        tool_call("search/web_search", serde_json::json!({"message": "hello"})),
    )
    .await;
    assert!(
        resp_denied.inner.is_err(),
        "math-only group should not access search tools"
    );

    // Verify ListTools counts are correct
    let resp = call(&mut math_only, McpRequest::ListTools(Default::default())).await;
    assert_eq!(
        list_tools_names(resp).len(),
        1,
        "math-only should have 1 tool"
    );

    let resp = call(
        &mut math_and_search,
        McpRequest::ListTools(Default::default()),
    )
    .await;
    assert_eq!(
        list_tools_names(resp).len(),
        2,
        "math-and-search should have 2 tools"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Per-group middleware (canary) with shared proxy
// ---------------------------------------------------------------------------

/// Verify that canary routing works correctly when applied to a group
/// that uses the shared proxy. The canary redirects calls between primary
/// and canary backends within the group scope.
#[tokio::test]
async fn test_per_group_middleware_canary() {
    use mcp_proxy::canary::CanaryService;

    // Build shared proxy with primary and canary backends
    let primary_router = McpRouter::new().server_info("primary", "1.0.0").tool(
        ToolBuilder::new("process")
            .description("Process via primary")
            .handler(|_: tower_mcp::NoParams| async move { Ok(CallToolResult::text("primary:ok")) })
            .build(),
    );

    let canary_router = McpRouter::new().server_info("canary", "1.0.0").tool(
        ToolBuilder::new("process")
            .description("Process via canary")
            .handler(|_: tower_mcp::NoParams| async move { Ok(CallToolResult::text("canary:ok")) })
            .build(),
    );

    let proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("api", ChannelTransport::new(primary_router))
        .await
        .backend("api-canary", ChannelTransport::new(canary_router))
        .await
        .build_strict()
        .await
        .expect("proxy should build");

    // Group with both primary and canary
    let group_proxy = wrap_with_group_filter(proxy.clone(), &["api/", "api-canary/"]);

    // Canary mapping: api -> api-canary (100% to canary for deterministic testing)
    let canary_mappings: HashMap<String, (String, u32, u32)> =
        HashMap::from([("api".to_string(), ("api-canary".to_string(), 0, 100))]);
    let mut canary_svc = CanaryService::new(group_proxy, canary_mappings, "/");

    // ListTools should only show primary's tools (canary tools are hidden by naming convention)
    let resp = call(&mut canary_svc, McpRequest::ListTools(Default::default())).await;
    let names = list_tools_names(resp);
    assert!(
        names.contains(&"api/process".to_string()),
        "should list primary tool: {:?}",
        names
    );
    // api-canary/process should NOT appear since canary tools use same name
    // and the canary middleware handles routing

    // Call api/process with 100% canary weight — should get canary response
    let resp = call(
        &mut canary_svc,
        tool_call("api/process", serde_json::json!({})),
    )
    .await;
    match resp.inner.unwrap() {
        McpResponse::CallTool(result) => {
            assert_eq!(
                result.all_text(),
                "canary:ok",
                "with 100% canary weight, should route to canary"
            );
        }
        other => panic!("expected CallTool, got: {:?}", other),
    }
}

/// Verify failover routing works with the shared proxy pattern.
///
/// FailoverService triggers on `Err(JsonRpcError)` at the JSON-RPC level,
/// NOT on `Ok(CallToolResult::error(...))`. So we use a custom mock that
/// returns transport-level errors for the primary backend.
#[tokio::test]
async fn test_per_group_middleware_failover() {
    use mcp_proxy::failover::FailoverService;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tower_mcp::protocol::ListToolsResult;

    /// Mock service: returns Err for primary/backend tools, Ok for backup.
    #[derive(Clone)]
    struct FailPrimaryMock;

    impl tower::Service<RouterRequest> for FailPrimaryMock {
        type Response = RouterResponse;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<RouterResponse, Infallible>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: RouterRequest) -> Self::Future {
            let id = req.id.clone();
            Box::pin(async move {
                let inner = match &req.inner {
                    McpRequest::ListTools(_) => Ok(McpResponse::ListTools(ListToolsResult {
                        tools: vec![
                            ToolDefinition {
                                name: "api/process".to_string(),
                                title: None,
                                description: Some("Process".to_string()),
                                input_schema: serde_json::json!({"type": "object"}),
                                output_schema: None,
                                icons: None,
                                annotations: None,
                                execution: None,
                                meta: None,
                            },
                            ToolDefinition {
                                name: "api-backup/process".to_string(),
                                title: None,
                                description: Some("Backup process".to_string()),
                                input_schema: serde_json::json!({"type": "object"}),
                                output_schema: None,
                                icons: None,
                                annotations: None,
                                execution: None,
                                meta: None,
                            },
                        ],
                        next_cursor: None,
                        ttl_ms: None,
                        cache_scope: None,
                        meta: None,
                    })),
                    McpRequest::CallTool(p) if p.name.starts_with("api/") => {
                        Err(tower_mcp_types::JsonRpcError {
                            code: -32603,
                            message: "primary backend unavailable".to_string(),
                            data: None,
                        })
                    }
                    McpRequest::CallTool(p) if p.name.starts_with("api-backup/") => {
                        Ok(McpResponse::CallTool(CallToolResult::text("backup:ok")))
                    }
                    _ => Ok(McpResponse::Pong(Default::default())),
                };
                Ok(RouterResponse { id, inner })
            })
        }
    }

    let failover_mappings: HashMap<String, Vec<String>> =
        HashMap::from([("api".to_string(), vec!["api-backup".to_string()])]);
    let mut failover_svc = FailoverService::new(FailPrimaryMock, failover_mappings, "/");

    // Call api/process — primary returns Err, should fall over to backup
    let resp = call(
        &mut failover_svc,
        tool_call("api/process", serde_json::json!({})),
    )
    .await;
    match resp.inner.unwrap() {
        McpResponse::CallTool(result) => {
            assert_eq!(
                result.all_text(),
                "backup:ok",
                "should failover to backup when primary fails"
            );
        }
        other => panic!("expected CallTool, got: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 6: Endpoint group shorthand syntax
// ---------------------------------------------------------------------------

/// Verify that `endpoint_group_list` shorthand expands correctly and the
/// resulting config produces endpoint groups that work with the shared proxy.
#[test]
fn test_shorthand_endpoint_groups() {
    let toml = r#"
        [proxy]
        name = "test-proxy"
        endpoint_group_list = ["os", "web"]
        [proxy.listen]

        [[backends]]
        name = "files"
        transport = "stdio"
        command = "echo"

        [[backends]]
        name = "browser"
        transport = "http"
        url = "http://localhost:9222"
    "#;

    let config = ProxyConfig::parse(toml).unwrap();

    // Shorthand should create 2 endpoint groups
    assert_eq!(config.proxy.endpoint_groups.len(), 2);

    let os_group = config
        .proxy
        .endpoint_groups
        .iter()
        .find(|g| g.name == "os")
        .unwrap();
    assert_eq!(os_group.path, "/os");
    // Shorthand groups have empty backends — membership is via reverse references
    assert!(os_group.backends.is_empty());
    assert_eq!(
        os_group.description.as_deref(),
        Some("Auto-generated endpoint group from shorthand")
    );

    let web_group = config
        .proxy
        .endpoint_groups
        .iter()
        .find(|g| g.name == "web")
        .unwrap();
    assert_eq!(web_group.path, "/web");
    assert!(web_group.backends.is_empty());
}

/// Shorthand groups can be overridden by explicit endpoint_groups.
#[test]
fn test_shorthand_endpoint_groups_override() {
    let toml = r#"
        [proxy]
        name = "test-proxy"
        endpoint_group_list = ["os", "web"]
        [proxy.listen]

        [[proxy.endpoint_groups]]
        name = "os"
        path = "/custom-os"
        backends = ["files"]
        description = "Custom OS group"

        [[backends]]
        name = "files"
        transport = "stdio"
        command = "echo"

        [[backends]]
        name = "browser"
        transport = "http"
        url = "http://localhost:9222"
    "#;

    let config = ProxyConfig::parse(toml).unwrap();

    // "os" should be overridden by the explicit entry
    let os_group = config
        .proxy
        .endpoint_groups
        .iter()
        .find(|g| g.name == "os")
        .unwrap();
    assert_eq!(os_group.path, "/custom-os");
    assert_eq!(os_group.backends, vec!["files"]);
    assert_eq!(os_group.description.as_deref(), Some("Custom OS group"));

    // "web" should be auto-generated from shorthand (empty backends)
    let web_group = config
        .proxy
        .endpoint_groups
        .iter()
        .find(|g| g.name == "web")
        .unwrap();
    assert_eq!(web_group.path, "/web");
    assert!(web_group.backends.is_empty());
}

// ---------------------------------------------------------------------------
// Test 7: Global backend_env
// ---------------------------------------------------------------------------

/// Verify that `[proxy.backend_env]` values are merged into each backend's
/// env map via `apply_global_defaults()`, with per-backend values taking
/// precedence.
#[test]
fn test_global_backend_env() {
    let toml = r#"
        [proxy]
        name = "test-proxy"
        [proxy.listen]

        [proxy.backend_env]
        LOG_LEVEL = "ERROR"
        MCP_LOG_LEVEL = "ERROR"
        SHARED_VAR = "global"

        [[backends]]
        name = "api"
        transport = "stdio"
        command = "echo"
        [backends.env]
        LOG_LEVEL = "DEBUG"
        LOCAL_VAR = "local"
    "#;

    let mut config = ProxyConfig::parse(toml).unwrap();
    config.apply_global_defaults();

    let api_backend = config.backends.iter().find(|b| b.name == "api").unwrap();

    // Global vars should be present
    assert_eq!(api_backend.env.get("MCP_LOG_LEVEL").unwrap(), "ERROR");
    assert_eq!(api_backend.env.get("SHARED_VAR").unwrap(), "global");

    // Per-backend should override global
    assert_eq!(
        api_backend.env.get("LOG_LEVEL").unwrap(),
        "DEBUG",
        "per-backend env should override global"
    );

    // Per-backend-only vars should be preserved
    assert_eq!(api_backend.env.get("LOCAL_VAR").unwrap(), "local");
}

/// Verify that global backend_env is also merged into backends that have
/// no per-backend env vars.
#[test]
fn test_global_backend_env_applied_to_all_backends() {
    let toml = r#"
        [proxy]
        name = "test-proxy"
        [proxy.listen]

        [proxy.backend_env]
        LOG_LEVEL = "ERROR"
        API_KEY = "secret"

        [[backends]]
        name = "api"
        transport = "stdio"
        command = "echo"

        [[backends]]
        name = "db"
        transport = "http"
        url = "http://localhost:5432"
        [backends.env]
        DB_HOST = "localhost"
    "#;

    let mut config = ProxyConfig::parse(toml).unwrap();
    config.apply_global_defaults();

    // api backend should get both global vars
    let api = config.backends.iter().find(|b| b.name == "api").unwrap();
    assert_eq!(api.env.get("LOG_LEVEL").unwrap(), "ERROR");
    assert_eq!(api.env.get("API_KEY").unwrap(), "secret");

    // db backend should get global vars + its own
    let db = config.backends.iter().find(|b| b.name == "db").unwrap();
    assert_eq!(db.env.get("LOG_LEVEL").unwrap(), "ERROR");
    assert_eq!(db.env.get("API_KEY").unwrap(), "secret");
    assert_eq!(db.env.get("DB_HOST").unwrap(), "localhost");
}

/// Global middleware defaults (timeout, circuit_breaker, retry) are merged
/// into backends that don't specify their own.
#[test]
fn test_global_middleware_defaults_merged() {
    // TimeoutConfig, CircuitBreakerConfig, RetryConfig are tested via ProxyConfig::parse

    let toml = r#"
        [proxy]
        name = "test-proxy"
        [proxy.listen]
        port = 9090

        [proxy.timeout]
        seconds = 60

        [proxy.retry]
        max_retries = 3
        initial_backoff_ms = 100
        max_backoff_ms = 5000
        budget_percent = 20.0

        [[backends]]
        name = "api"
        transport = "stdio"
        command = "echo"

        [[backends]]
        name = "fast"
        transport = "http"
        url = "http://localhost:8080"
        [backends.timeout]
        seconds = 5
    "#;

    let mut config = ProxyConfig::parse(toml).unwrap();
    config.apply_global_defaults();

    // api backend should get global timeout
    let api = config.backends.iter().find(|b| b.name == "api").unwrap();
    assert_eq!(api.timeout.as_ref().unwrap().seconds, 60);
    assert!(api.retry.is_some(), "api should get global retry");

    // fast backend should keep its own timeout (per-backend overrides)
    let fast = config.backends.iter().find(|b| b.name == "fast").unwrap();
    assert_eq!(
        fast.timeout.as_ref().unwrap().seconds,
        5,
        "per-backend timeout should override global"
    );
}

// ---------------------------------------------------------------------------
// Test 8: Shared proxy with all 3 backends visible (no filter)
// ---------------------------------------------------------------------------

/// Verify that the shared proxy without any group filter shows ALL tools
/// from ALL backends — this is the default /mcp endpoint behavior.
#[tokio::test]
async fn test_shared_proxy_no_filter_shows_all_tools() {
    let shared = build_shared_proxy().await;
    let mut proxy = shared.clone();

    let resp = call(&mut proxy, McpRequest::ListTools(Default::default())).await;
    let names = list_tools_names(resp);
    assert_eq!(
        names.len(),
        3,
        "unfiltered proxy should show all 3 tools: {:?}",
        names
    );
    assert!(names.contains(&"math/add".to_string()));
    assert!(names.contains(&"text/echo".to_string()));
    assert!(names.contains(&"search/web_search".to_string()));
}

// ---------------------------------------------------------------------------
// Test 9: Group filter with empty namespace set hides everything
// ---------------------------------------------------------------------------

/// Verify that a group with no allowed namespaces hides all tools.
#[tokio::test]
async fn test_group_filter_empty_namespaces_hides_all() {
    let shared = build_shared_proxy().await;
    let mut empty_group = wrap_with_group_filter(shared.clone(), &[]);

    let resp = call(&mut empty_group, McpRequest::ListTools(Default::default())).await;
    let names = list_tools_names(resp);
    assert!(
        names.is_empty(),
        "empty group should see no tools: {:?}",
        names
    );

    // Calling any tool should be rejected
    let resp = call(
        &mut empty_group,
        tool_call("math/add", serde_json::json!({"a": 1, "b": 2})),
    )
    .await;
    assert!(resp.inner.is_err(), "empty group should reject all calls");
}

// ---------------------------------------------------------------------------
// Test 10: Shared proxy — adding backend without updating group filter
// ---------------------------------------------------------------------------

/// Verify that adding a backend to the shared proxy but NOT updating the
/// group filter means the new backend is invisible to existing groups.
#[tokio::test]
async fn test_shared_proxy_new_backend_invisible_without_filter_update() {
    let shared = build_shared_proxy().await;

    // Group only includes math and text
    let mut group = wrap_with_group_filter(shared.clone(), &["math/", "text/"]);

    let resp = call(&mut group, McpRequest::ListTools(Default::default())).await;
    assert_eq!(list_tools_names(resp).len(), 2, "should start with 2 tools");

    // Add a new "extra" backend to the shared proxy
    let extra_router = McpRouter::new().server_info("extra", "1.0.0").tool(
        ToolBuilder::new("ping")
            .description("Ping")
            .handler(|_: tower_mcp::NoParams| async move {
                Ok(CallToolResult::text("pong".to_string()))
            })
            .build(),
    );
    shared
        .add_backend("extra", ChannelTransport::new(extra_router))
        .await
        .expect("add extra backend");

    // Group filter NOT updated — still only math/text
    let resp = call(&mut group, McpRequest::ListTools(Default::default())).await;
    let names = list_tools_names(resp);
    assert_eq!(
        names.len(),
        2,
        "group should still see only 2 tools: {:?}",
        names
    );
    assert!(
        !names.iter().any(|t| t.starts_with("extra/")),
        "extra/ should be invisible: {:?}",
        names
    );

    // Call extra/ping should fail (not in allowed namespaces)
    let resp = call(&mut group, tool_call("extra/ping", serde_json::json!({}))).await;
    assert!(
        resp.inner.is_err(),
        "extra/ping should be rejected by group filter"
    );
}
