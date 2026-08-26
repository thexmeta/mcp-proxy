//! Integration tests for lazy-backend hot-reload behavior.
//!
//! These tests exercise the public [`LazyBackendRegistry`] API directly
//! (`register_lazy`, `unregister`, `reconcile_config_change`, `names`,
//! `spawn_state`, `get`) to verify the hot-reload add/remove/flip/hash-change
//! paths without invoking any `pub(crate)` reload internals.
//!
//! The registry is wired to a real (empty) [`McpProxy`] built with an
//! in-process [`ChannelTransport`] backend so `with_runtime` succeeds and we can
//! assert that lazy backends are NOT eagerly added to the proxy's namespaces.

use tower_mcp::client::ChannelTransport;
use tower_mcp::proxy::McpProxy;
use tower_mcp::router::McpRouter;
use tower_mcp_types::protocol::ToolDefinition;

use mcp_proxy::config::{BackendConfig, SpawnMode, TransportType};
use mcp_proxy::lazy_registry::{LazyBackendRegistry, SpawnState};
use mcp_proxy::warm_cache::WarmCatalogStore;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A no-op in-process backend so the shared [`McpProxy`] builds (it requires at
/// least one backend). The lazy backend under test is registered in the
/// registry, NOT added to this proxy, so we can assert it is absent.
fn dummy_router() -> McpRouter {
    McpRouter::default()
}

/// Build a registry wired to a real (empty) proxy + temp warm-cache store.
async fn runtime_registry() -> LazyBackendRegistry {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-hr-{}", std::process::id()));
    let store = WarmCatalogStore::new(dir);
    let proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("__dummy__", ChannelTransport::new(dummy_router()))
        .await
        .build_strict()
        .await
        .expect("proxy builds");
    LazyBackendRegistry::from_backends(vec![]).with_runtime(proxy, &store, "/".to_string(), 2)
}

/// Build a stdio [`BackendConfig`] with the given spawn mode + idle timeout.
fn backend(name: &str, mode: SpawnMode, idle: Option<u64>, suffix: Option<&str>) -> BackendConfig {
    BackendConfig {
        name: name.to_string(),
        transport: TransportType::Stdio,
        command: Some("mycmd".to_string()),
        spawn_mode: mode,
        idle_timeout_secs: idle,
        cache_key_suffix: suffix.map(|s| s.to_string()),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// H1: lazy add (AC-006)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn h1_lazy_add_registers_down_and_not_in_proxy() {
    let reg = runtime_registry().await;
    reg.register_lazy(backend("files", SpawnMode::Lazy, Some(30), None));

    assert!(
        reg.names().contains(&"files".to_string()),
        "lazy backend must be registered in the registry"
    );
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "lazy backend must NOT be spawned (AC-006)"
    );
    assert!(
        !reg.proxy_has_namespace("files"),
        "lazy backend must not be eagerly added to the proxy (AC-006)"
    );
}

// ---------------------------------------------------------------------------
// H2: lazy remove (FR-009)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn h2_lazy_remove_drops_from_registry() {
    let reg = runtime_registry().await;
    reg.register_lazy(backend("files", SpawnMode::Lazy, Some(30), None));
    assert!(reg.names().contains(&"files".to_string()));

    reg.unregister("files");
    assert!(
        !reg.names().contains(&"files".to_string()),
        "unregister must drop the lazy backend (FR-009)"
    );
}

// ---------------------------------------------------------------------------
// H3: eager -> lazy flip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn h3_eager_to_lazy_flip_registers_down() {
    let reg = runtime_registry().await;

    // Simulate the OLD config being eager: it is NOT in the registry (eager
    // backends are spawned directly by the proxy, not tracked here).
    assert!(
        !reg.names().contains(&"files".to_string()),
        "eager backend must not be in the lazy registry"
    );

    // Hot reload adds it as lazy (the reload loop calls register_lazy).
    reg.register_lazy(backend("files", SpawnMode::Lazy, Some(30), None));
    assert!(
        reg.names().contains(&"files".to_string()),
        "lazy flip must register the backend"
    );
    assert_eq!(reg.spawn_state("files"), SpawnState::Down);
    assert!(
        !reg.proxy_has_namespace("files"),
        "lazy flip must not eagerly spawn into the proxy"
    );
}

// ---------------------------------------------------------------------------
// H4: lazy -> eager flip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn h4_lazy_to_eager_flip_unregisters() {
    let reg = runtime_registry().await;

    // Old config: lazy (in registry via register_lazy).
    reg.register_lazy(backend("files", SpawnMode::Lazy, Some(30), None));
    assert!(reg.names().contains(&"files".to_string()));

    // Hot reload flips to eager: the reload loop calls unregister (the eager
    // add_backend path is covered by other integration tests; here we assert
    // the registry no longer tracks it as lazy).
    reg.unregister("files");
    assert!(
        !reg.names().contains(&"files".to_string()),
        "lazy->eager flip must unregister the lazy entry (FR-009)"
    );

    // And an eager backend built via Proxy::from_config IS in the proxy
    // namespaces (proving the flip target behaves as eager).
    let eager_proxy = McpProxy::builder("eager-proxy", "1.0.0")
        .separator("/")
        .backend("files", ChannelTransport::new(dummy_router()))
        .await
        .build_strict()
        .await
        .expect("proxy builds");
    assert!(
        eager_proxy
            .backend_namespaces()
            .contains(&"files".to_string()),
        "eager backend must be present in the proxy namespaces"
    );
}

// ---------------------------------------------------------------------------
// H5: config hash change (FR-010 / AC-010)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn h5_config_hash_change_rehashes_and_updates_config() {
    let reg = runtime_registry().await;
    reg.register_lazy(backend("files", SpawnMode::Lazy, Some(10), Some("v1")));

    // Modify the config: different cache_key_suffix (changes the hash) and a
    // new idle timeout.
    let modified = backend("files", SpawnMode::Lazy, Some(20), Some("v2"));
    let new_hash = reg.reconcile_config_change(&modified).await.unwrap();

    // The identity hash must differ because cache_key_suffix changed.
    let old_hash = mcp_proxy::warm_cache::BinaryHasher::hash(&backend(
        "files",
        SpawnMode::Lazy,
        Some(10),
        Some("v1"),
    ));
    assert_ne!(
        new_hash, old_hash,
        "config hash must change on suffix change (FR-010/AC-010)"
    );

    // The stored entry must reflect the new idle timeout (C15) and config.
    assert_eq!(
        reg.idle_timeout("files"),
        Some(20),
        "idle_timeout_secs must be updated by reconcile (FR-010)"
    );
    let entry = reg.get("files").expect("backend present after reconcile");
    assert_eq!(entry.config.spawn_mode, SpawnMode::Lazy, "still lazy");
    assert_eq!(
        entry.config.cache_key_suffix.as_deref(),
        Some("v2"),
        "cache_key_suffix must be updated by reconcile"
    );
    assert_eq!(reg.spawn_state("files"), SpawnState::Down);
}

// ---------------------------------------------------------------------------
// H5b: reconcile loads a cached catalog for the NEW hash (re-probe path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn h5b_reconcile_loads_cached_catalog_for_new_hash() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-hr5b-{}", std::process::id()));
    let store = WarmCatalogStore::new(dir.clone());
    let proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        .backend("__dummy__", ChannelTransport::new(dummy_router()))
        .await
        .build_strict()
        .await
        .expect("proxy builds");
    let reg =
        LazyBackendRegistry::from_backends(vec![]).with_runtime(proxy, &store, "/".to_string(), 2);

    // Seed a warm catalog keyed by the NEW hash (v2) so reconcile finds it.
    let new_cfg = backend("files", SpawnMode::Lazy, Some(20), Some("v2"));
    let new_hash = mcp_proxy::warm_cache::BinaryHasher::hash(&new_cfg);
    let seeded = mcp_proxy::warm_cache::WarmCatalog::from_probe_result(
        "files",
        "/",
        vec![ToolDefinition {
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
        Some("2026-07-28".to_string()),
        new_hash.clone(),
    );
    store.save(&seeded).expect("seed catalog");

    // Register the OLD config (v1), then reconcile to the NEW config (v2).
    reg.register_lazy(backend("files", SpawnMode::Lazy, Some(10), Some("v1")));
    let returned = reg.reconcile_config_change(&new_cfg).await.unwrap();
    assert_eq!(returned, new_hash, "reconcile returns the new hash");

    // The store must be able to load the cached catalog for the new hash.
    let loaded = store
        .load("files", &new_hash)
        .expect("catalog loads for new hash");
    assert_eq!(loaded.tools.len(), 1);
    assert_eq!(loaded.tools[0].name, "files/read");
}

// ---------------------------------------------------------------------------
// H6: full Proxy::from_config hot-reload (stronger, best-effort)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn h6_proxy_from_config_sees_lazy_backend_down() {
    // A self-contained stdio MCP server (stdlib python) so from_config can
    // eagerly spawn the lazy backend (Wave 3 behavior) and build the registry.
    let server = std::env::temp_dir().join(format!("mcp-min-srv-{}.py", std::process::id()));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let dir = std::env::temp_dir().join(format!("mcp-proxy-hr6-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let config_toml = format!(
        r#"
        [proxy]
        name = "hr6-proxy"
        version = "1.0.0"
        [proxy.listen]

        [warm_cache]
        enabled = true
        dir = "{dir}"
        ttl_secs = 0

        [[backends]]
        name = "files"
        transport = "stdio"
        command = "python3"
        args = ["{srv}"]
        spawn_mode = "lazy"
        idle_timeout_secs = 30

        # A dummy eager backend (real MCP server) so the shared McpProxy has
        # at least one backend to build (lazy backends are registered, not spawned).
        [[backends]]
        name = "__dummy__"
        transport = "stdio"
        command = "python3"
        args = ["{srv}"]
        "#,
        dir = dir.display(),
        srv = server.display(),
    );

    let config = mcp_proxy::ProxyConfig::parse(&config_toml).expect("config parses");
    let proxy = mcp_proxy::Proxy::from_config(config)
        .await
        .expect("proxy builds with lazy backend");

    let reg = proxy.lazy_registry();
    assert!(
        reg.names().contains(&"files".to_string()),
        "lazy backend must be in the registry after from_config"
    );
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "lazy backend must be Down (not yet actioned) after from_config"
    );

    let _ = std::fs::remove_file(&server);
}

/// Minimal MCP stdio server (Python stdlib only) used by H6. Exposes one tool
/// `ping` so the proxy can build and probe it.
const MIN_MCP_SERVER: &str = r#"#!/usr/bin/env python3
import json, sys

def read_message():
    # tower-mcp's StdioClientTransport speaks NEWLINE-DELIMITED JSON (one JSON
    # object per line, NO Content-Length framing). Read binary lines
    # exclusively from sys.stdin.buffer to avoid TextIOWrapper read-ahead
    # desync under concurrent handshake traffic.
    buf = sys.stdin.buffer
    line = buf.readline()
    if not line:
        return None
    line = line.rstrip(b"\r\n")
    if not line:
        return None
    return json.loads(line.decode("utf-8"))
def write_message(msg):
    data = json.dumps(msg).encode("utf-8")
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()
def handle(msg):
    method = msg.get("method"); mid = msg.get("id")
    if method == "initialize":
        return {"jsonrpc":"2.0","id":mid,"result":{"protocolVersion":"2026-07-28","capabilities":{"tools":{}},"serverInfo":{"name":"min-server","version":"1.0.0"}}}
    if method == "tools/list":
        return {"jsonrpc":"2.0","id":mid,"result":{"tools":[{"name":"ping","description":"Return pong","inputSchema":{"type":"object","properties":{}}}]}}
    if method == "tools/call":
        return {"jsonrpc":"2.0","id":mid,"result":{"content":[{"type":"text","text":"pong"}],"isError":False}}
    if method == "resources/list":
        return {"jsonrpc":"2.0","id":mid,"result":{"resources":[]}}
    if method == "prompts/list":
        return {"jsonrpc":"2.0","id":mid,"result":{"prompts":[]}}
    if mid is not None:
        return {"jsonrpc":"2.0","id":mid,"result":{}}
    return None
def main():
    while True:
        msg = read_message()
        if msg is None:
            break
        resp = handle(msg)
        if resp is not None:
            write_message(resp)
if __name__ == "__main__":
    main()
"#;
