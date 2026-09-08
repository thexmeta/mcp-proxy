//! Integration tests for proxy-level warm-cache restart behavior.
//!
//! These tests prove that a warm catalog persisted to disk is reloaded into the
//! lazy backend registry when a second [`Proxy::from_config`] is constructed
//! with the same config (the Wave 1 bug-fix regression, integration-level).
//!
//! Because Wave 3 still eagerly spawns lazy backends inside `from_config`, the
//! lazy backend must point at a real, spawnable MCP stdio server. We use a
//! self-contained Python stdlib server (`MIN_MCP_SERVER`) so the test needs no
//! network or external packages.

use std::path::Path;

use tower_mcp_types::protocol::ToolDefinition;

use mcp_proxy::config::{BackendConfig, ProxyConfig, ProxySettings, SpawnMode, TransportType};
use mcp_proxy::warm_cache::{BinaryHasher, WarmCatalog, WarmCatalogStore};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a [`ProxyConfig`] in-code with warm cache enabled + one lazy stdio
/// backend pointing at the given python server script.
fn lazy_config(dir: &Path, server: &Path) -> ProxyConfig {
    ProxyConfig {
        proxy: ProxySettings {
            name: "restart-proxy".to_string(),
            version: "1.0.0".to_string(),
            separator: "/".to_string(),
            listen: mcp_proxy::config::ListenConfig {
                host: "127.0.0.1".to_string(),
                port: 0,
            },
            instructions: None,
            shutdown_timeout_seconds: 30,
            shutdown_kill_timeout_secs: 2,
            force_kill: false,
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
            default_spawn_mode: mcp_proxy::config::SpawnMode::Eager,
            default_idle_timeout_secs: None,
            protocol_support: mcp_proxy::config::ProtocolSupportConfig::default(),
            init_timeout: None,
        },
        backends: vec![
            // Dummy eager backend (real MCP server) so the shared McpProxy has
            // at least one backend to build (lazy backends are registered, not spawned).
            BackendConfig {
                name: "__dummy__".to_string(),
                enabled: true,
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                ..Default::default()
            },
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
        warm_cache: mcp_proxy::config::WarmCacheConfig {
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
fn ping_tool() -> ToolDefinition {
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

// ---------------------------------------------------------------------------
// R3: catalog survives restart (the bug-fix regression, integration-level)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn r3_warm_catalog_survives_proxy_restart() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-min-srv-r3-{}.py", std::process::id()));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let config = lazy_config(&dir, &server);

    // First proxy: build the registry, then seed a warm catalog on disk.
    let proxy = mcp_proxy::Proxy::from_config(lazy_config(&dir, &server))
        .await
        .expect("first proxy builds");
    let _reg = proxy.lazy_registry();

    let backend_cfg = config.backends[0].clone();
    let hash = BinaryHasher::hash(&backend_cfg);
    let catalog = WarmCatalog::from_probe_result(
        "files",
        "/",
        vec![ping_tool()],
        vec![],
        vec![],
        vec![],
        Some("2026-07-28".to_string()),
        hash.clone(),
    );
    let store = WarmCatalogStore::new(dir.clone());
    store.save(&catalog).expect("seed warm catalog");

    // Drop the first proxy (simulates a process restart).
    drop(proxy);

    // Second proxy with the SAME config: startup must load the persisted catalog.
    let proxy2 = mcp_proxy::Proxy::from_config(lazy_config(&dir, &server))
        .await
        .expect("second proxy builds");
    let reg2 = proxy2.lazy_registry();

    let loaded = reg2
        .get("files")
        .and_then(|b| b.catalog)
        .expect("warm catalog must be loaded from disk on restart (R3)");
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
}

// ---------------------------------------------------------------------------
// R-hash-key: saved file uses the REAL hash (not a "wave1" placeholder)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn r_hash_key_saved_file_uses_real_hash() {
    let dir = std::env::temp_dir().join(format!("mcp-proxy-restart-hash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let server = std::env::temp_dir().join(format!("mcp-min-srv-hash-{}.py", std::process::id()));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let config = lazy_config(&dir, &server);
    let backend_cfg = config.backends[0].clone();

    // The real identity hash for this backend (no "wave1" placeholder).
    let real_hash = BinaryHasher::hash(&backend_cfg);
    assert_ne!(
        real_hash, "wave1",
        "identity hash must be the real hash, never a 'wave1' placeholder"
    );

    // Seed a catalog keyed by the real hash and persist it.
    let catalog = WarmCatalog::from_probe_result(
        "files",
        "/",
        vec![ping_tool()],
        vec![],
        vec![],
        vec![],
        Some("2026-07-28".to_string()),
        real_hash.clone(),
    );
    let store = WarmCatalogStore::new(dir.clone());
    store.save(&catalog).expect("seed warm catalog");

    // The on-disk filename must contain the real hash, not "wave1".
    let expected_path = store.path_for("files", &real_hash);
    assert!(
        expected_path.exists(),
        "catalog file must exist at the real-hash path: {}",
        expected_path.display()
    );
    let file_name = expected_path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("filename");
    assert!(
        file_name.contains(&real_hash),
        "filename must contain the real hash '{real_hash}', got '{file_name}'"
    );
    assert!(
        !file_name.contains("wave1"),
        "filename must NOT contain the 'wave1' placeholder, got '{file_name}'"
    );

    // And the second proxy must load it via the real hash.
    let proxy2 = mcp_proxy::Proxy::from_config(lazy_config(&dir, &server))
        .await
        .expect("second proxy builds");
    let reg2 = proxy2.lazy_registry();
    let loaded = reg2.get("files").and_then(|b| b.catalog);
    assert!(
        loaded.is_some(),
        "warm catalog must load via the real hash on restart"
    );

    let _ = std::fs::remove_file(&server);
}

/// Minimal MCP stdio server (Python stdlib only). Exposes one tool `ping`.
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
