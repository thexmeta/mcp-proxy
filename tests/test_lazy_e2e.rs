//! End-to-end tests for the lazy-backend full pipeline.
//!
//! These tests exercise the complete lazy lifecycle through a real
//! [`Proxy::from_config`] build (matching production wiring), using an
//! in-process python stdio MCP server as the lazy backend so the suite needs
//! no network or external packages:
//!
//! - cached tools are visible at startup (served from the warm catalog while
//!   the backend is `Down`),
//! - the first action request spawns the backend on-demand (coalesced),
//! - subsequent calls hit the live backend,
//! - idle-out is a safe no-op on a `Down` backend (the full Down→Up→Down
//!   transition with the stateless protocol + expired timer is covered at the
//!   unit level in `src/lazy_registry.rs`),
//! - concurrent first-calls coalesce into a single spawn,
//! - hot-reload add/remove and protocol-version preservation.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use mcp_proxy::config::{BackendConfig, ProxyConfig, ProxySettings, SpawnMode, TransportType};
use mcp_proxy::lazy_registry::SpawnState;
use mcp_proxy::warm_cache::{BinaryHasher, WarmCatalog, WarmCatalogStore};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a [`ProxyConfig`] in-code with warm cache enabled + one lazy stdio
/// backend (`files`) and one dummy eager backend (`__dummy__`), both pointing at
/// the given python server script. The dummy eager backend lets the shared
/// [`McpProxy`] build (it requires ≥1 backend); the lazy backend is registered,
/// not spawned, at startup.
fn lazy_config(dir: &Path, server: &Path) -> ProxyConfig {
    ProxyConfig {
        proxy: ProxySettings {
            name: "e2e-proxy".to_string(),
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
            // spawned). `enabled: true` is REQUIRED — the `Default` derive sets
            // it to `false`, which would skip the backend entirely.
            BackendConfig {
                name: "__dummy__".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                enabled: true,
                ..Default::default()
            },
            // Lazy backend under test.
            BackendConfig {
                name: "files".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                spawn_mode: SpawnMode::Lazy,
                idle_timeout_secs: Some(30),
                enabled: true,
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

/// Unique per-test cache dir. Tests run concurrently under `cargo test`, so a
/// shared dir would let one test's `remove_dir_all` wipe another's seeded warm
/// catalog. Each test gets its own isolated directory.
static TEST_SEQ: AtomicU64 = AtomicU64::new(0);
fn test_dir(tag: &str) -> std::path::PathBuf {
    let seq = TEST_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "mcp-proxy-e2e-{}-{}-{}",
        std::process::id(),
        tag,
        seq
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A namespaced `ping` tool definition for the seeded warm catalog.
fn ping_tool() -> tower_mcp_types::protocol::ToolDefinition {
    tower_mcp_types::protocol::ToolDefinition {
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

/// Seed a warm catalog on disk for `files` keyed by the real backend hash.
fn seed_warm_catalog(dir: &Path, cfg: &BackendConfig, version: &str) {
    let store = WarmCatalogStore::new(dir.to_path_buf());
    let hash = BinaryHasher::hash(cfg);
    let catalog = WarmCatalog::from_probe_result(
        "files",
        "/",
        vec![ping_tool()],
        vec![],
        vec![],
        vec![],
        Some(version.to_string()),
        hash,
    );
    store.save(&catalog).expect("seed warm catalog");
}

// ---------------------------------------------------------------------------
// E1: cached tools visible at startup (backend Down)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e1_cached_tools_visible_at_startup_down() {
    let dir = test_dir("e1");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-e1-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    // Seed the warm catalog BEFORE building the proxy so startup loads it.
    let cfg = lazy_config(&dir, &server);
    let files_cfg = cfg.backends[1].clone();
    seed_warm_catalog(&dir, &files_cfg, "2026-07-28");

    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();

    // The backend must be Down (not spawned) but its cached tool is visible.
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "lazy backend must be Down at startup (E1)"
    );
    let loaded = reg
        .get("files")
        .and_then(|b| b.catalog)
        .expect("warm catalog must be loaded from disk at startup (E1)");
    assert_eq!(
        loaded.tools.len(),
        1,
        "cached catalog must contain one tool"
    );
    assert_eq!(
        loaded.tools[0].name, "files/ping",
        "cached tool must be namespaced as files/ping (E1)"
    );
    assert!(
        !reg.proxy_has_namespace("files"),
        "lazy backend must not be eagerly added to the proxy (E1)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// E2: first call spawns on-demand (Up)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2_first_call_spawns_on_demand() {
    let dir = test_dir("e2");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-e2-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let cfg = lazy_config(&dir, &server);
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();

    // Before any action, the backend is Down.
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "precondition (E2)"
    );

    // The first action request brings it Up (coalesced spawn of the real python
    // child + post-spawn probe).
    reg.ensure_spawned("files").await.expect("spawn succeeds");

    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Up,
        "backend must be Up after the first action request (E2)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// E3: subsequent calls hit the live backend (stays Up)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e3_subsequent_calls_hit_live_backend() {
    let dir = test_dir("e3");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-e3-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let cfg = lazy_config(&dir, &server);
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();

    reg.ensure_spawned("files").await.expect("first spawn");
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Up,
        "first spawn Up (E3)"
    );

    // A second action request must NOT re-spawn (idempotent fast-path).
    reg.ensure_spawned("files")
        .await
        .expect("second spawn is no-op");
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Up,
        "backend stays Up after subsequent calls (E3)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// E4: idle-out is a safe no-op on a Down backend
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e4_idle_out_is_safe_noop_on_down_backend() {
    // The full idle-out → Down transition (with stateless 2026-07-28 protocol
    // + expired idle timer) is covered at the unit level in
    // `src/lazy_registry.rs` (which has access to the `#[cfg(test)]` hooks).
    // Here we assert the public-API guard: idle_out on a Down backend is a safe
    // no-op that returns `Ok(false)` and leaves the backend Down.
    let dir = test_dir("e4");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-e4-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let cfg = lazy_config(&dir, &server);
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();

    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "precondition: backend is Down (E4)"
    );

    let out = reg.idle_out("files").await.expect("idle_out runs");
    assert!(
        !out,
        "idle_out on a Down backend must be a no-op (returns false) (E4)"
    );
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "backend must remain Down after a no-op idle-out (E4)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// E5: concurrent first calls coalesce into a single spawn
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e5_concurrent_first_calls_coalesce() {
    let dir = test_dir("e5");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-e5-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let cfg = lazy_config(&dir, &server);
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = std::sync::Arc::new(proxy.lazy_registry());

    // Fire several concurrent first-calls; all must succeed and the backend
    // must end Up. The spawn guard coalesces them behind a single spawn.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let r = std::sync::Arc::clone(&reg);
        handles.push(tokio::spawn(async move { r.ensure_spawned("files").await }));
    }
    for h in handles {
        h.await.expect("task joins").expect("spawn succeeds");
    }

    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Up,
        "backend must be Up after concurrent first-calls (E5)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// E6: hot-reload add lazy backend (not in proxy namespaces)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e6_hot_reload_add_lazy_not_in_proxy() {
    let dir = test_dir("e6");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-e6-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let cfg = lazy_config(&dir, &server);
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();

    // Hot-reload add a SECOND lazy backend at runtime.
    assert!(
        !reg.names().contains(&"docs".to_string()),
        "precondition: no 'docs' backend yet (E6)"
    );

    let docs = BackendConfig {
        name: "docs".to_string(),
        transport: TransportType::Stdio,
        command: Some("python3".to_string()),
        args: vec![server.to_string_lossy().to_string()],
        spawn_mode: SpawnMode::Lazy,
        idle_timeout_secs: Some(30),
        enabled: true,
        ..Default::default()
    };
    reg.register_lazy(docs);

    assert!(
        reg.names().contains(&"docs".to_string()),
        "hot-reload add must register the lazy backend (E6)"
    );
    assert_eq!(reg.spawn_state("docs"), SpawnState::Down, "added Down (E6)");
    assert!(
        !reg.proxy_has_namespace("docs"),
        "hot-reload add must not eagerly spawn into the proxy (E6)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// E7: protocol version preserved from warm catalog
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e7_protocol_version_preserved() {
    let dir = test_dir("e7");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-e7-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let cfg = lazy_config(&dir, &server);
    let files_cfg = cfg.backends[1].clone();
    seed_warm_catalog(&dir, &files_cfg, "2026-07-28");

    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();

    assert_eq!(
        reg.protocol_version("files").as_deref(),
        Some("2026-07-28"),
        "loaded warm catalog must preserve the seeded protocol version (E7)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// E8: hot-reload remove drops the lazy backend
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e8_hot_reload_remove_drops_backend() {
    let dir = test_dir("e8");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-e8-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let cfg = lazy_config(&dir, &server);
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();

    assert!(
        reg.names().contains(&"files".to_string()),
        "precondition: 'files' backend present (E8)"
    );

    reg.unregister("files");
    assert!(
        !reg.names().contains(&"files".to_string()),
        "hot-reload remove must drop the lazy backend (E8)"
    );

    let _ = std::fs::remove_file(&server);
}

/// Minimal MCP stdio server (Python stdlib only). Uses NEWLINE-DELIMITED JSON
/// (each JSON-RPC message on its own line) — NOT LSP `Content-Length` framing —
/// because tower-mcp's `StdioClientTransport` speaks newline-delimited JSON.
/// Exposes one tool `ping` so the proxy can build, probe, and spawn it.
const MIN_MCP_SERVER: &str = r#"#!/usr/bin/env python3
import sys, json
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
def read():
    line = sys.stdin.readline()
    return json.loads(line) if line else None
while True:
    req = read()
    if req is None:
        break
    mid = req.get("id")
    method = req.get("method")
    if method == "initialize":
        send({"jsonrpc":"2.0","id":mid,"result":{"protocolVersion":"2026-07-28","capabilities":{"tools":{}},"serverInfo":{"name":"min-server","version":"1.0.0"}}})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"tools":[{"name":"ping","description":"Return pong","inputSchema":{"type":"object"}}]}})
    elif method == "tools/call":
        send({"jsonrpc":"2.0","id":mid,"result":{"content":[{"type":"text","text":"pong"}],"isError":False}})
    elif method == "resources/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"resources":[]}})
    elif method == "prompts/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"prompts":[]}})
    elif method == "shutdown":
        send({"jsonrpc":"2.0","id":mid,"result":{}})
        break
    else:
        send({"jsonrpc":"2.0","id":mid,"result":{}})
"#;
