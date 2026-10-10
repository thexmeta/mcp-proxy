//! Child-lifecycle integration tests (plan `20261006-child-lifecycle`, task
//! `lifecycle-tests`).
//!
//! Covers, with short-lived stdio fixtures only (never the real CBM binary):
//!
//! - restart kills proxy-owned stdio children under BOTH kill modes
//!   (`force_kill=false` graceful, `force_kill=true` immediate) and reaps them;
//! - hot-reload removal terminates the old child (`remove_backend` drops the
//!   transport, which kills the child per its spawn-baked kill mode);
//! - a conflict-failed backend is absent from `tools/list` while `CallTool`
//!   returns the actionable JSON-RPC `invalid_params` error (`data.backend`,
//!   `data.conflict:true`, `data.hint` with stop hint); a restart-invalidated
//!   backend re-probes (its stale catalog file is gone, startup re-probes
//!   fresh).
//!
//! Liveness is observed by side effect (proxy namespaces / registry state /
//! catalog files), never by arbitrary-PID kill assertions. Child-exit waits
//! poll with a deadline (<10s), never a fixed sleep.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mcp_proxy::config::{BackendConfig, ProxyConfig, ProxySettings, SpawnMode, TransportType};
use mcp_proxy::lazy_registry::SpawnState;
use mcp_proxy::warm_cache::{BinaryHasher, WarmCatalog, WarmCatalogStore};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Unique per-test sequence so concurrent tests never share dirs or scripts.
static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

fn test_dir(tag: &str) -> PathBuf {
    let seq = TEST_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "mcp-proxy-lifecycle-{}-{}-{}",
        std::process::id(),
        tag,
        seq
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn write_server(tag: &str, body: &str) -> PathBuf {
    let seq = TEST_SEQ.fetch_add(1, Ordering::SeqCst);
    let server = std::env::temp_dir().join(format!(
        "mcp-lifecycle-srv-{}-{}-{}.py",
        std::process::id(),
        tag,
        seq
    ));
    std::fs::write(&server, body).expect("write server script");
    server
}

fn base_proxy_settings(name: &str, force_kill: bool) -> ProxySettings {
    ProxySettings {
        name: name.to_string(),
        version: "1.0.0".to_string(),
        separator: "/".to_string(),
        listen: mcp_proxy::config::ListenConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
        },
        instructions: None,
        shutdown_timeout_seconds: 30,
        shutdown_kill_timeout_secs: 2,
        force_kill,
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
    }
}

fn warm_cache_config(dir: &Path) -> mcp_proxy::config::WarmCacheConfig {
    mcp_proxy::config::WarmCacheConfig {
        enabled: true,
        dir: Some(dir.to_path_buf()),
        ttl_secs: 0,
        invalidate_on_hash_change: true,
    }
}

/// Build a [`ProxyConfig`] in-code with warm cache enabled, one eager stdio
/// backend (`eager`) and one lazy stdio backend (`files`), both pointing at
/// the given python server script. `force_kill` selects the kill mode for
/// BOTH backends (global OR-semantics apply at spawn).
fn lifecycle_config(dir: &Path, server: &Path, force_kill: bool) -> ProxyConfig {
    ProxyConfig {
        proxy: base_proxy_settings("lifecycle-proxy", force_kill),
        backends: vec![
            BackendConfig {
                name: "eager".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                enabled: true,
                ..Default::default()
            },
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
        warm_cache: warm_cache_config(dir),
        source_path: None,
        observability: mcp_proxy::config::ObservabilityConfig::default(),
    }
}

/// Build a [`ProxyConfig`] with a healthy eager backend plus a lazy backend
/// that exits immediately on spawn (fast `Connection closed` initialize
/// failure → spawn-conflict fast-fail). The lazy backend is flagged as an
/// externally-managed singleton so the actionable error carries the
/// configured stop hint.
fn conflict_config(dir: &Path, healthy_server: &Path, exit_script: &Path) -> ProxyConfig {
    ProxyConfig {
        proxy: base_proxy_settings("lifecycle-conflict-proxy", false),
        backends: vec![
            BackendConfig {
                name: "eager".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![healthy_server.to_string_lossy().to_string()],
                enabled: true,
                ..Default::default()
            },
            BackendConfig {
                name: "bad".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![exit_script.to_string_lossy().to_string()],
                spawn_mode: SpawnMode::Lazy,
                idle_timeout_secs: Some(30),
                enabled: true,
                singleton_externally_managed: true,
                singleton_stop_hint: Some("systemctl --user stop bad".to_string()),
                ..Default::default()
            },
        ],
        auth: None,
        performance: mcp_proxy::config::PerformanceConfig::default(),
        security: mcp_proxy::config::SecurityConfig::default(),
        cache: mcp_proxy::config::CacheBackendConfig::default(),
        composite_tools: vec![],
        warm_cache: warm_cache_config(dir),
        source_path: None,
        observability: mcp_proxy::config::ObservabilityConfig::default(),
    }
}

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

/// Seed a warm catalog on disk for `name` keyed by the real backend hash.
fn seed_catalog(dir: &Path, cfg: &BackendConfig, name: &str) {
    let store = WarmCatalogStore::new(dir.to_path_buf());
    let hash = BinaryHasher::hash(cfg);
    let catalog = WarmCatalog::from_probe_result(
        name,
        "/",
        vec![ping_tool()],
        vec![],
        vec![],
        vec![],
        Some("2026-07-28".to_string()),
        hash,
    );
    store.save(&catalog).expect("seed warm catalog");
}

/// Poll `cond` until it returns true or `deadline` elapses (<10s bound).
async fn poll_deadline(deadline: Duration, cond: impl Fn() -> bool, what: &str) {
    let start = std::time::Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < deadline,
            "timed out waiting for child exit: {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// L1/L2: restart kills proxy-owned stdio children under both kill modes
// ---------------------------------------------------------------------------

/// Spawn the proxy, bring the lazy backend Up, then shut both children down
/// via the same paths `Proxy::serve` uses (lazy `shutdown_all` + eager
/// `remove_backend`), and prove NEITHER child remains in the proxy namespaces
/// and the lazy state is Down. Runs under both `force_kill` modes.
async fn restart_kills_all_proxy_children(force_kill: bool) {
    let mode = if force_kill { "force" } else { "graceful" };
    let dir = test_dir(mode);
    let server = write_server(mode, MIN_MCP_SERVER);

    let cfg = lifecycle_config(&dir, &server, force_kill);
    seed_catalog(&dir, &cfg.backends[1], "files");

    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();
    let shared = proxy.shared_proxy();

    // Preconditions: eager child live in namespaces; lazy registered Down.
    assert!(
        shared.backend_namespaces().contains(&"eager".to_string()),
        "eager child must be live before restart ({mode})"
    );
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "lazy backend must start Down ({mode})"
    );

    // Bring the lazy child Up (real python child + post-spawn probe).
    reg.ensure_spawned("files").await.expect("spawn succeeds");
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Up,
        "lazy backend must be Up before restart ({mode})"
    );
    assert!(
        shared.backend_namespaces().contains(&"files".to_string()),
        "lazy child must be registered before restart ({mode})"
    );

    // Restart/shutdown path (mirrors `Proxy::serve` teardown order):
    // lazy-Up children first, then the disjoint eager set.
    let lazy_names: std::collections::HashSet<String> = reg.names().into_iter().collect();
    let eager_snapshot: Vec<String> = shared
        .backend_namespaces()
        .into_iter()
        .filter(|n| !lazy_names.contains(n))
        .collect();
    let terminated = reg.shutdown_all().await;
    assert!(
        terminated.contains(&"files".to_string()),
        "terminated set must include the lazy child ({mode})"
    );
    for name in &eager_snapshot {
        assert!(
            shared.remove_backend(name).await,
            "eager child '{name}' must be removed ({mode})"
        );
    }

    // Postconditions: no proxy-owned stdio child alive, lazy state Down.
    poll_deadline(
        Duration::from_secs(10),
        || {
            !shared.backend_namespaces().contains(&"eager".to_string())
                && !shared.backend_namespaces().contains(&"files".to_string())
        },
        &format!("proxy children to exit ({mode})"),
    )
    .await;
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "lazy backend must be Down after restart ({mode})"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn l1_restart_kills_children_graceful_mode() {
    restart_kills_all_proxy_children(false).await;
}

#[tokio::test]
async fn l2_restart_kills_children_force_mode() {
    restart_kills_all_proxy_children(true).await;
}

// ---------------------------------------------------------------------------
// L3: hot-reload removal terminates the old child
// ---------------------------------------------------------------------------

#[tokio::test]
async fn l3_hot_reload_removal_terminates_old_child() {
    let dir = test_dir("l3");
    let server = write_server("l3", MIN_MCP_SERVER);

    let cfg = lifecycle_config(&dir, &server, false);
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let shared = proxy.shared_proxy();

    // Precondition: the eager child is live in the proxy namespaces.
    assert!(
        shared.backend_namespaces().contains(&"eager".to_string()),
        "eager child must be live before hot-reload removal (L3)"
    );

    // Hot-reload remove path: `remove_backend` FIRST (kills the child).
    // The eager backend has no lazy entry, so removal alone is the kill
    // signal (lazy entries would `unregister` only AFTER this step).
    assert!(
        shared.remove_backend("eager").await,
        "remove_backend must report the old child removed (L3)"
    );

    // The old child must be terminated: namespaces drop it immediately and
    // a repeat removal is a no-op (already gone, idempotent).
    poll_deadline(
        Duration::from_secs(10),
        || !shared.backend_namespaces().contains(&"eager".to_string()),
        "old child to exit after hot-reload removal (L3)",
    )
    .await;
    assert!(
        !shared.remove_backend("eager").await,
        "second removal must be a no-op: old child already gone (L3)"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// L4: conflict backend absent from tools/list, CallTool actionable error
// ---------------------------------------------------------------------------

/// Full HTTP round-trip against a proxy whose lazy backend exits immediately:
/// cached tool visible while Down → `tools/call` fails fast with the
/// actionable `invalid_params` error (`data.backend`, `data.conflict:true`,
/// `data.hint` with the configured stop command) → `tools/list` omits the
/// backend until a healthy probe; registry stays Down for a fresh retry.
#[tokio::test]
async fn l4_conflict_backend_absent_from_list_with_actionable_call_error() {
    let dir = test_dir("l4");
    let healthy = write_server("l4-healthy", MIN_MCP_SERVER);
    let exit_script = write_server("l4-exit", EXIT_IMMEDIATELY);

    let cfg = conflict_config(&dir, &healthy, &exit_script);
    let bad_cfg = cfg.backends[1].clone();
    let bad_hash = BinaryHasher::hash(&bad_cfg);
    seed_catalog(&dir, &bad_cfg, "bad");

    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds with conflict backend lazy (not spawned at startup)");
    let reg = proxy.lazy_registry();

    // Precondition: Down with the seeded warm catalog loaded.
    assert_eq!(
        reg.spawn_state("bad"),
        SpawnState::Down,
        "conflict backend must start Down (L4)"
    );
    let loaded = reg
        .get("bad")
        .and_then(|b| b.catalog)
        .expect("seeded catalog must load at startup (L4)");
    assert_eq!(
        loaded.tools.len(),
        1,
        "seeded catalog must hold one tool (L4)"
    );
    assert_eq!(
        loaded.tools[0].name, "bad/ping",
        "seeded tool must be namespaced bad/ping (L4)"
    );

    // Serve the proxy on a random port and query it via HTTP.
    let (router, _handle) = proxy.into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let base = format!("http://{addr}/");

    async fn post(
        client: &reqwest::Client,
        base: &str,
        body: serde_json::Value,
    ) -> serde_json::Value {
        // Mirrors the E9 lazy-spawn-failure test: plain JSON POST, no Accept
        // header negotiation. Notifications (`notifications/initialized`)
        // must NOT go through this helper — the server answers 202 with an
        // empty body, which is not JSON.
        let resp = client
            .post(base)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&body).unwrap())
            .send()
            .await
            .expect("http post");
        let status = resp.status();
        let text = resp.text().await.expect("read body");
        assert!(
            status.is_success(),
            "MCP POST must succeed: status={status}, body={text}"
        );
        serde_json::from_str(&text).expect("json response")
    }

    // Session handshake (mirrors the E9 lazy-spawn-failure test).
    let init = post(
        &client,
        &base,
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": { "name": "l4-test", "version": "0.1.0" }
            }
        }),
    )
    .await;
    assert!(
        init.get("error").is_none(),
        "initialize must succeed (L4): {init}"
    );
    // Notifications are answered 202 with an empty body: fire-and-forget.
    let _ = client
        .post(&base)
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(
                &serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            )
            .unwrap(),
        )
        .send()
        .await;

    // While Down, the cached tool is served from the warm catalog.
    let list_before = post(
        &client,
        &base,
        serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
    )
    .await;
    let names_before: Vec<String> = list_before["result"]["tools"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
        .collect();
    assert!(
        names_before.contains(&"bad/ping".to_string()),
        "cached tool must be served while Down (L4): {names_before:?}"
    );

    // The first action request triggers the fast-failing spawn → actionable
    // JSON-RPC invalid_params error with backend + conflict + hint.
    let call = post(
        &client,
        &base,
        serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "bad/ping", "arguments": {} }
        }),
    )
    .await;
    assert!(
        call["error"].is_object(),
        "conflict CallTool must return a JSON-RPC error (L4): {call}"
    );
    assert_eq!(
        call["error"]["code"].as_i64(),
        Some(-32602),
        "conflict error must be invalid_params (L4): {call}"
    );
    let msg = call["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("bad"),
        "conflict error must name the backend (L4): {msg}"
    );
    assert_eq!(
        call["error"]["data"]["backend"].as_str(),
        Some("bad"),
        "conflict data must carry backend name (L4): {call}"
    );
    assert_eq!(
        call["error"]["data"]["conflict"],
        serde_json::json!(true),
        "conflict data must flag conflict:true (L4): {call}"
    );
    let hint = call["error"]["data"]["hint"].as_str().unwrap_or_default();
    assert!(
        hint.contains("systemctl --user stop bad"),
        "conflict data must carry the configured stop hint (L4): {hint}"
    );
    assert!(
        call["error"]["data"].get("pid").is_none(),
        "holder PID included only when known — never scanned (L4): {call}"
    );

    // After the proven conflict, tools/list omits the backend until a
    // healthy probe succeeds.
    let list_after = post(
        &client,
        &base,
        serde_json::json!({"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}}),
    )
    .await;
    let names_after: Vec<String> = list_after["result"]["tools"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
        .collect();
    assert!(
        !names_after.contains(&"bad/ping".to_string()),
        "invalidated backend must be omitted from tools/list (L4): {names_after:?}"
    );

    // Registry stays Down (next CallTool retries spawn fresh) and the stale
    // on-disk catalog is deleted.
    assert_eq!(
        reg.spawn_state("bad"),
        SpawnState::Down,
        "failed spawn leaves state Down (L4)"
    );
    assert!(
        reg.get("bad").and_then(|b| b.catalog).is_none(),
        "conflict must clear the stale in-memory catalog (L4)"
    );
    let store = WarmCatalogStore::new(dir.clone());
    assert!(
        store.load("bad", &bad_hash).is_none(),
        "conflict must delete the stale on-disk catalog (L4)"
    );
    assert!(
        !store.path_for("bad", &bad_hash).exists(),
        "stale catalog file must be gone (L4)"
    );

    let _ = std::fs::remove_file(&healthy);
    let _ = std::fs::remove_file(&exit_script);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// L5: restart-invalidated backend re-probes fresh on next startup
// ---------------------------------------------------------------------------

/// After `shutdown_all` invalidates a terminated backend's catalog file, a
/// new proxy built with the same (healthy) config re-probes and serves the
/// live catalog again instead of starting catalog-less.
#[tokio::test]
async fn l5_restart_invalidated_backend_reprobes_on_next_startup() {
    let dir = test_dir("l5");
    let server = write_server("l5", MIN_MCP_SERVER);

    let cfg = lifecycle_config(&dir, &server, false);
    let files_cfg = cfg.backends[1].clone();
    let files_hash = BinaryHasher::hash(&files_cfg);
    seed_catalog(&dir, &files_cfg, "files");

    // First proxy: bring the lazy child Up, then shut down (restart).
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("first proxy builds");
    let reg = proxy.lazy_registry();
    reg.ensure_spawned("files").await.expect("spawn succeeds");
    assert_eq!(reg.spawn_state("files"), SpawnState::Up);

    let terminated = reg.shutdown_all().await;
    assert!(terminated.contains(&"files".to_string()));
    let store = WarmCatalogStore::new(dir.clone());
    assert!(
        store.load("files", &files_hash).is_none(),
        "terminated backend catalog file must be invalidated (L5)"
    );
    drop(proxy);

    // Second proxy with the SAME healthy config: startup must re-probe fresh
    // (cache miss → live probe) and serve the live tool again.
    let proxy2 = mcp_proxy::Proxy::from_config(lifecycle_config(&dir, &server, false))
        .await
        .expect("second proxy builds");
    let reg2 = proxy2.lazy_registry();
    let reloaded = reg2
        .get("files")
        .and_then(|b| b.catalog)
        .expect("restart-invalidated backend must re-probe on next startup (L5)");
    assert!(
        reloaded.tools.iter().any(|t| t.name == "files/ping"),
        "re-probed catalog must serve the live tool (L5): {:?}",
        reloaded.tools.iter().map(|t| &t.name).collect::<Vec<_>>()
    );
    assert!(
        store
            .load("files", &BinaryHasher::hash(&files_cfg))
            .is_some(),
        "re-probed catalog must be persisted again (L5)"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Minimal MCP stdio server (Python stdlib only, newline-delimited JSON).
/// Exposes one tool `ping`.
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
        # Echo the offered version like a real server; 2026-07-28 is not an initialize-lifecycle version.
        send({"jsonrpc":"2.0","id":mid,"result":{"protocolVersion":(req.get("params") or {}).get("protocolVersion","2025-11-25"),"capabilities":{"tools":{}},"serverInfo":{"name":"min-server","version":"1.0.0"}}})
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

/// Fixture backend that exits immediately without answering `initialize`.
/// The proxy observes a fast `Connection closed` initialize failure — the
/// proven spawn-conflict signal (never the real CBM binary).
const EXIT_IMMEDIATELY: &str = r#"#!/usr/bin/env python3
import sys
sys.exit(1)
"#;
