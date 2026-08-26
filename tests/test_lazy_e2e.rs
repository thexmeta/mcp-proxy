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

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use tower::util::BoxCloneService;
use tower_mcp::router::{RouterRequest, RouterResponse};
use tower_mcp::{CallToolResult, McpRouter, ToolBuilder};

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

// ---------------------------------------------------------------------------
// G1: global [proxy].default_spawn_mode = "lazy" is inherited by a backend
//     that declares no per-backend spawn_mode, and flows through the real
//     Proxy::from_config wiring (registered Down, not eagerly spawned).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn g1_global_default_spawn_mode_registers_lazy() {
    let dir = test_dir("g1");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-g1-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    // Backend declares NO spawn_mode — it must inherit the global default.
    let cfg = ProxyConfig {
        proxy: ProxySettings {
            name: "g1-proxy".to_string(),
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
            default_spawn_mode: SpawnMode::Lazy,
            default_idle_timeout_secs: Some(600),
            protocol_support: mcp_proxy::config::ProtocolSupportConfig::default(),
        },
        backends: vec![
            BackendConfig {
                name: "__dummy__".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                // Explicitly eager so the proxy has ≥1 non-lazy backend to
                // build (the global default would otherwise make it lazy too).
                spawn_mode: SpawnMode::Eager,
                enabled: true,
                ..Default::default()
            },
            // No spawn_mode / idle_timeout_secs -> must inherit globals.
            BackendConfig {
                name: "files".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
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
    };

    // Seed the warm catalog so startup loads it (mirrors E1).
    let files_cfg = cfg.backends[1].clone();
    seed_warm_catalog(&dir, &files_cfg, "2026-07-28");

    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();

    // The backend must be registered lazy (Down) — proving the global default
    // was applied during normalization and flowed into the real proxy wiring.
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "backend with no spawn_mode must inherit global lazy default (G1)"
    );
    assert_eq!(
        reg.idle_timeout("files"),
        Some(600),
        "backend with no idle_timeout_secs must inherit global default (G1)"
    );
    assert!(
        !reg.proxy_has_namespace("files"),
        "lazy backend must not be eagerly added to the proxy (G1)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// g2: ALL-lazy proxy (zero eager backends)
// ---------------------------------------------------------------------------

/// Regression test for the all-lazy scenario: every backend has
/// `spawn_mode = "lazy"` and there are zero eager backends.
///
/// Before the tower-mcp patch that allows building an empty `McpProxy`,
/// this config would fail at startup with "No backends configured".
/// With the patch, the empty shared proxy is valid and
/// `WarmCatalogService` serves cached capabilities + triggers on-demand spawn.
#[tokio::test]
async fn g2_all_lazy_proxy_builds_with_zero_eager_backends() {
    let dir = test_dir("g2");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-g2-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let cfg = ProxyConfig {
        proxy: ProxySettings {
            name: "g2-all-lazy-proxy".to_string(),
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
            default_spawn_mode: SpawnMode::Lazy,
            default_idle_timeout_secs: Some(600),
            protocol_support: mcp_proxy::config::ProtocolSupportConfig::default(),
        },
        backends: vec![
            BackendConfig {
                name: "files".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                enabled: true,
                // No spawn_mode -> inherits global lazy default.
                ..Default::default()
            },
            BackendConfig {
                name: "tools".to_string(),
                transport: TransportType::Stdio,
                command: Some("python3".to_string()),
                args: vec![server.to_string_lossy().to_string()],
                enabled: true,
                // No spawn_mode -> inherits global lazy default.
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
    };

    // Seed warm catalogs for both lazy backends.
    seed_warm_catalog(&dir, &cfg.backends[0], "2026-07-28");
    seed_warm_catalog(&dir, &cfg.backends[1], "2026-07-28");

    // This MUST succeed — previously failed with "No backends configured".
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("all-lazy proxy must build (G2)");
    let reg = proxy.lazy_registry();

    // Both backends registered lazy (Down), not in the proxy.
    assert_eq!(
        reg.spawn_state("files"),
        SpawnState::Down,
        "files must be lazy (G2)"
    );
    assert_eq!(
        reg.spawn_state("tools"),
        SpawnState::Down,
        "tools must be lazy (G2)"
    );
    assert!(
        !reg.proxy_has_namespace("files"),
        "lazy backend must not be eagerly in the proxy (G2)"
    );
    assert!(
        !reg.proxy_has_namespace("tools"),
        "lazy backend must not be eagerly in the proxy (G2)"
    );

    let _ = std::fs::remove_file(&server);
}

// ---------------------------------------------------------------------------
// g3: default_spawn_mode = "lazy" must NOT exclude HTTP backends from proxy
// ---------------------------------------------------------------------------

/// Regression test for HTTP backends with lazy global default.
///
/// When `default_spawn_mode = "lazy"`, HTTP backends (which have no child
/// process to defer) must still be included in the shared proxy. Only lazy
/// STDIO backends are deferred to the LazyBackendRegistry.
#[tokio::test]
async fn g3_http_backends_not_excluded_by_lazy_default() {
    let dir = test_dir("g3");

    // Start a real HTTP MCP backend so the proxy can connect to it.
    let (http_addr, http_handle) = start_http_mcp_server(http_ping_router()).await;
    let http_url = format!("http://{}", http_addr);

    let cfg = ProxyConfig {
        proxy: ProxySettings {
            name: "g3-proxy".to_string(),
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
            default_spawn_mode: SpawnMode::Lazy,
            default_idle_timeout_secs: Some(600),
            protocol_support: mcp_proxy::config::ProtocolSupportConfig::default(),
        },
        backends: vec![
            // HTTP backend with NO explicit spawn_mode — inherits "lazy".
            // Must still be in the shared proxy (HTTP has no child process).
            BackendConfig {
                name: "http_backend".to_string(),
                transport: TransportType::Http,
                url: Some(http_url.clone()),
                enabled: true,
                ..Default::default()
            },
            // Stdio backend — inherits "lazy", should be deferred.
            BackendConfig {
                name: "stdio_lazy".to_string(),
                transport: TransportType::Stdio,
                command: Some("echo".to_string()),
                args: vec!["hello".to_string()],
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
    };

    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy must build (G3)");
    let reg = proxy.lazy_registry();

    // HTTP backend must be in the shared proxy (not lazy — HTTP has no process).
    assert!(
        reg.proxy_has_namespace("http_backend"),
        "HTTP backend must be in the shared proxy even with lazy default (G3)"
    );

    // Stdio backend must be deferred to the lazy registry.
    assert_eq!(
        reg.spawn_state("stdio_lazy"),
        SpawnState::Down,
        "stdio backend must be lazy (G3)"
    );
    assert!(
        !reg.proxy_has_namespace("stdio_lazy"),
        "lazy stdio backend must NOT be in the shared proxy (G3)"
    );

    http_handle.abort();
}

// ---------------------------------------------------------------------------
// G4: endpoint group with ONLY lazy stdio backends exposes warm-catalog tools
// ---------------------------------------------------------------------------

/// Regression test for the `in_group()` string-matching bug in
/// `src/warm_catalog_service.rs`.
///
/// THE BUG: `in_group()` did `name.split(separator).next()` to extract the
/// backend prefix, then checked `scope.contains(prefix)`. But the scope set is
/// built as `format!("{name}{separator}")` (e.g. `"files_"`), so the bare
/// prefix `"files"` (without the trailing separator) never matched → every
/// warm-catalog tool append for endpoint groups silently failed → groups
/// containing ONLY lazy stdio backends returned 0 tools.
///
/// This test builds a proxy with `default_spawn_mode = "lazy"` and an endpoint
/// group (`os`) whose ONLY members are two lazy stdio backends (`files`,
/// `term`), each seeded with a warm catalog. It then queries `/os/mcp` and
/// asserts BOTH backends' namespaced tools are exposed. Before the fix this
/// returned 0 tools; after the fix it returns 2.
#[tokio::test]
async fn g4_endpoint_group_with_only_lazy_stdio_exposes_warm_catalog_tools() {
    use mcp_proxy::config::EndpointGroupConfig;

    let dir = test_dir("g4");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-g4-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let mut cfg = lazy_config(&dir, &server);
    // Force lazy default so BOTH stdio backends are lazy.
    cfg.proxy.default_spawn_mode = SpawnMode::Lazy;
    // Use `_` separator to mirror the live config where the `in_group` bug was
    // discovered (scope set = {"files_", "term_"} with trailing separator).
    cfg.proxy.separator = "_".to_string();
    // Add a second lazy stdio backend (`term`) and an endpoint group `os`
    // containing both lazy backends.
    cfg.backends.push(BackendConfig {
        name: "term".to_string(),
        transport: TransportType::Stdio,
        command: Some("python3".to_string()),
        args: vec![server.to_string_lossy().to_string()],
        spawn_mode: SpawnMode::Lazy,
        idle_timeout_secs: Some(30),
        enabled: true,
        ..Default::default()
    });
    cfg.proxy.endpoint_groups = vec![EndpointGroupConfig {
        name: "os".to_string(),
        path: "/os".to_string(),
        backends: vec![],
        tools: vec![],
        description: None,
        tool_discovery: false,
    }];
    cfg.backends[1].endpoint_groups = vec!["os".to_string()];
    cfg.backends[2].endpoint_groups = vec!["os".to_string()];

    // Seed warm catalogs for BOTH lazy backends (namespaced `files_*`/`term_*`).
    let files_cfg = cfg.backends[1].clone();
    let term_cfg = cfg.backends[2].clone();
    seed_warm_catalog_for(&dir, &files_cfg, "files", "2026-07-28");
    seed_warm_catalog_for(&dir, &term_cfg, "term", "2026-07-28");

    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();
    assert_eq!(reg.spawn_state("files"), SpawnState::Down, "files Down");
    assert_eq!(reg.spawn_state("term"), SpawnState::Down, "term Down");

    // Serve the proxy on a random port and query the `/os` endpoint group.
    let (router, _handle) = proxy.into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    let client = reqwest::Client::new();
    let base = format!("http://{addr}/os/mcp");

    // initialize
    let init = client
        .post(&base)
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "g4-test", "version": "0.1.0" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("initialize");
    let init_status = init.status();
    let init_body = init.text().await.unwrap_or_default();
    assert!(
        init_status.is_success(),
        "initialize must succeed: status={}, body={}",
        init_status,
        init_body
    );
    let _ = client
        .post(&base)
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0", "method": "notifications/initialized"
            }))
            .unwrap(),
        )
        .send()
        .await;

    // tools/list
    let resp = client
        .post(&base)
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/list",
                "params": {}
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("tools/list");
    assert!(resp.status().is_success(), "tools/list must succeed");
    let body: serde_json::Value = resp.json().await.expect("json");
    let tools = body["result"]["tools"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let names: Vec<String> = tools
        .iter()
        .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
        .collect();

    assert!(
        names.iter().any(|n| n.starts_with("files_")),
        "endpoint group `os` must expose files_* warm-catalog tools, got: {names:?}"
    );
    assert!(
        names.iter().any(|n| n.starts_with("term_")),
        "endpoint group `os` must expose term_* warm-catalog tools, got: {names:?}"
    );
    assert_eq!(
        names.len(),
        2,
        "endpoint group `os` must expose exactly 2 warm-catalog tools, got: {names:?}"
    );
}

/// Seed a warm catalog on disk for an arbitrary backend name (G4 needs two).
fn seed_warm_catalog_for(dir: &Path, cfg: &BackendConfig, backend_name: &str, version: &str) {
    let store = WarmCatalogStore::new(dir.to_path_buf());
    let hash = BinaryHasher::hash(cfg);
    let catalog = WarmCatalog::from_probe_result(
        backend_name,
        "_",
        vec![ping_tool()],
        vec![],
        vec![],
        vec![],
        Some(version.to_string()),
        hash,
    );
    store.save(&catalog).expect("seed warm catalog");
}

/// Start a real HTTP MCP server on a random port and return (addr, handle).
/// The proxy can connect to it as an HTTP backend.
async fn start_http_mcp_server(router: McpRouter) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let service: BoxCloneService<RouterRequest, RouterResponse, std::convert::Infallible> =
        BoxCloneService::new(router);
    let (axum_router, _session_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(service).into_router_with_handle();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port");
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, axum_router).await.ok();
    });
    (addr, handle)
}

/// A minimal HTTP MCP backend exposing one tool `http_ping`.
fn http_ping_router() -> McpRouter {
    let ping = ToolBuilder::new("http_ping")
        .description("Return pong")
        .handler(|_: tower_mcp::NoParams| async move { Ok(CallToolResult::text("pong")) })
        .build();
    McpRouter::new()
        .server_info("http-ping", "1.0.0")
        .tool(ping)
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

// ---------------------------------------------------------------------------
// G5: endpoint group cache MISS probes an fs-like backend that errors on
//     resources/list — regression for the `?`-propagating probe abort.
// ---------------------------------------------------------------------------

/// An fs-like MCP server (Python stdlib only, newline-delimited JSON) that
/// exposes two tools (`read_file`, `write_file`) but ERRORS on
/// `resources/list`, `resources/templates/list`, and `prompts/list` with
/// "Server does not support resources" — exactly like rust-mcp-filesystem.
/// Modeled on the inline script in `probe_succeeds_when_resources_unsupported`
/// (src/warm_cache/probe.rs).
const FS_LIKE_SERVER: &str = r#"
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
        send({"jsonrpc":"2.0","id":mid,"result":{
            "protocolVersion":"2025-11-25",
            "capabilities":{"tools":{"listChanged":False}},
            "serverInfo":{"name":"fs-like","version":"0.1.0"}}})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"tools":[
            {"name":"read_file","description":"read","inputSchema":{"type":"object"}},
            {"name":"write_file","description":"write","inputSchema":{"type":"object"}}]}})
    elif method == "resources/list":
        send({"jsonrpc":"2.0","id":mid,"error":{
            "code":-32603,"message":"Server does not support resources (required for resources/list)"}})
    elif method == "resources/templates/list":
        send({"jsonrpc":"2.0","id":mid,"error":{
            "code":-32603,"message":"Server does not support resources"}})
    elif method == "prompts/list":
        send({"jsonrpc":"2.0","id":mid,"error":{
            "code":-32603,"message":"Server does not support prompts"}})
    elif method == "shutdown":
        send({"jsonrpc":"2.0","id":mid,"result":{}})
        break
    else:
        send({"jsonrpc":"2.0","id":mid,"result":{}})
"#;

/// Regression test for the live cache-MISS probe abort.
///
/// THE BUG: when the warm cache missed, `ProbeRunner::probe` ran at startup
/// against the real backend. If that backend exposed tools but did NOT support
/// `resources/list` (e.g. rust-mcp-filesystem), the probe's `?`-propagating
/// `list_all_resources()` call returned `Err` and aborted the WHOLE probe —
/// so the backend got NO warm catalog and its `files_*` tools were missing
/// from the `os` endpoint group (only `term_*` showed up).
///
/// This test forces a real cache MISS (fresh temp dir, no seeded catalog) for
/// an `os` endpoint group whose members are two lazy stdio backends:
///   - `files`: an fs-like server returning 2 tools but erroring on
///     resources/list (the regression trigger),
///   - `term`: a normal server returning 1 tool (`ping`) that does NOT error.
///
/// With the buggy probe this yields 0 `files_*` tools; with the fixed
/// best-effort probe it must yield BOTH backends' tools (2 + 1 = 3).
#[tokio::test]
async fn g5_endpoint_group_cache_miss_probes_fs_like_backend() {
    use mcp_proxy::config::EndpointGroupConfig;

    // Fresh, un-seeded warm cache dir -> forces a real probe at startup.
    let dir = test_dir("g5");
    let server = std::env::temp_dir().join(format!(
        "mcp-min-srv-g5-{}-{}.py",
        std::process::id(),
        TEST_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&server, MIN_MCP_SERVER).expect("write server script");

    let mut cfg = lazy_config(&dir, &server);
    // Force lazy default so BOTH stdio backends are lazy.
    cfg.proxy.default_spawn_mode = SpawnMode::Lazy;
    // `_` separator to mirror the live config where the bug was found.
    cfg.proxy.separator = "_".to_string();

    // `files` becomes the fs-like backend that errors on resources/list.
    cfg.backends[1].command = Some("python3".to_string());
    cfg.backends[1].args = vec!["-c".to_string(), FS_LIKE_SERVER.to_string()];

    // `term` is a normal backend (uses MIN_MCP_SERVER) that does NOT error.
    cfg.backends.push(BackendConfig {
        name: "term".to_string(),
        transport: TransportType::Stdio,
        command: Some("python3".to_string()),
        args: vec![server.to_string_lossy().to_string()],
        spawn_mode: SpawnMode::Lazy,
        idle_timeout_secs: Some(30),
        enabled: true,
        ..Default::default()
    });

    // Both lazy backends belong to the `os` endpoint group.
    cfg.proxy.endpoint_groups = vec![EndpointGroupConfig {
        name: "os".to_string(),
        path: "/os".to_string(),
        backends: vec![],
        tools: vec![],
        description: None,
        tool_discovery: false,
    }];
    cfg.backends[1].endpoint_groups = vec!["os".to_string()];
    cfg.backends[2].endpoint_groups = vec!["os".to_string()];

    // NO seeding — force a cache miss so the probe runs against the real
    // `files` backend (which errors on resources/list).
    let proxy = mcp_proxy::Proxy::from_config(cfg)
        .await
        .expect("proxy builds");
    let reg = proxy.lazy_registry();
    assert_eq!(reg.spawn_state("files"), SpawnState::Down, "files Down");
    assert_eq!(reg.spawn_state("term"), SpawnState::Down, "term Down");

    // The `files` warm catalog MUST have been captured despite the resources
    // error (best-effort probe). This is the core regression assertion at the
    // registry level.
    let files_cat = reg
        .get("files")
        .and_then(|b| b.catalog)
        .expect("files warm catalog must be captured on cache miss (G5)");
    assert_eq!(
        files_cat.tools.len(),
        2,
        "files must capture both tools despite resources error (G5)"
    );

    // Serve the proxy on a random port and query the `/os` endpoint group.
    let (router, _handle) = proxy.into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    let client = reqwest::Client::new();
    let base = format!("http://{addr}/os/mcp");

    let init = client
        .post(&base)
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "g5-test", "version": "0.1.0" }
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("initialize");
    let init_status = init.status();
    let init_body = init.text().await.unwrap_or_default();
    assert!(
        init_status.is_success(),
        "initialize must succeed: status={}, body={}",
        init_status,
        init_body
    );
    let _ = client
        .post(&base)
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0", "method": "notifications/initialized"
            }))
            .unwrap(),
        )
        .send()
        .await;

    let resp = client
        .post(&base)
        .header("Content-Type", "application/json")
        .body(
            serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/list",
                "params": {}
            }))
            .unwrap(),
        )
        .send()
        .await
        .expect("tools/list");
    assert!(resp.status().is_success(), "tools/list must succeed");
    let body: serde_json::Value = resp.json().await.expect("json");
    let tools = body["result"]["tools"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let names: Vec<String> = tools
        .iter()
        .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
        .collect();

    // THE REGRESSION ASSERTION: the fs-like `files` backend's tools must be
    // present even though it errored on resources/list during the cache-miss
    // probe.
    assert!(
        names.iter().any(|n| n.starts_with("files_")),
        "endpoint group `os` must expose files_* warm-catalog tools after a cache-miss probe, got: {names:?}"
    );
    assert!(
        names.iter().any(|n| n.starts_with("term_")),
        "endpoint group `os` must expose term_* warm-catalog tools, got: {names:?}"
    );
    // 2 from `files` (read_file, write_file) + 1 from `term` (ping).
    assert_eq!(
        names.len(),
        3,
        "endpoint group `os` must expose exactly 3 warm-catalog tools, got: {names:?}"
    );

    let _ = std::fs::remove_file(&server);
    let _ = std::fs::remove_dir_all(&dir);
}
