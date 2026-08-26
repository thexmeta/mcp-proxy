//! Config-parse tests for the lazy/on-demand backend spawning feature.
//!
//! Verifies that `[proxy.warm_cache]` and per-backend `spawn_mode`,
//! `idle_timeout_secs`, and `cache_key_suffix` deserialize correctly, and that
//! a backend without `spawn_mode` defaults to `Eager` (backward compatible).

use mcp_proxy::config::{ProxyConfig, SpawnMode};

#[test]
fn test_parse_lazy_backend_with_warm_cache() {
    let toml = r#"
    [proxy]
    name = "test"
    [proxy.listen]

    [warm_cache]
    enabled = true
    dir = "/tmp/mcp-proxy-warm-test"
    ttl_secs = 3600

    [[backends]]
    name = "files"
    transport = "stdio"
    command = "ls"
    spawn_mode = "lazy"
    idle_timeout_secs = 300
    cache_key_suffix = "v2"
    "#;

    let cfg = ProxyConfig::parse(toml).expect("config should parse");

    // Warm cache section.
    assert!(cfg.warm_cache.enabled);
    assert_eq!(
        cfg.warm_cache.dir.as_deref(),
        Some(std::path::Path::new("/tmp/mcp-proxy-warm-test"))
    );
    assert_eq!(cfg.warm_cache.ttl_secs, 3600);

    // Lazy backend fields.
    assert_eq!(cfg.backends.len(), 1);
    assert_eq!(cfg.backends[0].spawn_mode, SpawnMode::Lazy);
    assert_eq!(cfg.backends[0].idle_timeout_secs, Some(300));
    assert_eq!(cfg.backends[0].cache_key_suffix.as_deref(), Some("v2"));
}

#[test]
fn test_parse_backend_defaults_to_eager() {
    let toml = r#"
    [proxy]
    name = "test"
    [proxy.listen]

    [[backends]]
    name = "files"
    transport = "stdio"
    command = "ls"
    "#;

    let cfg = ProxyConfig::parse(toml).expect("config should parse");

    // No spawn_mode => Eager (backward compatible default).
    assert_eq!(cfg.backends[0].spawn_mode, SpawnMode::Eager);
    // Optional fields default to None.
    assert_eq!(cfg.backends[0].idle_timeout_secs, None);
    assert_eq!(cfg.backends[0].cache_key_suffix, None);
    // Warm cache defaults to disabled.
    assert!(!cfg.warm_cache.enabled);
    assert_eq!(cfg.warm_cache.ttl_secs, 0);
}

#[test]
fn test_global_default_spawn_mode_inherited_by_backend() {
    // A backend with no `spawn_mode` inherits `[proxy].default_spawn_mode`.
    let toml = r#"
    [proxy]
    name = "test"
    default_spawn_mode = "lazy"

    [proxy.listen]

    [[backends]]
    name = "files"
    transport = "stdio"
    command = "ls"
    "#;

    let cfg = ProxyConfig::parse(toml).expect("config should parse");
    // Normalization resolves `Unset` -> proxy default (`lazy`).
    assert_eq!(cfg.backends[0].spawn_mode, SpawnMode::Lazy);
}

#[test]
fn test_global_default_spawn_mode_overridden_per_backend() {
    // An explicit per-backend `spawn_mode` wins over the global default.
    let toml = r#"
    [proxy]
    name = "test"
    default_spawn_mode = "lazy"

    [proxy.listen]

    [[backends]]
    name = "files"
    transport = "stdio"
    command = "ls"
    spawn_mode = "eager"
    "#;

    let cfg = ProxyConfig::parse(toml).expect("config should parse");
    assert_eq!(cfg.backends[0].spawn_mode, SpawnMode::Eager);
}

#[test]
fn test_global_default_idle_timeout_inherited_by_backend() {
    // A backend with no `idle_timeout_secs` inherits
    // `[proxy].default_idle_timeout_secs`.
    let toml = r#"
    [proxy]
    name = "test"
    default_idle_timeout_secs = 600

    [proxy.listen]

    [[backends]]
    name = "files"
    transport = "stdio"
    command = "ls"
    spawn_mode = "lazy"
    "#;

    let cfg = ProxyConfig::parse(toml).expect("config should parse");
    assert_eq!(cfg.backends[0].idle_timeout_secs, Some(600));
}

#[test]
fn test_no_global_defaults_keeps_eager_backward_compatible() {
    // With no global defaults and no per-backend spawn_mode, the backend
    // resolves to `Eager` (the proxy-level default_spawn_mode default).
    let toml = r#"
    [proxy]
    name = "test"

    [proxy.listen]

    [[backends]]
    name = "files"
    transport = "stdio"
    command = "ls"
    "#;

    let cfg = ProxyConfig::parse(toml).expect("config should parse");
    assert_eq!(cfg.backends[0].spawn_mode, SpawnMode::Eager);
    assert_eq!(cfg.backends[0].idle_timeout_secs, None);
}
