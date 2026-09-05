//! Hot reload: watch the config file for changes and manage backends dynamically.
//!
//! Supports adding, removing, and replacing backends at runtime when the config
//! file changes. Uses content hashing to detect modifications.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use notify_debouncer_mini::new_debouncer;
use tokio::sync::mpsc;
use tower::util::BoxCloneService;
use tower_mcp::proxy::{BackendService, McpProxy};

use crate::config::{BackendConfig, ProxyConfig, TransportType, WatcherConfig};
use crate::endpoint_router::EndpointGroupRegistry;

/// Trait for config file watchers.
#[async_trait::async_trait]
trait ConfigWatcher: Send + Sync + 'static {
    async fn watch(&self, path: &Path) -> Result<mpsc::Receiver<()>>;
    fn name(&self) -> &'static str;
}

/// OS-level file watcher using notify (inotify/kqueue/fsevents).
struct InotifyWatcher;

#[async_trait::async_trait]
impl ConfigWatcher for InotifyWatcher {
    async fn watch(&self, path: &Path) -> Result<mpsc::Receiver<()>> {
        let (tx, rx) = mpsc::channel(1);
        let mut debouncer = new_debouncer(
            Duration::from_secs(2),
            move |res: Result<Vec<notify_debouncer_mini::DebouncedEvent>, notify::Error>| {
                if let Ok(events) = res {
                    let has_write = events
                        .iter()
                        .any(|e| matches!(e.kind, notify_debouncer_mini::DebouncedEventKind::Any));
                    if has_write {
                        let _ = tx.blocking_send(());
                    }
                }
            },
        )?;
        debouncer
            .watcher()
            .watch(path, notify::RecursiveMode::NonRecursive)?;
        // Keep debouncer alive by leaking it (it runs in background)
        std::mem::forget(debouncer);
        Ok(rx)
    }
    fn name(&self) -> &'static str {
        "inotify"
    }
}

/// Lightweight mtime-based polling watcher.
struct MtimeWatcher {
    interval: Duration,
}

#[async_trait::async_trait]
impl ConfigWatcher for MtimeWatcher {
    async fn watch(&self, path: &Path) -> Result<mpsc::Receiver<()>> {
        let (tx, rx) = mpsc::channel(1);
        let path = path.to_path_buf();
        let interval = self.interval;

        tokio::spawn(async move {
            let mut last_mtime = std::fs::metadata(&path)
                .ok()
                .and_then(|m| m.modified().ok());
            loop {
                tokio::time::sleep(interval).await;
                if let Ok(meta) = std::fs::metadata(&path)
                    && let Ok(mtime) = meta.modified()
                    && last_mtime != Some(mtime)
                {
                    last_mtime = Some(mtime);
                    if tx.send(()).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(rx)
    }
    fn name(&self) -> &'static str {
        "mtime"
    }
}

/// Signal-based watcher (SIGHUP).
struct SignalWatcher;

#[async_trait::async_trait]
impl ConfigWatcher for SignalWatcher {
    async fn watch(&self, _path: &Path) -> Result<mpsc::Receiver<()>> {
        let (tx, rx) = mpsc::channel(1);
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sighup = signal(SignalKind::hangup())?;
            tokio::spawn(async move {
                while sighup.recv().await.is_some() {
                    if tx.send(()).await.is_err() {
                        break;
                    }
                }
            });
        }
        #[cfg(not(unix))]
        {
            // On non-Unix, this watcher does nothing (never fires)
            tokio::spawn(async move {
                let _ = tx; // suppress unused warning
                // Channel never receives, receiver will hang forever
                // which is fine - we'll fall back to other watchers
                std::future::pending::<()>().await;
            });
        }
        Ok(rx)
    }
    fn name(&self) -> &'static str {
        "signal"
    }
}

/// Build a watcher from config.
fn build_watcher(config: &WatcherConfig) -> Box<dyn ConfigWatcher> {
    match config {
        WatcherConfig::Inotify => Box::new(InotifyWatcher),
        WatcherConfig::Mtime { interval_seconds } => Box::new(MtimeWatcher {
            interval: Duration::from_secs(*interval_seconds),
        }),
        WatcherConfig::Signal => Box::new(SignalWatcher),
    }
}

/// Spawn a background task that watches the config file and manages backends dynamically.
/// Tries watchers in order until one succeeds.
#[allow(clippy::too_many_arguments)]
pub fn spawn_config_watcher(
    config_path: PathBuf,
    proxy: McpProxy,
    shared_proxy: McpProxy,
    endpoint_group_registry: EndpointGroupRegistry,
    watchers: Vec<WatcherConfig>,
    alias_map: Option<std::sync::Arc<std::sync::RwLock<crate::alias::AliasMap>>>,
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
    #[cfg(feature = "discovery")] discovery_index: Option<(
        crate::discovery::SharedDiscoveryIndex,
        String,
    )>,
) {
    tokio::spawn(async move {
        watch_loop(
            config_path,
            proxy,
            shared_proxy,
            endpoint_group_registry,
            watchers,
            alias_map,
            lazy_registry,
            #[cfg(feature = "discovery")]
            discovery_index,
        )
        .await;
    });
}

#[allow(clippy::too_many_arguments)]
async fn watch_loop(
    config_path: PathBuf,
    proxy: McpProxy,
    shared_proxy: McpProxy,
    endpoint_group_registry: EndpointGroupRegistry,
    watchers: Vec<WatcherConfig>,
    alias_map: Option<std::sync::Arc<std::sync::RwLock<crate::alias::AliasMap>>>,
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
    #[cfg(feature = "discovery")] discovery_index: Option<(
        crate::discovery::SharedDiscoveryIndex,
        String,
    )>,
) {
    // Separate signal watchers from file watchers.
    // File watchers (inotify, mtime) are tried as fallbacks — use the first that succeeds.
    // Signal watchers (SIGHUP) are always registered alongside the chosen file watcher
    // to prevent SIGHUP from killing the process.
    let mut receivers: Vec<mpsc::Receiver<()>> = Vec::new();
    let mut file_watcher_started = false;

    for watcher_config in &watchers {
        let watcher = build_watcher(watcher_config);
        let is_signal = matches!(watcher_config, WatcherConfig::Signal);

        if !file_watcher_started && !is_signal {
            // File watchers: use first one that succeeds (fallback chain)
            tracing::info!(watcher = watcher.name(), "Trying config file watcher");
            match watcher.watch(&config_path).await {
                Ok(rx) => {
                    receivers.push(rx);
                    file_watcher_started = true;
                    tracing::info!(watcher = watcher.name(), "Config file watcher started");
                }
                Err(e) => {
                    tracing::warn!(watcher = watcher.name(), error = %e, "Watcher failed, trying next");
                }
            }
        } else if is_signal {
            // Signal watcher: always registered alongside the chosen file watcher
            // so SIGHUP never kills the process, regardless of which file watcher won.
            tracing::info!(watcher = watcher.name(), "Trying config file watcher");
            match watcher.watch(&config_path).await {
                Ok(rx) => {
                    receivers.push(rx);
                    tracing::info!(watcher = watcher.name(), "Config file watcher started");
                }
                Err(e) => {
                    tracing::warn!(watcher = watcher.name(), error = %e, "Watcher failed");
                }
            }
        }
    }

    if receivers.is_empty() {
        tracing::error!("All config file watchers failed, hot reload disabled");
        return;
    }

    // Track known backends and their config fingerprints for change detection.
    // IMPORTANT: resolve_env_vars() must be called before fingerprinting —
    // the hot-reload path resolves env vars before fingerprinting, so the
    // initial fingerprints must also resolve them to ensure a matching
    // comparison on the first reload cycle.
    let mut backend_fingerprints: HashMap<String, String> = {
        if let Ok(mut config) = ProxyConfig::load(&config_path) {
            config.resolve_env_vars();
            config
                .backends
                .iter()
                .map(|b| (b.name.clone(), config_fingerprint(b)))
                .collect()
        } else {
            HashMap::new()
        }
    };

    // Track known endpoint groups and their config fingerprints for change detection
    let mut endpoint_group_fingerprints: HashMap<String, String> = {
        if let Ok(config) = ProxyConfig::load(&config_path) {
            config
                .proxy
                .endpoint_groups
                .iter()
                .map(|eg| (eg.name.clone(), config_fingerprint_endpoint_group(eg)))
                .collect()
        } else {
            HashMap::new()
        }
    };

    // Track file mtime to skip redundant reloads when the watcher fires
    // but the config file hasn't actually been modified (e.g. spurious
    // inotify events from systemd ProtectSystem/BindReadOnlyPaths).
    let mut last_mtime: Option<std::time::SystemTime> = std::fs::metadata(&config_path)
        .ok()
        .and_then(|m| m.modified().ok());

    // Merge all watcher receivers into a single stream for concurrent polling.
    // This ensures signal watchers (SIGHUP) are checked alongside mtime watchers
    // without one blocking the other.
    use futures_util::stream::{SelectAll, StreamExt};
    use tokio_stream::wrappers::ReceiverStream;
    let mut stream: SelectAll<_> = receivers.into_iter().map(ReceiverStream::new).collect();

    loop {
        // Wait for a change event from any registered watcher concurrently.
        let notification = stream.next().await;

        match notification {
            Some(()) => { /* got a notification, proceed with reload */ }
            None => {
                // All watcher streams ended (all channels closed or watchers exited).
                tracing::info!("All config watcher channels closed, stopping hot reload");
                break;
            }
        }

        // Mtime guard: skip reload if the file hasn't actually been modified.
        // This filters out spurious inotify events caused by systemd sandboxing
        // (ProtectSystem, BindReadOnlyPaths) or other filesystem-level noise.
        if let Ok(meta) = std::fs::metadata(&config_path)
            && let Ok(mtime) = meta.modified()
        {
            if last_mtime == Some(mtime) {
                tracing::debug!("Config file watcher fired but mtime unchanged, skipping reload");
                continue;
            }
            last_mtime = Some(mtime);
        }

        tracing::info!("Config file changed, reloading backends and endpoint groups");

        let mut new_config = match ProxyConfig::load(&config_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to parse updated config, skipping reload");
                continue;
            }
        };
        new_config.resolve_env_vars();

        let kill_timeout_secs = new_config.proxy.shutdown_kill_timeout_secs;

        let new_fingerprints: HashMap<String, String> = new_config
            .backends
            .iter()
            .map(|b| (b.name.clone(), config_fingerprint(b)))
            .collect();

        let old_names: HashSet<&String> = backend_fingerprints.keys().collect();
        let new_names: HashSet<&String> = new_fingerprints.keys().collect();

        // Track actual changes for conditional discovery re-indexing
        let mut removed_backends: Vec<&String> = Vec::new();
        let mut added_backends: Vec<&String> = Vec::new();
        let mut replaced_backends: Vec<&String> = Vec::new();
        let mut removed_egs: Vec<&String> = Vec::new();
        let mut added_egs: Vec<&String> = Vec::new();
        let mut replaced_egs: Vec<&String> = Vec::new();

        // Remove backends that are no longer in config
        for removed in old_names.difference(&new_names) {
            tracing::info!(backend = %removed, "Removing backend via hot reload");
            removed_backends.push(removed);
            if proxy.remove_backend(removed).await {
                tracing::info!(backend = %removed, "Backend removed");
            } else {
                tracing::warn!(backend = %removed, "Backend not found for removal");
            }
            // FR-009: drop any lazy-registry entry (no-op if not lazy).
            lazy_registry.unregister(removed);
        }

        // Add new backends
        for backend in &new_config.backends {
            if backend_fingerprints.contains_key(&backend.name) {
                // Existing backend -- check for modification
                let old_fp = &backend_fingerprints[&backend.name];
                let new_fp = &new_fingerprints[&backend.name];

                if old_fp != new_fp {
                    tracing::info!(
                        backend = %backend.name,
                        "Backend config changed, replacing via hot reload"
                    );

                    replaced_backends.push(&backend.name);

                    // Determine the OLD spawn mode: a backend present in the lazy
                    // registry was lazy; otherwise it was eager (FR-009/AC-006).
                    let old_mode = lazy_registry
                        .get(&backend.name)
                        .map(|b| b.config.spawn_mode)
                        .unwrap_or(crate::config::SpawnMode::Eager);

                    match (old_mode, backend.spawn_mode.clone()) {
                        // eager -> lazy: kill the child, register lazy (no spawn).
                        (crate::config::SpawnMode::Eager, crate::config::SpawnMode::Lazy) => {
                            proxy.remove_backend(&backend.name).await;
                            lazy_registry.register_lazy(backend.clone());
                            tracing::info!(backend = %backend.name, "Backend flipped eager -> lazy (registered, not spawned)");
                        }
                        // lazy -> eager: drop lazy registration, eager spawn.
                        (crate::config::SpawnMode::Lazy, crate::config::SpawnMode::Eager) => {
                            lazy_registry.unregister(&backend.name);
                            if let Err(e) = add_backend(&proxy, backend, kill_timeout_secs).await {
                                tracing::error!(
                                    backend = %backend.name,
                                    error = %e,
                                    "Failed to flip backend lazy -> eager via hot reload"
                                );
                            } else {
                                tracing::info!(backend = %backend.name, "Backend flipped lazy -> eager (spawned)");
                            }
                        }
                        // both lazy: config changed but still lazy -> re-probe + cache update.
                        (crate::config::SpawnMode::Lazy, crate::config::SpawnMode::Lazy) => {
                            if let Err(e) = lazy_registry.reconcile_config_change(backend).await {
                                tracing::error!(
                                    backend = %backend.name,
                                    error = %e,
                                    "Failed to reconcile lazy backend config change"
                                );
                            } else {
                                tracing::info!(backend = %backend.name, "Lazy backend config reconciled (will re-probe on next spawn)");
                            }
                        }
                        // both eager: existing behavior — remove old, add new.
                        (crate::config::SpawnMode::Eager, crate::config::SpawnMode::Eager) => {
                            proxy.remove_backend(&backend.name).await;
                            if let Err(e) = add_backend(&proxy, backend, kill_timeout_secs).await {
                                tracing::error!(
                                    backend = %backend.name,
                                    error = %e,
                                    "Failed to replace backend via hot reload"
                                );
                            } else {
                                tracing::info!(backend = %backend.name, "Backend replaced");
                            }
                        }
                        // `Unset` should never reach here — `apply_global_defaults()`
                        // resolves it to Eager/Lazy at load time. Treat defensively
                        // as Eager (existing behavior) to keep the match exhaustive.
                        (crate::config::SpawnMode::Unset, _)
                        | (_, crate::config::SpawnMode::Unset) => {
                            proxy.remove_backend(&backend.name).await;
                            if let Err(e) = add_backend(&proxy, backend, kill_timeout_secs).await {
                                tracing::error!(
                                    backend = %backend.name,
                                    error = %e,
                                    "Failed to replace backend via hot reload"
                                );
                            } else {
                                tracing::info!(backend = %backend.name, "Backend replaced");
                            }
                        }
                    }
                }
                continue;
            }

            added_backends.push(&backend.name);
            tracing::info!(
                name = %backend.name,
                transport = ?backend.transport,
                spawn_mode = ?backend.spawn_mode,
                "Adding new backend via hot reload"
            );

            // AC-006: lazy STDIO backends are registered but NOT eagerly spawned.
            // HTTP/WebSocket backends have no child process to defer, so "lazy"
            // is meaningless for them — always spawn eagerly.
            if backend.spawn_mode == crate::config::SpawnMode::Lazy
                && backend.transport == crate::config::TransportType::Stdio
            {
                lazy_registry.register_lazy(backend.clone());
                tracing::info!(backend = %backend.name, "Lazy backend added via hot reload (not spawned)");
            } else if let Err(e) = add_backend(&proxy, backend, kill_timeout_secs).await {
                tracing::error!(
                    backend = %backend.name,
                    error = %e,
                    "Failed to add backend via hot reload"
                );
            } else {
                tracing::info!(backend = %backend.name, "Backend added via hot reload");
            }
        }

        // Handle endpoint group changes
        let new_eg_fingerprints: HashMap<String, String> = new_config
            .proxy
            .endpoint_groups
            .iter()
            .map(|eg| (eg.name.clone(), config_fingerprint_endpoint_group(eg)))
            .collect();

        let old_eg_names: HashSet<&String> = endpoint_group_fingerprints.keys().collect();
        let new_eg_names: HashSet<&String> = new_eg_fingerprints.keys().collect();

        // Remove endpoint groups that are no longer in config
        for removed in old_eg_names.difference(&new_eg_names) {
            tracing::info!(endpoint_group = %removed, "Removing endpoint group via hot reload");
            removed_egs.push(removed);
            endpoint_group_registry.remove(removed);
        }

        // Add or update endpoint groups
        for endpoint_group in &new_config.proxy.endpoint_groups {
            if endpoint_group_fingerprints.contains_key(&endpoint_group.name) {
                // Existing endpoint group -- check for modification
                let old_fp = &endpoint_group_fingerprints[&endpoint_group.name];
                let new_fp = &new_eg_fingerprints[&endpoint_group.name];

                if old_fp != new_fp {
                    tracing::info!(
                        endpoint_group = %endpoint_group.name,
                        "Endpoint group config changed, replacing via hot reload"
                    );

                    replaced_egs.push(&endpoint_group.name);
                    // Rebuild the endpoint group
                    if let Err(e) = rebuild_endpoint_group(
                        &endpoint_group_registry,
                        &shared_proxy,
                        &new_config,
                        endpoint_group,
                        lazy_registry.clone(),
                    )
                    .await
                    {
                        tracing::error!(
                            endpoint_group = %endpoint_group.name,
                            error = %e,
                            "Failed to replace endpoint group via hot reload"
                        );
                    } else {
                        tracing::info!(endpoint_group = %endpoint_group.name, "Endpoint group replaced");
                    }
                }
                continue;
            }

            added_egs.push(&endpoint_group.name);
            tracing::info!(
                name = %endpoint_group.name,
                path = %endpoint_group.path,
                "Adding new endpoint group via hot reload"
            );

            if let Err(e) = build_endpoint_group(
                &endpoint_group_registry,
                &shared_proxy,
                &new_config,
                endpoint_group,
                lazy_registry.clone(),
            )
            .await
            {
                tracing::error!(
                    endpoint_group = %endpoint_group.name,
                    error = %e,
                    "Failed to add endpoint group via hot reload"
                );
            } else {
                tracing::info!(endpoint_group = %endpoint_group.name, "Endpoint group added via hot reload");
            }
        }

        // Track whether anything actually changed
        let backends_changed = !removed_backends.is_empty()
            || !added_backends.is_empty()
            || !replaced_backends.is_empty();
        let endpoint_groups_changed =
            !removed_egs.is_empty() || !added_egs.is_empty() || !replaced_egs.is_empty();
        let anything_changed = backends_changed || endpoint_groups_changed;

        // Update fingerprints to reflect current state
        backend_fingerprints = new_fingerprints;
        endpoint_group_fingerprints = new_eg_fingerprints;

        // Rebuild the alias map when backends change.
        // The global AliasService holds a shared Arc<RwLock<AliasMap>>,
        // so updating the map via write lock immediately takes effect for
        // all subsequent requests.
        if backends_changed && let Some(ref shared_alias_map) = alias_map {
            if let Some(new_map) = crate::proxy::build_alias_map(&new_config) {
                let new_map = new_map.read().unwrap().clone();
                let mut map = shared_alias_map.write().unwrap();
                *map = new_map;
                tracing::info!("Alias map updated after backend changes");
            } else {
                // No aliases configured — clear the map
                let mut map = shared_alias_map.write().unwrap();
                map.forward.clear();
                map.reverse.clear();
                map.forward_rules.clear();
                tracing::info!("Alias map cleared (no aliases in updated config)");
            }
        }

        // Re-index discovery only when backends or endpoint groups actually changed
        #[cfg(feature = "discovery")]
        if anything_changed && let Some((ref index, ref separator)) = discovery_index {
            let mut proxy_clone = proxy.clone();
            crate::discovery::reindex(index, &mut proxy_clone, separator).await;
        }
    }
}

/// Generate a fingerprint for a backend config to detect changes.
/// Uses TOML serialization for a stable, content-based comparison.
///
/// To guarantee deterministic output, `env` (HashMap) and `default_args`
/// (serde_json::Map) are converted to sorted BTreeMap/BTreeMap before
/// serializing — HashMap/serde_json::Map have non-deterministic iteration
/// order, which would produce different TOML strings on each call even when
/// nothing changed.
fn config_fingerprint(backend: &BackendConfig) -> String {
    use std::collections::BTreeMap;

    // Sort env keys
    let sorted_env: BTreeMap<&str, &str> = backend
        .env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    // Sort default_args keys recursively
    let sorted_args: BTreeMap<&str, &serde_json::Value> = backend
        .default_args
        .iter()
        .map(|(k, v)| (k.as_str(), v))
        .collect();

    // Serialize to JSON with sorted maps, then back to serde_json::Value
    // to produce a deterministic TOML output
    let sorted_json = serde_json::json!({
        "name": backend.name,
        "enabled": backend.enabled,
        "transport": backend.transport,
        "command": backend.command,
        "args": backend.args,
        "url": backend.url,
        "env": sorted_env,
        "working_dir": backend.working_dir,
        "timeout": backend.timeout,
        "circuit_breaker": backend.circuit_breaker,
        "rate_limit": backend.rate_limit,
        "concurrency": backend.concurrency,
        "retry": backend.retry,
        "outlier_detection": backend.outlier_detection,
        "hedging": backend.hedging,
        "mirror_of": backend.mirror_of,
        "mirror_percent": backend.mirror_percent,
        "cache": backend.cache,
        "bearer_token": backend.bearer_token,
        "forward_auth": backend.forward_auth,
        "aliases": backend.aliases,
        "rename_all": backend.rename_all,
        "default_args": sorted_args,
        "inject_args": backend.inject_args,
        "param_overrides": backend.param_overrides,
        "expose_tools": backend.expose_tools,
        "hide_tools": backend.hide_tools,
        "expose_resources": backend.expose_resources,
        "hide_resources": backend.hide_resources,
        "expose_prompts": backend.expose_prompts,
        "hide_prompts": backend.hide_prompts,
        "hide_destructive": backend.hide_destructive,
        "read_only_only": backend.read_only_only,
        "failover_for": backend.failover_for,
        "priority": backend.priority,
        "canary_of": backend.canary_of,
        "weight": backend.weight,
        "endpoint_groups": backend.endpoint_groups,
    });

    // Convert serde_json::Value → TOML string for the fingerprint.
    // Filter out null values so the fingerprint stays minimal.
    json_value_to_toml_string(&sorted_json)
}

/// Generate a fingerprint for an endpoint group config to detect changes.
/// Uses TOML serialization for a stable, content-based comparison.
fn config_fingerprint_endpoint_group(eg: &crate::config::EndpointGroupConfig) -> String {
    toml::to_string(eg).unwrap_or_default()
}

/// Recursively normalize a `serde_json::Value` so that all maps have sorted
/// keys. This ensures deterministic serialization for fingerprinting.
fn normalize_json_value(val: &serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Object(map) => {
            let sorted: std::collections::BTreeMap<&str, &serde_json::Value> =
                map.iter().map(|(k, v)| (k.as_str(), v)).collect();
            serde_json::Value::Object(
                sorted
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), normalize_json_value(v)))
                    .collect(),
            )
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(normalize_json_value).collect())
        }
        other => other.clone(),
    }
}

/// Convert a `serde_json::Value` to a deterministic string for fingerprinting.
/// All maps are sorted by key, and null values are stripped.
fn json_value_to_toml_string(val: &serde_json::Value) -> String {
    let normalized = normalize_json_value(val);
    // Strip null values for a cleaner fingerprint
    let stripped = strip_nulls(&normalized);
    // JSON serialization is deterministic for BTreeMap-backed objects
    serde_json::to_string(&stripped).unwrap_or_default()
}

/// Recursively strip null values from a JSON value.
fn strip_nulls(val: &serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), strip_nulls(v)))
                .collect(),
        ),
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(strip_nulls).collect())
        }
        other => other.clone(),
    }
}

/// Build and register an endpoint group MCP proxy and router.
async fn build_endpoint_group(
    registry: &EndpointGroupRegistry,
    shared_proxy: &McpProxy,
    config: &ProxyConfig,
    endpoint_group: &crate::config::EndpointGroupConfig,
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
) -> anyhow::Result<()> {
    // Use the shared McpProxy — no duplicate process spawning.
    let group_router = crate::endpoint_router::build_single_endpoint_group(
        config,
        endpoint_group,
        Some(shared_proxy),
        lazy_registry,
    )
    .await?;

    // Register the endpoint group
    registry.insert(group_router);

    Ok(())
}

/// Rebuild an existing endpoint group (replace with new config).
async fn rebuild_endpoint_group(
    registry: &EndpointGroupRegistry,
    shared_proxy: &McpProxy,
    config: &ProxyConfig,
    endpoint_group: &crate::config::EndpointGroupConfig,
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
) -> anyhow::Result<()> {
    // Remove the old endpoint group first
    registry.remove(&endpoint_group.name);

    // Build and register the new one using the shared proxy
    build_endpoint_group(
        registry,
        shared_proxy,
        config,
        endpoint_group,
        lazy_registry,
    )
    .await
}

/// Connect and add a single backend to the proxy, including per-backend middleware.
///
/// Shared by the hot-reload path and the lazy registry's spawn path (Wave 5) so
/// per-backend middleware is applied identically (POS-004).
///
/// `kill_timeout_secs` controls SIGTERM→SIGKILL escalation for stdio backends.
pub(crate) async fn add_backend(
    proxy: &McpProxy,
    backend: &BackendConfig,
    kill_timeout_secs: u64,
) -> anyhow::Result<()> {
    // Skip disabled backends
    if !backend.enabled {
        tracing::info!(backend = %backend.name, "Skipping disabled backend");
        return Ok(());
    }

    let has_middleware = backend.timeout.is_some()
        || backend.circuit_breaker.is_some()
        || backend.rate_limit.is_some()
        || backend.concurrency.is_some()
        || backend.retry.is_some()
        || backend.hedging.is_some()
        || backend.outlier_detection.is_some();

    match backend.transport {
        TransportType::Stdio => {
            let kill_timeout = std::time::Duration::from_secs(kill_timeout_secs);
            let transport =
                crate::stdio_spawn::spawn_stdio_transport(backend, kill_timeout).await?;

            if has_middleware {
                let layer = build_backend_layer(backend);
                proxy
                    .add_backend_with_layer(&backend.name, transport, layer)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
            } else {
                proxy
                    .add_backend(&backend.name, transport)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
            }
        }
        TransportType::Http => {
            let url = backend
                .url
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("http backend requires 'url'"))?;
            let mut transport = tower_mcp::client::HttpClientTransport::new(url);
            if let Some(token) = &backend.bearer_token {
                transport = transport.bearer_token(token);
            }

            if has_middleware {
                let layer = build_backend_layer(backend);
                proxy
                    .add_backend_with_layer(&backend.name, transport, layer)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
            } else {
                proxy
                    .add_backend(&backend.name, transport)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
            }
        }
        #[cfg(feature = "websocket")]
        TransportType::Websocket => {
            let url = backend
                .url
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("websocket backend requires 'url'"))?;
            let transport = if let Some(token) = &backend.bearer_token {
                crate::ws_transport::WebSocketClientTransport::connect_with_bearer_token(
                    url,
                    token,
                    backend.protocol_version.as_deref(),
                )
                .await?
            } else {
                crate::ws_transport::WebSocketClientTransport::connect_with_protocol_version(
                    url,
                    backend.protocol_version.as_deref(),
                )
                .await?
            };

            if has_middleware {
                let layer = build_backend_layer(backend);
                proxy
                    .add_backend_with_layer(&backend.name, transport, layer)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
            } else {
                proxy
                    .add_backend(&backend.name, transport)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
            }
        }
        #[cfg(not(feature = "websocket"))]
        TransportType::Websocket => {
            anyhow::bail!(
                "WebSocket transport requires the 'websocket' feature.                  Rebuild with: cargo install mcp-proxy --features websocket"
            );
        }
    }

    if has_middleware {
        tracing::info!(
            backend = %backend.name,
            timeout = backend.timeout.is_some(),
            circuit_breaker = backend.circuit_breaker.is_some(),
            rate_limit = backend.rate_limit.is_some(),
            concurrency = backend.concurrency.is_some(),
            "Per-backend middleware applied to hot-reloaded backend"
        );
    }

    Ok(())
}

/// A type-erasing layer that builds the full per-backend middleware stack.
///
/// Uses `BoxCloneService` to erase the composed middleware types, allowing
/// arbitrary combinations of optional layers.
struct BackendMiddlewareLayer {
    build_fn: Box<
        dyn Fn(
                BackendService,
            )
                -> BoxCloneService<tower_mcp::RouterRequest, tower_mcp::RouterResponse, Infallible>
            + Send,
    >,
}

impl tower::Layer<BackendService> for BackendMiddlewareLayer {
    type Service = BoxCloneService<tower_mcp::RouterRequest, tower_mcp::RouterResponse, Infallible>;

    fn layer(&self, inner: BackendService) -> Self::Service {
        (self.build_fn)(inner)
    }
}

/// Build a type-erased layer for per-backend middleware from config.
///
/// Layers are applied inner to outer:
/// retry -> concurrency -> rate limit -> timeout -> circuit breaker -> outlier detection.
fn build_backend_layer(backend: &BackendConfig) -> BackendMiddlewareLayer {
    let retry_config = backend.retry.clone();
    let concurrency = backend.concurrency.as_ref().map(|cc| cc.max_concurrent);
    let rate_limit = backend
        .rate_limit
        .as_ref()
        .map(|rl| (rl.requests, rl.period_seconds));
    let timeout_secs = backend.timeout.as_ref().map(|t| t.seconds);
    let circuit_breaker = backend.circuit_breaker.as_ref().map(|cb| {
        (
            cb.failure_rate_threshold,
            cb.minimum_calls,
            cb.wait_duration_seconds,
            cb.permitted_calls_in_half_open,
        )
    });
    let hedging = backend.hedging.clone();
    let outlier = backend.outlier_detection.clone();
    let name = backend.name.clone();

    BackendMiddlewareLayer {
        build_fn: Box::new(move |inner: BackendService| {
            let mut svc: BoxCloneService<
                tower_mcp::RouterRequest,
                tower_mcp::RouterResponse,
                Infallible,
            > = BoxCloneService::new(inner);

            // Retry (innermost)
            if let Some(ref retry_cfg) = retry_config {
                let layer = crate::retry::build_retry_layer(retry_cfg, &name);
                let retried = tower::Layer::layer(&layer, svc);
                svc = BoxCloneService::new(retried);
            }

            // Hedging
            if let Some(ref hedge_cfg) = hedging {
                let delay = Duration::from_millis(hedge_cfg.delay_ms);
                let max_attempts = hedge_cfg.max_hedges + 1;
                let layer = if delay.is_zero() {
                    tower_resilience::hedge::HedgeLayer::builder()
                        .no_delay()
                        .max_hedged_attempts(max_attempts)
                        .name(format!("{}-hedge", name))
                        .build()
                } else {
                    tower_resilience::hedge::HedgeLayer::builder()
                        .delay(delay)
                        .max_hedged_attempts(max_attempts)
                        .name(format!("{}-hedge", name))
                        .build()
                };
                let hedged = tower::Layer::layer(&layer, svc);
                svc = BoxCloneService::new(tower_mcp::CatchError::new(hedged));
            }

            // Concurrency limit
            if let Some(max) = concurrency {
                let limited =
                    tower::Layer::layer(&tower::limit::ConcurrencyLimitLayer::new(max), svc);
                svc = BoxCloneService::new(tower_mcp::CatchError::new(limited));
            }

            // Rate limit
            if let Some((requests, period_seconds)) = rate_limit {
                let layer = tower_resilience::ratelimiter::RateLimiterLayer::builder()
                    .limit_for_period(requests)
                    .refresh_period(Duration::from_secs(period_seconds))
                    .name(format!("{}-ratelimit", name))
                    .build()
                    .expect("failed to build rate limiter layer");
                let limited = tower::Layer::layer(&layer, svc);
                svc = BoxCloneService::new(tower_mcp::CatchError::new(limited));
            }

            // Timeout
            if let Some(seconds) = timeout_secs {
                let limited = tower::Layer::layer(
                    &tower::timeout::TimeoutLayer::new(Duration::from_secs(seconds)),
                    svc,
                );
                svc = BoxCloneService::new(tower_mcp::CatchError::new(limited));
            }

            // Circuit breaker
            if let Some((failure_rate, min_calls, wait_secs, half_open)) = circuit_breaker {
                let layer = tower_resilience::circuitbreaker::CircuitBreakerLayer::builder()
                    .failure_rate_threshold(failure_rate)
                    .minimum_number_of_calls(min_calls)
                    .wait_duration_in_open(Duration::from_secs(wait_secs))
                    .permitted_calls_in_half_open(half_open)
                    .name(format!("{}-cb", name))
                    .build()
                    .expect("failed to build circuit breaker layer");
                let limited = tower::Layer::layer(&layer, svc);
                svc = BoxCloneService::new(tower_mcp::CatchError::new(limited));
            }

            // Outlier detection (outermost)
            if let Some(ref od_config) = outlier {
                // Hot-reloaded backends get their own detector (single-backend scope).
                // The main proxy build path uses a shared detector across all backends.
                let detector = crate::outlier::OutlierDetector::new(od_config.max_ejection_percent);
                let layer = crate::outlier::OutlierDetectionLayer::new(
                    name.clone(),
                    od_config.clone(),
                    detector,
                );
                let od_svc = tower::Layer::layer(&layer, svc);
                svc = BoxCloneService::new(od_svc);
            }

            svc
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_backend(name: &str, url: &str) -> BackendConfig {
        // Parse from TOML to get all default values automatically
        let toml = format!(
            r#"
            name = "{name}"
            transport = "http"
            url = "{url}"
            "#,
        );
        toml::from_str(&toml).unwrap()
    }

    #[test]
    fn test_config_fingerprint_stable() {
        let backend = http_backend("api", "http://localhost:8080");
        let fp1 = config_fingerprint(&backend);
        let fp2 = config_fingerprint(&backend);
        assert_eq!(fp1, fp2, "fingerprint should be stable across calls");
    }

    #[test]
    fn test_config_fingerprint_differs_on_url_change() {
        let b1 = http_backend("api", "http://localhost:8080");
        let b2 = http_backend("api", "http://localhost:9090");
        assert_ne!(
            config_fingerprint(&b1),
            config_fingerprint(&b2),
            "different URLs should produce different fingerprints"
        );
    }

    #[test]
    fn test_config_fingerprint_differs_on_name_change() {
        let b1 = http_backend("api", "http://localhost:8080");
        let b2 = http_backend("api2", "http://localhost:8080");
        assert_ne!(
            config_fingerprint(&b1),
            config_fingerprint(&b2),
            "different names should produce different fingerprints"
        );
    }

    #[test]
    fn test_config_fingerprint_differs_on_transport_change() {
        let b1 = http_backend("api", "http://localhost:8080");
        let b2: BackendConfig = toml::from_str(
            r#"
            name = "api"
            transport = "stdio"
            command = "echo"
            "#,
        )
        .unwrap();
        assert_ne!(
            config_fingerprint(&b1),
            config_fingerprint(&b2),
            "different transports should produce different fingerprints"
        );
    }

    #[test]
    fn test_config_fingerprint_differs_with_timeout() {
        let b1 = http_backend("api", "http://localhost:8080");
        let b2: BackendConfig = toml::from_str(
            r#"
            name = "api"
            transport = "http"
            url = "http://localhost:8080"
            [timeout]
            seconds = 30
            "#,
        )
        .unwrap();
        assert_ne!(
            config_fingerprint(&b1),
            config_fingerprint(&b2),
            "adding a timeout should change the fingerprint"
        );
    }

    #[test]
    fn test_fingerprint_map_detects_additions_and_removals() {
        let backends_v1 = [
            http_backend("api", "http://api:8080"),
            http_backend("db", "http://db:5432"),
        ];
        let backends_v2 = [
            http_backend("api", "http://api:8080"),
            http_backend("cache", "http://cache:6379"),
        ];

        let fp_v1: HashMap<String, String> = backends_v1
            .iter()
            .map(|b| (b.name.clone(), config_fingerprint(b)))
            .collect();
        let fp_v2: HashMap<String, String> = backends_v2
            .iter()
            .map(|b| (b.name.clone(), config_fingerprint(b)))
            .collect();

        let removed: HashSet<_> = fp_v1.keys().filter(|k| !fp_v2.contains_key(*k)).collect();
        let added: HashSet<_> = fp_v2.keys().filter(|k| !fp_v1.contains_key(*k)).collect();

        assert_eq!(removed.len(), 1);
        assert!(removed.contains(&"db".to_string()));
        assert_eq!(added.len(), 1);
        assert!(added.contains(&"cache".to_string()));
    }

    #[test]
    fn test_config_fingerprint_differs_on_enabled_toggle() {
        let b1: BackendConfig = toml::from_str(
            r#"
            name = "api"
            transport = "http"
            url = "http://localhost:8080"
            enabled = false
            "#,
        )
        .unwrap();
        let b2: BackendConfig = toml::from_str(
            r#"
            name = "api"
            transport = "http"
            url = "http://localhost:8080"
            enabled = true
            "#,
        )
        .unwrap();
        assert_ne!(
            config_fingerprint(&b1),
            config_fingerprint(&b2),
            "toggling enabled should produce different fingerprints"
        );
    }

    /// Regression test: fingerprint must be deterministic despite HashMap
    /// iteration order. Two configs with the same env and default_args
    /// must always produce identical fingerprints regardless of internal
    /// iteration order.
    #[test]
    fn test_fingerprint_deterministic_with_hashmap_fields() {
        let mut env1 = std::collections::HashMap::new();
        env1.insert("API_KEY".to_string(), "secret1".to_string());
        env1.insert("BASE_URL".to_string(), "http://api".to_string());

        // Re-insert in different order to simulate different iteration state
        let mut fresh_env = std::collections::HashMap::new();
        fresh_env.insert("BASE_URL".to_string(), "http://api".to_string());
        fresh_env.insert("API_KEY".to_string(), "secret1".to_string());

        let mut default_args1 = serde_json::Map::new();
        default_args1.insert("temperature".to_string(), serde_json::json!(0.7));
        default_args1.insert("max_tokens".to_string(), serde_json::json!(4096));

        let mut default_args2 = serde_json::Map::new();
        default_args2.insert("max_tokens".to_string(), serde_json::json!(4096));
        default_args2.insert("temperature".to_string(), serde_json::json!(0.7));

        let mut b1 = http_backend("api", "http://api:8080");
        b1.env = env1;
        b1.default_args = default_args1;

        let mut b2 = http_backend("api", "http://api:8080");
        b2.env = fresh_env;
        b2.default_args = default_args2;

        let fp1 = config_fingerprint(&b1);
        let fp2 = config_fingerprint(&b2);
        assert_eq!(
            fp1, fp2,
            "fingerprint must be deterministic for identical configs"
        );

        // Same config called twice must also be stable
        let fp3 = config_fingerprint(&b1);
        assert_eq!(fp1, fp3, "fingerprint must be stable across multiple calls");
    }
}
