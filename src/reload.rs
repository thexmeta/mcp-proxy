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
///
/// Returns the watcher's [`tokio::task::JoinHandle`] so the caller (e.g.
/// [`crate::proxy::Proxy::serve`] shutdown) can stop the watcher before
/// drain/snapshot and avoid racing teardown.
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
        crate::discovery::SchemaStore,
        String,
    )>,
) -> tokio::task::JoinHandle<()> {
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
    })
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
        crate::discovery::SchemaStore,
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
    //
    // The snapshot also keeps each backend's OLD config so removals can
    // terminate the old child per its effective kill mode (baked at spawn);
    // the new config no longer contains removed entries.
    let mut backend_snapshots: HashMap<String, BackendConfig> = {
        if let Ok(mut config) = ProxyConfig::load(&config_path) {
            config.resolve_env_vars();
            config
                .backends
                .iter()
                .map(|b| (b.name.clone(), b.clone()))
                .collect()
        } else {
            HashMap::new()
        }
    };
    let mut backend_fingerprints: HashMap<String, String> = backend_snapshots
        .iter()
        .map(|(name, b)| (name.clone(), config_fingerprint(b)))
        .collect();
    // Global kill settings effective when the snapshot was taken (used as the
    // OLD mode for removals/replaces).
    let (mut old_kill_timeout_secs, mut old_global_force_kill): (u64, bool) =
        match ProxyConfig::load(&config_path) {
            Ok(c) => (c.proxy.shutdown_kill_timeout_secs, c.proxy.force_kill),
            Err(_) => (2, false),
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
        new_config.apply_global_defaults();

        let kill_timeout_secs = new_config.proxy.shutdown_kill_timeout_secs;
        let global_force_kill = new_config.proxy.force_kill;

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
            let old_cfg = backend_snapshots.get(*removed);
            let (force_immediate, kill_timeout) = removal_kill_mode(
                old_cfg,
                old_global_force_kill,
                old_kill_timeout_secs,
                global_force_kill,
                kill_timeout_secs,
            );
            if old_cfg.is_none() {
                tracing::warn!(
                    backend = %removed,
                    "No snapshot of removed backend; falling back to new global kill mode"
                );
            }
            tracing::info!(
                backend = %removed,
                force_kill = force_immediate,
                kill_timeout_ms = kill_timeout.as_millis() as u64,
                "Removing backend via hot reload"
            );
            removed_backends.push(removed);
            if proxy.remove_backend(removed).await {
                tracing::info!(backend = %removed, "Backend removed");
            } else {
                tracing::warn!(backend = %removed, "Backend not found for removal");
            }
            // FR-009: drop any lazy-registry entry AFTER the child is gone
            // (`unregister` does not kill; see its ordering contract).
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
                            let old_cfg = backend_snapshots.get(&backend.name);
                            let (force_immediate, kill_timeout) = removal_kill_mode(
                                old_cfg,
                                old_global_force_kill,
                                old_kill_timeout_secs,
                                global_force_kill,
                                kill_timeout_secs,
                            );
                            if old_cfg.is_none() {
                                tracing::warn!(
                                    backend = %backend.name,
                                    "No snapshot of replaced backend; falling back to new global kill mode"
                                );
                            }
                            tracing::info!(
                                backend = %backend.name,
                                force_kill = force_immediate,
                                kill_timeout_ms = kill_timeout.as_millis() as u64,
                                "Flipping backend eager -> lazy via hot reload (terminating old child)"
                            );
                            proxy.remove_backend(&backend.name).await;
                            lazy_registry.register_lazy(backend.clone());
                            tracing::info!(backend = %backend.name, "Backend flipped eager -> lazy (registered, not spawned)");
                        }
                        // lazy -> eager: drop lazy registration, eager spawn.
                        (crate::config::SpawnMode::Lazy, crate::config::SpawnMode::Eager) => {
                            lazy_registry.unregister(&backend.name);
                            if let Err(e) =
                                add_backend(&proxy, backend, kill_timeout_secs, global_force_kill)
                                    .await
                            {
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
                        // both eager: existing behavior — remove old (per old mode), add new (per new mode).
                        (crate::config::SpawnMode::Eager, crate::config::SpawnMode::Eager) => {
                            let (force_immediate, kill_timeout) = removal_kill_mode(
                                backend_snapshots.get(&backend.name),
                                old_global_force_kill,
                                old_kill_timeout_secs,
                                global_force_kill,
                                kill_timeout_secs,
                            );
                            tracing::info!(
                                backend = %backend.name,
                                force_kill = force_immediate,
                                kill_timeout_ms = kill_timeout.as_millis() as u64,
                                "Replacing backend via hot reload (terminating old child)"
                            );
                            proxy.remove_backend(&backend.name).await;
                            if let Err(e) =
                                add_backend(&proxy, backend, kill_timeout_secs, global_force_kill)
                                    .await
                            {
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
                            let (force_immediate, kill_timeout) = removal_kill_mode(
                                backend_snapshots.get(&backend.name),
                                old_global_force_kill,
                                old_kill_timeout_secs,
                                global_force_kill,
                                kill_timeout_secs,
                            );
                            tracing::info!(
                                backend = %backend.name,
                                force_kill = force_immediate,
                                kill_timeout_ms = kill_timeout.as_millis() as u64,
                                "Replacing backend via hot reload (terminating old child)"
                            );
                            proxy.remove_backend(&backend.name).await;
                            if let Err(e) =
                                add_backend(&proxy, backend, kill_timeout_secs, global_force_kill)
                                    .await
                            {
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
            } else if let Err(e) =
                add_backend(&proxy, backend, kill_timeout_secs, global_force_kill).await
            {
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
        // Only the discovery re-index reads this.
        #[cfg_attr(not(feature = "discovery"), allow(unused_variables))]
        let anything_changed = backends_changed || endpoint_groups_changed;

        // Update fingerprints and snapshots to reflect current state
        backend_fingerprints = new_fingerprints;
        backend_snapshots = new_config
            .backends
            .iter()
            .map(|b| (b.name.clone(), b.clone()))
            .collect();
        old_kill_timeout_secs = kill_timeout_secs;
        old_global_force_kill = global_force_kill;
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
        if anything_changed && let Some((ref index, ref schemas, ref separator)) = discovery_index {
            let mut proxy_clone = proxy.clone();
            crate::discovery::reindex_with_schemas(index, schemas, &mut proxy_clone, separator)
                .await;
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

    // Build the fingerprint object incrementally (avoids `json!` macro
    // recursion limits as the field count grows).
    let mut obj = serde_json::Map::new();
    let mut put = |key: &str, val: serde_json::Value| {
        obj.insert(key.to_string(), val);
    };
    put(
        "name",
        serde_json::to_value(&backend.name).unwrap_or_default(),
    );
    put(
        "enabled",
        serde_json::to_value(backend.enabled).unwrap_or_default(),
    );
    put(
        "transport",
        serde_json::to_value(&backend.transport).unwrap_or_default(),
    );
    put(
        "command",
        serde_json::to_value(&backend.command).unwrap_or_default(),
    );
    put(
        "args",
        serde_json::to_value(&backend.args).unwrap_or_default(),
    );
    put(
        "url",
        serde_json::to_value(&backend.url).unwrap_or_default(),
    );
    put("env", serde_json::to_value(&sorted_env).unwrap_or_default());
    put(
        "working_dir",
        serde_json::to_value(&backend.working_dir).unwrap_or_default(),
    );
    put(
        "timeout",
        serde_json::to_value(&backend.timeout).unwrap_or_default(),
    );
    put(
        "circuit_breaker",
        serde_json::to_value(&backend.circuit_breaker).unwrap_or_default(),
    );
    put(
        "rate_limit",
        serde_json::to_value(&backend.rate_limit).unwrap_or_default(),
    );
    put(
        "concurrency",
        serde_json::to_value(&backend.concurrency).unwrap_or_default(),
    );
    put(
        "retry",
        serde_json::to_value(&backend.retry).unwrap_or_default(),
    );
    put(
        "outlier_detection",
        serde_json::to_value(&backend.outlier_detection).unwrap_or_default(),
    );
    put(
        "hedging",
        serde_json::to_value(&backend.hedging).unwrap_or_default(),
    );
    put(
        "mirror_of",
        serde_json::to_value(&backend.mirror_of).unwrap_or_default(),
    );
    put(
        "mirror_percent",
        serde_json::to_value(backend.mirror_percent).unwrap_or_default(),
    );
    put(
        "cache",
        serde_json::to_value(&backend.cache).unwrap_or_default(),
    );
    put(
        "bearer_token",
        serde_json::to_value(&backend.bearer_token).unwrap_or_default(),
    );
    put(
        "forward_auth",
        serde_json::to_value(backend.forward_auth).unwrap_or_default(),
    );
    put(
        "aliases",
        serde_json::to_value(&backend.aliases).unwrap_or_default(),
    );
    put(
        "rename_all",
        serde_json::to_value(&backend.rename_all).unwrap_or_default(),
    );
    put(
        "default_args",
        serde_json::to_value(&sorted_args).unwrap_or_default(),
    );
    put(
        "inject_args",
        serde_json::to_value(&backend.inject_args).unwrap_or_default(),
    );
    put(
        "param_overrides",
        serde_json::to_value(&backend.param_overrides).unwrap_or_default(),
    );
    put(
        "expose_tools",
        serde_json::to_value(&backend.expose_tools).unwrap_or_default(),
    );
    put(
        "hide_tools",
        serde_json::to_value(&backend.hide_tools).unwrap_or_default(),
    );
    put(
        "expose_resources",
        serde_json::to_value(&backend.expose_resources).unwrap_or_default(),
    );
    put(
        "hide_resources",
        serde_json::to_value(&backend.hide_resources).unwrap_or_default(),
    );
    put(
        "expose_prompts",
        serde_json::to_value(&backend.expose_prompts).unwrap_or_default(),
    );
    put(
        "hide_prompts",
        serde_json::to_value(&backend.hide_prompts).unwrap_or_default(),
    );
    put(
        "hide_destructive",
        serde_json::to_value(backend.hide_destructive).unwrap_or_default(),
    );
    put(
        "read_only_only",
        serde_json::to_value(backend.read_only_only).unwrap_or_default(),
    );
    put(
        "failover_for",
        serde_json::to_value(&backend.failover_for).unwrap_or_default(),
    );
    put(
        "priority",
        serde_json::to_value(backend.priority).unwrap_or_default(),
    );
    put(
        "canary_of",
        serde_json::to_value(&backend.canary_of).unwrap_or_default(),
    );
    put(
        "weight",
        serde_json::to_value(backend.weight).unwrap_or_default(),
    );
    put(
        "endpoint_groups",
        serde_json::to_value(&backend.endpoint_groups).unwrap_or_default(),
    );
    put(
        "tool_groups",
        serde_json::to_value(&backend.tool_groups).unwrap_or_default(),
    );
    put(
        "force_kill",
        serde_json::to_value(backend.force_kill).unwrap_or_default(),
    );
    put(
        "singleton_externally_managed",
        serde_json::to_value(backend.singleton_externally_managed).unwrap_or_default(),
    );
    put(
        "singleton_stop_hint",
        serde_json::to_value(&backend.singleton_stop_hint).unwrap_or_default(),
    );
    let sorted_json = serde_json::Value::Object(obj);

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

/// Marker embedded in enriched spawn-conflict errors so downstream layers
/// (lazy spawn → JSON-RPC mapping) can distinguish conflicts from timeouts
/// without re-measuring elapsed.
///
/// A spawn conflict means the stdio child exited immediately during the MCP
/// `initialize` handshake (e.g. a foreign singleton daemon holds the slot).
/// The proxy never kills foreign processes; the error carries an advisory
/// stop hint instead.
pub(crate) const SPAWN_CONFLICT_MARKER: &str = "spawn conflict";

/// Text tower-mcp puts in an `initialize` error when the server answered with
/// a protocol version the client does not accept.
pub(crate) const PROTOCOL_REJECTION_TEXT: &str = "server selected protocol version";

/// Fast-fail spawn-conflict classifier.
///
/// Returns true only when ALL hold:
/// - the error text carries the fast-fail signal (`Connection closed`). A
///   handshake the child answered, for example with an unsupported protocol
///   version or a JSON-RPC error, is not an immediate exit and does not
///   count, AND
/// - the error is NOT a timeout (`timed out` / `timeout` anywhere), AND
/// - `elapsed < init_timeout` (when a bound is configured; `None` means the
///   message signal alone decides).
///
/// Timeouts keep their existing timeout error text and are never classified
/// as conflicts.
pub(crate) fn is_fast_spawn_conflict(
    err_msg: &str,
    elapsed: std::time::Duration,
    init_timeout: Option<std::time::Duration>,
) -> bool {
    let lower = err_msg.to_lowercase();
    // Timeouts are never conflicts — they keep their existing error text.
    if lower.contains("timed out") || lower.contains("timeout") {
        return false;
    }
    let signal = lower.contains("connection closed");
    if !signal {
        return false;
    }
    match init_timeout {
        Some(bound) => elapsed < bound,
        None => true,
    }
}

/// Advisory hint text for spawn-conflict errors.
///
/// Flagged externally-managed singletons get their configured stop command;
/// unflagged backends get generic advisory wording only. Never includes a
/// PID — holder PIDs are surfaced only when actually known, and no
/// proc-table scan is performed.
pub(crate) fn spawn_conflict_hint(backend: &BackendConfig) -> String {
    if let Some(hint) = backend.singleton_stop_hint_resolved() {
        format!(
            "Backend '{}' exited immediately during initialize (possible singleton conflict: {}). \
             The proxy never kills foreign processes. Advisory stop command: {}",
            backend.name, SPAWN_CONFLICT_MARKER, hint
        )
    } else {
        format!(
            "Backend '{}' exited immediately during initialize ({}). Check its stderr in the \
             proxy log. The proxy never kills foreign processes.",
            backend.name, SPAWN_CONFLICT_MARKER
        )
    }
}

/// Structured JSON-RPC error `data` for spawn conflicts.
///
/// Shape: `{"backend": <name>, "conflict": true, "hint": <advisory text>}`.
/// The holder PID is included only when actually known — it never is today
/// (no proc-table scan), so the field is omitted.
pub(crate) fn spawn_conflict_data(backend: &BackendConfig) -> serde_json::Value {
    serde_json::json!({
        "backend": backend.name,
        "conflict": true,
        "hint": spawn_conflict_hint(backend),
    })
}

/// Enrich an initialize error with spawn-conflict context when it classifies
/// as a fast fail; otherwise return it unchanged.
///
/// On conflict: logs backend + hint (never signalling any foreign PID) and
/// returns an error carrying the backend name, the fast-fail signal, and the
/// advisory hint. On timeouts / other errors: returns the original error
/// untouched so existing error text is preserved.
pub(crate) fn map_init_error(
    backend: &BackendConfig,
    err: impl std::fmt::Display,
    elapsed: std::time::Duration,
    init_timeout: Option<std::time::Duration>,
) -> anyhow::Error {
    let raw = err.to_string();
    if raw.contains(PROTOCOL_REJECTION_TEXT) {
        tracing::warn!(
            backend = %backend.name,
            error = %raw,
            "stdio backend answered initialize with a protocol version the proxy does not support"
        );
        return anyhow::anyhow!(
            "backend '{}' protocol version not supported: {}",
            backend.name,
            raw
        );
    }
    if is_fast_spawn_conflict(&raw, elapsed, init_timeout) {
        let hint = spawn_conflict_hint(backend);
        tracing::warn!(
            backend = %backend.name,
            error = %raw,
            hint = %hint,
            "stdio backend fast initialize failure ({}); proxy never kills foreign processes",
            SPAWN_CONFLICT_MARKER
        );
        anyhow::anyhow!(
            "backend '{}' {} (fast initialize failure): {}. {}",
            backend.name,
            SPAWN_CONFLICT_MARKER,
            raw,
            hint
        )
    } else {
        anyhow::anyhow!("{}", raw)
    }
}

/// Resolve the kill mode for terminating a hot-reload-removed backend's child.
///
/// Kill mode is baked into the transport at spawn time (graceful SIGTERM
/// escalation vs SIGKILL-immediate), so the OLD backend config decides the
/// mode: the new config no longer contains the entry. A missing old config
/// (unknown at removal time) falls back to the NEW global timeout; the caller
/// logs a warning in that case.
///
/// Returns `(force_immediate, kill_timeout)`: `force_immediate` is true when
/// the old effective mode (old global OR old per-backend `force_kill`) — or,
/// on fallback, the new global `force_kill` — demands SIGKILL-immediate;
/// `kill_timeout` is the `Duration` to log (ZERO = immediate).
fn removal_kill_mode(
    old_backend: Option<&BackendConfig>,
    old_global_force_kill: bool,
    old_kill_timeout_secs: u64,
    new_global_force_kill: bool,
    new_kill_timeout_secs: u64,
) -> (bool, Duration) {
    match old_backend {
        Some(old) => {
            let timeout = old.effective_kill_timeout(old_global_force_kill, old_kill_timeout_secs);
            (timeout.is_zero(), timeout)
        }
        None => {
            // New global `force_kill` means ZERO (SIGKILL immediate); the raw
            // `shutdown_kill_timeout_secs` no longer reflects the effective
            // mode because `apply_global_defaults` zeroes it — but the config
            // may not have passed through it, so resolve explicitly.
            if new_global_force_kill {
                (true, Duration::ZERO)
            } else {
                let timeout = Duration::from_secs(new_kill_timeout_secs);
                (timeout.is_zero(), timeout)
            }
        }
    }
}

/// Connect and add a single backend to the proxy, including per-backend middleware.
///
/// Shared by the hot-reload path and the lazy registry's spawn path (Wave 5) so
/// per-backend middleware is applied identically (POS-004).
///
/// `kill_timeout_secs` controls SIGTERM→SIGKILL escalation for stdio backends.
/// `global_force_kill` (from `proxy.force_kill`) ORs with each backend's own
/// `force_kill` to resolve the effective spawn-time kill mode.
pub(crate) async fn add_backend(
    proxy: &McpProxy,
    backend: &BackendConfig,
    kill_timeout_secs: u64,
    global_force_kill: bool,
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

    // Per-backend init_timeout (already resolved by apply_global_defaults)
    let init_timeout = backend.init_timeout.map(std::time::Duration::from_secs);

    match backend.transport {
        TransportType::Stdio => {
            let kill_timeout = backend.effective_kill_timeout(global_force_kill, kill_timeout_secs);
            let transport =
                crate::stdio_spawn::spawn_stdio_transport(backend, kill_timeout).await?;

            // Fast-fail clock: started after transport spawn so only the
            // initialize handshake is timed against init_timeout.
            let init_start = std::time::Instant::now();
            if has_middleware {
                let layer = build_backend_layer(backend)?;
                proxy
                    .add_backend_with_layer_and_timeout(
                        &backend.name,
                        transport,
                        layer,
                        init_timeout,
                    )
                    .await
                    .map_err(|e| map_init_error(backend, e, init_start.elapsed(), init_timeout))?;
            } else {
                proxy
                    .add_backend_with_timeout(&backend.name, transport, init_timeout)
                    .await
                    .map_err(|e| map_init_error(backend, e, init_start.elapsed(), init_timeout))?;
            }
        }
        TransportType::Http => {
            let url = backend
                .url
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("http backend requires 'url'"))?;
            let transport = crate::proxy::build_http_transport(url, backend)?;

            if has_middleware {
                let layer = build_backend_layer(backend)?;
                proxy
                    .add_backend_with_layer_and_timeout(
                        &backend.name,
                        transport,
                        layer,
                        init_timeout,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
            } else {
                proxy
                    .add_backend_with_timeout(&backend.name, transport, init_timeout)
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
            let transport = crate::ws_transport::WebSocketClientTransport::connect_with_headers(
                url,
                backend.bearer_token.as_deref(),
                &backend.headers,
                backend.protocol_version.as_deref(),
            )
            .await?;

            if has_middleware {
                let layer = build_backend_layer(backend)?;
                proxy
                    .add_backend_with_layer_and_timeout(
                        &backend.name,
                        transport,
                        layer,
                        init_timeout,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
            } else {
                proxy
                    .add_backend_with_timeout(&backend.name, transport, init_timeout)
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
            init_timeout = backend.init_timeout.is_some(),
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
fn build_backend_layer(backend: &BackendConfig) -> anyhow::Result<BackendMiddlewareLayer> {
    let retry_config = backend.retry.clone();
    let concurrency = backend.concurrency.as_ref().map(|cc| cc.max_concurrent);
    let rate_limit = backend
        .rate_limit
        .as_ref()
        .map(|rl| {
            tower_resilience::ratelimiter::RateLimiterLayer::builder()
                .limit_for_period(rl.requests)
                .refresh_period(Duration::from_secs(rl.period_seconds))
                .name(format!("{}-ratelimit", backend.name))
                .build()
        })
        .transpose()?;
    let timeout_secs = backend.timeout.as_ref().map(|t| t.seconds);
    let circuit_breaker = backend
        .circuit_breaker
        .as_ref()
        .map(|cb| {
            tower_resilience::circuitbreaker::CircuitBreakerLayer::builder()
                .failure_rate_threshold(cb.failure_rate_threshold)
                .minimum_number_of_calls(cb.minimum_calls)
                .sliding_window_size(cb.minimum_calls)
                .wait_duration_in_open(Duration::from_secs(cb.wait_duration_seconds))
                .permitted_calls_in_half_open(cb.permitted_calls_in_half_open)
                .name(format!("{}-cb", backend.name))
                .build()
        })
        .transpose()?;
    let hedging = backend.hedging.clone();
    let outlier = backend.outlier_detection.clone();
    let name = backend.name.clone();

    Ok(BackendMiddlewareLayer {
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
            if let Some(ref layer) = rate_limit {
                let limited = tower::Layer::layer(layer, svc);
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
            if let Some(ref layer) = circuit_breaker {
                let limited = tower::Layer::layer(layer, svc);
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SpawnMode;

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
    fn invalid_reload_middleware_returns_error() {
        let mut backend = http_backend("api", "http://localhost:1");
        backend.rate_limit = Some(crate::config::RateLimitConfig {
            requests: 1,
            period_seconds: 0,
        });
        assert!(build_backend_layer(&backend).is_err());
        backend.rate_limit = None;
        backend.circuit_breaker = Some(toml::from_str("minimum_calls = 0").unwrap());
        assert!(build_backend_layer(&backend).is_err());
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
    fn test_config_fingerprint_differs_on_force_kill() {
        let b1: BackendConfig = toml::from_str(
            r#"
            name = "echo"
            transport = "stdio"
            command = "echo"
            "#,
        )
        .unwrap();
        let b2: BackendConfig = toml::from_str(
            r#"
            name = "echo"
            transport = "stdio"
            command = "echo"
            force_kill = true
            "#,
        )
        .unwrap();
        assert!(
            !b1.force_kill,
            "per-backend force_kill must default to false"
        );
        assert!(b2.force_kill);
        assert_ne!(
            config_fingerprint(&b1),
            config_fingerprint(&b2),
            "toggling force_kill should change the fingerprint"
        );
    }

    #[test]
    fn test_config_fingerprint_differs_on_singleton_policy() {
        let b1: BackendConfig = toml::from_str(
            r#"
            name = "codebase"
            transport = "stdio"
            command = "/usr/bin/codebase-memory-mcp"
            "#,
        )
        .unwrap();
        let b2: BackendConfig = toml::from_str(
            r#"
            name = "codebase"
            transport = "stdio"
            command = "/usr/bin/codebase-memory-mcp"
            singleton_externally_managed = true
            singleton_stop_hint = "systemctl --user stop codebase-memory"
            "#,
        )
        .unwrap();
        assert_ne!(
            config_fingerprint(&b1),
            config_fingerprint(&b2),
            "flagging a backend externally-managed should change the fingerprint"
        );

        let b3: BackendConfig = toml::from_str(
            r#"
            name = "codebase"
            transport = "stdio"
            command = "/usr/bin/codebase-memory-mcp"
            singleton_externally_managed = true
            singleton_stop_hint = "systemctl --user stop other-unit"
            "#,
        )
        .unwrap();
        assert_ne!(
            config_fingerprint(&b2),
            config_fingerprint(&b3),
            "changing the stop hint should change the fingerprint"
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

    // ------------------------------------------------------------------
    // Regression: hot-reload must call apply_global_defaults() so that
    // init_timeout is resolved for Stdio backends (30s default).
    // Without this, backend.initialize() has no timeout and hangs forever.
    // ------------------------------------------------------------------

    #[test]
    fn test_hotreload_stdio_backend_gets_init_timeout_via_apply_global_defaults() {
        // Simulate a hot-reloaded ProxyConfig where the Stdio backend has no
        // explicit init_timeout. After apply_global_defaults(), it must get
        // the transport-specific default (30s for Stdio).
        let mut config: ProxyConfig = toml::from_str(
            r#"
            [proxy]
            name = "test"
            version = "1.0"

            [proxy.listen]
            host = "127.0.0.1"
            port = 8080

            [[backends]]
            name = "lazy-stdio"
            transport = "stdio"
            command = "echo"
            spawn_mode = "lazy"
            "#,
        )
        .unwrap();

        // Before apply_global_defaults: init_timeout is None (raw parse).
        assert!(
            config.backends[0].init_timeout.is_none(),
            "raw parse must not set init_timeout"
        );

        // Simulate what the hot-reload path now does (after the fix).
        config.apply_global_defaults();

        // After apply_global_defaults: Stdio backend must get 30s init_timeout.
        assert_eq!(
            config.backends[0].init_timeout,
            Some(30),
            "hot-reloaded Stdio backend must get 30s init_timeout from apply_global_defaults"
        );
    }

    #[test]
    fn test_hotreload_spawn_mode_resolved_by_apply_global_defaults() {
        // Backend with spawn_mode = Unset must inherit proxy default after apply.
        let mut config: ProxyConfig = toml::from_str(
            r#"
            [proxy]
            name = "test"
            version = "1.0"
            default_spawn_mode = "lazy"

            [proxy.listen]
            host = "127.0.0.1"
            port = 8080

            [[backends]]
            name = "auto"
            transport = "http"
            url = "http://localhost:8080"
            "#,
        )
        .unwrap();

        // Before: Unset (raw parse default).
        assert_eq!(config.backends[0].spawn_mode, SpawnMode::Unset);

        config.apply_global_defaults();

        // After: inherits proxy default_spawn_mode.
        assert_eq!(config.backends[0].spawn_mode, SpawnMode::Lazy);
    }

    // ------------------------------------------------------------------
    // hot-reload-remove-kill: removal kill-mode resolution.
    // Kill mode is baked into the transport at spawn, so terminating a
    // removed backend's child honors the OLD config; a missing old entry
    // falls back to the new global timeout (caller logs a warning).
    // ------------------------------------------------------------------

    fn stdio_backend_with_kill(name: &str, force_kill: bool) -> BackendConfig {
        toml::from_str(&format!(
            r#"
            name = "{name}"
            transport = "stdio"
            command = "echo"
            force_kill = {force_kill}
            "#,
        ))
        .unwrap()
    }

    #[test]
    fn removal_kill_mode_uses_old_config_graceful_timeout() {
        let old = stdio_backend_with_kill("echo", false);
        // Old timeout (5s) wins over the new global timeout (9s).
        let (force, timeout) = removal_kill_mode(Some(&old), false, 5, false, 9);
        assert!(!force, "old backend without force_kill must be graceful");
        assert_eq!(timeout, Duration::from_secs(5));
    }

    #[test]
    fn removal_kill_mode_old_per_backend_force_kill_wins_over_new_global() {
        let old = stdio_backend_with_kill("echo", true);
        let (force, timeout) = removal_kill_mode(Some(&old), false, 5, false, 9);
        assert!(
            force,
            "old per-backend force_kill must force immediate kill"
        );
        assert_eq!(timeout, Duration::ZERO);
    }

    #[test]
    fn removal_kill_mode_old_global_force_kill_forces_immediate() {
        let old = stdio_backend_with_kill("echo", false);
        let (force, timeout) = removal_kill_mode(Some(&old), true, 5, false, 9);
        assert!(force, "old global force_kill must force immediate kill");
        assert_eq!(timeout, Duration::ZERO);
    }

    #[test]
    fn removal_kill_mode_falls_back_to_new_global_when_old_missing() {
        let (force, timeout) = removal_kill_mode(None, false, 5, false, 9);
        assert!(!force);
        assert_eq!(timeout, Duration::from_secs(9));
    }

    #[test]
    fn removal_kill_mode_fallback_honors_new_global_force_kill() {
        let (force, timeout) = removal_kill_mode(None, false, 5, true, 9);
        assert!(force);
        assert_eq!(timeout, Duration::ZERO);
    }

    // ------------------------------------------------------------------
    // actionable-spawn-error: fast-fail spawn-conflict classifier +
    // advisory error enrichment. Never kills foreign PIDs, no proc scan.
    // ------------------------------------------------------------------

    fn singleton_backend(name: &str) -> BackendConfig {
        toml::from_str(&format!(
            r#"
            name = "{name}"
            transport = "stdio"
            command = "/usr/bin/codebase-memory-mcp"
            singleton_externally_managed = true
            singleton_stop_hint = "systemctl --user stop codebase-memory"
            "#,
        ))
        .unwrap()
    }

    fn plain_backend(name: &str) -> BackendConfig {
        toml::from_str(&format!(
            r#"
            name = "{name}"
            transport = "stdio"
            command = "/usr/bin/some-mcp"
            "#,
        ))
        .unwrap()
    }

    /// Regression: the child answered `initialize`, so it did not exit. The
    /// old `failed to initialize` signal classified these as spawn conflicts,
    /// which invalidated the backend's warm catalog and blamed a singleton.
    #[test]
    fn answered_handshake_is_not_a_spawn_conflict() {
        for raw in [
            "failed to initialize: Transport error: server selected protocol version `1999-01-01`, which is not an initialize-lifecycle version this client supports; client supports: [\"2025-11-25\"]",
            "failed to initialize: JSON-RPC error -32603: internal error",
        ] {
            assert!(
                !is_fast_spawn_conflict(
                    raw,
                    Duration::from_millis(50),
                    Some(Duration::from_secs(30))
                ),
                "answered handshake misclassified as conflict: {raw}"
            );
        }
    }

    #[test]
    fn map_init_error_names_a_protocol_rejection() {
        let backend = plain_backend("legacy");
        let err = map_init_error(
            &backend,
            "failed to initialize: Transport error: server selected protocol version `1999-01-01`, which is not an initialize-lifecycle version this client supports",
            Duration::from_millis(50),
            Some(Duration::from_secs(30)),
        );
        let msg = err.to_string();
        assert!(msg.contains("legacy"), "must name backend: {msg}");
        assert!(msg.contains("protocol version not supported"), "{msg}");
        assert!(msg.contains("1999-01-01"), "must keep the version: {msg}");
        assert!(
            !msg.contains(SPAWN_CONFLICT_MARKER),
            "not a conflict: {msg}"
        );
        assert!(!msg.contains("singleton"), "no singleton claim: {msg}");
    }

    #[test]
    fn fast_conflict_connection_closed_under_timeout() {
        assert!(is_fast_spawn_conflict(
            "failed to initialize: Transport error: Connection closed",
            Duration::from_millis(200),
            Some(Duration::from_secs(30)),
        ));
    }

    #[test]
    fn fast_conflict_without_init_timeout_bound() {
        assert!(is_fast_spawn_conflict(
            "Connection closed",
            Duration::from_millis(100),
            None,
        ));
    }

    #[test]
    fn timeout_text_never_classified_as_conflict() {
        // Timeouts keep existing timeout error text (not misclassified).
        assert!(!is_fast_spawn_conflict(
            "Backend 'x' initialization timed out after 30s: Connection closed",
            Duration::from_secs(31),
            Some(Duration::from_secs(30)),
        ));
        assert!(!is_fast_spawn_conflict(
            "failed to initialize: request timed out",
            Duration::from_millis(100),
            Some(Duration::from_secs(30)),
        ));
    }

    #[test]
    fn slow_init_failure_not_classified_as_conflict() {
        assert!(!is_fast_spawn_conflict(
            "failed to initialize: Connection closed",
            Duration::from_secs(45),
            Some(Duration::from_secs(30)),
        ));
    }

    #[test]
    fn unrelated_error_not_classified_as_conflict() {
        assert!(!is_fast_spawn_conflict(
            "spawning backend 'x': No such file or directory",
            Duration::from_millis(50),
            Some(Duration::from_secs(30)),
        ));
    }

    #[test]
    fn map_init_error_enriches_conflict_with_name_and_stop_hint() {
        let backend = singleton_backend("codebase");
        let err = map_init_error(
            &backend,
            "failed to initialize: Transport error: Connection closed",
            Duration::from_millis(200),
            Some(Duration::from_secs(30)),
        );
        let msg = err.to_string();
        assert!(msg.contains("codebase"), "must name backend: {msg}");
        assert!(
            msg.contains(SPAWN_CONFLICT_MARKER),
            "must carry fast-fail signal: {msg}"
        );
        assert!(
            msg.contains("systemctl --user stop codebase-memory"),
            "must carry advisory stop hint: {msg}"
        );
        assert!(
            msg.contains("never kills foreign"),
            "must state no-kill policy: {msg}"
        );
    }

    #[test]
    fn map_init_error_unflagged_backend_gets_advisory_wording_only() {
        let backend = plain_backend("plain");
        let err = map_init_error(
            &backend,
            "failed to initialize: Connection closed",
            Duration::from_millis(200),
            Some(Duration::from_secs(30)),
        );
        let msg = err.to_string();
        assert!(msg.contains("plain"), "must name backend: {msg}");
        assert!(
            msg.contains(SPAWN_CONFLICT_MARKER),
            "must carry signal: {msg}"
        );
        assert!(
            !msg.contains("systemctl"),
            "unflagged backend must not invent a stop command: {msg}"
        );
        assert!(
            !msg.contains("singleton"),
            "unflagged backend must not be blamed on a singleton: {msg}"
        );
    }

    #[test]
    fn map_init_error_preserves_timeout_text_untouched() {
        let backend = singleton_backend("codebase");
        let raw = "Backend 'codebase' initialization timed out after 30s";
        let err = map_init_error(
            &backend,
            raw,
            Duration::from_secs(31),
            Some(Duration::from_secs(30)),
        );
        assert_eq!(err.to_string(), raw, "timeout text must be preserved");
    }

    #[test]
    fn spawn_conflict_data_shape_carries_backend_hint_no_pid() {
        let backend = singleton_backend("codebase");
        let data = spawn_conflict_data(&backend);
        assert_eq!(data["backend"], serde_json::json!("codebase"));
        assert_eq!(data["conflict"], serde_json::json!(true));
        let hint = data["hint"].as_str().expect("hint string");
        assert!(hint.contains("systemctl --user stop codebase-memory"));
        assert!(
            data.get("pid").is_none(),
            "holder PID included only when known — never scanned"
        );
    }
}
