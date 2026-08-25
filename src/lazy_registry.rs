//! Lazy backend registry: tracks per-backend warm catalog + spawn state.
//!
//! Wave 3 introduced the type and startup wiring. Waves 5-6 add the spawn
//! guard, refcount, activity tracking, and idle sweeper. This module owns the
//! on-demand spawn logic: the first action request targeting a lazy backend
//! brings its child process up (preserving per-backend middleware), coalesces
//! concurrent first-calls into a single spawn (FR-006), tracks activity for the
//! idle timer (C21), and reconciles capability drift against the warm cache
//! (T5.2 / C20).

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::config::BackendConfig;
use crate::warm_cache::{BinaryHasher, ProbeRunner, WarmCatalog, WarmCatalogStore};
use tower_mcp::proxy::McpProxy;

/// Injectable backend-spawn hook (normally [`crate::reload::add_backend`]).
///
/// Stored as an `Option` so production uses the real spawn while tests can
/// substitute a controllable fake (e.g. to assert coalescing, FR-006).
pub(crate) type SpawnBackendFn = Arc<
    dyn Fn(&BackendConfig) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Injectable post-spawn probe hook (normally [`ProbeRunner::probe`]).
///
/// Runs once per spawn to capture the live, negotiated catalog + protocol
/// version (C20 / T5.2). Overridable in tests to avoid spawning a probe child.
pub(crate) type ProbeBackendFn = Arc<
    dyn Fn(
            &BackendConfig,
            &str,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<WarmCatalog>> + Send>>
        + Send
        + Sync,
>;

/// Spawn lifecycle state for a lazy backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnState {
    /// No child process running (served from warm cache).
    Down,
    /// A spawn is in flight (coalesced behind the spawn guard).
    Spawning,
    /// Child process is up and registered with the proxy.
    Up,
}

/// A single lazy backend's tracked state at startup (public snapshot).
#[derive(Clone)]
pub struct LazyBackend {
    /// The backend's configuration (cloned from the startup [`ProxyConfig`]).
    pub config: BackendConfig,
    /// Warm catalog loaded/probed at startup, if any.
    pub catalog: Option<WarmCatalog>,
    /// Negotiated protocol version from the last spawn (overwrites cached, C20).
    pub protocol_version: Option<String>,
}

/// Internal per-backend entry holding mutable spawn state under locks.
///
/// The spawn guard ([`OnceLock`] of an `Arc<Mutex<()>>`) coalesces concurrent
/// first-calls so exactly one child is spawned (FR-006). `state`, `refcount`,
/// and `last_touch` are mutated under short-lived locks (never held across an
/// `.await`), keeping the [`DashMap`] shard uncontended.
struct LazyBackendEntry {
    config: BackendConfig,
    catalog: Option<WarmCatalog>,
    protocol_version: Option<String>,
    /// Spawn lifecycle (C20 / FR-006).
    state: Mutex<SpawnState>,
    /// In-flight request guard for the idle sweeper (C6 / C23).
    refcount: AtomicUsize,
    /// Last activity timestamp for the idle timer (C21).
    last_touch: Mutex<Instant>,
    /// Idle timeout (seconds) before this backend may be torn down (C15).
    /// `None` means never idle-out; `Some(0)` means keep-alive.
    idle_timeout_secs: Option<u64>,
    /// Coalescing guard: concurrent first-calls await the SAME in-flight spawn.
    spawn_guard: OnceLock<Arc<tokio::sync::Mutex<()>>>,
}

/// Registry of lazy backends, shared (via `Arc`) across the root stack, every
/// endpoint-group stack, and the hot-reload path so a backend referenced by
/// multiple groups uses ONE child process (C5 / C18).
///
/// Holds a clone of the shared [`McpProxy`] so it can register spawned backends,
/// the [`WarmCatalogStore`] for drift persistence, and the namespace `separator`.
pub struct LazyBackendRegistry {
    backends: Arc<DashMap<String, LazyBackendEntry>>,
    proxy: Option<McpProxy>,
    store: Option<Arc<WarmCatalogStore>>,
    separator: String,
    /// Test override for the backend spawn (see [`SpawnBackendFn`]).
    spawn_fn: Option<SpawnBackendFn>,
    /// Test override for the post-spawn probe (see [`ProbeBackendFn`]).
    probe_fn: Option<ProbeBackendFn>,
    /// Test override for backend removal during idle-out (see [`IdleOutFn`]).
    /// When set, [`idle_out`] calls this instead of `proxy.remove_backend` so
    /// unit tests can assert teardown without a live [`McpProxy`]. Always `None`
    /// in production, where the real proxy removal is used.
    idle_out_fn: Option<IdleOutFn>,
}

/// Injectable backend-removal hook for idle-out (normally [`McpProxy::remove_backend`]).
///
/// Stored as an `Option` so production uses the real proxy removal while tests
/// can substitute a controllable fake (e.g. to assert the Down transition).
pub(crate) type IdleOutFn =
    Arc<dyn Fn(&str) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

impl LazyBackendRegistry {
    /// Build a registry from the lazy backends discovered at startup.
    ///
    /// The registry is not yet wired to a proxy; call [`with_runtime`] (or rely
    /// on `from_config`) before invoking [`ensure_spawned`].
    pub fn from_backends(backends: Vec<LazyBackend>) -> Self {
        let map = DashMap::new();
        for b in backends {
            let idle_timeout_secs = b.config.idle_timeout_secs;
            map.insert(
                b.config.name.clone(),
                LazyBackendEntry {
                    config: b.config,
                    catalog: b.catalog,
                    protocol_version: b.protocol_version,
                    state: Mutex::new(SpawnState::Down),
                    refcount: AtomicUsize::new(0),
                    last_touch: Mutex::new(Instant::now()),
                    idle_timeout_secs,
                    spawn_guard: OnceLock::new(),
                },
            );
        }
        Self {
            backends: Arc::new(map),
            proxy: None,
            store: None,
            separator: String::new(),
            spawn_fn: None,
            probe_fn: None,
            idle_out_fn: None,
        }
    }

    /// Wire the registry to the shared proxy, warm-catalog store, and separator.
    ///
    /// Required before [`ensure_spawned`] can spawn. Called from `from_config`
    /// after the shared [`McpProxy`] is constructed.
    pub fn with_runtime(
        mut self,
        proxy: McpProxy,
        store: &WarmCatalogStore,
        separator: String,
    ) -> Self {
        self.proxy = Some(proxy);
        self.store = Some(store.clone_into_arc());
        self.separator = separator;
        self
    }

    /// Configured idle timeout (seconds) for a backend, if any (C15).
    ///
    /// `None` means never idle-out; `Some(0)` is treated as keep-alive by
    /// [`idle_out`].
    pub fn idle_timeout(&self, name: &str) -> Option<u64> {
        self.backends.get(name).and_then(|e| e.idle_timeout_secs)
    }

    /// Last activity timestamp for a backend, if tracked (C21).
    pub fn last_touch(&self, name: &str) -> Option<Instant> {
        self.backends
            .get(name)
            .map(|e| *e.last_touch.lock().unwrap())
    }

    /// Look up a lazy backend by name (public snapshot).
    pub fn get(&self, name: &str) -> Option<LazyBackend> {
        self.backends.get(name).map(|e| LazyBackend {
            config: e.config.clone(),
            catalog: e.catalog.clone(),
            protocol_version: e.protocol_version.clone(),
        })
    }

    /// All registered lazy backend names.
    pub fn names(&self) -> Vec<String> {
        self.backends.iter().map(|e| e.key().clone()).collect()
    }

    /// Whether the wired shared proxy currently serves a backend under `name`.
    ///
    /// Used by integration tests to assert that a lazy backend registered via
    /// [`register_lazy`] is NOT eagerly added to the proxy's routing table
    /// (AC-006): a lazy backend must be tracked in the registry but absent from
    /// the live proxy until its first action request. Returns `false` when no
    /// proxy is wired (e.g. a registry built without [`with_runtime`]).
    pub fn proxy_has_namespace(&self, name: &str) -> bool {
        self.proxy
            .as_ref()
            .map(|p| p.backend_namespaces().contains(&name.to_string()))
            .unwrap_or(false)
    }

    /// Register a lazy backend WITHOUT spawning it (hot-reload add, AC-006).
    ///
    /// Used by the hot-reload path when a newly-added backend is configured with
    /// `spawn_mode = "lazy"`: it is recorded in the registry so the first action
    /// request can bring it up on demand, but no child process is started now.
    ///
    /// If a backend with the same name already exists, it is replaced. The
    /// hot-reload fingerprint loop removes the old entry first for replaces, so
    /// this is only a defensive overwrite for the add path.
    pub fn register_lazy(&self, backend: BackendConfig) {
        let entry = LazyBackendEntry {
            config: backend.clone(),
            catalog: None, // loaded lazily on first spawn / from cache
            protocol_version: None,
            state: Mutex::new(SpawnState::Down),
            refcount: AtomicUsize::new(0),
            last_touch: Mutex::new(Instant::now()),
            idle_timeout_secs: backend.idle_timeout_secs,
            spawn_guard: OnceLock::new(),
        };
        self.backends.insert(backend.name.clone(), entry);
        tracing::info!(backend = %backend.name, "Registered lazy backend (not spawned)");
    }

    /// Remove a lazy backend from the registry (hot-reload remove, FR-009).
    ///
    /// Does NOT kill a spawned child — the caller is responsible for calling
    /// `proxy.remove_backend` first (which terminates the child). The warm
    /// catalog file is intentionally left on disk so a future config restore of
    /// the same backend can reuse it. This is a no-op if the name is absent.
    pub fn unregister(&self, name: &str) {
        self.backends.remove(name);
        tracing::info!(backend = %name, "Unregistered lazy backend");
    }

    /// Re-register a lazy backend whose config hash changed: update the stored
    /// config + idle timeout, and if a cached catalog exists for the NEW hash,
    /// load it; otherwise the next spawn will probe + populate (FR-010/AC-010).
    ///
    /// Returns the new identity hash so the caller can update its fingerprint
    /// bookkeeping. The warm catalog file for the old hash is left on disk
    /// (bounded by backend count); `load` ignores mismatched-hash files, so a
    /// stale cache is harmless until overwritten by a fresh probe.
    pub async fn reconcile_config_change(&self, backend: &BackendConfig) -> anyhow::Result<String> {
        let new_hash = BinaryHasher::hash(backend);
        // Update stored config (replaces entry, resets to Down).
        self.register_lazy(backend.clone());
        // If a cached catalog exists for the new hash, it is still valid; the
        // next spawn will load it via the store. Otherwise the next spawn probes
        // + populates (T5.1). We only log which path applies.
        if self
            .store
            .as_ref()
            .map(|s| s.load(&backend.name, &new_hash).is_some())
            .unwrap_or(false)
        {
            tracing::info!(backend = %backend.name, hash = %new_hash, "Warm cache valid after config change");
        } else {
            tracing::info!(backend = %backend.name, hash = %new_hash, "Warm cache stale after config change, will re-probe on next spawn");
        }
        Ok(new_hash)
    }

    /// Current spawn state of a backend (defaults to [`SpawnState::Down`]).
    pub fn spawn_state(&self, name: &str) -> SpawnState {
        self.backends
            .get(name)
            .map(|e| *e.state.lock().unwrap())
            .unwrap_or(SpawnState::Down)
    }

    /// Negotiated protocol version from the last spawn, if any (C3 / C20).
    pub fn protocol_version(&self, name: &str) -> Option<String> {
        self.backends
            .get(name)
            .and_then(|e| e.protocol_version.clone())
    }

    /// Current in-flight request count for a backend (C6 / C23).
    pub fn refcount(&self, name: &str) -> usize {
        self.backends
            .get(name)
            .map(|e| e.refcount.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Resolve the lazy backend that owns a resource `uri`, if any.
    ///
    /// Used to derive the target backend for `ReadResource` action requests,
    /// whose URI is not namespaced (unlike tool/prompt names).
    pub fn backend_for_resource_uri(&self, uri: &str) -> Option<String> {
        for e in self.backends.iter() {
            if let Some(cat) = &e.catalog
                && cat.resources.iter().any(|r| r.uri == uri)
            {
                return Some(e.key().clone());
            }
        }
        None
    }

    /// Mark a backend as recently active (C21).
    ///
    /// Called by [`crate::warm_catalog_service::WarmCatalogService`] immediately
    /// before forwarding an action request to a freshly-spawned backend, so the
    /// idle sweeper (Wave 6) does not tear it down mid-flight.
    pub fn touch(&self, name: &str) {
        if let Some(e) = self.backends.get_mut(name) {
            *e.last_touch.lock().unwrap() = Instant::now();
        }
    }

    /// Increment the in-flight request guard (C6 / C23).
    pub fn inc_refcount(&self, name: &str) {
        if let Some(e) = self.backends.get(name) {
            e.refcount.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Decrement the in-flight request guard (C6 / C23).
    pub fn dec_refcount(&self, name: &str) {
        if let Some(e) = self.backends.get(name) {
            e.refcount.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Ensure a lazy backend's child process is spawned and registered.
    ///
    /// Idempotent and coalesced: concurrent first-calls for the same backend
    /// await a single in-flight spawn (FR-006). On success the backend is marked
    /// [`SpawnState::Up`], a post-spawn probe captures the live catalog +
    /// negotiated protocol version, and any capability drift is reconciled into
    /// the warm cache (T5.2 / C20).
    ///
    /// Backends not present in the registry are ignored (they are served by the
    /// live routing table already). Spawn failures are returned as errors so the
    /// caller can surface a JSON-RPC error to the client.
    pub async fn ensure_spawned(&self, name: &str) -> anyhow::Result<()> {
        // Fast path: not a tracked lazy backend, or already up.
        let entry = match self.backends.get(name) {
            Some(e) => e,
            None => return Ok(()),
        };
        if *entry.state.lock().unwrap() == SpawnState::Up {
            return Ok(());
        }

        // Coalesce concurrent first-calls behind a per-entry OnceCell guard.
        let guard = entry
            .spawn_guard
            .get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        drop(entry); // release the DashMap shard before awaiting
        let _lock = guard.lock().await;

        // Double-checked: another task may have completed the spawn while we waited.
        {
            let e = self.backends.get(name);
            if let Some(e) = e
                && *e.state.lock().unwrap() == SpawnState::Up
            {
                return Ok(());
            }
        }

        let config = {
            let e = self
                .backends
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("lazy backend '{name}' vanished before spawn"))?;
            e.config.clone()
        };

        // Spawn the backend, preserving per-backend middleware (POS-004).
        // When a test spawn hook is installed we use it directly (no proxy
        // required); otherwise the shared proxy must be wired via `with_runtime`.
        match &self.spawn_fn {
            Some(f) => f(&config).await?,
            None => {
                let proxy = self.proxy.clone().ok_or_else(|| {
                    anyhow::anyhow!(
                        "lazy registry runtime (proxy) not configured; cannot spawn '{name}'"
                    )
                })?;
                crate::reload::add_backend(&proxy, &config).await?;
            }
        }

        // Mark up before reconcile so concurrent callers observe a live backend.
        {
            let e = self
                .backends
                .get_mut(name)
                .ok_or_else(|| anyhow::anyhow!("lazy backend '{name}' vanished during spawn"))?;
            *e.state.lock().unwrap() = SpawnState::Up;
        }

        // Re-probe to capture the live, negotiated catalog + protocol version
        // (C20 / T5.2). Bounded by the 5s probe timeout; runs only on first spawn.
        let sep = self.separator.clone();
        let live = match &self.probe_fn {
            Some(f) => f(&config, &sep).await,
            None => ProbeRunner::probe(&config, &sep)
                .await
                .map_err(anyhow::Error::from),
        };
        match live {
            Ok(catalog) => {
                if let Err(e) = self.reconcile(name, catalog).await {
                    tracing::warn!(
                        backend = %name,
                        error = %e,
                        "capability reconcile failed after spawn"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    backend = %name,
                    error = %e,
                    "post-spawn probe failed; cached catalog (if any) retained"
                );
            }
        }

        Ok(())
    }

    /// Reconcile a freshly-probed live catalog against the warm cache.
    ///
    /// - Overwrites `protocol_version` with the live negotiated version (C20).
    /// - If the cached catalog is absent or its capability name-sets diverge from
    ///   the live catalog, updates the entry and persists it to the store
    ///   (T5.2). `add_backend` already emitted `tools/list_changed` (and friends)
    ///   via its internal `notify_all_changed`; tower-mcp 0.22.1 exposes no
    ///   public re-notify, so drift updates are persisted but not re-broadcast.
    async fn reconcile(&self, name: &str, live: WarmCatalog) -> anyhow::Result<()> {
        let cached_names = self.backends.get(name).and_then(|e| {
            e.catalog
                .as_ref()
                .map(|c| (tool_name_set(c), resource_name_set(c), prompt_name_set(c)))
        });
        let live_names = (
            tool_name_set(&live),
            resource_name_set(&live),
            prompt_name_set(&live),
        );
        let drift = match cached_names {
            None => true,
            Some(c) => c != live_names,
        };

        if drift {
            tracing::info!(backend = %name, "capability drift detected; updating warm catalog");
            if let Some(store) = self.store.as_ref()
                && let Err(e) = store.save(&live)
            {
                tracing::warn!(
                    backend = %name,
                    error = %e,
                    "failed to persist reconciled warm catalog"
                );
            }
        }

        let mut e = self
            .backends
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("lazy backend '{name}' vanished during reconcile"))?;
        e.protocol_version = live.protocol_version.clone(); // C20
        if drift {
            e.catalog = Some(live);
        }
        Ok(())
    }

    /// Install test hooks for spawn + probe (unit tests only).
    #[cfg(test)]
    pub fn with_test_hooks(mut self, spawn_fn: SpawnBackendFn, probe_fn: ProbeBackendFn) -> Self {
        self.spawn_fn = Some(spawn_fn);
        self.probe_fn = Some(probe_fn);
        self
    }

    /// Mark a backend as [`SpawnState::Up`] without spawning (test helper).
    ///
    /// Lets tests exercise the "skip cached tools for Up backends" path (R4.3)
    /// without invoking the spawn/probe hooks.
    #[cfg(test)]
    pub fn mark_up_for_test(&self, name: &str) {
        if let Some(e) = self.backends.get_mut(name) {
            *e.state.lock().unwrap() = SpawnState::Up;
        }
    }

    /// Install a test hook for backend removal during idle-out (unit tests only).
    #[cfg(test)]
    pub fn with_idle_out_hook(self, idle_out_fn: IdleOutFn) -> Self {
        Self {
            idle_out_fn: Some(idle_out_fn),
            ..self
        }
    }

    /// Force the negotiated protocol version for a backend (test helper).
    #[cfg(test)]
    pub fn set_protocol_version_for_test(&self, name: &str, version: &str) {
        if let Some(mut e) = self.backends.get_mut(name) {
            e.protocol_version = Some(version.to_string());
        }
    }

    /// Set the last-activity timestamp to the given instant (test helper).
    #[cfg(test)]
    pub fn set_last_touch_for_test(&self, name: &str, at: Instant) {
        if let Some(e) = self.backends.get_mut(name) {
            *e.last_touch.lock().unwrap() = at;
        }
    }

    /// Mark a backend as [`SpawnState::Down`] and clear its in-flight guard.
    ///
    /// Used by [`idle_out`] after the child is terminated so the next action
    /// request re-spawns it and `List*` continues to serve the warm catalog.
    fn set_state_down(&self, name: &str) {
        if let Some(e) = self.backends.get_mut(name) {
            *e.state.lock().unwrap() = SpawnState::Down;
            e.refcount.store(0, Ordering::SeqCst);
        }
    }

    /// Idle-out a backend if it is Up, has no in-flight requests, and has been
    /// idle longer than its configured timeout. Preserves the warm catalog.
    ///
    /// Returns `Ok(true)` if the backend was idle-out (child terminated, state
    /// set to [`SpawnState::Down`]); `Ok(false)` if no idle-out was performed.
    /// Errors from the underlying removal are surfaced as [`anyhow::Error`].
    ///
    /// Guards (in order):
    /// - **C3**: only stateless `2026-07-28` backends are idle-out; session-based
    ///   `2025-11-25` (or unknown) backends are kept alive.
    /// - **Up-only**: a backend that is not [`SpawnState::Up`] is skipped.
    /// - **C6 / C23**: a backend with in-flight requests (`refcount > 0`) is
    ///   skipped so the sweeper never kills a backend mid-request.
    /// - **C15**: `idle_timeout_secs` must be `Some(secs)` with `secs > 0`;
    ///   `None` or `0` means keep-alive.
    /// - The backend must have been idle longer than `idle_timeout_secs`.
    pub async fn idle_out(&self, name: &str) -> anyhow::Result<bool> {
        // Guard 1: only idle-out stateless 2026-07-28 backends (C3).
        let pv = self.protocol_version(name);
        if pv.as_deref() != Some("2026-07-28") {
            return Ok(false); // session-based (2025-11-25) or unknown → keep alive
        }
        // Guard 2: must be Up.
        if self.spawn_state(name) != SpawnState::Up {
            return Ok(false);
        }
        // Guard 3: no in-flight requests (C6 / C23).
        if self.refcount(name) > 0 {
            return Ok(false);
        }
        // Guard 4: idle timeout configured and expired (C15).
        let Some(secs) = self.idle_timeout(name) else {
            return Ok(false);
        };
        if secs == 0 {
            return Ok(false); // idle_timeout_secs = 0 ⇒ keep-alive (C15)
        }
        let Some(last) = self.last_touch(name) else {
            return Ok(false);
        };
        if last.elapsed() < Duration::from_secs(secs) {
            return Ok(false);
        }
        // Perform idle-out: terminate the child and mark Down (catalog preserved).
        let removed = match &self.idle_out_fn {
            Some(f) => f(name).await,
            None => match &self.proxy {
                Some(proxy) => proxy.remove_backend(name).await,
                None => {
                    anyhow::bail!(
                        "lazy registry runtime (proxy) not configured; cannot idle-out '{name}'"
                    )
                }
            },
        };
        if removed {
            self.set_state_down(name);
            tracing::info!(backend = %name, "Idle-out lazy backend (stateless 2026-07-28)");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Spawn the background idle sweeper.
    ///
    /// Polls every `interval_secs` and idle-outs eligible backends via
    /// [`idle_out`]. Removal errors are logged and swallowed so the loop
    /// survives (R6.3); the task runs until the registry is dropped.
    pub fn start_sweeper(self: &Arc<Self>, interval_secs: u64) {
        let reg = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
            loop {
                ticker.tick().await;
                for name in reg.names() {
                    if let Err(e) = reg.idle_out(&name).await {
                        tracing::warn!(backend = %name, error = %e, "idle-out failed");
                    }
                }
            }
        });
    }
}

/// Name-set of a catalog's tools (for drift comparison).
fn tool_name_set(c: &WarmCatalog) -> BTreeSet<String> {
    c.tools.iter().map(|t| t.name.clone()).collect()
}

/// Name-set of a catalog's resources (for drift comparison).
fn resource_name_set(c: &WarmCatalog) -> BTreeSet<String> {
    c.resources.iter().map(|r| r.name.clone()).collect()
}

/// Name-set of a catalog's prompts (for drift comparison).
fn prompt_name_set(c: &WarmCatalog) -> BTreeSet<String> {
    c.prompts.iter().map(|p| p.name.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackendConfig, SpawnMode, TransportType};
    use crate::warm_cache::WarmCatalogStore;
    use crate::warm_cache::catalog::WarmCatalog;
    use tower_mcp::proxy::McpProxy;
    use tower_mcp_types::protocol::ToolDefinition;

    /// Build a registry wired to a real (empty) proxy + temp warm-cache store.
    /// Used to assert that `register_lazy` does NOT eagerly spawn (AC-006).
    async fn runtime_registry() -> LazyBackendRegistry {
        use tower_mcp::client::ChannelTransport;
        use tower_mcp::router::McpRouter;

        let dir = std::env::temp_dir().join(format!("mcp-proxy-test-{}", std::process::id()));
        let store = WarmCatalogStore::new(dir);
        // A dummy in-process backend so `build_strict` succeeds (it requires at
        // least one backend). The lazy backend under test is registered in the
        // registry, NOT added to this proxy, so we can assert it is absent.
        let dummy = McpRouter::default();
        let proxy = McpProxy::builder("test-proxy", "1.0.0")
            .separator("/")
            .backend("__dummy__", ChannelTransport::new(dummy))
            .await
            .build_strict()
            .await
            .expect("proxy builds");
        LazyBackendRegistry::from_backends(vec![]).with_runtime(proxy, &store, "/".to_string())
    }

    /// Build a stdio [`BackendConfig`] with the given spawn mode + idle timeout.
    fn backend(
        name: &str,
        mode: SpawnMode,
        idle: Option<u64>,
        suffix: Option<&str>,
    ) -> BackendConfig {
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

    #[tokio::test]
    async fn register_lazy_does_not_spawn_and_is_not_in_proxy() {
        let reg = runtime_registry().await;
        reg.register_lazy(backend("files", SpawnMode::Lazy, Some(30), None));

        assert!(
            reg.names().contains(&"files".to_string()),
            "lazy backend must be registered"
        );
        assert_eq!(
            reg.spawn_state("files"),
            SpawnState::Down,
            "lazy backend must NOT be spawned (AC-006)"
        );
        assert!(
            !reg.proxy
                .as_ref()
                .unwrap()
                .backend_namespaces()
                .contains(&"files".to_string()),
            "lazy backend must not be eagerly added to the proxy (AC-006)"
        );
    }

    #[tokio::test]
    async fn unregister_removes_lazy_backend() {
        let reg = runtime_registry().await;
        reg.register_lazy(backend("files", SpawnMode::Lazy, Some(30), None));
        assert!(reg.names().contains(&"files".to_string()));

        reg.unregister("files");
        assert!(
            !reg.names().contains(&"files".to_string()),
            "unregister must drop the lazy backend (FR-009)"
        );
    }

    #[tokio::test]
    async fn reconcile_config_change_rehashes_and_updates_idle_timeout() {
        let reg = runtime_registry().await;
        reg.register_lazy(backend("files", SpawnMode::Lazy, Some(10), Some("v1")));

        // Modify the config: different cache_key_suffix (changes the hash) and a
        // new idle timeout.
        let modified = backend("files", SpawnMode::Lazy, Some(20), Some("v2"));
        let new_hash = reg.reconcile_config_change(&modified).await.unwrap();

        // The identity hash must differ because cache_key_suffix changed.
        let old_hash = crate::warm_cache::BinaryHasher::hash(&backend(
            "files",
            SpawnMode::Lazy,
            Some(10),
            Some("v1"),
        ));
        assert_ne!(
            new_hash, old_hash,
            "config hash must change on suffix change (FR-010/AC-010)"
        );

        // The stored entry must reflect the new idle timeout (C15).
        assert_eq!(
            reg.idle_timeout("files"),
            Some(20),
            "idle_timeout_secs must be updated by reconcile (FR-010)"
        );
        assert_eq!(
            reg.get("files").unwrap().config.spawn_mode,
            SpawnMode::Lazy,
            "still lazy after reconcile"
        );
    }

    #[tokio::test]
    async fn flip_lazy_to_eager_then_back_transitions_registry_state() {
        let reg = runtime_registry().await;

        // Start lazy (e.g. added as lazy via hot reload).
        reg.register_lazy(backend("files", SpawnMode::Lazy, Some(30), None));
        assert!(reg.names().contains(&"files".to_string()));

        // lazy -> eager flip: unregister the lazy entry (the hot-reload path then
        // calls add_backend, which is covered by existing integration tests).
        reg.unregister("files");
        assert!(!reg.names().contains(&"files".to_string()));

        // eager -> lazy flip: re-register as lazy without spawning.
        reg.register_lazy(backend("files", SpawnMode::Lazy, Some(30), None));
        assert!(
            reg.names().contains(&"files".to_string()),
            "re-registering as lazy must restore the entry"
        );
        assert_eq!(reg.spawn_state("files"), SpawnState::Down);
    }

    fn lazy_backend(name: &str, tools: &[&str]) -> LazyBackend {
        let config = BackendConfig {
            name: name.to_string(),
            transport: TransportType::Stdio,
            ..Default::default()
        };
        let catalog_tools: Vec<ToolDefinition> = tools
            .iter()
            .map(|t| ToolDefinition {
                name: format!("{name}/{t}"),
                title: None,
                description: Some(format!("{t} tool")),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: None,
                icons: None,
                annotations: None,
                execution: None,
                meta: None,
            })
            .collect();
        let catalog = WarmCatalog::from_probe_result(
            name,
            "/",
            catalog_tools,
            vec![],
            vec![],
            vec![],
            Some("2025-11-25".to_string()),
            "testhash".to_string(),
        );
        LazyBackend {
            config,
            catalog: Some(catalog),
            protocol_version: None,
        }
    }

    /// Build a registry wired with a fake spawn + probe that count spawns and
    /// return a controllable live catalog.
    fn fake_registry(
        backends: Vec<LazyBackend>,
        spawn_count: Arc<AtomicUsize>,
        live_tools: Vec<String>,
        live_protocol: Option<String>,
    ) -> LazyBackendRegistry {
        let spawn_fn: SpawnBackendFn = {
            let count = spawn_count.clone();
            Arc::new(move |_cfg: &BackendConfig| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let probe_fn: ProbeBackendFn = Arc::new(move |cfg: &BackendConfig, _sep: &str| {
            let tools = live_tools
                .iter()
                .map(|t| ToolDefinition {
                    name: t.clone(),
                    title: None,
                    description: None,
                    input_schema: serde_json::json!({"type": "object"}),
                    output_schema: None,
                    icons: None,
                    annotations: None,
                    execution: None,
                    meta: None,
                })
                .collect();
            let catalog = WarmCatalog::from_probe_result(
                &cfg.name,
                "/",
                tools,
                vec![],
                vec![],
                vec![],
                live_protocol.clone(),
                "testhash".to_string(),
            );
            Box::pin(async move { Ok(catalog) })
        });

        LazyBackendRegistry::from_backends(backends).with_test_hooks(spawn_fn, probe_fn)
    }

    #[tokio::test]
    async fn ensure_spawned_spawns_once_and_marks_up() {
        let count = Arc::new(AtomicUsize::new(0));
        let reg = fake_registry(
            vec![lazy_backend("files", &["read"])],
            count.clone(),
            vec![],
            None,
        );

        reg.ensure_spawned("files").await.unwrap();

        assert_eq!(count.load(Ordering::SeqCst), 1, "exactly one spawn");
        assert_eq!(reg.spawn_state("files"), SpawnState::Up);
        // Second call is a no-op (already up).
        reg.ensure_spawned("files").await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1, "no second spawn when up");
    }

    #[tokio::test]
    async fn concurrent_first_calls_coalesce_into_one_spawn() {
        let count = Arc::new(AtomicUsize::new(0));
        let reg = Arc::new(fake_registry(
            vec![lazy_backend("files", &["read"])],
            count.clone(),
            vec![],
            None,
        ));

        // Fire N concurrent first-calls; they must share ONE in-flight spawn.
        let n = 16u32;
        let mut handles = Vec::new();
        for _ in 0..n {
            let reg = reg.clone();
            handles.push(tokio::spawn(async move {
                reg.ensure_spawned("files").await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "concurrent first-calls must coalesce into exactly one spawn (FR-006)"
        );
        assert_eq!(reg.spawn_state("files"), SpawnState::Up);
    }

    #[tokio::test]
    async fn reconcile_overwrites_protocol_version_and_detects_drift() {
        let count = Arc::new(AtomicUsize::new(0));
        // Cached catalog has tool "files/read"; live probe returns "files/write"
        // (drift) and a newer protocol version.
        let reg = fake_registry(
            vec![lazy_backend("files", &["read"])],
            count.clone(),
            vec!["write".to_string()],
            Some("2026-07-28".to_string()),
        );

        reg.ensure_spawned("files").await.unwrap();

        // C20: protocol_version overwritten with the live negotiated version.
        assert_eq!(
            reg.protocol_version("files").as_deref(),
            Some("2026-07-28"),
            "protocol_version must be overwritten with live negotiated version"
        );
        // T5.2: drift detected → cached catalog updated to live tool set.
        let lb = reg.get("files").expect("backend present");
        let names: Vec<&str> = lb
            .catalog
            .iter()
            .flat_map(|c| c.tools.iter().map(|t| t.name.as_str()))
            .collect();
        assert!(
            names.contains(&"files/write"),
            "drifted catalog must be persisted: {names:?}"
        );
        assert!(
            !names.contains(&"files/read"),
            "stale tool must be replaced: {names:?}"
        );
    }

    #[tokio::test]
    async fn refcount_and_touch_track_activity() {
        let count = Arc::new(AtomicUsize::new(0));
        let reg = fake_registry(
            vec![lazy_backend("files", &["read"])],
            count.clone(),
            vec![],
            None,
        );

        assert_eq!(reg.refcount("files"), 0);
        reg.inc_refcount("files");
        assert_eq!(reg.refcount("files"), 1);
        reg.touch("files");
        reg.dec_refcount("files");
        assert_eq!(reg.refcount("files"), 0);
    }

    #[tokio::test]
    async fn ensure_spawned_is_noop_for_unknown_backend() {
        let count = Arc::new(AtomicUsize::new(0));
        let reg = fake_registry(vec![], count.clone(), vec![], None);
        // Unknown backend: no spawn, no error.
        reg.ensure_spawned("ghost").await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn spawn_failure_propagates_error() {
        let spawn_fn: SpawnBackendFn =
            Arc::new(|_cfg: &BackendConfig| Box::pin(async move { Err(anyhow::anyhow!("boom")) }));
        let probe_fn: ProbeBackendFn = Arc::new(|_cfg: &BackendConfig, _sep: &str| {
            Box::pin(async move { Err(anyhow::anyhow!("should not probe")) })
        });
        let reg = LazyBackendRegistry::from_backends(vec![lazy_backend("files", &["read"])])
            .with_test_hooks(spawn_fn, probe_fn);

        let err = reg.ensure_spawned("files").await.unwrap_err();
        assert!(
            err.to_string().contains("boom"),
            "spawn error must propagate: {err}"
        );
        assert_eq!(
            reg.spawn_state("files"),
            SpawnState::Down,
            "failed spawn stays Down"
        );
    }

    /// Build a registry with one lazy backend configured with `idle_timeout_secs`
    /// and a fake removal hook that records whether idle-out removed it.
    fn idle_registry(idle_timeout_secs: Option<u64>) -> (LazyBackendRegistry, Arc<AtomicUsize>) {
        let removed = Arc::new(AtomicUsize::new(0));
        let idle_out_fn: IdleOutFn = {
            let removed = removed.clone();
            Arc::new(move |_name: &str| {
                let removed = removed.clone();
                Box::pin(async move {
                    removed.fetch_add(1, Ordering::SeqCst);
                    true
                })
            })
        };
        let config = BackendConfig {
            name: "files".to_string(),
            transport: TransportType::Stdio,
            idle_timeout_secs,
            ..Default::default()
        };
        let reg = LazyBackendRegistry::from_backends(vec![LazyBackend {
            config,
            catalog: None,
            protocol_version: None,
        }])
        .with_idle_out_hook(idle_out_fn);
        (reg, removed)
    }

    #[tokio::test]
    async fn idle_out_terminates_stateless_backend_after_timeout() {
        let (reg, removed) = idle_registry(Some(1));
        reg.mark_up_for_test("files");
        reg.set_protocol_version_for_test("files", "2026-07-28");
        // Stale last_touch: 2s ago, longer than the 1s timeout.
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(2));

        let out = reg.idle_out("files").await.unwrap();

        assert!(out, "backend should be idle-out");
        assert_eq!(removed.load(Ordering::SeqCst), 1, "child terminated once");
        assert_eq!(
            reg.spawn_state("files"),
            SpawnState::Down,
            "state reset to Down"
        );
    }

    #[tokio::test]
    async fn idle_out_never_terminates_session_based_backend() {
        let (reg, removed) = idle_registry(Some(1));
        reg.mark_up_for_test("files");
        // C3: session-based 2025-11-25 backend is NEVER idle-out.
        reg.set_protocol_version_for_test("files", "2025-11-25");
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(10));

        let out = reg.idle_out("files").await.unwrap();

        assert!(!out, "session-based backend must be kept alive (C3)");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
        assert_eq!(reg.spawn_state("files"), SpawnState::Up, "stays Up");
    }

    #[tokio::test]
    async fn idle_out_skips_backend_with_in_flight_requests() {
        let (reg, removed) = idle_registry(Some(1));
        reg.mark_up_for_test("files");
        reg.set_protocol_version_for_test("files", "2026-07-28");
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(2));
        reg.inc_refcount("files"); // C6 / C23: in-flight guard

        let out = reg.idle_out("files").await.unwrap();

        assert!(!out, "in-flight backend must not be idle-out (C6/C23)");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
        assert_eq!(reg.spawn_state("files"), SpawnState::Up, "stays Up");
    }

    #[tokio::test]
    async fn idle_out_keeps_alive_when_timeout_is_zero() {
        let (reg, removed) = idle_registry(Some(0)); // C15: 0 ⇒ keep-alive
        reg.mark_up_for_test("files");
        reg.set_protocol_version_for_test("files", "2026-07-28");
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(10));

        let out = reg.idle_out("files").await.unwrap();

        assert!(!out, "idle_timeout_secs = 0 must keep alive (C15)");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
        assert_eq!(reg.spawn_state("files"), SpawnState::Up, "stays Up");
    }

    #[tokio::test]
    async fn idle_out_keeps_alive_when_timeout_is_none() {
        let (reg, removed) = idle_registry(None);
        reg.mark_up_for_test("files");
        reg.set_protocol_version_for_test("files", "2026-07-28");
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(10));

        let out = reg.idle_out("files").await.unwrap();

        assert!(!out, "idle_timeout_secs = None must keep alive");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
        assert_eq!(reg.spawn_state("files"), SpawnState::Up, "stays Up");
    }

    #[tokio::test]
    async fn idle_out_skips_not_up_backend() {
        let (reg, removed) = idle_registry(Some(1));
        // Not marked Up (stays Down).
        reg.set_protocol_version_for_test("files", "2026-07-28");
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(2));

        let out = reg.idle_out("files").await.unwrap();

        assert!(!out, "Down backend must not be idle-out");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
    }

    #[tokio::test]
    async fn sweeper_idle_outs_stale_backend() {
        let (reg, removed) = idle_registry(Some(1));
        let reg = Arc::new(reg);
        reg.mark_up_for_test("files");
        reg.set_protocol_version_for_test("files", "2026-07-28");
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(2));

        // Poll every 1s; the stale backend must be idle-out within ~2s.
        reg.start_sweeper(1);
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert_eq!(
            removed.load(Ordering::SeqCst),
            1,
            "sweeper idle-out the backend"
        );
        assert_eq!(
            reg.spawn_state("files"),
            SpawnState::Down,
            "state reset to Down"
        );
    }

    // ----------------------------------------------------------------------
    // idle_out boundary sub-cases (genuine gaps beyond the existing guards).
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn idle_out_keeps_alive_within_not_expired_window() {
        let (reg, removed) = idle_registry(Some(1));
        reg.mark_up_for_test("files");
        reg.set_protocol_version_for_test("files", "2026-07-28");
        // Recent activity: touched just now, well within the 1s timeout.
        reg.set_last_touch_for_test("files", Instant::now());

        let out = reg.idle_out("files").await.unwrap();

        assert!(!out, "recently-touched backend must not be idle-out");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
        assert_eq!(reg.spawn_state("files"), SpawnState::Up, "stays Up");
    }

    #[tokio::test]
    async fn idle_out_keeps_alive_when_protocol_version_unknown() {
        let (reg, removed) = idle_registry(Some(1));
        reg.mark_up_for_test("files");
        // C3: protocol_version == None (unknown) → never idle-out.
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(10));

        let out = reg.idle_out("files").await.unwrap();

        assert!(!out, "unknown protocol version must be kept alive (C3)");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
        assert_eq!(reg.spawn_state("files"), SpawnState::Up, "stays Up");
    }

    #[tokio::test]
    async fn idle_out_unknown_backend_returns_false_without_panic() {
        let (reg, removed) = idle_registry(Some(1));

        // A name that was never registered must return false, not panic.
        let out = reg.idle_out("ghost").await.unwrap();

        assert!(!out, "unknown backend must not idle-out");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
    }

    #[tokio::test]
    async fn idle_out_preserves_catalog_after_teardown() {
        // The warm catalog must survive idle-out: the entry's catalog stays
        // Some and the state resets to Down so List* keeps serving it.
        let (reg, _removed) = idle_registry(Some(1));
        let catalog = WarmCatalog::from_probe_result(
            "files",
            "/",
            vec![ToolDefinition {
                name: "read".to_string(),
                title: None,
                description: Some("read".to_string()),
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
            Some("2025-11-25".to_string()),
            "testhash".to_string(),
        );
        // Inject a catalog into the entry before marking Up.
        {
            let mut e = reg.backends.get_mut("files").unwrap();
            e.catalog = Some(catalog);
        }
        reg.mark_up_for_test("files");
        reg.set_protocol_version_for_test("files", "2026-07-28");
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(2));

        let out = reg.idle_out("files").await.unwrap();
        assert!(out, "backend should be idle-out");

        // Catalog preserved, state Down.
        let lb = reg.get("files").expect("backend still registered");
        assert!(lb.catalog.is_some(), "warm catalog must survive idle-out");
        assert_eq!(lb.catalog.unwrap().tools[0].name, "files/read");
        assert_eq!(
            reg.spawn_state("files"),
            SpawnState::Down,
            "state reset to Down"
        );
    }

    // ----------------------------------------------------------------------
    // Regression: R2 session-based backend is NOT idle-out (C3).
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn regression_r2_session_based_not_idle_out() {
        let (reg, removed) = idle_registry(Some(1));
        reg.mark_up_for_test("files");
        // Session-based 2025-11-25 backend, stale last_touch.
        reg.set_protocol_version_for_test("files", "2025-11-25");
        reg.set_last_touch_for_test("files", Instant::now() - Duration::from_secs(2));

        let out = reg.idle_out("files").await.unwrap();

        assert!(!out, "session-based backend must NOT idle-out (R2/C3)");
        assert_eq!(removed.load(Ordering::SeqCst), 0, "no removal performed");
        assert_eq!(reg.spawn_state("files"), SpawnState::Up, "stays Up");
    }

    // ----------------------------------------------------------------------
    // Regression: R4 coalesced spawn count == 1 (FR-006).
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn regression_r4_coalesced_spawn_count_is_one() {
        let count = Arc::new(AtomicUsize::new(0));
        let reg = Arc::new(fake_registry(
            vec![lazy_backend("files", &["read"])],
            count.clone(),
            vec![],
            None,
        ));

        // N concurrent first-calls must coalesce into exactly one spawn.
        let n = 32u32;
        let mut handles = Vec::new();
        for _ in 0..n {
            let reg = reg.clone();
            handles.push(tokio::spawn(async move {
                reg.ensure_spawned("files").await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "concurrent first-calls must coalesce into exactly one spawn (R4/FR-006)"
        );
        assert_eq!(reg.spawn_state("files"), SpawnState::Up);
    }
}
