//! Core proxy construction and serving.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::{Path, Request};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::any;
use tower::Layer;
use tower::Service;
use tower::ServiceExt;
use tower::timeout::TimeoutLayer;
use tower::util::BoxCloneService;
use tower_http::cors::{Any, CorsLayer};
use tower_mcp::SessionHandle;
use tower_mcp::auth::{AuthLayer, StaticBearerValidator};
use tower_mcp::client::{HttpClientConfig, HttpClientTransport};
use tower_mcp::proxy::McpProxy;
use tower_mcp::{RouterRequest, RouterResponse};
use tower_resilience::retry::RetryLayer;

use crate::admin::BackendMeta;
use crate::alias;
use crate::cache;
use crate::coalesce;
use crate::config::{AuthConfig, BackendConfig, ProxyConfig};
use crate::discover;
use crate::endpoint_router;
use crate::filter::CapabilityFilterService;
use crate::meta_validation;
#[cfg(feature = "oauth")]
use crate::rbac::{RbacConfig, RbacService};
use crate::tool_group;
use crate::validation::{ValidationConfig, ValidationService};

/// Circuit breaker handle type alias.
pub type CbHandle = tower_resilience::circuitbreaker::CircuitBreakerHandle;

/// A fully constructed MCP proxy ready to serve or embed.
pub struct Proxy {
    router: Router,
    session_handle: SessionHandle,
    inner: McpProxy,
    /// The shared McpProxy containing ALL backends (each spawned exactly once).
    /// Endpoint groups clone this and apply group-specific filtering/middleware.
    shared_proxy: McpProxy,
    config: ProxyConfig,
    runtime: std::sync::Arc<crate::lifecycle::Runtime>,
    reload_policy: crate::reload_policy::ReloadPolicy,
    endpoint_group_registry: crate::endpoint_router::EndpointGroupRegistry,
    /// Registry of lazy backends (warm catalog + spawn state), shared across the
    /// root stack, every endpoint-group stack, and the hot-reload path.
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
    /// Shared alias map for hot-reload support.
    alias_map: Option<Arc<RwLock<crate::alias::AliasMap>>>,
    /// Hot-reload watcher task handle.
    hot_reload_handle: Option<tokio::task::JoinHandle<()>>,
    #[cfg(feature = "discovery")]
    discovery_index: Option<crate::discovery::SharedDiscoveryIndex>,
    #[cfg(feature = "discovery")]
    discovery_schemas: Option<crate::discovery::SchemaStore>,
}

/// Build an McpProxy with a specific set of backends and per-backend middleware.
/// Returns the proxy and a map of backend name -> circuit breaker handle.
///
/// Each stdio backend spawns exactly one child process. Environment variables
/// are resolved from per-backend `env` maps (which should already have global
/// defaults merged via [`ProxyConfig::apply_global_defaults`]).
///
/// This is also used by endpoint groups as a fallback when no shared proxy is
/// provided (legacy mode).
pub(crate) async fn build_mcp_proxy_for_backends(
    proxy_name: &str,
    proxy_version: &str,
    separator: &str,
    proxy_instructions: Option<&String>,
    backends: &[&crate::config::BackendConfig],
    kill_timeout_secs: u64,
    global_force_kill: bool,
) -> Result<(McpProxy, HashMap<String, CbHandle>)> {
    // Many stdio servers still answer `initialize` with 2024-11-05. Accept it
    // for startup backends and for every backend added later (hot reload,
    // lazy spawn, admin add), which inherit the setting from this proxy.
    let mut builder = McpProxy::builder(proxy_name, proxy_version)
        .separator(separator)
        .accept_protocol_2024_11_05(true);
    let mut cb_handles: HashMap<String, CbHandle> = HashMap::new();

    if let Some(instructions) = proxy_instructions {
        builder = builder.instructions(instructions);
    }

    // Create shared outlier detector if any backend has outlier_detection configured.
    // Use the max of all max_ejection_percent values.
    let outlier_detector = {
        let max_pct = backends
            .iter()
            .filter_map(|b| b.outlier_detection.as_ref())
            .map(|od| od.max_ejection_percent)
            .max();
        max_pct.map(crate::outlier::OutlierDetector::new)
    };

    for backend in backends {
        // Skip disabled backends
        if !backend.enabled {
            tracing::info!(name = %backend.name, "Skipping disabled backend");
            continue;
        }

        tracing::info!(name = %backend.name, transport = ?backend.transport, "Adding backend");

        match backend.transport {
            crate::config::TransportType::Stdio => {
                let kill_timeout =
                    backend.effective_kill_timeout(global_force_kill, kill_timeout_secs);
                let transport =
                    crate::stdio_spawn::spawn_stdio_transport(backend, kill_timeout).await?;

                builder = builder.backend(&backend.name, transport).await;
            }
            crate::config::TransportType::Http => {
                let url = backend.url.as_deref().unwrap();
                let transport = build_http_transport(url, backend)?;

                builder = builder.backend(&backend.name, transport).await;
            }
            #[cfg(feature = "websocket")]
            crate::config::TransportType::Websocket => {
                let url = backend.url.as_deref().unwrap();
                tracing::info!(url = %url, "Connecting to WebSocket backend");
                let transport =
                    crate::ws_transport::WebSocketClientTransport::connect_with_headers(
                        url,
                        backend.bearer_token.as_deref(),
                        &backend.headers,
                        backend.protocol_version.as_deref(),
                    )
                    .await
                    .with_context(|| {
                        format!("connecting to WebSocket backend '{}'", backend.name)
                    })?;

                builder = builder.backend(&backend.name, transport).await;
            }
            #[cfg(not(feature = "websocket"))]
            crate::config::TransportType::Websocket => {
                anyhow::bail!(
                    "WebSocket transport requires the 'websocket' feature. \
                     Rebuild with: cargo install mcp-proxy --features websocket"
                );
            }
        }

        // Apply per-backend init_timeout if configured
        if let Some(init_timeout_secs) = backend.init_timeout {
            builder =
                builder.backend_init_timeout(std::time::Duration::from_secs(init_timeout_secs));
        }

        // Per-backend middleware stack (applied in order: inner -> outer)
        builder = apply_backend_middleware(builder, backend, &outlier_detector, &mut cb_handles)?;
    }

    let result = builder.build().await?;

    if !result.skipped.is_empty() {
        for s in &result.skipped {
            tracing::warn!("Skipped backend: {s}");
        }
    }

    Ok((result.proxy, cb_handles))
}

/// Resolve the warm-cache directory: explicit config, else platform default.
pub(crate) fn warm_cache_dir(config: &ProxyConfig) -> std::path::PathBuf {
    match &config.warm_cache.dir {
        Some(dir) => dir.clone(),
        None => {
            let base = std::env::var("XDG_CACHE_HOME")
                .map(std::path::PathBuf::from)
                .ok()
                .or_else(|| {
                    std::env::var("HOME")
                        .ok()
                        .map(|h| std::path::PathBuf::from(h).join(".cache"))
                })
                .unwrap_or_else(|| std::path::PathBuf::from(".cache"));
            base.join("mcp-proxy").join("catalog")
        }
    }
}

/// Build the [`LazyBackendRegistry`] from the startup [`ProxyConfig`].
///
/// For every enabled stdio backend configured with `spawn_mode = "lazy"`:
/// 1. Compute its identity hash via [`crate::warm_cache::BinaryHasher`].
/// 2. Try to load a persisted [`WarmCatalog`] from the warm cache store.
/// 3. On a cache miss, attempt a one-shot [`crate::warm_cache::ProbeRunner`] probe;
///    on success persist the catalog, on failure log a warning and proceed with
///    `catalog = None` (degrade to lazy — NFR-003). A probe error NEVER aborts
///    startup.
///
/// After the registry is built, orphaned cache files are pruned: any
/// `{name}-{hash}.json` whose `(name, hash)` no longer matches a configured
/// backend is deleted (C7/C24). Pruning is scoped by `(name, hash)`, never name
/// alone, and all errors are logged and swallowed.
///
/// Eager/HTTP/WebSocket backends are intentionally NOT added here — they continue
/// to spawn eagerly through [`build_mcp_proxy_for_backends`]. The lazy registry
/// only ADDS entries; it does not remove backends from `all_backend_refs`
/// (that change arrives with Wave 4's serving layer).
async fn build_lazy_registry(
    config: &ProxyConfig,
    shared_proxy: McpProxy,
) -> Arc<crate::lazy_registry::LazyBackendRegistry> {
    use crate::warm_cache::{BinaryHasher, ProbeRunner, WarmCatalogStore};

    let cache_dir = warm_cache_dir(config);

    let store = WarmCatalogStore::new(cache_dir.clone());

    let mut lazy_backends: Vec<crate::lazy_registry::LazyBackend> = Vec::new();

    for backend in &config.backends {
        if !backend.enabled
            || backend.spawn_mode != crate::config::SpawnMode::Lazy
            || backend.transport != crate::config::TransportType::Stdio
        {
            continue;
        }

        let hash = BinaryHasher::hash(backend);

        // Try the persisted warm catalog first.
        let catalog = match store.load(&backend.name, &hash) {
            Some(catalog) => {
                tracing::info!(
                    name = %backend.name,
                    hash = %hash,
                    tools = catalog.tools.len(),
                    resources = catalog.resources.len(),
                    prompts = catalog.prompts.len(),
                    protocol_version = ?catalog.protocol_version,
                    "Loaded warm catalog from cache"
                );
                Some(catalog)
            }
            None => {
                // Cache miss: probe the backend once to capture its capabilities.
                tracing::info!(
                    name = %backend.name,
                    hash = %hash,
                    "Warm cache miss — probing backend"
                );
                match ProbeRunner::probe(backend, &config.proxy.separator).await {
                    Ok(catalog) => {
                        tracing::info!(
                            name = %backend.name,
                            tools = catalog.tools.len(),
                            resources = catalog.resources.len(),
                            prompts = catalog.prompts.len(),
                            protocol_version = ?catalog.protocol_version,
                            "Warm catalog probe completed"
                        );
                        if let Err(e) = store.save(&catalog) {
                            tracing::warn!(
                                name = %backend.name,
                                error = %e,
                                "Failed to persist warm catalog; continuing without cache"
                            );
                        }
                        Some(catalog)
                    }
                    Err(e) => {
                        // Probe failed — classify FIRST: a proven spawn-conflict
                        // fast-fail (fast Connection-closed, never timeouts)
                        // must NOT be rescued by the fuzzy fallback and must NOT
                        // be re-persisted: serving the stale catalog would mask
                        // the breakage. Clear the stale files + disk state for
                        // this backend and start with `catalog = None` so
                        // tools/list omits it until a fresh probe succeeds.
                        // Timeout/transient errors keep the degrade path below.
                        if crate::warm_cache::probe::is_conflict_probe_error(&e) {
                            tracing::warn!(
                                name = %backend.name,
                                error = %e,
                                "Warm catalog probe hit a spawn conflict; invalidating stale catalog (no fuzzy rescue)"
                            );
                            store.invalidate_backend(&backend.name);
                            None
                        } else {
                            // Transient probe error — try fuzzy fallback: find any
                            // existing catalog file for this backend name (hash may
                            // have changed due to config drift since the catalog
                            // was created). Re-save with the current hash so
                            // subsequent startups hit the fast `load()` path.
                            tracing::warn!(
                                name = %backend.name,
                                error = %e,
                                "Warm catalog probe failed; attempting fuzzy catalog fallback"
                            );
                            match store.load_any_for_backend(&backend.name) {
                                Some(mut catalog) => {
                                    let old_hash = catalog.identity_hash.clone();
                                    catalog.identity_hash = hash.clone();
                                    if let Err(e) = store.save(&catalog) {
                                        tracing::warn!(
                                            name = %backend.name,
                                            error = %e,
                                            "Failed to re-save fuzzy-matched catalog"
                                        );
                                    } else {
                                        tracing::info!(
                                            name = %backend.name,
                                            old_hash = %old_hash,
                                            new_hash = %hash,
                                            tools = catalog.tools.len(),
                                            "Recovered stale warm catalog via fuzzy fallback"
                                        );
                                    }
                                    Some(catalog)
                                }
                                None => {
                                    tracing::warn!(
                                        name = %backend.name,
                                        "Warm catalog probe failed and no stale catalog found; backend will start without a warm cache"
                                    );
                                    None
                                }
                            }
                        }
                    }
                }
            }
        };

        lazy_backends.push(crate::lazy_registry::LazyBackend {
            config: backend.clone(),
            // Propagate the warm catalog's protocol version (when present) so the
            // registry surfaces it without re-spawning the backend (C20). A cache
            // miss that falls back to a live probe also captures the version here.
            protocol_version: catalog.as_ref().and_then(|c| c.protocol_version.clone()),
            catalog,
        });
    }

    tracing::info!(
        total = lazy_backends.len(),
        with_catalog = lazy_backends.iter().filter(|b| b.catalog.is_some()).count(),
        without_catalog = lazy_backends.iter().filter(|b| b.catalog.is_none()).count(),
        "Lazy backend registry ready"
    );

    let registry = Arc::new(
        crate::lazy_registry::LazyBackendRegistry::from_backends(lazy_backends).with_runtime(
            shared_proxy.clone(),
            &store,
            config.proxy.separator.clone(),
            config.proxy.shutdown_kill_timeout_secs,
            config.proxy.force_kill,
        ),
    );

    // Startup cache GC (C7/C24): prune (name, hash)-orphaned catalog files.
    prune_orphaned_catalogs(config, &store);

    registry
}

/// Prune warm catalog files that no longer correspond to a configured backend.
///
/// For each `{name}-{hash}.json` in the cache directory, recompute the expected
/// hash for the named backend (if it exists) and delete the file when either the
/// backend is absent or its current hash differs. Scoped by `(name, hash)` — a
/// file is never deleted based on name alone. All errors are logged and swallowed.
fn prune_orphaned_catalogs(config: &ProxyConfig, store: &crate::warm_cache::WarmCatalogStore) {
    use crate::warm_cache::BinaryHasher;

    // Map of backend name -> current identity hash for stdio backends.
    let current_hashes: std::collections::HashMap<&str, String> = config
        .backends
        .iter()
        .filter(|b| b.transport == crate::config::TransportType::Stdio)
        .map(|b| (b.name.as_str(), BinaryHasher::hash(b)))
        .collect();

    let entries = match std::fs::read_dir(store.cache_dir()) {
        Ok(e) => e,
        Err(e) => {
            tracing::debug!(error = %e, "Warm cache GC: cannot read cache dir");
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let file_stem = match path.file_stem().and_then(|s| s.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        // Only consider files shaped like `{name}-{hash}`.
        let (name, hash) = match file_stem.rsplit_once('-') {
            Some((name, hash)) => (name.to_string(), hash.to_string()),
            None => continue,
        };

        let is_orphan = match current_hashes.get(name.as_str()) {
            Some(current_hash) => current_hash != &hash,
            None => true,
        };

        if is_orphan {
            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "Warm cache GC: failed to remove orphaned catalog"
                );
            } else {
                tracing::info!(path = %path.display(), "Warm cache GC: pruned orphaned catalog");
            }
        }
    }
}

/// Apply per-backend middleware to the builder as one composed layer.
///
/// Timeout, circuit breaker, and retry are resolved from per-backend config
/// (which may include global defaults merged via [`ProxyConfig::apply_global_defaults`]).
fn apply_backend_middleware(
    mut builder: tower_mcp::proxy::McpProxyBuilder,
    backend: &crate::config::BackendConfig,
    outlier_detector: &Option<crate::outlier::OutlierDetector>,
    cb_handles: &mut HashMap<String, CbHandle>,
) -> Result<tower_mcp::proxy::McpProxyBuilder> {
    // Per-backend middleware stack (applied in order: inner -> outer).
    // Collected into one BackendMiddlewareLayer and handed to tower-mcp
    // as a single backend_layer call; see the type's doc comment.
    let mut mw = BackendMiddlewareLayer::default();

    // Retry (innermost -- retries happen before other middleware)
    if let Some(retry_cfg) = &backend.retry {
        tracing::info!(
            backend = %backend.name,
            max_retries = retry_cfg.max_retries,
            initial_backoff_ms = retry_cfg.initial_backoff_ms,
            max_backoff_ms = retry_cfg.max_backoff_ms,
            "Applying retry policy"
        );
        mw.retry = Some(crate::retry::build_retry_layer(retry_cfg, &backend.name));
    }

    // Hedging (after retry, before concurrency -- hedges are separate requests)
    if let Some(hedge_cfg) = &backend.hedging {
        let delay = Duration::from_millis(hedge_cfg.delay_ms);
        let max_attempts = hedge_cfg.max_hedges + 1; // +1 for the primary request
        tracing::info!(
            backend = %backend.name,
            delay_ms = hedge_cfg.delay_ms,
            max_hedges = hedge_cfg.max_hedges,
            "Applying request hedging"
        );
        let layer = if delay.is_zero() {
            tower_resilience::hedge::HedgeLayer::builder()
                .no_delay()
                .max_hedged_attempts(max_attempts)
                .name(format!("{}-hedge", backend.name))
                .build()
        } else {
            tower_resilience::hedge::HedgeLayer::builder()
                .delay(delay)
                .max_hedged_attempts(max_attempts)
                .name(format!("{}-hedge", backend.name))
                .build()
        };
        mw.hedge = Some(layer);
    }

    // Concurrency limit
    if let Some(cc) = &backend.concurrency {
        tracing::info!(
            backend = %backend.name,
            max = cc.max_concurrent,
            "Applying concurrency limit"
        );
        mw.stage(tower::limit::ConcurrencyLimitLayer::new(cc.max_concurrent));
    }

    // Rate limit
    if let Some(rl) = &backend.rate_limit {
        tracing::info!(
            backend = %backend.name,
            requests = rl.requests,
            period_seconds = rl.period_seconds,
            "Applying rate limit"
        );
        let layer = tower_resilience::ratelimiter::RateLimiterLayer::builder()
            .limit_for_period(rl.requests)
            .refresh_period(Duration::from_secs(rl.period_seconds))
            .name(format!("{}-ratelimit", backend.name))
            .build()?;
        mw.stage(layer);
    }

    // Timeout
    if let Some(timeout) = &backend.timeout {
        tracing::info!(
            backend = %backend.name,
            seconds = timeout.seconds,
            "Applying timeout"
        );
        mw.stage(TimeoutLayer::new(Duration::from_secs(timeout.seconds)));
    }

    // Circuit breaker
    if let Some(cb) = &backend.circuit_breaker {
        tracing::info!(
            backend = %backend.name,
            failure_rate = cb.failure_rate_threshold,
            wait_seconds = cb.wait_duration_seconds,
            "Applying circuit breaker"
        );
        let (layer, handle) = build_breaker_layer(cb, &backend.name)?;
        cb_handles.insert(backend.name.clone(), handle);
        mw.stage(layer);
    }

    // Outlier detection (outermost -- observes errors after all other middleware)
    if let Some(od) = &backend.outlier_detection
        && let Some(detector) = outlier_detector
    {
        tracing::info!(
            backend = %backend.name,
            consecutive_errors = od.consecutive_errors,
            base_ejection_seconds = od.base_ejection_seconds,
            max_ejection_percent = od.max_ejection_percent,
            "Applying outlier detection"
        );
        let layer = crate::outlier::OutlierDetectionLayer::new(
            backend.name.clone(),
            od.clone(),
            detector.clone(),
        );
        mw.outlier = Some(layer);
    }

    if !mw.is_empty() {
        builder = builder.backend_layer(mw);
    }

    Ok(builder)
}

/// Build a dynamic router that delegates endpoint group requests to the registry.
/// This enables hot reload support by looking up the current router at request time.
///
/// # Routing
///
/// Two routes are registered for each pattern:
/// - `/{group_name}/mcp` — bare MCP endpoint (no trailing path)
/// - `/{group_name}/mcp/{*path}` — sub-path under the MCP endpoint
///
/// Both routes strip the `/{group_name}/mcp` prefix and forward to the group's
/// router, which serves at `/`. The `*path` wildcard requires the `{*path}`
/// syntax introduced in matchit 0.8+ and needs at least one path segment.
pub fn build_dynamic_endpoint_group_router(
    router: Router,
    registry: crate::endpoint_router::EndpointGroupRegistry,
) -> Router {
    // Route 1: /{group_name}/mcp (bare endpoint, no trailing path)
    let reg1 = registry.clone();
    let router = router.route(
        "/{group_name}/mcp",
        any(move |Path(group_name): Path<String>, req: Request| {
            let registry = reg1.clone();
            async move {
                if let Some(group_router) = registry.get(&group_name) {
                    // Strip the /{group_name}/mcp prefix → group router serves at /
                    let mut new_req = req;
                    *new_req.uri_mut() = "/".parse().unwrap();
                    group_router
                        .router
                        .clone()
                        .call(new_req)
                        .await
                        .unwrap_or_else(|_| {
                            (StatusCode::INTERNAL_SERVER_ERROR, "Router error").into_response()
                        })
                } else {
                    (StatusCode::NOT_FOUND, "Endpoint group not found").into_response()
                }
            }
        }),
    );

    // Route 2: /{group_name}/mcp/{*path} (sub-path under MCP endpoint)
    router.route(
        "/{group_name}/mcp/{*path}",
        any(
            move |Path((group_name, sub_path)): Path<(String, String)>, req: Request| {
                let registry = registry.clone();
                async move {
                    if let Some(group_router) = registry.get(&group_name) {
                        // Strip the /{group_name}/mcp prefix → group router serves at /
                        let mut new_req = req;
                        *new_req.uri_mut() = format!("/{sub_path}").parse().unwrap();
                        group_router
                            .router
                            .clone()
                            .call(new_req)
                            .await
                            .unwrap_or_else(|_| {
                                (StatusCode::INTERNAL_SERVER_ERROR, "Router error").into_response()
                            })
                    } else {
                        (StatusCode::NOT_FOUND, "Endpoint group not found").into_response()
                    }
                }
            },
        ),
    )
}

impl Proxy {
    /// Build a proxy from a [`ProxyConfig`].
    ///
    /// Runs [`ProxyConfig::prepare`] before any backend or runtime side effects,
    /// including for directly deserialized or subsequently mutated configs.
    /// Connects to all backends, builds the middleware stack, and prepares
    /// the axum router. Call [`serve()`](Self::serve) to run standalone or
    /// [`into_router()`](Self::into_router) to embed in an existing app.
    pub async fn from_config(config: ProxyConfig) -> Result<Self> {
        Self::from_config_inner(
            config,
            #[cfg(feature = "metrics")]
            None,
        )
        .await
    }

    /// Construct with a shared/custom Prometheus owner, without global state.
    ///
    /// Requires `observability.metrics.enabled = true`. Clone one recorder to
    /// aggregate multiple proxies, or supply distinct owners for isolation.
    /// Library callers independently own their tracing subscriber/provider.
    #[cfg(feature = "metrics")]
    pub async fn from_config_with_metrics(
        config: ProxyConfig,
        metrics: crate::metrics::MetricsRecorder,
    ) -> Result<Self> {
        Self::from_config_inner(config, Some(metrics)).await
    }

    async fn from_config_inner(
        mut config: ProxyConfig,
        #[cfg(feature = "metrics")] metrics: Option<crate::metrics::MetricsRecorder>,
    ) -> Result<Self> {
        config.prepare()?;
        #[cfg(feature = "metrics")]
        let metrics = if config.observability.metrics.enabled {
            Some(metrics.unwrap_or_default())
        } else {
            anyhow::ensure!(
                metrics.is_none(),
                "injected metrics require observability.metrics.enabled = true"
            );
            None
        };
        let reload_policy = crate::reload_policy::ReloadPolicy::new(&config);
        let runtime = crate::lifecycle::Runtime::new(reload_policy.connections().clone());
        let result = Self::construct(
            config,
            reload_policy,
            runtime.clone(),
            #[cfg(feature = "metrics")]
            metrics,
        )
        .await;
        if result.is_err() && runtime.shutdown().await.is_err() {
            tracing::error!("Proxy cleanup after construction failure failed");
        }
        result
    }

    async fn construct(
        mut config: ProxyConfig,
        reload_policy: crate::reload_policy::ReloadPolicy,
        runtime: std::sync::Arc<crate::lifecycle::Runtime>,
        #[cfg(feature = "metrics")] metrics: Option<crate::metrics::MetricsRecorder>,
    ) -> Result<Self> {
        config.apply_global_defaults();
        let endpoint_group_registry = crate::endpoint_router::EndpointGroupRegistry::new();

        let all_backend_refs: Vec<&crate::config::BackendConfig> = config
            .backends
            .iter()
            .filter(|b| {
                !(b.spawn_mode == crate::config::SpawnMode::Lazy
                    && b.transport == crate::config::TransportType::Stdio)
            })
            .collect();

        let (shared_proxy, cb_handles) = build_mcp_proxy_for_backends(
            &config.proxy.name,
            &config.proxy.version,
            &config.proxy.separator,
            config.proxy.instructions.as_ref(),
            &all_backend_refs,
            config.proxy.shutdown_kill_timeout_secs,
            config.proxy.force_kill,
        )
        .await?;

        *runtime.proxy.lock().expect("backend owner") = Some(shared_proxy.clone());

        let lazy_registry = build_lazy_registry(&config, shared_proxy.clone()).await;

        // Start the idle sweeper for lazy backends (Wave 6). Polls every 5s; the
        // per-backend `idle_timeout_secs` gates actual idle-out. Session-based
        // backends are kept alive (C3) unless they set `idle_teardown = "always"`
        // (ADR-0007).
        lazy_registry.start_sweeper(5);

        let proxy_for_admin = shared_proxy.clone();
        // Only the discovery index build mutates this clone.
        #[cfg_attr(not(feature = "discovery"), allow(unused_mut))]
        let mut proxy_for_caller = shared_proxy.clone();
        let proxy_for_management = shared_proxy.clone();

        // Step 3: Build endpoint group routers using the shared McpProxy.
        // Each group clones the shared proxy and applies group-specific middleware.
        let _endpoint_group_routers = endpoint_router::build_endpoint_group_routers(
            &config,
            Some(&endpoint_group_registry),
            Some(&shared_proxy),
            lazy_registry.clone(),
        )
        .await?;

        #[cfg(feature = "metrics")]
        let metrics_handle = metrics
            .as_ref()
            .map(crate::metrics::MetricsRecorder::handle);
        #[cfg(not(feature = "metrics"))]
        let metrics_handle = None;

        let (service, cache_handle, alias_map) =
            build_middleware_stack(&config, shared_proxy.clone(), lazy_registry.clone())?;
        #[cfg(feature = "metrics")]
        let service = if let Some(metrics) = metrics {
            BoxCloneService::new(crate::metrics::MetricsService::with_recorder(
                service, metrics, &config,
            ))
        } else {
            service
        };
        let service = if config.observability.tracing.enabled {
            BoxCloneService::new(crate::telemetry::RequestSpans::new(service, &config))
        } else {
            service
        };

        let protocol_support = build_protocol_support(&config)?;

        let (router, session_handle) =
            tower_mcp::transport::http::HttpTransport::from_service(service)
                .bridge_extension::<crate::management::Identity>()
                .session_principal_resolver(crate::management::session_principal)
                .protocol_support(protocol_support)
                .into_router_with_handle();

        // Auto-inject Mcp-Method and MCP-Protocol-Version headers for backward compatibility
        let router = router.layer(axum::middleware::from_fn(
            crate::mcp_compat::inject_mcp_compat_headers,
        ));

        // CORS layer for browser clients
        let router = if let Some(cors_config) = &config.security.cors {
            let mut cors = CorsLayer::new();
            if let Some(ref origins) = cors_config.allowed_origins {
                for origin in origins {
                    cors = cors.allow_origin(origin.parse::<axum::http::HeaderValue>()?);
                }
            } else {
                cors = cors.allow_origin(Any);
            }
            if let Some(ref methods) = cors_config.allowed_methods {
                let methods: Vec<axum::http::Method> = methods
                    .iter()
                    .map(|m| m.parse())
                    .collect::<Result<Vec<_>, _>>()
                    .context("invalid CORS method")?;
                cors = cors.allow_methods(methods);
            } else {
                cors = cors.allow_methods(Any);
            }
            if let Some(ref headers) = cors_config.allowed_headers {
                let headers: Vec<axum::http::HeaderName> = headers
                    .iter()
                    .map(|h| h.parse())
                    .collect::<Result<Vec<_>, _>>()
                    .context("invalid CORS header name")?;
                cors = cors.allow_headers(headers);
            } else {
                cors = cors.allow_headers(Any);
            }
            if cors_config.allow_credentials.unwrap_or(false) {
                cors = cors.allow_credentials(true);
            }
            tracing::info!(?cors_config, "CORS enabled");
            router.layer(cors)
        } else {
            router
        };

        // Inbound authentication (axum-level middleware)
        let transport = router.clone();
        let router = apply_auth(&config, router).await?;
        let router = crate::management::authenticate(&config, transport, router);

        // Build dynamic router that delegates to endpoint group registry
        let router = build_dynamic_endpoint_group_router(router, endpoint_group_registry.clone());

        // Collect backend metadata for the health checker
        let backend_meta: std::collections::HashMap<String, BackendMeta> = config
            .backends
            .iter()
            .map(|b| {
                (
                    b.name.clone(),
                    BackendMeta {
                        transport: format!("{:?}", b.transport).to_lowercase(),
                    },
                )
            })
            .collect();

        // Admin API
        *runtime.sessions.lock().expect("session owner") = Some(session_handle.clone());
        let (admin_state, health_checker) = crate::admin::spawn_health_checker(
            proxy_for_admin,
            config.proxy.name.clone(),
            config.proxy.version.clone(),
            config.backends.len(),
            backend_meta,
        );
        *runtime.health.lock().expect("health checker owner") = Some(health_checker);
        let router = router.nest(
            "/admin",
            crate::admin::admin_router_with_policy(
                admin_state.clone(),
                metrics_handle,
                session_handle.clone(),
                cache_handle,
                proxy_for_management,
                &config,
                config.source_path.clone(),
                cb_handles,
                reload_policy.clone(),
            ),
        );
        if crate::management::admin_token(&config).is_some() {
            tracing::info!("HTTP/MCP management enabled with a dedicated credential");
        } else {
            tracing::info!("Management disabled: configure security.admin_token to enable it");
        }

        // These process probes expose no backend/configuration information and
        // are added after authentication layers so orchestrators need no token.
        // Readiness means construction completed, not that every backend is up.
        let router = router
            .route("/livez", axum::routing::get(|| async { "ok" }))
            .route("/readyz", axum::routing::get(|| async { "ok" }));

        let router = if config.observability.tracing.enabled {
            crate::telemetry::http_spans(router)
        } else {
            router
        };

        // Build discovery index if enabled (search mode implies discovery)
        #[cfg(feature = "discovery")]
        let discovery_enabled = config.proxy.tool_discovery
            || config.proxy.tool_exposure == crate::config::ToolExposure::Search;
        #[cfg(feature = "discovery")]
        let (discovery_index, discovery_schemas, discovery_tools) = if discovery_enabled {
            let (index, schemas) = crate::discovery::build_index_with_schemas(
                &mut proxy_for_caller,
                &config.proxy.separator,
            )
            .await;
            let tools = crate::discovery::build_discovery_tools_with_schemas(
                index.clone(),
                schemas.clone(),
            );
            (Some(index), Some(schemas), Some(tools))
        } else {
            (None, None, None)
        };
        #[cfg(not(feature = "discovery"))]
        let discovery_tools: Option<Vec<tower_mcp::Tool>> = None;

        // MCP admin tools (proxy/ namespace)
        if let Err(e) = crate::admin_tools::register_admin_tools_with_policy(
            &proxy_for_caller,
            admin_state,
            session_handle.clone(),
            &config,
            discovery_tools,
            reload_policy.clone(),
        )
        .await
        {
            tracing::warn!("Failed to register admin tools: {e}");
        } else {
            tracing::info!("MCP admin tools registered under proxy/ namespace");
        }

        Ok(Self {
            router,
            session_handle,
            inner: proxy_for_caller,
            shared_proxy,
            config,
            runtime,
            reload_policy,
            endpoint_group_registry,
            lazy_registry,
            alias_map,
            hot_reload_handle: None,
            #[cfg(feature = "discovery")]
            discovery_index,
            #[cfg(feature = "discovery")]
            discovery_schemas,
        })
    }

    /// Get a reference to the session handle for monitoring active sessions.
    pub fn session_handle(&self) -> &SessionHandle {
        &self.session_handle
    }

    /// Get a reference to the underlying [`McpProxy`] for dynamic operations.
    ///
    /// Use this trusted administration handle to add backends dynamically via
    /// [`McpProxy::add_backend()`]. It bypasses the configured HTTP/MCP credential
    /// boundary; expose [`Self::into_router`] to untrusted clients instead.
    /// Retain a proxy/router/shutdown owner while using a raw clone. Transports
    /// added directly through this upstream API bypass managed transport
    /// registration and remain the trusted caller's cleanup responsibility.
    pub fn mcp_proxy(&self) -> &McpProxy {
        &self.inner
    }

    /// Get a clone of the shared [`McpProxy`] containing ALL backends.
    ///
    /// Endpoint groups clone this and apply group-specific filtering/middleware.
    /// Each stdio backend was spawned exactly once when this proxy was built.
    pub fn shared_proxy(&self) -> McpProxy {
        self.shared_proxy.clone()
    }

    /// Get a clone of the lazy backend registry.
    ///
    /// Tracks per-backend warm catalogs and spawn state for backends configured
    /// with `spawn_mode = "lazy"`. Shared across the root stack, every
    /// endpoint-group stack, and the hot-reload path so a backend referenced by
    /// multiple groups uses ONE child process (C5/C18).
    pub fn lazy_registry(&self) -> Arc<crate::lazy_registry::LazyBackendRegistry> {
        self.lazy_registry.clone()
    }

    /// Enable hot reload by watching the given config file path.
    ///
    /// New backends added to the config file will be connected dynamically
    pub fn enable_hot_reload(
        &mut self,
        config_path: std::path::PathBuf,
        watchers: Vec<crate::config::WatcherConfig>,
    ) {
        let mut watcher = self.runtime.watcher.lock().expect("config watcher owner");
        if watcher.is_some() || self.runtime.connections.stopped() {
            return;
        }
        tracing::info!("Hot reload enabled, watching config file for changes");
        *watcher = Some(crate::reload::spawn_config_watcher(
            config_path,
            self.inner.clone(),
            self.shared_proxy.clone(),
            self.endpoint_group_registry.clone(),
            watchers,
            self.alias_map.clone(),
            self.lazy_registry.clone(),
            #[cfg(feature = "discovery")]
            self.discovery_index
                .as_ref()
                .zip(self.discovery_schemas.as_ref())
                .map(|(idx, schemas)| {
                    (
                        idx.clone(),
                        schemas.clone(),
                        self.config.proxy.separator.clone(),
                    )
                }),
        ));
    }

    /// Consume the proxy and return the axum Router and SessionHandle.
    ///
    /// Use this to embed the proxy in an existing axum application:
    ///
    /// ```rust,no_run
    /// use axum::{Router, routing::get};
    /// # fn embed(proxy: mcp_proxy::Proxy) {
    /// let (proxy_router, session_handle) = proxy.into_router();
    ///
    /// let app = Router::new()
    ///     .merge(proxy_router)
    ///     .route("/app-health", get(|| async { "ok" }));
    /// # }
    /// ```
    pub fn into_router(self) -> (Router, SessionHandle) {
        let runtime = self.runtime;
        let gate = runtime.clone();
        let router = self
            .router
            .layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let runtime = gate.clone();
                    async move {
                        if runtime.connections.stopped() {
                            axum::response::IntoResponse::into_response(
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            )
                        } else {
                            next.run(request).await
                        }
                    }
                },
            ))
            .layer(axum::Extension(runtime));
        (router, self.session_handle)
    }

    /// Retain an explicit shutdown handle before embedding or serving.
    pub fn shutdown_handle(&self) -> crate::ShutdownHandle {
        crate::ShutdownHandle(self.runtime.clone())
    }

    /// Close sessions/backends and cancel/join owned workers without serving.
    pub async fn shutdown(self) -> Result<()> {
        self.runtime.shutdown().await
    }

    /// Serve until SIGTERM/SIGINT, draining at most the configured timeout.
    pub async fn serve(mut self) -> Result<()> {
        let addr = format!(
            "{}:{}",
            self.config.proxy.listen.host, self.config.proxy.listen.port
        );
        let timeout = Duration::from_secs(self.config.proxy.shutdown_timeout_seconds);
        let runtime = self.runtime.clone();
        // Tuple binding supports bare IPv6; accept bracketed IPv6 config too.
        let host = self
            .config
            .proxy
            .listen
            .host
            .trim_start_matches('[')
            .trim_end_matches(']');
        let listener =
            match tokio::net::TcpListener::bind((host, self.config.proxy.listen.port)).await {
                Ok(listener) => listener,
                Err(error) => {
                    if runtime.shutdown().await.is_err() {
                        tracing::error!("Proxy cleanup after bind failure failed");
                    }
                    return Err(error).with_context(|| format!("binding to {}", addr));
                }
            };
        tracing::info!(listen = %addr, "Proxy ready");
        let lazy_registry = self.lazy_registry.clone();
        let shared_proxy = self.shared_proxy.clone();
        let configured_backends = self.config.backends.clone();
        let global_force_kill = self.config.proxy.force_kill;
        let warm_cache_path = warm_cache_dir(&self.config);
        let runtime = self.runtime.clone();

        if let Some(handle) = self.hot_reload_handle.take() {
            handle.abort();
            tracing::info!("Hot-reload watcher stopped for shutdown");
        }

        let (router, _) = self.into_router();
        let result =
            crate::server::serve(listener, router, shutdown_signal(timeout), timeout).await;

        let lazy_names: std::collections::HashSet<String> =
            lazy_registry.names().into_iter().collect();
        let eager_snapshot: Vec<String> = shared_proxy
            .backend_namespaces()
            .into_iter()
            .filter(|n| !lazy_names.contains(n))
            .collect();

        let mut terminated = lazy_registry.shutdown_all().await;
        for name in &eager_snapshot {
            if let Some(cfg) = configured_backends.iter().find(|b| &b.name == name) {
                if cfg.transport != crate::config::TransportType::Stdio
                    || cfg.singleton_externally_managed
                {
                    continue;
                }
                let force = cfg.effective_force_kill(global_force_kill);
                let mode = if force { "force" } else { "graceful" };
                remove_eager_backend(&shared_proxy, name, mode).await;
                terminated.push(name.clone());
            } else {
                remove_eager_backend(&shared_proxy, name, "unknown").await;
                terminated.push(name.clone());
            }
        }
        {
            let store = crate::warm_cache::WarmCatalogStore::new(warm_cache_path);
            let _ = invalidate_terminated_backends(&store, &terminated);
        }

        let cleanup = runtime.shutdown().await;
        result?;
        cleanup?;
        tracing::info!("Proxy shut down");
        Ok(())
    }
}

/// Circuit breaker handle type alias.
pub type CbHandle =
    tower_resilience::circuitbreaker::CircuitBreakerHandle<crate::outcome::McpFailureClassifier>;

/// Remove one eager backend during shutdown with per-backend logging.
async fn remove_eager_backend(shared_proxy: &tower_mcp::proxy::McpProxy, name: &str, mode: &str) {
    let started = std::time::Instant::now();
    let removed = shared_proxy.remove_backend(name).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    if removed {
        tracing::info!(backend = %name, mode = %mode, elapsed_ms, "Eager backend shut down");
    } else {
        tracing::info!(backend = %name, mode = %mode, elapsed_ms, "Eager backend already gone at shutdown");
    }
}

/// Build the circuit breaker layer for one backend from its config.
///
/// The count-based breaker only evaluates once its sliding window is full,
/// and the window defaults to 100 calls. Aligning the window with
/// `minimum_calls` makes the failure rate cover the most recent
/// `minimum_calls` calls and evaluation begin exactly at the documented
/// threshold (#220).
fn build_breaker_layer(
    cb: &crate::config::CircuitBreakerConfig,
    backend_name: &str,
) -> Result<(
    tower_resilience::circuitbreaker::CircuitBreakerLayer<crate::outcome::McpFailureClassifier>,
    CbHandle,
)> {
    tower_resilience::circuitbreaker::CircuitBreakerLayer::builder()
        .failure_classifier_type(crate::outcome::McpFailureClassifier)
        .failure_rate_threshold(cb.failure_rate_threshold)
        .minimum_number_of_calls(cb.minimum_calls)
        .sliding_window_size(cb.minimum_calls)
        .wait_duration_in_open(Duration::from_secs(cb.wait_duration_seconds))
        .permitted_calls_in_half_open(cb.permitted_calls_in_half_open)
        .name(format!("{backend_name}-cb"))
        .build_with_handle()
        .map_err(Into::into)
}

type InfallibleService = BoxCloneService<RouterRequest, RouterResponse, Infallible>;
type MwService = BoxCloneService<RouterRequest, RouterResponse, tower::BoxError>;
type MwStage = Box<dyn Fn(MwService) -> MwService + Send + Sync>;

/// Every configured per-backend middleware composed into ONE tower-mcp
/// backend layer, built from the backend's full middleware configuration in
/// one place. tower-mcp's `backend_layer` used to replace the previously
/// applied layer rather than stack (joshrotenberg/tower-mcp#1173, fixed
/// upstream in 0.22.0); single-call composition predates that fix and
/// remains correct, producing one `CatchError` fold across all zones instead
/// of one per configured middleware.
///
/// Composition happens in three zones matching the middlewares' type
/// contracts: retry and hedging operate on the raw `Error = Infallible`
/// backend service (both classify failures from the response, and hedging
/// requires a `Clone` error type); concurrency, rate limit, timeout, and
/// circuit breaker operate in a widened `BoxError` plane where they can
/// produce service-level errors; a `CatchError` fold then converts those
/// errors into JSON-RPC error responses, and outlier detection sits on top,
/// observing the response plane (its `Service` impl requires
/// `Error = Infallible`).
#[derive(Default)]
struct BackendMiddlewareLayer {
    /// Innermost: retries against the raw backend service.
    retry: Option<RetryLayer<RouterRequest, RouterResponse, Infallible>>,
    /// Outside retry, still in the Infallible zone.
    hedge: Option<tower_resilience::hedge::HedgeLayer>,
    /// BoxError zone, applied outward in push order: concurrency, rate
    /// limit, timeout, circuit breaker.
    stages: Vec<MwStage>,
    /// Outermost, after the error fold: observes response-plane errors.
    outlier: Option<crate::outlier::OutlierDetectionLayer>,
}

impl BackendMiddlewareLayer {
    fn is_empty(&self) -> bool {
        self.retry.is_none()
            && self.hedge.is_none()
            && self.stages.is_empty()
            && self.outlier.is_none()
    }

    fn stage<L>(&mut self, layer: L)
    where
        L: Layer<MwService> + Send + Sync + 'static,
        L::Service:
            tower::Service<RouterRequest, Response = RouterResponse> + Clone + Send + 'static,
        <L::Service as tower::Service<RouterRequest>>::Error: std::fmt::Display,
        <L::Service as tower::Service<RouterRequest>>::Future: Send + 'static,
    {
        self.stages.push(Box::new(move |inner| {
            BoxCloneService::new(
                layer
                    .layer(inner)
                    .map_err(|e| -> tower::BoxError { e.to_string().into() }),
            )
        }));
    }
}

impl<S> Layer<S> for BackendMiddlewareLayer
where
    S: tower::Service<RouterRequest, Response = RouterResponse, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Service = InfallibleService;

    fn layer(&self, base: S) -> InfallibleService {
        fn widen(e: Infallible) -> tower::BoxError {
            match e {}
        }

        // Zone A: retry classifies responses over the Infallible base.
        // The hedge adapter promotes failed responses into a private Clone
        // error plane, then restores the original response after all failures.
        let after_retry: InfallibleService = match &self.retry {
            Some(retry) => BoxCloneService::new(retry.layer(base)),
            None => BoxCloneService::new(base),
        };

        // Zone B: concurrency/rate/timeout/breaker can produce typed errors.
        // Hedging has already restored its response plane before widening.
        let mut svc: MwService = match &self.hedge {
            Some(hedge) => {
                BoxCloneService::new(crate::outcome::hedged(after_retry, hedge).map_err(widen))
            }
            None => BoxCloneService::new(after_retry.map_err(widen)),
        };
        for stage in &self.stages {
            svc = stage(svc);
        }

        // Fold middleware errors into JSON-RPC error responses.
        let folded: InfallibleService =
            BoxCloneService::new(tower_mcp::transport::CatchError::new(svc));

        // Zone C: outlier detection observes the response plane.
        match &self.outlier {
            Some(outlier) => BoxCloneService::new(outlier.layer(folded)),
            None => folded,
        }
    }
}

/// Build the HTTP client config for a backend.
///
/// [`HttpClientTransport`] defaults to a 30 second `request_timeout`. Left at
/// that default it caps every HTTP backend request at 30 seconds no matter how
/// large `[backends.timeout]` is, because the configured backend timeout is
/// applied separately as a [`TimeoutLayer`] above the transport. Carrying the
/// configured timeout into the transport keeps the Tower layer the effective
/// deadline. Backends with no configured timeout keep the transport default.
pub(crate) fn http_client_config(backend: &BackendConfig) -> HttpClientConfig {
    let mut config = HttpClientConfig::default();
    if let Some(timeout) = &backend.timeout {
        config.request_timeout = Duration::from_secs(timeout.seconds);
    }
    config
}

/// Build an HTTP client transport for `backend`, applying its
/// custom HTTP client settings (`[backends.http]`) or else its configured
/// request timeout, then its bearer token and custom headers.
pub(crate) fn build_http_transport(
    url: &str,
    backend: &BackendConfig,
) -> Result<HttpClientTransport> {
    let mut transport = if let Some(http_cfg) = &backend.http {
        tracing::info!(
            name = %backend.name,
            connect_timeout = http_cfg.connect_timeout_secs,
            request_timeout = http_cfg.timeout_secs,
            "Using custom HTTP client config"
        );
        let client = reqwest::ClientBuilder::from(http_cfg).build()?;
        HttpClientTransport::with_client_and_config(url, client, HttpClientConfig::from(http_cfg))
    } else {
        HttpClientTransport::with_config(url, http_client_config(backend))
    };
    if let Some(token) = &backend.bearer_token
        && !backend
            .headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("authorization"))
    {
        transport = transport.bearer_token(token);
    }
    for (name, value) in &backend.headers {
        transport = transport.header(name, value);
    }
    Ok(transport)
}

/// Circuit breaker handle type alias.
/// Build a scope-enforcement layer from configured OAuth `required_scopes`.
///
/// Returns `None` when no scopes are required (the layer would be a no-op).
/// Otherwise returns a [`ScopeEnforcementLayer`](tower_mcp::oauth::ScopeEnforcementLayer)
/// whose default policy requires *all* of `required_scopes` to be present in the
/// token (AND semantics) for every request.
#[cfg(feature = "oauth")]
fn oauth_scope_layer(
    required_scopes: &[String],
) -> Option<tower_mcp::oauth::ScopeEnforcementLayer> {
    if required_scopes.is_empty() {
        return None;
    }
    let policy = tower_mcp::oauth::ScopePolicy::new().default_scopes(
        tower_mcp::oauth::ScopeRequirement::all(required_scopes.iter().cloned()),
    );
    Some(tower_mcp::oauth::ScopeEnforcementLayer::new(policy))
}

/// Apply caller policy to a public tool name before aliases or backend dispatch.
fn invocation_policy(
    config: &ProxyConfig,
    #[allow(unused_mut)] mut service: BoxCloneService<RouterRequest, RouterResponse, Infallible>,
) -> Result<BoxCloneService<RouterRequest, RouterResponse, Infallible>> {
    #[cfg(feature = "oauth")]
    if matches!(
        &config.auth,
        Some(AuthConfig::Bearer {
            scoped_tokens,
            ..
        }) if !scoped_tokens.is_empty()
    ) {
        tracing::info!("Enabling bearer token scoping middleware");
        service = BoxCloneService::new(crate::bearer_scope::BearerScopingService::new(service));
    }

    #[cfg(feature = "oauth")]
    {
        let rbac_config = match &config.auth {
            Some(
                AuthConfig::Jwt {
                    roles,
                    role_mapping: Some(mapping),
                    ..
                }
                | AuthConfig::OAuth {
                    roles,
                    role_mapping: Some(mapping),
                    ..
                },
            ) => {
                tracing::info!(
                    roles = roles.len(),
                    claim = %mapping.claim,
                    "Enabling RBAC"
                );
                Some(RbacConfig::new(roles, mapping)?)
            }
            _ => None,
        };

        if let Some(rbac) = rbac_config {
            service = BoxCloneService::new(RbacService::new(service, rbac));
        }

        let required_scopes: &[String] = match &config.auth {
            Some(AuthConfig::OAuth {
                required_scopes, ..
            }) => required_scopes,
            _ => &[],
        };
        if let Some(layer) = oauth_scope_layer(required_scopes) {
            tracing::info!(
                scopes = ?required_scopes,
                "Enabling OAuth required_scopes enforcement"
            );
            service = BoxCloneService::new(tower::Layer::layer(&layer, service));
        }
    }

    Ok(BoxCloneService::new(crate::management::Guard::new(
        service,
        &config.proxy.separator,
        false,
    )))
}

/// Build a shared alias map from all backends in the config.
pub(crate) fn build_alias_map(config: &ProxyConfig) -> Option<Arc<RwLock<crate::alias::AliasMap>>> {
    let alias_mappings: Vec<_> = config
        .backends
        .iter()
        .flat_map(|b| {
            let ns = format!("{}{}", b.name, config.proxy.separator);
            b.aliases
                .iter()
                .map(move |a| (ns.clone(), a.from.clone(), a.to.clone()))
        })
        .collect();

    let rename_all_mappings: Vec<_> = config
        .backends
        .iter()
        .flat_map(|b| {
            let ns = format!("{}{}", b.name, config.proxy.separator);
            b.rename_all
                .iter()
                .map(move |r| (ns.clone(), r.from.clone(), r.to.clone()))
        })
        .collect();

    crate::alias::AliasMap::new(alias_mappings, rename_all_mappings)
        .map(|m| Arc::new(RwLock::new(m)))
}

/// Build the [`tower_mcp::ProtocolSupport`] advertised by the HTTP transport.
pub fn build_protocol_support(config: &ProxyConfig) -> Result<tower_mcp::ProtocolSupport> {
    let versions = &config.proxy.protocol_support.versions;
    if versions.is_empty() {
        Ok(
            tower_mcp::ProtocolSupport::try_new(["2026-07-28", "2025-11-25"])
                .expect("default protocol versions are valid"),
        )
    } else {
        tower_mcp::ProtocolSupport::try_new(versions.iter().map(|s| s.as_str()))
            .context("invalid protocol versions")
    }
}

/// Apply the three innermost 2026-07-28 layers to a service.
pub fn apply_2026_layers(
    service: BoxCloneService<RouterRequest, RouterResponse, Infallible>,
    config: &ProxyConfig,
) -> BoxCloneService<RouterRequest, RouterResponse, Infallible> {
    let service =
        BoxCloneService::new(crate::subscriptions::SubscriptionsListenLayer.layer(service));
    let service = BoxCloneService::new(discover::DiscoverLayer::new(config).layer(service));
    BoxCloneService::new(meta_validation::MetaValidationLayer::new().layer(service))
}

/// Return type for [`build_middleware_stack`].
type MiddlewareStack = (
    BoxCloneService<RouterRequest, RouterResponse, Infallible>,
    Option<cache::CacheHandle>,
    Option<Arc<RwLock<crate::alias::AliasMap>>>,
);

/// Build the MCP-level middleware stack around the proxy.
fn build_middleware_stack(
    config: &ProxyConfig,
    proxy: McpProxy,
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
) -> Result<MiddlewareStack> {
    let admin_service = crate::management::Guard::new(proxy.clone(), &config.proxy.separator, true);
    let mut service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(crate::management::Guard::new(
            proxy,
            &config.proxy.separator,
            false,
        ));

    service = apply_2026_layers(service, config);

    service = BoxCloneService::new(
        crate::warm_catalog_service::WarmCatalogLayer::new(
            lazy_registry,
            config.proxy.separator.clone(),
            None,
        )
        .layer(service),
    );
    let mut cache_handle: Option<cache::CacheHandle> = None;

    // Argument injection (innermost -- merges default/per-tool args into CallTool requests)
    let injection_rules: Vec<_> = config
        .backends
        .iter()
        .filter(|b| !b.default_args.is_empty() || !b.inject_args.is_empty())
        .map(|b| {
            let namespace = format!("{}{}", b.name, config.proxy.separator);
            tracing::info!(
                backend = %b.name,
                default_args = b.default_args.len(),
                tool_rules = b.inject_args.len(),
                "Applying argument injection"
            );
            crate::inject::InjectionRules::new(
                namespace,
                b.default_args.clone(),
                b.inject_args.clone(),
            )
        })
        .collect();

    if !injection_rules.is_empty() {
        service = BoxCloneService::new(crate::inject::InjectArgsService::new(
            service,
            injection_rules,
        ));
    }

    // Parameter overrides (after inject, before filter -- hides/renames tool params)
    let param_overrides: Vec<_> = config
        .backends
        .iter()
        .filter(|b| !b.param_overrides.is_empty())
        .flat_map(|b| {
            let namespace = format!("{}{}", b.name, config.proxy.separator);
            tracing::info!(
                backend = %b.name,
                overrides = b.param_overrides.len(),
                "Applying parameter overrides"
            );
            b.param_overrides
                .iter()
                .map(move |c| crate::param_override::ToolOverride::new(&namespace, c))
        })
        .collect();

    if !param_overrides.is_empty() {
        service = BoxCloneService::new(crate::param_override::ParamOverrideService::new(
            service,
            param_overrides,
        ));
    }

    // Canary routing (rewrites requests from primary to canary namespace based on weight)
    let canary_mappings: std::collections::HashMap<String, (String, u32, u32)> = config
        .backends
        .iter()
        .filter_map(|b| {
            b.canary_of.as_ref().map(|primary_name| {
                // Find the primary backend's weight
                let primary_weight = config
                    .backends
                    .iter()
                    .find(|p| p.name == *primary_name)
                    .map(|p| p.weight)
                    .unwrap_or(100);
                (
                    primary_name.clone(),
                    (b.name.clone(), primary_weight, b.weight),
                )
            })
        })
        .collect();

    if !canary_mappings.is_empty() {
        for (primary, (canary, pw, cw)) in &canary_mappings {
            tracing::info!(
                primary = %primary,
                canary = %canary,
                primary_weight = pw,
                canary_weight = cw,
                "Enabling canary routing"
            );
        }
        service = BoxCloneService::new(crate::canary::CanaryService::new(
            service,
            canary_mappings,
            &config.proxy.separator,
        ));
    }

    // Failover routing (deterministic fallback on primary error)
    // Collect failover backends grouped by primary, sorted by priority (ascending).
    let mut failover_groups: std::collections::HashMap<String, Vec<(u32, String)>> =
        std::collections::HashMap::new();
    for b in &config.backends {
        if let Some(ref primary) = b.failover_for {
            failover_groups
                .entry(primary.clone())
                .or_default()
                .push((b.priority, b.name.clone()));
        }
    }
    // Sort each group by priority (lower = preferred)
    let failover_mappings: std::collections::HashMap<String, Vec<String>> = failover_groups
        .into_iter()
        .map(|(primary, mut backends)| {
            backends.sort_by_key(|(priority, _)| *priority);
            let names: Vec<String> = backends.into_iter().map(|(_, name)| name).collect();
            (primary, names)
        })
        .collect();

    if !failover_mappings.is_empty() {
        for (primary, failovers) in &failover_mappings {
            tracing::info!(
                primary = %primary,
                failovers = ?failovers,
                "Enabling failover routing"
            );
        }
        service = BoxCloneService::new(crate::failover::FailoverService::new(
            service,
            failover_mappings,
            &config.proxy.separator,
        ));
    }

    // Traffic mirroring (sends cloned requests through the proxy)
    let mirror_mappings: std::collections::HashMap<String, (String, u32)> = config
        .backends
        .iter()
        .filter_map(|b| {
            b.mirror_of
                .as_ref()
                .map(|source| (source.clone(), (b.name.clone(), b.mirror_percent)))
        })
        .collect();

    if !mirror_mappings.is_empty() {
        for (source, (mirror, pct)) in &mirror_mappings {
            tracing::info!(
                source = %source,
                mirror = %mirror,
                percent = pct,
                "Enabling traffic mirroring"
            );
        }
        service = BoxCloneService::new(crate::mirror::MirrorService::new(
            service,
            mirror_mappings,
            &config.proxy.separator,
        ));
    }

    // Response caching
    let cache_configs: Vec<_> = config
        .backends
        .iter()
        .filter_map(|b| {
            b.cache
                .as_ref()
                .map(|c| (format!("{}{}", b.name, config.proxy.separator), c))
        })
        .collect();

    if !cache_configs.is_empty() {
        for (ns, cfg) in &cache_configs {
            tracing::info!(
                backend = %ns.trim_end_matches(&config.proxy.separator),
                resource_ttl = cfg.resource_ttl_seconds,
                tool_ttl = cfg.tool_ttl_seconds,
                max_entries = cfg.max_entries,
                "Applying response cache"
            );
        }
        let (cache_svc, handle) =
            cache::CacheService::try_new(service, cache_configs, &config.cache)?;
        service = BoxCloneService::new(cache_svc);
        cache_handle = Some(handle);
    }

    // Request coalescing
    if config.performance.coalesce_requests {
        tracing::info!("Request coalescing enabled");
        service = BoxCloneService::new(coalesce::CoalesceService::with_config(
            service,
            &config.performance.coalesce,
        )?);
    }

    // Request validation
    if config.security.max_argument_size.is_some() {
        let validation = ValidationConfig {
            max_argument_size: config.security.max_argument_size,
        };
        if let Some(max) = validation.max_argument_size {
            tracing::info!(max_argument_size = max, "Applying request validation");
        }
        service = BoxCloneService::new(ValidationService::new(service, validation));
    }

    // Static capability filtering
    let filters: Vec<_> = config
        .backends
        .iter()
        .filter_map(|b| b.build_filter(&config.proxy.separator).transpose())
        .collect::<anyhow::Result<Vec<_>>>()?;

    if !filters.is_empty() {
        for f in &filters {
            tracing::info!(
                backend = %f.namespace.trim_end_matches(&config.proxy.separator),
                tool_filter = ?f.tool_filter,
                resource_filter = ?f.resource_filter,
                prompt_filter = ?f.prompt_filter,
                "Applying capability filter"
            );
        }
        service = BoxCloneService::new(CapabilityFilterService::new(service, filters));
    }

    // Search-mode filtering: hide all tools except proxy/ namespace
    if config.proxy.tool_exposure == crate::config::ToolExposure::Search {
        let prefix = format!("proxy{}", config.proxy.separator);
        tracing::info!(
            prefix = %prefix,
            "Search mode: ListTools will only show proxy/ namespace tools"
        );
        service =
            BoxCloneService::new(crate::filter::SearchModeFilterService::new(service, prefix));
    }

    // Tool aliasing
    let alias_map = build_alias_map(config);
    let alias_map_shared = alias_map.clone();
    if let Some(ref am) = alias_map_shared {
        let map = am.read().unwrap();
        let count = map.forward.len() + map.forward_rules.len();
        tracing::info!(aliases = count, "Applying tool aliases");
    }
    if let Some(ref am) = alias_map_shared {
        service = BoxCloneService::new(alias::AliasService::new_from_shared(
            service,
            Arc::clone(am),
        ));
    }

    // Tool grouping (virtual tool namespaces)
    if !config.proxy.tool_groups.is_empty()
        && let Some(tool_group_map) =
            tool_group::ToolGroupMap::new(config.proxy.tool_groups.clone(), &config.proxy.separator)
    {
        let count = tool_group_map.all_forward_mappings().len();
        tracing::info!(
            tool_groups = config.proxy.tool_groups.len(),
            mappings = count,
            "Applying tool groups"
        );
        service = BoxCloneService::new(tool_group::ToolGroupService::new(service, tool_group_map));
    }

    // Composite tools (fan-out to multiple backend tools)
    if !config.composite_tools.is_empty() {
        let count = config.composite_tools.len();
        tracing::info!(composite_tools = count, "Applying composite tool fan-out");
        // Each delegated target must pass the same caller policy independently.
        // The target service excludes composites, so delegation cannot recurse.
        let targets = invocation_policy(config, service.clone())?;
        service = BoxCloneService::new(crate::composite::CompositeService::with_targets(
            service,
            config.composite_tools.clone(),
            targets,
        ));
    }

    service = invocation_policy(config, service)?;

    // Resolve the search wrapper before the ordinary target's policy stack.
    if config.proxy.tool_exposure == crate::config::ToolExposure::Search {
        service = BoxCloneService::new(crate::invocation::SearchInvocation::new(
            service,
            &config.proxy.separator,
        ));
    }

    // Select authority before shared observability and the global rate limit,
    // but outside every data-plane cache, transformation, and policy layer.
    let data_service = crate::management::Guard::new(service, &config.proxy.separator, false);
    service = BoxCloneService::new(crate::management::Dispatch::new(
        data_service,
        admin_service,
    ));

    // Structured access logging
    if config.observability.access_log.enabled {
        tracing::info!("Access logging enabled (target: mcp::access)");
        service = BoxCloneService::new(crate::access_log::AccessLogService::new(
            service,
            &config.proxy.separator,
        ));
    }

    // Audit logging
    if config.observability.audit {
        tracing::info!("Audit logging enabled (target: mcp::audit)");
        service = BoxCloneService::new(crate::audit::AuditService::new(service));
    }

    // Global rate limit (outermost -- protects entire proxy)
    if let Some(ref rl) = config.proxy.rate_limit {
        tracing::info!(
            requests = rl.requests,
            period_seconds = rl.period_seconds,
            "Applying global rate limit"
        );
        let layer = tower_resilience::ratelimiter::RateLimiterLayer::builder()
            .limit_for_period(rl.requests)
            .refresh_period(Duration::from_secs(rl.period_seconds))
            .name("global-ratelimit")
            .build()?;
        let limited = tower::Layer::layer(&layer, service);
        service = BoxCloneService::new(tower_mcp::CatchError::new(limited));
    }

    // Per-client-identity rate limit (applied before global rate limit)
    if let Some(ref crl) = config.proxy.client_rate_limit {
        tracing::info!(
            max_requests = crl.max_requests,
            window_seconds = crl.window_seconds,
            "Applying per-client-identity rate limit"
        );
        let layer = crate::client_rate_limit::ClientIdentityRateLimitLayer::new(crl.clone());
        let limited = tower::Layer::layer(&layer, service);
        service = BoxCloneService::new(tower_mcp::CatchError::new(limited));
    }

    Ok((service, cache_handle, alias_map_shared))
}

#[cfg(feature = "oauth")]
fn with_oauth_metadata(
    router: Router,
    metadata: tower_mcp::oauth::ProtectedResourceMetadata,
) -> Result<Router> {
    let path = tower_mcp::oauth::ProtectedResourceMetadata::well_known_path_for_resource(
        &metadata.resource,
    )
    .map_err(|_| anyhow::anyhow!("invalid OAuth resource metadata URL"))?;
    Ok(router.route(
        &path,
        axum::routing::get(move || {
            let metadata = metadata.clone();
            async move { axum::Json(metadata) }
        }),
    ))
}

/// Apply inbound authentication middleware to the router.
async fn apply_auth(config: &ProxyConfig, router: Router) -> Result<Router> {
    let router = if let Some(auth) = &config.auth {
        match auth {
            AuthConfig::Bearer {
                tokens,
                scoped_tokens,
            } => {
                let total = tokens.len() + scoped_tokens.len();
                if scoped_tokens.is_empty() {
                    // Simple bearer auth: use StaticBearerValidator
                    tracing::info!(token_count = total, "Enabling bearer token auth");
                    let validator = StaticBearerValidator::new(tokens.iter().cloned());
                    let layer = AuthLayer::new(validator);
                    router.layer(layer)
                } else {
                    // Scoped bearer auth: use custom layer that injects TokenClaims
                    #[cfg(feature = "oauth")]
                    {
                        tracing::info!(
                            token_count = total,
                            scoped = scoped_tokens.len(),
                            "Enabling bearer token auth with per-token scoping"
                        );
                        let layer =
                            crate::bearer_scope::ScopedBearerAuthLayer::new(tokens, scoped_tokens);
                        router.layer(layer)
                    }
                    #[cfg(not(feature = "oauth"))]
                    {
                        anyhow::bail!(
                            "Per-token tool scoping requires the 'oauth' feature. \
                             Rebuild with: cargo install mcp-proxy --features oauth"
                        );
                    }
                }
            }
            #[cfg(feature = "oauth")]
            AuthConfig::Jwt {
                issuer,
                audience,
                jwks_uri,
                ..
            } => {
                tracing::info!("Enabling JWT auth (JWKS)");
                let validator = tower_mcp::oauth::JwksValidator::builder(jwks_uri)
                    .expected_audience(audience)
                    .expected_issuer(issuer)
                    .http_client(crate::introspection::auth_http_client())
                    .build()
                    .await
                    .map_err(|_| anyhow::anyhow!("building JWKS validator failed"))?;

                let metadata = tower_mcp::oauth::ProtectedResourceMetadata::new(audience)
                    .authorization_server(issuer);

                let layer = tower_mcp::oauth::OAuthLayer::new(validator, metadata.clone());
                with_oauth_metadata(router.layer(layer), metadata)?
            }
            #[cfg(not(feature = "oauth"))]
            AuthConfig::Jwt { .. } => {
                anyhow::bail!(
                    "JWT auth requires the 'oauth' feature. Rebuild with: cargo install mcp-proxy --features oauth"
                );
            }
            #[cfg(feature = "oauth")]
            AuthConfig::OAuth {
                issuer,
                audience,
                token_validation,
                jwks_uri,
                introspection_endpoint,
                client_id,
                client_secret,
                ..
            } => {
                use crate::config::TokenValidationStrategy;

                tracing::info!(
                    strategy = ?token_validation,
                    "Enabling OAuth 2.1 auth"
                );

                let needs_jwks = *token_validation != TokenValidationStrategy::Introspection;
                let needs_introspection = *token_validation != TokenValidationStrategy::Jwt;
                let discovered = if (needs_jwks && jwks_uri.is_none())
                    || (needs_introspection && introspection_endpoint.is_none())
                {
                    Some(
                        crate::introspection::discover_auth_server(issuer)
                            .await
                            .context("discovering OAuth authorization server")?,
                    )
                } else {
                    None
                };
                let effective_jwks_uri = jwks_uri
                    .as_deref()
                    .or_else(|| discovered.as_ref().and_then(|d| d.jwks_uri.as_deref()));
                let effective_introspection = introspection_endpoint.as_deref().or_else(|| {
                    discovered
                        .as_ref()
                        .and_then(|d| d.introspection_endpoint.as_deref())
                });

                let metadata = tower_mcp::oauth::ProtectedResourceMetadata::new(audience)
                    .authorization_server(issuer);

                match token_validation {
                    TokenValidationStrategy::Jwt => {
                        let validator = tower_mcp::oauth::JwksValidator::builder(
                            effective_jwks_uri.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "JWKS URI not found via discovery and not configured"
                                )
                            })?,
                        )
                        .expected_audience(audience)
                        .expected_issuer(issuer)
                        .http_client(crate::introspection::auth_http_client())
                        .build()
                        .await
                        .map_err(|_| anyhow::anyhow!("building JWKS validator failed"))?;
                        let layer = tower_mcp::oauth::OAuthLayer::new(validator, metadata.clone());
                        with_oauth_metadata(router.layer(layer), metadata)?
                    }
                    TokenValidationStrategy::Introspection => {
                        let endpoint = effective_introspection.ok_or_else(|| {
                            anyhow::anyhow!(
                                "introspection endpoint not found via discovery and not configured"
                            )
                        })?;
                        let validator = crate::introspection::IntrospectionValidator::new(
                            endpoint,
                            client_id.as_deref().ok_or_else(|| {
                                anyhow::anyhow!("introspection requires client_id")
                            })?,
                            client_secret.as_deref().ok_or_else(|| {
                                anyhow::anyhow!("introspection requires client_secret")
                            })?,
                        )
                        .expected_audience(audience)
                        .expected_issuer(issuer);
                        let layer = tower_mcp::oauth::OAuthLayer::new(validator, metadata.clone());
                        with_oauth_metadata(router.layer(layer), metadata)?
                    }
                    TokenValidationStrategy::Both => {
                        let endpoint = effective_introspection.ok_or_else(|| {
                            anyhow::anyhow!(
                                "introspection endpoint not found via discovery and not configured"
                            )
                        })?;
                        let jwt_validator = tower_mcp::oauth::JwksValidator::builder(
                            effective_jwks_uri.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "JWKS URI not found via discovery and not configured"
                                )
                            })?,
                        )
                        .expected_audience(audience)
                        .expected_issuer(issuer)
                        .http_client(crate::introspection::auth_http_client())
                        .build()
                        .await
                        .map_err(|_| anyhow::anyhow!("building JWKS validator failed"))?;
                        let introspection_validator =
                            crate::introspection::IntrospectionValidator::new(
                                endpoint,
                                client_id.as_deref().ok_or_else(|| {
                                    anyhow::anyhow!("introspection requires client_id")
                                })?,
                                client_secret.as_deref().ok_or_else(|| {
                                    anyhow::anyhow!("introspection requires client_secret")
                                })?,
                            )
                            .expected_audience(audience)
                            .expected_issuer(issuer);
                        let fallback = crate::introspection::FallbackValidator::new(
                            jwt_validator,
                            introspection_validator,
                        );
                        let layer = tower_mcp::oauth::OAuthLayer::new(fallback, metadata.clone());
                        with_oauth_metadata(router.layer(layer), metadata)?
                    }
                }
            }
            #[cfg(not(feature = "oauth"))]
            AuthConfig::OAuth { .. } => {
                anyhow::bail!(
                    "OAuth auth requires the 'oauth' feature. Rebuild with: cargo install mcp-proxy --features oauth"
                );
            }
        }
    } else {
        router
    };
    Ok(router)
}

/// Wait for SIGTERM or SIGINT, then log and return.
pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }

    tracing::info!("Shutdown signal received, draining connections");
}

#[cfg(all(test, feature = "oauth"))]
mod scope_enforcement_tests {
    use std::collections::HashMap;

    use tower::{Layer, Service};
    use tower_mcp::oauth::token::TokenClaims;
    use tower_mcp::protocol::{CallToolParams, McpRequest, RequestId};
    use tower_mcp::router::Extensions;

    use super::oauth_scope_layer;
    use crate::test_util::MockService;

    /// Build a `tools/call` request, optionally carrying a token with `scope`.
    fn call_with_scope(scope: Option<&str>) -> tower_mcp::RouterRequest {
        let mut extensions = Extensions::new();
        if let Some(scope) = scope {
            extensions.insert(TokenClaims {
                sub: Some("user".into()),
                iss: None,
                aud: None,
                exp: None,
                scope: Some(scope.to_string()),
                client_id: None,
                extra: HashMap::new(),
            });
        }
        tower_mcp::RouterRequest {
            id: RequestId::Number(1),
            inner: McpRequest::CallTool(CallToolParams {
                name: "fs/read".into(),
                arguments: serde_json::json!({}),
                input_responses: None,
                request_state: None,
                meta: None,
                task: None,
            }),
            extensions,
        }
    }

    #[test]
    fn no_required_scopes_yields_no_layer() {
        assert!(oauth_scope_layer(&[]).is_none());
    }

    #[tokio::test]
    async fn token_missing_required_scope_is_rejected() {
        let required = vec!["mcp:access".to_string()];
        let layer = oauth_scope_layer(&required).expect("layer for non-empty scopes");
        let mut svc = layer.layer(MockService::with_tools(&["fs/read"]));

        // Token carries a different scope -> missing the required one.
        let resp = svc
            .call(call_with_scope(Some("other:scope")))
            .await
            .unwrap();
        let err = resp.inner.unwrap_err();
        assert!(
            err.message.to_lowercase().contains("scope"),
            "expected insufficient-scope error, got: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn token_with_all_required_scopes_is_allowed() {
        let required = vec!["mcp:access".to_string(), "mcp:read".to_string()];
        let layer = oauth_scope_layer(&required).expect("layer for non-empty scopes");
        let mut svc = layer.layer(MockService::with_tools(&["fs/read"]));

        let resp = svc
            .call(call_with_scope(Some("mcp:access mcp:read mcp:extra")))
            .await
            .unwrap();
        assert!(
            resp.inner.is_ok(),
            "token carrying all required scopes should be allowed"
        );
    }
}

#[cfg(test)]
mod protocol_support_tests {
    use tower::util::BoxCloneService;
    use tower_mcp::{McpRequest, McpResponse, RouterRequest, RouterResponse};

    use super::{apply_2026_layers, build_protocol_support};
    use crate::config::ProxyConfig;
    use crate::test_util::{MockService, call_service};
    use std::convert::Infallible;

    /// `build_protocol_support` falls back to both versions when the config list is empty.
    #[test]
    fn build_protocol_support_uses_default_when_empty() {
        let config = ProxyConfig::parse(
            "[proxy]\nname = \"t\"\nversion = \"1.0.0\"\n[proxy.listen]\nhost = \"127.0.0.1\"\nport = 8080\n[[backends]]\nname = \"b\"\ntransport = \"stdio\"\ncommand = \"echo\"\n",
        )
        .unwrap();
        let support = build_protocol_support(&config).unwrap();
        assert!(support.contains("2026-07-28"));
        assert!(support.contains("2025-11-25"));
        assert_eq!(support.versions().len(), 2);
    }

    /// `build_protocol_support` honors an explicit configured version set.
    #[test]
    fn build_protocol_support_uses_configured_versions() {
        let config = ProxyConfig::parse(
            "[proxy]\nname = \"t\"\nversion = \"1.0.0\"\n[proxy.listen]\nhost = \"127.0.0.1\"\nport = 8080\n[[backends]]\nname = \"b\"\ntransport = \"stdio\"\ncommand = \"echo\"\n[proxy.protocol_support]\nversions = [\"2026-07-28\"]\n",
        )
        .unwrap();
        let support = build_protocol_support(&config).unwrap();
        assert!(support.contains("2026-07-28"));
        assert!(!support.contains("2025-11-25"));
        assert_eq!(support.versions().len(), 1);
    }

    /// `apply_2026_layers` intercepts `server/discover` via the Discover layer
    /// while still passing normal traffic (e.g. `ListTools`) through to the
    /// inner service. This guards against the trio swallowing or mis-ordering
    /// requests.
    #[tokio::test]
    async fn apply_2026_layers_intercepts_discover_and_passes_through_tools() {
        let service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
            BoxCloneService::new(MockService::with_tools(&["fs/read"]));
        let config = ProxyConfig::parse(
            "[proxy]\nname = \"t\"\nversion = \"1.0.0\"\n[proxy.listen]\nhost = \"127.0.0.1\"\nport = 8080\n[[backends]]\nname = \"b\"\ntransport = \"stdio\"\ncommand = \"echo\"\n",
        )
        .unwrap();
        let mut wrapped = apply_2026_layers(service, &config);

        // Discover must be intercepted by the Discover layer, not fall through
        // to the inner service (which would produce a -32601 / Pong).
        let discover_resp = call_service(
            &mut wrapped,
            McpRequest::Discover(tower_mcp::protocol::DiscoverParams { meta: None }),
        )
        .await;
        assert!(
            matches!(discover_resp.inner, Ok(McpResponse::Discover(_))),
            "server/discover should be intercepted by the Discover layer, got: {:?}",
            discover_resp.inner
        );

        // ListTools must reach the inner MockService unchanged.
        let list_resp = call_service(&mut wrapped, McpRequest::ListTools(Default::default())).await;
        assert!(
            matches!(list_resp.inner, Ok(McpResponse::ListTools(_))),
            "ListTools should pass through to the inner service, got: {:?}",
            list_resp.inner
        );
    }
}

/// Invalidate the warm catalog files of terminated backends on restart/shutdown.
///
/// Deletes every `{name}-*.json` file (any hash) for each name in
/// `terminated_names` so the next startup re-probes fresh instead of serving
/// the pre-restart catalog. Names not present on disk are a no-op (returns 0
/// for them). Returns the total files removed; all IO errors are swallowed
/// (the re-probe on next startup is the recovery). Logs per-backend results.
pub(crate) fn invalidate_terminated_backends(
    store: &crate::warm_cache::WarmCatalogStore,
    terminated_names: &[String],
) -> usize {
    let mut total = 0;
    for name in terminated_names {
        let removed = store.invalidate_backend(name);
        if removed > 0 {
            tracing::info!(
                backend = %name,
                files_removed = removed,
                "restart invalidation: deleted stale warm catalog; backend will re-probe on next startup"
            );
        }
        total += removed;
    }
    total
}

/// Unit test: `build_lazy_registry` loads a warm catalog from disk at startup
/// for a lazy backend that is registered Down (served from cache, not spawned).
///
/// The lazy backend points at a real python server (so `Proxy::from_config`
/// would spawn it on demand), but at startup it must remain Down and expose the
/// seeded warm catalog. The dummy eager backend MUST point at a real spawnable
/// server so `Proxy::from_config` can build the shared proxy.
#[cfg(test)]
mod lazy_warm_catalog_startup_tests {
    use super::*;
    use crate::config::{BackendConfig, ProxySettings, SpawnMode, TransportType, WarmCacheConfig};
    use crate::lazy_registry::SpawnState;
    use crate::warm_cache::{BinaryHasher, WarmCatalog, WarmCatalogStore};
    use std::io::Write;
    use tower_mcp_types::protocol::ToolDefinition;

    /// Minimal newline-delimited MCP stdio server (Python stdlib only). Exposes
    /// one `ping` tool. tower-mcp's `StdioClientTransport` speaks
    /// newline-delimited JSON (one object per line + `\n`); a Content-Length
    /// framed server DEADLOCKS.
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

    #[tokio::test]
    async fn build_lazy_registry_loads_warm_catalog_from_disk() {
        let dir = std::env::temp_dir().join(format!(
            "mcp-proxy-unit-lazy-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let server = dir.join("mcp-min-srv-unit.py");
        {
            let mut f = std::fs::File::create(&server).expect("create server script");
            f.write_all(MIN_MCP_SERVER.as_bytes())
                .expect("write server script");
        }

        let cfg = ProxyConfig {
            proxy: ProxySettings {
                name: "lazy-unit-proxy".to_string(),
                version: "1.0.0".to_string(),
                separator: "/".to_string(),
                listen: crate::config::ListenConfig {
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
                tool_exposure: crate::config::ToolExposure::default(),
                expose_grouped_in_default: false,
                endpoint_groups: vec![],
                tool_groups: vec![],
                watchers: vec![],
                backend_env: std::collections::HashMap::new(),
                timeout: None,
                circuit_breaker: None,
                retry: None,
                endpoint_group_list: vec![],
                protocol_support: crate::config::ProtocolSupportConfig::default(),
                default_spawn_mode: crate::config::SpawnMode::Eager,
                default_idle_timeout_secs: None,
                default_idle_teardown: Default::default(),
                init_timeout: None,
            },
            backends: vec![
                // Dummy eager backend (real MCP server) so the shared McpProxy
                // has at least one backend to build. `enabled: true` is REQUIRED.
                BackendConfig {
                    name: "__dummy__".to_string(),
                    enabled: true,
                    transport: TransportType::Stdio,
                    command: Some("python3".to_string()),
                    args: vec![server.to_string_lossy().to_string()],
                    ..Default::default()
                },
                // Lazy stdio backend under test (Down, served from cache).
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
            performance: crate::config::PerformanceConfig::default(),
            security: crate::config::SecurityConfig::default(),
            cache: crate::config::CacheBackendConfig::default(),
            composite_tools: vec![],
            warm_cache: WarmCacheConfig {
                enabled: true,
                dir: Some(dir.clone()),
                ttl_secs: 0,
                invalidate_on_hash_change: true,
            },
            source_path: None,
            observability: crate::config::ObservabilityConfig::default(),
        };

        // Seed a warm catalog on disk BEFORE building the proxy.
        let files_cfg = cfg
            .backends
            .iter()
            .find(|b| b.name == "files")
            .expect("files backend present")
            .clone();
        let hash = BinaryHasher::hash(&files_cfg);
        let catalog = WarmCatalog::from_probe_result(
            "files",
            "/",
            vec![ping_tool()],
            vec![],
            vec![],
            vec![],
            Some("2026-07-28".to_string()),
            hash,
        );
        WarmCatalogStore::new(dir.clone())
            .save(&catalog)
            .expect("seed warm catalog on disk");

        let proxy = Proxy::from_config(cfg).await.expect("proxy builds");
        let reg = proxy.lazy_registry();

        // The lazy backend must be Down (not spawned) but its cached tool is
        // loaded from disk.
        assert_eq!(
            reg.spawn_state("files"),
            SpawnState::Down,
            "lazy backend must be Down at startup (served from cache)"
        );
        let loaded = reg
            .get("files")
            .and_then(|b| b.catalog)
            .expect("warm catalog must be loaded from disk at startup");
        assert_eq!(
            loaded.tools.len(),
            1,
            "loaded catalog must contain one tool"
        );
        assert_eq!(
            loaded.tools[0].name, "files/ping",
            "loaded tool must be namespaced as files/ping"
        );

        let _ = std::fs::remove_file(&server);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
/// Unit tests for catalog invalidation on restart/conflict.
///
/// A backend whose spawn is a proven fast-fail conflict (or that was
/// terminated by graceful shutdown) must not keep serving a stale warm
/// catalog: its files are deleted, its in-memory entry is cleared, and
/// `tools/list` omits it until a fresh probe succeeds. Timeout/transient
/// errors keep the degrade path (fuzzy fallback + retained catalog).
#[cfg(test)]
mod catalog_invalidation_tests {
    use crate::warm_cache::probe::ProbeError;
    use crate::warm_cache::{WarmCatalog, WarmCatalogStore};

    fn seeded_store(dir: &std::path::Path) -> WarmCatalogStore {
        let store = WarmCatalogStore::new(dir.to_path_buf());
        for (name, hash) in [("files", "aaa"), ("db", "bbb")] {
            let cat = WarmCatalog::from_probe_result(
                name,
                "/",
                vec![],
                vec![],
                vec![],
                vec![],
                None,
                hash.to_string(),
            );
            // Write directly so the file lands even with no tools.
            let path = store.path_for(name, hash);
            std::fs::write(
                &path,
                serde_json::to_string_pretty(&cat).expect("serialize"),
            )
            .expect("seed file");
        }
        store
    }

    #[test]
    fn conflict_spawn_error_classifies_as_conflict() {
        let err = ProbeError::Spawn(
            "codebase".to_string(),
            anyhow::anyhow!("Transport error: Connection closed"),
        );
        assert!(
            crate::warm_cache::probe::is_conflict_probe_error(&err),
            "fast Connection-closed spawn failure must classify as conflict"
        );
    }

    #[test]
    fn probe_timeout_is_transient_not_conflict() {
        let err = ProbeError::Timeout("slow".to_string(), 5);
        assert!(
            !crate::warm_cache::probe::is_conflict_probe_error(&err),
            "timeouts must keep the degrade path"
        );
    }

    #[test]
    fn spawn_error_with_timeout_text_is_transient() {
        let err = ProbeError::Spawn(
            "x".to_string(),
            anyhow::anyhow!("failed to initialize: request timed out"),
        );
        assert!(
            !crate::warm_cache::probe::is_conflict_probe_error(&err),
            "timeout-flavored spawn errors must keep the degrade path"
        );
    }

    #[test]
    fn invalidate_terminated_backends_deletes_only_terminated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = seeded_store(dir.path());

        let removed = super::invalidate_terminated_backends(&store, &["files".to_string()]);
        assert_eq!(removed, 1, "exactly the terminated backend file is removed");
        assert!(
            !store.path_for("files", "aaa").exists(),
            "terminated backend file must be gone"
        );
        assert!(
            store.path_for("db", "bbb").exists(),
            "surviving backend file must remain"
        );
    }

    #[test]
    fn invalidate_terminated_backends_empty_set_is_noop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = seeded_store(dir.path());

        assert_eq!(super::invalidate_terminated_backends(&store, &[]), 0);
        assert!(store.path_for("files", "aaa").exists());
        assert!(store.path_for("db", "bbb").exists());
    }
}

#[cfg(test)]
mod middleware_stack_tests {
    //! Regression tests for #218: the per-backend middleware chain must
    //! compose as one stack. Written against tower-mcp's pre-0.22 last-wins
    //! `backend_layer` semantics (joshrotenberg/tower-mcp#1173, fixed
    //! upstream in 0.22.0): naively calling `backend_layer()` once per
    //! middleware would have silently kept only the last one, and the first
    //! test here would fail at "call 1 should time out". These tests pin
    //! `BackendMiddlewareLayer`'s single-call composition independent of
    //! upstream's stacking behavior.

    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use tower::Service;
    use tower_mcp::CallToolResult;
    use tower_mcp::protocol::{CallToolParams, McpRequest, McpResponse, RequestId};
    use tower_mcp::router::Extensions;
    use tower_mcp_types::JsonRpcError;

    use super::*;

    /// Infallible backend with a hit counter, switchable slowness, and a
    /// countdown of response-plane failures to emit before succeeding.
    #[derive(Clone)]
    struct FlakyBackend {
        hits: Arc<AtomicUsize>,
        slow: Arc<AtomicBool>,
        fail_responses: Arc<AtomicUsize>,
    }

    impl FlakyBackend {
        fn new() -> (Self, Arc<AtomicUsize>, Arc<AtomicBool>, Arc<AtomicUsize>) {
            let hits = Arc::new(AtomicUsize::new(0));
            let slow = Arc::new(AtomicBool::new(false));
            let fail_responses = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    hits: hits.clone(),
                    slow: slow.clone(),
                    fail_responses: fail_responses.clone(),
                },
                hits,
                slow,
                fail_responses,
            )
        }
    }

    impl tower::Service<RouterRequest> for FlakyBackend {
        type Response = RouterResponse;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<RouterResponse, Infallible>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: RouterRequest) -> Self::Future {
            let hits = self.hits.clone();
            let slow = self.slow.clone();
            let fail_responses = self.fail_responses.clone();
            Box::pin(async move {
                hits.fetch_add(1, Ordering::SeqCst);
                if slow.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                let should_fail = {
                    let mut remaining = fail_responses.load(Ordering::SeqCst);
                    loop {
                        if remaining == 0 {
                            break false;
                        }
                        match fail_responses.compare_exchange_weak(
                            remaining,
                            remaining - 1,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => break true,
                            Err(current) => remaining = current,
                        }
                    }
                };
                let inner = if should_fail {
                    Err(JsonRpcError::internal_error("transient backend failure"))
                } else {
                    Ok(McpResponse::CallTool(CallToolResult::text("pong")))
                };
                Ok(RouterResponse { id: req.id, inner })
            })
        }
    }

    fn req() -> RouterRequest {
        RouterRequest {
            id: RequestId::Number(1),
            inner: McpRequest::CallTool(CallToolParams {
                name: "ping".to_string(),
                arguments: serde_json::json!({}),
                input_responses: None,
                request_state: None,
                meta: None,
                task: None,
            }),
            extensions: Extensions::new(),
        }
    }

    async fn drive(svc: &mut InfallibleService) -> RouterResponse {
        svc.ready()
            .await
            .expect("infallible")
            .call(req())
            .await
            .expect("infallible")
    }

    fn test_breaker(
        minimum_calls: usize,
        wait_in_open: Duration,
        permitted_in_half_open: usize,
    ) -> tower_resilience::circuitbreaker::CircuitBreakerLayer {
        let (layer, _handle) = tower_resilience::circuitbreaker::CircuitBreakerLayer::builder()
            .failure_rate_threshold(0.5)
            .minimum_number_of_calls(minimum_calls)
            // The count-based window must be full before the breaker
            // evaluates; align it with minimum_calls (see #220 for the
            // production-side mapping fix).
            .sliding_window_size(minimum_calls)
            .wait_duration_in_open(wait_in_open)
            .permitted_calls_in_half_open(permitted_in_half_open)
            .name("test-cb")
            .build_with_handle()
            .unwrap();
        layer
    }

    /// Timeout and circuit breaker both apply; timeouts count as failures,
    /// the breaker opens, rejects without reaching the backend, and closes
    /// again through half-open probes once the fault clears.
    #[tokio::test]
    async fn timeout_and_breaker_compose_and_breaker_trips() {
        let (backend, hits, slow, _fail) = FlakyBackend::new();
        slow.store(true, Ordering::SeqCst);

        let mut mw = BackendMiddlewareLayer::default();
        mw.stage(TimeoutLayer::new(Duration::from_millis(10)));
        mw.stage(test_breaker(4, Duration::from_millis(200), 2));

        let mut svc = mw.layer(backend);

        for i in 1..=4 {
            let resp = drive(&mut svc).await;
            assert!(resp.inner.is_err(), "call {i} should time out");
        }
        assert_eq!(
            hits.load(Ordering::SeqCst),
            4,
            "all four calls reach the backend"
        );

        let resp = drive(&mut svc).await;
        assert!(resp.inner.is_err(), "open breaker rejects");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            4,
            "rejected call must not reach the backend"
        );

        tokio::time::sleep(Duration::from_millis(300)).await;
        slow.store(false, Ordering::SeqCst);
        for i in 1..=3 {
            let resp = drive(&mut svc).await;
            assert!(resp.inner.is_ok(), "recovered call {i} succeeds");
        }
        assert_eq!(hits.load(Ordering::SeqCst), 7);
    }

    /// Retry (Infallible zone) and timeout (BoxError zone) coexist: the
    /// retry policy absorbs transient response-plane failures while the
    /// timeout stays armed above it.
    #[tokio::test]
    async fn retry_and_timeout_compose() {
        let (backend, hits, _slow, fail) = FlakyBackend::new();
        fail.store(2, Ordering::SeqCst);

        let cfg = crate::config::RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            budget_percent: None,
            min_retries_per_sec: 10,
        };
        let mut mw = BackendMiddlewareLayer {
            retry: Some(crate::retry::build_retry_layer(&cfg, "test")),
            ..Default::default()
        };
        mw.stage(TimeoutLayer::new(Duration::from_millis(500)));

        let mut svc = mw.layer(backend);

        let resp = drive(&mut svc).await;
        assert!(resp.inner.is_ok(), "retries absorb the transient failures");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            3,
            "two failures plus the successful attempt"
        );
    }

    /// The empty layer is the identity.
    #[tokio::test]
    async fn empty_middleware_layer_is_identity() {
        let (backend, hits, _slow, _fail) = FlakyBackend::new();
        let mw = BackendMiddlewareLayer::default();
        assert!(mw.is_empty());

        let mut svc = mw.layer(backend);
        let resp = drive(&mut svc).await;
        assert!(resp.inner.is_ok());
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    /// Regression for #220: a breaker built through the production config
    /// mapping opens after exactly `minimum_calls` failing calls, not after
    /// the tower-resilience default 100-call window fills.
    #[tokio::test]
    async fn config_mapped_breaker_opens_at_minimum_calls() {
        let (backend, hits, slow, _fail) = FlakyBackend::new();
        slow.store(true, Ordering::SeqCst);

        let cfg = crate::config::CircuitBreakerConfig {
            failure_rate_threshold: 0.5,
            minimum_calls: 4,
            wait_duration_seconds: 60,
            permitted_calls_in_half_open: 1,
        };
        let (breaker, _handle) = build_breaker_layer(&cfg, "test").unwrap();

        let mut mw = BackendMiddlewareLayer::default();
        mw.stage(TimeoutLayer::new(Duration::from_millis(10)));
        mw.stage(breaker);

        let mut svc = mw.layer(backend);

        for i in 1..=4 {
            let resp = drive(&mut svc).await;
            assert!(resp.inner.is_err(), "call {i} should time out");
        }
        assert_eq!(hits.load(Ordering::SeqCst), 4);

        let resp = drive(&mut svc).await;
        assert!(resp.inner.is_err(), "open breaker rejects");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            4,
            "rejected call must not reach the backend"
        );
    }
    #[tokio::test]
    async fn administrative_and_data_requests_share_the_global_rate_limit() {
        let counter = Arc::new(AtomicUsize::new(0));
        let hits = counter.clone();
        let data = tower_mcp::McpRouter::new().tool(
            tower_mcp::ToolBuilder::new("ping")
                .handler(move |_: serde_json::Value| {
                    hits.fetch_add(1, Ordering::SeqCst);
                    async { Ok(CallToolResult::text("pong")) }
                })
                .build(),
        );
        let management = tower_mcp::McpRouter::new().tool(
            tower_mcp::ToolBuilder::new("config")
                .handler(|_: serde_json::Value| async {
                    Ok(CallToolResult::text("private diagnostic"))
                })
                .build(),
        );
        let proxy = McpProxy::builder("rate-test", "1")
            .separator("/")
            .backend("api", tower_mcp::client::ChannelTransport::new(data))
            .await
            .backend(
                "proxy",
                tower_mcp::client::ChannelTransport::new(management),
            )
            .await
            .build_strict()
            .await
            .unwrap();
        let config = ProxyConfig::parse("[proxy]\nname='rate-test'\n[proxy.listen]\n[proxy.rate_limit]\nrequests=1\nperiod_seconds=60\n[[backends]]\nname='api'\ntransport='stdio'\ncommand='fixture'\n").unwrap();
        let (service, _) = build_middleware_stack(&config, proxy).unwrap();
        let mut extensions = Extensions::new();
        extensions.insert(crate::management::Identity::Management);
        let response = service
            .clone()
            .oneshot(RouterRequest {
                id: RequestId::Number(1),
                inner: McpRequest::CallTool(
                    serde_json::from_value(
                        serde_json::json!({"name":"proxy/config","arguments":{}}),
                    )
                    .unwrap(),
                ),
                extensions,
            })
            .await
            .unwrap();
        assert!(response.inner.is_ok());
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            service.oneshot(RouterRequest {
                id: RequestId::Number(2),
                inner: McpRequest::CallTool(
                    serde_json::from_value(serde_json::json!({"name":"api/ping","arguments":{}}))
                        .unwrap(),
                ),
                extensions: Extensions::new(),
            }),
        )
        .await
        .expect("rate limit rejection must be bounded")
        .unwrap();
        assert!(response.inner.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod http_transport_tests {
    //! Regression tests for #245: an HTTP backend's configured timeout must
    //! reach the transport, whose own `request_timeout` otherwise caps every
    //! request at 30 seconds.

    use super::*;

    fn backend(toml_src: &str) -> BackendConfig {
        // Parse from TOML so every unrelated field gets its default.
        toml::from_str(toml_src).unwrap()
    }

    #[test]
    fn configured_timeout_reaches_the_transport() {
        let backend = backend(
            r#"
            name = "api"
            transport = "http"
            url = "http://localhost:8080"

            [timeout]
            seconds = 900
            "#,
        );

        let config = http_client_config(&backend);
        assert_eq!(config.request_timeout, Duration::from_secs(900));
    }

    #[test]
    fn no_configured_timeout_keeps_the_transport_default() {
        let backend = backend(
            r#"
            name = "api"
            transport = "http"
            url = "http://localhost:8080"
            "#,
        );

        let config = http_client_config(&backend);
        assert_eq!(config.request_timeout, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn transport_builds_with_timeout_and_bearer_token() {
        let backend = backend(
            r#"
            name = "api"
            transport = "http"
            url = "http://localhost:8080"
            bearer_token = "sk-test"

            [timeout]
            seconds = 120
            "#,
        );

        // Construction only; no request is made.
        let _transport = build_http_transport(backend.url.as_deref().unwrap(), &backend)
            .expect("transport builds");
    }
}
