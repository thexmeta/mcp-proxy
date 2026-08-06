//! Endpoint group routing - creates separate MCP endpoints for grouped backends.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use tokio::process::Command;
use tower::timeout::TimeoutLayer;
use tower::util::BoxCloneService;
use tower_mcp::SessionHandle;
use tower_mcp::client::StdioClientTransport;
use tower_mcp::proxy::McpProxy;
use tower_mcp::{RouterRequest, RouterResponse};

use crate::config::{EndpointGroupConfig, ProxyConfig, TransportType};

/// Shared registry for endpoint groups that supports hot reload.
/// This allows dynamic addition/removal/update of endpoint groups without restarting the proxy.
#[derive(Clone)]
pub struct EndpointGroupRegistry {
    inner: Arc<RwLock<HashMap<String, EndpointGroupRouter>>>,
}

impl EndpointGroupRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert or update an endpoint group router.
    pub fn insert(&self, router: EndpointGroupRouter) {
        let mut map = self.inner.write().unwrap();
        map.insert(router.name.clone(), router);
    }

    /// Remove an endpoint group by name.
    pub fn remove(&self, name: &str) -> Option<EndpointGroupRouter> {
        let mut map = self.inner.write().unwrap();
        map.remove(name)
    }

    /// Get an endpoint group by name.
    pub fn get(&self, name: &str) -> Option<EndpointGroupRouter> {
        let map = self.inner.read().unwrap();
        map.get(name).cloned()
    }

    /// Get all endpoint groups.
    pub fn all(&self) -> Vec<EndpointGroupRouter> {
        let map = self.inner.read().unwrap();
        map.values().cloned().collect()
    }

    /// Get the inner map for iteration.
    pub fn inner(&self) -> Arc<RwLock<HashMap<String, EndpointGroupRouter>>> {
        self.inner.clone()
    }
}

impl Default for EndpointGroupRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Holds the router and session handle for an endpoint group.
#[derive(Clone)]
pub struct EndpointGroupRouter {
    pub name: String,
    pub path: String,
    pub router: Router,
    pub session_handle: SessionHandle,
    pub inner: McpProxy,
}

/// Build an McpProxy for a specific set of backend configurations.
/// This is a variant of `build_mcp_proxy` that takes a pre-filtered list of backends.
async fn build_mcp_proxy_for_backends(
    proxy_name: &str,
    proxy_version: &str,
    separator: &str,
    proxy_instructions: Option<&String>,
    backends: &[&crate::config::BackendConfig],
) -> Result<(McpProxy, HashMap<String, crate::proxy::CbHandle>)> {
    let mut builder = McpProxy::builder(proxy_name, proxy_version).separator(separator);
    let cb_handles: HashMap<String, crate::proxy::CbHandle> = HashMap::new();

    if let Some(instructions) = proxy_instructions {
        builder = builder.instructions(instructions);
    }

    // Create shared outlier detector if any backend has outlier_detection configured.
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

        tracing::info!(name = %backend.name, transport = ?backend.transport, "Adding backend to endpoint group");

        match backend.transport {
            TransportType::Stdio => {
                let command = backend.command.as_deref().unwrap();
                let args: Vec<&str> = backend.args.iter().map(|s| s.as_str()).collect();

                let mut cmd = Command::new(command);
                cmd.args(&args);

                for (key, value) in &backend.env {
                    cmd.env(key, value);
                }

                if let Some(ref working_dir) = backend.working_dir {
                    cmd.current_dir(working_dir);
                }

                let transport = StdioClientTransport::spawn_command(&mut cmd)
                    .await
                    .with_context(|| format!("spawning backend '{}'", backend.name))?;

                builder = builder.backend(&backend.name, transport).await;
            }
            TransportType::Http => {
                let url = backend.url.as_deref().unwrap();
                let mut transport = tower_mcp::client::HttpClientTransport::new(url);
                if let Some(token) = &backend.bearer_token {
                    transport = transport.bearer_token(token);
                }

                builder = builder.backend(&backend.name, transport).await;
            }
            #[cfg(feature = "websocket")]
            TransportType::Websocket => {
                let url = backend.url.as_deref().unwrap();
                tracing::info!(url = %url, "Connecting to WebSocket backend");
                let transport = if let Some(token) = &backend.bearer_token {
                    crate::ws_transport::WebSocketClientTransport::connect_with_bearer_token(
                        url, token,
                    )
                    .await
                    .with_context(|| {
                        format!("connecting to WebSocket backend '{}'", backend.name)
                    })?
                } else {
                    crate::ws_transport::WebSocketClientTransport::connect(url)
                        .await
                        .with_context(|| {
                            format!("connecting to WebSocket backend '{}'", backend.name)
                        })?
                };

                builder = builder.backend(&backend.name, transport).await;
            }
            #[cfg(not(feature = "websocket"))]
            TransportType::Websocket => {
                anyhow::bail!(
                    "WebSocket transport requires the 'websocket' feature. \
                     Rebuild with: cargo install mcp-proxy --features websocket"
                );
            }
        }

        // Per-backend middleware stack (applied in order: inner -> outer)
        builder = apply_backend_middleware(builder, backend, &outlier_detector);
    }

    let result = builder.build().await?;

    if !result.skipped.is_empty() {
        for s in &result.skipped {
            tracing::warn!("Skipped backend: {s}");
        }
    }

    Ok((result.proxy, cb_handles))
}

/// Apply per-backend middleware layers to the builder.
fn apply_backend_middleware(
    mut builder: tower_mcp::proxy::McpProxyBuilder,
    backend: &crate::config::BackendConfig,
    outlier_detector: &Option<crate::outlier::OutlierDetector>,
) -> tower_mcp::proxy::McpProxyBuilder {
    // Retry (innermost -- retries happen before other middleware)
    if let Some(retry_cfg) = &backend.retry {
        tracing::info!(
            backend = %backend.name,
            max_retries = retry_cfg.max_retries,
            initial_backoff_ms = retry_cfg.initial_backoff_ms,
            max_backoff_ms = retry_cfg.max_backoff_ms,
            "Applying retry policy"
        );
        let layer = crate::retry::build_retry_layer(retry_cfg, &backend.name);
        builder = builder.backend_layer(layer);
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
        builder = builder.backend_layer(layer);
    }

    // Concurrency limit
    if let Some(cc) = &backend.concurrency {
        tracing::info!(
            backend = %backend.name,
            max = cc.max_concurrent,
            "Applying concurrency limit"
        );
        builder =
            builder.backend_layer(tower::limit::ConcurrencyLimitLayer::new(cc.max_concurrent));
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
            .build();
        builder = builder.backend_layer(layer);
    }

    // Timeout
    if let Some(timeout) = &backend.timeout {
        tracing::info!(
            backend = %backend.name,
            seconds = timeout.seconds,
            "Applying timeout"
        );
        builder = builder.backend_layer(TimeoutLayer::new(Duration::from_secs(timeout.seconds)));
    }

    // Circuit breaker
    if let Some(cb) = &backend.circuit_breaker {
        tracing::info!(
            backend = %backend.name,
            failure_rate = cb.failure_rate_threshold,
            wait_seconds = cb.wait_duration_seconds,
            "Applying circuit breaker"
        );
        let (layer, _handle) = tower_resilience::circuitbreaker::CircuitBreakerLayer::builder()
            .failure_rate_threshold(cb.failure_rate_threshold)
            .minimum_number_of_calls(cb.minimum_calls)
            .wait_duration_in_open(Duration::from_secs(cb.wait_duration_seconds))
            .permitted_calls_in_half_open(cb.permitted_calls_in_half_open)
            .name(format!("{}-cb", backend.name))
            .build_with_handle();
        // Note: cb_handles not tracked here for endpoint groups - could be added if needed
        builder = builder.backend_layer(layer);
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
        builder = builder.backend_layer(layer);
    }

    builder
}

/// Build the middleware stack around an McpProxy for an endpoint group.
/// This mirrors `build_middleware_stack` but for a specific group's config.
fn build_endpoint_group_middleware_stack(
    config: &ProxyConfig,
    group: &EndpointGroupConfig,
    proxy: McpProxy,
    group_backend_names: &HashSet<String>,
) -> Result<BoxCloneService<RouterRequest, RouterResponse, Infallible>> {
    let mut service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(proxy);

    // Filter backends to only those in this group for middleware that needs backend-specific config
    let group_backends: Vec<_> = config
        .backends
        .iter()
        .filter(|b| group_backend_names.contains(&b.name))
        .collect();

    // Argument injection
    let injection_rules: Vec<_> = group_backends
        .iter()
        .filter(|b| !b.default_args.is_empty() || !b.inject_args.is_empty())
        .map(|b| {
            let namespace = format!("{}{}", b.name, config.proxy.separator);
            tracing::info!(
                backend = %b.name,
                default_args = b.default_args.len(),
                tool_rules = b.inject_args.len(),
                "Applying argument injection (endpoint group: {})",
                group.name
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

    // Parameter overrides
    let param_overrides: Vec<_> = group_backends
        .iter()
        .filter(|b| !b.param_overrides.is_empty())
        .flat_map(|b| {
            let namespace = format!("{}{}", b.name, config.proxy.separator);
            tracing::info!(
                backend = %b.name,
                overrides = b.param_overrides.len(),
                "Applying parameter overrides (endpoint group: {})",
                group.name
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

    // Canary routing (only if both primary and canary are in this group)
    let canary_mappings: std::collections::HashMap<String, (String, u32, u32)> = group_backends
        .iter()
        .filter_map(|b| {
            b.canary_of.as_ref().map(|primary_name| {
                // Check if primary is also in this group
                if group_backend_names.contains(primary_name) {
                    let primary_weight = group_backends
                        .iter()
                        .find(|p| p.name == *primary_name)
                        .map(|p| p.weight)
                        .unwrap_or(100);
                    Some((
                        primary_name.clone(),
                        (b.name.clone(), primary_weight, b.weight),
                    ))
                } else {
                    None
                }
            })
        })
        .flatten()
        .collect();

    if !canary_mappings.is_empty() {
        for (primary, (canary, pw, cw)) in &canary_mappings {
            tracing::info!(
                primary = %primary,
                canary = %canary,
                primary_weight = pw,
                canary_weight = cw,
                "Enabling canary routing (endpoint group: {})",
                group.name
            );
        }
        service = BoxCloneService::new(crate::canary::CanaryService::new(
            service,
            canary_mappings,
            &config.proxy.separator,
        ));
    }

    // Failover routing (only if both primary and failover are in this group)
    let mut failover_groups: std::collections::HashMap<String, Vec<(u32, String)>> =
        std::collections::HashMap::new();
    for b in &group_backends {
        if let Some(ref primary) = b.failover_for
            && group_backend_names.contains(primary)
        {
            failover_groups
                .entry(primary.clone())
                .or_default()
                .push((b.priority, b.name.clone()));
        }
    }
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
                "Enabling failover routing (endpoint group: {})",
                group.name
            );
        }
        service = BoxCloneService::new(crate::failover::FailoverService::new(
            service,
            failover_mappings,
            &config.proxy.separator,
        ));
    }

    // Traffic mirroring (only if both source and mirror are in this group)
    let mirror_mappings: std::collections::HashMap<String, (String, u32)> = group_backends
        .iter()
        .filter_map(|b| {
            b.mirror_of.as_ref().and_then(|source| {
                if group_backend_names.contains(source) {
                    Some((source.clone(), (b.name.clone(), b.mirror_percent)))
                } else {
                    None
                }
            })
        })
        .collect();

    if !mirror_mappings.is_empty() {
        for (source, (mirror, pct)) in &mirror_mappings {
            tracing::info!(
                source = %source,
                mirror = %mirror,
                percent = pct,
                "Enabling traffic mirroring (endpoint group: {})",
                group.name
            );
        }
        service = BoxCloneService::new(crate::mirror::MirrorService::new(
            service,
            mirror_mappings,
            &config.proxy.separator,
        ));
    }

    // Response caching
    let cache_configs: Vec<_> = group_backends
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
                "Applying response cache (endpoint group: {})",
                group.name
            );
        }
        let (cache_svc, _handle) =
            crate::cache::CacheService::new(service, cache_configs, &config.cache);
        service = BoxCloneService::new(cache_svc);
    }

    // Request coalescing
    if config.performance.coalesce_requests {
        tracing::info!(
            "Request coalescing enabled (endpoint group: {})",
            group.name
        );
        service = BoxCloneService::new(crate::coalesce::CoalesceService::new(service));
    }

    // Request validation
    if config.security.max_argument_size.is_some() {
        let validation = crate::validation::ValidationConfig {
            max_argument_size: config.security.max_argument_size,
        };
        if let Some(max) = validation.max_argument_size {
            tracing::info!(
                max_argument_size = max,
                "Applying request validation (endpoint group: {})",
                group.name
            );
        }
        service = BoxCloneService::new(crate::validation::ValidationService::new(
            service, validation,
        ));
    }

    // Static capability filtering
    let filters: Vec<_> = group_backends
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
                "Applying capability filter (endpoint group: {})",
                group.name
            );
        }
        service = BoxCloneService::new(crate::filter::CapabilityFilterService::new(
            service, filters,
        ));
    }

    // Tool aliasing
    let alias_mappings: Vec<_> = group_backends
        .iter()
        .flat_map(|b| {
            let ns = format!("{}{}", b.name, config.proxy.separator);
            b.aliases
                .iter()
                .map(move |a| (ns.clone(), a.from.clone(), a.to.clone()))
        })
        .collect();

    let rename_all_mappings: Vec<_> = group_backends
        .iter()
        .flat_map(|b| {
            let ns = format!("{}{}", b.name, config.proxy.separator);
            b.rename_all
                .iter()
                .map(move |r| (ns.clone(), r.from.clone(), r.to.clone()))
        })
        .collect();

    if let Some(alias_map) = crate::alias::AliasMap::new(alias_mappings, rename_all_mappings) {
        let count = alias_map.forward.len() + alias_map.forward_rules.len();
        tracing::info!(
            aliases = count,
            "Applying tool aliases (endpoint group: {})",
            group.name
        );
        service = BoxCloneService::new(crate::alias::AliasService::new(service, alias_map));
    }

    // Composite tools (only those where all tools are in this group)
    let group_composite_tools: Vec<_> = config
        .composite_tools
        .iter()
        .filter(|ct| {
            ct.tools.iter().all(|tool| {
                // Check if tool's backend is in this group
                let backend_name = tool.split('/').next().unwrap_or("");
                group_backend_names.contains(backend_name)
            })
        })
        .cloned()
        .collect();

    if !group_composite_tools.is_empty() {
        let count = group_composite_tools.len();
        tracing::info!(
            composite_tools = count,
            "Applying composite tool fan-out (endpoint group: {})",
            group.name
        );
        service = BoxCloneService::new(crate::composite::CompositeService::new(
            service,
            group_composite_tools,
        ));
    }

    // Bearer token scoping
    #[cfg(feature = "oauth")]
    if matches!(
        &config.auth,
        Some(crate::config::AuthConfig::Bearer {
            scoped_tokens,
            ..
        }) if !scoped_tokens.is_empty()
    ) {
        tracing::info!(
            "Enabling bearer token scoping middleware (endpoint group: {})",
            group.name
        );
        service = BoxCloneService::new(crate::bearer_scope::BearerScopingService::new(service));
    }

    // RBAC (JWT auth only)
    #[cfg(feature = "oauth")]
    {
        let rbac_config = match &config.auth {
            Some(
                crate::config::AuthConfig::Jwt {
                    roles,
                    role_mapping: Some(mapping),
                    ..
                }
                | crate::config::AuthConfig::OAuth {
                    roles,
                    role_mapping: Some(mapping),
                    ..
                },
            ) if !roles.is_empty() => {
                tracing::info!(
                    roles = roles.len(),
                    claim = %mapping.claim,
                    "Enabling RBAC (endpoint group: {})",
                    group.name
                );
                Some(crate::rbac::RbacConfig::new(roles, mapping))
            }
            _ => None,
        };

        if let Some(rbac) = rbac_config {
            service = BoxCloneService::new(crate::rbac::RbacService::new(service, rbac));
        }

        // OAuth `required_scopes` enforcement
        let required_scopes: &[String] = match &config.auth {
            Some(crate::config::AuthConfig::OAuth {
                required_scopes, ..
            }) => required_scopes,
            _ => &[],
        };
        if let Some(layer) = oauth_scope_layer(required_scopes) {
            tracing::info!(
                scopes = ?required_scopes,
                "Enabling OAuth required_scopes enforcement (endpoint group: {})",
                group.name
            );
            service = BoxCloneService::new(tower::Layer::layer(&layer, service));
        }

        // Token passthrough (inject ClientToken for forward_auth backends in this group)
        let forward_namespaces: HashSet<String> = group_backends
            .iter()
            .filter(|b| b.forward_auth)
            .map(|b| format!("{}{}", b.name, config.proxy.separator))
            .collect();

        if !forward_namespaces.is_empty() {
            tracing::info!(
                backends = ?forward_namespaces,
                "Enabling token passthrough for forward_auth backends (endpoint group: {})",
                group.name
            );
            service = BoxCloneService::new(crate::token::TokenPassthroughService::new(
                service,
                forward_namespaces,
            ));
        }
    }

    // Metrics
    #[cfg(feature = "metrics")]
    if config.observability.metrics.enabled {
        service = BoxCloneService::new(crate::metrics::MetricsService::new(service));
    }

    // Structured access logging
    if config.observability.access_log.enabled {
        tracing::info!(
            "Access logging enabled (target: mcp::access) (endpoint group: {})",
            group.name
        );
        service = BoxCloneService::new(crate::access_log::AccessLogService::new(
            service,
            &config.proxy.separator,
        ));
    }

    // Audit logging
    if config.observability.audit {
        tracing::info!(
            "Audit logging enabled (target: mcp::audit) (endpoint group: {})",
            group.name
        );
        let audited = tower::Layer::layer(&tower_mcp::AuditLayer::new(), service);
        service = BoxCloneService::new(tower_mcp::CatchError::new(audited));
    }

    Ok(service)
}

/// Build endpoint group routers from the configuration.
/// Returns a vector of EndpointGroupRouter and the set of backend names used in groups.
pub async fn build_endpoint_group_routers(
    config: &ProxyConfig,
    registry: Option<&EndpointGroupRegistry>,
) -> Result<(Vec<EndpointGroupRouter>, HashSet<String>)> {
    let mut endpoint_group_routers = Vec::new();
    let mut grouped_backend_names = HashSet::new();

    for group in &config.proxy.endpoint_groups {
        let router = build_single_endpoint_group(config, group).await?;
        endpoint_group_routers.push(router.clone());
        grouped_backend_names.extend(group.backends.iter().cloned());

        // Populate registry if provided
        if let Some(reg) = registry {
            reg.insert(router);
        }
    }

    Ok((endpoint_group_routers, grouped_backend_names))
}

/// Build a single endpoint group router from configuration.
/// This can be called for hot reload to rebuild individual groups.
pub async fn build_single_endpoint_group(
    config: &ProxyConfig,
    group: &EndpointGroupConfig,
) -> Result<EndpointGroupRouter> {
    // Validate path
    if !group.path.starts_with('/') {
        anyhow::bail!("Endpoint group '{}' path must start with '/'", group.name);
    }
    if group.path == "/admin" || group.path == "/mcp" || group.path == "/health" {
        anyhow::bail!(
            "Endpoint group '{}' path '{}' conflicts with reserved paths",
            group.name,
            group.path
        );
    }

    // Collect backend configs for this group
    let group_backends: Vec<_> = config
        .backends
        .iter()
        .filter(|b| group.backends.contains(&b.name))
        .collect();

    if group_backends.is_empty() {
        anyhow::bail!("Endpoint group '{}' has no valid backends", group.name);
    }

    // Build MCP proxy for this group
    let proxy_name = format!("{}-{}", config.proxy.name, group.name);
    let (mcp_proxy, _cb_handles) = build_mcp_proxy_for_backends(
        &proxy_name,
        &config.proxy.version,
        &config.proxy.separator,
        config.proxy.instructions.as_ref(),
        &group_backends,
    )
    .await?;

    // Build middleware stack for this group
    let group_backend_names: HashSet<String> =
        group_backends.iter().map(|b| b.name.clone()).collect();
    let service = build_endpoint_group_middleware_stack(
        config,
        group,
        mcp_proxy.clone(),
        &group_backend_names,
    )?;

    // Create HTTP router for this group
    let (router, session_handle) =
        tower_mcp::transport::http::HttpTransport::from_service(service).into_router_with_handle();

    // Apply auth (same as main proxy)
    let router = apply_auth(config, router).await?;

    Ok(EndpointGroupRouter {
        name: group.name.clone(),
        path: group.path.clone(),
        router,
        session_handle,
        inner: mcp_proxy,
    })
}

/// Get a fingerprint for an endpoint group config to detect changes.
pub fn endpoint_group_fingerprint(group: &EndpointGroupConfig) -> String {
    toml::to_string(group).unwrap_or_default()
}

/// OAuth scope enforcement layer (copied from proxy.rs to avoid private function access).
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

/// Apply inbound authentication middleware to the router (copied from proxy.rs).
async fn apply_auth(config: &ProxyConfig, router: Router) -> Result<Router> {
    let router = if let Some(auth) = &config.auth {
        match auth {
            crate::config::AuthConfig::Bearer {
                tokens,
                scoped_tokens,
            } => {
                let total = tokens.len() + scoped_tokens.len();
                if scoped_tokens.is_empty() {
                    // Simple bearer auth: use StaticBearerValidator
                    tracing::info!(token_count = total, "Enabling bearer token auth");
                    let validator =
                        tower_mcp::auth::StaticBearerValidator::new(tokens.iter().cloned());
                    let layer = tower_mcp::auth::AuthLayer::new(validator);
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
            crate::config::AuthConfig::Jwt {
                issuer,
                audience,
                jwks_uri,
                ..
            } => {
                tracing::info!(
                    issuer = %issuer,
                    audience = %audience,
                    jwks_uri = %jwks_uri,
                    "Enabling JWT auth (JWKS)"
                );
                let validator = tower_mcp::oauth::JwksValidator::builder(jwks_uri)
                    .expected_audience(audience)
                    .expected_issuer(issuer)
                    .build()
                    .await
                    .context("building JWKS validator")?;

                let addr = format!(
                    "http://{}:{}",
                    config.proxy.listen.host, config.proxy.listen.port
                );
                let metadata = tower_mcp::oauth::ProtectedResourceMetadata::new(&addr)
                    .authorization_server(issuer);

                let layer = tower_mcp::oauth::OAuthLayer::new(validator, metadata);
                router.layer(layer)
            }
            #[cfg(not(feature = "oauth"))]
            crate::config::AuthConfig::Jwt { .. } => {
                anyhow::bail!(
                    "JWT auth requires the 'oauth' feature. Rebuild with: cargo install mcp-proxy --features oauth"
                );
            }
            #[cfg(feature = "oauth")]
            crate::config::AuthConfig::OAuth {
                issuer,
                audience,
                token_validation,
                jwks_uri,
                introspection_endpoint,
                client_id,
                client_secret,
                required_scopes: _required_scopes,
                ..
            } => {
                use crate::config::TokenValidationStrategy;

                tracing::info!(
                    issuer = %issuer,
                    audience = %audience,
                    strategy = ?token_validation,
                    "Enabling OAuth 2.1 auth"
                );

                // Auto-discover endpoints from issuer if not overridden
                let discovered = crate::introspection::discover_auth_server(issuer)
                    .await
                    .context("discovering OAuth authorization server")?;

                let effective_jwks_uri = jwks_uri
                    .as_deref()
                    .or(discovered.jwks_uri.as_deref())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "JWKS URI not found via discovery and not configured manually"
                        )
                    })?;

                let effective_introspection = introspection_endpoint
                    .as_deref()
                    .or(discovered.introspection_endpoint.as_deref());

                let addr = format!(
                    "http://{}:{}",
                    config.proxy.listen.host, config.proxy.listen.port
                );
                let metadata = tower_mcp::oauth::ProtectedResourceMetadata::new(&addr)
                    .authorization_server(issuer);

                match token_validation {
                    TokenValidationStrategy::Jwt => {
                        let validator =
                            tower_mcp::oauth::JwksValidator::builder(effective_jwks_uri)
                                .expected_audience(audience)
                                .expected_issuer(issuer)
                                .build()
                                .await
                                .context("building JWKS validator")?;
                        let layer = tower_mcp::oauth::OAuthLayer::new(validator, metadata);
                        router.layer(layer)
                    }
                    TokenValidationStrategy::Introspection => {
                        let endpoint = effective_introspection.ok_or_else(|| {
                            anyhow::anyhow!(
                                "introspection endpoint not found via discovery and not configured"
                            )
                        })?;
                        let validator = crate::introspection::IntrospectionValidator::new(
                            endpoint,
                            client_id.as_deref().unwrap(),
                            client_secret.as_deref().unwrap(),
                        )
                        .expected_audience(audience);
                        let layer = tower_mcp::oauth::OAuthLayer::new(validator, metadata);
                        router.layer(layer)
                    }
                    TokenValidationStrategy::Both => {
                        let endpoint = effective_introspection.ok_or_else(|| {
                            anyhow::anyhow!(
                                "introspection endpoint not found via discovery and not configured"
                            )
                        })?;
                        let jwt_validator =
                            tower_mcp::oauth::JwksValidator::builder(effective_jwks_uri)
                                .expected_audience(audience)
                                .expected_issuer(issuer)
                                .build()
                                .await
                                .context("building JWKS validator")?;
                        let introspection_validator =
                            crate::introspection::IntrospectionValidator::new(
                                endpoint,
                                client_id.as_deref().unwrap(),
                                client_secret.as_deref().unwrap(),
                            )
                            .expected_audience(audience);
                        let fallback = crate::introspection::FallbackValidator::new(
                            jwt_validator,
                            introspection_validator,
                        );
                        let layer = tower_mcp::oauth::OAuthLayer::new(fallback, metadata);
                        router.layer(layer)
                    }
                }
            }
            #[cfg(not(feature = "oauth"))]
            crate::config::AuthConfig::OAuth { .. } => {
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
