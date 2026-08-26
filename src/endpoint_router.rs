//! Endpoint group routing - creates separate MCP endpoints for grouped backends.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use axum::Router;
use tower::Layer;
use tower::util::BoxCloneService;
use tower_mcp::SessionHandle;
use tower_mcp::proxy::McpProxy;
use tower_mcp::{RouterRequest, RouterResponse};

use crate::config::{BackendConfig, EndpointGroupConfig, ProxyConfig};
use crate::proxy::{apply_2026_layers, build_protocol_support};

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

/// Build the middleware stack around an McpProxy for an endpoint group.
/// This mirrors `build_middleware_stack` but for a specific group's config.
///
/// When `group_namespaces` is `Some`, a [`GroupFilterService`] is inserted as
/// the innermost layer so that only tools/resources/prompts from the group's
/// member backends are visible. Per-backend capability filtering runs on top
/// of this, applying finer-grained allow/deny rules within the allowed set.
fn build_endpoint_group_middleware_stack(
    config: &ProxyConfig,
    group: &EndpointGroupConfig,
    proxy: McpProxy,
    group_backend_names: &HashSet<String>,
    group_namespaces: Option<HashSet<String>>,
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
) -> Result<BoxCloneService<RouterRequest, RouterResponse, Infallible>> {
    let mut service: BoxCloneService<RouterRequest, RouterResponse, Infallible> =
        BoxCloneService::new(proxy);

    // Clone the group scope up front: it is consumed by GroupFilterService below
    // and also needed (cloned again) for the warm-catalog layer after the 2026 trio.
    let group_scope_for_warm = group_namespaces.clone();

    // Group filter (innermost): restrict to member backend namespaces
    if let Some(namespaces) = group_namespaces
        && !namespaces.is_empty()
    {
        tracing::info!(
            namespaces = ?namespaces,
            "Applying group namespace filter (endpoint group: {})",
            group.name
        );
        service = BoxCloneService::new(crate::filter::GroupFilterService::new(service, namespaces));
    }

    // Innermost 2026-07-28 layers (SubscriptionsListen → Discover → MetaValidation).
    // Mirrors the root `/` stack via the shared `apply_2026_layers` helper so
    // group routes stay at protocol parity (e.g. `server/discover` works).
    // GroupFilter remains innermost-effective for tool scoping; these layers sit
    // immediately outside it and operate on the already-group-scoped request.
    tracing::info!(
        "Applying 2026-07-28 layers (SubscriptionsListen, Discover, MetaValidation) (endpoint group: {})",
        group.name
    );
    service = apply_2026_layers(service, config);

    // Warm catalog serving (C11/C19): immediately after the 2026 trio and
    // outside GroupFilter, so cached List* entries for down lazy backends are
    // appended and then scoped by the group filter. The group scope is the set
    // of member backend namespaces (or `None` when no group filter applies).
    tracing::info!(
        "Applying warm catalog serving layer (endpoint group: {})",
        group.name
    );
    service = BoxCloneService::new(
        crate::warm_catalog_service::WarmCatalogLayer::new(
            lazy_registry,
            config.proxy.separator.clone(),
            group_scope_for_warm,
        )
        .layer(service),
    );

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

/// Resolve the effective backend configs for an endpoint group.
///
/// Membership is the UNION of two sources that work together:
/// - backends explicitly listed in the group's `backends` field
/// - backends whose own `endpoint_groups` field references this group by name
///
/// A backend that references a group not declared in `config.proxy.endpoint_groups`
/// is ignored for membership (and a warning is emitted) so it is not silently
/// excluded from the default `/` endpoint.
pub fn resolve_group_backends<'a>(
    backends: &'a [BackendConfig],
    groups: &[EndpointGroupConfig],
    group: &EndpointGroupConfig,
) -> Vec<&'a BackendConfig> {
    let declared: HashSet<&str> = groups.iter().map(|g| g.name.as_str()).collect();
    backends
        .iter()
        .filter(|b| {
            let explicit = group.backends.contains(&b.name);
            let reverse = b.endpoint_groups.contains(&group.name);
            if reverse && !declared.contains(group.name.as_str()) {
                tracing::warn!(
                    backend = %b.name,
                    endpoint_group = %group.name,
                    "Backend references endpoint group that is not declared in proxy.endpoint_groups"
                );
            }
            explicit || reverse
        })
        .collect()
}

/// Build endpoint group routers from the configuration.
/// Returns a vector of EndpointGroupRouter and the set of backend names used in groups.
///
/// When `shared_proxy` is provided, each group reuses it (no duplicate process
/// spawning). When `None`, each group builds its own McpProxy (legacy mode).
pub async fn build_endpoint_group_routers(
    config: &ProxyConfig,
    registry: Option<&EndpointGroupRegistry>,
    shared_proxy: Option<&McpProxy>,
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
) -> Result<(Vec<EndpointGroupRouter>, HashSet<String>)> {
    let mut endpoint_group_routers = Vec::new();
    let mut grouped_backend_names = HashSet::new();

    for group in &config.proxy.endpoint_groups {
        let router =
            build_single_endpoint_group(config, group, shared_proxy, lazy_registry.clone()).await?;
        endpoint_group_routers.push(router.clone());
        grouped_backend_names.extend(
            resolve_group_backends(&config.backends, &config.proxy.endpoint_groups, group)
                .iter()
                .map(|b| b.name.clone()),
        );

        // Populate registry if provided
        if let Some(reg) = registry {
            reg.insert(router);
        }
    }

    Ok((endpoint_group_routers, grouped_backend_names))
}

/// Build a single endpoint group router from configuration.
/// This can be called for hot reload to rebuild individual groups.
///
/// When `shared_proxy` is provided, it is used directly (the group shares the
/// proxy with all other groups — each backend was spawned exactly once). When
/// `None`, a new McpProxy is built for this group only (legacy behavior).
pub async fn build_single_endpoint_group(
    config: &ProxyConfig,
    group: &EndpointGroupConfig,
    shared_proxy: Option<&McpProxy>,
    lazy_registry: Arc<crate::lazy_registry::LazyBackendRegistry>,
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

    // Collect backend configs for this group (UNION of explicit + reverse references)
    let group_backends =
        resolve_group_backends(&config.backends, &config.proxy.endpoint_groups, group);

    if group_backends.is_empty() {
        anyhow::bail!("Endpoint group '{}' has no valid backends", group.name);
    }

    // Use the shared McpProxy (all backends already spawned) or build a new one
    let mcp_proxy = match shared_proxy {
        Some(proxy) => {
            tracing::info!(
                group = %group.name,
                "Using shared McpProxy (no duplicate process spawning)"
            );
            proxy.clone()
        }
        None => {
            tracing::info!(
                group = %group.name,
                "Building dedicated McpProxy for endpoint group (legacy mode)"
            );
            let proxy_name = format!("{}-{}", config.proxy.name, group.name);
            let (proxy, _cb_handles) = crate::proxy::build_mcp_proxy_for_backends(
                &proxy_name,
                &config.proxy.version,
                &config.proxy.separator,
                config.proxy.instructions.as_ref(),
                &group_backends,
                config.proxy.shutdown_kill_timeout_secs,
            )
            .await?;
            proxy
        }
    };

    // Build middleware stack for this group
    let group_backend_names: HashSet<String> =
        group_backends.iter().map(|b| b.name.clone()).collect();

    // When using a shared proxy, build namespace allowlist so the group only
    // sees tools from its member backends. Legacy (dedicated) proxies already
    // have only the relevant backends, so no group filter is needed.
    let group_namespaces = if shared_proxy.is_some() {
        let namespaces: HashSet<String> = group_backend_names
            .iter()
            .map(|name| format!("{}{}", name, config.proxy.separator))
            .collect();
        Some(namespaces)
    } else {
        None
    };

    let service = build_endpoint_group_middleware_stack(
        config,
        group,
        mcp_proxy.clone(),
        &group_backend_names,
        group_namespaces,
        lazy_registry,
    )?;

    // Create HTTP router for this group. Protocol version support mirrors the
    // root `/` route via the shared `build_protocol_support` helper so group
    // routes honor `[proxy.protocol_support]` instead of the tower_mcp default.
    let (router, session_handle) = tower_mcp::transport::http::HttpTransport::from_service(service)
        .protocol_support(build_protocol_support(config)?)
        .into_router_with_handle();

    // Auto-inject Mcp-Method and MCP-Protocol-Version headers for backward compatibility
    let router = router.layer(axum::middleware::from_fn(
        crate::mcp_compat::inject_mcp_compat_headers,
    ));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TransportType;

    fn backend(name: &str, endpoint_groups: Vec<&str>) -> BackendConfig {
        BackendConfig {
            name: name.to_string(),
            transport: TransportType::Http,
            endpoint_groups: endpoint_groups.into_iter().map(String::from).collect(),
            ..Default::default()
        }
    }

    fn group(name: &str, path: &str, backends: Vec<&str>) -> EndpointGroupConfig {
        EndpointGroupConfig {
            name: name.to_string(),
            path: path.to_string(),
            backends: backends.into_iter().map(String::from).collect(),
            tools: vec![],
            description: None,
            tool_discovery: false,
        }
    }

    fn names<'a>(result: &[&'a BackendConfig]) -> Vec<&'a str> {
        result.iter().map(|b| b.name.as_str()).collect()
    }

    #[test]
    fn test_explicit_only() {
        let backends = vec![backend("a", vec![]), backend("b", vec![])];
        let groups = vec![group("g1", "/g1", vec!["a"])];
        let result = resolve_group_backends(&backends, &groups, &groups[0]);
        assert_eq!(names(&result), vec!["a"]);
    }

    #[test]
    fn test_reverse_only() {
        let backends = vec![backend("a", vec![]), backend("b", vec!["g1"])];
        let groups = vec![group("g1", "/g1", vec![])];
        let result = resolve_group_backends(&backends, &groups, &groups[0]);
        assert_eq!(names(&result), vec!["b"]);
    }

    #[test]
    fn test_union_both_sources() {
        let backends = vec![backend("a", vec![]), backend("b", vec!["g1"])];
        let groups = vec![group("g1", "/g1", vec!["a"])];
        let result = resolve_group_backends(&backends, &groups, &groups[0]);
        let mut result_names = names(&result);
        result_names.sort();
        assert_eq!(result_names, vec!["a", "b"]);
    }

    #[test]
    fn test_dedup_when_both_match() {
        let backends = vec![backend("a", vec!["g1"])];
        let groups = vec![group("g1", "/g1", vec!["a"])];
        let result = resolve_group_backends(&backends, &groups, &groups[0]);
        assert_eq!(result.len(), 1, "backend should appear exactly once");
        assert_eq!(names(&result), vec!["a"]);
    }

    #[test]
    fn test_unrelated_backend_excluded() {
        let backends = vec![backend("a", vec!["g2"]), backend("b", vec![])];
        let groups = vec![group("g1", "/g1", vec![]), group("g2", "/g2", vec![])];
        let result = resolve_group_backends(&backends, &groups, &groups[0]);
        assert!(result.is_empty(), "backend refs g2, not g1");
    }

    #[test]
    fn test_multiple_groups_independent() {
        let backends = vec![
            backend("a", vec!["g1"]),
            backend("b", vec!["g2"]),
            backend("c", vec!["g1"]),
        ];
        let groups = vec![group("g1", "/g1", vec![]), group("g2", "/g2", vec![])];
        let result_g1 = resolve_group_backends(&backends, &groups, &groups[0]);
        let result_g2 = resolve_group_backends(&backends, &groups, &groups[1]);
        let mut g1_names = names(&result_g1);
        g1_names.sort();
        assert_eq!(g1_names, vec!["a", "c"]);
        assert_eq!(names(&result_g2), vec!["b"]);
    }

    #[test]
    fn test_backend_in_multiple_groups() {
        let backends = vec![backend("a", vec!["g1", "g2"])];
        let groups = vec![group("g1", "/g1", vec![]), group("g2", "/g2", vec![])];
        let result_g1 = resolve_group_backends(&backends, &groups, &groups[0]);
        let result_g2 = resolve_group_backends(&backends, &groups, &groups[1]);
        assert_eq!(names(&result_g1), vec!["a"]);
        assert_eq!(names(&result_g2), vec!["a"]);
    }

    #[test]
    fn test_empty_group_empty_backends() {
        let backends = vec![backend("a", vec![]), backend("b", vec![])];
        let groups = vec![group("g1", "/g1", vec![])];
        let result = resolve_group_backends(&backends, &groups, &groups[0]);
        assert!(result.is_empty());
    }
}
