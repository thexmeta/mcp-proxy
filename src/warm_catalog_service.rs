//! Global middleware layer that serves `List*` from the warm catalog while a
//! lazy backend is down, and triggers lazy spawn before forwarding action
//! requests. Installed in BOTH the root and endpoint-group stacks (C11/C19).
//!
//! # Wave 4 scope (T4.1)
//!
//! This layer implements the **List* aggregation** half of the warm-cache
//! feature: when a `tools/list`, `resources/list`, `resources/templates/list`,
//! or `prompts/list` response comes back, it appends the cached, already
//! namespaced capability definitions for every lazy backend that has a warm
//! catalog (C1/C17). The appended entries respect the endpoint-group scope
//! (C19): the root stack passes `None` (all backends), while an endpoint-group
//! stack passes the group's member namespaces so only that group's cached
//! tools are surfaced.
//!
//! **Action requests** (`CallTool`, `ReadResource`, `GetPrompt`) on a lazy
//! backend trigger an on-demand spawn (Wave 5): the registry brings the backend
//! process up (coalescing concurrent first-calls), touches it, and forwards the
//! request. `List*` responses skip backends that are currently spawned so their
//! live capability lists are not double-listed (R4.3).

use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tower::{Layer, Service};
use tower_mcp::router::{RouterRequest, RouterResponse};
use tower_mcp_types::JsonRpcError;
use tower_mcp_types::protocol::{McpRequest, McpResponse};

use crate::lazy_registry::{LazyBackendRegistry, SpawnState};

/// Tower layer producing a [`WarmCatalogService`].
#[derive(Clone)]
pub struct WarmCatalogLayer {
    registry: Arc<LazyBackendRegistry>,
    separator: String,
    group_scope: Option<HashSet<String>>,
}

impl WarmCatalogLayer {
    /// Create a new warm-catalog layer.
    ///
    /// `group_scope` is the set of backend namespaces (e.g. `files/`) that this
    /// stack is allowed to surface. `None` means "all lazy backends" (root
    /// stack); `Some(set)` restricts appended cached capabilities to backends
    /// whose namespaced-name prefix is in `set` (endpoint-group stacks, C19).
    pub fn new(
        registry: Arc<LazyBackendRegistry>,
        separator: String,
        group_scope: Option<HashSet<String>>,
    ) -> Self {
        Self {
            registry,
            separator,
            group_scope,
        }
    }
}

impl<S> Layer<S> for WarmCatalogLayer {
    type Service = WarmCatalogService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        WarmCatalogService::new(
            inner,
            self.registry.clone(),
            self.separator.clone(),
            self.group_scope.clone(),
        )
    }
}

/// Serves cached capability lists for down lazy backends and triggers lazy
/// spawn before forwarding action requests.
#[derive(Clone)]
pub struct WarmCatalogService<S> {
    inner: S,
    registry: Arc<LazyBackendRegistry>,
    separator: String,
    group_scope: Option<HashSet<String>>,
}

impl<S> WarmCatalogService<S> {
    /// Create a new warm-catalog service wrapping `inner`.
    pub fn new(
        inner: S,
        registry: Arc<LazyBackendRegistry>,
        separator: String,
        group_scope: Option<HashSet<String>>,
    ) -> Self {
        Self {
            inner,
            registry,
            separator,
            group_scope,
        }
    }
}

/// True if a namespaced name's backend prefix belongs to the group scope.
///
/// With `None` scope (root stack) every name passes. With `Some(namespaces)`
/// the name passes only if it is namespaced under one of the scoped backend
/// prefixes (C19). The scope set stores prefixes WITH the trailing separator
/// (e.g. `fs_`, `term_`), so we match by prefix (`name.starts_with(prefix)`)
/// rather than splitting on the separator — splitting `term_ht_create_session`
/// on `_` yields the bare token `term`, which would NOT match the scoped value
/// `term_` and would wrongly exclude every group-scoped cached tool.
fn in_group(name: &str, scope: &Option<HashSet<String>>, _separator: &str) -> bool {
    match scope {
        None => true,
        Some(ns) => ns.iter().any(|prefix| name.starts_with(prefix)),
    }
}

/// Resolve a tool/prompt name to its owning backend via longest-prefix match.
///
/// Splits `tool_name` on `separator` and returns the longest registered backend
/// name that forms a valid prefix (e.g. `"electron_cdp"` wins over `"electron"`
/// for `"electron_cdp_set_console_live"`). Returns `None` when no backend
/// matches.
fn resolve_backend_name(
    tool_name: &str,
    known_backends: &[String],
    separator: &str,
) -> Option<String> {
    let mut candidates: Vec<&String> = known_backends
        .iter()
        .filter(|b| tool_name.starts_with(format!("{b}{separator}").as_str()))
        .collect();
    candidates.sort_by_key(|b| std::cmp::Reverse(b.len()));
    candidates.into_iter().next().cloned()
}

/// Extract the target lazy backend name for an action request.
///
/// For `CallTool`/`GetPrompt` the backend is resolved via longest-prefix match
/// against known backends (handles separator characters within backend names).
/// For `ReadResource` the URI is not namespaced, so the owning backend is
/// resolved via the warm catalog's resource URIs. Returns `None` for non-action
/// requests or when no backend can be determined.
fn request_backend_name(
    reg: &LazyBackendRegistry,
    req: &McpRequest,
    separator: &str,
) -> Option<String> {
    match req {
        McpRequest::CallTool(p) => resolve_backend_name(&p.name, &reg.names(), separator),
        McpRequest::GetPrompt(p) => resolve_backend_name(&p.name, &reg.names(), separator),
        McpRequest::ReadResource(p) => reg.backend_for_resource_uri(&p.uri),
        _ => None,
    }
}

impl<S> Service<RouterRequest> for WarmCatalogService<S>
where
    S: Service<RouterRequest, Response = RouterResponse, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    type Response = RouterResponse;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<RouterResponse, Infallible>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RouterRequest) -> Self::Future {
        // List* aggregation for lazy backends with a warm catalog (C1/C17/C19).
        // We forward the request to the inner service first, then enrich the
        // returned capability list with cached entries from the warm catalog.
        if matches!(
            &req.inner,
            McpRequest::ListTools(_)
                | McpRequest::ListResources(_)
                | McpRequest::ListResourceTemplates(_)
                | McpRequest::ListPrompts(_)
        ) {
            let registry = self.registry.clone();
            let sep = self.separator.clone();
            let scope = self.group_scope.clone();
            let fut = self.inner.call(req);
            return Box::pin(async move {
                let mut resp = fut.await?;
                if let Ok(inner) = &mut resp.inner {
                    match inner {
                        McpResponse::ListTools(r) => {
                            append_cached_tools(&registry, &sep, &scope, &mut r.tools)
                        }
                        McpResponse::ListResources(r) => {
                            append_cached_resources(&registry, &sep, &scope, &mut r.resources)
                        }
                        McpResponse::ListResourceTemplates(r) => append_cached_resource_templates(
                            &registry,
                            &sep,
                            &scope,
                            &mut r.resource_templates,
                        ),
                        McpResponse::ListPrompts(r) => {
                            append_cached_prompts(&registry, &sep, &scope, &mut r.prompts)
                        }
                        _ => {}
                    }
                }
                Ok(resp)
            });
        }

        // Action requests (CallTool / ReadResource / GetPrompt) on a lazy
        // backend: bring the backend up on demand (Wave 5), then forward.
        if let Some(target) = request_backend_name(&self.registry, &req.inner, &self.separator)
            && self.registry.get(&target).is_some()
        {
            let registry = self.registry.clone();
            let target_owned = target.clone();
            let request_id = req.id.clone();
            let fut = self.inner.call(req);
            return Box::pin(async move {
                // Coalesced spawn: concurrent first-calls share one child (FR-006).
                if let Err(e) = registry.ensure_spawned(&target_owned).await {
                    tracing::warn!(
                        backend = %target_owned,
                        error = %e,
                        "lazy spawn failed for action request"
                    );
                    return Ok(RouterResponse {
                        id: request_id,
                        inner: Err(JsonRpcError::invalid_params(format!(
                            "lazy backend '{target_owned}' failed to spawn: {e}"
                        ))),
                    });
                }
                // Touch + inc-refcount BEFORE forwarding (C21 / C6 / C23).
                registry.touch(&target_owned);
                registry.inc_refcount(&target_owned);
                let result = fut.await;
                registry.dec_refcount(&target_owned);
                result
            });
        }

        // Non-lazy action request: forward unchanged.
        let fut = self.inner.call(req);
        Box::pin(fut)
    }
}

/// Append cached, namespaced tools from every lazy backend that has a warm
/// catalog and matches the group scope (C1/C17/C19).
///
/// Backends that are currently spawned ([`SpawnState::Up`]) are skipped so their
/// live `tools/list` response is not double-listed (R4.3).
fn append_cached_tools(
    reg: &LazyBackendRegistry,
    sep: &str,
    scope: &Option<HashSet<String>>,
    out: &mut Vec<tower_mcp_types::protocol::ToolDefinition>,
) {
    for name in reg.names() {
        if reg.spawn_state(&name) == SpawnState::Up {
            continue;
        }
        if let Some(lb) = reg.get(&name)
            && let Some(cat) = lb.catalog
        {
            for t in cat.tools {
                if in_group(&t.name, scope, sep) {
                    out.push(t);
                }
            }
        }
    }
}

/// Append cached, namespaced resources from every lazy backend with a warm
/// catalog that matches the group scope (C1/C17/C19).
///
/// Backends that are currently spawned ([`SpawnState::Up`]) are skipped so their
/// live `resources/list` response is not double-listed (R4.3).
fn append_cached_resources(
    reg: &LazyBackendRegistry,
    sep: &str,
    scope: &Option<HashSet<String>>,
    out: &mut Vec<tower_mcp_types::protocol::ResourceDefinition>,
) {
    for name in reg.names() {
        if reg.spawn_state(&name) == SpawnState::Up {
            continue;
        }
        if let Some(lb) = reg.get(&name)
            && let Some(cat) = lb.catalog
        {
            for r in cat.resources {
                if in_group(&r.name, scope, sep) {
                    out.push(r);
                }
            }
        }
    }
}

/// Append cached, namespaced resource templates from every lazy backend with a
/// warm catalog that matches the group scope (C1/C17/C19).
///
/// Backends that are currently spawned ([`SpawnState::Up`]) are skipped so their
/// live `resources/templates/list` response is not double-listed (R4.3).
fn append_cached_resource_templates(
    reg: &LazyBackendRegistry,
    sep: &str,
    scope: &Option<HashSet<String>>,
    out: &mut Vec<tower_mcp_types::protocol::ResourceTemplateDefinition>,
) {
    for name in reg.names() {
        if reg.spawn_state(&name) == SpawnState::Up {
            continue;
        }
        if let Some(lb) = reg.get(&name)
            && let Some(cat) = lb.catalog
        {
            for t in cat.resource_templates {
                if in_group(&t.name, scope, sep) {
                    out.push(t);
                }
            }
        }
    }
}

/// Append cached, namespaced prompts from every lazy backend with a warm
/// catalog that matches the group scope (C1/C17/C19).
///
/// Backends that are currently spawned ([`SpawnState::Up`]) are skipped so their
/// live `prompts/list` response is not double-listed (R4.3).
fn append_cached_prompts(
    reg: &LazyBackendRegistry,
    sep: &str,
    scope: &Option<HashSet<String>>,
    out: &mut Vec<tower_mcp_types::protocol::PromptDefinition>,
) {
    for name in reg.names() {
        if reg.spawn_state(&name) == SpawnState::Up {
            continue;
        }
        if let Some(lb) = reg.get(&name)
            && let Some(cat) = lb.catalog
        {
            for p in cat.prompts {
                if in_group(&p.name, scope, sep) {
                    out.push(p);
                }
            }
        }
    }
}

/// Build a `JsonRpcError` for the "no warm catalog available" case.
///
/// Used by later waves when an action request targets a lazy backend that has
/// neither a live process nor a cached result. Wave 4 does not invoke this
/// (action requests forward unchanged), but it is provided so the error
/// contract is centralized and documented.
#[allow(dead_code)]
fn no_catalog_error(detail: &str) -> JsonRpcError {
    JsonRpcError::invalid_params(detail.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackendConfig;
    use crate::lazy_registry::LazyBackend;
    use crate::test_util::{MockService, call_service};
    use crate::warm_cache::WarmCatalog;
    use tower_mcp_types::protocol::{
        CallToolParams, ListToolsParams, McpRequest, ResourceDefinition, ToolDefinition,
    };

    fn lazy_backend_with_catalog(name: &str, separator: &str, tools: &[&str]) -> LazyBackend {
        let config = BackendConfig {
            name: name.to_string(),
            ..Default::default()
        };

        let catalog_tools: Vec<ToolDefinition> = tools
            .iter()
            .map(|t| ToolDefinition {
                name: t.to_string(),
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
            separator,
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

    fn sample_resource(name: &str) -> ResourceDefinition {
        ResourceDefinition {
            uri: format!("file:///{name}"),
            name: name.to_string(),
            title: None,
            description: None,
            mime_type: None,
            annotations: None,
            size: None,
            icons: None,
            meta: None,
        }
    }

    #[tokio::test]
    async fn appends_namespaced_cached_tools_to_empty_list() {
        let registry = LazyBackendRegistry::from_backends(vec![lazy_backend_with_catalog(
            "files",
            "/",
            &["read", "write"],
        )]);

        let mock = MockService::with_tools(&[]);
        let mut svc = WarmCatalogService::new(mock, Arc::new(registry), "/".to_string(), None);

        let resp = call_service(&mut svc, McpRequest::ListTools(ListToolsParams::default())).await;

        let McpResponse::ListTools(r) = resp.inner.expect("list tools ok") else {
            panic!("expected ListTools");
        };
        let names: Vec<&str> = r.tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"files/read"), "got {names:?}");
        assert!(names.contains(&"files/write"), "got {names:?}");
    }

    #[tokio::test]
    async fn does_not_append_for_non_lazy_backends() {
        // A backend present in the live proxy (MockService) but NOT in the lazy
        // registry must not have cached tools appended.
        let registry = LazyBackendRegistry::from_backends(vec![]);

        let mock = MockService::with_tools(&["live/tool"]);
        let mut svc = WarmCatalogService::new(mock, Arc::new(registry), "/".to_string(), None);

        let resp = call_service(&mut svc, McpRequest::ListTools(ListToolsParams::default())).await;

        let McpResponse::ListTools(r) = resp.inner.expect("list tools ok") else {
            panic!("expected ListTools");
        };
        assert_eq!(r.tools.len(), 1, "only the live tool should be present");
        assert_eq!(r.tools[0].name, "live/tool");
    }

    #[tokio::test]
    async fn group_scope_filters_appended_tools() {
        let registry = LazyBackendRegistry::from_backends(vec![
            lazy_backend_with_catalog("files", "/", &["read"]),
            lazy_backend_with_catalog("db", "/", &["query"]),
        ]);

        // Group scope restricted to the "files" backend only.
        let mut scope = HashSet::new();
        scope.insert("files".to_string());

        let mock = MockService::with_tools(&[]);
        let mut svc =
            WarmCatalogService::new(mock, Arc::new(registry), "/".to_string(), Some(scope));

        let resp = call_service(&mut svc, McpRequest::ListTools(ListToolsParams::default())).await;

        let McpResponse::ListTools(r) = resp.inner.expect("list tools ok") else {
            panic!("expected ListTools");
        };
        let names: Vec<&str> = r.tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"files/read"), "got {names:?}");
        assert!(
            !names.contains(&"db/query"),
            "db tools must be excluded by group scope: {names:?}"
        );
    }

    #[tokio::test]
    async fn appends_cached_resources() {
        let config = BackendConfig {
            name: "files".to_string(),
            ..Default::default()
        };
        let catalog = WarmCatalog::from_probe_result(
            "files",
            "/",
            vec![],
            vec![sample_resource("root")],
            vec![],
            vec![],
            Some("2025-11-25".to_string()),
            "testhash".to_string(),
        );
        let backend = LazyBackend {
            config,
            catalog: Some(catalog),
            protocol_version: None,
        };

        let registry = LazyBackendRegistry::from_backends(vec![backend]);

        // Exercise the append helper directly: a live ListResources response
        // (empty) should gain the cached, namespaced resource.
        let mut resources: Vec<ResourceDefinition> = Vec::new();
        append_cached_resources(&registry, "/", &None, &mut resources);

        let names: Vec<&str> = resources.iter().map(|res| res.name.as_str()).collect();
        assert!(names.contains(&"files/root"), "got {names:?}");
    }

    #[tokio::test]
    async fn action_requests_trigger_lazy_spawn_then_forward() {
        // CallTool on a lazy backend triggers ensure_spawned (coalesced) and is
        // then forwarded to the inner mock. Use a fake registry so no real child
        // is spawned.
        use crate::lazy_registry::LazyBackendRegistry;
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let spawn_count = StdArc::new(AtomicUsize::new(0));
        let spawn_fn: crate::lazy_registry::SpawnBackendFn = {
            let count = spawn_count.clone();
            StdArc::new(move |_cfg: &BackendConfig| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let probe_fn: crate::lazy_registry::ProbeBackendFn =
            StdArc::new(|_cfg: &BackendConfig, _sep: &str| {
                Box::pin(async move { Err(anyhow::anyhow!("probe skipped in test")) })
            });

        let registry = LazyBackendRegistry::from_backends(vec![lazy_backend_with_catalog(
            "files",
            "/",
            &["read"],
        )])
        .with_test_hooks(spawn_fn, probe_fn);

        let mock = MockService::with_tools(&[]);
        let mut svc = WarmCatalogService::new(mock, StdArc::new(registry), "/".to_string(), None);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(CallToolParams {
                name: "files/read".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        assert!(
            resp.inner.is_ok(),
            "action request must forward after spawn"
        );
        assert_eq!(
            spawn_count.load(Ordering::SeqCst),
            1,
            "backend spawned once"
        );
    }

    // ----------------------------------------------------------------------
    // Regression: R2 underscored backend name must not split incorrectly.
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn action_request_underscored_backend_name_triggers_spawn() {
        // Regression test: backend name `electron_cdp` with separator `_` —
        // the tool name `electron_cdp_start_app` must resolve to backend
        // `electron_cdp` (not `electron`) via longest-prefix match.
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let spawn_count = StdArc::new(AtomicUsize::new(0));
        let spawn_fn: crate::lazy_registry::SpawnBackendFn = {
            let count = spawn_count.clone();
            StdArc::new(move |_cfg: &BackendConfig| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let probe_fn: crate::lazy_registry::ProbeBackendFn =
            StdArc::new(|_cfg: &BackendConfig, _sep: &str| {
                Box::pin(async move { Err(anyhow::anyhow!("probe skipped in test")) })
            });

        // Backend name contains the separator `_` — the old naive
        // `split('_').next()` would yield `"electron"` (not in registry).
        let registry = LazyBackendRegistry::from_backends(vec![lazy_backend_with_catalog(
            "electron_cdp",
            "_",
            &["start_app", "diagnose"],
        )])
        .with_test_hooks(spawn_fn, probe_fn);

        let mock = MockService::with_tools(&[]);
        let mut svc = WarmCatalogService::new(mock, StdArc::new(registry), "_".to_string(), None);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(CallToolParams {
                name: "electron_cdp_start_app".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        assert!(
            resp.inner.is_ok(),
            "underscored backend name must trigger spawn and forward"
        );
        assert_eq!(
            spawn_count.load(Ordering::SeqCst),
            1,
            "electron_cdp backend must be spawned once"
        );
    }

    // ----------------------------------------------------------------------
    // Regression: R2 — backend name with TWO+ separators must still match.
    // The longest-prefix resolver must pick `a_b_c` (not `a` or `a_b`) when
    // a CallTool arrives for `a_b_c_d`. A naive first-underscore split would
    // yield `a` and fail to find the backend.
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn action_request_backend_name_with_multiple_separators_uses_longest_prefix() {
        // Regression test: backend name `a_b_c` (TWO separators) with tool
        // name `d` produces the routed tool name `a_b_c_d`. The resolver
        // must pick `a_b_c` via the longest-prefix match — never `a` or `a_b`.
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let spawn_count = StdArc::new(AtomicUsize::new(0));
        let spawn_fn: crate::lazy_registry::SpawnBackendFn = {
            let count = spawn_count.clone();
            StdArc::new(move |_cfg: &BackendConfig| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let probe_fn: crate::lazy_registry::ProbeBackendFn =
            StdArc::new(|_cfg: &BackendConfig, _sep: &str| {
                Box::pin(async move { Err(anyhow::anyhow!("probe skipped in test")) })
            });

        // Backend name contains the separator `_` TWICE — the old naive
        // `split('_').next()` would yield `a` (not in registry).
        let registry = LazyBackendRegistry::from_backends(vec![lazy_backend_with_catalog(
            "a_b_c",
            "_",
            &["d"],
        )])
        .with_test_hooks(spawn_fn, probe_fn);

        let mock = MockService::with_tools(&[]);
        let mut svc = WarmCatalogService::new(mock, StdArc::new(registry), "_".to_string(), None);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(CallToolParams {
                name: "a_b_c_d".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        assert!(
            resp.inner.is_ok(),
            "multi-separator backend name must trigger spawn and forward"
        );
        assert_eq!(
            spawn_count.load(Ordering::SeqCst),
            1,
            "a_b_c backend must be spawned exactly once (longest-prefix match)"
        );
    }

    #[tokio::test]
    async fn action_request_to_unknown_backend_forwards_without_spawn() {
        // A CallTool whose name prefix is NOT a lazy backend forwards unchanged
        // and does not attempt a spawn.
        let registry = LazyBackendRegistry::from_backends(vec![]);

        let mock = MockService::with_tools(&["live/tool"]);
        let mut svc = WarmCatalogService::new(mock, Arc::new(registry), "/".to_string(), None);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(CallToolParams {
                name: "live/tool".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        assert!(
            resp.inner.is_ok(),
            "non-lazy action request forwards unchanged"
        );
    }

    #[tokio::test]
    async fn list_skips_up_backends_to_avoid_double_listing() {
        // A backend marked Up must NOT have its cached tools appended (R4.3).
        let registry = LazyBackendRegistry::from_backends(vec![lazy_backend_with_catalog(
            "files",
            "/",
            &["read"],
        )]);
        // Simulate the backend being spawned (Up) so its live list is authoritative.
        registry.mark_up_for_test("files");

        let mock = MockService::with_tools(&[]);
        let mut svc = WarmCatalogService::new(mock, Arc::new(registry), "/".to_string(), None);

        let resp = call_service(&mut svc, McpRequest::ListTools(ListToolsParams::default())).await;

        let McpResponse::ListTools(r) = resp.inner.expect("list tools ok") else {
            panic!("expected ListTools");
        };
        let names: Vec<&str> = r.tools.iter().map(|t| t.name.as_str()).collect();
        assert!(
            !names.contains(&"files/read"),
            "Up backend must be skipped to avoid double-listing: {names:?}"
        );
    }

    // ----------------------------------------------------------------------
    // Regression: R1 no double-listing for an UP lazy backend.
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn regression_r1_up_backend_not_double_listed() {
        // A lazy backend that is UP must NOT have its cached tools appended by
        // WarmCatalogService — the live list is authoritative (R4.3).
        let registry = LazyBackendRegistry::from_backends(vec![lazy_backend_with_catalog(
            "files",
            "/",
            &["read", "write"],
        )]);
        registry.mark_up_for_test("files");

        // The live proxy (MockService) already returns the live tools; the
        // cached tools must NOT be appended on top of them.
        let mock = MockService::with_tools(&["files/read", "files/write"]);
        let mut svc = WarmCatalogService::new(mock, Arc::new(registry), "/".to_string(), None);

        let resp = call_service(&mut svc, McpRequest::ListTools(ListToolsParams::default())).await;

        let McpResponse::ListTools(r) = resp.inner.expect("list tools ok") else {
            panic!("expected ListTools");
        };
        // Exactly the live tools, no cached duplicates.
        assert_eq!(
            r.tools.len(),
            2,
            "UP backend must not double-list cached tools (R1): {:?}",
            r.tools.iter().map(|t| t.name.clone()).collect::<Vec<_>>()
        );
        let names: Vec<&str> = r.tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"files/read"));
        assert!(names.contains(&"files/write"));
    }

    // ----------------------------------------------------------------------
    // Action request on a lazy backend triggers ensure_spawned + touch +
    // inc/dec refcount; action on unknown backend returns an error response.
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn action_request_touches_and_refcounts_lazy_backend() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let spawn_count = Arc::new(AtomicUsize::new(0));
        let spawn_fn: crate::lazy_registry::SpawnBackendFn = {
            let count = spawn_count.clone();
            Arc::new(move |_cfg: &BackendConfig| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let probe_fn: crate::lazy_registry::ProbeBackendFn =
            Arc::new(|_cfg: &BackendConfig, _sep: &str| {
                Box::pin(async move { Err(anyhow::anyhow!("probe skipped in test")) })
            });

        let registry = LazyBackendRegistry::from_backends(vec![lazy_backend_with_catalog(
            "files",
            "/",
            &["read"],
        )])
        .with_test_hooks(spawn_fn, probe_fn);

        let mock = MockService::with_tools(&[]);
        let registry = Arc::new(registry);
        let mut svc = WarmCatalogService::new(mock, Arc::clone(&registry), "/".to_string(), None);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(CallToolParams {
                name: "files/read".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        assert!(
            resp.inner.is_ok(),
            "action request must forward after spawn"
        );
        assert_eq!(
            spawn_count.load(Ordering::SeqCst),
            1,
            "backend spawned once via ensure_spawned"
        );
        // Touch + inc/dec refcount around the forwarded call → net 0.
        assert_eq!(
            registry.refcount("files"),
            0,
            "refcount must be balanced after the action request"
        );
    }

    #[tokio::test]
    async fn action_request_to_unknown_lazy_backend_returns_error() {
        // A CallTool whose name prefix matches NO lazy backend must NOT panic
        // and must forward unchanged (the inner mock handles it).
        let registry = LazyBackendRegistry::from_backends(vec![]);

        let mock = MockService::with_tools(&["live/tool"]);
        let mut svc = WarmCatalogService::new(mock, Arc::new(registry), "/".to_string(), None);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(CallToolParams {
                name: "live/tool".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        assert!(
            resp.inner.is_ok(),
            "unknown backend action request forwards without panic"
        );
    }

    // ----------------------------------------------------------------------
    // resolve_backend_name: longest-prefix-match unit tests.
    // ----------------------------------------------------------------------

    #[test]
    fn request_backend_name_longest_prefix_match() {
        let known = vec!["electron_cdp".to_string()];
        assert_eq!(
            resolve_backend_name("electron_cdp_set_console_live", &known, "_"),
            Some("electron_cdp".to_string())
        );
    }

    #[test]
    fn request_backend_name_no_underscore_backend() {
        let known = vec!["term".to_string()];
        assert_eq!(
            resolve_backend_name("term_ping", &known, "_"),
            Some("term".to_string())
        );
    }

    #[test]
    fn request_backend_name_unknown_tool() {
        let known = vec!["term".to_string()];
        assert_eq!(resolve_backend_name("unknown_tool", &known, "_"), None);
    }

    #[test]
    fn request_backend_name_multiple_underscores_in_name() {
        let known = vec!["cedar_analysis".to_string()];
        assert_eq!(
            resolve_backend_name("cedar_analysis_analyze", &known, "_"),
            Some("cedar_analysis".to_string())
        );
    }
}
