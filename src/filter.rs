//! Capability filtering middleware for the proxy.
//!
//! This module provides two complementary filtering middlewares that control
//! which MCP capabilities (tools, resources, prompts) are visible and callable
//! through the proxy.
//!
//! # Capability filtering ([`CapabilityFilterService`])
//!
//! Wraps a `Service<RouterRequest>` and filters tools, resources, and prompts
//! based on per-backend allow/deny lists from config. Filtering happens in two
//! places:
//!
//! - **List responses** -- tools, resources, and prompts are removed from
//!   `ListTools`, `ListResources`, `ListResourceTemplates`, and `ListPrompts`
//!   responses before they reach the client.
//! - **Call/read/get requests** -- `CallTool`, `ReadResource`, and `GetPrompt`
//!   requests for filtered capabilities are rejected immediately with an
//!   `invalid_params` JSON-RPC error, without ever reaching the backend.
//!
//! ## Pattern support
//!
//! Filter patterns support three matching modes:
//!
//! - **Exact match** -- `"read_file"` matches only `read_file`.
//! - **Glob patterns** -- `"*_file"` matches `read_file`, `write_file`, etc.
//!   Standard glob wildcards (`*`, `?`) are supported.
//! - **Regex patterns** -- prefix a pattern with `re:` to use a regular
//!   expression: `"re:^list_.*$"` matches `list_files`, `list_users`, etc.
//!
//! ## Annotation-based filtering
//!
//! In addition to name-based allow/deny lists, the capability filter supports
//! filtering based on MCP tool annotations:
//!
//! - **`hide_destructive`** -- hides any tool whose `destructive_hint`
//!   annotation is `true`. Non-annotated tools are kept.
//! - **`read_only_only`** -- only exposes tools whose `read_only_hint`
//!   annotation is `true`. Tools without annotations are hidden (they are
//!   not known to be read-only).
//!
//! Name-based and annotation-based filters compose: a tool must pass both
//! the name filter and the annotation filter to be visible.
//!
//! ## Configuration
//!
//! Filters are configured per-backend in TOML. Use `expose_tools` (allowlist)
//! or `hide_tools` (denylist) -- not both:
//!
//! ```toml
//! [[backends]]
//! name = "files"
//! transport = "stdio"
//! command = "file-server"
//! # Allowlist: only these tools are visible
//! expose_tools = ["read_file", "list_*"]
//!
//! [[backends]]
//! name = "db"
//! transport = "stdio"
//! command = "db-server"
//! # Denylist: everything except these tools is visible
//! hide_tools = ["drop_table", "re:^delete_"]
//! # Annotation filter: hide destructive tools
//! hide_destructive = true
//!
//! [[backends]]
//! name = "safe"
//! transport = "stdio"
//! command = "safe-server"
//! # Only expose read-only tools
//! read_only_only = true
//! ```
//!
//! The same pattern applies to resources (`expose_resources` / `hide_resources`)
//! and prompts (`expose_prompts` / `hide_prompts`).
//!
//! ## Middleware stack position
//!
//! Capability filtering runs after request validation and before search-mode
//! filtering in the middleware stack. The ordering in `proxy.rs`:
//!
//! 1. Request coalescing
//! 2. Request validation ([`crate::validation`])
//! 3. **Capability filtering** (this module)
//! 4. Search-mode filtering (this module)
//! 5. Tool aliasing ([`crate::alias`])
//! 6. Composite tools ([`crate::composite`])
//!
//! # Search-mode filtering ([`SearchModeFilterService`])
//!
//! When the proxy is configured with `tool_exposure = "search"`, the
//! [`SearchModeFilterService`] hides all tools from `ListTools` responses
//! except those under the `proxy/` namespace prefix. This is useful when
//! aggregating many backends whose combined tool count would overwhelm an
//! LLM's context window.
//!
//! Backend tools remain callable -- they are just hidden from discovery.
//! Clients use `proxy/search_tools` to find tools and `proxy/call_tool`
//! to invoke them. Only `ListTools` responses are filtered; all other
//! request types (including `CallTool`) pass through unchanged.

use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tower::{Layer, Service};

use tower_mcp::protocol::{McpRequest, McpResponse};
use tower_mcp::{RouterRequest, RouterResponse};
use tower_mcp_types::JsonRpcError;

use crate::config::BackendFilter;

/// Tower layer that produces a [`CapabilityFilterService`].
///
/// # Example
///
/// ```rust,ignore
/// use tower::ServiceBuilder;
/// use mcp_proxy::filter::CapabilityFilterLayer;
///
/// let service = ServiceBuilder::new()
///     .layer(CapabilityFilterLayer::new(filters))
///     .service(proxy);
/// ```
#[derive(Clone)]
pub struct CapabilityFilterLayer {
    filters: Vec<BackendFilter>,
}

impl CapabilityFilterLayer {
    /// Create a new capability filter layer with the given filter rules.
    pub fn new(filters: Vec<BackendFilter>) -> Self {
        Self { filters }
    }
}

impl<S> Layer<S> for CapabilityFilterLayer {
    type Service = CapabilityFilterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CapabilityFilterService::new(inner, self.filters.clone())
    }
}

/// Middleware that filters capabilities from proxy responses.
#[derive(Clone)]
pub struct CapabilityFilterService<S> {
    inner: S,
    filters: Arc<Vec<BackendFilter>>,
}

impl<S> CapabilityFilterService<S> {
    /// Create a new capability filter service with the given filter rules.
    pub fn new(inner: S, filters: Vec<BackendFilter>) -> Self {
        Self {
            inner,
            filters: Arc::new(filters),
        }
    }
}

impl<S> Service<RouterRequest> for CapabilityFilterService<S>
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
        let filters = Arc::clone(&self.filters);
        let request_id = req.id.clone();

        // Check if this is a call/read/get for a filtered capability
        match &req.inner {
            McpRequest::CallTool(params) => {
                if let Some(reason) = check_tool_denied(&filters, &params.name) {
                    return Box::pin(async move {
                        Ok(RouterResponse {
                            id: request_id,
                            inner: Err(JsonRpcError::invalid_params(reason)),
                        })
                    });
                }
            }
            McpRequest::ReadResource(params) => {
                if let Some(reason) = check_resource_denied(&filters, &params.uri) {
                    return Box::pin(async move {
                        Ok(RouterResponse {
                            id: request_id,
                            inner: Err(JsonRpcError::invalid_params(reason)),
                        })
                    });
                }
            }
            McpRequest::GetPrompt(params) => {
                if let Some(reason) = check_prompt_denied(&filters, &params.name) {
                    return Box::pin(async move {
                        Ok(RouterResponse {
                            id: request_id,
                            inner: Err(JsonRpcError::invalid_params(reason)),
                        })
                    });
                }
            }
            _ => {}
        }

        let fut = self.inner.call(req);

        Box::pin(async move {
            let mut resp = fut.await?;

            // Filter list responses
            if let Ok(ref mut mcp_resp) = resp.inner {
                match mcp_resp {
                    McpResponse::ListTools(result) => {
                        result.tools.retain(|tool| {
                            for f in filters.iter() {
                                if let Some(local_name) = tool.name.strip_prefix(&f.namespace) {
                                    if !f.tool_filter.allows(local_name) {
                                        return false;
                                    }
                                    // Annotation-based filtering
                                    if let Some(ref annotations) = tool.annotations {
                                        if f.hide_destructive && annotations.destructive_hint {
                                            return false;
                                        }
                                        if f.read_only_only && !annotations.read_only_hint {
                                            return false;
                                        }
                                    } else if f.read_only_only {
                                        // No annotations = not known to be read-only
                                        return false;
                                    }
                                    return true;
                                }
                            }
                            true
                        });
                    }
                    McpResponse::ListResources(result) => {
                        result.resources.retain(|resource| {
                            for f in filters.iter() {
                                if let Some(local_uri) = resource.uri.strip_prefix(&f.namespace) {
                                    return f.resource_filter.allows(local_uri);
                                }
                            }
                            true
                        });
                    }
                    McpResponse::ListResourceTemplates(result) => {
                        result.resource_templates.retain(|template| {
                            for f in filters.iter() {
                                if let Some(local_uri) =
                                    template.uri_template.strip_prefix(&f.namespace)
                                {
                                    return f.resource_filter.allows(local_uri);
                                }
                            }
                            true
                        });
                    }
                    McpResponse::ListPrompts(result) => {
                        result.prompts.retain(|prompt| {
                            for f in filters.iter() {
                                if let Some(local_name) = prompt.name.strip_prefix(&f.namespace) {
                                    return f.prompt_filter.allows(local_name);
                                }
                            }
                            true
                        });
                    }
                    _ => {}
                }
            }

            Ok(resp)
        })
    }
}

/// Check if a namespaced tool name is denied by any filter.
/// Returns Some(reason) if denied.
fn check_tool_denied(filters: &[BackendFilter], namespaced_name: &str) -> Option<String> {
    for f in filters {
        if let Some(local_name) = namespaced_name.strip_prefix(&f.namespace) {
            if !f.tool_filter.allows(local_name) {
                return Some(format!("Tool not available: {}", namespaced_name));
            }
            return None;
        }
    }
    None
}

/// Check if a namespaced resource URI is denied by any filter.
fn check_resource_denied(filters: &[BackendFilter], namespaced_uri: &str) -> Option<String> {
    for f in filters {
        if let Some(local_uri) = namespaced_uri.strip_prefix(&f.namespace) {
            if !f.resource_filter.allows(local_uri) {
                return Some(format!("Resource not available: {}", namespaced_uri));
            }
            return None;
        }
    }
    None
}

/// Check if a namespaced prompt name is denied by any filter.
fn check_prompt_denied(filters: &[BackendFilter], namespaced_name: &str) -> Option<String> {
    for f in filters {
        if let Some(local_name) = namespaced_name.strip_prefix(&f.namespace) {
            if !f.prompt_filter.allows(local_name) {
                return Some(format!("Prompt not available: {}", namespaced_name));
            }
            return None;
        }
    }
    None
}

/// Tower layer that produces a [`GroupFilterService`].
///
/// Restricts which backend namespaces are visible within an endpoint group.
/// Only tools, resources, and prompts from the specified namespace prefixes
/// are shown; everything else is hidden and blocked.
///
/// This is used when a shared `McpProxy` is cloned for an endpoint group:
/// the group filter wraps the clone so that only its member backends'
/// capabilities are exposed.
#[derive(Clone)]
pub struct GroupFilterLayer {
    allowed_namespaces: HashSet<String>,
}

impl GroupFilterLayer {
    /// Create a new group filter layer that only allows the specified namespaces.
    ///
    /// Each namespace should include the separator suffix (e.g., `"fs/"`).
    pub fn new(allowed_namespaces: impl IntoIterator<Item = String>) -> Self {
        Self {
            allowed_namespaces: allowed_namespaces.into_iter().collect(),
        }
    }
}

impl<S> Layer<S> for GroupFilterLayer {
    type Service = GroupFilterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GroupFilterService::new(inner, self.allowed_namespaces.clone())
    }
}

/// Middleware that restricts capabilities to specific backend namespaces.
///
/// Only tools, resources, and prompts whose namespaced prefix is in the
/// allowed set are visible. Items from other backends are stripped from
/// list responses and blocked on call/read/get requests.
///
/// This middleware should be applied **before** [`CapabilityFilterService`]
/// (outermost in the stack) so that the group filter operates on the full
/// set of capabilities, and per-backend filtering runs on the already-scoped
/// result.
#[derive(Clone)]
pub struct GroupFilterService<S> {
    inner: S,
    allowed_namespaces: Arc<HashSet<String>>,
}

impl<S> GroupFilterService<S> {
    /// Create a new group filter service.
    pub fn new(inner: S, allowed_namespaces: HashSet<String>) -> Self {
        Self {
            inner,
            allowed_namespaces: Arc::new(allowed_namespaces),
        }
    }
}

impl<S> Service<RouterRequest> for GroupFilterService<S>
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
        let namespaces = Arc::clone(&self.allowed_namespaces);
        let request_id = req.id.clone();

        // Check for blocked call/read/get before passing to inner service.
        // Extract the name/uri early so we don't borrow req.inner across the call.
        let blocked = match &req.inner {
            McpRequest::CallTool(params) => {
                if !is_in_allowed_namespace(&namespaces, &params.name) {
                    Some(format!(
                        "Tool not available in this endpoint group: {}",
                        params.name
                    ))
                } else {
                    None
                }
            }
            McpRequest::ReadResource(params) => {
                if !is_in_allowed_namespace(&namespaces, &params.uri) {
                    Some(format!(
                        "Resource not available in this endpoint group: {}",
                        params.uri
                    ))
                } else {
                    None
                }
            }
            McpRequest::GetPrompt(params) => {
                if !is_in_allowed_namespace(&namespaces, &params.name) {
                    Some(format!(
                        "Prompt not available in this endpoint group: {}",
                        params.name
                    ))
                } else {
                    None
                }
            }
            _ => None,
        };

        if let Some(reason) = blocked {
            return Box::pin(async move {
                Ok(RouterResponse {
                    id: request_id,
                    inner: Err(JsonRpcError::invalid_params(reason)),
                })
            });
        }

        let fut = self.inner.call(req);

        Box::pin(async move {
            let mut resp = fut.await?;

            // Filter list responses to only show allowed namespaces
            if let Ok(ref mut mcp_resp) = resp.inner {
                match mcp_resp {
                    McpResponse::ListTools(result) => {
                        result
                            .tools
                            .retain(|tool| is_in_allowed_namespace(&namespaces, &tool.name));
                    }
                    McpResponse::ListResources(result) => {
                        result
                            .resources
                            .retain(|r| is_in_allowed_namespace(&namespaces, &r.uri));
                    }
                    McpResponse::ListResourceTemplates(result) => {
                        result
                            .resource_templates
                            .retain(|t| is_in_allowed_namespace(&namespaces, &t.uri_template));
                    }
                    McpResponse::ListPrompts(result) => {
                        result
                            .prompts
                            .retain(|p| is_in_allowed_namespace(&namespaces, &p.name));
                    }
                    _ => {}
                }
            }

            Ok(resp)
        })
    }
}

/// Check if a namespaced name starts with any of the allowed namespaces.
fn is_in_allowed_namespace(namespaces: &HashSet<String>, namespaced_name: &str) -> bool {
    namespaces
        .iter()
        .any(|ns| namespaced_name.starts_with(ns.as_str()))
}

/// Tower layer that produces a [`SearchModeFilterService`].
///
/// When search mode is enabled, `ListTools` responses are filtered to only
/// include tools under the given namespace prefix (typically `"proxy/"`).
/// All other requests pass through unchanged -- `CallTool` requests for
/// backend tools still work, allowing `proxy/call_tool` to forward them.
#[derive(Clone)]
pub struct SearchModeFilterLayer {
    prefix: String,
}

impl SearchModeFilterLayer {
    /// Create a new search mode filter that only lists tools matching `prefix`.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl<S> Layer<S> for SearchModeFilterLayer {
    type Service = SearchModeFilterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SearchModeFilterService {
            inner,
            prefix: self.prefix.clone(),
        }
    }
}

/// Middleware that filters `ListTools` responses to only show tools under
/// a specific namespace prefix.
///
/// Used by search mode to hide individual backend tools from tool listings
/// while keeping them callable through `proxy/call_tool`.
#[derive(Clone)]
pub struct SearchModeFilterService<S> {
    inner: S,
    prefix: String,
}

impl<S> SearchModeFilterService<S> {
    /// Create a new search mode filter service.
    pub fn new(inner: S, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
        }
    }
}

impl<S> Service<RouterRequest> for SearchModeFilterService<S>
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
        let prefix = self.prefix.clone();
        let fut = self.inner.call(req);

        Box::pin(async move {
            let mut resp = fut.await?;

            if let Ok(McpResponse::ListTools(ref mut result)) = resp.inner {
                result.tools.retain(|tool| tool.name.starts_with(&prefix));
            }

            Ok(resp)
        })
    }
}

#[cfg(test)]
mod tests {
    use tower_mcp::protocol::{McpRequest, McpResponse};

    use super::CapabilityFilterService;
    use crate::config::{BackendFilter, NameFilter};
    use crate::test_util::{MockService, call_service};

    fn allow_filter(namespace: &str, tools: &[&str]) -> BackendFilter {
        BackendFilter {
            namespace: namespace.to_string(),
            tool_filter: NameFilter::allow_list(tools.iter().map(|s| s.to_string())).unwrap(),
            resource_filter: NameFilter::PassAll,
            prompt_filter: NameFilter::PassAll,
            hide_destructive: false,
            read_only_only: false,
        }
    }

    fn deny_filter(namespace: &str, tools: &[&str]) -> BackendFilter {
        BackendFilter {
            namespace: namespace.to_string(),
            tool_filter: NameFilter::deny_list(tools.iter().map(|s| s.to_string())).unwrap(),
            resource_filter: NameFilter::PassAll,
            prompt_filter: NameFilter::PassAll,
            hide_destructive: false,
            read_only_only: false,
        }
    }

    #[tokio::test]
    async fn test_filter_allow_list_tools() {
        let mock = MockService::with_tools(&["fs/read", "fs/write", "fs/delete"]);
        let filters = vec![allow_filter("fs/", &["read", "write"])];
        let mut svc = CapabilityFilterService::new(mock, filters);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert!(names.contains(&"fs/read"));
                assert!(names.contains(&"fs/write"));
                assert!(!names.contains(&"fs/delete"), "delete should be filtered");
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_filter_deny_list_tools() {
        let mock = MockService::with_tools(&["fs/read", "fs/write", "fs/delete"]);
        let filters = vec![deny_filter("fs/", &["delete"])];
        let mut svc = CapabilityFilterService::new(mock, filters);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert!(names.contains(&"fs/read"));
                assert!(names.contains(&"fs/write"));
                assert!(!names.contains(&"fs/delete"));
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_filter_denies_call_to_hidden_tool() {
        let mock = MockService::with_tools(&["fs/read", "fs/delete"]);
        let filters = vec![allow_filter("fs/", &["read"])];
        let mut svc = CapabilityFilterService::new(mock, filters);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(tower_mcp::protocol::CallToolParams {
                name: "fs/delete".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        let err = resp.inner.unwrap_err();
        assert!(
            err.message.contains("not available"),
            "should deny: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn test_filter_allows_call_to_permitted_tool() {
        let mock = MockService::with_tools(&["fs/read"]);
        let filters = vec![allow_filter("fs/", &["read"])];
        let mut svc = CapabilityFilterService::new(mock, filters);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(tower_mcp::protocol::CallToolParams {
                name: "fs/read".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        assert!(resp.inner.is_ok(), "allowed tool should succeed");
    }

    #[tokio::test]
    async fn test_filter_pass_all_allows_everything() {
        let mock = MockService::with_tools(&["fs/read", "fs/write", "fs/delete"]);
        let filters = vec![BackendFilter {
            namespace: "fs/".to_string(),
            tool_filter: NameFilter::PassAll,
            resource_filter: NameFilter::PassAll,
            prompt_filter: NameFilter::PassAll,
            hide_destructive: false,
            read_only_only: false,
        }];
        let mut svc = CapabilityFilterService::new(mock, filters);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                assert_eq!(result.tools.len(), 3);
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_filter_unmatched_namespace_passes_through() {
        let mock = MockService::with_tools(&["db/query"]);
        let filters = vec![allow_filter("fs/", &["read"])];
        let mut svc = CapabilityFilterService::new(mock, filters);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                assert_eq!(result.tools.len(), 1, "unmatched namespace should pass");
                assert_eq!(result.tools[0].name, "db/query");
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    // --- Annotation-based filtering ---

    /// Create a mock service with tools that have annotations.
    fn mock_with_annotated_tools() -> MockService {
        use tower_mcp::protocol::ToolDefinition;
        use tower_mcp_types::protocol::ToolAnnotations;

        let tools = vec![
            ToolDefinition {
                name: "fs/read_file".to_string(),
                title: None,
                description: Some("Read a file".to_string()),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: None,
                icons: None,
                annotations: Some(ToolAnnotations {
                    title: None,
                    read_only_hint: true,
                    destructive_hint: false,
                    idempotent_hint: true,
                    open_world_hint: false,
                }),
                execution: None,
                meta: None,
            },
            ToolDefinition {
                name: "fs/delete_file".to_string(),
                title: None,
                description: Some("Delete a file".to_string()),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: None,
                icons: None,
                annotations: Some(ToolAnnotations {
                    title: None,
                    read_only_hint: false,
                    destructive_hint: true,
                    idempotent_hint: false,
                    open_world_hint: false,
                }),
                execution: None,
                meta: None,
            },
            ToolDefinition {
                name: "fs/write_file".to_string(),
                title: None,
                description: Some("Write a file".to_string()),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: None,
                icons: None,
                annotations: Some(ToolAnnotations {
                    title: None,
                    read_only_hint: false,
                    destructive_hint: false,
                    idempotent_hint: true,
                    open_world_hint: false,
                }),
                execution: None,
                meta: None,
            },
        ];
        MockService { tools }
    }

    #[tokio::test]
    async fn test_filter_hide_destructive() {
        let mock = mock_with_annotated_tools();
        let filters = vec![BackendFilter {
            namespace: "fs/".to_string(),
            tool_filter: NameFilter::PassAll,
            resource_filter: NameFilter::PassAll,
            prompt_filter: NameFilter::PassAll,
            hide_destructive: true,
            read_only_only: false,
        }];
        let mut svc = CapabilityFilterService::new(mock, filters);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert!(names.contains(&"fs/read_file"));
                assert!(names.contains(&"fs/write_file"));
                assert!(
                    !names.contains(&"fs/delete_file"),
                    "destructive tool should be hidden"
                );
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_filter_read_only_only() {
        let mock = mock_with_annotated_tools();
        let filters = vec![BackendFilter {
            namespace: "fs/".to_string(),
            tool_filter: NameFilter::PassAll,
            resource_filter: NameFilter::PassAll,
            prompt_filter: NameFilter::PassAll,
            hide_destructive: false,
            read_only_only: true,
        }];
        let mut svc = CapabilityFilterService::new(mock, filters);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert!(names.contains(&"fs/read_file"), "read-only tool visible");
                assert!(!names.contains(&"fs/delete_file"), "non-read-only hidden");
                assert!(!names.contains(&"fs/write_file"), "non-read-only hidden");
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    // --- Search mode filtering ---

    #[tokio::test]
    async fn test_search_mode_only_shows_prefix_tools() {
        let mock = MockService::with_tools(&[
            "proxy/search_tools",
            "proxy/call_tool",
            "proxy/tool_categories",
            "fs/read",
            "fs/write",
            "db/query",
        ]);
        let mut svc = super::SearchModeFilterService::new(mock, "proxy/");

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert_eq!(names.len(), 3, "only proxy/ tools should be listed");
                assert!(names.contains(&"proxy/search_tools"));
                assert!(names.contains(&"proxy/call_tool"));
                assert!(names.contains(&"proxy/tool_categories"));
                assert!(!names.contains(&"fs/read"));
                assert!(!names.contains(&"db/query"));
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_search_mode_allows_call_tool_for_backend() {
        let mock = MockService::with_tools(&["proxy/call_tool", "fs/read"]);
        let mut svc = super::SearchModeFilterService::new(mock, "proxy/");

        // CallTool requests should pass through regardless of namespace
        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(tower_mcp::protocol::CallToolParams {
                name: "fs/read".to_string(),
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
            "search mode should not block CallTool requests"
        );
    }

    #[tokio::test]
    async fn test_search_mode_no_proxy_tools_returns_empty() {
        let mock = MockService::with_tools(&["fs/read", "db/query"]);
        let mut svc = super::SearchModeFilterService::new(mock, "proxy/");

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                assert!(result.tools.is_empty(), "no proxy/ tools means empty list");
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    // --- Group filter tests ---

    use super::{GroupFilterLayer, GroupFilterService};
    use std::collections::HashSet;

    fn group_filter(svc: MockService, allowed: &[&str]) -> GroupFilterService<MockService> {
        let namespaces: HashSet<String> = allowed.iter().map(|s| s.to_string()).collect();
        GroupFilterService::new(svc, namespaces)
    }

    #[tokio::test]
    async fn test_group_filter_only_shows_member_namespaces() {
        let mock = MockService::with_tools(&["fs/read", "fs/write", "db/query", "db/schema"]);
        let mut svc = group_filter(mock, &["fs/"]);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert_eq!(names.len(), 2);
                assert!(names.contains(&"fs/read"));
                assert!(names.contains(&"fs/write"));
                assert!(!names.contains(&"db/query"), "db/ tools should be hidden");
                assert!(!names.contains(&"db/schema"), "db/ tools should be hidden");
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_group_filter_multiple_namespaces() {
        let mock = MockService::with_tools(&["fs/read", "db/query", "api/call", "ws/send"]);
        let mut svc = group_filter(mock, &["fs/", "db/"]);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert_eq!(names.len(), 2);
                assert!(names.contains(&"fs/read"));
                assert!(names.contains(&"db/query"));
                assert!(!names.contains(&"api/call"));
                assert!(!names.contains(&"ws/send"));
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_group_filter_blocks_call_for_non_member() {
        let mock = MockService::with_tools(&["fs/read", "db/query"]);
        let mut svc = group_filter(mock, &["fs/"]);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(tower_mcp::protocol::CallToolParams {
                name: "db/query".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        let err = resp.inner.unwrap_err();
        assert!(
            err.message.contains("not available in this endpoint group"),
            "should deny non-member: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn test_group_filter_allows_call_for_member() {
        let mock = MockService::with_tools(&["fs/read", "db/query"]);
        let mut svc = group_filter(mock, &["fs/"]);

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(tower_mcp::protocol::CallToolParams {
                name: "fs/read".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        assert!(resp.inner.is_ok(), "member tool should be callable");
    }

    #[tokio::test]
    async fn test_group_filter_empty_namespaces_hides_all() {
        let mock = MockService::with_tools(&["fs/read", "db/query"]);
        let mut svc = group_filter(mock, &[]);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                assert!(result.tools.is_empty(), "no allowed namespaces = no tools");
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_group_filter_layer_wraps_service() {
        let mock = MockService::with_tools(&["fs/read", "db/query"]);
        let layer = GroupFilterLayer::new(vec!["fs/".to_string()]);
        let mut svc = tower::ServiceBuilder::new().layer(layer).service(mock);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert_eq!(names, vec!["fs/read"]);
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }
}
