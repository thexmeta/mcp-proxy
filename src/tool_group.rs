//! Tool grouping middleware for the proxy.
//!
//! Creates virtual tool namespaces that group tools from multiple backends
//! under a common prefix in `ListTools` responses, without creating separate
//! HTTP endpoints. Useful for LLM tool discovery and organization.
//!
//! # How it works
//!
//! Tool grouping maintains a bidirectional mapping between original tool names
//! and virtual grouped names (stored in [`ToolGroupMap`]):
//!
//! - **Forward mapping** (original -> virtual) -- applied to `ListTools` responses
//!   so clients see the virtual grouped names (e.g., `search/web_search`).
//! - **Reverse mapping** (virtual -> original) -- applied to `CallTool` requests
//!   so the backend receives the original name it expects (e.g., `exa/web_search_exa`).
//!
//! Tools that have no group mapping pass through unchanged in both directions.
//!
//! # Configuration
//!
//! Tool groups are configured at the proxy level in TOML:
//!
//! ```toml
//! [[tool_groups]]
//! name = "search"
//! tools = ["context7/*", "deepwiki/*", "tavily/web_search", "exa/web_search_exa"]
//! description = "All search tools"
//! mirror_to_original = true
//! ```
//!
//! With this config:
//! - `context7/list_docs` appears as `search/list_docs` (and also as `context7/list_docs` if mirror_to_original)
//! - `exa/web_search_exa` appears as `search/web_search`
//! - Calling `search/web_search` is transparently forwarded to the backend as `exa/web_search_exa`
//!
//! # Middleware stack position
//!
//! Tool grouping runs after tool aliasing and before composite tools in the
//! middleware stack. The ordering in `proxy.rs`:
//!
//! 1. Tool aliasing ([`crate::alias`])
//! 2. **Tool grouping** (this module)
//! 3. Composite tools ([`crate::composite`])

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use tower::{Layer, Service};

use tower_mcp::protocol::{McpRequest, McpResponse};
use tower_mcp::{RouterRequest, RouterResponse};

use crate::config::ToolGroupConfig;

/// Tower layer that produces a [`ToolGroupService`].
///
/// # Example
///
/// ```rust,ignore
/// use tower::ServiceBuilder;
/// use mcp_proxy::tool_group::{ToolGroupLayer, ToolGroupMap};
///
/// let tool_group_map = ToolGroupMap::new(vec![
///     ("search".into(), vec!["exa/web_search_exa".into(), "tavily/web_search".into()]),
/// ]).unwrap();
///
/// let service = ServiceBuilder::new()
///     .layer(ToolGroupLayer::new(tool_group_map))
///     .service(proxy);
/// ```
#[derive(Clone)]
pub struct ToolGroupLayer {
    tool_group_map: ToolGroupMap,
}

impl ToolGroupLayer {
    /// Create a new tool group layer with the given tool group map.
    pub fn new(tool_group_map: ToolGroupMap) -> Self {
        Self { tool_group_map }
    }
}

impl<S> Layer<S> for ToolGroupLayer {
    type Service = ToolGroupService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ToolGroupService::new(inner, self.tool_group_map.clone())
    }
}

/// The service that applies tool group mappings to requests and responses.
#[derive(Clone)]
pub struct ToolGroupService<S> {
    inner: S,
    tool_group_map: Arc<ToolGroupMap>,
}

impl<S> ToolGroupService<S> {
    /// Create a new tool group service.
    pub fn new(inner: S, tool_group_map: ToolGroupMap) -> Self {
        Self {
            inner,
            tool_group_map: Arc::new(tool_group_map),
        }
    }
}

/// Resolved tool group mappings for all groups.
///
/// Maintains bidirectional mappings between original tool names and virtual
/// grouped tool names.
#[derive(Clone, Debug)]
pub struct ToolGroupMap {
    /// Maps "namespace/original" -> "group/virtual" (for list responses)
    /// Multiple entries can map to the same virtual name if they're aliases.
    forward: HashMap<String, String>,
    /// Maps "group/virtual" -> "namespace/original" (for call requests)
    reverse: HashMap<String, String>,
    /// The group names that are configured
    group_names: Vec<String>,
    /// Whether tools also appear at their original names (mirror_to_original)
    mirror_to_original: bool,
}

impl ToolGroupMap {
    /// Build a tool group map from a list of ToolGroupConfig.
    /// Returns `None` if no tool groups are configured.
    pub fn new(configs: Vec<ToolGroupConfig>, separator: &str) -> Option<Self> {
        if configs.is_empty() {
            return None;
        }

        let mut forward = HashMap::new();
        let mut reverse = HashMap::new();
        let mut group_names = Vec::new();
        let mut mirror_to_original = true;

        for config in configs {
            group_names.push(config.name.clone());
            mirror_to_original = config.mirror_to_original;

            let group_prefix = format!("{}{}", config.name, separator);

            for tool_spec in &config.tools {
                // Parse tool_spec: "backend/tool" or "backend/*"
                let (backend_name, tool_pattern) = match tool_spec.split_once('/') {
                    Some((b, t)) => (b, t),
                    None => {
                        tracing::warn!(
                            "Invalid tool spec '{}', expected 'backend/tool' or 'backend/*'. Skipping.",
                            tool_spec
                        );
                        continue;
                    }
                };

                let backend_prefix = format!("{}{}", backend_name, separator);

                if tool_pattern == "*" {
                    // Wildcard: we need to discover tools from the backend at runtime
                    // For now, we'll note this as a wildcard pattern that will be
                    // resolved when ListTools is called
                    tracing::warn!(
                        "Wildcard tool patterns in tool groups are not yet supported at config time. Use explicit tool names."
                    );
                    continue;
                }

                // Exact tool mapping
                let original_name = format!("{}{}", backend_prefix, tool_pattern);
                let virtual_name = format!("{}{}", group_prefix, tool_pattern);

                forward.insert(original_name.clone(), virtual_name.clone());
                reverse.insert(virtual_name, original_name);
            }
        }

        if forward.is_empty() && reverse.is_empty() {
            return None;
        }

        Some(Self {
            forward,
            reverse,
            group_names,
            mirror_to_original,
        })
    }

    /// Apply forward mapping (for list responses).
    /// Returns the virtual grouped name if the tool is in a group, None otherwise.
    pub fn apply_forward(&self, namespaced_name: &str) -> Option<String> {
        self.forward.get(namespaced_name).cloned()
    }

    /// Apply reverse mapping (for call requests).
    /// Returns the original namespaced name if the virtual name is in a group, None otherwise.
    pub fn apply_reverse(&self, namespaced_virtual: &str) -> Option<String> {
        self.reverse.get(namespaced_virtual).cloned()
    }

    /// Get all group names
    pub fn group_names(&self) -> &[String] {
        &self.group_names
    }

    /// Whether tools also appear at their original names
    pub fn mirror_to_original(&self) -> bool {
        self.mirror_to_original
    }

    /// Get all forward mappings (for ListTools response modification)
    pub fn all_forward_mappings(&self) -> &HashMap<String, String> {
        &self.forward
    }
}

impl<S> Service<RouterRequest> for ToolGroupService<S>
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

    fn call(&mut self, mut req: RouterRequest) -> Self::Future {
        let tool_group_map = Arc::clone(&self.tool_group_map);

        // Reverse-map virtual names back to originals in requests
        match &mut req.inner {
            McpRequest::CallTool(params) => {
                if let Some(original) = tool_group_map.apply_reverse(&params.name) {
                    tracing::debug!(
                        "Tool group reverse mapping: {} -> {}",
                        params.name,
                        original
                    );
                    params.name = original;
                }
            }
            McpRequest::ReadResource(params) => {
                if let Some(original) = tool_group_map.apply_reverse(&params.uri) {
                    tracing::debug!(
                        "Tool group reverse mapping (resource): {} -> {}",
                        params.uri,
                        original
                    );
                    params.uri = original;
                }
            }
            McpRequest::GetPrompt(params) => {
                if let Some(original) = tool_group_map.apply_reverse(&params.name) {
                    tracing::debug!(
                        "Tool group reverse mapping (prompt): {} -> {}",
                        params.name,
                        original
                    );
                    params.name = original;
                }
            }
            _ => {}
        }

        // Call inner service
        let mut inner = self.inner.clone();
        let fut = inner.call(req);

        // Forward-map originals to virtual names in ListTools responses
        Box::pin(async move {
            let mut result = fut.await?;

            // Forward-map original names to virtual names in responses
            if let Ok(mcp_resp) = &mut result.inner {
                match mcp_resp {
                    McpResponse::ListTools(r) => {
                        let mut new_tools = Vec::new();

                        for tool in &r.tools {
                            // Check if this tool has a virtual mapping
                            if let Some(virtual_name) = tool_group_map.apply_forward(&tool.name) {
                                // Create a virtual copy of the tool with the grouped name
                                let mut virtual_tool = tool.clone();
                                virtual_tool.name = virtual_name;
                                new_tools.push(virtual_tool);
                            }

                            // If mirror_to_original is true, keep the original tool
                            if tool_group_map.mirror_to_original() {
                                new_tools.push(tool.clone());
                            }
                        }

                        // Replace the tools array with the modified one
                        r.tools = new_tools;
                    }
                    McpResponse::ListResources(r) => {
                        for resource in &mut r.resources {
                            if let Some(original) = tool_group_map.apply_reverse(&resource.uri) {
                                resource.uri = original;
                            }
                        }
                    }
                    McpResponse::ListPrompts(r) => {
                        for prompt in &mut r.prompts {
                            if let Some(original) = tool_group_map.apply_reverse(&prompt.name) {
                                prompt.name = original;
                            }
                        }
                    }
                    _ => {}
                }
            }

            Ok(result)
        })
    }
}
