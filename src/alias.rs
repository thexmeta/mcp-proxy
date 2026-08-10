//! Tool aliasing middleware for the proxy.
//!
//! Rewrites tool names in list responses and call requests based on
//! per-backend alias configuration. This lets operators expose backend tools
//! under different names without modifying the backends themselves.
//!
//! # How it works
//!
//! Aliasing maintains a bidirectional mapping between original and aliased
//! names (stored in [`AliasMap`]):
//!
//! - **Forward mapping** (original -> alias) -- applied to `ListTools`,
//!   `ListResources`, and `ListPrompts` responses so clients see the
//!   aliased names.
//! - **Reverse mapping** (alias -> original) -- applied to `CallTool`,
//!   `ReadResource`, and `GetPrompt` requests so the backend receives
//!   the original name it expects.
//!
//! Names that have no alias configured pass through unchanged in both
//! directions.
//!
//! # Configuration
//!
//! Aliases are configured per-backend in TOML. The `from` field is the
//! backend-local tool name (without the namespace prefix); the `to` field
//! is the new name to expose:
//!
//! ```toml
//! [[backends]]
//! name = "files"
//! transport = "stdio"
//! command = "file-server"
//!
//! [[backends.aliases]]
//! from = "read_file"
//! to = "read"
//!
//! [[backends.aliases]]
//! from = "write_file"
//! to = "write"
//! ```
//!
//! With this config, `files/read_file` appears to clients as `files/read`,
//! and calling `files/read` is transparently forwarded to the backend as
//! `files/read_file`.
//!
//! # Middleware stack position
//!
//! Aliasing runs after capability filtering and search-mode filtering, so
//! filters operate on original names and aliases are applied last. The
//! ordering in `proxy.rs`:
//!
//! 1. Request validation ([`crate::validation`])
//! 2. Capability filtering ([`crate::filter`])
//! 3. Search-mode filtering ([`crate::filter`])
//! 4. **Tool aliasing** (this module)
//! 5. Composite tools ([`crate::composite`])

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use tower::{Layer, Service};
use tower_mcp::router::{RouterRequest, RouterResponse};
use tower_mcp_types::protocol::{McpRequest, McpResponse};

use crate::config::CompiledPattern;

/// Tower layer that produces an [`AliasService`].
///
/// # Example
///
/// ```rust,ignore
/// use tower::ServiceBuilder;
/// use mcp_proxy::alias::{AliasLayer, AliasMap};
///
/// let aliases = AliasMap::new(vec![
///     ("math/".into(), "add".into(), "sum".into()),
/// ]).unwrap();
///
/// let service = ServiceBuilder::new()
///     .layer(AliasLayer::new(aliases))
///     .service(proxy);
/// ```
#[derive(Clone)]
pub struct AliasLayer {
    aliases: AliasMap,
}

impl AliasLayer {
    /// Create a new alias layer with the given alias map.
    pub fn new(aliases: AliasMap) -> Self {
        Self { aliases }
    }
}

impl<S> Layer<S> for AliasLayer {
    type Service = AliasService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AliasService::new(inner, self.aliases.clone())
    }
}

/// The service that applies alias mappings to requests and responses.
#[derive(Clone)]
pub struct AliasService<S> {
    inner: S,
    aliases: Arc<AliasMap>,
}

impl<S> AliasService<S> {
    /// Create a new alias service.
    pub fn new(inner: S, aliases: AliasMap) -> Self {
        Self {
            inner,
            aliases: Arc::new(aliases),
        }
    }
}

/// A pattern-based rename rule for bulk tool renaming.
#[derive(Clone, Debug)]
pub struct RenameRule {
    /// The compiled pattern to match tool names.
    pattern: CompiledPattern,
    /// The replacement template (may contain $1, $2 for regex captures).
    replacement: String,
    /// The namespace prefix (e.g., "files/").
    namespace: String,
}

impl RenameRule {
    /// Create a new rename rule.
    pub fn new(namespace: String, from: String, to: String) -> Result<Self> {
        let pattern = CompiledPattern::compile(&from)?;
        Ok(Self {
            pattern,
            replacement: to,
            namespace,
        })
    }

    /// Check if this rule matches the given namespaced tool name.
    /// Returns the replacement if matched, None otherwise.
    pub fn apply_forward(&self, namespaced_name: &str) -> Option<String> {
        // Strip the namespace prefix
        let local_name = namespaced_name.strip_prefix(&self.namespace)?;
        if self.pattern.matches(local_name) {
            Some(format!(
                "{}{}",
                self.namespace,
                self.apply_replacement(local_name)
            ))
        } else {
            None
        }
    }

    /// Apply the replacement template to a matched local name.
    fn apply_replacement(&self, local_name: &str) -> String {
        match &self.pattern {
            CompiledPattern::Glob(pat) => {
                // For glob patterns, handle special cases:
                // 1. If pattern ends with "*" and replacement is empty -> strip prefix
                // 2. If pattern ends with "*" and replacement contains "$0" -> $0 is the ENTIRE matched string
                //
                // 3. Otherwise, replace $0 with entire matched string
                if pat.ends_with('*') {
                    let prefix = &pat[..pat.len() - 1]; // Remove trailing *
                    if local_name.starts_with(prefix) && self.replacement.is_empty() {
                        // Prefix stripping: from="prefix_*", to="" -> return suffix after prefix
                        return local_name
                            .strip_prefix(prefix)
                            .map(|s| s.to_string())
                            .unwrap_or_default();
                    }
                    // For replacement with $0, $0 = entire matched string (local_name)
                    // Fall through to default handling below
                }
                // Default: $0 is the entire matched string
                self.replacement.replace("$0", local_name)
            }
            CompiledPattern::Regex(re) => {
                // For regex patterns, replace $1, $2, etc. with capture groups
                let mut result = self.replacement.clone();
                if let Some(caps) = re.captures(local_name) {
                    for i in 1..=caps.len() {
                        if let Some(m) = caps.get(i) {
                            result = result.replace(&format!("${}", i), m.as_str());
                        }
                    }
                    // Also handle $0 for full match
                    result = result.replace("$0", local_name);
                }
                result
            }
        }
    }

    /// Try to reverse-match an aliased name back to the original.
    /// Returns the original namespaced name if matched, None otherwise.
    pub fn apply_reverse(&self, namespaced_aliased: &str) -> Option<String> {
        let local_aliased = namespaced_aliased.strip_prefix(&self.namespace)?;

        match &self.pattern {
            CompiledPattern::Glob(pat) => {
                // For glob patterns, we need to check if the aliased name
                // could have been produced by this rule.
                // We do a reverse glob match by checking if the pattern
                // could generate the aliased name.
                // For simplicity, we try to reconstruct the original by
                // reversing the replacement logic.
                self.reverse_glob_match(pat, local_aliased)
                    .map(|original_local| format!("{}{}", self.namespace, original_local))
            }
            CompiledPattern::Regex(re) => {
                // For regex with capture groups, we need to reverse the replacement.
                // This is complex; we'll try to match the replacement template
                // against the aliased name to extract captures, then apply the
                // original pattern.
                self.reverse_regex_match(re, local_aliased)
                    .map(|original_local| format!("{}{}", self.namespace, original_local))
            }
        }
    }

    /// Reverse match for glob patterns.
    /// Given a glob pattern like "tavily_*" and replacement like "search_$0",
    /// and an aliased name like "search_web", try to reconstruct "tavily_web".
    fn reverse_glob_match(&self, pat: &str, aliased: &str) -> Option<String> {
        // Simple case: pattern is "prefix_*" and replacement is "newprefix_$0"
        // or pattern is "*" and replacement is "prefix_$0"
        // or pattern is "prefix_*" and replacement is ""

        // For now, handle the common cases:
        if let Some(prefix) = pat.strip_suffix('*') {
            if self.replacement.is_empty() {
                // Prefix stripping case: from="prefix_*", to=""
                // The aliased name is the suffix (without prefix), so prepend prefix back
                Some(format!("{}{}", prefix, aliased))
            } else if self.replacement == "$0" {
                // Keep original case: from="prefix_*", to="$0"
                // The aliased name IS the original name, so return as-is
                Some(aliased.to_string())
            } else if self.replacement.contains("$0") {
                // Replacement uses $0 with prefix - e.g., from="*", to="prefix_$0"
                // Aliased name is "prefix_original", so original is aliased without "prefix_"
                let replacement_prefix = self.replacement.replace("$0", "");
                aliased
                    .strip_prefix(&replacement_prefix)
                    .map(|s| s.to_string())
            } else {
                None
            }
        } else if self.replacement.contains("$0") {
            // Pattern doesn't end with *, but replacement uses $0
            // This is a general case - $0 represents the entire match
            // For reverse, we'd need to know what the original pattern matched
            // which is complex. Return None for now.
            None
        } else {
            None
        }
    }

    /// Reverse match for regex patterns.
    fn reverse_regex_match(&self, _re: &regex::Regex, _aliased: &str) -> Option<String> {
        // This is complex - we'd need to reverse the replacement template.
        // For now, we'll skip regex reverse matching and rely on exact matches.
        // A full implementation would parse the replacement template and
        // reconstruct captures.
        None
    }
}

/// Resolved alias mappings for all backends.
#[derive(Clone)]
pub struct AliasMap {
    /// Maps "namespace/original" -> "namespace/alias" (for list responses)
    pub forward: HashMap<String, String>,
    /// Maps "namespace/alias" -> "namespace/original" (for call requests)
    pub reverse: HashMap<String, String>,
    /// Pattern-based rename rules (forward: original -> aliased)
    pub forward_rules: Vec<RenameRule>,
    /// Pattern-based rename rules (reverse: aliased -> original)
    reverse_rules: Vec<RenameRule>,
}

impl AliasMap {
    /// Build an alias map from exact mappings and pattern-based rename rules.
    /// Returns `None` if both are empty.
    pub fn new(
        exact_mappings: Vec<(String, String, String)>,
        rename_rules: Vec<(String, String, String)>, // (namespace, from, to)
    ) -> Option<Self> {
        if exact_mappings.is_empty() && rename_rules.is_empty() {
            return None;
        }

        let mut forward = HashMap::new();
        let mut reverse = HashMap::new();
        let mut forward_rules = Vec::new();
        let mut reverse_rules = Vec::new();

        // Process exact mappings
        for (namespace, from, to) in exact_mappings {
            let original = format!("{}{}", namespace, from);
            let aliased = format!("{}{}", namespace, to);
            forward.insert(original.clone(), aliased.clone());
            reverse.insert(aliased, original);
        }

        // Process pattern-based rename rules
        for (namespace, from, to) in rename_rules {
            if let Ok(rule) = RenameRule::new(namespace.clone(), from, to) {
                // Add to forward rules for list responses
                forward_rules.push(rule.clone());
                // Add to reverse rules for call requests (processed in reverse order for priority)
                reverse_rules.push(rule);
            }
        }

        // Reverse the reverse_rules so that later rules have higher priority
        // (first match wins when iterating in order)
        reverse_rules.reverse();

        Some(Self {
            forward,
            reverse,
            forward_rules,
            reverse_rules,
        })
    }

    /// Apply forward mapping (for list responses).
    /// First checks exact matches, then pattern-based rules (first match wins).
    pub fn apply_forward(&self, namespaced_name: &str) -> Option<String> {
        // Check exact matches first
        if let Some(aliased) = self.forward.get(namespaced_name) {
            return Some(aliased.clone());
        }
        // Check pattern-based rules in order
        for rule in &self.forward_rules {
            if let Some(aliased) = rule.apply_forward(namespaced_name) {
                return Some(aliased);
            }
        }
        None
    }

    /// Apply reverse mapping (for call requests).
    /// First checks exact matches, then pattern-based rules (first match wins).
    pub fn apply_reverse(&self, namespaced_aliased: &str) -> Option<String> {
        // Check exact matches first
        if let Some(original) = self.reverse.get(namespaced_aliased) {
            return Some(original.clone());
        }
        // Check pattern-based rules in order (reverse order for priority)
        for rule in &self.reverse_rules {
            if let Some(original) = rule.apply_reverse(namespaced_aliased) {
                return Some(original);
            }
        }
        None
    }
}

impl<S> Service<RouterRequest> for AliasService<S>
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
        let aliases = Arc::clone(&self.aliases);

        // Reverse-map aliased names back to originals in requests
        match &mut req.inner {
            McpRequest::CallTool(params) => {
                if let Some(original) = aliases.apply_reverse(&params.name) {
                    params.name = original;
                }
            }
            McpRequest::ReadResource(params) => {
                if let Some(original) = aliases.apply_reverse(&params.uri) {
                    params.uri = original;
                }
            }
            McpRequest::GetPrompt(params) => {
                if let Some(original) = aliases.apply_reverse(&params.name) {
                    params.name = original;
                }
            }
            _ => {}
        }

        let fut = self.inner.call(req);

        Box::pin(async move {
            let mut result = fut.await;

            // Forward-map original names to aliases in responses
            let Ok(ref mut resp) = result;
            if let Ok(mcp_resp) = &mut resp.inner {
                match mcp_resp {
                    McpResponse::ListTools(r) => {
                        for tool in &mut r.tools {
                            if let Some(aliased) = aliases.apply_forward(&tool.name) {
                                tool.name = aliased;
                            }
                        }
                    }
                    McpResponse::ListResources(r) => {
                        for res in &mut r.resources {
                            if let Some(aliased) = aliases.apply_forward(&res.uri) {
                                res.uri = aliased;
                            }
                        }
                    }
                    McpResponse::ListPrompts(r) => {
                        for prompt in &mut r.prompts {
                            if let Some(aliased) = aliases.apply_forward(&prompt.name) {
                                prompt.name = aliased;
                            }
                        }
                    }
                    _ => {}
                }
            }

            result
        })
    }
}

#[cfg(test)]
mod tests {
    use tower_mcp::protocol::{McpRequest, McpResponse};

    use super::{AliasMap, AliasService};
    use crate::test_util::{MockService, call_service};

    fn test_aliases() -> AliasMap {
        AliasMap::new(
            vec![
                ("files/".into(), "read_file".into(), "read".into()),
                ("files/".into(), "write_file".into(), "write".into()),
            ],
            vec![],
        )
        .unwrap()
    }

    #[test]
    fn test_alias_map_empty_returns_none() {
        assert!(AliasMap::new(vec![], vec![]).is_none());
    }

    #[test]
    fn test_alias_map_forward_and_reverse() {
        let aliases = test_aliases();
        assert_eq!(
            aliases.forward.get("files/read_file").unwrap(),
            "files/read"
        );
        assert_eq!(aliases.forward.len(), 2);
    }

    #[tokio::test]
    async fn test_alias_rewrites_list_tools() {
        let mock = MockService::with_tools(&["files/read_file", "files/write_file", "db/query"]);
        let mut svc = AliasService::new(mock, test_aliases());

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert!(names.contains(&"files/read"));
                assert!(names.contains(&"files/write"));
                assert!(names.contains(&"db/query")); // unchanged
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_alias_reverse_maps_call_tool() {
        let mock = MockService::with_tools(&["files/read_file"]);
        let mut svc = AliasService::new(mock, test_aliases());

        let resp = call_service(
            &mut svc,
            McpRequest::CallTool(tower_mcp::protocol::CallToolParams {
                name: "files/read".to_string(),
                arguments: serde_json::json!({}),
                meta: None,
                task: None,
                input_responses: None,
                request_state: None,
            }),
        )
        .await;

        match resp.inner.unwrap() {
            McpResponse::CallTool(result) => {
                assert_eq!(result.all_text(), "called: files/read_file");
            }
            other => panic!("expected CallTool, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_alias_passthrough_non_aliased() {
        let mock = MockService::with_tools(&["db/query"]);
        let mut svc = AliasService::new(mock, test_aliases());

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

        match resp.inner.unwrap() {
            McpResponse::CallTool(result) => {
                assert_eq!(result.all_text(), "called: db/query");
            }
            other => panic!("expected CallTool, got: {:?}", other),
        }
    }

    #[test]
    fn test_alias_map_with_rename_all_glob_patterns() {
        // Test glob pattern matching in forward direction
        let aliases = AliasMap::new(
            vec![],
            vec![
                ("files/".into(), "read_*".into(), "$0".into()), // Keep original name
                ("files/".into(), "write_*".into(), "write_$0".into()), // Prefix with "write_"
            ],
        )
        .unwrap();

        // Forward: original -> aliased
        // Pattern "read_*" with replacement "$0" keeps the name unchanged
        assert_eq!(
            aliases.apply_forward("files/read_file"),
            Some("files/read_file".to_string())
        );
        assert_eq!(
            aliases.apply_forward("files/read_data"),
            Some("files/read_data".to_string())
        );
        // Pattern "write_*" with replacement "write_$0" prepends "write_"
        assert_eq!(
            aliases.apply_forward("files/write_file"),
            Some("files/write_write_file".to_string())
        );
        assert_eq!(aliases.apply_forward("files/other"), None);

        // Reverse: aliased -> original (for prefix stripping patterns)
        assert_eq!(
            aliases.apply_reverse("files/read_file"),
            Some("files/read_file".to_string())
        );
        assert_eq!(
            aliases.apply_reverse("files/read_data"),
            Some("files/read_data".to_string())
        );
    }

    #[test]
    fn test_alias_map_with_rename_all_prefix_stripping() {
        // Test the common case: from="tavily_*", to="" (strip prefix)
        let aliases = AliasMap::new(
            vec![],
            vec![("tavily/".into(), "tavily_*".into(), "".into())],
        )
        .unwrap();

        // Forward: original -> aliased (strips prefix)
        assert_eq!(
            aliases.apply_forward("tavily/tavily_search"),
            Some("tavily/search".to_string())
        );
        assert_eq!(
            aliases.apply_forward("tavily/tavily_extract"),
            Some("tavily/extract".to_string())
        );

        // Reverse: aliased -> original (adds prefix back)
        assert_eq!(
            aliases.apply_reverse("tavily/search"),
            Some("tavily/tavily_search".to_string())
        );
        assert_eq!(
            aliases.apply_reverse("tavily/extract"),
            Some("tavily/tavily_extract".to_string())
        );
    }

    #[test]
    fn test_alias_map_with_rename_all_regex_patterns() {
        // Test regex pattern matching with capture groups
        let aliases = AliasMap::new(
            vec![],
            vec![
                ("db/".into(), "re:^list_(.+)$".into(), "get_$1".into()),
                (
                    "api/".into(),
                    "re:^query_(.+)_data$".into(),
                    "fetch_$1".into(),
                ),
            ],
        )
        .unwrap();

        // Forward: original -> aliased (with capture group replacement)
        assert_eq!(
            aliases.apply_forward("db/list_users"),
            Some("db/get_users".to_string())
        );
        assert_eq!(
            aliases.apply_forward("db/list_products"),
            Some("db/get_products".to_string())
        );
        assert_eq!(
            aliases.apply_forward("api/query_user_data"),
            Some("api/fetch_user".to_string())
        );
        assert_eq!(
            aliases.apply_forward("api/query_product_data"),
            Some("api/fetch_product".to_string())
        );

        // Non-matching patterns should pass through unchanged
        assert_eq!(aliases.apply_forward("db/create_user"), None);
        assert_eq!(aliases.apply_forward("api/query_other"), None);

        // Reverse: regex reverse matching is not fully implemented,
        // should fall back to exact matches (which there are none)
        // Note: regex reverse matching is complex and not yet implemented
    }

    #[tokio::test]
    async fn test_alias_rename_all_glob_in_list_tools() {
        let mock = MockService::with_tools(&[
            "files/read_file",
            "files/read_data",
            "files/write_file",
            "files/write_config",
            "db/query",
        ]);

        let aliases = AliasMap::new(
            vec![],
            vec![
                ("files/".into(), "read_*".into(), "$0".into()), // Keep original
                ("files/".into(), "write_*".into(), "write_$0".into()), // Prefix with "write_"
            ],
        )
        .unwrap();

        let mut svc = AliasService::new(mock, aliases);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                // Should see transformed names
                assert!(names.contains(&"files/read_file")); // unchanged (replacement is $0)
                assert!(names.contains(&"files/read_data")); // unchanged
                assert!(names.contains(&"files/write_write_file")); // prefixed
                assert!(names.contains(&"files/write_write_config")); // prefixed
                assert!(names.contains(&"db/query")); // unchanged
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_alias_rename_all_prefix_strip_in_list_tools() {
        let mock =
            MockService::with_tools(&["tavily/tavily_search", "tavily/tavily_extract", "db/query"]);

        let aliases = AliasMap::new(
            vec![],
            vec![("tavily/".into(), "tavily_*".into(), "".into())],
        )
        .unwrap();

        let mut svc = AliasService::new(mock, aliases);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                // Should see stripped names
                assert!(names.contains(&"tavily/search"));
                assert!(names.contains(&"tavily/extract"));
                assert!(names.contains(&"db/query")); // unchanged
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_alias_rename_all_regex_in_list_tools() {
        let mock = MockService::with_tools(&[
            "db/list_users",
            "db/list_products",
            "db/create_user",
            "api/query_user_data",
            "api/query_product_data",
        ]);

        let aliases = AliasMap::new(
            vec![],
            vec![
                ("db/".into(), "re:^list_(.+)$".into(), "get_$1".into()),
                (
                    "api/".into(),
                    "re:^query_(.+)_data$".into(),
                    "fetch_$1".into(),
                ),
            ],
        )
        .unwrap();

        let mut svc = AliasService::new(mock, aliases);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                // Should see transformed names
                assert!(names.contains(&"db/get_users"));
                assert!(names.contains(&"db/get_products"));
                assert!(names.contains(&"api/fetch_user"));
                assert!(names.contains(&"api/fetch_product"));
                // Non-matching should be unchanged
                assert!(names.contains(&"db/create_user"));
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }
}
