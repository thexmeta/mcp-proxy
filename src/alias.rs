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
    ///
    /// Matching is performed against the original (namespace-stripped) tool
    /// name only, never against the backend namespace prefix. This preserves
    /// the backend name and transforms only the server's own tool name. The
    /// transformed local name is then re-namespaced so the backend prefix
    /// stays intact.
    pub fn apply_forward(&self, namespaced_name: &str) -> Option<String> {
        // Match against the original (namespace-stripped) tool name, NOT the
        // backend namespace prefix. This preserves the backend name and only
        // transforms the server's own tool name.
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
                // Anchored glob handling. A glob with a trailing "*" (e.g.
                // "prefix_*") matches only at the start of the name, and a glob
                // with a leading "*" (e.g. "*_suffix") matches only at the end.
                // A bare "*" matches the entire name. Other shapes (middle "*",
                // or no "*") fall back to $0 substitution of the whole match.
                if pat.ends_with('*') && !pat.starts_with('*') {
                    let literal = &pat[..pat.len() - 1]; // strip trailing *
                    if local_name.starts_with(literal) && self.replacement.is_empty() {
                        // Prefix stripping: from="prefix_*", to="" -> drop the leading literal
                        return local_name
                            .strip_prefix(literal)
                            .map(|s| s.to_string())
                            .unwrap_or_default();
                    }
                    // $0 is the entire matched name
                    return self.replacement.replace("$0", local_name);
                }
                if pat.starts_with('*') && !pat.ends_with('*') {
                    let literal = &pat[1..]; // strip leading *
                    if local_name.ends_with(literal) && self.replacement.is_empty() {
                        // Suffix stripping: from="*_suffix", to="" -> drop the trailing literal
                        return local_name
                            .strip_suffix(literal)
                            .map(|s| s.to_string())
                            .unwrap_or_default();
                    }
                    // $0 is the entire matched name
                    return self.replacement.replace("$0", local_name);
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
        // Prefix glob: "prefix_*" (anchored at start)
        if pat.ends_with('*') && !pat.starts_with('*') {
            let literal = &pat[..pat.len() - 1];
            if self.replacement.is_empty() {
                // Prefix stripping: from="prefix_*", to="" -> prepend literal back
                Some(format!("{}{}", literal, aliased))
            } else if self.replacement == "$0" {
                // Keep original: from="prefix_*", to="$0" -> aliased IS the original
                Some(aliased.to_string())
            } else if self.replacement.contains("$0") {
                // Replacement uses $0 with a prefix, e.g. from="*", to="prefix_$0"
                let replacement_prefix = self.replacement.replace("$0", "");
                aliased
                    .strip_prefix(&replacement_prefix)
                    .map(|s| s.to_string())
            } else {
                None
            }
        }
        // Suffix glob: "*_suffix" (anchored at end)
        else if pat.starts_with('*') && !pat.ends_with('*') {
            let literal = &pat[1..];
            if self.replacement.is_empty() {
                // Suffix stripping: from="*_suffix", to="" -> append literal back
                Some(format!("{}{}", aliased, literal))
            } else if self.replacement == "$0" {
                // Keep original: from="*_suffix", to="$0" -> aliased IS the original
                Some(aliased.to_string())
            } else if self.replacement.contains("$0") {
                // Replacement uses $0 with a suffix, e.g. from="*", to="$0_suffix"
                let replacement_suffix = self.replacement.replace("$0", "");
                aliased
                    .strip_suffix(&replacement_suffix)
                    .map(|s| s.to_string())
            } else {
                None
            }
        } else if self.replacement.contains("$0") {
            // Non-anchored pattern with $0: cannot reliably reverse.
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
        // Test the common case: from="tavily_*", to="" (strip prefix).
        // Uses "_" separator so the backend namespace ("tavily_") collides
        // with the server's own "tavily_" prefix. The rule must match the
        // LOCAL name only and preserve the backend namespace.
        let aliases = AliasMap::new(
            vec![],
            vec![("tavily_".into(), "tavily_*".into(), "".into())],
        )
        .unwrap();

        // Forward: original -> aliased (strips the server's own prefix only,
        // backend namespace is preserved).
        assert_eq!(
            aliases.apply_forward("tavily_tavily_search"),
            Some("tavily_search".to_string())
        );
        assert_eq!(
            aliases.apply_forward("tavily_tavily_extract"),
            Some("tavily_extract".to_string())
        );

        // Reverse: aliased -> original (adds the server prefix back).
        assert_eq!(
            aliases.apply_reverse("tavily_search"),
            Some("tavily_tavily_search".to_string())
        );
        assert_eq!(
            aliases.apply_reverse("tavily_extract"),
            Some("tavily_tavily_extract".to_string())
        );
    }

    #[test]
    fn test_rename_all_strips_server_prefix_preserves_backend_namespace() {
        // Required test (1): from="tavily_*", to="" on the namespaced name
        // "tavily_tavily_search" must yield "tavily_search" — the backend
        // namespace is preserved and only the server's own "tavily_" prefix
        // is stripped.
        let aliases = AliasMap::new(
            vec![],
            vec![("tavily_".into(), "tavily_*".into(), "".into())],
        )
        .unwrap();

        assert_eq!(
            aliases.apply_forward("tavily_tavily_search"),
            Some("tavily_search".to_string())
        );
    }

    #[test]
    fn test_rename_all_no_match_when_local_name_lacks_prefix() {
        // Required test (2): from="roslyn_*", to="" on "roslyn_completion".
        // The local name is "completion", which does NOT match "roslyn_*",
        // so the rule does not apply and the name is unchanged (None).
        let aliases = AliasMap::new(
            vec![],
            vec![("roslyn_".into(), "roslyn_*".into(), "".into())],
        )
        .unwrap();

        assert_eq!(aliases.apply_forward("roslyn_completion"), None);
    }

    #[test]
    fn test_rename_all_reverse_round_trips_to_original() {
        // Required test (3): reverse of (1). Client sends aliased
        // "tavily_search" and it maps back to the original
        // "tavily_tavily_search".
        let aliases = AliasMap::new(
            vec![],
            vec![("tavily_".into(), "tavily_*".into(), "".into())],
        )
        .unwrap();

        assert_eq!(
            aliases.apply_reverse("tavily_search"),
            Some("tavily_tavily_search".to_string())
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
        // "_" separator: backend namespace "tavily_" collides with the server's
        // own "tavily_" prefix. The rule must strip only the server prefix and
        // keep the backend namespace, yielding "tavily_search" (not bare "search").
        let mock =
            MockService::with_tools(&["tavily_tavily_search", "tavily_tavily_extract", "db_query"]);

        let aliases = AliasMap::new(
            vec![],
            vec![("tavily_".into(), "tavily_*".into(), "".into())],
        )
        .unwrap();

        let mut svc = AliasService::new(mock, aliases);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                // Should see backend-prefixed, server-prefix-stripped names.
                assert!(
                    names.contains(&"tavily_search"),
                    "Expected tavily_search, got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"tavily_extract"),
                    "Expected tavily_extract, got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"db_query"),
                    "Expected db_query unchanged, got: {:?}",
                    names
                );
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

    /// Test rename_all where backend tools DON'T include their own prefix.
    /// E.g., Roslyn tools are `completion` (not `roslyn_completion`).
    /// After namespace prefix, full name is `roslyn_completion`.
    /// Pattern `roslyn_*` must match the LOCAL name only (`completion`),
    /// which does NOT match, so the backend namespace is preserved unchanged.
    #[tokio::test]
    async fn test_rename_all_backend_without_prefix() {
        let mock = MockService::with_tools(&[
            "roslyn_completion",
            "roslyn_get_call_graph",
            "roslyn_check_syntax",
            "lsp_rename_symbol",
            "lsp_find_references",
            "db_query",
        ]);

        let aliases = AliasMap::new(
            vec![],
            vec![
                ("roslyn_".into(), "roslyn_*".into(), "".into()),
                ("lsp_".into(), "lsp_*".into(), "".into()),
            ],
        )
        .unwrap();

        let mut svc = AliasService::new(mock, aliases);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                // Backend namespace must be preserved (rule does not apply to
                // local names that don't carry the server's own prefix).
                assert!(
                    names.contains(&"roslyn_completion"),
                    "Expected roslyn_completion preserved, got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"roslyn_get_call_graph"),
                    "Expected roslyn_get_call_graph preserved, got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"roslyn_check_syntax"),
                    "Expected roslyn_check_syntax preserved, got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"lsp_rename_symbol"),
                    "Expected lsp_rename_symbol preserved, got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"lsp_find_references"),
                    "Expected lsp_find_references preserved, got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"db_query"),
                    "Expected db_query unchanged, got: {:?}",
                    names
                );
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    /// Test rename_all where backend tools DO include their own prefix.
    /// E.g., Tavily tools are `tavily_search` (already prefixed).
    /// After namespace prefix, full name is `tavily_tavily_search`.
    /// Pattern `tavily_*` matches the LOCAL name `tavily_search`, strips the
    /// server's own `tavily_` prefix, then re-namespaces -> `tavily_search`.
    #[tokio::test]
    async fn test_rename_all_backend_with_prefix() {
        let mock =
            MockService::with_tools(&["tavily_tavily_search", "tavily_tavily_extract", "db_query"]);

        let aliases = AliasMap::new(
            vec![],
            vec![("tavily_".into(), "tavily_*".into(), "".into())],
        )
        .unwrap();

        let mut svc = AliasService::new(mock, aliases);

        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        match resp.inner.unwrap() {
            McpResponse::ListTools(result) => {
                let names: Vec<&str> = result.tools.iter().map(|t| t.name.as_str()).collect();
                assert!(
                    names.contains(&"tavily_search"),
                    "Expected tavily_search (backend prefix preserved), got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"tavily_extract"),
                    "Expected tavily_extract (backend prefix preserved), got: {:?}",
                    names
                );
                assert!(
                    names.contains(&"db_query"),
                    "Expected db_query unchanged, got: {:?}",
                    names
                );
            }
            other => panic!("expected ListTools, got: {:?}", other),
        }
    }

    #[test]
    fn test_rename_all_prefix_glob_strips_only_leading_occurrence() {
        // A trailing-* glob ("prefix_*") must match only at the start of the
        // name, stripping a single leading occurrence of the prefix.
        let aliases = AliasMap::new(
            vec![],
            vec![("tavily/".into(), "tavily_*".into(), "".into())],
        )
        .unwrap();

        // Single prefix occurrence: stripped once.
        assert_eq!(
            aliases.apply_forward("tavily/tavily_search"),
            Some("tavily/search".to_string())
        );
        // Double prefix occurrence: only the leading one is stripped.
        assert_eq!(
            aliases.apply_forward("tavily/tavily_tavily_search"),
            Some("tavily/tavily_search".to_string())
        );
        // No prefix occurrence: not matched.
        assert_eq!(aliases.apply_forward("tavily/search"), None);
    }

    #[test]
    fn test_rename_all_suffix_glob_strips_only_trailing_occurrence() {
        // A leading-* glob ("*_suffix") must match only at the end of the name,
        // stripping a single trailing occurrence of the suffix.
        let aliases =
            AliasMap::new(vec![], vec![("exa/".into(), "*_exa".into(), "".into())]).unwrap();

        // Single suffix occurrence: stripped once.
        assert_eq!(
            aliases.apply_forward("exa/web_search_exa"),
            Some("exa/web_search".to_string())
        );
        // Double suffix occurrence: only the trailing one is stripped.
        assert_eq!(
            aliases.apply_forward("exa/web_search_exa_exa"),
            Some("exa/web_search_exa".to_string())
        );
        // No suffix occurrence: not matched.
        assert_eq!(aliases.apply_forward("exa/web_search"), None);
    }

    #[test]
    fn test_rename_all_suffix_glob_reverse_restores_trailing_occurrence() {
        // Reverse of a suffix glob must re-append the stripped suffix.
        let aliases =
            AliasMap::new(vec![], vec![("exa/".into(), "*_exa".into(), "".into())]).unwrap();

        assert_eq!(
            aliases.apply_reverse("exa/web_search"),
            Some("exa/web_search_exa".to_string())
        );
    }
}
