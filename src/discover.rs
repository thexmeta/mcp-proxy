//! Discover middleware for handling `server/discover` RPC (SEP-2575).
//!
//! This middleware intercepts Discover requests and returns aggregated server
//! capabilities. It's needed for non-HTTP transports (like ChannelTransport in tests)
//! where the HTTP transport's automatic stateless Discover handling doesn't apply.

use std::convert::Infallible;
use std::sync::Arc;

use tower::Layer;
use tower::Service;
use tower_mcp::protocol::{DiscoverParams, DiscoverResult};
use tower_mcp::{
    McpRequest, McpResponse, RouterRequest, RouterResponse, ServerCapabilities, ToolsCapability,
};

use crate::config::ProtocolSupportConfig;

/// Service that handles `server/discover` requests by returning aggregated
/// server capabilities from the proxy configuration.
#[derive(Clone)]
pub struct DiscoverService<S> {
    inner: S,
    protocol_support: Arc<ProtocolSupportConfig>,
    instructions: Arc<Option<String>>,
}

impl<S> DiscoverService<S> {
    pub fn new(
        inner: S,
        protocol_support: &ProtocolSupportConfig,
        instructions: &Option<String>,
    ) -> Self {
        Self {
            inner,
            protocol_support: Arc::new(protocol_support.clone()),
            instructions: Arc::new(instructions.clone()),
        }
    }
}

impl<S> Service<RouterRequest> for DiscoverService<S>
where
    S: Service<RouterRequest, Response = RouterResponse, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    type Response = RouterResponse;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RouterRequest) -> Self::Future {
        // Check if this is a Discover request
        if let McpRequest::Discover(params) = &req.inner {
            let protocol_support = self.protocol_support.clone();
            let instructions = self.instructions.clone();
            let request_id = req.id;
            let params = params.clone();

            return Box::pin(async move {
                let result = build_discover_result(&protocol_support, &instructions, params).await;
                let response = RouterResponse {
                    id: request_id,
                    inner: Ok(McpResponse::Discover(result)),
                };
                Ok(response)
            });
        }

        // Not a Discover request, pass to inner service
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

/// Build the DiscoverResult from proxy configuration.
async fn build_discover_result(
    protocol_support: &ProtocolSupportConfig,
    instructions: &Option<String>,
    _params: DiscoverParams,
) -> DiscoverResult {
    // Get supported protocol versions from config
    let supported_versions = if protocol_support.versions.is_empty() {
        vec!["2026-07-28".to_string(), "2025-11-25".to_string()]
    } else {
        protocol_support.versions.clone()
    };

    // Build capabilities - proxy supports tools at minimum
    let capabilities = ServerCapabilities {
        tools: Some(ToolsCapability::default()),
        ..Default::default()
    };

    DiscoverResult {
        supported_versions,
        capabilities,
        ttl_ms: None,
        cache_scope: None,
        instructions: instructions.clone(),
        meta: None,
    }
}

/// Layer for adding Discover middleware to the stack.
pub struct DiscoverLayer {
    protocol_support: Arc<ProtocolSupportConfig>,
    instructions: Arc<Option<String>>,
}

impl DiscoverLayer {
    pub fn new(config: &crate::config::ProxyConfig) -> Self {
        Self {
            protocol_support: Arc::new(config.proxy.protocol_support.clone()),
            instructions: Arc::new(config.proxy.instructions.clone()),
        }
    }
}

impl<S> Layer<S> for DiscoverLayer {
    type Service = DiscoverService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DiscoverService::new(inner, &self.protocol_support, &self.instructions)
    }
}
