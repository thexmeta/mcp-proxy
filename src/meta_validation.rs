//! Per-request _meta validation middleware for MCP 2026-07-28 protocol (SEP-2243).
//!
//! This middleware validates that incoming requests for the 2026-07-28 protocol
//! include the required _meta fields: protocol_version, client_info, and client_capabilities.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tower::Service;

use tower_mcp::{RouterRequest, RouterResponse};
use tower_mcp_types::error::JsonRpcError;
use tower_mcp_types::protocol::RequestMeta;

/// Middleware that validates per-request _meta for MCP 2026-07-28 requests.
#[derive(Clone)]
pub struct MetaValidationService<S> {
    inner: S,
}

impl<S> MetaValidationService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<RouterRequest> for MetaValidationService<S>
where
    S: Service<RouterRequest, Response = RouterResponse, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = RouterResponse;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RouterRequest) -> Self::Future {
        // Check if this is a 2026-07-28 request by looking at the _meta
        let has_meta = req.extensions.get::<RequestMeta>().is_some();

        // For 2026-07-28, _meta is required. For older protocols, it's optional.
        // We can detect 2026-07-28 by checking if _meta exists and has protocol_version
        let is_2026 = has_meta
            && req
                .extensions
                .get::<RequestMeta>()
                .map(|m| m.protocol_version.as_deref() == Some("2026-07-28"))
                .unwrap_or(false);

        if is_2026 {
            // Validate required fields for 2026-07-28
            if let Some(meta) = req.extensions.get::<RequestMeta>() {
                if meta.protocol_version.is_none() {
                    return Box::pin(async {
                        Ok(RouterResponse {
                            id: req.id,
                            inner: Err(JsonRpcError::invalid_params(
                                "Missing required _meta.protocol_version for 2026-07-28",
                            )),
                        })
                    });
                }
                if meta.client_info.is_none() {
                    return Box::pin(async {
                        Ok(RouterResponse {
                            id: req.id,
                            inner: Err(JsonRpcError::invalid_params(
                                "Missing required _meta.client_info for 2026-07-28",
                            )),
                        })
                    });
                }
                if meta.client_capabilities.is_none() {
                    return Box::pin(async {
                        Ok(RouterResponse {
                            id: req.id,
                            inner: Err(JsonRpcError::invalid_params(
                                "Missing required _meta.client_capabilities for 2026-07-28",
                            )),
                        })
                    });
                }
            }
        }

        // Pass through to inner service
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

/// Layer for MetaValidationService.
#[derive(Clone)]
pub struct MetaValidationLayer;

impl MetaValidationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> tower::Layer<S> for MetaValidationLayer {
    type Service = MetaValidationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetaValidationService::new(inner)
    }
}

impl Default for MetaValidationLayer {
    fn default() -> Self {
        Self::new()
    }
}
