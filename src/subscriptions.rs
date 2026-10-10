//! Middleware to handle `subscriptions/listen` for the 2026-07-28 protocol.
//!
//! The tower-mcp `McpProxy` service does not handle `subscriptions/listen` —
//! it falls through to a catch-all that returns "Method not supported by
//! proxy" (HTTP 400).  When a 2026-07-28 client connects and immediately
//! sends `subscriptions/listen` (to open a notification stream), the proxy
//! kills the connection, which also breaks any in-flight `tools/list` call.
//!
//! This middleware intercepts the request *before* it reaches `McpProxy`
//! and returns an empty `SubscriptionsAccepted` response.  The transport
//! then opens the SSE stream, but because the accepted filter is empty,
//! no notifications are actually delivered — which is the correct
//! behaviour for a proxy that does not support push notifications.

use std::convert::Infallible;

use tower::Layer;
use tower::Service;
use tower_mcp::protocol::{SubscriptionFilter, SubscriptionsAcceptedResult};
use tower_mcp::{McpRequest, McpResponse, RouterRequest, RouterResponse};

/// Tower layer that installs [`SubscriptionsService`].
#[derive(Clone, Default)]
pub struct SubscriptionsListenLayer;

impl<S> Layer<S> for SubscriptionsListenLayer {
    type Service = SubscriptionsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SubscriptionsService { inner }
    }
}

/// Intercepts `subscriptions/listen` and returns an empty accepted filter.
///
/// Forwards everything else unchanged to the inner service.
#[derive(Clone)]
pub struct SubscriptionsService<S> {
    inner: S,
}

impl<S> Service<RouterRequest> for SubscriptionsService<S>
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
        if matches!(&req.inner, McpRequest::SubscriptionsListen(_)) {
            let request_id = req.id;
            tracing::debug!(
                id = ?request_id,
                "Intercepted subscriptions/listen — returning empty accepted filter"
            );
            return Box::pin(async move {
                Ok(RouterResponse {
                    id: request_id,
                    inner: Ok(McpResponse::SubscriptionsAccepted(
                        SubscriptionsAcceptedResult {
                            notifications: SubscriptionFilter::default(),
                        },
                    )),
                })
            });
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{MockService, call_service};
    #[tokio::test]
    async fn subscriptions_listen_returns_empty_accepted() {
        let mut svc = SubscriptionsService {
            inner: MockService::with_tools(&["dummy_tool"]),
        };

        let req = McpRequest::SubscriptionsListen(Default::default());
        let resp = call_service(&mut svc, req).await;
        match &resp.inner {
            Ok(McpResponse::SubscriptionsAccepted(result)) => {
                // Empty filter — no notification types accepted
                assert!(result.notifications.tools_list_changed.is_none());
                assert!(result.notifications.prompts_list_changed.is_none());
                assert!(result.notifications.resources_list_changed.is_none());
                assert!(result.notifications.resource_subscriptions.is_none());
                assert!(result.notifications.task_ids.is_none());
            }
            other => panic!("Expected SubscriptionsAccepted, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn other_requests_pass_through() {
        let mut svc = SubscriptionsService {
            inner: MockService::with_tools(&["my_tool"]),
        };

        // ListTools should pass through to the inner mock
        let req = McpRequest::ListTools(Default::default());
        let resp = call_service(&mut svc, req).await;
        match &resp.inner {
            Ok(McpResponse::ListTools(result)) => {
                assert_eq!(result.tools.len(), 1);
                assert_eq!(result.tools[0].name, "my_tool");
            }
            other => panic!("Expected ListTools, got {:?}", other),
        }
    }
}
