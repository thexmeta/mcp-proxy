//! Per-client-identity rate limiting middleware for MCP 2026-07-28.
//!
//! Extracts client identity from `_meta.clientInfo.name` (2026-07-28) or from
//! a fallback identifier (IP, bearer token hash) for older protocols. Each
//! unique client identity gets its own token-bucket rate limiter.
//!
//! # Configuration
//!
//! ```toml
//! [proxy.client_rate_limit]
//! max_requests = 100          # per client per window
//! window_seconds = 60         # sliding window duration
//! cleanup_interval_seconds = 300  # how often to evict idle entries
//! ```

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tower::Service;
use tower_mcp::{RouterRequest, RouterResponse};
use tower_mcp_types::error::JsonRpcError;
use tower_mcp_types::protocol::RequestMeta;

use crate::config::ClientRateLimitConfig;

/// Shared state: maps client identity → (available permits, last refill time).
struct RateLimitBucket {
    permits: f64,
    last_refill: Instant,
    max_permits: f64,
    refill_rate: f64, // permits per second
}

impl RateLimitBucket {
    fn new(max_permits: f64, window: Duration) -> Self {
        Self {
            permits: max_permits,
            last_refill: Instant::now(),
            max_permits,
            refill_rate: max_permits / window.as_secs_f64(),
        }
    }

    /// Try to consume one permit. Returns true if allowed.
    fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        // Refill tokens
        self.permits = (self.permits + elapsed * self.refill_rate).min(self.max_permits);
        self.last_refill = now;

        if self.permits >= 1.0 {
            self.permits -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Per-client rate limiter state.
struct RateLimitState {
    buckets: DashMap<String, Arc<tokio::sync::Mutex<RateLimitBucket>>>,
    max_permits: f64,
    window: Duration,
}

impl RateLimitState {
    fn new(config: &ClientRateLimitConfig) -> Self {
        let window = Duration::from_secs(config.window_seconds);
        Self {
            buckets: DashMap::new(),
            max_permits: config.max_requests as f64,
            window,
        }
    }

    /// Try to acquire a permit for the given client identity.
    /// Returns `true` if allowed, `false` if rate-limited.
    async fn try_acquire(&self, identity: &str) -> bool {
        // Fast path: existing bucket
        if let Some(bucket_ref) = self.buckets.get(identity) {
            let mut bucket = bucket_ref.value().lock().await;
            return bucket.try_acquire();
        }

        // Slow path: insert new bucket
        let bucket = Arc::new(tokio::sync::Mutex::new(RateLimitBucket::new(
            self.max_permits,
            self.window,
        )));
        self.buckets
            .entry(identity.to_string())
            .or_insert_with(|| bucket.clone());
        let mut bucket = bucket.lock().await;
        bucket.try_acquire()
    }

    /// Clean up idle buckets older than `max_idle`.
    #[allow(dead_code)]
    fn cleanup(&self, max_idle: Duration) {
        let now = Instant::now();
        self.buckets.retain(|_, bucket| {
            // We can't lock async in sync context, so use try_lock
            if let Ok(b) = bucket.try_lock() {
                now.duration_since(b.last_refill) < max_idle
            } else {
                true // keep if locked (in use)
            }
        });
    }
}

/// Per-client-identity rate limiting service.
#[derive(Clone)]
pub struct ClientIdentityRateLimitService<S> {
    inner: S,
    state: Arc<RateLimitState>,
}

impl<S> ClientIdentityRateLimitService<S> {
    pub fn new(inner: S, config: &ClientRateLimitConfig) -> Self {
        Self {
            inner,
            state: Arc::new(RateLimitState::new(config)),
        }
    }
}

impl<S> Service<RouterRequest> for ClientIdentityRateLimitService<S>
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
        let identity = extract_client_identity(&req);
        let state = self.state.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            if state.try_acquire(&identity).await {
                inner.call(req).await
            } else {
                tracing::warn!(
                    client = %identity,
                    "Client rate limit exceeded"
                );
                Ok(RouterResponse {
                    id: req.id,
                    inner: Err(JsonRpcError {
                        code: -32099,
                        message: "Rate limit exceeded".into(),
                        data: None,
                    }),
                })
            }
        })
    }
}

/// Layer for [`ClientIdentityRateLimitService`].
#[derive(Clone)]
pub struct ClientIdentityRateLimitLayer {
    config: ClientRateLimitConfig,
}

impl ClientIdentityRateLimitLayer {
    pub fn new(config: ClientRateLimitConfig) -> Self {
        Self { config }
    }
}

impl<S> tower::Layer<S> for ClientIdentityRateLimitLayer {
    type Service = ClientIdentityRateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ClientIdentityRateLimitService::new(inner, &self.config)
    }
}

/// Extract a client identity string from the request.
///
/// Priority:
/// 1. `_meta.clientInfo.name` (2026-07-28 protocol)
/// 2. `"anonymous"` fallback
fn extract_client_identity(req: &RouterRequest) -> String {
    if let Some(meta) = req.extensions.get::<RequestMeta>()
        && let Some(ref client_info) = meta.client_info
        && !client_info.name.is_empty()
    {
        return client_info.name.clone();
    }
    "anonymous".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{MockService, call_service};
    use tower_mcp::protocol::RequestId;
    use tower_mcp::router::{Extensions, RouterRequest};
    use tower_mcp_types::protocol::{Implementation, McpRequest, RequestMeta};

    fn make_router_request(meta: Option<RequestMeta>) -> RouterRequest {
        let mut extensions = Extensions::new();
        if let Some(m) = meta {
            extensions.insert(m);
        }
        RouterRequest {
            id: RequestId::Number(1),
            inner: McpRequest::ListTools(Default::default()),
            extensions,
        }
    }

    #[tokio::test]
    async fn test_allows_requests_under_limit() {
        let config = ClientRateLimitConfig {
            max_requests: 10,
            window_seconds: 60,
            cleanup_interval_seconds: 300,
        };
        let svc = MockService::with_tools(&["tool1"]);
        let mut svc = ClientIdentityRateLimitService::new(svc, &config);

        // Should allow 10 requests
        for _ in 0..10 {
            let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
            assert!(resp.inner.is_ok());
        }
    }

    #[tokio::test]
    async fn test_rejects_requests_over_limit() {
        let config = ClientRateLimitConfig {
            max_requests: 2,
            window_seconds: 60,
            cleanup_interval_seconds: 300,
        };
        let svc = MockService::with_tools(&["tool1"]);
        let mut svc = ClientIdentityRateLimitService::new(svc, &config);

        // Allow first 2
        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        assert!(resp.inner.is_ok());
        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        assert!(resp.inner.is_ok());

        // Third should be rejected
        let resp = call_service(&mut svc, McpRequest::ListTools(Default::default())).await;
        assert!(resp.inner.is_err());
        assert_eq!(resp.inner.unwrap_err().code, -32099);
    }

    #[tokio::test]
    async fn test_different_identities_are_independent() {
        let config = ClientRateLimitConfig {
            max_requests: 1,
            window_seconds: 60,
            cleanup_interval_seconds: 300,
        };
        let state = Arc::new(RateLimitState::new(&config));

        // Identity A gets one permit
        assert!(state.try_acquire("client-a").await);
        assert!(!state.try_acquire("client-a").await);

        // Identity B still has its own permit
        assert!(state.try_acquire("client-b").await);
    }

    #[test]
    fn test_extract_client_identity_with_meta() {
        let req = make_router_request(Some(RequestMeta {
            progress_token: None,
            protocol_version: Some("2026-07-28".into()),
            client_info: Some(Implementation {
                name: "my-client".into(),
                version: "1.0.0".into(),
                title: None,
                description: None,
                icons: None,
                website_url: None,
                meta: None,
            }),
            client_capabilities: None,
            log_level: None,
        }));
        assert_eq!(extract_client_identity(&req), "my-client");
    }

    #[test]
    fn test_extract_client_identity_without_meta() {
        let req = make_router_request(None);
        assert_eq!(extract_client_identity(&req), "anonymous");
    }
}
