//! Axum middleware to auto-inject the `Mcp-Method` header from the JSON-RPC body.
//!
//! This enables backward compatibility with clients that don't send the header
//! (e.g., antigravity, older MCP clients). tower-mcp requires `Mcp-Method` for
//! 2026-07-28 protocol (SEP-2243), but many clients claim 2026-07-28 without
//! implementing the full spec.

use axum::{
    body::Body,
    extract::Request,
    http::header::HeaderName,
    http::HeaderValue,
    http::Method,
    middleware::Next,
    response::Response,
};
use http_body_util::BodyExt;
use serde_json::Value;

/// Header name for MCP method (SEP-2243).
const MCP_METHOD_HEADER: &str = "mcp-method";

/// Middleware that injects the `Mcp-Method` header from the JSON-RPC body
/// if the client didn't send it.
///
/// This is a lenient fallback for clients that claim 2026-07-28 but don't
/// implement the full SEP-2243 spec.
pub async fn inject_mcp_method_header(req: Request, next: Next) -> Response {
    // Skip if header already present
    if req.headers().contains_key(MCP_METHOD_HEADER) {
        return next.run(req).await;
    }

    // Only for POST requests to the root path (MCP JSON-RPC endpoint)
    if req.method() != Method::POST {
        return next.run(req).await;
    }

    // Buffer the body to extract the method field
    let (parts, body) = req.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            // If we can't read body, just pass through
            let req = Request::from_parts(parts, Body::default());
            return next.run(req).await;
        }
    };

    // Try to parse JSON-RPC body and extract method
    let mcp_method: Option<String> = serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|v| v.get("method")?.as_str().map(String::from));

    // Reconstruct request with injected header if method found
    let mut req = Request::from_parts(parts, Body::from(bytes));
    if let Some(method) = mcp_method {
        tracing::debug!(method = %method, "Injecting missing Mcp-Method header");
        req.headers_mut().insert(
            HeaderName::from_static(MCP_METHOD_HEADER),
            method.parse().unwrap_or_else(|_| {
                HeaderValue::from_static("unknown")
            }),
        );
    }

    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode, Router};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_injects_header_when_missing() {
        let app = Router::new()
            .route("/", axum::routing::post(|req: Request| async move {
                let has_header = req.headers().contains_key(MCP_METHOD_HEADER);
                let method = req.headers()
                    .get(MCP_METHOD_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                (StatusCode::OK, format!("has_header={}, method={:?}", has_header, method))
            }))
            .layer(axum::middleware::from_fn(inject_mcp_method_header));

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "has_header=true, method=Some(\"initialize\")");
    }

    #[tokio::test]
    async fn test_preserves_existing_header() {
        let app = Router::new()
            .route("/", axum::routing::post(|req: Request| async move {
                let method = req.headers()
                    .get(MCP_METHOD_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                (StatusCode::OK, format!("method={:?}", method))
            }))
            .layer(axum::middleware::from_fn(inject_mcp_method_header));

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .header(MCP_METHOD_HEADER, "custom-value")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "method=Some(\"custom-value\")");
    }

    #[tokio::test]
    async fn test_skips_non_post_requests() {
        let app = Router::new()
            .route("/", axum::routing::get(|| async {
                (StatusCode::OK, "no header injected")
            }))
            .layer(axum::middleware::from_fn(inject_mcp_method_header));

        let req = axum::http::Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::default())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
