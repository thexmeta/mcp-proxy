//! Axum middleware for backward-compatible MCP header injection.
//!
//! Many clients claim 2026-07-28 protocol but don't send the required HTTP
//! headers (`Mcp-Method` per SEP-2243, `MCP-Protocol-Version` per SEP-2243).
//! This middleware extracts both from the JSON-RPC body and injects them as
//! HTTP headers if the client omitted them.

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
/// Header name for MCP protocol version.
const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

/// Middleware that injects `Mcp-Method` and `MCP-Protocol-Version` headers
/// from the JSON-RPC body if the client didn't send them.
pub async fn inject_mcp_compat_headers(req: Request, next: Next) -> Response {
    let has_method = req.headers().contains_key(MCP_METHOD_HEADER);
    let has_version = req.headers().contains_key(MCP_PROTOCOL_VERSION_HEADER);

    // Skip if both headers already present
    if has_method && has_version {
        return next.run(req).await;
    }

    // Only for POST requests (MCP JSON-RPC endpoint)
    if req.method() != Method::POST {
        return next.run(req).await;
    }

    // Buffer the body to extract fields
    let (parts, body) = req.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            let req = Request::from_parts(parts, Body::default());
            return next.run(req).await;
        }
    };

    let json: Option<Value> = serde_json::from_slice(&bytes).ok();

    // Extract method and protocol version from JSON-RPC body
    let mcp_method = json.as_ref()
        .and_then(|v| v.get("method")?.as_str().map(String::from));

    let protocol_version = json.as_ref()
        .and_then(|v| v.get("params")?.get("protocolVersion")?.as_str().map(String::from));

    // Reconstruct request with injected headers
    let mut req = Request::from_parts(parts, Body::from(bytes));

    if !has_method
        && let Some(method) = mcp_method
    {
        tracing::debug!(method = %method, "Injecting missing Mcp-Method header");
        req.headers_mut().insert(
            HeaderName::from_static(MCP_METHOD_HEADER),
            method.parse().unwrap_or_else(|_| HeaderValue::from_static("unknown")),
        );
    }

    if !has_version
        && let Some(version) = protocol_version
    {
        tracing::debug!(version = %version, "Injecting missing MCP-Protocol-Version header");
        req.headers_mut().insert(
            HeaderName::from_static(MCP_PROTOCOL_VERSION_HEADER),
            version.parse().unwrap_or_else(|_| HeaderValue::from_static("unknown")),
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
    async fn test_injects_method_header_when_missing() {
        let app = Router::new()
            .route("/", axum::routing::post(|req: Request| async move {
                let has_header = req.headers().contains_key(MCP_METHOD_HEADER);
                let method = req.headers()
                    .get(MCP_METHOD_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                (StatusCode::OK, format!("has_header={}, method={:?}", has_header, method))
            }))
            .layer(axum::middleware::from_fn(inject_mcp_compat_headers));

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28"}}"#;
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
    async fn test_injects_protocol_version_header() {
        let app = Router::new()
            .route("/", axum::routing::post(|req: Request| async move {
                let version = req.headers()
                    .get(MCP_PROTOCOL_VERSION_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                (StatusCode::OK, format!("version={:?}", version))
            }))
            .layer(axum::middleware::from_fn(inject_mcp_compat_headers));

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28"}}"#;
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "version=Some(\"2026-07-28\")");
    }

    #[tokio::test]
    async fn test_preserves_existing_headers() {
        let app = Router::new()
            .route("/", axum::routing::post(|req: Request| async move {
                let method = req.headers()
                    .get(MCP_METHOD_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                let version = req.headers()
                    .get(MCP_PROTOCOL_VERSION_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                (StatusCode::OK, format!("method={:?}, version={:?}", method, version))
            }))
            .layer(axum::middleware::from_fn(inject_mcp_compat_headers));

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28"}}"#;
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .header(MCP_METHOD_HEADER, "custom-method")
            .header(MCP_PROTOCOL_VERSION_HEADER, "custom-version")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "method=Some(\"custom-method\"), version=Some(\"custom-version\")");
    }

    #[tokio::test]
    async fn test_skips_non_post_requests() {
        let app = Router::new()
            .route("/", axum::routing::get(|| async {
                (StatusCode::OK, "no header injected")
            }))
            .layer(axum::middleware::from_fn(inject_mcp_compat_headers));

        let req = axum::http::Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::default())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
