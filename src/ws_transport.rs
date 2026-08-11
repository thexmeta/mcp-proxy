//! WebSocket client transport for connecting to WebSocket-based MCP backends.
//!
//! Implements tower-mcp's [`ClientTransport`](tower_mcp::client::ClientTransport) trait over a WebSocket connection
//! using `tokio-tungstenite`. Messages are sent and received as text frames
//! containing JSON-RPC payloads.
//!
//! # Example
//!
//! ```rust,no_run
//! use mcp_proxy::ws_transport::WebSocketClientTransport;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let transport = WebSocketClientTransport::connect("ws://localhost:8080/ws").await?;
//! // Pass to McpProxy::builder().backend("name", transport).await
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A WebSocket client transport for MCP backend connections.
///
/// Connects to a WebSocket endpoint and exchanges JSON-RPC messages
/// as text frames. Supports `ws://` and `wss://` (TLS) URLs.
///
/// For MCP 2026-07-28 stateless protocol, the transport supports
/// protocol version negotiation via the `Sec-WebSocket-Protocol` header
/// using the `mcp.version.{version}` subprotocol token.
pub struct WebSocketClientTransport {
    sink: Arc<Mutex<futures_util::stream::SplitSink<WsStream, Message>>>,
    stream: Arc<Mutex<futures_util::stream::SplitStream<WsStream>>>,
    connected: Arc<AtomicBool>,
    /// Negotiated protocol version from server response (if any)
    negotiated_version: Arc<Mutex<Option<String>>>,
}

impl WebSocketClientTransport {
    /// Connect to a WebSocket endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket handshake fails or the URL is invalid.
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        Self::connect_with_protocol_version(url, None).await
    }

    /// Connect to a WebSocket endpoint with a specific protocol version for MCP 2026-07-28 stateless protocol.
    ///
    /// The protocol version is sent in the `Sec-WebSocket-Protocol` header using the
    /// `mcp.version.{version}` subprotocol token. For example, `mcp.version.2026-07-28`.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket handshake fails or the URL is invalid.
    pub async fn connect_with_protocol_version(
        url: &str,
        version: Option<&str>,
    ) -> anyhow::Result<Self> {
        use tokio_tungstenite::tungstenite::http::Request;

        let mut request = Request::builder()
            .uri(url)
            .header("Host", host_header_from_url(url)?)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            );

        if let Some(v) = version {
            request = request.header("Sec-WebSocket-Protocol", format!("mcp.version.{v}"));
        }

        let request = request
            .body(())
            .map_err(|e| anyhow::anyhow!("invalid WebSocket request: {e}"))?;

        let (ws_stream, response) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| anyhow::anyhow!("WebSocket connection failed: {e}"))?;

        // Extract negotiated protocol version from server response
        let negotiated_version = response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("mcp.version."))
            .map(|s| s.to_string());

        let (sink, stream) = ws_stream.split();

        Ok(Self {
            sink: Arc::new(Mutex::new(sink)),
            stream: Arc::new(Mutex::new(stream)),
            connected: Arc::new(AtomicBool::new(true)),
            negotiated_version: Arc::new(Mutex::new(negotiated_version)),
        })
    }

    /// Get the negotiated protocol version from the server, if any.
    ///
    /// Returns the protocol version string (e.g. `"2026-07-28"`) if the server
    /// responded with a `Sec-WebSocket-Protocol: mcp.version.{version}` header
    /// during the WebSocket handshake. Returns `None` if no version was negotiated.
    pub async fn negotiated_version(&self) -> Option<String> {
        self.negotiated_version.lock().await.clone()
    }

    /// Connect to a WebSocket endpoint with a bearer token for authentication.
    ///
    /// The token is sent in the `Authorization` header during the handshake.
    /// Optionally, a protocol version can be specified for MCP 2026-07-28 stateless protocol
    /// via the `Sec-WebSocket-Protocol` header using the `mcp.version.{version}` subprotocol token.
    pub async fn connect_with_bearer_token(
        url: &str,
        token: &str,
        protocol_version: Option<&str>,
    ) -> anyhow::Result<Self> {
        use tokio_tungstenite::tungstenite::http::Request;

        let mut request = Request::builder()
            .uri(url)
            .header("Host", host_header_from_url(url)?)
            .header("Authorization", format!("Bearer {token}"))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            );

        if let Some(v) = protocol_version {
            request = request.header("Sec-WebSocket-Protocol", format!("mcp.version.{v}"));
        }

        let request = request
            .body(())
            .map_err(|e| anyhow::anyhow!("invalid WebSocket request: {e}"))?;

        let (ws_stream, response) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| anyhow::anyhow!("WebSocket connection failed: {e}"))?;

        // Extract negotiated protocol version from server response
        let negotiated_version = response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("mcp.version."))
            .map(|s| s.to_string());

        let (sink, stream) = ws_stream.split();

        Ok(Self {
            sink: Arc::new(Mutex::new(sink)),
            stream: Arc::new(Mutex::new(stream)),
            connected: Arc::new(AtomicBool::new(true)),
            negotiated_version: Arc::new(Mutex::new(negotiated_version)),
        })
    }
}

#[async_trait]
impl tower_mcp::client::ClientTransport for WebSocketClientTransport {
    async fn send(&mut self, message: &str) -> tower_mcp::error::Result<()> {
        let mut sink = self.sink.lock().await;
        sink.send(Message::Text(message.into()))
            .await
            .map_err(|e| tower_mcp::error::Error::Transport(e.to_string()))?;
        Ok(())
    }

    async fn recv(&mut self) -> tower_mcp::error::Result<Option<String>> {
        let mut stream = self.stream.lock().await;
        loop {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => return Ok(Some(text.as_str().to_owned())),
                Some(Ok(Message::Close(_))) | None => {
                    self.connected.store(false, Ordering::SeqCst);
                    return Ok(None);
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                    // Pong is handled automatically by tungstenite; skip control frames
                    continue;
                }
                Some(Ok(Message::Binary(data))) => {
                    // Try to interpret binary as UTF-8 text
                    let text = std::str::from_utf8(&data)
                        .map_err(|e| tower_mcp::error::Error::Transport(e.to_string()))?;
                    return Ok(Some(text.to_string()));
                }
                Some(Err(e)) => {
                    self.connected.store(false, Ordering::SeqCst);
                    return Err(tower_mcp::error::Error::Transport(e.to_string()));
                }
            }
        }
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    async fn close(&mut self) -> tower_mcp::error::Result<()> {
        self.connected.store(false, Ordering::SeqCst);
        let mut sink = self.sink.lock().await;
        let _ = sink.send(Message::Close(None)).await;
        Ok(())
    }
}

/// Extract the `Host` header value from a URL string per RFC 6455 §4.1.
///
/// Returns `host:port` when the port is non-standard for the scheme, or just
/// `host` when the port is the default (80 for `ws`/`http`, 443 for `wss`/`https`)
/// or absent.
fn host_header_from_url(url: &str) -> anyhow::Result<String> {
    use tokio_tungstenite::tungstenite::http::Uri;

    let uri: Uri = url
        .parse()
        .map_err(|e| anyhow::anyhow!("failed to parse URL for Host header: {e}"))?;

    let host = uri
        .host()
        .ok_or_else(|| anyhow::anyhow!("URL has no host component: {url}"))?
        .to_string();

    match uri.port_u16() {
        Some(port) => {
            let default_port = match uri.scheme_str() {
                Some("wss" | "https") => 443,
                _ => 80,
            };
            if port == default_port {
                Ok(host)
            } else {
                Ok(format!("{host}:{port}"))
            }
        }
        None => Ok(host),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_fails_with_invalid_url() {
        let result = WebSocketClientTransport::connect("ws://127.0.0.1:1").await;
        let err = result.err().expect("should fail").to_string();
        assert!(
            err.contains("WebSocket connection failed"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn connect_with_bearer_token_fails_with_invalid_url() {
        let result =
            WebSocketClientTransport::connect_with_bearer_token("ws://127.0.0.1:1", "tok", None)
                .await;
        let err = result.err().expect("should fail").to_string();
        assert!(
            err.contains("WebSocket connection failed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn host_header_extracts_host_and_non_default_port() {
        assert_eq!(
            host_header_from_url("ws://localhost:8080/ws").unwrap(),
            "localhost:8080"
        );
    }

    #[test]
    fn host_header_omits_default_ws_port() {
        assert_eq!(
            host_header_from_url("ws://localhost:80/ws").unwrap(),
            "localhost"
        );
    }

    #[test]
    fn host_header_omits_default_wss_port() {
        assert_eq!(
            host_header_from_url("wss://example.com:443/ws").unwrap(),
            "example.com"
        );
    }

    #[test]
    fn host_header_includes_non_default_wss_port() {
        assert_eq!(
            host_header_from_url("wss://example.com:8443/ws").unwrap(),
            "example.com:8443"
        );
    }

    #[test]
    fn host_header_without_port() {
        assert_eq!(
            host_header_from_url("ws://localhost/ws").unwrap(),
            "localhost"
        );
    }

    #[test]
    fn host_header_with_credentials_stripped() {
        assert_eq!(
            host_header_from_url("ws://user:pass@localhost:9090/ws").unwrap(),
            "localhost:9090"
        );
    }
}
