//! Model-Redirected Tool Results (MRTR) — proxy-side `ClientHandler`.
//!
//! When a backend MCP server calls `ctx.sample()` or `ctx.elicit_form()`,
//! the request arrives at the proxy's [`McpClient`] as a
//! `sampling/createMessage` or `elicitation/create` JSON-RPC request.
//! This handler receives those requests and relays them to the real client
//! via a [`ClientRequesterHandle`].
//!
//! # Architecture
//!
//! ```text
//! Backend (McpClient)                    Proxy                     Real Client
//!     │                                   │                           │
//!     │── ctx.sample() ──────────────────►│                           │
//!     │   sampling/createMessage           │                           │
//!     │                                   │── sampling/createMessage ─►│
//!     │                                   │◄── CreateMessageResult ───│
//!     │◄── CreateMessageResult ───────────│                           │
//! ```
//!
//! # Limitations
//!
//! Full relay requires correlating backend-initiated requests with the
//! originating real-client connection. This is complex in multi-client
//! deployments because:
//!
//! 1. The backend's `sampling/createMessage` has no reference to the
//!    original tool-call request that triggered it.
//! 2. Multiple real clients may be connected simultaneously.
//! 3. The [`ClientRequesterHandle`] is per-request on the server side,
//!    not a persistent per-client handle.
//!
//! For now, the handler advertises capabilities (so backends know
//! sampling/elicitation are theoretically supported) but returns a
//! clear error if the backend actually tries to use them. A future
//! implementation can add full relay with request correlation.
//!
//! [`McpClient`]: tower_mcp::client::McpClient
//! [`ClientRequesterHandle`]: tower_mcp::context::ClientRequesterHandle

use async_trait::async_trait;
use tower_mcp::client::{ClientHandler, NotificationHandler, ServerNotification};
use tower_mcp::protocol::{
    CreateMessageParams, CreateMessageResult, ElicitRequestParams, ElicitResult, ListRootsResult,
};
use tower_mcp_types::JsonRpcError;

/// A [`ClientHandler`] for the proxy that combines notification forwarding
/// with sampling/elicitation relay.
///
/// This wraps the standard [`NotificationHandler`] (which handles
/// `notifications/tools/list_changed` etc.) and adds stubs for
/// `sampling/createMessage` and `elicitation/create`.
///
/// When a backend calls `ctx.sample()` or `ctx.elicit_form()`, the
/// proxy returns a clear "not supported through proxy" error rather
/// than silently ignoring the request.
///
/// # Future Work
///
/// To enable full relay, store a [`ClientRequesterHandle`] in this
/// struct and forward requests through it. The handle must be set
/// after the real client connects and must be scoped to the specific
/// backend connection that triggered the request.
pub struct ProxyClientHandler {
    inner: NotificationHandler,
}

impl ProxyClientHandler {
    /// Create a new handler wrapping the given [`NotificationHandler`].
    pub fn new(inner: NotificationHandler) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl ClientHandler for ProxyClientHandler {
    /// Handle a `sampling/createMessage` request from a backend.
    ///
    /// Returns an error indicating sampling is not supported through the proxy.
    /// To enable sampling relay, forward `params` to the real client's
    /// `ClientRequesterHandle` and return the response.
    async fn handle_create_message(
        &self,
        _params: CreateMessageParams,
    ) -> Result<CreateMessageResult, JsonRpcError> {
        tracing::debug!(
            "Backend requested sampling/createMessage — \
             not relayed through proxy (MRTR not yet implemented)"
        );
        Err(JsonRpcError::internal_error(
            "sampling/createMessage is not supported through this proxy. \
             The backend must handle sampling locally or the proxy must be \
             configured with an LLM endpoint for MRTR relay.",
        ))
    }

    /// Handle an `elicitation/create` request from a backend.
    ///
    /// Returns an error indicating elicitation is not supported through the proxy.
    async fn handle_elicit(
        &self,
        _params: ElicitRequestParams,
    ) -> Result<ElicitResult, JsonRpcError> {
        tracing::debug!(
            "Backend requested elicitation/create — \
             not relayed through proxy (MRTR not yet implemented)"
        );
        Err(JsonRpcError::internal_error(
            "elicitation/create is not supported through this proxy. \
             The backend must handle elicitation locally or the proxy must be \
             configured with a frontend for MRTR relay.",
        ))
    }

    /// Delegate `roots/list` to the inner notification handler.
    async fn handle_list_roots(&self) -> Result<ListRootsResult, JsonRpcError> {
        self.inner.handle_list_roots().await
    }

    /// Delegate notifications to the inner notification handler.
    async fn on_notification(&self, notification: ServerNotification) {
        self.inner.on_notification(notification).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_mcp::client::NotificationHandler;
    use tower_mcp::protocol::{ElicitFormParams, ElicitRequestParams, SamplingMessage};

    fn make_handler() -> ProxyClientHandler {
        let inner = NotificationHandler::new();
        ProxyClientHandler::new(inner)
    }

    #[tokio::test]
    async fn test_create_message_returns_error() {
        let handler = make_handler();
        let params = CreateMessageParams {
            messages: vec![SamplingMessage::user("hello")],
            max_tokens: 100,
            system_prompt: None,
            temperature: None,
            stop_sequences: Vec::new(),
            model_preferences: None,
            include_context: None,
            metadata: None,
            tools: None,
            tool_choice: None,
            task: None,
            meta: None,
        };
        let result = handler.handle_create_message(params).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, -32603);
    }

    #[tokio::test]
    async fn test_elicit_returns_error() {
        use tower_mcp::protocol::ElicitFormSchema;
        let handler = make_handler();
        let params = ElicitRequestParams::Form(ElicitFormParams {
            mode: None,
            message: "What is your name?".into(),
            requested_schema: ElicitFormSchema {
                schema_type: "object".into(),
                properties: indexmap::IndexMap::new(),
                required: vec![],
            },
            meta: None,
        });
        let result = handler.handle_elicit(params).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, -32603);
    }
}
