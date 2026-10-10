use tower_mcp::McpRouter;
use tower_mcp::client::ChannelTransport;
use tower_mcp::proxy::McpProxy;

#[tokio::test]
async fn test_proxy_builder_methods() {
    let router = McpRouter::new().server_info("test", "1.0");
    let _proxy = McpProxy::builder("test-proxy", "1.0.0")
        .separator("/")
        // Try to see what methods are available
        .instructions("Test instructions") // Try instructions
        .backend("test", ChannelTransport::new(router))
        .await
        .build_strict()
        .await
        .expect("proxy should build");
}
