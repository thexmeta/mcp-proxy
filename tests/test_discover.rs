use tower_mcp::protocol::McpResponse;

#[tokio::test]
async fn test_discover_response_type_exists() {
    // Check that DiscoverResult exists in McpResponse
    let _resp: McpResponse = McpResponse::Discover(tower_mcp::protocol::DiscoverResult {
        ttl_ms: Some(1000),
        cache_scope: None,
        capabilities: tower_mcp::protocol::ServerCapabilities::default(),
        instructions: None,
        meta: None,
        supported_versions: vec!["2026-07-28".to_string()],
    });
    println!("Discover response works!");
}
