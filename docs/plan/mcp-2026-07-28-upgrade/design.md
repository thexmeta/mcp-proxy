# Design: MCP 2026-07-28 Spec Upgrade

## Architecture Overview

The upgrade involves two parallel tracks:
1. **Dependency Upgrade**: tower-mcp 0.12.0 → 0.20.1 with `protocol-2026-07-28` feature
2. **Code Migration**: Adapt mcp-proxy to new tower-mcp APIs and 2026-07-28 protocol changes

## Component Changes

### 1. Cargo.toml Changes

```toml
# Core dependency updates
tower-mcp = { version = "0.20", features = ["http", "http-client", "proxy", "websocket", "protocol-2026-07-28"] }
tower-mcp-types = "0.20"

# MSRV update
rust-version = "1.97"

# Remove local patch (verify upstream first)
# [patch.crates-io]
# tower-mcp = { path = "patches/tower-mcp/tower-mcp-0.12.0" }
```

### 2. Protocol Support Configuration

In `src/proxy.rs` and `src/config.rs`:
- Add `ProtocolSupport` configuration to enable 2026-07-28 at runtime
- Default to 2025-11-25 for backward compatibility
- Allow per-backend protocol version selection

```rust
use tower_mcp::ProtocolSupport;

// Enable both 2026-07-28 and 2025-11-25
let protocol_support = ProtocolSupport::compiled()
    .with_versions(&["2026-07-28", "2025-11-25"]);
```

### 3. HTTP Transport Changes (`src/proxy.rs`, `src/ws_transport.rs`)

**2025-11-25 (Current):**
- Session-based with `initialize`/`initialized` handshake
- `Mcp-Session-Id` header for session tracking
- SSE stream for notifications

**2026-07-28 (New):**
- Stateless - each request independent
- Headers: `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name`
- Per-request `_meta` with protocol version, client info, capabilities
- `subscriptions/listen` endpoint for notifications
- `server/discover` RPC for capability discovery

**Implementation:**
- Update HTTP router to handle new headers
- Add `subscriptions/listen` route
- Support both session-based and stateless modes simultaneously
- Per-backend middleware must work without session context

### 4. WebSocket Transport Changes (`src/ws_transport.rs`)

**Current state (T3.1 in-progress):**
- `connect_with_protocol_version(url, version)` added — sends `Sec-WebSocket-Protocol: mcp.version.{version}` header during handshake
- `connect_with_bearer_token(url, token, protocol_version)` updated with optional `protocol_version` parameter
- Server response parsed for `Sec-WebSocket-Protocol` header to extract negotiated version
- `negotiated_version` field added to struct for future use
- Callers in `proxy.rs`, `endpoint_router.rs`, `reload.rs` updated (pass `None` for now; T4.2 will wire to config)

**Remaining:**
- Remove session handshake requirement
- Add per-message `_meta` support
- Implement MRTR (Multi Round-Trip Requests) for sampling/elicitation
- Support `subscriptions/listen` over WebSocket

### 5. Admin Tools (`src/admin_tools.rs`)

- Ensure admin MCP tools work with both protocol versions
- May need to register tools for both protocol versions
- Test with `McpRouter::with_protocol_support()`

### 6. Configuration (`src/config.rs`)

Add new config options:
```toml
[proxy]
protocol_versions = ["2026-07-28", "2025-11-25"]  # Runtime allowlist
default_protocol_version = "2025-11-25"  # Default for backward compat

[backends]
# Per-backend protocol version override
protocol_version = "2026-07-28"  # optional
```

### 7. Backend Middleware (`src/reload.rs`, `src/proxy.rs`)

Per-backend middleware stack must work in stateless mode:
- Retry, circuit breaker, outlier detection - no session dependency
- Rate limiting - use client identity from `_meta` instead of session ID
- Authentication - extract from headers or `_meta`

### 8. Testing Strategy

**Integration Tests (`tests/integration.rs`):**
- Test both protocol versions
- Test stateless request handling
- Test `server/discover` RPC
- Test `subscriptions/listen` endpoint
- Test header-based routing

**E2E Tests (`tests/e2e.rs`):**
- Full proxy pipeline with 2026-07-28
- Backward compatibility with 2025-11-25
- Multi-backend with mixed protocol versions
- Conformance suite integration

**Conformance Tests:**
- Run official MCP conformance suite
- Target: 39/39 server, 265/265 client checks for 2026-07-28

## Data Flow Changes

### Request Flow (2026-07-28)

```
Client Request
    │
    ├── Headers: MCP-Protocol-Version: 2026-07-28, Mcp-Method: tools/call, Mcp-Name: my_tool
    │
    ├── Body: {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"my_tool","arguments":{...},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{},"io.modelcontextprotocol/clientInfo":{"name":"client","version":"1.0"}}}}
    │
    ▼
HTTP Router (proxy.rs)
    │
    ├── Parse headers for routing
    ├── Extract _meta for client identity/capabilities
    ├── No session lookup needed
    │
    ▼
Global Middleware Stack
    │
    ├── Auth (from headers/_meta)
    ├── Access Log
    ├── Metrics
    ├── RBAC (using client identity from _meta)
    │
    ▼
Backend Selection & Per-Backend Middleware
    │
    ├── Retry / Circuit Breaker / Outlier Detection (stateless)
    ├── Rate Limit (by client identity from _meta)
    │
    ▼
Backend Transport (ChannelTransport / HTTP / WebSocket)
    │
    ▼
Backend Server
    │
    ▼
Response (with cache hints, deterministic ordering)
```

### Backward Compatibility Flow (2025-11-25)

Existing flow preserved - session-based with `initialize` handshake, `Mcp-Session-Id`, SSE notifications.

## Error Handling

### New Error Types (2026-07-28)
- `UnsupportedProtocolVersion` - returned when client requests unsupported version
- Per-request error handling (no session context)
- Structured error responses per JSON-RPC 2.0

### Migration Strategy
- Default to 2025-11-25 for existing clients
- Opt-in to 2026-07-28 via headers
- Graceful degradation if 2026 features not compiled

## Security Considerations

- **Authorization**: Shift from session-based to per-request (headers/_meta)
- **Rate Limiting**: Use client identity from `_meta` instead of session ID
- **CORS**: Update for new headers
- **CSP**: May need updates for new endpoints

## Performance Impact

- **Positive**: No session state management overhead
- **Positive**: Better horizontal scaling (stateless)
- **Neutral**: Per-request metadata parsing
- **Monitoring**: Add metrics for protocol version distribution

## Rollout Plan

1. **Phase 1**: Compile with `protocol-2026-07-28` feature, fix compilation errors
2. **Phase 2**: Runtime enablement with `ProtocolSupport` (default 2025-11-25)
3. **Phase 3**: Add config for protocol version selection
4. **Phase 4**: Test both protocols, fix issues
5. **Phase 5**: Run conformance suite
6. **Phase 6**: Update MSRV to 1.97
7. **Phase 7**: Remove local patch
8. **Phase 8**: Documentation and release