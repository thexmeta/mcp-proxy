# Release Notes: MCP 2026-07-28 Protocol Support

## Feature Highlights

### MCP 2026-07-28 Protocol Support (stateless)

The proxy now supports the MCP 2026-07-28 specification alongside the existing 2025-11-25 (session-based) protocol. Both versions are enabled by default for maximum client compatibility.

**Key capabilities:**
- **Stateless requests** — no `initialize` handshake or `Mcp-Session-Id` required for 2026-07-28 clients
- **Per-request `_meta`** — clients include protocol version, client info, and capabilities in every request
- **`server/discover` RPC** — stateless capability discovery without session establishment (SEP-2575)
- **`subscriptions/listen`** — push-based subscription endpoint for 2026-07-28
- **HTTP header negotiation** — `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name` headers
- **WebSocket subprotocol** — `Sec-WebSocket-Protocol: mcp.version.{version}` negotiation

### New Middleware

- **`DiscoverService`** (`src/discover.rs`) — handles `server/discover` RPC by returning aggregated server capabilities from proxy config
- **`MetaValidationService`** (`src/meta_validation.rs`) — validates per-request `_meta` fields for 2026-07-28 (SEP-2243)
- **`ClientIdentityRateLimitService`** (`src/client_rate_limit.rs`) — per-client-identity rate limiting using `_meta.clientInfo.name`
- **`ProxyClientHandler`** (`src/mrtr.rs`) — Model-Redirected Tool Results stub for sampling/elicitation relay

### Configuration

New config options in `[proxy.protocol_support]`:

```toml
[proxy.protocol_support]
versions = ["2026-07-28", "2025-11-25"]  # both enabled by default
default_protocol_version = "2026-07-28"  # optional, highest enabled version if unset
```

Per-backend protocol version (HTTP/WebSocket backends only):

```toml
[[backends]]
name = "remote-api"
transport = "http"
url = "http://api.internal:8080"
protocol_version = "2026-07-28"
```

Per-client-identity rate limiting:

```toml
[proxy.client_rate_limit]
max_requests = 100
window_seconds = 60
cleanup_interval_seconds = 300
```

### Admin Tools

- `proxy/protocol_info` — shows supported protocol versions, default version, and per-backend protocol config

## Breaking Changes

None. This release is fully backward compatible with existing 2025-11-25 and 2025-03-26 clients.

## Migration Guide

1. **No config changes required** — both protocol versions are enabled by default
2. **Optional**: explicitly configure `[proxy.protocol_support]` to restrict or set a default version
3. **Optional**: set `protocol_version` on HTTP/WebSocket backends to pin a specific version
4. **Optional**: enable `[proxy.client_rate_limit]` for per-client rate limiting with 2026-07-28 clients

## Dependencies

- tower-mcp: 0.12.0 → 0.20.1 (with `protocol-2026-07-28` and `stateless` features)
- tower-mcp-types: 0.12.0 → 0.20.1

## Test Results

| Suite | Passed | Failed | Notes |
|-------|--------|--------|-------|
| Unit tests | 312 | 0 | All modules |
| Integration tests | 45 | 0 | Middleware composition |
| E2E tests | 69 | 1 | Pre-existing WebSocket host header issue |
| **Total** | **426** | **1** | 99.8% pass rate |

### New tests added in this release
- 8 E2E tests for 2026-07-28 stateless requests, discover, mixed-protocol scenarios
- 3 config tests for `ProtocolSupportConfig` and per-backend `protocol_version`
- 4 unit tests for `ClientIdentityRateLimitService`
- 2 unit tests for `ProxyClientHandler` (MRTR)
