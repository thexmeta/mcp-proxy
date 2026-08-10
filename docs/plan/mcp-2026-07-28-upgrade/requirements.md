# Requirements: MCP 2026-07-28 Spec Upgrade

## Progress Summary (as of 2026-08-10)

| Wave | Status | Completion | Key Achievements |
|------|--------|------------|------------------|
| **Wave 1: Foundation** | ✅ **Complete** | 100% | Cargo.toml updated (tower-mcp 0.20.1, protocol-2026-07-28, rust-version 1.97), local patch removed, all compilation errors fixed, CI updated, all 397+ tests passing, clippy/fmt clean |
| **Wave 2: Core Protocol Support** | ✅ **Complete** | 100% | ProtocolSupport config (try_new with both versions), HTTP headers handled by tower-mcp, subscriptions/listen handled by tower-mcp, Discover middleware (src/discover.rs), MetaValidationLayer (src/meta_validation.rs), backward compat verified |
| **Wave 3: WebSocket Transport** | ⏳ In Progress | ~17% | T3.1 partially done: ws_transport.rs updated with protocol version subprotocol support (connect_with_protocol_version, Sec-WebSocket-Protocol mcp.version.* header), callers updated. Remaining: wire to config, per-message _meta, MRTR, backend middleware, rate limiting, auth |
| **Wave 4: Configuration & Admin** | ⏳ Pending | 0% | Protocol version config, per-backend protocol override, admin tools dual-protocol, example configs |
| **Wave 5: Testing & Conformance** | ⏳ Pending | 0% | Integration/e2e tests for 2026-07-28, conformance suite (39 server + 265 client checks) |
| **Wave 6: Cleanup & Documentation** | ⏳ Pending | 0% | Remove patches/, update AGENTS.md, README.md, CHANGELOG.md, release notes |

**Overall Progress: ~39% (2 of 6 waves complete, Wave 3 in progress)**

## User Stories and Acceptance Criteria (EARS Notation)

### REQ-001: MCP Protocol Upgrade
**WHEN** the mcp-proxy is built with tower-mcp 0.20+, **THE SYSTEM SHALL** support the MCP 2026-07-28 specification.

**Acceptance Criteria:**
- `protocol-2026-07-28` feature is enabled in tower-mcp dependency
- Proxy can handle stateless requests (no session initialization required)
- Proxy supports `server/discover` RPC for capability discovery
- Proxy handles `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name` headers correctly
- Backward compatibility with 2025-11-25 and 2025-03-26 protocols maintained

### REQ-002: HTTP Transport Updates
**WHEN** a client makes an HTTP request with MCP 2026-07-28 headers, **THE SYSTEM SHALL** route and process the request using the stateless protocol model.

**Acceptance Criteria:**
- No `initialize`/`initialized` handshake required
- No `Mcp-Session-Id` header handling
- Per-request `_meta` field with protocol version, client identity, client capabilities
- `subscriptions/listen` endpoint works for 2026-07-28
- Cache hints on list responses (TTL, deterministic ordering)

### REQ-003: WebSocket Transport Updates
**WHEN** a client connects via WebSocket using MCP 2026-07-28, **THE SYSTEM SHALL** support stateless operation over WebSocket.

**Acceptance Criteria:**
- WebSocket transport works without session handshake
- Per-request `_meta` support
- MRTR (Multi Round-Trip Requests) for server-to-client calls

### REQ-004: Admin/MCP Tools Compatibility
**WHEN** admin tools are invoked via MCP, **THE SYSTEM SHALL** work with both 2025-11-25 and 2026-07-28 protocols.

**Acceptance Criteria:**
- Admin tools accessible via both protocol versions
- `proxy/` namespace tools work correctly

### REQ-005: Rust Version Upgrade
**WHEN** the project is built, **THE SYSTEM SHALL** use Rust 1.97+ as MSRV.

**Acceptance Criteria:**
- `rust-version = "1.97"` in Cargo.toml
- CI tests pass on Rust 1.97 (stable)
- MSRV job in CI updated to 1.97

### REQ-006: Local Patch Removal
**WHEN** the ChannelTransport notification fix is upstreamed, **THE SYSTEM SHALL** use upstream tower-mcp without local patches.

**Acceptance Criteria:**
- `[patch.crates-io]` section removed from Cargo.toml
- Local `patches/tower-mcp/` directory can be removed
- All tests pass with upstream version

### REQ-007: Conformance Testing
**WHEN** the proxy is tested against MCP conformance suite, **THE SYSTEM SHALL** pass all server and client checks for 2026-07-28.

**Acceptance Criteria:**
- 39/39 server conformance checks pass
- 265/265 client conformance checks pass
- New 2026-07-28 specific tests pass

## Dependencies and Constraints

- **tower-mcp 0.20+** required (current: 0.12.0 patched)
- **tower-mcp-types 0.20+** required
- **Breaking changes** in tower-mcp API between 0.12 and 0.20
- **MSRV 1.97** requires Rust 1.97+ (current: 1.90)
- Local patch for ChannelTransport must be verified as upstreamed

## Edge Cases and Failure Points

1. **API Breaking Changes**: tower-mcp 0.12 → 0.20 has significant API changes
2. **Protocol Negotiation**: Must handle multiple protocol versions simultaneously
3. **Session Migration**: Existing 2025-11-25 sessions must continue working
4. **Header Parsing**: New HTTP headers must be parsed correctly
5. **Stateless Backend Routing**: Per-backend middleware must work without session context