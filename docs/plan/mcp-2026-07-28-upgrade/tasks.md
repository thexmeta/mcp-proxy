# Tasks: MCP 2026-07-28 Spec Upgrade

## Implementation Plan

### Wave 1: Foundation - Dependency Upgrade & Compilation Fixes

| Task ID | Description | Agent | Dependencies | Status |
|---------|-------------|-------|--------------|--------|
| T1.1 | Update Cargo.toml: tower-mcp 0.20, tower-mcp-types 0.20, enable `protocol-2026-07-28` feature | gem-implementer | - | ✅ done |
| T1.2 | Update Cargo.toml: rust-version = "1.97" | gem-implementer | - | ✅ done |
| T1.3 | Remove `[patch.crates-io]` section for local tower-mcp | gem-implementer | T1.1 | ✅ done |
| T1.4 | Fix all compilation errors from tower-mcp API changes (0.12 → 0.20) | gem-implementer | T1.1 | ✅ done |
| T1.5 | Update `.github/workflows/ci.yml`: MSRV job to Rust 1.97 | gem-implementer | T1.2 | ✅ done |
| T1.6 | Run `cargo check` and `cargo test --lib` to verify compilation | gem-implementer | T1.4 | ✅ done |

### Wave 2: Core Protocol Support - HTTP Transport

| Task ID | Description | Agent | Dependencies | Status |
|---------|-------------|-------|--------------|--------|
| T2.1 | Add `ProtocolSupport` configuration in `src/proxy.rs` for 2026-07-28 + 2025-11-25 | gem-implementer | T1.6 | ✅ done |
| T2.2 | Update HTTP router to parse new headers: `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name` | gem-implementer | T2.1 | ✅ done (handled by tower-mcp) |
| T2.3 | Implement `subscriptions/listen` endpoint for 2026-07-28 | gem-implementer | T2.1 | ✅ done (handled by tower-mcp) |
| T2.4 | Add `server/discover` RPC handling | gem-implementer | T2.1 | ✅ done (Discover middleware in src/discover.rs) |
| T2.5 | Update per-request `_meta` extraction and validation | gem-implementer | T2.1 | ✅ done (MetaValidationLayer in src/meta_validation.rs) |
| T2.6 | Ensure backward compatibility: 2025-11-25 session-based flow still works | gem-implementer | T2.2 | ✅ done (verified by tests) |

### Wave 3: WebSocket Transport & Backend Integration

| Task ID | Description | Agent | Dependencies | Status |
|---------|-------------|-------|--------------|--------|
| T3.1 | Update `src/ws_transport.rs` for stateless WebSocket (no handshake) | gem-implementer | T1.6 | ⏳ in-progress |
| T3.2 | Add per-message `_meta` support in WebSocket | gem-implementer | T3.1 | pending |
| T3.3 | Implement MRTR (Multi Round-Trip Requests) for sampling/elicitation | gem-implementer | T3.1 | pending |
| T3.4 | Update backend middleware stack for stateless operation (retry, circuit breaker, outlier) | gem-implementer | T2.1 | pending |
| T3.5 | Update rate limiting to use client identity from `_meta` instead of session ID | gem-implementer | T2.5 | pending |
| T3.6 | Update authentication to extract from headers/_meta | gem-implementer | T2.5 | pending |

### Wave 4: Configuration & Admin Tools

| Task ID | Description | Agent | Dependencies | Status |
|---------|-------------|-------|--------------|--------|
| T4.1 | Add protocol version config options in `src/config.rs` | gem-implementer | T2.1 | pending |
| T4.2 | Add per-backend protocol_version config option | gem-implementer | T4.1 | pending |
| T4.3 | Update `src/admin_tools.rs` for dual-protocol support | gem-implementer | T2.1 | pending |
| T4.4 | Update example configs in `examples/` for 2026-07-28 | gem-implementer | T4.1 | pending |

### Wave 5: Testing & Conformance

| Task ID | Description | Agent | Dependencies | Status |
|---------|-------------|-------|--------------|--------|
| T5.1 | Update `tests/integration.rs` for 2026-07-28 stateless tests | gem-browser-tester | T2.6 | pending |
| T5.2 | Update `tests/e2e.rs` for full proxy pipeline with 2026-07-28 | gem-browser-tester | T3.6 | pending |
| T5.3 | Add conformance suite integration (39 server + 265 client checks) | gem-browser-tester | T5.1 | pending |
| T5.4 | Test backward compatibility with 2025-11-25 clients | gem-browser-tester | T2.6 | pending |
| T5.5 | Test mixed protocol versions (some backends 2026, some 2025) | gem-browser-tester | T3.6 | pending |
| T5.6 | Run full test suite: `cargo test --all-features` | gem-browser-tester | T5.2 | pending |

### Wave 6: Cleanup & Documentation

| Task ID | Description | Agent | Dependencies | Status |
|---------|-------------|-------|--------------|--------|
| T6.1 | Remove local `patches/tower-mcp/` directory | gem-implementer | T1.3, T5.6 | pending |
| T6.2 | Update AGENTS.md with new version info | gem-documentation-writer | T5.6 | pending |
| T6.3 | Update README.md with 2026-07-28 support info | gem-documentation-writer | T5.6 | pending |
| T6.4 | Update CHANGELOG.md with upgrade details | gem-documentation-writer | T5.6 | pending |
| T6.5 | Verify `cargo fmt --all -- --check` and `cargo clippy --all-targets --all-features -- -D warnings` pass | gem-implementer | T5.6 | pending |
| T6.6 | Create release notes for version bump | gem-documentation-writer | T6.2 | pending |

## Baseline

**Objective**: Upgrade mcp-proxy to support MCP 2026-07-28 specification with stateless protocol core, while maintaining backward compatibility with 2025-11-25 and 2025-03-26, and update MSRV to Rust 1.97.

**Acceptance Criteria**:
1. tower-mcp 0.20+ with `protocol-2026-07-28` feature compiles and works
2. Proxy handles stateless requests (no session handshake)
3. Proxy supports `server/discover` RPC and `subscriptions/listen` endpoint
4. HTTP headers `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name` parsed correctly
5. Per-request `_meta` with protocol version, client identity, capabilities supported
6. Backward compatibility: 2025-11-25 session-based flow works unchanged
7. MSRV updated to Rust 1.97, CI passes
8. Local tower-mcp patch removed
9. All tests pass (unit, integration, e2e)
10. MCP conformance suite: 39/39 server + 265/265 client checks pass for 2026-07-28

## Plan Lineage

```yaml
plan_lineage:
  revision: 1
  replan_count: 0
  max_replans: 2
```

## Context Version

```yaml
context_version: 4
context_updated_at: "2026-08-10T12:00:00Z"
changed_fields:
  - "Wave 1 completed: All Wave 1 tasks (T1.1-T1.6) done"
  - "Cargo.toml updated to tower-mcp 0.20.1 with protocol-2026-07-28 feature"
  - "Rust MSRV updated to 1.97"
  - "Local patch removed"
  - "All compilation errors fixed (CallToolParams, ReadResourceParams, GetPromptParams)"
  - "CI updated to Rust 1.97"
  - "All 397+ tests passing (302 lib + 95 integration/e2e)"
  - "cargo fmt --check and cargo clippy --all-targets --all-features -- -D warnings pass"
  - "Wave 2 completed: All Wave 2 tasks (T2.1-T2.6) done"
  - "ProtocolSupport configured for 2026-07-28 + 2025-11-25"
  - "Discover middleware implemented (src/discover.rs)"
  - "MetaValidationLayer implemented (src/meta_validation.rs)"
  - "Backward compatibility verified"
  - "Wave 3 T3.1 in-progress: WebSocket transport updated with protocol version subprotocol support"
  - "ws_transport.rs: connect_with_protocol_version() added, Sec-WebSocket-Protocol mcp.version.* header support"
  - "ws_transport.rs: connect_with_bearer_token() updated with optional protocol_version parameter"
  - "Callers in proxy.rs, endpoint_router.rs, reload.rs updated to pass None for protocol_version"
  - "All 302 lib tests still passing after ws_transport changes"
```

## Agent Assignments Summary

- **gem-implementer**: T1.1-T1.6, T2.1-T2.6, T3.1-T3.6, T4.1-T4.4, T6.1, T6.5
- **gem-browser-tester**: T5.1-T5.6
- **gem-documentation-writer**: T6.2-T6.4, T6.6

## Notes

- This is a HIGH complexity upgrade due to breaking changes in tower-mcp 0.12 → 0.20 and MCP protocol
- Consider delegating T1.4 (compilation fixes) to gem-debugger if errors are complex
- Wave 2 and 3 can partially overlap (different transports)
- Conformance testing (Wave 5) is critical - budget extra time
- Remove local patch only after verifying upstream fix (T1.3 → T6.1)