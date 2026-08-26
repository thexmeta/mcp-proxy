# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Bug Fixes

- **Endpoint groups with only lazy stdio backends returned 0 tools**: the `in_group()` helper in `WarmCatalogService` split tool names on the separator and matched the bare prefix (e.g. `"term"`) against the scope set, which stores prefixes WITH the trailing separator (e.g. `"term_"`). The mismatch caused every warm-catalog tool append for endpoint groups to silently fail, so groups containing ONLY lazy stdio backends (os, lsp, browser, desktop, python, cdp) exposed 0 tools. Fixed by matching on `name.starts_with(prefix)` instead of splitting. Added regression tests `g4_endpoint_group_with_only_lazy_stdio_exposes_warm_catalog_tools` and `g5_endpoint_group_cache_miss_probes_fs_like_backend` (both fail with the old code, pass with the fix) and live e2e tests `all_endpoint_groups_expose_tools` / `default_endpoint_exposes_full_tool_set`.
- **Lazy backends that don't support `resources/list` got NO warm catalog**: the warm-catalog probe aborted on the first capability-listing error, so servers that expose tools but not resources (e.g. `rust-mcp-filesystem`, `roslyn`, `codebase`, `sequential_thinking`, `cedar_analysis`) produced no warm catalog and their tools were missing from endpoint groups (the `os` group showed only `term_*`, not `fs_*`). Fixed by making `resources`/`resource_templates`/`prompts` listings best-effort — a failure is logged and skipped, while `tools` (the critical capability) still propagates. Added regression test `probe_succeeds_when_resources_unsupported`. After this fix all 16 live backends get a warm catalog and every endpoint group exposes its tools.

These two fixes were previously uncommitted working-tree edits; they are now durable in this commit. The deployed binary was rebuilt with both fixes, resolving the `os` endpoint-group regression (the `os` group now exposes the full 24-tool set: 17 `fs_*` + 7 `term_*`).

### Features

- **Shared backend pool**: each backend spawns exactly once regardless of how many endpoint groups reference it
- Global backend environment variables via `[proxy.backend_env]` (merged into all stdio backends)
- Global middleware defaults (`[proxy.timeout]`, `[proxy.circuit_breaker]`, `[proxy.retry]`) applied to all backends
- Endpoint group shorthand syntax: `proxy.endpoint_group_list = ["os", "web"]`
- Group-aware capability filtering (GroupFilterService) for endpoint group namespace isolation
- 14 new integration tests for shared proxy, endpoint groups, and group filtering
- MCP 2026-07-28 protocol support (stateless, per-request `_meta`, `server/discover`, `subscriptions/listen`)
- Per-client-identity rate limiting based on `_meta.clientInfo.name`
- `proxy/protocol_info` admin tool for dual-protocol introspection
- Model-Redirected Tool Results (MRTR) handler for sampling/elicitation stubs
- Per-backend `protocol_version` config option for HTTP/WebSocket backends
- `default_protocol_version` config option in `[proxy.protocol_support]`
- Discover middleware for `server/discover` RPC (SEP-2575)
- MetaValidation middleware for per-request `_meta` validation (SEP-2243)
- **Lazy backend spawning with persistent warm tool cache**: backends with `spawn_mode = "lazy"` are not spawned at startup; their tool catalog is served from a persisted warm cache and the backend is spawned on first request (coalesced). Idle stateless (2026-07-28) backends are terminated after `idle_timeout_secs`; the warm catalog survives restarts. See `examples/configs/lazy-backend.toml`.

### Dependencies

- Upgrade tower-mcp 0.12.0 → 0.20.1 with `protocol-2026-07-28` feature

### Miscellaneous Tasks

- Remove local `patches/tower-mcp/` directory (upstream fix incorporated)
- Update MSRV to Rust 1.97
- Update CI for Rust 1.97 MSRV

### Testing

- E2E tests for 2026-07-28 stateless requests, discover, mixed-protocol scenarios
- Config tests for `ProtocolSupportConfig` and per-backend `protocol_version`
- Unit tests for `ClientIdentityRateLimitService` and `MetaValidationService`

## [0.4.0] - 2026-06-10

### Bug Fixes

- Require admin_token when auth is jwt or oauth ([#174](https://github.com/joshrotenberg/mcp-proxy/pull/174))
- Patch TLS cert-validation advisories + compatible dep bumps ([#173](https://github.com/joshrotenberg/mcp-proxy/pull/173))
- Reject introspection tokens with missing aud when audience configured (closes #177) ([#180](https://github.com/joshrotenberg/mcp-proxy/pull/180))
- Enforce required_scopes in oauth auth (closes #175) ([#182](https://github.com/joshrotenberg/mcp-proxy/pull/182))

### Features

- Add rbac default_deny for unmapped scopes (closes #176) ([#181](https://github.com/joshrotenberg/mcp-proxy/pull/181))

### Miscellaneous Tasks

- Upgrade tower-mcp 0.9.2 -> 0.12.0 ([#184](https://github.com/joshrotenberg/mcp-proxy/pull/184))
- Upgrade opentelemetry stack 0.29 -> 0.32 ([#185](https://github.com/joshrotenberg/mcp-proxy/pull/185))
- Bump remaining dependencies to latest ([#186](https://github.com/joshrotenberg/mcp-proxy/pull/186))

### Testing

- Add http-level negative auth tests (closes #178) ([#183](https://github.com/joshrotenberg/mcp-proxy/pull/183))



## [0.3.1] - 2026-03-18

### Testing

- Add examples for library and config scenarios ([#170](https://github.com/joshrotenberg/mcp-proxy/pull/170))
- Add unit tests for admin_tools, coalesce, and reload modules ([#172](https://github.com/joshrotenberg/mcp-proxy/pull/172))
- Add unit tests for ws_transport, discovery, and skills modules ([#171](https://github.com/joshrotenberg/mcp-proxy/pull/171))



## [0.3.0] - 2026-03-18

### Bug Fixes

- Add PUT /admin/config endpoint for config updates ([#162](https://github.com/joshrotenberg/mcp-proxy/pull/162))

### Features

- Helm chart for Kubernetes deployment ([#158](https://github.com/joshrotenberg/mcp-proxy/pull/158))
- Agentskills.io compliant skills for proxy management ([#159](https://github.com/joshrotenberg/mcp-proxy/pull/159))
- Admin API auth protection ([#164](https://github.com/joshrotenberg/mcp-proxy/pull/164))
- Expose circuit breaker states via admin API ([#166](https://github.com/joshrotenberg/mcp-proxy/pull/166))

### Testing

- Add unit tests for session admin endpoints ([#160](https://github.com/joshrotenberg/mcp-proxy/pull/160))

### Research

- Benchmark proxy overhead with criterion ([#165](https://github.com/joshrotenberg/mcp-proxy/pull/165))



## [0.2.0] - 2026-03-17

### Bug Fixes

- Access log includes backend name in structured output ([#144](https://github.com/joshrotenberg/mcp-proxy/pull/144))
- --check warns about unset environment variables ([#145](https://github.com/joshrotenberg/mcp-proxy/pull/145))
- Add missing REST API endpoints ([#150](https://github.com/joshrotenberg/mcp-proxy/pull/150))
- Failover supports priority field for backend ordering ([#153](https://github.com/joshrotenberg/mcp-proxy/pull/153))
- Add remaining REST API endpoints ([#155](https://github.com/joshrotenberg/mcp-proxy/pull/155))

### Documentation

- Add runnable examples for library embedding ([#127](https://github.com/joshrotenberg/mcp-proxy/pull/127))
- Comprehensive config.example.toml with all options documented ([#142](https://github.com/joshrotenberg/mcp-proxy/pull/142))

### Features

- Add --check config validation flag ([#89](https://github.com/joshrotenberg/mcp-proxy/pull/89))
- Add structured access logging middleware ([#91](https://github.com/joshrotenberg/mcp-proxy/pull/91))
- Add glob pattern support for tool filtering ([#90](https://github.com/joshrotenberg/mcp-proxy/pull/90))
- Expose middleware as composable tower::Layer implementations ([#98](https://github.com/joshrotenberg/mcp-proxy/pull/98))
- Add backend failover routing ([#99](https://github.com/joshrotenberg/mcp-proxy/pull/99))
- Add global rate limiting across all backends ([#108](https://github.com/joshrotenberg/mcp-proxy/pull/108))
- Support .mcp.json as a backend config source ([#109](https://github.com/joshrotenberg/mcp-proxy/pull/109))
- Complete hot reload with backend removal and modification ([#110](https://github.com/joshrotenberg/mcp-proxy/pull/110))
- Add ProxyBuilder for programmatic proxy construction ([#111](https://github.com/joshrotenberg/mcp-proxy/pull/111))
- Add REST management API for backend lifecycle ([#112](https://github.com/joshrotenberg/mcp-proxy/pull/112))
- Integrate utoipa for OpenAPI spec generation ([#128](https://github.com/joshrotenberg/mcp-proxy/pull/128))
- Add cache backend config and validation ([#133](https://github.com/joshrotenberg/mcp-proxy/pull/133))
- Add composite/parallel tool fan-out middleware ([#135](https://github.com/joshrotenberg/mcp-proxy/pull/135))
- Add parameter hiding and renaming for tool customization ([#134](https://github.com/joshrotenberg/mcp-proxy/pull/134))
- Annotation-aware tool filtering ([#137](https://github.com/joshrotenberg/mcp-proxy/pull/137))
- Add per-token tool scoping for bearer auth ([#138](https://github.com/joshrotenberg/mcp-proxy/pull/138))
- Add WebSocket backend transport support ([#139](https://github.com/joshrotenberg/mcp-proxy/pull/139))
- BM25-based tool discovery and search ([#140](https://github.com/joshrotenberg/mcp-proxy/pull/140))
- Add OAuth 2.1 authorization flow support ([#141](https://github.com/joshrotenberg/mcp-proxy/pull/141))
- Support YAML config format ([#146](https://github.com/joshrotenberg/mcp-proxy/pull/146))
- Ergonomic per-backend builder methods for ProxyBuilder ([#147](https://github.com/joshrotenberg/mcp-proxy/pull/147))
- Regex support for tool filtering (re: prefix) ([#149](https://github.com/joshrotenberg/mcp-proxy/pull/149))
- Pure .mcp.json mode (no TOML config needed) ([#148](https://github.com/joshrotenberg/mcp-proxy/pull/148))
- CacheLayer tower::Layer implementation ([#151](https://github.com/joshrotenberg/mcp-proxy/pull/151))
- Search-mode tool exposure for large tool sets ([#154](https://github.com/joshrotenberg/mcp-proxy/pull/154))
- Bump tower-mcp 0.8.8, add session and circuit breaker admin endpoints ([#156](https://github.com/joshrotenberg/mcp-proxy/pull/156))
- Implement Redis and SQLite cache backends ([#157](https://github.com/joshrotenberg/mcp-proxy/pull/157))

### Miscellaneous Tasks

- Move scratch config files to examples/ ([#143](https://github.com/joshrotenberg/mcp-proxy/pull/143))

### Testing

- Add comprehensive end-to-end integration test suite ([#94](https://github.com/joshrotenberg/mcp-proxy/pull/94))
- HTTP transport-level E2E tests ([#152](https://github.com/joshrotenberg/mcp-proxy/pull/152))



## [0.1.1] - 2026-03-08

### Documentation

- Add installation methods to README
- Add installation methods and optimize dist profile ([#71](https://github.com/joshrotenberg/mcp-proxy/pull/71))

### Miscellaneous Tasks

- Release v0.1.0



## [0.1.0] - 2026-03-08

### Bug Fixes

- Exclude examples/docker-compose/proxy.toml from gitignore
- Update CI workflows to use main branch

### Documentation

- Add README, LICENSE files, CI workflow
- Add architecture patterns research and module doc improvements
- Add missing doc comments across all public APIs
- Update README and config.example.toml for all features
- Add AGENTS.md for AI agent context

### Features

- Library mode, hot reload, and gateway refactor
- Middleware modules and example configs
- MCP admin tools under gateway/ namespace
- Enrich per-backend health with timestamps, failure tracking, and transport info
- Add health_check and add_backend MCP admin tools
- Apply per-backend middleware to hot-reloaded backends
- Add cache stats and clear endpoints to admin API
- Add per-backend retry with exponential backoff
- Token passthrough and per-backend static auth
- Retry budget to prevent retry storms
- Passive health checks via outlier detection
- Traffic mirroring / shadowing
- Request hedging via tower-resilience
- Argument injection for tool calls
- Add library mode example
- Add canary/weighted routing middleware

### Miscellaneous Tasks

- Add Dockerfile and .dockerignore
- Switch tower-resilience to published 0.9.1 and fix stale mcp-gateway refs

### Refactor

- Rename to mcp-proxy
- Migrate retry to tower-resilience RetryLayer
- Rename gateway to proxy across entire codebase

### Styling

- Fix rustfmt formatting in retry.rs

### Testing

- Add unit tests for all middleware modules (46 tests)
- Add integration tests with in-process MCP backends
- Add integration tests for admin tools, dynamic backends, and cache stats
- Add admin and metrics test coverage
- Integration tests for inject, mirror, coalesce, and full stack


