# AGENTS.md

## Overview

mcp-proxy is a config-driven MCP (Model Context Protocol) reverse proxy built in Rust. It aggregates multiple MCP backends behind a single HTTP endpoint with per-backend middleware, authentication, and observability. Built on tower-mcp 0.21+ and the tower middleware ecosystem.

Package name: `mcp-proxy`. Binary name: `mcp-proxy`. Library name: `mcp_proxy`.

**MCP Protocol Support:**
- **2026-07-28** (stateless, no session handshake, per-request `_meta`, `server/discover`, `subscriptions/listen`)
- **2025-11-25** (session-based with `initialize` handshake, `Mcp-Session-Id`, SSE)
- **2025-03-26** (legacy)

Protocol version is configurable via `[proxy.protocol_support]` in TOML config:
```toml
[proxy.protocol_support]
versions = ["2026-07-28", "2025-11-25"]
default_protocol_version = "2026-07-28"
```

## Project structure

```
src/
  main.rs          # CLI entry point (clap), logging setup
  lib.rs           # Library root, re-exports Proxy and ProxyConfig
  proxy.rs         # Core: builds proxy, middleware stack, and axum router
  config.rs        # TOML config parsing, all config types
  admin.rs         # HTTP admin API (/admin/backends, /admin/health, etc.)
  admin_tools.rs   # MCP admin tools (proxy/ namespace, via ChannelTransport)
  reload.rs        # Hot reload: file watcher, dynamic backend addition
  test_util.rs     # MockService, ErrorMockService, call_service helper

  # Global middleware (wraps the entire proxy, applied in proxy.rs)
  access_log.rs    # Structured access logging (AccessLogService, target: mcp::access)
  alias.rs         # Tool renaming (AliasService)
  filter.rs        # Capability filtering -- allow/deny lists, glob patterns (CapabilityFilterService)
  inject.rs        # Argument injection into tool calls (InjectArgsService)
  mirror.rs        # Traffic mirroring to canary backends (MirrorService)
  cache.rs         # Response caching with TTL (CacheService)
  coalesce.rs      # Request deduplication (CoalesceService)
  validation.rs    # Argument size limits (ValidationService)
  metrics.rs       # Prometheus counters and histograms (MetricsService)
  rbac.rs          # Role-based access control (RbacService)
  token.rs         # Auth token passthrough to backends (TokenPassthroughService)
  client_rate_limit.rs # Per-client-identity rate limiting (ClientIdentityRateLimitService)
  discover.rs      # server/discover RPC handler (DiscoverService, SEP-2575)
  meta_validation.rs # Per-request _meta validation (MetaValidationService, SEP-2243)
  mrtr.rs          # Model-Redirected Tool Results (ProxyClientHandler for sampling/elicitation)

  # Per-backend middleware (applied per-backend in proxy.rs and reload.rs)
  retry.rs         # Retry with exponential backoff (McpRetryPolicy)
  outlier.rs       # Outlier detection and ejection (OutlierDetectionLayer)

tests/
  integration.rs   # Middleware composition tests with in-process backends via ChannelTransport
  e2e.rs           # Comprehensive E2E test suite (44 tests, 10 tiers)

examples/
  configs/*.toml  # Example proxy configs for different deployment patterns
  docker-compose/  # Docker compose example with HTTP backend
```

## Architecture

### Middleware stack ordering

The middleware stack is built in `proxy.rs::build_middleware_stack()`. Order matters -- outermost runs first.

**Global middleware** (wraps the entire proxy service):
```
Request flow (outer to inner):
Auth (axum layer) -> Audit -> Access Log -> Metrics -> Token Passthrough -> RBAC
  -> Client Rate Limit -> Alias -> Filter -> Validation -> Coalesce -> Cache
  -> Mirror -> Inject Args -> Discover -> MetaValidation -> McpProxy
```

**Per-backend middleware** (applied to each backend individually):
```
Request flow (inner to outer, applied via builder.backend_layer()):
Retry -> Hedge -> Concurrency Limit -> Rate Limit
  -> Timeout -> Circuit Breaker -> Outlier Detection -> Backend
```

### Endpoint-group middleware stacks

Endpoint-group routes (e.g. `/os/mcp`) build their OWN middleware stack in
`src/endpoint_router.rs::build_endpoint_group_middleware_stack`, SEPARATE from
the root stack in `src/proxy.rs::build_middleware_stack`. Both stacks must stay
at protocol parity.

**Convention (drift-proofing):** any protocol-level layer (2026-07-28 and later)
MUST be added via the shared `crate::proxy::apply_2026_layers` helper, and any
transport-level protocol configuration MUST go through
`crate::proxy::build_protocol_support`. Never add a protocol layer to only one
stack -- `apply_2026_layers` and `build_protocol_support` are the single source
of truth for both root and group routes.

Effective order (outer→inner) for an endpoint-group route:

    MetaValidation -> Discover -> SubscriptionsListen -> [GroupFilter] -> McpProxy

`GroupFilter` stays innermost-effective so group tool scoping is preserved; the
2026 layers sit immediately outside it, in the same relative position as in the
root stack (where they sit immediately outside `McpProxy`).

Endpoint groups intentionally do NOT mirror the following root-only layers
(out of scope for protocol parity): `SearchModeFilter`, `ToolGroup`,
`GlobalRateLimit`, `ClientRateLimit`. This asymmetry is deliberate, not a bug.

Hot reload inherits group fixes automatically: `src/reload.rs` builds group
routes via `build_single_endpoint_group`, which calls the same
`build_endpoint_group_middleware_stack` and `build_protocol_support`.

### Key design pattern: Error = Infallible

All services use `Error = Infallible`. Errors are represented inside the response:
`RouterResponse { id: RequestId, inner: Result<McpResponse, JsonRpcError> }`.

tower-resilience and tower middleware produce typed errors (e.g., `CircuitBreakerError<E>`).
These are converted to `RouterResponse` error values via `tower_mcp::CatchError` wrapper.
The pattern in `reload.rs` is:
```rust
let limited = tower::Layer::layer(&layer, svc);
svc = BoxCloneService::new(tower_mcp::CatchError::new(limited));
```

In `proxy.rs`, `builder.backend_layer(layer)` handles this internally.

### Adding a new middleware layer

Every middleware follows the same tower Service pattern. Use `inject.rs` as a template:

1. Create `src/your_middleware.rs` with:
   - A service struct wrapping an inner service: `YourService<S> { inner: S, config: ... }`
   - `impl<S> Service<RouterRequest> for YourService<S>` with `Response = RouterResponse`, `Error = Infallible`
   - Match on `req.inner` to inspect the MCP request type (ListTools, CallTool, etc.)
   - Unit tests using `MockService` and `call_service` from `test_util`

2. Add `pub mod your_middleware;` to `lib.rs`

3. Wire it into the stack in `proxy.rs::build_middleware_stack()`:
   - For global middleware: wrap the `BoxCloneService` at the appropriate position
   - For per-backend: use `builder.backend_layer(layer)` in the backend loop
   - For protocol-level layers (2026-07-28 and later): wire through
     `crate::proxy::apply_2026_layers` so BOTH the root and endpoint-group
     stacks receive it. Never add a protocol layer to only one stack. Endpoint
     groups inherit the fix automatically via `reload.rs::build_single_endpoint_group`.

4. Add config fields to `config.rs` (with `#[serde(default)]` for optional fields)

5. Wire the hot reload path in `reload.rs::build_backend_layer()` (per-backend only)

6. Add integration tests in `tests/integration.rs`

### Adding a new backend transport

Transports are added in `proxy.rs::build_mcp_proxy()` and `reload.rs::add_backend()`:

1. Add a variant to `TransportType` enum in `config.rs`
2. Add a match arm in both `build_mcp_proxy()` and `add_backend()`
3. The transport must implement tower-mcp's `ClientTransport` trait

### Configuration

All config is in `config.rs`. The top-level type is `ProxyConfig`:
- `proxy`: name, version, separator, listen address, hot_reload
- `backends[]`: name, transport, command/url, per-backend middleware configs
- `auth`: bearer tokens or JWT/JWKS with RBAC
- `performance`: request coalescing
- `security`: argument size limits
- `observability`: logging, metrics, tracing

Config maps directly to runtime: each optional config section (timeout, rate_limit, circuit_breaker, etc.) gates whether that middleware layer is applied.

Environment variable substitution: `${VAR_NAME}` in string values is resolved via `config.resolve_env_vars()`.

### Testing

Every feature gets tests. No exceptions.

**Unit tests** (`#[cfg(test)]` in each module):
- Use `MockService::with_tools(&["tool1", "tool2"])` for a service that returns tool lists and echoes call results
- Use `ErrorMockService` for a service that returns JSON-RPC errors
- Use `call_service(&mut svc, McpRequest::...)` to send requests through the service

**E2E tests** (`tests/e2e.rs`):
- Comprehensive test suite covering full proxy pipeline
- Use `ChannelTransport` to create in-process MCP backends (no external processes)
- Build an `McpProxy` with tools, compose middleware, verify end-to-end behavior
- Includes error backends, slow backends, concurrent request testing
- Use `tower-resilience-chaos` for chaos engineering / failure injection tests

**Integration tests** (`tests/integration.rs`):
- Middleware composition tests with real proxy routing

### Documentation

Rust docs are the single source of truth for both CLI users and library users.

- All public APIs must have doc comments
- Module-level docs explain purpose and usage patterns
- Doc examples should be runnable (`cargo test --doc`)
- Keep examples up to date -- stale doc examples break `cargo test --doc`

**Pre-push checks** (MUST pass before every push):
```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --all-features
cargo test --test '*' --all-features
cargo doc --no-deps --all-features       # catches missing/broken docs
cargo test --doc --all-features          # catches stale doc examples
```

## Conventions

- Rust 2024 edition, MSRV 1.97
- `anyhow` for application errors, `thiserror` for library errors
- Conventional commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`
- All public APIs have doc comments
- Metrics use `mcp_proxy_` prefix
- Admin tools live under `proxy/` namespace
- MCP protocol: supports 2026-07-28 (stateless) and 2025-11-25 (session-based)
- tower-mcp: 0.20+ with `protocol-2026-07-28` feature
