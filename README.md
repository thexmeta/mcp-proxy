# mcp-proxy

[![Crates.io](https://img.shields.io/crates/v/mcp-proxy.svg)](https://crates.io/crates/mcp-proxy)
[![docs.rs](https://docs.rs/mcp-proxy/badge.svg)](https://docs.rs/mcp-proxy)
[![CI](https://github.com/joshrotenberg/mcp-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/joshrotenberg/mcp-proxy/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/mcp-proxy.svg)](LICENSE-MIT)

A config-driven [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) reverse proxy built in Rust. Aggregates multiple MCP backends behind a single endpoint with per-backend middleware, authentication, and observability.

Built on [tower-mcp](https://github.com/joshrotenberg/tower-mcp) and the [tower](https://github.com/tower-rs/tower) middleware ecosystem.

## Features

### Proxy
- **Multi-backend proxy** -- connect stdio and HTTP MCP servers behind one endpoint
- **Capability filtering** -- allow/deny lists for tools, resources, and prompts per backend
- **Tool aliasing** -- rename tools exposed by backends
- **Argument injection** -- merge default or per-tool arguments into tool calls
- **Hot reload** -- watch config file and add new backends without restart
- **Library mode** -- embed the proxy in your own Rust application

### Resilience
- **Timeout** -- per-backend request timeouts
- **Rate limiting** -- per-backend request rate limits
- **Concurrency limiting** -- per-backend max concurrent requests
- **Circuit breaker** -- trip open on failure rate threshold
- **Retry** -- automatic retries with exponential backoff and optional budget
- **Request hedging** -- parallel redundant requests to reduce tail latency
- **Outlier detection** -- passive health checks that eject unhealthy backends

### Traffic Management
- **Traffic mirroring** -- shadow traffic to a canary backend (fire-and-forget)
- **Response caching** -- per-backend TTL-based caching for tool calls and resource reads
- **Request coalescing** -- deduplicate identical concurrent requests

### Security
- **Bearer token auth** -- static token validation
- **JWT/JWKS auth** -- token verification with RBAC (role-based access control)
- **Token passthrough** -- forward client auth tokens to backends
- **Request validation** -- argument size limits

### Observability
- **Prometheus metrics** -- request counts and duration histograms
- **OpenTelemetry tracing** -- distributed trace export via OTLP
- **Audit logging** -- structured logging of all MCP requests
- **Admin API** -- health checks, backend status, cache stats
- **Admin MCP tools** -- introspection tools under `proxy/` namespace

## Installation

### Homebrew

```bash
brew install joshrotenberg/brew/mcp-proxy
```

### Cargo

```bash
cargo install mcp-proxy
```

### Docker

```bash
docker pull ghcr.io/joshrotenberg/mcp-proxy:latest
docker run -v ./proxy.toml:/etc/mcp-proxy/proxy.toml:ro -p 8080:8080 ghcr.io/joshrotenberg/mcp-proxy:latest
```

### Pre-built binaries

Download from [GitHub Releases](https://github.com/joshrotenberg/mcp-proxy/releases).

## Quick Start

Create a `proxy.toml`:

```toml
[proxy]
name = "my-proxy"
separator = "/"

[proxy.listen]
host = "127.0.0.1"
port = 8080

[[backends]]
name = "files"
transport = "stdio"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
```

Run:

```bash
mcp-proxy --config proxy.toml
```

All tools from the filesystem server are now available under the `files/` namespace at `http://127.0.0.1:8080/mcp`.

## Configuration

See [`config.example.toml`](config.example.toml) for the full configuration reference with all options documented.

### Per-backend middleware

```toml
[[backends]]
name = "github"
transport = "stdio"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]

[backends.env]
GITHUB_PERSONAL_ACCESS_TOKEN = "${GITHUB_TOKEN}"

[backends.timeout]
seconds = 60

[backends.rate_limit]
requests = 30
period_seconds = 1

[backends.circuit_breaker]
failure_rate_threshold = 0.5
minimum_calls = 5
wait_duration_seconds = 30

[backends.retry]
max_retries = 3
initial_backoff_ms = 100
max_backoff_ms = 5000
budget_percent = 20.0

[backends.hedging]
delay_ms = 200
max_hedges = 1

[backends.outlier_detection]
consecutive_errors = 5
base_ejection_seconds = 30
max_ejection_percent = 50

[backends.cache]
tool_ttl_seconds = 60
resource_ttl_seconds = 300
```

### Argument injection

```toml
[[backends]]
name = "db"
transport = "http"
url = "http://db.internal:8080"

# Inject into all tool calls for this backend
[backends.default_args]
timeout = 30

# Inject into a specific tool (overrides default_args for matching keys)
[[backends.inject_args]]
tool = "query"
args = { read_only = true, max_rows = 1000 }

# Force overwrite existing arguments
[[backends.inject_args]]
tool = "dangerous_op"
args = { dry_run = true }
overwrite = true
```

### Traffic mirroring

```toml
[[backends]]
name = "api"
transport = "http"
url = "http://api-v1:8080"

[[backends]]
name = "api-v2"
transport = "http"
url = "http://api-v2:8080"
mirror_of = "api"
mirror_percent = 10
```

### Authentication

```toml
# Bearer token
[auth]
type = "bearer"
tokens = ["my-secret-token"]

# Or JWT with RBAC
[auth]
type = "jwt"
issuer = "https://auth.example.com"
audience = "mcp-proxy"
jwks_uri = "https://auth.example.com/.well-known/jwks.json"

[[auth.roles]]
name = "reader"
allow_tools = ["files/read_file", "files/list_directory"]

[[auth.roles]]
name = "admin"

[auth.role_mapping]
claim = "scope"
mapping = { "mcp:read" = "reader", "mcp:admin" = "admin" }
```

### Capability filtering

```toml
[[backends]]
name = "files"
transport = "stdio"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
# Only expose these tools
expose_tools = ["read_file", "list_directory"]
# Or hide specific tools
# hide_tools = ["write_file", "delete_file"]
```

### Endpoint Groups

Endpoint groups create separate MCP endpoints (`/{path}/mcp`) that expose a subset of backends. Useful for role-based tool access, team-specific tool sets, or logical organization.

```toml
# Declare backends with group membership
[[backends]]
name = "context7"
transport = "http"
url = "http://localhost:3001/mcp"
endpoint_groups = ["search", "coding"]   # reverse reference

[[backends]]
name = "tavily"
transport = "http"
url = "http://localhost:3003/mcp"
endpoint_groups = ["search"]

[[backends]]
name = "github"
transport = "http"
url = "http://localhost:3005/mcp"
endpoint_groups = ["coding"]

# Declare endpoint groups
[[proxy.endpoint_groups]]
name = "search"
path = "/search"
backends = ["context7", "tavily"]
description = "Search tools"

[[proxy.endpoint_groups]]
name = "coding"
path = "/coding"
backends = ["context7", "github"]
description = "Coding tools"
```

This creates:
- `/search/mcp` -- context7 + tavily tools
- `/coding/mcp` -- context7 + github tools
- `/mcp` -- all backends (by default)

#### Shorthand syntax

For simple cases where every group should expose all backends, use the array shorthand:

```toml
proxy.endpoint_group_list = ["os", "web"]
```

This auto-creates groups at `/os/mcp` and `/web/mcp` with all enabled backends. Explicit `[[proxy.endpoint_groups]]` entries with the same name override these.

### Shared Backend Pool

Each backend process is spawned exactly **once**, regardless of how many endpoint groups reference it. The proxy builds a single `McpProxy` with all backends, then each endpoint group applies its own middleware stack and namespace filter on top.

```
Client A --> /search/mcp --> [GroupFilter: search, coding] --> McpProxy --> context7 (1 process)
Client B --> /coding/mcp --> [GroupFilter: coding]        --> McpProxy --> tavily   (1 process)
                                                                    --> github  (1 process)
```

This means a backend like `context7` shared between `search` and `coding` groups runs only one process, saving resources and simplifying management.

### Lazy Backend Spawning & Warm Cache

Backends marked `spawn_mode = "lazy"` are **not** spawned at startup. Instead, their tool catalog is served from a persisted **warm cache** on disk, so `tools/list` (and friends) always shows them even while the backend process is dead. The backend is spawned on the first `tools/call` (coalesced across concurrent first-calls), then torn down again once idle.

**Why use it?**

- **Fast startup** — heavy backends (e.g. `uvx`/`npx` servers) no longer block proxy boot.
- **Always-visible catalog** — clients see the full tool list immediately, regardless of spawn state.
- **Resource savings** — idle backends are terminated, freeing processes and memory.

**Enable it** by setting `spawn_mode = "lazy"` on a backend and turning on the warm cache (a top-level `[warm_cache]` section):

```toml
[warm_cache]
enabled = true
dir = "/tmp/mcp-proxy-warm"   # optional; platform default if omitted
ttl_secs = 3600               # 0 = never expire by age

[[backends]]
name = "filesystem"
transport = "stdio"
command = "uvx"
args = ["mcp-server-filesystem", "/tmp"]
spawn_mode = "lazy"
idle_timeout_secs = 600       # terminate after 10 min idle (stateless only)
cache_key_suffix = "v1"       # folded into the cache identity hash
```

**`idle_timeout_secs` semantics (C3):** only meaningful for stateless `2026-07-28` backends. A lazy backend is terminated after this many seconds of inactivity. `None` means never idle-out; `Some(0)` keeps the backend alive. Session-based `2025-11-25` backends are **not** idle-timed-out (their sessions cannot be transparently recreated), so they stay running once spawned.

**`cache_key_suffix`:** an optional string folded into the backend's warm-cache identity hash (computed from the resolved command, args, working directory, and env *keys* — never secret values). Use it to pin a launcher/package version (e.g. an `uvx` package version) that cannot be auto-resolved offline, forcing a cache invalidation when it changes.

**On-demand spawn flow:** a `tools/call` for a down lazy backend triggers a spawn (coalesced so concurrent first-calls share one process), probes its live catalog, and reconciles it against the warm cache. The warm catalog is persisted to disk and **survives restarts** — after a restart the backend is again served from cache without respawn until the next call.

See [`examples/configs/lazy-backend.toml`](examples/configs/lazy-backend.toml) for a complete, runnable-looking example.

### Global Backend Configuration

Reduce config duplication with global defaults applied to all backends:

```toml
[proxy]
name = "my-proxy"

# Global env vars merged into ALL stdio backends
# (per-backend [backends.env] values take precedence)
[proxy.backend_env]
LOG_LEVEL = "ERROR"
MCP_LOG_LEVEL = "ERROR"

# Global timeout applied to all backends
# (per-backend [backends.timeout] overrides this)
[proxy.timeout]
seconds = 30

# Global circuit breaker
[proxy.circuit_breaker]
failure_rate_threshold = 0.5
minimum_calls = 5
wait_duration_seconds = 30

# Global retry policy
[proxy.retry]
max_retries = 3
initial_backoff_ms = 100
max_backoff_ms = 5000
```

### Protocol version support

mcp-proxy supports both MCP 2026-07-28 (stateless) and 2025-11-25 (session-based) protocols simultaneously. Clients auto-negotiate via HTTP headers or WebSocket subprotocol negotiation.

```toml
[proxy.protocol_support]
# Both enabled by default for maximum client compatibility
versions = ["2026-07-28", "2025-11-25"]
# Default version for new connections (optional)
default_protocol_version = "2026-07-28"
```

Per-backend protocol version (for HTTP/WebSocket backends):

```toml
[[backends]]
name = "remote-api"
transport = "http"
url = "http://api.internal:8080"
protocol_version = "2026-07-28"
```

## Library Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
mcp-proxy = "0.1"
```

```rust
use mcp_proxy::{Proxy, ProxyConfig};

let config = ProxyConfig::load("proxy.toml".as_ref())?;
let proxy = Proxy::from_config(config).await?;

// Embed in an existing axum app
let (router, session_handle) = proxy.into_router();

// Or serve standalone
proxy.serve().await?;
```

## Admin API

HTTP endpoints:

- `GET /admin/backends` -- list backends with health status and proxy info
- `GET /admin/health` -- health check summary (healthy/degraded)
- `GET /admin/metrics` -- Prometheus metrics
- `GET /admin/cache/stats` -- per-backend cache hit/miss rates
- `POST /admin/cache/clear` -- clear all caches

MCP tools (under `proxy/` namespace):

- `proxy/list_backends` -- list backends with health status
- `proxy/health_check` -- cached health check results
- `proxy/session_count` -- active session count
- `proxy/add_backend` -- dynamically add an HTTP backend
- `proxy/config` -- dump current config

## Architecture

### Middleware stack

```
Global (wraps entire proxy):
  Auth -> Audit -> Access Log -> Metrics -> Token Passthrough -> RBAC
  -> Client Rate Limit -> Alias -> Filter -> Validation -> Coalesce -> Cache
  -> Mirror -> Inject Args -> Discover -> MetaValidation -> McpProxy

Per-backend (applied individually):
  Retry -> Hedge -> Concurrency -> Rate Limit
  -> Timeout -> Circuit Breaker -> Outlier Detection -> Backend

Per-endpoint-group (on top of shared McpProxy):
  GroupFilter -> [group-level middleware] -> GroupRouter
```

Global middleware wraps the entire proxy. Per-backend middleware is applied individually to each backend connection. Endpoint group middleware adds a namespace filter so each group only sees its member backends' tools. All middleware is built with tower `Service` layers.

## Feature Flags

Pre-built binaries and `cargo install` include all features by default. If you're building from source and don't need everything, you can disable optional features for a smaller binary:

| Feature | Default | What it includes |
|---------|---------|-----------------|
| `otel` | yes | OpenTelemetry distributed tracing (OTLP export) |
| `metrics` | yes | Prometheus metrics and `/admin/metrics` endpoint |
| `oauth` | yes | JWT/JWKS auth, RBAC, and token passthrough |

```bash
# Minimal build (bearer auth only, no metrics/tracing/JWT)
cargo install mcp-proxy --no-default-features

# Just metrics, no otel or JWT
cargo install mcp-proxy --no-default-features --features metrics
```

Config parsing always works regardless of features -- if you reference a disabled feature in your config (e.g., `type = "jwt"` without the `oauth` feature), you'll get a clear error at startup.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
