# mcp-proxy — lazy backend lifecycle fork

A config-driven [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) reverse proxy in Rust. It aggregates multiple MCP backends behind a single endpoint with per-backend middleware, authentication, and observability. Built on [tower-mcp](https://github.com/joshrotenberg/tower-mcp) and the [tower](https://github.com/tower-rs/tower) middleware ecosystem.

This is a fork of [joshrotenberg/mcp-proxy](https://github.com/joshrotenberg/mcp-proxy) carrying an unreleased line of work on **backend lifecycle**. Three changes define it: backends are no longer spawned at startup, their tool catalogs are served from disk while the processes are dead, and each backend runs at most one process no matter how many endpoints expose it.

Measured against `upstream/main`: 56 source files changed, +13,512 lines; 12 new test files, +9,888 lines of tests. Version in this tree is `1.4.2-b3`, unreleased.

## What this fork adds

### Lazy backend spawning with a persistent warm catalog

A proxy fronting sixteen `npx` and `uvx` servers pays all sixteen cold starts before it can answer one request. Worse, a backend that is not running vanishes from `tools/list`, so the client cannot see a tool it is entitled to call.

Backends marked `spawn_mode = "lazy"` are not spawned at boot. Their catalog is probed once, hashed, and persisted to disk, so `tools/list`, `resources/list`, `resource_templates/list`, and `prompts/list` are answered with **no process running**. The first `tools/call` spawns the backend; concurrent first-calls await the same in-flight future behind a `OnceCell` lock, so exactly one process starts. The catalog survives restarts, and on every spawn the live capability set is reconciled against the persisted one, with `notifications/tools/list_changed` re-emitted on drift.

Two details that took the most thought:

- **Cache identity** is a SHA-256 over the resolved command, args, working directory, and environment variable *keys* — never their values, so no secret reaches the hash or the disk. Hashing `command` alone is useless here: for a launcher like `npx` or `uvx` it is byte-identical across completely unrelated servers. An optional `cache_key_suffix` pins a launcher package version that cannot be resolved offline.
- **Idle teardown is protocol-dependent.** Stateless `2026-07-28` backends are terminated after `idle_timeout_secs` of inactivity. Session-based `2025-11-25` backends are deliberately kept alive, because their session state cannot be transparently recreated and silently dropping it would surface as an unexplained failure in the client.

See [Lazy Backend Spawning & Warm Cache](#lazy-backend-spawning--warm-cache) for configuration.

### Shared backend pool

Each backend process is spawned exactly once, regardless of how many endpoint groups reference it. The proxy builds one `McpProxy` holding every backend, and each endpoint group layers its own middleware stack and namespace filter on top rather than owning its own connection. A backend shared across three groups is three routes to one process, not three processes.

### Endpoint groups with namespace-isolated filtering

Endpoint groups expose a subset of backends at their own MCP endpoint (`/{path}/mcp`) — role-scoped tool sets without running a second proxy. Membership can be declared from either side (on the group, or as a reverse reference on the backend), a group-aware capability filter enforces the namespace boundary, and a shorthand form (`proxy.endpoint_group_list = ["os", "web"]`) covers the common case where a group exposes everything.

### Protocol and traffic work

- **MCP `2026-07-28` support** alongside `2025-11-25`, negotiated per connection: stateless operation, per-request `_meta`, `server/discover`, and `subscriptions/listen`.
- **Per-client-identity rate limiting** keyed on `_meta.clientInfo.name`, so one misbehaving client cannot consume a shared backend's budget.
- **Global backend defaults** (`[proxy.backend_env]`, `[proxy.timeout]`, `[proxy.circuit_breaker]`, `[proxy.retry]`) with per-backend overrides.

### Correctness fixes found by running it

The lazy path held up under unit tests and then broke in three ways against sixteen real backends. Each fix landed with a regression test that fails against the previous code:

1. **Endpoint groups containing only lazy stdio backends exposed zero tools.** The warm-catalog append matched namespaces by splitting the tool name on the separator, but the stored prefix already included the trailing separator, so every append silently no-opped. Replaced with a prefix match.
2. **Backends without `resources/list` got no warm catalog at all.** The probe aborted on the first capability-listing error, so a server exposing tools but not resources ended up with an empty catalog and disappeared from its group. Resource, template, and prompt listings are now best-effort; only `tools` is treated as critical.
3. **`tools/call` returned "Unknown tool" when a backend name contained the separator.** Resolution took the first token of the split name, so `electron_cdp_start_app` resolved to backend `electron`, missed the registry, skipped the spawn path, and failed. Replaced with longest-prefix match against registered backends.

All three are being filed upstream as issues with their regression tests attached.

## Design records

The reasoning behind the lazy lifecycle is written down in [`docs/adr/`](docs/adr/) rather than left in commit messages:

- [Warm catalog persistence and invalidation](docs/adr/adr-0003-warm-catalog-persistence.md) — why the catalog is mirrored locally instead of reusing tower-mcp's cache type (it is `pub(crate)`, the leaf types are not), what belongs in the identity hash, and why script content is excluded from it.
- [Lazy spawn integration](docs/adr/adr-0004-lazy-spawn-integration.md) — why this was built on the existing public `add_backend` surface first, deferring a tower-mcp fork rather than starting with one.
- [Idle lifecycle and concurrency](docs/adr/adr-0005-idle-lifecycle-concurrency.md) — single-spawn guarantee under concurrent first-calls, and why idle teardown is enabled for stateless backends only.
- [Capability drift reconciliation](docs/adr/adr-0006-capability-drift.md) — persisted data is the cold-start source of truth, live data is authoritative after spawn, and drift self-heals on the next spawn.

All four are still marked *Proposed*: the implementation landed, the records have not been ratified by upstream.

## Relationship to upstream

Upstream is [joshrotenberg/mcp-proxy](https://github.com/joshrotenberg/mcp-proxy), dual-licensed MIT / Apache-2.0, and remains the place to get a released build.

This fork's default branch is `fork/main` and it does **not** descend from upstream's history. The tree was reconstructed from editor local history after the original checkout was lost, so its root commit is a recovery snapshot with no common ancestor upstream. The work is intact and the diff against `upstream/main` is meaningful; the commit graph simply cannot be replayed onto it. Upstream contributions are therefore prepared as single clean commits branched fresh off `upstream/main`, not as merges from this branch.

Related: [mcp-migration-check](https://github.com/AlpayC/mcp-migration-check) lints MCP servers for protocol migration gaps. Its `MCP010` rule recommends the `tower-mcp` `protocol-2026-07-28` upgrade — the same upgrade this fork performs.

## Building this fork

The published crate and the Homebrew and Docker artifacts below are upstream's and do **not** include this work. To run this tree:

```bash
git clone -b fork/main https://github.com/thexmeta/mcp-proxy.git
cd mcp-proxy
cargo build --release
```

---

Everything below is the upstream reference documentation, kept as-is.

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

## Troubleshooting

### EROFS (Read-only file system) errors

If you see `Read-only file system (os error 30)` when a backend tries to write files, this is likely caused by systemd sandboxing.

**Symptom:**
```
fs_write_file({"path": "/home/user/Desktop/test.txt", "content": "test"})
→ "Read-only file system (os error 30)"
```

**Cause:**

When running under systemd with `ProtectSystem=strict`, the root filesystem `/` is mounted read-only. Backends using `rust-mcp-filesystem` with `allowed_directories = ["/"]` will fail because cap-std opens `/` as a `Dir` capability and cannot traverse mount boundaries to reach writable paths.

**Detection:**

mcp-proxy detects this at startup and logs warnings:
```
WARN Backend has root directory '/' as allowed path, but root filesystem is read-only (likely ProtectSystem=strict). This will cause EROFS errors. Fix: change allowed path to a writable directory like '/home/<user>' or add ReadWritePaths to the systemd unit.
```

**Fix:**

Option A: Change the backend's allowed directory to a writable path:
```toml
# Before (fails with EROFS):
args = [ "/", "-d", "...", "--allow-write"]

# After (works):
args = [ "/home/user", "-d", "...", "--allow-write"]
```

Option B: Add the path to `ReadWritePaths` in the systemd unit:
```ini
[Service]
ProtectSystem=strict
ReadWritePaths=/home/user
```

Option C: Remove `ProtectSystem=strict` (not recommended for production).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
