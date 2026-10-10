# mcp-proxy — lazy backend lifecycle fork

A config-driven [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) reverse proxy in Rust. It aggregates multiple MCP backends behind a single endpoint with per-backend middleware, authentication, and observability. Built on [tower-mcp](https://github.com/joshrotenberg/tower-mcp) and the [tower](https://github.com/tower-rs/tower) middleware ecosystem.

This repository continues [joshrotenberg/mcp-proxy](https://github.com/joshrotenberg/mcp-proxy) by Josh Rotenberg with a line of work on **backend lifecycle**. Three changes define it: backends are no longer spawned at startup, their tool catalogs are served from disk while the processes are dead, and each backend runs at most one process no matter how many endpoints expose it.

## What this repository adds

### Lazy backend spawning with a persistent warm catalog

A proxy fronting sixteen `npx` and `uvx` servers pays all sixteen cold starts before it can answer one request. Worse, a backend that is not running vanishes from `tools/list`, so the client cannot see a tool it is entitled to call.

Backends marked `spawn_mode = "lazy"` are not spawned at boot. Their catalog is probed once, hashed, and persisted to disk, so `tools/list`, `resources/list`, `resource_templates/list`, and `prompts/list` are answered with **no process running**. The first `tools/call` spawns the backend; concurrent first-calls await the same in-flight future behind a `OnceCell` lock, so exactly one process starts. The catalog survives restarts, and on every spawn the live capability set is reconciled against the persisted one; a drifted catalog is updated and persisted (tower-mcp has no public way to re-broadcast `list_changed` after the fact, so clients see the change on their next list).

Two details that took the most thought:

- **Cache identity** is a SHA-256 over the resolved command, args, working directory, and environment variable *keys* — never their values, so no secret reaches the hash or the disk. Hashing `command` alone is useless here: for a launcher like `npx` or `uvx` it is byte-identical across completely unrelated servers. An optional `cache_key_suffix` pins a launcher package version that cannot be resolved offline.
- **Idle teardown is a per-backend policy.** By default (`idle_teardown = "stateless"`) only stateless `2026-07-28` backends are stopped after `idle_timeout_secs` of inactivity; session-based backends stay up, because their session state cannot be transparently recreated. `idle_teardown = "always"` also stops session-based backends, for servers whose state is cheap to lose. Terminal and browser backends should stay on `stateless`.

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

The lazy path held up under unit tests and then broke in several ways against real backends. Each fix landed with a regression test that fails against the previous code:

1. **Endpoint groups containing only lazy stdio backends exposed zero tools.** The warm-catalog append matched namespaces by splitting the tool name on the separator, but the stored prefix already included the trailing separator, so every append silently no-opped. Replaced with a prefix match.
2. **Backends without `resources/list` got no warm catalog at all.** The probe aborted on the first capability-listing error, so a server exposing tools but not resources ended up with an empty catalog and disappeared from its group. Resource, template, and prompt listings are now best-effort; only `tools` is treated as critical.
3. **`tools/call` returned "Unknown tool" when a backend name contained the separator.** Resolution took the first token of the split name, so `electron_cdp_start_app` resolved to backend `electron`, missed the registry, skipped the spawn path, and failed. Replaced with longest-prefix match against registered backends.

4. **A backend that answered `initialize` with an unsupported protocol version was reported as a spawn conflict** and lost its warm catalog. Only a closed connection now counts as an immediate exit; a protocol rejection reports "protocol version not supported".
5. **A config change on a running lazy backend left the old child running untracked**, and a config change during an in-flight spawn could leave two children alive. Reconcile now stops the running child first and serializes with in-flight spawns.

Many deployed stdio servers still answer `initialize` with MCP `2024-11-05`, which tower-mcp 0.23 rejects. The proxy opts in to accepting it (startup, hot reload, lazy spawn, admin add and the warm-catalog probe).

## Design records

The reasoning behind the lazy lifecycle is written down in [`docs/adr/`](docs/adr/) rather than left in commit messages:

- [Warm catalog persistence and invalidation](docs/adr/adr-0003-warm-catalog-persistence.md) — why the catalog is mirrored locally instead of reusing tower-mcp's cache type (it is `pub(crate)`, the leaf types are not), what belongs in the identity hash, and why script content is excluded from it.
- [Lazy spawn integration](docs/adr/adr-0004-lazy-spawn-integration.md) — why this was built on the existing public `add_backend` surface first, deferring a tower-mcp fork rather than starting with one.
- [Idle lifecycle and concurrency](docs/adr/adr-0005-idle-lifecycle-concurrency.md) — single-spawn guarantee under concurrent first-calls, and why idle teardown is enabled for stateless backends only.
- [Capability drift reconciliation](docs/adr/adr-0006-capability-drift.md) — persisted data is the cold-start source of truth, live data is authoritative after spawn, and drift self-heals on the next spawn.
- [Idle teardown opt-in](docs/adr/adr-0007-idle-teardown-opt-in.md) — the per-backend `idle_teardown` policy for session-based backends (Accepted; amends ADR-0005).

ADR-0003 to ADR-0006 are still marked *Proposed*: the implementation landed, the records were written for an upstream review that has not happened.

## Relationship to upstream

Upstream is [joshrotenberg/mcp-proxy](https://github.com/joshrotenberg/mcp-proxy) by Josh Rotenberg, dual-licensed MIT / Apache-2.0. This repository is maintained independently and is not affiliated with upstream. Its history starts from upstream's `v0.6.0` tag, so every upstream commit up to that release keeps its original author; the work described above sits on top as a short series of commits.

It builds against [thexmeta/tower-mcp](https://github.com/thexmeta/tower-mcp/commits/mcp-proxy-patches), a fork of Josh Rotenberg's [tower-mcp](https://github.com/joshrotenberg/tower-mcp) 0.23.2 with nine small patches, one commit each: stdio children are killed when their transport is dropped and are stopped with SIGTERM before SIGKILL, backend initialization can time out, a proxy may start with no backends, HTTP clients can be built from a caller's client and config, transports can reconnect and HTTP stays connected after transient errors, a dropped client sends shutdown, and accepting MCP `2024-11-05` is an opt-in switch. `Cargo.toml` pins it with `[patch.crates-io]`.

Related: [mcp-migration-check](https://github.com/AlpayC/mcp-migration-check) lints MCP servers for protocol migration gaps. Its `MCP010` rule recommends the `tower-mcp` `protocol-2026-07-28` upgrade, the same upgrade this repository performs.

## Building

The crates.io crate, Homebrew formula and container image published upstream do **not** include this work, and this repository publishes none of them. Build from source (Rust 1.97 or newer):

```bash
git clone https://github.com/thexmeta/mcp-proxy.git
cd mcp-proxy
cargo build --release --locked
```

Or install the binary with `cargo install --git https://github.com/thexmeta/mcp-proxy --locked`.

### Debian package and systemd

`scripts/build-deb.sh` builds a `.deb` with [nfpm](https://nfpm.goreleaser.com/). The packaged unit runs as a transient system user and reads `/etc/mcp-proxy/config.toml`. Put site settings (another user, a secret-loading `ExecStartPre`, extra `ReadWritePaths`) in a drop-in such as `/etc/systemd/system/mcp-proxy.service.d/40-local.conf`; it survives package upgrades:

```ini
[Service]
DynamicUser=no
User=alice
ExecStart=
ExecStart=/usr/bin/mcp-proxy --config /home/alice/.mcp-proxy/config.toml
ProtectHome=read-only
ReadWritePaths=/home/alice/.mcp-proxy
```

### Container image

```bash
docker build -t mcp-proxy .
docker run -v ./proxy.toml:/etc/mcp-proxy/proxy.toml:ro -p 8080:8080 mcp-proxy
```

---

Everything below is upstream's reference documentation, updated where this repository behaves differently.

> **Project status:** maintained with an intentionally stable scope. mcp-proxy
> is a tower-native gateway and reference deployment for aggregating MCP
> backends. Maintenance focuses on security, dependency and protocol updates,
> bug fixes, and documentation rather than speculative new features.

## Where it fits

- **Self-hosted internal MCP fleets** -- one authenticated, observable endpoint in front of the MCP servers a team already runs.
- **Non-Kubernetes and mixed deployments** -- a single binary and a TOML file; no service mesh, operator, or container platform required.
- **Rust applications that need a gateway** -- the same proxy is a library; mount it in an existing axum app or drive it from a builder.

For how mcp-proxy relates to other MCP gateways (Docker MCP Gateway, IBM ContextForge, Microsoft MCP Gateway, Kong), see [docs/comparison.md](docs/comparison.md).

## What it does

**Aggregates many MCP servers behind one endpoint.** Backends speaking stdio, HTTP, or WebSocket are exposed under per-backend namespaces at a single HTTP endpoint. Tools, resources, and prompts can be allow/deny filtered and aliased per backend; default or per-tool arguments can be injected into calls; composite tools fan one call out across multiple backend tools; hot reload adds new backends from config changes without a restart; and optional BM25 discovery can expose search instead of full tool lists.

**Contains backend failures.** Each backend gets its own resilience chain: timeouts, rate limits, concurrency caps, retries with exponential backoff and budgets, circuit breakers, request hedging, and outlier detection that temporarily ejects unhealthy backends.

**Manages traffic for rollouts and load.** Traffic mirroring shadows a percentage of requests to a canary backend; canary routing and ordered failover control weighted rollouts; response caching (in-memory, Redis, or SQLite) and request coalescing cut duplicate work.

**Applies policy at the front door.** Bearer token, JWT/JWKS, and OAuth 2.1 authentication; role-based tool visibility (RBAC); token passthrough to backends; and request argument validation.

**Reports what is happening.** Prometheus metrics, OpenTelemetry trace export, structured audit logging, an admin HTTP API for health, backend status, and cache stats, and admin MCP tools under the `proxy/` namespace.

Every option is documented in [`config.example.toml`](config.example.toml), deployment shapes in [`docs/architectures.md`](docs/architectures.md), and runnable configurations in [`examples/`](examples/).

## Installation

See [Building](#building) above.

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

All tools from the filesystem server are now available under the `files/` namespace at `http://127.0.0.1:8080/`.
The standalone MCP endpoint is `/`; external backends may expose their own
endpoint at a different path, such as `/mcp`, which belongs in that backend's `url`.

## Configuration

See [`config.example.toml`](config.example.toml) for the full configuration reference with all options documented.

For a complete production example (ten backends behind one endpoint, with JWT/RBAC, per-backend resilience, a mirrored canary, selective caching, and metrics), see [docs/production-gateway.md](docs/production-gateway.md) and [`examples/production-gateway.toml`](examples/production-gateway.toml).

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

### Backend headers and tool discovery

HTTP backends (including SSE responses) and WebSocket handshakes can use custom
outbound headers. Values support the same environment substitution as bearer tokens:

```toml
[[backends]]
name = "api"
transport = "http"
url = "https://mcp.example.com"
headers = { "X-API-Key" = "${API_KEY}" }
```

An explicit `Authorization` header overrides `bearer_token` regardless of casing.
Rust callers upgrading from 0.4 to 0.5 who construct `BackendConfig` literals
need to add `headers: Default::default()`; TOML and YAML configs can omit the map.
Invalid or duplicate header names, unknown backend fields, and headers configured
on stdio backends fail configuration validation.

With `[proxy] tool_exposure = "search"`, discover a tool through
`proxy/search_tools`, pass its returned `id` to `proxy/get_tool` to retrieve its
name, description, and complete input schema, then invoke it through
`proxy/call_tool`. This keeps full schemas out of search result lists while making
nested parameters and enums available before execution.

### Upgrading to 0.6

mcp-proxy 0.6 and this repository use tower-mcp 0.23. Library applications that exchange
`SessionHandle`, `McpProxy`, router requests/responses, or protocol types with
mcp-proxy should update their direct tower-mcp dependencies to 0.23 as well.

HTTP backends follow same-origin redirects only. A redirect to another scheme,
host, or port fails instead of forwarding custom headers or session identifiers;
HTTPS-to-HTTP redirects also fail. Configure the backend's final URL directly
if the previous deployment depended on a cross-origin redirect.

JWT/OAuth-authenticated HTTP sessions carrying a `sub` claim are bound to that
subject. Clients must initialize their own session and authenticate subsequent
requests as the same subject.

### Authentication

```toml
# Bearer token
[auth]
type = "bearer"
tokens = ["my-secret-token"]
```

Or JWT with RBAC:

```toml
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
idle_timeout_secs = 600       # stop after 10 min idle
idle_teardown = "stateless"   # default; "always" also stops session-based backends
cache_key_suffix = "v1"       # folded into the cache identity hash
```

**`idle_timeout_secs` and `idle_teardown`:** a lazy backend is stopped after `idle_timeout_secs` of inactivity, unset means never. With the default `idle_teardown = "stateless"` only backends that negotiated `2026-07-28` are stopped; session-based backends stay running once spawned, because their sessions cannot be transparently recreated. `idle_teardown = "always"` stops them too and loses their session state. `[proxy] default_idle_teardown` sets the default for all backends. In-flight requests always keep a backend up.

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

mcp-proxy supports both MCP 2026-07-28 (stateless) and 2025-11-25 (session-based) protocols simultaneously. Clients auto-negotiate via HTTP headers or WebSocket subprotocol negotiation. Backends that answer `initialize` with 2025-06-18, 2025-03-26 or 2024-11-05 are accepted too.

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
mcp-proxy = { git = "https://github.com/thexmeta/mcp-proxy" }

# [patch] is not inherited from dependencies: repeat this repository's pin.
[patch.crates-io]
tower-mcp = { git = "https://github.com/thexmeta/tower-mcp", rev = "679523389d4544c97bae5c9e5468a9e9e5fd71f7" }
tower-mcp-types = { git = "https://github.com/thexmeta/tower-mcp", rev = "679523389d4544c97bae5c9e5468a9e9e5fd71f7" }
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

Process probes at `GET /livez` and `GET /readyz` return `ok` without credentials.
Readiness indicates startup completed; detailed backend health remains at the
protected `/admin/health` endpoint. For Kubernetes deployment and release version
maintenance, see the [Helm chart guide](charts/mcp-proxy/README.md).

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

For an application-owned decision service, see [external tool gates](docs/external-tool-gates.md) for interception points, policy ordering, and a bounded non-production pilot.

## Feature Flags

A default build includes the default features. If you're building from source and don't need everything, you can disable optional features for a smaller binary:

| Feature | Default | What it includes |
|---------|---------|-----------------|
| `otel` | yes | OpenTelemetry distributed tracing (OTLP export) |
| `metrics` | yes | Prometheus metrics and `/admin/metrics` endpoint |
| `oauth` | yes | JWT/JWKS auth, RBAC, and token passthrough |
| `openapi` | yes | OpenAPI schema and endpoint support |
| `websocket` | yes | WebSocket backend transport |
| `discovery` | yes | BM25 tool discovery and search exposure mode |
| `yaml` | yes | YAML configuration files using the maintained `yaml_serde` parser |
| `skills` | yes | agentskills.io prompts for proxy administration |
| `redis-cache` | no | Shared Redis response cache |
| `sqlite-cache` | no | Persistent SQLite response cache |
| `protocol-2026-07-28` | no | Released MCP 2026-07-28 protocol support through tower-mcp |

```bash
# Minimal build (bearer auth only, no metrics/tracing/JWT)
cargo install --git https://github.com/thexmeta/mcp-proxy --locked --no-default-features

# Just metrics, no otel or JWT
cargo install --git https://github.com/thexmeta/mcp-proxy --locked --no-default-features --features metrics
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
# args = [ "/", "-d", "...", "--allow-write"]

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

### Stale singleton daemon blocks spawn (spawn conflict)

If a `tools/call` fails with a JSON-RPC `invalid_params` (`-32602`) error
containing `spawn conflict` naming a backend, a foreign daemonized singleton
(e.g. `codebase-memory-mcp`) is holding the slot and the freshly spawned child
exited immediately during `initialize`. **The proxy never kills foreign
processes** — resolve it from the docs/error alone, no PID hunting needed:

1. Read the error's `data.hint`: for a flagged singleton it names the advisory
   stop command (e.g. `systemctl --user stop codebase-memory`). Run it, then
   retry the call — the failed spawn leaves the backend `Down`, so the next
   call spawns fresh.
2. If the backend is an independently-daemonized singleton the proxy does not
   own, declare it so the error carries the stop command next time:

```toml
[[backends]]
name = "codebase"
transport = "stdio"
command = "/usr/bin/codebase-memory-mcp"
singleton_externally_managed = true   # proxy will never signal this process
singleton_stop_hint = "systemctl --user stop codebase-memory"  # advisory only, never executed
```

3. If a backend is slow to exit and blocks restarts, opt it into immediate
   kill instead (see kill modes below). Never `kill` by cmdline scan yourself
   for proxy-owned children — restart/shutdown already terminates them.

#### Child kill modes (`force_kill`)

Per-backend `force_kill` (default `false`) selects how the proxy terminates
**its own tracked children** on restart, shutdown, and hot-reload remove/replace:

- `false` (default, safe): bounded graceful shutdown — EOF/SIGTERM, then
  SIGKILL fallback after `shutdown_kill_timeout_secs` (default `2`).
- `true`: SIGKILL immediately, no grace wait. Use only for backends that do
  not need flush time; abrupt SIGKILL can lose in-flight writes.

Effective mode is `proxy.force_kill OR backend.force_kill`, resolved once at
spawn time (global `true` forces immediate kill for every backend):

```toml
[proxy]
shutdown_kill_timeout_secs = 2   # grace window for force_kill = false
# force_kill = true              # opt-in: SIGKILL-immediate for ALL backends

[[backends]]
name = "slow-indexer"
transport = "stdio"
command = "uvx"
args = ["mcp-server-filesystem", "/tmp"]
force_kill = true                # opt-in: SIGKILL-immediate for this backend only
```

Ownership boundary: `force_kill` applies only to proxy-owned stdio children
(eager + spawned lazy backends). Backends flagged
`singleton_externally_managed = true` are **never signalled, regardless of
`force_kill`** — they are skipped at shutdown with a log naming the stop hint.

#### Warm-catalog invalidation rule

The warm catalog can never mask a known-broken backend:

- **On restart/shutdown**, the terminated backends' catalog files are deleted,
  so the next startup re-probes fresh instead of serving the pre-restart
  catalog. Externally-managed singletons are skipped (nothing was terminated),
  so their catalogs are kept.
- **On a proven spawn conflict** (a fast `Connection closed` before
  `init_timeout`, never a timeout or an answered handshake), the stale catalog is
  invalidated with no fuzzy-fallback rescue, and `tools/list` omits the broken
  backend until a fresh probe succeeds.
- Transient/timeout probe errors keep the existing degrade path (fuzzy fallback
  + retained catalog).
- A backend that answers `initialize` with a protocol version the proxy does
  not accept fails with `protocol version not supported` and keeps its catalog.

This build always includes MCP 2026-07-28 support alongside 2025-11-25; the
`protocol-2026-07-28` feature is kept for compatibility and changes nothing.
Continuation fields such as `inputResponses` and `requestState` are preserved
through proxy routing.

## Credits

mcp-proxy was created by Josh Rotenberg ([joshrotenberg/mcp-proxy](https://github.com/joshrotenberg/mcp-proxy)); everything up to its `v0.6.0` release is his work and that of its contributors. The additions listed under [What this repository adds](#what-this-repository-adds) and the tower-mcp patches are by Eser Kelleci. Both are available under the same MIT OR Apache-2.0 terms; see `LICENSE-MIT`, `LICENSE-APACHE` and [`NOTICE`](NOTICE).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
