---
title: "ADR-0005: Idle Lifecycle and Concurrency"
status: "Proposed"
date: "2026-08-24"
authors: "mcp-proxy maintainers"
tags: ["architecture", "decision", "lifecycle", "lazy-spawn"]
supersedes: ""
superseded_by: ""
---

## Status

**Proposed**

## Context

Lazy-spawned backends should release their child process when idle to bound resource use, then respawn on demand. Idle-out must route through tower-mcp: `proxy.remove_backend(name).await` (service.rs:~640) → `McpClient::Drop` (mod.rs:2667) → `StdioClientTransport::close` kills the child (stdio.rs:216-247). Concurrent first-calls for the same namespace must not spawn duplicate processes. Protocol matters: stateless `2026-07-28` is safe to respawn (no session state); session-based `2025-11-25` loses state on respawn.

## Decision

Guard each namespace with a `OnceCell` spawn lock so the first live request spawns exactly once; concurrent callers await the same in-flight future. A `tokio` idle-sweep task tracks last-use and calls `remove_backend` after the idle timeout. Idle-out is enabled for stateless `2026-07-28`; for session-based `2025-11-25` the backend is kept alive (keep-alive) to preserve session state.

## Alternatives

- **No idle-out**: simplest, but unbounded process count defeats the resource goal. Rejected.
- **Refcounted idle**: precise but requires hooking every request path; `OnceCell` + sweep is simpler. Rejected.

## Consequences

- **POS-001**: Bounded process count; idle backends free OS resources.
- **POS-002**: `OnceCell` prevents duplicate spawns under concurrency.
- **POS-003**: Stateless/session split keeps `2025-11-25` sessions intact.
- **NEG-001**: Idle sweep adds a background task and timing tuning.
- **NEG-002**: Respawn latency on the next call after idle-out.
