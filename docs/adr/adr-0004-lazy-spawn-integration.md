---
title: "ADR-0004: Lazy Spawn Integration Strategy"
status: "Proposed"
date: "2026-08-24"
authors: "mcp-proxy maintainers"
tags: ["architecture", "decision", "tower-mcp", "lazy-spawn"]
supersedes: ""
superseded_by: ""
---

## Status

**Proposed**

## Context

The proxy must defer spawning a stdio child until the first `tools/call`, `resources/read`, or `prompts/get`. Today spawning is eager: `build_mcp_proxy_for_backends` (proxy.rs:104-114) → `builder.backend(name, transport)` (proxy.rs:113); `from_config` (proxy.rs:400-416); hot-reload `reload.rs:703`. tower-mcp's `McpProxyBuilder::backend()` (builder.rs:200) takes an already-spawned `ClientTransport` and connects immediately. The routing table `entries: Arc<Mutex<Vec<BackendEntry>>>` is `pub(super)` and `BackendEntry` is `pub(crate)` (backend.rs), so the repo cannot inject a lazy service into tower-mcp routing without a fork.

## Decision

Integrate in-repo first (milestones M1–M7): register backends via the existing public `McpProxy::add_backend` (service.rs:215) at first live request, serving lists from the warm catalog (see adr-0003-warm-catalog-persistence.md). Defer a tower-mcp fork (M8) that adds a lazy `add_backend` + `preload_cache` factory. The in-repo path uses `spawn_command` (stdio.rs:102) → `connect_with_handler` (mod.rs:713) → `initialize` (mod.rs:884) on demand.

## Alternatives

- **Fork tower-mcp now**: cleanest API but blocks on upstream release cadence; high risk. Rejected for M1–M7.
- **Replicate `BackendEntry`**: duplicates private state, breaks on tower-mcp bumps. Rejected.
- **Wrapper `BoxCloneService`**: cannot reach the routing table (`pub(super)`). Rejected.

## Consequences

- **POS-001**: No fork required to ship lazy spawning.
- **POS-002**: Reuses the already-exercised `add_backend` path (reload.rs, admin.rs).
- **NEG-001**: First live call pays spawn + initialize latency.
- **NEG-002**: Fork work (M8) remains to remove probe duplication.
