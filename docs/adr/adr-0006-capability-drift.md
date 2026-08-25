---
title: "ADR-0006: Capability Drift Reconciliation"
status: "Proposed"
date: "2026-08-24"
authors: "mcp-proxy maintainers"
tags: ["architecture", "decision", "capabilities", "lazy-spawn"]
supersedes: ""
superseded_by: ""
---

## Status

**Proposed**

## Context

A persisted warm catalog (see adr-0003-warm-catalog-persistence.md) can drift from the live server: the server may change between probe and use, or the hash may collide across semantically different builds. Clients subscribed to `tools/list_changed` must see the true current set. The leaf capability types are `pub` + serde (backend.rs:27-32), so live and persisted sets are directly comparable.

## Decision

On every lazy spawn, reconcile the live `list_all_*` (mod.rs:1734-1779) result against the persisted catalog. If they differ, update the persisted `WarmCatalog` and re-emit `notifications/tools/list_changed` to subscribers. Persisted data is the cold-start source of truth; live data is the authoritative source after spawn.

## Alternatives

- **Trust persisted only**: fast but serves stale tools after a server change; breaks client caches. Rejected.
- **Trust live-only**: ignores the warm cache's purpose (serve lists with no process); regresses to always-probe. Rejected.

## Consequences

- **POS-001**: Clients always converge to the true capability set.
- **POS-002**: Drift is self-healing on the next spawn.
- **NEG-001**: Extra `list_all_*` call per spawn (already needed to connect).
- **NEG-002**: `list_changed` churn if a server is non-deterministic across spawns.
