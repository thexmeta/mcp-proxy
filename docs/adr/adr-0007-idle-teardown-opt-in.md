---
title: "ADR-0007: Opt-in Idle Teardown for Session-based Backends"
status: "Accepted"
date: "2026-10-10"
authors: "Eser Kelleci (@thexmeta) <eserkelleci@gmail.com>"
tags: ["architecture", "decision", "lifecycle", "lazy-spawn"]
supersedes: ""
superseded_by: ""
---

## Status

**Accepted.** Amends [ADR-0005](adr-0005-idle-lifecycle-concurrency.md).

## Context

ADR-0005 lets the idle sweeper stop a lazy backend only when it negotiated the stateless `2026-07-28` protocol. Every other backend is kept alive to protect its session state.

In practice no deployed stdio backend negotiates `2026-07-28` yet. A live check on 2026-10-10 found the warm catalogs of 16 backends at `2024-11-05`, `2025-03-26`, `2025-06-18` and `2025-11-25`, none at `2026-07-28`, and zero idle-out events in 30 days of logs. The idle timeout therefore never stops anything, and the resource goal of ADR-0005 is not met.

Some session-based backends hold state worth keeping (terminal sessions, browser tabs). Others hold only caches they rebuild on start. The proxy cannot tell which from the protocol version alone.

## Decision

Add an idle-teardown policy with two values:

- `stateless` (default): current behaviour. Stop the backend only if it negotiated `2026-07-28`.
- `always`: also stop session-based backends, and backends whose version is unknown, after `idle_timeout_secs`. Their session state is lost; the next call spawns a fresh child and starts a new session.

The policy is set per backend with `idle_teardown`, or proxy-wide with `[proxy] default_idle_teardown`. A backend without the key inherits the proxy default, like `spawn_mode` and `idle_timeout_secs`. The value is part of the hot-reload fingerprint, so a change is picked up without a restart. The idle-out log line names the negotiated version and the policy.

The opt-in lifts only the protocol guard. The other guards stay: the backend must be up, have no in-flight requests, and have a non-zero `idle_timeout_secs` that has expired.

## Alternatives

- **Detect stateless servers through `server/discover`**: correct in principle, but no current backend speaks `2026-07-28`, so it changes nothing today. Rejected for now; it remains compatible with this policy.
- **Make `always` the default**: frees the most resources, but silently drops terminal and browser sessions for every user. Rejected.

## Consequences

- **POS-001**: Idle session-based backends can release their processes when the user accepts the trade.
- **POS-002**: The default keeps ADR-0005 behaviour; existing configs are unchanged.
- **NEG-001**: An opted-in backend loses its session on idle-out. Clients holding session handles (terminal ids, page ids) get errors after respawn. Keep such backends on `stateless`.
- **NEG-002**: Respawn latency on the next call, as in ADR-0005.
