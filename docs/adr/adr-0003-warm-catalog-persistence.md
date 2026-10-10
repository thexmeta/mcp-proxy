---
title: "ADR-0003: Warm Catalog Persistence and Invalidation"
status: "Proposed"
date: "2026-08-24"
authors: "Eser Kelleci (@thexmeta) <eserkelleci@gmail.com>"
tags: ["architecture", "decision", "caching", "lazy-spawn"]
supersedes: ""
superseded_by: ""
---

## Status

**Proposed**

## Context

Lazy spawning requires the proxy to answer `tools/list`, `resources/list`, `resource_templates/list`, and `prompts/list` before any backend process exists. The catalog must survive restarts and be invalidated when the server changes. tower-mcp's `CachedCapabilities` is `pub(crate)` (backend.rs:27-32), so the repo cannot reuse it; but the leaf types `ToolDefinition`, `ResourceDefinition`, `ResourceTemplateDefinition`, and `PromptDefinition` are `pub` + `Serialize/Deserialize` with `input_schema: serde_json::Value` (lossless round-trip). Stdio identity lives in `command: Option<String>` (config.rs:721), `args: Vec<String>` (724), and `working_dir: Option<PathBuf>` (733); `command` is often a launcher (`npx`, `uvx`), so hashing it alone is insufficient. No hash crate exists in Cargo.toml, so `sha2 = "0.10"` must be added.

## Decision

Persist a local `WarmCatalog` mirror per backend as JSON at `$XDG_CACHE_HOME/mcp-proxy/catalog/<name>-<hash>.json`. Compute a composite SHA-256 over resolved `command` + `args` + `working_dir` + `env_keys` (+ optional resolved launcher version / `cache_key_suffix` for launcher transports; `script_content` is NOT part of the hash surface — it is not a stable binary identity for launcher transports, and for inline-script transports only the script body MAY be folded in conditionally). On hash miss, run a one-time startup probe: `StdioClientTransport::spawn_command` (stdio.rs:102) → `McpClient::connect_with_handler` (client/mod.rs:713) → `initialize` (client/mod.rs:884) → `list_all_*` (client/mod.rs:1734-1779) → `shutdown` (client/mod.rs:2040). The catalog is built ONLY via this session probe (`discover()` returns no tools/resources/prompts). Cache the result; serve list requests from disk with no process running.

## Alternatives

- **Static TOML manifest only**: hand-maintained, drifts silently, no auto-detection of server upgrades. Rejected.
- **Always-probe only**: correct but defeats the lazy goal — every restart spawns all backends. Rejected.

## Consequences

- **POS-001**: List requests served with zero processes; fast cold start.
- **POS-002**: Server upgrades auto-detected via hash change.
- **NEG-001**: Extra disk I/O and a one-time probe cost on hash miss.
- **NEG-002**: Catalog files accumulate; need cache GC/prune.
- **NEG-003**: Adds the `sha2` dependency to Cargo.toml.
