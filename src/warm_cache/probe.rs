//! One-shot capability probe for lazy backends.
//!
//! [`ProbeRunner::probe`] spawns a stdio backend, performs the MCP initialize
//! handshake, lists all capabilities, and returns a namespaced [`WarmCatalog`].
//! It is timeout-bounded so a hung server never blocks proxy startup.

use std::time::Duration;

use tokio::process::Command;
use tower_mcp::client::{McpClient, StdioClientTransport};

use crate::config::BackendConfig;
use crate::warm_cache::BinaryHasher;
use crate::warm_cache::catalog::WarmCatalog;

/// Error returned when a capability probe fails or times out.
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    /// The probe exceeded its time budget.
    #[error("capability probe for backend '{0}' timed out after {1}s")]
    Timeout(String, u64),
    /// The backend failed to spawn or initialize.
    #[error("capability probe for backend '{0}' failed: {1}")]
    Spawn(String, #[source] anyhow::Error),
}

/// Default probe timeout in seconds (NFR-003: never block startup on a hung server).
const PROBE_TIMEOUT_SECS: u64 = 5;

/// Runs a one-shot capability probe against a stdio backend.
pub struct ProbeRunner;

impl ProbeRunner {
    /// Probe `backend`, returning a namespaced [`WarmCatalog`] of its capabilities.
    ///
    /// Spawns the backend, performs `initialize`, lists tools/resources/templates/prompts,
    /// then shuts the child down. Bounded by a [`PROBE_TIMEOUT_SECS`] timeout.
    pub async fn probe(
        backend: &BackendConfig,
        separator: &str,
    ) -> Result<WarmCatalog, ProbeError> {
        tokio::time::timeout(
            Duration::from_secs(PROBE_TIMEOUT_SECS),
            Self::probe_inner(backend, separator),
        )
        .await
        .map_err(|_| ProbeError::Timeout(backend.name.clone(), PROBE_TIMEOUT_SECS))?
    }

    async fn probe_inner(
        backend: &BackendConfig,
        separator: &str,
    ) -> Result<WarmCatalog, ProbeError> {
        let command = backend.command.as_deref().unwrap_or_default();
        let args: Vec<&str> = backend.args.iter().map(|s| s.as_str()).collect();
        let mut cmd = Command::new(command);
        cmd.args(&args);
        for (key, value) in &backend.env {
            cmd.env(key, value);
        }
        if let Some(ref working_dir) = backend.working_dir {
            cmd.current_dir(working_dir);
        }

        let transport = StdioClientTransport::spawn_command(&mut cmd)
            .await
            .map_err(|e| ProbeError::Spawn(backend.name.clone(), e.into()))?;

        let client = McpClient::connect_with_handler(transport, ())
            .await
            .map_err(|e| ProbeError::Spawn(backend.name.clone(), e.into()))?;

        let init = client
            .initialize("mcp-proxy", env!("CARGO_PKG_VERSION"))
            .await
            .map_err(|e| ProbeError::Spawn(backend.name.clone(), e.into()))?;

        let tools = client
            .list_all_tools()
            .await
            .map_err(|e| ProbeError::Spawn(backend.name.clone(), e.into()))?;
        let resources = client
            .list_all_resources()
            .await
            .map_err(|e| ProbeError::Spawn(backend.name.clone(), e.into()))?;
        let resource_templates = client
            .list_all_resource_templates()
            .await
            .map_err(|e| ProbeError::Spawn(backend.name.clone(), e.into()))?;
        let prompts = client
            .list_all_prompts()
            .await
            .map_err(|e| ProbeError::Spawn(backend.name.clone(), e.into()))?;

        // Best-effort shutdown so the child exits cleanly; ignore errors so we
        // still return the captured catalog.
        let _ = client.shutdown().await;

        Ok(WarmCatalog::from_probe_result(
            &backend.name,
            separator,
            tools,
            resources,
            resource_templates,
            prompts,
            Some(init.protocol_version),
            BinaryHasher::hash(backend),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn probe_error_display_contains_timed_out() {
        let msg = format!("{}", ProbeError::Timeout("x".to_string(), 5));
        assert!(msg.contains("timed out"), "unexpected message: {msg}");
    }

    #[test]
    fn probe_error_display_contains_backend_name() {
        let msg = format!(
            "{}",
            ProbeError::Spawn("svc".to_string(), anyhow::anyhow!("boom"))
        );
        assert!(msg.contains("svc"), "unexpected message: {msg}");
        assert!(msg.contains("boom"), "unexpected message: {msg}");
    }

    #[tokio::test]
    async fn probe_builds_command_with_env_and_working_dir() {
        // `echo` exits immediately and does not speak MCP, so initialize fails.
        // This still exercises the full command-building path (command, args,
        // env, working_dir) and proves the probe degrades to a ProbeError
        // rather than panicking.
        let mut env = std::collections::HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        let backend = BackendConfig {
            name: "echo-backend".to_string(),
            transport: crate::config::TransportType::Stdio,
            command: Some("echo".to_string()),
            args: vec!["hi".to_string()],
            env,
            working_dir: Some(PathBuf::from("/tmp")),
            ..Default::default()
        };

        let res = ProbeRunner::probe(&backend, "/").await;
        assert!(res.is_err(), "echo is not an MCP server; probe must error");
    }

    #[tokio::test]
    async fn probe_respects_timeout_bound() {
        // `sleep 30` never speaks MCP; the probe must time out well before that.
        let backend = BackendConfig {
            name: "sleeper".to_string(),
            transport: crate::config::TransportType::Stdio,
            command: Some("sleep".to_string()),
            args: vec!["30".to_string()],
            ..Default::default()
        };

        let start = std::time::Instant::now();
        let res = ProbeRunner::probe(&backend, "/").await;
        let elapsed = start.elapsed();

        assert!(res.is_err(), "sleep is not an MCP server; probe must error");
        // Allow generous slack over the 5s budget for CI scheduling jitter.
        assert!(
            elapsed < Duration::from_secs(8),
            "probe took {elapsed:?}, expected < 8s (timeout bound violated)"
        );
    }

    #[tokio::test]
    async fn probe_timeout_returns_timeout_error() {
        // `sleep 30` never speaks MCP; the probe must time out and surface a
        // `ProbeError::Timeout` (not a spawn/IO error).
        let backend = BackendConfig {
            name: "sleeper".to_string(),
            transport: crate::config::TransportType::Stdio,
            command: Some("sleep".to_string()),
            args: vec!["30".to_string()],
            ..Default::default()
        };

        let start = std::time::Instant::now();
        let res = ProbeRunner::probe(&backend, "/").await;
        let elapsed = start.elapsed();

        match res {
            Err(ProbeError::Timeout(name, secs)) => {
                assert_eq!(name, "sleeper");
                assert_eq!(secs, PROBE_TIMEOUT_SECS);
            }
            other => panic!("expected ProbeError::Timeout, got {other:?}"),
        }
        // Bound the elapsed time to prove the timeout actually fired.
        assert!(
            elapsed < Duration::from_secs(8),
            "probe took {elapsed:?}, expected < 8s (timeout bound violated)"
        );
    }

    #[tokio::test]
    async fn probe_spawn_failure_returns_spawn_error() {
        // A command that cannot be spawned must surface `ProbeError::Spawn`.
        let backend = BackendConfig {
            name: "ghost".to_string(),
            transport: crate::config::TransportType::Stdio,
            command: Some("this-command-does-not-exist-xyz".to_string()),
            ..Default::default()
        };

        let res = ProbeRunner::probe(&backend, "/").await;
        match res {
            Err(ProbeError::Spawn(name, _)) => assert_eq!(name, "ghost"),
            other => panic!("expected ProbeError::Spawn, got {other:?}"),
        }
    }

    /// A minimal MCP stdio server that answers the probe handshake and returns
    /// one namespaced tool. Spawned as a child via `python3` so the probe can
    /// complete a real `initialize` + `tools/list` round-trip.
    #[tokio::test]
    async fn probe_success_returns_namespaced_tools_and_version() {
        let script = r#"
import sys, json
def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
def read():
    line = sys.stdin.readline()
    return json.loads(line) if line else None
while True:
    req = read()
    if req is None:
        break
    mid = req.get("id")
    method = req.get("method")
    if method == "initialize":
        send({"jsonrpc":"2.0","id":mid,"result":{
            "protocolVersion":"2025-11-25",
            "capabilities":{"tools":{"listChanged":False},"resources":{}},
            "serverInfo":{"name":"probe-test","version":"0.1.0"}}})
    elif method == "notifications/initialized":
        continue
    elif method == "tools/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"tools":[
            {"name":"read","description":"read file","inputSchema":{"type":"object"}}]}})
    elif method == "resources/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"resources":[]}})
    elif method == "resources/templates/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"resourceTemplates":[]}})
    elif method == "prompts/list":
        send({"jsonrpc":"2.0","id":mid,"result":{"prompts":[]}})
    elif method == "shutdown":
        send({"jsonrpc":"2.0","id":mid,"result":{}})
        break
    else:
        send({"jsonrpc":"2.0","id":mid,"result":{}})
"#;
        let backend = BackendConfig {
            name: "probe-backend".to_string(),
            transport: crate::config::TransportType::Stdio,
            command: Some("python3".to_string()),
            args: vec!["-c".to_string(), script.to_string()],
            ..Default::default()
        };

        let res = ProbeRunner::probe(&backend, "/").await;
        let cat = res.expect("probe of a real MCP server must succeed");

        // Tools are namespaced as {backend_name}{separator}{local_name}.
        assert_eq!(cat.backend_name, "probe-backend");
        assert_eq!(cat.tools.len(), 1, "expected exactly one tool");
        assert_eq!(cat.tools[0].name, "probe-backend/read");
        // Negotiated protocol version is captured from initialize.
        assert_eq!(
            cat.protocol_version.as_deref(),
            Some("2025-11-25"),
            "negotiated protocol version must be captured"
        );
        // The catalog carries the real backend identity hash.
        assert_eq!(cat.identity_hash, BinaryHasher::hash(&backend));
    }
}
