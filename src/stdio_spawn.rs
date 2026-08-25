//! Shared stdio backend transport construction, used by eager spawn (proxy.rs),
//! lazy spawn (lazy_registry.rs, Wave 5), and hot reload (reload.rs, Wave 7).

use anyhow::Context;
use tokio::process::Command;
use tower_mcp::client::StdioClientTransport;

use crate::config::BackendConfig;

/// Build a [`StdioClientTransport`] for `backend`, resolving command/args/env/working_dir
/// exactly as the eager path did. Returns the spawned transport (child process running).
pub async fn spawn_stdio_transport(
    backend: &BackendConfig,
) -> anyhow::Result<StdioClientTransport> {
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
    StdioClientTransport::spawn_command(&mut cmd)
        .await
        .with_context(|| format!("spawning backend '{}'", backend.name))
}
