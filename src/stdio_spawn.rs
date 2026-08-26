//! Shared stdio backend transport construction, used by eager spawn (proxy.rs),
//! lazy spawn (lazy_registry.rs, Wave 5), and hot reload (reload.rs, Wave 7).

use std::time::Duration;

use anyhow::Context;
use tokio::process::Command;
use tower_mcp::client::StdioClientTransport;

use crate::config::BackendConfig;

/// Build a [`StdioClientTransport`] for `backend`, resolving command/args/env/working_dir
/// exactly as the eager path did. Returns the spawned transport (child process running).
///
/// `kill_timeout` is the maximum time to wait for the child process to exit
/// after SIGTERM before sending SIGKILL. Pass `Duration::ZERO` to skip
/// SIGTERM and kill immediately.
pub async fn spawn_stdio_transport(
    backend: &BackendConfig,
    kill_timeout: Duration,
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

    let mut transport = StdioClientTransport::spawn_command(&mut cmd)
        .await
        .with_context(|| format!("spawning backend '{}'", backend.name))?;

    // Configure shutdown behavior: sigterm_timeout is 1s when kill_timeout > 0,
    // otherwise skip SIGTERM entirely.
    #[cfg(unix)]
    {
        let sigterm_timeout = if kill_timeout.is_zero() {
            None
        } else {
            // Use 1s for SIGTERM wait, or the full kill_timeout if it's smaller.
            let one_sec = Duration::from_secs(1);
            Some(if kill_timeout < one_sec {
                kill_timeout
            } else {
                one_sec
            })
        };
        transport = transport
            .kill_timeout(kill_timeout)
            .sigterm_timeout(sigterm_timeout);
    }
    #[cfg(not(unix))]
    {
        transport = transport.kill_timeout(kill_timeout);
    }

    Ok(transport)
}
