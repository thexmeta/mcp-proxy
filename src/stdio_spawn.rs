//! Shared stdio backend transport construction, used by eager spawn (proxy.rs),
//! lazy spawn (lazy_registry.rs, Wave 5), and hot reload (reload.rs, Wave 7).

use std::time::Duration;

use anyhow::Context;
use tokio::process::Command;
use tower_mcp::client::StdioClientTransport;

use crate::config::BackendConfig;

/// Resolve the SIGTERM grace for a spawn-time `kill_timeout`.
///
/// `Duration::ZERO` (the `force_kill=true` effective mode) means SIGKILL
/// immediately with no graceful wait (`None`); a nonzero timeout keeps a
/// bounded SIGTERM wait capped at 1s (`force_kill=false` graceful mode).
fn sigterm_timeout_for(kill_timeout: Duration) -> Option<Duration> {
    if kill_timeout.is_zero() {
        None
    } else {
        // Use 1s for SIGTERM wait, or the full kill_timeout if it's smaller.
        let one_sec = Duration::from_secs(1);
        Some(if kill_timeout < one_sec {
            kill_timeout
        } else {
            one_sec
        })
    }
}

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
        let sigterm_timeout = sigterm_timeout_for(kill_timeout);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TransportType;

    #[test]
    fn zero_kill_timeout_skips_sigterm_grace() {
        // force_kill=true resolves to Duration::ZERO: SIGKILL immediate, no grace.
        assert_eq!(sigterm_timeout_for(Duration::ZERO), None);
    }

    #[test]
    fn nonzero_kill_timeout_keeps_sigterm_grace_capped_at_one_second() {
        // force_kill=false keeps a bounded SIGTERM wait, capped at 1s.
        assert_eq!(
            sigterm_timeout_for(Duration::from_secs(2)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            sigterm_timeout_for(Duration::from_millis(200)),
            Some(Duration::from_millis(200))
        );
    }

    #[tokio::test]
    async fn spawn_stdio_transport_builds_under_both_kill_modes() {
        // The effective kill mode (global OR per-backend force_kill) must yield
        // a spawnable transport under both modes; drop reaps the child.
        for force_kill in [false, true] {
            let cfg = BackendConfig {
                name: "kill-mode-probe".to_string(),
                transport: TransportType::Stdio,
                command: Some("true".to_string()),
                force_kill,
                ..Default::default()
            };
            let kill_timeout = cfg.effective_kill_timeout(false, 2);
            assert_eq!(
                kill_timeout.is_zero(),
                force_kill,
                "effective mode must map force_kill={force_kill} to ZERO/nonzero"
            );
            let _transport = spawn_stdio_transport(&cfg, kill_timeout)
                .await
                .expect("transport builds under both kill modes");
        }
    }
}
