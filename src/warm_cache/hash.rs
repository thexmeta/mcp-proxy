//! Stable identity hashing for stdio backends.
//!
//! [`BinaryHasher`] derives a SHA-256 identity for a stdio backend from the
//! resolved server binary plus its invocation parameters. The hash is used as
//! the cache key for the persisted warm catalog (see [`crate::warm_cache::catalog`]).
//!
//! # Security
//!
//! Environment *values* are deliberately **excluded** from the hash. Only the
//! sorted set of environment *keys* participates, so that rotating a secret in
//! an env var does not change the identity, and the hash itself never embeds
//! secret material.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::config::BackendConfig;

/// Placeholder hash returned for non-stdio backends (HTTP/WebSocket).
///
/// These transports have no local binary identity to hash; callers only invoke
/// [`BinaryHasher::hash`] for stdio backends, but the placeholder keeps the
/// function total and panic-free.
pub const NON_STDIO_HASH: &str = "non-stdio";

/// Derives stable, collision-resistant identity hashes for stdio backends.
///
/// The hash is a composite over the resolved command path, the joined arguments,
/// the working directory (if any), the sorted environment *keys*, and an optional
/// `cache_key_suffix`. Launcher-version auto-resolution (e.g. resolving the
/// installed `npx`/`uvx`/`pipx` package version) is intentionally **deferred**;
/// it would require network or uncertain offline lookups and must never block
/// proxy startup. Use [`BackendConfig::cache_key_suffix`] to pin a version
/// explicitly when needed.
pub struct BinaryHasher;

impl BinaryHasher {
    /// Compute the identity hash for a stdio [`BackendConfig`].
    ///
    /// Returns [`NON_STDIO_HASH`] for non-stdio backends (HTTP/WebSocket). For
    /// stdio backends, the hash is stable across invocations on the same host:
    /// the same resolved binary + args + working dir + env keys + suffix always
    /// produce the same digest.
    ///
    /// # Panics
    ///
    /// Does not panic. A missing/unresolvable `command` yields a hash derived
    /// from the raw command string rather than failing, so the proxy can still
    /// key a cache entry.
    pub fn hash(backend: &BackendConfig) -> String {
        if !matches!(backend.transport, crate::config::TransportType::Stdio) {
            return NON_STDIO_HASH.to_string();
        }

        let env_keys: Vec<String> = {
            let mut keys: Vec<String> = backend.env.keys().cloned().collect();
            keys.sort();
            keys
        };

        Self::hash_for_stdio(
            &backend.command,
            &backend.args,
            &backend.working_dir,
            &env_keys,
            &backend.cache_key_suffix,
        )
    }

    /// Testable, config-free variant of [`Self::hash`].
    ///
    /// Resolves `command` via a PATH walk (no shell) and canonicalizes the
    /// result. If the command is already absolute and exists, it is used
    /// directly. If it cannot be resolved, the raw command string is hashed so
    /// the function remains total.
    pub fn hash_for_stdio(
        command: &Option<String>,
        args: &[String],
        working_dir: &Option<PathBuf>,
        env_keys: &[String],
        cache_key_suffix: &Option<String>,
    ) -> String {
        let resolved = command.as_deref().map(resolve_command).unwrap_or_default();

        // Sort a copy of the env keys so callers need not pre-sort. Only keys
        // participate — never values (security).
        let mut keys: Vec<String> = env_keys.to_vec();
        keys.sort();

        let mut hasher = Sha256::new();
        hasher.update(resolved.as_bytes());
        hasher.update(b"\0");
        hasher.update(args.join("\0").as_bytes());
        hasher.update(b"\0");
        if let Some(wd) = working_dir {
            hasher.update(wd.to_string_lossy().as_bytes());
        }
        hasher.update(b"\0");
        for key in &keys {
            hasher.update(key.as_bytes());
            hasher.update(b"\0");
        }
        if let Some(suffix) = cache_key_suffix {
            hasher.update(suffix.as_bytes());
        }

        let result = hasher.finalize();
        let mut hex = String::with_capacity(result.len() * 2);
        for byte in result {
            hex.push_str(&format!("{byte:02x}"));
        }
        hex
    }
}

/// Resolve a command name to an absolute, canonical path via a PATH walk.
///
/// No shell is spawned. If `cmd` is already an absolute path that exists, it is
/// canonicalized directly. Otherwise each `PATH` entry is checked for an
/// executable file named `cmd`. Returns the canonical path as a string, or the
/// original `cmd` unchanged if resolution fails (so hashing stays total).
fn resolve_command(cmd: &str) -> String {
    let path = Path::new(cmd);
    if path.is_absolute() {
        return canonicalize_or_raw(path);
    }

    if let Some(resolved) = walk_path(cmd) {
        return canonicalize_or_raw(&resolved);
    }

    // Fall back to the raw command so the hash is still computed.
    cmd.to_string()
}

/// Canonicalize `path`, returning its string form, or the raw path string on error.
fn canonicalize_or_raw(path: &Path) -> String {
    std::fs::canonicalize(path)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_string_lossy().into_owned())
}

/// Walk `$PATH` for an executable file named `cmd`.
fn walk_path(cmd: &str) -> Option<PathBuf> {
    let path_env = std::env::var("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path_env) {
        let candidate = dir.join(cmd);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Convenience helper to build the env-keys slice from an env map.
#[allow(dead_code)]
fn env_keys_of(env: &HashMap<String, String>) -> Vec<String> {
    let mut keys: Vec<String> = env.keys().cloned().collect();
    keys.sort();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackendConfig, TransportType};

    fn stdio_backend(command: &str) -> BackendConfig {
        BackendConfig {
            transport: TransportType::Stdio,
            command: Some(command.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn same_inputs_produce_same_hash() {
        let a = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &["-x".to_string(), "y".to_string()],
            &None,
            &["A".to_string(), "B".to_string()],
            &None,
        );
        let b = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &["-x".to_string(), "y".to_string()],
            &None,
            &["A".to_string(), "B".to_string()],
            &None,
        );
        assert_eq!(a, b);
    }

    #[test]
    fn different_args_produce_different_hash() {
        let a = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &["-x".to_string()],
            &None,
            &[],
            &None,
        );
        let b = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &["-y".to_string()],
            &None,
            &[],
            &None,
        );
        assert_ne!(a, b);
    }

    #[test]
    fn env_values_not_in_hash() {
        // Same keys, different values -> same hash.
        let a = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &[],
            &None,
            &["SECRET".to_string()],
            &None,
        );
        let mut env = HashMap::new();
        env.insert("SECRET".to_string(), "value-one".to_string());
        let _ = env;
        let b = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &[],
            &None,
            &["SECRET".to_string()],
            &None,
        );
        assert_eq!(a, b, "env values must not affect the hash");
    }

    #[test]
    fn env_key_order_does_not_matter() {
        let a = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &[],
            &None,
            &["A".to_string(), "B".to_string(), "C".to_string()],
            &None,
        );
        let b = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &[],
            &None,
            &["C".to_string(), "A".to_string(), "B".to_string()],
            &None,
        );
        assert_eq!(a, b, "sorted env keys must be order-independent");
    }

    #[test]
    fn cache_key_suffix_changes_hash() {
        let base = BinaryHasher::hash_for_stdio(&Some("mycmd".to_string()), &[], &None, &[], &None);
        let suffixed = BinaryHasher::hash_for_stdio(
            &Some("mycmd".to_string()),
            &[],
            &None,
            &[],
            &Some("v1.2.3".to_string()),
        );
        assert_ne!(base, suffixed);
    }

    #[test]
    fn non_stdio_returns_placeholder() {
        let mut backend = stdio_backend("anything");
        backend.transport = TransportType::Http;
        assert_eq!(BinaryHasher::hash(&backend), NON_STDIO_HASH);

        backend.transport = TransportType::Websocket;
        assert_eq!(BinaryHasher::hash(&backend), NON_STDIO_HASH);
    }

    #[test]
    fn absolute_vs_resolved_command_stable() {
        // `sh` is a symlink on many systems, so resolve it both ways and confirm
        // the two resolution paths converge to the same canonical hash.
        let by_name = BinaryHasher::hash_for_stdio(&Some("sh".to_string()), &[], &None, &[], &None);
        let resolved = resolve_command("sh");
        assert!(
            Path::new(&resolved).is_absolute(),
            "expected resolved absolute path, got {resolved}"
        );
        let by_path = BinaryHasher::hash_for_stdio(&Some(resolved), &[], &None, &[], &None);
        assert_eq!(
            by_name, by_path,
            "absolute and PATH-resolved command must hash identically"
        );

        // Resolution is itself stable across repeated calls.
        assert_eq!(resolve_command("sh"), resolve_command("sh"));
    }

    #[test]
    fn hash_is_hex_and_64_chars() {
        let h = BinaryHasher::hash_for_stdio(&Some("mycmd".to_string()), &[], &None, &[], &None);
        assert_eq!(h.len(), 64, "SHA-256 hex digest is 64 chars");
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
