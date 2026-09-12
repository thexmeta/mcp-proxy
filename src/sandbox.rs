//! Detects systemd sandboxing (ProtectSystem=strict) by checking if the root
//! filesystem is read-only. Warns users when backend args might cause EROFS
//! errors from backends like `rust-mcp-filesystem`.

use std::path::Path;

/// Information about the current process's mount sandbox.
#[derive(Debug, Clone)]
pub struct SandboxInfo {
    /// Whether the root filesystem `/` is mounted read-only.
    pub root_read_only: bool,
    /// Paths that are writable (detected from mount table).
    pub writable_roots: Vec<String>,
}

impl SandboxInfo {
    /// Detect sandbox state by checking if `/` is read-only.
    pub fn detect() -> Self {
        let root_read_only = is_root_read_only();
        let writable_roots = detect_writable_roots();

        Self {
            root_read_only,
            writable_roots,
        }
    }

    /// Check if a given path would be writable in the current sandbox.
    pub fn is_path_writable(&self, path: &str) -> bool {
        if !self.root_read_only {
            return true; // Root is writable, everything should be writable.
        }

        let path = Path::new(path);
        // Check if any writable root is a prefix of the path or vice versa.
        self.writable_roots.iter().any(|root| {
            let root_path = Path::new(root);
            path.starts_with(root_path) || root_path.starts_with(path)
        })
    }

    /// Check if backend args might cause EROFS and log warnings.
    pub fn validate_backend_args(&self, command: &str, args: &[String]) {
        if !self.root_read_only {
            return; // No sandbox, no issue.
        }

        for arg in args {
            if arg == "/" || arg == "/*" {
                tracing::warn!(
                    command,
                    arg,
                    "Backend has root directory '/' as allowed path, but root filesystem \
                     is read-only (likely ProtectSystem=strict). This will cause EROFS \
                     errors. Fix: change allowed path to a writable directory like \
                     '/home/<user>' or add ReadWritePaths to the systemd unit.",
                );
            } else if arg.starts_with('/') && !self.is_path_writable(arg) {
                tracing::warn!(
                    command,
                    arg,
                    "Backend path '{}' is not in a writable mount. EROFS likely.", arg
                );
            }
        }

        // Check for symlinks that escape writable mounts.
        if let Err(e) = self.check_symlink_escapes(args) {
            tracing::warn!(
                command,
                "Symlink escape detected: {}. Symlinks that resolve outside writable \
                 mounts will cause EROFS. Consider adding the symlink target to \
                 ReadWritePaths or removing the symlink.",
                e
            );
        }
    }

    /// Check if any path in the args contains a symlink that resolves outside
    /// writable mounts. This catches the common case where `~/Desktop` is a
    /// symlink to `/var/local/Desktop` — the symlink escapes the `/home/user`
    /// bind mount back to the read-only root.
    fn check_symlink_escapes(&self, args: &[String]) -> Result<(), String> {
        if !self.root_read_only {
            return Ok(());
        }

        for arg in args {
            if !arg.starts_with('/') || arg == "/" || arg == "/*" {
                continue;
            }

            let path = Path::new(arg);
            // Only check directories (files might be created inside writable dirs).
            if !path.is_dir() {
                continue;
            }

            // Resolve symlinks in path components.
            match std::fs::canonicalize(path) {
                Ok(resolved) => {
                    let resolved_str = resolved.to_string_lossy().to_string();
                    if resolved_str != *arg && !self.is_path_writable(&resolved_str) {
                        return Err(format!(
                            "'{}' is a symlink that resolves to '{}', which is outside \
                             writable mounts",
                            arg, resolved_str
                        ));
                    }
                }
                Err(_) => {
                    // Path doesn't exist yet — can't check symlinks.
                    // This is fine for write targets.
                }
            }
        }

        Ok(())
    }
}

/// Check if the root filesystem `/` is mounted read-only.
/// Uses statvfs to check the ST_RDONLY flag.
#[cfg(unix)]
fn is_root_read_only() -> bool {
    use std::ffi::CString;

    let root = match CString::new("/") {
        Ok(c) => c,
        Err(_) => return false,
    };

    // SAFETY: statvfs writes to a stack-allocated statvfs struct. The pointer
    // is valid for the duration of the call.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let result = unsafe { libc::statvfs(root.as_ptr(), &mut stat as *mut _) };

    if result != 0 {
        tracing::debug!("statvfs('/') failed, assuming root is writable");
        return false;
    }

    // ST_RDONLY flag is bit 0 in f_flag.
    (stat.f_flag & libc::ST_RDONLY) != 0
}

/// Non-unix fallback: assume root is always writable.
#[cfg(not(unix))]
fn is_root_read_only() -> bool {
    false
}

/// Parse /proc/self/mountinfo to find writable mount points.
/// Returns paths that have rw mount options.
#[cfg(unix)]
fn detect_writable_roots() -> Vec<String> {
    let mountinfo = match std::fs::read_to_string("/proc/self/mountinfo") {
        Ok(content) => content,
        Err(e) => {
            tracing::debug!("Failed to read /proc/self/mountinfo: {}", e);
            return Vec::new();
        }
    };

    let mut writable = Vec::new();

    for line in mountinfo.lines() {
        // mountinfo format: mount_id parent_id major:minor root mount_point mount_opts ...
        // Fields are space-separated; field 6 (0-indexed: 5) is mount_opts.
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 6 {
            continue;
        }

        let mount_point = fields[4];
        let mount_opts = fields[5];

        // A mount is writable if it has 'rw' and does NOT have 'ro'.
        if mount_opts.contains("rw") && !mount_opts.contains("ro") {
            writable.push(mount_point.to_string());
        }
    }

    writable
}

/// Non-unix fallback: return empty list.
#[cfg(not(unix))]
fn detect_writable_roots() -> Vec<String> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_does_not_panic() {
        let info = SandboxInfo::detect();
        // On any system, detect() should complete without panicking.
        // We just verify the struct is populated.
        let _ = info.root_read_only;
        let _ = &info.writable_roots;
    }

    #[test]
    fn writable_roots_contains_root_when_not_sandboxed() {
        let info = SandboxInfo {
            root_read_only: false,
            writable_roots: vec!["/".to_string()],
        };
        assert!(info.is_path_writable("/home/user"));
        assert!(info.is_path_writable("/tmp"));
    }

    #[test]
    fn is_path_writable_checks_prefix_match() {
        let info = SandboxInfo {
            root_read_only: true,
            writable_roots: vec!["/tmp".to_string(), "/home".to_string()],
        };
        assert!(info.is_path_writable("/tmp/foo"));
        assert!(info.is_path_writable("/home/user/data"));
        assert!(!info.is_path_writable("/etc/hosts"));
    }

    #[test]
    fn is_path_writable_handles_exact_match() {
        let info = SandboxInfo {
            root_read_only: true,
            writable_roots: vec!["/data".to_string()],
        };
        assert!(info.is_path_writable("/data"));
    }

    #[test]
    fn validate_backend_args_no_warning_when_root_writable() {
        let info = SandboxInfo {
            root_read_only: false,
            writable_roots: vec!["/".to_string()],
        };
        // Should not log any warnings since root is writable.
        info.validate_backend_args("rust-mcp-filesystem", &["/".to_string()]);
    }

    #[test]
    fn check_symlink_escapes_returns_ok_when_not_sandboxed() {
        let info = SandboxInfo {
            root_read_only: false,
            writable_roots: vec!["/".to_string()],
        };
        assert!(info.check_symlink_escapes(&["/home/user".to_string()]).is_ok());
    }

    #[test]
    fn check_symlink_escapes_returns_ok_for_nonexistent_path() {
        let info = SandboxInfo {
            root_read_only: true,
            writable_roots: vec!["/home/user".to_string()],
        };
        // Non-existent paths can't be checked for symlinks — that's fine.
        assert!(info.check_symlink_escapes(&["/nonexistent/path".to_string()]).is_ok());
    }

    #[test]
    fn check_symlink_escapes_returns_ok_for_root_arg() {
        let info = SandboxInfo {
            root_read_only: true,
            writable_roots: vec!["/home/user".to_string()],
        };
        // Root "/" is handled separately by validate_backend_args.
        assert!(info.check_symlink_escapes(&["/".to_string()]).is_ok());
    }
}
