//! Persistent warm tool/resource/prompt catalog for lazy backends.
//!
//! A [`WarmCatalog`] is a snapshot of a backend's capabilities (tools, resources,
//! resource templates, prompts) captured from a one-time probe. It is persisted to
//! disk keyed by the backend's [`crate::warm_cache::BinaryHasher`] identity so that
//! `tools/list` (and friends) can be served while the backend process is dead.
//!
//! All names stored in the catalog are **namespaced** as
//! `{backend_name}{separator}{local_name}` (e.g. `filesystem/read`). The serving
//! layer (later waves) emits them as-is.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tower_mcp_types::protocol::{
    PromptDefinition, ResourceDefinition, ResourceTemplateDefinition, ToolDefinition,
};

/// A persisted snapshot of a backend's capabilities.
///
/// Construct via [`WarmCatalog::from_probe_result`], which rewrites every
/// tool/resource/prompt name into its namespaced form. The catalog is
/// `Serialize`/`Deserialize` so it can be round-tripped to JSON on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarmCatalog {
    /// Backend this catalog belongs to.
    pub backend_name: String,
    /// Stable identity hash of the backend this catalog was probed from.
    ///
    /// Derived from [`crate::warm_cache::BinaryHasher::hash`] over the backend's
    /// resolved command/args/working-dir/env-keys/suffix. It is the on-disk
    /// cache key, so a catalog saved by [`WarmCatalogStore::save`] can be
    /// reloaded by [`WarmCatalogStore::load`] with the same hash (see
    /// `build_lazy_registry`).
    pub identity_hash: String,
    /// Protocol version the probe was performed with, if known.
    pub protocol_version: Option<String>,
    /// Namespaced tool definitions.
    pub tools: Vec<ToolDefinition>,
    /// Namespaced resource definitions.
    pub resources: Vec<ResourceDefinition>,
    /// Namespaced resource template definitions.
    pub resource_templates: Vec<ResourceTemplateDefinition>,
    /// Namespaced prompt definitions.
    pub prompts: Vec<PromptDefinition>,
    /// When this catalog was captured.
    pub cached_at: DateTime<Utc>,
}

impl WarmCatalog {
    /// Build a catalog from a probe result, rewriting all names to the
    /// namespaced form `{backend_name}{separator}{local_name}`.
    ///
    /// This is the single place where namespacing happens, keeping the stored
    /// catalog self-describing for the serving layer.
    #[allow(clippy::too_many_arguments)]
    pub fn from_probe_result(
        backend_name: &str,
        separator: &str,
        tools: Vec<ToolDefinition>,
        resources: Vec<ResourceDefinition>,
        resource_templates: Vec<ResourceTemplateDefinition>,
        prompts: Vec<PromptDefinition>,
        protocol_version: Option<String>,
        identity_hash: String,
    ) -> Self {
        let ns_tool = |t: ToolDefinition| ToolDefinition {
            name: format!("{backend_name}{separator}{}", t.name),
            ..t
        };
        let ns_resource = |r: ResourceDefinition| ResourceDefinition {
            name: format!("{backend_name}{separator}{}", r.name),
            ..r
        };
        let ns_template = |t: ResourceTemplateDefinition| ResourceTemplateDefinition {
            name: format!("{backend_name}{separator}{}", t.name),
            ..t
        };
        let ns_prompt = |p: PromptDefinition| PromptDefinition {
            name: format!("{backend_name}{separator}{}", p.name),
            ..p
        };

        Self {
            backend_name: backend_name.to_string(),
            identity_hash,
            protocol_version,
            tools: tools.into_iter().map(ns_tool).collect(),
            resources: resources.into_iter().map(ns_resource).collect(),
            resource_templates: resource_templates.into_iter().map(ns_template).collect(),
            prompts: prompts.into_iter().map(ns_prompt).collect(),
            cached_at: Utc::now(),
        }
    }

    /// Strip the backend namespace prefix from a namespaced name, returning the
    /// local name. Returns the input unchanged if it does not start with the
    /// backend prefix.
    pub fn local_name<'a>(&self, namespaced: &'a str) -> &'a str {
        // The separator is not stored; match the backend_name prefix.
        if let Some(stripped) = namespaced.strip_prefix(&self.backend_name) {
            // The first char after the backend name is the separator; drop it.
            if !stripped.is_empty() {
                return &stripped[1..];
            }
        }
        namespaced
    }
}

/// On-disk store for [`WarmCatalog`] entries, keyed by backend name + identity hash.
///
/// Files are written atomically (temp file + rename) with `0600` permissions on
/// Unix. The cache directory is created with `0700` permissions on construction.
pub struct WarmCatalogStore {
    cache_dir: PathBuf,
}

impl WarmCatalogStore {
    /// Create a store rooted at `cache_dir`, creating the directory (mode `0700`)
    /// if it does not already exist.
    pub fn new(cache_dir: PathBuf) -> Self {
        create_cache_dir(&cache_dir);
        Self { cache_dir }
    }

    /// The cache directory this store reads/writes from.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Clone this store into a reference-counted handle.
    ///
    /// [`WarmCatalogStore`] is not `Copy`/`Clone` (it owns a cache path), but
    /// the lazy registry shares it across the root and endpoint-group stacks via
    /// an `Arc`. This helper avoids a manual `Arc::new(store.clone())` at every
    /// call site.
    pub fn clone_into_arc(&self) -> Arc<WarmCatalogStore> {
        Arc::new(WarmCatalogStore {
            cache_dir: self.cache_dir.clone(),
        })
    }

    /// Path for a catalog file.
    ///
    /// Filenames are `{name}-{hash}.json` when `backend_name` is a safe
    /// identifier (`^[A-Za-z0-9_-]+$`), otherwise `{hash}.json`. This prevents
    /// path-traversal via crafted backend names (e.g. containing `/` or `..`).
    pub fn path_for(&self, backend_name: &str, hash: &str) -> PathBuf {
        if is_safe_name(backend_name) {
            self.cache_dir.join(format!("{backend_name}-{hash}.json"))
        } else {
            self.cache_dir.join(format!("{hash}.json"))
        }
    }

    /// Load a catalog, returning `None` on any error (missing, corrupt, or
    /// unparseable file). Callers treat absence as "no warm cache available".
    pub fn load(&self, backend_name: &str, hash: &str) -> Option<WarmCatalog> {
        let path = self.path_for(backend_name, hash);
        let contents = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return None,
        };
        serde_json::from_str(&contents).ok()
    }

    /// Atomically persist a catalog.
    ///
    /// Writes to a temp file in the cache directory, then renames it into place
    /// (atomic on the same filesystem). Sets `0600` mode on Unix.
    pub fn save(&self, catalog: &WarmCatalog) -> anyhow::Result<()> {
        // Key the file by the REAL backend identity hash (never a placeholder),
        // so load(name, BinaryHasher::hash(backend)) can reload it.
        let hash = catalog.identity_hash.clone();
        let final_path = self.path_for(&catalog.backend_name, &hash);
        let tmp_path = self.cache_dir.join(format!(".tmp-{}.json", uuid_like()));

        let json =
            serde_json::to_string_pretty(catalog).context("serialize warm catalog to JSON")?;

        fs::write(&tmp_path, json)
            .with_context(|| format!("write warm catalog temp file {}", tmp_path.display()))?;

        set_mode_0600(&tmp_path);

        fs::rename(&tmp_path, &final_path).with_context(|| {
            format!(
                "atomically rename warm catalog {} -> {}",
                tmp_path.display(),
                final_path.display()
            )
        })?;

        Ok(())
    }

    /// Remove orphaned catalog files.
    ///
    /// For each `{name}-{hash}.json` in the cache directory:
    /// - if `name` is not in `active_backend_names`, delete it;
    /// - else if `ttl_secs > 0` and the file is older than `ttl_secs`, delete it.
    ///
    /// `ttl_secs == 0` disables the age check (only name-absence triggers deletion).
    pub fn prune_orphans(&self, active_backend_names: &[String], ttl_secs: u64) {
        let active: std::collections::HashSet<&str> =
            active_backend_names.iter().map(String::as_str).collect();

        let entries = match fs::read_dir(&self.cache_dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let file_name = match path.file_stem().and_then(|s| s.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            // Only consider files shaped like `{name}-{hash}`.
            let name = match file_name.rsplit_once('-') {
                Some((name, _hash)) => name.to_string(),
                None => continue,
            };

            let orphan_by_name = !active.contains(name.as_str());
            let orphan_by_age = ttl_secs > 0 && file_age_secs(&path) > ttl_secs;

            if orphan_by_name || orphan_by_age {
                let _ = fs::remove_file(&path);
            }
        }
    }
}

/// Create the cache directory with `0700` permissions (Unix) or best-effort
/// `create_dir_all` elsewhere.
fn create_cache_dir(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        let _ = builder.create(dir);
    }
    #[cfg(not(unix))]
    {
        let _ = fs::create_dir_all(dir);
    }
}

/// Set a file to mode `0600` on Unix; no-op elsewhere.
fn set_mode_0600(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(metadata) = fs::metadata(path) {
            let mut perms = metadata.permissions();
            perms.set_mode(0o600);
            let _ = fs::set_permissions(path, perms);
        }
    }
    let _ = path;
}

/// True if `name` is a safe single-path-component identifier.
fn is_safe_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Age of a file in seconds since modification, or `u64::MAX` if unknown.
fn file_age_secs(path: &Path) -> u64 {
    let modified = match fs::metadata(path).and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(_) => return u64::MAX,
    };
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(_) => return u64::MAX,
    };
    let mtime = match modified.duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(_) => return u64::MAX,
    };
    now.as_secs().saturating_sub(mtime.as_secs())
}

/// On-disk keying uses [`WarmCatalog::identity_hash`], which is populated from
/// [`crate::warm_cache::BinaryHasher::hash`] at probe time (see
/// [`WarmCatalog::from_probe_result`]). `save` therefore keys the file by the
/// real backend identity, matching `load(name, expected_hash)` in
/// `build_lazy_registry`.
///
/// Generate a process-unique temp suffix without pulling in a uuid dependency.
fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{pid}-{now}-{n}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_mcp_types::protocol::ToolDefinition;

    fn sample_tool(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.to_string(),
            title: None,
            description: Some(format!("{name} tool")),
            input_schema: serde_json::json!({"type": "object"}),
            output_schema: None,
            icons: None,
            annotations: None,
            execution: None,
            meta: None,
        }
    }

    fn sample_catalog() -> WarmCatalog {
        WarmCatalog::from_probe_result(
            "filesystem",
            "/",
            vec![sample_tool("read"), sample_tool("write")],
            vec![],
            vec![],
            vec![],
            Some("2025-11-25".to_string()),
            "abc123hash".to_string(),
        )
    }

    #[test]
    fn from_probe_result_namespaces_tools() {
        let cat = sample_catalog();
        assert_eq!(cat.tools[0].name, "filesystem/read");
        assert_eq!(cat.tools[1].name, "filesystem/write");
    }

    #[test]
    fn local_name_strips_namespace() {
        let cat = sample_catalog();
        assert_eq!(cat.local_name("filesystem/read"), "read");
        assert_eq!(cat.local_name("unrelated/foo"), "unrelated/foo");
    }

    #[test]
    fn round_trip_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());
        let cat = sample_catalog();

        store.save(&cat).expect("save must succeed");

        // save keys the file by the REAL identity_hash, so load with the same
        // hash returns the saved catalog.
        let loaded = store.load("filesystem", &cat.identity_hash);
        let loaded = loaded.expect("load must return the saved catalog");
        assert_eq!(loaded.backend_name, "filesystem");
        assert_eq!(loaded.identity_hash, cat.identity_hash);
        assert_eq!(loaded.tools.len(), 2);
        assert_eq!(loaded.tools[0].name, "filesystem/read");
        assert_eq!(loaded.protocol_version.as_deref(), Some("2025-11-25"));
    }

    /// Regression for the warm-cache persistence bug: `save` must key the file
    /// by the REAL `BinaryHasher::hash(backend)` (not a placeholder), so a
    /// `save` followed by `load(name, BinaryHasher::hash(backend))` round-trips.
    #[test]
    fn save_load_round_trip_keyed_by_real_backend_hash() {
        use crate::config::{BackendConfig, TransportType};
        use crate::warm_cache::BinaryHasher;

        let backend = BackendConfig {
            name: "filesystem".to_string(),
            transport: TransportType::Stdio,
            command: Some("mycmd".to_string()),
            args: vec!["--root".to_string(), "/tmp".to_string()],
            cache_key_suffix: Some("v1".to_string()),
            ..Default::default()
        };
        let hash = BinaryHasher::hash(&backend);
        assert_ne!(hash, "wave1", "real hash must not be the old placeholder");

        let cat = WarmCatalog::from_probe_result(
            &backend.name,
            "/",
            vec![sample_tool("read"), sample_tool("write")],
            vec![],
            vec![],
            vec![],
            Some("2025-11-25".to_string()),
            hash.clone(),
        );

        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());
        store.save(&cat).expect("save must succeed");

        // The file must be keyed by the real hash, not "wave1".
        assert!(
            !store.path_for("filesystem", "wave1").exists(),
            "placeholder 'wave1' key must be gone from the save path"
        );
        assert!(
            store.path_for("filesystem", &hash).exists(),
            "file must be keyed by the real backend hash"
        );

        // load with the real hash returns the saved catalog.
        let loaded = store.load("filesystem", &hash);
        let loaded = loaded.expect("load must return the saved catalog");
        assert_eq!(loaded.backend_name, "filesystem");
        assert_eq!(loaded.identity_hash, hash);
        assert_eq!(loaded.tools.len(), 2);
        assert_eq!(loaded.tools[0].name, "filesystem/read");
    }

    /// A fresh store pointed at the same directory must reload a catalog saved
    /// by a previous store instance (simulates a proxy restart, R3).
    #[test]
    fn catalog_survives_restart_with_real_hash() {
        use crate::config::{BackendConfig, TransportType};
        use crate::warm_cache::BinaryHasher;

        let backend = BackendConfig {
            name: "filesystem".to_string(),
            transport: TransportType::Stdio,
            command: Some("mycmd".to_string()),
            cache_key_suffix: Some("v2".to_string()),
            ..Default::default()
        };
        let hash = BinaryHasher::hash(&backend);
        let cat = WarmCatalog::from_probe_result(
            &backend.name,
            "/",
            vec![sample_tool("read")],
            vec![],
            vec![],
            vec![],
            Some("2025-11-25".to_string()),
            hash.clone(),
        );

        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let store = WarmCatalogStore::new(dir_path.clone());
            store.save(&cat).expect("save must succeed");
        }
        // Fresh store, same directory.
        let store2 = WarmCatalogStore::new(dir_path);
        let loaded = store2.load("filesystem", &hash);
        let loaded = loaded.expect("catalog must survive restart");
        assert_eq!(loaded.tools.len(), 1);
        assert_eq!(loaded.tools[0].name, "filesystem/read");
        assert_eq!(loaded.identity_hash, hash);
    }

    /// `load` returns `None` when the expected hash does not match the saved file.
    #[test]
    fn load_returns_none_for_wrong_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());
        let cat = sample_catalog();
        store.save(&cat).expect("save must succeed");

        // A different hash looks for a different filename → no file → None.
        assert!(
            store.load("filesystem", "wrong-hash").is_none(),
            "load must return None for a non-matching hash"
        );
    }

    /// `save` writes atomically: the final file exists and no `.tmp` is left.
    #[test]
    fn save_is_atomic_no_tmp_left() {
        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());
        let cat = sample_catalog();
        store.save(&cat).expect("save must succeed");

        let final_path = store.path_for("filesystem", &cat.identity_hash);
        assert!(final_path.exists(), "final catalog file must exist");

        // No leftover temp files (`.tmp-*.json`).
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with(".tmp-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no .tmp file should remain after save"
        );
    }

    #[test]
    fn load_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());
        assert!(store.load("nope", "missing").is_none());
    }

    #[test]
    fn path_for_sanitizes_unsafe_name() {
        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());

        let safe = store.path_for("good_name", "abc123");
        assert!(safe.ends_with("good_name-abc123.json"));

        let unsafe_slash = store.path_for("evil/name", "abc123");
        assert!(unsafe_slash.ends_with("abc123.json"));
        assert!(!unsafe_slash.to_string_lossy().contains("evil/name"));

        let unsafe_dotdot = store.path_for("..", "abc123");
        assert!(unsafe_dotdot.ends_with("abc123.json"));
    }

    #[test]
    fn save_sets_0600_on_unix() {
        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());
        let cat = sample_catalog();
        store.save(&cat).expect("save must succeed");

        let path = store.path_for("filesystem", &cat.identity_hash);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let mode = fs::metadata(&path).unwrap().mode() & 0o777;
            assert_eq!(mode, 0o600, "catalog file must be mode 0600");
        }
        #[cfg(not(unix))]
        {
            let _ = path;
        }
    }

    #[test]
    fn prune_removes_name_absent_and_keeps_active() {
        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());

        // Write two catalogs manually using the store's path_for.
        let active = sample_catalog();
        store.save(&active).unwrap();

        let orphan_cat = WarmCatalog::from_probe_result(
            "deadbackend",
            "/",
            vec![sample_tool("x")],
            vec![],
            vec![],
            vec![],
            None,
            "orphanhash".to_string(),
        );
        // Save the orphan under its own name by writing directly (its own hash).
        let orphan_path = store.path_for("deadbackend", &orphan_cat.identity_hash);
        fs::write(
            &orphan_path,
            serde_json::to_string_pretty(&orphan_cat).unwrap(),
        )
        .unwrap();

        let active_path = store.path_for("filesystem", &active.identity_hash);
        assert!(active_path.exists());
        assert!(orphan_path.exists());

        store.prune_orphans(&["filesystem".to_string()], 0);

        assert!(active_path.exists(), "active backend must be kept");
        assert!(!orphan_path.exists(), "name-absent backend must be pruned");
    }

    #[test]
    fn prune_respects_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let store = WarmCatalogStore::new(dir.path().to_path_buf());

        // Active by name, but we backdate the file 1 hour for the age check.
        let cat = sample_catalog();
        store.save(&cat).unwrap();
        let path = store.path_for("filesystem", &cat.identity_hash);

        // Backdate the file 1 hour using `touch -d` (test-only helper).
        backdate_file(path.as_path(), 3600);

        // ttl_secs = 0 -> only name-absence matters; file kept.
        store.prune_orphans(&["filesystem".to_string()], 0);
        assert!(path.exists(), "ttl=0 must not prune by age");

        // ttl_secs = 60 -> file is older, so it is pruned despite active name.
        store.prune_orphans(&["filesystem".to_string()], 60);
        assert!(!path.exists(), "ttl>age must prune even active-named file");
    }

    /// Backdate a file's mtime by `secs` seconds using `touch -d` (test helper).
    fn backdate_file(path: &Path, secs: u64) {
        // `date -d "now - N sec"` is portable across GNU/coreutils.
        if let Ok(out) = std::process::Command::new("date")
            .args(["-d", &format!("now - {secs} sec"), "+%Y-%m-%d %H:%M:%S"])
            .output()
            && out.status.success()
        {
            let stamp = String::from_utf8_lossy(&out.stdout).trim().to_string();
            let _ = std::process::Command::new("touch")
                .args(["-d", &stamp, &path.to_string_lossy()])
                .status();
        }
    }
}
