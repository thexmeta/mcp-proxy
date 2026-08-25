//! Lazy / on-demand backend spawning with a persistent warm tool cache.
//!
//! Wave 1 of the feature provides the two foundational, self-contained pieces:
//!
//! - [`hash`] — [`BinaryHasher`], which derives a stable, collision-resistant
//!   identity hash for a stdio backend from its resolved command, arguments,
//!   working directory, and (sorted) environment *keys* (never values).
//! - [`catalog`] — [`WarmCatalog`] and [`WarmCatalogStore`], which persist a
//!   backend's probed tool/resource/prompt catalog to disk so that
//!   `tools/list` (and friends) can be served while the backend process is dead.
//!
//! Later waves wire these into proxy routing, hot reload, and startup GC.

pub mod catalog;
pub mod hash;
pub mod probe;

pub use catalog::{WarmCatalog, WarmCatalogStore};
pub use hash::BinaryHasher;
pub use probe::ProbeRunner;
