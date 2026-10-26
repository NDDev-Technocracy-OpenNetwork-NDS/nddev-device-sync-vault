//! Native encrypted replica and server-managed key provisioning adapters.
mod client;
mod codec;
mod key_discovery;
pub mod replication;
pub use client::{
    CatalogLease, CheckpointStage, ConnectionPage, ConnectionSetting, GithubSourceSetting,
    InboxProgress, ModuleMutation, ModuleSetting, RepositoryCatalogCursor, RepositoryCatalogPage,
    RepositoryPublication, RepositoryRead, RepositoryResolution, RetainedRepositoryObservation,
    SyncAttempt, VaultClient,
};
use nddev_device_sync_accounts::sync3_wire;
pub mod view;
pub use view::{RecordView, VaultError as Error, VaultView};
#[cfg(target_arch = "wasm32")]
pub mod browser;
pub mod crypto;
mod journal;
mod storage;

/// Journal format required by this implementation after its owned migrations.
/// This is neither a database inspection nor permission to change stored data.
pub const fn journal_schema_version() -> u32 {
    journal::SCHEMA_VERSION
}
