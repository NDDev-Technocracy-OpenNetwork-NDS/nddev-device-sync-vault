//! Owner-wide connection intent uses the existing encrypted operation journal.
//! Intent never grants filesystem, credential or Git authority. Physical markers
//! are device-local; fleet size does not multiply synchronized connection rows.
use super::*;
use nddev_device_sync_domain::repositories::{
    catalog::RepositoryId,
    workspace::{CheckoutLocation, WorkspacePrincipal, connection_entity_id, write_permissions},
};

#[derive(Clone, Default)]
pub struct ConnectionSetting {
    pub location: Option<CheckoutLocation>,
    pub revision: u64,
    pub pending_id: Option<String>,
    pub conflicted: bool,
}

impl VaultClient {
    /// Rust-only scope metadata, not a grant. Every use still needs its original
    /// Accounts/source/credential/device-consent fence.
    pub fn workspace_principal(&self) -> WorkspacePrincipal {
        WorkspacePrincipal {
            origin: self.scope.origin.clone(),
            tenant: self.scope.tenant.clone(),
            user: self.scope.user.clone(),
            device: self.scope.device.clone(),
        }
    }
    pub async fn repository_connection(
        &self,
        repository: RepositoryId,
    ) -> Result<ConnectionSetting, Error> {
        let _permit = self.admitted()?;
        let key = self.key().await?.ok_or(Error::Locked)?;
        let id = connection_entity_id(repository);
        let Some(entity) = self.journal.entity("repository_state", &id).await? else {
            return Ok(ConnectionSetting::default());
        };
        let snapshot =
            self.decrypt_state_snapshot(&key, EntityKind::RepositoryState, &id, &entity.body)?;
        let State::RepositoryConnection { location } = snapshot.data else {
            return Err(Error::Invalid);
        };
        if location.repository_id != repository {
            return Err(Error::Invalid);
        }
        Ok(ConnectionSetting {
            location: Some(location),
            revision: entity.revision,
            pending_id: entity.pending_id,
            conflicted: entity.conflicted,
        })
    }
    /// Register the owner-wide intent after actual local connection. The Source,
    /// credential and this device's Git write consent are compared again in the
    /// existing enqueue transaction, together with the target revision/pending ID.
    pub async fn save_repository_connection(
        &self,
        read: &RepositoryRead,
        location: CheckoutLocation,
        expected_revision: u64,
        expected_pending: Option<String>,
    ) -> Result<Option<ModuleMutation>, Error> {
        let _permit = self.admitted()?;
        location.validate().map_err(|_| Error::Invalid)?;
        self.validate_repository_read_inner(read).await?;
        let context = read.source.binding.as_ref().ok_or(Error::Invalid)?;
        if context.module_id != self.module_configuration_id("gds")? {
            return Err(Error::Invalid);
        }
        let key = self.key().await?.ok_or(Error::Locked)?;
        let snapshot = self.decrypt_state_snapshot(
            &key,
            EntityKind::ModuleState,
            &context.module_id,
            &context.module,
        )?;
        let State::ModuleConfiguration { configuration, .. } = snapshot.data else {
            return Err(Error::Invalid);
        };
        configuration
            .admit(true, true, &write_permissions())
            .map_err(|_| Error::Cancelled)?;
        let id = connection_entity_id(location.repository_id);
        let current = self.journal.entity("repository_state", &id).await?;
        if current.as_ref().is_some_and(|value| value.conflicted)
            || current.as_ref().map_or(0, |value| value.revision) != expected_revision
            || current.as_ref().and_then(|value| value.pending_id.as_ref())
                != expected_pending.as_ref()
        {
            return Err(Error::Busy);
        }
        if let Some(current) = current {
            let snapshot =
                self.decrypt_state_snapshot(&key, EntityKind::RepositoryState, &id, &current.body)?;
            if let State::RepositoryConnection { location: previous } = snapshot.data {
                if previous == location {
                    self.validate_repository_read_inner(read).await?;
                    return Ok(None);
                }
                // A changed locator needs an explicit relocation decision,
                // never an automatic observation or a silent rebind.
                return Err(Error::Busy);
            }
            return Err(Error::Invalid);
        }
        let snapshot = Snapshot {
            schema_version: 2,
            observed_at_ms: now_ms()?,
            data: State::RepositoryConnection { location },
        };
        snapshot
            .validate(
                &DeviceId::new(&self.scope.device).map_err(dependency)?,
                EntityKind::RepositoryState,
                &id,
            )
            .map_err(|_| Error::Invalid)?;
        let bytes = Zeroizing::new(serde_json::to_vec(&snapshot).map_err(dependency)?);
        let receipt = self
            .put_inner_receipt(
                EntityKind::RepositoryState,
                id,
                &bytes,
                Some(Expected {
                    revision: expected_revision,
                    pending: expected_pending,
                    resolution: None,
                    observed_base: None,
                    observation: Some(read.source.clone()),
                    fence: None,
                }),
            )
            .await?;
        Ok(Some(ModuleMutation {
            operation_id: receipt.id,
            local_revision: receipt.local_revision,
        }))
    }
}
