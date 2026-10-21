//! Writer-scoped encrypted module intent through the existing journal.
use super::*;
use nddev_device_sync_domain::modules::control::Configuration as ModuleConfiguration;

/// Rust-only configuration witness. No credentials or provider data are exposed.
#[derive(Clone, Default)]
pub struct ModuleSetting {
    pub configuration: ModuleConfiguration,
    /// Stable across local acknowledgement, changed by every distinct edit.
    pub operation_id: Option<String>,
    pub revision: u64,
    pub local_revision: u64,
    pub pending_id: Option<String>,
    pub conflicted: bool,
}

/// Frozen local publication metadata; not a server acknowledgement.
pub struct ModuleMutation {
    pub operation_id: String,
    pub local_revision: u64,
}

impl VaultClient {
    fn module_configuration_id(&self, module: &str) -> Result<String, Error> {
        nddev_device_sync_domain::ModuleId::new(module).map_err(|_| Error::Invalid)?;
        if module.contains(':') {
            return Err(Error::Invalid);
        }
        Ok(format!("{}:{module}:configuration", self.scope.device))
    }

    /// Only this installation's desired state may authorize its operations.
    /// Remote observations, absent settings and conflicted edits grant nothing.
    pub async fn module_configuration(&self, module: &str) -> Result<ModuleSetting, Error> {
        let _permit = self.admitted()?;
        let id = self.module_configuration_id(module)?;
        let key = self.key().await?.ok_or(Error::Locked)?;
        let (entity, local_revision) = self.journal.versioned_entity("module_state", &id).await?;
        let Some(entity) = entity else {
            return Ok(ModuleSetting {
                local_revision,
                ..Default::default()
            });
        };
        let envelope = codec::versioned(&entity.body)?;
        let bytes = key.decrypt_versioned(&self.scope, &envelope)?;
        let snapshot: Snapshot = serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        snapshot
            .validate(
                &envelope.operation.device,
                envelope.operation.entity_kind,
                &id,
            )
            .map_err(|_| Error::Invalid)?;
        if envelope.operation.device.as_str() != self.scope.device {
            return Err(Error::Invalid);
        }
        let State::ModuleConfiguration { configuration, .. } = snapshot.data else {
            return Err(Error::Invalid);
        };
        Ok(ModuleSetting {
            configuration,
            operation_id: Some(envelope.operation.id),
            revision: entity.revision,
            local_revision,
            pending_id: entity.pending_id,
            conflicted: entity.conflicted,
        })
    }

    /// Acknowledges the durable local operation, not an inferred server commit.
    /// Existing encrypted outbox, writer fencing and revision/pending CAS apply.
    pub async fn set_module_configuration(
        &self,
        module: String,
        expected_revision: u64,
        expected_pending: Option<String>,
        configuration: ModuleConfiguration,
    ) -> Result<String, Error> {
        self.set_module_configuration_attempt(
            module,
            expected_revision,
            expected_pending,
            configuration,
        )
        .await
        .map(|receipt| receipt.operation_id)
    }
    pub async fn set_module_configuration_attempt(
        &self,
        module: String,
        expected_revision: u64,
        expected_pending: Option<String>,
        configuration: ModuleConfiguration,
    ) -> Result<ModuleMutation, Error> {
        let _permit = self.admitted()?;
        let id = self.module_configuration_id(&module)?;
        if self
            .journal
            .entity("module_state", &id)
            .await?
            .is_some_and(|value| value.conflicted)
        {
            return Err(Error::Busy);
        }
        let snapshot = Snapshot {
            schema_version: 2,
            observed_at_ms: now_ms()?,
            data: State::ModuleConfiguration {
                module_id: module,
                configuration,
            },
        };
        snapshot
            .validate(
                &DeviceId::new(&self.scope.device).map_err(dependency)?,
                EntityKind::ModuleState,
                &id,
            )
            .map_err(|_| Error::Invalid)?;
        let bytes = Zeroizing::new(serde_json::to_vec(&snapshot).map_err(dependency)?);
        let receipt = self
            .put_inner_receipt(
                EntityKind::ModuleState,
                id,
                &bytes,
                Some(Expected {
                    revision: expected_revision,
                    pending: expected_pending,
                    resolution: None,
                    observed_base: None,
                    observation: None,
                    fence: None,
                }),
            )
            .await?;
        Ok(ModuleMutation {
            operation_id: receipt.id,
            local_revision: receipt.local_revision,
        })
    }
}
