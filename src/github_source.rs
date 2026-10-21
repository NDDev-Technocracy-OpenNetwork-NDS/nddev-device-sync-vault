//! Owner-wide catalog intent in the existing encrypted replica. Neither this
//! preference nor its receipt grants device consent or provider authority.
use super::*;
use nddev_device_sync_domain::repositories::catalog::SOURCE_ENTITY_ID;

#[derive(Clone, Default)]
pub struct GithubSourceSetting {
    pub selection: Option<CredentialSelection>,
    pub revision: u64,
    pub pending_id: Option<String>,
    pub operation_id: Option<String>,
    pub local_revision: u64,
    pub conflicted: bool,
}

impl VaultClient {
    /// Read encrypted owner intent. An absent/conflicted/stale reference grants
    /// nothing; the consumer must check the protected record at point of use.
    pub async fn github_source(&self) -> Result<GithubSourceSetting, Error> {
        let _permit = self.admitted()?;
        let fence = self
            .identity
            .local_use_fence()
            .await
            .map_err(identity_failure)?;
        let key = self.key().await?.ok_or(Error::Locked)?;
        let (entity, local_revision) = self
            .journal
            .versioned_entity("repository_state", SOURCE_ENTITY_ID)
            .await?;
        self.identity
            .validate_local_use(&fence)
            .await
            .map_err(identity_failure)?;
        let Some(entity) = entity else {
            return Ok(GithubSourceSetting {
                local_revision,
                ..Default::default()
            });
        };
        let operation = codec::versioned(&entity.body)?;
        let bytes = key.decrypt_versioned(&self.scope, &operation)?;
        let snapshot: Snapshot = serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        snapshot
            .validate(
                &operation.operation.device,
                operation.operation.entity_kind,
                SOURCE_ENTITY_ID,
            )
            .map_err(|_| Error::Invalid)?;
        let State::GithubSource { selection } = snapshot.data else {
            return Err(Error::Invalid);
        };
        Ok(GithubSourceSetting {
            selection,
            revision: entity.revision,
            pending_id: entity.pending_id,
            operation_id: Some(operation.operation.id),
            local_revision,
            conflicted: entity.conflicted,
        })
    }

    /// Explicit owner choice, with the same revision/pending CAS as other
    /// settings. A selected record is checked again in the insertion transaction.
    /// Conflicts remain blocked; a setter is never an implicit resolution.
    pub async fn set_github_source(
        &self,
        selection: Option<CredentialSelection>,
        expected_revision: u64,
        expected_pending: Option<String>,
    ) -> Result<ModuleMutation, Error> {
        let _permit = self.admitted()?;
        let fence = Arc::new(
            self.identity
                .local_use_fence()
                .await
                .map_err(identity_failure)?,
        );
        let key = self.key().await?.ok_or(Error::Locked)?;
        if self
            .journal
            .entity("repository_state", SOURCE_ENTITY_ID)
            .await?
            .is_some_and(|value| value.conflicted)
        {
            return Err(Error::Busy);
        }
        let source = if let Some(selected) = &selection {
            selected.validate().map_err(|_| Error::Invalid)?;
            let entity = self
                .journal
                .entity("vault_record", selected.record_ref())
                .await?
                .ok_or(Error::SelectionChanged)?;
            let operation = codec::versioned(&entity.body)?;
            if entity.conflicted
                || !selected.matches_record(
                    &operation.operation,
                    entity.revision,
                    entity.pending_id.as_deref(),
                )
                || self.read_record(&key, &entity.body)?.deleted()
            {
                return Err(Error::SelectionChanged);
            }
            Some(repository::SourceGuard {
                selection: selected.clone(),
                operation: entity.body,
                fence: fence.clone(),
                cancellation: tokio_util::sync::CancellationToken::new(),
            })
        } else {
            None
        };
        let snapshot = Snapshot {
            schema_version: 2,
            observed_at_ms: now_ms()?,
            data: State::GithubSource { selection },
        };
        snapshot
            .validate(
                &DeviceId::new(&self.scope.device).map_err(dependency)?,
                EntityKind::RepositoryState,
                SOURCE_ENTITY_ID,
            )
            .map_err(|_| Error::Invalid)?;
        let bytes = Zeroizing::new(serde_json::to_vec(&snapshot).map_err(dependency)?);
        let receipt = self
            .put_inner_receipt(
                EntityKind::RepositoryState,
                SOURCE_ENTITY_ID.into(),
                &bytes,
                Some(Expected {
                    revision: expected_revision,
                    pending: expected_pending,
                    resolution: None,
                    observed_base: None,
                    observation: source,
                    fence: Some(fence),
                }),
            )
            .await?;
        Ok(ModuleMutation {
            operation_id: receipt.id,
            local_revision: receipt.local_revision,
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires an owned PostgreSQL/Mailpit fixture and isolated native credentials"]
    async fn actual_source_choice_survives_ack_restart_remote_replica_and_refuses_stale_secret() {
        let mut logger = super::super::tests::fixture_logger();
        let identity = super::super::tests::admitted_fixture_identity().await;
        let directory =
            std::env::temp_dir().join(format!("nds-github-source-{}", uuid::Uuid::new_v4()));
        let store = identity.vault_credentials().unwrap();
        let outcome = async {
            let vault = VaultClient::new(identity.clone(), directory.clone()).await?;
            crate::client::catalog_acceptance::settle(&vault).await?;
            assert!(vault.github_source().await?.selection.is_none());
            let original = vault
                .save_record(
                    Some("owned-source".into()),
                    0,
                    None,
                    "Source acceptance".into(),
                    String::new(),
                    Zeroizing::new("SYNTHETIC_SOURCE_CANARY".into()),
                )
                .await?;
            let selected =
                CredentialSelection::new("owned-source".into(), 0, Some(original)).unwrap();
            let receipt = vault
                .set_github_source(Some(selected.clone()), 0, None)
                .await?;
            let local = vault.github_source().await?;
            assert_eq!(local.selection.as_ref(), Some(&selected));
            assert_eq!(local.pending_id.as_ref(), Some(&receipt.operation_id));
            assert_eq!(local.local_revision, receipt.local_revision);
            assert!(matches!(
                vault.set_github_source(None, 0, None).await,
                Err(Error::Busy)
            ));
            crate::client::catalog_acceptance::settle(&vault).await?;
            let committed = vault.github_source().await?;
            assert_eq!(committed.revision, 1);
            assert!(committed.pending_id.is_none() && !committed.conflicted);
            assert_eq!(committed.operation_id.as_ref(), Some(&receipt.operation_id));
            assert_eq!(
                vault.native_secret_at(&selected).await?.as_str(),
                "SYNTHETIC_SOURCE_CANARY"
            );
            drop(vault);
            let vault = VaultClient::new(identity.clone(), directory.clone()).await?;
            assert_eq!(
                vault.github_source().await?.selection.as_ref(),
                Some(&selected)
            );
            vault.ensure_record_current(&selected).await?;

            let remote =
                VaultClient::new(identity.clone(), directory.join("fresh-replica")).await?;
            crate::client::catalog_acceptance::settle(&remote).await?;
            assert_eq!(
                remote.github_source().await?.selection.as_ref(),
                Some(&selected)
            );
            remote.ensure_record_current(&selected).await?;
            // A changed secret must never inherit the old selection, although
            // the record ID and account remain the same.
            let replacement = vault
                .save_record(
                    Some("owned-source".into()),
                    1,
                    None,
                    "Replaced source".into(),
                    String::new(),
                    Zeroizing::new("SYNTHETIC_REPLACEMENT_CANARY".into()),
                )
                .await?;
            assert!(matches!(
                vault.ensure_record_current(&selected).await,
                Err(Error::SelectionChanged)
            ));
            assert!(matches!(
                vault
                    .set_github_source(Some(selected.clone()), 1, None)
                    .await,
                Err(Error::SelectionChanged)
            ));
            let next =
                CredentialSelection::new("owned-source".into(), 1, Some(replacement.clone()))
                    .unwrap();
            vault.set_github_source(Some(next.clone()), 1, None).await?;
            crate::client::catalog_acceptance::settle(&vault).await?;
            crate::client::catalog_acceptance::settle(&remote).await?;
            assert_eq!(
                remote.github_source().await?.selection.as_ref(),
                Some(&next)
            );
            assert_eq!(
                remote.native_secret_at(&next).await?.as_str(),
                "SYNTHETIC_REPLACEMENT_CANARY"
            );
            vault.delete_record("owned-source".into(), 2, None).await?;
            assert!(matches!(
                vault.ensure_record_current(&next).await,
                Err(Error::SelectionChanged)
            ));
            // Clearing an obsolete choice needs no deleted credential.
            vault.set_github_source(None, 2, None).await?;
            crate::client::catalog_acceptance::settle(&vault).await?;
            crate::client::catalog_acceptance::settle(&remote).await?;
            assert!(remote.github_source().await?.selection.is_none());
            identity.logout().await.map_err(identity_failure)?;
            assert!(matches!(vault.github_source().await, Err(Error::Locked)));
            assert!(matches!(
                vault.set_github_source(None, 3, None).await,
                Err(Error::Locked)
            ));
            Ok::<_, Error>(())
        }
        .await;
        store.delete().await.unwrap();
        identity.logout().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
        logger.shutdown();
        outcome.unwrap();
    }
}
