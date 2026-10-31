//! Actual encrypted module-setting edits through the existing native adapters.
use super::*;
use nddev_device_sync_domain::{Permission, modules::control::Configuration};
use std::collections::BTreeSet;

#[tokio::test]
#[ignore = "requires an owned PostgreSQL/Mailpit fixture and actual native credentials"]
async fn actual_module_configuration_preserves_consent_offline_and_rejects_stale_edits() {
    let (receipt, identity) = tests::fixture_identity().await;
    let directory =
        std::env::temp_dir().join(format!("nds-module-acceptance-{}", uuid::Uuid::new_v4()));
    let store = identity.vault_credentials().unwrap();
    let result = async {
        let vault = VaultClient::new(identity.clone(), directory.clone()).await?;
        vault.refresh().await?;
        let key = store
            .read()
            .await
            .map_err(dependency)?
            .ok_or(Error::Locked)?;
        let initial = vault.module_configuration("github").await?;
        assert!(
            !initial.configuration.enabled && initial.revision == 0 && initial.pending_id.is_none()
        );
        assert!(matches!(
            vault.module_configuration("other:github").await,
            Err(Error::Invalid)
        ));
        let enabled = Configuration {
            enabled: true,
            permissions: BTreeSet::from([Permission::ReadOnly]),
        };
        let operation = vault
            .set_module_configuration("github".into(), 0, None, enabled.clone())
            .await?;
        let pending = vault.module_configuration("github").await?;
        assert_eq!(pending.configuration, enabled);
        assert_eq!(pending.operation_id.as_deref(), Some(operation.as_str()));
        assert_eq!(pending.pending_id.as_deref(), Some(operation.as_str()));
        assert!(matches!(
            vault
                .set_module_configuration("github".into(), 0, None, Configuration::default())
                .await,
            Err(Error::Busy)
        ));
        let configuration = Snapshot {
            schema_version: 2,
            observed_at_ms: now_ms()?,
            data: State::ModuleConfiguration {
                module_id: "github".into(),
                configuration: Configuration::default(),
            },
        };
        assert!(matches!(
            vault.publish_states(vec![configuration]).await,
            Err(Error::Invalid)
        ));
        let observation = Snapshot {
            schema_version: 1,
            observed_at_ms: now_ms()?,
            data: State::Module {
                module_id: "github".into(),
                version: "1".into(),
                configured: true,
                reachability: Reachability::NotChecked,
                checked_at_ms: None,
                error_class: None,
            },
        };
        vault.publish_states(vec![observation]).await?;
        let mut committed = vault.module_configuration("github").await?;
        for _ in 0..4 {
            if committed.pending_id.is_none() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            vault.synchronize().await?;
            committed = vault.module_configuration("github").await?;
        }
        assert_eq!(committed.configuration, enabled);
        // The retired name addresses the same retained record and revision.
        let legacy = vault.module_configuration("gds").await?;
        assert_eq!(legacy.configuration, enabled);
        assert_eq!(legacy.revision, committed.revision);
        assert!(
            vault
                .journal
                .entity(
                    "module_state",
                    &format!("{}:gds:configuration", vault.scope.device)
                )
                .await?
                .is_some()
        );
        assert_eq!(committed.operation_id, pending.operation_id);
        assert!(committed.local_revision >= pending.local_revision);
        assert!(
            committed.pending_id.is_none() && committed.revision == 1 && !committed.conflicted,
            "module setting did not settle: revision={} pending={} conflicted={} sync_error={}",
            committed.revision,
            committed.pending_id.is_some(),
            committed.conflicted,
            vault.snapshot().sync_error
        );
        let states = vault.states(String::new()).await?;
        assert!(states.items.iter().any(|v| v.kind == "module_configuration"
            && v.enabled == Some(true)
            && v.permissions == ["read_only"]));
        assert!(
            states
                .items
                .iter()
                .any(|v| v.kind == "module_state" && v.enabled.is_none() && v.configured)
        );

        tests::fixture_mode(&receipt, "offline").await?;
        identity.refresh().await.map_err(dependency)?;
        assert!(identity.view().offline_authorized);
        let mutation = vault
            .set_module_configuration_attempt(
                "github".into(),
                committed.revision,
                None,
                Configuration::default(),
            )
            .await?;
        let operation = mutation.operation_id;

        let reopened = VaultClient::new(identity.clone(), directory.clone()).await?;
        let retained = reopened.module_configuration("github").await?;
        assert_eq!(retained.configuration, Configuration::default());
        assert_eq!(retained.pending_id.as_deref(), Some(operation.as_str()));
        assert_eq!(retained.operation_id.as_deref(), Some(operation.as_str()));
        assert_ne!(retained.operation_id, committed.operation_id);
        assert_eq!(retained.local_revision, mutation.local_revision);
        assert!(retained.local_revision > committed.local_revision);
        assert!(matches!(
            vault
                .set_module_configuration("github".into(), committed.revision, None, enabled)
                .await,
            Err(Error::Busy)
        ));
        assert_eq!(
            store.read().await.map_err(dependency)?.unwrap().as_slice(),
            key.as_slice()
        );
        tests::fixture_mode(&receipt, "online").await?;
        identity.refresh().await.map_err(dependency)?;
        identity.refresh_devices().await.map_err(dependency)?;
        identity
            .maintain_authorization()
            .await
            .map_err(dependency)?;
        reopened.synchronize().await?;
        let settled = reopened.module_configuration("github").await?;
        assert_eq!(settled.configuration, Configuration::default());
        assert!(settled.pending_id.is_none() && settled.revision == 2 && !settled.conflicted);
        assert_eq!(settled.operation_id, retained.operation_id);
        // Returning to the same values is a new consent epoch, not revival of
        // the original authorization captured before the explicit withdrawal.
        let restored = reopened
            .set_module_configuration(
                "github".into(),
                settled.revision,
                None,
                pending.configuration.clone(),
            )
            .await?;
        let renewed = reopened.module_configuration("github").await?;
        assert_eq!(renewed.configuration, committed.configuration);
        assert_eq!(renewed.operation_id.as_deref(), Some(restored.as_str()));
        assert_ne!(renewed.operation_id, committed.operation_id);
        identity.logout().await.map_err(dependency)?;
        assert!(matches!(
            reopened.module_configuration("github").await,
            Err(Error::Locked)
        ));
        Ok::<_, Error>(())
    }
    .await;
    let _ = tests::fixture_mode(&receipt, "online").await;
    store.delete().await.unwrap();
    identity.logout().await.unwrap();
    if directory.exists() {
        std::fs::remove_dir_all(directory).unwrap();
    }
    result.unwrap();
}
