//! Actual native key store, encryption, SQLite and PostgreSQL. Locations are
//! generated input facts: this qualifies intent/fencing, not GitHub Git access.
use super::*;
use nddev_device_sync_domain::repositories::{
    catalog::{
        GithubAccountId, GithubOwner, GithubOwnerKind, RepositoryId, RepositorySummary,
        RepositoryVisibility,
    },
    workspace::{CheckoutLocation, write_permissions},
};
use nddev_device_sync_domain::{Permission, modules::control::Configuration};
use std::collections::BTreeSet;
use tokio_util::sync::CancellationToken;

async fn capture(
    vault: &VaultClient,
    permissions: &BTreeSet<Permission>,
) -> Result<RepositoryRead, Error> {
    let source = vault.github_source().await?;
    vault
        .capture_configured_repository_observations(
            permissions,
            source.operation_id.as_deref().ok_or(Error::Invalid)?,
            Arc::new(
                vault
                    .identity
                    .local_use_fence()
                    .await
                    .map_err(identity_failure)?,
            ),
            CancellationToken::new(),
        )
        .await
}

async fn exercise(identity: Arc<IdentityClient>, directory: PathBuf) -> Result<(), Error> {
    let vault = VaultClient::new(identity.clone(), directory.clone()).await?;
    vault.refresh().await?;
    catalog_acceptance::settle(&vault).await?;
    vault
        .save_record(
            Some("workspace-credential".into()),
            0,
            None,
            "Generated credential".into(),
            String::new(),
            Zeroizing::new("SYNTHETIC_INVALID_WORKSPACE_CREDENTIAL".into()),
        )
        .await?;
    catalog_acceptance::settle(&vault).await?;
    let record = vault
        .records(String::new())
        .await?
        .items
        .into_iter()
        .find(|record| record.id == "workspace-credential")
        .ok_or(Error::Invalid)?;
    vault
        .set_github_source(
            Some(
                CredentialSelection::new(record.id, record.revision, record.pending_id)
                    .map_err(dependency)?,
            ),
            0,
            None,
        )
        .await?;
    let mut required = write_permissions();
    required.remove(&Permission::GitWrite);
    vault
        .set_module_configuration(
            "gds".into(),
            0,
            None,
            Configuration {
                enabled: true,
                permissions: required.clone(),
            },
        )
        .await?;
    catalog_acceptance::settle(&vault).await?;
    let location = CheckoutLocation::from_repository(&RepositorySummary {
        id: RepositoryId::new(19).map_err(dependency)?,
        owner: GithubOwner {
            id: GithubAccountId::new(7).map_err(dependency)?,
            login: "generated-company".into(),
            kind: GithubOwnerKind::Organization,
        },
        name: "generated-repository".into(),
        visibility: RepositoryVisibility::Private,
        archived: false,
        default_branch: Some("main".into()),
    })
    .map_err(dependency)?;
    let read_only = capture(&vault, &required).await?;
    assert!(matches!(
        vault
            .save_repository_connection(&read_only, location.clone(), 0, None)
            .await,
        Err(Error::Cancelled)
    ));
    assert!(
        vault
            .repository_connection(location.repository_id)
            .await?
            .location
            .is_none()
    );
    // Generic observation publication cannot bypass explicit write consent.
    assert!(matches!(
        vault
            .stage_states(vec![Snapshot {
                schema_version: 2,
                observed_at_ms: now_ms()?,
                data: State::RepositoryConnection {
                    location: location.clone()
                }
            }])
            .await,
        Err(Error::Invalid)
    ));
    let setting = vault.module_configuration("gds").await?;
    vault
        .set_module_configuration(
            "gds".into(),
            setting.revision,
            setting.pending_id,
            Configuration {
                enabled: true,
                permissions: write_permissions(),
            },
        )
        .await?;
    catalog_acceptance::settle(&vault).await?;
    let read = capture(&vault, &write_permissions()).await?;
    let receipt = vault
        .save_repository_connection(&read, location.clone(), 0, None)
        .await?
        .ok_or(Error::Invalid)?;
    let pending = vault.repository_connection(location.repository_id).await?;
    assert!(pending.location.as_ref() == Some(&location));
    assert_eq!(
        pending.pending_id.as_deref(),
        Some(receipt.operation_id.as_str())
    );
    let before = catalog_acceptance::originals(&vault)?;
    assert!(
        vault
            .save_repository_connection(
                &read,
                location.clone(),
                pending.revision,
                pending.pending_id.clone()
            )
            .await?
            .is_none()
    );
    assert_eq!(catalog_acceptance::originals(&vault)?, before);
    catalog_acceptance::settle(&vault).await?;
    let committed = vault.repository_connection(location.repository_id).await?;
    assert!(committed.pending_id.is_none() && committed.revision == 1 && !committed.conflicted);
    // Acknowledgement during slow filesystem I/O changes metadata, not intent.
    assert!(
        vault
            .save_repository_connection(
                &read,
                location.clone(),
                pending.revision,
                pending.pending_id
            )
            .await?
            .is_none()
    );
    // Another replica receives the same owner-wide entity. Per-device markers
    // are not serialized, so receiving it never grants local Git write access.
    let peer = VaultClient::new(identity.clone(), directory.join("peer")).await?;
    catalog_acceptance::settle(&peer).await?;
    assert!(
        peer.repository_connection(location.repository_id)
            .await?
            .location
            == Some(location.clone())
    );
    assert!(
        peer.repository_catalog_page(None, CancellationToken::new())
            .await?
            .observations
            .is_empty()
    );
    let setting = vault.module_configuration("gds").await?;
    vault
        .set_module_configuration(
            "gds".into(),
            setting.revision,
            setting.pending_id,
            Configuration {
                enabled: true,
                permissions: required,
            },
        )
        .await?;
    assert!(matches!(
        vault
            .save_repository_connection(&read, location.clone(), committed.revision, None)
            .await,
        Err(Error::Cancelled | Error::SelectionChanged)
    ));
    assert!(
        vault
            .repository_connection(location.repository_id)
            .await?
            .location
            == Some(location)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires owned sync3 PostgreSQL/Mailpit and isolated actual native credentials"]
async fn actual_connection_intent_requires_write_consent_and_converges_without_duplicate_rows() {
    let mut logger = tests::fixture_logger();
    let identity = tests::admitted_fixture_identity().await;
    let directory =
        std::env::temp_dir().join(format!("nds-workspace-acceptance-{}", uuid::Uuid::new_v4()));
    let store = identity.vault_credentials().unwrap();
    let worker = exercise(identity.clone(), directory.clone());
    let outcome = tokio::spawn(async move {
        tokio::time::timeout(std::time::Duration::from_secs(120), worker).await
    })
    .await;
    store.delete().await.unwrap();
    identity.logout().await.unwrap();
    if directory.exists() {
        std::fs::remove_dir_all(directory).unwrap();
    }
    logger.shutdown();
    outcome.unwrap().unwrap().unwrap();
}
