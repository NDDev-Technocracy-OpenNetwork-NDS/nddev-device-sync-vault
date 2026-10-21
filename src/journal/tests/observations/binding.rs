use super::*;

fn setting_body(
    key: &VaultKey,
    selected: &CredentialSelection,
    module: bool,
    changed: bool,
) -> Vec<u8> {
    let mut operation = crate::codec::versioned(&observed(
        key,
        selected,
        match (module, changed) {
            (true, false) => "initial-module",
            (true, true) => "changed-module",
            (false, false) => "initial-source",
            (false, true) => "changed-source",
        },
    ))
    .unwrap();
    let data = if module {
        operation.operation.entity_kind = EntityKind::ModuleState;
        operation.operation.entity_id = "device:gds:configuration".into();
        State::ModuleConfiguration {
            module_id: "gds".into(),
            configuration: nddev_device_sync_domain::modules::control::Configuration {
                enabled: !changed,
                permissions: std::collections::BTreeSet::from([
                    nddev_device_sync_domain::Permission::ReadOnly,
                    nddev_device_sync_domain::Permission::CredentialStore,
                    nddev_device_sync_domain::Permission::LocalNetwork,
                ]),
            },
        }
    } else {
        operation.operation.entity_id = SOURCE_ENTITY_ID.into();
        State::GithubSource {
            selection: (!changed).then(|| selected.clone()),
        }
    };
    operation.operation.base_revision = u64::from(changed);
    let snapshot = Snapshot {
        schema_version: 2,
        observed_at_ms: 100,
        data,
    };
    snapshot
        .validate(
            &operation.operation.device,
            operation.operation.entity_kind,
            &operation.operation.entity_id,
        )
        .unwrap();
    operation.operation.payload = key
        .encrypt_versioned(
            &scope(),
            &operation,
            &serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();
    crate::codec::versioned_body(&operation).unwrap()
}

#[test]
fn actual_catalog_binding_survives_ack_but_refuses_source_permission_or_credential_changes() {
    for changed in ["source", "module", "credential"] {
        let (owned, journal, key, selected, before) = setup();
        let writer = guard(&key, &before);
        let binding = CatalogBinding {
            source: setting_body(&key, &selected, false, false),
            module_id: "device:gds:configuration".into(),
            module: setting_body(&key, &selected, true, false),
        };
        journal
            .enqueue_writer(binding.source.clone(), None, 0, None, &writer)
            .unwrap();
        journal
            .enqueue_writer(binding.module.clone(), None, 0, None, &writer)
            .unwrap();
        let captured = journal.catalog_baselines(&selected, &binding).unwrap();
        for (index, pending) in journal.pending().unwrap().iter().enumerate() {
            journal
                .acknowledge(
                    &pending.body,
                    &serde_json::to_vec(&result(&pending.id, 43 + index as i64)).unwrap(),
                    0,
                )
                .unwrap();
        }
        drop(journal);
        let journal = Journal::open(owned.0.join("replica.db")).unwrap();
        journal
            .validate_catalog_read(&selected, &captured.selected_operation, &binding)
            .unwrap();
        let command = |id| crate::storage::Command::EnqueueCatalogObservation {
            body: observed_repository(&key, &selected, "bound-observation", id),
            expected_revision: 0,
            expected_pending: None,
            selected: selected.clone(),
            selected_operation: captured.selected_operation.clone(),
            binding: binding.clone(),
            writer: guard(&key, &captured),
            resolution: None,
        };
        crate::storage::execute(&journal, command(9)).unwrap();
        match changed {
            "credential" => {
                journal
                    .enqueue(
                        crate::journal::tests::checkpoint::valid_legacy_record_body(
                            &key,
                            "new-secret",
                            "credential",
                            1,
                        ),
                        None,
                        1,
                        None,
                    )
                    .unwrap();
            }
            "source" | "module" => {
                journal
                    .enqueue_writer(
                        setting_body(&key, &selected, changed == "module", true),
                        None,
                        1,
                        None,
                        &writer,
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let stats = journal.stats().unwrap();
        assert!(matches!(
            journal.validate_catalog_read(&selected, &captured.selected_operation, &binding),
            Err(Error::SelectionChanged)
        ));
        assert!(matches!(
            journal.catalog_baselines(&selected, &binding),
            Err(Error::SelectionChanged)
        ));
        assert!(matches!(
            crate::storage::execute(&journal, command(10)),
            Err(Error::SelectionChanged)
        ));
        assert_eq!(journal.stats().unwrap(), stats);
        assert!(
            journal
                .entity("repository_state", "github:repo:10")
                .unwrap()
                .is_none()
        );
        let mut encoded = serde_json::to_value(command(10)).unwrap();
        encoded["EnqueueCatalogObservation"]
            .as_object_mut()
            .unwrap()
            .remove("binding");
        assert!(serde_json::from_value::<crate::storage::Command>(encoded).is_err());
    }
}
