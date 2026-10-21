use super::*;

#[test]
fn actual_selected_original_and_successor_survive_receipts_and_reopen() {
    let (owned, journal, key, selected, before) = setup();
    let pending = journal.pending().unwrap();
    journal
        .acknowledge(
            &pending[0].body,
            &serde_json::to_vec(&result("credential-op", 43)).unwrap(),
            0,
        )
        .unwrap();
    drop(journal);
    let journal = Journal::open(owned.0.join("replica.db")).unwrap();
    assert_eq!(
        journal
            .observation_baselines(&selected)
            .unwrap()
            .selected_operation,
        before.selected_operation
    );
    let original = journal
        .entity("vault_record", "credential")
        .unwrap()
        .unwrap();
    assert_eq!((original.revision, original.pending_id), (1, None));

    let predecessor = super::super::checkpoint::valid_legacy_record_body(
        &key,
        "predecessor",
        "successor-record",
        0,
    );
    journal.enqueue(predecessor.clone(), None, 0, None).unwrap();
    journal.pending().unwrap();
    let successor = super::super::checkpoint::valid_legacy_record_body(
        &key,
        "successor",
        "successor-record",
        1,
    );
    journal
        .enqueue(successor.clone(), None, 0, Some("predecessor".into()))
        .unwrap();
    let selected =
        CredentialSelection::new("successor-record".into(), 0, Some("successor".into())).unwrap();
    assert_eq!(
        journal
            .observation_baselines(&selected)
            .unwrap()
            .selected_operation,
        successor
    );
    journal
        .acknowledge(
            &predecessor,
            &serde_json::to_vec(&result("predecessor", 44)).unwrap(),
            0,
        )
        .unwrap();
    assert_eq!(
        journal
            .observation_baselines(&selected)
            .unwrap()
            .selected_operation,
        successor
    );
    journal.pending().unwrap();
    let mut receipt = result("successor", 45);
    let wire::Sync::Applied(applied) = &mut receipt else {
        panic!("applied")
    };
    applied.revision = 2;
    journal
        .acknowledge(&successor, &serde_json::to_vec(&receipt).unwrap(), 0)
        .unwrap();
    drop(journal);
    let journal = Journal::open(owned.0.join("replica.db")).unwrap();
    assert_eq!(
        journal
            .observation_baselines(&selected)
            .unwrap()
            .selected_operation,
        successor
    );
    journal
        .enqueue(
            super::super::checkpoint::valid_legacy_record_body(
                &key,
                "replacement",
                "successor-record",
                2,
            ),
            None,
            2,
            None,
        )
        .unwrap();
    assert!(matches!(
        journal.observation_baselines(&selected),
        Err(Error::SelectionChanged)
    ));
}

fn source_choice(key: &VaultKey, selected: &CredentialSelection, id: &str) -> Vec<u8> {
    let snapshot = Snapshot {
        schema_version: 2,
        observed_at_ms: 100,
        data: State::GithubSource {
            selection: Some(selected.clone()),
        },
    };
    let mut operation = crate::codec::versioned(&observed(key, selected, id)).unwrap();
    operation.operation.entity_id = SOURCE_ENTITY_ID.into();
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
fn actual_source_choice_is_encrypted_revision_checked_and_outside_observation_window() {
    let (owned, journal, key, selected, before) = setup();
    let writer = guard(&key, &before);
    for id in 1..=32 {
        journal
            .enqueue_observation(
                observed_repository(&key, &selected, &format!("observation-{id}"), id),
                0,
                None,
                &selected,
                &before.selected_operation,
                &writer,
            )
            .unwrap();
    }
    let body = source_choice(&key, &selected, "source-choice");
    journal
        .enqueue_observation(
            body.clone(),
            0,
            None,
            &selected,
            &before.selected_operation,
            &writer,
        )
        .unwrap();
    assert_eq!(
        journal
            .observation_baselines(&selected)
            .unwrap()
            .targets
            .len(),
        32
    );
    assert_eq!(journal.repository_page("").unwrap().entries.len(), 26);
    assert!(journal.state_page(None).unwrap().entries.is_empty());
    let stats = journal.stats().unwrap();
    assert!(matches!(
        journal.enqueue_observation(
            source_choice(&key, &selected, "stale-choice"),
            0,
            None,
            &selected,
            &before.selected_operation,
            &writer
        ),
        Err(Error::Busy)
    ));
    assert_eq!(journal.stats().unwrap(), stats);
    drop(journal);
    let journal = Journal::open(owned.0.join("replica.db")).unwrap();
    let (retained, revision) = journal
        .versioned_entity("repository_state", SOURCE_ENTITY_ID)
        .unwrap();
    let retained = retained.unwrap();
    assert_eq!(retained.body, body);
    assert_eq!(retained.pending_id.as_deref(), Some("v3:42:source-choice"));
    assert_eq!(revision, stats.4);
    let operation = crate::codec::versioned(&retained.body).unwrap();
    let decoded: Snapshot =
        serde_json::from_slice(&key.decrypt_versioned(&scope(), &operation).unwrap()).unwrap();
    decoded
        .validate(
            &DeviceId::new("device").unwrap(),
            EntityKind::RepositoryState,
            SOURCE_ENTITY_ID,
        )
        .unwrap();
    assert!(
        matches!(decoded.data, State::GithubSource { selection: Some(value) } if value==selected)
    );
    let mut other = scope();
    other.user = "another-owner".into();
    assert!(matches!(
        key.decrypt_versioned(&other, &operation),
        Err(Error::Authentication)
    ));
    assert!(
        !retained
            .body
            .windows(b"record_ref".len())
            .any(|bytes| bytes == b"record_ref")
    );
}
