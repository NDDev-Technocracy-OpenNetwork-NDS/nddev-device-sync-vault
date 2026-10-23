//! Real SQLite/encryption qualification with generated receipt inputs. This
//! exercises local durability, not a provider or deployed-device sync session.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn saved_1024_repository_catalog_preserves_committed_and_pending_rows_after_restart() {
    let (owned, journal, key, selected, before) = setup();
    let writer = guard(&key, &before);
    let mut expected = BTreeMap::new();
    let mut sequence = 42;
    let mut data = (1..=32)
        .map(|id| ObservationData::Owner {
            owner: GithubOwner {
                id: GithubAccountId::new(id).unwrap(),
                kind: if id == 1 {
                    GithubOwnerKind::User
                } else {
                    GithubOwnerKind::Organization
                },
                login: format!("owner-{id}"),
            },
        })
        .collect::<Vec<_>>();
    data.extend((1..=1024).map(|id| ObservationData::Repository {
        repository: RepositoryFact {
            id: RepositoryId::new(id).unwrap(),
            owner_id: GithubAccountId::new(1 + (id - 1) % 32).unwrap(),
            name: format!("private-saved-repository-{id}"),
            visibility: RepositoryVisibility::Private,
            archived: false,
            default_branch: Some("main".into()),
        },
    }));
    for (index, batch) in data.chunks(32).enumerate() {
        for (offset, value) in batch.iter().enumerate() {
            let body = observed_data(
                &key,
                &selected,
                &format!("catalog-{index}-{offset}"),
                value.clone(),
            );
            let operation = crate::codec::versioned(&body).unwrap();
            expected.insert(operation.operation.entity_id.clone(), body.clone());
            journal
                .enqueue_observation(
                    body,
                    0,
                    None,
                    &selected,
                    &before.selected_operation,
                    &writer,
                )
                .unwrap();
        }
        assert_eq!(journal.stats().unwrap().0, 33); //32 facts and the selected record.
        if index == 32 {
            break;
        } // Preserve the final32 encrypted pending facts.
        loop {
            let claimed = journal
                .pending()
                .unwrap()
                .into_iter()
                .filter(|row| {
                    crate::codec::versioned(&row.body)
                        .unwrap()
                        .operation
                        .entity_kind
                        == EntityKind::RepositoryState
                })
                .collect::<Vec<_>>();
            if claimed.is_empty() {
                break;
            }
            for row in claimed {
                sequence += 1;
                let receipt = serde_json::to_vec(&wire::Sync::Applied(wire::SyncApplied {
                    operation_id: row.id,
                    outcome: wire::SyncAppliedOutcome::Applied,
                    revision: 1,
                    server_seq: sequence,
                }))
                .unwrap();
                journal
                    .acknowledge_original(&row.body, &receipt, 201, 0)
                    .unwrap();
            }
        }
    }
    let before_restart = journal.stats().unwrap();
    drop(journal);
    let reopened = Journal::open(owned.0.join("replica.db")).unwrap();
    assert_eq!(reopened.stats().unwrap(), before_restart);
    let baseline = reopened.observation_baselines(&selected).unwrap();
    assert_eq!(baseline.targets.len(), 1056);
    assert_eq!(
        baseline
            .targets
            .iter()
            .filter(|row| row.pending_id.is_some())
            .count(),
        32
    );
    let mut after = String::new();
    let mut seen = BTreeSet::new();
    let mut pending = 0;
    let mut pages = 0;
    loop {
        let mut page = reopened.repository_page(&after).unwrap();
        assert!(page.entries.len() <= REPOSITORIES_PER_PAGE + 1);
        let more = page.entries.len() > REPOSITORIES_PER_PAGE;
        page.entries.truncate(REPOSITORIES_PER_PAGE);
        let mut owners = BTreeSet::new();
        for (id, row) in &page.entries {
            assert!(seen.insert(id.clone()));
            assert_eq!(row.body, expected[id]);
            let operation = crate::codec::versioned(&row.body).unwrap();
            let plain = key.decrypt_versioned(&scope(), &operation).unwrap();
            let snapshot: Snapshot = serde_json::from_slice(&plain).unwrap();
            snapshot
                .validate(&operation.operation.device, EntityKind::RepositoryState, id)
                .unwrap();
            let State::Repository { observation } = snapshot.data else {
                panic!("repository observation")
            };
            let ObservationData::Repository { repository } = observation.data else {
                panic!("repository fact")
            };
            owners.insert(format!("github:owner:{}", repository.owner_id.value()));
            if let Some(id) = &row.pending_id {
                pending += 1;
                assert_eq!(id, &operation.operation.id);
                assert_eq!(row.revision, 0);
            } else {
                assert_eq!(row.revision, 1);
            }
        }
        let owners = owners.into_iter().collect::<Vec<_>>();
        let profiles = reopened
            .repository_owners(&owners, page.local_revision)
            .unwrap();
        assert_eq!(profiles.entries.len(), owners.len());
        for (id, row) in profiles.entries {
            assert_eq!(row.body, expected[&id]);
        }
        pages += 1;
        if !more {
            break;
        }
        after = page.entries.last().unwrap().0.clone();
    }
    assert_eq!((seen.len(), pending, pages), (1024, 32, 41));
    // Claiming after reopen must recover the same immutable pending originals.
    for row in reopened.pending().unwrap() {
        let operation = crate::codec::versioned(&row.body).unwrap();
        if operation.operation.entity_kind == EntityKind::RepositoryState {
            assert_eq!(row.body, expected[&operation.operation.entity_id]);
        }
    }
}

#[test]
fn encrypted_connection_pages_cover_1024_intents_for_32_readers_without_rewriting_originals() {
    use nddev_device_sync_domain::repositories::workspace::{
        CheckoutLocation, connection_entity_id,
    };
    let (owned, journal, key, selected, before) = setup();
    let writer = guard(&key, &before);
    let mut expected = BTreeMap::new();
    for number in 1..=1024 {
        let location = CheckoutLocation::new(
            RepositoryId::new(number).unwrap(),
            GithubAccountId::new(1).unwrap(),
            "fixture-owner".into(),
            format!("repository-{number}"),
        )
        .unwrap();
        let id = connection_entity_id(location.repository_id);
        let mut operation =
            crate::codec::versioned(&observed(&key, &selected, &format!("connection-{number}")))
                .unwrap();
        operation.operation.entity_id = id.clone();
        let snapshot = Snapshot {
            schema_version: 2,
            observed_at_ms: 100,
            data: State::RepositoryConnection { location },
        };
        snapshot
            .validate(
                &operation.operation.device,
                EntityKind::RepositoryState,
                &id,
            )
            .unwrap();
        operation.operation.payload = key
            .encrypt_versioned(
                &scope(),
                &operation,
                &serde_json::to_vec(&snapshot).unwrap(),
            )
            .unwrap();
        let body = crate::codec::versioned_body(&operation).unwrap();
        journal
            .enqueue_writer(body.clone(), None, 0, None, &writer)
            .unwrap();
        expected.insert(id, body.clone());
        if number <= 992 {
            let row = journal
                .pending()
                .unwrap()
                .into_iter()
                .find(|row| row.id == operation.operation.id)
                .unwrap();
            let receipt = serde_json::to_vec(&wire::Sync::Applied(wire::SyncApplied {
                operation_id: row.id,
                outcome: wire::SyncAppliedOutcome::Applied,
                revision: 1,
                server_seq: (42 + number).try_into().unwrap(),
            }))
            .unwrap();
            journal
                .acknowledge_original(&body, &receipt, 201, 0)
                .unwrap();
        }
    }
    let originals = journal
        .pending()
        .unwrap()
        .into_iter()
        .map(|row| (row.id, row.body))
        .collect::<BTreeMap<_, _>>();
    drop(journal);
    let reopened = Journal::open(owned.0.join("replica.db")).unwrap();
    for _reader in 0..32 {
        let mut after = String::new();
        let mut seen = BTreeSet::new();
        loop {
            let mut page = reopened.connection_page(&after).unwrap();
            assert!(page.entries.len() <= 51);
            let more = page.entries.len() > 50;
            page.entries.truncate(50);
            for (id, entity) in &page.entries {
                assert!(seen.insert(id.clone()));
                assert_eq!(entity.body, expected[id]);
                let operation = crate::codec::versioned(&entity.body).unwrap();
                let plain = key.decrypt_versioned(&scope(), &operation).unwrap();
                let snapshot: Snapshot = serde_json::from_slice(&plain).unwrap();
                assert!(matches!(snapshot.data, State::RepositoryConnection { .. }));
            }
            if !more {
                break;
            }
            after = page.entries.last().unwrap().0.clone();
        }
        assert_eq!(seen.len(), 1024);
    }
    assert_eq!(
        reopened
            .pending()
            .unwrap()
            .into_iter()
            .map(|row| (row.id, row.body))
            .collect::<BTreeMap<_, _>>(),
        originals
    );
    assert!(matches!(
        reopened.connection_page("github:repo:1"),
        Err(Error::Invalid)
    ));
}
