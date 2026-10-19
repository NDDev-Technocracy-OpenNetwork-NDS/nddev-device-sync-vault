use super::*;

#[test]
fn capacity_refusal_binds_exact_dispatched_original_and_survives_reopen() {
    let (owned, journal, key) = owned_journal();
    let body = encrypted(&key, "refused", "record");
    journal.enqueue(body.clone(), None, 0, None).unwrap();
    assert!(matches!(
        journal.capacity_refused("refused", &body, 5, 2),
        Err(Error::Busy)
    ));
    journal.pending().unwrap();
    let other = encrypted(&key, "refused", "other-record");
    assert!(matches!(
        journal.capacity_refused("refused", &other, 5, 2),
        Err(Error::Invalid)
    ));
    assert!(journal.capacity_refused("refused", &body, 1, 2).is_err());
    assert!(journal.replica_snapshot().unwrap().capacity.is_none());
    journal.capacity_refused("refused", &body, 5, 2).unwrap();
    let before = journal.stats().unwrap();
    journal.capacity_refused("refused", &body, 5, 2).unwrap();
    assert_eq!(journal.stats().unwrap(), before);
    let reopened = Journal::open(owned.0.join("replica.db")).unwrap();
    assert_eq!(reopened.replica_snapshot().unwrap().capacity, Some((5, 2)));
    assert_eq!(reopened.pending().unwrap()[0].body, body);
    reopened.retry_capacity().unwrap();
    assert!(reopened.replica_snapshot().unwrap().capacity.is_none());
    assert_eq!(reopened.pending().unwrap()[0].body, body);
    reopened.capacity_refused("refused", &body, 6, 3).unwrap();
    reopened
        .mark_unknown("refused", &body, "basis_expired")
        .unwrap();
    let unknown = reopened.replica_snapshot().unwrap();
    assert!(unknown.capacity.is_none());
    assert_eq!(unknown.unknown_reason.as_deref(), Some("basis_expired"));
    reopened.retry_capacity().unwrap();
    assert_eq!(
        reopened.unknown_reason().unwrap().as_deref(),
        Some("basis_expired")
    );
}

#[test]
fn schema_seven_capacity_migration_preserves_originals_receipts_and_checkpoint() {
    let (owned, journal, key) = owned_journal();
    let committed = encrypted(&key, "committed", "committed-record");
    journal
        .ingest(&page(&committed, applied("committed", 1, 1), 1), &[0])
        .unwrap();
    let body = encrypted(&key, "refused", "record");
    journal.enqueue(body.clone(), None, 0, None).unwrap();
    journal.pending().unwrap();
    let successor = encrypted_at(&key, "successor", "record", 1);
    journal
        .enqueue(successor.clone(), None, 0, Some("refused".into()))
        .unwrap();
    let guard = journal
        .begin_checkpoint(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            &key.id(),
            1,
            0,
            1,
        )
        .unwrap();
    let before = journal.stats().unwrap();
    let checkpoint_before =
        serde_json::to_vec(&journal.checkpoint_progress(&guard).unwrap()).unwrap();
    let path = owned.0.join("replica.db");
    let db = Connection::open(&path).unwrap();
    db.execute_batch("ALTER TABLE v3_outbox DROP COLUMN capacity_floor; ALTER TABLE v3_outbox DROP COLUMN capacity_head; UPDATE v3_meta SET schema_version=7;").unwrap();
    // A rejected final version update rolls the whole migration back.
    db.execute_batch("CREATE TRIGGER reject_capacity_migration BEFORE UPDATE OF schema_version ON v3_meta WHEN NEW.schema_version=8 BEGIN SELECT RAISE(ABORT,'synthetic migration refusal'); END;").unwrap();
    assert!(Journal::open(path.clone()).is_err());
    let count: u32 = db
        .query_row(
            "SELECT count(*) FROM pragma_table_info('v3_outbox') WHERE name LIKE 'capacity_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        db.query_row("SELECT schema_version FROM v3_meta", [], |row| row
            .get::<_, u32>(0))
            .unwrap(),
        7
    );
    db.execute_batch("DROP TRIGGER reject_capacity_migration;")
        .unwrap();
    for _ in 0..2 {
        let reopened = Journal::open(path.clone()).unwrap();
        assert_eq!(reopened.stats().unwrap(), before);
        assert!(reopened.replica_snapshot().unwrap().capacity.is_none());
        assert_eq!(reopened.pending().unwrap()[0].body, body);
        assert!(
            reopened
                .checkpoint_handle(&guard.scope_tag, &guard.key_id)
                .unwrap()
                .is_some_and(|current| current == guard)
        );
        assert_eq!(
            serde_json::to_vec(&reopened.checkpoint_progress(&guard).unwrap()).unwrap(),
            checkpoint_before
        );
        assert_eq!(
            reopened
                .entity("vault_record", "committed-record")
                .unwrap()
                .unwrap()
                .body,
            committed
        );
        assert_eq!(
            db.query_row(
                "SELECT body FROM v3_outbox WHERE id='successor'",
                [],
                |row| row.get::<_, Vec<u8>>(0)
            )
            .unwrap(),
            successor
        );
    }
}
