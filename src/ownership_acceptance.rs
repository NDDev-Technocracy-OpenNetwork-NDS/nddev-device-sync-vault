//! Ordinary native owner/follower behavior through public identity/Vault APIs.
use super::*;
use crate::replication::{ReplicaContext, ReplicaTask};
use std::time::Duration;

async fn current(task: &ReplicaTask) -> VaultView {
    let mut updates = task.updates();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let observed = updates.borrow_and_update().clone();
            match observed {
                Some(Ok(view)) if !view.catching_up && view.sync_error.is_empty() => return view,
                Some(Err(Error::Busy)) | Some(Ok(_)) | None => {}
                Some(Err(error)) => panic!("replica failed: {}", error.code()),
            }
            updates.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

fn start(identity: Arc<IdentityClient>, vault: &Arc<VaultClient>) -> ReplicaTask {
    let schedule = Arc::new(Mutex::new(Default::default()));
    ReplicaTask::spawn(
        ReplicaContext {
            identity,
            vault: Arc::downgrade(vault),
            publication: Arc::default(),
            policy_slot: Arc::new(Mutex::new(schedule.clone())),
            schedule,
            client_version: None,
        },
        Duration::ZERO,
        true,
    )
}

#[tokio::test]
#[ignore = "requires a fresh owned --fixture-sync3 PG/SMTP fixture and isolated native credentials"]
async fn actual_native_owner_has_fresh_followers_and_hands_off() {
    let mut logger = super::tests::fixture_logger();
    let sdk_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic| {
        if let Some(location) = panic.location() {
            eprintln!(
                "Native replica acceptance failed at {}:{}",
                location.file(),
                location.line()
            );
        }
        sdk_hook(panic);
    }));
    let identity = super::tests::admitted_fixture_identity().await;
    struct Owned(PathBuf);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory =
        Owned(std::env::temp_dir().join(format!("nds-replica-owner-{}", uuid::Uuid::new_v4())));
    let owner = Arc::new(
        VaultClient::new(identity.clone(), directory.0.clone())
            .await
            .unwrap(),
    );
    assert!(!owner.synchronize().await.unwrap().catching_up);
    let mut primary = start(identity.clone(), &owner);
    current(&primary).await;
    assert!(primary.is_owner());
    // A real independent facade uses the same durable scope and credentials.
    let other_identity = Arc::new(IdentityClient::new(&identity.origin()).unwrap());
    assert!(other_identity.refresh().await.unwrap().authenticated);
    assert!(
        other_identity
            .prepare_device("Native vault acceptance".into())
            .await
            .unwrap()
            .session_verified
    );
    let follower = Arc::new(
        VaultClient::new(other_identity.clone(), directory.0.clone())
            .await
            .unwrap(),
    );
    assert!(matches!(follower.synchronize().await, Err(Error::Busy)));
    assert!(
        follower
            .observe_replica(&follower.begin_replica_observation().await.unwrap())
            .await
            .unwrap()
            .caught_up
    );
    owner
        .save_record(
            Some("owner-canary".into()),
            0,
            None,
            "Owner canary".into(),
            String::new(),
            Zeroizing::new("SYNTHETIC_OWNER_FOLLOWER_CANARY".into()),
        )
        .await
        .unwrap();
    let pending = owner.journal.pending().await.unwrap();
    assert_eq!(pending.len(), 1);
    let observed = follower
        .observe_replica(&follower.begin_replica_observation().await.unwrap())
        .await
        .unwrap();
    assert!(!observed.caught_up && observed.view.pending == 1);
    let retained = follower.journal.pending().await.unwrap();
    assert_eq!(retained[0].body, pending[0].body);
    // The follower task must observe, rather than run another reconciliation.
    let mut attached = start(other_identity, &follower);
    primary.set_active(true);
    let observed = current(&attached).await;
    assert!(observed.cursor > 0 && observed.pending == 0);
    assert!(primary.is_running());
    assert!(!attached.is_owner());
    assert!(matches!(follower.replica_owner().await, Err(Error::Busy)));
    assert_eq!(
        follower
            .native_secret("owner-canary".into())
            .await
            .unwrap()
            .as_str(),
        "SYNTHETIC_OWNER_FOLLOWER_CANARY"
    );
    if let Some(task) = primary.cancel() {
        task.join().await;
    }
    // The already attached follower can become the owner after actual release.
    attached.set_active(true);
    // A completed follower observation can retain Core's existing 15s idle delay.
    tokio::time::timeout(Duration::from_secs(20), async {
        while !attached.is_owner() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(attached.is_running());
    if let Some(task) = attached.cancel() {
        task.join().await;
    }
    // Joining the async task is distinct from actual native-worker completion.
    let _released = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match owner.replica_owner().await {
                Ok(lease) => break lease,
                Err(Error::Busy) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("release failed: {}", error.code()),
            }
        }
    })
    .await
    .unwrap();
    logger.shutdown();
}
