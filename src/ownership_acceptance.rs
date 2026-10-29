//! Ordinary native owner/follower behavior through public identity/Vault APIs.
use super::*;
use crate::replication::{ReplicaContext, ReplicaTask};
use std::time::Duration;

fn native_logger() -> nddev_device_sync_telemetry::Logger {
    let logger = super::tests::fixture_logger();
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
    logger
}

async fn current(task: &ReplicaTask) -> VaultView {
    current_updates(task.updates(), 0).await
}

async fn current_updates(
    updates: tokio::sync::watch::Receiver<Option<Result<VaultView, Error>>>,
    minimum_cursor: u64,
) -> VaultView {
    current_updates_for(updates, minimum_cursor, Duration::from_secs(30)).await
}

async fn current_updates_for(
    mut updates: tokio::sync::watch::Receiver<Option<Result<VaultView, Error>>>,
    minimum_cursor: u64,
    deadline: Duration,
) -> VaultView {
    tokio::time::timeout(deadline, async {
        loop {
            let observed = updates.borrow_and_update().clone();
            match observed {
                Some(Ok(view))
                    if !view.catching_up
                        && view.sync_error.is_empty()
                        && view.cursor >= minimum_cursor =>
                {
                    return view;
                }
                Some(Err(Error::Busy)) | Some(Ok(_)) | None => {}
                Some(Err(error)) => panic!("replica failed: {}", error.code()),
            }
            updates.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh owned --fixture-sync3 PG/SMTP fixture and isolated native credentials"]
async fn actual_native_clipboard_reader_follows_retained_owner() {
    use crate::replication::native::NativeReplica;
    let mut logger = native_logger();
    let identity = super::tests::admitted_fixture_identity().await;
    struct Owned(PathBuf);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory = Owned(
        std::env::temp_dir().join(format!("nds-clipboard-follower-{}", uuid::Uuid::new_v4())),
    );
    let owner = NativeReplica::attach(&identity, directory.0.clone(), None)
        .await
        .unwrap();
    let retained = owner.clone();
    let path = directory.0.clone();
    // A joined task contains assertion failures; the parent still stops and
    // joins the actual native owner before the fresh test directory is removed.
    let mut scenario = tokio::spawn(Box::pin(async move {
        let before = current_updates(retained.updates().unwrap(), 0).await;
        let reader = VaultClient::new(identity.clone(), path).await.unwrap();
        reader
            .share_clipboard(Zeroizing::new("SYNTHETIC_NATIVE_FOLLOWER_TEXT".into()))
            .await
            .unwrap();
        retained.wake();
        let committed = current_updates(retained.updates().unwrap(), before.cursor + 1).await;
        assert_eq!(committed.pending, 0);
        assert!(reader.snapshot().catching_up);
        assert!(matches!(reader.clipboard().await, Err(Error::Busy)));
        assert!(matches!(reader.synchronize_inbox().await, Err(Error::Busy)));

        let observed = reader.observe_replica_once().await.unwrap();
        assert!(observed.caught_up && !observed.view.catching_up);
        assert_eq!(observed.view.cursor, committed.cursor);
        let (metadata, text) = reader.clipboard().await.unwrap().unwrap();
        assert!(metadata.present && metadata.server_seq > before.cursor);
        assert!(metadata.server_seq <= committed.cursor);
        assert_eq!(text.as_str(), "SYNTHETIC_NATIVE_FOLLOWER_TEXT");
        assert!(retained.is_running());

        let permit = reader.admitted().unwrap();
        assert!(matches!(
            reader.observe_replica_once().await,
            Err(Error::Busy)
        ));
        drop(permit);
        identity.cancel();
        assert!(matches!(
            reader.observe_replica_once().await,
            Err(Error::Cancelled | Error::Locked)
        ));
        assert!(retained.is_running());
    }));
    let outcome = tokio::time::timeout(Duration::from_secs(120), &mut scenario).await;
    if outcome.is_err() {
        scenario.abort();
        let _ = scenario.await;
    }
    let shutdown = NativeReplica::shutdown().await;
    logger.shutdown();
    shutdown.unwrap();
    assert!(!owner.is_running());
    outcome
        .expect("owned clipboard follower scenario deadline")
        .unwrap();
}

type Policy = Arc<Mutex<Arc<Mutex<nddev_device_sync_application::lifecycle::ReplicaSchedule>>>>;

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[tokio::test]
#[ignore = "fills the production entity quota through real HTTP; fresh owned PG/SMTP/native credentials required"]
async fn actual_capacity_refusal_survives_owner_handoff_without_original_rewrite() {
    use nddev_device_sync_domain::sync::MAX_ENTITY_COUNT;
    let mut logger = native_logger();
    let identity = super::tests::admitted_fixture_identity().await;
    struct Owned(PathBuf);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory =
        Owned(std::env::temp_dir().join(format!("nds-owner-capacity-{}", uuid::Uuid::new_v4())));
    let owner = Arc::new(
        VaultClient::new(identity.clone(), directory.0.clone())
            .await
            .unwrap(),
    );
    assert!(!owner.synchronize().await.unwrap().catching_up);
    let mut primary = start(identity.clone(), &owner);
    current(&primary).await;
    // The production per-device replay cache retains at most 512 live
    // signature nonces. Loading 4096 entities must honor its expiry/backoff;
    // this fixture budget does not alter any product quota or batch deadline.
    tokio::time::timeout(Duration::from_secs(3300), async {
        for index in 0..MAX_ENTITY_COUNT {
            owner
                .save_record(
                    Some(format!("capacity-{index}")),
                    0,
                    None,
                    "Capacity seed".into(),
                    String::new(),
                    Zeroizing::new("SYNTHETIC_CAPACITY_VALUE".into()),
                )
                .await
                .unwrap();
            if (index + 1) % nddev_device_sync_application::replica::MAX_UPLOADS as u64 == 0 {
                // Use production retry/admission policy under real server pressure.
                primary.set_active(true);
                current_updates_for(primary.updates(), index + 1, Duration::from_secs(420)).await;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(owner.snapshot().cursor, MAX_ENTITY_COUNT);
    owner
        .save_record(
            Some("capacity-refused".into()),
            0,
            None,
            "Refused original".into(),
            String::new(),
            Zeroizing::new("SYNTHETIC_REFUSED_ORIGINAL".into()),
        )
        .await
        .unwrap();
    let original = owner.journal.pending().await.unwrap();
    assert_eq!(original.len(), 1);
    let mut updates = primary.updates();
    updates.borrow_and_update();
    primary.set_active(true);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let value = updates.borrow_and_update().clone();
            if matches!(value, Some(Err(Error::CapacityExhausted))) {
                break;
            }
            updates.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(owner.replica_snapshot().await.unwrap().capacity.is_some());
    let other_identity = Arc::new(IdentityClient::new(&identity.origin()).unwrap());
    other_identity.refresh().await.unwrap();
    other_identity
        .prepare_device("Native vault acceptance".into())
        .await
        .unwrap();
    let other = Arc::new(
        VaultClient::new(other_identity.clone(), directory.0.clone())
            .await
            .unwrap(),
    );
    let observed = other
        .observe_replica(&other.begin_replica_observation().await.unwrap())
        .await
        .unwrap();
    assert_eq!(observed.view.sync_error, Error::CapacityExhausted.code());
    assert!(!observed.caught_up);
    let (mut follower, policy) = start_policy(other_identity, &other);
    let mut observed = follower.updates();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let value = observed.borrow_and_update().clone();
            if value.is_some_and(|value| {
                value.is_ok_and(|view| view.sync_error == Error::CapacityExhausted.code())
            }) {
                break;
            }
            observed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(!follower.is_owner());
    if let Some(task) = primary.cancel() {
        task.join().await;
    }
    owner.drain_replica().await.unwrap();
    follower.set_active(true);
    observed.changed().await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !follower.is_owner() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(policy.lock().unwrap().lock().unwrap().is_capacity_blocked());
    if let Some(task) = follower.cancel() {
        task.join().await;
    }
    other.drain_replica().await.unwrap();
    assert_eq!(
        other.journal.pending().await.unwrap()[0].body,
        original[0].body
    );
    assert!(matches!(
        other.retry_synchronize_attempt().await.unwrap().result,
        Err(Error::CapacityExhausted)
    ));
    assert!(other.replica_snapshot().await.unwrap().capacity.is_some());
    assert_eq!(
        other.journal.pending().await.unwrap()[0].body,
        original[0].body
    );
    assert_eq!(other.snapshot().cursor, MAX_ENTITY_COUNT);
    logger.shutdown();
}

fn start(identity: Arc<IdentityClient>, vault: &Arc<VaultClient>) -> ReplicaTask {
    start_policy(identity, vault).0
}
fn start_policy(identity: Arc<IdentityClient>, vault: &Arc<VaultClient>) -> (ReplicaTask, Policy) {
    let policy = Arc::new(Mutex::new(Arc::new(Mutex::new(Default::default()))));
    let schedule = policy.lock().unwrap().clone();
    let task = ReplicaTask::spawn(
        ReplicaContext {
            identity,
            vault: Arc::downgrade(vault),
            publication: Arc::default(),
            policy_slot: policy.clone(),
            schedule,
            client_version: None,
            catalog: Arc::default(),
        },
        Duration::ZERO,
        true,
    );
    (task, policy)
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[tokio::test]
#[ignore = "requires a fresh owned --fixture-sync3 PG/SMTP fixture and isolated native credentials"]
async fn actual_retained_runtime_survives_facade_cancellation_and_reuses_policy() {
    use crate::replication::native::NativeReplica;
    let mut logger = native_logger();
    let identity = super::tests::admitted_fixture_identity().await;
    let endpoint = identity.origin();
    struct Owned(PathBuf);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory =
        Owned(std::env::temp_dir().join(format!("nds-retained-runtime-{}", uuid::Uuid::new_v4())));
    let retained = NativeReplica::attach(&identity, directory.0.clone(), None)
        .await
        .unwrap();
    let before = current_updates(retained.updates().unwrap(), 0).await;
    let policy = retained.schedule();
    // A real Git process waits for stdin under the SAME elected collector.
    // Replication must finish the canary below while this work is pending.
    struct SlowGit {
        started: tokio_util::sync::CancellationToken,
        cancelled: tokio_util::sync::CancellationToken,
        drained: tokio_util::sync::CancellationToken,
    }
    impl nddev_device_sync_application::repositories::catalog::collection::CatalogCollector
        for SlowGit
    {
        fn collect(
            self: Arc<Self>,
        ) -> nddev_device_sync_application::repositories::catalog::collection::Collection {
            Box::pin(async move {
                if self.cancelled.is_cancelled() {
                    return Default::default();
                }
                let mut git = tokio::process::Command::new("git")
                    .args(["hash-object", "--stdin"])
                    .env_clear()
                    .env("PATH", std::env::var_os("PATH").unwrap())
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .kill_on_drop(true)
                    .spawn()
                    .unwrap();
                self.started.cancel();
                tokio::select! {
                    _ = self.cancelled.cancelled() => {},
                    _ = tokio::time::sleep(Duration::from_secs(30)) => panic!("Vault did not advance while Git was pending"),
                }
                git.kill().await.unwrap();
                self.drained.cancel();
                Default::default()
            })
        }
        fn cancel(&self) {
            self.cancelled.cancel();
        }
    }
    let git = retained
        .catalog(|_, _| SlowGit {
            started: Default::default(),
            cancelled: Default::default(),
            drained: Default::default(),
        })
        .unwrap();
    retained.wake();
    tokio::time::timeout(Duration::from_secs(10), git.started.cancelled())
        .await
        .unwrap();
    let facade = VaultClient::new(identity.clone(), directory.0.clone())
        .await
        .unwrap();
    facade
        .save_record(
            Some("runtime-canary".into()),
            0,
            None,
            "Runtime canary".into(),
            String::new(),
            Zeroizing::new("SYNTHETIC_RETAINED_RUNTIME_CANARY".into()),
        )
        .await
        .unwrap();
    assert_eq!(facade.refresh_local().await.unwrap().pending, 1);
    assert!(matches!(
        facade.stage_states(Vec::new()).await,
        Err(Error::Invalid)
    ));
    let observations =
        crate::replication::device_observations(&identity, env!("CARGO_PKG_VERSION")).unwrap();
    assert!(facade.stage_states(observations).await.unwrap().pending > 1);
    let old_identity = Arc::downgrade(&identity);
    identity.cancel();
    drop(facade);
    drop(identity);
    assert!(old_identity.upgrade().is_none());
    retained.wake();
    let caught_up = current_updates(retained.updates().unwrap(), before.cursor + 1).await;
    assert_eq!(caught_up.pending, 0);
    assert!(
        !git.drained.is_cancelled(),
        "Vault must advance before the slow Git ends"
    );
    // Reopening attaches to the same independent task and policy identity.
    let reopened = Arc::new(IdentityClient::new(&endpoint).unwrap());
    assert!(reopened.refresh().await.unwrap().authenticated);
    reopened
        .prepare_device("Native vault acceptance".into())
        .await
        .unwrap();
    let attached = NativeReplica::attach(&reopened, directory.0.clone(), None)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&retained, &attached));
    assert!(Arc::ptr_eq(&policy, &attached.schedule()));
    let visible = VaultClient::new(reopened.clone(), directory.0.clone())
        .await
        .unwrap();
    assert_eq!(
        visible
            .native_secret("runtime-canary".into())
            .await
            .unwrap()
            .as_str(),
        "SYNTHETIC_RETAINED_RUNTIME_CANARY"
    );
    let retried = retained.synchronize().await.unwrap();
    assert!(
        git.drained.is_cancelled(),
        "manual restart must drain the old Git owner"
    );
    assert!(retried.pending == 0 && !retried.catching_up);
    assert!(retained.is_running());
    let mut previous = retained.updates().unwrap();
    // Poll the ordinary manual API once, then dispose only its caller. No
    // credential delay, private fault hook or replacement attachment is used.
    let mut retry = Box::pin(retained.synchronize());
    let pending = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(retry.as_mut(), context).is_pending())
    })
    .await;
    assert!(pending, "actual manual I/O must have an outstanding future");
    drop(retry);
    tokio::time::timeout(Duration::from_secs(30), async {
        while previous.changed().await.is_ok() {
            previous.borrow_and_update();
        }
        loop {
            if let Ok(updates) = retained.updates() {
                break current_updates(updates, caught_up.cursor).await;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("discarding a manual caller must not stop its retained runtime");
    assert!(retained.is_running());
    NativeReplica::shutdown().await.unwrap();
    assert!(!retained.is_running());
    // Shutdown drains the real worker before allowing a new explicit batch.
    assert!(!visible.synchronize().await.unwrap().catching_up);
    logger.shutdown();
}

#[tokio::test]
#[ignore = "requires a fresh owned --fixture-sync3 PG/SMTP fixture and isolated native credentials"]
async fn actual_native_owner_has_fresh_followers_and_hands_off() {
    let mut logger = native_logger();
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
