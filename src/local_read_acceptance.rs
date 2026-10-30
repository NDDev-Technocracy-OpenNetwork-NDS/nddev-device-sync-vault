//! Actual local-read publication after joined SQLite I/O and same-owner renewal.
use super::*;
use crate::storage::Command;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Read {
    EmptyClipboard,
    Records,
    States,
    Clipboard,
    Secret,
}

impl Read {
    fn selected(self) -> fn(&Command) -> bool {
        match self {
            Self::Records => |command| matches!(command, Command::Records { .. }),
            Self::States => |command| matches!(command, Command::States { .. }),
            Self::EmptyClipboard | Self::Clipboard => {
                |command| matches!(command, Command::Clipboard)
            }
            Self::Secret => |command| matches!(command, Command::Entity { .. }),
        }
    }

    async fn read(self, vault: &VaultClient) -> Result<(), Error> {
        let correct = match self {
            Self::EmptyClipboard => vault.clipboard().await?.is_none(),
            Self::Records => {
                let page = vault.records(String::new()).await?;
                page.items.iter().any(|item| {
                    item.id == "read-canary" && item.title == "SYNTHETIC_LOCAL_READ_TITLE"
                })
            }
            Self::States => vault
                .states(String::new())
                .await?
                .items
                .iter()
                .any(|item| item.kind == "device_state"),
            Self::Clipboard => vault
                .clipboard()
                .await?
                .is_some_and(|(_, text)| text.as_str() == "SYNTHETIC_LOCAL_READ_CLIPBOARD"),
            Self::Secret => {
                vault.native_secret("read-canary".into()).await?.as_str()
                    == "SYNTHETIC_LOCAL_READ_SECRET"
            }
        };
        if correct { Ok(()) } else { Err(Error::Invalid) }
    }
}

async fn encrypted_rows(vault: &VaultClient) -> Result<Vec<Vec<u8>>, Error> {
    let mut rows = Vec::new();
    for command in [
        Command::Records {
            after: String::new(),
        },
        Command::States { after: None },
        Command::Clipboard,
        Command::Stats,
    ] {
        rows.push(serde_json::to_vec(&vault.journal.call(command).await?).map_err(dependency)?);
    }
    Ok(rows)
}

async fn late_read(vault: &VaultClient, kind: Read) {
    let before = encrypted_rows(vault).await.unwrap();
    let identity = &vault.identity;
    let original = identity.view();
    let lifetime = identity.local_use_lifetime();
    let (entered, release) = vault.journal.hold_read_reply(kind.selected());
    let mut read = Box::pin(kind.read(vault));
    // No spawned test task can outlive an assertion. The held reply is reached
    // only after the real SQLite worker and its resource permits have joined.
    tokio::select! {
        early = &mut read => panic!("{kind:?} returned before the held reply: {early:?}"),
        reached = tokio::time::timeout(Duration::from_secs(10), entered) => {
            reached.expect("joined read reply deadline").expect("read reply sender");
        }
    }
    identity.cancel();
    let renewed = tokio::time::timeout(Duration::from_secs(5), async {
        identity.refresh().await?;
        identity
            .prepare_device("Native vault acceptance".into())
            .await
    })
    .await;
    let original_lost = identity
        .check_captured_local_use_lifetime(&lifetime)
        .is_err();
    let released = release.send(());
    let result = tokio::time::timeout(Duration::from_secs(10), &mut read).await;
    drop(read);
    // Always release and finish the actual read before checking the renewal or
    // its outcome. A renewed grant admits only a new explicit read.
    renewed.expect("same-owner renewal deadline").unwrap();
    released.unwrap();
    assert!(original_lost);
    assert!(matches!(result, Ok(Err(Error::Locked))), "{kind:?}");
    let current = identity.view();
    assert!(
        current.authenticated
            && current.tenant_id == original.tenant_id
            && current.user_id == original.user_id
            && current.current_device_id == original.current_device_id
    );
    kind.read(vault).await.unwrap();
    assert!(encrypted_rows(vault).await.unwrap() == before);
}

#[tokio::test]
#[ignore = "requires fresh owned --fixture-sync3 PG/SMTP and isolated native credentials"]
async fn actual_local_read_publication_retains_original_lifetime() {
    let mut logger = super::tests::fixture_logger();
    let identity = super::tests::admitted_fixture_identity().await;
    struct Owned(PathBuf);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory =
        Owned(std::env::temp_dir().join(format!("nds-local-read-{}", uuid::Uuid::new_v4())));
    let vault = VaultClient::new(identity, directory.0.clone())
        .await
        .unwrap();
    assert!(!vault.synchronize().await.unwrap().catching_up);
    late_read(&vault, Read::EmptyClipboard).await;
    vault
        .save_record(
            Some("read-canary".into()),
            0,
            None,
            "SYNTHETIC_LOCAL_READ_TITLE".into(),
            String::new(),
            Zeroizing::new("SYNTHETIC_LOCAL_READ_SECRET".into()),
        )
        .await
        .unwrap();
    vault
        .stage_states(
            crate::replication::device_observations(&vault.identity, env!("CARGO_PKG_VERSION"))
                .unwrap(),
        )
        .await
        .unwrap();
    vault
        .share_clipboard(Zeroizing::new("SYNTHETIC_LOCAL_READ_CLIPBOARD".into()))
        .await
        .unwrap();
    assert!(!vault.synchronize().await.unwrap().catching_up);
    for kind in [Read::Records, Read::States, Read::Clipboard, Read::Secret] {
        kind.read(&vault).await.unwrap();
        late_read(&vault, kind).await;
    }
    let before = encrypted_rows(&vault).await.unwrap();
    let (entered, release) = vault.journal.hold_read_reply(Read::Records.selected());
    let mut abandoned = Box::pin(vault.records(String::new()));
    tokio::select! {
        _ = &mut abandoned => panic!("read returned before the joined reply hold"),
        reached = tokio::time::timeout(Duration::from_secs(10), entered) => {
            reached.unwrap().unwrap();
        }
    }
    drop(abandoned);
    drop(release);
    Read::Records.read(&vault).await.unwrap();
    assert!(encrypted_rows(&vault).await.unwrap() == before);
    logger.shutdown();
}
