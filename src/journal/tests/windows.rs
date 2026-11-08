use super::*;
use std::process::Stdio;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::timeout,
};

/// Real native regression for SQLite's Windows mapped-SHM open boundary:
/// https://sqlite.org/forum/info/ba7afbd0e81501118563617430fe06d0aca052e55ece2b0d457f8e16fcf85bba
/// The external process keeps a mapping, without SQLite's DMS lock. Journal's
/// ordinary connection path must still recover and read its actual WAL state.
#[tokio::test]
async fn mapped_shm_allows_existing_wal_journal_reads_on_windows() {
    let (owned, journal, key) = owned_journal();
    let connection = journal.connection().unwrap();
    connection
        .set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )
        .unwrap();
    let original = encrypted(&key, "mapped-shm", "record");
    journal.enqueue(original.clone(), None, 0, None).unwrap();
    let expected = journal.stats().unwrap();
    drop(connection);
    // The test-only connection flag retains actual SQLite sidecars after the
    // last close. Production journal configuration is unchanged.
    let shm = owned.0.join("replica.db-shm");
    assert!(std::fs::metadata(&shm).unwrap().len() > 0);
    let script = owned.0.join("mapped-shm.ps1");
    std::fs::write(&script, include_str!("mapped_shm.ps1")).unwrap();
    let mut holder = Command::new("powershell.exe")
        .args(["-NoLogo", "-NoProfile", "-NonInteractive", "-File"])
        .arg(&script)
        .env("NDS_FIXTURE_SHM", &shm)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("Windows must provide its native mapping fixture host");
    let observed: Result<(), &'static str> = async {
        let mut stdout = holder.stdout.take().ok_or("missing mapping handshake")?;
        let mut ready = [0; b"NDS_SHM_READY\n".len()];
        timeout(Duration::from_secs(10), stdout.read_exact(&mut ready))
            .await
            .map_err(|_| "mapping readiness timed out")?
            .map_err(|_| "mapping holder exited before readiness")?;
        if ready.as_slice() != b"NDS_SHM_READY\n" {
            return Err("unexpected mapping handshake");
        }
        let actual = journal
            .stats()
            .map_err(|_| "mapped-SHM journal read failed")?;
        if actual != expected {
            return Err("mapped-SHM recovery changed journal metadata");
        }
        let pending = journal
            .pending()
            .map_err(|_| "mapped-SHM pending read failed")?;
        if pending.len() != 1 || pending[0].body != original {
            return Err("mapped-SHM recovery changed the retained encrypted original");
        }
        Ok(())
    }
    .await;
    // Reap this exact child before asserting, including the failure path. Never
    // delete a fixture while the mapping holder is still using its sidecar.
    let stopped = timeout(Duration::from_secs(5), async {
        let mut stdin = holder.stdin.take().ok_or(std::io::ErrorKind::BrokenPipe)?;
        stdin.write_all(b"R").await?;
        drop(stdin);
        holder.wait().await
    })
    .await;
    let status = match stopped {
        Ok(Ok(status)) => Some(status),
        _ => {
            let _ = holder.start_kill();
            timeout(Duration::from_secs(5), holder.wait())
                .await
                .ok()
                .and_then(Result::ok)
        }
    };
    if status.is_none() {
        // An uncertain child outcome never authorizes removing its live files.
        std::mem::forget(owned);
    }
    assert!(
        status.is_some_and(|status| status.success()),
        "mapping holder did not exit cleanly; probe={observed:?}"
    );
    assert_eq!(observed, Ok(()));
}
