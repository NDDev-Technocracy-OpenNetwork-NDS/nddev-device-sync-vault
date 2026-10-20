//! One bounded replica-task adapter shared by native/Flutter/browser/CLI callers.
//! Core owns scheduling and recovery policy; Vault owns the existing journal I/O.
use crate::{
    VaultClient,
    view::{VaultError, VaultView},
};
use nddev_device_sync_accounts::{
    IdentityClient,
    runtime::{self, Instant},
    view::ClientError,
};
use nddev_device_sync_application::{
    lifecycle::{BatchOutcome, CapacityCheck, ReplicaSchedule},
    replica::CapacityProbeOutcome,
};
use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tracing::Instrument;

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[path = "native_replication.rs"]
pub mod native;

/// Existing session/publication ownership supplied by the assembly. Starting or
/// replacing a task must hold `publication`; it is not an authorization grant.
pub struct ReplicaContext {
    pub identity: Arc<IdentityClient>,
    pub vault: Weak<VaultClient>,
    pub publication: Arc<Mutex<()>>,
    pub policy_slot: Arc<Mutex<Arc<Mutex<ReplicaSchedule>>>>,
    pub schedule: Arc<Mutex<ReplicaSchedule>>,
    /// Continuous clients publish their device observation once. A bounded CLI
    /// read does not rewrite installation metadata.
    pub client_version: Option<String>,
}

struct Activity {
    active: bool,
    interruption: Arc<()>,
}

pub struct ReplicaTask {
    active: tokio::sync::watch::Sender<Activity>,
    task: Option<runtime::Task>,
    updates: tokio::sync::watch::Receiver<Option<Result<VaultView, VaultError>>>,
    #[cfg(not(target_arch = "wasm32"))]
    owner: Arc<std::sync::atomic::AtomicBool>,
}
impl ReplicaTask {
    pub fn spawn(context: ReplicaContext, initial_delay: Duration, active: bool) -> Self {
        let ReplicaContext {
            identity,
            vault: weak,
            publication,
            policy_slot,
            schedule,
            client_version,
        } = context;
        let (active, mut enabled) = tokio::sync::watch::channel(Activity {
            active,
            interruption: Arc::new(()),
        });
        let (progress, updates) = tokio::sync::watch::channel(None);
        #[cfg(not(target_arch = "wasm32"))]
        let owner = Arc::new(std::sync::atomic::AtomicBool::new(false));
        #[cfg(not(target_arch = "wasm32"))]
        let acquired = owner.clone();
        let task = runtime::spawn(async move {
            let mut schedule = schedule;
            let mut delay = initial_delay;
            let mut published = client_version.is_none();
            #[cfg(not(target_arch = "wasm32"))]
            let mut ownership = None;
            #[cfg(not(target_arch = "wasm32"))]
            let mut observation: Option<crate::client::ReplicaObservation> = None;
            #[cfg(target_arch = "wasm32")]
            let mut restored_schedule = false;
            loop {
                if !enabled.borrow().active {
                    if enabled.changed().await.is_err() {
                        break;
                    }
                    delay = Duration::ZERO;
                    continue;
                }
                tokio::select! {
                    changed = enabled.changed() => { if changed.is_err() { break; } delay = Duration::ZERO; continue; }
                    _ = runtime::sleep(delay) => {}
                }
                let mut capacity_check = None;
                let mut inbox_only = false;
                let mut inbox_at_head = None;
                let mut inbox_empty = false;
                let mut capacity_outcome = None;
                let mut attempt_history = None;
                let lifetime = identity.local_use_lifetime();
                let Some(vault) = weak.upgrade() else {
                    break;
                };
                let Some(mut result) = run_enabled(
                    &mut enabled,
                    observe("sync.replica.batch", async {
                        // Authority maintenance belongs to this same replica
                        // lifetime even when presentation ticks are suspended.
                        identity
                            .maintain_authorization()
                            .await
                            .map_err(replica_auth_error)?;
                        #[cfg(not(target_arch = "wasm32"))]
                        let lease = if ownership.is_none() {
                            match vault.replica_owner().await {
                                Ok(lease) => Some(lease),
                                Err(VaultError::Busy) => {
                                    if observation.as_ref().is_none_or(|value| value.expired()) {
                                        observation =
                                            Some(vault.begin_replica_observation().await?);
                                    }
                                    let observed = vault
                                        .observe_replica(
                                            observation.as_ref().ok_or(VaultError::Invalid)?,
                                        )
                                        .await?;
                                    if observed.caught_up {
                                        observation = None;
                                    }
                                    inbox_at_head = Some(observed.inbox_at_head);
                                    return Ok(observed.view);
                                }
                                Err(error) => return Err(error),
                            }
                        } else {
                            None
                        };
                        #[cfg(not(target_arch = "wasm32"))]
                        let restore = lease.is_some();
                        #[cfg(target_arch = "wasm32")]
                        let restore = !restored_schedule;
                        if restore {
                            // Restore durable refusal facts before the first batch on
                            // every platform. Reload/attachment is not a user retry.
                            let restored =
                                Arc::new(Mutex::new(vault.restored_replica_schedule().await?));
                            let fence = identity
                                .local_use_fence_from(&lifetime)
                                .await
                                .map_err(replica_auth_error)?;
                            {
                                let _publication = publication
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner());
                                identity
                                    .check_local_use_lifetime(&fence)
                                    .map_err(replica_auth_error)?;
                                let mut current = policy_slot
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner());
                                if !Arc::ptr_eq(&current, &schedule) {
                                    return Err(VaultError::Cancelled);
                                }
                                *current = restored.clone();
                                schedule = restored;
                            }
                            #[cfg(not(target_arch = "wasm32"))]
                            {
                                ownership = lease;
                                acquired.store(true, std::sync::atomic::Ordering::Release);
                                tracing::info!(
                                    event.name = "sync.replica.owner",
                                    module = "vault",
                                    outcome = "ok"
                                );
                            }
                            #[cfg(target_arch = "wasm32")]
                            {
                                restored_schedule = true;
                            }
                        }
                        let probe = {
                            let policy = schedule.lock().unwrap_or_else(|error| error.into_inner());
                            capacity_check = policy.capacity_check();
                            policy.capacity_probe()
                        };
                        inbox_only = capacity_check.is_some() && probe.is_none();
                        if probe.is_some() {
                            let attempt = vault.probe_capacity_attempt().await?;
                            attempt_history = attempt.history;
                            capacity_outcome = attempt.result?;
                            return vault.refresh_local().await;
                        }
                        if inbox_only {
                            let attempt = vault.synchronize_attempt(true).await?;
                            attempt_history = attempt.history;
                            let progress = attempt.result?;
                            inbox_at_head = Some(progress.inbox_at_head);
                            inbox_empty = progress.caught_up;
                            return Ok(progress.view);
                        }
                        let attempt = vault.synchronize_attempt(false).await?;
                        attempt_history = attempt.history;
                        let progress = attempt.result?;
                        inbox_at_head = Some(progress.inbox_at_head);
                        let view = progress.view;
                        if !published
                            && !view.catching_up
                            && !schedule
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .is_capacity_blocked()
                        {
                            let attempt = vault
                                .publish_states_attempt(device_observations(
                                    &identity,
                                    client_version.as_deref().ok_or(VaultError::Invalid)?,
                                )?)
                                .await?;
                            attempt_history = attempt.history;
                            let view = attempt.result?;
                            published = true;
                            return Ok(view);
                        }
                        Ok(view)
                    }),
                )
                .await
                else {
                    delay = Duration::ZERO;
                    continue;
                };
                {
                    let mut policy = schedule.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(history) = attempt_history {
                        policy.observe_history(history);
                        if inbox_only
                            && result.is_ok()
                            && inbox_at_head == Some(true)
                            && let Some(check) = &capacity_check
                        {
                            policy.observe_capacity_inbox(check, history);
                        }
                    }
                    if inbox_only && inbox_at_head == Some(true) && inbox_empty && result.is_ok() {
                        capacity_outcome = Some(CapacityProbeOutcome::NoEligibleWork);
                    }
                }
                if let (Some(check), Some(completed)) = (&capacity_check, capacity_outcome)
                    && let Ok(fence) = identity.local_use_fence_from(&lifetime).await
                {
                    let replacement = {
                        let _publication = publication.lock().unwrap_or_else(|e| e.into_inner());
                        if identity.check_local_use_lifetime(&fence).is_ok() {
                            recovered_policy(&policy_slot, &schedule, check, completed)
                        } else {
                            None
                        }
                    };
                    if let Some(next) = replacement {
                        schedule = next;
                        tracing::info!(
                            event.name = "sync.capacity.recovered",
                            module = "vault",
                            outcome = "ok"
                        );
                    }
                }
                let outcome = match (result.is_ok(), inbox_at_head) {
                    (true, Some(true))
                        if inbox_only
                            || result.as_ref().is_ok_and(|v| !v.sync_error.is_empty()) =>
                    {
                        BatchOutcome::InboxHead
                    }
                    (true, Some(false)) if inbox_only => BatchOutcome::InboxMore,
                    _ => batch_outcome(&result),
                };
                let sample = if matches!(outcome, BatchOutcome::Retryable) {
                    uuid::Uuid::new_v4().as_u128() as u16
                } else {
                    0
                };
                let next = schedule
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .after_with_sample(outcome, sample);
                if let Ok(view) = &mut result {
                    project_policy(
                        view,
                        &schedule.lock().unwrap_or_else(|error| error.into_inner()),
                    );
                }
                progress.send_replace(Some(result));
                let Some(next) = next else {
                    break;
                };
                delay = next;
            }
        });
        Self {
            active,
            task: Some(task),
            updates,
            #[cfg(not(target_arch = "wasm32"))]
            owner,
        }
    }
    pub fn set_active(&self, active: bool) {
        set_activity(&self.active, active);
    }
    pub fn is_active(&self) -> bool {
        self.active.borrow().active
    }
    pub fn is_running(&self) -> bool {
        self.task.as_ref().is_some_and(|task| !task.is_finished())
    }
    /// This running task acquired native ownership. False after cancellation
    /// does not assert that an outstanding native worker has finished draining.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn is_owner(&self) -> bool {
        self.is_running() && self.owner.load(std::sync::atomic::Ordering::Acquire)
    }
    /// Latest actual completed batch; there is no unbounded notification queue.
    pub fn updates(&self) -> tokio::sync::watch::Receiver<Option<Result<VaultView, VaultError>>> {
        self.updates.clone()
    }
    /// Abort now and retain the future for the caller's existing bounded join.
    pub fn cancel(&mut self) -> Option<runtime::Task> {
        let task = self.task.take();
        if let Some(task) = &task {
            task.abort();
        }
        task
    }
}

/// Preserve the canonical upload refusal alongside a successful inbox read.
/// A read cannot clear upload policy or disguise a terminal/current read error.
pub fn project_policy(view: &mut VaultView, schedule: &ReplicaSchedule) {
    if schedule.is_capacity_blocked() {
        if view.sync_error.is_empty() {
            view.sync_error = VaultError::CapacityExhausted.code();
        }
        view.catching_up = true;
    }
}
impl Drop for ReplicaTask {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

// Caller holds publication and has checked the original Accounts lifetime.
pub fn recovered_policy(
    slot: &Mutex<Arc<Mutex<ReplicaSchedule>>>,
    expected: &Arc<Mutex<ReplicaSchedule>>,
    check: &CapacityCheck,
    outcome: CapacityProbeOutcome,
) -> Option<Arc<Mutex<ReplicaSchedule>>> {
    let mut current = slot.lock().unwrap_or_else(|e| e.into_inner());
    if !Arc::ptr_eq(&current, expected) {
        return None;
    }
    let replacement = expected
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .recovered_capacity(check, outcome)?;
    let replacement = Arc::new(Mutex::new(replacement));
    *current = replacement.clone();
    Some(replacement)
}

pub fn batch_outcome(result: &Result<VaultView, VaultError>) -> BatchOutcome {
    match result {
        Ok(view) if view.catching_up => BatchOutcome::CatchingUp,
        Ok(_) => BatchOutcome::CaughtUp,
        Err(VaultError::CapacityExhausted) => BatchOutcome::CapacityExhausted,
        Err(VaultError::Locked) => BatchOutcome::Locked,
        Err(_) => BatchOutcome::Retryable,
    }
}

pub fn device_observations(
    identity: &IdentityClient,
    client_version: &str,
) -> Result<Vec<nddev_device_sync_domain::state::Snapshot>, VaultError> {
    use nddev_device_sync_domain::state::{Snapshot, State};
    let view = identity.view();
    let device = view
        .devices
        .into_iter()
        .find(|value| value.is_current && !value.revoked)
        .ok_or(VaultError::Locked)?;
    let observed_at_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
        .try_into()
        .map_err(|_| VaultError::Invalid)?;
    let capture = |data| Snapshot {
        schema_version: 1,
        observed_at_ms,
        data,
    };
    let expires = time::OffsetDateTime::parse(
        &view.expires_at,
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(|_| VaultError::Invalid)?
    .unix_timestamp();
    let millis = |seconds: i64| {
        u64::try_from(seconds)
            .ok()
            .and_then(|value| value.checked_mul(1000))
            .ok_or(VaultError::Invalid)
    };
    let values = vec![
        capture(State::Device {
            display_name: device.display_name,
            platform: device.platform,
            client_version: client_version.into(),
        }),
        capture(State::Account {
            auth_method: view.auth_method,
            checked_at_ms: millis(view.session_checked_at)?,
            expires_at_ms: millis(expires)?,
        }),
    ];
    Ok(values)
}

pub fn replica_auth_error(error: ClientError) -> VaultError {
    match error {
        ClientError::Busy | ClientError::OnlineVerificationRequired => VaultError::Busy,
        ClientError::Server(code) if code == "server_busy" => VaultError::Busy,
        ClientError::Server(code) if code == "sync_capacity_exhausted" => {
            VaultError::CapacityExhausted
        }
        ClientError::DeviceRevoked | ClientError::RevalidationRequired => VaultError::Locked,
        _ => VaultError::Unavailable,
    }
}

async fn observe<T>(
    event: &'static str,
    work: impl std::future::Future<Output = Result<T, VaultError>>,
) -> Result<T, VaultError> {
    let span = nddev_device_sync_telemetry::operation_span("vault", "process")
        .map_err(|_| VaultError::Unavailable)?;
    let completion = span.clone();
    async {
        let start = Instant::now();
        let result = Box::pin(work).await;
        match &result {
            Ok(_) => tracing::info!(event.name=event, module="vault", duration_ms=start.elapsed().as_millis() as u64, outcome="ok"),
            Err(error) => {
                nddev_device_sync_telemetry::finish_operation_span(&completion, matches!(error, VaultError::Unavailable|VaultError::Authentication|VaultError::StorageFull|VaultError::CapacityExhausted));
                tracing::warn!(event.name=event, module="vault", duration_ms=start.elapsed().as_millis() as u64, error.type=%error.code(), outcome="error");
            }
        }
        result
    }.instrument(span).await
}

// A wake preserves the current I/O future. A pause changes its interruption
// identity even if pause/resume coalesce before the receiver is polled.
fn set_activity(sender: &tokio::sync::watch::Sender<Activity>, active: bool) {
    sender.send_modify(|state| {
        if state.active && !active {
            state.interruption = Arc::new(());
        }
        state.active = active;
    });
}

async fn run_enabled<T>(
    enabled: &mut tokio::sync::watch::Receiver<Activity>,
    work: impl std::future::Future<Output = T>,
) -> Option<T> {
    let epoch = {
        let state = enabled.borrow_and_update();
        if !state.active {
            return None;
        }
        state.interruption.clone()
    };
    let mut work = Box::pin(work);
    loop {
        tokio::select! {
            result = &mut work => {
                let state = enabled.borrow();
                return (state.active && Arc::ptr_eq(&epoch, &state.interruption)).then_some(result);
            }
            changed = enabled.changed() => {
                if changed.is_err() { return None; }
                let state = enabled.borrow_and_update();
                if !state.active || !Arc::ptr_eq(&epoch, &state.interruption) { return None; }
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod execution_tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::watch,
    };

    #[tokio::test]
    async fn wake_keeps_the_same_actual_tcp_read_alive() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut server, _) = listener.accept().await.unwrap();
            let work = async move {
                let mut byte = [0];
                client.read_exact(&mut byte).await.unwrap();
                byte[0]
            };
            let mut work = Box::pin(work);
            let (polls, mut observed) = watch::channel(0u32);
            let mut count = 0;
            let tracked = std::future::poll_fn(move |cx| {
                count += 1;
                polls.send_replace(count);
                work.as_mut().poll(cx)
            });
            let (active, mut enabled) = watch::channel(Activity {
                active: true,
                interruption: Arc::new(()),
            });
            let task = tokio::spawn(async move { run_enabled(&mut enabled, tracked).await });
            observed.changed().await.unwrap();
            observed.borrow_and_update();
            set_activity(&active, true);
            // A real repoll must occur after the wake. Cancelling/recreating the
            // read instead closes this observation channel and the TCP peer.
            observed.changed().await.unwrap();
            server.write_all(b"x").await.unwrap();
            assert_eq!(task.await.unwrap(), Some(b'x'));
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn coalesced_pause_resume_cancels_pending_tcp_io() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut server, _) = listener.accept().await.unwrap();
            let (started, ready) = tokio::sync::oneshot::channel();
            let work = async move {
                started.send(()).unwrap();
                let mut byte = [0];
                client.read_exact(&mut byte).await.unwrap();
                byte[0]
            };
            let (active, mut enabled) = watch::channel(Activity {
                active: true,
                interruption: Arc::new(()),
            });
            let task = tokio::spawn(async move { run_enabled(&mut enabled, work).await });
            ready.await.unwrap();
            set_activity(&active, false);
            set_activity(&active, true);
            assert_eq!(task.await.unwrap(), None);
            let mut byte = [0];
            assert_eq!(server.read(&mut byte).await.unwrap(), 0);
        })
        .await
        .unwrap();
    }
}
