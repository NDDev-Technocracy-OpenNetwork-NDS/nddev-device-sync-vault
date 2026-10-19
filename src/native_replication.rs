//! One retained desktop/headless owner, independent of presentation attachment.
//! The existing task elects the native owner across processes. No service, RPC,
//! additional scheduler or credential store is introduced here.
use super::*;
use crate::{Error, crypto::Scope};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

// A process serves one selected native account/endpoint. Changing that selection
// drains the previous owner before starting another; reopening reuses it.
static CURRENT: tokio::sync::Mutex<Option<Arc<NativeReplica>>> =
    tokio::sync::Mutex::const_new(None);

pub struct NativeReplica {
    scope: Scope,
    directory: PathBuf,
    identity: Arc<IdentityClient>,
    authority: nddev_device_sync_accounts::LocalUseFence,
    vault: Arc<VaultClient>,
    publication: Arc<Mutex<()>>,
    policy: Arc<Mutex<Arc<Mutex<ReplicaSchedule>>>>,
    task: Mutex<Option<ReplicaTask>>,
    control: tokio::sync::Mutex<()>,
    stopping: AtomicBool,
    client_version: Option<String>,
    delivery: Mutex<Option<nddev_device_sync_accounts::telemetry::Delivery>>,
}

impl NativeReplica {
    /// Attach after ordinary sign-in and device preparation. A separate Rust
    /// identity retains authority: cancelling a GUI facade cannot cancel it.
    pub async fn attach(
        facade: &Arc<IdentityClient>,
        directory: PathBuf,
        client_version: Option<String>,
    ) -> Result<Arc<Self>, Error> {
        if !directory.is_absolute() {
            return Err(Error::Invalid);
        }
        let lifetime = facade.local_use_lifetime();
        let fence = facade
            .local_use_fence_from(&lifetime)
            .await
            .map_err(replica_auth_error)?;
        let scope = crate::client::scope(facade)?;
        let mut selected = CURRENT.try_lock().map_err(|_| Error::Busy)?;
        if let Some(current) = selected.as_ref() {
            if current.scope == scope && current.directory == directory && current.is_running() {
                match current
                    .identity
                    .validate_local_use(&current.authority)
                    .await
                {
                    Ok(()) => {
                        facade
                            .validate_local_use(&fence)
                            .await
                            .map_err(replica_auth_error)?;
                        return Ok(current.clone());
                    }
                    Err(ClientError::RevalidationRequired | ClientError::DeviceRevoked) => {}
                    Err(error) => return Err(replica_auth_error(error)),
                }
            }
            current.stop().await?;
        }
        let identity = Arc::new(IdentityClient::new(&facade.origin()).map_err(replica_auth_error)?);
        let restored = identity.refresh().await.map_err(replica_auth_error)?;
        if restored.session_verified {
            let name = facade
                .view()
                .devices
                .into_iter()
                .find(|device| device.is_current && !device.revoked)
                .ok_or(Error::Locked)?
                .display_name;
            identity
                .prepare_device(name)
                .await
                .map_err(replica_auth_error)?;
        }
        if crate::client::scope(&identity)? != scope {
            return Err(Error::Locked);
        }
        let vault = Arc::new(VaultClient::new(identity.clone(), directory.clone()).await?);
        let authority = identity
            .local_use_fence()
            .await
            .map_err(replica_auth_error)?;
        facade
            .validate_local_use(&fence)
            .await
            .map_err(replica_auth_error)?;
        let owner = Arc::new(Self {
            scope,
            directory,
            identity,
            authority,
            vault,
            publication: Arc::default(),
            policy: Arc::default(),
            task: Mutex::default(),
            control: tokio::sync::Mutex::default(),
            stopping: AtomicBool::new(false),
            client_version,
            delivery: Mutex::default(),
        });
        owner.start(Duration::ZERO);
        *selected = Some(owner.clone());
        tracing::info!(
            event.name = "sync.runtime.started",
            module = "vault",
            outcome = "ok"
        );
        Ok(owner)
    }

    fn start(&self, delay: Duration) {
        if self.stopping.load(Ordering::Acquire) {
            return;
        }
        let schedule = self.schedule();
        *self.task.lock().unwrap_or_else(|error| error.into_inner()) = Some(ReplicaTask::spawn(
            ReplicaContext {
                identity: self.identity.clone(),
                vault: Arc::downgrade(&self.vault),
                publication: self.publication.clone(),
                policy_slot: self.policy.clone(),
                schedule,
                client_version: self.client_version.clone(),
            },
            delay,
            true,
        ));
    }
    pub fn is_running(&self) -> bool {
        !self.stopping.load(Ordering::Acquire)
            && (self
                .task
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .is_some_and(ReplicaTask::is_running)
                || self.control.try_lock().is_err())
    }
    pub fn schedule(&self) -> Arc<Mutex<ReplicaSchedule>> {
        self.policy
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    /// One process exporter follows the retained identity, independently of UI
    /// attachment disposal. The assembly supplies its existing SDK feeds once.
    pub fn ensure_delivery(
        &self,
        start: impl FnOnce(
            Arc<IdentityClient>,
        ) -> Option<nddev_device_sync_accounts::telemetry::Delivery>,
    ) {
        if self.stopping.load(Ordering::Acquire) {
            return;
        }
        let mut delivery = self
            .delivery
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if delivery.as_ref().is_none_or(|delivery| delivery.finished()) {
            delivery.take();
            *delivery = start(self.identity.clone());
        }
    }
    pub fn delivery_snapshot(&self) -> nddev_device_sync_accounts::view::DeliveryView {
        self.delivery
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map_or_else(Default::default, |delivery| delivery.snapshot())
    }
    pub async fn snapshot(&self) -> Result<VaultView, Error> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Locked);
        }
        self.identity
            .validate_local_use(&self.authority)
            .await
            .map_err(replica_auth_error)?;
        let view = self.vault.refresh_local().await?;
        self.identity
            .validate_local_use(&self.authority)
            .await
            .map_err(replica_auth_error)?;
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Locked);
        }
        Ok(view)
    }
    pub fn updates(
        &self,
    ) -> Result<tokio::sync::watch::Receiver<Option<Result<VaultView, Error>>>, Error> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Locked);
        }
        self.task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(ReplicaTask::updates)
            .ok_or(Error::Busy)
    }
    pub fn wake(&self) {
        if !self.stopping.load(Ordering::Acquire)
            && let Some(task) = self
                .task
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
        {
            task.set_active(true);
        }
    }
    async fn stop_task(&self) -> Result<(), Error> {
        let task = self
            .task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(mut task) = task
            && let Some(work) = task.cancel()
        {
            work.join().await;
        }
        self.vault.drain_replica().await
    }
    /// Explicit manual retry owns policy replacement. Reopening/foregrounding
    /// only attaches or wakes and never clears a capacity refusal.
    pub async fn synchronize(&self) -> Result<VaultView, Error> {
        let _control = self.control.try_lock().map_err(|_| Error::Busy)?;
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Locked);
        }
        let lifetime = self.identity.local_use_lifetime();
        self.stop_task().await?;
        let fence = self
            .identity
            .local_use_fence_from(&lifetime)
            .await
            .map_err(replica_auth_error)?;
        let schedule = Arc::new(Mutex::new(ReplicaSchedule::default()));
        {
            let _publication = self
                .publication
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.identity
                .check_local_use_lifetime(&fence)
                .map_err(replica_auth_error)?;
            *self
                .policy
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = schedule.clone();
        }
        let attempt = self.vault.synchronize_attempt(false).await;
        let (result, history) = match attempt {
            Ok(attempt) => (
                attempt.result.map(|progress| progress.view),
                attempt.history,
            ),
            Err(error) => (Err(error), None),
        };
        let next = {
            let mut policy = schedule.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(history) = history {
                policy.observe_history(history);
            }
            policy.after(batch_outcome(&result))
        };
        if let Some(delay) = next {
            self.start(delay);
        }
        result
    }
    /// Stop before logout, endpoint/account replacement or process shutdown.
    /// Native I/O drain is separate from joining the cancelled async task.
    pub async fn stop(&self) -> Result<(), Error> {
        if !self.stopping.swap(true, Ordering::AcqRel) {
            self.identity.cancel();
        }
        let _control = runtime::timeout(
            nddev_device_sync_application::replica::BATCH_DEADLINE,
            self.control.lock(),
        )
        .await
        .map_err(|_| Error::Busy)?;
        let owned_task = self
            .task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some();
        self.stop_task().await?;
        self.delivery
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if owned_task {
            tracing::info!(
                event.name = "sync.runtime.stopped",
                module = "vault",
                outcome = "stopped"
            );
        }
        Ok(())
    }
    pub async fn shutdown() -> Result<(), Error> {
        let mut selected = runtime::timeout(
            nddev_device_sync_application::replica::BATCH_DEADLINE,
            CURRENT.lock(),
        )
        .await
        .map_err(|_| Error::Busy)?;
        if let Some(owner) = selected.as_ref() {
            owner.stop().await?;
        }
        selected.take();
        Ok(())
    }
}
