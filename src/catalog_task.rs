//! One bounded collection owned by the elected replica, concurrent with Vault
//! batches. Its owner lease survives cancellation until native cleanup ends.
use super::*;
use nddev_device_sync_application::repositories::catalog::collection::CollectionProgress;

#[derive(Default)]
pub(super) struct CollectionSlot {
    stopped: bool,
    work: Option<CollectionTask>,
    ready: Arc<tokio::sync::Notify>,
}
struct CollectionTask {
    collector: Arc<dyn CatalogCollector>,
    task: runtime::Task,
    result: tokio::sync::oneshot::Receiver<CollectionProgress>,
}
impl CollectionSlot {
    pub(super) fn notification(&self) -> Arc<tokio::sync::Notify> {
        self.ready.clone()
    }
    fn completed(&mut self) -> Option<CollectionProgress> {
        let work = self.work.as_mut()?;
        match work.result.try_recv() {
            Ok(progress) => {
                self.work.take();
                Some(progress)
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => None,
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                self.work.take();
                Some(CollectionProgress::default())
            }
        }
    }
    /// Continue a completed traversal without changing the Vault batch timer.
    /// A finished round waits for the next ordinary replica tick.
    pub(super) fn continue_ready(
        &mut self,
        collector: Arc<dyn CatalogCollector>,
        owner: Arc<std::fs::File>,
    ) -> CollectionProgress {
        let Some(progress) = self.completed() else {
            return CollectionProgress::default();
        };
        if progress.continue_traversal {
            self.start(collector, owner);
        }
        progress
    }
    pub(super) fn advance(
        &mut self,
        collector: Arc<dyn CatalogCollector>,
        owner: Arc<std::fs::File>,
    ) -> CollectionProgress {
        let progress = self.completed().unwrap_or_default();
        self.start(collector, owner);
        progress
    }
    fn start(&mut self, collector: Arc<dyn CatalogCollector>, owner: Arc<std::fs::File>) {
        if self.stopped || self.work.is_some() {
            return;
        }
        let collection = collector.clone().collect();
        let (send, result) = tokio::sync::oneshot::channel();
        let ready = self.ready.clone();
        let task = runtime::spawn(async move {
            let _owner = owner;
            let progress = collection.await;
            let _ = send.send(progress);
            ready.notify_one();
        });
        self.work = Some(CollectionTask {
            collector,
            task,
            result,
        });
    }
    pub(super) fn cancel(&mut self) {
        self.stopped = true;
        self.interrupt();
    }
    pub(super) fn interrupt(&self) {
        if let Some(work) = &self.work {
            work.collector.cancel();
        }
    }
    pub(super) fn stop(&mut self) -> Option<runtime::Task> {
        self.cancel();
        self.work.take().map(|work| {
            // Do not abort: cancellation must reap native Git before releasing
            // the owner lease. The collector owns finite I/O deadlines.
            work.task
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use nddev_device_sync_application::repositories::catalog::collection::Collection;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    struct FiniteCollection(tokio::sync::Mutex<TcpStream>);
    impl CatalogCollector for FiniteCollection {
        fn collect(self: Arc<Self>) -> Collection {
            Box::pin(async move {
                let mut byte = [0];
                self.0.lock().await.read_exact(&mut byte).await.unwrap();
                CollectionProgress {
                    local_changes: false,
                    continue_traversal: byte[0] != b'!',
                }
            })
        }
        fn cancel(&self) {}
    }
    #[tokio::test]
    async fn completed_io_continues_without_replica_ticks_and_a_finished_round_stays_idle() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let directory =
                std::env::temp_dir().join(format!("nds-collection-wake-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&directory).unwrap();
            let journal = crate::journal::Journal::open(directory.join("vault.sqlite")).unwrap();
            let owner = Arc::new(journal.owner_lock().unwrap());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let stream = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut peer, _) = listener.accept().await.unwrap();
            let collector = Arc::new(FiniteCollection(tokio::sync::Mutex::new(stream)));
            let mut slot = CollectionSlot::default();
            let ready = slot.notification();
            slot.advance(collector.clone(), owner.clone());
            for byte in *b"ab!" {
                peer.write_all(&[byte]).await.unwrap();
                ready.notified().await;
                let progress = slot.continue_ready(collector.clone(), owner.clone());
                assert_eq!(progress.continue_traversal, byte != b'!');
                assert!(matches!(journal.owner_lock(), Err(Error::Busy)));
            }
            assert!(slot.work.is_none());
            assert!(
                tokio::time::timeout(Duration::from_millis(20), ready.notified())
                    .await
                    .is_err()
            );
            assert!(slot.stop().is_none());
            // A late completion may not re-admit work after cancellation.
            assert!(
                !slot
                    .continue_ready(collector, owner.clone())
                    .continue_traversal
            );
            assert!(slot.work.is_none());
            drop(owner);
            drop(journal.owner_lock().unwrap());
            std::fs::remove_dir_all(directory).unwrap();
        })
        .await
        .unwrap();
    }
    struct SlowCollection {
        stream: tokio::sync::Mutex<TcpStream>,
        cancelled: tokio_util::sync::CancellationToken,
        draining: tokio_util::sync::CancellationToken,
        release: tokio_util::sync::CancellationToken,
    }
    impl CatalogCollector for SlowCollection {
        fn collect(self: Arc<Self>) -> Collection {
            Box::pin(async move {
                let mut stream = self.stream.lock().await;
                let mut byte = [0];
                tokio::select! {
                    _ = self.cancelled.cancelled() => {},
                    result = stream.read_exact(&mut byte) => { result.unwrap(); },
                }
                self.draining.cancel();
                self.release.cancelled().await;
                CollectionProgress {
                    local_changes: true,
                    continue_traversal: true,
                }
            })
        }
        fn cancel(&self) {
            self.cancelled.cancel();
        }
    }
    #[tokio::test]
    async fn slow_collection_retains_owner_through_cancel_and_drain() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let directory =
                std::env::temp_dir().join(format!("nds-collection-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&directory).unwrap();
            let journal = crate::journal::Journal::open(directory.join("vault.sqlite")).unwrap();
            let owner = Arc::new(journal.owner_lock().unwrap());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let stream = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut server, _) = listener.accept().await.unwrap();
            let collector = Arc::new(SlowCollection {
                stream: tokio::sync::Mutex::new(stream),
                cancelled: Default::default(),
                draining: Default::default(),
                release: Default::default(),
            });
            let mut slot = CollectionSlot::default();
            slot.advance(collector.clone(), owner.clone());
            drop(owner);
            // Independent real I/O progresses while collection is pending.
            server.write_all(b"x").await.unwrap();
            collector.draining.cancelled().await;
            assert!(matches!(journal.owner_lock(), Err(Error::Busy)));
            slot.cancel(); // Natural replica exit must retain the join handle.
            assert!(matches!(journal.owner_lock(), Err(Error::Busy)));
            let task = slot.stop().unwrap();
            assert!(matches!(journal.owner_lock(), Err(Error::Busy)));
            assert!(slot.stop().is_none());
            collector.release.cancel();
            task.join().await;
            let next = journal.owner_lock().unwrap();
            drop(next);
            std::fs::remove_dir_all(directory).unwrap();
        })
        .await
        .unwrap();
    }
}
