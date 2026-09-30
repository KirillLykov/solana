use {
    agave_wake_channel::{Receiver, Sender, bounded},
    crossbeam_channel::{TryRecvError, TrySendError},
    log::error,
    solana_streamer::streamer::{ChannelSend, ChannelTryRecv},
    std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread::{self, JoinHandle},
    },
};

struct Shared {
    live_workers: AtomicUsize,
}

struct WorkerGuard(Arc<Shared>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.live_workers.fetch_sub(1, Ordering::Release);
    }
}

pub(super) struct WorkerPool {
    worker_handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    pub(super) fn build<J, F>(
        thread_name_prefix: &str,
        num_workers: usize,
        job_queue_capacity: usize,
        run: F,
    ) -> (PoolSender<J>, PoolReceiver<J>, Self)
    where
        J: Send + 'static,
        F: Fn(J, usize) + Send + Sync + 'static,
    {
        assert_ne!(num_workers, 0, "worker pool must have at least one worker");
        let (job_sender, job_receiver) = bounded::<J>(job_queue_capacity);
        let shared = Arc::new(Shared {
            live_workers: AtomicUsize::new(num_workers),
        });
        let run = Arc::new(run);
        let worker_handles = (0..num_workers)
            .map(|worker_id| {
                let job_receiver = job_receiver.clone();
                let shared = Arc::clone(&shared);
                let run = Arc::clone(&run);
                thread::Builder::new()
                    .name(format!("{thread_name_prefix}{worker_id:02}"))
                    .stack_size(2 * 1024 * 1024)
                    .spawn(move || {
                        let _guard = WorkerGuard(shared);
                        while let Ok(job) = job_receiver.recv() {
                            run(job, worker_id);
                        }
                    })
                    .expect("failed to spawn worker thread")
            })
            .collect();
        (
            PoolSender {
                inner: job_sender,
                shared,
            },
            PoolReceiver(job_receiver),
            Self { worker_handles },
        )
    }

    pub(super) fn join(mut self) -> thread::Result<()> {
        let mut result = Ok(());

        for worker_handle in self.worker_handles.drain(..) {
            if let Err(err) = worker_handle.join() {
                error!("worker thread failed: {err:?}");
                if result.is_ok() {
                    result = Err(err);
                }
            }
        }

        result
    }
}

pub(super) struct PoolSender<J> {
    inner: Sender<J>,
    shared: Arc<Shared>,
}

impl<J> Clone for PoolSender<J> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<J: Send + 'static> ChannelSend<J> for PoolSender<J> {
    fn try_send(&self, job: J) -> Result<(), TrySendError<J>> {
        if self.shared.live_workers.load(Ordering::Acquire) == 0 {
            return Err(TrySendError::Disconnected(job));
        }
        self.inner.try_send(job)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

pub(super) struct PoolReceiver<J>(Receiver<J>);

impl<J> Clone for PoolReceiver<J> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<J> ChannelTryRecv<J> for PoolReceiver<J> {
    fn try_recv(&self) -> Result<J, TryRecvError> {
        self.0.try_recv()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::{TrySendError, bounded, unbounded},
        solana_streamer::evicting_sender::EvictingSender,
    };

    #[test]
    fn test_evicting_sender_wraps_worker_queue() {
        let (started_sender, started_receiver) = bounded(1);
        let (release_sender, release_receiver) = bounded(1);
        let (processed_sender, processed_receiver) = unbounded();
        let (sender, receiver, pool) =
            WorkerPool::build("testWorker", 1, 1, move |job, _worker_id| {
                if job == 0 {
                    started_sender.send(()).unwrap();
                    release_receiver.recv().unwrap();
                }
                processed_sender.send(job).unwrap();
            });
        let sender = EvictingSender::new(sender, receiver);

        sender.try_send(0).unwrap();
        started_receiver.recv().unwrap();
        sender.try_send(1).unwrap();
        assert_eq!(sender.try_send(2), Err(TrySendError::Full(1)));

        release_sender.send(()).unwrap();
        drop(sender);
        pool.join().unwrap();
        assert_eq!(processed_receiver.into_iter().collect::<Vec<_>>(), [0, 2]);
    }

    #[test]
    fn test_sender_reports_no_live_workers() {
        let (worker_exited_sender, worker_exited_receiver) = bounded(1);
        let (sender, _receiver, pool) =
            WorkerPool::build("testWorker", 1, 1, move |(), _worker_id| {
                worker_exited_sender.send(()).unwrap();
                panic!("worker failure");
            });

        sender.try_send(()).unwrap();
        worker_exited_receiver.recv().unwrap();
        assert!(pool.join().is_err());
        assert_eq!(sender.try_send(()), Err(TrySendError::Disconnected(())));
    }
}
