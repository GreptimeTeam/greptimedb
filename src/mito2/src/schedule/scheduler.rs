use std::pin::Pin;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use common_telemetry::info;

pub type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

const STATE_RUNNING: u8 = 0;
const STATE_STOP: u8 = 1;
const STATE_AWAIT_TERMINATION: u8 = 2;

// producer and consumer count
const CONSUMER_NUM: u8 = 2;

/// [Scheduler] defines a set of API to schedule Jobs
#[async_trait::async_trait]
pub trait Scheduler {
    /// Schedules a Job
    async fn schedule(&mut self, req: Job) -> Result<()>;

    /// Stops scheduler
    async fn stop(&mut self, await_termination: bool) -> Result<()>;
}

pub struct LocalScheduler {
    sender: Option<flume::Sender<Job>>,
    handles: Vec<Option<JoinHandle<()>>>,
    /// Token used to halt the scheduler
    cancel_token: CancellationToken,
    /// State of scheduler.
    state: Arc<AtomicU8>,
}

impl LocalScheduler {
    /// cap: flume bounded cap 
    /// num: the number of bounded receiver
    pub fn new(cap: usize, num: usize) -> Self {
        let (tx, rx) = flume::bounded(cap);
        let token = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(STATE_RUNNING));

        let mut handles = Vec::with_capacity(num);

        for id in 0..num {
            let child = token.child_token().clone();
            let receiver = rx.clone();
            let state = Arc::clone(&state);
            let handle = tokio::spawn(async move {
                while state.load(Ordering::Relaxed) == STATE_RUNNING {
                    info!("Task scheduler loop.");
                    tokio::select! {
                        _ = child.cancelled() => {
                            info!("Task scheduler cancelled.");
                            return;
                        }
                        req_opt = receiver.recv_async() =>{
                            if let Ok(req) = req_opt{
                                info!("handle {} is doing", id);
                                req.await;
                            }
                        }
                    }
                    
                }
                // For correctness, we need to poll requests from fifo again.
                if state.load(Ordering::Relaxed) == STATE_AWAIT_TERMINATION {
                    while let Ok(req) = receiver.recv() {
                        req.await;
                    }
                }
            });
            handles.push(Some(handle));
        }

        Self {
            sender: Some(tx),
            cancel_token: token,
            handles: handles,
            state,
        }
    }

    #[inline]
    fn running(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STATE_RUNNING
    }
}

#[async_trait::async_trait]
impl Scheduler for LocalScheduler {
    async fn schedule(&mut self, req: Job) -> Result<()> {
        self.sender.as_mut().unwrap().send_async(req).await.unwrap();
        Ok(())
    }

    async fn stop(&mut self, await_termination: bool) -> Result<()> {
        let state = if await_termination {
            STATE_AWAIT_TERMINATION
        } else {
            STATE_STOP
        };
        self.state.store(state, Ordering::Relaxed);
        self.cancel_token.cancel();
        let _  = self.sender.take();

        for handle in &mut self.handles {
            if let Some(handle) = handle.take() {
                handle.await.unwrap();
            }
        }
        Ok(())
    }
}

impl Drop for LocalScheduler {
    fn drop(&mut self) {
        self.state.store(STATE_STOP, Ordering::Relaxed);
        self.cancel_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;
    use std::time::Duration;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn test_scheduler() {
        let mut local = LocalScheduler::new(3, 3);

        local
            .schedule(Box::pin(async move {
                println!("hello1");
            }))
            .await
            .unwrap();
        
        local
            .schedule(Box::pin(async move {
                println!("hello2");
            }))
            .await
            .unwrap();

        local
            .schedule(Box::pin(async move {
                println!("hello3");
            }))
            .await
            .unwrap();

    }

    #[tokio::test]
    async fn test_sum_cap() {

        let task_size = 1000;
        let sum = Arc::new(AtomicI32::new(0));
        let mut local = LocalScheduler::new(3, task_size);

        for _ in 0..task_size {
            let sum = Arc::clone(&sum);
            local.schedule(Box::pin(async move {
                sum.fetch_add(1, Ordering::Relaxed);
            }))
            .await
            .unwrap();
        }
        local.stop(true).await.unwrap();

        assert_eq!(sum.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_sum_consumer_num() {

        let task_size = 1000;
        let sum = Arc::new(AtomicI32::new(0));
        let mut local = LocalScheduler::new(task_size, 3);

        for _ in 0..task_size{
            let sum = Arc::clone(&sum);
            local.schedule(Box::pin(async move {
                sum.fetch_add(1, Ordering::Relaxed);
            }))
            .await
            .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(1)).await;
        local.stop(true).await.unwrap();

        assert_eq!(sum.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_scheduler_many() {
        let task_size = 1000;

        let barrier = Arc::new(Barrier::new(task_size + 1));
        let mut local: LocalScheduler = LocalScheduler::new(20, task_size + 1);

        for _ in 0..task_size {
            let barrier_clone = barrier.clone();
            local.schedule(Box::pin(async move {
                barrier_clone.wait().await;
            }))
            .await
            .unwrap();
        }
        
        barrier.wait().await;
    }

}
