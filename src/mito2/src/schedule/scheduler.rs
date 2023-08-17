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
    async fn schedule(&self, req: Job) -> Result<()>;

    /// Stops scheduler
    async fn stop(&mut self) -> Result<()>;
}

pub struct LocalScheduler {
    sender: flume::Sender<Job>,
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
                                println!("handle {} is doing", id);
                                req.await;
                            }
                        }
                    }
                    
                }
            });
            handles.push(Some(handle));
        }

        Self {
            sender: tx,
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
    async fn schedule(&self, req: Job) -> Result<()> {
        self.sender.send_async(req).await.unwrap();
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.cancel_token.cancel();
        self.state.store(STATE_STOP, Ordering::Relaxed);
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
    use std::time::{Instant, Duration};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    struct CountdownLatch {
        counter: std::sync::Mutex<usize>,
        notify: Notify,
    }

    impl CountdownLatch {
        fn new(size: usize) -> Self {
            Self {
                counter: std::sync::Mutex::new(size),
                notify: Notify::new(),
            }
        }

        fn countdown(&self) {
            let mut counter = self.counter.lock().unwrap();
            if *counter >= 1 {
                *counter -= 1;
                if *counter == 0 {
                    self.notify.notify_waiters();
                }
            }
        }

        /// Users should only call this once.
        async fn wait(&self) {
            self.notify.notified().await
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let mut local = LocalScheduler::new(32, 3);
        let latch = Arc::new(CountdownLatch::new(3));

        let latch_clone1 = latch.clone();
        local
            .schedule(Box::pin(async move {
                println!("hello1");
                latch_clone1.countdown();
            }))
            .await
            .unwrap();
        
        let latch_clone2= latch.clone();
        local
            .schedule(Box::pin(async move {
                println!("hello2");
                latch_clone2.countdown();
            }))
            .await
            .unwrap();

        let latch_clone3= latch.clone();
        local
            .schedule(Box::pin(async move {
                println!("hello3");
                latch_clone3.countdown();
            }))
            .await
            .unwrap();

        latch.wait().await;

        local.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_sum() {

        let task_size = 1000;
        let sum = Arc::new(AtomicI32::new(0));
        let mut local = LocalScheduler::new(32, 10);
        let latch = Arc::new(CountdownLatch::new(task_size));
        let start = Instant::now();
        for _ in 0..task_size {
            let sum = Arc::clone(&sum);
            let latch_clone = latch.clone();
            local.schedule(Box::pin(async move {
                sum.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(3)).await;
                latch_clone.countdown();
            }))
            .await
            .unwrap();
        }

        latch.wait().await;
        let end = Instant::now();
        println!("Elapsed time: {:?}s", (end - start).as_secs_f32());
        local.stop().await.unwrap();

        assert_eq!(sum.load(Ordering::Relaxed), 1000);

    }

    #[tokio::test]
    async fn test_scheduler_many() {
        let task_size = 100;

        let latch = Arc::new(CountdownLatch::new(task_size + 1));
        let mut local = LocalScheduler::new(20, 100);

        for _ in 0..task_size {
            let latch_clone = latch.clone();
            local.schedule(Box::pin(async move {
                latch_clone.countdown();
                latch_clone.wait().await;

            }))
            .await
            .unwrap();
        }
        
        tokio::time::sleep(Duration::from_millis(1000)).await;
        latch.countdown();
        local.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_scheduler_interval() {
        
        let task_size = 100;
        let latch = Arc::new(CountdownLatch::new(task_size));
        let mut local = LocalScheduler::new(32, 3);
        let sum = Arc::new(Mutex::new(0));
        
        for _ in 0..task_size / 2{
            let sum = Arc::clone(&sum);
            let latch_clone = latch.clone();
            local.schedule(Box::pin(async move {
                let mut sum = sum.lock().unwrap();
                *sum += 1;
                latch_clone.countdown();

            }))
            .await
            .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        for _ in 0..task_size / 2{
            let sum = Arc::clone(&sum);
            let latch_clone = latch.clone();
            local.schedule(Box::pin(async move {
                let mut sum = sum.lock().unwrap();
                *sum += 1;
                latch_clone.countdown();
            }))
            .await
            .unwrap();
        }

        latch.wait().await;
        local.stop().await.unwrap();

        assert_eq!(*sum.lock().unwrap(), task_size);

    }

}