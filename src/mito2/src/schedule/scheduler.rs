use std::pin::Pin;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::Result;

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

        for _ in 0..num {
            let child = token.child_token().clone();
            let receiver = rx.clone();
            let state = Arc::clone(&state);
            let handle = tokio::spawn(async move {
                while state.load(Ordering::Relaxed) == STATE_RUNNING {
                    tokio::select! {
                        _ = child.cancelled() => {
                            return;
                        }
                        req_opt = receiver.recv_async() =>{
                            if let Ok(req) = req_opt{
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
    use std::time::Duration;

    #[tokio::test]
    async fn test_scheduler() {
        let mut local = LocalScheduler::new(3, 32);
        local
            .schedule(Box::pin(async {
                println!("hello1");
            }))
            .await
            .unwrap();

        local
            .schedule(Box::pin(async {
                println!("hello2");
            }))
            .await
            .unwrap();

        local
            .schedule(Box::pin(async {
                println!("hello3");
            }))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(3)).await; 
        local.stop().await.unwrap();
    }
}