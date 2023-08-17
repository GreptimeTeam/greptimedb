// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use common_telemetry::info;

pub type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

/// state of scheduler
const STATE_RUNNING: u8 = 0;
const STATE_STOP: u8 = 1;
const STATE_AWAIT_TERMINATION: u8 = 2;

/// producer and consumer count
const CONSUMER_NUM: u8 = 2;

/// [Scheduler] defines a set of API to schedule Jobs
#[async_trait::async_trait]
pub trait Scheduler {
    /// Schedule a Job
    async fn schedule(&mut self, req: Job) -> Result<()>;

    /// Stops scheduler. If `await_termination` is set to true, the scheduler will
    /// wait until all tasks are processed.
    async fn stop(&mut self, await_termination: bool) -> Result<()>;
}

/// Request scheduler based on local state.
pub struct LocalScheduler {
    /// send jobs to flume bounded
    sender: Option<flume::Sender<Job>>,
    /// handle task
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
                    info!("Task scheduler loop.");
                    tokio::select! {
                        _ = child.cancelled() => {
                            info!("Task scheduler cancelled.");
                            return;
                        }
                        req_opt = receiver.recv_async() =>{
                            if let Ok(req) = req_opt{
                                req.await;
                            }
                        }
                    }
                    
                }
                // For correctness, we need to handle all tasks
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
