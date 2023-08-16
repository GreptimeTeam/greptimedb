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

#[async_trait::async_trait]
pub trait Scheduler {
    async fn schedule(&self, req: Job) -> Result<()>;
}

#[async_trait::async_trait]
pub trait Request {
    async fn handle_reqeust(&self) -> Result<()>;
}

pub struct LocalScheduler {
    sender: flume::Sender<Job>,
    handles: Vec<Option<JoinHandle<()>>>,
    cancel_token: CancellationToken,
    /// State of scheduler.
    state: Arc<AtomicU8>,
}

impl LocalScheduler {
    pub fn new() -> Self {
        // 创建一个用于发送任务的通道，容量为128
        let (tx, rx) = flume::bounded(128);
        // 创建一个取消令牌（Cancellation Token）
        let token = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(STATE_RUNNING));

        let mut handles = Vec::new();

        for _ in 0..CONSUMER_NUM {
            let child = token.child_token().clone();
            let receiver = rx.clone();
            let state = Arc::clone(&state);
            let handle = tokio::spawn(async move {
                while state.load(Ordering::Relaxed) == STATE_RUNNING {
                    tokio::select! {
                        // 1.如果子令牌被取消，就返回退出任务
                        _ = child.cancelled() => {
                            return;
                        }
                        // 2.等待接收任务请求
                        req_opt = receiver.recv_async() =>{
                            // 如果收到请求，等待请求中的异步任务完成
                            if let Ok(req) = req_opt{
                                req.await;
                            }
                        }
                    }
                    
                }
            });
            handles.push(Some(handle));
        }

        // 构造LocalScheduler结构体并返回
        Self {
            sender: tx,
            cancel_token: token,
            handles: handles,
            state,
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.cancel_token.cancel();
        self.state.store(STATE_STOP, Ordering::Relaxed);
        for handle in &mut self.handles {
            if let Some(handle) = handle.take() {
                handle.await.unwrap();
            }
        }
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_scheduler() {
        let mut local = LocalScheduler::new();
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