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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_error::prelude::ErrorExt;
use common_telemetry::logging;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{IllegalStateSnafu, Result, WaitGcTaskStopSnafu};
use crate::Runtime;

#[async_trait::async_trait]
pub trait TaskFunction<E: ErrorExt> {
    async fn call(&self) -> std::result::Result<(), E>;
    fn name(&self) -> &str;
}

pub type TaskFunctionRef<E> = Arc<dyn TaskFunction<E> + Send + Sync>;

pub struct RepeatedTask<E> {
    cancel_token: Mutex<Option<CancellationToken>>,
    gc_task_handle: Mutex<Option<JoinHandle<()>>>,
    started: AtomicBool,
    interval: Duration,
    task_fn: TaskFunctionRef<E>,
}

impl<E: ErrorExt> std::fmt::Display for RepeatedTask<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RepeatedTask({})", self.task_fn.name())
    }
}

impl<E: ErrorExt> std::fmt::Debug for RepeatedTask<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RepeatedTask")
            .field(&self.task_fn.name())
            .finish()
    }
}

impl<E: ErrorExt + 'static> RepeatedTask<E> {
    pub fn new(interval: Duration, task_fn: TaskFunctionRef<E>) -> Self {
        Self {
            cancel_token: Mutex::new(None),
            gc_task_handle: Mutex::new(None),
            started: AtomicBool::new(false),
            interval,
            task_fn,
        }
    }

    pub fn started(&self) -> bool {
        self.started.load(Ordering::Relaxed)
    }

    pub async fn start(&self, runtime: Runtime) -> Result<()> {
        let token = CancellationToken::new();
        let interval = self.interval;
        let child = token.child_token();
        let task_fn = self.task_fn.clone();
        // TODO(hl): Maybe spawn to a blocking runtime.
        let handle = runtime.spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = child.cancelled() => {
                        return;
                    }
                }
                if let Err(e) = task_fn.call().await {
                    logging::error!(e; "Failed to run repeated task: {}", task_fn.name());
                }
            }
        });
        *self.cancel_token.lock().await = Some(token);
        *self.gc_task_handle.lock().await = Some(handle);
        self.started.store(true, Ordering::Relaxed);

        logging::info!(
            "Repeated task {} started with interval: {:?}",
            self.task_fn.name(),
            self.interval
        );

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        let name = self.task_fn.name();
        ensure!(
            self.started
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok(),
            IllegalStateSnafu { name }
        );
        let token = self
            .cancel_token
            .lock()
            .await
            .take()
            .context(IllegalStateSnafu { name })?;
        let handle = self
            .gc_task_handle
            .lock()
            .await
            .take()
            .context(IllegalStateSnafu { name })?;

        token.cancel();
        handle.await.context(WaitGcTaskStopSnafu { name })?;

        logging::info!("Repeated task {} stopped", name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI32;

    use super::*;

    struct TickTask {
        n: AtomicI32,
    }

    #[async_trait::async_trait]
    impl TaskFunction<crate::error::Error> for TickTask {
        fn name(&self) -> &str {
            "test"
        }

        async fn call(&self) -> Result<()> {
            self.n.fetch_add(1, Ordering::Relaxed);

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_repeated_task() {
        common_telemetry::init_default_ut_logging();

        let task_fn = Arc::new(TickTask {
            n: AtomicI32::new(0),
        });

        let task = RepeatedTask::new(Duration::from_millis(100), task_fn.clone());

        task.start(crate::bg_runtime()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        task.stop().await.unwrap();

        assert_eq!(task_fn.n.load(Ordering::Relaxed), 5);
    }
}
