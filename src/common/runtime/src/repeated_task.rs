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
use std::sync::Mutex;
use std::time::Duration;

use common_error::ext::ErrorExt;
use common_telemetry::{debug, error};
use snafu::{ensure, ResultExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{IllegalStateSnafu, Result, WaitGcTaskStopSnafu};
use crate::Runtime;

/// Task to execute repeatedly.
#[async_trait::async_trait]
pub trait TaskFunction<E> {
    /// Invoke the task.
    async fn call(&mut self) -> std::result::Result<(), E>;

    /// Name of the task.
    fn name(&self) -> &str;
}

pub type BoxedTaskFunction<E> = Box<dyn TaskFunction<E> + Send + Sync + 'static>;

struct TaskInner<E> {
    /// The repeated task handle. This handle is Some if the task is started.
    task_handle: Option<JoinHandle<()>>,

    /// The task_fn to run. This is Some if the task is not started.
    task_fn: Option<BoxedTaskFunction<E>>,
}

pub struct RepeatedTask<E> {
    name: String,
    cancel_token: CancellationToken,
    inner: Mutex<TaskInner<E>>,
    started: AtomicBool,
    interval: Duration,
    initial_delay: Option<Duration>,
}

impl<E> std::fmt::Display for RepeatedTask<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RepeatedTask({})", self.name)
    }
}

impl<E> std::fmt::Debug for RepeatedTask<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RepeatedTask").field(&self.name).finish()
    }
}

impl<E> Drop for RepeatedTask<E> {
    fn drop(&mut self) {
        let inner = self.inner.get_mut().unwrap();
        if inner.task_handle.is_some() {
            // Cancel the background task.
            self.cancel_token.cancel();
        }
    }
}

impl<E: ErrorExt + 'static> RepeatedTask<E> {
    /// Creates a new repeated task. The `initial_delay` is the delay before the first execution.
    /// `initial_delay` default is None, the initial interval uses the `interval`.
    /// You can use `with_initial_delay` to set the `initial_delay`.
    pub fn new(interval: Duration, task_fn: BoxedTaskFunction<E>) -> Self {
        Self {
            name: task_fn.name().to_string(),
            cancel_token: CancellationToken::new(),
            inner: Mutex::new(TaskInner {
                task_handle: None,
                task_fn: Some(task_fn),
            }),
            started: AtomicBool::new(false),
            interval,
            initial_delay: None,
        }
    }

    pub fn with_initial_delay(mut self, initial_delay: Option<Duration>) -> Self {
        self.initial_delay = initial_delay;
        self
    }

    pub fn started(&self) -> bool {
        self.started.load(Ordering::Relaxed)
    }

    pub fn start(&self, runtime: Runtime) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        ensure!(
            inner.task_fn.is_some(),
            IllegalStateSnafu { name: &self.name }
        );

        let child = self.cancel_token.child_token();
        // Safety: The task is not started.
        let mut task_fn = inner.task_fn.take().unwrap();
        let interval = self.interval;
        let mut initial_delay = self.initial_delay;
        // TODO(hl): Maybe spawn to a blocking runtime.
        let handle = runtime.spawn(async move {
            loop {
                let sleep_time = initial_delay.take().unwrap_or(interval);
                if sleep_time > Duration::ZERO {
                    tokio::select! {
                        _ = tokio::time::sleep(sleep_time) => {}
                        _ = child.cancelled() => {
                            return;
                        }
                    }
                }
                if let Err(e) = task_fn.call().await {
                    error!(e; "Failed to run repeated task: {}", task_fn.name());
                }
            }
        });
        inner.task_handle = Some(handle);
        self.started.store(true, Ordering::Relaxed);

        debug!(
            "Repeated task {} started with interval: {:?}",
            self.name, self.interval
        );

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        let handle = {
            let mut inner = self.inner.lock().unwrap();
            if inner.task_handle.is_none() {
                // We allow stop the task multiple times.
                return Ok(());
            }

            self.cancel_token.cancel();
            self.started.store(false, Ordering::Relaxed);
            // Safety: The task is not stopped.
            inner.task_handle.take().unwrap()
        };

        handle
            .await
            .context(WaitGcTaskStopSnafu { name: &self.name })?;

        debug!("Repeated task {} stopped", self.name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI32;
    use std::sync::Arc;

    use super::*;
    use crate::error::Error;

    struct TickTask {
        n: Arc<AtomicI32>,
    }

    #[async_trait::async_trait]
    impl TaskFunction<Error> for TickTask {
        fn name(&self) -> &str {
            "test"
        }

        async fn call(&mut self) -> Result<()> {
            let _ = self.n.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_repeated_task() {
        common_telemetry::init_default_ut_logging();

        let n = Arc::new(AtomicI32::new(0));
        let task_fn = TickTask { n: n.clone() };

        let task = RepeatedTask::new(Duration::from_millis(100), Box::new(task_fn));

        task.start(crate::global_runtime()).unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        task.stop().await.unwrap();

        assert!(n.load(Ordering::Relaxed) >= 3);
    }

    #[tokio::test]
    async fn test_repeated_task_prior_exec() {
        common_telemetry::init_default_ut_logging();

        let n = Arc::new(AtomicI32::new(0));
        let task_fn = TickTask { n: n.clone() };

        let task = RepeatedTask::new(Duration::from_millis(100), Box::new(task_fn))
            .with_initial_delay(Some(Duration::ZERO));

        task.start(crate::global_runtime()).unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        task.stop().await.unwrap();

        assert!(n.load(Ordering::Relaxed) >= 4);
    }
}
