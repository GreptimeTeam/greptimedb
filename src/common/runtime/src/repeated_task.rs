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
use common_telemetry::logging;
use snafu::{ensure, ResultExt};
use tokio::task::JoinHandle;
use tokio::time::Instant;
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

    /// Generates the next interval.
    interval_generator: Option<Box<dyn IntervalGenerator>>,
}

pub trait IntervalGenerator: Send + Sync {
    /// Returns the next interval.
    fn next(&mut self) -> Duration;

    /// Returns whether the interval is regular and the interval if it is regular.
    fn is_regular(&self) -> (bool, Option<Duration>);
}

impl std::fmt::Debug for dyn IntervalGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut binding = f.debug_struct("IntervalGenerator");
        let mut builder = binding.field("is_regular", &self.is_regular().0);
        if self.is_regular().0 {
            builder = builder.field("interval", &self.is_regular().1);
        }
        builder.finish()
    }
}

impl IntervalGenerator for Duration {
    fn next(&mut self) -> Duration {
        *self
    }

    fn is_regular(&self) -> (bool, Option<Duration>) {
        (true, Some(*self))
    }
}

impl From<Duration> for Box<dyn IntervalGenerator> {
    fn from(value: Duration) -> Self {
        Box::new(value)
    }
}

pub struct FirstZeroInterval {
    first: bool,
    interval: Duration,
}

impl FirstZeroInterval {
    pub fn new(interval: Duration) -> Self {
        Self {
            first: false,
            interval,
        }
    }
}

impl IntervalGenerator for FirstZeroInterval {
    fn next(&mut self) -> Duration {
        if !self.first {
            self.first = true;
            Duration::ZERO
        } else {
            self.interval
        }
    }

    fn is_regular(&self) -> (bool, Option<Duration>) {
        (false, None)
    }
}

impl From<FirstZeroInterval> for Box<dyn IntervalGenerator> {
    fn from(value: FirstZeroInterval) -> Self {
        Box::new(value)
    }
}

pub struct RepeatedTask<E> {
    name: String,
    cancel_token: CancellationToken,
    inner: Mutex<TaskInner<E>>,
    started: AtomicBool,
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
    pub fn new<I: Into<Box<dyn IntervalGenerator>>>(
        interval: I,
        task_fn: BoxedTaskFunction<E>,
    ) -> Self {
        Self {
            name: task_fn.name().to_string(),
            cancel_token: CancellationToken::new(),
            inner: Mutex::new(TaskInner {
                task_handle: None,
                task_fn: Some(task_fn),
                interval_generator: Some(interval.into()),
            }),
            started: AtomicBool::new(false),
        }
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

        let mut interval_generator = inner.interval_generator.take().unwrap();
        let child = self.cancel_token.child_token();
        // Safety: The task is not started.
        let mut task_fn = inner.task_fn.take().unwrap();
        let interval = interval_generator.next();
        let interval_str = format!("{:?}", interval_generator);
        // TODO(hl): Maybe spawn to a blocking runtime.
        let handle = runtime.spawn(async move {
            let sleep = tokio::time::sleep(interval);

            tokio::pin!(sleep);
            loop {
                tokio::select! {
                _ = &mut sleep => {
                    let interval = interval_generator.next();
                    sleep.as_mut().reset(Instant::now() + interval);
                }
                _ = child.cancelled() => {
                    return;
                }}
                if let Err(e) = task_fn.call().await {
                    logging::error!(e; "Failed to run repeated task: {}", task_fn.name());
                }
            }
        });
        inner.task_handle = Some(handle);
        self.started.store(true, Ordering::Relaxed);

        logging::debug!(
            "Repeated task {} started with interval: {}",
            self.name,
            interval_str
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

        logging::debug!("Repeated task {} stopped", self.name);

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

        task.start(crate::bg_runtime()).unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        task.stop().await.unwrap();

        assert_eq!(n.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn test_repeated_task_prior_exec() {
        common_telemetry::init_default_ut_logging();

        let n = Arc::new(AtomicI32::new(0));
        let task_fn = TickTask { n: n.clone() };
        let interval = FirstZeroInterval::new(Duration::from_millis(100));
        let task = RepeatedTask::new(interval, Box::new(task_fn));

        task.start(crate::bg_runtime()).unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        task.stop().await.unwrap();

        assert_eq!(n.load(Ordering::Relaxed), 6);
    }
}
