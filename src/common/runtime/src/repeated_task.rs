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

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common_error::ext::ErrorExt;
use common_telemetry::{debug, error};
use snafu::{ResultExt, ensure};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Runtime;
use crate::error::{IllegalStateSnafu, Result, WaitGcTaskStopSnafu};
use crate::runtime::RuntimeTrait;

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

    /// Stops the task without waiting for it.
    ///
    /// It cancels the task, marks it not started, and aborts the task's future: a call
    /// that is in flight is dropped at its next await point instead of being allowed to
    /// finish, and this returns immediately after requesting the abort rather than
    /// waiting for it to take effect. Use [`RepeatedTask::stop`] when the caller must
    /// observe the task end, and this when the caller must not be delayed by a slow
    /// call, such as a shutdown path.
    ///
    /// Aborting drops the call's future in place, so everything it held is released and no
    /// further round runs. A call that must not be interrupted halfway through a side
    /// effect therefore does not belong in a task stopped this way.
    ///
    /// Like [`RepeatedTask::stop`], it is idempotent: calling it on a task that is not
    /// started, or calling it multiple times, succeeds without doing anything.
    pub fn stop_now(&self) -> Result<()> {
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

        // Dropping the handle would detach the task and let an in-flight call run to
        // completion; aborting it drops that call at its next await point instead.
        handle.abort();

        debug!("Repeated task {} aborted without waiting", self.name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicI32;

    use tokio::sync::Notify;

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

    /// Signals its own drop, which is how an aborted call reports that it was dropped.
    struct DroppedSignal(Arc<Notify>);

    impl Drop for DroppedSignal {
        fn drop(&mut self) {
            // Dropping is synchronous and `notify_one` keeps a permit for a waiter that
            // has not started waiting yet, so the signal cannot be lost.
            self.0.notify_one();
        }
    }

    /// A task whose first call signals that it started and then never returns.
    struct BlockingTask {
        entered: Arc<Notify>,
        dropped: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl TaskFunction<Error> for BlockingTask {
        fn name(&self) -> &str {
            "blocking"
        }

        async fn call(&mut self) -> Result<()> {
            let _dropped = DroppedSignal(self.dropped.clone());
            self.entered.notify_one();
            // Block the call forever: it only ever ends by being dropped, so the test
            // cannot pass by the call finishing on its own.
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_stop_now_aborts_an_in_flight_call() {
        common_telemetry::init_default_ut_logging();

        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(Notify::new());
        let task = RepeatedTask::new(
            // Far longer than the test, so only the initial delay can start the call.
            Duration::from_secs(3600),
            Box::new(BlockingTask {
                entered: entered.clone(),
                dropped: dropped.clone(),
            }),
        )
        .with_initial_delay(Some(Duration::ZERO));

        task.start(crate::global_runtime()).unwrap();
        // Waiting for the signal, not for a duration: the call either started or the
        // task is broken.
        entered.notified().await;
        assert!(task.started());

        // Returns while the call is still in flight.
        task.stop_now().unwrap();
        assert!(!task.started());

        // The abort drops the in-flight call. The bound only turns a dropped abort into
        // a failure; the signal itself is deterministic.
        tokio::time::timeout(Duration::from_secs(30), dropped.notified())
            .await
            .expect("stop_now must abort the call that is in flight");

        // Stopping again is a no-op, like `stop`: the handle was already taken.
        task.stop_now().unwrap();
        assert!(!task.started());
    }
}
