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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::watch::{self, Receiver};
use tokio::sync::RwLock;

use crate::error::{self, Error, Result};

/// The state of a async task.
#[derive(Debug, Default, Clone)]
pub(crate) enum TaskState<T: Send + Sync + Clone> {
    Error(Arc<Error>),
    #[default]
    Running,
    Done(T),
}

pub(crate) type TaskWatcher<T> = Receiver<TaskState<T>>;

async fn wait<T: Send + Sync + Clone>(watcher: &mut TaskWatcher<T>) -> Result<T> {
    loop {
        watcher
            .changed()
            .await
            .context(error::WatchAsyncTaskChangeSnafu)?;

        let r = &*watcher.borrow();
        match r {
            TaskState::Error(err) => return Err(err.clone()).context(error::AsyncTaskExecuteSnafu),
            TaskState::Running => {}
            TaskState::Done(value) => return Ok(value.clone()),
        }
    }
}

/// The running async task.
pub(crate) struct Task<T: Send + Sync + Clone> {
    watcher: TaskWatcher<T>,
}

pub(crate) struct TaskTrackerInner<T: Send + Sync + Clone> {
    state: HashMap<RegionId, Task<T>>,
}

impl<T: Send + Sync + Clone> Default for TaskTrackerInner<T> {
    fn default() -> Self {
        TaskTrackerInner {
            state: HashMap::new(),
        }
    }
}

/// Tracks the long-running async tasks.
#[derive(Clone)]
pub(crate) struct TaskTracker<T: Send + Sync + Clone> {
    inner: Arc<RwLock<TaskTrackerInner<T>>>,
}

/// The registering result of a async task.
pub(crate) enum RegisterResult<T: Send + Sync + Clone> {
    // The watcher of the running task.
    Busy(TaskWatcher<T>),
    // The watcher of the newly registered task.
    Running(TaskWatcher<T>),
}

impl<T: Send + Sync + Clone> RegisterResult<T> {
    pub(crate) fn into_watcher(self) -> TaskWatcher<T> {
        match self {
            RegisterResult::Busy(inner) => inner,
            RegisterResult::Running(inner) => inner,
        }
    }

    /// Returns true if it's [RegisterResult::Busy].
    pub(crate) fn is_busy(&self) -> bool {
        matches!(self, RegisterResult::Busy(_))
    }

    #[cfg(test)]
    /// Returns true if it's [RegisterResult::Running].
    pub(crate) fn is_running(&self) -> bool {
        matches!(self, RegisterResult::Running(_))
    }
}

/// The result of waiting.
pub(crate) enum WaitResult<T> {
    Timeout,
    Finish(Result<T>),
}

#[cfg(test)]
impl<T> WaitResult<T> {
    /// Returns true if it's [WaitResult::Timeout].
    pub(crate) fn is_timeout(&self) -> bool {
        matches!(self, WaitResult::Timeout)
    }

    /// Into the [WaitResult::Timeout] if it's.
    pub(crate) fn into_finish(self) -> Option<Result<T>> {
        match self {
            WaitResult::Timeout => None,
            WaitResult::Finish(result) => Some(result),
        }
    }
}

impl<T: Send + Sync + Clone + 'static> TaskTracker<T> {
    /// Returns an empty [AsyncTaskTracker].
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(TaskTrackerInner::default())),
        }
    }

    /// Waits for a [RegisterResult] and returns a [WaitResult].
    pub(crate) async fn wait(
        &self,
        watcher: &mut TaskWatcher<T>,
        timeout: Duration,
    ) -> WaitResult<T> {
        match tokio::time::timeout(timeout, wait(watcher)).await {
            Ok(result) => WaitResult::Finish(result),
            Err(_) => WaitResult::Timeout,
        }
    }

    /// Tries to register a new async task, returns [RegisterResult::Busy] if previous task is running.
    pub(crate) async fn try_register(
        &self,
        region_id: RegionId,
        fut: BoxFuture<'static, Result<T>>,
    ) -> RegisterResult<T> {
        let mut inner = self.inner.write().await;
        if let Some(task) = inner.state.get(&region_id) {
            RegisterResult::Busy(task.watcher.clone())
        } else {
            let moved_inner = self.inner.clone();
            let (tx, rx) = watch::channel(TaskState::<T>::Running);
            common_runtime::spawn_global(async move {
                match fut.await {
                    Ok(result) => {
                        let _ = tx.send(TaskState::Done(result));
                    }
                    Err(err) => {
                        let _ = tx.send(TaskState::Error(Arc::new(err)));
                    }
                };
                moved_inner.write().await.state.remove(&region_id);
            });
            inner.state.insert(
                region_id,
                Task {
                    watcher: rx.clone(),
                },
            );

            RegisterResult::Running(rx.clone())
        }
    }

    #[cfg(test)]
    async fn watcher(&self, region_id: RegionId) -> Option<TaskWatcher<T>> {
        self.inner
            .read()
            .await
            .state
            .get(&region_id)
            .map(|task| task.watcher.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use store_api::storage::RegionId;
    use tokio::sync::oneshot;

    use crate::heartbeat::task_tracker::{wait, TaskTracker};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestResult {
        value: i32,
    }

    #[tokio::test]
    async fn test_async_task_tracker_register() {
        let tracker = TaskTracker::<TestResult>::new();
        let region_id = RegionId::new(1024, 1);
        let (tx, rx) = oneshot::channel::<()>();

        let result = tracker
            .try_register(
                region_id,
                Box::pin(async move {
                    let _ = rx.await;
                    Ok(TestResult { value: 1024 })
                }),
            )
            .await;

        assert!(result.is_running());

        let result = tracker
            .try_register(
                region_id,
                Box::pin(async move { Ok(TestResult { value: 1023 }) }),
            )
            .await;
        assert!(result.is_busy());
        let mut watcher = tracker.watcher(region_id).await.unwrap();
        // Triggers first future return.
        tx.send(()).unwrap();

        assert_eq!(
            TestResult { value: 1024 },
            wait(&mut watcher).await.unwrap()
        );
        let result = tracker
            .try_register(
                region_id,
                Box::pin(async move { Ok(TestResult { value: 1022 }) }),
            )
            .await;
        assert!(result.is_running());
    }

    #[tokio::test]
    async fn test_async_task_tracker_wait_timeout() {
        let tracker = TaskTracker::<TestResult>::new();
        let region_id = RegionId::new(1024, 1);
        let (tx, rx) = oneshot::channel::<()>();

        let result = tracker
            .try_register(
                region_id,
                Box::pin(async move {
                    let _ = rx.await;
                    Ok(TestResult { value: 1024 })
                }),
            )
            .await;

        let mut watcher = result.into_watcher();
        let result = tracker.wait(&mut watcher, Duration::from_millis(100)).await;
        assert!(result.is_timeout());

        // Triggers first future return.
        tx.send(()).unwrap();
        let result = tracker
            .wait(&mut watcher, Duration::from_millis(100))
            .await
            .into_finish()
            .unwrap()
            .unwrap();
        assert_eq!(TestResult { value: 1024 }, result);
        assert!(tracker.watcher(region_id).await.is_none());
    }
}
