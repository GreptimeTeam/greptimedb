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

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use snafu::ResultExt;
use tokio::runtime::{Builder as RuntimeBuilder, Handle};
use tokio::sync::oneshot;
pub use tokio::task::{JoinError, JoinHandle};

use crate::error::*;
use crate::metrics::*;

static RUNTIME_ID: AtomicUsize = AtomicUsize::new(0);

/// A runtime to run future tasks
#[derive(Clone, Debug)]
pub struct Runtime {
    name: String,
    handle: Handle,
    // Used to receive a drop signal when dropper is dropped, inspired by databend
    _dropper: Arc<Dropper>,
}

/// Dropping the dropper will cause runtime to shutdown.
#[derive(Debug)]
pub struct Dropper {
    close: Option<oneshot::Sender<()>>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        // Send a signal to say i am dropping.
        let _ = self.close.take().map(|v| v.send(()));
    }
}

impl Runtime {
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Spawn a future and execute it in this thread pool
    ///
    /// Similar to tokio::runtime::Runtime::spawn()
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Run the provided function on an executor dedicated to blocking
    /// operations.
    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.handle.spawn_blocking(func)
    }

    /// Run a future to complete, this is the runtime's entry point
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub struct Builder {
    runtime_name: String,
    thread_name: String,
    builder: RuntimeBuilder,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            runtime_name: format!("runtime-{}", RUNTIME_ID.fetch_add(1, Ordering::Relaxed)),
            thread_name: "default-worker".to_string(),
            builder: RuntimeBuilder::new_multi_thread(),
        }
    }
}

impl Builder {
    /// Sets the number of worker threads the Runtime will use.
    ///
    /// This can be any number above 0. The default value is the number of cores available to the system.
    pub fn worker_threads(&mut self, val: usize) -> &mut Self {
        let _ = self.builder.worker_threads(val);
        self
    }

    /// Specifies the limit for additional threads spawned by the Runtime.
    ///
    /// These threads are used for blocking operations like tasks spawned through spawn_blocking,
    /// they are not always active and will exit if left idle for too long, You can change this timeout duration
    /// with thread_keep_alive. The default value is 512.
    pub fn max_blocking_threads(&mut self, val: usize) -> &mut Self {
        let _ = self.builder.max_blocking_threads(val);
        self
    }

    /// Sets a custom timeout for a thread in the blocking pool.
    ///
    /// By default, the timeout for a thread is set to 10 seconds.
    pub fn thread_keep_alive(&mut self, duration: Duration) -> &mut Self {
        let _ = self.builder.thread_keep_alive(duration);
        self
    }

    pub fn runtime_name(&mut self, val: impl Into<String>) -> &mut Self {
        self.runtime_name = val.into();
        self
    }

    /// Sets name of threads spawned by the Runtime thread pool
    pub fn thread_name(&mut self, val: impl Into<String>) -> &mut Self {
        self.thread_name = val.into();
        self
    }

    pub fn build(&mut self) -> Result<Runtime> {
        let runtime = self
            .builder
            .enable_all()
            .thread_name(self.thread_name.clone())
            .on_thread_start(on_thread_start(self.thread_name.clone()))
            .on_thread_stop(on_thread_stop(self.thread_name.clone()))
            .on_thread_park(on_thread_park(self.thread_name.clone()))
            .on_thread_unpark(on_thread_unpark(self.thread_name.clone()))
            .build()
            .context(BuildRuntimeSnafu)?;

        let name = self.runtime_name.clone();
        let handle = runtime.handle().clone();
        let (send_stop, recv_stop) = oneshot::channel();
        // Block the runtime to shutdown.
        let _ = thread::Builder::new()
            .name(format!("{}-blocker", self.thread_name))
            .spawn(move || runtime.block_on(recv_stop));

        #[cfg(tokio_unstable)]
        register_collector(name.clone(), &handle);

        Ok(Runtime {
            name,
            handle,
            _dropper: Arc::new(Dropper {
                close: Some(send_stop),
            }),
        })
    }
}

#[cfg(tokio_unstable)]
pub fn register_collector(name: String, handle: &Handle) {
    let name = name.replace("-", "_");
    let monitor = tokio_metrics::RuntimeMonitor::new(handle);
    let collector = tokio_metrics_collector::RuntimeCollector::new(monitor, name);
    let _ = prometheus::register(Box::new(collector));
}

fn on_thread_start(thread_name: String) -> impl Fn() + 'static {
    move || {
        METRIC_RUNTIME_THREADS_ALIVE
            .with_label_values(&[thread_name.as_str()])
            .inc();
    }
}

fn on_thread_stop(thread_name: String) -> impl Fn() + 'static {
    move || {
        METRIC_RUNTIME_THREADS_ALIVE
            .with_label_values(&[thread_name.as_str()])
            .dec();
    }
}

fn on_thread_park(thread_name: String) -> impl Fn() + 'static {
    move || {
        METRIC_RUNTIME_THREADS_IDLE
            .with_label_values(&[thread_name.as_str()])
            .inc();
    }
}

fn on_thread_unpark(thread_name: String) -> impl Fn() + 'static {
    move || {
        METRIC_RUNTIME_THREADS_IDLE
            .with_label_values(&[thread_name.as_str()])
            .dec();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use common_telemetry::dump_metrics;
    use tokio::sync::oneshot;
    use tokio_test::assert_ok;

    use super::*;

    fn runtime() -> Arc<Runtime> {
        let runtime = Builder::default()
            .worker_threads(2)
            .thread_name("test_spawn_join")
            .build();
        Arc::new(runtime.unwrap())
    }

    #[test]
    fn test_metric() {
        let runtime = Builder::default()
            .worker_threads(5)
            .thread_name("test_runtime_metric")
            .build()
            .unwrap();
        // wait threads created
        thread::sleep(Duration::from_millis(50));

        let _handle = runtime.spawn(async {
            thread::sleep(Duration::from_millis(50));
        });

        thread::sleep(Duration::from_millis(10));

        let metric_text = dump_metrics().unwrap();

        assert!(metric_text.contains("runtime_threads_idle{thread_name=\"test_runtime_metric\"}"));
        assert!(metric_text.contains("runtime_threads_alive{thread_name=\"test_runtime_metric\"}"));

        #[cfg(tokio_unstable)]
        {
            assert!(metric_text.contains("runtime_0_tokio_budget_forced_yield_count 0"));
            assert!(metric_text.contains("runtime_0_tokio_injection_queue_depth 0"));
            assert!(metric_text.contains("runtime_0_tokio_workers_count 5"));
        }
    }

    #[test]
    fn block_on_async() {
        let runtime = runtime();

        let out = runtime.block_on(async {
            let (tx, rx) = oneshot::channel();

            let _ = thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                tx.send("ZOMG").unwrap();
            });

            assert_ok!(rx.await)
        });

        assert_eq!(out, "ZOMG");
    }

    #[test]
    fn spawn_from_blocking() {
        let runtime = runtime();
        let runtime1 = runtime.clone();
        let out = runtime.block_on(async move {
            let runtime2 = runtime1.clone();
            let inner = assert_ok!(
                runtime1
                    .spawn_blocking(move || { runtime2.spawn(async move { "hello" }) })
                    .await
            );

            assert_ok!(inner.await)
        });

        assert_eq!(out, "hello")
    }

    #[test]
    fn test_spawn_join() {
        let runtime = runtime();
        let handle = runtime.spawn(async { 1 + 1 });

        assert_eq!(2, runtime.block_on(handle).unwrap());
    }
}
