use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use metrics::{decrement_gauge, increment_gauge};
use snafu::ResultExt;
use tokio::runtime::{Builder as RuntimeBuilder, Handle};
use tokio::sync::oneshot;
pub use tokio::task::{JoinError, JoinHandle};

use crate::error::*;
use crate::metric::*;

/// A runtime to run future tasks
#[derive(Clone, Debug)]
pub struct Runtime {
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
        self.close.take().map(|v| v.send(()));
    }
}

impl Runtime {
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
}

pub struct Builder {
    thread_name: String,
    builder: RuntimeBuilder,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
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
        self.builder.worker_threads(val);
        self
    }

    /// Specifies the limit for additional threads spawned by the Runtime.
    ///
    /// These threads are used for blocking operations like tasks spawned through spawn_blocking,
    /// they are not always active and will exit if left idle for too long, You can change this timeout duration
    /// with thread_keep_alive. The default value is 512.
    pub fn max_blocking_threads(&mut self, val: usize) -> &mut Self {
        self.builder.max_blocking_threads(val);
        self
    }

    /// Sets a custom timeout for a thread in the blocking pool.
    ///
    /// By default, the timeout for a thread is set to 10 seconds.
    pub fn thread_keep_alive(&mut self, duration: Duration) -> &mut Self {
        self.builder.thread_keep_alive(duration);
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

        let handle = runtime.handle().clone();
        let (send_stop, recv_stop) = oneshot::channel();
        // Block the runtime to shutdown.
        let _ = thread::Builder::new()
            .name(format!("{}-blocker", self.thread_name))
            .spawn(move || runtime.block_on(recv_stop));

        Ok(Runtime {
            handle,
            _dropper: Arc::new(Dropper {
                close: Some(send_stop),
            }),
        })
    }
}

fn on_thread_start(thread_name: String) -> impl Fn() + 'static {
    move || {
        let labels = [(THREAD_NAME_LABEL, thread_name.clone())];
        increment_gauge!(METRIC_RUNTIME_THREADS_ALIVE, 1.0, &labels);
    }
}

fn on_thread_stop(thread_name: String) -> impl Fn() + 'static {
    move || {
        let labels = [(THREAD_NAME_LABEL, thread_name.clone())];
        decrement_gauge!(METRIC_RUNTIME_THREADS_ALIVE, 1.0, &labels);
    }
}

fn on_thread_park(thread_name: String) -> impl Fn() + 'static {
    move || {
        let labels = [(THREAD_NAME_LABEL, thread_name.clone())];
        increment_gauge!(METRIC_RUNTIME_THREADS_IDLE, 1.0, &labels);
    }
}

fn on_thread_unpark(thread_name: String) -> impl Fn() + 'static {
    move || {
        let labels = [(THREAD_NAME_LABEL, thread_name.clone())];
        decrement_gauge!(METRIC_RUNTIME_THREADS_IDLE, 1.0, &labels);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use common_telemetry::metric;
    use tokio::sync::oneshot;
    use tokio_test::assert_ok;

    use super::*;

    fn runtime() -> Arc<Runtime> {
        common_telemetry::init_default_metrics_recorder();
        let runtime = Builder::default()
            .worker_threads(2)
            .thread_name("test_spawn_join")
            .build();
        assert!(runtime.is_ok());
        Arc::new(runtime.unwrap())
    }

    #[test]
    fn test_metric() {
        common_telemetry::init_default_metrics_recorder();
        let runtime = Builder::default()
            .worker_threads(5)
            .thread_name("test_runtime_metric")
            .build()
            .unwrap();
        // wait threads created
        thread::sleep(Duration::from_millis(50));

        runtime.spawn(async {
            thread::sleep(Duration::from_millis(50));
        });

        thread::sleep(Duration::from_millis(10));

        let handle = metric::try_handle().unwrap();
        let metric_text = handle.render();

        assert!(metric_text.contains("runtime_threads_idle{thread_name=\"test_runtime_metric\"}"));
        assert!(metric_text.contains("runtime_threads_alive{thread_name=\"test_runtime_metric\"}"));
    }

    #[test]
    fn block_on_async() {
        let runtime = runtime();

        let out = runtime.block_on(async {
            let (tx, rx) = oneshot::channel();

            thread::spawn(move || {
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
