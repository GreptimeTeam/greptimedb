//! Global runtimes
use std::future::Future;
use std::sync::{Arc, Mutex, Once};

use once_cell::sync::Lazy;
use paste::paste;

use crate::{Builder, JoinHandle, Runtime};

fn create_runtime(thread_name: &str) -> Runtime {
    Builder::default()
        .thread_name(thread_name)
        .build()
        .expect("Fail to create runtime")
}

struct GlobalRuntimes {
    read_runtime: Runtime,
    write_runtime: Runtime,
    bg_runtime: Runtime,
}

macro_rules! define_spawn {
    ($type: ident) => {
        paste! {

            fn [<spawn_ $type>]<F>(&self, future: F) -> JoinHandle<F::Output>
            where
                F: Future + Send + 'static,
                F::Output: Send + 'static,
            {
                self.[<$type _runtime>].spawn(future)
            }

            fn [<spawn_blocking_ $type>]<F, R>(&self, future: F) ->  JoinHandle<R>
            where
                F: FnOnce() -> R + Send + 'static,
                R: Send + 'static,
            {
                self.[<$type _runtime>].spawn_blocking(future)
            }

            fn [<block_on_ $type>]<F: Future>(&self, future: F) -> F::Output {
                self.[<$type _runtime>].block_on(future)
            }
        }
    };
}

impl GlobalRuntimes {
    define_spawn!(read);
    define_spawn!(write);
    define_spawn!(bg);
}

impl Default for GlobalRuntimes {
    fn default() -> Self {
        let mut c = CONFIG_RUNTIMES.as_ref().lock().unwrap();
        let read = std::mem::replace(&mut c.0, None);
        let write = std::mem::replace(&mut c.1, None);
        let background = std::mem::replace(&mut c.2, None);
        Self {
            read_runtime: read.unwrap_or_else(|| create_runtime("read-worker")),
            write_runtime: write.unwrap_or_else(|| create_runtime("write-worker")),
            bg_runtime: background.unwrap_or_else(|| create_runtime("bg-worker")),
        }
    }
}

static GLOBAL_RUNTIMES: Lazy<Arc<GlobalRuntimes>> =
    Lazy::new(|| Arc::new(GlobalRuntimes::default()));

static CONFIG_RUNTIMES: Lazy<Arc<Mutex<(Option<Runtime>, Option<Runtime>, Option<Runtime>)>>> =
    Lazy::new(|| Arc::new(Mutex::new((None, None, None))));

/// Initialize the global runtimes
///
/// You SHOULD call this function before using the global runtimes, otherwise the setting is not effective.
pub fn init_global_runtimes(
    read: Option<Runtime>,
    write: Option<Runtime>,
    background: Option<Runtime>,
) {
    static START: Once = Once::new();
    START.call_once(move || {
        let mut c = CONFIG_RUNTIMES.as_ref().lock().unwrap();
        *c = (read, write, background);
    });
}

macro_rules! define_global_runtime_spawn {
    ($type: ident) => {
        paste! {
            #[doc = "Returns the global `" $type "` thread pool."]
            pub fn [<$type _runtime>]() -> Runtime {
                GLOBAL_RUNTIMES.as_ref().[<$type _runtime>].clone()
            }

            #[doc = "Spawn a future and execute it in `" $type "` thread pool."]
            pub fn [<spawn_ $type>]<F>(future: F) -> JoinHandle<F::Output>
            where
                F: Future + Send + 'static,
                F::Output: Send + 'static,
            {
                GLOBAL_RUNTIMES.as_ref().[<spawn_ $type>](future)
            }

            #[doc = "Run the blocking operation in `" $type "` thread pool."]
            pub fn [<spawn_blocking_ $type>]<F, R>(future: F) ->  JoinHandle<R>
            where
                F: FnOnce() -> R + Send + 'static,
                R: Send + 'static,
            {
                GLOBAL_RUNTIMES.as_ref().[<spawn_blocking_ $type>](future)
            }

            #[doc = "Run a future to complete in `" $type "` thread pool."]
            pub fn [<block_on_ $type>]<F: Future>(future: F) -> F::Output {
                GLOBAL_RUNTIMES.as_ref().[<block_on_ $type>](future)
            }
        }
    };
}

define_global_runtime_spawn!(read);
define_global_runtime_spawn!(write);
define_global_runtime_spawn!(bg);

#[cfg(test)]
mod tests {
    use tokio_test::assert_ok;

    use super::*;

    #[test]
    fn test_spawn_block_on() {
        let handle = spawn_read(async { 1 + 1 });
        assert_eq!(2, block_on_read(handle).unwrap());

        let handle = spawn_write(async { 2 + 2 });
        assert_eq!(4, block_on_write(handle).unwrap());

        let handle = spawn_bg(async { 3 + 3 });
        assert_eq!(6, block_on_bg(handle).unwrap());
    }

    #[test]
    fn spawn_from_blocking() {
        let runtime = read_runtime();
        let out = runtime.block_on(async move {
            let inner = assert_ok!(
                spawn_blocking_read(move || { spawn_read(async move { "hello" }) }).await
            );

            assert_ok!(inner.await)
        });

        assert_eq!(out, "hello")
    }
}
