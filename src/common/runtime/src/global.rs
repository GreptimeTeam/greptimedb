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

//! Global runtimes
use std::future::Future;
use std::sync::{Mutex, Once};

use common_telemetry::info;
use once_cell::sync::Lazy;
use paste::paste;

use crate::{Builder, JoinHandle, Runtime};

const READ_WORKERS: usize = 8;
const WRITE_WORKERS: usize = 8;
const BG_WORKERS: usize = 8;

pub fn create_runtime(runtime_name: &str, thread_name: &str, worker_threads: usize) -> Runtime {
    info!("Creating runtime with runtime_name: {runtime_name}, thread_name: {thread_name}, work_threads: {worker_threads}.");
    Builder::default()
        .runtime_name(runtime_name)
        .thread_name(thread_name)
        .worker_threads(worker_threads)
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

    fn new(read: Option<Runtime>, write: Option<Runtime>, background: Option<Runtime>) -> Self {
        Self {
            read_runtime: read
                .unwrap_or_else(|| create_runtime("global-read", "read-worker", READ_WORKERS)),
            write_runtime: write
                .unwrap_or_else(|| create_runtime("global-write", "write-worker", WRITE_WORKERS)),
            bg_runtime: background
                .unwrap_or_else(|| create_runtime("global-bg", "bg-worker", BG_WORKERS)),
        }
    }
}

#[derive(Default)]
struct ConfigRuntimes {
    read_runtime: Option<Runtime>,
    write_runtime: Option<Runtime>,
    bg_runtime: Option<Runtime>,
    already_init: bool,
}

static GLOBAL_RUNTIMES: Lazy<GlobalRuntimes> = Lazy::new(|| {
    let mut c = CONFIG_RUNTIMES.lock().unwrap();
    let read = c.read_runtime.take();
    let write = c.write_runtime.take();
    let background = c.bg_runtime.take();
    c.already_init = true;

    GlobalRuntimes::new(read, write, background)
});

static CONFIG_RUNTIMES: Lazy<Mutex<ConfigRuntimes>> =
    Lazy::new(|| Mutex::new(ConfigRuntimes::default()));

/// Initialize the global runtimes
///
/// # Panics
/// Panics when the global runtimes are already initialized.
/// You should call this function before using any runtime functions.
pub fn init_global_runtimes(
    read: Option<Runtime>,
    write: Option<Runtime>,
    background: Option<Runtime>,
) {
    static START: Once = Once::new();
    START.call_once(move || {
        let mut c = CONFIG_RUNTIMES.lock().unwrap();
        assert!(!c.already_init, "Global runtimes already initialized");
        c.read_runtime = read;
        c.write_runtime = write;
        c.bg_runtime = background;
    });
}

macro_rules! define_global_runtime_spawn {
    ($type: ident) => {
        paste! {
            #[doc = "Returns the global `" $type "` thread pool."]
            pub fn [<$type _runtime>]() -> Runtime {
                GLOBAL_RUNTIMES.[<$type _runtime>].clone()
            }

            #[doc = "Spawn a future and execute it in `" $type "` thread pool."]
            pub fn [<spawn_ $type>]<F>(future: F) -> JoinHandle<F::Output>
            where
                F: Future + Send + 'static,
                F::Output: Send + 'static,
            {
                GLOBAL_RUNTIMES.[<spawn_ $type>](future)
            }

            #[doc = "Run the blocking operation in `" $type "` thread pool."]
            pub fn [<spawn_blocking_ $type>]<F, R>(future: F) ->  JoinHandle<R>
            where
                F: FnOnce() -> R + Send + 'static,
                R: Send + 'static,
            {
                GLOBAL_RUNTIMES.[<spawn_blocking_ $type>](future)
            }

            #[doc = "Run a future to complete in `" $type "` thread pool."]
            pub fn [<block_on_ $type>]<F: Future>(future: F) -> F::Output {
                GLOBAL_RUNTIMES.[<block_on_ $type>](future)
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

    macro_rules! define_spawn_blocking_test {
        ($type: ident) => {
            paste! {
                #[test]
                fn [<test_spawn_ $type _from_blocking>]() {
                    let runtime = [<$type _runtime>]();
                    let out = runtime.block_on(async move {
                        let inner = assert_ok!(
                            [<spawn_blocking_  $type>](move || {
                                [<spawn_ $type>](async move { "hello" })
                            }).await
                        );

                        assert_ok!(inner.await)
                    });

                    assert_eq!(out, "hello")
                }
            }
        };
    }

    define_spawn_blocking_test!(read);
    define_spawn_blocking_test!(write);
    define_spawn_blocking_test!(bg);
}
