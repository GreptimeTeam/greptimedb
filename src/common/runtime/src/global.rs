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
use serde::{Deserialize, Serialize};

use crate::runtime::{BuilderBuild, RuntimeTrait};
use crate::{Builder, JoinHandle, Runtime};

const GLOBAL_WORKERS: usize = 8;
const COMPACT_WORKERS: usize = 4;
const HB_WORKERS: usize = 2;

/// The options for the global runtimes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeOptions {
    /// The number of threads for the global default runtime.
    pub global_rt_size: usize,
    /// The number of threads to execute the runtime for compact operations.
    pub compact_rt_size: usize,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        let cpus = num_cpus::get();
        Self {
            global_rt_size: cpus,
            compact_rt_size: usize::max(cpus / 2, 1),
        }
    }
}

/// The options for datanode-specific global runtimes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DatanodeRuntimeOptions {
    #[serde(flatten)]
    pub base: RuntimeOptions,
    /// The number of threads to execute datanode query operations.
    pub query_rt_size: usize,
    /// The number of threads to execute datanode ingestion operations.
    pub ingest_rt_size: usize,
}

impl Default for DatanodeRuntimeOptions {
    fn default() -> Self {
        let cpus = num_cpus::get();
        Self {
            base: RuntimeOptions::default(),
            query_rt_size: usize::max(cpus.saturating_sub(1), 1),
            ingest_rt_size: cpus,
        }
    }
}

pub fn create_runtime(runtime_name: &str, thread_name: &str, worker_threads: usize) -> Runtime {
    info!(
        "Creating runtime with runtime_name: {runtime_name}, thread_name: {thread_name}, work_threads: {worker_threads}."
    );
    Builder::default()
        .runtime_name(runtime_name)
        .thread_name(thread_name)
        .worker_threads(worker_threads)
        .build()
        .expect("Fail to create runtime")
}

struct GlobalRuntimes {
    global_runtime: Runtime,
    compact_runtime: Runtime,
    hb_runtime: Runtime,
    query_runtime: Runtime,
    ingest_runtime: Runtime,
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
    define_spawn!(global);
    define_spawn!(compact);
    define_spawn!(hb);
    define_spawn!(query);
    define_spawn!(ingest);

    fn new(
        global: Option<Runtime>,
        compact: Option<Runtime>,
        heartbeat: Option<Runtime>,
        query: Option<Runtime>,
        ingest: Option<Runtime>,
    ) -> Self {
        let global_runtime =
            global.unwrap_or_else(|| create_runtime("global", "global-worker", GLOBAL_WORKERS));
        let query_runtime = query.unwrap_or_else(|| global_runtime.clone());
        let ingest_runtime = ingest.unwrap_or_else(|| global_runtime.clone());

        Self {
            global_runtime,
            compact_runtime: compact
                .unwrap_or_else(|| create_runtime("compact", "compact-worker", COMPACT_WORKERS)),
            hb_runtime: heartbeat
                .unwrap_or_else(|| create_runtime("heartbeat", "hb-worker", HB_WORKERS)),
            query_runtime,
            ingest_runtime,
        }
    }
}

#[derive(Default)]
struct ConfigRuntimes {
    global_runtime: Option<Runtime>,
    compact_runtime: Option<Runtime>,
    hb_runtime: Option<Runtime>,
    query_runtime: Option<Runtime>,
    ingest_runtime: Option<Runtime>,
    already_init: bool,
}

static GLOBAL_RUNTIMES: Lazy<GlobalRuntimes> = Lazy::new(|| {
    let mut c = CONFIG_RUNTIMES.lock().unwrap();
    let global = c.global_runtime.take();
    let compact = c.compact_runtime.take();
    let heartbeat = c.hb_runtime.take();
    let query = c.query_runtime.take();
    let ingest = c.ingest_runtime.take();
    c.already_init = true;

    GlobalRuntimes::new(global, compact, heartbeat, query, ingest)
});

static CONFIG_RUNTIMES: Lazy<Mutex<ConfigRuntimes>> =
    Lazy::new(|| Mutex::new(ConfigRuntimes::default()));

/// Initialize the global runtimes
///
/// # Panics
/// Panics when the global runtimes are already initialized.
/// You should call this function before using any runtime functions.
pub fn init_global_runtimes(options: &RuntimeOptions) {
    static START: Once = Once::new();
    START.call_once(move || {
        let mut c = CONFIG_RUNTIMES.lock().unwrap();
        assert!(!c.already_init, "Global runtimes already initialized");
        c.global_runtime = Some(create_runtime(
            "global",
            "global-worker",
            options.global_rt_size,
        ));
        c.compact_runtime = Some(create_runtime(
            "compact",
            "compact-worker",
            options.compact_rt_size,
        ));
        c.hb_runtime = Some(create_runtime("heartbeat", "hb-worker", HB_WORKERS));
    });
}

/// Initialize the datanode-specific global runtimes.
///
/// # Panics
/// Panics when the global runtimes are already initialized.
/// You should call this function before using any runtime functions.
pub fn init_datanode_runtimes(options: &DatanodeRuntimeOptions) {
    static START: Once = Once::new();
    START.call_once(move || {
        let mut c = CONFIG_RUNTIMES.lock().unwrap();
        c.query_runtime = Some(create_runtime(
            "query",
            "query-worker",
            options.query_rt_size,
        ));
        c.ingest_runtime = Some(create_runtime(
            "ingest",
            "ingest-worker",
            options.ingest_rt_size,
        ));
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

define_global_runtime_spawn!(global);
define_global_runtime_spawn!(compact);
define_global_runtime_spawn!(hb);
define_global_runtime_spawn!(query);
define_global_runtime_spawn!(ingest);

#[cfg(test)]
mod tests {
    use tokio_test::assert_ok;

    use super::*;

    #[test]
    fn test_datanode_runtime_options_default() {
        let options = DatanodeRuntimeOptions::default();
        let cpus = num_cpus::get();

        assert_eq!(cpus, options.base.global_rt_size);
        assert_eq!(usize::max(cpus / 2, 1), options.base.compact_rt_size);
        assert_eq!(usize::max(cpus.saturating_sub(1), 1), options.query_rt_size);
        assert_eq!(cpus, options.ingest_rt_size);
    }

    #[test]
    fn test_datanode_runtimes_fallback_to_global_runtime() {
        let runtimes = GlobalRuntimes::new(
            Some(create_runtime("test-global", "test-global-worker", 1)),
            None,
            None,
            None,
            None,
        );

        assert_eq!("test-global", runtimes.global_runtime.name());
        assert_eq!("test-global", runtimes.query_runtime.name());
        assert_eq!("test-global", runtimes.ingest_runtime.name());
    }

    #[test]
    fn test_datanode_runtime_spawn_block_on() {
        let handle = spawn_query(async { 1 + 1 });
        assert_eq!(2, block_on_query(handle).unwrap());

        let handle = spawn_ingest(async { 2 + 2 });
        assert_eq!(4, block_on_ingest(handle).unwrap());
    }

    #[test]
    fn test_spawn_block_on() {
        let handle = spawn_global(async { 1 + 1 });
        assert_eq!(2, block_on_global(handle).unwrap());

        let handle = spawn_compact(async { 2 + 2 });
        assert_eq!(4, block_on_compact(handle).unwrap());

        let handle = spawn_hb(async { 4 + 4 });
        assert_eq!(8, block_on_hb(handle).unwrap());
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

    define_spawn_blocking_test!(global);
    define_spawn_blocking_test!(compact);
    define_spawn_blocking_test!(hb);
}
