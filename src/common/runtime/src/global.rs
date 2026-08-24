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
use std::num::NonZeroU32;
use std::sync::{Mutex, Once};

use catio::{Scheduler, SchedulerStats, TaskClass};
use common_telemetry::{info, warn};
use once_cell::sync::Lazy;
use paste::paste;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use crate::metrics::register_workload_scheduler_metrics;
use crate::runtime::{BuilderBuild, RuntimeTrait};
use crate::{Builder, JoinHandle, Runtime};

const GLOBAL_WORKERS: usize = 8;
const COMPACT_WORKERS: usize = 4;
const HB_WORKERS: usize = 2;
/// The minimum number of worker threads for runtimes sized by CPU count.
/// A single-threaded runtime can easily deadlock in async code.
const MIN_RUNTIME_THREADS: usize = 2;
pub(crate) const QUERY_TASK_CLASS: TaskClass = TaskClass::new(1);
pub(crate) const WRITE_TASK_CLASS: TaskClass = TaskClass::new(2);

/// Experimental options for sharing Tokio capacity between query and write
/// workloads.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct WorkloadSchedulerOptions {
    /// Enables policy-controlled query and write task spawning.
    pub enable: bool,
    /// Maximum polls admitted to Tokio at once. Zero uses four times
    /// `global_rt_size` to keep worker queues fed without making them
    /// effectively unbounded.
    pub max_concurrent_polls: usize,
    /// Relative share for query polls while writes are also backlogged.
    pub query_weight: u32,
    /// Relative share for write polls while queries are also backlogged.
    pub write_weight: u32,
}

impl Default for WorkloadSchedulerOptions {
    fn default() -> Self {
        Self {
            enable: false,
            max_concurrent_polls: 0,
            query_weight: 2,
            write_weight: 8,
        }
    }
}

/// The options for the global runtimes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct RuntimeOptions {
    /// The number of threads for the global default runtime.
    pub global_rt_size: usize,
    /// The number of threads to execute the runtime for compact operations.
    pub compact_rt_size: usize,
    /// The maximum number of blocking threads for compact operations.
    pub compact_rt_max_blocking_threads: usize,
    /// The number of threads to execute datanode query operations.
    pub query_rt_size: usize,
    /// The number of threads to execute datanode ingestion operations.
    pub ingest_rt_size: usize,
    /// Experimental weighted scheduler for query and write workloads.
    pub experimental_workload_scheduler: WorkloadSchedulerOptions,
}

impl RuntimeOptions {
    fn with_num_cpus(cpus: usize) -> Self {
        let cpus = usize::max(cpus, MIN_RUNTIME_THREADS);
        Self {
            global_rt_size: cpus,
            compact_rt_size: usize::max(cpus / 2, MIN_RUNTIME_THREADS),
            compact_rt_max_blocking_threads: usize::max(cpus / 2, MIN_RUNTIME_THREADS),
            query_rt_size: usize::max(cpus.saturating_sub(1), MIN_RUNTIME_THREADS),
            ingest_rt_size: cpus,
            experimental_workload_scheduler: WorkloadSchedulerOptions::default(),
        }
    }
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self::with_num_cpus(num_cpus::get())
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

fn create_compact_runtime(
    runtime_name: &str,
    thread_name: &str,
    worker_threads: usize,
    max_blocking_threads: usize,
) -> Runtime {
    let max_blocking_threads = max_blocking_threads.max(1);
    info!(
        "Creating compact runtime with runtime_name: {runtime_name}, thread_name: {thread_name}, work_threads: {worker_threads}, max_blocking_threads: {max_blocking_threads}."
    );
    Builder::default()
        .runtime_name(runtime_name)
        .thread_name(thread_name)
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .build()
        .expect("Fail to create runtime")
}

struct GlobalRuntimes {
    global_runtime: Runtime,
    compact_runtime: Runtime,
    hb_runtime: Runtime,
    query_runtime: Runtime,
    ingest_runtime: Runtime,
    query_handle: Handle,
    ingest_handle: Handle,
    workload_scheduler: Option<Scheduler>,
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

macro_rules! define_scheduled_spawn {
    ($type: ident, $class: ident) => {
        paste! {
            fn [<spawn_ $type>]<F>(&self, future: F) -> JoinHandle<F::Output>
            where
                F: Future + Send + 'static,
                F::Output: Send + 'static,
            {
                match &self.workload_scheduler {
                    Some(scheduler) => scheduler.spawn_in_on(
                        &self.[<$type _handle>],
                        $class,
                        future,
                    ),
                    None => self.[<$type _runtime>].spawn(future),
                }
            }

            fn [<spawn_blocking_ $type>]<F, R>(&self, future: F) -> JoinHandle<R>
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
    define_scheduled_spawn!(query, QUERY_TASK_CLASS);
    define_scheduled_spawn!(ingest, WRITE_TASK_CLASS);

    fn new(
        global: Option<Runtime>,
        compact: Option<Runtime>,
        heartbeat: Option<Runtime>,
        query: Option<Runtime>,
        ingest: Option<Runtime>,
        workload_scheduler: Option<Scheduler>,
    ) -> Self {
        let global_runtime =
            global.unwrap_or_else(|| create_runtime("global", "global-worker", GLOBAL_WORKERS));
        let query_runtime = query.unwrap_or_else(|| global_runtime.clone());
        let ingest_runtime = ingest.unwrap_or_else(|| global_runtime.clone());
        let query_handle = query_runtime.handle();
        let ingest_handle = ingest_runtime.handle();
        Self {
            global_runtime,
            compact_runtime: compact.unwrap_or_else(|| {
                let max_blocking_threads =
                    RuntimeOptions::default().compact_rt_max_blocking_threads;
                create_compact_runtime(
                    "compact",
                    "compact-worker",
                    COMPACT_WORKERS,
                    max_blocking_threads,
                )
            }),
            hb_runtime: heartbeat
                .unwrap_or_else(|| create_runtime("heartbeat", "hb-worker", HB_WORKERS)),
            query_runtime,
            ingest_runtime,
            query_handle,
            ingest_handle,
            workload_scheduler,
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
    workload_scheduler: Option<Scheduler>,
    already_init: bool,
}

static GLOBAL_RUNTIMES: Lazy<GlobalRuntimes> = Lazy::new(|| {
    let mut c = CONFIG_RUNTIMES.lock().unwrap();
    let global = c.global_runtime.take();
    let compact = c.compact_runtime.take();
    let heartbeat = c.hb_runtime.take();
    let query = c.query_runtime.take();
    let ingest = c.ingest_runtime.take();
    let workload_scheduler = c.workload_scheduler.take();
    c.already_init = true;

    GlobalRuntimes::new(
        global,
        compact,
        heartbeat,
        query,
        ingest,
        workload_scheduler,
    )
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
        c.compact_runtime = Some(create_compact_runtime(
            "compact",
            "compact-worker",
            options.compact_rt_size,
            options.compact_rt_max_blocking_threads,
        ));
        c.hb_runtime = Some(create_runtime("heartbeat", "hb-worker", HB_WORKERS));
        c.workload_scheduler = create_workload_scheduler(options);
    });
}

fn create_workload_scheduler(options: &RuntimeOptions) -> Option<Scheduler> {
    let scheduler_options = &options.experimental_workload_scheduler;
    if !scheduler_options.enable {
        return None;
    }
    if scheduler_options.query_weight == 0 || scheduler_options.write_weight == 0 {
        warn!(
            "The experimental workload scheduler is disabled because query_weight and \
             write_weight must both be greater than zero"
        );
        return None;
    }

    let max_concurrent_polls = if scheduler_options.max_concurrent_polls == 0 {
        options.global_rt_size.saturating_mul(4)
    } else {
        scheduler_options.max_concurrent_polls
    };
    if max_concurrent_polls == 0 {
        warn!(
            "The experimental workload scheduler is disabled because max_concurrent_polls \
             resolved to zero"
        );
        return None;
    }

    let scheduler = Scheduler::builder()
        .max_concurrent_polls(max_concurrent_polls)
        .weight(QUERY_TASK_CLASS, scheduler_options.query_weight)
        .weight(WRITE_TASK_CLASS, scheduler_options.write_weight)
        .build();
    register_workload_scheduler_metrics(scheduler.clone());
    info!(
        "Enabled the experimental workload scheduler: max_concurrent_polls={}, \
         query_weight={}, write_weight={}",
        max_concurrent_polls, scheduler_options.query_weight, scheduler_options.write_weight
    );
    Some(scheduler)
}

/// Initialize the datanode-specific global runtimes.
///
/// # Panics
/// Panics when the global runtimes are already initialized.
/// You should call this function before using any runtime functions.
pub fn init_datanode_runtimes(options: &RuntimeOptions) {
    static START: Once = Once::new();
    START.call_once(move || {
        let mut c = CONFIG_RUNTIMES.lock().unwrap();
        assert!(!c.already_init, "Global runtimes already initialized");
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

/// Returns whether the experimental workload scheduler is currently enabled.
/// Returns `false` when no scheduler was constructed at startup.
pub fn workload_scheduler_enabled() -> bool {
    GLOBAL_RUNTIMES
        .workload_scheduler
        .as_ref()
        .is_some_and(Scheduler::is_enabled)
}

/// Enables or disables the experimental workload scheduler for new spawn
/// submissions. Returns `false` when no scheduler was constructed at startup.
pub fn set_workload_scheduler_enabled(enabled: bool) -> bool {
    let Some(scheduler) = GLOBAL_RUNTIMES.workload_scheduler.as_ref() else {
        warn!(
            "The experimental workload scheduler was not constructed at startup; ignoring enabled={enabled}"
        );
        return false;
    };

    scheduler.set_enabled(enabled);
    info!("Experimental workload scheduler enabled={enabled}");
    true
}

/// Returns scheduler counters when the experimental workload scheduler was
/// constructed at startup, including while it is dynamically disabled.
pub fn workload_scheduler_stats() -> Option<SchedulerStats> {
    GLOBAL_RUNTIMES
        .workload_scheduler
        .as_ref()
        .map(Scheduler::stats)
}

/// Dynamically adjusts the query and write weights of the experimental workload
/// scheduler at runtime. Returns `false` and leaves the scheduler unchanged when
/// it was not constructed at startup or either weight is zero.
pub fn set_workload_scheduler_weights(query_weight: u32, write_weight: u32) -> bool {
    let Some(scheduler) = GLOBAL_RUNTIMES.workload_scheduler.as_ref() else {
        warn!(
            "The experimental workload scheduler was not constructed at startup; ignoring query_weight={}, \
             write_weight={}",
            query_weight, write_weight
        );
        return false;
    };

    let Some(query_weight) = NonZeroU32::new(query_weight) else {
        warn!("Refusing to set workload scheduler weights: query_weight must be greater than zero");
        return false;
    };
    let Some(write_weight) = NonZeroU32::new(write_weight) else {
        warn!("Refusing to set workload scheduler weights: write_weight must be greater than zero");
        return false;
    };

    scheduler.set_weight(QUERY_TASK_CLASS, query_weight);
    scheduler.set_weight(WRITE_TASK_CLASS, write_weight);
    info!(
        "Updated experimental workload scheduler weights: query_weight={query_weight}, \
         write_weight={write_weight}"
    );
    true
}

/// Dynamically adjusts the maximum number of concurrent polls admitted to
/// Tokio by the experimental workload scheduler at runtime. Returns `false`
/// and leaves the scheduler unchanged when it was not constructed at startup
/// or the limit is zero.
pub fn set_workload_scheduler_max_concurrent_polls(limit: usize) -> bool {
    let Some(scheduler) = GLOBAL_RUNTIMES.workload_scheduler.as_ref() else {
        warn!(
            "The experimental workload scheduler was not constructed at startup; ignoring \
             max_concurrent_polls={limit}"
        );
        return false;
    };

    if limit == 0 {
        warn!("Refusing to set workload scheduler max_concurrent_polls to zero");
        return false;
    }

    scheduler.set_max_concurrent_polls(limit);
    info!("Updated experimental workload scheduler max_concurrent_polls to {limit}");
    true
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use tokio_test::assert_ok;

    use super::*;

    #[test]
    fn test_datanode_runtime_options_default() {
        let options = RuntimeOptions::default();
        let cpus = usize::max(num_cpus::get(), MIN_RUNTIME_THREADS);

        assert_eq!(cpus, options.global_rt_size);
        assert_eq!(
            usize::max(cpus / 2, MIN_RUNTIME_THREADS),
            options.compact_rt_size
        );
        assert_eq!(
            usize::max(cpus / 2, MIN_RUNTIME_THREADS),
            options.compact_rt_max_blocking_threads
        );
        assert_eq!(
            usize::max(cpus.saturating_sub(1), MIN_RUNTIME_THREADS),
            options.query_rt_size
        );
        assert_eq!(cpus, options.ingest_rt_size);
        assert_eq!(
            WorkloadSchedulerOptions::default(),
            options.experimental_workload_scheduler
        );
    }

    #[test]
    fn test_runtime_options_min_threads() {
        for cpus in [0, 1, 2] {
            let options = RuntimeOptions::with_num_cpus(cpus);
            assert!(
                options.global_rt_size >= MIN_RUNTIME_THREADS,
                "global_rt_size {} < {MIN_RUNTIME_THREADS} with {cpus} cpus",
                options.global_rt_size
            );
            assert!(
                options.compact_rt_size >= MIN_RUNTIME_THREADS,
                "compact_rt_size {} < {MIN_RUNTIME_THREADS} with {cpus} cpus",
                options.compact_rt_size
            );
            assert!(
                options.compact_rt_max_blocking_threads >= MIN_RUNTIME_THREADS,
                "compact_rt_max_blocking_threads {} < {MIN_RUNTIME_THREADS} with {cpus} cpus",
                options.compact_rt_max_blocking_threads
            );
            assert!(
                options.query_rt_size >= MIN_RUNTIME_THREADS,
                "query_rt_size {} < {MIN_RUNTIME_THREADS} with {cpus} cpus",
                options.query_rt_size
            );
            assert!(
                options.ingest_rt_size >= MIN_RUNTIME_THREADS,
                "ingest_rt_size {} < {MIN_RUNTIME_THREADS} with {cpus} cpus",
                options.ingest_rt_size
            );
        }
    }

    #[test]
    fn test_datanode_runtimes_fallback_to_global_runtime() {
        let runtimes = GlobalRuntimes::new(
            Some(create_runtime("test-global", "test-global-worker", 1)),
            None,
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
    fn test_create_compact_runtime_with_zero_max_blocking_threads() {
        let runtime = create_compact_runtime("test-compact", "test-compact-worker", 1, 0);
        let handle = runtime.spawn_blocking(|| 1 + 1);

        assert_eq!(2, runtime.block_on(handle).unwrap());
    }

    #[test]
    fn test_compact_runtime_limits_blocking_threads() {
        let runtime = create_compact_runtime("test-compact", "test-compact-worker", 1, 1);
        let (first_started_tx, first_started_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let first = runtime.spawn_blocking(move || {
            first_started_tx.send(()).unwrap();
            release_first_rx.recv().unwrap();
        });
        first_started_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();

        let (second_started_tx, second_started_rx) = mpsc::channel();
        let second = runtime.spawn_blocking(move || second_started_tx.send(()).unwrap());
        assert!(
            second_started_rx
                .recv_timeout(Duration::from_secs(1))
                .is_err()
        );

        release_first_tx.send(()).unwrap();
        second_started_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        runtime.block_on(async {
            first.await.unwrap();
            second.await.unwrap();
        });
    }

    #[test]
    fn test_workload_scheduler_default_admission_window() {
        let mut options = RuntimeOptions {
            global_rt_size: 3,
            ..RuntimeOptions::default()
        };
        options.experimental_workload_scheduler.enable = true;

        let scheduler = create_workload_scheduler(&options).unwrap();
        let stats = scheduler.stats();
        assert_eq!(12, stats.max_concurrent_polls);
        assert_eq!(2, stats.classes[&QUERY_TASK_CLASS].weight);
        assert_eq!(8, stats.classes[&WRITE_TASK_CLASS].weight);
    }

    #[test]
    fn test_workload_scheduler_bypasses_disabled_query_and_write_spawns() {
        let runtime = create_runtime("test-workload-bypass", "test-workload-bypass-worker", 2);
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(2)
            .weight(QUERY_TASK_CLASS, 2)
            .weight(WRITE_TASK_CLASS, 8)
            .build();
        scheduler.set_enabled(false);
        let runtimes = GlobalRuntimes::new(
            Some(runtime.clone()),
            Some(runtime.clone()),
            Some(runtime.clone()),
            Some(runtime.clone()),
            Some(runtime.clone()),
            Some(scheduler.clone()),
        );

        let query = runtimes.spawn_query(async { "query" });
        let write = runtimes.spawn_ingest(async { "write" });
        let (query, write) =
            runtime.block_on(async { (query.await.unwrap(), write.await.unwrap()) });

        assert_eq!("query", query);
        assert_eq!("write", write);
        let stats = scheduler.stats();
        for class in [QUERY_TASK_CLASS, WRITE_TASK_CLASS] {
            let class_stats = &stats.classes[&class];
            assert_eq!(0, class_stats.tasks);
            assert_eq!(0, class_stats.admitted);
            assert_eq!(0, class_stats.polls);
        }
    }

    #[test]
    fn test_workload_scheduler_wraps_query_and_write_spawns() {
        let runtime = create_runtime("test-workload", "test-workload-worker", 2);
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(2)
            .weight(QUERY_TASK_CLASS, 2)
            .weight(WRITE_TASK_CLASS, 8)
            .build();
        let runtimes = GlobalRuntimes::new(
            Some(runtime.clone()),
            Some(runtime.clone()),
            Some(runtime.clone()),
            Some(runtime.clone()),
            Some(runtime.clone()),
            Some(scheduler.clone()),
        );

        let query = runtimes.spawn_query(async { "query" });
        let write = runtimes.spawn_ingest(async { "write" });
        let (query, write) =
            runtime.block_on(async { (query.await.unwrap(), write.await.unwrap()) });

        assert_eq!("query", query);
        assert_eq!("write", write);
        let stats = scheduler.stats();
        assert_eq!(1, stats.classes[&QUERY_TASK_CLASS].polls);
        assert_eq!(1, stats.classes[&WRITE_TASK_CLASS].polls);
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
