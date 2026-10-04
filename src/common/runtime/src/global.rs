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
use std::collections::BTreeMap;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
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
    /// Relative share for query polls while writes are also backlogged.
    pub query_weight: NonZeroU32,
    /// Relative share for write polls while queries are also backlogged.
    pub write_weight: NonZeroU32,
    /// Number of polls between scheduler fairness samples.
    pub sample_every_polls: NonZeroUsize,
}

impl Default for WorkloadSchedulerOptions {
    fn default() -> Self {
        Self {
            enable: false,
            query_weight: NonZeroU32::new(2).unwrap(),
            write_weight: NonZeroU32::new(8).unwrap(),
            sample_every_polls: NonZeroUsize::new(16).unwrap(),
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
static START: Once = Once::new();

/// Initialize runtimes for frontend, metasrv, and flownode processes.
///
/// Query and ingest work share the global runtime and no workload scheduler is
/// constructed for these process roles.
pub fn init_global_runtimes(options: &RuntimeOptions) {
    START.call_once(|| {
        let mut c = CONFIG_RUNTIMES.lock().unwrap();
        assert!(!c.already_init, "Global runtimes already initialized");
        init_common_runtimes(&mut c, options);
        c.already_init = true;
    });
}

/// Initialize runtimes for a standalone process.
///
/// Query and ingest work share the global runtime. The scheduler is always
/// constructed, with the global runtime size as its internal bound, and starts
/// enabled according to configuration.
pub fn init_standalone_runtimes(options: &RuntimeOptions) {
    START.call_once(|| {
        let mut c = CONFIG_RUNTIMES.lock().unwrap();
        assert!(!c.already_init, "Global runtimes already initialized");
        init_common_runtimes(&mut c, options);
        c.workload_scheduler = Some(create_workload_scheduler(options, options.global_rt_size));
        c.already_init = true;
    });
}

/// Initialize runtimes for a datanode process.
///
/// Query and ingest use dedicated runtimes. The scheduler is always
/// constructed with their checked combined size as its internal bound, and
/// starts enabled according to configuration.
///
/// # Panics
///
/// Panics if the configured query and ingest runtime sizes overflow `usize`
/// when combined.
pub fn init_datanode_runtimes(options: &RuntimeOptions) {
    let capacity = options
        .query_rt_size
        .checked_add(options.ingest_rt_size)
        .expect("datanode workload scheduler runtime capacity overflowed usize");
    START.call_once(|| {
        let mut c = CONFIG_RUNTIMES.lock().unwrap();
        assert!(!c.already_init, "Global runtimes already initialized");
        init_common_runtimes(&mut c, options);
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
        c.workload_scheduler = Some(create_workload_scheduler(options, capacity));
        c.already_init = true;
    });
}

fn init_common_runtimes(c: &mut ConfigRuntimes, options: &RuntimeOptions) {
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
}

fn create_workload_scheduler(options: &RuntimeOptions, capacity: usize) -> Scheduler {
    assert!(
        capacity > 0,
        "experimental workload scheduler capacity must be greater than zero"
    );
    let scheduler_options = &options.experimental_workload_scheduler;
    let scheduler = Scheduler::builder()
        // This is deliberately an internal scheduler bound, not public config.
        .max_concurrent_polls(capacity)
        .sample_every_polls(scheduler_options.sample_every_polls.get())
        .weight(QUERY_TASK_CLASS, scheduler_options.query_weight.get())
        .weight(WRITE_TASK_CLASS, scheduler_options.write_weight.get())
        .build();
    scheduler.set_enabled(scheduler_options.enable);
    register_workload_scheduler_metrics(scheduler.clone());
    info!(
        "Constructed the experimental workload scheduler: internal_capacity={}, \
         query_weight={}, write_weight={}, sample_every_polls={}, enabled={}",
        capacity,
        scheduler_options.query_weight,
        scheduler_options.write_weight,
        scheduler_options.sample_every_polls,
        scheduler_options.enable
    );
    scheduler
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

/// Sets the query and write weights atomically. Returns `false` when no
/// scheduler was constructed at startup.
pub fn set_workload_scheduler_weights(query: NonZeroU32, write: NonZeroU32) -> bool {
    let Some(scheduler) = GLOBAL_RUNTIMES.workload_scheduler.as_ref() else {
        warn!(
            "The experimental workload scheduler was not constructed at startup; ignoring query_weight={query}, write_weight={write}"
        );
        return false;
    };

    let weights = BTreeMap::from([(QUERY_TASK_CLASS, query), (WRITE_TASK_CLASS, write)]);
    scheduler.set_weights(&weights);
    info!("Experimental workload scheduler weights query={query}, write={write}");
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};

    use tokio_test::assert_ok;

    use super::*;

    struct CooperativePolls {
        stop: Arc<AtomicBool>,
    }

    impl Future for CooperativePolls {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let deadline = Instant::now() + Duration::from_micros(100);
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }

            if self.stop.load(Ordering::Relaxed) {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn wait_until<F>(description: &str, condition: F)
    where
        F: Fn() -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {description}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

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
    fn test_workload_scheduler_builds_with_initial_enabled_state() {
        let mut options = RuntimeOptions::default();
        options.experimental_workload_scheduler.enable = false;
        options.experimental_workload_scheduler.sample_every_polls = NonZeroUsize::new(7).unwrap();
        let scheduler = create_workload_scheduler(&options, options.global_rt_size);
        assert_eq!(7, scheduler.stats().sample_every_polls);
        assert!(!scheduler.is_enabled());

        scheduler.set_enabled(true);
        assert!(scheduler.is_enabled());
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
    fn test_datanode_query_backlog_does_not_starve_ingest() {
        let query_runtime = create_runtime("test-datanode-query", "test-query-worker", 1);
        let ingest_runtime = create_runtime("test-datanode-ingest", "test-ingest-worker", 1);
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(2)
            .sample_every_polls(16)
            .weight(QUERY_TASK_CLASS, 2)
            .weight(WRITE_TASK_CLASS, 8)
            .build();
        let runtimes = GlobalRuntimes::new(
            Some(query_runtime.clone()),
            Some(query_runtime.clone()),
            Some(query_runtime.clone()),
            Some(query_runtime),
            Some(ingest_runtime),
            Some(scheduler.clone()),
        );

        let stop_queries = Arc::new(AtomicBool::new(false));
        let query_tasks = (0..3)
            .map(|_| {
                runtimes.spawn_query(CooperativePolls {
                    stop: stop_queries.clone(),
                })
            })
            .collect::<Vec<_>>();

        // Establish the actual datanode topology: two query polls occupy the
        // scheduler's capacity while the third self-waking query is queued.
        wait_until("two active query polls and a queued query", || {
            let stats = scheduler.stats();
            stats.active_polls == 2
                && stats
                    .classes
                    .get(&QUERY_TASK_CLASS)
                    .is_some_and(|class| class.tasks == 3 && class.queued >= 1)
        });

        let write = runtimes.spawn_ingest(async {});
        wait_until("write body", || write.is_finished());

        stop_queries.store(true, Ordering::Relaxed);
        for query in &query_tasks {
            query.abort();
        }
        runtimes.block_on_query(async {
            for query in query_tasks {
                let _ = query.await;
            }
        });
        runtimes.block_on_ingest(async {
            write.await.unwrap();
        });
        wait_until("scheduler polls to drain", || {
            scheduler.stats().active_polls == 0
        });
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
