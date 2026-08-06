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

//! Standalone micro-benchmark for the catio workload scheduler.
//!
//! Creates one catio [`Scheduler`] with two [`TaskClass`]es (query / write) and
//! floods it with `--tasks-per-class` concurrent tasks per class, then compares
//! three fairness metrics under the resulting dual-backlog load:
//!
//! 1. **Poll share** — per-class delta of `ClassStats::polls` divided by the
//!    total poll delta. Mechanism evidence: admission decisions made by the
//!    scheduler's stride policy.
//! 2. **E2E wall latency** — spawn → join time per task. User-perceived
//!    queue + service time.
//! 3. **Active execution time** — wall time the *inner* future is actually
//!    polled after admission, accumulated per class by the [`ExecTimed`]
//!    wrapper around the future passed to `schedule_in`. With `--workers 1`
//!    this approximates CPU time. The `Scheduled` future itself is never
//!    wrapped: a pre-admission `Poll::Pending` would be indistinguishable
//!    from genuine execution otherwise.
//! 4. **Admission wait** — catio's cumulative `ClassStats::total_admission_wait`
//!    / `admitted` deltas over the round, reported as `admitted` (tasks that
//!    made the QUEUED -> ADMITTED transition) and `adm_wait_mean` (mean
//!    scheduler-queue wait in ms, computed from the cumulative totals; catio
//!    does not expose per-event samples, so no percentiles are reported).
//!
//! Workload mix:
//! - `cpu`: both classes run a bounded Leibniz-pi compute loop with periodic
//!   `yield_now()` so a single task spans several polls.
//! - `io`: both classes run `sleep(500us)` + `yield_now()` loops (no
//!   disk/network).
//! - `mixed`: the query class runs the cpu task, the write class runs the io
//!   task. The reverse mapping is deliberately not benchmarked.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use catio::{Scheduler, TaskClass};
use clap::{Parser, ValueEnum};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;

/// Query class (weight given by the `Q` part of `--weights Q:W`).
const CLASS_QUERY: TaskClass = TaskClass::new(1);
/// Write class (weight given by the `W` part of `--weights Q:W`).
const CLASS_WRITE: TaskClass = TaskClass::new(2);

/// Bounded cpu compute iterations per task.
const CPU_ITERATIONS: usize = 6000;
/// Bounded io sleep iterations per task (500us each).
const IO_ITERATIONS: usize = 100;

#[derive(Debug, Parser)]
#[clap(name = "workload-scheduler-micro-bench")]
struct Cli {
    /// Query:write class weights, e.g. "2:8".
    #[clap(long, value_name = "Q:W", default_value = "2:8", value_parser = parse_weights)]
    weights: (u32, u32),

    /// Max concurrent admitted polls in the catio scheduler.
    #[clap(long, default_value_t = 4)]
    max_polls: usize,

    /// Workload mix: cpu, io, or mixed (mixed = query cpu + write io).
    #[clap(long, value_enum, default_value = "mixed")]
    mix: Mix,

    /// Number of tokio worker threads.
    #[clap(long, default_value_t = 4)]
    workers: usize,

    /// Tasks spawned per class per round.
    #[clap(long, default_value_t = 256)]
    tasks_per_class: usize,

    /// Benchmark rounds (results are averaged across rounds when > 1).
    #[clap(long, default_value_t = 1)]
    rounds: usize,

    /// Write the JSON result to this path (a human table is always printed).
    #[clap(long, value_name = "PATH")]
    json: Option<std::path::PathBuf>,

    /// Instrument each class with its own `tokio_metrics::TaskMonitor` and
    /// report cumulative per-class metrics alongside the hand-rolled
    /// wall/exec stats. Requires RUSTFLAGS="--cfg tokio_unstable": the
    /// tokio-metrics crate is a cfg-gated dev-dependency.
    #[clap(long)]
    use_tokio_metrics: bool,

    /// Ignored: `cargo bench` passes `--bench` to the harness even with
    /// `harness = false`; accept and ignore it so `cargo bench -- --args`
    /// works.
    #[clap(long, hide = true)]
    bench: bool,
}

/// Workload mix selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "snake_case")]
enum Mix {
    Cpu,
    Io,
    Mixed,
}

impl Mix {
    fn as_str(self) -> &'static str {
        match self {
            Mix::Cpu => "cpu",
            Mix::Io => "io",
            Mix::Mixed => "mixed",
        }
    }
}

/// Parses a `Q:W` weight string into exactly two positive `u32`s.
fn parse_weights(s: &str) -> Result<(u32, u32), String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(format!(
            "invalid weights {s:?}: expected exactly two positive integers separated by ':'"
        ));
    }
    let query = parts[0]
        .parse::<u32>()
        .map_err(|_| format!("invalid query weight {:?}", parts[0]))?;
    let write = parts[1]
        .parse::<u32>()
        .map_err(|_| format!("invalid write weight {:?}", parts[1]))?;
    if query == 0 || write == 0 {
        return Err(format!("invalid weights {s:?}: weights must be positive"));
    }
    Ok((query, write))
}

#[derive(Debug, Clone, Copy)]
struct Config {
    query_weight: u32,
    write_weight: u32,
    max_polls: usize,
    mix: Mix,
    workers: usize,
    tasks_per_class: usize,
    rounds: usize,
    use_tokio_metrics: bool,
}

impl Config {
    fn from_cli(cli: &Cli) -> Self {
        Self {
            query_weight: cli.weights.0,
            write_weight: cli.weights.1,
            max_polls: cli.max_polls,
            mix: cli.mix,
            workers: cli.workers,
            tasks_per_class: cli.tasks_per_class,
            rounds: cli.rounds,
            use_tokio_metrics: cli.use_tokio_metrics,
        }
    }

    fn weights_str(&self) -> String {
        format!("{}:{}", self.query_weight, self.write_weight)
    }
}

/// Leibniz pi approximation (same style as the old ratelimiter bench).
fn compute_pi_str(precision: usize) -> String {
    let mut pi = 0.0;
    let mut sign = 1.0;

    for i in 0..precision {
        pi += sign / (2 * i + 1) as f64;
        sign *= -1.0;
    }

    pi *= 4.0;
    format!("{:.prec$}", pi, prec = precision)
}

/// Bounded cpu compute loop; yields periodically so the task spans many polls.
async fn cpu_loop() {
    let prefix = 30;
    for _ in 0..CPU_ITERATIONS {
        let _ = compute_pi_str(prefix);
        tokio::task::yield_now().await;
    }
}

/// Bounded io loop: sleeps + yields, no disk/network.
async fn io_loop() {
    for _ in 0..IO_ITERATIONS {
        tokio::time::sleep(Duration::from_micros(500)).await;
        tokio::task::yield_now().await;
    }
}

/// Picks the inner workload for a (class, mix) pair.
async fn workload_task(class: TaskClass, mix: Mix) {
    match (mix, class) {
        (Mix::Cpu, _) | (Mix::Mixed, CLASS_QUERY) => cpu_loop().await,
        (Mix::Io, _) | (Mix::Mixed, CLASS_WRITE) => io_loop().await,
        (Mix::Mixed, _) => unreachable!("only query/write classes are scheduled"),
    }
}

/// Per-class sample accumulators (milliseconds).
#[derive(Default)]
struct ClassSamples {
    /// Spawn → join wall time per task.
    wall_ms: Mutex<Vec<f64>>,
    /// Accumulated inner-poll execution time per task.
    exec_ms: Mutex<Vec<f64>>,
}
/// Wraps the *inner* future handed to `schedule_in` and accumulates the wall
/// time each `inner.poll(cx)` takes. Each task accumulates its own poll
/// durations and pushes a single per-task sample into the shared per-class
/// cell when it completes — so `exec_*` stats are per-task active execution
/// time after admission (pre-admission `Pending`s never reach the inner
/// future). With `--workers 1` this approximates CPU time per task.
struct ExecTimed<F> {
    inner: F,
    samples: Arc<ClassSamples>,
    acc_ms: f64,
}

impl<F> ExecTimed<F> {
    fn new(inner: F, samples: Arc<ClassSamples>) -> Self {
        Self {
            inner,
            samples,
            acc_ms: 0.0,
        }
    }
}

impl<F: Future> Future for ExecTimed<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the inner future is never moved while pinned; we only hand a
        // re-pinned `&mut F` to `poll`.
        let this = unsafe { self.get_unchecked_mut() };
        let start = Instant::now();
        let poll = unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx);
        this.acc_ms += start.elapsed().as_secs_f64() * 1000.0;
        if poll.is_ready() {
            this.samples.exec_ms.lock().unwrap().push(this.acc_ms);
        }
        poll
    }
}

/// Peaks observed by the monitor thread during a round.
#[derive(Debug, Clone, Copy, Default, Serialize)]
struct Peaks {
    active_polls: usize,
    query_queued: usize,
    write_queued: usize,
}

impl Peaks {
    fn average(list: &[Peaks]) -> Peaks {
        let n = list.len().max(1) as f64;
        let avg = |v: usize| (v as f64 / n).round() as usize;
        Peaks {
            active_polls: avg(list.iter().map(|p| p.active_polls).sum()),
            query_queued: avg(list.iter().map(|p| p.query_queued).sum()),
            write_queued: avg(list.iter().map(|p| p.write_queued).sum()),
        }
    }
}

/// Per-class result of one round.
#[derive(Debug, Clone, Serialize)]
struct ClassOutcome {
    tasks: u64,
    completed: u64,
    cancelled: u64,
    polls_delta: u64,
    share: f64,
    wall_p50_ms: f64,
    wall_p95_ms: f64,
    wall_mean_ms: f64,
    exec_p50_ms: f64,
    exec_p95_ms: f64,
    exec_mean_ms: f64,
    /// Tasks admitted at least once during the round (QUEUED -> ADMITTED).
    admitted: u64,
    /// Mean scheduler-queue admission wait in ms, derived from the cumulative
    /// `total_admission_wait` / `admitted` delta (catio only exposes
    /// cumulative totals, so no per-event percentiles are available).
    mean_admission_wait_ms: f64,
}

impl ClassOutcome {
    fn average(list: &[ClassOutcome]) -> ClassOutcome {
        let n = list.len().max(1) as f64;
        let avg = |f: fn(&ClassOutcome) -> f64| list.iter().map(f).sum::<f64>() / n;
        let avg_u64 = |f: fn(&ClassOutcome) -> u64| {
            (list.iter().map(f).sum::<u64>() as f64 / n).round() as u64
        };
        ClassOutcome {
            tasks: avg_u64(|c| c.tasks),
            completed: avg_u64(|c| c.completed),
            cancelled: avg_u64(|c| c.cancelled),
            polls_delta: avg_u64(|c| c.polls_delta),
            share: avg(|c| c.share),
            wall_p50_ms: avg(|c| c.wall_p50_ms),
            wall_p95_ms: avg(|c| c.wall_p95_ms),
            wall_mean_ms: avg(|c| c.wall_mean_ms),
            exec_p50_ms: avg(|c| c.exec_p50_ms),
            exec_p95_ms: avg(|c| c.exec_p95_ms),
            exec_mean_ms: avg(|c| c.exec_mean_ms),
            admitted: avg_u64(|c| c.admitted),
            mean_admission_wait_ms: avg(|c| c.mean_admission_wait_ms),
        }
    }
}

/// Per-class cumulative `tokio_metrics::TaskMonitor` result of one round,
/// exported as millisecond totals plus raw counters. The tokio-metrics crate
/// is a `cfg(tokio_unstable)`-gated dev-dependency, so this plain struct lives
/// unconditionally but is only ever *populated* under `#[cfg(tokio_unstable)]`
/// (via `from_task_metrics`, which is itself cfg-gated).
#[derive(Debug, Clone, Default, Serialize)]
struct TokioMetricsOutcome {
    /// `total_first_poll_delay`: cumulative time from instrumentation to each
    /// task's first poll (pre-admission queueing delay), in ms.
    first_poll_delay_ms: f64,
    /// `total_scheduled_duration`: cumulative time tasks spent waiting to be
    /// polled after being woken (in-runner scheduling delay), in ms.
    scheduled_duration_ms: f64,
    /// `total_poll_duration`: cumulative active (inner-future) execution
    /// time, in ms.
    poll_duration_ms: f64,
    /// `total_idle_duration`: cumulative time tasks spent idling between
    /// polls (waiting on external events), in ms.
    idle_duration_ms: f64,
    /// `instrumented_count`: tasks instrumented with the monitor.
    instrumented_count: u64,
    /// `first_poll_count`: tasks polled at least once.
    first_poll_count: u64,
    /// `dropped_count`: instrumented futures dropped (completion or cancel).
    dropped_count: u64,
}

impl TokioMetricsOutcome {
    fn average(list: &[TokioMetricsOutcome]) -> TokioMetricsOutcome {
        let n = list.len().max(1) as f64;
        let avg = |f: fn(&TokioMetricsOutcome) -> f64| list.iter().map(f).sum::<f64>() / n;
        let avg_u64 = |f: fn(&TokioMetricsOutcome) -> u64| {
            (list.iter().map(f).sum::<u64>() as f64 / n).round() as u64
        };
        TokioMetricsOutcome {
            first_poll_delay_ms: avg(|o| o.first_poll_delay_ms),
            scheduled_duration_ms: avg(|o| o.scheduled_duration_ms),
            poll_duration_ms: avg(|o| o.poll_duration_ms),
            idle_duration_ms: avg(|o| o.idle_duration_ms),
            instrumented_count: avg_u64(|o| o.instrumented_count),
            first_poll_count: avg_u64(|o| o.first_poll_count),
            dropped_count: avg_u64(|o| o.dropped_count),
        }
    }

    /// Converts a `tokio_metrics::TaskMetrics` snapshot. Only available under
    /// `cfg(tokio_unstable)`, mirroring the dev-dependency gate.
    #[cfg(tokio_unstable)]
    fn from_task_metrics(m: tokio_metrics::TaskMetrics) -> Self {
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
        Self {
            first_poll_delay_ms: ms(m.total_first_poll_delay),
            scheduled_duration_ms: ms(m.total_scheduled_duration),
            poll_duration_ms: ms(m.total_poll_duration),
            idle_duration_ms: ms(m.total_idle_duration),
            instrumented_count: m.instrumented_count,
            first_poll_count: m.first_poll_count,
            dropped_count: m.dropped_count,
        }
    }
}

/// Result of one round.
#[derive(Debug, Clone, Serialize)]
struct RoundOutcome {
    wall_seconds: f64,
    per_class: BTreeMap<String, ClassOutcome>,
    peaks: Peaks,
    /// Per-class cumulative tokio-metrics, present when `--use-tokio-metrics`
    /// is enabled (and the build has `--cfg tokio_unstable`).
    #[serde(skip_serializing_if = "Option::is_none")]
    tokio_metrics: Option<BTreeMap<String, TokioMetricsOutcome>>,
}

/// A round plus the raw samples (kept for tests).
struct RoundResult {
    outcome: RoundOutcome,
    /// Raw wall samples per class; only read by tests.
    #[allow(dead_code)]
    wall_samples: BTreeMap<String, Vec<f64>>,
    /// Raw exec samples per class; only read by tests.
    #[allow(dead_code)]
    exec_samples: BTreeMap<String, Vec<f64>>,
}

/// Top-level JSON document: configuration, single/averaged outcome, and
/// per-round details when `--rounds > 1`.
#[derive(Debug, Serialize)]
struct BenchOutput {
    weights: String,
    max_polls: usize,
    mix: String,
    workers: usize,
    tasks_per_class: usize,
    rounds: usize,
    wall_seconds: f64,
    per_class: BTreeMap<String, ClassOutcome>,
    peaks: Peaks,
    /// Per-class cumulative tokio-metrics when `--use-tokio-metrics` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    tokio_metrics: Option<BTreeMap<String, TokioMetricsOutcome>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    round_details: Option<Vec<RoundOutcome>>,
}

/// Sorted-sample percentile mirroring `percentile()` semantics:
/// index = ceil(quantile * len) - 1, clamped to `[0, len - 1]`.
/// Returns `None` for an empty slice. The slice must be sorted ascending.
fn percentile_sorted(sorted: &[f64], quantile: f64) -> Option<f64> {
    let len = sorted.len();
    if len == 0 {
        return None;
    }
    let idx = ((quantile * len as f64).ceil() as usize)
        .saturating_sub(1)
        .min(len - 1);
    Some(sorted[idx])
}

/// Per-class poll share; guards against a zero total.
fn poll_share(class_polls: u64, total_polls: u64) -> f64 {
    if total_polls == 0 {
        0.0
    } else {
        class_polls as f64 / total_polls as f64
    }
}

fn mean_ms(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        samples.iter().sum::<f64>() / samples.len() as f64
    }
}

fn class_outcome(
    before: &catio::ClassStats,
    after: &catio::ClassStats,
    wall: &[f64],
    exec: &[f64],
) -> ClassOutcome {
    let mut wall_sorted = wall.to_vec();
    wall_sorted.sort_by(f64::total_cmp);
    let mut exec_sorted = exec.to_vec();
    exec_sorted.sort_by(f64::total_cmp);

    // catio only exposes cumulative admission-wait totals, so the mean is the
    // delta of `total_admission_wait` divided by the delta of `admitted`.
    let admitted = after.admitted.saturating_sub(before.admitted);
    let total_admission_wait = after
        .total_admission_wait
        .saturating_sub(before.total_admission_wait);

    ClassOutcome {
        tasks: after.tasks.saturating_sub(before.tasks),
        completed: after.completed.saturating_sub(before.completed),
        cancelled: after.cancelled.saturating_sub(before.cancelled),
        polls_delta: after.polls.saturating_sub(before.polls),
        share: 0.0, // filled in by the caller once the total is known
        wall_p50_ms: percentile_sorted(&wall_sorted, 0.50).unwrap_or(0.0),
        wall_p95_ms: percentile_sorted(&wall_sorted, 0.95).unwrap_or(0.0),
        wall_mean_ms: mean_ms(wall),
        exec_p50_ms: percentile_sorted(&exec_sorted, 0.50).unwrap_or(0.0),
        exec_p95_ms: percentile_sorted(&exec_sorted, 0.95).unwrap_or(0.0),
        exec_mean_ms: mean_ms(exec),
        admitted,
        mean_admission_wait_ms: if admitted == 0 {
            0.0
        } else {
            total_admission_wait.as_secs_f64() * 1000.0 / admitted as f64
        },
    }
}

/// Boxed per-task wall-timing future: spawns one scheduled task and returns
/// (class, spawn→join wall ms) when it completes.
type WallTask = Pin<Box<dyn std::future::Future<Output = (TaskClass, f64)> + Send>>;

/// Spawns the 1s-interval peak monitor thread; returns its `JoinHandle`.
/// The thread records peak `active_polls` and per-class queue depth while the
/// load runs, and exits once `shutdown` is set.
fn spawn_peak_monitor(
    scheduler: &Scheduler,
    peaks: &Arc<Mutex<Peaks>>,
    shutdown: &Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    let scheduler = scheduler.clone();
    let peaks = Arc::clone(peaks);
    let shutdown = Arc::clone(shutdown);
    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            let stats = scheduler.stats();
            let mut p = peaks.lock().unwrap();
            p.active_polls = p.active_polls.max(stats.active_polls);
            if let Some(c) = stats.classes.get(&CLASS_QUERY) {
                p.query_queued = p.query_queued.max(c.queued);
            }
            if let Some(c) = stats.classes.get(&CLASS_WRITE) {
                p.write_queued = p.write_queued.max(c.queued);
            }
            drop(p);
            std::thread::sleep(Duration::from_secs(1));
        }
    })
}

/// Builds and drives the per-task [`WallTask`]s for one round and collects the
/// per-class wall/exec sample vectors. `make_inner` constructs the future that
/// is handed to `schedule_in` for one task of a class: the plain path wraps it
/// in [`ExecTimed`], and the `--use-tokio-metrics` path additionally wraps it
/// in the class's `tokio_metrics::TaskMonitor`.
fn collect_samples<F, M>(
    scheduler: &Scheduler,
    runtime: &Arc<tokio::runtime::Runtime>,
    cfg: &Config,
    query_samples: &Arc<ClassSamples>,
    write_samples: &Arc<ClassSamples>,
    make_inner: M,
) -> (f64, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>)
where
    M: Fn(TaskClass, Arc<ClassSamples>) -> F + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let tasks_per_class = cfg.tasks_per_class;
    runtime.block_on(async move {
        let start = Instant::now();
        // Each entry spawns one task and reports its own spawn→join wall time
        // at the moment the task actually completes. Awaited concurrently via
        // FuturesUnordered so a slow task cannot skew another task's timing.
        let mut wall_tasks: Vec<WallTask> = Vec::with_capacity(tasks_per_class * 2);

        for _ in 0..tasks_per_class {
            let sched = scheduler.clone();
            let rt = Arc::clone(runtime);
            let samples = Arc::clone(query_samples);
            let inner_factory = make_inner.clone();
            wall_tasks.push(Box::pin(async move {
                let t0 = Instant::now();
                let inner = inner_factory(CLASS_QUERY, samples);
                let handle = rt.spawn(sched.schedule_in(CLASS_QUERY, inner));
                handle.await.expect("scheduled task panicked");
                (CLASS_QUERY, t0.elapsed().as_secs_f64() * 1000.0)
            }));

            let sched = scheduler.clone();
            let rt = Arc::clone(runtime);
            let samples = Arc::clone(write_samples);
            let inner_factory = make_inner.clone();
            wall_tasks.push(Box::pin(async move {
                let t0 = Instant::now();
                let inner = inner_factory(CLASS_WRITE, samples);
                let handle = rt.spawn(sched.schedule_in(CLASS_WRITE, inner));
                handle.await.expect("scheduled task panicked");
                (CLASS_WRITE, t0.elapsed().as_secs_f64() * 1000.0)
            }));
        }

        let mut pending = FuturesUnordered::from_iter(wall_tasks);
        while let Some((class, wall_ms)) = pending.next().await {
            let samples = if class == CLASS_QUERY {
                query_samples
            } else {
                write_samples
            };
            samples.wall_ms.lock().unwrap().push(wall_ms);
        }

        (
            start.elapsed().as_secs_f64(),
            query_samples.wall_ms.lock().unwrap().clone(),
            write_samples.wall_ms.lock().unwrap().clone(),
            query_samples.exec_ms.lock().unwrap().clone(),
            write_samples.exec_ms.lock().unwrap().clone(),
        )
    })
}

/// Computes the per-class `ClassOutcome`s (polls share, wall/exec percentiles)
/// from the before/after scheduler snapshots and the collected samples.
fn finalize_outcomes(
    before: &catio::SchedulerStats,
    after: &catio::SchedulerStats,
    wall_query: &[f64],
    wall_write: &[f64],
    exec_query: &[f64],
    exec_write: &[f64],
) -> BTreeMap<String, ClassOutcome> {
    let query_before = before
        .classes
        .get(&CLASS_QUERY)
        .cloned()
        .unwrap_or_default();
    let write_before = before
        .classes
        .get(&CLASS_WRITE)
        .cloned()
        .unwrap_or_default();
    let query_after = after.classes.get(&CLASS_QUERY).cloned().unwrap_or_default();
    let write_after = after.classes.get(&CLASS_WRITE).cloned().unwrap_or_default();

    let query_polls = query_after.polls.saturating_sub(query_before.polls);
    let write_polls = write_after.polls.saturating_sub(write_before.polls);
    let total_polls = query_polls + write_polls;

    let mut query = class_outcome(&query_before, &query_after, wall_query, exec_query);
    query.share = poll_share(query_polls, total_polls);
    let mut write = class_outcome(&write_before, &write_after, wall_write, exec_write);
    write.share = poll_share(write_polls, total_polls);

    BTreeMap::from([("query".to_string(), query), ("write".to_string(), write)])
}

/// Runs one round: builds a fresh tokio runtime + catio scheduler, floods it
/// with `tasks_per_class` tasks per class, monitors peaks, and returns the
/// outcome plus the raw wall/exec samples.
fn run_round(cfg: &Config) -> RoundResult {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cfg.workers)
            .enable_all()
            .build()
            .expect("failed to build tokio runtime"),
    );

    let scheduler = Scheduler::builder()
        .max_concurrent_polls(cfg.max_polls)
        .weight(CLASS_QUERY, cfg.query_weight)
        .weight(CLASS_WRITE, cfg.write_weight)
        .build();

    let query_samples = Arc::new(ClassSamples::default());
    let write_samples = Arc::new(ClassSamples::default());
    let peaks = Arc::new(Mutex::new(Peaks::default()));
    let shutdown = Arc::new(AtomicBool::new(false));

    let monitor = spawn_peak_monitor(&scheduler, &peaks, &shutdown);

    let before = scheduler.stats();

    let make_inner = {
        let mix = cfg.mix;
        move |class: TaskClass, samples: Arc<ClassSamples>| {
            Box::pin(ExecTimed::new(workload_task(class, mix), samples))
                as Pin<Box<dyn Future<Output = ()> + Send>>
        }
    };
    let (wall_seconds, wall_query, wall_write, exec_query, exec_write) = collect_samples(
        &scheduler,
        &runtime,
        cfg,
        &query_samples,
        &write_samples,
        make_inner,
    );

    shutdown.store(true, Ordering::Relaxed);
    monitor.join().expect("monitor thread panicked");
    let after = scheduler.stats();
    let peaks = *peaks.lock().unwrap();

    let per_class = finalize_outcomes(
        &before,
        &after,
        &wall_query,
        &wall_write,
        &exec_query,
        &exec_write,
    );

    let outcome = RoundOutcome {
        wall_seconds,
        per_class,
        peaks,
        tokio_metrics: None,
    };

    RoundResult {
        outcome,
        wall_samples: BTreeMap::from([
            ("query".to_string(), wall_query),
            ("write".to_string(), wall_write),
        ]),
        exec_samples: BTreeMap::from([
            ("query".to_string(), exec_query),
            ("write".to_string(), exec_write),
        ]),
    }
}

/// Same round as [`run_round`] but each class also gets its own
/// `tokio_metrics::TaskMonitor`; the future handed to `schedule_in` is wrapped
/// with `monitor.instrument(...)` and the cumulative per-class metrics are
/// exported after all tasks complete. Requires `--cfg tokio_unstable`.
#[cfg(tokio_unstable)]
fn run_round_tokio_metrics(cfg: &Config) -> RoundResult {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cfg.workers)
            .enable_all()
            .build()
            .expect("failed to build tokio runtime"),
    );

    let scheduler = Scheduler::builder()
        .max_concurrent_polls(cfg.max_polls)
        .weight(CLASS_QUERY, cfg.query_weight)
        .weight(CLASS_WRITE, cfg.write_weight)
        .build();

    let query_samples = Arc::new(ClassSamples::default());
    let write_samples = Arc::new(ClassSamples::default());
    let peaks = Arc::new(Mutex::new(Peaks::default()));
    let shutdown = Arc::new(AtomicBool::new(false));

    let monitor = spawn_peak_monitor(&scheduler, &peaks, &shutdown);

    // One TaskMonitor per class; `cumulative()` is read after all tasks
    // complete so each task's totals are folded in.
    let query_task_monitor = tokio_metrics::TaskMonitor::new();
    let write_task_monitor = tokio_metrics::TaskMonitor::new();

    let before = scheduler.stats();

    let make_inner = {
        let query_monitor = query_task_monitor.clone();
        let write_monitor = write_task_monitor.clone();
        let mix = cfg.mix;
        move |class: TaskClass, samples: Arc<ClassSamples>| {
            let monitor = if class == CLASS_QUERY {
                query_monitor.clone()
            } else {
                write_monitor.clone()
            };
            monitor.instrument(ExecTimed::new(workload_task(class, mix), samples))
        }
    };
    let (wall_seconds, wall_query, wall_write, exec_query, exec_write) = collect_samples(
        &scheduler,
        &runtime,
        cfg,
        &query_samples,
        &write_samples,
        make_inner,
    );

    shutdown.store(true, Ordering::Relaxed);
    monitor.join().expect("monitor thread panicked");
    let after = scheduler.stats();
    let peaks = *peaks.lock().unwrap();

    let per_class = finalize_outcomes(
        &before,
        &after,
        &wall_query,
        &wall_write,
        &exec_query,
        &exec_write,
    );

    let tokio_metrics = BTreeMap::from([
        (
            "query".to_string(),
            TokioMetricsOutcome::from_task_metrics(query_task_monitor.cumulative()),
        ),
        (
            "write".to_string(),
            TokioMetricsOutcome::from_task_metrics(write_task_monitor.cumulative()),
        ),
    ]);

    let outcome = RoundOutcome {
        wall_seconds,
        per_class,
        peaks,
        tokio_metrics: Some(tokio_metrics),
    };

    RoundResult {
        outcome,
        wall_samples: BTreeMap::from([
            ("query".to_string(), wall_query),
            ("write".to_string(), wall_write),
        ]),
        exec_samples: BTreeMap::from([
            ("query".to_string(), exec_query),
            ("write".to_string(), exec_write),
        ]),
    }
}

/// Dispatches to the plain or tokio-metrics round depending on the CLI flag.
#[cfg(tokio_unstable)]
fn run_round_dispatch(cfg: &Config) -> RoundResult {
    if cfg.use_tokio_metrics {
        run_round_tokio_metrics(cfg)
    } else {
        run_round(cfg)
    }
}

#[cfg(not(tokio_unstable))]
fn run_round_dispatch(cfg: &Config) -> RoundResult {
    // `main` already rejected `--use-tokio-metrics` without the cfg.
    debug_assert!(!cfg.use_tokio_metrics);
    run_round(cfg)
}

fn average_outcomes(outcomes: &[RoundOutcome]) -> RoundOutcome {
    let n = outcomes.len().max(1) as f64;
    let query = ClassOutcome::average(
        &outcomes
            .iter()
            .map(|o| &o.per_class["query"])
            .cloned()
            .collect::<Vec<_>>(),
    );
    let write = ClassOutcome::average(
        &outcomes
            .iter()
            .map(|o| &o.per_class["write"])
            .cloned()
            .collect::<Vec<_>>(),
    );
    let tokio_metrics = if outcomes.iter().any(|o| o.tokio_metrics.is_some()) {
        let class = |name: &str| {
            TokioMetricsOutcome::average(
                &outcomes
                    .iter()
                    .filter_map(|o| o.tokio_metrics.as_ref().and_then(|m| m.get(name)))
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        };
        Some(BTreeMap::from([
            ("query".to_string(), class("query")),
            ("write".to_string(), class("write")),
        ]))
    } else {
        None
    };
    RoundOutcome {
        wall_seconds: outcomes.iter().map(|o| o.wall_seconds).sum::<f64>() / n,
        per_class: BTreeMap::from([("query".to_string(), query), ("write".to_string(), write)]),
        peaks: Peaks::average(&outcomes.iter().map(|o| o.peaks).collect::<Vec<_>>()),
        tokio_metrics,
    }
}

impl BenchOutput {
    fn new(cfg: &Config, results: &[RoundResult]) -> Self {
        let outcomes: Vec<RoundOutcome> = results.iter().map(|r| r.outcome.clone()).collect();
        let (wall_seconds, per_class, peaks, tokio_metrics, round_details) = if results.len() > 1 {
            let avg = average_outcomes(&outcomes);
            (
                avg.wall_seconds,
                avg.per_class,
                avg.peaks,
                avg.tokio_metrics,
                Some(outcomes),
            )
        } else {
            let single = outcomes.into_iter().next().expect("rounds >= 1");
            (
                single.wall_seconds,
                single.per_class,
                single.peaks,
                single.tokio_metrics,
                None,
            )
        };

        Self {
            weights: cfg.weights_str(),
            max_polls: cfg.max_polls,
            mix: cfg.mix.as_str().to_string(),
            workers: cfg.workers,
            tasks_per_class: cfg.tasks_per_class,
            rounds: cfg.rounds,
            wall_seconds,
            per_class,
            peaks,
            tokio_metrics,
            round_details,
        }
    }
}

fn print_round_table(cfg: &Config, label: &str, outcome: &RoundOutcome) {
    println!();
    println!("{label}");
    println!(
        "weights={} max_polls={} mix={} workers={} tasks_per_class={}",
        cfg.weights_str(),
        cfg.max_polls,
        cfg.mix.as_str(),
        cfg.workers,
        cfg.tasks_per_class
    );
    println!(
        "{:<6}{:>7}{:>11}{:>10}{:>13}{:>8}{:>11}{:>11}{:>11}{:>10}{:>10}{:>10}{:>9}{:>16}   (latency/exec/adm_wait in ms)",
        "class",
        "tasks",
        "completed",
        "cancelled",
        "polls_delta",
        "share",
        "wall_p50",
        "wall_p95",
        "wall_mean",
        "exec_p50",
        "exec_p95",
        "exec_mean",
        "admitted",
        "adm_wait_mean"
    );
    for (name, c) in &outcome.per_class {
        println!(
            "{:<6}{:>7}{:>11}{:>10}{:>13}{:>8.3}{:>11.3}{:>11.3}{:>11.3}{:>10.3}{:>10.3}{:>10.3}{:>9}{:>16.3}",
            name,
            c.tasks,
            c.completed,
            c.cancelled,
            c.polls_delta,
            c.share,
            c.wall_p50_ms,
            c.wall_p95_ms,
            c.wall_mean_ms,
            c.exec_p50_ms,
            c.exec_p95_ms,
            c.exec_mean_ms,
            c.admitted,
            c.mean_admission_wait_ms
        );
    }
    println!(
        "peaks: active_polls={} query_queued={} write_queued={}",
        outcome.peaks.active_polls, outcome.peaks.query_queued, outcome.peaks.write_queued
    );

    if let Some(tokio_metrics) = &outcome.tokio_metrics {
        println!();
        println!("tokio-metrics (per class)                    (ms; cumulative)");
        println!(
            "{:<6}{:>17}{:>17}{:>15}{:>15}{:>14}{:>14}{:>12}",
            "class",
            "first_poll_delay",
            "scheduled",
            "poll",
            "idle",
            "instrumented",
            "first_poll",
            "dropped"
        );
        for (name, m) in tokio_metrics {
            println!(
                "{:<6}{:>17.3}{:>17.3}{:>15.3}{:>15.3}{:>14}{:>14}{:>12}",
                name,
                m.first_poll_delay_ms,
                m.scheduled_duration_ms,
                m.poll_duration_ms,
                m.idle_duration_ms,
                m.instrumented_count,
                m.first_poll_count,
                m.dropped_count
            );
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let cfg = Config::from_cli(&cli);

    if cfg.use_tokio_metrics && !cfg!(tokio_unstable) {
        eprintln!(
            "error: --use-tokio-metrics requires --cfg tokio_unstable (RUSTFLAGS='--cfg tokio_unstable')"
        );
        std::process::exit(1);
    }

    println!("workload-scheduler-micro-bench");
    let mut results = Vec::with_capacity(cli.rounds);
    for round in 0..cli.rounds {
        let result = run_round_dispatch(&cfg);
        print_round_table(
            &cfg,
            &format!(
                "round {}/{} (wall={:.3}s)",
                round + 1,
                cli.rounds,
                result.outcome.wall_seconds
            ),
            &result.outcome,
        );
        results.push(result);
    }

    let output = BenchOutput::new(&cfg, &results);
    if cli.rounds > 1 {
        let avg = average_outcomes(
            &results
                .iter()
                .map(|r| r.outcome.clone())
                .collect::<Vec<_>>(),
        );
        print_round_table(&cfg, "averaged across rounds", &avg);
    }

    if let Some(path) = &cli.json {
        let file = std::fs::File::create(path)
            .unwrap_or_else(|e| panic!("failed to create {}: {e}", path.display()));
        serde_json::to_writer_pretty(file, &output)
            .unwrap_or_else(|e| panic!("failed to write JSON to {}: {e}", path.display()));
        println!();
        println!("wrote JSON to {}", path.display());
    }
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use super::*;

    #[test]
    fn percentile_helper_semantics() {
        let sorted = vec![1.0, 2.0, 3.0, 4.0];
        // ceil(0.5 * 4) - 1 = 1 -> sorted[1]
        assert_eq!(percentile_sorted(&sorted, 0.5), Some(2.0));
        // ceil(0.0 * 4) - 1 saturates to 0 -> sorted[0]
        assert_eq!(percentile_sorted(&sorted, 0.0), Some(1.0));
        // ceil(1.0 * 4) - 1 = 3 -> sorted[3]
        assert_eq!(percentile_sorted(&sorted, 1.0), Some(4.0));
        // ceil(0.95 * 4) - 1 = 3 -> sorted[3]
        assert_eq!(percentile_sorted(&sorted, 0.95), Some(4.0));
        // Single element: clamped to index 0.
        assert_eq!(percentile_sorted(&[7.0], 0.5), Some(7.0));
        assert_eq!(percentile_sorted(&[], 0.5), None);
    }

    #[test]
    fn share_zero_total_guard() {
        assert_eq!(poll_share(0, 0), 0.0);
        assert_eq!(poll_share(10, 0), 0.0);
        assert!((poll_share(2, 10) - 0.2).abs() < 1e-12);
    }

    #[test]
    fn class_outcome_admission_wait_deltas() {
        use std::time::Duration;

        // before: 10 admitted tasks, 100 ms cumulative wait
        let before = catio::ClassStats {
            admitted: 10,
            total_admission_wait: Duration::from_millis(100),
            ..Default::default()
        };

        // after: 20 admitted tasks, 500 ms cumulative wait
        let after = catio::ClassStats {
            admitted: 20,
            total_admission_wait: Duration::from_millis(500),
            ..Default::default()
        };

        let outcome = class_outcome(&before, &after, &[], &[]);
        // admitted_delta = 20 - 10 = 10
        assert_eq!(outcome.admitted, 10);
        // total_wait_delta = 500 - 100 = 400 ms over 10 admitted = 40 ms
        assert!(
            (outcome.mean_admission_wait_ms - 40.0).abs() < 1e-9,
            "mean_admission_wait_ms = {}",
            outcome.mean_admission_wait_ms
        );
    }

    #[test]
    fn class_outcome_admission_wait_zero_guards() {
        use std::time::Duration;

        // No admitted tasks before or after: mean must be 0.0, not NaN/Inf.
        let before = catio::ClassStats::default();
        let after = catio::ClassStats {
            total_admission_wait: Duration::from_millis(100),
            ..Default::default()
        };

        let outcome = class_outcome(&before, &after, &[], &[]);
        assert_eq!(outcome.admitted, 0);
        assert_eq!(outcome.mean_admission_wait_ms, 0.0);

        // Saturating subtraction: a "regressed" cumulative total must not
        // underflow into a huge Duration.
        let before = catio::ClassStats {
            admitted: 5,
            total_admission_wait: Duration::from_millis(200),
            ..Default::default()
        };
        let after = catio::ClassStats::default();
        let outcome = class_outcome(&before, &after, &[], &[]);
        assert_eq!(outcome.admitted, 0);
        assert_eq!(outcome.mean_admission_wait_ms, 0.0);
    }

    #[test]
    fn weights_parsing() {
        assert_eq!(parse_weights("2:8"), Ok((2, 8)));
        // "1:1" is deliberately accepted: the CLI is also run with `--weights 1:1`
        // for comparison, so equal weights must be a valid configuration.
        assert_eq!(parse_weights("1:1"), Ok((1, 1)));
        // Reject "0" (not two parts).
        assert!(parse_weights("0").is_err());
        // Reject zero weights.
        assert!(parse_weights("0:8").is_err());
        assert!(parse_weights("2:0").is_err());
        // Reject three parts.
        assert!(parse_weights("1:1:1").is_err());
        assert!(parse_weights("2:8:3").is_err());
        // Reject non-numeric parts.
        assert!(parse_weights("a:b").is_err());
        assert!(parse_weights("1:x").is_err());
        assert!(parse_weights(":8").is_err());
    }

    #[test]
    fn cli_rejects_bad_weights() {
        assert!(Cli::try_parse_from(["bench", "--weights", "0:8"]).is_err());
        assert!(Cli::try_parse_from(["bench", "--weights", "1:1:1"]).is_err());
        assert!(Cli::try_parse_from(["bench", "--weights", "a:b"]).is_err());
    }

    #[test]
    fn mix_parsing() {
        assert_eq!(
            Cli::try_parse_from(["bench", "--mix", "cpu"]).unwrap().mix,
            Mix::Cpu
        );
        assert_eq!(
            Cli::try_parse_from(["bench", "--mix", "io"]).unwrap().mix,
            Mix::Io
        );
        assert_eq!(
            Cli::try_parse_from(["bench", "--mix", "mixed"])
                .unwrap()
                .mix,
            Mix::Mixed
        );
        assert!(Cli::try_parse_from(["bench", "--mix", "bogus"]).is_err());
        assert!(Cli::try_parse_from(["bench", "--mix", "CPU"]).is_err());
    }

    /// In-process integration round with the small config; keeps the whole
    /// test suite well under ~10s.
    #[test]
    fn integration_round_small() {
        let cfg = Config {
            query_weight: 2,
            write_weight: 8,
            max_polls: 2,
            mix: Mix::Mixed,
            workers: 1,
            tasks_per_class: 32,
            rounds: 1,
            use_tokio_metrics: false,
        };
        let result = run_round(&cfg);
        let outcome = &result.outcome;

        let query = &outcome.per_class["query"];
        let write = &outcome.per_class["write"];

        // Both classes must have been admitted and polled.
        assert!(query.polls_delta > 0, "query polls_delta must be > 0");
        assert!(write.polls_delta > 0, "write polls_delta must be > 0");
        assert_eq!(query.tasks, 32);
        assert_eq!(write.tasks, 32);
        assert_eq!(query.completed + query.cancelled, query.tasks);
        assert_eq!(write.completed + write.cancelled, write.tasks);

        // Backlog case: both classes must have admitted tasks with a
        // non-negative mean admission wait derived from cumulative totals.
        assert!(query.admitted > 0, "query admitted must be > 0");
        assert!(write.admitted > 0, "write admitted must be > 0");
        assert!(
            query.mean_admission_wait_ms >= 0.0,
            "query mean_admission_wait_ms = {}",
            query.mean_admission_wait_ms
        );
        assert!(
            write.mean_admission_wait_ms >= 0.0,
            "write mean_admission_wait_ms = {}",
            write.mean_admission_wait_ms
        );

        // Wall/exec samples must be non-empty for both classes.
        for (name, samples) in &result.wall_samples {
            assert!(!samples.is_empty(), "{name} wall samples empty");
        }
        for (name, samples) in &result.exec_samples {
            assert!(!samples.is_empty(), "{name} exec samples empty");
        }

        // Sorted-sample helper sanity: p50 <= p95 and p95 within [min, max].
        for (name, samples) in &result.wall_samples {
            let mut sorted = samples.clone();
            sorted.sort_by(f64::total_cmp);
            let p50 = percentile_sorted(&sorted, 0.50).expect("p50");
            let p95 = percentile_sorted(&sorted, 0.95).expect("p95");
            assert!(p50 <= p95 + 1e-9, "{name}: p50 {p50} > p95 {p95}");
            assert!(p95 >= sorted[0] && p95 <= *sorted.last().unwrap());
        }

        // Shares must sum to ~1.0.
        let total = query.polls_delta + write.polls_delta;
        assert!(total > 0, "total polls must be > 0");
        assert!(
            (query.share + write.share - 1.0).abs() < 1e-6,
            "shares must sum to 1.0, got {}",
            query.share + write.share
        );

        // Serialized outcome must parse back as JSON with the expected keys.
        let json = serde_json::to_value(outcome).unwrap();
        assert_eq!(
            json["per_class"]["query"]["polls_delta"].as_u64(),
            Some(query.polls_delta)
        );
        assert_eq!(
            json["per_class"]["query"]["admitted"].as_u64(),
            Some(query.admitted)
        );
        assert!(json["per_class"]["query"]["mean_admission_wait_ms"].is_number());
        assert!(json["peaks"]["active_polls"].is_number());
    }

    /// Direct `tokio_metrics::TaskMonitor` semantics check (no catio): the
    /// first-poll delay of a task is recorded when the task is first polled
    /// and equals the time the task spent *not* being polled after
    /// instrumentation. A paused (always-Pending) task that is left unpolled
    /// therefore grows the eventual `total_first_poll_delay` in proportion to
    /// the unpolled wait — this is the pre-admission queueing delay the bench
    /// compares against catio's polls-based share.
    #[cfg(tokio_unstable)]
    #[test]
    fn task_monitor_first_poll_delay_grows_while_unpolled() {
        use tokio_metrics::TaskMonitor;

        let monitor = TaskMonitor::new();
        // Paused task: returns Pending forever and never completes on its own.
        let task = monitor.instrument(std::future::pending::<()>());
        let mut task = Box::pin(task);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Instrumented but not yet polled: nothing is recorded.
        assert_eq!(monitor.cumulative().total_first_poll_delay, Duration::ZERO);
        assert_eq!(monitor.cumulative().first_poll_count, 0);

        // Leave the task unpolled — this is the queueing period the metric
        // must capture.
        let wait = Duration::from_millis(150);
        std::thread::sleep(wait);

        // First poll (inner stays Pending): the delay accrued while the task
        // was not polled is now folded into the cumulative total.
        assert!(task.as_mut().poll(&mut cx).is_pending());

        let metrics = monitor.cumulative();
        assert_eq!(metrics.instrumented_count, 1);
        assert_eq!(metrics.first_poll_count, 1);
        assert!(
            metrics.total_first_poll_delay >= wait,
            "total_first_poll_delay {:?} must be >= unpolled wait {:?}",
            metrics.total_first_poll_delay,
            wait
        );
        // Only the unpolled sleep (plus scheduling noise) may be counted.
        assert!(
            metrics.total_first_poll_delay < wait + Duration::from_secs(5),
            "total_first_poll_delay {:?} unexpectedly large",
            metrics.total_first_poll_delay
        );

        // The per-task delay is a snapshot taken at first poll: it stays
        // stable while the paused task remains unpolled afterwards.
        let stable = monitor.cumulative().total_first_poll_delay;
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(monitor.cumulative().total_first_poll_delay, stable);

        // Dropping the paused instrumented task is counted as a drop.
        drop(task);
        assert_eq!(monitor.cumulative().dropped_count, 1);
    }

    /// Subprocess integration test: runs the real binary with the small config
    /// and a `--json` output file, asserting exit 0 and a parseable JSON result.
    #[test]
    fn integration_binary_end_to_end() {
        let bin = bench_binary_path();
        if !bin.exists() {
            eprintln!(
                "skipping subprocess integration test: {} not built",
                bin.display()
            );
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let json_path = dir.path().join("out.json");

        let output = std::process::Command::new(&bin)
            .args([
                "--workers",
                "1",
                "--tasks-per-class",
                "32",
                "--max-polls",
                "2",
                "--mix",
                "mixed",
                "--weights",
                "2:8",
                "--json",
            ])
            .arg(&json_path)
            .output()
            .expect("failed to run benchmark binary");

        assert!(
            output.status.success(),
            "binary exited with {:?}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );

        let json: serde_json::Value = serde_json::from_slice(&std::fs::read(&json_path).unwrap())
            .expect("JSON output must parse");
        assert_eq!(json["weights"], "2:8");
        assert_eq!(json["max_polls"], 2);
        assert_eq!(json["mix"], "mixed");
        assert!(json["per_class"]["query"]["polls_delta"].as_u64().unwrap() > 0);
        assert!(json["per_class"]["write"]["polls_delta"].as_u64().unwrap() > 0);
        assert!(json["per_class"]["query"]["admitted"].as_u64().unwrap() > 0);
        assert!(json["per_class"]["write"]["admitted"].as_u64().unwrap() > 0);
        assert!(json["per_class"]["query"]["mean_admission_wait_ms"].is_number());
        assert!(json["per_class"]["write"]["mean_admission_wait_ms"].is_number());
        let shares = json["per_class"]["query"]["share"].as_f64().unwrap()
            + json["per_class"]["write"]["share"].as_f64().unwrap();
        assert!((shares - 1.0).abs() < 1e-6);
    }

    /// Without `--cfg tokio_unstable` the `--use-tokio-metrics` flag must be
    /// rejected with exit code 1 and a clear error message (the tokio-metrics
    /// dev-dependency is cfg-gated). Compiled only in non-unstable builds;
    /// under `--cfg tokio_unstable` the flag is a valid mode.
    #[cfg(not(tokio_unstable))]
    #[test]
    fn use_tokio_metrics_rejected_without_cfg() {
        let bin = bench_binary_path();
        if !bin.exists() {
            eprintln!("skipping subprocess test: {} not built", bin.display());
            return;
        }
        let output = std::process::Command::new(&bin)
            .args([
                "--workers",
                "1",
                "--tasks-per-class",
                "4",
                "--max-polls",
                "1",
                "--use-tokio-metrics",
            ])
            .output()
            .expect("failed to run benchmark binary");

        assert_eq!(
            output.status.code(),
            Some(1),
            "expected exit code 1, got {:?}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("requires --cfg tokio_unstable"),
            "stderr must mention the missing cfg, got: {stderr}"
        );
    }

    /// Locates the real benchmark binary: the `CARGO_BIN_EXE_*` env var is only
    /// guaranteed for integration tests, so fall back to walking up from the
    /// unit-test harness in `target/<profile>/deps/`.
    fn bench_binary_path() -> std::path::PathBuf {
        if let Some(path) = option_env!("CARGO_BIN_EXE_workload-scheduler-micro-bench") {
            return std::path::PathBuf::from(path);
        }
        let exe = std::env::current_exe().expect("current_exe");
        exe.parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("workload-scheduler-micro-bench"))
            .unwrap_or(exe)
    }
}
