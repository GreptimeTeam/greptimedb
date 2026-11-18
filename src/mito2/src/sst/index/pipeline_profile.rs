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

//! Linux test-only pipeline timers. Parent/child and parallel durations overlap.
use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;

static ENABLED: AtomicBool = AtomicBool::new(false);
static DATA: Mutex<BTreeMap<&'static str, Stage>> = Mutex::new(BTreeMap::new());

#[derive(Default, Serialize)]
struct Stage {
    calls: u64,
    polls: u64,
    pending_polls: u64,
    elapsed_ns: u64,
    poll_wall_ns: u64,
    thread_cpu_ns: u64,
    max_elapsed_ns: u64,
    active: u64,
    peak_active: u64,
}

fn cpu_ns() -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid writable timespec, sampled on the current polling thread.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut t) },
        0
    );
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}

pub(crate) fn start(enabled: bool) {
    DATA.lock().unwrap().clear();
    ENABLED.store(enabled, Ordering::Relaxed);
}

pub(crate) fn finish() -> serde_json::Value {
    ENABLED.store(false, Ordering::Relaxed);
    serde_json::to_value(&*DATA.lock().unwrap()).unwrap()
}

fn enter(name: &'static str) {
    let mut data = DATA.lock().unwrap();
    let s = data.entry(name).or_default();
    s.active += 1;
    s.peak_active = s.peak_active.max(s.active);
}

fn record(name: &'static str, v: Stage, leave: bool) {
    let mut data = DATA.lock().unwrap();
    let s = data.entry(name).or_default();
    s.calls += 1;
    s.polls += v.polls;
    s.pending_polls += v.pending_polls;
    s.elapsed_ns += v.elapsed_ns;
    s.poll_wall_ns += v.poll_wall_ns;
    s.thread_cpu_ns += v.thread_cpu_ns;
    s.max_elapsed_ns = s.max_elapsed_ns.max(v.elapsed_ns);
    if leave {
        s.active -= 1;
    }
}

pub(crate) async fn measure<F: Future>(name: &'static str, future: F) -> F::Output {
    if !ENABLED.load(Ordering::Relaxed) {
        return future.await;
    }
    let mut future = std::pin::pin!(future);
    enter(name);
    let start = Instant::now();
    let mut v = Stage::default();
    let result = poll_fn(|cx| {
        let wall = Instant::now();
        let cpu = cpu_ns();
        let result = future.as_mut().poll(cx);
        v.thread_cpu_ns += cpu_ns() - cpu;
        v.poll_wall_ns += wall.elapsed().as_nanos() as u64;
        v.polls += 1;
        v.pending_polls += u64::from(result.is_pending());
        result
    })
    .await;
    v.elapsed_ns = start.elapsed().as_nanos() as u64;
    record(name, v, true);
    result
}

/// Only use within synchronous code; never retain across `.await` or migrate threads.
pub(crate) struct SyncScope(Option<(&'static str, Instant, u64)>);

impl SyncScope {
    pub(crate) fn new(name: &'static str) -> Self {
        if ENABLED.load(Ordering::Relaxed) {
            enter(name);
            Self(Some((name, Instant::now(), cpu_ns())))
        } else {
            Self(None)
        }
    }
}

impl Drop for SyncScope {
    fn drop(&mut self) {
        if let Some((name, wall, cpu)) = self.0 {
            let cpu = cpu_ns() - cpu;
            let elapsed = wall.elapsed().as_nanos() as u64;
            record(
                name,
                Stage {
                    polls: 1,
                    elapsed_ns: elapsed,
                    poll_wall_ns: elapsed,
                    thread_cpu_ns: cpu,
                    ..Default::default()
                },
                true,
            );
        }
    }
}

pub(crate) fn queue_wait(name: &'static str, duration: Duration) {
    if ENABLED.load(Ordering::Relaxed) {
        record(
            name,
            Stage {
                elapsed_ns: duration.as_nanos() as u64,
                ..Default::default()
            },
            false,
        );
    }
}

pub(crate) fn field_stage(field: usize) -> &'static str {
    match field {
        0 => "encode_field_0",
        1 => "encode_field_1",
        2 => "encode_field_2",
        3 => "encode_field_3",
        4 => "encode_field_4",
        _ => "encode_field_other",
    }
}

#[tokio::test]
async fn distinguishes_pending_from_cpu() {
    start(true);
    measure("test_wait", tokio::time::sleep(Duration::from_millis(20))).await;
    {
        let _scope = SyncScope::new("test_sync");
        std::hint::black_box(42);
    }
    let v = finish();
    assert!(v["test_wait"]["pending_polls"].as_u64().unwrap() > 0);
    assert!(
        v["test_wait"]["elapsed_ns"].as_u64().unwrap()
            > v["test_wait"]["poll_wall_ns"].as_u64().unwrap()
    );
    assert_eq!(v["test_sync"]["calls"], 1);
    assert_eq!(v["test_sync"]["active"], 0);
}
