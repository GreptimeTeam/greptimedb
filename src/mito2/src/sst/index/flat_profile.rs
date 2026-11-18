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

//! Linux test-only timing. No changes to production index policy or scheduling.
//! CPU is measured around each poll, so migration between polls is safe.
use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

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
}

fn cpu_ns() -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid writable timespec; this clock measures only the polling thread.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut t) },
        0
    );
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}

pub(crate) fn start() {
    DATA.lock().unwrap().clear();
    ENABLED.store(true, Ordering::Relaxed);
}

pub(crate) fn finish() -> serde_json::Value {
    ENABLED.store(false, Ordering::Relaxed);
    serde_json::to_value(&*DATA.lock().unwrap()).unwrap()
}

fn record(name: &'static str, value: Stage) {
    let mut data = DATA.lock().unwrap();
    let s = data.entry(name).or_default();
    s.calls += 1;
    s.polls += value.polls;
    s.pending_polls += value.pending_polls;
    s.elapsed_ns += value.elapsed_ns;
    s.poll_wall_ns += value.poll_wall_ns;
    s.thread_cpu_ns += value.thread_cpu_ns;
}

pub(crate) async fn measure<F: Future>(name: &'static str, future: F) -> F::Output {
    if !ENABLED.load(Ordering::Relaxed) {
        return future.await;
    }
    let mut future = std::pin::pin!(future);
    let start = Instant::now();
    let mut s = Stage::default();
    let result = poll_fn(|cx| {
        let wall = Instant::now();
        let cpu = cpu_ns();
        let result = future.as_mut().poll(cx);
        s.thread_cpu_ns += cpu_ns() - cpu;
        s.poll_wall_ns += wall.elapsed().as_nanos() as u64;
        s.polls += 1;
        s.pending_polls += u64::from(result.is_pending());
        result
    })
    .await;
    s.elapsed_ns = start.elapsed().as_nanos() as u64;
    record(name, s);
    result
}

pub(crate) fn sync<T>(name: &'static str, f: impl FnOnce() -> T) -> T {
    if !ENABLED.load(Ordering::Relaxed) {
        return f();
    }
    let start = Instant::now();
    let cpu = cpu_ns();
    let result = f();
    let cpu = cpu_ns() - cpu;
    let elapsed = start.elapsed().as_nanos() as u64;
    record(
        name,
        Stage {
            polls: 1,
            elapsed_ns: elapsed,
            poll_wall_ns: elapsed,
            thread_cpu_ns: cpu,
            ..Default::default()
        },
    );
    result
}

#[tokio::test]
async fn pending_time_is_not_thread_cpu() {
    start();
    measure(
        "sleep",
        tokio::time::sleep(std::time::Duration::from_millis(20)),
    )
    .await;
    let result = finish();
    let s = &result["sleep"];
    assert!(s["pending_polls"].as_u64().unwrap() > 0);
    assert!(s["elapsed_ns"].as_u64().unwrap() > s["poll_wall_ns"].as_u64().unwrap());
}
