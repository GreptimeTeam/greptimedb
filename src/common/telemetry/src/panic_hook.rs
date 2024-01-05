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

use std::panic;
#[cfg(feature = "deadlock_detection")]
use std::time::Duration;

use backtrace::Backtrace;
use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref PANIC_COUNTER: IntCounter =
        register_int_counter!("greptime_panic_counter", "panic_counter").unwrap();
}

pub fn set_panic_hook() {
    // Set a panic hook that records the panic as a `tracing` event at the
    // `ERROR` verbosity level.
    //
    // If we are currently in a span when the panic occurred, the logged event
    // will include the current span, allowing the context in which the panic
    // occurred to be recorded.
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic| {
        let backtrace = Backtrace::new();
        let backtrace = format!("{backtrace:?}");
        if let Some(location) = panic.location() {
            tracing::error!(
                message = %panic,
                backtrace = %backtrace,
                panic.file = location.file(),
                panic.line = location.line(),
                panic.column = location.column(),
            );
        } else {
            tracing::error!(message = %panic, backtrace = %backtrace);
        }
        PANIC_COUNTER.inc();
        default_hook(panic);
    }));

    #[cfg(feature = "deadlock_detection")]
    let _ = std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(5));
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        tracing::info!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            tracing::info!("Deadlock #{}", i);
            for t in threads {
                tracing::info!("Thread Id {:#?}", t.thread_id());
                tracing::info!("{:#?}", t.backtrace());
            }
        }
    });
}
