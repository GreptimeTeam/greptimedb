// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// metric stuffs, inspired by databend

use std::sync::{Arc, Once, RwLock};
use std::time::{Duration, Instant};

use metrics::histogram;
use metrics_exporter_prometheus::PrometheusBuilder;
pub use metrics_exporter_prometheus::PrometheusHandle;
use once_cell::sync::Lazy;

static PROMETHEUS_HANDLE: Lazy<Arc<RwLock<Option<PrometheusHandle>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

pub fn init_default_metrics_recorder() {
    static START: Once = Once::new();
    START.call_once(init_prometheus_recorder)
}

/// Init prometheus recorder.
fn init_prometheus_recorder() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let mut h = PROMETHEUS_HANDLE.as_ref().write().unwrap();
    *h = Some(recorder.handle());
    // TODO(LFC): separate metrics for testing and metrics for production
    // `clear_recorder` is likely not expected to be called in production code, recorder should be
    // globally unique and used throughout the whole lifetime of an application.
    // It's marked as "unsafe" since [this PR](https://github.com/metrics-rs/metrics/pull/302), and
    // "metrics" version also upgraded to 0.19.
    // A quick look in the metrics codes suggests that the "unsafe" call is of no harm. However,
    // it required a further investigation in how to use metric properly.
    unsafe {
        metrics::clear_recorder();
    }
    match metrics::set_boxed_recorder(Box::new(recorder)) {
        Ok(_) => (),
        Err(err) => crate::warn!("Install prometheus recorder failed, cause: {}", err),
    };
}

pub fn try_handle() -> Option<PrometheusHandle> {
    PROMETHEUS_HANDLE.as_ref().read().unwrap().clone()
}

#[must_use = "Timer should be kept in a variable otherwise it cannot observe duration"]
#[derive(Debug)]
pub struct Timer {
    start: Instant,
    name: &'static str,
}

impl Timer {
    pub fn new(name: &'static str) -> Self {
        Self {
            start: Instant::now(),
            name,
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        histogram!(self.name, self.start.elapsed());
    }
}

#[macro_export]
macro_rules! timer {
    ($name: expr) => {
        $crate::metric::Timer::new($name)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elapsed_timer() {
        init_default_metrics_recorder();
        {
            let t = Timer::new("test_elapsed_timer_a");
            drop(t);
        }
        let handle = try_handle().unwrap();
        let text = handle.render();
        assert!(text.contains("test_elapsed_timer_a"));
        assert!(!text.contains("test_elapsed_timer_b"));

        let _ = timer!("test_elapsed_timer_b");
        let text = handle.render();
        assert!(text.contains("test_elapsed_timer_a"));
        assert!(text.contains("test_elapsed_timer_b"));
    }
}
