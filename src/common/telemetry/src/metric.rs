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

// metric stuffs, inspired by databend

use std::fmt;
use std::sync::{Arc, Once, RwLock};
use std::time::{Duration, Instant};

use metrics::{register_histogram, Histogram, IntoLabels};
use metrics_exporter_prometheus::PrometheusBuilder;
pub use metrics_exporter_prometheus::PrometheusHandle;
use metrics_util::layers::{Layer, PrefixLayer};
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
    let layer = PrefixLayer::new("greptime");
    let layered = layer.layer(recorder);
    match metrics::set_boxed_recorder(Box::new(layered)) {
        Ok(_) => (),
        Err(err) => crate::warn!("Install prometheus recorder failed, cause: {}", err),
    };
}

pub fn try_handle() -> Option<PrometheusHandle> {
    PROMETHEUS_HANDLE.as_ref().read().unwrap().clone()
}

/// A Histogram timer that emits the elapsed time to the histogram on drop.
#[must_use = "Timer should be kept in a variable otherwise it cannot observe duration"]
pub struct Timer {
    start: Instant,
    histogram: Histogram,
    observed: bool,
}

impl From<Histogram> for Timer {
    fn from(histogram: Histogram) -> Timer {
        Timer::from_histogram(histogram)
    }
}

impl fmt::Debug for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timer")
            .field("start", &self.start)
            .field("observed", &self.observed)
            .finish()
    }
}

impl Timer {
    /// Creates a timer from given histogram.
    pub fn from_histogram(histogram: Histogram) -> Self {
        Self {
            start: Instant::now(),
            histogram,
            observed: false,
        }
    }

    /// Creates a timer from given `name`.
    pub fn new(name: &'static str) -> Self {
        Self {
            start: Instant::now(),
            histogram: register_histogram!(name),
            observed: false,
        }
    }

    /// Creates a timer from given `name`.
    pub fn new_with_labels<L: IntoLabels>(name: &'static str, labels: L) -> Self {
        Self {
            start: Instant::now(),
            histogram: register_histogram!(name, labels),
            observed: false,
        }
    }

    /// Returns the elapsed duration from the time this timer created.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Discards the timer result.
    pub fn discard(mut self) {
        self.observed = true;
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if !self.observed {
            self.histogram.record(self.elapsed())
        }
    }
}

#[macro_export]
macro_rules! timer {
    ($name: expr) => {
        $crate::metric::Timer::new($name)
    };
    ($name:expr, $labels:expr) => {
        $crate::metric::Timer::new_with_labels($name, $labels)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elapsed_timer() {
        init_default_metrics_recorder();
        {
            let _t = timer!("test_elapsed_timer_a");
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

    #[test]
    fn test_elapsed_timer_with_label() {
        init_default_metrics_recorder();
        {
            let _t = timer!("test_elapsed_timer_a");
        }
        let handle = try_handle().unwrap();
        let text = handle.render();
        assert!(text.contains("test_elapsed_timer_a"));
        assert!(!text.contains("test_elapsed_timer_b"));
        let label_a = "label_a";
        let label_b = "label_b";
        assert!(!text.contains(label_a));
        assert!(!text.contains(label_b));

        {
            let _t = timer!("test_elapsed_timer_b", &[(label_a, "a"), (label_b, "b")]);
        }
        let text = handle.render();
        assert!(text.contains("test_elapsed_timer_a"));
        assert!(text.contains("test_elapsed_timer_b"));
        assert!(text.contains(label_a));
        assert!(text.contains(label_b));
    }
}
