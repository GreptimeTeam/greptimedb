// metric stuffs, inspired by databend

use std::sync::{Arc, Once, RwLock};
use std::time::Instant;

use metrics::histogram;
use metrics_exporter_prometheus::PrometheusBuilder;
pub use metrics_exporter_prometheus::PrometheusHandle;
use once_cell::sync::Lazy;

use crate::logging;

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
    metrics::clear_recorder();
    match metrics::set_boxed_recorder(Box::new(recorder)) {
        Ok(_) => (),
        Err(err) => logging::warn!("Install prometheus recorder failed, cause: {}", err),
    };
}

pub fn try_handle() -> Option<PrometheusHandle> {
    PROMETHEUS_HANDLE.as_ref().read().unwrap().clone()
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ElapsedTimer {
    start: Instant,
    name: &'static str,
}

impl ElapsedTimer {
    pub fn new(name: &'static str) -> Self {
        Self {
            start: Instant::now(),
            name,
        }
    }
}

impl Drop for ElapsedTimer {
    fn drop(&mut self) {
        histogram!(self.name, self.start.elapsed());
    }
}

#[macro_export]
macro_rules! elapsed_timer {
    ($name: expr) => {
        $crate::metric::ElapsedTimer::new($name)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elapsed_timer() {
        init_default_metrics_recorder();
        {
            let t = ElapsedTimer::new("test_elapsed_timer_a");
            drop(t);
        }
        let handle = try_handle().unwrap();
        let text = handle.render();
        assert!(text.contains("test_elapsed_timer_a"));
        assert!(!text.contains("test_elapsed_timer_b"));

        elapsed_timer!("test_elapsed_timer_b");
        let text = handle.render();
        assert!(text.contains("test_elapsed_timer_a"));
        assert!(text.contains("test_elapsed_timer_b"));
    }
}
