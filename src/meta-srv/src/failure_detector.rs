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

use std::collections::VecDeque;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// This is our port of Akka's "[PhiAccrualFailureDetector](https://github.com/akka/akka/blob/v2.6.21/akka-remote/src/main/scala/akka/remote/PhiAccrualFailureDetector.scala)"
/// under Apache License 2.0.
///
/// You can find it's document here:
/// <https://doc.akka.io/docs/akka/2.6.21/typed/failure-detector.html>
///
/// Implementation of 'The Phi Accrual Failure Detector' by Hayashibara et al. as defined in their
/// paper: <https://oneofus.la/have-emacs-will-hack/files/HDY04.pdf>
///
/// The suspicion level of failure is given by a value called φ (phi).
/// The basic idea of the φ failure detector is to express the value of φ on a scale that
/// is dynamically adjusted to reflect current network conditions. A configurable
/// threshold is used to decide if φ is considered to be a failure.
///
/// The value of φ is calculated as:
///
/// φ = -log10(1 - F(timeSinceLastHeartbeat)
///
/// where F is the cumulative distribution function of a normal distribution with mean
/// and standard deviation estimated from historical heartbeat inter-arrival times.
#[cfg_attr(test, derive(Clone))]
pub(crate) struct PhiAccrualFailureDetector {
    /// A low threshold is prone to generate many wrong suspicions but ensures a quick detection
    /// in the event of a real crash. Conversely, a high threshold generates fewer mistakes but
    /// needs more time to detect actual crashes.
    threshold: f32,

    /// Minimum standard deviation to use for the normal distribution used when calculating phi.
    /// Too low standard deviation might result in too much sensitivity for sudden, but normal,
    /// deviations in heartbeat inter arrival times.
    min_std_deviation_millis: f32,

    /// Duration corresponding to number of potentially lost/delayed heartbeats that will be
    /// accepted before considering it to be an anomaly.
    /// This margin is important to be able to survive sudden, occasional, pauses in heartbeat
    /// arrivals, due to for example network drop.
    acceptable_heartbeat_pause_millis: u32,

    /// Bootstrap the stats with heartbeats that corresponds to this duration, with a rather high
    /// standard deviation (since environment is unknown in the beginning).
    first_heartbeat_estimate_millis: u32,

    heartbeat_history: HeartbeatHistory,
    last_heartbeat_millis: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PhiAccrualFailureDetectorOptions {
    pub threshold: f32,
    #[serde(with = "humantime_serde")]
    pub min_std_deviation: Duration,
    #[serde(with = "humantime_serde")]
    pub acceptable_heartbeat_pause: Duration,
    #[serde(with = "humantime_serde")]
    pub first_heartbeat_estimate: Duration,
}

impl Default for PhiAccrualFailureDetectorOptions {
    fn default() -> Self {
        // default configuration is the same as of Akka:
        // https://github.com/akka/akka/blob/v2.6.21/akka-cluster/src/main/resources/reference.conf#L181
        Self {
            threshold: 8_f32,
            min_std_deviation: Duration::from_millis(100),
            acceptable_heartbeat_pause: Duration::from_millis(3000),
            first_heartbeat_estimate: Duration::from_millis(1000),
        }
    }
}

impl Default for PhiAccrualFailureDetector {
    fn default() -> Self {
        Self::from_options(Default::default())
    }
}

impl PhiAccrualFailureDetector {
    pub(crate) fn from_options(options: PhiAccrualFailureDetectorOptions) -> Self {
        Self {
            threshold: options.threshold,
            min_std_deviation_millis: options.min_std_deviation.as_millis() as f32,
            acceptable_heartbeat_pause_millis: options.acceptable_heartbeat_pause.as_millis()
                as u32,
            first_heartbeat_estimate_millis: options.first_heartbeat_estimate.as_millis() as u32,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        }
    }

    pub(crate) fn heartbeat(&mut self, ts_millis: i64) {
        if let Some(last_heartbeat_millis) = self.last_heartbeat_millis {
            if ts_millis < last_heartbeat_millis {
                return;
            }

            if self.is_available(ts_millis) {
                let interval = ts_millis - last_heartbeat_millis;
                self.heartbeat_history.add(interval)
            }
        } else {
            // guess statistics for first heartbeat,
            // important so that connections with only one heartbeat becomes unavailable
            // bootstrap with 2 entries with rather high standard deviation
            let std_deviation = self.first_heartbeat_estimate_millis / 4;
            self.heartbeat_history
                .add((self.first_heartbeat_estimate_millis - std_deviation) as _);
            self.heartbeat_history
                .add((self.first_heartbeat_estimate_millis + std_deviation) as _);
        }
        let _ = self.last_heartbeat_millis.insert(ts_millis);
    }

    pub(crate) fn is_available(&self, ts_millis: i64) -> bool {
        self.phi(ts_millis) < self.threshold as _
    }

    /// The suspicion level of the accrual failure detector.
    ///
    /// If a connection does not have any records in failure detector then it is considered healthy.
    pub(crate) fn phi(&self, ts_millis: i64) -> f64 {
        if let Some(last_heartbeat_millis) = self.last_heartbeat_millis {
            let time_diff = ts_millis - last_heartbeat_millis;
            let mean = self.heartbeat_history.mean();
            let std_deviation = self
                .heartbeat_history
                .std_deviation()
                .max(self.min_std_deviation_millis as _);

            phi(
                time_diff,
                mean + self.acceptable_heartbeat_pause_millis as f64,
                std_deviation,
            )
        } else {
            // treat unmanaged connections, e.g. with zero heartbeats, as healthy connections
            0.0
        }
    }

    #[cfg(test)]
    pub(crate) fn threshold(&self) -> f32 {
        self.threshold
    }

    #[cfg(test)]
    pub(crate) fn acceptable_heartbeat_pause_millis(&self) -> u32 {
        self.acceptable_heartbeat_pause_millis
    }
}

/// Calculation of phi, derived from the Cumulative distribution function for
/// N(mean, stdDeviation) normal distribution, given by
/// 1.0 / (1.0 + math.exp(-y * (1.5976 + 0.070566 * y * y)))
/// where y = (x - mean) / standard_deviation
/// This is an approximation defined in β Mathematics Handbook (Logistic approximation).
/// Error is 0.00014 at +- 3.16
/// The calculated value is equivalent to -log10(1 - CDF(y))
///
/// Usually phi = 1 means likeliness that we will make a mistake is about 10%.
/// The likeliness is about 1% with phi = 2, 0.1% with phi = 3 and so on.
fn phi(time_diff: i64, mean: f64, std_deviation: f64) -> f64 {
    assert_ne!(std_deviation, 0.0);

    let time_diff = time_diff as f64;
    let y = (time_diff - mean) / std_deviation;
    let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
    if time_diff > mean {
        -(e / (1.0 + e)).log10()
    } else {
        -(1.0 - 1.0 / (1.0 + e)).log10()
    }
}

/// Holds the heartbeat statistics.
/// It is capped by the number of samples specified in `max_sample_size`.
///
/// The stats (mean, variance, std_deviation) are not defined for empty HeartbeatHistory.
#[derive(Clone)]
struct HeartbeatHistory {
    /// Number of samples to use for calculation of mean and standard deviation of inter-arrival
    /// times.
    max_sample_size: u32,

    intervals: VecDeque<i64>,
    interval_sum: i64,
    squared_interval_sum: i64,
}

impl HeartbeatHistory {
    fn new(max_sample_size: u32) -> Self {
        Self {
            max_sample_size,
            intervals: VecDeque::with_capacity(max_sample_size as usize),
            interval_sum: 0,
            squared_interval_sum: 0,
        }
    }

    fn mean(&self) -> f64 {
        self.interval_sum as f64 / self.intervals.len() as f64
    }

    fn variance(&self) -> f64 {
        let mean = self.mean();
        self.squared_interval_sum as f64 / self.intervals.len() as f64 - mean * mean
    }

    fn std_deviation(&self) -> f64 {
        self.variance().sqrt()
    }

    fn add(&mut self, interval: i64) {
        if self.intervals.len() as u32 >= self.max_sample_size {
            self.drop_oldest();
        }
        self.intervals.push_back(interval);
        self.interval_sum += interval;
        self.squared_interval_sum += interval * interval;
    }

    fn drop_oldest(&mut self) {
        let oldest = self
            .intervals
            .pop_front()
            .expect("intervals must not be empty here");
        self.interval_sum -= oldest;
        self.squared_interval_sum -= oldest * oldest;
    }
}

#[cfg(test)]
mod tests {
    use common_time::util::current_time_millis;

    use super::*;

    #[test]
    fn test_is_available() {
        let ts_millis = current_time_millis();

        let mut fd = PhiAccrualFailureDetector::default();

        // is available before first heartbeat
        assert!(fd.is_available(ts_millis));

        fd.heartbeat(ts_millis);

        let acceptable_heartbeat_pause_millis = fd.acceptable_heartbeat_pause_millis as i64;
        // is available when heartbeat
        assert!(fd.is_available(ts_millis));
        // is available before heartbeat timeout
        assert!(fd.is_available(ts_millis + acceptable_heartbeat_pause_millis / 2));
        // is not available after heartbeat timeout
        assert!(!fd.is_available(ts_millis + acceptable_heartbeat_pause_millis * 2));
    }

    #[test]
    fn test_last_heartbeat() {
        let ts_millis = current_time_millis();

        let mut fd = PhiAccrualFailureDetector::default();

        // no heartbeat yet
        assert!(fd.last_heartbeat_millis.is_none());

        fd.heartbeat(ts_millis);
        assert_eq!(fd.last_heartbeat_millis, Some(ts_millis));
    }

    #[test]
    fn test_phi() {
        let ts_millis = current_time_millis();

        let mut fd = PhiAccrualFailureDetector::default();

        // phi == 0 before first heartbeat
        assert_eq!(fd.phi(ts_millis), 0.0);

        fd.heartbeat(ts_millis);

        let acceptable_heartbeat_pause_millis = fd.acceptable_heartbeat_pause_millis as i64;
        // phi == 0 when heartbeat
        assert_eq!(fd.phi(ts_millis), 0.0);
        // phi < threshold before heartbeat timeout
        let now = ts_millis + acceptable_heartbeat_pause_millis / 2;
        assert!(fd.phi(now) < fd.threshold as _);
        // phi >= threshold after heartbeat timeout
        let now = ts_millis + acceptable_heartbeat_pause_millis * 2;
        assert!(fd.phi(now) >= fd.threshold as _);
    }

    // The following test cases are port from Akka's tests under Apache License 2.0:
    // [AccrualFailureDetectorSpec.scala](https://github.com/akka/akka/blob/v2.6.21/akka-remote/src/test/scala/akka/remote/AccrualFailureDetectorSpec.scala).

    #[test]
    fn test_use_good_enough_cumulative_distribution_function() {
        fn cdf(phi: f64) -> f64 {
            1.0 - 10.0_f64.powf(-phi)
        }

        assert!((cdf(phi(0, 0.0, 10.0)) - 0.5).abs() < 0.001);
        assert!((cdf(phi(6, 0.0, 10.0)) - 0.7257).abs() < 0.001);
        assert!((cdf(phi(15, 0.0, 10.0)) - 0.9332).abs() < 0.001);
        assert!((cdf(phi(20, 0.0, 10.0)) - 0.97725).abs() < 0.001);
        assert!((cdf(phi(25, 0.0, 10.0)) - 0.99379).abs() < 0.001);
        assert!((cdf(phi(35, 0.0, 10.0)) - 0.99977).abs() < 0.001);
        assert!((cdf(phi(40, 0.0, 10.0)) - 0.99997).abs() < 0.0001);

        for w in (0..40).collect::<Vec<i64>>().windows(2) {
            assert!(phi(w[0], 0.0, 10.0) < phi(w[1], 0.0, 10.0));
        }

        assert!((cdf(phi(22, 20.0, 3.0)) - 0.7475).abs() < 0.001);
    }

    #[test]
    fn test_handle_outliers_without_losing_precision_or_hitting_exceptions() {
        assert!((phi(10, 0.0, 1.0) - 38.0).abs() < 1.0);
        assert_eq!(phi(-25, 0.0, 1.0), 0.0);
    }

    #[test]
    fn test_return_realistic_phi_values() {
        let test = vec![
            (0, 0.0),
            (500, 0.1),
            (1000, 0.3),
            (1200, 1.6),
            (1400, 4.7),
            (1600, 10.8),
            (1700, 15.3),
        ];
        for (time_diff, expected_phi) in test {
            assert!((phi(time_diff, 1000.0, 100.0) - expected_phi).abs() < 0.1);
        }

        // larger std_deviation results => lower phi
        assert!(phi(1100, 1000.0, 500.0) < phi(1100, 1000.0, 100.0));
    }

    #[test]
    fn test_return_phi_of_0_on_startup_when_no_heartbeats() {
        let fd = PhiAccrualFailureDetector {
            threshold: 8.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 0,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        };
        assert_eq!(fd.phi(current_time_millis()), 0.0);
        assert_eq!(fd.phi(current_time_millis()), 0.0);
    }

    #[test]
    fn test_return_phi_based_on_guess_when_only_one_heartbeat() {
        let mut fd = PhiAccrualFailureDetector {
            threshold: 8.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 0,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        };
        fd.heartbeat(0);
        assert!((fd.phi(1000)).abs() - 0.3 < 0.2);
        assert!((fd.phi(2000)).abs() - 4.5 < 0.3);
        assert!((fd.phi(3000)).abs() > 15.0);
    }

    #[test]
    fn test_return_phi_using_first_interval_after_second_heartbeat() {
        let mut fd = PhiAccrualFailureDetector {
            threshold: 8.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 0,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        };
        fd.heartbeat(0);
        assert!(fd.phi(100) > 0.0);
        fd.heartbeat(200);
        assert!(fd.phi(300) > 0.0);
    }

    #[test]
    fn test_is_available_after_a_series_of_successful_heartbeats() {
        let mut fd = PhiAccrualFailureDetector {
            threshold: 8.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 0,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        };
        assert!(fd.last_heartbeat_millis.is_none());
        fd.heartbeat(0);
        fd.heartbeat(1000);
        fd.heartbeat(1100);
        let _ = fd.last_heartbeat_millis.unwrap();
        assert!(fd.is_available(1200));
    }

    #[test]
    fn test_is_not_available_if_heartbeat_are_missed() {
        let mut fd = PhiAccrualFailureDetector {
            threshold: 3.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 0,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        };
        fd.heartbeat(0);
        fd.heartbeat(1000);
        fd.heartbeat(1100);
        assert!(fd.is_available(1200));
        assert!(!fd.is_available(8200));
    }

    #[test]
    fn test_is_available_if_it_starts_heartbeat_again_after_being_marked_dead_due_to_detection_of_failure(
    ) {
        let mut fd = PhiAccrualFailureDetector {
            threshold: 8.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 3000,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        };

        // 1000 regular intervals, 5 minute pause, and then a short pause again that should trigger
        // unreachable again

        let mut now = 0;
        for _ in 0..1000 {
            fd.heartbeat(now);
            now += 1000;
        }
        now += 5 * 60 * 1000;
        assert!(!fd.is_available(now)); // after the long pause
        now += 100;
        fd.heartbeat(now);
        now += 900;
        assert!(fd.is_available(now));
        now += 100;
        fd.heartbeat(now);
        now += 7000;
        assert!(!fd.is_available(now)); // after the 7 seconds pause
        now += 100;
        fd.heartbeat(now);
        now += 900;
        assert!(fd.is_available(now));
        now += 100;
        fd.heartbeat(now);
        now += 900;
        assert!(fd.is_available(now));
    }

    #[test]
    fn test_accept_some_configured_missing_heartbeats() {
        let mut fd = PhiAccrualFailureDetector {
            threshold: 8.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 3000,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        };
        fd.heartbeat(0);
        fd.heartbeat(1000);
        fd.heartbeat(2000);
        fd.heartbeat(3000);
        assert!(fd.is_available(7000));
        fd.heartbeat(8000);
        assert!(fd.is_available(9000));
    }

    #[test]
    fn test_fail_after_configured_acceptable_missing_heartbeats() {
        let mut fd = PhiAccrualFailureDetector {
            threshold: 8.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 3000,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(1000),
            last_heartbeat_millis: None,
        };
        fd.heartbeat(0);
        fd.heartbeat(1000);
        fd.heartbeat(2000);
        fd.heartbeat(3000);
        fd.heartbeat(4000);
        fd.heartbeat(5000);
        assert!(fd.is_available(5500));
        fd.heartbeat(6000);
        assert!(!fd.is_available(11000));
    }

    #[test]
    fn test_use_max_sample_size_heartbeats() {
        let mut fd = PhiAccrualFailureDetector {
            threshold: 8.0,
            min_std_deviation_millis: 100.0,
            acceptable_heartbeat_pause_millis: 0,
            first_heartbeat_estimate_millis: 1000,
            heartbeat_history: HeartbeatHistory::new(3),
            last_heartbeat_millis: None,
        };
        // 100 ms interval
        fd.heartbeat(0);
        fd.heartbeat(100);
        fd.heartbeat(200);
        fd.heartbeat(300);
        let phi1 = fd.phi(400);
        // 500 ms interval, should become same phi when 100 ms intervals have been dropped
        fd.heartbeat(1000);
        fd.heartbeat(1500);
        fd.heartbeat(2000);
        fd.heartbeat(2500);
        let phi2 = fd.phi(3000);
        assert_eq!(phi1, phi2);
    }

    #[test]
    fn test_heartbeat_history_calculate_correct_mean_and_variance() {
        let mut history = HeartbeatHistory::new(20);
        for i in [100, 200, 125, 340, 130] {
            history.add(i);
        }
        assert!((history.mean() - 179.0).abs() < 0.00001);
        assert!((history.variance() - 7584.0).abs() < 0.00001);
    }

    #[test]
    fn test_heartbeat_history_have_0_variance_for_one_sample() {
        let mut history = HeartbeatHistory::new(600);
        history.add(1000);
        assert!((history.variance() - 0.0).abs() < 0.00001);
    }

    #[test]
    fn test_heartbeat_history_be_capped_by_the_specified_max_sample_size() {
        let mut history = HeartbeatHistory::new(3);
        history.add(100);
        history.add(110);
        history.add(90);
        assert!((history.mean() - 100.0).abs() < 0.00001);
        assert!((history.variance() - 66.6666667).abs() < 0.00001);
        history.add(140);
        assert!((history.mean() - 113.333333).abs() < 0.00001);
        assert!((history.variance() - 422.222222).abs() < 0.00001);
        history.add(80);
        assert!((history.mean() - 103.333333).abs() < 0.00001);
        assert!((history.variance() - 688.88888889).abs() < 0.00001);
    }
}
