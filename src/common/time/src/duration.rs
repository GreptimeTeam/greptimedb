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

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use crate::timestamp::TimeUnit;

/// [Duration] represents the elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Duration {
    value: i64,
    unit: TimeUnit,
}

impl Duration {
    /// Create a new Duration with value and TimeUnit.
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { value, unit }
    }

    /// Create a new Duration in second.
    pub fn new_second(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Second,
        }
    }

    /// Create a new Duration in millisecond.
    pub fn new_millisecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Millisecond,
        }
    }

    /// Create a new Duration in microsecond.
    pub fn new_microsecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Microsecond,
        }
    }

    /// Create a new Duration in nanosecond.
    pub fn new_nanosecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Nanosecond,
        }
    }

    /// Return the TimeUnit of current Duration.
    pub fn unit(&self) -> TimeUnit {
        self.unit
    }

    /// Return the value of current Duration.
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Split a [Duration] into seconds part and nanoseconds part.
    /// Notice the seconds part of split result is always rounded down to floor.
    fn split(&self) -> (i64, u32) {
        let sec_mul = (TimeUnit::Second.factor() / self.unit.factor()) as i64;
        let nsec_mul = (self.unit.factor() / TimeUnit::Nanosecond.factor()) as i64;

        let sec_div = self.value.div_euclid(sec_mul);
        let sec_mod = self.value.rem_euclid(sec_mul);
        // safety:  the max possible value of `sec_mod` is 999,999,999
        let nsec = u32::try_from(sec_mod * nsec_mul).unwrap();
        (sec_div, nsec)
    }

    /// Convert to std::time::Duration.
    pub fn to_std_duration(self) -> std::time::Duration {
        self.into()
    }
}

/// Convert i64 to Duration Type.
/// Default TimeUnit is Millisecond.
impl From<i64> for Duration {
    fn from(v: i64) -> Self {
        Self {
            value: v,
            unit: TimeUnit::Millisecond,
        }
    }
}

/// return i64 value of Duration.
impl From<Duration> for i64 {
    fn from(d: Duration) -> Self {
        d.value
    }
}

/// Convert from std::time::Duration to common_time::Duration Type.
/// The range of std::time::Duration is [0, u64::MAX seconds + 999_999_999 nanoseconds]
/// The range of common_time::Duration is [i64::MIN, i64::MAX] with TimeUnit.
/// If the value of std::time::Duration is out of range of common_time::Duration,
/// it will be rounded to the nearest value.
impl From<std::time::Duration> for Duration {
    fn from(d: std::time::Duration) -> Self {
        // convert as high-precision as possible
        let value = d.as_nanos();
        if value <= i64::MAX as u128 {
            return Self {
                value: value as i64,
                unit: TimeUnit::Nanosecond,
            };
        }

        let value = d.as_micros();
        if value <= i64::MAX as u128 {
            return Self {
                value: value as i64,
                unit: TimeUnit::Microsecond,
            };
        }

        let value = d.as_millis();
        if value <= i64::MAX as u128 {
            return Self {
                value: value as i64,
                unit: TimeUnit::Millisecond,
            };
        }

        let value = d.as_secs();
        if value <= i64::MAX as u64 {
            return Self {
                value: value as i64,
                unit: TimeUnit::Second,
            };
        }

        // overflow, return the max of common_time::Duration
        Self {
            value: i64::MAX,
            unit: TimeUnit::Second,
        }
    }
}

impl From<Duration> for std::time::Duration {
    fn from(d: Duration) -> Self {
        if d.value < 0 {
            return std::time::Duration::new(0, 0);
        }
        match d.unit {
            TimeUnit::Nanosecond => std::time::Duration::from_nanos(d.value as u64),
            TimeUnit::Microsecond => std::time::Duration::from_micros(d.value as u64),
            TimeUnit::Millisecond => std::time::Duration::from_millis(d.value as u64),
            TimeUnit::Second => std::time::Duration::from_secs(d.value as u64),
        }
    }
}

impl From<Duration> for serde_json::Value {
    fn from(d: Duration) -> Self {
        serde_json::Value::String(d.to_string())
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Duration is ordable.
impl Ord for Duration {
    fn cmp(&self, other: &Self) -> Ordering {
        // fast path: most comparisons use the same unit.
        if self.unit == other.unit {
            return self.value.cmp(&other.value);
        }

        let (s_sec, s_nsec) = self.split();
        let (o_sec, o_nsec) = other.split();
        match s_sec.cmp(&o_sec) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => s_nsec.cmp(&o_nsec),
        }
    }
}

impl Display for Duration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.value, self.unit.short_name())
    }
}

impl PartialEq for Duration {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Duration {}

impl Hash for Duration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (sec, nsec) = self.split();
        state.write_i64(sec);
        state.write_u32(nsec);
    }
}

#[cfg(test)]
mod tests {

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use crate::timestamp::TimeUnit;
    use crate::Duration;

    #[test]
    fn test_duration() {
        let d = Duration::new(1, TimeUnit::Second);
        assert_eq!(TimeUnit::Second, d.unit());
        assert_eq!(1, d.value());
        assert_eq!(Duration::new(1000, TimeUnit::Millisecond), d);
        assert!(d > Duration::new(999, TimeUnit::Millisecond));
        assert!(d < Duration::new(1001, TimeUnit::Millisecond));
    }

    #[test]
    fn test_cmp_duration() {
        let d1 = Duration::new(1, TimeUnit::Second);
        let d2 = Duration::new(1, TimeUnit::Millisecond);
        assert!(d1 > d2);

        let d1 = Duration::new(1, TimeUnit::Second);
        let d2 = Duration::new(1, TimeUnit::Microsecond);
        assert!(d1 > d2);

        let d1 = Duration::new(1, TimeUnit::Second);
        let d2 = Duration::new(1_000_000_001, TimeUnit::Nanosecond);
        assert!(d1 < d2);

        let d1 = Duration::new(100, TimeUnit::Millisecond);
        let d2 = Duration::new(1_000_001, TimeUnit::Microsecond);
        assert!(d1 < d2);

        let d1 = Duration::new(i64::MAX / 1000, TimeUnit::Second);
        let d2 = Duration::new(i64::MAX / 1000 * 1000, TimeUnit::Millisecond);
        assert!(d1 == d2);

        let d1 = Duration::new(i64::MAX / 1000 + 1, TimeUnit::Second);
        let d2 = Duration::new(i64::MAX / 1000 * 1000, TimeUnit::Millisecond);
        assert!(d1 > d2);

        let d1 = Duration::new(-100, TimeUnit::Millisecond);
        let d2 = Duration::new(-100 * 999, TimeUnit::Microsecond);
        assert!(d1 < d2);

        let d1 = Duration::new(i64::MIN / 1000, TimeUnit::Millisecond);
        let d2 = Duration::new(i64::MIN / 1000 * 1000, TimeUnit::Microsecond);
        assert!(d1 == d2);
    }

    #[test]
    fn test_convert_i64() {
        let t = Duration::from(1);
        assert_eq!(TimeUnit::Millisecond, t.unit());
        assert_eq!(1, t.value());

        let i: i64 = t.into();
        assert_eq!(1, i);
    }

    #[test]
    fn test_hash() {
        let check_hash_eq = |d1: Duration, d2: Duration| {
            let mut hasher = DefaultHasher::new();
            d1.hash(&mut hasher);
            let d1_hash = hasher.finish();

            let mut hasher = DefaultHasher::new();
            d2.hash(&mut hasher);
            let d2_hash = hasher.finish();
            d1_hash == d2_hash
        };

        let d1 = Duration::new(1, TimeUnit::Second);
        let d2 = Duration::new(1, TimeUnit::Second);
        assert!(check_hash_eq(d1, d2));

        let d1 = Duration::new(1, TimeUnit::Second);
        let d2 = Duration::new(1000, TimeUnit::Millisecond);
        assert!(check_hash_eq(d1, d2));

        let d1 = Duration::new(1, TimeUnit::Second);
        let d2 = Duration::new(1_000_000, TimeUnit::Microsecond);
        assert!(check_hash_eq(d1, d2));

        let d1 = Duration::new(1, TimeUnit::Second);
        let d2 = Duration::new(1_000_000_000, TimeUnit::Nanosecond);
        assert!(check_hash_eq(d1, d2));

        // not equal
        let d1 = Duration::new(1, TimeUnit::Second);
        let d2 = Duration::new(2, TimeUnit::Second);
        assert!(!check_hash_eq(d1, d2));
    }

    #[test]
    fn test_duration_to_string() {
        let d = Duration::new(1, TimeUnit::Second);
        assert_eq!("1s", d.to_string());

        let d = Duration::new(2, TimeUnit::Millisecond);
        assert_eq!("2ms", d.to_string());

        let d = Duration::new(3, TimeUnit::Microsecond);
        assert_eq!("3us", d.to_string());

        let d = Duration::new(4, TimeUnit::Nanosecond);
        assert_eq!("4ns", d.to_string());
    }

    #[test]
    fn test_serialize_to_json_value() {
        let d = Duration::new(1, TimeUnit::Second);
        let json_value = serde_json::to_value(d).unwrap();
        assert_eq!(
            json_value,
            serde_json::json!({"value": 1, "unit": "Second"})
        );

        let d = Duration::new(1, TimeUnit::Millisecond);
        let json_value = serde_json::to_value(d).unwrap();
        assert_eq!(
            json_value,
            serde_json::json!({"value": 1, "unit": "Millisecond"})
        );
    }

    #[test]
    fn test_convert_with_std_duration() {
        // normal test
        let std_duration = std::time::Duration::new(0, 0);
        let duration = Duration::from(std_duration);
        assert_eq!(duration, Duration::new(0, TimeUnit::Nanosecond));

        let std_duration = std::time::Duration::new(1, 0);
        let duration = Duration::from(std_duration);
        assert_eq!(duration, Duration::new(1_000_000_000, TimeUnit::Nanosecond));

        let std_duration = std::time::Duration::from_nanos(i64::MAX as u64);
        let duration = Duration::from(std_duration);
        assert_eq!(duration, Duration::new(i64::MAX, TimeUnit::Nanosecond));

        let std_duration = std::time::Duration::from_nanos(i64::MAX as u64 + 1);
        let duration = Duration::from(std_duration);
        assert_eq!(
            duration,
            Duration::new(i64::MAX / 1000, TimeUnit::Microsecond)
        );

        let std_duration = std::time::Duration::from_nanos(u64::MAX);
        let duration = Duration::from(std_duration);
        assert_eq!(
            duration,
            Duration::new(18446744073709551, TimeUnit::Microsecond)
        );

        let std_duration =
            std::time::Duration::new(i64::MAX as u64 / 1_000, (i64::MAX % 1_000 * 1_000) as u32);
        let duration = Duration::from(std_duration);
        assert_eq!(
            duration,
            Duration::new(9223372036854775000, TimeUnit::Millisecond)
        );

        let std_duration = std::time::Duration::new(i64::MAX as u64, 0);
        let duration = Duration::from(std_duration);
        assert_eq!(duration, Duration::new(i64::MAX, TimeUnit::Second));

        // max std::time::Duration
        let std_duration = std::time::Duration::MAX;
        let duration = Duration::from(std_duration);
        assert_eq!(
            duration,
            Duration::new(9223372036854775807, TimeUnit::Second)
        );

        // overflow test
        let std_duration = std::time::Duration::new(i64::MAX as u64, 1);
        let duration = Duration::from(std_duration);
        assert_eq!(duration, Duration::new(i64::MAX, TimeUnit::Second));

        // convert back to std::time::Duration
        let duration = Duration::new(0, TimeUnit::Nanosecond);
        let std_duration = std::time::Duration::from(duration);
        assert_eq!(std_duration, std::time::Duration::new(0, 0));
    }
}
