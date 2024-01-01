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
use std::hash::{Hash, Hasher};

use chrono::{NaiveDateTime, NaiveTime, TimeZone as ChronoTimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::timestamp::TimeUnit;
use crate::timezone::{get_timezone, Timezone};

/// Time value, represents the elapsed time since midnight in the unit of `TimeUnit`.
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Time {
    value: i64,
    unit: TimeUnit,
}

impl Time {
    /// Creates the time by value and `TimeUnit`.
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { value, unit }
    }

    /// Creates the time in nanosecond.
    pub fn new_nanosecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Nanosecond,
        }
    }

    /// Creates the time in second.
    pub fn new_second(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Second,
        }
    }

    /// Creates the time in millisecond.
    pub fn new_millisecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Millisecond,
        }
    }

    /// Creates the time in microsecond.
    pub fn new_microsecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Microsecond,
        }
    }

    /// Returns the `TimeUnit` of the time.
    pub fn unit(&self) -> &TimeUnit {
        &self.unit
    }

    /// Returns the value of the time.
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Convert a time to given time unit.
    /// Return `None` if conversion causes overflow.
    pub fn convert_to(&self, unit: TimeUnit) -> Option<Time> {
        if self.unit().factor() >= unit.factor() {
            let mul = self.unit().factor() / unit.factor();
            let value = self.value.checked_mul(mul as i64)?;
            Some(Time::new(value, unit))
        } else {
            let mul = unit.factor() / self.unit().factor();
            Some(Time::new(self.value.div_euclid(mul as i64), unit))
        }
    }

    /// Split a [Time] into seconds part and nanoseconds part.
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

    /// Format Time to ISO8601 string. If the time exceeds what chrono time can
    /// represent, this function simply print the time unit and value in plain string.
    pub fn to_iso8601_string(&self) -> String {
        self.as_formatted_string("%H:%M:%S%.f%z", None)
    }

    /// Format Time for system timeszone.
    pub fn to_system_tz_string(&self) -> String {
        self.as_formatted_string("%H:%M:%S%.f", None)
    }

    /// Format Time for given timezone.
    /// When timezone is None, using system timezone by default.
    pub fn to_timezone_aware_string(&self, tz: Option<Timezone>) -> String {
        self.as_formatted_string("%H:%M:%S%.f", tz)
    }

    fn as_formatted_string(self, pattern: &str, timezone: Option<Timezone>) -> String {
        if let Some(time) = self.to_chrono_time() {
            let date = Utc::now().date_naive();
            let datetime = NaiveDateTime::new(date, time);
            match get_timezone(timezone) {
                Timezone::Offset(offset) => {
                    format!("{}", offset.from_utc_datetime(&datetime).format(pattern))
                }
                Timezone::Named(tz) => {
                    format!("{}", tz.from_utc_datetime(&datetime).format(pattern))
                }
            }
        } else {
            format!("[Time{}: {}]", self.unit, self.value)
        }
    }

    /// Cast the [Time] into chrono NaiveDateTime
    pub fn to_chrono_time(&self) -> Option<NaiveTime> {
        let (sec, nsec) = self.split();
        if let Ok(sec) = u32::try_from(sec) {
            NaiveTime::from_num_seconds_from_midnight_opt(sec, nsec)
        } else {
            None
        }
    }
}

impl From<i64> for Time {
    fn from(v: i64) -> Self {
        Self {
            value: v,
            unit: TimeUnit::Millisecond,
        }
    }
}

impl From<Time> for i64 {
    fn from(t: Time) -> Self {
        t.value
    }
}

impl From<Time> for i32 {
    fn from(t: Time) -> Self {
        t.value as i32
    }
}

impl From<Time> for serde_json::Value {
    fn from(d: Time) -> Self {
        serde_json::Value::String(d.to_iso8601_string())
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Time {
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

impl PartialEq for Time {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Time {}

impl Hash for Time {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (sec, nsec) = self.split();
        state.write_i64(sec);
        state.write_u32(nsec);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use serde_json::Value;

    use super::*;
    use crate::timezone::set_default_timezone;

    #[test]
    fn test_time() {
        let t = Time::new(1, TimeUnit::Millisecond);
        assert_eq!(TimeUnit::Millisecond, *t.unit());
        assert_eq!(1, t.value());
        assert_eq!(Time::new(1000, TimeUnit::Microsecond), t);
        assert!(t > Time::new(999, TimeUnit::Microsecond));
    }

    #[test]
    fn test_cmp_time() {
        let t1 = Time::new(0, TimeUnit::Millisecond);
        let t2 = Time::new(0, TimeUnit::Second);
        assert_eq!(t2, t1);

        let t1 = Time::new(100_100, TimeUnit::Millisecond);
        let t2 = Time::new(100, TimeUnit::Second);
        assert!(t1 > t2);

        let t1 = Time::new(10_010_001, TimeUnit::Millisecond);
        let t2 = Time::new(100, TimeUnit::Second);
        assert!(t1 > t2);

        let t1 = Time::new(10_010_001, TimeUnit::Nanosecond);
        let t2 = Time::new(100, TimeUnit::Second);
        assert!(t1 < t2);

        let t1 = Time::new(i64::MAX / 1000 * 1000, TimeUnit::Millisecond);
        let t2 = Time::new(i64::MAX / 1000, TimeUnit::Second);
        assert_eq!(t2, t1);

        let t1 = Time::new(i64::MAX, TimeUnit::Millisecond);
        let t2 = Time::new(i64::MAX / 1000 + 1, TimeUnit::Second);
        assert!(t2 > t1);

        let t1 = Time::new(i64::MAX, TimeUnit::Millisecond);
        let t2 = Time::new(i64::MAX / 1000, TimeUnit::Second);
        assert!(t2 < t1);

        let t1 = Time::new(10_010_001, TimeUnit::Millisecond);
        let t2 = Time::new(100, TimeUnit::Second);
        assert!(t1 > t2);

        let t1 = Time::new(-100 * 10_001, TimeUnit::Millisecond);
        let t2 = Time::new(-100, TimeUnit::Second);
        assert!(t2 > t1);
    }

    fn check_hash_eq(t1: Time, t2: Time) {
        let mut hasher = DefaultHasher::new();
        t1.hash(&mut hasher);
        let t1_hash = hasher.finish();

        let mut hasher = DefaultHasher::new();
        t2.hash(&mut hasher);
        let t2_hash = hasher.finish();
        assert_eq!(t2_hash, t1_hash);
    }

    #[test]
    fn test_hash() {
        check_hash_eq(
            Time::new(0, TimeUnit::Millisecond),
            Time::new(0, TimeUnit::Second),
        );
        check_hash_eq(
            Time::new(1000, TimeUnit::Millisecond),
            Time::new(1, TimeUnit::Second),
        );
        check_hash_eq(
            Time::new(1_000_000, TimeUnit::Microsecond),
            Time::new(1, TimeUnit::Second),
        );
        check_hash_eq(
            Time::new(1_000_000_000, TimeUnit::Nanosecond),
            Time::new(1, TimeUnit::Second),
        );
    }

    #[test]
    pub fn test_from_i64() {
        let t: Time = 42.into();
        assert_eq!(42, t.value());
        assert_eq!(TimeUnit::Millisecond, *t.unit());
    }

    #[test]
    fn test_to_iso8601_string() {
        set_default_timezone(Some("+10:00")).unwrap();
        let time_millis = 1000001;
        let ts = Time::new_millisecond(time_millis);
        assert_eq!("10:16:40.001+1000", ts.to_iso8601_string());

        let time_millis = 1000;
        let ts = Time::new_millisecond(time_millis);
        assert_eq!("10:00:01+1000", ts.to_iso8601_string());

        let time_millis = 1;
        let ts = Time::new_millisecond(time_millis);
        assert_eq!("10:00:00.001+1000", ts.to_iso8601_string());

        let time_seconds = 9 * 3600;
        let ts = Time::new_second(time_seconds);
        assert_eq!("19:00:00+1000", ts.to_iso8601_string());

        let time_seconds = 23 * 3600;
        let ts = Time::new_second(time_seconds);
        assert_eq!("09:00:00+1000", ts.to_iso8601_string());
    }

    #[test]
    fn test_serialize_to_json_value() {
        set_default_timezone(Some("+10:00")).unwrap();
        assert_eq!(
            "10:00:01+1000",
            match serde_json::Value::from(Time::new(1, TimeUnit::Second)) {
                Value::String(s) => s,
                _ => unreachable!(),
            }
        );

        assert_eq!(
            "10:00:00.001+1000",
            match serde_json::Value::from(Time::new(1, TimeUnit::Millisecond)) {
                Value::String(s) => s,
                _ => unreachable!(),
            }
        );

        assert_eq!(
            "10:00:00.000001+1000",
            match serde_json::Value::from(Time::new(1, TimeUnit::Microsecond)) {
                Value::String(s) => s,
                _ => unreachable!(),
            }
        );

        assert_eq!(
            "10:00:00.000000001+1000",
            match serde_json::Value::from(Time::new(1, TimeUnit::Nanosecond)) {
                Value::String(s) => s,
                _ => unreachable!(),
            }
        );
    }

    #[test]
    fn test_to_timezone_aware_string() {
        set_default_timezone(Some("+10:00")).unwrap();

        assert_eq!(
            "10:00:00.001",
            Time::new(1, TimeUnit::Millisecond).to_timezone_aware_string(None)
        );
        std::env::set_var("TZ", "Asia/Shanghai");
        assert_eq!(
            "08:00:00.001",
            Time::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("SYSTEM").unwrap()))
        );
        assert_eq!(
            "08:00:00.001",
            Time::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("+08:00").unwrap()))
        );
        assert_eq!(
            "07:00:00.001",
            Time::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("+07:00").unwrap()))
        );
        assert_eq!(
            "23:00:00.001",
            Time::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("-01:00").unwrap()))
        );
        assert_eq!(
            "08:00:00.001",
            Time::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("Asia/Shanghai").unwrap()))
        );
        assert_eq!(
            "00:00:00.001",
            Time::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("UTC").unwrap()))
        );
        assert_eq!(
            "03:00:00.001",
            Time::new(1, TimeUnit::Millisecond)
                .to_timezone_aware_string(Some(Timezone::from_tz_string("Europe/Moscow").unwrap()))
        );
    }
}
